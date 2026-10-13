"""E2E: sharelogs ckpool -> share_shipper (ckpool-share-exporter) -> Timescale -> pool-api -> client WebSocket.

Rien n'est mocké côté code: de vrais fichiers .sharelog grandissent dans un tmp_path, le vrai `ship_shares` les lit
(un sous-processus par instance ckpool simulée), les vraies migrations 000..014 de l'exporteur créent le schéma, et le
vrai pool-api (uvicorn + ShareHub + LISTEN/NOTIFY) sert un vrai client `websockets`.

Le shipper tourne en SOUS-PROCESSUS plutôt que dans le processus de pytest:
  - `ckpool_share_exporter.settings.settings` est un singleton lu à l'import (DB_*, BASE_LOG_DIR, POOL_INSTANCE_NAME) et
    `share_shipper._shipper` est un global: deux instances ckpool dans un même processus se marcheraient dessus;
  - les deux projets épinglent des versions différentes de chauff-cmn (module `chauff_cmn`, même nom) et ont chacun un
    `settings` top-level: un seul des deux pourrait être importé dans un même interpréteur;
  - un sous-processus se tue proprement (crash/redémarrage) et son stderr est capturé pour les diagnostics.

Prérequis (sinon skip): Docker, et CKPOOL_EXPORTER_PATH = racine du worktree de l'exporteur (contient src/, migrations/,
tests/). Optionnel: CKPOOL_EXPORTER_PYTHON (interpréteur ayant les dépendances de l'exporteur, défaut: celui de pytest),
TIMESCALE_IMAGE (défaut timescale/timescaledb-ha:pg17).

    CKPOOL_EXPORTER_PATH=/chemin/ckpool-share-exporter/.claude/worktrees/poc-share-shipper pytest tests/e2e
"""

import asyncio
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
import uvicorn
from fastapi import FastAPI
from websockets.asyncio.client import connect

from pool_api.container import Container, set_container
from pool_api.dao.ShareLog import ShareLogDAO
from pool_api.impl.v1.handlers import share_stream
from pool_api.impl.v1.services.share_hub import ShareHub

HERE = Path(__file__).parent
IMAGE = os.environ.get("TIMESCALE_IMAGE", "timescale/timescaledb-ha:pg17")
DB_USER = DB_PASSWORD = DB_NAME = "ckpool"
READY_LOG = "database system is ready to accept connections"
TIMEOUT = 20


# --- environnement -----------------------------------------------------------------------------------------------


@pytest.fixture(scope="session")
def exporter() -> Path:
    raw = os.environ.get("CKPOOL_EXPORTER_PATH")
    if not raw:
        pytest.skip("CKPOOL_EXPORTER_PATH non défini (racine du worktree ckpool-share-exporter)")
    root = Path(raw).resolve()
    for sub in ("src/ckpool_share_exporter", "migrations", "tests/helpers.py"):
        if not (root / sub).exists():
            pytest.skip(f"CKPOOL_EXPORTER_PATH={root} invalide: {sub} introuvable")
    if shutil.which("docker") is None:
        pytest.skip("Docker absent")
    return root


@pytest.fixture(scope="session")
def helpers(exporter):
    """tests/helpers.py de l'exporteur (stdlib seule): chargé par chemin, sans toucher à sys.path ni au cache d'import."""
    spec = importlib.util.spec_from_file_location("ckpool_exporter_test_helpers", exporter / "tests" / "helpers.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def exporter_env(exporter: Path, **extra: str) -> dict[str, str]:
    """Environnement minimal et propre: aucune variable de pool-api (DATABASE_URL...) ne fuit vers l'exporteur."""
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "LANG", "TMPDIR", "VIRTUAL_ENV")}
    env["PYTHONPATH"] = os.pathsep.join([str(exporter / "src"), str(exporter)])
    env["PYTHONUNBUFFERED"] = "1"
    env.update(extra)
    return env


def exporter_python() -> str:
    return os.environ.get("CKPOOL_EXPORTER_PYTHON", sys.executable)


def _wait_for_postgres(container, dsn_factory) -> str:
    """L'image timescaledb démarre, s'arrête, puis redémarre: attendre le SECOND 'ready' puis deux connexions de suite."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        out, err = container.get_logs()
        if (out + err).decode(errors="replace").count(READY_LOG) >= 2:
            break
        time.sleep(0.3)
    dsn = dsn_factory()

    async def probe() -> None:
        stable = 0
        while time.monotonic() < deadline:
            try:
                conn = await asyncpg.connect(dsn, timeout=5)
                await conn.execute("SELECT 1")
                await conn.close()
                stable += 1
                if stable >= 2:
                    return
            except Exception:
                stable = 0
            await asyncio.sleep(0.3)
        raise TimeoutError("Timescale injoignable")

    asyncio.run(probe())
    return dsn


@pytest.fixture(scope="session")
def timescale(exporter, tmp_path_factory):
    """(dsn, host, port) d'un Timescale jetable avec les migrations 000..014 de l'exporteur."""
    try:
        from testcontainers.core.container import DockerContainer

        container = (
            DockerContainer(IMAGE)
            .with_env("POSTGRES_USER", DB_USER)
            .with_env("POSTGRES_PASSWORD", DB_PASSWORD)
            .with_env("POSTGRES_DB", DB_NAME)
            .with_exposed_ports(5432)
        )
        container.start()
    except Exception as e:
        pytest.skip(f"Docker/Timescale indisponible: {e}")
    try:
        host, port = container.get_container_host_ip(), int(container.get_exposed_port(5432))
        dsn = _wait_for_postgres(container, lambda: f"postgresql://{DB_USER}:{DB_PASSWORD}@{host}:{port}/{DB_NAME}")
        result = subprocess.run(
            [exporter_python(), str(HERE / "apply_migrations.py"), dsn],
            env=exporter_env(exporter, DB_NAME="x", DB_HOST="x", DB_PASSWORD="x", DB_USER="x",
                             BASE_LOG_DIR="/nonexistent", POOL_INSTANCE_NAME="x"),
            cwd=tmp_path_factory.mktemp("migrations"),
            capture_output=True, text=True, timeout=180,
        )
        assert result.returncode == 0, f"migrations de l'exporteur en échec:\n{result.stdout}\n{result.stderr}"
        assert "15 migrations" in result.stdout, result.stdout
        yield dsn, host, port
    finally:
        container.stop()


@pytest_asyncio.fixture
async def pg(timescale):
    pool = await asyncpg.create_pool(timescale[0])
    # DELETE et non TRUNCATE: même précaution que l'exporteur (hypertables / agrégats continus)
    await pool.execute(
        "DELETE FROM share_log; DELETE FROM share_cursor; DELETE FROM instance_watermark;"
        " UPDATE share_seq SET epoch = 1, v = 0"
    )
    yield pool
    await pool.close()


# --- shippers (un sous-processus par instance ckpool) ------------------------------------------------------------


class Shipper:
    def __init__(self, proc: subprocess.Popen, stderr_path: Path):
        self.proc, self.stderr_path = proc, stderr_path

    def stderr(self) -> str:
        return self.stderr_path.read_text(errors="replace")

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


@pytest.fixture
def start_shipper(exporter, timescale, pg, tmp_path):
    """Lance le vrai ship_shares sur `base` en tant que `instance`. Tué à la fin du test, avant le nettoyage de la DB."""
    shippers: list[Shipper] = []

    def start(instance: str, base: Path) -> Shipper:
        _, host, port = timescale
        stderr_path = tmp_path / f"shipper-{instance}.err"
        proc = subprocess.Popen(
            [exporter_python(), str(HERE / "shipper_runner.py")],
            env=exporter_env(
                exporter, DB_NAME=DB_NAME, DB_HOST=host, DB_USER=DB_USER, DB_PASSWORD=DB_PASSWORD, PGPORT=str(port),
                BASE_LOG_DIR=str(base), POOL_INSTANCE_NAME=instance,
                SHARE_SHIPPER_BACKFILL="true",  # sinon un premier démarrage saute les fichiers existants
            ),
            cwd=tmp_path, stdout=subprocess.DEVNULL, stderr=stderr_path.open("w"),
        )
        shippers.append(Shipper(proc, stderr_path))
        return shippers[-1]

    yield start
    for s in shippers:
        s.stop()
    for s in shippers:
        err = s.stderr()
        assert "Traceback" not in err, f"le shipper a levé une exception:\n{err}"


# --- fausses sharelogs -------------------------------------------------------------------------------------------


class LogFile:
    """Un `<bloc>/<workinfoid>.sharelog` qui grossit par append, avec le share_id attendu de chaque ligne."""

    def __init__(self, helpers, base: Path, instance: str, block: str, workinfoid: int):
        assert len(block) == 8
        self.h, self.instance, self.block, self.workinfoid = helpers, instance, block, workinfoid
        self.path = base / block / f"{workinfoid}.sharelog"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch()
        self.pending: str | None = None

    def _expected(self, offset: int, diff: float, createdate: int) -> dict:
        return {
            "share_id": f"{self.instance}:{self.block}/{self.workinfoid}.sharelog:{offset}",
            "diff": diff, "instance": self.instance, "createdate": createdate,
        }

    def append(self, *diffs: float, age: int = 5, **kw) -> list[dict]:
        """Ajoute des lignes complètes; `diff` identifie le share. createdate = maintenant - age secondes."""
        createdate = int(time.time()) - age
        size, out, lines = self.path.stat().st_size, [], []
        for diff in diffs:
            text = self.h.line(workinfoid=self.workinfoid, diff=diff, createdate=createdate, **kw)
            out.append(self._expected(size, diff, createdate))
            lines.append(text)
            size += len(text.encode()) + 1
        self.h.append_sharelog(self.path, lines)
        return out

    def append_raw(self, text: str) -> dict:
        """Ligne arbitraire (illisible) terminée par \\n."""
        offset = self.path.stat().st_size
        self.h.append_sharelog(self.path, [text])
        return {"share_id": f"{self.instance}:{self.block}/{self.workinfoid}.sharelog:{offset}"}

    def append_partial(self, diff: float, age: int = 5) -> dict:
        """Écrit la moitié d'une ligne sans \\n, comme ckpool au milieu d'un write."""
        createdate = int(time.time()) - age
        text = self.h.line(workinfoid=self.workinfoid, diff=diff, createdate=createdate)
        cut = len(text) // 2
        expected = self._expected(self.path.stat().st_size, diff, createdate)
        self.h.append_sharelog(self.path, [text[:cut]], final_newline=False)
        self.pending = text[cut:]
        return expected

    def complete(self) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(self.pending + "\n")
        self.pending = None


@pytest.fixture
def make_log(helpers, tmp_path):
    def make(instance: str, block: str = "0000000a", workinfoid: int = 1001, base: Path | None = None) -> LogFile:
        return LogFile(helpers, base or tmp_path / instance, instance, block, workinfoid)

    return make


# --- pool-api (uvicorn réel) --------------------------------------------------------------------------------------


@pytest_asyncio.fixture
async def api(timescale, pg):
    """Le vrai pool-api: route /v1/shares/stream, ShareHub avec LISTEN réel. Le polling est quasi désactivé (30 s):
    seul NOTIFY peut réveiller le hub, sauf au démarrage."""
    hub = ShareHub(ShareLogDAO(pg), timescale[0], poll_interval_s=30, reconnect_delay_s=0.1)
    await hub.start()
    set_container(Container(pool_service=None, share_hub=hub, redis=None))  # type: ignore[arg-type]
    app = FastAPI()
    app.include_router(share_stream.router)
    server = uvicorn.Server(uvicorn.Config(app, port=0, log_level="warning", lifespan="off"))
    serving = asyncio.create_task(server.serve())
    await eventually(lambda: server.started and hub.listen_sessions >= 1)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"ws://127.0.0.1:{port}/v1/shares/stream"
    finally:
        server.should_exit = True
        await serving
        set_container(None)
        await hub.stop()


async def eventually(predicate, timeout: float = TIMEOUT, interval: float = 0.05, what: str = "condition"):
    """Polling borné; predicate peut être sync ou async. Renvoie la dernière valeur (truthy)."""
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timeout ({timeout}s) en attendant: {what}")
        await asyncio.sleep(interval)


class Stream:
    """Client WebSocket qui accumule tous les événements reçus."""

    def __init__(self, ws):
        self.ws, self.events = ws, []

    async def read_until(self, predicate, timeout: float = TIMEOUT, what: str = "événement attendu"):
        deadline = time.monotonic() + timeout
        while not predicate(self):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"timeout en attendant: {what}; reçu: {self.events}")
            self.events.append(json.loads(await asyncio.wait_for(self.ws.recv(), remaining)))
        return self

    def of(self, type_: str) -> list[dict]:
        return [e for e in self.events if e["type"] == type_]

    @property
    def shares(self) -> list[dict]:
        return self.of("share")

    async def until_shares(self, n: int, timeout: float = TIMEOUT):
        return await self.read_until(lambda s: len(s.shares) >= n, timeout, f"{n} shares")


def assert_gapless(shares: list[dict], first_seq: int, epoch: int = 1) -> None:
    assert [(s["epoch"], s["seq"]) for s in shares] == [(epoch, first_seq + i) for i in range(len(shares))]


def assert_matches(shares: list[dict], expected: list[dict]) -> None:
    assert [(s["share_id"], s["diff"], s["instance"]) for s in shares] == [
        (e["share_id"], e["diff"], e["instance"]) for e in expected
    ]
    assert all(not s["parse_error"] for s in shares)


async def db_count(pg) -> int:
    return await pg.fetchval("SELECT count(*) FROM share_log")


async def heartbeat(pg, instance: str):
    return await pg.fetchval("SELECT heartbeat_at FROM instance_watermark WHERE pool_instance = $1", instance)


async def wait_ticks(pg, instance: str, n: int = 2) -> None:
    """Attend n ticks complets du shipper (chaque tick fait avancer heartbeat_at): sert de barrière pour affirmer
    qu'une ligne n'a PAS été émise, sans sleep arbitraire."""
    seen = {await heartbeat(pg, instance)}
    await eventually(lambda: _more_beats(pg, instance, seen, n), what=f"{n} ticks de {instance}")


async def _more_beats(pg, instance, seen, n):
    seen.add(await heartbeat(pg, instance))
    return len(seen) >= n + 1


# --- scénarios ----------------------------------------------------------------------------------------------------


async def test_a_replay_then_live_in_order(pg, api, make_log, start_shipper):
    log = make_log("ckpool01")
    expected = log.append(1, 2, 3, 4, 5)  # écrits AVANT la connexion
    shipper = start_shipper("ckpool01", log.path.parent.parent)
    await eventually(lambda: _count_is(pg, 5), what="5 shares en base")

    async with connect(f"{api}?from=1:0") as ws:
        stream = await Stream(ws).until_shares(5)
        assert stream.events[0]["type"] == "hello" and stream.events[0]["head"] == "1:5"
        live = log.append(6, 7, 8)  # écrits APRÈS la connexion: arrivent via NOTIFY
        expected += live
        await stream.until_shares(8)
        await asyncio.sleep(0.3)  # laisse une chance à un doublon éventuel d'arriver
        stream.events.extend(await _drain(ws))
    assert shipper.proc.poll() is None
    assert_gapless(stream.shares, 1)
    assert len(stream.shares) == 8
    assert_matches(stream.shares, expected)
    first = stream.shares[0]
    assert first["username"] == log.h.ADDRESS_A and first["workername"].startswith(log.h.ADDRESS_A + ".")
    assert first["result"] is True and first["workinfoid"] == 1001 and first["sdiff"] == 1 * 17.5
    assert datetime.fromisoformat(first["created_at"]).timestamp() == pytest.approx(expected[0]["createdate"] + 0.9096, abs=1e-3)
    assert "path" not in first and "raw" not in first
    assert stream.of("watermark"), "un watermark doit accompagner le flux"


async def _count_is(pg, n):
    return await db_count(pg) == n


async def _drain(ws, quiet: float = 0.3) -> list[dict]:
    out = []
    while True:
        try:
            out.append(json.loads(await asyncio.wait_for(ws.recv(), quiet)))
        except asyncio.TimeoutError:
            return out


async def test_b_reconnect_resumes_exactly(pg, api, make_log, start_shipper):
    log = make_log("ckpool01")
    expected = log.append(1, 2, 3, 4, 5, 6)
    start_shipper("ckpool01", log.path.parent.parent)

    async with connect(f"{api}?from=1:0") as ws:
        first = await Stream(ws).until_shares(4)
    last_seen = first.shares[3]
    assert last_seen["seq"] == 4

    # pendant la déconnexion, le shipper continue
    expected += log.append(7, 8, 9)
    await eventually(lambda: _count_is(pg, 9), what="9 shares en base")

    async with connect(f"{api}?from={last_seen['epoch']}:{last_seen['seq']}") as ws:
        second = await Stream(ws).until_shares(5)
        expected += log.append(10)
        await second.until_shares(6)
        second.events.extend(await _drain(ws))
    assert_gapless(second.shares, 5)
    assert_matches(second.shares, expected[4:])
    # hello annonce bien le curseur demandé
    assert second.events[0] == {"type": "hello", "cursor": "1:4", "head": "1:9"}


async def test_c_two_instances_global_seq_and_min_watermark(pg, api, make_log, start_shipper):
    a, b = make_log("ckpool-a"), make_log("ckpool-b", workinfoid=2002)
    exp_a = a.append(1, 2, 3, age=1)
    exp_b = b.append(10, 20, age=30)
    partial_b = b.append_partial(30, age=30)  # B reste bloquée sur une ligne incomplète: son watermark ne bouge plus
    # B d'abord, et on attend sa ligne instance_watermark: le watermark du hub ne recule jamais, donc une instance
    # qui apparaît APRÈS que le hub a émis un watermark plus récent ne peut plus le tirer vers le bas (limite connue)
    start_shipper("ckpool-b", b.path.parent.parent)
    await eventually(lambda: heartbeat(pg, "ckpool-b"), what="première heartbeat de B")
    start_shipper("ckpool-a", a.path.parent.parent)
    await eventually(lambda: _count_is(pg, 5), what="5 shares des deux instances")
    await wait_ticks(pg, "ckpool-a")

    async with connect(f"{api}?from=1:0") as ws:
        stream = await Stream(ws).until_shares(5)
        frontier_b = datetime.fromtimestamp(exp_b[-1]["createdate"] + 0.9096, tz=None).astimezone()
        # watermark = min(instances actives) = frontière de B, bien avant celle de A
        await stream.read_until(lambda s: s.of("watermark"), what="watermark")
        wm = datetime.fromisoformat(stream.of("watermark")[-1]["watermark"])
        assert wm.timestamp() == pytest.approx(frontier_b.timestamp(), abs=1e-3)
        rows = {r["pool_instance"]: r["shipped_until"] for r in await pg.fetch("SELECT * FROM instance_watermark")}
        assert set(rows) == {"ckpool-a", "ckpool-b"}
        assert rows["ckpool-b"] == wm and rows["ckpool-a"] > wm

        # seq global, ordre de commit; au sein d'une instance, ordre du fichier
        assert_gapless(stream.shares, 1)
        by_instance = {i: [s for s in stream.shares if s["instance"] == i] for i in ("ckpool-a", "ckpool-b")}
        assert_matches(by_instance["ckpool-a"], exp_a)
        assert_matches(by_instance["ckpool-b"], exp_b)

        # la ligne de B se termine: elle part, et le watermark de B rejoint celui de A
        b.complete()
        await stream.until_shares(6)
        later = await stream.read_until(
            lambda s: datetime.fromisoformat(s.of("watermark")[-1]["watermark"]) > wm, what="watermark qui avance")
        newest = datetime.fromisoformat(later.of("watermark")[-1]["watermark"])
        db_min = await pg.fetchval("SELECT min(shipped_until) FROM instance_watermark")
        assert wm < newest <= db_min
    assert stream.shares[5]["share_id"] == partial_b["share_id"] and stream.shares[5]["seq"] == 6
    # le watermark ne recule jamais dans le flux
    marks = [datetime.fromisoformat(w["watermark"]) for w in stream.of("watermark")]
    assert marks == sorted(marks)


async def test_d_unreadable_line_emitted_with_null_fields(pg, api, make_log, start_shipper):
    log = make_log("ckpool01")
    before = log.append(1)
    garbage = log.append_raw('{"workinfoid": 1001, "diff": "pas un nombre", ')  # JSON tronqué mais terminé par \n
    after = log.append(2)
    start_shipper("ckpool01", log.path.parent.parent)

    async with connect(f"{api}?from=1:0") as ws:
        stream = await Stream(ws).until_shares(3)
    assert_gapless(stream.shares, 1)  # seq continu malgré la ligne illisible
    bad = stream.shares[1]
    assert bad["parse_error"] is True and bad["share_id"] == garbage["share_id"]
    for field in ("workinfoid", "username", "workername", "diff", "sdiff", "result"):
        assert bad[field] is None, field
    assert "raw" not in bad  # raw reste en base, jamais exposé
    # created_at de repli = celui du share précédent: le watermark reste cohérent
    assert bad["created_at"] == stream.shares[0]["created_at"]
    assert_matches([stream.shares[0], stream.shares[2]], before + after)
    assert await pg.fetchval("SELECT raw FROM share_log WHERE parse_error") == '{"workinfoid": 1001, "diff": "pas un nombre", '


async def test_e_partial_line_not_emitted_until_complete(pg, api, make_log, start_shipper):
    log = make_log("ckpool01")
    expected = log.append(1, 2)
    start_shipper("ckpool01", log.path.parent.parent)

    async with connect(f"{api}?from=1:0") as ws:
        stream = await Stream(ws).until_shares(2)
        partial = log.append_partial(3)
        await wait_ticks(pg, "ckpool01", 3)  # le shipper a vu le fichier plusieurs fois avec la ligne coupée
        assert await db_count(pg) == 2
        extra = await _drain(ws)  # rien de partagé sur le fil (un watermark est légitime)
        assert [e for e in extra if e["type"] == "share"] == []
        stream.events.extend(extra)
        log.complete()
        await stream.until_shares(3)
        expected.append(partial)
        expected += log.append(4)
        await stream.until_shares(4)
    assert_gapless(stream.shares, 1)
    assert_matches(stream.shares, expected)
    assert not any(s["parse_error"] for s in stream.shares)


async def test_f_block_change_mid_stream(pg, api, make_log, start_shipper):
    log_a = make_log("ckpool01", block="0000000a", workinfoid=1001)
    expected = log_a.append(1, 2)
    start_shipper("ckpool01", log_a.path.parent.parent)
    base = log_a.path.parent.parent

    async with connect(f"{api}?from=1:0") as ws:
        stream = await Stream(ws).until_shares(2)
        # nouveau bloc: ckpool ne grossit plus que celui-ci, l'ancien est gardé pour la bascule
        log_b = make_log("ckpool01", block="0000000b", workinfoid=2001, base=base)
        expected += log_b.append(3, 4)
        await stream.until_shares(4)
        expected += log_a.append(5)  # un retardataire sur l'ancien bloc, encore actif, est quand même livré
        expected += log_b.append(6)
        await stream.until_shares(6)
        # troisième bloc: le premier sort des 2 actifs, est lu jusqu'au bout puis fermé
        log_c = make_log("ckpool01", block="0000000c", workinfoid=3001, base=base)
        expected += log_c.append(7)
        await stream.until_shares(7)
        await eventually(
            lambda: pg.fetchval("SELECT closed FROM share_cursor WHERE path = '0000000a/1001.sharelog'"),
            what="curseur du premier bloc fermé",
        )
        stream.events.extend(await _drain(ws))
    assert_gapless(stream.shares, 1)
    # l'ordre inter-fichiers dépend des ticks: on vérifie l'ordre par fichier et l'absence de doublon/perte
    assert sorted(s["share_id"] for s in stream.shares) == sorted(e["share_id"] for e in expected)
    for block in ("0000000a", "0000000b"):
        in_file = [s["diff"] for s in stream.shares if f":{block}/" in s["share_id"]]
        assert in_file == sorted(in_file)
    assert {s["share_id"].split(":")[1].split("/")[0] for s in stream.shares} == {"0000000a", "0000000b", "0000000c"}
