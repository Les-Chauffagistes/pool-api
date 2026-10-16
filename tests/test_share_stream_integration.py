"""Tests contre un vrai Postgres (TimescaleDB + migration 013 de l'exporteur). Ignorés si Docker est indisponible."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
import uvicorn
from fakes import FakeWS
from fastapi import FastAPI
from websockets.asyncio.client import connect

from pool_api.container import Container, set_container
from pool_api.dao.ShareLog import ShareLogDAO
from pool_api.impl.v1.handlers import share_stream
from pool_api.impl.v1.handlers.share_stream import serve
from pool_api.impl.v1.services.share_hub import LISTEN_APPLICATION_NAME, ShareHub

IMAGE = "timescale/timescaledb-ha:pg17"
SQL = Path(__file__).parent / "sql" / "013_share_log.sql"
NOW = datetime.now(timezone.utc).replace(microsecond=0)


@pytest.fixture(scope="module")
def postgres_url():
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer(IMAGE, username="pool", password="pool", dbname="pool", driver=None)
        container.start()
    except Exception as e:  # pragma: no cover
        pytest.skip(f"Docker/Postgres indisponible: {e}")
    try:
        yield f"postgresql://pool:pool@{container.get_container_host_ip()}:{container.get_exposed_port(5432)}/pool"
    finally:
        container.stop()


@pytest_asyncio.fixture
async def pg(postgres_url):
    pool = await asyncpg.create_pool(postgres_url)
    async with pool.acquire() as c:
        if not await c.fetchval("SELECT to_regclass('share_log') IS NOT NULL"):
            await c.execute(SQL.read_text())
        await c.execute("TRUNCATE share_log, instance_watermark; UPDATE share_seq SET epoch = 1, v = 0")
    yield pool
    await pool.close()


async def ship(pg, n: int, instance="ckpool01", epoch=None, watermark=None) -> int:
    """Comme ShareLogDAO.ship du share_shipper: seq sous verrou de share_seq, NOTIFY au commit."""
    async with pg.acquire() as c, c.transaction():
        e, last = await c.fetchrow("UPDATE share_seq SET v = v + $1 WHERE id = 1 RETURNING epoch, v", n)
        for i in range(n):
            seq = last - n + 1 + i
            await c.execute(
                """INSERT INTO share_log (epoch, seq, created_at, pool_instance, path, byte_offset, workinfoid,
                   username, workername, diff, sdiff, result) VALUES ($1,$2,$3,$4,$5,$6,1,'u','w',1,2,true)""",
                e, seq, NOW + timedelta(seconds=seq), instance, "0001/a.sharelog", seq * 10,
            )
        if watermark:
            await c.execute(
                """INSERT INTO instance_watermark VALUES ($1, $2, now())
                   ON CONFLICT (pool_instance) DO UPDATE SET shipped_until = $2, heartbeat_at = now()""",
                instance, watermark,
            )
        await c.execute("SELECT pg_notify('shares', $1)", f"{e}:{last}")
        return last


async def make_hub(pg, url, **kw) -> ShareHub:
    hub = ShareHub(ShareLogDAO(pg), url, poll_interval_s=kw.pop("poll_interval_s", 0.1), reconnect_delay_s=0.1, **kw)
    await hub.start()
    return hub


async def until(predicate, timeout=10):
    async def loop():
        while not predicate():
            await asyncio.sleep(0.02)

    await asyncio.wait_for(loop(), timeout)


async def test_replay_then_live_with_real_notify(pg, postgres_url):
    await ship(pg, 5)
    hub = await make_hub(pg, postgres_url, poll_interval_s=30)  # seul LISTEN peut réveiller
    ws = FakeWS()
    task = asyncio.create_task(serve(ws, hub, (1, 2), heartbeat_s=60))
    try:
        await ws.wait_for(lambda: ws.seqs() == [3, 4, 5])
        await until(lambda: hub.listen_sessions == 1)
        await ship(pg, 3)
        await ws.wait_for(lambda: ws.seqs() == [3, 4, 5, 6, 7, 8])
    finally:
        ws.disconnect()
        await task
        await hub.stop()


async def test_listen_connection_killed_then_catch_up(pg, postgres_url):
    await ship(pg, 2)
    hub = await make_hub(pg, postgres_url, poll_interval_s=30)
    ws = FakeWS()
    task = asyncio.create_task(serve(ws, hub, None, heartbeat_s=60))
    try:
        await until(lambda: hub.listen_sessions == 1)
        await ship(pg, 1)
        await ws.wait_for(lambda: ws.seqs() == [3])
        # coupure de la connexion LISTEN, comme HAProxy
        await pg.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1",
            LISTEN_APPLICATION_NAME,
        )
        await ship(pg, 4)  # commités pendant la coupure
        await until(lambda: hub.listen_sessions == 2)
        await ship(pg, 1)
        await ws.wait_for(lambda: ws.seqs() == [3, 4, 5, 6, 7, 8])
    finally:
        ws.disconnect()
        await task
        await hub.stop()


async def test_resume_and_out_of_retention(pg, postgres_url):
    await ship(pg, 10)
    await pg.execute("DELETE FROM share_log WHERE seq <= 4")  # la rétention est passée par là
    hub = await make_hub(pg, postgres_url)
    try:
        ok = FakeWS()
        task = asyncio.create_task(serve(ok, hub, (1, 4), heartbeat_s=60))
        await ok.wait_for(lambda: ok.seqs() == [5, 6, 7, 8, 9, 10])
        ok.disconnect()
        await task

        old = FakeWS()
        await asyncio.wait_for(serve(old, hub, (1, 2), heartbeat_s=60), 5)
        assert old.closed[0] == 4410 and old.seqs() == []
    finally:
        await hub.stop()


async def test_watermark_min_of_active_instances_after_shares(pg, postgres_url):
    w1, w2 = NOW + timedelta(seconds=100), NOW + timedelta(seconds=50)
    await pg.execute(
        "INSERT INTO instance_watermark VALUES ('ckpool02', $1, now() - interval '1 hour')", NOW - timedelta(days=1)
    )  # instance inactive: ignorée
    await ship(pg, 1, "ckpool01", watermark=w1)
    await pg.execute("INSERT INTO instance_watermark VALUES ('ckpool03', $1, now())", w2)
    hub = await make_hub(pg, postgres_url)
    ws = FakeWS()
    task = asyncio.create_task(serve(ws, hub, (1, 0), heartbeat_s=60))
    try:
        await ws.wait_for(lambda: "watermark" in ws.types())
        assert ws.types() == ["hello", "share", "watermark"]
        assert ws.events[-1]["watermark"] == w2.isoformat()
        await pg.execute("UPDATE instance_watermark SET shipped_until = $1 WHERE pool_instance = 'ckpool03'", w1 + timedelta(seconds=5))
        await ws.wait_for(lambda: ws.types().count("watermark") == 2)
        assert ws.events[-1]["watermark"] == w1.isoformat()
    finally:
        ws.disconnect()
        await task
        await hub.stop()


async def test_websocket_end_to_end(pg, postgres_url):
    """Vraie route FastAPI via uvicorn: replay, live, heartbeat et codes de fermeture."""
    await ship(pg, 3)
    hub = await make_hub(pg, postgres_url)
    set_container(Container(pool_service=None, share_hub=hub, redis=None))  # type: ignore[arg-type]
    app = FastAPI()
    app.include_router(share_stream.router)
    server = uvicorn.Server(uvicorn.Config(app, port=0, log_level="warning", lifespan="off"))
    serving = asyncio.create_task(server.serve())
    await until(lambda: server.started)
    port = server.servers[0].sockets[0].getsockname()[1]
    url = f"ws://127.0.0.1:{port}/v1/shares/stream"

    async def read(ws):
        return json.loads(await asyncio.wait_for(ws.recv(), 5))

    try:
        async with connect(f"{url}?from=1:1") as ws:
            assert (await read(ws))["type"] == "hello"
            assert [(await read(ws))["seq"] for _ in range(2)] == [2, 3]
            await ship(pg, 1)
            event = await read(ws)
            assert event["seq"] == 4 and "path" not in event and "raw" not in event
        async with connect(f"{url}?from=oups") as ws:
            with pytest.raises(Exception) as closed:
                await ws.recv()
            assert closed.value.rcvd.code == 4400
        await pg.execute("DELETE FROM share_log WHERE seq <= 2")
        async with connect(f"{url}?from=1:0") as ws:
            assert (await read(ws))["type"] == "hello"
            with pytest.raises(Exception) as closed:
                await ws.recv()
            assert closed.value.rcvd.code == 4410
    finally:
        server.should_exit = True
        await serving
        set_container(None)
        await hub.stop()
