import asyncio
import json
import logging
from contextlib import suppress
from datetime import datetime
from typing import Awaitable, Callable

import asyncpg

from pool_api.dao.ShareLog import Cursor, ShareLogDAO, ShareLogRow

logger = logging.getLogger(__name__)

CHANNEL = "shares"
LISTEN_APPLICATION_NAME = "pool-api-share-listen"


class TooManyClients(Exception):
    pass


def format_cursor(cursor: Cursor) -> str:
    return f"{cursor[0]}:{cursor[1]}"


def parse_cursor(value: str) -> Cursor:
    epoch, seq = value.split(":")  # ValueError si le format est mauvais
    cursor = int(epoch), int(seq)
    if min(cursor) < 0:
        raise ValueError(value)
    return cursor


def block_height(path: str) -> int | None:
    """Hauteur du bloc: ckpool range les sharelogs dans `<hauteur en hex sur 8 chiffres>/<workinfoid>.sharelog`."""
    try:
        return int(path.split("/", 1)[0], 16)
    except ValueError:
        return None


def share_event(row: ShareLogRow) -> str:
    # path et raw ne sortent pas: le path n'est utile qu'au share_id, raw n'existe que pour parse_error
    return json.dumps(
        {
            "type": "share",
            "share_id": f"{row.pool_instance}:{row.path}:{row.byte_offset}",
            "epoch": row.epoch,
            "seq": row.seq,
            "instance": row.pool_instance,
            "created_at": row.created_at.isoformat(),
            "workinfoid": row.workinfoid,
            "block_height": block_height(row.path),
            "username": row.username,
            "workername": row.workername,
            "diff": row.diff,
            "sdiff": row.sdiff,
            "result": row.result,
            "parse_error": row.parse_error,
        }
    )


def watermark_event(watermark: datetime) -> str:
    return json.dumps({"type": "watermark", "watermark": watermark.isoformat()})


class ShareFilter:
    """Filtre un flux de shares par address (username, sensible à la casse: base58) et/ou workername (insensible).

    Un critère à None ne contraint rien; avec les deux, les deux doivent correspondre. Un share
    sans username/workername (parse_error) ne correspond à aucun critère renseigné.
    """

    def __init__(self, address: str | None = None, workername: str | None = None):
        self.address = address
        self.workername = workername.casefold() if workername is not None else None

    def matches(self, row: ShareLogRow) -> bool:
        if self.address is not None and (row.username is None or row.username != self.address):
            return False
        if self.workername is not None and (row.workername is None or row.workername.casefold() != self.workername):
            return False
        return True


class Subscriber:
    """File bornée d'un client: (curseur, texte JSON); curseur None pour les événements sans seq."""

    def __init__(self, buffer: int, share_filter: ShareFilter | None = None):
        self.filter = share_filter or ShareFilter()
        self.queue: asyncio.Queue[tuple[Cursor | None, str]] = asyncio.Queue(buffer)
        self.live = False  # tant qu'il rejoue depuis la base, le hub ne lui pousse rien
        # Dernier curseur parcouru par le hub pour ce client, shares écartés par le filtre compris. Avec une file
        # vide, tout share <= scanned qui correspond au filtre a déjà été envoyé: le heartbeat peut l'annoncer.
        self.scanned: Cursor | None = None
        self.overflow = False


class ShareHub:
    """Une seule boucle de lecture de share_log par processus, diffusée à tous les clients.

    LISTEN 'shares' réveille la boucle; un polling la réveille aussi si la connexion LISTEN est
    morte ou muette. Chaque tour lit tout ce qui suit `position`: reconnecter LISTEN rattrape donc
    exactement depuis le dernier seq diffusé. Le seq est gapless et suit l'ordre de commit, donc
    "tout ce qui suit position" est complet.
    """

    def __init__(
        self,
        dao: ShareLogDAO,
        dsn: str,
        *,
        poll_interval_s: float = 0.5,
        buffer: int = 1000,
        batch: int = 1000,
        max_clients: int = 200,
        instance_stale_s: float = 60,
        keepalive_s: float = 10,
        reconnect_delay_s: float = 1,
        connect: Callable[..., Awaitable[asyncpg.Connection]] = asyncpg.connect,
    ):
        self.dao = dao
        self.dsn = dsn
        self.poll_interval_s = poll_interval_s
        self.buffer = buffer
        self.batch = batch
        self.max_clients = max_clients
        self.instance_stale_s = instance_stale_s
        self.keepalive_s = keepalive_s
        self.reconnect_delay_s = reconnect_delay_s
        self._connect = connect
        self.position: Cursor = (0, 0)
        self.watermark: datetime | None = None  # dernier émis, jamais en recul
        self.listen_sessions = 0
        self._subs: set[Subscriber] = set()
        self._wake = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        self.position = await self.dao.head()
        self._tasks = [
            asyncio.create_task(self._poll_loop(), name="share-hub-poll"),
            asyncio.create_task(self._listen_loop(), name="share-hub-listen"),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    def subscribe(self, share_filter: ShareFilter | None = None) -> Subscriber:
        if len(self._subs) >= self.max_clients:
            raise TooManyClients
        sub = Subscriber(self.buffer, share_filter)
        self._subs.add(sub)
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        self._subs.discard(sub)

    def go_live(self, sub: Subscriber) -> tuple[Cursor, datetime | None]:
        """Le hub pousse désormais à `sub` tout ce qui suit la position renvoyée, avec le watermark valable à ce point."""
        sub.live = True
        sub.scanned = self.position
        return self.position, self.watermark

    def _broadcast(self, cursor: Cursor | None, text: str, row: ShareLogRow | None = None) -> None:
        """`row` est le share porté par `text`; None pour les événements qui vont à tous (watermark)."""
        for sub in self._subs:
            if not sub.live:
                continue
            if cursor is not None:
                sub.scanned = cursor
            if row is not None and not sub.filter.matches(row):
                continue  # filtré ici: ni file, ni envoi, et le buffer du client n'est pas consommé
            try:
                sub.queue.put_nowait((cursor, text))
            except asyncio.QueueFull:
                # pas de drop silencieux: le client est fermé (4408) avant son prochain envoi
                sub.live = False
                sub.overflow = True

    async def pump(self) -> None:
        # Le watermark est lu AVANT les shares: tout share <= watermark est alors dans les lignes lues ci-dessous.
        watermark = await self.dao.watermark(self.instance_stale_s)
        while True:
            rows = await self.dao.fetch_after(self.position, self.batch)
            for row in rows:
                self._broadcast(row.cursor, share_event(row), row)
                self.position = row.cursor
            if len(rows) < self.batch:
                break
        if watermark is not None and (self.watermark is None or watermark > self.watermark):
            self.watermark = watermark
            self._broadcast(None, watermark_event(watermark))

    def _on_notify(self, *_) -> None:
        self._wake.set()

    async def _poll_loop(self) -> None:
        # Indépendante de LISTEN: si la connexion LISTEN est morte ou ne se rétablit pas, le polling livre quand même.
        while True:
            self._wake.clear()
            try:
                await self.pump()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Lecture de share_log en échec")
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._wake.wait(), self.poll_interval_s)

    async def _listen_loop(self) -> None:
        while True:
            try:
                await self._listen_session()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Connexion LISTEN %s perdue, reconnexion", CHANNEL)
            await asyncio.sleep(self.reconnect_delay_s)

    async def _listen_session(self) -> None:
        conn = await self._connect(self.dsn, server_settings={"application_name": LISTEN_APPLICATION_NAME})
        try:
            await conn.add_listener(CHANNEL, self._on_notify)
            self.listen_sessions += 1
            # Rattrapage immédiat de ce qui a été commité pendant la coupure: pump relit tout depuis `position`.
            self._wake.set()
            terminated = asyncio.Event()
            conn.add_termination_listener(lambda _: terminated.set())
            while not terminated.is_set() and not conn.is_closed():
                # Du trafic régulier évite aussi que HAProxy coupe la connexion inactive.
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(terminated.wait(), self.keepalive_s)
                    break
                await asyncio.wait_for(conn.execute("SELECT 1"), 5)
            raise ConnectionError("connexion LISTEN fermée")
        finally:
            with suppress(Exception):
                await asyncio.wait_for(conn.close(), 5)
