import asyncio
import json
from datetime import datetime, timedelta, timezone

from pool_api.dao.ShareLog import Cursor, ShareLogRow

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_row(
    seq: int,
    epoch: int = 1,
    instance: str = "ckpool01",
    username: str | None = "bc1quser",
    workername: str | None = "rig1",
) -> ShareLogRow:
    return ShareLogRow(
        epoch=epoch,
        seq=seq,
        created_at=T0 + timedelta(seconds=seq),
        pool_instance=instance,
        path=f"0000abcd/{instance}.sharelog",
        byte_offset=seq * 100,
        workinfoid=7,
        username=username,
        workername=workername,
        diff=1.0,
        sdiff=2.5,
        result=True,
        parse_error=False,
    )


class FakeDAO:
    def __init__(self, rows: list[ShareLogRow] | None = None):
        self.rows = rows or []
        self.watermark_value: datetime | None = None
        self.calls: list[str] = []

    def add(self, *rows: ShareLogRow) -> None:
        self.rows.extend(rows)

    async def fetch_after(self, cursor: Cursor, limit: int) -> list[ShareLogRow]:
        self.calls.append("fetch")
        return sorted((r for r in self.rows if r.cursor > cursor), key=lambda r: r.cursor)[:limit]

    async def head(self) -> Cursor:
        return max((r.cursor for r in self.rows), default=(1, 0))

    async def watermark(self, active_within_s: float) -> datetime | None:
        self.calls.append("watermark")
        return self.watermark_value


class FakeWS:
    """Socket de test: enregistre les événements et peut bloquer l'envoi (client lent)."""

    def __init__(self):
        self.events: list[dict] = []
        self.closed: tuple[int, str | None] | None = None
        self.gate = asyncio.Event()
        self.gate.set()
        self.changed = asyncio.Condition()
        self._disconnect = asyncio.Event()

    async def send_text(self, data: str) -> None:
        await self.gate.wait()
        self.events.append(json.loads(data))
        async with self.changed:
            self.changed.notify_all()

    async def receive(self) -> dict:
        await self._disconnect.wait()
        return {"type": "websocket.disconnect"}

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        self.closed = (code, reason)
        async with self.changed:
            self.changed.notify_all()

    def disconnect(self) -> None:
        self._disconnect.set()

    async def wait_for(self, predicate, timeout: float = 5) -> None:
        async def loop():
            async with self.changed:
                await self.changed.wait_for(predicate)

        await asyncio.wait_for(loop(), timeout)

    def seqs(self) -> list[int]:
        return [e["seq"] for e in self.events if e["type"] == "share"]

    def types(self) -> list[str]:
        return [e["type"] for e in self.events]


class FakeConn:
    def __init__(self):
        self.closed = False
        self.listeners: list = []
        self.termination: list = []

    def add_termination_listener(self, callback):
        self.termination.append(callback)

    def kill(self):
        self.closed = True
        for callback in self.termination:
            callback(self)

    async def add_listener(self, channel, callback):
        self.listeners.append((channel, callback))

    def is_closed(self) -> bool:
        return self.closed

    async def execute(self, *_):
        if self.closed:
            raise ConnectionError

    async def close(self):
        self.closed = True

    def notify(self):
        for _, callback in self.listeners:
            callback(self, 0, "shares", "1:1")


class FakeConnector:
    """Remplace asyncpg.connect; `fail` > 0 fait échouer les n prochaines connexions."""

    def __init__(self):
        self.conns: list[FakeConn] = []
        self.fail = 0

    async def __call__(self, dsn, **_):
        if self.fail:
            self.fail -= 1
            raise OSError("connexion refusée")
        conn = FakeConn()
        self.conns.append(conn)
        return conn
