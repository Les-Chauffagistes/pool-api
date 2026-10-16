from dataclasses import dataclass
from datetime import datetime

import asyncpg

Cursor = tuple[int, int]  # (epoch, seq), ordre lexicographique


@dataclass(frozen=True, slots=True)
class ShareLogRow:
    epoch: int
    seq: int
    created_at: datetime
    pool_instance: str
    path: str
    byte_offset: int
    workinfoid: int | None
    username: str | None
    workername: str | None
    diff: float | None
    sdiff: float | None
    result: bool | None
    parse_error: bool

    @property
    def cursor(self) -> Cursor:
        return self.epoch, self.seq


class ShareLogDAO:
    """Lecture seule de share_log / instance_watermark (alimentés par le share_shipper de ckpool-share-exporter)."""

    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def fetch_after(self, cursor: Cursor, limit: int) -> list[ShareLogRow]:
        # raw n'est volontairement pas lu: il n'est jamais exposé
        rows = await self.pg.fetch(
            """
            SELECT epoch, seq, created_at, pool_instance, path, byte_offset, workinfoid, username,
                   workername, diff, sdiff, result, parse_error
            FROM share_log
            WHERE (epoch, seq) > ($1, $2)
            ORDER BY epoch, seq
            LIMIT $3
            """,
            *cursor,
            limit,
        )
        return [ShareLogRow(**dict(r)) for r in rows]

    async def head(self) -> Cursor:
        """Dernier (epoch, seq) attribué, d'après share_seq."""
        row = await self.pg.fetchrow("SELECT epoch, v FROM share_seq WHERE id = 1")
        return row["epoch"], row["v"]

    async def watermark(self, active_within_s: float) -> datetime | None:
        """Plus petit shipped_until des instances ayant émis un heartbeat récemment; None s'il n'y en a aucune."""
        return await self.pg.fetchval(
            """
            SELECT min(shipped_until)
            FROM instance_watermark
            WHERE heartbeat_at > now() - make_interval(secs => $1)
            """,
            active_within_s,
        )
