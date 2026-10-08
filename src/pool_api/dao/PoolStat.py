from datetime import datetime

import asyncpg
from pydantic import BaseModel


class PoolStatDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    class PoolStatDBModel(BaseModel):
        pool_instance: str
        updated_at: datetime
        runtime_s: int
        users: int
        workers: int
        idle: int
        disconnected: int
        hashrate_1m: int
        hashrate_5m: int
        hashrate_15m: int
        hashrate_1h: int
        hashrate_6h: int
        hashrate_1d: int
        hashrate_7d: int
        diff: float
        accepted: int
        rejected: int
        bestshare: int
        sps_1m: float
        sps_5m: float
        sps_15m: float
        sps_1h: float

    async def get_primary_pool_stat(self):
        row = await self.pg.fetchrow(
            """
            SELECT *
            FROM pool_stat
            WHERE pool_instance = 'ckpool01'
            """
        )
        return self.__class__.PoolStatDBModel.model_validate(dict(row))
