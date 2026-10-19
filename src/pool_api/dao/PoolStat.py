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
        hashrate_1m: float
        hashrate_5m: float
        hashrate_15m: float
        hashrate_1h: float
        hashrate_6h: float
        hashrate_1d: float
        hashrate_7d: float
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
