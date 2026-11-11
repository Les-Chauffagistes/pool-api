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

    class ClusterStatDBModel(BaseModel):
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

    async def get_cluster_stat(self):
        row = await self.pg.fetchrow(
            """
            select sum(hashrate_1m)  hashrate_1m,
                   sum(hashrate_5m)  hashrate_5m,
                   sum(hashrate_15m) hashrate_15m,
                   sum(hashrate_1h)  hashrate_1h,
                   sum(hashrate_6h)  hashrate_6h,
                   sum(hashrate_1d)  hashrate_1d,
                   sum(hashrate_7d)  hashrate_7d,
                   max(runtime_s)    runtime_s,
                   max(users)::bigint users,
                   sum(workers)::bigint workers,
                   sum(idle)::bigint idle,
                   sum(disconnected)::bigint disconnected,
                   max(diff)         diff,
                   sum(accepted)::bigint accepted,
                   sum(rejected)::bigint rejected,
                   max(bestshare)    bestshare,
                   sum(sps_1m)       sps_1m,
                   sum(sps_5m)       sps_5m,
                   sum(sps_15m)      sps_15m,
                   sum(sps_1h)       sps_1h,
                   max(updated_at)   updated_at
            from pool_stat
            """
        )
        return self.__class__.ClusterStatDBModel.model_validate(dict(row))