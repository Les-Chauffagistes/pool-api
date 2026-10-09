import json
from datetime import datetime

import asyncpg
from pydantic import BaseModel, field_validator


class UsersDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    class UsersDBModel(BaseModel):
        address: str
        hashrate1m: int
        hashrate5m: int
        hashrate1hr: int
        hashrate1d: int
        hashrate7d: int
        lastshare: datetime
        workers: int
        shares: int
        bestshare: int
        authorized: datetime

    class UsersAndWorkerDBModel(UsersDBModel):
        workers_details: list[dict]

        @field_validator("workers_details", mode="before")
        @classmethod
        def parse_workers_details(cls, v):
            # asyncpg renvoie les colonnes jsonb sous forme de str
            return json.loads(v) if isinstance(v, str) else v


    async def get_user_stat(self, address: str) -> UsersAndWorkerDBModel | None:
        row = await self.pg.fetchrow(
            """
            WITH user_summary AS (SELECT address,
                                         SUM(hashrate1m)  AS hashrate1m,
                                         SUM(hashrate5m)  AS hashrate5m,
                                         SUM(hashrate1hr) AS hashrate1hr,
                                         SUM(hashrate1d)  AS hashrate1d,
                                         SUM(hashrate7d)  AS hashrate7d,
                                         SUM(shares)      AS shares,
                                         MAX(bestshare)   AS bestshare,
                                         MIN(authorized)  AS authorized,
                                         SUM(workers)     AS workers,
                                         MAX(lastshare)   AS lastshare
                                  FROM users
                                  WHERE address = $1
                                  GROUP BY address),
                 worker_summary AS (SELECT address,
                                           jsonb_agg(
                                                   jsonb_build_object(
                                                           'workername', workername,
                                                           'hashrate1m', hashrate1m,
                                                           'hashrate5m', hashrate5m,
                                                           'hashrate1hr', hashrate1hr,
                                                           'hashrate1d', hashrate1d,
                                                           'hashrate7d', hashrate7d,
                                                           'shares', shares,
                                                           'bestshare', bestshare,
                                                           'lastshare', EXTRACT(EPOCH FROM lastshare)::bigint
                                                   ) ORDER BY lastshare DESC
                                           ) AS workers_details
                                    FROM (SELECT address,
                                                 workername,
                                                 SUM(hashrate1m)  AS hashrate1m,
                                                 SUM(hashrate5m)  AS hashrate5m,
                                                 SUM(hashrate1hr) AS hashrate1hr,
                                                 SUM(hashrate1d)  AS hashrate1d,
                                                 SUM(hashrate7d)  AS hashrate7d,
                                                 SUM(shares)      AS shares,
                                                 MAX(bestshare)   AS bestshare,
                                                 MAX(lastshare)   AS lastshare
                                          FROM user_workers
                                          WHERE
                                              address = $1
                                          GROUP BY address, workername) w
                                    GROUP BY address)
            SELECT u.address,
                   u.hashrate1m,
                   u.hashrate5m,
                   u.hashrate1hr,
                   u.hashrate1d,
                   u.hashrate7d,
                   u.shares,
                   u.bestshare,
                   u.authorized,
                   u.workers,
                   u.lastshare,
                   COALESCE(w.workers_details, '[]'::jsonb) AS workers_details
            FROM user_summary u
                     LEFT JOIN worker_summary w ON w.address = u.address;
            """,
            address,
        )
        if row is None:
            return None
        return self.__class__.UsersAndWorkerDBModel.model_validate(dict(row))