import json
from datetime import datetime

import asyncpg
from pydantic import BaseModel, field_validator
from pool_api.models.pool_top import PoolTop


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
                                          WHERE address = $1
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

    async def get_top(self) -> PoolTop:
        row = await self.pg.fetch(
            """
            WITH user_stats AS (SELECT left(address, 5) || '…' || right(address, 2) AS address,
                                       sum(workers)                                   AS workers,
                                       sum(hashrate1hr)                               AS hashrate,
                                       max(bestshare)                                 AS bestshare
                                FROM users
                                GROUP BY address)
            SELECT json_build_object(
                           'topHashrate', (SELECT COALESCE(json_agg(t), '[]'::json)
                                           FROM (SELECT address, workers AS "workerCount", hashrate AS "totalHashrate1hr"
                                                 FROM user_stats
                                                 ORDER BY hashrate DESC
                                                 LIMIT 10) t),
                           'topBestShares', (SELECT COALESCE(json_agg(t), '[]'::json)
                                             FROM (SELECT address, workers AS "workerCount", bestshare
                                                   FROM user_stats
                                                   ORDER BY bestshare DESC
                                                   LIMIT 10) t)
                   ) AS result;
            """
        )
        if not row:
            return PoolTop(top_best_shares=[], top_hashrate=[])
        result = row[0]["result"]
        if isinstance(result, str):
            result = json.loads(result)
        return PoolTop.model_validate(result)

    async def get_data(self) -> dict:
        row = await self.pg.fetchrow(
            """
            -- 1. Statistiques globales de la pool (agrégées sur toutes les instances)
            WITH pool_data AS (
                SELECT jsonb_build_object(
                    'runtime', jsonb_build_object(
                        'runtime', max(ps.runtime_s),
                        'lastupdate', extract(epoch from max(ps.updated_at))::bigint,
                        'Users', sum(ps.users),
                        'Workers', sum(ps.workers),
                        'Idle', sum(ps.idle),
                        'Disconnected', sum(ps.disconnected)
                    ),
                    'hashrates', jsonb_build_object(
                        'hashrate1m', sum(ps.hashrate_1m),
                        'hashrate5m', sum(ps.hashrate_5m),
                        'hashrate15m', sum(ps.hashrate_15m),
                        'hashrate1hr', sum(ps.hashrate_1h),
                        'hashrate6hr', sum(ps.hashrate_6h),
                        'hashrate1d', sum(ps.hashrate_1d),
                        'hashrate7d', sum(ps.hashrate_7d)
                    ),
                    'shares', jsonb_build_object(
                        'diff', max(ps.diff),
                        'accepted', sum(ps.accepted),
                        'rejected', sum(ps.rejected),
                        'bestshare', max(ps.bestshare),
                        'SPS1m', sum(ps.sps_1m),
                        'SPS5m', sum(ps.sps_5m),
                        'SPS15m', sum(ps.sps_15m),
                        'SPS1h', sum(ps.sps_1h)
                    )
                ) AS pool_json FROM pool_stat ps),
            -- 2. Agrégation des workers par (address, workername) sur toutes les instances
            aggregated_workers AS (
                SELECT w.address,
                    w.workername,
                    sum(w.hashrate1m)  AS hashrate1m,
                    sum(w.hashrate5m)  AS hashrate5m,
                    sum(w.hashrate1hr) AS hashrate1hr,
                    sum(w.hashrate1d)  AS hashrate1d,
                    sum(w.hashrate7d)  AS hashrate7d,
                    max(w.lastshare)   AS lastshare,
                    sum(w.shares)      AS shares,
                    max(w.bestshare)   AS bestshare
                FROM user_workers w
                GROUP BY w.address, w.workername
            ),

            -- 3. Liste des workers agrégés par utilisateur
            user_workers_agg AS (
                SELECT aw.address,
                    COALESCE(
                        jsonb_agg(
                            jsonb_build_object(
                                 'workername',  aw.workername,
                                 'hashrate1m',  aw.hashrate1m,
                                 'hashrate5m',  aw.hashrate5m,
                                 'hashrate1hr', aw.hashrate1hr,
                                 'hashrate1d',  aw.hashrate1d,
                                 'hashrate7d',  aw.hashrate7d,
                                 'lastshare',
                                 extract(epoch from aw.lastshare)::bigint,
                                 'shares', aw.shares,
                                 'bestshare', aw.bestshare,
                                 'bestever', aw.bestshare
                            ) ORDER BY aw.workername
                        ) FILTER (WHERE aw.workername IS NOT NULL),
                        '[]'::jsonb
                    ) AS workers_list
                    FROM aggregated_workers aw
                    GROUP BY aw.address),

                -- 4. Agrégation des utilisateurs sur toutes les instances
                aggregated_users AS (
                    SELECT u.address,
                         sum(u.hashrate1m)  AS hashrate1m,
                         sum(u.hashrate5m)  AS hashrate5m,
                         sum(u.hashrate1hr) AS hashrate1hr,
                         sum(u.hashrate1d)  AS hashrate1d,
                         sum(u.hashrate7d)  AS hashrate7d,
                         max(u.lastshare)   AS lastshare,
                         sum(u.workers)     AS workers,
                         sum(u.shares)      AS shares,
                         max(u.bestshare)   AS bestshare,
                         min(u.authorized)  AS authorised
                    FROM users u
                    GROUP BY u.address
                ),

                -- 5. Construction de l'objet dictionnaire pour les users
                users_data AS (
                    SELECT COALESCE(
                        jsonb_object_agg(
                            au.address,
                            jsonb_build_object(
                                'hashrate1m', au.hashrate1m,
                                'hashrate5m', au.hashrate5m,
                                'hashrate1hr', au.hashrate1hr,
                                'hashrate1d', au.hashrate1d,
                                'hashrate7d', au.hashrate7d,
                                'lastshare', extract(epoch from au.lastshare)::bigint,
                                'workers', au.workers,
                                'shares', au.shares,
                                'bestshare', au.bestshare,
                                'bestever', au.bestshare,
                                'authorised', extract(epoch from au.authorised)::bigint,
                                'worker', COALESCE(uwa.workers_list, '[]'::jsonb)
                            )
                        ),
                       '{}'::jsonb
                    ) AS users_json
                    FROM aggregated_users au
                    LEFT JOIN user_workers_agg uwa ON au.address = uwa.address
                 ),

                -- 6. Calcul de la répartition globale sur tous les workers
                repartition_calc AS (
                    SELECT sw.workername,
                        sum(sw.shares)                                              AS shares,
                        sum(sw.diff_sum)                                            AS total_vardiff,
                        sum(sw.diff_sum)                                            AS weight,
                        sum(sw.diff_sum) / nullif(sum(sum(sw.diff_sum)) over (), 0) AS percentage
                    FROM share_weights_hourly sw
                    GROUP BY sw.workername
                ),
                repartition_data AS (
                    SELECT COALESCE(
                        jsonb_object_agg(
                            workername,
                            jsonb_build_object(
                                'shares', shares,
                                'total_vardiff', total_vardiff,
                                'weight', weight,
                                'percentage', percentage
                            )
                         ),
                         '{}'::jsonb
                    ) AS repartition_json
                    FROM repartition_calc),

                 -- 7. Liste des monthly bests
                monthly_bests_data AS (
                    SELECT COALESCE(
                        jsonb_agg(
                            jsonb_build_object(
                                'month', to_char(month, 'YYYY-MM'),
                                'sdiff', best_diff,
                                'username', "user",
                                'workername', workername,
                                'epoch', extract(epoch from updated_at)
                            ) ORDER BY month DESC, best_diff DESC
                        ),
                        '[]'::jsonb
                    ) AS monthly_bests_json
                    FROM monthly_bests
                )

                -- 8. Assemblage final
                SELECT jsonb_build_object(
                    'pool', p.pool_json,
                    'users', u.users_json,
                    'repartition', r.repartition_json,
                    'monthly_bests', m.monthly_bests_json
                ) AS result
                FROM pool_data p
                CROSS JOIN users_data u
                CROSS JOIN repartition_data r
                CROSS JOIN monthly_bests_data m;
            """
        )
        result = row["result"]
        return json.loads(result) if isinstance(result, str) else result
