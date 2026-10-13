import asyncpg


class MonthlyBestsDAO:

    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def get_monthly_bests(self):
        # Colonnes renommées pour correspondre à PoolPrivateBestRecord (month, sdiff, address, epoch)
        return await self.pg.fetch(
            """
            SELECT DISTINCT ON (monthly_bests.month) to_char(month, 'YYYY-MM')             AS month,
                                       best_diff                             AS sdiff,
                                       "user"                                AS address,
                                       extract(epoch from updated_at)::bigint AS epoch
            FROM monthly_bests
            ORDER BY monthly_bests.month, best_diff DESC;
            """)
