import asyncpg


class HistoryDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def get_hashrate(self):
        return await self.pg.fetch(
            """
            SELECT day AS date, SUM(avg_hashrate1h) AS hashrate
            FROM worker_stats_1d
            GROUP BY day
            ORDER BY day
            """
        )