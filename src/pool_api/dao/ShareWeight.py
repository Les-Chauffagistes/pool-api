import asyncpg

from src.settings import settings


class ShareWeightDAO:
    def __init__(self, pg: asyncpg.Pool):
        self.pg = pg

    async def distribution(
            self, username: str, pool_instance: str, window_days: int | None = None
    ) -> list[asyncpg.Record]:
        """Repartition du travail entre les workers d'une adresse de paiement.

        Lit l'agregat horaire, qui est en temps reel: la part reflete les shares
        ingerees a la seconde pres, sans attendre le rafraichissement.
        """
        if window_days is None:
            window_days = settings.distribution_window_days
        return await self.pg.fetch(
            """SELECT workername,
                      SUM(diff_sum)                              AS diff_sum,
                      SUM(diff_sum) / SUM(SUM(diff_sum)) OVER () AS part,
                      SUM(shares_ok)                             AS shares_ok,
                      SUM(shares_ko)                             AS shares_ko
               FROM share_weights_hourly
               WHERE username = $1
                 AND pool_instance = $2
                 AND hour >= NOW() - MAKE_INTERVAL(days => $3)
               GROUP BY workername
               ORDER BY part DESC""",
            username, pool_instance, window_days)

    async def cluster_distribution(
            self,
            username: str,
            window_days: int = 14,
    ) -> list[asyncpg.Record]:
        """Repartition d'une adresse sur toutes les instances du cluster."""
        return await self.pg.fetch(
            """SELECT workername,
                      SUM(diff_sum)                              AS diff_sum,
                      SUM(diff_sum) / SUM(SUM(diff_sum)) OVER () AS part,
                      SUM(shares_ok)                             AS shares_ok,
                      SUM(shares_ko)                             AS shares_ko
               FROM share_weights_hourly
               WHERE username = $1
                 AND hour >= NOW() - MAKE_INTERVAL(days => $2)
               GROUP BY workername
               ORDER BY part DESC""",
            username, window_days)
