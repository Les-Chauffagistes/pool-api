from pool_api.dao.History import HistoryDAO
from pool_api.models.pool_hashrate_record import PoolHashrateRecord
from pool_api.models.pool_private_best_record import PoolPrivateBestRecord
import logging
import time

from pool_api.clients.prometheus import PrometheusClient
from pool_api.clients.redis import RedisClient
from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.dao.Users import UsersDAO
from pool_api.dao.MonthlyBests import MonthlyBestsDAO
from pool_api.exceptions import NotFoundError
from pool_api.models.node import Node
from pool_api.models.pool import Pool
from pool_api.models.pool_api_data_payload import PoolApiDataPayload
from pool_api.models.pool_hashrates import PoolHashrates
from pool_api.models.pool_runtime import PoolRuntime
from pool_api.models.pool_shares import PoolShares
from pool_api.models.pool_stats import PoolStats
from pool_api.models.pool_stats_global_stats import PoolStatsGlobalStats
from pool_api.impl.v1.services.ping_monitor import PingMonitor
from pool_api.models.pool_ping import PoolPing
from pool_api.models.worker import Worker
from pool_api.utils import format_bitcoin_subversion, from_number_to_string

HASHRATE_KEYS = ("hashrate1m", "hashrate5m", "hashrate1hr", "hashrate1d", "hashrate7d")

DATA_CACHE_KEY = "pool_api:data"
DATA_CACHE_TTL_S = 60

logger = logging.getLogger(__name__)


class PoolService:
    def __init__(self, pool_stat_dao: PoolStatDAO, users_dao: UsersDAO, monthly_bests_dao: MonthlyBestsDAO,
                 history_dao: HistoryDAO, prometheus: PrometheusClient, ping_monitor: PingMonitor, redis: RedisClient):
        self.pool_stat_dao = pool_stat_dao
        self.users_dao = users_dao
        self.monthly_bests_dao = monthly_bests_dao
        self.history_dao = history_dao
        self.prometheus = prometheus
        self.ping_monitor = ping_monitor
        self.redis = redis

    async def get_primary_pool_stat(self) -> Pool:
        data = await self.pool_stat_dao.get_primary_pool_stat()
        runtime = PoolRuntime(
            runtime=data.runtime_s,
            lastupdate=int(data.updated_at.timestamp()),
            Users=data.users,
            Workers=data.workers,
            Idle=data.idle,
            Disconnected=data.disconnected
        )

        hashrate = PoolHashrates(
            hashrate1m=from_number_to_string(data.hashrate_1m),
            hashrate5m=from_number_to_string(data.hashrate_5m),
            hashrate15m=from_number_to_string(data.hashrate_15m),
            hashrate1hr=from_number_to_string(data.hashrate_1h),
            hashrate1d=from_number_to_string(data.hashrate_1d),
            hashrate6hr=from_number_to_string(data.hashrate_6h),
            hashrate7d=from_number_to_string(data.hashrate_7d),
        )

        shares = PoolShares(
            diff=data.diff,
            accepted=data.accepted,
            rejected=data.rejected,
            bestshare=data.bestshare,
            SPS1m=data.sps_1m,
            SPS5m=data.sps_5m,
            SPS15m=data.sps_15m,
            SPS1h=data.sps_1h,
        )

        return Pool(
            runtime=runtime,
            hashrates=hashrate,
            shares=shares,
        )

    async def get_primary_node_stat(self) -> Node:
        height = await self.prometheus.query_scalar("bitcoin_blocks")
        version = await self.prometheus.query_scalar("bitcoin_server_version")
        peers = await self.prometheus.query_scalar("bitcoin_peers")
        return Node(
            height=int(height),
            subversion=format_bitcoin_subversion(int(version)),
            peers=int(peers),
        )

    async def get_user_stats(self, user: str) -> PoolStats:
        data = await self.users_dao.get_user_stat(user)
        if data is None:
            raise NotFoundError("Adresse introuvable")
        global_stats = PoolStatsGlobalStats(
            hashrate1m=from_number_to_string(data.hashrate1m),
            hashrate5m=from_number_to_string(data.hashrate5m),
            hashrate1hr=from_number_to_string(data.hashrate1hr),
            hashrate1d=from_number_to_string(data.hashrate1d),
            hashrate7d=from_number_to_string(data.hashrate7d),
            shares=data.shares,
            bestshare=data.bestshare,
            workers=data.workers,
        )
        return PoolStats(
            address=data.address,
            globalStats=global_stats,
            workers=[self._to_worker(w) for w in data.workers_details],
        )

    @staticmethod
    def _to_worker(w: dict) -> Worker:
        return Worker(
            **{
                **w,
                **{k: from_number_to_string(w[k]) for k in HASHRATE_KEYS},
                # bestever n'est pas enregistré en base (valeur identique à bestshare)
                "bestever": w["bestshare"],
            }
        )

    async def get_top(self):
        return await self.users_dao.get_top()

    async def get_pings(self) -> PoolPing:
        return self.ping_monitor.latest or await self.ping_monitor.refresh()

    @staticmethod
    def _format_data_hashrates(raw: dict) -> dict:
        """Le SQL renvoie des hashrates numériques, le contrat de l'API les attend lisibles ("1.5 M")."""

        def fmt(d: dict, keys) -> dict:
            return {**d, **{k: from_number_to_string(d[k]) for k in keys if k in d}}

        users = {
            address: {**fmt(u, HASHRATE_KEYS), "worker": [fmt(w, HASHRATE_KEYS) for w in u["worker"]]}
            for address, u in raw["users"].items()
        }
        pool_keys = (*HASHRATE_KEYS, "hashrate15m", "hashrate6hr")
        return {**raw, "users": users, "pool": {**raw["pool"], "hashrates": fmt(raw["pool"]["hashrates"], pool_keys)}}

    async def get_data(self) -> PoolApiDataPayload:
        # Redis first : la requête SQL est lourde, on sert le cache tant que le TTL n'a pas expiré.
        # Une panne Redis ne doit pas casser la route : on retombe alors sur la base.
        start = time.perf_counter()
        try:
            cached = await self.redis.raw.get(DATA_CACHE_KEY)
            if cached is not None:
                payload = PoolApiDataPayload.from_json(cached)
                logger.info("get_data: cache HIT (%.0f ms)", (time.perf_counter() - start) * 1000)
                return payload
        except Exception:
            logger.warning("Lecture du cache Redis impossible, repli sur la base", exc_info=True)

        logger.info("get_data: cache MISS, lecture en base")
        data = await self.refresh_data()
        logger.info("get_data: cache MISS terminé (%.0f ms)", (time.perf_counter() - start) * 1000)
        return data

    async def refresh_data(self) -> PoolApiDataPayload:
        """Reconstruit le payload depuis la base et Prometheus, puis le met en cache (appelé aussi par le worker)."""
        raw = self._format_data_hashrates(await self.users_dao.get_data())
        data = PoolApiDataPayload.from_dict({
            **raw,
            "backup_pool": False,  # codé en dur pour le moment
            "node": (await self.get_primary_node_stat()).to_dict(),
        })

        try:
            await self.redis.raw.set(DATA_CACHE_KEY, data.to_json(), ex=DATA_CACHE_TTL_S)
        except Exception:
            logger.warning("Écriture du cache Redis impossible", exc_info=True)
        return data

    async def get_monthly_bests(self) -> list[PoolPrivateBestRecord]:
        rows = await self.monthly_bests_dao.get_monthly_bests()
        return [PoolPrivateBestRecord(**row) for row in rows]

    async def get_hashrate(self) -> list[PoolHashrateRecord]:
        return [PoolHashrateRecord(
            date = row["date"].isoformat(),
            hashrate = int(row["hashrate"])
        ) for row in await self.history_dao.get_hashrate()]