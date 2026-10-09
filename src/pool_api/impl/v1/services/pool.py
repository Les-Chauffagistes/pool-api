from chauff_cmn.models import PoolHashrates, PoolShares
from chauff_cmn.models import Pool, PoolRuntime

from pool_api.clients.prometheus import PrometheusClient
from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.dao.Users import UsersDAO
from pool_api.exceptions import NotFoundError
from pool_api.models.node import Node
from pool_api.models.pool_stats import PoolStats
from pool_api.models.pool_stats_global_stats import PoolStatsGlobalStats
from pool_api.models.worker import Worker
from pool_api.utils import format_bitcoin_subversion, from_number_to_string

HASHRATE_KEYS = ("hashrate1m", "hashrate5m", "hashrate1hr", "hashrate1d", "hashrate7d")


class PoolService:
    def __init__(self, pool_stat_dao: PoolStatDAO, users_dao: UsersDAO, prometheus: PrometheusClient):
        self.pool_stat_dao = pool_stat_dao
        self.users_dao = users_dao
        self.prometheus = prometheus

    async def get_primary_pool_stat(self) -> Pool:
        data = await self.pool_stat_dao.get_primary_pool_stat()
        runtime = PoolRuntime(
            runtime=data.runtime_s,
            lastupdate=data.updated_at.timestamp(),
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
            hashrate=hashrate,
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