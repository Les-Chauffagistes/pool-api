from chauff_cmn.models import PoolHashrates, PoolShares
from chauff_cmn.models import Pool, PoolRuntime

from pool_api.clients.prometheus import PrometheusClient
from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.models.node import Node
from pool_api.utils import format_bitcoin_subversion, from_number_to_string


class PoolService:
    def __init__(self, pool_stat_dao: PoolStatDAO, prometheus: PrometheusClient):
        self.pool_stat_dao = pool_stat_dao
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