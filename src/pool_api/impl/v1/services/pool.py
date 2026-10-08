from chauff_cmn.models import PoolHashrates, PoolShares
from chauff_cmn.models import Pool, PoolRuntime

from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.utils import from_number_to_string


class PoolService:
    def __init__(self, pool_stat_dao: PoolStatDAO):
        self.pool_stat_dao = pool_stat_dao

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
