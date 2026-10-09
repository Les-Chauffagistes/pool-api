from pool_api.models.pool_ping import PoolPing
from chauff_cmn.models import Pool

from pool_api.apis.pool_api_base import BasePoolApi
from pool_api.container import get_container
from pool_api.models.node import Node
from pool_api.models.pool_stats import PoolStats
from pool_api.models.pool_top import PoolTop


class PoolApi(BasePoolApi):
    async def get_pool(self) -> Pool:
        return await get_container().pool_service.get_primary_pool_stat()

    async def get_node(
        self,
    ) -> Node:
        return await get_container().pool_service.get_primary_node_stat()

    async def get_user_stats(
        self,
        user: str,
    ) -> PoolStats:
        return await get_container().pool_service.get_user_stats(user)

    async def get_top(
        self,
    ) -> PoolTop:
        return await get_container().pool_service.get_top()

    async def get_pings(
        self,
    ) -> PoolPing:
        return await get_container().pool_service.get_pings()