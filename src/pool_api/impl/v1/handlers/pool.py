from chauff_cmn.models import Pool

from pool_api.apis.pool_api_base import BasePoolApi
from pool_api.container import get_container


class PoolApi(BasePoolApi):
    async def get_pool(self) -> Pool:
        return await get_container().pool_service.get_primary_pool_stat()
