import asyncio
import logging

from fastapi import HTTPException

from pool_api.apis.health_api_base import BaseHealthApi
from pool_api.container import get_container

logger = logging.getLogger(__name__)

DB_CHECK_TIMEOUT_S = 3


class HealthApi(BaseHealthApi):
    async def get_health(self) -> None:
        container = get_container()
        for name, pool in (("pg", container.pg), ("pg_history", container.pg_history)):
            if pool is None:
                continue
            try:
                await asyncio.wait_for(pool.fetchval("SELECT 1"), DB_CHECK_TIMEOUT_S)
            except Exception:
                logger.exception("Health check : base %s injoignable", name)
                raise HTTPException(status_code=503, detail=f"Database {name} unavailable")

    async def head_health(self) -> None:
        return None
