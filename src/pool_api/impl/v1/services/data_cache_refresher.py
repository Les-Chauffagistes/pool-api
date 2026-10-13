import asyncio
import logging
import time
from contextlib import suppress

from pool_api.clients.redis import RedisClient
from pool_api.impl.v1.services.pool import PoolService

logger = logging.getLogger(__name__)

REFRESH_LOCK_KEY = "pool_api:data:refresh_lock"


class DataCacheRefresher:
    """Rafraîchit périodiquement le cache de /v1/data pour que les requêtes ne paient jamais un miss.

    Un verrou Redis (non relâché, il expire juste avant le prochain tick) limite le rafraîchissement
    à un seul processus par intervalle : utile si plusieurs instances tournent en même temps
    (ex. rolling update). Le chemin à la demande de PoolService.get_data reste le filet de sécurité.
    """

    def __init__(self, pool_service: PoolService, redis: RedisClient, interval_s: float):
        self.pool_service = pool_service
        self.redis = redis
        self.interval_s = interval_s
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="data-cache-refresher")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def refresh(self) -> bool:
        """Rafraîchit le cache si on obtient le verrou ; renvoie False si une autre instance s'en charge."""
        lock_ms = max(int(self.interval_s * 0.8 * 1000), 1)
        if not await self.redis.raw.set(REFRESH_LOCK_KEY, "1", nx=True, px=lock_ms):
            logger.debug("Rafraîchissement du cache ignoré : verrou détenu par une autre instance")
            return False
        start = time.perf_counter()
        await self.pool_service.refresh_data()
        logger.info("Cache /v1/data rafraîchi (%.0f ms)", (time.perf_counter() - start) * 1000)
        return True

    async def _run(self) -> None:
        while True:
            try:
                await self.refresh()
            except Exception:  # une erreur ponctuelle ne doit pas tuer la boucle
                logger.exception("Échec du rafraîchissement du cache /v1/data")
            await asyncio.sleep(self.interval_s)
