import logging
from pool_api.dao.Users import UsersDAO
import importlib
import pkgutil
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

import pool_api.apis
from pool_api.clients.prometheus import PrometheusClient
from pool_api.clients.redis import RedisClient
from pool_api.container import Container, set_container
from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.dao.ShareLog import ShareLogDAO
from pool_api.exceptions import NotFoundError
from pool_api.impl.v1.handlers import share_stream
from pool_api.impl.v1.services.data_cache_refresher import DataCacheRefresher
from pool_api.impl.v1.services.ping_monitor import PingMonitor
from pool_api.impl.v1.services.pool import PoolService
from pool_api.impl.v1.services.share_hub import ShareHub
from settings import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s:     %(name)s - %(message)s")


@asynccontextmanager
async def lifespan(_: FastAPI):
    pg = await asyncpg.create_pool(settings.database_url)
    prometheus_http = httpx.AsyncClient(base_url=settings.prometheus_url, timeout=5)
    redis = RedisClient(settings.redis_url)
    ping_monitor = PingMonitor("config.json", settings.ping_interval_s)
    await ping_monitor.start()
    share_hub = ShareHub(
        ShareLogDAO(pg),
        settings.database_url,
        poll_interval_s=settings.share_stream_poll_interval_s,
        buffer=settings.share_stream_buffer,
        batch=settings.share_stream_replay_batch,
        max_clients=settings.share_stream_max_clients,
        instance_stale_s=settings.share_stream_instance_stale_s,
    )
    await share_hub.start()
    pool_service = PoolService(
        PoolStatDAO(pg), UsersDAO(pg), PrometheusClient(prometheus_http), ping_monitor, redis
    )
    set_container(Container(pool_service=pool_service, share_hub=share_hub, redis=redis))
    data_refresher = DataCacheRefresher(pool_service, redis, settings.data_refresh_interval_s)
    await data_refresher.start()
    try:
        yield
    finally:
        set_container(None)
        await share_hub.stop()
        await data_refresher.stop()
        await ping_monitor.stop()
        await prometheus_http.aclose()
        await redis.close()
        await pg.close()


app = FastAPI(
    title="Les Chauffagistes mining pool API",
    description="API Les Chauffagistes pour accéder aux métriques de minage",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(NotFoundError)
async def not_found_handler(_: Request, exc: NotFoundError):
    return JSONResponse(status_code=404, content={"error": exc.message})


# Un router par tag est généré dans apis/<tag>_api.py : on les inclut tous, pour qu'un nouveau tag
# dans openapi.yaml soit pris en compte sans modifier ce fichier (ignoré par le générateur).
for _, name, _ in pkgutil.iter_modules(pool_api.apis.__path__, pool_api.apis.__name__ + "."):
    if not name.endswith("_base"):
        app.include_router(importlib.import_module(name).router)

# Le générateur ne sait pas déclarer de route WebSocket: branchée ici, documentée dans openapi.yaml (x-websocket-routes).
app.include_router(share_stream.router)
