from pool_api.dao.Users import UsersDAO
import importlib
import os
import pkgutil
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

import pool_api.apis
from pool_api.clients.prometheus import PrometheusClient
from pool_api.container import Container, set_container
from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.exceptions import NotFoundError
from pool_api.impl.v1.services.pool import PoolService


@asynccontextmanager
async def lifespan(_: FastAPI):
    pg = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    prometheus_http = httpx.AsyncClient(base_url=os.environ["PROMETHEUS_URL"], timeout=5)
    set_container(
        Container(pool_service=PoolService(PoolStatDAO(pg), UsersDAO(pg), PrometheusClient(prometheus_http)))
    )
    try:
        yield
    finally:
        set_container(None)
        await prometheus_http.aclose()
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
