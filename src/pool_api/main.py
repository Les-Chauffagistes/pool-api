import importlib
import os
import pkgutil
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

import pool_api.apis
from pool_api.container import Container, set_container
from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.impl.v1.services.pool import PoolService


@asynccontextmanager
async def lifespan(_: FastAPI):
    pg = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    set_container(Container(pool_service=PoolService(PoolStatDAO(pg))))
    try:
        yield
    finally:
        set_container(None)
        await pg.close()


app = FastAPI(
    title="Les Chauffagistes mining pool API",
    description="API Les Chauffagistes pour accéder aux métriques de minage",
    version="1.0.0",
    lifespan=lifespan,
)

# Un router par tag est généré dans apis/<tag>_api.py : on les inclut tous, pour qu'un nouveau tag
# dans openapi.yaml soit pris en compte sans modifier ce fichier (ignoré par le générateur).
for _, name, _ in pkgutil.iter_modules(pool_api.apis.__path__, pool_api.apis.__name__ + "."):
    if not name.endswith("_base"):
        app.include_router(importlib.import_module(name).router)
