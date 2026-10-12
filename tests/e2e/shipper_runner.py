"""Sous-processus: le VRAI `ship_shares` de l'exporteur, rappelé toutes les 0,2 s comme `run_forever` dans son
__main__ (qu'on n'importe pas: il tire aiohttp et démarre tous les autres workers).

Configuration par variables d'env (DB_*, BASE_LOG_DIR, POOL_INSTANCE_NAME, ...), lues par Settings() de l'exporteur.
Le port Postgres passe par PGPORT, comme l'exporteur n'a pas de DB_PORT.
"""

import asyncio
import sys
import traceback

import asyncpg
from ckpool_share_exporter.settings import settings
from ckpool_share_exporter.workers.share_shipper import ship_shares


async def main() -> None:
    async with asyncpg.create_pool(
        user=settings.db_user, password=settings.db_password, database=settings.db_name, host=settings.db_host
    ) as pool:
        while True:
            try:
                await ship_shares(pool)
                await asyncio.sleep(0.2)
            except Exception:
                traceback.print_exc(file=sys.stderr)
                await asyncio.sleep(1)


asyncio.run(main())
