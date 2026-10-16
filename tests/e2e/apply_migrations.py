"""Sous-processus: applique les VRAIES migrations de l'exporteur (000 à 014) sur le DSN donné.

Lancé avec l'interpréteur/PYTHONPATH de l'exporteur: on réutilise `migration_files` et `split_statements` de son
conftest d'intégration (le découpage SQL est subtil: blocs $$ et CREATE MATERIALIZED VIEW hors transaction).
"""

import asyncio
import sys

import asyncpg
from tests.integration.conftest import migration_files, split_statements  # exporteur


async def main(dsn: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        files = migration_files()
        for migration in files:
            for statement in split_statements(migration.read_text(encoding="utf-8")):
                await conn.execute(statement)
        print(f"{len(files)} migrations appliquées: {files[0].name} .. {files[-1].name}")
    finally:
        await conn.close()


asyncio.run(main(sys.argv[1]))
