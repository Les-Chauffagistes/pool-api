"""DAO contre un vrai Postgres (testcontainers). Ignorés si Docker est indisponible.

Le schéma (tests/sql/pool_schema.sql) est déduit des requêtes : voir l'avertissement en tête de ce fichier SQL.
"""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from pool_api.dao.History import HistoryDAO
from pool_api.dao.MonthlyBests import MonthlyBestsDAO
from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.dao.Users import UsersDAO

SCHEMA = Path(__file__).parent / "sql" / "pool_schema.sql"
T0 = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
ADDR = "bc1qaddressexample0000000000000000000000"


@pytest.fixture(scope="module")
def postgres_url():
    try:
        from testcontainers.community.postgres import PostgresContainer

        container = PostgresContainer("postgres:17-alpine", username="pool", password="pool", dbname="pool", driver=None)
        container.start()
    except Exception as e:  # pragma: no cover
        pytest.skip(f"Docker/Postgres indisponible: {e}")
    try:
        yield f"postgresql://pool:pool@{container.get_container_host_ip()}:{container.get_exposed_port(5432)}/pool"
    finally:
        container.stop()


@pytest_asyncio.fixture
async def pg(postgres_url):
    pool = await asyncpg.create_pool(postgres_url)
    async with pool.acquire() as c:
        if not await c.fetchval("SELECT to_regclass('pool_stat') IS NOT NULL"):
            await c.execute(SCHEMA.read_text())
        await c.execute(
            "TRUNCATE pool_stat, users, user_workers, monthly_bests, share_weights_hourly, worker_stats_1d"
        )
    yield pool
    await pool.close()


async def add_user(pg, instance, address, *, h1m=0, h1hr=0, workers=1, shares=0, best=0, last=T0, auth=T0):
    await pg.execute(
        """INSERT INTO users (pool_instance, address, hashrate1m, hashrate1hr, workers, shares, bestshare,
                              lastshare, authorized) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
        instance, address, h1m, h1hr, workers, shares, best, last, auth,
    )


async def add_worker(pg, instance, address, name, *, h1m=0, shares=0, best=0, last=T0):
    await pg.execute(
        """INSERT INTO user_workers (pool_instance, address, workername, hashrate1m, shares, bestshare, lastshare)
           VALUES ($1,$2,$3,$4,$5,$6,$7)""",
        instance, address, name, h1m, shares, best, last,
    )


# --- UsersDAO.get_user_stat -------------------------------------------------------------------------------------


async def test_get_user_stat_unknown_address_returns_none(pg):
    assert await UsersDAO(pg).get_user_stat("inconnue") is None


async def test_get_user_stat_aggregates_instances_and_workers(pg):
    await add_user(pg, "ckpool01", ADDR, h1m=100, workers=2, shares=10, best=500, auth=T0 + timedelta(days=1))
    await add_user(pg, "ckpool02", ADDR, h1m=50, workers=1, shares=5, best=900, auth=T0)
    await add_user(pg, "ckpool01", "autre", h1m=999, best=1)
    await add_worker(pg, "ckpool01", ADDR, "rig1", h1m=60, shares=6, best=500, last=T0)
    await add_worker(pg, "ckpool02", ADDR, "rig1", h1m=40, shares=4, best=300, last=T0 + timedelta(hours=1))
    await add_worker(pg, "ckpool01", ADDR, "rig2", h1m=50, shares=5, best=100, last=T0 + timedelta(hours=2))

    user = await UsersDAO(pg).get_user_stat(ADDR)

    assert user.address == ADDR
    assert (user.hashrate1m, user.workers, user.shares) == (150, 3, 15)
    assert user.bestshare == 900  # MAX sur les instances
    assert user.authorized == T0  # MIN sur les instances
    # un worker présent sur deux instances est fusionné ; tri par dernier share décroissant
    assert [w["workername"] for w in user.workers_details] == ["rig2", "rig1"]
    rig1 = next(w for w in user.workers_details if w["workername"] == "rig1")
    assert (rig1["hashrate1m"], rig1["shares"], rig1["bestshare"]) == (100, 10, 500)
    assert rig1["lastshare"] == int((T0 + timedelta(hours=1)).timestamp())


async def test_get_user_stat_without_workers_has_empty_list(pg):
    await add_user(pg, "ckpool01", ADDR, h1m=1)
    user = await UsersDAO(pg).get_user_stat(ADDR)
    assert user.workers_details == []


# --- UsersDAO.get_top -------------------------------------------------------------------------------------------


async def test_get_top_masks_addresses_ranks_and_limits_to_ten(pg):
    for i in range(12):
        await add_user(pg, "ckpool01", f"bc1qtopuser{i:02d}xxxxxxxxxxxxxxxxxxxxxxxxx{i:02d}", h1hr=1000 + i, best=i, workers=i + 1)
    await add_user(pg, "ckpool02", "bc1qtopuser11xxxxxxxxxxxxxxxxxxxxxxxxx11", h1hr=1000)  # même adresse, autre instance

    top = await UsersDAO(pg).get_top()

    assert len(top.top_hashrate) == len(top.top_best_shares) == 10
    first = top.top_hashrate[0]
    assert first.address == "bc1qt…11"  # 5 premiers caractères + … + 2 derniers
    assert first.total_hashrate1hr == 2011  # somme des instances
    assert [t.total_hashrate1hr for t in top.top_hashrate] == sorted(
        (t.total_hashrate1hr for t in top.top_hashrate), reverse=True
    )
    assert top.top_best_shares[0].bestshare == 11


async def test_get_top_on_empty_tables_returns_empty_lists(pg):
    top = await UsersDAO(pg).get_top()
    assert top.top_hashrate == [] and top.top_best_shares == []


# --- UsersDAO.get_data ------------------------------------------------------------------------------------------


async def seed_pool_stat(pg, instance="ckpool01", **over):
    values = dict(
        runtime_s=100, users=2, workers=3, idle=1, disconnected=0, hashrate_1m=10, hashrate_5m=20, hashrate_15m=30,
        hashrate_1h=40, hashrate_6h=50, hashrate_1d=60, hashrate_7d=70, diff=1.5, accepted=100, rejected=2,
        bestshare=77, sps_1m=0.1, sps_5m=0.2, sps_15m=0.3, sps_1h=0.4,
    ) | over
    cols = ", ".join(values)
    marks = ", ".join(f"${i + 3}" for i in range(len(values)))
    await pg.execute(
        f"INSERT INTO pool_stat (pool_instance, updated_at, {cols}) VALUES ($1, $2, {marks})",
        instance, T0, *values.values(),
    )


async def test_get_data_builds_the_full_payload(pg):
    await seed_pool_stat(pg, "ckpool01")
    await seed_pool_stat(pg, "ckpool02", hashrate_1m=5, bestshare=99, runtime_s=200)
    await add_user(pg, "ckpool01", ADDR, h1m=100, workers=1, shares=10, best=500)
    await add_user(pg, "ckpool02", ADDR, h1m=20, workers=1, shares=2, best=600)
    await add_worker(pg, "ckpool01", ADDR, "rig1", h1m=100, shares=10, best=500)
    await pg.execute("INSERT INTO share_weights_hourly VALUES ('rig1', $1, 4, 30), ('rig2', $1, 1, 10)", T0)
    await pg.execute(
        """INSERT INTO monthly_bests (month, best_diff, "user", workername, updated_at)
           VALUES ('2026-01-01', 12.5, 'u1', 'w1', $1), ('2025-12-01', 3, 'u2', 'w2', $1)""", T0
    )

    data = await UsersDAO(pg).get_data()

    pool = data["pool"]
    assert pool["hashrates"]["hashrate1m"] == 15  # somme des instances
    assert pool["runtime"]["runtime"] == 200  # max
    assert pool["shares"]["bestshare"] == 99
    user = data["users"][ADDR]
    assert (user["hashrate1m"], user["workers"], user["shares"], user["bestshare"]) == (120, 2, 12, 600)
    assert user["bestever"] == 600
    assert [w["workername"] for w in user["worker"]] == ["rig1"]
    assert data["repartition"]["rig1"]["percentage"] == pytest.approx(0.75)
    assert data["repartition"]["rig2"]["percentage"] == pytest.approx(0.25)
    assert [m["month"] for m in data["monthly_bests"]] == ["2026-01", "2025-12"]  # plus récent d'abord


async def test_get_data_user_without_workers_has_empty_worker_list(pg):
    await seed_pool_stat(pg)
    await add_user(pg, "ckpool01", ADDR, h1m=1)
    data = await UsersDAO(pg).get_data()
    assert data["users"][ADDR]["worker"] == []
    assert data["repartition"] == {} and data["monthly_bests"] == []


# --- PoolStatDAO ------------------------------------------------------------------------------------------------


async def test_get_primary_pool_stat_reads_the_primary_instance_only(pg):
    await seed_pool_stat(pg, "ckpool01", users=2)
    await seed_pool_stat(pg, "ckpool02", users=50)
    stat = await PoolStatDAO(pg).get_primary_pool_stat()
    assert (stat.pool_instance, stat.users, stat.updated_at) == ("ckpool01", 2, T0)


async def test_get_primary_pool_stat_accepts_fractional_hashrates(pg):
    # hashrate_* sont des `double precision` en base : ils peuvent ne pas être entiers
    # (ex. "1.23T" converti en flottant), ce que le modèle ne doit pas refuser.
    await seed_pool_stat(pg, "ckpool01", hashrate_1m=1234567.5, hashrate_7d=2.5e12 + 0.25)
    stat = await PoolStatDAO(pg).get_primary_pool_stat()
    assert stat.hashrate_1m == pytest.approx(1234567.5)
    assert stat.hashrate_7d == pytest.approx(2.5e12 + 0.25)


async def test_get_primary_pool_stat_missing_instance(pg):
    await seed_pool_stat(pg, "ckpool02")
    # comportement actuel : dict(None) -> TypeError, donc un 500 sur /v1/pool si ckpool01 n'a pas encore écrit
    with pytest.raises(TypeError):
        await PoolStatDAO(pg).get_primary_pool_stat()


# --- MonthlyBestsDAO / HistoryDAO -------------------------------------------------------------------------------


async def test_monthly_bests_keeps_the_best_row_per_month(pg):
    await pg.execute(
        """INSERT INTO monthly_bests (month, best_diff, "user", workername, updated_at) VALUES ('2026-01-01', 5, 'low', 'w', $1),
           ('2026-01-01', 9, 'high', 'w2', $1), ('2025-12-01', 2, 'dec', 'w', $1)""",
        T0,
    )
    rows = await MonthlyBestsDAO(pg).get_monthly_bests()
    assert [(r["month"], r["address"], r["sdiff"]) for r in rows] == [("2025-12", "dec", 2), ("2026-01", "high", 9)]
    assert rows[0]["epoch"] == int(T0.timestamp())


async def test_history_sums_workers_per_day_in_order(pg):
    await pg.execute(
        "INSERT INTO worker_stats_1d VALUES ('2026-01-02','a',10),('2026-01-02','b',5),('2026-01-01','a',7)"
    )
    rows = await HistoryDAO(pg).get_hashrate()
    assert [(r["date"], float(r["hashrate"])) for r in rows] == [(date(2026, 1, 1), 7.0), (date(2026, 1, 2), 15.0)]
