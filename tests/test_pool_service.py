from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from pool_api.dao.PoolStat import PoolStatDAO
from pool_api.dao.Users import UsersDAO
from pool_api.exceptions import NotFoundError
from pool_api.impl.v1.services.pool import DATA_CACHE_KEY, DATA_CACHE_TTL_S, PoolService
from pool_api.models.pool_ping import PoolPing


class FakeRaw:
    def __init__(self, store=None, fail_get=False, fail_set=False):
        self.store = store or {}
        self.fail_get, self.fail_set = fail_get, fail_set
        self.set_calls: list[tuple] = []

    async def get(self, key):
        if self.fail_get:
            raise ConnectionError("redis down")
        return self.store.get(key)

    async def set(self, key, value, **kw):
        if self.fail_set:
            raise ConnectionError("redis down")
        self.set_calls.append((key, value, kw))
        self.store[key] = value
        return True


class FakeRedis:
    def __init__(self, **kw):
        self.raw = FakeRaw(**kw)


class FakePrometheus:
    def __init__(self, values=None):
        self.values = values or {"bitcoin_blocks": "900000", "bitcoin_server_version": "280100", "bitcoin_peers": "12"}
        self.queries: list[str] = []

    async def query_scalar(self, promql):
        self.queries.append(promql)
        return self.values[promql]


class FakePoolStatDAO:
    async def get_primary_pool_stat(self):
        return PoolStatDAO.PoolStatDBModel(
            pool_instance="ckpool01",
            updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            runtime_s=3600, users=5, workers=9, idle=1, disconnected=2,
            hashrate_1m=1_500, hashrate_5m=2_000_000, hashrate_15m=3, hashrate_1h=4_000_000_000,
            hashrate_6h=5, hashrate_1d=6, hashrate_7d=7,
            diff=1.5, accepted=100, rejected=3, bestshare=999,
            sps_1m=0.1, sps_5m=0.2, sps_15m=0.3, sps_1h=0.4,
        )


WORKER = {
    "workername": "rig1", "hashrate1m": 1_000, "hashrate5m": 2_000, "hashrate1hr": 3_000,
    "hashrate1d": 4_000, "hashrate7d": 5_000, "shares": 10, "bestshare": 77, "lastshare": 1_700_000_000,
}


class FakeUsersDAO:
    def __init__(self, user=None, raw=None):
        self.user, self.raw = user, raw

    async def get_user_stat(self, address):
        return self.user

    async def get_top(self):
        return "top"

    async def get_data(self):
        return self.raw


class FakeMonthlyDAO:
    async def get_monthly_bests(self):
        return [{"month": "2026-01", "sdiff": 1.5, "address": "bc1q", "epoch": 1_700_000_000}]


class FakeHistoryDAO:
    async def get_hashrate(self):
        return [{"date": date(2026, 1, 2), "hashrate": Decimal("1234.0")}]


class FakePing:
    def __init__(self, latest=None):
        self.latest, self.refreshed = latest, 0

    async def refresh(self):
        self.refreshed += 1
        self.latest = PoolPing(updatedAt="now", total=0, online=0, offline=0, hosts=[])
        return self.latest


def user_row():
    return UsersDAO.UsersAndWorkerDBModel(
        address="bc1qaddr", hashrate1m=1_000, hashrate5m=2_000_000, hashrate1hr=3, hashrate1d=4, hashrate7d=5,
        lastshare=datetime(2026, 1, 1, tzinfo=timezone.utc), workers=1, shares=10, bestshare=77,
        authorized=datetime(2026, 1, 1, tzinfo=timezone.utc), workers_details=[WORKER],
    )


def raw_data():
    worker = {**WORKER, "bestever": 77}
    return {
        "pool": {
            "runtime": {"runtime": 1, "lastupdate": 2, "Users": 3, "Workers": 4, "Idle": 5, "Disconnected": 6},
            "hashrates": {
                "hashrate1m": 1_000, "hashrate5m": 2_000_000, "hashrate15m": 3, "hashrate1hr": 4,
                "hashrate6hr": 5_000_000_000, "hashrate1d": 6, "hashrate7d": 7,
            },
            "shares": {"diff": 1, "accepted": 2, "rejected": 3, "bestshare": 4, "SPS1m": 1, "SPS5m": 2, "SPS15m": 3, "SPS1h": 4},
        },
        "users": {
            "bc1q": {
                "hashrate1m": 1_500, "hashrate5m": 2, "hashrate1hr": 3, "hashrate1d": 4, "hashrate7d": 5,
                "lastshare": 1, "workers": 1, "shares": 2, "bestshare": 3, "bestever": 3, "authorised": 4,
                "worker": [worker],
            }
        },
        "repartition": {"rig1": {"shares": 1, "total_vardiff": 2, "weight": 2, "percentage": 1}},
        "monthly_bests": [{"month": "2026-01", "sdiff": 1, "username": "u", "workername": "w", "epoch": 1}],
    }


def make_service(*, redis=None, users=None, prometheus=None, ping=None) -> PoolService:
    return PoolService(
        FakePoolStatDAO(), users or FakeUsersDAO(), FakeMonthlyDAO(), FakeHistoryDAO(),
        prometheus or FakePrometheus(), ping or FakePing(), redis or FakeRedis(),
    )


async def test_get_primary_pool_stat_maps_and_formats():
    pool = await make_service().get_primary_pool_stat()
    assert pool.runtime.runtime == 3600
    assert pool.runtime.lastupdate == int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
    assert (pool.runtime.users, pool.runtime.workers, pool.runtime.idle, pool.runtime.disconnected) == (5, 9, 1, 2)
    assert pool.hashrates.hashrate1m == "1.5 K"
    assert pool.hashrates.hashrate5m == "2 M"
    assert pool.hashrates.hashrate1hr == "4 G"
    assert pool.shares.accepted == 100 and pool.shares.sps1h == 0.4


async def test_get_primary_node_stat_converts_prometheus_values():
    prom = FakePrometheus()
    node = await make_service(prometheus=prom).get_primary_node_stat()
    assert (node.height, node.subversion, node.peers) == (900000, "/Satoshi:28.1.0/", 12)
    assert prom.queries == ["bitcoin_blocks", "bitcoin_server_version", "bitcoin_peers"]


async def test_get_user_stats_maps_workers_and_bestever():
    stats = await make_service(users=FakeUsersDAO(user=user_row())).get_user_stats("bc1qaddr")
    assert stats.address == "bc1qaddr"
    assert stats.global_stats.hashrate1m == "1 K"
    assert stats.global_stats.hashrate5m == "2 M"
    [worker] = stats.workers
    assert worker.hashrate1hr == "3 K" and worker.bestever == worker.bestshare == 77


async def test_get_user_stats_unknown_address_raises_not_found():
    with pytest.raises(NotFoundError, match="Adresse introuvable"):
        await make_service(users=FakeUsersDAO(user=None)).get_user_stats("nope")


async def test_get_top_delegates_to_dao():
    assert await make_service().get_top() == "top"


async def test_get_pings_prefers_latest_then_refreshes():
    cached = PoolPing(updatedAt="old", total=0, online=0, offline=0, hosts=[])
    assert await make_service(ping=FakePing(latest=cached)).get_pings() is cached

    ping = FakePing()
    assert (await make_service(ping=ping).get_pings()).updated_at == "now"
    assert ping.refreshed == 1


def test_format_data_hashrates_formats_pool_users_and_workers():
    out = PoolService._format_data_hashrates(raw_data())
    assert out["pool"]["hashrates"]["hashrate5m"] == "2 M"
    assert out["pool"]["hashrates"]["hashrate6hr"] == "5 G"
    assert out["users"]["bc1q"]["hashrate1m"] == "1.5 K"
    assert out["users"]["bc1q"]["worker"][0]["hashrate1m"] == "1 K"
    assert out["repartition"] == raw_data()["repartition"]


async def test_refresh_data_builds_payload_and_caches_it_with_ttl():
    redis = FakeRedis()
    data = await make_service(users=FakeUsersDAO(raw=raw_data()), redis=redis).refresh_data()
    assert data.backup_pool is False
    assert data.node.height == 900000
    [(key, value, kw)] = redis.raw.set_calls
    assert key == DATA_CACHE_KEY and kw == {"ex": DATA_CACHE_TTL_S}
    assert value == data.to_json()


async def test_refresh_data_survives_redis_write_failure():
    data = await make_service(users=FakeUsersDAO(raw=raw_data()), redis=FakeRedis(fail_set=True)).refresh_data()
    assert data.node.peers == 12


async def test_get_data_cache_hit_skips_the_database():
    seed = make_service(users=FakeUsersDAO(raw=raw_data()))
    cached = (await seed.refresh_data()).to_json()

    users = FakeUsersDAO(raw=None)  # un accès base ferait échouer le test
    data = await make_service(users=users, redis=FakeRedis(store={DATA_CACHE_KEY: cached})).get_data()
    assert data.node.height == 900000


async def test_get_data_cache_miss_reads_database_and_fills_cache():
    redis = FakeRedis()
    data = await make_service(users=FakeUsersDAO(raw=raw_data()), redis=redis).get_data()
    assert data.users["bc1q"].hashrate1m is not None
    assert DATA_CACHE_KEY in redis.raw.store


@pytest.mark.parametrize("redis", [FakeRedis(fail_get=True), FakeRedis(store={DATA_CACHE_KEY: "{pas du json"})])
async def test_get_data_falls_back_to_database_when_cache_unusable(redis):
    data = await make_service(users=FakeUsersDAO(raw=raw_data()), redis=redis).get_data()
    assert data.node.height == 900000


async def test_get_monthly_bests_maps_rows():
    [record] = await make_service().get_monthly_bests()
    assert (record.month, record.address, record.epoch) == ("2026-01", "bc1q", 1_700_000_000)


async def test_get_hashrate_serialises_date_and_casts_to_int():
    [record] = await make_service().get_hashrate()
    assert record.var_date == "2026-01-02" and record.hashrate == 1234
