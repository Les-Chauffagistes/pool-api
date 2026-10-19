import asyncio

import pytest

from pool_api.impl.v1.services.data_cache_refresher import REFRESH_LOCK_KEY, DataCacheRefresher


class FakeRaw:
    def __init__(self, lock_free=True):
        self.lock_free = lock_free
        self.calls: list[tuple] = []

    async def set(self, key, value, **kw):
        self.calls.append((key, value, kw))
        return self.lock_free or None


class FakeRedis:
    def __init__(self, **kw):
        self.raw = FakeRaw(**kw)


class FakeService:
    def __init__(self, fail_times=0):
        self.refreshed = 0
        self.fail_times = fail_times
        self.attempts = 0

    async def refresh_data(self):
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise RuntimeError("boom")
        self.refreshed += 1


async def test_refresh_takes_the_lock_then_refreshes():
    service, redis = FakeService(), FakeRedis()
    assert await DataCacheRefresher(service, redis, interval_s=10).refresh() is True
    assert service.refreshed == 1
    [(key, _, kw)] = redis.raw.calls
    assert key == REFRESH_LOCK_KEY
    assert kw == {"nx": True, "px": 8000}  # expire à 80 % de l'intervalle


async def test_refresh_skipped_when_another_instance_holds_the_lock():
    service = FakeService()
    assert await DataCacheRefresher(service, FakeRedis(lock_free=False), interval_s=10).refresh() is False
    assert service.refreshed == 0


async def test_lock_duration_is_never_zero():
    redis = FakeRedis()
    await DataCacheRefresher(FakeService(), redis, interval_s=0.0001).refresh()
    assert redis.raw.calls[0][2]["px"] == 1


async def test_loop_survives_errors_and_keeps_refreshing():
    service = FakeService(fail_times=1)
    refresher = DataCacheRefresher(service, FakeRedis(), interval_s=0.01)
    await refresher.start()
    for _ in range(100):
        if service.refreshed >= 2:
            break
        await asyncio.sleep(0.01)
    await refresher.stop()
    assert service.attempts >= 3 and service.refreshed >= 2


async def test_stop_without_start_is_a_noop_and_stop_is_idempotent():
    refresher = DataCacheRefresher(FakeService(), FakeRedis(), interval_s=10)
    await refresher.stop()
    await refresher.start()
    await refresher.stop()
    await refresher.stop()
    assert refresher._task is None
