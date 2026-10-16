"""Le lifespan câble les dépendances et les libère à l'arrêt (asyncpg, redis, tâches de fond remplacés par des faux)."""

import pytest
from fastapi.testclient import TestClient

import pool_api.main as main
from pool_api.container import get_container


class Recorder:
    events: list[str] = []


class FakePg:
    async def close(self):
        Recorder.events.append("pg.close")


def make_fake(name):
    class Fake:
        def __init__(self, *args, **kwargs):
            self.args = args
            Recorder.events.append(f"{name}.init")

        async def start(self):
            Recorder.events.append(f"{name}.start")

        async def stop(self):
            Recorder.events.append(f"{name}.stop")

        async def close(self):
            Recorder.events.append(f"{name}.close")

    return Fake


@pytest.fixture
def patched(monkeypatch):
    Recorder.events = []

    async def create_pool(dsn, *a, **kw):
        Recorder.events.append(f"create_pool:{dsn}")
        return FakePg()

    monkeypatch.setattr(main.asyncpg, "create_pool", create_pool)
    for name in ("RedisClient", "PingMonitor", "ShareHub", "DataCacheRefresher"):
        monkeypatch.setattr(main, name, make_fake(name))
    return Recorder.events


def test_lifespan_builds_the_container_then_tears_everything_down(patched):
    with TestClient(main.app):
        container = get_container()
        assert container.pool_service is not None and container.share_hub is not None
        assert {"PingMonitor.start", "ShareHub.start", "DataCacheRefresher.start"} <= set(patched)
        assert "PingMonitor.stop" not in patched

    with pytest.raises(RuntimeError):  # container retiré à l'arrêt
        get_container()
    assert {"ShareHub.stop", "DataCacheRefresher.stop", "PingMonitor.stop", "RedisClient.close", "pg.close"} <= set(patched)
    # les deux bases (pool + historique) sont ouvertes
    assert sum(e.startswith("create_pool:") for e in patched) == 2
