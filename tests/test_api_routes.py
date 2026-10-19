"""Routes HTTP de bout en bout (hors base) : app réelle, container branché sur un faux PoolService."""

import pytest
from fastapi.testclient import TestClient

from pool_api.container import Container, get_container, set_container
from pool_api.exceptions import NotFoundError
from pool_api.impl.v1.services.pool import PoolService
from pool_api.main import app
from pool_api.models.node import Node
from pool_api.models.pool import Pool
from pool_api.models.pool_api_data_payload import PoolApiDataPayload
from pool_api.models.pool_hashrate_record import PoolHashrateRecord
from pool_api.models.pool_hashrates import PoolHashrates
from pool_api.models.pool_ping import PoolPing
from pool_api.models.pool_private_best_record import PoolPrivateBestRecord
from pool_api.models.pool_runtime import PoolRuntime
from pool_api.models.pool_shares import PoolShares
from pool_api.models.pool_top import PoolTop
from settings import settings
from test_pool_service import raw_data


class FakeService:
    def __init__(self):
        self.not_found = False

    async def get_primary_pool_stat(self):
        return Pool(
            runtime=PoolRuntime(runtime=1, lastupdate=2, Users=3, Workers=4, Idle=5, Disconnected=6),
            hashrates=PoolHashrates(
                hashrate1m="1 K", hashrate5m="2 K", hashrate15m="3 K", hashrate1hr="4 K",
                hashrate6hr="5 K", hashrate1d="6 K", hashrate7d="7 K",
            ),
            shares=PoolShares(diff=1, accepted=2, rejected=3, bestshare=4, SPS1m=1, SPS5m=2, SPS15m=3, SPS1h=4),
        )

    async def get_primary_node_stat(self):
        return Node(height=1, subversion="/Satoshi:28.1.0/", peers=8)

    async def get_user_stats(self, user):
        if self.not_found:
            raise NotFoundError("Adresse introuvable")
        raise AssertionError("non utilisé")

    async def get_top(self):
        return PoolTop(topBestShares=[], topHashrate=[])

    async def get_pings(self):
        return PoolPing(updatedAt="2026-01-01T00:00:00", total=0, online=0, offline=0, hosts=[])

    async def get_monthly_bests(self):
        return [PoolPrivateBestRecord(month="2026-01", sdiff=1.5, address="bc1q", epoch=1)]

    async def get_hashrate(self):
        return [PoolHashrateRecord(date="2026-01-02", hashrate=10)]


@pytest.fixture
def service():
    return FakeService()


@pytest.fixture
def client(service):
    set_container(Container(pool_service=service, share_hub=None, redis=None))
    yield TestClient(app)  # sans `with` : le lifespan (asyncpg, redis...) n'est pas lancé
    set_container(None)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/v1/pool", lambda j: j["runtime"]["Users"] == 3 and j["hashrates"]["hashrate1m"] == "1 K"),
        ("/v1/node", lambda j: j == {"height": 1, "subversion": "/Satoshi:28.1.0/", "peers": 8}),
        ("/v1/top", lambda j: j == {"topBestShares": [], "topHashrate": []}),
        ("/v1/pings", lambda j: j["updatedAt"] == "2026-01-01T00:00:00" and j["hosts"] == []),
        ("/v1/monthlyBests", lambda j: j[0]["month"] == "2026-01" and j[0]["address"] == "bc1q"),
        ("/v1/hashrate", lambda j: j == [{"date": "2026-01-02", "hashrate": 10}]),
    ],
)
def test_public_routes(client, path, expected):
    r = client.get(path)
    assert r.status_code == 200
    assert expected(r.json())


def test_unknown_user_returns_404_with_error_body(client, service):
    service.not_found = True
    r = client.get("/v1/stats/bc1qunknown")
    assert r.status_code == 404
    assert r.json() == {"error": "Adresse introuvable"}


def test_data_requires_a_valid_bearer_token(client, monkeypatch):
    monkeypatch.setattr(settings, "api_key", "secret")
    assert client.get("/v1/data").status_code in (401, 403)
    assert client.get("/v1/data", headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_data_with_valid_token_returns_the_payload(client, service, monkeypatch):
    monkeypatch.setattr(settings, "api_key", "secret")
    payload = PoolApiDataPayload.from_dict(
        {**PoolService._format_data_hashrates(raw_data()), "backup_pool": False, "node": {"height": 1, "subversion": "/Satoshi:28.1.0/", "peers": 8}}
    )

    async def get_data():
        return payload

    monkeypatch.setattr(service, "get_data", get_data, raising=False)
    r = client.get("/v1/data", headers={"Authorization": "Bearer secret"})
    assert r.status_code == 200
    assert r.json()["node"]["height"] == 1 and r.json()["backup_pool"] is False


def test_cors_allows_any_origin(client):
    r = client.get("/v1/node", headers={"Origin": "https://example.org"})
    assert r.headers["access-control-allow-origin"] == "*"


def test_get_container_before_startup_raises():
    set_container(None)
    with pytest.raises(RuntimeError, match="Container non initialisé"):
        get_container()
