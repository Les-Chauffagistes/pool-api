import pool_api.clients.redis as redis_module
from pool_api.clients.redis import RedisClient


class FakeRedis:
    closed = False

    async def ping(self):
        return True

    async def aclose(self):
        FakeRedis.closed = True


def test_client_is_built_from_url_with_decoded_responses(monkeypatch):
    seen = {}

    def from_url(url, **kw):
        seen.update(url=url, **kw)
        return FakeRedis()

    monkeypatch.setattr(redis_module.Redis, "from_url", from_url)
    client = RedisClient("redis://example:6379/1")
    assert seen == {"url": "redis://example:6379/1", "decode_responses": True}
    assert isinstance(client.raw, FakeRedis)


async def test_ping_and_close_delegate_to_the_underlying_client(monkeypatch):
    FakeRedis.closed = False
    monkeypatch.setattr(redis_module.Redis, "from_url", lambda url, **kw: FakeRedis())
    client = RedisClient("redis://x")
    assert await client.ping() is True
    await client.close()
    assert FakeRedis.closed
