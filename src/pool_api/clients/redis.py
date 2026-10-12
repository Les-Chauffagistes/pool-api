from redis.asyncio import Redis


class RedisClient:
    """Client Redis asynchrone (compatible avec Valkey)."""

    def __init__(self, url: str):
        self._client = Redis.from_url(url, decode_responses=True)

    async def ping(self) -> bool:
        return await self._client.ping()

    async def close(self) -> None:
        await self._client.aclose()

    @property
    def raw(self) -> Redis:
        """Accès au client redis-py pour les opérations Redis spécifiques."""
        return self._client
