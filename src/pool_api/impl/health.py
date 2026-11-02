from pool_api.apis.health_api_base import BaseHealthApi


class HealthApi(BaseHealthApi):
    async def get_health(self) -> None:
        return None

    async def head_health(self) -> None:
        return None
