from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str
    prometheus_url: str
    api_key: str
    redis_url: str = "redis://localhost:6379/0"
    ping_interval_s: float = 30
    data_refresh_interval_s: float = 30

    model_config = {"env_file": ".env", "extra": "allow"}

settings = Settings()
