from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str
    prometheus_url: str
    ping_interval_s: float = 30

    model_config = {"env_file": ".env", "extra": "allow"}

settings = Settings()