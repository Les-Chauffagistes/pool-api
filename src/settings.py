from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str
    prometheus_url: str
    api_key: str
    redis_url: str = "redis://localhost:6379/0"
    ping_interval_s: float = 30
    share_stream_poll_interval_s: float = 0.5
    share_stream_heartbeat_s: float = 15
    share_stream_buffer: int = 1000
    share_stream_send_timeout_s: float = 30
    share_stream_max_clients: int = 200
    share_stream_replay_batch: int = 1000
    # une instance sans heartbeat depuis ce délai n'entre plus dans le watermark
    share_stream_instance_stale_s: float = 60
    data_refresh_interval_s: float = 30

    model_config = {"env_file": ".env", "extra": "allow"}

settings = Settings()
