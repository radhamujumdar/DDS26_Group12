import os
from dataclasses import dataclass


DEFAULT_GATEWAY_URL = "http://gateway:80"
DEFAULT_RECOVERY_INTERVAL_SECONDS = 2.0
DEFAULT_RECOVERY_STARTUP_DELAY_SECONDS = 1.0


@dataclass(frozen=True)
class StockConfig:
    redis_host: str
    redis_port: int
    redis_password: str
    redis_db: int
    gateway_url: str
    recovery_interval_seconds: float
    recovery_startup_delay_seconds: float

    @classmethod
    def from_env(cls) -> "StockConfig":
        return cls(
            redis_host=os.environ["REDIS_HOST"],
            redis_port=int(os.environ["REDIS_PORT"]),
            redis_password=os.environ["REDIS_PASSWORD"],
            redis_db=int(os.environ["REDIS_DB"]),
            gateway_url=os.environ.get("GATEWAY_URL", DEFAULT_GATEWAY_URL).rstrip("/"),
            recovery_interval_seconds=float(
                os.environ.get("RECOVERY_INTERVAL_SECONDS", DEFAULT_RECOVERY_INTERVAL_SECONDS)
            ),
            recovery_startup_delay_seconds=float(
                os.environ.get("RECOVERY_STARTUP_DELAY_SECONDS", DEFAULT_RECOVERY_STARTUP_DELAY_SECONDS)
            ),
        )
