import os
from dataclasses import dataclass


DEFAULT_GATEWAY_URL = "http://gateway:80"
DEFAULT_RECOVERY_INTERVAL_SECONDS = 2.0
DEFAULT_RECOVERY_STARTUP_DELAY_SECONDS = 1.0
DEFAULT_SAGA_MQ_ENABLED = True
DEFAULT_SAGA_MQ_STREAM_PARTITIONS = 4
DEFAULT_SAGA_MQ_BLOCK_MS = 1000
DEFAULT_SAGA_MQ_BATCH_SIZE = 16
DEFAULT_SAGA_MQ_COMMAND_STREAM_MAXLEN = 100000
DEFAULT_SAGA_MQ_RESULT_STREAM_MAXLEN = 100000
DEFAULT_SAGA_MQ_CONSUMER_GROUP = "saga-payment-workers"


def _read_bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class PaymentConfig:
    redis_host: str
    redis_port: int
    redis_password: str
    redis_db: int
    gateway_url: str
    recovery_interval_seconds: float
    recovery_startup_delay_seconds: float
    saga_mq_enabled: bool
    enable_saga_worker: bool
    saga_mq_redis_host: str
    saga_mq_redis_port: int
    saga_mq_redis_password: str
    saga_mq_redis_db: int
    saga_mq_stream_partitions: int
    saga_mq_consumer_group: str
    saga_mq_block_ms: int
    saga_mq_batch_size: int
    saga_mq_command_stream_maxlen: int
    saga_mq_result_stream_maxlen: int

    @classmethod
    def from_env(cls) -> "PaymentConfig":
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
            saga_mq_enabled=_read_bool_env("SAGA_MQ_ENABLED", DEFAULT_SAGA_MQ_ENABLED),
            enable_saga_worker=_read_bool_env("ENABLE_SAGA_WORKER", True),
            saga_mq_redis_host=os.environ.get("SAGA_MQ_REDIS_HOST", os.environ["REDIS_HOST"]),
            saga_mq_redis_port=int(os.environ.get("SAGA_MQ_REDIS_PORT", os.environ["REDIS_PORT"])),
            saga_mq_redis_password=os.environ.get("SAGA_MQ_REDIS_PASSWORD", os.environ["REDIS_PASSWORD"]),
            saga_mq_redis_db=int(os.environ.get("SAGA_MQ_REDIS_DB", "0")),
            saga_mq_stream_partitions=int(
                os.environ.get("SAGA_MQ_STREAM_PARTITIONS", DEFAULT_SAGA_MQ_STREAM_PARTITIONS)
            ),
            saga_mq_consumer_group=os.environ.get("SAGA_MQ_CONSUMER_GROUP", DEFAULT_SAGA_MQ_CONSUMER_GROUP),
            saga_mq_block_ms=int(os.environ.get("SAGA_MQ_BLOCK_MS", DEFAULT_SAGA_MQ_BLOCK_MS)),
            saga_mq_batch_size=int(os.environ.get("SAGA_MQ_BATCH_SIZE", DEFAULT_SAGA_MQ_BATCH_SIZE)),
            saga_mq_command_stream_maxlen=int(
                os.environ.get("SAGA_MQ_COMMAND_STREAM_MAXLEN", DEFAULT_SAGA_MQ_COMMAND_STREAM_MAXLEN)
            ),
            saga_mq_result_stream_maxlen=int(
                os.environ.get("SAGA_MQ_RESULT_STREAM_MAXLEN", DEFAULT_SAGA_MQ_RESULT_STREAM_MAXLEN)
            ),
        )
