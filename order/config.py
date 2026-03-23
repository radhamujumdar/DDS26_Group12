import os
from dataclasses import dataclass

from models import TxMode


DEFAULT_STOCK_SERVICE_URL = "http://stock-service:5000"
DEFAULT_PAYMENT_SERVICE_URL = "http://payment-service:5000"
DEFAULT_ENABLE_ORDER_DISPATCHER = True
DEFAULT_SAGA_MQ_STREAM_PARTITIONS = 4
DEFAULT_SAGA_MQ_RESPONSE_TIMEOUT_MS = 6000
DEFAULT_SAGA_MQ_COMMAND_STREAM_MAXLEN = 100000
DEFAULT_SAGA_MQ_RESULT_STREAM_MAXLEN = 100000
DEFAULT_SAGA_MQ_PENDING_TTL_SECONDS = 3600
DEFAULT_SAGA_MQ_PENDING_STALE_AFTER_MS = 3000
DEFAULT_SAGA_MQ_POLL_INTERVAL_SECONDS = 0.05
DEFAULT_SAGA_MQ_DISPATCH_BLOCK_MS = 1000
DEFAULT_SAGA_MQ_DISPATCH_LEASE_TTL_SECONDS = 10
DEFAULT_SAGA_MQ_DISPATCH_RENEW_INTERVAL_SECONDS = 2.0


def _read_bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class OrderConfig:
    redis_host: str
    redis_port: int
    redis_password: str
    redis_db: int
    redis_sentinel_hosts: str | None
    redis_master_name: str | None
    redis_cluster_nodes: str | None
    tx_mode: str
    stock_service_url: str
    payment_service_url: str
    enable_order_dispatcher: bool
    saga_mq_redis_host: str
    saga_mq_redis_port: int
    saga_mq_redis_password: str
    saga_mq_redis_db: int
    saga_mq_sentinel_hosts: str | None
    saga_mq_master_name: str | None
    saga_mq_stream_partitions: int
    saga_mq_response_timeout_ms: int
    saga_mq_command_stream_maxlen: int
    saga_mq_result_stream_maxlen: int
    saga_mq_pending_ttl_seconds: int
    saga_mq_pending_stale_after_ms: int
    saga_mq_poll_interval_seconds: float
    saga_mq_dispatch_block_ms: int
    saga_mq_dispatch_lease_ttl_seconds: int
    saga_mq_dispatch_renew_interval_seconds: float
    saga_mq_cluster_nodes: str | None
    saga_mq_owner_id: str | None

    @classmethod
    def from_env(cls) -> "OrderConfig":
        return cls(
            redis_host=os.environ["REDIS_HOST"],
            redis_port=int(os.environ["REDIS_PORT"]),
            redis_password=os.environ["REDIS_PASSWORD"],
            redis_db=int(os.environ["REDIS_DB"]),
            redis_sentinel_hosts=os.environ.get("REDIS_SENTINEL_HOSTS"),
            redis_master_name=os.environ.get("REDIS_MASTER_NAME"),
            redis_cluster_nodes=os.environ.get("REDIS_CLUSTER_NODES"),
            tx_mode=os.environ.get("TX_MODE", TxMode.TWO_PC.value).lower(),
            stock_service_url=os.environ.get("STOCK_SERVICE_URL", DEFAULT_STOCK_SERVICE_URL).rstrip("/"),
            payment_service_url=os.environ.get("PAYMENT_SERVICE_URL", DEFAULT_PAYMENT_SERVICE_URL).rstrip("/"),
            enable_order_dispatcher=_read_bool_env("ENABLE_ORDER_DISPATCHER", DEFAULT_ENABLE_ORDER_DISPATCHER),
            saga_mq_redis_host=os.environ.get("SAGA_MQ_REDIS_HOST", os.environ["REDIS_HOST"]),
            saga_mq_redis_port=int(os.environ.get("SAGA_MQ_REDIS_PORT", os.environ["REDIS_PORT"])),
            saga_mq_redis_password=os.environ.get("SAGA_MQ_REDIS_PASSWORD", os.environ["REDIS_PASSWORD"]),
            saga_mq_redis_db=int(os.environ.get("SAGA_MQ_REDIS_DB", "0")),
            saga_mq_sentinel_hosts=os.environ.get("SAGA_MQ_SENTINEL_HOSTS"),
            saga_mq_master_name=os.environ.get("SAGA_MQ_MASTER_NAME"),
            saga_mq_stream_partitions=int(
                os.environ.get("SAGA_MQ_STREAM_PARTITIONS", DEFAULT_SAGA_MQ_STREAM_PARTITIONS)
            ),
            saga_mq_response_timeout_ms=int(
                os.environ.get("SAGA_MQ_RESPONSE_TIMEOUT_MS", DEFAULT_SAGA_MQ_RESPONSE_TIMEOUT_MS)
            ),
            saga_mq_command_stream_maxlen=int(
                os.environ.get("SAGA_MQ_COMMAND_STREAM_MAXLEN", DEFAULT_SAGA_MQ_COMMAND_STREAM_MAXLEN)
            ),
            saga_mq_result_stream_maxlen=int(
                os.environ.get("SAGA_MQ_RESULT_STREAM_MAXLEN", DEFAULT_SAGA_MQ_RESULT_STREAM_MAXLEN)
            ),
            saga_mq_pending_ttl_seconds=int(
                os.environ.get("SAGA_MQ_PENDING_TTL_SECONDS", DEFAULT_SAGA_MQ_PENDING_TTL_SECONDS)
            ),
            saga_mq_pending_stale_after_ms=int(
                os.environ.get("SAGA_MQ_PENDING_STALE_AFTER_MS", DEFAULT_SAGA_MQ_PENDING_STALE_AFTER_MS)
            ),
            saga_mq_poll_interval_seconds=float(
                os.environ.get("SAGA_MQ_POLL_INTERVAL_SECONDS", DEFAULT_SAGA_MQ_POLL_INTERVAL_SECONDS)
            ),
            saga_mq_dispatch_block_ms=int(
                os.environ.get("SAGA_MQ_DISPATCH_BLOCK_MS", DEFAULT_SAGA_MQ_DISPATCH_BLOCK_MS)
            ),
            saga_mq_dispatch_lease_ttl_seconds=int(
                os.environ.get("SAGA_MQ_DISPATCH_LEASE_TTL_SECONDS", DEFAULT_SAGA_MQ_DISPATCH_LEASE_TTL_SECONDS)
            ),
            saga_mq_dispatch_renew_interval_seconds=float(
                os.environ.get(
                    "SAGA_MQ_DISPATCH_RENEW_INTERVAL_SECONDS",
                    DEFAULT_SAGA_MQ_DISPATCH_RENEW_INTERVAL_SECONDS,
                )
            ),
            saga_mq_cluster_nodes=os.environ.get("SAGA_MQ_CLUSTER_NODES"),
            saga_mq_owner_id=os.environ.get("SAGA_MQ_OWNER_ID"),
        )
