"""
Shared Redis client factory with optional Sentinel support.

When REDIS_SENTINEL_HOSTS is set, creates a Sentinel-aware client that
automatically discovers the current master and handles failover.
Otherwise, falls back to a direct Redis connection.
"""

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError

COMMON_REDIS_KWARGS = dict(
    health_check_interval=2,
    # Saga stream consumers use blocking reads (XREAD/XREADGROUP with block=1000),
    # so the socket timeout must be comfortably above 1s to avoid self-induced
    # read timeouts and retry backoff during normal operation.
    socket_timeout=5.0,
    socket_connect_timeout=5.0,
    retry_on_timeout=True,
    retry_on_error=[ConnectionError, TimeoutError, ConnectionRefusedError],
    retry=Retry(ExponentialBackoff(cap=2, base=1), 3),
    max_connections=2000,
)


def create_redis_client(
    host: str,
    port: int,
    password: str,
    db: int,
    sentinel_hosts: str | None = None,
    master_name: str | None = None,
) -> Redis:
    """
    Create a Redis client, optionally backed by Sentinel.

    Args:
        host: Redis host (used as fallback when Sentinel is not configured).
        port: Redis port (used as fallback when Sentinel is not configured).
        password: Redis password.
        db: Redis database number.
        sentinel_hosts: Comma-separated list of sentinel host:port pairs,
                        e.g. "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379".
                        If empty/None, falls back to direct connection.
        master_name: The Sentinel master name (e.g. "payment-db").
                     Required when sentinel_hosts is set.
    """
    if sentinel_hosts and master_name:
        from redis.asyncio.sentinel import Sentinel

        hosts = []
        for entry in sentinel_hosts.split(","):
            h, p = entry.strip().rsplit(":", 1)
            hosts.append((h, int(p)))
        sentinel = Sentinel(hosts, sentinel_kwargs={"password": None})
        return sentinel.master_for(
            master_name,
            password=password,
            db=db,
            **COMMON_REDIS_KWARGS,
        )

    return Redis(
        host=host,
        port=port,
        password=password,
        db=db,
        **COMMON_REDIS_KWARGS,
    )


def create_sharded_redis(
    shard_hosts: str | None,
    host: str,
    port: int,
    password: str,
    db: int,
    sentinel_hosts: str | None = None,
    master_name: str | None = None,
):
    """
    Create a ShardedRedis from REDIS_SHARD_HOSTS (comma-separated host:port pairs).
    Falls back to a single-shard wrapper around the legacy connection when not set.
    """
    from sharded_redis import ShardedRedis

    if not shard_hosts:
        single = create_redis_client(
            host=host, port=port, password=password, db=db,
            sentinel_hosts=sentinel_hosts, master_name=master_name,
        )
        return ShardedRedis([single])

    shards: list[Redis] = []
    for entry in shard_hosts.split(","):
        entry = entry.strip()
        h, p = entry.rsplit(":", 1)
        shards.append(create_redis_client(
            host=h, port=int(p), password=password, db=db,
        ))
    return ShardedRedis(shards)
