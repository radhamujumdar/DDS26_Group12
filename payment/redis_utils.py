"""
Redis client factory supporting Cluster, Sentinel, or direct connections.

Priority: REDIS_CLUSTER_NODES > REDIS_SENTINEL_HOSTS > direct host:port.
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

CLUSTER_REDIS_KWARGS = dict(
    socket_timeout=5.0,
    socket_connect_timeout=5.0,
)


def create_redis_client(
    host: str,
    port: int,
    password: str,
    db: int,
    sentinel_hosts: str | None = None,
    master_name: str | None = None,
    cluster_nodes: str | None = None,
) -> Redis:
    """
    Create a Redis client: Cluster > Sentinel > direct.

    Args:
        cluster_nodes: Comma-separated "host:port" pairs for cluster startup
                       nodes, e.g. "redis-node-1:6379,redis-node-2:6379".
                       When set, cluster mode is used and sentinel/direct args
                       are ignored.
        host/port: Used for direct connection fallback.
        sentinel_hosts: Comma-separated sentinel "host:port" pairs.
        master_name: Sentinel master name (required when sentinel_hosts set).
        password: Redis password.
        db: Redis DB number (ignored in cluster mode; cluster uses db=0).
    """
    if cluster_nodes:
        from redis.asyncio.cluster import ClusterNode, RedisCluster

        nodes = []
        for entry in cluster_nodes.split(","):
            h, p = entry.strip().rsplit(":", 1)
            nodes.append(ClusterNode(h, int(p)))
        return RedisCluster(
            startup_nodes=nodes,
            password=password,
            **CLUSTER_REDIS_KWARGS,
        )

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
