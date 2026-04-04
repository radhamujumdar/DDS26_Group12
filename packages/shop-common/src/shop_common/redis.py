"""Shared async Redis helpers for the shopping-cart services."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import unquote, urlparse

from msgspec import msgpack
from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.sentinel import Sentinel

from .config import RedisSettings


def create_connection_pool(settings: RedisSettings) -> Any:
    if settings.redis_mode == "direct":
        return ConnectionPool.from_url(
            settings.url,
            decode_responses=False,
            max_connections=64,
        )

    sentinel = _ShopSentinel(
        _parse_sentinel_endpoints(settings.sentinel_endpoints),
        min_other_sentinels=settings.sentinel_min_other_sentinels,
        sentinel_kwargs=_sentinel_kwargs(settings),
        address_remap=_sentinel_discovery_remap(
            _parse_sentinel_endpoints(settings.sentinel_endpoints)
        ),
    )
    redis = sentinel.master_for(
        settings.sentinel_service_name,
        redis_class=Redis,
        **_redis_connection_kwargs(settings),
    )
    return redis.connection_pool


def create_redis_client(settings: RedisSettings) -> Redis:
    if settings.redis_mode == "direct":
        return Redis.from_pool(create_connection_pool(settings))

    sentinel = _ShopSentinel(
        _parse_sentinel_endpoints(settings.sentinel_endpoints),
        min_other_sentinels=settings.sentinel_min_other_sentinels,
        sentinel_kwargs=_sentinel_kwargs(settings),
        address_remap=_sentinel_discovery_remap(
            _parse_sentinel_endpoints(settings.sentinel_endpoints)
        ),
    )
    return sentinel.master_for(
        settings.sentinel_service_name,
        redis_class=Redis,
        **_redis_connection_kwargs(settings),
    )


async def close_redis_client(redis: Redis) -> None:
    close_error: BaseException | None = None
    try:
        await redis.aclose(close_connection_pool=True)
    except TypeError:
        await redis.aclose()
    except BaseException as exc:
        close_error = exc

    pool = getattr(redis, "connection_pool", None)
    if pool is not None:
        disconnect = getattr(pool, "disconnect", None)
        if callable(disconnect):
            try:
                await disconnect(inuse_connections=True)
            except TypeError:
                try:
                    await disconnect()
                except BaseException as exc:
                    close_error = close_error or exc
            except BaseException as exc:
                close_error = close_error or exc

        sentinel_manager = getattr(pool, "sentinel_manager", None)
        sentinels = getattr(sentinel_manager, "sentinels", ())
        for sentinel_client in sentinels:
            try:
                await sentinel_client.aclose(close_connection_pool=True)
            except TypeError:
                try:
                    await sentinel_client.aclose()
                except BaseException as exc:
                    close_error = close_error or exc
            except BaseException as exc:
                close_error = close_error or exc

            sentinel_pool = getattr(sentinel_client, "connection_pool", None)
            sentinel_disconnect = getattr(sentinel_pool, "disconnect", None)
            if callable(sentinel_disconnect):
                try:
                    await sentinel_disconnect(inuse_connections=True)
                except TypeError:
                    try:
                        await sentinel_disconnect()
                    except BaseException as exc:
                        close_error = close_error or exc
                except BaseException as exc:
                    close_error = close_error or exc

    if close_error is not None:
        raise close_error


def activity_dedupe_key(activity_execution_id: str) -> str:
    return f"activity-dedupe:{activity_execution_id}"


def encode_success_record(result: dict[str, Any]) -> bytes:
    return msgpack.encode({"status": "success", "result": result})


def encode_error_record(code: str, message: str) -> bytes:
    return msgpack.encode(
        {
            "status": "error",
            "error": {
                "code": code,
                "message": message,
            },
        }
    )


def decode_dedupe_record(raw: bytes) -> dict[str, Any]:
    decoded = msgpack.decode(raw)
    if not isinstance(decoded, dict):
        raise TypeError("Dedupe record must decode to a dictionary.")
    return decoded


def _parse_sentinel_endpoints(value: str) -> tuple[tuple[str, int], ...]:
    endpoints: list[tuple[str, int]] = []
    for raw_entry in value.split(","):
        entry = raw_entry.strip()
        if not entry:
            continue
        if ":" not in entry:
            raise ValueError(
                "sentinel_endpoints must be a comma-separated list of host:port entries."
            )
        host, port_text = entry.rsplit(":", 1)
        host = host.strip()
        port_text = port_text.strip()
        if not host:
            raise ValueError("Sentinel endpoint host must be non-empty.")
        endpoints.append((host, int(port_text)))
    if not endpoints:
        raise ValueError(
            "sentinel_endpoints must include at least one host:port entry in sentinel mode."
        )
    return tuple(endpoints)


def _sentinel_kwargs(settings: RedisSettings) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if settings.sentinel_username is not None:
        kwargs["username"] = settings.sentinel_username
    if settings.sentinel_password is not None:
        kwargs["password"] = settings.sentinel_password
    return kwargs


def _redis_connection_kwargs(settings: RedisSettings) -> dict[str, Any]:
    parsed = urlparse(settings.url)
    kwargs: dict[str, Any] = {
        "db": settings.db,
        "decode_responses": False,
        "max_connections": 64,
    }
    if parsed.username:
        kwargs["username"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)
    if parsed.scheme == "rediss":
        kwargs["ssl"] = True
    return kwargs


class _ShopSentinel(Sentinel):
    def __init__(
        self,
        *args: Any,
        address_remap: Callable[[tuple[str, int]], tuple[str, int]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._address_remap = address_remap

    async def discover_master(self, service_name: str) -> tuple[str, int]:
        address = await super().discover_master(service_name)
        if self._address_remap is None:
            return address
        return self._address_remap(address)


def _sentinel_discovery_remap(
    endpoints: tuple[tuple[str, int], ...],
) -> Callable[[tuple[str, int]], tuple[str, int]] | None:
    loopback_hosts = {"127.0.0.1", "::1", "localhost"}
    if not endpoints or not all(host in loopback_hosts for host, _ in endpoints):
        return None

    def remap(address: tuple[str, int]) -> tuple[str, int]:
        host, port = address
        if host in {"host.docker.internal", "gateway.docker.internal"}:
            return ("127.0.0.1", port)
        return (host, port)

    return remap
