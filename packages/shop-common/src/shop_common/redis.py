"""Shared async Redis helpers for the shopping-cart services."""

from __future__ import annotations

from typing import Any

from msgspec import msgpack
from redis.asyncio import ConnectionPool, Redis

from .config import RedisSettings


def create_connection_pool(settings: RedisSettings) -> ConnectionPool:
    return ConnectionPool.from_url(
        settings.url,
        decode_responses=False,
        max_connections=64,
    )


def create_redis_client(settings: RedisSettings) -> Redis:
    return Redis.from_pool(create_connection_pool(settings))


async def close_redis_client(redis: Redis) -> None:
    try:
        await redis.aclose(close_connection_pool=True)
    except TypeError:
        await redis.aclose()


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
