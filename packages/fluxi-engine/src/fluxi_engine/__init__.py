"""Fluxi engine package exports."""

from __future__ import annotations

from typing import Any

from .config import FluxiSettings, close_redis_client, create_redis_client
from .scheduler import FluxiScheduler
from .store import FluxiRedisStore

__all__ = [
    "FluxiRedisStore",
    "FluxiScheduler",
    "FluxiSettings",
    "close_redis_client",
    "create_app",
    "create_redis_client",
]


def __getattr__(name: str) -> Any:
    if name == "create_app":
        from .server import create_app

        return create_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
