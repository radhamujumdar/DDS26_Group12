"""Shared environment configuration helpers for the shopping-cart services."""

from __future__ import annotations

from dataclasses import dataclass
import os
from urllib.parse import quote


def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()


def optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return value.strip()


@dataclass(frozen=True, slots=True)
class RedisSettings:
    host: str
    port: int
    password: str | None
    db: int

    @classmethod
    def from_env(cls) -> RedisSettings:
        password = optional_env("REDIS_PASSWORD")
        return cls(
            host=required_env("REDIS_HOST"),
            port=int(required_env("REDIS_PORT")),
            password=password,
            db=int(required_env("REDIS_DB")),
        )

    @property
    def url(self) -> str:
        auth = ""
        if self.password is not None:
            auth = f":{quote(self.password, safe='')}@"
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


@dataclass(frozen=True, slots=True)
class GatewaySettings:
    gateway_url: str

    @classmethod
    def from_env(cls) -> GatewaySettings:
        return cls(gateway_url=required_env("GATEWAY_URL").rstrip("/"))
