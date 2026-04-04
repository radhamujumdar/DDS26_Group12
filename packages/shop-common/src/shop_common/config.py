"""Shared environment configuration helpers for the shopping-cart services."""

from __future__ import annotations

import os
from dataclasses import dataclass
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
    redis_mode: str = "direct"
    host: str = "localhost"
    port: int = 6379
    password: str | None = None
    db: int = 0
    sentinel_endpoints: str = ""
    sentinel_service_name: str = "redis-master"
    sentinel_min_other_sentinels: int = 0
    sentinel_username: str | None = None
    sentinel_password: str | None = None

    def __post_init__(self) -> None:
        mode = self.redis_mode.strip().lower()
        if mode not in {"direct", "sentinel"}:
            raise ValueError("redis_mode must be either 'direct' or 'sentinel'.")
        object.__setattr__(self, "redis_mode", mode)

        object.__setattr__(self, "host", self.host.strip())
        object.__setattr__(self, "sentinel_service_name", self.sentinel_service_name.strip())
        object.__setattr__(
            self,
            "sentinel_endpoints",
            ",".join(
                part.strip()
                for part in self.sentinel_endpoints.split(",")
                if part.strip()
            ),
        )
        object.__setattr__(self, "sentinel_username", _normalize_optional_text(self.sentinel_username))
        object.__setattr__(self, "sentinel_password", _normalize_optional_text(self.sentinel_password))

        if self.port < 1 or self.port > 65535:
            raise ValueError("port must be within 1-65535.")
        if self.db < 0:
            raise ValueError("db must be at least 0.")
        if self.sentinel_min_other_sentinels < 0:
            raise ValueError("sentinel_min_other_sentinels must be at least 0.")

        if mode == "direct" and not self.host:
            raise ValueError("host must be non-empty in direct mode.")
        if mode == "sentinel":
            if not self.sentinel_service_name:
                raise ValueError(
                    "sentinel_service_name must be non-empty in sentinel mode."
                )
            _parse_sentinel_endpoints(self.sentinel_endpoints)

    @classmethod
    def from_env(cls) -> RedisSettings:
        defaults = cls()
        mode = optional_env("REDIS_MODE") or defaults.redis_mode
        password = optional_env("REDIS_PASSWORD")
        if mode.strip().lower() == "direct":
            host = required_env("REDIS_HOST")
            port = int(required_env("REDIS_PORT"))
            db = int(required_env("REDIS_DB"))
        else:
            host = optional_env("REDIS_HOST") or defaults.host
            port = int(optional_env("REDIS_PORT") or str(defaults.port))
            db = int(optional_env("REDIS_DB") or str(defaults.db))
        return cls(
            redis_mode=mode,
            host=host,
            port=port,
            password=password,
            db=db,
            sentinel_endpoints=optional_env("REDIS_SENTINEL_ENDPOINTS") or defaults.sentinel_endpoints,
            sentinel_service_name=optional_env("REDIS_SENTINEL_SERVICE_NAME") or defaults.sentinel_service_name,
            sentinel_min_other_sentinels=int(
                optional_env("REDIS_SENTINEL_MIN_OTHER_SENTINELS")
                or str(defaults.sentinel_min_other_sentinels)
            ),
            sentinel_username=optional_env("REDIS_SENTINEL_USERNAME"),
            sentinel_password=optional_env("REDIS_SENTINEL_PASSWORD"),
        )

    @property
    def url(self) -> str:
        auth = ""
        if self.password is not None:
            auth = f":{quote(self.password, safe='')}@"
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


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
        try:
            port = int(port_text)
        except ValueError as exc:
            raise ValueError(
                f"Sentinel endpoint {entry!r} has an invalid port."
            ) from exc
        if port < 1 or port > 65535:
            raise ValueError(
                f"Sentinel endpoint {entry!r} has a port outside 1-65535."
            )
        endpoints.append((host, port))

    if not endpoints:
        raise ValueError(
            "sentinel_endpoints must include at least one host:port entry in sentinel mode."
        )
    return tuple(endpoints)


@dataclass(frozen=True, slots=True)
class GatewaySettings:
    gateway_url: str

    @classmethod
    def from_env(cls) -> GatewaySettings:
        return cls(gateway_url=required_env("GATEWAY_URL").rstrip("/"))
