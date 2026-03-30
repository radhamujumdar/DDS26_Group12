"""Runtime configuration for the Fluxi engine."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any
from urllib.parse import unquote, urlparse

from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value


def _optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return value.strip()


@dataclass(frozen=True, slots=True)
class FluxiSettings:
    redis_mode: str = "direct"
    redis_url: str = "redis://localhost:6379/0"
    sentinel_endpoints: str = ""
    sentinel_service_name: str = "fluxi-master"
    sentinel_min_other_sentinels: int = 0
    sentinel_username: str | None = None
    sentinel_password: str | None = None
    key_prefix: str = "fluxi"
    workflow_consumer_group: str = "fluxi-workflow-workers"
    activity_consumer_group: str = "fluxi-activity-workers"
    workflow_task_timeout_ms: int = 30_000
    result_poll_interval_ms: int = 100
    timer_poll_interval_ms: int = 250
    pending_idle_threshold_ms: int = 30_000
    pending_claim_count: int = 100

    def __post_init__(self) -> None:
        mode = self.redis_mode.strip().lower()
        if mode not in {"direct", "sentinel"}:
            raise ValueError("redis_mode must be either 'direct' or 'sentinel'.")
        object.__setattr__(self, "redis_mode", mode)

        sentinel_service_name = self.sentinel_service_name.strip()
        object.__setattr__(self, "sentinel_service_name", sentinel_service_name)

        sentinel_endpoints = ",".join(
            part.strip()
            for part in self.sentinel_endpoints.split(",")
            if part.strip()
        )
        object.__setattr__(self, "sentinel_endpoints", sentinel_endpoints)

        sentinel_username = _normalize_optional_text(self.sentinel_username)
        sentinel_password = _normalize_optional_text(self.sentinel_password)
        object.__setattr__(self, "sentinel_username", sentinel_username)
        object.__setattr__(self, "sentinel_password", sentinel_password)

        if self.sentinel_min_other_sentinels < 0:
            raise ValueError("sentinel_min_other_sentinels must be at least 0.")

        if mode == "sentinel":
            if not sentinel_service_name:
                raise ValueError(
                    "sentinel_service_name must be a non-empty string in sentinel mode."
                )
            _parse_sentinel_endpoints(sentinel_endpoints)

    @classmethod
    def from_env(cls) -> FluxiSettings:
        defaults = cls()
        return cls(
            redis_mode=_env("FLUXI_REDIS_MODE", defaults.redis_mode),
            redis_url=_env("FLUXI_REDIS_URL", defaults.redis_url),
            sentinel_endpoints=_env(
                "FLUXI_SENTINEL_ENDPOINTS",
                defaults.sentinel_endpoints,
            ),
            sentinel_service_name=_env(
                "FLUXI_SENTINEL_SERVICE_NAME",
                defaults.sentinel_service_name,
            ),
            sentinel_min_other_sentinels=int(
                _env(
                    "FLUXI_SENTINEL_MIN_OTHER_SENTINELS",
                    str(defaults.sentinel_min_other_sentinels),
                )
            ),
            sentinel_username=_optional_env("FLUXI_SENTINEL_USERNAME"),
            sentinel_password=_optional_env("FLUXI_SENTINEL_PASSWORD"),
            key_prefix=_env("FLUXI_KEY_PREFIX", defaults.key_prefix),
            workflow_consumer_group=_env(
                "FLUXI_WORKFLOW_CONSUMER_GROUP",
                defaults.workflow_consumer_group,
            ),
            activity_consumer_group=_env(
                "FLUXI_ACTIVITY_CONSUMER_GROUP",
                defaults.activity_consumer_group,
            ),
            workflow_task_timeout_ms=int(
                _env(
                    "FLUXI_WORKFLOW_TASK_TIMEOUT_MS",
                    str(defaults.workflow_task_timeout_ms),
                )
            ),
            result_poll_interval_ms=int(
                _env(
                    "FLUXI_RESULT_POLL_INTERVAL_MS",
                    str(defaults.result_poll_interval_ms),
                )
            ),
            timer_poll_interval_ms=int(
                _env(
                    "FLUXI_TIMER_POLL_INTERVAL_MS",
                    str(defaults.timer_poll_interval_ms),
                )
            ),
            pending_idle_threshold_ms=int(
                _env(
                    "FLUXI_PENDING_IDLE_THRESHOLD_MS",
                    str(defaults.pending_idle_threshold_ms),
                )
            ),
            pending_claim_count=int(
                _env(
                    "FLUXI_PENDING_CLAIM_COUNT",
                    str(defaults.pending_claim_count),
                )
            ),
        )


def create_redis_client(settings: FluxiSettings) -> Redis:
    """Create the async Redis client used by the engine."""

    if settings.redis_mode == "direct":
        return Redis.from_url(settings.redis_url, decode_responses=False)

    sentinel = Sentinel(
        _parse_sentinel_endpoints(settings.sentinel_endpoints),
        min_other_sentinels=settings.sentinel_min_other_sentinels,
        sentinel_kwargs=_sentinel_kwargs(settings),
    )
    return sentinel.master_for(
        settings.sentinel_service_name,
        redis_class=Redis,
        **_redis_connection_kwargs_from_url(settings.redis_url),
    )


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


def _sentinel_kwargs(settings: FluxiSettings) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if settings.sentinel_username is not None:
        kwargs["username"] = settings.sentinel_username
    if settings.sentinel_password is not None:
        kwargs["password"] = settings.sentinel_password
    return kwargs


def _redis_connection_kwargs_from_url(redis_url: str) -> dict[str, Any]:
    parsed = urlparse(redis_url)
    if parsed.scheme not in {"redis", "rediss"}:
        raise ValueError(
            "redis_url must use the redis:// or rediss:// scheme."
        )

    kwargs: dict[str, Any] = {"decode_responses": False}

    db = 0
    if parsed.path and parsed.path != "/":
        try:
            db = int(parsed.path.lstrip("/"))
        except ValueError as exc:
            raise ValueError("redis_url database path must be an integer.") from exc
    kwargs["db"] = db

    if parsed.username:
        kwargs["username"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)
    if parsed.scheme == "rediss":
        kwargs["ssl"] = True

    return kwargs
