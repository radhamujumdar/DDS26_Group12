"""Runtime configuration for the Fluxi engine."""

from __future__ import annotations

from dataclasses import dataclass
import os

from redis.asyncio import Redis


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value


@dataclass(frozen=True, slots=True)
class FluxiSettings:
    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "fluxi"
    workflow_consumer_group: str = "fluxi-workflow-workers"
    activity_consumer_group: str = "fluxi-activity-workers"
    workflow_task_timeout_ms: int = 30_000
    result_poll_interval_ms: int = 100
    timer_poll_interval_ms: int = 250
    pending_idle_threshold_ms: int = 30_000
    pending_claim_count: int = 100

    @classmethod
    def from_env(cls) -> FluxiSettings:
        return cls(
            redis_url=_env("FLUXI_REDIS_URL", cls.redis_url),
            key_prefix=_env("FLUXI_KEY_PREFIX", cls.key_prefix),
            workflow_consumer_group=_env(
                "FLUXI_WORKFLOW_CONSUMER_GROUP",
                cls.workflow_consumer_group,
            ),
            activity_consumer_group=_env(
                "FLUXI_ACTIVITY_CONSUMER_GROUP",
                cls.activity_consumer_group,
            ),
            workflow_task_timeout_ms=int(
                _env(
                    "FLUXI_WORKFLOW_TASK_TIMEOUT_MS",
                    str(cls.workflow_task_timeout_ms),
                )
            ),
            result_poll_interval_ms=int(
                _env(
                    "FLUXI_RESULT_POLL_INTERVAL_MS",
                    str(cls.result_poll_interval_ms),
                )
            ),
            timer_poll_interval_ms=int(
                _env(
                    "FLUXI_TIMER_POLL_INTERVAL_MS",
                    str(cls.timer_poll_interval_ms),
                )
            ),
            pending_idle_threshold_ms=int(
                _env(
                    "FLUXI_PENDING_IDLE_THRESHOLD_MS",
                    str(cls.pending_idle_threshold_ms),
                )
            ),
            pending_claim_count=int(
                _env(
                    "FLUXI_PENDING_CLAIM_COUNT",
                    str(cls.pending_claim_count),
                )
            ),
        )


def create_redis_client(settings: FluxiSettings) -> Redis:
    """Create the async Redis client used by the engine."""

    return Redis.from_url(settings.redis_url, decode_responses=False)
