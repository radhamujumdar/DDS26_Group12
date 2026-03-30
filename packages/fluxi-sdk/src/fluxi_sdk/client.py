"""Public workflow client contract."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import os
from typing import Any, Callable, Protocol

from .types import StartPolicy, WorkflowReference
from .workflow import _get_workflow_name


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
class EngineConnectionConfig:
    server_url: str = "http://127.0.0.1:8000"
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
    result_poll_interval_ms: int = 100

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

        object.__setattr__(
            self,
            "sentinel_username",
            _normalize_optional_text(self.sentinel_username),
        )
        object.__setattr__(
            self,
            "sentinel_password",
            _normalize_optional_text(self.sentinel_password),
        )

        if self.sentinel_min_other_sentinels < 0:
            raise ValueError("sentinel_min_other_sentinels must be at least 0.")

        if mode == "sentinel":
            if not sentinel_service_name:
                raise ValueError(
                    "sentinel_service_name must be a non-empty string in sentinel mode."
                )
            _parse_sentinel_endpoints(sentinel_endpoints)

    @classmethod
    def from_env(cls) -> EngineConnectionConfig:
        defaults = cls()
        return cls(
            server_url=_env("FLUXI_SERVER_URL", defaults.server_url),
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
            result_poll_interval_ms=int(
                _env(
                    "FLUXI_RESULT_POLL_INTERVAL_MS",
                    str(defaults.result_poll_interval_ms),
                )
            ),
        )


class _WorkerBindingBackend(Protocol):
    async def start(self) -> None: ...

    async def wait(self) -> None: ...

    async def shutdown(self) -> None: ...


class _WorkflowClientBackend(Protocol):
    async def _execute_workflow(
        self,
        workflow: WorkflowReference,
        *,
        workflow_id: str,
        task_queue: str,
        args: tuple[Any, ...],
        start_policy: StartPolicy,
    ) -> Any: ...

    def _create_worker_binding(
        self,
        *,
        task_queue: str,
        workflows: Sequence[type[Any]],
        activities: Sequence[Callable[..., Any]],
    ) -> _WorkerBindingBackend: ...


class WorkflowClient:
    """Concrete client facade backed by either the fake runtime or the engine."""

    def __init__(self, backend: _WorkflowClientBackend) -> None:
        self._backend = backend

    @classmethod
    def connect(
        cls,
        *,
        runtime: _WorkflowClientBackend | None = None,
        engine: EngineConnectionConfig | None = None,
    ) -> WorkflowClient:
        if runtime is not None and engine is not None:
            raise ValueError("Pass either runtime= or engine=, not both.")
        if runtime is not None:
            return cls(runtime)
        if engine is not None:
            from ._engine_backend import EngineWorkflowBackend

            return cls(EngineWorkflowBackend(engine))
        raise TypeError("WorkflowClient.connect() requires runtime= or engine=.")

    async def execute_workflow(
        self,
        workflow: WorkflowReference,
        *workflow_args: Any,
        id: str | None = None,
        task_queue: str | None = None,
        start_policy: StartPolicy = StartPolicy.ATTACH_OR_START,
        workflow_key: str | None = None,
        args: Sequence[Any] | None = None,
    ) -> Any:
        """Execute or attach to a workflow using the provided start policy."""

        workflow_id = _coalesce_workflow_id(id, workflow_key)
        if task_queue is None:
            raise TypeError(
                "execute_workflow() missing required keyword argument: 'task_queue'"
            )

        normalized_args = _normalize_workflow_args(workflow_args, args)
        return await self._backend._execute_workflow(
            workflow,
            workflow_id=workflow_id,
            task_queue=task_queue,
            args=normalized_args,
            start_policy=start_policy,
        )

    def _create_worker_binding(
        self,
        *,
        task_queue: str,
        workflows: Sequence[type[Any]],
        activities: Sequence[Callable[..., Any]],
    ) -> _WorkerBindingBackend:
        return self._backend._create_worker_binding(
            task_queue=task_queue,
            workflows=workflows,
            activities=activities,
        )

    def _resolve_workflow_name(
        self,
        workflow: WorkflowReference,
    ) -> str:
        return _get_workflow_name(workflow)


def _coalesce_workflow_id(id: str | None, workflow_key: str | None) -> str:
    if id is not None and workflow_key is not None and id != workflow_key:
        raise ValueError("Use either id or workflow_key, not both with different values.")
    workflow_id = id or workflow_key
    if workflow_id is None:
        raise TypeError("execute_workflow() missing required keyword argument: 'id'")
    if not workflow_id.strip():
        raise ValueError("Workflow id must be a non-empty string.")
    return workflow_id


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _normalize_workflow_args(
    workflow_args: tuple[Any, ...],
    args: Sequence[Any] | None,
) -> tuple[Any, ...]:
    if args is not None and workflow_args:
        raise ValueError("Use positional workflow args or args=, not both.")
    if args is not None:
        return tuple(args)
    return workflow_args


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


__all__ = ["EngineConnectionConfig", "WorkflowClient"]
