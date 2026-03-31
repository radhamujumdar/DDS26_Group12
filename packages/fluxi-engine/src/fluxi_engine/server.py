"""FastAPI control plane for the Fluxi engine."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

from .codecs import from_base64, to_base64
from .config import FluxiSettings, create_redis_client
from .models import RetryPolicyConfig, WorkflowTaskCommand
from .store import FluxiRedisStore


class RetryPolicyModel(BaseModel):
    max_attempts: int | None = Field(default=None, ge=1)
    initial_interval_ms: int | None = Field(default=None, ge=1)
    backoff_coefficient: float | None = Field(default=None, ge=1.0)
    max_interval_ms: int | None = Field(default=None, ge=1)

    def to_config(self) -> RetryPolicyConfig:
        return RetryPolicyConfig(
            max_attempts=self.max_attempts,
            initial_interval_ms=self.initial_interval_ms,
            backoff_coefficient=self.backoff_coefficient,
            max_interval_ms=self.max_interval_ms,
        )


class StartWorkflowRequest(BaseModel):
    workflow_id: str
    workflow_name: str
    task_queue: str
    start_policy: str = "attach_or_start"
    input_payload_b64: str | None = None


class StartWorkflowResponse(BaseModel):
    decision: str
    run_id: str | None
    status: str
    run_no: int | None
    result_payload_b64: str | None = None
    error_payload_b64: str | None = None


class WorkflowResultResponse(BaseModel):
    workflow_id: str
    run_id: str | None
    status: str
    run_no: int | None
    ready: bool
    result_payload_b64: str | None = None
    error_payload_b64: str | None = None


class WorkflowCommandModel(BaseModel):
    kind: Literal["schedule_activity", "complete_workflow", "fail_workflow"]
    activity_name: str | None = None
    activity_task_queue: str | None = None
    input_payload_b64: str | None = None
    retry_policy: RetryPolicyModel | None = None
    schedule_to_close_timeout_ms: int | None = Field(default=None, ge=1)
    result_payload_b64: str | None = None
    error_payload_b64: str | None = None

    @model_validator(mode="after")
    def validate_for_kind(self) -> WorkflowCommandModel:
        if self.kind == "schedule_activity":
            if self.activity_name is None or self.activity_task_queue is None:
                raise ValueError(
                    "schedule_activity requires activity_name and activity_task_queue."
                )
        elif self.kind == "complete_workflow":
            if self.result_payload_b64 is None:
                raise ValueError("complete_workflow requires result_payload_b64.")
        elif self.kind == "fail_workflow":
            if self.error_payload_b64 is None:
                raise ValueError("fail_workflow requires error_payload_b64.")
        return self

    def to_command(self) -> WorkflowTaskCommand:
        return WorkflowTaskCommand(
            kind=self.kind,
            activity_name=self.activity_name,
            activity_task_queue=self.activity_task_queue,
            input_payload=from_base64(self.input_payload_b64),
            retry_policy=self.retry_policy.to_config() if self.retry_policy else None,
            schedule_to_close_timeout_ms=self.schedule_to_close_timeout_ms,
            result_payload=from_base64(self.result_payload_b64),
            error_payload=from_base64(self.error_payload_b64),
        )


class WorkflowTaskCompletionRequest(BaseModel):
    run_id: str
    workflow_task_id: str
    attempt_no: int = Field(ge=1)
    command: WorkflowCommandModel


class WorkflowTaskCompletionResponse(BaseModel):
    outcome: str
    run_id: str | None = None
    activity_execution_id: str | None = None
    terminal_status: str | None = None


class ActivityTaskCompletionRequest(BaseModel):
    activity_execution_id: str
    attempt_no: int = Field(ge=1)
    status: Literal["completed", "failed"]
    result_payload_b64: str | None = None
    error_payload_b64: str | None = None

    @model_validator(mode="after")
    def validate_payload(self) -> ActivityTaskCompletionRequest:
        if self.status == "completed" and self.result_payload_b64 is None:
            raise ValueError("completed activity completions require result_payload_b64.")
        if self.status == "failed" and self.error_payload_b64 is None:
            raise ValueError("failed activity completions require error_payload_b64.")
        return self

    def payload_bytes(self) -> bytes | None:
        return from_base64(
            self.result_payload_b64
            if self.status == "completed"
            else self.error_payload_b64
        )


class ActivityTaskCompletionResponse(BaseModel):
    outcome: str
    run_id: str | None = None
    activity_execution_id: str | None = None
    next_workflow_task_id: str | None = None


def create_app(
    settings: FluxiSettings | None = None,
    store: FluxiRedisStore | None = None,
) -> FastAPI:
    settings = settings or FluxiSettings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        runtime_store = store
        if runtime_store is None:
            redis = create_redis_client(settings)
            runtime_store = FluxiRedisStore(redis, settings)
        app.state.store = runtime_store
        app.state.settings = settings
        try:
            yield
        finally:
            if store is None:
                await runtime_store.aclose()

    app = FastAPI(title="Fluxi Server", version="0.1.0", lifespan=lifespan)

    def normalize_payload(value: Any) -> Any:
        if isinstance(value, bytes):
            return to_base64(value)
        if isinstance(value, list):
            return [normalize_payload(item) for item in value]
        if isinstance(value, dict):
            return {key: normalize_payload(item) for key, item in value.items()}
        return value

    def get_store() -> FluxiRedisStore:
        return app.state.store

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> dict[str, str]:
        await get_store().redis.ping()
        return {"status": "ready"}

    @app.post("/workflows/start-or-attach", response_model=StartWorkflowResponse)
    async def start_or_attach_workflow(
        request: StartWorkflowRequest,
    ) -> StartWorkflowResponse:
        result = await get_store().start_or_attach_workflow(
            workflow_id=request.workflow_id,
            workflow_name=request.workflow_name,
            task_queue=request.task_queue,
            start_policy=request.start_policy,
            input_payload=from_base64(request.input_payload_b64),
        )
        return StartWorkflowResponse(
            decision=result.decision,
            run_id=result.run_id,
            status=result.status,
            run_no=result.run_no,
            result_payload_b64=to_base64(result.result_payload),
            error_payload_b64=to_base64(result.error_payload),
        )

    @app.get("/workflows/{workflow_id}/result", response_model=WorkflowResultResponse)
    async def get_workflow_result(
        workflow_id: str,
        wait_ms: int = Query(default=0, ge=0, le=60_000),
    ) -> WorkflowResultResponse:
        snapshot = (
            await get_store().wait_for_workflow_result(workflow_id, timeout_ms=wait_ms)
            if wait_ms > 0
            else await get_store().get_workflow_result(workflow_id)
        )
        if snapshot is None:
            raise HTTPException(status_code=404, detail="Unknown workflow ID.")
        return WorkflowResultResponse(
            workflow_id=snapshot.workflow_id,
            run_id=snapshot.run_id,
            status=snapshot.status,
            run_no=snapshot.run_no,
            ready=snapshot.status in {"completed", "failed"},
            result_payload_b64=to_base64(snapshot.result_payload),
            error_payload_b64=to_base64(snapshot.error_payload),
        )

    @app.post(
        "/workflow-tasks/complete",
        response_model=WorkflowTaskCompletionResponse,
    )
    async def complete_workflow_task(
        request: WorkflowTaskCompletionRequest,
    ) -> WorkflowTaskCompletionResponse:
        result = await get_store().complete_workflow_task(
            run_id=request.run_id,
            workflow_task_id=request.workflow_task_id,
            attempt_no=request.attempt_no,
            command=request.command.to_command(),
        )
        return WorkflowTaskCompletionResponse(
            outcome=result.outcome,
            run_id=result.run_id,
            activity_execution_id=result.activity_execution_id,
            terminal_status=result.terminal_status,
        )

    @app.post(
        "/activity-tasks/complete",
        response_model=ActivityTaskCompletionResponse,
    )
    async def complete_activity_task(
        request: ActivityTaskCompletionRequest,
    ) -> ActivityTaskCompletionResponse:
        result = await get_store().complete_activity_task(
            activity_execution_id=request.activity_execution_id,
            attempt_no=request.attempt_no,
            status=request.status,
            payload=request.payload_bytes(),
        )
        return ActivityTaskCompletionResponse(
            outcome=result.outcome,
            run_id=result.run_id,
            activity_execution_id=result.activity_execution_id,
            next_workflow_task_id=result.next_workflow_task_id,
        )

    @app.get("/runs/{run_id}/history")
    async def get_run_history(run_id: str) -> dict[str, Any]:
        events = await get_store().get_history(run_id)
        return {"events": normalize_payload(events)}

    @app.get("/runs/{run_id}/state")
    async def get_run_state(run_id: str) -> dict[str, Any]:
        state = await get_store().get_run_state(run_id)
        if not state:
            raise HTTPException(status_code=404, detail="Unknown run ID.")
        result: dict[str, Any] = {}
        for key, value in state.items():
            if key.endswith("_payload"):
                result[key] = to_base64(value if isinstance(value, bytes) else None)
            else:
                result[key] = value.decode("utf-8") if isinstance(value, bytes) else value
        return result

    return app
