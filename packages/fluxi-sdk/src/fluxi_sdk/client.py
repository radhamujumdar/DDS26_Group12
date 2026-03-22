"""Public workflow client contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from .types import StartPolicy, WorkflowReference


class WorkflowClient(ABC):
    """Abstract client interface for executing workflows."""

    @abstractmethod
    async def execute_workflow(
        self,
        workflow: WorkflowReference,
        *,
        workflow_key: str,
        args: Sequence[Any] = (),
        start_policy: StartPolicy = StartPolicy.ATTACH_OR_START,
    ) -> Any:
        """Execute or attach to a workflow using the provided start policy."""


__all__ = ["WorkflowClient"]
