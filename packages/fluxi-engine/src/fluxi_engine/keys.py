"""Redis key naming helpers."""

from __future__ import annotations


def workflow_control(prefix: str, workflow_id: str) -> str:
    return f"{prefix}:workflow:{workflow_id}:control"


def workflow_state(prefix: str, run_id: str) -> str:
    return f"{prefix}:run:{run_id}:state"


def workflow_history(prefix: str, run_id: str) -> str:
    return f"{prefix}:run:{run_id}:history"


def workflow_queue(prefix: str, task_queue: str) -> str:
    return f"{prefix}:queue:workflow:{task_queue}"


def activity_queue(prefix: str, task_queue: str) -> str:
    return f"{prefix}:queue:activity:{task_queue}"


def activity_state(prefix: str, activity_execution_id: str) -> str:
    return f"{prefix}:activity:{activity_execution_id}"


def timers(prefix: str) -> str:
    return f"{prefix}:timers"


def workflow_task_queues(prefix: str) -> str:
    return f"{prefix}:task-queues:workflow"


def activity_task_queues(prefix: str) -> str:
    return f"{prefix}:task-queues:activity"
