"""SDK-specific exceptions exposed as part of the public contract."""


class FluxiError(Exception):
    """Base exception for Fluxi SDK failures."""


class DefinitionError(FluxiError):
    """Raised when a workflow or activity definition is invalid."""


class InvalidWorkflowDefinitionError(DefinitionError):
    """Raised when a workflow declaration is invalid."""


class InvalidActivityDefinitionError(DefinitionError):
    """Raised when an activity declaration is invalid."""


class RegistrationError(FluxiError):
    """Raised for registration-related failures."""


class DuplicateWorkflowRegistrationError(RegistrationError):
    """Raised when a workflow is registered more than once."""


class DuplicateActivityRegistrationError(RegistrationError):
    """Raised when an activity is registered more than once."""


class UnknownWorkflowError(RegistrationError):
    """Raised when a referenced workflow is not registered."""


class UnknownActivityError(RegistrationError):
    """Raised when a referenced activity is not registered."""


class WorkflowContextError(FluxiError):
    """Raised when workflow-only APIs are used from the wrong context."""


class WorkflowContextUnavailableError(WorkflowContextError):
    """Raised when workflow execution context is not available."""


class ActivityContextError(FluxiError):
    """Raised when activity-only APIs are used from the wrong context."""


class ActivityContextUnavailableError(ActivityContextError):
    """Raised when activity execution context is not available."""


class WorkflowAlreadyStartedError(FluxiError):
    """Raised when a workflow key collides with a start policy."""


class RemoteExecutionError(FluxiError):
    """Raised when a remote workflow or activity fails without local rehydration."""

    def __init__(
        self,
        message: str,
        *,
        remote_module: str | None = None,
        remote_qualname: str | None = None,
        remote_args: tuple[object, ...] = (),
    ) -> None:
        super().__init__(message)
        self.remote_module = remote_module
        self.remote_qualname = remote_qualname
        self.remote_args = remote_args


class RemoteWorkflowError(RemoteExecutionError):
    """Raised when a remote workflow fails and cannot be rehydrated locally."""


class RemoteActivityError(RemoteExecutionError):
    """Raised when a remote activity fails and cannot be rehydrated locally."""


class NonDeterministicWorkflowError(RemoteWorkflowError):
    """Raised when workflow code does not match the stored event history."""


class WorkerError(FluxiError):
    """Raised for worker lifecycle and task-queue availability failures."""


class NoWorkflowWorkerAvailableError(WorkerError):
    """Raised when no running worker can execute a workflow on a task queue."""


class NoActivityWorkerAvailableError(WorkerError):
    """Raised when no running worker can execute an activity on a task queue."""
