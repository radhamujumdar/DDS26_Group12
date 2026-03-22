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


class UnknownWorkflowError(RegistrationError):
    """Raised when a referenced workflow is not registered."""


class UnknownActivityError(RegistrationError):
    """Raised when a referenced activity is not registered."""


class WorkflowContextError(FluxiError):
    """Raised when workflow-only APIs are used from the wrong context."""


class WorkflowContextUnavailableError(WorkflowContextError):
    """Raised when workflow execution context is not available."""


class WorkflowAlreadyStartedError(FluxiError):
    """Raised when a workflow key collides with a start policy."""
