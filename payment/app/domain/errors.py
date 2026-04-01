class UserNotFoundError(Exception):
    """Raised when a user does not exist."""


class InsufficientCreditError(Exception):
    """Raised when a charge would drive credit below zero."""
