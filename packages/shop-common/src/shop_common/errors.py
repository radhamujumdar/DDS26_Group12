"""Shared infrastructure-level exceptions for the shopping-cart services."""


class DatabaseError(Exception):
    """Raised when a service cannot read or write its Redis store."""


class UpstreamServiceError(Exception):
    """Raised when a service cannot reach a required upstream dependency."""
