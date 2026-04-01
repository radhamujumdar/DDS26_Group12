class StockItemNotFoundError(Exception):
    """Raised when a stock item does not exist."""


class StockInsufficientError(Exception):
    """Raised when an operation would drive stock below zero."""
