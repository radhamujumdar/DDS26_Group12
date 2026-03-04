from enum import StrEnum

from msgspec import Struct


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


class TxState(StrEnum):
    INIT = "INIT"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"


class TxRecord(Struct):
    tx_id: str
    order_id: str
    state: str
    created_at: float
    updated_at: float
    error: str | None = None
