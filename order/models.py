from enum import StrEnum

from msgspec import Struct, field


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
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]
    state: str
    created_at: float
    updated_at: float
    stock_prepared_items: list[tuple[str, int]] = field(default_factory=list)
    stock_committed_items: list[tuple[str, int]] = field(default_factory=list)
    payment_prepared: bool = False
    payment_committed: bool = False
    attempts: int = 0
    error: str | None = None


class ParticipantResult(Struct):
    ok: bool
    retryable: bool = False
    detail: str | None = None
