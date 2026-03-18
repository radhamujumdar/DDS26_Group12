from enum import StrEnum

from msgspec import Struct, field


REQ_ERROR_STR = "Requests error order"


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


class TxMode(StrEnum):
    TWO_PC = "2pc"
    SAGA = "saga"


class SagaState(StrEnum):
    INIT = "INIT"
    RESERVING_STOCK = "RESERVING_STOCK"
    STOCK_RESERVED = "STOCK_RESERVED"
    DEBITTING_PAYMENT = "DEBITTING_PAYMENT"
    PAYMENT_DEBITED = "PAYMENT_DEBITED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


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


class SagaTxRecord(Struct):
    tx_id: str
    order_id: str
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]
    state: str
    created_at: float
    updated_at: float
    stock_reserved_items: list[tuple[str, int]] = field(default_factory=list)
    stock_released_items: list[tuple[str, int]] = field(default_factory=list)
    payment_debited: bool = False
    payment_refunded: bool = False
    attempts: int = 0
    error: str | None = None


class ParticipantResult(Struct):
    ok: bool
    retryable: bool = False
    detail: str | None = None
    correlation_id: str | None = None
    status: str | None = None
