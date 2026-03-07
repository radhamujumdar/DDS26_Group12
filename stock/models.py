from enum import StrEnum

from msgspec import Struct


DB_ERROR_STR = "DB error"


class ReservationState(StrEnum):
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"


class RecoveryAction(StrEnum):
    COMMIT = "commit"
    ABORT = "abort"


class StockValue(Struct):
    stock: int
    price: int


class Reservation(Struct):
    tx_id: str
    item_id: str
    amount: int
    state: ReservationState
