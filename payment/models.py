from enum import StrEnum

from msgspec import Struct


DB_ERROR_STR = "DB error"


class TxnState(StrEnum):
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"


class RecoveryAction(StrEnum):
    COMMIT = "commit"
    ABORT = "abort"


class SagaDebitState(StrEnum):
    DEBITED = "debited"
    REFUNDED = "refunded"
    FAILED = "failed"


class UserValue(Struct):
    credit: int


class PrepareRecord(Struct):
    txn_id: str
    user_id: str
    delta: int
    old_credit: int
    new_credit: int
    state: TxnState


class SagaDebitRecord(Struct):
    tx_id: str
    user_id: str
    amount: int
    old_credit: int
    new_credit: int
    state: SagaDebitState
