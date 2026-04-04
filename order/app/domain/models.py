from msgspec import Struct


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    checkout_attempt_seq: int = 0
    active_checkout_attempt_no: int | None = None
    active_checkout_status: str | None = None
