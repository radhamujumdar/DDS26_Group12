from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fluxi_sdk import activity
from shop_common.checkout import PaymentDeclinedError, PaymentReceipt

from ..domain.errors import InsufficientCreditError, UserNotFoundError
from ..repositories.payment_repository import PaymentRepository


class PaymentService:
    def __init__(self, repository: PaymentRepository) -> None:
        self._repository = repository

    async def create_user(self) -> str:
        return await self._repository.create_user()

    async def batch_init(self, *, count: int, starting_money: int) -> None:
        await self._repository.batch_init(count=count, starting_money=starting_money)

    async def find_user(self, user_id: str) -> dict[str, int | str]:
        entry = await self._repository.get_user(user_id)
        return {"user_id": user_id, "credit": entry.credit}

    async def add_funds(self, user_id: str, amount: int) -> int:
        entry = await self._repository.add_funds(user_id, amount)
        return entry.credit

    async def pay(self, user_id: str, amount: int) -> int:
        entry = await self._repository.pay(user_id, amount)
        return entry.credit

    async def charge_payment(
        self,
        user_id: str,
        amount: int,
        *,
        activity_execution_id: str,
    ) -> PaymentReceipt:
        try:
            return await self._repository.charge_payment_idempotent(
                user_id,
                amount=amount,
                activity_execution_id=activity_execution_id,
            )
        except (UserNotFoundError, InsufficientCreditError) as exc:
            raise PaymentDeclinedError(str(exc)) from exc


def create_payment_activities(
    payment_service: PaymentService,
) -> tuple[Callable[..., Any]]:
    @activity.defn(name="charge_payment")
    async def charge_payment(user_id: str, amount: int) -> PaymentReceipt:
        return await payment_service.charge_payment(
            user_id,
            amount,
            activity_execution_id=activity.info().activity_execution_id,
        )

    return (charge_payment,)
