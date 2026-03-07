from typing import Annotated

from fastapi import APIRouter, Depends, Path, Request
from fastapi.responses import PlainTextResponse

from services.payment_service import PaymentService


NonEmptyStr = Annotated[str, Path(min_length=1)]
IntValue = Annotated[int, Path()]

router = APIRouter()


def get_payment_service(request: Request) -> PaymentService:
    return request.app.state.payment_service


@router.post("/2pc/prepare/{txn_id}/{user_id}/{amount}")
async def prepare(
    txn_id: NonEmptyStr,
    user_id: NonEmptyStr,
    amount: IntValue,
    service: PaymentService = Depends(get_payment_service),
):
    return await service.prepare(txn_id, user_id, amount)


@router.post("/2pc/commit/{txn_id}")
async def commit(
    txn_id: NonEmptyStr,
    service: PaymentService = Depends(get_payment_service),
):
    return await service.commit(txn_id)


@router.post("/2pc/abort/{txn_id}")
async def abort_txn(
    txn_id: NonEmptyStr,
    service: PaymentService = Depends(get_payment_service),
):
    return await service.abort(txn_id)


@router.post("/saga/debit/{txn_id}/{user_id}/{amount}")
async def saga_debit(
    txn_id: NonEmptyStr,
    user_id: NonEmptyStr,
    amount: IntValue,
    service: PaymentService = Depends(get_payment_service),
):
    return await service.saga_debit(txn_id=txn_id, user_id=user_id, amount=amount)


@router.post("/saga/refund/{txn_id}")
async def saga_refund(
    txn_id: NonEmptyStr,
    service: PaymentService = Depends(get_payment_service),
):
    return await service.saga_refund(txn_id=txn_id)


@router.post("/create_user")
async def create_user(service: PaymentService = Depends(get_payment_service)):
    return await service.create_user()


@router.post("/batch_init/{n}/{starting_money}")
async def batch_init_users(
    n: IntValue,
    starting_money: IntValue,
    service: PaymentService = Depends(get_payment_service),
):
    return await service.batch_init_users(n=n, starting_money=starting_money)


@router.get("/find_user/{user_id}")
async def find_user(
    user_id: NonEmptyStr,
    service: PaymentService = Depends(get_payment_service),
):
    return await service.find_user(user_id)


@router.post("/add_funds/{user_id}/{amount}")
async def add_credit(
    user_id: NonEmptyStr,
    amount: IntValue,
    service: PaymentService = Depends(get_payment_service),
):
    credit = await service.add_funds(user_id, amount)
    return PlainTextResponse(f"User: {user_id} credit updated to: {credit}", status_code=200)


@router.post("/pay/{user_id}/{amount}")
async def remove_credit(
    user_id: NonEmptyStr,
    amount: IntValue,
    service: PaymentService = Depends(get_payment_service),
):
    credit = await service.pay(user_id, amount)
    return PlainTextResponse(f"User: {user_id} credit updated to: {credit}", status_code=200)
