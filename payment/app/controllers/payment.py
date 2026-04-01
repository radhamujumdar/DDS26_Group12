from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from shop_common.errors import DatabaseError
from shop_common.http import MessageResponse

from ..dependencies import get_payment_service
from ..domain.errors import InsufficientCreditError, UserNotFoundError
from ..services.payment_service import PaymentService


router = APIRouter()


class CreateUserResponse(BaseModel):
    user_id: str


class UserResponse(BaseModel):
    user_id: str
    credit: int


def _database_error(exc: DatabaseError) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.post("/create_user", response_model=CreateUserResponse)
async def create_user(
    service: PaymentService = Depends(get_payment_service),
) -> CreateUserResponse:
    try:
        user_id = await service.create_user()
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return CreateUserResponse(user_id=user_id)


@router.post("/batch_init/{n}/{starting_money}", response_model=MessageResponse)
async def batch_init_users(
    n: int,
    starting_money: int,
    service: PaymentService = Depends(get_payment_service),
) -> MessageResponse:
    try:
        await service.batch_init(count=n, starting_money=starting_money)
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return MessageResponse(msg="Batch init for users successful")


@router.get("/find_user/{user_id}", response_model=UserResponse)
async def find_user(
    user_id: str,
    service: PaymentService = Depends(get_payment_service),
) -> UserResponse:
    try:
        payload = await service.find_user(user_id)
    except UserNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!") from exc
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return UserResponse(**payload)


@router.post("/add_funds/{user_id}/{amount}", response_class=PlainTextResponse)
async def add_funds(
    user_id: str,
    amount: int,
    service: PaymentService = Depends(get_payment_service),
) -> PlainTextResponse:
    try:
        credit = await service.add_funds(user_id, amount)
    except UserNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!") from exc
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return PlainTextResponse(
        f"User: {user_id} credit updated to: {credit}",
        status_code=200,
    )


@router.post("/pay/{user_id}/{amount}", response_class=PlainTextResponse)
async def pay(
    user_id: str,
    amount: int,
    service: PaymentService = Depends(get_payment_service),
) -> PlainTextResponse:
    try:
        credit = await service.pay(user_id, amount)
    except UserNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!") from exc
    except InsufficientCreditError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return PlainTextResponse(
        f"User: {user_id} credit updated to: {credit}",
        status_code=200,
    )
