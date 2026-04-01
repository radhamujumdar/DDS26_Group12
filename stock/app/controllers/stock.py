from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from shop_common.errors import DatabaseError
from shop_common.http import MessageResponse

from ..dependencies import get_stock_service
from ..domain.errors import StockInsufficientError, StockItemNotFoundError
from ..services.stock_service import StockService


router = APIRouter()


class CreateItemResponse(BaseModel):
    item_id: str


class StockItemResponse(BaseModel):
    stock: int
    price: int


def _database_error(exc: DatabaseError) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.post("/item/create/{price}", response_model=CreateItemResponse)
async def create_item(
    price: int,
    service: StockService = Depends(get_stock_service),
) -> CreateItemResponse:
    try:
        item_id = await service.create_item(price)
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return CreateItemResponse(item_id=item_id)


@router.post("/batch_init/{n}/{starting_stock}/{item_price}", response_model=MessageResponse)
async def batch_init_stock(
    n: int,
    starting_stock: int,
    item_price: int,
    service: StockService = Depends(get_stock_service),
) -> MessageResponse:
    try:
        await service.batch_init(
            count=n,
            starting_stock=starting_stock,
            item_price=item_price,
        )
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return MessageResponse(msg="Batch init for stock successful")


@router.get("/find/{item_id}", response_model=StockItemResponse)
async def find_item(
    item_id: str,
    service: StockService = Depends(get_stock_service),
) -> StockItemResponse:
    try:
        payload = await service.find_item(item_id)
    except StockItemNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!") from exc
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return StockItemResponse(**payload)


@router.post("/add/{item_id}/{amount}", response_class=PlainTextResponse)
async def add_stock(
    item_id: str,
    amount: int,
    service: StockService = Depends(get_stock_service),
) -> PlainTextResponse:
    try:
        stock = await service.add_stock(item_id, amount)
    except StockItemNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!") from exc
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return PlainTextResponse(f"Item: {item_id} stock updated to: {stock}", status_code=200)


@router.post("/subtract/{item_id}/{amount}", response_class=PlainTextResponse)
async def subtract_stock(
    item_id: str,
    amount: int,
    service: StockService = Depends(get_stock_service),
) -> PlainTextResponse:
    try:
        stock = await service.subtract_stock(item_id, amount)
    except StockItemNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!") from exc
    except StockInsufficientError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return PlainTextResponse(f"Item: {item_id} stock updated to: {stock}", status_code=200)
