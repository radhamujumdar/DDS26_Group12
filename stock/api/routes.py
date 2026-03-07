from typing import Annotated

from fastapi import APIRouter, Depends, Path, Request
from fastapi.responses import PlainTextResponse

from services.stock_service import StockService


NonEmptyStr = Annotated[str, Path(min_length=1)]
IntValue = Annotated[int, Path()]

router = APIRouter()


def get_stock_service(request: Request) -> StockService:
    return request.app.state.stock_service


@router.post("/item/create/{price}")
async def create_item(
    price: IntValue,
    service: StockService = Depends(get_stock_service),
):
    return await service.create_item(price)


@router.post("/batch_init/{n}/{starting_stock}/{item_price}")
async def batch_init_items(
    n: IntValue,
    starting_stock: IntValue,
    item_price: IntValue,
    service: StockService = Depends(get_stock_service),
):
    return await service.batch_init(n=n, starting_stock=starting_stock, item_price=item_price)


@router.get("/find/{item_id}")
async def find_item(
    item_id: NonEmptyStr,
    service: StockService = Depends(get_stock_service),
):
    return await service.find_item(item_id)


@router.post("/add/{item_id}/{amount}")
async def add_stock(
    item_id: NonEmptyStr,
    amount: IntValue,
    service: StockService = Depends(get_stock_service),
):
    updated = await service.add_stock(item_id=item_id, amount=amount)
    return PlainTextResponse(f"Item: {item_id} stock updated to: {updated}", status_code=200)


@router.post("/subtract/{item_id}/{amount}")
async def remove_stock(
    item_id: NonEmptyStr,
    amount: IntValue,
    service: StockService = Depends(get_stock_service),
):
    updated = await service.remove_stock(item_id=item_id, amount=amount)
    return PlainTextResponse(f"Item: {item_id} stock updated to: {updated}", status_code=200)


@router.post("/2pc/prepare/{tx_id}/{item_id}/{amount}")
async def prepare(
    tx_id: NonEmptyStr,
    item_id: NonEmptyStr,
    amount: IntValue,
    service: StockService = Depends(get_stock_service),
):
    return await service.prepare(tx_id=tx_id, item_id=item_id, amount=amount)


@router.post("/2pc/commit/{tx_id}/{item_id}/{amount}")
async def commit(
    tx_id: NonEmptyStr,
    item_id: NonEmptyStr,
    amount: IntValue,
    service: StockService = Depends(get_stock_service),
):
    return await service.commit(tx_id=tx_id, item_id=item_id, amount=amount)


@router.post("/2pc/abort/{tx_id}/{item_id}/{amount}")
async def abort_txn(
    tx_id: NonEmptyStr,
    item_id: NonEmptyStr,
    amount: IntValue,
    service: StockService = Depends(get_stock_service),
):
    return await service.abort(tx_id=tx_id, item_id=item_id, amount=amount)
