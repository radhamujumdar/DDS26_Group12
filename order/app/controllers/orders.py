from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from shop_common.checkout import OrderNotFoundError, PaymentDeclinedError, StockUnavailableError
from shop_common.errors import DatabaseError, UpstreamServiceError
from shop_common.http import MessageResponse

from ..clients.stock_client import StockItemNotFoundError
from ..dependencies import get_checkout_service, get_order_service
from ..services.checkout_service import CheckoutService
from ..services.order_service import OrderService


router = APIRouter()


class CreateOrderResponse(BaseModel):
    order_id: str


class OrderResponse(BaseModel):
    order_id: str
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def _database_error(exc: DatabaseError) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


def _service_unavailable_error(exc: Exception) -> HTTPException:
    return HTTPException(status_code=503, detail=str(exc))


@router.post("/create/{user_id}", response_model=CreateOrderResponse)
async def create_order(
    user_id: str,
    service: OrderService = Depends(get_order_service),
) -> CreateOrderResponse:
    try:
        return CreateOrderResponse(order_id=await service.create_order(user_id))
    except DatabaseError as exc:
        raise _database_error(exc) from exc


@router.post(
    "/batch_init/{n}/{n_items}/{n_users}/{item_price}",
    response_model=MessageResponse,
)
async def batch_init_orders(
    n: int,
    n_items: int,
    n_users: int,
    item_price: int,
    service: OrderService = Depends(get_order_service),
) -> MessageResponse:
    try:
        await service.batch_init(
            count=n,
            n_items=n_items,
            n_users=n_users,
            item_price=item_price,
        )
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return MessageResponse(msg="Batch init for orders successful")


@router.get("/find/{order_id}", response_model=OrderResponse)
async def find_order(
    order_id: str,
    service: OrderService = Depends(get_order_service),
) -> OrderResponse:
    try:
        payload = await service.find_order(order_id)
    except OrderNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!") from exc
    except DatabaseError as exc:
        raise _database_error(exc) from exc
    return OrderResponse(**payload)


@router.post("/addItem/{order_id}/{item_id}/{quantity}", response_class=PlainTextResponse)
async def add_item_to_order(
    order_id: str,
    item_id: str,
    quantity: int,
    service: OrderService = Depends(get_order_service),
) -> PlainTextResponse:
    try:
        total_cost = await service.add_item(order_id, item_id=item_id, quantity=quantity)
    except OrderNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!") from exc
    except StockItemNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except UpstreamServiceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DatabaseError as exc:
        raise _database_error(exc) from exc

    return PlainTextResponse(
        f"Item: {item_id} added to: {order_id} price updated to: {total_cost}",
        status_code=200,
    )


@router.post("/checkout/{order_id}", response_class=PlainTextResponse)
async def checkout_order(
    order_id: str,
    service: CheckoutService = Depends(get_checkout_service),
) -> PlainTextResponse:
    try:
        result = await service.checkout(order_id)
    except StockUnavailableError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PaymentDeclinedError as exc:
        raise HTTPException(status_code=400, detail="User out of credit") from exc
    except OrderNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!") from exc
    except DatabaseError as exc:
        raise _service_unavailable_error(exc) from exc
    except UpstreamServiceError as exc:
        raise _service_unavailable_error(exc) from exc
    return PlainTextResponse(
        CheckoutService.to_http_response_text(result),
        status_code=200,
    )
