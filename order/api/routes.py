import random
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from fastapi.responses import PlainTextResponse
from msgspec import msgpack

from clients.saga_bus import SagaCommandBus
from clients.stock_client import StockClient
from coordinator.saga import SagaCoordinator
from coordinator.two_pc import TwoPCCoordinator
from models import OrderValue, TxMode
from repository.order_repo import OrderRepository


NonEmptyStr = Annotated[str, Path(min_length=1)]
IntValue = Annotated[int, Path()]

router = APIRouter()


def get_order_repo(request: Request) -> OrderRepository:
    return request.app.state.order_repo


def get_stock_client(request: Request) -> StockClient:
    return request.app.state.stock_client


def get_two_pc_coordinator(request: Request) -> TwoPCCoordinator:
    return request.app.state.coordinator


def get_saga_coordinator(request: Request) -> SagaCoordinator:
    return request.app.state.saga_coordinator


def get_saga_bus(request: Request) -> SagaCommandBus | None:
    return request.app.state.saga_bus


def get_tx_mode(request: Request) -> str:
    return request.app.state.tx_mode


@router.post("/create/{user_id}")
async def create_order(
    user_id: NonEmptyStr,
    order_repo: OrderRepository = Depends(get_order_repo),
):
    key = await order_repo.create_order(user_id)
    return {"order_id": key}


@router.post("/batch_init/{n}/{n_items}/{n_users}/{item_price}")
async def batch_init_users(
    n: IntValue,
    n_items: IntValue,
    n_users: IntValue,
    item_price: IntValue,
    order_repo: OrderRepository = Depends(get_order_repo),
):
    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        return OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )

    kv_pairs: dict[str, bytes] = {f"{index}": msgpack.encode(generate_entry()) for index in range(n)}
    await order_repo.batch_set_orders(kv_pairs)
    return {"msg": "Batch init for orders successful"}


@router.get("/find/{order_id}")
async def find_order(
    order_id: NonEmptyStr,
    order_repo: OrderRepository = Depends(get_order_repo),
):
    order_entry = await order_repo.get_order(order_id)
    return {
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
    }


@router.post("/addItem/{order_id}/{item_id}/{quantity}")
async def add_item(
    order_id: NonEmptyStr,
    item_id: NonEmptyStr,
    quantity: IntValue,
    order_repo: OrderRepository = Depends(get_order_repo),
    stock_client: StockClient = Depends(get_stock_client),
):
    if quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be greater than zero")

    order_entry = await order_repo.get_order(order_id)
    if order_entry.paid:
        raise HTTPException(status_code=400, detail=f"Order: {order_id} is already paid")

    item_reply = await stock_client.find_item(item_id)
    if item_reply.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} does not exist!")

    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, quantity))
    order_entry.total_cost += quantity * item_json["price"]
    await order_repo.save_order(order_id, order_entry)
    return PlainTextResponse(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status_code=200,
    )


@router.post("/checkout/{order_id}")
async def checkout(
    order_id: NonEmptyStr,
    order_repo: OrderRepository = Depends(get_order_repo),
    tx_mode: str = Depends(get_tx_mode),
    coordinator: TwoPCCoordinator = Depends(get_two_pc_coordinator),
    saga_coordinator: SagaCoordinator = Depends(get_saga_coordinator),
):
    order_entry = await order_repo.get_order(order_id)
    if tx_mode == TxMode.SAGA.value:
        await saga_coordinator.checkout(order_id, order_entry)
    else:
        await coordinator.checkout(order_id, order_entry)
    return PlainTextResponse("Checkout successful", status_code=200)


@router.get("/2pc/tx/{tx_id}")
async def get_tx_state(
    tx_id: NonEmptyStr,
    coordinator: TwoPCCoordinator = Depends(get_two_pc_coordinator),
):
    tx = await coordinator.tx_repo.get(tx_id)
    if tx is None:
        raise HTTPException(status_code=400, detail=f"Transaction {tx_id} not found")
    return {"tx_id": tx.tx_id, "state": tx.state}


@router.get("/saga/tx/{tx_id}")
async def get_saga_tx_state(
    tx_id: NonEmptyStr,
    saga_coordinator: SagaCoordinator = Depends(get_saga_coordinator),
):
    tx = await saga_coordinator.saga_repo.get(tx_id)
    if tx is None:
        raise HTTPException(status_code=400, detail=f"Saga transaction {tx_id} not found")
    return {"tx_id": tx.tx_id, "state": tx.state}


@router.get("/saga/metrics")
async def get_saga_metrics(
    saga_bus: SagaCommandBus | None = Depends(get_saga_bus),
    saga_coordinator: SagaCoordinator = Depends(get_saga_coordinator),
    tx_mode: str = Depends(get_tx_mode),
):
    if saga_bus is None:
        raise HTTPException(status_code=400, detail="Saga mode is disabled")
    snapshot = await saga_bus.get_metrics_snapshot()
    active_ids = await saga_coordinator.saga_repo.list_active()
    snapshot["active_saga_transactions"] = len(active_ids)
    snapshot["tx_mode"] = tx_mode
    return snapshot
