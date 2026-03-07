import os

import httpx


BASE_URL = os.environ.get("DDS_BASE_URL", "http://localhost:8000")


async def create_user(client: httpx.AsyncClient, credit: int = 0) -> str:
    response = await client.post(f"{BASE_URL}/payment/create_user")
    assert response.status_code == 200, response.text
    user_id = response.json()["user_id"]
    if credit > 0:
        add_funds = await client.post(f"{BASE_URL}/payment/add_funds/{user_id}/{credit}")
        assert add_funds.status_code == 200, add_funds.text
    return user_id


async def create_item(client: httpx.AsyncClient, price: int, stock: int = 0) -> str:
    response = await client.post(f"{BASE_URL}/stock/item/create/{price}")
    assert response.status_code == 200, response.text
    item_id = response.json()["item_id"]
    if stock > 0:
        add_stock = await client.post(f"{BASE_URL}/stock/add/{item_id}/{stock}")
        assert add_stock.status_code == 200, add_stock.text
    return item_id


async def create_order(client: httpx.AsyncClient, user_id: str) -> str:
    response = await client.post(f"{BASE_URL}/orders/create/{user_id}")
    assert response.status_code == 200, response.text
    return response.json()["order_id"]


async def add_item_to_order(
    client: httpx.AsyncClient,
    order_id: str,
    item_id: str,
    quantity: int,
) -> None:
    response = await client.post(f"{BASE_URL}/orders/addItem/{order_id}/{item_id}/{quantity}")
    assert response.status_code == 200, response.text


async def get_credit(client: httpx.AsyncClient, user_id: str) -> int:
    response = await client.get(f"{BASE_URL}/payment/find_user/{user_id}")
    assert response.status_code == 200, response.text
    return response.json()["credit"]


async def get_stock(client: httpx.AsyncClient, item_id: str) -> int:
    response = await client.get(f"{BASE_URL}/stock/find/{item_id}")
    assert response.status_code == 200, response.text
    return response.json()["stock"]


async def get_order(client: httpx.AsyncClient, order_id: str) -> dict:
    response = await client.get(f"{BASE_URL}/orders/find/{order_id}")
    assert response.status_code == 200, response.text
    return response.json()
