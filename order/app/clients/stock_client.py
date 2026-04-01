from __future__ import annotations

from dataclasses import dataclass

import httpx

from shop_common.errors import UpstreamServiceError


class StockItemNotFoundError(Exception):
    """Raised when the stock service does not know the requested item."""


@dataclass(frozen=True, slots=True)
class StockItemView:
    stock: int
    price: int


class StockGatewayClient:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def find_item(self, item_id: str) -> StockItemView:
        try:
            response = await self._client.get(f"/stock/find/{item_id}")
        except httpx.HTTPError as exc:
            raise UpstreamServiceError("Requests error") from exc

        if response.status_code != 200:
            raise StockItemNotFoundError(f"Item: {item_id} does not exist!")

        payload = response.json()
        return StockItemView(
            stock=int(payload["stock"]),
            price=int(payload["price"]),
        )
