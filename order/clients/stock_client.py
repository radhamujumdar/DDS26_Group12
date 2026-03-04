import httpx
from fastapi import HTTPException

from models import REQ_ERROR_STR


class StockClient:
    def __init__(self, session: httpx.AsyncClient, gateway_url: str):
        self.session = session
        self.base_url = f"{gateway_url}/stock"

    async def find_item(self, item_id: str) -> httpx.Response:
        return await self._get(f"/find/{item_id}")

    async def subtract(self, item_id: str, amount: int) -> httpx.Response:
        return await self._post(f"/subtract/{item_id}/{amount}")

    async def add(self, item_id: str, amount: int) -> httpx.Response:
        return await self._post(f"/add/{item_id}/{amount}")

    async def _post(self, path: str) -> httpx.Response:
        try:
            return await self.session.post(f"{self.base_url}{path}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc

    async def _get(self, path: str) -> httpx.Response:
        try:
            return await self.session.get(f"{self.base_url}{path}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc
