import httpx
from fastapi import HTTPException

from models import REQ_ERROR_STR


class PaymentClient:
    def __init__(self, session: httpx.AsyncClient, gateway_url: str):
        self.session = session
        self.base_url = f"{gateway_url}/payment"

    async def pay(self, user_id: str, amount: int) -> httpx.Response:
        return await self._post(f"/pay/{user_id}/{amount}")

    async def _post(self, path: str) -> httpx.Response:
        try:
            return await self.session.post(f"{self.base_url}{path}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc
