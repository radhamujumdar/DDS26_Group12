import httpx
from fastapi import HTTPException

from models import ParticipantResult, REQ_ERROR_STR


class StockClient:
    def __init__(self, session: httpx.AsyncClient, gateway_url: str):
        self.session = session
        self.base_url = f"{gateway_url}/stock"

    async def find_item(self, item_id: str) -> httpx.Response:
        return await self._safe_get(f"/find/{item_id}")

    async def prepare_item(self, tx_id: str, item_id: str, amount: int) -> ParticipantResult:
        response = await self._safe_post(f"/2pc/prepare/{tx_id}/{item_id}/{amount}")
        return self._status_to_result(response)

    async def commit_item(self, tx_id: str, item_id: str, amount: int) -> ParticipantResult:
        response = await self._safe_post(f"/2pc/commit/{tx_id}/{item_id}/{amount}")
        return self._status_to_result(response)

    async def abort_item(self, tx_id: str, item_id: str, amount: int) -> ParticipantResult:
        response = await self._safe_post(f"/2pc/abort/{tx_id}/{item_id}/{amount}")
        return self._status_to_result(response)

    async def saga_reserve_item(self, tx_id: str, item_id: str, amount: int) -> ParticipantResult:
        response = await self._safe_post(f"/saga/reserve/{tx_id}/{item_id}/{amount}")
        return self._status_to_result(response)

    async def saga_release_item(self, tx_id: str, item_id: str, amount: int) -> ParticipantResult:
        response = await self._safe_post(f"/saga/release/{tx_id}/{item_id}/{amount}")
        return self._status_to_result(response)

    @staticmethod
    def _status_to_result(response: httpx.Response) -> ParticipantResult:
        if response.status_code == 200:
            return ParticipantResult(ok=True)
        if response.status_code >= 500:
            return ParticipantResult(ok=False, retryable=True, detail="Stock service unavailable")
        detail = response.text.strip() or "Stock request failed"
        return ParticipantResult(ok=False, detail=detail)

    async def _safe_post(self, path: str) -> httpx.Response:
        try:
            return await self.session.post(f"{self.base_url}{path}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc

    async def _safe_get(self, path: str) -> httpx.Response:
        try:
            return await self.session.get(f"{self.base_url}{path}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc
