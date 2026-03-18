import httpx
from fastapi import HTTPException

from clients.saga_bus import SagaCommandBus
from models import ParticipantResult, REQ_ERROR_STR


class PaymentClient:
    def __init__(
        self,
        session: httpx.AsyncClient,
        service_url: str,
        saga_bus: SagaCommandBus | None = None,
    ):
        self.session = session
        self.base_url = service_url.rstrip("/")
        self.saga_bus = saga_bus

    async def prepare(self, tx_id: str, user_id: str, amount: int) -> ParticipantResult:
        response = await self._safe_post(f"/2pc/prepare/{tx_id}/{user_id}/{amount}")
        return self._status_to_result(response)

    async def commit(self, tx_id: str) -> ParticipantResult:
        response = await self._safe_post(f"/2pc/commit/{tx_id}")
        return self._status_to_result(response)

    async def abort(self, tx_id: str) -> ParticipantResult:
        response = await self._safe_post(f"/2pc/abort/{tx_id}")
        return self._status_to_result(response)

    async def saga_debit(self, tx_id: str, user_id: str, amount: int, attempt: int) -> ParticipantResult:
        if self.saga_bus is None:
            raise HTTPException(status_code=400, detail="Saga MQ bus is not configured")
        return await self.saga_bus.request(
            participant="payment",
            action="debit",
            tx_id=tx_id,
            payload={"user_id": user_id, "amount": int(amount)},
            attempt=attempt,
        )

    async def saga_refund(self, tx_id: str, user_id: str, amount: int, attempt: int) -> ParticipantResult:
        if self.saga_bus is None:
            raise HTTPException(status_code=400, detail="Saga MQ bus is not configured")
        return await self.saga_bus.request(
            participant="payment",
            action="refund",
            tx_id=tx_id,
            payload={"user_id": user_id, "amount": int(amount)},
            attempt=attempt,
        )

    @staticmethod
    def _status_to_result(response: httpx.Response) -> ParticipantResult:
        if response.status_code == 200:
            return ParticipantResult(ok=True)
        if response.status_code >= 500:
            return ParticipantResult(ok=False, retryable=True, detail="Payment service unavailable")
        detail = response.text.strip() or "Payment request failed"
        return ParticipantResult(ok=False, detail=detail)

    async def _safe_post(self, path: str) -> httpx.Response:
        try:
            return await self.session.post(f"{self.base_url}{path}")
        except httpx.RequestError as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc
