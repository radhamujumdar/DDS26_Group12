import httpx
from fastapi import HTTPException

from clients.saga_bus import SagaCommandBus
from clients.two_pc_bus import TwoPCCommandBus
from models import ParticipantResult, REQ_ERROR_STR


class StockClient:
    def __init__(
        self,
        session: httpx.AsyncClient,
        service_url: str,
        saga_bus: SagaCommandBus | None = None,
        # 2pc message queue change: inject a dedicated 2PC bus so the client can
        # keep the existing HTTP fallback but use Redis Streams in 2PC mode.
        two_pc_bus: TwoPCCommandBus | None = None,
    ):
        self.session = session
        self.base_url = service_url.rstrip("/")
        self.saga_bus = saga_bus
        self.two_pc_bus = two_pc_bus

    async def find_item(self, item_id: str) -> httpx.Response:
        return await self._safe_get(f"/find/{item_id}")

    async def prepare_item(self, tx_id: str, item_id: str, amount: int, attempt: int = 1) -> ParticipantResult:
        # 2pc message queue change: send prepare through the same broker pattern as
        # Saga so coordinator transport becomes message-driven without changing
        # participant business logic.
        if self.two_pc_bus is not None:
            return await self.two_pc_bus.request(
                participant="stock",
                action="prepare",
                tx_id=tx_id,
                payload={"item_id": item_id, "amount": int(amount)},
                attempt=attempt,
            )
        response = await self._safe_post(f"/2pc/prepare/{tx_id}/{item_id}/{amount}")
        return self._status_to_result(response)

    async def commit_item(self, tx_id: str, item_id: str, amount: int, attempt: int = 1) -> ParticipantResult:
        # 2pc message queue change: commit uses the same request/reply MQ path so
        # the commit decision is delivered by Redis Streams instead of HTTP.
        if self.two_pc_bus is not None:
            return await self.two_pc_bus.request(
                participant="stock",
                action="commit",
                tx_id=tx_id,
                payload={"item_id": item_id, "amount": int(amount)},
                attempt=attempt,
            )
        response = await self._safe_post(f"/2pc/commit/{tx_id}/{item_id}/{amount}")
        return self._status_to_result(response)

    async def abort_item(self, tx_id: str, item_id: str, amount: int, attempt: int = 1) -> ParticipantResult:
        # 2pc message queue change: abort follows the same MQ transport so the
        # rollback path stays consistent with the queued prepare/commit flow.
        if self.two_pc_bus is not None:
            return await self.two_pc_bus.request(
                participant="stock",
                action="abort",
                tx_id=tx_id,
                payload={"item_id": item_id, "amount": int(amount)},
                attempt=attempt,
            )
        response = await self._safe_post(f"/2pc/abort/{tx_id}/{item_id}/{amount}")
        return self._status_to_result(response)

    async def saga_reserve_item(self, tx_id: str, item_id: str, amount: int, attempt: int) -> ParticipantResult:
        if self.saga_bus is None:
            raise HTTPException(status_code=400, detail="Saga MQ bus is not configured")
        return await self.saga_bus.request(
            participant="stock",
            action="reserve",
            tx_id=tx_id,
            payload={"item_id": item_id, "amount": int(amount)},
            attempt=attempt,
        )

    async def saga_release_item(self, tx_id: str, item_id: str, amount: int, attempt: int) -> ParticipantResult:
        if self.saga_bus is None:
            raise HTTPException(status_code=400, detail="Saga MQ bus is not configured")
        return await self.saga_bus.request(
            participant="stock",
            action="release",
            tx_id=tx_id,
            payload={"item_id": item_id, "amount": int(amount)},
            attempt=attempt,
        )

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
