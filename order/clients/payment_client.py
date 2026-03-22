import httpx
from fastapi import HTTPException

from clients.saga_bus import SagaCommandBus
from clients.two_pc_bus import TwoPCCommandBus
from models import ParticipantResult, REQ_ERROR_STR


class PaymentClient:
    def __init__(
        self,
        session: httpx.AsyncClient,
        service_url: str,
        saga_bus: SagaCommandBus | None = None,
        # 2pc message queue change: inject a dedicated 2PC bus so payment 2PC
        # traffic can use Redis Streams while keeping the current service logic.
        two_pc_bus: TwoPCCommandBus | None = None,
    ):
        self.session = session
        self.base_url = service_url.rstrip("/")
        self.saga_bus = saga_bus
        self.two_pc_bus = two_pc_bus

    async def prepare(self, tx_id: str, user_id: str, amount: int, attempt: int = 1) -> ParticipantResult:
        # 2pc message queue change: prepare moves onto the broker so 2PC uses
        # the same message transport pattern as Saga.
        if self.two_pc_bus is not None:
            return await self.two_pc_bus.request(
                participant="payment",
                action="prepare",
                tx_id=tx_id,
                payload={"user_id": user_id, "amount": int(amount)},
                attempt=attempt,
            )
        response = await self._safe_post(f"/2pc/prepare/{tx_id}/{user_id}/{amount}")
        return self._status_to_result(response)

    async def commit(self, tx_id: str, attempt: int = 1) -> ParticipantResult:
        # 2pc message queue change: commit becomes a queued decision delivery,
        # aligned with the 2PC bus used by the coordinator.
        if self.two_pc_bus is not None:
            return await self.two_pc_bus.request(
                participant="payment",
                action="commit",
                tx_id=tx_id,
                payload={},
                attempt=attempt,
            )
        response = await self._safe_post(f"/2pc/commit/{tx_id}")
        return self._status_to_result(response)

    async def abort(self, tx_id: str, attempt: int = 1) -> ParticipantResult:
        # 2pc message queue change: abort is delivered over Redis Streams to keep
        # the rollback path on the same transport as prepare/commit.
        if self.two_pc_bus is not None:
            return await self.two_pc_bus.request(
                participant="payment",
                action="abort",
                tx_id=tx_id,
                payload={},
                attempt=attempt,
            )
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
