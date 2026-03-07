import asyncio
import logging

import httpx
from fastapi import HTTPException

from logging_utils import log_event
from models import RecoveryAction
from repository.stock_repo import StockRepository
from services.stock_service import StockService


class StockRecoveryService:
    def __init__(
        self,
        repo: StockRepository,
        stock_service: StockService,
        gateway_client: httpx.AsyncClient,
        gateway_url: str,
        logger: logging.Logger,
    ):
        self.repo = repo
        self.stock_service = stock_service
        self.gateway_client = gateway_client
        self.gateway_url = gateway_url
        self.logger = logger

    async def recover_once(self) -> int:
        recovered = 0
        tx_state_cache: dict[str, str | None] = {}
        pending = await self.repo.list_prepared_reservations()
        if not pending:
            return recovered

        self._log("recovery_scan", pending_count=len(pending))
        for reservation in pending:
            tx_state = tx_state_cache.get(reservation.tx_id)
            if reservation.tx_id not in tx_state_cache:
                tx_state = await self._get_coordinator_tx_state(reservation.tx_id)
                tx_state_cache[reservation.tx_id] = tx_state

            action = self._decision_action(tx_state)
            if action is None:
                continue

            try:
                if action == RecoveryAction.COMMIT:
                    await self.stock_service.commit(
                        tx_id=reservation.tx_id,
                        item_id=reservation.item_id,
                        amount=reservation.amount,
                    )
                else:
                    await self.stock_service.abort(
                        tx_id=reservation.tx_id,
                        item_id=reservation.item_id,
                        amount=reservation.amount,
                    )
                recovered += 1
                self._log(
                    "recovery_reservation_finalized",
                    tx_id=reservation.tx_id,
                    item_id=reservation.item_id,
                    action=action.value,
                    coordinator_state=tx_state,
                )
            except HTTPException as exc:
                self._log(
                    "recovery_reservation_failed",
                    level="warning",
                    tx_id=reservation.tx_id,
                    item_id=reservation.item_id,
                    action=action.value,
                    coordinator_state=tx_state,
                    detail=str(exc.detail),
                )

        return recovered

    async def run_loop(self, startup_delay_seconds: float, interval_seconds: float):
        await asyncio.sleep(startup_delay_seconds)
        while True:
            try:
                recovered_count = await self.recover_once()
                if recovered_count:
                    self._log("recovery_pass_complete", recovered_count=recovered_count)
            except Exception as exc:
                self._log("recovery_loop_failed", level="warning", detail=str(exc))
            await asyncio.sleep(interval_seconds)

    async def _get_coordinator_tx_state(self, tx_id: str) -> str | None:
        try:
            response = await self.gateway_client.get(f"{self.gateway_url}/orders/2pc/tx/{tx_id}")
        except httpx.HTTPError:
            return None

        if response.status_code == 200:
            payload = response.json()
            return str(payload.get("state", ""))

        if response.status_code == 400:
            try:
                detail = str(response.json().get("detail", "")).lower()
            except ValueError:
                detail = ""
            if "not found" in detail:
                return "UNKNOWN"

        return None

    @staticmethod
    def _decision_action(coordinator_state: str | None) -> RecoveryAction | None:
        if coordinator_state in {"COMMITTED", "COMMITTING"}:
            return RecoveryAction.COMMIT
        if coordinator_state in {"ABORTED", "ABORTING", "UNKNOWN"}:
            return RecoveryAction.ABORT
        return None

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="stock-service",
            component="recovery",
            **fields,
        )
