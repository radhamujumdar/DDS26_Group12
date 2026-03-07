import asyncio
import logging

import httpx
from fastapi import HTTPException

from logging_utils import log_event
from models import RecoveryAction
from repository.payment_repo import PaymentRepository
from services.payment_service import PaymentService


class PaymentRecoveryService:
    def __init__(
        self,
        repo: PaymentRepository,
        payment_service: PaymentService,
        gateway_client: httpx.AsyncClient,
        gateway_url: str,
        logger: logging.Logger,
    ):
        self.repo = repo
        self.payment_service = payment_service
        self.gateway_client = gateway_client
        self.gateway_url = gateway_url
        self.logger = logger

    async def recover_once(self) -> int:
        recovered = 0
        pending_tx_ids = await self.repo.list_prepared_tx_ids()
        if not pending_tx_ids:
            return recovered

        self._log("recovery_scan", pending_count=len(pending_tx_ids))
        for txn_id in pending_tx_ids:
            coordinator_state = await self._get_coordinator_tx_state(txn_id)
            action = self._decision_action(coordinator_state)
            if action is None:
                continue

            try:
                if action == RecoveryAction.COMMIT:
                    await self.payment_service.commit(txn_id)
                else:
                    await self.payment_service.abort(txn_id)
                recovered += 1
                self._log(
                    "recovery_tx_finalized",
                    tx_id=txn_id,
                    action=action.value,
                    coordinator_state=coordinator_state,
                )
            except HTTPException as exc:
                self._log(
                    "recovery_tx_failed",
                    level="warning",
                    tx_id=txn_id,
                    action=action.value,
                    coordinator_state=coordinator_state,
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

    async def _get_coordinator_tx_state(self, txn_id: str) -> str | None:
        try:
            response = await self.gateway_client.get(f"{self.gateway_url}/orders/2pc/tx/{txn_id}")
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
            service="payment-service",
            component="recovery",
            **fields,
        )
