import asyncio
import logging
import os
import socket

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
        order_client: httpx.AsyncClient,
        order_service_url: str,
        logger: logging.Logger,
        enable_loop: bool = True,
        lease_key: str = "stock:recovery:lease",
        lease_ttl_seconds: int = 10,
        owner_id: str | None = None,
    ):
        self.repo = repo
        self.stock_service = stock_service
        self.order_client = order_client
        self.order_service_url = order_service_url.rstrip("/")
        self.logger = logger
        self.enable_loop = bool(enable_loop)
        self.lease_key = lease_key
        self.lease_ttl_seconds = int(lease_ttl_seconds)
        self.owner_id = owner_id or f"{socket.gethostname()}-{os.getpid()}"
        self._is_leader = False

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
        if not self.enable_loop:
            self._log("recovery_disabled")
            return

        while True:
            try:
                is_leader = await self._acquire_or_renew_leadership()
                if is_leader and not self._is_leader:
                    self._log("recovery_leader_acquired", lease_key=self.lease_key, owner_id=self.owner_id)
                if not is_leader and self._is_leader:
                    self._log(
                        "recovery_leader_lost",
                        level="warning",
                        lease_key=self.lease_key,
                        owner_id=self.owner_id,
                    )
                self._is_leader = is_leader
                if not is_leader:
                    await asyncio.sleep(min(interval_seconds, 1.0))
                    continue

                recovered_count = await self.recover_once()
                if recovered_count:
                    self._log("recovery_pass_complete", recovered_count=recovered_count)
            except Exception as exc:
                self._log("recovery_loop_failed", level="warning", detail=str(exc))
            await asyncio.sleep(interval_seconds)

    async def _acquire_or_renew_leadership(self) -> bool:
        try:
            primary = self.repo.db.primary()
            current_owner = await primary.get(self.lease_key)
            if self._decode(current_owner) == self.owner_id:
                await primary.expire(self.lease_key, self.lease_ttl_seconds)
                return True
            if current_owner is None:
                acquired = await primary.set(
                    self.lease_key,
                    self.owner_id,
                    nx=True,
                    ex=self.lease_ttl_seconds,
                )
                return bool(acquired)
            return False
        except Exception as exc:
            self._log("recovery_leader_check_failed", level="warning", detail=str(exc))
            return False

    async def _get_coordinator_tx_state(self, tx_id: str) -> str | None:
        try:
            response = await self.order_client.get(f"{self.order_service_url}/2pc/tx/{tx_id}")
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

    @staticmethod
    def _decode(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode()
        return str(value)
