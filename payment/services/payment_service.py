import logging
import time
import uuid

from fastapi import HTTPException
from msgspec import msgpack

from logging_utils import log_event
from models import PrepareRecord, TxnState, UserValue
from repository.payment_repo import PaymentRepository


class PaymentService:
    def __init__(self, repo: PaymentRepository, logger: logging.Logger):
        self.repo = repo
        self.logger = logger

    async def prepare(self, txn_id: str, user_id: str, amount: int) -> dict:
        self._require_positive(amount, "amount")
        started = time.perf_counter()
        self._log("prepare_begin", tx_id=txn_id, user_id=user_id, amount=amount)
        try:
            record = await self.repo.prepare_transaction(txn_id, user_id, amount)
            response = {"status": record.state.value, "txn_id": txn_id}
            self._log(
                "prepare_complete",
                tx_id=txn_id,
                user_id=user_id,
                amount=amount,
                status=record.state.value,
                duration_ms=self._duration_ms(started),
            )
            return response
        except HTTPException as exc:
            self._log(
                "prepare_failed",
                level="warning",
                tx_id=txn_id,
                user_id=user_id,
                amount=amount,
                detail=str(exc.detail),
                duration_ms=self._duration_ms(started),
            )
            raise

    async def commit(self, txn_id: str) -> dict:
        started = time.perf_counter()
        self._log("commit_begin", tx_id=txn_id)
        try:
            record = await self.repo.commit_transaction(txn_id)
            self._log(
                "commit_complete",
                tx_id=txn_id,
                status=record.state.value,
                duration_ms=self._duration_ms(started),
            )
            return {"status": TxnState.COMMITTED.value, "txn_id": txn_id}
        except HTTPException as exc:
            self._log(
                "commit_failed",
                level="warning",
                tx_id=txn_id,
                detail=str(exc.detail),
                duration_ms=self._duration_ms(started),
            )
            raise

    async def abort(self, txn_id: str) -> dict:
        started = time.perf_counter()
        self._log("abort_begin", tx_id=txn_id)
        try:
            aborted = await self.repo.abort_transaction(txn_id)
            if not aborted:
                raise HTTPException(status_code=400, detail=f"Transaction {txn_id} already committed, cannot abort")
            self._log("abort_complete", tx_id=txn_id, duration_ms=self._duration_ms(started))
            return {"status": TxnState.ABORTED.value, "txn_id": txn_id}
        except HTTPException as exc:
            self._log(
                "abort_failed",
                level="warning",
                tx_id=txn_id,
                detail=str(exc.detail),
                duration_ms=self._duration_ms(started),
            )
            raise

    async def create_user(self) -> dict:
        user_id = str(uuid.uuid4())
        await self.repo.create_user(user_id=user_id, credit=0)
        self._log("create_user", user_id=user_id)
        return {"user_id": user_id}

    async def batch_init_users(self, n: int, starting_money: int) -> dict:
        self._require_positive(n, "n")
        self._require_positive(starting_money, "starting_money")
        kv_pairs: dict[str, bytes] = {
            f"{idx}": msgpack.encode(UserValue(credit=starting_money))
            for idx in range(n)
        }
        await self.repo.batch_init_users(kv_pairs)
        self._log("batch_init_users", count=n, starting_money=starting_money)
        return {"msg": "Batch init for users successful"}

    async def find_user(self, user_id: str) -> dict:
        user = await self.repo.get_user_or_error(user_id)
        return {"user_id": user_id, "credit": user.credit}

    async def add_funds(self, user_id: str, amount: int) -> int:
        self._require_positive(amount, "amount")
        user = await self.repo.get_user_or_error(user_id)
        user.credit += int(amount)
        await self.repo.save_user(user_id, user)
        self._log("add_funds", user_id=user_id, amount=amount, credit=user.credit)
        return user.credit

    async def pay(self, user_id: str, amount: int) -> int:
        self._require_positive(amount, "amount")
        new_credit = await self.repo.pay(user_id, amount)
        self._log("pay", user_id=user_id, amount=amount, credit=new_credit)
        return new_credit

    async def saga_debit(self, txn_id: str, user_id: str, amount: int) -> dict:
        self._require_positive(amount, "amount")
        ok, retryable, detail = await self.handle_saga_command(
            action="debit",
            tx_id=txn_id,
            payload={"user_id": user_id, "amount": int(amount)},
        )
        if not ok:
            raise HTTPException(status_code=400, detail=detail or "Saga debit failed")
        return {"status": "done", "retryable": retryable}

    async def saga_refund(self, txn_id: str) -> dict:
        ok, retryable, detail = await self.handle_saga_command(
            action="refund",
            tx_id=txn_id,
            payload={},
        )
        if not ok:
            raise HTTPException(status_code=400, detail=detail or "Saga refund failed")
        return {"status": "done", "retryable": retryable}

    async def handle_saga_command(self, action: str, tx_id: str, payload: dict) -> tuple[bool, bool, str]:
        if action == "debit":
            return await self._handle_saga_debit(tx_id=tx_id, payload=payload)
        if action == "refund":
            return await self._handle_saga_refund(tx_id=tx_id, payload=payload)
        return False, False, f"Unsupported payment saga action: {action}"

    async def _handle_saga_debit(self, tx_id: str, payload: dict) -> tuple[bool, bool, str | None]:
        user_id = str(payload.get("user_id", ""))
        amount = int(payload.get("amount", 0) or 0)
        if not user_id or amount <= 0:
            return False, False, "Invalid debit payload"
        return await self.repo.saga_debit(tx_id=tx_id, user_id=user_id, amount=amount)

    async def _handle_saga_refund(self, tx_id: str, payload: dict) -> tuple[bool, bool, str | None]:
        user_id = str(payload.get("user_id", "")).strip() or None
        amount_raw = payload.get("amount")
        amount = int(amount_raw) if amount_raw is not None else None
        if amount is not None and amount <= 0:
            return False, False, "Invalid refund payload"
        return await self.repo.saga_refund(tx_id=tx_id, user_id=user_id, amount=amount)

    async def get_prepare_record(self, txn_id: str) -> PrepareRecord | None:
        return await self.repo.get_prepare_record(txn_id)

    async def save_prepare_record(self, rec: PrepareRecord):
        await self.repo.save_prepare_record(rec)

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="payment-service",
            component="participant",
            **fields,
        )

    @staticmethod
    def _duration_ms(started: float) -> float:
        return round((time.perf_counter() - started) * 1000, 3)

    @staticmethod
    def _require_positive(value: int, field_name: str):
        if int(value) <= 0:
            raise HTTPException(status_code=400, detail=f"{field_name} must be greater than zero")
