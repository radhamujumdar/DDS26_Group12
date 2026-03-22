# 2pc message queue change: mirror the Saga worker shape so payment can consume
# queued 2PC prepare/commit/abort commands without changing the existing
# participant service logic.
import json
import time

from fastapi import HTTPException

from logging_utils import log_event
from models import DB_ERROR_STR
from services.saga_worker_service import PaymentSagaMqWorkerService


class PaymentTwoPCMqWorkerService(PaymentSagaMqWorkerService):
    PARTICIPANT = "payment"
    COMMAND_STREAM_PREFIX = "two_pc:cmd:payment:p"
    RESULT_STREAM_PREFIX = "two_pc:res:payment:p"

    async def _handle_command(self, stream_name: str, message_id: str, fields: dict[str, str], partition: int):
        correlation_id = fields.get("correlation_id", "")
        tx_id = fields.get("tx_id", "")
        action = fields.get("action", "")
        payload_raw = fields.get("payload", "{}")

        if not correlation_id or not tx_id or not action:
            self._log(
                "two_pc_worker_invalid_command",
                level="warning",
                stream=stream_name,
                message_id=message_id,
                correlation_id=correlation_id,
                tx_id=tx_id,
                action=action,
            )
            await self.db.xack(stream_name, self.consumer_group, message_id)
            return

        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {}

        started = time.perf_counter()
        ok = False
        retryable = False
        detail: str | None = None

        try:
            if action == "prepare":
                user_id = str(payload.get("user_id", ""))
                # 2pc message queue change: keep malformed command payloads
                # terminal instead of retrying them as worker/runtime failures.
                try:
                    amount = int(payload.get("amount", 0) or 0)
                except (TypeError, ValueError) as exc:
                    raise HTTPException(status_code=400, detail="amount must be an integer") from exc
                response = await self.payment_service.prepare(tx_id, user_id, amount)
            elif action == "commit":
                response = await self.payment_service.commit(tx_id)
            elif action == "abort":
                response = await self.payment_service.abort(tx_id)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported payment 2PC action: {action}")
            ok = True
            detail = response.get("status")
        except HTTPException as exc:
            detail = str(exc.detail)
            retryable = detail == DB_ERROR_STR
        except Exception as exc:
            detail = str(exc)
            retryable = True

        duration_ms = round((time.perf_counter() - started) * 1000, 3)

        await self.db.xadd(
            name=self._result_stream(partition),
            fields={
                "schema_version": self.SCHEMA_VERSION,
                "correlation_id": correlation_id,
                "tx_id": tx_id,
                "participant": self.PARTICIPANT,
                "action": action,
                "ok": "1" if ok else "0",
                "retryable": "1" if retryable else "0",
                "detail": detail or "",
                "processed_at_ms": str(int(time.time() * 1000)),
            },
            maxlen=self.result_stream_maxlen,
            approximate=True,
        )
        await self.db.xack(stream_name, self.consumer_group, message_id)

        self._log(
            "two_pc_worker_command_processed",
            level="info" if ok else "warning",
            stream=stream_name,
            message_id=message_id,
            correlation_id=correlation_id,
            tx_id=tx_id,
            action=action,
            partition=partition,
            ok=ok,
            retryable=retryable,
            detail=detail,
            duration_ms=duration_ms,
        )

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="payment-service",
            component="two-pc-worker",
            **fields,
        )
