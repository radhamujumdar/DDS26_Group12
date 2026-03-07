import json
import time
import uuid

import redis
from redis.asyncio import Redis

from logging_utils import log_event
from models import ParticipantResult


class SagaCommandBus:
    COMMAND_STREAM_PREFIX = "saga:cmd:"

    def __init__(
        self,
        db: Redis,
        logger,
        response_timeout_ms: int = 3000,
        command_stream_maxlen: int = 100000,
    ):
        self.db = db
        self.logger = logger
        self.response_timeout_ms = int(response_timeout_ms)
        self.command_stream_maxlen = int(command_stream_maxlen)

    async def request(
        self,
        participant: str,
        action: str,
        tx_id: str,
        payload: dict,
    ) -> ParticipantResult:
        operation_id = str(uuid.uuid4())
        reply_stream = f"saga:reply:{operation_id}"
        command_stream = f"{self.COMMAND_STREAM_PREFIX}{participant}"
        command = {
            "operation_id": operation_id,
            "tx_id": tx_id,
            "participant": participant,
            "action": action,
            "reply_stream": reply_stream,
            "payload": json.dumps(payload, separators=(",", ":"), default=str),
            "sent_at_ms": str(int(time.time() * 1000)),
        }

        try:
            await self.db.xadd(
                name=command_stream,
                fields=command,
                maxlen=self.command_stream_maxlen,
                approximate=True,
            )
        except redis.exceptions.RedisError as exc:
            self._log(
                "saga_mq_publish_failed",
                level="warning",
                participant=participant,
                action=action,
                tx_id=tx_id,
                detail=str(exc),
            )
            return ParticipantResult(ok=False, retryable=True, detail="Saga MQ publish failed")

        self._log(
            "saga_mq_publish",
            participant=participant,
            action=action,
            tx_id=tx_id,
            operation_id=operation_id,
            reply_stream=reply_stream,
        )

        try:
            response = await self.db.xread(
                streams={reply_stream: "0-0"},
                count=1,
                block=self.response_timeout_ms,
            )
        except redis.exceptions.RedisError as exc:
            self._log(
                "saga_mq_consume_failed",
                level="warning",
                participant=participant,
                action=action,
                tx_id=tx_id,
                operation_id=operation_id,
                detail=str(exc),
            )
            return ParticipantResult(ok=False, retryable=True, detail="Saga MQ consume failed")
        finally:
            try:
                await self.db.delete(reply_stream)
            except redis.exceptions.RedisError:
                pass

        if not response:
            self._log(
                "saga_mq_timeout",
                level="warning",
                participant=participant,
                action=action,
                tx_id=tx_id,
                operation_id=operation_id,
                timeout_ms=self.response_timeout_ms,
            )
            return ParticipantResult(
                ok=False,
                retryable=True,
                detail=f"Saga MQ response timeout for {participant}.{action}",
            )

        _, entries = response[0]
        _, fields = entries[0]

        ok_raw = self._decode(fields.get(b"ok") or fields.get("ok"))
        retryable_raw = self._decode(fields.get(b"retryable") or fields.get("retryable"))
        detail = self._decode(fields.get(b"detail") or fields.get("detail")) or None

        ok = ok_raw.lower() in ("1", "true", "yes", "ok")
        retryable = retryable_raw.lower() in ("1", "true", "yes")

        self._log(
            "saga_mq_response",
            level="info" if ok else "warning",
            participant=participant,
            action=action,
            tx_id=tx_id,
            operation_id=operation_id,
            ok=ok,
            retryable=retryable,
            detail=detail,
        )
        return ParticipantResult(ok=ok, retryable=retryable, detail=detail)

    @staticmethod
    def _decode(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="order-service",
            component="saga-mq",
            **fields,
        )
