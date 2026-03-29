import asyncio
import json
import os
import socket
import time

import redis
from redis.asyncio import Redis

from logging_utils import log_event
from services.stock_service import StockService


class StockSagaMqWorkerService:
    PARTICIPANT = "stock"
    SCHEMA_VERSION = "v1"
    COMMAND_STREAM_PREFIX = "saga:cmd:stock:p"
    RESULT_STREAM_PREFIX = "saga:res:stock:p"

    def __init__(
        self,
        db: Redis,
        stock_service: StockService,
        logger,
        stream_partitions: int,
        consumer_group: str,
        block_ms: int,
        batch_size: int,
        command_stream_maxlen: int,
        result_stream_maxlen: int,
    ):
        self.db = db
        self.stock_service = stock_service
        self.logger = logger
        self.stream_partitions = int(stream_partitions)
        self.consumer_group = consumer_group
        self.block_ms = int(block_ms)
        self.batch_size = int(batch_size)
        self.command_stream_maxlen = int(command_stream_maxlen)
        self.result_stream_maxlen = int(result_stream_maxlen)
        self.consumer_name = f"{socket.gethostname()}-{os.getpid()}"

    async def run_loop(self):
        await self._ensure_consumer_groups()
        self._log(
            "saga_worker_started",
            participant=self.PARTICIPANT,
            stream_partitions=self.stream_partitions,
            consumer_group=self.consumer_group,
            consumer_name=self.consumer_name,
        )
        while True:
            try:
                streams = {self._command_stream(partition): ">" for partition in range(self.stream_partitions)}
                entries = await self.db.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=self.consumer_name,
                    streams=streams,
                    count=self.batch_size,
                    block=self.block_ms,
                )
            except redis.exceptions.RedisError as exc:
                self._log("saga_worker_read_failed", level="warning", detail=str(exc))
                await asyncio.sleep(0.2)
                continue

            if not entries:
                continue

            tasks = []
            for stream_name_raw, stream_entries in entries:
                stream_name = self._decode(stream_name_raw)
                partition = self._parse_partition(stream_name)
                if partition < 0:
                    continue
                for message_id_raw, fields_raw in stream_entries:
                    message_id = self._decode(message_id_raw)
                    fields = self._decode_dict(fields_raw)
                    tasks.append(self._handle_command(stream_name, message_id, fields, partition))
            if tasks:
                await asyncio.gather(*tasks)

    async def _handle_command(self, stream_name: str, message_id: str, fields: dict[str, str], partition: int):
        correlation_id = fields.get("correlation_id", "")
        tx_id = fields.get("tx_id", "")
        action = fields.get("action", "")
        payload_raw = fields.get("payload", "{}")

        if not correlation_id or not tx_id or not action:
            self._log(
                "saga_worker_invalid_command",
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
        ok, retryable, detail = await self.stock_service.handle_saga_command(action=action, tx_id=tx_id, payload=payload)
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
            "saga_worker_command_processed",
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

    async def _ensure_consumer_groups(self):
        for partition in range(self.stream_partitions):
            stream = self._command_stream(partition)
            try:
                await self.db.xgroup_create(stream, self.consumer_group, id="0-0", mkstream=True)
            except redis.exceptions.ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise

    def _command_stream(self, partition: int) -> str:
        return f"{self.COMMAND_STREAM_PREFIX}{partition}"

    def _result_stream(self, partition: int) -> str:
        return f"{self.RESULT_STREAM_PREFIX}{partition}"

    @staticmethod
    def _decode(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    def _decode_dict(self, raw: dict) -> dict[str, str]:
        return {self._decode(key): self._decode(value) for key, value in raw.items()}

    @staticmethod
    def _parse_partition(stream_name: str) -> int:
        # Expected: saga:cmd:stock:p<partition>
        parts = stream_name.split(":")
        if len(parts) != 4:
            return -1
        p = parts[3]
        if not p.startswith("p"):
            return -1
        try:
            return int(p[1:])
        except ValueError:
            return -1

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="stock-service",
            component="saga-worker",
            **fields,
        )
