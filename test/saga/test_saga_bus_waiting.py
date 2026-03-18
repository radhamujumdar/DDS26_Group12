import asyncio
import logging
import sys
from pathlib import Path

import pytest


ORDER_DIR = Path(__file__).resolve().parents[2] / "order"
if str(ORDER_DIR) not in sys.path:
    sys.path.insert(0, str(ORDER_DIR))

from clients.saga_bus import SagaCommandBus


class FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}
        self.expirations: dict[str, int] = {}
        self.counters: dict[str, int] = {}

    async def hset(self, key: str, mapping: dict[str, str]):
        bucket = self.hashes.setdefault(key, {})
        for field, value in mapping.items():
            bucket[str(field)] = str(value)
        return len(mapping)

    async def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    async def expire(self, key: str, ttl_seconds: int):
        self.expirations[key] = int(ttl_seconds)
        return True

    async def incrby(self, key: str, delta: int):
        self.counters[key] = self.counters.get(key, 0) + int(delta)
        return self.counters[key]


@pytest.mark.anyio
async def test_waiter_observes_completed_result_without_local_event():
    db = FakeRedis()
    bus = SagaCommandBus(
        db=db,
        logger=logging.getLogger("test.saga_bus"),
        poll_interval_seconds=0.01,
    )
    correlation_id = "corr-1"
    pending_key = bus._pending_key(correlation_id)
    await db.hset(pending_key, {"status": "pending"})

    async def complete_in_other_process():
        await asyncio.sleep(0.02)
        await db.hset(
            pending_key,
            {
                "status": "completed",
                "ok": "1",
                "retryable": "0",
                "detail": "",
            },
        )

    task = asyncio.create_task(complete_in_other_process())
    try:
        result = await bus._await_result(correlation_id, timeout_ms=200)
    finally:
        await task

    assert result.status == "completed"
    assert result.ok is True
    assert result.retryable is False
    assert result.detail is None


@pytest.mark.anyio
async def test_timeout_keeps_pending_record_available_for_late_result():
    db = FakeRedis()
    bus = SagaCommandBus(
        db=db,
        logger=logging.getLogger("test.saga_bus"),
        poll_interval_seconds=0.01,
    )
    correlation_id = "corr-timeout"
    pending_key = bus._pending_key(correlation_id)
    await db.hset(pending_key, {"status": "pending"})

    result = await bus._await_result(correlation_id, timeout_ms=30)

    assert result.status == "timed_out"
    assert result.ok is False
    assert result.retryable is True
    assert result.detail == "Saga MQ response timeout"
    assert (await db.hgetall(pending_key))["status"] == "timed_out"


@pytest.mark.anyio
async def test_late_result_can_overwrite_timed_out_pending_status():
    db = FakeRedis()
    bus = SagaCommandBus(
        db=db,
        logger=logging.getLogger("test.saga_bus"),
        poll_interval_seconds=0.01,
    )
    correlation_id = "corr-late"
    pending_key = bus._pending_key(correlation_id)
    await db.hset(
        pending_key,
        {
            "status": "timed_out",
            "ok": "0",
            "retryable": "1",
            "detail": "Saga MQ response timeout",
        },
    )

    await bus._apply_result(
        correlation_id,
        {
            "tx_id": "tx-1",
            "participant": "payment",
            "action": "debit",
            "ok": "1",
            "retryable": "0",
            "detail": "",
        },
    )

    stored = await db.hgetall(pending_key)
    assert stored["status"] == "completed"
    assert stored["ok"] == "1"
    assert stored["retryable"] == "0"


@pytest.mark.anyio
async def test_wait_late_result_observes_completion_after_timeout():
    db = FakeRedis()
    bus = SagaCommandBus(
        db=db,
        logger=logging.getLogger("test.saga_bus"),
        poll_interval_seconds=0.01,
    )
    correlation_id = "corr-late-wait"
    pending_key = bus._pending_key(correlation_id)
    await db.hset(
        pending_key,
        {
            "status": "timed_out",
            "ok": "0",
            "retryable": "1",
            "detail": "Saga MQ response timeout",
        },
    )

    async def complete_in_other_process():
        await asyncio.sleep(0.02)
        await db.hset(
            pending_key,
            {
                "status": "completed",
                "ok": "1",
                "retryable": "0",
                "detail": "released",
            },
        )

    task = asyncio.create_task(complete_in_other_process())
    try:
        result = await bus.await_late_result(correlation_id, timeout_ms=200)
    finally:
        await task

    assert result.ok is True
    assert result.retryable is False
    assert result.detail == "released"
    assert result.correlation_id == correlation_id
    assert result.status == "completed"
