from __future__ import annotations

import asyncio

from fluxi_engine_test_support import FluxiEngineAsyncTestCase
from fluxi_engine.scheduler import FluxiScheduler


class TestFluxiEngineIntegration(FluxiEngineAsyncTestCase):
    async def test_wait_for_workflow_result_wakes_on_terminal_notification(self) -> None:
        started = await self.start_workflow()
        run_id = started["run_id"]
        self.assertIsNotNone(run_id)

        _, workflow_task = await self.latest_stream_payload(self.workflow_stream_key())
        waiter = asyncio.create_task(
            self.store.wait_for_workflow_result("checkout:1", timeout_ms=1_000)
        )
        await asyncio.sleep(0.05)
        self.assertFalse(waiter.done())

        completion_response = await self.client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task["workflow_task_id"],
                "attempt_no": workflow_task["attempt_no"],
                "command": {
                    "kind": "complete_workflow",
                    "result_payload_b64": self.payload_b64({"status": "paid"}),
                },
            },
        )
        completion_response.raise_for_status()
        snapshot = await asyncio.wait_for(waiter, timeout=1.0)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.status, "completed")

    async def test_wait_for_workflow_result_times_out_with_running_snapshot(self) -> None:
        started = await self.start_workflow()
        self.assertEqual(started["decision"], "started")

        snapshot = await self.store.wait_for_workflow_result(
            "checkout:1",
            timeout_ms=50,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.status, "running")

    async def test_happy_path_vertical_slice(self) -> None:
        started = await self.start_workflow()
        self.assertEqual(started["decision"], "started")
        run_id = started["run_id"]
        self.assertIsNotNone(run_id)

        _, workflow_task = await self.latest_stream_payload(self.workflow_stream_key())
        self.assertEqual(workflow_task["kind"], "workflow_task")
        self.assertEqual(workflow_task["attempt_no"], 1)
        self.assertEqual(workflow_task["workflow_name"], "CheckoutWorkflow")

        schedule_response = await self.client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task["workflow_task_id"],
                "attempt_no": workflow_task["attempt_no"],
                "command": {
                    "kind": "schedule_activity",
                    "activity_name": "reserve_stock",
                    "activity_task_queue": "stock",
                    "input_payload_b64": self.payload_b64({"order_id": "checkout:1"}),
                    "retry_policy": {"max_attempts": 1},
                    "schedule_to_close_timeout_ms": 200,
                },
            },
        )
        schedule_response.raise_for_status()
        schedule_body = schedule_response.json()
        self.assertEqual(schedule_body["outcome"], "scheduled_activity")
        activity_execution_id = schedule_body["activity_execution_id"]

        _, activity_task = await self.latest_stream_payload(self.activity_stream_key("stock"))
        self.assertEqual(activity_task["activity_execution_id"], activity_execution_id)
        self.assertEqual(activity_task["attempt_no"], 1)

        activity_response = await self.client.post(
            "/activity-tasks/complete",
            json={
                "activity_execution_id": activity_execution_id,
                "attempt_no": 1,
                "status": "completed",
                "result_payload_b64": self.payload_b64({"reserved": True}),
            },
        )
        activity_response.raise_for_status()
        self.assertEqual(activity_response.json()["outcome"], "accepted")

        _, workflow_task_2 = await self.latest_stream_payload(self.workflow_stream_key())
        self.assertEqual(workflow_task_2["attempt_no"], 1)
        self.assertEqual(workflow_task_2["workflow_name"], "CheckoutWorkflow")
        self.assertNotEqual(
            workflow_task_2["workflow_task_id"],
            workflow_task["workflow_task_id"],
        )

        completion_response = await self.client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task_2["workflow_task_id"],
                "attempt_no": workflow_task_2["attempt_no"],
                "command": {
                    "kind": "complete_workflow",
                    "result_payload_b64": self.payload_b64({"status": "paid"}),
                },
            },
        )
        completion_response.raise_for_status()
        self.assertEqual(completion_response.json()["outcome"], "completed")

        result_response = await self.client.get(f"/workflows/checkout:1/result", params={"wait_ms": 200})
        result_response.raise_for_status()
        self.assertEqual(result_response.json()["status"], "completed")
        self.assertTrue(result_response.json()["ready"])

        history_response = await self.client.get(f"/runs/{run_id}/history")
        history_response.raise_for_status()
        event_types = [event["event_type"] for event in history_response.json()["events"]]
        self.assertEqual(
            event_types,
            [
                "WorkflowStarted",
                "ActivityScheduled",
                "ActivityCompleted",
                "WorkflowCompleted",
            ],
        )

    async def test_start_or_attach_semantics(self) -> None:
        started = await self.start_workflow(start_policy="attach_or_start")
        run_id = started["run_id"]
        self.assertEqual(started["decision"], "started")

        attached = await self.start_workflow(start_policy="attach_or_start")
        self.assertEqual(attached["decision"], "attached")
        self.assertEqual(attached["run_id"], run_id)

        _, workflow_task = await self.latest_stream_payload(self.workflow_stream_key())
        completion_response = await self.client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task["workflow_task_id"],
                "attempt_no": workflow_task["attempt_no"],
                "command": {
                    "kind": "complete_workflow",
                    "result_payload_b64": self.payload_b64({"status": "already-paid"}),
                },
            },
        )
        completion_response.raise_for_status()

        terminal = await self.start_workflow(start_policy="attach_or_start")
        self.assertEqual(terminal["decision"], "existing_terminal")
        self.assertEqual(terminal["run_id"], run_id)
        self.assertEqual(terminal["status"], "completed")
        self.assertIsNotNone(terminal["result_payload_b64"])

    async def test_scheduler_retries_timeout_and_rejects_stale_completion(self) -> None:
        started = await self.start_workflow()
        run_id = started["run_id"]

        _, workflow_task = await self.latest_stream_payload(self.workflow_stream_key())
        schedule_response = await self.client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task["workflow_task_id"],
                "attempt_no": workflow_task["attempt_no"],
                "command": {
                    "kind": "schedule_activity",
                    "activity_name": "charge_payment",
                    "activity_task_queue": "payment",
                    "input_payload_b64": self.payload_b64({"amount": 10}),
                    "retry_policy": {
                        "max_attempts": 2,
                        "initial_interval_ms": 1,
                        "backoff_coefficient": 1.0,
                    },
                    "schedule_to_close_timeout_ms": 20,
                },
            },
        )
        activity_execution_id = schedule_response.json()["activity_execution_id"]

        _, activity_task_1 = await self.latest_stream_payload(self.activity_stream_key("payment"))
        self.assertEqual(activity_task_1["attempt_no"], 1)

        scheduler = FluxiScheduler(self.store, self.settings)
        await asyncio.sleep(0.05)
        timeout_run = await scheduler.run_once()
        self.assertEqual(timeout_run["timer_result"]["outcome"], "retry_scheduled")
        await asyncio.sleep(0.01)
        retry_run = await scheduler.run_once()
        self.assertEqual(retry_run["timer_result"]["outcome"], "retried")

        _, activity_task_2 = await self.latest_stream_payload(self.activity_stream_key("payment"))
        self.assertEqual(activity_task_2["attempt_no"], 2)

        stale_completion = await self.client.post(
            "/activity-tasks/complete",
            json={
                "activity_execution_id": activity_execution_id,
                "attempt_no": 1,
                "status": "completed",
                "result_payload_b64": self.payload_b64({"late": True}),
            },
        )
        stale_completion.raise_for_status()
        self.assertEqual(stale_completion.json()["outcome"], "stale")

        accepted_completion = await self.client.post(
            "/activity-tasks/complete",
            json={
                "activity_execution_id": activity_execution_id,
                "attempt_no": 2,
                "status": "completed",
                "result_payload_b64": self.payload_b64({"charged": True}),
            },
        )
        accepted_completion.raise_for_status()
        self.assertEqual(accepted_completion.json()["outcome"], "accepted")

        _, workflow_task_2 = await self.latest_stream_payload(self.workflow_stream_key())
        finalize_response = await self.client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task_2["workflow_task_id"],
                "attempt_no": workflow_task_2["attempt_no"],
                "command": {
                    "kind": "complete_workflow",
                    "result_payload_b64": self.payload_b64({"status": "charged"}),
                },
            },
        )
        finalize_response.raise_for_status()

        history_response = await self.client.get(f"/runs/{run_id}/history")
        event_types = [event["event_type"] for event in history_response.json()["events"]]
        self.assertIn("ActivityAttemptTimedOut", event_types)
        self.assertIn("ActivityRetryScheduled", event_types)

    async def test_cleanup_acknowledges_stale_pending_workflow_messages(self) -> None:
        started = await self.start_workflow()
        run_id = started["run_id"]

        message_id, workflow_task = await self.read_group_message(
            stream_key=self.workflow_stream_key(),
            group_name=self.settings.workflow_consumer_group,
            consumer_name="worker-1",
        )
        self.assertEqual(workflow_task["attempt_no"], 1)

        pending_before = await self.store.redis.xpending_range(
            self.workflow_stream_key(),
            self.settings.workflow_consumer_group,
            "-",
            "+",
            10,
        )
        self.assertEqual(len(pending_before), 1)

        scheduler = FluxiScheduler(self.store, self.settings)
        await asyncio.sleep(0.05)
        timer_result = await scheduler.run_once()
        self.assertEqual(timer_result["timer_result"]["outcome"], "retried")
        self.assertGreaterEqual(timer_result["cleaned_pending_entries"], 1)
        cleaned = await self.store.cleanup_stale_pending_entries()
        self.assertEqual(cleaned, 0)

        pending_after = await self.store.redis.xpending_range(
            self.workflow_stream_key(),
            self.settings.workflow_consumer_group,
            "-",
            "+",
            10,
        )
        self.assertEqual(pending_after, [])

        _, retried_workflow_task = await self.latest_stream_payload(self.workflow_stream_key())
        self.assertEqual(retried_workflow_task["workflow_task_id"], workflow_task["workflow_task_id"])
        self.assertEqual(retried_workflow_task["attempt_no"], 2)
        self.assertEqual(retried_workflow_task["workflow_name"], "CheckoutWorkflow")
