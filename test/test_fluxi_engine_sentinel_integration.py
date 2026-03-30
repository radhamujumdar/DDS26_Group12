from __future__ import annotations

import asyncio

from fluxi_engine.scheduler import FluxiScheduler
from fluxi_engine_test_support import FluxiSentinelEngineAsyncTestCase


class TestFluxiEngineSentinelIntegration(FluxiSentinelEngineAsyncTestCase):

    async def test_start_or_attach_and_completion_work_in_sentinel_mode(self) -> None:
        master_host, master_port = self.harness.wait_until_ready()
        self.assertTrue(master_host)
        self.assertIn(master_port, {self.harness.master_port, self.harness.replica_port})

        started = await self.start_workflow(start_policy="attach_or_start")
        self.assertEqual(started["decision"], "started")
        run_id = started["run_id"]
        self.assertIsNotNone(run_id)

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
                    "result_payload_b64": self.payload_b64({"status": "paid"}),
                },
            },
        )
        completion_response.raise_for_status()
        self.assertEqual(completion_response.json()["outcome"], "completed")

        result_response = await self.client.get(
            "/workflows/checkout:1/result",
            params={"wait_ms": 100},
        )
        result_response.raise_for_status()
        self.assertEqual(result_response.json()["status"], "completed")

    async def test_engine_operations_continue_after_master_failover(self) -> None:
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
                    "retry_policy": {"max_attempts": 1},
                    "schedule_to_close_timeout_ms": 200,
                },
            },
        )
        schedule_response.raise_for_status()
        activity_execution_id = schedule_response.json()["activity_execution_id"]

        old_master = self.harness.wait_until_ready()
        new_master = self.harness.force_failover()
        self.assertNotEqual(old_master, new_master)

        activity_response = await self.client.post(
            "/activity-tasks/complete",
            json={
                "activity_execution_id": activity_execution_id,
                "attempt_no": 1,
                "status": "completed",
                "result_payload_b64": self.payload_b64({"charged": True}),
            },
        )
        activity_response.raise_for_status()
        self.assertEqual(activity_response.json()["outcome"], "accepted")

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
        self.assertEqual(finalize_response.json()["outcome"], "completed")

    async def test_scheduler_resumes_timeout_processing_after_failover(self) -> None:
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
        schedule_response.raise_for_status()
        activity_execution_id = schedule_response.json()["activity_execution_id"]

        scheduler = FluxiScheduler(self.store, self.settings)
        await asyncio.sleep(0.05)
        old_master = self.harness.wait_until_ready()
        new_master = self.harness.force_failover()
        self.assertNotEqual(old_master, new_master)

        timeout_run = await scheduler.run_once()
        self.assertEqual(timeout_run["timer_result"]["outcome"], "retry_scheduled")

        await asyncio.sleep(0.01)
        retry_run = await scheduler.run_once()
        self.assertEqual(retry_run["timer_result"]["outcome"], "retried")

        _, activity_task = await self.latest_stream_payload(self.activity_stream_key("payment"))
        self.assertEqual(activity_task["activity_execution_id"], activity_execution_id)
        self.assertEqual(activity_task["attempt_no"], 2)
