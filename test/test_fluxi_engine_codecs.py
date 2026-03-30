import unittest

import msgpack

import fluxi_sdk_test_support  # noqa: F401

from fluxi_engine.codecs import packb, unpackb


class TestFluxiEngineCodecs(unittest.TestCase):

    def test_unpackb_decodes_lua_style_payload_fields_as_bytes(self) -> None:
        payload = msgpack.packb(
            {
                b"kind": b"activity_task",
                b"activity_name": b"charge_payment",
                b"input_payload": b"\x81\xa6amount\n",
                b"metadata": {b"status": b"scheduled"},
            },
            use_bin_type=False,
        )

        decoded = unpackb(payload)

        self.assertEqual(decoded["kind"], "activity_task")
        self.assertEqual(decoded["activity_name"], "charge_payment")
        self.assertEqual(decoded["metadata"]["status"], "scheduled")
        self.assertEqual(decoded["input_payload"], b"\x81\xa6amount\n")

    def test_unpackb_preserves_nested_error_payload_bytes(self) -> None:
        payload = msgpack.packb(
            {
                b"event_type": b"ActivityFailed",
                b"error_payload": b"\x81\xa7message\xa4boom",
                b"activity_execution_id": b"activity-1",
            },
            use_bin_type=False,
        )

        decoded = unpackb(payload)

        self.assertEqual(decoded["event_type"], "ActivityFailed")
        self.assertEqual(decoded["activity_execution_id"], "activity-1")
        self.assertEqual(decoded["error_payload"], b"\x81\xa7message\xa4boom")

    def test_unpackb_uses_fast_path_for_regular_engine_payloads(self) -> None:
        encoded = packb(
            {
                "kind": "workflow_task",
                "attempt_no": 2,
                "workflow_name": "CheckoutWorkflow",
            }
        )

        decoded = unpackb(encoded)

        self.assertEqual(
            decoded,
            {
                "kind": "workflow_task",
                "attempt_no": 2,
                "workflow_name": "CheckoutWorkflow",
            },
        )


if __name__ == "__main__":
    unittest.main()
