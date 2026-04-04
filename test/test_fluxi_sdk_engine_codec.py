import unittest

import fluxi_sdk_test_support  # noqa: F401

from fluxi_sdk._codec import (
    decode_args_payload,
    decode_failure_payload,
    decode_payload,
    encode_args_payload,
    encode_failure_payload,
    encode_payload,
)
from fluxi_sdk._engine_backend import _decode_workflow_failure
from fluxi_sdk.errors import RemoteActivityError
from fluxi_sdk.examples.checkout import (
    CheckoutItem,
    CheckoutOrder,
    CheckoutWorkflowResult,
    PaymentDeclinedError,
    PaymentReceipt,
    StockReservation,
)


class TestFluxiSdkEngineCodec(unittest.TestCase):

    def test_round_trips_nested_dataclass_payload(self):
        payload = encode_payload(
            CheckoutWorkflowResult(
                order_id="order-1",
                status="paid",
                payment=PaymentReceipt(
                    payment_id="payment-1",
                    user_id="user-1",
                    amount=20,
                ),
                released_items=(
                    StockReservation(item_id="item-1", quantity=2),
                ),
            )
        )

        decoded = decode_payload(payload)

        self.assertEqual(
            decoded,
            CheckoutWorkflowResult(
                order_id="order-1",
                status="paid",
                payment=PaymentReceipt(
                    payment_id="payment-1",
                    user_id="user-1",
                    amount=20,
                ),
                released_items=(
                    StockReservation(item_id="item-1", quantity=2),
                ),
            ),
        )

    def test_preserves_tuple_structure(self):
        payload = encode_payload(("alpha", 2, True))

        decoded = decode_payload(payload)

        self.assertEqual(decoded, ("alpha", 2, True))
        self.assertIsInstance(decoded, tuple)

    def test_decodes_args_using_function_annotations(self):
        payload = encode_args_payload(
            (
                CheckoutOrder(
                    order_id="order-1",
                    user_id="user-1",
                    total_cost=20,
                    items=(CheckoutItem(item_id="item-1", quantity=2),),
                ),
                3,
            )
        )

        def handler(order: CheckoutOrder, attempt: int) -> None:
            return None

        decoded = decode_args_payload(payload, handler)

        self.assertIsInstance(decoded[0], CheckoutOrder)
        self.assertIsInstance(decoded[0].items[0], CheckoutItem)
        self.assertEqual(decoded[1], 3)

    def test_rehydrates_importable_exception_types(self):
        payload = encode_failure_payload(
            PaymentDeclinedError("User 'user-1' has insufficient credit.")
        )

        decoded = decode_failure_payload(
            payload,
            fallback_error=RemoteActivityError,
        )

        self.assertIsInstance(decoded, PaymentDeclinedError)
        self.assertEqual(str(decoded), "User 'user-1' has insufficient credit.")

    def test_uses_fallback_for_unimportable_exceptions(self):
        class LocalFailure(Exception):
            pass

        payload = encode_failure_payload(LocalFailure("boom"))

        decoded = decode_failure_payload(
            payload,
            fallback_error=RemoteActivityError,
        )

        self.assertIsInstance(decoded, RemoteActivityError)
        self.assertEqual(decoded.remote_qualname, "TestFluxiSdkEngineCodec.test_uses_fallback_for_unimportable_exceptions.<locals>.LocalFailure")
        self.assertEqual(decoded.remote_args, ("boom",))

    def test_decode_workflow_failure_falls_back_when_payload_is_missing(self):
        decoded = _decode_workflow_failure(None)

        self.assertIsInstance(decoded, Exception)
        self.assertEqual(
            str(decoded),
            "Workflow failed without an encoded failure payload.",
        )

    def test_decode_workflow_failure_falls_back_when_payload_is_invalid(self):
        decoded = _decode_workflow_failure("bm90LWEtZmFpbHVyZS1lbnZlbG9wZQ==")

        self.assertIsInstance(decoded, Exception)
        self.assertEqual(
            str(decoded),
            "Workflow failed with an invalid failure payload.",
        )


if __name__ == "__main__":
    unittest.main()
