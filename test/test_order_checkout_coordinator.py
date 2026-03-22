from __future__ import annotations

from dataclasses import dataclass
import logging
import unittest

from flask import Flask
from werkzeug.exceptions import BadRequest

import fluxi_sdk_test_support  # noqa: F401
from checkout_coordinator import FluxiCheckoutCoordinator


@dataclass
class _OrderEntry:
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


@dataclass(frozen=True)
class _GatewayReply:
    status_code: int


class _GatewayStub:
    def __init__(self, responses: dict[str, int]) -> None:
        self._responses = responses
        self.calls: list[str] = []

    def post(self, url: str) -> _GatewayReply:
        self.calls.append(url)
        if url not in self._responses:
            raise AssertionError(f"Unexpected gateway call: {url}")
        return _GatewayReply(status_code=self._responses[url])


class TestFluxiCheckoutCoordinator(unittest.TestCase):
    def setUp(self) -> None:
        self._app = Flask(__name__)
        self._logger = logging.getLogger("test.fluxi.checkout")

    def test_fluxi_checkout_happy_path_returns_existing_http_contract(self):
        order_entry = _OrderEntry(
            paid=False,
            items=[("item-1", 1), ("item-1", 2), ("item-2", 1)],
            user_id="user-1",
            total_cost=35,
        )
        gateway = _GatewayStub(
            {
                "http://gateway/stock/subtract/item-1/3": 200,
                "http://gateway/stock/subtract/item-2/1": 200,
                "http://gateway/payment/pay/user-1/35": 200,
            }
        )
        saved_orders: list[tuple[str, bool]] = []

        coordinator = FluxiCheckoutCoordinator(
            gateway_url="http://gateway",
            get_order=lambda order_id: order_entry,
            save_order=lambda order_id, saved_order: saved_orders.append(
                (order_id, saved_order.paid)
            ),
            send_post_request=gateway.post,
            logger=self._logger,
        )

        with self._app.test_request_context():
            response = coordinator.checkout("order-1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_data(as_text=True), "Checkout successful")
        self.assertTrue(order_entry.paid)
        self.assertEqual(saved_orders, [("order-1", True)])
        self.assertEqual(
            gateway.calls,
            [
                "http://gateway/stock/subtract/item-1/3",
                "http://gateway/stock/subtract/item-2/1",
                "http://gateway/payment/pay/user-1/35",
            ],
        )

    def test_fluxi_checkout_payment_failure_restores_stock_and_raises_bad_request(
        self,
    ) -> None:
        order_entry = _OrderEntry(
            paid=False,
            items=[("item-1", 2), ("item-2", 1)],
            user_id="user-2",
            total_cost=20,
        )
        gateway = _GatewayStub(
            {
                "http://gateway/stock/subtract/item-1/2": 200,
                "http://gateway/stock/subtract/item-2/1": 200,
                "http://gateway/payment/pay/user-2/20": 400,
                "http://gateway/stock/add/item-2/1": 200,
                "http://gateway/stock/add/item-1/2": 200,
            }
        )
        saved_orders: list[tuple[str, bool]] = []

        coordinator = FluxiCheckoutCoordinator(
            gateway_url="http://gateway",
            get_order=lambda order_id: order_entry,
            save_order=lambda order_id, saved_order: saved_orders.append(
                (order_id, saved_order.paid)
            ),
            send_post_request=gateway.post,
            logger=self._logger,
        )

        with self._app.test_request_context():
            with self.assertRaises(BadRequest) as context:
                coordinator.checkout("order-2")

        self.assertEqual(context.exception.description, "User out of credit")
        self.assertFalse(order_entry.paid)
        self.assertEqual(saved_orders, [])
        self.assertEqual(
            gateway.calls,
            [
                "http://gateway/stock/subtract/item-1/2",
                "http://gateway/stock/subtract/item-2/1",
                "http://gateway/payment/pay/user-2/20",
                "http://gateway/stock/add/item-2/1",
                "http://gateway/stock/add/item-1/2",
            ],
        )

    def test_fluxi_checkout_out_of_stock_maps_to_bad_request(self) -> None:
        order_entry = _OrderEntry(
            paid=False,
            items=[("item-7", 1)],
            user_id="user-7",
            total_cost=5,
        )
        gateway = _GatewayStub(
            {
                "http://gateway/stock/subtract/item-7/1": 400,
            }
        )

        coordinator = FluxiCheckoutCoordinator(
            gateway_url="http://gateway",
            get_order=lambda order_id: order_entry,
            save_order=lambda order_id, saved_order: None,
            send_post_request=gateway.post,
            logger=self._logger,
        )

        with self._app.test_request_context():
            with self.assertRaises(BadRequest) as context:
                coordinator.checkout("order-7")

        self.assertEqual(
            context.exception.description,
            "Out of stock on item_id: item-7",
        )
        self.assertFalse(order_entry.paid)
        self.assertEqual(
            gateway.calls,
            ["http://gateway/stock/subtract/item-7/1"],
        )


if __name__ == "__main__":
    unittest.main()
