import unittest
from pathlib import Path
import os
import shutil
import socket
import subprocess
import time

import requests

import utils as tu


class TestMicroservices(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        docker = shutil.which("docker")
        if docker is None:
            raise unittest.SkipTest("Docker is not available for microservice tests.")

        try:
            subprocess.run(
                [docker, "info"],
                check=True,
                capture_output=True,
                text=True,
                timeout=15,
            )
        except Exception as exc:
            raise unittest.SkipTest(
                f"Docker is unavailable for microservice tests: {exc}"
            ) from exc

        cls._compose_env = os.environ.copy()
        cls._compose_env["DDS_GATEWAY_PORT"] = str(cls._find_free_port())
        cls._compose_env["DDS_FLUXI_SERVER_PORT"] = str(cls._find_free_port())
        cls._compose_env["DDS_GATEWAY_URL"] = (
            f"http://127.0.0.1:{cls._compose_env['DDS_GATEWAY_PORT']}"
        )
        os.environ["DDS_GATEWAY_URL"] = cls._compose_env["DDS_GATEWAY_URL"]

        cls._repo_root = Path(__file__).resolve().parents[1]
        subprocess.run(
            [docker, "compose", "up", "-d", "--build"],
            cwd=cls._repo_root,
            env=cls._compose_env,
            check=True,
            capture_output=True,
            text=True,
            timeout=600,
        )
        cls._wait_until_gateway_ready()

    @classmethod
    def tearDownClass(cls) -> None:
        docker = shutil.which("docker")
        if docker is not None:
            subprocess.run(
                [docker, "compose", "down", "-v", "--remove-orphans"],
                cwd=getattr(cls, "_repo_root", Path(__file__).resolve().parents[1]),
                env=getattr(cls, "_compose_env", os.environ.copy()),
                check=False,
                capture_output=True,
                text=True,
                timeout=180,
            )
        super().tearDownClass()

    @classmethod
    def _find_free_port(cls) -> int:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        return port

    @classmethod
    def _wait_until_gateway_ready(cls) -> None:
        deadline = time.time() + 120
        gateway_url = cls._compose_env["DDS_GATEWAY_URL"]
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                response = requests.get(gateway_url, timeout=2)
                if response.status_code in {200, 404}:
                    return
            except Exception as exc:
                last_error = exc
            time.sleep(1)
        raise TimeoutError(
            f"Timed out waiting for gateway to become ready: {last_error}"
        )

    def test_stock(self):
        # Test /stock/item/create/<price>
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        # Test /stock/find/<item_id>
        item: dict = tu.find_item(item_id)
        self.assertEqual(item['price'], 5)
        self.assertEqual(item['stock'], 0)

        # Test /stock/add/<item_id>/<number>
        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(200 <= int(add_stock_response) < 300)

        stock_after_add: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_add, 50)

        # Test /stock/subtract/<item_id>/<number>
        over_subtract_stock_response = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(int(over_subtract_stock_response)))

        subtract_stock_response = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(int(subtract_stock_response)))

        stock_after_subtract: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_subtract, 35)

    def test_payment(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # Test /users/credit/add/<user_id>/<amount>
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # add item to the stock service
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        payment_response = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_success(payment_response))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 5)

    def test_order(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        # add item to the stock service
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1)
        item_id1: str = item1['item_id']
        add_stock_response = tu.add_stock(item_id1, 15)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # add item to the stock service
        item2: dict = tu.create_item(5)
        self.assertIn('item_id', item2)
        item_id2: str = item2['item_id']
        add_stock_response = tu.add_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        add_item_response = tu.add_item_to_order(order_id, item_id1, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        subtract_stock_response = tu.subtract_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(subtract_stock_response))

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 15)

        add_stock_response = tu.add_stock(item_id2, 15)
        self.assertTrue(tu.status_code_is_success(int(add_stock_response)))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 0)

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(int(add_credit_response)))

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 15)

        stock: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock, 15)

        checkout_response = tu.checkout_order(order_id)
        print(checkout_response.text)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 14)

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 5)


if __name__ == '__main__':
    unittest.main()
