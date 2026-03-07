import os
from pathlib import Path
import sys

import pytest

TEST_DIR = Path(__file__).resolve().parents[1]
if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))

from saga_helpers import (
    BASE_URL,
    add_item_to_order,
    create_item,
    create_order,
    create_user,
    get_credit,
    get_order,
    get_stock,
)


RUN_SAGA_TESTS = os.environ.get("RUN_SAGA_TESTS", "0") == "1"


@pytest.mark.skipif(
    not RUN_SAGA_TESTS,
    reason="Set RUN_SAGA_TESTS=1 and run stack with TX_MODE=saga to execute behavior tests.",
)
class TestSagaBehavior:
    @pytest.mark.anyio
    async def test_checkout_happy_path_debits_payment_and_stock(self, client):
        user_id = await create_user(client, credit=200)
        item_id = await create_item(client, price=50, stock=5)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=2)

        checkout = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")
        assert checkout.status_code == 200, checkout.text
        assert await get_credit(client, user_id) == 100
        assert await get_stock(client, item_id) == 3
        assert (await get_order(client, order_id))["paid"] is True

    @pytest.mark.anyio
    async def test_checkout_payment_failure_compensates_stock(self, client):
        user_id = await create_user(client, credit=10)
        item_id = await create_item(client, price=50, stock=5)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=1)

        checkout = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")
        assert checkout.status_code == 400
        assert await get_credit(client, user_id) == 10
        assert await get_stock(client, item_id) == 5
        assert (await get_order(client, order_id))["paid"] is False

    @pytest.mark.anyio
    async def test_duplicate_checkout_is_idempotent(self, client):
        user_id = await create_user(client, credit=100)
        item_id = await create_item(client, price=10, stock=10)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=3)

        checkout_1 = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")
        checkout_2 = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")
        assert checkout_1.status_code == 200, checkout_1.text
        assert checkout_2.status_code == 200, checkout_2.text
        assert await get_stock(client, item_id) == 7
        assert await get_credit(client, user_id) == 70
        assert (await get_order(client, order_id))["paid"] is True

    @pytest.mark.anyio
    async def test_retry_after_failure_keeps_state_consistent(self, client):
        user_id = await create_user(client, credit=5)
        item_id = await create_item(client, price=10, stock=5)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=1)

        first_checkout = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")
        second_checkout = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")

        assert first_checkout.status_code == 400
        assert second_checkout.status_code == 400
        assert await get_credit(client, user_id) == 5
        assert await get_stock(client, item_id) == 5
        assert (await get_order(client, order_id))["paid"] is False
