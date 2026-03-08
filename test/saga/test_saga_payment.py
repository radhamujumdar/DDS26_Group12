import asyncio
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
class TestSagaPaymentIdempotency:

    @pytest.mark.anyio
    async def test_duplicate_debit_charges_only_once(self, client):
        """Same checkout called twice after success: credit only debited once."""
        user_id = await create_user(client, credit=100)
        item_id = await create_item(client, price=30, stock=5)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=1)

        r1 = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")
        r2 = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")

        assert r1.status_code == 200, r1.text
        assert r2.status_code == 200, r2.text
        assert await get_credit(client, user_id) == 70
        assert await get_stock(client, item_id) == 4

    @pytest.mark.anyio
    async def test_refund_with_no_prior_debit_is_noop(self, client):
        """
        Payment compensation when payment was never charged (stock failed before
        payment step) must leave credit untouched.
        Simulated by: stock=0 so checkout fails before debit ever runs.
        """
        user_id = await create_user(client, credit=100)
        item_id = await create_item(client, price=10, stock=0)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=1)

        r = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")

        assert r.status_code == 400
        assert await get_credit(client, user_id) == 100
        assert (await get_order(client, order_id))["paid"] is False

    @pytest.mark.anyio
    async def test_refund_after_debit_restores_credit(self, client):
        """
        Stock for item 1 reserved, payment debited, then stock for item 2
        fails → compensation must refund payment and release stock for item 1.
        """
        user_id = await create_user(client, credit=200)
        item1_id = await create_item(client, price=50, stock=5)
        item2_id = await create_item(client, price=50, stock=0)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item1_id, quantity=1)
        await add_item_to_order(client, order_id, item2_id, quantity=1)

        r = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")

        assert r.status_code == 400
        assert await get_credit(client, user_id) == 200
        assert await get_stock(client, item1_id) == 5
        assert (await get_order(client, order_id))["paid"] is False

    @pytest.mark.anyio
    async def test_repeated_failed_checkout_does_not_leak_credit(self, client):
        """Calling a failing checkout multiple times never leaks credit."""
        user_id = await create_user(client, credit=5)
        item_id = await create_item(client, price=100, stock=5)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=1)

        for _ in range(3):
            r = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")
            assert r.status_code == 400

        assert await get_credit(client, user_id) == 5
        assert await get_stock(client, item_id) == 5

    @pytest.mark.anyio
    async def test_concurrent_checkouts_on_same_order_charge_once(self, client):
        """
        Three concurrent checkout calls on the same order: all must agree on
        the outcome and credit/stock must reflect a single charge.
        """
        user_id = await create_user(client, credit=100)
        item_id = await create_item(client, price=40, stock=5)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=1)

        results = await asyncio.gather(
            client.post(f"{BASE_URL}/orders/checkout/{order_id}"),
            client.post(f"{BASE_URL}/orders/checkout/{order_id}"),
            client.post(f"{BASE_URL}/orders/checkout/{order_id}"),
        )

        statuses = [r.status_code for r in results]
        assert all(s == 200 for s in statuses), f"Got statuses: {statuses}"
        assert await get_credit(client, user_id) == 60
        assert await get_stock(client, item_id) == 4

    @pytest.mark.anyio
    async def test_independent_orders_for_same_user_both_debit(self, client):
        """Two separate orders for the same user each debit correctly."""
        user_id = await create_user(client, credit=100)
        item_id = await create_item(client, price=30, stock=10)

        order1_id = await create_order(client, user_id)
        await add_item_to_order(client, order1_id, item_id, quantity=1)

        order2_id = await create_order(client, user_id)
        await add_item_to_order(client, order2_id, item_id, quantity=1)

        r1 = await client.post(f"{BASE_URL}/orders/checkout/{order1_id}")
        r2 = await client.post(f"{BASE_URL}/orders/checkout/{order2_id}")

        assert r1.status_code == 200, r1.text
        assert r2.status_code == 200, r2.text
        assert await get_credit(client, user_id) == 40
        assert await get_stock(client, item_id) == 8
