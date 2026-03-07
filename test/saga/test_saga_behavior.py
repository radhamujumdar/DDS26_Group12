import os
import uuid
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
    async def test_payment_debit_and_refund_are_idempotent(self, client):
        user_id = await create_user(client, credit=100)
        tx_id = str(uuid.uuid4())

        first_debit = await client.post(f"{BASE_URL}/payment/saga/debit/{tx_id}/{user_id}/30")
        second_debit = await client.post(f"{BASE_URL}/payment/saga/debit/{tx_id}/{user_id}/30")
        first_refund = await client.post(f"{BASE_URL}/payment/saga/refund/{tx_id}")
        second_refund = await client.post(f"{BASE_URL}/payment/saga/refund/{tx_id}")

        assert first_debit.status_code == 200, first_debit.text
        assert second_debit.status_code == 200, second_debit.text
        assert first_refund.status_code == 200, first_refund.text
        assert second_refund.status_code == 200, second_refund.text
        assert await get_credit(client, user_id) == 100

    @pytest.mark.anyio
    async def test_stock_reserve_and_release_are_idempotent(self, client):
        item_id = await create_item(client, price=10, stock=10)
        tx_id = str(uuid.uuid4())

        first_reserve = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/3")
        second_reserve = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/3")
        first_release = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{item_id}/3")
        second_release = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{item_id}/3")

        assert first_reserve.status_code == 200, first_reserve.text
        assert second_reserve.status_code == 200, second_reserve.text
        assert first_release.status_code == 200, first_release.text
        assert second_release.status_code == 200, second_release.text
        assert await get_stock(client, item_id) == 10
