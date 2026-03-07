"""
Behavior and regression tests for the distributed shopping-cart system.

The suite is split into:
1. Core integration behavior that should pass now.
2. Operational/config checks.
3. Consistency and recovery regressions.
"""

import asyncio
import random
import uuid
from pathlib import Path

import httpx
import pytest

BASE_URL = "http://localhost:8000"


async def create_user(client: httpx.AsyncClient, credit: int = 0) -> str:
    response = await client.post(f"{BASE_URL}/payment/create_user")
    assert response.status_code == 200, response.text
    user_id = response.json()["user_id"]

    if credit > 0:
        add_funds = await client.post(f"{BASE_URL}/payment/add_funds/{user_id}/{credit}")
        assert add_funds.status_code == 200, add_funds.text

    return user_id


async def create_item(client: httpx.AsyncClient, price: int, stock: int = 0) -> str:
    response = await client.post(f"{BASE_URL}/stock/item/create/{price}")
    assert response.status_code == 200, response.text
    item_id = response.json()["item_id"]

    if stock > 0:
        add_stock = await client.post(f"{BASE_URL}/stock/add/{item_id}/{stock}")
        assert add_stock.status_code == 200, add_stock.text

    return item_id


async def create_order(client: httpx.AsyncClient, user_id: str) -> str:
    response = await client.post(f"{BASE_URL}/orders/create/{user_id}")
    assert response.status_code == 200, response.text
    return response.json()["order_id"]


async def add_item_to_order(client: httpx.AsyncClient, order_id: str, item_id: str, quantity: int) -> None:
    response = await client.post(f"{BASE_URL}/orders/addItem/{order_id}/{item_id}/{quantity}")
    assert response.status_code == 200, response.text


async def get_credit(client: httpx.AsyncClient, user_id: str) -> int:
    response = await client.get(f"{BASE_URL}/payment/find_user/{user_id}")
    assert response.status_code == 200, response.text
    return response.json()["credit"]


async def get_stock(client: httpx.AsyncClient, item_id: str) -> int:
    response = await client.get(f"{BASE_URL}/stock/find/{item_id}")
    assert response.status_code == 200, response.text
    return response.json()["stock"]


async def get_order(client: httpx.AsyncClient, order_id: str) -> dict:
    response = await client.get(f"{BASE_URL}/orders/find/{order_id}")
    assert response.status_code == 200, response.text
    return response.json()


def _extract_async_function_source(path: Path, function_name: str, next_function_name: str | None = None) -> str:
    source = path.read_text(encoding="utf-8")
    start = source.find(f"async def {function_name}")
    assert start != -1, f"Could not find function {function_name} in {path}"
    if next_function_name is None:
        return source[start:]
    end = source.find(f"async def {next_function_name}", start)
    assert end != -1, f"Could not find function {next_function_name} in {path}"
    return source[start:end]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=10.0) as session:
        yield session


class TestCoreCheckoutFlow:
    @pytest.mark.anyio
    async def test_successful_checkout_updates_credit_stock_and_order_state(self, client):
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
    async def test_failed_checkout_keeps_credit_stock_and_order_state(self, client):
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
    async def test_competing_checkouts_have_single_success(self, client):
        user_id = await create_user(client, credit=100)
        item_id = await create_item(client, price=100, stock=1)

        order_1 = await create_order(client, user_id)
        order_2 = await create_order(client, user_id)
        await add_item_to_order(client, order_1, item_id, quantity=1)
        await add_item_to_order(client, order_2, item_id, quantity=1)

        responses = await asyncio.gather(
            client.post(f"{BASE_URL}/orders/checkout/{order_1}"),
            client.post(f"{BASE_URL}/orders/checkout/{order_2}"),
            return_exceptions=True,
        )

        status_codes = [response.status_code if isinstance(response, httpx.Response) else 500 for response in responses]
        assert status_codes.count(200) == 1, status_codes
        assert status_codes.count(400) == 1, status_codes

        assert await get_stock(client, item_id) == 0
        assert await get_credit(client, user_id) == 0


class TestParticipantSemantics:
    @pytest.mark.anyio
    async def test_payment_abort_restores_reserved_credit(self, client):
        user_id = await create_user(client, credit=100)
        txn_a = str(uuid.uuid4())
        txn_b = str(uuid.uuid4())

        assert (await client.post(f"{BASE_URL}/payment/2pc/prepare/{txn_a}/{user_id}/40")).status_code == 200
        assert (await client.post(f"{BASE_URL}/payment/2pc/prepare/{txn_b}/{user_id}/40")).status_code == 200
        assert (await client.post(f"{BASE_URL}/payment/2pc/commit/{txn_b}")).status_code == 200
        assert (await client.post(f"{BASE_URL}/payment/2pc/abort/{txn_a}")).status_code == 200

        assert await get_credit(client, user_id) == 60

    @pytest.mark.anyio
    async def test_stock_abort_is_idempotent(self, client):
        item_id = await create_item(client, price=10, stock=10)
        txn_id = str(uuid.uuid4())

        assert (await client.post(f"{BASE_URL}/stock/2pc/prepare/{txn_id}/{item_id}/3")).status_code == 200
        assert await get_stock(client, item_id) == 7

        for _ in range(3):
            abort_response = await client.post(f"{BASE_URL}/stock/2pc/abort/{txn_id}/{item_id}/3")
            assert abort_response.status_code == 200, abort_response.text

        assert await get_stock(client, item_id) == 10

    @pytest.mark.anyio
    async def test_direct_pay_is_blocked_while_prepared_transaction_exists(self, client):
        user_id = await create_user(client, credit=100)
        txn_id = str(uuid.uuid4())

        assert (await client.post(f"{BASE_URL}/payment/2pc/prepare/{txn_id}/{user_id}/40")).status_code == 200
        blocked_pay = await client.post(f"{BASE_URL}/payment/pay/{user_id}/30")
        assert 400 <= blocked_pay.status_code < 500, blocked_pay.text

        assert (await client.post(f"{BASE_URL}/payment/2pc/commit/{txn_id}")).status_code == 200
        allowed_pay = await client.post(f"{BASE_URL}/payment/pay/{user_id}/10")
        assert allowed_pay.status_code == 200, allowed_pay.text


class TestOperationalConfiguration:
    def test_watchdog_command_uses_service_label_lookup(self):
        compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
        content = compose_path.read_text(encoding="utf-8")
        assert "label=com.docker.compose.service=" in content
        assert "dds_group12" not in content


class TestKnownConsistencyGaps:
    def test_coordinator_does_not_abort_after_commit_phase_starts(self):
        coordinator_path = Path(__file__).resolve().parents[1] / "order" / "coordinator" / "two_pc.py"
        commit_phase_source = _extract_async_function_source(coordinator_path, "_commit_phase", "_abort_phase")
        assert "_start_abort(" not in commit_phase_source

    @pytest.mark.anyio
    async def test_payment_commit_and_abort_are_terminally_exclusive(self, client):
        for attempt in range(80):
            user_id = await create_user(client, credit=100)
            txn_id = str(uuid.uuid4())
            prepared = await client.post(f"{BASE_URL}/payment/2pc/prepare/{txn_id}/{user_id}/40")
            assert prepared.status_code == 200, prepared.text

            await asyncio.sleep(random.random() * 0.01)
            commit_response, abort_response = await asyncio.gather(
                client.post(f"{BASE_URL}/payment/2pc/commit/{txn_id}"),
                client.post(f"{BASE_URL}/payment/2pc/abort/{txn_id}"),
            )

            if commit_response.status_code == 200 and abort_response.status_code == 200:
                final_credit = await get_credit(client, user_id)
                pytest.fail(
                    f"Attempt {attempt}: commit and abort both returned 200 for txn {txn_id}; "
                    f"credit={final_credit}"
                )

    @pytest.mark.anyio
    async def test_stock_commit_and_abort_are_terminally_exclusive(self, client):
        item_id = await create_item(client, price=10, stock=5000)

        for attempt in range(80):
            txn_id = str(uuid.uuid4())
            prepared = await client.post(f"{BASE_URL}/stock/2pc/prepare/{txn_id}/{item_id}/1")
            assert prepared.status_code == 200, prepared.text

            await asyncio.sleep(random.random() * 0.01)
            commit_response, abort_response = await asyncio.gather(
                client.post(f"{BASE_URL}/stock/2pc/commit/{txn_id}/{item_id}/1"),
                client.post(f"{BASE_URL}/stock/2pc/abort/{txn_id}/{item_id}/1"),
            )

            if commit_response.status_code == 200 and abort_response.status_code == 200:
                final_stock = await get_stock(client, item_id)
                pytest.fail(
                    f"Attempt {attempt}: commit and abort both returned 200 for txn {txn_id}; "
                    f"stock={final_stock}"
                )

    @pytest.mark.anyio
    async def test_negative_values_are_rejected_by_order_payment_and_stock(self, client):
        user_id = await create_user(client, credit=100)
        item_id = await create_item(client, price=5, stock=20)
        order_id = await create_order(client, user_id)

        negative_item = await client.post(f"{BASE_URL}/orders/addItem/{order_id}/{item_id}/-2")
        assert 400 <= negative_item.status_code < 500

        negative_payment = await client.post(f"{BASE_URL}/payment/2pc/prepare/{uuid.uuid4()}/{user_id}/-10")
        assert 400 <= negative_payment.status_code < 500

        negative_stock = await client.post(f"{BASE_URL}/stock/2pc/prepare/{uuid.uuid4()}/{item_id}/-3")
        assert 400 <= negative_stock.status_code < 500

    @pytest.mark.anyio
    async def test_paid_order_is_not_mutable(self, client):
        user_id = await create_user(client, credit=200)
        item_id = await create_item(client, price=10, stock=20)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, quantity=1)

        checkout = await client.post(f"{BASE_URL}/orders/checkout/{order_id}")
        assert checkout.status_code == 200, checkout.text

        add_after_paid = await client.post(f"{BASE_URL}/orders/addItem/{order_id}/{item_id}/1")
        assert 400 <= add_after_paid.status_code < 500

    def test_payment_prepare_lookup_propagates_db_errors(self):
        payment_path = Path(__file__).resolve().parents[1] / "payment" / "repository" / "payment_repo.py"
        function_source = _extract_async_function_source(payment_path, "get_prepare_record", "save_prepare_record")
        assert "except RedisError" in function_source
        assert "raise HTTPException" in function_source

    def test_payment_prepared_scan_surfaces_fetch_and_decode_errors(self):
        payment_path = Path(__file__).resolve().parents[1] / "payment" / "repository" / "payment_repo.py"
        function_source = _extract_async_function_source(payment_path, "list_prepared_tx_ids", "has_active_prepare")
        assert "except RedisError as exc" in function_source
        assert "raise HTTPException" in function_source
        assert "except HTTPException" not in function_source

    @pytest.mark.anyio
    async def test_direct_pay_and_prepare_cannot_both_succeed(self, client):
        for attempt in range(80):
            user_id = await create_user(client, credit=100)
            txn_id = str(uuid.uuid4())

            await asyncio.sleep(random.random() * 0.005)
            pay_response, prepare_response = await asyncio.gather(
                client.post(f"{BASE_URL}/payment/pay/{user_id}/30"),
                client.post(f"{BASE_URL}/payment/2pc/prepare/{txn_id}/{user_id}/80"),
            )

            if pay_response.status_code == 200 and prepare_response.status_code == 200:
                final_credit = await get_credit(client, user_id)
                pytest.fail(
                    f"Attempt {attempt}: /pay and /2pc/prepare both returned 200 for user {user_id}; "
                    f"credit={final_credit}"
                )

    @pytest.mark.anyio
    async def test_stock_prepare_replay_with_changed_amount_is_rejected(self, client):
        item_id = await create_item(client, price=10, stock=20)
        txn_id = str(uuid.uuid4())

        first_prepare = await client.post(f"{BASE_URL}/stock/2pc/prepare/{txn_id}/{item_id}/1")
        assert first_prepare.status_code == 200, first_prepare.text

        mismatched_prepare = await client.post(f"{BASE_URL}/stock/2pc/prepare/{txn_id}/{item_id}/3")
        assert 400 <= mismatched_prepare.status_code < 500

    @pytest.mark.anyio
    async def test_orphaned_prepared_reservation_is_recovered(self, client):
        item_id = await create_item(client, price=10, stock=1)
        txn_a = str(uuid.uuid4())
        txn_b = str(uuid.uuid4())

        prepared = await client.post(f"{BASE_URL}/stock/2pc/prepare/{txn_a}/{item_id}/1")
        assert prepared.status_code == 200, prepared.text

        # Stock recovery loop should abort orphaned prepare after coordinator reports unknown txn.
        recovered = False
        for _ in range(30):
            retry = await client.post(f"{BASE_URL}/stock/2pc/prepare/{txn_b}/{item_id}/1")
            if retry.status_code == 200:
                recovered = True
                break
            await asyncio.sleep(0.2)

        assert recovered, "Prepared reservation did not recover within the expected window"
        assert await get_stock(client, item_id) == 0
