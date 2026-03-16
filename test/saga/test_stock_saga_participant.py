import asyncio
from pathlib import Path
import sys
import uuid

import pytest


TEST_DIR = Path(__file__).resolve().parents[1]
if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))

from saga_helpers import BASE_URL, create_item, get_stock


@pytest.mark.anyio
async def test_reserve_unknown_item_rejected(client):
    tx_id = str(uuid.uuid4())
    missing_item = str(uuid.uuid4())

    response = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{missing_item}/1")
    assert 400 <= response.status_code < 500


@pytest.mark.anyio
async def test_reserve_insufficient_stock_rejected(client):
    item_id = await create_item(client, price=10, stock=1)
    tx_id = str(uuid.uuid4())

    response = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/2")
    assert 400 <= response.status_code < 500
    assert await get_stock(client, item_id) == 1


@pytest.mark.anyio
async def test_reserve_amount_must_be_positive(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    zero_amount = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/0")
    negative_amount = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/-1")

    assert 400 <= zero_amount.status_code < 500
    assert 400 <= negative_amount.status_code < 500
    assert await get_stock(client, item_id) == 5


@pytest.mark.anyio
async def test_reserve_non_integer_amount_rejected_by_router(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    response = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/abc")
    assert response.status_code == 422
    assert await get_stock(client, item_id) == 5


@pytest.mark.anyio
async def test_reserve_idempotent(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    r1 = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/3")
    r2 = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/3")

    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    assert await get_stock(client, item_id) == 2


@pytest.mark.anyio
async def test_reserve_amount_mismatch_rejected(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    ok = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/2")
    bad = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/3")

    assert ok.status_code == 200, ok.text
    assert 400 <= bad.status_code < 500
    assert await get_stock(client, item_id) == 3


@pytest.mark.anyio
async def test_release_idempotent(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    reserve = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/2")
    rel1 = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{item_id}/2")
    rel2 = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{item_id}/2")

    assert reserve.status_code == 200, reserve.text
    assert rel1.status_code == 200, rel1.text
    assert rel2.status_code == 200, rel2.text
    assert await get_stock(client, item_id) == 5


@pytest.mark.anyio
async def test_release_without_reserve_is_noop(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    rel = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{item_id}/2")
    assert rel.status_code == 200, rel.text
    assert await get_stock(client, item_id) == 5


@pytest.mark.anyio
async def test_release_amount_mismatch_rejected(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    reserve = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/2")
    bad_release = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{item_id}/3")

    assert reserve.status_code == 200, reserve.text
    assert 400 <= bad_release.status_code < 500
    assert await get_stock(client, item_id) == 3


@pytest.mark.anyio
async def test_reserve_after_release_same_tx_rejected(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    reserve = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/2")
    release = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{item_id}/2")
    second_reserve = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/2")

    assert reserve.status_code == 200, reserve.text
    assert release.status_code == 200, release.text
    assert 400 <= second_reserve.status_code < 500
    assert await get_stock(client, item_id) == 5


@pytest.mark.anyio
async def test_release_unknown_item_without_reserve_is_noop(client):
    tx_id = str(uuid.uuid4())
    missing_item = str(uuid.uuid4())

    response = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{missing_item}/1")
    assert response.status_code == 200, response.text


@pytest.mark.anyio
async def test_oversell_protection_parallel_two_txs(client):
    item_id = await create_item(client, price=10, stock=1)
    tx_a = str(uuid.uuid4())
    tx_b = str(uuid.uuid4())

    r1, r2 = await asyncio.gather(
        client.post(f"{BASE_URL}/stock/saga/reserve/{tx_a}/{item_id}/1"),
        client.post(f"{BASE_URL}/stock/saga/reserve/{tx_b}/{item_id}/1"),
    )

    codes = sorted([r1.status_code, r2.status_code])
    assert codes == [200, 400] or codes == [200, 409]
    assert await get_stock(client, item_id) == 0


@pytest.mark.anyio
async def test_duplicate_parallel_reserve_decrements_once(client):
    item_id = await create_item(client, price=10, stock=5)
    tx_id = str(uuid.uuid4())

    reqs = [
        client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/2")
        for _ in range(20)
    ]
    responses = await asyncio.gather(*reqs)
    assert all(r.status_code == 200 for r in responses), [r.text for r in responses]
    assert await get_stock(client, item_id) == 3
