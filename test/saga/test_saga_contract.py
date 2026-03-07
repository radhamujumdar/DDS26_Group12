import uuid
from pathlib import Path
import sys

import pytest

TEST_DIR = Path(__file__).resolve().parents[1]
if str(TEST_DIR) not in sys.path:
    sys.path.insert(0, str(TEST_DIR))

from saga_helpers import BASE_URL, create_item, create_user


class TestSagaContract:
    @pytest.mark.anyio
    async def test_compose_exposes_transaction_mode_toggle(self):
        compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
        content = compose_path.read_text(encoding="utf-8")
        assert "TX_MODE=${TX_MODE:-2pc}" in content

    @pytest.mark.anyio
    async def test_order_registers_saga_tx_status_endpoint(self):
        order_app_path = Path(__file__).resolve().parents[2] / "order" / "app.py"
        content = order_app_path.read_text(encoding="utf-8")
        assert '"/saga/tx/{tx_id}"' in content

    @pytest.mark.anyio
    async def test_payment_saga_routes_are_registered(self, client):
        user_id = await create_user(client, credit=100)
        tx_id = str(uuid.uuid4())

        debit = await client.post(f"{BASE_URL}/payment/saga/debit/{tx_id}/{user_id}/10")
        assert debit.status_code != 404, debit.text
        assert debit.status_code != 405, debit.text

        refund = await client.post(f"{BASE_URL}/payment/saga/refund/{tx_id}")
        assert refund.status_code != 404, refund.text
        assert refund.status_code != 405, refund.text

    @pytest.mark.anyio
    async def test_stock_saga_routes_are_registered(self, client):
        item_id = await create_item(client, price=10, stock=10)
        tx_id = str(uuid.uuid4())

        reserve = await client.post(f"{BASE_URL}/stock/saga/reserve/{tx_id}/{item_id}/2")
        assert reserve.status_code != 404, reserve.text
        assert reserve.status_code != 405, reserve.text

        release = await client.post(f"{BASE_URL}/stock/saga/release/{tx_id}/{item_id}/2")
        assert release.status_code != 404, release.text
        assert release.status_code != 405, release.text
