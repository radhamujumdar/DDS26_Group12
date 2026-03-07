from pathlib import Path


class TestSagaContract:
    def test_compose_exposes_tx_mode_and_saga_broker(self):
        compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
        content = compose_path.read_text(encoding="utf-8")
        assert "TX_MODE=${TX_MODE:-2pc}" in content
        assert "saga-broker:" in content
        assert "SAGA_MQ_REDIS_HOST=saga-broker" in content

    def test_order_wires_saga_command_bus(self):
        order_app_path = Path(__file__).resolve().parents[2] / "order" / "app.py"
        content = order_app_path.read_text(encoding="utf-8")
        assert "SagaCommandBus" in content
        assert "if TX_MODE == TxMode.SAGA.value:" in content

    def test_mq_contract_document_exists(self):
        contract_path = Path(__file__).resolve().parents[2] / "order" / "SAGA_MQ_CONTRACT.md"
        content = contract_path.read_text(encoding="utf-8")
        assert "saga:cmd:stock" in content
        assert "saga:cmd:payment" in content
        assert "reply_stream" in content
        assert "operation_id" in content
        assert "ok" in content
        assert "retryable" in content
