from pathlib import Path


class TestSagaContract:
    def test_compose_exposes_tx_mode_and_saga_broker(self):
        compose_path = Path(__file__).resolve().parents[2] / "docker-compose.yml"
        content = compose_path.read_text(encoding="utf-8")
        assert "TX_MODE=${TX_MODE:-2pc}" in content
        assert "ENABLE_ORDER_DISPATCHER=${ENABLE_ORDER_DISPATCHER:-true}" in content
        assert "saga-broker:" in content
        assert "SAGA_MQ_REDIS_HOST=saga-broker" in content
        assert "SAGA_MQ_STREAM_PARTITIONS=${SAGA_MQ_STREAM_PARTITIONS:-4}" in content
        assert "ENABLE_SAGA_WORKER=${STOCK_ENABLE_SAGA_WORKER:-true}" in content
        assert "ENABLE_SAGA_WORKER=${PAYMENT_ENABLE_SAGA_WORKER:-true}" in content

    def test_order_wires_saga_command_bus(self):
        order_app_path = Path(__file__).resolve().parents[2] / "order" / "app.py"
        content = order_app_path.read_text(encoding="utf-8")
        assert "SagaCommandBus" in content
        assert "if TX_MODE == TxMode.SAGA.value:" in content
        assert "await saga_bus.start()" in content
        assert "recover_stale_pending" in content
        assert "ENABLE_ORDER_DISPATCHER" in content
        assert "/saga/metrics" in content

    def test_mq_contract_document_exists(self):
        contract_path = Path(__file__).resolve().parents[2] / "order" / "SAGA_MQ_CONTRACT.md"
        content = contract_path.read_text(encoding="utf-8")
        assert "saga:cmd:stock:p{partition}" in content
        assert "saga:cmd:payment:p{partition}" in content
        assert "saga:res:stock:p{partition}" in content
        assert "saga:res:payment:p{partition}" in content
        assert "saga:mq:lease:p{partition}" in content
        assert "correlation_id" in content
        assert "msg_id" in content
        assert "ok" in content
        assert "retryable" in content

    def test_participant_worker_skeletons_are_wired(self):
        payment_app = (Path(__file__).resolve().parents[2] / "payment" / "app.py").read_text(encoding="utf-8")
        stock_app = (Path(__file__).resolve().parents[2] / "stock" / "app.py").read_text(encoding="utf-8")
        assert "PaymentSagaMqWorkerService" in payment_app
        assert "StockSagaMqWorkerService" in stock_app
