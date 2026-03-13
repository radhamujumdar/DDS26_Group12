import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmark.config import expand_run_specs, parse_cli, read_deployment_sizing


class BenchmarkConfigTests(unittest.TestCase):
    def test_expand_run_specs_respects_backend_scenario_mode_user_run_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = parse_cli(
                [
                    "--backends",
                    "docker-compose",
                    "minikube",
                    "--scenarios",
                    "throughput",
                    "ha",
                    "--modes",
                    "2pc",
                    "saga",
                    "--users",
                    "500",
                    "1000",
                    "--runs",
                    "2",
                ],
                script_dir=Path(temp_dir),
            )
            timestamps = (f"ts-{index}" for index in range(1, 100))
            run_specs = expand_run_specs(config, timestamp_factory=lambda: next(timestamps))

        self.assertEqual(len(run_specs), 32)
        self.assertEqual(run_specs[0].backend, "docker-compose")
        self.assertEqual(run_specs[0].scenario, "throughput")
        self.assertEqual(run_specs[0].mode, "2pc")
        self.assertEqual(run_specs[0].users, 500)
        self.assertEqual(run_specs[0].run_number, 1)
        self.assertEqual(run_specs[1].run_number, 2)
        self.assertEqual(run_specs[2].users, 1000)
        self.assertEqual(run_specs[4].mode, "saga")
        self.assertEqual(run_specs[8].scenario, "ha")
        self.assertEqual(run_specs[16].backend, "minikube")
        self.assertEqual(run_specs[-1].backend, "minikube")
        self.assertEqual(run_specs[-1].scenario, "ha")
        self.assertEqual(run_specs[-1].mode, "saga")
        self.assertEqual(run_specs[-1].users, 1000)
        self.assertEqual(run_specs[-1].run_number, 2)

    def test_read_deployment_sizing_uses_single_topology_defaults_and_overrides(self) -> None:
        defaults = read_deployment_sizing({})
        self.assertEqual(defaults.order_replicas, 1)
        self.assertEqual(defaults.payment_replicas, 1)
        self.assertEqual(defaults.stock_replicas, 1)
        self.assertEqual(defaults.sentinel_replicas, 3)

        overrides = {
            "ORDER_REPLICAS": "2",
            "PAYMENT_REPLICAS": "3",
            "STOCK_REPLICAS": "4",
            "ORDER_DB_REPLICA_COUNT": "2",
            "PAYMENT_DB_REPLICA_COUNT": "2",
            "STOCK_DB_REPLICA_COUNT": "2",
            "SAGA_BROKER_REPLICA_COUNT": "2",
            "SENTINEL_REPLICAS": "5",
        }
        with patch.dict("os.environ", overrides, clear=False):
            sizing = read_deployment_sizing(overrides)

        self.assertEqual(sizing.order_replicas, 2)
        self.assertEqual(sizing.payment_replicas, 3)
        self.assertEqual(sizing.stock_replicas, 4)
        self.assertEqual(sizing.order_db_replica_count, 2)
        self.assertEqual(sizing.payment_db_replica_count, 2)
        self.assertEqual(sizing.stock_db_replica_count, 2)
        self.assertEqual(sizing.saga_broker_replica_count, 2)
        self.assertEqual(sizing.sentinel_replicas, 5)


if __name__ == "__main__":
    unittest.main()
