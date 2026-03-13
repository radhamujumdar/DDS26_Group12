import json
import tempfile
import unittest
from pathlib import Path

from benchmark.config import BenchmarkConfig, BenchmarkPaths, RunSpec, read_deployment_sizing
from benchmark.runner import build_locust_command, build_metadata_payload, build_result_path, write_metadata
from benchmark.scenarios.ha import SCENARIO as HA_SCENARIO
from benchmark.scenarios.throughput import SCENARIO as THROUGHPUT_SCENARIO


def make_paths(tmp_path: Path) -> BenchmarkPaths:
    benchmark_dir = tmp_path / "benchmark-workspace"
    stress_test_dir = benchmark_dir / "stress-test"
    consistency_test_dir = benchmark_dir / "consistency-test"
    stress_test_dir.mkdir(parents=True)
    consistency_test_dir.mkdir(parents=True)
    return BenchmarkPaths(
        script_dir=tmp_path,
        project_dir=tmp_path,
        benchmark_dir=benchmark_dir,
        stress_test_dir=stress_test_dir,
        consistency_test_dir=consistency_test_dir,
        results_dir=tmp_path / "benchmark-results",
        compose_file=tmp_path / "docker-compose.yml",
        k8s_dir=tmp_path / "k8s",
    )


def make_config(tmp_path: Path) -> BenchmarkConfig:
    return BenchmarkConfig(
        modes=("2pc", "saga"),
        backends=("docker-compose",),
        scenarios=("throughput",),
        users=(500, 1000, 2000),
        runs=3,
        duration="30s",
        spawn_rate=25,
        locust_workers=2,
        startup_timeout=300,
        clean=False,
        sizing=read_deployment_sizing({}),
        paths=make_paths(tmp_path),
    )


class BenchmarkRunnerTests(unittest.TestCase):
    def test_scenario_policies_differ_only_in_failure_behavior(self) -> None:
        self.assertIsNone(THROUGHPUT_SCENARIO.kill_schedule)
        self.assertEqual(THROUGHPUT_SCENARIO.extra_stabilization_seconds, 0)
        self.assertIsNotNone(HA_SCENARIO.kill_schedule)
        self.assertEqual(HA_SCENARIO.extra_stabilization_seconds, 30)
        self.assertEqual(THROUGHPUT_SCENARIO.locust_defaults, HA_SCENARIO.locust_defaults)

    def test_locust_command_uses_explicit_worker_count(self) -> None:
        command = build_locust_command(
            gateway_url="http://localhost:8000",
            duration="30s",
            users=500,
            spawn_rate=25,
            csv_prefix="/tmp/out/bm",
            stress_test_dir=Path("/tmp/stress-test"),
            locust_workers=2,
            platform="linux",
            python_executable="python3",
        )

        self.assertIn("--processes=2", command)
        self.assertNotIn("--processes=-1", command)

    def test_result_path_and_metadata_include_backend_scenario_and_workers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            tmp_path = Path(temp_dir)
            config = make_config(tmp_path)
            run_spec = RunSpec(
                mode="saga",
                backend="docker-compose",
                scenario="ha",
                users=1000,
                run_number=2,
                timestamp="20260313-120000-000",
            )

            result_path = build_result_path(config.paths.results_dir, run_spec)
            self.assertEqual(
                result_path,
                config.paths.results_dir
                / "docker-compose"
                / "ha"
                / "saga"
                / "users_1000"
                / "run_2_20260313-120000-000",
            )

            result_path.mkdir(parents=True)
            payload = build_metadata_payload(config, run_spec, HA_SCENARIO)
            write_metadata(result_path, payload)

            metadata = json.loads((result_path / "metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata["backend"], "docker-compose")
            self.assertEqual(metadata["scenario"], "ha")
            self.assertEqual(metadata["mode"], "saga")
            self.assertEqual(metadata["users"], 1000)
            self.assertEqual(metadata["locust_workers"], 2)
            self.assertEqual(metadata["order_replicas"], 1)
            self.assertEqual(metadata["payment_replicas"], 1)
            self.assertEqual(metadata["stock_replicas"], 1)
            self.assertEqual(metadata["sentinel_replicas"], 3)
            self.assertTrue(metadata["kill_schedule_enabled"])


if __name__ == "__main__":
    unittest.main()
