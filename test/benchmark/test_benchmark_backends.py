import tempfile
import unittest
from pathlib import Path
from unittest.mock import ANY, patch

from benchmark.backends.docker_compose import DockerComposeBackend
from benchmark.backends.minikube import MinikubeBackend
from benchmark.config import BenchmarkPaths, read_deployment_sizing
from benchmark.scenarios.throughput import SCENARIO as THROUGHPUT_SCENARIO


def make_paths(tmp_path: Path) -> BenchmarkPaths:
    return BenchmarkPaths(
        script_dir=tmp_path,
        project_dir=tmp_path,
        benchmark_dir=tmp_path / "benchmark-workspace",
        stress_test_dir=tmp_path / "benchmark-workspace" / "stress-test",
        consistency_test_dir=tmp_path / "benchmark-workspace" / "consistency-test",
        results_dir=tmp_path / "benchmark-results",
        compose_file=tmp_path / "docker-compose.yml",
        k8s_dir=tmp_path / "k8s",
    )


class BenchmarkBackendReadinessTests(unittest.TestCase):
    def test_docker_compose_wait_ready_requires_databases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backend = DockerComposeBackend(make_paths(Path(temp_dir)), 30, read_deployment_sizing({}))
            with (
                patch.object(backend, "_wait_for_databases", return_value=False) as wait_databases,
                patch.object(backend, "resolve_gateway_url") as resolve_gateway_url,
                patch.object(backend, "_wait_for_gateway_health", return_value=True) as wait_gateway_health,
            ):
                ready = backend.wait_ready(THROUGHPUT_SCENARIO)

        self.assertFalse(ready)
        wait_databases.assert_called_once()
        resolve_gateway_url.assert_not_called()
        wait_gateway_health.assert_not_called()

    def test_docker_compose_wait_ready_checks_gateway_after_databases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backend = DockerComposeBackend(make_paths(Path(temp_dir)), 30, read_deployment_sizing({}))
            with (
                patch.object(backend, "_wait_for_databases", return_value=True) as wait_databases,
                patch.object(backend, "resolve_gateway_url", return_value="http://localhost:8000") as resolve_gateway_url,
                patch.object(backend, "_wait_for_gateway_health", return_value=True) as wait_gateway_health,
            ):
                ready = backend.wait_ready(THROUGHPUT_SCENARIO)

        self.assertTrue(ready)
        wait_databases.assert_called_once()
        resolve_gateway_url.assert_called_once_with()
        wait_gateway_health.assert_called_once_with("http://localhost:8000", ANY)

    def test_minikube_wait_ready_requires_databases_after_deployments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backend = MinikubeBackend(make_paths(Path(temp_dir)), 30, read_deployment_sizing({}))
            with (
                patch.object(backend, "_wait_for_deployments", return_value=True) as wait_deployments,
                patch.object(backend, "_wait_for_databases", return_value=False) as wait_databases,
                patch.object(backend, "_resolve_gateway_url") as resolve_gateway_url,
                patch.object(backend, "_wait_for_gateway_health", return_value=True) as wait_gateway_health,
            ):
                ready = backend.wait_ready(THROUGHPUT_SCENARIO)

        self.assertFalse(ready)
        wait_deployments.assert_called_once()
        wait_databases.assert_called_once()
        resolve_gateway_url.assert_not_called()
        wait_gateway_health.assert_not_called()

    def test_minikube_wait_ready_checks_gateway_after_databases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backend = MinikubeBackend(make_paths(Path(temp_dir)), 30, read_deployment_sizing({}))
            with (
                patch.object(backend, "_wait_for_deployments", return_value=True) as wait_deployments,
                patch.object(backend, "_wait_for_databases", return_value=True) as wait_databases,
                patch.object(backend, "_resolve_gateway_url", return_value="http://10.0.0.1:8000") as resolve_gateway_url,
                patch.object(backend, "_wait_for_gateway_health", return_value=True) as wait_gateway_health,
            ):
                ready = backend.wait_ready(THROUGHPUT_SCENARIO)

        self.assertTrue(ready)
        wait_deployments.assert_called_once()
        wait_databases.assert_called_once()
        resolve_gateway_url.assert_called_once_with(ANY)
        wait_gateway_health.assert_called_once_with("http://10.0.0.1:8000", ANY)


if __name__ == "__main__":
    unittest.main()
