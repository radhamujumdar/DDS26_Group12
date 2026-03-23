from __future__ import annotations

import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from benchmark.config import BenchmarkPaths, DeploymentSizing, ScenarioSpec

COMPOSE_KILL_TARGETS = {
    "payment-deployment": "payment-service",
    "stock-deployment": "stock-service",
    "order-deployment": "order-service",
    "payment-db": "payment-db",
    "stock-db": "stock-db",
    "order-db": "order-db",
}
COMPOSE_REDIS_SERVICES = (
    "main-cluster-1",
    "main-cluster-2",
    "main-cluster-3",
    "saga-cluster-1",
    "saga-cluster-2",
    "saga-cluster-3",
)
COMPOSE_SENTINEL_SERVICES: tuple[()] = ()
COMPOSE_SENTINEL_MASTERS: tuple[()] = ()
COMPOSE_REDIS_PASSWORD = "redis"


class DockerComposeBackend:
    def __init__(self, paths: BenchmarkPaths, startup_timeout: int, sizing: DeploymentSizing):
        self.paths = paths
        self.startup_timeout = startup_timeout
        self.sizing = sizing
        self.compose_file = paths.compose_file

    def down(self) -> None:
        self._run_compose(
            self.compose_file,
            ["down", "-v", "--remove-orphans"],
            capture_output=True,
        )

    def up(self, mode: str, scenario: ScenarioSpec) -> None:
        del scenario

        result = self._run_compose(
            self.compose_file,
            [
                "up",
                "-d",
                "--build",
                *self._compose_scale_args(),
            ],
            env=self._compose_env(mode),
            capture_output=True,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"docker compose up failed: {message}")

    def wait_ready(self, scenario: ScenarioSpec) -> bool:
        del scenario
        deadline = time.time() + self.startup_timeout
        if not self._wait_for_databases(self._remaining_timeout(deadline)):
            return False
        remaining_timeout = self._remaining_timeout(deadline)
        if remaining_timeout <= 0:
            return False
        return self._wait_for_gateway_health(self.resolve_gateway_url(), remaining_timeout)

    def resolve_gateway_url(self) -> str:
        return "http://localhost:8000"

    def kill_target(self, target: str) -> None:
        service = COMPOSE_KILL_TARGETS.get(target)
        if not service:
            raise RuntimeError(f"No Docker Compose kill target mapping for {target}")

        result = self._run_compose(
            self.compose_file,
            ["kill", service],
            capture_output=True,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"docker compose kill failed for {service}: {message}")

    def collect_diagnostics(self, output_dir: Path) -> None:
        sections = [
            (
                "docker compose ps",
                self._capture_compose(self.compose_file, ["ps", "--all"]),
            ),
            (
                "docker compose logs",
                self._capture_compose(self.compose_file, ["logs", "--no-color", "--tail", "200"]),
            ),
        ]

        diagnostics_path = output_dir / "startup-diagnostics.txt"
        with diagnostics_path.open("w", encoding="utf-8") as handle:
            for title, body in sections:
                handle.write(f"===== {title} =====\n")
                handle.write(body or "<no output>")
                handle.write("\n\n")

    def _compose_env(self, mode: str) -> dict[str, str]:
        env = os.environ.copy()
        env["TX_MODE"] = mode
        return env

    def _compose_scale_args(self) -> list[str]:
        args: list[str] = []
        for service, replicas in self.sizing.compose_scales().items():
            args.extend(["--scale", f"{service}={replicas}"])
        return args

    def _run_compose(
        self,
        compose_file: Path,
        args: list[str],
        *,
        env: dict[str, str] | None = None,
        capture_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["docker", "compose", "-f", str(compose_file), *args],
            cwd=self.paths.project_dir,
            env=env,
            text=True,
            capture_output=capture_output,
            check=False,
        )

    def _capture_compose(self, compose_file: Path, args: list[str]) -> str:
        result = self._run_compose(compose_file, args, capture_output=True)
        output = result.stdout or ""
        if result.stderr:
            output += ("\n" if output else "") + result.stderr
        return output.strip()

    def _wait_for_databases(self, timeout: float, interval: int = 3) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            pending_services = self._pending_redis_services()
            pending_masters = self._pending_sentinel_masters()
            if not pending_services and not pending_masters:
                return True
            time.sleep(interval)
        return False

    def _pending_redis_services(self) -> list[str]:
        return [service for service in COMPOSE_REDIS_SERVICES if not self._redis_service_ready(service)]

    def _redis_service_ready(self, service: str) -> bool:
        result = self._run_compose(
            self.compose_file,
            [
                "exec",
                "-T",
                service,
                "redis-cli",
                "--no-auth-warning",
                "-a",
                COMPOSE_REDIS_PASSWORD,
                "ping",
            ],
            capture_output=True,
        )
        return result.returncode == 0 and "PONG" in (result.stdout or "")

    def _pending_sentinel_masters(self) -> list[str]:
        pending: list[str] = []
        for sentinel_service in COMPOSE_SENTINEL_SERVICES:
            for master_name in COMPOSE_SENTINEL_MASTERS:
                if not self._sentinel_master_ready(sentinel_service, master_name):
                    pending.append(f"{sentinel_service}:{master_name}")
        return pending

    def _sentinel_master_ready(self, sentinel_service: str, master_name: str) -> bool:
        result = self._run_compose(
            self.compose_file,
            [
                "exec",
                "-T",
                sentinel_service,
                "redis-cli",
                "--raw",
                "-p",
                "26379",
                "SENTINEL",
                "get-master-addr-by-name",
                master_name,
            ],
            capture_output=True,
        )
        if result.returncode != 0:
            return False
        resolved = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
        return len(resolved) >= 2

    def _remaining_timeout(self, deadline: float) -> float:
        return max(0.0, deadline - time.time())

    def _wait_for_gateway_health(self, gateway_url: str, timeout: int, interval: int = 3) -> bool:
        endpoints = ("/orders/health", "/payment/health", "/stock/health")
        deadline = time.time() + timeout
        while time.time() < deadline:
            failures: list[str] = []
            for endpoint in endpoints:
                try:
                    with urllib.request.urlopen(f"{gateway_url}{endpoint}", timeout=5) as response:
                        if not 200 <= response.status < 300:
                            failures.append(f"{endpoint} -> HTTP {response.status}")
                except urllib.error.HTTPError as exc:
                    failures.append(f"{endpoint} -> HTTP {exc.code}")
                except Exception:
                    failures.append(f"{endpoint} -> unavailable")
            if not failures:
                return True
            time.sleep(interval)
        return False
