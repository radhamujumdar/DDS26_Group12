from __future__ import annotations

import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from benchmark.config import BenchmarkPaths, ScenarioSpec

SENTINEL_ENV_VARS = (
    "REDIS_SENTINEL_HOSTS",
    "REDIS_MASTER_NAME",
    "SAGA_MQ_SENTINEL_HOSTS",
    "SAGA_MQ_MASTER_NAME",
)
THROUGHPUT_SERVICES = (
    "gateway",
    "order-service",
    "stock-service",
    "payment-service",
    "order-db",
    "stock-db",
    "payment-db",
    "saga-broker",
)
COMPOSE_KILL_TARGETS = {
    "payment-deployment": "payment-service",
    "stock-deployment": "stock-service",
    "order-deployment": "order-service",
    "payment-db": "payment-db",
    "stock-db": "stock-db",
    "order-db": "order-db",
}


class DockerComposeBackend:
    def __init__(self, paths: BenchmarkPaths, startup_timeout: int):
        self.paths = paths
        self.startup_timeout = startup_timeout
        self.compose_file = paths.compose_file
        self.active_compose_file: Path | None = None
        self.temp_compose_file: Path | None = None

    def down(self) -> None:
        compose_file = self.active_compose_file or self.compose_file
        self._run_compose(
            compose_file,
            ["down", "-v", "--remove-orphans"],
            capture_output=True,
        )
        self._cleanup_temp_compose_file()

    def up(self, mode: str, scenario: ScenarioSpec) -> None:
        if scenario.compose_profile == "throughput":
            self.active_compose_file = self._create_throughput_compose_file()
        else:
            self.active_compose_file = self.compose_file

        result = self._run_compose(
            self.active_compose_file,
            ["up", "-d", "--build"],
            env=self._compose_env(mode),
            capture_output=True,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"docker compose up failed: {message}")

    def wait_ready(self, scenario: ScenarioSpec) -> bool:
        del scenario
        return self._wait_for_gateway_health(self.resolve_gateway_url(), self.startup_timeout)

    def resolve_gateway_url(self) -> str:
        return "http://localhost:8000"

    def kill_target(self, target: str) -> None:
        service = COMPOSE_KILL_TARGETS.get(target)
        if not service:
            raise RuntimeError(f"No Docker Compose kill target mapping for {target}")

        result = self._run_compose(
            self.active_compose_file or self.compose_file,
            ["kill", service],
            capture_output=True,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"docker compose kill failed for {service}: {message}")

    def collect_diagnostics(self, output_dir: Path) -> None:
        compose_file = self.active_compose_file or self.compose_file
        sections = [
            (
                "docker compose ps",
                self._capture_compose(compose_file, ["ps", "--all"]),
            ),
            (
                "docker compose logs",
                self._capture_compose(compose_file, ["logs", "--no-color", "--tail", "200"]),
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

    def _create_throughput_compose_file(self) -> Path:
        try:
            import yaml
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "docker-compose throughput runs require PyYAML. Install it with "
                "`pip install PyYAML` in the benchmark environment."
            ) from exc

        with self.compose_file.open("r", encoding="utf-8") as handle:
            document = yaml.safe_load(handle)

        services = document.get("services", {})
        throughput_services = {name: services[name] for name in THROUGHPUT_SERVICES}
        for service_name in ("order-service", "stock-service", "payment-service"):
            definition = throughput_services[service_name]
            definition["environment"] = self._strip_sentinel_env(definition.get("environment"))
            definition["depends_on"] = [
                dependency
                for dependency in self._normalize_depends_on(definition.get("depends_on"))
                if dependency in THROUGHPUT_SERVICES
            ]

        document["services"] = throughput_services

        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".benchmark-compose.yml",
            prefix="benchmark-",
            dir=self.paths.project_dir,
            delete=False,
        )
        with temp_file:
            yaml.safe_dump(document, temp_file, sort_keys=False)

        self.temp_compose_file = Path(temp_file.name)
        return self.temp_compose_file

    def _strip_sentinel_env(self, environment: object) -> dict[str, str | None]:
        normalized: dict[str, str | None] = {}
        if isinstance(environment, dict):
            normalized.update(environment)
        elif isinstance(environment, list):
            for item in environment:
                if not isinstance(item, str):
                    continue
                key, separator, value = item.partition("=")
                normalized[key] = value if separator else None

        for key in SENTINEL_ENV_VARS:
            normalized.pop(key, None)
        return normalized

    def _normalize_depends_on(self, depends_on: object) -> list[str]:
        if isinstance(depends_on, dict):
            return list(depends_on.keys())
        if isinstance(depends_on, list):
            return [item for item in depends_on if isinstance(item, str)]
        return []

    def _cleanup_temp_compose_file(self) -> None:
        if self.temp_compose_file and self.temp_compose_file.exists():
            self.temp_compose_file.unlink()
        self.temp_compose_file = None
        self.active_compose_file = None
