from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Mapping, Protocol, Sequence

DEFAULT_KILL_SCHEDULE: tuple[tuple[int, str], ...] = (
    (30, "payment-deployment"),
    (50, "stock-deployment"),
    (70, "order-deployment"),
    (90, "payment-db"),
    (110, "stock-db"),
    (130, "order-db"),
)
DEFAULT_STARTUP_TIMEOUT_SECONDS = 300
DEFAULT_MODES = ("2pc", "saga")
DEFAULT_BACKENDS = ("docker-compose",)
DEFAULT_SCENARIOS = ("throughput",)
DEFAULT_USERS = (500, 1000, 2000)
DEFAULT_RUNS = 3
DEFAULT_DURATION = "3m"
DEFAULT_SPAWN_RATE = 10
DEFAULT_LOCUST_WORKERS = 2
DEFAULT_ORDER_REPLICAS = 1
DEFAULT_PAYMENT_REPLICAS = 1
DEFAULT_STOCK_REPLICAS = 1
DEFAULT_ORDER_DB_REPLICA_COUNT = 1
DEFAULT_PAYMENT_DB_REPLICA_COUNT = 1
DEFAULT_STOCK_DB_REPLICA_COUNT = 1
DEFAULT_SAGA_BROKER_REPLICA_COUNT = 1
DEFAULT_SENTINEL_REPLICAS = 1


@dataclass(frozen=True)
class BenchmarkPaths:
    script_dir: Path
    project_dir: Path
    benchmark_dir: Path
    stress_test_dir: Path
    consistency_test_dir: Path
    results_dir: Path
    default_compose_file: Path
    compose_file: Path
    k8s_dir: Path


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    kill_schedule: tuple[tuple[int, str], ...] | None
    extra_stabilization_seconds: int
    locust_defaults: Mapping[str, object]


@dataclass(frozen=True)
class DeploymentSizing:
    order_replicas: int
    payment_replicas: int
    stock_replicas: int
    order_db_replica_count: int
    payment_db_replica_count: int
    stock_db_replica_count: int
    saga_broker_replica_count: int
    sentinel_replicas: int

    def k8s_targets(self) -> dict[str, int]:
        return {
            "gateway": 1,
            "order-db": 1,
            "stock-db": 1,
            "payment-db": 1,
            "saga-broker": 1,
            "order-db-replica": self.order_db_replica_count,
            "stock-db-replica": self.stock_db_replica_count,
            "payment-db-replica": self.payment_db_replica_count,
            "saga-broker-replica": self.saga_broker_replica_count,
            "sentinel": self.sentinel_replicas,
            "order-deployment": self.order_replicas,
            "stock-deployment": self.stock_replicas,
            "payment-deployment": self.payment_replicas,
        }

    def compose_scales(self) -> dict[str, int]:
        return {
            "order-service": self.order_replicas,
            "stock-service": self.stock_replicas,
            "payment-service": self.payment_replicas,
        }


@dataclass(frozen=True)
class BenchmarkConfig:
    modes: tuple[str, ...]
    backends: tuple[str, ...]
    scenarios: tuple[str, ...]
    users: tuple[int, ...]
    runs: int
    duration: str
    spawn_rate: int
    locust_workers: int
    startup_timeout: int
    clean: bool
    sizing: DeploymentSizing
    paths: BenchmarkPaths = field(repr=False)


@dataclass(frozen=True)
class RunSpec:
    mode: str
    backend: str
    scenario: str
    users: int
    run_number: int
    timestamp: str


class Backend(Protocol):
    def down(self) -> None:
        ...

    def up(self, mode: str, scenario: ScenarioSpec) -> None:
        ...

    def wait_ready(self, scenario: ScenarioSpec) -> bool:
        ...

    def resolve_gateway_url(self) -> str:
        ...

    def kill_target(self, target: str) -> None:
        ...

    def collect_diagnostics(self, output_dir: Path) -> None:
        ...


def resolve_benchmark_dir(script_dir: Path) -> Path:
    candidates = (
        script_dir.parent / "bm",
        script_dir.parent / "wdm-project-benchmark",
    )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return candidates[0]


def resolve_paths(
    script_dir: str | Path | None = None,
    compose_file: str | Path | None = None,
) -> BenchmarkPaths:
    resolved_script_dir = Path(script_dir or Path(__file__).resolve().parent.parent).resolve()
    benchmark_dir = resolve_benchmark_dir(resolved_script_dir)
    default_compose_file = resolved_script_dir / "docker-compose.yml"
    selected_compose_file = Path(compose_file).resolve() if compose_file else default_compose_file
    return BenchmarkPaths(
        script_dir=resolved_script_dir,
        project_dir=resolved_script_dir,
        benchmark_dir=benchmark_dir,
        stress_test_dir=benchmark_dir / "stress-test",
        consistency_test_dir=benchmark_dir / "consistency-test",
        results_dir=resolved_script_dir / "benchmark-results",
        default_compose_file=default_compose_file,
        compose_file=selected_compose_file,
        k8s_dir=resolved_script_dir / "k8s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Automated benchmark runner for 2PC/SAGA deployment matrices"
    )
    parser.add_argument(
        "--backends",
        nargs="+",
        default=list(DEFAULT_BACKENDS),
        choices=["docker-compose", "minikube"],
        help="One or more deployment backends to benchmark",
    )
    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=list(DEFAULT_SCENARIOS),
        choices=["throughput", "ha"],
        help="One or more benchmark scenarios to run",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        default=list(DEFAULT_MODES),
        choices=["2pc", "saga"],
        help="Checkout modes to benchmark",
    )
    parser.add_argument(
        "--users",
        nargs="+",
        type=int,
        default=list(DEFAULT_USERS),
        help="One or more concurrent Locust user counts",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        help="Number of repetitions per matrix item",
    )
    parser.add_argument(
        "--duration",
        default=DEFAULT_DURATION,
        help="Locust run duration, for example 30s, 1m, 3m",
    )
    parser.add_argument(
        "--spawn-rate",
        type=int,
        default=DEFAULT_SPAWN_RATE,
        help="Locust user spawn rate",
    )
    parser.add_argument(
        "--locust-workers",
        type=int,
        default=DEFAULT_LOCUST_WORKERS,
        help="Number of Locust worker processes on non-Windows hosts",
    )
    parser.add_argument(
        "--startup-timeout",
        type=int,
        default=DEFAULT_STARTUP_TIMEOUT_SECONDS,
        help="Maximum seconds to wait for service readiness",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete previous benchmark results before starting",
    )
    parser.add_argument(
        "--compose-file",
        default=os.environ.get("BENCHMARK_COMPOSE_FILE"),
        help="Optional Docker Compose file to benchmark instead of docker-compose.yml",
    )
    return parser


def read_deployment_sizing(environment: Mapping[str, str] | None = None) -> DeploymentSizing:
    env = environment or {}
    return DeploymentSizing(
        order_replicas=int(env.get("ORDER_REPLICAS", DEFAULT_ORDER_REPLICAS)),
        payment_replicas=int(env.get("PAYMENT_REPLICAS", DEFAULT_PAYMENT_REPLICAS)),
        stock_replicas=int(env.get("STOCK_REPLICAS", DEFAULT_STOCK_REPLICAS)),
        order_db_replica_count=int(env.get("ORDER_DB_REPLICA_COUNT", DEFAULT_ORDER_DB_REPLICA_COUNT)),
        payment_db_replica_count=int(env.get("PAYMENT_DB_REPLICA_COUNT", DEFAULT_PAYMENT_DB_REPLICA_COUNT)),
        stock_db_replica_count=int(env.get("STOCK_DB_REPLICA_COUNT", DEFAULT_STOCK_DB_REPLICA_COUNT)),
        saga_broker_replica_count=int(env.get("SAGA_BROKER_REPLICA_COUNT", DEFAULT_SAGA_BROKER_REPLICA_COUNT)),
        sentinel_replicas=int(env.get("SENTINEL_REPLICAS", DEFAULT_SENTINEL_REPLICAS)),
    )


def parse_cli(argv: Sequence[str] | None = None, script_dir: str | Path | None = None) -> BenchmarkConfig:
    parser = build_parser()
    args = parser.parse_args(argv)
    return BenchmarkConfig(
        modes=tuple(args.modes),
        backends=tuple(args.backends),
        scenarios=tuple(args.scenarios),
        users=tuple(args.users),
        runs=args.runs,
        duration=args.duration,
        spawn_rate=args.spawn_rate,
        locust_workers=args.locust_workers,
        startup_timeout=args.startup_timeout,
        clean=args.clean,
        sizing=read_deployment_sizing(os.environ),
        paths=resolve_paths(script_dir, compose_file=args.compose_file),
    )


def default_timestamp_factory() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]


def expand_run_specs(
    config: BenchmarkConfig,
    timestamp_factory: Callable[[], str] | None = None,
) -> list[RunSpec]:
    factory = timestamp_factory or default_timestamp_factory
    run_specs: list[RunSpec] = []
    for backend in config.backends:
        for scenario in config.scenarios:
            for mode in config.modes:
                for users in config.users:
                    for run_number in range(1, config.runs + 1):
                        run_specs.append(
                            RunSpec(
                                mode=mode,
                                backend=backend,
                                scenario=scenario,
                                users=users,
                                run_number=run_number,
                                timestamp=factory(),
                            )
                        )
    return run_specs
