from __future__ import annotations

import argparse
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


@dataclass(frozen=True)
class BenchmarkPaths:
    script_dir: Path
    project_dir: Path
    benchmark_dir: Path
    stress_test_dir: Path
    consistency_test_dir: Path
    results_dir: Path
    compose_file: Path
    k8s_dir: Path


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    kill_schedule: tuple[tuple[int, str], ...] | None
    extra_stabilization_seconds: int
    locust_defaults: Mapping[str, object]
    k8s_targets: Mapping[str, int]
    compose_profile: str


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


def resolve_paths(script_dir: str | Path | None = None) -> BenchmarkPaths:
    resolved_script_dir = Path(script_dir or Path(__file__).resolve().parent.parent).resolve()
    benchmark_dir = resolve_benchmark_dir(resolved_script_dir)
    return BenchmarkPaths(
        script_dir=resolved_script_dir,
        project_dir=resolved_script_dir,
        benchmark_dir=benchmark_dir,
        stress_test_dir=benchmark_dir / "stress-test",
        consistency_test_dir=benchmark_dir / "consistency-test",
        results_dir=resolved_script_dir / "benchmark-results",
        compose_file=resolved_script_dir / "docker-compose.yml",
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
    return parser


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
        paths=resolve_paths(script_dir),
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
