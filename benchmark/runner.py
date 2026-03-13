from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Sequence

from benchmark.backends import create_backend
from benchmark.config import BenchmarkConfig, RunSpec, ScenarioSpec, expand_run_specs, parse_cli
from benchmark.scenarios import get_scenario


def log(message: str) -> None:
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def build_result_path(base_dir: Path, run_spec: RunSpec) -> Path:
    return (
        base_dir
        / run_spec.backend
        / run_spec.scenario
        / run_spec.mode
        / f"users_{run_spec.users}"
        / f"run_{run_spec.run_number}_{run_spec.timestamp}"
    )


def build_metadata_payload(
    config: BenchmarkConfig,
    run_spec: RunSpec,
    scenario: ScenarioSpec,
) -> dict[str, object]:
    return {
        "mode": run_spec.mode,
        "backend": run_spec.backend,
        "scenario": run_spec.scenario,
        "users": run_spec.users,
        "run_number": run_spec.run_number,
        "timestamp": run_spec.timestamp,
        "spawn_rate": config.spawn_rate,
        "duration": config.duration,
        "locust_workers": config.locust_workers,
        "startup_timeout": config.startup_timeout,
        "order_replicas": config.sizing.order_replicas,
        "payment_replicas": config.sizing.payment_replicas,
        "stock_replicas": config.sizing.stock_replicas,
        "order_db_replica_count": config.sizing.order_db_replica_count,
        "payment_db_replica_count": config.sizing.payment_db_replica_count,
        "stock_db_replica_count": config.sizing.stock_db_replica_count,
        "saga_broker_replica_count": config.sizing.saga_broker_replica_count,
        "sentinel_replicas": config.sizing.sentinel_replicas,
        "kill_schedule_enabled": scenario.kill_schedule is not None,
        "kill_schedule": [
            {"delay_seconds": delay_seconds, "target": target}
            for delay_seconds, target in (scenario.kill_schedule or ())
        ],
    }


def write_metadata(output_dir: Path, payload: dict[str, object]) -> None:
    with (output_dir / "metadata.json").open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def configure_benchmark_urls(benchmark_dir: Path, gateway_url: str) -> None:
    urls_payload = {
        "ORDER_URL": gateway_url,
        "PAYMENT_URL": gateway_url,
        "STOCK_URL": gateway_url,
    }
    with (benchmark_dir / "urls.json").open("w", encoding="utf-8") as handle:
        json.dump(urls_payload, handle, indent=2)
        handle.write("\n")


def build_locust_command(
    *,
    gateway_url: str,
    duration: str,
    users: int,
    spawn_rate: int,
    csv_prefix: str,
    stress_test_dir: Path,
    locust_workers: int,
    platform: str | None = None,
    python_executable: str | None = None,
) -> list[str]:
    command = [
        python_executable or sys.executable,
        "-m",
        "locust",
        "--headless",
        f"--host={gateway_url}",
        f"--users={users}",
        f"--spawn-rate={spawn_rate}",
        f"--run-time={duration}",
        f"--csv={csv_prefix}",
        "-f",
        str(stress_test_dir / "locustfile.py"),
    ]
    if (platform or sys.platform) != "win32":
        command.append(f"--processes={locust_workers}")
    return command


def run_init_orders(stress_test_dir: Path) -> tuple[bool, str]:
    result = subprocess.run(
        [sys.executable, "init_orders.py"],
        cwd=stress_test_dir,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    output = (result.stdout or "") + (("\n" if result.stderr and result.stdout else "") + (result.stderr or ""))
    return result.returncode == 0, output


def run_consistency_test(consistency_test_dir: Path, output_file: Path) -> bool:
    script_path = consistency_test_dir / "run_consistency_test.py"
    if not script_path.is_file():
        output_file.write_text("SKIPPED: Consistency test script not found\n", encoding="utf-8")
        return False

    try:
        result = subprocess.run(
            [sys.executable, script_path.name],
            cwd=consistency_test_dir,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        output_file.write_text(
            "TIMEOUT: Consistency test did not finish within 5 minutes\n",
            encoding="utf-8",
        )
        return False

    output_file.write_text((result.stdout or "") + ("\n" if result.stderr else "") + (result.stderr or ""), encoding="utf-8")
    return result.returncode == 0


def collect_locust_csvs(csv_prefix: str, output_dir: Path) -> None:
    prefix_path = Path(csv_prefix)
    prefix_dir = prefix_path.parent if prefix_path.parent != Path("") else Path(".")
    prefix_name = prefix_path.name

    for file_path in prefix_dir.glob(f"{prefix_name}*.csv"):
        clean_name = file_path.name.replace(prefix_name, "locust", 1)
        cleaned_lines = [line for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        (output_dir / clean_name).write_text("\n".join(cleaned_lines) + ("\n" if cleaned_lines else ""), encoding="utf-8")
        file_path.unlink()


def run_kill_schedule(backend: object, schedule: tuple[tuple[int, str], ...], stop_event: threading.Event) -> None:
    start = time.time()
    for delay_seconds, target in schedule:
        while time.time() - start < delay_seconds:
            if stop_event.is_set():
                return
            time.sleep(0.5)
        if stop_event.is_set():
            return
        log(f"  [T+{int(time.time() - start)}s] Killing {target}")
        backend.kill_target(target)


def validate_environment(config: BenchmarkConfig) -> None:
    paths = config.paths
    required_paths = {
        "benchmark directory": paths.benchmark_dir,
        "stress test directory": paths.stress_test_dir,
        "consistency test directory": paths.consistency_test_dir,
        "docker compose file": paths.compose_file,
        "k8s directory": paths.k8s_dir,
    }
    for label, path in required_paths.items():
        if not path.exists():
            raise FileNotFoundError(f"Missing {label}: {path}")


def run_single_benchmark(config: BenchmarkConfig, run_spec: RunSpec) -> bool:
    scenario = get_scenario(run_spec.scenario)
    backend = create_backend(run_spec.backend, config)
    output_dir = build_result_path(config.paths.results_dir, run_spec)
    output_dir.mkdir(parents=True, exist_ok=False)
    write_metadata(output_dir, build_metadata_payload(config, run_spec, scenario))

    log("=" * 72)
    log(
        " | ".join(
            [
                f"BACKEND: {run_spec.backend}",
                f"SCENARIO: {run_spec.scenario}",
                f"MODE: {run_spec.mode}",
                f"USERS: {run_spec.users}",
                f"RUN: {run_spec.run_number}",
            ]
        )
    )
    log(f"OUTPUT: {output_dir}")
    log("=" * 72)

    diagnostics_needed = False
    consistency_ok = False
    locust_exit_code = 0
    stop_event = threading.Event()
    kill_thread: threading.Thread | None = None

    try:
        backend.down()
        backend.up(run_spec.mode, scenario)
        if not backend.wait_ready(scenario):
            diagnostics_needed = True
            return False

        gateway_url = backend.resolve_gateway_url()
        configure_benchmark_urls(config.paths.benchmark_dir, gateway_url)

        if scenario.extra_stabilization_seconds:
            log(f"Waiting {scenario.extra_stabilization_seconds}s for scenario stabilization")
            time.sleep(scenario.extra_stabilization_seconds)

        log("Running init_orders.py")
        init_ok, init_output = run_init_orders(config.paths.stress_test_dir)
        (output_dir / "init_orders.txt").write_text(init_output, encoding="utf-8")
        if not init_ok:
            log("init_orders.py failed; see init_orders.txt")
            diagnostics_needed = True
            return False

        csv_prefix = str(output_dir / "bm")
        command = build_locust_command(
            gateway_url=gateway_url,
            duration=config.duration,
            users=run_spec.users,
            spawn_rate=config.spawn_rate,
            csv_prefix=csv_prefix,
            stress_test_dir=config.paths.stress_test_dir,
            locust_workers=config.locust_workers,
        )
        log(f"Starting Locust with {config.locust_workers} worker process(es)")
        locust_process = subprocess.Popen(
            command,
            cwd=config.paths.stress_test_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        if scenario.kill_schedule:
            kill_thread = threading.Thread(
                target=run_kill_schedule,
                args=(backend, scenario.kill_schedule, stop_event),
                daemon=True,
            )
            kill_thread.start()

        locust_output = locust_process.communicate()[0]
        locust_exit_code = locust_process.returncode
        (output_dir / "locust_output.txt").write_text(locust_output, encoding="utf-8")
        collect_locust_csvs(csv_prefix, output_dir)

        stop_event.set()
        if kill_thread:
            kill_thread.join(timeout=5)

        log("Waiting for recovery before consistency test")
        if not backend.wait_ready(scenario):
            diagnostics_needed = True
            (output_dir / "consistency.txt").write_text(
                "SKIPPED: Services did not recover before consistency test\n",
                encoding="utf-8",
            )
            return False

        consistency_ok = run_consistency_test(
            config.paths.consistency_test_dir,
            output_dir / "consistency.txt",
        )
        diagnostics_needed = diagnostics_needed or locust_exit_code != 0 or not consistency_ok
        return locust_exit_code == 0 and consistency_ok
    except Exception as exc:
        diagnostics_needed = True
        (output_dir / "runner-error.txt").write_text(f"{exc}\n", encoding="utf-8")
        return False
    finally:
        stop_event.set()
        if kill_thread:
            kill_thread.join(timeout=5)
        if diagnostics_needed:
            backend.collect_diagnostics(output_dir)
        backend.down()
        log(
            f"Run complete: success={locust_exit_code == 0 and consistency_ok} "
            f"locust_exit={locust_exit_code}"
        )


def log_configuration(config: BenchmarkConfig, run_specs: Sequence[RunSpec]) -> None:
    log("Benchmark configuration:")
    log(f"  Backends:        {', '.join(config.backends)}")
    log(f"  Scenarios:       {', '.join(config.scenarios)}")
    log(f"  Modes:           {', '.join(config.modes)}")
    log(f"  Users:           {', '.join(str(user) for user in config.users)}")
    log(f"  Runs:            {config.runs}")
    log(f"  Duration:        {config.duration}")
    log(f"  Spawn rate:      {config.spawn_rate}")
    log(f"  Locust workers:  {config.locust_workers}")
    log(f"  Startup timeout: {config.startup_timeout}s")
    log(
        "  Sizing:          "
        f"order={config.sizing.order_replicas}, "
        f"payment={config.sizing.payment_replicas}, "
        f"stock={config.sizing.stock_replicas}, "
        f"sentinel={config.sizing.sentinel_replicas}"
    )
    log(f"  Results:         {config.paths.results_dir}")
    log(f"  Total runs:      {len(run_specs)}")


def main(argv: Sequence[str] | None = None) -> int:
    config = parse_cli(argv)
    validate_environment(config)

    if config.clean and config.paths.results_dir.exists():
        shutil.rmtree(config.paths.results_dir)
    config.paths.results_dir.mkdir(parents=True, exist_ok=True)

    run_specs = expand_run_specs(config)
    log_configuration(config, run_specs)

    completed = 0
    failed = 0
    for run_spec in run_specs:
        if run_single_benchmark(config, run_spec):
            completed += 1
        else:
            failed += 1

    log("=" * 72)
    log(f"BENCHMARK COMPLETE | Total: {len(run_specs)} | Completed: {completed} | Failed: {failed}")
    log(f"Results: {config.paths.results_dir}")
    log("=" * 72)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
