#!/usr/bin/env python3
"""
Automated benchmark script for comparing baseline, 2PC, and SAGA checkout
implementations under identical conditions with container failure injection.

Usage:
    python benchmark.py                           # Run all modes, 3 runs each, 2m duration
    python benchmark.py --modes 2pc               # Only 2PC mode
    python benchmark.py --modes 2pc saga          # 2PC and SAGA only
    python benchmark.py --runs 1 --duration 30s   # Quick test: 1 run, 30s
    python benchmark.py --no-kills                # Skip container kills
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
import threading

# from order.app import GATEWAY_URL

# ---------------------------------------------------------------------------
# Paths (adjust if your layout differs)
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = SCRIPT_DIR  # DDS_Group12
BASELINE_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "wdm-project-template")
BENCHMARK_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "bm")
STRESS_TEST_DIR = os.path.join(BENCHMARK_DIR, "stress-test")
CONSISTENCY_TEST_DIR = os.path.join(BENCHMARK_DIR, "consistency-test")
RESULTS_DIR = os.path.join(SCRIPT_DIR, "benchmark-results")

print(SCRIPT_DIR)
print(PROJECT_DIR)
print(BASELINE_DIR)
print(BENCHMARK_DIR)
print(STRESS_TEST_DIR)
print(CONSISTENCY_TEST_DIR)
print(RESULTS_DIR)

GATEWAY_URL = "http://localhost:8000"

# ---------------------------------------------------------------------------
# Kill schedule: (seconds_after_locust_start, container_name)
# One kill at a time, ~20 s recovery between each.
# ---------------------------------------------------------------------------
DEFAULT_KILL_SCHEDULE = [
    (30, "payment-service"),
    (50, "stock-service"),
    (70, "order-service"),
    (90, "payment-db"),
    (110, "stock-db"),
    (130, "order-db"),
]


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------
def docker_compose_down(project_dir: str):
    log(f"  docker compose down -v  ({os.path.basename(project_dir)})")
    subprocess.run(
        ["docker", "compose", "down", "-v", "--remove-orphans"],
        cwd=project_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def docker_compose_up(project_dir: str, env_overrides: dict | None = None):
    log(f"  docker compose up -d --build  ({os.path.basename(project_dir)})")
    env = {**os.environ, **(env_overrides or {})}
    result = subprocess.run(
        ["docker", "compose", "up", "-d", "--build"],
        cwd=project_dir,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log(f"  ERROR: docker compose up failed:\n{result.stderr}")
        sys.exit(1)


def docker_kill_container(project_dir: str, service_name: str):
    """Kill a single container by its compose service name."""
    result = subprocess.run(
        ["docker", "compose", "kill", service_name],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log(f"  WARNING: failed to kill {service_name}: {result.stderr.strip()}")
    else:
        log(f"  KILLED {service_name}")


def wait_for_gateway(timeout: int = 120, interval: int = 3):
    """Poll the gateway until it responds or timeout is reached."""
    import urllib.request
    import urllib.error

    endpoints = [
        "/orders/find/healthcheck",
        "payment/healthcheck",
        "stock/healthcheck",
    ]

    deadline = time.time() + timeout
    while time.time() < deadline:
        all_services_up = True

        for endpoint in endpoints:
            try:
                req = urllib.request.Request(f"{GATEWAY_URL}{endpoint}")
                urllib.request.urlopen(req, timeout=5)
                # Any response (even 4xx) means the gateway is up
                log("  Gateway is responding")
                return True
            except urllib.error.HTTPError:
                # 4xx/5xx means gateway is up and routing works
                log("  Gateway is responding")
                return True
            except Exception:
                all_services_up = False
                break

        if all_services_up:
            log("  Gateway and all services are responding")
            return True

        time.sleep(interval)

    log("  ERROR: Gateway did not become available in time")
    return False


# ---------------------------------------------------------------------------
# Benchmark steps
# ---------------------------------------------------------------------------
def run_init_orders():
    """Run the stress-test init_orders.py to populate databases."""
    log("  Populating databases (init_orders.py) ...")
    result = subprocess.run(
        [sys.executable, "init_orders.py"],
        cwd=STRESS_TEST_DIR,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        log(f"  ERROR: init_orders failed:\n{result.stderr}")
        return False
    log("  Databases populated")
    return True


def run_locust(duration: str, users: int, spawn_rate: int, csv_prefix: str) -> subprocess.Popen:
    """Start locust in headless mode. Returns the Popen object."""
    cmd = [
        sys.executable, "-m", "locust",
        "--headless",
        f"--host={GATEWAY_URL}",
        f"--users={users}",
        f"--spawn-rate={spawn_rate}",
        f"--run-time={duration}",
        f"--csv={csv_prefix}",
        "--csv-full-history",
        "-f", os.path.join(STRESS_TEST_DIR, "locustfile.py"),
    ]
    log(f"  Starting locust: {users} users, spawn-rate {spawn_rate}, duration {duration}")
    proc = subprocess.Popen(
        cmd,
        cwd=STRESS_TEST_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc


def run_kill_schedule(project_dir: str, schedule: list, stop_event: threading.Event):
    """Execute the kill schedule in a background thread."""
    start = time.time()
    for delay_s, service in schedule:
        # Wait until the scheduled time (or until told to stop)
        while time.time() - start < delay_s:
            if stop_event.is_set():
                return
            time.sleep(0.5)
        if stop_event.is_set():
            return
        log(f"  [T+{int(time.time() - start)}s] Killing {service} ...")
        docker_kill_container(project_dir, service)


def run_consistency_test(output_file: str) -> bool:
    """Run the consistency test and capture output."""
    log("  Running consistency test ...")
    try:
        result = subprocess.run(
            [sys.executable, "run_consistency_test.py"],
            cwd=CONSISTENCY_TEST_DIR,
            capture_output=True,
            text=True,
            timeout=300,
        )
        output = result.stdout + "\n" + result.stderr
        with open(output_file, "w") as f:
            f.write(output)
        log("  Consistency test completed")
        return True
    except subprocess.TimeoutExpired:
        log("  WARNING: Consistency test timed out")
        with open(output_file, "w") as f:
            f.write("TIMEOUT: Consistency test did not finish within 5 minutes\n")
        return False
    except Exception as exc:
        log(f"  WARNING: Consistency test error: {exc}")
        with open(output_file, "w") as f:
            f.write(f"ERROR: {exc}\n")
        return False


def collect_locust_csvs(csv_prefix: str, output_dir: str):
    """Move locust CSV files to the output directory."""
    prefix_dir = os.path.dirname(csv_prefix)
    prefix_base = os.path.basename(csv_prefix)
    if not prefix_dir:
        prefix_dir = "."

    for fname in os.listdir(prefix_dir):
        if fname.startswith(prefix_base) and fname.endswith(".csv"):
            src = os.path.join(prefix_dir, fname)
            # Rename to remove the prefix and keep a clean name
            clean_name = fname.replace(prefix_base, "locust", 1)
            dst = os.path.join(output_dir, clean_name)
            with open(src, "r", encoding="utf-8") as infile:
                cleaned_lines = [line for line in infile if line.strip()]

            # Write it to the destination without the extra carriage returns
            with open(dst, "w", encoding="utf-8", newline="") as outfile:
                outfile.writelines(cleaned_lines)

            # Delete the original source file (replacing shutil.move)
            os.remove(src)


# ---------------------------------------------------------------------------
# Main benchmark runner
# ---------------------------------------------------------------------------
def run_single_benchmark(
    mode: str,
    run_number: int,
    duration: str,
    users: int,
    spawn_rate: int,
    kill_schedule: list | None,
):
    """Run a single benchmark iteration for one mode."""

    # Determine project directory and env overrides
    if mode == "baseline":
        project_dir = BASELINE_DIR
        env_overrides = None
    elif mode == "2pc":
        project_dir = PROJECT_DIR
        env_overrides = {"TX_MODE": "2pc"}
    elif mode == "saga":
        project_dir = PROJECT_DIR
        env_overrides = {"TX_MODE": "saga"}
    else:
        log(f"Unknown mode: {mode}")
        return False

    # Create output directory
    output_dir = os.path.join(RESULTS_DIR, mode, f"run_{run_number}")
    os.makedirs(output_dir, exist_ok=True)

    log(f"\n{'='*60}")
    log(f"MODE: {mode.upper()} | RUN: {run_number}")
    log(f"{'='*60}")

    # 1. Clean start
    docker_compose_down(project_dir)
    time.sleep(2)

    # 2. Start services
    docker_compose_up(project_dir, env_overrides)

    # 3. Wait for gateway
    if not wait_for_gateway():
        log("  SKIPPING run - gateway not available")
        docker_compose_down(project_dir)
        return False

    # Extra wait for services to fully initialize (recovery, saga workers, etc.)
    log("  Waiting 10s for services to fully initialize ...")
    time.sleep(10)

    # 4. Populate databases
    if not run_init_orders():
        log("  SKIPPING run - database population failed")
        docker_compose_down(project_dir)
        return False

    # 5. Start locust
    csv_prefix = os.path.join(output_dir, "bm")
    locust_proc = run_locust(duration, users, spawn_rate, csv_prefix)

    # 6. Start kill schedule in background (if enabled)
    stop_event = threading.Event()
    kill_thread = None
    if kill_schedule:
        kill_thread = threading.Thread(
            target=run_kill_schedule,
            args=(project_dir, kill_schedule, stop_event),
            daemon=True,
        )
        kill_thread.start()

    # 7. Wait for locust to finish
    log("  Waiting for locust to complete ...")
    locust_output = locust_proc.communicate()[0]
    locust_exit_code = locust_proc.returncode

    # Save locust stdout
    with open(os.path.join(output_dir, "locust_output.txt"), "w") as f:
        f.write(locust_output)

    # Stop kill schedule thread
    stop_event.set()
    if kill_thread:
        kill_thread.join(timeout=5)

    log(f"  Locust finished (exit code: {locust_exit_code})")

    # 8. Collect locust CSVs
    collect_locust_csvs(csv_prefix, output_dir)

    # 9. Wait a bit for system to stabilize after locust
    log("  Waiting 5s for system to stabilize ...")
    time.sleep(5)

    # 10. Run consistency test
    consistency_file = os.path.join(output_dir, "consistency.txt")
    run_consistency_test(consistency_file)

    # 11. Cleanup
    docker_compose_down(project_dir)

    log(f"  Results saved to: {output_dir}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Automated benchmark for baseline/2PC/SAGA comparison"
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        default=["baseline", "2pc", "saga"],
        choices=["baseline", "2pc", "saga"],
        help="Which modes to benchmark (default: all three)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of runs per mode (default: 3)",
    )
    parser.add_argument(
        "--duration",
        default="3m",
        help="Locust test duration (default: 3m). Examples: 30s, 1m, 2m",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=100,
        help="Number of concurrent locust users (default: 100)",
    )
    parser.add_argument(
        "--spawn-rate",
        type=int,
        default=10,
        help="Locust user spawn rate (default: 10)",
    )
    parser.add_argument(
        "--no-kills",
        action="store_true",
        help="Skip container kill schedule (no failure injection)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete previous benchmark results before starting",
    )
    args = parser.parse_args()

    # Validate paths
    if "baseline" in args.modes and not os.path.isdir(BASELINE_DIR):
        log(f"ERROR: Baseline project not found at {BASELINE_DIR}")
        log("  Adjust BASELINE_DIR in the script or remove 'baseline' from --modes")
        sys.exit(1)

    if not os.path.isdir(BENCHMARK_DIR):
        log(f"ERROR: Benchmark scripts not found at {BENCHMARK_DIR}")
        sys.exit(1)

    # Clean previous results if requested
    if args.clean and os.path.isdir(RESULTS_DIR):
        shutil.rmtree(RESULTS_DIR)
        log("Cleaned previous benchmark results")

    os.makedirs(RESULTS_DIR, exist_ok=True)

    kill_schedule = None if args.no_kills else DEFAULT_KILL_SCHEDULE

    log(f"Benchmark configuration:")
    log(f"  Modes:      {', '.join(args.modes)}")
    log(f"  Runs:       {args.runs}")
    log(f"  Duration:   {args.duration}")
    log(f"  Users:      {args.users}")
    log(f"  Spawn rate: {args.spawn_rate}")
    log(f"  Kills:      {'disabled' if args.no_kills else 'enabled'}")
    log(f"  Results:    {RESULTS_DIR}")
    log("")

    total_runs = len(args.modes) * args.runs
    completed = 0
    failed = 0

    for mode in args.modes:
        for run_number in range(1, args.runs + 1):
            success = run_single_benchmark(
                mode=mode,
                run_number=run_number,
                duration=args.duration,
                users=args.users,
                spawn_rate=args.spawn_rate,
                kill_schedule=kill_schedule,
            )
            if success:
                completed += 1
            else:
                failed += 1

    log(f"\n{'='*60}")
    log(f"BENCHMARK COMPLETE")
    log(f"  Total: {total_runs} | Completed: {completed} | Failed: {failed}")
    log(f"  Results in: {RESULTS_DIR}")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
