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
import json
import os
import shutil
import subprocess
import sys
import time
import threading
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths (adjust if your layout differs)
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = SCRIPT_DIR  # DDS_Group12
BASELINE_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "wdm-project-template")


def resolve_benchmark_dir() -> str:
    candidates = [
        os.path.join(os.path.dirname(SCRIPT_DIR), "bm"),
        os.path.join(os.path.dirname(SCRIPT_DIR), "wdm-project-benchmark"),
    ]
    for candidate in candidates:
        if os.path.isdir(candidate):
            return candidate
    return candidates[0]


BENCHMARK_DIR = resolve_benchmark_dir()
STRESS_TEST_DIR = os.path.join(BENCHMARK_DIR, "stress-test")
CONSISTENCY_TEST_DIR = os.path.join(BENCHMARK_DIR, "consistency-test")
RESULTS_DIR = os.path.join(SCRIPT_DIR, "benchmark-results")

GATEWAY_URL: str | None = None

# ---------------------------------------------------------------------------
# Kill schedule: (seconds_after_locust_start, container_name)
# One kill at a time, ~20 s recovery between each.
# ---------------------------------------------------------------------------
DEFAULT_KILL_SCHEDULE = [
    (30, "payment-deployment"),
    (50, "stock-deployment"),
    (70, "order-deployment"),
    (90, "payment-db"),
    (110, "stock-db"),
    (130, "order-db"),
]

DEFAULT_STARTUP_TIMEOUT_SECONDS = 300
K8S_STARTUP_DEPLOYMENTS = (
    "gateway",
    "sentinel",
    "order-db",
    "stock-db",
    "payment-db",
    "saga-broker",
    "order-deployment",
    "stock-deployment",
    "payment-deployment",
)


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------
def docker_compose_down(project_dir: str):
    log(f"  Cleaning up K8s resources... ({os.path.basename(project_dir)})")

    # 1. Delete all resources in the k8s folder
    subprocess.run(
        ["kubectl", "delete", "-f", "k8s/", "--ignore-not-found", "--wait=false"],
        cwd=project_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    
    # 2. Delete Persistent Volume Claims to ensure clean databases
    log("    Deleting persistent storage (PVCs)...")
    subprocess.run(
        ["kubectl", "delete", "pvc", "--all"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    
    # 3. Hard wait for pods to actually disappear (timeout 60s)
    log("    Waiting for all pods to terminate...")
    subprocess.run(
        ["kubectl", "wait", "--for=delete", "pods", "--all", "--timeout=60s"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    
    # 4. Small pause for network/loadbalancer stabilization
    time.sleep(2)


def _normalize_gateway_url(url: str) -> str:
    return url.strip().rstrip("/")


def resolve_gateway_url(project_dir: str, timeout: int = 120, interval: int = 3) -> str:
    configured_url = os.environ.get("BENCHMARK_GATEWAY_URL")
    if configured_url:
        return _normalize_gateway_url(configured_url)

    deadline = time.time() + timeout
    while time.time() < deadline:
        lb_ip_result = subprocess.run(
            ["kubectl", "get", "svc", "gateway", "-o", "jsonpath={.status.loadBalancer.ingress[0].ip}"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        if lb_ip_result.returncode == 0:
            lb_ip = lb_ip_result.stdout.strip()
            if lb_ip:
                return f"http://{lb_ip}:8000"

        lb_hostname_result = subprocess.run(
            ["kubectl", "get", "svc", "gateway", "-o", "jsonpath={.status.loadBalancer.ingress[0].hostname}"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        if lb_hostname_result.returncode == 0:
            lb_hostname = lb_hostname_result.stdout.strip()
            if lb_hostname:
                return f"http://{lb_hostname}:8000"

        get_svc_result = subprocess.run(
            ["kubectl", "get", "svc", "gateway"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        if get_svc_result.returncode != 0:
            log("    Waiting for gateway service to be created...")
        else:
            log("    Waiting for gateway service EXTERNAL-IP...")
        time.sleep(interval)

    raise RuntimeError(
        "Failed to resolve a direct gateway URL. "
        "For minikube LoadBalancer services on Docker/WSL, run 'minikube tunnel' "
        "in a separate terminal until the gateway service gets an EXTERNAL-IP, "
        "or set BENCHMARK_GATEWAY_URL explicitly."
    )


def configure_benchmark_urls(gateway_url: str):
    urls_payload = {
        "ORDER_URL": gateway_url,
        "PAYMENT_URL": gateway_url,
        "STOCK_URL": gateway_url,
    }
    urls_path = os.path.join(BENCHMARK_DIR, "urls.json")
    with open(urls_path, "w", encoding="utf-8") as urls_file:
        json.dump(urls_payload, urls_file, indent=2)
        urls_file.write("\n")


def docker_compose_up(project_dir: str, env_overrides: dict | None = None):
    global GATEWAY_URL
    log(f"  Building images and applying K8s manifests... ({os.path.basename(project_dir)})")

    # 1. Build images inside minikube's Docker daemon so K8s can find them
    minikube_env = os.environ.copy()
    result = subprocess.run(
        ["minikube", "docker-env", "--shell", "none"],
        capture_output=True, text=True,
    )
    for line in result.stdout.strip().splitlines():
        if "=" in line and not line.startswith("#"):
            key, val = line.split("=", 1)
            minikube_env[key] = val

    for svc in ["order", "stock", "payment"]:
        log(f"    docker build -t {svc}:latest ./{svc}")
        subprocess.run(
            ["docker", "build", "-t", f"{svc}:latest", f"./{svc}"],
            cwd=project_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            env=minikube_env,
        )

    # 2. Apply K8s manifests
    log(f"    kubectl apply -f k8s/")
    subprocess.run(
        ["kubectl", "apply", "-f", "k8s/"],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )

    apply_k8s_env_overrides(project_dir, env_overrides)

    GATEWAY_URL = resolve_gateway_url(project_dir)
    configure_benchmark_urls(GATEWAY_URL)
    log(f"    Using gateway URL: {GATEWAY_URL}")

    # 3. Wait for gateway routing to become ready
    log("    Waiting for gateway pod to be ready...")
    # Retry kubectl wait because the pod may not exist yet right after apply
    deadline = time.time() + 90
    while time.time() < deadline:
        result = subprocess.run(
            ["kubectl", "wait", "--for=condition=ready", "pod", "-l", "app=gateway",
             "--timeout=10s"],
            capture_output=True,
        )
        if result.returncode == 0:
            break
        time.sleep(2)
    else:
        log("    WARNING: gateway pod did not become ready in 90s")


def apply_k8s_env_overrides(project_dir: str, env_overrides: dict | None = None):
    if not env_overrides:
        return

    # Only the order deployment needs checkout mode overrides today.
    deployment_envs: dict[str, list[str]] = {"order-deployment": []}
    for key, value in env_overrides.items():
        if key == "TX_MODE":
            deployment_envs["order-deployment"].append(f"{key}={value}")

    for deployment, entries in deployment_envs.items():
        if not entries:
            continue

        log(f"    Applying env overrides to {deployment}: {', '.join(entries)}")
        result = subprocess.run(
            ["kubectl", "set", "env", f"deployment/{deployment}", *entries],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to apply env overrides to {deployment}: "
                f"{(result.stderr or result.stdout).strip()}"
            )
        log(f"    Env overrides applied to {deployment}; rollout will be validated by health checks")



def docker_kill_container(project_dir: str, service_name: str):
    """Kill a single pod by its deployment name."""
    # Find a pod name for the deployment
    result = subprocess.run(
        ["kubectl", "get", "pods", "-l", f"component={service_name.replace('-deployment', '')}", "-o", "jsonpath={.items[0].metadata.name}"],
        capture_output=True,
        text=True,
    )
    # If that fails (it might be a DB name or replica which uses 'app' label)
    if not result.stdout:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-l", f"app={service_name}", "-o", "jsonpath={.items[0].metadata.name}"],
            capture_output=True,
            text=True,
        )
    
    pod_name = result.stdout.strip()
    if pod_name:
        subprocess.run(["kubectl", "delete", "pod", pod_name])
        log(f"  KILLED POD {pod_name}")
    else:
        log(f"  WARNING: could not find pod for {service_name}")


def wait_for_k8s_deployments(project_dir: str, timeout: int = DEFAULT_STARTUP_TIMEOUT_SECONDS, interval: int = 5):
    """Wait for the key K8s deployments to have all desired replicas ready."""
    deadline = time.time() + timeout

    while time.time() < deadline:
        pending: list[str] = []

        for deployment in K8S_STARTUP_DEPLOYMENTS:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    deployment,
                    "-o",
                    "jsonpath={.status.readyReplicas}/{.status.replicas}",
                ],
                cwd=project_dir,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                pending.append(f"{deployment} -> missing")
                continue

            ready_raw = result.stdout.strip() or "0/0"
            try:
                ready_str, desired_str = ready_raw.split("/", 1)
                ready = int(ready_str or "0")
                desired = int(desired_str or "0")
            except ValueError:
                pending.append(f"{deployment} -> unknown ({ready_raw})")
                continue

            if desired == 0:
                pending.append(f"{deployment} -> scaled to 0")
            elif ready < desired:
                pending.append(f"{deployment} -> {ready}/{desired} ready")

        if not pending:
            log("  Kubernetes deployments are ready")
            return True

        log(f"  Waiting for deployments: {', '.join(pending)}")
        time.sleep(interval)

    log("  ERROR: Kubernetes deployments did not become ready in time")
    return False


def collect_startup_diagnostics(project_dir: str, output_dir: str):
    """Capture K8s state and recent logs when startup fails."""

    def capture(command: list[str], timeout: int = 60) -> str:
        try:
            result = subprocess.run(
                command,
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return f"Timed out after {timeout}s"
        except Exception as exc:
            return f"Failed to run command: {exc}"

        output = result.stdout or ""
        if result.stderr:
            output += ("\n" if output else "") + result.stderr
        return output.strip()

    sections: list[tuple[str, str]] = [
        ("kubectl get deployments", capture(["kubectl", "get", "deployments", "-o", "wide"])),
        ("kubectl get pods", capture(["kubectl", "get", "pods", "-o", "wide"])),
        ("kubectl get services", capture(["kubectl", "get", "services"])),
        ("kubectl get endpoints", capture(["kubectl", "get", "endpoints"])),
    ]

    for deployment in K8S_STARTUP_DEPLOYMENTS:
        sections.append(
            (
                f"kubectl describe deployment/{deployment}",
                capture(["kubectl", "describe", f"deployment/{deployment}"]),
            )
        )

    log_targets = [
        ("component=order", "order"),
        ("component=payment", "payment"),
        ("component=stock", "stock"),
        ("app=saga-broker", "saga-broker"),
        ("app=sentinel", "sentinel"),
        ("app=gateway", "gateway"),
    ]
    for selector, label in log_targets:
        pods_raw = capture(["kubectl", "get", "pods", "-l", selector, "-o", "jsonpath={.items[*].metadata.name}"])
        pod_names = [name for name in pods_raw.split() if name][:2]
        if not pod_names:
            sections.append((f"logs[{label}]", "No pods found"))
            continue

        for pod_name in pod_names:
            sections.append(
                (
                    f"kubectl logs {pod_name}",
                    capture(["kubectl", "logs", pod_name, "--all-containers=true", "--tail=200"]),
                )
            )
            previous_logs = capture(
                ["kubectl", "logs", pod_name, "--all-containers=true", "--previous", "--tail=200"]
            )
            if previous_logs:
                sections.append((f"kubectl logs {pod_name} --previous", previous_logs))

    diagnostics_path = os.path.join(output_dir, "startup-diagnostics.txt")
    with open(diagnostics_path, "w", encoding="utf-8") as diagnostics_file:
        for title, body in sections:
            diagnostics_file.write(f"===== {title} =====\n")
            diagnostics_file.write(body or "<no output>")
            diagnostics_file.write("\n\n")

    log(f"  Startup diagnostics saved to: {diagnostics_path}")


def wait_for_gateway(timeout: int = DEFAULT_STARTUP_TIMEOUT_SECONDS, interval: int = 3):
    """Wait until gateway routing works for all service health endpoints."""
    import urllib.request
    import urllib.error

    if not GATEWAY_URL:
        log("  ERROR: Gateway URL is not configured")
        return False

    endpoints = [
        "/orders/health",
        "/payment/health",
        "/stock/health",
    ]

    deadline = time.time() + timeout
    while time.time() < deadline:
        healthy_endpoints: list[str] = []
        failed_endpoints: list[str] = []

        for endpoint in endpoints:
            try:
                req = urllib.request.Request(f"{GATEWAY_URL}{endpoint}")
                with urllib.request.urlopen(req, timeout=5) as response:
                    if 200 <= response.status < 300:
                        healthy_endpoints.append(endpoint)
                    else:
                        failed_endpoints.append(f"{endpoint} -> HTTP {response.status}")
            except urllib.error.HTTPError as exc:
                failed_endpoints.append(f"{endpoint} -> HTTP {exc.code}")
            except Exception:
                failed_endpoints.append(f"{endpoint} -> unavailable")

        if len(healthy_endpoints) == len(endpoints):
            log("  Gateway and all services are responding")
            return True

        log(f"  Waiting for healthy services: {', '.join(failed_endpoints)}")
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
    if not GATEWAY_URL:
        raise RuntimeError("Gateway URL is not configured")

    cmd = [
        sys.executable, "-m", "locust",
        "--headless",
        f"--host={GATEWAY_URL}",
        f"--users={users}",
        f"--spawn-rate={spawn_rate}",
        f"--run-time={duration}",
        f"--csv={csv_prefix}",
        "-f", os.path.join(STRESS_TEST_DIR, "locustfile.py"),
    ]

    # Use all CPU cores on non-Windows (--processes forks workers automatically).
    # NOTE: --csv-full-history is omitted because locust strips --csv from worker
    # args but not --csv-full-history, causing workers to error out.
    if sys.platform != "win32":
        cmd.append("--processes=-1")
        log(f"  Starting locust (multi-core): {users} users, spawn-rate {spawn_rate}, duration {duration}")
    else:
        log(f"  Starting locust: {users} users, spawn-rate {spawn_rate}, duration {duration}")
    proc = subprocess.Popen(
        cmd,
        cwd=STRESS_TEST_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1, # Line buffered
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
        if not wait_for_gateway(timeout=120, interval=3):
            log("  WARNING: Services did not recover before consistency test")
            with open(output_file, "w") as f:
                f.write("SKIPPED: Services did not recover before consistency test\n")
            return False

        # Check if run_consistency_test.py exists in CONSISTENCY_TEST_DIR
        if not os.path.isfile(os.path.join(CONSISTENCY_TEST_DIR, "run_consistency_test.py")):
            log(f"  WARNING: Consistency test script not found in {CONSISTENCY_TEST_DIR}")
            with open(output_file, "w") as f:
                f.write("SKIPPED: Consistency test script not found\n")
            return False

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
        if result.returncode != 0:
            log(f"  WARNING: Consistency test failed (exit code: {result.returncode})")
            return False
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
    run_timestamp: str,
    duration: str,
    users: int,
    spawn_rate: int,
    kill_schedule: list | None,
    startup_timeout: int,
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
    run_folder_name = f"run_{run_number}_{run_timestamp}"
    output_dir = os.path.join(RESULTS_DIR, mode, run_folder_name)
    os.makedirs(output_dir, exist_ok=False)

    log(f"\n{'='*60}")
    log(f"MODE: {mode.upper()} | RUN: {run_number}")
    log(f"RUN FOLDER: {run_folder_name}")
    log(f"{'='*60}")

    # 1. Clean start
    docker_compose_down(project_dir)
    time.sleep(2)

    # 2. Start services
    try:
        docker_compose_up(project_dir, env_overrides)
    except RuntimeError as exc:
        log(f"  SKIPPING run - deployment failed: {exc}")
        collect_startup_diagnostics(project_dir, output_dir)
        docker_compose_down(project_dir)
        return False

    # 3. Wait for K8s workloads and gateway routing
    if not wait_for_k8s_deployments(project_dir, timeout=startup_timeout):
        log("  SKIPPING run - Kubernetes deployments not ready")
        collect_startup_diagnostics(project_dir, output_dir)
        docker_compose_down(project_dir)
        return False

    if not wait_for_gateway(timeout=startup_timeout):
        log("  SKIPPING run - gateway not available")
        collect_startup_diagnostics(project_dir, output_dir)
        docker_compose_down(project_dir)
        return False

    # Extra wait for services to fully initialize (Sentinel master discovery, replicas syncing, etc.)
    log("  Waiting 30s for HA services to fully stabilize ...")
    time.sleep(30)

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

    # 9. Wait for services to recover after the load test before running consistency checks
    log("  Waiting for services to recover after locust ...")
    if not wait_for_gateway(timeout=120, interval=3):
        log("  WARNING: Services did not fully recover after locust")

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
    parser.add_argument(
        "--startup-timeout",
        type=int,
        default=DEFAULT_STARTUP_TIMEOUT_SECONDS,
        help="Maximum seconds to wait for K8s deployments and gateway health before skipping a run (default: 300)",
    )
    args = parser.parse_args()

    # Validate paths
    if "baseline" in args.modes and not os.path.isdir(BASELINE_DIR):
        log(f"ERROR: Baseline project not found at {BASELINE_DIR}")
        log("  Adjust BASELINE_DIR in the script or remove 'baseline' from --modes")
        sys.exit(1)

    if not os.path.isdir(BENCHMARK_DIR):
        log(f"ERROR: Benchmark directory not found at {BENCHMARK_DIR}")
        sys.exit(1)

    if not os.path.isdir(STRESS_TEST_DIR):
        log(f"ERROR: Stress test directory not found at {STRESS_TEST_DIR}")
        sys.exit(1)

    os.makedirs(RESULTS_DIR, exist_ok=True)

    kill_schedule = None if args.no_kills else DEFAULT_KILL_SCHEDULE

    run_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]

    log(f"Benchmark configuration:")
    log(f"  Modes:      {', '.join(args.modes)}")
    log(f"  Runs:       {args.runs}")
    log(f"  Duration:   {args.duration}")
    log(f"  Users:      {args.users}")
    log(f"  Spawn rate: {args.spawn_rate}")
    log(f"  Kills:      {'disabled' if args.no_kills else 'enabled'}")
    log(f"  Startup:    {args.startup_timeout}s timeout")
    log(f"  Results:    {RESULTS_DIR}")
    log(f"  Timestamp:  {run_timestamp}")
    log("")

    total_runs = len(args.modes) * args.runs
    completed = 0
    failed = 0

    for mode in args.modes:
        for run_number in range(1, args.runs + 1):
            success = run_single_benchmark(
                mode=mode,
                run_number=run_number,
                run_timestamp=run_timestamp,
                duration=args.duration,
                users=args.users,
                spawn_rate=args.spawn_rate,
                kill_schedule=kill_schedule,
                startup_timeout=args.startup_timeout,
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
