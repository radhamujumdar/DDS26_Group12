from __future__ import annotations

import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from benchmark.config import BenchmarkPaths, DeploymentSizing, ScenarioSpec


class MinikubeBackend:
    def __init__(self, paths: BenchmarkPaths, startup_timeout: int, sizing: DeploymentSizing):
        self.paths = paths
        self.startup_timeout = startup_timeout
        self.sizing = sizing
        self.current_gateway_url: str | None = None
        self.current_targets: tuple[str, ...] = ()

    def down(self) -> None:
        subprocess.run(
            ["kubectl", "delete", "-f", str(self.paths.k8s_dir), "--ignore-not-found", "--wait=false"],
            cwd=self.paths.project_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(
            ["kubectl", "delete", "pvc", "--all"],
            cwd=self.paths.project_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(
            ["kubectl", "wait", "--for=delete", "pods", "--all", "--timeout=60s"],
            cwd=self.paths.project_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        self.current_gateway_url = None
        self.current_targets = ()
        time.sleep(2)

    def up(self, mode: str, scenario: ScenarioSpec) -> None:
        del scenario
        self._build_images()
        apply_result = subprocess.run(
            ["kubectl", "apply", "-f", str(self.paths.k8s_dir)],
            cwd=self.paths.project_dir,
            text=True,
            capture_output=True,
            check=False,
        )
        if apply_result.returncode != 0:
            message = (apply_result.stderr or apply_result.stdout or "").strip()
            raise RuntimeError(f"kubectl apply failed: {message}")

        self._set_mode(mode)
        self._apply_deployment_sizing()
        self.current_targets = tuple(self.sizing.k8s_targets().keys())

    def wait_ready(self, scenario: ScenarioSpec) -> bool:
        del scenario
        if not self._wait_for_deployments(self.sizing.k8s_targets(), self.startup_timeout):
            return False
        gateway_url = self.resolve_gateway_url()
        return self._wait_for_gateway_health(gateway_url, self.startup_timeout)

    def resolve_gateway_url(self) -> str:
        configured_url = os.environ.get("BENCHMARK_GATEWAY_URL")
        if configured_url:
            self.current_gateway_url = configured_url.strip().rstrip("/")
            return self.current_gateway_url

        if self.current_gateway_url:
            return self.current_gateway_url

        deadline = time.time() + self.startup_timeout
        while time.time() < deadline:
            for jsonpath in (
                "{.status.loadBalancer.ingress[0].ip}",
                "{.status.loadBalancer.ingress[0].hostname}",
            ):
                result = subprocess.run(
                    ["kubectl", "get", "svc", "gateway", "-o", f"jsonpath={jsonpath}"],
                    cwd=self.paths.project_dir,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                value = result.stdout.strip()
                if result.returncode == 0 and value:
                    self.current_gateway_url = f"http://{value}:8000"
                    return self.current_gateway_url
            time.sleep(3)

        raise RuntimeError(
            "Failed to resolve the gateway LoadBalancer address. Run `minikube tunnel` "
            "or set BENCHMARK_GATEWAY_URL explicitly."
        )

    def kill_target(self, target: str) -> None:
        selectors = []
        if target.endswith("-deployment"):
            selectors.append(f"component={target.removesuffix('-deployment')}")
        selectors.append(f"app={target}")

        pod_name = ""
        for selector in selectors:
            result = subprocess.run(
                ["kubectl", "get", "pods", "-l", selector, "-o", "jsonpath={.items[0].metadata.name}"],
                cwd=self.paths.project_dir,
                capture_output=True,
                text=True,
                check=False,
            )
            pod_name = result.stdout.strip()
            if pod_name:
                break

        if not pod_name:
            raise RuntimeError(f"Could not resolve a pod for target {target}")

        subprocess.run(
            ["kubectl", "delete", "pod", pod_name],
            cwd=self.paths.project_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    def collect_diagnostics(self, output_dir: Path) -> None:
        sections: list[tuple[str, str]] = [
            ("kubectl get deployments", self._capture(["kubectl", "get", "deployments", "-o", "wide"])),
            ("kubectl get pods", self._capture(["kubectl", "get", "pods", "-o", "wide"])),
            ("kubectl get services", self._capture(["kubectl", "get", "services"])),
            ("kubectl get endpoints", self._capture(["kubectl", "get", "endpoints"])),
        ]

        for deployment in self.current_targets:
            sections.append(
                (
                    f"kubectl describe deployment/{deployment}",
                    self._capture(["kubectl", "describe", f"deployment/{deployment}"]),
                )
            )

        for selector, label in (
            ("component=order", "order"),
            ("component=payment", "payment"),
            ("component=stock", "stock"),
            ("app=saga-broker", "saga-broker"),
            ("app=sentinel", "sentinel"),
            ("app=gateway", "gateway"),
        ):
            pods = self._capture(
                ["kubectl", "get", "pods", "-l", selector, "-o", "jsonpath={.items[*].metadata.name}"]
            )
            pod_names = [name for name in pods.split() if name][:2]
            if not pod_names:
                sections.append((f"logs[{label}]", "No pods found"))
                continue
            for pod_name in pod_names:
                sections.append(
                    (
                        f"kubectl logs {pod_name}",
                        self._capture(["kubectl", "logs", pod_name, "--all-containers=true", "--tail=200"]),
                    )
                )

        diagnostics_path = output_dir / "startup-diagnostics.txt"
        with diagnostics_path.open("w", encoding="utf-8") as handle:
            for title, body in sections:
                handle.write(f"===== {title} =====\n")
                handle.write(body or "<no output>")
                handle.write("\n\n")

    def _build_images(self) -> None:
        minikube_env = os.environ.copy()
        result = subprocess.run(
            ["minikube", "docker-env", "--shell", "none"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"minikube docker-env failed: {message}")

        for line in result.stdout.strip().splitlines():
            if "=" in line and not line.startswith("#"):
                key, value = line.split("=", 1)
                minikube_env[key] = value

        for service in ("order", "stock", "payment"):
            build_result = subprocess.run(
                ["docker", "build", "-t", f"{service}:latest", f"./{service}"],
                cwd=self.paths.project_dir,
                env=minikube_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                check=False,
            )
            if build_result.returncode != 0:
                raise RuntimeError(f"docker build failed for {service}")

    def _set_mode(self, mode: str) -> None:
        result = subprocess.run(
            ["kubectl", "set", "env", "deployment/order-deployment", f"TX_MODE={mode}"],
            cwd=self.paths.project_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(f"Failed to set TX_MODE={mode}: {message}")

    def _apply_deployment_sizing(self) -> None:
        for deployment, replicas in self.sizing.k8s_targets().items():
            scale_result = subprocess.run(
                ["kubectl", "scale", f"deployment/{deployment}", f"--replicas={replicas}"],
                cwd=self.paths.project_dir,
                capture_output=True,
                text=True,
                check=False,
            )
            if scale_result.returncode != 0:
                message = (scale_result.stderr or scale_result.stdout or "").strip()
                raise RuntimeError(f"Failed to scale deployment/{deployment}: {message}")

    def _wait_for_deployments(self, targets: dict[str, int] | object, timeout: int, interval: int = 5) -> bool:
        deadline = time.time() + timeout
        target_map = dict(targets)
        while time.time() < deadline:
            pending: list[str] = []
            for deployment, desired in target_map.items():
                result = subprocess.run(
                    [
                        "kubectl",
                        "get",
                        "deployment",
                        deployment,
                        "-o",
                        "jsonpath={.status.readyReplicas}/{.spec.replicas}",
                    ],
                    cwd=self.paths.project_dir,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode != 0:
                    pending.append(f"{deployment} -> missing")
                    continue

                ready_raw = result.stdout.strip() or "0/0"
                ready_str, _, spec_str = ready_raw.partition("/")
                ready = int(ready_str or "0")
                spec_replicas = int(spec_str or "0")
                if spec_replicas != desired:
                    pending.append(f"{deployment} -> spec {spec_replicas}/{desired}")
                elif ready < desired:
                    pending.append(f"{deployment} -> ready {ready}/{desired}")
            if not pending:
                return True
            time.sleep(interval)
        return False

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

    def _capture(self, command: list[str], timeout: int = 60) -> str:
        try:
            result = subprocess.run(
                command,
                cwd=self.paths.project_dir,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return f"Timed out after {timeout}s"
        except Exception as exc:
            return f"Failed to run command: {exc}"

        output = result.stdout or ""
        if result.stderr:
            output += ("\n" if output else "") + result.stderr
        return output.strip()
