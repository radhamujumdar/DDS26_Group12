from __future__ import annotations

import asyncio
import base64
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import unittest
import uuid

import httpx
from redis import Redis as SyncRedis
from redis.sentinel import Sentinel as SyncSentinel

REPO_ROOT = Path(__file__).resolve().parents[1]
ENGINE_SRC = REPO_ROOT / "packages" / "fluxi-engine" / "src"


def _purge_preloaded_modules(prefix: str) -> None:
    for name in tuple(sys.modules):
        if name == prefix or name.startswith(f"{prefix}."):
            del sys.modules[name]


for module_prefix in ("fluxi_engine",):
    _purge_preloaded_modules(module_prefix)
resolved_engine_src = str(ENGINE_SRC)
if resolved_engine_src in sys.path:
    sys.path.remove(resolved_engine_src)
sys.path.insert(0, resolved_engine_src)

from fluxi_engine import FluxiRedisStore, FluxiSettings, create_app, create_redis_client
from fluxi_engine.codecs import packb, unpackb
from fluxi_engine.keys import activity_queue, workflow_queue


def _find_free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _docker_path() -> str:
    docker = shutil.which("docker")
    if docker is None:
        raise unittest.SkipTest("Docker is not available for Fluxi integration tests.")
    return docker


def _ensure_docker_available() -> str:
    docker = _docker_path()
    try:
        subprocess.run(
            [docker, "info"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception as exc:
        raise unittest.SkipTest(
            f"Docker is unavailable for Fluxi integration tests: {exc}"
        ) from exc
    return docker


def _best_effort_run(
    args: list[str],
    *,
    timeout: int = 15,
) -> None:
    try:
        subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return


class RedisHarness:
    def __init__(self) -> None:
        self._container_id: str | None = None
        self._owns_container = False
        self.url: str | None = None
        self.port: int | None = None

    def start(self) -> None:
        explicit_url = os.getenv("FLUXI_TEST_REDIS_URL")
        if explicit_url:
            self.url = explicit_url
            return

        docker = _ensure_docker_available()

        self.port = _find_free_port()
        result = subprocess.run(
            [
                docker,
                "run",
                "--rm",
                "-d",
                "-p",
                f"{self.port}:6379",
                "redis:7.2-bookworm",
                "redis-server",
                "--save",
                "",
                "--appendonly",
                "no",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self._container_id = result.stdout.strip()
        self._owns_container = True
        self.url = f"redis://127.0.0.1:{self.port}/0"

    def stop(self) -> None:
        if not self._owns_container or not self._container_id:
            return
        docker = shutil.which("docker")
        if docker is None:
            return
        _best_effort_run([docker, "rm", "-f", self._container_id])
        self._container_id = None


class SentinelHarness:
    def __init__(self) -> None:
        self.service_name = "fluxi-master"
        self.redis_password = "redis"
        self.redis_db = 0
        self.master_port = _find_free_port()
        self.replica_port = _find_free_port()
        self.sentinel_ports = [_find_free_port() for _ in range(3)]
        self._network_name = f"fluxi-sentinel-net-{uuid.uuid4().hex[:8]}"
        self._master_name = f"fluxi-redis-master-{uuid.uuid4().hex[:8]}"
        self._replica_name = f"fluxi-redis-replica-{uuid.uuid4().hex[:8]}"
        self._sentinel_names = [
            f"fluxi-sentinel-{index}-{uuid.uuid4().hex[:8]}"
            for index in range(1, 4)
        ]
        self._master_container_id: str | None = None
        self._replica_container_id: str | None = None
        self._sentinel_container_ids: list[str] = []

    @property
    def redis_url(self) -> str:
        return f"redis://:{self.redis_password}@127.0.0.1:{self.master_port}/{self.redis_db}"

    @property
    def sentinel_endpoints(self) -> str:
        return ",".join(f"127.0.0.1:{port}" for port in self.sentinel_ports)

    def start(self) -> None:
        docker = _ensure_docker_available()
        self._run(docker, ["network", "create", self._network_name])
        try:
            self._master_container_id = self._start_master(docker)
            self._replica_container_id = self._start_replica(docker)
            self._sentinel_container_ids = [
                self._start_sentinel(docker, name, port)
                for name, port in zip(self._sentinel_names, self.sentinel_ports, strict=True)
            ]
            self.wait_until_ready()
            self.wait_until_replica_ready()
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        docker = shutil.which("docker")
        if docker is None:
            return
        container_ids = [
            *self._sentinel_container_ids,
            self._replica_container_id,
            self._master_container_id,
        ]
        for container_id in container_ids:
            if not container_id:
                continue
            _best_effort_run([docker, "rm", "-f", container_id])
        self._sentinel_container_ids = []
        self._replica_container_id = None
        self._master_container_id = None
        _best_effort_run([docker, "network", "rm", self._network_name])

    def discover_master_address(self) -> tuple[str, int]:
        sentinel = SyncSentinel(
            [("127.0.0.1", port) for port in self.sentinel_ports],
            socket_timeout=1.0,
        )
        try:
            host, port = sentinel.discover_master(self.service_name)
            return _remap_discovered_address((str(host), int(port)))
        finally:
            for sentinel_client in sentinel.sentinels:
                sentinel_client.close()

    def wait_until_ready(self, timeout_seconds: float = 45.0) -> tuple[str, int]:
        deadline = time.time() + timeout_seconds
        last_error: BaseException | None = None
        while time.time() < deadline:
            try:
                host, port = self.discover_master_address()
                client = SyncRedis(
                    host=host,
                    port=port,
                    password=self.redis_password,
                    db=self.redis_db,
                    socket_timeout=1.0,
                )
                try:
                    client.ping()
                    return host, port
                finally:
                    client.close()
            except Exception as exc:
                last_error = exc
                time.sleep(0.25)
        raise TimeoutError(
            f"Timed out waiting for Sentinel to become ready: {last_error}"
        )

    def force_failover(self, timeout_seconds: float = 60.0) -> tuple[str, int]:
        old_host, old_port = self.wait_until_ready()
        if old_port == self.master_port:
            self.wait_until_replica_ready(timeout_seconds=timeout_seconds)
        self._stop_container_for_port(old_port)

        deadline = time.time() + timeout_seconds
        last_error: BaseException | None = None
        while time.time() < deadline:
            try:
                host, port = self.wait_until_ready(timeout_seconds=5.0)
                if (host, port) != (old_host, old_port):
                    return host, port
            except Exception as exc:
                last_error = exc
            time.sleep(0.5)
        raise TimeoutError(
            f"Timed out waiting for Sentinel failover to complete: {last_error}"
        )

    def stop_current_master(self) -> None:
        _, current_port = self.wait_until_ready()
        self._stop_container_for_port(current_port)

    def wait_until_replica_ready(self, timeout_seconds: float = 45.0) -> None:
        deadline = time.time() + timeout_seconds
        last_error: BaseException | None = None
        while time.time() < deadline:
            client = SyncRedis(
                host="127.0.0.1",
                port=self.replica_port,
                password=self.redis_password,
                db=self.redis_db,
                socket_timeout=1.0,
            )
            try:
                info = client.info("replication")
                if (
                    info.get("role") == "slave"
                    and info.get("master_link_status") == "up"
                ):
                    return
            except Exception as exc:
                last_error = exc
            finally:
                client.close()
            time.sleep(0.25)
        raise TimeoutError(
            f"Timed out waiting for replica to sync with master: {last_error}"
        )

    def _stop_container_for_port(self, port: int) -> None:
        docker = _docker_path()
        target: str | None
        if port == self.master_port:
            target = self._master_container_id
            self._master_container_id = None
        elif port == self.replica_port:
            target = self._replica_container_id
            self._replica_container_id = None
        else:
            raise RuntimeError(f"Cannot map port {port} to a Redis container.")
        if target is None:
            raise RuntimeError(f"No container is registered for Redis port {port}.")
        _best_effort_run([docker, "rm", "-f", target])

    def _start_master(self, docker: str) -> str:
        result = self._run(
            docker,
            [
                "run",
                "--rm",
                "-d",
                "--name",
                self._master_name,
                "--network",
                self._network_name,
                "--publish",
                f"{self.master_port}:6379",
                "--add-host",
                "host.docker.internal:host-gateway",
                "redis:7.2-bookworm",
                "redis-server",
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--save",
                "",
                "--protected-mode",
                "no",
                "--requirepass",
                self.redis_password,
                "--maxmemory",
                "512mb",
            ],
        )
        return result.stdout.strip()

    def _start_replica(self, docker: str) -> str:
        result = self._run(
            docker,
            [
                "run",
                "--rm",
                "-d",
                "--name",
                self._replica_name,
                "--network",
                self._network_name,
                "--publish",
                f"{self.replica_port}:6379",
                "--add-host",
                "host.docker.internal:host-gateway",
                "redis:7.2-bookworm",
                "redis-server",
                "--replicaof",
                "host.docker.internal",
                str(self.master_port),
                "--masterauth",
                self.redis_password,
                "--requirepass",
                self.redis_password,
                "--replica-announce-ip",
                "host.docker.internal",
                "--replica-announce-port",
                str(self.replica_port),
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "no",
                "--maxmemory",
                "512mb",
            ],
        )
        return result.stdout.strip()

    def _start_sentinel(self, docker: str, name: str, port: int) -> str:
        sentinel_command = "\n".join(
            [
                "cat <<'EOF' >/tmp/sentinel.conf",
                "port 26379",
                "dir /tmp",
                "protected-mode no",
                "sentinel resolve-hostnames yes",
                "sentinel announce-hostnames yes",
                f"sentinel monitor {self.service_name} host.docker.internal {self.master_port} 2",
                f"sentinel auth-pass {self.service_name} {self.redis_password}",
                f"sentinel down-after-milliseconds {self.service_name} 3000",
                f"sentinel failover-timeout {self.service_name} 10000",
                f"sentinel parallel-syncs {self.service_name} 1",
                "EOF",
                "redis-server /tmp/sentinel.conf --sentinel",
            ]
        )
        result = self._run(
            docker,
            [
                "run",
                "--rm",
                "-d",
                "--name",
                name,
                "--network",
                self._network_name,
                "--publish",
                f"{port}:26379",
                "--add-host",
                "host.docker.internal:host-gateway",
                "redis:7.2-bookworm",
                "sh",
                "-c",
                sentinel_command,
            ],
        )
        return result.stdout.strip()

    def _run(self, docker: str, args: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [docker, *args],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )


class FluxiEngineAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    harness: RedisHarness
    settings: FluxiSettings
    store: FluxiRedisStore
    client: httpx.AsyncClient

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.harness = RedisHarness()
        cls.harness.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.harness.stop()
        super().tearDownClass()

    async def asyncSetUp(self) -> None:
        if self.harness.url is None:
            raise unittest.SkipTest("No Redis URL available for engine tests.")

        self.settings = FluxiSettings(
            redis_url=self.harness.url,
            key_prefix=f"fluxi-test:{self._testMethodName}",
            workflow_task_timeout_ms=50,
            result_poll_interval_ms=10,
            timer_poll_interval_ms=10,
            pending_idle_threshold_ms=1,
            pending_claim_count=100,
        )
        redis = create_redis_client(self.settings)
        self.store = FluxiRedisStore(redis, self.settings)
        await self._wait_until_redis_ready()
        await self.store.flushdb()

        app = create_app(self.settings, store=self.store)
        app.state.store = self.store
        app.state.settings = self.settings
        transport = httpx.ASGITransport(app=app)
        self.client = httpx.AsyncClient(transport=transport, base_url="http://testserver")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        await self.store.flushdb()
        await self.store.aclose()

    async def _wait_until_redis_ready(self) -> None:
        deadline = time.time() + 15
        while True:
            try:
                await self.store.redis.ping()
                return
            except Exception:
                if time.time() >= deadline:
                    raise
                await asyncio.sleep(0.1)

    async def start_workflow(
        self,
        *,
        workflow_id: str = "checkout:1",
        workflow_name: str = "CheckoutWorkflow",
        task_queue: str = "orders",
        start_policy: str = "allow_duplicate",
        input_payload: dict | None = None,
    ) -> dict:
        response = await self.client.post(
            "/workflows/start-or-attach",
            json={
                "workflow_id": workflow_id,
                "workflow_name": workflow_name,
                "task_queue": task_queue,
                "start_policy": start_policy,
                "input_payload_b64": self.payload_b64(
                    input_payload or {"order_id": workflow_id}
                ),
            },
        )
        response.raise_for_status()
        return response.json()

    async def latest_stream_payload(self, stream_key: str) -> tuple[str, dict]:
        entries = await self.store.redis.xrange(stream_key)
        entry_id, values = entries[-1]
        payload = values[b"payload"] if b"payload" in values else values["payload"]
        return (
            entry_id.decode("utf-8") if isinstance(entry_id, bytes) else entry_id,
            unpackb(payload),
        )

    async def read_group_message(
        self,
        *,
        stream_key: str,
        group_name: str,
        consumer_name: str,
    ) -> tuple[str, dict]:
        messages = await self.store.redis.xreadgroup(
            group_name,
            consumer_name,
            {stream_key: ">"},
            count=1,
            block=100,
        )
        stream_messages = messages[0][1]
        message_id, values = stream_messages[0]
        payload = values[b"payload"] if b"payload" in values else values["payload"]
        return (
            message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id,
            unpackb(payload),
        )

    def workflow_stream_key(self, task_queue: str = "orders") -> str:
        return workflow_queue(self.settings.key_prefix, task_queue)

    def activity_stream_key(self, task_queue: str) -> str:
        return activity_queue(self.settings.key_prefix, task_queue)

    def payload_b64(self, payload: object) -> str:
        return base64.b64encode(packb(payload)).decode("ascii")


def _remap_discovered_address(address: tuple[str, int]) -> tuple[str, int]:
    host, port = address
    if host in {"host.docker.internal", "gateway.docker.internal"}:
        return ("127.0.0.1", port)
    return address


class FluxiSentinelEngineAsyncTestCase(FluxiEngineAsyncTestCase):
    harness: SentinelHarness

    @classmethod
    def setUpClass(cls) -> None:
        unittest.IsolatedAsyncioTestCase.setUpClass()
        cls.harness = SentinelHarness()
        cls.harness.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.harness.stop()
        unittest.IsolatedAsyncioTestCase.tearDownClass()

    async def asyncSetUp(self) -> None:
        try:
            self.harness.wait_until_ready(timeout_seconds=10.0)
        except Exception:
            self.harness.stop()
            type(self).harness = SentinelHarness()
            self.harness = type(self).harness
            self.harness.start()

        self.settings = FluxiSettings(
            redis_mode="sentinel",
            redis_url=self.harness.redis_url,
            sentinel_endpoints=self.harness.sentinel_endpoints,
            sentinel_service_name=self.harness.service_name,
            key_prefix=f"fluxi-sentinel-test:{self._testMethodName}",
            workflow_task_timeout_ms=50,
            result_poll_interval_ms=10,
            timer_poll_interval_ms=10,
            pending_idle_threshold_ms=1,
            pending_claim_count=100,
        )
        redis = create_redis_client(self.settings)
        self.store = FluxiRedisStore(redis, self.settings)
        await self._wait_until_redis_ready()
        await self.store.flushdb()

        app = create_app(self.settings, store=self.store)
        app.state.store = self.store
        app.state.settings = self.settings
        transport = httpx.ASGITransport(app=app)
        self.client = httpx.AsyncClient(transport=transport, base_url="http://testserver")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        try:
            await self.store.flushdb()
        except Exception:
            pass
        await self.store.aclose()
