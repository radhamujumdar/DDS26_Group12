from benchmark.backends.docker_compose import DockerComposeBackend
from benchmark.backends.minikube import MinikubeBackend
from benchmark.config import Backend, BenchmarkConfig


def create_backend(name: str, config: BenchmarkConfig) -> Backend:
    if name == "docker-compose":
        return DockerComposeBackend(config.paths, config.startup_timeout)
    if name == "minikube":
        return MinikubeBackend(config.paths, config.startup_timeout)
    raise ValueError(f"Unsupported backend: {name}")
