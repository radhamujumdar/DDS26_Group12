from benchmark.config import Backend, BenchmarkConfig


def create_backend(name: str, config: BenchmarkConfig) -> Backend:
    if name == "docker-compose":
        from benchmark.backends.docker_compose import DockerComposeBackend

        return DockerComposeBackend(config.paths, config.startup_timeout)
    if name == "minikube":
        from benchmark.backends.minikube import MinikubeBackend

        return MinikubeBackend(config.paths, config.startup_timeout)
    raise ValueError(f"Unsupported backend: {name}")
