from benchmark.config import Backend, BenchmarkConfig


def create_backend(name: str, config: BenchmarkConfig) -> Backend:
    if name == "docker-compose":
        from benchmark.backends.docker_compose import DockerComposeBackend

        return DockerComposeBackend(config.paths, config.startup_timeout, config.sizing)
    if name == "minikube":
        from benchmark.backends.minikube import MinikubeBackend

        return MinikubeBackend(config.paths, config.startup_timeout, config.sizing)
    raise ValueError(f"Unsupported backend: {name}")
