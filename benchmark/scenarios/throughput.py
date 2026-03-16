from benchmark.config import ScenarioSpec

SCENARIO = ScenarioSpec(
    name="throughput",
    kill_schedule=None,
    extra_stabilization_seconds=0,
    locust_defaults={},
)
