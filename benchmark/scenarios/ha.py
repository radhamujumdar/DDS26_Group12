from benchmark.config import DEFAULT_KILL_SCHEDULE, ScenarioSpec

SCENARIO = ScenarioSpec(
    name="ha",
    kill_schedule=DEFAULT_KILL_SCHEDULE,
    extra_stabilization_seconds=30,
    locust_defaults={},
)
