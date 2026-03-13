from benchmark.config import DEFAULT_KILL_SCHEDULE, ScenarioSpec

SCENARIO = ScenarioSpec(
    name="ha",
    kill_schedule=DEFAULT_KILL_SCHEDULE,
    extra_stabilization_seconds=30,
    locust_defaults={},
    k8s_targets={
        "gateway": 1,
        "order-db": 1,
        "stock-db": 1,
        "payment-db": 1,
        "saga-broker": 1,
        "order-db-replica": 1,
        "stock-db-replica": 1,
        "payment-db-replica": 1,
        "saga-broker-replica": 1,
        "sentinel": 3,
        "order-deployment": 5,
        "stock-deployment": 5,
        "payment-deployment": 5,
    },
    compose_profile="ha",
)
