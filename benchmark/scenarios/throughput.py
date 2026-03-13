from benchmark.config import ScenarioSpec

SCENARIO = ScenarioSpec(
    name="throughput",
    kill_schedule=None,
    extra_stabilization_seconds=0,
    locust_defaults={},
    k8s_targets={
        "gateway": 1,
        "order-db": 1,
        "stock-db": 1,
        "payment-db": 1,
        "saga-broker": 1,
        "order-deployment": 1,
        "stock-deployment": 1,
        "payment-deployment": 1,
        "order-db-replica": 0,
        "stock-db-replica": 0,
        "payment-db-replica": 0,
        "saga-broker-replica": 0,
        "sentinel": 0,
    },
    compose_profile="throughput",
)
