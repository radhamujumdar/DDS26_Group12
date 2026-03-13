from benchmark.config import ScenarioSpec
from benchmark.scenarios.ha import SCENARIO as HA_SCENARIO
from benchmark.scenarios.throughput import SCENARIO as THROUGHPUT_SCENARIO

SCENARIOS: dict[str, ScenarioSpec] = {
    THROUGHPUT_SCENARIO.name: THROUGHPUT_SCENARIO,
    HA_SCENARIO.name: HA_SCENARIO,
}


def get_scenario(name: str) -> ScenarioSpec:
    return SCENARIOS[name]
