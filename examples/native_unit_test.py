"""Run dbt's native unit test against the late-correction fixture."""

from dataclasses import replace
from pathlib import Path

from incremental_replay.runner import native_unit_demo
from incremental_replay.scenario import load_scenario

if __name__ == "__main__":
    path = Path(__file__).parent / "late-correction/scenario.yml"
    scenario = load_scenario(path)
    # Use the second step's clock for the native test's late-arriving fixture.
    step = replace(scenario.steps[0], variables=scenario.steps[1].variables)
    print(native_unit_demo(replace(scenario, steps=(step,))), end="")
