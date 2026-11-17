"""Run native dbt key tests on the stale table after the second build."""

import json
from pathlib import Path

import duckdb

from incremental_replay.compare import relation
from incremental_replay.runner import replay
from incremental_replay.scenario import ReplayError, load_scenario


def main() -> None:
    scenario = load_scenario(Path(__file__).with_name("scenario.yml"))

    def check_persisted_keys(index, build, table, _reference):
        if index != len(scenario.steps):
            return

        build.invoke(
            ["test", "--select", "fct_orders,test_type:generic"],
            scenario.steps[-1].variables,
        )
        artifact = build.directory / "target" / "run_results.json"
        results = json.loads(artifact.read_bytes())["results"]
        observed = {result["unique_id"].split(".")[2]: result["status"] for result in results}
        expected = {
            "not_null_fct_orders_order_id": "pass",
            "unique_fct_orders_order_id": "pass",
        }
        if len(results) != len(expected) or observed != expected:
            raise ReplayError(f"Expected both native dbt key tests to pass, got {observed}")

        with duckdb.connect(str(build.db), read_only=True) as connection:
            [(amount_cents,)] = connection.execute(
                f"SELECT amount_cents FROM {relation(*table)} WHERE order_id = ?", [42]
            ).fetchall()
        for name, status in observed.items():
            print(f"{status.upper()} dbt data test {name}")
        print(f"Batch {index:02} persisted order_id=42 amount_cents={amount_cents}")

    replay(scenario, observe=check_persisted_keys)


if __name__ == "__main__":
    main()
