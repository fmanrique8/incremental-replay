import json
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from incremental_replay.cli import main
from incremental_replay.runner import LocalBuild, native_unit_demo, replay
from incremental_replay.scenario import ReplayError, load_scenario

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def test_native_unit_test_passes_and_actual_merge_drifts(monkeypatch):
    # A developer's warehouse profile or full-refresh override must not leak into the replay.
    monkeypatch.setenv("DBT_PROFILES_DIR", "unavailable")
    monkeypatch.setenv("DBT_FULL_REFRESH", "true")
    scenario = load_scenario(EXAMPLES / "late-correction/scenario.yml")
    fixture = replace(scenario.steps[0], variables=scenario.steps[1].variables)
    native = replace(scenario, steps=(fixture, *scenario.steps[1:]))
    assert "PASS fct_orders::late_correction_emits_correct_merge_row" in native_unit_demo(native)
    report = replay(scenario)
    assert report["first_divergence"] == {"step": 2, "name": "late correction"}
    failing = report["steps"][1]
    changed = failing["differences"][0]
    assert changed["key"] == {"order_id": 42}
    assert sorted(row["amount_cents"] for row in changed["incremental"]) == [7500, 10000]
    assert [row["amount_cents"] for row in changed["rebuilt"]] == [7500]
    assert report["steps"][2]["retry"] and report["steps"][2]["status"] == "drift"


def test_fixed_model_passes_retry_and_writes_report(tmp_path):
    scenario = EXAMPLES / "late-correction/fixed.yml"
    output = tmp_path / "replay.json"
    assert main(["check", str(scenario), "--report", str(output)]) == 0
    result = json.loads(output.read_bytes())
    assert result["status"] == "pass"
    assert all(step["status"] == "pass" for step in result["steps"])
    assert result["steps"][2]["retry"]


def test_silent_missed_update_has_no_duplicate_key():
    result = replay(load_scenario(EXAMPLES / "missed-update/scenario.yml"))
    assert result["first_divergence"] == {
        "step": 2,
        "name": "correction ties the high-water mark",
    }
    last = result["steps"][-1]
    assert last["changed_keys"] == last["additional_rows"] == last["missing_rows"] == 1
    assert last["key_issues"]["incremental"] == {
        "null_rows": 0,
        "duplicate_groups": 0,
        "duplicate_examples": [],
    }
    [change] = last["differences"]
    assert change["key"] == {"order_id": 42}
    assert [row["amount_cents"] for row in change["incremental"]] == [10000]
    assert [row["amount_cents"] for row in change["rebuilt"]] == [7500]


def test_parquet_and_header_only_csv_share_declared_schema(tmp_path):
    scenario = load_scenario(EXAMPLES / "late-correction/fixed.yml")
    empty = tmp_path / "empty.csv"
    empty.write_text("order_id,order_date,updated_at,amount_cents\n", encoding="utf-8")
    parquet = tmp_path / "orders.parquet"
    with duckdb.connect(config={"threads": 1}) as connection:
        sample = """SELECT 42::BIGINT AS order_id, DATE '2026-03-01' AS order_date,
                    TIMESTAMP '2026-03-01 12:00:00' AS updated_at, 7500::BIGINT AS amount_cents"""
        connection.execute(f"COPY ({sample}) TO ?", [str(parquet)])
    build = LocalBuild(scenario, tmp_path / "build")
    for path, size in [(empty, 0), (parquet, 1)]:
        step = replace(scenario.steps[0], snapshots={"raw.orders": path})
        build.load(step, {path: path})
        with duckdb.connect(str(build.db), read_only=True) as connection:
            assert connection.execute("SELECT count(*) FROM raw.orders").fetchone()[0] == size


def test_invalid_source_headers_fail_instead_of_being_relabelled(tmp_path):
    scenario = load_scenario(EXAMPLES / "late-correction/fixed.yml")
    invalid = tmp_path / "wrong.csv"
    invalid.write_text("amount_cents,order_date,updated_at,order_id\n", encoding="utf-8")
    step = replace(scenario.steps[0], snapshots={"raw.orders": invalid})
    build = LocalBuild(scenario, tmp_path / "build")
    with pytest.raises(ReplayError, match="declared CSV header"):
        build.load(step, {invalid: invalid})


def test_cli_refuses_to_replace_input_and_emits_json_error(capsys):
    scenario = EXAMPLES / "late-correction/fixed.yml"
    code = main(["check", str(scenario), "--format", "json", "--report", str(scenario)])
    assert code == 2
    assert '"status": "error"' in capsys.readouterr().out


def test_two_sources_alias_and_selective_checkpoints(tmp_path):
    project = tmp_path / "project"
    models = project / "models"
    models.mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        'name: example\nversion: "1.0"\nconfig-version: 2\nprofile: example\n',
        encoding="utf-8",
    )
    (models / "sources.yml").write_text(
        "version: 2\nsources:\n  - name: raw\n    tables:\n"
        "      - name: orders\n      - name: customers\n",
        encoding="utf-8",
    )
    (models / "sales.sql").write_text(
        "{{ config(materialized='incremental', incremental_strategy='merge', "
        "unique_key='order_id', alias='enriched_orders') }}\n"
        "select o.order_id, c.segment from {{ source('raw', 'orders') }} o "
        "join {{ source('raw', 'customers') }} c using (customer_id)\n",
        encoding="utf-8",
    )
    (tmp_path / "orders.csv").write_text("order_id,customer_id\n101,42\n", encoding="utf-8")
    (tmp_path / "a.csv").write_text("customer_id,segment\n42,retail\n", encoding="utf-8")
    (tmp_path / "b.csv").write_text("customer_id,segment\n42,wholesale\n", encoding="utf-8")
    config = tmp_path / "case.yml"
    config.write_text(
        "project: project\nmodel: sales\nkey: [order_id]\nsources:\n"
        "  raw.orders: {order_id: BIGINT, customer_id: BIGINT}\n"
        "  raw.customers: {customer_id: BIGINT, segment: VARCHAR}\n"
        "steps:\n  - name: initial\n"
        "    snapshots: {raw.orders: orders.csv, raw.customers: a.csv}\n"
        "  - name: updated\n"
        "    snapshots: {raw.orders: orders.csv, raw.customers: b.csv}\n"
        "checkpoints: [updated]\n",
        encoding="utf-8",
    )
    before = {p.relative_to(project): p.read_bytes() for p in project.rglob("*") if p.is_file()}
    report = replay(load_scenario(config))
    assert [step["status"] for step in report["steps"]] == ["unchecked", "pass"]
    assert report["steps"][-1]["incremental_rows"] == 1
    after = {p.relative_to(project): p.read_bytes() for p in project.rglob("*") if p.is_file()}
    assert after == before
