from pathlib import Path

import duckdb
import pytest

from incremental_replay.compare import compare


def database(path: Path, rows, schema="id INTEGER, region VARCHAR, amount INTEGER") -> Path:
    with duckdb.connect(str(path), config={"threads": 1}) as connection:
        connection.execute(f"CREATE TABLE orders ({schema})")
        if rows:
            values = ", ".join("?" for _ in rows[0])
            connection.executemany(f"INSERT INTO orders VALUES ({values})", rows)
    return path


def compare_rows(tmp_path, actual, expected, key=("id", "region"), limit=10):
    table = ("main", "orders")
    return compare(
        database(tmp_path / "actual.duckdb", actual),
        table,
        database(tmp_path / "expected.duckdb", expected),
        table,
        key,
        limit,
    )


def test_identical_duplicate_is_counted_as_an_additional_row(tmp_path):
    row = (42, "east", 7500)
    report = compare_rows(tmp_path, [row, row], [row])
    assert report["status"] == "drift"
    assert report["additional_rows"] == 1
    assert report["missing_rows"] == 0
    assert report["changed_keys"] == 1
    assert report["key_issues"]["incremental"]["duplicate_groups"] == 1
    assert report["differences"][0]["key"] == {"id": 42, "region": "east"}


def test_equal_invalid_key_relations_fail(tmp_path):
    rows = [(None, "east", 1), (2, "west", 2), (2, "west", 2)]
    report = compare_rows(tmp_path, rows, rows)
    assert report["status"] == "drift"
    assert report["changed_keys"] == 0
    assert report["key_issues"]["rebuilt"]["null_rows"] == 1
    assert report["key_issues"]["rebuilt"]["duplicate_groups"] == 1


@pytest.mark.parametrize("rows", [[], [(1, "east", None)]])
def test_clean_empty_or_nullable_measure(tmp_path, rows):
    assert compare_rows(tmp_path, rows, rows)["status"] == "pass"


def test_counts_remain_exact_when_samples_are_limited(tmp_path):
    actual = [(1, "east", 20), (2, "east", 100)]
    expected = [(1, "east", 10), (3, "east", 100)]
    report = compare_rows(tmp_path, actual, expected, limit=1)
    assert report["changed_keys"] == 3
    assert report["additional_rows"] == report["missing_rows"] == 2
    assert len(report["differences"]) == 1


def test_report_schema_drift_before_value_coercion(tmp_path):
    a = database(tmp_path / "actual.duckdb", [(1, "a")], "id INTEGER, amount VARCHAR")
    b = database(tmp_path / "expected.duckdb", [(1, 1)], "id INTEGER, amount INTEGER")
    report = compare(a, ("main", "orders"), b, ("main", "orders"), ("id",), 5)
    assert report["status"] == "drift"
    assert report["schema"]["incremental"]["amount"] == "VARCHAR"
