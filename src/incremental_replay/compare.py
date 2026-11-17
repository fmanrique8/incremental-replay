from __future__ import annotations

from pathlib import Path

import duckdb

from .scenario import ReplayError


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def relation(*parts: str) -> str:
    return ".".join(ident(part) for part in parts)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def schema(connection: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    rows = connection.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()
    return {name: datatype for name, datatype, *_ in rows}


def _key_issues(
    connection: duckdb.DuckDBPyConnection, table: str, keys: list[str], limit: int
) -> dict:
    key_sql = ", ".join(keys)
    null_sql = " OR ".join(f"{key} IS NULL" for key in keys)
    null_rows = connection.execute(f"SELECT count(*) FROM {table} WHERE {null_sql}").fetchone()[0]
    non_unique = f"SELECT {key_sql}, count(*) FROM {table} GROUP BY {key_sql} HAVING count(*) > 1"
    duplicate_groups = connection.execute(f"SELECT count(*) FROM ({non_unique})").fetchone()[0]
    examples = connection.execute(
        f"SELECT * FROM ({non_unique}) ORDER BY {key_sql} LIMIT ?", [limit]
    ).fetchall()
    return {
        "null_rows": null_rows,
        "duplicate_groups": duplicate_groups,
        "duplicate_examples": [{"key": row[:-1], "rows": row[-1]} for row in examples],
    }


def _compare_rows(
    connection, a: str, b: str, columns: list[str], key_names: list[str], limit: int
) -> dict:
    column_sql = ", ".join(map(ident, columns))
    keys = list(map(ident, key_names))
    key_sql = ", ".join(keys)
    counts = {}
    for target, left, right in [("additional", a, b), ("missing", b, a)]:
        connection.execute(
            f"CREATE TEMP TABLE {target} AS "
            f"SELECT {column_sql} FROM {left} EXCEPT ALL SELECT {column_sql} FROM {right}"
        )
        counts[f"{target}_rows"] = connection.execute(f"SELECT count(*) FROM {target}").fetchone()[
            0
        ]
    changed = f"SELECT {key_sql} FROM additional UNION SELECT {key_sql} FROM missing"
    different_keys = connection.execute(f"SELECT count(*) FROM ({changed})").fetchone()[0]
    examples = connection.execute(
        f"SELECT * FROM ({changed}) ORDER BY {key_sql} LIMIT ?", [limit]
    ).fetchall()
    key_filter = " AND ".join(f"{k} IS NOT DISTINCT FROM ?" for k in keys)
    changes = []
    for values in examples:
        change = {"key": dict(zip(key_names, values, strict=True))}
        for side, table in (("incremental", a), ("rebuilt", b)):
            rows = connection.execute(
                f"SELECT {column_sql} FROM {table} WHERE {key_filter} ORDER BY ALL LIMIT 5",
                list(values),
            ).fetchall()
            change[side] = [dict(zip(columns, row, strict=True)) for row in rows]
        changes.append(change)
    return counts | {"changed_keys": different_keys, "differences": changes}


def compare(
    incremental_db: Path,
    incremental_table: tuple[str, str],
    rebuild_db: Path,
    rebuild_table: tuple[str, str],
    key: tuple[str, ...],
    limit: int,
) -> dict:
    a = relation("incremental_output", *incremental_table)
    b = relation("clean_output", *rebuild_table)
    with duckdb.connect(config={"threads": 1}) as connection:
        connection.execute(
            f"ATTACH {_literal(str(incremental_db))} AS incremental_output (READ_ONLY)"
        )
        connection.execute(f"ATTACH {_literal(str(rebuild_db))} AS clean_output (READ_ONLY)")
        actual_schema = schema(connection, a)
        expected_schema = schema(connection, b)
        report = {
            "incremental_rows": connection.execute(f"SELECT count(*) FROM {a}").fetchone()[0],
            "rebuilt_rows": connection.execute(f"SELECT count(*) FROM {b}").fetchone()[0],
        }
        if actual_schema != expected_schema:
            return report | {
                "status": "drift",
                "schema": {
                    "incremental": actual_schema,
                    "rebuilt": expected_schema,
                },
            }

        names = {column.casefold(): column for column in expected_schema}
        if any(k.casefold() not in names for k in key):
            raise ReplayError("The declared key contains columns absent from the result")
        key_names = [names[k.casefold()] for k in key]
        keys = list(map(ident, key_names))
        key_issues = {
            "incremental": _key_issues(connection, a, keys, limit),
            "rebuilt": _key_issues(connection, b, keys, limit),
        }
        differences = _compare_rows(connection, a, b, list(expected_schema), key_names, limit)
        failed = differences["changed_keys"] or any(
            issues["null_rows"] or issues["duplicate_groups"] for issues in key_issues.values()
        )
        return (
            report
            | differences
            | {
                "status": "drift" if failed else "pass",
                "key_issues": key_issues,
            }
        )
