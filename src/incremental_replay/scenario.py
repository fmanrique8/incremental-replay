from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import duckdb
import yaml


class ReplayError(Exception):
    """An invalid scenario or a build that could not be compared."""


@dataclass(frozen=True)
class Source:
    table: str
    columns: dict[str, str]


@dataclass(frozen=True)
class Step:
    name: str
    snapshots: dict[str, Path]
    variables: dict
    retry: bool


@dataclass(frozen=True)
class Scenario:
    project: Path
    profile: str
    model: str
    key: tuple[str, ...]
    sources: tuple[Source, ...]
    steps: tuple[Step, ...]
    checkpoints: frozenset[str]


class ScenarioLoader(yaml.SafeLoader):
    # Preserve scenario clocks as strings, including dates without quotes.
    yaml_implicit_resolvers: ClassVar[dict] = {
        prefix: [rule for rule in rules if rule[0] != "tag:yaml.org,2002:timestamp"]
        for prefix, rules in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }


def read_yaml(path: Path) -> dict:
    try:
        data = yaml.load(path.read_text(encoding="utf-8-sig"), Loader=ScenarioLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ReplayError(f"Cannot read {path}: {exc}") from exc
    return _mapping(data, str(path))


def _mapping(value: object, label: str) -> dict:
    if not isinstance(value, dict) or any(not isinstance(k, str) for k in value):
        raise ReplayError(f"{label} must be a mapping with string keys")
    return value


def _fields(data: dict, allowed: set[str], label: str) -> None:
    unknown = data.keys() - allowed
    if unknown:
        raise ReplayError(f"{label} has unknown fields: {', '.join(sorted(unknown))}")


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
        raise ReplayError(f"{label} must contain only letters, digits or underscores")
    return value


def _path(base: Path, value: object, label: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ReplayError(f"{label} must be a file or directory path")
    return (base / value).resolve()


def _variables(value: object, label: str) -> dict:
    variables = _mapping(value, label)
    for name, item in variables.items():
        if item is not None and type(item) not in {bool, int, float, str}:
            raise ReplayError(f"{label}.{name} must be a scalar value")
        if type(item) is float and not math.isfinite(item):
            raise ReplayError(f"{label}.{name} must be finite")
    return variables


def _sources(declarations: dict) -> tuple[Source, ...]:
    if len(declarations) not in {1, 2}:
        raise ReplayError("Declare one or two input source tables")
    if len({table.casefold() for table in declarations}) != len(declarations):
        raise ReplayError("Source tables must be distinct")
    sources = []
    for table, columns in declarations.items():
        parts = table.split(".")
        if len(parts) != 2:
            raise ReplayError(f"Source {table!r} must be schema.table")
        for part in parts:
            _identifier(part, "source name")
        if parts[0].casefold() in {"replay", "information_schema", "pg_catalog"}:
            raise ReplayError(f"Source {table!r} uses a reserved schema")
        columns = _mapping(columns, f"sources.{table}")
        if not columns:
            raise ReplayError(f"Declare the columns and DuckDB types for {table}")
        if len({c.casefold() for c in columns}) != len(columns):
            raise ReplayError(f"Source {table} contains duplicate column names")
        typed = {}
        for column, datatype in columns.items():
            _identifier(column, "source column")
            if not isinstance(datatype, str):
                raise ReplayError(f"Column {table}.{column} must have a DuckDB type")
            try:
                typed[column] = str(duckdb.sqltype(datatype))
            except duckdb.Error as exc:
                raise ReplayError(f"Invalid type for {table}.{column}: {datatype}") from exc
        sources.append(Source(table, typed))
    return tuple(sources)


def _steps(raw_steps: object, tables: set[str], defaults: dict, root: Path) -> tuple[Step, ...]:
    if not isinstance(raw_steps, list) or not raw_steps:
        raise ReplayError("steps must be a non-empty list")
    steps: list[Step] = []
    names = set()
    for i, raw_step in enumerate(raw_steps, 1):
        label = f"Step {i}"
        item = _mapping(raw_step, label)
        _fields(item, {"name", "snapshots", "vars", "retry"}, label)
        name = item.get("name")
        if not isinstance(name, str) or not name.strip() or name in names:
            raise ReplayError(f"{label} must have a distinct, non-empty name")
        names.add(name)
        retry = item.get("retry", False)
        if type(retry) is not bool:
            raise ReplayError(f"{label} retry must be a boolean")
        if retry:
            if not steps or "snapshots" in item or "vars" in item:
                raise ReplayError(f"{label} retry must reuse a preceding step without overrides")
            previous = steps[-1]
            steps.append(Step(name, previous.snapshots, previous.variables, True))
            continue

        inputs = _mapping(item.get("snapshots"), f"{label} snapshots")
        if inputs.keys() != tables:
            raise ReplayError(f"{label} must supply a complete snapshot for each declared source")
        snapshots = {}
        for table, value in inputs.items():
            snapshot = _path(root, value, f"{label} {table}")
            if snapshot.suffix.lower() not in {".csv", ".parquet"} or not snapshot.is_file():
                raise ReplayError(f"{label} {table} must point to an existing CSV or Parquet file")
            snapshots[table] = snapshot
        variables = defaults | _variables(item.get("vars", {}), f"{label} vars")
        steps.append(Step(name, snapshots, variables, False))
    return tuple(steps)


def load_scenario(path: Path) -> Scenario:
    path = path.resolve()
    data = read_yaml(path)
    _fields(
        data, {"project", "model", "key", "sources", "steps", "vars", "checkpoints"}, "Scenario"
    )
    project = _path(path.parent, data.get("project"), "project")
    profile = read_yaml(project / "dbt_project.yml").get("profile")
    if not isinstance(profile, str) or not profile.strip():
        raise ReplayError("dbt_project.yml must specify a literal profile name")
    model = _identifier(data.get("model"), "model")

    key = data.get("key")
    if not isinstance(key, list) or not key:
        raise ReplayError("key must be a non-empty list of result columns")
    keys = tuple(_identifier(name, "key column") for name in key)
    if len({k.casefold() for k in keys}) != len(keys):
        raise ReplayError("key columns must be distinct")

    declarations = _mapping(data.get("sources"), "sources")
    sources = _sources(declarations)
    steps = _steps(
        data.get("steps"), set(declarations), _variables(data.get("vars", {}), "vars"), path.parent
    )
    names = {step.name for step in steps}
    checks = data.get("checkpoints", list(names))
    if (
        not isinstance(checks, list)
        or not checks
        or any(not isinstance(x, str) or x not in names for x in checks)
        or len(set(checks)) != len(checks)
    ):
        raise ReplayError("checkpoints must list distinct step names from the scenario")
    return Scenario(project, profile, model, keys, sources, steps, frozenset(checks))
