from __future__ import annotations

import csv
import io
import json
import os
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory
from threading import Lock

import duckdb
import yaml
from dbt.cli.main import dbtRunner

from .compare import compare, relation, schema
from .scenario import ReplayError, Scenario, Step

_DBT_LOCK = Lock()


def _frozen_snapshots(scenario: Scenario, root: Path) -> dict[Path, Path]:
    snapshots = {}
    for stage in scenario.steps:
        for path in stage.snapshots.values():
            if path not in snapshots:
                copy = root / f"snapshot-{len(snapshots)}{path.suffix.lower()}"
                copyfile(path, copy)
                snapshots[path] = copy
    return snapshots


class LocalBuild:
    """Run dbt against one scratch database and its independent dbt artifacts."""

    def __init__(self, scenario: Scenario, directory: Path) -> None:
        self.scenario = scenario
        self.directory = directory
        self.directory.mkdir(parents=True)
        self.db = directory / "warehouse.duckdb"
        profile = {
            "target": "local",
            "outputs": {
                "local": {
                    "type": "duckdb",
                    "path": self.db.as_posix(),
                    "schema": "replay",
                    "threads": 1,
                    "keep_open": False,
                }
            },
        }
        (directory / "profiles.yml").write_text(
            yaml.safe_dump({scenario.profile: profile}), encoding="utf-8"
        )

    def load(self, step: Step, frozen: dict[Path, Path]) -> None:
        with duckdb.connect(str(self.db), config={"threads": 1}) as connection:
            for source in self.scenario.sources:
                namespace, name = source.table.split(".")
                original = step.snapshots[source.table]
                path = frozen[original]
                table = relation(namespace, name)
                connection.execute(f"CREATE SCHEMA IF NOT EXISTS {relation(namespace)}")
                if path.suffix == ".csv":
                    with path.open(encoding="utf-8-sig", newline="") as handle:
                        header = next(csv.reader(handle, strict=True), [])
                    if header != list(source.columns):
                        raise ReplayError(
                            f"{original.name} must have the declared CSV header for {source.table}"
                        )
                    connection.execute(
                        f"CREATE OR REPLACE TABLE {table} AS "
                        "SELECT * FROM read_csv(?, columns = ?, header = true, "
                        "auto_detect = false, delim = ',', strict_mode = true)",
                        [str(path), source.columns],
                    )
                else:
                    connection.execute(
                        f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet(?)",
                        [str(path)],
                    )
                    if schema(connection, table) != source.columns:
                        raise ReplayError(
                            f"{original.name} must have the declared Parquet schema for {source.table}"
                        )

    def invoke(self, subcommand: list[str], variables: dict) -> str:
        command = [
            *subcommand,
            "--project-dir",
            str(self.scenario.project),
            "--profiles-dir",
            str(self.directory),
            "--target",
            "local",
            "--target-path",
            str(self.directory / "target"),
            "--log-path",
            str(self.directory / "logs"),
            "--log-level-file",
            "none",
            "--no-send-anonymous-usage-stats",
            "--no-use-colors",
            "--vars",
            json.dumps(variables, allow_nan=False),
        ]
        log = io.StringIO()
        with _DBT_LOCK, redirect_stdout(log), redirect_stderr(log):
            # dbtRunner shares process state. Isolate inherited dbt CLI overrides.
            environment = {
                name: os.environ.pop(name)
                for name in list(os.environ)
                if name.upper().startswith("DBT_")
            }
            try:
                result = dbtRunner().invoke(command)
            finally:
                os.environ.update(environment)
        detail = log.getvalue()
        if not result.success:
            lines = [line for line in detail.splitlines() if line.strip()]
            reason = "\n".join(lines[-14:]) or str(result.exception or "See dbt output")
            raise ReplayError(f"dbt {subcommand[0]} failed:\n{reason}")
        return detail

    def build(self, step: Step, *, clean: bool = False) -> tuple[str, str]:
        command = ["run", "--select", self.scenario.model]
        if clean:
            command.append("--full-refresh")
        self.invoke(command, step.variables)
        target = self.directory / "target"
        manifest = json.loads((target / "manifest.json").read_bytes())
        results = json.loads((target / "run_results.json").read_bytes())["results"]
        if len(results) != 1 or results[0]["status"] != "success":
            raise ReplayError("Expected one successful top-level dbt model build")
        node = manifest["nodes"][results[0]["unique_id"]]
        config = node["config"]
        unique_key = config.get("unique_key")
        keys = [unique_key] if isinstance(unique_key, str) else unique_key
        if (
            node["name"] != self.scenario.model
            or node.get("language") != "sql"
            or node["database"] != self.db.stem
            or node["schema"] != "replay"
            or config["materialized"] != "incremental"
            or config.get("incremental_strategy") != "merge"
            or config.get("full_refresh") is not None
            or not isinstance(keys, list)
            or any(not isinstance(k, str) for k in keys)
            or [k.casefold() for k in keys] != [k.casefold() for k in self.scenario.key]
        ):
            raise ReplayError(
                "Expected one SQL incremental merge model in the replay schema "
                "with the declared unique_key and no full_refresh override"
            )
        dependencies = node["depends_on"]["nodes"]
        dbt_sources = manifest["sources"]
        if any(source not in dbt_sources for source in dependencies):
            raise ReplayError("The selected model may depend directly on sources only")
        source_tables = set()
        for dependency in dependencies:
            source = dbt_sources[dependency]
            if source["database"] != self.db.stem:
                raise ReplayError("Source database must be the local replay database")
            source_tables.add(f"{source['schema']}.{source['identifier']}".casefold())
        if source_tables != {source.table.casefold() for source in self.scenario.sources}:
            raise ReplayError("The declared sources must match the model's source() dependencies")
        return node["schema"], node["alias"]


def replay(
    scenario: Scenario,
    limit: int = 10,
    *,
    observe: Callable[[int, LocalBuild, tuple[str, str], LocalBuild | None], None] | None = None,
) -> dict:
    """Run every step and compare selected checkpoints in sequence."""
    first = None
    steps = []
    with TemporaryDirectory(prefix="dbt-replay-") as tmp:
        root = Path(tmp)
        frozen = _frozen_snapshots(scenario, root)
        incremental = LocalBuild(scenario, root / "incremental")
        for index, step in enumerate(scenario.steps, 1):
            detail = {"step": index, "name": step.name, "retry": step.retry}
            try:
                incremental.load(step, frozen)
                current = incremental.build(step)
                reference = None
                if step.name in scenario.checkpoints:
                    reference = LocalBuild(scenario, root / f"rebuild-{index}")
                    reference.load(step, frozen)
                    rebuilt = reference.build(step, clean=True)
                    detail.update(
                        compare(incremental.db, current, reference.db, rebuilt, scenario.key, limit)
                    )
                    if detail["status"] == "drift" and first is None:
                        first = {"step": index, "name": step.name}
                else:
                    detail["status"] = "unchecked"
                if observe is not None:
                    observe(index, incremental, current, reference)
            except (OSError, UnicodeError, ValueError, csv.Error, duckdb.Error, ReplayError) as exc:
                raise ReplayError(f"Step {index} ({step.name}): {exc}") from exc
            steps.append(detail)
    return {
        "report_version": 1,
        "status": "drift" if first else "pass",
        "model": scenario.model,
        "first_divergence": first,
        "steps": steps,
    }


def native_unit_demo(scenario: Scenario) -> str:
    """Bootstrap the example relation before running native dbt unit tests."""
    with TemporaryDirectory(prefix="dbt-replay-unit-") as tmp:
        root = Path(tmp)
        frozen = _frozen_snapshots(scenario, root)
        target = LocalBuild(scenario, root / "native")
        target.load(scenario.steps[0], frozen)
        target.build(scenario.steps[0])
        return target.invoke(["test", "--select", "test_type:unit"], scenario.steps[0].variables)
