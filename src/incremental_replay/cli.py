from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile

from .runner import replay
from .scenario import ReplayError, load_scenario


def _json_ready(value):
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            return [[_json_ready(key), _json_ready(item)] for key, item in value.items()]
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def _json(report: dict) -> str:
    return json.dumps(_json_ready(report), indent=2, default=str, allow_nan=False)


def _atomic_report(path: Path, report: str) -> None:
    with NamedTemporaryFile(
        dir=path.parent,
        prefix=".replay-",
        suffix=".tmp",
        delete=False,
        mode="w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        temp = Path(handle.name)
        handle.write(report + "\n")
    try:
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def _text(report: dict) -> None:
    for step in report["steps"]:
        status = {"pass": "PASS", "drift": "FAIL", "unchecked": "SKIP"}[step["status"]]
        print(f"{status:4} step {step['step']:02}  {step['name']}")
    first = report["first_divergence"]
    if first is None:
        checked = sum(step["status"] != "unchecked" for step in report["steps"])
        print(f"{report['model']} agreed with clean rebuilds at all {checked} checked steps.")
        return
    detail = report["steps"][first["step"] - 1]
    print(f"First checked divergence: step {first['step']:02} ({first['name']})")
    if "schema" in detail:
        print("  Schema mismatch:", _json(detail["schema"]))
        return
    print(
        f"  {detail['changed_keys']} changed keys, {detail['additional_rows']} additional rows, "
        f"{detail['missing_rows']} missing rows"
    )
    for side, findings in detail["key_issues"].items():
        if findings["duplicate_groups"] or findings["null_rows"]:
            print(
                f"  {side}: {findings['duplicate_groups']} duplicate key groups, "
                f"{findings['null_rows']} null-key rows"
            )
    for change in detail["differences"]:
        print("  " + ", ".join(f"{key}={value}" for key, value in change["key"].items()))
        for side in ("incremental", "rebuilt"):
            rows = json.dumps(_json_ready(change[side]), default=str, allow_nan=False)
            print(f"    {side:11} {rows}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dbt-replay", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="replay one model and compare its checkpoints")
    check.add_argument("scenario", type=Path)
    check.add_argument("--report", type=Path, help="write the JSON report to this path")
    check.add_argument("--format", choices=("text", "json"), default="text")
    check.add_argument("--max-examples", type=int, default=10)
    args = parser.parse_args(argv)
    if not 1 <= args.max_examples <= 100:
        parser.error("--max-examples must be between 1 and 100")

    try:
        scenario = load_scenario(args.scenario)
        if args.report:
            report_path = args.report.resolve()
            inputs = {
                args.scenario.resolve(),
                *(path for step in scenario.steps for path in step.snapshots.values()),
            }
            if report_path in inputs or report_path.is_relative_to(scenario.project):
                raise ReplayError("Write the report outside the dbt project and scenario inputs")
            if report_path.suffix.lower() != ".json":
                raise ReplayError("--report requires a .json output path")
        report = replay(scenario, args.max_examples)
        if args.report:
            _atomic_report(report_path, _json(report))
    except (ReplayError, OSError, ValueError) as exc:
        if args.format == "json":
            print(_json({"report_version": 1, "status": "error", "message": str(exc)}))
        else:
            print(f"Error: {exc}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(_json(report))
    else:
        _text(report)
        if args.report:
            print(f"Report: {args.report}")
    return int(report["status"] == "drift")


if __name__ == "__main__":
    raise SystemExit(main())
