"""Compare two checkpoint policies over the same synthetic dbt replay inputs."""

from __future__ import annotations

import argparse
import csv
import json
import platform
import random
import subprocess
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter

import duckdb
import yaml

from incremental_replay.compare import relation
from incremental_replay.runner import replay
from incremental_replay.scenario import load_scenario

PROJECT = Path(__file__).resolve().parents[1] / "examples/orders"
COLUMNS = {
    "order_id": "BIGINT",
    "order_date": "DATE",
    "updated_at": "TIMESTAMP",
    "amount_cents": "BIGINT",
}
MODES = ("fixed", "broken_merge", "strict_watermark", "skip_current_batch", "shared_amount")
POLICIES = ("even", "risk")
HELD_OUT = (271, 379, 487)
REPLAY_BUILDS = 8
REBUILDS = 3
TIMEOUT = 120


def fixtures(root: Path, seed: int) -> tuple[list[dict], list[str]]:
    rng = random.Random(seed)
    root.mkdir()
    retry_at = rng.choice((4, 7))
    candidates = [step for step in (2, 3, 5, 6, 7) if step != retry_at]
    late_at, quiet_at, transient_at = rng.sample(candidates, 3)
    first_day = datetime(2026, 3, 11)
    prior_watermark = datetime(2026, 3, 10, 12)
    rows = {
        42: (42, "2026-03-01", "2026-03-01 12:00:00", rng.randrange(9000, 11000)),
        43: (43, "2026-03-10", str(prior_watermark), 2000),
    }
    steps = []
    risk = []
    for step in range(1, REPLAY_BUILDS + 1):
        name = f"batch-{step}"
        if step == retry_at:
            steps.append({"name": name, "retry": True})
            risk.append(name)
            continue

        clock = first_day + timedelta(days=step - 1)
        if step not in {1, quiet_at}:
            if step == late_at:
                # An old business-date correction ties the most recently seen update time.
                rows[42] = (42, rows[42][1], str(prior_watermark), rows[42][3] - 1275)
                risk.append(name)
            else:
                updated = clock if step == transient_at else clock - timedelta(hours=1)
                if step == 2 and step != transient_at:
                    # The newly visible event's update time precedes the prior high-water mark.
                    updated = prior_watermark - timedelta(hours=2)
                identifier = 100 + step
                rows[identifier] = (
                    identifier,
                    clock.date().isoformat(),
                    str(updated),
                    rng.randrange(100, 4000),
                )
                prior_watermark = max(prior_watermark, updated)

        snapshot = root / f"{name}.csv"
        with snapshot.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(COLUMNS)
            writer.writerows(rows.values())
        steps.append(
            {
                "name": name,
                "snapshots": {"raw.orders": snapshot.name},
                "vars": {"as_of": str(clock)},
            }
        )
    return steps, risk


def checkpoint_names(steps: list[dict], risk: list[str], policy: str) -> list[str]:
    regular = [steps[index - 1]["name"] for index in (3, 6)]
    final = steps[-1]["name"]
    candidates = [final] + (list(reversed(risk)) + regular if policy == "risk" else regular)
    selected = {final}
    for name in candidates[1:]:
        selected.add(name)
        if len(selected) == REBUILDS:
            break
    return [step["name"] for step in steps if step["name"] in selected]


def _oracle(path: Path, clock: str) -> Counter:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = {
            int(row["order_id"]): (
                int(row["order_id"]),
                row["order_date"],
                row["updated_at"],
                int(row["amount_cents"]),
            )
            for row in csv.DictReader(handle)
            if row["updated_at"] <= clock
        }
    return Counter(rows.values())


def _matches(database: Path, model: tuple[str, str], truth: Counter) -> bool:
    with duckdb.connect(str(database), read_only=True, config={"threads": 1}) as connection:
        rows = connection.execute(
            f"SELECT order_id, CAST(order_date AS VARCHAR), CAST(updated_at AS VARCHAR), "
            f"amount_cents FROM {relation(*model)}"
        ).fetchall()
    return Counter(rows) == truth


def evaluate(path: Path) -> dict:
    scenario = load_scenario(path)
    drift_steps = []
    bad_references = []

    def observe(index, incremental, model, reference):
        step = scenario.steps[index - 1]
        expected = _oracle(step.snapshots["raw.orders"], step.variables["as_of"])
        if not _matches(incremental.db, model, expected):
            drift_steps.append(index)
        if reference and not _matches(reference.db, model, expected):
            bad_references.append(index)

    start = perf_counter()
    report = replay(scenario, observe=observe)
    elapsed = perf_counter() - start
    checked = [step["step"] for step in report["steps"] if step["status"] != "unchecked"]
    detections = [step["step"] for step in report["steps"] if step["status"] == "drift"]
    assert len(report["steps"]) == REPLAY_BUILDS and len(checked) == REBUILDS
    return {
        "seconds": round(elapsed, 3),
        "replay_builds": REPLAY_BUILDS,
        "reference_builds": len(checked),
        "checked_steps": checked,
        "truth_drift_steps": drift_steps,
        "detected_steps": detections,
        "reference_disagrees_with_oracle": bad_references,
        "first_detection_delay": detections[0] - drift_steps[0]
        if detections and drift_steps
        else None,
    }


def run_pair(root: Path, seed: int, mode: str, first: str) -> list[dict]:
    case = root / f"{seed}-{mode}"
    steps, risk = fixtures(case, seed)
    results = []
    order = (first, next(policy for policy in POLICIES if policy != first))
    for policy in order:
        checks = checkpoint_names(steps, risk, policy)
        assert len(checks) == REBUILDS
        path = case / f"{policy}.yml"
        path.write_text(
            yaml.safe_dump(
                {
                    "project": PROJECT.as_posix(),
                    "model": "fct_orders",
                    "key": ["order_id"],
                    "sources": {"raw.orders": COLUMNS},
                    "vars": {"mode": mode},
                    "steps": steps,
                    "checkpoints": checks,
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        result = subprocess.run(
            [sys.executable, __file__, "--worker", str(path)],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=TIMEOUT,
        )
        row = json.loads(result.stdout) | {"seed": seed, "mode": mode, "policy": policy}
        results.append(row)
    if results[0]["truth_drift_steps"] != results[1]["truth_drift_steps"]:
        raise RuntimeError(f"Checkpoint policy changed the replay trajectory for {seed} {mode}")
    return results


def summarize(rows: list[dict]) -> dict:
    by_policy = {}
    for policy in POLICIES:
        selected = [row for row in rows if row["policy"] == policy]
        eligible = [r for r in selected if r["truth_drift_steps"] and r["mode"] != "shared_amount"]
        detected = [r for r in eligible if r["detected_steps"]]
        family = {}
        for mode in MODES[1:-1]:
            panel = [r for r in eligible if r["mode"] == mode]
            family[mode] = {
                "cases": len(panel),
                "caught": sum(bool(r["detected_steps"]) for r in panel),
            }
        delays = [r["first_detection_delay"] for r in detected]
        clean = [r for r in selected if r["mode"] == "fixed"]
        by_policy[policy] = {
            "eligible_fault_cases": len(eligible),
            "caught": len(detected),
            "detection_rate": len(detected) / len(eligible) if eligible else None,
            "by_fault": family,
            "median_delay_batches": median(delays) if delays else None,
            "clean_cases": len(clean),
            "clean_false_alarms": sum(bool(r["detected_steps"]) for r in clean),
            "median_seconds": round(median(r["seconds"] for r in selected), 3),
        }
    return by_policy


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results.json"))
    parser.add_argument("--jobs", type=int, choices=range(1, 5), default=2)
    parser.add_argument("--seed", type=int, action="append", help="override held-out seeds")
    parser.add_argument("--mode", choices=MODES, action="append", help="limit to selected modes")
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(evaluate(args.worker)))
        return
    seeds = tuple(dict.fromkeys(args.seed or HELD_OUT))
    modes = tuple(dict.fromkeys(args.mode or MODES))
    rows = []
    with (
        TemporaryDirectory(prefix="dbt-replay-benchmark-") as temp,
        ThreadPoolExecutor(max_workers=args.jobs) as executor,
    ):
        tasks = [
            executor.submit(run_pair, Path(temp), seed, mode, POLICIES[(i + j) % 2])
            for i, seed in enumerate(seeds)
            for j, mode in enumerate(modes)
        ]
        for task in tasks:
            try:
                completed = task.result()
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                for pending in tasks:
                    pending.cancel()
                raise SystemExit(exc.stderr or exc.stdout or str(exc)) from exc
            rows.extend(completed)
            print(f"Measured {completed[0]['seed']} {completed[0]['mode']}", flush=True)

    payload = {
        "schema_version": 1,
        "recorded_at": datetime.now(UTC).isoformat(),
        "seeds": seeds,
        "modes": modes,
        "jobs": args.jobs,
        "timeout_seconds": TIMEOUT,
        "versions": {name: version(name) for name in ("dbt-core", "dbt-duckdb", "duckdb")}
        | {"python": platform.python_version(), "os": platform.system()},
        "summary": summarize(rows),
        "runs": rows,
    }
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))


if __name__ == "__main__":
    main()
