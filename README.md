# Incremental Replay

**Both dbt key tests pass. Order 42 still shows $100.00. A clean rebuild
shows $75.00.**

Incremental Replay runs an actual dbt model across local snapshots and points
to the first batch and key where the persisted table differs from a rebuild.

## Five-minute walkthrough

Start in the repository root, the folder containing `pyproject.toml`. You need
[uv](https://docs.astral.sh/uv/getting-started/installation/) on your PATH.
The demo uses synthetic CSVs and a temporary DuckDB database. It needs no
warehouse or credentials. Run each command separately so you can continue
after the expected failure.

**1. Set up the local environment.** On a first run, uv may download Python
3.12 and the locked dependencies.

~~~sh
uv sync --locked --python 3.12
~~~

If uv reports a hardlink warning, it is copying the packages instead. Continue
once setup finishes.

**2. Check the persisted table.**

~~~sh
uv run --locked python examples/missed-update/check_keys.py
~~~

~~~text
PASS dbt data test not_null_fct_orders_order_id
PASS dbt data test unique_fct_orders_order_id
Batch 02 persisted order_id=42 amount_cents=10000
~~~

**3. Find the discrepancy.** This command is expected to exit **1**.

~~~sh
uv run --locked dbt-replay check examples/missed-update/scenario.yml
~~~

~~~text
PASS step 01  initial orders
FAIL step 02  correction ties the high-water mark
First checked divergence: step 02 (correction ties the high-water mark)
  1 changed keys, 1 additional rows, 1 missing rows
  order_id=42
    incremental [{"order_id": 42, "order_date": "2026-03-01", "updated_at": "2026-03-01 12:00:00", "amount_cents": 10000}]
    rebuilt     [{"order_id": 42, "order_date": "2026-03-01", "updated_at": "2026-03-10 12:00:00", "amount_cents": 7500}]
~~~

**4. Verify the repair.** This runs the same snapshots from an empty
database and exits **0**.

~~~sh
uv run --locked dbt-replay check examples/missed-update/fixed.yml
~~~

~~~text
PASS step 01  initial orders
PASS step 02  correction ties the high-water mark
fct_orders agreed with clean rebuilds at all 2 checked steps.
~~~

### Reading the diff

In batch 2, the one additional row is the stale version of order 42. The one
missing row is the corrected version of the same order. The key tests can
pass because each persisted order has a single, non-null key. The amounts are
integer cents, so the persisted table is $25.00 too high for order 42.

The batch label and the two update timestamps point to the watermark filter.
The correction has the same timestamp as the existing high-water mark. The
strict greater-than filter skips it. The repaired scenario checks for row
changes by key and picks up the correction even when timestamps tie.

## Duplicate order after MERGE

A destination date predicate can cause a different failure. The native dbt
unit test passes because the model emits the correct candidate row. The
actual MERGE inserts that row alongside the original because the predicate
excludes the older target row from the match.

~~~sh
uv run --locked python examples/native_unit_test.py
uv run --locked dbt-replay check examples/late-correction/scenario.yml
uv run --locked dbt-replay check examples/late-correction/fixed.yml
~~~

The replay for the broken scenario exits **1** and reports order 42 twice.
The repaired scenario clears the predicate and exits **0**, including an
identical retry. See the [SQL model](examples/orders/models/fct_orders.sql)
and the [article outlines](docs/article-outlines.md) for interview discussion.

## Your model

Create a scenario beside your snapshots using
[the example](examples/late-correction/scenario.yml) as a template.

| Field | Meaning |
| --- | --- |
| project | Path to a dbt project with a literal profile name. Paths are relative to the scenario. |
| model | Name of one SQL incremental merge model in the generated profile's default schema, named replay. |
| key | Ordered list of output columns matching the model's unique_key. Composite keys are supported. |
| sources | One or two physical schema.table names with exact DuckDB column types. Use source() for those inputs. |
| vars | Default dbt variables. Set a deterministic clock using the variable your model reads. |
| steps | Ordered, uniquely named states. Supply a complete CSV or Parquet snapshot for every source and optional per-step vars. A step with retry: true repeats its predecessor's data and vars. |
| checkpoints | Optional list of step names to compare. Every step executes. The default compares every step. |

CSV headers must match the declared names and order. A header-only CSV makes
an empty table with the declared types. Parquet column names and DuckDB types
must match the declaration. Each snapshot represents the complete visible
source state, including corrections.

Both paths read copies of the same fixture files, share the same dbt variables
and target name, and use separate temporary DuckDB databases. The replay retains
its output table between steps. A reference database starts empty per
checkpoint. The comparator checks column names and types, key uniqueness and
nulls, and exact row multiplicity in both directions using DuckDB EXCEPT ALL.
Output samples are bounded. Counts cover the complete difference. Exit codes
are **0** for agreement, **1** for drift or a key violation, and **2** for an
invalid scenario or execution error. Use --format json for machine-readable
stdout and --report PATH for a JSON file.

This version covers deterministic current-state models with one model build
per step. Warehouse-specific adapters, downstream models, schema overrides,
history-dependent results, and wall-clock SQL are outside this contract. Use
trusted local dbt projects. The profile isolates dbt targets and artifacts, but
project SQL and hooks are executable code. A passing replay means the checked
output agreed with the rebuild on the supplied fixture. A transformation bug
shared by both paths can escape that comparison.

## Tests and benchmark

~~~sh
uv run --locked ruff check .
uv run --locked pytest -q
uv run --locked python benchmarks/checkpoints.py --jobs 3
~~~

Tests cover real merge and retry behavior, equal timestamps, identical
duplicates, null keys, schema changes, empty data, Parquet, and JSON reports.
Read the [equal-budget experiment](benchmarks/README.md) for the method and
recorded results. Benchmarking is separate from the demo.

See the [dbt incremental unit testing guide](https://docs.getdbt.com/docs/build/unit-tests#unit-testing-incremental-models)
and the [DuckDB adapter's merge options](https://github.com/duckdb/dbt-duckdb#incremental-strategy-configuration).
