# Checkpoint experiment

Compare the two policies on held-out seeds **271, 379, 487**. Freeze the
generator and these seeds before running the panel. Each seed generates one
eight-step snapshot history shared by each policy and each model variant.
The variants are a clean control, a duplicate-inducing merge predicate,
a too-strict watermark, a transient cutoff error, and a transformation error
shared by both the incremental and clean paths.

Each policy executes eight dbt builds and independently rebuilds three
checkpoints. Even spacing checks steps 3, 6, and 8. Risk-based selection checks
the final step and the two most recent tagged late-correction or retry steps.
Both policies get the same 120-second per-run timeout. Their order alternates
across seed and variant. Timings include rebuilding, comparison, and oracle
inspection. Concurrent jobs introduce timing noise.

The independent Python oracle reads visible input rows by key and checks the
expected output at all steps. Clean runs must agree with it and have zero
replay false alarms. Measure detection by fault family, delay from first
oracle mismatch, runtime, and how often the clean reference itself disagrees
with the oracle. Exclude the shared fault from detection-rate denominators and
report it as a negative control for the reference design.

Adopt risk-based selection only if it improves held-out detection by at least
10 percentage points and the gain spans multiple fault families. Explicit
checkpoints remain the product default during the experiment.

Run the full panel from the repository root:

    uv run --locked python benchmarks/checkpoints.py --jobs 3

Use '--seed 271 --mode fixed --output scratch-results.json' for a quick smoke
run. The full panel is intentionally separate from the interview demo.

## Recorded result

The [raw results](results.json) contain 30 runs from 15 paired cases.

| Fault family | Even spacing | Risk-based |
| --- | ---: | ---: |
| Destination predicate creates duplicates | 3/3 | 3/3 |
| Strict watermark misses corrections | 3/3 | 3/3 |
| Cutoff error heals before a later checkpoint | 2/3 | 1/3 |
| Total detected | 8/9 | 7/9 |

Both policies had zero false alarms across their three clean cases. Among
detected cases, median delay was one batch for even spacing and zero batches
for risk-based selection. The two policies detected different transient cases,
so these conditional delay numbers do not establish a speed advantage. Median
wall-clock times were 45.7 and 35.4 seconds under concurrent load.

All six shared-transformation runs passed the replay comparison while the
Python oracle reported incorrect amounts. Both policies saw the same fault
trajectories and used three reference builds per case.

**Decision.** Keep explicitly selected checkpoints and the default of checking
every step. The tested risk heuristic caught fewer faults on this small
synthetic panel.
