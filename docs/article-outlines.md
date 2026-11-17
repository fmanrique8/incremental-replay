# Article outlines for fmanrique.com

## The dbt unit test passed. Why did an order get counted twice?

Open with the two amounts for order 42 and their effect on the reporting total.
Walk through the select result, destination predicate, actual merge, and clean
rebuild. Draw a four-panel figure showing the original state, late correction,
incremental output, and reference output. Include the two input snapshots,
the native dbt PASS, the replay JSON, and the repaired output. Explain the
distinction using dbt's incremental unit-testing documentation.

## Three rebuilds, eight batches

Explain how a bad value can disappear before the last checkpoint. Show a
timeline from the recorded runs with checkpoints, oracle differences, and
detections. Plot detection count by fault family at the measured budget of
three rebuilds. Add points at other budgets after running those experiments.
Include the frozen seeds, generator, raw timings, shared-transformation
negative control, and the limits of the small synthetic panel.
