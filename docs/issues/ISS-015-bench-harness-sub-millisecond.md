# ISS-015 — Bench harness measures sub-millisecond cases in bursts

- Kind: measurement · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: The SF10 bench-gate BLOCKed `meta_tables`/`meta_column_types` (+42–48 %) on DataFusion 54; high-repetition A/B tests showed no regression ≥ 10 % (engine delta +12–35 µs). The harness samples the cgroup and `/metrics` in a thread per repetition, which inflates sub-millisecond cases 2–3×.
- Accepted: Measure cases under 1 ms in bursts, without per-repetition sampling; re-measure the DataFusion 53 baseline with the new harness; gate thresholds unchanged. Benchmarks run at the end of the implementation phase.
- Risk: Until then the bench-gate stays BLOCKed on those cases; the global Definition of Done waits for it.
- Revisit when: Done when the harness change lands and the bench-gate passes with unchanged thresholds.
- References: `bench/reports/2026-09-25-*`, `scratchpad/ab-meta` evidence.
