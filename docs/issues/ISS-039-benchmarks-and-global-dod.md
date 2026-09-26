# ISS-039 — Benchmarks and global Definition of Done pending

- Kind: pending work · Recorded: 2026-09-26 · Status: open
- Owner's decision (2026-09-25): implement everything first, benchmark at the end.
- SF10 (DataFusion 54): p50 0.38×, memory 0.05×, CPU 0.30× of the reference engine, 33/33 same rows; micro-batch 1.33× slower (not investigated); bench-gate BLOCK on sub-millisecond metadata cases (harness noise, ISS-015).
- SF100 stopped by the owner: reference engine 17/33 measured with memory parity (Q4 heap OOM, Q9 over 4.8 GB, host load during cases 1–16); engine not measured; partial data in `scratchpad/sf100-parcial/`.
- Not run: ~100 GB test, mutants (core ≥ 80 %), coverage on the final main, bench-gate without BLOCK.
