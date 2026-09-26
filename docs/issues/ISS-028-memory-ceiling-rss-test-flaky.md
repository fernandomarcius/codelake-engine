# ISS-028 — Memory-ceiling hash-join test measures process RSS with a thin margin

- Kind: test reliability · Accepted by: Fernando Farias, 2026-09-26 (by recommendation) · Status: open
- Context: `memory_ceiling::heavy_hash_join_fails_cleanly_under_the_ceiling` failed once in the full integration run of F6 + F8 (RSS grew 171.8 MiB over its idle size, budget 170.6 MiB, 0.7 % over); it passed 3/3 alone and in the next full run (287/287).
- Accepted: merge with the next full run green; rewrite the test to assert the pool's exact accounting (as done for RT10) instead of whole-process RSS, keeping an RSS bound only as a coarse backstop.
- Risk: until rewritten, the integration gate can fail spuriously, or a real small leak outside the pool can hide in the noise.
- Revisit when: the rewrite lands.
- References: `crates/codelake-server/tests/memory_ceiling.rs:597`, ADR 0003.
