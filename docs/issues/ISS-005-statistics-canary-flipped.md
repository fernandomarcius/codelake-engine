# ISS-005 — Statistics canary test flipped after DataFusion 54 fixed the defect

- Kind: test change · Accepted by: Fernando Farias, 2026-09-24 · Status: open
- Context: `statistics_guard::upstream_answers_from_an_estimate_promoted_to_exact` existed to fail once upstream stopped promoting estimates to exact counts; DataFusion 54 did.
- Accepted: the test became `upstream_keeps_estimates_inexact` and asserts the fixed behavior; the guard itself stays.
- Risk: none on results; the guard is now redundant code.
- Revisit when: the hive statistics defect tests pass without the guard — then remove it.
- References: `crates/codelake-core/src/statistics_guard.rs`, front/df54.
