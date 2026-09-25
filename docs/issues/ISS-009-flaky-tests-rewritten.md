# ISS-009 — Two unstable tests fixed to match their ADRs

- Kind: test change · Accepted by: Fernando Farias, 2026-09-25 · Status: open (to implement)
- `hive::optimize_tests::concurrent_optimize_and_reads_never_duplicate_rows` failed ~3 in 20 runs with a missing-file error that ADR 0009 allows ("loud, not wrong"). Accepted: the test accepts only that error and still requires zero duplicated rows.
- `f3_acceptance::queue_limit_rejects_with_query_queue_full` takes 60–116 s and times out under coverage. Accepted: rework it with controllably blocking queries so it runs in seconds; the timeout is not raised.
- Risk: none if the rewrite keeps the assertions; the reviewer must check no assertion is loosened.
- References: ADR 0009, ADR 0012.
