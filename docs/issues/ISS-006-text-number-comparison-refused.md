# ISS-006 — Text compared with a number refused on both client protocols

- Kind: breaking change · Accepted by: Fernando Farias, 2026-09-25 · Status: closed
- Context: DataFusion 54 coerces text to number in comparisons (`varchar_col > 9`), silently changing answers from DataFusion 53. The reference engine and PostgreSQL both reject the comparison.
- Accepted: refuse text/number comparisons and `time ± interval` on the HTTP query protocol (done, ADR 0014) and on the PostgreSQL wire, marked breaking in the CHANGELOG.
- Done on the PostgreSQL wire (3024056): typed text against a number fails with SQLSTATE `42883` ("operator does not exist: text > integer"), `time ± interval` with `0A000`; a quoted literal stays untyped, as in PostgreSQL (`int_col > '20'` answers). Tests: `operand_checks.rs` unit tests, `tests/e2e/pg_operands.rs` (tokio-postgres, simple and extended protocol).
- Risk: clients that relied on the implicit coercion now get an error and must cast explicitly.
- Revisit when: a client needs the coercion — it would have to be opt-in per session.
- References: ADR 0014, `operand_checks.rs`, `hooks.rs`, commits 9ef792e, 3024056.
