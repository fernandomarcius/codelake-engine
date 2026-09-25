# ISS-006 — Text compared with a number refused on both client protocols

- Kind: breaking change · Accepted by: Fernando Farias, 2026-09-25 · Status: open (PostgreSQL wire part pending)
- Context: DataFusion 54 coerces text to number in comparisons (`varchar_col > 9`), silently changing answers from DataFusion 53. The reference engine and PostgreSQL both reject the comparison.
- Accepted: refuse text/number comparisons and `time ± interval` on the HTTP query protocol (done, ADR 0014) and on the PostgreSQL wire (to do), marked breaking in the CHANGELOG.
- Risk: clients that relied on the implicit coercion now get an error and must cast explicitly.
- Revisit when: a client needs the coercion — it would have to be opt-in per session.
- References: ADR 0014, `operand_checks.rs`, commit 9ef792e.
