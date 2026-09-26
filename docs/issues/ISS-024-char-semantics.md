# ISS-024 — char(n) semantics instead of refusals

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: closed
- Context: Char values are refused in many comparisons (ISS-014 items 3 and 5) because the engine did not keep them padded.
- Accepted: Implement padded char(n) semantics and coercion as the reference engine does, and drop refusals the reference engine does not have.
- Done (front/w1-df54, ADR 0015): every char(n) value is held padded; where char values meet other text both are converted to the reference's common char type (comparisons, `IN`/`ANY`/`ALL`, joins, `CASE`, `coalesce`, set operations, `VALUES`, `USING`, `||`); `TRY_CAST` follows `CAST`. The refusals of W1-4 and DEFECT-W1T-2 are gone. Checked against Trino 431 by `char_semantics::tests` (100 queries) and the integration matrix `char_matrix` (1656 row operations, 384 set operations, joins and subqueries over six origins of the char value): every answer is the reference's rows, or both refuse.
- Refusals kept: where the reference fails too (`NATURAL JOIN`, `char || <unbounded varchar>`, `char || NULL`); and where the engine cannot derive the reference's common type — char output with text of an unknown length, char values inside arrays that are not written out (ADR 0015).
- Risk: The rewrites follow the reference type through the plan; a construct they do not know (a function whose result length is not derived) is refused, not guessed. Two answers differ from the reference on purpose, where the reference is wrong (`EXISTS` repeating rows; a scalar subquery in `VALUES`).
- Revisit when: A new function or plan node carries char values (extend the type rules and the matrix).
- References: ISS-014, ISS-025, ADR 0015, ADR 0007.
