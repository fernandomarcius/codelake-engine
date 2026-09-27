# ISS-049 — dbt_pilot numbers CDC rows with ties: output differs between runs

- Kind: use-case defect (reference dbt project, not the engine) · Recorded: 2026-09-26 · Status: open
- Context: the `pilot` models compute `ROW_NUMBER() OVER (PARTITION BY "__$start_lsn" ORDER BY "__$seqval")`, then `dataregistro = lsn_time + (n - 1) * 5 ms` and keep `__$operation <> 3`. An UPDATE writes two change rows (operations 3 and 4) with the same `__$seqval`, so the order is a tie and any engine may number either row first.
- Evidence: shadow run cycle 4 (`out-4`): `pilot`/`streaming` `recebidas`, `contasreceber`, `depositos`, `itensvenda`, `vendaslogparc` differ in 4733, 1018, 2, 2 and 58 rows per side; every differing row belongs to a tied `(__$start_lsn, __$seqval)` group (DuckDB check over both warehouses). Row counts and per-column checksums otherwise match.
- Recommendation: order by `"__$seqval", "__$operation"` (or another unique key) in the reference project; the reference engine can also change the result from one run to the next. For the shadow run, the project copy gets the same tie-breaker on both sides.
- Revisit when: the reference project fixes the ordering.
- References: ISS-042..046 (shadow run of 2026-09-26).
