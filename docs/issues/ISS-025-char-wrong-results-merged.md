# ISS-025 — Known char(n) wrong results merged into main

- Kind: known defect · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: Independent verification of the W1 front found char values still escaping the refusals with wrong rows (IN/NOT IN/= ANY with char in the subquery; scalar subqueries of char; CASE/COALESCE mixing char and text; `CAST(... AS char(n))` unpadded), plus `TRY_CAST(x AS varchar(n))` not truncating and a panic on `max(ch)` over an in-memory char table. All already existed in main before the merge, several in worse form.
- Accepted: Merge W1, DataFusion 54 and F10 into main now (main becomes strictly better), with these defects registered, and fix them right after (ISS-024).
- Risk: Queries over char columns in the listed shapes can return wrong rows until ISS-024 lands.
- Revisit when: Close when ISS-024 lands with the differential matrix against the reference engine.
- References: `scratchpad/verif-char-trino.log` P1–P16.
