# ISS-025 — Known char(n) wrong results merged into main

- Kind: known defect · Accepted by: Fernando Farias, 2026-09-25 · Status: closed
- Context: Independent verification of the W1 front found char values still escaping the refusals with wrong rows (IN/NOT IN/= ANY with char in the subquery; scalar subqueries of char; CASE/COALESCE mixing char and text; `CAST(... AS char(n))` unpadded), plus `TRY_CAST(x AS varchar(n))` not truncating and a panic on `max(ch)` over an in-memory char table. All already existed in main before the merge, several in worse form.
- Accepted: Merge W1, DataFusion 54 and F10 into main now (main becomes strictly better), with these defects registered, and fix them right after (ISS-024).
- Closed by: ISS-024 on front/w1-df54 (ADR 0015). P1–P16 answer the reference's rows (P16 fails in the reference; the engine answers what the same `VALUES` answers without the subquery), `TRY_CAST` follows `CAST`, and in-memory char columns keep their Arrow type (no panic).
- Risk: None known for the listed shapes; see ISS-024 for the refusals kept.
- Revisit when: Closed.
- References: `scratchpad/verif-char-trino.log` P1–P16, `hive_redteam_w1::w1t_char_values_compared_outside_predicates_like_trino_431`.
