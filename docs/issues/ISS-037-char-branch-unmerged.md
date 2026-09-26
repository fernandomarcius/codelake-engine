# ISS-037 — char(n) semantics branch not merged (round 5 failed, round 6 not run)

- Kind: pending work / known defects · Recorded: 2026-09-26 · Status: open
- Branch: `front/w1-df54`, HEAD `4a9ad90`, 31 commits over main: char(n) with the reference engine's padded semantics instead of refusals (ISS-024), TRY_CAST, trims, typeof, varchar(n) on the PostgreSQL wire, multiset set operations.
- Round 5 verdict (independent): fixes correct (14/14 mutants caught) but new wrong rows — arrays/rows of char stored by CTAS/INSERT compare unpadded (typed `array(varchar)`), `CAST(ch AS varchar)` loses trailing spaces on the PostgreSQL wire, set operations / `IS NOT DISTINCT FROM` on hive lose NULL rows (overlaps ISS-033), `-0.0` divergences, and a regression from `c4a9c35`: `WHERE real_col = 0` → internal error (fixed differently in ISS-033's branch).
- Owner's decision (2026-09-26): resolve these, not refuse them.
- Main still carries the ISS-025 char wrong results.
- Next: round 6 (containers of char typed `array(char(n))`, PG CAST), reconcile the `-0.0` work with ISS-033, verification with the differential gate (ISS-035), merge; then ISS-019.
