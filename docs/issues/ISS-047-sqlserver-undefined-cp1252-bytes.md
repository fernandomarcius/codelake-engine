# ISS-047 — Bytes undefined in code page 1252 keep their value (U+0081), the reference replaces them with U+FFFD

- Kind: expected difference · Accepted by: Fernando Farias, 2026-09-26 · Status: closed
- Context: `erp1.dbo.PesFis.cargo_pf` (`varchar(90)`) holds byte 0x81 in 17 rows. The candidate reads it as U+0081 (what SQL Server itself gives when converting to `nvarchar`); the reference writes U+FFFD, losing the byte. Lengths, trailing spaces and every other non-ASCII character match.
- Evidence: Cycle 2: `erp_raw_b1.pesfis_b1`, 190,327 rows on both sides, 34 differing row hashes (17 per side), only column `cargo_pf`.
- Recommendation: Keep the lossless value (do not copy the reference's loss) and declare the difference with a rule in the shadow harness's expected differences. Confirmed by the user on 2026-09-26.
- Revisit when: a source whose readers need the reference's replacement. The harness declares it with a `replaced_characters` rule (`characters: c1`) for the exact table and column; values are compared after the same replacement, every other value exactly.
- References: Shadow run on OrbStack, 2026-09-26 (dbt_pilot of reference-dbt-project, selection `+path:models/pilot +path:models/streaming`, 120 models, full refresh; reference Trino 431 with the production catalog settings, candidate `codelake:shadow-a1c2f23`). Reports: `~/Documents/Github.nosync/CodeLake-shadow/out` (cycle 1) and `out-2` (cycle 2, project copy with the workarounds below).
