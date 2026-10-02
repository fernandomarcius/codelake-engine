# ISS-010 — Ten red-team/semantics decisions applied by recommendation

- Kind: behavior · Applied: 2026-09-24 · Status: closed (confirmed by Fernando Farias, 2026-09-25; closed 2026-10-02: record only, nothing left to do)
1. Test SQL hashes text as `lower(to_hex(md5(to_utf8(...))))`, valid on the reference engine.
2. A test divides with `1.0E0/3` (double) to match the reference engine.
3. `to_timestamp_millis` replaced by reference-engine functions in test SQL.
4. CTAS goes through the function analyzer; `arrow_cast` keeps engine literal reading.
5. Re-seeding the sink checkpoint is out of F9.
6. `char(n)` kept as implemented (padding semantics of the reference engine).
7. OPTIMIZE preserves the original timestamp precision.
8. Table and schema names containing `/` are refused.
9. `CREATE OR REPLACE` on hive tables is refused, as on the reference engine. Overturned 2026-09-25 (ISS-017): replaced atomically since 2026-09-26.
10. Differential red-team tests that need users wait for F5 (auth).
- Implemented: 6 in 2ccc9a7 (char comparisons refused wherever they are, W1-4); 7 in 91b7ee4 (W1-3/W1-7); 8 in 0f364c7; 9 in 87072b8 (W1-1). Decisions the W1 corrections added by recommendation: ISS-014.
- Revisit when: confirmed or overturned by a human.
