# ISS-010 — Ten red-team/semantics decisions applied by recommendation

- Kind: behavior · Applied: 2026-09-24 · Status: awaiting-confirmation (no explicit human answer; each is reversible)
1. Test SQL hashes text as `lower(to_hex(md5(to_utf8(...))))`, valid on the reference engine.
2. A test divides with `1.0E0/3` (double) to match the reference engine.
3. `to_timestamp_millis` replaced by reference-engine functions in test SQL.
4. CTAS goes through the function analyzer; `arrow_cast` keeps engine literal reading.
5. Re-seeding the sink checkpoint is out of F9.
6. `char(n)` kept as implemented (padding semantics of the reference engine).
7. OPTIMIZE preserves the original timestamp precision.
8. Table and schema names containing `/` are refused.
9. `CREATE OR REPLACE` on hive tables is refused, as on the reference engine.
10. Differential red-team tests that need users wait for F5 (auth).
- Revisit when: confirmed or overturned by a human; 7–9 are implemented in the W1 front.
