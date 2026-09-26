# ISS-038 — Roadmap fronts not started

- Kind: pending work · Recorded: 2026-09-26 · Status: open
- F5 authentication (SCRAM-SHA-256, password/JWT, table ownership R11, remove `allow_unauthenticated_network`); an old uncommitted draft exists in `scratchpad/pausa-seq/f5-auth.patch` (base `0b0b0ff`, stale).
- Rename to CodeLake names (ISS-011) — alone, after every open branch is merged.
- ISS-019 (unbounded varchar for CTAS literals) — after ISS-037.
- Arrow Flight SQL (ISS-021), official clients (Python, JDBC, dbt-codelake; ISS-022), web panel (not on the roadmap yet).
- Dedicated CPU runtime (query execution shares Tokio threads with the HTTP protocol).
- F12 Iceberg (blocked: no published integration for DataFusion 54, ISS-004), F13/F14 distributed (needs a cluster for the DoD).
