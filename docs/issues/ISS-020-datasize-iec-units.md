# ISS-020 — Data sizes accept MiB/GiB

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: Sizes accept only `512MB`-style units (the reference engine's airlift DataSize).
- Accepted: Also accept `MiB`, `GiB`, …; `MB` keeps its binary meaning so existing settings do not change.
- Risk: None if `MB` is unchanged.
- Revisit when: Done when implemented.
- References: ADR 0009.
