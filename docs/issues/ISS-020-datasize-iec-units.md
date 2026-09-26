# ISS-020 — Data sizes accept MiB/GiB

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: closed (2026-09-26)
- Context: Sizes accept only `512MB`-style units (the reference engine's airlift DataSize).
- Accepted: Also accept `MiB`, `GiB`, …; `MB` keeps its binary meaning so existing settings do not change.
- Risk: None if `MB` is unchanged.
- Revisit when: Done when implemented. Done 2026-09-26: `parse_data_size` accepts `KiB MiB GiB TiB PiB` with the meaning of `kB MB GB TB PB` (powers of 1024), in session properties and procedure arguments; errors list the accepted units. The server configuration already accepted `KiB…TiB`; there `KB MB GB TB` stay powers of 1000 (unchanged, so existing configurations keep their limits) and the error says so.
- References: ADR 0009.
