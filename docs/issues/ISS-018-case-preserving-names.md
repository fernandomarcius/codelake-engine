# ISS-018 — Case-preserving names, case-insensitive resolution

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: Hive names are folded to lower case because the reference engine does.
- Accepted: Preserve the case a name was written with, resolve names without regard to case, and refuse two names that differ only by case; no compatibility mode.
- Risk: Readers with case-insensitive collations (SQL Server OPENROWSET) would otherwise read the wrong column.
- Revisit when: Done when implemented with tests over hive and OPENROWSET readers.
- References: ADR 0007.
