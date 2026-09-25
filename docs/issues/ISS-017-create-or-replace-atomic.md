# ISS-017 — CREATE OR REPLACE on hive tables, atomic

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: Refused because the reference engine's hive connector refuses it (ADR 0007); a naive replace on a file warehouse is not atomic (red-team W1-1 deleted a foreign directory).
- Accepted: Remove the refusal; write the new table to a new directory and swap it in the metastore in one step; no compatibility mode.
- Risk: A reader or crash mid-swap must see the old or the new table, never a mix or nothing.
- Revisit when: Done when implemented with crash and concurrent-reader tests.
- References: ADR 0007; front: removal of copied limitations.
