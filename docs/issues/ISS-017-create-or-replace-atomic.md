# ISS-017 — CREATE OR REPLACE on hive tables, atomic

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: closed (2026-09-26)
- Context: Refused because the reference engine's hive connector refuses it (ADR 0007); a naive replace on a file warehouse is not atomic (red-team W1-1 deleted a foreign directory).
- Accepted: Remove the refusal; write the new table to a new directory and swap it in the metastore in one step; no compatibility mode.
- Risk: A reader or crash mid-swap must see the old or the new table, never a mix or nothing.
- Revisit when: Done when implemented with crash and concurrent-reader tests. Done 2026-09-26: the staging directory and the table directory are exchanged in one step (`renameat2` `RENAME_EXCHANGE`) under the locks `DROP TABLE` takes; the old table is removed after the swap; views, external tables, foreign directories, object stores and file systems that cannot exchange are refused with nothing changed; statements planned against the replaced table fail (`codelake.table_id`). Tests: crash while writing, crash before and after the swap, readers of two processes racing replacements, concurrent replacements, planned statements (ADR 0007).
- References: ADR 0007; front: removal of copied limitations.
