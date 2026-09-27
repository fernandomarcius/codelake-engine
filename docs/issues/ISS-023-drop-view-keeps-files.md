# ISS-023 — DROP VIEW never deletes data files

- Kind: deliberate divergence · Accepted by: Fernando Farias, 2026-09-25 · Status: closed (2026-09-26)
- Context: A view created over a directory it does not own deletes that directory's files on `DROP VIEW`; the reference engine 431 does the same.
- Accepted: Diverge: `DROP VIEW` removes only the view's metadata. Data safety over parity.
- Risk: None on data; behavior differs from the reference engine.
- Revisit when: Done when implemented (front: removal of copied limitations). Done 2026-09-26: `DROP VIEW` removes only the view's metadata (and the directory only when it is left empty); `ALTER VIEW … RENAME` moves only the metadata; `CREATE VIEW` over a directory with foreign files does not take them over (tests with foreign files intact byte for byte). ADR 0007.
- References: Red-team W1 report item 5.
