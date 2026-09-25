# ISS-023 — DROP VIEW never deletes data files

- Kind: deliberate divergence · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: A view created over a directory it does not own deletes that directory's files on `DROP VIEW`; the reference engine 431 does the same.
- Accepted: Diverge: `DROP VIEW` removes only the view's metadata. Data safety over parity.
- Risk: None on data; behavior differs from the reference engine.
- Revisit when: Done when implemented (front: removal of copied limitations).
- References: Red-team W1 report item 5.
