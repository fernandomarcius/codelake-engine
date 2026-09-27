# ISS-041 — Human gates pending

- Kind: human decision · Recorded: 2026-09-26 · Status: open
- F8 shadow run in homologation (ISS-016 authorized the source network): needs the isolated shadow destination (database/schema, warehouse prefix) and who runs it.
- F10 production DoD (14 days without wrong results, SLO ≤ 5 min): deployment in HML/PRD.
- Upstream issues/PRs drafted in `docs/upstream/` (DataFusion 54 defects, federation lost filter): opening them needs approval (ADR 0005).
- Confirmations pending: ISS-018, ISS-029, ISS-030, ISS-031, ISS-052 and the choices recorded as awaiting-confirmation in the open branches.
- ISS-013 (reference engine credentials in git): rotation.
