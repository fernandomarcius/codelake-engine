# ISS-024 — char(n) semantics instead of refusals

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: Char values are refused in many comparisons (ISS-014 items 3 and 5) because the engine did not keep them padded.
- Accepted: Implement padded char(n) semantics and coercion as the reference engine does, and drop refusals the reference engine does not have.
- Risk: Until done, some queries over char columns are refused.
- Revisit when: In progress on front/w1-df54 after this merge.
- References: ISS-014, ISS-025.
