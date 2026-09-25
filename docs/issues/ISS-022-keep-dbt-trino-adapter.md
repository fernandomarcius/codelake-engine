# ISS-022 — Keep the existing dbt adapter until a native one is validated

- Kind: dependency · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: dbt runs through the reference engine's dbt adapter.
- Accepted: Keep it; a native `dbt-codelake` adapter replaces it only after it passes the shadow run.
- Risk: dbt features are bounded by the existing adapter.
- Revisit when: Revisit with the official clients front.
- References: ROADMAP.
