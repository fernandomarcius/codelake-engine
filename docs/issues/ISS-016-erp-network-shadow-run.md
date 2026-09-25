# ISS-016 — Access to the source network authorized for the F8 shadow run

- Kind: security · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: F8 compares the engine with the reference engine on the real dbt project and the real sources.
- Accepted: Read-only access to the production source network for the shadow run; writes only to an isolated shadow destination.
- Risk: Reads production data; a misconfigured destination could write where it should not.
- Revisit when: Still needed: the isolated shadow destination (database/schema, warehouse prefix) and who runs it (SSH host or the user).
- References: ROADMAP F8.
