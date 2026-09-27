# ISS-053 — Client authentication comes from an external identity service

- Kind: scope decision · Accepted by: Fernando Farias, 2026-09-27 · Status: open
- Context: ROADMAP M1 item 5 (F5) planned client authentication in CodeLake: SCRAM-SHA-256 on the PostgreSQL wire, password/JWT on the HTTP protocol, table ownership (R11), and removing `allow_unauthenticated_network`.
- Accepted: F5 is stopped; authentication will be provided by the external identity service in the future. CodeLake keeps listening without client authentication, on isolated networks only (`allow_unauthenticated_network` and loopback defaults unchanged).
- Risk: until the integration exists, anyone who reaches a listener can query every catalog; deployments must keep the listeners on private networks. Table ownership (R11) stays unenforced.
- Revisit when: the identity-service integration is designed (how identities reach CodeLake: a gateway in front, or tokens CodeLake validates).
- References: ROADMAP M1 item 5, ISS-041.
