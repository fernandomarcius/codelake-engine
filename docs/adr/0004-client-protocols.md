# ADR 0004 — Client protocols: PostgreSQL wire now, Trino REST protocol in M1

- Status: Accepted
- Date: 2026-09-23

## Context

Existing clients (dbt-trino, curl/urllib scripts, a .NET API) speak the Trino REST
protocol. BI tools and most drivers speak the PostgreSQL wire protocol.

## Decision

Keep the PostgreSQL wire listener (via `datafusion-postgres`) and add a Trino
protocol endpoint in M1, so existing clients move without code changes.

`datafusion-postgres` 0.17 has no real authentication (its `AuthManager` compares
plaintext and accepts empty passwords). Until our own SCRAM-SHA-256 handler lands,
configuration validation refuses a non-loopback `pg_listen` unless
`allow_unauthenticated_network = true` is set explicitly, and the Kubernetes
manifests restrict ingress with a `NetworkPolicy`.
