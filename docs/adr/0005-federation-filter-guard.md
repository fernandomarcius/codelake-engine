# ADR 0005 — Guard federated scans against lost filters

- Status: Accepted
- Date: 2026-09-23

## Context

The first MySQL × PostgreSQL integration test returned a row that the `WHERE`
clause excluded. Root cause: `datafusion-federation` 0.5.3's `VirtualExecutionPlan`
reports `PushedDown::Yes` for every filter DataFusion 53's physical
`FilterPushdown` offers, the `FilterExec` is removed, and the
`datafusion-table-providers` SQL executors ignore the filters they receive.

## Decision

Insert `FederationFilterGuard` first in the physical optimizer: it wraps every
`VirtualExecutionPlan` (matched by type) in a pass-through node that refuses parent
filters. The filter runs in the engine. Logical pushdown into the remote SQL is
unaffected.

## Consequences

Filters that could not be expressed in the logical plan run locally instead of at
the source, which may move more rows. Correctness wins. The same pass-through
barrier idea guards the subquery side of null-aware anti joins on DataFusion 54
(ADR 0014). Remove the guard only when
an upstream fix ships and the regression test passes without it; the upstream
report is pending (needs maintainer approval to file).
