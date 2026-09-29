# ISS-057 — Upstream unparser and decorrelation defects worked around in the engine

- Kind: upstream defect (worked around) · Applied by recommendation, 2026-09-29 · Status: open
- Context: the daily receipts reports, run against the reference engine on the same SQL Server
  catalogs, gave a wrong result, two SQL Server refusals and two internal errors. Each root cause
  below is in DataFusion 54.1 or datafusion-federation 0.5.5; each is worked around in the engine,
  without a fork, and covered by `sqlserver_audit` item 12 and the differential file
  `correlated_subqueries`.

| # | Upstream behavior | Symptom | Workaround |
|---|---|---|---|
| 1 | The unparser renders an `Aggregate` with no projection above it as `SELECT <aggregates>, <groups>`, the reverse of the node's schema (`unparser/plan.rs`, `LogicalPlan::Aggregate`) | a grouping federated alone (below a join kept in the engine) came back as `(count, key)`: **wrong result** | `federation_policy::with_select_list` puts a projection of its columns above it |
| 2 | A `Filter` between a `SubqueryAlias` and its `TableScan` is rendered with the table's name; only a scan's own filters (and a join's sides) are rewritten to the alias | `FROM t AS [o] WHERE [t].[c] = 1`, "could not be bound" | `federation_policy::aliased_scan_filters` makes them filters of the scan |
| 3 | A `Projection` below an `Aggregate` becomes a derived table (`derived_projection`) whose outer references keep the inner table's name | "The multi-part identifier `o.k` could not be bound" | `federation_policy::aggregate_over_derived_columns` keeps such an aggregation in the engine |
| 4 | `LogicalPlan::with_new_exprs(plan.expressions(), inputs)` fails on an `Unnest`, which lists expressions it does not take back; datafusion-federation's `optimize_plan_recursively` rebuilds every node on an ambiguous path this way | `Assertion failed: expr.is_empty()` | the engine's own rebuilds go through `federation_policy::with_new_inputs`; the upstream path is only reached by an unnest over two sources, none known |
| 5 | Decorrelation of an aggregated subquery requires each correlated equality to compare a plain (or cast) inner column (`decorrelate.rs`, `can_pullup_over_aggregation`) | `Physical plan does not support logical expression ScalarSubquery` | `subquery::CorrelatedExpressionKeys` computes the inner expression as a column |

- Cost: (3) sends the rows of the inner grouping instead of one aggregated row; the inner grouping
  still runs at the source.
- Revisit when: DataFusion or datafusion-federation is upgraded; drop each workaround whose
  upstream behavior is fixed (the tests above keep the behavior).
- References: branch `fix/federation-subquery-unnest`.
