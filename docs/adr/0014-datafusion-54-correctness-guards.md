# ADR 0014 — Correctness guards for the DataFusion 54 train

- Status: Accepted
- Date: 2026-09-24 (defects 13 and 14 added 2026-09-26)
- Related: ADR 0002 (version train), ADR 0003 (memory model), ADR 0005
  (federation filter guard), ADR 0006 (pushdown policy)

## Context

The move to DataFusion 54.1 (ADR 0002) passed the unit suite, but an
independent verification against Trino 431 (the same Parquet files read by
both engines, `crates/codelake-connectors/tests/integration/join_regressions.rs`)
found joins that answer wrong rows, a comparison whose answers changed without
notice, and a merge that could truncate its output in silence. Each defect was
confirmed in the published 54.1.0 source (checksum as in `Cargo.lock`).

| # | Defect | Where (54.1.0) | Upstream | Also in 53? |
|---|---|---|---|---|
| 1 | `x NOT IN (SELECT y …)` (null-aware `LeftAnti` hash join): the join's dynamic filter is pushed into the scan of the subquery and drops its NULL; the query returns rows instead of none | `hash_join/exec.rs` `allow_join_dynamic_filter_pushdown` uses `on_lr_is_preserved()`; 53 allowed only `Inner` | apache/datafusion#23104 (`OR key IS NULL`, on `main`, not in 54.1.0) | no |
| 2 | Physical `FilterPushdown` sends a parent filter on the key of a semi/anti join to the other side, the subquery of a null-aware join included | `hash_join/exec.rs` `gather_filters_for_pushdown` | none known | yes |
| 3 | Logical `push_down_filter` infers `y > 5` for the subquery from `x > 5 AND x NOT IN (SELECT y …)` and drops its NULL | `push_down_filter.rs` `infer_join_predicates` (no null-aware check) | none known | not run on 53 |
| 4 | With `datafusion.optimizer.prefer_hash_join = false` a null-aware join is planned as `SortMergeJoinExec`, which ignores NULLs | `physical_planner.rs` (SMJ branch has no `!null_aware`) | apache/datafusion#22810 | yes |
| 5 | `NOT IN` compared with a constant (`3 NOT IN (…)`) or over a correlated subquery: no null-aware join on the one key `x = y`, so NULLs are ignored (nested loop join) or the correlation keys are checked for NULLs instead of `y` | `decorrelate_predicate_subquery.rs`, `hash_join/stream.rs` | none known | yes |
| 6 | `NestedLoopJoinExec` over the memory limit falls back to spilling its build side; the fallback emits a `LEFT`/`LEFT ANTI` join's unmatched rows once per probe partition | `nested_loop_join.rs` (gate covers only `Full`) | apache/datafusion#24675 | no (no fallback) |
| 7 | The fallback drops the unmatched probe rows of a `RIGHT`/`RIGHT ANTI` join when the last chunk ends on a refused batch | `nested_loop_join.rs` | apache/datafusion#24746 | no |
| 8 | The fallback executes the build side a second time; a file scan shares one queue of unopened files per plan instance (`enable_file_stream_work_stealing`), so the second execution reads only what the first left unopened; an exchange panics ("partition not used yet") | `nested_loop_join.rs`, `file_stream/work_source.rs` | apache/datafusion#24677 | no |
| 9 | Text compared with a number: 53 compared as text, 54 converts the text to the number; Trino 431 refuses both ("Cannot apply operator") | type coercion | by design upstream | changed |
| 10 | `time ± interval` answers an interval (`24 hours 30 mins`) instead of a time of day | type coercion | changed in DataFusion 55 | yes |
| 11 | `AccountedMergeExec` (engine): a partition task that panics ends the merged stream as if it had finished; the query returns part of its rows | `crates/codelake-core/src/output.rs` | — | yes |
| 12 | `x [NOT] IN (subquery)` outside a conjunct of a `WHERE`/`HAVING` (under `OR`, in a `CASE`, a function argument, a projection) is answered by a mark join whose mark is FALSE where SQL says UNKNOWN: `x NOT IN (…NULL…) OR x = 99` keeps rows Trino filters out | `decorrelate_predicate_subquery.rs` `mark_join` | none known (draft 3 in `docs/upstream/datafusion-54-joins.md`) | yes |
| 13 | A hash join whose keys match NULL with NULL (`JOIN … ON a IS NOT DISTINCT FROM b`, `[NOT] EXISTS (… a IS NOT DISTINCT FROM b)`, `INTERSECT`, `EXCEPT`) pushes its dynamic filter, built from the build side's non-NULL keys, into the scan of its probe side, which prunes the row groups holding only NULL keys (what an `INSERT` of a NULL row leaves) and, with `pushdown_filters`, drops the NULL rows: `t EXCEPT t` returns the NULL row, `t INTERSECT t` loses it | `hash_join/exec.rs` `gather_filters_for_pushdown` (the filter ignores `null_equality`) | none known (draft 4 in `docs/upstream/datafusion-54-joins.md`) | no |
| 14 | `a IS [NOT] DISTINCT FROM b AND c` is parsed as `a IS [NOT] DISTINCT FROM (b AND c)` (the right operand is parsed as a whole expression): over booleans it answers other rows, otherwise the statement fails (`Utf8 AND Boolean`) | `sqlparser` 0.62 `parser/mod.rs` (`parse_expr` after `DISTINCT FROM`) | none known (draft 5) | not run on 53 |

## Decision

Wrong rows are the worst defect. Each defect is kept off the path that
produces them, in the engine, without patching DataFusion. A guard either
routes the query to a correct plan or refuses it with a clear error; none
changes a correct answer.

| # | Guard | Where |
|---|---|---|
| 1, 2 | `NullAwareAntiJoinGuard` (physical, first rule, next to the federation guard): puts `NullAwareProbeBarrier` above the subquery side of every null-aware hash join; it refuses every filter from above, so neither the dynamic filter nor a parent filter reaches the scan | `codelake_core::join_guard` |
| 3 | `NullAwareFilterPushdown` wraps every `push_down_filter`: the rule runs with the keys of null-aware joins moved into their join filter (nothing is inferred from a filter), and the keys are moved back | `codelake_core::join_guard` |
| 4 | `JoinQueryPlanner` (query planner around the federation planner): a plan holding a null-aware join is planned with hash joins preferred | `codelake_core::join_guard` |
| 5 | `NullSafeNotIn` (logical, right before `decorrelate_predicate_subquery`): a `NOT IN` conjunct compared with a constant or over a correlated subquery becomes `NOT EXISTS (… WHERE <correlation> AND (x = y OR x IS NULL OR y IS NULL))`, its exact meaning where UNKNOWN filters a row out | `codelake_core::join_guard` |
| 6, 7, 8 | `NestedLoopJoinSpillGuard` (physical, after every rule that rebuilds joins): runs each `NestedLoopJoinExec` with a runtime that cannot write temporary files (the fallback's precondition) and gives its inputs the engine's runtime back, so sorts and aggregations below still spill. Over the limit the join fails with `Resources exhausted`, as in DataFusion 53 | `codelake_core::join_guard` |
| 9, 10 | `operand_checks::check_plan` in `Engine::sql` before analysis (the bodies of the views a statement reads included, so a view stored without the check is refused when read): text compared with a number (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`, `IN` lists, `IN (subquery)`, `BETWEEN`, `CASE x WHEN`) fails with `TYPE_MISMATCH`; `time ± interval` fails with `NOT_SUPPORTED`. On the PostgreSQL wire the same checks run in the wire hooks (`operand_checks::postgres_refusal`; a simple statement is checked by `SimpleRunHook` on the plan it then runs, planned once; an extended one by `PolicyHook` at execute) with PostgreSQL's typing, where a quoted literal or a parameter is untyped and takes the other side's type (`int_col > '20'` answers), and PostgreSQL's errors: SQLSTATE `42883` "operator does not exist: text > integer", `0A000` for `time ± interval` | `codelake_core::operand_checks`, `codelake-server` `hooks.rs` |
| 11 | A partition task that panics sends an error into the merge (`catch_unwind`) | `codelake_core::output` |
| 12 | `UnknownMembershipGuard` (logical, right before `decorrelate_predicate_subquery`, after `NullSafeNotIn`): an `IN (subquery)` whose value or subquery column may be NULL is refused (`NOT_SUPPORTED`) unless it sits where FALSE and UNKNOWN filter the same rows: a conjunct of a filter (a semi or null-aware anti join), or inside a filter under `AND`/`OR` with an even number of negations above an `IN` (odd above a `NOT IN`). Non-nullable sides answer everywhere | `codelake_core::join_guard` |
| 13 | `NullSafeJoinGuard` (physical, right before `FilterPushdown(Post)`, after `JoinSelection` has picked build and probe sides): puts `NullKeysKept` above the probe side of every null-safe hash join; each dynamic filter going through it gets `OR key IS NULL` for every key (the upstream fix of #23104 for null-aware joins), other filters go on as they are, and every filter is reported unsupported so the operators above keep theirs | `codelake_core::join_guard` |
| 14 | `trino_sql_checks::regroup_distinct_from` regroups the operands of every `IS [NOT] DISTINCT FROM` with `AND`, `OR`, `NOT` and the postfix `IS …` forms by their precedence (Trino's and PostgreSQL's), before planning, on the Trino protocol and in `Engine::sql`; on the PostgreSQL wire the simple protocol plans the regrouped statement and the extended one plans it when regrouping changed it (`hooks.rs`) | `codelake_core::trino_sql_checks`, `codelake-server` `hooks.rs` |

Defects 1, 3, 4, 6, 12 and 13 have a canary unit test that asserts the defect is
still there without the guard (`upstream_*` in `join_guard.rs`); when upstream
fixes it, the canary fails and points here. Defects 2, 5, 7 and 8 have none
yet: 2 shares its guard (and its effect) with 1, 5 is covered by
`semantics_tests` and the rows of `join_regressions.rs`, and 7 and 8 need a
spilled build side over a multi-file scan or an exchange, which only the
integration suite builds. Their guards stay until a release notes the fix and
the matching `join_regressions.rs` test passes without them. The integration
tests of `join_regressions.rs` check the rows against Trino 431.

## Consequences

- A nested loop join whose build side does not fit the pool fails instead of
  spilling, as it did on DataFusion 53. Joins that need it wait for the
  upstream fixes (#24675, #24746, #24677) or an engine-side fallback.
- `NOT IN` over a correlated subquery runs as an anti join whose residual
  condition has an `OR`: its correlation equalities stay hash keys, the
  value comparison is checked per candidate row. `3 NOT IN (…)` runs as a
  nested loop join.
- A session with `prefer_hash_join = false` still gets a hash join for a
  query holding `NOT IN (subquery)`.
- `x NOT IN (subquery) OR …`, `CASE WHEN x [NOT] IN (subquery) …`,
  `coalesce(x IN (subquery), …)` and `(x IN (subquery)) IS NULL` fail with
  `NOT_SUPPORTED` when either side may be NULL, where they answered wrong
  rows on DataFusion 53 and 54. Write them with `EXISTS`, move the `NOT IN`
  to its own conjunct, or exclude the NULLs (`coalesce`) on both sides.
- Clients of the engine that relied on comparing text with numbers now get
  `TYPE_MISMATCH`, as on Trino. Cast explicitly. This is a breaking change
  (CHANGELOG), shipped as `fix(core)` in 9ef792e without the `!` marker.
- PostgreSQL wire clients get `42883` ("operator does not exist: text >
  integer") for typed text against a number (`varchar_col > 9`,
  `'7'::varchar = 7`, `varchar_col IN (1, 2)`), as from PostgreSQL itself,
  where DataFusion 53 compared text and 54 converted the text to the number;
  `time ± interval` gets `0A000`. Untyped literals keep PostgreSQL's
  meaning (`int_col > '20'` compares numbers). Breaking (CHANGELOG, ISS-006).
  A simple-protocol statement is planned twice (once in the hook to check
  it); a statement the hook cannot plan is left to `DataFusion`, which
  reports its own error.
- `time ± interval` is refused until the train reaches DataFusion 55.
- A null-safe join's dynamic filter still skips the row groups whose keys it
  excludes, but not those with NULL keys; the rows are those of Trino 431
  (`join_regressions::null_safe_joins_keep_the_rows_whose_key_is_null`,
  tables written by Trino and by the engine).
- `a IS DISTINCT FROM b = c` keeps the parser's reading, `a IS DISTINCT FROM
  (b = c)` (PostgreSQL's; Trino 431 rejects it).
- Removing a guard needs the DataFusion release with the fix **and** the
  matching test of `join_regressions.rs` passing without it. Upstream reports
  are drafted in `docs/upstream/datafusion-54-joins.md`; filing them needs
  maintainer approval (as for ADR 0005).
