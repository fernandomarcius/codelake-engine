# Upstream reports: DataFusion 54.1 join defects

Drafts for apache/datafusion, prepared with ADR 0014. **Not filed**: filing
needs maintainer approval. Each draft is self-contained; the reproductions use
`datafusion-cli` 54.1.0 unless noted.

## Already reported upstream (link, add our reproduction as a comment)

| Defect | Issue | What to add |
|---|---|---|
| Dynamic filter drops the NULL of a null-aware anti join's probe side | apache/datafusion#23104 (fixed on `main`, not in 54.1.0) | Ask for a 54.x backport: it is a wrong result on default settings with Parquet (file pruning, and `pushdown_filters = true` for a NULL that shares a file). |
| Null-aware anti join planned as sort-merge join | apache/datafusion#22810 | Reproduction below (`prefer_hash_join = false`, 4 partitions). |
| NLJ spill fallback repeats unmatched rows | apache/datafusion#24675 | The fallback is on by default whenever the runtime has a disk manager. |
| NLJ spill fallback drops unmatched probe rows | apache/datafusion#24746 | — |
| NLJ spill fallback re-executes the build side | apache/datafusion#24677 | With `enable_file_stream_work_stealing` (default) the second execution of the same scan reads only the files the first left unopened: rows are lost in silence. Below an exchange, `RepartitionExec` panics ("partition not used yet"). |

## Draft 1 — Filter inference drops NULLs from the subquery of a null-aware anti join

**Title:** `push_down_filter` infers predicates for the right side of a
null-aware anti join (wrong `NOT IN` result)

**Body:**

`NOT IN (subquery)` is planned as a null-aware `LeftAnti` join. When a filter
on the outer key sits above it, `push_down_filter` infers the same filter for
the subquery's key (`infer_join_predicates`) and pushes it below the join.
The inferred predicate removes the subquery's NULL, and the null-aware join
then returns rows where SQL requires none.

```sql
CREATE TABLE o(k BIGINT) AS VALUES (1), (6), (7), (NULL);
CREATE TABLE s(n BIGINT) AS VALUES (1), (NULL);
SELECT count(*) FROM o WHERE k > 5 AND k NOT IN (SELECT n FROM s);
-- expected 0 (s holds a NULL); DataFusion 54.1.0 counts the rows with k > 5
```

(Reproduced with Parquet files in CodeLake:
`join_guard::tests::upstream_still_infers_filters_for_the_subquery`.)

The same happens in the physical `FilterPushdown`:
`HashJoinExec::gather_filters_for_pushdown` adds the outer key columns of a
semi/anti join to the allowed set of the other side, so a parent filter on
`k` reaches the scan of `s`.

Suggested fix: skip inference towards the right side (and the semi/anti key
remapping in the physical rule) when `join.null_aware` is set, or add
`OR key IS NULL` to the inferred predicate as #23104 does for the dynamic
filter.

## Draft 2 — `NOT IN` compared with a constant or over a correlated subquery ignores NULLs

**Title:** `NOT IN (subquery)` loses NULL semantics when the value is a
constant or the subquery is correlated

**Body:**

`decorrelate_predicate_subquery` marks the join null-aware, but the
null-aware hash join handles NULLs only for a single equi-key `x = y` and no
residual filter.

```sql
-- constant value: planned as a nested loop join, NULL ignored
SELECT x FROM (VALUES 1, 2) t(x)
WHERE 3 NOT IN (SELECT y FROM (VALUES 1, NULL) u(y));
-- expected: no rows; DataFusion 54.1.0: 1, 2

-- correlated: the correlation key becomes the join key, `3 = i` the filter;
-- the join checks the correlation key for NULLs, not `i`
SELECT id FROM (VALUES (1, 1), (2, 2)) o(id, g)
WHERE 3 NOT IN (SELECT i FROM (VALUES (1, 1), (NULL, 2)) n(i, g) WHERE n.g = o.g);
-- expected: 1; DataFusion 54.1.0: 1, 2

-- correlated with a column value: planning error
SELECT id FROM (VALUES (1, 1), (2, 2), (3, 3)) o(id, g)
WHERE id NOT IN (SELECT i FROM (VALUES (1, 1), (NULL, 2), (5, 3)) n(i, g) WHERE n.g = o.g);
-- expected: 3; DataFusion 54.1.0: "null_aware anti join only supports single column join key"
```

Workaround used downstream: rewrite such a conjunct, before decorrelation,
as `NOT EXISTS (SELECT … WHERE <correlation> AND (x = y OR x IS NULL OR
y IS NULL))`.

## Draft 3 — A mark join answers `x IN (subquery)` as FALSE where it is UNKNOWN

**Title:** `IN (subquery)` outside a top-level conjunct loses its UNKNOWN
(mark join)

**Body:**

Outside a top-level conjunct of a filter, `decorrelate_predicate_subquery`
replaces `x [NOT] IN (subquery)` with the mark column of a `LeftMark` join.
The mark is TRUE or FALSE; SQL requires UNKNOWN when nothing matches and `x`
or a value of the subquery is NULL. Wherever UNKNOWN and FALSE differ, the
answer is wrong.

```sql
SELECT count(*) FROM (VALUES (1), (2), (3)) t(x)
WHERE x NOT IN (SELECT y FROM (VALUES (1), (NULL)) u(y)) OR x = 99;
-- expected 0; DataFusion 53 and 54.1.0: 2

SELECT count(*) FROM (VALUES (1), (2), (3)) t(x)
WHERE CASE WHEN x NOT IN (SELECT y FROM (VALUES (1), (NULL)) u(y)) THEN true ELSE false END;
-- expected 0; DataFusion 53 and 54.1.0: 2
```

(Reproduced in CodeLake:
`join_guard::tests::upstream_mark_join_still_reads_unknown_as_false`.)

Suggested fix: a null-aware mark (NULL when unmatched and the probe value
or any build value is NULL), or plan such a membership only when both sides
are non-nullable.

Workaround used downstream: refuse the statement when a NULL is possible and
the position is not one where FALSE and UNKNOWN filter the same rows (under
`AND`/`OR` in a filter, with the right parity of negations).

## Draft 4 — A null-safe hash join's dynamic filter drops the probe side's NULL keys

**Title:** Dynamic filter of a hash join with `NullEqualsNull` prunes NULL
keys from the probe side (`EXCEPT`, `INTERSECT`, `IS NOT DISTINCT FROM`)

**Body:**

A `HashJoinExec` whose `null_equality` is `NullEqualsNull` matches a NULL
key with a NULL key, but the dynamic filter it pushes into its probe side
(bounds or `IN` of the build side's keys) is built from the non-NULL keys
only. The probe scan then prunes row groups holding only NULL keys, and with
`pushdown_filters = true` drops NULL rows in any row group; those rows never
reach the join.

```sql
COPY (SELECT 1::bigint AS id, 'a' AS v UNION ALL SELECT 2, 'b') TO 't/part-0.parquet';
COPY (SELECT 3::bigint AS id, CAST(NULL AS varchar) AS v) TO 't/part-1.parquet';
CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 't/';
SET datafusion.execution.parquet.pushdown_filters = true;
SELECT count(*) FROM t a JOIN t b ON a.v IS NOT DISTINCT FROM b.v;  -- expected 3; 54.1.0: 2
SELECT * FROM t EXCEPT SELECT * FROM t;                            -- expected no row; 54.1.0: (3, NULL)
```

(Reproduced in CodeLake:
`join_guard::tests::upstream_null_safe_join_still_prunes_null_only_row_groups`;
through Hive tables written by an `INSERT` per row group, also with default
settings.)

Suggested fix: when `null_equality` is `NullEqualsNull`, push
`<filter> OR key IS NULL` for every key, as apache/datafusion#23104 does for
null-aware anti joins.

## Draft 5 — `IS [NOT] DISTINCT FROM` takes the rest of the expression as its right operand

**Title (apache/datafusion-sqlparser-rs):** `a IS DISTINCT FROM b AND c` is
parsed as `a IS DISTINCT FROM (b AND c)`

**Body:**

After `IS [NOT] DISTINCT FROM` the parser calls `parse_expr()`, which parses
the right operand at the lowest precedence, so `AND`/`OR` (and a following
`IS NULL`) end up inside it. PostgreSQL, Trino and the SQL standard bind the
operator tighter than `AND`/`OR`. Over booleans the query answers other rows
(`ON a.f IS NOT DISTINCT FROM b.f AND a.id = b.id` joins every pair whose `f`
matches `b.f AND a.id = b.id`); over other types planning fails
(`Utf8 AND Boolean`).

```sql
SELECT (true IS DISTINCT FROM false AND false);
-- PostgreSQL: false ((true IS DISTINCT FROM false) AND false)
-- sqlparser 0.62: true (true IS DISTINCT FROM (false AND false))
```

Suggested fix: parse the right operand with `parse_subexpr` at the precedence
of `IS` (`self.dialect.prec_value(Precedence::Is)`), as the other `IS` forms
are.
