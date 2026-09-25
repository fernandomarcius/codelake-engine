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
