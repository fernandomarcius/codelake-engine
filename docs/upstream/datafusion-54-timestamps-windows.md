# Upstream reports: DataFusion 54.1 timestamp precision and window frames

Drafts for apache/datafusion, found by the shadow run of 2026-09-26
(ISS-042, ISS-045). **Not filed**: filing needs maintainer approval
(ADR 0005). CodeLake works around both; the workarounds are named below.

## Draft 1 — Unwrapping a cast truncates a timestamp literal (wrong result)

**Title:** `unwrap_cast_in_comparison` converts a timestamp literal to a
coarser unit by truncation

**Body:**

The simplifier rewrites `CAST(col AS Timestamp(µs)) >= literal` into
`col >= CAST(literal AS Timestamp(ms))` when `col` is `Timestamp(ms)`.
`cast_between_timestamp` (`datafusion-expr-common`, `casts.rs`) divides the
literal by the unit ratio without checking that nothing is lost, so
`.123100` becomes `.123`, and the comparison changes:

```sql
SELECT k FROM (VALUES (1, arrow_cast('2020-01-01 00:00:00.123', 'Timestamp(Millisecond, None)'))) t(k, ts)
WHERE CAST(ts AS TIMESTAMP) >= arrow_cast('2020-01-01 00:00:00.123100', 'Timestamp(Microsecond, None)');
-- expected no row (.123 < .1231); DataFusion 54.1.0 returns k = 1
```

Expected: unwrap only when the literal converts exactly, or round in the
direction the operator needs (`>=` up, `<=` down; `=` of an inexact literal
is false).

Related: comparison coercion (`timeunit_coercion` in `type_coercion/binary.rs`)
picks the **coarser** unit of two timestamps, truncating the finer side;
SQL engines compare at the finer precision.

CodeLake: `coercion::unify_timestamps` casts every timestamp to the finest
unit and wraps the cast of a column in `codelake_exact_cast` so the
simplifier does not unwrap it
(`semantics_tests::timestamps_of_different_precisions_compare_at_the_finer_one`).

## Draft 2 — `RANGE` frame on a binary sort key fails without offsets

**Title:** "Cannot run range queries on datatype: Binary" for a window with
the default frame

**Body:**

```sql
SELECT rank() OVER (ORDER BY b) FROM (VALUES (X'02'), (X'01')) t(b);
-- Internal error: Cannot run range queries on datatype: Binary
```

A window with `ORDER BY` and no frame gets `RANGE BETWEEN UNBOUNDED
PRECEDING AND CURRENT ROW`. Type coercion (`coerce_window_frame`) asks for a
bound type from the sort key even when no bound has an offset, and
`extract_window_frame_target_type` has none for binary keys. The error is
an internal error ("likely caused by a bug in DataFusion's code").

Expected: bounds without offsets need no type; the query plans (the frame
holds the current row's peers, as `GROUPS … CURRENT ROW` does).

CodeLake: `window_frames::RangeFramesAsGroups` rewrites such frames to
`GROUPS` before type coercion
(`semantics_tests::windows_ordered_by_varbinary_keep_range_peers`).
