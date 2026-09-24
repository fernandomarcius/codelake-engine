# ADR 0008 — Trino function library with golden tests against Trino 431

- Status: Proposed (F4)
- Date: 2026-09-23

## Context

CodeLake replaces Trino, so every SQL text a client sends must mean what it
meant on Trino. `DataFusion` ships many functions with Trino's *names* but
PostgreSQL/Spark *semantics*: `substr('abcde', -2)` returns `abcde` instead of
`de`, `greatest(1, NULL)` returns 1 instead of NULL, `concat` skips NULLs,
`to_hex` prints lower case, `regexp_replace` replaces only the first match,
`round(int)` returns `double`, `CAST(2.5 AS integer)` truncates to 2 (Trino: 3),
`CAST(1.5E0 AS varchar)` gives `1.5` (Trino: `1.5E0`), and a
`timestamp with time zone` cast to `timestamp` keeps the UTC wall clock instead
of the local one. None of these fail: they return a different answer, the
worst defect a query engine can have.

The two reference pipelines use `date_add('unit', n, ts)`, `date_diff`,
`date_format`, `format_datetime`, `with_timezone`, `AT TIME ZONE`,
`localtimestamp`, `from_big_endian_64(substr(varbinary, 3))`, `to_hex`,
`checksum`, `max_by`, `approx_percentile`, `array_join(array_agg(…))`,
`uuid()`, `concat_ws`, `greatest`, `round(decimal, n)`, `regexp_replace`, `chr`
and `replace` (inventory in `docs/requirements/trino-replacement.md`). They are
the acceptance floor, not the scope: the library follows Trino's documentation
categories for any client.

## Decision

1. `codelake_core::trino_functions`, one module per Trino category (date/time,
   formatting, string, regexp, binary, math, conditional, aggregate, array/map,
   JSON, URL), installed by `Engine::new`. A function with a Trino name replaces
   the `DataFusion` built-in of the same name; the Trino behaviour wins.
2. Errors carry Trino's error name as a message prefix
   (`INVALID_FUNCTION_ARGUMENT: …`); `trino_functions::trino_error_name`
   classifies any engine error (Arrow's division by zero, overflow and cast
   errors included) into Trino's `errorName`, for the Trino protocol and for
   `try`.
3. Six hooks beyond UDFs, each the smallest place that sees the construct:
   - a text pre-pass before parsing (`preprocess`), editing only the matched
     token spans: `VALUES 1, 2` rows without parentheses (commas inside
     `ARRAY[…]` do not split rows), `TRIM(LEADING | TRAILING | BOTH FROM s)`
     without characters, and
     `CAST(x AS array(T) | map(K, V) | row(…))`, which the parser only knows
     with angle brackets, become a cast function carrying Trino's type text;
   - a function rewrite (before type coercion) swaps built-ins the SQL planner
     creates without a name lookup (`EXTRACT`, `POSITION`, `SUBSTRING … FROM`)
     and turns `if(…)` into `CASE` so only one branch is evaluated;
   - an analyzer rule (after type coercion, so it also sees implicit casts)
     gives `CAST` Trino semantics where Arrow's differs (zone ↔ plain
     timestamps, timestamp precision rounding, `double`/`timestamp` → `varchar`
     text, `double`/`decimal` → integer rounding, `varchar` → zoned timestamp);
   - an AST rewrite in `Engine::sql_with` for literal syntax: `TIMESTAMP '…'`
     gets the precision of its digits (and a zone suffix makes it zoned),
     `CAST(x AS timestamp)` means `timestamp(3)`, `DECIMAL '1.50'` is
     `decimal(3,2)`, `x AT TIME ZONE z` is Trino's `at_timezone(x, z)`,
     `CAST(x AS varchar(n))` keeps its length (Arrow drops it),
     `listagg(…) WITHIN GROUP (ORDER BY …)` becomes
     `array_join(array_agg(… ORDER BY …), sep)`, `CAST(x AS json)` and
     `JSON '…'` build json values, `EXTRACT`/`POSITION`/`SUBSTRING` and
     `x[i]` name Trino's functions, numeric literals get Trino's types (`1`
     is `integer`, `1.5` is `decimal(2,1)`, `1.5E0` is `double`, the sign
     belongs to the literal so `-2147483648` is an `integer`, and an integer
     beyond `bigint` is a `SYNTAX_ERROR`; positional
     `ORDER BY 1`/`GROUP BY 1`, `LIMIT` and window arguments are left alone),
     unaliased output expressions and the columns of a bare `VALUES` are
     named `_col<i>`, `a || b` calls Trino's `concat` operator (strings,
     `varbinary`, arrays, an array and an element), `X'ED A0 80'` drops its
     blanks, `UNNEST(m) AS t(k, v)` unnests a map's keys and values (or the
     fields of an array of rows), and `t CROSS JOIN UNNEST(t.a) AS u(x)` (or
     `t, UNNEST(…)`) becomes a derived table that unnests in its projection
     (`DataFusion` cannot decorrelate the lateral join);
   - a physical optimizer rule, run after the engine's own, that installs
     `try(…)` in projections, filters, aggregates, hash and nested-loop join
     keys and conditions and sort keys (a `PhysicalExpr` that bisects the
     batch and returns NULL for the rows failing with a user error) and turns
     on `fail_on_overflow` for `+`, `-`, `*` (`DataFusion` wraps around
     silently; Trino fails) and IEEE NaN semantics for float comparisons
     (`DataFusion` uses a total order: `nan() > 1` is true). New hash-join
     keys drop the join's dynamic filter (the pushed copy stays `true`);
   - a logical optimizer rule, run after every other rule (after federation,
     so remote plans are untouched): float equi-join keys become
     `codelake_trino_float_key(k)` (`-0.0` is `0.0`, NaN matches nothing),
     grouping keys, window partitions and `DISTINCT` arguments
     `codelake_trino_float_group_key(k)` (`-0.0` is `0.0`, one NaN group)
     under their original names, and integer
     `+ - *` in window function arguments become checked functions (the
     window operator evaluates them where the physical rule cannot reach).
   `Engine::sql` runs the analyzer before building the `DataFrame`, so the
   frame's schema is the one its batches have (the rewrites change result
   types, e.g. decimal arithmetic).
   `json` has no Arrow type: it is `varchar` on a field whose metadata says
   `codelake.trino_type = json`; the cast rule sends `CAST(json AS T)` to a
   function that reads the JSON value (Trino's `JsonUtil`) instead of casting
   the text. `sum`, `min` and `max` are replaced too: `bigint` sums are
   exact and fail on overflow, `real` stays `real`, `decimal(p, s)` sums to
   `decimal(38, s)`, and `min`/`max` skip NaN; decimal `+ - * / %` use
   Trino's result types and half-up rounding (`bigint / 3600.0` has one
   fractional digit, as the reference sensors expect).
   The session zone defaults to UTC (Trino's reference setting), configurable through
   `datafusion.execution.time_zone`; `ORDER BY` without `NULLS FIRST|LAST`
   puts NULLs last in both directions, in queries, windows and ordered
   aggregates (`datafusion.sql_parser.default_null_ordering = nulls_last`;
   `DataFusion` puts them first for `DESC`, so a `row_number() … DESC` dedup
   kept the NULL row).
   `array_agg` is the library's own: `DataFusion`'s trusts the planner's
   input order and reverses an aggregate whose `ORDER BY` is the opposite of
   another's, so two orderings in one query came out equal. This one keeps
   the ordering keys, sorts them itself (stable), never reverses, and
   retracts rows for sliding window frames; `listagg` filters NULL values
   (a group of only NULLs is NULL). `decimal` sums run on 76 digits and fail
   (`Decimal overflow`) when the result does not fit `decimal(38, s)`;
   `avg(real)` is `real`, `bitwise_*_agg` are `bigint`, and
   `corr`/`covar_*`/`regr_*` over `integer`/`real` resolve to the `real`
   overload, as in Trino.
4. **Golden tests.** `crates/codelake-core/tests/fixtures/trino431-functions/*.json`
   hold SQL cases and the result a real `trinodb/trino:431` returned (types,
   rows in client-protocol JSON, or the error name).
   `scripts/golden/regenerate-functions.sh` rewrites them from a Trino
   container. The unit test `trino_functions::golden` runs every case through
   `Engine::sql` in the fast gate, without Trino, and compares types, values
   (doubles bit for bit unless the case sets `ulps`) and error names. A case is
   written from the SQL alone; the expected answer only ever comes from Trino.
   Trino syntax the engine cannot plan (lambdas) keeps its Trino result and is
   marked `"unsupported": "<reason>"`: the test then requires the engine to
   fail, so it can never start returning unchecked rows.
5. Memory: aggregate state is reported through `Accumulator::size`, which the
   aggregation operator reserves from the pool. `approx_distinct` and
   `approx_percentile` run in bounded memory, as Trino's `HyperLogLog` and
   `qdigest`: `approx_distinct` is exact up to 4096 distinct hashes, then a
   `HyperLogLog` of 2^14 registers (standard error 0.8%); `approx_percentile`
   keeps weighted values, merges them without loss when the buffer fills
   (equal values, runs of consecutive integers: dense integer data stays
   exact, as Trino's digest answers it), and only then summarizes to 4096
   equal-weight items (rank error under 0.03%). A buffer keeps its capacity once grown: the aggregation
   operator only grows its reservation, so a buffer that shrank and grew
   again would be counted twice. `max_by(x, y, n)` keeps a bounded heap.
6. Regular expressions: Rust's `regex` (linear time) for every pattern it
   accepts; `fancy-regex` (backtracking, like Joni) only for look-around and
   back-references, which `regex` rejects. The dependency is MIT and already
   shares `regex-automata`/`regex-syntax` with the graph.

## Consequences

- Federation: these functions exist only in the engine. SQL Server's pushdown
  policy keeps every function local. PostgreSQL and MySQL still use the
  unrestricted `datafusion-table-providers` federation, which would push a call
  such as `substr(x, -2)` to a database that evaluates *its* `substr`. Their
  pushdown policy (ROADMAP M1 follow-up) must reject calls for which
  `trino_functions::is_trino_function` is true.
- The PostgreSQL wire path calls `SessionContext::sql` directly: UDFs, the
  rewrite and the cast rule apply, the AST rewrite (literal syntax) does not,
  until that path goes through `Engine::sql`.
- Arrow limits: one time zone per column (functions whose result zone comes
  from an argument need a constant zone), four timestamp units
  (`timestamp(1)` behaves as `timestamp(3)`), no `uuid` type (`varchar`
  here), no per-row zones. The json mark lives in field metadata: an
  expression that drops it (`CASE`, `COALESCE`) turns json back into
  `varchar`.
- `try`, overflow checks and NaN comparisons cover projections, filters,
  aggregates, join keys and conditions (hash and nested-loop joins) and sort
  keys; window function arguments fail on overflow (logical rule) but keep a
  pass-through `try` (a failing `try` argument fails the query there, never
  returns a different answer). Sort-merge and symmetric hash joins (not
  planned by default) keep the engine's arithmetic in their conditions.
- `-0.0`/NaN keys follow Trino in joins, `GROUP BY`, `DISTINCT`, set
  operations, window `PARTITION BY`, `histogram`, `map_agg`, `checksum` and
  `array_distinct`; `IN (…)` lists keep the engine's bitwise float
  identity.
- `format_datetime(…, 'z')` prints Java's short zone names: the IANA
  abbreviation when it is a name, a table of the JDK names for the regions
  whose IANA abbreviation is a number (South America); any other zone fails
  with `NOT_SUPPORTED` rather than print a different name.
- Output names follow Trino (`_col<i>`), not `DataFusion`'s expression text;
  a client that relied on names like `count(*)` sees `_col0`.
- Gaps are listed in `docs/trino-functions.md`; each is either unimplemented
  (planning error, never a silent difference) or documented with its bound.
