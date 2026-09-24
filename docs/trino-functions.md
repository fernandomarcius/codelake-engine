# Trino function library

CodeLake evaluates Trino SQL with Trino 431 semantics: functions, casts,
literals and the operators around them
(`crates/codelake-core/src/trino_functions`, [ADR 0008](adr/0008-trino-function-library.md)).
Every function below is covered by golden cases recorded from a real Trino 431
(`crates/codelake-core/tests/fixtures/trino431-functions`, one file per
category) and checked by the fast gate, without Trino.

## Golden cases

A case is `{"name", "sql"}`, plus optional `"ulps"` (tolerance in units in the
last place, only for transcendental functions and floating-point
accumulations) and `"unsupported": "<reason>"` (Trino syntax the engine cannot
plan: the gate then requires CodeLake to *fail*, never to return rows). The
script fills `"trino"` with Trino's types and rows, or its `errorName`:

```bash
scripts/golden/regenerate-functions.sh              # starts trinodb/trino:431 (heap pinned to 1 GB), rewrites every fixture
scripts/golden/regenerate-functions.sh math.json    # only some files
TRINO_GOLDEN_URL=http://127.0.0.1:18431 scripts/golden/regenerate-functions.sh   # an already running Trino
```

Never edit `"trino"` by hand; review the diff of a regeneration like code.
Each function has at least three cases (a normal value, NULL, an edge such as
a leap day, a DST change, a negative number, an overflow, a precision limit)
and its error cases.

## Implemented

| Category | Functions and semantics |
|---|---|
| Date and time | `date_add`, `date_diff` (Joda month/year arithmetic), `date_trunc`, `year`, `quarter`, `month`, `week`/`week_of_year`, `day`/`day_of_month`, `day_of_week`/`dow`, `day_of_year`/`doy`, `year_of_week`/`yow`, `hour`, `minute`, `second`, `millisecond`, `timezone_hour`, `timezone_minute`, `EXTRACT`, `last_day_of_month`, `date`, `current_timestamp`/`now`, `localtimestamp`, `current_date`, `localtime`, `current_timezone`, `with_timezone`, `at_timezone`/`AT TIME ZONE`, `from_unixtime` (1, 2 and 3 arguments, rounded like `Math.round`), `from_unixtime_nanos`, `to_unixtime`, `to_iso8601`, `from_iso8601_date`, `from_iso8601_timestamp`, `from_iso8601_timestamp_nanos`, `human_readable_seconds`, `parse_duration`, `to_milliseconds`, `INTERVAL` arithmetic (`'1-2' YEAR TO MONTH`, `'1 02:03:04' DAY TO SECOND`) |
| Formatting | `date_format`, `date_parse` (MySQL specifiers), `format_datetime`, `parse_datetime` (Joda patterns) |
| Casts and literals | `CAST`/`TRY_CAST` with Trino semantics where Arrow differs: zoned ↔ plain timestamps, timestamp precision (half up), `double`/`real`/`timestamp` → `varchar` (`1.5E0`), `double`/`decimal` → integers (half away from zero), `double` → `decimal` (shortest text, half up), `varchar` → integers (no blanks), `double`, `decimal` (half up), `boolean` (`t`/`true`/`1`/`f`/`false`/`0`), `date`, `timestamp`, `timestamp with time zone`; `varchar(n)` truncates text and rejects values that do not fit; `array(T)`, `map(K, V)`, `row(…)` targets; `varbinary` → `varchar` is `TYPE_MISMATCH`. Literals: `1` is `integer`, `1.5` is `decimal(2,1)`, `1.5E0` is `double`, `TIMESTAMP '…'` has the precision of its digits (and its zone), `DECIMAL '…'`, `JSON '…'`, `X'…'`; `VALUES 1, 2` rows |
| Arithmetic | decimal `+ - * / %` with Trino's result types and half-up rounding; integer and decimal overflow fails (`NUMERIC_VALUE_OUT_OF_RANGE`) instead of wrapping; `double`/`real` comparisons with IEEE NaN semantics |
| String | `substr`/`substring`, `length`, `lower`, `upper`, `trim`/`ltrim`/`rtrim` (and `TRIM(… FROM …)`), `concat` (varchar, varbinary, arrays), `concat_ws` (values or array), `replace`, `strpos`/`position`, `starts_with`, `lpad`, `rpad`, `split`, `split_part`, `split_to_map`, `chr`, `codepoint`, `reverse`, `levenshtein_distance`, `hamming_distance`, `translate`, `soundex`, `luhn_check`, `normalize` (`NFC`, `NFD`, `NFKC`, `NFKD`), `to_utf8`, `from_utf8` |
| Regular expressions | `regexp_like`, `regexp_replace`, `regexp_extract`, `regexp_extract_all`, `regexp_count`, `regexp_position`, `regexp_split`; `\d`, `\w`, `\s`, `\b` are ASCII as in Joni; matches iterate like Java's `Matcher.find()` |
| Binary | `to_hex`, `from_hex`, `to_big_endian_32/64`, `from_big_endian_32/64`, `to_ieee754_32/64`, `from_ieee754_32/64`, `md5`, `sha1`, `sha256`, `sha512`, `xxhash64`, `crc32`, `hmac_md5/sha1/sha256/sha512`, `to_base64`, `from_base64`, `to_base64url`, `from_base64url`, `length`, `substr`, `concat`, `reverse` |
| Math | `round`, `ceil`/`ceiling`, `floor`, `truncate`, `abs`, `sign`, `mod`, `power`/`pow`, `sqrt`, `cbrt`, `exp`, `ln`, `log2`, `log10`, `log`, `degrees`, `radians`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `cosh`, `tanh`, `e`, `pi`, `infinity`, `nan`, `is_nan`, `is_finite`, `is_infinite`, `random`/`rand`, `width_bucket`, `to_base`, `from_base`, `bitwise_and/or/xor/not`, `bitwise_left_shift`, `bitwise_right_shift`, `bitwise_right_shift_arithmetic` (width of the argument's type), `bit_count`, `normal_cdf`, `inverse_normal_cdf`, `wilson_interval_lower`, `wilson_interval_upper`, `cosine_similarity` |
| Comparison and conditional | `greatest`, `least` (Trino's decimal super type), `if`, `try` (per row), `typeof`, `COALESCE`, `NULLIF`, `CASE` |
| Aggregate | `sum` (`bigint` exact with overflow error, `real` stays `real`, `decimal(p,s)` → `decimal(38,s)`), `avg`, `min`/`max` (NaN skipped), `count`, `count_if`, `checksum`, `max_by`/`min_by` (and the `n` form), `arbitrary`/`any_value`, `approx_distinct`, `approx_percentile` (value, weighted, array of percentiles), `bool_and`/`bool_or`/`every`, `bitwise_and_agg`/`bitwise_or_agg`, `geometric_mean`, `skewness`, `kurtosis`, `histogram`, `map_agg`, `listagg … WITHIN GROUP`, `array_agg`, `stddev`/`stddev_samp`/`stddev_pop`, `variance`/`var_samp`/`var_pop`, `corr`, `covar_pop`, `covar_samp`, `regr_slope`, `regr_intercept`, `FILTER (WHERE …)` |
| Window | `row_number`, `rank`, `dense_rank`, `ntile` (`bigint`), `percent_rank`, `cume_dist`, `lag`, `lead`, `first_value`, `last_value`, `nth_value`, aggregates over frames |
| Array and map | `cardinality`, `element_at`, subscript `x[i]` / `m[k]` (errors out of bounds and for a missing key), `contains`, `contains_sequence`, `array_position`, `array_distinct`, `array_sort`, `array_max`, `array_min`, `array_remove`, `slice`, `trim_array`, `repeat`, `sequence` (bigint, date, timestamp; `INTERVAL` steps), `array_union`, `array_intersect`, `array_except`, `arrays_overlap`, `array_join`, `concat`, `reverse`, `flatten`, `ngrams`, `combinations`, `zip`, `map` (unique non-NULL keys), `map_keys`, `map_values`, `map_entries`, `map_from_entries`, `map_concat`, `UNNEST` |
| JSON | `json_parse`, `json_format`, `json_extract`, `json_extract_scalar`, `json_size`, `json_array_length`, `json_array_get`, `json_array_contains`, `is_json_scalar`, `CAST(x AS json)`, `CAST(json AS varchar/boolean/integers/double/decimal/array/map/row)` (Trino's `JsonUtil` rules) |
| URL | `url_extract_protocol`, `url_extract_host`, `url_extract_port`, `url_extract_path`, `url_extract_query`, `url_extract_fragment`, `url_extract_parameter`, `url_encode`, `url_decode` |

Errors carry Trino's `errorName`: `trino_functions::trino_error_name` maps
every engine error (this library's, Arrow's division by zero, overflow and
cast errors, planner errors) to the name Trino reports.

## Not implemented, or bounded

Everything below either fails loudly (planning or execution error) or is a
documented bound; none returns a different answer silently.

| Category | Gap | Why |
|---|---|---|
| Syntax | Lambda expressions: `transform`, `filter`, `reduce`, `any_match`, `all_match`, `none_match`, `zip_with`, `map_filter`, `transform_keys`, `transform_values`, `array_sort` with a comparator | DataFusion 53 has no lambda in its logical plan; the golden cases are marked `unsupported` (the engine must fail) |
| Syntax | `UNNEST … WITH ORDINALITY` | not implemented by DataFusion 53's planner (`unsupported` case) |
| Syntax | Trino type syntax outside `CAST`/`TRY_CAST` (e.g. `array(bigint)` in `CREATE TABLE`) | the text rewrite only covers casts; DDL types belong to the catalog front |
| Syntax | Two aggregate calls that differ only by a `CAST` of the argument (`sum(x), sum(CAST(x AS double))`) | DataFusion names a cast after its operand, so both aggregates get one name: planning error (alias does not help; use a subquery) |
| Syntax | `SELECT a, a` (duplicate column references) | DataFusion needs unique output names; unaliased expressions are named `_col<i>` like Trino, column references keep their name |
| Types | Timestamp precisions 1, 2, 4, 5, 7, 8 | Arrow has four units: `timestamp(1)` is stored and printed as `timestamp(3)` (same instant, three fractional digits) |
| Types | One time zone per `timestamp with time zone` column | Arrow keeps the zone in the type. A literal keeps its own zone; values of other zones in one column (`from_iso8601_timestamp`, `parse_datetime` with a zone, `CAST(varchar AS timestamp with time zone)`) fail with `NOT_SUPPORTED` instead of being shifted |
| Types | `json` is `varchar` with a field mark | expressions that drop field metadata (`CASE`, `COALESCE` over json) turn it back into `varchar`; `CAST` of such a value casts the text |
| Types | `varchar(n)` lengths | Arrow strings have no length: `typeof('a')` is `varchar` (Trino: `varchar(1)`); `CAST(x AS varchar(n))` still truncates or fails as in Trino |
| Types | `char(n)` padding semantics, `time with time zone`, `current_time`, `interval` values in results, `uuid`/`ipaddress` types | no Arrow type; `uuid()` returns `varchar` |
| Arithmetic | Overflow checks, NaN comparisons and `try` inside join conditions, sort keys and window function arguments | the physical rule rewrites projections, filters and aggregates; elsewhere `try` passes errors through (the query fails) and arithmetic wraps like DataFusion. A float equi-join matches NaN keys (Trino does not) |
| Arithmetic | `mod(n, 0)` | fails with `DIVISION_BY_ZERO`; Trino reports `GENERIC_INTERNAL_ERROR` (`/ by zero`) |
| Arithmetic | `sum(bigint)` whose running total overflows but final total fits | returns the exact total; Trino fails on the intermediate overflow |
| Arithmetic | Decimal super type of `CASE`/`COALESCE`/`IN` branches | the engine's common type can have more precision than Trino's (same value) |
| Math | Transcendental functions (`exp`, `ln`, `sin`, …, `normal_cdf`, `inverse_normal_cdf`) | libm vs Java `StrictMath`/Commons Math: equal within 1–2 units in the last place (golden `ulps`) |
| Math | `beta_cdf`, `inverse_beta_cdf`, `t_cdf`, `t_pdf` | not implemented (planning error) |
| String | `format`, `word_stem` | `format` needs Java's `Formatter`; `word_stem` needs Lucene's stemmers |
| Aggregate | `approx_most_frequent`, `approx_set`, `merge`, `numeric_histogram`, `qdigest_agg`, `tdigest_agg`, `set_agg`, `set_union`, `multimap_agg`, `reduce_agg`, `min(x, n)`, `max(x, n)`, `listagg … ON OVERFLOW TRUNCATE` | not implemented (planning error); `listagg` has no 1 MB limit |
| Aggregate | `stddev`/`variance` over sliding window frames | incremental retraction: within a few units in the last place of Trino |
| Array/map | `array_histogram`, `shuffle`, `remove_nulls`, `multimap_from_entries`, `map_zip_with` | not implemented (planning error) |
| JSON | `json_value`, `json_query`, `json_exists`, `json_object`, `json_array` (SQL:2016 JSON path) | not implemented (planning error) |
| Federation | Functions are evaluated in the engine | SQL Server's pushdown policy keeps them local; PostgreSQL/MySQL federation still needs a policy that refuses `is_trino_function` calls (ADR 0008) |
| Protocol | The PostgreSQL wire path | calls `SessionContext::sql`: UDFs and analyzer rules apply, the SQL text and AST rewrites (literals, `VALUES 1, 2`, container casts, `_col<i>`) do not until it goes through `Engine::sql` |
