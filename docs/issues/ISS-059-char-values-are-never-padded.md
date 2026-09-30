# ISS-059 — `char(n)` values are never padded, from any origin

- Kind: deliberate divergence · Accepted by: Fernando Farias, 2026-09-30 · Status: open
- Context: Trino 431 shows a `char(n)` value padded with spaces to `n` characters, and so did
  the engine for a `char(n)` of a Hive table, an in-memory table or view, a `CAST`, a literal
  and every common char type of `COALESCE`/`CASE`/`UNION`/`USING` (ISS-024, ADR 0015):
  `COALESCE(varchar, char(36))` is a `char(65536)`, 65 536 characters per value. A SQL Server
  `char(n)` was not (ISS-058), so the same model answered differently by origin.
- Accepted: the engine never pads a `char(n)` value, whatever its origin: a char value is
  held, returned (both client protocols) and written without trailing spaces — the content
  without the spaces the reference adds. Not inherited from Trino 431: the padding protects no
  data and takes space (up to 64 KiB per value). No compatible mode.
- Kept as the reference: the declared type (`char(n)` in `typeof`, `DESCRIBE`, the client's
  column type, the metastore) and every comparison, observed on `trinodb/trino:431`:
  `=`, `<>`, `IN`, `IS DISTINCT FROM`, joins, `GROUP BY`/`DISTINCT`/set operations (trailing
  spaces never matter); `<`, `BETWEEN`, `ORDER BY` (of a query, a window, an aggregate),
  `min`/`max`, `min_by`/`max_by` by a char key, `greatest`/`least` (the values compared as if
  padded, so `'a' || chr(9)` sorts below `'a'`); `LIKE` (the pattern matched against the
  value padded to `n`: `CAST('ab' AS char(5)) LIKE 'ab %'` is true).
- Follows from the value (checked against Trino 431 by `char_semantics::tests`, `char_matrix`,
  `char_whitespace` and `tools/differential/cases/char.toml`, every difference accepted here):
  - `length(ch)` counts the characters of the value: `length(CAST('ab' AS char(5)))` is 2
    (Trino 431: 5, the declared length); `length(COALESCE(varchar, char(36)))` is the value's.
    Coherent with what the client receives: `length(x)` is the length of the text returned.
  - `CAST(ch AS varchar)` is the value (`'ab'`; Trino 431: `'ab   '`), and so is every
    function over it (`substr`, `to_utf8`, `strpos`, …): `substr(CAST('ab' AS char(5)), 2)` is
    `'b'` (Trino 431: `'b   '`).
  - `||` of a char value concatenates the texts as held: `CAST('ab' AS char(3)) || 'x'` is
    `'abx'` (Trino 431: `'ab x'`); other text keeps its own trailing spaces. The result is
    still `char(total)`, and the refusals of ADR 0015 stay (`char(n) || <unbounded varchar>`,
    `char(n) || NULL`).
  - A value converted to char (a `varchar` in a common char type, `CAST(' ab  ' AS char(5))`,
    an `INSERT` into a char column) loses its trailing spaces, which a char value never holds
    (Trino 431 drops them too before padding).
  - A cast of a char value to a number parses the value: `CAST(CAST('12' AS char(5)) AS
    bigint)` is 12, where Trino 431 refuses the cast (differential V2, as V1: ISS-032).
- Risk: a consumer that depends on the padded value sees another result: its length, text
  concatenated after it, a hash or hex of it, a comparison of `CAST(ch AS varchar)` with a
  padded literal (`CAST(CAST('ab' AS char(5)) AS varchar) = 'ab   '` is false). The shadow run
  compares with Trino 431, which pads: a char column it outputs differs by trailing spaces
  until its harness compares without them (ISS-058), now for every origin.
- Not covered: the SQL Server connector still reads `char(n)`/`nchar(n)` as text with the
  spaces SQL Server stores (not the engine's `char(n)`, ISS-058); an ordering of rows or arrays
  of char values that are not written out, `array_sort`/`array_min`/`array_max` of char values
  and `min`/`max` of a char value as a window function compare the values as held (a character
  below the space after a common prefix sorts as the reference's only when padded).
- Revisit when: a consumer depends on the padded value.
- References: ISS-024, ISS-058, ADR 0015 (amended 2026-09-30); `codelake_core::char_semantics`.
