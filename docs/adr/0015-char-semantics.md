# ADR 0015 — `char(n)` semantics instead of refusals

- Status: Accepted
- Date: 2026-09-25
- Related: ADR 0007 (hive catalog), ADR 0008 (function library), ADR 0014,
  ISS-024, ISS-025

## Context

Arrow has no char type. The hive catalog reads a `char(n)` column padded to
`n` characters, as Trino 431 shows it, and records `char(n)` in the field
metadata. Trino compares char values padded: for a `char(5)` holding `'ab'`,
`ch = 'ab'` and `ch = 'ab  '` are true, and `ch < 'ab' || chr(9)` is false
(the tab sorts below the padding space).

Until now the engine refused every comparison it recognized as one of char
values (`codelake_core::char_comparisons`, a black list of plan shapes), and
held a `CAST … AS char(n)` unpadded. Three rounds of independent verification
against Trino 431 found shapes the list missed, each answering wrong rows
without an error: `IN`/`NOT IN`/`= ANY` with the char value in the subquery,
scalar subqueries of char values, `CASE`/`coalesce` mixing char and text,
`CAST(CAST('ab' AS char(5)) AS varchar)` unpadded, `TRY_CAST(x AS varchar(n))`
not cutting, and a panic of `max(ch)` over an in-memory char table
(`scratchpad/verif-char-trino.log`, P1–P16). A black list cannot be complete;
the owner decided (ISS-024) to implement the semantics.

## Decision

`codelake_core::char_semantics`, an analyzer rule before type coercion,
gives char values Trino 431's semantics (every rule below observed on
`trinodb/trino:431`):

1. **Representation.** Every `char(n)` value is held padded with spaces to
   `n` characters: hive scans (as before), `CAST`/`TRY_CAST … AS char(n)`,
   `INSERT` and `UPDATE` of a char column (in-memory and hive; the hive
   writer still stores values without trailing spaces, as Trino), values
   the rule pads. Text keeps the Arrow type it came with (`Utf8View` stays
   `Utf8View`), which removes the `Utf8`×`Utf8View` panic of in-memory char
   tables.
2. **Types.** The rule follows the Trino type of every text value through
   the plan: declared fields, literals (`varchar(length)`), `CAST`s the
   statement wrote (marked `codelake_trino_type(…, '')`, so the casts type
   coercion adds between Arrow's text types keep their argument's type and
   a plan analyzed twice is rewritten once), aggregates (`max`, `min`,
   `first_value`, `min_by`, `max_by`, `array_agg`, the keys and values of
   `map_agg`, the keys of `histogram`, …), `try`, window functions, `CASE` and the functions
   whose result length Trino derives from their arguments (`upper`, `rtrim`,
   `substr`, `concat`, …). A projection records `varchar(n)` in the field
   of text it computes from a char value (`rtrim(ch)`, `substr(ch, 2)`,
   `nullif(v3, ch)`: `varchar(5)`, `varchar(3)`, as Trino types them), and
   `char(n)` in the field of
   each char column it computes (a client, `typeof`, `CREATE TABLE … AS` and
   views see it) and removes it from a text a cast made from a char value. A
   scalar subquery computed ahead (`crate::subquery`) becomes a literal that
   records its char type.
3. **Common type.** `char(n)` with `char(m)` is `char(max)`; with `varchar(k)`
   `char(max(n, k))`; with unbounded `varchar` `char(65536)`; `NULL` takes
   the other type.
4. **Comparisons** (`=`, `<>`, `<`…, `IS [NOT] DISTINCT FROM`, `IN` lists,
   `BETWEEN`, simple `CASE`, `nullif`, join conditions, `IN (subquery)`,
   rows and arrays written out, `contains`/`array_position`) compare the
   values padded to the common length: equality on the values without
   trailing spaces (`codelake_char_key`), ordering with
   `codelake_char_compare` (character by character, the shorter padded with
   spaces), a text literal short enough padded to the char's length instead.
   A key looked up in a map keyed by char values (`histogram(c3)['a']`,
   `element_at(map_agg(ch, id), 'ab')`) is compared as that char.
   `x op ANY/ALL (subquery)` becomes the `EXISTS` tests it means, `NULL`
   alike: DataFusion 54 cannot plan the mark join it makes of a quantified
   comparison over a computed column.
   Rows and arrays written out (also as the planner folds them into
   literals) are compared value by value: `=` is false when a pair differs
   and `NULL` when none differs but a pair is `NULL` (a `NULL` field never
   makes rows equal, char or not); `<>` is its negation; `IS DISTINCT FROM`
   holds when a pair is distinct; arrays of different lengths differ.
   Rows and arrays not written out (a subquery's or a CTE's column, a
   table's, `array_agg`, a `CASE` of rows) are compared the same way at run
   time, char or not (`codelake_nested_test`, module `nested_values`): the
   rule follows the type of every value inside rows and arrays through the
   plan (a `Shape`) and marks the pairs holding a char value, compared
   without trailing spaces. So are join conditions on such values (a join
   on rows or arrays is planned as a nested-loop join), their `IN` lists
   (a constant list of several items looked up as a set, as below).
   `x IN (a, b)` is `x = a OR x = b`, except that a constant list of
   several items without `NULL`s is a set lookup in Trino 431: a row
   holding a `NULL` is then `NULL` (`(s, id) IN (('ab', 1), ('cd', 3))` for
   `s` `NULL`, `id` 4), and so it is in the engine.
5. **Values in a common type** are padded to its length: `CASE` branches,
   `coalesce`, `nvl`, `ifnull`, `nvl2`, `greatest`, `least`, the default of
   `lag`/`lead`, array elements (also of arrays concatenated with `||` or
   `concat`, and the elements and value of `array_remove`), the columns of `UNION`, `INTERSECT`,
   `EXCEPT`, `VALUES` and the key of a `USING` join (Trino outputs the
   `USING` column in the common type: `char(65536)` against unbounded
   `varchar`, padded to 65536 characters). Char values inside rows and
   arrays are padded to the common type of the rows or arrays too: the
   branches of `CASE`, `coalesce` and the like, the elements of an array of
   arrays (`flatten`), both arrays of `arrays_overlap`, `array_intersect`,
   `array_union` and `array_except`, and the columns of `UNION`
   (`codelake_nested_pad` where they are not written out).
6. **Concatenation** of a char value is `char(total)`, every other operand
   padded to its own length (`char(3) || varchar(2)` is `char(5)`).
7. **Functions** see the padded text: `upper`/`lower` are `char(n)`,
   `substr` is `varchar(n)` of the padded text, `LIKE` matches it,
   `length` counts it. The trims (`ltrim`, `rtrim`, `trim`, with or without
   the characters to remove) are `varchar(n)` of the value without its
   padding: `ltrim(CAST(' ab' AS char(5)))` is `'ab'`, `ltrim(ch, 'a')` of
   `'ab'` is `'b'`; trimming other characters from its end drops the spaces
   that end up last too (`rtrim(CAST('a b' AS char(5)), 'b')` is `'a'`).
   `typeof` of a constant answers its type (`typeof('a')` is
   `varchar(1)`, `typeof(CAST('ab' AS char(3)))` `char(3)`). Functions Trino has no char version of (`strpos`,
   `replace`, …; Trino refuses them) see the padded text, as
   `CAST(ch AS varchar)` gives it.
8. **Scans** never refuse a filter on a char column any more: the filter is
   applied above the scan (pushdown `Inexact`), and statistics never prune a
   file by it (the scan rewrites the column to its padded value, which the
   statistics of the stored text cannot answer); a table with a file per
   value is part of the matrix.
9. `TRY_CAST(x AS varchar(n) | char(n) | timestamp(p))` answers what `CAST`
   answers, `NULL` where `CAST` fails (P13).

### Refusals that remain

Where Trino 431 fails too:

- `NATURAL JOIN` ("Natural join not supported"), refused before planning,
  whatever the columns;
- `char(n) || <unbounded varchar>` (Trino: "Unknown type: char(65541)"),
  `char(n) || NULL` ("Could not choose a best candidate operator"), and a
  concatenation longer than `char(65536)`.

Where the engine cannot guarantee Trino's answer (documented, each an
error naming a way around it):

- a char value output together with text whose length the rule does not
  know (`CASE WHEN … THEN ch ELSE element_at(t.arr, 1) END` over an array
  column, `to_iso8601(timestamp)`): Trino
  types it `char(max(n, k))` with a `k` the rule does not derive; comparing
  such values is answered. The lengths Trino derives for text functions are
  followed where observed (`typeof` on `trinodb/trino:431`):
  `replace(varchar(x), y, varchar(z))` is `varchar(x + z·(x + 1))`,
  `regexp_replace(varchar(x), p, varchar(y))` `varchar(x + max(x·y/2, y)·(x + 1))`,
  `url_encode(varchar(x))` `varchar(12x)`, `soundex` `varchar(4)`; the two-argument
  `replace`/`regexp_replace`, `json_extract_scalar`, `url_decode` and the
  `url_extract_*` functions keep `varchar(x)`; an element (`element_at`,
  `[i]`) of `split`, `regexp_split` or `regexp_extract_all` of `varchar(x)`
  is `varchar(x)`; `to_iso8601(date)` is `varchar(16)`; `from_utf8`,
  `to_base64url`, `human_readable_seconds`, `current_timezone`, `version`
  and `||` of text without char values are unbounded `varchar`; a length
  past 2147483646 is unbounded `varchar`;
- a char value searched in, or removed from, an array of text not written
  out, text other than a literal searched in an array of char values not
  written out, and an array of char values not written out concatenated to
  an array of another element type: the element
  type of an array is followed through `ARRAY[…]`, `array_agg`, `||` and
  `concat` of arrays, the functions that keep the elements (`array_sort`,
  `array_distinct`, `array_remove`, `slice`, `reverse`, …) and those that
  pick one (`[i]`, `element_at`, `array_max`, `array_min`: a `char(n)`
  value), not through every array function;
- on the PostgreSQL wire, a `char(n)` column (`CREATE TABLE`, `ADD COLUMN`)
  or a cast to `char(n)`/`bpchar` (`0A000`): statements there run on the
  session as `DataFusion` plans them, without the engine's preparation, so
  the column would be unbounded text for every protocol and the cast would
  not pad. A `char(n)` table created through the Trino protocol reads padded
  on the wire (reported as `text`). `varchar(n)` keeps its length there
  (`codelake_core::PostgresStatement`): a cast to it is rewritten as the
  engine does, and a `CREATE TABLE` records its `varchar(n)` columns, whose
  `INSERT`s are then checked on both protocols;
- a cast to an array, map or row of `char(n)` or `varchar(n)`
  (`CAST(ARRAY[v] AS array(char(3)))`): Trino pads or cuts each value, the
  conversion does not;
- an ordering of rows or arrays of char values whose common length is not
  known, and `ANY`/`ALL` of a value that holds an aggregate or a subquery.

### Where the reference engine is wrong

- `EXISTS (SELECT 1 FROM v WHERE v.s = t.ch)` repeats a row of `t` once per
  distinct `varchar` value of `v` equal to it as char (Trino 431 joins on the
  distinct varchar values before converting them to char). The engine
  answers each row once, as `EXISTS` means.
- `VALUES ((SELECT CAST('ab' AS char(5)))), ('ab ')` fails in Trino 431
  ("Unexpected subquery expression in logical plan"); the engine answers
  what the same `VALUES` answers without the subquery.

## Consequences

- Breaking for queries that were refused (they now answer Trino's rows), for
  `NATURAL JOIN` over any columns (now refused, as Trino), and for char values
  a `CAST` computed (now padded: `length(CAST('ab' AS char(5)))` is 5).
- The `USING` key against unbounded `varchar` and the other `char(65536)`
  results hold 65,536 characters per value, as Trino's.
- Found on the way, not a char defect: DataFusion 54 fails to plan
  `x < ALL (SELECT c FROM <hive table> WHERE c IS NOT NULL)` for any text
  column (ProjectionPushdown: "Input field name c does not match with the
  projection expression"). It fails loudly and is left to the DataFusion
  54 guards (ADR 0014).

## Verification

- Differential (round 5): the verifier's probes s1–s7 (HTTP and PostgreSQL
  wire, hive and in-memory origins) rerun against `trinodb/trino:431` answer
  no other rows, but for ties of `ORDER BY … LIMIT` and `ROWS` frames and
  the rows of statements refused in the probes' own setup (an `INSERT` of a
  `ROW(…)` into a `row(x …, y …)` column; the same queries over the table
  written with `CAST(ROW(…) AS row(…))` answer Trino's rows); types of arrays
  and rows of char values are still reported as `varchar`. Where Trino 431
  fails comparing stored rows or arrays that hold a `NULL` ("ROW comparison
  not supported for fields with null elements"), the engine answers with
  the semantics above.
- Unit (`char_semantics::tests`): 100 queries over in-memory tables, each with
  the rows Trino 431 answers to the same SQL in its memory catalog; the
  engine's own storage (in-memory `INSERT`/`UPDATE`, CTAS, views); the
  refusals.
- Integration (`hive_redteam_w1`, against `trinodb/trino:431` on one
  warehouse): CH1–CH16 and P1–P16; and the matrix `char_matrix`: operations
  × operand (`char(3)`, `char(5)`, `varchar`, `varchar(3)`, six literals,
  `NULL`) × origin of the char value (hive table Trino wrote, in-memory
  table, literals, subquery, view Trino wrote, hive table the engine wrote)
  — 1656 row operations and 384 set operations, joins and subqueries, every
  one Trino's rows or refused by both.
