# ISS-058 — `COALESCE` of a `varchar` and a SQL Server `char(n)` is not padded

- Kind: deliberate divergence · Accepted by: Fernando Farias, 2026-09-30 · Status: open
- Context: in Trino 431 the common type of an unbounded `varchar` and a `char(n)` is `char(65536)`.
  `COALESCE(p.id, q.id)` with `p.id varchar` and `q.id char(36)` is a `char(65536)`, and
  `CAST(COALESCE(p.id, q.id) AS varchar)` is the value followed by spaces up to 65 536 characters.
  CodeLake returns the value without that padding (36 characters). Found by the shadow run on the
  production model `validacao_detalhe`: its column `chave` is that expression, `p.id` the replica's
  key (Hive, `varchar`) and `q.id` SQL Server's (`char(36)`), and differed in the 2 629 rows of
  `vendahist`.
- Why CodeLake differs: a SQL Server `char(n)` or `nchar(n)` column is read as text, holding the
  value as SQL Server returns it (padded to `n`), not as the engine's `char(n)`
  (`sqlserver::declared_text` declares only `varchar(n)` and `nvarchar(n)`). The two arguments of
  `COALESCE` are then text, and there is no common `char` type to pad to. A `char(n)` of a Hive or
  in-memory table, or of a `CAST`, is the engine's `char(n)` and follows the reference, this
  padding included (ISS-024, ADR 0015 on char semantics; `tools/differential/cases/char.toml`).
- Accepted: CodeLake keeps the value without the spaces. Not inherited from Trino 431: the padding
  protects no data and takes 64 KiB for each value.
- Extended 2026-09-30 to every origin by ISS-059 (Accepted by: Fernando Farias, 2026-09-30): a
  `char(n)` of a Hive or in-memory table, of a `CAST` or of a common char type is no longer
  padded either, so `COALESCE` of a `varchar` and any `char(n)` is the value without trailing
  spaces, whatever the origin of the char value. The "Why CodeLake differs" above describes
  the state before that change.
- Risk: a consumer that depends on the padded value sees another result: its length (65 536 in
  Trino 431), text concatenated after it, the type a `CREATE TABLE AS` stores for the bare
  `COALESCE` (`char(65536)` in Trino 431).
- Shadow run: the harness has no rule for it (`declared_type`, `name_case` and
  `replaced_characters` do not cover trailing padding), so `validacao_detalhe` differs in those
  rows on every cycle until `chave` is compared without trailing spaces.
- Revisit when: a consumer depends on the padded value. The SQL Server connector declaring
  `char(n)` columns as `char(n)` would no longer add padding (ISS-059); it would drop the
  trailing spaces SQL Server stores, as every char value.
- References: shadow run of 2026-09-30, model `validacao_detalhe` (binary 23ae047);
  `sqlserver_column_pruning::every_scan_of_a_reconciliation_reads_only_the_columns_it_uses` (the
  model against SQL Server 2022 and Trino 431: the longest `chave` of `vendahist` is 65 536
  characters in Trino 431 and 36 in CodeLake).
