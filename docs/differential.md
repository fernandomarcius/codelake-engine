# Differential gate

CodeLake answers the same SQL as a reference engine, and a wrong answer is silent
([gates.md](gates.md)). The differential gate runs a set of recorded cases against
CodeLake and an **oracle**, and fails on any answer the case file does not expect:

```bash
heavy.sh scripts/dev.sh scripts/gates.sh differential              # every case file
heavy.sh scripts/dev.sh scripts/gates.sh differential --only char  # some files (comma-separated)
```

(`heavy.sh` is the lock parallel fronts share on one machine; alone, drop it.)

The gate runs the harness's unit tests, builds `codelake` (default features, as
shipped), then `tools/differential/run.py`, which starts what the cases need as
sibling containers through the Docker socket on a private network, with **no port
published on the host**, and removes them, the network and the warehouse volume when
it ends (also on failure):

| Engine | Image | Reached as |
|---|---|---|
| Trino 431 (oracle) | `trinodb/trino:431`, `hive` catalog on a file metastore | HTTP statement protocol |
| PostgreSQL 17 (oracle and source) | `postgres:17-alpine` | PostgreSQL wire; as a source, the `pg` catalog of both engines |
| CodeLake under test | the toolbelt image + the binary just built, `tools/differential/stack/codelake.toml` | Trino protocol (`[server.trino]`) and PostgreSQL wire |

When a case file needs PostgreSQL, both engines also read it as a source, through a catalog
named `pg` (Trino's `postgresql` connector, CodeLake's `postgres` connector) connected as the
role `source`, whose sessions run in `America/Sao_Paulo` (`stack/postgres-source.sql`). Such a
file creates its tables in the `source` schema with `setup_postgres` and compares CodeLake with
Trino reading the same rows (`postgres_pushdown.toml`). Every file's `setup_postgres` runs
before CodeLake starts: CodeLake lists a source's tables once, when it starts.

Trino and CodeLake share one warehouse volume, so a case can read with one engine
files the other wrote (`setup_trino` writes, CodeLake reads). Timings: about 1 min to
start Trino, then a few seconds per case file.

## Outcomes

| Outcome | Meaning |
|---|---|
| `OK` | same rows (and, for a Trino oracle over HTTP, the same column types), or the same error |
| `WRONG` | both answered, the rows differ as a multiset: a **wrong result** |
| `RECUSA` | the oracle answered, CodeLake refused |
| `EXTRA` | CodeLake answered, the oracle refused |
| `ERRO` | both refused, with different errors (Trino `errorName`, SQLSTATE) |
| `ORDEM` | same rows in another order, where the case asks for order |
| `TIPO` | same rows, other column types |
| `TRAVA` | CodeLake did not answer in time (15 s on the PostgreSQL wire, 280 s over HTTP) |

Values are compared exactly as the client decodes them: `-0.0` is not `0.0`, `1`
is not `1.0` nor `true`. Over the PostgreSQL wire against Trino, Trino's values are
first printed as PostgreSQL text (`t`/`f`, `1.5`, `-0`).

## Expectations: nothing divergent passes silently

A case expects `OK` unless it says otherwise. Any other expectation must say why,
with exactly one of:

- `known = "ISS-025"` — an **open defect**. The case passes while the defect is
  there, and the report lists it under *known open defects (NOT green)*.
- `accepted = "ISS-030 item 5"` — a **deliberate divergence** recorded in an issue
  or ADR. Listed under *accepted divergences*.

The reference must start with `ISS-NNN`, `ADR NNNN` or `front/<branch>` (the front
fixing a defect not yet recorded in `docs/issues`). The gate fails (exit 1) when:

- an outcome differs from the expectation: an unmarked `WRONG`, a regression, and
  also a known defect that is now fixed (drop the marker in the same change: the
  file then records the fix);
- a session case diverges at other steps than its `diverges_at` lists;
- a case with `error = "…"` is refused with an error that does not contain it;
- a setup statement fails on any engine.

Exit 2 is the harness itself failing (Docker, an engine that does not start); the
CodeLake log tail is printed.

## Case files

`tools/differential/cases/*.toml`, one theme per file, validated strictly (an
unknown key is an error). Each file uses its own schema names, since all files
share the engines.

```toml
# file level (all optional)
description = "…"
oracle = "trino"            # trino | postgres | value — default for the file's cases
protocol = "trino"          # CodeLake's side: trino (HTTP) | pg (wire); pg for the postgres oracle
compare = "multiset"        # multiset | ordered (ordered is the default for steps)
setup = ["CREATE SCHEMA {c}.chp", "…"]   # both engines; {c} = memory (Trino) / datafusion (CodeLake)
setup_trino = ["CREATE TABLE hive.chs.cht …"]   # Trino only: files CodeLake then reads
setup_codelake = ["…"]
setup_postgres = ["…"]

[[case]]
id = "CH10"                  # unique in the file
sql = "SELECT CAST('ab' AS char(5)) = 'ab '"
expect = "RECUSA"            # default OK
known = "ISS-024"            # or accepted = "…"
error = "is not supported yet"   # optional: CodeLake's refusal must contain it
note = "…"
```

Other case keys:

| Key | For | Meaning |
|---|---|---|
| `steps = ["BEGIN", "DECLARE …", "FETCH 1 FROM c"]` | postgres oracle | one session per engine, compared statement by statement (rows or SQLSTATE); the case's outcome is its worst step |
| `{ sql = "…", any_error = true }` | a step | a statement there only to fail (to abort a transaction block): any error on both sides matches, whatever its SQLSTATE; SQLSTATEs have their own cases |
| `diverges_at = [3, 5]` | steps | the 1-based steps expected to diverge |
| `params = ["ab"]`, `param_types = [25]` | pg protocol | extended protocol (Parse/Bind/Execute) with these text parameters and type OIDs (0 = unspecified) |
| `oracle_sql` | trino oracle | the SQL Trino runs when it differs (inlined parameters) |
| `rows_json = '[[1, null]]'` | value oracle | the expected rows (JSON: TOML has no null) |
| `codelake_session = "k=v"` | trino protocol | `X-Trino-Session` sent to CodeLake only; the `/*CLSESSION k=v*/ SQL` prefix does the same |
| `types = false` | trino oracle | do not compare column types |
| `compare` | any | as at file level |

`{e}` in SQL becomes `tr` for Trino and `cl` for CodeLake (one table per writer).

## Report

`tools/differential/out/report.json` (ignored by git): summary counts per outcome,
`failures`, `known`, `accepted`, `setup_errors`, and every case with both answers
(first 50 rows) and, for sessions, every step, stamped in the operator's time zone
(`scripts/dev.sh` passes the host's as `CODELAKE_HOST_TZ`). The console prints the same
summary.

## Part of the Definition of Done

A front that touches planning, pushdown, a connector, a client protocol or SQL
semantics runs `gates.sh differential` before delivering, and:

- adds a case for every wrong result it finds or fixes (the query that showed it,
  with the oracle that proves the right answer);
- when it fixes a known defect, drops the `known` marker of the cases that now pass
  (the gate fails until it does);
- never marks a new divergence `known` or `accepted` without the issue that records
  it.
