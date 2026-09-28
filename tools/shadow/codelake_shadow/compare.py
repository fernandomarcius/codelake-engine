"""Table-by-table comparison of two warehouses with DuckDB over Parquet.

Each table is read from ``<warehouse>/<schema>/<table>/`` (the file-metastore
layout: one directory per schema and per table; dotfiles and ``_``-prefixed
files are metadata or in-flight writes and are skipped).

Per table and side, one pass computes:

- the row count;
- a checksum that ignores row order and counts duplicates: the sum of a
  128-bit hash (two 64-bit halves of MD5) of each row's canonical text;
- one checksum per column (sum of the hash of that column's canonical
  value), which says *which* columns differ when the row checksums do;
- the count of NULLs per column.

Canonical text of a value (see ``canonical_expr``):

- NULL is ``~``, distinct from every non-NULL value (so NULL != '');
- a string is itself, exactly: no trimming, no case folding;
- integers and decimals by value: ``12.500000`` and ``12.5`` are equal;
- timestamps as nanoseconds since the epoch of the wall-clock value the
  engines read: an ``INT64`` timestamp as stored (naive values read as UTC),
  an ``INT96`` timestamp converted from the UTC instant it stores to the
  Parquet time zone of the hive catalogs (``parquet_time_zone``), as both
  engines read it. ``INT96`` without a configured zone is not compared;
- binary as hex; dates, times, booleans and floats as their exact text.

Every non-NULL value is length-prefixed and tagged with its class, so the
concatenation of a row's values is unambiguous.

Rows that differ are shown only as hashes with their count on each side.
Their values are printed only with ``show_rows`` (opt-in), because the
tables hold production data.

Schema, all of it a divergence when it differs (unless an expected-difference
rule documents a declared type or a name's case, ``expected.py``):

- the metastore file (``dataColumns``) present on one side only;
- declared columns: names, order (``SELECT *`` and a positional ``UNION``
  depend on it) and types, from the metastore file of both sides, or from
  the Parquet schema of both sides when neither has one; a table without
  data files is still compared on them;
- Parquet columns and their type class;
- Parquet storage of a column: the type DuckDB reads (a decimal's precision
  and scale, a timestamp's unit) and a timestamp's encoding (``INT96``, or
  ``INT64`` with its unit and UTC adjustment). Another reader of the files
  sees another value when it differs, even when both engines do not;
- a table or column name that differs only by case.

Table directories are found without regard to case. Partitioned tables (files
in subdirectories, or partition columns in the metastore) are refused, never
compared as empty.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import json
import os
import re
from typing import Any

import duckdb

from . import expected as expected_rules

# Table metadata file of the file metastore: a compatibility name, the layout
# both engines write (docs/adr/0007).
# Metadata file of a table: the reference engine's name (Trino 431's file
# metastore) and CodeLake's default (its `metastore_names`, ISS-011). Each
# side's name is configured (SHADOW_<SIDE>_METASTORE_FILE).
METASTORE_FILE = ".trinoSchema"
CANDIDATE_METASTORE_FILE = ".codelakeSchema"

# DuckDB type families, by the prefix of the type name DESCRIBE reports.
_INTEGER_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
}
_FLOAT_TYPES = {"FLOAT", "DOUBLE", "REAL"}


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def type_class(duck_type: str) -> str:
    """Comparison class of a DuckDB type: N, F, B, T, D, H, X, S or O."""
    t = duck_type.upper()
    base = t.split("(")[0].strip()
    if base in _INTEGER_TYPES or base == "DECIMAL":
        return "N"
    if base in _FLOAT_TYPES:
        return "F"
    if base == "BOOLEAN":
        return "B"
    if base.startswith("TIMESTAMP"):
        return "T"
    if base == "DATE":
        return "D"
    if base.startswith("TIME"):
        return "H"
    if base in {"BLOB", "BYTEA", "VARBINARY", "BINARY"}:
        return "X"
    if base in {"VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR"}:
        return "S"
    return "O"


def _nanos(col: str, duck_type: str, int96_zone: str | None = None) -> str:
    """Nanoseconds since the epoch (HUGEINT) of the wall-clock value of any
    timestamp type. ``int96_zone``: the column is ``INT96``, whose stored UTC
    instant the engines read as the wall-clock time in that zone."""
    t = duck_type.upper()
    if int96_zone is not None:
        wall = f"timezone({quote_literal(int96_zone)}, timezone('UTC', CAST({col} AS TIMESTAMP)))"
        return f"(CAST(epoch_us({wall}) AS HUGEINT) * 1000)"
    if t.startswith("TIMESTAMP_NS"):
        return f"CAST(epoch_ns({col}) AS HUGEINT)"
    if t.startswith("TIMESTAMP_MS"):
        return f"(CAST(epoch_ms({col}) AS HUGEINT) * 1000000)"
    if t.startswith("TIMESTAMP_S"):
        return f"(CAST(epoch({col})::BIGINT AS HUGEINT) * 1000000000)"
    # TIMESTAMP (microseconds) and TIMESTAMP WITH TIME ZONE (UTC instant).
    return f"(CAST(epoch_us({col}) AS HUGEINT) * 1000)"


def canonical_expr(
    name: str, duck_type: str, int96_zone: str | None = None, replace_class: str = ""
) -> str:
    """SQL for the canonical text of one column; never NULL. ``replace_class``
    (a regular expression): each matching character of a text value becomes
    U+FFFD first (a ``replaced_characters`` rule, ``expected.py``)."""
    col = quote_ident(name)
    cls = type_class(duck_type)
    if replace_class and cls == "S":
        col = f"regexp_replace({col}, {quote_literal(replace_class)}, {quote_literal(chr(0xFFFD))}, 'g')"
    if cls == "N":
        text = f"CAST({col} AS VARCHAR)"
        # Drop trailing zeros of the fraction, then a bare point; -0 is 0.
        text = (
            f"regexp_replace(regexp_replace({text}, '(\\.[0-9]*?)0+$', '\\1'), '\\.$', '')"
        )
        text = f"(CASE WHEN {text} = '-0' THEN '0' ELSE {text} END)"
    elif cls == "F":
        text = f"CAST({col} AS VARCHAR)"
    elif cls == "B":
        text = f"(CASE WHEN {col} THEN '1' ELSE '0' END)"
    elif cls == "T":
        text = f"CAST({_nanos(col, duck_type, int96_zone)} AS VARCHAR)"
    elif cls == "X":
        text = f"hex({col})"
    else:
        text = f"CAST({col} AS VARCHAR)"
    value = f"'{cls}' || {text}"
    return (
        f"(CASE WHEN {col} IS NULL THEN '~' "
        f"ELSE CAST(length({value}) AS VARCHAR) || ':' || {value} END)"
    )


class TableError(ValueError):
    """A table that cannot be compared (the message never holds values)."""


@dataclasses.dataclass
class Location:
    """Where one side's tables are: a local directory or an s3:// prefix."""

    warehouse: str
    metastore_file: str = METASTORE_FILE

    def table_dir(self, schema: str, table: str) -> str:
        return self.warehouse.rstrip("/") + "/" + schema.lower() + "/" + table.lower()

    @property
    def is_s3(self) -> bool:
        return self.warehouse.startswith("s3://")


@dataclasses.dataclass
class TableDir:
    """A table's directory, with the schema and table names as written on disk."""

    path: str
    schema: str
    table: str
    metastore_file: str = METASTORE_FILE


def _visible(name: str) -> bool:
    return not (name.startswith(".") or name.startswith("_"))


def _entry(parent: str, name: str) -> str | None:
    """The one directory entry of ``parent`` named ``name`` in any case."""
    try:
        found = [e for e in os.listdir(parent) if e.lower() == name.lower() and os.path.isdir(os.path.join(parent, e))]
    except OSError:
        return None
    if len(found) > 1:
        raise TableError("two directories whose names differ only by case")
    return found[0] if found else None


def find_table(con: duckdb.DuckDBPyConnection, location: Location, schema: str, table: str) -> TableDir | None:
    """The table's directory, matched without regard to case; None when absent."""
    root = location.warehouse.rstrip("/")
    if location.is_s3:
        for s, t in dict.fromkeys(((schema, table), (schema.lower(), table.lower()))):
            path = f"{root}/{s}/{t}"
            if con.execute("SELECT count(*) FROM glob(?)", [path + "/**"]).fetchone()[0]:
                return TableDir(path, s, t, location.metastore_file)
        return None
    schema_name = _entry(root, schema)
    if schema_name is None:
        return None
    table_name = _entry(os.path.join(root, schema_name), table)
    if table_name is None:
        return None
    return TableDir(os.path.join(root, schema_name, table_name), schema_name, table_name, location.metastore_file)


def data_files(con: duckdb.DuckDBPyConnection, directory: TableDir) -> list[str]:
    """Data files of a table directory. A partitioned table is refused."""
    if directory.path.startswith("s3://"):
        rows = con.execute("SELECT file FROM glob(?)", [directory.path + "/**"]).fetchall()
        files = []
        for (path,) in rows:
            relative = path[len(directory.path) + 1 :]
            if "/" in relative and _visible(relative.split("/", 1)[0]):
                raise TableError("partitioned table (files in subdirectories): not compared")
            if "/" not in relative and _visible(relative):
                files.append(path)
        return sorted(files)
    files = []
    for name in os.listdir(directory.path):
        path = os.path.join(directory.path, name)
        if not _visible(name):
            continue
        if os.path.isdir(path):
            raise TableError("partitioned table (files in subdirectories): not compared")
        if os.path.isfile(path):
            files.append(path)
    return sorted(files)


def declared_columns(con: duckdb.DuckDBPyConnection, directory: TableDir) -> dict[str, tuple[str, str]] | None:
    """Declared columns of the table's metastore file: lower-case name ->
    (name as written, type). None when the file is absent or unreadable."""
    path = directory.path.rstrip("/") + "/" + directory.metastore_file
    try:
        if directory.path.startswith("s3://"):
            row = con.execute("SELECT content FROM read_text(?)", [path]).fetchone()
            text = row[0] if row else None
        else:
            with open(path, encoding="utf-8") as f:
                text = f.read()
        metadata = json.loads(text) if text else None
    except (OSError, ValueError, duckdb.Error):
        return None
    if not isinstance(metadata, dict) or not isinstance(metadata.get("dataColumns"), list):
        return None
    if metadata.get("partitionColumns"):
        raise TableError("partitioned table (partition columns in the metastore): not compared")
    columns: dict[str, tuple[str, str]] = {}
    for column in metadata["dataColumns"]:
        if isinstance(column, dict) and isinstance(column.get("name"), str) and isinstance(column.get("type"), str):
            columns[column["name"].lower()] = (column["name"], column["type"])
    return columns


def _relation(files: list[str]) -> str:
    listed = ", ".join(quote_literal(f) for f in files)
    return f"read_parquet([{listed}], union_by_name = true)"


def describe(con: duckdb.DuckDBPyConnection, files: list[str]) -> dict[str, tuple[str, str]]:
    """Column name (lower case) -> (name as written in the files, DuckDB type)."""
    rows = con.execute(f"DESCRIBE SELECT * FROM {_relation(files)}").fetchall()
    return {r[0].lower(): (r[0], r[1]) for r in rows}


def ignored(table: str, column: str, patterns: list[str]) -> bool:
    """Whether ``schema.table.column`` (or ``table.column``) matches a pattern."""
    for pattern in patterns:
        pattern = pattern.strip().lower()
        if not pattern:
            continue
        parts = pattern.split(".")
        if len(parts) == 3:
            target = f"{table}.{column}"
        elif len(parts) == 2:
            target = f"{table.split('.', 1)[-1]}.{column}"
        else:
            target = column
        if fnmatch.fnmatchcase(target, pattern):
            return True
    return False


def timestamp_encoding(physical: str | None, logical: str | None, converted: str | None) -> str | None:
    """How a Parquet column stores a timestamp: ``INT96``, or ``INT64 <unit>
    <utc|local>``; None when the column is not a timestamp."""
    if physical == "INT96":
        return "INT96"
    text = logical or ""
    if text.startswith("TimestampType"):
        if "NANOS=NanoSeconds" in text:
            unit = "NANOS"
        elif "MICROS=MicroSeconds" in text:
            unit = "MICROS"
        else:
            unit = "MILLIS"
        return f"{physical} {unit} {'utc' if 'isAdjustedToUTC=1' in text else 'local'}"
    if converted in {"TIMESTAMP_MICROS", "TIMESTAMP_MILLIS"}:
        # The legacy converted types are UTC-adjusted by definition.
        return f"{physical} {converted.split('_', 1)[1]} utc"
    return None


@dataclasses.dataclass
class FileGroup:
    """Data files of one side that store the same columns as ``INT96``."""

    files: list[str]
    columns: dict[str, tuple[str, str]]
    int96: frozenset[str]


@dataclasses.dataclass
class SideData:
    """One side's data files: their columns (union by name), the files grouped
    by ``INT96`` columns, and each timestamp column's storage encodings."""

    files: list[str]
    columns: dict[str, tuple[str, str]]
    groups: list[FileGroup]
    encodings: dict[str, set[str]]

    def storage(self, column: str) -> str:
        text = self.columns[column][1]
        if self.encodings.get(column):
            text += " [" + ", ".join(sorted(self.encodings[column])) + "]"
        return text


def read_side(con: duckdb.DuckDBPyConnection, files: list[str]) -> SideData:
    """Columns and storage of a side's data files (at least one file)."""
    columns = describe(con, files)
    listed = ", ".join(quote_literal(f) for f in files)
    rows = con.execute(
        f"SELECT file_name, name, type, logical_type, converted_type FROM parquet_schema([{listed}])"
    ).fetchall()
    int96: dict[str, set[str]] = {f: set() for f in files}
    encodings: dict[str, set[str]] = {}
    for file_name, name, physical, logical, converted in rows:
        if file_name not in int96:
            raise TableError("the Parquet schema names a file that was not listed: not compared")
        lower = (name or "").lower()
        if lower not in columns:
            continue
        encoding = timestamp_encoding(physical, logical, converted)
        if encoding is None:
            continue
        encodings.setdefault(lower, set()).add(encoding)
        if encoding == "INT96":
            int96[file_name].add(lower)
    by_set: dict[frozenset[str], list[str]] = {}
    for f in files:
        by_set.setdefault(frozenset(int96[f]), []).append(f)
    if len(by_set) == 1:
        (only, group_files), = by_set.items()
        groups = [FileGroup(group_files, columns, only)]
    else:
        groups = [FileGroup(fs, describe(con, fs), key) for key, fs in by_set.items()]
    return SideData(files, columns, groups, encodings)


def canonical_relation(
    side: SideData, columns: list[str], zone: str, replace: dict[str, str] | None = None
) -> str:
    """SQL of the side's rows as canonical texts ``k0..kn`` of ``columns``
    (lower-case names), one SELECT per file group; ``replace``: column ->
    character class replaced with U+FFFD (see ``canonical_expr``)."""
    parts = []
    for group in side.groups:
        items = []
        for i, column in enumerate(columns):
            if column not in group.columns:
                items.append(f"'~' AS k{i}")
                continue
            name, duck_type = group.columns[column]
            int96_zone = None
            if column in group.int96 and type_class(duck_type) == "T":
                if not zone:
                    raise TableError(
                        f"column {column} is stored as INT96: its value depends on the hive catalogs' "
                        "Parquet time zone, which is not configured (SHADOW_PARQUET_TIME_ZONE, "
                        "--parquet-time-zone): not compared"
                    )
                int96_zone = zone
            replace_class = (replace or {}).get(column, "")
            items.append(f"{canonical_expr(name, duck_type, int96_zone, replace_class)} AS k{i}")
        if not items:
            items.append("1 AS k_")
        parts.append(f"SELECT {', '.join(items)} FROM {_relation(group.files)}")
    return " UNION ALL ".join(parts)


def _row_expr(columns: list[str]) -> str:
    if not columns:
        return "''"
    parts = [f"{quote_literal(name + '=')} || k{i}" for i, name in enumerate(columns)]
    return " || '|' || ".join(parts)


@dataclasses.dataclass
class SideStats:
    rows: int
    checksum: str
    columns: dict[str, str]
    nulls: dict[str, int]


def side_stats(con: duckdb.DuckDBPyConnection, relation: str, columns: list[str]) -> SideStats:
    row = _row_expr(columns)
    col_sums = []
    null_counts = []
    for i, name in enumerate(columns):
        piece = f"{quote_literal(name + '=')} || k{i}"
        col_sums.append(f"sum(md5_number_lower({piece}))::VARCHAR AS c{i}")
        null_counts.append(f"count(*) FILTER (WHERE k{i} = '~') AS n{i}")
    select = ", ".join(
        [
            "count(*) AS n",
            f"coalesce(sum(md5_number_lower({row})), 0)::VARCHAR AS lo",
            f"coalesce(sum(md5_number_upper({row})), 0)::VARCHAR AS hi",
        ]
        + col_sums
        + null_counts
    )
    result = con.execute(f"SELECT {select} FROM ({relation}) AS r").fetchone()
    assert result is not None
    n, lo, hi = result[0], result[1], result[2]
    k = len(columns)
    sums = result[3 : 3 + k]
    nulls = result[3 + k : 3 + 2 * k]
    return SideStats(
        rows=int(n),
        checksum=f"{lo}:{hi}",
        columns={name: str(s or 0) for name, s in zip(columns, sums)},
        nulls={name: int(c) for name, c in zip(columns, nulls)},
    )


def differing_rows(
    con: duckdb.DuckDBPyConnection,
    ref_relation: str,
    cand_relation: str,
    columns: list[str],
    limit: int,
    show_rows: bool,
) -> tuple[int, list[dict[str, Any]]]:
    """Hashes whose row count differs between sides: (total, sample)."""
    row = _row_expr(columns)
    keep = ", any_value(r) AS r" if show_rows else ""
    sql = f"""
        WITH ref AS (
            SELECT md5(r) AS h, count(*) AS n{keep}
            FROM (SELECT {row} AS r FROM ({ref_relation}) AS x) GROUP BY 1
        ), cand AS (
            SELECT md5(r) AS h, count(*) AS n{keep}
            FROM (SELECT {row} AS r FROM ({cand_relation}) AS x) GROUP BY 1
        ), diff AS (
            SELECT coalesce(ref.h, cand.h) AS h,
                   coalesce(ref.n, 0) AS ref_n,
                   coalesce(cand.n, 0) AS cand_n
                   {", coalesce(ref.r, cand.r) AS r" if show_rows else ""}
            FROM ref FULL OUTER JOIN cand ON ref.h = cand.h
            WHERE coalesce(ref.n, 0) <> coalesce(cand.n, 0)
        )
        SELECT (SELECT count(*) FROM diff) AS total, * FROM diff ORDER BY h LIMIT {int(limit)}
    """
    rows = con.execute(sql).fetchall()
    total = int(rows[0][0]) if rows else 0
    sample = []
    for r in rows:
        item: dict[str, Any] = {"hash": r[1], "reference": int(r[2]), "candidate": int(r[3])}
        if show_rows:
            item["row"] = r[4]
        sample.append(item)
    return total, sample


@dataclasses.dataclass
class Options:
    ignore_columns: list[str] = dataclasses.field(default_factory=list)
    sample: int = 10
    show_rows: bool = False
    expected: list[expected_rules.Rule] = dataclasses.field(default_factory=list)
    # IANA zone the hive catalogs of both engines convert INT96 timestamps
    # with (``parquet_time_zone`` / ``hive.parquet.time-zone``).
    parquet_time_zone: str = ""


def _classify(
    options: Options,
    kind: str,
    table: str,
    column: str,
    item: dict[str, Any],
    expected_out: list[dict[str, Any]],
    unexpected_out: list[dict[str, Any]],
) -> None:
    """Files a type or name-case difference as expected (a rule documents it)
    or unexpected (a divergence)."""
    rule = expected_rules.match(
        options.expected, kind, table, column, item.get("reference", ""), item.get("candidate", "")
    )
    if rule is None:
        unexpected_out.append(item)
    else:
        expected_out.append(dict(item, kind=kind, rule=rule.id, reason=rule.reason))


def compare_table(
    con: duckdb.DuckDBPyConnection,
    reference: Location,
    candidate: Location,
    schema: str,
    table: str,
    options: Options,
) -> dict[str, Any]:
    """Compares one table; the result is JSON-ready."""
    name = f"{schema.lower()}.{table.lower()}"
    out: dict[str, Any] = {"table": name}
    try:
        return _compare_table(con, reference, candidate, schema, table, options, out)
    except TableError as err:
        out["status"] = "error"
        out["error"] = str(err)
        return out


def _compare_table(
    con: duckdb.DuckDBPyConnection,
    reference: Location,
    candidate: Location,
    schema: str,
    table: str,
    options: Options,
    out: dict[str, Any],
) -> dict[str, Any]:
    name = out["table"]
    ref_dir = find_table(con, reference, schema, table)
    cand_dir = find_table(con, candidate, schema, table)
    if ref_dir is None or cand_dir is None:
        if ref_dir is None and cand_dir is None:
            out["status"] = "missing_both"
        else:
            out["status"] = "missing_reference" if ref_dir is None else "missing_candidate"
        return out
    ref_files = data_files(con, ref_dir)
    cand_files = data_files(con, cand_dir)
    ref_declared = declared_columns(con, ref_dir)
    cand_declared = declared_columns(con, cand_dir)
    out["files"] = {"reference": len(ref_files), "candidate": len(cand_files)}
    reasons: list[str] = []
    expected: list[dict[str, Any]] = []
    name_case: list[dict[str, Any]] = []

    ref_table, cand_table = f"{ref_dir.schema}.{ref_dir.table}", f"{cand_dir.schema}.{cand_dir.table}"
    if ref_table != cand_table:
        item = {"object": "table", "reference": ref_table, "candidate": cand_table}
        _classify(options, "name_case", name, "", item, expected, name_case)

    # A metastore file on one side only: the declared schema of the other
    # side is unknown, never replaced by its Parquet types.
    if (ref_declared is None) != (cand_declared is None):
        reasons.append("metastore")
        out["metastore"] = {"reference": ref_declared is not None, "candidate": cand_declared is not None}

    ref_side = read_side(con, ref_files) if ref_files else None
    cand_side = read_side(con, cand_files) if cand_files else None

    schema_out: dict[str, Any] = {}
    class_mismatch: list[str] = []
    compared: list[str] = []
    skipped: list[str] = []
    if ref_side is not None and cand_side is not None:
        ref_types = {c: t for c, (_, t) in ref_side.columns.items()}
        cand_types = {c: t for c, (_, t) in cand_side.columns.items()}
        only_ref = [c for c in ref_types if c not in cand_types]
        only_cand = [c for c in cand_types if c not in ref_types]
        common = [c for c in ref_types if c in cand_types]
        skipped = [c for c in common if ignored(name, c, options.ignore_columns)]
        compared = [c for c in common if c not in skipped]
        class_mismatch = [c for c in compared if type_class(ref_types[c]) != type_class(cand_types[c])]
        if only_ref or only_cand:
            reasons.append("columns")
        if class_mismatch:
            reasons.append("column_types")
        schema_out.update(
            {
                "only_reference": sorted(only_ref),
                "only_candidate": sorted(only_cand),
                "type_class_mismatch": [
                    {"column": c, "reference": ref_types[c], "candidate": cand_types[c]} for c in class_mismatch
                ],
            }
        )

    # Declared columns: the metastore of both sides, else (no metastore on
    # either side) the Parquet schema of both sides.
    from_metastore = ref_declared is not None and cand_declared is not None
    if from_metastore:
        ref_decl, cand_decl = ref_declared, cand_declared
    elif ref_side is not None and cand_side is not None:
        ref_decl, cand_decl = ref_side.columns, cand_side.columns
    else:
        ref_decl = cand_decl = None
    type_differences: list[dict[str, Any]] = []
    if ref_decl is not None and cand_decl is not None:
        schema_out["declared_types_from"] = "metastore" if from_metastore else "parquet"
        decl_only_ref = [c for c in ref_decl if c not in cand_decl]
        decl_only_cand = [c for c in cand_decl if c not in ref_decl]
        if decl_only_ref or decl_only_cand:
            reasons.append("columns")
        if from_metastore:
            schema_out["declared_only_reference"] = sorted(decl_only_ref)
            schema_out["declared_only_candidate"] = sorted(decl_only_cand)
        ref_order = [c for c in ref_decl if c in cand_decl]
        cand_order = [c for c in cand_decl if c in ref_decl]
        if ref_order != cand_order:
            reasons.append("column_order")
            schema_out["column_order"] = {"reference": ref_order, "candidate": cand_order}
        for c in ref_order:
            (ref_name, ref_type), (cand_name, cand_type) = ref_decl[c], cand_decl[c]
            if ref_name != cand_name:
                item = {"object": "column", "column": c, "reference": ref_name, "candidate": cand_name}
                _classify(options, "name_case", name, c, item, expected, name_case)
            if c in class_mismatch or ignored(name, c, options.ignore_columns):
                continue
            if expected_rules.normalize_type(ref_type) != expected_rules.normalize_type(cand_type):
                item = {"column": c, "reference": ref_type, "candidate": cand_type}
                _classify(options, "declared_type", name, c, item, expected, type_differences)
        if type_differences:
            reasons.append("declared_types")
    schema_out["type_differences"] = type_differences

    if ref_side is None or cand_side is None:
        # A side without data files has no Parquet schema: its rows are
        # counted, its declared columns compared above.
        ref_rows = side_stats(con, canonical_relation(ref_side, [], ""), []).rows if ref_side else 0
        cand_rows = side_stats(con, canonical_relation(cand_side, [], ""), []).rows if cand_side else 0
        out["rows"] = {"reference": ref_rows, "candidate": cand_rows}
        if ref_rows != cand_rows:
            reasons.append("row_count")
        out["schema"] = schema_out
        return _finish(out, reasons, expected, name_case)

    differing_declared = {d["column"] for d in type_differences} if not from_metastore else set()
    # An expected declared type change stores the column in another type too
    # (`real` as FLOAT, `double` as DOUBLE): the rule covers both.
    expected_types = {e["column"] for e in expected if e.get("kind") == "declared_type"}
    storage = [
        {"column": c, "reference": ref_side.storage(c), "candidate": cand_side.storage(c)}
        for c in compared
        if c not in class_mismatch
        and c not in differing_declared
        and c not in expected_types
        and ref_side.storage(c) != cand_side.storage(c)
    ]
    if storage:
        reasons.append("storage_types")
    schema_out.update(
        {
            "storage_type_differences": storage,
            "ignored_columns": skipped,
            "compared_columns": len(compared),
        }
    )
    out["schema"] = schema_out
    zone = options.parquet_time_zone
    ref_relation = canonical_relation(ref_side, compared, zone)
    replace: dict[str, str] = {}
    for c in compared:
        rule = expected_rules.replacements(options.expected, name, c)
        if rule is None or type_class(cand_side.columns[c][1]) != "S":
            continue
        pattern = expected_rules.CHARACTER_CLASSES[rule.characters]
        replace[c] = pattern
        column_sql = quote_ident(cand_side.columns[c][0])
        changed = con.execute(
            f"SELECT count(*) FROM {_relation(cand_side.files)} "
            f"WHERE regexp_matches({column_sql}, {quote_literal(pattern)})"
        ).fetchone()[0]
        expected.append(
            {
                "column": c,
                "kind": "replaced_characters",
                "rule": rule.id,
                "reason": rule.reason,
                "reference": "U+FFFD",
                "candidate": f"{rule.characters} ({changed} rows)",
            }
        )
    cand_relation = canonical_relation(cand_side, compared, zone, replace)
    ref = side_stats(con, ref_relation, compared)
    cand = side_stats(con, cand_relation, compared)
    out["rows"] = {"reference": ref.rows, "candidate": cand.rows}
    out["checksum"] = {"reference": ref.checksum, "candidate": cand.checksum}
    if ref.rows != cand.rows:
        reasons.append("row_count")
    if ref.checksum != cand.checksum:
        reasons.append("checksum")
    diff_columns = [c for c in compared if ref.columns[c] != cand.columns[c]]
    out["columns_differing"] = [
        {
            "column": c,
            "nulls": {"reference": ref.nulls[c], "candidate": cand.nulls[c]},
        }
        for c in diff_columns
    ]
    if ref.checksum != cand.checksum:
        total, sample = differing_rows(
            con, ref_relation, cand_relation, compared, options.sample, options.show_rows
        )
        out["differing_row_hashes"] = total
        out["sample"] = sample
    return _finish(out, reasons, expected, name_case)


def _finish(
    out: dict[str, Any],
    reasons: list[str],
    expected: list[dict[str, Any]],
    name_case: list[dict[str, Any]],
) -> dict[str, Any]:
    if name_case:
        reasons.append("name_case")
    reasons = list(dict.fromkeys(reasons))
    out["name_case_differences"] = name_case
    out["expected_differences"] = expected
    out["status"] = "divergent" if reasons else "equal"
    if reasons:
        out["reasons"] = reasons
    return out


def connect(memory_limit: str, temp_dir: str, s3: dict[str, str] | None = None) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with bounded memory that spills to ``temp_dir``."""
    os.makedirs(temp_dir, exist_ok=True)
    settings: dict[str, Any] = {"memory_limit": memory_limit, "temp_directory": temp_dir}
    extensions = os.environ.get("CODELAKE_SHADOW_DUCKDB_EXTENSIONS", "")
    if extensions:
        settings["extension_directory"] = extensions
    con = duckdb.connect(database=":memory:", config=settings)
    # Canonical texts never depend on the session zone; pin it anyway.
    con.execute("SET TimeZone = 'UTC'")
    if s3 is not None:
        con.execute("LOAD httpfs")
        options = ["TYPE S3"]
        if s3.get("key_id"):
            options.append(f"KEY_ID {quote_literal(s3['key_id'])}")
            options.append(f"SECRET {quote_literal(s3.get('secret', ''))}")
        else:
            options.append("PROVIDER credential_chain")
        if s3.get("region"):
            options.append(f"REGION {quote_literal(s3['region'])}")
        endpoint = s3.get("endpoint", "")
        if endpoint:
            match = re.match(r"^(https?)://(.+?)/?$", endpoint)
            if match:
                options.append(f"ENDPOINT {quote_literal(match.group(2))}")
                options.append(f"USE_SSL {'true' if match.group(1) == 'https' else 'false'}")
            else:
                options.append(f"ENDPOINT {quote_literal(endpoint)}")
        if s3.get("path_style") == "true":
            options.append("URL_STYLE 'path'")
        try:
            con.execute(f"CREATE SECRET shadow_s3 ({', '.join(options)})")
        except duckdb.Error:
            # The statement holds the key; never let an error message quote it.
            raise RuntimeError("cannot configure DuckDB's S3 access (check SHADOW_S3_* and AWS_*)") from None
    return con
