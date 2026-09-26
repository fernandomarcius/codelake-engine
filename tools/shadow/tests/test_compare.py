"""Unit tests of the comparison, run in the harness image:

    docker run --rm --entrypoint python codelake-shadow:<tag> -m unittest discover -s tests
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest

import duckdb

from codelake_shadow import compare, config, expected, rss
from codelake_shadow.__main__ import model_status_differences

SECRET_VALUE = "row-value-that-must-not-leak"


class Warehouses:
    """Two warehouses in a temporary directory, filled with DuckDB SQL."""

    def __init__(self) -> None:
        self.dir = tempfile.TemporaryDirectory()
        self.ref = os.path.join(self.dir.name, "ref")
        self.cand = os.path.join(self.dir.name, "cand")
        self.con = duckdb.connect()

    def write(
        self, side: str, table: str, sql: str, files: int = 1, declared: list[tuple[str, str]] | None = None
    ) -> None:
        schema, name = table.split(".")
        root = self.ref if side == "ref" else self.cand
        directory = os.path.join(root, schema, name)
        os.makedirs(directory, exist_ok=True)
        # Metadata and in-flight files the comparison must skip. Without
        # `declared`, the metastore file holds no columns (Parquet types
        # stand for the declared ones).
        metadata = {} if declared is None else {
            "dataColumns": [{"name": n, "type": t, "properties": {}} for n, t in declared],
            "partitionColumns": [],
        }
        with open(os.path.join(directory, compare.METASTORE_FILE), "w") as f:
            json.dump(metadata, f)
        with open(os.path.join(directory, ".tmp-part"), "w") as f:
            f.write("partial")
        self.con.execute(f"CREATE OR REPLACE TABLE src AS {sql}")
        total = self.con.execute("SELECT count(*) FROM src").fetchone()[0]
        for part in range(files):
            path = os.path.join(directory, f"part-{part}")
            self.con.execute(
                f"COPY (SELECT * FROM (SELECT *, row_number() OVER () AS __rn FROM src) "
                f"WHERE __rn % {files} = {part} OR {total} = 0) "
                f"TO '{path}' (FORMAT PARQUET)"
            )
            # Drop the helper column by rewriting without it.
            self.con.execute(
                f"COPY (SELECT * EXCLUDE (__rn) FROM read_parquet('{path}')) TO '{path}.p' (FORMAT PARQUET)"
            )
            os.replace(path + ".p", path)

    def place(self, side: str, table: str, sources: list[str], declared: list[tuple[str, str]] | None = None) -> None:
        """Copies Parquet files as they are (e.g. written by another engine)."""
        schema, name = table.split(".")
        directory = os.path.join(self.ref if side == "ref" else self.cand, schema, name)
        os.makedirs(directory, exist_ok=True)
        metadata = {} if declared is None else {
            "dataColumns": [{"name": n, "type": t, "properties": {}} for n, t in declared],
            "partitionColumns": [],
        }
        with open(os.path.join(directory, compare.METASTORE_FILE), "w") as f:
            json.dump(metadata, f)
        for index, source in enumerate(sources):
            with open(source, "rb") as src, open(os.path.join(directory, f"part-{index}"), "wb") as dst:
                dst.write(src.read())

    def compare(self, table: str = "s.t", **options) -> dict:
        if isinstance(options.get("expected"), dict):
            options["expected"] = expected.parse(options["expected"])
        con = compare.connect("256MB", os.path.join(self.dir.name, "tmp"))
        schema, name = table.split(".")
        return compare.compare_table(
            con,
            compare.Location(self.ref),
            compare.Location(self.cand),
            schema,
            name,
            compare.Options(**options),
        )


BASE = """
SELECT * FROM (VALUES
    (1, 'a',  12.500000::DECIMAL(18,6), TIMESTAMP '2026-01-01 10:00:00.123', 'x'::BLOB),
    (2, 'b',  -0.000001::DECIMAL(18,6), TIMESTAMP '2026-01-02 00:00:00', NULL),
    (3, NULL, 0::DECIMAL(18,6),         NULL, '\\x00\\xff'::BLOB),
    (4, '',   NULL,                     TIMESTAMP '1969-12-31 23:59:59.999', ''::BLOB)
) v(id, note, amount, created_at, code)
"""


class CompareTests(unittest.TestCase):
    def setUp(self) -> None:
        self.w = Warehouses()

    def tearDown(self) -> None:
        self.w.dir.cleanup()

    def test_equal_regardless_of_row_order_and_file_split(self) -> None:
        self.w.write("ref", "s.t", BASE)
        self.w.write("cand", "s.t", f"SELECT * FROM ({BASE}) ORDER BY id DESC", files=3)
        result = self.w.compare()
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(result["rows"], {"reference": 4, "candidate": 4})
        self.assertEqual(result["files"], {"reference": 1, "candidate": 3})

    DECLARED = [("id", "int"), ("note", "varchar(10)"), ("amount", "decimal(18,6)"),
                ("created_at", "timestamp"), ("code", "binary")]

    def test_equal_values_in_another_storage_are_a_storage_divergence(self) -> None:
        # Same declared types and values, other Parquet storage (decimal
        # width, timestamp unit): values equal, the storage diverges.
        self.w.write("ref", "s.t", BASE, declared=self.DECLARED)
        self.w.write(
            "cand",
            "s.t",
            f"""SELECT id, note, CAST(amount AS DECIMAL(38, 10)) AS amount,
                       CAST(created_at AS TIMESTAMP_NS) AS created_at, code FROM ({BASE})""",
            declared=self.DECLARED,
        )
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["reasons"], ["storage_types"], result)
        self.assertEqual(result["checksum"]["reference"], result["checksum"]["candidate"])
        self.assertEqual(result["schema"]["declared_types_from"], "metastore")
        self.assertEqual(
            sorted(d["column"] for d in result["schema"]["storage_type_differences"]), ["amount", "created_at"]
        )

    def test_without_metastore_parquet_types_are_the_declared_ones(self) -> None:
        self.w.write("ref", "s.t", BASE)
        self.w.write(
            "cand", "s.t", f"SELECT * REPLACE (CAST(amount AS DECIMAL(38, 10)) AS amount) FROM ({BASE})"
        )
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["reasons"], ["declared_types"], result)
        self.assertEqual(result["schema"]["declared_types_from"], "parquet")
        self.assertEqual([d["column"] for d in result["schema"]["type_differences"]], ["amount"])

    def test_declared_type_difference_is_a_divergence_unless_a_rule_documents_it(self) -> None:
        ref = [("id", "int"), ("label", "varchar(2)")]
        cand = [("id", "int"), ("label", "string")]
        self.w.write("ref", "s.t", "SELECT 1 AS id, 'um' AS label", declared=ref)
        self.w.write("cand", "s.t", "SELECT 1 AS id, 'um' AS label", declared=cand)
        result = self.w.compare()
        self.assertEqual(result["reasons"], ["declared_types"], result)
        rules = {"differences": [{"id": "ISS-019", "kind": "declared_type", "reason": "literal",
                                  "table": "s.t", "column": "label",
                                  "reference": "varchar(*)", "candidate": ["varchar", "string"]}]}
        result = self.w.compare(expected=rules)
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(
            result["expected_differences"],
            [{"column": "label", "reference": "varchar(2)", "candidate": "string",
              "kind": "declared_type", "rule": "ISS-019", "reason": "literal"}],
        )
        # A rule for another type change does not apply.
        other = {"differences": [dict(rules["differences"][0], reference="char(*)")]}
        self.assertEqual(self.w.compare(expected=other)["status"], "divergent")

    def test_a_rule_never_hides_a_value_difference(self) -> None:
        self.w.write("ref", "s.t", "SELECT 1 AS id, 'um' AS label", declared=[("id", "int"), ("label", "varchar(2)")])
        self.w.write("cand", "s.t", "SELECT 1 AS id, 'um ' AS label", declared=[("id", "int"), ("label", "string")])
        rules = {"differences": [{"id": "ISS-019", "kind": "declared_type", "reason": "literal",
                                  "table": "s.t", "column": "label",
                                  "reference": "varchar(*)", "candidate": "string"}]}
        result = self.w.compare(expected=rules)
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["reasons"], ["checksum"], result)
        self.assertEqual([c["column"] for c in result["columns_differing"]], ["label"])
        self.assertEqual([e["rule"] for e in result["expected_differences"]], ["ISS-019"])

    def test_name_case_differences(self) -> None:
        self.w.write("ref", "s.t", "SELECT 1 AS order_id, 'a' AS status")
        self.w.write("cand", "S.T", 'SELECT 1 AS "Order_Id", \'a\' AS status')
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["reasons"], ["name_case"], result)
        self.assertEqual(
            result["name_case_differences"],
            [
                {"object": "table", "reference": "s.t", "candidate": "S.T"},
                {"object": "column", "column": "order_id", "reference": "order_id", "candidate": "Order_Id"},
            ],
        )
        rules = {"differences": [{"id": "ISS-018", "kind": "name_case", "reason": "case kept"}]}
        result = self.w.compare(expected=rules)
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(len(result["expected_differences"]), 2)
        # Values under names that differ by case are still compared.
        self.w.write("cand", "S.T", 'SELECT 2 AS "Order_Id", \'a\' AS status')
        result = self.w.compare(expected=rules)
        self.assertEqual(result["reasons"], ["checksum"], result)

    def test_partitioned_tables_are_refused(self) -> None:
        self.w.write("ref", "s.t", "SELECT 1 AS a")
        self.w.write("cand", "s.t", "SELECT 1 AS a")
        os.makedirs(os.path.join(self.w.cand, "s", "t", "p=1"))
        result = self.w.compare()
        self.assertEqual(result["status"], "error", result)
        self.assertIn("partitioned", result["error"])
        os.rmdir(os.path.join(self.w.cand, "s", "t", "p=1"))
        self.w.write("cand", "s.t", "SELECT 1 AS a", declared=[("a", "int")])
        with open(os.path.join(self.w.cand, "s", "t", compare.METASTORE_FILE)) as f:
            metadata = json.load(f)
        metadata["partitionColumns"] = [{"name": "p", "type": "string"}]
        with open(os.path.join(self.w.cand, "s", "t", compare.METASTORE_FILE), "w") as f:
            json.dump(metadata, f)
        self.assertEqual(self.w.compare()["status"], "error")

    def test_duplicates_count(self) -> None:
        self.w.write("ref", "s.t", "SELECT * FROM (VALUES (1), (1), (2)) v(x)")
        self.w.write("cand", "s.t", "SELECT * FROM (VALUES (1), (2), (2)) v(x)")
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent")
        self.assertEqual(result["rows"], {"reference": 3, "candidate": 3})
        self.assertEqual(result["reasons"], ["checksum"])
        self.assertEqual(result["differing_row_hashes"], 2)

    def assert_detected(self, candidate_sql: str, column: str | None, reasons: list[str]) -> dict:
        self.w.write("ref", "s.t", BASE)
        self.w.write("cand", "s.t", candidate_sql)
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["reasons"], reasons, result)
        if column is not None:
            self.assertEqual([c["column"] for c in result["columns_differing"]], [column], result)
        return result

    def test_detects_one_changed_value_per_kind(self) -> None:
        cases = {
            "amount": "amount + CASE WHEN id = 1 THEN 0.000001 ELSE 0 END AS amount",
            "created_at": "CASE WHEN id = 1 THEN created_at + INTERVAL 3 HOUR ELSE created_at END AS created_at",
            "note": "CASE WHEN id = 1 THEN note || ' ' ELSE note END AS note",
            "code": "CASE WHEN id = 2 THEN ''::BLOB ELSE code END AS code",
        }
        for column, expr in cases.items():
            with self.subTest(column=column):
                self.tearDown()
                self.setUp()
                self.assert_detected(f"SELECT * REPLACE ({expr}) FROM ({BASE})", column, ["checksum"])

    def test_null_is_not_empty_string(self) -> None:
        result = self.assert_detected(
            f"SELECT * REPLACE (CASE WHEN id = 3 THEN '' ELSE note END AS note) FROM ({BASE})",
            "note",
            ["checksum"],
        )
        self.assertEqual(result["columns_differing"][0]["nulls"], {"reference": 1, "candidate": 0})

    def test_extra_and_missing_rows(self) -> None:
        self.assert_detected(f"SELECT * FROM ({BASE}) UNION ALL SELECT * FROM ({BASE}) WHERE id = 1", None, ["row_count", "checksum"])
        self.tearDown()
        self.setUp()
        self.assert_detected(f"SELECT * FROM ({BASE}) WHERE id <> 4", None, ["row_count", "checksum"])

    def test_values_appear_only_when_asked(self) -> None:
        self.w.write("ref", "s.t", f"SELECT '{SECRET_VALUE}' AS v")
        self.w.write("cand", "s.t", f"SELECT '{SECRET_VALUE} ' AS v")
        hidden = json.dumps(self.w.compare())
        self.assertNotIn(SECRET_VALUE, hidden)
        shown = json.dumps(self.w.compare(show_rows=True))
        self.assertIn(SECRET_VALUE, shown)

    def test_schema_differences(self) -> None:
        self.w.write("ref", "s.t", "SELECT 1 AS a, 'x' AS b")
        self.w.write("cand", "s.t", "SELECT '1' AS a, 'x' AS b, 2 AS c")
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent")
        self.assertIn("columns", result["reasons"])
        self.assertIn("column_types", result["reasons"])
        self.assertEqual(result["schema"]["only_candidate"], ["c"])

    def test_ignored_columns(self) -> None:
        self.w.write("ref", "s.t", "SELECT 1 AS a, uuid()::VARCHAR AS id")
        self.w.write("cand", "s.t", "SELECT 1 AS a, uuid()::VARCHAR AS id")
        self.assertEqual(self.w.compare()["status"], "divergent")
        for pattern in ("s.t.id", "t.id", "id", "*.id"):
            with self.subTest(pattern=pattern):
                result = self.w.compare(ignore_columns=[pattern])
                self.assertEqual(result["status"], "equal", result)
                self.assertEqual(result["schema"]["ignored_columns"], ["id"])

    def test_missing_tables(self) -> None:
        self.w.write("ref", "s.t", "SELECT 1 AS a")
        self.assertEqual(self.w.compare()["status"], "missing_candidate")
        self.assertEqual(self.w.compare("s.nope")["status"], "missing_both")


class ExpectedTests(unittest.TestCase):
    def test_shipped_rules_parse_and_cite_an_issue(self) -> None:
        rules = expected.load(config.DEFAULT_EXPECTED)
        self.assertEqual(sorted(r.id for r in rules), ["ISS-018"])

    def test_invalid_rules_are_reported_together(self) -> None:
        document = {"differences": [
            {"kind": "declared_type", "reason": "r"},
            {"id": "X", "kind": "value", "reason": "r"},
            {"id": "Y", "kind": "name_case", "reason": "", "reference": "int"},
            {"id": "Z", "kind": "declared_type", "reason": "r", "colour": "red", "reference": []},
        ]}
        with self.assertRaises(expected.ExpectedError) as err:
            expected.parse(document)
        text = str(err.exception)
        for part in ("differences[0]: `id` is required",
                     "differences[0]: a declared_type rule must name the reference or the candidate type",
                     "differences[1]: `kind` must be one of",
                     "differences[2]: `reason` is required",
                     "differences[2]: a name_case rule takes no types",
                     "differences[3]: unknown keys colour",
                     "differences[3].reference: must be a non-empty string"):
            self.assertIn(part, text)

    def test_types_match_without_case_or_spaces(self) -> None:
        rule = expected.parse({"differences": [
            {"id": "A", "kind": "declared_type", "reason": "r", "table": "s.t", "column": "col",
             "reference": "decimal(18,*)", "candidate": "decimal(38,*)"}]})[0]
        self.assertTrue(rule.matches("declared_type", "S.T", "COL", "DECIMAL(18, 6)", "decimal(38,6)"))
        self.assertFalse(rule.matches("declared_type", "x.t", "col", "decimal(18,6)", "decimal(38,6)"))
        self.assertFalse(rule.matches("declared_type", "s.t", "id", "decimal(18,6)", "decimal(38,6)"))
        self.assertFalse(rule.matches("name_case", "s.t", "col"))

    def test_a_declared_type_rule_names_its_column(self) -> None:
        # A table-wide or project-wide type rule would also cover the same
        # change where it is a defect (F8-2: a source varchar(n) length lost).
        for scope in ({}, {"table": "s.t"}, {"table": "s.*", "column": "label"}, {"table": "s.t", "column": "l?bel"}):
            with self.subTest(scope=scope), self.assertRaisesRegex(expected.ExpectedError, "names one `"):
                expected.parse({"differences": [dict(
                    {"id": "ISS-019", "kind": "declared_type", "reason": "r",
                     "reference": "varchar(*)", "candidate": "string"}, **scope)]})


class ConfigTests(unittest.TestCase):
    ENV = {
        "SHADOW_REFERENCE_URL": "http://ref:8080",
        "SHADOW_CANDIDATE_URL": "http://cand:8080",
        "SHADOW_REFERENCE_WAREHOUSE": "/wh/ref",
        "SHADOW_CANDIDATE_WAREHOUSE": "/wh/cand",
        "SHADOW_DBT_PROJECT_DIR": "/p",
        "SHADOW_DBT_PROFILES_DIR": "/p",
        "SHADOW_OUT_DIR": "/out",
    }

    def test_valid(self) -> None:
        c = config.load(self.ENV)
        self.assertEqual((c.reference.host, c.reference.port), ("ref", 8080))
        self.assertEqual(c.reference.target, "reference")
        self.assertEqual(c.cycles, 1)
        # No rule unless asked: a rule is a decision, not a default.
        self.assertEqual((c.expected_differences, c.expected), ("", []))
        c = config.load(dict(self.ENV, SHADOW_EXPECTED_DIFFERENCES="default"))
        self.assertEqual(c.expected_differences, config.DEFAULT_EXPECTED)
        self.assertTrue(c.expected)
        c = config.load(dict(self.ENV, SHADOW_EXPECTED_DIFFERENCES="none"))
        self.assertEqual((c.expected_differences, c.expected), ("", []))

    def test_unreadable_expected_differences_are_a_config_error(self) -> None:
        with self.assertRaisesRegex(config.ConfigError, "cannot read expected differences"):
            config.load(dict(self.ENV, SHADOW_EXPECTED_DIFFERENCES="/nope.json"))

    def test_same_destination_is_refused(self) -> None:
        env = dict(self.ENV, SHADOW_CANDIDATE_WAREHOUSE="/wh/ref/")
        with self.assertRaisesRegex(config.ConfigError, "own destination"):
            config.load(env)

    def test_every_problem_at_once(self) -> None:
        env = dict(self.ENV, SHADOW_REFERENCE_URL="http://u:p@ref:1", SHADOW_CYCLES="0", SHADOW_DBT_COMMAND="seed")
        del env["SHADOW_OUT_DIR"]
        with self.assertRaises(config.ConfigError) as err:
            config.load(env)
        text = str(err.exception)
        for expected in ("credentials", "SHADOW_CYCLES", "SHADOW_DBT_COMMAND", "SHADOW_OUT_DIR"):
            self.assertIn(expected, text)
        self.assertNotIn("u:p", text)


class RssTests(unittest.TestCase):
    def test_parse_bytes(self) -> None:
        self.assertEqual(rss.parse_bytes("123"), 123)
        self.assertEqual(rss.parse_bytes("2 kB"), 2048)
        self.assertEqual(rss.parse_bytes("1.5GiB"), 1536 * 1024**2)
        with self.assertRaises(ValueError):
            rss.parse_bytes("lots")

    def test_process_tree_and_cgroup(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            for pid, ppid, kb in ((10, 1, 100), (11, 10, 50), (12, 11, 25), (13, 1, 999)):
                os.makedirs(f"{d}/proc/{pid}")
                with open(f"{d}/proc/{pid}/status", "w") as f:
                    f.write(f"Name:\tx\nVmRSS:\t{kb} kB\n")
                with open(f"{d}/proc/{pid}/stat", "w") as f:
                    f.write(f"{pid} (a b) S {ppid} 0 0\n")
            self.assertEqual(rss.Meter("pid:10", proc=f"{d}/proc").read(), 175 * 1024)
            self.assertIsNone(rss.Meter("pid:99", proc=f"{d}/proc").read())
            os.makedirs(f"{d}/cg/system.slice/docker-abc.scope")
            with open(f"{d}/cg/system.slice/docker-abc.scope/memory.stat", "w") as f:
                f.write("anon 4096\nfile 8192\n")
            meter = rss.Meter("container:engine", container_id="abc", cgroup_root=f"{d}/cg")
            self.assertEqual(meter.read(), 4096)

    def test_cmd_meter_and_sampler(self) -> None:
        meter = rss.Meter("cmd:echo 3k")
        self.assertEqual(meter.describe(), "cmd")
        with rss.Sampler(meter, 0.01) as sampler:
            pass
        self.assertEqual(sampler.peak, 3072)
        self.assertGreaterEqual(sampler.samples, 1)
        self.assertIsNone(rss.Meter("cmd:false").read())
        with self.assertRaises(rss.MeterError):
            rss.Meter("pid:abc")


class StatusTests(unittest.TestCase):
    def test_model_status_differences(self) -> None:
        diff = model_status_differences({"a": "success", "b": "error"}, {"a": "success", "c": "success"})
        self.assertEqual(
            diff,
            [
                {"node": "b", "reference": "error", "candidate": "not run"},
                {"node": "c", "reference": "not run", "candidate": "success"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
