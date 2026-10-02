"""Unit tests of the differential harness: classification, expectations and case
validation. No engine is needed: python3 -m unittest discover -s tools/differential/tests
"""

import os
import sys
import tempfile
import textwrap
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from harness import cases, runner  # noqa: E402
from harness.compare import as_pg_text, classify, worst  # noqa: E402


def ans(rows, types=None):
    r = {"rows": rows}
    if types is not None:
        r["types"] = types
    return r


def err(code):
    return {"error": code, "message": "m"}


class Classify(unittest.TestCase):
    def test_same_rows_in_any_order_are_ok_for_a_multiset(self):
        self.assertEqual(classify(ans([[1], [2]]), ans([[2], [1]])), "OK")

    def test_order_counts_only_when_asked(self):
        self.assertEqual(classify(ans([[1], [2]]), ans([[2], [1]]), ordered=True), "ORDEM")

    def test_a_missing_duplicate_is_wrong(self):
        self.assertEqual(classify(ans([[1], [1]]), ans([[1]])), "WRONG")

    def test_negative_zero_is_not_zero(self):
        # Python's == says -0.0 == 0.0; the comparison must not.
        self.assertEqual(classify(ans([[0.0]]), ans([[-0.0]])), "WRONG")

    def test_integer_is_not_float_nor_boolean(self):
        self.assertEqual(classify(ans([[1]]), ans([[1.0]])), "WRONG")
        self.assertEqual(classify(ans([[1]]), ans([[True]])), "WRONG")

    def test_refusals_and_extras(self):
        self.assertEqual(classify(ans([[1]]), err("NOT_SUPPORTED")), "RECUSA")
        self.assertEqual(classify(err("DIVISION_BY_ZERO"), ans([[1]])), "EXTRA")
        self.assertEqual(classify(err("A"), err("A")), "OK")
        self.assertEqual(classify(err("A"), err("B")), "ERRO")

    def test_hang_is_its_own_outcome(self):
        self.assertEqual(classify(ans([[1]]), err("HANG")), "TRAVA")
        self.assertEqual(classify(err("42P01"), err("HANG")), "TRAVA")

    def test_types_compared_only_when_asked(self):
        self.assertEqual(classify(ans([["a"]], ["varchar"]), ans([["a"]], ["varchar(1)"])), "TIPO")
        self.assertEqual(classify(ans([["a"]], ["varchar"]), ans([["a"]], ["varchar(1)"]), compare_types=False), "OK")

    def test_names_compared_with_the_types(self):
        want = dict(ans([[1]], ["integer"]), names=["FirstDay"])
        self.assertEqual(classify(want, dict(ans([[1]], ["integer"]), names=["firstday"])), "NOME")
        self.assertEqual(classify(want, dict(ans([[1]], ["integer"]), names=["FirstDay"])), "OK")
        self.assertEqual(classify(want, dict(ans([[1]], ["bigint"]), names=["firstday"])), "TIPO")
        self.assertEqual(classify(want, dict(ans([[1]], ["integer"]), names=["x"]), compare_types=False), "OK")

    def test_worst_step_wins(self):
        self.assertEqual(worst(["OK", "ORDEM", "WRONG", "RECUSA"]), "WRONG")
        self.assertEqual(worst([]), "OK")

    def test_trino_values_as_postgresql_text(self):
        self.assertEqual([as_pg_text(v) for v in (True, False, None, 1, 1.5, 2.0, -0.0, "1.50")],
                         ["t", "f", None, "1", "1.5", "2", "-0", "1.50"])


def load(toml):
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "s.toml"), "w") as f:
            f.write(textwrap.dedent(toml))
        return cases.load(d)[0]


class CaseFiles(unittest.TestCase):
    def test_minimal_case_defaults(self):
        s = load('''
            [[case]]
            id = "a"
            sql = "SELECT 1"
        ''')
        c = s.cases[0]
        self.assertEqual((c.oracle, c.protocol, c.compare, c.expect, c.types), ("trino", "trino", "multiset", "OK", True))
        self.assertEqual(s.needs(), {"codelake", "trino"})

    def test_unknown_key_is_refused(self):
        with self.assertRaisesRegex(cases.CaseError, "unknown case keys"):
            load('''
                [[case]]
                id = "a"
                sql = "SELECT 1"
                expected = "OK"
            ''')

    def test_divergence_needs_a_reference(self):
        with self.assertRaisesRegex(cases.CaseError, "known .* or accepted"):
            load('''
                [[case]]
                id = "a"
                sql = "SELECT 1"
                expect = "WRONG"
            ''')
        with self.assertRaisesRegex(cases.CaseError, "must start with"):
            load('''
                [[case]]
                id = "a"
                sql = "SELECT 1"
                expect = "WRONG"
                known = "somewhere"
            ''')

    def test_divergent_session_names_its_steps(self):
        with self.assertRaisesRegex(cases.CaseError, "diverges_at"):
            load('''
                oracle = "postgres"
                [[case]]
                id = "a"
                steps = ["BEGIN", "ROLLBACK"]
                expect = "ERRO"
                known = "ISS-030"
            ''')

    def test_a_step_there_only_to_fail(self):
        s = load('''
            oracle = "postgres"
            [[case]]
            id = "a"
            steps = ["BEGIN", { sql = "SELECT * FROM nope", any_error = true }, "ROLLBACK"]
        ''')
        self.assertEqual((s.cases[0].steps[1], s.cases[0].any_error), ("SELECT * FROM nope", {2}))
        with self.assertRaisesRegex(cases.CaseError, "sql and any_error only"):
            load('''
                oracle = "postgres"
                [[case]]
                id = "a"
                steps = [{ sql = "SELECT 1", expect = "OK" }]
            ''')

    def test_steps_need_the_postgres_oracle(self):
        with self.assertRaisesRegex(cases.CaseError, "postgres oracle"):
            load('''
                [[case]]
                id = "a"
                steps = ["SELECT 1"]
            ''')

    def test_a_time_zone_for_both_engines(self):
        s = load('''
            time_zone = "America/Sao_Paulo"
            [[case]]
            id = "a"
            sql = "SELECT now()"
            [[case]]
            id = "b"
            sql = "SELECT now()"
            time_zone = "Asia/Tokyo"
        ''')
        self.assertEqual([c.time_zone for c in s.cases], ["America/Sao_Paulo", "Asia/Tokyo"])
        with self.assertRaisesRegex(cases.CaseError, "trino oracle over the trino protocol"):
            load('''
                oracle = "postgres"
                [[case]]
                id = "a"
                sql = "SELECT 1"
                time_zone = "UTC"
            ''')

    def test_duplicated_ids(self):
        with self.assertRaisesRegex(cases.CaseError, "duplicated"):
            load('''
                [[case]]
                id = "a"
                sql = "SELECT 1"
                [[case]]
                id = "a"
                sql = "SELECT 2"
            ''')

    def test_the_repository_case_files_are_valid(self):
        here = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cases")
        self.assertTrue(cases.load(here))


class Expectations(unittest.TestCase):
    def case(self, **kw):
        raw = {"id": "x", "sql": "SELECT 1", **kw}
        return cases.Case("s", raw, {})

    def test_known_wrong_passes_and_a_fix_fails_until_the_marker_goes(self):
        c = self.case(expect="WRONG", known="ISS-025")
        self.assertIsNone(runner.judge(c, "WRONG", [], ans([[1]])))
        self.assertIn("drop the marker", runner.judge(c, "OK", [], ans([[1]])))

    def test_an_unmarked_wrong_fails(self):
        self.assertEqual(runner.judge(self.case(), "WRONG", [], ans([[1]])), "expected OK, got WRONG")

    def test_refusal_must_be_the_expected_one(self):
        c = self.case(expect="RECUSA", known="ISS-024", error="is not supported yet")
        self.assertIsNone(runner.judge(c, "RECUSA", [], {"error": "X", "message": "char is not supported yet"}))
        self.assertIn("does not contain", runner.judge(c, "RECUSA", [], {"error": "X", "message": "boom"}))

    def test_session_divergence_must_stay_where_it_was(self):
        c = cases.Case("s", {"id": "x", "steps": ["a", "b", "c"], "oracle": "postgres", "expect": "ERRO",
                             "accepted": "ISS-030 item 5", "diverges_at": [2]}, {})
        self.assertIsNone(runner.judge(c, "ERRO", [2], None))
        self.assertIn("diverges at steps [2, 3]", runner.judge(c, "ERRO", [2, 3], None))


class Report(unittest.TestCase):
    def test_timestamps_in_the_operators_zone(self):
        os.environ["CODELAKE_HOST_TZ"] = "America/Sao_Paulo"
        try:
            self.assertEqual(runner.local_time(1790445313).isoformat(), "2026-09-26T14:55:13-03:00")
        finally:
            del os.environ["CODELAKE_HOST_TZ"]


class Session(unittest.TestCase):
    def test_codelake_only_session_prefix(self):
        self.assertEqual(runner.split_session("/*CLSESSION a=1*/ SELECT 1", None), ("SELECT 1", "a=1"))
        self.assertEqual(runner.split_session("/*CLSESSION b=2*/SELECT 1", "a=1"), ("SELECT 1", "a=1,b=2"))
        self.assertEqual(runner.split_session("SELECT 1", "a=1"), ("SELECT 1", "a=1"))

    def test_catalog_templates(self):
        self.assertEqual(runner.for_trino("{c}.s.t_{e}"), "memory.s.t_tr")
        self.assertEqual(runner.for_codelake("{c}.s.t_{e}"), "datafusion.s.t_cl")


if __name__ == "__main__":
    unittest.main()
