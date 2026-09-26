"""Classifies one CodeLake answer against its oracle.

Outcomes (the report and docs/differential.md use these names):
  OK      same rows (and, for a Trino oracle, same column types), or the same error
  WRONG   both answered, different rows (as a multiset): a wrong result
  RECUSA  the oracle answered, CodeLake refused
  EXTRA   CodeLake answered, the oracle refused
  ERRO    both refused, with different errors (Trino errorName / SQLSTATE)
  ORDEM   same multiset of rows, different order, where the case asks for order
  TIPO    same rows, different column types
  TRAVA   CodeLake gave no answer in time (the connection is dropped)
"""

import json

OUTCOMES = ("OK", "WRONG", "RECUSA", "EXTRA", "ERRO", "ORDEM", "TIPO", "TRAVA")
# Worst first: a session's outcome is its worst step.
SEVERITY = {"WRONG": 0, "TRAVA": 1, "EXTRA": 2, "RECUSA": 3, "ERRO": 4, "ORDEM": 5, "TIPO": 6, "OK": 7}


def canonical(value):
    """A value as text that keeps what Python's == would hide (-0.0 vs 0.0, 1 vs 1.0, True vs 1)."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def as_pg_text(value):
    """A Trino JSON value as the PostgreSQL text format would print it."""
    if value is None:
        return None
    if value is True:
        return "t"
    if value is False:
        return "f"
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value in (float("inf"), float("-inf")):
            return "Infinity" if value > 0 else "-Infinity"
        if value.is_integer() and abs(value) < 1e15:
            return "-0" if str(value).startswith("-0") else str(int(value))
        return repr(value)
    if isinstance(value, (list, dict)):
        return canonical(value)
    return str(value)


def rows_as_pg_text(rows):
    return [[as_pg_text(v) for v in row] for row in rows]


def classify(oracle, codelake, ordered=False, compare_types=True):
    """Outcome of `codelake` against `oracle`; both are client result dicts."""
    oe, ce = "error" in oracle, "error" in codelake
    if ce and codelake["error"] in ("HANG", "CONNECTION"):
        return "OK" if oe and oracle["error"] == codelake["error"] else "TRAVA"
    if oe and ce:
        return "OK" if oracle["error"] == codelake["error"] else "ERRO"
    if oe:
        return "EXTRA"
    if ce:
        return "RECUSA"
    want = [canonical(r) for r in oracle["rows"]]
    got = [canonical(r) for r in codelake["rows"]]
    if want != got:
        if sorted(want) != sorted(got):
            return "WRONG"
        if ordered:
            return "ORDEM"
    if compare_types and "types" in oracle and "types" in codelake and oracle["types"] != codelake["types"]:
        return "TIPO"
    return "OK"


def worst(outcomes):
    return min(outcomes, key=lambda o: SEVERITY[o]) if outcomes else "OK"
