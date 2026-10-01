"""Runs case files against CodeLake and their oracles; builds the report."""

import datetime
import hashlib
import os
import re
import time
import zoneinfo

from .clients import Pg, PgError, Trino
from .compare import OUTCOMES, classify, rows_as_pg_text, worst

CLSESSION = re.compile(r"^/\*CLSESSION\s+(.*?)\*/\s*", re.S)


def split_session(sql, session):
    """`/*CLSESSION k=v,...*/ SQL`: session properties sent to CodeLake only."""
    m = CLSESSION.match(sql)
    if m:
        extra = m.group(1).strip()
        session = f"{session},{extra}" if session else extra
        sql = sql[m.end():]
    return sql, session


def for_trino(sql):
    return sql.replace("{c}", "memory").replace("{e}", "tr")


def for_codelake(sql):
    return sql.replace("{c}", "datafusion").replace("{e}", "cl")


def short(result, limit=1200):
    """A result trimmed for the report."""
    if result is None:
        return None
    if "error" in result:
        return {"error": result["error"], "message": result.get("message", "")[:400]}
    out = {"rows": result["rows"][:50]}
    if len(result["rows"]) > 50:
        out["rows_total"] = len(result["rows"])
    if "types" in result:
        out["types"] = result["types"]
    return out


class Engines:
    def __init__(self, endpoints):
        self.endpoints = endpoints
        self.trino = Trino(endpoints["trino"]) if "trino" in endpoints else None
        self.codelake = Trino(endpoints["codelake_trino"])
        self.users = 0

    def codelake_pg(self):
        # One user per connection: a case never waits on another's admission slots.
        self.users += 1
        host, port = self.endpoints["codelake_pg"]
        return Pg(host, port, f"diff{self.users}", "datafusion")

    def postgres(self):
        host, port = self.endpoints["postgres"]
        return Pg(host, port, "postgres", "postgres")

    def wait(self, log, alive=lambda: None, timeout=300):
        """`alive` raises when the CodeLake container is gone: no point waiting on it."""
        deadline = time.time() + timeout
        targets = [("codelake", self.codelake)] + ([("trino", self.trino)] if self.trino else [])
        for name, engine in targets:
            while not engine.ready():
                if name == "codelake":
                    alive()
                if time.time() > deadline:
                    raise RuntimeError(f"{name} did not start in {timeout}s")
                time.sleep(1)
            log(f"{name} ready")
        if self.trino:
            while True:
                r = self.trino.query("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'", "system")
                if r.get("rows") and r["rows"][0][0]:
                    break
                if time.time() > deadline:
                    raise RuntimeError("trino has no active node")
                time.sleep(1)
            # The first write to a catalog can race the catalog's own start.
            self.trino.query("CREATE TABLE memory.default.differential_warmup AS SELECT 1 AS a", "memory")
            while "error" in self.trino.query("SELECT a FROM memory.default.differential_warmup", "memory"):
                if time.time() > deadline:
                    raise RuntimeError("trino memory catalog not writable")
                time.sleep(1)
        for name in ("codelake_pg", "postgres"):
            if name not in self.endpoints:
                continue
            while True:
                try:
                    (self.codelake_pg() if name == "codelake_pg" else self.postgres()).close()
                    break
                except (OSError, PgError):
                    if time.time() > deadline:
                        raise RuntimeError(f"{name} does not accept connections") from None
                    time.sleep(1)
            log(f"{name} ready")


def run_setup(engines, suite):
    errors = []

    def check(where, sql, result):
        if "error" in result:
            errors.append({"suite": suite.name, "engine": where, "sql": sql, "result": short(result)})

    for sql in suite.setup:
        check("trino", sql, engines.trino.query(for_trino(sql), "memory"))
        check("codelake", sql, engines.codelake.query(for_codelake(sql), "datafusion"))
    for sql in suite.setup_trino:
        check("trino", sql, engines.trino.query(for_trino(sql), "memory"))
    for sql in suite.setup_codelake:
        check("codelake", sql, engines.codelake.query(for_codelake(sql), "datafusion"))
    # setup_postgres runs before CodeLake starts (stack.Stack), which lists a
    # source's tables once, at start.
    return errors


def run_case(engines, case):
    """Returns (outcome, divergent steps, detail)."""
    ordered = case.compare == "ordered"
    if case.steps is not None or case.oracle == "postgres":
        steps = case.steps if case.steps is not None else [case.sql]
        oracle_conn = engines.postgres()
        cl = engines.codelake_pg()
        outcomes, detail = [], []
        try:
            for i, sql in enumerate(steps, 1):
                if case.params is not None:
                    want = oracle_conn.extended(sql, case.params, case.param_types)
                    got = cl.extended(sql, case.params, case.param_types)
                else:
                    want, got = oracle_conn.query(sql), cl.query(sql)
                k = classify(want, got, ordered=ordered, compare_types=False)
                if k == "ERRO" and i in case.any_error:
                    k = "OK"
                outcomes.append(k)
                detail.append({"step": i, "sql": sql, "outcome": k, "oracle": short(want), "codelake": short(got)})
        finally:
            oracle_conn.close()
            cl.close()
        divergent = [d["step"] for d in detail if d["outcome"] != "OK"]
        return worst(outcomes), divergent, {"steps": detail}, (detail[-1]["codelake"] if detail else None)

    sql, session = split_session(case.sql, case.session)
    if case.oracle == "trino":
        want = engines.trino.query(for_trino(case.oracle_sql or sql), "memory")
    else:
        want = {"rows": case.rows}
    if case.protocol == "trino":
        got = engines.codelake.query(for_codelake(sql), "datafusion", session=session)
        compare_types = case.types and case.oracle == "trino"
    else:
        if case.oracle == "trino" and "rows" in want:
            want = {"rows": rows_as_pg_text(want["rows"])}
        cl = engines.codelake_pg()
        try:
            got = (cl.extended(for_codelake(sql), case.params, case.param_types) if case.params is not None
                   else cl.query(for_codelake(sql)))
        finally:
            cl.close()
        compare_types = False
    k = classify(want, got, ordered=ordered, compare_types=compare_types)
    return k, [], {"oracle": short(want), "codelake": short(got)}, got


def judge(case, outcome, divergent, got):
    """None when the outcome is the case's expectation, else why not."""
    if outcome != case.expect:
        if case.expect == "OK":
            return f"expected OK, got {outcome}"
        return (f"expected {case.expect} ({'known' if case.known else 'accepted'}: {case.ref}), got {outcome}"
                + ("; if it is fixed, drop the marker from the case" if outcome == "OK" else ""))
    if case.diverges_at is not None and sorted(divergent) != sorted(case.diverges_at):
        return f"diverges at steps {divergent}, the case says {sorted(case.diverges_at)}"
    if case.error:
        text = f"{got.get('error', '')}: {got.get('message', '')}" if got and "error" in got else ""
        if case.error not in text:
            return f"CodeLake's error does not contain {case.error!r}: {text[:200]!r}"
    return None


def run(suites, engines, log, binary=None, setup_errors=()):
    started = time.time()
    results, setup_errors = [], list(setup_errors)
    for suite in suites:
        log(f"suite {suite.name}: {len(suite.cases)} cases")
        setup_errors += run_setup(engines, suite)
        for case in suite.cases:
            outcome, divergent, detail, got = run_case(engines, case)
            why = judge(case, outcome, divergent, got)
            results.append({
                "suite": suite.name, "id": case.id, "oracle": case.oracle, "protocol": case.protocol,
                "sql": case.sql, "expect": case.expect, "outcome": outcome,
                "divergent_steps": divergent or None,
                "status": "fail" if why else "pass", "reason": why,
                "known": case.known, "accepted": case.accepted, "note": case.note, **detail,
            })
    return report(results, setup_errors, started, binary)


def local_time(ts):
    """`ts` in the operator's zone (CODELAKE_HOST_TZ, from scripts/dev.sh), else the container's."""
    zone = os.environ.get("CODELAKE_HOST_TZ")
    if zone:
        try:
            return datetime.datetime.fromtimestamp(ts, zoneinfo.ZoneInfo(zone))
        except (zoneinfo.ZoneInfoNotFoundError, ValueError):
            pass
    return datetime.datetime.fromtimestamp(ts).astimezone()


def report(results, setup_errors, started, binary):
    outcomes = {k: 0 for k in OUTCOMES}
    for r in results:
        outcomes[r["outcome"]] += 1
    fails = [r for r in results if r["status"] == "fail"]
    known = [r for r in results if r["status"] == "pass" and r["known"]]
    accepted = [r for r in results if r["status"] == "pass" and r["accepted"]]
    rep = {
        "started": local_time(started).isoformat(timespec="seconds"),
        "seconds": round(time.time() - started, 1),
        "summary": {"cases": len(results), "pass": len(results) - len(fails), "fail": len(fails),
                    "setup_errors": len(setup_errors), "known": len(known), "accepted": len(accepted),
                    "outcomes": {k: v for k, v in outcomes.items() if v}},
        "failures": [{k: r[k] for k in ("suite", "id", "expect", "outcome", "reason", "sql")} for r in fails],
        "known": [{k: r[k] for k in ("suite", "id", "outcome", "known", "sql")} for r in known],
        "accepted": [{k: r[k] for k in ("suite", "id", "outcome", "accepted", "sql")} for r in accepted],
        "setup_errors": setup_errors,
        "cases": results,
    }
    if binary:
        with open(binary, "rb") as f:
            rep["codelake_binary_sha256"] = hashlib.sha256(f.read()).hexdigest()
    return rep


def summary_text(rep):
    s = rep["summary"]
    out = [f"differential: {s['cases']} cases, {s['pass']} as expected, {s['fail']} unexpected, "
           f"{s['setup_errors']} setup errors ({rep['seconds']}s)",
           "outcomes: " + " ".join(f"{k}={v}" for k, v in s["outcomes"].items())]

    def line(r, ref):
        sql = (r["sql"] or "<steps>").replace("\n", " ")
        return f"  [{r['outcome']}] {r['suite']}:{r['id']} {ref} :: {sql[:110]}"

    if rep["known"]:
        out.append(f"known open defects ({len(rep['known'])}; expected, NOT green):")
        out += [line(r, r["known"]) for r in rep["known"]]
    if rep["accepted"]:
        out.append(f"accepted divergences ({len(rep['accepted'])}):")
        out += [line(r, r["accepted"]) for r in rep["accepted"]]
    if rep["setup_errors"]:
        out.append("setup errors:")
        out += [f"  {e['suite']} {e['engine']}: {e['sql'][:100]} -> {e['result']}" for e in rep["setup_errors"]]
    if rep["failures"]:
        out.append(f"UNEXPECTED ({len(rep['failures'])}):")
        out += [f"  {r['suite']}:{r['id']} {r['reason']} :: {(r['sql'] or '<steps>')[:110]}" for r in rep["failures"]]
    return "\n".join(out)
