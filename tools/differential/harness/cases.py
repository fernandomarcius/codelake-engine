"""Case files: declarative TOML, validated strictly (an unknown key is an error,
so a misspelled expectation cannot pass silently). Format: docs/differential.md.
"""

import json
import os
import re
import tomllib

from .compare import OUTCOMES

ORACLES = ("trino", "postgres", "value")
PROTOCOLS = ("trino", "pg")
FILE_KEYS = {"description", "oracle", "protocol", "compare", "time_zone", "setup", "setup_trino",
             "setup_codelake", "setup_postgres", "case"}
CASE_KEYS = {"id", "sql", "steps", "oracle", "protocol", "compare", "types", "expect",
             "known", "accepted", "diverges_at", "error", "params", "param_types",
             "rows_json", "oracle_sql", "codelake_session", "time_zone", "note"}
# A reference names where the divergence is tracked: an accepted issue, an ADR,
# or the front (branch) fixing it, optionally followed by details.
REF = re.compile(r"^(ISS-\d{3}|ADR \d{4}|front/[\w.-]+)\b")


class CaseError(Exception):
    pass


class Case:
    def __init__(self, suite, raw, defaults):
        self.suite = suite
        unknown = set(raw) - CASE_KEYS
        if unknown:
            raise CaseError(f"{suite}: unknown case keys {sorted(unknown)}")
        self.id = raw.get("id") or _fail(f"{suite}: a case without id")
        where = f"{suite}:{self.id}"
        self.sql = raw.get("sql")
        self.steps = raw.get("steps")
        if (self.sql is None) == (self.steps is None):
            raise CaseError(f"{where}: exactly one of sql / steps")
        # A step is its SQL, or { sql = "…", any_error = true } for a statement there only to
        # fail (to abort a transaction block): any error on both sides matches, whatever its
        # SQLSTATE. SQLSTATEs are checked by the cases about them.
        self.any_error = set()
        if self.steps is not None:
            plain = []
            for i, step in enumerate(self.steps, 1):
                if isinstance(step, dict):
                    if set(step) - {"sql", "any_error"} or "sql" not in step:
                        raise CaseError(f"{where}: step {i} takes sql and any_error only")
                    if step.get("any_error"):
                        self.any_error.add(i)
                    step = step["sql"]
                plain.append(step)
            self.steps = plain
        self.oracle = raw.get("oracle", defaults.get("oracle", "trino"))
        if self.oracle not in ORACLES:
            raise CaseError(f"{where}: oracle must be one of {ORACLES}")
        default_protocol = "pg" if self.oracle == "postgres" else "trino"
        self.protocol = raw.get("protocol", defaults.get("protocol", default_protocol))
        if self.protocol not in PROTOCOLS:
            raise CaseError(f"{where}: protocol must be one of {PROTOCOLS}")
        if self.oracle == "postgres" and self.protocol != "pg":
            raise CaseError(f"{where}: the postgres oracle is compared over the pg protocol")
        if self.steps is not None and self.oracle != "postgres":
            raise CaseError(f"{where}: steps (one session, statement by statement) need the postgres oracle")
        self.oracle_sql = raw.get("oracle_sql")
        if self.oracle_sql is not None and self.oracle != "trino":
            raise CaseError(f"{where}: oracle_sql is the SQL Trino runs in place of sql")
        default_compare = "ordered" if self.steps is not None else "multiset"
        self.compare = raw.get("compare", defaults.get("compare", default_compare))
        if self.compare not in ("multiset", "ordered"):
            raise CaseError(f"{where}: compare must be multiset or ordered")
        self.types = raw.get("types", self.protocol == "trino" and self.oracle == "trino")
        self.expect = raw.get("expect", "OK")
        if self.expect not in OUTCOMES:
            raise CaseError(f"{where}: expect must be one of {OUTCOMES}")
        self.known, self.accepted = raw.get("known"), raw.get("accepted")
        if self.expect == "OK":
            if self.known or self.accepted or raw.get("diverges_at") or raw.get("error"):
                raise CaseError(f"{where}: known/accepted/diverges_at/error need an expect other than OK")
        else:
            if bool(self.known) == bool(self.accepted):
                raise CaseError(f"{where}: expect {self.expect} needs exactly one of known (open defect) "
                                "or accepted (deliberate divergence)")
            ref = self.known or self.accepted
            if not REF.match(ref):
                raise CaseError(f"{where}: reference {ref!r} must start with ISS-NNN, ADR NNNN or front/<branch>")
        self.diverges_at = raw.get("diverges_at")
        if self.diverges_at is not None:
            if self.steps is None:
                raise CaseError(f"{where}: diverges_at is for steps")
            if not all(isinstance(i, int) and 1 <= i <= len(self.steps) for i in self.diverges_at):
                raise CaseError(f"{where}: diverges_at holds 1-based step numbers")
        elif self.steps is not None and self.expect != "OK":
            raise CaseError(f"{where}: a divergent session names its divergent steps (diverges_at)")
        self.error = raw.get("error")
        self.params = raw.get("params")
        self.param_types = raw.get("param_types")
        if self.params is not None and (self.protocol != "pg" or self.sql is None):
            raise CaseError(f"{where}: params are for one sql over the pg protocol")
        self.rows = None
        if self.oracle == "value":
            if "rows_json" not in raw:
                raise CaseError(f"{where}: the value oracle needs rows_json")
            self.rows = json.loads(raw["rows_json"])
        elif "rows_json" in raw:
            raise CaseError(f"{where}: rows_json is for the value oracle")
        self.session = raw.get("codelake_session")
        self.time_zone = raw.get("time_zone", defaults.get("time_zone"))
        if self.time_zone is not None and (self.protocol != "trino" or self.oracle != "trino"):
            raise CaseError(f"{where}: time_zone (X-Trino-Time-Zone, to both engines) is for the trino oracle "
                            "over the trino protocol")
        self.note = raw.get("note")

    @property
    def ref(self):
        return self.known or self.accepted


def _fail(msg):
    raise CaseError(msg)


class Suite:
    def __init__(self, path):
        self.path = path
        self.name = os.path.splitext(os.path.basename(path))[0]
        with open(path, "rb") as f:
            raw = tomllib.load(f)
        unknown = set(raw) - FILE_KEYS
        if unknown:
            raise CaseError(f"{self.name}: unknown file keys {sorted(unknown)}")
        self.description = raw.get("description", "")
        defaults = {k: raw[k] for k in ("oracle", "protocol", "compare", "time_zone") if k in raw}
        self.setup = raw.get("setup", [])
        self.setup_trino = raw.get("setup_trino", [])
        self.setup_codelake = raw.get("setup_codelake", [])
        self.setup_postgres = raw.get("setup_postgres", [])
        self.cases = [Case(self.name, c, defaults) for c in raw.get("case", [])]
        ids = [c.id for c in self.cases]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise CaseError(f"{self.name}: duplicated case ids {sorted(dupes)}")

    def needs(self):
        """Engines this suite talks to, so the stack starts only what is needed."""
        need = {"codelake"}
        if self.setup or self.setup_trino or any(c.oracle == "trino" for c in self.cases):
            need.add("trino")
        if self.setup_postgres or any(c.oracle == "postgres" for c in self.cases):
            need.add("postgres")
        return need


def load(directory, only=None):
    suites = []
    for name in sorted(os.listdir(directory)):
        if name.endswith(".toml") and (not only or os.path.splitext(name)[0] in only):
            suites.append(Suite(os.path.join(directory, name)))
    if only:
        missing = set(only) - {s.name for s in suites}
        if missing:
            raise CaseError(f"no case file named {sorted(missing)}")
    return suites
