"""Expected differences: declared, documented differences between the engines.

The candidate removed some limitations it would otherwise have copied from
the reference engine (accepted issues in ``docs/issues``): a CTAS of a literal
makes an unbounded ``varchar`` instead of ``varchar(n)`` (ISS-019), names keep
the case they were written with (ISS-018). Such a difference is not a defect,
but it must stay visible. A rule file declares them:

.. code-block:: json

    {"differences": [
      {"id": "ISS-019", "kind": "declared_type",
       "reason": "CTAS of a literal infers unbounded varchar",
       "table": "marts.orders_by_status", "column": "label",
       "reference": ["varchar(*)"], "candidate": ["varchar", "string"]},
      {"id": "ISS-018", "kind": "name_case",
       "reason": "Case-preserving names"}
    ]}

- ``kind`` is ``declared_type`` (a column's declared type differs),
  ``name_case`` (a table or column name differs only by letter case) or
  ``replaced_characters`` (the reference replaced characters of a text
  column with U+FFFD; ``characters`` names which: ``c1``, the C1 controls
  U+0080-U+009F, ISS-047).
- ``table`` (``schema.table``) and ``column``: globs for ``name_case``
  (default ``*``); exact names for ``declared_type``, required. A type
  difference is expected only where someone checked that column: the same
  change elsewhere can be a defect (ISS-019's ``varchar(n)`` -> unbounded is
  also what a lost source length looks like, finding F8-2).
- ``reference`` / ``candidate``: globs (one or a list) over the declared type
  of each side, lower case, without spaces (``decimal(18,6)``). A
  ``declared_type`` rule must constrain at least one side: a rule that would
  accept any type change is refused.

A ``declared_type`` or ``name_case`` rule only classifies a difference of
**type** or **name case** as expected and documented. Values are always
compared: such a rule never turns a checksum, row count or column difference
into an equal table. The one exception is ``replaced_characters``, for one
exact table and column: the candidate's values of that column are compared
after the same replacement the reference made (each character of the class
becomes U+FFFD), every other value and column exactly; the rows it changed
are counted in the report.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import json

KINDS = ("declared_type", "name_case", "replaced_characters")

#: Character classes a ``replaced_characters`` rule may name, as regular
#: expressions (RE2, DuckDB's ``regexp_replace``).
CHARACTER_CLASSES = {"c1": "[\u0080-\u009f]"}


class ExpectedError(ValueError):
    pass


def normalize_type(text: str) -> str:
    """Declared type as rules match it: lower case, no whitespace."""
    return "".join(text.split()).lower()


def _globs(value: object, where: str, errors: list[str]) -> tuple[str, ...]:
    if value is None:
        return ("*",)
    items = [value] if isinstance(value, str) else value
    if not isinstance(items, list) or not items or not all(isinstance(i, str) and i for i in items):
        errors.append(f"{where}: must be a non-empty string or list of strings")
        return ("*",)
    return tuple(normalize_type(i) for i in items)


@dataclasses.dataclass(frozen=True)
class Rule:
    id: str
    reason: str
    kind: str
    table: str = "*"
    column: str = "*"
    reference: tuple[str, ...] = ("*",)
    candidate: tuple[str, ...] = ("*",)
    characters: str = ""

    def _scope(self, table: str, column: str) -> bool:
        return fnmatch.fnmatchcase(table.lower(), self.table.lower()) and fnmatch.fnmatchcase(
            column.lower(), self.column.lower()
        )

    def matches(self, kind: str, table: str, column: str, reference: str = "", candidate: str = "") -> bool:
        if kind != self.kind or not self._scope(table, column):
            return False
        if kind == "name_case":
            return True
        ref, cand = normalize_type(reference), normalize_type(candidate)
        return any(fnmatch.fnmatchcase(ref, g) for g in self.reference) and any(
            fnmatch.fnmatchcase(cand, g) for g in self.candidate
        )


def parse(document: object, source: str = "expected differences") -> list[Rule]:
    """Rules of a parsed document; every problem is reported at once."""
    errors: list[str] = []
    entries = document.get("differences") if isinstance(document, dict) else None
    if not isinstance(entries, list):
        raise ExpectedError(f"{source}: expected an object with a `differences` list")
    rules = []
    for index, entry in enumerate(entries):
        where = f"{source}: differences[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{where}: must be an object")
            continue
        unknown = sorted(
            set(entry) - {"id", "reason", "kind", "table", "column", "reference", "candidate", "characters"}
        )
        if unknown:
            errors.append(f"{where}: unknown keys {', '.join(unknown)}")
        rule_id, reason, kind = entry.get("id"), entry.get("reason"), entry.get("kind")
        if not isinstance(rule_id, str) or not rule_id.strip():
            errors.append(f"{where}: `id` is required (the accepted issue that documents it)")
        if not isinstance(reason, str) or not reason.strip():
            errors.append(f"{where}: `reason` is required")
        if kind not in KINDS:
            errors.append(f"{where}: `kind` must be one of {', '.join(KINDS)}")
        table, column = entry.get("table", "*"), entry.get("column", "*")
        for key, value in (("table", table), ("column", column)):
            if not isinstance(value, str) or not value:
                errors.append(f"{where}: `{key}` must be a non-empty glob")
        reference = _globs(entry.get("reference"), f"{where}.reference", errors)
        candidate = _globs(entry.get("candidate"), f"{where}.candidate", errors)
        if kind in ("declared_type", "replaced_characters"):
            for key, value in (("table", table), ("column", column)):
                if key not in entry or (isinstance(value, str) and any(ch in value for ch in "*?[")):
                    errors.append(
                        f"{where}: a {kind} rule names one `{key}` exactly (no glob): "
                        "a difference is expected only on the columns it was checked on"
                    )
        if kind == "declared_type" and reference == ("*",) and candidate == ("*",):
            errors.append(f"{where}: a declared_type rule must name the reference or the candidate type")
        if kind in ("name_case", "replaced_characters") and ("reference" in entry or "candidate" in entry):
            errors.append(f"{where}: a {kind} rule takes no types")
        characters = entry.get("characters", "")
        if kind == "replaced_characters" and characters not in CHARACTER_CLASSES:
            errors.append(
                f"{where}: a replaced_characters rule names `characters`: one of {', '.join(CHARACTER_CLASSES)}"
            )
        if kind != "replaced_characters" and "characters" in entry:
            errors.append(f"{where}: only a replaced_characters rule takes `characters`")
        rules.append(
            Rule(
                id=str(rule_id or "").strip(),
                reason=str(reason or "").strip(),
                kind=str(kind),
                table=str(table),
                column=str(column),
                reference=reference,
                candidate=candidate,
                characters=str(characters or ""),
            )
        )
    if errors:
        raise ExpectedError("invalid expected differences:\n  - " + "\n  - ".join(errors))
    return rules


def load(path: str) -> list[Rule]:
    """Rules of a JSON file; an empty path means none."""
    if not path:
        return []
    try:
        with open(path, encoding="utf-8") as f:
            document = json.load(f)
    except OSError as err:
        raise ExpectedError(f"cannot read expected differences {path}: {err.strerror}") from None
    except ValueError as err:
        raise ExpectedError(f"expected differences {path} is not valid JSON: {err}") from None
    return parse(document, path)


def match(rules: list[Rule], kind: str, table: str, column: str, reference: str = "", candidate: str = "") -> Rule | None:
    """The first rule that declares this difference, if any."""
    for rule in rules:
        if rule.matches(kind, table, column, reference, candidate):
            return rule
    return None


def replacements(rules: list[Rule], table: str, column: str) -> Rule | None:
    """The ``replaced_characters`` rule of this exact table and column."""
    for rule in rules:
        if rule.kind == "replaced_characters" and rule.table.lower() == table.lower() and (
            rule.column.lower() == column.lower()
        ):
            return rule
    return None
