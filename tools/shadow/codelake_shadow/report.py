"""Cycle and cumulative reports: JSON (for machines) and Markdown (for people)."""

from __future__ import annotations

import glob
import json
import os
from typing import Any


def _bytes(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / 1024**2:,.1f} MiB"


def cycle_markdown(cycle: dict[str, Any]) -> str:
    lines = [f"# Shadow run — cycle {cycle['cycle']}", ""]
    lines.append(f"- Started: {cycle['started_at']}; finished: {cycle['finished_at']}")
    lines.append(f"- Cutoff (`{cycle.get('cutoff_var', '')}`): `{cycle.get('cutoff')}`")
    lines.append(f"- Verdict: **{cycle['verdict']}**")
    lines.append("")
    lines.append("| Side | dbt exit | seconds | nodes | statuses | peak RSS | samples |")
    lines.append("|---|---|---|---|---|---|---|")
    for name in ("reference", "candidate"):
        side = cycle["sides"].get(name, {})
        dbt = side.get("dbt", {})
        rss = side.get("rss", {})
        statuses = ", ".join(f"{k} {v}" for k, v in sorted(dbt.get("statuses", {}).items()))
        lines.append(
            f"| {name} | {dbt.get('exit_code')} | {dbt.get('seconds')} | {dbt.get('nodes')} | "
            f"{statuses} | {_bytes(rss.get('peak_bytes'))} | {rss.get('samples', 0)} |"
        )
    ratio = cycle.get("rss_ratio")
    if ratio is not None:
        lines.append("")
        lines.append(f"Peak RSS candidate / reference: **{ratio:.2%}**")
    if cycle.get("model_status_differences"):
        lines += ["", "## Model statuses that differ", "", "| Node | reference | candidate |", "|---|---|---|"]
        for d in cycle["model_status_differences"]:
            lines.append(f"| `{d['node']}` | {d['reference']} | {d['candidate']} |")
    lines += ["", "## Tables", "", "| Table | Status | Rows (ref / cand) | Columns differing |", "|---|---|---|---|"]
    for t in cycle["tables"]:
        rows = t.get("rows", {})
        cols = ", ".join(c["column"] for c in t.get("columns_differing", []))
        lines.append(
            f"| `{t['table']}` | {t['status']}{' (' + ', '.join(t['reasons']) + ')' if t.get('reasons') else ''} | "
            f"{rows.get('reference', '-')} / {rows.get('candidate', '-')} | {cols} |"
        )
    divergent = [t for t in cycle["tables"] if t["status"] != "equal"]
    for t in divergent:
        lines += ["", f"### `{t['table']}`", ""]
        schema = t.get("schema", {})
        for key in ("only_reference", "only_candidate"):
            if schema.get(key):
                lines.append(f"- Columns {key.replace('_', ' ')}: {', '.join(schema[key])}")
        for m in schema.get("type_class_mismatch", []):
            lines.append(f"- Type class differs: `{m['column']}` {m['reference']} vs {m['candidate']}")
        for m in schema.get("type_differences", []):
            lines.append(
                f"- Declared type differs ({schema.get('declared_types_from', 'metastore')}), "
                f"no rule documents it: `{m['column']}` {m['reference']} vs {m['candidate']}"
            )
        for m in t.get("name_case_differences", []):
            lines.append(
                f"- Name differs by case, no rule documents it: {m['object']} `{m['reference']}` vs `{m['candidate']}`"
            )
        if t.get("error"):
            lines.append(f"- Not compared: {t['error']}")
        if "differing_row_hashes" in t:
            lines.append(f"- Row hashes with a different count: {t['differing_row_hashes']}")
            for s in t.get("sample", []):
                row = f" — `{s['row']}`" if "row" in s else ""
                lines.append(f"  - `{s['hash']}` reference {s['reference']}, candidate {s['candidate']}{row}")
    if cycle.get("skipped_models"):
        lines += ["", "## Not compared", ""]
        for s in cycle["skipped_models"]:
            lines.append(f"- `{s['model']}`: {s['reason']}")
    documented = [(t["table"], e) for t in cycle["tables"] for e in t.get("expected_differences", [])]
    if documented:
        lines += [
            "",
            "## Expected and documented differences (type or name case only; values compared)",
            "",
            "| Table | Rule | Kind | What | Reference | Candidate |",
            "|---|---|---|---|---|---|",
        ]
        for table, e in documented:
            what = e.get("column") or e.get("object", "")
            lines.append(
                f"| `{table}` | {e['rule']} | {e['kind']} | `{what}` | `{e.get('reference', '')}` | "
                f"`{e.get('candidate', '')}` |"
            )
        reasons = {e["rule"]: e["reason"] for _, e in documented}
        lines.append("")
        for rule, reason in sorted(reasons.items()):
            lines.append(f"- {rule}: {reason}")
    warnings = [
        (t["table"], d) for t in cycle["tables"] for d in t.get("schema", {}).get("storage_type_differences", [])
    ]
    if warnings:
        lines += ["", "## Storage type differences under equal declared types (values compared by class, not a divergence)", ""]
        for table, d in warnings:
            lines.append(f"- `{table}.{d['column']}`: {d['reference']} vs {d['candidate']}")
    return "\n".join(lines) + "\n"


def cycles(out_dir: str) -> list[dict[str, Any]]:
    found = []
    for path in sorted(glob.glob(os.path.join(out_dir, "cycle-*.json"))):
        with open(path, encoding="utf-8") as f:
            found.append(json.load(f))
    return found


def next_cycle_number(out_dir: str) -> int:
    existing = cycles(out_dir)
    return max((c["cycle"] for c in existing), default=0) + 1


def _expected_counts(all_cycles: list[dict[str, Any]]) -> dict[str, int]:
    """Occurrences of each documented difference, by rule, over every cycle."""
    counts: dict[str, int] = {}
    for c in all_cycles:
        for t in c["tables"]:
            for e in t.get("expected_differences", []):
                counts[e["rule"]] = counts.get(e["rule"], 0) + 1
    return dict(sorted(counts.items()))


def cumulative(out_dir: str) -> dict[str, Any]:
    all_cycles = cycles(out_dir)
    peaks = {"reference": [], "candidate": []}
    for c in all_cycles:
        for side in peaks:
            peak = c["sides"].get(side, {}).get("rss", {}).get("peak_bytes")
            if peak is not None:
                peaks[side].append(peak)
    max_ref = max(peaks["reference"], default=None)
    max_cand = max(peaks["candidate"], default=None)
    return {
        "cycles": len(all_cycles),
        "equal_cycles": sum(1 for c in all_cycles if c["verdict"] == "equal"),
        "divergent_cycles": [c["cycle"] for c in all_cycles if c["verdict"] != "equal"],
        "tables_compared": sum(len(c["tables"]) for c in all_cycles),
        "expected_differences": _expected_counts(all_cycles),
        "peak_rss_bytes": {"reference": max_ref, "candidate": max_cand},
        "peak_rss_ratio": (max_cand / max_ref) if max_ref and max_cand is not None else None,
        "verdict": "equal" if all_cycles and all(c["verdict"] == "equal" for c in all_cycles) else "divergent",
        "per_cycle": [
            {
                "cycle": c["cycle"],
                "started_at": c["started_at"],
                "cutoff": c.get("cutoff"),
                "verdict": c["verdict"],
                "tables": len(c["tables"]),
                "divergent_tables": [t["table"] for t in c["tables"] if t["status"] != "equal"],
                "rss_ratio": c.get("rss_ratio"),
            }
            for c in all_cycles
        ],
    }


def cumulative_markdown(summary: dict[str, Any]) -> str:
    lines = ["# Shadow run — cumulative", ""]
    lines.append(f"- Cycles: {summary['cycles']}, equal: {summary['equal_cycles']}")
    lines.append(f"- Verdict: **{summary['verdict']}**")
    peaks = summary["peak_rss_bytes"]
    lines.append(
        f"- Peak RSS: reference {_bytes(peaks['reference'])}, candidate {_bytes(peaks['candidate'])}"
        + (f" ({summary['peak_rss_ratio']:.2%})" if summary["peak_rss_ratio"] is not None else "")
    )
    if summary.get("expected_differences"):
        lines.append(
            "- Expected and documented differences (type or name case only): "
            + ", ".join(f"{rule} x{n}" for rule, n in summary["expected_differences"].items())
        )
    lines += ["", "| Cycle | Started | Cutoff | Verdict | Tables | Divergent | RSS ratio |", "|---|---|---|---|---|---|---|"]
    for c in summary["per_cycle"]:
        ratio = f"{c['rss_ratio']:.2%}" if c["rss_ratio"] is not None else "n/a"
        lines.append(
            f"| {c['cycle']} | {c['started_at']} | `{c['cutoff']}` | {c['verdict']} | {c['tables']} | "
            f"{', '.join(c['divergent_tables'])} | {ratio} |"
        )
    return "\n".join(lines) + "\n"


def write_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)


def write_text(path: str, text: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)
