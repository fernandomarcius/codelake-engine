#!/usr/bin/env python3
"""Runs golden cases on Trino and stores the results in the fixture files.

Usage: trino_golden.py <trino-url> <fixture.json>...

A fixture is {"category": ..., "cases": [{"name", "sql", ["ulps"], "trino"}]}.
"trino" becomes {"types": [...], "rows": [[...]]} or {"error": NAME, "message": TEXT}.
"""
import json
import sys
import time
import urllib.request

HEADERS = {
    "X-Trino-User": "codelake-golden",
    "X-Trino-Time-Zone": "UTC",
    "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME",
}


def run(url, sql):
    request = urllib.request.Request(url + "/v1/statement", data=sql.encode(), headers=HEADERS)
    response = json.load(urllib.request.urlopen(request))
    columns, rows = None, []
    while True:
        if columns is None and "columns" in response:
            columns = response["columns"]
        rows += response.get("data", [])
        if "error" in response:
            error = response["error"]
            return {"error": error.get("errorName"), "message": error.get("message")}
        if "nextUri" not in response:
            break
        response = json.load(urllib.request.urlopen(response["nextUri"]))
    return {"types": [c["type"] for c in columns or []], "rows": rows}


def wait_for_active_worker(url, seconds=240):
    """The server logs "SERVER STARTED" before its own worker registers; a
    case run in between would record "No nodes available to run query" as
    its golden answer."""
    deadline = time.monotonic() + seconds
    while True:
        answer = run(url, "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")
        if answer.get("rows") and answer["rows"][0][0] > 0:
            return
        if time.monotonic() > deadline:
            sys.exit(f"no active worker at {url}: {answer}")
        time.sleep(1)


def main():
    url, paths = sys.argv[1], sys.argv[2:]
    wait_for_active_worker(url)
    for path in paths:
        with open(path, encoding="utf-8") as handle:
            fixture = json.load(handle)
        for case in fixture["cases"]:
            case["trino"] = run(url, case["sql"])
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(fixture, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
        print(f"{path}: {len(fixture['cases'])} cases")


if __name__ == "__main__":
    main()
