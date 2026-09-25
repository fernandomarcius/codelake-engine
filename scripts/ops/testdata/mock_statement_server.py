#!/usr/bin/env python3
"""Minimal stand-in for the engine's HTTP statement protocol, for
scripts/ops/canary-selftest.sh. Answers a fixed set of statements with paged
results, errors, slow pages and a query that never finishes. Not a server to
run against anything real.

Usage: mock_statement_server.py <port-file> <cancel-log>
Writes the port it listens on to <port-file>; appends the id of every canceled
query to <cancel-log>. A base path of /ref serves the "reference engine"
variant (see `SELECT drift`).
"""

import base64
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PREFIX = "X-Trino-"
QUERIES = {}
LOCK = threading.Lock()
COUNTER = [0]


def answer(sql, reference):
    """(columns, pages, error, delay) for a statement."""
    now = time.time()
    one = [{"name": "v", "type": "bigint"}]
    if sql == "SELECT 42":
        return one, [[], [[42]]], None, 0
    if sql == "SELECT rows":
        cols = [{"name": "k", "type": "varchar"}, {"name": "n", "type": "bigint"}]
        return cols, [[["b", 2]], [["a", 1], [None, 3]]], None, 0
    if sql == "SELECT fresh":
        return one, [[[now - 10]]], None, 0
    if sql == "SELECT stale":
        return one, [[[now - 10_000]]], None, 0
    if sql == "SELECT slow":
        return one, [[[1]]], None, 1.5
    if sql == "SELECT fail":
        error = {"errorName": "TABLE_NOT_FOUND", "errorType": "USER_ERROR",
                 "message": "line 1:15: Table 'lake.s.missing' does not exist"}
        return None, [], error, 0
    if sql == "SELECT hang":
        return one, None, None, 0
    if sql == "SELECT stall":  # a page that takes longer than the client waits
        return one, None, None, 5
    if sql == "SELECT cut":  # the stream stops after one page, still RUNNING
        return one, [[[1]]], None, 0
    if sql == "SELECT drift":
        return one, [[[7 if reference else 8]]], None, 0
    return None, [], {"errorName": "SYNTAX_ERROR", "message": "unknown statement"}, 0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def send_json(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def base(self, reference):
        host = self.headers.get("Host")
        return f"http://{host}" + ("/ref" if reference else "")

    def do_POST(self):
        reference = self.path.startswith("/ref/")
        if not self.path.endswith("/v1/statement"):
            return self.send_json(404, {})
        length = int(self.headers.get("Content-Length", "0"))
        sql = self.rfile.read(length).decode()
        if not self.headers.get(PREFIX + "User"):
            return self.send_json(401, {"message": "missing user"})
        if sql == "SELECT empty":  # an empty 200 from something in the way
            return self.send_json(200, {})
        if sql == "SELECT elsewhere":  # a nextUri that leaves the endpoint
            return self.send_json(200, {"id": "q0", "stats": {"state": "QUEUED"},
                                        "nextUri": "http://elsewhere.invalid/v1/statement/executing/q0/x/0"})
        with LOCK:
            COUNTER[0] += 1
            qid = f"q{COUNTER[0]}"
            QUERIES[qid] = answer(sql, reference) + (sql,)
            if sql == "SELECT whoami":  # the Basic credentials, as received
                auth = self.headers.get("Authorization", "")
                user = base64.b64decode(auth[6:]).decode() if auth.startswith("Basic ") else ""
                QUERIES[qid] = ([{"name": "u", "type": "varchar"}], [[[user]]], None, 0, sql)
        body = {"id": qid, "stats": {"state": "QUEUED"},
                "nextUri": f"{self.base(reference)}/v1/statement/executing/{qid}/x/0"}
        self.send_json(200, body)

    def do_GET(self):
        reference = self.path.startswith("/ref/")
        parts = self.path.split("/")
        qid, token = parts[-3], int(parts[-1])
        columns, pages, error, delay, sql = QUERIES[qid]
        time.sleep(delay)
        body = {"id": qid}
        if error is not None:
            body["error"] = error
            body["stats"] = {"state": "FAILED"}
            return self.send_json(200, body)
        if pages is None:  # never finishes
            time.sleep(0.2)
            body["stats"] = {"state": "RUNNING"}
            body["nextUri"] = f"{self.base(reference)}/v1/statement/executing/{qid}/x/{token + 1}"
            return self.send_json(200, body)
        body["columns"] = columns
        if token < len(pages) and pages[token]:
            body["data"] = pages[token]
        if token + 1 < len(pages):
            body["nextUri"] = f"{self.base(reference)}/v1/statement/executing/{qid}/x/{token + 1}"
            body["stats"] = {"state": "RUNNING"}
        else:
            body["stats"] = {"state": "RUNNING" if sql == "SELECT cut" else "FINISHED"}
        self.send_json(200, body)

    def do_DELETE(self):
        qid = self.path.split("/")[-3]
        with open(sys.argv[2], "a", encoding="utf-8") as log:
            log.write(qid + "\n")
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    with open(sys.argv[1], "w", encoding="utf-8") as port_file:
        port_file.write(str(server.server_address[1]))
    server.serve_forever()


if __name__ == "__main__":
    main()
