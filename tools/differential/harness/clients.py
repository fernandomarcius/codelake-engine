"""Clients the harness talks through: the Trino HTTP statement protocol and the
PostgreSQL wire protocol (simple and extended query), standard library only.

Every call returns a result dict and never raises for a query error:
  {"rows": [[...]], "types": [...], "names": [...]}   an answer
  {"error": "<code>", "message": "..."}      a refusal (Trino errorName or SQLSTATE)
  {"error": "HANG", "message": "..."}        no answer within the timeout
"""

import json
import socket
import struct
import time
import urllib.error
import urllib.request

QUERY_TIMEOUT = 280


def _http(method, url, body=None, headers=None, timeout=300):
    req = urllib.request.Request(url, data=body.encode() if body is not None else None, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


class Trino:
    """A Trino protocol endpoint (Trino itself, or CodeLake's `[server.trino]`)."""

    def __init__(self, base, user="differential"):
        self.base = base.rstrip("/")
        self.user = user

    def query(self, sql, catalog, schema=None, session=None):
        h = {"X-Trino-User": self.user, "X-Trino-Catalog": catalog,
             "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME"}
        if schema:
            h["X-Trino-Schema"] = schema
        if session:
            h["X-Trino-Session"] = session
        try:
            st, body = _http("POST", self.base + "/v1/statement", sql, h)
        except (OSError, TimeoutError) as e:
            return {"error": "HANG", "message": str(e)}
        if st != 200:
            return {"error": f"HTTP{st}", "message": body[:400]}
        page = json.loads(body)
        if not page.get("nextUri"):
            # The reference engine answers every submission with a nextUri, and a client
            # may follow it without reading the rest (the .NET driver does).
            return {"error": "NO_NEXT_URI", "message": body[:400]}
        cols, rows = None, []
        started = time.time()
        while True:
            if cols is None and page.get("columns"):
                cols = page["columns"]
            rows.extend(page.get("data") or [])
            if page.get("error"):
                e = page["error"]
                return {"error": e.get("errorName") or "ERROR", "message": (e.get("message") or "")[:400]}
            nxt = page.get("nextUri")
            if not nxt:
                break
            if time.time() - started > QUERY_TIMEOUT:
                return {"error": "HANG", "message": f"no answer in {QUERY_TIMEOUT}s"}
            try:
                st, body = _http("GET", nxt, None, {"X-Trino-User": self.user})
            except (OSError, TimeoutError) as e:
                return {"error": "HANG", "message": str(e)}
            if st != 200:
                return {"error": f"HTTP{st}", "message": body[:400]}
            page = json.loads(body)
        cols = cols or []
        return {"types": [c["type"] for c in cols], "names": [c["name"] for c in cols], "rows": rows}

    def ready(self):
        try:
            st, body = _http("GET", self.base + "/v1/info", timeout=5)
        except (OSError, TimeoutError):
            return False
        return st == 200 and '"starting":false' in body.replace(" ", "")


class PgError(Exception):
    pass


class Pg:
    """One PostgreSQL wire connection (CodeLake's `pg_listen`, or PostgreSQL)."""

    def __init__(self, host, port, user, database, password=None, timeout=15.0):
        self.timeout = timeout
        self.broken = False
        self.s = socket.create_connection((host, port), timeout=timeout)
        params = b"user\0" + user.encode() + b"\0database\0" + database.encode() + b"\0\0"
        body = struct.pack("!i", 196608) + params
        self.s.sendall(struct.pack("!i", len(body) + 4) + body)
        while True:
            t, p = self._msg()
            if t == b"R":
                code = struct.unpack("!i", p[:4])[0]
                if code == 3:
                    self._send(b"p", (password or user).encode() + b"\0")
                elif code != 0:
                    raise PgError(f"authentication method {code} not supported")
            elif t == b"E":
                raise PgError(self._err(p)[1])
            elif t == b"Z":
                break

    def close(self):
        try:
            self._send(b"X", b"")
        except OSError:
            pass
        self.s.close()

    def _recvn(self, n):
        b = b""
        while len(b) < n:
            chunk = self.s.recv(n - len(b))
            if not chunk:
                raise PgError("connection closed")
            b += chunk
        return b

    def _msg(self):
        t = self._recvn(1)
        n = struct.unpack("!i", self._recvn(4))[0]
        return t, self._recvn(n - 4)

    def _send(self, t, p):
        self.s.sendall(t + struct.pack("!i", len(p) + 4) + p)

    @staticmethod
    def _err(p):
        f = {}
        for part in p.split(b"\0"):
            if part:
                f[chr(part[0])] = part[1:].decode(errors="replace")
        return f.get("C", "XX000"), f.get("M", "")

    def _collect(self):
        """Rows of every statement until ReadyForQuery; an error wins, as in libpq."""
        rows, err = [], None
        while True:
            t, p = self._msg()
            if t == b"D":
                n = struct.unpack("!h", p[:2])[0]
                i, row = 2, []
                for _ in range(n):
                    ln = struct.unpack("!i", p[i:i + 4])[0]
                    i += 4
                    if ln < 0:
                        row.append(None)
                    else:
                        row.append(p[i:i + ln].decode(errors="replace"))
                        i += ln
                rows.append(row)
            elif t == b"E":
                err = self._err(p)
            elif t == b"Z":
                if err:
                    return {"error": err[0], "message": err[1][:400]}
                return {"rows": rows}

    def _guard(self, work):
        if self.broken:
            return {"error": "HANG", "message": "connection lost to an earlier hang"}
        try:
            return work()
        except (TimeoutError, socket.timeout):
            self.broken = True
            return {"error": "HANG", "message": f"no answer in {self.timeout}s"}
        except (OSError, PgError) as e:
            self.broken = True
            return {"error": "CONNECTION", "message": str(e)}

    def query(self, sql):
        def work():
            self._send(b"Q", sql.encode() + b"\0")
            return self._collect()
        return self._guard(work)

    def extended(self, sql, params, oids=None):
        oids = oids or [0] * len(params)

        def work():
            p = b"\0" + sql.encode() + b"\0" + struct.pack("!h", len(oids))
            p += b"".join(struct.pack("!i", o) for o in oids)
            self._send(b"P", p)
            b = b"\0\0" + struct.pack("!h", 0) + struct.pack("!h", len(params))
            for v in params:
                if v is None:
                    b += struct.pack("!i", -1)
                else:
                    e = v.encode()
                    b += struct.pack("!i", len(e)) + e
            b += struct.pack("!h", 0)
            self._send(b"B", b)
            self._send(b"E", b"\0" + struct.pack("!i", 0))
            self._send(b"S", b"")
            return self._collect()
        return self._guard(work)
