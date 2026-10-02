"""Starts the engines the cases need as sibling containers, through the Docker
Engine API on /var/run/docker.sock (the toolbelt image has no docker CLI), and
removes every one of them, the network and the warehouse volume on exit.

Nothing is published on the host: the harness container joins a private
network with Trino 431, PostgreSQL 17 and CodeLake (the binary built by the gate,
copied into a container of the toolbelt image). When PostgreSQL runs, both engines
also read it as a source, through a catalog named `pg` (stack/pg.properties,
stack/codelake-postgres.toml, stack/postgres-source.sql), and through `pg_overflow`
and `pg_rounded`, the same database with the reference's
`decimal-mapping=ALLOW_OVERFLOW` (the second rounding `HALF_EVEN` to scale 2).
"""

import http.client
import io
import json
import os
import secrets
import socket
import tarfile
import tempfile
import time
import urllib.parse

from .clients import Pg, PgError

TRINO_IMAGE = "trinodb/trino:431"
POSTGRES_IMAGE = "postgres:17-alpine"
STACK = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "stack")


class DockerError(Exception):
    pass


class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, path, timeout=600):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class Docker:
    def __init__(self, path="/var/run/docker.sock"):
        if not os.path.exists(path):
            raise DockerError(f"{path} not found: run through scripts/dev.sh, which forwards the Docker socket")
        self.path = path

    def call(self, method, url, body=None, query=None, ok=(200, 201, 204), raw=None, ctype=None, parse=True,
             length=None):
        if query:
            url += "?" + urllib.parse.urlencode(query)
        conn = _UnixConnection(self.path)
        headers = {}
        data = None
        if raw is not None:
            data, headers["Content-Type"] = raw, ctype or "application/x-tar"
            if length is not None:
                headers["Content-Length"] = str(length)
        elif body is not None:
            data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
        conn.request(method, url, body=data, headers=headers)
        r = conn.getresponse()
        payload = r.read()
        conn.close()
        if r.status not in ok:
            raise DockerError(f"{method} {url}: HTTP {r.status} {payload[:300]!r}")
        if parse and payload and r.getheader("Content-Type", "").startswith("application/json"):
            return json.loads(payload)
        return payload

    def image_present(self, image):
        try:
            self.call("GET", f"/images/{image}/json")
            return True
        except DockerError:
            return False

    def pull(self, image):
        name, tag = image.rsplit(":", 1)
        # The answer is a stream of JSON progress objects, not one document.
        self.call("POST", "/images/create", query={"fromImage": name, "tag": tag}, parse=False)
        if not self.image_present(image):
            raise DockerError(f"pull of {image} failed")

    def copy_in(self, container, directory, files):
        """files: {path under directory: (bytes or a local path, mode)}; `directory` must exist.
        The archive is spooled to disk: the debug binary is hundreds of MB."""
        buf = tempfile.TemporaryFile()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for d in sorted({os.path.dirname(n) for n in files if os.path.dirname(n)}):
                info = tarfile.TarInfo(d)
                info.type, info.mode, info.mtime = tarfile.DIRTYPE, 0o755, int(time.time())
                tar.addfile(info)
            for name, (data, mode) in files.items():
                if isinstance(data, str):
                    info = tar.gettarinfo(data, arcname=name)
                    info.mode, info.uid, info.gid, info.uname, info.gname = mode, 0, 0, "root", "root"
                    with open(data, "rb") as f:
                        tar.addfile(info, f)
                else:
                    info = tarfile.TarInfo(name)
                    info.size, info.mode, info.mtime = len(data), mode, int(time.time())
                    tar.addfile(info, io.BytesIO(data))
        size = buf.tell()
        buf.seek(0)
        self.call("PUT", f"/containers/{container}/archive", query={"path": directory}, raw=buf, ok=(200,),
                  length=size)
        buf.close()

    def running(self, container):
        state = self.call("GET", f"/containers/{container}/json")["State"]
        return state.get("Running", False), state.get("ExitCode")

    def logs(self, container, tail=200):
        raw = self.call("GET", f"/containers/{container}/logs", query={"stdout": 1, "stderr": 1, "tail": tail})
        out, i = [], 0
        while i + 8 <= len(raw):  # multiplexed stream: 8-byte frame headers
            n = int.from_bytes(raw[i + 4:i + 8], "big")
            out.append(raw[i + 8:i + 8 + n])
            i += 8 + n
        return b"".join(out).decode(errors="replace")


class Stack:
    """Context manager: the engines, reachable by name from this container."""

    def __init__(self, need, binary, log=print, postgres_setup=()):
        """postgres_setup: (suite, sql) pairs run on PostgreSQL before CodeLake
        starts, which reads its sources' table listing once, at start."""
        self.need, self.binary, self.log = need, binary, log
        self.postgres_setup, self.setup_errors = list(postgres_setup), []
        self.docker = Docker()
        self.tag = "codelake-diff-" + secrets.token_hex(4)
        self.containers, self.network, self.volume, self.joined = [], None, None, None
        self.endpoints = {}

    def __enter__(self):
        try:
            self._start()
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def _self_container(self):
        me = socket.gethostname()
        try:
            return self.docker.call("GET", f"/containers/{me}/json")
        except DockerError as e:
            raise DockerError("the harness must run inside a container (scripts/dev.sh) to join the "
                              f"engines' private network: {e}") from e

    def _run(self, name, image, alias, binds=(), user=None, entrypoint=None, cmd=None, env=None):
        if not self.docker.image_present(image):
            self.log(f"pulling {image}")
            self.docker.pull(image)
        body = {"Image": image, "Labels": {"codelake.differential": self.tag},
                "HostConfig": {"Binds": list(binds), "NetworkMode": self.network},
                "NetworkingConfig": {"EndpointsConfig": {self.network: {"Aliases": [alias]}}}}
        if user:
            body["User"] = user
        if entrypoint:
            body["Entrypoint"] = entrypoint
        if cmd:
            body["Cmd"] = cmd
        if env:
            body["Env"] = env
        created = self.docker.call("POST", "/containers/create", body, query={"name": f"{self.tag}-{name}"})
        self.containers.append(created["Id"])
        return created["Id"]

    def _start(self):
        me = self._self_container()
        own_image = me["Config"]["Image"]
        self.network = self.tag
        self.docker.call("POST", "/networks/create", {"Name": self.network, "Labels": {"codelake.differential": self.tag}})
        self.docker.call("POST", f"/networks/{self.network}/connect", {"Container": me["Id"]}, ok=(200,))
        self.joined = me["Id"]
        self.volume = self.tag + "-wh"
        self.docker.call("POST", "/volumes/create", {"Name": self.volume, "Labels": {"codelake.differential": self.tag}})
        # Both engines write the warehouse: open it before either starts.
        init = self._run("init", own_image, "init", binds=[f"{self.volume}:/wh"],
                         entrypoint=["chmod", "0777", "/wh"])
        self.docker.call("POST", f"/containers/{init}/start")
        self.docker.call("POST", f"/containers/{init}/wait", ok=(200,))

        def read(name):
            with open(os.path.join(STACK, name), "rb") as f:
                return f.read()

        source = "postgres" in self.need
        if source:
            p = self._run("postgres", POSTGRES_IMAGE, "postgres", env=["POSTGRES_HOST_AUTH_METHOD=trust"])
            self.docker.copy_in(p, "/docker-entrypoint-initdb.d",
                                {"source.sql": (read("postgres-source.sql"), 0o644)})
            self.docker.call("POST", f"/containers/{p}/start")
            self.endpoints["postgres"] = ("postgres", 5432)
        if "trino" in self.need:
            t = self._run("trino", TRINO_IMAGE, "trino", binds=[f"{self.volume}:/wh"])
            self.docker.copy_in(t, "/etc/trino", {"jvm.config": (read("jvm.config"), 0o644)})
            catalogs = {"hive.properties": (read("hive.properties"), 0o644)}
            if source:
                for name in ("pg", "pg_overflow", "pg_rounded"):
                    catalogs[f"{name}.properties"] = (read(f"{name}.properties"), 0o644)
            self.docker.copy_in(t, "/etc/trino/catalog", catalogs)
            self.docker.call("POST", f"/containers/{t}/start")
            self.endpoints["trino"] = "http://trino:8080"
        config = read("codelake.toml")
        if source:
            # CodeLake attaches its sources when it starts: PostgreSQL must be up.
            self._wait_postgres()
            self._setup_postgres()
            config += read("codelake-postgres.toml")
        c = self._run("codelake", own_image, "codelake", binds=[f"{self.volume}:/wh"], user="1000:1000",
                      entrypoint=["/opt/codelake/codelake"], cmd=["serve", "--config", "/opt/codelake/codelake.toml"],
                      env=["CODELAKE_DIFF_PG_PASSWORD=unused"] if source else None)
        self.docker.copy_in(c, "/opt", {"codelake/codelake": (self.binary, 0o755),
                                        "codelake/codelake.toml": (config, 0o644)})
        self.codelake = c
        self.docker.call("POST", f"/containers/{c}/start")
        self.endpoints["codelake_trino"] = "http://codelake:8081"
        self.endpoints["codelake_pg"] = ("codelake", 5432)

    def _wait_postgres(self, timeout=120):
        """Until PostgreSQL accepts TCP connections as `source`: its entrypoint
        runs the init scripts on a server that listens on a socket only."""
        deadline = time.time() + timeout
        while True:
            try:
                Pg("postgres", 5432, "source", "postgres").close()
                return
            except (OSError, PgError):
                if time.time() > deadline:
                    raise DockerError(f"postgres did not accept connections in {timeout}s") from None
                time.sleep(1)

    def _setup_postgres(self):
        pg = Pg("postgres", 5432, "postgres", "postgres")
        try:
            for suite, sql in self.postgres_setup:
                result = pg.query(sql)
                if "error" in result:
                    self.setup_errors.append({"suite": suite, "engine": "postgres", "sql": sql,
                                              "result": {"error": result["error"],
                                                         "message": result.get("message", "")[:400]}})
        finally:
            pg.close()

    def codelake_alive(self):
        running, code = self.docker.running(self.codelake)
        if not running:
            raise RuntimeError(f"the CodeLake container exited with code {code}")

    def codelake_logs(self):
        return self.docker.logs(self.codelake)

    def __exit__(self, *exc):
        for cid in reversed(self.containers):
            try:
                self.docker.call("DELETE", f"/containers/{cid}", query={"force": 1, "v": 1}, ok=(204, 404))
            except DockerError as e:
                self.log(f"cleanup: {e}")
        if self.joined:
            try:
                self.docker.call("POST", f"/networks/{self.network}/disconnect",
                                 {"Container": self.joined, "Force": True}, ok=(200, 404))
            except DockerError as e:
                self.log(f"cleanup: {e}")
        if self.network:
            try:
                self.docker.call("DELETE", f"/networks/{self.network}", ok=(204, 404))
            except DockerError as e:
                self.log(f"cleanup: {e}")
        if self.volume:
            try:
                self.docker.call("DELETE", f"/volumes/{self.volume}", ok=(204, 404))
            except DockerError as e:
                self.log(f"cleanup: {e}")
        return False
