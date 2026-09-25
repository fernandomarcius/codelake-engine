# syntax=docker/dockerfile:1.7
# Shadow-run harness: dbt-trino (the client both engines are driven with) and
# DuckDB (the comparison), pinned by hash in requirements.txt. Base image
# pinned by digest.
FROM python:3.12-slim@sha256:2f17fc044b579bab302c2e8054d3a686e2cb9a83de48e70534b94cd8ebbe06a9

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false

COPY requirements.txt /opt/shadow/requirements.txt
RUN pip install --require-hashes --no-deps -r /opt/shadow/requirements.txt \
 && pip check
# DuckDB's S3 support, fetched at build time so runs need no extension
# download. Read by every user from a fixed directory.
RUN mkdir -p /opt/duckdb-extensions \
 && python -c "import duckdb; c = duckdb.connect(config={'extension_directory': '/opt/duckdb-extensions'}); c.execute('INSTALL httpfs')" \
 && chmod -R a+rX /opt/duckdb-extensions
ENV CODELAKE_SHADOW_DUCKDB_EXTENSIONS=/opt/duckdb-extensions

COPY codelake_shadow /opt/shadow/codelake_shadow
COPY tests /opt/shadow/tests
WORKDIR /opt/shadow
# dbt writes its user file and caches under $HOME; runs as any uid.
ENV HOME=/tmp
USER 65532:65532
ENTRYPOINT ["python", "-m", "codelake_shadow"]
