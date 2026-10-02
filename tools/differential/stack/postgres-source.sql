-- PostgreSQL 17 as a source both engines read through a `pg` catalog (Trino's
-- postgresql connector, CodeLake's postgres connector), besides being an oracle.
-- They connect as `source`, whose default zone is not UTC (as in a Brazilian
-- deployment): a session that kept it would read a literal that only means
-- something in the session's zone differently. Neither engine keeps it (Trino
-- sets Etc/UTC, CodeLake the client's zone; ISS-060). Case files create their tables in
-- the `source` schema (setup_postgres runs as postgres).
CREATE ROLE source LOGIN;
ALTER ROLE source SET timezone TO 'America/Sao_Paulo';
CREATE SCHEMA source AUTHORIZATION source;
ALTER DEFAULT PRIVILEGES IN SCHEMA source GRANT SELECT ON TABLES TO source;
