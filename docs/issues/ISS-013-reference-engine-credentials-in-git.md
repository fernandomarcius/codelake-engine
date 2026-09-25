# ISS-013 — Reference engine catalog credentials committed in an external repository

- Kind: security (external) · Deferred by: Fernando Farias, 2026-09-24 ("I will do the credential steps later") · Status: open
- Context: the deployment repository of the reference engine versions its catalog files with plaintext passwords and keys (SQL Server, PostgreSQL, S3, MongoDB); no `${ENV:...}` references.
- Accepted for now: no change in that repository; CodeLake's config import never reads, prints or stores those values (secrets go to a 0600 `.env` on the host).
- Risk: anyone with read access to that repository or an old clone holds production credentials.
- To close: rotate every credential, replace values with `${ENV:NAME}` fed from the deployment's secrets, remove stale copies on the host; history rewrite optional.
