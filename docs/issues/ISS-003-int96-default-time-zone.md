# ISS-003 — INT96 time zone configurable per hive catalog, default `America/Sao_Paulo`

- Kind: compatibility risk · Accepted by: Fernando Farias, 2026-09-24 (risk explained before acceptance) · Status: open (implementation pending in the W1 front)
- Accepted: `parquet_time_zone` per hive catalog, IANA name, default `America/Sao_Paulo`, matching the reference engine's `hive.parquet.time-zone` as deployed.
- Risk: Parquet files whose INT96 values were written in UTC (by another engine or another installation) are read shifted by the zone offset; the default is installation-specific, not neutral.
- Revisit when: an installation reads files from more than one writer, or INT96 is dropped in favor of INT64 timestamps.
- References: ADR 0007; W1 front.
