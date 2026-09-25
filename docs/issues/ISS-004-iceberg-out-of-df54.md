# ISS-004 — DataFusion 54.1 train without Iceberg

- Kind: scope · Accepted by: Fernando Farias, 2026-09-24 · Status: open
- Accepted: DataFusion 54.1, datafusion-federation =0.5.5, table-providers 0.13.1, datafusion-postgres 0.18.0; the Iceberg connector parked outside the build.
- Risk: no Iceberg source or sink until a published integration exists for the train (M3 / F12 blocked); a config declaring an Iceberg source fails at startup with a clear message.
- Revisit when: an Iceberg DataFusion integration is published for the train's DataFusion major with opendal ≥ 0.58.1.
- References: ADR 0002, ADR 0013.
