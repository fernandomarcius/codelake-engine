# ISS-062 — Nanoseconds meet a coarser timestamp in microseconds

- Kind: deliberate divergence · Applied by recommendation (review of `fix/union-timestamp-precision`,
  2026-10-02), no explicit human answer yet · Status: awaiting-confirmation
- Context: Trino 431 has one `timestamp(p)` for every `p` up to 12 and the whole year range. An Arrow
  timestamp in nanoseconds holds only the years 1677 to 2262; microseconds hold far beyond. SQL Server
  `datetime2` columns (`datetime2(7)` by default, read in microseconds as `timestamp(6)`, the 7th digit
  cut: connector documentation) hold `9999-12-31 23:59:59.9999999` as an open-end sentinel in the
  Warehouse and ERP databases. Where such a column met a nanosecond value (`TIMESTAMP '… .1234567'`,
  `CAST(x AS timestamp(9))`), widening it to nanoseconds overflowed ("Arithmetic overflow"), and before
  the union fix the common type with a `timestamp(3)` was milliseconds, which rounded the sentinel to
  `+10000-01-01 00:00:00.000`.
- Applied: nanoseconds are never the common type of a coarser value (unions, `VALUES`, `INTERSECT`/`EXCEPT`,
  `CASE`, `coalesce`, `greatest`/`least`, and comparisons with a nanosecond constant): the values meet in
  microseconds, a nanosecond value converted exactly or refused with `INVALID_CAST_ARGUMENT: TIMESTAMP '…'
  has more than 6 fraction digits …` (`codelake_exact_micros`), never rounded. Nanosecond values among
  themselves keep nanoseconds and their declared `timestamp(7..9)`.
- Divergence: the reference answers `timestamp(7..9)` with every digit; the engine answers `timestamp(6)`
  with the same values when they have no 7th digit (differential `timestamp_common_types` TU16) and refuses
  the statement when one has (TU11; `sqlserver_timestamp_sentinels`). A `datetime2(7)` value's 7th digit
  is not read at all (before this issue): its column compared with a constant that has one is refused
  rather than answered without the row the reference finds.
- Risk: a statement mixing a constant with seven to nine digits and a coarser timestamp fails where the
  reference answers; never a wrong value.
- Revisit when: the engine has a timestamp carrier with both the range and nine digits, or a client needs
  `timestamp(7)` values from SQL Server `datetime2(7)`.
- References: `crates/codelake-core/src/coercion.rs` (`common_temporal`, `unify_timestamps`,
  `ExactMicros`); ISS-042; differential `timestamp_common_types.toml`; integration
  `sqlserver_timestamp_sentinels`.
