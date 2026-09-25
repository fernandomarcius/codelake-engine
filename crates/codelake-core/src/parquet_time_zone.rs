//! The time zone a hive catalog reads and writes Parquet `INT96` timestamps
//! in (the reference engine's `hive.parquet.time-zone`).
//!
//! A hive `timestamp` has no zone: it is a wall-clock time. `INT96` stores an
//! instant, taken as UTC by every reader. A writer converts the wall-clock
//! time from the catalog's zone to UTC; a reader converts the instant back to
//! the wall-clock time of its catalog's zone. Two engines agree on a value
//! only when their catalogs use the same zone: a file written in UTC and read
//! in `America/Sao_Paulo` shows every value three hours earlier.
//!
//! The arithmetic is Joda-Time's, which the reference engine uses, over the
//! IANA database of `chrono-tz`:
//!
//! - UTC to local time adds the zone's offset at that instant;
//! - local time to UTC (`convertLocalToUTC(local, false)`) moves a time
//!   the clocks skip (a gap) forward by the length of the gap; a time they
//!   repeat is the earlier instant in a zone west of Greenwich and the later
//!   one in a zone east of it.
//!
//! Offsets are whole seconds (IANA's local mean times included), so the
//! conversion never touches the digits below a second.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Offset, TimeZone};
use chrono_tz::Tz;

/// Zone of a hive catalog that does not set `parquet_time_zone`.
pub const DEFAULT_PARQUET_TIME_ZONE: &str = "America/Sao_Paulo";

const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// Names of IANA zones whose offset is always zero.
const UTC_NAMES: [&str; 18] = [
    "UTC",
    "Etc/UTC",
    "UCT",
    "Etc/UCT",
    "Universal",
    "Etc/Universal",
    "Zulu",
    "Etc/Zulu",
    "GMT",
    "Etc/GMT",
    "GMT0",
    "Etc/GMT0",
    "GMT+0",
    "Etc/GMT+0",
    "GMT-0",
    "Etc/GMT-0",
    "Greenwich",
    "Etc/Greenwich",
];

/// An IANA time zone for Parquet `INT96` timestamps (see the module
/// documentation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParquetTimeZone(Tz);

impl Default for ParquetTimeZone {
    fn default() -> Self {
        Self(chrono_tz::America::Sao_Paulo)
    }
}

impl FromStr for ParquetTimeZone {
    type Err = String;

    /// An IANA zone name, spelled as the database spells it
    /// (`America/Sao_Paulo`, `UTC`); offsets such as `-03:00` are not zones.
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Tz::from_str(name).map(Self).map_err(|_| {
            format!("`{name}` is not an IANA time zone name (for example America/Sao_Paulo or UTC)")
        })
    }
}

impl fmt::Display for ParquetTimeZone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl ParquetTimeZone {
    /// UTC.
    pub const UTC: Self = Self(Tz::UTC);

    /// The zone's IANA name.
    pub fn name(&self) -> &'static str {
        self.0.name()
    }

    /// Whether the zone's offset is always zero: no value changes.
    pub fn is_utc(&self) -> bool {
        UTC_NAMES.contains(&self.name())
    }

    /// Seconds east of UTC at the instant `utc_seconds` (seconds since the
    /// epoch); `None` outside the dates the calendar holds.
    pub fn offset_at(&self, utc_seconds: i64) -> Option<i64> {
        let instant = DateTime::from_timestamp(utc_seconds, 0)?;
        Some(i64::from(
            self.0
                .offset_from_utc_datetime(&instant.naive_utc())
                .fix()
                .local_minus_utc(),
        ))
    }

    /// The wall-clock time, in nanoseconds since the epoch, of the instant
    /// `utc_nanos`.
    pub fn utc_to_local(&self, utc_nanos: i128) -> Option<i128> {
        let seconds = i64::try_from(utc_nanos.div_euclid(NANOS_PER_SECOND)).ok()?;
        Some(utc_nanos + i128::from(self.offset_at(seconds)?) * NANOS_PER_SECOND)
    }

    /// The instant, in nanoseconds since the epoch, of the wall-clock time
    /// `local_nanos` (Joda-Time's `convertLocalToUTC(local, false)`, see the
    /// module documentation).
    pub fn local_to_utc(&self, local_nanos: i128) -> Option<i128> {
        let local = i64::try_from(local_nanos.div_euclid(NANOS_PER_SECOND)).ok()?;
        // First estimate: the offset at `local` read as an instant; then the
        // offset at the instant that estimate gives.
        let offset_local = self.offset_at(local)?;
        let mut offset = self.offset_at(local.checked_sub(offset_local)?)?;
        // The two estimates straddle a transition. West of Greenwich, a gap
        // (the second estimate does not hold at its own instant) keeps the
        // first estimate, so the time moves forward; east of it the second
        // estimate already does.
        if offset_local != offset && offset_local < 0 && self.offset_at(local.checked_sub(offset)?)? != offset
        {
            offset = offset_local;
        }
        Some(local_nanos - i128::from(offset) * NANOS_PER_SECOND)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS: i128 = NANOS_PER_SECOND;

    /// Seconds since the epoch of a UTC date and time.
    fn at(text: &str) -> i128 {
        i128::from(
            chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
                .unwrap()
                .and_utc()
                .timestamp(),
        )
    }

    fn zone(name: &str) -> ParquetTimeZone {
        name.parse().unwrap()
    }

    #[test]
    fn names_are_iana_zones() {
        assert_eq!(ParquetTimeZone::default().name(), DEFAULT_PARQUET_TIME_ZONE);
        assert_eq!(zone("UTC"), ParquetTimeZone::UTC);
        for wrong in ["-03:00", "america/sao_paulo", "Brasilia", ""] {
            let err = wrong.parse::<ParquetTimeZone>().unwrap_err();
            assert!(err.contains("is not an IANA time zone name"), "{err}");
        }
        assert!(zone("Etc/UTC").is_utc());
        assert!(!zone("Etc/GMT+3").is_utc());
        assert!(!ParquetTimeZone::default().is_utc());
    }

    #[test]
    fn standard_time_moves_by_the_offset_both_ways() {
        let sp = ParquetTimeZone::default();
        // 2026-01-01 10:00:00.123456789 in Sao Paulo is 13:00 UTC.
        let local = at("2026-01-01 10:00:00") * NS + 123_456_789;
        let utc = at("2026-01-01 13:00:00") * NS + 123_456_789;
        assert_eq!(sp.local_to_utc(local), Some(utc));
        assert_eq!(sp.utc_to_local(utc), Some(local));
        // Before 1914, Sao Paulo's local mean time: -03:06:28.
        let far = at("1500-06-01 00:00:00") * NS;
        let far_utc = at("1500-06-01 03:06:28") * NS;
        assert_eq!(sp.local_to_utc(far), Some(far_utc));
        assert_eq!(sp.utc_to_local(far_utc), Some(far));
        // Year 9999 and the digits below a second, before the epoch too.
        let last = at("9999-12-31 23:59:59") * NS + 999_999_999;
        assert_eq!(sp.utc_to_local(sp.local_to_utc(last).unwrap()), Some(last));
        let before = -1;
        assert_eq!(sp.local_to_utc(before), Some(before + 3 * 3600 * NS));
        assert_eq!(ParquetTimeZone::UTC.local_to_utc(before), Some(before));
    }

    #[test]
    fn a_gap_moves_forward_west_of_greenwich() {
        let sp = ParquetTimeZone::default();
        // 2018-11-04: 00:00 (-03) became 01:00 (-02); 00:30 does not exist.
        let gap = at("2018-11-04 00:30:00") * NS;
        let utc = sp.local_to_utc(gap).unwrap();
        assert_eq!(utc, at("2018-11-04 03:30:00") * NS);
        assert_eq!(sp.utc_to_local(utc), Some(at("2018-11-04 01:30:00") * NS));
    }

    #[test]
    fn a_repeated_time_is_the_earlier_instant() {
        let sp = ParquetTimeZone::default();
        // 2019-02-17: 00:00 (-02) became 23:00 (-03) of the 16th.
        let twice = at("2019-02-16 23:30:00") * NS;
        assert_eq!(sp.local_to_utc(twice), Some(at("2019-02-17 01:30:00") * NS));
        assert_eq!(
            sp.utc_to_local(at("2019-02-17 02:30:00") * NS),
            Some(twice),
            "the later instant reads as the same wall-clock time"
        );
    }

    #[test]
    fn east_of_greenwich_a_gap_moves_forward_and_a_repeated_time_is_the_later_instant() {
        let berlin = zone("Europe/Berlin");
        // 2024-03-31: 02:00 (+01) became 03:00 (+02); 02:30 does not exist.
        let gap = at("2024-03-31 02:30:00") * NS;
        let utc = berlin.local_to_utc(gap).unwrap();
        assert_eq!(utc, at("2024-03-31 01:30:00") * NS);
        assert_eq!(berlin.utc_to_local(utc), Some(at("2024-03-31 03:30:00") * NS));
        // 2024-10-27: 03:00 (+02) became 02:00 (+01); 02:30 happens twice.
        let twice = at("2024-10-27 02:30:00") * NS;
        assert_eq!(berlin.local_to_utc(twice), Some(at("2024-10-27 01:30:00") * NS));
    }
}
