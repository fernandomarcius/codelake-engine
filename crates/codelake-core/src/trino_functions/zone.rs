//! Time zones with Trino's (Java `ZoneId` + Joda) rules.
//!
//! - Ids are case-sensitive IANA regions or offsets (`+05:30`, `+0530`, `+05`,
//!   `UTC+05:30`, `GMT-03:00`); every UTC equivalent (`Z`, `GMT`, `UT`,
//!   `Etc/UTC`, `+00:00`, …) is reported as `UTC`.
//! - Local → UTC follows Joda's `convertLocalToUTC(local, false)`: a local time
//!   inside a DST gap moves forward; inside an overlap the offset in force at
//!   the local instant read as UTC wins.

use chrono::{Duration, NaiveDateTime, Offset, TimeZone};
use chrono_tz::Tz;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Zone {
    /// Fixed offset in seconds east of UTC.
    Fixed(i32),
    Named(Tz),
}

const UTC_ALIASES: &[&str] = &[
    "UTC",
    "Z",
    "GMT",
    "UT",
    "UCT",
    "GMT0",
    "Greenwich",
    "Universal",
    "Zulu",
    "Etc/UTC",
    "Etc/UCT",
    "Etc/GMT",
    "Etc/GMT0",
    "Etc/GMT+0",
    "Etc/GMT-0",
    "Etc/Greenwich",
    "Etc/Universal",
    "Etc/Zulu",
    "Etc/UT",
];

/// Maximum offset Trino accepts (±14:00).
const MAX_OFFSET_SECONDS: i32 = 14 * 3600;

fn parse_offset(text: &str) -> Option<i32> {
    // Byte slicing below is only safe on ASCII.
    if !text.is_ascii() {
        return None;
    }
    let (sign, digits) = match text.as_bytes().first()? {
        b'+' => (1, &text[1..]),
        b'-' => (-1, &text[1..]),
        _ => return None,
    };
    let number = |s: &str| -> Option<i32> {
        (s.len() == 2 && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse().ok())
            .flatten()
    };
    let (hours, minutes, seconds) = match digits.len() {
        1 if digits.as_bytes()[0].is_ascii_digit() => (i32::from(digits.as_bytes()[0] - b'0'), 0, 0),
        2 => (number(digits)?, 0, 0),
        4 => (number(&digits[..2])?, number(&digits[2..])?, 0),
        5 if &digits[2..3] == ":" => (number(&digits[..2])?, number(&digits[3..])?, 0),
        6 => (
            number(&digits[..2])?,
            number(&digits[2..4])?,
            number(&digits[4..])?,
        ),
        8 if &digits[2..3] == ":" && &digits[5..6] == ":" => (
            number(&digits[..2])?,
            number(&digits[3..5])?,
            number(&digits[6..])?,
        ),
        _ => return None,
    };
    if minutes > 59 || seconds > 59 || seconds != 0 {
        return None;
    }
    let total = sign * (hours * 3600 + minutes * 60);
    (total.abs() <= MAX_OFFSET_SECONDS).then_some(total)
}

impl Zone {
    pub(crate) const UTC: Zone = Zone::Fixed(0);

    /// Parses a Trino zone id; `None` when Trino would reject it.
    pub(crate) fn parse(id: &str) -> Option<Zone> {
        if UTC_ALIASES.contains(&id) {
            return Some(Zone::UTC);
        }
        if id.starts_with('+') || id.starts_with('-') {
            return parse_offset(id).map(Zone::Fixed);
        }
        for prefix in ["UTC", "GMT", "UT"] {
            if let Some(rest) = id.strip_prefix(prefix)
                && (rest.starts_with('+') || rest.starts_with('-'))
            {
                return parse_offset(rest).map(Zone::Fixed);
            }
        }
        id.parse::<Tz>().ok().map(Zone::Named)
    }

    /// Canonical name, as Trino prints it after a value.
    pub(crate) fn name(self) -> String {
        match self {
            Zone::Fixed(0) => "UTC".to_owned(),
            Zone::Fixed(seconds) => {
                let sign = if seconds < 0 { '-' } else { '+' };
                let abs = seconds.abs();
                format!("{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60)
            }
            Zone::Named(tz) => tz.name().to_owned(),
        }
    }

    /// Offset (seconds east of UTC) in force at a UTC instant.
    pub(crate) fn offset_at(self, utc: NaiveDateTime) -> i32 {
        match self {
            Zone::Fixed(seconds) => seconds,
            Zone::Named(tz) => tz.offset_from_utc_datetime(&utc).fix().local_minus_utc(),
        }
    }

    pub(crate) fn to_local(self, utc: NaiveDateTime) -> NaiveDateTime {
        utc + Duration::seconds(i64::from(self.offset_at(utc)))
    }

    fn is_gap(self, local: NaiveDateTime) -> bool {
        match self {
            Zone::Fixed(_) => false,
            Zone::Named(tz) => matches!(tz.from_local_datetime(&local), chrono::LocalResult::None),
        }
    }

    /// Joda `convertLocalToUTC(local, strict = false)`.
    pub(crate) fn to_utc(self, local: NaiveDateTime) -> NaiveDateTime {
        let offset_local = self.offset_at(local);
        let adjusted = local - Duration::seconds(i64::from(offset_local));
        let mut offset = self.offset_at(adjusted);
        if offset != offset_local && offset_local < 0 && self.is_gap(local) {
            offset = offset_local;
        }
        local - Duration::seconds(i64::from(offset))
    }

    /// Joda `convertLocalToUTC(local, false, original)`: keeps the original
    /// instant's offset when it still applies (overlaps keep their side).
    pub(crate) fn to_utc_near(self, local: NaiveDateTime, original_utc: NaiveDateTime) -> NaiveDateTime {
        let offset_original = self.offset_at(original_utc);
        let candidate = local - Duration::seconds(i64::from(offset_original));
        if self.offset_at(candidate) == offset_original {
            return candidate;
        }
        self.to_utc(local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn at(y: i32, m: u32, d: u32, h: u32, min: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, 0)
            .unwrap()
    }

    #[test]
    fn parses_like_java_zone_id() {
        assert_eq!(Zone::parse("UTC"), Some(Zone::UTC));
        assert_eq!(Zone::parse("Etc/UTC"), Some(Zone::UTC));
        assert_eq!(Zone::parse("+00:00"), Some(Zone::UTC));
        assert_eq!(Zone::parse("utc"), None, "ids are case-sensitive");
        assert_eq!(Zone::parse("america/sao_paulo"), None);
        assert_eq!(Zone::parse("+05:30"), Some(Zone::Fixed(19_800)));
        assert_eq!(Zone::parse("+0530"), Some(Zone::Fixed(19_800)));
        assert_eq!(Zone::parse("+05"), Some(Zone::Fixed(18_000)));
        assert_eq!(Zone::parse("GMT-03:00"), Some(Zone::Fixed(-10_800)));
        assert_eq!(Zone::parse("+15:00"), None);
        assert_eq!(Zone::parse("Foo/Bar"), None);
        assert_eq!(Zone::parse("-03:00").unwrap().name(), "-03:00");
        assert_eq!(
            Zone::parse("America/Sao_Paulo").unwrap().name(),
            "America/Sao_Paulo"
        );
    }

    #[test]
    fn gap_moves_forward_and_overlap_follows_joda() {
        let ny = Zone::parse("America/New_York").unwrap();
        // 02:30 does not exist on 2024-03-10: Joda answers 03:30 EDT = 07:30Z.
        assert_eq!(ny.to_utc(at(2024, 3, 10, 2, 30)), at(2024, 3, 10, 7, 30));
        // 01:30 on 2024-11-03 happens twice: negative offsets pick EDT (earlier).
        assert_eq!(ny.to_utc(at(2024, 11, 3, 1, 30)), at(2024, 11, 3, 5, 30));
        let berlin = Zone::parse("Europe/Berlin").unwrap();
        // Positive offsets: gap moves forward, overlap picks CET (later).
        assert_eq!(berlin.to_utc(at(2024, 3, 31, 2, 30)), at(2024, 3, 31, 1, 30));
        assert_eq!(berlin.to_utc(at(2024, 10, 27, 2, 30)), at(2024, 10, 27, 1, 30));
        // Keeping the original side of an overlap.
        assert_eq!(
            berlin.to_utc_near(at(2024, 10, 27, 2, 30), at(2024, 10, 27, 0, 30)),
            at(2024, 10, 27, 0, 30)
        );
    }
}
