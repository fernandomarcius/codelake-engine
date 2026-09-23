//! Human-readable byte sizes (`"512MiB"`, `"4GiB"`) for memory and spill limits.
//!
//! Memory limits are the main operational lever of `CodeLake`, so parsing is strict:
//! an ambiguous or overflowing value is an error, never a silent default.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, de};

/// A size in bytes, parsed from an integer or a string with a binary/decimal unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(u64);

/// Error returned when a byte size string cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ByteSizeError {
    #[error("empty byte size")]
    Empty,
    #[error("invalid number in byte size `{0}`")]
    InvalidNumber(String),
    #[error("unknown unit `{unit}` in byte size `{input}` (use B, KB, MB, GB, TB, KiB, MiB, GiB, TiB)")]
    UnknownUnit { input: String, unit: String },
    #[error("byte size `{0}` overflows u64")]
    Overflow(String),
}

impl ByteSize {
    pub const fn b(n: u64) -> Self {
        Self(n)
    }

    pub const fn mib(n: u64) -> Self {
        Self(n * 1024 * 1024)
    }

    pub const fn gib(n: u64) -> Self {
        Self(n * 1024 * 1024 * 1024)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Size as `usize`, saturating on 32-bit targets instead of truncating.
    pub fn as_usize(self) -> usize {
        usize::try_from(self.0).unwrap_or(usize::MAX)
    }
}

fn unit_multiplier(unit: &str) -> Option<u64> {
    let m = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1 << 40,
        _ => return None,
    };
    Some(m)
}

impl FromStr for ByteSize {
    type Err = ByteSizeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(ByteSizeError::Empty);
        }
        let split = trimmed
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(trimmed.len());
        let (number, unit) = trimmed.split_at(split);
        if number.is_empty() {
            return Err(ByteSizeError::InvalidNumber(input.to_owned()));
        }
        let value: u64 = number
            .parse()
            .map_err(|_| ByteSizeError::Overflow(input.to_owned()))?;
        let unit = unit.trim();
        let multiplier = unit_multiplier(unit).ok_or_else(|| ByteSizeError::UnknownUnit {
            input: input.to_owned(),
            unit: unit.to_owned(),
        })?;
        value
            .checked_mul(multiplier)
            .map(Self)
            .ok_or_else(|| ByteSizeError::Overflow(input.to_owned()))
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const UNITS: [(&str, u64); 4] = [
            ("TiB", 1 << 40),
            ("GiB", 1 << 30),
            ("MiB", 1 << 20),
            ("KiB", 1 << 10),
        ];
        for (name, size) in UNITS {
            if self.0 >= size && self.0.is_multiple_of(size) {
                return write!(f, "{}{name}", self.0 / size);
            }
        }
        write!(f, "{}B", self.0)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Int(u64),
            Str(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Int(n) => Ok(Self(n)),
            Raw::Str(s) => s.parse().map_err(de::Error::custom),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_binary_and_decimal_units() {
        assert_eq!("512MiB".parse::<ByteSize>().unwrap(), ByteSize::mib(512));
        assert_eq!("4GiB".parse::<ByteSize>().unwrap(), ByteSize::gib(4));
        assert_eq!("2 GB".parse::<ByteSize>().unwrap(), ByteSize::b(2_000_000_000));
        assert_eq!("1kb".parse::<ByteSize>().unwrap(), ByteSize::b(1_000));
        assert_eq!("1KiB".parse::<ByteSize>().unwrap(), ByteSize::b(1_024));
        assert_eq!("1TiB".parse::<ByteSize>().unwrap(), ByteSize::b(1 << 40));
        assert_eq!("1TB".parse::<ByteSize>().unwrap(), ByteSize::b(1_000_000_000_000));
        assert_eq!("3MB".parse::<ByteSize>().unwrap(), ByteSize::b(3_000_000));
        assert_eq!("42".parse::<ByteSize>().unwrap(), ByteSize::b(42));
        assert_eq!("42B".parse::<ByteSize>().unwrap(), ByteSize::b(42));
    }

    #[test]
    fn rejects_garbage_instead_of_defaulting() {
        assert_eq!("".parse::<ByteSize>(), Err(ByteSizeError::Empty));
        assert!(matches!(
            "GiB".parse::<ByteSize>(),
            Err(ByteSizeError::InvalidNumber(_))
        ));
        assert!(matches!(
            "1.5GiB".parse::<ByteSize>(),
            Err(ByteSizeError::UnknownUnit { .. })
        ));
        assert!(matches!(
            "4 gigs".parse::<ByteSize>(),
            Err(ByteSizeError::UnknownUnit { .. })
        ));
        assert!(matches!(
            "-1GiB".parse::<ByteSize>(),
            Err(ByteSizeError::InvalidNumber(_))
        ));
    }

    #[test]
    fn rejects_overflow() {
        assert!(matches!(
            "99999999999TiB".parse::<ByteSize>(),
            Err(ByteSizeError::Overflow(_))
        ));
        assert!(matches!(
            "999999999999999999999".parse::<ByteSize>(),
            Err(ByteSizeError::Overflow(_))
        ));
    }

    #[test]
    fn displays_largest_exact_unit() {
        assert_eq!(ByteSize::gib(4).to_string(), "4GiB");
        assert_eq!(ByteSize::mib(1536).to_string(), "1536MiB");
        assert_eq!(ByteSize::b(1_024).to_string(), "1KiB");
        assert_eq!(ByteSize::b(1_000).to_string(), "1000B");
        assert_eq!(ByteSize::b(1 << 40).to_string(), "1TiB");
    }

    #[test]
    fn deserializes_from_int_or_string() {
        #[derive(Deserialize)]
        struct W {
            v: ByteSize,
        }
        let a: W = toml::from_str("v = 1024").unwrap();
        let b: W = toml::from_str("v = \"1KiB\"").unwrap();
        assert_eq!(a.v, b.v);
        assert!(toml::from_str::<W>("v = \"lots\"").is_err());
    }

    proptest! {
        #[test]
        fn display_round_trips(n in 0u64..(1u64 << 50)) {
            let size = ByteSize::b(n);
            prop_assert_eq!(size.to_string().parse::<ByteSize>().unwrap(), size);
        }
    }
}
