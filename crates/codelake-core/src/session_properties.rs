//! Session properties, following Trino's model: system properties
//! (`query_max_run_time`) and catalog properties (`hive.some_flag`) declared by
//! each connector. Every property states its effect:
//!
//! - [`Effect::Enforced`]: `CodeLake` honors it.
//! - [`Effect::PerformanceOnly`]: a Trino tuning knob that changes how work is
//!   scheduled, never the result. Accepted so existing clients keep working,
//!   and reported back as a warning, because it has no effect here.
//!
//! A property that is not declared is rejected, as Trino does. Accepting and
//! ignoring an unknown name would hide a limit the client believes is active.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyType {
    Boolean,
    Integer,
    /// airlift duration: `1h`, `30m`, `2.5s`.
    Duration,
    /// data size: `1GB`, `512MB`.
    DataSize,
    Varchar,
    /// One of the listed values, case-insensitive; stored upper-case.
    Enum(&'static [&'static str]),
}

impl PropertyType {
    pub fn name(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Duration | Self::DataSize | Self::Varchar | Self::Enum(_) => "varchar",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    Enforced,
    PerformanceOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionProperty {
    pub name: String,
    pub property_type: PropertyType,
    pub default: String,
    pub description: String,
    pub effect: Effect,
}

impl SessionProperty {
    pub fn new(
        name: &str,
        property_type: PropertyType,
        default: &str,
        effect: Effect,
        description: &str,
    ) -> Self {
        Self {
            name: name.to_owned(),
            property_type,
            default: default.to_owned(),
            description: description.to_owned(),
            effect,
        }
    }

    /// Normalizes `value` or explains why it is invalid.
    fn parse(&self, value: &str) -> Result<String, String> {
        let value = value.trim();
        let ok = match self.property_type {
            PropertyType::Boolean => {
                let lower = value.to_ascii_lowercase();
                return matches!(lower.as_str(), "true" | "false")
                    .then_some(lower)
                    .ok_or_else(|| format!("{} is invalid: {value} (expected true or false)", self.name));
            }
            PropertyType::Integer => value.parse::<i64>().is_ok(),
            PropertyType::Duration => parse_duration(value).is_some(),
            PropertyType::DataSize => parse_data_size(value).is_ok(),
            PropertyType::Varchar => true,
            PropertyType::Enum(allowed) => {
                let upper = value.to_ascii_uppercase();
                return allowed.contains(&upper.as_str()).then_some(upper).ok_or_else(|| {
                    format!(
                        "{} is invalid: {value} (one of {})",
                        self.name,
                        allowed.join(", ")
                    )
                });
            }
        };
        ok.then(|| value.to_owned())
            .ok_or_else(|| format!("{} is invalid: {value}", self.name))
    }
}

/// airlift `Duration`: a decimal number and a unit (`ns us ms s m h d`).
pub fn parse_duration(text: &str) -> Option<Duration> {
    let text = text.trim();
    let split = text.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
    let (number, unit) = text.split_at(split);
    let number: f64 = number.parse().ok()?;
    let seconds = match unit.trim() {
        "ns" => 1e-9,
        "us" => 1e-6,
        "ms" => 1e-3,
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        "d" => 86_400.0,
        _ => return None,
    };
    Duration::try_from_secs_f64(number * seconds).ok()
}

/// airlift `DataSize`, as Trino parses session and procedure sizes: a decimal
/// number and a case-sensitive unit (`B kB MB GB TB PB`), powers of 1024.
/// `"100MB"` is 104 857 600 bytes. Errors carry airlift's messages.
pub fn parse_data_size(text: &str) -> Result<u64, String> {
    if text.is_empty() {
        return Err("size is empty".into());
    }
    let invalid = || format!("size is not a valid data size string: {text}");
    let trimmed = text.trim();
    let split = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .ok_or_else(invalid)?;
    let (number, unit) = trimmed.split_at(split);
    let unit = unit.trim_start();
    let well_formed = !number.is_empty()
        && !number.starts_with('.')
        && !number.ends_with('.')
        && number.matches('.').count() <= 1
        && !unit.is_empty()
        && unit.chars().all(|c| c.is_ascii_alphabetic());
    if !well_formed {
        return Err(invalid());
    }
    let exponent = match unit {
        "B" => 0,
        "kB" => 1,
        "MB" => 2,
        "GB" => 3,
        "TB" => 4,
        "PB" => 5,
        other => return Err(format!("Unknown unit: {other}")),
    };
    // airlift keeps whole numbers exact and rounds a fraction to the nearest
    // byte, half up (`374.5B` is 375 bytes). Digits past the 19th cannot move
    // the result by a byte and are ignored.
    let too_large = || format!("size is too large to be represented in bytes: {text}");
    let multiplier = 1_u128 << (10 * exponent);
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    let whole: u128 = whole.parse().map_err(|_| too_large())?;
    let mut bytes = whole.checked_mul(multiplier).ok_or_else(too_large)?;
    if !fraction.is_empty() {
        let kept = &fraction[..fraction.len().min(19)];
        let digits = u32::try_from(kept.len()).map_err(|_| invalid())?;
        let scale = 10_u128.pow(digits);
        let numerator: u128 = kept.parse().map_err(|_| invalid())?;
        let scaled = numerator * multiplier;
        let rounded = scaled / scale + u128::from(2 * (scaled % scale) >= scale);
        bytes = bytes.checked_add(rounded).ok_or_else(too_large)?;
    }
    if bytes > u128::from(i64::MAX.unsigned_abs()) {
        return Err(too_large());
    }
    u64::try_from(bytes).map_err(|_| too_large())
}

pub const QUERY_MAX_RUN_TIME: &str = "query_max_run_time";
pub const QUERY_MAX_EXECUTION_TIME: &str = "query_max_execution_time";

type Declaration = (&'static str, PropertyType, &'static str, Effect, &'static str);

/// System properties. Trino tuning knobs listed as `PerformanceOnly` are the
/// ones whose only observable effect is speed or file layout.
const SYSTEM: &[Declaration] = &[
    (
        QUERY_MAX_RUN_TIME,
        PropertyType::Duration,
        "100d",
        Effect::Enforced,
        "Maximum wall time of a query, queueing and client paging included",
    ),
    (
        QUERY_MAX_EXECUTION_TIME,
        PropertyType::Duration,
        "100d",
        Effect::Enforced,
        "Maximum execution time of a query",
    ),
    (
        "scale_writers",
        PropertyType::Boolean,
        "true",
        Effect::PerformanceOnly,
        "Writer scaling (CodeLake writes one file per INSERT)",
    ),
    (
        "task_scale_writers_enabled",
        PropertyType::Boolean,
        "true",
        Effect::PerformanceOnly,
        "Task-level writer scaling",
    ),
    (
        "redistribute_writes",
        PropertyType::Boolean,
        "true",
        Effect::PerformanceOnly,
        "Repartition data before writing",
    ),
    (
        "use_preferred_write_partitioning",
        PropertyType::Boolean,
        "true",
        Effect::PerformanceOnly,
        "Preferred write partitioning",
    ),
    (
        "task_concurrency",
        PropertyType::Integer,
        "16",
        Effect::PerformanceOnly,
        "Local parallelism per task",
    ),
    (
        "hash_partition_count",
        PropertyType::Integer,
        "100",
        Effect::PerformanceOnly,
        "Partitions for distributed joins and aggregations",
    ),
    (
        "join_distribution_type",
        PropertyType::Enum(&["AUTOMATIC", "BROADCAST", "PARTITIONED"]),
        "AUTOMATIC",
        Effect::PerformanceOnly,
        "Join distribution strategy",
    ),
    (
        "join_reordering_strategy",
        PropertyType::Enum(&["AUTOMATIC", "ELIMINATE_CROSS_JOINS", "NONE"]),
        "AUTOMATIC",
        Effect::PerformanceOnly,
        "Join reordering strategy",
    ),
    (
        "max_reordered_joins",
        PropertyType::Integer,
        "8",
        Effect::PerformanceOnly,
        "Joins considered for reordering",
    ),
    (
        "enable_dynamic_filtering",
        PropertyType::Boolean,
        "true",
        Effect::PerformanceOnly,
        "Dynamic filtering",
    ),
    (
        "spill_enabled",
        PropertyType::Boolean,
        "false",
        Effect::PerformanceOnly,
        "Spill to disk (CodeLake always spills within its memory ceiling)",
    ),
    (
        "writer_scaling_min_data_processed",
        PropertyType::DataSize,
        "120MB",
        Effect::PerformanceOnly,
        "Data per writer before scaling",
    ),
    (
        "push_aggregation_through_outer_join",
        PropertyType::Boolean,
        "true",
        Effect::PerformanceOnly,
        "Optimizer rule toggle",
    ),
    (
        "push_partial_aggregation_through_join",
        PropertyType::Boolean,
        "false",
        Effect::PerformanceOnly,
        "Optimizer rule toggle",
    ),
];

pub fn system_properties() -> Vec<SessionProperty> {
    SYSTEM
        .iter()
        .map(|&(name, property_type, default, effect, description)| {
            SessionProperty::new(name, property_type, default, effect, description)
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PropertyError(pub String);

/// Every property the engine accepts: system ones plus each catalog's.
#[derive(Debug, Clone)]
pub struct PropertyRegistry {
    system: Vec<SessionProperty>,
    catalogs: HashMap<String, Vec<SessionProperty>>,
}

impl Default for PropertyRegistry {
    fn default() -> Self {
        Self {
            system: system_properties(),
            catalogs: HashMap::new(),
        }
    }
}

impl PropertyRegistry {
    pub fn add_catalog(&mut self, catalog: &str, properties: Vec<SessionProperty>) {
        if !properties.is_empty() {
            self.catalogs.insert(catalog.to_owned(), properties);
        }
    }

    /// `name` is `property` or `catalog.property`.
    pub fn lookup(&self, name: &str) -> Result<&SessionProperty, PropertyError> {
        let found = match name.split_once('.') {
            None => self.system.iter().find(|p| p.name == name),
            Some((catalog, property)) => self
                .catalogs
                .get(catalog)
                .and_then(|list| list.iter().find(|p| p.name == property)),
        };
        found.ok_or_else(|| PropertyError(format!("Session property '{name}' does not exist")))
    }

    /// Validates and normalizes one assignment.
    pub fn validate(&self, name: &str, value: &str) -> Result<(String, &SessionProperty), PropertyError> {
        let property = self.lookup(name)?;
        property
            .parse(value)
            .map(|v| (v, property))
            .map_err(PropertyError)
    }

    /// Validates a full set of client-supplied properties.
    pub fn session(&self, values: BTreeMap<String, String>) -> Result<SessionProperties, PropertyError> {
        let mut normalized = BTreeMap::new();
        let mut ignored = Vec::new();
        for (name, value) in values {
            let (value, property) = self.validate(&name, &value)?;
            if property.effect == Effect::PerformanceOnly {
                ignored.push(name.clone());
            }
            normalized.insert(name, value);
        }
        Ok(SessionProperties {
            values: normalized,
            ignored,
        })
    }

    /// Effective values of `catalog`'s properties in `session`: the value the
    /// client set, or the declared default.
    pub fn catalog_session(&self, catalog: &str, session: &SessionProperties) -> CatalogSession {
        let values = self
            .catalogs
            .get(catalog)
            .into_iter()
            .flatten()
            .map(|property| {
                let value = session
                    .get(&format!("{catalog}.{}", property.name))
                    .unwrap_or(&property.default);
                (property.name.clone(), value.to_owned())
            })
            .collect();
        CatalogSession { values }
    }

    /// `(name, default, type, description)` rows, for `SHOW SESSION`.
    pub fn describe(&self) -> Vec<(String, &SessionProperty)> {
        let mut rows: Vec<_> = self.system.iter().map(|p| (p.name.clone(), p)).collect();
        for (catalog, list) in &self.catalogs {
            rows.extend(list.iter().map(|p| (format!("{catalog}.{}", p.name), p)));
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }
}

/// Validated session properties of one query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionProperties {
    values: BTreeMap<String, String>,
    ignored: Vec<String>,
}

impl SessionProperties {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// Accepted properties that have no effect in `CodeLake`.
    pub fn ignored(&self) -> &[String] {
        &self.ignored
    }

    /// Smallest of `query_max_run_time` and `query_max_execution_time`.
    pub fn max_run_time(&self) -> Option<Duration> {
        [QUERY_MAX_RUN_TIME, QUERY_MAX_EXECUTION_TIME]
            .iter()
            .filter_map(|name| self.get(name).and_then(parse_duration))
            .min()
    }
}

/// One catalog's session properties as seen by its connector, without the
/// `<catalog>.` prefix and with defaults applied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogSession {
    values: BTreeMap<String, String>,
}

impl CatalogSession {
    pub fn new(values: BTreeMap<String, String>) -> Self {
        Self { values }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// True when a boolean property is set to `true`.
    pub fn is_enabled(&self, name: &str) -> bool {
        self.get(name) == Some("true")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> PropertyRegistry {
        let mut registry = PropertyRegistry::default();
        registry.add_catalog(
            "hive",
            vec![SessionProperty::new(
                "compression_codec",
                PropertyType::Enum(&["SNAPPY", "ZSTD"]),
                "SNAPPY",
                Effect::Enforced,
                "Parquet compression",
            )],
        );
        registry
    }

    #[test]
    fn system_and_catalog_properties_validate() {
        let registry = registry();
        assert_eq!(registry.validate("query_max_run_time", "1h").unwrap().0, "1h");
        assert_eq!(registry.validate("scale_writers", "FALSE").unwrap().0, "false");
        assert_eq!(
            registry
                .validate("join_distribution_type", "broadcast")
                .unwrap()
                .0,
            "BROADCAST"
        );
        assert_eq!(
            registry.validate("hive.compression_codec", "zstd").unwrap().0,
            "ZSTD"
        );
        assert!(registry.validate("hive.nope", "1").is_err());
        assert!(registry.validate("nope", "1").is_err());
        assert!(registry.validate("query_max_run_time", "soon").is_err());
        assert!(registry.validate("task_concurrency", "many").is_err());
        assert!(
            registry
                .validate("writer_scaling_min_data_processed", "lots")
                .is_err()
        );
    }

    #[test]
    fn session_reports_ignored_and_limits() {
        let registry = registry();
        let session = registry
            .session(BTreeMap::from([
                ("query_max_run_time".to_owned(), "1h".to_owned()),
                ("query_max_execution_time".to_owned(), "30m".to_owned()),
                ("scale_writers".to_owned(), "false".to_owned()),
            ]))
            .unwrap();
        assert_eq!(session.max_run_time(), Some(Duration::from_mins(30)));
        assert_eq!(session.ignored(), ["scale_writers"]);
        assert!(
            registry
                .session(BTreeMap::from([("x".to_owned(), "1".to_owned())]))
                .is_err()
        );
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("1.5m"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("10 s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("h"), None);
        assert_eq!(parse_duration("5 years"), None);
    }

    #[test]
    fn data_sizes_follow_airlift() {
        assert_eq!(parse_data_size("100MB"), Ok(100 << 20));
        assert_eq!(parse_data_size("1GB"), Ok(1 << 30));
        assert_eq!(parse_data_size("10kB"), Ok(10 << 10));
        assert_eq!(parse_data_size("10 MB"), Ok(10 << 20));
        assert_eq!(parse_data_size(" 10MB "), Ok(10 << 20));
        assert_eq!(parse_data_size("1.5MB"), Ok(1_572_864));
        // Fractions round to the nearest byte, half up (Trino 431).
        assert_eq!(parse_data_size("0.5B"), Ok(1));
        assert_eq!(parse_data_size("374.5B"), Ok(375));
        assert_eq!(parse_data_size("374.4B"), Ok(374));
        assert_eq!(parse_data_size("0.36572265625kB"), Ok(375));
        assert_eq!(parse_data_size("0B"), Ok(0));
        assert_eq!(parse_data_size("1PB"), Ok(1 << 50));
        assert_eq!(parse_data_size("10mb"), Err("Unknown unit: mb".into()));
        assert_eq!(parse_data_size("10KB"), Err("Unknown unit: KB".into()));
        assert_eq!(parse_data_size("1EB"), Err("Unknown unit: EB".into()));
        for bad in ["abc", "100", "-1MB", ".5MB", "1.MB", "1.2.3MB", "10 M B"] {
            assert_eq!(
                parse_data_size(bad),
                Err(format!("size is not a valid data size string: {bad}")),
                "{bad}"
            );
        }
        assert_eq!(parse_data_size(""), Err("size is empty".into()));
        assert_eq!(
            parse_data_size("99999999999PB"),
            Err("size is too large to be represented in bytes: 99999999999PB".into())
        );
        assert_eq!(parse_data_size("8191PB"), Ok(8191 << 50));
        assert!(parse_data_size("8192PB").is_err());
        assert_eq!(
            parse_data_size("1.99999999999999999999999999999999999PB"),
            Ok(2 << 50)
        );
        assert_eq!(parse_data_size("0.001kB"), Ok(1));
    }

    #[test]
    fn catalog_session_applies_defaults() {
        let registry = registry();
        let empty = registry.catalog_session("hive", &SessionProperties::default());
        assert_eq!(empty.get("compression_codec"), Some("SNAPPY"));
        let set = registry
            .session(BTreeMap::from([(
                "hive.compression_codec".to_owned(),
                "zstd".to_owned(),
            )]))
            .unwrap();
        let session = registry.catalog_session("hive", &set);
        assert_eq!(session.get("compression_codec"), Some("ZSTD"));
        assert_eq!(session.get("nope"), None);
        assert!(!session.is_enabled("compression_codec"));
        assert_eq!(registry.catalog_session("other", &set), CatalogSession::default());
        let flags = CatalogSession::new(BTreeMap::from([("on".to_owned(), "true".to_owned())]));
        assert!(flags.is_enabled("on") && !flags.is_enabled("off"));
    }

    #[test]
    fn describe_is_sorted_and_qualified() {
        let registry = registry();
        let names: Vec<_> = registry.describe().into_iter().map(|(n, _)| n).collect();
        assert!(names.contains(&"hive.compression_codec".to_owned()));
        assert!(names.windows(2).all(|w| w[0] <= w[1]));
    }
}
