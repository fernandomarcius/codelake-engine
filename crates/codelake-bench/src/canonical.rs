//! Engine-independent form of a query result, used by the correctness eval.
//!
//! Every driver turns its native values (PostgreSQL text, Trino JSON, Arrow)
//! into [`Cell`]s. Two results are equal when they have the same number of
//! columns and rows and every cell matches after both sides are sorted into a
//! canonical order. Only floating-point cells get a tolerance
//! ([`FLOAT_REL_TOL`]); decimals, integers, text and dates must match exactly.

use serde::{Deserialize, Serialize};

/// Relative tolerance for floating-point cells (`double`/`real`). Sums of
/// floats legitimately differ in the last bits with the aggregation order.
pub(crate) const FLOAT_REL_TOL: f64 = 1e-9;
/// Absolute tolerance for floating-point cells close to zero.
pub(crate) const FLOAT_ABS_TOL: f64 = 1e-12;
/// Rows kept per result in the run files. Larger results are compared by row
/// count and checksum only.
pub(crate) const MAX_STORED_ROWS: usize = 50_000;

/// How a column's values are compared, derived from the engine's type name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Class {
    /// Integers and decimals: exact value, trailing zeros ignored.
    Number,
    Float,
    Bool,
    Timestamp,
    Binary,
    Text,
}

impl Class {
    /// Trino type names (`bigint`, `decimal(15,2)`, `varchar(25)`, ...).
    pub(crate) fn from_trino(type_name: &str) -> Self {
        let t = type_name.to_ascii_lowercase();
        let base = t.split('(').next().unwrap_or_default().trim();
        match base {
            "bigint" | "integer" | "smallint" | "tinyint" | "decimal" => Self::Number,
            "double" | "real" => Self::Float,
            "boolean" => Self::Bool,
            "timestamp" => Self::Timestamp,
            "varbinary" => Self::Binary,
            _ => Self::Text,
        }
    }

    /// PostgreSQL type names as reported by the server (`int8`, `numeric`, ...).
    pub(crate) fn from_postgres(type_name: &str) -> Self {
        match type_name {
            "int2" | "int4" | "int8" | "numeric" | "oid" => Self::Number,
            "float4" | "float8" => Self::Float,
            "bool" => Self::Bool,
            "timestamp" | "timestamptz" => Self::Timestamp,
            "bytea" => Self::Binary,
            _ => Self::Text,
        }
    }

    /// Arrow data types.
    pub(crate) fn from_arrow(data_type: &datafusion::arrow::datatypes::DataType) -> Self {
        use datafusion::arrow::datatypes::DataType as T;
        match data_type {
            T::Int8
            | T::Int16
            | T::Int32
            | T::Int64
            | T::UInt8
            | T::UInt16
            | T::UInt32
            | T::UInt64
            | T::Decimal32(..)
            | T::Decimal64(..)
            | T::Decimal128(..)
            | T::Decimal256(..) => Self::Number,
            T::Float16 | T::Float32 | T::Float64 => Self::Float,
            T::Boolean => Self::Bool,
            T::Timestamp(..) => Self::Timestamp,
            T::Binary | T::LargeBinary | T::BinaryView | T::FixedSizeBinary(_) => Self::Binary,
            _ => Self::Text,
        }
    }
}

/// One canonical value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cell {
    Null,
    Bool(bool),
    /// Normalized decimal text: no leading/trailing zeros, `-0` is `0`.
    Number(String),
    Float(f64),
    /// Text, dates, timestamps (normalized) and binary (lowercase hex).
    Text(String),
}

impl Cell {
    /// Builds a cell from a textual value of a column of class `class`.
    pub(crate) fn from_text(class: Class, raw: &str) -> Self {
        match class {
            Class::Number => normalize_decimal(raw).map_or_else(
                || {
                    raw.trim()
                        .parse::<f64>()
                        .map_or_else(|_| Self::Text(raw.to_owned()), Self::Float)
                },
                Self::Number,
            ),
            Class::Float => raw
                .trim()
                .parse::<f64>()
                .map_or_else(|_| Self::Text(raw.to_owned()), Self::Float),
            Class::Bool => match raw.trim().to_ascii_lowercase().as_str() {
                "t" | "true" => Self::Bool(true),
                "f" | "false" => Self::Bool(false),
                _ => Self::Text(raw.to_owned()),
            },
            Class::Timestamp => Self::Text(normalize_timestamp(raw)),
            Class::Binary => Self::Text(raw.strip_prefix("\\x").unwrap_or(raw).to_ascii_lowercase()),
            Class::Text => Self::Text(raw.to_owned()),
        }
    }

    /// Compact, JSON-safe token stored in run files (`N:12.5`, `F:0.1`, ...).
    pub(crate) fn token(&self) -> String {
        match self {
            Self::Null => "Z".to_owned(),
            Self::Bool(b) => format!("B:{b}"),
            Self::Number(n) => format!("N:{n}"),
            Self::Float(f) => format!("F:{f:?}"),
            Self::Text(s) => format!("S:{s}"),
        }
    }

    pub(crate) fn from_token(token: &str) -> Self {
        match token.split_once(':') {
            Some(("B", v)) => Self::Bool(v == "true"),
            Some(("N", v)) => Self::Number(v.to_owned()),
            Some(("F", v)) => v.parse().map_or_else(|_| Self::Text(v.to_owned()), Self::Float),
            Some(("S", v)) => Self::Text(v.to_owned()),
            _ => Self::Null,
        }
    }

    /// Sort/checksum key: numbers of either class are keyed by their value
    /// with 9 significant digits, so a decimal on one side and a float on the
    /// other still land in the same position.
    fn key(&self) -> String {
        match self {
            Self::Null => "\u{0}".to_owned(),
            Self::Bool(b) => format!("b{b}"),
            Self::Number(n) => n.parse::<f64>().map_or_else(|_| format!("n{n}"), float_key),
            Self::Float(f) => float_key(*f),
            Self::Text(s) => format!("s{s}"),
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(n) => n.parse().ok(),
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }
}

fn float_key(f: f64) -> String {
    if f.is_nan() {
        "fNaN".to_owned()
    } else if f.abs() < f64::MIN_POSITIVE {
        "f0".to_owned()
    } else {
        format!("f{f:.8e}")
    }
}

/// `"-000123.4500"` → `"-123.45"`. `None` when the text is not a plain decimal.
pub(crate) fn normalize_decimal(raw: &str) -> Option<String> {
    let s = raw.trim();
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if (int.is_empty() && frac.is_empty())
        || !int.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let int = int.trim_start_matches('0');
    let frac = frac.trim_end_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    if int == "0" && frac.is_empty() {
        return Some("0".to_owned());
    }
    let sign = if negative { "-" } else { "" };
    if frac.is_empty() {
        Some(format!("{sign}{int}"))
    } else {
        Some(format!("{sign}{int}.{frac}"))
    }
}

/// `2026-01-01T10:00:00.120` and `2026-01-01 10:00:00.12` → `2026-01-01 10:00:00.12`.
pub(crate) fn normalize_timestamp(raw: &str) -> String {
    let mut s = raw.trim().to_owned();
    if s.get(10..11) == Some("T") {
        s.replace_range(10..11, " ");
    }
    let Some((head, frac)) = s.split_once('.') else {
        return s;
    };
    let digits: String = frac.chars().take_while(char::is_ascii_digit).collect();
    let rest = &frac[digits.len()..];
    let trimmed = digits.trim_end_matches('0');
    if trimmed.is_empty() {
        format!("{head}{rest}")
    } else {
        format!("{head}.{trimmed}{rest}")
    }
}

/// Lowercase hex of raw bytes.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// A result in canonical order, ready to be stored and compared.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CanonicalResult {
    pub(crate) columns: Vec<Column>,
    pub(crate) row_count: u64,
    /// FNV-1a 64 over the sorted row keys (floats rounded to 9 digits).
    pub(crate) checksum: String,
    /// Sorted rows as cell tokens; empty when `truncated`.
    pub(crate) rows: Vec<Vec<String>>,
    pub(crate) truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Column {
    pub(crate) name: String,
    /// Type name as the engine reported it.
    pub(crate) engine_type: String,
    pub(crate) class: Class,
}

fn row_key(row: &[Cell]) -> String {
    row.iter().map(Cell::key).collect::<Vec<_>>().join("\u{1f}")
}

fn fnv1a(data: &str, mut hash: u64) -> u64 {
    for byte in data.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

impl CanonicalResult {
    pub(crate) fn new(columns: Vec<Column>, rows: Vec<Vec<Cell>>) -> Self {
        let mut keyed: Vec<(String, Vec<Cell>)> = rows.into_iter().map(|r| (row_key(&r), r)).collect();
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for (key, _) in &keyed {
            hash = fnv1a(key, hash);
            hash = fnv1a("\n", hash);
        }
        let row_count = u64::try_from(keyed.len()).unwrap_or(u64::MAX);
        let truncated = keyed.len() > MAX_STORED_ROWS;
        let rows = if truncated {
            Vec::new()
        } else {
            keyed
                .into_iter()
                .map(|(_, row)| row.iter().map(Cell::token).collect())
                .collect()
        };
        Self {
            columns,
            row_count,
            checksum: format!("{hash:016x}"),
            rows,
            truncated,
        }
    }
}

/// Outcome of comparing one result against the reference (Trino).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Comparison {
    pub(crate) equal: bool,
    /// Wrong results: any entry here makes the comparison fail.
    pub(crate) divergences: Vec<String>,
    /// Differences that do not change values (column names, number types).
    pub(crate) notes: Vec<String>,
}

/// Divergences listed per result before the rest is summarized.
const MAX_LISTED: usize = 5;

fn cells_match(a: &Cell, b: &Cell) -> (bool, bool) {
    match (a, b) {
        (Cell::Float(x), Cell::Float(y)) => (floats_equal(*x, *y), false),
        (Cell::Float(_), Cell::Number(_)) | (Cell::Number(_), Cell::Float(_)) => {
            let equal = matches!((a.as_f64(), b.as_f64()), (Some(x), Some(y)) if floats_equal(x, y));
            (equal, true)
        }
        _ => (a == b, false),
    }
}

pub(crate) fn floats_equal(a: f64, b: f64) -> bool {
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a.is_infinite() || b.is_infinite() {
        return a.to_bits() == b.to_bits();
    }
    let diff = (a - b).abs();
    diff <= FLOAT_ABS_TOL || diff <= FLOAT_REL_TOL * a.abs().max(b.abs())
}

/// Compares `candidate` (`CodeLake`) with `reference` (Trino).
pub(crate) fn compare(reference: &CanonicalResult, candidate: &CanonicalResult) -> Comparison {
    let mut divergences = Vec::new();
    let mut notes = Vec::new();
    if reference.columns.len() == candidate.columns.len() {
        for (r, c) in reference.columns.iter().zip(&candidate.columns) {
            if !r.name.eq_ignore_ascii_case(&c.name) {
                notes.push(format!("column name: trino=`{}` codelake=`{}`", r.name, c.name));
            }
        }
    } else {
        divergences.push(format!(
            "column count: trino={} codelake={}",
            reference.columns.len(),
            candidate.columns.len()
        ));
    }
    if reference.row_count != candidate.row_count {
        divergences.push(format!(
            "row count: trino={} codelake={}",
            reference.row_count, candidate.row_count
        ));
    }
    if divergences.is_empty() {
        if reference.truncated || candidate.truncated {
            if reference.checksum != candidate.checksum {
                divergences.push(format!(
                    "checksum (result above {MAX_STORED_ROWS} rows, compared by checksum only): trino={} codelake={}",
                    reference.checksum, candidate.checksum
                ));
            }
        } else {
            compare_rows(reference, candidate, &mut divergences, &mut notes);
        }
    }
    Comparison {
        equal: divergences.is_empty(),
        divergences,
        notes,
    }
}

fn compare_rows(
    reference: &CanonicalResult,
    candidate: &CanonicalResult,
    divergences: &mut Vec<String>,
    notes: &mut Vec<String>,
) {
    let mut mismatched = 0_usize;
    let mut type_noted = vec![false; reference.columns.len()];
    for (index, (r_row, c_row)) in reference.rows.iter().zip(&candidate.rows).enumerate() {
        for (col, (r_tok, c_tok)) in r_row.iter().zip(c_row).enumerate() {
            let (r, c) = (Cell::from_token(r_tok), Cell::from_token(c_tok));
            let (equal, mixed) = cells_match(&r, &c);
            if mixed && !type_noted.get(col).copied().unwrap_or(true) {
                if let Some(flag) = type_noted.get_mut(col) {
                    *flag = true;
                }
                let type_of = |r: &CanonicalResult| {
                    r.columns
                        .get(col)
                        .map_or_else(|| "?".to_owned(), |c| c.engine_type.clone())
                };
                notes.push(format!(
                    "column {}: number type differs (trino `{}`, codelake `{}`)",
                    col + 1,
                    type_of(reference),
                    type_of(candidate),
                ));
            }
            if !equal {
                mismatched += 1;
                if mismatched <= MAX_LISTED {
                    let name = reference.columns.get(col).map_or("?", |c| c.name.as_str());
                    divergences.push(format!(
                        "row {} column `{name}`: trino={} codelake={}",
                        index + 1,
                        r_tok,
                        c_tok
                    ));
                }
            }
        }
    }
    if mismatched > MAX_LISTED {
        divergences.push(format!("... {} more mismatched cells", mismatched - MAX_LISTED));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, class: Class) -> Column {
        Column {
            name: name.into(),
            engine_type: format!("{class:?}"),
            class,
        }
    }

    #[test]
    fn decimals_normalize_by_value() {
        assert_eq!(normalize_decimal("10.50").as_deref(), Some("10.5"));
        assert_eq!(normalize_decimal("-0010.000").as_deref(), Some("-10"));
        assert_eq!(normalize_decimal("-0.00").as_deref(), Some("0"));
        assert_eq!(normalize_decimal(".5").as_deref(), Some("0.5"));
        assert_eq!(normalize_decimal("12").as_deref(), Some("12"));
        assert_eq!(normalize_decimal("1e5"), None);
        assert_eq!(normalize_decimal("abc"), None);
        assert_eq!(normalize_decimal(""), None);
    }

    #[test]
    fn timestamps_normalize_separator_and_fraction() {
        assert_eq!(
            normalize_timestamp("2026-01-01T10:00:00.120"),
            "2026-01-01 10:00:00.12"
        );
        assert_eq!(
            normalize_timestamp("2026-01-01 10:00:00.000"),
            "2026-01-01 10:00:00"
        );
        assert_eq!(normalize_timestamp("2026-01-01 10:00:00"), "2026-01-01 10:00:00");
        assert_eq!(
            normalize_timestamp("2026-01-01 10:00:00.500 UTC"),
            "2026-01-01 10:00:00.5 UTC"
        );
    }

    #[test]
    fn cells_from_each_protocol_agree() {
        assert_eq!(
            Cell::from_text(Class::Number, "25.500"),
            Cell::from_text(Class::Number, "25.5")
        );
        assert_eq!(
            Cell::from_text(Class::Bool, "t"),
            Cell::from_text(Class::Bool, "true")
        );
        assert_eq!(
            Cell::from_text(Class::Binary, "\\x0A0b"),
            Cell::Text("0a0b".into())
        );
        assert_eq!(hex(&[0x0a, 0x0b]), "0a0b");
        for cell in [
            Cell::Null,
            Cell::Bool(true),
            Cell::Number("-1.5".into()),
            Cell::Float(0.1),
            Cell::Text("a:b".into()),
        ] {
            assert_eq!(Cell::from_token(&cell.token()), cell);
        }
    }

    #[test]
    fn equal_results_in_any_order_match() {
        let columns = vec![col("k", Class::Text), col("v", Class::Number)];
        let a = CanonicalResult::new(
            columns.clone(),
            vec![
                vec![Cell::Text("x".into()), Cell::Number("1".into())],
                vec![Cell::Text("y".into()), Cell::Number("2".into())],
            ],
        );
        let b = CanonicalResult::new(
            columns,
            vec![
                vec![Cell::Text("y".into()), Cell::Number("2".into())],
                vec![Cell::Text("x".into()), Cell::Number("1".into())],
            ],
        );
        assert_eq!(a.checksum, b.checksum);
        assert!(compare(&a, &b).equal);
    }

    #[test]
    fn decimal_values_must_match_exactly() {
        let columns = vec![col("avg", Class::Number)];
        let trino = CanonicalResult::new(columns.clone(), vec![vec![Cell::Number("25.51".into())]]);
        let codelake = CanonicalResult::new(columns, vec![vec![Cell::Number("25.507251".into())]]);
        let cmp = compare(&trino, &codelake);
        assert!(!cmp.equal);
        assert!(cmp.divergences[0].contains("N:25.51"), "{cmp:?}");
    }

    #[test]
    fn floats_get_the_declared_tolerance_only() {
        assert!(floats_equal(0.1 + 0.2, 0.3));
        assert!(floats_equal(1e6, 1e6 * (1.0 + 1e-12)));
        assert!(!floats_equal(1.0, 1.0001));
        assert!(floats_equal(f64::NAN, f64::NAN));
        assert!(!floats_equal(f64::NAN, 1.0));
        let columns = vec![col("x", Class::Float)];
        let a = CanonicalResult::new(columns.clone(), vec![vec![Cell::Float(0.3)]]);
        let b = CanonicalResult::new(columns, vec![vec![Cell::Float(0.1 + 0.2)]]);
        assert!(compare(&a, &b).equal);
    }

    #[test]
    fn decimal_against_float_is_a_note_when_values_agree() {
        let a = CanonicalResult::new(
            vec![col("x", Class::Number)],
            vec![vec![Cell::Number("0.05".into())]],
        );
        let b = CanonicalResult::new(vec![col("x", Class::Float)], vec![vec![Cell::Float(0.05)]]);
        let cmp = compare(&a, &b);
        assert!(cmp.equal, "{cmp:?}");
        assert_eq!(cmp.notes.len(), 1);
    }

    #[test]
    fn shape_differences_fail() {
        let one = CanonicalResult::new(vec![col("a", Class::Text)], vec![vec![Cell::Null]]);
        let two_rows = CanonicalResult::new(
            vec![col("a", Class::Text)],
            vec![vec![Cell::Null], vec![Cell::Null]],
        );
        let two_cols = CanonicalResult::new(
            vec![col("a", Class::Text), col("b", Class::Text)],
            vec![vec![Cell::Null, Cell::Null]],
        );
        assert!(!compare(&one, &two_rows).equal);
        assert!(!compare(&one, &two_cols).equal);
        let renamed = CanonicalResult::new(vec![col("B", Class::Text)], vec![vec![Cell::Null]]);
        let cmp = compare(&one, &renamed);
        assert!(cmp.equal && cmp.notes.len() == 1, "{cmp:?}");
    }

    #[test]
    fn large_results_compare_by_checksum() {
        let rows = |n: usize, bump: usize| {
            (0..n)
                .map(|i| vec![Cell::Number((i + bump).to_string())])
                .collect::<Vec<_>>()
        };
        let columns = vec![col("n", Class::Number)];
        let a = CanonicalResult::new(columns.clone(), rows(MAX_STORED_ROWS + 1, 0));
        let b = CanonicalResult::new(columns.clone(), rows(MAX_STORED_ROWS + 1, 0));
        let c = CanonicalResult::new(columns, rows(MAX_STORED_ROWS + 1, 1));
        assert!(a.truncated && a.rows.is_empty());
        assert!(compare(&a, &b).equal);
        assert!(!compare(&a, &c).equal);
    }

    #[test]
    fn classes_from_type_names() {
        use datafusion::arrow::datatypes::DataType;
        assert_eq!(Class::from_trino("decimal(15,2)"), Class::Number);
        assert_eq!(Class::from_trino("double"), Class::Float);
        assert_eq!(Class::from_trino("varchar(25)"), Class::Text);
        assert_eq!(Class::from_trino("timestamp(3)"), Class::Timestamp);
        assert_eq!(Class::from_postgres("numeric"), Class::Number);
        assert_eq!(Class::from_postgres("float8"), Class::Float);
        assert_eq!(Class::from_postgres("date"), Class::Text);
        assert_eq!(Class::from_arrow(&DataType::Decimal128(15, 2)), Class::Number);
        assert_eq!(Class::from_arrow(&DataType::Float64), Class::Float);
        assert_eq!(Class::from_arrow(&DataType::Utf8View), Class::Text);
    }
}
