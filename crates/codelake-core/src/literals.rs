//! Trino's `TIMESTAMP '… <zone>'` literal.
//!
//! In Trino, a timestamp literal that names a time zone
//! (`TIMESTAMP '2026-01-01 10:00:00.123 America/Sao_Paulo'`, `… +03:00`) is a
//! `timestamp(p) with time zone` that keeps the zone, with the precision `p`
//! of its fractional digits. `DataFusion` reads every `TIMESTAMP '…'` as a
//! nanosecond timestamp without zone, shifting the value to UTC.
//!
//! Before planning, such a literal becomes
//! `arrow_cast('<local date-time>', 'Timestamp(<unit>, Some("<zone>"))')`: the
//! local date-time is read in the literal's zone, so the instant is exact and
//! the zone stays in the type. The unit is the smallest that holds `p` digits
//! (0 → seconds, 1–3 → milliseconds, 4–6 → microseconds, 7–9 → nanoseconds).
//! A literal without a zone is a `timestamp(p)`, `p` its fractional digits
//! (`DataFusion`: always nanoseconds), and becomes `arrow_cast('<date-time>',
//! 'Timestamp(<unit>, None)')`. A precision no Arrow unit has (1, 2, 4, 5, 7,
//! 8) is recorded with `codelake_trino_type` (see [`crate::declared`]).
//! Anything this does not recognise is left to `DataFusion`.
//!
//! A `TIME 'HH:MM[:SS[.f…]]'` literal is likewise a `time(p)`, `p` its
//! fractional digits (`DataFusion`: always `time(9)`). Its Arrow type stays
//! nanoseconds, so it meets any other time as before (`DataFusion` has no
//! common type for two time units, not even in `VALUES`); the precision is
//! recorded with `codelake_trino_type` (see [`crate::declared`]), which the
//! client protocol shows. An expression over it (`CASE`, a `VALUES` row) is
//! `time(9)` again.
//!
//! Numbers take their types from the function library's literal rewrite
//! (`crate::trino_functions`: `1.5` is `decimal(2,1)`, `1e5` a `double`).

use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    CastKind, DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement as SqlStatement, TimezoneInfo, TypedString, Value,
    ValueWithSpan, visit_expressions_mut,
};

/// A timestamp literal split into its parts.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ZonedTimestamp {
    /// `YYYY-MM-DD HH:MM:SS[.f…]`, without zone.
    local: String,
    /// Fractional digits (0–9).
    precision: usize,
    /// Arrow time zone: an IANA name, `UTC` or `+HH:MM`; `None` for a
    /// `timestamp(p)` literal.
    zone: Option<String>,
}

fn is_date(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() == 10
        && bytes.iter().enumerate().all(|(i, b)| {
            if i == 4 || i == 7 {
                *b == b'-'
            } else {
                b.is_ascii_digit()
            }
        })
}

/// `HH:MM[:SS[.f…]]` → (`HH:MM:SS[.f…]`, fractional digits).
fn parse_time(text: &str) -> Option<(String, usize)> {
    let (clock, fraction) = match text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (text, None),
    };
    let parts: Vec<&str> = clock.split(':').collect();
    let two_digits = |p: &&str| p.len() == 2 && p.bytes().all(|b| b.is_ascii_digit());
    let clock = match parts.as_slice() {
        [h, m] if two_digits(h) && two_digits(m) => format!("{h}:{m}:00"),
        [h, m, s] if two_digits(h) && two_digits(m) && two_digits(s) => format!("{h}:{m}:{s}"),
        _ => return None,
    };
    match fraction {
        None => Some((clock, 0)),
        Some(f)
            if (1..=9).contains(&f.len()) && f.bytes().all(|b| b.is_ascii_digit()) && parts.len() == 3 =>
        {
            Some((format!("{clock}.{f}"), f.len()))
        }
        Some(_) => None,
    }
}

/// `+03`, `+03:00`, `-0300` → `+03:00`.
fn parse_offset(text: &str) -> Option<String> {
    let sign = text.chars().next().filter(|c| *c == '+' || *c == '-')?;
    let digits: String = text[1..].chars().filter(|c| *c != ':').collect();
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (hours, minutes) = match digits.len() {
        1 | 2 => (digits.as_str(), "00"),
        4 => digits.split_at(2),
        _ => return None,
    };
    let hours: u32 = hours.parse().ok()?;
    let minutes: u32 = minutes.parse().ok()?;
    (hours <= 14 && minutes < 60).then(|| format!("{sign}{hours:02}:{minutes:02}"))
}

fn parse_zone(text: &str) -> Option<String> {
    if text.eq_ignore_ascii_case("Z") || text.eq_ignore_ascii_case("UTC") || text.eq_ignore_ascii_case("GMT")
    {
        return Some("UTC".into());
    }
    if text.starts_with(['+', '-']) {
        return parse_offset(text);
    }
    // An IANA name (`America/Sao_Paulo`); Arrow rejects unknown ones.
    let valid = text
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | '+'));
    (valid && text.chars().next().is_some_and(|c| c.is_ascii_alphabetic())).then(|| text.to_owned())
}

/// `YYYY-MM-DD[ HH:MM[:SS[.f…]]][ <zone>]`.
fn parse(literal: &str) -> Option<ZonedTimestamp> {
    let literal = literal.trim();
    let date = literal.get(..10).filter(|d| is_date(d))?;
    let rest = literal[10..].trim_start();
    let (time, zone) = if rest.starts_with(|c: char| c.is_ascii_digit()) {
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == ':' || c == '.'))
            .unwrap_or(rest.len());
        (Some(&rest[..end]), rest[end..].trim())
    } else {
        (None, rest)
    };
    let (clock, precision) = match time {
        Some(time) => parse_time(time)?,
        None => ("00:00:00".to_owned(), 0),
    };
    let zone = if zone.is_empty() {
        None
    } else {
        Some(parse_zone(zone)?)
    };
    Some(ZonedTimestamp {
        local: format!("{date} {clock}"),
        precision,
        zone,
    })
}

impl ZonedTimestamp {
    fn arrow_type(&self) -> String {
        let unit = match self.precision {
            0 => "Second",
            1..=3 => "Millisecond",
            4..=6 => "Microsecond",
            _ => "Nanosecond",
        };
        match &self.zone {
            Some(zone) => format!("Timestamp({unit}, Some(\"{zone}\"))"),
            None => format!("Timestamp({unit}, None)"),
        }
    }

    fn to_expr(&self) -> Expr {
        let arg = |text: String| {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(Value::SingleQuotedString(
                text,
            ))))
        };
        let call = |name: &str, args: Vec<FunctionArg>| {
            Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new(name)]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args,
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            })
        };
        let value = call(
            "arrow_cast",
            vec![arg(self.local.clone()), arg(self.arrow_type())],
        );
        if [0, 3, 6, 9].contains(&self.precision) {
            return value;
        }
        // No Arrow unit has these digits: the type is recorded aside.
        call(
            crate::declared::TRINO_TYPED,
            vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(value)),
                arg(format!("timestamp({})", self.precision)),
            ],
        )
    }
}

/// Rewrites every `TIMESTAMP '…'` and `TIME '…'` literal of `statement`
/// (see the module documentation).
pub(crate) fn rewrite_zoned_timestamps(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = visit_expressions_mut(inner.as_mut(), |expr| {
        if let Expr::TypedString(TypedString {
            data_type: DataType::Timestamp(None, TimezoneInfo::None),
            value,
            ..
        }) = expr
            && let Some(text) = value.clone().into_string()
            && let Some(zoned) = parse(&text)
        {
            *expr = zoned.to_expr();
        } else if let Expr::TypedString(TypedString {
            data_type: DataType::Time(None, TimezoneInfo::None),
            value,
            ..
        }) = expr
            && let Some(text) = value.clone().into_string()
            && let Some(time) = time_literal(&text)
        {
            *expr = time;
        }
        ControlFlow::<()>::Continue(())
    });
}

/// `TIME '<text>'` as a nanosecond time declaring the precision of its
/// fractional digits; `None` for a text that is not `HH:MM[:SS[.f…]]` (a
/// zone, for one), left to the planner.
fn time_literal(text: &str) -> Option<Expr> {
    let (clock, precision) = parse_time(text.trim())?;
    let value = named_cast(
        "arrow_cast",
        Expr::value(Value::SingleQuotedString(clock)),
        "Time64(Nanosecond)".to_owned(),
    );
    if precision == 9 {
        return Some(value);
    }
    let arg = |e: Expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(e));
    Some(Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(crate::declared::TRINO_TYPED)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                arg(value),
                arg(Expr::value(Value::SingleQuotedString(format!(
                    "time({precision})"
                )))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }))
}

/// `TIMESTAMP` without precision is `timestamp(3)` in Trino.
fn trino_precision(precision: Option<u64>) -> Option<u64> {
    Some(precision.unwrap_or(3)).filter(|p| [0, 3, 6, 9].contains(p))
}

fn unit_name(precision: u64) -> &'static str {
    match precision {
        0 => "Second",
        3 => "Millisecond",
        6 => "Microsecond",
        _ => "Nanosecond",
    }
}

/// The zone a text literal names, as Trino prints it.
fn literal_zone(expr: &Expr) -> Option<String> {
    let Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(text),
        ..
    }) = expr
    else {
        return None;
    };
    crate::timestamps::zone_of_text(text)
}

/// `<function>(expr, '<Arrow type>')`: `arrow_cast` or its `TRY_CAST`
/// counterpart.
fn named_cast(function: &str, expr: Expr, arrow_type: String) -> Expr {
    let arg = |e: Expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(e));
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(function)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![arg(expr), arg(Expr::value(Value::SingleQuotedString(arrow_type)))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn with_time_zone(tz: TimezoneInfo) -> bool {
    matches!(tz, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz)
}

/// Trino's timestamp types (see the module documentation of
/// [`crate::timestamps`]): `TIMESTAMP` is `timestamp(3)` in casts and column
/// definitions, and `CAST(x AS TIMESTAMP(p) WITH TIME ZONE)` keeps a zone —
/// the one a text literal names, or `session_zone` — instead of none.
pub(crate) fn rewrite_timestamp_types(statement: &mut DFStatement, session_zone: &str) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    if let SqlStatement::CreateTable(create) = inner.as_mut() {
        for column in &mut create.columns {
            if let DataType::Timestamp(precision @ None, tz) = &column.data_type {
                column.data_type = DataType::Timestamp(trino_precision(*precision), *tz);
            }
        }
    }
    let _ = visit_expressions_mut(inner.as_mut(), |expr| {
        let Expr::Cast {
            kind,
            expr: source,
            data_type: DataType::Timestamp(precision, tz),
            ..
        } = expr
        else {
            return ControlFlow::<()>::Continue(());
        };
        let Some(p) = trino_precision(*precision) else {
            return ControlFlow::Continue(());
        };
        if with_time_zone(*tz) {
            let zone = literal_zone(source).unwrap_or_else(|| session_zone.to_owned());
            let arrow_type = format!("Timestamp({}, Some(\"{zone}\"))", unit_name(p));
            let function = if matches!(kind, CastKind::TryCast | CastKind::SafeCast) {
                crate::timestamp_casts::TRY_ARROW_CAST
            } else {
                "arrow_cast"
            };
            *expr = named_cast(function, source.as_ref().clone(), arrow_type);
        } else {
            *precision = Some(p);
        }
        ControlFlow::Continue(())
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewritten(sql: &str) -> String {
        use datafusion::sql::parser::DFParser;
        let mut statement = DFParser::parse_sql(sql).unwrap().pop_front().unwrap();
        rewrite_timestamp_types(&mut statement, "UTC");
        statement.to_string()
    }

    #[test]
    fn timestamps_take_trino_precision_and_zone() {
        assert_eq!(
            rewritten("SELECT CAST(x AS TIMESTAMP), CAST(x AS TIMESTAMP(6)), x::timestamp FROM t"),
            "SELECT CAST(x AS TIMESTAMP(3)), CAST(x AS TIMESTAMP(6)), x::TIMESTAMP(3) FROM t"
        );
        assert_eq!(
            rewritten("SELECT CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP(3) WITH TIME ZONE)"),
            "SELECT arrow_cast('2026-01-01 10:00:00 America/Sao_Paulo', \
             'Timestamp(Millisecond, Some(\"America/Sao_Paulo\"))')"
        );
        assert_eq!(
            rewritten(
                "SELECT CAST(x AS TIMESTAMP WITH TIME ZONE), CAST('2026-01-01' AS TIMESTAMP(0) WITH TIME ZONE)"
            ),
            "SELECT arrow_cast(x, 'Timestamp(Millisecond, Some(\"UTC\"))'), \
             arrow_cast('2026-01-01', 'Timestamp(Second, Some(\"UTC\"))')"
        );
        assert_eq!(
            rewritten("SELECT CAST('2026-01-01 10:00 +03' AS TIMESTAMP(6) WITH TIME ZONE)"),
            "SELECT arrow_cast('2026-01-01 10:00 +03', 'Timestamp(Microsecond, Some(\"+03:00\"))')"
        );
        // Precisions Arrow cannot hold are left to the planner, which refuses them.
        assert_eq!(
            rewritten("SELECT CAST(x AS TIMESTAMP(1))"),
            "SELECT CAST(x AS TIMESTAMP(1))"
        );
        assert_eq!(
            rewritten("CREATE TABLE t (a TIMESTAMP, b TIMESTAMP(6), c INT)"),
            "CREATE TABLE t (a TIMESTAMP(3), b TIMESTAMP(6), c INT)"
        );
    }

    fn zoned(local: &str, precision: usize, zone: &str) -> ZonedTimestamp {
        ZonedTimestamp {
            local: local.into(),
            precision,
            zone: Some(zone.into()),
        }
    }

    /// Trino 431: `TIMESTAMP '2026-01-01 10:00:00.123'` is `timestamp(3)`,
    /// `TIMESTAMP '2026-01-01'` is `timestamp(0)` at midnight.
    #[test]
    fn a_literal_without_zone_takes_its_digits() {
        let local = |text: &str, precision| ZonedTimestamp {
            local: text.into(),
            precision,
            zone: None,
        };
        assert_eq!(
            parse("2026-01-01 10:00:00.123"),
            Some(local("2026-01-01 10:00:00.123", 3))
        );
        assert_eq!(parse("2026-01-01"), Some(local("2026-01-01 00:00:00", 0)));
        assert_eq!(
            parse("1969-12-31 23:59:59.5"),
            Some(local("1969-12-31 23:59:59.5", 1))
        );
        assert_eq!(local("x", 3).arrow_type(), "Timestamp(Millisecond, None)");
        let mut statement = datafusion::sql::parser::DFParser::parse_sql(
            "SELECT TIMESTAMP '2026-01-01 10:00:00.123', TIMESTAMP '1969-12-31 23:59:59.5'",
        )
        .unwrap()
        .pop_front()
        .unwrap();
        rewrite_zoned_timestamps(&mut statement);
        assert_eq!(
            statement.to_string(),
            "SELECT arrow_cast('2026-01-01 10:00:00.123', 'Timestamp(Millisecond, None)'), \
             codelake_trino_type(arrow_cast('1969-12-31 23:59:59.5', 'Timestamp(Millisecond, None)'), 'timestamp(1)')"
        );
    }

    /// The reference engine: `TIME '12:34:56.789'` is `time(3)`, `TIME '12:34'` is
    /// `time(0)`; a zone is not this literal's.
    #[test]
    fn a_time_literal_takes_its_digits() {
        let mut statement = datafusion::sql::parser::DFParser::parse_sql(
            "SELECT TIME '12:34', TIME '12:34:56.789', TIME '12:34:56.7', TIME ' 12:34:56.1234 ', \
             TIME '12:34:56.123456789', TIME '12:34:56+03:00', TIME 'noon', TIME '12:34:56.'",
        )
        .unwrap()
        .pop_front()
        .unwrap();
        rewrite_zoned_timestamps(&mut statement);
        assert_eq!(
            statement.to_string(),
            "SELECT codelake_trino_type(arrow_cast('12:34:00', 'Time64(Nanosecond)'), 'time(0)'), \
             codelake_trino_type(arrow_cast('12:34:56.789', 'Time64(Nanosecond)'), 'time(3)'), \
             codelake_trino_type(arrow_cast('12:34:56.7', 'Time64(Nanosecond)'), 'time(1)'), \
             codelake_trino_type(arrow_cast('12:34:56.1234', 'Time64(Nanosecond)'), 'time(4)'), \
             arrow_cast('12:34:56.123456789', 'Time64(Nanosecond)'), \
             TIME '12:34:56+03:00', TIME 'noon', TIME '12:34:56.'"
        );
    }

    #[test]
    fn parses_trino_literal_forms() {
        let cases = [
            (
                "2026-01-01 10:00:00.123 America/Sao_Paulo",
                zoned("2026-01-01 10:00:00.123", 3, "America/Sao_Paulo"),
            ),
            (
                "2026-01-01 10:00:00 +03:00",
                zoned("2026-01-01 10:00:00", 0, "+03:00"),
            ),
            (
                "2026-01-01 10:00:00.1 -03:00",
                zoned("2026-01-01 10:00:00.1", 1, "-03:00"),
            ),
            (
                "2026-01-01 10:00:00.123456 UTC",
                zoned("2026-01-01 10:00:00.123456", 6, "UTC"),
            ),
            (
                "2026-01-01 10:00:00.123456789 Z",
                zoned("2026-01-01 10:00:00.123456789", 9, "UTC"),
            ),
            (
                "2026-01-01 10:00:00.123 GMT",
                zoned("2026-01-01 10:00:00.123", 3, "UTC"),
            ),
            (
                "2026-01-01 10:00:00.123 +03",
                zoned("2026-01-01 10:00:00.123", 3, "+03:00"),
            ),
            (
                "2026-01-01 10:00:00.123+03:00",
                zoned("2026-01-01 10:00:00.123", 3, "+03:00"),
            ),
            (
                "2026-01-01 America/Sao_Paulo",
                zoned("2026-01-01 00:00:00", 0, "America/Sao_Paulo"),
            ),
            (
                "2026-01-01 10:00 America/Sao_Paulo",
                zoned("2026-01-01 10:00:00", 0, "America/Sao_Paulo"),
            ),
            (
                "  2026-01-01 10:00:00 Europe/Lisbon ",
                zoned("2026-01-01 10:00:00", 0, "Europe/Lisbon"),
            ),
        ];
        for (literal, expected) in cases {
            assert_eq!(parse(literal), Some(expected), "{literal}");
        }
    }

    #[test]
    fn leaves_other_literals_to_datafusion() {
        for literal in [
            "2026-1-1 10:00:00 UTC",
            "2026-01-01 10:00:00.1234567890 UTC",
            "2026-01-01 10:00:00 +25:00",
            "2026-01-01 10:0:00 UTC",
            "garbage",
            "",
        ] {
            assert_eq!(parse(literal), None, "{literal}");
        }
    }

    #[test]
    fn unit_follows_the_fractional_digits() {
        let types: Vec<String> = [0, 1, 3, 4, 6, 7, 9]
            .into_iter()
            .map(|precision| {
                ZonedTimestamp {
                    local: String::new(),
                    precision,
                    zone: Some("UTC".into()),
                }
                .arrow_type()
            })
            .collect();
        assert_eq!(
            types,
            [
                "Timestamp(Second, Some(\"UTC\"))",
                "Timestamp(Millisecond, Some(\"UTC\"))",
                "Timestamp(Millisecond, Some(\"UTC\"))",
                "Timestamp(Microsecond, Some(\"UTC\"))",
                "Timestamp(Microsecond, Some(\"UTC\"))",
                "Timestamp(Nanosecond, Some(\"UTC\"))",
                "Timestamp(Nanosecond, Some(\"UTC\"))",
            ]
        );
    }
}
