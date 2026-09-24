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
//! Literals without a zone, and anything this does not recognise, are left to
//! `DataFusion`.
//!
//! Numbers: the engine reads `1.5` as `decimal(2,1)`, like Trino
//! (`sql_parser.parse_float_as_decimal`), but a number with an exponent
//! (`1e5`, `2.5E-3`) is a `double` in Trino; [`rewrite_exponent_numbers`]
//! turns it into `CAST('<number>' AS DOUBLE)` before planning.

use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    CastKind, DataType, ExactNumberInfo, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
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
    /// Arrow time zone: an IANA name, `UTC` or `+HH:MM`.
    zone: String,
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

/// `YYYY-MM-DD[ HH:MM[:SS[.f…]]] <zone>`; `None` without a zone.
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
    if zone.is_empty() {
        return None;
    }
    let (clock, precision) = match time {
        Some(time) => parse_time(time)?,
        None => ("00:00:00".to_owned(), 0),
    };
    Some(ZonedTimestamp {
        local: format!("{date} {clock}"),
        precision,
        zone: parse_zone(zone)?,
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
        format!("Timestamp({unit}, Some(\"{}\"))", self.zone)
    }

    fn to_expr(&self) -> Expr {
        let arg = |text: String| {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(Value::SingleQuotedString(
                text,
            ))))
        };
        Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("arrow_cast")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![arg(self.local.clone()), arg(self.arrow_type())],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })
    }
}

/// Rewrites every `TIMESTAMP '… <zone>'` literal of `statement` (see the
/// module documentation).
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
        }
        ControlFlow::<()>::Continue(())
    });
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

fn arrow_cast(expr: Expr, arrow_type: String) -> Expr {
    let arg = |e: Expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(e));
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("arrow_cast")]),
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
            if let DataType::Timestamp(precision @ None, tz) = &column.data_type
                && !with_time_zone(*tz)
            {
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
        if !with_time_zone(*tz) {
            *precision = Some(p);
        } else if !matches!(kind, CastKind::TryCast | CastKind::SafeCast) {
            let zone = literal_zone(source).unwrap_or_else(|| session_zone.to_owned());
            let arrow_type = format!("Timestamp({}, Some(\"{zone}\"))", unit_name(p));
            *expr = arrow_cast(source.as_ref().clone(), arrow_type);
        }
        ControlFlow::Continue(())
    });
}

/// Whether a numeric literal has an exponent (`1e5`): a `double` in Trino.
fn has_exponent(number: &str) -> bool {
    number.contains(['e', 'E'])
}

/// Rewrites every number with an exponent of `statement` as a `double` (see
/// the module documentation).
pub(crate) fn rewrite_exponent_numbers(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = visit_expressions_mut(inner.as_mut(), |expr| {
        if let Expr::Value(ValueWithSpan {
            value: Value::Number(number, _),
            ..
        }) = expr
            && has_exponent(number)
        {
            *expr = Expr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(Expr::value(Value::SingleQuotedString(number.clone()))),
                data_type: DataType::Double(ExactNumberInfo::None),
                array: false,
                format: None,
            };
        }
        ControlFlow::<()>::Continue(())
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

    #[test]
    fn only_numbers_with_an_exponent_become_double() {
        use datafusion::sql::parser::DFParser;
        let mut statements =
            DFParser::parse_sql("SELECT 1e5, 2.5E-3, -1e0, 1.5, 10, x FROM t WHERE y > 1e3").unwrap();
        let mut statement = statements.pop_front().unwrap();
        rewrite_exponent_numbers(&mut statement);
        assert_eq!(
            statement.to_string(),
            "SELECT CAST('1e5' AS DOUBLE), CAST('2.5E-3' AS DOUBLE), -CAST('1e0' AS DOUBLE), 1.5, 10, x \
             FROM t WHERE y > CAST('1e3' AS DOUBLE)"
        );
    }

    fn zoned(local: &str, precision: usize, zone: &str) -> ZonedTimestamp {
        ZonedTimestamp {
            local: local.into(),
            precision,
            zone: zone.into(),
        }
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
            "2026-01-01 10:00:00.123",
            "2026-01-01",
            "2026-01-01 10:00:00",
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
                    zone: "UTC".into(),
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
