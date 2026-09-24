//! Trino syntax that `DataFusion` parses with a different meaning, rewritten
//! on the SQL AST before planning:
//!
//! - `TIMESTAMP '2024-01-01 10:00:00.123'` has the precision of its digits
//!   (Trino) instead of nanoseconds; a zone suffix makes it a
//!   `timestamp with time zone` (`with_timezone(…)`).
//! - `CAST(x AS TIMESTAMP)` means `timestamp(3)`, as in Trino.
//! - `DECIMAL '1.50'` is `decimal(3,2)`, not the default decimal.
//! - `x AT TIME ZONE z` becomes `at_timezone(x, z)` (Trino's definition:
//!   same instant, other zone; a plain timestamp is read in the session zone).
//! - `listagg(x [, sep]) WITHIN GROUP (ORDER BY …)` becomes
//!   `array_join(array_agg(x ORDER BY …) FILTER (WHERE x IS NOT NULL), sep)`:
//!   same order, NULLs skipped, NULL when no value is left (`DataFusion` has no `listagg`).
//! - `CAST(x AS varchar(n))` keeps the length Arrow drops: a string is cut to
//!   `n` characters, any other value that does not fit fails (`TRY_CAST`: NULL).
//! - `CAST(x AS json)` and `JSON '…'` build Trino `json` values (see `json_cast`).
//!
//! Arrow only has four timestamp units, so precisions 1–2, 4–5 and 7–8 use the
//! next unit up (`timestamp(1)` behaves as `timestamp(3)`).

use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    AccessExpr, BinaryOperator, CastKind, CharacterLength, DataType, DateTimeField, ExactNumberInfo, Expr,
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArgumentList, FunctionArguments,
    Ident, ObjectName, Subscript, TableFactor, TimezoneInfo, TypedString, Value, ValueWithSpan, VisitMut,
    VisitorMut, visit_expressions_mut,
};

use super::cast::{TRY_VARCHAR_N, VARCHAR_N, parse_timestamp_text, render_timestamp};
use super::json_cast::TO_JSON;

/// Arrow-representable precision for a Trino timestamp precision.
fn arrow_precision(digits: u64) -> u64 {
    match digits {
        0 => 0,
        1..=3 => 3,
        4..=6 => 6,
        _ => 9,
    }
}

fn string_value(text: String) -> Expr {
    Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(text)))
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn cast(expr: Expr, data_type: DataType) -> Expr {
    Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(expr),
        data_type,
        array: false,
        format: None,
    }
}

fn is_plain_timestamp(tz: TimezoneInfo) -> bool {
    matches!(tz, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
}

/// `TIMESTAMP '…'` → `CAST('…' AS TIMESTAMP(p))`, or `with_timezone(…)` when
/// the literal carries a zone.
fn timestamp_literal(text: &str) -> Option<Expr> {
    let parsed = parse_timestamp_text(text)?;
    let precision = arrow_precision(u64::from(parsed.fraction_digits));
    let local = render_timestamp(parsed.local, u32::try_from(precision).ok()?);
    let local = cast(
        string_value(local),
        DataType::Timestamp(Some(precision), TimezoneInfo::None),
    );
    Some(match parsed.zone {
        None => local,
        Some(zone) => call("with_timezone", vec![local, string_value(zone)]),
    })
}

/// `DECIMAL '-1.50'` → `CAST('-1.50' AS DECIMAL(3, 2))`.
fn decimal_literal(text: &str) -> Option<Expr> {
    let trimmed = text.trim();
    let unsigned = trimmed.trim_start_matches(['+', '-']);
    let (int_part, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if int_part.is_empty() && fraction.is_empty()
        || !int_part
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let significant = int_part.trim_start_matches('0').len() + fraction.len();
    let precision = u64::try_from(significant.max(1)).ok()?;
    let scale = u64::try_from(fraction.len()).ok()?;
    Some(cast(
        string_value(trimmed.to_owned()),
        DataType::Decimal(ExactNumberInfo::PrecisionAndScale(
            precision.max(scale),
            i64::try_from(scale).ok()?,
        )),
    ))
}

/// `listagg(x [, sep]) WITHIN GROUP (ORDER BY …)` → `array_join(array_agg(x ORDER BY …), sep)`.
/// Other shapes (`ON OVERFLOW …`, `OVER (…)`) are left to the planner, which rejects them.
fn listagg(function: &Function) -> Option<Expr> {
    if !function.name.to_string().eq_ignore_ascii_case("listagg")
        || function.within_group.is_empty()
        || function.over.is_some()
    {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    if !list.clauses.is_empty() {
        return None;
    }
    let (value, separator) = match list.args.as_slice() {
        [value] => (value.clone(), string_value(String::new())),
        [value, FunctionArg::Unnamed(FunctionArgExpr::Expr(separator))] => (value.clone(), separator.clone()),
        _ => return None,
    };
    // NULLs are skipped: a group of only NULLs aggregates no row, so the
    // result is NULL (Trino), not the empty string.
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(value_expr)) = &value else {
        return None;
    };
    let not_null = Expr::IsNotNull(Box::new(value_expr.clone()));
    let filter = Some(Box::new(match &function.filter {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(Expr::Nested(existing.clone())),
            op: BinaryOperator::And,
            right: Box::new(not_null),
        },
        None => not_null,
    }));
    let aggregate = Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("array_agg")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: list.duplicate_treatment,
            args: vec![value],
            clauses: vec![FunctionArgumentClause::OrderBy(function.within_group.clone())],
        }),
        filter,
        null_treatment: None,
        over: None,
        within_group: vec![],
    });
    Some(call("array_join", vec![aggregate, separator]))
}

fn is_ranking(function: &Function) -> bool {
    function.over.is_some()
        && ["row_number", "rank", "dense_rank", "ntile"]
            .iter()
            .any(|n| function.name.to_string().eq_ignore_ascii_case(n))
}

/// `normalize(x, NFD)`: the form is a keyword in Trino; pass it as text.
fn normalization_form(function: &mut Function) {
    if !function.name.to_string().eq_ignore_ascii_case("normalize") {
        return;
    }
    let FunctionArguments::List(list) = &mut function.args else {
        return;
    };
    if let [_, FunctionArg::Unnamed(FunctionArgExpr::Expr(form))] = list.args.as_mut_slice()
        && let Expr::Identifier(ident) = form
        && ["NFC", "NFD", "NFKC", "NFKD"].contains(&ident.value.to_ascii_uppercase().as_str())
    {
        *form = string_value(ident.value.to_ascii_uppercase());
    }
}

/// Length of a `varchar(n)` type, if it has one.
fn varchar_length(data_type: &DataType) -> Option<u64> {
    match data_type {
        DataType::Varchar(Some(CharacterLength::IntegerLength { length, .. }))
        | DataType::CharacterVarying(Some(CharacterLength::IntegerLength { length, .. }))
        | DataType::CharVarying(Some(CharacterLength::IntegerLength { length, .. })) => Some(*length),
        _ => None,
    }
}

/// `INTERVAL '1-2' YEAR TO MONTH`, `INTERVAL '1 02:03:04' DAY TO SECOND`
/// (Trino's compound intervals) as the unit-by-unit text the planner reads.
fn compound_interval(value: &str, leading: &DateTimeField, last: &DateTimeField) -> Option<String> {
    let text = value.trim();
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let sign = if negative { "-" } else { "" };
    let parts: Vec<&str> = match (leading, last) {
        (DateTimeField::Year, DateTimeField::Month) => text.split('-').collect(),
        _ => text.split([' ', ':']).collect(),
    };
    let units: &[&str] = match (leading, last) {
        (DateTimeField::Year, DateTimeField::Month) => &["years", "months"],
        (DateTimeField::Day, DateTimeField::Hour) => &["days", "hours"],
        (DateTimeField::Day, DateTimeField::Minute) => &["days", "hours", "minutes"],
        (DateTimeField::Day, DateTimeField::Second) => &["days", "hours", "minutes", "seconds"],
        (DateTimeField::Hour, DateTimeField::Minute) => &["hours", "minutes"],
        (DateTimeField::Hour, DateTimeField::Second) => &["hours", "minutes", "seconds"],
        (DateTimeField::Minute, DateTimeField::Second) => &["minutes", "seconds"],
        _ => return None,
    };
    if parts.len() != units.len()
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.chars().all(|c| c.is_ascii_digit() || c == '.'))
    {
        return None;
    }
    Some(
        parts
            .iter()
            .zip(units)
            .map(|(n, unit)| format!("{sign}{n} {unit}"))
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn take(expr: &mut Expr) -> Expr {
    std::mem::replace(expr, Expr::Value(Value::Null.into()))
}

/// Constructs the engine plans with its own functions or semantics.
fn rewrite_builtin_syntax(expr: &mut Expr) -> bool {
    match expr {
        // `x[i][j]`: Trino fails out of bounds and on a missing map key.
        Expr::CompoundFieldAccess { root, access_chain }
            if !access_chain.is_empty()
                && access_chain
                    .iter()
                    .all(|a| matches!(a, AccessExpr::Subscript(Subscript::Index { .. }))) =>
        {
            let mut current = take(root);
            for access in std::mem::take(access_chain) {
                if let AccessExpr::Subscript(Subscript::Index { index }) = access {
                    current = call(super::collections::SUBSCRIPT, vec![current, index]);
                }
            }
            *expr = current;
        }
        // `(expr).field`: the engine only reads fields of columns by dot.
        Expr::CompoundFieldAccess { root, access_chain }
            if !matches!(root.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                && !access_chain.is_empty()
                && access_chain
                    .iter()
                    .all(|a| matches!(a, AccessExpr::Dot(Expr::Identifier(_)))) =>
        {
            let mut current = take(root);
            for access in std::mem::take(access_chain) {
                if let AccessExpr::Dot(Expr::Identifier(field)) = access {
                    current = call("get_field", vec![current, string_value(field.value)]);
                }
            }
            *expr = current;
        }
        // Planned by the engine as its own functions; name the Trino ones.
        Expr::Extract {
            field, expr: inner, ..
        } => {
            let field = string_value(field.to_string());
            let inner = take(inner);
            *expr = call("date_part", vec![field, inner]);
        }
        Expr::Position { expr: needle, r#in } => {
            let (needle, haystack) = (take(needle), take(r#in));
            *expr = call("strpos", vec![haystack, needle]);
        }
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![take(inner)];
            args.push(substring_from.as_deref_mut().map_or_else(
                || Expr::Value(ValueWithSpan::from(Value::Number("1".into(), false))),
                take,
            ));
            if let Some(length) = substring_for.as_deref_mut() {
                args.push(take(length));
            }
            *expr = call("substr", args);
        }
        Expr::Interval(interval) => {
            if let (Some(leading), Some(last)) = (&interval.leading_field, &interval.last_field)
                && let Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(text),
                    ..
                }) = interval.value.as_ref()
                && let Some(units) = compound_interval(text, leading, last)
            {
                *interval.value = string_value(units);
                interval.leading_field = None;
                interval.last_field = None;
                interval.leading_precision = None;
                interval.fractional_seconds_precision = None;
            }
        }
        _ => return false,
    }
    true
}

/// Operators and literals whose Trino meaning the engine lacks.
fn rewrite_operators(expr: &mut Expr) -> bool {
    match expr {
        // `a || b` is Trino's `concat` (strings, varbinary, arrays and
        // elements); the engine's operator only joins strings.
        Expr::BinaryOp {
            op: BinaryOperator::StringConcat,
            left,
            right,
        } => {
            let (left, right) = (take(left), take(right));
            *expr = call(super::string::CONCAT_OPERATOR, vec![left, right]);
        }
        // `X'ED A0 80'`: Trino allows blanks between the hex digits.
        Expr::Value(ValueWithSpan {
            value: Value::HexStringLiteral(digits),
            ..
        }) if digits.contains(char::is_whitespace) => digits.retain(|c| !c.is_whitespace()),
        _ => return false,
    }
    true
}

fn rewrite(expr: &mut Expr) {
    if rewrite_builtin_syntax(expr) || rewrite_operators(expr) {
        return;
    }
    match expr {
        Expr::Cast {
            data_type: data_type @ (DataType::Varbinary(_) | DataType::Binary(_)),
            ..
        } => *data_type = DataType::Bytea,
        // `interval day to second` / `year to month`: one Arrow interval type.
        Expr::Cast {
            data_type: DataType::Interval { fields, precision },
            ..
        } => {
            *fields = None;
            *precision = None;
        }
        Expr::Function(function) => {
            if let Some(replacement) = listagg(function) {
                *expr = replacement;
            } else if is_ranking(function) {
                // Unsigned in the engine, `bigint` in Trino.
                *expr = cast(take(expr), DataType::BigInt(None));
            } else {
                normalization_form(function);
            }
        }
        Expr::Cast {
            expr: inner,
            data_type: DataType::JSON,
            format: None,
            ..
        } => {
            let inner = std::mem::replace(inner.as_mut(), Expr::Value(Value::Null.into()));
            *expr = call(TO_JSON, vec![inner]);
        }
        Expr::TypedString(TypedString {
            data_type: DataType::JSON,
            value,
            ..
        }) => {
            if let Value::SingleQuotedString(text) = &value.value {
                *expr = call("json_parse", vec![string_value(text.clone())]);
            }
        }
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            format: None,
            ..
        } if varchar_length(data_type).is_some() => {
            let length = varchar_length(data_type).unwrap_or_default();
            let name = if matches!(kind, CastKind::Cast | CastKind::DoubleColon) {
                VARCHAR_N
            } else {
                TRY_VARCHAR_N
            };
            let inner = std::mem::replace(inner.as_mut(), Expr::Value(Value::Null.into()));
            let length = Expr::Value(ValueWithSpan::from(Value::Number(length.to_string(), false)));
            *expr = call(name, vec![inner, length]);
        }
        Expr::TypedString(TypedString {
            data_type: DataType::Timestamp(_, tz),
            value,
            ..
        }) if is_plain_timestamp(*tz) => {
            if let Value::SingleQuotedString(text) = &value.value
                && let Some(replacement) = timestamp_literal(text)
            {
                *expr = replacement;
            }
        }
        Expr::TypedString(TypedString {
            data_type: DataType::Decimal(ExactNumberInfo::None) | DataType::Numeric(ExactNumberInfo::None),
            value,
            ..
        }) => {
            if let Value::SingleQuotedString(text) = &value.value
                && let Some(replacement) = decimal_literal(text)
            {
                *expr = replacement;
            }
        }
        Expr::Cast {
            data_type: DataType::Timestamp(precision @ None, _),
            ..
        } => *precision = Some(3),
        Expr::AtTimeZone { timestamp, time_zone } => {
            let timestamp = std::mem::replace(timestamp.as_mut(), Expr::Value(Value::Null.into()));
            let time_zone = std::mem::replace(time_zone.as_mut(), Expr::Value(Value::Null.into()));
            *expr = call("at_timezone", vec![timestamp, time_zone]);
        }
        _ => {}
    }
}

/// `UNNEST(x) AS t(a, b, …)`: one argument, several columns (a map, or an
/// array of rows) becomes one array per column (see `collections`).
#[derive(Debug)]
struct UnnestColumns;

impl VisitorMut for UnnestColumns {
    type Break = ();

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::UNNEST {
            alias: Some(alias),
            array_exprs,
            with_ordinality: false,
            ..
        } = factor
            && array_exprs.len() == 1
            && alias.columns.len() >= 2
        {
            let argument = array_exprs.remove(0);
            for i in 0..alias.columns.len() {
                let index = Expr::Value(ValueWithSpan::from(Value::Number(i.to_string(), false)));
                array_exprs.push(call(
                    super::collections::UNNEST_PART,
                    vec![argument.clone(), index],
                ));
            }
        }
        ControlFlow::Continue(())
    }
}

/// Applies the rewrites to every expression of a statement.
pub(crate) fn rewrite_statement(statement: &mut DFStatement) -> Result<()> {
    match statement {
        DFStatement::Statement(inner) => {
            super::literals::type_literals(inner.as_mut())?;
            super::literals::name_columns(inner.as_mut());
            let _ = inner.visit(&mut UnnestColumns);
            super::lateral::rewrite(inner.as_mut());
            let flow: ControlFlow<DataFusionError> = visit_expressions_mut(inner.as_mut(), |expr| {
                rewrite(expr);
                ControlFlow::Continue(())
            });
            match flow {
                ControlFlow::Break(err) => Err(err),
                ControlFlow::Continue(()) => Ok(()),
            }
        }
        DFStatement::Explain(explain) => rewrite_statement(explain.statement.as_mut()),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::parser::DFParser;

    /// The rewritten SQL without the `_col<i>` output names (tested apart).
    fn rewritten(sql: &str) -> String {
        let mut statement = DFParser::parse_sql(sql).unwrap().pop_front().unwrap();
        rewrite_statement(&mut statement).unwrap();
        let names = regex::Regex::new(r" AS _col\d+").unwrap();
        names.replace_all(&statement.to_string(), "").into_owned()
    }

    #[test]
    fn unnamed_outputs_are_numbered_like_trino() {
        let mut statement = DFParser::parse_sql("SELECT a, t.b, a + 1, count(*) AS n, upper(c) FROM t")
            .unwrap()
            .pop_front()
            .unwrap();
        rewrite_statement(&mut statement).unwrap();
        assert_eq!(
            statement.to_string(),
            "SELECT a, t.b, a + CAST(1 AS INTEGER) AS _col2, count(*) AS n, upper(c) AS _col4 FROM t"
        );
    }

    #[test]
    fn ranking_functions_are_bigint() {
        assert_eq!(
            rewritten("SELECT row_number() OVER (ORDER BY x), rank() OVER (ORDER BY x) FROM t"),
            "SELECT CAST(row_number() OVER (ORDER BY x) AS BIGINT), CAST(rank() OVER (ORDER BY x) AS BIGINT) FROM t"
        );
    }

    #[test]
    fn timestamp_literals_get_their_precision() {
        assert_eq!(
            rewritten("SELECT TIMESTAMP '2024-01-01 10:00:00'"),
            "SELECT CAST('2024-01-01 10:00:00' AS TIMESTAMP(0))"
        );
        assert_eq!(
            rewritten("SELECT TIMESTAMP '2024-01-01 10:00:00.12'"),
            "SELECT CAST('2024-01-01 10:00:00.120' AS TIMESTAMP(3))"
        );
        assert_eq!(
            rewritten("SELECT TIMESTAMP '2024-01-01 10:00:00.1234567'"),
            "SELECT CAST('2024-01-01 10:00:00.123456700' AS TIMESTAMP(9))"
        );
        assert_eq!(
            rewritten("SELECT TIMESTAMP '2024-01-01 10:00:00 America/Sao_Paulo'"),
            "SELECT with_timezone(CAST('2024-01-01 10:00:00' AS TIMESTAMP(0)), 'America/Sao_Paulo')"
        );
    }

    #[test]
    fn casts_and_at_time_zone() {
        assert_eq!(
            rewritten("SELECT CAST(x AS TIMESTAMP), TRY_CAST(y AS TIMESTAMP(6)) FROM t"),
            "SELECT CAST(x AS TIMESTAMP(3)), TRY_CAST(y AS TIMESTAMP(6)) FROM t"
        );
        assert_eq!(
            rewritten("SELECT x AT TIME ZONE 'UTC' FROM t"),
            "SELECT at_timezone(x, 'UTC') FROM t"
        );
        assert_eq!(
            rewritten("SELECT CAST(current_timestamp AT TIME ZONE 'America/Sao_Paulo' AS TIMESTAMP(3))"),
            "SELECT CAST(at_timezone(current_timestamp, 'America/Sao_Paulo') AS TIMESTAMP(3))"
        );
    }

    #[test]
    fn listagg_and_varchar_length() {
        assert_eq!(
            rewritten("SELECT listagg(x, ',') WITHIN GROUP (ORDER BY y DESC) FROM t"),
            "SELECT array_join(array_agg(x ORDER BY y DESC) FILTER (WHERE x IS NOT NULL), ',') FROM t"
        );
        assert_eq!(
            rewritten("SELECT listagg(DISTINCT x) WITHIN GROUP (ORDER BY x) FROM t"),
            "SELECT array_join(array_agg(DISTINCT x ORDER BY x) FILTER (WHERE x IS NOT NULL), '') FROM t"
        );
        assert_eq!(
            rewritten("SELECT string_agg(x, ',') FROM t"),
            "SELECT string_agg(x, ',') FROM t"
        );
        assert_eq!(
            rewritten("SELECT normalize(x, nfd) FROM t"),
            "SELECT normalize(x, 'NFD') FROM t"
        );
        assert_eq!(
            rewritten("SELECT normalize(x, y) FROM t"),
            "SELECT normalize(x, y) FROM t"
        );
        assert_eq!(
            rewritten("SELECT CAST(x AS JSON), JSON '[1]' FROM t"),
            format!("SELECT {TO_JSON}(x), json_parse('[1]') FROM t")
        );
        assert_eq!(
            rewritten("SELECT CAST(x AS VARCHAR(3)), TRY_CAST(y AS VARCHAR(2)), CAST(z AS VARCHAR) FROM t"),
            format!("SELECT {VARCHAR_N}(x, 3), {TRY_VARCHAR_N}(y, 2), CAST(z AS VARCHAR) FROM t")
        );
    }

    #[test]
    fn decimal_literals_get_their_scale() {
        assert_eq!(
            rewritten("SELECT DECIMAL '1.50'"),
            "SELECT CAST('1.50' AS DECIMAL(3,2))"
        );
        assert_eq!(
            rewritten("SELECT DECIMAL '-12'"),
            "SELECT CAST('-12' AS DECIMAL(2,0))"
        );
        assert_eq!(
            rewritten("SELECT DECIMAL '0.05'"),
            "SELECT CAST('0.05' AS DECIMAL(2,2))"
        );
    }

    #[test]
    fn explain_is_rewritten_and_other_text_is_untouched() {
        assert!(rewritten("EXPLAIN SELECT TIMESTAMP '2024-01-01 10:00:00'").contains("TIMESTAMP(0)"));
        assert_eq!(
            rewritten("SELECT 'TIMESTAMP' FROM t"),
            "SELECT 'TIMESTAMP' FROM t"
        );
    }
}
