//! Trino types narrower than their Arrow type: `varchar(n)`, `char(n)` and
//! `timestamp(p)` with `p` other than 0, 3, 6 or 9 (see
//! [`crate::trino_types::Declared`]).
//!
//! `DataFusion` reads `VARCHAR(5)` and `CHAR(3)` as unbounded text and has no
//! `TIMESTAMP(1)`. Trino 431:
//!
//! - `CAST(x AS varchar(n))` of text keeps the first `n` characters; of
//!   anything else fails when its text is longer (`Value 123 cannot be
//!   represented as varchar(2)`); `CAST(x AS char(n))` pads with spaces;
//! - `INSERT` into a `varchar(n)` or `char(n)` column fails when a value has
//!   more than `n` characters besides trailing spaces (`Cannot truncate
//!   non-space characters when casting from varchar(13) to varchar(10) on
//!   INSERT`);
//! - `timestamp(p)` holds `p` fraction digits, rounded half up.
//!
//! Before planning, [`rewrite_declared_casts`] turns such a cast into
//! `codelake_trino_type(x, '<type>')` ([`TrinoTyped`]), which converts the
//! value and names the type in the result field's metadata; the protocol,
//! `information_schema` and `DESCRIBE` read it from there. A `timestamp(p)`
//! is carried by the smallest Arrow unit holding `p` digits.
//! [`declared_columns`] reads the declared types of a `CREATE TABLE`, which
//! the engine records on the new table's fields, and [`check_insert`] adds
//! Trino's length check to an `INSERT` into such columns.
//!
//! A `char(n)` value is stored without its padding (comparisons ignore
//! trailing spaces in Trino) and padded when sent to a client.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::logical_plan::dml::{DmlStatement, WriteOp};
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Projection, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    self as sql, CastKind, CharacterLength, Statement as SqlStatement, TimezoneInfo, visit_expressions_mut,
};

use crate::trino_types::Declared;

/// Name of [`TrinoTyped`].
pub(crate) const TRINO_TYPED: &str = "codelake_trino_type";

/// How a [`TrinoTyped`] converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Mode {
    /// `CAST` of text: truncate.
    Cast,
    /// `CAST` of another type, already cast to text: fail when too long.
    Strict,
    /// `INSERT` into a column: fail on non-space characters past the length.
    Insert,
}

/// `codelake_trino_type(x, '<type>')`: `x` as a value of a declared type
/// (see the module documentation); `''` drops a declared type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct TrinoTyped {
    mode: Mode,
    signature: Signature,
}

impl TrinoTyped {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

pub(crate) fn trino_typed() -> ScalarUDF {
    ScalarUDF::new_from_impl(TrinoTyped::new(Mode::Cast))
}

fn declared_of(scalar: Option<&ScalarValue>) -> Result<Option<Declared>> {
    let Some(
        ScalarValue::Utf8(Some(text))
        | ScalarValue::Utf8View(Some(text))
        | ScalarValue::LargeUtf8(Some(text)),
    ) = scalar
    else {
        return plan_err!("{TRINO_TYPED} expects a type name as its second argument");
    };
    if text.is_empty() {
        return Ok(None);
    }
    let Some(declared) = Declared::parse(text) else {
        return plan_err!("{TRINO_TYPED}: unknown type {text}");
    };
    Ok(Some(declared))
}

/// The smallest Arrow unit holding `p` fraction digits.
fn unit_for(precision: u8) -> TimeUnit {
    match precision {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

fn result_type(declared: Option<Declared>, input: &DataType) -> Result<DataType> {
    Ok(match (declared, input) {
        (None, other) => other.clone(),
        (Some(Declared::Varchar(_) | Declared::Char(_)), _) => DataType::Utf8,
        (Some(Declared::Timestamp(p)), DataType::Timestamp(_, zone)) => {
            DataType::Timestamp(unit_for(p), zone.clone())
        }
        (Some(Declared::Timestamp(p)), other) => {
            return plan_err!("{TRINO_TYPED}: timestamp({p}) of {other}");
        }
    })
}

fn too_long(mode: Mode, text: &str, declared: Declared) -> ArrowError {
    let target = declared.name(&DataType::Utf8);
    match mode {
        // Named like the function library's errors (`<errorName>: …`), so
        // every layer reports it the same way, whichever catalog the table
        // is in.
        Mode::Insert => ArrowError::InvalidArgumentError(format!(
            "INVALID_CAST_ARGUMENT: Cannot truncate non-space characters when casting from varchar({}) to \
             {target} on INSERT",
            text.chars().count()
        )),
        _ => ArrowError::CastError(format!("Value {text} cannot be represented as {target}")),
    }
}

/// A text as a `varchar(n)` / `char(n)` value (`char` without its padding).
fn bounded(mode: Mode, text: &str, declared: Declared, length: u32) -> Result<String, ArrowError> {
    let length = usize::try_from(length).unwrap_or(usize::MAX);
    let mut value: String = if text.chars().count() <= length {
        text.to_owned()
    } else {
        let (kept, rest) = text.split_at(text.char_indices().nth(length).map_or(text.len(), |(i, _)| i));
        match mode {
            Mode::Cast => kept.to_owned(),
            Mode::Insert if rest.chars().all(|c| c == ' ') => kept.to_owned(),
            Mode::Strict | Mode::Insert => return Err(too_long(mode, text, declared)),
        }
    };
    if matches!(declared, Declared::Char(_)) {
        value.truncate(value.trim_end_matches(' ').len());
    }
    Ok(value)
}

fn texts(array: &ArrayRef) -> Result<Vec<Option<String>>> {
    let text = cast(array, &DataType::Utf8)?;
    Ok(text
        .as_string::<i32>()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect())
}

/// Timestamps rounded half up to `precision` digits, in its unit.
fn rounded_timestamps(array: &ArrayRef, precision: u8) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, zone) = array.data_type() else {
        return exec_err!("{TRINO_TYPED}: timestamp({precision}) of {}", array.data_type());
    };
    let digits = crate::timestamp_casts::digits_of(*unit);
    let raw = cast(array, &DataType::Int64)?;
    let target = unit_for(precision);
    let step = 10_i64.pow(digits.saturating_sub(u32::from(precision)));
    let per_target = 10_i64.pow(digits.saturating_sub(crate::timestamp_casts::digits_of(target)));
    let mut out = Vec::with_capacity(raw.len());
    for value in raw.as_primitive::<datafusion::arrow::datatypes::Int64Type>() {
        out.push(match value {
            None => None,
            Some(v) => {
                let rounded = v
                    .checked_add(step / 2)
                    .map(|v| v.div_euclid(step) * step)
                    .ok_or_else(|| ArrowError::CastError("Timestamp out of range".into()))?;
                Some(rounded.div_euclid(per_target))
            }
        });
    }
    Ok(crate::timestamps::timestamps(target, out, zone.clone()))
}

impl ScalarUDFImpl for TrinoTyped {
    fn name(&self) -> &'static str {
        TRINO_TYPED
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("{TRINO_TYPED} is typed by its arguments")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared = declared_of(args.scalar_arguments.get(1).copied().flatten())?;
        let Some(input) = args.arg_fields.first() else {
            return plan_err!("{TRINO_TYPED} expects two arguments");
        };
        let field = Field::new(
            self.name(),
            result_type(declared, input.data_type())?,
            input.is_nullable(),
        );
        Ok(Arc::new(match declared {
            Some(declared) => field.with_metadata(declared.metadata()),
            None => field,
        }))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value, name] = args.args.as_slice() else {
            return exec_err!("{TRINO_TYPED} expects two arguments");
        };
        let ColumnarValue::Scalar(name) = name else {
            return exec_err!("{TRINO_TYPED} expects a type name as its second argument");
        };
        let declared = declared_of(Some(name))?;
        let array = value.to_array(args.number_rows)?;
        let result: ArrayRef = match declared {
            None => array,
            Some(Declared::Timestamp(p)) => rounded_timestamps(&array, p)?,
            Some(declared @ (Declared::Varchar(n) | Declared::Char(n))) => {
                let mut out = Vec::with_capacity(array.len());
                for text in texts(&array)? {
                    out.push(match text {
                        None => None,
                        Some(text) => Some(bounded(self.mode, &text, declared, n)?),
                    });
                }
                Arc::new(StringArray::from(out))
            }
        };
        Ok(match value {
            ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?),
            ColumnarValue::Array(_) => ColumnarValue::Array(result),
        })
    }
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// `codelake_trino_type(<not text>, 'varchar(n)')` → the Trino text of the
/// value, checked against the length (the text cast then follows Trino's
/// casts, see [`crate::overflow`]).
pub(crate) fn check_typed(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = &expr else {
        return Ok(Transformed::no(expr));
    };
    let Some(typed) = func.inner().downcast_ref::<TrinoTyped>() else {
        return Ok(Transformed::no(expr));
    };
    let [value, name] = args.as_slice() else {
        return Ok(Transformed::no(expr));
    };
    let is_bounded_text = matches!(
        name,
        Expr::Literal(ScalarValue::Utf8(Some(n)), _) if matches!(Declared::parse(n), Some(Declared::Varchar(_) | Declared::Char(_)))
    );
    if typed.mode != Mode::Cast || !is_bounded_text || is_text(&value.get_type(schema)?) {
        return Ok(Transformed::no(expr));
    }
    let text = Expr::Cast(Cast::new(Box::new(value.clone()), DataType::Utf8));
    let strict = ScalarUDF::new_from_impl(TrinoTyped::new(Mode::Strict));
    Ok(Transformed::yes(strict.call(vec![text, name.clone()])))
}

// ---------------------------------------------------------------- SQL

fn declared_type(data_type: &sql::DataType) -> Option<Declared> {
    use sql::DataType as T;
    let length = |l: &Option<CharacterLength>| match l {
        Some(CharacterLength::IntegerLength { length, .. }) => u32::try_from(*length).ok(),
        _ => None,
    };
    match data_type {
        T::Varchar(l) | T::CharacterVarying(l) | T::CharVarying(l) => length(l).map(Declared::Varchar),
        T::Char(l) | T::Character(l) => Some(Declared::Char(length(l).unwrap_or(1))),
        T::Timestamp(Some(p), _) if ![0, 3, 6, 9].contains(p) && *p <= 9 => {
            Some(Declared::Timestamp(u8::try_from(*p).ok()?))
        }
        _ => None,
    }
}

fn typed_call(expr: sql::Expr, declared: Declared) -> sql::Expr {
    use sql::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    };
    let name = match declared {
        Declared::Varchar(n) => format!("varchar({n})"),
        Declared::Char(n) => format!("char({n})"),
        Declared::Timestamp(p) => format!("timestamp({p})"),
    };
    let arg = |e: sql::Expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(e));
    sql::Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(TRINO_TYPED)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                arg(expr),
                arg(sql::Expr::value(sql::Value::SingleQuotedString(name))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// `CAST(x AS varchar(n) | char(n) | timestamp(p))` → `codelake_trino_type`
/// (see the module documentation). A `timestamp(p)` is first read with nine
/// digits, then rounded.
pub(crate) fn rewrite_declared_casts(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = visit_expressions_mut(inner.as_mut(), |expr| {
        if let sql::Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            data_type,
            ..
        } = expr
            && let Some(declared) = declared_type(data_type)
        {
            let mut cast = std::mem::replace(expr, sql::Expr::value(sql::Value::Null));
            if let (sql::Expr::Cast { data_type, .. }, Declared::Timestamp(_)) = (&mut cast, declared) {
                let zone = match data_type {
                    sql::DataType::Timestamp(_, tz) => *tz,
                    _ => TimezoneInfo::None,
                };
                *data_type = sql::DataType::Timestamp(Some(9), zone);
                *expr = typed_call(cast, declared);
            } else if let sql::Expr::Cast { expr: value, .. } = cast {
                *expr = typed_call(*value, declared);
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// The declared types of the columns of a `CREATE TABLE` (`varchar(n)`,
/// `char(n)`), by column name; the columns become unbounded text, as
/// `DataFusion` reads them anyway.
pub(crate) fn declared_columns(statement: &mut DFStatement) -> Vec<(String, Declared)> {
    let DFStatement::Statement(inner) = statement else {
        return Vec::new();
    };
    let SqlStatement::CreateTable(create) = inner.as_mut() else {
        return Vec::new();
    };
    let mut declared = Vec::new();
    for column in &mut create.columns {
        if let Some(d @ (Declared::Varchar(_) | Declared::Char(_))) = declared_type(&column.data_type) {
            declared.push((column.name.value.clone(), d));
            column.data_type = sql::DataType::Varchar(None);
        }
    }
    declared
}

/// Trino's type syntax in a `CREATE TABLE` column list, spelled the way the
/// parser reads it: `array(T)` → `ARRAY<T>`, `row(a T, …)` → `STRUCT<a T,
/// …>` (`map(K, V)` parses as it is). Any other statement is left alone.
pub fn trino_type_syntax(sql: &str) -> std::borrow::Cow<'_, str> {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::keywords::Keyword;
    use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        return std::borrow::Cow::Borrowed(sql);
    };
    let significant: Vec<usize> = (0..tokens.len())
        .filter(|i| !matches!(tokens[*i], Token::Whitespace(_)))
        .collect();
    let keyword = |k: usize| match significant.get(k).map(|i| &tokens[*i]) {
        Some(Token::Word(w)) => w.keyword,
        _ => Keyword::NoKeyword,
    };
    // CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name[.name…] (
    let mut k = 0;
    if keyword(k) != Keyword::CREATE {
        return std::borrow::Cow::Borrowed(sql);
    }
    k += 1;
    if keyword(k) == Keyword::OR && keyword(k + 1) == Keyword::REPLACE {
        k += 2;
    }
    if keyword(k) != Keyword::TABLE {
        return std::borrow::Cow::Borrowed(sql);
    }
    k += 1;
    if keyword(k) == Keyword::IF && keyword(k + 1) == Keyword::NOT && keyword(k + 2) == Keyword::EXISTS {
        k += 3;
    }
    loop {
        if !matches!(significant.get(k).map(|i| &tokens[*i]), Some(Token::Word(_))) {
            return std::borrow::Cow::Borrowed(sql);
        }
        k += 1;
        if matches!(significant.get(k).map(|i| &tokens[*i]), Some(Token::Period)) {
            k += 1;
        } else {
            break;
        }
    }
    let Some(&start) = significant.get(k).filter(|i| tokens[**i] == Token::LParen) else {
        return std::borrow::Cow::Borrowed(sql);
    };
    let mut out: Vec<String> = tokens.iter().map(ToString::to_string).collect();
    let (mut depth, mut angles, mut pending, mut changed) = (0_usize, Vec::new(), false, false);
    for (position, &i) in significant.iter().enumerate().skip(k) {
        match &tokens[i] {
            Token::LParen => {
                depth += 1;
                if pending {
                    out[i] = "<".into();
                    angles.push(depth);
                    pending = false;
                }
            }
            Token::RParen => {
                if angles.last() == Some(&depth) {
                    out[i] = ">".into();
                    angles.pop();
                }
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    break;
                }
            }
            Token::Word(w) if i > start => {
                let next_is_paren = significant
                    .get(position + 1)
                    .is_some_and(|n| tokens[*n] == Token::LParen);
                let spelled = match w.value.to_ascii_lowercase().as_str() {
                    "array" if next_is_paren && w.quote_style.is_none() => Some("ARRAY"),
                    "row" if next_is_paren && w.quote_style.is_none() => Some("STRUCT"),
                    _ => None,
                };
                if let Some(spelled) = spelled {
                    out[i] = spelled.into();
                    pending = true;
                    changed = true;
                }
            }
            _ => {}
        }
    }
    if changed {
        std::borrow::Cow::Owned(out.concat())
    } else {
        std::borrow::Cow::Borrowed(sql)
    }
}

/// `map(K, V)` and `varbinary` columns of a `CREATE TABLE`, which
/// `DataFusion` cannot read: `varbinary` becomes `BYTEA` (binary); a map
/// column becomes `BYTEA` too and is returned with its key and value types,
/// for the engine to give the new table its Arrow map type.
pub(crate) fn map_columns(statement: &mut DFStatement) -> Vec<(String, sql::DataType, sql::DataType)> {
    let DFStatement::Statement(inner) = statement else {
        return Vec::new();
    };
    let SqlStatement::CreateTable(create) = inner.as_mut() else {
        return Vec::new();
    };
    let mut maps = Vec::new();
    for column in &mut create.columns {
        match &column.data_type {
            sql::DataType::Varbinary(_) => column.data_type = sql::DataType::Bytea,
            sql::DataType::Map(key, value) => {
                maps.push((
                    column.name.value.clone(),
                    key.as_ref().clone(),
                    value.as_ref().clone(),
                ));
                column.data_type = sql::DataType::Bytea;
            }
            _ => {}
        }
    }
    maps
}

/// `schema` with each declared column's type recorded in its metadata.
pub(crate) fn with_declared(
    schema: &datafusion::arrow::datatypes::Schema,
    declared: &[(String, Declared)],
) -> datafusion::arrow::datatypes::Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            match declared
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(field.name()))
            {
                Some((_, d)) if d.fits(field.data_type()) => {
                    let mut metadata = field.metadata().clone();
                    metadata.extend(d.metadata());
                    field.as_ref().clone().with_metadata(metadata)
                }
                _ => field.as_ref().clone(),
            }
        })
        .collect();
    datafusion::arrow::datatypes::Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// `INSERT` into `varchar(n)` / `char(n)` columns: each value checked as Trino
/// checks it (see the module documentation).
pub(crate) fn check_insert(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(Transformed::no(plan));
    };
    if !matches!(dml.op, WriteOp::Insert(_)) {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    }
    let table = dml.target.schema();
    let bounded: Vec<Option<Declared>> = table
        .fields()
        .iter()
        .map(|f| {
            crate::trino_types::declared(f).filter(|d| matches!(d, Declared::Varchar(_) | Declared::Char(_)))
        })
        .collect();
    let input_schema = dml.input.schema();
    if bounded.iter().all(Option::is_none) || input_schema.fields().len() != bounded.len() {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    }
    let insert = ScalarUDF::new_from_impl(TrinoTyped::new(Mode::Insert));
    let exprs = input_schema
        .iter()
        .zip(&bounded)
        .map(|((qualifier, field), declared)| {
            let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
            match declared {
                Some(d) => insert
                    .call(vec![
                        column,
                        Expr::Literal(ScalarValue::Utf8(Some(d.name(&DataType::Utf8))), None),
                    ])
                    .alias(field.name()),
                None => column,
            }
        })
        .collect();
    let input = LogicalPlan::Projection(Projection::try_new(exprs, Arc::clone(&dml.input))?);
    Ok(Transformed::yes(LogicalPlan::Dml(DmlStatement::new(
        dml.table_name,
        dml.target,
        dml.op,
        Arc::new(input),
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values observed on Trino 431.
    #[test]
    fn bounded_text_follows_trino() {
        let v5 = Declared::Varchar(5);
        assert_eq!(bounded(Mode::Cast, "abcdefgh", v5, 5).unwrap(), "abcde");
        assert_eq!(bounded(Mode::Cast, "ab", v5, 5).unwrap(), "ab");
        assert_eq!(
            bounded(Mode::Cast, "ação", Declared::Varchar(2), 2).unwrap(),
            "aç"
        );
        assert_eq!(
            bounded(Mode::Cast, "ab   ", Declared::Varchar(3), 3).unwrap(),
            "ab "
        );
        assert!(bounded(Mode::Strict, "123", Declared::Varchar(2), 2).is_err());
        let v10 = Declared::Varchar(10);
        let error = bounded(Mode::Insert, "abcdefghijKLM", v10, 10)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("from varchar(13) to varchar(10) on INSERT"),
            "{error}"
        );
        assert_eq!(
            bounded(Mode::Insert, "abcdefghij   ", v10, 10).unwrap(),
            "abcdefghij"
        );
        let c3 = Declared::Char(3);
        assert_eq!(bounded(Mode::Cast, "abcdefg", c3, 3).unwrap(), "abc");
        assert_eq!(bounded(Mode::Cast, "a  ", c3, 3).unwrap(), "a");
    }

    #[test]
    fn trino_type_names_parse() {
        assert_eq!(
            trino_type_syntax(
                "CREATE TABLE s.t (a array(integer), r row(x integer, y array(varchar)), m map(varchar, bigint))"
            ),
            "CREATE TABLE s.t (a ARRAY<integer>, r STRUCT<x integer, y ARRAY<varchar>>, m map(varchar, bigint))"
        );
        // Only the column list of a CREATE TABLE.
        for untouched in [
            "SELECT row(1, 2), array(1)",
            "CREATE TABLE t AS SELECT row(1, 2) AS r",
            "CREATE TABLE t (\"array\" integer)",
        ] {
            assert_eq!(trino_type_syntax(untouched), untouched);
        }
        let mut create = datafusion::sql::parser::DFParser::parse_sql(
            "CREATE TABLE t (m map(varchar, bigint), b varbinary, c int)",
        )
        .unwrap()
        .pop_front()
        .unwrap();
        let maps = map_columns(&mut create);
        assert_eq!(maps.len(), 1);
        assert_eq!(
            (maps[0].0.as_str(), maps[0].1.to_string(), maps[0].2.to_string()),
            ("m", "VARCHAR".into(), "BIGINT".into())
        );
        assert_eq!(create.to_string(), "CREATE TABLE t (m BYTEA, b BYTEA, c INT)");
    }

    #[test]
    fn declared_sql_types() {
        use datafusion::sql::parser::DFParser;
        let mut statement = DFParser::parse_sql(
            "SELECT CAST(a AS VARCHAR(5)), CAST(b AS CHAR(3)), CAST(c AS VARCHAR), CAST(d AS TIMESTAMP(1)), \
             CAST(e AS TIMESTAMP(6)), CAST(f AS TIMESTAMP(2) WITH TIME ZONE)",
        )
        .unwrap()
        .pop_front()
        .unwrap();
        rewrite_declared_casts(&mut statement);
        assert_eq!(
            statement.to_string(),
            "SELECT codelake_trino_type(a, 'varchar(5)'), codelake_trino_type(b, 'char(3)'), CAST(c AS VARCHAR), \
             codelake_trino_type(CAST(d AS TIMESTAMP(9)), 'timestamp(1)'), CAST(e AS TIMESTAMP(6)), \
             codelake_trino_type(CAST(f AS TIMESTAMP(9) WITH TIME ZONE), 'timestamp(2)')"
        );
        let mut create =
            DFParser::parse_sql("CREATE TABLE t (a VARCHAR(10), b CHAR(3), c VARCHAR, d BIGINT)")
                .unwrap()
                .pop_front()
                .unwrap();
        assert_eq!(
            declared_columns(&mut create),
            [
                ("a".to_owned(), Declared::Varchar(10)),
                ("b".to_owned(), Declared::Char(3))
            ]
        );
        assert_eq!(
            create.to_string(),
            "CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR, d BIGINT)"
        );
    }
}
