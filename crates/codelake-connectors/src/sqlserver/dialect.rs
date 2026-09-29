//! T-SQL rendering and the SQL Server pushdown policy.
//!
//! The policy only lets SQL Server evaluate operators whose result is identical
//! to the engine's. Everything else stays local (see
//! `codelake_core::federation_policy`). Each rule names the semantic difference
//! it avoids:
//!
//! | Kept local | Why |
//! |---|---|
//! | comparisons, `GROUP BY`, `MIN/MAX`, `DISTINCT`, joins on text | collations are usually case-insensitive and ignore trailing spaces |
//! | `AVG` | T-SQL `AVG(int)` truncates to `int` |
//! | `SUM` of `int`/`smallint` | T-SQL sums in `int` and overflows at 2^31 |
//! | decimal `/` and `%` | T-SQL and `DataFusion` use different result scales |
//! | `ORDER BY` | SQL Server sorts NULLs first |
//! | binary, boolean and non-ASCII text literals | no faithful T-SQL rendering in the unparser |
//! | temporal literals, except compared with a `date`, `smalldatetime` or `datetime2(0..6)` column | the engine shows `datetime` at the millisecond (ISS-054) and truncates `datetime2(7)` to the microsecond |
//! | functions, `CASE`, `LIKE`, casts other than numeric widening | semantics differ or are unverified |

use std::ops::ControlFlow;
use std::sync::Arc;

use codelake_core::federation_policy::PushdownPolicy;
use codelake_core::functions::{
    EXACT_CAST, VARBINARY_LITERAL, as_binary_literal, as_superset_filter, binary_literal_expr, decode_hex,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ExprSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::{AggregateFunction, Alias};
use datafusion::logical_expr::{Aggregate, BinaryExpr, Expr, ExprSchemable, JoinType, LogicalPlan, Operator};
use datafusion::sql::sqlparser::ast::{
    self, Expr as SqlExpr, Ident, LimitClause, ObjectName, ObjectNamePart, Query, SetExpr, Statement, Top,
    TopQuantity, VisitMut, VisitorMut,
};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{CharacterLengthStyle, Dialect};

/// SQL Server dialect for the `DataFusion` unparser.
#[derive(Debug, Default)]
pub(crate) struct TsqlDialect;

impl Dialect for TsqlDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        // Brackets work regardless of the session's QUOTED_IDENTIFIER setting.
        Some('[')
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        false
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Nvarchar(Some(ast::CharacterLength::Max))
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Nvarchar(Some(ast::CharacterLength::Max))
    }

    fn float64_ast_dtype(&self) -> ast::DataType {
        ast::DataType::Float(ast::ExactNumberInfo::None)
    }

    /// T-SQL `timestamp` is `rowversion`. Timestamp literals are only pushed
    /// against columns `datetime2(7)` converts to exactly (see
    /// `exact_temporal_column`), and hold at most microseconds.
    fn timestamp_cast_dtype(&self, _time_unit: &TimeUnit, _tz: &Option<Arc<str>>) -> ast::DataType {
        ast::DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new("datetime2"))]),
            vec!["7".to_owned()],
        )
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        CharacterLengthStyle::CharacterLength
    }

    fn requires_derived_table_alias(&self) -> bool {
        true
    }

    fn scalar_function_to_sql_overrides(
        &self,
        _unparser: &Unparser<'_>,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<SqlExpr>> {
        if func_name != VARBINARY_LITERAL {
            return Ok(None);
        }
        match args {
            [Expr::Literal(ScalarValue::Utf8(Some(hex)), _)] if decode_hex(hex).is_some() => {
                // T-SQL binary constant: 0x followed by the bytes in hex.
                Ok(Some(SqlExpr::value(ast::Value::Number(
                    format!("0x{hex}"),
                    false,
                ))))
            }
            _ => Err(DataFusionError::Plan(format!(
                "sqlserver: {VARBINARY_LITERAL} needs a hexadecimal literal"
            ))),
        }
    }
}

/// Rewrites generic SQL produced by the unparser into T-SQL.
pub(crate) fn rewrite(mut statement: Statement) -> Result<Statement> {
    let mut visitor = TsqlRewriter;
    match statement.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(statement),
        ControlFlow::Break(err) => Err(err),
    }
}

struct TsqlRewriter;

impl VisitorMut for TsqlRewriter {
    type Break = DataFusionError;

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // The unparser renders a projection without columns (e.g. below
        // COUNT(*)) as `SELECT FROM …`, which T-SQL rejects.
        if let SetExpr::Select(select) = query.body.as_mut()
            && select.projection.is_empty()
        {
            // Derived tables need named columns in T-SQL.
            select.projection.push(ast::SelectItem::ExprWithAlias {
                expr: SqlExpr::value(ast::Value::Number("1".into(), false)),
                alias: Ident::new("codelake_one"),
            });
        }
        let Some(clause) = query.limit_clause.take() else {
            return ControlFlow::Continue(());
        };
        let limit = match clause {
            LimitClause::LimitOffset {
                limit: None,
                offset: None,
                limit_by,
            } if limit_by.is_empty() => return ControlFlow::Continue(()),
            LimitClause::LimitOffset {
                limit: Some(limit),
                offset: None,
                limit_by,
            } if limit_by.is_empty() => limit,
            _ => {
                return ControlFlow::Break(DataFusionError::NotImplemented(
                    "sqlserver: LIMIT with OFFSET or BY cannot be pushed down".into(),
                ));
            }
        };
        let SetExpr::Select(select) = query.body.as_mut() else {
            return ControlFlow::Break(DataFusionError::NotImplemented(
                "sqlserver: LIMIT over a set operation cannot be pushed down".into(),
            ));
        };
        select.top = Some(Top {
            with_ties: false,
            percent: false,
            quantity: Some(TopQuantity::Expr(limit)),
        });
        select.top_before_distinct = false;
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        if let SqlExpr::Function(function) = expr
            && function.name.to_string().eq_ignore_ascii_case("count")
        {
            // COUNT returns int (overflows at 2^31); COUNT_BIG returns bigint,
            // matching the engine's Int64.
            function.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("COUNT_BIG"))]);
        }
        ControlFlow::Continue(())
    }
}

/// Which plan nodes SQL Server may evaluate.
#[derive(Debug, Default)]
pub(crate) struct SqlServerPolicy;

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_)
    )
}

fn is_integer(data_type: &DataType) -> bool {
    data_type.is_integer()
}

fn is_float(data_type: &DataType) -> bool {
    data_type.is_floating()
}

fn type_of(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    expr.get_type(schema).ok()
}

/// SQL Server type of a column (field metadata written by the catalog).
fn sql_type_of(expr: &Expr, schema: &DFSchema) -> Option<String> {
    let Expr::Column(column) = expr else {
        return None;
    };
    let field = schema.field_from_column(column).ok()?;
    field.metadata().get(super::SQL_TYPE).cloned()
}

/// Byte width of a fixed-length binary operand: a `binary(n)` column or a
/// binary literal. `None` for `varbinary` and anything else.
fn binary_width(expr: &Expr, schema: &DFSchema) -> Option<usize> {
    if let Some(hex) = as_binary_literal(expr) {
        return Some(hex.len() / 2);
    }
    if let Expr::Literal(ScalarValue::Binary(Some(bytes)), _) = expr {
        return Some(bytes.len());
    }
    let sql = sql_type_of(expr, schema)?;
    sql.strip_prefix("binary(")?.strip_suffix(')')?.parse().ok()
}

/// Temporal SQL types whose values the engine truncates (100 ns → µs).
fn is_truncated_temporal(sql_type: &str) -> bool {
    sql_type.ends_with("(7)")
}

/// Whether `left <op> right` (comparison, grouping, join key) gives the same
/// answer in SQL Server and in the engine.
fn comparable(left: &Expr, left_schema: &DFSchema, right: &Expr, right_schema: &DFSchema) -> bool {
    let (Some(lt), Some(rt)) = (type_of(left, left_schema), type_of(right, right_schema)) else {
        return false;
    };
    if is_text(&lt) || is_text(&rt) || lt == DataType::Boolean || rt == DataType::Boolean {
        // Collation (case, trailing spaces); T-SQL has no boolean comparisons.
        return false;
    }
    if is_binary(&lt) || is_binary(&rt) {
        // SQL Server pads the shorter operand with zeros: only equal widths agree.
        return matches!(
            (binary_width(left, left_schema), binary_width(right, right_schema)),
            (Some(a), Some(b)) if a == b
        );
    }
    if lt.is_temporal() || rt.is_temporal() {
        let left = widened_column(left, left_schema).unwrap_or(left);
        let right = widened_column(right, right_schema).unwrap_or(right);
        return match (sql_type_of(left, left_schema), sql_type_of(right, right_schema)) {
            // Same SQL type on both sides, and no precision the engine truncates.
            (Some(a), Some(b)) => a == b && !is_truncated_temporal(&a),
            (Some(column), None) => lt == rt && exact_temporal_column(&column) && is_temporal_literal(right),
            (None, Some(column)) => lt == rt && exact_temporal_column(&column) && is_temporal_literal(left),
            (None, None) => false,
        };
    }
    lt.is_numeric() && rt.is_numeric()
}

/// Temporal SQL types the engine reads exactly and SQL Server converts to
/// `datetime2(7)` without loss, so comparing them with a literal rendered as
/// `datetime2(7)` (or `date`) agrees with the engine. Not `datetime` (1/300 s
/// steps shown at the millisecond, ISS-054), `datetime2(7)` (truncated to the
/// microsecond), `time` or `datetimeoffset`.
fn exact_temporal_column(sql_type: &str) -> bool {
    match sql_type {
        "date" | "smalldatetime" => true,
        other => other
            .strip_prefix("datetime2(")
            .and_then(|p| p.strip_suffix(')'))
            .and_then(|p| p.parse::<u8>().ok())
            .is_some_and(|precision| precision <= 6),
    }
}

/// Days of 0001-01-01 and 9999-12-31 from the Unix epoch: the range of
/// `date` and `datetime2`.
const TEMPORAL_DAYS: std::ops::RangeInclusive<i64> = -719_162..=2_932_896;

/// A date or zoneless timestamp literal inside the range of `datetime2`, with
/// at most microseconds (`datetime2(7)` holds 100 ns, the unparser renders
/// `CAST('…' AS datetime2(7))` or `CAST('…' AS DATE)`).
fn is_temporal_literal(expr: &Expr) -> bool {
    const DAY: i64 = 86_400;
    let days = match expr {
        Expr::Literal(ScalarValue::Date32(Some(days)), _) => i64::from(*days),
        Expr::Literal(ScalarValue::TimestampSecond(Some(v), None), _) => v.div_euclid(DAY),
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(v), None), _) => v.div_euclid(DAY * 1_000),
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), None), _) => v.div_euclid(DAY * 1_000_000),
        // Rendered in microseconds by `prepare` (`in_microseconds`).
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), None), _) if v % 1_000 == 0 => {
            v.div_euclid(DAY * 1_000_000_000)
        }
        _ => return false,
    };
    TEMPORAL_DAYS.contains(&days)
}

/// Operand of a comparison: a safe expression, or a temporal literal or a
/// widened timestamp column that [`comparable`] then checks against the other
/// side.
fn comparison_operand(expr: &Expr, schema: &DFSchema) -> bool {
    safe_expr(expr, schema) || is_temporal_literal(expr) || widened_column(expr, schema).is_some()
}

fn unit_rank(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// The column of `EXACT_CAST(CAST(column AS timestamp(finer unit)))` (or of
/// the bare cast): the engine compares a timestamp column with a literal of a
/// finer unit by widening the column, which changes no value. `prepare`
/// sends the column itself (`without_widening`).
fn widened_column<'a>(expr: &'a Expr, schema: &DFSchema) -> Option<&'a Expr> {
    let inner = match expr {
        Expr::ScalarFunction(function) if function.func.name() == EXACT_CAST => {
            match function.args.as_slice() {
                [arg] => arg,
                _ => return None,
            }
        }
        other => other,
    };
    let Expr::Cast(cast) = inner else {
        return None;
    };
    let column = cast.expr.as_ref();
    if !matches!(column, Expr::Column(_)) {
        return None;
    }
    match (type_of(column, schema)?, cast.field.data_type()) {
        (DataType::Timestamp(from, None), DataType::Timestamp(to, None))
            if unit_rank(from) <= unit_rank(*to) =>
        {
            Some(column)
        }
        _ => None,
    }
}

/// Single value usable as a grouping key or `DISTINCT`/`MIN`/`MAX` operand.
fn groupable(data_type: &DataType, sql_type: Option<&str>) -> bool {
    if is_text(data_type) || *data_type == DataType::Boolean {
        return false;
    }
    if is_binary(data_type) {
        return sql_type.is_some_and(|t| t.starts_with("binary("));
    }
    if data_type.is_temporal() {
        return sql_type.is_some_and(|t| !is_truncated_temporal(t));
    }
    true
}

fn safe_literal(value: &ScalarValue) -> bool {
    if value.is_null() {
        return true;
    }
    match value {
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::Float32(_)
        | ScalarValue::Float64(_)
        | ScalarValue::Decimal128(..)
        // Rendered as a T-SQL constant by `prepare` + the dialect override.
        | ScalarValue::Binary(Some(_)) => true,
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            s.is_ascii()
        }
        _ => false,
    }
}

fn integer_bits(data_type: &DataType) -> Option<u32> {
    Some(match data_type {
        DataType::Int8 | DataType::UInt8 => 8,
        DataType::Int16 | DataType::UInt16 => 16,
        DataType::Int32 | DataType::UInt32 => 32,
        DataType::Int64 | DataType::UInt64 => 64,
        _ => return None,
    })
}

/// Only casts that cannot change a value: widening integers, integer to
/// double, integer to a decimal with room for every digit.
fn widening_cast(from: &DataType, to: &DataType) -> bool {
    let Some(bits) = integer_bits(from) else {
        return false;
    };
    let digits: u8 = match bits {
        8 => 3,
        16 => 5,
        32 => 10,
        _ => 19,
    };
    match to {
        DataType::Float64 => bits <= 32,
        DataType::Decimal128(p, s) => i16::from(*p) - i16::from(*s) >= i16::from(digits),
        other => integer_bits(other).is_some_and(|to_bits| to_bits >= bits && other.is_signed_integer()),
    }
}

/// Expression whose value SQL Server computes exactly like the engine.
fn safe_expr(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Literal(value, _) => safe_literal(value),
        Expr::ScalarFunction(_) => as_binary_literal(expr).is_some(),
        Expr::Alias(Alias { expr, .. }) => safe_expr(expr, schema),
        Expr::BinaryExpr(binary) => safe_binary(binary, schema),
        Expr::Not(inner) => safe_predicate(inner, schema),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => safe_expr(inner, schema),
        Expr::Between(between) => {
            [&*between.expr, &*between.low, &*between.high]
                .iter()
                .all(|e| comparison_operand(e, schema))
                && comparable(&between.expr, schema, &between.low, schema)
                && comparable(&between.expr, schema, &between.high, schema)
        }
        Expr::InList(list) => {
            safe_expr(&list.expr, schema)
                && list
                    .list
                    .iter()
                    .all(|e| comparison_operand(e, schema) && comparable(&list.expr, schema, e, schema))
        }
        Expr::Cast(cast) => {
            safe_expr(&cast.expr, schema)
                && type_of(&cast.expr, schema)
                    .is_some_and(|from| widening_cast(&from, cast.field.data_type()))
        }
        _ => false,
    }
}

fn safe_binary(binary: &BinaryExpr, schema: &DFSchema) -> bool {
    let (left, right) = (&*binary.left, &*binary.right);
    let (Some(lt), Some(rt)) = (type_of(left, schema), type_of(right, schema)) else {
        return false;
    };
    let operands = safe_expr(left, schema) && safe_expr(right, schema);
    let both_int = is_integer(&lt) && is_integer(&rt);
    let both_float = is_float(&lt) && is_float(&rt);
    match binary.op {
        Operator::And | Operator::Or => safe_predicate(left, schema) && safe_predicate(right, schema),
        Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            comparison_operand(left, schema)
                && comparison_operand(right, schema)
                && comparable(left, schema, right, schema)
        }
        // Decimal arithmetic stays local: SQL Server reduces the scale when the
        // result precision would pass 38; the engine keeps it.
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide => {
            operands && (both_int || both_float)
        }
        // T-SQL has no modulo for float.
        Operator::Modulo => operands && both_int,
        _ => false,
    }
}

/// Boolean expression usable as `WHERE`/`ON`: T-SQL has no bare boolean values.
fn safe_predicate(expr: &Expr, schema: &DFSchema) -> bool {
    if let Some(predicate) = as_superset_filter(expr) {
        return superset_shape(predicate, schema);
    }
    match expr {
        Expr::BinaryExpr(_)
        | Expr::Not(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::Between(_)
        | Expr::InList(_) => safe_expr(expr, schema),
        _ => false,
    }
}

/// Text a comparison can send as a superset filter: a plain column of a
/// character type T-SQL compares with `=` (not `text`/`ntext`).
fn superset_text_column(expr: &Expr, schema: &DFSchema) -> bool {
    matches!(expr, Expr::Column(_))
        && type_of(expr, schema).is_some_and(|t| is_text(&t))
        && sql_type_of(expr, schema).is_some_and(|t| {
            ["char", "varchar", "nchar", "nvarchar"]
                .iter()
                .any(|base| t == *base || t.starts_with(&format!("{base}(")))
        })
}

fn ascii_text(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _)
            if s.is_ascii()
    )
}

/// A plain temporal column of a type SQL Server compares with a whole-second
/// literal exactly as the engine shows its values.
fn superset_temporal_column(expr: &Expr, schema: &DFSchema) -> bool {
    matches!(expr, Expr::Column(_))
        && matches!(type_of(expr, schema), Some(DataType::Timestamp(_, None)))
        && sql_type_of(expr, schema)
            .is_some_and(|t| t == "datetime" || t == "smalldatetime" || t.starts_with("datetime2"))
}

/// A zoneless timestamp literal at a whole second: every SQL Server temporal
/// type holds it exactly, so a bound there does not depend on the fraction
/// the engine shows (`datetime`'s 1/300 s, ISS-054; `datetime2(7)`'s 100 ns).
fn whole_second_literal(expr: &Expr) -> bool {
    let whole = match expr {
        Expr::Literal(ScalarValue::TimestampSecond(Some(_), None), _) => true,
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(v), None), _) => v % 1_000 == 0,
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), None), _) => v % 1_000_000 == 0,
        _ => false,
    };
    whole && is_temporal_literal(expr)
}

/// A superset filter [`SqlServerPolicy::superset_of`] produces: text equality
/// or `IN` against ASCII literals, and whole-second bounds on a temporal
/// column, combined with `AND`/`OR`. SQL Server keeps at least the rows the
/// engine keeps (its collations ignore case and trailing spaces, and match
/// whenever the bytes match); the engine applies the exact predicate above.
fn superset_shape(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And | Operator::Or,
            right,
        }) => superset_shape(left, schema) && superset_shape(right, schema),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => {
            (superset_text_column(left, schema) && ascii_text(right))
                || (superset_text_column(right, schema) && ascii_text(left))
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq,
            right,
        }) => {
            (superset_temporal_column(left, schema) && whole_second_literal(right))
                || (superset_temporal_column(right, schema) && whole_second_literal(left))
        }
        Expr::InList(list) => {
            !list.negated && superset_text_column(&list.expr, schema) && list.list.iter().all(ascii_text)
        }
        _ => false,
    }
}

/// `date(column)` of a temporal column (Trino's `DATE(x)`).
fn date_of_column<'a>(expr: &'a Expr, schema: &DFSchema) -> Option<&'a Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    match function.args.as_slice() {
        [column] if function.name() == "date" && superset_temporal_column(column, schema) => Some(column),
        _ => None,
    }
}

/// Midnight of `days` (from the Unix epoch) in the unit of `column`.
fn midnight(column: &Expr, schema: &DFSchema, days: i32) -> Option<Expr> {
    const DAY: i64 = 86_400;
    let seconds = i64::from(days) * DAY;
    let value = match type_of(column, schema)? {
        DataType::Timestamp(TimeUnit::Second, None) => ScalarValue::TimestampSecond(Some(seconds), None),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            ScalarValue::TimestampMillisecond(Some(seconds.checked_mul(1_000)?), None)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            ScalarValue::TimestampMicrosecond(Some(seconds.checked_mul(1_000_000)?), None)
        }
        _ => return None,
    };
    let literal = Expr::Literal(value, None);
    is_temporal_literal(&literal).then_some(literal)
}

fn date_literal(expr: &Expr) -> Option<i32> {
    match expr {
        Expr::Literal(ScalarValue::Date32(Some(days)), _) => Some(*days),
        _ => None,
    }
}

/// `date(column) <op> DATE d` as bounds on the column itself: `>= d` is
/// `column >= d 00:00`, `<= d` is `column < (d + 1) 00:00`. Exact, since a
/// timestamp is on day `d` exactly when it lies in `[d 00:00, d+1 00:00)`.
fn date_bounds(column: &Expr, op: Operator, days: i32, schema: &DFSchema) -> Option<Expr> {
    let at = |d: i32| midnight(column, schema, d);
    let next = days.checked_add(1)?;
    Some(match op {
        Operator::GtEq => column.clone().gt_eq(at(days)?),
        Operator::Gt => column.clone().gt_eq(at(next)?),
        Operator::Lt => column.clone().lt(at(days)?),
        Operator::LtEq => column.clone().lt(at(next)?),
        Operator::Eq => column.clone().gt_eq(at(days)?).and(column.clone().lt(at(next)?)),
        _ => return None,
    })
}

fn flipped(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Eq => Operator::Eq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

impl SqlServerPolicy {
    /// See [`PushdownPolicy::superset`].
    fn superset_of(term: &Expr, schema: &DFSchema) -> Option<Expr> {
        match term {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => Some(Self::superset_of(left, schema)?.or(Self::superset_of(right, schema)?)),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => match (Self::superset_of(left, schema), Self::superset_of(right, schema)) {
                (Some(a), Some(b)) => Some(a.and(b)),
                (Some(one), None) | (None, Some(one)) => Some(one),
                (None, None) => None,
            },
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if let (Some(column), Some(days)) = (date_of_column(left, schema), date_literal(right)) {
                    return date_bounds(column, *op, days, schema);
                }
                if let (Some(days), Some(column)) = (date_literal(left), date_of_column(right, schema)) {
                    return date_bounds(column, flipped(*op)?, days, schema);
                }
                superset_shape(term, schema).then(|| term.clone())
            }
            Expr::Between(between) if !between.negated => {
                let column = date_of_column(&between.expr, schema)?;
                let (low, high) = (date_literal(&between.low)?, date_literal(&between.high)?);
                Some(date_bounds(column, Operator::GtEq, low, schema)?.and(date_bounds(
                    column,
                    Operator::LtEq,
                    high,
                    schema,
                )?))
            }
            Expr::InList(_) => superset_shape(term, schema).then(|| term.clone()),
            _ => None,
        }
    }
}

/// A projected value T-SQL can return: boolean expressions only as plain
/// `bit` columns.
fn safe_projection(expr: &Expr, schema: &DFSchema) -> bool {
    let inner = match expr {
        Expr::Alias(Alias { expr, .. }) => expr.as_ref(),
        other => other,
    };
    let boolean = type_of(inner, schema).is_some_and(|t| t == DataType::Boolean);
    safe_expr(inner, schema) && !has_binary_literal(inner) && (!boolean || matches!(inner, Expr::Column(_)))
}

fn groupable_expr(expr: &Expr, schema: &DFSchema) -> bool {
    type_of(expr, schema).is_some_and(|t| groupable(&t, sql_type_of(expr, schema).as_deref()))
}

fn safe_aggregate(aggregate: &Aggregate) -> bool {
    let schema = aggregate.input.schema();
    let groups_ok = aggregate
        .group_expr
        .iter()
        .all(|e| safe_expr(e, schema) && !has_binary_literal(e) && groupable_expr(e, schema));
    groups_ok
        && aggregate.aggr_expr.iter().all(|expr| {
            let expr = match expr {
                Expr::Alias(Alias { expr, .. }) => expr.as_ref(),
                other => other,
            };
            let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
                return false;
            };
            if params.filter.is_some() || !params.order_by.is_empty() {
                return false;
            }
            if !params
                .args
                .iter()
                .all(|a| safe_expr(a, schema) && !has_binary_literal(a))
            {
                return false;
            }
            let first = params.args.first();
            let arg_type = first.and_then(|a| type_of(a, schema));
            match func.name() {
                // NULLs are not counted on either side; DISTINCT compares values.
                "count" => !params.distinct || params.args.iter().all(|a| groupable_expr(a, schema)),
                "min" | "max" => first.is_some_and(|a| groupable_expr(a, schema)),
                "sum" => {
                    !params.distinct
                        && arg_type.is_some_and(|t| {
                            matches!(t, DataType::Int64 | DataType::Decimal128(..)) || is_float(&t)
                        })
                }
                _ => false,
            }
        })
}

impl PushdownPolicy for SqlServerPolicy {
    fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::TableScan(scan) => scan.filters.is_empty() && scan.fetch.is_none(),
            LogicalPlan::Projection(p) => p.expr.iter().all(|e| safe_projection(e, p.input.schema())),
            LogicalPlan::Filter(f) => safe_predicate(&f.predicate, f.input.schema()),
            LogicalPlan::SubqueryAlias(_) | LogicalPlan::Union(_) => true,
            LogicalPlan::Join(join) => {
                let schema = join.schema.as_ref();
                let left = join.left.schema();
                let right = join.right.schema();
                matches!(
                    join.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) && join
                    .on
                    .iter()
                    .all(|(l, r)| safe_expr(l, left) && safe_expr(r, right) && comparable(l, left, r, right))
                    && join.filter.as_ref().is_none_or(|f| safe_predicate(f, schema))
            }
            // TOP cannot apply to a set operation.
            LogicalPlan::Limit(limit) => {
                limit.skip.is_none()
                    && !matches!(limit.input.as_ref(), LogicalPlan::Union(_))
                    && matches!(limit.fetch.as_deref(), Some(Expr::Literal(ScalarValue::Int64(Some(n)), _)) if *n > 0)
            }
            LogicalPlan::Aggregate(aggregate) => safe_aggregate(aggregate),
            LogicalPlan::Distinct(_) => plan.schema().fields().iter().all(|f| {
                groupable(
                    f.data_type(),
                    f.metadata().get(super::SQL_TYPE).map(String::as_str),
                )
            }),
            _ => false,
        }
    }

    fn superset(&self, term: &Expr, schema: &DFSchema) -> Option<Expr> {
        Self::superset_of(term, schema)
    }

    /// Adapts a subtree about to be unparsed:
    /// - a projection of plain columns directly below an aggregation is
    ///   dropped: the unparser would turn it into a derived table and then
    ///   reference the base table's qualifier from outside it, which T-SQL
    ///   rejects;
    /// - the optimizer folds `codelake_varbinary('..')` into a binary literal
    ///   the unparser cannot render; predicate literals become the function again.
    fn prepare(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // List columns explicitly: `SELECT *` would also return columns the
        // catalog does not expose (unsupported types).
        let plan = plan
            .transform_up(|node| {
                let LogicalPlan::TableScan(scan) = &node else {
                    return Ok(Transformed::no(node));
                };
                if scan.projection.is_some() {
                    return Ok(Transformed::no(node));
                }
                let mut scan = scan.clone();
                scan.projection = Some((0..scan.source.schema().fields().len()).collect());
                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            })
            .data()?;
        let plan = plan
            .transform_up(|node| {
                let LogicalPlan::Aggregate(aggregate) = &node else {
                    return Ok(Transformed::no(node));
                };
                let LogicalPlan::Projection(projection) = aggregate.input.as_ref() else {
                    return Ok(Transformed::no(node));
                };
                if !projection.expr.iter().all(|e| matches!(e, Expr::Column(_))) {
                    return Ok(Transformed::no(node));
                }
                match Aggregate::try_new(
                    Arc::clone(&projection.input),
                    aggregate.group_expr.clone(),
                    aggregate.aggr_expr.clone(),
                ) {
                    Ok(flat) => Ok(Transformed::yes(LogicalPlan::Aggregate(flat))),
                    Err(_) => Ok(Transformed::no(node)),
                }
            })
            .data()?;
        plan.transform_up(|node| {
            if !matches!(node, LogicalPlan::Filter(_) | LogicalPlan::Join(_)) {
                return Ok(Transformed::no(node));
            }
            // Columns of a filter's or a join's predicate resolve in its schema.
            let schema = Arc::clone(node.schema());
            node.map_expressions(|expr| {
                // Top-down: `EXACT_CAST(CAST(c))` is replaced whole, before its cast.
                expr.transform_down(|e| match &e {
                    Expr::Literal(ScalarValue::Binary(Some(bytes)), _) => {
                        Ok(Transformed::yes(binary_literal_expr(bytes)))
                    }
                    Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), None), metadata)
                        if v % 1_000 == 0 =>
                    {
                        // `datetime2(7)` takes at most 7 fractional digits.
                        Ok(Transformed::yes(Expr::Literal(
                            ScalarValue::TimestampMicrosecond(Some(v / 1_000), None),
                            metadata.clone(),
                        )))
                    }
                    _ => match widened_column(&e, &schema) {
                        Some(column) => Ok(Transformed::yes(column.clone())),
                        None => Ok(Transformed::no(e)),
                    },
                })
            })
        })
        .data()
    }
}

/// Binary literals are only rendered inside predicates (see `prepare`).
fn has_binary_literal(expr: &Expr) -> bool {
    let mut found = false;
    let _ = expr.apply(|e| {
        if matches!(e, Expr::Literal(ScalarValue::Binary(Some(_)), _)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Shared handle used by the executor.
pub(crate) fn policy() -> Arc<dyn PushdownPolicy> {
    Arc::new(SqlServerPolicy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::MsSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn rewritten(sql: &str) -> Result<String> {
        let statement = Parser::parse_sql(&datafusion::sql::sqlparser::dialect::GenericDialect {}, sql)
            .map_err(|e| DataFusionError::Plan(e.to_string()))?
            .remove(0);
        let out = rewrite(statement)?.to_string();
        // The result must be valid T-SQL syntax.
        Parser::parse_sql(&MsSqlDialect {}, &out)
            .map_err(|e| DataFusionError::Plan(format!("{out}: {e}")))?;
        Ok(out)
    }

    #[test]
    fn limit_becomes_top_including_in_subqueries() {
        let out = rewritten("SELECT a FROM (SELECT a FROM t LIMIT 5) AS s LIMIT 10").unwrap();
        assert_eq!(out, "SELECT TOP (10) a FROM (SELECT TOP (5) a FROM t) AS s");
    }

    #[test]
    fn limit_with_offset_is_refused() {
        let err = rewritten("SELECT a FROM t LIMIT 5 OFFSET 2").unwrap_err();
        assert!(err.to_string().contains("OFFSET"), "{err}");
        assert!(rewritten("SELECT a FROM t UNION ALL SELECT a FROM u LIMIT 1").is_err());
    }

    #[test]
    fn empty_select_list_becomes_select_one() {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::functions_aggregate::expr_fn::count;
        use datafusion::logical_expr::{col, lit, table_scan};
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let plan = table_scan(Some("t"), &schema, None)
            .unwrap()
            .filter(col("id").gt(lit(1_i64)))
            .unwrap()
            .project(Vec::<Expr>::new())
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();
        let statement = Unparser::new(&TsqlDialect).plan_to_sql(&plan).unwrap();
        let sql = rewrite(statement).unwrap().to_string();
        assert_eq!(
            sql,
            "SELECT COUNT_BIG(1) FROM (SELECT 1 AS codelake_one FROM [t] WHERE ([t].[id] > 1)) AS [derived_projection]"
        );
        Parser::parse_sql(&MsSqlDialect {}, &sql).unwrap();
    }

    #[test]
    fn prepare_flattens_column_projections_below_aggregates() {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::functions_aggregate::expr_fn::{count, sum};
        use datafusion::logical_expr::{col, lit, table_scan};
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("n", DataType::Int64, false),
        ]);
        let base = || {
            table_scan(Some("Grandes"), &schema, None)
                .unwrap()
                .filter(col("id").gt(lit(1_i64)))
                .unwrap()
        };
        let summed = base()
            .project(vec![col("n")])
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![sum(col("n"))])
            .unwrap()
            .build()
            .unwrap();
        let counted = base()
            .project(Vec::<Expr>::new())
            .unwrap()
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();
        let render = |plan: LogicalPlan| {
            let prepared = SqlServerPolicy.prepare(plan).unwrap();
            let statement = Unparser::new(&TsqlDialect).plan_to_sql(&prepared).unwrap();
            let sql = rewrite(statement).unwrap().to_string();
            Parser::parse_sql(&MsSqlDialect {}, &sql).unwrap();
            sql
        };
        assert_eq!(
            render(summed),
            "SELECT sum([grandes].[n]) FROM [grandes] WHERE ([grandes].[id] > 1)"
        );
        assert_eq!(
            render(counted),
            "SELECT COUNT_BIG(1) FROM [grandes] WHERE ([grandes].[id] > 1)"
        );
    }

    #[test]
    fn empty_limit_clause_is_dropped() {
        let statement = Parser::parse_sql(
            &datafusion::sql::sqlparser::dialect::GenericDialect {},
            "SELECT a FROM t",
        )
        .unwrap()
        .remove(0);
        let mut statement = statement;
        if let Statement::Query(query) = &mut statement {
            query.limit_clause = Some(LimitClause::LimitOffset {
                limit: None,
                offset: None,
                limit_by: vec![],
            });
        }
        assert_eq!(rewrite(statement).unwrap().to_string(), "SELECT a FROM t");
    }

    #[test]
    fn count_becomes_count_big() {
        let out = rewritten("SELECT count(*), COUNT(a) FROM t").unwrap();
        assert_eq!(out, "SELECT COUNT_BIG(*), COUNT_BIG(a) FROM t");
        assert_eq!(rewritten("SELECT sum(a) FROM t").unwrap(), "SELECT sum(a) FROM t");
    }

    #[test]
    fn binary_literal_renders_as_tsql_constant() {
        use codelake_core::functions::binary_literal_expr;
        use datafusion::logical_expr::col;
        let unparser = Unparser::new(&TsqlDialect);
        let expr = col("lsn").gt(binary_literal_expr(&[0, 0, 0, 42]));
        let sql = unparser.expr_to_sql(&expr).unwrap().to_string();
        assert_eq!(sql, "([lsn] > 0x0000002a)");
        let bad =
            codelake_core::functions::varbinary_literal().call(vec![datafusion::logical_expr::lit("zz")]);
        assert!(unparser.expr_to_sql(&bad).is_err());
    }

    #[test]
    fn dialect_quotes_with_brackets_and_casts_to_nvarchar() {
        let dialect = TsqlDialect;
        assert_eq!(dialect.identifier_quote_style("__$start_lsn"), Some('['));
        assert_eq!(dialect.utf8_cast_dtype().to_string(), "NVARCHAR(MAX)");
        assert_eq!(dialect.large_utf8_cast_dtype().to_string(), "NVARCHAR(MAX)");
        assert!(!dialect.supports_nulls_first_in_sort());
        assert!(dialect.requires_derived_table_alias());
        assert_eq!(dialect.float64_ast_dtype().to_string(), "FLOAT");
    }

    mod policy_rules {
        use super::*;
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
        use datafusion::logical_expr::{LogicalPlanBuilder, col, lit, table_scan};

        fn typed(name: &str, data_type: DataType, sql: &str) -> Field {
            Field::new(name, data_type, true).with_metadata(std::collections::HashMap::from([(
                super::super::super::SQL_TYPE.to_owned(),
                sql.to_owned(),
            )]))
        }

        fn scan() -> LogicalPlanBuilder {
            use datafusion::arrow::datatypes::TimeUnit;
            let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("small", DataType::Int32, true),
                Field::new("amount", DataType::Decimal128(18, 2), true),
                typed("valor", DataType::Decimal128(19, 4), "money"),
                Field::new("ratio", DataType::Float64, true),
                Field::new("name", DataType::Utf8, true),
                typed("lsn", DataType::Binary, "binary(10)"),
                typed("vb", DataType::Binary, "varbinary(16)"),
                typed("flag", DataType::Boolean, "bit"),
                typed("dt", ts.clone(), "datetime"),
                typed("dt_b", ts.clone(), "datetime"),
                typed("dt2", ts.clone(), "datetime2(7)"),
                typed("dt2_b", ts, "datetime2(7)"),
                typed(
                    "loaded",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    "datetime2(3)",
                ),
                typed(
                    "dt_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    "datetime",
                ),
                typed(
                    "minute",
                    DataType::Timestamp(TimeUnit::Second, None),
                    "smalldatetime",
                ),
                typed("day", DataType::Date32, "date"),
                typed("code", DataType::Utf8, "varchar(5)"),
                typed("memo", DataType::Utf8, "text"),
            ]);
            table_scan(Some("t"), &schema, None).unwrap()
        }

        fn pushable(plan: LogicalPlanBuilder) -> bool {
            SqlServerPolicy.node_is_pushable(&plan.build().unwrap())
        }

        #[test]
        fn plain_scans_and_numeric_filters_are_pushed() {
            assert!(pushable(scan()));
            assert!(pushable(scan().filter(col("id").gt(lit(10_i64))).unwrap()));
            assert!(pushable(
                scan()
                    .filter(col("id").gt(lit(1_i64)).and(col("ratio").lt_eq(lit(0.5))))
                    .unwrap()
            ));
            assert!(pushable(scan().filter(col("name").is_null()).unwrap()));
            assert!(pushable(
                scan()
                    .filter(col("id").in_list(vec![lit(1_i64), lit(2_i64)], false))
                    .unwrap()
            ));
            assert!(pushable(
                scan().filter(col("id").between(lit(1_i64), lit(9_i64))).unwrap()
            ));
        }

        #[test]
        fn text_comparisons_stay_local() {
            assert!(!pushable(scan().filter(col("name").eq(lit("Ana"))).unwrap()));
            assert!(!pushable(
                scan().filter(col("name").in_list(vec![lit("a")], false)).unwrap()
            ));
            assert!(!pushable(
                scan().filter(col("name").between(lit("a"), lit("b"))).unwrap()
            ));
            assert!(!pushable(scan().filter(col("name").like(lit("A%"))).unwrap()));
        }

        #[test]
        fn folded_binary_literals_are_pushed_in_predicates_only() {
            let folded = lit(ScalarValue::Binary(Some(vec![0; 10])));
            assert!(pushable(scan().filter(col("lsn").gt(folded.clone())).unwrap()));
            assert!(!pushable(scan().project(vec![folded.alias("b")]).unwrap()));
        }

        #[test]
        fn prepare_restores_renderable_binary_literals() {
            let folded = lit(ScalarValue::Binary(Some(vec![0, 42])));
            let plan = scan().filter(col("lsn").gt(folded)).unwrap().build().unwrap();
            let prepared = SqlServerPolicy.prepare(plan).unwrap();
            let sql = datafusion::sql::unparser::Unparser::new(&TsqlDialect)
                .plan_to_sql(&prepared)
                .unwrap()
                .to_string();
            assert!(sql.contains("[lsn] > 0x002a"), "{sql}");
        }

        #[test]
        fn binary_watermark_literal_is_pushed() {
            let watermark = codelake_core::functions::binary_literal_expr(&[0; 10]);
            assert!(pushable(scan().filter(col("lsn").gt(watermark)).unwrap()));
        }

        #[test]
        fn unfaithful_literals_stay_local() {
            let fixed = lit(ScalarValue::FixedSizeBinary(1, Some(vec![0])));
            assert!(!pushable(scan().filter(col("lsn").gt(fixed)).unwrap()));
            assert!(!pushable(scan().filter(col("flag").eq(lit(true))).unwrap()));
            assert!(
                !pushable(scan().filter(col("flag")).unwrap()),
                "bare boolean is not T-SQL"
            );
            assert!(!pushable(scan().project(vec![lit("ação").alias("x")]).unwrap()));
            assert!(pushable(scan().project(vec![lit("acao").alias("x")]).unwrap()));
        }

        #[test]
        fn arithmetic_rules() {
            assert!(pushable(
                scan()
                    .project(vec![(col("id") + col("small")).alias("x")])
                    .unwrap()
            ));
            assert!(pushable(
                scan().project(vec![(col("id") / lit(2_i64)).alias("x")]).unwrap()
            ));
            assert!(pushable(
                scan()
                    .project(vec![(col("ratio") / lit(2.0)).alias("x")])
                    .unwrap()
            ));
            assert!(!pushable(
                scan()
                    .project(vec![(col("amount") / lit(2_i64)).alias("x")])
                    .unwrap()
            ));
            assert!(!pushable(
                scan()
                    .project(vec![(col("amount") % col("amount")).alias("x")])
                    .unwrap()
            ));
        }

        #[test]
        fn aggregate_rules() {
            let agg = |group: Vec<Expr>, aggr: Vec<Expr>| pushable(scan().aggregate(group, aggr).unwrap());
            assert!(agg(
                vec![col("id")],
                vec![count(col("id")), sum(col("amount")), min(col("ratio"))]
            ));
            assert!(agg(vec![], vec![sum(col("id")), max(col("lsn"))]));
            assert!(!agg(vec![col("name")], vec![count(col("id"))]), "text group key");
            assert!(!agg(vec![], vec![avg(col("id"))]), "AVG truncates in T-SQL");
            assert!(
                !agg(vec![], vec![sum(col("small"))]),
                "int SUM overflows in T-SQL"
            );
            assert!(!agg(vec![], vec![max(col("name"))]), "text MAX follows collation");
        }

        #[test]
        fn joins_and_other_nodes() {
            let left = scan();
            let right = scan().alias("u").unwrap().build().unwrap();
            let numeric_join = left
                .clone()
                .join(right.clone(), JoinType::Inner, (vec!["t.id"], vec!["u.id"]), None)
                .unwrap();
            assert!(pushable(numeric_join));
            let text_join = left
                .clone()
                .join(
                    right.clone(),
                    JoinType::Inner,
                    (vec!["t.name"], vec!["u.name"]),
                    None,
                )
                .unwrap();
            assert!(!pushable(text_join));
            let semi = left
                .join(right, JoinType::LeftSemi, (vec!["t.id"], vec!["u.id"]), None)
                .unwrap();
            assert!(!pushable(semi));

            assert!(
                !pushable(scan().sort(vec![col("id").sort(true, false)]).unwrap()),
                "NULL order differs"
            );
            assert!(pushable(scan().limit(0, Some(5)).unwrap()));
            assert!(!pushable(scan().limit(2, Some(5)).unwrap()));
            assert!(pushable(
                scan().project(vec![col("id")]).unwrap().distinct().unwrap()
            ));
            assert!(!pushable(
                scan().project(vec![col("name")]).unwrap().distinct().unwrap()
            ));
            assert!(pushable(
                scan()
                    .project(vec![col("id")])
                    .unwrap()
                    .union(scan().project(vec![col("id")]).unwrap().build().unwrap())
                    .unwrap()
            ));
        }

        // Findings of the independent F1 audit: each must stay local.
        #[test]
        fn audit_money_and_decimal_arithmetic_stay_local() {
            assert!(!pushable(
                scan()
                    .project(vec![(col("valor") / lit(3_i64)).alias("x")])
                    .unwrap()
            ));
            assert!(!pushable(
                scan()
                    .project(vec![(col("valor") * col("valor")).alias("x")])
                    .unwrap()
            ));
            assert!(!pushable(
                scan()
                    .project(vec![(col("amount") + col("amount")).alias("x")])
                    .unwrap()
            ));
            assert!(pushable(
                scan()
                    .filter(col("valor").gt(lit(ScalarValue::Decimal128(Some(10_000), 19, 4))))
                    .unwrap()
            ));
        }

        #[test]
        fn audit_variable_width_binary_stays_local() {
            let short = lit(ScalarValue::Binary(Some(vec![1])));
            assert!(
                !pushable(scan().filter(col("vb").eq(short.clone())).unwrap()),
                "0x01 = 0x0100 in SQL Server"
            );
            assert!(
                !pushable(scan().filter(col("lsn").eq(short)).unwrap()),
                "width 1 vs binary(10)"
            );
            assert!(!pushable(
                scan().aggregate(vec![col("vb")], Vec::<Expr>::new()).unwrap()
            ));
            assert!(pushable(
                scan().aggregate(vec![col("lsn")], Vec::<Expr>::new()).unwrap()
            ));
        }

        #[test]
        fn audit_temporal_comparisons() {
            assert!(
                pushable(scan().filter(col("dt").lt(col("dt_b"))).unwrap()),
                "same SQL type"
            );
            assert!(
                !pushable(scan().filter(col("dt").lt(col("dt2"))).unwrap()),
                "datetime vs datetime2"
            );
            assert!(
                !pushable(scan().filter(col("dt2").lt(col("dt2_b"))).unwrap()),
                "100 ns precision"
            );
            assert!(!pushable(
                scan()
                    .aggregate(Vec::<Expr>::new(), vec![max(col("dt2"))])
                    .unwrap()
            ));
        }

        fn ms(value: i64) -> Expr {
            lit(ScalarValue::TimestampMillisecond(Some(value), None))
        }

        #[test]
        fn temporal_literals_are_pushed_only_against_exact_columns() {
            // 2026-09-28 10:36:03.654, a sink watermark folded from localtimestamp.
            let watermark = ms(1_790_591_763_654);
            assert!(
                pushable(scan().filter(col("loaded").gt(watermark.clone())).unwrap()),
                "datetime2(3)"
            );
            assert!(pushable(
                scan()
                    .filter(col("loaded").between(ms(0), watermark.clone()))
                    .unwrap()
            ));
            assert!(pushable(
                scan().filter(col("loaded").in_list(vec![ms(0)], false)).unwrap()
            ));
            assert!(
                pushable(
                    scan()
                        .filter(col("day").eq(lit(ScalarValue::Date32(Some(20_000)))))
                        .unwrap()
                ),
                "date"
            );
            assert!(
                pushable(
                    scan()
                        .filter(col("minute").lt(lit(ScalarValue::TimestampSecond(Some(60), None))))
                        .unwrap()
                ),
                "smalldatetime"
            );
            assert!(
                !pushable(scan().filter(col("dt_ms").gt(watermark.clone())).unwrap()),
                "datetime: 1/300 s steps shown at the millisecond (ISS-054)"
            );
            assert!(
                !pushable(
                    scan()
                        .filter(col("dt2").gt(lit(ScalarValue::TimestampMicrosecond(Some(0), None))))
                        .unwrap()
                ),
                "datetime2(7) is truncated by the engine"
            );
            assert!(
                !pushable(
                    scan()
                        .filter(col("loaded").gt(lit(ScalarValue::TimestampMillisecond(
                            Some(0),
                            Some("UTC".into())
                        ))))
                        .unwrap()
                ),
                "zoned literal"
            );
            assert!(
                !pushable(scan().filter(col("loaded").gt(ms(i64::MAX / 2))).unwrap()),
                "outside the range of datetime2"
            );
            assert!(
                !pushable(scan().project(vec![watermark.alias("x")]).unwrap()),
                "temporal literals are compared at the source, never returned by it"
            );
        }

        #[test]
        fn a_column_widened_to_the_literal_unit_is_compared_at_the_source() {
            use datafusion::logical_expr::cast;
            let ns = |v: i64| lit(ScalarValue::TimestampNanosecond(Some(v), None));
            let widened = || cast(col("loaded"), DataType::Timestamp(TimeUnit::Nanosecond, None));
            // TIMESTAMP '2026-09-28 10:36:03.654100': microseconds, exact in datetime2(7).
            let filter = widened().gt(ns(1_790_591_763_654_100_000));
            assert!(pushable(scan().filter(filter.clone()).unwrap()));
            assert!(
                !pushable(
                    scan()
                        .filter(widened().gt(ns(1_790_591_763_654_100_001)))
                        .unwrap()
                ),
                "a nanosecond has no datetime2(7) rendering"
            );
            let plan = scan().filter(filter).unwrap().build().unwrap();
            let prepared = SqlServerPolicy.prepare(plan).unwrap();
            let statement = Unparser::new(&TsqlDialect).plan_to_sql(&prepared).unwrap();
            let sql = rewrite(statement).unwrap().to_string();
            assert!(
                sql.contains("([t].[loaded] > CAST('2026-09-28 10:36:03.654100' AS datetime2(7)))"),
                "{sql}"
            );
        }

        #[test]
        fn temporal_literals_render_as_exact_tsql_constants() {
            use datafusion::sql::sqlparser::dialect::MsSqlDialect;
            use datafusion::sql::sqlparser::parser::Parser;
            let plan = scan()
                .filter(
                    col("loaded")
                        .gt(ms(1_790_591_763_654))
                        .and(col("day").lt_eq(lit(ScalarValue::Date32(Some(20_000))))),
                )
                .unwrap()
                .build()
                .unwrap();
            let prepared = SqlServerPolicy.prepare(plan).unwrap();
            let statement = Unparser::new(&TsqlDialect).plan_to_sql(&prepared).unwrap();
            let sql = rewrite(statement).unwrap().to_string();
            assert!(
                sql.contains("([t].[loaded] > CAST('2026-09-28 10:36:03.654' AS datetime2(7)))"),
                "{sql}"
            );
            assert!(sql.contains("([t].[day] <= CAST('2024-10-04' AS DATE))"), "{sql}");
            Parser::parse_sql(&MsSqlDialect {}, &sql).unwrap();
        }

        fn date_of(column: Expr) -> Expr {
            use datafusion::logical_expr::{ColumnarValue, create_udf};
            let udf = create_udf(
                "date",
                vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
                DataType::Date32,
                datafusion::logical_expr::Volatility::Immutable,
                Arc::new(|_: &[ColumnarValue]| unreachable!("planned only")),
            );
            udf.call(vec![column])
        }

        fn superset(term: &Expr) -> Option<Expr> {
            let plan = scan().build().unwrap();
            SqlServerPolicy.superset(term, plan.schema())
        }

        #[test]
        fn text_equality_is_sent_as_a_superset_the_engine_narrows() {
            let term = col("code").in_list(vec![lit("GBVRF"), lit("GBVRI")], false);
            assert_eq!(
                superset(&term),
                Some(term.clone()),
                "the collation keeps at least these rows"
            );
            let either = col("code").eq(lit("A")).or(col("code").eq(lit("B")));
            assert_eq!(superset(&either), Some(either));
            assert!(
                !pushable(scan().filter(term.clone()).unwrap()),
                "never as the exact filter"
            );
            assert!(pushable(
                scan()
                    .filter(codelake_core::functions::superset_filter(term))
                    .unwrap()
            ));
            assert_eq!(superset(&col("code").eq(lit("ação"))), None, "non-ASCII literal");
            assert_eq!(superset(&col("memo").eq(lit("a"))), None, "text/ntext have no =");
            assert_eq!(
                superset(&col("code").not_eq(lit("A"))),
                None,
                "<> is not a superset"
            );
            assert_eq!(superset(&col("code").like(lit("A%"))), None);
        }

        #[test]
        fn a_date_of_a_timestamp_becomes_midnight_bounds() {
            // 2026-09-01 is day 20697 from the epoch.
            let day = |d: i32| lit(ScalarValue::Date32(Some(d)));
            let midnight = |d: i64| ms(d * 86_400_000);
            assert_eq!(
                superset(&date_of(col("dt_ms")).gt_eq(day(20_697))),
                Some(col("dt_ms").gt_eq(midnight(20_697))),
                "datetime: midnight is exact in 1/300 s steps"
            );
            assert_eq!(
                superset(&date_of(col("dt_ms")).lt_eq(day(20_726))),
                Some(col("dt_ms").lt(midnight(20_727)))
            );
            assert_eq!(
                superset(&day(20_697).lt_eq(date_of(col("loaded")))),
                Some(col("loaded").gt_eq(midnight(20_697))),
                "literal on the left"
            );
            assert_eq!(
                superset(&date_of(col("loaded")).between(day(20_697), day(20_726))),
                Some(
                    col("loaded")
                        .gt_eq(midnight(20_697))
                        .and(col("loaded").lt(midnight(20_727)))
                )
            );
            assert_eq!(superset(&date_of(col("dt_ms")).not_eq(day(20_697))), None);
            let bound = col("dt_ms").gt_eq(midnight(20_697));
            assert!(pushable(
                scan()
                    .filter(codelake_core::functions::superset_filter(bound))
                    .unwrap()
            ));
            let fraction = col("dt_ms").gt_eq(ms(20_697 * 86_400_000 + 1));
            assert!(
                !pushable(
                    scan()
                        .filter(codelake_core::functions::superset_filter(fraction))
                        .unwrap()
                ),
                "a bound inside a second depends on datetime's rounding"
            );
        }

        #[test]
        fn audit_tsql_rejects_stay_local() {
            assert!(!pushable(
                scan()
                    .project(vec![col("id").gt(lit(1_i64)).alias("alto")])
                    .unwrap()
            ));
            assert!(
                pushable(scan().project(vec![col("flag")]).unwrap()),
                "plain bit column"
            );
            assert!(!pushable(
                scan()
                    .aggregate(Vec::<Expr>::new(), vec![max(col("flag"))])
                    .unwrap()
            ));
            assert!(!pushable(
                scan()
                    .project(vec![(col("ratio") % lit(2.0)).alias("x")])
                    .unwrap()
            ));
            let union = scan()
                .project(vec![col("id")])
                .unwrap()
                .union(scan().project(vec![col("id")]).unwrap().build().unwrap())
                .unwrap()
                .limit(0, Some(1))
                .unwrap();
            assert!(!pushable(union), "TOP cannot apply to UNION");
        }

        #[test]
        fn casts_only_widen_integers() {
            use datafusion::logical_expr::cast;
            assert!(pushable(
                scan()
                    .project(vec![cast(col("small"), DataType::Int64).alias("x")])
                    .unwrap()
            ));
            assert!(!pushable(
                scan()
                    .project(vec![cast(col("ratio"), DataType::Int64).alias("x")])
                    .unwrap()
            ));
            assert!(!pushable(
                scan()
                    .project(vec![cast(col("id"), DataType::Utf8).alias("x")])
                    .unwrap()
            ));
        }
    }
}
