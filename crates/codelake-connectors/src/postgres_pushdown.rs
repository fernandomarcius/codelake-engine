//! The PostgreSQL pushdown policy.
//!
//! The policy only lets PostgreSQL evaluate operators whose result is identical
//! to the engine's. Everything else stays local (see
//! `codelake_core::federation_policy`). Each rule names the semantic difference
//! it avoids:
//!
//! | Kept local | Why |
//! |---|---|
//! | functions, `CASE`, `LIKE` | the engine's own functions (`count_if`, `arrays_overlap`, subscripts) do not exist in PostgreSQL or differ in NULL handling and errors; `LIKE` escapes with `\` by default |
//! | `ORDER BY`, `<`, `>`, `BETWEEN`, `MIN/MAX` on text | PostgreSQL orders text by the column's collation (`pt-BR-x-icu`, `en_US.utf8`), the engine by code point |
//! | comparisons, grouping, ordering of `real`/`double precision` | PostgreSQL treats `NaN` as equal to itself and greater than any number, and `-0` as `0` |
//! | `AVG`, `SUM` of `bigint` or floats, aggregates with `FILTER` or `ORDER BY` | `numeric` results (`AVG(int)`, `SUM(bigint)` never overflows), order-dependent float sums |
//! | arithmetic other than on integers, casts other than widening | decimal result scales differ; float overflow is an error |
//! | timestamp literals finer than the microsecond, temporal literals out of years 1–9999 | PostgreSQL rounds to the microsecond; no faithful rendering |
//!
//! A timestamp column the engine widens to a finer unit to compare it with a
//! literal (`codelake_exact_cast(CAST(c AS timestamp(9)))`, which changes no
//! value) is compared at the source as the column itself.
//!
//! Text equality, `IN`, `GROUP BY`, `DISTINCT` and join keys on text are
//! pushed: under a deterministic collation (every collation PostgreSQL ships)
//! two strings are equal only when their bytes are.
//!
//! A list of values is sent as a list: the engine's simplifier turns `x IN
//! (a, b, c)` into `x = a OR x = b OR x = c` (and `NOT IN` into `<>` terms
//! joined by `AND`), which SQL defines as the same predicate, but PostgreSQL
//! estimates the two spellings differently. Over a view, the `OR` chain could
//! pick a plan forty times slower than `x = ANY(…)` for the same rows; the
//! source gets the list back ([`in_lists`]).
//!
//! A `timestamp with time zone` literal is sent as `CAST('…+00' AS TIMESTAMP
//! WITH TIME ZONE)`, an instant: the unparser's `CAST('…' AS TIMESTAMP)` drops
//! the offset, and PostgreSQL then reads it in the session's `TimeZone`.
//!
//! The statement runs in the engine session's time zone
//! ([`codelake_core::federation_guard::remote_time_zone`], read when the
//! remote node starts; see `postgres::pool`).

use std::sync::Arc;

use async_trait::async_trait;
use codelake_core::federation_policy::{PushdownPolicy, restrict_federation};
use codelake_core::functions::EXACT_CAST;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::temporal_conversions::timestamp_us_to_datetime;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, NullEquality, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::expr::{AggregateFunction, Alias};
use datafusion::logical_expr::utils::{conjunction, disjunction, split_binary_owned};
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Cast, Distinct, Expr, ExprSchemable, JoinType, LogicalPlan, Operator,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::{Dialect, PostgreSqlDialect};
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_federation::sql::{
    LogicalOptimizer, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::GenericError;
use datafusion_table_providers_common::sql::sql_provider_datafusion::{SqlTable, to_execution_error};
use futures::TryStreamExt;
use futures::future::BoxFuture;

/// Which plan nodes PostgreSQL may evaluate.
#[derive(Debug, Default)]
pub(crate) struct PostgresPolicy;

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_exact_number(data_type: &DataType) -> bool {
    data_type.is_integer() || matches!(data_type, DataType::Decimal128(..))
}

fn type_of(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    expr.get_type(schema).ok()
}

/// Values PostgreSQL orders like the engine: numbers without floats, dates,
/// timestamps, booleans (`false` first).
fn orderable(data_type: &DataType) -> bool {
    is_exact_number(data_type)
        || matches!(
            data_type,
            DataType::Date32 | DataType::Timestamp(..) | DataType::Boolean
        )
}

/// Values PostgreSQL groups like the engine: the orderable ones and text.
fn groupable(data_type: &DataType) -> bool {
    orderable(data_type) || is_text(data_type)
}

/// Whether `left <op> right` gives the same answer in PostgreSQL and in the
/// engine. Text only for (in)equality; temporal values only against the same
/// kind (a date, a timestamp with or without a zone): mixing them converts
/// through the session's `TimeZone`.
fn comparable(op: Operator, left: &DataType, right: &DataType) -> bool {
    if is_text(left) || is_text(right) {
        return is_text(left) && is_text(right) && matches!(op, Operator::Eq | Operator::NotEq);
    }
    match (left, right) {
        (DataType::Date32, DataType::Date32) | (DataType::Boolean, DataType::Boolean) => true,
        (DataType::Timestamp(_, a), DataType::Timestamp(_, b)) => a.is_some() == b.is_some(),
        _ => is_exact_number(left) && is_exact_number(right),
    }
}

/// Days of 0001-01-01 and 9999-12-31 from the Unix epoch.
const TEMPORAL_DAYS: std::ops::RangeInclusive<i64> = -719_162..=2_932_896;

/// Microseconds of a timestamp literal, when it holds no finer fraction.
fn literal_micros(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampSecond(Some(v), _) => v.checked_mul(1_000_000),
        ScalarValue::TimestampMillisecond(Some(v), _) => v.checked_mul(1_000),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampNanosecond(Some(v), _) if v % 1_000 == 0 => Some(v / 1_000),
        _ => None,
    }
}

/// A date or timestamp literal PostgreSQL reads exactly as the engine holds it.
fn temporal_literal(expr: &Expr) -> bool {
    const DAY_US: i64 = 86_400_000_000;
    match expr {
        Expr::Literal(ScalarValue::Date32(Some(days)), _) => TEMPORAL_DAYS.contains(&i64::from(*days)),
        Expr::Literal(value, _) => {
            literal_micros(value).is_some_and(|us| TEMPORAL_DAYS.contains(&us.div_euclid(DAY_US)))
        }
        _ => false,
    }
}

fn safe_literal(value: &ScalarValue) -> bool {
    if value.is_null() {
        return true;
    }
    match value {
        ScalarValue::Boolean(_)
        | ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::Decimal128(..) => true,
        // PostgreSQL text holds no NUL; a backslash is an escape when
        // `standard_conforming_strings` is off.
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            !s.contains(['\0', '\\'])
        }
        _ => false,
    }
}

/// Only casts that cannot change a value: widening integers, integer to a
/// decimal with room for every digit.
fn widening_cast(from: &DataType, to: &DataType) -> bool {
    let digits: i16 = match from {
        DataType::Int8 => 3,
        DataType::Int16 => 5,
        DataType::Int32 => 10,
        DataType::Int64 => 19,
        _ => return false,
    };
    match to {
        DataType::Int16 | DataType::Int32 | DataType::Int64 => to.primitive_width() >= from.primitive_width(),
        DataType::Decimal128(p, s) => i16::from(*p) - i16::from(*s) >= digits,
        _ => false,
    }
}

/// Expression whose value PostgreSQL computes exactly like the engine.
fn safe_expr(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Literal(value, _) => safe_literal(value),
        Expr::Alias(Alias { expr, .. }) => safe_expr(expr, schema),
        Expr::BinaryExpr(binary) => safe_binary(binary, schema),
        Expr::Not(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner) => safe_predicate(inner, schema),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => safe_expr(inner, schema),
        Expr::Negative(inner) => {
            safe_expr(inner, schema) && type_of(inner, schema).is_some_and(|t| t.is_integer())
        }
        Expr::Between(between) => {
            safe_comparison(&between.expr, Operator::Lt, &between.low, schema)
                && safe_comparison(&between.expr, Operator::Lt, &between.high, schema)
        }
        Expr::InList(list) => {
            safe_expr(&list.expr, schema)
                && list
                    .list
                    .iter()
                    .all(|e| safe_comparison(&list.expr, Operator::Eq, e, schema))
        }
        Expr::Cast(cast) => {
            safe_expr(&cast.expr, schema)
                && type_of(&cast.expr, schema)
                    .is_some_and(|from| widening_cast(&from, cast.field.data_type()))
        }
        _ => false,
    }
}

fn unit_rank(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// The column of `codelake_exact_cast(CAST(column AS <finer timestamp>))`,
/// with or without a zone as the column: the engine widens a timestamp
/// column to compare it with a finer literal, which changes no value.
/// [`PostgresPolicy::prepare`] sends the column itself.
fn widened_column<'a>(expr: &'a Expr, schema: &DFSchema) -> Option<&'a Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    let ([Expr::Cast(cast)], true) = (function.args.as_slice(), function.func.name() == EXACT_CAST) else {
        return None;
    };
    let column = cast.expr.as_ref();
    if !matches!(column, Expr::Column(_)) {
        return None;
    }
    match (type_of(column, schema)?, cast.field.data_type()) {
        (DataType::Timestamp(from, from_zone), DataType::Timestamp(to, to_zone))
            if unit_rank(from) <= unit_rank(*to) && from_zone.is_some() == to_zone.is_some() =>
        {
            Some(column)
        }
        _ => None,
    }
}

/// `left <op> right`: both operands safe (or a temporal literal, rendered
/// by [`PostgresPolicy::prepare`], or a widened timestamp column) and of
/// types PostgreSQL compares alike.
fn safe_comparison(left: &Expr, op: Operator, right: &Expr, schema: &DFSchema) -> bool {
    let operand =
        |e: &Expr| safe_expr(e, schema) || temporal_literal(e) || widened_column(e, schema).is_some();
    let (Some(lt), Some(rt)) = (type_of(left, schema), type_of(right, schema)) else {
        return false;
    };
    operand(left) && operand(right) && comparable(op, &lt, &rt)
}

fn safe_binary(binary: &BinaryExpr, schema: &DFSchema) -> bool {
    let (left, right) = (&*binary.left, &*binary.right);
    match binary.op {
        Operator::And | Operator::Or => safe_predicate(left, schema) && safe_predicate(right, schema),
        Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            safe_comparison(left, binary.op, right, schema)
        }
        // Integer arithmetic truncates and fails on overflow and on a zero
        // divisor on both sides.
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo => {
            safe_expr(left, schema)
                && safe_expr(right, schema)
                && type_of(left, schema).is_some_and(|t| t.is_integer())
                && type_of(right, schema).is_some_and(|t| t.is_integer())
        }
        _ => false,
    }
}

/// A boolean expression usable as `WHERE`/`ON`.
fn safe_predicate(expr: &Expr, schema: &DFSchema) -> bool {
    safe_expr(expr, schema) && type_of(expr, schema).is_some_and(|t| t == DataType::Boolean)
}

fn safe_aggregate(aggregate: &Aggregate) -> bool {
    let schema = aggregate.input.schema();
    let groups_ok = aggregate
        .group_expr
        .iter()
        .all(|e| safe_expr(e, schema) && type_of(e, schema).is_some_and(|t| groupable(&t)));
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
            if !params.args.iter().all(|a| safe_expr(a, schema)) {
                return false;
            }
            let arg_type = params.args.first().and_then(|a| type_of(a, schema));
            match func.name() {
                // NULLs are not counted on either side; DISTINCT compares values.
                "count" => {
                    !params.distinct
                        || params
                            .args
                            .iter()
                            .all(|a| type_of(a, schema).is_some_and(|t| groupable(&t)))
                }
                // The engine sums integers as `bigint`: PostgreSQL's `sum` of a
                // `smallint`/`integer` is `bigint` too (`prepare` drops the
                // widening), of a `bigint` is `numeric`. `numeric` sums are exact.
                "sum" => {
                    !params.distinct
                        && (summed_integer(params.args.first(), schema).is_some()
                            || matches!(arg_type, Some(DataType::Decimal128(..))))
                }
                "min" | "max" => arg_type.is_some_and(|t| orderable(&t) && t != DataType::Boolean),
                "bool_and" | "bool_or" => arg_type == Some(DataType::Boolean),
                _ => false,
            }
        })
}

/// `x` of a `CAST(x AS bigint)` sum argument, when `x` is a `smallint` or an
/// `integer`.
fn summed_integer<'a>(arg: Option<&'a Expr>, schema: &DFSchema) -> Option<&'a Expr> {
    let Some(Expr::Cast(cast)) = arg else {
        return None;
    };
    let narrow = type_of(&cast.expr, schema).is_some_and(|t| matches!(t, DataType::Int16 | DataType::Int32));
    (narrow && *cast.field.data_type() == DataType::Int64 && safe_expr(&cast.expr, schema))
        .then_some(&*cast.expr)
}

/// `sum(CAST(x AS bigint))` as `sum(x)`, for [`summed_integer`] arguments:
/// PostgreSQL then answers `bigint`, the engine's type, not `numeric`. Only
/// where the aggregate keeps its name (the planner names a coerced sum after
/// its uncoerced argument), which the plan above refers to.
fn narrow_sums(node: LogicalPlan) -> Transformed<LogicalPlan> {
    let LogicalPlan::Aggregate(aggregate) = &node else {
        return Transformed::no(node);
    };
    let schema = aggregate.input.schema();
    let mut changed = false;
    let aggr_expr = aggregate
        .aggr_expr
        .iter()
        .map(|expr| {
            let inner = match expr {
                Expr::Alias(Alias { expr, .. }) => expr.as_ref(),
                other => other,
            };
            let Expr::AggregateFunction(AggregateFunction { func, params }) = inner else {
                return expr.clone();
            };
            let Some(narrow) = (func.name() == "sum")
                .then(|| summed_integer(params.args.first(), schema))
                .flatten()
            else {
                return expr.clone();
            };
            let mut params = params.clone();
            params.args = vec![narrow.clone()];
            let summed = Expr::AggregateFunction(AggregateFunction {
                func: Arc::clone(func),
                params,
            });
            if summed.schema_name().to_string() != expr.schema_name().to_string() {
                return expr.clone();
            }
            changed = true;
            summed
        })
        .collect();
    if !changed {
        return Transformed::no(node);
    }
    match Aggregate::try_new(
        Arc::clone(&aggregate.input),
        aggregate.group_expr.clone(),
        aggr_expr,
    ) {
        Ok(narrowed) if narrowed.schema.fields() == aggregate.schema.fields() => {
            Transformed::yes(LogicalPlan::Aggregate(narrowed))
        }
        _ => Transformed::no(node),
    }
}

fn literal_count(expr: Option<&Expr>) -> bool {
    expr.is_none_or(|e| matches!(e, Expr::Literal(ScalarValue::Int64(Some(n)), _) if *n >= 0))
}

impl PushdownPolicy for PostgresPolicy {
    fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::TableScan(scan) => scan.filters.is_empty(),
            LogicalPlan::Projection(p) => p.expr.iter().all(|e| safe_expr(e, p.input.schema())),
            LogicalPlan::Filter(f) => safe_predicate(&f.predicate, f.input.schema()),
            LogicalPlan::SubqueryAlias(_) | LogicalPlan::Union(_) => true,
            LogicalPlan::Join(join) => {
                let left = join.left.schema();
                let right = join.right.schema();
                matches!(
                    join.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) && join.null_equality == NullEquality::NullEqualsNothing
                    && join.on.iter().all(|(l, r)| {
                        safe_expr(l, left)
                            && safe_expr(r, right)
                            && matches!(
                                (type_of(l, left), type_of(r, right)),
                                (Some(a), Some(b)) if comparable(Operator::Eq, &a, &b)
                            )
                    })
                    && join
                        .filter
                        .as_ref()
                        .is_none_or(|f| safe_predicate(f, join.schema.as_ref()))
            }
            LogicalPlan::Sort(sort) => {
                let schema = sort.input.schema();
                sort.expr.iter().all(|s| {
                    safe_expr(&s.expr, schema) && type_of(&s.expr, schema).is_some_and(|t| orderable(&t))
                })
            }
            LogicalPlan::Limit(limit) => {
                literal_count(limit.skip.as_deref()) && literal_count(limit.fetch.as_deref())
            }
            LogicalPlan::Aggregate(aggregate) => safe_aggregate(aggregate),
            LogicalPlan::Distinct(Distinct::All(_)) => {
                plan.schema().fields().iter().all(|f| groupable(f.data_type()))
            }
            _ => false,
        }
    }

    /// Adapts a subtree about to be unparsed:
    /// - a scan lists its columns (never `SELECT *`);
    /// - a projection of plain columns directly below an aggregation is
    ///   dropped: the unparser would turn it into a derived table and then
    ///   reference the base table's qualifier from outside it (`SELECT DISTINCT
    ///   c` became "missing FROM-clause entry");
    /// - `sum(CAST(x AS bigint))` of a narrower integer becomes `sum(x)`;
    /// - a `timestamp with time zone` literal becomes a cast of its UTC text;
    /// - a timestamp column widened to a finer unit becomes the column;
    /// - equalities of one expression to literals joined by `OR` become an
    ///   `IN` list, inequalities joined by `AND` a `NOT IN` list.
    fn prepare(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
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
        let plan = plan.transform_up(|node| Ok(narrow_sums(node))).data()?;
        plan.transform_up(|node| {
            if !matches!(node, LogicalPlan::Filter(_) | LogicalPlan::Join(_)) {
                return Ok(Transformed::no(node));
            }
            // Columns of a filter's or a join's predicate resolve in its schema.
            let schema = Arc::clone(node.schema());
            node.map_expressions(|expr| {
                let listed = in_lists(expr)?;
                listed
                    .data
                    .transform_down(|e| match widened_column(&e, &schema) {
                        Some(column) => Ok(Transformed::yes(column.clone())),
                        None => Ok(instant_literal(e)),
                    })
                    .map(|done| {
                        Transformed::new_transformed(done.data, done.transformed || listed.transformed)
                    })
            })
        })
        .data()
    }
}

/// `x = a OR x = b OR …` as `x IN (a, b, …)` and `x <> a AND x <> b AND …` as
/// `x NOT IN (a, b, …)`, wherever they appear in `expr`: the spelling a
/// client's list had before the engine's simplifier split it.
///
/// SQL defines `x IN (a, b)` as `x = a OR x = b` and `x NOT IN (a, b)` as `x
/// <> a AND x <> b`, NULLs included, and `AND`/`OR` of comparisons without
/// side effects do not depend on the order of their terms: the rows are the
/// same. Only non-NULL literals of one type join a list, so PostgreSQL reads
/// each of them as it reads the single comparison (an untyped `'…'` takes the
/// type of `x` in both). Terms on other expressions stay as they are.
fn in_lists(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_up(|e| match &e {
        Expr::BinaryExpr(BinaryExpr {
            op: op @ (Operator::Or | Operator::And),
            ..
        }) => {
            let op = *op;
            Ok(listed(e, op))
        }
        _ => Ok(Transformed::no(e)),
    })
}

/// The expression and the literals of `term` when it is one of the
/// comparisons a list joined by `op` is made of: `x = a` (or an `IN` list)
/// under `OR`, `x <> a` (or a `NOT IN` list) under `AND`.
fn list_member(term: &Expr, op: Operator) -> Option<(&Expr, Vec<&Expr>)> {
    let value = |e: &Expr| matches!(e, Expr::Literal(v, _) if !v.is_null());
    let negated = op == Operator::And;
    let member = match term {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: compared,
            right,
        }) if *compared == if negated { Operator::NotEq } else { Operator::Eq } => {
            match (value(left), value(right)) {
                (false, true) => Some((left.as_ref(), vec![right.as_ref()])),
                (true, false) => Some((right.as_ref(), vec![left.as_ref()])),
                _ => None,
            }
        }
        Expr::InList(list) if list.negated == negated && list.list.iter().all(value) => {
            Some((list.expr.as_ref(), list.list.iter().collect()))
        }
        _ => None,
    };
    // A volatile expression is evaluated once per comparison; one list
    // would evaluate it once.
    member.filter(|(x, _)| !x.is_volatile())
}

fn literal_type(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Literal(value, _) => Some(value.data_type()),
        _ => None,
    }
}

/// `expr`, a chain of `op`, with the members of each list (see
/// [`list_member`]) on one expression gathered into a single list where the
/// first of them stood.
fn listed(expr: Expr, op: Operator) -> Transformed<Expr> {
    let terms = split_binary_owned(expr.clone(), op);
    // For each term, the index of the first term on the same expression whose
    // literals have the same type, when it is a list member.
    let mut heads: Vec<Option<usize>> = Vec::with_capacity(terms.len());
    for (i, term) in terms.iter().enumerate() {
        let head = list_member(term, op).and_then(|(x, values)| {
            let kind = literal_type(values[0])?;
            if values.iter().any(|v| literal_type(v).as_ref() != Some(&kind)) {
                return None;
            }
            let first = terms[..i].iter().position(|other| {
                list_member(other, op).is_some_and(|(y, others)| {
                    y == x && others.iter().all(|v| literal_type(v).as_ref() == Some(&kind))
                })
            });
            Some(first.unwrap_or(i))
        });
        heads.push(head);
    }
    let gathers = |i: usize| heads.iter().filter(|h| **h == Some(i)).count() > 1;
    if !(0..terms.len()).any(gathers) {
        return Transformed::no(expr);
    }
    let mut out = Vec::with_capacity(terms.len());
    for (i, term) in terms.iter().enumerate() {
        match heads[i] {
            Some(head) if head == i && gathers(i) => {
                let Some((x, _)) = list_member(term, op) else {
                    return Transformed::no(expr);
                };
                let values = terms
                    .iter()
                    .zip(&heads)
                    .filter(|(_, h)| **h == Some(i))
                    .filter_map(|(t, _)| list_member(t, op).map(|(_, v)| v))
                    .flatten()
                    .cloned()
                    .collect();
                out.push(Expr::InList(InList::new(
                    Box::new(x.clone()),
                    values,
                    op == Operator::And,
                )));
            }
            Some(head) if head != i => {}
            _ => out.push(term.clone()),
        }
    }
    let joined = if op == Operator::Or {
        disjunction(out)
    } else {
        conjunction(out)
    };
    match joined {
        Some(joined) => Transformed::yes(joined),
        None => Transformed::no(expr),
    }
}

/// `TIMESTAMP '…' WITH TIME ZONE` literal as `CAST('<UTC text>+00' AS
/// TIMESTAMP WITH TIME ZONE)`; any other expression unchanged.
fn instant_literal(expr: Expr) -> Transformed<Expr> {
    let Expr::Literal(value, _) = &expr else {
        return Transformed::no(expr);
    };
    let data_type = value.data_type();
    let DataType::Timestamp(_, Some(_)) = data_type else {
        return Transformed::no(expr);
    };
    let Some(utc) = literal_micros(value).and_then(timestamp_us_to_datetime) else {
        return Transformed::no(expr);
    };
    let text = Expr::Literal(ScalarValue::Utf8(Some(format!("{utc}+00"))), None);
    Transformed::yes(Expr::Cast(Cast::new(Box::new(text), data_type)))
}

/// Shared handle used by the executor.
pub(crate) fn policy() -> Arc<dyn PushdownPolicy> {
    Arc::new(PostgresPolicy)
}

/// Where a PostgreSQL table's statements run: a statement with the schema
/// of its answer, in the time zone of the engine session it answers
/// (`None`: the engine's, UTC).
pub(crate) trait ZonedSource: Send + Sync {
    fn query(
        &self,
        time_zone: Option<String>,
        sql: String,
        schema: SchemaRef,
    ) -> BoxFuture<'static, std::result::Result<SendableRecordBatchStream, GenericError>>;
}

/// The executor of a PostgreSQL table, with [`PostgresPolicy`] restricting
/// what federation sends to it; its statements run at `source`.
pub(crate) struct PostgresExecutor<E> {
    inner: Arc<E>,
    source: Arc<dyn ZonedSource>,
}

impl<E> std::fmt::Debug for PostgresExecutor<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresExecutor").finish_non_exhaustive()
    }
}

#[async_trait]
impl<E: SQLExecutor + 'static> SQLExecutor for PostgresExecutor<E> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn compute_context(&self) -> Option<String> {
        self.inner.compute_context()
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.inner.dialect()
    }

    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        let policy = policy();
        Some(Box::new(move |plan: LogicalPlan| {
            restrict_federation(plan, policy.as_ref())
        }))
    }

    /// Runs `query` in the zone of the session whose remote node is being
    /// started (the dynamic `filters` are ignored, as upstream).
    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        let answer = self.source.query(
            codelake_core::federation_guard::remote_time_zone(),
            query.to_owned(),
            Arc::clone(&schema),
        );
        let stream =
            futures::stream::once(async move { answer.await.map_err(to_execution_error) }).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        self.inner.table_names().await
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        self.inner.get_table_schema(table_name).await
    }
}

/// The remote table `reference` as a federated provider whose sub-plans go
/// through [`PostgresPolicy`] and run at `source`.
pub(crate) async fn table_provider<T: 'static, P: 'static>(
    pool: &Arc<dyn DbConnectionPool<T, P> + Send + Sync>,
    source: Arc<dyn ZonedSource>,
    reference: TableReference,
) -> Result<Arc<dyn TableProvider>> {
    let table = SqlTable::new("postgres", pool, reference.clone())
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .with_dialect(Arc::new(PostgreSqlDialect {}));
    let schema = table.schema();
    let executor = Arc::new(PostgresExecutor {
        inner: Arc::new(table),
        source,
    });
    let source = Arc::new(SQLTableSource::new_with_schema(
        Arc::new(SQLFederationProvider::new(executor)),
        RemoteTableRef::from(reference),
        schema,
    ));
    // No fallback provider: filters then stay `Filter` nodes the policy
    // splits term by term, instead of filters embedded in the scan (which
    // the policy can only keep or lift whole).
    Ok(Arc::new(FederatedTableProviderAdaptor::new(source)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
    use datafusion::functions_aggregate::expr_fn::{count, count_distinct, max, min, sum};
    use datafusion::logical_expr::{LogicalPlanBuilder, cast, col, lit, table_scan};
    use datafusion::sql::unparser::Unparser;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("big", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
            Field::new("n", DataType::Decimal128(12, 2), true),
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "tz",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("b", DataType::Boolean, true),
        ])
    }

    fn base() -> LogicalPlanBuilder {
        table_scan(Some("t"), &schema(), None).unwrap()
    }

    fn pushable(plan: &LogicalPlan) -> bool {
        PostgresPolicy.node_is_pushable(plan)
    }

    fn filter(predicate: Expr) -> LogicalPlan {
        base().filter(predicate).unwrap().build().unwrap()
    }

    fn render(plan: LogicalPlan) -> String {
        let prepared = PostgresPolicy.prepare(plan).unwrap();
        Unparser::new(&PostgreSqlDialect {})
            .plan_to_sql(&prepared)
            .unwrap()
            .to_string()
    }

    fn tz_literal(us: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(
            Some(us * 1_000),
            Some("+00:00".into()),
        ))
    }

    #[test]
    fn text_is_compared_for_equality_only() {
        assert!(pushable(&filter(col("s").eq(lit("a")))));
        assert!(pushable(&filter(col("s").not_eq(lit("a")))));
        assert!(pushable(&filter(
            col("s").in_list(vec![lit("a"), lit("b")], false)
        )));
        assert!(!pushable(&filter(col("s").lt(lit("a")))));
        assert!(!pushable(&filter(col("s").gt_eq(lit("a")))));
        assert!(!pushable(&filter(col("s").between(lit("a"), lit("b")))));
        assert!(!pushable(&filter(col("s").like(lit("a%")))));
    }

    #[test]
    fn literals_postgresql_reads_differently_stay_local() {
        assert!(!pushable(&filter(col("s").eq(lit("a\\b")))));
        assert!(!pushable(&filter(col("s").eq(lit("a\0")))));
        assert!(pushable(&filter(col("s").eq(lit("ação 'x'")))));
        assert!(!pushable(&filter(col("f").eq(lit(1.5_f64)))));
        assert!(!pushable(&filter(col("f").gt(lit(0.0_f64)))));
    }

    #[test]
    fn numbers_dates_and_timestamps_compare_against_their_own_kind() {
        assert!(pushable(&filter(col("id").gt(lit(3_i32)))));
        assert!(pushable(&filter(col("n").lt_eq(lit(ScalarValue::Decimal128(
            Some(150),
            12,
            2
        ))))));
        assert!(pushable(&filter(
            col("d").gt_eq(lit(ScalarValue::Date32(Some(20_000))))
        )));
        assert!(pushable(&filter(
            col("tz").gt_eq(tz_literal(1_757_808_000_000_000))
        )));
        assert!(pushable(&filter(col("b").eq(lit(true)))));
        let naive = lit(ScalarValue::TimestampNanosecond(Some(1_000), None));
        assert!(pushable(&filter(col("ts").lt(naive.clone()))));
        // A zoneless literal against a zoned column (and the reverse) goes
        // through the session's TimeZone in PostgreSQL.
        assert!(!comparable(
            Operator::Lt,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        ));
        assert!(!comparable(
            Operator::Eq,
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        ));
        // Finer than the microsecond: PostgreSQL rounds it.
        let nanos = lit(ScalarValue::TimestampNanosecond(Some(1_001), None));
        assert!(!pushable(&filter(col("ts").lt(nanos))));
        // Past year 9999.
        let far = lit(ScalarValue::Date32(Some(3_000_000)));
        assert!(!pushable(&filter(col("d").lt(far))));
    }

    #[test]
    fn functions_stay_local() {
        let lower = datafusion::functions::string::expr_fn::lower(col("s"));
        assert!(!pushable(&filter(lower.eq(lit("a")))));
        let projected = base()
            .project(vec![col("id"), col("id") + lit(1_i32)])
            .unwrap()
            .build()
            .unwrap();
        assert!(pushable(&projected));
        let decimal = base()
            .project(vec![col("n") + col("n")])
            .unwrap()
            .build()
            .unwrap();
        assert!(!pushable(&decimal));
        let float = base()
            .project(vec![col("f") * col("f")])
            .unwrap()
            .build()
            .unwrap();
        assert!(!pushable(&float));
    }

    #[test]
    fn aggregates_postgresql_computes_alike() {
        let agg = |aggs: Vec<Expr>| base().aggregate(vec![col("s")], aggs).unwrap().build().unwrap();
        // As planned: integers are summed as `bigint`.
        let widened = |c: &str| sum(cast(col(c), DataType::Int64));
        assert!(pushable(&agg(vec![count(lit(1)), widened("id"), sum(col("n"))])));
        assert!(!pushable(&agg(vec![widened("big")])));
        assert!(!pushable(&agg(vec![sum(col("id"))])));
        assert!(pushable(&agg(vec![
            count_distinct(col("s")),
            min(col("d")),
            max(col("tz"))
        ])));
        assert!(!pushable(&agg(vec![sum(col("big"))])));
        assert!(!pushable(&agg(vec![sum(col("f"))])));
        assert!(!pushable(&agg(vec![min(col("s"))])));
        assert!(!pushable(&agg(vec![max(col("b"))])));
        assert!(!pushable(&agg(vec![count_distinct(col("f"))])));
        let by_float = base()
            .aggregate(vec![col("f")], vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();
        assert!(!pushable(&by_float));
        let avg = datafusion::functions_aggregate::expr_fn::avg(col("id"));
        assert!(!pushable(&agg(vec![avg])));
    }

    #[test]
    fn text_and_floats_are_not_sorted_remotely() {
        let sorted = |e: Expr| base().sort(vec![e.sort(true, false)]).unwrap().build().unwrap();
        assert!(pushable(&sorted(col("d"))));
        assert!(pushable(&sorted(col("id"))));
        assert!(!pushable(&sorted(col("s"))));
        assert!(!pushable(&sorted(col("f"))));
        let distinct_text = base()
            .project(vec![col("s")])
            .unwrap()
            .distinct()
            .unwrap()
            .build()
            .unwrap();
        assert!(pushable(&distinct_text));
        let distinct_float = base()
            .project(vec![col("f")])
            .unwrap()
            .distinct()
            .unwrap()
            .build()
            .unwrap();
        assert!(!pushable(&distinct_float));
    }

    #[test]
    fn limits_with_offsets_are_pushed() {
        let limited = base().limit(50, Some(51)).unwrap().build().unwrap();
        assert!(pushable(&limited));
    }

    #[test]
    fn distinct_over_a_column_renders_without_a_derived_table() {
        let plan = base()
            .project(vec![col("s")])
            .unwrap()
            .aggregate(vec![col("s")], Vec::<Expr>::new())
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(render(plan), r#"SELECT "t"."s" FROM "t" GROUP BY "t"."s""#);
    }

    #[test]
    fn integer_sums_are_sent_without_their_widening() {
        let plan = base()
            .aggregate(
                vec![col("d")],
                vec![
                    // Named as the planner names a coerced sum.
                    sum(cast(col("id"), DataType::Int64)).alias("sum(t.id)"),
                    sum(col("n")),
                ],
            )
            .unwrap()
            .build()
            .unwrap();
        let schema = Arc::clone(plan.schema());
        let prepared = PostgresPolicy.prepare(plan).unwrap();
        assert_eq!(prepared.schema(), &schema);
        let sql = Unparser::new(&PostgreSqlDialect {})
            .plan_to_sql(&prepared)
            .unwrap()
            .to_string();
        assert!(sql.contains(r#"sum("t"."id")"#), "{sql}");
        assert!(sql.contains(r#"sum("t"."n")"#), "{sql}");
        assert!(!sql.contains("BIGINT"), "{sql}");
    }

    #[test]
    fn scans_list_their_columns() {
        let sql = render(base().build().unwrap());
        assert!(sql.starts_with(r#"SELECT "t"."id", "t"."big", "t"."s""#), "{sql}");
    }

    #[test]
    fn zoned_timestamp_literals_are_instants() {
        // 2025-09-14 00:00:00 UTC.
        let sql = render(filter(col("tz").gt_eq(tz_literal(1_757_808_000_000_000))));
        assert!(
            sql.ends_with(
                r#"WHERE ("t"."tz" >= CAST('2025-09-14 00:00:00+00' AS TIMESTAMP WITH TIME ZONE))"#
            ),
            "{sql}"
        );
        let sql = render(filter(col("tz").lt(tz_literal(1_757_808_000_123_456))));
        assert!(sql.contains("'2025-09-14 00:00:00.123456+00'"), "{sql}");
        // Zoneless literals keep the unparser's rendering.
        let naive = lit(ScalarValue::TimestampNanosecond(
            Some(1_757_808_000_000_000_000),
            None,
        ));
        let sql = render(filter(col("ts").gt_eq(naive)));
        assert!(sql.contains("CAST('2025-09-14 00:00:00' AS TIMESTAMP)"), "{sql}");
    }

    /// The `OR`/`AND` chains the simplifier makes of `IN`/`NOT IN` reach
    /// PostgreSQL as lists again; anything else in the chain stays a term.
    #[test]
    fn value_lists_split_by_the_simplifier_are_sent_as_lists() {
        let eq = |c: &str, v: Expr| col(c).eq(v);
        let sql = render(filter(
            eq("s", lit("a")).or(eq("s", lit("b"))).or(eq("s", lit("c"))),
        ));
        assert!(sql.ends_with(r#"WHERE "t"."s" IN ('a', 'b', 'c')"#), "{sql}");

        let sql = render(filter(
            col("s")
                .not_eq(lit("a"))
                .and(col("id").eq(lit(2)))
                .and(col("s").not_eq(lit("b"))),
        ));
        assert!(sql.contains(r#""t"."s" NOT IN ('a', 'b')"#), "{sql}");
        assert!(sql.contains(r#"("t"."id" = 2)"#), "{sql}");

        // Other columns, literals on the left and lists already written join.
        let sql = render(filter(
            eq("s", lit("a"))
                .or(eq("id", lit(3)))
                .or(lit("b").eq(col("s")))
                .or(col("s").in_list(vec![lit("c"), lit("d"), lit("e"), lit("f")], false)),
        ));
        assert!(
            sql.contains(r#""t"."s" IN ('a', 'b', 'c', 'd', 'e', 'f')"#),
            "{sql}"
        );
        assert!(sql.contains(r#"("t"."id" = 3)"#), "{sql}");

        // Inside a conjunction, as the planner leaves it.
        let sql = render(filter(
            col("id")
                .eq(lit(2))
                .and(eq("s", lit("a")).or(eq("s", lit("b"))))
                .and(col("tz").gt_eq(tz_literal(1_757_808_000_000_000))),
        ));
        assert!(sql.contains(r#""t"."s" IN ('a', 'b')"#), "{sql}");
        assert!(
            sql.contains("CAST('2025-09-14 00:00:00+00' AS TIMESTAMP WITH TIME ZONE)"),
            "{sql}"
        );

        // Zoned instants are listed as instants.
        let sql = render(filter(
            col("tz")
                .eq(tz_literal(1_757_808_000_000_000))
                .or(col("tz").eq(tz_literal(1_757_811_600_000_000))),
        ));
        assert!(
            sql.contains(
                r#""t"."tz" IN (CAST('2025-09-14 00:00:00+00' AS TIMESTAMP WITH TIME ZONE), CAST('2025-09-14 01:00:00+00' AS TIMESTAMP WITH TIME ZONE))"#
            ),
            "{sql}"
        );

        // Left alone: one term, NULL literals, mixed literal types, `=` under
        // `AND`, `<>` under `OR`, comparisons of two columns.
        let unchanged = [
            eq("s", lit("a")),
            eq("s", lit(ScalarValue::Utf8(None))).or(eq("s", lit("a"))),
            eq("n", lit(ScalarValue::Decimal128(Some(150), 12, 2)))
                .or(eq("n", lit(ScalarValue::Decimal128(Some(1), 12, 0)))),
            eq("s", lit("a")).and(eq("s", lit("b"))),
            col("s").not_eq(lit("a")).or(col("s").not_eq(lit("b"))),
            eq("s", col("s")).or(eq("s", col("s"))),
        ];
        for predicate in unchanged {
            let sql = render(filter(predicate.clone()));
            assert!(!sql.contains(" IN ("), "{predicate}: {sql}");
        }
        // A volatile expression is evaluated per comparison: never one list.
        let random = datafusion::functions::math::expr_fn::random;
        let volatile = random().eq(lit(1.0)).or(random().eq(lit(2.0)));
        assert!(!in_lists(volatile).unwrap().transformed);
    }

    /// A timestamp column the engine widens (`codelake_exact_cast`) to meet
    /// a finer literal is compared at the source as itself; a narrowing, a
    /// change of zone kind or a literal finer than the microsecond stays local.
    #[test]
    fn timestamp_columns_widened_for_a_literal_are_compared_as_themselves() {
        use datafusion::logical_expr::expr::ScalarFunction;
        use datafusion::logical_expr::{Volatility, create_udf};
        let micros = Schema::new(vec![
            Field::new("us", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "utz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let filtered = |predicate: Expr| {
            table_scan(Some("t"), &micros, None)
                .unwrap()
                .filter(predicate)
                .unwrap()
                .build()
                .unwrap()
        };
        let widened = |column: &str, to: DataType| {
            let identity = create_udf(
                EXACT_CAST,
                vec![to.clone()],
                to.clone(),
                Volatility::Immutable,
                Arc::new(|args: &[datafusion::logical_expr::ColumnarValue]| Ok(args[0].clone())),
            );
            Expr::ScalarFunction(ScalarFunction::new_udf(
                Arc::new(identity),
                vec![cast(col(column), to)],
            ))
        };
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let ns_utc = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        let naive = |v: i64| lit(ScalarValue::TimestampNanosecond(Some(v), None));
        let zoned = |v: i64| lit(ScalarValue::TimestampNanosecond(Some(v), Some("UTC".into())));

        let plan = filtered(widened("us", ns.clone()).gt_eq(naive(1_757_808_000_000_000_000)));
        assert!(pushable(&plan));
        let sql = render(plan);
        assert!(
            sql.ends_with(r#"WHERE ("t"."us" >= CAST('2025-09-14 00:00:00' AS TIMESTAMP))"#),
            "{sql}"
        );
        let plan = filtered(widened("utz", ns_utc.clone()).lt(zoned(1_757_808_000_123_456_000)));
        assert!(pushable(&plan));
        let sql = render(plan);
        assert!(
            sql.ends_with(
                r#"WHERE ("t"."utz" < CAST('2025-09-14 00:00:00.123456+00' AS TIMESTAMP WITH TIME ZONE))"#
            ),
            "{sql}"
        );
        // Finer than the microsecond: PostgreSQL would round the literal.
        assert!(!pushable(&filtered(
            widened("us", ns.clone()).gt_eq(naive(1_757_808_000_000_000_001))
        )));
        // A narrowing changes values; a zone added or dropped changes the instant.
        let ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        assert!(!pushable(&filtered(widened("us", ms).gt_eq(lit(
            ScalarValue::TimestampMillisecond(Some(1_757_808_000_000), None)
        )))));
        assert!(!pushable(&filtered(
            widened("us", ns_utc).gt_eq(zoned(1_757_808_000_000_000_000))
        )));
        // Only a widened column: a bare cast stays local.
        assert!(!pushable(&filtered(
            cast(col("us"), ns).gt_eq(naive(1_757_808_000_000_000_000))
        )));
    }

    #[test]
    fn joins_need_comparable_keys_and_plain_null_equality() {
        let other = table_scan(Some("u"), &schema(), None).unwrap().build().unwrap();
        let join = |l: &str, r: &str, ty: JoinType| {
            base()
                .join(
                    other.clone(),
                    ty,
                    (vec![format!("t.{l}")], vec![format!("u.{r}")]),
                    None,
                )
                .unwrap()
                .build()
                .unwrap()
        };
        assert!(pushable(&join("s", "s", JoinType::Inner)));
        assert!(pushable(&join("id", "id", JoinType::Left)));
        assert!(!pushable(&join("f", "f", JoinType::Inner)));
        assert!(!pushable(&join("id", "id", JoinType::LeftSemi)));
        let null_equal = base()
            .join_detailed(
                other.clone(),
                JoinType::Inner,
                (vec!["t.id"], vec!["u.id"]),
                None,
                NullEquality::NullEqualsNull,
            )
            .unwrap()
            .build()
            .unwrap();
        assert!(!pushable(&null_equal));
    }
}
