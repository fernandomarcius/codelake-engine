//! Operand types a statement may not combine (ADR 0014). [`check_plan`]
//! refuses them in a planned statement, before the analyzer coerces
//! anything. It runs for statements planned by `Engine::sql` (the client
//! protocols of the engine); the PostgreSQL wire path keeps `DataFusion`'s
//! coercion, where an untyped literal takes the type of the other side as in
//! PostgreSQL.
//!
//! - **Text and numbers are not comparable.** `x > '20'` with `x` an
//!   `integer`, `CAST(5.0 AS double) = '5'`, `varchar_column > 9`,
//!   `x IN ('1.0')`, `CASE 'x' WHEN 1.0 …` and `x BETWEEN '1' AND 9` fail
//!   with `TYPE_MISMATCH` ("Cannot apply operator: integer > varchar"): no
//!   implicit conversion exists between `varchar` and the number types.
//!   `DataFusion` coerces them instead, and changed direction between majors:
//!   53 compared the text (`5 > '20'` was false, `100 > '20'` false), 54
//!   converts the text to the number (`100 > '20'` true, `'a'` a cast error
//!   at run time). Either way a query that must fail answered rows, and they
//!   changed without notice when the engine moved to `DataFusion` 54.
//! - **`time ± interval` is refused** (`NOT_SUPPORTED`): `DataFusion` 54
//!   answers an interval (`TIME '23:30:00' + INTERVAL '1' HOUR` is
//!   `24 hours 30 mins`) where the answer is the time of day `00:30:00`.
//!   `DataFusion` 55 changes the operator to return a time; until the engine
//!   is on it, a loud refusal replaces the wrong value.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Between, BinaryExpr, Case, Expr, ExprSchemable, LogicalPlan, Operator};

use crate::trino_types::display_name;

fn value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, values) => values,
        other => other,
    }
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        value_type(data_type),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_number(data_type: &DataType) -> bool {
    value_type(data_type).is_numeric()
}

fn is_time(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Time32(_) | DataType::Time64(_))
}

fn is_interval(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Interval(_) | DataType::Duration(_))
}

/// Text on one side, a number on the other.
fn mixed(a: &DataType, b: &DataType) -> bool {
    (is_text(a) && is_number(b)) || (is_number(a) && is_text(b))
}

fn mismatch(message: &str) -> DataFusionError {
    DataFusionError::Plan(format!("TYPE_MISMATCH: {message}"))
}

/// Type of `expr`, or `None` when it cannot be known yet (a placeholder, a
/// reference the planner resolves later): nothing to check then.
fn type_of(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    expr.get_type(schema).ok()
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

fn check_expr(expr: &Expr, schema: &DFSchema) -> Result<()> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(*op) => {
            if let (Some(l), Some(r)) = (type_of(left, schema), type_of(right, schema))
                && mixed(&l, &r)
            {
                return Err(mismatch(&format!(
                    "Cannot apply operator: {} {op} {}",
                    display_name(&l),
                    display_name(&r)
                )));
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(op, Operator::Plus | Operator::Minus) =>
        {
            if let (Some(l), Some(r)) = (type_of(left, schema), type_of(right, schema))
                && (is_time(&l) && is_interval(&r) || is_interval(&l) && is_time(&r))
            {
                return Err(DataFusionError::Plan(format!(
                    "NOT_SUPPORTED: {} {op} {} is not supported",
                    display_name(&l),
                    display_name(&r)
                )));
            }
        }
        Expr::InList(InList { expr, list, .. }) => {
            let Some(value) = type_of(expr, schema) else {
                return Ok(());
            };
            for item in list {
                if let Some(item) = type_of(item, schema)
                    && mixed(&value, &item)
                {
                    return Err(mismatch(&format!(
                        "IN value and list items must be the same type or coercible to a common type. \
                         Cannot find common type between {} and {}",
                        display_name(&value),
                        display_name(&item)
                    )));
                }
            }
        }
        Expr::Between(Between { expr, low, high, .. }) => {
            if let (Some(value), Some(low), Some(high)) =
                (type_of(expr, schema), type_of(low, schema), type_of(high, schema))
                && (mixed(&value, &low) || mixed(&value, &high))
            {
                return Err(mismatch(&format!(
                    "Cannot check if {} is BETWEEN {} and {}",
                    display_name(&value),
                    display_name(&low),
                    display_name(&high)
                )));
            }
        }
        Expr::Case(Case {
            expr: Some(operand),
            when_then_expr,
            ..
        }) => {
            let Some(operand) = type_of(operand, schema) else {
                return Ok(());
            };
            for (when, _) in when_then_expr {
                if let Some(when) = type_of(when, schema)
                    && mixed(&operand, &when)
                {
                    return Err(mismatch(&format!(
                        "CASE operand type does not match WHEN clause operand type: {} vs {}",
                        display_name(&operand),
                        display_name(&when)
                    )));
                }
            }
        }
        Expr::InSubquery(in_subquery) => {
            let fields = in_subquery.subquery.subquery.schema().fields();
            if let (Some(value), [column]) = (type_of(&in_subquery.expr, schema), fields.as_ref())
                && mixed(&value, column.data_type())
            {
                return Err(mismatch(&format!(
                    "Value expression and result of subquery must be of the same type: {} vs {}",
                    display_name(&value),
                    display_name(column.data_type())
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

/// The schema the expressions of `node` see: its inputs', or its own for a
/// leaf.
fn expression_schema(node: &LogicalPlan) -> DFSchema {
    let inputs = node.inputs();
    let Some((first, rest)) = inputs.split_first() else {
        return node.schema().as_ref().clone();
    };
    let mut schema = first.schema().as_ref().clone();
    for input in rest {
        schema.merge(input.schema());
    }
    schema
}

/// Refuses the operand types of the module documentation anywhere in
/// `plan`, its subqueries and the bodies of the views it reads included (a
/// view stored before these checks, or through another path, is checked
/// when it is read).
pub(crate) fn check_plan(plan: &LogicalPlan) -> Result<()> {
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(body) = scan.source.get_logical_plan()
        {
            check_plan(&body)?;
        }
        let schema = expression_schema(node);
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                check_expr(e, &schema)?;
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}
