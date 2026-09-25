//! `UPDATE` and `DELETE` change exactly the rows their `WHERE` selects.
//!
//! `DataFusion` 54.1 hands a table's `update` / `delete_from` only the
//! conditions it finds, in the optimized plan, on `Filter` nodes and on the
//! scan of the table (`extract_dml_filters`); whatever else selected rows is
//! dropped without an error, and the table changes every row:
//!
//! - `id IN (SELECT 2)`, `EXISTS (…)`, a correlated scalar subquery: the
//!   optimizer turns them into a semi or mark join, which is not a filter;
//! - a `WHERE` the optimizer proves false (`1 = 0`, `id = 1 AND id = 2`):
//!   the input becomes an empty relation, with no filter at all.
//!
//! [`DmlQueryPlanner`] rebuilds the input of an `UPDATE` / `DELETE` before
//! the physical planner reads it: the table's scan, one filter holding every
//! condition, and the new values of an `UPDATE`, each written over the
//! table's own columns (what a projection of the optimizer computed, such as
//! a common subexpression, is put back in place). The input must be the
//! table's scan under filters, projections and aliases only, without a
//! subquery; anything else is refused (`NOT_SUPPORTED`) before a row
//! changes. An input proven empty selects no row: the table is asked to
//! change the rows `false` selects (none), so it answers 0 rows, or refuses
//! as it refuses any `UPDATE` / `DELETE`.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFSchema, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::logical_plan::dml::{DmlStatement, WriteOp};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    Expr, Filter, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias, lit,
};
use datafusion::physical_plan::ExecutionPlan;

/// Query planner that checks every `UPDATE` / `DELETE` (see the module
/// documentation), then plans with `inner`.
#[derive(Debug)]
pub(crate) struct DmlQueryPlanner {
    inner: Arc<dyn QueryPlanner + Send + Sync>,
}

impl DmlQueryPlanner {
    pub(crate) fn new(inner: Arc<dyn QueryPlanner + Send + Sync>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl QueryPlanner for DmlQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !has_row_change(logical_plan) {
            return self.inner.create_physical_plan(logical_plan, session_state).await;
        }
        let checked = logical_plan.clone().transform_down(check_row_change).data()?;
        self.inner.create_physical_plan(&checked, session_state).await
    }
}

fn is_row_change(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Update | WriteOp::Delete))
}

/// `plan` (an `EXPLAIN` included) holds an `UPDATE` or a `DELETE`.
fn has_row_change(plan: &LogicalPlan) -> bool {
    plan.exists(|node| Ok(is_row_change(node))).unwrap_or(true)
}

fn check_row_change(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(Transformed::no(plan));
    };
    if !matches!(dml.op, WriteOp::Update | WriteOp::Delete) {
        return Ok(Transformed::no(LogicalPlan::Dml(dml)));
    }
    let input = rebuilt_input(&dml)?;
    Ok(Transformed::yes(LogicalPlan::Dml(DmlStatement::new(
        dml.table_name,
        dml.target,
        dml.op,
        Arc::new(input),
    ))))
}

fn refusal(dml: &DmlStatement, what: &str) -> DataFusionError {
    let op = match dml.op {
        WriteOp::Delete => "DELETE",
        _ => "UPDATE",
    };
    DataFusionError::NotImplemented(format!(
        "{op} of {table} with {what} is not supported yet: the rows to change can only be selected \
         by conditions on the columns of {table}",
        table = dml.table_name
    ))
}

/// The input of `dml` as the physical planner reads it without losing a
/// condition: the table's scan, one filter with every condition (`false`
/// when the optimizer proved there is no row) and, for an `UPDATE`, the new
/// value of each column, all over the table's own columns. Refused when the
/// input is anything but the scan under filters, projections and aliases,
/// or a condition or value holds a subquery.
fn rebuilt_input(dml: &DmlStatement) -> Result<LogicalPlan> {
    let mut chain = Vec::new();
    let mut node = dml.input.as_ref();
    let scan = loop {
        match node {
            LogicalPlan::Projection(Projection { input, .. })
            | LogicalPlan::Filter(Filter { input, .. })
            | LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                chain.push(node);
                node = input;
            }
            LogicalPlan::TableScan(scan)
                if scan.table_name.resolved_eq(&dml.table_name) && scan.fetch.is_none() =>
            {
                break Some(scan);
            }
            LogicalPlan::EmptyRelation(empty) if !empty.produce_one_row => break None,
            _ => return Err(refusal(dml, "a subquery or a join")),
        }
    };
    let builder = LogicalPlanBuilder::scan(dml.table_name.clone(), Arc::clone(&dml.target), None)?;
    let Some(scan) = scan else {
        // No row is selected.
        return builder.filter(lit(false))?.build();
    };
    let table = dml.target.schema();
    // What each column of the current node is, over the table's columns.
    let mut values: Vec<Expr> = scan
        .projected_schema
        .fields()
        .iter()
        .map(|field| Expr::Column(Column::new_unqualified(field.name())))
        .collect();
    let mut conditions = Vec::new();
    for condition in scan.filters.iter().flat_map(split_conjunction) {
        conditions.push(of_table(dml, condition, &dml.table_name, &table)?);
    }
    for node in chain.into_iter().rev() {
        match node {
            LogicalPlan::Filter(filter) => {
                for condition in split_conjunction(&filter.predicate) {
                    conditions.push(substituted(dml, condition, filter.input.schema(), &values)?);
                }
            }
            LogicalPlan::Projection(projection) => {
                values = projection
                    .expr
                    .iter()
                    .map(|expr| substituted(dml, &expr.clone().unalias(), projection.input.schema(), &values))
                    .collect::<Result<_>>()?;
            }
            // Same columns, other names.
            _ => {}
        }
    }
    let mut builder = builder;
    if let Some(predicate) = conjunction(conditions) {
        builder = builder.filter(predicate)?;
    }
    if matches!(dml.op, WriteOp::Update) {
        let output = dml.input.schema();
        let assignments = table
            .fields()
            .iter()
            .map(|field| {
                let index = output
                    .fields()
                    .iter()
                    .position(|f| f.name() == field.name())
                    .ok_or_else(|| refusal(dml, &format!("no value for the column {}", field.name())))?;
                Ok(values[index].clone().alias(field.name()))
            })
            .collect::<Result<Vec<_>>>()?;
        builder = builder.project(assignments)?;
    }
    builder.build()
}

/// A subquery `expr` holds, if any.
fn holds_subquery(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::ScalarSubquery(_)
                | Expr::SetComparison(_)
                | Expr::OuterReferenceColumn(..)
        ))
    })
    .unwrap_or(true)
}

/// `expr`, over the columns of `schema`, with each column replaced by its
/// value in `values` (by position).
fn substituted(dml: &DmlStatement, expr: &Expr, schema: &DFSchema, values: &[Expr]) -> Result<Expr> {
    if holds_subquery(expr) {
        return Err(refusal(dml, "a subquery"));
    }
    expr.clone()
        .transform(|e| match e {
            Expr::Column(column) => {
                let value = schema
                    .index_of_column(&column)
                    .ok()
                    .and_then(|index| values.get(index))
                    .ok_or_else(|| refusal(dml, &format!("the column {column}, not one of the table's")))?;
                Ok(Transformed::yes(value.clone()))
            }
            other => Ok(Transformed::no(other)),
        })
        .data()
}

/// A condition of the table's scan, over its columns unqualified.
fn of_table(dml: &DmlStatement, expr: &Expr, name: &TableReference, table: &SchemaRef) -> Result<Expr> {
    if holds_subquery(expr) {
        return Err(refusal(dml, "a subquery"));
    }
    expr.clone()
        .transform(|e| match e {
            Expr::Column(column)
                if column.relation.as_ref().is_none_or(|r| r.resolved_eq(name))
                    && table.field_with_name(&column.name).is_ok() =>
            {
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    column.name,
                ))))
            }
            Expr::Column(column) => Err(refusal(
                dml,
                &format!("the column {column}, not one of the table's"),
            )),
            other => Ok(Transformed::no(other)),
        })
        .data()
}
