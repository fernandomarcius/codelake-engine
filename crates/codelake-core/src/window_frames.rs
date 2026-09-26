//! `RANGE` frames ordered by a type without arithmetic.
//!
//! A window with `ORDER BY` and no frame gets `RANGE BETWEEN UNBOUNDED
//! PRECEDING AND CURRENT ROW`. `DataFusion`'s type coercion computes a type
//! for the bounds of every `RANGE` frame from the first sort key and fails on
//! a key it cannot subtract from ("Cannot run range queries on datatype:
//! Binary"), even when no bound has an offset; Trino 431 accepts
//! `row_number() OVER (ORDER BY "__$start_lsn")`.
//!
//! With bounds that are only `UNBOUNDED` or `CURRENT ROW`, a `RANGE` frame
//! and a `GROUPS` frame hold the same rows: `CURRENT ROW` is the current row
//! with its peers in both. [`RangeFramesAsGroups`] (analyzer, before type
//! coercion) rewrites such frames to `GROUPS` when the sort key has no range
//! arithmetic, so nothing about the answer changes. A frame with an offset
//! on such a key still fails, as it must.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, WindowFrameBound, WindowFrameUnits};
use datafusion::optimizer::AnalyzerRule;

/// Sort key types `DataFusion` computes `RANGE` bounds for (see
/// `extract_window_frame_target_type` in its type coercion).
fn has_range_arithmetic(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, value) => has_range_arithmetic(value),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(..)
        | DataType::Boolean
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(..)
        | DataType::Duration(_)
        | DataType::Interval(_) => true,
        other => other.is_numeric() || other.is_string() || other.is_null(),
    }
}

fn without_offset(bound: &WindowFrameBound) -> bool {
    match bound {
        WindowFrameBound::CurrentRow => true,
        WindowFrameBound::Preceding(v) | WindowFrameBound::Following(v) => v.is_null(),
    }
}

fn rewrite_window(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::WindowFunction(mut window) = expr else {
        return Ok(Transformed::no(expr));
    };
    let WindowFunction { params, .. } = window.as_mut();
    let frame = &params.window_frame;
    let applies = frame.units == WindowFrameUnits::Range
        && without_offset(&frame.start_bound)
        && without_offset(&frame.end_bound)
        && match params.order_by.first() {
            Some(sort) => !has_range_arithmetic(&sort.expr.get_type(schema)?),
            None => false,
        };
    if !applies {
        return Ok(Transformed::no(Expr::WindowFunction(window)));
    }
    params.window_frame.units = WindowFrameUnits::Groups;
    Ok(Transformed::yes(Expr::WindowFunction(window)))
}

/// The frame is part of a window expression's name: the rewritten one keeps
/// the name the plan above refers to.
fn rewrite_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let schema = datafusion::logical_expr::utils::merge_schema(&plan.inputs());
    let names = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let name = names.save(&expr);
        expr.transform_up(|e| rewrite_window(e, &schema))
            .map(|t| t.update_data(|e| name.restore(e)))
    })
}

/// See the module documentation.
#[derive(Debug, Default)]
pub(crate) struct RangeFramesAsGroups;

impl AnalyzerRule for RangeFramesAsGroups {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_node).map(|t| t.data)
    }

    fn name(&self) -> &'static str {
        "codelake_range_frames_as_groups"
    }
}
