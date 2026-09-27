//! `0.0` and `-0.0` are one value (ISS-050, ADR 0017).
//!
//! The engine orders floats by their bits, which puts `-0.0` below `0.0`:
//! `x = 0` dropped `-0.0`, `x < 0` and `x IS DISTINCT FROM 0` kept it, and
//! `IN (0, …)`, `BETWEEN 0 AND 1`, `CASE x WHEN 0` and `nullif(x, 0)` did the
//! same. The SQL standard, PostgreSQL and Trino's comparisons have `-0.0 =
//! 0.0`. The engine does so everywhere: comparisons, `IS [NOT] DISTINCT
//! FROM`, `IN`, joins, `IN (subquery)`, `GROUP BY` and `DISTINCT`.
//!
//! [`FloatZeros`] (logical, right before `simplify_expressions`) rewrites a
//! comparison of a float with a zero constant into one the bit order answers
//! as IEEE 754 does over both zeros: `x = 0` is `x = -0.0 OR x = 0.0`, `x >=
//! 0` is `x >= -0.0`, `x <= -0.0` is `x <= 0.0`, `x < 0` is `x < -0.0`, `x >
//! -0.0` is `x > 0.0`. The forms stay plain comparisons: a scan prunes row
//! groups by statistics with them (a bit order `x >= 0.0` pruned a row group
//! holding only `-0.0`), a remote source reads them as the same condition,
//! `x = 0` no longer tells the planner that `x` is a constant (a window
//! partitioned by `x` above it would be left unsorted), and the simplifier's
//! negations stay exact (`NOT (x = 0)` becomes `x <> -0.0 AND x <> 0.0`).
//! `IN` lists get both zeros, `BETWEEN` with a zero bound and `CASE x WHEN`
//! and `nullif` are spelled out as comparisons, and a comparison of two
//! constants is folded here with IEEE semantics, before the simplifier folds
//! it with the bit order. `x IS NOT DISTINCT FROM 0` is `x IS NOT NULL AND
//! (x = -0.0 OR x = 0.0)`, and `x IS DISTINCT FROM 0` its negation. `<>`
//! keeps its form: the bit order answers it with a superset where it is
//! pushed, and [`super::nan::NanAwareComparison`] corrects it (and every
//! comparison of two columns) where it runs.
//!
//! Comparisons of two columns are corrected where they run
//! (`nan::NanAwareComparison`); joins, grouping and `DISTINCT` compare
//! normalized keys (`keys`).

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion::logical_expr::expr::{Between, Case, InList};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// `isnan(e)`, for a float operand of a negated comparison.
fn nan_of(e: &Expr) -> Expr {
    datafusion::functions::math::expr_fn::isnan(e.clone())
}

/// Name of [`FloatZeros`].
pub(crate) const FLOAT_ZEROS: &str = "codelake_float_zeros";

/// Logical rule, first of the optimizer (before `simplify_expressions` folds
/// constants with the bit order): see the module documentation.
#[derive(Debug, Default)]
pub(crate) struct FloatZeros;

fn is_float(t: &DataType) -> bool {
    matches!(t, DataType::Float32 | DataType::Float64)
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

/// `k op x` as `x op' k`.
fn mirrored(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// A float constant.
#[derive(Clone, Copy)]
enum Constant {
    Null,
    Float(f64),
}

impl Constant {
    fn of(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Float64(v) => Some(v.map_or(Self::Null, Self::Float)),
            ScalarValue::Float32(v) => Some(v.map_or(Self::Null, |v| Self::Float(f64::from(v)))),
            ScalarValue::Null => Some(Self::Null),
            _ => None,
        }
    }

    fn value(self) -> Option<f64> {
        match self {
            Self::Null => None,
            Self::Float(v) => Some(v),
        }
    }

    fn is_zero(self) -> bool {
        matches!(self, Self::Float(v) if v == 0.0)
    }
}

/// A zero of `t` with the given sign.
fn zero(t: &DataType, negative: bool) -> Expr {
    let value = match (t, negative) {
        (DataType::Float32, false) => ScalarValue::Float32(Some(0.0)),
        (DataType::Float32, true) => ScalarValue::Float32(Some(-0.0)),
        (_, false) => ScalarValue::Float64(Some(0.0)),
        (_, true) => ScalarValue::Float64(Some(-0.0)),
    };
    Expr::Literal(value, None)
}

fn compare(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

/// `x` is `0.0` or `-0.0`: `x = -0.0 OR x = 0.0`. Plain equalities, so a
/// negation stays exact (`x <> -0.0 AND x <> 0.0` keeps NaN), and not `x = k`
/// alone, which tells the planner that `x` is a constant.
fn at_zero(x: &Expr, t: &DataType) -> Expr {
    compare(x.clone(), Operator::Eq, zero(t, true)).or(compare(x.clone(), Operator::Eq, zero(t, false)))
}

/// `e` is `x = <zero>`: the sign of the zero.
fn zero_equality(e: &Expr) -> Option<(&Expr, bool)> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = e
    else {
        return None;
    };
    let sign = |e: &Expr| match e {
        Expr::Literal(ScalarValue::Float64(Some(v)), _) if *v == 0.0 => Some(v.is_sign_negative()),
        Expr::Literal(ScalarValue::Float32(Some(v)), _) if *v == 0.0 => Some(v.is_sign_negative()),
        _ => None,
    };
    sign(right).map(|negative| (left.as_ref(), negative))
}

/// IEEE 754 result of `a op b` over constants, NaN included (`IS [NOT]
/// DISTINCT FROM` puts every NaN in one group, as `GROUP BY` does).
fn fold(a: Constant, op: Operator, b: Constant) -> ScalarValue {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let same = |a: f64, b: f64| a.partial_cmp(&b) == Some(Equal) || (a.is_nan() && b.is_nan());
    let result = match (op, a.value(), b.value()) {
        (Operator::IsDistinctFrom, Some(a), Some(b)) => Some(!same(a, b)),
        (Operator::IsNotDistinctFrom, Some(a), Some(b)) => Some(same(a, b)),
        (Operator::IsDistinctFrom, a, b) => Some(a.is_some() != b.is_some()),
        (Operator::IsNotDistinctFrom, a, b) => Some(a.is_some() == b.is_some()),
        (op, Some(a), Some(b)) => {
            let order = a.partial_cmp(&b);
            match op {
                Operator::Eq => Some(order == Some(Equal)),
                Operator::NotEq => Some(order != Some(Equal)),
                Operator::Lt => Some(order == Some(Less)),
                Operator::LtEq => Some(matches!(order, Some(Less | Equal))),
                Operator::Gt => Some(order == Some(Greater)),
                Operator::GtEq => Some(matches!(order, Some(Greater | Equal))),
                _ => None,
            }
        }
        _ => None,
    };
    ScalarValue::Boolean(result)
}

struct Zeros<'a> {
    schema: &'a DFSchema,
    simplifier: ExprSimplifier,
}

impl Zeros<'_> {
    fn float_type(&self, e: &Expr) -> Option<DataType> {
        e.get_type(self.schema).ok().filter(is_float)
    }

    /// The value of a constant float expression.
    fn constant(&self, e: &Expr) -> Option<Constant> {
        if !e.column_refs().is_empty() || e.is_volatile() {
            return None;
        }
        let mut placeholder = false;
        let _ = e.apply(|node| {
            placeholder |= matches!(
                node,
                Expr::Placeholder(_) | Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)
            );
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        });
        if placeholder {
            return None;
        }
        match self.simplifier.simplify(e.clone()) {
            Ok(Expr::Literal(value, _)) => Constant::of(&value),
            _ => None,
        }
    }

    /// `left op right` of two floats (operands already rewritten), as
    /// described in the module documentation, or `None` when the bit order
    /// answers it right where it runs (`<>`: see `nan::NanAwareComparison`).
    fn comparison(&self, left: &Expr, op: Operator, right: &Expr) -> Option<Expr> {
        let t = self.float_type(left)?;
        self.float_type(right)?;
        let (l, r) = (self.constant(left), self.constant(right));
        if let (Some(a), Some(b)) = (l, r) {
            return Some(Expr::Literal(fold(a, op, b), None));
        }
        let (x, op) = match (l, r) {
            (None, Some(k)) if k.is_zero() => (left, op),
            (Some(k), None) if k.is_zero() => (right, mirrored(op)),
            _ => return None,
        };
        // The zero that makes the bit order exact over both zeros.
        let bound = |op: Operator, negative: bool| compare(x.clone(), op, zero(&t, negative));
        match op {
            // `x` appears twice: only when it gives one value each time.
            Operator::Eq if !x.is_volatile() => Some(at_zero(x, &t)),
            // A scan reads `x IS NOT DISTINCT FROM 0.0` as `x = 0.0` to prune
            // row groups, and dropped those holding only `-0.0`.
            Operator::IsNotDistinctFrom if !x.is_volatile() => {
                Some(x.clone().is_not_null().and(at_zero(x, &t)))
            }
            Operator::IsDistinctFrom if !x.is_volatile() => Some(x.clone().is_null().or(
                compare(x.clone(), Operator::NotEq, zero(&t, true)).and(compare(
                    x.clone(),
                    Operator::NotEq,
                    zero(&t, false),
                )),
            )),
            Operator::Lt | Operator::GtEq => Some(bound(op, true)),
            Operator::LtEq | Operator::Gt => Some(bound(op, false)),
            _ => None,
        }
    }

    /// `left op right`, rewritten if needed.
    fn comparison_or_same(&self, left: Expr, op: Operator, right: Expr) -> Expr {
        self.comparison(&left, op, &right)
            .unwrap_or_else(|| compare(left, op, right))
    }

    /// `expr` with every comparison below rewritten (see the module
    /// documentation). `x = -0.0 OR x = 0.0`, what the rule writes for `x =
    /// 0`, is left as it is, so the rule can run again over its own output.
    fn rewrite(&self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => self.disjunction(compare(*left, Operator::Or, *right)),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
                let (left, right) = (self.rewrite(*left)?, self.rewrite(*right)?);
                let changed = left.transformed || right.transformed;
                let rewritten = self.comparison(&left.data, op, &right.data);
                let same = compare(left.data, op, right.data);
                Ok(match rewritten {
                    // Already in the form the rule writes (`x >= -0.0`).
                    Some(rewritten) if rewritten != same => Transformed::yes(rewritten),
                    _ => Transformed::new_transformed(same, changed),
                })
            }
            Expr::Not(inner) => {
                let inner = self.rewrite(*inner)?;
                let changed = inner.transformed;
                Ok(match self.negated_ordering(&inner.data) {
                    Some(negated) => Transformed::yes(negated),
                    None => Transformed::new_transformed(Expr::Not(Box::new(inner.data)), changed),
                })
            }
            other => {
                let children = other.map_children(|child| self.rewrite(child))?;
                let changed = children.transformed;
                let node = match children.data {
                    Expr::InList(list) => self.in_list(list),
                    Expr::Between(between) => self.between(between),
                    Expr::Case(case) => self.case(case),
                    Expr::ScalarFunction(function) if function.name() == "nullif" => {
                        self.nullif(Expr::ScalarFunction(function))
                    }
                    node => Transformed::no(node),
                };
                Ok(Transformed::new_transformed(
                    node.data,
                    changed || node.transformed,
                ))
            }
        }
    }

    /// An `OR`: each disjunct rewritten, except a `x = <zero>` whose other
    /// zero is also there (the rule's own output).
    fn disjunction(&self, or: Expr) -> Result<Transformed<Expr>> {
        let mut disjuncts = Vec::new();
        let mut pending = vec![or];
        while let Some(e) = pending.pop() {
            match e {
                Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Or,
                    right,
                }) => {
                    pending.push(*right);
                    pending.push(*left);
                }
                other => disjuncts.push(other),
            }
        }
        let zeros: Vec<(&Expr, bool)> = disjuncts.iter().filter_map(zero_equality).collect();
        let paired: Vec<bool> = disjuncts
            .iter()
            .map(|d| {
                zero_equality(d)
                    .is_some_and(|(x, negative)| zeros.iter().any(|(y, n)| *y == x && *n != negative))
            })
            .collect();
        let mut changed = false;
        let mut rewritten = Vec::with_capacity(disjuncts.len());
        for (disjunct, paired) in disjuncts.into_iter().zip(paired) {
            if paired {
                rewritten.push(disjunct);
            } else {
                let result = self.rewrite(disjunct)?;
                changed |= result.transformed;
                rewritten.push(result.data);
            }
        }
        let mut disjuncts = rewritten.into_iter();
        let first = disjuncts
            .next()
            .unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(false)), None));
        let or = disjuncts.fold(first, Expr::or);
        Ok(Transformed::new_transformed(or, changed))
    }

    /// `x [NOT] IN (…)` of a float: a zero in the list brings the other zero;
    /// a list with a non-constant item is spelled out as comparisons.
    fn in_list(&self, list: InList) -> Transformed<Expr> {
        let Some(t) = self.float_type(&list.expr) else {
            return Transformed::no(Expr::InList(list));
        };
        let constants: Vec<Option<Constant>> = list.list.iter().map(|item| self.constant(item)).collect();
        if list.expr.is_volatile() || self.constant(&list.expr).is_some() {
            return Transformed::no(Expr::InList(list));
        }
        if constants.iter().any(Option::is_none) {
            let op = if list.negated {
                Operator::NotEq
            } else {
                Operator::Eq
            };
            let comparisons = list
                .list
                .into_iter()
                .map(|item| self.comparison_or_same(list.expr.as_ref().clone(), op, item));
            let joined = if list.negated {
                comparisons.reduce(Expr::and)
            } else {
                comparisons.reduce(Expr::or)
            };
            return match joined {
                Some(joined) => Transformed::yes(joined),
                None => Transformed::no(Expr::InList(InList::new(list.expr, Vec::new(), list.negated))),
            };
        }
        if !constants.iter().flatten().any(|k| k.is_zero()) {
            return Transformed::no(Expr::InList(list));
        }
        // Both zeros first (the order `x = -0.0 OR x = 0.0 OR …` comes in),
        // then the other values.
        let others = list
            .list
            .iter()
            .zip(constants)
            .filter(|(_, k)| !k.is_some_and(Constant::is_zero))
            .map(|(item, _)| item.clone());
        let items: Vec<Expr> = [zero(&t, true), zero(&t, false)]
            .into_iter()
            .chain(others)
            .collect();
        let changed = items != list.list;
        Transformed::new_transformed(Expr::InList(InList::new(list.expr, items, list.negated)), changed)
    }

    /// `NOT (a < b)` of floats (any ordering comparison) as the negated
    /// comparison or a NaN operand: every comparison with NaN is false, so
    /// its negation is true (Trino). The simplifier would write `a >= b`,
    /// false for NaN.
    fn negated_ordering(&self, expr: &Expr) -> Option<Expr> {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            return None;
        };
        let negated = match op {
            Operator::Lt => Operator::GtEq,
            Operator::LtEq => Operator::Gt,
            Operator::Gt => Operator::LtEq,
            Operator::GtEq => Operator::Lt,
            _ => return None,
        };
        self.float_type(left).or_else(|| self.float_type(right))?;
        Some(
            compare(left.as_ref().clone(), negated, right.as_ref().clone())
                .or(nan_of(left))
                .or(nan_of(right)),
        )
    }

    /// `x BETWEEN a AND b` of floats with a zero bound as its two
    /// comparisons; `NOT BETWEEN` of floats as `x < a OR x > b` or a NaN
    /// operand.
    fn between(&self, between: Between) -> Transformed<Expr> {
        if self.float_type(&between.expr).is_none() {
            return Transformed::no(Expr::Between(between));
        }
        if between.negated {
            // `NOT (x >= a AND x <= b)`: true where a comparison is false,
            // NaN included (`x < a OR x > b` alone drops NaN rows).
            let Between { expr, low, high, .. } = between;
            let below = self.comparison_or_same(expr.as_ref().clone(), Operator::Lt, low.as_ref().clone());
            let above = self.comparison_or_same(expr.as_ref().clone(), Operator::Gt, high.as_ref().clone());
            return Transformed::yes(
                below
                    .or(above)
                    .or(nan_of(&expr))
                    .or(nan_of(&low))
                    .or(nan_of(&high)),
            );
        }
        let Between { expr, low, high, .. } = between;
        let (at_low, at_high) = (
            self.comparison(&expr, Operator::GtEq, &low),
            self.comparison(&expr, Operator::LtEq, &high),
        );
        if at_low.is_none() && at_high.is_none() {
            return Transformed::no(Expr::Between(Between {
                expr,
                negated: false,
                low,
                high,
            }));
        }
        let at_low = at_low.unwrap_or_else(|| compare(expr.as_ref().clone(), Operator::GtEq, *low));
        let at_high = at_high.unwrap_or_else(|| compare(expr.as_ref().clone(), Operator::LtEq, *high));
        Transformed::yes(at_low.and(at_high))
    }

    /// `CASE x WHEN a THEN …` of a float as `CASE WHEN x = a THEN …`.
    fn case(&self, case: Case) -> Transformed<Expr> {
        let Some(base) = case.expr.as_deref() else {
            return Transformed::no(Expr::Case(case));
        };
        if self.float_type(base).is_none() || base.is_volatile() {
            return Transformed::no(Expr::Case(case));
        }
        let base = base.clone();
        let when_then_expr = case
            .when_then_expr
            .into_iter()
            .map(|(when, then)| {
                (
                    Box::new(self.comparison_or_same(base.clone(), Operator::Eq, *when)),
                    then,
                )
            })
            .collect();
        Transformed::yes(Expr::Case(Case {
            expr: None,
            when_then_expr,
            else_expr: case.else_expr,
        }))
    }

    /// `nullif(a, b)` of floats as `CASE WHEN a = b THEN NULL ELSE a END`.
    fn nullif(&self, call: Expr) -> Transformed<Expr> {
        let Expr::ScalarFunction(function) = &call else {
            return Transformed::no(call);
        };
        let [a, b] = function.args.as_slice() else {
            return Transformed::no(call);
        };
        let Some(t) = self.float_type(a) else {
            return Transformed::no(call);
        };
        if a.is_volatile() || self.float_type(b).is_none() {
            return Transformed::no(call);
        }
        let Ok(null) = ScalarValue::try_from(&t) else {
            return Transformed::no(call);
        };
        let equal = self.comparison_or_same(a.clone(), Operator::Eq, b.clone());
        Transformed::yes(Expr::Case(Case {
            expr: None,
            when_then_expr: vec![(Box::new(equal), Box::new(Expr::Literal(null, None)))],
            else_expr: Some(Box::new(a.clone())),
        }))
    }
}

/// The schema a node's own expressions are typed against (as the
/// simplifier picks it).
fn expression_schema(plan: &LogicalPlan) -> Result<DFSchemaRef> {
    if !plan.inputs().is_empty() {
        return Ok(Arc::new(merge_schema(&plan.inputs())));
    }
    if let LogicalPlan::TableScan(scan) = plan {
        return Ok(Arc::new(DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            &scan.source.schema(),
        )?));
    }
    Ok(Arc::new(DFSchema::empty()))
}

impl OptimizerRule for FloatZeros {
    fn name(&self) -> &str {
        FLOAT_ZEROS
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let schema = expression_schema(&plan)?;
        let context = SimplifyContext::builder()
            .with_schema(Arc::clone(&schema))
            .with_config_options(config.options())
            .with_query_execution_start_time(config.query_execution_start_time())
            .build();
        let zeros = Zeros {
            schema: &schema,
            simplifier: ExprSimplifier::new(context),
        };
        let names = NamePreserver::new(&plan);
        let mut rewrite = |expr: Expr| {
            let name = names.save(&expr);
            let rewritten = zeros.rewrite(expr)?;
            Ok(rewritten.update_data(|e| name.restore(e)))
        };
        plan.map_expressions(|expr| {
            if let Expr::GroupingSet(_) = &expr {
                expr.map_children(&mut rewrite)
            } else {
                rewrite(expr)
            }
        })
    }
}
