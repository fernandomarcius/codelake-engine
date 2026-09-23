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
//! | binary, temporal, boolean and non-ASCII text literals | no faithful T-SQL rendering in the unparser |
//! | functions, `CASE`, `LIKE`, casts other than numeric widening | semantics differ or are unverified |

use std::ops::ControlFlow;
use std::sync::Arc;

use codelake_core::federation_policy::PushdownPolicy;
use codelake_core::functions::{VARBINARY_LITERAL, as_binary_literal, binary_literal_expr, decode_hex};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ScalarValue};
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

fn is_integer(data_type: &DataType) -> bool {
    data_type.is_integer()
}

fn is_float(data_type: &DataType) -> bool {
    data_type.is_floating()
}

fn type_of(expr: &Expr, schema: &DFSchema) -> Option<DataType> {
    expr.get_type(schema).ok()
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

/// Expression whose value SQL Server computes exactly like the engine.
fn safe_expr(expr: &Expr, schema: &DFSchema) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Literal(value, _) => safe_literal(value),
        // Binary comparisons are byte-wise on both sides. SQL Server pads the
        // shorter operand with zeros, the engine treats a prefix as smaller:
        // they differ only when comparing an all-zero value to a shorter one.
        Expr::ScalarFunction(_) => as_binary_literal(expr).is_some(),
        Expr::Alias(Alias { expr, .. }) => safe_expr(expr, schema),
        Expr::BinaryExpr(binary) => safe_binary(binary, schema),
        Expr::Not(inner) => safe_predicate(inner, schema),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => safe_expr(inner, schema),
        Expr::Between(between) => [&*between.expr, &*between.low, &*between.high]
            .iter()
            .all(|e| safe_expr(e, schema) && type_of(e, schema).is_some_and(|t| !is_text(&t))),
        Expr::InList(list) => {
            type_of(&list.expr, schema).is_some_and(|t| !is_text(&t))
                && safe_expr(&list.expr, schema)
                && list.list.iter().all(|e| safe_expr(e, schema))
        }
        Expr::Cast(cast) => {
            let from = type_of(&cast.expr, schema);
            safe_expr(&cast.expr, schema)
                && from.is_some_and(|f| is_integer(&f) && (cast.data_type.is_numeric()))
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
    match binary.op {
        Operator::And | Operator::Or => safe_predicate(left, schema) && safe_predicate(right, schema),
        Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            operands && !is_text(&lt) && !is_text(&rt) && lt != DataType::Boolean
        }
        Operator::Plus | Operator::Minus | Operator::Multiply => {
            operands && lt.is_numeric() && rt.is_numeric()
        }
        Operator::Divide | Operator::Modulo => {
            operands && ((is_integer(&lt) && is_integer(&rt)) || (is_float(&lt) && is_float(&rt)))
        }
        _ => false,
    }
}

/// Boolean expression usable as `WHERE`/`ON`: T-SQL has no bare boolean values.
fn safe_predicate(expr: &Expr, schema: &DFSchema) -> bool {
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

fn safe_aggregate(aggregate: &Aggregate) -> bool {
    let schema = aggregate.input.schema();
    let groups_ok = aggregate
        .group_expr
        .iter()
        .all(|e| safe_expr(e, schema) && type_of(e, schema).is_some_and(|t| !is_text(&t)));
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
            let args_ok = params
                .args
                .iter()
                .all(|a| safe_expr(a, schema) && type_of(a, schema).is_some_and(|t| !is_text(&t)));
            let arg_type = params.args.first().and_then(|a| type_of(a, schema));
            args_ok
                && match func.name() {
                    "count" => true,
                    "min" | "max" => !params.distinct,
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
            LogicalPlan::Projection(p) => p
                .expr
                .iter()
                .all(|e| safe_expr(e, p.input.schema()) && !has_binary_literal(e)),
            LogicalPlan::Filter(f) => safe_predicate(&f.predicate, f.input.schema()),
            LogicalPlan::SubqueryAlias(_) | LogicalPlan::Union(_) => true,
            LogicalPlan::Join(join) => {
                let schema = join.schema.as_ref();
                let left = join.left.schema();
                let right = join.right.schema();
                matches!(
                    join.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) && join.on.iter().all(|(l, r)| {
                    safe_expr(l, left)
                        && safe_expr(r, right)
                        && type_of(l, left).is_some_and(|t| !is_text(&t))
                        && type_of(r, right).is_some_and(|t| !is_text(&t))
                }) && join.filter.as_ref().is_none_or(|f| safe_predicate(f, schema))
            }
            LogicalPlan::Limit(limit) => {
                limit.skip.is_none()
                    && matches!(limit.fetch.as_deref(), Some(Expr::Literal(ScalarValue::Int64(Some(n)), _)) if *n > 0)
            }
            LogicalPlan::Aggregate(aggregate) => {
                safe_aggregate(aggregate)
                    && !aggregate
                        .group_expr
                        .iter()
                        .chain(&aggregate.aggr_expr)
                        .any(has_binary_literal)
            }
            LogicalPlan::Distinct(_) => plan.schema().fields().iter().all(|f| !is_text(f.data_type())),
            _ => false,
        }
    }

    /// Adapts a subtree about to be unparsed:
    /// - a projection of plain columns directly below an aggregation is
    ///   dropped: the unparser would turn it into a derived table and then
    ///   reference the base table's qualifier from outside it, which T-SQL
    ///   rejects;
    /// - the optimizer folds `codelake_varbinary('..')` into a binary literal
    ///   the unparser cannot render; predicate literals become the function again.
    fn prepare(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
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
            node.map_expressions(|expr| {
                expr.transform(|e| match &e {
                    Expr::Literal(ScalarValue::Binary(Some(bytes)), _) => {
                        Ok(Transformed::yes(binary_literal_expr(bytes)))
                    }
                    _ => Ok(Transformed::no(e)),
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

        fn scan() -> LogicalPlanBuilder {
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("small", DataType::Int32, true),
                Field::new("amount", DataType::Decimal128(18, 2), true),
                Field::new("ratio", DataType::Float64, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("lsn", DataType::Binary, true),
                Field::new("flag", DataType::Boolean, true),
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
            let folded = lit(ScalarValue::Binary(Some(vec![0, 1])));
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
            let watermark = codelake_core::functions::binary_literal_expr(&[0, 1]);
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
