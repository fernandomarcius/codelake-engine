//! Operand types a statement may not combine (ADR 0014), refused on both
//! client protocols before the analyzer coerces anything:
//!
//! - `check_plan` for statements planned by `Engine::sql` (the Trino
//!   protocol and the engine's other clients), with Trino's errors;
//! - [`postgres_refusal`] for the PostgreSQL wire protocol, with
//!   PostgreSQL's (SQLSTATE `42883`, "operator does not exist: text >
//!   integer"). A quoted literal there is untyped, as in PostgreSQL: it
//!   takes the type of the other side (`int_col > '20'` answers), so only
//!   typed text meets a number (`text_col > 9`, `'7'::text = 7`).
//!
//! What is refused:
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
//! - **`time ± interval` is refused** (`NOT_SUPPORTED`, SQLSTATE `0A000`):
//!   `DataFusion` 54 answers an interval (`TIME '23:30:00' + INTERVAL '1'
//!   HOUR` is `24 hours 30 mins`) where the answer is the time of day
//!   `00:30:00`. `DataFusion` 55 changes the operator to return a time;
//!   until the engine is on it, a loud refusal replaces the wrong value.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue};
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

/// The protocol whose typing rules apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    /// Every operand is typed: `'20'` is a `varchar`.
    Trino,
    /// A quoted literal or a parameter is untyped and takes the other
    /// side's type.
    Postgres,
}

/// One side of a refused operation.
#[derive(Debug, Clone)]
struct Operand {
    data_type: DataType,
    /// The literal the side is, if it is one (PostgreSQL names the type of
    /// a number literal by its value).
    literal: Option<ScalarValue>,
}

impl Operand {
    /// `expr` as an operand, or `None` when its type is not known yet (a
    /// placeholder, a reference the planner resolves later) or, for
    /// PostgreSQL, when it is untyped: nothing to check then.
    fn of(expr: &Expr, schema: &DFSchema, protocol: Protocol) -> Option<Self> {
        if protocol == Protocol::Postgres && is_untyped(expr) {
            return None;
        }
        let data_type = expr.get_type(schema).ok()?;
        let literal = match expr {
            Expr::Literal(value, _) => Some(value.clone()),
            _ => None,
        };
        Some(Self { data_type, literal })
    }

    fn column(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            literal: None,
        }
    }

    fn postgres_name(&self) -> String {
        match &self.literal {
            // PostgreSQL types an integer constant `integer` when it fits,
            // `bigint` otherwise, and a constant with a point `numeric`.
            Some(ScalarValue::Int64(Some(value))) if i32::try_from(*value).is_ok() => "integer".into(),
            Some(ScalarValue::Float64(Some(_)) | ScalarValue::Float32(Some(_))) => "numeric".into(),
            _ => postgres_type_name(&self.data_type),
        }
    }
}

/// A quoted literal or a parameter: PostgreSQL gives it the type of the
/// other operand.
fn is_untyped(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_),
            _
        ) | Expr::Placeholder(_)
    )
}

/// PostgreSQL's name of the type `data_type` is sent as on the wire
/// (`arrow-pg`: every text type is `text`).
fn postgres_type_name(data_type: &DataType) -> String {
    match value_type(data_type) {
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => "smallint".into(),
        DataType::Int32 | DataType::UInt16 => "integer".into(),
        DataType::Int64 | DataType::UInt32 => "bigint".into(),
        DataType::UInt64
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => "numeric".into(),
        DataType::Float16 | DataType::Float32 => "real".into(),
        DataType::Float64 => "double precision".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "text".into(),
        DataType::Time32(_) | DataType::Time64(_) => "time without time zone".into(),
        DataType::Interval(_) | DataType::Duration(_) => "interval".into(),
        other => other.to_string().to_lowercase(),
    }
}

/// Where the refused operands met.
#[derive(Debug, Clone)]
enum Construct {
    Comparison,
    InList,
    InSubquery,
    /// `value BETWEEN low AND high`: the refusal's operands are the value
    /// and the bound of the other kind; Trino names all three.
    Between {
        low: DataType,
        high: DataType,
    },
    CaseWhen,
    TimeInterval,
}

/// Two operands the engine refuses to combine with `op` (`=` for `IN` and
/// `CASE`, `>=` or `<=` for the bound of a `BETWEEN`).
#[derive(Debug, Clone)]
struct Refusal {
    construct: Construct,
    op: Operator,
    left: Operand,
    right: Operand,
}

impl Refusal {
    fn trino_error(&self) -> DataFusionError {
        let (l, r) = (
            display_name(&self.left.data_type),
            display_name(&self.right.data_type),
        );
        let op = self.op;
        let message = match &self.construct {
            Construct::Comparison => format!("Cannot apply operator: {l} {op} {r}"),
            Construct::InList => format!(
                "IN value and list items must be the same type or coercible to a common type. \
                 Cannot find common type between {l} and {r}"
            ),
            Construct::InSubquery => {
                format!("Value expression and result of subquery must be of the same type: {l} vs {r}")
            }
            Construct::Between { low, high } => format!(
                "Cannot check if {l} is BETWEEN {} and {}",
                display_name(low),
                display_name(high)
            ),
            Construct::CaseWhen => {
                format!("CASE operand type does not match WHEN clause operand type: {l} vs {r}")
            }
            Construct::TimeInterval => {
                return DataFusionError::Plan(format!("NOT_SUPPORTED: {l} {op} {r} is not supported"));
            }
        };
        DataFusionError::Plan(format!("TYPE_MISMATCH: {message}"))
    }

    fn postgres(&self) -> PostgresRefusal {
        let (l, r) = (self.left.postgres_name(), self.right.postgres_name());
        let op = match self.op {
            Operator::Eq | Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => "=",
            Operator::NotEq => "<>",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Plus => "+",
            _ => "-",
        };
        if matches!(self.construct, Construct::TimeInterval) {
            return PostgresRefusal {
                code: "0A000",
                message: format!("{l} {op} {r} is not supported"),
                hint: Some("Add the interval to a timestamp and cast the result to time."),
            };
        }
        PostgresRefusal {
            code: "42883",
            message: format!("operator does not exist: {l} {op} {r}"),
            hint: Some(
                "No operator matches the given name and argument types. You might need to add explicit type casts.",
            ),
        }
    }
}

/// A statement refused on the PostgreSQL wire, as PostgreSQL words it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresRefusal {
    /// SQLSTATE: `42883` (`undefined_function`) for text against a number,
    /// `0A000` (`feature_not_supported`) for `time ± interval`.
    pub code: &'static str,
    pub message: String,
    pub hint: Option<&'static str>,
}

fn mixed(a: &Operand, b: &Operand) -> bool {
    (is_text(&a.data_type) && is_number(&b.data_type)) || (is_number(&a.data_type) && is_text(&b.data_type))
}

fn refusal(construct: Construct, op: Operator, left: &Operand, right: &Operand) -> Refusal {
    Refusal {
        construct,
        op,
        left: left.clone(),
        right: right.clone(),
    }
}

fn check_expr(expr: &Expr, schema: &DFSchema, protocol: Protocol) -> Option<Refusal> {
    let operand = |e: &Expr| Operand::of(e, schema, protocol);
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(*op) => {
            let (l, r) = (operand(left)?, operand(right)?);
            if mixed(&l, &r) {
                return Some(refusal(Construct::Comparison, *op, &l, &r));
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(op, Operator::Plus | Operator::Minus) =>
        {
            let (l, r) = (operand(left)?, operand(right)?);
            if is_time(&l.data_type) && is_interval(&r.data_type)
                || is_interval(&l.data_type) && is_time(&r.data_type)
            {
                return Some(refusal(Construct::TimeInterval, *op, &l, &r));
            }
        }
        Expr::InList(InList { expr, list, .. }) => {
            let value = operand(expr)?;
            for item in list {
                if let Some(item) = operand(item)
                    && mixed(&value, &item)
                {
                    return Some(refusal(Construct::InList, Operator::Eq, &value, &item));
                }
            }
        }
        Expr::Between(Between { expr, low, high, .. }) => {
            let value = operand(expr)?;
            let (low, high) = (operand(low), operand(high));
            let construct = Construct::Between {
                low: low.as_ref().map_or(DataType::Null, |o| o.data_type.clone()),
                high: high.as_ref().map_or(DataType::Null, |o| o.data_type.clone()),
            };
            if let Some(low) = low.filter(|low| mixed(&value, low)) {
                return Some(refusal(construct, Operator::GtEq, &value, &low));
            }
            if let Some(high) = high.filter(|high| mixed(&value, high)) {
                return Some(refusal(construct, Operator::LtEq, &value, &high));
            }
        }
        Expr::Case(Case {
            expr: Some(value),
            when_then_expr,
            ..
        }) => {
            let value = operand(value)?;
            for (when, _) in when_then_expr {
                if let Some(when) = operand(when)
                    && mixed(&value, &when)
                {
                    return Some(refusal(Construct::CaseWhen, Operator::Eq, &value, &when));
                }
            }
        }
        Expr::InSubquery(in_subquery) => {
            let value = operand(&in_subquery.expr)?;
            let fields = in_subquery.subquery.subquery.schema().fields();
            if let [column] = fields.as_ref() {
                let column = Operand::column(column.data_type());
                if mixed(&value, &column) {
                    return Some(refusal(Construct::InSubquery, Operator::Eq, &value, &column));
                }
            }
        }
        _ => {}
    }
    None
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

/// The first refused combination anywhere in `plan`, its subqueries and the
/// bodies of the views it reads included (a view stored before these
/// checks, or through another path, is checked when it is read).
fn find(plan: &LogicalPlan, protocol: Protocol) -> Result<Option<Refusal>> {
    let mut found = None;
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(body) = scan.source.get_logical_plan()
            && let Some(refusal) = find(&body, protocol)?
        {
            found = Some(refusal);
            return Ok(TreeNodeRecursion::Stop);
        }
        let schema = expression_schema(node);
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                Ok(match check_expr(e, &schema, protocol) {
                    Some(refusal) => {
                        found = Some(refusal);
                        TreeNodeRecursion::Stop
                    }
                    None => TreeNodeRecursion::Continue,
                })
            })
        })
    })?;
    Ok(found)
}

/// Refuses the operand types of the module documentation anywhere in
/// `plan`, with Trino's error (`TYPE_MISMATCH`, `NOT_SUPPORTED`).
pub(crate) fn check_plan(plan: &LogicalPlan) -> Result<()> {
    match find(plan, Protocol::Trino)? {
        Some(refusal) => Err(refusal.trino_error()),
        None => Ok(()),
    }
}

/// The refusal a statement planned on the PostgreSQL wire gets, if any,
/// with PostgreSQL's typing rules (a quoted literal is untyped) and error.
pub fn postgres_refusal(plan: &LogicalPlan) -> Result<Option<PostgresRefusal>> {
    Ok(find(plan, Protocol::Postgres)?.map(|refusal| refusal.postgres()))
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::DFParser;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;

    use super::*;

    async fn plan(sql: &str) -> LogicalPlan {
        let ctx = SessionContext::new();
        ctx.sql(
            "CREATE TABLE t AS SELECT CAST('10' AS varchar) AS s, CAST(5 AS int) AS n, \
             CAST(1 AS bigint) AS big, CAST(1.5 AS double) AS d, CAST('10:00:00' AS time) AS tm",
        )
        .await
        .unwrap();
        let mut statements = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}).unwrap();
        ctx.state()
            .statement_to_plan(statements.pop_front().unwrap())
            .await
            .unwrap()
    }

    async fn postgres(sql: &str) -> Option<PostgresRefusal> {
        postgres_refusal(&plan(sql).await).unwrap()
    }

    #[tokio::test]
    async fn postgres_refuses_typed_text_against_a_number_with_its_own_error() {
        for (sql, message) in [
            (
                "SELECT * FROM t WHERE s > 9",
                "operator does not exist: text > integer",
            ),
            (
                "SELECT * FROM t WHERE s <> 9",
                "operator does not exist: text <> integer",
            ),
            (
                "SELECT * FROM t WHERE 9.5 <= s",
                "operator does not exist: numeric <= text",
            ),
            (
                "SELECT * FROM t WHERE s = 5000000000",
                "operator does not exist: text = bigint",
            ),
            (
                "SELECT * FROM t WHERE s = n",
                "operator does not exist: text = integer",
            ),
            (
                "SELECT * FROM t WHERE d = s",
                "operator does not exist: double precision = text",
            ),
            (
                "SELECT '7'::varchar = 7",
                "operator does not exist: text = integer",
            ),
            (
                "SELECT CAST('7' AS text) = 7",
                "operator does not exist: text = integer",
            ),
            (
                "SELECT * FROM t WHERE s IN (1, 2)",
                "operator does not exist: text = integer",
            ),
            (
                "SELECT * FROM t WHERE s BETWEEN 1 AND '9'",
                "operator does not exist: text >= integer",
            ),
            (
                "SELECT * FROM t WHERE s BETWEEN '1' AND 9",
                "operator does not exist: text <= integer",
            ),
            (
                "SELECT CASE s WHEN 1 THEN 'a' END FROM t",
                "operator does not exist: text = integer",
            ),
            (
                "SELECT * FROM t WHERE s IS DISTINCT FROM big",
                "operator does not exist: text = bigint",
            ),
            (
                "SELECT * FROM t WHERE n IN (SELECT s FROM t)",
                "operator does not exist: integer = text",
            ),
            (
                "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.s = t.n)",
                "operator does not exist: text = integer",
            ),
        ] {
            let refusal = postgres(sql)
                .await
                .unwrap_or_else(|| panic!("not refused: {sql}"));
            assert_eq!(refusal.code, "42883", "{sql}");
            assert_eq!(refusal.message, message, "{sql}");
            assert!(refusal.hint.unwrap().contains("explicit type casts"), "{sql}");
        }
    }

    #[tokio::test]
    async fn postgres_refuses_time_plus_interval_as_not_supported() {
        for (sql, message) in [
            (
                "SELECT tm + INTERVAL '1 hour' FROM t",
                "time without time zone + interval is not supported",
            ),
            (
                "SELECT INTERVAL '1 hour' + tm FROM t",
                "interval + time without time zone is not supported",
            ),
            (
                "SELECT TIME '00:30:00' - INTERVAL '1 hour'",
                "time without time zone - interval is not supported",
            ),
        ] {
            let refusal = postgres(sql)
                .await
                .unwrap_or_else(|| panic!("not refused: {sql}"));
            assert_eq!(refusal.code, "0A000", "{sql}");
            assert_eq!(refusal.message, message, "{sql}");
        }
    }

    /// A quoted literal is untyped in PostgreSQL: it takes the other side's
    /// type, so these plan as PostgreSQL does. Numbers against numbers,
    /// text against text and explicit casts are not touched either.
    #[tokio::test]
    async fn postgres_keeps_untyped_literals_casts_and_same_kind_comparisons() {
        for sql in [
            "SELECT * FROM t WHERE n > '3'",
            "SELECT * FROM t WHERE '3' < n",
            "SELECT * FROM t WHERE s = '10'",
            "SELECT * FROM t WHERE n IN ('1', '5')",
            "SELECT * FROM t WHERE n BETWEEN '1' AND '9'",
            "SELECT CASE n WHEN '5' THEN 'a' END FROM t",
            "SELECT * FROM t WHERE CAST(s AS int) > 9",
            "SELECT * FROM t WHERE s > CAST(9 AS varchar)",
            "SELECT * FROM t WHERE n = 5 AND d > 1 AND big < 2.5",
            "SELECT * FROM t WHERE s < 'b'",
            "SELECT * FROM t WHERE n = $1",
            "SELECT '5' = 5",
        ] {
            assert_eq!(postgres(sql).await, None, "{sql}");
        }
    }

    /// The Trino protocol types every literal: a statement the PostgreSQL
    /// wire accepts is refused there, with Trino's error.
    #[tokio::test]
    async fn trino_types_quoted_literals() {
        let err = check_plan(&plan("SELECT * FROM t WHERE n > '3'").await).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: TYPE_MISMATCH: Cannot apply operator: integer > varchar"
        );
        let err = check_plan(&plan("SELECT * FROM t WHERE s BETWEEN '1' AND 9").await).unwrap_err();
        assert!(
            err.to_string()
                .ends_with("Cannot check if varchar is BETWEEN varchar and bigint"),
            "{err}"
        );
    }
}
