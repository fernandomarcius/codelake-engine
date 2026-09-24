//! A view's query as Trino 431 prints it (`SqlFormatter`), for
//! `information_schema.views.view_definition`:
//!
//! ```text
//! SELECT
//!   id
//! , (dc * 2) dc2
//! FROM
//!   memory.sem_vd.t
//! ```
//!
//! One select item per line after the first when there are several, every
//! operator in parentheses, `AND`/`OR` chains flattened, joins nested left to
//! right, subqueries indented by three spaces, aliases without `AS`, decimal
//! literals as `DECIMAL '2.5'`, `||` as `concat`, `NOT IN` as `NOT (… IN …)`.
//! A query using anything this does not know is returned as written rather
//! than printed differently from Trino.

use std::fmt::Write as _;

use datafusion::sql::sqlparser::ast::{
    BinaryOperator, CaseWhen, CastKind, DataType, Distinct, DuplicateTreatment, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, LimitClause,
    ObjectName, OrderBy, OrderByExpr, OrderByKind, Query, Select, SelectItem, SetExpr, SetOperator,
    SetQuantifier, Statement, TableFactor, TableWithJoins, TimezoneInfo, UnaryOperator, Value, WindowType,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// A construct Trino's formatter would print, but this one does not know.
#[derive(Debug)]
struct Unsupported;

type Out<T = ()> = Result<T, Unsupported>;

const INDENT: &str = "   ";

/// `sql` (a query, or a `CREATE VIEW`) as Trino prints the view's query; the
/// text itself when it cannot be printed like Trino.
pub(crate) fn view_definition(sql: &str) -> String {
    let Ok(mut statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return sql.to_owned();
    };
    let query = match (statements.pop(), statements.is_empty()) {
        (Some(Statement::Query(query)), true) => query,
        (Some(Statement::CreateView(view)), true) => view.query,
        _ => return sql.to_owned(),
    };
    let mut formatter = Formatter::default();
    match formatter.query(&query, 0) {
        Ok(()) => formatter.out,
        Err(Unsupported) => sql.to_owned(),
    }
}

#[derive(Default)]
struct Formatter {
    out: String,
}

fn indent(level: usize) -> String {
    INDENT.repeat(level)
}

impl Formatter {
    fn append(&mut self, level: usize, text: &str) {
        self.out.push_str(&indent(level));
        self.out.push_str(text);
    }

    fn query(&mut self, query: &Query, level: usize) -> Out {
        if query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return Err(Unsupported);
        }
        if let Some(with) = &query.with {
            self.append(level, "WITH");
            if with.recursive {
                self.out.push_str(" RECURSIVE");
            }
            self.out.push_str("\n  ");
            for (i, cte) in with.cte_tables.iter().enumerate() {
                if cte.from.is_some() || cte.materialized.is_some() {
                    return Err(Unsupported);
                }
                self.append(level, &ident(&cte.alias.name));
                if !cte.alias.columns.is_empty() {
                    let columns: Vec<String> = cte.alias.columns.iter().map(|c| ident(&c.name)).collect();
                    let _ = write!(self.out, " ({})", columns.join(", "));
                }
                self.out.push_str(" AS ");
                self.table_subquery(&cte.query, level)?;
                self.out.push('\n');
                if i + 1 < with.cte_tables.len() {
                    self.out.push_str(", ");
                }
            }
        }
        self.relation(&query.body, level)?;
        if let Some(order_by) = &query.order_by {
            let text = order_by_text(order_by)?;
            self.append(level, &text);
            self.out.push('\n');
        }
        match &query.limit_clause {
            None => {}
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) if limit_by.is_empty() => {
                if let Some(offset) = offset {
                    let text = format!("OFFSET {} ROWS\n", expr(&offset.value)?);
                    self.append(level, &text);
                }
                if let Some(limit) = limit {
                    let text = format!("LIMIT {}\n", expr(limit)?);
                    self.append(level, &text);
                }
            }
            Some(_) => return Err(Unsupported),
        }
        Ok(())
    }

    fn relation(&mut self, body: &SetExpr, level: usize) -> Out {
        match body {
            SetExpr::Select(select) => self.query_specification(select, level),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                self.relation(left, level)?;
                self.out.push_str(match op {
                    SetOperator::Union => "UNION ",
                    SetOperator::Intersect => "INTERSECT ",
                    SetOperator::Except => "EXCEPT ",
                    SetOperator::Minus => return Err(Unsupported),
                });
                match set_quantifier {
                    SetQuantifier::All => self.out.push_str("ALL "),
                    SetQuantifier::Distinct | SetQuantifier::None => {}
                    _ => return Err(Unsupported),
                }
                self.relation(right, level)
            }
            SetExpr::Table(table) => {
                let name = match (&table.schema_name, &table.table_name) {
                    (Some(schema), Some(table)) => format!("{schema}.{table}"),
                    (None, Some(table)) => table.clone(),
                    _ => return Err(Unsupported),
                };
                let _ = writeln!(self.out, "TABLE {name}");
                Ok(())
            }
            _ => Err(Unsupported),
        }
    }

    fn query_specification(&mut self, select: &Select, level: usize) -> Out {
        if select.top.is_some()
            || select.exclude.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
            || select.select_modifiers.is_some()
        {
            return Err(Unsupported);
        }
        self.append(level, "SELECT");
        match &select.distinct {
            None | Some(Distinct::All) => {}
            Some(Distinct::Distinct) => self.out.push_str(" DISTINCT"),
            Some(Distinct::On(_)) => return Err(Unsupported),
        }
        let items = select
            .projection
            .iter()
            .map(select_item)
            .collect::<Out<Vec<_>>>()?;
        if let [only] = items.as_slice() {
            self.out.push(' ');
            self.out.push_str(only);
        } else {
            for (i, item) in items.iter().enumerate() {
                self.out.push('\n');
                self.out.push_str(&indent(level));
                self.out.push_str(if i == 0 { "  " } else { ", " });
                self.out.push_str(item);
            }
        }
        self.out.push('\n');
        if !select.from.is_empty() {
            self.append(level, "FROM");
            self.out.push('\n');
            self.append(level, "  ");
            self.from(&select.from, level)?;
        }
        self.out.push('\n');
        if let Some(selection) = &select.selection {
            let text = format!("WHERE {}", expr(selection)?);
            self.append(level, &text);
            self.out.push('\n');
        }
        match &select.group_by {
            GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
                if !exprs.is_empty() {
                    let text = format!("GROUP BY {}", exprs_text(exprs)?);
                    self.append(level, &text);
                    self.out.push('\n');
                }
            }
            _ => return Err(Unsupported),
        }
        if let Some(having) = &select.having {
            let text = format!("HAVING {}", expr(having)?);
            self.append(level, &text);
            self.out.push('\n');
        }
        Ok(())
    }

    /// `a, b` is an implicit join, nested left to right.
    fn from(&mut self, from: &[TableWithJoins], level: usize) -> Out {
        let Some((last, rest)) = from.split_last() else {
            return Ok(());
        };
        if rest.is_empty() {
            return self.table_with_joins(last, level);
        }
        self.from(rest, level)?;
        self.out.push('\n');
        self.append(level, ", ");
        self.table_with_joins(last, level)
    }

    fn table_with_joins(&mut self, table: &TableWithJoins, level: usize) -> Out {
        self.out.push_str(&"(".repeat(table.joins.len()));
        self.table_factor(&table.relation, level)?;
        for join in &table.joins {
            if join.global {
                return Err(Unsupported);
            }
            let (kind, constraint) = match &join.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) => ("INNER", Some(c)),
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => ("LEFT", Some(c)),
                JoinOperator::Right(c) | JoinOperator::RightOuter(c) => ("RIGHT", Some(c)),
                JoinOperator::FullOuter(c) => ("FULL", Some(c)),
                JoinOperator::CrossJoin(JoinConstraint::None) => ("CROSS", None),
                _ => return Err(Unsupported),
            };
            let natural = matches!(constraint, Some(JoinConstraint::Natural));
            self.out.push('\n');
            let text = format!("{}{kind} JOIN ", if natural { "NATURAL " } else { "" });
            self.append(level, &text);
            self.table_factor(&join.relation, level)?;
            match constraint {
                None | Some(JoinConstraint::Natural) => {}
                Some(JoinConstraint::On(on)) => {
                    let _ = write!(self.out, " ON {}", expr(on)?);
                }
                Some(JoinConstraint::Using(columns)) => {
                    let columns = columns.iter().map(object_name).collect::<Out<Vec<_>>>()?;
                    let _ = write!(self.out, " USING ({})", columns.join(", "));
                }
                Some(JoinConstraint::None) => return Err(Unsupported),
            }
            self.out.push(')');
        }
        Ok(())
    }

    fn table_factor(&mut self, factor: &TableFactor, level: usize) -> Out {
        let alias = match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                with_hints,
                version: None,
                with_ordinality: false,
                partitions,
                json_path: None,
                sample: None,
                index_hints,
            } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty() => {
                self.out.push_str(&object_name(name)?);
                alias
            }
            TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
                sample: None,
            } => {
                self.table_subquery(subquery, level)?;
                alias
            }
            _ => return Err(Unsupported),
        };
        if let Some(alias) = alias {
            let _ = write!(self.out, " {}", ident(&alias.name));
            if !alias.columns.is_empty() {
                let columns: Vec<String> = alias.columns.iter().map(|c| ident(&c.name)).collect();
                let _ = write!(self.out, "({})", columns.join(", "));
            }
        }
        Ok(())
    }

    fn table_subquery(&mut self, query: &Query, level: usize) -> Out {
        self.out.push_str("(\n");
        self.query(query, level + 1)?;
        self.append(level, ") ");
        Ok(())
    }
}

/// A subquery in an expression: the query printed at the left margin.
fn subquery(query: &Query) -> Out<String> {
    let mut formatter = Formatter::default();
    formatter.query(query, 0)?;
    Ok(format!("({})", formatter.out))
}

fn ident(ident: &Ident) -> String {
    match ident.quote_style {
        Some(_) => format!("\"{}\"", ident.value.replace('"', "\"\"")),
        None => ident.value.clone(),
    }
}

fn object_name(name: &ObjectName) -> Out<String> {
    let parts = name
        .0
        .iter()
        .map(|part| part.as_ident().map(ident).ok_or(Unsupported))
        .collect::<Out<Vec<_>>>()?;
    Ok(parts.join("."))
}

fn select_item(item: &SelectItem) -> Out<String> {
    Ok(match item {
        SelectItem::UnnamedExpr(e) => expr(e)?,
        SelectItem::ExprWithAlias { expr: e, alias } => format!("{} {}", expr(e)?, ident(alias)),
        SelectItem::Wildcard(options) if options.to_string().is_empty() => "*".into(),
        SelectItem::QualifiedWildcard(kind, options) if options.to_string().is_empty() => format!("{kind}"),
        _ => return Err(Unsupported),
    })
}

fn exprs_text(exprs: &[Expr]) -> Out<String> {
    Ok(exprs.iter().map(expr).collect::<Out<Vec<_>>>()?.join(", "))
}

fn order_by_item(item: &OrderByExpr) -> Out<String> {
    if item.with_fill.is_some() {
        return Err(Unsupported);
    }
    let mut text = expr(&item.expr)?;
    text.push_str(if item.options.asc == Some(false) {
        " DESC"
    } else {
        " ASC"
    });
    match item.options.nulls_first {
        Some(true) => text.push_str(" NULLS FIRST"),
        Some(false) => text.push_str(" NULLS LAST"),
        None => {}
    }
    Ok(text)
}

fn order_by_text(order_by: &OrderBy) -> Out<String> {
    let OrderByKind::Expressions(items) = &order_by.kind else {
        return Err(Unsupported);
    };
    if order_by.interpolate.is_some() {
        return Err(Unsupported);
    }
    let items = items.iter().map(order_by_item).collect::<Out<Vec<_>>>()?;
    Ok(format!("ORDER BY {}", items.join(", ")))
}

/// A `double` literal as Java's `DecimalFormat("0.###################E0")`.
fn double_literal(number: &str) -> Out<String> {
    let value: f64 = number.parse().map_err(|_| Unsupported)?;
    Ok(format!("{value:e}").replace('e', "E"))
}

fn value(value: &Value) -> Out<String> {
    Ok(match value {
        Value::Number(number, _) if number.contains(['e', 'E']) => double_literal(number)?,
        Value::Number(number, _) if number.contains('.') => format!("DECIMAL '{number}'"),
        Value::Number(number, _) => number.clone(),
        Value::SingleQuotedString(text) => format!("'{}'", text.replace('\'', "''")),
        Value::Boolean(b) => b.to_string(),
        Value::Null => "null".into(),
        _ => return Err(Unsupported),
    })
}

fn data_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Timestamp(precision, zone) => {
            let mut text = match precision {
                Some(p) => format!("timestamp({p})"),
                None => "timestamp".into(),
            };
            match zone {
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => {}
                TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => text.push_str(" with time zone"),
            }
            text
        }
        other => other.to_string().replace(',', ", "),
    }
}

/// Trino's name of a binary operator, `None` for `AND`/`OR`/`||`.
fn operator(op: &BinaryOperator) -> Option<&'static str> {
    Some(match op {
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::Eq => "=",
        BinaryOperator::NotEq => "<>",
        BinaryOperator::Lt => "<",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::Gt => ">",
        BinaryOperator::GtEq => ">=",
        _ => return None,
    })
}

/// The terms of an `AND` (or `OR`) chain.
fn terms<'a>(e: &'a Expr, op: &BinaryOperator, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::BinaryOp { left, op: o, right } if o == op => {
            terms(left, op, out);
            terms(right, op, out);
        }
        Expr::Nested(inner) if matches!(inner.as_ref(), Expr::BinaryOp { op: o, .. } if o == op) => {
            terms(inner, op, out);
        }
        other => out.push(other),
    }
}

fn not(text: &str) -> String {
    format!("(NOT {text})")
}

fn function(f: &Function) -> Out<String> {
    if f.uses_odbc_syntax
        || !matches!(f.parameters, FunctionArguments::None)
        || f.null_treatment.is_some()
        || !f.within_group.is_empty()
    {
        return Err(Unsupported);
    }
    let name = object_name(&f.name)?;
    let special = ["coalesce", "nullif"]
        .into_iter()
        .find(|s| name.eq_ignore_ascii_case(s));
    let name = special.map_or(name, str::to_uppercase);
    let args = match &f.args {
        FunctionArguments::None => String::new(),
        FunctionArguments::List(list) => {
            if !list.clauses.is_empty() {
                return Err(Unsupported);
            }
            let args = list
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr(e),
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok("*".into()),
                    _ => Err(Unsupported),
                })
                .collect::<Out<Vec<_>>>()?
                .join(", ");
            match list.duplicate_treatment {
                Some(DuplicateTreatment::Distinct) => format!("DISTINCT {args}"),
                Some(DuplicateTreatment::All) => return Err(Unsupported),
                None => args,
            }
        }
        FunctionArguments::Subquery(_) => return Err(Unsupported),
    };
    let mut text = format!("{name}({args})");
    if let Some(filter) = &f.filter {
        let _ = write!(text, " FILTER (WHERE {})", expr(filter)?);
    }
    match &f.over {
        None => {}
        Some(WindowType::WindowSpec(spec)) if spec.window_name.is_none() && spec.window_frame.is_none() => {
            let mut parts = Vec::new();
            if !spec.partition_by.is_empty() {
                parts.push(format!("PARTITION BY {}", exprs_text(&spec.partition_by)?));
            }
            if !spec.order_by.is_empty() {
                let items = spec.order_by.iter().map(order_by_item).collect::<Out<Vec<_>>>()?;
                parts.push(format!("ORDER BY {}", items.join(", ")));
            }
            let _ = write!(text, " OVER ({})", parts.join(" "));
        }
        Some(_) => return Err(Unsupported),
    }
    Ok(text)
}

fn like(inner: &Expr, pattern: &Expr, escape: Option<&Value>, negated: bool) -> Out<String> {
    let mut text = format!("({} LIKE {}", expr(inner)?, expr(pattern)?);
    if let Some(escape) = escape {
        let _ = write!(text, " ESCAPE {}", value(escape)?);
    }
    text.push(')');
    Ok(if negated { not(&text) } else { text })
}

fn cast(kind: &CastKind, inner: &Expr, target: &DataType) -> Out<String> {
    let name = match kind {
        CastKind::Cast => "CAST",
        CastKind::TryCast | CastKind::SafeCast => "TRY_CAST",
        CastKind::DoubleColon => return Err(Unsupported),
    };
    Ok(format!("{name}({} AS {})", expr(inner)?, data_type(target)))
}

fn case(operand: Option<&Expr>, conditions: &[CaseWhen], otherwise: Option<&Expr>) -> Out<String> {
    let mut text = String::from("(CASE");
    if let Some(operand) = operand {
        let _ = write!(text, " {}", expr(operand)?);
    }
    for when in conditions {
        let _ = write!(
            text,
            " WHEN {} THEN {}",
            expr(&when.condition)?,
            expr(&when.result)?
        );
    }
    if let Some(otherwise) = otherwise {
        let _ = write!(text, " ELSE {}", expr(otherwise)?);
    }
    text.push_str(" END)");
    Ok(text)
}

/// An expression as Trino's `ExpressionFormatter` prints it.
fn expr(e: &Expr) -> Out<String> {
    Ok(match e {
        Expr::Identifier(i) => ident(i),
        Expr::CompoundIdentifier(parts) => parts.iter().map(ident).collect::<Vec<_>>().join("."),
        Expr::Nested(inner) => expr(inner)?,
        Expr::Value(v) => value(&v.value)?,
        Expr::TypedString(typed) => {
            let name = match typed.data_type {
                DataType::Date => "DATE",
                DataType::Timestamp(None, TimezoneInfo::None) => "TIMESTAMP",
                _ => return Err(Unsupported),
            };
            let Some(text) = typed.value.clone().into_string() else {
                return Err(Unsupported);
            };
            format!("{name} '{}'", text.replace('\'', "''"))
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                let mut all = Vec::new();
                terms(e, op, &mut all);
                let joiner = if *op == BinaryOperator::And {
                    " AND "
                } else {
                    " OR "
                };
                format!(
                    "({})",
                    all.into_iter().map(expr).collect::<Out<Vec<_>>>()?.join(joiner)
                )
            }
            BinaryOperator::StringConcat => format!("concat({}, {})", expr(left)?, expr(right)?),
            other => {
                let symbol = operator(other).ok_or(Unsupported)?;
                format!("({} {symbol} {})", expr(left)?, expr(right)?)
            }
        },
        Expr::UnaryOp { op, expr: inner } => match op {
            UnaryOperator::Not => not(&expr(inner)?),
            UnaryOperator::Minus => format!("-{}", expr(inner)?),
            UnaryOperator::Plus => format!("+{}", expr(inner)?),
            _ => return Err(Unsupported),
        },
        Expr::IsNull(inner) => format!("({} IS NULL)", expr(inner)?),
        Expr::IsNotNull(inner) => format!("({} IS NOT NULL)", expr(inner)?),
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let text = format!("({} IN ({}))", expr(inner)?, exprs_text(list)?);
            if *negated { not(&text) } else { text }
        }
        Expr::InSubquery {
            expr: inner,
            subquery: query,
            negated,
        } => {
            let text = format!("({} IN {})", expr(inner)?, subquery(query)?);
            if *negated { not(&text) } else { text }
        }
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let text = format!("({} BETWEEN {} AND {})", expr(inner)?, expr(low)?, expr(high)?);
            if *negated { not(&text) } else { text }
        }
        Expr::Like {
            negated,
            any: false,
            expr: inner,
            pattern,
            escape_char,
        } => like(inner, pattern, escape_char.as_ref().map(|c| &c.value), *negated)?,
        Expr::Cast {
            kind,
            expr: inner,
            data_type: target,
            array: false,
            format: None,
        } => cast(kind, inner, target)?,
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => case(operand.as_deref(), conditions, else_result.as_deref())?,
        Expr::Function(f) => function(f)?,
        Expr::Subquery(query) => subquery(query)?,
        Expr::Exists {
            subquery: query,
            negated,
        } => {
            let text = format!("(EXISTS {})", subquery(query)?);
            if *negated { not(&text) } else { text }
        }
        _ => return Err(Unsupported),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Texts observed on Trino 431 (`information_schema.views`).
    #[test]
    fn views_print_like_trino() {
        let cases = [
            (
                "SELECT id, dc * 2 AS dc2 FROM memory.sem_vd.t",
                "SELECT\n  id\n, (dc * 2) dc2\nFROM\n  memory.sem_vd.t\n",
            ),
            (
                "SELECT * FROM memory.sem_f.t WHERE id > 1 AND dc < 2.5 OR NOT (id = 3)",
                "SELECT *\nFROM\n  memory.sem_f.t\nWHERE (((id > 1) AND (dc < DECIMAL '2.5')) OR (NOT (id = 3)))\n",
            ),
            (
                "select a.id, b.id as bid from memory.sem_f.t a join memory.sem_f.t b on a.id = b.id \
                 left join memory.sem_f.t c on c.id = a.id",
                "SELECT\n  a.id\n, b.id bid\nFROM\n  ((memory.sem_f.t a\nINNER JOIN memory.sem_f.t b ON (a.id = b.id))\n\
                 LEFT JOIN memory.sem_f.t c ON (c.id = a.id))\n",
            ),
            (
                "SELECT CAST(id AS VARCHAR) AS s, CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'many' END AS w, \
                 coalesce(dc, 0) c FROM memory.sem_f.t",
                "SELECT\n  CAST(id AS VARCHAR) s\n, (CASE WHEN (id = 1) THEN 'one' WHEN (id = 2) THEN 'two' ELSE 'many' END) w\n\
                 , COALESCE(dc, 0) c\nFROM\n  memory.sem_f.t\n",
            ),
            (
                "WITH x AS (SELECT id FROM memory.sem_f.t) SELECT id FROM x UNION ALL SELECT 1",
                "WITH\n  x AS (\n   SELECT id\n   FROM\n     memory.sem_f.t\n) \nSELECT id\nFROM\n  x\nUNION ALL SELECT 1\n\n",
            ),
            (
                "SELECT id FROM memory.sem_f.t WHERE id IN (1, 2, 3) AND dc BETWEEN 1 AND 2 AND v LIKE 'a%' AND v IS NOT NULL",
                "SELECT id\nFROM\n  memory.sem_f.t\nWHERE ((id IN (1, 2, 3)) AND (dc BETWEEN 1 AND 2) AND (v LIKE 'a%') AND (v IS NOT NULL))\n",
            ),
            (
                "SELECT \"ID\", \"Mixed Case\" FROM (SELECT id AS \"ID\", 1 AS \"Mixed Case\" FROM memory.sem_f.t) q",
                "SELECT\n  \"ID\"\n, \"Mixed Case\"\nFROM\n  (\n   SELECT\n     id \"ID\"\n   , 1 \"Mixed Case\"\n   \
                 FROM\n     memory.sem_f.t\n)  q\n",
            ),
            (
                "SELECT id, row_number() OVER (PARTITION BY v ORDER BY id) rn, avg(dc) OVER () a FROM memory.sem_f.t",
                "SELECT\n  id\n, row_number() OVER (PARTITION BY v ORDER BY id ASC) rn\n, avg(dc) OVER () a\nFROM\n  memory.sem_f.t\n",
            ),
            (
                "SELECT id FROM memory.sem_f.t WHERE EXISTS (SELECT 1 FROM memory.sem_f.t u WHERE u.id = t.id) \
                 AND id = (SELECT max(id) FROM memory.sem_f.t)",
                "SELECT id\nFROM\n  memory.sem_f.t\nWHERE ((EXISTS (SELECT 1\nFROM\n  memory.sem_f.t u\nWHERE (u.id = t.id)\n)) \
                 AND (id = (SELECT max(id)\nFROM\n  memory.sem_f.t\n)))\n",
            ),
            (
                "SELECT DISTINCT v FROM memory.sem_f.t ORDER BY v NULLS FIRST OFFSET 1 LIMIT 2",
                "SELECT DISTINCT v\nFROM\n  memory.sem_f.t\nORDER BY v ASC NULLS FIRST\nOFFSET 1 ROWS\nLIMIT 2\n",
            ),
            (
                "SELECT id FROM memory.sem_g.t WHERE id NOT IN (1, 2) AND v NOT LIKE 'a%' AND id <> 3 AND id != 4",
                "SELECT id\nFROM\n  memory.sem_g.t\nWHERE ((NOT (id IN (1, 2))) AND (NOT (v LIKE 'a%')) AND (id <> 3) AND (id <> 4))\n",
            ),
            (
                "SELECT a.id FROM memory.sem_g.t a, memory.sem_g.t b CROSS JOIN memory.sem_g.t c WHERE a.id = b.id",
                "SELECT a.id\nFROM\n  memory.sem_g.t a\n, (memory.sem_g.t b\nCROSS JOIN memory.sem_g.t c)\nWHERE (a.id = b.id)\n",
            ),
            (
                "SELECT v || 'x' AS c, -1 AS m, 1e-20 AS d, 2.50 AS dec, nullif(id, 1) n, CASE id WHEN 1 THEN 'a' END s \
                 FROM memory.sem_g.t",
                "SELECT\n  concat(v, 'x') c\n, -1 m\n, 1E-20 d\n, DECIMAL '2.50' dec\n, NULLIF(id, 1) n\n\
                 , (CASE id WHEN 1 THEN 'a' END) s\nFROM\n  memory.sem_g.t\n",
            ),
            (
                "SELECT id FROM memory.sem_g.t UNION SELECT id FROM memory.sem_g.t INTERSECT SELECT 1",
                "SELECT id\nFROM\n  memory.sem_g.t\nUNION SELECT id\nFROM\n  memory.sem_g.t\nINTERSECT SELECT 1\n\n",
            ),
            (
                "SELECT count(DISTINCT id) cd, max(id) FILTER (WHERE id > 1) m, 1.5E0 i, 123456789.25E0 j FROM t",
                "SELECT\n  count(DISTINCT id) cd\n, max(id) FILTER (WHERE (id > 1)) m\n, 1.5E0 i\n, 1.2345678925E8 j\nFROM\n  t\n",
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(view_definition(sql), expected, "{sql}");
        }
        let create = "CREATE VIEW v AS SELECT id FROM t";
        assert_eq!(view_definition(create), "SELECT id\nFROM\n  t\n");
        // Not printed like Trino: kept as written.
        let unknown = "SELECT id FROM t QUALIFY row_number() OVER () = 1";
        assert_eq!(view_definition(unknown), unknown);
    }
}
