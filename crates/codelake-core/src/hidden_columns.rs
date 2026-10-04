//! Hidden columns of Hive tables, as the reference engine's Hive connector
//! has them: `"$path"` (the URI of the data file a row comes from),
//! `"$file_size"` (its bytes, `bigint`) and `"$file_modified_time"` (when it
//! was last written, `timestamp(3) with time zone` in UTC). A query reads,
//! filters, groups and joins by them like any column; `*`, `DESCRIBE`,
//! `SHOW COLUMNS` and `information_schema` never show them. `"$partition"`
//! and `"$bucket"` exist only on partitioned or bucketed tables, which the
//! Hive connector does not read: they are unknown columns there, as in the
//! reference engine on a table without partitions or buckets.
//!
//! The planner has no hidden columns: every column of a relation is in its
//! `*`. So a catalog serves a table with its hidden columns only under a
//! second name ([`with_hidden_columns`]: `t` → `t$hidden`), and [`expose`]
//! rewrites a statement before it is planned:
//!
//! - a `SELECT` that names a hidden column in one of its clauses (a subquery
//!   of them included: a correlated reference names a column of the outer
//!   `SELECT`) reads each table of its `FROM` that has hidden columns under
//!   that name, with the alias it was written with (`FROM hive.s.t` →
//!   `FROM hive.s."t$hidden" AS t`); a `SELECT` that names none is planned as
//!   before, and so is any statement that names none;
//! - its wildcards leave the hidden columns out: `*` and `t.*` get
//!   `EXCLUDE ("$path", "$file_size", "$file_modified_time")`, and a `*` over
//!   several such tables becomes one `x.*` per relation of the `FROM`.
//!
//! A table its catalog serves without hidden columns (another connector, a
//! view, a CTE) is left as it is: naming a hidden column there fails as any
//! unknown column does, as in the reference engine.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::sqlparser::ast::{
    ExcludeSelectItem, Expr, Ident, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, OrderBy, Query,
    Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins, Visit, VisitMut, Visitor, VisitorMut, WildcardAdditionalOptions, visit_expressions,
};

/// URI of the data file a row was read from (`varchar`).
pub const PATH: &str = "$path";
/// Size of that file in bytes (`bigint`).
pub const FILE_SIZE: &str = "$file_size";
/// When that file was last modified (`timestamp(3) with time zone`, UTC).
pub const FILE_MODIFIED_TIME: &str = "$file_modified_time";
/// The hidden columns of a Hive table, in the reference engine's order.
pub const HIDDEN_COLUMNS: [&str; 3] = [PATH, FILE_SIZE, FILE_MODIFIED_TIME];
/// Suffix of the name a catalog serves a table under with its hidden columns.
pub const SUFFIX: &str = "$hidden";

/// The name a catalog serves `table` under with its hidden columns.
#[must_use]
pub fn with_hidden_columns(table: &str) -> String {
    format!("{table}{SUFFIX}")
}

/// The table `name` asks for with its hidden columns, if it does.
#[must_use]
pub fn hidden_columns_of(name: &str) -> Option<&str> {
    name.strip_suffix(SUFFIX).filter(|table| !table.is_empty())
}

/// Whether `ident` names a hidden column (identifiers are case-insensitive).
fn is_hidden(ident: &Ident) -> bool {
    HIDDEN_COLUMNS
        .iter()
        .any(|hidden| ident.value.eq_ignore_ascii_case(hidden))
}

/// Whether an expression in `node` names a hidden column, alone or
/// qualified (`"$path"`, `t."$path"`).
fn names_hidden<V: Visit>(node: &V) -> bool {
    visit_expressions(node, |expr| match expr {
        Expr::Identifier(ident) if is_hidden(ident) => ControlFlow::Break(()),
        Expr::CompoundIdentifier(parts) if parts.last().is_some_and(is_hidden) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// The constraint of a join, if it has one.
fn join_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint)
        | JoinOperator::AsOf { constraint, .. } => Some(constraint),
        _ => None,
    }
}

/// Whether the `FROM` part `table` names a hidden column of its `SELECT`: in
/// a join condition, or in a relation that sees the tables before it
/// (`LATERAL`, `UNNEST`).
fn from_names_hidden(table: &TableWithJoins) -> bool {
    factor_names_hidden(&table.relation)
        || table.joins.iter().any(|join| {
            factor_names_hidden(&join.relation)
                || matches!(join_constraint(&join.join_operator), Some(JoinConstraint::On(on)) if names_hidden(on))
        })
}

fn factor_names_hidden(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Derived {
            lateral: true,
            subquery,
            ..
        } => names_hidden(subquery),
        TableFactor::UNNEST { array_exprs, .. } => names_hidden(array_exprs),
        TableFactor::NestedJoin { table_with_joins, .. } => from_names_hidden(table_with_joins),
        _ => false,
    }
}

/// Whether `select` (with the `ORDER BY` of its query, when it is the whole
/// query) names a hidden column.
fn scope_names_hidden(select: &Select, order_by: Option<&OrderBy>) -> bool {
    names_hidden(&select.projection)
        || names_hidden(&select.selection)
        || names_hidden(&select.group_by)
        || names_hidden(&select.having)
        || names_hidden(&select.qualify)
        || names_hidden(&select.named_window)
        || select.from.iter().any(from_names_hidden)
        || order_by.is_some_and(names_hidden)
}

/// `ident` as the planner resolves it: an unquoted name folds to lower case.
fn folded(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// The parts of `name` as the planner resolves them; `None` if one is not an
/// identifier.
fn folded_parts(name: &ObjectName) -> Option<Vec<String>> {
    name.0.iter().map(|part| part.as_ident().map(folded)).collect()
}

/// Whether `name` is one of the statement's CTEs.
fn is_cte(name: &ObjectName, ctes: &HashSet<String>) -> bool {
    matches!(folded_parts(name).as_deref(), Some([single]) if ctes.contains(single))
}

/// The tables of a `FROM` that may have hidden columns: named tables (not
/// table functions), in nested joins too. A CTE of the same name is told
/// apart when the tables are switched ([`Expose`]).
fn candidates<'a>(from: &'a [TableWithJoins], out: &mut Vec<&'a ObjectName>) {
    for table in from {
        for factor in std::iter::once(&table.relation).chain(table.joins.iter().map(|join| &join.relation)) {
            match factor {
                TableFactor::Table { name, args: None, .. } => out.push(name),
                TableFactor::NestedJoin { table_with_joins, .. } => {
                    candidates(std::slice::from_ref(table_with_joins), out);
                }
                _ => {}
            }
        }
    }
}

/// The `SELECT`s of a query body (set operations included; nested queries
/// are queries of their own).
fn selects<'a>(body: &'a SetExpr, out: &mut Vec<&'a Select>) {
    match body {
        SetExpr::Select(select) => out.push(select.as_ref()),
        SetExpr::SetOperation { left, right, .. } => {
            selects(left, out);
            selects(right, out);
        }
        _ => {}
    }
}

/// The `SELECT`s of a query body, as [`selects`], to change them.
fn selects_mut<'a>(body: &'a mut SetExpr, out: &mut Vec<&'a mut Select>) {
    match body {
        SetExpr::Select(select) => out.push(select.as_mut()),
        SetExpr::SetOperation { left, right, .. } => {
            selects_mut(left, out);
            selects_mut(right, out);
        }
        _ => {}
    }
}

/// The names of the statement's CTEs, folded.
#[derive(Default)]
struct Ctes(HashSet<String>);

impl Visitor for Ctes {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            self.0
                .extend(with.cte_tables.iter().map(|cte| folded(&cte.alias.name)));
        }
        ControlFlow::Continue(())
    }
}

/// First pass: the tables of the `SELECT`s that name a hidden column, and
/// the refusal of a name that asks for hidden columns itself.
#[derive(Default)]
struct Scan {
    candidates: Vec<ObjectName>,
}

impl Visitor for Scan {
    type Break = DataFusionError;

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<DataFusionError> {
        // The second name of a table is the engine's, never a client's: a
        // client asking for it gets what the reference engine answers.
        let asks = relation
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .is_some_and(|ident| hidden_columns_of(&ident.value.to_lowercase()).is_some());
        if asks {
            return ControlFlow::Break(DataFusionError::Plan(format!("table '{relation}' not found")));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<DataFusionError> {
        let order_by = match query.body.as_ref() {
            SetExpr::Select(_) => query.order_by.as_ref(),
            _ => None,
        };
        let mut scopes = Vec::new();
        selects(&query.body, &mut scopes);
        for select in scopes {
            if scope_names_hidden(select, order_by) {
                let mut names = Vec::new();
                candidates(&select.from, &mut names);
                self.candidates.extend(names.into_iter().cloned());
            }
        }
        ControlFlow::Continue(())
    }
}

/// The candidates whose catalog serves them with hidden columns, by their
/// name as written.
async fn with_hidden(state: &SessionState, names: Vec<ObjectName>) -> HashSet<String> {
    let normalize = state.config().options().sql_parser.enable_ident_normalization;
    let mut answers: HashMap<String, bool> = HashMap::new();
    for name in names {
        let key = name.to_string();
        if answers.contains_key(&key) {
            continue;
        }
        let mut has = false;
        if let Ok(reference) = object_name_to_table_reference(name, normalize) {
            let table = with_hidden_columns(reference.table());
            if let Ok(schema) = state.schema_for_ref(reference) {
                // Any error is an answer of "no": the statement is then
                // planned as written, and fails as it would.
                has = matches!(schema.table(&table).await, Ok(Some(_)));
            }
        }
        answers.insert(key, has);
    }
    answers
        .into_iter()
        .filter_map(|(name, has)| has.then_some(name))
        .collect()
}

/// A table a `SELECT` now reads with its hidden columns.
#[derive(Debug, Clone)]
struct Switched {
    /// The alias the table is read under (the one written, or its name).
    alias: Ident,
    /// Its name as written, folded, when it had no alias: a wildcard
    /// qualified by several of its last parts (`s.t.*`) names it too.
    name: Option<Vec<String>>,
}

impl Switched {
    /// Whether the qualifier `parts` (folded) names this table.
    fn qualified_by(&self, parts: &[String]) -> bool {
        match parts {
            [] => false,
            [single] => *single == folded(&self.alias),
            _ => self.name.as_ref().is_some_and(|name| name.ends_with(parts)),
        }
    }
}

/// The hidden columns as wildcard exclusions.
fn hidden_exclusions() -> Vec<ObjectName> {
    HIDDEN_COLUMNS
        .iter()
        .map(|column| ObjectName::from(vec![Ident::with_quote('"', *column)]))
        .collect()
}

/// `options` that also exclude the hidden columns.
fn excluding(mut options: WildcardAdditionalOptions) -> WildcardAdditionalOptions {
    let mut excluded = match options.opt_exclude.take() {
        None => Vec::new(),
        Some(ExcludeSelectItem::Single(name)) => vec![name],
        Some(ExcludeSelectItem::Multiple(names)) => names,
    };
    excluded.extend(hidden_exclusions());
    options.opt_exclude = Some(ExcludeSelectItem::Multiple(excluded));
    options
}

/// Whether the table `name` was switched to its hidden columns.
fn is_switched(name: &ObjectName) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .is_some_and(|ident| hidden_columns_of(&ident.value).is_some())
}

/// Why a `*` over several tables read with their hidden columns cannot be
/// written as one wildcard per relation.
fn several_tables(reason: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "SELECT * over several tables whose hidden columns ({}) the query reads, {reason}: \
         qualify the wildcard (t.*) or list the columns",
        HIDDEN_COLUMNS.join(", ")
    ))
}

/// The relations of `from` in the order `*` lists their columns, each as a
/// wildcard qualifier and whether it was switched.
fn relations(from: &[TableWithJoins], out: &mut Vec<(ObjectName, bool)>) -> Result<()> {
    for table in from {
        relation(&table.relation, out)?;
        for join in &table.joins {
            if matches!(
                join_constraint(&join.join_operator),
                Some(JoinConstraint::Using(_) | JoinConstraint::Natural)
            ) {
                return Err(several_tables("with JOIN … USING"));
            }
            relation(&join.relation, out)?;
        }
    }
    Ok(())
}

fn relation(factor: &TableFactor, out: &mut Vec<(ObjectName, bool)>) -> Result<()> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let qualifier = alias.as_ref().map_or_else(
                || name.clone(),
                |alias| ObjectName::from(vec![alias.name.clone()]),
            );
            out.push((qualifier, is_switched(name)));
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias: None,
        } => relations(std::slice::from_ref(table_with_joins), out)?,
        TableFactor::Derived {
            alias: Some(alias), ..
        }
        | TableFactor::UNNEST {
            alias: Some(alias), ..
        }
        | TableFactor::Function {
            alias: Some(alias), ..
        }
        | TableFactor::TableFunction {
            alias: Some(alias), ..
        } => out.push((ObjectName::from(vec![alias.name.clone()]), false)),
        _ => return Err(several_tables("with a relation that has no name")),
    }
    Ok(())
}

/// The wildcards of `select` without the hidden columns of the tables in
/// `switched`.
fn exclude_hidden(select: &mut Select, switched: &[Switched]) -> Result<()> {
    let mut items = Vec::with_capacity(select.projection.len());
    for item in std::mem::take(&mut select.projection) {
        match item {
            SelectItem::Wildcard(options) if switched.len() == 1 => {
                items.push(SelectItem::Wildcard(excluding(options)));
            }
            SelectItem::Wildcard(options) => {
                if !options.to_string().is_empty() {
                    return Err(several_tables("with wildcard options"));
                }
                let mut listed = Vec::new();
                relations(&select.from, &mut listed)?;
                for (qualifier, hidden) in listed {
                    let options = if hidden {
                        excluding(WildcardAdditionalOptions::default())
                    } else {
                        WildcardAdditionalOptions::default()
                    };
                    items.push(SelectItem::QualifiedWildcard(
                        SelectItemQualifiedWildcardKind::ObjectName(qualifier),
                        options,
                    ));
                }
            }
            SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(qualifier),
                options,
            ) => {
                let table = folded_parts(&qualifier)
                    .and_then(|parts| switched.iter().find(|table| table.qualified_by(&parts)));
                items.push(match table {
                    Some(table) => SelectItem::QualifiedWildcard(
                        SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![
                            table.alias.clone(),
                        ])),
                        excluding(options),
                    ),
                    None => SelectItem::QualifiedWildcard(
                        SelectItemQualifiedWildcardKind::ObjectName(qualifier),
                        options,
                    ),
                });
            }
            other => items.push(other),
        }
    }
    select.projection = items;
    Ok(())
}

/// Second pass: switches the tables found by [`Scan`] that have hidden
/// columns, and leaves those columns out of the wildcards.
struct Expose<'a> {
    ctes: &'a HashSet<String>,
    tables: &'a HashSet<String>,
}

impl Expose<'_> {
    fn switch_from(&self, from: &mut [TableWithJoins], switched: &mut Vec<Switched>) {
        for table in from {
            self.switch(&mut table.relation, switched);
            for join in &mut table.joins {
                self.switch(&mut join.relation, switched);
            }
        }
    }

    fn switch(&self, factor: &mut TableFactor, switched: &mut Vec<Switched>) {
        match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } if !is_cte(name, self.ctes) && self.tables.contains(&name.to_string()) => {
                let full = folded_parts(name);
                let Some(ObjectNamePart::Identifier(last)) = name.0.last_mut() else {
                    return;
                };
                let written = last.clone();
                *last = Ident::with_quote('"', with_hidden_columns(&folded(&written)));
                let unaliased = alias.is_none();
                let alias = alias.get_or_insert_with(|| TableAlias {
                    explicit: true,
                    name: written,
                    columns: Vec::new(),
                    at: None,
                });
                switched.push(Switched {
                    alias: alias.name.clone(),
                    name: if unaliased { full } else { None },
                });
            }
            TableFactor::NestedJoin { table_with_joins, .. } => {
                self.switch_from(std::slice::from_mut(table_with_joins.as_mut()), switched);
            }
            _ => {}
        }
    }

    fn scope(&self, select: &mut Select, order_by: Option<&OrderBy>) -> Result<()> {
        if !scope_names_hidden(select, order_by) {
            return Ok(());
        }
        let mut switched = Vec::new();
        self.switch_from(&mut select.from, &mut switched);
        if switched.is_empty() {
            return Ok(());
        }
        exclude_hidden(select, &switched)
    }
}

impl VisitorMut for Expose<'_> {
    type Break = DataFusionError;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<DataFusionError> {
        let Query { body, order_by, .. } = query;
        let result = match body.as_mut() {
            SetExpr::Select(select) => self.scope(select, order_by.as_ref()),
            other => {
                let mut scopes = Vec::new();
                selects_mut(other, &mut scopes);
                scopes.into_iter().try_for_each(|select| self.scope(select, None))
            }
        };
        match result {
            Ok(()) => ControlFlow::Continue(()),
            Err(err) => ControlFlow::Break(err),
        }
    }
}

/// Whether `statement` names a hidden column anywhere: only such a
/// statement needs [`expose`].
#[must_use]
pub fn names_hidden_columns(statement: &Statement) -> bool {
    names_hidden(statement)
}

/// The statement inside `statement` (an `EXPLAIN`'s, …), if it is SQL.
fn sql_statement(statement: &mut DFStatement) -> Option<&mut Statement> {
    match statement {
        DFStatement::Statement(inner) => Some(inner),
        DFStatement::Explain(explain) => sql_statement(&mut explain.statement),
        _ => None,
    }
}

/// Rewrites `statement` so that its `SELECT`s that name a hidden column read
/// the hidden columns of their tables (see the module documentation).
/// Applied right before planning to every statement the engine plans, and so
/// to the SQL of a view a connector plans itself.
///
/// # Errors
///
/// The statement names a table by the name the engine reads its hidden
/// columns under (`t$hidden`: not found, as in the reference engine), or has
/// a `*` over several tables whose hidden columns it reads that cannot be
/// written as one wildcard per relation (`JOIN … USING`, a relation without
/// a name).
pub async fn expose(state: &SessionState, statement: &mut DFStatement) -> Result<()> {
    let Some(statement) = sql_statement(statement) else {
        return Ok(());
    };
    let mut scan = Scan::default();
    if let ControlFlow::Break(err) = (&*statement).visit(&mut scan) {
        return Err(err);
    }
    if scan.candidates.is_empty() {
        return Ok(());
    }
    let tables = with_hidden(state, scan.candidates).await;
    if tables.is_empty() {
        return Ok(());
    }
    let mut ctes = Ctes::default();
    let _ = (&*statement).visit(&mut ctes);
    let mut expose = Expose {
        ctes: &ctes.0,
        tables: &tables,
    };
    match statement.visit(&mut expose) {
        ControlFlow::Break(err) => Err(err),
        ControlFlow::Continue(()) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::TableReference;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::parser::DFParser;
    use datafusion::sql::sqlparser::dialect::GenericDialect;

    use super::*;

    /// A table `name` of `columns` (integers), and the same table with
    /// hidden columns under its second name: rows in files `f1`, `f1`, `f2`…
    fn register(ctx: &SessionContext, name: &str, columns: &[(&str, Vec<i32>)], hidden: bool) {
        let rows = columns[0].1.len();
        let fields: Vec<Field> = columns
            .iter()
            .map(|(column, _)| Field::new(*column, DataType::Int32, true))
            .collect();
        let values: Vec<ArrayRef> = columns
            .iter()
            .map(|(_, values)| Arc::new(Int32Array::from(values.clone())) as ArrayRef)
            .collect();
        let schema = Arc::new(Schema::new(fields.clone()));
        let batch = RecordBatch::try_new(Arc::clone(&schema), values.clone()).unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(TableReference::bare(name), Arc::new(table))
            .unwrap();
        if !hidden {
            return;
        }
        let files: Vec<String> = (0..rows).map(|row| format!("f{}", row / 2 + 1)).collect();
        let sizes: Vec<i64> = (0..rows)
            .map(|row| 10 * (i64::try_from(row).unwrap() / 2 + 1))
            .collect();
        let times: Vec<i64> = (0..rows)
            .map(|row| 1_000 * (i64::try_from(row).unwrap() / 2 + 1))
            .collect();
        let mut fields = fields;
        fields.extend([
            Field::new(PATH, DataType::Utf8, true),
            Field::new(FILE_SIZE, DataType::Int64, true),
            Field::new(
                FILE_MODIFIED_TIME,
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
        ]);
        let mut values = values;
        values.extend([
            Arc::new(StringArray::from(files)) as ArrayRef,
            Arc::new(Int64Array::from(sizes)) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(times).with_timezone("UTC")) as ArrayRef,
        ]);
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), values).unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(TableReference::bare(with_hidden_columns(name)), Arc::new(table))
            .unwrap();
    }

    /// `t(a, b)` and `u(k)` have hidden columns, `v(a, c)` has none.
    fn context() -> SessionContext {
        let ctx = SessionContext::new();
        register(&ctx, "t", &[("a", vec![1, 2, 3]), ("b", vec![10, 20, 30])], true);
        register(&ctx, "u", &[("k", vec![1, 3])], true);
        register(&ctx, "v", &[("a", vec![1, 2]), ("c", vec![5, 6])], false);
        ctx
    }

    fn parse(sql: &str) -> DFStatement {
        DFParser::parse_sql_with_dialect(sql, &GenericDialect {})
            .unwrap()
            .pop_front()
            .unwrap()
    }

    /// `sql` as [`expose`] rewrites it.
    async fn exposed(ctx: &SessionContext, sql: &str) -> Result<String> {
        let mut statement = parse(sql);
        expose(&ctx.state(), &mut statement).await?;
        Ok(statement.to_string())
    }

    /// The answer to `sql`, after [`expose`], as a table.
    async fn run(ctx: &SessionContext, sql: &str) -> Result<String> {
        let mut statement = parse(sql);
        let state = ctx.state();
        expose(&state, &mut statement).await?;
        let plan = state.statement_to_plan(statement).await?;
        let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
        Ok(pretty_format_batches(&batches)?.to_string())
    }

    fn lines(table: &str) -> Vec<&str> {
        table.lines().filter(|line| !line.starts_with('+')).collect()
    }

    #[tokio::test]
    async fn a_statement_without_hidden_columns_is_left_as_written() {
        let ctx = context();
        for sql in [
            "SELECT * FROM t",
            "SELECT t.* FROM t JOIN u ON t.a = u.k",
            "SELECT a FROM (SELECT * FROM t) x WHERE a > 1 ORDER BY a",
        ] {
            assert_eq!(exposed(&ctx, sql).await.unwrap(), parse(sql).to_string(), "{sql}");
        }
        assert_eq!(
            lines(&run(&ctx, "SELECT * FROM t ORDER BY a").await.unwrap())[0],
            "| a | b  |"
        );
    }

    #[tokio::test]
    async fn star_never_shows_the_hidden_columns_named_columns_read_them() {
        let ctx = context();
        assert_eq!(
            exposed(&ctx, "SELECT *, \"$path\" FROM t").await.unwrap(),
            "SELECT * EXCLUDE (\"$path\", \"$file_size\", \"$file_modified_time\"), \"$path\" \
             FROM \"t$hidden\" AS t"
        );
        assert_eq!(
            lines(&run(&ctx, "SELECT *, \"$path\" FROM t ORDER BY a").await.unwrap()),
            [
                "| a | b  | $path |",
                "| 1 | 10 | f1    |",
                "| 2 | 20 | f1    |",
                "| 3 | 30 | f2    |",
            ]
        );
        assert_eq!(
            lines(
                &run(
                    &ctx,
                    "SELECT x.*, \"$file_size\", x.\"$file_modified_time\" FROM t AS x WHERE \"$path\" = 'f2'"
                )
                .await
                .unwrap()
            ),
            [
                "| a | b  | $file_size | $file_modified_time  |",
                "| 3 | 30 | 20         | 1970-01-01T00:00:02Z |",
            ]
        );
    }

    #[tokio::test]
    async fn every_clause_can_name_a_hidden_column() {
        let ctx = context();
        let cases = [
            (
                "SELECT a FROM t WHERE \"$path\" = 'f1' ORDER BY a",
                vec!["| a |", "| 1 |", "| 2 |"],
            ),
            (
                "SELECT \"$path\", count(*) AS n FROM t GROUP BY \"$path\" HAVING max(\"$file_size\") > 15",
                vec!["| $path | n |", "| f2    | 1 |"],
            ),
            (
                "SELECT a FROM t ORDER BY \"$path\" DESC, a LIMIT 1",
                vec!["| a |", "| 3 |"],
            ),
            (
                "SELECT x.a, y.k FROM t x JOIN u y ON x.\"$path\" = y.\"$path\" ORDER BY 1, 2",
                vec!["| a | k |", "| 1 | 1 |", "| 1 | 3 |", "| 2 | 1 |", "| 2 | 3 |"],
            ),
            (
                "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.\"$path\" = t.\"$path\" AND u.k <> t.a) ORDER BY a",
                vec!["| a |", "| 1 |", "| 2 |"],
            ),
            (
                "SELECT \"$path\" FROM t WHERE a = 3 UNION ALL SELECT \"$path\" FROM u",
                vec!["| $path |", "| f2    |", "| f1    |", "| f1    |"],
            ),
            (
                "SELECT public.t.a, \"$path\" FROM public.t WHERE public.t.\"$file_size\" = 20",
                vec!["| a | $path |", "| 3 | f2    |"],
            ),
            (
                "SELECT public.t.*, \"$path\" FROM datafusion.public.t WHERE a = 1",
                vec!["| a | b  | $path |", "| 1 | 10 | f1    |"],
            ),
            (
                "SELECT x.\"$path\" FROM (SELECT \"$path\" FROM t WHERE a = 1) x",
                vec!["| $path |", "| f1    |"],
            ),
        ];
        for (sql, expected) in cases {
            let answer = run(&ctx, sql).await.unwrap_or_else(|err| panic!("{sql}: {err}"));
            assert_eq!(lines(&answer), expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn star_over_several_tables_lists_each_relation_without_hidden_columns() {
        let ctx = context();
        let sql = "SELECT * FROM t x JOIN u y ON x.\"$path\" = y.\"$path\" AND x.a = y.k, v ORDER BY 1, 4";
        assert_eq!(
            exposed(&ctx, sql).await.unwrap(),
            "SELECT x.* EXCLUDE (\"$path\", \"$file_size\", \"$file_modified_time\"), \
             y.* EXCLUDE (\"$path\", \"$file_size\", \"$file_modified_time\"), v.* \
             FROM \"t$hidden\" x JOIN \"u$hidden\" y ON x.\"$path\" = y.\"$path\" AND x.a = y.k, v \
             ORDER BY 1, 4"
        );
        assert_eq!(
            lines(&run(&ctx, sql).await.unwrap()),
            [
                "| a | b  | k | a | c |",
                "| 1 | 10 | 1 | 1 | 5 |",
                "| 1 | 10 | 1 | 2 | 6 |",
            ]
        );
        let derived = "SELECT * FROM t x JOIN u y ON x.a = y.k, (SELECT 7 AS z) d WHERE x.\"$path\" = 'f2'";
        assert_eq!(
            lines(&run(&ctx, derived).await.unwrap()),
            ["| a | b  | k | z |", "| 3 | 30 | 3 | 7 |"]
        );
        let nested = "SELECT * FROM (t x JOIN u y ON x.a = y.k) WHERE x.\"$path\" = 'f2'";
        assert_eq!(
            lines(&run(&ctx, nested).await.unwrap()),
            ["| a | b  | k |", "| 3 | 30 | 3 |"]
        );
        // One table with hidden columns among others: one exclusion is enough,
        // and USING keeps its meaning.
        let using = "SELECT * FROM t JOIN v USING (a) WHERE \"$file_size\" = 10 ORDER BY a";
        assert_eq!(
            lines(&run(&ctx, using).await.unwrap()),
            ["| a | b  | c |", "| 1 | 10 | 5 |", "| 2 | 20 | 6 |"]
        );
    }

    #[tokio::test]
    async fn star_over_several_tables_that_cannot_be_listed_is_refused() {
        let ctx = context();
        for (sql, reason) in [
            (
                "SELECT * FROM t x JOIN t y USING (a) WHERE x.\"$path\" = y.\"$path\"",
                "with JOIN … USING",
            ),
            (
                "SELECT * FROM t x CROSS JOIN u y CROSS JOIN (SELECT 1) WHERE x.\"$path\" = y.\"$path\"",
                "with a relation that has no name",
            ),
        ] {
            let err = exposed(&ctx, sql).await.unwrap_err().to_string();
            assert!(
                err.contains(reason) && err.contains("qualify the wildcard"),
                "{sql}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn tables_without_hidden_columns_are_left_as_written() {
        let ctx = context();
        for sql in [
            // A table of a catalog that has no hidden columns.
            "SELECT \"$path\" FROM v",
            // A CTE named as a table that has them.
            "WITH t AS (SELECT 1 AS a) SELECT \"$path\" FROM t",
            // `*` of a subquery does not carry them.
            "SELECT \"$path\" FROM (SELECT * FROM t) x",
        ] {
            assert_eq!(exposed(&ctx, sql).await.unwrap(), parse(sql).to_string(), "{sql}");
            let err = run(&ctx, sql).await.unwrap_err().to_string();
            assert!(err.contains("No field named \"$path\""), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn the_second_name_is_not_a_clients() {
        let ctx = context();
        for sql in [
            "SELECT * FROM \"t$hidden\"",
            "SELECT * FROM public.\"T$HIDDEN\"",
            "INSERT INTO \"t$hidden\" VALUES (1, 2)",
        ] {
            let err = exposed(&ctx, sql).await.unwrap_err().to_string();
            assert!(err.contains("not found"), "{sql}: {err}");
        }
        // A name that only contains it is a name.
        assert!(exposed(&ctx, "SELECT * FROM \"t$hiddenx\"").await.is_ok());
    }

    #[tokio::test]
    async fn explain_and_nested_statements_are_rewritten() {
        let ctx = context();
        assert_eq!(
            exposed(&ctx, "EXPLAIN SELECT \"$path\" FROM t").await.unwrap(),
            "EXPLAIN SELECT \"$path\" FROM \"t$hidden\" AS t"
        );
        assert_eq!(
            exposed(&ctx, "INSERT INTO v SELECT a, \"$file_size\" FROM t")
                .await
                .unwrap(),
            "INSERT INTO v SELECT a, \"$file_size\" FROM \"t$hidden\" AS t"
        );
        // A view's own statement: not SQL to rewrite.
        let mut copy = DFStatement::CreateExternalTable(
            match parse("CREATE EXTERNAL TABLE x STORED AS CSV LOCATION '/nowhere'") {
                DFStatement::CreateExternalTable(create) => create,
                other => panic!("{other}"),
            },
        );
        assert!(expose(&ctx.state(), &mut copy).await.is_ok());
    }

    #[tokio::test]
    async fn written_spelling_and_alias_are_kept() {
        let ctx = context();
        assert_eq!(
            exposed(&ctx, "SELECT T.\"$path\" FROM T").await.unwrap(),
            "SELECT T.\"$path\" FROM \"t$hidden\" AS T"
        );
        assert_eq!(
            exposed(&ctx, "SELECT \"$PATH\" FROM \"t\" AS q").await.unwrap(),
            "SELECT \"$PATH\" FROM \"t$hidden\" AS q"
        );
    }

    #[test]
    fn second_names() {
        assert_eq!(with_hidden_columns("t"), "t$hidden");
        assert_eq!(hidden_columns_of("t$hidden"), Some("t"));
        assert_eq!(hidden_columns_of("$hidden"), None);
        assert_eq!(hidden_columns_of("t"), None);
        assert_eq!(hidden_columns_of("t$hiddenx"), None);
    }

    fn select(sql: &str) -> (Box<Select>, Option<OrderBy>) {
        let DFStatement::Statement(statement) = parse(sql) else {
            panic!("{sql}")
        };
        let Statement::Query(query) = *statement else {
            panic!("{sql}")
        };
        let Query { body, order_by, .. } = *query;
        let SetExpr::Select(select) = *body else {
            panic!("{sql}")
        };
        (select, order_by)
    }

    #[test]
    fn each_clause_of_a_select_is_its_scope() {
        for sql in [
            "SELECT \"$path\" FROM t",
            "SELECT x.\"$PATH\" FROM t x",
            "SELECT a FROM t WHERE \"$file_size\" > 0",
            "SELECT a FROM t GROUP BY \"$path\"",
            "SELECT a FROM t GROUP BY a HAVING max(\"$file_modified_time\") IS NOT NULL",
            "SELECT a FROM t QUALIFY row_number() OVER (ORDER BY \"$path\") = 1",
            "SELECT a FROM t WINDOW w AS (PARTITION BY \"$path\")",
            "SELECT a FROM t ORDER BY \"$path\"",
            "SELECT a FROM t JOIN u ON t.\"$path\" = u.k",
            "SELECT a FROM t, LATERAL (SELECT t.\"$path\") x",
            "SELECT a FROM t CROSS JOIN UNNEST(ARRAY[\"$path\"]) AS z(p)",
            "SELECT a FROM (t JOIN u ON t.\"$path\" = u.k)",
            "SELECT a FROM t WHERE a IN (SELECT k FROM u WHERE \"$path\" = 'x')",
        ] {
            let (select, order_by) = select(sql);
            assert!(scope_names_hidden(&select, order_by.as_ref()), "{sql}");
        }
        for sql in [
            "SELECT a FROM t",
            "SELECT a AS \"$path\" FROM t",
            "SELECT a FROM t JOIN (SELECT \"$path\" FROM u) x ON true",
            "SELECT a FROM t JOIN u USING (a)",
            "SELECT \"$partition\", \"$bucket\", path FROM t",
        ] {
            let (select, order_by) = select(sql);
            assert!(!scope_names_hidden(&select, order_by.as_ref()), "{sql}");
        }
    }

    #[test]
    fn exclusions_add_to_the_written_ones() {
        let hidden: Vec<String> = HIDDEN_COLUMNS.iter().map(|c| format!("\"{c}\"")).collect();
        let excluded = |options: WildcardAdditionalOptions| match options.opt_exclude {
            Some(ExcludeSelectItem::Multiple(names)) => {
                names.iter().map(ToString::to_string).collect::<Vec<_>>()
            }
            other => panic!("{other:?}"),
        };
        assert_eq!(excluded(excluding(WildcardAdditionalOptions::default())), hidden);
        let one = WildcardAdditionalOptions {
            opt_exclude: Some(ExcludeSelectItem::Single(ObjectName::from(vec![Ident::new("x")]))),
            ..WildcardAdditionalOptions::default()
        };
        let mut expected = vec!["x".to_owned()];
        expected.extend(hidden.clone());
        assert_eq!(excluded(excluding(one)), expected);
        let two = WildcardAdditionalOptions {
            opt_exclude: Some(ExcludeSelectItem::Multiple(vec![
                ObjectName::from(vec![Ident::new("x")]),
                ObjectName::from(vec![Ident::new("y")]),
            ])),
            ..WildcardAdditionalOptions::default()
        };
        let mut expected = vec!["x".to_owned(), "y".to_owned()];
        expected.extend(hidden);
        assert_eq!(excluded(excluding(two)), expected);
    }

    #[test]
    fn qualifiers_name_a_switched_table_by_its_alias_or_the_end_of_its_name() {
        let aliased = Switched {
            alias: Ident::new("x"),
            name: None,
        };
        let unaliased = Switched {
            alias: Ident::new("t"),
            name: Some(vec!["hive".into(), "s".into(), "t".into()]),
        };
        let parts = |names: &[&str]| names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
        assert!(aliased.qualified_by(&parts(&["x"])));
        assert!(!aliased.qualified_by(&parts(&["t"])));
        assert!(!aliased.qualified_by(&parts(&["s", "x"])));
        assert!(unaliased.qualified_by(&parts(&["t"])));
        assert!(unaliased.qualified_by(&parts(&["s", "t"])));
        assert!(unaliased.qualified_by(&parts(&["hive", "s", "t"])));
        assert!(!unaliased.qualified_by(&parts(&["other", "t"])));
        assert!(!unaliased.qualified_by(&parts(&[])));
        assert_eq!(folded(&Ident::new("MiXed")), "mixed");
        assert_eq!(folded(&Ident::with_quote('"', "MiXed")), "MiXed");
    }

    #[test]
    fn names_of_hidden_columns_are_case_insensitive() {
        for name in ["$path", "$PATH", "$File_Size", "$file_modified_time"] {
            assert!(is_hidden(&Ident::new(name)), "{name}");
        }
        for name in ["path", "$partition", "$bucket", "$path2"] {
            assert!(!is_hidden(&Ident::new(name)), "{name}");
        }
        let DFStatement::Statement(statement) = parse("SELECT a FROM t WHERE \"$path\" = 'x'") else {
            panic!()
        };
        assert!(names_hidden_columns(&statement));
        let DFStatement::Statement(statement) = parse("SELECT a FROM t") else {
            panic!()
        };
        assert!(!names_hidden_columns(&statement));
    }
}
