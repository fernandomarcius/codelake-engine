//! Remote SQL Server table exposed with lower-case column names.
//!
//! Queries reference columns in lower case (Trino's convention). A database
//! with a case-sensitive collation rejects `[id]` for a column named `Id`, and
//! renaming identifiers in the generated SQL is unsafe across aliases and
//! derived tables. Instead, each reference to this table is replaced by a
//! derived table that renames its columns once:
//!
//! ```sql
//! FROM (SELECT [Id] AS [id], [Name] AS [name] FROM [dbo].[Customers]) AS [c]
//! ```
//!
//! Every other part of the statement then refers to lower-case names that
//! exist. SQL Server inlines derived tables, so the plan is unchanged.

use std::any::Any;
use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{
    ObjectNamePart, SetExpr, Statement, TableFactor, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::MsSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_federation::sql::{AstAnalyzer, RemoteTableRef, SQLTable};

#[derive(Debug)]
pub(crate) struct SqlServerTable {
    remote: RemoteTableRef,
    schema: SchemaRef,
    /// Real schema and table name.
    real: (String, String),
    /// lower-case name -> real name, only when at least one differs.
    renames: Option<Arc<BTreeMap<String, String>>>,
}

impl SqlServerTable {
    /// `real_names`: lower-case name -> real name of every exposed column.
    /// Names must not contain `]` (filtered by the catalog).
    pub(crate) fn new(
        real_schema: &str,
        real_table: &str,
        schema: SchemaRef,
        real_names: BTreeMap<String, String>,
    ) -> Self {
        let needs_renames = real_names.iter().any(|(lower, real)| lower != real);
        Self {
            remote: RemoteTableRef::from(TableReference::partial(real_schema, real_table)),
            schema,
            real: (real_schema.to_owned(), real_table.to_owned()),
            renames: needs_renames.then(|| Arc::new(real_names)),
        }
    }
}

struct Renamer<'a> {
    schema: &'a str,
    table: &'a str,
    renames: &'a BTreeMap<String, String>,
}

impl Renamer<'_> {
    fn is_this_table(&self, factor: &TableFactor) -> bool {
        let TableFactor::Table { name, .. } = factor else {
            return false;
        };
        let parts: Vec<&str> = name
            .0
            .iter()
            .filter_map(|p| match p {
                ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
                ObjectNamePart::Function(_) => None,
            })
            .collect();
        parts == [self.schema, self.table]
    }

    fn derived(&self, alias: &str) -> Result<TableFactor> {
        let columns: Vec<String> = self
            .renames
            .iter()
            .map(|(lower, real)| format!("[{real}] AS [{lower}]"))
            .collect();
        let sql = format!(
            "SELECT * FROM (SELECT {} FROM [{}].[{}]) AS [{}]",
            columns.join(", "),
            self.schema,
            self.table,
            alias.replace(']', "]]")
        );
        let mut statements = Parser::parse_sql(&MsSqlDialect {}, &sql)
            .map_err(|e| DataFusionError::Plan(format!("sqlserver: cannot build derived table: {e}")))?;
        let Some(Statement::Query(query)) = statements.pop() else {
            return Err(DataFusionError::Plan(
                "sqlserver: derived table is not a query".into(),
            ));
        };
        let SetExpr::Select(select) = *query.body else {
            return Err(DataFusionError::Plan(
                "sqlserver: derived table is not a select".into(),
            ));
        };
        select
            .from
            .into_iter()
            .next()
            .map(|t| t.relation)
            .ok_or_else(|| DataFusionError::Plan("sqlserver: derived table without FROM".into()))
    }
}

impl VisitorMut for Renamer<'_> {
    type Break = DataFusionError;

    // Post-visit: the replacement is not visited again, so it is not re-wrapped.
    fn post_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        if !self.is_this_table(factor) {
            return ControlFlow::Continue(());
        }
        let alias = match factor {
            TableFactor::Table {
                alias: Some(alias), ..
            } => alias.name.value.clone(),
            _ => self.table.to_owned(),
        };
        match self.derived(&alias) {
            Ok(derived) => {
                *factor = derived;
                ControlFlow::Continue(())
            }
            Err(err) => ControlFlow::Break(err),
        }
    }
}

/// Replaces references to `schema.table` by the renaming derived table.
pub(crate) fn expose_lower_case(
    mut statement: Statement,
    real: &(String, String),
    renames: &BTreeMap<String, String>,
) -> Result<Statement> {
    let mut visitor = Renamer {
        schema: &real.0,
        table: &real.1,
        renames,
    };
    match statement.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(statement),
        ControlFlow::Break(err) => Err(err),
    }
}

impl SQLTable for SqlServerTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_reference(&self) -> TableReference {
        self.remote.table_ref().clone()
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        let renames = Arc::clone(self.renames.as_ref()?);
        let real = self.real.clone();
        Some(Box::new(move |statement| {
            expose_lower_case(statement, &real, &renames)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(sql: &str) -> String {
        let renames = BTreeMap::from([
            ("id".to_owned(), "Id".to_owned()),
            ("saldo".to_owned(), "Saldo".to_owned()),
        ]);
        let statement = Parser::parse_sql(&MsSqlDialect {}, sql).unwrap().remove(0);
        let out = expose_lower_case(statement, &("dbo".into(), "Parceiros".into()), &renames)
            .unwrap()
            .to_string();
        Parser::parse_sql(&MsSqlDialect {}, &out).unwrap();
        out
    }

    #[test]
    fn aliased_and_unaliased_references_are_wrapped() {
        assert_eq!(
            rewrite("SELECT [c].[id] FROM [dbo].[Parceiros] AS [c] WHERE [c].[saldo] >= 200"),
            "SELECT [c].[id] FROM (SELECT [Id] AS [id], [Saldo] AS [saldo] FROM [dbo].[Parceiros]) AS [c] \
             WHERE [c].[saldo] >= 200"
        );
        assert_eq!(
            rewrite("SELECT [Parceiros].[id] FROM [dbo].[Parceiros]"),
            "SELECT [Parceiros].[id] FROM (SELECT [Id] AS [id], [Saldo] AS [saldo] FROM [dbo].[Parceiros]) AS [Parceiros]"
        );
    }

    #[test]
    fn other_tables_and_nested_queries() {
        let out = rewrite(
            "SELECT [x].[id] FROM (SELECT [p].[id] FROM [dbo].[Parceiros] AS [p] JOIN [dbo].[Outra] AS [o] ON [o].[id] = [p].[id]) AS [x]",
        );
        assert!(
            out.contains("FROM (SELECT [Id] AS [id], [Saldo] AS [saldo] FROM [dbo].[Parceiros]) AS [p]"),
            "{out}"
        );
        assert!(out.contains("JOIN [dbo].[Outra] AS [o]"), "{out}");
        assert_eq!(
            out.matches("FROM [dbo].[Parceiros]").count(),
            1,
            "wrapped exactly once: {out}"
        );
    }
}
