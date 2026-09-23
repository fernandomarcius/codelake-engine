//! Remote SQL Server table that renders its columns with their real spelling.
//!
//! Columns are exposed to SQL in lower case (Trino's convention), but a database
//! with a case-sensitive collation rejects `[id]` for a column named `Id`. This
//! table's AST analyzer maps its own column identifiers back to the real names.

use std::any::Any;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{Expr, Ident, Statement, VisitMut, VisitorMut};
use datafusion_federation::sql::{AstAnalyzer, RemoteTableRef, SQLTable};

#[derive(Debug)]
pub(crate) struct SqlServerTable {
    remote: RemoteTableRef,
    schema: SchemaRef,
    /// lower-case name -> real name.
    real_names: Arc<HashMap<String, String>>,
}

impl SqlServerTable {
    pub(crate) fn new(
        remote: RemoteTableRef,
        schema: SchemaRef,
        real_names: HashMap<String, String>,
    ) -> Self {
        Self {
            remote,
            schema,
            real_names: Arc::new(real_names),
        }
    }
}

struct RealNames<'a>(&'a HashMap<String, String>);

impl RealNames<'_> {
    fn fix(&self, ident: &mut Ident) {
        if let Some(real) = self.0.get(&ident.value)
            && *real != ident.value
        {
            ident.value.clone_from(real);
        }
    }
}

impl VisitorMut for RealNames<'_> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(ident) => self.fix(ident),
            Expr::CompoundIdentifier(parts) => {
                if let Some(column) = parts.last_mut() {
                    self.fix(column);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Renames column identifiers of this table to their real spelling.
pub(crate) fn restore_names(mut statement: Statement, real_names: &HashMap<String, String>) -> Statement {
    let _ = statement.visit(&mut RealNames(real_names));
    statement
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
        let names = Arc::clone(&self.real_names);
        Some(Box::new(move |statement| Ok(restore_names(statement, &names))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::MsSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    #[test]
    fn restores_real_spelling_of_bare_and_qualified_columns() {
        let names = HashMap::from([
            ("id".to_owned(), "Id".to_owned()),
            ("__$start_lsn".to_owned(), "__$start_lsn".to_owned()),
        ]);
        let statement = Parser::parse_sql(
            &MsSqlDialect {},
            "SELECT [c].[id], [__$start_lsn], [other] FROM [dbo].[Contratos] AS [c] WHERE [id] > 1",
        )
        .unwrap()
        .remove(0);
        let out = restore_names(statement, &names).to_string();
        assert_eq!(
            out,
            "SELECT [c].[Id], [__$start_lsn], [other] FROM [dbo].[Contratos] AS [c] WHERE [Id] > 1"
        );
    }
}
