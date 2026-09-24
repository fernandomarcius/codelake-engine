//! Routing of DDL to writable catalogs.
//!
//! `DataFusion` implements `CREATE TABLE AS`, views and schemas in memory. For a
//! catalog that persists them (the Hive catalog), the engine hands the
//! statement to its [`WritableCatalog`] instead. `ALTER TABLE ADD COLUMN` is not
//! planned by `DataFusion` at all and is routed from the AST.
//!
//! Trino's `CREATE TABLE … WITH (format = 'PARQUET')` is accepted; any other
//! table option is rejected rather than ignored.

use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::sql::TableReference;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    AlterTableOperation, CreateTableOptions, Expr as SqlExpr, ObjectName, ObjectNamePart, SqlOption,
    Statement, Value,
};

use datafusion::error::{DataFusionError, Result};

/// A statement handled outside `DataFusion`'s planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Prepared {
    AddColumn {
        table: TableReference,
        column: String,
        sql_type: String,
    },
    /// The view's query text, needed to persist the view.
    ViewSql(String),
}

fn ident_value(part: &ObjectNamePart) -> Result<String> {
    match part {
        ObjectNamePart::Identifier(ident) if ident.quote_style.is_some() => Ok(ident.value.clone()),
        ObjectNamePart::Identifier(ident) => Ok(ident.value.to_lowercase()),
        ObjectNamePart::Function(_) => {
            Err(DataFusionError::Plan("unexpected function in object name".into()))
        }
    }
}

pub(crate) fn table_reference(name: &ObjectName) -> Result<TableReference> {
    let parts = name.0.iter().map(ident_value).collect::<Result<Vec<_>>>()?;
    match parts.as_slice() {
        [table] => Ok(TableReference::bare(table.as_str())),
        [schema, table] => Ok(TableReference::partial(schema.as_str(), table.as_str())),
        [catalog, schema, table] => Ok(TableReference::full(
            catalog.as_str(),
            schema.as_str(),
            table.as_str(),
        )),
        _ => Err(DataFusionError::Plan(format!("invalid table name {name}"))),
    }
}

fn only_parquet_format(options: &[SqlOption]) -> Result<()> {
    for option in options {
        let SqlOption::KeyValue { key, value } = option else {
            return Err(DataFusionError::NotImplemented(format!(
                "table option {option} is not supported"
            )));
        };
        let is_parquet = matches!(
            value,
            SqlExpr::Value(v) if matches!(&v.value, Value::SingleQuotedString(s) if s.eq_ignore_ascii_case("parquet"))
        );
        if !key.value.eq_ignore_ascii_case("format") || !is_parquet {
            return Err(DataFusionError::NotImplemented(format!(
                "table option {option} is not supported (only format = 'PARQUET')"
            )));
        }
    }
    Ok(())
}

/// Inspects and adjusts a parsed statement before planning.
pub(crate) fn prepare(statement: &mut DFStatement) -> Result<Option<Prepared>> {
    let DFStatement::Statement(inner) = statement else {
        return Ok(None);
    };
    match inner.as_mut() {
        Statement::CreateTable(create) => {
            if let CreateTableOptions::With(options) = &create.table_options {
                only_parquet_format(options)?;
                create.table_options = CreateTableOptions::None;
            }
            Ok(None)
        }
        Statement::CreateView(view) => Ok(Some(Prepared::ViewSql(view.query.to_string()))),
        Statement::AlterTable(alter) => {
            let [AlterTableOperation::AddColumn { column_def, .. }] = alter.operations.as_slice() else {
                return Err(DataFusionError::NotImplemented(
                    "only ALTER TABLE … ADD COLUMN is supported".into(),
                ));
            };
            Ok(Some(Prepared::AddColumn {
                table: table_reference(&alter.name)?,
                column: column_def.name.value.to_lowercase(),
                sql_type: column_def.data_type.to_string(),
            }))
        }
        _ => Ok(None),
    }
}

/// DDL plans a writable catalog may take over.
pub(crate) fn is_routable(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Ddl(
            DdlStatement::CreateMemoryTable(_)
                | DdlStatement::DropTable(_)
                | DdlStatement::CreateView(_)
                | DdlStatement::DropView(_)
                | DdlStatement::CreateCatalogSchema(_)
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::parser::DFParser;

    fn parsed(sql: &str) -> DFStatement {
        DFParser::parse_sql(sql).unwrap().pop_front().unwrap()
    }

    #[test]
    fn parquet_format_option_is_accepted_and_stripped() {
        let mut statement = parsed("CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS SELECT 1 AS a");
        assert_eq!(prepare(&mut statement).unwrap(), None);
        assert!(!statement.to_string().contains("WITH ("), "{statement}");
    }

    #[test]
    fn other_table_options_are_rejected() {
        for sql in [
            "CREATE TABLE t WITH (format = 'ORC') AS SELECT 1",
            "CREATE TABLE t WITH (partitioned_by = ARRAY['a']) AS SELECT 1",
        ] {
            let mut statement = parsed(sql);
            assert!(prepare(&mut statement).is_err(), "{sql}");
        }
    }

    #[test]
    fn add_column_is_extracted() {
        let mut statement = parsed("ALTER TABLE hive.Raw.\"T\" ADD COLUMN Extra varchar(10)");
        assert_eq!(
            prepare(&mut statement).unwrap(),
            Some(Prepared::AddColumn {
                table: TableReference::full("hive", "raw", "T"),
                column: "extra".into(),
                sql_type: "VARCHAR(10)".into(),
            })
        );
        let mut other = parsed("ALTER TABLE t DROP COLUMN x");
        assert!(prepare(&mut other).is_err());
    }

    #[test]
    fn view_query_text_is_kept() {
        let mut statement = parsed("CREATE VIEW hive.s.v AS SELECT id FROM hive.s.t WHERE id > 0");
        assert_eq!(
            prepare(&mut statement).unwrap(),
            Some(Prepared::ViewSql("SELECT id FROM hive.s.t WHERE id > 0".into()))
        );
    }

    #[test]
    fn table_names() {
        assert!(table_reference(&ObjectName::from(vec![])).is_err());
    }
}
