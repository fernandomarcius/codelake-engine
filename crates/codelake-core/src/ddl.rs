//! Routing of DDL to writable catalogs.
//!
//! `DataFusion` implements `CREATE TABLE AS`, views and schemas in memory. For a
//! catalog that persists them (the Hive catalog), the engine hands the
//! statement to its [`WritableCatalog`] instead. `ALTER TABLE ADD COLUMN` is not
//! planned by `DataFusion` at all and is routed from the AST.
//!
//! Trino's `CREATE TABLE … WITH (format = 'PARQUET')` is accepted; any other
//! table option is rejected rather than ignored.
//!
//! Trino's `ALTER TABLE <table> EXECUTE <procedure>[(args)] [WHERE …]` is not
//! in `sqlparser`'s grammar: [`parse_table_execute`] recognizes it with
//! `sqlparser`'s own tokenizer and expression parser, so quoting, comments,
//! case and whitespace follow the dialect exactly.

use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::sql::TableReference;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    AlterTableOperation, CreateTableOptions, Expr as SqlExpr, ObjectName, ObjectNamePart, SqlOption,
    Statement, Value,
};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;

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

/// `ALTER TABLE <table> EXECUTE <procedure>(<arguments>) [WHERE <filter>]`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableExecute {
    pub(crate) table: TableReference,
    /// Trino's canonical name: upper-cased unless quoted (`"optimize"` stays
    /// lower case and matches no procedure, as in Trino).
    pub(crate) procedure: String,
    /// `(name, expression)`; `name` is lower-cased, `None` when positional.
    pub(crate) arguments: Vec<(Option<String>, SqlExpr)>,
    pub(crate) filter: Option<SqlExpr>,
}

fn syntax(err: ParserError) -> DataFusionError {
    DataFusionError::SQL(Box::new(err), None)
}

/// Recognizes Trino's `ALTER TABLE … EXECUTE`. Returns `None` for any other
/// statement (including text that does not tokenize), which then goes
/// through the regular parser and its error messages.
pub(crate) fn parse_table_execute(sql: &str, dialect: &dyn Dialect) -> Result<Option<TableExecute>> {
    let Ok(mut parser) = Parser::new(dialect).try_with_sql(sql) else {
        return Ok(None);
    };
    if !parser.parse_keywords(&[Keyword::ALTER, Keyword::TABLE]) {
        return Ok(None);
    }
    let Ok(name) = parser.parse_object_name(false) else {
        return Ok(None);
    };
    if !parser.parse_keyword(Keyword::EXECUTE) {
        return Ok(None);
    }
    let table = table_reference(&name)?;
    let procedure = parser.parse_identifier().map_err(syntax)?;
    let procedure = if procedure.quote_style.is_some() {
        procedure.value
    } else {
        procedure.value.to_uppercase()
    };
    let mut arguments = Vec::new();
    if parser.consume_token(&Token::LParen) && !parser.consume_token(&Token::RParen) {
        loop {
            let named = matches!(parser.peek_token().token, Token::Word(_))
                && parser.peek_nth_token(1).token == Token::RArrow;
            let argument_name = if named {
                let ident = parser.parse_identifier().map_err(syntax)?;
                parser.expect_token(&Token::RArrow).map_err(syntax)?;
                Some(ident.value.to_lowercase())
            } else {
                None
            };
            arguments.push((argument_name, parser.parse_expr().map_err(syntax)?));
            if !parser.consume_token(&Token::Comma) {
                parser.expect_token(&Token::RParen).map_err(syntax)?;
                break;
            }
        }
    }
    let filter = if parser.parse_keyword(Keyword::WHERE) {
        Some(parser.parse_expr().map_err(syntax)?)
    } else {
        None
    };
    while parser.consume_token(&Token::SemiColon) {}
    let rest = parser.peek_token();
    if rest.token != Token::EOF {
        return Err(syntax(ParserError::ParserError(format!(
            "Expected: end of statement, found: {}",
            rest.token
        ))));
    }
    Ok(Some(TableExecute {
        table,
        procedure,
        arguments,
        filter,
    }))
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

    fn execute(sql: &str) -> Result<Option<TableExecute>> {
        parse_table_execute(sql, &datafusion::sql::sqlparser::dialect::GenericDialect {})
    }

    fn literal(expr: &SqlExpr) -> String {
        expr.to_string()
    }

    #[test]
    fn table_execute_is_recognized_robustly() {
        let plain = execute("ALTER TABLE hive.raw.orders EXECUTE optimize")
            .unwrap()
            .unwrap();
        assert_eq!(plain.table, TableReference::full("hive", "raw", "orders"));
        assert_eq!(plain.procedure, "OPTIMIZE");
        assert!(plain.arguments.is_empty() && plain.filter.is_none());

        let quoted = execute(
            "  alter   table \"hive\".\"Raw\".\"Order Items\"\n  execute \"OPTIMIZE\" (\n file_size_threshold  =>  '10MB' ) ;",
        )
        .unwrap()
        .unwrap();
        assert_eq!(quoted.table, TableReference::full("hive", "Raw", "Order Items"));
        assert_eq!(quoted.procedure, "OPTIMIZE");
        assert_eq!(quoted.arguments.len(), 1);
        assert_eq!(quoted.arguments[0].0.as_deref(), Some("file_size_threshold"));
        assert_eq!(literal(&quoted.arguments[0].1), "'10MB'");

        let partial = execute("ALTER TABLE Raw.T EXECUTE Optimize('1' || 'MB') -- trailing comment")
            .unwrap()
            .unwrap();
        assert_eq!(partial.table, TableReference::partial("raw", "t"));
        assert_eq!(partial.procedure, "OPTIMIZE");
        let lower = execute("ALTER TABLE t EXECUTE \"optimize\"").unwrap().unwrap();
        assert_eq!(lower.procedure, "optimize", "a quoted name is kept as written");
        assert_eq!(partial.arguments[0].0, None);
        assert_eq!(literal(&partial.arguments[0].1), "'1' || 'MB'");

        let bare = execute("/* c */ ALTER TABLE t EXECUTE optimize() WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(bare.table, TableReference::bare("t"));
        assert!(bare.arguments.is_empty());
        assert_eq!(bare.filter.map(|f| f.to_string()).as_deref(), Some("id = 1"));

        let two = execute("ALTER TABLE t EXECUTE p(\"A\" => 1, b => CAST('x' AS varchar))")
            .unwrap()
            .unwrap();
        assert_eq!(two.arguments[0].0.as_deref(), Some("a"));
        assert_eq!(two.arguments[1].0.as_deref(), Some("b"));
    }

    #[test]
    fn other_statements_are_left_to_the_regular_parser() {
        for sql in [
            "SELECT 1",
            "ALTER TABLE t ADD COLUMN x int",
            "ALTER VIEW v AS SELECT 1",
            "ALTER TABLE",
            "INSERT INTO t VALUES ('unterminated",
            "",
        ] {
            assert_eq!(execute(sql).unwrap(), None, "{sql}");
        }
    }

    #[test]
    fn malformed_execute_is_a_syntax_error() {
        for sql in [
            "ALTER TABLE t EXECUTE",
            "ALTER TABLE t EXECUTE optimize(",
            "ALTER TABLE t EXECUTE optimize(1 2)",
            "ALTER TABLE t EXECUTE optimize(x =>)",
            "ALTER TABLE t EXECUTE optimize garbage",
            "ALTER TABLE t EXECUTE optimize; SELECT 1",
            "ALTER TABLE a.b.c.d EXECUTE optimize",
        ] {
            assert!(execute(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn table_names() {
        assert!(table_reference(&ObjectName::from(vec![])).is_err());
    }
}
