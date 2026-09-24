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
    AlterTableOperation, ColumnDef, ColumnOption, CreateTableOptions, Expr as SqlExpr, ObjectName,
    ObjectNamePart, ShowStatementFilter, ShowStatementFilterPosition, SqlOption, Statement, Value,
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

/// A column as the DDL statement declared it: what a writable catalog may
/// have to store or refuse, and Arrow types do not keep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DeclaredColumn {
    pub(crate) name: String,
    /// The type as written (`VARCHAR(3)`, `TIMESTAMP(6)`).
    pub(crate) sql_type: String,
    pub(crate) comment: Option<String>,
    pub(crate) not_null: bool,
    /// Any other column option (`DEFAULT …`, `PRIMARY KEY`, …), as written.
    pub(crate) other: Option<String>,
}

fn declared(column: &ColumnDef) -> DeclaredColumn {
    let mut declared = DeclaredColumn {
        name: column.name.value.clone(),
        sql_type: column.data_type.to_string(),
        ..DeclaredColumn::default()
    };
    for option in &column.options {
        match &option.option {
            ColumnOption::Null => {}
            ColumnOption::NotNull => declared.not_null = true,
            ColumnOption::Comment(comment) => declared.comment = Some(comment.clone()),
            other => {
                declared.other.get_or_insert_with(|| other.to_string());
            }
        }
    }
    declared
}

/// Columns declared by `CREATE TABLE t (…)` (in order) or by
/// `ALTER TABLE … ADD COLUMN`; empty for any other statement, and for CTAS.
pub(crate) fn declared_columns(statement: &DFStatement) -> Vec<DeclaredColumn> {
    let DFStatement::Statement(inner) = statement else {
        return Vec::new();
    };
    match inner.as_ref() {
        Statement::CreateTable(create) if create.query.is_none() => {
            create.columns.iter().map(declared).collect()
        }
        Statement::AlterTable(alter) => alter
            .operations
            .iter()
            .filter_map(|operation| match operation {
                AlterTableOperation::AddColumn { column_def, .. } => Some(declared(column_def)),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
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

/// Trino's `SHOW TABLES FROM|IN [catalog.]schema [LIKE pattern]`, which the
/// SQL planner does not plan ("SHOW TABLES IN not supported"), as the query
/// on the catalog's `information_schema` it means, with Trino's column name
/// (`Table`). Any other statement is left as it is.
pub(crate) fn rewrite_show_tables(statement: &mut DFStatement) -> Result<()> {
    let DFStatement::Statement(inner) = statement else {
        return Ok(());
    };
    let Statement::ShowTables {
        terse: false,
        history: false,
        extended: false,
        full: false,
        external: false,
        show_options,
    } = inner.as_ref()
    else {
        return Ok(());
    };
    let Some(parent) = show_options
        .show_in
        .as_ref()
        .and_then(|show_in| show_in.parent_name.as_ref())
    else {
        return Ok(());
    };
    let parts = parent.0.iter().map(ident_value).collect::<Result<Vec<_>>>()?;
    let quoted = |text: &str| format!("'{}'", text.replace('\'', "''"));
    let (listing, schema) = match parts.as_slice() {
        [schema] => ("information_schema.tables".to_owned(), schema),
        [catalog, schema] => (
            format!("\"{}\".information_schema.tables", catalog.replace('"', "\"\"")),
            schema,
        ),
        _ => return Err(DataFusionError::Plan(format!("invalid schema name {parent}"))),
    };
    let like = match &show_options.filter_position {
        None => String::new(),
        Some(
            ShowStatementFilterPosition::Infix(ShowStatementFilter::Like(pattern))
            | ShowStatementFilterPosition::Suffix(ShowStatementFilter::Like(pattern)),
        ) => format!(" AND table_name LIKE {}", quoted(pattern)),
        Some(_) => {
            return Err(DataFusionError::NotImplemented(
                "SHOW TABLES accepts only a LIKE filter".into(),
            ));
        }
    };
    let sql = format!(
        "SELECT table_name AS \"Table\" FROM {listing} WHERE table_schema = {}{like} ORDER BY table_name",
        quoted(schema)
    );
    let mut parsed = datafusion::sql::parser::DFParser::parse_sql(&sql)?;
    if let Some(query) = parsed.pop_front() {
        *statement = query;
    }
    Ok(())
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
    fn declared_columns_keep_types_comments_and_options() {
        let create = parsed("CREATE TABLE t (a VARCHAR(3) COMMENT 'x', b TIMESTAMP(6) NOT NULL, c INT NULL)");
        let columns = declared_columns(&create);
        assert_eq!(columns.len(), 3);
        assert_eq!(
            (columns[0].sql_type.as_str(), columns[0].comment.as_deref()),
            ("VARCHAR(3)", Some("x"))
        );
        assert!(columns[1].not_null && columns[1].sql_type == "TIMESTAMP(6)");
        assert!(!columns[2].not_null && columns[2].other.is_none());
        let add = parsed("ALTER TABLE t ADD COLUMN n INT DEFAULT 1");
        assert_eq!(declared_columns(&add)[0].other.as_deref(), Some("DEFAULT 1"));
        assert!(declared_columns(&parsed("CREATE TABLE t AS SELECT 1 AS a")).is_empty());
    }

    #[test]
    fn show_tables_from_a_schema_lists_its_information_schema() {
        let mut statement = parsed("SHOW TABLES FROM hive.S");
        rewrite_show_tables(&mut statement).unwrap();
        assert_eq!(
            statement.to_string(),
            "SELECT table_name AS \"Table\" FROM \"hive\".information_schema.tables WHERE table_schema = 's' \
             ORDER BY table_name"
        );
        let mut like = parsed("SHOW TABLES IN raw LIKE 'o''k%'");
        rewrite_show_tables(&mut like).unwrap();
        assert!(like.to_string().contains("table_name LIKE 'o''k%'"), "{like}");
        let mut plain = parsed("SHOW TABLES");
        rewrite_show_tables(&mut plain).unwrap();
        assert_eq!(plain.to_string(), "SHOW TABLES");
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
