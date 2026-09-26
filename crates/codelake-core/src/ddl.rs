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
//! Trino syntax that `sqlparser` does not accept is recognized before parsing
//! ([`rewrite`]): `CREATE VIEW … SECURITY DEFINER | INVOKER` (the clause is
//! recorded and removed) and `ALTER VIEW … RENAME TO …`. `ALTER TABLE … RENAME
//! TO …` parses and is routed from the AST like `ADD COLUMN`.
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
    ObjectNamePart, RenameTableNameKind, ShowStatementFilter, ShowStatementFilterPosition, SqlOption,
    Statement, Value,
};
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Location, Token};

use datafusion::error::{DataFusionError, Result};

/// Trino's `SECURITY` clause of `CREATE VIEW`. Without the clause a view is
/// `DEFINER`, as in Trino. `CodeLake` has no access control yet, so both modes
/// read the same data; the mode is stored with the view (Hive: `runAsInvoker`)
/// so Trino, reading the same metastore, applies it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ViewSecurity {
    #[default]
    Definer,
    Invoker,
}

/// What `ALTER … RENAME TO` renames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationKind {
    Table,
    View,
}

impl RelationKind {
    /// Word Trino uses in messages (`Table 'x' does not exist`).
    pub fn noun(self) -> &'static str {
        match self {
            Self::Table => "Table",
            Self::View => "View",
        }
    }
}

/// A statement handled outside `DataFusion`'s planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Prepared {
    AddColumn {
        table: TableReference,
        column: String,
        sql_type: String,
    },
    /// The view's query text, needed to persist the view, and its security.
    View { sql: String, security: ViewSecurity },
    Rename {
        kind: RelationKind,
        from: TableReference,
        to: TableReference,
        /// `ALTER TABLE IF EXISTS`: a missing source is not an error.
        if_exists: bool,
    },
}

/// Trino statements that `sqlparser` rejects, recognized from their tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Rewrite {
    /// `ALTER VIEW a RENAME TO b`, which never reaches the planner.
    RenameView {
        from: TableReference,
        to: TableReference,
    },
    /// `CREATE VIEW … SECURITY …`: the statement without the clause.
    ViewSecurity { sql: String, security: ViewSecurity },
}

/// Byte offset of a tokenizer location (1-based line and character column).
fn offset(sql: &str, location: Location) -> usize {
    let (mut line, mut column) = (1, 1);
    for (index, c) in sql.char_indices() {
        if line == location.line && column == location.column {
            return index;
        }
        if c == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    sql.len()
}

fn table_name(parser: &mut Parser<'_>) -> Result<TableReference> {
    table_reference(&parser.parse_object_name(false)?)
}

/// End of statement: an optional `;`, then nothing.
fn expect_end(parser: &mut Parser<'_>) -> Result<()> {
    let _ = parser.consume_token(&Token::SemiColon);
    parser.expect_token(&Token::EOF)?;
    Ok(())
}

/// Recognizes the Trino-only forms described in the module docs. Anything
/// else (including text the tokenizer rejects) is left to the planner, which
/// reports the error.
pub(crate) fn rewrite(sql: &str) -> Result<Option<Rewrite>> {
    let dialect = GenericDialect {};
    let Ok(mut parser) = Parser::new(&dialect).try_with_sql(sql) else {
        return Ok(None);
    };
    if parser.parse_keywords(&[Keyword::ALTER, Keyword::VIEW]) {
        let Ok(from) = table_name(&mut parser) else {
            return Ok(None);
        };
        if !parser.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            return Ok(None);
        }
        let to = table_name(&mut parser)?;
        expect_end(&mut parser)?;
        return Ok(Some(Rewrite::RenameView { from, to }));
    }
    if !parser.parse_keyword(Keyword::CREATE) {
        return Ok(None);
    }
    let _ = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    if !parser.parse_keyword(Keyword::VIEW) || parser.parse_object_name(false).is_err() {
        return Ok(None);
    }
    let clause = parser.peek_token();
    if !parser.parse_keyword(Keyword::SECURITY) {
        return Ok(None);
    }
    let mode = parser.next_token();
    let security = match &mode.token {
        Token::Word(word) if word.keyword == Keyword::DEFINER => ViewSecurity::Definer,
        Token::Word(word) if word.keyword == Keyword::INVOKER => ViewSecurity::Invoker,
        _ => {
            return Err(ParserError::ParserError(format!(
                "Expected: DEFINER or INVOKER, found: {} at {}",
                mode.token, mode.span.start
            ))
            .into());
        }
    };
    let (start, end) = (offset(sql, clause.span.start), offset(sql, mode.span.end));
    Ok(Some(Rewrite::ViewSecurity {
        sql: format!("{}{}", &sql[..start], &sql[end..]),
        security,
    }))
}

/// Trino `updateType` of the statements this module adds to what `sqlparser`
/// understands, for client protocols that announce it before execution.
pub fn update_type(sql: &str) -> Option<&'static str> {
    match rewrite(sql) {
        Ok(Some(Rewrite::RenameView { .. })) => return Some("RENAME VIEW"),
        Ok(Some(Rewrite::ViewSecurity { .. })) => return Some("CREATE VIEW"),
        Ok(None) | Err(_) => {}
    }
    let mut statements = datafusion::sql::parser::DFParser::parse_sql(sql).ok()?;
    match statements.pop_front()? {
        DFStatement::Statement(statement) => match *statement {
            Statement::AlterTable(alter)
                if matches!(
                    alter.operations.as_slice(),
                    [AlterTableOperation::RenameTable { .. }]
                ) =>
            {
                Some("RENAME TABLE")
            }
            _ => None,
        },
        _ => None,
    }
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

/// Inspects and adjusts a parsed statement before planning. `security` is the
/// view security [`rewrite`] removed from the text, if any.
pub(crate) fn prepare(statement: &mut DFStatement, security: ViewSecurity) -> Result<Option<Prepared>> {
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
        Statement::CreateView(view) => Ok(Some(Prepared::View {
            sql: view.query.to_string(),
            security,
        })),
        Statement::AlterTable(alter) => match alter.operations.as_slice() {
            [AlterTableOperation::AddColumn { column_def, .. }] => Ok(Some(Prepared::AddColumn {
                table: table_reference(&alter.name)?,
                // As SQL reads it: unquoted folds, quoted keeps its case
                // (a catalog may keep the case, ISS-018).
                column: ident_value(&ObjectNamePart::Identifier(column_def.name.clone()))?,
                sql_type: column_def.data_type.to_string(),
            })),
            [
                AlterTableOperation::RenameTable {
                    table_name: RenameTableNameKind::To(target),
                },
            ] => Ok(Some(Prepared::Rename {
                kind: RelationKind::Table,
                from: table_reference(&alter.name)?,
                to: table_reference(target)?,
                if_exists: alter.if_exists,
            })),
            _ => Err(DataFusionError::NotImplemented(
                "only ALTER TABLE … ADD COLUMN and RENAME TO are supported".into(),
            )),
        },
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
        assert_eq!(prepare(&mut statement, ViewSecurity::Definer).unwrap(), None);
        assert!(!statement.to_string().contains("WITH ("), "{statement}");
    }

    #[test]
    fn other_table_options_are_rejected() {
        for sql in [
            "CREATE TABLE t WITH (format = 'ORC') AS SELECT 1",
            "CREATE TABLE t WITH (partitioned_by = ARRAY['a']) AS SELECT 1",
        ] {
            let mut statement = parsed(sql);
            assert!(prepare(&mut statement, ViewSecurity::Definer).is_err(), "{sql}");
        }
    }

    #[test]
    fn add_column_is_extracted() {
        let mut statement = parsed("ALTER TABLE hive.Raw.\"T\" ADD COLUMN Extra varchar(10)");
        assert_eq!(
            prepare(&mut statement, ViewSecurity::Definer).unwrap(),
            Some(Prepared::AddColumn {
                table: TableReference::full("hive", "raw", "T"),
                column: "extra".into(),
                sql_type: "VARCHAR(10)".into(),
            })
        );
        let mut quoted = parsed("ALTER TABLE t ADD COLUMN \"CustomerCode\" bigint");
        assert!(matches!(
            prepare(&mut quoted, ViewSecurity::Definer).unwrap(),
            Some(Prepared::AddColumn { column, .. }) if column == "CustomerCode"
        ));
        let mut other = parsed("ALTER TABLE t DROP COLUMN x");
        assert!(prepare(&mut other, ViewSecurity::Definer).is_err());
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
            prepare(&mut statement, ViewSecurity::Definer).unwrap(),
            Some(Prepared::View {
                sql: "SELECT id FROM hive.s.t WHERE id > 0".into(),
                security: ViewSecurity::Definer,
            })
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
    fn rename_table_is_extracted() {
        let mut statement = parsed("ALTER TABLE IF EXISTS s.\"T\" RENAME TO c.s.t2");
        assert_eq!(
            prepare(&mut statement, ViewSecurity::Definer).unwrap(),
            Some(Prepared::Rename {
                kind: RelationKind::Table,
                from: TableReference::partial("s", "T"),
                to: TableReference::full("c", "s", "t2"),
                if_exists: true,
            })
        );
        assert_eq!(update_type("alter table a rename to b"), Some("RENAME TABLE"));
        assert_eq!(update_type("ALTER TABLE a ADD COLUMN b int"), None);
        assert_eq!(update_type("SELECT 1"), None);
    }

    #[test]
    fn view_security_clause_is_removed_and_recorded() {
        let sql = "/* {\"app\": \"dbt\"} */\n  create or replace view\n    \"memory\".\"s\".\"v\"\n  security invoker\n  as\n  select 'security definer' AS x";
        let Some(Rewrite::ViewSecurity {
            sql: rewritten,
            security,
        }) = rewrite(sql).unwrap()
        else {
            panic!("not recognized");
        };
        assert_eq!(security, ViewSecurity::Invoker);
        assert_eq!(
            rewritten,
            "/* {\"app\": \"dbt\"} */\n  create or replace view\n    \"memory\".\"s\".\"v\"\n  \n  as\n  select 'security definer' AS x"
        );
        let Some(Rewrite::ViewSecurity {
            sql: rewritten,
            security,
        }) = rewrite("CREATE VIEW v SECURITY DEFINER AS SELECT 1").unwrap()
        else {
            panic!("not recognized");
        };
        assert_eq!(
            (rewritten.as_str(), security),
            ("CREATE VIEW v  AS SELECT 1", ViewSecurity::Definer)
        );
        assert!(rewrite("CREATE VIEW v SECURITY NOBODY AS SELECT 1").is_err());
        assert_eq!(rewrite("CREATE VIEW v AS SELECT 1").unwrap(), None);
        assert_eq!(rewrite("CREATE TABLE security (a int)").unwrap(), None);
        assert_eq!(rewrite("SELECT 'unterminated").unwrap(), None);
        assert_eq!(
            update_type("CREATE VIEW ação SECURITY DEFINER AS SELECT 1"),
            Some("CREATE VIEW")
        );
    }

    #[test]
    fn offsets_count_characters_per_line() {
        let sql = "ação\nxé y";
        assert_eq!(offset(sql, Location::new(1, 3)), "aç".len());
        assert_eq!(offset(sql, Location::new(2, 4)), "ação\nxé ".len());
        assert_eq!(offset(sql, Location::new(9, 1)), sql.len());
    }

    #[test]
    fn rename_view_is_recognized() {
        assert_eq!(
            rewrite("alter view \"c\".s.v rename to c.s.w;").unwrap(),
            Some(Rewrite::RenameView {
                from: TableReference::full("c", "s", "v"),
                to: TableReference::full("c", "s", "w"),
            })
        );
        assert_eq!(update_type("ALTER VIEW v RENAME TO w"), Some("RENAME VIEW"));
        assert!(rewrite("ALTER VIEW v RENAME TO w extra").is_err());
        assert_eq!(rewrite("ALTER VIEW v AS SELECT 1").unwrap(), None);
    }

    #[test]
    fn table_names() {
        assert!(table_reference(&ObjectName::from(vec![])).is_err());
    }
}
