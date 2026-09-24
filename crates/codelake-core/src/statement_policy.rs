//! What a client statement may do, whatever protocol carried it.
//!
//! Trino has no `COPY` and no `CREATE EXTERNAL TABLE`: a client reaches files
//! only through a configured catalog, which decides where its files live and
//! how they are committed. `DataFusion` adds statements that read or write any
//! path the server process can reach (`COPY … TO '<path>'`, `CREATE EXTERNAL
//! TABLE … LOCATION '<path>'`) and runtime settings that move spill files or
//! replace the memory pool (`SET datafusion.runtime.*`). They are refused with
//! `NOT_SUPPORTED` before planning, and again on the logical plan, so no
//! protocol (Trino, PostgreSQL) and no nesting (`EXPLAIN ANALYZE COPY …`)
//! reaches them.
//!
//! Also here: Trino's lexical rule against zero-length delimited identifiers
//! (`""`), the checks every name must pass before a writable catalog turns it
//! into a path, and the rule that a stored view is a query.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan, Statement as PlanStatement};
use datafusion::sql::parser::{DFParser, ResetStatement, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{Query, Statement};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

/// Prefix of the `DataFusion` settings that change the process' runtime
/// (memory pool, spill directory, caches): never a client's to change.
const RUNTIME_SETTINGS: &str = "datafusion.runtime.";

fn refused(what: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "{what} is not supported: files are reached only through a configured catalog"
    ))
}

fn runtime_setting(name: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "setting {name} is not supported: runtime settings belong to the server configuration"
    ))
}

/// Trino's lexer refuses `""` anywhere in a statement: "Zero-length
/// delimited identifier not allowed" (`SYNTAX_ERROR`). Text that does not
/// tokenize is left to the parser and its own messages.
pub fn check_tokens(sql: &str, dialect: &dyn Dialect) -> Result<()> {
    let Ok(tokens) = Tokenizer::new(dialect, sql).tokenize() else {
        return Ok(());
    };
    let empty = tokens.iter().any(
        |token| matches!(token, Token::Word(word) if word.quote_style.is_some() && word.value.is_empty()),
    );
    if empty {
        return Err(DataFusionError::SQL(
            Box::new(ParserError::ParserError(
                "Zero-length delimited identifier not allowed".into(),
            )),
            None,
        ));
    }
    Ok(())
}

/// Refuses statements that reach server files or runtime settings.
pub fn check_statement(statement: &DFStatement) -> Result<()> {
    match statement {
        DFStatement::CopyTo(_) => Err(refused("COPY")),
        DFStatement::CreateExternalTable(_) => Err(refused("CREATE EXTERNAL TABLE")),
        DFStatement::Explain(explain) => check_statement(&explain.statement),
        DFStatement::Reset(ResetStatement::Variable(name)) => {
            let name = name.to_string().to_ascii_lowercase();
            if name.starts_with(RUNTIME_SETTINGS) {
                return Err(runtime_setting(&name));
            }
            Ok(())
        }
        DFStatement::Statement(inner) => check_sql_statement(inner),
    }
}

fn check_sql_statement(statement: &Statement) -> Result<()> {
    match statement {
        Statement::Copy { .. } | Statement::CopyIntoSnowflake { .. } | Statement::Unload { .. } => {
            Err(refused("COPY"))
        }
        Statement::LoadData { .. } => Err(refused("LOAD DATA")),
        Statement::CreateTable(create) if create.external || create.location.is_some() => {
            Err(refused("CREATE EXTERNAL TABLE"))
        }
        Statement::Set(_) => {
            let text = statement.to_string().to_ascii_lowercase();
            if text.contains(RUNTIME_SETTINGS) {
                return Err(runtime_setting(RUNTIME_SETTINGS));
            }
            Ok(())
        }
        Statement::Explain { statement, .. } | Statement::Prepare { statement, .. } => {
            check_sql_statement(statement)
        }
        _ => Ok(()),
    }
}

/// [`check_statement`] on a statement that another parser produced
/// (`datafusion-postgres` parses with `sqlparser` alone): its text is parsed
/// again the way `DataFusion` will run it. Text `DataFusion` cannot parse
/// cannot run either, and is left to its own error.
pub fn check_sql(sql: &str, dialect: &dyn Dialect) -> Result<()> {
    check_tokens(sql, dialect)?;
    let Ok(statements) = DFParser::parse_sql_with_dialect(sql, dialect) else {
        return Ok(());
    };
    statements.iter().try_for_each(check_statement)
}

/// The same rule on a logical plan, wherever the node sits (`EXPLAIN`,
/// prepared statements).
pub fn check_plan(plan: &LogicalPlan) -> Result<()> {
    let mut found = None;
    plan.apply(|node| {
        let refusal = match node {
            LogicalPlan::Copy(_) => Some(refused("COPY")),
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(_)) => Some(refused("CREATE EXTERNAL TABLE")),
            LogicalPlan::Statement(PlanStatement::SetVariable(set))
                if set.variable.to_ascii_lowercase().starts_with(RUNTIME_SETTINGS) =>
            {
                Some(runtime_setting(&set.variable))
            }
            LogicalPlan::Statement(PlanStatement::ResetVariable(reset))
                if reset.variable.to_ascii_lowercase().starts_with(RUNTIME_SETTINGS) =>
            {
                Some(runtime_setting(&reset.variable))
            }
            _ => None,
        };
        Ok(match refusal {
            Some(err) => {
                found = Some(err);
                TreeNodeRecursion::Stop
            }
            None => TreeNodeRecursion::Continue,
        })
    })?;
    found.map_or(Ok(()), Err)
}

/// Kinds of names a writable catalog turns into paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Schema,
    Table,
    View,
    Column,
}

impl ObjectKind {
    fn label(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::Table => "table",
            Self::View => "view",
            Self::Column => "column",
        }
    }
}

/// A name a writable catalog may store. Schemas, tables and views become
/// directories: not empty, not `.` or `..`, no path separator and no control
/// character, or the path would not be the object's own directory (an empty
/// table name is the schema's directory; `..` is its parent). Trino answers
/// `Invalid table name` for `.` and `..` (`GENERIC_USER_ERROR`); the others are
/// refused the same way. Columns only live in metadata: only empty is refused.
pub fn check_object_name(kind: ObjectKind, name: &str) -> Result<()> {
    let invalid = if kind == ObjectKind::Column {
        name.is_empty()
    } else {
        name.is_empty()
            || name == "."
            || name == ".."
            || name.contains(['/', '\\'])
            || name.chars().any(char::is_control)
    };
    if invalid {
        return Err(DataFusionError::Plan(format!(
            "GENERIC_USER_ERROR: Invalid {} name",
            kind.label()
        )));
    }
    Ok(())
}

/// Whether `name` passes [`check_object_name`].
pub fn is_valid_object_name(name: &str) -> bool {
    check_object_name(ObjectKind::Table, name).is_ok()
}

/// Parses the SQL stored for a view, checks that it is a query (`SELECT`,
/// `WITH`, `VALUES`), the only thing Trino plans a view as, and prepares it
/// the way a client's query is prepared (Trino syntax and literals: a
/// view's SQL is Trino SQL, and must answer what the same SQL answers
/// outside the view). Anything else (`INSERT`, `COPY`, DDL) is refused,
/// whoever wrote it into the metastore: reading a view must never write.
pub fn parse_view_query(sql: &str, dialect: &dyn Dialect) -> Result<DFStatement> {
    let sql = crate::trino_functions::preprocess_sql(sql);
    let mut statements = DFParser::parse_sql_with_dialect(&sql, dialect)?;
    let not_a_query = || DataFusionError::Plan("a view must be a single query (SELECT or WITH)".into());
    if statements.len() != 1 {
        return Err(not_a_query());
    }
    let query: Box<Query> = match statements.pop_front() {
        Some(DFStatement::Statement(statement)) => match *statement {
            Statement::Query(query) => query,
            _ => return Err(not_a_query()),
        },
        _ => return Err(not_a_query()),
    };
    let mut statement = DFStatement::Statement(Box::new(Statement::Query(query)));
    crate::trino_functions::rewrite_statement(&mut statement)?;
    crate::literals::rewrite_zoned_timestamps(&mut statement);
    Ok(statement)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::{GenericDialect, PostgreSqlDialect};

    fn parsed(sql: &str) -> DFStatement {
        DFParser::parse_sql(sql).unwrap().pop_front().unwrap()
    }

    #[test]
    fn file_statements_are_refused() {
        for sql in [
            "COPY (SELECT 1) TO '/tmp/x.csv'",
            "COPY (SELECT 1 AS a) TO '/tmp/x' STORED AS PARQUET",
            "CREATE EXTERNAL TABLE leak (line VARCHAR) STORED AS CSV LOCATION '/etc/hostname'",
            "EXPLAIN COPY (SELECT 1) TO '/tmp/x.csv'",
            "EXPLAIN ANALYZE COPY (SELECT 1) TO '/tmp/x.csv'",
            "SET datafusion.runtime.memory_limit = '100G'",
            "SET datafusion.runtime.temp_directory = '/etc'",
            "RESET datafusion.runtime.memory_limit",
        ] {
            let err = check_statement(&parsed(sql)).unwrap_err();
            assert!(matches!(err, DataFusionError::NotImplemented(_)), "{sql}: {err}");
        }
    }

    #[test]
    fn ordinary_statements_pass() {
        for sql in [
            "SELECT 1",
            "WITH t AS (SELECT 1) SELECT * FROM t",
            "CREATE TABLE hive.s.t AS SELECT 1 AS a",
            "INSERT INTO t VALUES (1)",
            "SET datafusion.execution.batch_size = 1024",
            "EXPLAIN SELECT 1",
            "CREATE VIEW v AS SELECT 1",
        ] {
            check_statement(&parsed(sql)).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[test]
    fn statements_parsed_elsewhere_are_checked_as_datafusion_reads_them() {
        let postgres = PostgreSqlDialect {};
        assert!(check_sql("COPY (SELECT 1) TO '/tmp/x.csv'", &postgres).is_err());
        assert!(
            check_sql(
                "CREATE EXTERNAL TABLE t (a int) STORED AS CSV LOCATION '/x'",
                &postgres
            )
            .is_err()
        );
        assert!(check_sql("SELECT \"\" FROM t", &postgres).is_err());
        assert!(check_sql("SELECT 1; SELECT 2", &postgres).is_ok());
        assert!(
            check_sql("not sql at all", &postgres).is_ok(),
            "left to the parser"
        );
    }

    #[test]
    fn zero_length_delimited_identifiers_are_syntax_errors() {
        let dialect = GenericDialect {};
        for sql in [
            "SELECT 1 AS \"\"",
            "CREATE VIEW hive.s.\"\" AS SELECT 1",
            "CREATE SCHEMA hive.\"\"",
            "DROP VIEW hive.\"\".victim",
        ] {
            let err = check_tokens(sql, &dialect).unwrap_err();
            assert!(matches!(err, DataFusionError::SQL(..)), "{sql}: {err}");
            assert!(
                err.to_string()
                    .contains("Zero-length delimited identifier not allowed")
            );
        }
        for sql in [
            "SELECT '' AS a",
            "SELECT 'a\"\"b'",
            "SELECT \"a\"\"b\" FROM t",
            "SELECT \" \"",
        ] {
            check_tokens(sql, &dialect).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[test]
    fn names_that_are_not_their_own_directory_are_refused() {
        for name in ["", ".", "..", "a/b", "..\\x", "../outside", "a\0b", "tab\tname"] {
            let err = check_object_name(ObjectKind::Table, name).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Error during planning: GENERIC_USER_ERROR: Invalid table name"
            );
        }
        for name in ["t", "ação", ".hidden", "_u", "a.b", "%2E%2E", "MixedCase", "..."] {
            check_object_name(ObjectKind::Schema, name).unwrap_or_else(|e| panic!("{name}: {e}"));
        }
        assert!(
            check_object_name(ObjectKind::Column, "")
                .unwrap_err()
                .to_string()
                .contains("Invalid column name")
        );
        check_object_name(ObjectKind::Column, "t.a / t.b").unwrap();
    }

    #[test]
    fn stored_views_must_be_queries() {
        let dialect = GenericDialect {};
        for sql in [
            "SELECT 1",
            "WITH t AS (SELECT 1 AS a) SELECT a FROM t",
            "VALUES (1)",
        ] {
            parse_view_query(sql, &dialect).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        for sql in [
            "INSERT INTO hive.s.t VALUES (99)",
            "COPY (SELECT 'x' AS a) TO '/tmp/x.csv' STORED AS CSV",
            "DROP TABLE hive.s.t",
            "SELECT 1; INSERT INTO t VALUES (1)",
            "CREATE TABLE t AS SELECT 1",
        ] {
            assert!(parse_view_query(sql, &dialect).is_err(), "{sql}");
        }
    }
}
