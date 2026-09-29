//! `SHOW CREATE TABLE` and `SHOW CREATE VIEW`: the statement that creates a
//! relation as it is now, one row in one `Create Table` (`Create View`)
//! column, printed as the reference engine prints it:
//!
//! ```text
//! CREATE TABLE hive.raw.t (
//!    id bigint NOT NULL,
//!    amount decimal(12, 2) COMMENT 'net',
//!    "from" ROW(x integer, "y z" varchar)
//! )
//! WITH (
//!    format = 'PARQUET'
//! )
//! ```
//!
//! A table's columns come from its schema: types in SQL syntax
//! (`decimal(12, 2)`, `ROW(…)`), `NOT NULL` for a column that holds no
//! nulls, the comment its field records. The `WITH` clause holds the catalog's
//! [`WritableCatalog::table_properties`], sorted by name; a table without
//! any (a federated source's) has none. A view is `CREATE VIEW <name>
//! SECURITY DEFINER|INVOKER AS` and its query as `information_schema.views`
//! prints it.
//!
//! Names are quoted when they are not a plain identifier or are a reserved
//! word (`"from"`). The column's type is `varchar(n)`, `n` the length of the
//! text.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

use crate::connector::{COLUMN_COMMENT, ViewSecurity, WritableCatalog};
use crate::trino_types::{Declared, declared, display_name};

const INDENT: &str = "   ";

/// Words a name cannot be without quotes: `SELECT "from" FROM t`.
const RESERVED: &[&str] = &[
    "ALTER",
    "AND",
    "AS",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_PATH",
    "CURRENT_ROLE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEALLOCATE",
    "DELETE",
    "DESCRIBE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXECUTE",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IN",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "JSON_ARRAY",
    "JSON_EXISTS",
    "JSON_OBJECT",
    "JSON_QUERY",
    "JSON_TABLE",
    "JSON_VALUE",
    "LEFT",
    "LIKE",
    "LISTAGG",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NATURAL",
    "NORMALIZE",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PREPARE",
    "RECURSIVE",
    "RIGHT",
    "ROLLUP",
    "SELECT",
    "SKIP",
    "TABLE",
    "THEN",
    "TRIM",
    "TRUE",
    "UESCAPE",
    "UNION",
    "UNNEST",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    "WITH",
];

/// The relation `name` names, `None` when its catalog, schema or itself
/// does not exist.
async fn relation(
    ctx: &SessionContext,
    [catalog, schema, name]: [&str; 3],
) -> Result<Option<Arc<dyn TableProvider>>> {
    match ctx.catalog(catalog).and_then(|c| c.schema(schema)) {
        Some(provider) => provider.table(name).await,
        None => Ok(None),
    }
}

/// `SHOW CREATE TABLE catalog.schema.table` (the name resolved against the
/// session defaults); `writable` is the catalog's DDL support, which knows
/// the table's properties.
pub(crate) async fn create_table(
    ctx: &SessionContext,
    name: [&str; 3],
    writable: Option<&Arc<dyn WritableCatalog>>,
) -> Result<DataFrame> {
    let qualified = name.join(".");
    let provider = relation(ctx, name)
        .await?
        .ok_or_else(|| DataFusionError::Plan(format!("Table '{qualified}' does not exist")))?;
    if provider.table_type() == TableType::View {
        return Err(DataFusionError::NotImplemented(format!(
            "Relation '{qualified}' is a view, not a table"
        )));
    }
    let properties = match writable {
        Some(writable) => writable.table_properties(name[1], name[2]).await?,
        None => BTreeMap::new(),
    };
    let text = table_statement(
        name,
        provider.schema().fields().iter().map(AsRef::as_ref),
        &properties,
    );
    answer(ctx, "Create Table", text)
}

/// `SHOW CREATE VIEW catalog.schema.view`; `writable` knows the view's
/// security (a catalog without one has only `DEFINER` views).
pub(crate) async fn create_view(
    ctx: &SessionContext,
    name: [&str; 3],
    writable: Option<&Arc<dyn WritableCatalog>>,
) -> Result<DataFrame> {
    let qualified = name.join(".");
    let provider = relation(ctx, name)
        .await?
        .ok_or_else(|| DataFusionError::Plan(format!("View '{qualified}' does not exist")))?;
    if provider.table_type() != TableType::View {
        return Err(DataFusionError::NotImplemented(format!(
            "Relation '{qualified}' is a table, not a view"
        )));
    }
    let query = provider
        .get_table_definition()
        .ok_or_else(|| DataFusionError::Plan(format!("View '{qualified}' has no definition")))?;
    let security = match writable {
        Some(writable) => writable.view_security(name[1], name[2]).await?,
        None => ViewSecurity::Definer,
    };
    answer(ctx, "Create View", view_statement(name, security, query))
}

/// One row, one `varchar(n)` column `column` holding `text`.
fn answer(ctx: &SessionContext, column: &str, text: String) -> Result<DataFrame> {
    let length = u32::try_from(text.chars().count()).unwrap_or(u32::MAX);
    let field = Field::new(column, DataType::Utf8, false).with_metadata(Declared::Varchar(length).metadata());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(StringArray::from(vec![text]))],
    )?;
    ctx.read_batch(batch)
}

/// The `CREATE TABLE` text of table `name` with `columns` and `properties`.
fn table_statement<'a>(
    name: [&str; 3],
    columns: impl Iterator<Item = &'a Field>,
    properties: &BTreeMap<String, String>,
) -> String {
    let columns: Vec<String> = columns
        .map(|field| format!("{INDENT}{}", column(field)))
        .collect();
    let mut text = format!("CREATE TABLE {} (\n{}\n)", qualified(name), columns.join(",\n"));
    if !properties.is_empty() {
        let properties: Vec<String> = properties
            .iter()
            .map(|(key, value)| format!("{INDENT}{} = {}", identifier(key), string_literal(value)))
            .collect();
        let _ = write!(text, "\nWITH (\n{}\n)", properties.join(",\n"));
    }
    text
}

/// The `CREATE VIEW` text of view `name` over `query` (its SQL as stored).
fn view_statement(name: [&str; 3], security: ViewSecurity, query: &str) -> String {
    let security = match security {
        ViewSecurity::Definer => "DEFINER",
        ViewSecurity::Invoker => "INVOKER",
    };
    let query = crate::trino_sql::view_definition(query);
    format!(
        "CREATE VIEW {} SECURITY {security} AS\n{}",
        qualified(name),
        query.trim_end()
    )
}

fn qualified(name: [&str; 3]) -> String {
    name.map(identifier).join(".")
}

/// One column definition: name, type, `NOT NULL`, `COMMENT`.
fn column(field: &Field) -> String {
    let mut text = format!("{} {}", identifier(field.name()), sql_type(field));
    if !field.is_nullable() {
        text.push_str(" NOT NULL");
    }
    if let Some(comment) = field.metadata().get(COLUMN_COMMENT) {
        let _ = write!(text, " COMMENT {}", string_literal(comment));
    }
    text
}

/// The type of `field` in SQL syntax: its name as clients see it, with a
/// space after each comma of the parameters (`decimal(12, 2)`) and rows as
/// `ROW(name type, …)`, their field names as identifiers.
fn sql_type(field: &Field) -> String {
    if let Some(declared) = declared(field) {
        return declared.name(field.data_type());
    }
    match field.data_type() {
        DataType::List(element) | DataType::LargeList(element) | DataType::FixedSizeList(element, _) => {
            format!("array({})", sql_type(element))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => match &fields[..] {
                [key, value] => format!("map({}, {})", sql_type(key), sql_type(value)),
                _ => display_name(field.data_type()),
            },
            _ => display_name(field.data_type()),
        },
        DataType::Struct(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|f| format!("{} {}", identifier(f.name()), sql_type(f)))
                .collect();
            format!("ROW({})", fields.join(", "))
        }
        other => display_name(other).replace(',', ", "),
    }
}

/// A name as written in SQL: bare when it is a letter or `_` followed by
/// letters, digits and `_`, and not a [`RESERVED`] word;
/// otherwise double-quoted, a `"` doubled.
fn identifier(name: &str) -> String {
    let mut chars = name.chars();
    let bare = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !RESERVED.contains(&name.to_ascii_uppercase().as_str());
    if bare {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// A string literal: `'…'` with `'` doubled when every character is
/// printable ASCII; otherwise a Unicode literal, `U&'…'`, with each other
/// character as `\XXXX` (`\+XXXXXX` beyond the basic plane) and `\` doubled.
fn string_literal(value: &str) -> String {
    let value = value.replace('\'', "''");
    let printable = |c: char| (' '..='~').contains(&c);
    if value.chars().all(printable) {
        return format!("'{value}'");
    }
    let mut text = String::from("U&'");
    for c in value.chars() {
        match c {
            '\\' => text.push_str("\\\\"),
            c if printable(c) => text.push(c),
            c if u32::from(c) <= 0xFFFF => {
                let _ = write!(text, "\\{:04X}", u32::from(c));
            }
            c => {
                let _ = write!(text, "\\+{:06X}", u32::from(c));
            }
        }
    }
    text.push('\'');
    text
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{Fields, TimeUnit};

    use super::*;

    #[test]
    fn columns_properties_and_their_absence() {
        let columns = [
            Field::new("start_lsn", DataType::Binary, true),
            Field::new(
                "tran_begin_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("id", DataType::Int32, false),
        ];
        let properties = BTreeMap::from([("format".to_owned(), "PARQUET".to_owned())]);
        assert_eq!(
            table_statement(["hive", "raw", "t"], columns.iter(), &properties),
            "CREATE TABLE hive.raw.t (\n   start_lsn varbinary,\n   tran_begin_time timestamp(3),\n   \
             id integer NOT NULL\n)\nWITH (\n   format = 'PARQUET'\n)"
        );
        assert_eq!(
            table_statement(["db", "dbo", "t"], columns[2..].iter(), &BTreeMap::new()),
            "CREATE TABLE db.dbo.t (\n   id integer NOT NULL\n)"
        );
        let two = BTreeMap::from([
            ("format".to_owned(), "ORC".to_owned()),
            ("external_location".to_owned(), "s3://b/it's".to_owned()),
        ]);
        assert!(
            table_statement(["c", "s", "t"], columns[..1].iter(), &two)
                .ends_with("WITH (\n   external_location = 's3://b/it''s',\n   format = 'ORC'\n)"),
        );
        assert!(
            table_statement(["c", "s", "from"], columns[..1].iter(), &BTreeMap::new())
                .starts_with("CREATE TABLE c.s.\"from\" (\n")
        );
    }

    #[test]
    fn a_column_prints_its_comment() {
        let field = Field::new("v", DataType::Utf8, true)
            .with_metadata(HashMap::from([(COLUMN_COMMENT.to_owned(), "it's".to_owned())]));
        assert_eq!(column(&field), "v varchar COMMENT 'it''s'");
    }

    #[test]
    fn types_are_printed_in_sql_syntax() {
        let row = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Decimal128(12, 2), true),
            Field::new("y z", DataType::Utf8, true),
        ]));
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new(
                        "value",
                        DataType::new_list(DataType::Decimal128(12, 2), true),
                        true,
                    ),
                ])),
                false,
            )),
            false,
        );
        let varchar = Field::new("v", DataType::Utf8, true).with_metadata(Declared::Varchar(10).metadata());
        for (field, printed) in [
            (
                Field::new("d", DataType::Decimal128(38, 5), true),
                "decimal(38, 5)",
            ),
            (Field::new("u", DataType::UInt64, true), "decimal(20, 0)"),
            (
                Field::new("r", row.clone(), true),
                "ROW(x decimal(12, 2), \"y z\" varchar)",
            ),
            (
                Field::new("a", DataType::new_list(row, true), true),
                "array(ROW(x decimal(12, 2), \"y z\" varchar))",
            ),
            (Field::new("m", map, true), "map(varchar, array(decimal(12, 2)))"),
            (
                Field::new(
                    "tz",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    true,
                ),
                "timestamp(3) with time zone",
            ),
            (varchar.clone(), "varchar(10)"),
            (
                Field::new("l", DataType::new_list(DataType::Utf8, true), true)
                    .with_metadata(varchar.metadata().clone()),
                "array(varchar)",
            ),
        ] {
            assert_eq!(sql_type(&field), printed, "{field:?}");
        }
    }

    #[test]
    fn names_are_quoted_unless_bare() {
        for (name, printed) in [
            ("id", "id"),
            ("_sink_loaded_at", "_sink_loaded_at"),
            ("Mixed9", "Mixed9"),
            ("a:b@c", "\"a:b@c\""),
            ("user", "user"),
            ("from", "\"from\""),
            ("End", "\"End\""),
            ("json_table", "\"json_table\""),
            ("with", "\"with\""),
            ("alter", "\"alter\""),
            ("9a", "\"9a\""),
            (":a", "\":a\""),
            ("a b", "\"a b\""),
            ("a-b", "\"a-b\""),
            ("q\"t", "\"q\"\"t\""),
            ("código", "\"código\""),
            ("", "\"\""),
        ] {
            assert_eq!(identifier(name), printed, "{name}");
        }
    }

    #[test]
    fn text_outside_printable_ascii_is_a_unicode_literal() {
        assert_eq!(string_literal("PARQUET"), "'PARQUET'");
        assert_eq!(string_literal(" ~"), "' ~'");
        assert_eq!(string_literal("a\\b"), "'a\\b'");
        assert_eq!(string_literal("ção'\\"), "U&'\\00E7\\00E3o''\\\\'");
        assert_eq!(string_literal("\u{7f}\u{1f}"), "U&'\\007F\\001F'");
        assert_eq!(string_literal("\u{FFFF}\u{1F600}"), "U&'\\FFFF\\+01F600'");
    }

    #[test]
    fn a_view_is_its_security_and_formatted_query() {
        assert_eq!(
            view_statement(
                ["hive", "s", "v"],
                ViewSecurity::Definer,
                "SELECT id, dc * 2 AS dc2 FROM hive.s.t WHERE id > 1"
            ),
            "CREATE VIEW hive.s.v SECURITY DEFINER AS\nSELECT\n  id\n, (dc * 2) dc2\nFROM\n  hive.s.t\nWHERE (id > 1)"
        );
        assert_eq!(
            view_statement(
                ["hive", "s", "select"],
                ViewSecurity::Invoker,
                "SELECT id FROM hive.s.t"
            ),
            "CREATE VIEW hive.s.\"select\" SECURITY INVOKER AS\nSELECT id\nFROM\n  hive.s.t"
        );
    }
}
