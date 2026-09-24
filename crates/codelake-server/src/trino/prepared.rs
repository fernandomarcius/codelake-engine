//! Prepared statements, as Trino's protocol carries them: the server keeps
//! nothing. `PREPARE name FROM sql` answers `X-Trino-Added-Prepare`, the client
//! sends every prepared statement back in `X-Trino-Prepared-Statement`, and
//! `DEALLOCATE PREPARE name` answers `X-Trino-Deallocated-Prepare`.
//! `EXECUTE name USING ...` and `EXECUTE IMMEDIATE 'sql' USING ...` bind the
//! parameters.
//!
//! Binding replaces each `?` of the parsed statement with the parsed parameter
//! expression — the tree, never the text — so a parameter cannot change the
//! statement's structure (`'x'' OR 1=1'` stays one string literal).

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{Expr, Value, visit_expressions, visit_expressions_mut};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Location, Token};

use super::errors::{Code, QueryError};
use super::session::{percent_decode, strip_keyword};

/// A statement about prepared statements.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Command {
    Prepare { name: String, sql: String },
    Deallocate { name: String },
    Execute { name: String, parameters: Vec<Expr> },
    ExecuteImmediate { sql: String, parameters: Vec<Expr> },
}

fn syntax(message: impl Into<String>) -> QueryError {
    QueryError::new(Code::SyntaxError, message)
}

fn parser_error(error: &ParserError) -> QueryError {
    syntax(error.to_string())
}

/// An identifier at the start of `text`: `"quoted ""name"""` or a bare word.
/// Returns it and the rest of the text.
fn identifier(text: &str) -> Option<(String, &str)> {
    let text = text.trim_start();
    if let Some(quoted) = text.strip_prefix('"') {
        let mut name = String::new();
        let mut chars = quoted.char_indices().peekable();
        while let Some((index, c)) = chars.next() {
            if c == '"' {
                if chars.peek().is_some_and(|(_, next)| *next == '"') {
                    chars.next();
                    name.push('"');
                } else {
                    return (!name.is_empty()).then(|| (name, &quoted[index + 1..]));
                }
            } else {
                name.push(c);
            }
        }
        return None;
    }
    let end = text
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    (end > 0).then(|| (text[..end].to_owned(), &text[end..]))
}

fn only_end(rest: &str) -> bool {
    rest.trim().trim_end_matches(';').trim().is_empty()
}

/// Parameters after `USING`, then the end of the statement.
fn using(parser: &mut Parser<'_>) -> Result<Vec<Expr>, QueryError> {
    let parameters = if parser.parse_keyword(Keyword::USING) {
        parser
            .parse_comma_separated(Parser::parse_expr)
            .map_err(|e| parser_error(&e))?
    } else {
        Vec::new()
    };
    while parser.consume_token(&Token::SemiColon) {}
    let next = parser.peek_token();
    if next.token != Token::EOF {
        return Err(syntax(format!("Unexpected {} after EXECUTE", next.token)));
    }
    Ok(parameters)
}

/// Recognizes `PREPARE`, `DEALLOCATE PREPARE`, `EXECUTE` and `EXECUTE IMMEDIATE`.
pub(crate) fn command(sql: &str) -> Result<Option<Command>, QueryError> {
    let text = sql.trim();
    if let Some(rest) = strip_keyword(text, "PREPARE") {
        let (name, rest) = identifier(rest).ok_or_else(|| syntax("PREPARE expects a statement name"))?;
        let statement = strip_keyword(rest.trim_start(), "FROM")
            .ok_or_else(|| syntax("PREPARE expects: PREPARE name FROM statement"))?;
        let statement = statement.trim().trim_end_matches(';').trim_end();
        return Ok(Some(Command::Prepare {
            name,
            sql: statement.to_owned(),
        }));
    }
    if let Some(rest) = strip_keyword(text, "DEALLOCATE") {
        let rest = strip_keyword(rest, "PREPARE").ok_or_else(|| syntax("DEALLOCATE expects PREPARE"))?;
        let (name, rest) =
            identifier(rest).ok_or_else(|| syntax("DEALLOCATE PREPARE expects a statement name"))?;
        if !only_end(rest) {
            return Err(syntax("Unexpected text after DEALLOCATE PREPARE name"));
        }
        return Ok(Some(Command::Deallocate { name }));
    }
    let Some(rest) = strip_keyword(text, "EXECUTE") else {
        return Ok(None);
    };
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(rest)
        .map_err(|e| parser_error(&e))?;
    if parser.parse_keyword(Keyword::IMMEDIATE) {
        let statement = parser.parse_literal_string().map_err(|e| parser_error(&e))?;
        let parameters = using(&mut parser)?;
        return Ok(Some(Command::ExecuteImmediate {
            sql: statement,
            parameters,
        }));
    }
    let name = parser.parse_identifier().map_err(|e| parser_error(&e))?.value;
    let parameters = using(&mut parser)?;
    Ok(Some(Command::Execute { name, parameters }))
}

fn is_placeholder(expr: &Expr) -> Option<Location> {
    match expr {
        Expr::Value(value) if matches!(&value.value, Value::Placeholder(p) if p == "?") => {
            Some(value.span.start)
        }
        _ => None,
    }
}

/// A parameter as it replaces `?`: parenthesized unless it is a single term,
/// so `? * 3` with `1 + 2` stays `(1 + 2) * 3`.
fn operand(parameter: &Expr) -> Expr {
    match parameter {
        Expr::Value(_)
        | Expr::TypedString(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Function(_)
        | Expr::Cast { .. }
        | Expr::Nested(_)
        | Expr::Array(_) => parameter.clone(),
        other => Expr::Nested(Box::new(other.clone())),
    }
}

/// The statement `sql` with its `?` placeholders replaced, in textual order,
/// by `parameters`.
pub(crate) fn bind(sql: &str, parameters: &[Expr]) -> Result<String, QueryError> {
    let mut statements = DFParser::parse_sql(sql).map_err(|e| QueryError::from(&e))?;
    if statements.len() != 1 {
        return Err(syntax("A prepared statement must hold exactly one statement"));
    }
    let Some(DfStatement::Statement(mut statement)) = statements.pop_front() else {
        return if parameters.is_empty() {
            Ok(sql.to_owned())
        } else {
            Err(QueryError::new(
                Code::NotSupported,
                "Parameters are only supported in SQL statements",
            ))
        };
    };
    let mut found = Vec::new();
    let _ = visit_expressions(statement.as_ref(), |expr| {
        if let Some(at) = is_placeholder(expr) {
            found.push(at);
        }
        ControlFlow::<()>::Continue(())
    });
    if found.len() != parameters.len() {
        return Err(QueryError::new(
            Code::InvalidParameterUsage,
            format!(
                "Incorrect number of parameters: expected {} but found {}",
                found.len(),
                parameters.len()
            ),
        ));
    }
    if found.is_empty() {
        return Ok(sql.to_owned());
    }
    // Parameters follow the text; the tree may list placeholders in another order.
    let mut textual = found.clone();
    textual.sort_unstable();
    textual.dedup();
    let by_position = textual.len() == found.len() && textual.iter().all(|l| l.line > 0);
    let mut visited = 0;
    let _ = visit_expressions_mut(statement.as_mut(), |expr| {
        if let Some(at) = is_placeholder(expr) {
            let index = if by_position {
                textual.binary_search(&at).unwrap_or(visited)
            } else {
                visited
            };
            visited += 1;
            if let Some(parameter) = parameters.get(index) {
                *expr = operand(parameter);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(statement.to_string())
}

/// `X-Trino-Prepared-Statement`: `name=<url-encoded sql>` pairs, comma-separated,
/// possibly over several header lines.
pub(crate) fn parse_header<'a>(
    values: impl Iterator<Item = &'a str>,
) -> Result<BTreeMap<String, String>, String> {
    let mut prepared = BTreeMap::new();
    for value in values {
        for pair in value.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (name, sql) = pair
                .split_once('=')
                .ok_or_else(|| format!("Invalid X-Trino-Prepared-Statement header: {pair}"))?;
            let decode = |text: &str| {
                percent_decode(text.trim())
                    .map_err(|_| format!("Invalid X-Trino-Prepared-Statement header: {pair}"))
            };
            prepared.insert(decode(name)?, decode(sql)?);
        }
    }
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(text: &str) -> Vec<Expr> {
        match command(&format!("EXECUTE s USING {text}")).unwrap() {
            Some(Command::Execute { parameters, .. }) => parameters,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn commands_are_recognized() {
        assert_eq!(
            command("PREPARE statement1 FROM SELECT a FROM t WHERE a = ?;").unwrap(),
            Some(Command::Prepare {
                name: "statement1".into(),
                sql: "SELECT a FROM t WHERE a = ?".into()
            })
        );
        assert_eq!(
            command("prepare \"My \"\"st\"\" \" from\nSELECT 1").unwrap(),
            Some(Command::Prepare {
                name: "My \"st\" ".into(),
                sql: "SELECT 1".into()
            })
        );
        assert_eq!(
            command("DEALLOCATE PREPARE st_1").unwrap(),
            Some(Command::Deallocate { name: "st_1".into() })
        );
        let Some(Command::ExecuteImmediate { sql, parameters }) =
            command("EXECUTE IMMEDIATE 'SELECT ?, ''x''' USING 1, 'a''b'").unwrap()
        else {
            panic!()
        };
        assert_eq!(sql, "SELECT ?, 'x'");
        assert_eq!(parameters.len(), 2);
        assert_eq!(params("DATE '1969-07-20', NULL").len(), 2);
        assert!(command("EXECUTE s USING 1 garbage").is_err());
        assert!(command("PREPARE s SELECT 1").is_err());
        assert_eq!(command("SELECT 1").unwrap(), None);
        assert_eq!(command("EXECUTED_BY").unwrap(), None);
    }

    #[test]
    fn parameters_are_bound_in_the_tree() {
        assert_eq!(
            bind("SELECT ?, ?", &params("1, 'a''b'")).unwrap(),
            "SELECT 1, 'a''b'"
        );
        // Text that looks like SQL stays one string literal.
        assert_eq!(
            bind("SELECT * FROM t WHERE b = ?", &params("'x'' OR 1=1 --'")).unwrap(),
            "SELECT * FROM t WHERE b = 'x'' OR 1=1 --'"
        );
        assert_eq!(
            bind("SELECT ? * 3", &params("1 + 2")).unwrap(),
            "SELECT (1 + 2) * 3"
        );
        assert_eq!(
            bind(
                "SELECT a FROM t WHERE a = ? AND b <> ? LIMIT ?",
                &params("BIGINT '1', 'x', 5")
            )
            .unwrap(),
            "SELECT a FROM t WHERE a = BIGINT '1' AND b <> 'x' LIMIT 5"
        );
        assert_eq!(bind("SELECT 1", &[]).unwrap(), "SELECT 1");
    }

    #[test]
    fn parameter_count_must_match() {
        let error = bind("SELECT ?, ?", &params("1")).unwrap_err();
        assert_eq!(error.name(), "INVALID_PARAMETER_USAGE");
        assert!(bind("SELECT 1", &params("1")).is_err());
    }

    #[test]
    fn header_lists_prepared_statements() {
        let parsed = parse_header(["a=SELECT+1%2C+2", "b=SELECT%20%3F"].into_iter()).unwrap();
        assert_eq!(parsed["a"], "SELECT 1, 2");
        assert_eq!(parsed["b"], "SELECT ?");
        assert!(parse_header(["nope"].into_iter()).is_err());
    }
}
