//! Trino syntax the SQL parser rejects, rewritten on the text before parsing.
//!
//! Only the matched spans are edited (from token positions), so string
//! literals, comments and everything else stay byte for byte:
//!
//! - `VALUES 1, 2` (rows without parentheses) becomes `VALUES (1), (2)`;
//! - `CAST(x AS array(T))`, `map(K, V)` and `row(…)` targets, which the parser
//!   only knows with angle brackets, become
//!   `codelake_trino_cast_type(x, 'array(T)')` (`TRY_CAST`: the `try` form),
//!   planned by `cast_type` with Trino's type grammar.
//!
//! A text the tokenizer rejects is returned unchanged: the parser reports the
//! error as it would have.

use std::borrow::Cow;

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

use super::cast_type::{CAST_TYPE, TRY_CAST_TYPE};

/// Guards against pathological inputs: each pass rewrites one construct.
const MAX_PASSES: usize = 10_000;

/// Byte offset of every line start, to turn token locations into offsets.
struct Offsets<'a> {
    text: &'a str,
    lines: Vec<usize>,
}

impl<'a> Offsets<'a> {
    fn new(text: &'a str) -> Self {
        let mut lines = vec![0];
        lines.extend(text.match_indices('\n').map(|(i, _)| i + 1));
        Self { text, lines }
    }

    /// Byte offset of a 1-based (line, column-in-characters) location.
    fn of(&self, location: Location) -> Option<usize> {
        let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        let start = *self.lines.get(line)?;
        let rest = self.text.get(start..)?;
        Some(start + rest.char_indices().nth(column).map_or(rest.len(), |(i, _)| i))
    }
}

/// Tokens without whitespace, with byte spans.
fn tokens(text: &str) -> Option<Vec<(Token, usize, usize)>> {
    let dialect = GenericDialect {};
    let raw: Vec<TokenWithSpan> = Tokenizer::new(&dialect, text).tokenize_with_location().ok()?;
    let offsets = Offsets::new(text);
    raw.into_iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_) | Token::EOF))
        .map(|t| Some((t.token, offsets.of(t.span.start)?, offsets.of(t.span.end)?)))
        .collect()
}

fn is_keyword(token: &Token, keyword: Keyword) -> bool {
    matches!(token, Token::Word(w) if w.keyword == keyword && w.quote_style.is_none())
}

fn is_word(token: &Token, name: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(name))
}

/// Index of the token closing the parenthesis opened at `open`.
fn closing(tokens: &[(Token, usize, usize)], open: usize) -> Option<usize> {
    let mut depth = 0_usize;
    for (i, (token, _, _)) in tokens.iter().enumerate().skip(open) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// `VALUES a, b` → `VALUES (a), (b)` for the first such list; `None` if none.
fn values_rows(text: &str, tokens: &[(Token, usize, usize)]) -> Option<String> {
    let start = tokens.iter().enumerate().position(|(i, (t, _, _))| {
        is_keyword(t, Keyword::VALUES)
            && tokens
                .get(i + 1)
                .is_some_and(|(next, _, _)| !matches!(next, Token::LParen | Token::SemiColon))
    })? + 1;
    let ends_list = |t: &Token| {
        matches!(t, Token::SemiColon)
            || [
                Keyword::ORDER,
                Keyword::LIMIT,
                Keyword::OFFSET,
                Keyword::FETCH,
                Keyword::UNION,
                Keyword::EXCEPT,
                Keyword::INTERSECT,
            ]
            .iter()
            .any(|k| is_keyword(t, *k))
    };
    // Byte ranges of each row expression.
    let mut rows: Vec<(usize, usize)> = Vec::new();
    let mut depth = 0_usize;
    let mut row_start: Option<usize> = None;
    let mut last_end = 0;
    for (token, from, to) in tokens.iter().skip(start) {
        if depth == 0 && (matches!(token, Token::RParen) || ends_list(token)) {
            break;
        }
        match token {
            Token::Comma if depth == 0 => {
                rows.push((row_start?, last_end));
                row_start = None;
                continue;
            }
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ => {}
        }
        row_start.get_or_insert(*from);
        last_end = *to;
    }
    rows.push((row_start?, last_end));
    let mut out = String::with_capacity(text.len() + 2 * rows.len());
    let mut copied = 0;
    for (from, to) in rows {
        out.push_str(text.get(copied..from)?);
        out.push('(');
        out.push_str(text.get(from..to)?);
        out.push(')');
        copied = to;
    }
    out.push_str(text.get(copied..)?);
    Some(out)
}

/// The last `CAST(x AS array|map|row(…))` rewritten; `None` if there is none.
/// The last one to start contains no other, so rewriting from the end
/// handles nesting.
fn container_cast(text: &str, tokens: &[(Token, usize, usize)]) -> Option<String> {
    for i in (0..tokens.len()).rev() {
        let (token, cast_start, _) = &tokens[i];
        let function = if is_keyword(token, Keyword::CAST) {
            CAST_TYPE
        } else if is_keyword(token, Keyword::TRY_CAST) {
            TRY_CAST_TYPE
        } else {
            continue;
        };
        if !matches!(tokens.get(i + 1), Some((Token::LParen, _, _))) {
            continue;
        }
        let Some(close) = closing(tokens, i + 1) else {
            continue;
        };
        // `AS` at the cast's own depth.
        let mut depth = 0_usize;
        let mut as_index = None;
        for (j, (t, _, _)) in tokens.iter().enumerate().take(close).skip(i + 2) {
            match t {
                Token::LParen => depth += 1,
                Token::RParen => depth = depth.saturating_sub(1),
                _ if depth == 0 && is_keyword(t, Keyword::AS) => as_index = Some(j),
                _ => {}
            }
        }
        let Some(as_index) = as_index else { continue };
        let (Some((type_word, type_start, _)), Some((Token::LParen, _, _))) =
            (tokens.get(as_index + 1), tokens.get(as_index + 2))
        else {
            continue;
        };
        if !["array", "map", "row"]
            .iter()
            .any(|name| is_word(type_word, name))
        {
            continue;
        }
        let (_, expr_start, _) = tokens.get(i + 2)?;
        let (_, as_start, _) = tokens.get(as_index)?;
        let (_, close_start, close_end) = tokens.get(close)?;
        let expression = text.get(*expr_start..*as_start)?.trim_end();
        let type_text = text.get(*type_start..*close_start)?.trim().replace('\'', "''");
        let mut out = String::with_capacity(text.len() + 32);
        out.push_str(text.get(..*cast_start)?);
        out.push_str(function);
        out.push('(');
        out.push_str(expression);
        out.push_str(", '");
        out.push_str(&type_text);
        out.push_str("')");
        out.push_str(text.get(*close_end..)?);
        return Some(out);
    }
    None
}

/// Rewrites the constructs listed in the module docs.
pub(crate) fn preprocess(sql: &str) -> Cow<'_, str> {
    let mut text = Cow::Borrowed(sql);
    for _ in 0..MAX_PASSES {
        let Some(tokens) = tokens(&text) else { return text };
        let next = container_cast(&text, &tokens).or_else(|| values_rows(&text, &tokens));
        match next {
            Some(rewritten) => text = Cow::Owned(rewritten),
            None => return text,
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_rows_get_parentheses() {
        assert_eq!(
            preprocess("SELECT x FROM (VALUES 1, 2, NULL) t(x)"),
            "SELECT x FROM (VALUES (1), (2), (NULL)) t(x)"
        );
        assert_eq!(
            preprocess("SELECT * FROM (VALUES CAST(1 AS bigint), f(2, 3)) t(x) ORDER BY 1"),
            "SELECT * FROM (VALUES (CAST(1 AS bigint)), (f(2, 3))) t(x) ORDER BY 1"
        );
        assert_eq!(preprocess("VALUES 'a''b', 'ç'"), "VALUES ('a''b'), ('ç')");
        let untouched = "INSERT INTO t VALUES (1, 'a'), (2, 'b')";
        assert_eq!(preprocess(untouched), untouched);
        assert_eq!(
            preprocess("VALUES 1 UNION ALL VALUES 2"),
            "VALUES (1) UNION ALL VALUES (2)"
        );
    }

    #[test]
    fn container_casts_carry_trino_type_text() {
        assert_eq!(
            preprocess(
                "SELECT CAST(NULL AS array(bigint)), TRY_CAST(j AS map(varchar, array(integer))) FROM t"
            ),
            format!(
                "SELECT {CAST_TYPE}(NULL, 'array(bigint)'), {TRY_CAST_TYPE}(j, 'map(varchar, array(integer))') FROM t"
            )
        );
        assert_eq!(
            preprocess("SELECT CAST(CAST(x AS row(a integer)) AS array(row(a integer)))"),
            format!("SELECT {CAST_TYPE}({CAST_TYPE}(x, 'row(a integer)'), 'array(row(a integer))')")
        );
        let untouched = "SELECT CAST(x AS varchar), 'CAST(y AS array(bigint))', ARRAY[1]";
        assert_eq!(preprocess(untouched), untouched);
    }

    #[test]
    fn multi_line_text_keeps_offsets() {
        assert_eq!(
            preprocess("SELECT\n  'ação',\n  CAST(x AS array(varchar))\nFROM t"),
            format!("SELECT\n  'ação',\n  {CAST_TYPE}(x, 'array(varchar)')\nFROM t")
        );
    }
}
