//! Reader for Java `.properties` files, with the semantics of
//! `java.util.Properties.load`: `#`/`!` comments, blank lines, `=`, `:` or
//! whitespace as the separator, backslash line continuation and the escapes
//! `\t \n \r \f \uXXXX` (any other escaped character stands for itself).
//!
//! Errors carry the line number only, never the line: a catalog file holds
//! passwords.

use std::collections::BTreeMap;

/// A malformed file. The message never quotes the file's content.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("line {line}: {problem}")]
pub(crate) struct PropertiesError {
    pub(crate) line: usize,
    pub(crate) problem: &'static str,
}

/// Decodes a file's bytes: UTF-8 when valid, ISO-8859-1 otherwise (the
/// encoding `Properties.load(InputStream)` assumes).
pub(crate) fn decode(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_owned(),
        Err(_) => bytes.iter().map(|&b| char::from(b)).collect(),
    }
}

/// Parses a properties document. A key that appears twice keeps its last
/// value, as in Java.
pub(crate) fn parse(text: &str) -> Result<BTreeMap<String, String>, PropertiesError> {
    let mut out = BTreeMap::new();
    for (line, logical) in logical_lines(text) {
        let (key, value) = split_key_value(&logical);
        let key = unescape(key, line)?;
        let value = unescape(value, line)?;
        out.insert(key, value);
    }
    Ok(out)
}

const fn is_blank(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\u{c}')
}

/// Logical lines (continuations joined), without comments and blank lines,
/// each with the number of its first natural line.
fn logical_lines(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut current: Option<(usize, String)> = None;
    for (index, natural) in natural_lines(text).into_iter().enumerate() {
        let number = index + 1;
        let trimmed = natural.trim_start_matches(is_blank);
        let (start, mut buffer) = if let Some(open) = current.take() {
            open
        } else {
            if trimmed.is_empty() || trimmed.starts_with(['#', '!']) {
                continue;
            }
            (number, String::new())
        };
        let trailing = trimmed.chars().rev().take_while(|&c| c == '\\').count();
        if trailing % 2 == 1 {
            buffer.push_str(&trimmed[..trimmed.len() - 1]);
            current = Some((start, buffer));
        } else {
            buffer.push_str(trimmed);
            out.push((start, buffer));
        }
    }
    // A continuation at the end of the file: Java drops the backslash.
    if let Some(open) = current {
        out.push(open);
    }
    out
}

/// Natural lines, split on `\n`, `\r` or `\r\n`.
fn natural_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        if let Some(at) = rest.find(['\n', '\r']) {
            lines.push(&rest[..at]);
            let skip = if rest[at..].starts_with("\r\n") { 2 } else { 1 };
            rest = &rest[at + skip..];
        } else {
            lines.push(rest);
            rest = "";
        }
    }
    lines
}

/// Splits a logical line into its raw (still escaped) key and value.
fn split_key_value(line: &str) -> (&str, &str) {
    let mut escaped = false;
    let mut key_end = line.len();
    let mut value_start = line.len();
    let mut has_separator = false;
    for (at, c) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
        } else if c == '=' || c == ':' {
            key_end = at;
            value_start = at + 1;
            has_separator = true;
            break;
        } else if is_blank(c) {
            key_end = at;
            value_start = at + 1;
            break;
        }
    }
    let mut rest = &line[value_start..];
    rest = rest.trim_start_matches(is_blank);
    if !has_separator && let Some(after) = rest.strip_prefix(['=', ':']) {
        rest = after.trim_start_matches(is_blank);
    }
    (&line[..key_end], rest)
}

fn unescape(raw: &str, line: usize) -> Result<String, PropertiesError> {
    let malformed = PropertiesError {
        line,
        problem: "malformed \\uXXXX escape",
    };
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('f') => out.push('\u{c}'),
            Some('u') => {
                let hex: String = chars.by_ref().take(4).collect();
                if hex.len() != 4 {
                    return Err(malformed);
                }
                let code = u32::from_str_radix(&hex, 16).map_err(|_| malformed.clone())?;
                // Java keeps lone surrogates; Rust strings cannot hold them.
                out.push(char::from_u32(code).ok_or(PropertiesError {
                    line,
                    problem: "\\u escape of a lone surrogate",
                })?);
            }
            Some(other) => out.push(other),
            // Only reachable through the dropped end-of-file continuation.
            None => {}
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn comments_blank_lines_and_separators() {
        let text = "# comment\n! also a comment\n\n   \n\
                    a=1\nb = 2\nc:3\nd 4\n  e\t=\t5\nf\n g =\n";
        assert_eq!(
            parse(text).unwrap(),
            map(&[
                ("a", "1"),
                ("b", "2"),
                ("c", "3"),
                ("d", "4"),
                ("e", "5"),
                ("f", ""),
                ("g", "")
            ])
        );
    }

    #[test]
    fn only_the_first_separator_splits() {
        let parsed = parse("url=jdbc:sqlserver://h:1433;databaseName=x\nk : v=w").unwrap();
        assert_eq!(parsed["url"], "jdbc:sqlserver://h:1433;databaseName=x");
        assert_eq!(parsed["k"], "v=w");
        // Whitespace ends the key, then one `=` or `:` is still skipped.
        assert_eq!(parse("key  =  value").unwrap()["key"], "value");
        assert_eq!(parse("key  value = x").unwrap()["key"], "value = x");
    }

    #[test]
    fn trailing_whitespace_of_a_value_is_kept() {
        assert_eq!(parse("k=v  \n").unwrap()["k"], "v  ");
    }

    #[test]
    fn continuation_lines_join_without_leading_whitespace() {
        let text = "list = a,\\\n        b,\\\n\t c\nnext=1";
        let parsed = parse(text).unwrap();
        assert_eq!(parsed["list"], "a,b,c");
        assert_eq!(parsed["next"], "1");
    }

    #[test]
    fn an_even_number_of_backslashes_does_not_continue() {
        let parsed = parse("path=c:\\\\\nnext=1").unwrap();
        assert_eq!(parsed["path"], "c:\\");
        assert_eq!(parsed["next"], "1");
    }

    #[test]
    fn a_comment_line_never_continues() {
        let parsed = parse("# ends with a backslash \\\nk=v").unwrap();
        assert_eq!(parsed, map(&[("k", "v")]));
    }

    #[test]
    fn a_continuation_line_may_start_with_a_comment_character() {
        assert_eq!(parse("k=a\\\n#b").unwrap()["k"], "a#b");
    }

    #[test]
    fn continuation_at_end_of_file_drops_the_backslash() {
        assert_eq!(parse("k=v\\").unwrap()["k"], "v");
    }

    #[test]
    fn escapes_follow_java() {
        let text = "tab=a\\tb\nnl=a\\nb\nuni=\\u00e9t\\u00e9\nsep\\=key\\:x=1\nsp\\ key=2\nother=\\q\\\\\n";
        let parsed = parse(text).unwrap();
        assert_eq!(parsed["tab"], "a\tb");
        assert_eq!(parsed["nl"], "a\nb");
        assert_eq!(parsed["uni"], "été");
        assert_eq!(parsed["sep=key:x"], "1");
        assert_eq!(parsed["sp key"], "2");
        assert_eq!(parsed["other"], "q\\");
    }

    #[test]
    fn line_endings_cr_lf_and_crlf() {
        let parsed = parse("a=1\r\nb=2\rc=3\n").unwrap();
        assert_eq!(parsed, map(&[("a", "1"), ("b", "2"), ("c", "3")]));
    }

    #[test]
    fn last_duplicate_wins() {
        assert_eq!(parse("k=1\nk=2").unwrap()["k"], "2");
    }

    #[test]
    fn malformed_unicode_escape_names_the_line_not_the_content() {
        let err = parse("ok=1\n\npassword=s3cr\\u12").unwrap_err();
        assert_eq!(err.line, 3);
        let shown = err.to_string();
        assert!(!shown.contains("s3cr"), "{shown}");
        assert!(parse("k=\\uZZZZ").is_err());
        assert!(parse("k=\\ud800").is_err());
    }

    #[test]
    fn line_numbers_count_natural_lines() {
        let err = parse("a=\\\n  b\nc=\\u1").unwrap_err();
        assert_eq!(err.line, 3);
    }

    #[test]
    fn latin1_fallback_for_non_utf8_bytes() {
        assert_eq!(decode(b"k=\xe9t\xe9"), "k=été");
        assert_eq!(decode("k=été".as_bytes()), "k=été");
    }
}
