//! Regular expression functions (Trino "Regular expression functions").
//!
//! Trino uses Joni (Java syntax); this uses Rust's `regex`, which accepts the
//! common subset. Patterns with backreferences or look-around are rejected
//! with `INVALID_FUNCTION_ARGUMENT` instead of being evaluated differently.
//! Replacement strings follow `java.util.regex.Matcher`: `$n`, `${name}`, and
//! `\` to escape the next character. `regexp_replace` replaces every match.
//! `\d`, `\w`, `\s` and `\b` are ASCII, as in Joni (`\p{L}` stays Unicode).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::logical_expr::ScalarUDF;
use regex::{Captures, Regex};

use super::support::{
    At, Call, Func, as_i64, as_str, bool_array, coerce_any, coerce_each, int_array, ints, invalid_argument,
    string_array, strings,
};

/// ASCII meaning of a shorthand class inside `[...]` (Joni, like Java, gives
/// `\d`, `\w`, `\s` their ASCII sets; Rust's are Unicode).
fn ascii_class_items(class: char) -> Option<&'static str> {
    match class {
        'd' => Some("0-9"),
        'w' => Some("0-9A-Za-z_"),
        's' => Some("\\t\\n\\x0B\\f\\r "),
        _ => None,
    }
}

/// Rewrites a Trino (Joni) pattern for Rust's `regex`: shorthand classes and
/// word boundaries become their ASCII forms; everything else is kept.
fn java_pattern(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 8);
    let mut chars = pattern.chars().peekable();
    let mut class_depth = 0_usize;
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let Some(next) = chars.next() else {
                    out.push(c);
                    break;
                };
                let lower = next.to_ascii_lowercase();
                match (
                    ascii_class_items(lower),
                    next.is_ascii_uppercase(),
                    class_depth > 0,
                ) {
                    (Some(items), true, _) => {
                        out.push_str("[^");
                        out.push_str(items);
                        out.push(']');
                    }
                    (Some(items), false, false) => {
                        out.push('[');
                        out.push_str(items);
                        out.push(']');
                    }
                    (Some(items), false, true) => out.push_str(items),
                    (None, _, false) if lower == 'b' => {
                        out.push_str("(?-u:\\");
                        out.push(next);
                        out.push(')');
                    }
                    _ => {
                        out.push(c);
                        out.push(next);
                    }
                }
            }
            '[' => {
                class_depth += 1;
                out.push(c);
                // A `]` right after `[` or `[^` is a literal.
                if chars.peek() == Some(&'^') {
                    out.push('^');
                    chars.next();
                }
                if chars.peek() == Some(&']') {
                    out.push_str("\\]");
                    chars.next();
                }
            }
            ']' if class_depth > 0 => {
                class_depth -= 1;
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

/// Compiled patterns of one batch (patterns are almost always constant).
#[derive(Debug, Default)]
struct Patterns(HashMap<String, Regex>);

impl Patterns {
    fn get(&mut self, pattern: &str) -> Result<&Regex> {
        if !self.0.contains_key(pattern) {
            let regex = Regex::new(&java_pattern(pattern))
                .map_err(|e| invalid_argument(format!("Invalid regular expression: {pattern}: {e}")))?;
            self.0.insert(pattern.to_owned(), regex);
        }
        self.0
            .get(pattern)
            .ok_or_else(|| invalid_argument("pattern cache"))
    }
}

fn string_list() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)))
}

fn group_index(regex: &Regex, group: i64) -> Result<usize> {
    let groups = regex.captures_len() - 1;
    usize::try_from(group)
        .ok()
        .filter(|g| *g <= groups)
        .ok_or_else(|| {
            invalid_argument(format!(
                "Pattern has {groups} groups. Cannot access group {group}"
            ))
        })
}

/// Expands a Java replacement string for one match.
fn expand(replacement: &str, caps: &Captures<'_>, regex: &Regex, out: &mut String) -> Result<()> {
    let groups = regex.captures_len() - 1;
    let mut chars = replacement.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(escaped) => out.push(escaped),
                None => {
                    return Err(invalid_argument(
                        "Illegal escape character at the end of the replacement",
                    ));
                }
            },
            '$' => {
                if chars.peek() == Some(&'{') {
                    chars.next();
                    let mut name = String::new();
                    loop {
                        match chars.next() {
                            Some('}') => break,
                            Some(ch) => name.push(ch),
                            None => {
                                return Err(invalid_argument(
                                    "Named capturing group is missing trailing '}'",
                                ));
                            }
                        }
                    }
                    let group = caps
                        .name(&name)
                        .map(|m| m.as_str())
                        .or_else(|| {
                            regex
                                .capture_names()
                                .any(|n| n == Some(name.as_str()))
                                .then_some("")
                        })
                        .ok_or_else(|| invalid_argument(format!("No group with name {{{name}}}")))?;
                    out.push_str(group);
                } else {
                    let Some(first) = chars.peek().and_then(|d| d.to_digit(10)) else {
                        return Err(invalid_argument("Illegal group reference"));
                    };
                    chars.next();
                    let mut number = usize::try_from(first).unwrap_or(usize::MAX);
                    if number > groups {
                        return Err(invalid_argument(format!("No group {number}")));
                    }
                    while let Some(next) = chars.peek().and_then(|d| d.to_digit(10)) {
                        let candidate = number * 10 + usize::try_from(next).unwrap_or(0);
                        if candidate > groups {
                            break;
                        }
                        number = candidate;
                        chars.next();
                    }
                    out.push_str(caps.get(number).map_or("", |m| m.as_str()));
                }
            }
            other => out.push(other),
        }
    }
    Ok(())
}

/// Matches from byte `from` on, as Java's `Matcher.find()` loop sees them:
/// after a non-empty match, an empty match may start where it ended (Rust's
/// iterators skip it); after an empty match the search moves one character.
fn java_captures<'t>(regex: &Regex, text: &'t str, from: usize) -> Vec<Captures<'t>> {
    let mut out = Vec::new();
    let mut at = from;
    while at <= text.len() {
        let Some(caps) = regex.captures_at(text, at) else {
            break;
        };
        let Some(whole) = caps.get(0) else { break };
        at = if whole.is_empty() {
            whole.end()
                + text
                    .get(whole.end()..)
                    .and_then(|r| r.chars().next())
                    .map_or(1, char::len_utf8)
        } else {
            whole.end()
        };
        out.push(caps);
    }
    out
}

fn replace_all(text: &str, regex: &Regex, replacement: &str) -> Result<String> {
    let mut out = String::new();
    let mut last = 0;
    for caps in java_captures(regex, text, 0) {
        let Some(whole) = caps.get(0) else { continue };
        out.push_str(&text[last..whole.start()]);
        expand(replacement, &caps, regex, &mut out)?;
        last = whole.end();
    }
    out.push_str(&text[last..]);
    Ok(out)
}

pub(crate) fn regexp_like() -> ScalarUDF {
    Func::new(
        "regexp_like",
        |name, types| coerce_each(name, types, &[as_str, as_str]),
        |_| Ok(DataType::Boolean),
        |call| {
            let text = strings(call.arg(0)?)?;
            let pattern = strings(call.arg(1)?)?;
            let mut cache = Patterns::default();
            let out = (0..call.rows)
                .map(|i| match (text.at(i), pattern.at(i)) {
                    (Some(t), Some(p)) => Ok(Some(cache.get(p)?.is_match(t))),
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(bool_array(out))
        },
    )
    .udf()
}

pub(crate) fn regexp_replace() -> ScalarUDF {
    Func::new(
        "regexp_replace",
        |name, types| coerce_any(name, types, &[&[as_str, as_str], &[as_str, as_str, as_str]]),
        |_| Ok(DataType::Utf8),
        |call| {
            let text = strings(call.arg(0)?)?;
            let pattern = strings(call.arg(1)?)?;
            let replacement = match call.args.get(2) {
                Some(r) => Some(strings(r)?),
                None => None,
            };
            let mut cache = Patterns::default();
            let out = (0..call.rows)
                .map(|i| {
                    let with = match replacement {
                        Some(r) => r.at(i),
                        None => Some(""),
                    };
                    match (text.at(i), pattern.at(i), with) {
                        (Some(t), Some(p), Some(w)) => replace_all(t, cache.get(p)?, w).map(Some),
                        _ => Ok(None),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .udf()
}

fn group_arg(call: &Call<'_>, index: usize, row: usize) -> Result<Option<i64>> {
    match call.args.get(index) {
        Some(g) => Ok(ints(g)?.at(row)),
        None => Ok(Some(0)),
    }
}

pub(crate) fn regexp_extract() -> ScalarUDF {
    Func::new(
        "regexp_extract",
        |name, types| coerce_any(name, types, &[&[as_str, as_str], &[as_str, as_str, as_i64]]),
        |_| Ok(DataType::Utf8),
        |call| {
            let text = strings(call.arg(0)?)?;
            let pattern = strings(call.arg(1)?)?;
            let mut cache = Patterns::default();
            let out = (0..call.rows)
                .map(|i| {
                    let (Some(t), Some(p), Some(g)) = (text.at(i), pattern.at(i), group_arg(call, 2, i)?)
                    else {
                        return Ok(None);
                    };
                    let regex = cache.get(p)?;
                    let group = group_index(regex, g)?;
                    Ok(regex
                        .captures(t)
                        .and_then(|c| c.get(group).map(|m| m.as_str().to_owned())))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .udf()
}

pub(crate) fn regexp_extract_all() -> ScalarUDF {
    Func::new(
        "regexp_extract_all",
        |name, types| coerce_any(name, types, &[&[as_str, as_str], &[as_str, as_str, as_i64]]),
        |_| Ok(string_list()),
        |call| {
            let text = strings(call.arg(0)?)?;
            let pattern = strings(call.arg(1)?)?;
            let mut cache = Patterns::default();
            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..call.rows {
                let (Some(t), Some(p), Some(g)) = (text.at(i), pattern.at(i), group_arg(call, 2, i)?) else {
                    builder.append_null();
                    continue;
                };
                let regex = cache.get(p)?;
                let group = group_index(regex, g)?;
                for caps in java_captures(regex, t, 0) {
                    match caps.get(group) {
                        Some(m) => builder.values().append_value(m.as_str()),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        },
    )
    .udf()
}

pub(crate) fn regexp_count() -> ScalarUDF {
    Func::new(
        "regexp_count",
        |name, types| coerce_each(name, types, &[as_str, as_str]),
        |_| Ok(DataType::Int64),
        |call| {
            let text = strings(call.arg(0)?)?;
            let pattern = strings(call.arg(1)?)?;
            let mut cache = Patterns::default();
            let out = (0..call.rows)
                .map(|i| match (text.at(i), pattern.at(i)) {
                    (Some(t), Some(p)) => Ok(Some(
                        i64::try_from(java_captures(cache.get(p)?, t, 0).len()).unwrap_or(i64::MAX),
                    )),
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

/// 1-based code point position of the `occurrence`-th match at or after `start`; -1 if none.
fn position(text: &str, regex: &Regex, start: i64, occurrence: i64) -> Result<i64> {
    if start < 1 {
        return Err(invalid_argument("start position cannot be smaller than 1"));
    }
    if occurrence < 1 {
        return Err(invalid_argument("occurrence cannot be smaller than 1"));
    }
    let skip = usize::try_from(start - 1).unwrap_or(usize::MAX);
    let Some((offset, _)) = text.char_indices().nth(skip) else {
        return Ok(-1);
    };
    let nth = usize::try_from(occurrence - 1).unwrap_or(usize::MAX);
    Ok(
        match java_captures(regex, text, offset).get(nth).and_then(|c| c.get(0)) {
            Some(m) => i64::try_from(text[..m.start()].chars().count()).unwrap_or(i64::MAX) + 1,
            None => -1,
        },
    )
}

pub(crate) fn regexp_position() -> ScalarUDF {
    Func::new(
        "regexp_position",
        |name, types| {
            coerce_any(
                name,
                types,
                &[
                    &[as_str, as_str],
                    &[as_str, as_str, as_i64],
                    &[as_str, as_str, as_i64, as_i64],
                ],
            )
        },
        |_| Ok(DataType::Int32),
        |call| {
            let text = strings(call.arg(0)?)?;
            let pattern = strings(call.arg(1)?)?;
            let mut cache = Patterns::default();
            let optional = |index: usize, row: usize| -> Result<Option<i64>> {
                match call.args.get(index) {
                    Some(a) => Ok(ints(a)?.at(row)),
                    None => Ok(Some(1)),
                }
            };
            let out = (0..call.rows)
                .map(
                    |i| match (text.at(i), pattern.at(i), optional(2, i)?, optional(3, i)?) {
                        (Some(t), Some(p), Some(s), Some(o)) => position(t, cache.get(p)?, s, o).map(Some),
                        _ => Ok(None),
                    },
                )
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

pub(crate) fn regexp_split() -> ScalarUDF {
    Func::new(
        "regexp_split",
        |name, types| coerce_each(name, types, &[as_str, as_str]),
        |_| Ok(string_list()),
        |call| {
            let text = strings(call.arg(0)?)?;
            let pattern = strings(call.arg(1)?)?;
            let mut cache = Patterns::default();
            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..call.rows {
                let (Some(t), Some(p)) = (text.at(i), pattern.at(i)) else {
                    builder.append_null();
                    continue;
                };
                for part in cache.get(p)?.split(t) {
                    builder.values().append_value(part);
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        },
    )
    .udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        regexp_like(),
        regexp_replace(),
        regexp_extract(),
        regexp_extract_all(),
        regexp_count(),
        regexp_position(),
        regexp_split(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shorthand_classes_are_ascii() {
        assert_eq!(java_pattern(r"\d+"), "[0-9]+");
        assert_eq!(java_pattern(r"[\w-]"), "[0-9A-Za-z_-]");
        assert_eq!(java_pattern(r"[^\D]"), "[^[^0-9]]");
        assert_eq!(java_pattern(r"\\d"), r"\\d");
        assert_eq!(java_pattern(r"\p{L}"), r"\p{L}");
        let word = Regex::new(&java_pattern(r"^\w$")).unwrap();
        assert!(word.is_match("a") && !word.is_match("ç"));
        let digit = Regex::new(&java_pattern(r"\d")).unwrap();
        assert!(!digit.is_match("٣"));
        let boundary = Regex::new(&java_pattern(r"\bb")).unwrap();
        assert!(boundary.is_match("a b") && boundary.is_match("çb"));
        let space = Regex::new(&java_pattern(r"^\s$")).unwrap();
        assert!(space.is_match("\t") && !space.is_match("\u{a0}"));
    }

    #[test]
    fn replacement_follows_java() {
        let regex = Regex::new("(a)(b)?").unwrap();
        assert_eq!(replace_all("ab-a", &regex, "[$1$2]").unwrap(), "[ab]-[a]");
        assert_eq!(replace_all("ab", &regex, "\\$1").unwrap(), "$1");
        assert_eq!(
            replace_all("ab", &regex, "$12").unwrap(),
            "a2",
            "group 12 does not exist: $1 then 2"
        );
        assert!(replace_all("ab", &regex, "$3").is_err());
        assert!(replace_all("ab", &regex, "$x").is_err());
        let named = Regex::new("(?P<n>b)").unwrap();
        assert_eq!(replace_all("abc", &named, "<${n}>").unwrap(), "a<b>c");
        assert_eq!(
            replace_all("abc", &Regex::new("").unwrap(), "x").unwrap(),
            "xaxbxcx"
        );
    }

    #[test]
    fn positions() {
        let regex = Regex::new("b").unwrap();
        assert_eq!(position("açbab", &regex, 1, 1).unwrap(), 3);
        assert_eq!(position("açbab", &regex, 1, 2).unwrap(), 5);
        assert_eq!(position("açbab", &regex, 4, 1).unwrap(), 5);
        assert_eq!(position("açbab", &regex, 9, 1).unwrap(), -1);
        assert!(position("a", &regex, 0, 1).is_err());
    }
}
