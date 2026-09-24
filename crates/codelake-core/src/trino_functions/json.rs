//! JSON functions (Trino "JSON functions and operators").
//!
//! There is no JSON type in Arrow: `json` values are `varchar` holding JSON
//! text. `json_parse` produces Trino's canonical text (Jackson with sorted
//! keys, doubles printed by Java's `Double.toString`); `json_extract` keeps
//! the input's key order; `json_extract_scalar` returns number tokens as
//! written. Paths use Trino's simple `JSONPath`: `$`, `.key`, `["key"]`, `[n]`.

use std::fmt::Write as _;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ScalarUDF;

use super::support::{
    At, Call, Func, as_i64, as_str, bool_array, coerce_each, int_array, ints, invalid_argument, is_float,
    is_integer, is_string, not_found, string_array, strings,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    /// The number token exactly as written.
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

struct Parser<'a> {
    bytes: &'a [u8],
    text: &'a str,
    pos: usize,
    depth: usize,
}

const MAX_DEPTH: usize = 1_000;

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while self
            .bytes
            .get(self.pos)
            .is_some_and(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.pos += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> Option<()> {
        (self.bytes.get(self.pos) == Some(&byte)).then(|| self.pos += 1)
    }

    fn literal(&mut self, word: &str, value: Json) -> Option<Json> {
        self.text.get(self.pos..)?.starts_with(word).then(|| {
            self.pos += word.len();
            value
        })
    }

    fn value(&mut self) -> Option<Json> {
        self.skip_ws();
        match self.bytes.get(self.pos)? {
            b'n' => self.literal("null", Json::Null),
            b't' => self.literal("true", Json::Bool(true)),
            b'f' => self.literal("false", Json::Bool(false)),
            b'"' => self.string().map(Json::String),
            b'[' => self.array(),
            b'{' => self.object(),
            b'-' | b'0'..=b'9' => self.number(),
            _ => None,
        }
    }

    fn number(&mut self) -> Option<Json> {
        let start = self.pos;
        self.eat(b'-');
        let digits = |p: &mut Self| {
            let s = p.pos;
            while p.bytes.get(p.pos).is_some_and(u8::is_ascii_digit) {
                p.pos += 1;
            }
            p.pos > s
        };
        if self.eat(b'0').is_none() && !digits(self) {
            return None;
        }
        if self.eat(b'.').is_some() && !digits(self) {
            return None;
        }
        if self.bytes.get(self.pos).is_some_and(|b| *b == b'e' || *b == b'E') {
            self.pos += 1;
            if self.eat(b'+').is_none() {
                self.eat(b'-');
            }
            if !digits(self) {
                return None;
            }
        }
        Some(Json::Number(self.text.get(start..self.pos)?.to_owned()))
    }

    fn hex4(&mut self) -> Option<u32> {
        let hex = self.text.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        u32::from_str_radix(hex, 16).ok()
    }

    fn string(&mut self) -> Option<String> {
        self.eat(b'"')?;
        let mut out = String::new();
        loop {
            let rest = self.text.get(self.pos..)?;
            let next = rest.find(['"', '\\'])?;
            let chunk = &rest[..next];
            if chunk.bytes().any(|b| b < 0x20) {
                return None;
            }
            out.push_str(chunk);
            self.pos += next;
            if self.eat(b'"').is_some() {
                return Some(out);
            }
            self.pos += 1;
            let escaped = *self.bytes.get(self.pos)?;
            self.pos += 1;
            match escaped {
                b'"' => out.push('"'),
                b'\\' => out.push('\\'),
                b'/' => out.push('/'),
                b'b' => out.push('\u{8}'),
                b'f' => out.push('\u{c}'),
                b'n' => out.push('\n'),
                b'r' => out.push('\r'),
                b't' => out.push('\t'),
                b'u' => {
                    let first = self.hex4()?;
                    let code = if (0xD800..0xDC00).contains(&first) {
                        if self.text.get(self.pos..self.pos + 2)? != "\\u" {
                            return None;
                        }
                        self.pos += 2;
                        let second = self.hex4()?;
                        0x10000 + ((first - 0xD800) << 10) + (second.checked_sub(0xDC00)?)
                    } else {
                        first
                    };
                    out.push(char::from_u32(code)?);
                }
                _ => return None,
            }
        }
    }

    fn array(&mut self) -> Option<Json> {
        self.eat(b'[')?;
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return None;
        }
        let mut items = Vec::new();
        self.skip_ws();
        if self.eat(b']').is_none() {
            loop {
                items.push(self.value()?);
                self.skip_ws();
                if self.eat(b']').is_some() {
                    break;
                }
                self.eat(b',')?;
            }
        }
        self.depth -= 1;
        Some(Json::Array(items))
    }

    fn object(&mut self) -> Option<Json> {
        self.eat(b'{')?;
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return None;
        }
        let mut entries = Vec::new();
        self.skip_ws();
        if self.eat(b'}').is_none() {
            loop {
                self.skip_ws();
                let key = self.string()?;
                self.skip_ws();
                self.eat(b':')?;
                entries.push((key, self.value()?));
                self.skip_ws();
                if self.eat(b'}').is_some() {
                    break;
                }
                self.eat(b',')?;
            }
        }
        self.depth -= 1;
        Some(Json::Object(entries))
    }
}

pub(crate) fn parse(text: &str) -> Option<Json> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        text,
        pos: 0,
        depth: 0,
    };
    let value = parser.value()?;
    parser.skip_ws();
    (parser.pos == text.len()).then_some(value)
}

// ---------------------------------------------------------------------------
// Printing (Jackson)
// ---------------------------------------------------------------------------

/// Java's `Double.toString`.
pub(crate) fn java_double(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    let abs = value.abs();
    let sign = if value < 0.0 { "-" } else { "" };
    if (1e-3..1e7).contains(&abs) {
        let text = format!("{abs}");
        return if text.contains('.') {
            format!("{sign}{text}")
        } else {
            format!("{sign}{text}.0")
        };
    }
    let scientific = super::cast::format_double(abs);
    format!("{sign}{scientific}")
}

pub(crate) fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04X}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

pub(crate) fn is_integer_token(token: &str) -> bool {
    !token.contains(['.', 'e', 'E'])
}

fn write_number(token: &str, out: &mut String) {
    if is_integer_token(token) {
        match token.parse::<i128>() {
            Ok(v) => out.push_str(&v.to_string()),
            Err(_) => out.push_str(token.trim_start_matches('+')),
        }
    } else {
        out.push_str(&java_double(token.parse::<f64>().unwrap_or(f64::NAN)));
    }
}

/// Jackson serialization; `sort_keys` for `json_parse`'s canonical form.
pub(crate) fn write(value: &Json, sort_keys: bool, out: &mut String) {
    match value {
        Json::Null => out.push_str("null"),
        Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Json::Number(token) => write_number(token, out),
        Json::String(s) => write_string(s, out),
        Json::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(item, sort_keys, out);
            }
            out.push(']');
        }
        Json::Object(entries) => {
            out.push('{');
            let mut ordered: Vec<&(String, Json)> = if sort_keys {
                // A map keeps the last value of a repeated key.
                let mut last: Vec<&(String, Json)> = Vec::new();
                for entry in entries {
                    last.retain(|e| e.0 != entry.0);
                    last.push(entry);
                }
                last
            } else {
                entries.iter().collect()
            };
            if sort_keys {
                ordered.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
            }
            for (i, (key, item)) in ordered.into_iter().map(|e| (&e.0, &e.1)).enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(key, out);
                out.push(':');
                write(item, sort_keys, out);
            }
            out.push('}');
        }
    }
}

pub(crate) fn to_text(value: &Json, sort_keys: bool) -> String {
    let mut out = String::new();
    write(value, sort_keys, &mut out);
    out
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Key(String),
    Index(usize),
}

fn invalid_path(path: &str) -> DataFusionError {
    invalid_argument(format!("Invalid JSON path: '{path}'"))
}

fn parse_path(path: &str) -> Result<Vec<Step>> {
    let mut chars = path.trim().chars().peekable();
    if chars.next() != Some('$') {
        return Err(invalid_path(path));
    }
    let mut steps = Vec::new();
    while let Some(c) = chars.next() {
        match c {
            '.' => {
                let mut key = String::new();
                while let Some(&k) = chars.peek() {
                    if k == '.' || k == '[' {
                        break;
                    }
                    if !(k.is_alphanumeric() || k == '_' || k == ':' || k == '-') {
                        return Err(invalid_path(path));
                    }
                    key.push(k);
                    chars.next();
                }
                if key.is_empty() {
                    return Err(invalid_path(path));
                }
                steps.push(Step::Key(key));
            }
            '[' => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    let mut key = String::new();
                    loop {
                        match chars.next() {
                            Some('"') => break,
                            Some('\\') => key.push(chars.next().ok_or_else(|| invalid_path(path))?),
                            Some(k) => key.push(k),
                            None => return Err(invalid_path(path)),
                        }
                    }
                    steps.push(Step::Key(key));
                } else {
                    let mut digits = String::new();
                    while let Some(d) = chars.peek().copied().filter(char::is_ascii_digit) {
                        digits.push(d);
                        chars.next();
                    }
                    steps.push(Step::Index(digits.parse().map_err(|_| invalid_path(path))?));
                }
                if chars.next() != Some(']') {
                    return Err(invalid_path(path));
                }
            }
            _ => return Err(invalid_path(path)),
        }
    }
    Ok(steps)
}

fn navigate<'a>(value: &'a Json, steps: &[Step]) -> Option<&'a Json> {
    let mut current = value;
    for step in steps {
        current = match (current, step) {
            (Json::Object(entries), Step::Key(key)) => {
                entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)?
            }
            (Json::Array(items), Step::Index(i)) => items.get(*i)?,
            _ => return None,
        };
    }
    Some(current)
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

fn json_arg(t: &DataType) -> Option<DataType> {
    as_str(t)
}

fn with_path(call: &Call<'_>, f: impl Fn(&Json) -> Option<String>) -> Result<ArrayRef> {
    let docs = strings(call.arg(0)?)?;
    let paths = strings(call.arg(1)?)?;
    let mut parsed_paths: Vec<(String, Vec<Step>)> = Vec::new();
    let out = (0..call.rows)
        .map(|i| {
            let (Some(doc), Some(path)) = (docs.at(i), paths.at(i)) else {
                return Ok(None);
            };
            let steps = if let Some((_, s)) = parsed_paths.iter().find(|(p, _)| p == path) {
                s.clone()
            } else {
                let s = parse_path(path)?;
                parsed_paths.push((path.to_owned(), s.clone()));
                s
            };
            Ok(parse(doc).and_then(|json| navigate(&json, &steps).and_then(&f)))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(string_array(out))
}

pub(crate) fn json_parse() -> ScalarUDF {
    Func::new(
        "json_parse",
        |name, types| coerce_each(name, types, &[as_str]),
        |_| Ok(DataType::Utf8),
        |call| {
            let text = strings(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    text.at(i)
                        .map(|t| {
                            parse(t).map(|json| to_text(&json, true)).ok_or_else(|| {
                                invalid_argument(format!("Cannot convert value to JSON: '{t}'"))
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .returns_json()
    .udf()
}

pub(crate) fn json_format() -> ScalarUDF {
    Func::new(
        "json_format",
        |name, types| coerce_each(name, types, &[json_arg]),
        |_| Ok(DataType::Utf8),
        |call| {
            let text = strings(call.arg(0)?)?;
            Ok(string_array(
                (0..call.rows)
                    .map(|i| {
                        text.at(i)
                            .map(|t| parse(t).map_or_else(|| t.to_owned(), |j| to_text(&j, true)))
                    })
                    .collect(),
            ))
        },
    )
    .udf()
}

pub(crate) fn json_extract() -> ScalarUDF {
    Func::new(
        "json_extract",
        |name, types| coerce_each(name, types, &[json_arg, as_str]),
        |_| Ok(DataType::Utf8),
        |call| with_path(call, |v| Some(to_text(v, false))),
    )
    .returns_json()
    .udf()
}

fn scalar_text(value: &Json) -> Option<String> {
    match value {
        Json::Bool(b) => Some(b.to_string()),
        Json::Number(token) => Some(token.clone()),
        Json::String(s) => Some(s.clone()),
        Json::Null | Json::Array(_) | Json::Object(_) => None,
    }
}

pub(crate) fn json_extract_scalar() -> ScalarUDF {
    Func::new(
        "json_extract_scalar",
        |name, types| coerce_each(name, types, &[json_arg, as_str]),
        |_| Ok(DataType::Utf8),
        |call| with_path(call, scalar_text),
    )
    .udf()
}

pub(crate) fn json_size() -> ScalarUDF {
    Func::new(
        "json_size",
        |name, types| coerce_each(name, types, &[json_arg, as_str]),
        |_| Ok(DataType::Int64),
        |call| {
            let sizes = with_path(call, |v| {
                Some(
                    match v {
                        Json::Array(items) => items.len(),
                        Json::Object(entries) => entries.len(),
                        _ => 0,
                    }
                    .to_string(),
                )
            })?;
            Ok(datafusion::arrow::compute::cast(&sizes, &DataType::Int64)?)
        },
    )
    .udf()
}

pub(crate) fn json_array_length() -> ScalarUDF {
    Func::new(
        "json_array_length",
        |name, types| coerce_each(name, types, &[json_arg]),
        |_| Ok(DataType::Int64),
        |call| {
            let text = strings(call.arg(0)?)?;
            Ok(int_array(
                (0..call.rows)
                    .map(|i| match parse(text.at(i)?)? {
                        Json::Array(items) => i64::try_from(items.len()).ok(),
                        _ => None,
                    })
                    .collect(),
            ))
        },
    )
    .udf()
}

pub(crate) fn json_array_get() -> ScalarUDF {
    Func::new(
        "json_array_get",
        |name, types| coerce_each(name, types, &[json_arg, as_i64]),
        |_| Ok(DataType::Utf8),
        |call| {
            let text = strings(call.arg(0)?)?;
            let index = ints(call.arg(1)?)?;
            Ok(string_array(
                (0..call.rows)
                    .map(|i| {
                        let Json::Array(items) = parse(text.at(i)?)? else {
                            return None;
                        };
                        let n = index.at(i)?;
                        let len = i64::try_from(items.len()).ok()?;
                        let position = if n >= 0 { n } else { len + n };
                        let item = items.get(usize::try_from(position).ok()?)?;
                        Some(to_text(item, false))
                    })
                    .collect(),
            ))
        },
    )
    .returns_json()
    .udf()
}

fn matches_value(item: &Json, needle: &ScalarValue) -> bool {
    match (item, needle) {
        (Json::Bool(b), ScalarValue::Boolean(Some(v))) => b == v,
        (Json::String(s), ScalarValue::Utf8(Some(v))) => s == v,
        (Json::Number(t), ScalarValue::Int64(Some(v))) => {
            is_integer_token(t) && t.parse::<i64>().ok() == Some(*v)
        }
        (Json::Number(t), ScalarValue::Float64(Some(v))) => t.parse::<f64>().ok() == Some(*v),
        _ => false,
    }
}

pub(crate) fn json_array_contains() -> ScalarUDF {
    Func::new(
        "json_array_contains",
        |name, types| match types {
            [j, v] => {
                let j = json_arg(j).ok_or_else(|| not_found(name, types))?;
                let v = if is_integer(v) {
                    DataType::Int64
                } else if is_float(v) {
                    DataType::Float64
                } else if is_string(v) {
                    DataType::Utf8
                } else if matches!(v, DataType::Boolean) {
                    DataType::Boolean
                } else {
                    return Err(not_found(name, types));
                };
                Ok(vec![j, v])
            }
            _ => Err(not_found(name, types)),
        },
        |_| Ok(DataType::Boolean),
        |call| {
            let text = strings(call.arg(0)?)?;
            let needles = call.arg(1)?;
            let out = (0..call.rows)
                .map(|i| {
                    let needle = ScalarValue::try_from_array(needles, i)?;
                    if needle.is_null() {
                        return Ok(None);
                    }
                    Ok(text.at(i).and_then(parse).and_then(|json| match json {
                        Json::Array(items) => Some(items.iter().any(|item| matches_value(item, &needle))),
                        _ => None,
                    }))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(bool_array(out))
        },
    )
    .udf()
}

pub(crate) fn is_json_scalar() -> ScalarUDF {
    Func::new(
        "is_json_scalar",
        |name, types| coerce_each(name, types, &[json_arg]),
        |_| Ok(DataType::Boolean),
        |call| {
            let text = strings(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    text.at(i)
                        .map(|t| {
                            parse(t)
                                .map(|j| !matches!(j, Json::Array(_) | Json::Object(_)))
                                .ok_or_else(|| invalid_argument(format!("Invalid JSON value: {t}")))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(bool_array(out))
        },
    )
    .udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        json_parse(),
        json_format(),
        json_extract(),
        json_extract_scalar(),
        json_size(),
        json_array_length(),
        json_array_get(),
        json_array_contains(),
        is_json_scalar(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_form_sorts_keys_and_prints_java_doubles() {
        let json = parse(r#" {"b": 1.10, "a": [1e2, -0, "xç\n"], "c": null, "a": true} "#).unwrap();
        assert_eq!(to_text(&json, true), r#"{"a":true,"b":1.1,"c":null}"#);
        assert_eq!(
            to_text(&parse(r#"{"b":1.10,"a":[1e2,-0,"xç\n"]}"#).unwrap(), false),
            r#"{"b":1.1,"a":[100.0,0,"xç\n"]}"#
        );
    }

    #[test]
    fn java_double_formats() {
        assert_eq!(java_double(100.0), "100.0");
        assert_eq!(java_double(0.001), "0.001");
        assert_eq!(java_double(1e7), "1.0E7");
        assert_eq!(java_double(1.5e-4), "1.5E-4");
        assert_eq!(java_double(-2.5), "-2.5");
    }

    #[test]
    fn rejects_invalid_documents() {
        for text in ["", "{", "[1,]", "{'a':1}", "01", "1 2", "\"\u{1}\"", "nul"] {
            assert!(parse(text).is_none(), "{text:?}");
        }
        assert_eq!(parse("\"\\ud83d\\ude00\"").unwrap(), Json::String("😀".into()));
    }

    #[test]
    fn paths() {
        let json = parse(r#"{"store":{"book":[{"title":"a"},{"title":"b"}]},"x.y":1}"#).unwrap();
        let steps = parse_path("$.store.book[1].title").unwrap();
        assert_eq!(navigate(&json, &steps), Some(&Json::String("b".into())));
        let steps = parse_path(r#"$["x.y"]"#).unwrap();
        assert_eq!(navigate(&json, &steps), Some(&Json::Number("1".into())));
        assert!(parse_path("store").is_err());
        assert!(parse_path("$.").is_err());
        assert!(parse_path("$[x]").is_err());
    }
}
