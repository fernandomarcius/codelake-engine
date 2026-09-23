//! Parameter maps for `datafusion-table-providers` pools. Values are wrapped in
//! [`SecretString`] end to end, so a `Debug` print of a pool never shows a password.

use std::collections::HashMap;

use secrecy::SecretString;

pub(crate) fn map<const N: usize>(pairs: [(&str, SecretString); N]) -> HashMap<String, SecretString> {
    pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

pub(crate) fn plain(value: impl Into<String>) -> SecretString {
    SecretString::from(value.into())
}
