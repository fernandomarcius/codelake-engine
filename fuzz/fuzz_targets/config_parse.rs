//! The config file is operator input: parsing and validation must never panic,
//! whatever bytes arrive.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        if let Ok(config) = toml::from_str::<codelake_core::Config>(text) {
            let _ = config.validate(&|_| None, &|_| false);
        }
    }
});
