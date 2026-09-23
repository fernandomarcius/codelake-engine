//! Memory limits are parsed from strings; a panic here would crash startup and
//! a wrong value would disable the memory ceiling.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    if let Ok(size) = data.parse::<codelake_core::ByteSize>() {
        let round_trip: codelake_core::ByteSize = size.to_string().parse().expect("display must re-parse");
        assert_eq!(size, round_trip);
    }
});
