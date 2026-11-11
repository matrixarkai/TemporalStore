// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::cell::RefCell;
use std::collections::HashMap;

#[test]
fn selected_slab_reads_match_full_configuration_values_and_errors() {
    let values = [
        None,
        Some(""),
        Some(" \t"),
        Some("invalid"),
        Some("-1"),
        Some("0"),
        Some("1023"),
        Some("1024"),
        Some(" 65536 "),
        Some("1073741824"),
        Some("4294967296"),
        Some("4294967297"),
        Some("18446744073709551615"),
        Some("18446744073709551616"),
    ];
    let mut checked = 0;
    let mut accepted = 0;
    let mut refused = 0;
    for current in values {
        for previous in values {
            for blob in values {
                let mut config = HashMap::from([
                    (TS_COMPACTION_WATERMARK_BYTES, "456"),
                    (TS_CONTEXT_BLOCK_TARGET_BYTES, "32768"),
                    (TS_COLD_SCAN_NO_CACHE_FILL, " Off "),
                ]);
                for (name, value) in [
                    (TS_BLOCK_SLAB_TARGET_BYTES, current),
                    (TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME, previous),
                    (TS_STREAM_MAX_BLOB_SIZE, blob),
                ] {
                    if let Some(value) = value {
                        config.insert(name, value);
                    }
                }
                let get = |name: &str| config.get(name).map(|v| v.to_string());
                let expected = StorageTuningConfig::try_from_getter(get)
                    .map(StorageTuningConfig::effective_slab_target_bytes);
                checked += 1;
                if expected.is_ok() {
                    accepted += 1;
                } else {
                    refused += 1;
                }
                assert_eq!(try_slab_target_from_getter(get), expected);
            }
        }
    }
    assert_eq!(checked, 2744);
    assert!(accepted > 0 && refused > 0);
}

#[test]
fn slab_projection_looks_up_only_size_dependencies() {
    let seen = RefCell::new(Vec::new());
    let value = try_slab_target_from_getter(|name| {
        seen.borrow_mut().push(name.to_owned());
        assert!(matches!(
            name,
            TS_BLOCK_SLAB_TARGET_BYTES
                | TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME
                | TS_STREAM_MAX_BLOB_SIZE
        ));
        None
    })
    .unwrap();
    assert_eq!(value, DEFAULT_BLOCK_SLAB_TARGET_BYTES);
    assert_eq!(
        *seen.borrow(),
        [
            TS_BLOCK_SLAB_TARGET_BYTES,
            TS_BLOCK_SLAB_TARGET_BYTES_PREVIOUS_NAME,
            TS_STREAM_MAX_BLOB_SIZE
        ]
    );
    seen.borrow_mut().clear();
    assert_eq!(
        try_slab_target_from_getter(|name| {
            seen.borrow_mut().push(name.to_owned());
            (name == TS_BLOCK_SLAB_TARGET_BYTES).then(|| "2147483648".to_owned())
        })
        .unwrap(),
        2147483648
    );
    assert_eq!(
        *seen.borrow(),
        [TS_BLOCK_SLAB_TARGET_BYTES, TS_STREAM_MAX_BLOB_SIZE]
    );
}

#[test]
fn slab_projection_does_not_cache_values_or_errors_between_calls() {
    let current = RefCell::new(Some("2147483648".to_owned()));
    let get = |name: &str| {
        if name == TS_BLOCK_SLAB_TARGET_BYTES {
            current.borrow().clone()
        } else {
            None
        }
    };
    assert_eq!(try_slab_target_from_getter(get).unwrap(), 2147483648);
    *current.borrow_mut() = Some("4294967297".into());
    assert!(try_slab_target_from_getter(get).is_err());
    *current.borrow_mut() = Some("4294967296".into());
    assert_eq!(try_slab_target_from_getter(get).unwrap(), 4294967296);
    *current.borrow_mut() = None;
    assert_eq!(
        try_slab_target_from_getter(get).unwrap(),
        DEFAULT_BLOCK_SLAB_TARGET_BYTES
    );
}

#[test]
fn public_getter_matches_current_environment() {
    let expected = StorageTuningConfig::try_from_getter(|name| std::env::var(name).ok())
        .map(StorageTuningConfig::effective_slab_target_bytes);
    let actual = std::panic::catch_unwind(effective_block_slab_target_bytes);
    match expected {
        Ok(value) => assert_eq!(actual.unwrap(), value),
        Err(message) => {
            let panic = actual.unwrap_err();
            let text = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .expect("configuration refusal must contain text");
            assert_eq!(text, message);
        }
    }
}

#[test]
#[ignore]
fn measure_live_slab_lookup() {
    use std::hint::black_box;
    use std::time::Instant;

    const CALLS: u32 = 50_000;
    let expected = StorageTuningConfig::from_env().effective_slab_target_bytes();
    assert_eq!(effective_block_slab_target_bytes(), expected);
    for pass in 0..3 {
        let mut nanos = [0_u128; 2];
        for arm in if pass % 2 == 0 { [0, 1] } else { [1, 0] } {
            let start = Instant::now();
            for _ in 0..CALLS {
                let value = if arm == 0 {
                    StorageTuningConfig::from_env().effective_slab_target_bytes()
                } else {
                    effective_block_slab_target_bytes()
                };
                assert_eq!(black_box(value), expected);
            }
            nanos[arm] = start.elapsed().as_nanos() / u128::from(CALLS);
        }
        println!(
            "SLAB_LOOKUP pass={pass} full_ns={} selected_ns={}",
            nanos[0], nanos[1]
        );
    }
}
