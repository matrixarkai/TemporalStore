// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn fixture(count: usize) -> HashFieldMap {
    (0..count)
        .map(|i| {
            (
                format!("field-{i:05}"),
                ElementEntry::from_parts(7, i as u64 * 64, 64, None, None),
            )
        })
        .collect()
}

#[test]
fn borrowed_hash_fields_keep_the_existing_map_wire_shape() {
    for count in [0, 1, 64, 8192] {
        let fields = fixture(count);
        let expected: HashMap<String, ElementEntry> = fields.clone().into();
        let bytes = rmp_serde::to_vec_named(&fields).unwrap();
        let decoded: HashMap<String, ElementEntry> = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.len(), count);
        assert!(
            decoded == expected,
            "borrowed encoding must keep every field and address"
        );
        let decoded: HashFieldMap = rmp_serde::from_slice(&bytes).unwrap();
        assert!(decoded == fields);
        assert!(serde_json::to_value(&fields).unwrap() == serde_json::to_value(&expected).unwrap());
    }
}

#[test]
fn borrowed_hash_field_names_preserve_empty_unicode_and_control_bytes() {
    let expected: HashMap<String, ElementEntry> = ["", "a:b", "nul\0name", "caf\u{00e9}"]
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            (
                name.to_owned(),
                ElementEntry::from_parts(9, i as u64 * 64, 64, None, None),
            )
        })
        .collect();
    let fields = HashFieldMap::from(expected.clone());
    let decoded: HashMap<String, ElementEntry> =
        rmp_serde::from_slice(&rmp_serde::to_vec_named(&fields).unwrap()).unwrap();
    assert!(decoded == expected);
}

#[cfg(feature = "alloc-probe")]
#[test]
fn borrowed_hash_serialization_avoids_a_name_copy_per_field() {
    for count in [1, 64, 8192] {
        let fields = fixture(count);
        let probe = crate::alloc_probe::Probe::start();
        let old_bytes = {
            let owned: HashMap<String, ElementEntry> = fields.clone().into();
            rmp_serde::to_vec_named(&owned).unwrap()
        };
        let old = probe.stop();
        let probe = crate::alloc_probe::Probe::start();
        let new_bytes = rmp_serde::to_vec_named(&fields).unwrap();
        let new = probe.stop();
        let before: HashMap<String, ElementEntry> = rmp_serde::from_slice(&old_bytes).unwrap();
        let after: HashMap<String, ElementEntry> = rmp_serde::from_slice(&new_bytes).unwrap();
        assert!(before == after);
        assert!(old.allocs >= new.allocs + count as u64);
        assert!(old.alloc_bytes > new.alloc_bytes);
        println!("fields={count} owned_calls={} borrowed_calls={} owned_rust_bytes={} borrowed_rust_bytes={}",
            old.allocs, new.allocs, old.alloc_bytes, new.alloc_bytes);
    }
}
