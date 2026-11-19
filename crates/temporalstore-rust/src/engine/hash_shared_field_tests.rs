// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::container_blocks::SharedHashFields;

#[test]
fn shared_field_construction_is_exact_sorted_and_uses_canonical_names() {
    for count in [0, 1, 3, 8, 21, 64, 256] {
        let names: Vec<_> = (0..count).rev().map(|i| format!("field-{i:03}")).collect();
        let shared = SharedHashFields::new(names.iter().map(String::as_str)).unwrap();
        let mut pool = std::collections::HashSet::new();
        for name in &names {
            pool.insert(FieldName::from(name.as_str()));
        }
        let address = ElementEntry::from_parts(7, 11, 48, None, None);
        reset_binary_search_count_for_test();
        let map = HashFieldMap::from_shared_fields(&shared, &mut pool, &address);
        assert_eq!(binary_search_count_for_test(), 0);
        assert_eq!(map.len(), count);
        assert_eq!(map.capacity_for_test(), count);
        assert!(map.is_sorted_by_field());
        assert_eq!(pool.len(), count);
        for (name, held) in map.iter() {
            assert!(std::sync::Arc::ptr_eq(name, pool.get(name.as_ref()).unwrap()));
            assert_eq!(held, &address);
        }
    }
}

#[test]
fn shared_field_rows_keep_the_map_wire_shape_and_mutation_behavior() {
    let shared = SharedHashFields::new(["c", "", "a"].into_iter()).unwrap();
    let mut pool = std::collections::HashSet::new();
    let address = ElementEntry::from_parts(7, 11, 48, None, None);
    let replacement = ElementEntry::from_parts(8, 12, 49, None, None);
    let mut map = HashFieldMap::from_shared_fields(&shared, &mut pool, &address);
    let encoded = rmp_serde::to_vec_named(&map).unwrap();
    let plain: HashMap<String, ElementEntry> = rmp_serde::from_slice(&encoded).unwrap();
    assert_eq!(plain.len(), 3);
    assert!(plain.values().all(|held| held == &address));
    let mut decoded: HashFieldMap = rmp_serde::from_slice(&encoded).unwrap();
    assert_eq!(decoded, map);
    for candidate in [&mut map, &mut decoded] {
        assert_eq!(candidate.insert(FieldName::from("a"), replacement.clone()), Some(address.clone()));
        assert_eq!(candidate.remove("c"), Some(address.clone()));
        assert_eq!(candidate.insert(FieldName::from("b"), replacement.clone()), None);
        assert!(candidate.is_sorted_by_field());
        assert_eq!(candidate.get(""), Some(&address));
        assert_eq!(candidate.get("a"), Some(&replacement));
        assert_eq!(candidate.get("b"), Some(&replacement));
        assert!(candidate.get("c").is_none());
    }
    assert_eq!(map, decoded);
}

#[test]
fn shared_field_construction_avoids_wide_row_growth_slack() {
    let names: Vec<_> = (0..21).rev().map(|i| format!("field-{i:03}")).collect();
    let shared = SharedHashFields::new(names.iter().map(String::as_str)).unwrap();
    let address = ElementEntry::from_parts(7, 11, 48, None, None);
    let mut pool = std::collections::HashSet::new();
    let map = HashFieldMap::from_shared_fields(&shared, &mut pool, &address);
    let mut incremental = HashFieldMap::default();
    for name in &names {
        incremental.insert(super::super::state::intern_shared(&mut pool, name), address.clone());
    }
    assert_eq!(map, incremental);
    let entry_bytes = std::mem::size_of::<(FieldName, ElementEntry)>();
    let packed = map.capacity_for_test() * entry_bytes;
    let prior = incremental.capacity_for_test() * entry_bytes;
    assert!(packed < prior);
    println!("fields=21 incremental_capacity_bytes={prior} packed_capacity_bytes={packed}");
}
