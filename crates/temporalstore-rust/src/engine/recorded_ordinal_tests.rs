// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn address(id: u64) -> ElementEntry {
    ElementEntry::from_parts(1, id * 16, 16, Some(id), None)
}

fn hash() -> RecordedMap<HashKind> {
    let mut map = RecordedMap::default();
    for id in 0..8 {
        map.insert_element_for_test(
            "history",
            std::sync::Arc::from(format!("field-{id}")),
            address(id),
        );
    }
    map
}

#[test]
fn cache_does_not_change_clone_equality_or_wire_bytes() {
    let map = hash();
    // The field codec uses a hash map; canonicalize its existing arbitrary key order.
    let cold = serde_json::to_vec(&serde_json::to_value(&map).unwrap()).unwrap();
    assert_eq!(map.highest_block_id("history"), Some(7));
    assert_eq!(cold, serde_json::to_vec(&serde_json::to_value(&map).unwrap()).unwrap());
    let cloned = map.clone();
    assert_eq!(map, cloned);
    assert_eq!(cloned.highest_block_id("history"), Some(7));
    let loaded: RecordedMap<HashKind> = serde_json::from_slice(&cold).unwrap();
    assert_eq!(loaded.highest_block_id("history"), Some(7));
}

#[test]
fn replacing_the_highest_member_does_not_repeat_the_resident_scan() {
    let mut map = hash();
    assert_eq!(map.highest_block_id("history"), Some(7));
    let scans = super::super::state::SIBLING_SCAN_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let mut previous = std::sync::Arc::from("field-7");
    for index in 0..1000 {
        assert!(map.remove_element(RecordedElementRemoval::<HashKind> {
            object_key: "history".into(),
            element: previous,
        }));
        assert_eq!(map.highest_block_id("history"), Some(6));
        let next: ModelKey = std::sync::Arc::from(format!("new-{index}"));
        map.insert_element_for_test("history", next.clone(), address(7));
        assert_eq!(map.highest_block_id("history"), Some(7));
        previous = next;
    }
    assert_eq!(
        super::super::state::SIBLING_SCAN_CALLS.load(std::sync::atomic::Ordering::Relaxed),
        scans
    );
}

#[test]
fn mutations_replay_and_compaction_cannot_keep_stale_maxima() {
    let mut map = hash();
    assert_eq!(map.highest_block_id("history"), Some(7));
    map.replay_remove_element("history", &std::sync::Arc::from("field-7"));
    assert_eq!(map.highest_block_id("history"), Some(6));
    map.insert_element_for_test("history", std::sync::Arc::from("field-6"), address(2));
    assert_eq!(map.highest_block_id("history"), Some(5));
    for (_, mut values) in map.element_values_mut() {
        for (_, value) in values.iter_mut() {
            *value = address(1);
        }
    }
    assert_eq!(map.highest_block_id("history"), Some(1));
    map.remove_for_test("history");
    assert_eq!(map.highest_block_id("history"), None);
    map.replay_install_element("history", std::sync::Arc::from("new"), address(3));
    assert_eq!(map.highest_block_id("history"), Some(3));
    map.clear_for_test();
    assert_eq!(map.highest_block_id("history"), None);
}

#[test]
fn durable_fold_and_reconcile_invalidate_prior_summaries() {
    let mut map = hash();
    assert_eq!(map.highest_block_id("history"), Some(7));
    let mut carried = super::super::hash_field_map::HashFieldMap::default();
    carried.insert(std::sync::Arc::from("later"), address(12));
    let live = std::iter::once(super::super::live_block_key(&address(12))).collect();
    let mut skipped = 0;
    let rows: Vec<_> = carried.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    map.fold_carried_elements(
        "history",
        Some(&serde_json::to_value(&rows).unwrap()),
        &live,
        &mut skipped,
    );
    assert_eq!(map.highest_block_id("history"), Some(12));
    let mut derived = HashMap::new();
    derived.insert(std::sync::Arc::from("history"), carried);
    let mut refused = 0;
    map.reconcile_from_durable(derived, &live, &mut refused);
    assert_eq!(map.highest_block_id("history"), Some(12));
    map.elements_mut_for_test("history")
        .unwrap()
        .insert(std::sync::Arc::from("newest"), address(15));
    assert_eq!(map.highest_block_id("history"), Some(15));
}
