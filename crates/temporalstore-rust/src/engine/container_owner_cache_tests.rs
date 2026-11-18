// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn address(offset: u64, ordinal: u64) -> ElementEntry {
    ElementEntry::from_parts(1, offset, 64, Some(ordinal), None)
}

fn vacated<K: RecordedKind>(
    map: &RecordedMap<K>,
    element: &<K::Elements as ElementMap>::Element,
) -> bool
where
    K::Elements: AddressedElementMap,
{
    map.block_an_element_removal_vacates("object", element)
        .is_some()
}

#[test]
fn set_shared_frames_survive_removal_and_same_ordinal_relocation() {
    let mut map = RecordedMap::<SetKind>::default();
    let first = b"first".to_vec();
    let second = b"second".to_vec();
    map.install_into("object", first.clone(), address(0, 7));
    map.install_into("object", second.clone(), address(0, 99));
    assert!(!vacated(&map, &first));
    assert!(!vacated(&map, &second));
    assert_eq!(map.highest_block_id("object"), Some(99));
    map.install_into("object", second.clone(), address(64, 99));
    assert!(vacated(&map, &first));
    assert!(vacated(&map, &second));
    map.install_into("object", second.clone(), address(0, 99));
    assert!(!vacated(&map, &first));
    assert!(map.remove_element(RecordedElementRemoval::<SetKind> {
        object_key: "object".into(),
        element: second,
    }));
    assert!(vacated(&map, &first));
    assert!(map.remove_element(RecordedElementRemoval::<SetKind> {
        object_key: "object".into(),
        element: first.clone(),
    }));
    assert!(!map.contains_key("object"));
    map.install_into("object", first.clone(), address(0, 7));
    assert!(vacated(&map, &first));
}

#[test]
fn list_and_scored_members_keep_physical_ownership_separate_from_ordinals() {
    let mut list = RecordedMap::<ListKind>::default();
    list.install_into("object", 0, address(0, 1));
    list.install_into("object", 1, address(0, 2));
    assert!(!vacated(&list, &0));
    list.install_into("object", 1, address(64, 2));
    assert!(vacated(&list, &0));

    let mut scored = RecordedMap::<ZSetKind>::default();
    let a = b"a".to_vec();
    let b = b"b".to_vec();
    scored.install_into("object", a.clone(), (17, address(0, 1)));
    scored.install_into("object", b.clone(), (23, address(0, 2)));
    assert!(!vacated(&scored, &a));
    scored.install_into("object", b.clone(), (99, address(64, 2)));
    assert!(vacated(&scored, &a));
    assert!(vacated(&scored, &b));
}

fn hash() -> RecordedMap<HashKind> {
    let mut map = RecordedMap::default();
    map.install_into("object", std::sync::Arc::from("a"), address(0, 1));
    map.install_into("object", std::sync::Arc::from("b"), address(0, 2));
    map
}

#[test]
fn replay_compaction_and_object_replacement_invalidate_ownership() {
    let mut map = hash();
    let a = std::sync::Arc::from("a");
    let b = std::sync::Arc::from("b");
    assert!(!vacated(&map, &a));
    assert!(map.replay_remove_element("object", &b));
    assert!(vacated(&map, &a));
    map.replay_install_element("object", b.clone(), address(0, 2));
    assert!(!vacated(&map, &a));
    for (_, mut values) in map.element_values_mut() {
        for (field, value) in values.iter_mut() {
            *value = if &field[..] == "a" {
                address(64, 1)
            } else {
                address(128, 2)
            };
        }
    }
    assert!(vacated(&map, &a));
    assert!(vacated(&map, &b));
    let selected = HashSet::from([ModelKey::from("object")]);
    map.try_visit_element_values_mut(Some(&selected), |_, mut values| {
        for (_, value) in values.iter_mut() {
            *value = address(0, 1);
        }
        Ok::<_, ()>(())
    })
    .unwrap();
    assert!(!vacated(&map, &a));
    map.remove_for_test("object");
    map.replay_install_element("object", a.clone(), address(0, 1));
    assert!(vacated(&map, &a));
    map.clear_for_test();
    map.replay_install_element("object", a.clone(), address(0, 1));
    assert!(vacated(&map, &a));
}

#[test]
fn durable_fold_and_reconcile_rebuild_ownership_from_current_members() {
    let mut map = hash();
    let a = std::sync::Arc::from("a");
    let b: ModelKey = std::sync::Arc::from("b");
    assert!(!vacated(&map, &a));
    let live = HashSet::from([
        super::super::live_block_key(&address(0, 1)),
        super::super::live_block_key(&address(64, 2)),
    ]);
    let carried = serde_json::to_value(vec![(b.clone(), address(64, 2))]).unwrap();
    map.fold_carried_elements("object", Some(&carried), &live, &mut 0);
    assert!(vacated(&map, &a));
    assert!(vacated(&map, &b));
    let mut derived = super::super::hash_field_map::HashFieldMap::default();
    derived.insert(a.clone(), address(0, 1));
    derived.insert(b.clone(), address(0, 2));
    map.reconcile_from_durable(
        HashMap::from([(ModelKey::from("object"), derived)]),
        &live,
        &mut 0,
    );
    assert!(!vacated(&map, &a));
    assert!(!vacated(&map, &b));
}

#[test]
fn cache_does_not_change_wire_clone_or_reloaded_membership() {
    let map = hash();
    let a = std::sync::Arc::from("a");
    let cold = serde_json::to_value(&map).unwrap();
    assert!(!vacated(&map, &a));
    assert_eq!(cold, serde_json::to_value(&map).unwrap());
    let cloned = map.clone();
    assert_eq!(map, cloned);
    assert!(!vacated(&cloned, &a));
    let loaded: RecordedMap<HashKind> = serde_json::from_value(cold).unwrap();
    assert!(!vacated(&loaded, &a));
}

#[test]
fn oversized_container_uses_exact_scan_for_shared_and_unshared_frames() {
    let mut map = RecordedMap::<SetKind>::default();
    for index in 0..8300u64 {
        map.install_into(
            "object",
            index.to_be_bytes().to_vec(),
            address(index * 64, index),
        );
    }
    let first = 0u64.to_be_bytes().to_vec();
    let alias = b"alias".to_vec();
    assert!(vacated(&map, &first));
    map.install_into("object", alias.clone(), address(0, 60000));
    assert!(!vacated(&map, &first));
    assert!(!vacated(&map, &alias));
    map.replay_remove_element("object", &alias);
    assert!(vacated(&map, &first));
}
