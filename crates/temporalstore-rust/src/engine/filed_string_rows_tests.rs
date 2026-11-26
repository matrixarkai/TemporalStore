// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::Arc;
use crate::engine::state::BlockLookupRef;

fn key(value: &str) -> ModelKey { Arc::from(value) }

fn slot() -> ObjectSlot { ObjectSlot::default() }

#[test]
fn singleton_collision_and_collapse_keep_one_slot_per_key() {
    let mut rows = FiledStringRows::default();
    assert!(rows.is_empty());
    rows.insert(key("a"), 7, 7, slot());
    assert!(matches!(rows.owners[&7], FiledStringOwner::One(..)));
    let canonical = rows.get_key_value("a", 7).unwrap().0.clone();
    rows.insert(key("b"), 7, 7, slot());
    assert!(matches!(rows.owners[&7], FiledStringOwner::Many(..)));
    assert!(Arc::ptr_eq(&canonical, rows.get_key_value("a", 7).unwrap().0));
    assert_eq!(rows.len(), 2);
    assert!(rows.get("other", 7).is_none());
    assert!(rows.remove("other", 7).is_none());
    assert!(rows.remove("b", 7).is_some());
    assert!(matches!(rows.owners[&7], FiledStringOwner::One(..)));
    assert!(rows.exceptions.is_empty());
    assert!(rows.remove("a", 7).is_some());
    assert!(rows.is_empty());
    assert!(rows.owners.is_empty());
}

#[test]
fn reroute_updates_only_locator_and_never_orphans_misfiled_row() {
    let mut rows = FiledStringRows::default();
    rows.insert(key("a"), 1, 4, slot());
    let pointer = rows.get("a", 1).unwrap() as *const ObjectSlot;
    rows.reroute("a", 1, 2).unwrap();
    assert_eq!(rows.exceptions.get("a"), Some(&4));
    assert_eq!(rows.get("a", 2).unwrap() as *const ObjectSlot, pointer);
    rows.reroute("a", 2, 4).unwrap();
    assert!(rows.exceptions.is_empty());
    assert_eq!(rows.get("a", 4).unwrap() as *const ObjectSlot, pointer);
    rows.reroute("a", 4, 9).unwrap();
    assert_eq!(rows.exceptions.get("a"), Some(&4));
    assert_eq!(rows.capture(4).count(), 1);
    assert_eq!(rows.capture(9).count(), 0);
    assert!(rows.remove("a", 9).is_some());
    assert!(rows.exceptions.is_empty());
    assert!(rows.is_empty());
}

#[test]
fn remap_moves_rekeys_and_rejects_collision_without_changes() {
    let mut rows = FiledStringRows::default();
    rows.insert(key("a"), 1, 4, slot());
    rows.insert(key("b"), 2, 4, slot());
    assert_eq!(rows.remap("a", 1, key("b"), 2, 8), Err(RemapError::DestinationExists));
    assert_eq!(rows.capture(4).count(), 2);
    assert_eq!(rows.exceptions.get("a"), Some(&4));
    assert_eq!(rows.remap("missing", 1, key("c"), 3, 8), Err(RemapError::MissingSource));
    rows.remap("a", 1, key("c"), 3, 8).unwrap();
    assert!(rows.get("a", 1).is_none());
    assert!(!rows.exceptions.contains_key("a"));
    assert_eq!(rows.exceptions.get("c"), Some(&8));
    assert!(rows.get("c", 3).is_some());
    assert!(matches!(rows.owners[&4], FiledStringOwner::One(..)));
    rows.remap("c", 3, key("c"), 8, 8).unwrap();
    assert!(!rows.exceptions.contains_key("c"));
    assert_eq!(rows.len(), 2);
    rows.remap("c", 8, key("d"), 9, 8).unwrap();
    assert_eq!(rows.capture(8).next().unwrap().0.as_ref(), "d");
    assert!(rows.get("d", 9).is_some());
}

#[test]
fn metadata_replacement_and_remap_preserve_blocks_and_owned_elements() {
    let mut rows = FiledStringRows::default();
    let block = BlockLookupRef { routing_bucket: 6, block_ref_key: 42 };
    let mut original = ObjectSlot::from(BTreeMap::from([(10, crate::block_store::ElementEntry::default())]));
    assert!(original.file_ref(block));
    let elements_pointer = original.owned_elements().unwrap().as_ptr();
    rows.insert(key("a"), 1, 4, original);
    rows.replace_record("a", 1, ObjectRecord::default()).unwrap();
    assert!(!rows.get_mut("a", 1).unwrap().file_ref(block));
    assert_eq!(rows.get("a", 1).unwrap().owned_elements().unwrap().as_ptr(), elements_pointer);
    rows.remap("a", 1, key("renamed"), 2, 5).unwrap();
    assert!(!rows.get_mut("renamed", 2).unwrap().file_ref(block));
    assert_eq!(rows.get("renamed", 2).unwrap().owned_elements().unwrap().as_ptr(), elements_pointer);
    let displaced = rows.insert(key("renamed"), 2, 6, slot()).unwrap();
    assert!(displaced.refs().is_some());
    assert_eq!(displaced.owned_elements().unwrap().as_ptr(), elements_pointer);
    assert!(rows.get("renamed", 2).unwrap().refs().is_none());
    assert_eq!(rows.exceptions.get("renamed"), Some(&6));
    assert!(!rows.owners.contains_key(&5));
    assert_eq!(rows.len(), 1);
}

#[test]
fn bounded_cursor_seeks_exclusively_in_one_owner() {
    let mut rows = FiledStringRows::default();
    for name in ["c", "a", "b"] { rows.insert(key(name), 1, 4, slot()); }
    rows.insert(key("elsewhere"), 8, 8, slot());
    let page = |after, limit| rows.rows_after(4, after, limit)
        .map(|(key, _)| key.to_string()).collect::<Vec<_>>();
    assert_eq!(page(None, 2), ["a", "b"]);
    assert_eq!(page(Some("b"), 2), ["c"]);
    assert_eq!(page(Some("az"), 1), ["b"]);
    assert!(page(Some("z"), 5).is_empty());
    assert!(page(None, 0).is_empty());
    assert!(rows.rows_after(99, None, 1).next().is_none());
    assert!(rows.rows_after(8, Some("elsewhere"), 1).next().is_none());
    assert_eq!(rows.capture(4).count(), 3);
}

#[test]
fn same_owner_replacement_keeps_the_canonical_key_and_slot_position() {
    for second_row in [false, true] {
        let mut rows = FiledStringRows::default();
        let original = key("hot-row");
        let mut before = slot();
        before.set_logical_size(91);
        rows.insert(original.clone(), 1, 4, before);
        if second_row { rows.insert(key("peer"), 1, 4, slot()); }
        rows.insert(key("other-owner"), 8, 8, slot());
        let position = rows.get("hot-row", 1).unwrap() as *const ObjectSlot;
        let fresh_key = key("hot-row");
        assert!(!Arc::ptr_eq(&original, &fresh_key));
        let mut replacement = slot();
        replacement.set_logical_size(17);
        let retired = rows.insert(fresh_key, 1, 4, replacement).unwrap();
        assert_eq!(retired.logical_size(), Some(91));
        assert_eq!(rows.get("hot-row", 1).unwrap().logical_size(), Some(17));
        assert_eq!(rows.get("hot-row", 1).unwrap() as *const ObjectSlot, position);
        assert!(Arc::ptr_eq(&original, rows.get_key_value("hot-row", 1).unwrap().0));
        assert_eq!(rows.len(), if second_row { 3 } else { 2 });
        assert_eq!(rows.capture(4).count(), if second_row { 2 } else { 1 });
        assert_eq!(rows.exceptions.get("hot-row"), Some(&4));
        assert!(rows.get("other-owner", 8).is_some());
    }
}

#[test]
fn flat_mutable_iteration_has_unique_slots_and_exact_size() {
    let mut rows = FiledStringRows::default();
    for (name, filed) in [("b", 2), ("a", 2), ("c", 5)] {
        rows.insert(key(name), 1, filed, slot());
    }
    let mut iter: FiledRowsIterMut<'_> = (&mut rows).into_iter();
    assert_eq!(iter.len(), 3);
    // Retain simultaneous mutable borrows across next() and across owner boundaries.
    let (_, first) = iter.next().unwrap();
    let (_, second) = iter.next().unwrap();
    let (_, third) = iter.next().unwrap();
    assert_eq!(iter.len(), 0);
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
    first.file_ref(BlockLookupRef { routing_bucket: 1, block_ref_key: 1 });
    second.file_ref(BlockLookupRef { routing_bucket: 1, block_ref_key: 2 });
    third.file_ref(BlockLookupRef { routing_bucket: 1, block_ref_key: 3 });
    assert!(rows.values().all(|slot| slot.refs().is_some()));
    assert_eq!(rows.keys().map(|key| key.as_ref()).collect::<Vec<_>>(), ["a", "b", "c"]);
    assert_eq!(rows.iter_filed().map(|(filed, _, _)| filed).collect::<Vec<_>>(), [2, 2, 5]);
    for (_, _, slot) in rows.iter_filed_mut() { assert!(slot.take_refs().is_some()); }
    for slot in rows.values_mut() { assert!(slot.refs().is_none()); }
    let iter: FiledRowsIter<'_> = (&rows).into_iter();
    assert_eq!(iter.len(), 3);
    assert_eq!(rows.clone().len(), 3);
}

#[test]
fn consuming_export_import_moves_allocations_and_retains_filed_identity() {
    let payload = ObjectSlot::from(BTreeMap::from([(10, crate::block_store::ElementEntry::default())]));
    let pointer = payload.owned_elements().unwrap().as_ptr();
    let mut rows = FiledStringRows::default();
    rows.insert(key("a"), 1, 7, payload);
    rows.insert(key("b"), 1, 7, slot());
    let restored = FiledStringRows::try_from_rows(rows.into_rows(), |_| 2).unwrap();
    assert_eq!(restored.get("a", 2).unwrap().owned_elements().unwrap().as_ptr(), pointer);
    assert_eq!(restored.exceptions.get("a"), Some(&7));
    let mut owners = restored.into_owners();
    let (filed, owner) = owners.next().unwrap();
    assert_eq!(filed, 7);
    assert_eq!(owner.into_rows().count(), 2);
    assert!(owners.next().is_none());
    assert!(FiledStringRows::try_from_rows([(1, key("x"), slot()), (2, key("x"), slot())], |_| 3).is_err());
}

#[test]
fn retain_mutates_without_copying_and_cleans_locators_and_owners() {
    let mut rows = FiledStringRows::default();
    for (name, filed) in [("a", 4), ("b", 4), ("c", 4), ("d", 8)] {
        rows.insert(key(name), 1, filed, slot());
    }
    let canonical = rows.get_key_value("a", 1).unwrap().0.clone();
    let mut visits = 0;
    rows.retain(|key, slot| {
        visits += 1;
        slot.file_ref(BlockLookupRef { routing_bucket: 2, block_ref_key: 99 });
        key.as_ref() == "a"
    });
    assert_eq!(visits, 4);
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows.owners[&4], FiledStringOwner::One(..)));
    assert!(!rows.owners.contains_key(&8));
    assert_eq!(rows.exceptions.len(), 1);
    assert_eq!(rows.exceptions.get("a"), Some(&4));
    assert!(Arc::ptr_eq(&canonical, rows.get_key_value("a", 1).unwrap().0));
    assert!(rows.get("a", 1).unwrap().refs().is_some());
    rows.retain(|_, _| false);
    assert!(rows.is_empty());
    assert!(rows.owners.is_empty());
    assert!(rows.exceptions.is_empty());
}
