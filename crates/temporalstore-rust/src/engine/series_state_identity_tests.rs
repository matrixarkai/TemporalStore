// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::timestamped_series::{TimestampedSeries, TimestampedSeriesWrite};

type Points = BTreeMap<u64, ElementEntry>;
type Slots = HashMap<state::ModelKey, object_record::ObjectSlot>;

fn address(offset: u64) -> ElementEntry {
    ElementEntry::from_parts(1, offset, 16, None, None)
}

fn slot_with_filed_blocks() -> object_record::ObjectSlot {
    let mut record = object_record::ObjectRecord::default();
    record.set_block_size(123);
    let mut slot = object_record::ObjectSlot::from_record(record);
    slot.set(1, address(16));
    assert!(slot.file_ref(state::BlockLookupRef {
        routing_bucket: 7,
        block_ref_key: 11
    }));
    assert!(slot.file_ref(state::BlockLookupRef {
        routing_bucket: 9,
        block_ref_key: 12
    }));
    slot
}

#[test]
fn series_state_restore_keeps_the_record_and_all_filed_identities() {
    let slot = slot_with_filed_blocks();
    let record = slot.record();
    let refs = slot.refs().unwrap().as_slice().to_vec();
    let mut slots = Slots::from([("series".into(), slot)]);
    let points = Points::from([(0, address(32)), (u64::MAX, address(48))]);
    let value = serde_json::to_value(&points).unwrap();
    apply_key_state_field::<Points, _>(&mut slots, "series", Some(&value));
    let restored = slots.get("series").unwrap();
    assert_eq!(restored.record(), record);
    assert_eq!(restored.refs().unwrap().as_slice(), refs.as_slice());
    assert_eq!(restored.to_btree(), points);
    assert!(
        restored.get(1).is_none(),
        "old membership must not survive replacement"
    );
}

#[test]
fn clearing_series_keeps_filed_identities_until_the_block_fold_removes_them() {
    let slot = slot_with_filed_blocks();
    let record = slot.record();
    let refs = slot.refs().unwrap().as_slice().to_vec();
    let mut slots = Slots::from([("series".into(), slot)]);
    apply_key_state_field::<Points, _>(&mut slots, "series", None);
    let cleared = slots.get("series").unwrap();
    assert!(cleared.is_empty());
    assert_eq!(cleared.record(), record);
    assert_eq!(cleared.refs().unwrap().as_slice(), refs.as_slice());
    slots.get_mut("series").unwrap().take_refs();
    apply_key_state_field::<Points, _>(&mut slots, "series", None);
    assert!(!slots.contains_key("series"));
    apply_key_state_field::<Points, _>(&mut slots, "missing", None);
    assert!(slots.is_empty());
}

#[test]
fn explicit_empty_series_and_absent_series_remain_distinct() {
    let mut slots = Slots::new();
    let value = serde_json::to_value(Points::new()).unwrap();
    apply_key_state_field::<Points, _>(&mut slots, "empty", Some(&value));
    let empty = slots.get("empty").unwrap();
    assert!(empty.is_empty());
    assert!(empty.refs().is_none());
    apply_key_state_field::<Points, _>(&mut slots, "empty", Some(&serde_json::Value::Null));
    assert!(slots.is_empty());
    let points = Points::from([(4, address(64))]);
    let value = serde_json::to_value(&points).unwrap();
    apply_key_state_field::<Points, _>(&mut slots, "new", Some(&value));
    assert_eq!(slots.get("new").unwrap().to_btree(), points);
}
