// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::object_record::{BlockAddress, ObjectRecord, ObjectSlot};
use super::state::{BlockIndex, BlockKeysByPosition, BlockLookupRef, CoreIndex, ModelKey};
use super::storage_bucket_internals::StoredModelKind;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use std::collections::HashMap;

fn embedded_at<'a>(
    index: &CoreIndex,
    keys: &BlockKeysByPosition<'_>,
    kind: StoredModelKind,
    key: &str,
    slot: &'a ObjectSlot,
) -> Option<&'a BlockLookupRef> {
    if kind != StoredModelKind::String
        || slot.owned_elements().is_some()
        || !matches!(slot.record().page_address(), BlockAddress::One(_))
    {
        return None;
    }
    let refs = slot.refs()?;
    if refs.len() != 1 {
        return None;
    }
    let position = refs.iter().next()?;
    let (stored_kind, stored_key) =
        keys.live_identity(position.routing_bucket, position.block_ref_key)?;
    if stored_kind != kind.as_str() || stored_key.as_ref() != key {
        return None;
    }
    let block = index
        .bucket_map
        .get(&position.routing_bucket)?
        .block_index
        .get(&position.block_ref_key)?;
    if block.deleted() || block.model_id() != kind || slot.address() != block.address {
        return None;
    }
    Some(position)
}

pub(super) fn embedded_slot<'a>(
    index: &'a CoreIndex,
    keys: &BlockKeysByPosition<'_>,
    routing_bucket: u32,
    handle: u64,
    key: &str,
    block: &BlockIndex,
) -> Option<&'a ObjectSlot> {
    if block.model_id() != StoredModelKind::String {
        return None;
    }
    let slot = index.objects.get(&StoredModelKind::String)?.get(key)?;
    let position = embedded_at(index, keys, StoredModelKind::String, key, slot)?;
    if position.routing_bucket != routing_bucket || position.block_ref_key != handle {
        return None;
    }
    Some(slot)
}

pub(super) fn metadata(slot: &ObjectSlot) -> ObjectRecord {
    let mut metadata = slot.record();
    // The block entry owns the address; every other record field travels unchanged.
    metadata.set_block_address(BlockAddress::One(0));
    metadata.set_block_size(0);
    metadata
}

pub(super) struct ObjectRows<'a> {
    pub index: &'a CoreIndex,
    pub keys: &'a BlockKeysByPosition<'a>,
}

impl Serialize for ObjectRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = serializer.serialize_map(Some(self.index.objects.len()))?;
        for (kind, rows) in &self.index.objects {
            if *kind == StoredModelKind::String {
                output.serialize_entry(
                    kind,
                    &SeparateRows {
                        index: self.index,
                        keys: self.keys,
                        kind: *kind,
                        rows,
                    },
                )?;
            } else {
                output.serialize_entry(kind, rows)?;
            }
        }
        output.end()
    }
}

struct SeparateRows<'a> {
    index: &'a CoreIndex,
    keys: &'a BlockKeysByPosition<'a>,
    kind: StoredModelKind,
    rows: &'a HashMap<ModelKey, ObjectSlot>,
}

impl Serialize for SeparateRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let count = self
            .rows
            .iter()
            .filter(|(key, slot)| {
                embedded_at(self.index, self.keys, self.kind, key, slot).is_none()
            })
            .count();
        let mut output = serializer.serialize_map(Some(count))?;
        for (key, slot) in self.rows {
            if embedded_at(self.index, self.keys, self.kind, key, slot).is_none() {
                output.serialize_entry(key, slot)?;
            }
        }
        output.end()
    }
}

pub(super) fn restore_record(
    mut record: ObjectRecord,
    block: &BlockIndex,
) -> Result<ObjectRecord, String> {
    if block.model_id() != StoredModelKind::String
        || block.deleted()
        || block.address.block_id().is_some_and(|id| id != 0)
    {
        return Err("embedded object record requires a live single-block string".into());
    }
    if !matches!(record.page_address(), BlockAddress::One(0)) || record.block_size() != 0 {
        return Err("embedded object record must not carry a second address".into());
    }
    record.set_address(&block.address);
    Ok(record)
}

#[cfg(test)]
#[path = "checkpoint_object_rows_tests.rs"]
mod tests;
