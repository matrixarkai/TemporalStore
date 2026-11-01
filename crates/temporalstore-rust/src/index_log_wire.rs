// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LIVE DELTA RECORD, IN THE PROTOBUF SCHEMA OF THE DESIGN BEING FOLLOWED, LOSSLESSLY.
//!
//! [`crate::index_log_record`] is a faithful copy of that schema and says why it cannot be the
//! live row: its object id is 32 bits, so a key's identity collides after 296,006 distinct keys,
//! and its item carries no characters, so replay has nothing to rebuild an entry's key from. Both
//! objections are about what the schema CANNOT HOLD, and neither is about the schema's numbers. So
//! this module keeps every field number, wire type and meaning the schema already has, and carries
//! what the live record needs at field numbers the schema does not use. A reader built from the
//! plain schema skips those numbers; a reader built from this one reads all of them.
//!
//! WHAT IS THE SCHEMA'S, UNCHANGED: tags 1..=9 of the record and 1..=7 of the item, and the
//! meta item's five tags. WHAT IS OURS: record tags 20.., item tags 8.., slab tags 9... Nothing is
//! renumbered and nothing is reused.
//!
//! IT MAPS THE STRUCT THE LOG ALREADY HOLDS AFTER ITS STRIPS, value for value. The omissions the
//! positional row banks -- the block handle when it is the derivation of its neighbours, the object
//! id when it is the hash of the row's own fields, the object key when the record hoists it -- are
//! empty or zero in that struct, and a protobuf field that is empty or zero is not written at all.
//! So the strips survive as ABSENCE, which is a stronger form than the sentinel they use in the
//! positional row, and `restore_*` at the decode site needs no change.
//!
//! WHAT IT DOES NOT DO: it does not change what is on disk. No writer calls it. See the tests for
//! the three claims it exists to hold -- the round trip is exact, the plain schema reads our bytes,
//! and each field it adds is load-bearing (an inverse plant per field).

use std::collections::BTreeMap;

use prost::Message;

use crate::block_store::ElementEntry;
use crate::index_log::{
    IndexDeltaRecord, IndexItem, IndexItemKind, MetaItem, SlabCatalogEntry, SlabCatalogState,
    MODEL_ID_NUMBERS,
};
use crate::types::ShardId;

/// On-disk version of the record, as the schema numbers it.
pub const INDEX_LOG_WIRE_VERSION: u32 = 1;

/// The schema's slab states, including the gap at 3 and RECYCLED at 4.
const SLAB_CREATED: i32 = 1;
const SLAB_FROZEN: i32 = 2;
const SLAB_RECYCLED: i32 = 4;

/// One block-location entry.
#[derive(Clone, PartialEq, Message)]
pub struct ProtoIndexItem {
    // ---- the schema's seven fields -------------------------------------------------------
    /// The low 32 bits of the object id, which is all the schema can hold. The exact value is at
    /// tag 8; this is what a reader built from the plain schema sees.
    #[prost(uint32, tag = "1")]
    pub object_id: u32,
    #[prost(uint32, tag = "2")]
    pub block_id: u32,
    /// The slab in the high half, the offset in the low.
    #[prost(uint64, tag = "3")]
    pub address: u64,
    #[prost(uint32, tag = "4")]
    pub size: u32,
    /// The block still lives in the write-ahead log. DERIVED from the entry on the way in and
    /// cross-checked on the way out.
    #[prost(bool, tag = "5")]
    pub in_wal: bool,
    #[prost(bool, tag = "6")]
    pub deleted: bool,
    /// The model, as one more than its position in the model table; 0 means "named at tag 16, or
    /// no model at all".
    #[prost(uint32, tag = "7")]
    pub model_id: u32,
    // ---- ours ----------------------------------------------------------------------------
    /// The exact object id. Zero when the row stripped it, exactly as the positional row writes
    /// zero there.
    #[prost(uint64, tag = "8")]
    pub object_id_wide: u64,
    /// Per ITEM, because one record is scoped to a shard and a write touches many buckets.
    #[prost(uint32, tag = "9")]
    pub routing_bucket: u32,
    /// The characters of the object key. Empty when the record hoists it (tag 22 of the record).
    #[prost(string, tag = "10")]
    pub object_key: String,
    /// The composite handle, empty when it is the derivation of the fields beside it.
    #[prost(string, tag = "11")]
    pub block_ref_key: String,
    /// 0 block (the default, so not written), 1 object, 2 meta.
    #[prost(uint32, tag = "12")]
    pub kind: u32,
    /// The item names no entry. Inverted so the ordinary item does not pay for the flag.
    #[prost(bool, tag = "13")]
    pub no_entry: bool,
    /// The entry carries no generation (an index written before the field existed). Inverted for
    /// the same reason.
    #[prost(bool, tag = "14")]
    pub without_generation: bool,
    /// A model the table does not list, by name.
    #[prost(string, tag = "16")]
    pub model_name: String,
}

/// One slab's catalog entry.
#[derive(Clone, PartialEq, Message)]
pub struct ProtoSlabInfo {
    // ---- the schema's eight fields, of which init/frozen/recycled time have no source here ---
    #[prost(uint32, tag = "1")]
    pub slab_id: u32,
    #[prost(uint64, tag = "2")]
    pub total_bytes: u64,
    #[prost(enumeration = "ProtoSlabState", tag = "3")]
    pub state: i32,
    #[prost(uint64, tag = "4")]
    pub init_time_ms: u64,
    /// `optional` changes presence, not the wire: a plain reader sees the same varint.
    #[prost(uint64, optional, tag = "5")]
    pub created_time_ms: Option<u64>,
    #[prost(uint64, tag = "6")]
    pub frozen_time_ms: u64,
    #[prost(uint64, tag = "7")]
    pub recycled_time_ms: u64,
    #[prost(uint64, tag = "8")]
    pub version: u64,
    // ---- ours ----------------------------------------------------------------------------
    /// The exact slab id; zero when it fits tag 1.
    #[prost(uint64, tag = "9")]
    pub slab_id_wide: u64,
    /// The schema has one state for "reclaimed"; we have two. Set for the purged one.
    #[prost(bool, tag = "10")]
    pub purged: bool,
    #[prost(uint64, optional, tag = "11")]
    pub updated_time_ms: Option<u64>,
    #[prost(uint64, tag = "12")]
    pub logical_bytes: u64,
    #[prost(uint64, optional, tag = "13")]
    pub first_block_id: Option<u64>,
    #[prost(uint64, optional, tag = "14")]
    pub last_block_id: Option<u64>,
}

/// Slab lifecycle state, numbered as the schema numbers it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ProtoSlabState {
    Init = 0,
    Created = 1,
    Frozen = 2,
    Recycled = 4,
}

/// The index meta record: the dump watermark and the slab catalog.
#[derive(Clone, PartialEq, Message)]
pub struct ProtoMetaItem {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    /// The lowest WAL position still needed. A sequence here, where the schema's is a byte
    /// position: same role, different unit, and the unit is ours.
    #[prost(uint64, tag = "2")]
    pub start_wal_id: u64,
    #[prost(btree_map = "uint32, message", tag = "3")]
    pub slabs: BTreeMap<u32, ProtoSlabInfo>,
    #[prost(uint64, tag = "4")]
    pub timestamp_ms: u64,
    #[prost(uint64, tag = "5")]
    pub slab_version: u64,
    // ---- ours ----------------------------------------------------------------------------
    /// Entries whose 32-bit key collided with another's. The map cannot hold two under one key,
    /// and a catalog that silently lost a slab would be a slab nothing ever collects.
    #[prost(message, repeated, tag = "15")]
    pub overflow_slabs: Vec<ProtoSlabInfo>,
}

/// One delta record.
#[derive(Clone, PartialEq, Message)]
pub struct ProtoIndexLog {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    // Tag 2 is retired in the schema. Not reused.
    /// The schema scopes a record to one routing bucket. A live record spans many, so the bucket
    /// is on the item and this is not written.
    #[prost(fixed64, tag = "3")]
    pub routing_bucket: u64,
    #[prost(message, repeated, tag = "4")]
    pub items: Vec<ProtoIndexItem>,
    #[prost(message, optional, tag = "5")]
    pub meta_item: Option<ProtoMetaItem>,
    // Tag 6 (per-object metadata) has no live producer here.
    #[prost(uint64, tag = "7")]
    pub sequence: u64,
    /// The WAL sequence this record reflects. `optional`: none and zero are different answers.
    #[prost(uint64, optional, tag = "8")]
    pub wal_sequence: Option<u64>,
    #[prost(uint64, tag = "9")]
    pub timestamp_ms: u64,
    // ---- ours ----------------------------------------------------------------------------
    #[prost(uint64, tag = "20")]
    pub shard_id: u64,
    #[prost(bool, tag = "21")]
    pub upsert: bool,
    /// The one object key every item shares, when the record hoists it. `optional`: an empty
    /// shared key is a real answer and is not the same as no hoist.
    #[prost(string, optional, tag = "22")]
    pub shared_object_key: Option<String>,
    /// Opaque per-key state, one JSON document each.
    #[prost(bytes = "vec", repeated, tag = "23")]
    pub key_states: Vec<Vec<u8>>,
}

fn kind_to_number(kind: IndexItemKind) -> u32 {
    match kind {
        IndexItemKind::Block => 0,
        IndexItemKind::Object => 1,
        IndexItemKind::Meta => 2,
    }
}

fn kind_from_number(number: u32) -> Result<IndexItemKind, String> {
    match number {
        0 => Ok(IndexItemKind::Block),
        1 => Ok(IndexItemKind::Object),
        2 => Ok(IndexItemKind::Meta),
        other => Err(format!("unknown index item kind {other}")),
    }
}

impl ProtoIndexItem {
    pub fn from_item(item: &IndexItem) -> Self {
        let mut wire = ProtoIndexItem {
            object_id: item.object_id as u32,
            object_id_wide: item.object_id,
            deleted: item.deleted,
            routing_bucket: item.routing_bucket,
            object_key: item.object_key.to_string(),
            block_ref_key: item.block_ref_key.clone(),
            kind: kind_to_number(item.kind),
            ..ProtoIndexItem::default()
        };
        match MODEL_ID_NUMBERS.iter().position(|name| *name == item.model_id) {
            Some(position) => wire.model_id = position as u32 + 1,
            None => wire.model_name = item.model_id.clone(),
        }
        match item.entry.as_ref() {
            Some(entry) => {
                wire.block_id = entry.block_id().unwrap_or_default() as u32;
                wire.address = entry.address_word();
                wire.size = entry.length() as u32;
                wire.in_wal = entry.log_resident();
                wire.without_generation = entry.generation().is_none();
            }
            None => wire.no_entry = true,
        }
        wire
    }

    pub fn into_item(self) -> Result<IndexItem, String> {
        let model_id = if !self.model_name.is_empty() {
            self.model_name.clone()
        } else if self.model_id > 0 {
            MODEL_ID_NUMBERS
                .get(self.model_id as usize - 1)
                .map(|name| (*name).to_string())
                .ok_or_else(|| format!("unknown model number {}", self.model_id))?
        } else {
            String::new()
        };
        let entry = if self.no_entry {
            None
        } else {
            let slab = self.address >> 32;
            let offset = self.address & 0xFFFF_FFFF;
            let block_id = (!self.in_wal).then_some(u64::from(self.block_id));
            let entry = ElementEntry::try_from_wire_parts(
                slab,
                offset,
                u64::from(self.size),
                block_id,
                !self.without_generation,
            )
            .map_err(|out_of_range| out_of_range.to_string())?;
            // The word is the schema's own field, so a reader that wrote it differently is
            // refused here rather than read as a different block.
            if entry.address_word() != self.address {
                return Err(format!(
                    "index item address {:#x} does not survive the split into slab {slab} and \
                     offset {offset}",
                    self.address
                ));
            }
            Some(entry)
        };
        let object_id = if self.object_id_wide != 0 {
            self.object_id_wide
        } else {
            u64::from(self.object_id)
        };
        Ok(IndexItem {
            kind: kind_from_number(self.kind)?,
            routing_bucket: self.routing_bucket,
            block_ref_key: self.block_ref_key,
            object_key: std::sync::Arc::from(self.object_key.as_str()),
            model_id,
            object_id,
            entry,
            deleted: self.deleted,
        })
    }
}

impl ProtoSlabInfo {
    pub fn from_entry(entry: &SlabCatalogEntry) -> Self {
        let (state, purged) = match entry.state {
            SlabCatalogState::Active => (SLAB_CREATED, false),
            SlabCatalogState::Sealed => (SLAB_FROZEN, false),
            SlabCatalogState::DelayedDestroy => (SLAB_RECYCLED, false),
            SlabCatalogState::Purged => (SLAB_RECYCLED, true),
        };
        ProtoSlabInfo {
            slab_id: entry.block_slab_id as u32,
            slab_id_wide: if entry.block_slab_id > u64::from(u32::MAX) {
                entry.block_slab_id
            } else {
                0
            },
            total_bytes: entry.physical_bytes,
            state,
            purged,
            created_time_ms: entry.created_unix_ms,
            updated_time_ms: entry.updated_unix_ms,
            logical_bytes: entry.logical_bytes,
            first_block_id: entry.first_block_id,
            last_block_id: entry.last_block_id,
            version: entry.version,
            ..ProtoSlabInfo::default()
        }
    }

    pub fn into_entry(self) -> Result<SlabCatalogEntry, String> {
        let state = match (self.state, self.purged) {
            // The schema's INIT has no counterpart here: a slab reserved and not yet created is
            // one nothing writes to, which is what Active without a file already is.
            (0, _) | (SLAB_CREATED, _) => SlabCatalogState::Active,
            (SLAB_FROZEN, _) => SlabCatalogState::Sealed,
            (SLAB_RECYCLED, true) => SlabCatalogState::Purged,
            (SLAB_RECYCLED, false) => SlabCatalogState::DelayedDestroy,
            (other, _) => return Err(format!("unknown slab state {other}")),
        };
        Ok(SlabCatalogEntry {
            block_slab_id: if self.slab_id_wide != 0 {
                self.slab_id_wide
            } else {
                u64::from(self.slab_id)
            },
            state,
            physical_bytes: self.total_bytes,
            logical_bytes: self.logical_bytes,
            created_unix_ms: self.created_time_ms,
            updated_unix_ms: self.updated_time_ms,
            first_block_id: self.first_block_id,
            last_block_id: self.last_block_id,
            version: self.version,
        })
    }
}

impl ProtoMetaItem {
    pub fn from_meta(meta: &MetaItem) -> Self {
        let mut wire = ProtoMetaItem {
            version: meta.version,
            start_wal_id: meta.start_wal_sequence,
            timestamp_ms: meta.timestamp_ms,
            slab_version: meta.slab_version,
            ..ProtoMetaItem::default()
        };
        for entry in &meta.slabs {
            let info = ProtoSlabInfo::from_entry(entry);
            // Keyed by the 32 bits the schema has room for. A second slab under the same key goes
            // to the overflow list instead of replacing the first.
            if wire.slabs.contains_key(&info.slab_id) {
                wire.overflow_slabs.push(info);
            } else {
                wire.slabs.insert(info.slab_id, info);
            }
        }
        wire
    }

    /// The catalog comes back ordered by slab id, which is the order `slab_catalog` builds it in.
    pub fn into_meta(self) -> Result<MetaItem, String> {
        let mut slabs = Vec::with_capacity(self.slabs.len() + self.overflow_slabs.len());
        for info in self.slabs.into_values().chain(self.overflow_slabs) {
            slabs.push(info.into_entry()?);
        }
        slabs.sort_by_key(|entry| entry.block_slab_id);
        Ok(MetaItem {
            version: self.version,
            start_wal_sequence: self.start_wal_id,
            timestamp_ms: self.timestamp_ms,
            slabs,
            slab_version: self.slab_version,
        })
    }
}

impl ProtoIndexLog {
    pub fn from_record(record: &IndexDeltaRecord) -> Result<Self, String> {
        let mut key_states = Vec::with_capacity(record.key_states.len());
        for state in &record.key_states {
            key_states.push(serde_json::to_vec(state).map_err(|error| error.to_string())?);
        }
        Ok(ProtoIndexLog {
            version: INDEX_LOG_WIRE_VERSION,
            items: record.items.iter().map(ProtoIndexItem::from_item).collect(),
            meta_item: record.meta.as_ref().map(ProtoMetaItem::from_meta),
            sequence: record.sequence,
            wal_sequence: record.applied_wal_sequence,
            shard_id: record.shard_id,
            upsert: record.upsert,
            shared_object_key: record.shared_object_key.clone(),
            key_states,
            ..ProtoIndexLog::default()
        })
    }

    pub fn into_record(self) -> Result<IndexDeltaRecord, String> {
        let mut items = Vec::with_capacity(self.items.len());
        for item in self.items {
            items.push(item.into_item()?);
        }
        let mut key_states = Vec::with_capacity(self.key_states.len());
        for state in &self.key_states {
            key_states.push(serde_json::from_slice(state).map_err(|error| error.to_string())?);
        }
        Ok(IndexDeltaRecord {
            shard_id: self.shard_id as ShardId,
            sequence: self.sequence,
            items,
            meta: self.meta_item.map(ProtoMetaItem::into_meta).transpose()?,
            applied_wal_sequence: self.wal_sequence,
            key_states,
            upsert: self.upsert,
            shared_object_key: self.shared_object_key,
        })
    }
}

/// The record as protobuf bytes.
pub fn encode_delta_record(record: &IndexDeltaRecord) -> Result<Vec<u8>, String> {
    Ok(ProtoIndexLog::from_record(record)?.encode_to_vec())
}

/// The record from protobuf bytes.
pub fn decode_delta_record(bytes: &[u8]) -> Result<IndexDeltaRecord, String> {
    ProtoIndexLog::decode(bytes)
        .map_err(|error| error.to_string())?
        .into_record()
}

#[cfg(test)]
#[path = "index_log_wire_tests.rs"]
mod tests;
