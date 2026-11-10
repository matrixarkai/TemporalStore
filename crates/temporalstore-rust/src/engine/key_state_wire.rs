// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PER-KEY STATE BLOB IS A PROTOBUF MESSAGE ON DISK, AND NOTHING ELSE.
//!
//! A delta record carries, beside its block items, one blob per touched key: either a CAPTURE (the
//! authoritative post-write value of the per-key maps a block cannot reconstruct -- the feature and
//! context series, the deadline, the control-state maps, the context node) or a CARRY (the container
//! elements a write touched, with the address each one points at). The record already held them as
//! `serde_json::Value`s inside its positional msgpack row, so a stored blob was a self-describing
//! document: every field spelled by NAME, every address an object of five named slots, every
//! distinct-count sketch a list of 4,096 numbers.
//!
//! This module is the one place those blobs become bytes. The in-memory shape is unchanged -- the
//! capture and the fold still build and read a `Value`, so no reader of a blob moves -- and what the
//! record STORES is a protobuf message whose fields are numbered. The record's row holds that message
//! in a binary field, and a record written before this change holds a map there instead, which the
//! row's reader refuses by name rather than reading as something else.
//!
//! # THE SCHEMA, AND WHY THE NUMBERS ARE WHAT THEY ARE
//!
//! ```text
//! message KeyState {
//!   string            key                     = 1;   // required: a blob with no key names nothing
//!   optional Series   features                = 2;   // a PRESENT field restores, an ABSENT one removes
//!   optional uint64   expires_at_ms           = 3;
//!   optional Changes  control_state_changes   = 4;
//!   optional Selection control_state_selection = 5;
//!   optional Address  context_nodes           = 6;
//!   optional Series   context_events          = 7;
//!   optional Series   context_indexes         = 8;
//!   optional Series   context_audits          = 9;
//!   optional Series   context_children        = 10;
//!   optional Series   context_summaries       = 11;
//!   optional Series   context_compressions    = 12;
//!   optional Series   context_entities        = 13;
//!   // 14..=19 are unassigned.
//!   repeated SetElement  set_elements         = 20;  // the carry
//!   repeated ZSetElement zset_elements        = 21;
//!   repeated ListElement list_elements        = 22;
//!   repeated HashField   hash_fields          = 23;
//! }
//! ```
//!
//! `optional` on a message field is the whole point of the capture: an ABSENT map field means "this
//! key had none" and the fold REMOVES the entry, while a present-but-empty one INSERTS an empty
//! series, and those are different answers. Protobuf spells both -- no tag, and a tag with a zero
//! length -- so the distinction the `Value` carried as "key missing" survives. The carry arrays are
//! `repeated`, where empty and absent are the same, and that is correct: a carry is built from at
//! least one touched element and a blob naming none is not a blob.
//!
//! An ADDRESS is the five facts an entry's own stored form holds -- slab, offset, length, the
//! optional block id, and whether a generation is present (its VALUE is derived from the block id,
//! which is why it is a bit and not a number) -- written as separate varints rather than as the one
//! merged word, because a small slab and a small offset are two short varints and the merged word is
//! a nine-byte one.
//!
//! # WHAT A BAD BLOB DOES
//!
//! It is REFUSED. `decode_blob` walks the top-level fields before decoding anything and refuses a
//! tag this schema does not number, a wire type it does not use, a missing key, and a payload that
//! does not parse; it never reads a document as a message that happens to have the same bytes. The
//! reader of the row turns that into a refusal of the record, and the fold turns THAT into the load's
//! refusal (`index_log_delta_corruption`) -- loud, where the document form let a field it no longer
//! understood be dropped on the floor.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use serde_json::{Map, Value};

use super::hll::{DistinctBucket, Hll};
use super::state::ControlStateSelectionValue;
use crate::block_store::ElementEntry;
use crate::types::ControlStateSelectionType;

/// The twelve per-key maps a CAPTURE names, in the order of the schema's tags 2..=13.
pub(crate) const CAPTURE_FIELDS: [&str; 12] = [
    "features",
    "expires_at_ms",
    "control_state_changes",
    "control_state_selection",
    "context_nodes",
    "context_events",
    "context_indexes",
    "context_audits",
    "context_children",
    "context_summaries",
    "context_compressions",
    "context_entities",
];

#[derive(Clone, PartialEq, Message)]
struct ProtoAddress {
    #[prost(uint64, tag = "1")]
    slab: u64,
    #[prost(uint64, tag = "2")]
    offset: u64,
    #[prost(uint64, tag = "3")]
    length: u64,
    #[prost(uint64, optional, tag = "4")]
    block_id: Option<u64>,
    #[prost(bool, tag = "5")]
    has_generation: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoPoint {
    #[prost(uint64, tag = "1")]
    at: u64,
    #[prost(message, optional, tag = "2")]
    address: Option<ProtoAddress>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoSeries {
    #[prost(message, repeated, tag = "1")]
    points: Vec<ProtoPoint>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoBucket {
    #[prost(uint64, tag = "1")]
    bucket_ms: u64,
    /// The exact distinct values, while the bucket is exact. Ignored when `sketch` is present.
    #[prost(bytes = "vec", repeated, tag = "2")]
    exact: Vec<Vec<u8>>,
    /// The sketch's registers, once the bucket was converted. PRESENT means sketched.
    #[prost(bytes = "vec", optional, tag = "3")]
    sketch: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoChanges {
    #[prost(message, repeated, tag = "1")]
    buckets: Vec<ProtoBucket>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoSelection {
    #[prost(uint64, tag = "1")]
    occur_time_ms: u64,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
    /// 0 is `First`, 1 is `Last`.
    #[prost(uint32, tag = "3")]
    selection_type: u32,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoSetElement {
    #[prost(bytes = "vec", tag = "1")]
    member: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    address: Option<ProtoAddress>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoZSetElement {
    #[prost(bytes = "vec", tag = "1")]
    member: Vec<u8>,
    #[prost(uint64, tag = "2")]
    score_bits: u64,
    #[prost(message, optional, tag = "3")]
    address: Option<ProtoAddress>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoListElement {
    #[prost(sint64, tag = "1")]
    sequence: i64,
    #[prost(message, optional, tag = "2")]
    address: Option<ProtoAddress>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoHashField {
    #[prost(string, tag = "1")]
    field: String,
    #[prost(message, optional, tag = "2")]
    address: Option<ProtoAddress>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoKeyState {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    features: Option<ProtoSeries>,
    #[prost(uint64, optional, tag = "3")]
    expires_at_ms: Option<u64>,
    #[prost(message, optional, tag = "4")]
    control_state_changes: Option<ProtoChanges>,
    #[prost(message, optional, tag = "5")]
    control_state_selection: Option<ProtoSelection>,
    #[prost(message, optional, tag = "6")]
    context_nodes: Option<ProtoAddress>,
    #[prost(message, optional, tag = "7")]
    context_events: Option<ProtoSeries>,
    #[prost(message, optional, tag = "8")]
    context_indexes: Option<ProtoSeries>,
    #[prost(message, optional, tag = "9")]
    context_audits: Option<ProtoSeries>,
    #[prost(message, optional, tag = "10")]
    context_children: Option<ProtoSeries>,
    #[prost(message, optional, tag = "11")]
    context_summaries: Option<ProtoSeries>,
    #[prost(message, optional, tag = "12")]
    context_compressions: Option<ProtoSeries>,
    #[prost(message, optional, tag = "13")]
    context_entities: Option<ProtoSeries>,
    #[prost(message, repeated, tag = "20")]
    set_elements: Vec<ProtoSetElement>,
    #[prost(message, repeated, tag = "21")]
    zset_elements: Vec<ProtoZSetElement>,
    #[prost(message, repeated, tag = "22")]
    list_elements: Vec<ProtoListElement>,
    #[prost(message, repeated, tag = "23")]
    hash_fields: Vec<ProtoHashField>,
}

/// The top-level tags the schema numbers, with the wire type each is written with.
const KNOWN_TAGS: [(u64, u8); 17] = [
    (1, 2),
    (2, 2),
    (3, 0),
    (4, 2),
    (5, 2),
    (6, 2),
    (7, 2),
    (8, 2),
    (9, 2),
    (10, 2),
    (11, 2),
    (12, 2),
    (13, 2),
    (20, 2),
    (21, 2),
    (22, 2),
    (23, 2),
];

// ------------------------------------------------------------------------------------------
// addresses
// ------------------------------------------------------------------------------------------

fn address_to_proto(entry: &ElementEntry) -> ProtoAddress {
    ProtoAddress {
        slab: entry.block_slab_id(),
        offset: entry.offset(),
        length: entry.length(),
        block_id: entry.block_id(),
        has_generation: entry.generation().is_some(),
    }
}

fn address_from_proto(address: &Option<ProtoAddress>) -> Result<ElementEntry, String> {
    let address = address
        .as_ref()
        .ok_or_else(|| "a key-state element carries no address".to_string())?;
    ElementEntry::try_from_wire_parts(
        address.slab,
        address.offset,
        address.length,
        address.block_id,
        address.has_generation,
    )
    .map_err(|out_of_range| out_of_range.to_string())
}

// ------------------------------------------------------------------------------------------
// value <-> typed, one field at a time
// ------------------------------------------------------------------------------------------

fn typed<T: serde::de::DeserializeOwned>(name: &str, value: &Value) -> Result<T, String> {
    serde_json::from_value::<T>(value.clone())
        .map_err(|error| format!("key-state field `{name}` is not the type it is stored as: {error}"))
}

fn series_to_proto(series: &BTreeMap<u64, ElementEntry>) -> ProtoSeries {
    ProtoSeries {
        points: series
            .iter()
            .map(|(at, entry)| ProtoPoint {
                at: *at,
                address: Some(address_to_proto(entry)),
            })
            .collect(),
    }
}

fn series_from_proto(series: &ProtoSeries) -> Result<BTreeMap<u64, ElementEntry>, String> {
    let mut out = BTreeMap::new();
    for point in &series.points {
        out.insert(point.at, address_from_proto(&point.address)?);
    }
    Ok(out)
}

fn series_field(name: &str, value: Option<&Value>) -> Result<Option<ProtoSeries>, String> {
    match value {
        Some(value) if !value.is_null() => Ok(Some(series_to_proto(&typed::<
            BTreeMap<u64, ElementEntry>,
        >(name, value)?))),
        _ => Ok(None),
    }
}

fn changes_to_proto(changes: &BTreeMap<u64, DistinctBucket>) -> ProtoChanges {
    ProtoChanges {
        buckets: changes
            .iter()
            .map(|(bucket_ms, bucket)| match bucket {
                DistinctBucket::Exact(values) => ProtoBucket {
                    bucket_ms: *bucket_ms,
                    exact: values.iter().cloned().collect(),
                    sketch: None,
                },
                DistinctBucket::Sketch(sketch) => ProtoBucket {
                    bucket_ms: *bucket_ms,
                    exact: Vec::new(),
                    sketch: Some(sketch.registers().to_vec()),
                },
            })
            .collect(),
    }
}

fn changes_from_proto(changes: &ProtoChanges) -> Result<BTreeMap<u64, DistinctBucket>, String> {
    let mut out = BTreeMap::new();
    for bucket in &changes.buckets {
        let value = match &bucket.sketch {
            Some(registers) => DistinctBucket::Sketch(
                Hll::from_registers(registers.clone())
                    .ok_or_else(|| "a distinct-count sketch has the wrong register count".to_string())?,
            ),
            None => DistinctBucket::Exact(bucket.exact.iter().cloned().collect::<BTreeSet<_>>()),
        };
        out.insert(bucket.bucket_ms, value);
    }
    Ok(out)
}

fn selection_to_proto(selection: &ControlStateSelectionValue) -> ProtoSelection {
    ProtoSelection {
        occur_time_ms: selection.occur_time_ms,
        value: selection.value.clone(),
        selection_type: match selection.selection_type {
            ControlStateSelectionType::First => 0,
            ControlStateSelectionType::Last => 1,
        },
    }
}

fn selection_from_proto(selection: &ProtoSelection) -> Result<ControlStateSelectionValue, String> {
    Ok(ControlStateSelectionValue {
        occur_time_ms: selection.occur_time_ms,
        value: selection.value.clone(),
        selection_type: match selection.selection_type {
            0 => ControlStateSelectionType::First,
            1 => ControlStateSelectionType::Last,
            other => return Err(format!("unknown selection type {other}")),
        },
    })
}

fn put_series(out: &mut Map<String, Value>, name: &str, series: &Option<ProtoSeries>) -> Result<(), String> {
    if let Some(series) = series {
        let map = series_from_proto(series)?;
        out.insert(
            name.to_string(),
            serde_json::to_value(&map).map_err(|error| error.to_string())?,
        );
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------
// the blob
// ------------------------------------------------------------------------------------------

fn carry_array<T: serde::Serialize>(items: Vec<T>) -> Result<Value, String> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(serde_json::to_value(item).map_err(|error| error.to_string())?);
    }
    Ok(Value::Array(out))
}

/// One blob, as the protobuf bytes a record stores.
///
/// REFUSES a blob that is not one this schema describes: not an object, no string `key`, a field the
/// schema does not number, or a field whose value is not the type its map holds. The writers
/// (`capture_key_states`, `capture_container_element_states`) produce exactly the sixteen names, so
/// this refusing is a writer bug surfacing, not a case to tolerate.
pub(crate) fn encode_blob(blob: &Value) -> Result<Vec<u8>, String> {
    let object = blob
        .as_object()
        .ok_or_else(|| "a key-state blob is not an object".to_string())?;
    let key = object
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| "a key-state blob names no key".to_string())?;
    for name in object.keys() {
        let known = name == "key"
            || CAPTURE_FIELDS.contains(&name.as_str())
            || super::CARRIED_CONTAINER_FIELDS.contains(&name.as_str());
        if !known {
            return Err(format!("a key-state blob carries a field this schema does not number: `{name}`"));
        }
    }
    let get = |name: &str| object.get(name).filter(|value| !value.is_null());
    let mut wire = ProtoKeyState {
        key: key.to_string(),
        features: series_field("features", get("features"))?,
        context_events: series_field("context_events", get("context_events"))?,
        context_indexes: series_field("context_indexes", get("context_indexes"))?,
        context_audits: series_field("context_audits", get("context_audits"))?,
        context_children: series_field("context_children", get("context_children"))?,
        context_summaries: series_field("context_summaries", get("context_summaries"))?,
        context_compressions: series_field("context_compressions", get("context_compressions"))?,
        context_entities: series_field("context_entities", get("context_entities"))?,
        ..ProtoKeyState::default()
    };
    if let Some(value) = get("expires_at_ms") {
        wire.expires_at_ms = Some(typed::<u64>("expires_at_ms", value)?);
    }
    if let Some(value) = get("control_state_changes") {
        wire.control_state_changes = Some(changes_to_proto(&typed::<BTreeMap<u64, DistinctBucket>>(
            "control_state_changes",
            value,
        )?));
    }
    if let Some(value) = get("control_state_selection") {
        wire.control_state_selection = Some(selection_to_proto(&typed::<ControlStateSelectionValue>(
            "control_state_selection",
            value,
        )?));
    }
    if let Some(value) = get("context_nodes") {
        wire.context_nodes = Some(address_to_proto(&typed::<ElementEntry>("context_nodes", value)?));
    }
    let carried = super::CARRIED_CONTAINER_FIELDS;
    if let Some(value) = get(carried[0]) {
        for (member, entry) in typed::<Vec<(Vec<u8>, ElementEntry)>>(carried[0], value)? {
            wire.set_elements.push(ProtoSetElement {
                member,
                address: Some(address_to_proto(&entry)),
            });
        }
    }
    if let Some(value) = get(carried[1]) {
        for (member, (score_bits, entry)) in
            typed::<Vec<(Vec<u8>, (u64, ElementEntry))>>(carried[1], value)?
        {
            wire.zset_elements.push(ProtoZSetElement {
                member,
                score_bits,
                address: Some(address_to_proto(&entry)),
            });
        }
    }
    if let Some(value) = get(carried[2]) {
        for (sequence, entry) in typed::<Vec<(i64, ElementEntry)>>(carried[2], value)? {
            wire.list_elements.push(ProtoListElement {
                sequence,
                address: Some(address_to_proto(&entry)),
            });
        }
    }
    if let Some(value) = get(carried[3]) {
        for (field, entry) in typed::<Vec<(String, ElementEntry)>>(carried[3], value)? {
            wire.hash_fields.push(ProtoHashField {
                field,
                address: Some(address_to_proto(&entry)),
            });
        }
    }
    Ok(wire.encode_to_vec())
}

/// Walk the top-level fields of a message and refuse any this schema does not number, or number
/// with another wire type. Run BEFORE decoding, so a document (JSON text, or a msgpack map) is
/// refused by its first byte rather than being read as a message that happens to parse.
fn refuse_unknown_fields(bytes: &[u8]) -> Result<(), String> {
    fn varint(bytes: &[u8], at: &mut usize) -> Result<u64, String> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *bytes
                .get(*at)
                .ok_or_else(|| "a key-state blob is truncated inside a varint".to_string())?;
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err("a key-state blob carries a varint wider than 64 bits".to_string())
    }
    let mut at = 0usize;
    while at < bytes.len() {
        let tag = varint(bytes, &mut at)?;
        let (field, wire_type) = (tag >> 3, (tag & 7) as u8);
        if !KNOWN_TAGS.contains(&(field, wire_type)) {
            return Err(format!(
                "a key-state blob carries field {field} with wire type {wire_type}, which this \
                 schema does not number: it is not a protobuf key-state message"
            ));
        }
        match wire_type {
            0 => {
                varint(bytes, &mut at)?;
            }
            2 => {
                let length = varint(bytes, &mut at)? as usize;
                at = at
                    .checked_add(length)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| "a key-state blob is truncated inside a field".to_string())?;
            }
            _ => return Err("a key-state blob uses a wire type this schema does not".to_string()),
        }
    }
    Ok(())
}

/// One blob, from the protobuf bytes a record stores. The inverse of [`encode_blob`], value for
/// value: what it returns is the `Value` the fold already reads. A refusal is counted.
pub(crate) fn decode_blob(bytes: &[u8]) -> Result<Value, String> {
    let decoded = decode_blob_uncounted(bytes);
    if decoded.is_err() {
        BLOBS_REFUSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    decoded
}

fn decode_blob_uncounted(bytes: &[u8]) -> Result<Value, String> {
    refuse_unknown_fields(bytes)?;
    let wire = ProtoKeyState::decode(bytes)
        .map_err(|error| format!("a key-state blob does not decode as a protobuf message: {error}"))?;
    if wire.key.is_empty() {
        return Err("a key-state blob names no key".to_string());
    }
    let mut out = Map::new();
    out.insert("key".to_string(), Value::String(wire.key.clone()));
    put_series(&mut out, "features", &wire.features)?;
    if let Some(expires) = wire.expires_at_ms {
        out.insert("expires_at_ms".to_string(), Value::from(expires));
    }
    if let Some(changes) = &wire.control_state_changes {
        out.insert(
            "control_state_changes".to_string(),
            serde_json::to_value(changes_from_proto(changes)?).map_err(|error| error.to_string())?,
        );
    }
    if let Some(selection) = &wire.control_state_selection {
        out.insert(
            "control_state_selection".to_string(),
            serde_json::to_value(selection_from_proto(selection)?).map_err(|error| error.to_string())?,
        );
    }
    if let Some(address) = &wire.context_nodes {
        out.insert(
            "context_nodes".to_string(),
            serde_json::to_value(address_from_proto(&Some(address.clone()))?)
                .map_err(|error| error.to_string())?,
        );
    }
    put_series(&mut out, "context_events", &wire.context_events)?;
    put_series(&mut out, "context_indexes", &wire.context_indexes)?;
    put_series(&mut out, "context_audits", &wire.context_audits)?;
    put_series(&mut out, "context_children", &wire.context_children)?;
    put_series(&mut out, "context_summaries", &wire.context_summaries)?;
    put_series(&mut out, "context_compressions", &wire.context_compressions)?;
    put_series(&mut out, "context_entities", &wire.context_entities)?;
    let carried = super::CARRIED_CONTAINER_FIELDS;
    if !wire.set_elements.is_empty() {
        let mut items = Vec::new();
        for element in &wire.set_elements {
            items.push((element.member.clone(), address_from_proto(&element.address)?));
        }
        out.insert(carried[0].to_string(), carry_array(items)?);
    }
    if !wire.zset_elements.is_empty() {
        let mut items = Vec::new();
        for element in &wire.zset_elements {
            items.push((
                element.member.clone(),
                (element.score_bits, address_from_proto(&element.address)?),
            ));
        }
        out.insert(carried[1].to_string(), carry_array(items)?);
    }
    if !wire.list_elements.is_empty() {
        let mut items = Vec::new();
        for element in &wire.list_elements {
            items.push((element.sequence, address_from_proto(&element.address)?));
        }
        out.insert(carried[2].to_string(), carry_array(items)?);
    }
    if !wire.hash_fields.is_empty() {
        let mut items = Vec::new();
        for element in &wire.hash_fields {
            items.push((element.field.clone(), address_from_proto(&element.address)?));
        }
        out.insert(carried[3].to_string(), carry_array(items)?);
    }
    Ok(Value::Object(out))
}

/// The serde adapter a record's `key_states` field is declared with: each blob is one binary field
/// holding one protobuf message.
///
/// The row is positional msgpack, so the element type is part of the format. A record written before
/// this change holds a MAP in each slot; the visitor below has no map arm, so such a record is
/// refused with the type it found rather than being read.
pub(crate) mod blobs {
    use serde::de::{self, Deserializer, SeqAccess, Visitor};
    use serde::ser::{Error as _, SerializeSeq, Serializer};
    use serde_json::Value;

    struct Encoded<'a>(&'a [u8]);

    impl serde::Serialize for Encoded<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(self.0)
        }
    }

    struct Blob(Value);

    struct BlobVisitor;

    impl<'de> Visitor<'de> for BlobVisitor {
        type Value = Blob;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a protobuf key-state message in a binary field")
        }

        fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Blob, E> {
            super::decode_blob_uncounted(bytes).map(Blob).map_err(E::custom)
        }

        fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Blob, E> {
            self.visit_bytes(&bytes)
        }

        /// A format with no byte type (JSON) spells bytes as a list of numbers. That is still a
        /// message in a binary field, and it is the only sequence accepted: a MAP, which is what a
        /// record written before this change holds, has no arm here and is refused.
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Blob, A::Error> {
            let mut bytes = Vec::new();
            while let Some(byte) = seq.next_element::<u8>()? {
                bytes.push(byte);
            }
            super::decode_blob_uncounted(&bytes).map(Blob).map_err(de::Error::custom)
        }
    }

    impl<'de> serde::Deserialize<'de> for Blob {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            deserializer.deserialize_bytes(BlobVisitor)
        }
    }

    struct BlobsVisitor;

    impl<'de> Visitor<'de> for BlobsVisitor {
        type Value = Vec<Value>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a list of protobuf key-state messages")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<Value>, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(1 << 16));
            while let Some(Blob(value)) = seq.next_element::<Blob>()? {
                out.push(value);
            }
            Ok(out)
        }
    }

    pub fn serialize<S: Serializer>(blobs: &[Value], serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(blobs.len()))?;
        for blob in blobs {
            let bytes = super::encode_blob(blob).map_err(S::Error::custom)?;
            seq.serialize_element(&Encoded(&bytes))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Value>, D::Error> {
        let result = deserializer.deserialize_seq(BlobsVisitor);
        if result.is_err() {
            // COUNTED ONCE PER REFUSED ROW, so a record refused for its blobs is told apart from one
            // that was torn or never written: the shape a record written before this change has (a
            // map where a message belongs) and a message that does not parse both land here.
            super::BLOBS_REFUSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }
}

static BLOBS_REFUSED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many records the reader refused for their key-state blobs since the last reset.
pub fn key_state_blobs_refused() -> u64 {
    BLOBS_REFUSED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own reads rather than the suite before it.
pub fn reset_key_state_blobs_refused() {
    BLOBS_REFUSED.store(0, std::sync::atomic::Ordering::Relaxed);
}
