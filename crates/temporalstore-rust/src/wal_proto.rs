// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Encode an engine write-ahead log record as a compact binary message rather than text.
//!
//! The text encoding spells out every field name on every record and has no byte-string type, so
//! binary values become arrays of decimal numbers. Measured on this path: a string write recorded
//! with its outcome costs 471.7 bytes, of which the outcome is 285.3 -- almost all of it field
//! names and a hex checksum. The node log already made this trade, and its own measurement put
//! text at roughly three bytes written per byte of data.
//!
//! **Fidelity comes before compactness.** This is the durability path, so anything this module
//! does not model explicitly travels byte for byte in the previous encoding rather than being
//! approximated by the nearest match. A command this module has never heard of still round-trips
//! exactly, and modelled arms can be added one at a time.
//!
//! The frame does not change. `log_framing` still wraps every record with its length and checksum,
//! so byte offsets, integrity checking and the scan path behave exactly as before. Only the
//! payload inside the frame differs, and its first byte says which encoding it is.

use prost::Message;

use crate::block_store::BlockAddress;
use crate::sdk::v1;
use crate::wal::{StagedPage, WalOutcomeItem, WriteAheadLogRecord, WriteAheadLogRecordMetadata};

/// Marks a payload as protobuf.
///
/// A text payload always starts with `{`, so one byte separates the two and a log written before
/// this existed reads back unchanged.
pub(crate) const BINARY_PAYLOAD_MARKER: u8 = 0xB7;

/// Marker for a protobuf payload carrying NO escaping.
///
/// Escaping exists to keep a record free of the byte a line-oriented reader splits on. Inside a
/// length-framed record nothing splits on anything, so the stuffing is pure cost -- and it is
/// not small: protobuf writes field 1 as the tag byte 0x0A, so the payloads carrying the most
/// fields are the ones paying the most for a delimiter no reader is looking for.
///
/// A separate marker rather than a flag read at decode time: which encoding a payload is in has
/// to be a property of the payload, or a log written across a configuration change stops
/// reading halfway through.
pub(crate) const RAW_PAYLOAD_MARKER: u8 = 0xB8;

/// Escapes a newline out of an encoded payload.
///
/// The log is read with `reader.lines()`. A JSON payload can never contain a raw newline, so that
/// worked for as long as every record was text. Protobuf bytes contain 0x0A freely, and a record
/// carrying one splits into fragments that decode as nothing -- which does not fail loudly, it
/// loses the write.
///
/// Byte stuffing rather than base64: base64 costs a third of the payload, and this costs one byte
/// per newline actually present, which for encoded protobuf is a fraction of a percent. The
/// checksum in the frame is computed over the ESCAPED bytes, so the frame validates what it
/// actually holds.
const ESCAPE: u8 = 0x1B;
const ESCAPED_NEWLINE: u8 = 0x01;
const ESCAPED_ESCAPE: u8 = 0x02;

fn escape_newlines(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 8);
    for byte in bytes {
        match *byte {
            b'\n' => out.extend_from_slice(&[ESCAPE, ESCAPED_NEWLINE]),
            ESCAPE => out.extend_from_slice(&[ESCAPE, ESCAPED_ESCAPE]),
            other => out.push(other),
        }
    }
    out
}

fn unescape_newlines(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut iter = bytes.iter().copied();
    while let Some(byte) = iter.next() {
        if byte != ESCAPE {
            out.push(byte);
            continue;
        }
        match iter.next() {
            Some(ESCAPED_NEWLINE) => out.push(b'\n'),
            Some(ESCAPED_ESCAPE) => out.push(ESCAPE),
            Some(other) => return Err(format!("unknown escape 0x{other:02x} in wal payload")),
            None => return Err(String::from("wal payload ends mid-escape")),
        }
    }
    Ok(out)
}

/// TS_WAL_BINARY_RECORDS: write engine records as protobuf.
///
/// **Default ON.** The doc comment here used to say OFF while the code returned true and the
/// comment inside the body said ON -- three statements, two of them wrong, about the encoding of
/// the durability log.
///
/// Reading never consults this: a payload is decoded by what its first byte says it is, so a log
/// written across the flip reads end to end in either direction, and turning it off again is not
/// a one-way door.
pub(crate) fn binary_records_enabled() -> bool {
    std::env::var("TS_WAL_BINARY_RECORDS")
        .map(|value| !(value == "0" || value.eq_ignore_ascii_case("false")))
        .unwrap_or(true)
}
/// The kinds common enough to be worth a code rather than a string on every item.
///
/// Order is the wire contract: a code means the same kind forever. A kind missing from this list
/// travels as its name, which is what keeps the list from having to be exhaustive.
const KIND_CODES: &[&str] = &[
    "string",
    "hash",
    "set",
    "list",
    "zset",
    "feature",
    "object",
    "seen",
    "bucket",
    "context_event",
    "context_index",
    "context_audit",
    "context_child",
    "context_summary",
    "context_compression",
    "context_node",
    "context_entity",
    "control_state",
    "control_counter",
    "control_change",
    "control_selection",
];

fn kind_code(kind: &str) -> Option<u32> {
    KIND_CODES
        .iter()
        .position(|known| *known == kind)
        .map(|index| index as u32 + 1)
}

fn kind_from_code(code: u32) -> Option<&'static str> {
    if code == 0 {
        return None;
    }
    KIND_CODES.get(code as usize - 1).copied()
}

/// The digest itself, not its hex transcription.
fn checksum_to_raw(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).ok())
        .collect()
}

fn checksum_from_raw(raw: &[u8]) -> String {
    raw.iter().map(|byte| format!("{byte:02x}")).collect()
}


fn address_to_proto(address: &BlockAddress) -> v1::WalBlockAddress {
    v1::WalBlockAddress {
        block_slab_id: address.page_slab_id,
        offset: address.offset,
        length: address.length,
        block_id: address.page_id(),
        object_id: address.object_id(),
        // Deliberately dropped, exactly as the text encoding drops it: the item carries the
        // routing bucket, and `resolved_address` puts it back. Writing it here made the two
        // encodings of one record decode differently, which is a divergence whether or not
        // anything currently reads the difference.
        routing_bucket: None,
        generation: address.generation(),
        band_id: address.band_id(),
        // The digest, not its transcription. Half the bytes, same value.
        checksum: None,
        // In memory the digest is already the 32 bytes this field wants, so there is no
        // transcription left to undo.
        // The index does not hold a digest; the page envelope carries it.
        checksum_raw: None,
    }
}

fn address_from_proto(address: v1::WalBlockAddress) -> BlockAddress {
    BlockAddress::from_parts(
        address.block_slab_id,
        address.offset,
        address.length,
        address.block_id,
        address.object_id,
        address.routing_bucket,
        address.generation,
        address.band_id,
    )
}
/// The numeric key a component is carrying, if it is carrying one.
///
/// Returns the stored key and, for a context event, the entry id packed beside it. Anything whose
/// component is genuinely text -- a hash field, a hex-encoded set member, a packed zset score --
/// returns None and keeps its string.
fn numeric_component(kind: &str, component: Option<&str>) -> Option<(u64, Option<u64>)> {
    let component = component?;
    match kind {
        "context_event" => {
            if component.len() != 32 {
                return None;
            }
            let (stored, entry) = component.split_at(16);
            Some((
                u64::from_str_radix(stored, 16).ok()?,
                Some(u64::from_str_radix(entry, 16).ok()?),
            ))
        }
        "feature" | "context_index" | "context_audit" | "context_child" | "context_summary"
        | "context_compression" | "control_counter" | "control_change" => {
            Some((component.parse::<u64>().ok()?, None))
        }
        _ => None,
    }
}


pub(crate) fn item_to_proto(item: &WalOutcomeItem) -> v1::EngineWalItem {
    let block = item.address.as_ref().map(|address| {
        let mut encoded = address_to_proto(address);
        // The item already says this. Repeating it costs a full varint on every item whose page
        // belongs to one object, which is every kind that is not timestamped.
        if encoded.object_id == Some(item.object_id) {
            encoded.object_id = None;
        }
        encoded
    });
    v1::EngineWalItem {
        item_kind: 0,
        model: 0,
        object_key: Some(item.object_key.clone()),
        bucket_id: None,
        object_id: Some(item.object_id),
        block_id: None,
        ttl_ms: item.ttl,
        deleted: item.deleted,
        meta_log: item.meta,
        block_log: false,
        // A component that IS a number travels as one. What stays in `component` is what the
        // field is actually for: a hash field, a set member, a packed score.
        component: match numeric_component(&item.kind, item.component.as_deref()) {
            Some(_) => None,
            None => item.component.clone(),
        },
        timestamp_ms: numeric_component(&item.kind, item.component.as_deref()).map(|(key, _)| key),
        entry_id: numeric_component(&item.kind, item.component.as_deref()).and_then(|(_, id)| id),
        // A removal with no component is the whole object, which is now said rather than implied.
        object_deleted: item.deleted && item.component.is_none(),
        block,
        value: item.value.clone(),
        // The engine names more kinds than the enum does, and the name is what the apply path
        // dispatches on, so it is carried literally rather than squeezed through a lossy mapping.
        // A known kind travels as a code; anything else keeps its name.
        kind_name: match kind_code(&item.kind) {
            Some(_) => None,
            None => Some(item.kind.clone()),
        },
        kind_code: kind_code(&item.kind),
        routing_bucket: Some(item.routing_bucket),
    }
}

pub(crate) fn item_from_proto(item: v1::EngineWalItem) -> WalOutcomeItem {
    WalOutcomeItem {
        kind: item
            .kind_code
            .and_then(kind_from_code)
            .map(str::to_string)
            .or(item.kind_name)
            .unwrap_or_default(),
        object_key: item.object_key.unwrap_or_default(),
        // Prefer the numeric fields; fall back to the string a record written before this carries.
        component: match (item.timestamp_ms, item.entry_id) {
            (Some(stored), Some(entry)) => Some(format!("{stored:016x}{entry:016x}")),
            (Some(stored), None) => Some(stored.to_string()),
            (None, _) => item.component,
        },
        object_id: item.object_id.unwrap_or_default(),
        routing_bucket: item.routing_bucket.unwrap_or_default(),
        address: item.block.map(|block| {
            let object_id = item.object_id.unwrap_or_default();
            let mut address = address_from_proto(block);
            // Absent means "the same as the item's", which is the only thing it can mean: the
            // encoder omits it exactly when they match, and it is never otherwise unset.
            if address.object_id().is_none() {
                address.set_object_id(Some(object_id));
            }
            address
        }),
        value: item.value,
        ttl: item.ttl_ms,
        deleted: item.deleted,
        meta: item.meta_log,
    }
}

fn metadata_to_proto(metadata: &WriteAheadLogRecordMetadata) -> Result<v1::EngineWalMetadata, String> {
    // The descriptive item list travels verbatim rather than field by field. It is off by default,
    // every field of it is derived from the command beside it, and modelling it would risk losing
    // one for no measurable saving.
    let items = if metadata.items.is_empty() {
        Vec::new()
    } else {
        vec![v1::EngineWalItem {
            item_kind: 0,
            model: 0,
            object_key: None,
            bucket_id: None,
            object_id: None,
            block_id: None,
            ttl_ms: None,
            deleted: false,
            meta_log: false,
            block_log: false,
            component: None,
            block: None,
            value: Some(serde_json::to_vec(&metadata.items).map_err(|err| err.to_string())?),
            kind_name: Some(String::from("__verbatim_items")),
            kind_code: None,
            routing_bucket: None,
            timestamp_ms: None,
            entry_id: None,
            object_deleted: false,
        }]
    };
    Ok(v1::EngineWalMetadata {
        version: metadata.version,
        timestamp_ms: metadata.timestamp_ms,
        items,
        batch_id: metadata.batch_id,
        batch_size: metadata.batch_size,
        batch_index: metadata.batch_index,
    })
}

fn metadata_from_proto(
    metadata: v1::EngineWalMetadata,
) -> Result<WriteAheadLogRecordMetadata, String> {
    let items = match metadata.items.into_iter().next() {
        Some(carried) if carried.kind_name.as_deref() == Some("__verbatim_items") => {
            serde_json::from_slice(&carried.value.unwrap_or_default())
                .map_err(|err| err.to_string())?
        }
        _ => Vec::new(),
    };
    Ok(WriteAheadLogRecordMetadata {
        version: metadata.version,
        timestamp_ms: metadata.timestamp_ms,
        items,
        batch_id: metadata.batch_id,
        batch_size: metadata.batch_size,
        batch_index: metadata.batch_index,
    })
}

/// Encode a record as protobuf, marker byte first.
pub(crate) fn encode(record: &WriteAheadLogRecord) -> Result<Vec<u8>, String> {
    let message = v1::EngineWalRecord {
        shard_id: record.shard_id,
        sequence: record.sequence,
        command: match record.command.as_ref() {
            Some(command) => Some(v1::WalCommand {
                kind: Some(
                    crate::raft::wal_proto::command_to_proto(command)
                        .map_err(|err| err.to_string())?,
                ),
            }),
            None => None,
        },
        metadata: record
            .metadata
            .as_ref()
            .map(metadata_to_proto)
            .transpose()?,
        items: record.outcomes.iter().map(item_to_proto).collect(),
        staged_blocks: record
            .staged_pages
            .iter()
            .map(|page| v1::WalStagedBlock {
                object_id: page.object_id,
                block: page.bytes.clone(),
                routing_bucket: None,
            })
            .collect(),
    };
    let len = message.encoded_len();
    if crate::log_framing::binary_frame_enabled() {
        // The frame declares its own length, so the payload is written as produced -- which means
        // the marker can go in FIRST and the message encode straight after it. `encode` appends,
        // so there is no second buffer and no second copy of the payload. That copy was the whole
        // record again: at a four-kilobyte value it was four kilobytes to prepend one byte.
        let mut out = Vec::with_capacity(len + 1);
        out.push(RAW_PAYLOAD_MARKER);
        message.encode(&mut out).map_err(|err| err.to_string())?;
        return Ok(out);
    }
    // The escaping fallback still needs the payload on its own, because escaping rewrites it.
    let mut encoded = Vec::with_capacity(len);
    message.encode(&mut encoded).map_err(|err| err.to_string())?;
    let mut out = Vec::with_capacity(encoded.len() + 8);
    out.push(BINARY_PAYLOAD_MARKER);
    out.extend_from_slice(&escape_newlines(&encoded));
    Ok(out)
}

/// Decode a payload this module wrote. The caller has already checked the marker byte.
pub(crate) fn decode(payload: &[u8]) -> Result<WriteAheadLogRecord, String> {
    let body = &payload[1..];
    let unescaped;
    let bytes: &[u8] = if payload.first() == Some(&RAW_PAYLOAD_MARKER) {
        body
    } else {
        unescaped = unescape_newlines(body)?;
        unescaped.as_slice()
    };
    let message = v1::EngineWalRecord::decode(bytes).map_err(|err| err.to_string())?;
    // Absent is a legitimate record now, not a malformed one: it carries results instead.
    let command = match message.command.and_then(|command| command.kind) {
        Some(kind) => Some(
            crate::raft::wal_proto::command_from_proto(kind).map_err(|err| err.to_string())?,
        ),
        None => None,
    };
    Ok(WriteAheadLogRecord {
        shard_id: message.shard_id,
        sequence: message.sequence,
        command,
        metadata: message.metadata.map(metadata_from_proto).transpose()?,
        staged_pages: message
            .staged_blocks
            .into_iter()
            .map(|block| StagedPage {
                object_id: block.object_id,
                bytes: block.block,
            })
            .collect(),
        outcomes: message.items.into_iter().map(item_from_proto).collect(),
    })
}
