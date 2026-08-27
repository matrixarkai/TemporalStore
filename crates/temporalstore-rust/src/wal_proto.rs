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
/// Default OFF while it proves out. Reading never consults this -- a payload is decoded by what
/// its first byte says it is -- so turning it on and off again leaves a log that still reads end
/// to end.
pub(crate) fn binary_records_enabled() -> bool {
    std::env::var("TS_WAL_BINARY_RECORDS")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn address_to_proto(address: &BlockAddress) -> v1::WalBlockAddress {
    v1::WalBlockAddress {
        block_slab_id: address.page_slab_id,
        offset: address.offset,
        length: address.length,
        block_id: address.page_id,
        object_id: address.object_id,
        // Deliberately dropped, exactly as the text encoding drops it: the item carries the
        // routing bucket, and `resolved_address` puts it back. Writing it here made the two
        // encodings of one record decode differently, which is a divergence whether or not
        // anything currently reads the difference.
        routing_bucket: None,
        generation: address.generation,
        band_id: address.band_id,
        checksum: address.sha256.clone(),
    }
}

fn address_from_proto(address: v1::WalBlockAddress) -> BlockAddress {
    BlockAddress {
        page_slab_id: address.block_slab_id,
        offset: address.offset,
        length: address.length,
        page_id: address.block_id,
        object_id: address.object_id,
        routing_bucket: address.routing_bucket,
        generation: address.generation,
        band_id: address.band_id,
        sha256: address.checksum,
    }
}

fn item_to_proto(item: &WalOutcomeItem) -> v1::EngineWalItem {
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
        component: item.component.clone(),
        block: item.address.as_ref().map(address_to_proto),
        value: item.value.clone(),
        // The engine names more kinds than the enum does, and the name is what the apply path
        // dispatches on, so it is carried literally rather than squeezed through a lossy mapping.
        kind_name: Some(item.kind.clone()),
        routing_bucket: Some(item.routing_bucket),
    }
}

fn item_from_proto(item: v1::EngineWalItem) -> WalOutcomeItem {
    WalOutcomeItem {
        kind: item.kind_name.unwrap_or_default(),
        object_key: item.object_key.unwrap_or_default(),
        component: item.component,
        object_id: item.object_id.unwrap_or_default(),
        routing_bucket: item.routing_bucket.unwrap_or_default(),
        address: item.block.map(address_from_proto),
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
            routing_bucket: None,
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
        command: Some(v1::WalCommand {
            kind: Some(
                crate::raft::wal_proto::command_to_proto(&record.command)
                    .map_err(|err| err.to_string())?,
            ),
        }),
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
    let mut encoded = Vec::with_capacity(message.encoded_len());
    message.encode(&mut encoded).map_err(|err| err.to_string())?;
    let mut out = Vec::with_capacity(encoded.len() + 8);
    out.push(BINARY_PAYLOAD_MARKER);
    out.extend_from_slice(&escape_newlines(&encoded));
    Ok(out)
}

/// Decode a payload this module wrote. The caller has already checked the marker byte.
pub(crate) fn decode(payload: &[u8]) -> Result<WriteAheadLogRecord, String> {
    let unescaped = unescape_newlines(&payload[1..])?;
    let message =
        v1::EngineWalRecord::decode(unescaped.as_slice()).map_err(|err| err.to_string())?;
    let command = message
        .command
        .and_then(|command| command.kind)
        .ok_or_else(|| String::from("engine wal record carried no command"))
        .and_then(|kind| {
            crate::raft::wal_proto::command_from_proto(kind).map_err(|err| err.to_string())
        })?;
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
