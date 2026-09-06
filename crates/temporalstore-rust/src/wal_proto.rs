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

/// Marker for a protobuf payload that is zstd-compressed and carries no escaping.
///
/// A record's payload is the largest thing this log writes and the most repetitive: the same
/// field names, scope keys and policy blocks over and over. Measured on one live segment of the
/// hook store -- 151 records, 13.5 KB each -- zstd at level 3 takes 2,038,147 bytes to 236,714,
/// which is 8.61x, and projects to 617 MB off a 698 MB log.
///
/// Compressing the whole SEGMENT instead reaches 22.28x, and is not available here: a log id is a
/// byte position, page references point at those positions, and a block that has to be inflated
/// before any record inside it can be found does not have them. Per record keeps every record
/// independently addressable, which is the property the log is built on.
pub(crate) const COMPRESSED_RAW_PAYLOAD_MARKER: u8 = 0xB9;

/// The same, for a delimited frame, where the compressed bytes still have to be escaped.
///
/// Two markers rather than one plus a flag, for the reason the pair above gives: which encoding a
/// payload is in has to be a property of the payload, or a log written across a configuration
/// change stops reading halfway through.
pub(crate) const COMPRESSED_ESCAPED_PAYLOAD_MARKER: u8 = 0xBA;

/// Compression level. The same level the index already compresses at, so the log and the index
/// make the same trade rather than two unexplained ones.
const COMPRESSION_LEVEL: i32 = 3;

/// Below this many bytes a payload is written uncompressed.
///
/// Not a guess: the page store measured this exact question and found a 1-byte floor worse than
/// no compression at all -- the saving stopped while the median write rose, because a tiny
/// payload costs more to compress than it gives back. 256 is the floor that measurement chose.
const COMPRESSION_MIN_BYTES: usize = 256;

/// Whether new records are written compressed. DEFAULT OFF.
///
/// Reading never consults this. A payload says what encoding it is in, so a log written across a
/// change reads end to end and turning it off again is not a one-way door -- the same contract
/// `TS_WAL_BINARY_RECORDS` keeps.
/// **Default ON.** It was built off, which meant every deployment paid to store a log it had the
/// code to shrink. Compression is applied only where it pays twice over: a payload under
/// `COMPRESSION_MIN_BYTES` is left alone, and a payload whose compressed form is not actually
/// smaller is written raw under its own marker.
///
/// The variable now opts OUT, like `TS_WAL_BINARY_RECORDS` and `TS_WAL_BINARY_FRAME` beside it --
/// and for the same reason it is safe to flip either way: which encoding a payload is in is a
/// property of the payload, not of this flag.
pub(crate) fn compress_records_enabled() -> bool {
    !matches!(
        std::env::var("TS_WAL_COMPRESS_RECORDS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

/// Compress `encoded` if it is worth compressing, returning None when it is not.
///
/// None for a payload under the floor, and None when the compressed form is not actually smaller.
/// The second check is the page store's: compression that does not pay is written uncompressed
/// rather than trusted to pay on average.
fn compress_payload(encoded: &[u8]) -> Option<Vec<u8>> {
    if encoded.len() < COMPRESSION_MIN_BYTES {
        return None;
    }
    let compressed = zstd::stream::encode_all(encoded, COMPRESSION_LEVEL).ok()?;
    (compressed.len() < encoded.len()).then_some(compressed)
}

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
    // Spelled the way every other engine flag is spelled. This read used to accept only "0" and
    // "false", so "off" and "no" -- which turn any of its neighbours off -- left protobuf on
    // here. It also put the default somewhere no check could find it, which is why the portal
    // could not offer this setting: an offered knob has to show a default the source can be
    // asked for.
    !matches!(
        std::env::var("TS_WAL_BINARY_RECORDS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
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

/// The record split into the part written by hand and the part left to the generated encoder.
///
/// Fields one to three are written here; four to six are still a `prost` message. The split is
/// what lets the command borrow: only the command carries a payload worth not copying, and it is
/// the one field the generated encoder cannot be handed a borrow of.
struct RecordParts<'a> {
    command: Option<crate::raft::wal_proto::CommandEncoding<'a>>,
    /// Fields four to six only. proto3 omits a scalar holding its default and `command` is None,
    /// so encoding this writes the tail and nothing else -- which is why the bytes come out in
    /// tag order and identical to encoding the whole record at once.
    tail: v1::EngineWalRecord,
    len: usize,
}

fn record_parts(record: &WriteAheadLogRecord) -> Result<RecordParts<'_>, String> {
    let command = match record.command.as_ref() {
        Some(command) => Some(
            crate::raft::wal_proto::command_encoding(command).map_err(|err| err.to_string())?,
        ),
        None => None,
    };
    let tail = v1::EngineWalRecord {
        shard_id: 0,
        sequence: 0,
        command: None,
        metadata: record
            .metadata
            .as_ref()
            .map(metadata_to_proto)
            .transpose()?,
        items: record.outcomes.iter().map(item_to_proto).collect(),
        // Written by hand below, from the pages themselves. A staged page is a whole page, and
        // filling this field copied every one of them before the encoder copied them again.
        staged_blocks: Vec::new(),
    };
    let len = crate::raft::wal_proto::varint_field_len(1, record.shard_id)
        + crate::raft::wal_proto::varint_field_len(2, record.sequence)
        + command.as_ref().map_or(0, |command| command.encoded_len_at(3))
        + tail.encoded_len()
        + record
            .staged_pages
            .iter()
            .map(|page| crate::raft::wal_proto::len_delimited_len(6, staged_block_body_len(page)))
            .sum::<usize>();
    Ok(RecordParts {
        command,
        tail,
        len,
    })
}

impl RecordParts<'_> {
    fn put(&self, record: &WriteAheadLogRecord, out: &mut Vec<u8>) -> Result<(), String> {
        crate::raft::wal_proto::put_varint_field(1, record.shard_id, out);
        crate::raft::wal_proto::put_varint_field(2, record.sequence, out);
        if let Some(command) = self.command.as_ref() {
            command.put_at(3, out);
        }
        self.tail.encode(out).map_err(|err| err.to_string())?;
        for page in &record.staged_pages {
            crate::raft::wal_proto::put_staged_block(6, page, out);
        }
        Ok(())
    }
}

/// Bytes a staged page occupies as a `WalStagedBlock` body.
///
/// `routing_bucket` is never set on this path, and proto3 omits an absent optional, so it costs
/// nothing here and is not written.
fn staged_block_body_len(page: &crate::wal::StagedPage) -> usize {
    crate::raft::wal_proto::varint_field_len(1, page.object_id)
        + if page.bytes.is_empty() {
            0
        } else {
            crate::raft::wal_proto::len_delimited_len(2, page.bytes.len())
        }
}

/// A record measured but not yet written.
///
/// `encode` below builds the payload into its own buffer and the framing layer then copies it
/// into a second one, so a write carries the record twice. Handing the framing layer a length and
/// a writer instead lets it reserve once and have the payload land in the bytes that go to disk.
///
/// The length has to be exact, which is the whole reason this can exist: `RecordParts::len` is
/// asserted equal to the bytes written, for every arm, by
/// `the_borrowing_encoder_writes_the_same_bytes`.
pub(crate) struct PreparedRecord<'a> {
    parts: RecordParts<'a>,
}

impl PreparedRecord<'_> {
    /// Bytes the payload will occupy: the marker, then the record.
    pub(crate) fn payload_len(&self) -> usize {
        self.parts.len + 1
    }

    pub(crate) fn put(
        &self,
        record: &WriteAheadLogRecord,
        out: &mut Vec<u8>,
    ) -> Result<(), String> {
        out.push(RAW_PAYLOAD_MARKER);
        self.parts.put(record, out)
    }
}

/// Measure a record so it can be framed without an intermediate buffer.
pub(crate) fn prepare(record: &WriteAheadLogRecord) -> Result<PreparedRecord<'_>, String> {
    Ok(PreparedRecord {
        parts: record_parts(record)?,
    })
}

/// Encode a record as protobuf, marker byte first.
pub(crate) fn encode(record: &WriteAheadLogRecord) -> Result<Vec<u8>, String> {
    let parts = record_parts(record)?;
    if compress_records_enabled() {
        // Compression cannot use the borrowing writer above: that path reserves the frame from
        // `payload_len()` before the payload exists, and how long a compressed payload will be is
        // not knowable until it has been compressed. So this arm builds the payload first and the
        // frame around it, which is what the escaping arm below has always done.
        let mut encoded = Vec::with_capacity(parts.len);
        parts.put(record, &mut encoded)?;
        if let Some(compressed) = compress_payload(&encoded) {
            let escaping = !crate::log_framing::binary_frame_enabled();
            let mut out = Vec::with_capacity(compressed.len() + 8);
            out.push(if escaping {
                COMPRESSED_ESCAPED_PAYLOAD_MARKER
            } else {
                COMPRESSED_RAW_PAYLOAD_MARKER
            });
            if escaping {
                out.extend_from_slice(&escape_newlines(&compressed));
            } else {
                out.extend_from_slice(&compressed);
            }
            return Ok(out);
        }
        // Not worth compressing. Fall through and write it the way it would have been written
        // anyway, under its own marker -- a reader cannot tell that this record was considered.
    }
    if crate::log_framing::binary_frame_enabled() {
        // The frame declares its own length, so the payload is written as produced -- which means
        // the marker can go in FIRST and the message encode straight after it. `encode` appends,
        // so there is no second buffer and no second copy of the payload. That copy was the whole
        // record again: at a four-kilobyte value it was four kilobytes to prepend one byte.
        let mut out = Vec::with_capacity(parts.len + 1);
        out.push(RAW_PAYLOAD_MARKER);
        parts.put(record, &mut out)?;
        return Ok(out);
    }
    // The escaping fallback still needs the payload on its own, because escaping rewrites it.
    let mut encoded = Vec::with_capacity(parts.len);
    parts.put(record, &mut encoded)?;
    let mut out = Vec::with_capacity(encoded.len() + 8);
    out.push(BINARY_PAYLOAD_MARKER);
    out.extend_from_slice(&escape_newlines(&encoded));
    Ok(out)
}

/// Decode a payload this module wrote. The caller has already checked the marker byte.
pub(crate) fn decode(payload: &[u8]) -> Result<WriteAheadLogRecord, String> {
    let body = &payload[1..];
    let marker = payload.first().copied();
    let escaped = marker == Some(BINARY_PAYLOAD_MARKER)
        || marker == Some(COMPRESSED_ESCAPED_PAYLOAD_MARKER);
    let compressed = marker == Some(COMPRESSED_RAW_PAYLOAD_MARKER)
        || marker == Some(COMPRESSED_ESCAPED_PAYLOAD_MARKER);
    let unescaped;
    let framed: &[u8] = if escaped {
        unescaped = unescape_newlines(body)?;
        unescaped.as_slice()
    } else {
        body
    };
    let inflated;
    let bytes: &[u8] = if compressed {
        inflated = zstd::stream::decode_all(framed)
            .map_err(|err| format!("compressed wal payload did not inflate: {err}"))?;
        inflated.as_slice()
    } else {
        framed
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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;
    use crate::wal::{StagedPage, WriteAheadLogRecord, WriteAheadLogRecordMetadata};

    /// The encoder this replaced: build the whole message, owning every payload, then serialise.
    ///
    /// Kept here because it is the specification. The hand-written head is only allowed to exist
    /// if it produces exactly these bytes -- a record already on disk is read by the generated
    /// decoder, and a head that decoded correctly but differed byte for byte would still be a
    /// format change, silently, on every record written from here on.
    fn owned_bytes(record: &WriteAheadLogRecord) -> Vec<u8> {
        let message = v1::EngineWalRecord {
            shard_id: record.shard_id,
            sequence: record.sequence,
            command: record.command.as_ref().map(|command| v1::WalCommand {
                kind: Some(crate::raft::wal_proto::command_to_proto(command).unwrap()),
            }),
            metadata: record
                .metadata
                .as_ref()
                .map(metadata_to_proto)
                .transpose()
                .unwrap(),
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
        message.encode(&mut encoded).unwrap();
        encoded
    }

    /// A record big and repetitive enough to be worth compressing, which most real ones are.
    fn compressible_record() -> WriteAheadLogRecord {
        let mut record = record_with(Some(Command::StringSet {
            key: "compressible".to_string(),
            // Repetition is the point: a real record repeats field names, scope keys and policy
            // blocks, and a payload of one byte over and over would flatter the codec in a way
            // those do not. This alternates, so it is compressible without being trivial.
            value: (0..4096u32).map(|index| (index % 7) as u8).collect(),
        }));
        record.outcomes = vec![crate::wal::WalOutcomeItem {
            kind: "page".to_string(),
            object_key: "tenant/1/object/9".to_string(),
            component: Some("body".to_string()),
            object_id: 9,
            routing_bucket: 8539,
            address: None,
            value: Some(vec![3; 512]),
            ttl: Some(60_000),
            deleted: false,
            meta: false,
        }];
        record
    }

    /// Build the compressed payload by hand, so decoding is tested without touching the
    /// environment: what a reader must accept is a property of the bytes, not of a flag.
    fn compressed_payload(record: &WriteAheadLogRecord, escaping: bool) -> Vec<u8> {
        let mut encoded = Vec::new();
        record_parts(record).unwrap().put(record, &mut encoded).unwrap();
        let compressed = zstd::stream::encode_all(encoded.as_slice(), COMPRESSION_LEVEL).unwrap();
        let mut out = Vec::new();
        if escaping {
            out.push(COMPRESSED_ESCAPED_PAYLOAD_MARKER);
            out.extend_from_slice(&escape_newlines(&compressed));
        } else {
            out.push(COMPRESSED_RAW_PAYLOAD_MARKER);
            out.extend_from_slice(&compressed);
        }
        out
    }

    #[test]
    fn a_compressed_payload_decodes_to_the_record_that_made_it() {
        let record = compressible_record();
        for escaping in [false, true] {
            let payload = compressed_payload(&record, escaping);
            let back = decode(&payload).expect("compressed payload decodes");
            assert_eq!(record, back, "escaping={escaping}");
        }
    }

    #[test]
    fn a_log_holding_every_encoding_reads_end_to_end() {
        // What a log looks like across a configuration change: records written under different
        // settings, sitting next to each other, all of which must still read.
        let record = compressible_record();
        let mut encoded = Vec::new();
        record_parts(&record).unwrap().put(&record, &mut encoded).unwrap();

        let mut raw = vec![RAW_PAYLOAD_MARKER];
        raw.extend_from_slice(&encoded);
        let mut escaped = vec![BINARY_PAYLOAD_MARKER];
        escaped.extend_from_slice(&escape_newlines(&encoded));

        for payload in [
            raw,
            escaped,
            compressed_payload(&record, false),
            compressed_payload(&record, true),
        ] {
            assert_eq!(record, decode(&payload).expect("payload decodes"));
        }
    }

    #[test]
    fn a_payload_under_the_floor_is_left_alone() {
        // Compressing a tiny payload costs more than it gives back; the page store measured that
        // and chose this floor, so the check is that the floor is honoured, not that it is right.
        let tiny = vec![1u8; COMPRESSION_MIN_BYTES - 1];
        assert!(compress_payload(&tiny).is_none());
    }

    #[test]
    fn a_payload_that_would_not_shrink_is_left_alone() {
        // Random bytes do not compress. Writing them "compressed" would add a frame header to a
        // payload that got no smaller, so the encoder declines.
        let mut incompressible = Vec::with_capacity(4096);
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..4096 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            incompressible.push((state >> 24) as u8);
        }
        assert!(compress_payload(&incompressible).is_none());
    }

    #[test]
    fn compression_is_worth_doing_on_a_record_shaped_like_a_real_one() {
        let record = compressible_record();
        let mut encoded = Vec::new();
        record_parts(&record).unwrap().put(&record, &mut encoded).unwrap();
        let compressed = compress_payload(&encoded).expect("a repetitive record compresses");
        assert!(
            compressed.len() * 2 < encoded.len(),
            "expected better than 2x, got {} -> {}",
            encoded.len(),
            compressed.len()
        );
    }

    #[test]
    fn the_flag_decides_what_is_written_and_never_what_is_read() {
        // One test rather than several: these set a process-wide variable, and separate tests
        // would race each other inside the same binary.
        let record = compressible_record();

        std::env::set_var("TS_WAL_COMPRESS_RECORDS", "1");
        let compressed = encode(&record).expect("encodes with compression on");
        // "0", not unset. Unset is ON now, and this line said "off" while meaning "default" -- the
        // kind of test that keeps passing through exactly the change it should have caught.
        std::env::set_var("TS_WAL_COMPRESS_RECORDS", "0");
        let plain = encode(&record).expect("encodes with compression off");

        assert!(
            matches!(
                compressed.first(),
                Some(&COMPRESSED_RAW_PAYLOAD_MARKER) | Some(&COMPRESSED_ESCAPED_PAYLOAD_MARKER)
            ),
            "compression on should write a compressed marker, got {:?}",
            compressed.first()
        );
        assert!(
            matches!(
                plain.first(),
                Some(&RAW_PAYLOAD_MARKER) | Some(&BINARY_PAYLOAD_MARKER)
            ),
            "compression off should write an uncompressed marker, got {:?}",
            plain.first()
        );
        assert!(compressed.len() < plain.len());

        // The flag is off now, and the compressed record still reads. That is the contract: a
        // deployment that turns this off does not lose the log it already wrote.
        assert_eq!(record, decode(&compressed).expect("still decodes with the flag off"));
        assert_eq!(record, decode(&plain).expect("uncompressed decodes"));
    }

    #[test]
    fn compression_is_on_when_nothing_says_otherwise() {
        // The default is the whole point: a log nobody configured is the log almost every
        // deployment writes. Asserted through `encode`, not by reading the flag, because what
        // matters is which marker reaches the file.
        let record = compressible_record();
        std::env::remove_var("TS_WAL_COMPRESS_RECORDS");
        let written = encode(&record).expect("encodes with nothing set");
        assert!(
            matches!(
                written.first(),
                Some(&COMPRESSED_RAW_PAYLOAD_MARKER) | Some(&COMPRESSED_ESCAPED_PAYLOAD_MARKER)
            ),
            "an unconfigured deployment should compress, got marker {:?}",
            written.first()
        );
        assert_eq!(record, decode(&written).expect("and it reads back"));

        // Every spelling its neighbours accept turns it off, which is what the old read did not do.
        for spelling in ["0", "false", "no", "off", "OFF"] {
            std::env::set_var("TS_WAL_COMPRESS_RECORDS", spelling);
            let plain = encode(&record).expect("encodes with compression off");
            assert!(
                matches!(
                    plain.first(),
                    Some(&RAW_PAYLOAD_MARKER) | Some(&BINARY_PAYLOAD_MARKER)
                ),
                "{spelling:?} should turn compression off, got marker {:?}",
                plain.first()
            );
        }
        std::env::remove_var("TS_WAL_COMPRESS_RECORDS");
    }

    fn record_with(command: Option<Command>) -> WriteAheadLogRecord {
        WriteAheadLogRecord {
            shard_id: 7,
            sequence: 42,
            command,
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        }
    }

    fn cases() -> Vec<(&'static str, WriteAheadLogRecord)> {
        let long_key = "k".repeat(300);
        let mut with_metadata = record_with(Some(Command::StringSet {
            key: "m".to_string(),
            value: vec![1, 2, 3],
        }));
        with_metadata.metadata = Some(WriteAheadLogRecordMetadata {
            version: crate::wal::WRITE_AHEAD_LOG_FORMAT_VERSION,
            timestamp_ms: 1_787_270_070_192,
            items: Vec::new(),
            batch_id: Some(11),
            batch_size: Some(3),
            batch_index: Some(1),
        });
        let mut with_pages = record_with(Some(Command::StringSet {
            key: "p".to_string(),
            value: vec![9; 10],
        }));
        with_pages.staged_pages = vec![StagedPage {
            object_id: 900,
            bytes: vec![7; 4096],
        }];
        let mut with_outcomes = record_with(Some(Command::StringSet {
            key: "o".to_string(),
            value: vec![4; 20],
        }));
        with_outcomes.outcomes = vec![crate::wal::WalOutcomeItem {
            kind: "page".to_string(),
            object_key: "tenant/1/object/9".to_string(),
            component: Some("body".to_string()),
            object_id: 9,
            routing_bucket: 8539,
            address: None,
            // An outcome carries a payload of its own, so one case has to populate it.
            value: Some(vec![8; 128]),
            ttl: Some(60_000),
            deleted: false,
            meta: false,
        }];
        let mut everything = record_with(Some(Command::HashSet {
            key: "k".to_string(),
            field: "f".to_string(),
            value: vec![2; 64],
        }));
        everything.metadata = Some(WriteAheadLogRecordMetadata {
            version: crate::wal::WRITE_AHEAD_LOG_FORMAT_VERSION,
            timestamp_ms: 1_787_270_070_192,
            items: Vec::new(),
            batch_id: Some(4),
            batch_size: Some(2),
            batch_index: Some(0),
        });
        everything.outcomes = vec![crate::wal::WalOutcomeItem {
            kind: "page".to_string(),
            object_key: "tenant/1/object/10".to_string(),
            component: None,
            object_id: 10,
            routing_bucket: 1,
            address: None,
            value: None,
            ttl: None,
            deleted: true,
            meta: true,
        }];
        everything.staged_pages = vec![
            StagedPage {
                object_id: 10,
                bytes: vec![1; 4096],
            },
            StagedPage {
                object_id: 11,
                bytes: Vec::new(),
            },
        ];
        let mut zeroed = record_with(Some(Command::StringSet {
            key: "z".to_string(),
            value: vec![0],
        }));
        zeroed.shard_id = 0;
        zeroed.sequence = 0;

        vec![
            (
                "string set",
                record_with(Some(Command::StringSet {
                    key: "key".to_string(),
                    value: vec![1, 2, 3, 4],
                })),
            ),
            (
                "string set, empty value",
                record_with(Some(Command::StringSet {
                    key: "key".to_string(),
                    value: Vec::new(),
                })),
            ),
            (
                "string set, empty key",
                record_with(Some(Command::StringSet {
                    key: String::new(),
                    value: vec![5],
                })),
            ),
            (
                "string set, both empty",
                record_with(Some(Command::StringSet {
                    key: String::new(),
                    value: Vec::new(),
                })),
            ),
            (
                "string set, key past one varint byte",
                record_with(Some(Command::StringSet {
                    key: long_key,
                    value: vec![6; 5000],
                })),
            ),
            (
                "string set ex, ttl set",
                record_with(Some(Command::StringSetEx {
                    key: "key".to_string(),
                    value: vec![1, 2],
                    ttl_ms: 60_000,
                })),
            ),
            // The trap the arm list documents: a zero TTL cannot round-trip through the modelled
            // form, so it must go verbatim. If the borrowing encoder ever disagreed with
            // `command_to_proto` about which arm this takes, this case is where it would show.
            (
                "string set ex, zero ttl goes verbatim",
                record_with(Some(Command::StringSetEx {
                    key: "key".to_string(),
                    value: vec![1, 2],
                    ttl_ms: 0,
                })),
            ),
            (
                "hash set",
                record_with(Some(Command::HashSet {
                    key: "key".to_string(),
                    field: "field".to_string(),
                    value: vec![3; 100],
                })),
            ),
            (
                "hash set, empty field",
                record_with(Some(Command::HashSet {
                    key: "key".to_string(),
                    field: String::new(),
                    value: vec![3],
                })),
            ),
            (
                "not modelled, goes verbatim",
                record_with(Some(Command::StringDelete {
                    key: "key".to_string(),
                })),
            ),
            ("no command", record_with(None)),
            ("with outcomes", with_outcomes),
            // Every tag at once: the hand-written head, the generated tail, and the hand-written
            // field six behind it. If the three ever stopped writing in tag order, this is the
            // case that says so.
            ("every field populated", everything),
            ("zero shard and sequence", zeroed),
            ("with metadata", with_metadata),
            ("with staged pages", with_pages),
        ]
    }

    /// The head written by hand must be byte for byte what the owned message produced.
    ///
    /// Only the body is compared: the marker and the escaping around it are framing, shared by
    /// both paths and untouched by this change.
    #[test]
    fn the_borrowing_encoder_writes_the_same_bytes() {
        for (label, record) in cases() {
            let parts = record_parts(&record).expect("parts");
            let mut ours = Vec::with_capacity(parts.len);
            parts.put(&record, &mut ours).expect("put");
            let theirs = owned_bytes(&record);
            assert_eq!(ours, theirs, "bytes differ for {label}");
            // The reserved length has to be exact, or the append allocates twice.
            assert_eq!(parts.len, ours.len(), "reserved length wrong for {label}");
            assert_eq!(
                parts.len,
                theirs.len(),
                "reserved length disagrees with the message for {label}"
            );
        }
    }

    /// And what it writes still decodes to the record that went in.
    ///
    /// Byte equality already implies this, but only while the case list covers every arm. This
    /// asserts the property directly, so a case added without a matching arm still fails.
    #[test]
    fn what_it_writes_decodes_back() {
        for (label, record) in cases() {
            let encoded = encode(&record).expect("encode");
            let decoded = decode(&encoded).expect("decode");
            assert_eq!(decoded.shard_id, record.shard_id, "shard for {label}");
            assert_eq!(decoded.sequence, record.sequence, "sequence for {label}");
            assert_eq!(
                serde_json::to_vec(&decoded.command).unwrap(),
                serde_json::to_vec(&record.command).unwrap(),
                "command for {label}"
            );
            assert_eq!(
                decoded.staged_pages.len(),
                record.staged_pages.len(),
                "staged pages for {label}"
            );
        }
    }
}
