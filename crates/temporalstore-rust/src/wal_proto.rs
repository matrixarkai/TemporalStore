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

/// The zstd level records are compressed at.
///
/// Measured on this path rather than inherited. The comment this replaces said the level matched
/// the page store's so the two would "make the same trade rather than two unexplained ones" --
/// which is a good instinct, and was never checked against what a WAL record actually looks like.
///
/// Checked now, on an encoded record whose value is prose, which is what the serving log holds
/// (its records compress about 8.6x, and this payload compresses 9.2x). Fastest of 400 runs, twice,
/// on a quiet box:
///
/// | value | level 1 | level 3 | level 6 |
/// |---|---|---|---|
/// | 1 KiB | 40.4 us, 531 B | 51.3 us, 528 B | 119.0 us, 526 B |
/// | 4 KiB | 47.1 us, 606 B | 60.9 us, 600 B | 168.4 us, 596 B |
///
/// Level 3 costs 27-29% more time than level 1 and returns under 1% in size. Level 6 costs two to
/// three times level 1 for another fraction of a percent. On a payload of random bytes the three
/// levels produce byte-IDENTICAL output, so the higher levels buy nothing there at all.
///
/// So: level 1. This is now a different level from the page store's, which the previous comment
/// was right to want to avoid -- but the two differ in what they compress. A page is large and
/// written once; a WAL record is small, written on the commit path, and its compressible part is
/// the envelope around a value that is usually already dense. A number that was matched for
/// tidiness is worth less than one that was measured, and if the page store's level is ever
/// measured the same way it may well move too.
const COMPRESSION_LEVEL: i32 = 1;

/// Below this many bytes a payload is written uncompressed.
///
/// Not a guess: the page store measured this exact question and found a 1-byte floor worse than
/// no compression at all -- the saving stopped while the median write rose, because a tiny
/// payload costs more to compress than it gives back. 256 is the floor that measurement chose.
const COMPRESSION_MIN_BYTES: usize = 256;

/// Whether new records are written compressed. **DEFAULT ON.**
///
/// Reading never consults this. A payload says what encoding it is in, so a log written across a
/// change reads end to end and turning it off again is not a one-way door -- the same contract
/// `TS_WAL_BINARY_RECORDS` keeps.
///
/// It was built off, which meant every deployment paid to store a log it had the
/// code to shrink. Compression is applied only where it pays twice over: a payload under
/// `COMPRESSION_MIN_BYTES` is left alone, and a payload whose compressed form is not actually
/// smaller is written raw under its own marker.
///
/// The variable now opts OUT, like `TS_WAL_BINARY_RECORDS` and `TS_WAL_BINARY_FRAME` beside it --
/// and for the same reason it is safe to flip either way: which encoding a payload is in is a
/// property of the payload, not of this flag.
///
/// IT DOES NOTHING WITHOUT `TS_WAL_BINARY_RECORDS`. The compressor is reached only from `encode`
/// below, and `encode_wal_payload` calls that only when `binary_records_enabled()`; the JSON arm
/// writes `serde_json::to_vec` straight out. So on a deployment that turns binary records off this
/// flag reads as on, reports as on, and compresses nothing. Turn binary records on first, or the
/// log stays raw JSON.
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
/// The compressor, kept for the life of the thread rather than built per record.
///
/// `zstd::stream::encode_all` builds a fresh compression context on every call, and a level-3
/// context is tens of kilobytes. Measured per append, with an incompressible payload so the
/// figures are the machinery and not the data: at a 1 KiB value compression added 6 allocations
/// and 35,951 bytes, taking the append from 1,159 to 37,110 bytes -- 32x, for a record that is
/// barely a kilobyte. At 4 KiB it added 45,167 bytes.
///
/// The context does not depend on the payload, so there is no reason to build one per record.
/// `bulk::Compressor` holds it and reuses it. What remains per record is the output buffer, which
/// is the compressed bytes themselves and cannot be avoided here.
///
/// Thread-local rather than shared: the compressor is `&mut` to use, so sharing it across threads
/// would need a lock on the append path, and appends are the thing this is trying not to slow
/// down.
thread_local! {
    static RECORD_COMPRESSOR: std::cell::RefCell<Option<zstd::bulk::Compressor<'static>>> =
        const { std::cell::RefCell::new(None) };
}

/// The largest scratch buffer worth keeping between records.
///
/// The buffer grows to the largest record a thread has encoded, and would otherwise hold that
/// capacity for the life of the thread. Past this it is dropped instead.
const MAX_ENCODE_SCRATCH_BYTES: usize = 1024 * 1024;

thread_local! {
    /// The payload buffer the compressing arm of `encode` builds into, kept between records.
    ///
    /// Thread-local for the same reason the compressor beside it is: no lock on the append path.
    /// Nothing borrows it across a call -- the borrow ends inside `encode`, before the compressed
    /// payload is framed -- so a second record on the same thread always finds it free.
    static ENCODE_SCRATCH: std::cell::RefCell<Vec<u8>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn compress_payload(encoded: &[u8]) -> Option<Vec<u8>> {
    if encoded.len() < COMPRESSION_MIN_BYTES {
        return None;
    }
    let compressed = RECORD_COMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = zstd::bulk::Compressor::new(COMPRESSION_LEVEL).ok();
        }
        slot.as_mut()?.compress(encoded).ok()
    })?;
    (compressed.len() < encoded.len()).then_some(compressed)
}

/// The largest decompressed record the reused decompressor will size itself for.
///
/// The capacity comes from the frame header, which is bytes on disk. A corrupted header can
/// declare any length it likes, and `Decompressor::decompress` allocates whatever capacity it is
/// handed -- so without a bound a single bad byte becomes a huge allocation. Past this bound the
/// allocating decoder takes the record instead: it grows to fit what the frame actually holds
/// rather than what it claims to hold.
const MAX_REUSED_INFLATE_BYTES: usize = 16 * 1024 * 1024;

/// Thread-local for the same reason the compressor is: `decompress` takes `&mut self`, and sharing
/// one across threads would put a lock on the path that replays the log.
thread_local! {
    static RECORD_DECOMPRESSOR: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { std::cell::RefCell::new(None) };
}

/// Inflate a compressed payload, reusing one decompression context per thread where the frame
/// allows it.
///
/// `zstd::stream::decode_all` builds a fresh decompression context per call and drops it again --
/// a flat 128 KiB allocated and freed for every compressed record, whatever the record's size.
/// Replay inflates every compressed record in the log, so that was paid once per record on every
/// recovery. Holding the context makes it once per thread.
///
/// Reusing it needs a capacity up front, and the honest source for one is the frame itself:
/// `ZSTD_getFrameContentSize` reports the length the compressor recorded in the header. Frames the
/// append path writes carry it; frames written by the streaming encoder -- every record in a log
/// older than this -- do not, and neither do frames declaring more than the bound above. Those
/// fall through to the decoder that shipped, so no record inflates differently than it used to and
/// none can fail to inflate that would have succeeded.
fn decompress_payload(framed: &[u8]) -> Result<Vec<u8>, String> {
    if let Some(capacity) = declared_inflated_len(framed) {
        let reused = RECORD_DECOMPRESSOR.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = zstd::bulk::Decompressor::new().ok();
            }
            slot.as_mut()?.decompress(framed, capacity).ok()
        });
        if let Some(inflated) = reused {
            return Ok(inflated);
        }
    }
    zstd::stream::decode_all(framed)
        .map_err(|err| format!("compressed wal payload did not inflate: {err}"))
}

/// The decompressed length this frame declares, when it declares one this will size for.
fn declared_inflated_len(framed: &[u8]) -> Option<usize> {
    capacity_within_bound(zstd::zstd_safe::get_frame_content_size(framed).ok()??)
}

fn capacity_within_bound(declared: u64) -> Option<usize> {
    let capacity = usize::try_from(declared).ok()?;
    (capacity <= MAX_REUSED_INFLATE_BYTES).then_some(capacity)
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

/// Bytes an outcome item occupies as an `EngineWalItem` body.
///
/// Paired with `put_wal_item` below, which must write exactly this many. The pair exists so the
/// item's `value` reaches the frame without being copied into a proto message first -- the same
/// reason `staged_blocks` has a hand-written encoder.
///
/// The presence rules are the fiddly part and are the reason this is checked byte for byte against
/// `item_to_proto`. Fields 3, 5, 7, 11, 13, 14, 15, 16, 17 and 19 are proto3 `optional`, which means
/// explicit presence: `Some(0)` and `Some("")` are WRITTEN. Fields 8, 9 and 18 are plain `bool`,
/// which means a false is omitted. The helpers in `raft::wal_proto` implement the plain rule, so
/// they are deliberately not used for the optional fields.
fn wal_item_body_len(item: &WalOutcomeItem, derived: &DerivedItem<'_>) -> usize {
    let mut len = optional_bytes_len(3, Some(item.object_key.len()))
        + optional_varint_len(5, Some(item.object_id))
        + optional_varint_len(7, item.ttl)
        + plain_bool_len(8, item.deleted)
        + plain_bool_len(9, item.meta)
        + optional_bytes_len(11, derived.component.map(str::len));
    if let Some(block) = derived.block.as_ref() {
        len += crate::raft::wal_proto::len_delimited_len(12, block.encoded_len());
    }
    len += optional_bytes_len(13, item.value.as_ref().map(Vec::len))
        + optional_bytes_len(14, derived.kind_name.map(str::len))
        + optional_varint_len(15, Some(u64::from(item.routing_bucket)))
        + optional_varint_len(16, derived.timestamp_ms)
        + optional_varint_len(17, derived.entry_id)
        + plain_bool_len(18, derived.object_deleted)
        + optional_varint_len(19, derived.kind_code.map(u64::from));
    len
}

/// Write an outcome item as field `tag`, its key, component, value and kind name borrowed.
fn put_wal_item(tag: u32, item: &WalOutcomeItem, derived: &DerivedItem<'_>, out: &mut Vec<u8>) {
    let body = wal_item_body_len(item, derived);
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(body as u64, out);

    // Ascending field order, because that is the order prost writes them in.
    put_optional_bytes(3, Some(item.object_key.as_bytes()), out);
    put_optional_varint(5, Some(item.object_id), out);
    put_optional_varint(7, item.ttl, out);
    put_plain_bool(8, item.deleted, out);
    put_plain_bool(9, item.meta, out);
    put_optional_bytes(11, derived.component.map(str::as_bytes), out);
    if let Some(block) = derived.block.as_ref() {
        prost::encoding::encode_key(12, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(block.encoded_len() as u64, out);
        block.encode_raw(out);
    }
    put_optional_bytes(13, item.value.as_deref(), out);
    put_optional_bytes(14, derived.kind_name.map(str::as_bytes), out);
    put_optional_varint(15, Some(u64::from(item.routing_bucket)), out);
    put_optional_varint(16, derived.timestamp_ms, out);
    put_optional_varint(17, derived.entry_id, out);
    put_plain_bool(18, derived.object_deleted, out);
    put_optional_varint(19, derived.kind_code.map(u64::from), out);
}

/// What `item_to_proto` derives, computed once instead of three times.
struct DerivedItem<'a> {
    component: Option<&'a str>,
    kind_name: Option<&'a str>,
    kind_code: Option<u32>,
    timestamp_ms: Option<u64>,
    entry_id: Option<u64>,
    object_deleted: bool,
    block: Option<v1::WalBlockAddress>,
}

fn derive_item(item: &WalOutcomeItem) -> DerivedItem<'_> {
    // `item_to_proto` calls this three times for three fields; it is one answer.
    let numeric = numeric_component(&item.kind, item.component.as_deref());
    let code = kind_code(&item.kind);
    DerivedItem {
        component: match numeric {
            Some(_) => None,
            None => item.component.as_deref(),
        },
        kind_name: match code {
            Some(_) => None,
            None => Some(item.kind.as_str()),
        },
        kind_code: code,
        timestamp_ms: numeric.map(|(key, _)| key),
        entry_id: numeric.and_then(|(_, id)| id),
        object_deleted: item.deleted && item.component.is_none(),
        block: item.address.as_ref().map(|address| {
            let mut encoded = address_to_proto(address);
            if encoded.object_id == Some(item.object_id) {
                encoded.object_id = None;
            }
            encoded
        }),
    }
}

// Presence-aware field helpers. `optional` in proto3 means Some(0) and Some("") are written; the
// helpers in `raft::wal_proto` implement the PLAIN rule, where a default is omitted, and using them
// here would silently drop those values.
fn optional_varint_len(tag: u32, value: Option<u64>) -> usize {
    match value {
        Some(value) => {
            prost::encoding::key_len(tag) + prost::encoding::encoded_len_varint(value)
        }
        None => 0,
    }
}

fn put_optional_varint(tag: u32, value: Option<u64>, out: &mut Vec<u8>) {
    if let Some(value) = value {
        prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
        prost::encoding::encode_varint(value, out);
    }
}

fn optional_bytes_len(tag: u32, len: Option<usize>) -> usize {
    match len {
        Some(len) => crate::raft::wal_proto::len_delimited_len(tag, len),
        None => 0,
    }
}

fn put_optional_bytes(tag: u32, payload: Option<&[u8]>, out: &mut Vec<u8>) {
    if let Some(payload) = payload {
        prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
        prost::encoding::encode_varint(payload.len() as u64, out);
        out.extend_from_slice(payload);
    }
}

fn plain_bool_len(tag: u32, value: bool) -> usize {
    if value {
        prost::encoding::key_len(tag) + 1
    } else {
        0
    }
}

fn put_plain_bool(tag: u32, value: bool, out: &mut Vec<u8>) {
    if value {
        prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
        prost::encoding::encode_varint(1, out);
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
    /// The outcome items, derived from the record but not copied out of it.
    items: Vec<DerivedItem<'a>>,
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
        // Written by hand below, from the outcomes themselves, for the reason the staged blocks
        // are: filling this copied every outcome's `value` into a proto message before the encoder
        // copied it again into the frame, and the bytes allocated per append tracked the outcome
        // value one for one. `the_hand_written_item_is_the_same_item` holds the hand-written form
        // against this one -- which the test helper `owned_bytes` still builds -- byte for byte.
        items: Vec::new(),
        // Written by hand below, from the pages themselves. A staged page is a whole page, and
        // filling this field copied every one of them before the encoder copied them again.
        staged_blocks: Vec::new(),
    };
    let items = record.outcomes.iter().map(derive_item).collect::<Vec<_>>();
    let items_len = record
        .outcomes
        .iter()
        .zip(items.iter())
        .map(|(item, derived)| {
            crate::raft::wal_proto::len_delimited_len(5, wal_item_body_len(item, derived))
        })
        .sum::<usize>();
    let len = crate::raft::wal_proto::varint_field_len(1, record.shard_id)
        + crate::raft::wal_proto::varint_field_len(2, record.sequence)
        + command.as_ref().map_or(0, |command| command.encoded_len_at(3))
        + tail.encoded_len()
        + items_len
        + record
            .staged_pages
            .iter()
            .map(|page| crate::raft::wal_proto::len_delimited_len(6, staged_block_body_len(page)))
            .sum::<usize>();
    Ok(RecordParts {
        command,
        items,
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
        // Ascending field order, the order prost would have written them: the tail carries field
        // four, the outcomes are field five, the staged blocks are field six.
        self.tail.encode(out).map_err(|err| err.to_string())?;
        for (item, derived) in record.outcomes.iter().zip(self.items.iter()) {
            put_wal_item(5, item, derived, out);
        }
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
        //
        // The buffer it builds into is kept between records rather than allocated per append. At a
        // four-kilobyte record that allocation measured 4,682 B, nearly half of everything this
        // path allocated, and none of it had to be new: the bytes are consumed before `encode`
        // returns.
        let compressed = ENCODE_SCRATCH.with(|cell| -> Result<Option<Vec<u8>>, String> {
            let mut scratch = cell.borrow_mut();
            scratch.clear();
            scratch.reserve(parts.len);
            parts.put(record, &mut scratch)?;
            let compressed = compress_payload(&scratch);
            if scratch.capacity() > MAX_ENCODE_SCRATCH_BYTES {
                // One outsized record should not leave every thread that saw it holding the buffer.
                *scratch = Vec::new();
            }
            Ok(compressed)
        })?;
        if let Some(compressed) = compressed {
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
        inflated = decompress_payload(framed)?;
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

    /// The frames the append path writes declare their decompressed length.
    ///
    /// This is the positive control for the reused decompressor. `decompress_payload` falls back
    /// to the allocating decoder whenever a frame declares nothing, and that fallback is silent by
    /// design -- so if the append path ever stopped declaring a length, every read would quietly
    /// take the slow path and no test would fail. This is the test that would fail instead.
    #[test]
    fn the_frames_the_append_path_writes_declare_their_length() {
        let record = compressible_record();
        let mut encoded = Vec::new();
        record_parts(&record).unwrap().put(&record, &mut encoded).unwrap();

        let written = compress_payload(&encoded).expect("a compressible record compresses");
        assert_eq!(
            declared_inflated_len(&written),
            Some(encoded.len()),
            "the frame must declare exactly the length the decompressor is sized for",
        );

        // The streaming encoder wrote every compressed record in a log older than this change, and
        // it declares nothing. That is the case the fallback exists for, not a defect.
        let streamed = zstd::stream::encode_all(encoded.as_slice(), COMPRESSION_LEVEL).unwrap();
        assert_eq!(
            declared_inflated_len(&streamed),
            None,
            "a streamed frame declares no length, so it must take the fallback",
        );
    }

    /// Both shapes of frame inflate to the same bytes, whichever path they take.
    #[test]
    fn a_frame_inflates_the_same_whether_or_not_it_declares_a_length() {
        let record = compressible_record();
        let mut encoded = Vec::new();
        record_parts(&record).unwrap().put(&record, &mut encoded).unwrap();

        let declared = compress_payload(&encoded).expect("a compressible record compresses");
        let streamed = zstd::stream::encode_all(encoded.as_slice(), COMPRESSION_LEVEL).unwrap();

        assert_eq!(decompress_payload(&declared).unwrap(), encoded, "declared frame");
        assert_eq!(decompress_payload(&streamed).unwrap(), encoded, "streamed frame");
    }

    /// A declared length past the bound is refused, because the length came off disk.
    #[test]
    fn a_declared_length_past_the_bound_is_refused() {
        assert_eq!(capacity_within_bound(0), Some(0));
        assert_eq!(
            capacity_within_bound(MAX_REUSED_INFLATE_BYTES as u64),
            Some(MAX_REUSED_INFLATE_BYTES),
        );
        assert_eq!(capacity_within_bound(MAX_REUSED_INFLATE_BYTES as u64 + 1), None);
        assert_eq!(capacity_within_bound(u64::MAX), None);
    }

    /// What `encode` allocates, compressed against raw.
    ///
    /// The compressed arm builds three buffers per record: the payload, the compressed copy of it,
    /// and the output the compressed copy is then memcpy'd into. `compress_to_buffer` could put the
    /// compressor straight into the output and remove the middle one. This measures what that is
    /// worth before anyone writes it.
    #[test]
    #[ignore]
    #[cfg(feature = "alloc-probe")]
    fn what_encoding_a_record_allocates() {
        let record = compressible_record();
        let runs = 64usize;

        std::env::set_var("TS_WAL_BINARY_RECORDS", "1");

        std::env::set_var("TS_WAL_COMPRESS_RECORDS", "0");
        let raw_len = encode(&record).unwrap().len();
        let probe = crate::alloc_probe::Probe::start();
        for _ in 0..runs {
            std::hint::black_box(encode(std::hint::black_box(&record)).unwrap());
        }
        let raw = probe.stop();

        std::env::set_var("TS_WAL_COMPRESS_RECORDS", "1");
        let squeezed_len = encode(&record).unwrap().len();
        let probe = crate::alloc_probe::Probe::start();
        for _ in 0..runs {
            std::hint::black_box(encode(std::hint::black_box(&record)).unwrap());
        }
        let squeezed = probe.stop();

        println!(
            "  ENCODE raw        {raw_len:>6} B out | {:>5.1} allocs {:>8.0} B per record",
            raw.allocs as f64 / runs as f64,
            raw.alloc_bytes as f64 / runs as f64,
        );
        println!(
            "  ENCODE compressed {squeezed_len:>6} B out | {:>5.1} allocs {:>8.0} B per record | the compress arm adds {:>5.1} allocs and {:>8.0} B",
            squeezed.allocs as f64 / runs as f64,
            squeezed.alloc_bytes as f64 / runs as f64,
            (squeezed.allocs as f64 - raw.allocs as f64) / runs as f64,
            (squeezed.alloc_bytes as f64 - raw.alloc_bytes as f64) / runs as f64,
        );

        std::env::remove_var("TS_WAL_COMPRESS_RECORDS");
        std::env::remove_var("TS_WAL_BINARY_RECORDS");
    }

    /// Whether building the proto tail copies each outcome value before the encoder copies it.
    ///
    /// `staged_blocks` was taken out of the proto struct for exactly this reason -- the comment in
    /// `record_parts` says filling it copied every page before the encoder copied them again. The
    /// `items` field is still filled the ordinary way, and every outcome carries an owned value.
    /// If that value is copied, the bytes allocated per append rise with it, one for one.
    #[test]
    #[ignore]
    #[cfg(feature = "alloc-probe")]
    fn does_the_outcome_value_get_copied() {
        for value_len in [512usize, 4096, 16384] {
            let mut record = record_with(Some(Command::StringSet {
                key: "k".to_string(),
                value: vec![1u8; 16],
            }));
            record.outcomes = vec![crate::wal::WalOutcomeItem {
                kind: "page".to_string(),
                object_key: "tenant/1/object/9".to_string(),
                component: Some("body".to_string()),
                object_id: 9,
                routing_bucket: 8539,
                address: None,
                value: Some(vec![7u8; value_len]),
                ttl: Some(60_000),
                deleted: false,
                meta: false,
            }];

            let mut out = Vec::with_capacity(1024 * 1024);
            let prepared = prepare(&record).unwrap();
            out.clear();
            prepared.put(&record, &mut out).unwrap();
            let payload_len = out.len();

            let runs = 32usize;
            let probe = crate::alloc_probe::Probe::start();
            for _ in 0..runs {
                let prepared = prepare(std::hint::black_box(&record)).unwrap();
                out.clear();
                prepared.put(&record, &mut out).unwrap();
                std::hint::black_box(out.len());
            }
            let counts = probe.stop();

            println!(
                "  ITEMS outcome value {value_len:>6} B | payload {payload_len:>6} B | {:>5.1} allocs {:>8.0} B per append",
                counts.allocs as f64 / runs as f64,
                counts.alloc_bytes as f64 / runs as f64,
            );
        }
        println!("  (if the bytes rise with the value, the outcome payload is being copied)");
    }

    /// The hand-written item encoder writes exactly what `item_to_proto` writes, for every shape.
    ///
    /// This is the whole basis for the hand-written encoder existing. It is not enough that the
    /// bytes decode -- they have to be the SAME bytes, because a log holds records written by both
    /// and a length that disagrees with what was written corrupts every record after it.
    ///
    /// The cross product below is chosen for presence rather than for variety. proto3 `optional`
    /// means explicit presence, so `Some(0)` and `Some("")` are written where a plain field holding
    /// its default is omitted, and that distinction is the one a hand-written encoder gets wrong.
    /// So the axes are: an empty and a non-empty string, a zero and a non-zero integer, absent and
    /// present optionals, both booleans, a kind with a code and a kind without one, a component
    /// that parses as a number and one that does not, and an address whose object id repeats the
    /// item's (which is dropped) against one that does not.
    #[test]
    fn the_hand_written_item_is_the_same_item() {
        use prost::Message;

        let addresses = [
            None,
            // object_id repeats the item's, so `item_to_proto` drops it from the address.
            Some(crate::block_store::BlockAddress::from_parts(
                42, 1_048_576, 4096, Some(7), Some(9), Some(8539), Some(3), Some(9),
            )),
            // and one that does not repeat it, so it stays.
            Some(crate::block_store::BlockAddress::from_parts(
                42, 0, 0, None, Some(4_242), None, None, None,
            )),
        ];

        let mut checked = 0usize;
        for kind in ["page", "feature", "context_index", "an_unmapped_kind_name"] {
            for object_key in ["", "tenant/1/object/9"] {
                for object_id in [0u64, 9] {
                    for component in [None, Some(""), Some("body"), Some("1788748713578")] {
                        for value in [None, Some(Vec::new()), Some(vec![7u8; 5])] {
                            for ttl in [None, Some(0u64), Some(60_000)] {
                                for (deleted, meta) in
                                    [(false, false), (true, false), (false, true), (true, true)]
                                {
                                    for routing_bucket in [0u32, 8539] {
                                        for address in addresses.iter() {
                                            let item = crate::wal::WalOutcomeItem {
                                                kind: kind.to_string(),
                                                object_key: object_key.to_string(),
                                                component: component.map(str::to_string),
                                                object_id,
                                                routing_bucket,
                                                address: address.clone(),
                                                value: value.clone(),
                                                ttl,
                                                deleted,
                                                meta,
                                            };

                                            let expected_body =
                                                item_to_proto(&item).encode_to_vec();
                                            let derived = derive_item(&item);

                                            assert_eq!(
                                                wal_item_body_len(&item, &derived),
                                                expected_body.len(),
                                                "length disagrees for {item:?}",
                                            );

                                            let mut mine = Vec::new();
                                            put_wal_item(5, &item, &derived, &mut mine);

                                            let mut theirs = Vec::new();
                                            prost::encoding::encode_key(
                                                5,
                                                prost::encoding::WireType::LengthDelimited,
                                                &mut theirs,
                                            );
                                            prost::encoding::encode_varint(
                                                expected_body.len() as u64,
                                                &mut theirs,
                                            );
                                            theirs.extend_from_slice(&expected_body);

                                            assert_eq!(mine, theirs, "bytes differ for {item:?}");
                                            checked += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // A positive control: an assertion loop that ran zero times would pass just as quietly.
        assert!(checked > 3_000, "only {checked} shapes checked");
        println!("  ITEMENC {checked} shapes, byte-identical");
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
