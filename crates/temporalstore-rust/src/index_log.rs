// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::block_store::BlockAddress;
use crate::types::ShardId;

#[derive(Debug, Error)]
pub enum IndexLogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A committed, newline-terminated served-index-log record failed its per-record
    /// integrity envelope (framing length / SHA-256 digest) or violated delta
    /// sequence-continuity. Surfaced as data loss so the load aborts instead of silently
    /// skipping a delta (an eviction/removal recorded ONLY in the delta -- not the WAL --
    /// would otherwise be lost, resurrecting the removed point or dangling its page ref).
    #[error("index-log record integrity error: {0}")]
    Corruption(String),
    /// A binary-container record could not be decoded: an unknown codec id (written by a
    /// newer binary) or a malformed payload. Distinct from `Corruption` because the framing
    /// envelope already passed -- the bytes arrived intact and this build cannot read them.
    #[error("index-log record encoding error: {0}")]
    Encoding(String),
}

/// Marks an index-log payload as a binary container rather than JSON.
///
/// A reader never has to be told which it is holding: a JSON record starts with `{`, a
/// container with this magic. That is the same discriminator the served-index container
/// uses, and it is what lets one log file hold both shapes while a deployment rolls.
/// Codec and shape, in one byte: codec in the high nibble, shape in the low one.
///
/// This replaced a nine-byte prefix -- a seven-byte magic, a codec byte and a shape byte -- on a
/// record averaging eighty-five bytes, so a tenth of the log was a header saying what the record
/// was. The magic was a second delimiter inside an already-delimited frame: every record reaches
/// disk through `log_framing`, which writes its own marker, a length and a CRC32C, so where a
/// record starts and whether it is intact are already answered before these bytes are read.
///
/// The magic also told a binary payload from the JSON one that predated it. Nothing writes JSON
/// any more, so that question has no second answer either.
fn index_container_byte(codec: u8, shape: u8) -> u8 {
    (codec << 4) | (shape & 0x0f)
}

/// The codec and shape a container byte carries.
fn index_container_parts(byte: u8) -> (u8, u8) {
    (byte >> 4, byte & 0x0f)
}

/// Payload codec: msgpack, struct-as-map.
pub(crate) const INDEX_LOG_CODEC_MSGPACK: u8 = 1;

/// Which record shape a payload holds.
///
/// Two shapes share this log: a whole-index record and a delta. While each row was a map the
/// reader could tell them apart by the names inside, and could read one as the other by matching
/// what it recognised. Rows carry no names, and a map is not proof of shape either -- a whole-index
/// record embeds a `serde_json::Value`, which is a map whatever the rows around it are. So the
/// container says which it holds instead of the reader guessing.
pub(crate) const INDEX_LOG_SHAPE_WHOLE: u8 = 0;
pub(crate) const INDEX_LOG_SHAPE_DELTA: u8 = 1;

/// The compaction anchor: which index a log position describes, without embedding it.
///
/// It used to be written as JSON, with the digest spelled out as 64 hex characters -- the only
/// thing in this log that was not binary, and the reason the reader had to keep a JSON path at
/// all. The row carries the digest as its 32 raw bytes, and its first two elements are the shard
/// and the sequence, which is what the tail scan reads and all it reads.
pub(crate) const INDEX_LOG_SHAPE_ANCHOR: u8 = 2;

/// What an anchor says: the index it identifies, by digest and length.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexAnchorRecord {
    pub shard_id: ShardId,
    pub sequence: u64,
    #[serde(with = "crate::bytes_serde")]
    pub index_sha256: Vec<u8>,
    pub index_len: u64,
}

/// Payload codec: msgpack, struct-as-map, zstd-compressed.
///
/// The index log is the largest durable tier on an ingest-heavy store and almost all of it
/// is text: object keys repeated thousands of times and ids written as decimal digits.
/// Measured on a one-box log of 14.4 MB: 24.8% of the bytes were long decimal ids and the
/// six most repeated key strings were another 20%, and the whole file compressed 11.1x.
///
/// A reader always understands this codec; a writer only emits it when asked, so a store
/// can be rolled forward and back without a format migration. An older binary refuses it
/// as an unknown codec rather than misreading it, which is what the codec byte is for.
pub(crate) const INDEX_LOG_CODEC_MSGPACK_ZSTD: u8 = 2;

/// The single place an index-log record becomes payload bytes.
///
/// Both append paths go through here so the whole-index record and the delta record cannot
/// drift into different shapes -- the reader tells them apart by their fields, which only
/// works while both are encoded the same way.
/// Whether new index-log payloads are written compressed. Off by default.
///
/// Reading codec 2 is unconditional; only writing it is gated, so a store can be rolled
/// forward and back without a migration.
fn index_log_compression_enabled() -> bool {
    matches!(
        std::env::var("TS_INDEX_LOG_COMPRESSION_ENABLED")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Payloads below this many bytes are stored uncompressed.
///
/// zstd on a very small record spends a frame header to save little; the page tier draws
/// the same line at 256 bytes for the same reason.
fn index_log_compression_min_bytes() -> usize {
    std::env::var("TS_INDEX_LOG_COMPRESSION_MIN_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(256)
}

fn encode_index_payload<T: serde::Serialize>(
    record: &T,
    shape: u8,
) -> Result<Vec<u8>, IndexLogError> {
    // Struct-as-ARRAY: positional, with no field name written anywhere. `Serializer::new`
    // without `.with_struct_map()` is the compact form, and the rest of this comment only makes
    // sense that way -- `IndexRecordHead` takes the FIRST TWO VALUES of a record and ignores
    // whatever follows, which is a thing you can only do to an array.
    //
    // (This said "struct-as-MAP, not struct-as-array" while doing the opposite. On a durable
    // format that is not a typo: a reader who believes records are self-describing maps will
    // reorder or insert a field, and every record ever written then decodes into the wrong
    // fields with no error anywhere.)
    //
    // What makes positional safe is that nothing is ever skipped: every record writes every
    // field, so a position always means the same thing. That is the rule to keep. Adding a
    // field means adding it at the END and never reordering what precedes it.
    //
    // It is also why this encoding is SMALLER than the tagged alternative rather than larger.
    // Measured on a delta record carrying eight page items: 550 bytes here against 595 as
    // protobuf, because a tag costs a byte per present field and a position costs nothing.
    let mut packed = Vec::new();
    // Values in field order, for the record as well as for the rows inside it: nothing written
    // here spells a field name.
    let mut serializer = rmp_serde::Serializer::new(&mut packed);
    if serde::Serialize::serialize(record, &mut serializer).is_ok() {
        if index_log_compression_enabled() && packed.len() >= index_log_compression_min_bytes() {
            if let Ok(squeezed) = zstd::stream::encode_all(packed.as_slice(), 3) {
                // Only when it actually helps: a record that does not compress would
                // otherwise pay the frame overhead for nothing.
                if squeezed.len() < packed.len() {
                    let mut out = Vec::with_capacity(squeezed.len() + 1);
                    out.push(index_container_byte(INDEX_LOG_CODEC_MSGPACK_ZSTD, shape));
                    out.extend_from_slice(&squeezed);
                    return Ok(out);
                }
            }
        }
        let mut out = Vec::with_capacity(packed.len() + 1);
        out.push(index_container_byte(INDEX_LOG_CODEC_MSGPACK, shape));
        out.extend_from_slice(&packed);
        return Ok(out);
    }
    // An encode failure must not cost the record: fall through to the bytes that always work,
    // which the reader still takes. This is the only path that now produces JSON.
    Ok(serde_json::to_vec(record)?)
}

/// The single place index-log payload bytes become a record, whatever wrote them.
///
/// Every decode site goes through here. The previous attempt at a binary served index failed
/// because its decoders were scattered and could not move together; this file has four decode
/// sites and they move as one.
pub(crate) fn decode_index_payload<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, IndexLogError> {
    let Some((container, body)) = payload.split_first() else {
        return Err(IndexLogError::Encoding(
            "index-log record has no container byte".to_string(),
        ));
    };
    let (codec, _shape) = index_container_parts(*container);
    match codec {
        INDEX_LOG_CODEC_MSGPACK => rmp_serde::from_slice(body)
            .map_err(|error| IndexLogError::Encoding(error.to_string())),
        INDEX_LOG_CODEC_MSGPACK_ZSTD => {
            let plain = zstd::stream::decode_all(body)
                .map_err(|error| IndexLogError::Encoding(error.to_string()))?;
            rmp_serde::from_slice(&plain)
                .map_err(|error| IndexLogError::Encoding(error.to_string()))
        }
        other => Err(IndexLogError::Encoding(format!(
            "unknown index-log payload codec {other}"
        ))),
    }
}

impl From<crate::log_framing::FramingError> for IndexLogError {
    fn from(err: crate::log_framing::FramingError) -> Self {
        IndexLogError::Corruption(err.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexLogRecord {
    #[serde(rename = "s", alias = "shard_id")]
    pub shard_id: ShardId,
    #[serde(rename = "q", alias = "sequence")]
    pub sequence: u64,
    // Default so a delta-record line (which carries `items`/`meta` but no `index`) still
    // parses as an IndexLogRecord for the sequence-tail scan (`last_sequence_at`), which
    // only reads `.sequence`. Whole-index records always carry `index`.
    #[serde(default)]
    pub index: serde_json::Value,
}

/// Kind of a single delta item in the append-only served-index log. A write emits a
/// bounded set of these (the pages/objects it touched), so the log grows by O(delta)
/// per write instead of O(store). Follows the on-disk item taxonomy: a block item is a
/// concrete page-index entry change, an OBJECT item is an object-level change (e.g. a
/// TTL-only or whole-object tombstone), and a META item carries the compaction anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexItemKind {
    #[serde(rename = "page")]
    Page,
    #[serde(rename = "object")]
    Object,
    #[serde(rename = "meta")]
    Meta,
}

impl Default for IndexItemKind {
    fn default() -> Self {
        IndexItemKind::Page
    }
}

/// One delta item: the smallest change to the served index a write can produce. The
/// field set is the native Rust page-index projection -- `routing_bucket`/`page_ref_key`
/// locate the entry in the bucket map, and `address`/`size`/`in_log`/`deleted`/`model_id`/
/// `object_id`/`page_id` mirror the durable page metadata so replay reconstructs the same
/// `BlockIndex` the whole-index serialization would have produced. `deleted` is a
/// tombstone: replaying it removes the entry.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct IndexItem {
    #[serde(
        rename = "k",
        alias = "kind",
        default,
        serialize_with = "item_kind_as_number",
        deserialize_with = "item_kind_either_shape"
    )]
    pub kind: IndexItemKind,
    #[serde(rename = "rb", alias = "routing_bucket", alias = "routing_slot", default)]
    pub routing_bucket: u32,
    /// The page handle. A `String` in this struct, a NUMBER on the wire whenever it holds one.
    ///
    /// It is a `u64` everywhere else -- `BlockLookupRef::page_ref_key` is one, and the write path
    /// stringifies it on the way in. As decimal text it was 20 bytes of a 161-byte item, 12.4%;
    /// as a number it is about nine.
    ///
    /// Both halves have landed. `block_ref_key_as_number_when_it_is_one` parses the string and
    /// serializes a `u64` when it parses, falling back to a string when it does not; the reader
    /// takes either shape, because records written before the writer half exist on disk and are
    /// not going to rewrite themselves.
    ///
    /// (This said "Nothing writes a number yet" while sitting directly above the serializer that
    /// writes one. That order was right when the reader half landed alone -- a writer emitting a
    /// number to a reader expecting a string would have been refused outright by msgpack rather
    /// than degrading -- and the sentence outlived the sequencing it described. On a durable
    /// format a stale claim about what is on the wire is the kind that gets acted on: the same
    /// file's encoder comment claimed struct-as-map while calling the array form.)
    #[serde(
        rename = "pk",
        alias = "page_ref_key",
        default,
        deserialize_with = "block_ref_key_either_shape",
        serialize_with = "block_ref_key_as_number_when_it_is_one"
    )]
    pub block_ref_key: String,
    #[serde(rename = "ok", alias = "object_key", default)]
    pub object_key: String,
    #[serde(
        rename = "mi",
        alias = "model_id",
        default,
        serialize_with = "model_id_as_number_when_it_is_known",
        deserialize_with = "model_id_either_shape"
    )]
    pub model_id: String,
    #[serde(rename = "c", alias = "component", default)]
    pub component: Option<String>,
    #[serde(rename = "oi", alias = "object_id", default)]
    pub object_id: u64,
    #[serde(rename = "pi", alias = "page_id", default)]
    pub page_id: u64,
    #[serde(rename = "a", alias = "address", default)]
    pub address: Option<BlockAddress>,
    #[serde(rename = "sz", alias = "size", default)]
    pub size: u64,
    #[serde(rename = "il", alias = "in_log", default)]
    pub in_log: bool,
    #[serde(rename = "d", alias = "deleted", default)]
    pub deleted: bool,
}

/// A field whose value is its default says nothing, and every field here carries
/// `#[serde(default)]` -- so a reader that meets an absent one fills in the same value it would
/// have read. That is what makes omitting them safe in a single step, with no ordering between
/// writers and readers: unlike a renamed field or a changed type, an absent field with a default
/// is exactly what an older reader already handles.
fn is_false(value: &bool) -> bool {
    !*value
}

/// See [`is_false`].
fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Accept a page handle written either as text or as a number.
///
/// The handle is a `u64`; it has been carried as decimal text. This lets a reader consume a log
/// whose writer has moved to the number, so the writer can move whenever every reader has this.
/// Write the page-ref key as a NUMBER when it is one, and as text when it is not.
///
/// This is the second half of the two-step change the reader half landed for. The key is a `u64`
/// everywhere else and the write path stringifies it on the way in; as decimal text it was 20
/// bytes of a 161-byte item, and as a msgpack integer it is about nine.
///
/// Safe to flip because `block_ref_key_either_shape` already takes both, and it has shipped: a
/// reader that had only ever seen the string would have had msgpack refuse the type outright
/// rather than degrade, which is exactly why the reader went first.
///
/// A key that does NOT parse as a number still goes out as text, so nothing depends on the write
/// path's stringification being reversible.
impl IndexItem {
    /// Drop from the address what the item already states, so it is not written twice.
    ///
    /// A page item carries `object_id` and `routing_bucket`, and the address it points at carries
    /// both again. For a page belonging to one object they are the same value. Measured, the pair
    /// is 18 bytes of a 142-byte item -- 12.7%. The WAL side already strips exactly this on its way
    /// to protobuf; `item_to_proto` calls it "a full varint on every item whose page belongs to one
    /// object".
    ///
    /// Only what MATCHES is dropped. An address that carries a different object id keeps it, and
    /// `restore_address_repeats` puts back only what is absent, so the pair round-trips.
    /// Drop the composite key when the record already carries every part of it.
    ///
    /// `page_ref_key` is `block_ref_key_from_parts` of this item's own model, object key,
    /// component and address -- measured at 43 bytes of a 176-byte record, the largest single
    /// field in the index log, and every byte of it is spelled out again in the fields beside it.
    /// A reader rebuilds it with the same function, so the log carries the parts and not the
    /// concatenation.
    ///
    /// Cleared only on an exact match. Anything the derivation does not reproduce is written as
    /// it stands, so a key that is not the composite -- the numeric handle form, for one --
    /// survives untouched.
    fn strip_block_ref_key_repeat(&mut self) {
        let Some(address) = self.address.as_ref() else {
            return;
        };
        let derived = block_ref_key_from_parts(
            &self.model_id,
            &self.object_key,
            self.component.as_deref(),
            address.block_slab_id,
            address.offset,
            address.length,
            address.page_id().unwrap_or_default(),
            address.generation().unwrap_or_default(),
        );
        if derived == self.block_ref_key {
            self.block_ref_key.clear();
        }
    }

    /// Rebuild the composite key the writer left out.
    ///
    /// The inverse of `strip_block_ref_key_repeat`. A log written before that stripping carries the
    /// key, and this leaves it alone: it fills only what is absent.
    fn restore_block_ref_key_repeat(&mut self) {
        if !self.block_ref_key.is_empty() {
            return;
        }
        let Some(address) = self.address.as_ref() else {
            return;
        };
        self.block_ref_key = block_ref_key_from_parts(
            &self.model_id,
            &self.object_key,
            self.component.as_deref(),
            address.block_slab_id,
            address.offset,
            address.length,
            address.page_id().unwrap_or_default(),
            address.generation().unwrap_or_default(),
        );
    }

    /// Drop the size when the address already states it.
    ///
    /// `size` and `address.length` are the same number on a page item, written twice.
    fn strip_size_repeat(&mut self) {
        if let Some(address) = self.address.as_ref() {
            if self.size == address.length {
                self.size = 0;
            }
        }
    }

    /// Put the size back from the address that carries it.
    fn restore_size_repeat(&mut self) {
        if self.size == 0 {
            if let Some(address) = self.address.as_ref() {
                self.size = address.length;
            }
        }
    }

    /// Drop the object id when it is the hash of what this row already says.
    ///
    /// An object id is `stable_block_object_id` of the shard, the kind, the key and the
    /// component -- and a row carries the last three, with the record carrying the shard. So the
    /// nine bytes it takes are nine bytes restating a hash of fields sitting beside it.
    ///
    /// Stripped only when the derivation AGREES with what is stored. A row whose id came from
    /// somewhere else keeps it, so a disagreement costs bytes rather than correctness.
    fn strip_object_id_repeat(&mut self, shard_id: ShardId) {
        if self.object_id == self.derived_object_id(shard_id) {
            self.object_id = 0;
        }
    }

    /// Put the object id back by deriving it again.
    fn restore_object_id_repeat(&mut self, shard_id: ShardId) {
        if self.object_id == 0 {
            self.object_id = self.derived_object_id(shard_id);
        }
    }

    fn derived_object_id(&self, shard_id: ShardId) -> u64 {
        crate::engine::hashing::stable_block_object_id(
            shard_id,
            &self.model_id,
            &self.object_key,
            self.component.as_deref(),
        )
    }

    fn strip_address_repeats(&mut self) {
        let object_id = self.object_id;
        let routing_bucket = self.routing_bucket;
        if let Some(address) = self.address.as_mut() {
            if address.object_id() == Some(object_id) {
                address.set_object_id(None);
            }
            if address.routing_bucket() == Some(routing_bucket) {
                address.set_routing_bucket(None);
            }
        }
    }

    /// Put back what the writer left out, from the fields that carry it.
    ///
    /// The inverse of `strip_address_repeats` for anything that writer produced. A log written
    /// before that stripping still carries both, and this leaves those alone: it fills only what is
    /// absent.
    fn restore_address_repeats(&mut self) {
        let object_id = self.object_id;
        let routing_bucket = self.routing_bucket;
        if let Some(address) = self.address.as_mut() {
            if address.object_id().is_none() {
                address.set_object_id(Some(object_id));
            }
            if address.routing_bucket().is_none() {
                address.set_routing_bucket(Some(routing_bucket));
            }
        }
    }
}

/// The model names this log writes as a number instead of as themselves.
///
/// A page header in this design spells the model as an integer; ours spelled it as its name, on
/// every item -- "string" and "hash" written out again for every page a shard has ever indexed,
/// measured at 7 bytes of a 176-byte item.
///
/// APPEND ONLY, and never reordered. The position IS what goes on disk, so moving a name changes
/// what an already-written log says. A name that is not in this table is written as itself, so the
/// table never has to be complete and a new model costs nothing until it is added here.
/// The item kind, as a number.
///
/// The item taxonomy in the design this follows is an enum -- an integer on the wire. Ours was
/// three words, `"page"`, `"object"` and `"meta"`, written once per item, and `"page"` is nearly
/// every item a shard writes.
///
/// The numbers are fixed by this function and its inverse; a kind added later takes the next one
/// and must never take another kind's. A name is still read, so a log written before this decodes
/// unchanged.
fn item_kind_as_number<S>(value: &IndexItemKind, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_u64(match value {
        IndexItemKind::Page => 0,
        IndexItemKind::Object => 1,
        IndexItemKind::Meta => 2,
    })
}

fn item_kind_either_shape<'de, D>(deserializer: D) -> Result<IndexItemKind, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct EitherShape;

    impl serde::de::Visitor<'_> for EitherShape {
        type Value = IndexItemKind;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an item kind, as its name or as its number")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<IndexItemKind, E> {
            match value {
                "page" => Ok(IndexItemKind::Page),
                "object" => Ok(IndexItemKind::Object),
                "meta" => Ok(IndexItemKind::Meta),
                other => Err(E::custom(format!("unknown item kind {other}"))),
            }
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<IndexItemKind, E> {
            self.visit_str(&value)
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<IndexItemKind, E> {
            match value {
                0 => Ok(IndexItemKind::Page),
                1 => Ok(IndexItemKind::Object),
                2 => Ok(IndexItemKind::Meta),
                // Refused rather than defaulted: a kind this cannot name would otherwise arrive as
                // a page item, and a page item names an address that an object or meta item does
                // not have.
                other => Err(E::custom(format!("unknown item kind number {other}"))),
            }
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<IndexItemKind, E> {
            if value < 0 {
                return Err(E::custom(format!("negative item kind {value}")));
            }
            self.visit_u64(value as u64)
        }
    }

    deserializer.deserialize_any(EitherShape)
}

/// Which shape a payload says it holds, if it says.
///
/// A payload with no container magic is the JSON fallback, which only the whole-index path ever
/// wrote, so it answers for that shape rather than for nothing.
pub(crate) fn index_payload_shape(payload: &[u8]) -> Option<u8> {
    match payload.first() {
        Some(container) => Some(index_container_parts(*container).1),
        // No container: written before there was one. Nothing writes that shape now -- even
        // `append_json` wraps its record -- so a payload without a container says nothing about
        // which record it holds, and answering None hands it to the decoder. That serves both
        // cases it can be: a record from an older writer decodes, and a well-framed payload of
        // garbage is REPORTED rather than skipped, because a sweep that quietly skips what it
        // cannot read turns committed corruption into silent data loss.
        None => None,
    }
}

/// The first two values of a record, whatever the rest of it holds.
///
/// Two record shapes share this log and the sweep reads one as the other: it wants a sequence and
/// nothing else, and a delta record also parses as a whole index record with its delta fields
/// dropped. A map allowed that, matching by name and defaulting what was absent. A row cannot: a
/// reader built for three values refuses a row of six.
///
/// Both shapes begin with the same two values, so this takes those by position and ignores
/// whatever follows. It reads a map too, so a record written before rows still answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexRecordHead {
    pub(crate) shard_id: ShardId,
    pub(crate) sequence: u64,
    pub(crate) applied_wal_sequence: Option<u64>,
}

impl<'de> serde::Deserialize<'de> for IndexRecordHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Head;

        impl<'de> serde::de::Visitor<'de> for Head {
            type Value = IndexRecordHead;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an index-log record, as a row or as a map")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<IndexRecordHead, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                use serde::de::Error as _;
                let shard_id: ShardId = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::custom("record has no shard"))?;
                let sequence: u64 = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::custom("record has no sequence"))?;
                // Position two is the payload -- rows, or a whole index. A whole-index record
                // ends there; a delta carries its meta and then its applied sequence.
                let _payload: Option<serde::de::IgnoredAny> = seq.next_element()?;
                // Position three is the meta item on a delta record; a whole-index record ends
                // before it. It is skipped rather than read: reading it as the applied sequence
                // is a five-field struct answering a question about a number.
                let _meta: Option<serde::de::IgnoredAny> = seq.next_element()?;
                let applied_wal_sequence: Option<u64> =
                    seq.next_element::<Option<u64>>()?.flatten();
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(IndexRecordHead {
                    shard_id,
                    sequence,
                    applied_wal_sequence,
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<IndexRecordHead, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                use serde::de::Error as _;
                let mut shard_id = None;
                let mut sequence = None;
                let mut applied = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "s" | "shard_id" => shard_id = Some(map.next_value()?),
                        "q" | "sequence" => sequence = Some(map.next_value()?),
                        "aw" | "applied_wal_sequence" => applied = map.next_value()?,
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(IndexRecordHead {
                    shard_id: shard_id.ok_or_else(|| A::Error::custom("record has no shard"))?,
                    sequence: sequence.ok_or_else(|| A::Error::custom("record has no sequence"))?,
                    applied_wal_sequence: applied,
                })
            }
        }

        deserializer.deserialize_any(Head)
    }
}

/// A row, not a map: the values in field order, with the order fixed here.
///
/// Every row in this log has the same shape, so the shape does not belong in the row. As a map,
/// each row carried its own field names -- measured at about 25 bytes of a 126-byte record, the
/// largest single line item left in it. A protobuf design pays a tag for this; a self-describing
/// one pays a name; a row pays nothing and is read by position.
///
/// The record AROUND the rows stays a map on purpose. Two record shapes share this log and one is
/// read as the other -- a delta container is decoded as a whole index record by the sweep -- which
/// works because a map matches by name and a missing field defaults. Positions cannot do that, so
/// only the rows are positional.
///
/// The derived `Deserialize` takes either shape, so rows written before this still decode; the
/// order below must match the field order of the struct and must not be reordered.
impl serde::Serialize for IndexItem {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq as _;

        struct Kind<'a>(&'a IndexItemKind);
        impl serde::Serialize for Kind<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                item_kind_as_number(self.0, s)
            }
        }
        struct Handle<'a>(&'a str);
        impl serde::Serialize for Handle<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                block_ref_key_as_number_when_it_is_one(self.0, s)
            }
        }
        struct Model<'a>(&'a str);
        impl serde::Serialize for Model<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                model_id_as_number_when_it_is_known(self.0, s)
            }
        }

        let mut row = serializer.serialize_seq(Some(12))?;
        row.serialize_element(&Kind(&self.kind))?;
        row.serialize_element(&self.routing_bucket)?;
        row.serialize_element(&Handle(&self.block_ref_key))?;
        row.serialize_element(&self.object_key)?;
        row.serialize_element(&Model(&self.model_id))?;
        row.serialize_element(&self.component)?;
        row.serialize_element(&self.object_id)?;
        row.serialize_element(&self.page_id)?;
        row.serialize_element(&self.address)?;
        row.serialize_element(&self.size)?;
        row.serialize_element(&self.in_log)?;
        row.serialize_element(&self.deleted)?;
        row.end()
    }
}

const MODEL_ID_NUMBERS: &[&str] = &[
    "string",
    "hash",
    "set",
    "feature",
    "sequence",
    "control_state",
    "context_node",
    "context_event",
    "context_index",
    "context_audit",
    "context_child",
    "context_embedding",
    "context_summary",
    "context_compression",
    "context_entity",
];

fn model_id_as_number_when_it_is_known<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match MODEL_ID_NUMBERS.iter().position(|name| *name == value) {
        Some(index) => serializer.serialize_u64(index as u64),
        None => serializer.serialize_str(value),
    }
}

fn model_id_either_shape<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct EitherShape;

    impl serde::de::Visitor<'_> for EitherShape {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a model, as its name or as its number")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<String, E> {
            Ok(value)
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<String, E> {
            // A number this table cannot name is refused rather than guessed. Answering the wrong
            // model would file a page under a model that did not write it, which reads as missing
            // data for one model and foreign data for another.
            MODEL_ID_NUMBERS
                .get(value as usize)
                .map(|name| (*name).to_string())
                .ok_or_else(|| E::custom(format!("unknown model number {value}")))
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<String, E> {
            if value < 0 {
                return Err(E::custom(format!("negative model number {value}")));
            }
            self.visit_u64(value as u64)
        }
    }

    deserializer.deserialize_any(EitherShape)
}

fn block_ref_key_as_number_when_it_is_one<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value.parse::<u64>() {
        Ok(number) => serializer.serialize_u64(number),
        Err(_) => serializer.serialize_str(value),
    }
}

fn block_ref_key_either_shape<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct EitherShape;

    impl serde::de::Visitor<'_> for EitherShape {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a page handle, as a string or an unsigned integer")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<String, E> {
            Ok(value)
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<String, E> {
            Ok(value.to_string())
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<String, E> {
            Ok(value.to_string())
        }
    }

    deserializer.deserialize_any(EitherShape)
}

/// Slab lifecycle state folded into the index-log MetaItem. 1:1 with
/// `block_store::BlockStoreSlabState` and with the on-disk slab-state encoding
/// (INIT/CREATED/FROZEN/RECYCLED): Active==CREATED, Sealed==FROZEN, DelayedDestroy/Purged
/// cover the RECYCLED grace. Serialized snake_case so it round-trips with the slab manifest's
/// own state enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlabCatalogState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

/// One slab catalog entry folded into the index-log MetaItem, mirroring this design
/// the durable slab catalog in the index-log anchor. Carries the DURABLE catalog fields the
/// slab descriptor tracks -- lifecycle state, byte counts, timestamps, page-id range, version.
/// The slab descriptor's DIAGNOSTIC fields (readable_prefix_physical_bytes / has_corruption /
/// first_error*) are intentionally ABSENT: they are recomputed on load by scanning the slab
/// (`inspect_slab`, driven by `rebuild_slab_manifest_at` / reconcile-on-open), exactly as the
/// are not persisted. So this is the lossless durable projection of a slab.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlabCatalogEntry {
    #[serde(alias = "zone_id")]
    #[serde(rename = "page_slab_id")]
    pub block_slab_id: u64,
    pub state: SlabCatalogState,
    #[serde(alias = "total_bytes")]
    pub physical_bytes: u64,
    #[serde(default)]
    pub logical_bytes: u64,
    #[serde(default)]
    pub created_unix_ms: Option<u64>,
    #[serde(default)]
    pub updated_unix_ms: Option<u64>,
    #[serde(rename = "first_page_id", default)]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_page_id", default)]
    pub last_block_id: Option<u64>,
    #[serde(default)]
    pub version: u64,
}

/// Compaction anchor for the delta log. `start_wal_sequence` is the lowest WAL sequence
/// still required to reconstruct the served index on top of the base snapshot: once the
/// base `shard-{id}.index.json` is rewritten at a compaction point, the anchor advances
/// and every delta record at or before it can be truncated. Matches
/// MetaItem's `start_WAL_id` role in the native WAL vocabulary.
///
/// `slabs` folds the slab catalog into the anchor, and serializes under its original
/// `zones` key. It
/// is populated ONLY at a threshold dump;
/// with the gate off it is always empty and (via `skip_serializing_if`) not serialized, so an
/// anchor record is byte-identical to the pre-fold record. Anchors without `zones`
/// deserialize to an empty catalog and replay unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaItem {
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub start_wal_sequence: u64,
    #[serde(default)]
    pub timestamp_ms: u64,
    #[serde(rename = "zones", default)]
    pub slabs: Vec<SlabCatalogEntry>,
    #[serde(rename = "zone_version", default)]
    pub slab_version: u64,
}

/// One appended delta record: either a batch of page/object item deltas (PAGE/OBJECT) or
/// a compaction anchor (META). Written one JSON line per record to the same append-only
/// log file as `IndexLogRecord`; the two are distinguished on read by the presence of the
/// `items`/`meta` fields, so legacy whole-index records replay untouched.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDeltaRecord {
    #[serde(rename = "s", alias = "shard_id")]
    pub shard_id: ShardId,
    #[serde(rename = "q", alias = "sequence")]
    pub sequence: u64,
    #[serde(default)]
    pub items: Vec<IndexItem>,
    #[serde(default)]
    pub meta: Option<MetaItem>,
    /// The WAL sequence this write reflected (the served-index anchor at append time). On
    /// load, deltas with a sequence at or below the base snapshot's anchor are already
    /// folded into the base and are skipped; folding the rest advances the reconstructed
    /// anchor so WAL replay re-executes only the uncaptured tail (never relocating the
    /// pages the deltas already pin at their original addresses).
    #[serde(rename = "aw", alias = "applied_wal_sequence", default)]
    pub applied_wal_sequence: Option<u64>,
    /// When true, this record's items are the exact pages the write produced: replay replaces
    /// each item's (kind, object, component) predecessor and inserts, WITHOUT the covered-key
    /// wipe -- mirroring the write path's upsert_bucket_index_block. When false (the default,
    /// and every record written before this field existed), the record snapshots each covered
    /// object's whole page set and replay wipes-then-restores. The snapshot shape is what made
    /// every append O(store): a batch touching the grow-with-the-store index hashes logged
    /// every page of each of them, every time.
    #[serde(rename = "u", alias = "upsert", default)]
    pub upsert: bool,
    /// Opaque per-touched-key state blobs (one JSON object per key) carrying the
    /// authoritative post-write value of the maps that are NOT reconstructable from a
    /// single page-index entry -- packed timestamped series (feature membership survives
    /// eviction) and the non-page maps (TTL, control-state change/selection, context
    /// nodes). Opaque here so the index-log layer stays decoupled from `ShardState`; the
    /// engine builds and applies them. Replaying these on load pins the exact membership a
    /// write produced, so reconstruction from physical pages cannot resurrect evicted data.
    #[serde(rename = "ks", alias = "key_states", default)]
    pub key_states: Vec<serde_json::Value>,
    /// The one object key every item carries, when they all carry the same one.
    ///
    /// A timestamped write stages ONE ITEM PER REF -- all for the same object, differing only in
    /// their component -- so a feature append of thirty-two points wrote that key thirty-two
    /// times. Measured on a record of that shape: 21 bytes per item, 24-31% of the record.
    ///
    /// Set only when every item agrees, and then each item's own `object_key` is written empty
    /// and this is what the reader puts back.
    ///
    /// Deliberately NOT a "same as the previous item" marker. That has to read an empty key as
    /// "repeat", and an item whose key really is empty cannot be ruled out here: the empty-key
    /// guards live in the HTTP handlers and the proxy, not in the engine. This is unambiguous in
    /// every case instead -- keys that differ leave it `None` and are all written, and a record
    /// whose items all share the empty string sets it to `Some(String::new())` and restores
    /// exactly that.
    ///
    /// No `skip_serializing_if`. The encoding is POSITIONAL, so a skipped field shifts every
    /// field after it; `None` goes on the wire as nil. Appended at the END, which is the only
    /// safe place to add to a positional record.
    #[serde(rename = "sk", alias = "shared_object_key", default)]
    pub shared_object_key: Option<String>,
}

/// The key a page is written under, from its parts.
///
/// One definition, called from the page index's serialization and from the replay log, so the two
/// cannot drift into different spellings of the same page.
pub fn block_ref_key_from_parts(
    kind: &str,
    object_key: &str,
    component: Option<&str>,
    block_slab_id: u64,
    offset: u64,
    length: u64,
    page_id: u64,
    generation: u64,
) -> String {
    // Built into one buffer rather than with `format!`, which allocates its own and then
    // allocates again to hand back a String. This runs once per page on every dump of the index,
    // where it was the largest single source of allocations.
    use std::fmt::Write as _;
    let component = component.unwrap_or("");
    // Five u64 at their widest, plus the seven separators.
    let mut key = String::with_capacity(kind.len() + object_key.len() + component.len() + 7 + 100);
    key.push_str(kind);
    key.push(':');
    key.push_str(object_key);
    key.push(':');
    key.push_str(component);
    // `write!` into a String appends in place; it does not allocate.
    let _ = write!(
        key,
        ":{block_slab_id}:{offset}:{length}:{page_id}:{generation}"
    );
    key
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    /// Records this store has read back and handed to a decoder.
    ///
    /// Beside `bytes_read`, which is the same work measured the other way. Both are what makes a
    /// claim about what a round COSTS checkable without a clock: a guard that asserts a bound on
    /// milliseconds asserts something about the machine, and this box cannot hold one still.
    #[serde(default)]
    pub records_read: u64,
    pub last_sequence: u64,
}

/// What the index-GC gate needs, and what answering it cost.
///
/// The gate decides on a REMOVABLE-RECORD RATIO, which is two counts. Getting them by scanning
/// and decoding the whole log made the maintenance round O(log size) even after reclaim itself
/// stopped being -- the cost moved from the collector to the thing that decides whether to
/// collect. Every one of these numbers except the last two is arithmetic on piece NAMES plus the
/// one piece being written.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogGateSummary {
    pub shard_id: ShardId,
    /// The retain floor the counts below were taken against.
    pub retain_from_sequence: u64,
    /// On-disk bytes of the whole log: one `stat` per piece, nothing opened.
    pub bytes: u64,
    /// How many records the log holds.
    pub records: usize,
    /// How many of them sit below the floor.
    pub removable_records: usize,
    /// Files the log is in, the one being written included. This is what the gate now costs.
    pub pieces: usize,
    /// How many of those were answered from the NAME alone -- every sealed one.
    pub pieces_named: usize,
    /// Bytes read and decoded: the piece being written, and nothing else. Bounded by the rolling
    /// threshold, not by the log.
    pub bytes_decoded: u64,
    /// Records read and decoded, bounded the same way.
    pub records_decoded: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogGcReport {
    pub shard_id: ShardId,
    pub retain_from_sequence: u64,
    #[serde(default)]
    pub max_entries_per_round: usize,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    #[serde(default)]
    pub removable_records_before_budget: usize,
    #[serde(default)]
    pub budget_exhausted: bool,
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// On-disk bytes the sweep found reclaimable, whether or not it rewrote the log for them.
    #[serde(default)]
    pub reclaimable_bytes: u64,
    /// The sweep read the log, decided what was reclaimable, and did NOT rewrite it -- because
    /// nothing was reclaimable, or too little was to be worth the barrier. Distinguishes a log
    /// that had nothing to give from one that was rewritten and gave nothing, which otherwise
    /// look identical: both report zero records removed and the same bytes before and after.
    #[serde(default)]
    pub rewrite_skipped: bool,
    /// Bytes the round COPIED: what it wrote into the replacement for the piece being written.
    ///
    /// This is what a round costs. The collector used to rewrite every record it retained, so
    /// this number was the size of everything it KEPT, and a round that removed less copied
    /// more. Earlier pieces are now unlinked instead, and nothing in them is read or copied, so
    /// this is bounded by the piece being written however deep the log is.
    #[serde(default)]
    pub bytes_copied: u64,
    /// Whole pieces of the log this round unlinked.
    #[serde(default)]
    pub dropped_segments: usize,
    /// What those pieces held on disk.
    #[serde(default)]
    pub dropped_segment_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct LocalIndexLogStore {
    inner: Arc<Mutex<IndexLogInner>>,
    /// One barrier gate per shard log, so appends that arrive while an fsync is in flight ride it
    /// instead of queueing to take an identical one.
    flush_gates: Arc<crate::flush_gate::FlushRegistry>,
}

#[derive(Debug)]
struct IndexLogInner {
    root: PathBuf,
    stats: IndexLogStats,
    last_sequence_by_shard: HashMap<ShardId, u64>,
    /// MANIFEST-CONFORMANCE FOLD: the index-log byte length recorded at the last catalog dump, per
    /// shard. `undumped_len_since_dump` subtracts this from the current on-disk length to get the
    /// undumped gap that drives the threshold-dump cadence. Reset to 0
    /// on process restart, so the first post-restart cycle may dump once -- harmless (a dump only
    /// materializes durable state that is already recoverable).
    last_dumped_len_by_shard: HashMap<ShardId, u64>,
    /// When the last catalog dump of a shard completed. Read by `ms_since_catalog_dump` to hold
    /// the next dump off until the minimum interval has passed.
    ///
    /// An `Instant`, not a wall clock, because the question is "how long since", and a wall
    /// clock that steps backwards would hold a dump off for as long as the step, while one that
    /// steps forward would release it early. A shard never dumped in this process has no entry,
    /// and no entry means no floor: the first dump after a restart is never delayed.
    last_dumped_at_by_shard: HashMap<ShardId, std::time::Instant>,
    // Set only by Default: the store owns its minted scratch directory, and the last
    // clone's drop removes it. Never set for a caller-supplied root.
    scratch: Option<std::sync::Arc<crate::scratch::ScratchDirGuard>>,
}

fn indexlog_enabled() -> bool {
    // The index-log is the delta served-index's durable per-write delta stream: the
    // load-time fold reconstructs the served index (at the original page addresses) from
    // these records, so it must be written in ALL builds, not just tests. It is kept
    // bounded by GC-at-compaction, which truncates every delta already folded into the
    // durably-rewritten base snapshot (retain-from-anchor). Always on.
    true
}

fn bulk_ingest_mode() -> bool {
    crate::engine::bulk_ingest_mode()
}

/// Defer the ack-path index-log fsync (WAL replay is the durable recovery source). This is the
/// single-barrier default; restored to a synchronous fsync only under the TS_WAL_LEGACY_RECOVERY
/// escape hatch (whose delta-fold recovery trusts the durable delta).
fn indexlog_wal_only_sync() -> bool {
    // See `block_store::block_wal_single_barrier`: one reader, in `engine`, so the three barriers
    // this hatch controls cannot end up disagreeing about whether it is set.
    !crate::engine::wal_legacy_recovery()
}

/// MANIFEST-CONFORMANCE FOLD, always on: the slab catalog is folded into the index-log
/// anchor at a threshold dump, and the per-write slab-manifest file is no longer the catalog's
/// source of truth (it is reconstructed on load from the durable pages plus the folded anchor).
/// `TS_INDEX_CATALOG_FOLD` used to be able to skip all of it. It shipped dark (the flip once
/// broke proxy tests), then defaulted on once the upsert-delta and single-barrier work landed,
/// and nothing ever selected the off position again -- the five tests that exercise the fold
/// pinned it ON rather than off. The threshold dump is also what lets the embedded (proxy)
/// engine reclaim its index-log and WAL at all, so the off side disabled reclaim entirely.
///
/// The off side's compatibility claim still holds and is not conditional on anything: an anchor
/// whose `slabs` is empty serializes with no `zones` key, byte-identical to a pre-fold MetaItem.
/// `meta_item_without_zones_serializes_byte_identically_to_pre_fold` asserts exactly that.
///
/// Threshold decision for the background catalog/index dump, mirroring this design
/// index-meta dump gate (the dump-delay check compares the undumped
/// WAL length against the 1 MiB gap). `undumped_bytes` is the served-index-log growth since
/// the last dumped watermark; when it crosses `gap_bytes` the dump fires. A zero gap disables
/// the cadence (never dump on threshold) so an operator can pin dumps to compaction/unload only.
pub fn should_dump_index_catalog(undumped_bytes: u64, gap_bytes: u64) -> bool {
    gap_bytes > 0 && undumped_bytes >= gap_bytes
}

/// The threshold decision above, with a floor on how often it may say yes.
///
/// `ms_since_dump` is `None` for a shard this process has never dumped, which is not a dump
/// "0 ms ago" -- it is no dump at all, and nothing to wait behind. Getting that backwards would
/// silence the first dump after every restart for the length of the interval, on exactly the
/// engine that has the most to reclaim.
///
/// A zero interval is no floor at all, so the byte gap alone decides. That is what a test which
/// is not exercising the timer passes, and it says so by passing it.
pub fn should_dump_index_catalog_now(
    undumped_bytes: u64,
    gap_bytes: u64,
    ms_since_dump: Option<u64>,
    min_interval_ms: u64,
) -> bool {
    if !should_dump_index_catalog(undumped_bytes, gap_bytes) {
        return false;
    }
    match ms_since_dump {
        Some(elapsed) => elapsed >= min_interval_ms,
        None => true,
    }
}

impl LocalIndexLogStore {
    /// On-disk byte length of a shard's index-log file (0 if absent). Used as the "undumped
    /// length" signal for the threshold-dump cadence: the growth of this file since the last
    /// dumped watermark is the undumped-length signal.
    pub fn log_len_bytes(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("index log lock poisoned");
        // Every piece, not just the one being written. After a roll the active piece is the
        // NEWEST and smallest part of the log, so reporting its length as the log's length makes
        // a log SHRINK as it grows -- and the dump cadence that reads this would stop firing
        // exactly when there is most to dump.
        index_log_total_bytes(&inner.root, shard_id)
    }

    /// Undumped index-log length for a shard: the on-disk byte growth since the last catalog
    /// dump (`mark_catalog_dumped`). This is the native analog of this design
    /// the undumped WAL length, and is the signal compared against `index_dump_wal_gap_bytes`
    /// to decide a threshold dump. A shard never dumped this process (or freshly restarted)
    /// reports the whole current length.
    /// Milliseconds since this shard's last catalog dump, or `None` if it has not dumped in
    /// this process. Feeds `should_dump_index_catalog_now`'s interval check.
    pub fn ms_since_catalog_dump(&self, shard_id: ShardId) -> Option<u64> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        inner
            .last_dumped_at_by_shard
            .get(&shard_id)
            .map(|at| at.elapsed().as_millis() as u64)
    }

    pub fn undumped_len_since_dump(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("index log lock poisoned");
        let current = index_log_total_bytes(&inner.root, shard_id);
        let dumped = inner
            .last_dumped_len_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        current.saturating_sub(dumped)
    }

    /// Record that a catalog dump captured the shard's index-log up to its current on-disk
    /// length, resetting the undumped gap to 0. Called by the engine ONLY after the dump's base
    /// index + folded anchor are durably written, so the watermark never advances past
    /// non-durable state (restart-during-dump re-dumps rather than skipping).
    pub fn mark_catalog_dumped(&self, shard_id: ShardId) {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let current = index_log_total_bytes(&inner.root, shard_id);
        inner.last_dumped_len_by_shard.insert(shard_id, current);
        // Stamped in the same call as the length, so the two halves of the cadence -- how much
        // has accumulated, and how long ago -- can never disagree about which dump they describe.
        inner
            .last_dumped_at_by_shard
            .insert(shard_id, std::time::Instant::now());
    }

    /// The most recent `MetaItem` anchor carrying a folded slab catalog, or `None` if no
    /// anchor with a non-empty `slabs` list has been written. Used on load (gate on) to seed the
    /// block-store slab catalog from the folded anchor when the slab-manifest file is absent.
    pub fn latest_slab_catalog(&self, shard_id: ShardId) -> Result<Option<MetaItem>, IndexLogError> {
        // The LAST matching record wins, so this cannot stop early -- but it never needed to
        // hold the records it walks past. It kept every record in the log, with every item each
        // one carries, to take one field out of one of them.
        let mut latest = None;
        self.for_each_delta_record(shard_id, 0, |record| {
            if let Some(meta) = record.meta {
                if !meta.slabs.is_empty() {
                    latest = Some(meta);
                }
            }
        })?;
        Ok(latest)
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        Self {
            inner: Arc::new(Mutex::new(IndexLogInner {
                root,
                stats: IndexLogStats::default(),
                last_sequence_by_shard: HashMap::new(),
                last_dumped_len_by_shard: HashMap::new(),
                last_dumped_at_by_shard: HashMap::new(),
                scratch: None,
            })),
            flush_gates: Arc::new(crate::flush_gate::FlushRegistry::default()),
        }
    }

    /// Append an index record, PARSING the bytes into a value first.
    ///
    /// Prefer [`append_index_bytes`](Self::append_index_bytes), which splices the already
    /// serialized bytes into the record instead. This one parses the whole index into a
    /// `serde_json::Value` and then re-encodes it, so a multi-megabyte index is walked twice
    /// more per append -- measured at 2.31 MB for a 2,000-key shard. It remains for callers
    /// that genuinely need the parsed record back; every engine caller writes an index it has
    /// just serialized and discards the result, so they all take the splicing path.
    pub fn append_json(
        &self,
        shard_id: ShardId,
        index_bytes: &[u8],
    ) -> Result<IndexLogRecord, IndexLogError> {
        // Bulk backfill: skip the replay-log append entirely (deferred to a
        // single flush of the served index). Removes the per-record fsync bomb.
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(IndexLogRecord {
                shard_id,
                sequence: 0,
                index: serde_json::Value::Null,
            });
        }
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        // Seal the piece being written if this record would take it past the rolling threshold,
        // so reclaim has whole pieces to unlink instead of a file to rewrite. The check is a
        // stat; the piece is walked once per ROLL, never once per append -- re-reading the file
        // to answer "is this piece full" on every write is the cost the write-ahead log had to
        // take back out.
        //
        // AFTER the sequence probe above, not before it. That probe is what trims a tail a crash
        // left half-written, and only the piece being written is ever trimmed -- so rolling first
        // would seal the torn bytes into a piece nothing trims again, and the piece's recorded
        // end would be taken from the last record before them.
        roll_index_log_segment_if_due(&inner.root, shard_id)?;
        let next_sequence = last_sequence.saturating_add(1);
        let record = IndexLogRecord {
            shard_id,
            sequence: next_sequence,
            index: serde_json::from_slice(index_bytes)?,
        };
        // Frame the record with a length + SHA-256 digest (crate::log_framing) so a later
        // value-preserving bit-flip in this committed line is detected on read.
        let bytes =
            crate::log_framing::encode_record(&encode_index_payload(&record, INDEX_LOG_SHAPE_WHOLE)?);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path index-log append. Under the single-barrier default defer this fsync (bytes
        // still written): the WAL is the durable recovery source and replay rebuilds the
        // served index, so the replay-log checkpoint need not be crash-durable per write.
        if !indexlog_wal_only_sync() {
            file.sync_data()?;
        }
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(record)
    }

    pub fn append_index_bytes(
        &self,
        shard_id: ShardId,
        index_bytes: &[u8],
    ) -> Result<u64, IndexLogError> {
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(0);
        }
        // This once asserted the bytes parse as JSON, from when this appender spliced the whole
        // index into the record. It no longer does -- it writes a digest and a length, and never
        // looks inside -- so the JSON demand was a precondition that outlived its reason, and it
        // fires the moment the index is written in its binary container. Assert what this code
        // actually needs: that it was handed an index at all, in either format a reader accepts.
        debug_assert!(
            crate::engine::bytes_look_like_served_index(index_bytes),
            "index-log anchor was handed {} bytes that are not a served index in any known format",
            index_bytes.len()
        );
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        // Seal the piece being written if this record would take it past the rolling threshold,
        // so reclaim has whole pieces to unlink instead of a file to rewrite. The check is a
        // stat; the piece is walked once per ROLL, never once per append -- re-reading the file
        // to answer "is this piece full" on every write is the cost the write-ahead log had to
        // take back out.
        //
        // AFTER the sequence probe above, not before it. That probe is what trims a tail a crash
        // left half-written, and only the piece being written is ever trimmed -- so rolling first
        // would seal the torn bytes into a piece nothing trims again, and the piece's recorded
        // end would be taken from the last record before them.
        roll_index_log_segment_if_due(&inner.root, shard_id)?;
        let next_sequence = last_sequence.saturating_add(1);
        // Record WHICH index this checkpoint anchors, not a second copy of it.
        //
        // This used to splice the whole served index into the record -- 2.31 MB for a 2,000-key
        // shard, written again here after it had just been written to the index file. Nothing
        // ever read it back: every path that reconstructs a shard decodes the index FILE or a
        // dump manifest, and the log's own readers take only `sequence` (the tail scan and the
        // GC anchor probe) or raw bytes (the debug stream). Removing the payload and running
        // the reload, recovery-sweep, storage-lifecycle, dump, index-format, load-index and
        // anchor suites changed nothing -- 77 tests, all still green.
        //
        // The digest keeps what the copy was actually good for: an anchor can still be checked
        // against the index file it claims to describe. `append_json` still embeds the whole
        // index for any caller that wants the record to carry it.
        let digest = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(index_bytes);
            hasher.finalize().to_vec()
        };
        let index_len = index_bytes.len();
        let anchor = IndexAnchorRecord {
            shard_id,
            sequence: next_sequence,
            index_sha256: digest,
            index_len: index_len as u64,
        };
        let bytes = crate::log_framing::encode_record(&encode_index_payload(
            &anchor,
            INDEX_LOG_SHAPE_ANCHOR,
        )?);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        // Ack-path index-log append. Under the single-barrier default defer this fsync (bytes
        // still written): the WAL is the durable recovery source and replay rebuilds the
        // served index, so the replay-log checkpoint need not be crash-durable per write.
        if !indexlog_wal_only_sync() {
            file.sync_data()?;
        }
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        Ok(next_sequence)
    }

    /// Append one O(delta) served-index record: the page/object items a single write
    /// touched, optionally carrying a compaction anchor. Unlike `append_json`, this never
    /// serializes the whole index -- the appended bytes are proportional to the change,
    /// which is what turns the per-write served-index persist cost from O(store) into
    /// O(delta). Returns the assigned monotonic sequence.
    ///
    /// `durable` controls the fsync: the normal (single-node / shared-store) path passes
    /// `true` (the record is fsync'd before returning). The raft apply path passes `false` --
    /// the record is written + flushed to the OS but NOT fsync'd, because there the raft log
    /// is the durability + reconstruction source and a lost non-fsync'd tail is rebuilt by
    /// raft-log replay on restart. The consumer-aware GC never truncates such a tail (it
    /// retains from the durable dump/cursor/snapshot frontier).
    pub fn append_delta(
        &self,
        shard_id: ShardId,
        items: Vec<IndexItem>,
        key_states: Vec<serde_json::Value>,
        applied_wal_sequence: Option<u64>,
        meta: Option<MetaItem>,
        upsert: bool,
        durable: bool,
    ) -> Result<u64, IndexLogError> {
        if bulk_ingest_mode() || !indexlog_enabled() {
            return Ok(0);
        }
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_sequence_at(&inner.root, shard_id)?;
                inner.last_sequence_by_shard.insert(shard_id, sequence);
                sequence
            }
        };
        // Seal the piece being written if this record would take it past the rolling threshold,
        // so reclaim has whole pieces to unlink instead of a file to rewrite. The check is a
        // stat; the piece is walked once per ROLL, never once per append -- re-reading the file
        // to answer "is this piece full" on every write is the cost the write-ahead log had to
        // take back out.
        //
        // AFTER the sequence probe above, not before it. That probe is what trims a tail a crash
        // left half-written, and only the piece being written is ever trimmed -- so rolling first
        // would seal the torn bytes into a piece nothing trims again, and the piece's recorded
        // end would be taken from the last record before them.
        roll_index_log_segment_if_due(&inner.root, shard_id)?;
        let next_sequence = last_sequence.saturating_add(1);
        // Do not write what the item already says. Each item carries `object_id` and
        // `routing_bucket`, and the address it points at repeats both -- 18 bytes of a 142-byte
        // item. `restore_address_repeats` at the decode site puts them back.
        let mut items = items;
        for item in items.iter_mut() {
            item.strip_block_ref_key_repeat();
            item.strip_size_repeat();
            // The address first, because it is stripped against the item's id, and the item's
            // id is what goes next.
            item.strip_address_repeats();
            item.strip_object_id_repeat(shard_id);
        }
        // Hoist the object key when every item names the same one, and blank the copies.
        //
        // LAST, after the strips above. `strip_block_ref_key_repeat` DERIVES the page handle from
        // the item's own fields and `object_key` is one of them, so blanking the key first would
        // derive a different handle and the strip would decline -- costing more than this saves.
        let shared_object_key = match items.split_first() {
            Some((first, rest)) if rest.iter().all(|item| item.object_key == first.object_key) => {
                Some(first.object_key.clone())
            }
            _ => None,
        };
        if shared_object_key.is_some() {
            for item in items.iter_mut() {
                item.object_key.clear();
            }
        }
        let record = IndexDeltaRecord {
            shard_id,
            sequence: next_sequence,
            items,
            meta,
            applied_wal_sequence,
            key_states,
            upsert,
            shared_object_key,
        };
        // Frame the delta record with a length + SHA-256 digest (crate::log_framing) so a
        // value-preserving bit-flip (e.g. a flipped `deleted` flag or page address) in this
        // committed line is detected on read rather than replayed as truth on recovery.
        let bytes =
            crate::log_framing::encode_record(&encode_index_payload(&record, INDEX_LOG_SHAPE_DELTA)?);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(index_log_path(&inner.root, shard_id))?;
        file.write_all(&bytes)?;
        file.flush()?;
        inner.stats.writes += 1;
        inner.stats.bytes_written += bytes.len() as u64;
        inner.stats.last_sequence = next_sequence;
        inner.last_sequence_by_shard.insert(shard_id, next_sequence);
        // The bytes are in the file and the bookkeeping is done, so claim a barrier and then let
        // the lock go before taking it. Holding the lock across the fsync meant writers could
        // never reach the barrier together, so each paid for one that would have covered all of
        // them.
        let barrier = durable.then(|| {
            let gate = self.flush_gates.gate((shard_id as u64, 0));
            let ticket = gate.register_write();
            (gate, ticket)
        });
        drop(inner);
        if let Some((gate, ticket)) = barrier {
            gate.await_durable(ticket, || {
                crate::durability_metrics::record_barrier("engine_index_log_append");
                file.sync_data()
            })?;
        }
        Ok(next_sequence)
    }

    /// Read every delta record for a shard in log order. Records at or before
    /// `retain_after_sequence` (the base snapshot's anchor) are skipped, so replay applies
    /// only the suffix the base does not already reflect. Legacy whole-index
    /// (`IndexLogRecord`) lines are ignored here -- they are not delta records.
    pub fn read_delta_records(
        &self,
        shard_id: ShardId,
        retain_after_sequence: u64,
    ) -> Result<Vec<IndexDeltaRecord>, IndexLogError> {
        let mut records = Vec::new();
        self.for_each_delta_record(shard_id, retain_after_sequence, |record| {
            records.push(record);
        })?;
        Ok(records)
    }

    /// The same walk, handing each record over instead of collecting them.
    ///
    /// The file was already read a frame at a time, so what a caller held was never the FILE --
    /// it was every record the file DECODES to, which is the larger of the two and lives until
    /// the caller drops the vector. A caller that folds the records into one answer has no use
    /// for that, and the folding caller here runs on the load path, which is when a store can
    /// least afford to hold the log twice over.
    ///
    /// `read_delta_records` keeps its shape on top of this, so a caller that genuinely wants all
    /// of them -- the tests that assert what a log holds -- is unchanged.
    pub fn for_each_delta_record(
        &self,
        shard_id: ShardId,
        retain_after_sequence: u64,
        mut take: impl FnMut(IndexDeltaRecord),
    ) -> Result<(), IndexLogError> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        // Every piece of the log, oldest first. A log that has never rolled is one file, and this
        // is then the single path this fold has always read.
        //
        // `last_sequence` is carried ACROSS the pieces, not reset per piece: the continuity check
        // below is what refuses a holed delta stream, and per-piece it would stop seeing a hole
        // that falls on a boundary -- which is the only new place a hole can appear.
        let mut last_sequence = 0_u64;
        for path in index_log_segment_paths(&inner.root, shard_id) {
            if !path.exists() {
                continue;
            }
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            // Read by FRAME, not by line. A record's payload may be binary, and a binary payload
            // may contain 0x0A -- a reader splitting on newlines would cut such a record in half
            // and, being `lines()`, would also demand it be valid UTF-8. `read_frame` takes the
            // length the frame declares instead, and reads text-framed and legacy unframed
            // records unchanged, so one loop reads every shape the log has ever held. Streaming
            // rather than reading the file whole keeps memory bounded by the largest record.
            while let Some((_, payload)) = crate::log_framing::read_frame(&mut reader)? {
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                let payload = payload.as_slice();
                // PROPAGATE decode/parse failures instead of silently skipping the line. Silently
                // dropping an unparseable interior delta record and continuing the fold advances
                // the reconstructed anchor past it, so an eviction/removal recorded ONLY in that
                // delta (not the WAL) is recovered from neither source = silent loss / dangling
                // ref. `decode_line` also verifies the per-record integrity envelope, so a
                // value-preserving bit-flip surfaces here as `Corruption`.
                // Skip only a payload that SAYS it is the other shape. Anything else -- an
                // unrecognised shape, a truncated container -- goes to the decoder and is
                // reported, because a sweep that quietly skips what it cannot read is how
                // committed corruption becomes silent data loss.
                if index_payload_shape(payload) == Some(INDEX_LOG_SHAPE_WHOLE) {
                    continue;
                }
                let mut record: IndexDeltaRecord = decode_index_payload(payload)?;
                // Put back what the writer left out because the item already stated it. A record
                // written before that stripping carries both already, and this leaves those
                // alone. The hoisted key FIRST: everything below derives from the item's own
                // fields and `object_key` is one of them.
                if let Some(shared) = record.shared_object_key.clone() {
                    for item in record.items.iter_mut() {
                        item.object_key = shared.clone();
                    }
                }
                for item in record.items.iter_mut() {
                    // The item's id first: the address is restored FROM it.
                    item.restore_object_id_repeat(record.shard_id);
                    item.restore_address_repeats();
                    item.restore_block_ref_key_repeat();
                    item.restore_size_repeat();
                }
                // Enforce delta sequence-continuity: sequences are assigned strictly
                // monotonically across ALL appended records (whole-index and delta share one
                // counter), reclaim only ever removes a leading prefix, and pieces are read in
                // log order -- so each record's sequence must be strictly greater than the
                // previous one, boundaries included. A drop below or a duplicate means a lost /
                // reordered / corrupted record, or pieces read out of order; refuse rather than
                // fold a holed delta stream.
                if record.sequence <= last_sequence {
                    return Err(IndexLogError::Corruption(format!(
                        "index-log delta sequence continuity violation: record sequence {} is not greater than previous {}",
                        record.sequence, last_sequence
                    )));
                }
                last_sequence = record.sequence;
                // A whole-index IndexLogRecord also deserializes into IndexDeltaRecord (its
                // `index` field is ignored, leaving the delta fields empty). Only keep records
                // that carry a delta payload OR a WAL anchor (an anchor-only record still
                // advances the reconstructed watermark on load).
                if record.items.is_empty()
                    && record.meta.is_none()
                    && record.key_states.is_empty()
                    && record.applied_wal_sequence.is_none()
                {
                    continue;
                }
                if record.sequence > retain_after_sequence {
                    take(record);
                }
            }
        }
        Ok(())
    }

    /// Raw bytes of the log at `offset`.
    ///
    /// `offset` is a position in the LOG, not in whichever piece holds it: the pieces are read
    /// back to back, so a window that starts in one continues into the next rather than stopping
    /// at a boundary the caller cannot see.
    pub fn read_range(
        &self,
        shard_id: ShardId,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, IndexLogError> {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let mut bytes: Vec<u8> = Vec::new();
        let mut at = 0_u64;
        for path in index_log_segment_paths(&inner.root, shard_id) {
            if bytes.len() as u64 >= size {
                break;
            }
            let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            if length == 0 {
                continue;
            }
            // A piece that ends before the window starts holds nothing the caller asked for, and
            // skipping it costs a stat rather than a read.
            if at.saturating_add(length) <= offset {
                at = at.saturating_add(length);
                continue;
            }
            let start_in_piece = offset.saturating_sub(at);
            let want = size.saturating_sub(bytes.len() as u64);
            let mut file = File::open(&path)?;
            file.seek(SeekFrom::Start(start_in_piece))?;
            file.take(want).read_to_end(&mut bytes)?;
            at = at.saturating_add(length);
        }
        inner.stats.reads += 1;
        inner.stats.bytes_read += bytes.len() as u64;
        Ok(bytes)
    }

    /// The records in the window. Says nothing about whether the window was exhausted, which
    /// is why anything reporting completeness to a caller wants `scan_bounded` instead.
    pub fn scan(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IndexLogError> {
        self.scan_bounded(shard_id, start_offset, end_offset, max_bytes)
            .map(|(records, _)| records)
    }

    /// The records in the window, and whether `max_bytes` cut the scan short.
    ///
    /// The walk below stops for two unrelated reasons: the window ended, or the byte budget ran
    /// out. Returning only the records conflates them, and a caller that cannot tell them apart
    /// reports a truncated read as a complete one.
    pub fn scan_bounded(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
    ) -> Result<(Vec<(u64, Vec<u8>)>, bool), IndexLogError> {
        self.scan_collect(shard_id, start_offset, end_offset, max_bytes, |offset, raw| {
            (offset, raw)
        })
    }

    /// How many records the log holds, without building them.
    ///
    /// The caller that wanted this asked `scan(.., u64::MAX, u64::MAX)` and took `.len()` of the
    /// result, so it read the whole log into a vector to learn how many records were in it --
    /// on the plan path of every maintenance round.
    ///
    /// The same walk, projecting to `()`. A zero-sized element costs nothing per record, so peak
    /// memory is one record rather than all of them, and the count cannot drift from what a scan
    /// would have returned because it IS the scan. The write-ahead log store has the twin of
    /// this, for the same caller.
    pub fn record_count(&self, shard_id: ShardId) -> Result<usize, IndexLogError> {
        self.scan_collect(shard_id, 0, u64::MAX, u64::MAX, |_offset, _raw| ())
            .map(|(records, _truncated)| records.len())
    }

    /// The one walk every scan of this log shares, so they cannot drift about what a window
    /// contains. Mirrors `scan_collect` on the write-ahead log store.
    fn scan_collect<T>(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
        mut take: impl FnMut(u64, Vec<u8>) -> T,
    ) -> Result<(Vec<T>, bool), IndexLogError> {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let segments = index_log_segment_paths(&inner.root, shard_id)
            .into_iter()
            .filter(|path| path.exists())
            .collect::<Vec<_>>();
        if segments.is_empty() {
            inner.stats.scans += 1;
            return Ok((Vec::new(), false));
        }
        let _ = last_sequence_at(&inner.root, shard_id)?;
        // A record's position is where it sits in the LOG, counted across the pieces in order --
        // not an offset into whichever file holds it, which would mean nothing to a caller once
        // there is more than one.
        let mut offset = 0_u64;
        let mut total = 0;
        let mut truncated = false;
        let mut records = Vec::new();

        'segments: for path in segments {
            let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            if offset.saturating_add(length) <= start_offset {
                offset = offset.saturating_add(length);
                continue;
            }
            let mut file = File::open(&path)?;
            let skip = start_offset.saturating_sub(offset);
            file.seek(SeekFrom::Start(skip))?;
            offset = offset.saturating_add(skip);
            let mut reader = BufReader::new(file);

            // Walk by RECORD, not by newline. What this returns is each record's raw framed
            // bytes -- the caller ships them onward untouched -- so the walk has to agree with
            // the writer about where a record ends. A newline scan decides that from a delimiter
            // a binary payload may itself contain, and would hand the caller half a record that
            // still looks like a whole one. The write-ahead log walks its own records with this
            // same reader, for this same reason.
            while let Some(raw) = crate::log_framing::read_raw_record(&mut reader)? {
                let read = raw.len() as u64;
                let next_offset = offset.saturating_add(read);
                if next_offset > end_offset {
                    break 'segments;
                }
                if total + read > max_bytes {
                    // Out of budget with the window not yet walked: there is more to read.
                    truncated = true;
                    break 'segments;
                }
                records.push(take(offset, raw));
                offset = next_offset;
                total += read;
            }
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += total;
        inner.stats.records_read += records.len() as u64;
        Ok((records, truncated))
    }

    /// Answer the index-GC gate from the piece NAMES plus the piece being written.
    ///
    /// The gate wants three numbers -- the log's size, how many records it holds, and how many of
    /// them sit below a retain floor -- and used to get the last two by scanning the whole log and
    /// decoding every record. That is the cost `drop_covered_index_segments` stopped paying: a
    /// sealed piece's name already carries `start`, `end` and its highest WAL anchor, so
    ///
    /// - the piece holds `end - start` records, because sequences are assigned `last + 1` with no
    ///   holes -- the same arithmetic a round already reports its removals with, so the gate and the
    ///   collector cannot disagree about a piece;
    /// - the records in it below a floor are `min(end, floor) - start`, saturating, for exactly the
    ///   same reason: contiguity makes a count out of a subtraction, and a floor at or under
    ///   `start` leaves none.
    ///
    /// Only the piece being WRITTEN has no numbers in its name, so only that one is opened. It is
    /// at most the rolling threshold (64 KiB shipped), whatever the log has grown to, which is
    /// what makes this cost the number of PIECES rather than the number of records.
    ///
    /// THE ONE SHAPE THIS DOES NOT BOUND, said plainly rather than left to be discovered: a log
    /// that has never rolled is ONE piece and that piece is the whole log, so this decodes all of
    /// it -- exactly what the old gate did, no better and no worse. `TS_INDEX_LOG_SEGMENT_BYTES=0`
    /// and any store written before pieces are that shape. The bound is on the ROLLED log, and a
    /// guard asserting it has to say how many pieces its fixture actually had.
    ///
    /// Infallible on purpose. This is a GC-pressure metric, not the recovery path: a corrupt frame
    /// in the piece being written stops the walk and leaves the sealed pieces counted, where
    /// returning an error would have the caller read the whole log as EMPTY -- which reads as a
    /// log with nothing to collect rather than as a log that could not be measured. Replay is
    /// where a corrupt record is surfaced, and it still surfaces it.
    pub fn gate_summary(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> IndexLogGateSummary {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let root = inner.root.clone();
        let active = index_log_path(&root, shard_id);
        let mut summary = IndexLogGateSummary {
            shard_id,
            retain_from_sequence,
            ..IndexLogGateSummary::default()
        };
        for path in index_log_segment_paths(&root, shard_id) {
            // A `stat`, and for a sealed piece that is the whole of it. A piece that has just been
            // unlinked out from under this walk is simply not counted, the same way
            // `index_log_total_bytes` does not count it.
            let Ok(metadata) = path.metadata() else {
                continue;
            };
            summary.pieces += 1;
            summary.bytes = summary.bytes.saturating_add(metadata.len());
            if path != active {
                let Some(span) = sealed_index_log_span(&path, shard_id) else {
                    continue;
                };
                summary.pieces_named += 1;
                let held = span.end.saturating_sub(span.start);
                let removable = span.end.min(retain_from_sequence).saturating_sub(span.start);
                summary.records = summary.records.saturating_add(held as usize);
                summary.removable_records = summary
                    .removable_records
                    .saturating_add(removable as usize);
                continue;
            }
            let Ok(file) = File::open(&path) else {
                continue;
            };
            let mut reader = BufReader::new(file);
            // By FRAME, not by newline, for the reason every other walk of this log gives: a
            // binary payload may contain the delimiter a newline scan would stop at.
            while let Ok(Some((frame_bytes, payload))) = crate::log_framing::read_frame(&mut reader)
            {
                summary.bytes_decoded = summary.bytes_decoded.saturating_add(frame_bytes as u64);
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                summary.records_decoded += 1;
                summary.records += 1;
                // Through the index log's own decoder, and only the HEAD of the record: two shapes
                // share this log and both carry a sequence. A record that will not decode is
                // COUNTED but not called removable, which is what the scan this replaces did -- it
                // makes the ratio smaller, so it can only ever decline a round, never fire one.
                if let Ok(head) = decode_index_payload::<IndexRecordHead>(&payload) {
                    if head.sequence < retain_from_sequence {
                        summary.removable_records += 1;
                    }
                }
            }
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += summary.bytes_decoded;
        inner.stats.records_read += summary.records_decoded as u64;
        summary
    }

    /// How many files this shard's log is in, the one being written included.
    pub fn piece_count(&self, shard_id: ShardId) -> usize {
        let inner = self.inner.lock().expect("index log lock poisoned");
        index_log_segment_paths(&inner.root, shard_id)
            .into_iter()
            .filter(|path| path.exists())
            .count()
    }

    pub fn gc_before_sequence(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        self.gc_before_sequence_limited(shard_id, retain_from_sequence, 0)
    }

    pub fn gc_before_sequence_limited(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
        // The bound applies to the REWRITE of the piece being written, which is the only part of
        // a round that copies anything. Whole earlier pieces are unlinked, and that is not work a
        // round needs protecting from: a stat and an unlink each, whatever they hold.
        //
        // It used to bound the whole round, and bounding the whole round cost MORE: the round
        // rewrote what it retained, so removing fewer records meant copying more of them --
        // 40,000 records were 357 ms in one unlimited round against 492 ms limited to 200. That
        // inversion is what the pieces remove; see
        // `reclaim_costs_what_it_removes_not_what_it_keeps`.
        max_entries_per_round: usize,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let root = inner.root.clone();
        let path = index_log_path(&root, shard_id);
        if !index_log_segment_paths(&root, shard_id)
            .iter()
            .any(|piece| piece.exists())
        {
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence,
                max_entries_per_round,
                ..IndexLogGcReport::default()
            });
        }

        let bytes_before = index_log_total_bytes(&root, shard_id);
        // Whole earlier pieces first. A piece whose sequences all sit below the floor is unlinked
        // without being opened, so what it HELD costs nothing -- which is the point: this round
        // used to pay for every record it kept.
        let (dropped_segments, dropped_segment_bytes, dropped_records) =
            drop_covered_index_segments(&root, shard_id, retain_from_sequence)?;

        let _ = last_sequence_at(&root, shard_id)?;
        let mut records_before = 0usize;
        let mut removed_this_round = 0usize;
        let mut removable_records_before_budget = 0usize;
        let mut retained = Vec::new();
        let mut reclaimable_bytes = 0u64;
        if path.exists() {
            let file = File::open(&path)?;
            // By frame, not by line: see the fold path above.
            let mut reader = BufReader::new(file);
            while let Some((frame_bytes, payload)) = crate::log_framing::read_frame(&mut reader)? {
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                records_before += 1;
                // Preserve the exact on-disk payload for retained records so a delta record is
                // not silently re-encoded as a whole-index record (which would drop its
                // items/meta). Decode verifies the integrity envelope; the retained raw payload
                // is re-framed on write-out below.
                let record: IndexRecordHead = decode_index_payload(&payload)?;
                if record.sequence < retain_from_sequence {
                    removable_records_before_budget =
                        removable_records_before_budget.saturating_add(1);
                }
                if record.sequence >= retain_from_sequence
                    || (max_entries_per_round > 0 && removed_this_round >= max_entries_per_round)
                {
                    retained.push(payload);
                } else {
                    removed_this_round = removed_this_round.saturating_add(1);
                    reclaimable_bytes = reclaimable_bytes.saturating_add(frame_bytes as u64);
                }
            }
        }

        // Nothing in the piece being written is reclaimable, which is the ordinary case once the
        // log rolls: the removable prefix lives in the pieces that were just unlinked. Rewriting
        // the piece would write it back byte for byte and take a barrier to do it.
        let mut bytes_copied = 0u64;
        let rewrite_skipped = removed_this_round == 0;
        if !rewrite_skipped {
            let temp_path = path.with_extension("gc.tmp");
            {
                let mut temp = File::create(&temp_path)?;
                for payload in &retained {
                    let framed = crate::log_framing::encode_record(payload);
                    bytes_copied = bytes_copied.saturating_add(framed.len() as u64);
                    temp.write_all(&framed)?;
                }
                temp.flush()?;
                crate::durability_metrics::record_barrier("engine_index_log_gc");
                temp.sync_all()?;
            }
            fs::rename(&temp_path, &path)?;
            sync_parent_dir(&path)?;
        }
        let bytes_after = index_log_total_bytes(&root, shard_id);
        let removed_from_active = records_before.saturating_sub(retained.len());
        Ok(IndexLogGcReport {
            shard_id,
            retain_from_sequence,
            max_entries_per_round,
            records_before: dropped_records.saturating_add(records_before),
            records_after: retained.len(),
            records_removed: dropped_records.saturating_add(removed_from_active),
            removable_records_before_budget: dropped_records
                .saturating_add(removable_records_before_budget),
            budget_exhausted: max_entries_per_round > 0
                && removable_records_before_budget > max_entries_per_round,
            bytes_before,
            bytes_after,
            reclaimable_bytes: dropped_segment_bytes.saturating_add(reclaimable_bytes),
            rewrite_skipped,
            bytes_copied,
            dropped_segments,
            dropped_segment_bytes,
        })
    }

    /// Remove every record a completed catalog dump has made redundant, deciding retention per
    /// record on CONTENT rather than on log position alone.
    ///
    /// A dump durably materializes the base served index at WAL anchor `wal_anchor` and then
    /// appends its folded catalog anchor, which lands at index-log sequence `meta_sequence`.
    /// Everything the base reflects -- records whose own WAL anchor is at or below `wal_anchor`
    /// -- is redundant on load (the fold skips them), so it goes. Two kinds of record sit below
    /// `meta_sequence` yet must SURVIVE:
    ///
    /// - a delta a concurrent writer appended between the dump's serialization and its anchor
    ///   append: its WAL anchor is above `wal_anchor`, so the base does not reflect it, and
    ///   removing it would lose an eviction/removal that lives only in the delta stream;
    /// - nothing else -- a legacy whole-index line carries no anchor and is read by no load
    ///   path, so it is treated as reflected and removed.
    ///
    /// The folded catalog anchor itself is at `meta_sequence`, above the removal window, so the
    /// load-time catalog seed always survives. Position (`sequence < meta_sequence`) still
    /// bounds the sweep so a record appended AFTER the dump with a stale-looking anchor is never
    /// touched.
    /// `min_reclaimable_bytes` is the least the sweep must be able to reclaim before it will
    /// rewrite the log. Below it -- and always when nothing at all is reclaimable -- the log is
    /// left exactly as it is and the report says so.
    pub fn gc_reflected_before_anchor(
        &self,
        shard_id: ShardId,
        wal_anchor: u64,
        meta_sequence: u64,
        min_reclaimable_bytes: u64,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let root = inner.root.clone();
        let path = index_log_path(&root, shard_id);
        if !index_log_segment_paths(&root, shard_id)
            .iter()
            .any(|piece| piece.exists())
        {
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence: meta_sequence,
                ..IndexLogGcReport::default()
            });
        }

        let bytes_before = index_log_total_bytes(&root, shard_id);
        // Whole earlier pieces first, and unconditionally: the threshold below exists to decline
        // a REWRITE that copies almost everything to reclaim almost nothing, and an unlink copies
        // nothing at all. A piece only goes when its name says every record in it is both below
        // the position bound and reflected by the base the dump wrote.
        let (dropped_segments, dropped_segment_bytes, dropped_records) =
            drop_reflected_index_segments(&root, shard_id, wal_anchor, meta_sequence)?;

        let _ = last_sequence_at(&root, shard_id)?;
        let mut records_before = 0usize;
        let mut retained = Vec::new();
        // `read_frame` hands back how many bytes the record OCCUPIED, which this loop used to
        // discard. It is the exact on-disk size of what a rewrite would drop, so the threshold
        // below is measured in the same bytes the file is measured in rather than in payloads.
        let mut reclaimable_bytes = 0u64;
        if path.exists() {
            let file = File::open(&path)?;
            // By frame, not by line: see the fold path above.
            let mut reader = BufReader::new(file);
            while let Some((frame_bytes, payload)) = crate::log_framing::read_frame(&mut reader)? {
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                records_before += 1;
                // Decode verifies the integrity envelope; the retained raw payload is re-framed
                // on write-out below, so a retained delta record keeps its exact on-disk bytes.
                let probe: IndexRecordHead = decode_index_payload(&payload)?;
                let reflected = probe.applied_wal_sequence.unwrap_or(0) <= wal_anchor;
                if probe.sequence >= meta_sequence || !reflected {
                    retained.push(payload);
                } else {
                    reclaimable_bytes = reclaimable_bytes.saturating_add(frame_bytes as u64);
                }
            }
        }

        // Rewriting while retaining every record writes the file back byte for byte, so the
        // barrier and the rename buy nothing at all -- that half is arithmetic, not policy. The
        // threshold is the policy: a rewrite that reclaims a handful of bytes still costs a full
        // read, a full write and an fsync, and the post-dump sweep runs on every dump.
        let removable = records_before.saturating_sub(retained.len());
        if removable == 0 || reclaimable_bytes < min_reclaimable_bytes {
            let bytes_after = index_log_total_bytes(&root, shard_id);
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence: meta_sequence,
                records_before: dropped_records.saturating_add(records_before),
                records_after: records_before,
                records_removed: dropped_records,
                removable_records_before_budget: dropped_records.saturating_add(removable),
                bytes_before,
                bytes_after,
                reclaimable_bytes: dropped_segment_bytes.saturating_add(reclaimable_bytes),
                rewrite_skipped: true,
                bytes_copied: 0,
                dropped_segments,
                dropped_segment_bytes,
                ..IndexLogGcReport::default()
            });
        }

        let temp_path = path.with_extension("gc.tmp");
        let mut bytes_copied = 0u64;
        {
            let mut temp = File::create(&temp_path)?;
            for payload in &retained {
                let framed = crate::log_framing::encode_record(payload);
                bytes_copied = bytes_copied.saturating_add(framed.len() as u64);
                temp.write_all(&framed)?;
            }
            temp.flush()?;
            crate::durability_metrics::record_barrier("engine_index_log_gc");
            temp.sync_all()?;
        }
        fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        let bytes_after = index_log_total_bytes(&root, shard_id);
        Ok(IndexLogGcReport {
            shard_id,
            retain_from_sequence: meta_sequence,
            max_entries_per_round: 0,
            records_before: dropped_records.saturating_add(records_before),
            records_after: retained.len(),
            records_removed: dropped_records.saturating_add(removable),
            removable_records_before_budget: dropped_records.saturating_add(removable),
            budget_exhausted: false,
            bytes_before,
            bytes_after,
            reclaimable_bytes: dropped_segment_bytes.saturating_add(reclaimable_bytes),
            rewrite_skipped: false,
            bytes_copied,
            dropped_segments,
            dropped_segment_bytes,
        })
    }

    pub fn stats(&self, shard_id: ShardId) -> IndexLogStats {
        let inner = self.inner.lock().expect("index log lock poisoned");
        IndexLogStats {
            last_sequence: last_sequence_at(&inner.root, shard_id).unwrap_or_default(),
            ..inner.stats
        }
    }
}

impl Default for LocalIndexLogStore {
    fn default() -> Self {
        let scratch = crate::scratch::owned_scratch_dir("index-logs");
        let store = Self::new(scratch.path());
        store
            .inner
            .lock()
            .expect("index log lock poisoned")
            .scratch = Some(scratch);
        store
    }
}

/// The suffix an index log is written with.
///
/// Its records are binary -- the same framing the write-ahead log uses, a frame magic then a
/// payload -- and have been since the log stopped encoding JSON. A store written before this
/// carries the older name, which claimed a format it no longer held.
const INDEX_LOG_SUFFIX: &str = "bin";

/// What the same file used to be called. Still read, never written.
///
/// Dropping it rather than keeping it would find no index log where one exists, which reads as an
/// empty log rather than an error -- and an empty index log is a silently emptier shard.
const LEGACY_INDEX_LOG_SUFFIX: &str = "jsonl";

fn index_log_path(root: &Path, shard_id: ShardId) -> PathBuf {
    let renamed = root.join(format!("shard-{shard_id}.indexlog.{INDEX_LOG_SUFFIX}"));
    if renamed.exists() {
        return renamed;
    }
    // An existing store keeps the name it already has, so one shard's log is never split across
    // two names. A store with neither is new, and starts under the current one.
    let legacy = root.join(format!("shard-{shard_id}.indexlog.{LEGACY_INDEX_LOG_SUFFIX}"));
    if legacy.exists() {
        return legacy;
    }
    renamed
}

fn last_sequence_at(root: &Path, shard_id: ShardId) -> Result<u64, IndexLogError> {
    // A sealed piece SAYS what its last sequence is, in its name. It was sealed after a complete
    // append, so the number is authoritative -- and reading the piece back to learn it would make
    // every sequence probe cost the log's whole history again, which is the cost the pieces exist
    // to remove.
    let mut last = 0_u64;
    for piece in index_log_segment_paths(root, shard_id) {
        if let Some(span) = sealed_index_log_span(&piece, shard_id) {
            last = last.max(span.end.saturating_sub(1));
        }
    }
    let path = index_log_path(root, shard_id);
    if !path.exists() {
        return Ok(last);
    }
    // Only the piece being written can have a torn tail: a sealed piece was made durable and
    // renamed after a whole record landed, and nothing appends to it afterwards. So this trims
    // the active piece, exactly as it trimmed the single file before there were pieces.
    let file = OpenOptions::new().read(true).write(true).open(&path)?;
    let mut reader = BufReader::new(file.try_clone()?);
    let mut good_offset = 0_u64;
    loop {
        // By FRAME, not by newline. This function truncates: it trims the file back to the
        // last whole record. A newline scan decides where records end by looking for a
        // delimiter, which a binary payload may legitimately contain -- so it would find an
        // end that is not one, call the remainder torn, and set_len durable records away.
        // `read_frame` takes the length the record declares, and still reads text-framed and
        // legacy unframed records, so what counts as "whole" no longer depends on the payload
        // encoding. Mirrors wal.rs::last_wal_sequence_at.
        match crate::log_framing::read_frame(&mut reader) {
            // A complete record. Whitespace-only filler advances the good offset without
            // being parsed, exactly as the newline scan did.
            Ok(Some((consumed, payload))) => {
                good_offset = good_offset.saturating_add(consumed as u64);
                if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                    continue;
                }
                // A COMPLETE record that fails to parse is committed corruption, not a torn
                // tail. Treating it as end-of-log would set_len the file down to the last
                // parseable record -- silently dropping durable index-log records after the
                // corrupt one AND rewinding the sequence counter (the next append reuses a
                // sequence that durable dump manifests already cite). Surface it as an error;
                // index-log replay returns DataLoss on a hole or digest mismatch, never trims.
                let record = decode_index_payload::<IndexRecordHead>(&payload)?;
                last = last.max(record.sequence);
            }
            // Nothing further, or fewer bytes than the record declares: a crash mid-append.
            // That tail is what `good_offset` trims below.
            Ok(None) => break,
            // A whole record whose digest does not match its payload: committed damage.
            Err(err) => return Err(IndexLogError::Corruption(err.0)),
        }
    }
    if good_offset < file.metadata()?.len() {
        file.set_len(good_offset)?;
        crate::durability_metrics::record_barrier("engine_index_log_seq_probe");
        file.sync_all()?;
        sync_parent_dir(&path)?;
    }
    Ok(last)
}

/// TS_INDEX_LOG_SEGMENT_BYTES: roll the index log into a new piece once the one being written
/// passes this many bytes.
///
/// Zero never rolls, which is one file -- the shape every store written before this has, and the
/// shape this code still reads.
///
/// Rolling is what makes reclaim cost what it REMOVES. One file per shard has to be reclaimed by
/// reading it and rewriting every record it keeps, so a round that removes little copies almost
/// everything: 40,000 records took 357 ms in one unlimited round against 492 ms limited to 200 --
/// bounding the round made it DEARER, because the bound is on what it removes and the cost is on
/// what it retains. In pieces the removal is an unlink: a stat and a `remove_file`, whatever the
/// piece holds, and nothing is read or copied.
///
/// **Default 64 KiB**, chosen against the gate that lets this log grow rather than by feel. Index
/// GC fires at `DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD`, 768 KiB, and only when at least 40%
/// of the records are removable -- so the log a round actually meets is about 768 KiB with a
/// removable prefix of about 300 KiB. A round can only unlink pieces that lie WHOLLY inside that
/// prefix, so the piece size is the granularity of reclaim:
///
/// | piece | pieces in 768 KiB | unlinked from a 40% prefix |
/// |---|---|---|
/// | 256 KiB (the write-ahead log's) | 3 | 1 -- a third of the log |
/// | 64 KiB | 12 | 4 -- 87% of what is removable |
/// | 8 KiB | 96 | 38 -- 99%, and 96 names per shard |
///
/// 64 KiB is where the granularity stops being the limit and the file count has not yet started
/// to be one. The write-ahead log's 256 KiB is larger because it is pinned to its preallocation
/// chunk, which this log does not have.
fn index_log_segment_bytes() -> u64 {
    if let Some(threshold) = INDEX_SEGMENT_BYTES_OVERRIDE.with(|value| value.get()) {
        return threshold;
    }
    std::env::var("TS_INDEX_LOG_SEGMENT_BYTES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(DEFAULT_INDEX_LOG_SEGMENT_BYTES)
}

/// Rolling threshold when nothing sets one. See [`index_log_segment_bytes`].
pub const DEFAULT_INDEX_LOG_SEGMENT_BYTES: u64 = 64 * 1024;

thread_local! {
    /// Per-thread override of the rolling threshold.
    ///
    /// Per thread, not per process: appending happens on the calling thread, and a test that set
    /// a process-wide threshold would make every other test running beside it roll too.
    static INDEX_SEGMENT_BYTES_OVERRIDE: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
}

/// Set the rolling threshold for THIS THREAD. The environment variable is the supported way to
/// set it; this exists so a test can roll without disturbing anything running beside it.
pub fn set_index_log_segment_bytes_for_test(threshold: Option<u64>) {
    INDEX_SEGMENT_BYTES_OVERRIDE.with(|value| value.set(threshold));
}

/// What a sealed piece of the index log holds -- read from its NAME, not from the file.
///
/// Reclaim decides entirely from these three numbers, so deciding costs a `read_dir` entry rather
/// than a pass over the piece. They are written when the piece is sealed, which happens after a
/// complete append, so each one is final.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexSegmentSpan {
    /// The first sequence in the piece.
    start: u64,
    /// One PAST the last sequence in the piece: the piece holds `start..end`. Sequences are
    /// assigned `last + 1` with no gaps, so `end - start` is also how many records it holds --
    /// which is what lets a round report what it removed without reading what it removed.
    end: u64,
    /// The highest WAL anchor any record in the piece carries, 0 when none does. The post-dump
    /// sweep keeps a record the dumped base does not reflect, and this is what says at a glance
    /// whether a whole piece is reflected.
    max_applied_wal: u64,
}

fn sealed_index_log_path(root: &Path, shard_id: ShardId, span: IndexSegmentSpan) -> PathBuf {
    // Zero-padded so the names sort into log order, which is the order the pieces are read in.
    root.join(format!(
        "shard-{shard_id}.indexlog.{:020}-{:020}-{:020}.{INDEX_LOG_SUFFIX}",
        span.start, span.end, span.max_applied_wal
    ))
}

/// Whether this file is a sealed piece of the given shard's index log, and what it holds.
///
/// The piece being written has no numbers in its name, so it is not one of these.
fn sealed_index_log_span(path: &Path, shard_id: ShardId) -> Option<IndexSegmentSpan> {
    let name = path.file_name()?.to_str()?;
    let middle = name.strip_prefix(&format!("shard-{shard_id}.indexlog."))?;
    // Either suffix is a piece of the log. A store part-way through the rename holds both, and
    // reading only one of them would silently skip whichever half it did not recognise.
    let middle = middle
        .strip_suffix(&format!(".{INDEX_LOG_SUFFIX}"))
        .or_else(|| middle.strip_suffix(&format!(".{LEGACY_INDEX_LOG_SUFFIX}")))?;
    let mut parts = middle.split('-');
    let start = parts.next()?.parse().ok()?;
    let end = parts.next()?.parse().ok()?;
    let max_applied_wal = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(IndexSegmentSpan {
        start,
        end,
        max_applied_wal,
    })
}

/// Every file that makes up a shard's index log, oldest first, with the one being written last.
///
/// A log that has never rolled is one file, and this returns just that -- the same path the rest
/// of the code has always used, which is what makes a store written before pieces still load.
fn index_log_segment_paths(root: &Path, shard_id: ShardId) -> Vec<PathBuf> {
    let mut sealed = fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter_map(|path| sealed_index_log_span(&path, shard_id).map(|span| (span.start, path)))
        .collect::<Vec<_>>();
    sealed.sort_by_key(|(start, _)| *start);
    let mut paths = sealed.into_iter().map(|(_, path)| path).collect::<Vec<_>>();
    paths.push(index_log_path(root, shard_id));
    paths
}

/// Bytes of every piece of this shard's index log, sealed pieces included.
fn index_log_total_bytes(root: &Path, shard_id: ShardId) -> u64 {
    index_log_segment_paths(root, shard_id)
        .into_iter()
        .filter_map(|path| path.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

/// What the piece being written holds, by walking it.
///
/// Called once per ROLL -- the piece is at most the rolling threshold, and this runs once in the
/// thousands of appends that fill it. Deliberately NOT called per append: the write-ahead log
/// opened its file and read its header on every write to answer a question of the same shape,
/// which was 8,240 bytes and an open per record before it was taken back out.
///
/// `None` when the piece holds no record at all, which is nothing to seal.
fn index_log_segment_span_of(path: &Path) -> Result<Option<IndexSegmentSpan>, IndexLogError> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut first = None;
    let mut last = 0_u64;
    let mut max_applied_wal = 0_u64;
    while let Some((_, payload)) = crate::log_framing::read_frame(&mut reader)? {
        if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        let head: IndexRecordHead = decode_index_payload(&payload)?;
        first.get_or_insert(head.sequence);
        last = last.max(head.sequence);
        max_applied_wal = max_applied_wal.max(head.applied_wal_sequence.unwrap_or(0));
    }
    Ok(first.map(|start| IndexSegmentSpan {
        start,
        end: last.saturating_add(1),
        max_applied_wal,
    }))
}

/// Seal the piece being written and start a fresh one, if it has grown past the threshold.
///
/// Called with the append lock held, before the append opens the file -- so no handle is left
/// pointing at a piece across the rename, and the record about to be written lands in the new
/// piece rather than growing the one just sealed.
fn roll_index_log_segment_if_due(root: &Path, shard_id: ShardId) -> Result<bool, IndexLogError> {
    let threshold = index_log_segment_bytes();
    if threshold == 0 {
        return Ok(false);
    }
    let path = index_log_path(root, shard_id);
    // A stat, not a read. This runs on EVERY append and is the whole per-append cost of rolling.
    let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    if length < threshold {
        return Ok(false);
    }
    let Some(span) = index_log_segment_span_of(&path)? else {
        return Ok(false);
    };

    // Make the piece durable BEFORE sealing it. Index-log appends defer their fsync under the
    // single-barrier default, so the piece can hold bytes no barrier has covered -- and the next
    // barrier opens the piece being written, which after this rename is a different file. Sealing
    // first would leave those bytes with no barrier that ever covers them, on a write that has
    // already been acked.
    {
        let file = OpenOptions::new().write(true).open(&path)?;
        crate::durability_metrics::record_barrier("engine_index_log_seal");
        file.sync_all()?;
    }

    // Seal by rename: atomic, so the piece is either being written or sealed, never neither. The
    // next append finds no file under the active name and creates an empty one.
    let sealed = sealed_index_log_path(root, shard_id, span);
    fs::rename(&path, &sealed)?;
    sync_parent_dir(&sealed)?;
    Ok(true)
}

/// Drop whole pieces that hold nothing at or above the retain floor.
///
/// A piece's name says the sequence one past its last record, so a piece is below the floor when
/// that number is at or below it -- decided without opening the file. Stops at the first piece
/// that still holds something: the pieces are in order, so everything after it does too.
///
/// Returns how many pieces went, what they held on disk, and how many records they held.
fn drop_covered_index_segments(
    root: &Path,
    shard_id: ShardId,
    retain_from_sequence: u64,
) -> Result<(usize, u64, usize), IndexLogError> {
    let active = index_log_path(root, shard_id);
    let mut dropped = 0usize;
    let mut freed = 0u64;
    let mut records = 0usize;
    for path in index_log_segment_paths(root, shard_id) {
        if path == active {
            continue;
        }
        let Some(span) = sealed_index_log_span(&path, shard_id) else {
            continue;
        };
        if span.end > retain_from_sequence {
            break;
        }
        freed = freed.saturating_add(path.metadata().map(|meta| meta.len()).unwrap_or(0));
        // Sequences are assigned with no gaps, so the span IS the record count. Counting them by
        // reading the piece would put the cost of a round back on the bytes it removes.
        records = records.saturating_add(span.end.saturating_sub(span.start) as usize);
        fs::remove_file(&path)?;
        dropped += 1;
    }
    // Once after the loop, not once per file. The write-ahead log's reclaim does the same.
    if dropped > 0 {
        sync_parent_dir(&active)?;
    }
    Ok((dropped, freed, records))
}

/// Drop whole pieces a completed catalog dump has made redundant.
///
/// Two conditions, and a piece's name carries both: every sequence in it is below the sweep's
/// position bound, and the highest WAL anchor it holds is one the dumped base already reflects.
/// Anything unclear leaves the piece alone -- unlinking a piece that still holds the only record
/// of an eviction cannot be undone.
fn drop_reflected_index_segments(
    root: &Path,
    shard_id: ShardId,
    wal_anchor: u64,
    meta_sequence: u64,
) -> Result<(usize, u64, usize), IndexLogError> {
    let active = index_log_path(root, shard_id);
    let mut dropped = 0usize;
    let mut freed = 0u64;
    let mut records = 0usize;
    for path in index_log_segment_paths(root, shard_id) {
        if path == active {
            continue;
        }
        let Some(span) = sealed_index_log_span(&path, shard_id) else {
            continue;
        };
        if span.end > meta_sequence || span.max_applied_wal > wal_anchor {
            break;
        }
        freed = freed.saturating_add(path.metadata().map(|meta| meta.len()).unwrap_or(0));
        records = records.saturating_add(span.end.saturating_sub(span.start) as usize);
        fs::remove_file(&path)?;
        dropped += 1;
    }
    if dropped > 0 {
        sync_parent_dir(&active)?;
    }
    Ok((dropped, freed, records))
}

fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            crate::durability_metrics::record_barrier("engine_index_log_dir");
            dir.sync_all()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    /// The splicing appender and the parsing one must produce the SAME record bytes.
    ///
    /// The engine's index-log appends were routed to the splicing path to stop re-parsing and
    /// re-encoding a multi-megabyte index on every append. That is only safe while the two
    /// produce identical bytes, which holds because `IndexLogRecord` declares its fields in
    /// exactly the order the splice writes them. This test fails if either side drifts.
    /// The two appenders now differ ON PURPOSE: `append_index_bytes` writes a constant-size
    /// anchor, `append_json` embeds the whole index for callers that want the record to carry
    /// it. This pins that difference, so neither silently becomes the other.
    #[test]
    fn the_anchor_appender_writes_far_less_than_the_embedding_one() {
        let anchor_dir = tempfile::tempdir().unwrap();
        let embed_dir = tempfile::tempdir().unwrap();
        let anchoring = LocalIndexLogStore::new(anchor_dir.path());
        let embedding = LocalIndexLogStore::new(embed_dir.path());
        // An index big enough that copying it is obviously different from naming it.
        let mut index = br#"{"index_format_version":3,"strings":{"#.to_vec();
        for i in 0..500 {
            if i > 0 {
                index.push(b',');
            }
            index.extend_from_slice(format!("\"k{i:04}\":{{\"page_slab_id\":{i}}}").as_bytes());
        }
        index.extend_from_slice(b"}}");

        anchoring.append_index_bytes(11, &index).unwrap();
        embedding.append_json(11, &index).unwrap();

        let anchor_bytes = std::fs::read(index_log_path(anchor_dir.path(), 11)).unwrap();
        let embed_bytes = std::fs::read(index_log_path(embed_dir.path(), 11)).unwrap();
        assert!(
            anchor_bytes.len() < 250,
            "the anchor record should be constant-size (got {})",
            anchor_bytes.len()
        );
        // That the embedding appender carries the index was asserted by BYTE COUNT, which
        // was an assumption about the encoding rather than about the record: the container
        // spells a 500-key index in fewer bytes than its JSON does, so the comparison broke
        // while the property it meant to check still held. Decode the record and look.
        let embed_payload = crate::log_framing::next_frame(&embed_bytes)
            .unwrap()
            .expect("the embedding record is one whole frame")
            .1;
        let embedded: IndexLogRecord = decode_index_payload(embed_payload).unwrap();
        assert_eq!(
            embedded
                .index
                .get("strings")
                .and_then(|strings| strings.as_object())
                .map(|strings| strings.len()),
            Some(500),
            "the embedding appender still carries the whole index"
        );
        assert!(
            embed_bytes.len() > anchor_bytes.len() * 20,
            "anchoring must be dramatically smaller than embedding ({} vs {})",
            anchor_bytes.len(),
            embed_bytes.len()
        );
    }

    use super::*;

    #[test]
    fn default_store_scratch_dir_dies_with_the_last_clone() {
        let store = LocalIndexLogStore::default();
        let root = store.inner.lock().unwrap().root.clone();
        assert!(root.exists(), "Default must create its scratch dir");
        let clone = store.clone();
        drop(store);
        assert!(root.exists(), "a live clone must keep the scratch dir");
        drop(clone);
        assert!(!root.exists(), "the last clone's drop must remove the scratch dir");
    }

    #[test]
    fn gc_before_sequence_rewrites_index_log_with_retained_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for value in [1, 2, 3] {
            store
                .append_json(5, format!("{{\"value\":{value}}}").as_bytes())
                .unwrap();
        }

        let report = store.gc_before_sequence(5, 2).unwrap();
        assert_eq!(report.records_before, 3);
        assert_eq!(report.records_after, 2);
        assert_eq!(report.records_removed, 1);
        assert_eq!(store.stats(5).last_sequence, 3);
        let reopened = LocalIndexLogStore::new(dir.path());
        assert_eq!(reopened.stats(5).last_sequence, 3);
        assert_eq!(reopened.scan(5, 0, u64::MAX, u64::MAX).unwrap().len(), 2);
        store.append_json(5, b"{\"value\":4}").unwrap();
        assert_eq!(store.stats(5).last_sequence, 4);
    }

    /// A checkpoint record ANCHORS an index; it does not carry a second copy of it.
    #[test]
    fn append_index_bytes_writes_an_anchor_that_identifies_the_index_without_embedding_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let index = b"{\"value\":1}";
        let sequence = store.append_index_bytes(5, index).unwrap();
        assert_eq!(sequence, 1);
        let rows = store.scan(5, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(rows.len(), 1);
        let payload = crate::log_framing::decode_line(&rows[0].1).unwrap();

        // It says it is an anchor, and the tail scan still reads its sequence -- the scan
        // takes the first two elements of any row, and an anchor puts the same two first.
        assert_eq!(index_payload_shape(payload), Some(INDEX_LOG_SHAPE_ANCHOR));
        let head: IndexRecordHead = decode_index_payload(payload).unwrap();
        assert_eq!(head.shard_id, 5);
        assert_eq!(head.sequence, 1);

        // And it identifies exactly the index it anchors, by digest and length.
        let anchor: IndexAnchorRecord = decode_index_payload(payload).unwrap();
        let expected = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(index);
            hasher.finalize().to_vec()
        };
        assert_eq!(anchor.index_sha256, expected);
        assert_eq!(anchor.index_len, index.len() as u64);
        assert!(
            !payload
                .windows(index.len())
                .any(|window| window == index),
            "the index itself must not be copied into the log"
        );
        assert!(
            payload.len() < 64,
            "a row anchor is the digest and two numbers, not 160 bytes of JSON (got {} bytes)",
            payload.len()
        );
    }

    #[test]
    fn corrupt_tail_is_truncated_and_append_resumes_after_last_valid_index_log_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store.append_json(5, b"{\"value\":1}").unwrap();
        store.append_json(5, b"{\"value\":2}").unwrap();
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(index_log_path(dir.path(), 5))
                .unwrap();
            file.write_all(b"{\"shard_id\":5,\"sequence\":3").unwrap();
            file.sync_all().unwrap();
        }

        let reopened = LocalIndexLogStore::new(dir.path());
        assert_eq!(reopened.stats(5).last_sequence, 2);
        assert_eq!(reopened.scan(5, 0, u64::MAX, u64::MAX).unwrap().len(), 2);
        let record = reopened.append_json(5, b"{\"value\":3}").unwrap();
        assert_eq!(record.sequence, 3);
        assert_eq!(reopened.scan(5, 0, u64::MAX, u64::MAX).unwrap().len(), 3);
    }


    /// Split a log file into its records, whatever shape they are in.
    ///
    /// Tests that rewrite a log -- to corrupt one record, or to reorder them -- used to split
    /// on newlines. A record may now hold one, so they walk frames instead. Splitting on
    /// newlines here would silently produce fragments that are not records, and the tests
    /// would then "pass" while exercising nothing.
    fn split_records(bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut at = 0usize;
        while at < bytes.len() {
            match crate::log_framing::next_frame(&bytes[at..]) {
                Ok(Some((consumed, _))) => {
                    out.push(bytes[at..at + consumed].to_vec());
                    at += consumed;
                }
                _ => break,
            }
        }
        out
    }

    /// A well-formed record whose payload cannot be decoded: committed corruption rather than
    /// a torn tail. Framed the way the writer frames, so what fails is the DECODE -- which is
    /// the thing these tests are about. A raw text splice would fail the frame check instead,
    /// and would stop being a record at all once records stopped being lines.
    fn undecodable_record() -> Vec<u8> {
        crate::log_framing::encode_record(b"corrupt-not-a-record")
    }

    /// Encode the way the writer did BEFORE rows: a map, with absent fields left out.
    ///
    /// The decoder still takes this shape, and these tests are what keeps that true. A row cannot
    /// express a partial item -- that is the point of a row -- so a test about what happens to an
    /// absent field has to write the shape that could leave one out.
    fn encode_as_map<T: serde::Serialize>(record: &T, shape: u8) -> Vec<u8> {
        let mut packed = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut packed).with_struct_map();
        serde::Serialize::serialize(record, &mut serializer).expect("map encode");
        let mut out = vec![index_container_byte(INDEX_LOG_CODEC_MSGPACK, shape)];
        out.extend_from_slice(&packed);
        out
    }

    fn page_item(bucket: u32, key: &str, deleted: bool) -> IndexItem {
        IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: bucket,
            block_ref_key: key.to_string(),
            object_key: key.to_string(),
            model_id: "m".to_string(),
            component: None,
            object_id: 1,
            page_id: 0,
            address: None,
            size: 8,
            in_log: false,
            deleted,
        }
    }

    /// An object id that is the hash of the row's own fields is not written, and comes back.
    ///
    /// It is `stable_block_object_id` of the shard, the model, the key and the component -- and a
    /// row carries the last three while the record carries the shard. Nine bytes restating a
    /// hash of fields sitting beside it.
    #[test]
    fn a_row_does_not_write_the_object_id_it_can_derive() {
        let shard_id: ShardId = 7;
        let mut derivable = page_item(3, "tenant/1/object/9", false);
        derivable.object_id = crate::engine::hashing::stable_block_object_id(
            shard_id,
            &derivable.model_id,
            &derivable.object_key,
            derivable.component.as_deref(),
        );

        let mut stripped = derivable.clone();
        stripped.strip_object_id_repeat(shard_id);
        assert_eq!(stripped.object_id, 0, "a derivable id is not written");
        stripped.restore_object_id_repeat(shard_id);
        assert_eq!(
            stripped.object_id, derivable.object_id,
            "and it comes back as what it was"
        );

        // An id that is NOT the hash of this row is kept, because a reader could not put it
        // back. Being wrong here costs bytes, not correctness.
        let mut foreign = derivable.clone();
        foreign.object_id = derivable.object_id ^ 0xFFFF;
        let kept = foreign.clone();
        foreign.strip_object_id_repeat(shard_id);
        assert_eq!(
            foreign.object_id, kept.object_id,
            "an id that cannot be derived stays in the row"
        );
    }

    #[test]
    fn a_delta_record_written_with_the_old_field_names_still_loads() {
        // The record-level names are short now. `items` and `meta` are NOT among them: whole-index
        // and delta records are told apart on read by the PRESENCE of those two keys, so renaming
        // them breaks record-type detection rather than just the labels.
        let legacy = serde_json::json!({
            "shard_id": 7,
            "sequence": 3,
            "items": [],
            "applied_wal_sequence": 11,
            "upsert": true,
            "key_states": [{"key": "m:0"}]
        });
        let record: IndexDeltaRecord =
            serde_json::from_value(legacy).expect("a legacy delta record must load");
        assert_eq!(record.shard_id, 7);
        assert_eq!(record.sequence, 3);
        assert_eq!(record.applied_wal_sequence, Some(11));
        assert!(record.upsert);
        assert_eq!(record.key_states.len(), 1);
    }

    #[test]
    fn a_delta_record_still_announces_itself_by_its_items_key() {
        // The property the discriminator depends on: a written delta record carries a literal
        // `items` key, which is how a reader tells it from a whole-index line.
        let record = IndexDeltaRecord {
            shard_id: 7,
            sequence: 1,
            items: vec![page_item(1, "a", false)],
            meta: None,
            applied_wal_sequence: Some(2),
            upsert: true,
            key_states: Vec::new(),
            shared_object_key: None,
        };
        let encoded = serde_json::to_string(&record).unwrap();
        assert!(encoded.contains("\"items\""), "the discriminator must survive: {encoded}");
        // And the long record-level spellings are gone from the written form.
        for gone in ["shard_id", "applied_wal_sequence", "key_states"] {
            assert!(!encoded.contains(gone), "{gone} should not be written any more");
        }
        // Not vacuous: it still round-trips with its values.
        let back: IndexDeltaRecord = serde_json::from_str(&encoded).unwrap();
        assert_eq!(back.shard_id, 7);
        assert_eq!(back.applied_wal_sequence, Some(2));
    }

    #[test]
    fn append_delta_grows_log_by_only_the_changed_items() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let seq1 = store
            .append_delta(7, vec![page_item(1, "a", false)], Vec::new(), None, None, false, true)
            .unwrap();
        assert_eq!(seq1, 1);
        let seq2 = store
            .append_delta(7, vec![page_item(1, "b", false)], Vec::new(), None, None, false, true)
            .unwrap();
        assert_eq!(seq2, 2);
        // The two single-item deltas together are far smaller than a whole-index blob
        // would be, and each append wrote only its own item.
        let records = store.read_delta_records(7, 0).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].items.len(), 1);
        assert_eq!(records[1].items[0].block_ref_key, "b");
        // The sequence tail is readable across a reopen even though the log now holds
        // delta records rather than whole-index records.
        let reopened = LocalIndexLogStore::new(dir.path());
        assert_eq!(reopened.stats(7).last_sequence, 2);
    }

    /// The slab catalog is the LAST record that carries one, and nothing after it clears it.
    ///
    /// The fold keeps a running answer instead of every record it walks past, so three things
    /// an ordering change can break are pinned here: a later catalog must WIN; a later record
    /// carrying a meta with NO slabs must not replace it; and a log whose only meta carries no
    /// slabs must answer None rather than that meta.
    ///
    /// The second and third are what make this more than one assertion. Writing it with only a
    /// catalog-less record after the catalogs let a fold that kept "the last meta, slabs or not"
    /// pass -- the record had no meta at all, so the wrong rule never fired.
    #[test]
    fn the_slab_catalog_is_the_last_record_that_carries_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());

        let catalog = |slab: u64| MetaItem {
            version: 1,
            start_wal_sequence: 1,
            timestamp_ms: 1,
            slab_version: 1,
            slabs: vec![SlabCatalogEntry {
                block_slab_id: slab,
                state: SlabCatalogState::Active,
                physical_bytes: 1,
                logical_bytes: 1,
                created_unix_ms: None,
                updated_unix_ms: None,
                first_block_id: None,
                last_block_id: None,
                version: 1,
            }],
        };
        // A meta that carries no slabs. An anchor looks like this whenever the fold is off.
        let slabless = MetaItem {
            version: 2,
            start_wal_sequence: 2,
            timestamp_ms: 2,
            slab_version: 0,
            slabs: Vec::new(),
        };

        store
            .append_delta(4, Vec::new(), Vec::new(), None, Some(catalog(11)), false, true)
            .unwrap();
        store
            .append_delta(4, Vec::new(), Vec::new(), None, Some(catalog(22)), false, true)
            .unwrap();
        // A meta AFTER the catalogs that carries none of its own.
        store
            .append_delta(4, Vec::new(), Vec::new(), None, Some(slabless.clone()), false, true)
            .unwrap();
        // And a record with no meta at all.
        store
            .append_delta(4, vec![page_item(1, "later", false)], Vec::new(), None, None, false, true)
            .unwrap();

        let found = store.latest_slab_catalog(4).unwrap().expect("a catalog");
        assert_eq!(
            found.slabs.first().map(|slab| slab.block_slab_id),
            Some(22),
            "the last record CARRYING a catalog wins, and neither record after it clears it"
        );

        // A log whose only meta carries no slabs has no catalog to find. This is the control:
        // a fold that kept the last meta whether or not it held slabs passes the assertion
        // above and fails here.
        store
            .append_delta(5, Vec::new(), Vec::new(), None, Some(slabless), false, true)
            .unwrap();
        assert!(
            store.latest_slab_catalog(5).unwrap().is_none(),
            "a meta carrying no slabs is not a catalog"
        );
    }

    #[test]
    fn read_delta_records_skips_anchor_and_ignores_whole_index_lines() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        // A legacy whole-index record and a delta record share the log file.
        store.append_json(9, b"{\"value\":1}").unwrap();
        let anchor_seq = store
            .append_delta(9, vec![page_item(1, "a", false)], Vec::new(), None, None, false, true)
            .unwrap();
        store
            .append_delta(9, vec![page_item(1, "b", false)], Vec::new(), None, None, false, true)
            .unwrap();
        // Only delta records are returned; the whole-index line is ignored.
        let all = store.read_delta_records(9, 0).unwrap();
        assert_eq!(all.len(), 2);
        // Retaining after the first delta's sequence yields only the later delta.
        let suffix = store.read_delta_records(9, anchor_seq).unwrap();
        assert_eq!(suffix.len(), 1);
        assert_eq!(suffix[0].items[0].block_ref_key, "b");
    }

    #[test]
    fn append_delta_meta_anchor_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store
            .append_delta(3, vec![page_item(1, "a", false)], Vec::new(), None, None, false, true)
            .unwrap();
        let meta = MetaItem {
            version: 1,
            start_wal_sequence: 42,
            timestamp_ms: 100,
            ..MetaItem::default()
        };
        store.append_delta(3, Vec::new(), Vec::new(), None, Some(meta), false, true).unwrap();
        let records = store.read_delta_records(3, 0).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].meta.as_ref().unwrap().start_wal_sequence, 42);
    }

    /// The sweep only rewrites the log when the rewrite is worth taking.
    ///
    /// The middle pair carries the weight: the SAME log, with the same records reclaimable, is
    /// left alone one byte above what it can give and rewritten at exactly what it can give.
    /// Without that pairing, "it did not rewrite" could just as well mean "there was nothing to
    /// remove" -- which is the last state, and is asserted on its own terms -- and "enough"
    /// could quietly mean "more than enough", which a mutation to a strict comparison survived
    /// until those two thresholds sat either side of the boundary.
    ///
    /// The first two states are proved by the log itself and not only by the report: a rewrite
    /// there removes three of four records, so the surviving count and the byte total say
    /// plainly whether it happened. The third cannot be proved that way -- a rewrite retaining
    /// every record writes the file back byte for byte -- which is why the field exists.
    #[test]
    fn a_sweep_rewrites_the_log_only_when_the_rewrite_is_worth_taking() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for key in ["a", "b", "c"] {
            store
                .append_delta(9, vec![page_item(1, key, false)], Vec::new(), Some(2), None, false, true)
                .unwrap();
        }
        let meta_sequence = store
            .append_delta(9, Vec::new(), Vec::new(), Some(2), Some(MetaItem::default()), false, true)
            .unwrap();

        // 1. Reclaimable, but nowhere near a megabyte of it: the log is left exactly as it is.
        let held = store
            .gc_reflected_before_anchor(9, 2, meta_sequence, 1 << 20)
            .unwrap();
        assert!(held.rewrite_skipped, "a sweep this small must not rewrite the log");
        assert_eq!(held.records_removed, 0);
        assert_eq!(
            held.removable_records_before_budget, 3,
            "and it must still report what it COULD have removed, or the skip is unreadable"
        );
        assert!(
            held.reclaimable_bytes > 0,
            "the bytes it declined to reclaim are the reason it declined"
        );
        assert_eq!(held.bytes_after, held.bytes_before);
        assert_eq!(
            store.read_delta_records(9, 0).unwrap().len(),
            4,
            "every record is still there"
        );

        // 1b. One byte more than it can reclaim is still too much to ask: the near side of the
        //     boundary, without which "enough" could quietly mean "strictly more than enough".
        let short = store
            .gc_reflected_before_anchor(9, 2, meta_sequence, held.reclaimable_bytes + 1)
            .unwrap();
        assert!(
            short.rewrite_skipped,
            "one byte short of the threshold is short of the threshold"
        );

        // 2. The same log, the same records reclaimable, and a threshold it meets EXACTLY: it
        //    rewrites. Reaching the threshold is enough; it does not have to be exceeded.
        let swept = store
            .gc_reflected_before_anchor(9, 2, meta_sequence, held.reclaimable_bytes)
            .unwrap();
        assert!(
            !swept.rewrite_skipped,
            "a sweep that meets the threshold exactly must rewrite"
        );
        assert_eq!(swept.records_removed, 3);
        assert!(swept.bytes_after < swept.bytes_before);
        assert_eq!(
            swept.reclaimable_bytes, held.reclaimable_bytes,
            "the same log offers the same bytes either way -- only the decision changed"
        );
        assert_eq!(store.read_delta_records(9, 0).unwrap().len(), 1);

        // 3. Nothing left to reclaim: skipped even with the threshold turned off, because a
        //    rewrite that retains every record writes the file back byte for byte.
        let empty = store.gc_reflected_before_anchor(9, 2, meta_sequence, 0).unwrap();
        assert!(
            empty.rewrite_skipped,
            "a sweep with nothing to remove must not take a barrier for it"
        );
        assert_eq!(empty.removable_records_before_budget, 0);
        assert_eq!(empty.reclaimable_bytes, 0);
    }

    #[test]
    fn gc_reflected_before_anchor_keeps_unreflected_deltas_and_the_catalog_anchor() {
        // Content-based post-dump sweep: records whose WAL anchor the durable base already
        // reflects go; a delta a concurrent writer landed with a HIGHER anchor survives even
        // though it sits below the catalog anchor in the log, and the anchor record itself
        // (the load-time catalog seed) survives its own sweep.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        // seq 1: legacy whole-index line (no anchor; read by no load path -> reflected).
        store.append_json(7, b"{\"value\":1}").unwrap();
        // seq 2: delta reflected by the dump (WAL anchor 2 <= dump anchor 2).
        store
            .append_delta(7, vec![page_item(1, "covered", false)], Vec::new(), Some(2), None, false, true)
            .unwrap();
        // seq 3: concurrent delta landed after the dump serialized (WAL anchor 5 > 2).
        store
            .append_delta(7, vec![page_item(1, "racing", false)], Vec::new(), Some(5), None, false, true)
            .unwrap();
        // seq 4: the dump's folded catalog anchor.
        let meta = MetaItem {
            version: 1,
            start_wal_sequence: 2,
            timestamp_ms: 100,
            ..MetaItem::default()
        };
        let meta_sequence = store
            .append_delta(7, Vec::new(), Vec::new(), Some(2), Some(meta), false, true)
            .unwrap();

        let report = store.gc_reflected_before_anchor(7, 2, meta_sequence, 0).unwrap();
        assert_eq!(report.records_before, 4);
        assert_eq!(report.records_removed, 2, "the whole-index line and the covered delta go");
        assert!(report.bytes_after < report.bytes_before);

        let survivors = store.read_delta_records(7, 0).unwrap();
        assert_eq!(survivors.len(), 2);
        assert_eq!(
            survivors[0].items[0].block_ref_key, "racing",
            "the unreflected concurrent delta must survive the sweep"
        );
        assert!(
            survivors[1].meta.is_some(),
            "the folded catalog anchor must survive the sweep"
        );
        // Sequence continuity: the next append lands above the anchor record.
        let next = store
            .append_delta(7, vec![page_item(1, "later", false)], Vec::new(), Some(6), None, false, true)
            .unwrap();
        assert_eq!(next, meta_sequence + 1);
    }

    #[test]
    fn interior_corruption_is_fatal_not_silent_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for i in 1..=4 {
            store
                .append_json(5, format!("{{\"value\":{i}}}").as_bytes())
                .unwrap();
        }
        drop(store);
        // Corrupt the 2nd record IN PLACE, keeping it newline-terminated with records 3 & 4
        // intact after it. A newline-terminated line that fails to parse is committed
        // corruption, not a torn tail.
        let path = index_log_path(dir.path(), 5);
        let contents = std::fs::read(&path).unwrap();
        let mut records = split_records(&contents);
        assert_eq!(records.len(), 4);
        records[1] = undecodable_record();
        std::fs::write(&path, records.concat()).unwrap();
        // scan drives last_sequence_at, which must surface interior corruption as an error
        // rather than silently truncating records 3 & 4 and rewinding the sequence counter
        // (which durable dump manifests reference via index_log_sequence).
        let reopened = LocalIndexLogStore::new(dir.path());
        assert!(
            reopened.scan(5, 0, u64::MAX, u64::MAX).is_err(),
            "interior index-log corruption must be fatal, not silently truncated"
        );
    }

    #[test]
    fn read_delta_records_propagates_interior_delta_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for key in ["a", "b", "c"] {
            store
                .append_delta(5, vec![page_item(1, key, false)], Vec::new(), Some(1), None, false, true)
                .unwrap();
        }
        drop(store);
        // Corrupt the 2nd delta record in place, keeping it newline-terminated with the 3rd
        // intact after it. Previously read_delta_records `if let Ok(..)` SILENTLY SKIPPED such
        // a line and folded on -- losing a removal/eviction recorded only in that delta. It
        // must now propagate the error and abort.
        let path = index_log_path(dir.path(), 5);
        let contents = std::fs::read(&path).unwrap();
        let mut records = split_records(&contents);
        assert_eq!(records.len(), 3);
        records[1] = undecodable_record();
        std::fs::write(&path, records.concat()).unwrap();
        let reopened = LocalIndexLogStore::new(dir.path());
        assert!(
            reopened.read_delta_records(5, 0).is_err(),
            "an interior delta record that fails to decode must abort, not be silently skipped"
        );
    }

    #[test]
    fn read_delta_records_enforces_sequence_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for (index, key) in ["a", "b", "c"].iter().enumerate() {
            store
                .append_delta(
                    5,
                    vec![page_item(1, key, false)],
                    Vec::new(),
                    Some(index as u64 + 1),
                    None,
                    false,
                    true,
                )
                .unwrap();
        }
        drop(store);
        // Reorder the (valid, framed) records so their sequences read 1, 3, 2 -- a continuity
        // violation that means a record was lost/reordered. Each line is individually valid
        // (correct digest), so this exercises the sequence-continuity guard, not the checksum.
        let path = index_log_path(dir.path(), 5);
        let contents = std::fs::read(&path).unwrap();
        let records = split_records(&contents);
        assert_eq!(records.len(), 3);
        let reordered: Vec<u8> = [0usize, 2, 1]
            .iter()
            .flat_map(|index| records[*index].clone())
            .collect();
        std::fs::write(&path, reordered).unwrap();
        let reopened = LocalIndexLogStore::new(dir.path());
        match reopened.read_delta_records(5, 0) {
            Err(IndexLogError::Corruption(_)) => {}
            other => panic!("out-of-order delta sequences must be a Corruption error, got {other:?}"),
        }
    }

    #[test]
    fn an_index_item_written_with_the_old_field_names_still_loads() {
        // The names are short now because they repeat once per ITEM for the life of the log.
        // Every record already on disk spells them out, so each short name keeps its old
        // spelling as an alias -- including `routing_slot`, which was itself a rename.
        let legacy = serde_json::json!({
            "kind": "page",
            "routing_slot": 545210715_u32,
            "page_ref_key": "string:m:0::0:0:126:0:0",
            "object_key": "m:0",
            "model_id": "string",
            "object_id": 122110326161599232_u64,
            "page_id": 0,
            "size": 126,
            "in_log": false,
            "deleted": false
        });
        let item: IndexItem = serde_json::from_value(legacy).expect("a legacy item must load");
        assert_eq!(item.routing_bucket, 545210715);
        assert_eq!(item.object_key, "m:0");
        assert_eq!(item.object_id, 122110326161599232);
        assert_eq!(item.size, 126);
        assert!(!item.deleted);
    }

    #[test]
    fn an_index_item_costs_far_less_than_its_field_names_used_to() {
        // Field names were 65.3% of a measured 859-byte index-log record: they are written once
        // per item, forever, and cost more than the addresses they label.
        let item = IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: 545210715,
            block_ref_key: "string:m:0::0:0:126:0:0".to_string(),
            object_key: "m:0".to_string(),
            model_id: "string".to_string(),
            component: None,
            object_id: 122110326161599232,
            page_id: 0,
            address: None,
            size: 126,
            in_log: false,
            deleted: false,
        };
        let encoded = serde_json::to_string(&item).unwrap();
        // Not vacuous: the item must still carry its values, so assert the payload is present
        // before asserting the envelope is small.
        assert!(encoded.contains("545210715"));
        assert!(encoded.contains("m:0"));
        assert!(encoded.contains("122110326161599232"));
        // The long spellings must be gone from the WRITTEN form.
        for gone in ["routing_slot", "page_ref_key", "object_key", "model_id", "object_id"] {
            assert!(!encoded.contains(gone), "{gone} should not be written any more");
        }
        assert!(
            encoded.len() < 150,
            "expected a compact item, got {} bytes: {encoded}",
            encoded.len()
        );
    }

    #[test]
    fn meta_item_without_zones_serializes_byte_identically_to_pre_fold() {
        // Pre-fold compatibility invariant: an anchor whose `slabs` is empty must serialize with
        // NO `zones` key and NO
        // `slab_version` beyond what a pre-fold MetaItem produced. `slab_version` defaults to 0
        // and is not skipped, so it appears; assert the value carries only the legacy three
        // fields plus a zero slab_version and no zones array.
        let meta = MetaItem {
            version: 7,
            start_wal_sequence: 11,
            timestamp_ms: 22,
            ..MetaItem::default()
        };
        let value = serde_json::to_value(&meta).unwrap();
        let object = value.as_object().unwrap();
        // Every field is written now, empty or not: a row is read by position, so a field that
        // disappears when it is empty moves every field behind it. An empty slab list costs one
        // byte and keeps the position of everything after it.
        assert_eq!(
            object.get("zones").expect("the slab list is always written"),
            &serde_json::json!([]),
            "an empty slab list is written, not skipped"
        );
        assert_eq!(object.get("version").unwrap(), 7);
        assert_eq!(object.get("start_wal_sequence").unwrap(), 11);
        assert_eq!(object.get("timestamp_ms").unwrap(), 22);
        assert_eq!(object.get("zone_version").unwrap(), 0);
        // And it round-trips.
        let back: MetaItem = serde_json::from_value(value).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn meta_item_slab_catalog_round_trips_through_the_delta_log() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let meta = MetaItem {
            version: 1,
            start_wal_sequence: 5,
            timestamp_ms: 100,
            slab_version: 3,
            slabs: vec![
                SlabCatalogEntry {
                    block_slab_id: 0,
                    state: SlabCatalogState::Sealed,
                    physical_bytes: 4096,
                    logical_bytes: 4000,
                    created_unix_ms: Some(10),
                    updated_unix_ms: Some(20),
                    first_block_id: Some(0),
                    last_block_id: Some(9),
                    version: 3,
                },
                SlabCatalogEntry {
                    block_slab_id: 1,
                    state: SlabCatalogState::Active,
                    physical_bytes: 512,
                    logical_bytes: 512,
                    created_unix_ms: Some(30),
                    updated_unix_ms: Some(30),
                    first_block_id: Some(10),
                    last_block_id: Some(10),
                    version: 3,
                },
            ],
        };
        store
            .append_delta(4, Vec::new(), Vec::new(), Some(5), Some(meta.clone()), false, true)
            .unwrap();
        // A reopen reads the folded catalog back exactly, and latest_slab_catalog finds it.
        let reopened = LocalIndexLogStore::new(dir.path());
        let recovered = reopened.latest_slab_catalog(4).unwrap().unwrap();
        assert_eq!(recovered, meta);
        assert_eq!(recovered.slabs.len(), 2);
        assert_eq!(recovered.slabs[0].state, SlabCatalogState::Sealed);
        assert_eq!(recovered.slabs[1].block_slab_id, 1);
    }

    #[test]
    fn latest_slab_catalog_prefers_the_newest_anchor_and_ignores_empty_ones() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let older = MetaItem {
            version: 1,
            start_wal_sequence: 1,
            timestamp_ms: 1,
            slab_version: 1,
            slabs: vec![SlabCatalogEntry {
                block_slab_id: 0,
                state: SlabCatalogState::Active,
                physical_bytes: 1,
                logical_bytes: 1,
                created_unix_ms: None,
                updated_unix_ms: None,
                first_block_id: None,
                last_block_id: None,
                version: 1,
            }],
        };
        let newer = MetaItem {
            version: 2,
            start_wal_sequence: 9,
            timestamp_ms: 9,
            slab_version: 2,
            slabs: vec![SlabCatalogEntry {
                block_slab_id: 0,
                state: SlabCatalogState::Sealed,
                physical_bytes: 2,
                logical_bytes: 2,
                created_unix_ms: None,
                updated_unix_ms: None,
                first_block_id: None,
                last_block_id: None,
                version: 2,
            }],
        };
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(1), Some(older), false, true)
            .unwrap();
        // An anchor with no slabs between them must not shadow the folded catalog.
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(5), Some(MetaItem::default()), false, true)
            .unwrap();
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(9), Some(newer.clone()), false, true)
            .unwrap();
        assert_eq!(store.latest_slab_catalog(6).unwrap().unwrap(), newer);
    }

    /// The interval holds a second dump off, and a shard that has never dumped is not held.
    ///
    /// Both halves matter and neither alone is enough. A rule that only checked the gap would
    /// satisfy "fires when enough has accumulated"; a rule that treated "never dumped" as
    /// "dumped just now" would satisfy "holds a second dump off" while silencing the FIRST dump
    /// of every restart -- the case with the most to reclaim and the one no interval should
    /// ever cover.
    #[test]
    fn the_dump_interval_holds_off_a_second_dump_but_never_the_first() {
        // Never dumped: no floor to wait behind, however long the interval.
        assert!(should_dump_index_catalog_now(4096, 1024, None, 60_000));
        // Dumped recently: the gap is crossed and the dump still waits.
        assert!(!should_dump_index_catalog_now(4096, 1024, Some(200), 1_500));
        // Waited long enough: it fires. The boundary itself counts as waited.
        assert!(should_dump_index_catalog_now(4096, 1024, Some(1_500), 1_500));
        assert!(should_dump_index_catalog_now(4096, 1024, Some(9_000), 1_500));
        // A zero interval is no floor -- the gap alone decides, in both directions.
        assert!(should_dump_index_catalog_now(4096, 1024, Some(0), 0));
        assert!(!should_dump_index_catalog_now(512, 1024, Some(0), 0));
        // And the interval never SUBSTITUTES for the gap: waiting is not accumulating.
        assert!(!should_dump_index_catalog_now(512, 1024, Some(9_000), 1_500));
    }

    /// A record whose items share one object key writes it once, and reads back identical.
    ///
    /// All four corners, because three of them are the ways this could be wrong rather than
    /// merely unhelpful:
    ///
    /// - items that SHARE a key: hoisted, and every item gets it back
    /// - items that share the EMPTY key: hoisted as `Some("")` and restored as empty, which is
    ///   why this is a record-level key and not a "same as the previous item" marker -- that
    ///   marker cannot tell a repeat from a genuinely empty key
    /// - items that DIFFER: not hoisted, every key written and returned unchanged
    /// - a single item: hoisted (it trivially agrees with itself) and restored
    ///
    /// The round-trip goes through the real writer and the real reader, so it also proves the
    /// ORDER holds: `page_ref_key` is derived from `object_key` among other fields, so a reader
    /// that restored the key after the handle would put back a handle derived against an empty
    /// key.
    #[test]
    fn a_record_whose_items_share_an_object_key_writes_it_once() {
        // Carries an ADDRESS, so `page_ref_key` is really derived from `object_key` and the
        // restore ORDER is exercised. With no address the derive returns early and a reader that
        // restored the key too late would still pass -- which is what the first version of this
        // test did.
        fn item(key: &str, component: &str) -> IndexItem {
            let address = crate::block_store::BlockAddress::from_parts(
                7,
                4096,
                832,
                None,
                None,
                None,
                None,
            );
            let block_ref_key = block_ref_key_from_parts(
                "feature",
                key,
                Some(component),
                address.block_slab_id,
                address.offset,
                address.length,
                address.page_id().unwrap_or_default(),
                address.generation().unwrap_or_default(),
            );
            IndexItem {
                kind: IndexItemKind::Page,
                routing_bucket: 1024,
                block_ref_key,
                object_key: key.to_string(),
                model_id: "feature".to_string(),
                component: Some(component.to_string()),
                object_id: 0,
                page_id: 0,
                address: Some(address),
                size: 832,
                in_log: false,
                deleted: false,
            }
        }

        let cases: Vec<(&str, Vec<IndexItem>)> = vec![
            ("shared", vec![item("ctx:event:41", "a"), item("ctx:event:41", "b"), item("ctx:event:41", "c")]),
            ("shared-empty", vec![item("", "a"), item("", "b")]),
            ("differing", vec![item("ctx:event:41", "a"), item("ctx:event:99", "b")]),
            ("single", vec![item("ctx:event:41", "a")]),
        ];

        for (label, items) in cases {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalIndexLogStore::new(dir.path());
            let expected: Vec<String> = items.iter().map(|item| item.object_key.clone()).collect();
            let expected_handles: Vec<String> =
                items.iter().map(|item| item.block_ref_key.clone()).collect();
            store
                .append_delta(11, items.clone(), Vec::new(), Some(1), None, false, true)
                .unwrap();

            let read = store.read_delta_records(11, 0).unwrap();
            assert_eq!(read.len(), 1, "{label}: expected one record");
            let back: Vec<String> = read[0].items.iter().map(|item| item.object_key.clone()).collect();
            assert_eq!(back, expected, "{label}: object keys did not round-trip");
            // The derived handle too: it is derived FROM the object key, so a key restored after
            // it would have produced a handle derived against an empty key.
            let handles: Vec<String> =
                read[0].items.iter().map(|item| item.block_ref_key.clone()).collect();
            assert_eq!(handles, expected_handles, "{label}: page handles did not round-trip");

            let distinct = expected.iter().collect::<std::collections::BTreeSet<_>>().len();
            let hoisted = read[0].shared_object_key.is_some();
            assert_eq!(
                hoisted,
                distinct == 1,
                "{label}: hoisted={hoisted} with {distinct} distinct key(s)"
            );
        }
    }

    /// What hoisting the key is worth, on a record shaped like a timestamped write.
    #[test]
    fn hoisting_a_shared_object_key_shrinks_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let items: Vec<IndexItem> = (0..32)
            .map(|index| IndexItem {
                kind: IndexItemKind::Page,
                routing_bucket: 1024,
                block_ref_key: String::new(),
                object_key: "ctx:event:41:tenant-7".to_string(),
                model_id: "feature".to_string(),
                component: Some((1_787_429_651_961u64 + index).to_string()),
                object_id: 0,
                page_id: index,
                address: None,
                size: 832,
                in_log: false,
                deleted: false,
            })
            .collect();

        let mut hoisted = items.clone();
        let shared = hoisted[0].object_key.clone();
        for item in hoisted.iter_mut() {
            item.object_key.clear();
        }
        let before = encode_index_payload(
            &IndexDeltaRecord {
                shard_id: 11,
                sequence: 1,
                items,
                meta: None,
                applied_wal_sequence: Some(1),
                upsert: true,
                key_states: Vec::new(),
                shared_object_key: None,
            },
            INDEX_LOG_SHAPE_DELTA,
        )
        .unwrap()
        .len();
        let after = encode_index_payload(
            &IndexDeltaRecord {
                shard_id: 11,
                sequence: 1,
                items: hoisted,
                meta: None,
                applied_wal_sequence: Some(1),
                upsert: true,
                key_states: Vec::new(),
                shared_object_key: Some(shared),
            },
            INDEX_LOG_SHAPE_DELTA,
        )
        .unwrap()
        .len();
        eprintln!(
            "\n  32 items sharing one object key: {before} B -> {after} B ({:.1}% smaller)\n",
            100.0 * (before - after) as f64 / before as f64
        );
        assert!(after < before, "hoisting must shrink the record: {before} -> {after}");
        let _ = &store;
    }

    #[test]
    fn should_dump_index_catalog_fires_only_past_the_gap() {
        assert!(!should_dump_index_catalog(0, 1024));
        assert!(!should_dump_index_catalog(1023, 1024));
        assert!(should_dump_index_catalog(1024, 1024));
        assert!(should_dump_index_catalog(4096, 1024));
        // A zero gap disables the threshold cadence entirely.
        assert!(!should_dump_index_catalog(u64::MAX, 0));
    }

    /// Set the rolling threshold for this test and put it back afterwards.
    ///
    /// The override is per THREAD, and with `--test-threads=1` every test in this binary runs on
    /// the same one -- so a test that set it and walked away would decide how every test after it
    /// rolls. Restored on drop, so a panicking assertion restores it too.
    struct RollingThreshold;

    impl Drop for RollingThreshold {
        fn drop(&mut self) {
            set_index_log_segment_bytes_for_test(None);
        }
    }

    fn roll_at(bytes: u64) -> RollingThreshold {
        set_index_log_segment_bytes_for_test(Some(bytes));
        RollingThreshold
    }

    /// Reclaim costs what it REMOVES, not what it KEEPS.
    ///
    /// This log used to be one file per shard, and the collector reclaimed it by reading the file
    /// and rewriting every record it retained. The cost was therefore on the survivors, which
    /// inverts the thing reclaim is for: a round that removed LESS copied MORE. Measured on the
    /// shape this replaces -- 2,000 records took 22.7 ms in one unlimited round against 29.0 ms
    /// limited to 200, and 40,000 took 356.7 ms against 492.0 ms -- and the bounded round left the
    /// rest of the work for later rounds that were dearer still. Every caller passed 0 because
    /// unbounded was the cheap option.
    ///
    /// The log now rolls into pieces and a round unlinks whole pieces, so what it removes is never
    /// read and never copied. What a round copies is the piece being WRITTEN, whatever the log
    /// retains -- which is the assertion below, and the one that fails if the collector goes back
    /// to rewriting its survivors.
    ///
    /// Asserted on bytes rather than on time: bytes are what makes it true, and they do not depend
    /// on the machine or on what else is running on it.
    #[test]
    fn reclaim_costs_what_it_removes_not_what_it_keeps() {
        let piece = 8 * 1024u64;
        let _rolling = roll_at(piece);
        let records = 8_000usize;

        // Three rounds on three copies of the same log, at three retain floors: one that removes
        // everything it can, one that removes most of it, and one that removes a sliver off the
        // front and keeps the rest. The sliver is the case the old shape was worst at -- it kept
        // the most, and the old cost was on what was kept.
        let mut rounds = Vec::new();
        for retain_from in [records as u64, (records * 9 / 10) as u64, (records / 10) as u64] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalIndexLogStore::new(dir.path());
            for value in 0..records {
                store
                    .append_json(5, format!("{{\"value\":{value}}}").as_bytes())
                    .unwrap();
            }
            let written = store.log_len_bytes(5);
            let pieces = index_log_segment_paths(dir.path(), 5).len();
            // Sequences run 1..=records, and a record goes when its sequence is below the floor.
            let report = store.gc_before_sequence_limited(5, retain_from, 0).unwrap();

            // A round that unlinks whole pieces is cheap in exactly the way a round that unlinks
            // too many is. Say what it must not lose, beside what it must not cost: every record
            // AT OR ABOVE the floor is still there afterwards.
            let survivors = store
                .scan(5, 0, u64::MAX, u64::MAX)
                .unwrap()
                .into_iter()
                .filter_map(|(_, raw)| {
                    let payload = crate::log_framing::decode_line(&raw).ok()?;
                    decode_index_payload::<IndexRecordHead>(payload).ok()
                })
                .map(|head| head.sequence)
                .collect::<std::collections::HashSet<_>>();
            let lost = (retain_from..=records as u64)
                .filter(|sequence| !survivors.contains(sequence))
                .count();
            assert_eq!(
                lost, 0,
                "reclaiming to {retain_from} lost {lost} record(s) it had to keep"
            );

            // What a round REPORTS removed is now taken from the unlinked pieces' names rather
            // than from reading them, so it is arithmetic on numbers written at seal time -- and
            // arithmetic that is one out reads as a cheaper round rather than as an error. Pin it
            // against the sequences that are actually gone.
            let gone = (1..=records as u64)
                .filter(|sequence| !survivors.contains(sequence))
                .count();
            assert_eq!(
                report.records_removed, gone,
                "reclaiming to {retain_from}: the round reported {} removed, {gone} are gone",
                report.records_removed
            );

            rounds.push((written, pieces, report));
        }

        let (all_written, all_pieces, remove_all) = &rounds[0];
        let (_, most_pieces, remove_most) = &rounds[1];
        let (_, sliver_pieces, remove_sliver) = &rounds[2];

        // THE DENOMINATOR, first. Every bound below is "no more than one piece", which says
        // nothing at all about a log that IS one piece -- so print what the log actually was.
        assert!(
            *all_written > piece * 6,
            "the log must be many pieces deep or the bounds below are vacuous: wrote \
             {all_written} B in {all_pieces} piece(s), piece size {piece} B"
        );
        assert!(
            *all_pieces >= 6 && *most_pieces >= 6 && *sliver_pieces >= 6,
            "every log must have rolled: {all_pieces}, {most_pieces}, {sliver_pieces} piece(s)"
        );
        assert!(
            remove_all.dropped_segments >= 1
                && remove_most.dropped_segments >= 1
                && remove_sliver.dropped_segments >= 1,
            "every round must have had whole pieces to unlink ({}, {}, {})",
            remove_all.dropped_segments,
            remove_most.dropped_segments,
            remove_sliver.dropped_segments
        );

        // What a round COPIES is bounded by the piece being written, however much it retains.
        // Under the shape this replaces the last round below copied nine tenths of the log.
        for (label, report) in [
            ("removing all", remove_all),
            ("removing most", remove_most),
            ("removing a sliver", remove_sliver),
        ] {
            assert!(
                report.bytes_copied <= piece,
                "{label}: the round retained {} B and copied {} B of them; the collector is \
                 rewriting what it keeps again (piece size {piece} B, {all_written} B written)",
                report.bytes_after,
                report.bytes_copied
            );
        }

        // RECLAIMING LESS MUST NOT COST MORE. This is the inversion itself: the sliver round
        // removes a ninth of what the other removes, and used to copy nine times as much.
        assert!(
            remove_sliver.bytes_copied <= remove_most.bytes_copied.saturating_add(piece),
            "the round that removes LESS copied MORE ({} B against {} B) -- the inversion is back",
            remove_sliver.bytes_copied,
            remove_most.bytes_copied
        );

        // The rewrite is still there and still does its job: a floor that lands INSIDE the piece
        // being written rewrites that piece, and only that piece.
        assert!(
            remove_all.bytes_copied > 0,
            "a floor inside the active piece must still rewrite it"
        );
        assert!(
            remove_all.bytes_after * 8 < remove_all.bytes_before,
            "removing everything it can must shrink the log: {} B -> {} B",
            remove_all.bytes_before,
            remove_all.bytes_after
        );
        assert!(
            remove_all.records_removed * 10 > records * 9,
            "the round must report what the unlinked pieces held: {} of {records}",
            remove_all.records_removed
        );
        assert_eq!(
            remove_all.records_after, 1,
            "the piece being written keeps exactly the record above the floor"
        );
    }

    /// The gate's counts come from the piece NAMES and agree with reading every record.
    ///
    /// `gate_summary` is only worth having if it answers the same question the whole-log scan it
    /// replaces answered. Two halves, and both are needed:
    ///
    /// - AGREEMENT. The counts are checked against the scan-and-decode this replaces, run here
    ///   over the same log. Filename arithmetic that is one out reads as a cheaper gate rather
    ///   than as an error, which is the way this fails silently.
    /// - COST. What it decodes is bounded by ONE piece, and does not grow when the log does. The
    ///   second log is three times the first; if the bound tracked records it would be three
    ///   times as much.
    #[test]
    fn the_gate_reads_piece_names_not_every_record() {
        let piece = 8 * 1024u64;
        let _rolling = roll_at(piece);
        // A record is roughly 120 B here, so the small log is about 60 pieces and the large one
        // about 180. Both are far enough past `piece` that "no more than one piece" says something.
        let small = 4_000usize;
        let large = 12_000usize;

        /// The shape this replaces, run here as the thing to agree with: the whole log into a
        /// vector, then a decode of every record to count the ones below the floor.
        fn by_reading_every_record(
            store: &LocalIndexLogStore,
            shard_id: ShardId,
            floor: u64,
        ) -> (usize, usize, u64) {
            let records = store.scan(shard_id, 0, u64::MAX, u64::MAX).unwrap();
            let bytes = records.iter().map(|(_, raw)| raw.len() as u64).sum::<u64>();
            let removable = records
                .iter()
                .filter_map(|(_, raw)| {
                    let payload = crate::log_framing::decode_line(raw).ok()?;
                    decode_index_payload::<IndexRecordHead>(payload).ok()
                })
                .filter(|head| head.sequence < floor)
                .count();
            (records.len(), removable, bytes)
        }

        let mut arms = Vec::new();
        for records in [small, large] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalIndexLogStore::new(dir.path());
            for value in 0..records {
                store
                    .append_json(
                        7,
                        format!("{{\"value\":{value},\"pad\":\"{}\"}}", "v".repeat(48)).as_bytes(),
                    )
                    .unwrap();
            }
            // Sequences run 1..=records, so a floor at 60% leaves `floor - 1` below it -- a
            // removable ratio of neither nothing nor everything, which is where an off-by-a-piece
            // shows up.
            let floor = (records * 6 / 10) as u64;
            let written = store.log_len_bytes(7);
            let pieces = store.piece_count(7);
            let gate = store.gate_summary(7, floor);
            let (scanned_records, scanned_removable, scanned_bytes) =
                by_reading_every_record(&store, 7, floor);

            // THE DENOMINATORS, before any bound that mentions a piece. "No more than one piece"
            // is vacuously true of a log that IS one piece, and "did not grow" is vacuous if the
            // two logs are the same size.
            assert!(
                pieces >= 20 && written > piece * 20,
                "the log must be many pieces deep or every bound below is vacuous: {records} \
                 records, {written} B in {pieces} piece(s), piece size {piece} B"
            );
            assert_eq!(
                gate.pieces, pieces,
                "the gate must have walked every piece: {} of {pieces}",
                gate.pieces
            );
            assert_eq!(
                gate.pieces_named,
                pieces - 1,
                "every piece but the one being written must be answered from its NAME: {} named \
                 of {pieces}",
                gate.pieces_named
            );

            // AGREEMENT with reading every record, and with the arithmetic said out loud.
            assert_eq!(
                (gate.records, gate.removable_records),
                (scanned_records, scanned_removable),
                "the gate disagrees with a full scan of the same log ({records} records, floor \
                 {floor}): names say {}/{} records/removable, reading says \
                 {scanned_records}/{scanned_removable}",
                gate.records,
                gate.removable_records
            );
            assert_eq!(
                (gate.records, gate.removable_records),
                (records, floor as usize - 1),
                "sequences run 1..={records} and the floor is {floor}, so the log holds {records} \
                 records of which {} are below it",
                floor - 1
            );
            assert_eq!(
                (gate.bytes, gate.bytes.min(scanned_bytes)),
                (written, scanned_bytes),
                "the gate's byte count must be the log's on-disk length ({written} B) and must \
                 not be under what the records themselves occupy ({scanned_bytes} B), got {}",
                gate.bytes
            );

            // COST. What it decoded is the piece being written, and nothing else.
            assert!(
                gate.bytes_decoded <= piece,
                "the gate decoded {} B of a {written} B log ({records} records, {pieces} pieces); \
                 it is reading the whole log again (piece size {piece} B)",
                gate.bytes_decoded
            );
            assert!(
                gate.bytes_decoded * 4 < written && gate.records_decoded * 4 < records,
                "the gate decoded {} B / {} records of a {written} B / {records} record log in \
                 {pieces} pieces -- at this piece count one piece is a twentieth of the log, so \
                 anything near a quarter of it is a whole-log walk",
                gate.bytes_decoded,
                gate.records_decoded
            );
            arms.push((records, written, pieces, gate));
        }

        let (_, small_written, small_pieces, small_gate) = arms[0];
        let (_, large_written, large_pieces, large_gate) = arms[1];

        // THE DENOMINATOR for "did not grow": the second log really is much bigger, in bytes,
        // records and pieces. Without this the comparison below could pass on two equal logs.
        assert!(
            large_written > small_written * 2
                && large_gate.records > small_gate.records * 2
                && large_pieces > small_pieces * 2,
            "the two log sizes must differ or the bound below is vacuous: {small_written} B / \
             {} records / {small_pieces} pieces against {large_written} B / {} records / \
             {large_pieces} pieces",
            small_gate.records,
            large_gate.records
        );
        // The log tripled and what the gate decoded did not. The bound is one piece either way,
        // which is the whole claim: the cost is a property of the PIECE SIZE, not of the log.
        assert!(
            large_gate.bytes_decoded <= small_gate.bytes_decoded.saturating_add(piece),
            "the gate's cost grew with the LOG: {} B / {} records decoded on a {small_written} B \
             log against {} B / {} records on a {large_written} B one -- it tracks records again",
            small_gate.bytes_decoded,
            small_gate.records_decoded,
            large_gate.bytes_decoded,
            large_gate.records_decoded
        );
    }

    /// What a reclaim round COSTS, measured both ways in one binary.
    ///
    /// `#[ignore]`d: it is a measurement, not a guard, and it writes a quarter of a million
    /// records. Run it with
    /// `cargo test -p temporalstore-rust --lib measure_reclaim_cost_shape -- --ignored --nocapture`.
    ///
    /// Both arms run here rather than across two builds, because the rolling threshold is the
    /// only difference between them: zero is one file per shard, which is byte for byte the shape
    /// this replaces. The PIECES column is the proof the treatment actually ran -- one piece in
    /// the first arm, many in the second -- so an arm that silently failed to roll cannot be read
    /// as a win.
    #[test]
    #[ignore]
    fn measure_reclaim_cost_shape() {
        // Enough to clear the 768 KiB byte threshold that gates index GC, so the round being
        // measured is the size of round that actually happens.
        let records = 40_000usize;
        println!(
            "\n  {:<16} {:>9} {:>7} {:>13} {:>11} {:>11} {:>9}",
            "shape", "retained", "pieces", "log before", "copied", "unlinked", "ms"
        );
        for (label, threshold) in [
            ("one file", 0u64),
            ("64 KiB pieces", DEFAULT_INDEX_LOG_SEGMENT_BYTES),
        ] {
            for retained_percent in [10usize, 40, 60, 90, 99] {
                let _rolling = roll_at(threshold);
                let dir = tempfile::tempdir().unwrap();
                let store = LocalIndexLogStore::new(dir.path());
                for value in 0..records {
                    store
                        .append_json(5, format!("{{\"value\":{value}}}").as_bytes())
                        .unwrap();
                }
                let pieces = index_log_segment_paths(dir.path(), 5).len();
                let retain_from = (records * (100 - retained_percent) / 100) as u64;
                let at = std::time::Instant::now();
                let report = store.gc_before_sequence_limited(5, retain_from, 0).unwrap();
                let ms = at.elapsed().as_secs_f64() * 1000.0;
                println!(
                    "  {label:<16} {:>8}% {pieces:>7} {:>13} {:>11} {:>11} {ms:>9.1}",
                    retained_percent,
                    report.bytes_before,
                    report.bytes_copied,
                    report.dropped_segment_bytes,
                );
            }
        }
    }

    /// A log written before there were pieces still loads, still folds, and still reclaims.
    ///
    /// One file per shard is what every existing store holds, and dropping that shape would find
    /// no index log where one exists -- which reads as an EMPTY log rather than an error, and an
    /// empty index log is a silently emptier shard.
    #[test]
    fn a_log_written_as_one_file_still_reads_and_reclaims() {
        let dir = tempfile::tempdir().unwrap();
        let records = 2_000usize;
        {
            // Never rolling is the shape of a store written before this.
            let _never = roll_at(0);
            let store = LocalIndexLogStore::new(dir.path());
            for value in 0..records {
                store
                    .append_json(5, format!("{{\"value\":{value}}}").as_bytes())
                    .unwrap();
            }
            assert_eq!(
                index_log_segment_paths(dir.path(), 5).len(),
                1,
                "the fixture must be ONE file, or this proves nothing about the old shape"
            );
            assert!(index_log_path(dir.path(), 5).exists());
        }

        let _rolling = roll_at(16 * 1024);
        let store = LocalIndexLogStore::new(dir.path());
        assert_eq!(store.stats(5).last_sequence, records as u64);
        assert_eq!(store.record_count(5).unwrap(), records);

        // It still reclaims: there is no piece to unlink, so the rewrite does the whole job,
        // exactly as it did before.
        let report = store.gc_before_sequence_limited(5, records as u64, 0).unwrap();
        assert_eq!(report.dropped_segments, 0, "a one-file log has no piece to drop");
        assert_eq!(report.records_removed, records - 1);
        assert!(report.bytes_copied > 0, "the single file was rewritten");
        assert_eq!(store.record_count(5).unwrap(), 1);
    }

    /// The fold crosses a piece boundary, with both record shapes on either side of it.
    ///
    /// The log holds two shapes -- whole-index records and deltas -- sharing one sequence counter.
    /// A boundary that split or reordered them would surface as a hole in the delta stream, which
    /// the fold's continuity check refuses, or as a record read as the wrong shape.
    #[test]
    fn the_fold_crosses_piece_boundaries_with_both_record_shapes() {
        let _rolling = roll_at(8 * 1024);
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let mut expected = Vec::new();
        for index in 0..400usize {
            if index % 25 == 24 {
                // A whole-index record shares this log and is not a delta.
                store
                    .append_json(6, format!("{{\"value\":{index}}}").as_bytes())
                    .unwrap();
            } else {
                let sequence = store
                    .append_delta(
                        6,
                        vec![page_item(1, &format!("tenant/1/object/{index:06}"), false)],
                        Vec::new(),
                        Some(index as u64 + 1),
                        None,
                        false,
                        false,
                    )
                    .unwrap();
                expected.push(sequence);
            }
        }

        let pieces = index_log_segment_paths(dir.path(), 6).len();
        assert!(
            pieces >= 3,
            "the log must have rolled or this crosses no boundary (got {pieces} piece(s))"
        );
        let folded = store.read_delta_records(6, 0).unwrap();
        assert_eq!(
            folded.iter().map(|record| record.sequence).collect::<Vec<_>>(),
            expected,
            "every delta record, in log order, across the piece boundaries"
        );
        assert_eq!(
            folded[0].items[0].object_key, "tenant/1/object/000000",
            "an item survives the boundary with its fields put back"
        );
        assert_eq!(store.record_count(6).unwrap(), 400, "both shapes are still counted");
        assert_eq!(store.stats(6).last_sequence, 400);
        // The raw stream reads back across the boundary too: the debug read is addressed by
        // position in the LOG, not in whichever piece holds it.
        let head = store.read_range(6, 0, u64::MAX).unwrap();
        assert_eq!(
            head.len() as u64,
            store.log_len_bytes(6),
            "reading the whole log must not stop at the first boundary"
        );
    }

    /// The post-dump sweep unlinks whole pieces the dumped base already reflects, and keeps every
    /// record it does not.
    ///
    /// This sweep decides on CONTENT, not position alone: a delta appended between the dump's
    /// serialization and its anchor carries a WAL anchor the base does not reflect, and removing
    /// it would lose an eviction that lives only in the delta stream. A piece's name carries the
    /// highest anchor in it, so a piece goes only when all of it is reflected.
    #[test]
    fn the_post_dump_sweep_unlinks_pieces_the_base_reflects() {
        let piece = 16 * 1024u64;
        let _rolling = roll_at(piece);
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let total = 1_200usize;
        for index in 0..total {
            store
                .append_delta(
                    7,
                    vec![page_item(1, &format!("tenant/1/object/{index:06}"), false)],
                    Vec::new(),
                    // Anchor == sequence, so "reflected" and "below the bound" move together and
                    // the piece boundary is the only thing deciding what survives.
                    Some(index as u64 + 1),
                    None,
                    false,
                    false,
                )
                .unwrap();
        }
        let pieces_before = index_log_segment_paths(dir.path(), 7).len();
        assert!(
            pieces_before >= 4,
            "the log must be several pieces deep (got {pieces_before})"
        );

        // A dump that materialised the base through WAL anchor 600, anchored at index sequence 900.
        let report = store.gc_reflected_before_anchor(7, 600, 900, 0).unwrap();
        assert!(
            report.dropped_segments >= 1,
            "whole pieces the base reflects must be unlinked, not rewritten"
        );
        assert!(
            report.bytes_copied <= piece,
            "the sweep copied {} B; it must copy no more than the piece being written ({piece} B)",
            report.bytes_copied
        );
        assert!(
            report.bytes_after < report.bytes_before,
            "the log must actually shrink: {} B -> {} B",
            report.bytes_before,
            report.bytes_after
        );

        let survivors = store
            .read_delta_records(7, 0)
            .unwrap()
            .into_iter()
            .map(|record| record.sequence)
            .collect::<std::collections::HashSet<_>>();
        let lost = (601..=total as u64)
            .filter(|sequence| !survivors.contains(sequence))
            .count();
        assert_eq!(
            lost, 0,
            "every record the dumped base does not reflect must survive the sweep"
        );
    }

    /// Concurrent appends to one shard's index log must SHARE durability barriers.
    ///
    /// An fsync makes every byte already in the file durable, so a barrier taken while other
    /// writers are queued behind it covers them too. This held the store lock across the fsync,
    /// so writers could not reach the barrier together and each paid for one of its own -- the
    /// same shape the raft node log had.
    #[test]
    fn concurrent_index_log_appends_share_barriers() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(LocalIndexLogStore::new(dir.path()));
        let writers = 16usize;
        let each = 8usize;
        // Release the threads together, so they genuinely overlap rather than trickling through
        // one at a time and each finding no barrier to ride.
        let start = std::sync::Arc::new(std::sync::Barrier::new(writers));

        crate::durability_metrics::reset();
        let handles: Vec<_> = (0..writers)
            .map(|writer| {
                let store = std::sync::Arc::clone(&store);
                let start = std::sync::Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for index in 0..each {
                        store
                            .append_delta(
                                4,
                                vec![page_item(1, &format!("k-{writer}-{index}"), false)],
                                Vec::new(),
                                None,
                                None,
                                false,
                                true,
                            )
                            .unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let appends = (writers * each) as u64;
        let barriers = crate::durability_metrics::snapshot()
            .get("engine_index_log_append")
            .copied()
            .unwrap_or(0);
        assert!(barriers >= 1, "some barrier must actually be taken");
        assert!(
            barriers < appends,
            "{barriers} barriers for {appends} concurrent appends -- nothing coalesced"
        );

        // Sharing a barrier must cost nobody their record: every append is still readable.
        let records = store.read_delta_records(4, 0).unwrap();
        assert_eq!(records.len() as u64, appends, "every append must survive");
    }

    /// What a binary index-log record would actually save, measured on the record the log
    /// really writes rather than a hand-made one.
    ///
    /// The line below is copied verbatim from a 100k-record ingest, so the shape, the string
    /// lengths and the integer magnitudes are the ones that occur -- parsing it here also
    /// keeps the current code honest about loading what it wrote. Name shortening has already
    /// taken this record from 962.8 to 365.5 bytes across four rounds and is asymptotic: what
    /// is left is the JSON itself -- quotes, braces, commas, and integers spelled in decimal.
    #[test]
    fn what_is_left_to_gain_from_a_binary_index_record() {
        // One page item plus a key-state blob: the common delta the ingest path appends.
        let line = r#"{"s":1,"q":1,"items":[{"k":"page","rb":545210715,
            "pk":"string:m:0::0:0:126:0:0","ok":"m:0","mi":"string",
            "oi":122110326161599232,"pi":0,
            "a":{"ps":0,"o":0,"l":126,"pi":0,"oi":122110326161599232,
                 "rs":545210715,"g":0,"b":0},
            "sz":126,"il":false,"d":false}],
            "aw":1,"u":true,
            "ks":[{"key":"string:m:0","kind":"string","pages":[0],"version":1}]}"#;
        let record: IndexDeltaRecord =
            serde_json::from_str(line).expect("the record the log writes must still load");

        let json = serde_json::to_vec(&record).unwrap();
        let today = crate::log_framing::encode_line(&json);

        // struct-as-MAP, matching the served-index container: the array form is positional,
        // and positional mis-reads a struct that skipped an absent optional -- and the GC's
        // anchor probe deserializes two fields by NAME out of an arbitrary record, which a
        // positional encoding makes impossible.
        let mut packed = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut packed).with_struct_map();
        serde::Serialize::serialize(&record, &mut serializer).unwrap();
        let framed_packed = crate::log_framing::encode_line(&packed);

        let zstd_json = zstd::stream::encode_all(json.as_slice(), 3).unwrap();
        let zstd_packed = zstd::stream::encode_all(packed.as_slice(), 3).unwrap();

        println!("  framing            {:>5} B", today.len() - json.len());
        println!("  json (today)       {:>5} B framed {:>5} B", json.len(), today.len());
        println!(
            "  msgpack named      {:>5} B framed {:>5} B  ({:.2}x)",
            packed.len(),
            framed_packed.len(),
            today.len() as f64 / framed_packed.len() as f64
        );
        println!("  zstd(json)         {:>5} B", zstd_json.len());
        println!("  zstd(msgpack)      {:>5} B", zstd_packed.len());

        // The round trip has to be exact, or the saving is not a saving.
        let back: IndexDeltaRecord = rmp_serde::from_slice(&packed).unwrap();
        assert_eq!(back, record, "msgpack must round-trip the record exactly");

        // And the GC's probe must still find its two fields by name in the binary form,
        // because GC retains raw payloads and never re-encodes them.
        #[derive(serde::Deserialize)]
        struct AnchorProbe {
            #[serde(rename = "q", alias = "sequence")]
            sequence: u64,
            #[serde(rename = "aw", alias = "applied_wal_sequence", default)]
            applied_wal_sequence: Option<u64>,
        }
        let probe: AnchorProbe = rmp_serde::from_slice(&packed).unwrap();
        assert_eq!(probe.sequence, record.sequence);
        assert_eq!(probe.applied_wal_sequence, record.applied_wal_sequence);

        // Pin the direction, not a brittle exact size: binary must actually be smaller, and
        // the assertion is paired with a nonzero check so it cannot pass by measuring nothing.
        assert!(packed.len() > 0, "measured nothing");
        assert!(
            packed.len() < json.len(),
            "binary {} B is not smaller than json {} B",
            packed.len(),
            json.len()
        );
    }

    /// The decoder sniffs, so one file may hold both shapes -- which is exactly what a log
    /// written across a rollout looks like. Nothing records which shape a line is; if the
    /// sniff were wrong this is where it would show.
    #[test]
    fn one_log_file_can_hold_both_shapes() {
        let json_record = IndexDeltaRecord {
            shard_id: 1,
            sequence: 1,
            items: vec![page_item(1, "a", false)],
            meta: None,
            applied_wal_sequence: Some(1),
            upsert: false,
            key_states: Vec::new(),
            shared_object_key: None,
        };
        let binary_record = IndexDeltaRecord {
            sequence: 2,
            items: vec![page_item(2, "b", false)],
            ..json_record.clone()
        };

        let mut whole_packed = Vec::new();
        let mut whole_ser = rmp_serde::Serializer::new(&mut whole_packed).with_struct_map();
        serde::Serialize::serialize(&json_record, &mut whole_ser).unwrap();
        let mut json_payload =
            vec![index_container_byte(INDEX_LOG_CODEC_MSGPACK, INDEX_LOG_SHAPE_WHOLE)];
        json_payload.extend_from_slice(&whole_packed);
        let mut packed = Vec::new();
        let mut ser = rmp_serde::Serializer::new(&mut packed).with_struct_map();
        serde::Serialize::serialize(&binary_record, &mut ser).unwrap();
        let mut binary_payload =
            vec![index_container_byte(INDEX_LOG_CODEC_MSGPACK, INDEX_LOG_SHAPE_DELTA)];
        binary_payload.extend_from_slice(&packed);

        // Framing is independent of the payload shape, so both frame and verify the same way.
        for payload in [&json_payload, &binary_payload] {
            let framed = crate::log_framing::encode_line(payload);
            let back = crate::log_framing::decode_line(&framed[..framed.len() - 1]).unwrap();
            assert_eq!(back, payload.as_slice(), "framing must not care about the shape");
        }

        let a: IndexDeltaRecord = decode_index_payload(&json_payload).unwrap();
        let b: IndexDeltaRecord = decode_index_payload(&binary_payload).unwrap();
        assert_eq!(a.sequence, 1);
        assert_eq!(b.sequence, 2);
        assert_eq!(b.items[0].block_ref_key, "b");
    }

    /// GC retains the raw payload of a record it keeps, never re-encoding it -- a delta record
    /// also parses as an IndexLogRecord, so re-serializing would drop its items. A binary
    /// payload has to survive that path byte-for-byte too.
    #[test]
    fn the_anchor_probe_reads_a_binary_record_without_decoding_the_rest() {
        #[derive(serde::Deserialize)]
        struct AnchorProbe {
            #[serde(rename = "q", alias = "sequence")]
            sequence: u64,
            #[serde(rename = "aw", alias = "applied_wal_sequence", default)]
            applied_wal_sequence: Option<u64>,
        }

        let record = IndexDeltaRecord {
            shard_id: 4,
            sequence: 77,
            items: vec![page_item(5, "c", false)],
            meta: None,
            applied_wal_sequence: Some(31),
            upsert: true,
            key_states: Vec::new(),
            shared_object_key: None,
        };
        let mut packed = Vec::new();
        let mut ser = rmp_serde::Serializer::new(&mut packed).with_struct_map();
        serde::Serialize::serialize(&record, &mut ser).unwrap();
        let mut payload =
            vec![index_container_byte(INDEX_LOG_CODEC_MSGPACK, INDEX_LOG_SHAPE_DELTA)];
        payload.extend_from_slice(&packed);

        let probe: AnchorProbe = decode_index_payload(&payload).unwrap();
        assert_eq!(probe.sequence, 77);
        assert_eq!(probe.applied_wal_sequence, Some(31));

        // And the bytes GC would retain are the bytes it was given.
        let retained = payload.clone();
        let again: AnchorProbe = decode_index_payload(&retained).unwrap();
        assert_eq!(again.sequence, 77);
        assert_eq!(retained, payload, "GC must retain the payload untouched");
    }

    /// A container written by a newer binary is refused with a clear error rather than
    /// mis-parsed. Silently mis-reading a durable record is the failure worth preventing.
    #[test]
    fn an_unknown_payload_codec_is_refused_not_guessed_at() {
        let mut payload = vec![index_container_byte(9, INDEX_LOG_SHAPE_DELTA)];
        payload.extend_from_slice(b"whatever a later format puts here");
        let result: Result<IndexDeltaRecord, _> = decode_index_payload(&payload);
        match result {
            Err(IndexLogError::Encoding(message)) => {
                assert!(message.contains('9'), "the error should name the codec: {message}")
            }
            other => panic!("an unknown codec must be refused, got {other:?}"),
        }

        // An empty payload has no container byte at all, and is refused the same way.
        let result: Result<IndexDeltaRecord, _> = decode_index_payload(&[]);
        assert!(matches!(result, Err(IndexLogError::Encoding(_))));
    }

    /// An item that omits its default-valued fields still decodes, with those defaults.
    ///
    /// This is what makes omitting them safe in ONE step, with no ordering between writers and
    /// readers. A renamed field or a changed type breaks an old reader -- it meets a name it does
    /// not know, or a type it refuses. An ABSENT field with `#[serde(default)]` is a case every
    /// reader already handles, including ones deployed long before this.
    ///
    /// So the property to pin is that the fields really do carry defaults, and that a record
    /// written without them comes back saying the same thing.
    #[test]
    fn an_item_missing_its_default_fields_still_decodes() {
        // A record from a writer that omits everything default-valued.
        #[derive(serde::Serialize)]
        struct Sparse {
            #[serde(rename = "k")]
            kind: IndexItemKind,
            #[serde(rename = "rb")]
            routing_bucket: u32,
            #[serde(rename = "pk")]
            block_ref_key: String,
            #[serde(rename = "ok")]
            object_key: String,
            #[serde(rename = "mi")]
            model_id: String,
            #[serde(rename = "oi")]
            object_id: u64,
        }

        let sparse = Sparse {
            kind: IndexItemKind::Page,
            routing_bucket: 8539,
            block_ref_key: "17665223918442101733".to_string(),
            object_key: "tenant/7/object/000000123".to_string(),
            model_id: "string".to_string(),
            object_id: 12_345,
        };
        let encoded = encode_as_map(&sparse, INDEX_LOG_SHAPE_DELTA);
        let decoded: IndexItem =
            decode_index_payload(&encoded).expect("an item missing defaults must decode");

        assert!(!decoded.in_log, "in_log must default to false when absent");
        assert!(!decoded.deleted, "deleted must default to false when absent");
        assert_eq!(decoded.page_id, 0, "page_id must default to zero when absent");
        assert_eq!(decoded.size, 0, "size must default to zero when absent");
        assert_eq!(decoded.routing_bucket, 8539, "what WAS written must survive");
        assert_eq!(decoded.object_key, "tenant/7/object/000000123");

        // And a full item still round-trips: skipping is about what is written, not what is meant.
        let full = IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: 8539,
            block_ref_key: "17665223918442101733".to_string(),
            object_key: "tenant/7/object/000000123".to_string(),
            model_id: "string".to_string(),
            component: None,
            object_id: 12_345,
            page_id: 7,
            address: None,
            size: 4096,
            in_log: true,
            deleted: true,
        };
        let round_tripped: IndexItem = decode_index_payload(
            &encode_index_payload(&full, INDEX_LOG_SHAPE_DELTA).expect("encode"),
        )
        .expect("decode");
        assert_eq!(round_tripped.page_id, 7, "a set page_id must still be written");
        assert_eq!(round_tripped.size, 4096, "a set size must still be written");
        assert!(round_tripped.in_log, "a true in_log must still be written");
        assert!(round_tripped.deleted, "a true deleted must still be written");
    }

    /// A page handle reads whether it was written as a number or as text.
    ///
    /// The handle is a `u64` everywhere but the log, where it is stringified -- 20 bytes of a
    /// 161-byte item. Moving the writer to the number cannot come first: msgpack refuses a type it
    /// was not expecting rather than degrading, so a reader that only knows the string shape fails
    /// outright on a log the new writer produced.
    ///
    /// This pins the half that lands first. Readers take both; the writer still emits text.
    #[test]
    fn a_block_handle_reads_as_a_number_or_a_string() {
        // The same field names the item uses, with the handle as a NUMBER -- what a future writer
        // would produce.
        #[derive(serde::Serialize)]
        struct NumericHandle {
            #[serde(rename = "k")]
            kind: IndexItemKind,
            #[serde(rename = "rb")]
            routing_bucket: u32,
            #[serde(rename = "pk")]
            block_ref_key: u64,
            #[serde(rename = "ok")]
            object_key: String,
            #[serde(rename = "mi")]
            model_id: String,
            #[serde(rename = "oi")]
            object_id: u64,
            #[serde(rename = "pi")]
            page_id: u64,
            #[serde(rename = "sz")]
            size: u64,
            #[serde(rename = "il")]
            in_log: bool,
            #[serde(rename = "d")]
            deleted: bool,
        }

        let handle = 17_665_223_918_442_101_733u64;
        let numeric = NumericHandle {
            kind: IndexItemKind::Page,
            routing_bucket: 8539,
            block_ref_key: handle,
            object_key: "tenant/7/object/000000123".to_string(),
            model_id: "string".to_string(),
            object_id: 12_345,
            page_id: 7,
            size: 4096,
            in_log: false,
            deleted: false,
        };
        let as_number = encode_as_map(&numeric, INDEX_LOG_SHAPE_DELTA);
        let decoded: IndexItem =
            decode_index_payload(&as_number).expect("a numeric handle must decode");
        assert_eq!(
            decoded.block_ref_key,
            handle.to_string(),
            "a handle written as a number must come back as the same handle"
        );
        assert_eq!(decoded.routing_bucket, 8539, "the rest of the item must survive too");

        // The text shape still reads, and is still what gets written.
        let textual = IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: 8539,
            block_ref_key: handle.to_string(),
            object_key: "tenant/7/object/000000123".to_string(),
            model_id: "string".to_string(),
            component: None,
            object_id: 12_345,
            page_id: 7,
            address: None,
            size: 4096,
            in_log: false,
            deleted: false,
        };
        let as_text = encode_as_map(&textual, INDEX_LOG_SHAPE_DELTA);
        let round_tripped: IndexItem = decode_index_payload(&as_text).expect("text must decode");
        assert_eq!(round_tripped.block_ref_key, handle.to_string());

        // The writer now emits the number, so `as_text` above is ALSO numeric and the two are no
        // longer a text-vs-number comparison -- they are two different structs. `NumericHandle`
        // is a hand-rolled mirror that does not skip its false bools, where `IndexItem` does, so
        // it comes out SEVEN BYTES LARGER despite carrying the same handle. Comparing them was
        // only ever meaningful while the text penalty outweighed that difference.
        //
        // Compare like with like instead: the same item, with a handle that parses as a number
        // and one of the same length that does not.
        let mut unparseable = textual.clone();
        unparseable.block_ref_key = format!("x{}", &handle.to_string()[1..]);
        assert_eq!(
            unparseable.block_ref_key.len(),
            handle.to_string().len(),
            "the two handles must be the same length or the comparison measures the length"
        );
        let as_forced_text =
            encode_as_map(&unparseable, INDEX_LOG_SHAPE_DELTA);
        assert!(
            as_forced_text.len() > as_text.len(),
            "a handle that parses should be written as a number and be smaller: text {} vs number {}",
            as_forced_text.len(),
            as_text.len()
        );
        println!(
            "  HANDLE same item, 20-char handle: as text {} B, as a number {} B (saves {} B a record)",
            as_forced_text.len(),
            as_text.len(),
            as_forced_text.len() - as_text.len()
        );
        println!(
            "  HANDLE text {} B vs number {} B ({} B a record if the writer ever moves)",
            as_text.len(),
            as_number.len(),
            as_text.len().saturating_sub(as_number.len())
        );
    }

    /// Stripping the address repeats and putting them back returns the item that went in.
    ///
    /// The shape that decides this is the third one: an address whose object id DIFFERS from the
    /// item's. Stripping must leave it alone, or the restore would overwrite a real value with the
    /// item's -- silently pointing an index entry at the wrong object. The fourth covers an address
    /// that never had one, where restore fills from the item, which is what the WAL does too.
    #[test]
    fn the_address_repeats_round_trip() {
        let object_id = 12_345_678_901_234_567u64;
        let bucket = 8539u32;
        let build = |address| IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: bucket,
            block_ref_key: 17_665_223_918_442_101_733u64.to_string(),
            object_key: "tenant/7/object/000000123".to_string(),
            model_id: "string".to_string(),
            component: None,
            object_id,
            page_id: 7,
            address,
            size: 4096,
            in_log: false,
            deleted: false,
        };
        let cases = [
            ("no address", None),
            ("address repeats both", Some(crate::block_store::BlockAddress::from_parts(
                42, 1_048_576, 4096, Some(7), Some(object_id), Some(bucket), Some(3)))),
            ("address holds a DIFFERENT object", Some(crate::block_store::BlockAddress::from_parts(
                42, 0, 0, None, Some(object_id + 1), Some(bucket + 1), None))),
            ("address holds neither", Some(crate::block_store::BlockAddress::from_parts(
                42, 0, 0, None, None, None, None))),
        ];

        for (label, address) in cases {
            let original = build(address);
            let mut stripped = original.clone();
            stripped.strip_address_repeats();

            let payload = encode_index_payload(&stripped, INDEX_LOG_SHAPE_DELTA).expect("encode");
            let mut back: IndexItem = decode_index_payload(&payload).expect("decode");
            back.restore_address_repeats();

            match label {
                // An address that never carried them gains the item's, which is the same answer
                // the WAL gives and is what the index means by a page of this object.
                "address holds neither" => {
                    let addr = back.address.as_ref().expect("address survives");
                    assert_eq!(addr.object_id(), Some(object_id), "{label}");
                    assert_eq!(addr.routing_bucket(), Some(bucket), "{label}");
                }
                _ => assert_eq!(back, original, "{label} did not round-trip"),
            }
        }
    }

    /// What the address repeats costs, per index item.
    ///
    /// `BlockAddress` carries `object_id` and `routing_bucket`, and the item carries both again as
    /// its own fields. For a page belonging to one object they hold the same value, so the pair is
    /// written twice per item. The WAL side already strips that on its way to protobuf --
    /// `item_to_proto` drops the address's object id when it repeats the item's, and says why --
    /// and this measures whether the index log is paying what the WAL stopped paying.
    #[test]
    #[ignore]
    fn what_the_address_repeats_costs() {
        let object_id = 12_345_678_901_234_567u64;
        let bucket = 8539u32;
        let item = |address| IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: bucket,
            block_ref_key: 17_665_223_918_442_101_733u64.to_string(),
            object_key: "tenant/7/object/000000123".to_string(),
            model_id: "string".to_string(),
            component: None,
            object_id,
            page_id: 7,
            address,
            size: 4096,
            in_log: false,
            deleted: false,
        };

        // As written today: the address repeats the item's object id and routing bucket.
        let repeats = item(Some(crate::block_store::BlockAddress::from_parts(
            42, 1_048_576, 4096, Some(7), Some(object_id), Some(bucket), Some(3),
        )));
        // The same address with the two the item already states left out.
        let deduped = item(Some(crate::block_store::BlockAddress::from_parts(
            42, 1_048_576, 4096, Some(7), None, None, Some(3),
        )));

        let a = encode_index_payload(&repeats, INDEX_LOG_SHAPE_DELTA).expect("encode").len();
        let b = encode_index_payload(&deduped, INDEX_LOG_SHAPE_DELTA).expect("encode").len();
        println!(
            "  REPEATS item {a} B with the repeats, {b} B without -- {} B, {:.1}% of the item",
            a - b,
            100.0 * (a - b) as f64 / a as f64,
        );
        assert!(a >= b, "dropping fields cannot make it larger");
    }

    /// What an index-log item is actually made of, field by field.
    ///
    /// The whole record measures 233 bytes on a real ingest. Before proposing to narrow anything,
    /// find out which field is paying for it -- a guess about which one dominates is how the last
    /// three measurements in this area went wrong.
    ///
    /// Measured by encoding the item, then encoding it again with one field cleared, and taking
    /// the difference. That prices each field in the SHAPE THE LOG ACTUALLY WRITES rather than in
    /// the size of the Rust type.
    #[test]
    #[ignore]
    fn what_an_index_item_is_made_of() {
        let full = IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: 8539,
            block_ref_key: 17_665_223_918_442_101_733u64.to_string(),
            object_key: "tenant/7/object/000000123".to_string(),
            model_id: "string".to_string(),
            component: None,
            object_id: 12_345_678_901_234_567u64,
            page_id: 7,
            address: Some(crate::block_store::BlockAddress::from_parts(
                42, 1_048_576, 4096, Some(7), Some(12_345_678_901_234_567), Some(8539),
                Some(3),
            )),
            size: 4096,
            in_log: false,
            deleted: false,
        };

        let whole = encode_index_payload(&full, INDEX_LOG_SHAPE_DELTA).expect("encode").len();
        let price = |label: &str, mut cleared: IndexItem| {
            let without = encode_index_payload(&cleared, INDEX_LOG_SHAPE_DELTA).expect("encode").len();
            let _ = &mut cleared;
            println!(
                "    ITEMFIELD {label:<14} {:>4} B ({:>4.1}% of {whole})",
                whole.saturating_sub(without),
                100.0 * whole.saturating_sub(without) as f64 / whole as f64
            );
        };

        println!("  ITEM whole record {whole} B");
        price("address", IndexItem { address: None, ..full.clone() });
        price("object_key", IndexItem { object_key: String::new(), ..full.clone() });
        price("page_ref_key", IndexItem { block_ref_key: String::new(), ..full.clone() });
        price("model_id", IndexItem { model_id: String::new(), ..full.clone() });
        price("object_id", IndexItem { object_id: 0, ..full.clone() });

        assert!(whole > 0, "the probe must encode something");
    }


    /// Append a binary-framed record carrying a msgpack payload, after whatever is already in
    /// the log. Returns the path so a test can measure the file.
    #[cfg(test)]
    fn append_binary_framed(dir: &std::path::Path, record: &IndexDeltaRecord) -> PathBuf {
        let mut packed = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut packed).with_struct_map();
        serde::Serialize::serialize(record, &mut serializer).unwrap();
        let mut payload =
            vec![index_container_byte(INDEX_LOG_CODEC_MSGPACK, INDEX_LOG_SHAPE_DELTA)];
        payload.extend_from_slice(&packed);

        let path = index_log_path(dir, record.shard_id);
        let mut file = OpenOptions::new().create(true).append(true).open(&path).unwrap();
        file.write_all(&crate::log_framing::encode_frame(&payload)).unwrap();
        file.sync_all().unwrap();
        path
    }

    /// A record carrying a raw newline inside its payload must still be read whole.
    ///
    /// Sequence 10 encodes as the single byte 0x0A -- a literal newline sitting in the middle
    /// of a record. A reader that finds record boundaries by scanning for '\n' cuts this
    /// record in half; one that takes the length the frame declares does not. This is the
    /// whole reason every read path had to stop splitting on newlines BEFORE a binary payload
    /// could ever be written: with the old reader this record is unreadable, and the sequence
    /// tail scan would have trimmed it off the end of the file as a torn append.
    #[test]
    fn a_record_whose_payload_holds_a_newline_byte_is_read_whole() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store
            .append_delta(3, vec![page_item(1, "first", false)], Vec::new(), Some(1), None, true, true)
            .unwrap();
        drop(store);

        let record = IndexDeltaRecord {
            shard_id: 3,
            sequence: 10,
            items: vec![page_item(2, "second", false)],
            meta: None,
            applied_wal_sequence: Some(2),
            upsert: true,
            key_states: Vec::new(),
            shared_object_key: None,
        };
        let path = append_binary_framed(dir.path(), &record);
        let on_disk = std::fs::read(&path).unwrap();
        assert!(
            on_disk.windows(1).any(|b| b == b"\n"),
            "the log must contain a newline byte for this test to mean anything"
        );

        let reopened = LocalIndexLogStore::new(dir.path());
        let records = reopened.read_delta_records(3, 0).unwrap();
        assert_eq!(records.len(), 2, "both records must survive: {records:?}");
        assert_eq!(records[1].sequence, 10);
        assert_eq!(records[1].items[0].block_ref_key, "second");
        assert_eq!(records[1].applied_wal_sequence, Some(2));
    }

    /// The tail scan trims the file back to its last whole record. Given a record it cannot
    /// find the end of, it would trim a durable record away -- so this pins that it does not.
    #[test]
    fn the_tail_scan_does_not_trim_a_whole_binary_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store
            .append_delta(4, vec![page_item(1, "a", false)], Vec::new(), Some(1), None, true, true)
            .unwrap();
        drop(store);

        let record = IndexDeltaRecord {
            shard_id: 4,
            sequence: 10,
            items: vec![page_item(2, "b", false)],
            meta: None,
            applied_wal_sequence: Some(2),
            upsert: true,
            key_states: Vec::new(),
            shared_object_key: None,
        };
        let path = append_binary_framed(dir.path(), &record);
        let before = std::fs::metadata(&path).unwrap().len();

        // `scan` drives last_sequence_at, which is the path that truncates.
        let reopened = LocalIndexLogStore::new(dir.path());
        reopened.scan(4, 0, u64::MAX, u64::MAX).unwrap();

        let after = std::fs::metadata(&path).unwrap().len();
        assert_eq!(after, before, "the tail scan trimmed a whole record");
        assert_eq!(reopened.read_delta_records(4, 0).unwrap().len(), 2);
    }

    /// `scan_bounded` hands the caller each record's raw framed bytes to ship onward, so its
    /// idea of where a record ends has to be the writer's. A newline slice would hand on half
    /// a record, and the half would still look like a plausible one.
    #[test]
    fn scan_bounded_hands_back_whole_records_not_newline_slices() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store
            .append_delta(6, vec![page_item(1, "a", false)], Vec::new(), Some(1), None, true, true)
            .unwrap();
        drop(store);

        let record = IndexDeltaRecord {
            shard_id: 6,
            sequence: 10,
            items: vec![page_item(2, "b", false)],
            meta: None,
            applied_wal_sequence: Some(2),
            upsert: true,
            key_states: Vec::new(),
            shared_object_key: None,
        };
        append_binary_framed(dir.path(), &record);

        let reopened = LocalIndexLogStore::new(dir.path());
        let (rows, truncated) = reopened.scan_bounded(6, 0, u64::MAX, u64::MAX).unwrap();
        assert!(!truncated, "the whole log fits in the budget");
        assert_eq!(rows.len(), 2, "one row per record");

        // The binary row must be a complete frame on its own, and decode to what was written.
        let (consumed, payload) = crate::log_framing::next_frame(&rows[1].1)
            .unwrap()
            .expect("the row must be a whole frame");
        assert_eq!(consumed, rows[1].1.len(), "the row is exactly one record");
        let back: IndexDeltaRecord = decode_index_payload(payload).unwrap();
        assert_eq!(back, record);
    }

    /// A torn binary tail -- a crash partway through an append -- is trimmed, not reported as
    /// corruption. The distinction matters: trimming committed damage loses durable records,
    /// and erroring on a torn tail wedges a node that merely crashed at the wrong moment.
    #[test]
    fn a_torn_binary_tail_is_trimmed_rather_than_called_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store
            .append_delta(8, vec![page_item(1, "a", false)], Vec::new(), Some(1), None, true, true)
            .unwrap();
        drop(store);

        let record = IndexDeltaRecord {
            shard_id: 8,
            sequence: 10,
            items: vec![page_item(2, "b", false)],
            meta: None,
            applied_wal_sequence: Some(2),
            upsert: true,
            key_states: Vec::new(),
            shared_object_key: None,
        };
        let path = append_binary_framed(dir.path(), &record);
        let whole = std::fs::metadata(&path).unwrap().len();

        // Chop the last few bytes: the frame now declares more than the file holds.
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(whole - 3).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reopened = LocalIndexLogStore::new(dir.path());
        let records = reopened
            .read_delta_records(8, 0)
            .expect("a torn tail is not corruption");
        assert_eq!(records.len(), 1, "the torn record is dropped, the whole one kept");
        assert_eq!(records[0].items[0].block_ref_key, "a");
    }


    /// End to end: what the store writes now is a container in a binary frame, and it reads
    /// back. The unit tests above encode and decode by hand; this one goes through the append
    /// path, the file, and the fold -- which is where a mismatch between writer and reader
    /// would actually show up.
    #[test]
    fn a_record_written_now_is_a_container_and_still_folds() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        store
            .append_delta(9, vec![page_item(1, "a", false)], Vec::new(), Some(1), None, true, true)
            .unwrap();
        store
            .append_delta(9, vec![page_item(2, "b", false)], Vec::new(), Some(2), None, true, true)
            .unwrap();
        drop(store);

        let path = index_log_path(dir.path(), 9);
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(
            raw.first(),
            Some(&crate::log_framing::FRAME_MAGIC_V3),
            "the record carries the binary frame"
        );
        assert!(
            raw.iter().any(|byte| {
                let (codec, shape) = index_container_parts(*byte);
                (codec == INDEX_LOG_CODEC_MSGPACK || codec == INDEX_LOG_CODEC_MSGPACK_ZSTD)
                    && (shape == INDEX_LOG_SHAPE_WHOLE || shape == INDEX_LOG_SHAPE_DELTA)
            }),
            "the payload carries a container byte"
        );
        assert!(
            !raw.starts_with(b"#tsf2 "),
            "nothing should still be writing the text frame"
        );

        let reopened = LocalIndexLogStore::new(dir.path());
        let records = reopened.read_delta_records(9, 0).unwrap();
        assert_eq!(records.len(), 2, "both records fold back: {records:?}");
        assert_eq!(records[0].items[0].block_ref_key, "a");
        assert_eq!(records[1].items[0].block_ref_key, "b");
        assert_eq!(records[1].applied_wal_sequence, Some(2));
    }


    /// GC decodes EVERY record as an IndexLogRecord -- including delta records, of which it
    /// reads only `sequence`. A container that mis-read that one field would make GC retain
    /// everything while reporting nothing reclaimable.
    #[test]
    fn a_delta_container_still_reads_as_a_whole_index_record_for_gc() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for i in 1..=3u64 {
            store
                .append_delta(5, vec![page_item(1, "k", false)], Vec::new(), Some(i), None, true, true)
                .unwrap();
        }
        drop(store);

        let contents = std::fs::read(index_log_path(dir.path(), 5)).unwrap();
        let records = split_records(&contents);
        assert_eq!(records.len(), 3, "three records on disk");

        let sequences: Vec<u64> = records
            .iter()
            .map(|raw| {
                let payload = crate::log_framing::next_frame(raw).unwrap().unwrap().1;
                // What the sweep actually reads: the head of the record, whichever shape it
                // is. A delta no longer decodes as a whole-index record -- rows carry no names to
                // match on -- and the head is what made that unnecessary.
                let record: IndexRecordHead = decode_index_payload(payload)
                    .expect("a delta container must still answer for its sequence");
                record.sequence
            })
            .collect();
        assert_eq!(sequences, vec![1, 2, 3], "GC must see the real sequences");
    }

}
