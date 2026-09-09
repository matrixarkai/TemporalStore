// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};
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
    // struct-as-MAP, not struct-as-array. The array form is positional, which mis-reads a
    // struct that skipped an absent optional, so nothing is skipped now: every record writes
    // every field and a position always means the same thing. The reader that pulled two fields
    // out of an arbitrary record by name reads them by position instead -- see `IndexRecordHead`,
    // which takes the first two values and ignores whatever follows, so a delta record and a
    // whole-index record both answer it.
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
    /// The page handle, carried as text.
    ///
    /// It is a `u64` everywhere else -- `BlockLookupRef::page_ref_key` is one, and the write path
    /// stringifies it on the way in. As decimal text it is 20 bytes of a 161-byte item, 12.4%;
    /// as a number it would be about nine.
    ///
    /// The reader takes either shape as of this change, which is the half that has to land first:
    /// a writer that emitted a number today would hand it to a reader expecting a string, and
    /// msgpack would refuse the type outright rather than degrade. Nothing writes a number yet.
    #[serde(
        rename = "pk",
        alias = "page_ref_key",
        default,
        deserialize_with = "page_ref_key_either_shape",
        serialize_with = "page_ref_key_as_number_when_it_is_one"
    )]
    pub page_ref_key: String,
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
/// Safe to flip because `page_ref_key_either_shape` already takes both, and it has shipped: a
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
    /// `page_ref_key` is `page_ref_key_from_parts` of this item's own model, object key,
    /// component and address -- measured at 43 bytes of a 176-byte record, the largest single
    /// field in the index log, and every byte of it is spelled out again in the fields beside it.
    /// A reader rebuilds it with the same function, so the log carries the parts and not the
    /// concatenation.
    ///
    /// Cleared only on an exact match. Anything the derivation does not reproduce is written as
    /// it stands, so a key that is not the composite -- the numeric handle form, for one --
    /// survives untouched.
    fn strip_page_ref_key_repeat(&mut self) {
        let Some(address) = self.address.as_ref() else {
            return;
        };
        let derived = page_ref_key_from_parts(
            &self.model_id,
            &self.object_key,
            self.component.as_deref(),
            address.block_slab_id,
            address.offset,
            address.length,
            address.page_id().unwrap_or_default(),
            address.generation().unwrap_or_default(),
        );
        if derived == self.page_ref_key {
            self.page_ref_key.clear();
        }
    }

    /// Rebuild the composite key the writer left out.
    ///
    /// The inverse of `strip_page_ref_key_repeat`. A log written before that stripping carries the
    /// key, and this leaves it alone: it fills only what is absent.
    fn restore_page_ref_key_repeat(&mut self) {
        if !self.page_ref_key.is_empty() {
            return;
        }
        let Some(address) = self.address.as_ref() else {
            return;
        };
        self.page_ref_key = page_ref_key_from_parts(
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
    /// An object id is `stable_page_object_id` of the shard, the kind, the key and the
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
        crate::engine::hashing::stable_page_object_id(
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
                page_ref_key_as_number_when_it_is_one(self.0, s)
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
        row.serialize_element(&Handle(&self.page_ref_key))?;
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

fn page_ref_key_as_number_when_it_is_one<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value.parse::<u64>() {
        Ok(number) => serializer.serialize_u64(number),
        Err(_) => serializer.serialize_str(value),
    }
}

fn page_ref_key_either_shape<'de, D>(deserializer: D) -> Result<String, D::Error>
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

/// Band lifecycle state folded into the index-log MetaItem. 1:1 with
/// `block_store::BlockStoreBandState` and with the on-disk band-state encoding
/// (INIT/CREATED/FROZEN/RECYCLED): Active==CREATED, Sealed==FROZEN, DelayedDestroy/Purged
/// cover the RECYCLED grace. Serialized snake_case so it round-trips with the band manifest's
/// own state enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BandCatalogState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

/// One band catalog entry folded into the index-log MetaItem, mirroring this design
/// the durable band catalog in the index-log anchor. Carries the DURABLE catalog fields the
/// band descriptor tracks -- lifecycle state, byte counts, timestamps, page-id range, version.
/// The band descriptor's DIAGNOSTIC fields (readable_prefix_physical_bytes / has_corruption /
/// first_error*) are intentionally ABSENT: they are recomputed on load by scanning the slab
/// (`inspect_slab`, driven by `rebuild_band_manifest_at` / reconcile-on-open), exactly as the
/// are not persisted. So this is the lossless durable projection of a band.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BandCatalogEntry {
    #[serde(alias = "zone_id")]
    #[serde(rename = "page_slab_id")]
    pub block_slab_id: u64,
    pub state: BandCatalogState,
    #[serde(alias = "total_bytes")]
    pub physical_bytes: u64,
    #[serde(default)]
    pub logical_bytes: u64,
    #[serde(default)]
    pub created_unix_ms: Option<u64>,
    #[serde(default)]
    pub updated_unix_ms: Option<u64>,
    #[serde(default)]
    pub first_page_id: Option<u64>,
    #[serde(default)]
    pub last_page_id: Option<u64>,
    #[serde(default)]
    pub version: u64,
}

/// Compaction anchor for the delta log. `start_wal_sequence` is the lowest WAL sequence
/// still required to reconstruct the served index on top of the base snapshot: once the
/// base `shard-{id}.index.json` is rewritten at a compaction point, the anchor advances
/// and every delta record at or before it can be truncated. Matches
/// MetaItem's `start_WAL_id` role in the native WAL vocabulary.
///
/// `bands` folds the band catalog into the anchor, and serializes under its original
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
    pub bands: Vec<BandCatalogEntry>,
    #[serde(rename = "zone_version", default)]
    pub band_version: u64,
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
    /// wipe -- mirroring the write path's upsert_bucket_index_page. When false (the default,
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
}

/// The key a page is written under, from its parts.
///
/// One definition, called from the page index's serialization and from the replay log, so the two
/// cannot drift into different spellings of the same page.
pub fn page_ref_key_from_parts(
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

/// Fold an ordered stream of delta items onto a base map keyed by `(routing_bucket,
/// page_ref_key)`. Later items win; a `deleted` tombstone removes the key. This is the
/// pure replay step: `base` is the page-index projection of the base snapshot and the
/// returned map is the reconstructed, current page index -- O(base + deltas), and the
/// per-write producer side is O(delta).
pub fn fold_index_items(
    base: BTreeMap<(u32, String), IndexItem>,
    deltas: &[IndexItem],
) -> BTreeMap<(u32, String), IndexItem> {
    let mut folded = base;
    for item in deltas {
        let key = (item.routing_bucket, item.page_ref_key.clone());
        if item.deleted {
            folded.remove(&key);
        } else {
            folded.insert(key, item.clone());
        }
    }
    folded
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub last_sequence: u64,
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
    // See `block_store::page_wal_single_barrier`: one reader, in `engine`, so the three barriers
    // this hatch controls cannot end up disagreeing about whether it is set.
    !crate::engine::wal_legacy_recovery()
}

/// MANIFEST-CONFORMANCE FOLD, always on: the band catalog is folded into the index-log
/// anchor at a threshold dump, and the per-write band-manifest file is no longer the catalog's
/// source of truth (it is reconstructed on load from the durable pages plus the folded anchor).
/// `TS_INDEX_CATALOG_FOLD` used to be able to skip all of it. It shipped dark (the flip once
/// broke proxy tests), then defaulted on once the upsert-delta and single-barrier work landed,
/// and nothing ever selected the off position again -- the five tests that exercise the fold
/// pinned it ON rather than off. The threshold dump is also what lets the embedded (proxy)
/// engine reclaim its index-log and WAL at all, so the off side disabled reclaim entirely.
///
/// The off side's compatibility claim still holds and is not conditional on anything: an anchor
/// whose `bands` is empty serializes with no `zones` key, byte-identical to a pre-fold MetaItem.
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

impl LocalIndexLogStore {
    /// On-disk byte length of a shard's index-log file (0 if absent). Used as the "undumped
    /// length" signal for the threshold-dump cadence: the growth of this file since the last
    /// dumped watermark is the undumped-length signal.
    pub fn log_len_bytes(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("index log lock poisoned");
        index_log_path(&inner.root, shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }

    /// Undumped index-log length for a shard: the on-disk byte growth since the last catalog
    /// dump (`mark_catalog_dumped`). This is the native analog of this design
    /// the undumped WAL length, and is the signal compared against `index_dump_wal_gap_bytes`
    /// to decide a threshold dump. A shard never dumped this process (or freshly restarted)
    /// reports the whole current length.
    pub fn undumped_len_since_dump(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("index log lock poisoned");
        let current = index_log_path(&inner.root, shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0);
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
        let current = index_log_path(&inner.root, shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        inner.last_dumped_len_by_shard.insert(shard_id, current);
    }

    /// The most recent `MetaItem` anchor carrying a folded band catalog, or `None` if no
    /// anchor with a non-empty `bands` list has been written. Used on load (gate on) to seed the
    /// block-store band catalog from the folded anchor when the band-manifest file is absent.
    pub fn latest_band_catalog(&self, shard_id: ShardId) -> Result<Option<MetaItem>, IndexLogError> {
        // The LAST matching record wins, so this cannot stop early -- but it never needed to
        // hold the records it walks past. It kept every record in the log, with every item each
        // one carries, to take one field out of one of them.
        let mut latest = None;
        self.for_each_delta_record(shard_id, 0, |record| {
            if let Some(meta) = record.meta {
                if !meta.bands.is_empty() {
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
        let next_sequence = last_sequence.saturating_add(1);
        // Do not write what the item already says. Each item carries `object_id` and
        // `routing_bucket`, and the address it points at repeats both -- 18 bytes of a 142-byte
        // item. `restore_address_repeats` at the decode site puts them back.
        let mut items = items;
        for item in items.iter_mut() {
            item.strip_page_ref_key_repeat();
            item.strip_size_repeat();
            // The address first, because it is stripped against the item's id, and the item's
            // id is what goes next.
            item.strip_address_repeats();
            item.strip_object_id_repeat(shard_id);
        }
        let record = IndexDeltaRecord {
            shard_id,
            sequence: next_sequence,
            items,
            meta,
            applied_wal_sequence,
            key_states,
            upsert,
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
        let path = index_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(());
        }
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut last_sequence = 0_u64;
        // Read by FRAME, not by line. A record's payload may be binary, and a binary payload
        // may contain 0x0A -- a reader splitting on newlines would cut such a record in half
        // and, being `lines()`, would also demand it be valid UTF-8. `read_frame` takes the
        // length the frame declares instead, and reads text-framed and legacy unframed
        // records unchanged, so one loop reads every shape the log has ever held. Streaming
        // rather than reading the file whole keeps memory bounded by the largest record.
        let mut reader = reader;
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
            // value-preserving bit-flip surfaces here as `Corruption`. Consistent with the
            // scan / last_sequence_at path, which already treats interior corruption as fatal.
            // A whole-index record shares this file and is not a delta. It used to be read as
            // one and answer with defaults; now the container says what it is, so it is skipped
            // rather than mis-read.
            // Skip only a payload that SAYS it is the other shape. Anything else -- an
            // unrecognised shape, a truncated container -- goes to the decoder and is reported,
            // because a sweep that quietly skips what it cannot read is how committed corruption
            // becomes silent data loss.
            if index_payload_shape(payload) == Some(INDEX_LOG_SHAPE_WHOLE) {
                continue;
            }
            let mut record: IndexDeltaRecord = decode_index_payload(payload)?;
            // Put back what the writer left out because the item already stated it. A record
            // written before that stripping carries both already, and this leaves those alone.
            for item in record.items.iter_mut() {
                // The item's id first: the address is restored FROM it.
                item.restore_object_id_repeat(record.shard_id);
                item.restore_address_repeats();
                item.restore_page_ref_key_repeat();
                item.restore_size_repeat();
            }
            // Enforce delta sequence-continuity: sequences are assigned strictly monotonically
            // across ALL appended records (whole-index and delta share one counter), and GC
            // only truncates a leading prefix, so in file order each record's sequence must be
            // strictly greater than the previous. A drop below or duplicate means a lost /
            // reordered / corrupted record -- refuse rather than fold a holed delta stream.
            if record.sequence <= last_sequence {
                return Err(IndexLogError::Corruption(format!(
                    "index-log delta sequence continuity violation: record sequence {} is not greater than previous {}",
                    record.sequence, last_sequence
                )));
            }
            last_sequence = record.sequence;
            // A whole-index IndexLogRecord also deserializes into IndexDeltaRecord (its `index`
            // field is ignored, leaving the delta fields empty). Only keep records that carry a
            // delta payload OR a WAL anchor (an anchor-only record still advances the
            // reconstructed watermark on load).
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
        Ok(())
    }

    pub fn read_range(
        &self,
        shard_id: ShardId,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, IndexLogError> {
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let path = index_log_path(&inner.root, shard_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; size as usize];
        let read = file.read(&mut bytes)?;
        bytes.truncate(read);
        inner.stats.reads += 1;
        inner.stats.bytes_read += read as u64;
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
        let mut inner = self.inner.lock().expect("index log lock poisoned");
        let path = index_log_path(&inner.root, shard_id);
        if !path.exists() {
            inner.stats.scans += 1;
            return Ok((Vec::new(), false));
        }
        let _ = last_sequence_at(&inner.root, shard_id)?;
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(start_offset))?;
        let mut reader = BufReader::new(file);
        let mut offset = start_offset;
        let mut total = 0;
        let mut truncated = false;
        let mut records = Vec::new();

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
                break;
            }
            if total + read > max_bytes {
                // Out of budget with the window not yet walked: there is more to read.
                truncated = true;
                break;
            }
            records.push((offset, raw));
            offset = next_offset;
            total += read;
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += total;
        Ok((records, truncated))
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
        // Bounding this per round makes it WORSE. The round rewrites what it retains, so
        // removing fewer records means copying more of them: 40,000 records took 357 ms in one
        // unlimited round against 492 ms for a round limited to 200 -- dearer, and with the rest
        // still to do. Zero, which every caller passes, is the cheap option. See
        // `bounding_the_index_collector_per_round_costs_more_not_less`.
        max_entries_per_round: usize,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        let inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = index_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence,
                max_entries_per_round,
                ..IndexLogGcReport::default()
            });
        }

        let bytes_before = path.metadata()?.len();
        let _ = last_sequence_at(&inner.root, shard_id)?;
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut records_before = 0usize;
        let mut removed_this_round = 0usize;
        let mut removable_records_before_budget = 0usize;
        let mut retained = Vec::new();
        // By frame, not by line: see the fold path above.
        let mut reader = reader;
        while let Some((_, payload)) = crate::log_framing::read_frame(&mut reader)? {
            if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            records_before += 1;
            // Preserve the exact on-disk payload for retained records so a delta record is not
            // silently re-encoded as a whole-index record (which would drop its items/meta).
            // Decode verifies the integrity envelope; the retained raw payload is re-framed on
            // write-out below.
            let record: IndexRecordHead = decode_index_payload(&payload)?;
            if record.sequence < retain_from_sequence {
                removable_records_before_budget = removable_records_before_budget.saturating_add(1);
            }
            if record.sequence >= retain_from_sequence
                || (max_entries_per_round > 0 && removed_this_round >= max_entries_per_round)
            {
                // Retain the EXACT decoded payload, not a re-serialized IndexLogRecord: a delta
                // record also parses as IndexLogRecord (its delta fields are dropped), so
                // re-encoding the parsed struct would silently destroy retained delta items on
                // a GC round. Re-frame the untouched payload on write-out below.
                retained.push(payload);
            } else {
                removed_this_round = removed_this_round.saturating_add(1);
            }
        }

        let temp_path = path.with_extension("jsonl.tmp");
        {
            let mut temp = File::create(&temp_path)?;
            for payload in &retained {
                temp.write_all(&crate::log_framing::encode_record(payload))?;
            }
            temp.flush()?;
            crate::durability_metrics::record_barrier("engine_index_log_gc");
            temp.sync_all()?;
        }
        fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        let bytes_after = path.metadata()?.len();
        Ok(IndexLogGcReport {
            shard_id,
            retain_from_sequence,
            max_entries_per_round,
            records_before,
            records_after: retained.len(),
            records_removed: records_before.saturating_sub(retained.len()),
            removable_records_before_budget,
            budget_exhausted: max_entries_per_round > 0
                && removable_records_before_budget > max_entries_per_round,
            bytes_before,
            bytes_after,
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
    pub fn gc_reflected_before_anchor(
        &self,
        shard_id: ShardId,
        wal_anchor: u64,
        meta_sequence: u64,
    ) -> Result<IndexLogGcReport, IndexLogError> {
        // A SECOND reader of the same on-disk record, so it has to know every spelling the
        // record has ever used. It previously named the long forms only; when those were
        // shortened this probe stopped seeing `sequence` at all and read
        // `applied_wal_sequence` as None, which silently changed what the sweep removed.
        #[derive(serde::Deserialize)]
        struct AnchorProbe {
            #[serde(rename = "q", alias = "sequence")]
            sequence: u64,
            #[serde(rename = "aw", alias = "applied_wal_sequence", default)]
            applied_wal_sequence: Option<u64>,
        }
        let inner = self.inner.lock().expect("index log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = index_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(IndexLogGcReport {
                shard_id,
                retain_from_sequence: meta_sequence,
                ..IndexLogGcReport::default()
            });
        }

        let bytes_before = path.metadata()?.len();
        let _ = last_sequence_at(&inner.root, shard_id)?;
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut records_before = 0usize;
        let mut retained = Vec::new();
        // By frame, not by line: see the fold path above.
        let mut reader = reader;
        while let Some((_, payload)) = crate::log_framing::read_frame(&mut reader)? {
            if payload.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            records_before += 1;
            // Decode verifies the integrity envelope; the retained raw payload is re-framed on
            // write-out below, so a retained delta record keeps its exact on-disk bytes.
            let probe: IndexRecordHead = decode_index_payload(&payload)?;
            let reflected = probe.applied_wal_sequence.unwrap_or(0) <= wal_anchor;
            if probe.sequence >= meta_sequence || !reflected {
                retained.push(payload);
            }
        }

        let temp_path = path.with_extension("jsonl.tmp");
        {
            let mut temp = File::create(&temp_path)?;
            for payload in &retained {
                temp.write_all(&crate::log_framing::encode_record(payload))?;
            }
            temp.flush()?;
            crate::durability_metrics::record_barrier("engine_index_log_gc");
            temp.sync_all()?;
        }
        fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        let bytes_after = path.metadata()?.len();
        Ok(IndexLogGcReport {
            shard_id,
            retain_from_sequence: meta_sequence,
            max_entries_per_round: 0,
            records_before,
            records_after: retained.len(),
            records_removed: records_before.saturating_sub(retained.len()),
            removable_records_before_budget: records_before.saturating_sub(retained.len()),
            budget_exhausted: false,
            bytes_before,
            bytes_after,
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
    let path = index_log_path(root, shard_id);
    if !path.exists() {
        return Ok(0);
    }
    let file = OpenOptions::new().read(true).write(true).open(&path)?;
    let mut reader = BufReader::new(file.try_clone()?);
    let mut last = 0;
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
            page_ref_key: key.to_string(),
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
    /// It is `stable_page_object_id` of the shard, the model, the key and the component -- and a
    /// row carries the last three while the record carries the shard. Nine bytes restating a
    /// hash of fields sitting beside it.
    #[test]
    fn a_row_does_not_write_the_object_id_it_can_derive() {
        let shard_id: ShardId = 7;
        let mut derivable = page_item(3, "tenant/1/object/9", false);
        derivable.object_id = crate::engine::hashing::stable_page_object_id(
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
        assert_eq!(records[1].items[0].page_ref_key, "b");
        // The sequence tail is readable across a reopen even though the log now holds
        // delta records rather than whole-index records.
        let reopened = LocalIndexLogStore::new(dir.path());
        assert_eq!(reopened.stats(7).last_sequence, 2);
    }

    #[test]
    fn fold_index_items_applies_tombstones_and_last_writer_wins() {
        let mut base = BTreeMap::new();
        base.insert((1u32, "a".to_string()), page_item(1, "a", false));
        base.insert((1u32, "b".to_string()), page_item(1, "b", false));
        // Delta: delete a, overwrite b, add c.
        let mut updated_b = page_item(1, "b", false);
        updated_b.size = 99;
        let deltas = vec![page_item(1, "a", true), updated_b, page_item(2, "c", false)];
        let folded = fold_index_items(base, &deltas);
        assert!(!folded.contains_key(&(1, "a".to_string())));
        assert_eq!(folded.get(&(1, "b".to_string())).unwrap().size, 99);
        assert!(folded.contains_key(&(2, "c".to_string())));
        assert_eq!(folded.len(), 2);
    }

    /// The band catalog is the LAST record that carries one, and nothing after it clears it.
    ///
    /// The fold keeps a running answer instead of every record it walks past, so three things
    /// an ordering change can break are pinned here: a later catalog must WIN; a later record
    /// carrying a meta with NO bands must not replace it; and a log whose only meta carries no
    /// bands must answer None rather than that meta.
    ///
    /// The second and third are what make this more than one assertion. Writing it with only a
    /// catalog-less record after the catalogs let a fold that kept "the last meta, bands or not"
    /// pass -- the record had no meta at all, so the wrong rule never fired.
    #[test]
    fn the_band_catalog_is_the_last_record_that_carries_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());

        let catalog = |slab: u64| MetaItem {
            version: 1,
            start_wal_sequence: 1,
            timestamp_ms: 1,
            band_version: 1,
            bands: vec![BandCatalogEntry {
                block_slab_id: slab,
                state: BandCatalogState::Active,
                physical_bytes: 1,
                logical_bytes: 1,
                created_unix_ms: None,
                updated_unix_ms: None,
                first_page_id: None,
                last_page_id: None,
                version: 1,
            }],
        };
        // A meta that carries no bands. An anchor looks like this whenever the fold is off.
        let bandless = MetaItem {
            version: 2,
            start_wal_sequence: 2,
            timestamp_ms: 2,
            band_version: 0,
            bands: Vec::new(),
        };

        store
            .append_delta(4, Vec::new(), Vec::new(), None, Some(catalog(11)), false, true)
            .unwrap();
        store
            .append_delta(4, Vec::new(), Vec::new(), None, Some(catalog(22)), false, true)
            .unwrap();
        // A meta AFTER the catalogs that carries none of its own.
        store
            .append_delta(4, Vec::new(), Vec::new(), None, Some(bandless.clone()), false, true)
            .unwrap();
        // And a record with no meta at all.
        store
            .append_delta(4, vec![page_item(1, "later", false)], Vec::new(), None, None, false, true)
            .unwrap();

        let found = store.latest_band_catalog(4).unwrap().expect("a catalog");
        assert_eq!(
            found.bands.first().map(|band| band.block_slab_id),
            Some(22),
            "the last record CARRYING a catalog wins, and neither record after it clears it"
        );

        // A log whose only meta carries no bands has no catalog to find. This is the control:
        // a fold that kept the last meta whether or not it held bands passes the assertion
        // above and fails here.
        store
            .append_delta(5, Vec::new(), Vec::new(), None, Some(bandless), false, true)
            .unwrap();
        assert!(
            store.latest_band_catalog(5).unwrap().is_none(),
            "a meta carrying no bands is not a catalog"
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
        assert_eq!(suffix[0].items[0].page_ref_key, "b");
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

        let report = store.gc_reflected_before_anchor(7, 2, meta_sequence).unwrap();
        assert_eq!(report.records_before, 4);
        assert_eq!(report.records_removed, 2, "the whole-index line and the covered delta go");
        assert!(report.bytes_after < report.bytes_before);

        let survivors = store.read_delta_records(7, 0).unwrap();
        assert_eq!(survivors.len(), 2);
        assert_eq!(
            survivors[0].items[0].page_ref_key, "racing",
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
            page_ref_key: "string:m:0::0:0:126:0:0".to_string(),
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
        // Pre-fold compatibility invariant: an anchor whose `bands` is empty must serialize with
        // NO `zones` key and NO
        // `band_version` beyond what a pre-fold MetaItem produced. `band_version` defaults to 0
        // and is not skipped, so it appears; assert the value carries only the legacy three
        // fields plus a zero band_version and no zones array.
        let meta = MetaItem {
            version: 7,
            start_wal_sequence: 11,
            timestamp_ms: 22,
            ..MetaItem::default()
        };
        let value = serde_json::to_value(&meta).unwrap();
        let object = value.as_object().unwrap();
        // Every field is written now, empty or not: a row is read by position, so a field that
        // disappears when it is empty moves every field behind it. An empty band list costs one
        // byte and keeps the position of everything after it.
        assert_eq!(
            object.get("zones").expect("bands are always written"),
            &serde_json::json!([]),
            "an empty band list is written, not skipped"
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
    fn meta_item_band_catalog_round_trips_through_the_delta_log() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let meta = MetaItem {
            version: 1,
            start_wal_sequence: 5,
            timestamp_ms: 100,
            band_version: 3,
            bands: vec![
                BandCatalogEntry {
                    block_slab_id: 0,
                    state: BandCatalogState::Sealed,
                    physical_bytes: 4096,
                    logical_bytes: 4000,
                    created_unix_ms: Some(10),
                    updated_unix_ms: Some(20),
                    first_page_id: Some(0),
                    last_page_id: Some(9),
                    version: 3,
                },
                BandCatalogEntry {
                    block_slab_id: 1,
                    state: BandCatalogState::Active,
                    physical_bytes: 512,
                    logical_bytes: 512,
                    created_unix_ms: Some(30),
                    updated_unix_ms: Some(30),
                    first_page_id: Some(10),
                    last_page_id: Some(10),
                    version: 3,
                },
            ],
        };
        store
            .append_delta(4, Vec::new(), Vec::new(), Some(5), Some(meta.clone()), false, true)
            .unwrap();
        // A reopen reads the folded catalog back exactly, and latest_band_catalog finds it.
        let reopened = LocalIndexLogStore::new(dir.path());
        let recovered = reopened.latest_band_catalog(4).unwrap().unwrap();
        assert_eq!(recovered, meta);
        assert_eq!(recovered.bands.len(), 2);
        assert_eq!(recovered.bands[0].state, BandCatalogState::Sealed);
        assert_eq!(recovered.bands[1].block_slab_id, 1);
    }

    #[test]
    fn latest_band_catalog_prefers_the_newest_anchor_and_ignores_empty_ones() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        let older = MetaItem {
            version: 1,
            start_wal_sequence: 1,
            timestamp_ms: 1,
            band_version: 1,
            bands: vec![BandCatalogEntry {
                block_slab_id: 0,
                state: BandCatalogState::Active,
                physical_bytes: 1,
                logical_bytes: 1,
                created_unix_ms: None,
                updated_unix_ms: None,
                first_page_id: None,
                last_page_id: None,
                version: 1,
            }],
        };
        let newer = MetaItem {
            version: 2,
            start_wal_sequence: 9,
            timestamp_ms: 9,
            band_version: 2,
            bands: vec![BandCatalogEntry {
                block_slab_id: 0,
                state: BandCatalogState::Sealed,
                physical_bytes: 2,
                logical_bytes: 2,
                created_unix_ms: None,
                updated_unix_ms: None,
                first_page_id: None,
                last_page_id: None,
                version: 2,
            }],
        };
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(1), Some(older), false, true)
            .unwrap();
        // An anchor with no bands between them must not shadow the folded catalog.
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(5), Some(MetaItem::default()), false, true)
            .unwrap();
        store
            .append_delta(6, Vec::new(), Vec::new(), Some(9), Some(newer.clone()), false, true)
            .unwrap();
        assert_eq!(store.latest_band_catalog(6).unwrap().unwrap(), newer);
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

    /// Bounding this collector per round costs MORE, not less.
    ///
    /// Every other sweep here is bounded per round, this one takes a limit, and the production
    /// path never passes one -- which looks exactly like an oversight worth fixing. It is not.
    ///
    /// The collector rewrites what it KEEPS: retained records are copied into a fresh file. So a
    /// round that removes fewer records retains more and copies more. Timed on one round, removing
    /// everything but the last record: 2,000 records took 22.7 ms unlimited against 29.0 ms
    /// limited to 200; 40,000 took 356.7 ms against 492.0 ms. The bounded round is dearer and
    /// leaves the rest of the work for later rounds that are dearer still.
    ///
    /// The assertion is on bytes rewritten rather than time, because that is the thing that makes
    /// it true and it does not depend on the machine.
    #[test]
    fn bounding_the_index_collector_per_round_costs_more_not_less() {
        let records = 4_000usize;
        let mut rewritten = Vec::new();
        for limit in [0usize, 200] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalIndexLogStore::new(dir.path());
            for value in 0..records {
                store
                    .append_json(5, format!("{{\"value\":{value}}}").as_bytes())
                    .unwrap();
            }
            let report = store
                .gc_before_sequence_limited(5, records as u64, limit)
                .unwrap();
            // What the round costs is what it copies, which is what it retained.
            rewritten.push((limit, report.records_removed, report.bytes_after));
        }

        let (_, unlimited_removed, unlimited_bytes) = rewritten[0];
        let (_, limited_removed, limited_bytes) = rewritten[1];
        assert_eq!(unlimited_removed, records - 1, "unlimited should clear the log");
        assert_eq!(limited_removed, 200, "the limit should be respected");
        assert!(
            limited_bytes > unlimited_bytes * 10,
            "a bounded round should be shown rewriting far more than an unbounded one \
             ({limited_bytes} bytes against {unlimited_bytes}); if that is no longer true the \
             collector has stopped copying what it keeps, and a per-round bound may now be worth \
             having"
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
        assert_eq!(b.items[0].page_ref_key, "b");
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
            page_ref_key: String,
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
            page_ref_key: "17665223918442101733".to_string(),
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
            page_ref_key: "17665223918442101733".to_string(),
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
    fn a_page_handle_reads_as_a_number_or_a_string() {
        // The same field names the item uses, with the handle as a NUMBER -- what a future writer
        // would produce.
        #[derive(serde::Serialize)]
        struct NumericHandle {
            #[serde(rename = "k")]
            kind: IndexItemKind,
            #[serde(rename = "rb")]
            routing_bucket: u32,
            #[serde(rename = "pk")]
            page_ref_key: u64,
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
            page_ref_key: handle,
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
            decoded.page_ref_key,
            handle.to_string(),
            "a handle written as a number must come back as the same handle"
        );
        assert_eq!(decoded.routing_bucket, 8539, "the rest of the item must survive too");

        // The text shape still reads, and is still what gets written.
        let textual = IndexItem {
            kind: IndexItemKind::Page,
            routing_bucket: 8539,
            page_ref_key: handle.to_string(),
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
        assert_eq!(round_tripped.page_ref_key, handle.to_string());

        // The writer now emits the number, so `as_text` above is ALSO numeric and the two are no
        // longer a text-vs-number comparison -- they are two different structs. `NumericHandle`
        // is a hand-rolled mirror that does not skip its false bools, where `IndexItem` does, so
        // it comes out SEVEN BYTES LARGER despite carrying the same handle. Comparing them was
        // only ever meaningful while the text penalty outweighed that difference.
        //
        // Compare like with like instead: the same item, with a handle that parses as a number
        // and one of the same length that does not.
        let mut unparseable = textual.clone();
        unparseable.page_ref_key = format!("x{}", &handle.to_string()[1..]);
        assert_eq!(
            unparseable.page_ref_key.len(),
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
            page_ref_key: 17_665_223_918_442_101_733u64.to_string(),
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
                42, 1_048_576, 4096, Some(7), Some(object_id), Some(bucket), Some(3), Some(9)))),
            ("address holds a DIFFERENT object", Some(crate::block_store::BlockAddress::from_parts(
                42, 0, 0, None, Some(object_id + 1), Some(bucket + 1), None, None))),
            ("address holds neither", Some(crate::block_store::BlockAddress::from_parts(
                42, 0, 0, None, None, None, None, None))),
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
            page_ref_key: 17_665_223_918_442_101_733u64.to_string(),
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
            42, 1_048_576, 4096, Some(7), Some(object_id), Some(bucket), Some(3), Some(9),
        )));
        // The same address with the two the item already states left out.
        let deduped = item(Some(crate::block_store::BlockAddress::from_parts(
            42, 1_048_576, 4096, Some(7), None, None, Some(3), Some(9),
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
            page_ref_key: 17_665_223_918_442_101_733u64.to_string(),
            object_key: "tenant/7/object/000000123".to_string(),
            model_id: "string".to_string(),
            component: None,
            object_id: 12_345_678_901_234_567u64,
            page_id: 7,
            address: Some(crate::block_store::BlockAddress::from_parts(
                42, 1_048_576, 4096, Some(7), Some(12_345_678_901_234_567), Some(8539),
                Some(3), Some(9),
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
        price("page_ref_key", IndexItem { page_ref_key: String::new(), ..full.clone() });
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
        assert_eq!(records[1].items[0].page_ref_key, "second");
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
        assert_eq!(records[0].items[0].page_ref_key, "a");
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
        assert_eq!(records[0].items[0].page_ref_key, "a");
        assert_eq!(records[1].items[0].page_ref_key, "b");
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
