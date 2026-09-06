// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The write-ahead log.
//!
//! A shard's log is a sequence of pieces. `shard-{id}.wal.jsonl` is the one being written;
//! sealed ones are `shard-{id}.wal.{start_log_id:020}.jsonl`, named so they sort into log order.
//! `TS_WAL_SEGMENT_BYTES` sets when a piece is sealed. It defaults to
//! [`DEFAULT_WAL_SEGMENT_BYTES`], 256 KiB, so a log ROLLS unless something says
//! otherwise. Zero is still accepted and still means never seal, which leaves the log a
//! single file behaving exactly as it did before pieces existed.
//!
//! Positions are **log ids**: a byte position in the log's whole history, not in whichever file
//! happens to hold it. Each piece records in its first bytes the log id its contents start at, so
//! a log id says which piece holds a record and where inside it, by arithmetic. A log id keeps
//! meaning across pieces, and survives reclaim.
//!
//! Reclaim unlinks whole pieces that hold nothing above the retain floor, and copies only within
//! the piece being written.
//!
//! # Two things deliberately absent
//!
//! **There is no per-piece index from record number to byte offset.** A segmented log usually
//! carries one, as a sidecar file, so that "read entries N through M" is a seek rather than a walk
//! from the start of the piece. Here it would have no reader. Random access is by log id, which is
//! already arithmetic and needs no index; and the only component that reads the log sequentially is
//! recovery (`engine::lifecycle::replay_wal_into_shard`), which starts at a watermark and reads
//! forward to the end, so a seek into the middle buys it nothing. Replication does not read this
//! log at all -- the raft log is a separate structure under `raft::local_wal`. Adding the sidecar
//! would mean another file to write, checksum, recover and keep consistent with the piece beside
//! it, in exchange for nothing, and it would have to be correct across reclaim and a torn tail.
//!
//! Should something ever need to find a record by sequence without reading forward to it, this is
//! the thing to build, and the reason it was not built is only that nothing needed it.
//!
//! **Pieces are not preallocated.** Writing into a file that is already its full size avoids
//! persisting a new size on every barrier, which measured 42.7% cheaper per record at eight
//! records per barrier. It is not done because the file's length is what tells the log where its
//! records end -- used to pick the next write offset, find the tail, repair a torn tail, seal a
//! piece, and bound every reader -- so preallocating means threading a separate logical end
//! through all of them. Reserving blocks without changing the size (`FALLOC_FL_KEEP_SIZE`), which
//! would need none of that, was measured and made no difference: the cost is persisting the size,
//! not allocating the blocks.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{Command, ShardId};

#[derive(Debug, Error)]
pub enum WriteAheadLogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A committed, newline-terminated record failed its per-record integrity envelope (the
    /// length or CRC/SHA-256 digest did not match the payload). This is silent value-loss --
    /// a bit-flip that still parses as JSON -- so recovery surfaces it as data loss and
    /// refuses to trust the record rather than replaying the corrupted value.
    #[error("wal record integrity error: {0}")]
    Corruption(String),
}

impl From<crate::log_framing::FramingError> for WriteAheadLogError {
    fn from(err: crate::log_framing::FramingError) -> Self {
        WriteAheadLogError::Corruption(err.0)
    }
}

fn current_write_ahead_log_format_version() -> u32 {
    WRITE_AHEAD_LOG_FORMAT_VERSION
}

fn is_current_write_ahead_log_format_version(version: &u32) -> bool {
    *version == WRITE_AHEAD_LOG_FORMAT_VERSION
}

/// Decode one raw WAL line (framed or legacy-unframed) into a [`WriteAheadLogRecord`],
/// verifying the per-record integrity envelope when present. Used by every WAL reader
/// (`last_wal_sequence_at`, `scan` consumers, GC, `info`) so a value-preserving bit-flip in a
/// committed record surfaces as `Corruption` instead of replaying as truth.
/// Encode a record exactly as the append path would frame it.
///
/// Exposed so a test can compare encodings of the SAME record rather than of two separately
/// written logs, which is the only way to say anything about fidelity.
pub fn encode_wal_line_for_test(
    record: &WriteAheadLogRecord,
) -> Result<Vec<u8>, WriteAheadLogError> {
    Ok(crate::log_framing::encode_line(&encode_wal_payload(record)?))
}


/// Read one record's bytes AS FRAMED, or `None` at the end of the records.
///
/// Lives in `log_framing` now, because the served-index log needs exactly this reader for
/// exactly this reason and a second copy of the rule would be free to drift from this one.
fn read_raw_record<R: std::io::BufRead>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    crate::log_framing::read_raw_record(reader)
}

pub fn decode_wal_line(line: &[u8]) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
    let payload = crate::log_framing::decode_line(line)?;
    // Which encoding a payload is in is a property of the payload, never of configuration: a log
    // written across a flag change still reads end to end.
    if payload.first() == Some(&crate::wal_proto::BINARY_PAYLOAD_MARKER)
        || payload.first() == Some(&crate::wal_proto::RAW_PAYLOAD_MARKER)
        || payload.first() == Some(&crate::wal_proto::COMPRESSED_RAW_PAYLOAD_MARKER)
        || payload.first() == Some(&crate::wal_proto::COMPRESSED_ESCAPED_PAYLOAD_MARKER)
    {
        return crate::wal_proto::decode(payload).map_err(|err| {
            WriteAheadLogError::Corruption(format!("engine wal record decode failed: {err}"))
        });
    }
    let (document, carried) = split_carried_payloads(payload)?;
    if carried.is_empty() {
        return Ok(serde_json::from_slice::<WriteAheadLogRecord>(document)?);
    }
    crate::bytes_serde::with_payloads(carried, || {
        serde_json::from_slice::<WriteAheadLogRecord>(document)
    })
    .map_err(WriteAheadLogError::from)
}

/// Separates the document from the payloads carried beside it.
///
/// A serde_json document never contains a literal 0x1f -- control bytes are escaped -- so this can
/// only be the separator this writer put there. A record written without carried payloads has none
/// and is returned whole.
const CARRIED_SEPARATOR: u8 = 0x1f;

fn split_carried_payloads(payload: &[u8]) -> Result<(&[u8], Vec<Vec<u8>>), WriteAheadLogError> {
    let Some(document_end) = payload.iter().position(|byte| *byte == CARRIED_SEPARATOR) else {
        return Ok((payload, Vec::new()));
    };
    let document = &payload[..document_end];
    let rest = &payload[document_end + 1..];
    let lengths_end = rest
        .iter()
        .position(|byte| *byte == CARRIED_SEPARATOR)
        .ok_or_else(|| {
            WriteAheadLogError::Corruption(
                "record carries payloads but does not say how long they are".to_string(),
            )
        })?;
    let lengths = std::str::from_utf8(&rest[..lengths_end])
        .map_err(|_| {
            WriteAheadLogError::Corruption("payload lengths are not readable".to_string())
        })?
        .split(',')
        .map(|length| {
            length.parse::<usize>().map_err(|_| {
                WriteAheadLogError::Corruption(format!("payload length is not a number: {length}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut carried = Vec::with_capacity(lengths.len());
    let mut at = lengths_end + 1;
    for length in lengths {
        let end = at.checked_add(length).filter(|end| *end <= rest.len()).ok_or_else(|| {
            WriteAheadLogError::Corruption(
                "record claims more carried payload than it holds".to_string(),
            )
        })?;
        carried.push(
            crate::bytes_serde::unescape_payload(&rest[at..end])
                .map_err(WriteAheadLogError::Corruption)?,
        );
        at = end;
    }
    if at != rest.len() {
        return Err(WriteAheadLogError::Corruption(
            "record holds more carried payload than it claims".to_string(),
        ));
    }
    Ok((document, carried))
}

/// Encode a record: the document, and the payloads worth carrying beside it.
fn encode_wal_payload(record: &WriteAheadLogRecord) -> Result<Vec<u8>, WriteAheadLogError> {
    if crate::wal_proto::binary_records_enabled() {
        return crate::wal_proto::encode(record).map_err(|err| {
            WriteAheadLogError::Corruption(format!("engine wal record encode failed: {err}"))
        });
    }
    let (document, carried) =
        crate::bytes_serde::carrying_payloads(|| serde_json::to_vec(record));
    let mut payload = document?;
    if carried.is_empty() {
        // Nothing worth carrying: written exactly as it always was.
        return Ok(payload);
    }
    let escaped = carried
        .iter()
        .map(|bytes| crate::bytes_serde::escape_payload(bytes))
        .collect::<Vec<_>>();
    let lengths = escaped
        .iter()
        .map(|bytes| bytes.len().to_string())
        .collect::<Vec<_>>()
        .join(",");
    payload.push(CARRIED_SEPARATOR);
    payload.extend_from_slice(lengths.as_bytes());
    payload.push(CARRIED_SEPARATOR);
    for bytes in &escaped {
        payload.extend_from_slice(bytes);
    }
    Ok(payload)
}
impl WalOutcomeItem {
    /// The address with the routing bucket the item carries put back.
    ///
    /// Anything installing a recorded page must go through this rather than reading `address`
    /// directly, or the index entry it builds is missing its routing bucket -- which the
    /// bucket-index half of the equivalence gate fails on.
    pub fn resolved_address(&self) -> Option<crate::block_store::BlockAddress> {
        self.address.clone().map(|mut address| {
            address.set_routing_bucket(Some(self.routing_bucket));
            address
        })
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteAheadLogRecord {
    // The names below are deliberately short. They are repeated in every record for the life of
    // the log, and on a small write they cost more than the data does; the alias on each keeps
    // every record already written readable.
    #[serde(rename = "s", alias = "shard_id")]
    pub shard_id: ShardId,
    #[serde(rename = "q", alias = "sequence")]
    pub sequence: u64,
    /// The operation this write performed, for a record that cannot say what it DID.
    ///
    /// Absent once a record carries its results, which is the point: replaying an operation
    /// reproduces state only if everything that influenced it is reproduced too, and a record that
    /// states results needs none of that. Present on every record written before results existed,
    /// and on any write that still records nothing, so both replay exactly as they always did.
    #[serde(
        rename = "c",
        alias = "command",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub command: Option<Command>,
    #[serde(
        rename = "m",
        alias = "metadata",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub metadata: Option<WriteAheadLogRecordMetadata>,
    /// Pages this write produced, carried in the record that records the write.
    ///
    /// A page is often derived state rather than the command's own bytes, so it cannot be
    /// rebuilt from the command alone. Carrying it here is what lets a read serve the page back
    /// out of the log. Empty and skipped for every write that stages nothing, so records
    /// written without this are byte-identical to before.
    #[serde(
        rename = "p",
        alias = "staged_pages",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub staged_pages: Vec<StagedPage>,
    /// What this write did, stated as results rather than as the operation that caused them.
    ///
    /// Called `outcomes` and not `items` on purpose: [`WriteAheadLogRecordMetadata`] already has
    /// an `items` field describing the record's shape, and two different `items` on one record
    /// would be read wrong by whoever came next.
    ///
    /// Empty and skipped unless [`wal_outcome_items_enabled`], so every record written without
    /// it is byte-identical to before.
    #[serde(
        rename = "t",
        alias = "outcomes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub outcomes: Vec<WalOutcomeItem>,
}
/// TS_WAL_DATA_ONLY: stop writing the operation into a record that already states its results.
///
/// Default ON. Carrying both is strictly bigger for no benefit -- the results are what replay
/// installs, and the operation is consulted only when there are none. Set to a falsey value to
/// keep writing both, which is what a consumer reading records directly would want.
pub fn wal_data_only_enabled() -> bool {
    std::env::var("TS_WAL_DATA_ONLY")
        .map(|value| !(value == "0" || value.eq_ignore_ascii_case("false")))
        .unwrap_or(true)
}

/// What a record should carry as its operation, given what it recorded and what is behind it.
///
/// A result names where a block LANDED. Installing it reproduces the write only if a reader can
/// then FIND that block, and there are two ways it cannot.
///
/// An asynchronous write leaves its block buffered, so a crash can leave a record whose result
/// points at bytes that were never written. And a write whose block travels INSIDE the record --
/// the log-resident path -- needs that block registered against the record's log id before any
/// read can resolve it, which the write path does and installing an address does not.
///
/// So results replace the operation when the block behind them can be found afterwards: a
/// synchronous write whose block is already in the block store, or ANY write carrying its block
/// inside the record, because replay now registers what it replays. What is left is the
/// asynchronous write with nothing carried -- its result names a block-store address a crash may
/// leave unwritten, and registering cannot conjure a block that was never stored. That one keeps
/// its operation and recovers by re-running it.
///
/// Both halves were found the same way: a recovery test failing 8 runs in 12 against 0 in 12 on
/// the code before this, measured interleaved on one machine because an uncontrolled comparison
/// had already told me the opposite once.
///
/// The carried-block half of that measurement predates replay registering blocks. The rule tried
/// then -- accepting carried blocks -- failed 5 runs in 12 precisely because nothing registered
/// them on the way back in. That is now done, which is what makes the same rule correct.
pub(crate) fn record_command(
    command: Command,
    outcomes: &[WalOutcomeItem],
    blocks_are_recoverable: bool,
) -> Option<Command> {
    if outcomes.is_empty() || !wal_data_only_enabled() || !blocks_are_recoverable {
        Some(command)
    } else {
        None
    }
}


/// TS_WAL_OUTCOME_ITEMS: also record what a write DID, beside the command that did it.
///
/// The log records commands, so replay re-executes them -- which reproduces state only if
/// everything that influenced the original execution is reproduced too. That is why replay has
/// to walk a config log to re-apply the eviction config effective at each record, and why it
/// pins a replay clock so TTLs resolve against the leader's timestamp instead of the restart
/// clock. Both are scar tissue from logging operations rather than results.
///
/// An outcome states the result instead: this object's page now lives at this address, or this
/// object is gone. Replay can install that without running anything.
///
/// DEFAULT ON. The comment here used to say "default OFF while both are carried", and described a
/// state that no longer exists: records no longer carry both. A mixed workload was walked across
/// every write shape -- synchronous and asynchronous, separate and batched -- and ZERO records
/// carry an operation, so the payoff this flag was waiting for has arrived and the command has
/// come out.
///
/// The flip was correct; the comment simply outlived it. A flag comment that states the opposite
/// default from the code is worse than no comment, because the two are indistinguishable from
/// outside: a stale note and a default nobody meant to change read exactly alike.
pub fn wal_outcome_items_enabled() -> bool {
    // Default ON. Recording stopped costing the group-commit coalescing once results travelled
    // through the reserve-only append, and recovery prefers installing them over re-running an
    // operation wherever it has them. The variable now opts OUT.
    !matches!(
        std::env::var("TS_WAL_OUTCOME_ITEMS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

fn outcome_not_deleted(deleted: &bool) -> bool {
    !*deleted
}
/// The address inside a recorded outcome, without the routing bucket the item already carries.
///
/// The item and its address both state a routing bucket, always the same one, so every recorded
/// outcome said it twice.
///
/// Their `object_id`s look equally redundant and are NOT. For a timestamped kind the item's is
/// derived from (kind, key, COMPONENT) and the page's from (kind, key, None) -- per point against
/// per series -- so restoring one from the other writes the wrong id into the index entry. The
/// bucket-index half of the equivalence gate is what caught that; the field stays.
///
/// The page checksum stays too: the read path verifies it whenever it is present, and a rebuilt
/// index entry without one would quietly stop being integrity-checked.
mod outcome_address_serde {
    use crate::block_store::BlockAddress;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<BlockAddress>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            None => serializer.serialize_none(),
            Some(address) => {
                let mut trimmed = address.clone();
                trimmed.set_routing_bucket(None);
                Some(trimmed).serialize(serializer)
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<BlockAddress>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<BlockAddress>::deserialize(deserializer)
    }
}


/// One index mutation a write produced, stated as a result.
///
/// Field for field this is the identity half of the log item being followed: the object it
/// names, the kind/model it belongs to, its routing bucket, its object id, and where its page
/// ended up. `deleted` covers the other outcome. Nothing here says which command ran, because
/// replay does not need to know -- that is the entire point.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalOutcomeItem {
    #[serde(rename = "k", alias = "kind")]
    pub kind: String,
    #[serde(rename = "o", alias = "object_key")]
    pub object_key: String,
    #[serde(
        rename = "c",
        alias = "component",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub component: Option<String>,
    #[serde(rename = "i", alias = "object_id")]
    pub object_id: u64,
    #[serde(rename = "b", alias = "routing_bucket")]
    pub routing_bucket: u32,
    /// Where the page went. `None` for state that no page backs -- the seen-sets and token
    /// buckets persist with the index snapshot and have no address to name.
    #[serde(
        rename = "a",
        alias = "address",
        default,
        with = "outcome_address_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub address: Option<crate::block_store::BlockAddress>,
    /// The bytes themselves, for an outcome with no page behind it.
    ///
    /// A coverage probe over twelve accepted writes found four that recorded nothing --
    /// BucketTake and SeenCheck because their state has no page, CommonDelete and CommonExpire
    /// because they remove or re-stamp rather than upsert. An item carrying only an address
    /// cannot state those outcomes, which is why the design being followed gives its log item a
    /// `value` beside its `page`, and a `meta_log` flag to tell them apart.
    #[serde(
        rename = "v",
        alias = "value",
        default,
        skip_serializing_if = "Option::is_none",
        with = "outcome_value_serde"
    )]
    pub value: Option<Vec<u8>>,
    /// The deadline this outcome set, if it set one.
    #[serde(
        rename = "x",
        alias = "ttl",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl: Option<u64>,
    /// The object is gone.
    #[serde(
        rename = "d",
        alias = "deleted",
        default,
        skip_serializing_if = "outcome_not_deleted"
    )]
    pub deleted: bool,
    /// This item states metadata rather than a page image -- their `meta_log`.
    #[serde(
        rename = "m",
        alias = "meta",
        default,
        skip_serializing_if = "outcome_not_deleted"
    )]
    pub meta: bool,
}

/// `Option<Vec<u8>>` through the same byte encoding every other payload gets.
mod outcome_value_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => {
                #[derive(Serialize)]
                struct Wrapper<'a>(#[serde(with = "crate::bytes_serde")] &'a Vec<u8>);
                Wrapper(bytes).serialize(serializer)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        #[derive(Deserialize)]
        struct Wrapper(#[serde(with = "crate::bytes_serde")] Vec<u8>);
        Ok(Option::<Wrapper>::deserialize(deserializer)?.map(|wrapper| wrapper.0))
    }
}

/// A page put aside during a write, to be carried in that write's log record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagedPage {
    /// The object the page belongs to, which is what a read has when it comes looking.
    pub object_id: u64,
    /// The page contents.
    ///
    /// Carried beside the document when that is smaller, and encoded into it otherwise -- the same
    /// choice every other payload gets. A page is the whole point of this record, so it is the
    /// field where the difference between carrying bytes and encoding them shows up most.
    #[serde(with = "crate::bytes_serde")]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteAheadLogRecordMetadata {
    /// Omitted while it is the current version: a record that does not say otherwise is current,
    /// and saying so in every record costs more than the statement is worth.
    #[serde(
        rename = "v",
        alias = "version",
        default = "current_write_ahead_log_format_version",
        skip_serializing_if = "is_current_write_ahead_log_format_version"
    )]
    pub version: u32,
    #[serde(rename = "t", alias = "timestamp_ms")]
    pub timestamp_ms: u64,
    /// Per-item description of the write.
    ///
    /// Every field is derived from the command in the same record, so this is a convenience
    /// rather than a source of truth -- see [`WriteAheadLogItemMetadata::from_command`], which
    /// reconstructs it. New records leave it empty and skip it entirely rather than spend 147
    /// bytes per write on data the record already contains; records written before that still
    /// carry theirs and still load.
    #[serde(
        rename = "i",
        alias = "items",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub items: Vec<WriteAheadLogItemMetadata>,
    // Atomic-batch framing (all three set together, or all absent for a standalone write). A
    // batch of N commands is written as N contiguously-sequenced records sharing one `batch_id`,
    // buffered and made durable by a SINGLE barrier after the last record. `batch_index` is
    // 1-based; the record with `batch_index == batch_size` is the commit marker. Replay drops a
    // trailing batch that is missing its commit marker (a crash between the buffered appends and
    // the barrier), so a partially-persisted batch is never applied -- all-or-nothing. Absent
    // (skipped in JSON) for every non-batch record, so standalone-write WALs are byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<u32>,
}

impl WriteAheadLogRecordMetadata {
    /// Metadata for a record carrying one command, without the per-item description.
    ///
    /// That description is derived entirely from the command this record already carries, and
    /// read by nothing, so writing it costs 147 fsynced bytes per record to say what the record
    /// says twice. `WriteAheadLogItemMetadata::from_command` reconstructs it for any caller that
    /// wants it, and `single_command_with_items` writes it for one that cannot.
    pub fn single_command(command: &Command) -> Self {
        Self::for_command(command, false)
    }

    /// The same, carrying the per-item description, for a consumer that reads records directly
    /// and has not moved to deriving it.
    ///
    /// `TS_WAL_ITEM_METADATA` used to select this for every writer in the process. Nothing set
    /// it, and the cost it added was per write, so restoring the description is a call rather
    /// than an export -- made by whoever knows the consumer that needs it.
    pub fn single_command_with_items(command: &Command) -> Self {
        Self::for_command(command, true)
    }

    fn for_command(command: &Command, with_items: bool) -> Self {
        Self {
            version: WRITE_AHEAD_LOG_FORMAT_VERSION,
            timestamp_ms: current_time_ms(),
            items: if with_items {
                vec![WriteAheadLogItemMetadata::from_command(command)]
            } else {
                Vec::new()
            },
            batch_id: None,
            batch_size: None,
            batch_index: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteAheadLogItemMetadata {
    pub item_kind: WriteAheadLogItemKind,
    pub model: WriteAheadLogModel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "slot_id")]
    pub bucket_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub meta_log: bool,
    #[serde(default)]
    pub block_log: bool,
}

impl WriteAheadLogItemMetadata {
    pub fn from_command(command: &Command) -> Self {
        let object_key = command_object_key(command);
        let bucket_id = object_key.as_deref().map(command_bucket_id);
        let item_kind = command_item_kind(command);
        Self {
            item_kind,
            model: command_model(command),
            object_key,
            bucket_id,
            object_id: None,
            block_id: None,
            ttl_ms: command_ttl_ms(command),
            deleted: matches!(
                command,
                Command::CommonDelete { .. }
                    | Command::StringDelete { .. }
                    | Command::HashDelete { .. }
                    | Command::SetRemove { .. }
                    | Command::FeatureDelete { .. }
            ),
            meta_log: matches!(
                command,
                Command::CommonExpire { .. } | Command::CommonTtl { .. }
            ),
            block_log: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteAheadLogItemKind {
    Kv,
    Block,
    Ttl,
    DeleteObject,
    Query,
    Admin,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteAheadLogModel {
    Common,
    String,
    Hash,
    Set,
    Feature,
    Sequence,
    ControlState,
    Context,
    Admin,
    Unknown,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogStats {
    pub writes: u64,
    pub reads: u64,
    pub scans: u64,
    pub flushes: u64,
    pub syncs: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub last_sequence: u64,
    pub last_flushed_sequence: u64,
    pub persistent_bytes: u64,
    /// Diagnostics: full-file `last_wal_sequence_at` rescans taken on the client append path for
    /// this shard. Without the log's `flat_append()` this increments once per append (O(writes));
    /// with it on the count stays O(1) once the shard's length cache is warm. Read by the phase-1 aging test.
    #[serde(default)]
    pub append_full_scans: u64,
    /// Diagnostics: full-file `last_wal_sequence_at` rescans taken inside `stats()`. The per-write
    /// index-anchor step reads `stats().last_sequence`; without `flat_append()` that rescans on
    /// every write (O(writes)); with it on the engine anchors off `cached_last_sequence` so this
    /// stays flat. Read (via `raw_stats`, which does NOT itself scan) by the phase-1 aging test.
    #[serde(default)]
    pub stats_full_scans: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogGcReport {
    pub shard_id: ShardId,
    pub retain_from_sequence: u64,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// The retain floor actually used, after clamping. Differs from
    /// `retain_from_sequence` when the caller asked to reclaim further than the tail or the
    /// block-retention floor allowed.
    pub effective_retain_from_sequence: u64,
    /// The block-retention floor held this reclaim back: records at or above it may still be
    /// the only copy of a block's bytes.
    pub clamped_by_block_retention: bool,
    /// The durable-index anchor held this reclaim back: the caller asked to drop records that
    /// the durable served index does not yet reflect, so the ask was narrowed to what the index
    /// can actually replace.
    #[serde(default)]
    pub clamped_by_durable_index: bool,
    /// Bytes reclaimed from the head of this shard's log over its lifetime, after this pass.
    /// A record's log id minus this is where it now physically lives.
    pub base_offset: u64,
    /// Bytes rewritten to keep the survivors. Reclaim copies what it keeps, so this -- not
    /// `records_removed` -- is the cost of the pass, and it tracks the RETAINED size.
    pub bytes_copied: u64,
    /// The pass was declined because the copy it required bought too little space. The records
    /// are untouched and a later pass, once the prefix has grown, will take them.
    pub skipped_not_worth_rewrite: bool,
    /// Whole pieces of the log that went without being copied, because everything in them was
    /// below the floor.
    #[serde(default)]
    pub dropped_segments: usize,
    /// What those pieces held.
    #[serde(default)]
    pub dropped_segment_bytes: u64,
}

/// Evidence that the durable served index for a shard already reflects every WAL record at or
/// below `through_sequence` -- and therefore that those records may be reclaimed.
///
/// A WAL record may only be dropped once the state that supersedes it is on disk. That ordering
/// held here by convention: each reclaim site was expected to have written a manifest, or run a
/// dump, before calling. One site did not. The operator `/gc` RPC took a sequence off the wire
/// and reclaimed straight to it, with no check that anything durable could replace what it was
/// about to delete -- the tail-continuity and block-retention floors were the only things
/// standing in the way, and neither of those knows about the index.
///
/// Making the proof an argument turns that convention into a precondition of the call: a site
/// that wants to reclaim has to name the durable state it is reclaiming against. The clamp is
/// the safety net -- an anchor that understates what is durable costs an under-reclaim, which
/// the next pass picks up, while the alternative costs acked data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableIndexAnchor {
    shard_id: ShardId,
    through_sequence: u64,
}

impl DurableIndexAnchor {
    /// Assert that the shard's durable served index reflects every record at or below
    /// `through_sequence`.
    ///
    /// The caller is making a claim about work it has already completed -- a dump whose base
    /// index was written durably, or a set of bucket-dump manifests covering every live
    /// generation. Mint this from that completed work, never from an intent to do it.
    pub fn proven_durable_through(shard_id: ShardId, through_sequence: u64) -> Self {
        Self {
            shard_id,
            through_sequence,
        }
    }

    /// An anchor that proves nothing and therefore constrains nothing.
    ///
    /// For callers with no durable state to point at: measurement harnesses driving reclaim
    /// directly, and the operator RPC on a shard that has never dumped, where narrowing to a
    /// non-existent frontier would silently turn the endpoint into a no-op. Reclaim through one
    /// of these is exactly as safe as it was before the anchor existed -- which is the point of
    /// naming it, because the call site now says so.
    pub fn unproven(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            through_sequence: u64::MAX,
        }
    }

    /// The highest WAL sequence this anchor vouches for.
    pub fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    /// The shard this anchor speaks for. An anchor proves nothing about any other shard.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogFlushReport {
    pub shard_id: ShardId,
    pub path: PathBuf,
    pub last_sequence: u64,
    pub persistent_bytes: u64,
    pub synced: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogAppendReport {
    pub shard_id: ShardId,
    pub requested_sequence: u64,
    pub current_sequence: u64,
    pub appended: bool,
    pub offset: u64,
    pub size: u64,
    pub persistent_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteAheadLogInfo {
    pub shard_id: ShardId,
    pub path: PathBuf,
    pub start_sequence: u64,
    pub current_sequence: u64,
    pub records: usize,
    pub length_bytes: u64,
    pub persistent_length_bytes: u64,
    pub last_flushed_sequence: u64,
    pub format_version: u32,
}

pub const WRITE_AHEAD_LOG_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct LocalWriteAheadLogStore {
    inner: Arc<Mutex<WriteAheadLogInner>>,
    // Group-commit coordinator: serializes WAL fsyncs so many concurrent writers
    // share one durability barrier. Always consulted -- `group_commit_configured` returns true,
    // and `TS_GROUP_COMMIT`, which this comment used to name as its condition, is read by
    // nothing.
    // Writers append their bytes under the `inner` lock, RELEASE it, then coalesce
    // their fsync here -- so a burst of concurrent writes amortizes onto ~1 fsync.
    sync_coord: Arc<Mutex<HashMap<ShardId, GroupCommitState>>>,
    /// Whether an append resolves its sequence from the warm cache. Read on the append path and
    /// on every execute, so it is an atomic beside the lock rather than a field inside it -- the
    /// engine asks without taking the log's lock.
    flat_append: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct GroupCommitState {
    // Highest WAL sequence proven durable (fdatasync'd) for this shard. A caller
    // whose sequence is already <= this returns without issuing its own fsync.
    durable_seq: u64,
    // The WAL file's parent-directory entry has been fsync'd at least once this
    // process lifetime. Appends only grow the file (inode), never the directory,
    // so the per-append dir fsync is redundant after the first.
    dir_synced: bool,
}

pub type WalError = WriteAheadLogError;
pub type WalRecord = WriteAheadLogRecord;
pub type WalStats = WriteAheadLogStats;
pub type WalGcReport = WriteAheadLogGcReport;
pub type LocalWalStore = LocalWriteAheadLogStore;

#[derive(Debug)]
struct WriteAheadLogInner {
    root: PathBuf,
    /// The active piece's path, built once per shard.
    ///
    /// `write_ahead_log_path` is a `format!` and a `join` -- two allocations -- and the append
    /// path asked for the same answer six times a write. The name is stable: a roll seals the old
    /// piece under a numbered name and recreates this one, so the active path outlives any roll.
    ///
    /// An `Arc` so callers can hold it while the rest of this struct stays borrowable; cloning it
    /// is a refcount bump.
    active_path_by_shard: HashMap<ShardId, std::sync::Arc<PathBuf>>,
    /// The append lock file, held open per shard.
    ///
    /// It used to be opened and closed on every append: a `format!`, a `PathBuf`, an open and a
    /// close, for a file whose name never changes. The lock itself is `flock`, still taken and
    /// released per append -- this only stops re-opening the thing being locked.
    ///
    /// Safe to hold open: `flock` guards against OTHER PROCESSES, and threads inside this one are
    /// already serialised by the mutex around this struct.
    append_lock_by_shard: HashMap<ShardId, std::sync::Arc<File>>,
    /// Reused to frame each record, instead of allocating a frame per append.
    ///
    /// The frame is the payload plus about ten bytes, so allocating one per write allocated the
    /// whole record again every time -- four kilobytes an append at a four-kilobyte value. Kept
    /// here so its capacity survives; a steady workload stops allocating for it entirely.
    encode_scratch: Vec<u8>,
    // Set only by Default: the store owns its minted scratch directory, and the last
    // clone's drop removes it. Never set for a caller-supplied root.
    scratch: Option<std::sync::Arc<crate::scratch::ScratchDirGuard>>,
    stats: WriteAheadLogStats,
    last_sequence_by_shard: HashMap<ShardId, u64>,
    /// Per shard: how far the ACTIVE segment has actually been made durable, and the highest
    /// sequence covered by that barrier.
    ///
    /// `stats` is one struct for the whole store, so its `persistent_bytes` and
    /// `last_flushed_sequence` describe whichever shard synced most recently -- reporting them
    /// for a specific shard attributes another shard's barrier to this one. Durability is a
    /// property of a log, so it is tracked per log.
    durable_active_bytes_by_shard: HashMap<ShardId, u64>,
    durable_sequence_by_shard: HashMap<ShardId, u64>,
    /// Per shard, for the block footer: the offset and sequence of the last record that
    /// STARTED in the block currently being filled. When that block closes, this is what its
    /// footer records, and it is what a reopen reads instead of walking the log.
    block_last_record_by_shard: HashMap<ShardId, (u64, u64)>,
    /// Per shard: whether this log is written in blocks. Decided the first time this process
    /// appends to it and never revisited, because the answer is a property of the bytes already
    /// on disk rather than of the current configuration.
    block_mode_by_shard: HashMap<ShardId, bool>,
    // MANIFEST-CONFORMANCE / phase-1 flat-append cache (`flat_append()`). The WAL file byte length as
    // this process last left it after its own append (or after a full reconcile scan), per shard.
    // On the next append the fast path stats the file: if the on-disk length still equals this,
    // no other writer touched the file since we wrote it (the append lock is cross-process) and --
    // because we only ever append complete framed lines -- there is no torn tail, so the warm
    // `last_sequence_by_shard` is authoritative and the O(records) `last_wal_sequence_at` scan is
    // skipped. Any mismatch (external append, or first touch this process lifetime) falls back to
    // the full scan. Only consulted when `flat_append()`; harmless to maintain when off.
    verified_len_by_shard: HashMap<ShardId, u64>,
    // Lowest sequence whose record may still hold the only copy of a block's bytes -- the dump
    // watermark. A block written into the WAL is addressed by the byte offset of its record and
    // has no copy in a band until it is dumped, so reclaiming that record destroys the block.
    // GC clamps its retain floor to this. Absent = no shard has WAL-resident blocks, and GC is
    // unconstrained, which is the behaviour when nothing registers a floor.
    block_retention_floor_by_shard: HashMap<ShardId, u64>,
    // Cached (reclaim base, header length) per shard so turning a record's physical offset into
    // the log id that survives reclaim costs no file read on the write path. Filled on first use
    // and refreshed by reclaim -- the only thing that moves the base.
    base_by_shard: HashMap<ShardId, (u64, u64)>,
    // Under TS_WAL_PREALLOCATE: the physical file size this process last grew the file to.
    // Records stop at `verified_len_by_shard`; the file itself ends here. The append fast path
    // compares the on-disk length against THIS (not the record end) to detect another writer,
    // because under preallocation those two are allowed to differ.
    prealloc_physical_by_shard: HashMap<ShardId, u64>,
    // Shards whose piece being written was created without its directory entry being made durable.
    // A roll creates a new file under the same name, so the entry has to reach disk again before
    // any record in that piece is acked -- otherwise the file can vanish and take an acked record
    // with it. The roll does not pay for that barrier itself: it records the debt here and the next
    // durable barrier, which was going to run anyway, settles it.
    dir_sync_owed_by_shard: HashSet<ShardId>,
}

impl LocalWriteAheadLogStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        Self {
            inner: Arc::new(Mutex::new(WriteAheadLogInner {
                root,
                active_path_by_shard: HashMap::new(),
                append_lock_by_shard: HashMap::new(),
                encode_scratch: Vec::new(),
                scratch: None,
                stats: WriteAheadLogStats::default(),
                last_sequence_by_shard: HashMap::new(),
                durable_active_bytes_by_shard: HashMap::new(),
                block_last_record_by_shard: HashMap::new(),
                block_mode_by_shard: HashMap::new(),
                durable_sequence_by_shard: HashMap::new(),
                verified_len_by_shard: HashMap::new(),
                block_retention_floor_by_shard: HashMap::new(),
                base_by_shard: HashMap::new(),
                prealloc_physical_by_shard: HashMap::new(),
                dir_sync_owed_by_shard: HashSet::new(),
            })),
            sync_coord: Arc::new(Mutex::new(HashMap::new())),
            flat_append: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Whether an append resolves its sequence from the warm in-process cache.
    ///
    /// On, and settable off only by a test. Off, the live client-write append path calls
    /// `last_wal_sequence_at()` on EVERY append -- a full read of this shard's log from offset 0
    /// that decodes every record to find the max sequence, so an ingest costs O(records) per
    /// append and O(n^2) overall. That scan is the dominant cost of the work done under the
    /// engine's shards write lock: longer than the ~3.3 ms fsync and serialized under the same
    /// lock, so concurrent writers never overlap at the fsync barrier and group commit cannot
    /// coalesce.
    ///
    /// On, the cache is trusted whenever the file's on-disk length is still exactly what this
    /// process last left it -- an O(1) `metadata()` stat instead of the O(n) scan. That is safe
    /// because the append lock is cross-process, so any external appender changes the length and
    /// forces the full scan; and because only complete framed records are ever appended, so a
    /// matching length rules out a torn tail. The result is byte-identical to the scanning path.
    ///
    /// The same setting decides whether the engine repeats its promote reconcile scan, which is
    /// the other per-write cost that ages with the store. One switch flattens both, which is why
    /// it lives here rather than in each place that consults it.
    pub fn flat_append(&self) -> bool {
        self.flat_append.load(AtomicOrdering::Relaxed)
    }

    /// Rescan the whole log on every append, as builds before the warm cache did.
    ///
    /// For the test that measures both: it writes to one log with the cache trusted and to
    /// another without, and asserts the scan counts of the second track its write volume while
    /// the first stay flat.
    #[cfg(test)]
    pub(crate) fn rescan_on_every_append_for_test(&self) {
        self.flat_append.store(false, AtomicOrdering::Relaxed);
    }

    pub fn append(
        &self,
        shard_id: ShardId,
        command: Command,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        self.append_with_sync(shard_id, command, !wal_bulk_relaxed_durability())
    }

    /// Append a WAL record. `sync=true` fsyncs before returning (durable);
    /// `sync=false` writes the record but defers the fsync. The WAL
    /// writer ALWAYS records the entry; EVENT_REPLICATION_SYNC
    /// vs ASYNC_STORAGE only changes whether the commit blocks
    /// The WAL commit runs synchronously vs deferred.
    /// Append, and report the log id the record landed at.
    ///
    /// The log id is the record's position in the log's whole history, so it stays valid across
    /// a reclaim. That is what lets a block carried in this record be addressed by it.
    pub fn append_with_sync_reporting(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, Vec::new(), Vec::new())
    }

    /// Append, carrying pages this write produced, and report the log id it landed at.
    ///
    /// The pages travel in the same record as the command, so one durability barrier covers
    /// both and a read that finds the record finds the page with it.
    pub fn append_with_sync_staged(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
        staged_pages: Vec<StagedPage>,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, staged_pages, Vec::new())
    }

    /// [`append_with_sync_staged`](Self::append_with_sync_staged), also carrying what the write
    /// DID -- see [`WalOutcomeItem`].
    pub fn append_with_outcomes(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
        staged_pages: Vec<StagedPage>,
        outcomes: Vec<WalOutcomeItem>,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, staged_pages, outcomes)
    }

    pub fn append_with_sync(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, Vec::new(), Vec::new())
            .map(|(record, _)| record)
    }

    fn append_with_sync_inner(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
        staged_pages: Vec<StagedPage>,
        outcomes: Vec<WalOutcomeItem>,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        // The durable barrier is deferred out of the append critical section (below), so the
        // byte-append records with sync=false and the fsync is coalesced across concurrent
        // writers. Every acked write is still durable before its ack returns; only the fsync is
        // shared.
        let group = sync;
        let record;
        let next_sequence;
        let log_id;
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            // Acquiring the append lock creates the directory the first time it opens the lock
            // file; doing it again on every append was a syscall to learn something unchanged.
            let _append_lock = WalAppendLock::acquire(&mut inner, shard_id)?;
            ensure_active_wal_segment(&mut inner, shard_id)?;
            let (last_sequence, on_disk_len) =
                resolve_last_sequence_for_append(&mut inner, shard_id, self.flat_append())?;
            inner.last_sequence_by_shard.insert(shard_id, last_sequence);
            let seq = last_sequence.saturating_add(1);
            let rec = WriteAheadLogRecord {
                shard_id,
                sequence: seq,
                metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
                // Durable AND in the block store: the two conditions under which installing an
                // address is enough on its own.
                // A carried block is findable after recovery now: replay registers the blocks
                // of every record it replays, which is the one thing the rule below was waiting
                // for. So a record carrying its own blocks can state results and drop the
                // operation, the same as a synchronous write whose blocks are already durable.
                //
                // What still cannot: an ASYNCHRONOUS write with nothing carried. Its result names
                // an address in the block store that a crash may leave unwritten, and no amount
                // of registering helps a block that was never stored.
                command: record_command(command, &outcomes, sync || !staged_pages.is_empty()),
                staged_pages,
                outcomes,
            };
            let report = append_record_locked(&mut inner, &rec, sync && !group, Some(on_disk_len))?;
            inner.stats.last_sequence = report.current_sequence;
            // Where the record landed, in the addressing that survives reclaim -- taken BEFORE any
            // roll. The record is in the piece being written now; a roll starts a new piece with a
            // new starting log id, and reading that afterwards would address this record as though
            // it were in the piece that comes next.
            let (base, header_len) = cached_wal_base(&mut inner, shard_id)?;
            log_id = base.saturating_add(report.offset.saturating_sub(header_len));
            // Roll after the record is written, so the piece being sealed is whole.
            let rolled = roll_wal_segment_if_due(&mut inner, shard_id, Some(report.persistent_bytes))?;
            // Record the file length we just left behind so the next append's fast path can
            // confirm no other writer touched the file (O(1) stat) and skip the full scan.
            if !rolled {
                // After a roll the piece being written is new and its length was recorded by the
                // roll; the length this record left behind belongs to the piece just sealed.
                inner
                    .verified_len_by_shard
                    .insert(shard_id, report.persistent_bytes);
            }
            // last_flushed_sequence is advanced by append_record_locked ONLY when the record was
            // actually fsynced (sync=true, non-group). An unconditional overwrite here reported an
            // async / bulk-mode (unsynced) record as durable -- overstating durability, a latent
            // trap for any future reclaim/ack gate that reads it. The group path advances it below
            // after the coalesced barrier actually reaches disk.
            inner.last_sequence_by_shard.insert(shard_id, seq);
            record = rec;
            next_sequence = seq;
            // `inner` and `_append_lock` are released here so a concurrent writer can
            // append while this writer's group-commit fsync is in flight (the fsync
            // duration is the natural batching window).
        }
        if group {
            self.group_commit_sync(shard_id, next_sequence)?;
        }
        Ok((record, log_id))
    }

    /// Phase 1 of a two-phase durable commit: append the record bytes and RESERVE its WAL
    /// sequence WITHOUT taking the durable barrier. The record is written with sync=false
    /// (bytes buffered, sequence assigned + `last_sequence_by_shard` advanced) and the
    /// returned `WriteAheadLogRecord.sequence` is then passed to `commit_barrier` AFTER the
    /// caller has released its own state lock, so the fsync coalesces across concurrent
    /// writers (see `group_commit_sync`). Mirrors the byte-append half of
    /// `append_with_sync`'s group branch, minus the in-line barrier call. The caller MUST
    /// await `commit_barrier(shard_id, record.sequence)` before acking a synchronous write.
    /// Append without the durable barrier, carrying what the write recorded.
    ///
    /// Outcomes were kept off this path on the assumption that anything a record must CARRY forces
    /// the staged branch. That is true of staged pages, whose addresses are back-patched once the
    /// log id exists. It is not true of outcomes: their addresses are already resolved by the time
    /// they are staged, so they travel in the record like any other field -- and keeping them out
    /// cost every write its place in the group-commit queue for no durability reason.
    pub fn append_for_group_commit(
        &self,
        shard_id: ShardId,
        command: Command,
        outcomes: Vec<WalOutcomeItem>,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        // Acquiring the append lock creates the directory the first time it opens the lock
        // file; doing it again on every append was a syscall to learn something unchanged.
        let _append_lock = WalAppendLock::acquire(&mut inner, shard_id)?;
        // Start a piece if a crash left none, exactly as the single-record path does. Without it
        // the record creates the file with no base header, so it reads as starting at log id zero
        // -- an address the sealed pieces already own.
        ensure_active_wal_segment(&mut inner, shard_id)?;
        let (last_sequence, _on_disk_len) = resolve_last_sequence_for_append(&mut inner, shard_id, self.flat_append())?;
        inner.last_sequence_by_shard.insert(shard_id, last_sequence);
        let seq = last_sequence.saturating_add(1);
        let rec = WriteAheadLogRecord {
            shard_id,
            sequence: seq,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
            // This path exists to coalesce the fsync of a SYNCHRONOUS write, so the blocks behind
            // these results are durable by the time the barrier this record waits on returns.
            command: record_command(command, &outcomes, true),
            staged_pages: Vec::new(),
            outcomes,
        };
        // sync=false: write the bytes, defer the fdatasync to `commit_barrier`. Same as the
        // group branch of append_with_sync. `last_flushed_sequence` is NOT advanced here (the
        // record is not yet durable); the coalesced barrier advances it once it reaches disk.
        let report = append_record_locked(&mut inner, &rec, false, None)?;
        inner.stats.last_sequence = report.current_sequence;
        // Roll after the record lands, as the single-record path does. This is only safe because
        // sealing a piece now fsyncs its contents: the barrier this record is waiting for opens
        // the piece being WRITTEN, which after a roll is the new one, so without that fsync the
        // record would be sealed away with no barrier covering it.
        let rolled = roll_wal_segment_if_due(&mut inner, shard_id, Some(report.persistent_bytes))?;
        inner.last_sequence_by_shard.insert(shard_id, seq);
        if !rolled {
            // After a roll the piece being written is new and its length was recorded by the roll;
            // the length this record left behind belongs to the piece just sealed.
            inner
                .verified_len_by_shard
                .insert(shard_id, report.persistent_bytes);
        }
        // `inner` + `_append_lock` drop here so a concurrent writer can append while this
        // writer's (later, lock-released) group-commit fsync is in flight.
        Ok(rec)
    }

    /// Phase 2 of a two-phase durable commit: the coalesced durable barrier for a sequence
    /// reserved by `append_for_group_commit`. Returns once every record up to
    /// `required_sequence` is fdatasync'd (issuing at most one shared fsync per group; returns
    /// immediately if an earlier writer's barrier already covered `required_sequence`). Public
    /// wrapper over `group_commit_sync` so the engine can run the barrier AFTER releasing its
    /// `shards` write lock.
    pub fn commit_barrier(
        &self,
        shard_id: ShardId,
        required_sequence: u64,
    ) -> Result<(), WriteAheadLogError> {
        self.group_commit_sync(shard_id, required_sequence)
    }

    /// Append a group of commands as ONE crash-atomic batch: N contiguously-sequenced records
    /// sharing a single `batch_id`, buffered, then made durable by a SINGLE barrier after the
    /// last record (the commit marker). Either the whole batch is durable (barrier completed) or,
    /// on a crash before the barrier, the trailing partial batch is dropped on replay -- so a
    /// retry never double-applies a durable prefix of a non-idempotent / time-unspecified command
    /// (e.g. FeatureAppend occur_time=0). `sync=false` skips the barrier (async / bulk mode: the
    /// whole batch is buffered and lost-or-kept together, still all-or-nothing).
    ///
    /// Records are assigned a contiguous sequence block under a single `inner`-lock hold, so no
    /// other writer can interleave a record into the middle of the batch (the engine also
    /// serializes writes through its shard lock). A single command takes the standalone
    /// `append_with_sync` path (no batch framing, byte-identical WAL).
    /// Append a whole batch as ONE record carrying every item it produced.
    ///
    /// A batch has always been crash-atomic; what changed is how. Written as N records sharing a
    /// batch id, atomicity is bookkeeping: each record carries the id, the size and its index,
    /// the last one is the commit marker, and replay drops a trailing group whose marker never
    /// arrived. Written as one record it is atomic by construction -- a torn write leaves an
    /// incomplete frame, and an incomplete frame was never a record.
    ///
    /// So the three batch fields go unused, the commit marker stops being a concept, and the
    /// per-record cost is paid once instead of N times. Measured on eight items: 1015 bytes
    /// against 1224, and the eight barriers were already one.
    ///
    /// The caller decides whether this is allowed: it needs every item's block to be findable
    /// after a crash, which is the same rule a single write follows.
    pub fn append_batch_as_one_record(
        &self,
        shard_id: ShardId,
        outcomes: Vec<WalOutcomeItem>,
        staged_pages: Vec<StagedPage>,
        sync: bool,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let _append_lock = WalAppendLock::acquire(&mut inner, shard_id)?;
        let (last_sequence, _on_disk_len) = resolve_last_sequence_for_append(&mut inner, shard_id, self.flat_append())?;
        let seq = last_sequence.saturating_add(1);
        let record = WriteAheadLogRecord {
            shard_id,
            sequence: seq,
            // No command: the items say what the batch did. No batch id, size or index either --
            // one record needs none of them to be recovered as a unit.
            command: None,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(
                &Command::StringGet {
                    key: String::new(),
                },
            )),
            staged_pages,
            outcomes,
        };
        let report = append_record_locked(&mut inner, &record, sync, None)?;
        inner.stats.last_sequence = report.current_sequence;
        inner.last_sequence_by_shard.insert(shard_id, seq);
        Ok(record)
    }

    pub fn append_batch_atomic(
        &self,
        shard_id: ShardId,
        commands: Vec<Command>,
        sync: bool,
    ) -> Result<Vec<WriteAheadLogRecord>, WriteAheadLogError> {
        if commands.len() <= 1 {
            let mut records = Vec::with_capacity(commands.len());
            if let Some(command) = commands.into_iter().next() {
                records.push(self.append_with_sync(shard_id, command, sync)?);
            }
            return Ok(records);
        }
        let batch_id = next_batch_id();
        let batch_size = commands.len() as u32;
        let mut records = Vec::with_capacity(commands.len());
        let last_sequence;
        let mut batch_end: Option<u64> = None;
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            // Acquiring the append lock creates the directory the first time it opens the lock
            // file; doing it again on every append was a syscall to learn something unchanged.
            let _append_lock = WalAppendLock::acquire(&mut inner, shard_id)?;
            // Start a piece if a crash left none. Without this the first record of the batch
            // creates the file with no base header, so it reads as starting at log id zero -- an
            // address the sealed pieces already own. The single-record path has always done this;
            // the batch path did not, and nothing about a batch makes it safe to skip.
            ensure_active_wal_segment(&mut inner, shard_id)?;
            let (disk_last_sequence, _) = last_wal_sequence_at(&inner.root, shard_id)?;
            let cached_last_sequence = inner
                .last_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or_default();
            let mut seq = cached_last_sequence.max(disk_last_sequence);
            // ONE open for the whole batch. Each record used to open, write, stat and close the
            // piece for itself -- 4N syscalls against the single barrier below, which is the one
            // thing a batch exists to amortise. Safe to hold across the loop because the append
            // lock and the `inner` lock are both held throughout and the roll happens AFTER the
            // batch, so nothing can seal and rename the piece underneath this handle.
            let batch_path = active_wal_path(&mut inner, shard_id);
            let prealloc = wal_preallocate_enabled();
            #[cfg(test)]
            WAL_FILE_OPENS.with(|opens| opens.set(opens.get() + 1));
            let mut batch_physical: u64 = 0;
            let mut batch_file = if prealloc {
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(batch_path.as_path())?
            } else {
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(batch_path.as_path())?
            };
            for (index, command) in commands.into_iter().enumerate() {
                seq = seq.saturating_add(1);
                let mut metadata = WriteAheadLogRecordMetadata::single_command(&command);
                metadata.batch_id = Some(batch_id);
                metadata.batch_size = Some(batch_size);
                metadata.batch_index = Some(index as u32 + 1);
                let rec = WriteAheadLogRecord {
                    shard_id,
                    sequence: seq,
                    metadata: Some(metadata),
                    // No results on this path, so the operation is what replay has.
                    command: Some(command),
                    staged_pages: Vec::new(),
                    outcomes: Vec::new(),
                };
                // Buffer every record (sync=false); the single durability barrier below covers
                // the whole batch. append_record_locked keeps last_flushed_sequence honest -- it
                // only advances on an actual fsync, which happens once, after the loop.
                let report = append_record_locked_on(
                    &mut inner,
                    &rec,
                    false,
                    None,
                    Some((&mut batch_file, &mut batch_physical)),
                )?;
                inner.stats.last_sequence = report.current_sequence;
                batch_end = Some(report.persistent_bytes);
                records.push(rec);
            }
            // Closed before the roll below, which may seal and rename this very piece.
            drop(batch_file);
            inner.last_sequence_by_shard.insert(shard_id, seq);
            last_sequence = seq;
            // Roll AFTER the batch, never inside it: rolling mid-batch would split one
            // crash-atomic group across two pieces for nothing, and taken here the batch stays
            // contiguous while the next one starts in the new piece. Without this a batch appends
            // into the piece being written without ever asking whether it is full, so segmentation
            // stops applying to the path that writes the most -- and reclaim cannot unlink the
            // piece being written, so the unlinking stops working with it.
            roll_wal_segment_if_due(&mut inner, shard_id, batch_end)?;
        }
        if sync {
            // One coalesced fdatasync makes the entire buffered batch durable. Reusing the
            // group-commit barrier keeps the durable-watermark bookkeeping consistent.
            self.group_commit_sync(shard_id, last_sequence)?;
        }
        Ok(records)
    }

    /// Coalesced WAL durability barrier (group commit). Ensures every byte appended for
    /// `shard_id` up to at least `required_sequence` is `fdatasync`'d to disk before
    /// returning. Concurrent callers serialize on `sync_coord`; the first to run fsyncs
    /// the file -- which flushes ALL pending appends, not just its own -- records the
    /// durable watermark, and any caller whose sequence is already covered returns
    /// without a second barrier. A burst of N concurrent writes therefore shares ~1
    /// fsync instead of paying N. Correctness: every appender writes its bytes under the
    /// `inner` lock and releases it before calling here, so the snapshotted high-water
    /// sequence names only bytes already in the page cache; the fsync makes exactly those
    /// durable, and the watermark is never advanced past them.
    fn group_commit_sync(
        &self,
        shard_id: ShardId,
        required_sequence: u64,
    ) -> Result<(), WriteAheadLogError> {
        let mut coord = self.sync_coord.lock().expect("wal sync coordinator poisoned");
        let entry = coord.entry(shard_id).or_default();
        if entry.durable_seq >= required_sequence {
            return Ok(());
        }
        let (path, snapshot) = {
            let inner = self.inner.lock().expect("write-ahead log lock poisoned");
            let path = write_ahead_log_path(&inner.root, shard_id);
            let snapshot = inner
                .last_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or(required_sequence)
                .max(required_sequence);
            (path, snapshot)
        };
        if !path.exists() {
            entry.durable_seq = entry.durable_seq.max(snapshot);
            return Ok(());
        }
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        crate::durability_metrics::record_barrier("engine_wal_group_commit");
        file.sync_data()?;
        let owed = {
            let inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.dir_sync_owed_by_shard.contains(&shard_id)
        };
        if !entry.dir_synced || owed {
            // The first durable append for this shard this process lifetime makes the directory
            // entry durable once, and afterwards nothing touches it -- until a roll creates a new
            // file under the same name, which is what `owed` reports.
            sync_parent_dir(&path)?;
            entry.dir_synced = true;
            self.inner
                .lock()
                .expect("write-ahead log lock poisoned")
                .dir_sync_owed_by_shard
                .remove(&shard_id);
        }
        entry.durable_seq = entry.durable_seq.max(snapshot);
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.stats.flushes += 1;
            inner.stats.syncs += 1;
            if snapshot > inner.stats.last_flushed_sequence {
                inner.stats.last_flushed_sequence = snapshot;
            }
            // The barrier above covered everything written to this shard's active segment, so
            // its whole current length is durable. Recorded per shard for the same reason the
            // flush path does it.
            let durable_bytes = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            let bytes_entry = inner
                .durable_active_bytes_by_shard
                .entry(shard_id)
                .or_default();
            *bytes_entry = (*bytes_entry).max(durable_bytes);
            let sequence_entry = inner.durable_sequence_by_shard.entry(shard_id).or_default();
            *sequence_entry = (*sequence_entry).max(snapshot);
        }
        Ok(())
    }

    pub fn append_replayed_record(
        &self,
        record: WriteAheadLogRecord,
    ) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        // Start a piece if a crash left none, before anything reads where the log ends. Replay
        // writes a record whose sequence is already decided, so creating the file without a base
        // header here would give it an address the sealed pieces already own -- and a replayed
        // record is exactly the one nobody would think to re-check. No roll: replay is
        // reconstructing a log that already exists, and choosing new piece boundaries part-way
        // through that is a separate decision.
        ensure_active_wal_segment(&mut inner, record.shard_id)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&record.shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let (sequence, _) = last_wal_sequence_at(&inner.root, record.shard_id)?;
                inner
                    .last_sequence_by_shard
                    .insert(record.shard_id, sequence);
                sequence
            }
        };
        if record.sequence <= last_sequence {
            let path = active_wal_path(&mut inner, record.shard_id);
            return Ok(WriteAheadLogAppendReport {
                shard_id: record.shard_id,
                requested_sequence: record.sequence,
                current_sequence: last_sequence,
                appended: false,
                offset: path.metadata().map(|metadata| metadata.len()).unwrap_or(0),
                size: 0,
                persistent_bytes: path.metadata().map(|metadata| metadata.len()).unwrap_or(0),
            });
        }
        let report = append_record_locked(&mut inner, &record, true, None)?;
        inner
            .last_sequence_by_shard
            .insert(record.shard_id, record.sequence);
        inner.stats.last_sequence = record.sequence;
        inner.stats.last_flushed_sequence = record.sequence;
        Ok(report)
    }

    pub fn read_range(
        &self,
        shard_id: ShardId,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        // `offset` is a log id: a position in the log's whole history, not in whichever file
        // happens to hold it. That is the only position that means anything once the log is in
        // pieces, and unlike a file offset it survives reclaim.
        let Some((path, physical)) = locate_log_id(&inner.root, shard_id, offset)? else {
            return Ok(Vec::new());
        };
        // The records may stop before the file does: under preallocation the active piece ends
        // in a zeros reservation, which is room, not log content, and must not be served to a
        // caller reading the stream. Sealed pieces end exactly at their records, so the clamp
        // is a no-op there.
        let (_, record_end) = last_wal_sequence_in(&path)?;
        let size = size.min(record_end.saturating_sub(physical));
        let bytes = read_at(&path, physical, size)?;
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
    ) -> Result<Vec<(u64, Vec<u8>)>, WriteAheadLogError> {
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
    ) -> Result<(Vec<(u64, Vec<u8>)>, bool), WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let segments = wal_segment_paths(&inner.root, shard_id)
            .into_iter()
            .filter(|path| path.exists())
            .collect::<Vec<_>>();
        // No file at all is "nothing to scan", not an error. Distinguishing it here lets recovery
        // treat a missing log as "nothing to replay" while still surfacing a genuine decode failure
        // (corruption) as data loss (see engine::lifecycle replay).
        if segments.is_empty() {
            inner.stats.scans += 1;
            return Ok((Vec::new(), false));
        }
        let _ = last_wal_sequence_at(&inner.root, shard_id)?;
        let mut total = 0;
        let mut truncated = false;
        let mut records = Vec::new();
        'segments: for path in segments {
            // Each piece says where in the log's history its contents begin, so a record's position
            // is that plus how far into the piece it sits. Positions are log ids: an offset into
            // one file would mean nothing to a caller once there is more than one, and would not
            // survive reclaim.
            let (base, header_len) = read_wal_base(&path)?;
            // A piece that ends before the window starts holds nothing the caller asked for. Its
            // start plus its length says where it ends, so skipping it costs a stat rather than a
            // read of every line in it. This is what makes a windowed scan cost the window: without
            // it the loop below still reads the whole log and merely declines to return most of it.
            // Also what skips a piece reclaimed since the listing above: an absent piece reads
            // as base zero with no length, so this says it ends before any window and the loop
            // moves on rather than opening a file that is gone. See `read_wal_base`.
            let piece_len = path.metadata().map(|meta| meta.len()).unwrap_or(0);
            if base.saturating_add(piece_len.saturating_sub(header_len)) <= start_offset {
                continue;
            }
            let mut file = File::open(&path)?;
            file.seek(SeekFrom::Start(header_len))?;
            let mut reader = BufReader::new(file);
            let mut log_id = base;
            loop {
                // Step over a block footer this lands on. The zero-run check below only catches a
                // boundary that has padding in front of its footer; a block whose records end
                // flush against the footer has none, and the footer would then be read as a
                // record -- its first byte is not a frame marker, so the reader falls to
                // newline-delimited mode and hands back a fragment. `last_wal_sequence_forward`
                // takes this same first step; the two walkers read one format and must agree.
                let at = header_len.saturating_add(log_id.saturating_sub(base));
                let stepped = skip_block_footer_if_due(&mut reader, at, header_len)?;
                if stepped != at {
                    // Log ids are byte positions in the log's history, so the footer bytes
                    // stepped over are counted here exactly as the closed-block branch counts
                    // the ones it skips.
                    log_id = log_id.saturating_add(stepped.saturating_sub(at));
                }
                // Reads a record by the length it declares when it is framed that way, and to
                // the newline when it is not, so one loop walks a log holding both. The
                // preallocated zero run that trails the records ends the loop from inside the
                // reader: no frame can start with a zero byte.
                let Some(line) = read_raw_record(&mut reader)? else {
                    // a scan must cross block boundaries: the zeros here are either the padding
                    // before a closed block's footer, with records continuing after it, or the
                    // reservation past the end. The footer is what tells them apart, and reading
                    // it wrong loses every record after the first boundary -- silently, because a
                    // short log looks exactly like a log that is short.
                    let physical = header_len.saturating_add(log_id.saturating_sub(base));
                    match block_is_closed(&mut reader, physical, header_len, piece_len)? {
                        Some(next_physical) => {
                            // Log ids are byte positions in the log's history, so the bytes
                            // stepped over have to be counted or every address after this block
                            // moves.
                            log_id =
                                log_id.saturating_add(next_physical.saturating_sub(physical));
                            continue;
                        }
                        None => break,
                    }
                };
                let read = line.len();
                if read == 0 {
                    break;
                }
                let next_log_id = log_id.saturating_add(read as u64);
                if log_id < start_offset {
                    // Before the window: skip it, but keep counting position.
                    log_id = next_log_id;
                    continue;
                }
                if next_log_id > end_offset {
                    break 'segments;
                }
                if total + read as u64 > max_bytes {
                    // Out of budget with the window not yet walked: there is more to read.
                    truncated = true;
                    break 'segments;
                }
                // Refuse a corrupt record here, where it is being read. This used to happen only as
                // a side effect of walking the whole file to find the log's end; that walk no longer
                // reads everything, and a guarantee that depends on unrelated work is not a
                // guarantee. A blank line carries nothing to verify and is passed through as before.
                if !line.iter().all(|byte| byte.is_ascii_whitespace()) {
                    decode_wal_line(&line)?;
                }
                records.push((log_id, line));
                log_id = next_log_id;
                total += read as u64;
            }
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += total;
        Ok((records, truncated))
    }

    /// Where reading can start, to see every record after `sequence` and no whole piece before it.
    ///
    /// Recovery knows a sequence -- the one its durable index already covers -- and needs a
    /// position to read from. Sequences ascend through the log, so a piece whose last sequence is
    /// at or below the watermark holds nothing left to replay, and reading can begin at the first
    /// piece that is not. The answer is conservative: it never skips a record after the watermark,
    /// and it may include some before it, which the caller already filters.
    ///
    /// A log in one piece answers zero, which is what it did before any of this existed.
    pub fn log_id_after_sequence(
        &self,
        shard_id: ShardId,
        sequence: u64,
    ) -> Result<u64, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let mut start = 0u64;
        for path in wal_segment_paths(&inner.root, shard_id) {
            if !path.exists() {
                continue;
            }
            let (last, record_end) = last_wal_sequence_in(&path)?;
            // An empty piece holds nothing either way; keep looking past it.
            if last > 0 && last > sequence {
                return Ok(start);
            }
            let (base, header_len) = read_wal_base(&path)?;
            // Where the RECORDS end, not where the file does: under preallocation the active
            // piece's file length includes the zeros reservation, which holds no log ids.
            start = base.saturating_add(record_end.saturating_sub(header_len));
        }
        Ok(start)
    }

    pub fn flush(&self, shard_id: ShardId) -> Result<WriteAheadLogFlushReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = active_wal_path(&mut inner, shard_id);
        let (last_sequence, _) = last_wal_sequence_at(&inner.root, shard_id)?;
        if !path.exists() {
            return Ok(WriteAheadLogFlushReport {
                shard_id,
                path: path.as_ref().clone(),
                last_sequence,
                persistent_bytes: 0,
                synced: false,
            });
        }
        let file = OpenOptions::new().read(true).write(true).open(path.as_path())?;
        crate::durability_metrics::record_barrier("engine_wal_flush");
        file.sync_all()?;
        sync_parent_dir(&path)?;
        let persistent_bytes = path.metadata()?.len();
        inner.stats.flushes += 1;
        inner.stats.syncs += 1;
        inner.stats.last_flushed_sequence = last_sequence;
        inner.stats.persistent_bytes = persistent_bytes;
        // Per shard, so a later barrier on a different shard cannot be read as this one's.
        inner
            .durable_active_bytes_by_shard
            .insert(shard_id, persistent_bytes);
        inner
            .durable_sequence_by_shard
            .insert(shard_id, last_sequence);
        Ok(WriteAheadLogFlushReport {
            shard_id,
            path: path.as_ref().clone(),
            last_sequence,
            persistent_bytes,
            synced: true,
        })
    }

    /// Hold WAL reclaim at `sequence`: records at or above it may still be the only copy of a
    /// block's bytes, so GC must not reclaim past it.
    ///
    /// Set this to the dump watermark and advance it as blocks are dumped into bands. Until a
    /// shard registers a floor its GC is unconstrained, so this is inert for callers that do not
    /// put blocks in the WAL.
    /// Whether two handles address the SAME underlying log (clones of one store). Registrations
    /// keyed only by (shard, object) conflate engines -- every embedded engine in a process
    /// serves shard 1 -- so consumers that aggregate across a registry must filter by the log
    /// identity the registration actually points at.
    pub fn same_log(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn set_block_retention_floor(&self, shard_id: ShardId, sequence: u64) {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.block_retention_floor_by_shard.insert(shard_id, sequence);
    }

    /// The shard's block-retention floor, if one is registered.
    pub fn block_retention_floor(&self, shard_id: ShardId) -> Option<u64> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.block_retention_floor_by_shard.get(&shard_id).copied()
    }

    /// Drop the shard's floor, letting GC reclaim freely again. Call this only once no block
    /// resolves inside this shard's WAL.
    pub fn clear_block_retention_floor(&self, shard_id: ShardId) {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.block_retention_floor_by_shard.remove(&shard_id);
    }

    /// Bytes reclaimed from the head of this shard's log so far.
    ///
    /// A record's log id is its byte offset in the log's whole history, which does not change
    /// when the head is reclaimed. This is the difference between that and where the record
    /// physically sits now.
    pub fn base_offset(&self, shard_id: ShardId) -> Result<u64, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        Ok(read_wal_base(&path)?.0)
    }

    /// Physical byte offset of the record with this log id, or `None` if the log id has been
    /// reclaimed and the record no longer exists.
    ///
    /// Returning `None` rather than a wrong offset is the point: a reclaimed log id would
    /// otherwise resolve into the middle of the file and parse as some other record.
    pub fn resolve_log_id(
        &self,
        shard_id: ShardId,
        log_id: u64,
    ) -> Result<Option<u64>, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        Ok(locate_log_id(&inner.root, shard_id, log_id)?.map(|(_, physical)| physical))
    }

    /// Log id of a record currently at `physical_offset`. The inverse of [`Self::resolve_log_id`],
    /// used when recording the address of a record just appended.
    pub fn log_id_at(
        &self,
        shard_id: ShardId,
        physical_offset: u64,
    ) -> Result<u64, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        let (base, header_len) = read_wal_base(&path)?;
        Ok(base.saturating_add(physical_offset.saturating_sub(header_len)))
    }

    /// Read `size` bytes of the record at `log_id`, following the reclaim base.
    ///
    /// `Ok(None)` means the log id was reclaimed.
    pub fn read_at_log_id(
        &self,
        shard_id: ShardId,
        log_id: u64,
        size: u64,
    ) -> Result<Option<Vec<u8>>, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let Some((path, physical)) = locate_log_id(&inner.root, shard_id, log_id)? else {
            return Ok(None);
        };
        // A caller asking by log id does not know how long the record is, so it asks for an upper
        // bound. Reading past the end of the piece would fail and report the record as absent -- a
        // read that should have succeeded -- so clamp to what that piece actually holds.
        let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        let available = length.saturating_sub(physical);
        if available == 0 {
            return Ok(None);
        }
        let bytes = read_at(&path, physical, size.min(available))?;
        inner.stats.reads += 1;
        inner.stats.bytes_read += bytes.len() as u64;
        Ok(Some(bytes))
    }

    /// Reclaim this shard's WAL prefix below `retain_from_sequence`, never going past what the
    /// durable served index proves it can replace.
    ///
    /// See [`DurableIndexAnchor`] for why the proof is an argument. A caller that asks for more
    /// than its anchor covers gets a narrowed reclaim and `clamped_by_durable_index` set, not an
    /// error: the ask is a target, the anchor is the limit, and the difference is reported
    /// rather than silently folded into the counts.
    pub fn gc_before_sequence(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
        durable_index: &DurableIndexAnchor,
    ) -> Result<WriteAheadLogGcReport, WriteAheadLogError> {
        // An anchor minted for another shard says nothing about this one, so it authorizes
        // nothing. Retaining from 0 keeps every record.
        let ceiling = if durable_index.shard_id() == shard_id {
            durable_index.through_sequence().saturating_add(1)
        } else {
            0
        };
        let clamped = retain_from_sequence > ceiling;
        let mut report =
            self.gc_before_sequence_unchecked(shard_id, retain_from_sequence.min(ceiling))?;
        // Report the sequence the caller ASKED for. `effective_retain_from_sequence` already
        // carries what was actually used, and a reclaim that did less than requested should be
        // visible as such instead of looking like a smaller request that succeeded exactly.
        report.retain_from_sequence = retain_from_sequence;
        report.clamped_by_durable_index = clamped;
        Ok(report)
    }

    /// [`gc_before_sequence`](Self::gc_before_sequence) without the durable-index clamp,
    /// subject only to the tail-continuity and block-retention floors below.
    ///
    /// This is the reclaim primitive; the anchored wrapper is the way in. Tests drive this
    /// directly to exercise those two floors in isolation.
    pub(crate) fn gc_before_sequence_unchecked(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> Result<WriteAheadLogGcReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = active_wal_path(&mut inner, shard_id);
        if !path.exists() {
            return Ok(WriteAheadLogGcReport {
                shard_id,
                retain_from_sequence,
                effective_retain_from_sequence: retain_from_sequence,
                ..WriteAheadLogGcReport::default()
            });
            // (an absent log has reclaimed nothing, so the default base of 0 is correct)
        }

        let bytes_before = path.metadata()?.len();
        let (last_sequence, _) = last_wal_sequence_at(&inner.root, shard_id)?;
        // Never delete the highest-sequence record. The WAL file is the sequence-generator
        // source on restart (append seeds last_sequence_by_shard from last_wal_sequence_at), so
        // emptying it entirely on a full reclaim (retain_from > max) would regress the next
        // append to sequence 1 -> sequence REUSE + silent loss: the re-used seq is <= the
        // persisted applied_wal_sequence anchor, so replay's `sequence > watermark` filter drops
        // it. the zone-aligned wal Truncate always retains the tail zone holding the highest
        // sequence for exactly this continuity reason. Clamp the retain floor to keep the tail.
        let effective_retain = retain_from_sequence.min(last_sequence);
        // Never reclaim past the block-retention floor. A record at or above it may carry the
        // only copy of a block's bytes -- a block in the WAL has no copy in a band until it is
        // dumped -- so removing it loses data that the served index still points at, and the
        // read fails at some later, unrelated moment.
        let floor = inner
            .block_retention_floor_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(u64::MAX);
        let clamped_by_block_retention = effective_retain > floor;
        let effective_retain = effective_retain.min(floor);
        // Whole pieces below the retain point go without being read or copied. This uses the
        // NARROWED point, not the one the caller asked for: the two clamps above exist to keep the
        // highest-sequence record and anything a block still depends on, and a piece unlinked here
        // is gone in a way the copy path below could never undo. What remains is the piece being
        // written, which that path handles as it always has.
        let (dropped_segments, dropped_bytes) =
            drop_covered_wal_segments(&inner.root, shard_id, effective_retain)?;
        // Records are appended in strictly ascending sequence -- the live path increments under
        // the append lock, and the replay path refuses anything at or below the last sequence --
        // so the records to keep are a contiguous SUFFIX and the ones to drop are a prefix.
        // That is what makes the whole reclaim expressible as one number: every survivor moves
        // down by exactly the length of the removed prefix.
        let (base_offset, header_len) = read_wal_base(&path)?;
        // Where the records stop. Under preallocation the file continues past them as a zeros
        // reservation, which this pass must neither decode as a record nor copy into the
        // rewritten file; the tail scan also repairs any torn non-zero tail first, exactly as
        // an append would.
        let (_, record_end) = last_wal_sequence_in(&path)?;
        // One line at a time. A log is not bounded by memory, so neither this search nor the
        // copy below may hold it: reclaiming a large log otherwise costs a transient allocation
        // the size of the whole file.
        let mut source = File::open(path.as_path())?;
        source.seek(SeekFrom::Start(header_len))?;
        // Bounded by the cursor below rather than by `take`, because a Take is not seekable and
        // stepping over a block's footer is a seek. the reclaim walk crosses blocks now.
        let mut reader = BufReader::new(source);
        let walk_until = record_end;
        let mut records_before = 0usize;
        let mut records_after = 0usize;
        let mut split = None;
        let mut cursor = header_len;
        loop {
            if cursor >= walk_until {
                break;
            }
            // Step over a block footer this lands on, before reading. The branch below only
            // catches a boundary with PADDING in front of its footer; a block whose records end
            // flush against the footer has none, and the footer is then read as a record -- its
            // first byte is not a frame marker, so the reader falls to newline-delimited mode and
            // returns a fragment.
            //
            // That matters more here than in a scan that only counts: `cursor` becomes `split`
            // below, the byte offset this reclaim keeps from. A fragment read at a boundary moves
            // the line between what is copied and what is dropped, so the pass can retain or
            // discard the wrong records rather than merely report the wrong number of them.
            let stepped = skip_block_footer_if_due(&mut reader, cursor, header_len)?;
            if stepped != cursor {
                cursor = stepped;
                if cursor >= walk_until {
                    break;
                }
            }
            let Some(line) = read_raw_record(&mut reader)? else {
                // Padding before a closed block's footer, or the end. The footer decides.
                match block_is_closed(&mut reader, cursor, header_len, walk_until)? {
                    Some(next) => {
                        cursor = next;
                        continue;
                    }
                    None => break,
                }
            };
            let read = line.len();
            if read == 0 {
                break;
            }
            let trimmed = line
                .strip_suffix(b"\n")
                .unwrap_or(line.as_slice());
            if !trimmed.iter().all(|byte| byte.is_ascii_whitespace()) {
                records_before += 1;
                if split.is_some() {
                    // Past the split every remaining record is retained, and the sequence is
                    // ascending, so there is nothing left to decide -- count it and move on
                    // rather than parsing it again.
                    records_after += 1;
                } else if decode_wal_line(trimmed)?.sequence >= effective_retain {
                    split = Some(cursor);
                    records_after += 1;
                }
            }
            cursor = cursor.saturating_add(read as u64);
        }
        // Nothing retained means everything before the end goes; the split is the end of file.
        let split = split.unwrap_or(cursor);
        let removed_bytes = split.saturating_sub(header_len);
        let new_base = base_offset.saturating_add(removed_bytes);
        let retained_bytes = cursor.saturating_sub(split);

        // Copying the survivors IS the cost of a pass, so reclaiming a sliver off the front of a
        // large log rewrites almost all of it to buy almost nothing. Decline that and let the
        // prefix grow: the same copy then frees far more. Nothing is lost by waiting -- the
        // records stay where they are -- and the condition is self-correcting, because a growing
        // prefix raises the freed fraction until a pass is worth running.
        //
        // Below the copy floor the ratio is meaningless and the rewrite is cheap either way, so
        // small logs reclaim exactly as they did before.
        let worth_rewriting = reclaim_is_worth_rewriting(
            removed_bytes,
            retained_bytes,
            reclaim_min_copy_bytes(),
            reclaim_min_freed_percent(),
        );
        if !worth_rewriting {
            return Ok(WriteAheadLogGcReport {
                shard_id,
                retain_from_sequence,
                effective_retain_from_sequence: effective_retain,
                clamped_by_block_retention,
                clamped_by_durable_index: false,
                records_before,
                records_after: records_before,
                records_removed: 0,
                bytes_before,
                bytes_after: bytes_before,
                base_offset,
                bytes_copied: 0,
                skipped_not_worth_rewrite: true,
                dropped_segments,
                dropped_segment_bytes: dropped_bytes,
            });
        }

        let temp_path = path.with_extension("jsonl.tmp");
        {
            let mut temp = File::create(&temp_path)?;
            // The base goes inside the file so it is swapped in by the same rename as the bytes
            // it describes. A base kept beside the file could survive a crash disagreeing with
            // them, and every address would then resolve to the wrong record.
            temp.write_all(&crate::log_framing::encode_base_header(new_base))?;
            // Copy the retained records byte for byte rather than decoding and re-encoding
            // them. Re-encoding could change a record's length, which would break the offset
            // arithmetic this whole scheme rests on, and it costs a parse per record.
            let mut source = File::open(path.as_path())?;
            source.seek(SeekFrom::Start(split))?;
            std::io::copy(
                &mut BufReader::new(source.take(record_end.saturating_sub(split))),
                &mut temp,
            )?;
            temp.flush()?;
            temp.sync_all()?;
        }
        fs::rename(&temp_path, path.as_path())?;
        sync_parent_dir(&path)?;
        // Reclaim is the only thing that moves the base, so refresh the cache here rather than
        // letting an append compute a log id against a stale one.
        let new_header_len = crate::log_framing::encode_base_header(new_base).len() as u64;
        inner
            .base_by_shard
            .insert(shard_id, (new_base, new_header_len));
        let bytes_after = path.metadata()?.len();
        // The rewritten file ends exactly at its records: no reservation survives the rename,
        // and the record end IS the file length again. Leaving the old entries in place would
        // make the next append place its bytes against the pre-rewrite file.
        inner.verified_len_by_shard.insert(shard_id, bytes_after);
        inner.prealloc_physical_by_shard.remove(&shard_id);
        Ok(WriteAheadLogGcReport {
            shard_id,
            retain_from_sequence,
            records_before,
            records_after,
            records_removed: records_before.saturating_sub(records_after),
            bytes_before,
            bytes_after,
            base_offset: new_base,
            effective_retain_from_sequence: effective_retain,
            clamped_by_block_retention,
            clamped_by_durable_index: false,
            bytes_copied: retained_bytes,
            skipped_not_worth_rewrite: false,
            dropped_segments,
            dropped_segment_bytes: dropped_bytes,
        })
    }

    pub fn stats(&self, shard_id: ShardId) -> WriteAheadLogStats {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.stats.stats_full_scans = inner.stats.stats_full_scans.saturating_add(1);
        let path = active_wal_path(&mut inner, shard_id);
        // Durable bytes, not written bytes. An append puts a record in the file whether or not
        // a barrier followed it, so the file's length is what has been WRITTEN -- reporting it
        // as `persistent_bytes` says unsynced records are on disk to survive a crash, which is
        // the one thing this number exists to answer. Sealed pieces are durable by construction
        // (a piece is only sealed after its barrier); the active piece counts only as far as
        // its last barrier reached.
        let sealed_bytes = wal_all_segment_bytes(&inner.root, shard_id)
            .saturating_sub(path.metadata().map(|metadata| metadata.len()).unwrap_or(0));
        let durable_active = inner
            .durable_active_bytes_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        let durable_sequence = inner
            .durable_sequence_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        WriteAheadLogStats {
            last_sequence: last_wal_sequence_at(&inner.root, shard_id)
                .map(|(sequence, _)| sequence)
                .unwrap_or_default(),
            persistent_bytes: sealed_bytes.saturating_add(durable_active),
            last_flushed_sequence: durable_sequence,
            ..inner.stats
        }
    }

    /// Non-scanning snapshot of the accumulated counters (`inner.stats`) WITHOUT the full-file
    /// `last_wal_sequence_at` scan that `stats()` performs. `last_sequence` / `persistent_bytes`
    /// carry their last-appended cached values rather than a fresh disk read. Used by the phase-1
    /// aging test to read the scan counters without perturbing them.
    pub fn raw_stats(&self, shard_id: ShardId) -> WriteAheadLogStats {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        WriteAheadLogStats {
            last_sequence: inner
                .last_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or(inner.stats.last_sequence),
            ..inner.stats
        }
    }

    /// O(1) read of the cached last WAL sequence for `shard_id` -- the value advanced by the most
    /// recent append on this store -- WITHOUT the full-file `last_wal_sequence_at` scan that
    /// `stats()` runs. The per-write index-anchor step (`shard.applied_wal_sequence = last
    /// sequence`) otherwise calls `stats()` on EVERY write, so its embedded rescan is a second
    /// O(records)-per-write cost under the engine `shards` lock (equal in weight to the append-path
    /// rescan, and confirmed dominant by stack sampling). Under flat append the engine anchors
    /// off this cached value instead. Returns 0 if this store has not yet observed the shard (no
    /// append and no scan); the write path always has, so the value equals the on-disk maximum.
    pub fn cached_last_sequence(&self, shard_id: ShardId) -> u64 {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner
            .last_sequence_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn info(&self, shard_id: ShardId) -> Result<WriteAheadLogInfo, WriteAheadLogError> {
        let inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        if !path.exists() {
            return Ok(WriteAheadLogInfo {
                shard_id,
                path,
                format_version: WRITE_AHEAD_LOG_FORMAT_VERSION,
                ..WriteAheadLogInfo::default()
            });
        }
        let _ = last_wal_sequence_at(&inner.root, shard_id)?;
        let (_, header_len) = read_wal_base(&path)?;
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(header_len))?;
        let mut reader = BufReader::new(file);
        let mut start_sequence = 0_u64;
        let mut current_sequence = 0_u64;
        let mut records = 0_usize;
        // Read BYTE lines, not String lines.
        //
        // `lines()` yields a String and therefore demands valid UTF-8 of every record. That held
        // for as long as every payload was text, and it fails on the first binary record with
        // "stream did not contain valid UTF-8" -- before the decoder, which handles both
        // encodings, ever gets to look at it. Escaping the newline out of a binary payload keeps
        // records SPLITTABLE; it cannot make arbitrary bytes valid UTF-8, and nothing should
        // require them to be.
        let (_, info_header_len) = read_wal_base(&path)?;
        let info_len = path.metadata().map(|meta| meta.len()).unwrap_or(0);
        let mut info_at = info_header_len;
        loop {
            // The same first step the other walkers take. Without it a footer landed on flush
            // against a block's last record is read as a record, and the counts and sequences
            // reported here describe a log that stops at the first such boundary.
            info_at = skip_block_footer_if_due(&mut reader, info_at, info_header_len)?;
            let Some(mut line) = read_raw_record(&mut reader)? else {
                match block_is_closed(&mut reader, info_at, info_header_len, info_len)? {
                    Some(next) => {
                        info_at = next;
                        continue;
                    }
                    None => break,
                }
            };
            info_at = info_at.saturating_add(line.len() as u64);
            // Only a text record carries a trailing newline; a binary frame ends where its
            // declared length ends, and trimming it would take a byte of the payload.
            if line.first() != Some(&crate::log_framing::FRAME_MAGIC_V3) {
                while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
                    line.pop();
                }
            }
            if line.is_empty() {
                continue;
            }
            let record = decode_wal_line(&line)?;
            if start_sequence == 0 {
                start_sequence = record.sequence;
            }
            current_sequence = current_sequence.max(record.sequence);
            records += 1;
        }
        // The whole log, not just its newest piece.
        let length_bytes = wal_all_segment_bytes(&inner.root, shard_id);
        let sealed_bytes = length_bytes.saturating_sub(path.metadata()?.len());
        let durable_active = inner
            .durable_active_bytes_by_shard
            .get(&shard_id)
            .copied()
            .unwrap_or(0);
        Ok(WriteAheadLogInfo {
            shard_id,
            path,
            start_sequence,
            current_sequence,
            records,
            length_bytes,
            // Durable bytes and the sequence a barrier actually covered. The previous values --
            // the file length, and `last_flushed_sequence.max(current_sequence)` -- both took
            // the highest number in sight, so an unsynced append reported itself as durable.
            // Understating is survivable; overstating is the failure this field exists to
            // prevent, so a shard this handle has never synced reports 0 rather than guessing.
            persistent_length_bytes: sealed_bytes.saturating_add(durable_active),
            last_flushed_sequence: inner
                .durable_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or(0),
            format_version: WRITE_AHEAD_LOG_FORMAT_VERSION,
        })
    }
}

impl Default for LocalWriteAheadLogStore {
    fn default() -> Self {
        let scratch = crate::scratch::owned_scratch_dir("wals");
        let store = Self::new(scratch.path());
        store
            .inner
            .lock()
            .expect("write-ahead log lock poisoned")
            .scratch = Some(scratch);
        store
    }
}

/// Read at most `size` bytes from `path`, starting at `physical`.
/// Drop whole pieces that hold nothing worth keeping.
///
/// A piece whose last record is below the retain floor holds nothing above it -- records are
/// appended in ascending sequence -- so the whole file goes, and nothing is copied. Stops at the
/// first piece that still holds something: the pieces are in order, so everything after it does too.
///
/// Returns how many pieces went and how many bytes they held.
fn drop_covered_wal_segments(
    root: &Path,
    shard_id: ShardId,
    retain_from_sequence: u64,
) -> Result<(usize, u64), WriteAheadLogError> {
    let active = write_ahead_log_path(root, shard_id);
    let mut dropped = 0usize;
    let mut freed = 0u64;
    // Stop after this many, so the lock every append needs is not held for the whole backlog.
    // Whatever is left is dropped by the next pass: pieces go from the front in order, so stopping
    // early leaves the rest exactly where the next pass would have looked anyway.
    let limit = wal_reclaim_max_segments_per_pass();
    for path in wal_segment_paths(root, shard_id) {
        if limit > 0 && dropped >= limit {
            break;
        }
        if path == active || !path.exists() {
            continue;
        }
        let (last, _) = last_wal_sequence_in(&path)?;
        if last == 0 {
            // Holds no record at all; nothing to keep and nothing to lose.
        } else if last >= retain_from_sequence {
            break;
        }
        freed = freed.saturating_add(path.metadata().map(|meta| meta.len()).unwrap_or(0));
        fs::remove_file(&path)?;
        dropped += 1;
    }
    if dropped > 0 {
        sync_parent_dir(&active)?;
    }
    Ok((dropped, freed))
}

/// One byte at `offset`, or None at/past the end of the file.
fn byte_at(path: &Path, offset: u64) -> Result<Option<u8>, WriteAheadLogError> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut byte = [0u8; 1];
    match std::io::Read::read(&mut file, &mut byte)? {
        0 => Ok(None),
        _ => Ok(Some(byte[0])),
    }
}

fn read_at(path: &Path, physical: u64, size: u64) -> Result<Vec<u8>, WriteAheadLogError> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(physical))?;
    let mut bytes = vec![0; size as usize];
    let read = file.read(&mut bytes)?;
    bytes.truncate(read);
    Ok(bytes)
}

/// TS_WAL_SEGMENT_BYTES: roll the log into a new piece once the one being written passes this.
///
/// Zero never rolls, which is one file. Rolling lets reclaim unlink a whole earlier piece instead
/// of rewriting the file to keep the survivors.
///
/// **Default 256 KiB**, chosen by measurement rather than by feel:
///
/// * It matches `wal_preallocate_chunk`, so a piece is a whole number of the units the file is
///   already grown in and rolling costs no extra preallocation. Measured on-disk overhead against
///   never rolling was 1,258 bytes on a 1.3 MB log.
/// * It is the largest size that still rolls usefully inside the log the index-dump threshold
///   leaves. That threshold is a megabyte, so 1 MiB pieces would barely roll in a steady system;
///   256 KiB gives four or five.
///
/// What it buys, measured across two runs at 4,000 records:
///
/// | | reclaim keeping 10% | keeping 90% |
/// |---|---|---|
/// | never rolling | 34-68 ms, copies 119,748 B | copies 1,075,475 B |
/// | 256 KiB | 12-15 ms, copies 119,748 B, unlinks 4 pieces | copies 144,482 B |
///
/// The copy is IDENTICAL in the first column: reclaim copies survivors, and when most of the log
/// is dropped the survivors are small either way. The saving there is the SCAN -- a rolled reclaim
/// skips the pieces it unlinks instead of reading the file to find what stays. Rolling reduces the
/// COPY only when most of the log is kept, which is what a blocked floor produces.
///
/// The exposure to weigh against that is file count: pieces accumulate while reclaim is blocked,
/// and a log that reached a gigabyte would hold four thousand of them.
fn wal_segment_bytes() -> u64 {
    if let Some(threshold) = SEGMENT_BYTES_OVERRIDE.with(|value| value.get()) {
        return threshold;
    }
    std::env::var("TS_WAL_SEGMENT_BYTES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(DEFAULT_WAL_SEGMENT_BYTES)
}

/// Rolling threshold when nothing sets one. See [`wal_segment_bytes`] for how it was chosen.
/// Zero is still accepted and still means "never roll".
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 256 * 1024;

thread_local! {
    /// Per-thread override of the rolling threshold.
    ///
    /// Per thread, not per process: appending happens on the calling thread, and a test that set a
    /// process-wide threshold would make every other test running beside it roll too.
    static SEGMENT_BYTES_OVERRIDE: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
}

/// Set the rolling threshold for THIS THREAD. The environment variable is the supported way to set
/// it; this exists so a test can roll without disturbing anything running beside it.
pub fn set_wal_segment_bytes_for_test(threshold: Option<u64>) {
    SEGMENT_BYTES_OVERRIDE.with(|value| value.set(threshold));
}

/// The log id one past everything the sealed pieces hold.
///
/// Derived rather than stored: sealing is a rename and creating the next piece is a separate step,
/// so a crash can land between them. Computing this from what is on disk gives the same answer
/// either way.
fn log_id_after_sealed(root: &Path, shard_id: ShardId) -> Result<u64, WriteAheadLogError> {
    let mut end = 0;
    for path in wal_segment_paths(root, shard_id) {
        if path == write_ahead_log_path(root, shard_id) || !path.exists() {
            continue;
        }
        let (base, header_len) = read_wal_base(&path)?;
        let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        end = end.max(base.saturating_add(length.saturating_sub(header_len)));
    }
    Ok(end)
}

/// Seal the piece being written and start a fresh one, if it has grown past the threshold.
///
/// Called with the append lock held, after a record has been made durable, so the piece being
/// sealed is complete.
/// Make sure there is a piece to append to, starting where the sealed pieces end.
///
/// Sealing is a rename and creating the next piece is a separate step, so a crash can leave sealed
/// pieces and nothing being written. An absent piece reads as starting at log id zero, which would
/// hand out log ids the sealed pieces already own. Creating it here, from what is on disk, gives
/// the same answer whether or not that crash happened.
fn ensure_active_wal_segment(
    inner: &mut WriteAheadLogInner,
    shard_id: ShardId,
) -> Result<(), WriteAheadLogError> {
    let path = active_wal_path(inner, shard_id);
    // A piece of zero length is one that was created and never got its header, which a crash
    // between the two leaves behind. It has to be treated as absent: `read_wal_base` reads an
    // empty file as starting at log id ZERO, and if there are sealed pieces then zero is an
    // address they already own, so leaving it alone hands the same address out twice.
    let empty = path
        .metadata()
        .map(|metadata| metadata.len() == 0)
        .unwrap_or(false);
    if path.exists() && !empty {
        return Ok(());
    }
    let start = log_id_after_sealed(&inner.root, shard_id)?;
    if start == 0 {
        // No sealed pieces: a log that starts at the beginning needs no header, exactly as before.
        return Ok(());
    }
    let header = crate::log_framing::encode_base_header(start);
    let mut file = File::create(path.as_path())?;
    file.write_all(&header)?;
    file.flush()?;
    file.sync_all()?;
    sync_parent_dir(&path)?;
    inner
        .base_by_shard
        .insert(shard_id, (start, header.len() as u64));
    inner
        .verified_len_by_shard
        .insert(shard_id, header.len() as u64);
    Ok(())
}

fn roll_wal_segment_if_due(
    inner: &mut WriteAheadLogInner,
    shard_id: ShardId,
    record_end: Option<u64>,
) -> Result<bool, WriteAheadLogError> {
    let threshold = wal_segment_bytes();
    if threshold == 0 {
        return Ok(false);
    }
    let path = active_wal_path(inner, shard_id);
    let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    // From the cache, not the file. This runs on EVERY append to answer "is the piece full", and
    // `read_wal_base` opens the file and reads its header to do it -- through a `BufReader`, whose
    // buffer is eight kilobytes. That was 8,240 bytes and an open per write, to learn a number the
    // shard already knows: `base_by_shard` is written when a piece is created and refreshed by
    // both reclaim and the roll below, which are the only things that move it.
    let (base, header_len) = cached_wal_base(inner, shard_id)?;
    // Where the records stop. Under preallocation the file is longer than the records, and both
    // decisions below -- is this piece full, and what log id does the next piece start at --
    // must come from the records: log ids are cumulative RECORD bytes, and a base derived from
    // the reservation would hand out ids nothing ever occupies.
    let record_end = record_end.unwrap_or(length).min(length);
    let holds = record_end.saturating_sub(header_len);
    if holds < threshold {
        return Ok(false);
    }

    // Trim the reservation before sealing, so a sealed piece's length IS its contents and every
    // reader that derives a sealed piece's end from its file length keeps being right.
    //
    // The fsync here is what makes the piece's CONTENTS durable, and it has to happen whether or
    // not there was anything to trim. A record appended with sync=false sits in the page cache
    // until a barrier, and the barrier that follows -- `group_commit_sync` -- opens the piece
    // being written, which after this rename is the NEW one. Sealing an unsynced piece would
    // therefore leave those bytes with no barrier that ever covers them, while the commit returns
    // success: acked and not durable.
    //
    // Until now that only held by accident. The trim runs when a preallocated file is longer than
    // its records, which is the default, so the fsync usually happened -- for an unrelated reason.
    // With preallocation off it did not happen at all. The block store's own roll has always done
    // this deliberately ("the outgoing slab may hold relaxed (un-fsynced) bulk appends; make them
    // durable before we seal"); this one now does too.
    {
        let file = OpenOptions::new().write(true).open(path.as_path())?;
        if record_end < length {
            file.set_len(record_end)?;
        }
        crate::durability_metrics::record_barrier("wal_seal_outgoing_piece");
        file.sync_all()?;
    }
    inner.prealloc_physical_by_shard.remove(&shard_id);

    // Seal by rename: atomic, so the piece is either being written or sealed, never neither.
    let sealed = sealed_wal_path(&inner.root, shard_id, base);
    fs::rename(path.as_path(), &sealed)?;
    sync_parent_dir(&sealed)?;
    // Sealed, and nothing to append to yet. A crash here leaves a log whose pieces are all sealed,
    // and an absent piece reads as starting at log id zero -- which those pieces already own.
    crate::fault::point("wal/roll/after_rename");

    // Start the next piece where this one ended. If a crash lands before this, the next append
    // derives the same starting point from the sealed pieces.
    let next_base = base.saturating_add(holds);
    let mut file = File::create(path.as_path())?;
    // Created, no header yet. A crash here leaves a piece of zero length, which reads as starting
    // at log id zero for the same reason.
    crate::fault::point("wal/roll/after_create");
    file.write_all(&crate::log_framing::encode_base_header(next_base))?;
    file.flush()?;
    // No barrier for the header, and none for the directory entry. The header rides the barrier
    // that makes the first record in this piece durable -- same file, and fsync covers the inode
    // rather than the handle -- and if no record ever lands here the header was worth nothing. The
    // directory entry is owed until the next durable barrier, which is before any record in this
    // piece can be acked.
    inner.dir_sync_owed_by_shard.insert(shard_id);

    let new_header_len = crate::log_framing::encode_base_header(next_base).len() as u64;
    inner
        .base_by_shard
        .insert(shard_id, (next_base, new_header_len));
    inner.verified_len_by_shard.insert(shard_id, new_header_len);
    // Whether a log is written in blocks is a property of the PIECE, and this just replaced it.
    // The decision is cached per shard because it is answered once per piece, not once per
    // append -- but nothing invalidated it here, so a shard that latched "no blocks" over a
    // piece that was already occupied when this process first appended to it kept that answer
    // for every piece it rolled into afterwards, however empty they started. Measured on the
    // serving store: the binary carries the footer writer, the log rolls steadily, and not one
    // piece across the whole store carries a footer.
    //
    // Dropping the entry rather than setting it: the next append re-reads the new piece, which is
    // the same question this asks, and there is only one place that answers it.
    inner.block_mode_by_shard.remove(&shard_id);
    // The offset and sequence a footer would record belong to the piece that just sealed.
    inner.block_last_record_by_shard.remove(&shard_id);
    // How many bytes of the ACTIVE piece a barrier has covered -- and the active piece is now a
    // different, empty one. The piece just sealed is counted from the filesystem as a sealed
    // piece from here on, so leaving its length here counts it twice, and
    // `persistent_length_bytes` then reports more durable bytes than the log physically holds.
    // That is the direction the field exists to rule out: understating is survivable, overstating
    // says unsynced records are on disk to survive a crash.
    //
    // It could not correct itself either. One of the three writers takes a `.max()` rather than
    // overwriting, so a stale high value survives every barrier that path serves.
    //
    // Removed rather than set: absent reads as zero, and zero is what a piece whose header has
    // not been barriered yet has actually got durable.
    inner.durable_active_bytes_by_shard.remove(&shard_id);
    Ok(true)
}

fn write_ahead_log_path(root: &Path, shard_id: ShardId) -> PathBuf {
    root.join(format!("shard-{shard_id}.wal.jsonl"))
}

/// The active piece's path for this shard, built once and kept.
///
/// Same answer as [`write_ahead_log_path`], without rebuilding it. Used on the append path, which
/// asked for it six times a write; callers that are not per-write can keep using the free
/// function.
/// How many times an append asks for the active path. Test-only: the answer decides whether
/// returning the cached `Arc` instead of a copy of it is worth the call sites it would touch.
#[cfg(test)]
thread_local! {
    pub(crate) static ACTIVE_PATH_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn active_wal_path(inner: &mut WriteAheadLogInner, shard_id: ShardId) -> std::sync::Arc<PathBuf> {
    #[cfg(test)]
    ACTIVE_PATH_CALLS.with(|calls| calls.set(calls.get() + 1));
    if let Some(path) = inner.active_path_by_shard.get(&shard_id) {
        // The Arc, not a copy of what it holds. Measured at four asks per append and a
        // thirty-three byte name, the copies were 132 of the append's 225 allocated bytes.
        return path.clone();
    }
    // NOTE: the free function, deliberately. This IS the thing that builds the name; calling the
    // accessor here would be infinite recursion.
    let built = std::sync::Arc::new(write_ahead_log_path(&inner.root, shard_id));
    inner.active_path_by_shard.insert(shard_id, built.clone());
    built
}

/// An earlier piece of a shard's log, named by the log id its contents start at.
///
/// Zero-padded so the names sort into log order, which is the order the pieces are read in.
fn sealed_wal_path(root: &Path, shard_id: ShardId, start_log_id: u64) -> PathBuf {
    root.join(format!("shard-{shard_id}.wal.{start_log_id:020}.jsonl"))
}

/// Whether this file is an earlier piece of the given shard's log.
///
/// The piece being written has no number in its name, so it is not one of these -- an empty middle
/// section does not parse as a log id.
fn sealed_wal_start_log_id(path: &Path, shard_id: ShardId) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let middle = name
        .strip_prefix(&format!("shard-{shard_id}.wal."))?
        .strip_suffix(".jsonl")?;
    middle.parse().ok()
}

/// Every file that makes up a shard's log, oldest first, with the one being written last.
///
/// A log that has never been rolled is one file, and this returns just that -- the same path the
/// rest of the code has always used.
/// Bytes of every piece of this shard's log, sealed pieces included.
///
/// The active segment alone is not the log: after a roll its length is the size of the newest
/// piece, so reporting it as the log's length makes a log SHRINK as it grows.
fn wal_all_segment_bytes(root: &Path, shard_id: ShardId) -> u64 {
    wal_segment_paths(root, shard_id)
        .into_iter()
        .filter_map(|path| path.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn wal_segment_paths(root: &Path, shard_id: ShardId) -> Vec<PathBuf> {
    let mut sealed = fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter_map(|path| {
            sealed_wal_start_log_id(&path, shard_id).map(|start| (start, path))
        })
        .collect::<Vec<_>>();
    sealed.sort_by_key(|(start, _)| *start);
    let mut paths = sealed.into_iter().map(|(_, path)| path).collect::<Vec<_>>();
    paths.push(write_ahead_log_path(root, shard_id));
    paths
}

/// Where a log id lives: which piece of the log, and how far into it.
///
/// `None` when the log id is behind everything still kept -- reclaimed -- or ahead of everything
/// written.
fn locate_log_id(
    root: &Path,
    shard_id: ShardId,
    log_id: u64,
) -> Result<Option<(PathBuf, u64)>, WriteAheadLogError> {
    for path in wal_segment_paths(root, shard_id) {
        if !path.exists() {
            continue;
        }
        let (base, header_len) = read_wal_base(&path)?;
        let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        if length <= header_len {
            continue;
        }
        let holds = length - header_len;
        if log_id < base {
            // Behind this piece, and the pieces are in order, so behind the whole log.
            return Ok(None);
        }
        if log_id - base < holds {
            return Ok(Some((path, header_len + (log_id - base))));
        }
    }
    Ok(None)
}

/// Whether a handle still refers to a file that exists under some name.
///
/// A descriptor outlives the unlinking of its file, and `flock` on an unlinked inode succeeds --
/// so a cached lock handle whose file has been removed would grant a lock nobody else contends
/// for. Checked per acquisition; a link count of zero means re-open.
#[cfg(unix)]
fn still_linked(file: &File) -> bool {
    use std::os::unix::fs::MetadataExt;
    file.metadata().map(|meta| meta.nlink() > 0).unwrap_or(false)
}

/// Locks are advisory no-ops off unix, so a stale handle costs nothing there.
#[cfg(not(unix))]
fn still_linked(_file: &File) -> bool {
    true
}

struct WalAppendLock {
    file: std::sync::Arc<File>,
}

impl WalAppendLock {
    /// Take the append lock, opening the lock file only the first time this shard needs it.
    ///
    /// An `Arc` clone is a refcount bump, so taking the lock allocates nothing once the handle
    /// exists. What still happens every time is the `flock` and its release -- that is the lock.
    fn acquire(
        inner: &mut WriteAheadLogInner,
        shard_id: ShardId,
    ) -> Result<Self, WriteAheadLogError> {
        let file = match inner.append_lock_by_shard.get(&shard_id) {
            // Only if the handle still names a file. A cached descriptor on an UNLINKED inode
            // would still lock successfully while another process, opening the path afresh, locked
            // a different inode -- both would think they held the append lock. Re-opening by path
            // every time made that impossible; this restores the guarantee for the cost of one
            // fstat, which is still cheaper than the open and close it replaces.
            Some(file) if still_linked(file) => file.clone(),
            _ => {
                fs::create_dir_all(&inner.root)?;
                let path = inner.root.join(format!("shard-{shard_id}.wal.lock"));
                let file = std::sync::Arc::new(
                    OpenOptions::new()
                        .create(true)
                        .read(true)
                        .write(true)
                        .open(path)?,
                );
                inner.append_lock_by_shard.insert(shard_id, file.clone());
                file
            }
        };
        lock_file_exclusive(&file)?;
        Ok(Self { file })
    }
}

impl Drop for WalAppendLock {
    fn drop(&mut self) {
        let _ = unlock_file(&self.file);
    }
}

#[cfg(unix)]
fn lock_file_exclusive(file: &File) -> Result<(), std::io::Error> {
    const LOCK_EX: i32 = 2;
    loop {
        let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX) };
        if rc == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

#[cfg(unix)]
fn unlock_file(file: &File) -> Result<(), std::io::Error> {
    const LOCK_UN: i32 = 8;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
unsafe extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[cfg(not(unix))]
fn lock_file_exclusive(_file: &File) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(not(unix))]
fn unlock_file(_file: &File) -> Result<(), std::io::Error> {
    Ok(())
}

/// Process-unique atomic-batch identifier. Only needs to disambiguate concurrently-live batches
/// within one WAL replay window (a monotonic counter is more than enough); it is never persisted
/// as a stable key across restarts.
fn next_batch_id() -> u64 {
    static BATCH_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    BATCH_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn wal_bulk_relaxed_durability() -> bool {
    crate::engine::bulk_ingest_mode()
}


/// Resolve the last WAL sequence for `shard_id` immediately before an append, under the `inner`
/// lock. Fast path (gate on): if a cached sequence AND a verified file length are both present and
/// the file on disk is still exactly that length, the warm cache is authoritative -- return it
/// without the O(records) `last_wal_sequence_at` scan. Otherwise fall back to the full scan (which
/// also repairs a torn tail via `set_len`), reconcile the verified length against the resulting
/// on-disk length, and return `max(cache, disk)` exactly as the pre-gate path did.
///
/// Also reports the log's length as it was observed, because the caller is about to append at
/// exactly that offset and the append lock is held across both -- so asking again would be asking
/// a question already answered.
fn resolve_last_sequence_for_append(
    inner: &mut WriteAheadLogInner,
    shard_id: ShardId,
    flat_append: bool,
) -> Result<(u64, u64), WriteAheadLogError> {
    let cached_last_sequence = inner
        .last_sequence_by_shard
        .get(&shard_id)
        .copied()
        .unwrap_or_default();
    if flat_append {
        if let (true, Some(&verified_len)) = (
            inner.last_sequence_by_shard.contains_key(&shard_id),
            inner.verified_len_by_shard.get(&shard_id),
        ) {
            let path = active_wal_path(inner, shard_id);
            let on_disk_len = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            // Under preallocation the file is allowed to be longer than the records; unchanged
            // means "still exactly the size this process grew it to". Either way, the offset
            // handed back is where the RECORDS end, which is where the next one goes.
            let expected_physical = if wal_preallocate_enabled() {
                inner
                    .prealloc_physical_by_shard
                    .get(&shard_id)
                    .copied()
                    .unwrap_or(verified_len)
            } else {
                verified_len
            };
            if on_disk_len == expected_physical {
                // An unchanged length is not enough under preallocation: another process
                // appending INSIDE the reservation leaves the file the same size. Its write
                // starts with a frame byte, never NUL, so one byte read at our record end tells
                // the two apart -- zero (or reading at the very end) means the reservation is
                // still ours from here on.
                let interloper = wal_preallocate_enabled()
                    && verified_len < on_disk_len
                    && byte_at(&path, verified_len)?.is_some_and(|byte| byte != 0);
                if !interloper {
                    return Ok((cached_last_sequence, verified_len));
                }
            }
        }
    }
    inner.stats.append_full_scans = inner.stats.append_full_scans.saturating_add(1);
    let (disk_last_sequence, active_record_end) = last_wal_sequence_at(&inner.root, shard_id)?;
    // The scan may have truncated a torn tail, and under preallocation it reports where the
    // records stop rather than where the file ends -- the file may keep a zeros reservation
    // after them. The next append belongs at the record end; the fast path compares the file
    // against its physical size separately.
    let reconciled_physical = active_wal_path(inner, shard_id)
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let record_end = active_record_end.min(reconciled_physical);
    inner.verified_len_by_shard.insert(shard_id, record_end);
    if wal_preallocate_enabled() {
        inner
            .prealloc_physical_by_shard
            .insert(shard_id, reconciled_physical);
    }
    Ok((cached_last_sequence.max(disk_last_sequence), record_end))
}

fn wal_env_flag_on(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Default-ON gate read: the fix is LIVE unless explicitly disabled with
/// `=0|false|no|off`. Shipped write-path/raft fixes use this so production gets the
/// fixed behavior by default; the env var remains only as an escape hatch.
fn wal_env_flag_default_on(name: &str) -> bool {
    !matches!(
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

/// TS_WAL_RECLAIM_MAX_SEGMENTS_PER_PASS: how many sealed pieces one reclaim pass may unlink.
///
/// The unlinking happens while the log's lock is held, and every append takes that lock, so an
/// unbounded pass stops every writer for as long as it takes -- about 45 microseconds per piece,
/// measured. Zero means unbounded, which is what this did before.
///
/// A bound does not reduce total work; it increases it, because every extra pass pays the
/// directory barrier again. It bounds how long the lock is held in one pass, which is the thing
/// writers actually feel.
fn wal_reclaim_max_segments_per_pass() -> usize {
    std::env::var("TS_WAL_RECLAIM_MAX_SEGMENTS_PER_PASS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(64)
}

/// TS_WAL_PREALLOCATE (default ON): write records inside a file that has already been grown
/// to size instead of growing it on every append. fdatasync on a growing file must persist the
/// new length -- the bytes are unreadable without it -- so every barrier pays a metadata write.
/// Growing the file in chunks moves that cost to once per chunk: measured 42-55% cheaper at a
/// barrier per record (issue #188). Live by default now that the full suite runs green with it
/// forced on; the env var remains the escape hatch (=0 restores growing appends, and a log
/// written either way reads back under the other, since the tail scan treats a zeros run as
/// room and a plain file simply ends at its records).
fn wal_preallocate_enabled() -> bool {
    wal_env_flag_default_on("TS_WAL_PREALLOCATE")
}

/// TS_WAL_PREALLOCATE_CHUNK: how far past the write the file is grown when it runs out of room.
/// Bounds both how often the size-persisting barrier is paid and how many zero bytes a crash
/// leaves for the tail scan to walk back over.
fn wal_preallocate_chunk() -> u64 {
    std::env::var("TS_WAL_PREALLOCATE_CHUNK")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .filter(|bytes| *bytes >= 4096)
        .unwrap_or(256 * 1024)
}

/// Group commit is unconditional: concurrent WAL fsyncs coalesce into shared durability
/// barriers. The append records every byte durably before ack; only the fsync is batched across
/// writers, so an acked write is durable whether or not it shared its barrier.
///
/// This was gated and default-ON. The off path forced an exact per-append fsync and no test
/// exercised it, so it was a configuration that shipped untested -- removed rather than kept.
pub fn group_commit_configured() -> bool {
    true
}

/// `TS_WAL_COMMIT_DELAY_US` (config `[wal] commit_delay_us`): optional deliberate widening of the
/// group-commit batch window under extreme load. Default 0 = pure timer-less coalescing (the group
/// is exactly what accumulates during one durable barrier's in-flight duration).
pub fn group_commit_delay() -> std::time::Duration {
    let micros = std::env::var("TS_WAL_COMMIT_DELAY_US")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    std::time::Duration::from_micros(micros)
}

/// The redundant per-append WAL parent-dir fsync is always skipped -- it is safe once the file
/// exists, and group commit is unconditional.
///
/// This read `!TS_WAL_LEGACY_RECOVERY || group_commit_enabled()`. With group commit always on the
/// disjunction was already always true, so the legacy-recovery hatch had no effect here even
/// before the gate was removed.
fn wal_relaxed_dir_sync() -> bool {
    true
}


/// Whether THIS log is written in blocks, decided once and remembered.
///
/// block mode is a property of the log, not of a flag. A log written without blocks has records
/// sitting exactly where footer slots would go, and there is no safe way to start blocking it
/// afterwards: writing a footer lands on durable bytes, and skipping the slot leaves a gap that
/// only a footer could tell a reader to cross. Both were tried; the first corrupts a record and
/// the second orphans every record written after it.
///
/// So a log gets blocks if it is EMPTY when this process first appends to it. An existing log
/// keeps the shape it already has, whatever the flag says, for as long as it exists.
fn shard_uses_blocks(
    inner: &mut WriteAheadLogInner,
    shard_id: ShardId,
    path: &Path,
) -> Result<bool, WriteAheadLogError> {
    // The footer is unconditional: the record framing that ships ON cannot locate the log tail
    // without it, and a store written with framing but no footer degrades 3.8x and keeps growing.
    // Kept as a `false` branch rather than deleted so the surrounding shape stays reviewable.
    if false {
        return Ok(false);
    }
    if let Some(known) = inner.block_mode_by_shard.get(&shard_id) {
        return Ok(*known);
    }
    let (_, header_len) = read_wal_base(path)?;
    let empty = match path.metadata() {
        Ok(meta) => meta.len() <= header_len,
        Err(_) => true,
    };
    // An existing log qualifies only if it already carries a footer, which means it was written
    // in blocks from the start.
    let uses_blocks = if empty {
        true
    } else {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        last_written_footer(&file, header_len, len)?.is_some()
    };
    inner.block_mode_by_shard.insert(shard_id, uses_blocks);
    Ok(uses_blocks)
}

/// How many times an append OPENS the active piece, and how many times it asks the filesystem for
/// that piece's physical length.
///
/// A batch is one crash-atomic group under one barrier, but it used to open, write, stat and close
/// the piece once per record -- 4N syscalls against that single barrier. It now opens once and
/// reads the length once, and these say so as a COUNT, which is the honest measurement when the
/// syscalls are the cost and the machine cannot resolve the wall clock.
///
/// Thread-local and test-only, like `ACTIVE_PATH_CALLS` above, and for the same reason: the suite
/// runs tests in parallel and every one of them that touches a log opens a file. A process-global
/// counter reads other tests' work as its own -- which it did, as a flake on `main`.
#[cfg(test)]
thread_local! {
    pub(crate) static WAL_FILE_OPENS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(crate) static WAL_FILE_STATS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn append_record_locked(
    inner: &mut WriteAheadLogInner,
    record: &WriteAheadLogRecord,
    sync: bool,
    known_offset: Option<u64>,
) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
    append_record_locked_on(inner, record, sync, known_offset, None)
}

/// The same append, optionally writing through a handle the caller already opened.
///
/// Only the atomic batch passes one. It holds the append lock and the `inner` lock across its
/// whole loop and rolls the piece only AFTER the batch, so the path cannot be sealed and renamed
/// underneath a handle held across records -- which is the one thing that would make reusing it
/// unsafe.
fn append_record_locked_on(
    inner: &mut WriteAheadLogInner,
    record: &WriteAheadLogRecord,
    sync: bool,
    known_offset: Option<u64>,
    reuse: Option<(&mut std::fs::File, &mut u64)>,
) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
    let path = active_wal_path(inner, record.shard_id);
    // The caller has usually just measured this under the same append lock, so taking its answer
    // costs nothing and asking again costs a stat. With no answer in hand, the record end comes
    // from the cache this function itself maintains -- under preallocation the file's length is
    // the RESERVATION, so the metadata fallback would put the record after the zeros; and a
    // caller looping appends (the atomic batch) needs each write to advance the next one's
    // offset, which only holds if the cache is updated here, where the write happens.
    let mut offset = match known_offset {
        Some(offset) => offset,
        None => match inner.verified_len_by_shard.get(&record.shard_id) {
            Some(&record_end) if wal_preallocate_enabled() => record_end,
            _ => {
                if wal_preallocate_enabled() && path.exists() {
                    // Cold under the gate: the file may end in a reservation another process
                    // left, so ask the tail scan where the records stop.
                    let (_, record_end) = last_wal_sequence_in(&path)?;
                    record_end
                } else {
                    path.metadata().map(|metadata| metadata.len()).unwrap_or(0)
                }
            }
        },
    };
    // Frame the record with a length + SHA-256 digest so a later value-preserving bit-flip in
    // this committed line is detected on read (see `crate::log_framing`). Offsets/stats below
    // use the real byte length, so framing is transparent to the append report and replication.
    // Taken out and returned below: framing borrows the buffer while the rest of `inner` is still
    // needed, and `mem::take` is the cheap way to say that without splitting the struct.
    let mut bytes = std::mem::take(&mut inner.encode_scratch);
    // BOTH flags, not just the first. The fused path writes the RAW marker and unescaped
    // protobuf, which is what the length-prefixed frame wants and what the LINE frame cannot
    // take: a line frame has to escape the newlines out of the payload first, and carries a
    // different marker to say so. Fusing on `binary_records` alone wrote raw bytes into a line
    // frame, and a value containing a newline then split into two unreadable records --
    // `what_each_frame_costs_on_disk` toggles the frame flag precisely to catch that, and did.
    // Compression is excluded from the borrowing writer deliberately: it reserves
    // the frame from a length the payload does not have yet. See `encode`.
    if crate::wal_proto::binary_records_enabled()
        && crate::log_framing::binary_frame_enabled()
        && !crate::wal_proto::compress_records_enabled()
    {
        // The payload lands directly in the frame. Building it separately meant carrying the
        // record twice -- once into its own buffer and once into this one -- and at a four
        // kilobyte value the second copy was four kilobytes of pure duplication.
        let prepared = crate::wal_proto::prepare(record).map_err(|err| {
            WriteAheadLogError::Corruption(format!("engine wal record encode failed: {err}"))
        })?;
        let written = crate::log_framing::encode_framed_into(
            prepared.payload_len(),
            |out| prepared.put(record, out),
            &mut bytes,
        );
        match written {
            // The record wrote a different number of bytes than it measured. The frame declares
            // its length up front, so writing it would make every record after it unreadable --
            // fail the append instead, with the buffer already cleared.
            Err(mismatch) => {
                inner.encode_scratch = bytes;
                return Err(WriteAheadLogError::Corruption(format!(
                    "engine wal record framing failed: {mismatch}"
                )));
            }
            Ok(Err(err)) => {
                inner.encode_scratch = bytes;
                return Err(WriteAheadLogError::Corruption(format!(
                    "engine wal record encode failed: {err}"
                )));
            }
            Ok(Ok(())) => {}
        }
    } else {
        let payload = encode_wal_payload(record)?;
        crate::log_framing::encode_record_into(&payload, &mut bytes);
    }
    // Close the block if this record will not fit in what is left of it, and start the next.
    //
    // A record is never split across the boundary: the one that does not fit moves whole into
    // the next block. That wastes the tail of a block -- bounded by the largest record -- and
    // buys a reader that never has to reassemble anything, which is the trade a log makes when
    // its own footer is the thing keeping recovery cheap.
    let mut close_block_and_advance: Option<(u64, Vec<u8>)> = None;
    if shard_uses_blocks(inner, record.shard_id, &path)? {
        let (_, header_len) = cached_wal_base(inner, record.shard_id)?;
        if offset >= header_len {
            let relative = offset - header_len;
            let index = block_of(relative);
            let data_end = block_data_end(index);
            // A footer never writes a footer behind where records already reach. In a log that
            // was written WITHOUT blocks -- every log that predates this gate -- the bytes at a
            // slot's offset belong to a record, and closing that block would write the footer
            // straight through durable data. The window is small, about 128 bytes in 131072, and
            // that is exactly what makes it dangerous: a corruption that rare survives testing
            // and turns up in a log nobody is watching.
            //
            // When the slot is already occupied the block simply does not get a footer. Readers
            // fall back to walking it, which is correct and only unoptimised.
            let slot_is_free = relative <= data_end;
            if relative + bytes.len() as u64 > data_end && slot_is_free {
                // What this block ends up describing: the last record that STARTED in it.
                let (last_offset, last_sequence) = inner
                    .block_last_record_by_shard
                    .get(&record.shard_id)
                    .copied()
                    .unwrap_or((offset, record.sequence.saturating_sub(1)));
                let slot = encode_block_footer(
                    index,
                    (relative.saturating_sub(index * WAL_BLOCK_BYTES)) as u32,
                    last_offset,
                    last_sequence,
                    0,
                );
                close_block_and_advance = Some((header_len + block_footer_at(index), slot));
                offset = header_len + (index + 1) * WAL_BLOCK_BYTES;
            }
            // When the slot is occupied nothing happens at all: no footer, and no advance
            // either. Advancing was the first attempt and it lost every record written
            // afterwards -- skipping the slot leaves a run of zeros, and the ONLY thing that
            // tells a reader records continue past a gap is a footer, which is precisely what
            // an occupied slot cannot have. So this block keeps being written straight through,
            // exactly as it was before blocks existed, and there is no gap to cross. Later
            // blocks, whose slots this log has not reached, close normally.
        }
    }
    let prealloc = wal_preallocate_enabled();
    let mut opened;
    let mut carried_physical: Option<&mut u64> = None;
    let file: &mut std::fs::File = match reuse {
        Some((handle, physical)) => {
            carried_physical = Some(physical);
            handle
        }
        None => {
            #[cfg(test)]
            WAL_FILE_OPENS.with(|opens| opens.set(opens.get() + 1));
            opened = if prealloc {
                // Positioned write, not O_APPEND: with a reservation, the physical end of the
                // file is zeros, and O_APPEND would put this record after them.
                OpenOptions::new().create(true).write(true).open(path.as_path())?
            } else {
                OpenOptions::new().create(true).append(true).open(path.as_path())?
            };
            &mut opened
        }
    };
    if prealloc {
        let needed = offset.saturating_add(bytes.len() as u64);
        // The batch carries the length it already knows. Nothing else can change it while the
        // append lock and the `inner` lock are both held, and the batch updates its copy below
        // whenever it grows the file -- so asking the filesystem again would be a syscall to
        // learn a number already in hand.
        let physical = match carried_physical.as_deref() {
            Some(&known) if known > 0 => known,
            _ => {
                #[cfg(test)]
                WAL_FILE_STATS.with(|stats| stats.set(stats.get() + 1));
                file.metadata()?.len()
            }
        };
        // Carried rather than re-read. The length after this block is either the one just read or
        // the one just set, so asking the filesystem again is a second stat a write to learn a
        // number already in hand.
        let physical = if physical < needed {
            // Grow to a chunk boundary. This is the one place the file's length changes, so it
            // is the one place a barrier still has to persist a size -- once per chunk instead
            // of once per record.
            let chunk = wal_preallocate_chunk();
            let target = needed
                .saturating_add(chunk.saturating_sub(1))
                .saturating_div(chunk)
                .saturating_mul(chunk);
            file.set_len(target)?;
            target
        } else {
            physical
        };
        if let Some(known) = carried_physical.as_deref_mut() {
            *known = physical;
        }
        inner
            .prealloc_physical_by_shard
            .insert(record.shard_id, physical);
        if let Some((at, slot)) = close_block_and_advance.as_ref() {
            file.seek(SeekFrom::Start(*at))?;
            file.write_all(slot)?;
        }
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&bytes)?;
    } else {
        if let Some((at, slot)) = close_block_and_advance.as_ref() {
            // Without preallocation the file is opened for append, so the footer needs its own
            // positioned handle: a footer belongs at a computed offset, never at the end.
            let mut positioned = OpenOptions::new().write(true).open(path.as_path())?;
            positioned.seek(SeekFrom::Start(*at))?;
            positioned.write_all(slot)?;
            positioned.flush()?;
        }
        file.write_all(&bytes)?;
    }
    if inner
        .block_mode_by_shard
        .get(&record.shard_id)
        .copied()
        .unwrap_or(false)
    {
        // Remember what this block will say when it closes.
        inner
            .block_last_record_by_shard
            .insert(record.shard_id, (offset, record.sequence));
    }
    if sync {
        file.flush()?;
        crate::durability_metrics::record_barrier("engine_wal_append");
        file.sync_data()?;
        // The parent-directory entry for the WAL file only needs a durable barrier when
        // the file is first created; appends grow the file (inode) without changing the
        // directory. Under relaxed-sync -- the single-barrier default, with group commit
        // unconditional -- skip the redundant per-append dir fsync once the file already has
        // content (offset > 0).
        // `offset == 0` is the file's first write, which is the usual reason the directory entry
        // is not yet durable. After a roll it is not zero -- the header is already there -- so the
        // debt the roll recorded is what says the entry still has to reach disk.
        let owed = inner.dir_sync_owed_by_shard.contains(&record.shard_id);
        if offset == 0 || owed || !wal_relaxed_dir_sync() {
            sync_parent_dir(&path)?;
            inner.dir_sync_owed_by_shard.remove(&record.shard_id);
        }
        inner.stats.flushes += 1;
        inner.stats.syncs += 1;
        inner.stats.last_flushed_sequence = record.sequence;
    }
    // Ask the handle that was just written, not the path: it is cheaper, and it is the file
    // this record actually went into rather than whatever the name refers to now. Under
    // preallocation the file's length is the reservation, not the records, so the record end is
    // where this write stopped.
    let persistent_bytes = if prealloc {
        offset.saturating_add(bytes.len() as u64)
    } else {
        file.metadata()?.len()
    };
    // The record-end cache advances with the write itself, so a caller that appends in a loop
    // without re-measuring (the atomic batch) still places every record after the previous one.
    inner
        .verified_len_by_shard
        .insert(record.shard_id, persistent_bytes);
    inner.stats.writes += 1;
    inner.stats.bytes_written += bytes.len() as u64;
    inner.stats.persistent_bytes = persistent_bytes;
    if sync {
        // EVERY path that makes a record durable records it per shard, not just flush() and the
        // group-commit barrier: a replayed append syncs here and nowhere else, and a durability
        // figure that misses one sync path understates exactly the writes a follower just took.
        inner
            .durable_active_bytes_by_shard
            .insert(record.shard_id, persistent_bytes);
        let durable_sequence = inner
            .durable_sequence_by_shard
            .entry(record.shard_id)
            .or_default();
        *durable_sequence = (*durable_sequence).max(record.sequence);
    }
    let size = bytes.len() as u64;
    // Give the buffer back with its capacity, which is the whole reason it was borrowed. An early
    // return above simply drops it and the next append allocates once -- correct either way, just
    // not free that once.
    inner.encode_scratch = bytes;
    Ok(WriteAheadLogAppendReport {
        shard_id: record.shard_id,
        requested_sequence: record.sequence,
        current_sequence: record.sequence,
        appended: true,
        offset,
        size,
        persistent_bytes,
    })
}

/// Read the reclaim-base header, returning `(base_offset, header_len_bytes)`.
///
/// A file with no header has never been reclaimed, so nothing has shifted: base 0, header
/// length 0. Every reader skips `header_len` bytes; every address resolves against `base`.
/// Cached `(base, header_len)` for a shard, reading it from disk only the first time.
fn cached_wal_base(
    inner: &mut WriteAheadLogInner,
    shard_id: ShardId,
) -> Result<(u64, u64), WriteAheadLogError> {
    if let Some(cached) = inner.base_by_shard.get(&shard_id) {
        return Ok(*cached);
    }
    let path = active_wal_path(inner, shard_id);
    let base = read_wal_base(&path)?;
    inner.base_by_shard.insert(shard_id, base);
    Ok(base)
}

/// Whether a reclaim pass buys enough space to justify the copy it requires.
///
/// `removed_bytes` is what the pass would free; `retained_bytes` is what it must rewrite to do
/// so. Below `min_copy_bytes` the rewrite is cheap and the ratio is a meaningless measure of a
/// small log, so the pass always proceeds; above it the pass must free at least `min_freed_percent`
/// of what it copies.
fn reclaim_is_worth_rewriting(
    removed_bytes: u64,
    retained_bytes: u64,
    min_copy_bytes: u64,
    min_freed_percent: u32,
) -> bool {
    retained_bytes <= min_copy_bytes
        || removed_bytes.saturating_mul(100)
            >= retained_bytes.saturating_mul(u64::from(min_freed_percent))
}

/// TS_WAL_RECLAIM_MIN_COPY_BYTES: below this much retained data, always reclaim. The rewrite is
/// cheap at that size and the freed fraction is a meaningless measure of a small log.
fn reclaim_min_copy_bytes() -> u64 {
    std::env::var("TS_WAL_RECLAIM_MIN_COPY_BYTES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(64 * 1024 * 1024)
}

/// TS_WAL_RECLAIM_MIN_FREED_PERCENT: above the copy floor, how much a pass must free as a
/// percentage of what it would have to copy before the rewrite is worth running at all.
fn reclaim_min_freed_percent() -> u32 {
    std::env::var("TS_WAL_RECLAIM_MIN_FREED_PERCENT")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .filter(|percent| *percent > 0)
        .unwrap_or(25)
}

fn read_wal_base(path: &Path) -> Result<(u64, u64), WriteAheadLogError> {
    // An absent piece answers (0, 0) rather than failing, and `scan` DEPENDS on that. A piece can
    // be reclaimed between being listed and being read -- reclaim removes whole pieces from the
    // front -- and answering zero here makes the length check in `scan` treat it as ending before
    // any window, so the loop skips it instead of opening a file that is gone.
    //
    // Turning this into an error would surface as shards refusing to load: recovery treats ANY
    // scan failure as data loss, so a piece legitimately reclaimed mid-scan would stop a shard
    // coming up. If this ever has to report absence distinctly, `scan` needs to skip on it
    // explicitly at the same time.
    if !path.exists() {
        return Ok((0, 0));
    }
    let mut reader = BufReader::new(File::open(path)?);
    let mut line = Vec::new();
    let read = reader.read_until(b'\n', &mut line)?;
    if read == 0 {
        return Ok((0, 0));
    }
    match crate::log_framing::decode_base_header(&line)? {
        Some(base) => Ok((base, read as u64)),
        None => Ok((0, 0)),
    }
}

/// The sequence the log last reached, found by reading its END rather than all of it.
///
/// A restart needs this before it can append. Reading and decoding every record made the cost grow
/// with the whole log -- about 83 ms per megabyte, forever -- when the answer is in the last
/// record. This walks backward to the last complete one instead.
///
/// A trailing write with no newline is a torn tail: it is truncated away, exactly as before, and
/// nothing at or below the last complete record is ever cut. A corrupt record in the MIDDLE is no
/// longer reported here, since reaching it meant decoding everything; replay still refuses it, and
/// replay is the path that would act on it.
/// `(last sequence across the log, record end of the ACTIVE piece)`. The active piece's record
/// end is what an appender needs -- it is where the next record goes -- and under preallocation
/// it is the one length the file's own size no longer answers.
fn last_wal_sequence_at(root: &Path, shard_id: ShardId) -> Result<(u64, u64), WriteAheadLogError> {
    // Newest first: the piece being written is empty right after a roll, and an empty piece is not
    // where the log ends. Only the piece being written can have a torn tail -- the sealed ones were
    // whole when they were renamed -- so this repairs that one and reads the rest.
    let mut pieces = wal_segment_paths(root, shard_id);
    pieces.reverse();
    let mut active_record_end = None;
    for piece in pieces {
        let (sequence, record_end) = last_wal_sequence_in(&piece)?;
        let active_record_end = *active_record_end.get_or_insert(record_end);
        if sequence > 0 {
            return Ok((sequence, active_record_end));
        }
    }
    Ok((0, active_record_end.unwrap_or(0)))
}

/// `(last sequence, record end)` for one piece. The record end is the byte offset just past the
/// last complete record -- the file's length, unless a torn tail was repaired or a preallocated
/// zeros reservation follows the records (kept, not truncated: it is not damage, it is room).
/// `(last sequence, record end)` found by walking the records FORWARD from the start.
///
/// The windowed scan below finds a log's last record by searching backward for the final
/// newline. That is exact for records that end in one and wrong for records that do not: a
/// length-framed payload carries 0x0A bytes of its own, so the search lands in the middle of a
/// payload and reports a boundary that was never a boundary. There is no backward equivalent --
/// a length prefix can only be read from in front of the record it describes -- so the frames
/// have to be walked in order.
///
/// The cost is the file rather than its last window, which is why the binary frame is opt-in.

/// Step a walking reader over a block's footer slot when it reaches one.
///
/// Blocks make the record stream discontinuous, which every walker here assumed it was not: a
/// footer sits between the last record of one block and the first of the next, and a reader that
/// meets it sees bytes that are not a frame and concludes the log ended. It does not -- it
/// continues in the next block. Returns the offset the reader should now be at.
///
/// This is the cost of a footer, and it is the reason the reference's readers know about blocks
/// rather than treating a stream as a flat run of records.

/// Whether the block holding `at` has been closed, i.e. its footer slot is written.
///
/// This is what tells zeros apart. A run of zeros inside a CLOSED block is the padding between
/// its last record and its footer -- the records continue in the next block. The same zeros in
/// the OPEN block are the preallocated reservation, and the records really have ended. Without
/// the footer to distinguish them a walker stops at the first block boundary and reports a log
/// a fraction of its real length, which is precisely what it did before this existed.
fn block_is_closed<R: std::io::BufRead + std::io::Seek>(
    reader: &mut R,
    at: u64,
    header_len: u64,
    len: u64,
) -> Result<Option<u64>, WriteAheadLogError> {
    if at < header_len {
        return Ok(None);
    }
    let index = block_of(at - header_len);
    let slot_at = header_len + block_footer_at(index);
    if slot_at + WAL_BLOCK_FOOTER_BYTES > len {
        return Ok(None);
    }
    reader.seek(SeekFrom::Start(slot_at))?;
    let mut slot = vec![0u8; WAL_BLOCK_FOOTER_BYTES as usize];
    if reader.read_exact(&mut slot).is_err() {
        return Ok(None);
    }
    if decode_block_footer(&slot).is_none() {
        return Ok(None);
    }
    let next = header_len + (index + 1) * WAL_BLOCK_BYTES;
    if next >= len {
        return Ok(None);
    }
    reader.seek(SeekFrom::Start(next))?;
    Ok(Some(next))
}

fn skip_block_footer_if_due<R: std::io::BufRead + std::io::Seek>(
    reader: &mut R,
    at: u64,
    header_len: u64,
) -> Result<u64, WriteAheadLogError> {
    if at < header_len {
        return Ok(at);
    }
    let relative = at - header_len;
    let index = block_of(relative);
    if relative < block_data_end(index) {
        return Ok(at);
    }
    // At or past this block's data end -- but that is a statement about POSITION, and a log
    // written before footers existed has records occupying exactly these bytes. Skipping on
    // position alone discards up to 128 bytes of record and resumes mid-record, which decodes
    // as garbage and refuses a load whose bytes were entirely intact: measured on a real
    // store, 92 bytes holding the start of a record were stepped over and the walk resumed
    // on `"key":...`, reported as `invalid type: string "key" ... at line 1 column 5`.
    //
    // So verify the slot before believing it: a written footer carries its magic, and
    // `block_is_closed` already refuses to treat a slot without one as a footer. Reading the
    // slot moves the cursor, so the position it was read from is restored on the way out.
    let slot_at = header_len + block_footer_at(index);
    let resume = reader.stream_position()?;
    let mut slot = vec![0u8; WAL_BLOCK_FOOTER_BYTES as usize];
    reader.seek(SeekFrom::Start(slot_at))?;
    let footer_present =
        reader.read_exact(&mut slot).is_ok() && decode_block_footer(&slot).is_some();
    if !footer_present {
        // Records, not a footer. Carry on reading where we were.
        reader.seek(SeekFrom::Start(resume))?;
        return Ok(at);
    }
    let next = header_len + (index + 1) * WAL_BLOCK_BYTES;
    reader.seek(SeekFrom::Start(next))?;
    Ok(next)
}

fn last_wal_sequence_forward(path: &Path) -> Result<(u64, u64), WriteAheadLogError> {
    let (_, header_len) = read_wal_base(path)?;
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    if len <= header_len {
        return Ok((0, len.min(header_len)));
    }
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(header_len))?;
    let mut last_sequence = 0u64;
    let mut record_end = header_len;
    loop {
        record_end = skip_block_footer_if_due(&mut reader, record_end, header_len)?;
        if record_end >= len {
            break;
        }
        let Some(raw) = read_raw_record(&mut reader)? else {
            // No record here. In a CLOSED block that means padding before its footer and the
            // records go on in the next one; in the open block it means the end.
            match block_is_closed(&mut reader, record_end, header_len, len)? {
                Some(next) => {
                    record_end = next;
                    continue;
                }
                None => break,
            }
        };
        if raw.is_empty() {
            break;
        }
        // A blank line carries nothing and is skipped, exactly as the windowed scan skips it.
        if raw.iter().all(|byte| byte.is_ascii_whitespace()) {
            record_end = record_end.saturating_add(raw.len() as u64);
            continue;
        }
        // A record that will not decode is one of two different things, and conflating them
        // either loses committed data or refuses a load that should succeed. If nothing follows
        // it, the append was interrupted -- a torn tail is not corruption, and everything before
        // it stands. If bytes follow it, a complete record went bad while records around it are
        // fine, which is interior corruption and must stay fatal.
        match decode_wal_line(&raw) {
            Ok(record) => {
                last_sequence = last_sequence.max(record.sequence);
                record_end = record_end.saturating_add(raw.len() as u64);
            }
            Err(err) => {
                let at_end = {
                    let buffered = reader.fill_buf()?;
                    buffered.is_empty() || buffered.iter().all(|byte| *byte == 0)
                };
                if at_end {
                    break;
                }
                return Err(err);
            }
        }
    }
    Ok((last_sequence, record_end))
}


/// Fixed-size blocks, so a footer can be found by ARITHMETIC instead of by searching.
///
/// This is the piece that pays for length framing. A delimited log finds its tail by scanning
/// backward for the last delimiter; a length-framed one cannot, because a length prefix is only
/// readable from in front of the record it describes, so the tail has to be walked to from the
/// start. Walking is correct and costs the file.
///
/// Blocks fix that by making the answer writable in advance: every block ends with a footer
/// naming the last record that started inside it, and because blocks are a fixed size, block N's
/// footer is at a computed offset. Reopening reads the last footer present -- one seek, one small
/// read -- and then walks only the final, still-open block. The walk stops being the file and
/// becomes at most one block.
const WAL_BLOCK_BYTES: u64 = 128 * 1024;

/// Reserved at the end of every block for its footer. The encoded footer is far smaller; the
/// slot is fixed so that the arithmetic above stays arithmetic.
const WAL_BLOCK_FOOTER_BYTES: u64 = 128;

/// Marks a footer slot that has actually been written, so a slot inside preallocated zeros is
/// not mistaken for a footer describing block zero.
const WAL_BLOCK_FOOTER_MAGIC: u64 = 0xB10C_F007_E12A_5EEDu64;

/// Block footers are UNCONDITIONAL.
///
/// Every fixed-size block reserves a footer at its end holding the offset and sequence of the last
/// record that starts in that block. Finding the log tail is then: seek to the final block, read
/// its footer, jump straight to the last record. O(1) in the size of the log.
///
/// This is not an optimisation, it is the prerequisite for the record framing that ships ON.
/// A length-framed record cannot be found by scanning BACKWARD -- there is no sentinel to
/// resynchronise on -- so without a footer the only way to locate the tail is to read FORWARD from
/// the start of the log, and that cost grows with every record ever written.
///
/// Measured, same binary, only the two flags differing, over one run of sustained appends:
///
///     frame on, footer off    643.2 ms p50, degrading 3.83x across the run
///     frame on, footer on     110.2 / 103.1 ms, 0.97x / 1.21x
///     frame off (unframed)     94.5 ms, 1.10x
///
/// The middle row is the shipped configuration. The top row is what shipped when framing was
/// defaulted ON and the footer was left OFF "while it earned trust": neither decision was wrong
/// alone, and together they made the configuration everyone runs the only one with neither fast
/// path. There is deliberately no way to turn the footer off any more.
///
/// Reading stays tolerant: a log written before footers existed has records occupying what would
/// be the footer slots, so the readers below handle a footerless block and fall back to the
/// forward scan for it. `turning_blocks_on_over_a_log_written_without_them` and
/// `blocks_turned_on_over_a_log_that_ends_inside_a_slot` cover both transitions.
fn block_footer_at(index: u64) -> u64 {
    index * WAL_BLOCK_BYTES + (WAL_BLOCK_BYTES - WAL_BLOCK_FOOTER_BYTES)
}

/// The last byte a record may occupy in block `index`, relative to the start of the records.
fn block_data_end(index: u64) -> u64 {
    block_footer_at(index)
}

fn block_of(relative_offset: u64) -> u64 {
    relative_offset / WAL_BLOCK_BYTES
}

/// Encode a footer into its fixed-size slot, zero-padded.
fn encode_block_footer(
    index: u64,
    block_end: u32,
    last_record_offset: u64,
    last_record_sequence: u64,
    block_crc: u32,
) -> Vec<u8> {
    use prost::Message;
    let footer = crate::storage_descriptor::BlockFooter {
        magic: WAL_BLOCK_FOOTER_MAGIC,
        version: WRITE_AHEAD_LOG_FORMAT_VERSION,
        timestamp_ms: current_time_ms(),
        block_crc,
        block_number: index,
        block_end,
        last_record_offset,
        // Records never straddle a block here: one that does not fit starts the next block
        // instead. That is what keeps this field zero and the reader free of the case.
        last_record_left_size: 0,
        last_record_sequence,
        client_token: Vec::new(),
        truncated_offset: 0,
    };
    let message = footer.encode_to_vec();
    assert!(
        message.len() as u64 + 2 <= WAL_BLOCK_FOOTER_BYTES,
        "a block footer must fit its slot: {} + 2 > {}",
        message.len(),
        WAL_BLOCK_FOOTER_BYTES
    );
    // The slot says how long the message is, because the slot is a FIXED size and the message is
    // not. Padding the message out to the slot and handing the whole thing to a decoder does not
    // work: a trailing zero byte reads as field number 0, which is not a legal field, so every
    // padded footer fails to decode and reads as absent. That failure is silent and total -- the
    // footers were being written correctly and nothing could see any of them.
    let mut slot = Vec::with_capacity(WAL_BLOCK_FOOTER_BYTES as usize);
    slot.extend_from_slice(&(message.len() as u16).to_le_bytes());
    slot.extend_from_slice(&message);
    slot.resize(WAL_BLOCK_FOOTER_BYTES as usize, 0);
    slot
}

/// Read a footer out of its slot. `None` for a slot that was never written -- which is what a
/// preallocated block looks like, and what a crash before the footer leaves.
fn decode_block_footer(slot: &[u8]) -> Option<crate::storage_descriptor::BlockFooter> {
    use prost::Message;
    if slot.len() < 2 || slot.iter().all(|byte| *byte == 0) {
        return None;
    }
    let declared = u16::from_le_bytes([slot[0], slot[1]]) as usize;
    if declared == 0 || 2 + declared > slot.len() {
        return None;
    }
    let footer = crate::storage_descriptor::BlockFooter::decode(&slot[2..2 + declared]).ok()?;
    if footer.magic != WAL_BLOCK_FOOTER_MAGIC {
        return None;
    }
    Some(footer)
}

/// The last footer this file holds, if any: `(block index, footer)`.
///
/// Reads backwards through the footer SLOTS, which is not the same as scanning backwards through
/// the file -- each slot is at a computed offset, so this is a handful of small reads whatever
/// the log's size, and it stops at the first one that was written.
fn last_written_footer(
    file: &File,
    header_len: u64,
    file_len: u64,
) -> Result<Option<(u64, crate::storage_descriptor::BlockFooter)>, WriteAheadLogError> {
    if file_len <= header_len {
        return Ok(None);
    }
    let data_len = file_len - header_len;
    let mut index = block_of(data_len.saturating_sub(1));
    loop {
        let at = header_len + block_footer_at(index);
        if at + WAL_BLOCK_FOOTER_BYTES <= file_len {
            let mut slot = vec![0u8; WAL_BLOCK_FOOTER_BYTES as usize];
            let mut reader = BufReader::new(file.try_clone()?);
            reader.seek(SeekFrom::Start(at))?;
            if reader.read_exact(&mut slot).is_ok() {
                if let Some(footer) = decode_block_footer(&slot) {
                    return Ok(Some((index, footer)));
                }
            }
        }
        if index == 0 {
            return Ok(None);
        }
        index -= 1;
    }
}


/// What the last written footer knows: the highest sequence it covers, and the offset the still
/// open block begins at. `None` when no block has closed yet -- a young log, or one whose first
/// block is still filling.
fn footer_tail_hint(path: &Path) -> Result<Option<(u64, u64)>, WriteAheadLogError> {
    let (_, header_len) = read_wal_base(path)?;
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let Some((index, footer)) = last_written_footer(&file, header_len, file_len)? else {
        return Ok(None);
    };
    Ok(Some((
        footer.last_record_sequence,
        header_len + (index + 1) * WAL_BLOCK_BYTES,
    )))
}

/// The forward walk, started from a position a footer vouched for rather than from the top.
fn last_wal_sequence_forward_from(
    path: &Path,
    from: u64,
    known_sequence: u64,
) -> Result<(u64, u64), WriteAheadLogError> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    if from >= len {
        return Ok((known_sequence, from.min(len)));
    }
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(from))?;
    let mut last_sequence = known_sequence;
    let mut record_end = from;
    let (_, header_len) = read_wal_base(path)?;
    loop {
        record_end = skip_block_footer_if_due(&mut reader, record_end, header_len)?;
        if record_end >= len {
            break;
        }
        let Some(raw) = read_raw_record(&mut reader)? else {
            // No record here. In a CLOSED block that means padding before its footer and the
            // records go on in the next one; in the open block it means the end.
            match block_is_closed(&mut reader, record_end, header_len, len)? {
                Some(next) => {
                    record_end = next;
                    continue;
                }
                None => break,
            }
        };
        if raw.is_empty() {
            break;
        }
        if raw.iter().all(|byte| byte.is_ascii_whitespace()) {
            record_end = record_end.saturating_add(raw.len() as u64);
            continue;
        }
        match decode_wal_line(&raw) {
            Ok(record) => {
                last_sequence = last_sequence.max(record.sequence);
                record_end = record_end.saturating_add(raw.len() as u64);
            }
            Err(err) => {
                let at_end = {
                    let buffered = reader.fill_buf()?;
                    buffered.is_empty() || buffered.iter().all(|byte| *byte == 0)
                };
                if at_end {
                    break;
                }
                return Err(err);
            }
        }
    }
    Ok((last_sequence, record_end))
}

/// Where the records of one log end, for measurements that need bytes rather than counts.
pub fn last_wal_sequence_in_for_test(path: &Path) -> Result<(u64, u64), WriteAheadLogError> {
    last_wal_sequence_in(path)
}

fn last_wal_sequence_in(path: &Path) -> Result<(u64, u64), WriteAheadLogError> {
    if !path.exists() {
        return Ok((0, 0));
    }
    if crate::log_framing::binary_frame_enabled() {
        // The footer says where to start, so the walk covers the open block instead of the log.
        // Without one -- a log written before blocks, or an empty one -- the walk starts at the
        // header and covers everything, which is the same answer for more reading.
        if let Some((sequence, from)) = footer_tail_hint(path)? {
            return last_wal_sequence_forward_from(path, from, sequence);
        }
        return last_wal_sequence_forward(path);
    }
    let (_, header_len) = read_wal_base(path)?;
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let len = file.metadata()?.len();
    if len <= header_len {
        return Ok((0, len.min(header_len)));
    }

    // Pull in the tail, growing the window until it holds a whole record. Records are small and
    // the first window almost always suffices; the loop is for the ones that are not.
    let mut window = 64 * 1024u64;
    let (line, good_offset, tail_is_reservation) = loop {
        let window_start = header_len.max(len.saturating_sub(window));
        let mut reader = BufReader::new(file.try_clone()?);
        reader.seek(SeekFrom::Start(window_start))?;
        let mut data = vec![0u8; (len - window_start) as usize];
        reader.read_exact(&mut data)?;

        // Everything after the final newline was never finished being written.
        let Some(last_newline) = data.iter().rposition(|byte| *byte == b'\n') else {
            if window_start == header_len {
                // Not one complete record in the file: the whole body is either a torn write or
                // an untouched preallocated reservation.
                let zeros = data.iter().all(|byte| *byte == 0);
                break (None, header_len, zeros);
            }
            window = window.saturating_mul(4);
            continue;
        };
        let good_offset = window_start + last_newline as u64 + 1;
        // Whatever follows the last complete record: all zeros is a preallocated reservation
        // (kept -- it is room, not damage); anything else is a torn write (repaired as always).
        let tail_is_reservation = data[last_newline + 1..].iter().all(|byte| *byte == 0)
            && last_newline as u64 + 1 < (len - window_start);

        // Walk back over any blank lines to the last record that actually says something.
        let mut line_end = last_newline;
        loop {
            let line_start = data[..line_end]
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map(|index| index + 1);
            let (line_start, reached_the_front) = match line_start {
                Some(index) => (index, false),
                None => (0usize, true),
            };
            let candidate = &data[line_start..line_end];
            if !candidate.iter().all(|byte| byte.is_ascii_whitespace()) {
                break;
            }
            if line_start == 0 {
                if reached_the_front && window_start > header_len {
                    // The blank run may continue below this window.
                    line_end = usize::MAX;
                    break;
                }
                // Blank all the way back to the start: no record in this log.
                return Ok((0, header_len));
            }
            line_end = line_start - 1;
        }
        if line_end == usize::MAX {
            window = window.saturating_mul(4);
            continue;
        }

        let line_start = data[..line_end]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map(|index| index + 1);
        match line_start {
            Some(index) => break (Some(data[index..line_end].to_vec()), good_offset, tail_is_reservation),
            None if window_start == header_len => {
                break (Some(data[..line_end].to_vec()), good_offset, tail_is_reservation)
            }
            // The record starts before the window: widen and look again.
            None => window = window.saturating_mul(4),
        }
    };

    if good_offset < len && !tail_is_reservation {
        file.set_len(good_offset)?;
        file.sync_all()?;
        sync_parent_dir(path)?;
    }
    let Some(line) = line else {
        return Ok((0, good_offset));
    };
    Ok((decode_wal_line(&line)?.sequence, good_offset))
}

/// Test-only override for the wall clock stamped onto a record. Recovery tests need records
/// whose leader timestamps are OLD relative to restart, and the only honest way to get that
/// is to state the timestamp: the alternative is sleeping for the difference and racing every
/// other thread on the machine for it.
///
/// Process-wide rather than per-thread, and that is the whole point. A write does not promise
/// to append on the thread that executed it -- group commit exists precisely so it does not --
/// so a per-thread pin was applied or skipped depending on where the append happened to land,
/// which made the record's timestamp real often enough to fail one run in twenty-five. Zero
/// means unset; callers pin it only inside a guard that restores it.
#[cfg(test)]
static TEST_RECORD_CLOCK_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn set_test_record_clock_ms(clock_ms: Option<u64>) {
    TEST_RECORD_CLOCK_MS.store(
        clock_ms.unwrap_or(0),
        std::sync::atomic::Ordering::SeqCst,
    );
}

fn current_time_ms() -> u64 {
    #[cfg(test)]
    {
        let pinned = TEST_RECORD_CLOCK_MS.load(std::sync::atomic::Ordering::SeqCst);
        if pinned != 0 {
            return pinned;
        }
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn command_variant_name(command: &Command) -> Option<String> {
    let value = serde_json::to_value(command).ok()?;
    let object = value.as_object()?;
    object
        .get("kind")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
}

fn command_payload(command: &Command) -> Option<serde_json::Map<String, serde_json::Value>> {
    let value = serde_json::to_value(command).ok()?;
    let object = value.as_object()?;
    let mut payload = object.clone();
    payload.remove("kind");
    Some(payload)
}

fn command_object_key(command: &Command) -> Option<String> {
    let payload = command_payload(command)?;
    if let Some(key) = payload.get("key").and_then(|value| value.as_str()) {
        return Some(key.to_string());
    }
    let tenant = payload.get("tenant_hash").and_then(|value| value.as_u64());
    let node = payload
        .get("node_hash")
        .or_else(|| payload.get("parent_hash"))
        .or_else(|| payload.get("start_node_hash"))
        .and_then(|value| value.as_u64());
    match (tenant, node) {
        (Some(tenant), Some(node)) => Some(format!("context:{tenant}:{node}")),
        (Some(tenant), None) => Some(format!("context:{tenant}")),
        _ => None,
    }
}

fn command_ttl_ms(command: &Command) -> Option<u64> {
    let payload = command_payload(command)?;
    payload.get("ttl_ms").and_then(|value| value.as_u64())
}

fn command_bucket_id(key: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash & 0x3fff
}

fn command_model(command: &Command) -> WriteAheadLogModel {
    let Some(name) = command_variant_name(command) else {
        return WriteAheadLogModel::Unknown;
    };
    if name.starts_with("common_") {
        WriteAheadLogModel::Common
    } else if name.starts_with("string_") {
        WriteAheadLogModel::String
    } else if name.starts_with("hash_") {
        WriteAheadLogModel::Hash
    } else if name.starts_with("set_") {
        WriteAheadLogModel::Set
    } else if name.starts_with("feature_") {
        WriteAheadLogModel::Feature
    } else if name.starts_with("sequence_") {
        WriteAheadLogModel::Sequence
    } else if name.starts_with("control_state_") {
        WriteAheadLogModel::ControlState
    } else if name.starts_with("context_") {
        WriteAheadLogModel::Context
    } else {
        WriteAheadLogModel::Unknown
    }
}

fn command_item_kind(command: &Command) -> WriteAheadLogItemKind {
    let Some(name) = command_variant_name(command) else {
        return WriteAheadLogItemKind::Admin;
    };
    if name.contains("delete") || name.contains("remove") {
        WriteAheadLogItemKind::DeleteObject
    } else if name.contains("expire") || name.ends_with("ttl") {
        WriteAheadLogItemKind::Ttl
    } else if name.contains("get")
        || name.contains("query")
        || name.contains("count")
        || name.contains("stat")
        || name.contains("debug")
        || name.contains("manager")
        || name.contains("members")
        || name.contains("exists")
    {
        WriteAheadLogItemKind::Query
    } else {
        WriteAheadLogItemKind::Kv
    }
}

fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            crate::durability_metrics::record_barrier("engine_wal_dir");
            dir.sync_all()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    /// eight records against one record holding eight items, on identical outcomes.
    ///
    /// An ingest writes eight things. Today that is eight records, each paying a frame, a shard
    /// id, a sequence, a timestamp and — in a batch — three more fields of batch bookkeeping so
    /// the group can be recovered as a unit. The record format already carries `repeated items`,
    /// so the same eight outcomes fit in one record that is atomic by construction and needs no
    /// commit marker at all.
    ///
    /// This encodes both shapes from the same items and compares them, which says what the change
    /// is worth before anything is rebuilt to get it.
    #[test]
    fn eight_records_against_one_record_holding_eight() {
        fn item(index: usize) -> WalOutcomeItem {
            WalOutcomeItem {
                kind: "string".to_string(),
                object_key: format!("ingest-00042/{index}"),
                component: None,
                object_id: 1000 + index as u64,
                routing_bucket: 7,
                address: None,
                value: Some(vec![b'v'; 96]),
                ttl: None,
                deleted: false,
                meta: false,
            }
        }

        // Shape one: eight records, one item each, as a batch (so batch metadata is counted).
        let mut separate = 0usize;
        for index in 0..8 {
            let record = WriteAheadLogRecord {
                shard_id: 1,
                sequence: 100 + index as u64,
                command: None,
                metadata: Some(WriteAheadLogRecordMetadata {
                    version: WRITE_AHEAD_LOG_FORMAT_VERSION,
                    timestamp_ms: 1_787_270_070_192,
                    items: Vec::new(),
                    batch_id: Some(9),
                    batch_size: Some(8),
                    batch_index: Some(index as u32 + 1),
                }),
                staged_pages: Vec::new(),
                outcomes: vec![item(index)],
            };
            separate += crate::log_framing::encode_record(&encode_wal_payload(&record).unwrap()).len();
        }

        // Shape two: one record carrying all eight. Atomic because it is one record.
        let record = WriteAheadLogRecord {
            shard_id: 1,
            sequence: 100,
            command: None,
            metadata: Some(WriteAheadLogRecordMetadata {
                version: WRITE_AHEAD_LOG_FORMAT_VERSION,
                timestamp_ms: 1_787_270_070_192,
                items: Vec::new(),
                batch_id: None,
                batch_size: None,
                batch_index: None,
            }),
            staged_pages: Vec::new(),
            outcomes: (0..8).map(item).collect(),
        };
        let together = crate::log_framing::encode_record(&encode_wal_payload(&record).unwrap()).len();

        println!(
            "  eight records   {separate:>6} B\n  \
             one record      {together:>6} B\n  \
             saving          {:>6} B   {:.1}%",
            separate.saturating_sub(together),
            100.0 * (separate as f64 - together as f64) / separate as f64,
        );
        // And it must still decode to the same eight outcomes.
        let framed = crate::log_framing::encode_record(&encode_wal_payload(&record).unwrap());
        let back = decode_wal_line(&framed).unwrap();
        assert_eq!(back.outcomes.len(), 8, "one record must carry all eight");
        assert!(
            together < separate,
            "one record should not cost more than eight: {together} against {separate}"
        );
    }

    /// Resident bytes this process holds, from the kernel rather than from a guess.
    fn resident_bytes() -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: u64 = rest
                    .trim()
                    .trim_end_matches(" kB")
                    .trim()
                    .parse()
                    .unwrap_or(0);
                return kb * 1024;
            }
        }
        0
    }

    /// what the log holds in memory, and whether it grows with the LOG or with the SHARDS.
    ///
    /// The claim this exists to check is one I had been repeating from reading the struct: every
    /// map the log retains is keyed by shard, so memory is bounded by shard count and not by how
    /// much has been written. That is an argument. This is the measurement.
    ///
    /// Ignored: it allocates thousands of shards and reads RSS, which is a process-wide number
    /// and useless beside other tests.
    ///
    ///   cargo test -p temporalstore-rust --lib what_the_log_holds_in_memory -- --ignored --nocapture
    #[test]
    #[ignore]
    fn what_the_log_holds_in_memory() {
        // Some(0) is "never roll"; None takes the default, which rolls.
        set_wal_segment_bytes_for_test(Some(0));
        const SHARDS: u64 = 1_000;

        // A: one record per shard. Whatever the log keeps per shard is now resident.
        let dir_a = tempfile::tempdir().unwrap();
        let before_a = resident_bytes();
        let store_a = LocalWriteAheadLogStore::new(dir_a.path());
        for shard in 1..=SHARDS {
            store_a
                .append_with_sync(
                    shard,
                    Command::StringSet {
                        key: format!("k{shard}"),
                        value: vec![b'v'; 128],
                    },
                    false,
                )
                .unwrap();
        }
        let after_a = resident_bytes();
        let per_shard = (after_a.saturating_sub(before_a)) as f64 / SHARDS as f64;

        // B: the same shards, a hundred times the records. If memory tracks the LOG this grows a
        // hundredfold; if it tracks the SHARDS it does not move.
        let dir_b = tempfile::tempdir().unwrap();
        let before_b = resident_bytes();
        let store_b = LocalWriteAheadLogStore::new(dir_b.path());
        for shard in 1..=SHARDS {
            for index in 0..100 {
                store_b
                    .append_with_sync(
                        shard,
                        Command::StringSet {
                            key: format!("k{shard}-{index}"),
                            value: vec![b'v'; 128],
                        },
                        false,
                    )
                    .unwrap();
            }
        }
        let after_b = resident_bytes();
        let per_shard_deep = (after_b.saturating_sub(before_b)) as f64 / SHARDS as f64;

        println!(
            "  {SHARDS} shards, 1 record each     resident +{:>9} B   {:>8.0} B/shard\n  \
             {SHARDS} shards, 100 records each   resident +{:>9} B   {:>8.0} B/shard\n  \
             records written: {} against {}   ratio of per-shard cost: {:.2}x",
            after_a.saturating_sub(before_a),
            per_shard,
            after_b.saturating_sub(before_b),
            per_shard_deep,
            SHARDS,
            SHARDS * 100,
            per_shard_deep / per_shard.max(1.0),
        );
        // A hundredfold more log for the same shards must not cost a hundredfold more memory.
        // Ten is a wide bar on purpose: RSS is a high-water mark and the allocator keeps what it
        // has taken, so a strict bound would be measuring malloc rather than the log.
        assert!(
            per_shard_deep < per_shard * 10.0 + 4096.0,
            "memory looks like it tracks the log rather than the shards: {per_shard:.0} B/shard \
             at one record, {per_shard_deep:.0} B/shard at a hundred"
        );
        drop(store_a);
        drop(store_b);
    }

    /// what a byte of value costs in the log, across payload sizes.
    ///
    /// Ignored: a measurement. The ratio is meaningless without its payload size -- the frame is
    /// fixed and the key is per record, so both dilute as the value grows. Quoting one number
    /// for "amplification" is quoting a coincidence, which is why this prints a table.
    ///
    ///   cargo test -p temporalstore-rust --lib what_a_byte_of_value_costs -- --ignored --nocapture
    #[test]
    #[ignore]
    fn what_a_byte_of_value_costs() {
        // Some(0) is "never roll"; None takes the default, which rolls and would split the bytes
        // this measures across pieces it does not stat.
        set_wal_segment_bytes_for_test(Some(0));
        println!("  value     records      payload        wal bytes    per record   ratio");
        for value_bytes in [64usize, 256, 1024, 4096, 16384] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            let records = 400usize;
            for index in 0..records {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("tenant/7/object/{index:06}"),
                            value: vec![b'v'; value_bytes],
                        },
                        false,
                    )
                    .unwrap();
            }
            let path = write_ahead_log_path(dir.path(), 1);
            let (_, record_end) = last_wal_sequence_in(&path).unwrap();
            let (_, header_len) = read_wal_base(&path).unwrap();
            let wal_bytes = record_end.saturating_sub(header_len);
            let payload = (records * value_bytes) as u64;
            println!(
                "  {value_bytes:>5}   {records:>7}   {payload:>10}   {wal_bytes:>12}   \
                 {:>10.1}   {:>5.2}x",
                wal_bytes as f64 / records as f64,
                wal_bytes as f64 / payload as f64,
            );
        }
        // And what the file COSTS, which is not the same as what the records occupy:
        // preallocation reserves ahead, and that reservation is real disk.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in 0..400 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![b'v'; 1024],
                    },
                    false,
                )
                .unwrap();
        }
        let path = write_ahead_log_path(dir.path(), 1);
        let (_, record_end) = last_wal_sequence_in(&path).unwrap();
        let file_len = std::fs::metadata(&path).unwrap().len();
        println!(
            "\n  records occupy {record_end} bytes; the file is {file_len} bytes ({:.2}x), the \
             difference being the reservation",
            file_len as f64 / record_end.max(1) as f64
        );
    }

    /// The narrow case: a log written WITHOUT blocks whose records end INSIDE where a footer
    /// slot would go. About 128 bytes in 131072 of log lengths land here.
    ///
    /// IGNORED, and honestly rather than quietly. Turning blocks on over an existing log is not
    /// a supported transition: block layout is a property a log is born with, which is what
    /// `shard_uses_blocks` decides. This case says that decision is not yet holding -- the 50
    /// records written after the switch land past a gap the readers cannot cross, and four
    /// attempts (guarding the slot, not advancing, deciding per log, pinning the rolling
    /// threshold) each produced the SAME numbers, which means none of them was the cause.
    ///
    /// Identical numbers across four different fixes is the finding: the thing being changed is
    /// not the thing deciding the outcome. What is known: the writer puts the records at 131072
    /// leaving 130971..131072 unparseable, and only the footer-hint path finds them afterwards.
    /// Left here as a red flag rather than deleted, because the day this is defaulted is the day
    /// it has to be answered.
    /// Was ignored while the footer was a flag: the transition was unsupported and the guard did
    /// not exist. The footer is unconditional now, this passes, and it is the regression test for
    /// the case the comment above said had to be answered "the day this is defaulted".
    #[test]
    fn blocks_turned_on_over_a_log_that_ends_inside_a_slot() {
        let dir = tempfile::tempdir().unwrap();
        // Some(0) is "never roll"; None takes the default, which rolls.
        set_wal_segment_bytes_for_test(Some(0));
        let store = LocalWriteAheadLogStore::new(dir.path());
        let path = write_ahead_log_path(dir.path(), 1);
        // Fill until the records end inside block 0's footer slot.
        let mut written = 0usize;
        loop {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{written:05}"),
                        value: vec![b'v'; 64],
                    },
                    false,
                )
                .unwrap();
            written += 1;
            let (_, end) = last_wal_sequence_in(&path).unwrap();
            if end > block_footer_at(0) {
                break;
            }
            assert!(written < 20_000, "never reached the slot");
        }
        let (_, end_before) = last_wal_sequence_in(&path).unwrap();
        assert!(
            end_before > block_footer_at(0),
            "the log must end past the slot start for this to be the case under test"
        );
        drop(store);

        // Now turn blocks on and keep writing.
        // rolling is off for this test. The segment threshold is a THREAD-LOCAL override and
        // these tests share a thread, so whatever the previous test left is inherited -- and a
        // log that rolls mid-test splits across files the assertions never look at.
        //
        // `Some(0)`, not `None`: None clears the override and takes the DEFAULT, and the default
        // rolls now. These assertions are about blocks within one piece, so they say "off"
        // explicitly instead of relying on what the default happens to be.
        set_wal_segment_bytes_for_test(Some(0));
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in written..written + 50 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:05}"),
                        value: vec![b'v'; 64],
                    },
                    false,
                )
                .unwrap();
        }
        let (_, end_after) = last_wal_sequence_in(&path).unwrap();
        let after = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        // walk the file by hand, with no store state involved at all, so the two readers can be
        // compared on the same bytes. Whichever disagrees with this one is the one at fault.
        let raw = std::fs::read(&path).unwrap();
        let mut by_hand = 0usize;
        let mut at = 0usize;
        while at < raw.len() {
            if raw[at] == 0 {
                break;
            }
            match crate::log_framing::next_frame(&raw[at..]) {
                Ok(Some((consumed, _))) if consumed > 0 => {
                    by_hand += 1;
                    at += consumed;
                }
                _ => break,
            }
        }
        println!("  by hand: {by_hand} records, stopping at {at}");
        // narrow-case diagnostic: say what moved, because "50 records missing" does not
        // distinguish a writer that put them somewhere unreadable from a reader that stops early.
        println!(
            "  wrote {written} before, then 50 more\n  \
             records end: {end_before} -> {end_after}  (slot at {})\n  \
             scan returned {}",
            block_footer_at(0),
            after.len()
        );
        assert_eq!(
            after.len(),
            written + 50,
            "records written before blocks were turned on must survive turning them on"
        );
        for (_, line) in &after {
            decode_wal_line(line).expect("no record may be written through");
        }
    }

    /// Turning blocks on over a log that was written without them.
    ///
    /// The footer slot is at a computed offset, so in a log written WITHOUT block reservations
    /// those bytes belong to a record. Writing a footer there would land in the middle of one.
    /// This is the case that decides whether the gate can be defaulted or has to be a property
    /// of the log, and it is worth knowing before flipping it rather than after.
    #[test]
    fn turning_blocks_on_over_a_log_written_without_them() {
        // Off for BOTH phases. This test writes before it configures anything, and the phase-one
        // log has to stay one file for the boundary check below to mean what it says -- pinning
        // only the second phase left the first rolling on the default.
        set_wal_segment_bytes_for_test(Some(0));
        let dir = tempfile::tempdir().unwrap();
        // Phase one: no blocks. Records run straight through where slots would be.
        let written = {
            let store = LocalWriteAheadLogStore::new(dir.path());
            for index in 0..600 {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("k{index:05}"),
                            value: vec![b'v'; 1024],
                        },
                        false,
                    )
                    .unwrap();
            }
            store.scan(1, 0, u64::MAX, u64::MAX).unwrap().len()
        };
        let path = write_ahead_log_path(dir.path(), 1);
        let (_, end_before) = last_wal_sequence_in(&path).unwrap();
        assert!(
            end_before > WAL_BLOCK_BYTES,
            "the log must already run past a block boundary for this to mean anything: \
             {end_before}"
        );

        // Phase two: blocks on, same log, more records.
        // rolling is off for this test. The segment threshold is a THREAD-LOCAL override and
        // these tests share a thread, so whatever the previous test left is inherited -- and a
        // log that rolls mid-test splits across files the assertions never look at.
        //
        // `Some(0)`, not `None`: None clears the override and takes the DEFAULT, and the default
        // rolls now. These assertions are about blocks within one piece, so they say "off"
        // explicitly instead of relying on what the default happens to be.
        set_wal_segment_bytes_for_test(Some(0));
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in 600..700 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:05}"),
                        value: vec![b'v'; 1024],
                    },
                    false,
                )
                .unwrap();
        }
        let after = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(
            after.len(),
            written + 100,
            "every record written before the switch must still be readable after it"
        );
        for (_, line) in &after {
            decode_wal_line(line).expect("every record must still decode");
        }
    }

    /// Durable bytes never exceed the bytes the log holds, across a roll.
    ///
    /// `persistent_length_bytes` is "the sealed pieces, plus how far a barrier reached into the
    /// active one". The second half is remembered per shard. A roll replaces the active piece, so
    /// a value left behind describes a piece that is now counted from the filesystem as a sealed
    /// one -- counted twice.
    ///
    /// The append that rolls usually repairs this by syncing straight afterwards and overwriting
    /// the entry. This drives the case it does not: a barrier lands, THEN a roll happens on an
    /// append that does not sync. One of the three writers of this entry takes a `.max()` rather
    /// than overwriting, so once the value is stale-high it cannot come back down that way.
    ///
    /// Overstating is the direction this field exists to rule out: it claims unsynced records are
    /// on disk to survive a crash. Reporting more durable bytes than the log physically holds is
    /// the plainest form of that.
    #[test]
    fn durable_bytes_never_exceed_the_bytes_the_log_holds() {
        let segment = 2 * WAL_BLOCK_BYTES;
        set_wal_segment_bytes_for_test(Some(segment));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        let sealed_count = || {
            std::fs::read_dir(dir.path())
                .unwrap()
                .flatten()
                .filter(|entry| {
                    // A sealed piece, not the active one and not the append LOCK file --
                    // `shard-1.wal.lock` also contains "wal." and counted as a sealed piece,
                    // which made this read as rolled before anything had rolled.
                    let name = entry.file_name().to_string_lossy().into_owned();
                    name.starts_with("shard-1.wal.")
                        && name.ends_with(".jsonl")
                        && name != "shard-1.wal.jsonl"
                })
                .count()
        };
        let append = |index: usize, sync: bool| {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![b'v'; 1024],
                    },
                    sync,
                )
                .unwrap();
        };

        // Fill the ACTIVE piece to a little over half the roll threshold, then put a barrier on
        // it. `info().length_bytes` is the whole log, pieces included, so it is the wrong ruler
        // here -- reading it as the active piece's length ran this loop straight past the roll.
        let active = write_ahead_log_path(dir.path(), 1);
        let active_len = || active.metadata().map(|meta| meta.len()).unwrap_or(0);
        let mut index = 0usize;
        while active_len() < segment / 2 {
            append(index, false);
            index += 1;
            assert!(index < 40_000, "the piece must fill within a bounded number of appends");
        }
        store.flush(1).unwrap();
        let before = store.info(1).unwrap();
        assert!(
            before.persistent_length_bytes > 0,
            "the barrier must have recorded a durable extent for this to be the case that matters"
        );
        assert_eq!(sealed_count(), 0, "nothing should have rolled yet");

        // Now roll, on appends that do not ask for a barrier of their own.
        while sealed_count() == 0 {
            append(index, false);
            index += 1;
            assert!(index < 40_000, "the log must roll within a bounded number of appends");
        }

        let info = store.info(1).unwrap();
        assert!(
            info.persistent_length_bytes <= info.length_bytes,
            "durable bytes ({}) must not exceed what the log holds ({}) -- the piece that just \
             sealed is counted both as a sealed piece and as the active piece's durable extent",
            info.persistent_length_bytes,
            info.length_bytes
        );
        set_wal_segment_bytes_for_test(None);
    }

    /// A piece rolled into gets blocks, even when the piece before it had none.
    ///
    /// Whether a log is written in blocks is decided per PIECE and cached per SHARD. Nothing
    /// invalidated that cache when the log rolled, so a shard that latched "no blocks" -- which is
    /// what happens whenever a process first appends to a piece that already has records in it,
    /// i.e. after any restart -- kept the answer for every piece it rolled into afterwards.
    ///
    /// This is not hypothetical. On the serving store the deployed binary carries the footer
    /// writer and the log rolls steadily, and the footer magic appears zero times in every piece
    /// of the whole store. Without a footer a reader cannot find the tail from one block and walks
    /// the log instead.
    #[test]
    fn a_rolled_piece_is_written_in_blocks_even_when_the_one_before_it_was_not() {
        // Big enough that a piece holds more than one 128 KiB block before it rolls, so a block
        // can actually close and write its footer.
        let segment = 6 * WAL_BLOCK_BYTES;
        set_wal_segment_bytes_for_test(Some(segment));
        let dir = tempfile::tempdir().unwrap();
        let path = write_ahead_log_path(dir.path(), 1);

        let append = |store: &LocalWriteAheadLogStore, index: usize| {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![b'v'; 1024],
                    },
                    false,
                )
                .unwrap();
        };

        // Phase one leaves the piece with records in it, and then goes away. This is the state a
        // restart finds: an active piece that is not empty.
        {
            let store = LocalWriteAheadLogStore::new(dir.path());
            for index in 0..40 {
                append(&store, index);
            }
        }
        assert!(
            path.metadata().unwrap().len() > 0,
            "the piece must be non-empty for this to be the case that matters"
        );

        // Phase two is a fresh process over that piece: it decides "no blocks", correctly, for
        // THIS piece -- and then rolls into new ones.
        let store = LocalWriteAheadLogStore::new(dir.path());
        let mut index = 40usize;
        let mut rolled = 0usize;
        let sealed_now = || {
            std::fs::read_dir(dir.path())
                .unwrap()
                .flatten()
                .filter(|entry| {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    name.contains("wal.") && name != "shard-1.wal.jsonl"
                })
                .count()
        };
        let before = sealed_now();
        // Enough to roll at least twice and then fill more than a block of the newest piece.
        while rolled < 2 || index < 40 + (segment / 1024) as usize * 3 {
            append(&store, index);
            index += 1;
            rolled = sealed_now().saturating_sub(before);
            assert!(index < 40_000, "the log must roll within a bounded number of appends");
        }

        let active_len = path.metadata().unwrap().len();
        assert!(
            active_len > WAL_BLOCK_BYTES,
            "the piece rolled into must hold more than one block for a footer to be due: \
             {active_len}"
        );

        let file = File::open(&path).unwrap();
        let (_, header_len) = read_wal_base(&path).unwrap();
        let footer = last_written_footer(&file, header_len, active_len).unwrap();
        assert!(
            footer.is_some(),
            "a piece the log rolled into starts empty, so it is written in blocks and its first \
             closed block carries a footer -- found none in {active_len} bytes"
        );

        // And the records are all still there, which is what the footer is in service of.
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert!(
            scanned.len() >= index - 40,
            "every record written to the rolled pieces must still read back: {} of {}",
            scanned.len(),
            index - 40
        );
        for (_, line) in &scanned {
            decode_wal_line(line).expect("every record must still decode");
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// A record that ENDS inside a block's footer slot, in a log written without footers.
    ///
    /// Reading is documented to tolerate such a log -- "records occupying what would be the
    /// footer slots" -- and the two transition tests above cover a record that STARTS in a slot.
    /// Neither covers one that ends there, and that is the case the tail walk got wrong: it
    /// skipped to the next block on POSITION alone, without checking whether the slot held a
    /// footer at all, discarding up to 128 bytes of record and resuming mid-record. On a real
    /// store that skipped 92 bytes holding the start of a record and resumed on `"key":...`,
    /// which the decoder reported as `invalid type: string "key" ... at line 1 column 5` --
    /// refusing to open a store whose bytes were entirely intact.
    #[test]
    fn a_record_that_ends_inside_a_footer_slot_is_not_skipped() {
        // Rolling off: these assertions are about blocks within one piece.
        set_wal_segment_bytes_for_test(Some(0));
        let dir = tempfile::tempdir().unwrap();
        let path = write_ahead_log_path(dir.path(), 1);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        // Frames of a fixed size, laid end to end with nothing reserved -- a log as a writer
        // that predates footers left it. 250 divides into the first block so that record 524
        // ends at 131_000, which is inside the slot that starts at 130_944.
        const FRAME: usize = 250;
        let framed = |sequence: u64| -> Vec<u8> {
            let mut pad = 8usize;
            loop {
                let payload = format!(
                    "{{\"shard_id\":1,\"sequence\":{sequence},\"command\":{{\"kind\":\"string_set\",\"key\":\"{}\",\"value\":[118]}}}}",
                    "k".repeat(pad)
                );
                let line = crate::log_framing::encode_line(payload.as_bytes());
                match line.len().cmp(&FRAME) {
                    std::cmp::Ordering::Equal => return line,
                    std::cmp::Ordering::Less => pad += FRAME - line.len(),
                    std::cmp::Ordering::Greater => {
                        pad = pad
                            .checked_sub(line.len() - FRAME)
                            .expect("a frame of this size must be reachable by padding the key");
                    }
                }
            }
        };

        let records = (WAL_BLOCK_BYTES as usize / FRAME) + 40;
        let mut bytes = Vec::with_capacity(records * FRAME);
        for sequence in 1..=records as u64 {
            bytes.extend_from_slice(&framed(sequence));
        }
        std::fs::write(&path, &bytes).unwrap();

        // The precondition this test exists for: some record must END inside a footer slot.
        // Without it the test would pass on any reader and prove nothing.
        let slot_start = block_data_end(0);
        let ends_in_slot = (1..=records as u64)
            .map(|n| n * FRAME as u64)
            .filter(|end| *end >= slot_start && *end < WAL_BLOCK_BYTES)
            .count();
        assert!(
            ends_in_slot > 0,
            "no record ends inside the footer slot, so this test would not exercise the skip"
        );

        let store = LocalWriteAheadLogStore::new(dir.path());
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(
            scanned.len(),
            records,
            "every record must survive the block boundary in a log written without footers"
        );
        for (_, line) in &scanned {
            decode_wal_line(line).expect("every record must still decode");
        }
        let (last, _) = last_wal_sequence_in(&path).unwrap();
        assert_eq!(
            last, records as u64,
            "the tail walk must reach the last record rather than stopping at a slot"
        );
    }

    /// What the footer actually buys, on equivalent logs. Ignored: a measurement, not a gate.
    ///
    // The footer-on / footer-off benchmark that lived here cannot run any more: there is no way
    // to build the footer-off arm now that the footer is unconditional. Its numbers are preserved
    // in the doc comment on the footer itself, which is where they justify the design.


    /// With blocks on, a scan has to walk past the footers between them. It did not: the first
    /// footer looked like the end of the log, so every record after the first block vanished
    /// from every reader that scans -- replay included.
    #[test]
    fn a_scan_returns_records_from_every_block_not_just_the_first() {
        // rolling is off for this test. The segment threshold is a THREAD-LOCAL override and
        // these tests share a thread, so whatever the previous test left is inherited -- and a
        // log that rolls mid-test splits across files the assertions never look at.
        //
        // `Some(0)`, not `None`: None clears the override and takes the DEFAULT, and the default
        // rolls now. These assertions are about blocks within one piece, so they say "off"
        // explicitly instead of relying on what the default happens to be.
        set_wal_segment_bytes_for_test(Some(0));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let records = 2_000usize;
        for index in 0..records {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:05}"),
                        value: vec![b'v'; 1024],
                    },
                    false,
                )
                .unwrap();
        }
        let path = write_ahead_log_path(dir.path(), 1);
        let (_, record_end) = last_wal_sequence_in(&path).unwrap();
        assert!(
            record_end > 2 * WAL_BLOCK_BYTES,
            "the workload must span several blocks or this proves nothing: {record_end}"
        );
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(
            scanned.len(),
            records,
            "a scan must return every record, not only the first block's"
        );
        // And each one still decodes: the offsets the scan reports have to name whole records.
        let sequences: Vec<u64> = scanned
            .iter()
            .map(|(_, line)| decode_wal_line(line).unwrap().sequence)
            .collect();
        assert_eq!(sequences.first().copied(), Some(1));
        assert_eq!(sequences.last().copied(), Some(records as u64));
    }

    /// Not an assertion about behaviour -- a look at what the writer actually did, because the
    /// agreement test only says the footer is absent, not why.
    #[test]
    fn what_the_footer_writer_actually_did() {
        // rolling is off for this test. The segment threshold is a THREAD-LOCAL override and
        // these tests share a thread, so whatever the previous test left is inherited -- and a
        // log that rolls mid-test splits across files the assertions never look at.
        //
        // `Some(0)`, not `None`: None clears the override and takes the DEFAULT, and the default
        // rolls now. These assertions are about blocks within one piece, so they say "off"
        // explicitly instead of relying on what the default happens to be.
        set_wal_segment_bytes_for_test(Some(0));
        // ~327 B a record here, so a few hundred fill ONE block. Cross several on purpose.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in 0..2000 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:05}"),
                        value: vec![b'v'; 1024],
                    },
                    false,
                )
                .unwrap();
        }
        let path = write_ahead_log_path(dir.path(), 1);
        let bytes = std::fs::read(&path).unwrap();
        let (_, header_len) = read_wal_base(&path).unwrap();
        let (_, record_end) = last_wal_sequence_in(&path).unwrap();
        let slot_at = (header_len + block_footer_at(0)) as usize;
        let slot_written = bytes
            .get(slot_at..slot_at + WAL_BLOCK_FOOTER_BYTES as usize)
            .map(|slot| !slot.iter().all(|byte| *byte == 0))
            .unwrap_or(false);
        println!(
            "  file {} bytes, header {header_len}, records end at {record_end}\n  \
             block 0 footer slot at {slot_at}: {}\n  \
             blocks the records span: {}",
            bytes.len(),
            if slot_written { "WRITTEN" } else { "still zeros" },
            record_end / WAL_BLOCK_BYTES
        );
        assert!(
            record_end > WAL_BLOCK_BYTES,
            "the workload must cross a block boundary or this proves nothing: ended at \
             {record_end}, block is {WAL_BLOCK_BYTES}"
        );
        assert!(
            slot_written,
            "records crossed a block boundary, so block 0's footer must have been written"
        );
    }

    /// A footer is only worth having if it says what walking the log would have said. This
    /// writes enough records to close several blocks, then compares the two answers on the very
    /// same file: the fast path is otherwise just a faster way to be wrong.
    #[test]
    fn the_footer_and_the_walk_agree_about_where_the_log_ends() {
        // rolling is off for this test. The segment threshold is a THREAD-LOCAL override and
        // these tests share a thread, so whatever the previous test left is inherited -- and a
        // log that rolls mid-test splits across files the assertions never look at.
        //
        // `Some(0)`, not `None`: None clears the override and takes the DEFAULT, and the default
        // rolls now. These assertions are about blocks within one piece, so they say "off"
        // explicitly instead of relying on what the default happens to be.
        set_wal_segment_bytes_for_test(Some(0));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        // 128KiB blocks; a ~1KiB value closes several of them.
        for index in 0..400 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:05}"),
                        value: vec![b'v'; 1024],
                    },
                    false,
                )
                .unwrap();
        }
        let path = write_ahead_log_path(dir.path(), 1);

        let hint = footer_tail_hint(&path).unwrap();
        assert!(
            hint.is_some(),
            "several blocks were filled, so at least one footer must have been written"
        );
        let (footer_sequence, from) = hint.unwrap();

        let with_footer = last_wal_sequence_in(&path).unwrap();
        let without_footer = last_wal_sequence_forward(&path).unwrap();

        assert_eq!(
            with_footer, without_footer,
            "the footer path and the full walk must agree: footer said {with_footer:?}, \
             walking said {without_footer:?} (hint was sequence {footer_sequence} from {from})"
        );
        assert!(
            from > 0,
            "the walk should start after a closed block, not at the top"
        );
    }

    /// The point of the footer is that finding the tail stops costing the file. Reading it must
    /// therefore touch a bounded amount regardless of how much log precedes it.
    #[test]
    fn finding_the_tail_reads_a_block_not_the_log() {
        // rolling is off for this test. The segment threshold is a THREAD-LOCAL override and
        // these tests share a thread, so whatever the previous test left is inherited -- and a
        // log that rolls mid-test splits across files the assertions never look at.
        //
        // `Some(0)`, not `None`: None clears the override and takes the DEFAULT, and the default
        // rolls now. These assertions are about blocks within one piece, so they say "off"
        // explicitly instead of relying on what the default happens to be.
        set_wal_segment_bytes_for_test(Some(0));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in 0..800 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:05}"),
                        value: vec![b'v'; 1024],
                    },
                    false,
                )
                .unwrap();
        }
        let path = write_ahead_log_path(dir.path(), 1);
        let (_, from) = footer_tail_hint(&path).unwrap().expect("a closed block");
        // Measure to where the RECORDS end, not to the file's length: preallocation leaves
        // megabytes of reservation past the last record and the walk stops at the records. An
        // earlier version compared against the file and was measuring the reservation.
        let (_, record_end) = last_wal_sequence_in(&path).unwrap();
        let walked = record_end.saturating_sub(from);
        assert!(
            walked <= WAL_BLOCK_BYTES,
            "the walk after the footer must be bounded by one block: {walked} bytes, from \
             {from} to {record_end}"
        );
    }

    /// A crash leaves the open block without its footer. Recovery must fall back to the last
    /// footer that WAS written and walk from there -- never trust a slot that holds zeros.
    #[test]
    fn a_block_that_never_closed_falls_back_to_the_one_that_did() {
        // rolling is off for this test. The segment threshold is a THREAD-LOCAL override and
        // these tests share a thread, so whatever the previous test left is inherited -- and a
        // log that rolls mid-test splits across files the assertions never look at.
        //
        // `Some(0)`, not `None`: None clears the override and takes the DEFAULT, and the default
        // rolls now. These assertions are about blocks within one piece, so they say "off"
        // explicitly instead of relying on what the default happens to be.
        set_wal_segment_bytes_for_test(Some(0));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in 0..400 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:05}"),
                        value: vec![b'v'; 1024],
                    },
                    false,
                )
                .unwrap();
        }
        drop(store);
        let path = write_ahead_log_path(dir.path(), 1);
        let expected = last_wal_sequence_forward(&path).unwrap();
        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let seen = reopened.stats(1).last_sequence;
        assert_eq!(
            seen, expected.0,
            "reopening must see every record the walk sees, footer or no footer"
        );
    }

    /// Rolling must not cost extra durability barriers per write.
    ///
    /// A roll creates a new file, and its directory entry has to reach disk before any record in
    /// that piece is acked -- so rolling COULD add an fsync every piece. The append path defers
    /// that debt to the next barrier that was going to run anyway, and this checks the deferral
    /// actually holds.
    ///
    /// Counted, not timed. The count is the thing: a barrier either happened or it did not, and a
    /// duration on a shared machine mostly measures what else was running.
    #[test]
    fn rolling_adds_no_durability_barriers() {
        fn syncs_for(roll: u64) -> (u64, u64, usize) {
            set_wal_segment_bytes_for_test(Some(roll));
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            for index in 0..3_000u64 {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("k{index:06}"),
                            value: vec![118u8; 256],
                        },
                        // Sync ON: with it off nothing syncs, the counters read zero on both
                        // sides, and the comparison below passes on having measured nothing.
                        true,
                    )
                    .unwrap();
            }
            let stats = store.stats(1);
            let pieces = wal_segment_paths(dir.path(), 1).len();
            set_wal_segment_bytes_for_test(None);
            (stats.syncs, stats.flushes, pieces)
        }

        let (never_syncs, never_flushes, never_pieces) = syncs_for(0);
        let (rolled_syncs, rolled_flushes, rolled_pieces) = syncs_for(DEFAULT_WAL_SEGMENT_BYTES);
        println!(
            "  never rolling: {never_syncs} syncs, {never_flushes} flushes, {never_pieces} piece(s)
               at the default: {rolled_syncs} syncs, {rolled_flushes} flushes, {rolled_pieces} piece(s)"
        );

        // A bound passes most easily when nothing was measured -- and this one already did once,
        // reading 0 syncs against 0 syncs because the appends were not asking for durability.
        assert!(never_syncs > 0, "the probe must observe syncs: {never_syncs}");
        assert!(rolled_syncs > 0, "the probe must observe syncs: {rolled_syncs}");
        assert!(never_pieces == 1, "zero must still mean one piece");
        assert!(rolled_pieces > 1, "the default must actually roll this workload");

        // The deferral is the claim: rolling adds pieces without adding a barrier per piece.
        // Allow one per piece as slack; anything beyond that means the debt is NOT being deferred.
        let extra = rolled_syncs.saturating_sub(never_syncs);
        assert!(
            extra <= rolled_pieces as u64,
            "rolling added {extra} syncs over {} pieces -- the roll debt is not being deferred",
            rolled_pieces
        );
    }

    /// Which part of an append allocates. Encoding, framing, and everything else.
    ///
    /// A whole-append figure says a write costs 34 allocations and about nine kilobytes for a
    /// 64-byte value, but not which step to look at. This prices the steps that are callable on
    /// their own, and infers the rest by subtraction rather than by reading the code and guessing.
    #[test]
    #[cfg(feature = "alloc-probe")]
    #[ignore]
    fn where_an_appends_allocations_go() {
        for value_len in [64usize, 4096] {
            let record = WriteAheadLogRecord {
                shard_id: 1,
                sequence: 7,
                metadata: Some(WriteAheadLogRecordMetadata {
                    version: WRITE_AHEAD_LOG_FORMAT_VERSION,
                    timestamp_ms: 1_787_270_070_192,
                    items: Vec::new(),
                    batch_id: None,
                    batch_size: None,
                    batch_index: None,
                }),
                command: Some(Command::StringSet {
                    key: "tenant/7/object/000000123".to_string(),
                    value: vec![118u8; value_len],
                }),
                staged_pages: Vec::new(),
                outcomes: Vec::new(),
            };

            let runs = 200usize;

            let probe = crate::alloc_probe::Probe::start();
            for _ in 0..runs {
                let payload = encode_wal_payload(&record).expect("encode");
                std::hint::black_box(&payload);
            }
            let encode = probe.stop();

            let probe = crate::alloc_probe::Probe::start();
            for _ in 0..runs {
                let payload = encode_wal_payload(&record).expect("encode");
                let framed = crate::log_framing::encode_record(&payload);
                std::hint::black_box(&framed);
            }
            let encode_and_frame = probe.stop();

            println!(
                "  STEP value {value_len:>5}B | encode {:>5.1} allocs {:>7.0} B |                  +frame {:>5.1} allocs {:>7.0} B (frame alone {:>5.1} / {:>7.0} B)",
                encode.allocs as f64 / runs as f64,
                encode.alloc_bytes as f64 / runs as f64,
                encode_and_frame.allocs as f64 / runs as f64,
                encode_and_frame.alloc_bytes as f64 / runs as f64,
                (encode_and_frame.allocs as f64 - encode.allocs as f64) / runs as f64,
                (encode_and_frame.alloc_bytes as f64 - encode.alloc_bytes as f64) / runs as f64,
            );

            // A bound passes most easily when nothing was measured.
            assert!(encode.allocs > 0, "the probe must observe the encode");
        }
    }

    /// A lock file that was removed gets opened again, rather than locked as a ghost.
    ///
    /// The append lock handle is kept open per shard now, instead of being opened and closed on
    /// every append. That is safe only while the handle still names a file: a descriptor outlives
    /// the unlinking of its file, and `flock` on an unlinked inode SUCCEEDS -- so a stale handle
    /// would hand out a lock that another process, opening the path afresh, could hold at the same
    /// time. Two writers, both certain they held the append lock.
    ///
    /// Removing the file is the observable form of that: if the handle were reused blindly, the
    /// file would never come back.
    #[test]
    fn a_removed_lock_file_is_opened_again() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let write = |index: usize| {
            store
                .append(
                    1,
                    Command::StringSet {
                        key: format!("k{index}"),
                        value: b"v".to_vec(),
                    },
                )
                .unwrap()
        };

        write(0);
        let lock_path = dir.path().join("shard-1.wal.lock");
        assert!(lock_path.exists(), "the first append must create the lock file");

        // Someone removes it out from under the running store.
        std::fs::remove_file(&lock_path).expect("remove the lock file");
        assert!(!lock_path.exists(), "the probe must actually remove it");

        let record = write(1);
        assert_eq!(record.sequence, 2, "the append must still succeed");
        assert!(
            lock_path.exists(),
            "the lock file must be opened again, not locked as an unlinked ghost"
        );

        // And the log itself is intact across it.
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(scanned.len(), 2, "both records must be readable: {scanned:?}");
    }

    /// Every `Command` variant's payload, sized by the compiler rather than by eye.
    ///
    /// `large_enum_variant` stays silent when the top variants are all wide -- it compares the
    /// largest against the SECOND largest, so a family of equally fat variants reads as no problem.
    /// That is a different question from "would boxing a few of them collapse the enum", which is
    /// what this answers.
    #[test]
    #[ignore]
    fn every_command_variant_payload_size() {
        // Glob so every field type resolves without guessing which need qualifying.
        #[allow(unused_imports)]
        use crate::types::*;
        let mut rows: Vec<(&str, usize)> = vec![
        ("CommonDelete", std::mem::size_of::<(String)>()),
        ("CommonExpire", std::mem::size_of::<(String, u64)>()),
        ("CommonTtl", std::mem::size_of::<(String)>()),
        ("CommonPersist", std::mem::size_of::<(String)>()),
        ("CommonExists", std::mem::size_of::<(String)>()),
        ("StringSet", std::mem::size_of::<(String, Vec<u8>)>()),
        ("StringSetEx", std::mem::size_of::<(String, Vec<u8>, u64)>()),
        ("StringSetConditional", std::mem::size_of::<(String, Vec<u8>, Option<u64>, StringSetCondition, bool)>()),
        ("StringGet", std::mem::size_of::<(String)>()),
        ("StringDelete", std::mem::size_of::<(String)>()),
        ("HashSet", std::mem::size_of::<(String, String, Vec<u8>)>()),
        ("HashGet", std::mem::size_of::<(String, String)>()),
        ("HashMultiGet", std::mem::size_of::<(String, Vec<String>)>()),
        ("HashMultiSet", std::mem::size_of::<(String, Vec<(String, Vec<u8>)>)>()),
        ("HashIncrBy", std::mem::size_of::<(String, String, i64)>()),
        ("HashGetAll", std::mem::size_of::<(String)>()),
        ("HashLen", std::mem::size_of::<(String)>()),
        ("HashDelete", std::mem::size_of::<(String, String)>()),
        ("SetAdd", std::mem::size_of::<(String, Vec<u8>)>()),
        ("ZSetAdd", std::mem::size_of::<(String, Vec<u8>, f64)>()),
        ("ZSetScore", std::mem::size_of::<(String, Vec<u8>)>()),
        ("ZSetRemove", std::mem::size_of::<(String, Vec<u8>)>()),
        ("ZSetCard", std::mem::size_of::<(String)>()),
        ("ZSetRange", std::mem::size_of::<(String, i64, i64, bool)>()),
        ("ZSetRangeByScore", std::mem::size_of::<(String, f64, f64, bool, bool, bool)>()),
        ("SeenCheck", std::mem::size_of::<(String, Vec<u8>, u64)>()),
        ("SeenCard", std::mem::size_of::<(String)>()),
        ("BucketTake", std::mem::size_of::<(String, f64, f64, f64)>()),
        ("BucketPeek", std::mem::size_of::<(String, f64, f64, f64)>()),
        ("ZSetIncrBy", std::mem::size_of::<(String, Vec<u8>, f64)>()),
        ("ZSetPop", std::mem::size_of::<(String, bool, u64)>()),
        ("ZSetRank", std::mem::size_of::<(String, Vec<u8>, bool)>()),
        ("ListPush", std::mem::size_of::<(String, Vec<u8>, bool)>()),
        ("ListPop", std::mem::size_of::<(String, bool)>()),
        ("ListRange", std::mem::size_of::<(String, i64, i64)>()),
        ("ListLen", std::mem::size_of::<(String)>()),
        ("SetMembers", std::mem::size_of::<(String)>()),
        ("SetRemove", std::mem::size_of::<(String, Vec<u8>)>()),
        ("FeatureAppend", std::mem::size_of::<(String, Vec<FeaturePoint>)>()),
        ("FeatureAppendWithPolicy", std::mem::size_of::<(String, Vec<FeaturePoint>, FeatureWritePolicy)>()),
        ("FeatureQuery", std::mem::size_of::<(String, u64, u64, Option<usize>)>()),
        ("FeatureQueryFiltered", std::mem::size_of::<(String, u64, u64, Option<usize>, Vec<FeatureFilter>)>()),
        ("FeatureReplace", std::mem::size_of::<(String, u64, u64, Vec<FeaturePoint>)>()),
        ("FeatureDelete", std::mem::size_of::<(String)>()),
        ("FeatureAggQuery", std::mem::size_of::<(String, u64, u64, String, Option<usize>)>()),
        ("SequenceAdd", std::mem::size_of::<(String, Vec<SequenceFeatureRow>)>()),
        ("SequenceQuery", std::mem::size_of::<(String, u64, u64, usize, Vec<FeatureFilter>)>()),
        ("SequenceBatchQuery", std::mem::size_of::<(Vec<SequenceQuerySpec>)>()),
        ("ControlStateIncrement", std::mem::size_of::<(String, u64, i64)>()),
        ("ControlStateIncrementWithOptions", std::mem::size_of::<(String, u64, i64, Option<u64>, Option<u64>)>()),
        ("ControlStateChangeAdd", std::mem::size_of::<(String, u64, Vec<u8>, Option<u64>, Option<u64>)>()),
        ("ControlStateCount", std::mem::size_of::<(String, u64, u64)>()),
        ("ControlStateQuery", std::mem::size_of::<(String, u64, u64, String)>()),
        ("ControlStateDetail", std::mem::size_of::<(String, u64, u64, Option<usize>)>()),
        ("ControlStateSet", std::mem::size_of::<(ControlStateFamily, String, u64, i64)>()),
        ("ControlStateSetAndGet", std::mem::size_of::<(ControlStateFamily, String, u64, i64, u64, u64, String)>()),
        ("ControlStateSetAndGetWithOptions", std::mem::size_of::<(ControlStateFamily, String, u64, i64, u64, u64, String, Option<u64>, Option<u64>, Option<String>)>()),
        ("ControlStateFamilyQuery", std::mem::size_of::<(ControlStateFamily, String, u64, u64, String)>()),
        ("ControlStateSelectionSet", std::mem::size_of::<(String, Vec<u8>, u64, u64, ControlStateSelectionType)>()),
        ("ControlStateSelectionQuery", std::mem::size_of::<(String)>()),
        ("ControlStateManager", std::mem::size_of::<(String, Option<String>, Vec<(String, String)>, String, String, bool)>()),
        ("ControlStateDebug", std::mem::size_of::<(String, u64, u64)>()),
        ("ContextQueryNodeEmbeddings", std::mem::size_of::<(u64, Vec<u64>)>()),
        ("ContextResourceBlobBegin", std::mem::size_of::<(u64)>()),
        ("ContextResourceBlobAppend", std::mem::size_of::<(u64, String, String)>()),
        ("ContextResourceBlobCommit", std::mem::size_of::<(u64, String)>()),
        ("ContextResourceBlobPut", std::mem::size_of::<(u64, String)>()),
        ("ContextResourceBlobFetch", std::mem::size_of::<(String, u64, u64)>()),
        ("ContextResourceBlobSweep", std::mem::size_of::<(u64, Vec<u64>, u64)>()),
        ("ContextSetNodeEmbedding", std::mem::size_of::<(u64, u64, u64, Vec<f32>, u64)>()),
        ("ContextUpsertNode", std::mem::size_of::<(u64, Box<ContextNode>)>()),
        ("ContextGetNode", std::mem::size_of::<(u64, u64)>()),
        ("ContextGetNodes", std::mem::size_of::<(u64, Vec<u64>)>()),
        ("ContextWriteEvent", std::mem::size_of::<(u64, u64, Box<ContextEvent>, bool, bool)>()),
        ("ContextWriteExtractedEvent", std::mem::size_of::<(u64, u64, Box<ContextEvent>, ContextExtractedEventIndexes, bool, bool)>()),
        ("ContextQueryEvents", std::mem::size_of::<(u64, u64, u64, u64, Option<usize>, Option<usize>, bool, u64, Vec<u32>, Vec<u32>, f32, f32)>()),
        ("ContextWriteIndexRef", std::mem::size_of::<(u64, String, u64, u64, u64, ContextIndexRef)>()),
        ("ContextQueryIndex", std::mem::size_of::<(u64, String, u64, u64, u64, u64, Option<usize>)>()),
        ("ContextQueryIndexIntersection", std::mem::size_of::<(u64, Vec<ContextIndexLookup>, Option<usize>)>()),
        ("ContextWritePackAudit", std::mem::size_of::<(u64, ContextPackAudit)>()),
        ("ContextQueryPackAudit", std::mem::size_of::<(u64, u64, u64, u64, Option<usize>)>()),
        ("ContextMarkSummaryDirty", std::mem::size_of::<(u64, u64, u64, u32, u32)>()),
        ("ContextQuerySummaryDirty", std::mem::size_of::<(u64, u64, u64, u64, Option<usize>)>()),
        ("ContextMarkEmbeddingDirty", std::mem::size_of::<(u64, u64, u64, u32, u32, bool)>()),
        ("ContextQueryEmbeddingDirty", std::mem::size_of::<(u64, u64, u64, u64, Option<usize>)>()),
        ("ContextUpsertEntity", std::mem::size_of::<(u64, ContextEntity)>()),
        ("ContextGetEntity", std::mem::size_of::<(u64, u64, u64)>()),
        ("ContextQueryEntities", std::mem::size_of::<(u64, u64, Vec<u64>, Option<usize>)>()),
        ("ContextUpsertChildRef", std::mem::size_of::<(u64, ContextChildRef)>()),
        ("ContextQueryChildren", std::mem::size_of::<(u64, u64, Option<usize>)>()),
        ("ContextTraverseTree", std::mem::size_of::<(u64, u64, Vec<f32>, Option<u32>, Option<usize>, Option<usize>, Option<usize>, bool)>()),
        ("ContextUpsertSummary", std::mem::size_of::<(u64, ContextSummary)>()),
        ("ContextQuerySummaries", std::mem::size_of::<(u64, u64, u32, u64, Option<usize>)>()),
        ("ContextQuerySummaryVectors", std::mem::size_of::<(u64, Vec<u64>, u32, u64)>()),
        ("ContextWriteCompressionEvent", std::mem::size_of::<(u64, ContextCompressionEvent)>()),
        ("ContextQueryCompressionEvents", std::mem::size_of::<(u64, Vec<u64>, u64, u64, Option<usize>)>()),
        ("ContextCompressEvents", std::mem::size_of::<(u64, u64, u64, u64, u64, Option<usize>, f32, f32)>()),
        ("ContextQueryNodeContext", std::mem::size_of::<(u64, u64, Option<u32>, u64, u64, u64, Option<usize>)>()),
        ];
        rows.sort_by(|a, b| b.1.cmp(&a.1));
        let whole = std::mem::size_of::<Command>();
        println!("  Command is {whole} B");
        println!("  FIELD ContextEvent                    {:>4} B", std::mem::size_of::<ContextEvent>());
        println!("  FIELD ContextExtractedEventIndexes    {:>4} B", std::mem::size_of::<ContextExtractedEventIndexes>());
        println!("  FIELD Box<ContextEvent>               {:>4} B", std::mem::size_of::<Box<ContextEvent>>());
        let mut cumulative = 0usize;
        for (index, (name, size)) in rows.iter().enumerate().take(12) {
            cumulative += size;
            println!("    VARIANT {:>2}. {:<36} {:>4} B", index + 1, name, size);
        }
        let _ = cumulative;
        println!("  boxing the top N drops the enum to the (N+1)th plus a discriminant");
        assert!(whole > 0);
    }

    /// What their item SHAPE costs us, since we already have the type.
    ///
    /// `WalItem` mirrors their operation-log item field for field -- the field numbers match
    /// one-for-one and a test pins that. It is one flat message with optional fields, where
    /// `Command` is a 99-variant sum type as wide as its widest arm. That difference, not any
    /// individual field, is why our record is bigger than theirs.
    ///
    /// If the record carried the flat item the way theirs does, this is what it would occupy.
    #[test]
    #[ignore]
    fn what_their_item_shape_would_cost() {
        use std::mem::size_of;
        let wal_item = size_of::<crate::wal_record::WalItem>();
        let command = size_of::<Option<Command>>();
        let record = size_of::<WriteAheadLogRecord>();
        println!("  SHAPE WalItem (their item, our type)  {wal_item:>4} B");
        println!("  SHAPE Option<Command> (a sum type)    {command:>4} B");
        println!("  SHAPE record as it stands             {record:>4} B");
        println!(
            "  SHAPE record with the flat item instead {:>4} B  (record - command + item)",
            record - command + wal_item
        );
        println!("  their item is ~20 B of scalars plus the message, held by value");
        assert!(wal_item > 0);
    }

    /// The cached active path still names the live piece after a roll.
    ///
    /// The append path keeps the active piece's path per shard instead of rebuilding it six times
    /// a write. That is only safe because a roll SEALS the old piece under a numbered name and
    /// recreates the active one under the same name -- the cached path keeps pointing at the live
    /// file. If a roll ever renamed the active piece instead, the cache would address the sealed
    /// file and every later append would land in the wrong place.
    #[test]
    fn the_cached_active_path_survives_a_roll() {
        set_wal_segment_bytes_for_test(Some(4 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let value = vec![118u8; 512];

        for index in 0..40usize {
            store
                .append(
                    1,
                    Command::StringSet {
                        key: format!("k{index:04}"),
                        value: value.clone(),
                    },
                )
                .unwrap();
        }
        let pieces = wal_segment_paths(dir.path(), 1).len();
        set_wal_segment_bytes_for_test(None);
        assert!(pieces > 1, "the workload must roll or this proves nothing: {pieces}");

        // Every record is still readable, in order, across the roll.
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(scanned.len(), 40, "every record must survive the roll: {}", scanned.len());

        // And the next append lands in the live piece, not a sealed one.
        let after = store
            .append(
                1,
                Command::StringSet {
                    key: "after-roll".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(after.sequence, 41, "the append after a roll must continue the sequence");
        let reread = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(reread.len(), 41, "the post-roll record must be readable too");
    }

    /// The physical length the append records matches what the file actually is.
    ///
    /// The append used to stat the file a second time to learn its length after possibly growing
    /// it. That length is now carried: either the one just read, or the one just set. If those ever
    /// disagreed, the cached figure would put the next record at the wrong offset -- so this checks
    /// the carried number against the filesystem across both branches, the append that grows the
    /// file and the many that do not.
    #[test]
    fn the_carried_physical_length_matches_the_file() {
        // `Some(0)`, not `None`: rolling is on by default, and a roll seals the active piece and
        // starts a new one -- so the active file drops back to a header while the carried figure
        // keeps counting. This checks the ACTIVE file's bookkeeping, so it needs one file.
        set_wal_segment_bytes_for_test(Some(0));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let path = write_ahead_log_path(dir.path(), 1);

        let mut grew = 0usize;
        let mut last = 0u64;
        for index in 0..600usize {
            store
                .append(
                    1,
                    Command::StringSet {
                        key: format!("k{index:05}"),
                        value: vec![118u8; 512],
                    },
                )
                .unwrap();
            let on_disk = std::fs::metadata(&path).unwrap().len();
            let carried = store.stats(1).persistent_bytes;
            assert!(
                carried <= on_disk,
                "record {index}: carried {carried} exceeds the file's {on_disk}"
            );
            if on_disk != last {
                grew += 1;
                last = on_disk;
            }
        }

        // A bound passes most easily when nothing was measured: the run must actually have grown
        // the file more than once, or it never exercised the branch that sets the length.
        assert!(grew > 1, "the workload must grow the file repeatedly: {grew}");

        // And every record still reads back, which is what a wrong offset would break.
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        set_wal_segment_bytes_for_test(None);
        assert_eq!(scanned.len(), 600, "every record must be readable: {}", scanned.len());
    }

    /// An append allocates no buffer of its own beyond the record it is writing.
    ///
    /// This bounds the number rather than printing it, because the thing it guards against is
    /// invisible in a passing suite: the roll check used to call `read_wal_base` on EVERY append,
    /// which opens the piece and reads its header through a `BufReader` -- and that reader's
    /// buffer is eight kilobytes. A write of a 256-byte value allocated 8,987 bytes, almost all of
    /// it that one buffer, to answer "is this piece full" from a number the shard already had.
    ///
    /// Anything that re-introduces a per-append reader, or any other fixed buffer, fails here.
    #[test]
    #[cfg(feature = "alloc-probe")]
    fn an_append_allocates_no_hidden_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let value = vec![118u8; 256];

        // Warm past the first write, which pays for the file and the header.
        store
            .append(1, Command::StringSet { key: "warm".to_string(), value: value.clone() })
            .unwrap();

        let runs = 100usize;
        let commands = (0..runs)
            .map(|index| Command::StringSet {
                key: format!("k{index:06}"),
                value: value.clone(),
            })
            .collect::<Vec<_>>();

        let probe = crate::alloc_probe::Probe::start();
        for command in commands {
            store.append(1, command).unwrap();
        }
        let counts = probe.stop();
        let per_write = counts.alloc_bytes as f64 / runs as f64;
        println!("  a 256-byte write allocates {per_write:.0} B");

        // A bound passes most easily when nothing was measured.
        assert!(counts.allocs > 0, "the probe must observe the appends");

        // Eight kilobytes is the size of the reader that used to be here. Two is comfortably
        // above what the record itself needs and far below anything with a hidden buffer in it.
        assert!(
            per_write < 2_000.0,
            "a 256-byte write allocated {per_write:.0} B: something is allocating a buffer per              append again"
        );
    }

    /// What one append allocates, and how much of it is the value it carries.
    ///
    /// Allocation count is latency and allocation bytes are memory; both are deterministic, unlike
    /// a timing on a shared machine. Reported per append across value sizes, so a cost that scales
    /// with the payload can be told apart from a fixed one -- a fixed cost is the one worth
    /// attacking, since it is paid by every write however small.
    /// How many times one append asks for the active log path, and what each ask costs.
    ///
    /// The path is cached as an `Arc`, but every caller gets a COPY of it: the cache removed the
    /// cost of BUILDING the name and left the cost of copying it. Whether that is worth chasing
    /// is a question of how often an append asks, which is cheaper to count than to find out by
    /// rewriting every call site.
    ///
    /// No `alloc-probe` feature here -- it counts calls, not allocations. An earlier version of
    /// this probe inherited that cfg from the test it was pasted above and vanished from the
    /// build entirely.
    /// A batch that outgrows its reservation several times still reads back whole.
    ///
    /// 512 records of 4 KiB is ~2 MiB against a 256 KiB chunk, so the reservation is grown
    /// roughly eight times inside one batch -- the case a single-chunk batch never reaches.
    ///
    /// What the carried physical length can and cannot break, since the distinction decides what
    /// this test is worth: the carried number gates GROWTH only. The write offset comes from
    /// `verified_len_by_shard`, not from the file's physical length, so a stale carried value
    /// causes a redundant `set_len` and never a misplaced record. Checked by control -- refusing
    /// to update it after a growth leaves this test passing and only the syscall count moving.
    ///
    /// So this is an end-to-end check that a multi-growth batch survives, not the discriminating
    /// test for the carried length: `a_batch_opens_the_piece_once_not_once_per_record` is that.
    #[test]
    fn a_batch_that_outgrows_its_reservation_reads_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let commands: Vec<Command> = (0..512)
            .map(|index| Command::StringSet {
                key: format!("grown-{index:05}"),
                value: vec![(index % 251) as u8; 4096],
            })
            .collect();

        let written = store
            .append_batch_atomic(1, commands, true)
            .expect("the batch appends");
        assert_eq!(written.len(), 512);

        let read = store
            .scan(1, 0, u64::MAX, u64::MAX)
            .expect("the log scans back");
        assert_eq!(
            read.len(),
            512,
            "the batch wrote 512 records and the log reads back {}; a carried physical length              that missed a growth puts records at the wrong offset",
            read.len()
        );

        // Sequences are contiguous and in order -- a record landing at a wrong offset shows up
        // here before it shows up anywhere a user would look.
        let sequences: Vec<u64> = written.iter().map(|record| record.sequence).collect();
        let mut expected = sequences.clone();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(
            sequences, expected,
            "the batch produced out-of-order or duplicate sequences"
        );
    }

    /// A batch opens the active piece ONCE, not once per record.
    ///
    /// A batch is one crash-atomic group under one durability barrier, and that barrier is the
    /// thing it exists to amortise. Each record used to open, write, stat and close the piece for
    /// itself, so a batch of N paid 4N syscalls against that one barrier.
    ///
    /// Counted rather than timed: the syscalls ARE the cost being removed, and this machine cannot
    /// resolve the difference in wall clock under load. The single-append arm is the control --
    /// without it, a counter that had simply stopped incrementing would read as a perfect result.
    #[test]
    fn a_batch_opens_the_piece_once_not_once_per_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        // Warm: the first append creates the piece and its header, which is not per-record work.
        store
            .append(1, Command::StringSet { key: "warm".to_string(), value: vec![1] })
            .unwrap();

        let commands: Vec<Command> = (0..64)
            .map(|index| Command::StringSet {
                key: format!("batched-{index:04}"),
                value: vec![7; 32],
            })
            .collect();

        let reset = || {
            super::WAL_FILE_OPENS.with(|opens| opens.set(0));
            super::WAL_FILE_STATS.with(|stats| stats.set(0));
        };
        let opens = || super::WAL_FILE_OPENS.with(|opens| opens.get());
        let stats = || super::WAL_FILE_STATS.with(|stats| stats.get());

        reset();
        store.append_batch_atomic(1, commands.clone(), true).unwrap();
        let batched = opens();
        let batched_stats = stats();

        reset();
        for command in commands {
            store.append_with_sync(1, command, true).unwrap();
        }
        let one_at_a_time = opens();
        let one_at_a_time_stats = stats();

        assert!(
            one_at_a_time >= 64,
            "the control opened {one_at_a_time} times for 64 single appends, so the counter is              not counting opens and the batch figure below means nothing"
        );
        assert_eq!(
            batched, 1,
            "a batch of 64 records opened the piece {batched} times; it holds the append lock and              the inner lock across the whole loop and rolls only afterwards, so one open covers it"
        );

        // The same argument covers the physical length. Under preallocation each record asked the
        // filesystem whether the reservation still had room; nothing else can change that length
        // while both locks are held, so the batch reads it once and carries it, growing its own
        // copy when it grows the file.
        assert!(
            one_at_a_time_stats >= 1,
            "the control never asked for the physical length ({one_at_a_time_stats}), so the              batch figure below is not measuring anything"
        );
        assert_eq!(
            batched_stats, 1,
            "a batch of 64 records asked for the piece's physical length {batched_stats} times"
        );
    }

    #[test]
    #[ignore]
    fn how_often_an_append_asks_for_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append(1, Command::StringSet { key: "warm".to_string(), value: vec![1] })
            .unwrap();

        super::ACTIVE_PATH_CALLS.with(|calls| calls.set(0));
        let runs = 50usize;
        for index in 0..runs {
            store
                .append(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 256],
                    },
                )
                .unwrap();
        }
        let calls = super::ACTIVE_PATH_CALLS.with(|calls| calls.get());
        let path_len = write_ahead_log_path(dir.path(), 1).as_os_str().len();
        println!(
            "  PATHCALLS {:.2} per append | the name is {path_len} B, so the copies cost about {:.0} B/append",
            calls as f64 / runs as f64,
            calls as f64 / runs as f64 * path_len as f64,
        );
        assert!(calls > 0, "the counter must observe the appends");
    }

    /// How much of a record's envelope could a batch actually share?
    ///
    /// Their logger pays its record header once per COMMIT and ours once per RECORD, so the
    /// amortisation prize looks like the whole envelope. It is not: part of what sits outside the
    /// value travels WITH each item and cannot be shared -- the key above all. Only the part that
    /// is per-record can amortise.
    ///
    /// This separates the two by writing the same value under keys of two lengths. What moves
    /// with the key is per-item; what stays is the fixed, shareable remainder.
    #[test]
    #[ignore]
    fn how_much_of_the_envelope_can_amortise() {
        let value_len = 64usize;
        let measure = |key_len: usize| -> f64 {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            let path = write_ahead_log_path(dir.path(), 1);
            let value = vec![118u8; value_len];
            let append = |index: usize| {
                let key = format!("{:0width$}", index, width = key_len);
                store
                    .append(1, Command::StringSet { key, value: value.clone() })
                    .unwrap();
            };
            append(0);
            let (_, before) = last_wal_sequence_in(&path).unwrap();
            let runs = 16usize;
            for index in 1..=runs {
                append(index);
            }
            let (_, after) = last_wal_sequence_in(&path).unwrap();
            (after - before) as f64 / runs as f64
        };

        let short = measure(8);
        let long = measure(40);
        // Each extra key byte costs one byte on the wire, so the difference IS the key span.
        let per_key_byte = (long - short) / 32.0;
        let key_part = 8.0 * per_key_byte;
        let fixed = short - value_len as f64 - key_part;

        println!(
            "  AMORTISE 8-char key {short:.1} B/record, 40-char {long:.1} B/record | value {value_len} B | key ~{key_part:.1} B | FIXED (shareable) {fixed:.1} B"
        );
        for items in [8usize, 64] {
            let n_records = short * items as f64;
            let one_record = fixed + (short - fixed) * items as f64;
            println!(
                "    at {items:>3} items: {n_records:.0} B as records vs {one_record:.0} B sharing the fixed part = {:.1}% ",
                100.0 * (n_records - one_record) / n_records
            );
        }

        assert!(short > 0.0 && long > short, "the probe must observe a key-length difference");
        assert!(fixed > 0.0, "the fixed part cannot be negative: {fixed}");
    }

    #[test]
    #[cfg(feature = "alloc-probe")]
    #[ignore]
    fn what_one_append_allocates() {
        for value_len in [64usize, 256, 1024, 4096] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            let value = vec![118u8; value_len];

            // Warm: the first append pays for the file, the header and any lazy setup.
            store
                .append(
                    1,
                    Command::StringSet {
                        key: "warm".to_string(),
                        value: value.clone(),
                    },
                )
                .unwrap();

            // Built BEFORE the probe starts. Constructing the command is the caller's cost, not
            // the append's, and a `format!` plus a `clone` inside the window put two allocations a
            // write on the append's account that it never paid.
            let runs = 200usize;
            let commands = (0..runs)
                .map(|index| Command::StringSet {
                    key: format!("k{index:06}"),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();

            let probe = crate::alloc_probe::Probe::start();
            for command in commands {
                store.append(1, command).unwrap();
            }
            let counts = probe.stop();

            println!(
                "  APPEND value {value_len:>5}B -> {:>6.1} allocs, {:>8.0} B allocated                  ({:>5.2}x the value)",
                counts.allocs as f64 / runs as f64,
                counts.alloc_bytes as f64 / runs as f64,
                counts.alloc_bytes as f64 / runs as f64 / value_len as f64,
            );

            // A bound passes most easily when nothing was measured.
            assert!(counts.allocs > 0, "the probe must observe the appends");
        }
    }

    /// Sweep: what each rolling threshold costs and saves. Ignored -- a measurement, not a bound.
    ///
    ///   cargo test -p temporalstore-rust --lib sweep_segment_sizes -- --ignored --nocapture
    #[test]
    #[ignore]
    fn sweep_segment_sizes() {
        // Preallocation is ON by default and grows a file 256 KiB at a time, so a piece smaller
        // than that still costs a full chunk on disk. That sets the floor for the candidates.
        for (label, roll) in [
            ("off (today)", None),
            ("256 KiB", Some(256u64 * 1024)),
            ("512 KiB", Some(512 * 1024)),
            ("1 MiB", Some(1024 * 1024)),
            ("4 MiB", Some(4 * 1024 * 1024)),
        ] {
            for (shape, keep_fraction) in [("trim oldest 10%", 0.10f64), ("dump-driven, keep 10%", 0.90)] {
                set_wal_segment_bytes_for_test(roll);
                let dir = tempfile::tempdir().unwrap();
                let store = LocalWriteAheadLogStore::new(dir.path());
                let records = 4000u64;
                for index in 0..records {
                    store
                        .append(
                            1,
                            Command::StringSet {
                                key: format!("k{index:06}"),
                                value: vec![118u8; 256],
                            },
                        )
                        .unwrap();
                }
                let files_before = wal_segment_paths(dir.path(), 1).len();
                let on_disk_before: u64 = wal_segment_paths(dir.path(), 1)
                    .iter()
                    .filter_map(|p| p.metadata().ok().map(|m| m.len()))
                    .sum();

                // `bytes_copied` misses the read side: an unrolled reclaim reads the whole file to
                // find survivors, a rolled one skips the pieces it unlinks. `stats().bytes_read`
                // does NOT cover the reclaim path -- it reported a flat zero here, which is the
                // instrument not observing rather than reads being free -- so this times the call.
                // A timing number, reported and never asserted on.
                let retain_from = (records as f64 * keep_fraction) as u64;
                let started = std::time::Instant::now();
                let report = store.gc_before_sequence_unchecked(1, retain_from).unwrap();
                let elapsed_us = started.elapsed().as_micros();

                let on_disk_after: u64 = wal_segment_paths(dir.path(), 1)
                    .iter()
                    .filter_map(|p| p.metadata().ok().map(|m| m.len()))
                    .sum();
                set_wal_segment_bytes_for_test(None);

                println!(
                    "  SWEEP {label:>11} | {shape:>21} | files {files_before:>3} |                      on-disk {on_disk_before:>8} -> {on_disk_after:>8} | copied {:>8} |                      {elapsed_us:>7} us | unlinked {:>2} / {:>8} B",
                    report.bytes_copied, report.dropped_segments, report.dropped_segment_bytes
                );
            }
        }
    }

    /// The shipped default ROLLS, and rolling is what lets reclaim unlink instead of rewrite.
    ///
    /// The previous version of this test claimed it would fail if rolling ever became the
    /// default. It would not have: it wrote 2,000 records of 64 bytes, a log of about 226 KB,
    /// which never reaches a 256 KiB threshold -- so it passed either way and proved nothing about
    /// the default it was named for. The log here is deliberately several times the threshold.
    ///
    /// Counted in bytes and pieces rather than timed. The scan saving that rolling also buys is
    /// real and larger in the drop-most shape, but it is a wall-clock number and belongs in
    /// `sweep_segment_sizes`, not in an assertion.
    ///
    /// **What these numbers are NOT.** They are per pass. Reclaim copies what it KEEPS, so a
    /// bigger log means a bigger copy for the same fraction freed -- but that does not compound
    /// over the log's life, because two things bound how big the log gets between passes:
    /// `DEFAULT_INDEX_DUMP_WAL_GAP_BYTES` is a megabyte, so the index dumps once the log runs that
    /// far ahead, and the storage-manager cycle that drives reclaim is wired (`bin/server.rs`
    /// submits it). The steady-state cost is bounded write amplification. The quadratic shape
    /// needs something to BLOCK reclaim so the log grows, and that case clamps to the floor rather
    /// than refusing at it.
    ///
    /// This paragraph is here because it was written once already, as a correction to a claim that
    /// the copy "grows with the log while the amount freed does not" -- a lifetime cost asserted
    /// from a per-pass measurement. Rewriting this test for the new default dropped the correction
    /// along with the test it was attached to. A cost per pass says nothing about total cost until
    /// you know what bounds the passes.
    #[test]
    fn the_shipped_default_rolls_the_log() {
        fn run(roll: Option<u64>, keep_fraction: f64) -> (u64, usize, u64, usize) {
            set_wal_segment_bytes_for_test(roll);
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            let records = 4000u64;
            for index in 0..records {
                store
                    .append(
                        1,
                        Command::StringSet {
                            key: format!("k{index:06}"),
                            value: vec![118u8; 256],
                        },
                    )
                    .unwrap();
            }
            let files = wal_segment_paths(dir.path(), 1).len();
            let report = store
                .gc_before_sequence_unchecked(1, (records as f64 * keep_fraction) as u64)
                .unwrap();
            set_wal_segment_bytes_for_test(None);
            (
                report.bytes_copied,
                report.dropped_segments,
                report.dropped_segment_bytes,
                files,
            )
        }

        // `Some(0)` is the old behaviour, still reachable: zero means never roll.
        let (never_copied, never_pieces, _, never_files) = run(Some(0), 0.90);
        let (default_copied, default_pieces, default_bytes, default_files) = run(None, 0.90);

        println!(
            "  never rolling: {never_files} file(s), copied {never_copied} B, unlinked {never_pieces}
               at the default: {default_files} file(s), copied {default_copied} B, unlinked              {default_pieces} piece(s) holding {default_bytes} B"
        );

        // A bound passes most easily when nothing was measured.
        assert!(never_copied > 0, "the probe must observe a reclaim");
        assert_eq!(never_files, 1, "zero must still mean one file");

        // The default rolls: the log is several pieces and reclaim drops whole ones.
        assert!(
            default_files > 1,
            "at a {}-byte threshold this log should be several pieces, got {default_files} file(s)",
            DEFAULT_WAL_SEGMENT_BYTES
        );
        assert!(
            default_pieces > 0,
            "reclaim should unlink whole pieces at the default, not rewrite: {default_pieces}"
        );
        assert!(
            default_bytes > 0,
            "the pieces it dropped should have held something: {default_bytes}"
        );

        // Where rolling reduces the COPY: keeping most of the log confines the rewrite to the
        // boundary piece. Dropping most of it copies the same either way -- the survivors are
        // small however the log is laid out -- so this shape is the one worth asserting on.
        let (never_keep_most, _, _, _) = run(Some(0), 0.10);
        let (default_keep_most, _, _, _) = run(None, 0.10);
        println!(
            "  keeping most: never rolling copied {never_keep_most} B, at the default              {default_keep_most} B"
        );
        assert!(
            default_keep_most * 4 < never_keep_most,
            "rolling should confine the rewrite to the boundary piece: {default_keep_most} vs              {never_keep_most}"
        );
    }

    /// What a batch costs on disk each way, as the batch grows.
    ///
    /// The comparison tree pays its record envelope once per COMMIT: its logger accumulates items
    /// and serialises all of them into a single stream append, so at N items per commit the
    /// envelope is amortised N ways. Written as N records ours is paid N times, and the three
    /// batch fields are added to every record on top.
    ///
    /// This measures both, so the amortisation is a number rather than an argument.
    #[test]
    #[ignore]
    fn what_a_batch_costs_each_way() {
        for items in [1usize, 8, 64] {
            let value_len = 64usize;

            let atomic_bytes = {
                let dir = tempfile::tempdir().unwrap();
                let store = LocalWriteAheadLogStore::new(dir.path());
                let path = write_ahead_log_path(dir.path(), 1);
                let commands = (0..items)
                    .map(|index| Command::StringSet {
                        key: format!("batch-key-{index:09}"),
                        value: vec![118u8; value_len],
                    })
                    .collect::<Vec<_>>();
                let (_, before) = last_wal_sequence_in(&path).unwrap_or((0, 0));
                store.append_batch_atomic(1, commands, false).unwrap();
                let (_, after) = last_wal_sequence_in(&path).unwrap();
                after - before
            };

            let one_record_bytes = {
                let dir = tempfile::tempdir().unwrap();
                let store = LocalWriteAheadLogStore::new(dir.path());
                let path = write_ahead_log_path(dir.path(), 1);
                let outcomes = (0..items)
                    .map(|index| WalOutcomeItem {
                        kind: "string".to_string(),
                        object_key: format!("batch-key-{index:09}"),
                        component: None,
                        object_id: index as u64,
                        routing_bucket: index as u32,
                        address: None,
                        value: Some(vec![118u8; value_len]),
                        ttl: None,
                        deleted: false,
                        meta: false,
                    })
                    .collect::<Vec<_>>();
                let (_, before) = last_wal_sequence_in(&path).unwrap_or((0, 0));
                store
                    .append_batch_as_one_record(1, outcomes, Vec::new(), false)
                    .unwrap();
                let (_, after) = last_wal_sequence_in(&path).unwrap();
                after - before
            };

            let payload = (items * value_len) as f64;
            println!(
                "  BATCH {items:>3} items | N records {atomic_bytes:>7} B ({:>5.2}x) | one record {one_record_bytes:>7} B ({:>5.2}x) | saved {:>5.1}%",
                atomic_bytes as f64 / payload,
                one_record_bytes as f64 / payload,
                100.0 * (atomic_bytes as f64 - one_record_bytes as f64) / atomic_bytes as f64,
            );

            // Both figures read as a saving if either side measured nothing.
            assert!(atomic_bytes > 0, "the N-record form wrote nothing at {items}");
            assert!(
                one_record_bytes > 0,
                "the one-record form wrote nothing at {items}"
            );
        }
    }

    /// What a record actually costs on disk AT THE DEFAULTS.
    ///
    /// The neighbouring size analysis builds its "today" baseline by hand, as JSON. Both encoding
    /// flags now default ON, so that hand-built baseline is not necessarily what the log writes,
    /// and a stale baseline overstates what is left -- which is the exact error that analysis
    /// warns about. This one asks the store instead, with no flags set.
    ///
    /// Measured as the DELTA between records rather than the file size: the log preallocates in
    /// large steps, so file size answers a different question.
    #[test]
    fn what_a_record_actually_costs_on_disk() {
        for value_len in [64usize, 1024, 4096] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            let value = vec![118u8; value_len];
            let path = write_ahead_log_path(dir.path(), 1);

            let append = |index: usize| {
                store
                    .append(
                        1,
                        Command::StringSet {
                            key: format!("scale-key-{index:09}"),
                            value: value.clone(),
                        },
                    )
                    .unwrap();
            };

            // Warm past any once-only header, then measure a run of appends.
            append(0);
            let (_, before) = last_wal_sequence_in(&path).unwrap();
            let runs = 16usize;
            for index in 1..=runs {
                append(index);
            }
            let (_, after) = last_wal_sequence_in(&path).unwrap();

            let per_record = (after - before) as f64 / runs as f64;
            println!(
                "  ONDISK value {value_len:>5}B -> {per_record:>7.1} B/record ({:>5.2}x)",
                per_record / value_len as f64
            );

            // A bound passes most easily when nothing was measured.
            assert!(
                after > before,
                "the probe must observe the appends at {value_len}B"
            );
            assert!(
                per_record > value_len as f64,
                "a record cannot be smaller than the value it carries: {per_record}"
            );

            // The envelope must stay in binary territory. The text fallback carries a ~122-byte
            // envelope and base64s the value at a flat third on top, so this bound fails wide if
            // either default ever flips back -- which is the drift that left the neighbouring
            // analysis describing a saving that had already been taken.
            let envelope = per_record - value_len as f64;
            assert!(
                envelope < 80.0,
                "envelope is {envelope:.0} B at {value_len}B: that is the text fallback's shape,                  not the binary one -- has a default flipped?"
            );
        }
    }

    /// What each frame costs in TIME, not just bytes. Ignored by default: it is a measurement,
    /// and a timing assertion in the suite would be a flake generator.
    ///
    ///   cargo test -p temporalstore-rust --lib what_each_frame_costs_in_time -- --ignored --nocapture
    #[test]
    #[ignore]
    fn what_each_frame_costs_in_time() {
        fn run(binary: bool, records: usize) -> (std::time::Duration, u64) {
            let previous = std::env::var("TS_WAL_BINARY_FRAME").ok();
            std::env::set_var("TS_WAL_BINARY_FRAME", if binary { "1" } else { "0" });
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            // A payload with the byte a delimited reader splits on, because escaping is what
            // the length frame stops paying for and it is charged per occurrence.
            let value: Vec<u8> = (0..512u32)
                .map(|i| if i % 16 == 0 { b'\n' } else { b'v' })
                .collect();
            let started = std::time::Instant::now();
            for index in 0..records {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("tenant/7/object/{index:06}"),
                            value: value.clone(),
                        },
                        false,
                    )
                    .unwrap();
            }
            let elapsed = started.elapsed();
            let path = write_ahead_log_path(dir.path(), 1);
            let (_, bytes) = last_wal_sequence_in(&path).unwrap();
            match previous {
                Some(value) => std::env::set_var("TS_WAL_BINARY_FRAME", value),
                None => std::env::remove_var("TS_WAL_BINARY_FRAME"),
            }
            (elapsed, bytes)
        }

        let records = 4_000usize;
        // Alternate the order so a cold page cache or a warming disk cannot be read as a win.
        let (text_a, text_bytes) = run(false, records);
        let (binary_a, binary_bytes) = run(true, records);
        let (binary_b, _) = run(true, records);
        let (text_b, _) = run(false, records);
        let text = (text_a + text_b) / 2;
        let binary = (binary_a + binary_b) / 2;

        let text_us = text.as_secs_f64() * 1e6 / records as f64;
        let binary_us = binary.as_secs_f64() * 1e6 / records as f64;
        println!(
            "  records {records}\n  \
             delimited  {text_us:.2} us/append, {} B/record\n  \
             length     {binary_us:.2} us/append, {} B/record\n  \
             time  {:+.1}%   bytes {:+.1}%",
            text_bytes / records as u64,
            binary_bytes / records as u64,
            100.0 * (binary_us - text_us) / text_us,
            100.0 * (binary_bytes as f64 - text_bytes as f64) / text_bytes as f64,
        );
    }
    use super::*;

    /// What the two frames actually cost, measured on the file rather than argued from the
    /// header sizes: same records, same store, one flag apart.
    #[test]
    fn what_each_frame_costs_on_disk() {
        fn write_and_measure(binary: bool, dir: &std::path::Path) -> (u64, usize) {
            let previous = std::env::var("TS_WAL_BINARY_FRAME").ok();
            std::env::set_var("TS_WAL_BINARY_FRAME", if binary { "1" } else { "0" });
            let store = LocalWriteAheadLogStore::new(dir);
            let records = 200usize;
            for index in 0..records {
                store
                    .append(
                        1,
                        Command::StringSet {
                            // A realistic key and a value with the byte a line reader splits on,
                            // because that byte is exactly what escaping charges for.
                            key: format!("tenant/9/object/{index:06}/field"),
                            value: format!("value-{index}\nsecond line\nthird").into_bytes(),
                        },
                    )
                    .unwrap();
            }
            let path = write_ahead_log_path(dir, 1);
            // Preallocated room is reservation, not records: measure what the records occupy.
            let (_, record_end) = last_wal_sequence_in(&path).unwrap();
            match previous {
                Some(value) => std::env::set_var("TS_WAL_BINARY_FRAME", value),
                None => std::env::remove_var("TS_WAL_BINARY_FRAME"),
            }
            (record_end, records)
        }

        let text_dir = tempfile::tempdir().unwrap();
        let binary_dir = tempfile::tempdir().unwrap();
        let (text_bytes, count) = write_and_measure(false, text_dir.path());
        let (binary_bytes, _) = write_and_measure(true, binary_dir.path());

        let text_per = text_bytes as f64 / count as f64;
        let binary_per = binary_bytes as f64 / count as f64;
        let saved = 100.0 * (text_per - binary_per) / text_per;
        println!(
            "  text   {text_bytes} bytes over {count} records = {text_per:.1} B/record\n  \
             binary {binary_bytes} bytes over {count} records = {binary_per:.1} B/record\n  \
             saved  {saved:.1}%"
        );
        assert!(
            binary_bytes < text_bytes,
            "the binary frame must not cost more: {binary_bytes} vs {text_bytes}"
        );
    }
    use crate::types::Command;

    #[test]
    fn default_store_scratch_dir_dies_with_the_last_clone() {
        let store = LocalWriteAheadLogStore::default();
        let root = store.inner.lock().unwrap().root.clone();
        assert!(root.exists(), "Default must create its scratch dir");
        let clone = store.clone();
        drop(store);
        assert!(root.exists(), "a live clone must keep the scratch dir");
        drop(clone);
        assert!(!root.exists(), "the last clone's drop must remove the scratch dir");
    }

    #[test]
    fn wal_interleaved_processes_refresh_sequence_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let first = LocalWriteAheadLogStore::new(dir.path());
        let second = LocalWriteAheadLogStore::new(dir.path());

        let one = first
            .append(
                1,
                Command::StringSet {
                    key: "first".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        let two = second
            .append(
                1,
                Command::StringSet {
                    key: "second".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        let three = first
            .append(
                1,
                Command::StringSet {
                    key: "third".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();

        assert_eq!(one.sequence, 1);
        assert_eq!(two.sequence, 2);
        assert_eq!(three.sequence, 3);
        assert_eq!(last_wal_sequence_at(dir.path(), 1).unwrap().0, 3);
    }

    #[test]
    fn wal_full_reclaim_retains_tail_so_sequence_does_not_regress_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for i in 0..5 {
            store
                .append(
                    1,
                    Command::StringSet {
                        key: format!("k{i}"),
                        value: b"v".to_vec(),
                    },
                )
                .unwrap();
        }
        assert_eq!(store.stats(1).last_sequence, 5);
        // Full reclaim: retain floor past the max sequence would empty the file.
        store.gc_before_sequence_unchecked(1, 6).unwrap();
        drop(store);
        // Restart: the sequence generator is seeded from the file. The tail record must have
        // survived so the next sequence is 6, not a regressed 1 (which would reuse a sequence
        // <= the durable anchor and be dropped by replay).
        let restarted = LocalWriteAheadLogStore::new(dir.path());
        let next = restarted
            .append(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(
            next.sequence, 6,
            "sequence must continue at 6 after a full reclaim + restart, not regress"
        );
    }

    #[test]
    fn wal_interior_corruption_is_fatal_not_silent_truncation() {
        // Pins the TEXT encoding: the corruption this injects is a line that fails to parse
        // as a document, which is a text-shaped fault by construction.
        std::env::set_var("TS_WAL_BINARY_RECORDS", "0");
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for i in 0..4 {
            store
                .append(
                    1,
                    Command::StringSet {
                        key: format!("k{i}"),
                        value: b"v".to_vec(),
                    },
                )
                .unwrap();
        }
        drop(store);
        // Corrupt the 2nd record IN PLACE, keeping it newline-terminated and leaving records
        // 3 & 4 intact after it. A newline-terminated line that fails to parse is committed
        // corruption, not a torn tail.
        let path = write_ahead_log_path(dir.path(), 1);
        // Read bytes and walk frames: a record is not a line once the frame declares its own
        // length, and `read_to_string` fails outright on a payload that is not valid UTF-8.
        let contents = std::fs::read(&path).unwrap();
        let mut spans: Vec<(usize, usize)> = Vec::new();
        let mut at = data_start(&contents);
        while at < contents.len() {
            // Preallocated room trails the records and is not one of them.
            if contents[at] == 0 {
                break;
            }
            match crate::log_framing::next_frame(&contents[at..]) {
                Ok(Some((consumed, _))) if consumed > 0 => {
                    spans.push((at, at + consumed));
                    at += consumed;
                }
                _ => break,
            }
        }
        assert_eq!(spans.len(), 4);
        // The damage this injects is a COMPLETE record that will not parse -- framed correctly,
        // checksum intact, contents not a record. That is committed corruption, distinct from a
        // torn tail, and the whole point of the test is that the two are handled differently.
        let mut corrupted = Vec::new();
        corrupted.extend_from_slice(&contents[..spans[0].1]);
        corrupted.extend_from_slice(&crate::log_framing::encode_record(b"corrupt-not-json"));
        corrupted.extend_from_slice(&contents[spans[2].0..spans[2].1]);
        corrupted.extend_from_slice(&contents[spans[3].0..spans[3].1]);
        std::fs::write(&path, &corrupted).unwrap();
        // scan drives last_wal_sequence_at, which must surface the interior corruption as an
        // error rather than silently truncating away records 3 & 4 (which would defeat the
        // strict replay-continuity DataLoss guard).
        let restarted = LocalWriteAheadLogStore::new(dir.path());
        assert!(
            restarted.scan(1, 0, u64::MAX, u64::MAX).is_err(),
            "interior WAL corruption must be fatal, not silently truncated to the last good record"
        );
    
        // Unpin it: this variable is process-global, and leaving it set makes every
        // test that runs after this one inherit an encoding it never asked for.
        std::env::remove_var("TS_WAL_BINARY_RECORDS");
    }

    #[test]
    fn framed_wal_bitflip_that_still_parses_json_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append(
                1,
                Command::StringSet {
                    key: "alpha".to_string(),
                    value: b"one".to_vec(),
                },
            )
            .unwrap();
        store
            .append(
                1,
                Command::StringSet {
                    key: "target".to_string(),
                    value: b"two".to_vec(),
                },
            )
            .unwrap();
        drop(store);
        // Flip one byte inside the second record's key ("target" -> "tbrget"): the framed line
        // stays VALID JSON but no longer matches its per-record SHA-256 digest. Without framing
        // this would replay as truth; with it, the committed corruption is fatal.
        let path = write_ahead_log_path(dir.path(), 1);
        let mut bytes = std::fs::read(&path).unwrap();
        let position = bytes
            .windows(6)
            .position(|window| window == b"target")
            .expect("second record contains the target key");
        bytes[position + 1] = b'b';
        std::fs::write(&path, &bytes).unwrap();
        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let scanned = reopened.scan(1, 0, u64::MAX, u64::MAX);
        match scanned {
            Err(WriteAheadLogError::Corruption(_)) => {}
            other => panic!("expected a Corruption error from a value-preserving bit-flip, got {other:?}"),
        }
    }

    #[test]
    fn legacy_unframed_wal_still_loads_after_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        // Simulate a pre-upgrade on-disk WAL: raw single-line JSON records, no framing.
        let path = write_ahead_log_path(dir.path(), 3);
        let make = |sequence: u64, key: &str| WriteAheadLogRecord {
            shard_id: 3,
            sequence,
            command: Some(Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            }),
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };
        let mut raw = serde_json::to_vec(&make(1, "k1")).unwrap();
        raw.push(b'\n');
        raw.extend_from_slice(&serde_json::to_vec(&make(2, "k2")).unwrap());
        raw.push(b'\n');
        std::fs::write(&path, &raw).unwrap();

        let store = LocalWriteAheadLogStore::new(dir.path());
        // The legacy (unframed) records still load: sequence tail + scan see both.
        assert_eq!(store.stats(3).last_sequence, 2);
        let rows = store.scan(3, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(decode_wal_line(&rows[0].1).unwrap().sequence, 1);
        assert_eq!(decode_wal_line(&rows[1].1).unwrap().sequence, 2);
        // A new append is framed and continues the sequence; the mixed file still loads.
        let appended = store
            .append(
                3,
                Command::StringSet {
                    key: "k3".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(appended.sequence, 3);
        let reopened = LocalWriteAheadLogStore::new(dir.path());
        assert_eq!(reopened.scan(3, 0, u64::MAX, u64::MAX).unwrap().len(), 3);
        assert_eq!(reopened.stats(3).last_sequence, 3);
    }

    // rust-internal: verifies Rust WAL alias exports remain wired to the local mutation log API.
    #[test]
    fn wal_aliases_cover_local_mutation_log_api() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWalStore::new(dir.path());
        let record: WalRecord = store
            .append(
                5,
                Command::StringSet {
                    key: "wal-key".to_string(),
                    value: b"wal-value".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(record.sequence, 1);
        let stats: WalStats = store.stats(5);
        assert_eq!(stats.last_sequence, 1);
        let gc: WalGcReport = store.gc_before_sequence_unchecked(5, 1).unwrap();
        assert_eq!(gc.records_removed, 0);
    }

    #[test]
    fn gc_before_sequence_rewrites_wal_with_retained_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for key in ["a", "b", "c"] {
            store
                .append(
                    7,
                    Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    },
                )
                .unwrap();
        }

        let report = store.gc_before_sequence_unchecked(7, 3).unwrap();
        assert_eq!(report.records_before, 3);
        assert_eq!(report.records_after, 1);
        assert_eq!(report.records_removed, 2);
        assert_eq!(store.stats(7).last_sequence, 3);
        let reopened = LocalWriteAheadLogStore::new(dir.path());
        assert_eq!(reopened.stats(7).last_sequence, 3);
        assert_eq!(reopened.scan(7, 0, u64::MAX, u64::MAX).unwrap().len(), 1);
        store
            .append(
                7,
                Command::StringSet {
                    key: "d".to_string(),
                    value: b"d".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(store.stats(7).last_sequence, 4);
    }

    #[test]
    fn corrupt_tail_is_truncated_and_append_resumes_after_last_valid_wal_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append(
                7,
                Command::StringSet {
                    key: "a".to_string(),
                    value: b"a".to_vec(),
                },
            )
            .unwrap();
        store
            .append(
                7,
                Command::StringSet {
                    key: "b".to_string(),
                    value: b"b".to_vec(),
                },
            )
            .unwrap();
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(write_ahead_log_path(dir.path(), 7))
                .unwrap();
            file.write_all(b"{\"shard_id\":7,\"sequence\":3").unwrap();
            file.sync_all().unwrap();
        }

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        assert_eq!(reopened.stats(7).last_sequence, 2);
        assert_eq!(reopened.scan(7, 0, u64::MAX, u64::MAX).unwrap().len(), 2);
        let record = reopened
            .append(
                7,
                Command::StringSet {
                    key: "c".to_string(),
                    value: b"c".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(record.sequence, 3);
        assert_eq!(reopened.scan(7, 0, u64::MAX, u64::MAX).unwrap().len(), 3);
    }

    // shared-corpus: storage_wal_structure_api_flush_parity
    #[test]
    fn wal_record_metadata_tracks_style_log_item_shape() {
        // The derivation is the contract: anything that wants the per-item description can
        // rebuild it from the command, which is why the record no longer stores it.
        let command = Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        };
        let item = WriteAheadLogItemMetadata::from_command(&command);
        assert_eq!(item.item_kind, WriteAheadLogItemKind::Kv);
        assert_eq!(item.model, WriteAheadLogModel::String);
        assert_eq!(item.object_key.as_deref(), Some("k"));
        assert!(!item.deleted);
        assert!(!item.meta_log);
        assert!(!item.block_log);

        // The record does not carry it -- 147 fsynced bytes per write saying what the record
        // already says.
        let lean = WriteAheadLogRecordMetadata::single_command(&command);
        assert!(
            lean.items.is_empty(),
            "the derived description should not be written by default"
        );
        let encoded = serde_json::to_string(&lean).unwrap();
        assert!(
            !encoded.contains("items"),
            "an empty description must be skipped entirely, got {encoded}"
        );

        // The other constructor restores it for a consumer reading records directly. It is
        // named rather than selected, so no environment can point the live path at it.
        let full = WriteAheadLogRecordMetadata::single_command_with_items(&command);
        assert_eq!(full.items.len(), 1);
        assert_eq!(full.items[0].object_key.as_deref(), Some("k"));
    }

    // shared-corpus: storage_wal_structure_api_flush_parity
    #[test]
    fn append_replayed_record_is_idempotent_and_flushes_like_stream_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let command = Command::HashSet {
            key: "h".to_string(),
            field: "f".to_string(),
            value: b"v".to_vec(),
        };
        let replayed = WriteAheadLogRecord {
            shard_id: 3,
            sequence: 8,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
            command: Some(command),
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };

        let first = store.append_replayed_record(replayed.clone()).unwrap();
        assert!(first.appended);
        assert_eq!(first.current_sequence, 8);
        assert!(first.size > 0);
        assert!(first.persistent_bytes >= first.size);
        let duplicate = store.append_replayed_record(replayed).unwrap();
        assert!(!duplicate.appended);
        assert_eq!(duplicate.current_sequence, 8);
        assert_eq!(store.scan(3, 0, u64::MAX, u64::MAX).unwrap().len(), 1);
        let stats = store.stats(3);
        assert_eq!(stats.last_sequence, 8);
        assert_eq!(stats.last_flushed_sequence, 8);
        assert!(stats.flushes >= 1);
        assert!(stats.syncs >= 1);
    }

    // shared-corpus: storage_wal_structure_api_flush_parity
    #[test]
    fn flush_and_info_report_persistent_wal_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append(
                4,
                Command::SetAdd {
                    key: "s".to_string(),
                    member: b"m1".to_vec(),
                },
            )
            .unwrap();
        store
            .append(
                4,
                Command::SetAdd {
                    key: "s".to_string(),
                    member: b"m2".to_vec(),
                },
            )
            .unwrap();

        let flush = store.flush(4).unwrap();
        assert!(flush.synced);
        assert_eq!(flush.last_sequence, 2);
        assert!(flush.persistent_bytes > 0);
        let info = store.info(4).unwrap();
        assert_eq!(info.start_sequence, 1);
        assert_eq!(info.current_sequence, 2);
        assert_eq!(info.records, 2);
        assert_eq!(info.persistent_length_bytes, flush.persistent_bytes);
        assert_eq!(info.format_version, WRITE_AHEAD_LOG_FORMAT_VERSION);
    }

    // ---- block-retention floor -------------------------------------------------------------

    /// Byte offset at which records begin: past the reclaim-base header if the file has one.
    ///
    /// Raw-file helpers have to step over the header for the same reason the readers do --
    /// decoding it as a record fails.
    fn data_start(bytes: &[u8]) -> usize {
        if bytes.starts_with(crate::log_framing::BASE_HEADER_MAGIC) {
            bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|index| index + 1)
                .unwrap_or(bytes.len())
        } else {
            0
        }
    }

    /// The file bytes with any trailing preallocated-zeros reservation stripped:
    /// exactly the records (a record line always ends in a newline, never a NUL).
    fn strip_reservation(mut bytes: Vec<u8>) -> Vec<u8> {
        while bytes.last() == Some(&0) {
            bytes.pop();
        }
        bytes
    }

    /// Decode the shard's WAL the way GC does, by frames, so a test can assert on what
    /// survived. Splitting on newlines answers correctly only while every record ends with one:
    /// a record that declares its own length carries 0x0A inside its payload, and the split
    /// then reports fragments that were never records. Asking each frame how far it runs works
    /// for a file holding either kind, which is what an upgrade or a reclaim rewrite leaves.
    fn sequences_on_disk(root: &std::path::Path, shard: ShardId) -> Vec<u64> {
        let bytes = std::fs::read(write_ahead_log_path(root, shard)).unwrap();
        let mut sequences = Vec::new();
        let mut at = data_start(&bytes);
        while at < bytes.len() {
            // A zero where a record should start is preallocated room, not a record.
            if bytes[at] == 0 {
                break;
            }
            match crate::log_framing::next_frame(&bytes[at..]) {
                Ok(Some((consumed, _))) if consumed > 0 => {
                    let raw = &bytes[at..at + consumed];
                    if !raw.iter().all(|byte| byte.is_ascii_whitespace()) {
                        sequences.push(decode_wal_line(raw).unwrap().sequence);
                    }
                    at += consumed;
                }
                _ => break,
            }
        }
        sequences
    }

    /// Byte offset at which each record starts, in order, past any base header.
    /// Where each record starts. Walk the frames, do not hunt for newlines: a record ends where
    /// its frame says it does, and a length-framed payload carries 0x0A of its own, so counting
    /// newlines reports boundaries that were never boundaries. Works for either frame, which is
    /// what a log written across a format change actually contains.
    fn record_offsets_on_disk(root: &std::path::Path, shard: ShardId) -> Vec<usize> {
        let bytes = std::fs::read(write_ahead_log_path(root, shard)).unwrap();
        let start = data_start(&bytes);
        let mut offsets = Vec::new();
        let mut at = start;
        while at < bytes.len() {
            // Preallocated room ends the records: nothing starts inside the zeros.
            if bytes[at] == 0 {
                break;
            }
            match crate::log_framing::next_frame(&bytes[at..]) {
                Ok(Some((consumed, _))) if consumed > 0 => {
                    offsets.push(at);
                    at += consumed;
                }
                _ => break,
            }
        }
        if offsets.is_empty() {
            offsets.push(start);
        }
        offsets
    }

    /// Bytes of the record starting at `offset`, including its newline.
    fn record_bytes_at(root: &std::path::Path, shard: ShardId, offset: usize) -> Vec<u8> {
        let bytes = std::fs::read(write_ahead_log_path(root, shard)).unwrap();
        // The frame says how far the record runs; a newline only marks the end of one kind.
        let end = match crate::log_framing::next_frame(&bytes[offset..]) {
            Ok(Some((consumed, _))) if consumed > 0 => offset + consumed,
            _ => bytes.len(),
        };
        bytes[offset..end].to_vec()
    }

    fn append_n(store: &LocalWriteAheadLogStore, shard: ShardId, count: usize) {
        for index in 0..count {
            store
                .append(
                    shard,
                    Command::StringSet {
                        key: format!("k{index}"),
                        value: b"v".to_vec(),
                    },
                )
                .unwrap();
        }
    }

    #[test]
    fn an_unsynced_append_is_not_reported_as_durable() {
        // The whole point of a persistent-length figure is "what survives a crash". An append
        // puts its record in the file whether or not a barrier followed, so reading the file's
        // length back as `persistent_length_bytes` answers a different question than the one
        // asked -- and answers it in the dangerous direction.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();

        let info = store.info(1).unwrap();
        assert!(info.length_bytes > 0, "the record was written");
        assert_eq!(
            info.persistent_length_bytes, 0,
            "nothing has been synced, so nothing is durable"
        );
        assert_eq!(
            info.last_flushed_sequence, 0,
            "no barrier has covered any sequence yet"
        );
        assert!(info.current_sequence > info.last_flushed_sequence);

        // After a barrier the two agree.
        store.flush(1).unwrap();
        let synced = store.info(1).unwrap();
        assert_eq!(synced.persistent_length_bytes, synced.length_bytes);
        assert_eq!(synced.last_flushed_sequence, synced.current_sequence);
    }

    #[test]
    fn one_shards_barrier_is_not_reported_as_another_shards() {
        // `stats` is one struct for the whole store, so a barrier on shard 1 used to raise the
        // durability reported for shard 2, which never synced at all.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append_with_sync(
                2,
                Command::StringSet {
                    key: "untouched".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "synced".to_string(),
                    value: b"v".to_vec(),
                },
                true,
            )
            .unwrap();

        assert!(
            store.info(1).unwrap().persistent_length_bytes > 0,
            "shard 1 synced"
        );
        assert_eq!(
            store.info(2).unwrap().persistent_length_bytes,
            0,
            "shard 2 never synced; another shard's barrier is not its own"
        );
        assert_eq!(store.info(2).unwrap().last_flushed_sequence, 0);
    }

    #[test]
    fn the_log_length_counts_every_piece_not_just_the_newest() {
        // After a roll the active piece is the SMALLEST one. Reporting its length as the log's
        // makes the log appear to shrink as it grows, and hides every sealed byte from anyone
        // deciding whether to compact.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        set_wal_segment_bytes_for_test(Some(256));
        append_n(&store, 1, 40);
        set_wal_segment_bytes_for_test(None);

        let segments = wal_segment_paths(dir.path(), 1);
        assert!(
            segments.len() > 1,
            "the test needs a roll to have happened, got {segments:?}"
        );
        let active_len = write_ahead_log_path(dir.path(), 1).metadata().unwrap().len();
        let total: u64 = segments
            .iter()
            .filter_map(|path| path.metadata().ok())
            .map(|metadata| metadata.len())
            .sum();

        let info = store.info(1).unwrap();
        assert_eq!(info.length_bytes, total, "every piece counts");
        assert!(
            info.length_bytes > active_len,
            "the sealed pieces were being dropped from the total"
        );
    }

    #[test]
    fn gc_is_unconstrained_when_no_block_retention_floor_is_registered() {
        // The floor is opt-in: a caller that puts no blocks in the WAL must see the reclaim
        // behaviour it had before the floor existed.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        assert_eq!(store.block_retention_floor(1), None);
        let report = store.gc_before_sequence_unchecked(1, 4).unwrap();

        assert_eq!(report.records_before, 6);
        assert_eq!(report.records_after, 3, "sequences 4, 5, 6 survive");
        assert!(!report.clamped_by_block_retention);
        assert_eq!(report.effective_retain_from_sequence, 4);
    }

    #[test]
    fn gc_will_not_reclaim_past_the_durable_index_anchor() {
        // The durable served index reflects sequences 1..=3. Asking to drop everything below 6
        // would delete records 4 and 5, whose effects survive only in an index write whose
        // barrier is still deferred -- a crash there turns acked writes into missing ones.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        let durable_index = DurableIndexAnchor::proven_durable_through(1, 3);
        let report = store.gc_before_sequence(1, 6, &durable_index).unwrap();

        assert!(report.clamped_by_durable_index);
        assert_eq!(
            report.retain_from_sequence, 6,
            "the report states the sequence the caller ASKED for"
        );
        assert_eq!(
            report.effective_retain_from_sequence, 4,
            "narrowed to one past what the durable index proves"
        );
        assert_eq!(report.records_after, 3, "sequences 4, 5 and 6 survive");
    }

    #[test]
    fn an_anchor_minted_for_another_shard_authorizes_no_reclaim() {
        // Durability is per shard. An anchor that proves shard 2 is dumped through sequence 6
        // says nothing whatever about shard 1, and must not be read as though it did.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        let durable_index = DurableIndexAnchor::proven_durable_through(2, 6);
        let report = store.gc_before_sequence(1, 6, &durable_index).unwrap();

        assert!(report.clamped_by_durable_index);
        assert_eq!(report.records_removed, 0);
        assert_eq!(report.records_after, 6, "every record survives");
    }

    #[test]
    fn an_unproven_anchor_reclaims_exactly_as_the_bare_primitive_does() {
        // What the measurement harnesses and the never-dumped operator path use: no proof, so
        // no clamp, and the same outcome the unanchored primitive gives for the same ask.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        let durable_index = DurableIndexAnchor::unproven(1);
        let report = store.gc_before_sequence(1, 4, &durable_index).unwrap();

        assert!(!report.clamped_by_durable_index);
        assert_eq!(report.effective_retain_from_sequence, 4);
        assert_eq!(report.records_after, 3, "sequences 4, 5 and 6 survive");
    }

    #[test]
    fn gc_will_not_reclaim_past_the_block_retention_floor() {
        // Records at or above the floor may hold the only copy of a block's bytes. Reclaiming
        // them destroys data the served index still points at.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        store.set_block_retention_floor(1, 3);
        let report = store.gc_before_sequence_unchecked(1, 6).unwrap();

        assert!(
            report.clamped_by_block_retention,
            "the reclaim asked to go past the floor and must report being held back"
        );
        assert_eq!(report.effective_retain_from_sequence, 3);
        assert_eq!(report.records_after, 4, "sequences 3..=6 are retained");

        // The retained records are the ones at and above the floor, in order.
        assert_eq!(sequences_on_disk(dir.path(), 1), vec![3, 4, 5, 6]);
    }

    #[test]
    fn advancing_the_floor_lets_the_held_back_records_go() {
        // The floor is the dump watermark: as blocks are dumped into bands the WAL stops being
        // their only copy, and the records become reclaimable.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        store.set_block_retention_floor(1, 2);
        assert_eq!(store.gc_before_sequence_unchecked(1, 6).unwrap().records_after, 5);

        store.set_block_retention_floor(1, 5);
        let report = store.gc_before_sequence_unchecked(1, 6).unwrap();
        assert_eq!(report.effective_retain_from_sequence, 5);
        assert_eq!(report.records_after, 2, "sequences 5 and 6");

        store.clear_block_retention_floor(1);
        assert_eq!(store.block_retention_floor(1), None);
        let report = store.gc_before_sequence_unchecked(1, 6).unwrap();
        assert!(!report.clamped_by_block_retention);
        assert_eq!(report.records_after, 1, "the tail record is always kept");
    }

    #[test]
    fn the_floor_never_overrides_the_tail_retention_rule() {
        // Clamping must compose with the existing rule that the highest-sequence record is
        // never removed -- the WAL is the sequence generator on restart, so emptying it would
        // restart sequencing at 1 and silently drop the re-used sequences on replay.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 3);

        store.set_block_retention_floor(1, u64::MAX);
        let report = store.gc_before_sequence_unchecked(1, u64::MAX).unwrap();

        assert_eq!(report.records_after, 1, "the tail survives both clamps");
        assert_eq!(report.effective_retain_from_sequence, 3);
    }

    #[test]
    fn reclaim_shifts_physical_offsets_but_log_ids_stay_stable() {
        // Reclaim still compacts, so where a record physically sits does change. What must not
        // change is its log id -- its offset in the log's whole history -- because that is the
        // address a block carries. Resolving a log id after a reclaim has to land on the same
        // record it named before.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        let offsets_before = record_offsets_on_disk(dir.path(), 1);
        assert_eq!(sequences_on_disk(dir.path(), 1), vec![1, 2, 3, 4, 5, 6]);
        // Nothing reclaimed yet, so a log id is simply the physical offset.
        assert_eq!(store.base_offset(1).unwrap(), 0);
        let tail_physical_before = *offsets_before.last().unwrap() as u64;
        let tail_log_id = store.log_id_at(1, tail_physical_before).unwrap();
        assert_eq!(tail_log_id, tail_physical_before);
        let tail_bytes_before = record_bytes_at(dir.path(), 1, tail_physical_before as usize);

        store.gc_before_sequence_unchecked(1, 4).unwrap();

        // The record moved.
        let tail_physical_after = *record_offsets_on_disk(dir.path(), 1).last().unwrap() as u64;
        assert_ne!(
            tail_physical_before, tail_physical_after,
            "reclaim compacts, so the physical position is expected to change"
        );

        // Its log id still names it.
        let resolved = store
            .resolve_log_id(1, tail_log_id)
            .unwrap()
            .expect("a retained log id must still resolve");
        assert_eq!(
            resolved, tail_physical_after,
            "the log id must resolve to where the record now lives"
        );
        let tail_bytes_after = record_bytes_at(dir.path(), 1, resolved as usize);
        assert_eq!(
            tail_bytes_before, tail_bytes_after,
            "the log id must name the same record, byte for byte"
        );
    }

    #[test]
    fn a_reclaimed_log_id_resolves_to_nothing_rather_than_the_wrong_record() {
        // The dangerous failure is silent: an offset below the base would otherwise land in the
        // middle of the file and parse as some other record.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);
        let first_log_id = record_offsets_on_disk(dir.path(), 1)[0] as u64;

        store.gc_before_sequence_unchecked(1, 4).unwrap();

        assert!(store.base_offset(1).unwrap() > first_log_id);
        assert_eq!(
            store.resolve_log_id(1, first_log_id).unwrap(),
            None,
            "a reclaimed log id must resolve to nothing"
        );
    }

    #[test]
    fn retained_records_are_copied_verbatim() {
        // The offset arithmetic assumes a survivor's bytes and length are untouched. Decoding
        // and re-encoding could change either, so reclaim copies the range as-is.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        let raw_before =
            strip_reservation(std::fs::read(write_ahead_log_path(dir.path(), 1)).unwrap());
        let offsets = record_offsets_on_disk(dir.path(), 1);
        // Reclaiming from sequence 4 keeps records from index 3 onward.
        let split = offsets[3];
        let suffix_before = raw_before[split..].to_vec();

        store.gc_before_sequence_unchecked(1, 4).unwrap();

        let raw_after = std::fs::read(write_ahead_log_path(dir.path(), 1)).unwrap();
        let header_len = raw_after
            .iter()
            .position(|byte| *byte == b'\n')
            .expect("a reclaimed file carries a base header")
            + 1;
        assert_eq!(
            &raw_after[header_len..],
            suffix_before.as_slice(),
            "retained bytes must survive reclaim unchanged"
        );
    }

    #[test]
    fn the_base_accumulates_across_repeated_reclaims() {
        // Each reclaim shifts survivors again; the base is the running total, not the last hop.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 12);

        let all_offsets = record_offsets_on_disk(dir.path(), 1);
        let last_log_id = *all_offsets.last().unwrap() as u64;
        let last_bytes = record_bytes_at(dir.path(), 1, last_log_id as usize);

        store.gc_before_sequence_unchecked(1, 4).unwrap();
        let base_after_first = store.base_offset(1).unwrap();
        store.gc_before_sequence_unchecked(1, 9).unwrap();
        let base_after_second = store.base_offset(1).unwrap();

        assert!(
            base_after_second > base_after_first,
            "the base must keep climbing, got {base_after_first} then {base_after_second}"
        );

        // The surviving tail is still reachable by the log id it had at the very beginning.
        let resolved = store
            .resolve_log_id(1, last_log_id)
            .unwrap()
            .expect("the tail survives both reclaims");
        assert_eq!(record_bytes_at(dir.path(), 1, resolved as usize), last_bytes);
    }

    #[test]
    fn a_log_that_was_never_reclaimed_reads_as_base_zero() {
        // Existing files carry no header, and must keep loading unchanged.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 3);

        assert_eq!(store.base_offset(1).unwrap(), 0);
        let raw = std::fs::read(write_ahead_log_path(dir.path(), 1)).unwrap();
        assert!(
            !raw.starts_with(b"#tsb1 "),
            "an un-reclaimed log must not grow a header it does not need"
        );
        assert_eq!(sequences_on_disk(dir.path(), 1), vec![1, 2, 3]);
        assert_eq!(store.info(1).unwrap().records, 3);
    }

    #[test]
    fn readers_keep_working_after_a_reclaim() {
        // The header sits where readers expect the first record. Every path that walks the file
        // has to step over it, or it decodes as a corrupt record and surfaces as data loss.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 8);
        store.gc_before_sequence_unchecked(1, 5).unwrap();

        // scan(), which recovery and checkpoint publishing both read through
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(scanned.len(), 4, "sequences 5..=8 survive");
        for (_, line) in &scanned {
            decode_wal_line(line).expect("no scanned line may be the header");
        }

        // info()
        let info = store.info(1).unwrap();
        assert_eq!(info.records, 4);
        assert_eq!(info.current_sequence, 8);

        // the sequence cache rebuilt from disk, which drives the next append
        let store_reopened = LocalWriteAheadLogStore::new(dir.path());
        let report = store_reopened
            .append(
                1,
                Command::StringSet {
                    key: "after-reclaim".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(
            report.sequence, 9,
            "sequencing must continue past the reclaim, not restart"
        );
    }

    #[test]
    fn an_append_reports_the_log_id_its_record_landed_at() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let mut reported = Vec::new();
        for index in 0..5 {
            let (record, log_id) = store
                .append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index}"),
                        value: b"v".to_vec(),
                    },
                    true,
                )
                .unwrap();
            reported.push((log_id, record.sequence));
        }

        // Nothing reclaimed yet, so each reported id is where the record physically sits.
        let offsets = record_offsets_on_disk(dir.path(), 1);
        for (index, (log_id, _)) in reported.iter().enumerate() {
            assert_eq!(*log_id, offsets[index] as u64);
        }

        // And each one reads back as the record it named.
        for (log_id, sequence) in &reported {
            let bytes = store
                .read_at_log_id(1, *log_id, 4096)
                .unwrap()
                .expect("a live log id must resolve");
            let (consumed, _) = crate::log_framing::next_frame(&bytes)
                .unwrap()
                .expect("a live log id must name a whole record");
            let line = &bytes[..consumed];
            assert_eq!(decode_wal_line(line).unwrap().sequence, *sequence);
        }
    }

    #[test]
    fn a_reported_log_id_still_names_its_record_after_a_reclaim() {
        // This is the whole point of reporting a log id rather than a file offset: the record
        // moves, and the id has to keep naming it.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let mut reported = Vec::new();
        for index in 0..8 {
            let (record, log_id) = store
                .append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index}"),
                        value: b"v".to_vec(),
                    },
                    true,
                )
                .unwrap();
            reported.push((log_id, record.sequence));
        }

        store.gc_before_sequence_unchecked(1, 5).unwrap();
        let base = store.base_offset(1).unwrap();
        assert!(base > 0, "the reclaim must have moved the base");

        for (log_id, sequence) in &reported {
            match store.read_at_log_id(1, *log_id, 4096).unwrap() {
                Some(bytes) => {
                    let (consumed, _) = crate::log_framing::next_frame(&bytes)
                .unwrap()
                .expect("a live log id must name a whole record");
            let line = &bytes[..consumed];
                    assert_eq!(
                        decode_wal_line(line).unwrap().sequence,
                        *sequence,
                        "a surviving log id must still name its own record"
                    );
                }
                None => assert!(
                    *log_id < base,
                    "only a reclaimed log id may fail to resolve, but {log_id} is at or above {base}"
                ),
            }
        }
    }

    #[test]
    fn log_ids_reported_after_a_reclaim_account_for_the_base() {
        // The base is cached on the write path; a stale cache would report ids that are wrong by
        // exactly the reclaimed prefix -- and they would still resolve, to the wrong record.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);
        store.gc_before_sequence_unchecked(1, 4).unwrap();
        let base = store.base_offset(1).unwrap();
        assert!(base > 0);

        let (record, log_id) = store
            .append_with_sync_reporting(
                1,
                Command::StringSet {
                    key: "after-reclaim".to_string(),
                    value: b"v".to_vec(),
                },
                true,
            )
            .unwrap();
        assert!(
            log_id >= base,
            "a record appended after a reclaim cannot have a log id below the base"
        );
        let bytes = store
            .read_at_log_id(1, log_id, 4096)
            .unwrap()
            .expect("the just-appended record must resolve");
        let (consumed, _) = crate::log_framing::next_frame(&bytes)
                .unwrap()
                .expect("a live log id must name a whole record");
            let line = &bytes[..consumed];
        assert_eq!(decode_wal_line(line).unwrap().sequence, record.sequence);
    }

    #[test]
    fn a_staged_page_costs_about_a_third_over_its_contents() {
        // A byte vector serializes as an array of numbers -- about 5 bytes of log per byte of
        // page -- which is what kept this from being on by default. Encoded, the record should
        // be close to the page it carries.
        let page = vec![b'x'; 4096];
        let record = WriteAheadLogRecord {
            shard_id: 1,
            sequence: 1,
            command: Some(Command::StringSet {
                key: "k".to_string(),
                value: Vec::new(),
            }),
            metadata: None,
            staged_pages: vec![StagedPage {
                object_id: 7,
                bytes: page.clone(),
            }],
            outcomes: Vec::new(),
        };
        let encoded = serde_json::to_vec(&record).unwrap();
        let overhead = encoded.len() as f64 / page.len() as f64;
        assert!(
            overhead < 1.6,
            "a staged page should cost about a third over its contents, got {overhead:.2}x              ({} bytes of record for {} bytes of page)",
            encoded.len(),
            page.len()
        );

        // And it round-trips.
        let decoded: WriteAheadLogRecord = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.staged_pages[0].bytes, page);
    }

    #[test]
    fn a_record_written_with_the_array_shape_still_loads() {
        // Records written before the encoding change must keep loading, or a log written by an
        // earlier build becomes unreadable.
        let json = br#"{"shard_id":1,"sequence":2,"command":{"kind":"string_set","key":"k","value":[]},"staged_pages":[{"object_id":9,"bytes":[104,105]}]}"#;
        let decoded: WriteAheadLogRecord = serde_json::from_slice(json).unwrap();
        assert_eq!(decoded.staged_pages[0].object_id, 9);
        assert_eq!(decoded.staged_pages[0].bytes, b"hi".to_vec());
    }

    #[test]
    fn a_record_with_no_staged_page_is_unchanged_on_disk() {
        // The gate-off path must serialize exactly as it did before staging existed.
        let record = WriteAheadLogRecord {
            shard_id: 1,
            sequence: 3,
            command: Some(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            }),
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };
        let encoded = String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap();
        assert!(
            !encoded.contains("staged_pages"),
            "an empty staged list must be skipped entirely, got {encoded}"
        );
    }

    /// A pass that must rewrite a large log to free a sliver of it is declined.
    ///
    /// Reclaim copies what it KEEPS, so its cost tracks the retained size. The case this exists
    /// for is measured: one pass copied 19.6 MB of survivors to free 3.8 MB, which is 19.4% --
    /// under the 25% default, so it is declined and the prefix is left to grow.
    #[test]
    fn reclaim_declines_a_rewrite_that_frees_too_little() {
        const MB: u64 = 1024 * 1024;
        let floor = 8 * MB;

        // The measured case: 3.8 MB freed for a 19.6 MB copy.
        assert!(!reclaim_is_worth_rewriting(3_800_000, 19_600_000, floor, 25));
        // Exactly at the threshold is worth running -- 25% of 20 MB is 5 MB.
        assert!(reclaim_is_worth_rewriting(5 * MB, 20 * MB, floor, 25));
        // A hair under is not.
        assert!(!reclaim_is_worth_rewriting(5 * MB - 1, 20 * MB, floor, 25));
        // Freeing nothing never justifies a rewrite.
        assert!(!reclaim_is_worth_rewriting(0, 20 * MB, floor, 25));
    }

    /// Below the copy floor a pass always runs, so small logs reclaim exactly as they did before
    /// the guard existed. This is what keeps the change inert for the whole existing suite.
    #[test]
    fn reclaim_below_the_copy_floor_always_runs() {
        const MB: u64 = 1024 * 1024;
        let floor = 64 * MB;

        // A ratio that would be declined on a large log proceeds on a small one.
        assert!(reclaim_is_worth_rewriting(1, 32 * MB, floor, 25));
        assert!(reclaim_is_worth_rewriting(0, 0, floor, 25));
        // Once the copy exceeds the floor, the ratio governs again.
        assert!(!reclaim_is_worth_rewriting(1, 64 * MB + 1, floor, 25));
    }

    /// The condition is self-correcting: a declined pass becomes worthwhile as the prefix grows,
    /// so declining does not let the log grow without bound.
    #[test]
    fn a_declined_reclaim_becomes_worthwhile_as_the_prefix_grows() {
        const MB: u64 = 1024 * 1024;
        let floor = 8 * MB;
        let retained = 100 * MB;

        assert!(!reclaim_is_worth_rewriting(5 * MB, retained, floor, 25));
        assert!(!reclaim_is_worth_rewriting(20 * MB, retained, floor, 25));
        // The prefix has grown past a quarter of what the pass must copy.
        assert!(reclaim_is_worth_rewriting(25 * MB, retained, floor, 25));
    }

    /// What is left in a small record once the payload stops being the problem.
    ///
    /// Prints one real record's bytes and attributes them, so the next decision about the format
    /// is made against a breakdown rather than an impression.
    #[test]
    fn small_record_byte_breakdown() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "bench-key-00000000".to_string(),
                    value: vec![118u8; 64],
                },
                false,
            )
            .unwrap();
        let raw = std::fs::read(write_ahead_log_path(dir.path(), 1)).unwrap();
        let line = String::from_utf8_lossy(&raw);
        let line = line.trim_end();
        println!("  whole record ({} B):", line.len() + 1);
        println!("    {line}");

        let payload_start = line
            .match_indices(' ')
            .nth(2)
            .map(|(index, _)| index + 1)
            .unwrap_or(0);
        let frame = payload_start + 1; // + the newline
        let payload = &line[payload_start..];
        let value_chars = payload
            .split("\"value\":\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .map(str::len)
            .unwrap_or(0);
        let metadata = payload
            .find("\"m\":")
            .map(|start| payload[start..].len() - 1)
            .unwrap_or(0);
        let key_chars = "bench-key-00000000".len();
        let structure = payload.len() - value_chars - metadata - key_chars;

        println!("    frame + newline : {frame:>4} B");
        println!("    field names etc : {structure:>4} B");
        println!("    the key         : {key_chars:>4} B");
        println!("    the value       : {value_chars:>4} B  (64 B of data)");
        println!("    metadata block  : {metadata:>4} B");
        println!("    total           : {:>4} B for 64 B of user data", line.len() + 1);
    }


    /// A record written with the long field names still loads.
    ///
    /// The rename is only safe because every field keeps an alias for what it was called, and
    /// nothing rewrites logs already on disk. This is that promise, in the shape the previous code
    /// actually wrote -- including the format version, which new records leave out.
    #[test]
    fn a_record_with_the_long_field_names_still_loads() {
        let written_before = concat!(
            r#"{"shard_id":1,"sequence":7,"command":{"kind":"string_set","key":"k","#,
            r#""value":[104,105]},"metadata":{"version":1,"timestamp_ms":1787429651961}}"#
        );
        let record: WriteAheadLogRecord =
            serde_json::from_str(written_before).expect("the long field names must still load");

        assert_eq!(record.shard_id, 1);
        assert_eq!(record.sequence, 7);
        assert_eq!(
            record.command,
            Some(Command::StringSet {
                key: "k".to_string(),
                value: b"hi".to_vec(),
            })
        );
        let metadata = record.metadata.expect("the metadata block should load");
        assert_eq!(metadata.version, WRITE_AHEAD_LOG_FORMAT_VERSION);
        assert_eq!(metadata.timestamp_ms, 1_787_429_651_961);
    }

    /// A record that omits the version is read as the current one.
    ///
    /// New records leave it out precisely because that is what its absence means; if the default
    /// were zero instead, every new record would read back claiming an unknown format.
    #[test]
    fn an_omitted_version_reads_as_the_current_one() {
        let record: WriteAheadLogRecord = serde_json::from_str(
            r#"{"s":1,"q":2,"c":{"kind":"string_get","key":"k"},"m":{"t":17}}"#,
        )
        .expect("a record without a version must load");
        assert_eq!(
            record.metadata.expect("metadata").version,
            WRITE_AHEAD_LOG_FORMAT_VERSION,
            "a record that does not name a version is the current one"
        );
    }

    /// The short names really are what gets written, and a written record reads back unchanged.
    #[test]
    fn a_written_record_round_trips_through_the_short_names() {
        let record = WriteAheadLogRecord {
            shard_id: 3,
            sequence: 11,
            command: Some(Command::StringSet {
                key: "round-trip".to_string(),
                value: vec![0u8, 127, 255],
            }),
            metadata: Some(WriteAheadLogRecordMetadata {
                version: WRITE_AHEAD_LOG_FORMAT_VERSION,
                timestamp_ms: 42,
                items: Vec::new(),
                batch_id: None,
                batch_size: None,
                batch_index: None,
            }),
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };
        let encoded = serde_json::to_string(&record).unwrap();
        assert!(
            !encoded.contains("shard_id") && !encoded.contains("timestamp_ms"),
            "the long names should not be written any more, got {encoded}"
        );
        assert!(
            !encoded.contains("version"),
            "the current version should be left out, got {encoded}"
        );
        let decoded: WriteAheadLogRecord = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, record, "a record must survive its own encoding");
    }

    /// Barriers taken per durable write, as concurrency rises.
    ///
    /// A durable write costs an fsync, and the fsync dominates it -- so the lever on throughput is
    /// how many writes share one barrier. A ratio near 1.0 means every writer paid for its own; the
    /// lower it goes, the better concurrent writers are being coalesced.
    #[test]
    fn durable_write_barriers_per_write_under_concurrency() {
        use std::sync::Arc;

        for writers in [1usize, 2, 4, 8, 16] {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalWriteAheadLogStore::new(dir.path()));
            let per_writer = 40u64;
            let started = std::time::Instant::now();
            let mut handles = Vec::new();
            for writer in 0..writers {
                let store = Arc::clone(&store);
                handles.push(std::thread::spawn(move || {
                    for index in 0..per_writer {
                        store
                            .append_with_sync(
                                1,
                                Command::StringSet {
                                    key: format!("w{writer}-{index:06}"),
                                    value: vec![118u8; 128],
                                },
                                true,
                            )
                            .unwrap();
                    }
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
            let elapsed = started.elapsed();
            let total = writers as u64 * per_writer;
            let stats = store.raw_stats(1);
            let per_write = stats.syncs as f64 / total as f64;
            println!(
                "  {writers:>2} writers: {total:>4} writes, {:>4} barriers = {per_write:.2} per write, {:>7.0} writes/s, {:>7.1} us/write",
                stats.syncs,
                total as f64 / elapsed.as_secs_f64(),
                elapsed.as_secs_f64() * 1e6 / total as f64
            );

            // One barrier per write is the ceiling -- more than that would mean a durable write
            // paid for someone else's fsync as well as its own.
            assert!(
                stats.syncs <= total,
                "{writers} writers took {} barriers for {total} writes",
                stats.syncs
            );
            if writers >= 8 {
                // Measured at 0.20 and 0.17 on an idle machine; this only has to catch coalescing
                // being switched off or broken, which shows up as 1.00 at every concurrency.
                assert!(
                    per_write < 0.9,
                    "concurrent writers should share barriers, but {writers} writers took \
                     {per_write:.2} per write -- that is one each, so nothing is being coalesced"
                );
            }
        }
    }

    /// How long it takes to find the end of the log, as the log grows.
    ///
    /// Learning the last sequence is what a restart needs before it can append. Today that reads
    /// and decodes every record, so the cost tracks the size of the whole log rather than the size
    /// of its tail -- and a log is not bounded by how much of it is interesting.
    #[test]
    fn finding_the_end_of_the_log_as_it_grows() {
        for records in [1_000u64, 5_000, 20_000] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            for index in 0..records {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("key-{index:08}"),
                            value: vec![118u8; 128],
                        },
                        false,
                    )
                    .unwrap();
            }
            let bytes = std::fs::metadata(write_ahead_log_path(dir.path(), 1))
                .unwrap()
                .len();

            let started = std::time::Instant::now();
            let rounds = 5;
            let mut last = 0;
            for _ in 0..rounds {
                last = last_wal_sequence_at(dir.path(), 1).unwrap().0;
            }
            let micros = started.elapsed().as_secs_f64() * 1e6 / rounds as f64;
            assert_eq!(last, records, "it should find the real end");
            println!(
                "  {records:>6} records ({:>8} B): {micros:>9.0} us to find the end  ({:.2} us per 1k records)",
                bytes,
                micros / (records as f64 / 1000.0)
            );
        }
    }

    /// A record larger than the first tail window is still found.
    ///
    /// The search starts with a 64 KiB window and widens until it holds a whole record. Nothing
    /// else in the suite writes a record big enough to need that, so this is the test for it.
    #[test]
    fn the_end_is_found_even_when_the_last_record_is_huge() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "small".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        // Comfortably past the first window, and past the second as well once encoded.
        store
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "huge".to_string(),
                    value: vec![118u8; 400 * 1024],
                },
                false,
            )
            .unwrap();
        drop(store);

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let next = reopened
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        assert_eq!(
            next.sequence, 3,
            "the sequence must continue after a record wider than the search window"
        );
    }

    /// A trailing write that never finished is cut, and nothing above it is.
    #[test]
    fn a_torn_trailing_write_is_cut_back_to_the_last_whole_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in 0..3 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index}"),
                        value: b"v".to_vec(),
                    },
                    false,
                )
                .unwrap();
        }
        drop(store);

        // A write that stopped partway: bytes with no newline after them.
        let path = write_ahead_log_path(dir.path(), 1);
        let whole = strip_reservation(std::fs::read(&path).unwrap());
        let mut torn = whole.clone();
        torn.extend_from_slice(b"#tsf2 99 deadbeef {\"s\":1,\"q\":4,\"c\"");
        std::fs::write(&path, &torn).unwrap();

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let next = reopened
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        assert_eq!(
            next.sequence, 4,
            "the torn write should be gone and the sequence continue from the last whole record"
        );
        let after = std::fs::read(&path).unwrap();
        assert!(
            after.starts_with(&whole),
            "every record that was complete must still be there, byte for byte"
        );
    }

    /// Blank lines at the end do not hide the last record.
    #[test]
    fn blank_lines_after_the_last_record_are_stepped_over() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in 0..2 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index}"),
                        value: b"v".to_vec(),
                    },
                    false,
                )
                .unwrap();
        }
        drop(store);
        let path = write_ahead_log_path(dir.path(), 1);
        let mut padded = strip_reservation(std::fs::read(&path).unwrap());
        padded.extend_from_slice(b"\n\n   \n");
        std::fs::write(&path, padded).unwrap();

        assert_eq!(
            last_wal_sequence_at(dir.path(), 1).unwrap().0,
            2,
            "blank trailing lines should be stepped over, not read as the end of the log"
        );
    }

    /// Appending must not get slower as the log gets longer.
    ///
    /// Learning the last sequence is what an append needs first. If that reads the whole log, then
    /// N appends read it N times and an ingest is quadratic. The check is direct: the cost of the
    /// last thousand appends against the first thousand, into the same log.
    #[test]
    fn appending_does_not_get_slower_as_the_log_grows() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let batch = 1_000u64;
        let batches = 20u64;
        let mut first = 0.0f64;
        let mut last = 0.0f64;

        for round in 0..batches {
            let started = std::time::Instant::now();
            for index in 0..batch {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("k{:08}", round * batch + index),
                            value: vec![118u8; 128],
                        },
                        false,
                    )
                    .unwrap();
            }
            let per_append = started.elapsed().as_secs_f64() * 1e6 / batch as f64;
            if round == 0 {
                first = per_append;
            }
            if round == batches - 1 {
                last = per_append;
            }
            if round % 5 == 0 || round == batches - 1 {
                println!(
                    "  after {:>6} records: {per_append:>7.1} us per append",
                    round * batch
                );
            }
        }

        let stats = store.raw_stats(1);
        let bytes = std::fs::metadata(write_ahead_log_path(dir.path(), 1))
            .unwrap()
            .len();
        println!(
            "  {} records, {bytes} B, {} full walks of the log, last/first = {:.2}x",
            batches * batch,
            stats.append_full_scans,
            last / first
        );

        // Quadratic would show as the last batch costing many times the first. Some growth is
        // fair -- the file is larger and the page cache is working harder -- so this is loose
        // enough to pass on a busy machine and still catch the shape coming back.
        assert!(
            last < first * 4.0,
            "appending got {:.1}x slower as the log grew ({first:.1} us -> {last:.1} us), which is \
             the shape of an ingest that re-reads the log on every append",
            last / first
        );
        assert!(
            stats.append_full_scans <= 2,
            "the append path walked the whole log {} times; it should learn the end once",
            stats.append_full_scans
        );
    }

    /// Every byte value survives being carried beside the document.
    #[test]
    fn a_carried_payload_survives_every_byte_value() {
        let all_bytes: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let record = WriteAheadLogRecord {
            shard_id: 1,
            sequence: 9,
            command: Some(Command::StringSet {
                key: "k".to_string(),
                value: all_bytes.clone(),
            }),
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };
        // Encode with whichever frame is configured, so the payload's escaping and the frame
        // that carries it are decided by the same thing. Pairing a raw payload with a frame that
        // ends at a newline is a log no writer produces.
        let framed = crate::log_framing::encode_record(&encode_wal_payload(&record).unwrap());
        if !crate::log_framing::binary_frame_enabled() {
            // Only a delimiter-terminated record needs this: it is the whole reason escaping
            // exists. A length-framed record carries the byte freely and is read by its length.
            assert!(
                !framed[..framed.len() - 1].contains(&b'\n'),
                "a delimited record must not contain the delimiter, or every reader loses the log"
            );
        }
        let decoded = decode_wal_line(&framed).unwrap();
        assert_eq!(decoded, record, "the payload changed on the way back");
    }

    /// Several payloads in one record come back in the right order, not swapped.
    #[test]
    fn several_carried_payloads_come_back_in_the_right_order() {
        let record = WriteAheadLogRecord {
            shard_id: 1,
            sequence: 3,
            command: Some(Command::HashMultiSet {
                key: "k".to_string(),
                entries: vec![
                    ("first".to_string(), vec![1u8; 600]),
                    ("second".to_string(), vec![2u8; 600]),
                    ("third".to_string(), vec![3u8; 600]),
                ],
            }),
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };
        let framed = crate::log_framing::encode_line(&encode_wal_payload(&record).unwrap());
        assert_eq!(decode_wal_line(&framed).unwrap(), record);
    }

    /// A payload that escaping would grow stays encoded, so the record never gets bigger.
    #[test]
    fn a_payload_that_escaping_would_grow_stays_encoded() {
        let newlines = vec![b'\n'; 2048];
        let record = WriteAheadLogRecord {
            shard_id: 1,
            sequence: 4,
            command: Some(Command::StringSet {
                key: "k".to_string(),
                value: newlines.clone(),
            }),
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };
        let payload = encode_wal_payload(&record).unwrap();
        assert!(
            !payload.contains(&0x1f),
            "a payload that would grow should have been left in the document"
        );
        let framed = crate::log_framing::encode_line(&payload);
        assert_eq!(decode_wal_line(&framed).unwrap(), record);
    }

    /// A record with nothing worth carrying is written exactly as it was before.
    #[test]
    fn a_record_with_no_payload_is_unchanged_on_disk() {
        // Pins the TEXT encoding: this test is about the shape of a text payload, which the
        // binary one does not have. It asserts a property of that encoding, not of the log.
        std::env::set_var("TS_WAL_BINARY_RECORDS", "0");
        let record = WriteAheadLogRecord {
            shard_id: 1,
            sequence: 5,
            command: Some(Command::StringGet {
                key: "k".to_string(),
            }),
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };
        let payload = encode_wal_payload(&record).unwrap();
        assert_eq!(
            payload,
            serde_json::to_vec(&record).unwrap(),
            "a record that carries nothing must be byte-identical to the document"
        );
    
        // Unpin it: this variable is process-global, and leaving it set makes every
        // test that runs after this one inherit an encoding it never asked for.
        std::env::remove_var("TS_WAL_BINARY_RECORDS");
    }

    /// Records written before payloads were carried still load.
    #[test]
    fn a_record_written_before_payloads_were_carried_still_loads() {
        let written_before = concat!(
            r#"{"s":1,"q":7,"c":{"kind":"string_set","key":"k","value":"aGk="},"#,
            r#""m":{"t":1787429651961}}"#
        );
        let framed = crate::log_framing::encode_line(written_before.as_bytes());
        let decoded = decode_wal_line(&framed).unwrap();
        assert_eq!(
            decoded.command,
            Some(Command::StringSet {
                key: "k".to_string(),
                value: b"hi".to_vec(),
            })
        );
    }

    /// A truncated payload section is corruption, not a record with fewer bytes in it.
    #[test]
    fn a_truncated_carried_payload_is_refused() {
        let record = WriteAheadLogRecord {
            shard_id: 1,
            sequence: 8,
            command: Some(Command::StringSet {
                key: "k".to_string(),
                value: vec![7u8; 900],
            }),
            metadata: None,
            staged_pages: Vec::new(),
            outcomes: Vec::new(),
        };
        let mut payload = encode_wal_payload(&record).unwrap();
        payload.truncate(payload.len() - 40);
        let framed = crate::log_framing::encode_line(&payload);
        assert!(
            decode_wal_line(&framed).is_err(),
            "a record holding less than it claims must be refused"
        );
    }

    /// Through the real log: written, reopened, and read back.
    #[test]
    fn carried_payloads_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let payloads: Vec<Vec<u8>> = (0..8)
            .map(|index| (0..=255u8).cycle().skip(index).take(1500).collect())
            .collect();
        for payload in &payloads {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: "hot".to_string(),
                        value: payload.clone(),
                    },
                    false,
                )
                .unwrap();
        }
        drop(store);

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let scanned = reopened.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(scanned.len(), payloads.len(), "every record should be there");
        for (index, (_, line)) in scanned.iter().enumerate() {
            let record = decode_wal_line(line).unwrap();
            match record.command.expect("a record built with an operation still carries one") {
                Command::StringSet { value, .. } => {
                    assert_eq!(value, payloads[index], "record {index} came back changed")
                }
                other => panic!("unexpected command: {other:?}"),
            }
        }
        // And the sequence still resolves, which is the backward walk over binary payloads.
        assert_eq!(
            reopened
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: "after".to_string(),
                        value: b"v".to_vec(),
                    },
                    false,
                )
                .unwrap()
                .sequence,
            payloads.len() as u64 + 1
        );
    }

    /// A rolled log reads as one log, and its records keep their addresses.
    #[test]
    fn a_rolled_log_reads_whole_and_keeps_its_addresses() {
        set_wal_segment_bytes_for_test(Some(4 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        let mut written = Vec::new();
        for index in 0..200u64 {
            let value = format!("value-{index:06}").into_bytes();
            let (record, log_id) = store
                .append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: value.clone(),
                    },
                    false,
                )
                .unwrap();
            written.push((record.sequence, log_id, value));
        }

        // It really did roll, or the rest of this proves nothing.
        let pieces = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("shard-1.wal."))
            })
            .count();
        assert!(pieces > 1, "the log should be in more than one piece, saw {pieces}");

        // Read as one log, in order, with every record present.
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(scanned.len(), written.len(), "every record should be scanned");
        let mut previous = None;
        for ((log_id, line), (sequence, _, _)) in scanned.iter().zip(written.iter()) {
            if let Some(previous) = previous {
                assert!(*log_id > previous, "log ids should increase across pieces");
            }
            previous = Some(*log_id);
            assert_eq!(decode_wal_line(line).unwrap().sequence, *sequence);
        }

        // Addresses handed out at append time still resolve, including into sealed pieces.
        for (_, log_id, value) in &written {
            let bytes = store
                .read_at_log_id(1, *log_id, 4096)
                .unwrap()
                .unwrap_or_else(|| panic!("log id {log_id} should still resolve"));
            // Where this record ends is the frame's answer, not the first newline's: a
            // length-framed payload carries 0x0A itself, so the first one is usually inside the
            // record and slicing there hands a decoder half a record.
            let end = match crate::log_framing::next_frame(&bytes) {
                Ok(Some((consumed, _))) if consumed > 0 => consumed,
                _ => bytes.len(),
            };
            let record = decode_wal_line(&bytes[..end]).unwrap();
            match record.command.expect("a record built with an operation still carries one") {
                Command::StringSet { value: found, .. } => {
                    assert_eq!(&found, value, "log id {log_id} resolved to the wrong record")
                }
                other => panic!("unexpected command: {other:?}"),
            }
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// A reopened rolled log continues where it left off.
    #[test]
    fn a_rolled_log_continues_after_a_reopen() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        for index in 0..120u64 {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap();
        }
        drop(store);

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let next = reopened
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        assert_eq!(next.sequence, 121, "the sequence should continue across pieces");
        assert_eq!(
            reopened.scan(1, 0, u64::MAX, u64::MAX).unwrap().len(),
            121,
            "every record should still be there"
        );
        set_wal_segment_bytes_for_test(None);
    }

    /// A crash between sealing a piece and starting the next one does not reuse an address.
    ///
    /// Sealing is a rename; creating the next piece is a separate step. In between there are sealed
    /// pieces and nothing to append to, and an absent piece reads as starting at log id zero --
    /// which would hand out addresses the sealed pieces already own, so a reader holding an old
    /// address would be pointed at a record written later.
    ///
    /// What must hold is about addresses that still resolve. Records in the piece that vanished are
    /// gone, and their addresses are free; records in the sealed pieces are not.
    #[test]
    fn a_crash_between_sealing_and_starting_the_next_piece_does_not_reuse_addresses() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let mut earlier = Vec::new();
        for index in 0..100u64 {
            let (_, log_id) = store
                .append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap();
            earlier.push(log_id);
        }

        drop(store);

        // The crash: sealed pieces are on disk, the piece being written never made it.
        let active = write_ahead_log_path(dir.path(), 1);
        assert!(active.exists());
        std::fs::remove_file(&active).unwrap();

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        // Which addresses still resolve, AFTER the crash and BEFORE anything new is written.
        // Records in the piece that vanished are gone and their addresses are free; these are the
        // ones that are not. Asking after the append would count the new record's own address.
        let surviving = earlier
            .iter()
            .copied()
            .filter(|log_id| {
                reopened
                    .read_at_log_id(1, *log_id, 4096)
                    .ok()
                    .flatten()
                    .is_some()
            })
            .collect::<Vec<_>>();

        let (_, log_id) = reopened
            .append_with_sync_reporting(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        assert!(
            !surviving.is_empty(),
            "the sealed pieces should still be readable, or this proves nothing"
        );
        assert!(
            surviving.iter().all(|survivor| *survivor < log_id),
            "a record written after the crash took an address that still resolves to an \
             earlier one: new {log_id}, highest surviving {:?}",
            surviving.iter().max()
        );
        set_wal_segment_bytes_for_test(None);
    }

    /// Reclaim drops whole pieces instead of copying the records that stay.
    ///
    /// Copying is what made reclaim expensive: the cost tracks what it KEEPS, so dropping a prefix
    /// of a large log rewrote nearly all of it -- one measured pass copied 19.6 MB to free 3.8 MB.
    /// A piece whose every record is below the floor needs no copying at all.
    #[test]
    fn reclaim_drops_whole_pieces_without_copying_them() {
        set_wal_segment_bytes_for_test(Some(4 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        let mut ids = Vec::new();
        for index in 0..300u64 {
            let (record, log_id) = store
                .append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap();
            ids.push((record.sequence, log_id));
        }

        let before: u64 = wal_segment_paths(dir.path(), 1)
            .iter()
            .filter_map(|path| path.metadata().ok().map(|meta| meta.len()))
            .sum();

        // Keep only the last fifty records.
        let retain_from = 251u64;
        let report = store.gc_before_sequence_unchecked(1, retain_from).unwrap();

        assert!(
            report.dropped_segments > 0,
            "whole pieces should have gone: {report:?}"
        );
        assert!(
            report.dropped_segment_bytes > 0,
            "the pieces that went should have held something"
        );
        // What matters: the freed space did not have to be paid for in copying.
        assert!(
            report.bytes_copied < report.dropped_segment_bytes,
            "reclaim copied {} bytes to free {} by unlinking -- the copying is what this avoids",
            report.bytes_copied,
            report.dropped_segment_bytes
        );

        let after: u64 = wal_segment_paths(dir.path(), 1)
            .iter()
            .filter_map(|path| path.metadata().ok().map(|meta| meta.len()))
            .sum();
        assert!(after < before, "the log should be smaller: {before} -> {after}");

        // Everything at or above the floor is still readable, and still says what it said.
        for (sequence, log_id) in ids.iter().filter(|(sequence, _)| *sequence >= retain_from) {
            let bytes = store
                .read_at_log_id(1, *log_id, 4096)
                .unwrap()
                .unwrap_or_else(|| panic!("sequence {sequence} should have been kept"));
            // Where this record ends is the frame's answer, not the first newline's: a
            // length-framed payload carries 0x0A itself, so the first one is usually inside the
            // record and slicing there hands a decoder half a record.
            let end = match crate::log_framing::next_frame(&bytes) {
                Ok(Some((consumed, _))) if consumed > 0 => consumed,
                _ => bytes.len(),
            };
            assert_eq!(decode_wal_line(&bytes[..end]).unwrap().sequence, *sequence);
        }

        // And a scan reads the survivors as one log.
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert!(
            !scanned.is_empty(),
            "the records that were kept should still scan"
        );
        for (_, line) in &scanned {
            assert!(
                decode_wal_line(line).unwrap().sequence >= 1,
                "every scanned record should decode"
            );
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// What reclaim costs with the log in one piece against many.
    #[test]
    fn what_reclaim_costs_in_one_piece_against_many() {
        for segment_bytes in [0u64, 8 * 1024] {
            set_wal_segment_bytes_for_test(Some(segment_bytes));
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            let records = 2_000u64;
            for index in 0..records {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("k{index:06}"),
                            value: vec![118u8; 128],
                        },
                        false,
                    )
                    .unwrap();
            }
            let started = std::time::Instant::now();
            let report = store.gc_before_sequence_unchecked(1, records - 100).unwrap();
            let micros = started.elapsed().as_secs_f64() * 1e6;
            println!(
                "  piece size {:>6}: {micros:>9.0} us, copied {:>8} B, unlinked {} piece(s) holding {} B",
                if segment_bytes == 0 { "one".to_string() } else { format!("{segment_bytes}") },
                report.bytes_copied,
                report.dropped_segments,
                report.dropped_segment_bytes
            );
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// Reading from a watermark never skips a record after it.
    ///
    /// This is the property recovery depends on. The strict sequence-continuity check downstream
    /// turns a wrongly-skipped record into a refused load, so the answer must be conservative at
    /// every watermark, not just convenient ones.
    #[test]
    fn reading_from_a_watermark_never_skips_a_record_after_it() {
        set_wal_segment_bytes_for_test(Some(4 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let records = 400u64;
        for index in 0..records {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap();
        }

        for watermark in 0..=records {
            let start = store.log_id_after_sequence(1, watermark).unwrap();
            let seen = store.scan(1, start, u64::MAX, u64::MAX).unwrap();
            let sequences: Vec<u64> = seen
                .iter()
                .map(|(_, line)| decode_wal_line(line).unwrap().sequence)
                .collect();
            for expected in (watermark + 1)..=records {
                assert!(
                    sequences.contains(&expected),
                    "watermark {watermark} skipped sequence {expected}, which still had to be replayed"
                );
            }
            // And the first thing read is never past the hole the caller would notice.
            if let Some(first) = sequences.first() {
                assert!(
                    *first <= watermark + 1,
                    "watermark {watermark} started at sequence {first}, leaving a hole"
                );
            }
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// What a restart reads: from the beginning of the log, against from the watermark.
    #[test]
    fn what_a_restart_reads_from_zero_against_from_the_watermark() {
        for segment_bytes in [0u64, 64 * 1024] {
            set_wal_segment_bytes_for_test(Some(segment_bytes));
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            let records = 20_000u64;
            for index in 0..records {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("k{index:06}"),
                            value: vec![118u8; 128],
                        },
                        false,
                    )
                    .unwrap();
            }
            // The durable index already covers all but the last hundred records.
            let watermark = records - 100;

            let before = std::time::Instant::now();
            let all = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
            let from_zero = before.elapsed().as_secs_f64() * 1e6;

            let before = std::time::Instant::now();
            let start = store.log_id_after_sequence(1, watermark).unwrap();
            let windowed = store.scan(1, start, u64::MAX, u64::MAX).unwrap();
            let from_watermark = before.elapsed().as_secs_f64() * 1e6;

            println!(
                "  piece size {:>6}: from zero {from_zero:>9.0} us ({} records read) -> from the watermark {from_watermark:>8.0} us ({} read)",
                if segment_bytes == 0 { "one".to_string() } else { format!("{segment_bytes}") },
                all.len(),
                windowed.len()
            );
            assert!(windowed.len() >= 100, "the tail must be there to replay");
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// Unlinking pieces honours the block-retention floor.
    ///
    /// A record at or above that floor may hold the only copy of a block's bytes -- a block in the
    /// log has no copy in a band until it is dumped -- so reclaim narrows the caller's retain point
    /// to it. Dropping whole pieces has to respect the narrowed point too, or reclaim removes
    /// exactly what the floor exists to keep, and the loss surfaces later as a read that fails.
    #[test]
    fn unlinking_pieces_honours_the_block_retention_floor() {
        set_wal_segment_bytes_for_test(Some(4 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        let mut ids = Vec::new();
        for index in 0..300u64 {
            let (record, log_id) = store
                .append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap();
            ids.push((record.sequence, log_id));
        }

        // A block still depends on everything from sequence 50 on.
        store.set_block_retention_floor(1, 50);
        // The caller asks to keep only the last fifty records, which is well past the floor.
        let report = store.gc_before_sequence_unchecked(1, 251).unwrap();
        assert!(
            report.effective_retain_from_sequence <= 50,
            "the retain point should have been narrowed to the floor, got {}",
            report.effective_retain_from_sequence
        );

        // Everything the floor protects must still be readable.
        for (sequence, log_id) in ids.iter().filter(|(sequence, _)| *sequence >= 50) {
            let bytes = store
                .read_at_log_id(1, *log_id, 4096)
                .unwrap()
                .unwrap_or_else(|| {
                    panic!("sequence {sequence} is at or above the block-retention floor and was removed")
                });
            // Where this record ends is the frame's answer, not the first newline's: a
            // length-framed payload carries 0x0A itself, so the first one is usually inside the
            // record and slicing there hands a decoder half a record.
            let end = match crate::log_framing::next_frame(&bytes) {
                Ok(Some((consumed, _))) if consumed > 0 => consumed,
                _ => bytes.len(),
            };
            assert_eq!(decode_wal_line(&bytes[..end]).unwrap().sequence, *sequence);
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// Unlinking pieces never removes the highest-sequence record.
    ///
    /// The next append seeds its sequence from the log's tail, so emptying the log entirely makes
    /// the next record reuse sequence 1 -- at or below the persisted anchor, where replay's
    /// `sequence > watermark` filter silently drops it. Reclaim clamps the retain point to keep the
    /// tail; dropping whole pieces has to obey that clamp as well.
    #[test]
    fn unlinking_pieces_never_removes_the_last_record() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        // Stop right after a roll, so the piece being written is EMPTY and the highest-sequence
        // record lives in a SEALED piece -- the only arrangement where dropping pieces can take
        // it. Stopping at a round number instead would usually leave that record in the piece
        // being written, which is never unlinked, and the test would pass without proving
        // anything.
        let mut written = 0u64;
        loop {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{written:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap();
            written += 1;
            let active = write_ahead_log_path(dir.path(), 1);
            let (_, header_len) = read_wal_base(&active).unwrap();
            let active_len = active.metadata().map(|meta| meta.len()).unwrap_or(0);
            if active_len == header_len && written >= 100 {
                break;
            }
            assert!(written < 5_000, "never caught the log just after a roll");
        }
        assert!(
            wal_segment_paths(dir.path(), 1).len() > 1,
            "the log should be in more than one piece"
        );

        // Ask for everything to go.
        store.gc_before_sequence_unchecked(1, u64::MAX).unwrap();

        assert_eq!(
            last_wal_sequence_at(dir.path(), 1).unwrap().0,
            written,
            "the highest-sequence record must survive a full reclaim"
        );
        // And a reopened log continues rather than reusing a sequence.
        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let next = reopened
            .append_with_sync(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        assert_eq!(
            next.sequence,
            written + 1,
            "a reclaimed log reused a sequence, which silently drops the record on replay"
        );
        set_wal_segment_bytes_for_test(None);
    }

    /// Run `body` with TS_WAL_PREALLOCATE on, restoring the environment afterward even on
    /// panic, so a failing assertion cannot leak the gate into every later test. Env vars are
    /// process-wide: like the other env-gated tests here, these are only meaningful under
    /// --test-threads=1.
    fn with_preallocate<T>(body: impl FnOnce() -> T + std::panic::UnwindSafe) -> T {
        std::env::set_var("TS_WAL_PREALLOCATE", "1");
        let outcome = std::panic::catch_unwind(body);
        std::env::remove_var("TS_WAL_PREALLOCATE");
        match outcome {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    /// The escape hatch, pinned: =0 must restore growing appends exactly.
    #[test]
    fn the_preallocate_escape_hatch_restores_growing_appends() {
        std::env::set_var("TS_WAL_PREALLOCATE", "0");
        let outcome = std::panic::catch_unwind(|| {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            append_n(&store, 1, 10);
            let path = dir.path().join("shard-1.wal.jsonl");
            let physical = path.metadata().unwrap().len();
            let records = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
            assert_eq!(10, records.len());
            let record_bytes: u64 = records.iter().map(|(_, line)| line.len() as u64).sum();
            assert_eq!(
                physical, record_bytes,
                "with the hatch pulled, the file must end exactly at its records"
            );
        });
        std::env::remove_var("TS_WAL_PREALLOCATE");
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    #[test]
    fn preallocated_appends_grow_the_file_in_chunks_not_per_record() {
        with_preallocate(|| {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            append_n(&store, 1, 50);
            let path = dir.path().join("shard-1.wal.jsonl");
            let physical = path.metadata().unwrap().len();
            // The file was grown to a chunk boundary, not to the records.
            assert_eq!(
                physical % wal_preallocate_chunk(),
                0,
                "file length must sit on a chunk boundary"
            );
            let records = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
            assert_eq!(50, records.len(), "every record must be read back through the zeros");
        });
    }

    #[test]
    fn a_scan_does_not_tear_down_the_reservation() {
        // The whole point of respecting the reservation: scans run constantly under
        // replication, and a scan that truncated the zeros would make the next append re-grow
        // the file -- paying the metadata barrier per scan/append cycle instead of per chunk.
        with_preallocate(|| {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            append_n(&store, 1, 10);
            let path = dir.path().join("shard-1.wal.jsonl");
            let physical_before = path.metadata().unwrap().len();
            for _ in 0..5 {
                let records = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
                assert_eq!(10, records.len());
            }
            assert_eq!(
                physical_before,
                path.metadata().unwrap().len(),
                "scanning must leave the reservation alone"
            );
            append_n(&store, 1, 1);
            let records = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
            assert_eq!(11, records.len(), "the post-scan append must land after the records");
        });
    }

    #[test]
    fn a_restart_mid_reservation_resumes_the_sequence() {
        with_preallocate(|| {
            let dir = tempfile::tempdir().unwrap();
            {
                let store = LocalWriteAheadLogStore::new(dir.path());
                append_n(&store, 1, 7);
            }
            // A fresh store has cold caches: its first append takes the slow path, which walks
            // the tail through the zeros the previous process reserved.
            let store = LocalWriteAheadLogStore::new(dir.path());
            let report = store
                .append(
                    1,
                    Command::StringSet {
                        key: "after-restart".to_string(),
                        value: b"v".to_vec(),
                    },
                )
                .unwrap();
            assert_eq!(8, report.sequence, "the sequence must resume, not restart");
            let records = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
            assert_eq!(8, records.len());
        });
    }

    #[test]
    fn a_torn_write_inside_the_reservation_is_repaired_not_kept() {
        // A crash mid-write leaves a non-zero partial line where the records end, with the rest
        // of the reservation's zeros after it. Only the zeros are a reservation; the partial
        // line is damage and must be cut exactly as a torn tail always was.
        with_preallocate(|| {
            let dir = tempfile::tempdir().unwrap();
            let sequence_end;
            {
                let store = LocalWriteAheadLogStore::new(dir.path());
                append_n(&store, 1, 5);
                sequence_end = store.scan(1, 0, u64::MAX, u64::MAX).unwrap().len();
            }
            assert_eq!(5, sequence_end);
            let path = dir.path().join("shard-1.wal.jsonl");
            // Find where the records stop and plant garbage after it. The last newline answers
            // that only for records delimited by one -- a length-framed payload carries 0x0A of
            // its own, so the search lands inside a record and the garbage would overwrite half
            // of it, testing something else entirely. Walk the frames to their end instead.
            let bytes = fs::read(&path).unwrap();
            let record_end = {
                let mut at = data_start(&bytes);
                while at < bytes.len() {
                    if bytes[at] == 0 {
                        break;
                    }
                    match crate::log_framing::next_frame(&bytes[at..]) {
                        Ok(Some((consumed, _))) if consumed > 0 => at += consumed,
                        _ => break,
                    }
                }
                at
            };
            {
                use std::io::{Seek, Write};
                let mut file = OpenOptions::new().write(true).open(&path).unwrap();
                file.seek(SeekFrom::Start(record_end as u64)).unwrap();
                file.write_all(b"torn-partial-record-without-newline").unwrap();
            }
            let store = LocalWriteAheadLogStore::new(dir.path());
            let records = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
            assert_eq!(5, records.len(), "the torn garbage must not surface as a record");
            let report = store
                .append(
                    1,
                    Command::StringSet {
                        key: "after-repair".to_string(),
                        value: b"v".to_vec(),
                    },
                )
                .unwrap();
            assert_eq!(6, report.sequence);
        });
    }

    #[test]
    fn sealing_trims_the_reservation_off_the_sealed_piece() {
        with_preallocate(|| {
            let dir = tempfile::tempdir().unwrap();
            set_wal_segment_bytes_for_test(Some(512));
            let store = LocalWriteAheadLogStore::new(dir.path());
            append_n(&store, 1, 30);
            set_wal_segment_bytes_for_test(None);
            let mut sealed = 0;
            for entry in fs::read_dir(dir.path()).unwrap() {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_string_lossy().to_string();
                if name.starts_with("shard-1.wal.") && name.ends_with(".jsonl") && name != "shard-1.wal.jsonl" {
                    sealed += 1;
                    let bytes = fs::read(&path).unwrap();
                    assert!(!bytes.is_empty(), "a sealed piece must hold its records");
                    // The property is that the piece ends AT its last record, with no
                    // reservation left behind it. A trailing newline tested that only while a
                    // record ended with one; walking the frames tests it for either format, and
                    // tests it harder: the records must tile the file exactly.
                    let mut at = data_start(&bytes);
                    while at < bytes.len() {
                        match crate::log_framing::next_frame(&bytes[at..]) {
                            Ok(Some((consumed, _))) if consumed > 0 => at += consumed,
                            _ => break,
                        }
                    }
                    assert_eq!(
                        at,
                        bytes.len(),
                        "a sealed piece must end exactly at its last record, not in zeros"
                    );
                }
            }
            assert!(sealed > 0, "the threshold must actually have sealed pieces");
            let records = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
            assert_eq!(30, records.len(), "records must survive across sealed pieces");
        });
    }

    /// Not an assertion, a measurement -- run by hand with and without TS_WAL_PREALLOCATE:
    ///   cargo test --lib -- --ignored --exact wal::tests::measure_barrier_cost --nocapture
    #[test]
    #[ignore]
    fn measure_barrier_cost() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let count = 2000usize;
        let start = std::time::Instant::now();
        for index in 0..count {
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index}"),
                        value: vec![0x61u8; 200],
                    },
                    true,
                )
                .unwrap();
        }
        let elapsed = start.elapsed();
        let gate = std::env::var("TS_WAL_PREALLOCATE").unwrap_or_default();
        println!(
            "prealloc={gate:?} {count} synced appends: {:.1} us/append",
            elapsed.as_micros() as f64 / count as f64
        );
    }

    /// A crash before the new piece's header reaches disk does not reuse addresses.
    ///
    /// The roll no longer barriers the header -- it rides the barrier that makes the first record
    /// durable. That leaves a window where the piece exists and the header does not, and an empty
    /// piece reads as starting at log id zero, which the sealed pieces already own. An empty piece
    /// therefore has to be treated as one that was never started.
    ///
    /// Entered by stopping in it, so the state on disk is what a crash at that line leaves rather
    /// than what this test guessed it would leave.
    #[test]
    fn a_crash_before_the_header_reaches_disk_does_not_reuse_addresses() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        // Stop inside the roll, between creating the piece and writing its header. Reproducing
        // this window by hand instead -- appending until the piece happens to hold no records, then
        // truncating it -- would encode two unchecked assumptions: that such a piece is where the
        // roll left off, and that truncating to zero is what a crash before the header leaves.
        // Stopping AT the line needs neither: what is on disk afterwards is what that line leaves.
        let mut written = Vec::new();
        let mut index = 0u64;
        let mut stopped = false;
        while index < 5_000 {
            let armed = crate::fault::arm(
                "wal/roll/after_create",
                crate::fault::FaultAction::Stop,
            );
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store.append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
            }));
            drop(armed);
            match outcome {
                Ok(Ok((record, log_id))) => written.push((record.sequence, log_id)),
                Ok(Err(err)) => panic!("append failed: {err}"),
                Err(_) => {
                    stopped = true;
                    break;
                }
            }
            index += 1;
        }
        assert!(stopped, "never reached a roll, so this tested nothing");
        assert!(
            wal_segment_paths(dir.path(), 1).len() > 1,
            "the log should be in more than one piece"
        );
        drop(store);

        // And this is what that line left: the piece exists and has nothing in it.
        let active = write_ahead_log_path(dir.path(), 1);
        assert_eq!(
            active.metadata().unwrap().len(),
            0,
            "the piece should exist and be empty -- that is the window"
        );

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let (_, log_id) = reopened
            .append_with_sync_reporting(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        assert!(
            written.iter().all(|(_, earlier)| *earlier < log_id),
            "a record written after the crash took an address an earlier one already owns"
        );
        // And every earlier address still resolves to the record it was handed out for.
        for (sequence, earlier) in &written {
            if let Some(bytes) = reopened.read_at_log_id(1, *earlier, 4096).unwrap() {
                // The frame says where this record ends. The first newline does not: a
                // length-framed payload carries 0x0A itself, so slicing there hands the
                // decoder half a record.
                let end = match crate::log_framing::next_frame(&bytes) {
                    Ok(Some((consumed, _))) if consumed > 0 => consumed,
                    _ => bytes.len(),
                };
                assert_eq!(
                    decode_wal_line(&bytes[..end]).unwrap().sequence,
                    *sequence,
                    "log id {earlier} resolved to the wrong record"
                );
            }
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// What a roll costs the append that triggers it.
    #[test]
    fn what_a_roll_costs_the_append_that_triggers_it() {
        set_wal_segment_bytes_for_test(Some(64 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        let mut plain = Vec::new();
        let mut rolled = Vec::new();
        let mut pieces_before = 1usize;
        for index in 0..4_000u64 {
            let started = std::time::Instant::now();
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 128],
                    },
                    false,
                )
                .unwrap();
            let micros = started.elapsed().as_secs_f64() * 1e6;
            let pieces = wal_segment_paths(dir.path(), 1).len();
            if pieces > pieces_before {
                pieces_before = pieces;
                rolled.push(micros);
            } else {
                plain.push(micros);
            }
        }
        let at = |values: &mut Vec<f64>, q: f64| -> f64 {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            values[((values.len() as f64 - 1.0) * q) as usize]
        };
        let mut p = plain.clone();
        let mut r = rolled.clone();
        println!(
            "  {} ordinary appends: p50 {:.0} us  p99 {:.0} us",
            plain.len(),
            at(&mut p, 0.50),
            at(&mut p, 0.99),
        );
        println!(
            "  {} appends that rolled: p50 {:.0} us  p99 {:.0} us  max {:.0} us",
            rolled.len(),
            at(&mut r, 0.50),
            at(&mut r, 0.99),
            at(&mut r, 1.0),
        );
        assert!(!rolled.is_empty(), "nothing rolled, so this measured nothing");
        set_wal_segment_bytes_for_test(None);
    }

    /// The window between sealing a piece and starting the next one, entered by stopping there.
    ///
    /// There is already a test for this window that builds its state by hand -- it removes the
    /// piece being written and reopens. That is only a test of this window if removing that file is
    /// what a crash there leaves behind, and nothing checks the claim. Applying the same reasoning
    /// one window over produced a test that failed for the wrong reason, so the reasoning is worth
    /// removing rather than repeating: here the roll is stopped AT the line, and whatever is on
    /// disk afterwards is what a crash at that line leaves, by construction.
    #[test]
    fn stopping_a_roll_after_the_rename_does_not_reuse_addresses() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        // Write until a roll is about to happen, with the point disarmed.
        let mut written = Vec::new();
        let mut index = 0u64;
        let mut stopped = false;
        while index < 5_000 {
            let armed = crate::fault::arm(
                "wal/roll/after_rename",
                crate::fault::FaultAction::Stop,
            );
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store.append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
            }));
            drop(armed);
            match outcome {
                Ok(Ok((record, log_id))) => written.push((record.sequence, log_id)),
                Ok(Err(err)) => panic!("append failed: {err}"),
                Err(_) => {
                    // The roll stopped after the rename. The store's lock is poisoned by that
                    // unwind, which is exactly what a crash would cost us: this handle is done.
                    stopped = true;
                    break;
                }
            }
            index += 1;
        }
        assert!(stopped, "never reached a roll, so this tested nothing");
        drop(store);

        // Everything is sealed and there is nothing to append to -- the state a crash there leaves.
        let active = write_ahead_log_path(dir.path(), 1);
        assert!(!active.exists(), "the piece being written should be gone");
        assert!(
            wal_segment_paths(dir.path(), 1).iter().any(|p| p != &active),
            "the sealed pieces should still be there"
        );

        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let (_, log_id) = reopened
            .append_with_sync_reporting(
                1,
                Command::StringSet {
                    key: "after".to_string(),
                    value: b"v".to_vec(),
                },
                false,
            )
            .unwrap();
        assert!(
            written.iter().all(|(_, earlier)| *earlier < log_id),
            "a record written after the crash took an address an earlier one already owns"
        );
        // And every address handed out before the crash still resolves to its own record.
        for (sequence, earlier) in &written {
            if let Some(bytes) = reopened.read_at_log_id(1, *earlier, 4096).unwrap() {
                // The frame says where this record ends. The first newline does not: a
                // length-framed payload carries 0x0A itself, so slicing there hands the
                // decoder half a record.
                let end = match crate::log_framing::next_frame(&bytes) {
                    Ok(Some((consumed, _))) if consumed > 0 => consumed,
                    _ => bytes.len(),
                };
                assert_eq!(
                    decode_wal_line(&bytes[..end]).unwrap().sequence,
                    *sequence,
                    "log id {earlier} resolved to the wrong record"
                );
            }
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// One reclaim pass unlinks at most its bound, and later passes take the rest.
    ///
    /// The unlinking runs while the log's lock is held and every append needs that lock, so an
    /// unbounded pass over a large backlog stops every writer for as long as it takes. Bounding it
    /// does not make the total cheaper -- each pass pays the directory barrier again -- it bounds
    /// how long any one pass holds the lock.
    #[test]
    fn one_reclaim_pass_unlinks_at_most_its_bound() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        std::env::set_var("TS_WAL_RECLAIM_MAX_SEGMENTS_PER_PASS", "4");
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        let mut last = 0u64;
        for index in 0..400u64 {
            last = store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap()
                .sequence;
        }
        let sealed_before = wal_segment_paths(dir.path(), 1).len();
        assert!(sealed_before > 8, "need a backlog worth bounding, saw {sealed_before}");

        // One pass drops no more than the bound.
        let first = store.gc_before_sequence_unchecked(1, last).unwrap();
        assert!(
            first.dropped_segments <= 4,
            "one pass dropped {} pieces, past its bound",
            first.dropped_segments
        );
        assert!(first.dropped_segments > 0, "the pass should have dropped something");

        // Later passes take the rest, without recomputing anything.
        let mut passes = 1;
        loop {
            let report = store.gc_before_sequence_unchecked(1, last).unwrap();
            if report.dropped_segments == 0 {
                break;
            }
            assert!(
                report.dropped_segments <= 4,
                "a later pass dropped {} pieces, past its bound",
                report.dropped_segments
            );
            passes += 1;
            assert!(passes < 500, "reclaim never drained");
        }
        assert!(passes > 1, "the backlog should have taken more than one pass");

        // And the records that had to survive still do.
        let survivors = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert!(!survivors.is_empty(), "the retained tail should still be there");
        for (_, line) in &survivors {
            decode_wal_line(line).expect("every surviving record should decode");
        }
        std::env::remove_var("TS_WAL_RECLAIM_MAX_SEGMENTS_PER_PASS");
        set_wal_segment_bytes_for_test(None);
    }

    /// Not an assertion, a measurement -- how long one reclaim pass holds the log's lock.
    ///   cargo test --lib -- --ignored --exact wal::tests::measure_reclaim_pass --nocapture
    #[test]
    #[ignore]
    fn measure_reclaim_pass() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let mut last = 0u64;
        for index in 0..3_000u64 {
            last = store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap()
                .sequence;
        }
        let pieces = wal_segment_paths(dir.path(), 1).len();
        let started = std::time::Instant::now();
        let report = store.gc_before_sequence_unchecked(1, last).unwrap();
        let micros = started.elapsed().as_secs_f64() * 1e6;
        println!(
            "bound={:?} backlog {pieces} pieces: one pass {micros:.0} us, dropped {}",
            std::env::var("TS_WAL_RECLAIM_MAX_SEGMENTS_PER_PASS").unwrap_or_default(),
            report.dropped_segments
        );
        set_wal_segment_bytes_for_test(None);
    }

    /// A batch append rolls when the piece it is writing fills up.
    ///
    /// Without this, batch ingest appends into one piece forever: segmentation stops applying to
    /// the path that writes the most, and reclaim -- which cannot unlink the piece being written --
    /// loses the unlinking it exists for.
    #[test]
    fn a_batch_append_rolls_when_the_piece_fills() {
        set_wal_segment_bytes_for_test(Some(4 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        for round in 0..12u64 {
            let commands: Vec<Command> = (0..20u64)
                .map(|index| Command::StringSet {
                    key: format!("k{round:03}_{index:03}"),
                    value: vec![118u8; 64],
                })
                .collect();
            store.append_batch_atomic(1, commands, false).unwrap();
        }

        assert!(
            wal_segment_paths(dir.path(), 1).len() > 1,
            "batch ingest should have rolled the log, not grown one piece"
        );
        // And the log still reads as one log, in order, with everything present.
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(scanned.len(), 240, "every record should still be there");
        let mut expected = 1u64;
        for (_, line) in &scanned {
            assert_eq!(
                decode_wal_line(line).unwrap().sequence,
                expected,
                "the records should read back in order across pieces"
            );
            expected += 1;
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// A batch append after a crash between sealing and starting the next piece does not reuse
    /// addresses.
    ///
    /// The single-record path rebuilds the missing piece from what is on disk; the batch path did
    /// not, and instead let the first record create the file with no base header -- so it read as
    /// starting at log id zero, an address the sealed pieces already own.
    ///
    /// Entered by stopping inside the roll rather than by building the state by hand, so what is on
    /// disk is what a crash at that line leaves.
    #[test]
    fn a_batch_append_after_a_crash_mid_roll_does_not_reuse_addresses() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());

        let mut written = Vec::new();
        let mut index = 0u64;
        let mut stopped = false;
        while index < 5_000 {
            let armed = crate::fault::arm(
                "wal/roll/after_rename",
                crate::fault::FaultAction::Stop,
            );
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store.append_with_sync_reporting(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
            }));
            drop(armed);
            match outcome {
                Ok(Ok((record, log_id))) => written.push((record.sequence, log_id)),
                Ok(Err(err)) => panic!("append failed: {err}"),
                Err(_) => {
                    stopped = true;
                    break;
                }
            }
            index += 1;
        }
        assert!(stopped, "never reached a roll, so this tested nothing");
        drop(store);

        let active = write_ahead_log_path(dir.path(), 1);
        assert!(!active.exists(), "the piece being written should be gone");

        // The batch path is what has to recover here.
        let reopened = LocalWriteAheadLogStore::new(dir.path());
        let commands: Vec<Command> = (0..4u64)
            .map(|index| Command::StringSet {
                key: format!("after{index}"),
                value: b"v".to_vec(),
            })
            .collect();
        let records = reopened.append_batch_atomic(1, commands, false).unwrap();
        assert_eq!(records.len(), 4);

        // Every address the batch took must be past everything handed out before the crash.
        for record in &records {
            let log_id = reopened.log_id_at(1, record.sequence).unwrap();
            assert!(
                written.iter().all(|(_, earlier)| *earlier < log_id),
                "a batch record took an address an earlier record already owns"
            );
        }
        set_wal_segment_bytes_for_test(None);
    }

    /// Sealing a piece makes its contents durable, whether or not there was a reservation to trim.
    ///
    /// A record appended with sync=false sits in the page cache until a barrier, and the barrier
    /// that follows opens the piece being WRITTEN, which after a roll is the new one. So the bytes
    /// in the piece just sealed have no barrier that ever covers them unless the roll provides one.
    ///
    /// This counts the barrier. A test cannot show the loss itself without a real crash: the page
    /// cache serves the bytes back to this process whether or not they reached the device.
    #[test]
    fn sealing_a_piece_makes_its_contents_durable() {
        set_wal_segment_bytes_for_test(Some(2 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let seal_barriers = || -> u64 {
            crate::durability_metrics::snapshot()
                .get("wal_seal_outgoing_piece")
                .copied()
                .unwrap_or(0)
        };
        let before = seal_barriers();
        let mut rolled = false;
        let mut index = 0u64;
        while index < 5_000 && !rolled {
            let pieces_before = wal_segment_paths(dir.path(), 1).len();
            store
                .append_with_sync(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    false,
                )
                .unwrap();
            rolled = wal_segment_paths(dir.path(), 1).len() > pieces_before;
            index += 1;
        }
        assert!(rolled, "never rolled, so this tested nothing");
        assert!(
            seal_barriers() > before,
            "sealing took no barrier, so the records in the sealed piece have none"
        );
        set_wal_segment_bytes_for_test(None);
    }

    /// The group-commit reserve path rolls, and its records survive the roll.
    #[test]
    fn the_group_commit_path_rolls_and_keeps_its_records() {
        set_wal_segment_bytes_for_test(Some(4 * 1024));
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let mut last = 0u64;
        for index in 0..300u64 {
            last = store
                .append_for_group_commit(
                    1,
                    Command::StringSet {
                        key: format!("k{index:06}"),
                        value: vec![118u8; 64],
                    },
                    Vec::new(),
                )
                .unwrap()
                .sequence;
        }
        store.commit_barrier(1, last).unwrap();
        assert!(
            wal_segment_paths(dir.path(), 1).len() > 1,
            "the group-commit path should have rolled the log, not grown one piece"
        );
        let scanned = store.scan(1, 0, u64::MAX, u64::MAX).unwrap();
        assert_eq!(scanned.len(), 300, "every reserved record should still be there");
        let mut expected = 1u64;
        for (_, line) in &scanned {
            assert_eq!(decode_wal_line(line).unwrap().sequence, expected);
            expected += 1;
        }
        set_wal_segment_bytes_for_test(None);
    }
}
