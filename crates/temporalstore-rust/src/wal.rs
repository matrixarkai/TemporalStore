// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
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
pub fn decode_wal_line(line: &[u8]) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
    let payload = crate::log_framing::decode_line(line)?;
    Ok(serde_json::from_slice::<WriteAheadLogRecord>(payload)?)
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
    #[serde(rename = "c", alias = "command")]
    pub command: Command,
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
}

/// A page put aside during a write, to be carried in that write's log record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagedPage {
    /// The object the page belongs to, which is what a read has when it comes looking.
    pub object_id: u64,
    /// The page contents, stored text-encoded.
    ///
    /// A byte vector serializes as an array of numbers, about five bytes of log per byte of
    /// page. For a field that exists to carry page contents that is the dominant cost of the
    /// whole record, so it is encoded instead -- about a third of the size.
    #[serde(with = "staged_page_bytes")]
    pub bytes: Vec<u8>,
}

/// Text encoding for a staged page's contents.
///
/// Reading accepts the array form too, so a log written before this still loads.
mod staged_page_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::de::{Deserializer, Error, SeqAccess, Visitor};
    use serde::Serializer;

    pub(super) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        struct EitherShape;

        impl<'de> Visitor<'de> for EitherShape {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an encoded page, or the array of bytes written before")
            }

            fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
                STANDARD.decode(value).map_err(E::custom)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(bytes)
            }
        }

        deserializer.deserialize_any(EitherShape)
    }
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
    pub fn single_command(command: &Command) -> Self {
        Self {
            version: WRITE_AHEAD_LOG_FORMAT_VERSION,
            timestamp_ms: current_time_ms(),
            // Derived from the command this record already carries, and read by nothing, so
            // writing it costs 147 fsynced bytes per record to say what the record says twice.
            // `from_command` reconstructs it for any caller that wants it.
            items: if wal_item_metadata_enabled() {
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

/// TS_WAL_ITEM_METADATA: write the per-item description into each record.
///
/// Default OFF. Every field is derived from the command in the same record and nothing reads it
/// back, so writing it is 147 bytes of amplification per write. Set to a truthy value to restore
/// it for a consumer that reads records directly and has not moved to deriving it.
fn wal_item_metadata_enabled() -> bool {
    matches!(
        std::env::var("TS_WAL_ITEM_METADATA")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
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
    /// this shard. Without TS_PHASE1_FLAT this increments once per append (O(writes)); with the
    /// gate on it stays O(1) once the shard's length cache is warm. Read by the phase-1 aging test.
    #[serde(default)]
    pub append_full_scans: u64,
    /// Diagnostics: full-file `last_wal_sequence_at` rescans taken inside `stats()`. The per-write
    /// index-anchor step reads `stats().last_sequence`; without TS_PHASE1_FLAT that rescans on every
    /// write (O(writes)); with the gate on the engine anchors off `cached_last_sequence` so this
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
    /// Bytes reclaimed from the head of this shard's log over its lifetime, after this pass.
    /// A record's log id minus this is where it now physically lives.
    pub base_offset: u64,
    /// Bytes rewritten to keep the survivors. Reclaim copies what it keeps, so this -- not
    /// `records_removed` -- is the cost of the pass, and it tracks the RETAINED size.
    pub bytes_copied: u64,
    /// The pass was declined because the copy it required bought too little space. The records
    /// are untouched and a later pass, once the prefix has grown, will take them.
    pub skipped_not_worth_rewrite: bool,
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
    // share one durability barrier. Consulted ONLY when TS_GROUP_COMMIT is set.
    // Writers append their bytes under the `inner` lock, RELEASE it, then coalesce
    // their fsync here -- so a burst of concurrent writes amortizes onto ~1 fsync.
    sync_coord: Arc<Mutex<HashMap<ShardId, GroupCommitState>>>,
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
    stats: WriteAheadLogStats,
    last_sequence_by_shard: HashMap<ShardId, u64>,
    // MANIFEST-PARITY / phase-1 flat-append cache (TS_PHASE1_FLAT). The WAL file byte length as
    // this process last left it after its own append (or after a full reconcile scan), per shard.
    // On the next append the fast path stats the file: if the on-disk length still equals this,
    // no other writer touched the file since we wrote it (the append lock is cross-process) and --
    // because we only ever append complete framed lines -- there is no torn tail, so the warm
    // `last_sequence_by_shard` is authoritative and the O(records) `last_wal_sequence_at` scan is
    // skipped. Any mismatch (external append, or first touch this process lifetime) falls back to
    // the full scan. Only consulted when `wal_fast_append_seq()`; harmless to maintain when off.
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
}

impl LocalWriteAheadLogStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        Self {
            inner: Arc::new(Mutex::new(WriteAheadLogInner {
                root,
                stats: WriteAheadLogStats::default(),
                last_sequence_by_shard: HashMap::new(),
                verified_len_by_shard: HashMap::new(),
                block_retention_floor_by_shard: HashMap::new(),
                base_by_shard: HashMap::new(),
            })),
            sync_coord: Arc::new(Mutex::new(HashMap::new())),
        }
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
        self.append_with_sync_inner(shard_id, command, sync, Vec::new())
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
        self.append_with_sync_inner(shard_id, command, sync, staged_pages)
    }

    pub fn append_with_sync(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        self.append_with_sync_inner(shard_id, command, sync, Vec::new())
            .map(|(record, _)| record)
    }

    fn append_with_sync_inner(
        &self,
        shard_id: ShardId,
        command: Command,
        sync: bool,
        staged_pages: Vec<StagedPage>,
    ) -> Result<(WriteAheadLogRecord, u64), WriteAheadLogError> {
        // In group-commit mode the durable barrier is deferred out of the append
        // critical section (below), so the byte-append records with sync=false and the
        // fsync is coalesced across concurrent writers. Default mode keeps the exact
        // per-append in-lock fsync behavior.
        let group = sync && group_commit_enabled();
        let record;
        let next_sequence;
        let log_id;
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            fs::create_dir_all(&inner.root)?;
            let _append_lock = WalAppendLock::acquire(&inner.root, shard_id)?;
            let (last_sequence, on_disk_len) =
                resolve_last_sequence_for_append(&mut inner, shard_id)?;
            inner.last_sequence_by_shard.insert(shard_id, last_sequence);
            let seq = last_sequence.saturating_add(1);
            let rec = WriteAheadLogRecord {
                shard_id,
                sequence: seq,
                metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
                command,
                staged_pages,
            };
            let report = append_record_locked(&mut inner, &rec, sync && !group, Some(on_disk_len))?;
            inner.stats.last_sequence = report.current_sequence;
            // Record the file length we just left behind so the next append's fast path can
            // confirm no other writer touched the file (O(1) stat) and skip the full scan.
            inner
                .verified_len_by_shard
                .insert(shard_id, report.persistent_bytes);
            // last_flushed_sequence is advanced by append_record_locked ONLY when the record was
            // actually fsynced (sync=true, non-group). An unconditional overwrite here reported an
            // async / bulk-mode (unsynced) record as durable -- overstating durability, a latent
            // trap for any future reclaim/ack gate that reads it. The group path advances it below
            // after the coalesced barrier actually reaches disk.
            inner.last_sequence_by_shard.insert(shard_id, seq);
            // Where the record landed, in the addressing space that survives reclaim.
            let (base, header_len) = cached_wal_base(&mut inner, shard_id)?;
            log_id = base.saturating_add(report.offset.saturating_sub(header_len));
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
    pub fn append_for_group_commit(
        &self,
        shard_id: ShardId,
        command: Command,
    ) -> Result<WriteAheadLogRecord, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let _append_lock = WalAppendLock::acquire(&inner.root, shard_id)?;
        let (last_sequence, _on_disk_len) = resolve_last_sequence_for_append(&mut inner, shard_id)?;
        inner.last_sequence_by_shard.insert(shard_id, last_sequence);
        let seq = last_sequence.saturating_add(1);
        let rec = WriteAheadLogRecord {
            shard_id,
            sequence: seq,
            metadata: Some(WriteAheadLogRecordMetadata::single_command(&command)),
            command,
            staged_pages: Vec::new(),
        };
        // sync=false: write the bytes, defer the fdatasync to `commit_barrier`. Same as the
        // group branch of append_with_sync. `last_flushed_sequence` is NOT advanced here (the
        // record is not yet durable); the coalesced barrier advances it once it reaches disk.
        let report = append_record_locked(&mut inner, &rec, false, None)?;
        inner.stats.last_sequence = report.current_sequence;
        inner.last_sequence_by_shard.insert(shard_id, seq);
        inner
            .verified_len_by_shard
            .insert(shard_id, report.persistent_bytes);
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
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            fs::create_dir_all(&inner.root)?;
            let _append_lock = WalAppendLock::acquire(&inner.root, shard_id)?;
            let disk_last_sequence = last_wal_sequence_at(&inner.root, shard_id)?;
            let cached_last_sequence = inner
                .last_sequence_by_shard
                .get(&shard_id)
                .copied()
                .unwrap_or_default();
            let mut seq = cached_last_sequence.max(disk_last_sequence);
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
                    command,
                    staged_pages: Vec::new(),
                };
                // Buffer every record (sync=false); the single durability barrier below covers
                // the whole batch. append_record_locked keeps last_flushed_sequence honest -- it
                // only advances on an actual fsync, which happens once, after the loop.
                let report = append_record_locked(&mut inner, &rec, false, None)?;
                inner.stats.last_sequence = report.current_sequence;
                records.push(rec);
            }
            inner.last_sequence_by_shard.insert(shard_id, seq);
            last_sequence = seq;
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
        file.sync_data()?;
        if !entry.dir_synced {
            // First durable append for this shard this process lifetime: make the
            // directory entry durable once. Subsequent appends never touch it.
            sync_parent_dir(&path)?;
            entry.dir_synced = true;
        }
        entry.durable_seq = entry.durable_seq.max(snapshot);
        {
            let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
            inner.stats.flushes += 1;
            inner.stats.syncs += 1;
            if snapshot > inner.stats.last_flushed_sequence {
                inner.stats.last_flushed_sequence = snapshot;
            }
        }
        Ok(())
    }

    pub fn append_replayed_record(
        &self,
        record: WriteAheadLogRecord,
    ) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let last_sequence = match inner.last_sequence_by_shard.get(&record.shard_id).copied() {
            Some(sequence) => sequence,
            None => {
                let sequence = last_wal_sequence_at(&inner.root, record.shard_id)?;
                inner
                    .last_sequence_by_shard
                    .insert(record.shard_id, sequence);
                sequence
            }
        };
        if record.sequence <= last_sequence {
            let path = write_ahead_log_path(&inner.root, record.shard_id);
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
        let path = write_ahead_log_path(&inner.root, shard_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; size as usize];
        let read = file.read(&mut bytes)?;
        bytes.truncate(read);
        inner.stats.reads += 1;
        inner.stats.bytes_read += read as u64;
        Ok(bytes)
    }

    pub fn scan(
        &self,
        shard_id: ShardId,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        let path = write_ahead_log_path(&inner.root, shard_id);
        // An absent WAL file is "nothing to scan", not an error. Distinguishing it here lets
        // recovery treat a missing WAL as "nothing to replay" while still surfacing a genuine
        // scan/decode failure (corruption) as data loss (see engine::lifecycle replay).
        if !path.exists() {
            inner.stats.scans += 1;
            return Ok(Vec::new());
        }
        let _ = last_wal_sequence_at(&inner.root, shard_id)?;
        // Start past the reclaim-base header. A caller asking from 0 means "from the first
        // record"; handing it the header would decode as a corrupt record and be reported as
        // data loss.
        let (_, header_len) = read_wal_base(&path)?;
        let start_offset = start_offset.max(header_len);
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(start_offset))?;
        let mut reader = BufReader::new(file);
        let mut offset = start_offset;
        let mut total = 0;
        let mut records = Vec::new();
        loop {
            let mut line = Vec::new();
            let read = reader.read_until(b'\n', &mut line)?;
            if read == 0 {
                break;
            }
            let next_offset = offset.saturating_add(read as u64);
            if next_offset > end_offset || total + read as u64 > max_bytes {
                break;
            }
            // Refuse a corrupt record here, where it is being read. This used to happen only as a
            // side effect of walking the whole file to find the log's end; that walk no longer
            // reads everything, and a guarantee that depends on unrelated work is not a guarantee.
            // A blank line carries nothing to verify and is passed through as before.
            if !line.iter().all(|byte| byte.is_ascii_whitespace()) {
                decode_wal_line(&line)?;
            }
            records.push((offset, line));
            offset = next_offset;
            total += read as u64;
        }
        inner.stats.scans += 1;
        inner.stats.bytes_read += total;
        Ok(records)
    }

    pub fn flush(&self, shard_id: ShardId) -> Result<WriteAheadLogFlushReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = write_ahead_log_path(&inner.root, shard_id);
        let last_sequence = last_wal_sequence_at(&inner.root, shard_id)?;
        if !path.exists() {
            return Ok(WriteAheadLogFlushReport {
                shard_id,
                path,
                last_sequence,
                persistent_bytes: 0,
                synced: false,
            });
        }
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        file.sync_all()?;
        sync_parent_dir(&path)?;
        let persistent_bytes = path.metadata()?.len();
        inner.stats.flushes += 1;
        inner.stats.syncs += 1;
        inner.stats.last_flushed_sequence = last_sequence;
        inner.stats.persistent_bytes = persistent_bytes;
        Ok(WriteAheadLogFlushReport {
            shard_id,
            path,
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
        let path = write_ahead_log_path(&inner.root, shard_id);
        let (base, header_len) = read_wal_base(&path)?;
        if log_id < base {
            return Ok(None);
        }
        let physical = header_len.saturating_add(log_id - base);
        let length = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        if physical >= length {
            return Ok(None);
        }
        Ok(Some(physical))
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
        let Some(physical) = self.resolve_log_id(shard_id, log_id)? else {
            return Ok(None);
        };
        // A caller asking by log id does not know how long the record is, so it asks for an
        // upper bound. Reading past the end of the file would fail and report the value as
        // absent -- a read that should have succeeded -- so clamp to what is actually there.
        let length = {
            let inner = self.inner.lock().expect("write-ahead log lock poisoned");
            let path = write_ahead_log_path(&inner.root, shard_id);
            path.metadata().map(|metadata| metadata.len()).unwrap_or(0)
        };
        let available = length.saturating_sub(physical);
        if available == 0 {
            return Ok(None);
        }
        self.read_range(shard_id, physical, size.min(available))
            .map(Some)
    }

    pub fn gc_before_sequence(
        &self,
        shard_id: ShardId,
        retain_from_sequence: u64,
    ) -> Result<WriteAheadLogGcReport, WriteAheadLogError> {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = write_ahead_log_path(&inner.root, shard_id);
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
        let last_sequence = last_wal_sequence_at(&inner.root, shard_id)?;
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
        // Records are appended in strictly ascending sequence -- the live path increments under
        // the append lock, and the replay path refuses anything at or below the last sequence --
        // so the records to keep are a contiguous SUFFIX and the ones to drop are a prefix.
        // That is what makes the whole reclaim expressible as one number: every survivor moves
        // down by exactly the length of the removed prefix.
        let (base_offset, header_len) = read_wal_base(&path)?;
        // One line at a time. A log is not bounded by memory, so neither this search nor the
        // copy below may hold it: reclaiming a large log otherwise costs a transient allocation
        // the size of the whole file.
        let mut reader = BufReader::new(File::open(&path)?);
        reader.seek(SeekFrom::Start(header_len))?;
        let mut records_before = 0usize;
        let mut records_after = 0usize;
        let mut split = None;
        let mut cursor = header_len;
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader.read_until(b'\n', &mut line)?;
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
                records_before,
                records_after: records_before,
                records_removed: 0,
                bytes_before,
                bytes_after: bytes_before,
                base_offset,
                bytes_copied: 0,
                skipped_not_worth_rewrite: true,
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
            let mut source = File::open(&path)?;
            source.seek(SeekFrom::Start(split))?;
            std::io::copy(&mut BufReader::new(source), &mut temp)?;
            temp.flush()?;
            temp.sync_all()?;
        }
        fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        // Reclaim is the only thing that moves the base, so refresh the cache here rather than
        // letting an append compute a log id against a stale one.
        let new_header_len = crate::log_framing::encode_base_header(new_base).len() as u64;
        inner
            .base_by_shard
            .insert(shard_id, (new_base, new_header_len));
        let bytes_after = path.metadata()?.len();
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
            bytes_copied: retained_bytes,
            skipped_not_worth_rewrite: false,
        })
    }

    pub fn stats(&self, shard_id: ShardId) -> WriteAheadLogStats {
        let mut inner = self.inner.lock().expect("write-ahead log lock poisoned");
        inner.stats.stats_full_scans = inner.stats.stats_full_scans.saturating_add(1);
        let path = write_ahead_log_path(&inner.root, shard_id);
        WriteAheadLogStats {
            last_sequence: last_wal_sequence_at(&inner.root, shard_id).unwrap_or_default(),
            persistent_bytes: path.metadata().map(|metadata| metadata.len()).unwrap_or(0),
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
    /// rescan, and confirmed dominant by stack sampling). Under TS_PHASE1_FLAT the engine anchors
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
        let reader = BufReader::new(file);
        let mut start_sequence = 0_u64;
        let mut current_sequence = 0_u64;
        let mut records = 0_usize;
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let record = decode_wal_line(line.as_bytes())?;
            if start_sequence == 0 {
                start_sequence = record.sequence;
            }
            current_sequence = current_sequence.max(record.sequence);
            records += 1;
        }
        let length_bytes = path.metadata()?.len();
        Ok(WriteAheadLogInfo {
            shard_id,
            path,
            start_sequence,
            current_sequence,
            records,
            length_bytes,
            persistent_length_bytes: length_bytes,
            last_flushed_sequence: inner.stats.last_flushed_sequence.max(current_sequence),
            format_version: WRITE_AHEAD_LOG_FORMAT_VERSION,
        })
    }
}

impl Default for LocalWriteAheadLogStore {
    fn default() -> Self {
        Self::new(unique_temp_path("wals"))
    }
}

fn write_ahead_log_path(root: &Path, shard_id: ShardId) -> PathBuf {
    root.join(format!("shard-{shard_id}.wal.jsonl"))
}

struct WalAppendLock {
    #[allow(dead_code)]
    file: File,
}

impl WalAppendLock {
    fn acquire(root: &Path, shard_id: ShardId) -> Result<Self, WriteAheadLogError> {
        fs::create_dir_all(root)?;
        let path = root.join(format!("shard-{shard_id}.wal.lock"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
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
    matches!(
        std::env::var("MATRIXARK_BULK_INGEST")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// TS_PHASE1_FLAT: make per-append WAL sequence resolution O(1) so phase-1 (the work under the
/// engine `shards` write lock) stops aging O(n) with data size. The live client-write append path
/// (`append_with_sync` / `append_for_group_commit`) otherwise calls `last_wal_sequence_at()` on
/// EVERY append -- a full read of the per-shard WAL file from offset 0 that decodes every record to
/// find the max sequence (O(records) per append -> O(n^2) ingest). That per-write scan is the
/// dominant WAL-side phase-1 cost, longer than the ~3.3 ms fsync and serialized under the global
/// lock, so concurrent writers never overlap at the fsync barrier and group commit cannot coalesce.
/// With the gate on we trust the warm in-process `last_sequence_by_shard` cache whenever the file's
/// on-disk length is still exactly what we last left it (`verified_len_by_shard`) -- an O(1)
/// `metadata()` stat instead of the O(n) scan. Safe because (a) the append lock is cross-process, so
/// any external appender changes the length and forces the full scan, and (b) we only ever append
/// complete framed lines, so a length match rules out a torn tail. Default OFF, byte-identical to
/// the unconditional-scan path when off. Mirrors the warm-cache fast path already used by
/// `index_log::append_delta` and `append_replayed_record`.
fn wal_fast_append_seq() -> bool {
    wal_env_flag_default_on("TS_PHASE1_FLAT")
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
) -> Result<(u64, u64), WriteAheadLogError> {
    let cached_last_sequence = inner
        .last_sequence_by_shard
        .get(&shard_id)
        .copied()
        .unwrap_or_default();
    if wal_fast_append_seq() {
        if let (true, Some(&verified_len)) = (
            inner.last_sequence_by_shard.contains_key(&shard_id),
            inner.verified_len_by_shard.get(&shard_id),
        ) {
            let path = write_ahead_log_path(&inner.root, shard_id);
            let on_disk_len = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            if on_disk_len == verified_len {
                return Ok((cached_last_sequence, on_disk_len));
            }
        }
    }
    inner.stats.append_full_scans = inner.stats.append_full_scans.saturating_add(1);
    let disk_last_sequence = last_wal_sequence_at(&inner.root, shard_id)?;
    // `last_wal_sequence_at` may have truncated a torn tail; refresh the verified length to the
    // reconciled on-disk length so the next fast-path comparison is against the real file.
    let reconciled_len = write_ahead_log_path(&inner.root, shard_id)
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    inner.verified_len_by_shard.insert(shard_id, reconciled_len);
    Ok((cached_last_sequence.max(disk_last_sequence), reconciled_len))
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

/// TS_GROUP_COMMIT: coalesce concurrent WAL fsyncs into shared durability barriers.
/// The WAL append still records every byte durably before ack; only the fsync is
/// batched across writers. Default ON (set TS_GROUP_COMMIT=0 to force exact per-append
/// fsync behavior); every acked write is still durable before its ack returns.
fn group_commit_enabled() -> bool {
    match std::env::var("TS_GROUP_COMMIT") {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => true,
    }
}

/// Public read of the `TS_GROUP_COMMIT` (config `[wal] group_commit`) gate so paths outside this
/// module — notably the shared-store object-store SYNC writer — honor the SAME switch as the local
/// WAL fsync coalescing. Default ON.
pub fn group_commit_configured() -> bool {
    group_commit_enabled()
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

/// Whether the redundant per-append WAL parent-dir fsync may be skipped (safe once the
/// file exists). Enabled under the single-barrier default or group-commit; restored to a
/// per-append dir fsync only under the TS_WAL_LEGACY_RECOVERY escape hatch with group-commit off.
fn wal_relaxed_dir_sync() -> bool {
    !wal_env_flag_on("TS_WAL_LEGACY_RECOVERY") || group_commit_enabled()
}

fn append_record_locked(
    inner: &mut WriteAheadLogInner,
    record: &WriteAheadLogRecord,
    sync: bool,
    known_offset: Option<u64>,
) -> Result<WriteAheadLogAppendReport, WriteAheadLogError> {
    let path = write_ahead_log_path(&inner.root, record.shard_id);
    // The caller has usually just measured this under the same append lock, so taking its answer
    // costs nothing and asking again costs a stat.
    let offset = match known_offset {
        Some(offset) => offset,
        None => path.metadata().map(|metadata| metadata.len()).unwrap_or(0),
    };
    // Frame the record with a length + SHA-256 digest so a later value-preserving bit-flip in
    // this committed line is detected on read (see `crate::log_framing`). Offsets/stats below
    // use the real byte length, so framing is transparent to the append report and replication.
    let bytes = crate::log_framing::encode_line(&serde_json::to_vec(record)?);
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    file.write_all(&bytes)?;
    if sync {
        file.flush()?;
        file.sync_data()?;
        // The parent-directory entry for the WAL file only needs a durable barrier when
        // the file is first created; appends grow the file (inode) without changing the
        // directory. Under relaxed-sync (single-barrier default / TS_GROUP_COMMIT) skip the
        // redundant per-append dir fsync once the file already has content (offset > 0).
        if offset == 0 || !wal_relaxed_dir_sync() {
            sync_parent_dir(&path)?;
        }
        inner.stats.flushes += 1;
        inner.stats.syncs += 1;
        inner.stats.last_flushed_sequence = record.sequence;
    }
    // Ask the handle that was just written, not the path: it is cheaper, and it is the file
    // this record actually went into rather than whatever the name refers to now.
    let persistent_bytes = file.metadata()?.len();
    inner.stats.writes += 1;
    inner.stats.bytes_written += bytes.len() as u64;
    inner.stats.persistent_bytes = persistent_bytes;
    Ok(WriteAheadLogAppendReport {
        shard_id: record.shard_id,
        requested_sequence: record.sequence,
        current_sequence: record.sequence,
        appended: true,
        offset,
        size: bytes.len() as u64,
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
    let path = write_ahead_log_path(&inner.root, shard_id);
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
fn last_wal_sequence_at(root: &Path, shard_id: ShardId) -> Result<u64, WriteAheadLogError> {
    let path = write_ahead_log_path(root, shard_id);
    if !path.exists() {
        return Ok(0);
    }
    let (_, header_len) = read_wal_base(&path)?;
    let file = OpenOptions::new().read(true).write(true).open(&path)?;
    let len = file.metadata()?.len();
    if len <= header_len {
        return Ok(0);
    }

    // Pull in the tail, growing the window until it holds a whole record. Records are small and
    // the first window almost always suffices; the loop is for the ones that are not.
    let mut window = 64 * 1024u64;
    let (line, good_offset) = loop {
        let window_start = header_len.max(len.saturating_sub(window));
        let mut reader = BufReader::new(file.try_clone()?);
        reader.seek(SeekFrom::Start(window_start))?;
        let mut data = vec![0u8; (len - window_start) as usize];
        reader.read_exact(&mut data)?;

        // Everything after the final newline was never finished being written.
        let Some(last_newline) = data.iter().rposition(|byte| *byte == b'\n') else {
            if window_start == header_len {
                // Not one complete record in the file: the whole body is a torn write.
                break (None, header_len);
            }
            window = window.saturating_mul(4);
            continue;
        };
        let good_offset = window_start + last_newline as u64 + 1;

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
                return Ok(0);
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
            Some(index) => break (Some(data[index..line_end].to_vec()), good_offset),
            None if window_start == header_len => {
                break (Some(data[..line_end].to_vec()), good_offset)
            }
            // The record starts before the window: widen and look again.
            None => window = window.saturating_mul(4),
        }
    };

    if good_offset < len {
        file.set_len(good_offset)?;
        file.sync_all()?;
        sync_parent_dir(&path)?;
    }
    let Some(line) = line else {
        return Ok(0);
    };
    Ok(decode_wal_line(&line)?.sequence)
}

fn current_time_ms() -> u64 {
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
            dir.sync_all()?;
        }
    }
    Ok(())
}

fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Command;

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
        assert_eq!(last_wal_sequence_at(dir.path(), 1).unwrap(), 3);
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
        store.gc_before_sequence(1, 6).unwrap();
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
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 4);
        let corrupted = format!(
            "{}\ncorrupt-not-json\n{}\n{}\n",
            lines[0], lines[2], lines[3]
        );
        std::fs::write(&path, corrupted).unwrap();
        // scan drives last_wal_sequence_at, which must surface the interior corruption as an
        // error rather than silently truncating away records 3 & 4 (which would defeat the
        // strict replay-continuity DataLoss guard).
        let restarted = LocalWriteAheadLogStore::new(dir.path());
        assert!(
            restarted.scan(1, 0, u64::MAX, u64::MAX).is_err(),
            "interior WAL corruption must be fatal, not silently truncated to the last good record"
        );
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
            command: Command::StringSet {
                key: key.to_string(),
                value: b"v".to_vec(),
            },
            metadata: None,
            staged_pages: Vec::new(),
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
        let gc: WalGcReport = store.gc_before_sequence(5, 1).unwrap();
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

        let report = store.gc_before_sequence(7, 3).unwrap();
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

        // By default the record does not carry it -- 147 fsynced bytes per write saying what
        // the record already says.
        std::env::remove_var("TS_WAL_ITEM_METADATA");
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

        // The escape hatch restores it for a consumer reading records directly.
        std::env::set_var("TS_WAL_ITEM_METADATA", "1");
        let full = WriteAheadLogRecordMetadata::single_command(&command);
        std::env::remove_var("TS_WAL_ITEM_METADATA");
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
            command,
            staged_pages: Vec::new(),
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

    /// Decode the shard's WAL the way GC does, so a test can assert on what survived.
    fn sequences_on_disk(root: &std::path::Path, shard: ShardId) -> Vec<u64> {
        let bytes = std::fs::read(write_ahead_log_path(root, shard)).unwrap();
        bytes[data_start(&bytes)..]
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| decode_wal_line(line).unwrap().sequence)
            .collect()
    }

    /// Byte offset at which each record starts, in order, past any base header.
    fn record_offsets_on_disk(root: &std::path::Path, shard: ShardId) -> Vec<usize> {
        let bytes = std::fs::read(write_ahead_log_path(root, shard)).unwrap();
        let start = data_start(&bytes);
        let mut offsets = vec![start];
        for (index, byte) in bytes.iter().enumerate().skip(start) {
            if *byte == b'\n' && index + 1 < bytes.len() {
                offsets.push(index + 1);
            }
        }
        offsets
    }

    /// Bytes of the record starting at `offset`, including its newline.
    fn record_bytes_at(root: &std::path::Path, shard: ShardId, offset: usize) -> Vec<u8> {
        let bytes = std::fs::read(write_ahead_log_path(root, shard)).unwrap();
        let end = bytes[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|index| offset + index + 1)
            .unwrap_or(bytes.len());
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
    fn gc_is_unconstrained_when_no_block_retention_floor_is_registered() {
        // The floor is opt-in: a caller that puts no blocks in the WAL must see the reclaim
        // behaviour it had before the floor existed.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        assert_eq!(store.block_retention_floor(1), None);
        let report = store.gc_before_sequence(1, 4).unwrap();

        assert_eq!(report.records_before, 6);
        assert_eq!(report.records_after, 3, "sequences 4, 5, 6 survive");
        assert!(!report.clamped_by_block_retention);
        assert_eq!(report.effective_retain_from_sequence, 4);
    }

    #[test]
    fn gc_will_not_reclaim_past_the_block_retention_floor() {
        // Records at or above the floor may hold the only copy of a block's bytes. Reclaiming
        // them destroys data the served index still points at.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        append_n(&store, 1, 6);

        store.set_block_retention_floor(1, 3);
        let report = store.gc_before_sequence(1, 6).unwrap();

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
        assert_eq!(store.gc_before_sequence(1, 6).unwrap().records_after, 5);

        store.set_block_retention_floor(1, 5);
        let report = store.gc_before_sequence(1, 6).unwrap();
        assert_eq!(report.effective_retain_from_sequence, 5);
        assert_eq!(report.records_after, 2, "sequences 5 and 6");

        store.clear_block_retention_floor(1);
        assert_eq!(store.block_retention_floor(1), None);
        let report = store.gc_before_sequence(1, 6).unwrap();
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
        let report = store.gc_before_sequence(1, u64::MAX).unwrap();

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

        store.gc_before_sequence(1, 4).unwrap();

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

        store.gc_before_sequence(1, 4).unwrap();

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

        let raw_before = std::fs::read(write_ahead_log_path(dir.path(), 1)).unwrap();
        let offsets = record_offsets_on_disk(dir.path(), 1);
        // Reclaiming from sequence 4 keeps records from index 3 onward.
        let split = offsets[3];
        let suffix_before = raw_before[split..].to_vec();

        store.gc_before_sequence(1, 4).unwrap();

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

        store.gc_before_sequence(1, 4).unwrap();
        let base_after_first = store.base_offset(1).unwrap();
        store.gc_before_sequence(1, 9).unwrap();
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
        store.gc_before_sequence(1, 5).unwrap();

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
            let line = bytes.split(|byte| *byte == 10u8).next().unwrap();
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

        store.gc_before_sequence(1, 5).unwrap();
        let base = store.base_offset(1).unwrap();
        assert!(base > 0, "the reclaim must have moved the base");

        for (log_id, sequence) in &reported {
            match store.read_at_log_id(1, *log_id, 4096).unwrap() {
                Some(bytes) => {
                    let line = bytes.split(|byte| *byte == 10u8).next().unwrap();
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
        store.gc_before_sequence(1, 4).unwrap();
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
        let line = bytes.split(|byte| *byte == 10u8).next().unwrap();
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
            command: Command::StringSet {
                key: "k".to_string(),
                value: Vec::new(),
            },
            metadata: None,
            staged_pages: vec![StagedPage {
                object_id: 7,
                bytes: page.clone(),
            }],
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
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
            metadata: None,
            staged_pages: Vec::new(),
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

    /// What a write costs on disk and in time, comparing the two payload shapes in ONE run.
    ///
    /// Both are measured back to back in the same process, alternating, because a timing taken
    /// from a separate run on a shared machine mostly measures what else was running then. The
    /// byte counts are deterministic; the timings are not, so they are reported as a ratio
    /// between two shapes measured together rather than as absolute figures.
    #[test]
    fn payload_shape_footprint_and_latency() {
        fn run(array_shape: bool, value_len: usize, records: u64) -> (u64, f64) {
            crate::bytes_serde::set_array_shape_for_measurement(array_shape);
            let dir = tempfile::tempdir().unwrap();
            let store = LocalWriteAheadLogStore::new(dir.path());
            let started = std::time::Instant::now();
            for index in 0..records {
                store
                    .append_with_sync(
                        1,
                        Command::StringSet {
                            key: format!("bench-key-{index:08}"),
                            value: vec![118u8; value_len],
                        },
                        false,
                    )
                    .unwrap();
            }
            let micros = started.elapsed().as_secs_f64() * 1e6 / records as f64;
            let bytes = std::fs::metadata(write_ahead_log_path(dir.path(), 1))
                .unwrap()
                .len();
            (bytes, micros)
        }

        let median = |mut values: Vec<f64>| {
            values.sort_by(|left, right| left.partial_cmp(right).unwrap());
            values[values.len() / 2]
        };

        for value_len in [64usize, 256, 1024, 4096] {
            let records = 200u64;
            let (array_bytes, _) = run(true, value_len, records);
            let (encoded_bytes, _) = run(false, value_len, records);
            let mut array_us = Vec::new();
            let mut encoded_us = Vec::new();
            // Alternate, so a burst of load on the machine lands on both and not just one.
            for _ in 0..3 {
                array_us.push(run(true, value_len, records).1);
                encoded_us.push(run(false, value_len, records).1);
            }
            let array_us = median(array_us);
            let encoded_us = median(encoded_us);
            let user = records * value_len as u64;
            println!(
                "  value {value_len:>5}B: array {array_bytes:>9} B ({:.2}x user, {array_us:>7.1} us/write)   encoded {encoded_bytes:>9} B ({:.2}x user, {encoded_us:>7.1} us/write)   -> {:.2}x smaller, {:.2}x faster",
                array_bytes as f64 / user as f64,
                encoded_bytes as f64 / user as f64,
                array_bytes as f64 / encoded_bytes as f64,
                array_us / encoded_us
            );
            assert!(
                encoded_bytes < array_bytes,
                "the encoded shape must be smaller at {value_len}B"
            );
        }
        // Leave the process on the default for whatever test runs next.
        crate::bytes_serde::set_array_shape_for_measurement(false);
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
            Command::StringSet {
                key: "k".to_string(),
                value: b"hi".to_vec(),
            }
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
            command: Command::StringSet {
                key: "round-trip".to_string(),
                value: vec![0u8, 127, 255],
            },
            metadata: Some(WriteAheadLogRecordMetadata {
                version: WRITE_AHEAD_LOG_FORMAT_VERSION,
                timestamp_ms: 42,
                items: Vec::new(),
                batch_id: None,
                batch_size: None,
                batch_index: None,
            }),
            staged_pages: Vec::new(),
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
                last = last_wal_sequence_at(dir.path(), 1).unwrap();
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
        let whole = std::fs::read(&path).unwrap();
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
        let mut padded = std::fs::read(&path).unwrap();
        padded.extend_from_slice(b"\n\n   \n");
        std::fs::write(&path, padded).unwrap();

        assert_eq!(
            last_wal_sequence_at(dir.path(), 1).unwrap(),
            2,
            "blank trailing lines should be stepped over, not read as the end of the log"
        );
    }
}
