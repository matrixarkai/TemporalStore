// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use temporalstore_snapshot::object_store::{
    AppendBlobReceipt, FileObjectStore, MatrixObjectHttpStore, ObjectStore, ObjectStoreError,
};
use thiserror::Error;

use tokio::sync::oneshot;

use crate::block_store::{BlockStoreError, LazyCheckpointBand, LocalBlockStore, SharedSlabSource};
use crate::engine::TemporalEngine;
use crate::sdk::{self, v1};
use crate::types::{Command, ExecuteRequest, ShardId, Status};

#[derive(Debug, Error)]
pub enum SharedStoreReplicationError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("page store error: {0}")]
    BlockStore(#[from] BlockStoreError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),
    #[error("checksum mismatch for {path}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    #[error("no shared-store checkpoint found for shard {0}")]
    CheckpointNotFound(ShardId),
    #[error(
        "checkpoint for shard {shard_id} references live slab {page_slab_id} that was not uploaded; refusing to publish a manifest that would lose durable pages"
    )]
    CheckpointSlabNotDurable {
        shard_id: ShardId,
        page_slab_id: u64,
    },
    #[error("replicated command failed at WAL index {wal_index}: {status:?}")]
    ApplyFailed { wal_index: u64, status: Status },
    /// The durable single-writer fence rejected this operation: a newer owner (higher
    /// load_version) holds the shard lease, so this (stale) writer must ABORT rather than
    /// double-append to the shared WAL. Maps [`ObjectStoreError::ConditionFailed`].
    #[error("shared-store fence rejected write: {detail}")]
    StoreConditionFailed { detail: String },
    /// A lease acquisition was refused because the object store already holds a lease at an
    /// equal-or-higher load_version — this writer's ownership claim is stale.
    #[error(
        "shared-store lease for shard {shard_id} held at load_version {current} >= attempted {attempted}"
    )]
    StaleOwnership {
        shard_id: ShardId,
        current: u64,
        attempted: u64,
    },
    #[error("WAL replay gap: expected index {expected}, got {actual}")]
    ReplayGap { expected: u64, actual: u64 },
    #[error(
        "shared-store GC would remove WAL entry needed by replay cursor {cursor_wal_index} before retain {retain_from_wal_index}"
    )]
    GcBlockedByReplayCursor {
        cursor_wal_index: u64,
        retain_from_wal_index: u64,
    },
    #[error(
        "shared-store checkpoint GC would remove checkpoint {checkpoint_id} at WAL index {checkpoint_wal_index} needed by replay cursor {cursor_wal_index}"
    )]
    CheckpointGcBlockedByReplayCursor {
        cursor_wal_index: u64,
        checkpoint_wal_index: u64,
        checkpoint_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalEntry {
    pub shard_id: ShardId,
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    /// The operation, for an entry that cannot say what it DID.
    ///
    /// Absent once the entry carries results, exactly as in the engine record it comes from. An
    /// entry published before results existed still carries one, and a successor still replays it
    /// by re-running it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Command>,
    /// The pages this write produced, carried beside the command that produced them.
    ///
    /// A command is not always enough to rebuild what it wrote. A page can be DERIVED state --
    /// a serialized counter series, a hash map -- and re-executing the command that bumped it
    /// reconstructs it only if every earlier write is also replayed, in order, from a state
    /// that still exists. Locally that is what the engine WAL record carries, for exactly this
    /// reason; a shared log that carried commands alone could hand a successor a shard it could
    /// not finish rebuilding.
    ///
    /// Empty for the overwhelming majority of writes, and `serde(default)` so an entry written
    /// before this field existed still loads.
    #[serde(default)]
    pub staged_pages: Vec<crate::wal::StagedPage>,
    /// What this write DID, so a successor can install results instead of re-running operations.
    ///
    /// Carrying pages was the same idea reached halfway: a page is derived state the command
    /// cannot rebuild, so the pages travelled beside the command. These finish the thought. A
    /// successor that installs them needs no clock of its own, no eviction config from the
    /// original node, and no assumption that re-executing here lands where it landed there.
    ///
    /// `serde(default)` and skipped when empty, so an entry written before this reads back
    /// unchanged and replays exactly as it used to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outcomes: Vec<crate::wal::WalOutcomeItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalObject {
    pub entry: SharedStoreWalEntry,
    pub entry_byte_size: u64,
    pub entry_sha256: String,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreWalOffsetMetadata {
    pub shard_id: ShardId,
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    pub wal_blob_key: String,
    pub wal_blob_start_offset: u64,
    pub wal_blob_end_offset: u64,
    pub wal_blob_bytes_written: u64,
    pub wal_blob_object_length: u64,
    #[serde(default)]
    pub wal_blob_physical_band_count: u64,
    pub wal_blob_first_physical_offset: Option<u64>,
    pub command_byte_size: u64,
    pub command_sha256: String,
    pub command_encoding: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SharedStoreWalIndexedRead {
    pub metadata: SharedStoreWalOffsetMetadata,
    pub entry: SharedStoreWalEntry,
    pub range_bytes_read: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStorePageSlab {
    #[serde(alias = "page_segment_id")]
    pub page_slab_id: u64,
    pub key: String,
    pub byte_size: u64,
    pub sha256: String,
    // Per-slab SEALED-band metadata carried so a lazy restore can install complete band
    // descriptors (physical/logical bytes + page-id range) BEFORE the first on-demand slab
    // fetch, keeping GC/compaction accounting from under-counting sealed shared bands between
    // restore and first fetch. All default so older manifests (without these fields) still load;
    // `byte_size` above is the slab's physical byte size.
    #[serde(default)]
    pub logical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreCheckpointManifest {
    pub cluster_id: String,
    pub shard_id: ShardId,
    pub checkpoint_id: String,
    #[serde(rename = "checkpoint_wal_index")]
    pub checkpoint_wal_index: u64,
    pub created_at_ms: u64,
    pub index_key: String,
    pub index_byte_size: u64,
    pub index_sha256: String,
    #[serde(alias = "page_segments")]
    pub page_slabs: Vec<SharedStorePageSlab>,
    /// Next free block page id at checkpoint time. A lazy restore advances the fresh
    /// owner's page-id counter past this floor so replayed/new writes never reuse a
    /// page id that a lazily-fetched checkpoint slab still carries. Defaults to 0 for
    /// manifests written before this field existed (backward compatible).
    #[serde(default)]
    pub next_page_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreReplayCursor {
    pub shard_id: ShardId,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
    pub last_replay_time_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SharedStoreStorageMode {
    Sync,
    Async,
}

impl Default for SharedStoreStorageMode {
    fn default() -> Self {
        Self::Async
    }
}

impl SharedStoreStorageMode {
    pub fn from_sync_flag(sync: bool) -> Self {
        if sync {
            Self::Sync
        } else {
            Self::Async
        }
    }

    pub fn is_sync(self) -> bool {
        matches!(self, Self::Sync)
    }

    pub fn is_async(self) -> bool {
        matches!(self, Self::Async)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SharedStoreWalAppendMode {
    JsonPerKey,
    ProtobufAppendBlob,
}

impl Default for SharedStoreWalAppendMode {
    fn default() -> Self {
        Self::JsonPerKey
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreWriteReport {
    #[serde(rename = "wal_index")]
    pub wal_index: u64,
    pub published: bool,
    pub queued: bool,
    #[serde(default)]
    pub wal_blob_start_offset: Option<u64>,
    #[serde(default)]
    pub wal_blob_end_offset: Option<u64>,
    #[serde(default)]
    pub wal_blob_bytes_written: Option<u64>,
    #[serde(default)]
    pub wal_blob_object_length: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreFlushReport {
    pub flushed: usize,
    pub remaining: usize,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
    #[serde(default)]
    pub last_wal_blob_start_offset: Option<u64>,
    #[serde(default)]
    pub last_wal_blob_end_offset: Option<u64>,
    #[serde(default)]
    pub last_wal_blob_object_length: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreGcReport {
    pub shard_id: ShardId,
    #[serde(rename = "deleted_wal_objects")]
    pub deleted_wal_objects: usize,
    pub deleted_checkpoints: usize,
    pub deleted_checkpoint_objects: usize,
    pub retained_checkpoint_ids: Vec<String>,
    #[serde(rename = "retained_for_cursor_wal_index", default)]
    pub retained_for_cursor_wal_index: Option<u64>,
    #[serde(default)]
    pub retained_for_cursor_checkpoint_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SharedStoreRetryPolicy {
    pub max_attempts: usize,
    pub backoff_ms: u64,
}

impl Default for SharedStoreRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff_ms: 0,
        }
    }
}

#[derive(Debug)]
pub struct SharedStoreReplicator<O> {
    cluster_id: String,
    object_store: Arc<O>,
    retry_policy: SharedStoreRetryPolicy,
    wal_append_mode: SharedStoreWalAppendMode,
    /// Durable single-writer fence, configured by `with_fence` and applying whenever it is.
    /// Every WAL append and checkpoint publish then re-validates that this writer's
    /// `load_version` still owns the shard lease in the object store, aborting a superseded
    /// stale owner before it can double-append.
    ///
    /// `TS_SHARED_STORE_FENCE` used to gate this as well, so a caller could configure a fence
    /// and have it silently not apply. Nothing outside the R2 test configures one, so nothing
    /// production does changes by the gate going: with no fence there is nothing to enforce.
    fence: Option<ShardFenceConfig>,
}

/// Ownership token carried by a fenced replicator: the `load_version` (monotonic ownership
/// epoch) this writer believes it holds, plus a human-readable owner tag for diagnostics.
#[derive(Debug, Clone)]
struct ShardFenceConfig {
    load_version: u64,
    /// Retained for diagnostics / future lease-holder attribution; the fence decision keys on
    /// `load_version` alone.
    #[allow(dead_code)]
    owner: String,
}

/// Durable lease object persisted in the shared object store, keyed by cluster+shard. The
/// `load_version` is the fence token: a writer may only install a lease strictly greater than
/// the currently stored one, and may only append while the stored value still equals its own.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ShardLease {
    load_version: u64,
    owner: String,
}

/// TS_SHARED_STORE_MAX_PENDING: how many entries the async queue may hold before a write
/// stops being allowed to defer its own durability.
///
/// The async path acks a write once its entry is on an in-memory queue, so the queue depth IS
/// the size of what a non-graceful exit loses. Unbounded, a store that is merely slow turns
/// every ack into memory the process never gets back, and the eventual loss is the entire
/// backlog -- the failure gets worse the longer it goes unnoticed, which is backwards.
///
/// At the cap the next write publishes itself synchronously instead of queueing. The ack slows
/// to the store's own latency, which is precisely the signal a caller needs, and the backlog
/// stops growing. `0` restores the previous unbounded behaviour.
fn shared_store_max_pending() -> usize {
    std::env::var("TS_SHARED_STORE_MAX_PENDING")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(50_000)
}

impl<O> Clone for SharedStoreReplicator<O> {
    fn clone(&self) -> Self {
        Self {
            cluster_id: self.cluster_id.clone(),
            object_store: Arc::clone(&self.object_store),
            retry_policy: self.retry_policy,
            wal_append_mode: self.wal_append_mode,
            fence: self.fence.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct SharedStoreStagedPageProto {
    #[prost(uint64, tag = "1")]
    object_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct SharedStoreWalFrameProto {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    wal_index: u64,
    #[prost(bytes = "vec", tag = "3")]
    command_payload: Vec<u8>,
    #[prost(uint64, tag = "4")]
    command_byte_size: u64,
    #[prost(string, tag = "5")]
    command_sha256: String,
    #[prost(uint32, tag = "6")]
    command_encoding: u32,
    /// Tag 7, added after the fact: an older reader ignores it and an older writer leaves it
    /// empty, so both directions stay readable across the change.
    #[prost(message, repeated, tag = "7")]
    staged_pages: Vec<SharedStoreStagedPageProto>,
    /// What the write DID, in the SAME message the engine log uses.
    ///
    /// Not a shared-store item type: the shared log needs a destination, not a schema. Carrying
    /// the engine's item means a successor installs exactly what the origin recorded, with no
    /// conversion in between to disagree about.
    ///
    /// Tag 8, same compatibility shape as tag 7. Without it this path would carry the command and
    /// silently drop the results, which is worse than not carrying them at all -- the successor
    /// would re-execute and look correct.
    #[prost(message, repeated, tag = "8")]
    items: Vec<crate::sdk::v1::EngineWalItem>,
}

#[derive(Clone, PartialEq, Message)]
struct SharedStoreWalOffsetMetadataProto {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    wal_index: u64,
    #[prost(string, tag = "3")]
    wal_blob_key: String,
    #[prost(uint64, tag = "4")]
    wal_blob_start_offset: u64,
    #[prost(uint64, tag = "5")]
    wal_blob_end_offset: u64,
    #[prost(uint64, tag = "6")]
    wal_blob_bytes_written: u64,
    #[prost(uint64, tag = "7")]
    wal_blob_object_length: u64,
    #[prost(uint64, tag = "8")]
    command_byte_size: u64,
    #[prost(string, tag = "9")]
    command_sha256: String,
    #[prost(uint32, tag = "10")]
    command_encoding: u32,
    #[prost(uint64, tag = "11")]
    wal_blob_physical_band_count: u64,
    #[prost(uint64, optional, tag = "12")]
    wal_blob_first_physical_offset: Option<u64>,
}

#[derive(Debug)]
pub struct SharedStoreStorageWriter<O> {
    replicator: SharedStoreReplicator<O>,
    mode: SharedStoreStorageMode,
    next_wal_index: AtomicU64,
    pending: Mutex<VecDeque<SharedStoreWalEntry>>,
    /// Depth at which the async path stops deferring. See [`shared_store_max_pending`].
    max_pending: usize,
    /// How many writes have been forced to publish themselves because the queue was full.
    /// Non-zero means the store is not keeping up and acks are paying for it.
    capacity_hits: AtomicU64,
    /// Timer-less queue-coalesced group commit for the SYNC path. When enabled, concurrent
    /// sync writers append their entry to `commit` and await a covering durable barrier instead
    /// of each publishing inline, amortizing N object-store appends onto ~1 per group. Ignored
    /// on the async path (that path is already off the ack critical path).
    group_commit: bool,
    /// Optional deliberate widening of the group-commit window under extreme load. 0 (default)
    /// keeps it purely timer-less — the group is exactly what accumulates during one append's
    /// in-flight duration.
    commit_delay: Duration,
    /// Group-commit staging buffer + leader flag. A writer pushes its `(entry, waker)` here; the
    /// first arrival becomes the flusher (leader) and every later arrival awaits its waker.
    commit: Mutex<GroupCommitBuffer>,
}

/// One sync writer waiting on the group-commit barrier: its entry (staged for the next flush)
/// and the channel the flusher wakes once the covering append has (or has not) reached the store.
#[derive(Debug)]
struct GroupCommitWaiter {
    entry: SharedStoreWalEntry,
    waker: oneshot::Sender<GroupCommitOutcome>,
}

#[derive(Debug, Default)]
struct GroupCommitBuffer {
    /// Entries appended by writers but not yet covered by a completed durable barrier.
    queue: VecDeque<GroupCommitWaiter>,
    /// A leader is currently running (or about to run) a flush round. Only the leader clears this,
    /// and only under the lock once the queue is drained empty — so no entry is ever stranded.
    flushing: bool,
}

/// Result the flusher hands each waiter. Errors are stringified so a single failed append can
/// fan out to every covered writer (the typed `SharedStoreReplicationError` is not `Clone`); a
/// stringified error is still an error — the durability contract only requires that a covered
/// writer whose barrier failed is NEVER told Ok.
#[derive(Debug, Clone)]
enum GroupCommitOutcome {
    Committed(SharedStoreWriteReport),
    Failed(String),
}

impl GroupCommitOutcome {
    fn into_result(self) -> Result<SharedStoreWriteReport, SharedStoreReplicationError> {
        match self {
            GroupCommitOutcome::Committed(report) => Ok(report),
            GroupCommitOutcome::Failed(message) => Err(SharedStoreReplicationError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, message),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayReport {
    pub applied: usize,
    #[serde(rename = "last_wal_index")]
    pub last_wal_index: u64,
    #[serde(default)]
    pub offset_index_reads: usize,
    #[serde(default)]
    pub range_bytes_read: u64,
}

impl<O> SharedStoreReplicator<O>
where
    O: ObjectStore + 'static,
{
    pub fn new(cluster_id: impl Into<String>, object_store: Arc<O>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            object_store,
            retry_policy: SharedStoreRetryPolicy::default(),
            wal_append_mode: SharedStoreWalAppendMode::default(),
            fence: None,
        }
    }

    pub fn with_retry_policy(
        cluster_id: impl Into<String>,
        object_store: Arc<O>,
        retry_policy: SharedStoreRetryPolicy,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            object_store,
            retry_policy: SharedStoreRetryPolicy {
                max_attempts: retry_policy.max_attempts.max(1),
                backoff_ms: retry_policy.backoff_ms,
            },
            wal_append_mode: SharedStoreWalAppendMode::default(),
            fence: None,
        }
    }

    pub fn with_wal_append_mode(mut self, mode: SharedStoreWalAppendMode) -> Self {
        self.wal_append_mode = mode;
        self
    }

    /// Attach a durable single-writer fence: this replicator (and every storage writer it
    /// spawns) claims `load_version` for its shard leases, and re-validates that claim on every
    /// append and checkpoint. Combine with
    /// [`acquire_shard_lease`](Self::acquire_shard_lease) to install the lease.
    ///
    /// Attaching one IS enabling it. `TS_SHARED_STORE_FENCE` used to be a second condition, so a
    /// caller could ask for a fence and not get one.
    pub fn with_fence(mut self, load_version: u64, owner: impl Into<String>) -> Self {
        self.fence = Some(ShardFenceConfig {
            load_version,
            owner: owner.into(),
        });
        self
    }

    fn lease_key(&self, shard_id: ShardId) -> String {
        format!("leases/{}/shard_{}.lease", self.cluster_id, shard_id)
    }

    /// Durably claim ownership of `shard_id` at `load_version` via a store compare-and-set.
    /// Succeeds only if the currently persisted lease is absent or holds a strictly lower
    /// load_version; a stale writer (equal-or-lower load_version) is rejected with
    /// [`SharedStoreReplicationError::StaleOwnership`], and a lost CAS race with
    /// [`SharedStoreReplicationError::StoreConditionFailed`]. This is the ONLY way a writer
    /// becomes the fenced owner — it does not mutate in-memory state, so ownership is decided
    /// by the durable store, not a local view.
    pub async fn acquire_shard_lease(
        &self,
        shard_id: ShardId,
        load_version: u64,
        owner: impl Into<String>,
    ) -> Result<(), SharedStoreReplicationError> {
        let key = self.lease_key(shard_id);
        let current_bytes = match self.object_store.get(&key).await {
            Ok(bytes) => Some(bytes),
            Err(ObjectStoreError::NotFound(_)) => None,
            Err(err) => return Err(err.into()),
        };
        if let Some(bytes) = &current_bytes {
            let current: ShardLease = serde_json::from_slice(bytes)?;
            if current.load_version >= load_version {
                return Err(SharedStoreReplicationError::StaleOwnership {
                    shard_id,
                    current: current.load_version,
                    attempted: load_version,
                });
            }
        }
        let lease = ShardLease {
            load_version,
            owner: owner.into(),
        };
        let new_bytes = Bytes::from(serde_json::to_vec(&lease)?);
        match self
            .object_store
            .compare_and_swap(&key, current_bytes, new_bytes)
            .await
        {
            Ok(()) => Ok(()),
            Err(ObjectStoreError::ConditionFailed { detail, .. }) => {
                Err(SharedStoreReplicationError::StoreConditionFailed { detail })
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Re-read the durable lease and confirm it still records `load_version`. Any other value
    /// (a newer owner took over) or a missing lease is a fence breach → StoreConditionFailed.
    pub async fn validate_shard_lease(
        &self,
        shard_id: ShardId,
        load_version: u64,
    ) -> Result<(), SharedStoreReplicationError> {
        let key = self.lease_key(shard_id);
        let bytes = match self.object_store.get(&key).await {
            Ok(bytes) => bytes,
            Err(ObjectStoreError::NotFound(_)) => {
                return Err(SharedStoreReplicationError::StoreConditionFailed {
                    detail: format!("shard {shard_id} lease missing; ownership not held"),
                });
            }
            Err(err) => return Err(err.into()),
        };
        let current: ShardLease = serde_json::from_slice(&bytes)?;
        if current.load_version != load_version {
            return Err(SharedStoreReplicationError::StoreConditionFailed {
                detail: format!(
                    "shard {shard_id} lease held by load_version {} (owner {}), not {}",
                    current.load_version, current.owner, load_version
                ),
            });
        }
        Ok(())
    }

    /// Fence check invoked on every WAL append + checkpoint publish. A no-op unless a fence is
    /// configured, which is a decision the caller makes by name, so the default write path is
    /// unchanged. When active it re-validates the durable lease, aborting a superseded writer.
    async fn enforce_fence(
        &self,
        shard_id: ShardId,
    ) -> Result<(), SharedStoreReplicationError> {
        let Some(fence) = &self.fence else {
            return Ok(());
        };
        self.validate_shard_lease(shard_id, fence.load_version).await
    }

    pub async fn publish_wal_entry(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        // R2 single-writer fence: re-validate ownership before appending to the shared WAL so
        // a partitioned-but-alive stale owner is rejected rather than double-appending.
        self.enforce_fence(entry.shard_id).await?;
        if matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        ) {
            return self.publish_wal_entry_protobuf_blob(entry).await;
        }
        let key = self.wal_key(entry.shard_id, entry.wal_index);
        let entry_bytes = serde_json::to_vec(&entry)?;
        let object = SharedStoreWalObject {
            entry,
            entry_byte_size: entry_bytes.len() as u64,
            entry_sha256: sha256_hex(&entry_bytes),
        };
        self.put_with_retry(&key, Bytes::from(serde_json::to_vec(&object)?))
            .await?;
        Ok(None)
    }

    async fn publish_wal_entry_protobuf_blob(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        let key = self.wal_blob_key(entry.shard_id);
        let command_metadata = wal_command_metadata(entry.command.as_ref())?;
        let frame = encode_wal_proto_frame(&entry)?;
        let receipt = self
            .append_blob_with_retry(&key, Bytes::from(frame))
            .await?
            .expect("protobuf append blob must return a receipt");
        self.publish_wal_offset_metadata(&entry, &key, &receipt, command_metadata)
            .await?;
        Ok(Some(receipt))
    }

    async fn publish_wal_offset_metadata(
        &self,
        entry: &SharedStoreWalEntry,
        wal_blob_key: &str,
        receipt: &AppendBlobReceipt,
        command_metadata: WalCommandMetadata,
    ) -> Result<(), SharedStoreReplicationError> {
        let metadata = SharedStoreWalOffsetMetadata {
            shard_id: entry.shard_id,
            wal_index: entry.wal_index,
            wal_blob_key: wal_blob_key.to_string(),
            wal_blob_start_offset: receipt.start_offset,
            wal_blob_end_offset: receipt.end_offset,
            wal_blob_bytes_written: receipt.bytes_written,
            wal_blob_object_length: receipt.object_length,
            wal_blob_physical_band_count: receipt.physical_band_count as u64,
            wal_blob_first_physical_offset: receipt.first_physical_offset,
            command_byte_size: command_metadata.byte_size,
            command_sha256: command_metadata.sha256,
            command_encoding: command_metadata.encoding,
        };
        let frame = encode_wal_offset_metadata_frame(&metadata);
        self.append_blob_with_retry(
            &self.wal_offset_index_blob_key(entry.shard_id),
            Bytes::from(frame),
        )
        .await?;
        Ok(())
    }

    /// Group-commit barrier: make an entire batch of WAL entries durable with the FEWEST possible
    /// object-store appends, returning a per-entry [`SharedStoreWriteReport`] aligned with `entries`.
    ///
    /// In `ProtobufAppendBlob` mode the shared WAL is a single per-shard appendable log of
    /// length-prefixed frames, so the whole batch is concatenated and made durable in ONE
    /// `append_blob` (plus ONE append of the concatenated offset-index frames) — `N` entries share
    /// exactly `2` appends instead of `2N`. Per-entry logical offsets are derived from the single
    /// batch receipt (`start_offset` + cumulative frame lengths); `FileObjectStore::append_blob`
    /// appends contiguously from the prior object length, so each frame is individually
    /// range-readable exactly as a one-at-a-time publish would have written it.
    ///
    /// Every entry must belong to the same shard (the caller — a per-shard storage writer —
    /// guarantees this). The fence is re-validated ONCE for the batch. In `JsonPerKey` mode each
    /// index is its own object, so no single-object barrier exists; the batch degrades to per-entry
    /// publishes (still correct, just no fsync coalescing — the win is inherent to append-blob mode).
    ///
    /// Durability: this returns `Ok` only after the covering append(s) have reached the store, so a
    /// caller that fans `Ok` out to waiters can never ack a write before its barrier completed.
    pub async fn publish_wal_entries_batch(
        &self,
        entries: &[SharedStoreWalEntry],
    ) -> Result<Vec<SharedStoreWriteReport>, SharedStoreReplicationError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let shard_id = entries[0].shard_id;
        let single_shard = entries.iter().all(|entry| entry.shard_id == shard_id);
        let append_blob_mode = matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        );
        if !append_blob_mode || !single_shard {
            // No single-object barrier available (JsonPerKey, or a mixed-shard batch): publish each
            // entry on its own durable barrier. Correct, just uncoalesced.
            let mut reports = Vec::with_capacity(entries.len());
            for entry in entries {
                let receipt = self.publish_wal_entry(entry.clone()).await?;
                reports.push(write_report_from_receipt(entry.wal_index, receipt.as_ref()));
            }
            return Ok(reports);
        }

        // R2 single-writer fence: re-validate ownership once before the coalesced append.
        self.enforce_fence(shard_id).await?;
        let wal_blob_key = self.wal_blob_key(shard_id);

        // Concatenate every entry's length-prefixed frame into one append payload, remembering each
        // frame's length (and command metadata) so per-entry offsets can be derived from the single
        // receipt below.
        let mut wal_payload: Vec<u8> = Vec::new();
        let mut frame_lengths: Vec<u64> = Vec::with_capacity(entries.len());
        let mut command_metadata: Vec<WalCommandMetadata> = Vec::with_capacity(entries.len());
        for entry in entries {
            let frame = encode_wal_proto_frame(entry)?;
            frame_lengths.push(frame.len() as u64);
            command_metadata.push(wal_command_metadata(entry.command.as_ref())?);
            wal_payload.extend_from_slice(&frame);
        }
        let receipt = self
            .append_blob_with_retry(&wal_blob_key, Bytes::from(wal_payload))
            .await?
            .expect("protobuf append blob must return a receipt");

        // Derive each entry's [start, end) from the batch receipt and build the concatenated
        // offset-index payload, then make it durable in a single append.
        let mut offset_payload: Vec<u8> = Vec::new();
        let mut reports = Vec::with_capacity(entries.len());
        let mut cursor = receipt.start_offset;
        for ((entry, frame_len), metadata) in entries
            .iter()
            .zip(frame_lengths.iter().copied())
            .zip(command_metadata.into_iter())
        {
            let start = cursor;
            let end = start.saturating_add(frame_len);
            cursor = end;
            let offset_metadata = SharedStoreWalOffsetMetadata {
                shard_id: entry.shard_id,
                wal_index: entry.wal_index,
                wal_blob_key: wal_blob_key.clone(),
                wal_blob_start_offset: start,
                wal_blob_end_offset: end,
                wal_blob_bytes_written: frame_len,
                wal_blob_object_length: receipt.object_length,
                wal_blob_physical_band_count: receipt.physical_band_count as u64,
                wal_blob_first_physical_offset: receipt.first_physical_offset,
                command_byte_size: metadata.byte_size,
                command_sha256: metadata.sha256,
                command_encoding: metadata.encoding,
            };
            offset_payload.extend_from_slice(&encode_wal_offset_metadata_frame(&offset_metadata));
            reports.push(SharedStoreWriteReport {
                wal_index: entry.wal_index,
                published: true,
                queued: false,
                wal_blob_start_offset: Some(start),
                wal_blob_end_offset: Some(end),
                wal_blob_bytes_written: Some(frame_len),
                wal_blob_object_length: Some(receipt.object_length),
            });
        }
        self.append_blob_with_retry(
            &self.wal_offset_index_blob_key(shard_id),
            Bytes::from(offset_payload),
        )
        .await?;
        Ok(reports)
    }

    pub async fn lookup_wal_offset_metadata(
        &self,
        shard_id: ShardId,
        wal_index: u64,
    ) -> Result<Option<SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
        Ok(self
            .load_wal_offset_metadata(shard_id)
            .await?
            .remove(&wal_index))
    }

    pub async fn read_wal_entry_by_offset_metadata(
        &self,
        shard_id: ShardId,
        wal_index: u64,
    ) -> Result<Option<SharedStoreWalIndexedRead>, SharedStoreReplicationError> {
        let Some(metadata) = self
            .lookup_wal_offset_metadata(shard_id, wal_index)
            .await?
        else {
            return Ok(None);
        };
        let length = metadata
            .wal_blob_end_offset
            .checked_sub(metadata.wal_blob_start_offset)
            .ok_or_else(|| SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid WAL offset metadata range for shard {shard_id} index {wal_index}"),
            )))?;
        if length != metadata.wal_blob_bytes_written {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "WAL offset metadata bytes mismatch for shard {shard_id} index {wal_index}"
                ),
            )));
        }
        let bytes = self
            .object_store
            .get_range(
                &metadata.wal_blob_key,
                metadata.wal_blob_start_offset,
                length,
            )
            .await?;
        let entry = decode_wal_proto_frame_exact(&bytes, &metadata.wal_blob_key)?;
        if entry.shard_id != shard_id || entry.wal_index != wal_index {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("WAL offset metadata points to shard {} index {}, expected shard {shard_id} index {wal_index}", entry.shard_id, entry.wal_index),
            )));
        }
        Ok(Some(SharedStoreWalIndexedRead {
            metadata,
            entry,
            range_bytes_read: length,
        }))
    }

    pub fn storage_writer(
        &self,
        mode: SharedStoreStorageMode,
        next_wal_index: u64,
    ) -> SharedStoreStorageWriter<O> {
        SharedStoreStorageWriter {
            replicator: self.clone(),
            mode,
            next_wal_index: AtomicU64::new(next_wal_index.max(1)),
            pending: Mutex::default(),
            max_pending: shared_store_max_pending(),
            capacity_hits: AtomicU64::new(0),
            group_commit: false,
            commit_delay: Duration::ZERO,
            commit: Mutex::default(),
        }
    }

    pub fn default_storage_writer(&self, next_wal_index: u64) -> SharedStoreStorageWriter<O> {
        self.storage_writer(SharedStoreStorageMode::default(), next_wal_index)
    }

    pub async fn publish_index(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<(), SharedStoreReplicationError> {
        self.object_store
            .put(
                &self.index_key(shard_id),
                Bytes::from(engine.export_index_bytes(shard_id)?),
            )
            .await?;
        Ok(())
    }

    pub async fn publish_page_slabs(
        &self,
        shard_id: ShardId,
        block_store: &LocalBlockStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let mut published = Vec::new();
        for page_slab_id in block_store.slab_ids()? {
            self.object_store
                .put(
                    &self.page_slab_key(shard_id, page_slab_id),
                    Bytes::from(block_store.read_slab(page_slab_id)?),
                )
                .await?;
            published.push(page_slab_id);
        }
        Ok(published)
    }

    pub async fn publish_checkpoint(
        &self,
        shard_id: ShardId,
        checkpoint_wal_index: u64,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        // R2 single-writer fence: a checkpoint publish is a durable-frontier advance, so a
        // superseded stale owner must be rejected here just as on a WAL append.
        self.enforce_fence(shard_id).await?;
        // Durability barrier: fsync any bulk-deferred page bytes and persist the band manifest
        // BEFORE capturing the slab set, mirroring the local dump path
        // (bucket_dump_manifest_methods) which fsyncs pages+WAL before recording slab ids. Without
        // this a relaxed (bulk) writer could enumerate a slab whose tail bytes are not yet on disk,
        // uploading a torn page or racing a not-yet-durable slab into the checkpoint.
        // Make the index PORTABLE before exporting it. A page that lives in a WAL record or in
        // memory is named by a slab that is not a file, resolvable only through this process's
        // registry -- so an index carrying one names a place the restoring node cannot reach, and
        // that read returns nothing with no error. Materialising them first means the index names
        // only slabs this checkpoint actually uploads.
        let materialised = engine.materialize_synthetic_pages(shard_id);
        if materialised > 0 {
            tracing::info!(
                shard_id,
                pages = materialised,
                "materialised in-process-only pages so the checkpoint index can travel"
            );
        }
        block_store.sync_durable()?;
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let prefix = self.checkpoint_prefix(shard_id, &checkpoint_id);
        let index_key = format!("{prefix}index/shard.index.json");
        let index = engine.export_index_bytes(shard_id)?;
        self.object_store
            .put(&index_key, Bytes::from(index.clone()))
            .await?;

        // Snapshot the local band descriptors so each uploaded slab carries its sealed-band
        // metadata (logical bytes + page-id range) into the manifest for S3 restore-time install.
        let band_by_slab: BTreeMap<u64, _> = block_store
            .band_descriptors()
            .into_iter()
            .map(|band| (band.page_slab_id, band))
            .collect();
        let mut page_slabs = Vec::new();
        let mut uploaded_slab_ids = std::collections::BTreeSet::new();
        for page_slab_id in block_store.slab_ids()? {
            let bytes = block_store.read_slab(page_slab_id)?;
            let key = format!("{prefix}page_segments/page_segment_{page_slab_id:020}.seg");
            self.object_store
                .put(&key, Bytes::from(bytes.clone()))
                .await?;
            uploaded_slab_ids.insert(page_slab_id);
            let band = band_by_slab.get(&page_slab_id);
            page_slabs.push(SharedStorePageSlab {
                page_slab_id,
                key,
                byte_size: bytes.len() as u64,
                sha256: sha256_hex(&bytes),
                logical_bytes: band.map(|band| band.logical_bytes).unwrap_or(0),
                first_page_id: band.and_then(|band| band.first_page_id),
                last_page_id: band.and_then(|band| band.last_page_id),
                created_unix_ms: band.and_then(|band| band.created_unix_ms),
                updated_unix_ms: band.and_then(|band| band.updated_unix_ms),
            });
        }

        // Completeness barrier: every slab the served index actually references must be covered by
        // an uploaded slab before the manifest is written, so a restore never resolves a live page
        // to a slab that is absent from the checkpoint. The synthetic in-memory hot-page slab
        // (u64::MAX) is folded into the exported index, not a durable slab, so it is excluded.
        const HOT_PAGE_SLAB_ID: u64 = u64::MAX;
        for referenced in engine.live_page_slab_ids(shard_id) {
            if referenced == HOT_PAGE_SLAB_ID {
                continue;
            }
            if !uploaded_slab_ids.contains(&referenced) {
                return Err(SharedStoreReplicationError::CheckpointSlabNotDurable {
                    shard_id,
                    page_slab_id: referenced,
                });
            }
        }

        let manifest = SharedStoreCheckpointManifest {
            cluster_id: self.cluster_id.clone(),
            shard_id,
            checkpoint_id,
            checkpoint_wal_index,
            created_at_ms: now_ms(),
            index_key,
            index_byte_size: index.len() as u64,
            index_sha256: sha256_hex(&index),
            page_slabs,
            next_page_id: block_store.next_page_id(),
        };
        self.object_store
            .put(
                &self.checkpoint_manifest_key(shard_id, &manifest.checkpoint_id),
                Bytes::from(serde_json::to_vec_pretty(&manifest)?),
            )
            .await?;
        Ok(manifest)
    }

    pub async fn restore_index_and_pages(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<Vec<u64>, SharedStoreReplicationError> {
        let index = self.object_store.get(&self.index_key(shard_id)).await?;
        engine.install_index_bytes(shard_id, &index)?;

        let prefix = self.page_slab_prefix(shard_id);
        let mut restored = Vec::new();
        for key in self.object_store.list(&prefix).await? {
            let Some(page_slab_id) = parse_page_slab_id(&key) else {
                continue;
            };
            let bytes = self.object_store.get(&key).await?;
            block_store.install_slab(page_slab_id, &bytes)?;
            restored.push(page_slab_id);
        }
        restored.sort_unstable();
        Ok(restored)
    }

    pub async fn list_checkpoints(
        &self,
        shard_id: ShardId,
    ) -> Result<Vec<SharedStoreCheckpointManifest>, SharedStoreReplicationError> {
        let mut manifests = Vec::new();
        for key in self
            .object_store
            .list(&self.checkpoints_prefix(shard_id))
            .await?
        {
            if !key.ends_with("/manifest.json") {
                continue;
            }
            manifests.push(serde_json::from_slice(&self.object_store.get(&key).await?)?);
        }
        manifests.sort_by_key(|manifest: &SharedStoreCheckpointManifest| {
            (manifest.checkpoint_wal_index, manifest.created_at_ms)
        });
        Ok(manifests)
    }

    /// Publish this shard's bucket-dump manifests to the object store.
    ///
    /// A dump manifest is the durable reclaim watermark plus the recovery index for the data it
    /// covers. In shared mode that data outlives the node -- it is in the object store -- but
    /// the manifest describing it was only ever written to the node's local index dir, so
    /// losing the node lost the lineage for data that was still perfectly present. A manifest
    /// should be as durable as the data it describes; this is the half that was missing.
    ///
    /// Deliberately not on the write path. Manifests are produced at dump cadence, so this is
    /// one put per manifest at checkpoint time and nothing at all in the steady state -- unlike
    /// the WAL, which is the per-write barrier and stays node-local for exactly that reason.
    pub async fn publish_bucket_dump_manifests(
        &self,
        shard_id: ShardId,
        manifests: &[crate::BucketDumpManifest],
    ) -> Result<usize, SharedStoreReplicationError> {
        let mut published = 0usize;
        for manifest in manifests {
            if manifest.shard_id != shard_id {
                continue;
            }
            self.object_store
                .put(
                    &self.bucket_dump_manifest_key(shard_id, &manifest.manifest_id),
                    Bytes::from(serde_json::to_vec_pretty(manifest)?),
                )
                .await?;
            published += 1;
        }
        Ok(published)
    }

    /// Land every published bucket-dump manifest for this shard in the local index dir.
    ///
    /// The counterpart to [`publish_bucket_dump_manifests`](Self::publish_bucket_dump_manifests):
    /// a node taking over a shard inherits the dump lineage along with the data, instead of
    /// starting blind and treating every live generation as never dumped -- which is what
    /// blocks WAL reclaim until this node has dumped everything again itself.
    ///
    /// A manifest already present locally is left alone. A local manifest is at least as
    /// current as a published one, and rewriting it would churn the very file that authorizes
    /// WAL reclaim.
    pub async fn restore_bucket_dump_manifests(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<usize, SharedStoreReplicationError> {
        let existing: std::collections::BTreeSet<String> = engine
            .list_bucket_dump_manifests(shard_id)
            .into_iter()
            .map(|manifest| manifest.manifest_id)
            .collect();
        let mut restored = 0usize;
        for key in self
            .object_store
            .list(&self.bucket_dump_prefix(shard_id))
            .await?
        {
            if !key.ends_with(".json") {
                continue;
            }
            let manifest: crate::BucketDumpManifest =
                serde_json::from_slice(&self.object_store.get(&key).await?)?;
            // A manifest filed under another shard's prefix, or one this node already has,
            // is not ours to write.
            if manifest.shard_id != shard_id || existing.contains(&manifest.manifest_id) {
                continue;
            }
            engine.store_bucket_dump_manifest(&manifest)?;
            restored += 1;
        }
        Ok(restored)
    }

    pub async fn restore_checkpoint(
        &self,
        manifest: &SharedStoreCheckpointManifest,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<(), SharedStoreReplicationError> {
        let index = self.object_store.get(&manifest.index_key).await?;
        verify_checksum(
            &manifest.index_key,
            &index,
            manifest.index_byte_size,
            &manifest.index_sha256,
        )?;
        engine.install_index_bytes(manifest.shard_id, &index)?;

        for slab in &manifest.page_slabs {
            let bytes = self.object_store.get(&slab.key).await?;
            verify_checksum(&slab.key, &bytes, slab.byte_size, &slab.sha256)?;
            block_store.install_slab(slab.page_slab_id, &bytes)?;
        }
        Ok(())
    }

    pub async fn restore_latest_checkpoint(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        let manifest = self
            .list_checkpoints(shard_id)
            .await?
            .pop()
            .ok_or(SharedStoreReplicationError::CheckpointNotFound(shard_id))?;
        self.restore_checkpoint(&manifest, engine, block_store)
            .await?;
        Ok(manifest)
    }

    pub async fn replay_wal(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        if matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        ) {
            if let Some(report) = self
                .replay_wal_from_offset_metadata(shard_id, after_wal_index, engine, false)
                .await?
            {
                return Ok(report);
            }
        }
        let wal_entries = self.load_wal_entries(shard_id).await?;

        let mut report = ReplayReport {
            applied: 0,
            last_wal_index: after_wal_index,
            offset_index_reads: 0,
            range_bytes_read: 0,
        };
        for (wal_index, entry) in wal_entries {
            if wal_index <= after_wal_index {
                continue;
            }
            // An entry that says what its write DID is installed, not re-executed. Re-executing
            // reproduces the write only if everything that influenced it is reproduced too, and on
            // a SUCCESSOR that is a stronger assumption than it is on a restart: a different node,
            // a different clock, and whatever config it happens to hold.
            //
            // The fallback is what makes this safe to land. An entry carrying no outcomes replays
            // exactly as it used to, so a shared log written before this still applies.
            if !entry.outcomes.is_empty() {
                if !engine.install_shared_outcomes_with_blocks(shard_id, &entry.outcomes, &entry.staged_pages) {
                    return Err(SharedStoreReplicationError::ApplyFailed {
                        wal_index,
                        status: Status::error(
                            "shared_wal_outcome_refused",
                            "a recorded outcome could not be installed; refusing to serve a shard missing it",
                        ),
                    });
                }
                report.applied += 1;
                report.last_wal_index = wal_index;
                continue;
            }
            // Hand the replayed write the pages the ORIGINAL write produced. Re-executing the
            // command would otherwise substitute this node's reconstruction for the bytes that
            // were actually acked -- identical for a plain value, and not necessarily so for
            // derived state.
            let Some(command) = entry.command else {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index,
                    status: Status::error(
                        "shared_wal_entry_empty",
                        "entry carries neither results nor an operation; refusing rather than skipping a durable write",
                    ),
                });
            };
            let response = engine.execute_with_carried_pages(
                ExecuteRequest { shard_id, command },
                entry.staged_pages,
            );
            if !response.status.ok {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index,
                    status: response.status,
                });
            }
            report.applied += 1;
            report.last_wal_index = wal_index;
        }
        Ok(report)
    }

    pub async fn replay_wal_strict(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        if matches!(
            self.wal_append_mode,
            SharedStoreWalAppendMode::ProtobufAppendBlob
        ) {
            if let Some(report) = self
                .replay_wal_from_offset_metadata(shard_id, after_wal_index, engine, true)
                .await?
            {
                return Ok(report);
            }
        }
        let wal_entries = self.load_wal_entries(shard_id).await?;

        let mut expected = after_wal_index + 1;
        let mut report = ReplayReport {
            applied: 0,
            last_wal_index: after_wal_index,
            offset_index_reads: 0,
            range_bytes_read: 0,
        };
        for (wal_index, entry) in wal_entries {
            if wal_index <= after_wal_index {
                continue;
            }
            if wal_index != expected {
                return Err(SharedStoreReplicationError::ReplayGap {
                    expected,
                    actual: wal_index,
                });
            }
            // An entry that says what its write DID is installed, not re-executed. Re-executing
            // reproduces the write only if everything that influenced it is reproduced too, and on
            // a SUCCESSOR that is a stronger assumption than it is on a restart: a different node,
            // a different clock, and whatever config it happens to hold.
            //
            // The fallback is what makes this safe to land. An entry carrying no outcomes replays
            // exactly as it used to, so a shared log written before this still applies.
            if !entry.outcomes.is_empty() {
                if !engine.install_shared_outcomes_with_blocks(shard_id, &entry.outcomes, &entry.staged_pages) {
                    return Err(SharedStoreReplicationError::ApplyFailed {
                        wal_index,
                        status: Status::error(
                            "shared_wal_outcome_refused",
                            "a recorded outcome could not be installed; refusing to serve a shard missing it",
                        ),
                    });
                }
                report.applied += 1;
                report.last_wal_index = wal_index;
                continue;
            }
            // Hand the replayed write the pages the ORIGINAL write produced. Re-executing the
            // command would otherwise substitute this node's reconstruction for the bytes that
            // were actually acked -- identical for a plain value, and not necessarily so for
            // derived state.
            let Some(command) = entry.command else {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index,
                    status: Status::error(
                        "shared_wal_entry_empty",
                        "entry carries neither results nor an operation; refusing rather than skipping a durable write",
                    ),
                });
            };
            let response = engine.execute_with_carried_pages(
                ExecuteRequest { shard_id, command },
                entry.staged_pages,
            );
            if !response.status.ok {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index,
                    status: response.status,
                });
            }
            report.applied += 1;
            report.last_wal_index = wal_index;
            expected += 1;
        }
        Ok(report)
    }

    async fn replay_wal_from_offset_metadata(
        &self,
        shard_id: ShardId,
        after_wal_index: u64,
        engine: &TemporalEngine,
        strict: bool,
    ) -> Result<Option<ReplayReport>, SharedStoreReplicationError> {
        let offset_metadata = self.load_wal_offset_metadata(shard_id).await?;
        if offset_metadata.is_empty() {
            return Ok(None);
        }

        let mut expected = after_wal_index + 1;
        let mut report = ReplayReport {
            applied: 0,
            last_wal_index: after_wal_index,
            offset_index_reads: 0,
            range_bytes_read: 0,
        };
        for (wal_index, _) in offset_metadata.range((after_wal_index + 1)..) {
            if strict && *wal_index != expected {
                return Err(SharedStoreReplicationError::ReplayGap {
                    expected,
                    actual: *wal_index,
                });
            }
            let read = self
                .read_wal_entry_by_offset_metadata(shard_id, *wal_index)
                .await?
                .ok_or_else(|| {
                    SharedStoreReplicationError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "missing WAL offset metadata for shard {shard_id} index {wal_index}"
                        ),
                    ))
                })?;
            // Neither results nor an operation: refusing beats skipping a durable write.
            let Some(command) = read.entry.command else {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index: *wal_index,
                    status: Status::error(
                        "shared_wal_entry_empty",
                        "entry carries neither results nor an operation",
                    ),
                });
            };
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command,
            });
            if !response.status.ok {
                return Err(SharedStoreReplicationError::ApplyFailed {
                    wal_index: *wal_index,
                    status: response.status,
                });
            }
            report.applied += 1;
            report.last_wal_index = *wal_index;
            report.offset_index_reads += 1;
            report.range_bytes_read += read.range_bytes_read;
            expected += 1;
        }
        Ok(Some(report))
    }

    pub async fn load_replay_cursor(
        &self,
        shard_id: ShardId,
    ) -> Result<SharedStoreReplayCursor, SharedStoreReplicationError> {
        match self
            .object_store
            .get(&self.replay_cursor_key(shard_id))
            .await
        {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(ObjectStoreError::NotFound(_)) => Ok(SharedStoreReplayCursor {
                shard_id,
                last_wal_index: 0,
                last_replay_time_ms: 0,
            }),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn save_replay_cursor(
        &self,
        cursor: &SharedStoreReplayCursor,
    ) -> Result<(), SharedStoreReplicationError> {
        self.object_store
            .put(
                &self.replay_cursor_key(cursor.shard_id),
                Bytes::from(serde_json::to_vec_pretty(cursor)?),
            )
            .await?;
        Ok(())
    }

    pub async fn replay_wal_strict_with_cursor(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
    ) -> Result<ReplayReport, SharedStoreReplicationError> {
        let mut cursor = self.load_replay_cursor(shard_id).await?;
        let report = self
            .replay_wal_strict(shard_id, cursor.last_wal_index, engine)
            .await?;
        if report.last_wal_index > cursor.last_wal_index {
            cursor.last_wal_index = report.last_wal_index;
            cursor.last_replay_time_ms = now_ms();
            self.save_replay_cursor(&cursor).await?;
        }
        Ok(report)
    }

    pub async fn gc_wal_before(
        &self,
        shard_id: ShardId,
        retain_from_wal_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let mut deleted_wal_objects = 0usize;
        for key in self.object_store.list(&self.wal_prefix(shard_id)).await? {
            let Some(wal_index) = parse_wal_index(&key) else {
                continue;
            };
            if wal_index < retain_from_wal_index {
                self.object_store.delete(&key).await?;
                deleted_wal_objects += 1;
            }
        }
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_wal_objects,
            ..SharedStoreGcReport::default()
        })
    }

    pub async fn gc_wal_before_cursor_safe(
        &self,
        shard_id: ShardId,
        retain_from_wal_index: u64,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let cursor = self.load_replay_cursor(shard_id).await?;
        if cursor.last_wal_index > 0
            && retain_from_wal_index > cursor.last_wal_index.saturating_add(1)
        {
            return Err(SharedStoreReplicationError::GcBlockedByReplayCursor {
                cursor_wal_index: cursor.last_wal_index,
                retain_from_wal_index: retain_from_wal_index,
            });
        }
        let mut report = self.gc_wal_before(shard_id, retain_from_wal_index).await?;
        if cursor.last_wal_index > 0 {
            report.retained_for_cursor_wal_index = Some(cursor.last_wal_index);
        }
        Ok(report)
    }

    pub async fn gc_checkpoints(
        &self,
        shard_id: ShardId,
        keep_last: usize,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let keep_last = keep_last.max(1);
        let manifests = self.list_checkpoints(shard_id).await?;
        let delete_count = manifests.len().saturating_sub(keep_last);
        let retained_checkpoint_ids = manifests[delete_count..]
            .iter()
            .map(|manifest| manifest.checkpoint_id.clone())
            .collect::<Vec<_>>();
        let mut deleted_checkpoint_objects = 0usize;
        for manifest in manifests.iter().take(delete_count) {
            deleted_checkpoint_objects += self
                .delete_prefix(&self.checkpoint_prefix(shard_id, &manifest.checkpoint_id))
                .await?;
        }
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_checkpoints: delete_count,
            deleted_checkpoint_objects,
            retained_checkpoint_ids,
            ..SharedStoreGcReport::default()
        })
    }

    pub async fn gc_checkpoints_cursor_safe(
        &self,
        shard_id: ShardId,
        keep_last: usize,
    ) -> Result<SharedStoreGcReport, SharedStoreReplicationError> {
        let keep_last = keep_last.max(1);
        let manifests = self.list_checkpoints(shard_id).await?;
        let cursor = self.load_replay_cursor(shard_id).await?;
        let retain_start = manifests.len().saturating_sub(keep_last);
        let cursor_anchor = if cursor.last_wal_index > 0 {
            manifests
                .iter()
                .enumerate()
                .rev()
                .find(|(_, manifest)| manifest.checkpoint_wal_index <= cursor.last_wal_index)
                .map(|(index, _)| index)
        } else {
            None
        };
        let mut retained_checkpoint_ids = manifests[retain_start..]
            .iter()
            .map(|manifest| manifest.checkpoint_id.clone())
            .collect::<Vec<_>>();
        let mut deleted_checkpoints = 0usize;
        let mut deleted_checkpoint_objects = 0usize;
        for (index, manifest) in manifests.iter().enumerate() {
            let retained_by_keep_last = index >= retain_start;
            let retained_by_cursor = cursor_anchor == Some(index);
            if retained_by_keep_last || retained_by_cursor {
                if retained_by_cursor
                    && !retained_checkpoint_ids
                        .iter()
                        .any(|id| id == &manifest.checkpoint_id)
                {
                    retained_checkpoint_ids.push(manifest.checkpoint_id.clone());
                }
                continue;
            }
            if cursor.last_wal_index > 0
                && manifest.checkpoint_wal_index <= cursor.last_wal_index
                && cursor_anchor.is_none()
            {
                return Err(
                    SharedStoreReplicationError::CheckpointGcBlockedByReplayCursor {
                        cursor_wal_index: cursor.last_wal_index,
                        checkpoint_wal_index: manifest.checkpoint_wal_index,
                        checkpoint_id: manifest.checkpoint_id.clone(),
                    },
                );
            }
            deleted_checkpoint_objects += self
                .delete_prefix(&self.checkpoint_prefix(shard_id, &manifest.checkpoint_id))
                .await?;
            deleted_checkpoints += 1;
        }
        retained_checkpoint_ids.sort();
        retained_checkpoint_ids.dedup();
        Ok(SharedStoreGcReport {
            shard_id,
            deleted_checkpoints,
            deleted_checkpoint_objects,
            retained_checkpoint_ids,
            retained_for_cursor_wal_index: (cursor.last_wal_index > 0)
                .then_some(cursor.last_wal_index),
            retained_for_cursor_checkpoint_id: cursor_anchor
                .map(|index| manifests[index].checkpoint_id.clone()),
            ..SharedStoreGcReport::default()
        })
    }

    fn shard_prefix(&self, shard_id: ShardId) -> String {
        format!("{}/shards/{}/shared/", self.cluster_id, shard_id)
    }

    fn index_key(&self, shard_id: ShardId) -> String {
        format!("{}index/shard.index.json", self.shard_prefix(shard_id))
    }

    fn page_slab_prefix(&self, shard_id: ShardId) -> String {
        format!("{}page_segments/", self.shard_prefix(shard_id))
    }

    fn page_slab_key(&self, shard_id: ShardId, page_slab_id: u64) -> String {
        format!(
            "{}page_segment_{page_slab_id:020}.seg",
            self.page_slab_prefix(shard_id)
        )
    }

    fn wal_prefix(&self, shard_id: ShardId) -> String {
        format!("{}wal/", self.shard_prefix(shard_id))
    }

    fn wal_key(&self, shard_id: ShardId, wal_index: u64) -> String {
        format!(
            "{}wal_{wal_index:020}.json",
            self.wal_prefix(shard_id)
        )
    }

    fn wal_blob_key(&self, shard_id: ShardId) -> String {
        format!("{}wal.protobuf.blob", self.wal_prefix(shard_id))
    }

    fn wal_offset_index_blob_key(&self, shard_id: ShardId) -> String {
        format!(
            "{}wal.offset_index.protobuf.blob",
            self.wal_prefix(shard_id)
        )
    }

    pub async fn load_wal_offset_metadata(
        &self,
        shard_id: ShardId,
    ) -> Result<BTreeMap<u64, SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
        match self
            .object_store
            .get(&self.wal_offset_index_blob_key(shard_id))
            .await
        {
            Ok(bytes) => decode_wal_offset_metadata_blob(&bytes),
            Err(ObjectStoreError::NotFound(_)) => Ok(BTreeMap::new()),
            Err(err) => Err(err.into()),
        }
    }

    fn replay_cursor_key(&self, shard_id: ShardId) -> String {
        format!("{}replay_cursor.json", self.shard_prefix(shard_id))
    }

    fn checkpoints_prefix(&self, shard_id: ShardId) -> String {
        format!("{}checkpoints/", self.shard_prefix(shard_id))
    }

    fn bucket_dump_prefix(&self, shard_id: ShardId) -> String {
        format!("{}slot_dumps/", self.shard_prefix(shard_id))
    }

    fn bucket_dump_manifest_key(&self, shard_id: ShardId, manifest_id: &str) -> String {
        format!("{}{manifest_id}.json", self.bucket_dump_prefix(shard_id))
    }

    fn checkpoint_prefix(&self, shard_id: ShardId, checkpoint_id: &str) -> String {
        format!("{}{checkpoint_id}/", self.checkpoints_prefix(shard_id))
    }

    fn checkpoint_manifest_key(&self, shard_id: ShardId, checkpoint_id: &str) -> String {
        format!(
            "{}manifest.json",
            self.checkpoint_prefix(shard_id, checkpoint_id)
        )
    }

    async fn read_wal_entry(
        &self,
        key: &str,
    ) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
        let bytes = self.object_store.get(key).await?;
        if let Ok(object) = serde_json::from_slice::<SharedStoreWalObject>(&bytes) {
            let entry_bytes = serde_json::to_vec(&object.entry)?;
            verify_checksum(
                key,
                &entry_bytes,
                object.entry_byte_size,
                &object.entry_sha256,
            )?;
            return Ok(object.entry);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn parse_wal_entry_object(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
        if let Ok(object) = serde_json::from_slice::<SharedStoreWalObject>(bytes) {
            let entry_bytes = serde_json::to_vec(&object.entry)?;
            verify_checksum(
                key,
                &entry_bytes,
                object.entry_byte_size,
                &object.entry_sha256,
            )?;
            return Ok(object.entry);
        }
        Ok(serde_json::from_slice(bytes)?)
    }

    /// The highest WAL index currently persisted in shared storage for
    /// `shard_id`, or `0` when no WAL exists yet. A restarting node uses this to
    /// resume publishing WAL entries at `latest + 1` without clobbering
    /// already-persisted entries. Works for both WAL append encodings (the
    /// protobuf offset index and the per-key JSON objects).
    pub async fn latest_persisted_wal_index(
        &self,
        shard_id: ShardId,
    ) -> Result<u64, SharedStoreReplicationError> {
        let offset_metadata = self.load_wal_offset_metadata(shard_id).await?;
        if let Some(max) = offset_metadata.keys().max().copied() {
            return Ok(max);
        }
        let entries = self.load_wal_entries(shard_id).await?;
        Ok(entries.keys().max().copied().unwrap_or(0))
    }

    async fn load_wal_entries(
        &self,
        shard_id: ShardId,
    ) -> Result<BTreeMap<u64, SharedStoreWalEntry>, SharedStoreReplicationError> {
        let mut entries = BTreeMap::new();
        let mut keys = self.object_store.list(&self.wal_prefix(shard_id)).await?;
        keys.sort();
        let indexed_keys = keys
            .into_iter()
            .filter_map(|key| parse_wal_index(&key).map(|wal_index| (wal_index, key)))
            .collect::<Vec<_>>();
        if !indexed_keys.is_empty() {
            let object_keys = indexed_keys
                .iter()
                .map(|(_, key)| key.clone())
                .collect::<Vec<_>>();
            let objects = self.object_store.get_many(&object_keys).await?;
            if objects.len() == indexed_keys.len()
                && objects
                    .iter()
                    .zip(&indexed_keys)
                    .all(|((actual, _), (_, expected))| actual == expected)
            {
                for ((_, bytes), (wal_index, key)) in objects.iter().zip(&indexed_keys) {
                    entries.insert(*wal_index, self.parse_wal_entry_object(key, bytes)?);
                }
            } else {
                for (wal_index, key) in &indexed_keys {
                    let entry = self.read_wal_entry(key).await?;
                    entries.insert(*wal_index, entry);
                }
            }
        }

        match self.object_store.get(&self.wal_blob_key(shard_id)).await {
            Ok(bytes) => {
                for entry in decode_wal_proto_blob(&bytes)? {
                    entries.insert(entry.wal_index, entry);
                }
            }
            Err(ObjectStoreError::NotFound(_)) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(entries)
    }

    async fn put_with_retry(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<(), SharedStoreReplicationError> {
        let attempts = self.retry_policy.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.object_store.put(key, bytes.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    last_error = Some(err);
                    if attempt + 1 < attempts && self.retry_policy.backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(self.retry_policy.backoff_ms))
                            .await;
                    }
                }
            }
        }
        Err(last_error
            .expect("retry loop must record failed object-store error")
            .into())
    }

    async fn append_blob_with_retry(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<Option<AppendBlobReceipt>, SharedStoreReplicationError> {
        let attempts = self.retry_policy.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.object_store.append_blob(key, bytes.clone()).await {
                Ok(receipt) => return Ok(Some(receipt)),
                Err(err) => {
                    last_error = Some(err);
                    if attempt + 1 < attempts && self.retry_policy.backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(self.retry_policy.backoff_ms))
                            .await;
                    }
                }
            }
        }
        Err(last_error
            .expect("retry loop must record failed object-store error")
            .into())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<usize, SharedStoreReplicationError> {
        let keys = self.object_store.list(prefix).await?;
        let deleted = keys.len();
        for key in keys {
            self.object_store.delete(&key).await?;
        }
        Ok(deleted)
    }
}

/// One slab's location + integrity in a shared-storage checkpoint: the object key to
/// read it from and the checksum to verify it against before caching it locally.
#[derive(Debug, Clone)]
struct SharedSlabAddress {
    key: String,
    byte_size: u64,
    sha256: String,
}

/// Lazy read-through source backing conformance recovery on the shared-filesystem
/// (`FileObjectStore`) backend. Holds the checkpoint's slab address map (slab id ->
/// shared object key) with no slab bytes, and resolves each slab on demand to a
/// synchronous filesystem read of the shared store, verifying the checkpoint
/// checksum before returning the bytes. The block store installs (caches) the
/// returned bytes locally, so each shared slab is fetched at most once and only when
/// a read actually needs it — never eagerly at recovery time.
#[derive(Debug)]
pub struct SharedPathSlabSource {
    object_store: Arc<FileObjectStore>,
    slabs: BTreeMap<u64, SharedSlabAddress>,
}

impl SharedPathSlabSource {
    fn new(object_store: Arc<FileObjectStore>, slabs: BTreeMap<u64, SharedSlabAddress>) -> Self {
        Self {
            object_store,
            slabs,
        }
    }

    /// Number of slabs this source can serve lazily (i.e. the checkpoint's slab
    /// count). Useful for asserting that recovery installed an address map, not bytes.
    pub fn slab_count(&self) -> usize {
        self.slabs.len()
    }
}

impl SharedSlabSource for SharedPathSlabSource {
    fn fetch_slab(&self, page_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let Some(address) = self.slabs.get(&page_slab_id) else {
            return Ok(None);
        };
        let path = self.object_store.object_path(&address.key).map_err(|err| {
            BlockStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                err.to_string(),
            ))
        })?;
        let bytes = std::fs::read(&path)?;
        // Conformance with restore_checkpoint: reject a corrupt shared slab before caching it.
        let actual = sha256_hex(&bytes);
        if bytes.len() as u64 != address.byte_size || actual != address.sha256 {
            return Err(BlockStoreError::ChecksumMismatch {
                page_slab_id,
                offset: 0,
                length: address.byte_size,
                expected: address.sha256.clone(),
                actual,
            });
        }
        Ok(Some(bytes))
    }
}

/// Lazy read-through source backing conformance recovery on the *networked*
/// matrixobject (`MatrixObjectHttpStore`) backend. Same contract as
/// [`SharedPathSlabSource`], but resolves each slab to a synchronous networked
/// GET ([`MatrixObjectHttpStore::get_blocking`]) of the shared object instead of a
/// local filesystem read, verifying the checkpoint checksum before the block store
/// caches it. Old (pre-checkpoint) pages therefore *follow the shard across
/// nodes*: fetched on demand, over the network, at most once each, only when a
/// read actually needs them — never eagerly at recovery time.
#[derive(Debug)]
pub struct MatrixObjectSlabSource {
    object_store: Arc<MatrixObjectHttpStore>,
    slabs: BTreeMap<u64, SharedSlabAddress>,
}

impl MatrixObjectSlabSource {
    fn new(object_store: Arc<MatrixObjectHttpStore>, slabs: BTreeMap<u64, SharedSlabAddress>) -> Self {
        Self {
            object_store,
            slabs,
        }
    }

    /// Number of slabs this source can serve lazily (the checkpoint's slab count).
    /// Useful for asserting that recovery installed an address map, not bytes.
    pub fn slab_count(&self) -> usize {
        self.slabs.len()
    }
}

impl SharedSlabSource for MatrixObjectSlabSource {
    fn fetch_slab(&self, page_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let Some(address) = self.slabs.get(&page_slab_id) else {
            return Ok(None);
        };
        let bytes = self
            .object_store
            .get_blocking(&address.key)
            .map_err(|err| {
                BlockStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    err.to_string(),
                ))
            })?
            .to_vec();
        // Conformance with restore_checkpoint: reject a corrupt shared slab before caching it.
        let actual = sha256_hex(&bytes);
        if bytes.len() as u64 != address.byte_size || actual != address.sha256 {
            return Err(BlockStoreError::ChecksumMismatch {
                page_slab_id,
                offset: 0,
                length: address.byte_size,
                expected: address.sha256.clone(),
                actual,
            });
        }
        Ok(Some(bytes))
    }
}

impl<O> SharedStoreReplicator<O>
where
    O: ObjectStore + 'static,
{
    /// Shared body of the conformance LAZY restore, parameterized over the
    /// slab-source constructor so every shared-storage backend reuses the identical
    /// index-install + address-map + lazy-range-reserve logic; only the per-slab
    /// fetch transport differs (a local filesystem read vs a networked GET). Install
    /// the served INDEX and a per-slab shared address map WITHOUT downloading any
    /// slab bytes; `make_source` builds the backend-specific lazy read-through the
    /// block store consults on the first read that misses a slab. The caller replays
    /// only the WAL tail after `manifest.checkpoint_wal_index`, so recovery cost is
    /// O(index + recent WAL), not O(full history + all slabs). Returns
    /// `CheckpointNotFound` when no checkpoint exists so the caller can fall back to
    /// a full WAL replay.
    async fn restore_index_and_page_addresses_with<F>(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
        make_source: F,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError>
    where
        F: FnOnce(Arc<O>, BTreeMap<u64, SharedSlabAddress>) -> Arc<dyn SharedSlabSource>,
    {
        let manifest = self
            .list_checkpoints(shard_id)
            .await?
            .pop()
            .ok_or(SharedStoreReplicationError::CheckpointNotFound(shard_id))?;
        let index = self.object_store.get(&manifest.index_key).await?;
        verify_checksum(
            &manifest.index_key,
            &index,
            manifest.index_byte_size,
            &manifest.index_sha256,
        )?;
        engine.install_index_bytes(manifest.shard_id, &index)?;

        let mut slabs = BTreeMap::new();
        let mut max_slab_id = 0u64;
        for slab in &manifest.page_slabs {
            max_slab_id = max_slab_id.max(slab.page_slab_id);
            slabs.insert(
                slab.page_slab_id,
                SharedSlabAddress {
                    key: slab.key.clone(),
                    byte_size: slab.byte_size,
                    sha256: slab.sha256.clone(),
                },
            );
        }
        let source = make_source(Arc::clone(&self.object_store), slabs);
        block_store.attach_shared_slab_source(source);
        // Roll local appends past the checkpoint's slab/page-id range so replayed WAL-tail
        // and new writes never overwrite a slab still served lazily from shared storage.
        if !manifest.page_slabs.is_empty() {
            block_store.reserve_lazy_checkpoint_range(max_slab_id, manifest.next_page_id)?;
            // S3: install SEALED band descriptors for the lazily-backed checkpoint slabs so
            // GC/compaction accounting is complete immediately after restore, before the first
            // on-demand fetch materializes any slab locally. Runs AFTER the reserve so the freshly
            // reserved slab stays the active band and every checkpoint slab is sealed.
            let lazy_bands: Vec<LazyCheckpointBand> = manifest
                .page_slabs
                .iter()
                .map(|slab| LazyCheckpointBand {
                    page_slab_id: slab.page_slab_id,
                    physical_bytes: slab.byte_size,
                    logical_bytes: slab.logical_bytes,
                    first_page_id: slab.first_page_id,
                    last_page_id: slab.last_page_id,
                    created_unix_ms: slab.created_unix_ms,
                    updated_unix_ms: slab.updated_unix_ms,
                })
                .collect();
            block_store.install_lazy_checkpoint_bands(&lazy_bands)?;
        }
        Ok(manifest)
    }
}

impl SharedStoreReplicator<FileObjectStore> {
    /// Conformance LAZY restore for the shared-filesystem backend: install the served
    /// INDEX and a per-slab shared address map WITHOUT downloading any slab bytes.
    /// Old (pre-checkpoint) pages are then read lazily through [`SharedPathSlabSource`]
    /// on the first read that needs them. See
    /// [`restore_index_and_page_addresses_with`](Self::restore_index_and_page_addresses_with)
    /// for the shared logic.
    pub async fn restore_index_and_page_addresses(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        self.restore_index_and_page_addresses_with(shard_id, engine, block_store, |store, slabs| {
            Arc::new(SharedPathSlabSource::new(store, slabs)) as Arc<dyn SharedSlabSource>
        })
        .await
    }
}

impl SharedStoreReplicator<MatrixObjectHttpStore> {
    /// Conformance LAZY restore for the *networked* matrixobject backend: install
    /// the served INDEX and a per-slab shared address map WITHOUT downloading any slab
    /// bytes. Old (pre-checkpoint) pages are then fetched lazily over the network
    /// through [`MatrixObjectSlabSource`] on the first read that needs them, so shard
    /// data follows the shard across nodes without an eager full download. See
    /// [`restore_index_and_page_addresses_with`](Self::restore_index_and_page_addresses_with)
    /// for the shared logic.
    pub async fn restore_index_and_page_addresses(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        self.restore_index_and_page_addresses_with(shard_id, engine, block_store, |store, slabs| {
            Arc::new(MatrixObjectSlabSource::new(store, slabs)) as Arc<dyn SharedSlabSource>
        })
        .await
    }
}

/// Lazy read-through source for the NODE-LOCAL matrixobject backend
/// ([`crate::matrixobject_store::MatrixObjectObjectStore`]): the same contract as
/// [`MatrixObjectSlabSource`], resolved against the in-process store under its mutex
/// instead of a networked GET, verifying the checkpoint checksum before the block
/// store caches it.
#[cfg(feature = "matrixobject")]
#[derive(Debug)]
pub struct MatrixObjectLocalSlabSource {
    object_store: Arc<crate::matrixobject_store::MatrixObjectObjectStore>,
    slabs: BTreeMap<u64, SharedSlabAddress>,
}

#[cfg(feature = "matrixobject")]
impl SharedSlabSource for MatrixObjectLocalSlabSource {
    fn fetch_slab(&self, page_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let Some(address) = self.slabs.get(&page_slab_id) else {
            return Ok(None);
        };
        let bytes = self
            .object_store
            .get_blocking(&address.key)
            .map_err(|err| {
                BlockStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    err.to_string(),
                ))
            })?
            .to_vec();
        // Conformance with restore_checkpoint: reject a corrupt shared slab before caching it.
        let actual = sha256_hex(&bytes);
        if bytes.len() as u64 != address.byte_size || actual != address.sha256 {
            return Err(BlockStoreError::ChecksumMismatch {
                page_slab_id,
                offset: 0,
                length: address.byte_size,
                expected: address.sha256.clone(),
                actual,
            });
        }
        Ok(Some(bytes))
    }
}

#[cfg(feature = "matrixobject")]
impl SharedStoreReplicator<crate::matrixobject_store::MatrixObjectObjectStore> {
    /// Conformance LAZY restore for the node-local matrixobject backend: install the
    /// served INDEX and a per-slab shared address map WITHOUT installing any slab
    /// bytes; old (pre-checkpoint) pages are read lazily out of the local store on the
    /// first read that needs them. The same flow the shared-filesystem and networked
    /// backends already run -- it is what lets this backend's recovery replay only the
    /// WAL tail after a checkpoint instead of all history.
    pub async fn restore_index_and_page_addresses(
        &self,
        shard_id: ShardId,
        engine: &TemporalEngine,
        block_store: &LocalBlockStore,
    ) -> Result<SharedStoreCheckpointManifest, SharedStoreReplicationError> {
        self.restore_index_and_page_addresses_with(shard_id, engine, block_store, |store, slabs| {
            Arc::new(MatrixObjectLocalSlabSource {
                object_store: store,
                slabs,
            }) as Arc<dyn SharedSlabSource>
        })
        .await
    }
}

/// Build a per-entry [`SharedStoreWriteReport`] from an optional append receipt (JsonPerKey mode
/// has no receipt; append-blob mode carries the byte range).
fn write_report_from_receipt(
    wal_index: u64,
    receipt: Option<&AppendBlobReceipt>,
) -> SharedStoreWriteReport {
    SharedStoreWriteReport {
        wal_index,
        published: true,
        queued: false,
        wal_blob_start_offset: receipt.map(|receipt| receipt.start_offset),
        wal_blob_end_offset: receipt.map(|receipt| receipt.end_offset),
        wal_blob_bytes_written: receipt.map(|receipt| receipt.bytes_written),
        wal_blob_object_length: receipt.map(|receipt| receipt.object_length),
    }
}

impl<O> SharedStoreStorageWriter<O>
where
    O: ObjectStore + 'static,
{
    /// Enable timer-less queue-coalesced group commit on the SYNC path. `commit_delay` (default
    /// `ZERO`) optionally widens each batch under extreme load; `ZERO` keeps it purely timer-less.
    /// Override the async queue depth at which writes stop deferring. `0` is unbounded.
    pub fn with_max_pending(mut self, max_pending: usize) -> Self {
        self.max_pending = max_pending;
        self
    }

    /// How many writes have published themselves because the queue was at capacity.
    ///
    /// Zero is the healthy state. Non-zero means the async path is no longer absorbing the
    /// write rate, and the acks that hit it paid the store's latency rather than silently
    /// growing the amount a crash would lose.
    pub fn queue_capacity_hits(&self) -> u64 {
        self.capacity_hits.load(Ordering::Relaxed)
    }

    pub fn with_group_commit(mut self, enabled: bool, commit_delay: Duration) -> Self {
        self.group_commit = enabled;
        self.commit_delay = commit_delay;
        self
    }

    pub async fn write(
        &self,
        shard_id: ShardId,
        command: Command,
    ) -> Result<SharedStoreWriteReport, SharedStoreReplicationError> {
        let wal_index = self.next_wal_index.fetch_add(1, Ordering::Relaxed);
        let entry = SharedStoreWalEntry {
            shard_id,
            wal_index,
            command: Some(command),
        
                        staged_pages: Vec::new(),
                                outcomes: Vec::new(),
        };
        match self.mode {
            SharedStoreStorageMode::Sync if self.group_commit => {
                self.group_commit_write(entry).await
            }
            SharedStoreStorageMode::Sync => {
                let receipt = self.replicator.publish_wal_entry(entry).await?;
                Ok(write_report_from_receipt(wal_index, receipt.as_ref()))
            }
            SharedStoreStorageMode::Async => {
                // Read the depth and release the lock before any await: holding it across the
                // publish below would serialize every writer behind one object-store round trip.
                let at_capacity = {
                    let pending = self
                        .pending
                        .lock()
                        .expect("shared-store async queue lock poisoned");
                    self.max_pending > 0 && pending.len() >= self.max_pending
                };
                if at_capacity {
                    // The queue is the loss window, so it is not allowed to grow without end.
                    // This write pays for its own durability instead of adding to what a
                    // non-graceful exit would drop. The report already tells the truth about
                    // which happened -- `published`, not `queued`.
                    self.capacity_hits.fetch_add(1, Ordering::Relaxed);
                    let receipt = self.replicator.publish_wal_entry(entry).await?;
                    return Ok(write_report_from_receipt(wal_index, receipt.as_ref()));
                }
                self.pending
                    .lock()
                    .expect("shared-store async queue lock poisoned")
                    .push_back(entry);
                Ok(SharedStoreWriteReport {
                    wal_index,
                    published: false,
                    queued: true,
                    wal_blob_start_offset: None,
                    wal_blob_end_offset: None,
                    wal_blob_bytes_written: None,
                    wal_blob_object_length: None,
                })
            }
        }
    }

    /// Timer-less queue-coalesced group commit (this design sync-closure group-commit model). The
    /// writer stages its entry and either becomes the flush LEADER (first arrival) or a FOLLOWER
    /// that awaits the leader's covering durable barrier. The leader issues ONE coalesced append
    /// per round covering every entry staged before that round began (the batch window is bounded
    /// naturally by the append's own in-flight duration), then wakes each covered waiter. A lone
    /// writer drains a one-entry batch immediately, adding no latency.
    ///
    /// Durability invariant: a waiter is told `Ok` STRICTLY AFTER the append covering its entry
    /// returned `Ok`; on failure every covered waiter (leader included) gets `Err`. A leader that
    /// vanishes without delivering (its `waker` dropped) surfaces as `Err` to the follower — never
    /// a false ack. Exactly amortizes N durable barriers onto ~1 per group.
    async fn group_commit_write(
        &self,
        entry: SharedStoreWalEntry,
    ) -> Result<SharedStoreWriteReport, SharedStoreReplicationError> {
        let wal_index = entry.wal_index;
        let (waker, waiter) = oneshot::channel();
        let is_leader = {
            let mut buffer = self.commit.lock().expect("group-commit buffer lock poisoned");
            buffer.queue.push_back(GroupCommitWaiter { entry, waker });
            if buffer.flushing {
                false
            } else {
                buffer.flushing = true;
                true
            }
        };

        if !is_leader {
            // Follower: block until the leader publishes our entry. A canceled channel means the
            // leader disappeared without delivering — report an error, never a false ack.
            return match waiter.await {
                Ok(outcome) => outcome.into_result(),
                Err(_canceled) => Err(SharedStoreReplicationError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "group-commit leader dropped before publishing WAL index {wal_index}"
                    ),
                ))),
            };
        }

        // Leader. Optionally widen the batch window under extreme load (default ZERO = timer-less).
        if !self.commit_delay.is_zero() {
            tokio::time::sleep(self.commit_delay).await;
        }

        let mut my_outcome: Option<GroupCommitOutcome> = None;
        loop {
            // Drain everything staged so far into this flush round. Entries that arrive DURING the
            // append below are the next round's natural group.
            let batch: Vec<GroupCommitWaiter> = {
                let mut buffer = self.commit.lock().expect("group-commit buffer lock poisoned");
                if buffer.queue.is_empty() {
                    // Nothing left to flush: hand leadership back. A writer that enqueues after this
                    // (under the same lock) will see `flushing == false` and become the next leader.
                    buffer.flushing = false;
                    break;
                }
                buffer.queue.drain(..).collect()
            };

            let entries: Vec<SharedStoreWalEntry> =
                batch.iter().map(|waiter| waiter.entry.clone()).collect();
            let result = self.replicator.publish_wal_entries_batch(&entries).await;
            match result {
                Ok(reports) => {
                    for (waiter, report) in batch.into_iter().zip(reports.into_iter()) {
                        let outcome = GroupCommitOutcome::Committed(report);
                        if waiter.entry.wal_index == wal_index {
                            my_outcome = Some(outcome);
                        } else {
                            // A dropped receiver just means that follower's future was canceled;
                            // its entry is already durable, so nothing to undo.
                            let _ = waiter.waker.send(outcome);
                        }
                    }
                }
                Err(err) => {
                    // The covering append failed: every entry in this round is NOT durable. Fan the
                    // error out to all of them (never a false ack). The stringified error is shared;
                    // the leader keeps a typed error for its own return.
                    let message = err.to_string();
                    for waiter in batch.into_iter() {
                        let outcome = GroupCommitOutcome::Failed(message.clone());
                        if waiter.entry.wal_index == wal_index {
                            my_outcome = Some(outcome);
                        } else {
                            let _ = waiter.waker.send(outcome);
                        }
                    }
                }
            }
        }

        // Our own entry was staged before the first round drained, so it was covered by round one.
        my_outcome
            .expect("group-commit leader must observe its own entry in a flush round")
            .into_result()
    }

    pub fn queued_len(&self) -> usize {
        self.pending
            .lock()
            .expect("shared-store async queue lock poisoned")
            .len()
    }

    pub async fn flush_pending(
        &self,
        max_entries: usize,
    ) -> Result<SharedStoreFlushReport, SharedStoreReplicationError> {
        let limit = max_entries.max(1);
        let mut drained = Vec::new();
        {
            let mut pending = self
                .pending
                .lock()
                .expect("shared-store async queue lock poisoned");
            for _ in 0..limit {
                let Some(entry) = pending.pop_front() else {
                    break;
                };
                drained.push(entry);
            }
        }

        let mut last_wal_index = 0;
        let mut last_receipt = None;
        for (index, entry) in drained.iter().cloned().enumerate() {
            last_wal_index = entry.wal_index;
            match self.replicator.publish_wal_entry(entry).await {
                Ok(receipt) => last_receipt = receipt,
                Err(err) => {
                    let mut pending = self
                        .pending
                        .lock()
                        .expect("shared-store async queue lock poisoned");
                    for entry in drained[index..].iter().rev().cloned() {
                        pending.push_front(entry);
                    }
                    return Err(err);
                }
            }
        }
        let remaining = self.queued_len();
        Ok(SharedStoreFlushReport {
            flushed: drained.len(),
            remaining,
            last_wal_index,
            last_wal_blob_start_offset: last_receipt.as_ref().map(|receipt| receipt.start_offset),
            last_wal_blob_end_offset: last_receipt.as_ref().map(|receipt| receipt.end_offset),
            last_wal_blob_object_length: last_receipt.as_ref().map(|receipt| receipt.object_length),
        })
    }
}

fn parse_page_slab_id(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .strip_prefix("page_segment_")?
        .strip_suffix(".seg")?
        .parse()
        .ok()
}

fn parse_wal_index(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .strip_prefix("wal_")?
        .strip_suffix(".json")?
        .parse()
        .ok()
}

fn verify_checksum(
    key: &str,
    bytes: &[u8],
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(), SharedStoreReplicationError> {
    let actual = sha256_hex(bytes);
    if bytes.len() as u64 != expected_size || actual != expected_sha256 {
        return Err(SharedStoreReplicationError::ChecksumMismatch {
            path: key.to_string(),
            expected: expected_sha256.to_string(),
            actual,
        });
    }
    Ok(())
}

const WAL_COMMAND_ENCODING_JSON_SERDE: u32 = 0;
const WAL_COMMAND_ENCODING_SDK_PROTO: u32 = 1;

struct WalCommandMetadata {
    byte_size: u64,
    sha256: String,
    encoding: u32,
}

/// Describe the operation an entry carries, for the offset sidecar.
///
/// An entry carrying results has no operation to describe, and this sidecar exists to say how the
/// operation was encoded. Empty is the honest answer rather than a fabricated one.
fn wal_command_metadata(
    command: Option<&Command>,
) -> Result<WalCommandMetadata, SharedStoreReplicationError> {
    let Some(command) = command else {
        return Ok(WalCommandMetadata {
            byte_size: 0,
            sha256: sha256_hex(&[]),
            encoding: WAL_COMMAND_ENCODING_JSON_SERDE,
        });
    };
    let (command_payload, command_encoding) = match command_to_sdk_proto(command) {
        Some(command) => (command.encode_to_vec(), WAL_COMMAND_ENCODING_SDK_PROTO),
        None => (
            serde_json::to_vec(command)?,
            WAL_COMMAND_ENCODING_JSON_SERDE,
        ),
    };
    Ok(WalCommandMetadata {
        byte_size: command_payload.len() as u64,
        sha256: sha256_hex(&command_payload),
        encoding: command_encoding,
    })
}

fn encode_wal_proto_frame(
    entry: &SharedStoreWalEntry,
) -> Result<Vec<u8>, SharedStoreReplicationError> {
    let (command_payload, command_encoding) = match entry.command.as_ref() {
        // No operation to carry: the entry states results instead.
        None => (Vec::new(), WAL_COMMAND_ENCODING_JSON_SERDE),
        Some(command) => match command_to_sdk_proto(command) {
            Some(encoded) => (encoded.encode_to_vec(), WAL_COMMAND_ENCODING_SDK_PROTO),
            None => (serde_json::to_vec(command)?, WAL_COMMAND_ENCODING_JSON_SERDE),
        },
    };
    let frame = SharedStoreWalFrameProto {
        shard_id: entry.shard_id,
        wal_index: entry.wal_index,
        command_byte_size: command_payload.len() as u64,
        command_sha256: sha256_hex(&command_payload),
        command_payload,
        command_encoding,
        staged_pages: entry
            .staged_pages
            .iter()
            .map(|page| SharedStoreStagedPageProto {
                object_id: page.object_id,
                bytes: page.bytes.clone(),
            })
            .collect(),
        items: entry
            .outcomes
            .iter()
            .map(crate::wal_proto::item_to_proto)
            .collect(),
    };
    let mut encoded = frame.encode_to_vec();
    let mut out = Vec::with_capacity(4 + encoded.len());
    out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    out.append(&mut encoded);
    Ok(out)
}

fn encode_wal_offset_metadata_frame(metadata: &SharedStoreWalOffsetMetadata) -> Vec<u8> {
    let proto = SharedStoreWalOffsetMetadataProto {
        shard_id: metadata.shard_id,
        wal_index: metadata.wal_index,
        wal_blob_key: metadata.wal_blob_key.clone(),
        wal_blob_start_offset: metadata.wal_blob_start_offset,
        wal_blob_end_offset: metadata.wal_blob_end_offset,
        wal_blob_bytes_written: metadata.wal_blob_bytes_written,
        wal_blob_object_length: metadata.wal_blob_object_length,
        command_byte_size: metadata.command_byte_size,
        command_sha256: metadata.command_sha256.clone(),
        command_encoding: metadata.command_encoding,
        wal_blob_physical_band_count: metadata.wal_blob_physical_band_count,
        wal_blob_first_physical_offset: metadata.wal_blob_first_physical_offset,
    };
    let mut encoded = proto.encode_to_vec();
    let mut out = Vec::with_capacity(4 + encoded.len());
    out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
    out.append(&mut encoded);
    out
}

fn decode_wal_proto_frame_exact(
    bytes: &[u8],
    checksum_source: &str,
) -> Result<SharedStoreWalEntry, SharedStoreReplicationError> {
    if bytes.len() < 4 {
        return Err(SharedStoreReplicationError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated protobuf WAL frame length",
        )));
    }
    let len = u32::from_be_bytes(
        bytes[0..4]
            .try_into()
            .expect("length slice is exactly 4 bytes"),
    ) as usize;
    if bytes.len() != len + 4 {
        return Err(SharedStoreReplicationError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "protobuf WAL frame range does not match frame length",
        )));
    }
    let frame = SharedStoreWalFrameProto::decode(&bytes[4..])?;
    verify_checksum(
        checksum_source,
        &frame.command_payload,
        frame.command_byte_size,
        &frame.command_sha256,
    )?;
    let command = match frame.command_encoding {
        WAL_COMMAND_ENCODING_SDK_PROTO => {
            let command = v1::Command::decode(frame.command_payload.as_slice())?;
            sdk::sdk_command_to_types(command).map_err(|status| {
                SharedStoreReplicationError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    status.to_string(),
                ))
            })?
        }
        WAL_COMMAND_ENCODING_JSON_SERDE => serde_json::from_slice(&frame.command_payload)?,
        other => {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported protobuf WAL command encoding {other}"),
            )));
        }
    };
    Ok(SharedStoreWalEntry {
        shard_id: frame.shard_id,
        wal_index: frame.wal_index,
        command: Some(command),
        outcomes: frame
            .items
            .into_iter()
            .map(crate::wal_proto::item_from_proto)
            .collect(),
        staged_pages: frame
            .staged_pages
            .into_iter()
            .map(|page| crate::wal::StagedPage {
                object_id: page.object_id,
                bytes: page.bytes,
            })
            .collect(),
    })
}

fn decode_wal_proto_blob(
    bytes: &[u8],
) -> Result<Vec<SharedStoreWalEntry>, SharedStoreReplicationError> {
    let mut cursor = 0usize;
    let mut entries = Vec::new();
    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL frame length",
            )));
        }
        let len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("length slice is exactly 4 bytes"),
        ) as usize;
        cursor += 4;
        if bytes.len().saturating_sub(cursor) < len {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL frame payload",
            )));
        }
        let entry =
            decode_wal_proto_frame_exact(&bytes[cursor - 4..cursor + len], "protobuf-wal-blob")?;
        cursor += len;
        entries.push(entry);
    }
    Ok(entries)
}

fn decode_wal_offset_metadata_blob(
    bytes: &[u8],
) -> Result<BTreeMap<u64, SharedStoreWalOffsetMetadata>, SharedStoreReplicationError> {
    let mut cursor = 0usize;
    let mut entries = BTreeMap::new();
    while cursor < bytes.len() {
        if bytes.len().saturating_sub(cursor) < 4 {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL offset metadata frame length",
            )));
        }
        let len = u32::from_be_bytes(
            bytes[cursor..cursor + 4]
                .try_into()
                .expect("length slice is exactly 4 bytes"),
        ) as usize;
        cursor += 4;
        if bytes.len().saturating_sub(cursor) < len {
            return Err(SharedStoreReplicationError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated protobuf WAL offset metadata frame payload",
            )));
        }
        let frame = SharedStoreWalOffsetMetadataProto::decode(&bytes[cursor..cursor + len])?;
        cursor += len;
        entries.insert(
            frame.wal_index,
            SharedStoreWalOffsetMetadata {
                shard_id: frame.shard_id,
                wal_index: frame.wal_index,
                wal_blob_key: frame.wal_blob_key,
                wal_blob_start_offset: frame.wal_blob_start_offset,
                wal_blob_end_offset: frame.wal_blob_end_offset,
                wal_blob_bytes_written: frame.wal_blob_bytes_written,
                wal_blob_object_length: frame.wal_blob_object_length,
                wal_blob_physical_band_count: frame.wal_blob_physical_band_count,
                wal_blob_first_physical_offset: frame.wal_blob_first_physical_offset,
                command_byte_size: frame.command_byte_size,
                command_sha256: frame.command_sha256,
                command_encoding: frame.command_encoding,
            },
        );
    }
    Ok(entries)
}

fn command_to_sdk_proto(command: &Command) -> Option<v1::Command> {
    let kind = match command {
        Command::CommonExpire { key, ttl_ms } => {
            v1::command::Kind::CommonExpire(v1::CommonExpire {
                key: key.clone(),
                ttl_ms: *ttl_ms,
            })
        }
        Command::CommonExists { key } => {
            v1::command::Kind::CommonExists(v1::CommonExists { key: key.clone() })
        }
        Command::StringSet { key, value } => v1::command::Kind::StringSet(v1::StringSet {
            key: key.clone(),
            value: value.clone(),
            ttl_ms: 0,
        }),
        Command::StringSetEx { key, value, ttl_ms } => {
            v1::command::Kind::StringSet(v1::StringSet {
                key: key.clone(),
                value: value.clone(),
                ttl_ms: *ttl_ms,
            })
        }
        Command::StringGet { key } => {
            v1::command::Kind::StringGet(v1::StringGet { key: key.clone() })
        }
        Command::StringDelete { key } => {
            v1::command::Kind::StringDelete(v1::StringDelete { key: key.clone() })
        }
        Command::HashSet { key, field, value } => v1::command::Kind::HashSet(v1::HashSet {
            key: key.clone(),
            field: field.clone(),
            value: value.clone(),
        }),
        Command::HashGet { key, field } => v1::command::Kind::HashGet(v1::HashGet {
            key: key.clone(),
            field: field.clone(),
        }),
        Command::HashMultiSet { key, entries } => {
            v1::command::Kind::HashMultiSet(v1::HashMultiSet {
                key: key.clone(),
                entries: entries
                    .iter()
                    .map(|(field, value)| v1::HashEntry {
                        field: field.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            })
        }
        Command::HashMultiGet { key, fields } => {
            v1::command::Kind::HashMultiGet(v1::HashMultiGet {
                key: key.clone(),
                fields: fields.clone(),
            })
        }
        Command::SetAdd { key, member } => v1::command::Kind::SetAdd(v1::SetAdd {
            key: key.clone(),
            member: member.clone(),
        }),
        Command::SetMembers { key } => {
            v1::command::Kind::SetMembers(v1::SetMembers { key: key.clone() })
        }
        Command::FeatureAppend { key, points } => {
            v1::command::Kind::FeatureAppend(v1::FeatureAppend {
                key: key.clone(),
                points: points
                    .iter()
                    .map(|point| v1::FeaturePoint {
                        timestamp_ms: point.timestamp_ms,
                        value: point.value.clone(),
                    })
                    .collect(),
            })
        }
        Command::FeatureQuery {
            key,
            start_ms,
            end_ms,
            count,
        } => v1::command::Kind::FeatureQuery(v1::FeatureQuery {
            key: key.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            limit: count.unwrap_or(0).min(u32::MAX as usize) as u32,
        }),
        Command::SequenceAdd { key, rows } => {
            v1::command::Kind::SequenceAppend(v1::SequenceAppend {
                key: key.clone(),
                rows: rows
                    .iter()
                    .map(|row| v1::SequenceFeatureRow {
                        timestamp_ms: row.timestamp_ms,
                        gid: row.gid,
                        action_type: row.action_type,
                        duration: row.duration,
                        author_id: row.author_id,
                    })
                    .collect(),
            })
        }
        Command::SequenceQuery {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } if filters.is_empty() => v1::command::Kind::SequenceQuery(v1::SequenceQuery {
            key: key.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
            limit: (*count).min(u32::MAX as usize) as u32,
        }),
        Command::ControlStateIncrement {
            key,
            timestamp_ms,
            amount,
        } => v1::command::Kind::ControlStateIncrement(v1::ControlStateIncrement {
            key: key.clone(),
            family: String::new(),
            delta: *amount,
            timestamp_ms: *timestamp_ms,
        }),
        Command::ControlStateQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
        } => v1::command::Kind::ControlStateQuery(v1::ControlStateQuery {
            key: key.clone(),
            family: aggregator.clone(),
            start_ms: *start_ms,
            end_ms: *end_ms,
        }),
        _ => return None,
    };
    Some(v1::Command { kind: Some(kind) })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore, ObjectStoreError};

    use super::*;
    use crate::types::CommandResponse;

    const TEST_CLUSTER_ID: &str = "cluster-a";
    const TEST_CACHE_BYTES: usize = 1024;

    fn test_engine(root: &Path, role: &str) -> TemporalEngine {
        test_engine_with_cache(root, role, TEST_CACHE_BYTES)
    }

    fn test_engine_with_cache(root: &Path, role: &str, cache_bytes: usize) -> TemporalEngine {
        TemporalEngine::with_local_dirs(
            cache_bytes,
            root.join(format!("{role}-cache")),
            root.join(format!("{role}-pages")),
            root.join(format!("{role}-index")),
        )
    }

    fn test_shared_store(
        root: &Path,
    ) -> (Arc<FileObjectStore>, SharedStoreReplicator<FileObjectStore>) {
        let store = Arc::new(FileObjectStore::new(root.join("objects")));
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone());
        (store, replicator)
    }

    #[tokio::test]
    async fn shared_store_restores_index_pages_and_replays_later_wal() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let (_store, replicator) = test_shared_store(dir.path());
        replicator.publish_index(1, &primary).await.unwrap();
        replicator
            .publish_page_slabs(1, &primary.block_store())
            .await
            .unwrap();
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
                command: Some(Command::StringSet {
                    key: "after".to_string(),
                    value: b"wal-value".to_vec(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        let restored = replicator
            .restore_index_and_pages(1, &follower, &follower.block_store())
            .await
            .unwrap();
        assert_eq!(restored, vec![0]);
        follower.load_shard(1);

        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec())
            }
        );

        let report = replicator.replay_wal(1, 1, &follower).await.unwrap();
        assert_eq!(
            report,
            ReplayReport {
                applied: 1,
                last_wal_index: 2,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "after".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"wal-value".to_vec())
            }
        );
    }

    // A per-key delete produces a durable WAL tombstone that a fresh engine, replaying the WAL
    // from scratch on the SAME on-disk pages/index dirs, reconstructs as a MISS -- the delete must
    // never resurrect on recovery (the failure mode a read-time-only soft-delete would hit).
    #[test]
    fn delete_read_miss_survives_wal_replay_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let pages = dir.path().join("pages");
        let indexes = dir.path().join("indexes");

        let engine =
            TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &pages, &indexes);
        engine.load_shard(1);
        for (key, value) in [("keep", b"keep-value".to_vec()), ("gone", b"gone-value".to_vec())] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value,
                },
            });
        }
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete {
                key: "gone".to_string(),
            },
        });
        // Read-miss immediately after delete.
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "gone".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None }
        );
        engine.unload_shard(1);

        // Fresh engine, same pages/index dirs, different cache -> genuine WAL-replay recovery.
        let reopened =
            TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-b"), &pages, &indexes);
        reopened.load_shard(1);
        assert_eq!(
            reopened
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "gone".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None },
            "deleted key must not resurrect after WAL replay"
        );
        assert_eq!(
            reopened
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "keep".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"keep-value".to_vec())
            },
            "a co-resident key must survive recovery untouched"
        );
    }

    // A delete applied BEFORE the checkpoint rides the published served index/pages: a follower
    // restoring from shared storage sees the key as a miss without ever replaying a WAL tail.
    #[tokio::test]
    async fn delete_tombstone_replicates_in_shared_store_base_index() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        for (key, value) in [("keep", b"keep-value".to_vec()), ("gone", b"gone-value".to_vec())] {
            primary.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value,
                },
            });
        }
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete {
                key: "gone".to_string(),
            },
        });

        let (_store, replicator) = test_shared_store(dir.path());
        replicator.publish_index(1, &primary).await.unwrap();
        replicator
            .publish_page_slabs(1, &primary.block_store())
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        replicator
            .restore_index_and_pages(1, &follower, &follower.block_store())
            .await
            .unwrap();
        follower.load_shard(1);

        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "gone".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None },
            "the delete tombstone must replicate through the shared-store base index"
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "keep".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"keep-value".to_vec())
            }
        );
    }

    // A delete published as a WAL-tail entry replicates too: a follower that restored a base where
    // the key was still live applies the tail delete on replay and then reads it as a miss.
    #[tokio::test]
    async fn delete_tombstone_replicates_via_wal_tail() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        for (key, value) in [("keep", b"keep-value".to_vec()), ("gone", b"gone-value".to_vec())] {
            primary.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value,
                },
            });
        }

        let (_store, replicator) = test_shared_store(dir.path());
        // Base captures both keys LIVE...
        replicator.publish_index(1, &primary).await.unwrap();
        replicator
            .publish_page_slabs(1, &primary.block_store())
            .await
            .unwrap();
        // ...then the delete arrives as a WAL-tail entry after the base.
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
                command: Some(Command::CommonDelete {
                    key: "gone".to_string(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        replicator
            .restore_index_and_pages(1, &follower, &follower.block_store())
            .await
            .unwrap();
        follower.load_shard(1);
        // Before replay the follower still has the pre-delete value.
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "gone".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"gone-value".to_vec())
            }
        );
        let report = replicator.replay_wal(1, 1, &follower).await.unwrap();
        assert_eq!(report.applied, 1);
        assert_eq!(report.last_wal_index, 2);
        // After replaying the tail delete, the key is a miss; the untouched key stays live.
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "gone".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None },
            "the WAL-tail delete must replicate and remove the key on the follower"
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "keep".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"keep-value".to_vec())
            }
        );
    }

    #[cfg(feature = "matrixobject")]
    #[tokio::test]
    async fn matrixobject_local_lazy_restore_replays_only_wal_tail() {
        // The node-local matrixobject backend recovers like every other shared backend:
        // checkpoint index + lazy addresses, then ONLY the WAL tail -- not all history.
        use crate::matrixobject_store::MatrixObjectObjectStore;

        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });
        let store = Arc::new(
            MatrixObjectObjectStore::with_default_options("temporalstore-shared").unwrap(),
        );
        let replicator = SharedStoreReplicator::new("cluster-a", store);
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        assert!(!manifest.page_slabs.is_empty());
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
                command: Some(Command::StringSet {
                    key: "after".to_string(),
                    value: b"wal-value".to_vec(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        let restored = replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();
        assert_eq!(restored.checkpoint_wal_index, 1);
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);
        follower.load_shard(1);
        let report = replicator
            .replay_wal(1, restored.checkpoint_wal_index, &follower)
            .await
            .unwrap();
        assert_eq!(report.applied, 1, "only the tail entry replays");
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "after".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"wal-value".to_vec())
            }
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec())
            }
        );
        assert_eq!(
            follower.block_store().stats().shared_slab_fetches,
            1,
            "the old key is served by exactly one on-demand slab fetch"
        );
    }

    /// OBJECT-STORE MODE: two followers from one origin must agree, and both must serve.
    ///
    /// One follower proves the path works. Two prove it is deterministic: installing the same
    /// results on two nodes that never wrote them has to land in the same place, or a read is
    /// answered differently depending on which replica took it -- which is the failure a single
    /// follower can never show.
    ///
    /// Both are also checked for SERVING rather than for shape. A node whose maps agree and whose
    /// addresses do not resolve passes every comparison and answers nothing, which is exactly the
    /// defect that a restored node had.
    #[tokio::test]
    async fn two_followers_of_one_origin_agree_and_both_serve() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "origin");
        primary.load_shard(1);

        let workload: Vec<(String, Vec<u8>)> = (0..10)
            .map(|index| {
                (
                    format!("tf-{index:02}"),
                    format!("two-followers-{index:02}").into_bytes(),
                )
            })
            .collect();
        for (key, value) in &workload {
            let response = primary.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.clone(),
                    value: value.clone(),
                },
            });
            assert!(response.status.ok);
        }

        let (_store, replicator) = test_shared_store(dir.path());
        let published: Vec<_> = primary
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .iter()
            .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
            .filter(|record| !record.outcomes.is_empty())
            .collect();
        assert!(
            published.len() >= workload.len(),
            "expected a record per write carrying results, got {}",
            published.len()
        );
        for record in &published {
            // Carry the blocks the results point at: a follower has its own block store, and an
            // address alone names a place it cannot reach.
            let mut carried = record.staged_pages.clone();
            if carried.is_empty() {
                for item in &record.outcomes {
                    if let Some(address) = item.resolved_address() {
                        if let Ok(bytes) = primary.block_store().read(&address) {
                            carried.push(crate::wal::StagedPage {
                                object_id: item.object_id,
                                bytes,
                            });
                        }
                    }
                }
            }
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index: record.sequence,
                    command: record.command.clone(),
                    staged_pages: carried,
                    outcomes: record.outcomes.clone(),
                })
                .await
                .unwrap();
        }

        let mut served = Vec::new();
        for role in ["follower-a", "follower-b"] {
            let follower = test_engine(dir.path(), role);
            follower.load_shard(1);
            let before = follower.replay_installs_for_test();
            let report = replicator.replay_wal(1, 0, &follower).await.unwrap();
            let installed = follower.replay_installs_for_test() - before;
            assert!(
                report.applied > 0,
                "{role} took nothing from the shared log"
            );

            let mut answers = Vec::new();
            for (key, value) in &workload {
                let response = follower.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet { key: key.clone() },
                });
                match response.response {
                    CommandResponse::Bytes { value: Some(got) } => {
                        assert_eq!(&got, value, "{role} served the wrong bytes for {key}");
                        answers.push(got);
                    }
                    other => panic!("{role} could not serve {key}: {other:?}"),
                }
            }
            println!("[two-followers] {role}: applied={} installed={installed}", report.applied);
            served.push(answers);
        }

        assert_eq!(
            served[0], served[1],
            "the two followers answered the same reads differently"
        );
    }

    /// RESTORATION on the live format, and the constraint it runs into.
    ///
    /// A restored node takes its index and pages from a checkpoint and everything after it from
    /// the log. On the live format those tail entries carry RESULTS, and this asks whether a
    /// restored node can install them.
    ///
    /// It can install the index entry. It cannot necessarily SERVE it, and that is the finding: a
    /// result names an address in the ORIGIN's block store. Carrying the bytes in the entry does
    /// not help, because installing an address does not write bytes into the successor's store.
    /// Across nodes an address only means something when the SLAB is reachable -- which in shared
    /// mode means published.
    ///
    /// So this asserts the whole property rather than half of it: the node serves both the
    /// checkpointed half and the tail, and reports which path the tail took. A test that asserted
    /// installation alone would pass on a node that cannot answer a single read.
    #[tokio::test]
    async fn a_restored_node_installs_the_tail_instead_of_re_running_it() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);

        // Everything up to here will be covered by the checkpoint.
        for index in 0..4 {
            let response = primary.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("base-{index}"),
                    value: format!("base-value-{index}").into_bytes(),
                },
            });
            assert!(response.status.ok);
        }
        let checkpoint_through = primary
            .write_ahead_log_store()
            .info(1)
            .unwrap()
            .current_sequence;

        let (_store, replicator) = test_shared_store(dir.path());
        replicator
            .publish_checkpoint(1, checkpoint_through, &primary, &primary.block_store())
            .await
            .unwrap();

        // Now the TAIL: written after the checkpoint, and published with what it recorded.
        let tail_write = primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "tail-key".to_string(),
                value: b"tail-value".to_vec(),
            },
        });
        assert!(tail_write.status.ok);

        let tail = primary
            .write_ahead_log_store()
            .scan(1, 0, u64::MAX, u64::MAX)
            .unwrap()
            .iter()
            .filter_map(|(_, line)| crate::wal::decode_wal_line(line).ok())
            .find(|record| record.sequence > checkpoint_through && !record.outcomes.is_empty())
            .expect("the tail write recorded what it did");
        // Publish it the way a correct publisher must: with the BYTES the results point at.
        // A result names an address in the primary's block store, and the follower has its own,
        // so an entry carrying only the address gives the follower an index entry pointing at
        // bytes it does not have -- a shard that looks whole and serves nothing.
        let mut carried = tail.staged_pages.clone();
        if carried.is_empty() {
            for item in &tail.outcomes {
                if let Some(address) = item.resolved_address() {
                    if let Ok(bytes) = primary.block_store().read(&address) {
                        carried.push(crate::wal::StagedPage {
                            object_id: item.object_id,
                            bytes,
                        });
                    }
                }
            }
        }
        assert!(
            !carried.is_empty(),
            "the tail's results point at no readable block, so a follower could not serve them"
        );
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: tail.sequence,
                command: tail.command.clone(),
                staged_pages: carried,
                outcomes: tail.outcomes.clone(),
            })
            .await
            .unwrap();

        // A node that has never seen this shard: restore, then take the tail.
        let follower = test_engine(dir.path(), "restored");
        let restored = replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();
        follower.load_shard(1);
        let installs_before = follower.replay_installs_for_test();
        let report = replicator
            .replay_wal(1, restored.checkpoint_wal_index, &follower)
            .await
            .unwrap();
        let installed = follower.replay_installs_for_test() - installs_before;

        // Reported, not required. Installing is preferred and only possible when the tail's slab
        // is reachable; re-running is the fallback that exists for exactly when it is not. What
        // must hold either way is that the node SERVES what it took.
        println!(
            "[restore] tail applied={} installed={installed}",
            report.applied
        );
        assert!(
            report.applied > 0,
            "the restored node took nothing from the tail at all"
        );
        // Read back BOTH halves: something the checkpoint carried, and something only the tail
        // did. A restored node that serves one and not the other has half a shard.
        for (label, key, want) in [
            ("checkpoint", "base-0", b"base-value-0".to_vec()),
            ("tail", "tail-key", b"tail-value".to_vec()),
        ] {
            let response = follower.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            });
            assert_eq!(
                response.response,
                CommandResponse::Bytes { value: Some(want) },
                "a restored node could not serve the {label} half"
            );
        }
        println!(
            "[restore] checkpoint through {checkpoint_through}, {installed} tail result(s) installed"
        );
    }

    #[tokio::test]
    async fn shared_store_lazy_restore_reads_old_page_on_demand() {
        // On-demand lazy recovery: a fresh node with ONLY shared storage restores the
        // served index + a slab ADDRESS map (no slab bytes), replays the WAL tail, and
        // fetches an old (pre-checkpoint) slab ON DEMAND the first time a read needs it.
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let (_store, replicator) = test_shared_store(dir.path());
        // Publish a real metadata+slab checkpoint at WAL index 1, then a WAL tail entry.
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        assert!(
            !manifest.page_slabs.is_empty(),
            "checkpoint must upload the slab bytes so a lazy owner can fetch them"
        );
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
                command: Some(Command::StringSet {
                    key: "after".to_string(),
                    value: b"wal-value".to_vec(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();

        // Fresh owner: no local slabs at all.
        let follower = test_engine(dir.path(), "follower");
        assert!(follower.block_store().slab_ids().unwrap().is_empty());

        // Lazy restore: index + address map only, NO slab bytes installed up front.
        let restored = replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();
        assert_eq!(restored.checkpoint_wal_index, 1);
        assert!(
            follower.block_store().has_shared_slab_source(),
            "restore must attach a shared read-through source"
        );
        // Proof of laziness: the only local slab is the reserved (empty) append slab;
        // the checkpoint's slab 0 was NOT installed, and nothing was fetched yet.
        assert!(
            !follower.block_store().slab_ids().unwrap().contains(&0),
            "checkpoint slab 0 must not be installed eagerly"
        );
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);

        follower.load_shard(1);
        // Replay only the WAL tail after the checkpoint index: applies exactly "after".
        let report = replicator
            .replay_wal(1, restored.checkpoint_wal_index, &follower)
            .await
            .unwrap();
        assert_eq!(report.applied, 1);
        assert_eq!(report.last_wal_index, 2);
        // The tail write did not touch any pre-checkpoint slab, so still no lazy fetch.
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);

        // Reading the OLD key triggers exactly ONE on-demand slab fetch and is correct.
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec())
            }
        );
        assert_eq!(
            follower.block_store().stats().shared_slab_fetches,
            1,
            "reading one old key must fetch exactly one slab on demand (not all slabs up front)"
        );

        // The fetched slab is now cached locally; a second read does NOT re-fetch.
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec())
            }
        );
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 1);

        // The WAL-tail value is served from the reserved local slab.
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "after".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"wal-value".to_vec())
            }
        );
    }

    #[tokio::test]
    async fn s4_publish_checkpoint_covers_every_index_referenced_slab() {
        // S4 completeness barrier: every slab the served index references must be uploaded and
        // recorded in the manifest before it is written, so a restore never resolves a live page
        // to a slab absent from the checkpoint.
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        for i in 0..8 {
            primary.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("k{i}"),
                    value: vec![b'v'; 512],
                },
            });
        }
        let (_store, replicator) = test_shared_store(dir.path());
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        let manifest_slab_ids: std::collections::BTreeSet<u64> =
            manifest.page_slabs.iter().map(|s| s.page_slab_id).collect();
        for referenced in primary.live_page_slab_ids(1) {
            if referenced == u64::MAX {
                continue;
            }
            assert!(
                manifest_slab_ids.contains(&referenced),
                "index-referenced slab {referenced} must be covered by the checkpoint manifest"
            );
        }
    }

    #[tokio::test]
    async fn s4_publish_checkpoint_rejects_referenced_slab_not_durable() {
        // S4: if the served index references a slab that is NOT present as a durable/uploaded
        // slab, publish must FAIL rather than write a manifest that would lose those pages.
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });
        // The write landed in on-disk slab 0 and the index references it.
        assert!(primary.live_page_slab_ids(1).contains(&0));
        // Simulate a lost/never-durable slab: remove slab 0 from disk so slab_ids() no longer
        // enumerates it while the in-memory index still references it.
        let slab0 = dir
            .path()
            .join("primary-pages")
            .join("page_segment_00000000000000000000.seg");
        std::fs::remove_file(&slab0).unwrap();
        assert!(!primary.block_store().slab_ids().unwrap().contains(&0));

        let (_store, replicator) = test_shared_store(dir.path());
        let err = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .expect_err("publish must reject a checkpoint that would drop a referenced slab");
        assert!(
            matches!(
                err,
                SharedStoreReplicationError::CheckpointSlabNotDurable {
                    shard_id: 1,
                    page_slab_id: 0
                }
            ),
            "expected CheckpointSlabNotDurable, got {err:?}"
        );
    }

    #[tokio::test]
    async fn s3_lazy_restore_installs_complete_sealed_band_descriptors_before_any_fetch() {
        // S3: after a lazy metadata restore, the sealed-band descriptors for the checkpoint's
        // lazily-backed slabs are installed immediately, so GC/compaction accounting is complete
        // BEFORE the first on-demand slab fetch (previously they under-counted until a fetch).
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });
        let primary_band = primary
            .block_store()
            .band_descriptors()
            .into_iter()
            .find(|b| b.page_slab_id == 0)
            .expect("primary must have a band for slab 0");
        assert!(primary_band.logical_bytes > 0);

        let (_store, replicator) = test_shared_store(dir.path());
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        // The manifest carries the per-slab band metadata.
        let slab0 = manifest
            .page_slabs
            .iter()
            .find(|s| s.page_slab_id == 0)
            .expect("manifest must record slab 0");
        assert_eq!(slab0.logical_bytes, primary_band.logical_bytes);

        let follower = test_engine(dir.path(), "follower");
        replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();

        // No slab has been fetched yet...
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);
        assert!(
            !follower.block_store().slab_ids().unwrap().contains(&0),
            "checkpoint slab 0 must not be materialized locally yet"
        );
        // ...but the sealed band descriptor for slab 0 is already present and complete.
        let follower_band = follower
            .block_store()
            .band_descriptors()
            .into_iter()
            .find(|b| b.page_slab_id == 0)
            .expect("restore must install a band descriptor for the lazily-backed slab 0");
        assert_eq!(follower_band.state, crate::block_store::BlockStoreBandState::Sealed);
        assert_eq!(follower_band.logical_bytes, primary_band.logical_bytes);
        assert_eq!(follower_band.physical_bytes, slab0.byte_size);
        // The band summary counts the sealed shared band immediately (accounting is complete).
        assert!(
            follower.block_store().zone_summary().sealed_bands >= 1,
            "sealed shared band must be counted before any lazy fetch"
        );
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);
    }

    #[tokio::test]
    async fn shared_store_restore_before_load_auto_serves_but_load_before_restore_does_not() {
        // Regression for the fresh-node startup ordering bug: on a fresh node the shared
        // restore installs the served index onto the on-disk BASE only; the in-memory
        // shard is populated from that base by `load_shard`. So the ORDER matters:
        //   * restore THEN load  -> load reads the restored index -> node auto-serves.
        //   * load THEN restore  -> load reads an EMPTY index -> reads return null even
        //                           though the restore later wrote the real index to disk.
        // A default fresh node (no join-empty, no manual /load) must use the first order.
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let (_store, replicator) = test_shared_store(dir.path());
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        assert_eq!(manifest.checkpoint_wal_index, 1);
        // One post-checkpoint tail record so we also cover WAL-tail replay ordering.
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
                command: Some(Command::StringSet {
                    key: "after".to_string(),
                    value: b"wal-value".to_vec(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();

        // BUGGY order (load BEFORE restore): reproduces the old default-startup path.
        let buggy = test_engine(dir.path(), "buggy");
        buggy.load_shard(1); // reads an empty on-disk index into memory
        replicator
            .restore_index_and_page_addresses(1, &buggy, &buggy.block_store())
            .await
            .unwrap(); // writes the real index to DISK, but the in-memory shard is already loaded empty
        replicator
            .replay_wal(1, manifest.checkpoint_wal_index, &buggy)
            .await
            .unwrap();
        assert_eq!(
            buggy
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes { value: None },
            "load-before-restore leaves the checkpoint data unreadable in memory (the bug)"
        );

        // FIXED order (restore BEFORE load): what the fixed server startup + /load now do.
        let fixed = test_engine(dir.path(), "fixed");
        replicator
            .restore_index_and_page_addresses(1, &fixed, &fixed.block_store())
            .await
            .unwrap(); // installs the served index onto the on-disk base first
        fixed.load_shard(1); // load now reads the restored index into memory
        replicator
            .replay_wal(1, manifest.checkpoint_wal_index, &fixed)
            .await
            .unwrap();
        // The pre-checkpoint value is served (fetched lazily from shared storage)...
        assert_eq!(
            fixed
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec()),
            },
            "restore-before-load must auto-serve the restored checkpoint data"
        );
        // ...and so is the post-checkpoint WAL-tail value.
        assert_eq!(
            fixed
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "after".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"wal-value".to_vec()),
            },
            "restore-before-load must also serve the replayed WAL tail"
        );
    }

    #[tokio::test]
    async fn shared_store_lazy_restore_replays_only_wal_tail() {
        // WAL-tail replay is O(recent): applied count == number of post-checkpoint
        // records, not the full history.
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let (_store, replicator) = test_shared_store(dir.path());
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        assert_eq!(manifest.checkpoint_wal_index, 1);

        // Three post-checkpoint tail records.
        for (wal_index, key) in [(2u64, "k2"), (3, "k3"), (4, "k4")] {
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Some(Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    }),
                
                                       staged_pages: Vec::new(),
                                                       outcomes: Vec::new(),
                })
                .await
                .unwrap();
        }

        let follower = test_engine(dir.path(), "follower");
        let restored = replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();
        follower.load_shard(1);
        let report = replicator
            .replay_wal(1, restored.checkpoint_wal_index, &follower)
            .await
            .unwrap();
        assert_eq!(
            report.applied, 3,
            "replay applies only the 3 post-checkpoint records, not the full history"
        );
        assert_eq!(report.last_wal_index, 4);
        for key in ["k2", "k3", "k4"] {
            assert_eq!(
                follower
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string()
                        },
                    })
                    .response,
                CommandResponse::Bytes {
                    value: Some(key.as_bytes().to_vec())
                }
            );
        }
    }

    #[tokio::test]
    async fn shared_store_lazy_restore_no_checkpoint_falls_back_to_full_replay() {
        // Backward compatible: with no checkpoint published, the lazy restore reports
        // CheckpointNotFound and the caller replays the full shared WAL from 0.
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        for (wal_index, key, value) in [
            (1u64, "a", b"1".to_vec()),
            (2, "b", b"2".to_vec()),
        ] {
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Some(Command::StringSet {
                        key: key.to_string(),
                        value,
                    }),
                
                                       staged_pages: Vec::new(),
                                                       outcomes: Vec::new(),
                })
                .await
                .unwrap();
        }

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        let after = match replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
        {
            Err(SharedStoreReplicationError::CheckpointNotFound(_)) => 0,
            other => panic!("expected CheckpointNotFound, got {other:?}"),
        };
        assert!(!follower.block_store().has_shared_slab_source());
        let report = replicator.replay_wal(1, after, &follower).await.unwrap();
        assert_eq!(report.applied, 2);
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "a".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"1".to_vec())
            }
        );
    }

    // ---- Networked matrixobject lazy data-follow ----
    // Faithful in-process loopback server speaking the SAME `MORP1` TcpStream
    // request/response frames MatrixObjectHttpStore speaks, so the networked
    // lazy-recovery path is exercised end to end over real sockets (no enterprise
    // crate needed). Each connection is served on its own thread and loops over
    // pooled/keep-alive requests until the client drops it.
    const MOCK_RPC_MAGIC: &[u8; 5] = b"MORP1";

    fn spawn_mock_matrixobject_store() -> String {
        use std::collections::BTreeMap;
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let uri = format!("matrixobject://{addr}");
        let store: Arc<Mutex<BTreeMap<String, Vec<u8>>>> = Arc::default();

        fn serve_conn(mut stream: TcpStream, store: Arc<Mutex<BTreeMap<String, Vec<u8>>>>) {
            loop {
                let mut header = [0u8; 18];
                if stream.read_exact(&mut header).is_err() {
                    return; // client dropped the pooled connection
                }
                if &header[..5] != MOCK_RPC_MAGIC {
                    return;
                }
                let op = header[5];
                let key_len = u32::from_le_bytes(header[6..10].try_into().unwrap()) as usize;
                let value_len = u64::from_le_bytes(header[10..18].try_into().unwrap()) as usize;
                let mut key_bytes = vec![0u8; key_len];
                if stream.read_exact(&mut key_bytes).is_err() {
                    return;
                }
                let mut value = vec![0u8; value_len];
                if stream.read_exact(&mut value).is_err() {
                    return;
                }
                let key = String::from_utf8_lossy(&key_bytes).to_string();
                let (status, body) = {
                    let mut map = store.lock().unwrap();
                    match op {
                        1 => {
                            // PUT
                            map.insert(key, value);
                            (0u8, Vec::new())
                        }
                        2 => match map.get(&key) {
                            // GET
                            Some(bytes) => (0u8, bytes.clone()),
                            None => (1u8, key.into_bytes()),
                        },
                        3 => {
                            // DELETE
                            map.remove(&key);
                            (0u8, Vec::new())
                        }
                        4 => {
                            // LIST prefix
                            let mut keys: Vec<String> =
                                map.keys().filter(|k| k.starts_with(&key)).cloned().collect();
                            keys.sort();
                            (0u8, keys.join("\n").into_bytes())
                        }
                        5 => {
                            // LIST_AFTER prefix=key, after=value
                            let after = String::from_utf8_lossy(&value).to_string();
                            let mut keys: Vec<String> = map
                                .keys()
                                .filter(|k| k.starts_with(&key) && k.as_str() > after.as_str())
                                .cloned()
                                .collect();
                            keys.sort();
                            (0u8, keys.join("\n").into_bytes())
                        }
                        6 => {
                            // GET_MANY: value = keys joined by '\n'
                            let mut out = Vec::new();
                            let mut entries = Vec::new();
                            for k in String::from_utf8_lossy(&value)
                                .split('\n')
                                .filter(|k| !k.is_empty())
                            {
                                if let Some(bytes) = map.get(k) {
                                    entries.push((k.to_string(), bytes.clone()));
                                }
                            }
                            out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                            for (k, v) in entries {
                                out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                                out.extend_from_slice(&(v.len() as u64).to_le_bytes());
                                out.extend_from_slice(k.as_bytes());
                                out.extend_from_slice(&v);
                            }
                            (0u8, out)
                        }
                        7 => {
                            // PUT_MANY: value = count u32 + [key_len u32, value_len u64, key, value]*
                            if value.len() >= 4 {
                                let count =
                                    u32::from_le_bytes(value[0..4].try_into().unwrap()) as usize;
                                let mut off = 4usize;
                                for _ in 0..count {
                                    let kl = u32::from_le_bytes(
                                        value[off..off + 4].try_into().unwrap(),
                                    ) as usize;
                                    off += 4;
                                    let vl = u64::from_le_bytes(
                                        value[off..off + 8].try_into().unwrap(),
                                    ) as usize;
                                    off += 8;
                                    let k =
                                        String::from_utf8_lossy(&value[off..off + kl]).to_string();
                                    off += kl;
                                    let v = value[off..off + vl].to_vec();
                                    off += vl;
                                    map.insert(k, v);
                                }
                            }
                            (0u8, Vec::new())
                        }
                        _ => (2u8, b"unknown op".to_vec()),
                    }
                };
                let mut resp = Vec::with_capacity(14 + body.len());
                resp.extend_from_slice(MOCK_RPC_MAGIC);
                resp.push(status);
                resp.extend_from_slice(&(body.len() as u64).to_le_bytes());
                resp.extend_from_slice(&body);
                if stream.write_all(&resp).is_err() {
                    return;
                }
                let _ = stream.flush();
            }
        }

        std::thread::spawn(move || {
            for conn in listener.incoming() {
                match conn {
                    Ok(stream) => {
                        let store = Arc::clone(&store);
                        std::thread::spawn(move || serve_conn(stream, store));
                    }
                    Err(_) => return,
                }
            }
        });
        uri
    }

    fn networked_replicator(
        uri: &str,
    ) -> (
        Arc<MatrixObjectHttpStore>,
        SharedStoreReplicator<MatrixObjectHttpStore>,
    ) {
        let store = Arc::new(MatrixObjectHttpStore::new(uri).unwrap());
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone());
        (store, replicator)
    }

    #[tokio::test]
    async fn matrixobject_get_blocking_round_trips_over_the_socket() {
        // The sync accessor the lazy slab source relies on must round-trip a real
        // object across the networked MORP1 wire, and surface NotFound for absent keys.
        let uri = spawn_mock_matrixobject_store();
        let store = MatrixObjectHttpStore::new(&uri).unwrap();
        store
            .put("k/one", Bytes::from_static(b"hello-networked"))
            .await
            .unwrap();
        assert_eq!(
            store.get_blocking("k/one").unwrap(),
            Bytes::from_static(b"hello-networked")
        );
        assert!(matches!(
            store.get_blocking("k/missing"),
            Err(ObjectStoreError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn matrixobject_networked_lazy_restore_reads_old_page_on_demand() {
        // Conformance lazy data-follow over the NETWORK: a fresh node with only
        // the networked matrixobject store restores the served index + a slab ADDRESS
        // map (no slab bytes), replays the WAL tail, and fetches an old (pre-checkpoint)
        // slab ON DEMAND over the socket the first time a read needs it.
        let uri = spawn_mock_matrixobject_store();
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let (_store, replicator) = networked_replicator(&uri);
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        assert!(
            !manifest.page_slabs.is_empty(),
            "checkpoint must upload the slab bytes so a lazy owner can fetch them"
        );
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
                command: Some(Command::StringSet {
                    key: "after".to_string(),
                    value: b"wal-value".to_vec(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();

        // Fresh owner: no local slabs at all.
        let follower = test_engine(dir.path(), "follower");
        assert!(follower.block_store().slab_ids().unwrap().is_empty());

        // Lazy restore: index + address map only, NO slab bytes installed up front.
        let restored = replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();
        assert_eq!(restored.checkpoint_wal_index, 1);
        assert!(
            follower.block_store().has_shared_slab_source(),
            "restore must attach a networked shared read-through source"
        );
        assert!(
            !follower.block_store().slab_ids().unwrap().contains(&0),
            "checkpoint slab 0 must not be installed eagerly"
        );
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);

        follower.load_shard(1);
        let report = replicator
            .replay_wal(1, restored.checkpoint_wal_index, &follower)
            .await
            .unwrap();
        assert_eq!(report.applied, 1);
        assert_eq!(report.last_wal_index, 2);
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 0);

        // Reading the OLD key triggers exactly ONE on-demand networked slab fetch.
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec())
            }
        );
        assert_eq!(
            follower.block_store().stats().shared_slab_fetches,
            1,
            "reading one old key must fetch exactly one slab over the network (not all slabs)"
        );

        // Cached now: a second read does NOT re-fetch across the network.
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec())
            }
        );
        assert_eq!(follower.block_store().stats().shared_slab_fetches, 1);

        // The WAL-tail value is served from the reserved local slab.
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "after".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"wal-value".to_vec())
            }
        );
    }

    #[tokio::test]
    async fn matrixobject_networked_lazy_restore_replays_only_wal_tail() {
        // Networked WAL-tail replay is O(recent): applied == post-checkpoint records.
        let uri = spawn_mock_matrixobject_store();
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "before".to_string(),
                value: b"snapshot-value".to_vec(),
            },
        });

        let (_store, replicator) = networked_replicator(&uri);
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        assert_eq!(manifest.checkpoint_wal_index, 1);

        for (wal_index, key) in [(2u64, "k2"), (3, "k3"), (4, "k4")] {
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Some(Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    }),
                
                                       staged_pages: Vec::new(),
                                                       outcomes: Vec::new(),
                })
                .await
                .unwrap();
        }

        let follower = test_engine(dir.path(), "follower");
        let restored = replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
            .unwrap();
        follower.load_shard(1);
        let report = replicator
            .replay_wal(1, restored.checkpoint_wal_index, &follower)
            .await
            .unwrap();
        assert_eq!(
            report.applied, 3,
            "replay applies only the 3 post-checkpoint records, not the full history"
        );
        assert_eq!(report.last_wal_index, 4);
        for key in ["k2", "k3", "k4"] {
            assert_eq!(
                follower
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string()
                        },
                    })
                    .response,
                CommandResponse::Bytes {
                    value: Some(key.as_bytes().to_vec())
                }
            );
        }
    }

    #[tokio::test]
    async fn matrixobject_networked_lazy_restore_no_checkpoint_falls_back_to_full_replay() {
        // Backward compatible over the network: with no checkpoint published, the lazy
        // restore reports CheckpointNotFound and the caller replays the full shared WAL.
        let uri = spawn_mock_matrixobject_store();
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = networked_replicator(&uri);
        for (wal_index, key, value) in
            [(1u64, "a", b"1".to_vec()), (2, "b", b"2".to_vec())]
        {
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Some(Command::StringSet {
                        key: key.to_string(),
                        value,
                    }),
                
                                       staged_pages: Vec::new(),
                                                       outcomes: Vec::new(),
                })
                .await
                .unwrap();
        }

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        let after = match replicator
            .restore_index_and_page_addresses(1, &follower, &follower.block_store())
            .await
        {
            Err(SharedStoreReplicationError::CheckpointNotFound(_)) => 0,
            other => panic!("expected CheckpointNotFound, got {other:?}"),
        };
        assert!(!follower.block_store().has_shared_slab_source());
        let report = replicator.replay_wal(1, after, &follower).await.unwrap();
        assert_eq!(report.applied, 2);
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "a".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"1".to_vec())
            }
        );
    }

    #[tokio::test]
    async fn a_published_entry_keeps_the_pages_its_write_produced() {
        // A command is not always enough to rebuild what it wrote. If the shared log drops the
        // pages, a successor replays the command and reconstructs derived state from whatever
        // it happens to have -- which is not necessarily the bytes that were acked.
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());

        let entry = SharedStoreWalEntry {
            shard_id: 1,
            wal_index: 1,
            command: Some(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            }),
            staged_pages: vec![crate::wal::StagedPage {
                object_id: 77,
                bytes: b"derived-page-bytes".to_vec(),
            }],
                    outcomes: Vec::new(),
        };
        replicator.publish_wal_entry(entry.clone()).await.unwrap();

        let loaded = replicator.load_wal_entries(1).await.unwrap();
        assert_eq!(loaded.len(), 1);
        let round_tripped = loaded.values().next().expect("one entry");
        assert_eq!(
            round_tripped.staged_pages, entry.staged_pages,
            "the pages must survive the trip, not just the command"
        );
    }

    #[tokio::test]
    async fn an_entry_written_before_pages_existed_still_loads() {
        // `staged_pages` is serde(default), so a WAL object published by an older writer -- one
        // that never had the field -- must still deserialize rather than failing the whole
        // replay of a shard's history.
        // Build the legacy shape from a real entry rather than by hand, so this tests the
        // actual serialized form instead of a guess at it: serialize, drop the field an older
        // writer never emitted, and load what remains.
        let entry = SharedStoreWalEntry {
            shard_id: 1,
            wal_index: 1,
            command: Some(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            }),
            staged_pages: Vec::new(),
                    outcomes: Vec::new(),
        };
        let mut legacy = serde_json::to_value(&entry).unwrap();
        legacy
            .as_object_mut()
            .expect("entry serializes to an object")
            .remove("staged_pages");
        assert!(
            legacy.get("staged_pages").is_none(),
            "the field must actually be gone for this to test anything"
        );

        let loaded: SharedStoreWalEntry = serde_json::from_value(legacy)
            .expect("an entry without the field must still load");
        assert!(loaded.staged_pages.is_empty());
        assert_eq!(loaded.command, entry.command);
    }

    #[tokio::test]
    async fn the_async_queue_stops_growing_once_it_reaches_its_cap() {
        // The queue depth IS the loss window: an async write acks once its entry is in memory,
        // so whatever is queued when the process dies non-gracefully is gone. Unbounded, a slow
        // store makes that window grow without end -- the failure gets worse the longer nobody
        // notices. At the cap a write publishes itself instead, which bounds the loss and makes
        // the ack latency say so.
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        let writer = replicator
            .storage_writer(SharedStoreStorageMode::Async, 1)
            .with_max_pending(2);

        for key in ["a", "b"] {
            let report = writer
                .write(
                    1,
                    Command::StringSet {
                        key: key.to_string(),
                        value: b"v".to_vec(),
                    },
                )
                .await
                .unwrap();
            assert!(report.queued, "below the cap a write defers: {report:?}");
            assert!(!report.published);
        }
        assert_eq!(writer.queued_len(), 2);
        assert_eq!(writer.queue_capacity_hits(), 0);

        // The third write finds the queue full and pays for its own durability.
        let report = writer
            .write(
                1,
                Command::StringSet {
                    key: "c".to_string(),
                    value: b"v".to_vec(),
                },
            )
            .await
            .unwrap();
        assert!(report.published, "at the cap the write publishes: {report:?}");
        assert!(!report.queued);
        assert_eq!(
            writer.queued_len(),
            2,
            "the backlog must not have grown past the cap"
        );
        assert_eq!(writer.queue_capacity_hits(), 1);
    }

    #[tokio::test]
    async fn an_unbounded_async_queue_still_defers_every_write() {
        // 0 restores the previous behaviour exactly, so an operator who wants the old
        // unbounded queue can still have it -- and the escape hatch is tested, not assumed.
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        let writer = replicator
            .storage_writer(SharedStoreStorageMode::Async, 1)
            .with_max_pending(0);

        for key in ["a", "b", "c", "d"] {
            let report = writer
                .write(
                    1,
                    Command::StringSet {
                        key: key.to_string(),
                        value: b"v".to_vec(),
                    },
                )
                .await
                .unwrap();
            assert!(report.queued, "unbounded: every write defers: {report:?}");
        }
        assert_eq!(writer.queued_len(), 4);
        assert_eq!(writer.queue_capacity_hits(), 0);
    }

    #[tokio::test]
    async fn bucket_dump_manifests_travel_with_the_data_to_a_new_owner() {
        // In shared mode the data outlives the node, but the manifest describing it was only
        // ever written to that node's local index dir. Losing the node lost the lineage for
        // data that was still perfectly present -- the next owner could not tell what had
        // already been dumped, so it had to treat every live generation as undumped.
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());

        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        let dumped = primary.create_bucket_dump_manifest(1, Vec::new()).unwrap();

        let published = replicator
            .publish_bucket_dump_manifests(1, &primary.list_bucket_dump_manifests(1))
            .await
            .unwrap();
        assert_eq!(published, 1);

        // A different node: its own index dir, and no local lineage whatsoever.
        let successor = test_engine(dir.path(), "successor");
        successor.load_shard(1);
        assert!(successor.list_bucket_dump_manifests(1).is_empty());

        let restored = replicator
            .restore_bucket_dump_manifests(1, &successor)
            .await
            .unwrap();
        assert_eq!(restored, 1);

        let inherited = successor.list_bucket_dump_manifests(1);
        assert_eq!(inherited.len(), 1);
        assert_eq!(inherited[0].manifest_id, dumped.manifest_id);
        assert_eq!(
            inherited[0].wal_sequence, dumped.wal_sequence,
            "the reclaim watermark has to survive the move, not merely the file"
        );
    }

    #[tokio::test]
    async fn restoring_bucket_dump_manifests_leaves_a_local_one_alone() {
        // A local manifest is at least as current as a published one, and it is the file that
        // authorizes WAL reclaim. Rewriting it on every restore would churn exactly the wrong
        // thing, so a manifest this node already has is skipped rather than overwritten.
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());

        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        primary.create_bucket_dump_manifest(1, Vec::new()).unwrap();
        replicator
            .publish_bucket_dump_manifests(1, &primary.list_bucket_dump_manifests(1))
            .await
            .unwrap();

        // Restoring onto the very node that produced them writes nothing.
        let restored = replicator
            .restore_bucket_dump_manifests(1, &primary)
            .await
            .unwrap();
        assert_eq!(restored, 0);
        assert_eq!(primary.list_bucket_dump_manifests(1).len(), 1);
    }

    #[tokio::test]
    async fn shared_store_checkpoint_rejects_corrupt_page_slab() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        primary.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });

        let (store, replicator) = test_shared_store(dir.path());
        let manifest = replicator
            .publish_checkpoint(1, 1, &primary, &primary.block_store())
            .await
            .unwrap();
        store
            .put(
                &manifest.page_slabs[0].key,
                Bytes::from_static(b"corrupt"),
            )
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        assert!(matches!(
            replicator
                .restore_checkpoint(&manifest, &follower, &follower.block_store())
                .await
                .unwrap_err(),
            SharedStoreReplicationError::ChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn shared_store_strict_replay_rejects_wal_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 2,
                command: Some(Command::StringSet {
                    key: "gap".to_string(),
                    value: b"v".to_vec(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        assert!(matches!(
            replicator
                .replay_wal_strict(1, 0, &follower)
                .await
                .unwrap_err(),
            SharedStoreReplicationError::ReplayGap {
                expected: 1,
                actual: 2
            }
        ));
    }

    #[tokio::test]
    async fn shared_store_sync_storage_publishes_and_cursor_replay_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        let writer = replicator.storage_writer(SharedStoreStorageMode::Sync, 1);

        let report = writer
            .write(
                1,
                Command::StringSet {
                    key: "sync".to_string(),
                    value: b"published".to_vec(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            report,
            SharedStoreWriteReport {
                wal_index: 1,
                published: true,
                queued: false,
                wal_blob_start_offset: None,
                wal_blob_end_offset: None,
                wal_blob_bytes_written: None,
                wal_blob_object_length: None,
            }
        );

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        let replay = replicator
            .replay_wal_strict_with_cursor(1, &follower)
            .await
            .unwrap();
        assert_eq!(
            replay,
            ReplayReport {
                applied: 1,
                last_wal_index: 1,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );
        assert_eq!(
            replicator
                .load_replay_cursor(1)
                .await
                .unwrap()
                .last_wal_index,
            1
        );
        assert_eq!(
            replicator
                .replay_wal_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 0,
                last_wal_index: 1,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "sync".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"published".to_vec())
            }
        );
    }

    #[tokio::test]
    async fn shared_store_async_storage_flushes_in_order_with_limit() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, replicator) = test_shared_store(dir.path());
        let writer = replicator.storage_writer(SharedStoreStorageMode::Async, 1);

        for (key, value) in [("a", b"1".to_vec()), ("b", b"2".to_vec())] {
            let report = writer
                .write(
                    1,
                    Command::StringSet {
                        key: key.to_string(),
                        value,
                    },
                )
                .await
                .unwrap();
            assert!(report.queued);
            assert!(!report.published);
        }
        assert_eq!(writer.queued_len(), 2);

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        assert_eq!(
            replicator
                .replay_wal_strict(1, 0, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 0,
                last_wal_index: 0,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );

        assert_eq!(
            writer.flush_pending(1).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 1,
                last_wal_index: 1,
                last_wal_blob_start_offset: None,
                last_wal_blob_end_offset: None,
                last_wal_blob_object_length: None,
            }
        );
        assert_eq!(
            replicator
                .replay_wal_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 1,
                last_wal_index: 1,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );

        assert_eq!(
            writer.flush_pending(8).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 0,
                last_wal_index: 2,
                last_wal_blob_start_offset: None,
                last_wal_blob_end_offset: None,
                last_wal_blob_object_length: None,
            }
        );
        assert_eq!(
            replicator
                .replay_wal_strict_with_cursor(1, &follower)
                .await
                .unwrap(),
            ReplayReport {
                applied: 1,
                last_wal_index: 2,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "b".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"2".to_vec())
            }
        );
    }

    // shared-corpus: storage_disk_shared_store_persistence_parity
    #[tokio::test]
    async fn disk_and_shared_store_persistence_recover_through_restart_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        assert!(
            primary
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "disk-key".to_string(),
                        value: b"disk-value".to_vec(),
                    },
                })
                .status
                .ok
        );
        drop(primary);

        let restarted_primary = test_engine(dir.path(), "primary");
        restarted_primary.load_shard(1);
        assert_eq!(
            restarted_primary
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "disk-key".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"disk-value".to_vec())
            }
        );

        let (_store, replicator) = test_shared_store(dir.path());
        let manifest = replicator
            .publish_checkpoint(1, 1, &restarted_primary, &restarted_primary.block_store())
            .await
            .unwrap();
        assert_eq!(manifest.checkpoint_wal_index, 1);
        assert!(!manifest.page_slabs.is_empty());

        let sync_writer = replicator.storage_writer(SharedStoreStorageMode::Sync, 2);
        assert_eq!(
            sync_writer
                .write(
                    1,
                    Command::StringSet {
                        key: "shared-sync".to_string(),
                        value: b"sync-value".to_vec(),
                    },
                )
                .await
                .unwrap(),
            SharedStoreWriteReport {
                wal_index: 2,
                published: true,
                queued: false,
                wal_blob_start_offset: None,
                wal_blob_end_offset: None,
                wal_blob_bytes_written: None,
                wal_blob_object_length: None,
            }
        );

        let async_writer = replicator.storage_writer(SharedStoreStorageMode::Async, 3);
        assert_eq!(
            async_writer
                .write(
                    1,
                    Command::StringSet {
                        key: "shared-async".to_string(),
                        value: b"async-value".to_vec(),
                    },
                )
                .await
                .unwrap(),
            SharedStoreWriteReport {
                wal_index: 3,
                published: false,
                queued: true,
                wal_blob_start_offset: None,
                wal_blob_end_offset: None,
                wal_blob_bytes_written: None,
                wal_blob_object_length: None,
            }
        );
        assert_eq!(
            async_writer.flush_pending(8).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 0,
                last_wal_index: 3,
                last_wal_blob_start_offset: None,
                last_wal_blob_end_offset: None,
                last_wal_blob_object_length: None,
            }
        );

        let follower = test_engine_with_cache(dir.path(), "follower", 32);
        replicator
            .restore_checkpoint(&manifest, &follower, &follower.block_store())
            .await
            .unwrap();
        // Capture the baseline BEFORE load_shard: eager cache warm-on-load (default ON,
        // MATRIXARK_EAGER_CACHE_WARM_ON_LOAD) reads the checkpointed disk-backed segments into
        // cache during load, so the subsequent StringGet may be served from that warm cache.
        // Counting from before load makes the assertion robust to warm-on-load either way.
        let reads_before = follower.block_store().stats().reads;
        follower.load_shard(1);
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "disk-key".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"disk-value".to_vec())
            }
        );
        assert!(
            follower.block_store().stats().reads > reads_before,
            "restored follower should read checkpointed bytes from disk-backed block segments \
             (via eager cache warm on load and/or the read)"
        );

        let replay = replicator.replay_wal_strict(1, 1, &follower).await.unwrap();
        assert_eq!(
            replay,
            ReplayReport {
                applied: 2,
                last_wal_index: 3,
                offset_index_reads: 0,
                range_bytes_read: 0,
            }
        );
        for (key, value) in [
            ("shared-sync", b"sync-value".to_vec()),
            ("shared-async", b"async-value".to_vec()),
        ] {
            assert_eq!(
                follower
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string()
                        },
                    })
                    .response,
                CommandResponse::Bytes { value: Some(value) }
            );
        }

        replicator
            .save_replay_cursor(&SharedStoreReplayCursor {
                shard_id: 1,
                last_wal_index: replay.last_wal_index,
                last_replay_time_ms: now_ms(),
            })
            .await
            .unwrap();
        let gc = replicator
            .gc_wal_before_cursor_safe(1, 5)
            .await
            .unwrap_err();
        assert!(matches!(
            gc,
            SharedStoreReplicationError::GcBlockedByReplayCursor {
                cursor_wal_index: 3,
                retain_from_wal_index: 5,
            }
        ));
    }

    #[tokio::test]
    async fn shared_store_rejects_corrupt_wal_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let (store, replicator) = test_shared_store(dir.path());
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 1,
                command: Some(Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();

        let key = "cluster-a/shards/1/shared/wal/wal_00000000000000000001.json";
        let mut object: SharedStoreWalObject =
            serde_json::from_slice(&store.get(key).await.unwrap()).unwrap();
        object.entry_sha256 = "bad".to_string();
        store
            .put(
                key,
                Bytes::from(serde_json::to_vec_pretty(&object).unwrap()),
            )
            .await
            .unwrap();

        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        assert!(matches!(
            replicator
                .replay_wal_strict(1, 0, &follower)
                .await
                .unwrap_err(),
            SharedStoreReplicationError::ChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn shared_store_retry_policy_retries_transient_put_failures() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FlakyObjectStore {
            inner: FileObjectStore::new(dir.path().join("objects")),
            fail_puts: Mutex::new(1),
        });
        let replicator = SharedStoreReplicator::with_retry_policy(
            "cluster-a",
            store,
            SharedStoreRetryPolicy {
                max_attempts: 2,
                backoff_ms: 0,
            },
        );
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index: 1,
                command: Some(Command::StringSet {
                    key: "retry".to_string(),
                    value: b"ok".to_vec(),
                }),
            
                                   staged_pages: Vec::new(),
                                               outcomes: Vec::new(),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shared_store_async_flush_requeues_after_publish_failure() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FlakyObjectStore {
            inner: FileObjectStore::new(dir.path().join("objects")),
            fail_puts: Mutex::new(1),
        });
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store);
        let writer = replicator.storage_writer(SharedStoreStorageMode::Async, 1);
        writer
            .write(
                1,
                Command::StringSet {
                    key: "retry-queue".to_string(),
                    value: b"ok".to_vec(),
                },
            )
            .await
            .unwrap();

        assert!(writer.flush_pending(1).await.is_err());
        assert_eq!(writer.queued_len(), 1);
        assert_eq!(
            writer.flush_pending(1).await.unwrap(),
            SharedStoreFlushReport {
                flushed: 1,
                remaining: 0,
                last_wal_index: 1,
                last_wal_blob_start_offset: None,
                last_wal_blob_end_offset: None,
                last_wal_blob_object_length: None,
            }
        );
    }

    #[tokio::test]
    async fn shared_store_gc_removes_old_wal_and_checkpoint_generations() {
        let dir = tempfile::tempdir().unwrap();
        let primary = test_engine(dir.path(), "primary");
        primary.load_shard(1);
        let (store, replicator) = test_shared_store(dir.path());

        for wal_index in 1..=3 {
            replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Some(Command::StringSet {
                        key: format!("k{wal_index}"),
                        value: vec![wal_index as u8],
                    }),
                
                                       staged_pages: Vec::new(),
                                                       outcomes: Vec::new(),
                })
                .await
                .unwrap();
        }
        let wal_gc = replicator.gc_wal_before(1, 3).await.unwrap();
        assert_eq!(wal_gc.deleted_wal_objects, 2);
        let wal_keys = store
            .list("cluster-a/shards/1/shared/wal/")
            .await
            .unwrap();
        assert_eq!(wal_keys.len(), 1);
        assert!(wal_keys[0].ends_with("wal_00000000000000000003.json"));

        for checkpoint_wal_index in 1..=3 {
            primary.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("checkpoint-{checkpoint_wal_index}"),
                    value: vec![checkpoint_wal_index as u8],
                },
            });
            replicator
                .publish_checkpoint(1, checkpoint_wal_index, &primary, &primary.block_store())
                .await
                .unwrap();
        }
        let checkpoint_gc = replicator.gc_checkpoints(1, 1).await.unwrap();
        assert_eq!(checkpoint_gc.deleted_checkpoints, 2);
        assert_eq!(checkpoint_gc.retained_checkpoint_ids.len(), 1);
        assert_eq!(replicator.list_checkpoints(1).await.unwrap().len(), 1);
    }

    #[cfg(feature = "matrixobject")]
    #[tokio::test]
    async fn shared_store_protobuf_append_blob_replays_matrixobject_blob() {
        use crate::matrixobject_store::MatrixObjectObjectStore;
        use matrixobjectstore_rs::StoreOptions;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            MatrixObjectObjectStore::new(
                "temporalstore-shared",
                StoreOptions {
                    segment_size: 16,
                    max_extent_bytes: 4,
                    chunk_size: 4,
                    ..StoreOptions::default()
                },
            )
            .unwrap(),
        );
        let replicator = SharedStoreReplicator::new("cluster-a", store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

        let mut append_receipts = Vec::new();
        for (wal_index, key, value) in [
            (1, "proto-a", b"one".to_vec()),
            (2, "proto-b", b"two".to_vec()),
        ] {
            let receipt = replicator
                .publish_wal_entry(SharedStoreWalEntry {
                    shard_id: 1,
                    wal_index,
                    command: Some(Command::StringSet {
                        key: key.to_string(),
                        value,
                    }),
                
                                       staged_pages: Vec::new(),
                                                       outcomes: Vec::new(),
                })
                .await
                .unwrap();
            append_receipts.push(receipt.expect("protobuf append blob should return offsets"));
        }
        assert_eq!(append_receipts[0].start_offset, 0);
        assert_eq!(
            append_receipts[0].end_offset,
            append_receipts[1].start_offset
        );
        assert_eq!(
            append_receipts[1].end_offset,
            append_receipts[1].object_length
        );
        assert!(append_receipts[1].physical_band_count > 1);

        let blob_key = "cluster-a/shards/1/shared/wal/wal.protobuf.blob";
        let offset_index_key = "cluster-a/shards/1/shared/wal/wal.offset_index.protobuf.blob";
        assert_eq!(
            store
                .list("cluster-a/shards/1/shared/wal/")
                .await
                .unwrap(),
            vec![offset_index_key.to_string(), blob_key.to_string()]
        );
        assert!(!store.get(blob_key).await.unwrap().is_empty());
        assert!(!store.get(offset_index_key).await.unwrap().is_empty());
        let offset_metadata = replicator.load_wal_offset_metadata(1).await.unwrap();
        assert_eq!(offset_metadata.len(), append_receipts.len());
        for (index, receipt) in append_receipts.iter().enumerate() {
            let metadata = offset_metadata
                .get(&(index as u64 + 1))
                .expect("offset metadata by wal index");
            assert_eq!(metadata.wal_blob_key, blob_key);
            assert_eq!(metadata.wal_blob_start_offset, receipt.start_offset);
            assert_eq!(metadata.wal_blob_end_offset, receipt.end_offset);
            assert_eq!(metadata.wal_blob_bytes_written, receipt.bytes_written);
        }
        let matrixobject_blob = store
            .inner()
            .lock()
            .expect("matrixobject lock poisoned")
            .get_object("temporalstore-shared", blob_key)
            .unwrap();
        assert!(
            matrixobject_blob.metadata.extents.len() > 1,
            "protobuf WAL frames should be appended into one MatrixObject blob"
        );

        let restarted = SharedStoreReplicator::new("cluster-a", store)
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        assert_eq!(
            restarted.replay_wal_strict(1, 0, &follower).await.unwrap(),
            ReplayReport {
                applied: 2,
                last_wal_index: 2,
                offset_index_reads: 2,
                range_bytes_read: append_receipts
                    .iter()
                    .map(|receipt| receipt.bytes_written)
                    .sum(),
            }
        );
        for (key, value) in [("proto-a", b"one".to_vec()), ("proto-b", b"two".to_vec())] {
            assert_eq!(
                follower
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string()
                        },
                    })
                    .response,
                CommandResponse::Bytes { value: Some(value) }
            );
        }
    }

    /// R2: a durable, cross-owner single-writer fence. Proves rejection happens at the STORE
    /// layer (a superseded stale owner cannot double-append to the shared WAL), not merely via
    /// an in-memory load_version check.
    #[tokio::test]
    async fn r2_stale_load_version_writer_is_rejected_at_store_layer() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileObjectStore::new(dir.path().join("objects")));
        let shard: ShardId = 7;

        // Owner A claims the shard at load_version 1 and writes successfully.
        let repl_a = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone()).with_fence(1, "A");
        repl_a.acquire_shard_lease(shard, 1, "A").await.unwrap();
        let writer_a = repl_a.storage_writer(SharedStoreStorageMode::Sync, 1);
        writer_a
            .write(
                shard,
                Command::StringSet {
                    key: "k".to_string(),
                    value: b"from-a".to_vec(),
                },
            )
            .await
            .expect("owner A write should succeed while it holds the lease");

        // A newer owner B takes over at a strictly higher load_version 2.
        let repl_b = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone()).with_fence(2, "B");
        repl_b.acquire_shard_lease(shard, 2, "B").await.unwrap();

        // The stale owner A is now fenced OUT: its next WAL append is rejected at the store
        // layer, so it cannot double-append and corrupt the shared WAL.
        let err = writer_a
            .write(
                shard,
                Command::StringSet {
                    key: "k".to_string(),
                    value: b"stale-a".to_vec(),
                },
            )
            .await
            .expect_err("stale owner A must be rejected after being superseded");
        assert!(
            matches!(err, SharedStoreReplicationError::StoreConditionFailed { .. }),
            "expected fence rejection, got {err:?}"
        );

        // Owner B, holding the current lease, still writes fine.
        let writer_b = repl_b.storage_writer(SharedStoreStorageMode::Sync, 2);
        writer_b
            .write(
                shard,
                Command::StringSet {
                    key: "k".to_string(),
                    value: b"from-b".to_vec(),
                },
            )
            .await
            .expect("current owner B write should succeed");

        // A stale ownership claim (equal-or-lower load_version) cannot acquire the lease.
        let repl_c = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone()).with_fence(1, "C");
        let acquire_err = repl_c
            .acquire_shard_lease(shard, 1, "C")
            .await
            .expect_err("a stale load_version must not be able to acquire the lease");
        assert!(
            matches!(acquire_err, SharedStoreReplicationError::StaleOwnership { .. }),
            "expected StaleOwnership, got {acquire_err:?}"
        );

        // The underlying store CAS is itself fenced: a wrong `expected` is rejected.
        let cas_err = store
            .compare_and_swap(
                "leases/probe.lease",
                Some(Bytes::from_static(b"never-written")),
                Bytes::from_static(b"new"),
            )
            .await
            .expect_err("CAS with a mismatched expected value must fail");
        assert!(matches!(cas_err, ObjectStoreError::ConditionFailed { .. }));
    }

    #[derive(Debug)]
    struct FlakyObjectStore {
        inner: FileObjectStore,
        fail_puts: Mutex<usize>,
    }

    #[async_trait]
    impl ObjectStore for FlakyObjectStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
            {
                let mut fail_puts = self.fail_puts.lock().expect("flaky store lock poisoned");
                if *fail_puts > 0 {
                    *fail_puts -= 1;
                    return Err(ObjectStoreError::InvalidKey("injected failure".to_string()));
                }
            }
            self.inner.put(key, bytes).await
        }

        async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
            self.inner.get(key).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn uri(&self, key: &str) -> String {
            self.inner.uri(key)
        }
    }

    // ---- Group-commit (timer-less queue-coalesced) tests --------------------------------------

    /// Wraps a real `FileObjectStore` and counts durable `append_blob` barriers so a test can prove
    /// that N concurrent sync writers were coalesced onto far fewer than N appends. Everything else
    /// delegates straight through, so replay/recovery behave exactly as against the inner store.
    #[derive(Debug)]
    struct CountingObjectStore {
        inner: FileObjectStore,
        append_blobs: std::sync::atomic::AtomicUsize,
    }

    impl CountingObjectStore {
        fn new(inner: FileObjectStore) -> Self {
            Self {
                inner,
                append_blobs: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn append_blob_count(&self) -> usize {
            self.append_blobs.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ObjectStore for CountingObjectStore {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), ObjectStoreError> {
            self.inner.put(key, bytes).await
        }
        async fn append_blob(
            &self,
            key: &str,
            bytes: Bytes,
        ) -> Result<AppendBlobReceipt, ObjectStoreError> {
            self.append_blobs
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.append_blob(key, bytes).await
        }
        async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
            self.inner.get(key).await
        }
        async fn get_range(
            &self,
            key: &str,
            offset: u64,
            length: u64,
        ) -> Result<Bytes, ObjectStoreError> {
            self.inner.get_range(key, offset, length).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }
        fn uri(&self, key: &str) -> String {
            self.inner.uri(key)
        }
    }

    /// Fails EVERY `append_blob` (the shared-store durable barrier) so the group-commit failure
    /// path can be exercised: no covered writer may be told Ok.
    #[derive(Debug, Default)]
    struct FailingAppendStore;

    #[async_trait]
    impl ObjectStore for FailingAppendStore {
        async fn put(&self, _key: &str, _bytes: Bytes) -> Result<(), ObjectStoreError> {
            Err(ObjectStoreError::InvalidKey("injected append failure".to_string()))
        }
        async fn append_blob(
            &self,
            _key: &str,
            _bytes: Bytes,
        ) -> Result<AppendBlobReceipt, ObjectStoreError> {
            Err(ObjectStoreError::InvalidKey("injected append failure".to_string()))
        }
        async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
            Err(ObjectStoreError::NotFound(key.to_string()))
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
            Ok(Vec::new())
        }
        async fn delete(&self, _key: &str) -> Result<(), ObjectStoreError> {
            Ok(())
        }
        fn uri(&self, key: &str) -> String {
            key.to_string()
        }
    }

    /// (a) Concurrent sync writers coalesce onto FAR fewer durable appends than writes, while every
    /// write is Ok with a distinct byte range, and (b) every acked write is recoverable after a
    /// simulated restart (fresh replicator replays the shared WAL) — proving durability.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_coalesces_concurrent_sync_writers_and_stays_durable() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(CountingObjectStore::new(FileObjectStore::new(
            dir.path().join("objects"),
        )));
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
        // A small commit_delay widens the batch deterministically so the spawned writers land in the
        // same group; the coalescing itself does not depend on it (delay 0 still batches whatever
        // accrues during one append's in-flight window).
        let writer = Arc::new(
            replicator
                .storage_writer(SharedStoreStorageMode::Sync, 1)
                .with_group_commit(true, Duration::from_millis(5)),
        );

        const WRITERS: usize = 16;
        let mut handles = Vec::new();
        for i in 0..WRITERS {
            let writer = Arc::clone(&writer);
            handles.push(tokio::spawn(async move {
                writer
                    .write(
                        1,
                        Command::StringSet {
                            key: format!("k{i}"),
                            value: format!("v{i}").into_bytes(),
                        },
                    )
                    .await
            }));
        }
        let mut reports = Vec::new();
        for handle in handles {
            reports.push(handle.await.unwrap().expect("every group-commit write must ack Ok"));
        }

        // Every write acked durable with a distinct, non-overlapping byte range.
        let mut ranges: Vec<(u64, u64)> = reports
            .iter()
            .map(|report| {
                (
                    report.wal_blob_start_offset.expect("start offset"),
                    report.wal_blob_end_offset.expect("end offset"),
                )
            })
            .collect();
        ranges.sort();
        for pair in ranges.windows(2) {
            assert!(
                pair[0].1 <= pair[1].0,
                "WAL byte ranges must not overlap: {:?} vs {:?}",
                pair[0],
                pair[1]
            );
        }

        // THE proof: far fewer durable appends than writes (each round is 2 appends: WAL + offset).
        let appends = store.append_blob_count();
        eprintln!(
            "group-commit: {WRITERS} concurrent sync writes coalesced onto {appends} object-store appends (WAL+offset per round)"
        );
        assert!(
            appends < WRITERS,
            "expected group commit to coalesce {WRITERS} writes onto fewer appends, saw {appends}"
        );

        // Durability: a fresh replicator (simulated restart) recovers EVERY acked write.
        let restarted = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
        assert_eq!(
            restarted.latest_persisted_wal_index(1).await.unwrap(),
            WRITERS as u64
        );
        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        let report = restarted.replay_wal(1, 0, &follower).await.unwrap();
        assert_eq!(report.applied, WRITERS);
        for i in 0..WRITERS {
            assert_eq!(
                follower
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet { key: format!("k{i}") },
                    })
                    .response,
                CommandResponse::Bytes {
                    value: Some(format!("v{i}").into_bytes())
                },
                "acked write k{i} must be durable after restart"
            );
        }
    }

    /// (c) Fsync/append-failure path: when the covering append fails, EVERY covered writer gets an
    /// Err — none is falsely acked — and nothing is persisted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_append_failure_fails_all_covered_writers() {
        let store = Arc::new(FailingAppendStore);
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store)
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
        let writer = Arc::new(
            replicator
                .storage_writer(SharedStoreStorageMode::Sync, 1)
                // Widen so the writers share one (failing) barrier — proving the fan-out of the error.
                .with_group_commit(true, Duration::from_millis(5)),
        );

        const WRITERS: usize = 8;
        let mut handles = Vec::new();
        for i in 0..WRITERS {
            let writer = Arc::clone(&writer);
            handles.push(tokio::spawn(async move {
                writer
                    .write(
                        1,
                        Command::StringSet {
                            key: format!("k{i}"),
                            value: b"v".to_vec(),
                        },
                    )
                    .await
            }));
        }
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(
                result.is_err(),
                "a write whose covering append failed must NOT be acked Ok"
            );
        }
    }

    /// (d) A lone sync writer under group commit is correct and does not stall waiting for a batch:
    /// it drains a one-entry group immediately (2 appends: WAL + offset) and is recoverable.
    #[tokio::test]
    async fn group_commit_single_writer_is_correct_and_immediate() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(CountingObjectStore::new(FileObjectStore::new(
            dir.path().join("objects"),
        )));
        let replicator = SharedStoreReplicator::new(TEST_CLUSTER_ID, store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
        // Pure timer-less (delay 0): a lone writer must not wait on anything.
        let writer = replicator
            .storage_writer(SharedStoreStorageMode::Sync, 1)
            .with_group_commit(true, Duration::ZERO);

        let report = writer
            .write(
                1,
                Command::StringSet {
                    key: "solo".to_string(),
                    value: b"value".to_vec(),
                },
            )
            .await
            .expect("lone group-commit write must ack Ok");
        assert_eq!(report.wal_index, 1);
        assert!(report.published);
        assert_eq!(report.wal_blob_start_offset, Some(0));
        // Exactly one group → one WAL append + one offset-index append.
        assert_eq!(store.append_blob_count(), 2);

        let restarted = SharedStoreReplicator::new(TEST_CLUSTER_ID, store)
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
        let follower = test_engine(dir.path(), "follower");
        follower.load_shard(1);
        restarted.replay_wal(1, 0, &follower).await.unwrap();
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet { key: "solo".to_string() },
                })
                .response,
            CommandResponse::Bytes { value: Some(b"value".to_vec()) }
        );
    }
}
