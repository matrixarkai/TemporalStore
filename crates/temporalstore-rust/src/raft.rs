// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{
    Condvar,
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex, RwLock,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::engine::TemporalEngine;
use crate::http::{
    json_response, parse_json, post_json_with_options, HttpRequest, HttpRequestOptions,
};
use crate::meta::{
    AckResponse, AddNamespaceRequest, AddTableRequest, DeleteTableRequest, GetShardResponse,
    GetTableTopologyRequest, ListNamespacesResponse, ListProxiesResponse, ListServersResponse,
    MetaMetricsReport, StateTally,
    ListShardsResponse,
    ListTablesResponse, LoadFinishRequest, MetaEntityState, MetaInfo, MetaMutation,
    MetaPreflightReport, MetaSnapshot, MetaStats, ProxyHeartbeatRequest, ProxyHeartbeatResponse,
    PublishShardSnapshotRequest, RegisterProxyRequest, RegisterServerRequest, RegisterShardRequest,
    RegisterShardResponse, SafeModePolicy, SafeModeReport, ServerHeartbeatRequest,
    ServerHeartbeatResponse, ShardLocation, ShardSnapshotRef, SingleNodeMeta, StaleResourceReport,
    StaleServerReport, StateChangeRequest, TableTopologyResponse, TopologyVersionReport,
    TopologyVersionRequest, UpdateTableRequest,
};
use crate::rebalance::RaftPersistedSchedulerState;
use crate::types::{Command, CommandResponse, ExecuteRequest, ShardId, Status};

mod matrixraft;
mod membership;
mod readiness;
mod cluster_snapshot;
mod production_runtime;
mod local_wal;
pub(crate) mod follower_pipeline;
pub(crate) mod wal_proto;
mod cluster_meta;
mod cluster_meta_inner;
mod cluster_inner;
mod cluster_inner_admin_report;
mod cluster_election;
mod cluster_membership;
mod cluster_replication;
mod cluster_read_status;
use bytes::Bytes;
pub use matrixraft::*;
pub use membership::*;
pub use readiness::*;
pub use production_runtime::*;
use temporalstore_snapshot::{ObjectStore, S3SnapshotStore, SnapshotRef, SnapshotStore};

pub type RaftNodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftReplicaRole {
    Voter,
    Learner,
    Witness,
}

impl RaftReplicaRole {
    fn participates_in_quorum(self) -> bool {
        matches!(self, Self::Voter | Self::Witness)
    }

    fn can_serve_data(self) -> bool {
        matches!(self, Self::Voter | Self::Learner)
    }

    fn can_be_leader(self) -> bool {
        matches!(self, Self::Voter)
    }
}

impl Default for RaftReplicaRole {
    fn default() -> Self {
        Self::Voter
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub shard_id: ShardId,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RaftSnapshot {
    pub shard_id: ShardId,
    pub last_included_term: u64,
    pub last_included_index: u64,
    #[serde(default)]
    pub external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    pub entries: Vec<RaftLogEntry>,
    /// S2: opaque engine state image captured at `last_included_index`. When present, install
    /// reconstructs state from this image (O(state)) instead of replaying `entries`, and `entries`
    /// is empty. Absent (default) on the classic entry-carrying snapshot, so older peers and the
    /// gate-off path deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_image: Option<RaftSnapshotStateImage>,
    /// True when the image lives in its own file beside the log rather than inside the record.
    /// A record is written per persist and the image is large; embedding it re-serialized the
    /// whole image into every record that followed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub state_image_externalized: bool,
}

/// S2: an opaque engine STATE IMAGE — the exported served index plus every referenced page slab —
/// that reconstructs a shard's applied state without replaying the committed log. Mirrors the
/// metadata-checkpoint the shared-store recovery path builds (index bytes + slab bytes +
/// next-page-id), letting a snapshot install run in O(state) rather than O(total history).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotStateImage {
    pub index_bytes: Vec<u8>,
    pub next_page_id: u64,
    pub slabs: Vec<RaftSnapshotStateImageSlab>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotStateImageSlab {
    pub page_slab_id: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotTriggerReport {
    pub triggered: bool,
    pub reason: String,
    pub leader_id: RaftNodeId,
    pub applied_index: u64,
    pub last_snapshot_index: u64,
    pub applied_log_bytes: u64,
    pub max_applied_log_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftApplySnapshotFence {
    pub applied_index: u64,
    pub commit_index: u64,
    pub installed_snapshot_index: u64,
    pub first_retained_log_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftStorageApplyFence {
    pub shard_id: ShardId,
    pub raft_term: u64,
    pub committed_index: u64,
    pub applied_index: u64,
    pub snapshot_id: Option<String>,
    pub storage_epoch: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftDataNodeAtomicDurabilityReport {
    pub node_id: RaftNodeId,
    pub shard_id: ShardId,
    pub commit_index: u64,
    pub applied_index: u64,
    pub wal_commit_index: u64,
    pub fence_committed_index: u64,
    pub fence_applied_index: u64,
    pub storage_epoch: u64,
    pub snapshot_id: Option<String>,
    pub storage_apply_fence_valid: bool,
    pub storage_mutation_atomic_commit_present: bool,
    pub snapshot_install_atomic_commit_present: bool,
    pub ready: bool,
    pub blockers: Vec<String>,
}

pub const DATA_RAFT_LOG_MAGIC: u32 = 0x5453_5246; // "TSRF"
pub const DATA_RAFT_COMMAND_MAGIC: u32 = 0x5453_5243; // "TSRC"
pub const DATA_RAFT_CODEC_VERSION: u32 = 1;

const DATA_RAFT_LOG_HEADER_LEN: usize = 56;
const DATA_RAFT_COMMAND_HEADER_LEN: usize = 40;

/// Read a boolean gate env var (`1`/`true`/`yes`/`on`), default OFF.
///
/// The replication-SAFETY properties (quorum election, durable pre-vote persistence, applied-read
/// freshness, §7 snapshot-boundary truncation) are no longer gated -- they are what makes this
/// Raft, not options, and shipping them dark meant the default build was the only configuration
/// nobody tested. What remains behind this helper is genuinely optional: an optimization whose
/// cost profile an operator may not want, or a hardening that is not yet complete enough to be
/// the default. Each surviving gate documents which of the two it is.
fn raft_env_flag_on(name: &str) -> bool {
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
fn raft_env_flag_default_on(name: &str) -> bool {
    !matches!(
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "no" | "off"
    )
}

/// R4: after an election, withhold read-index/lease reads until the new leader has committed a
/// no-op entry in its own term (leader-ready barrier).
///
/// STILL GATED, deliberately. Raft releases this barrier by having a new leader append a no-op
/// entry at its own term the moment it is elected (§8). This implementation never appends one:
/// `leader_has_committed_current_term` can only be satisfied by a CLIENT write landing at the new
/// term. Enabling the gate unconditionally would therefore reject every leader-served
/// read-index/lease read from the end of an election until the next write commits -- indefinitely
/// on an idle cluster. Promoting this gate requires appending the election no-op first, which is a
/// new `Command` variant and thus a wire-format change for `#[serde(tag = "kind")]` peers.
fn raft_leader_ready_barrier_on() -> bool {
    raft_env_flag_on("TS_RAFT_LEADER_READY_BARRIER")
}

/// S2: snapshot the engine STATE IMAGE (exported index + page slabs) instead of every committed
/// log entry. When on, `create_snapshot` captures an opaque image at the leader's applied index
/// and drops the replayable entries, so a far-behind follower installs in O(state) rather than
/// replaying O(total history); install reconstructs state from the image. Default ON; set the
/// variable to 0 and the snapshot still carries entries and behavior is byte-identical.
fn raft_snapshot_state_image_on() -> bool {
    // Default ON since the restore path learned to install images and compaction proved to
    // bound the log with them (a restart after compaction serves every value; the log stays a
    // fraction of the history). TS_RAFT_SNAPSHOT_STATE_IMAGE=0 opts back to entry-carrying
    // snapshots, which re-encode history rather than reduce it.
    raft_env_flag_default_on("TS_RAFT_SNAPSHOT_STATE_IMAGE")
}

/// P1 (fsync coalescing): skip a node's WAL fdatasync when none of its DURABILITY-relevant state
/// changed since the last persist. Driven purely by whether hard_state / log / membership /
/// snapshot / fences changed, so it can never skip a persist that Raft safety requires -- only the
/// volatile `pipeline_state` + `read_safety_state` (match/next index, inflight/queue depths,
/// read-index accounting counters) are excluded from the change check. Default ON; set the
/// variable to 0 and every call fsyncs exactly as before (byte-identical).
fn raft_wal_coalesce_on() -> bool {
    raft_env_flag_default_on("TS_RAFT_WAL_COALESCE")
}

/// P2 (in-order propose): hold a per-cluster serialize lock across the append+replicate+commit
/// critical section of `propose_distributed_one` so concurrent proposals reach followers in log
/// order and never trigger a `prev_log` mismatch + full-deadline stall. Default ON; set the
/// variable to 0 to propose without the serialize lock.
fn raft_propose_serialize_on() -> bool {
    raft_env_flag_default_on("TS_RAFT_PROPOSE_SERIALIZE")
}

/// Fingerprint of the DURABILITY-relevant subset of a WAL record. `pipeline_state` and
/// `read_safety_state` are cleared before hashing because they are volatile (reinitialised on
/// election / re-driven on restart) and pure metrics respectively -- excluding them is what lets
/// the read-index/tick/match-index storm coalesce to zero fsyncs while any change to term,
/// voted_for, commit_index, the log, membership, snapshots, or the apply/storage fences still
/// flips the fingerprint and forces a durable persist.
fn raft_durable_fingerprint(record: &RaftWalRecord) -> u64 {
    // Everything Raft must persist participates, but the log is SUMMARISED rather than
    // serialised. Hashing every entry made this check -- which runs on every persist, for
    // every node -- cost O(log length), the very cost incremental WAL records exist to
    // remove. A raft log only ever grows at the tail or has a suffix replaced, so its
    // length together with the first and last (index, term) changes whenever it does.
    let reduced = RaftWalRecord {
        hard_state: record.hard_state.clone(),
        membership: record.membership.clone(),
        replica_role: record.replica_role,
        joint_membership: record.joint_membership.clone(),
        latest_external_snapshot_ref: record.latest_external_snapshot_ref.clone(),
        installed_snapshot: record.installed_snapshot.clone(),
        apply_snapshot_fence: record.apply_snapshot_fence.clone(),
        storage_apply_fence: record.storage_apply_fence.clone(),
        pipeline_state: RaftPeerPipelineRuntimeState::default(),
        read_safety_state: RaftReadSafetyRuntimeState::default(),
        membership_evidence: record.membership_evidence.clone(),
        entries: Vec::new(),
    };
    let bytes = serde_json::to_vec(&reduced).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hasher::write(&mut hasher, &bytes);
    std::hash::Hasher::write_u64(&mut hasher, record.entries.len() as u64);
    for entry in [record.entries.first(), record.entries.last()].into_iter().flatten() {
        std::hash::Hasher::write_u64(&mut hasher, entry.index);
        std::hash::Hasher::write_u64(&mut hasher, entry.term);
    }
    std::hash::Hasher::finish(&hasher)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataRaftLogCodecEntry {
    pub shard_id: ShardId,
    pub raft_index: u64,
    pub log_id: u64,
    pub log_size: u64,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataRaftCommandCodecEntry {
    pub shard_id: ShardId,
    pub raft_index: u64,
    pub request_id: u64,
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftPeer {
    pub replica_id: RaftNodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    #[serde(default)]
    pub auto_promote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftConsensusOptions {
    pub shard_id: ShardId,
    pub replica_id: RaftNodeId,
    pub group_id: u64,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub wal_dir: Option<PathBuf>,
    pub snapshot_dir: Option<PathBuf>,
    pub wal_sync: bool,
    pub bootstrap_as_learner: bool,
    pub peers: Vec<DataRaftPeer>,
    pub initial_applied_index: u64,
}

impl Default for DataRaftConsensusOptions {
    fn default() -> Self {
        Self {
            shard_id: 0,
            replica_id: 0,
            group_id: 0,
            raft_addr: String::new(),
            snapshot_addr: String::new(),
            wal_dir: None,
            snapshot_dir: None,
            wal_sync: true,
            bootstrap_as_learner: false,
            peers: Vec::new(),
            initial_applied_index: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftStatus {
    pub running: bool,
    pub leader: bool,
    pub learner: bool,
    pub term: u64,
    pub leader_replica_id: RaftNodeId,
    pub committed_index: u64,
    pub applied_index: u64,
    pub first_index: u64,
    pub last_index: u64,
    pub pending_config_change_index: u64,
    pub voter_count: u64,
    pub learner_count: u64,
    pub fatal_event_count: u64,
    pub snapshot_creating: bool,
    pub snapshot_loading: bool,
}

pub trait DataRaftConsensusBackend {
    fn start(&mut self) -> Result<(), RaftError>;
    fn stop(&mut self);
    fn is_leader(&self) -> bool;
    fn status(&self) -> Result<DataRaftStatus, RaftError>;
    fn propose(&mut self, serialized_entry: Vec<u8>) -> Result<u64, RaftError>;
    fn wait_for_applied_index(&self, index: u64, timeout_ms: u64) -> Result<(), RaftError>;
    fn trigger_snapshot(&mut self) -> Result<u64, RaftError>;
    fn read_index(&self, timeout_ms: u64) -> Result<(), RaftError>;
    fn add_peer(&mut self, peer: DataRaftPeer) -> Result<(), RaftError>;
    fn add_learner(&mut self, peer: DataRaftPeer) -> Result<(), RaftError>;
    fn promote_peer(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError>;
    fn remove_peer(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError>;
    fn transfer_leader(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError>;
    fn campaign(&mut self, timeout_ms: u64, force: bool) -> Result<(), RaftError>;
    fn can_serve_bounded_stale_read(&self, max_stale_index_lag: u64) -> Result<(), RaftError>;
}

#[derive(Debug, Clone)]
pub struct UnavailableDataRaftConsensusBackend {
    options: DataRaftConsensusOptions,
}

impl UnavailableDataRaftConsensusBackend {
    pub fn new(options: DataRaftConsensusOptions) -> Self {
        Self { options }
    }

    fn unavailable(operation: &'static str) -> RaftError {
        RaftError::Transport(format!("data raft backend unavailable: {operation}"))
    }
}

impl DataRaftConsensusBackend for UnavailableDataRaftConsensusBackend {
    fn start(&mut self) -> Result<(), RaftError> {
        Err(Self::unavailable("start"))
    }

    fn stop(&mut self) {}

    fn is_leader(&self) -> bool {
        false
    }

    fn status(&self) -> Result<DataRaftStatus, RaftError> {
        Ok(DataRaftStatus {
            running: false,
            leader: false,
            learner: false,
            term: 0,
            leader_replica_id: 0,
            committed_index: 0,
            applied_index: self.options.initial_applied_index,
            first_index: 0,
            last_index: 0,
            pending_config_change_index: 0,
            voter_count: self.options.peers.len() as u64,
            learner_count: 0,
            fatal_event_count: 0,
            snapshot_creating: false,
            snapshot_loading: false,
        })
    }

    fn propose(&mut self, _serialized_entry: Vec<u8>) -> Result<u64, RaftError> {
        Err(Self::unavailable("propose"))
    }

    fn wait_for_applied_index(&self, _index: u64, _timeout_ms: u64) -> Result<(), RaftError> {
        Err(Self::unavailable("wait_for_applied_index"))
    }

    fn trigger_snapshot(&mut self) -> Result<u64, RaftError> {
        Err(Self::unavailable("trigger_snapshot"))
    }

    fn read_index(&self, _timeout_ms: u64) -> Result<(), RaftError> {
        Err(Self::unavailable("read_index"))
    }

    fn add_peer(&mut self, _peer: DataRaftPeer) -> Result<(), RaftError> {
        Err(Self::unavailable("add_peer"))
    }

    fn add_learner(&mut self, _peer: DataRaftPeer) -> Result<(), RaftError> {
        Err(Self::unavailable("add_learner"))
    }

    fn promote_peer(&mut self, _replica_id: RaftNodeId) -> Result<(), RaftError> {
        Err(Self::unavailable("promote_peer"))
    }

    fn remove_peer(&mut self, _replica_id: RaftNodeId) -> Result<(), RaftError> {
        Err(Self::unavailable("remove_peer"))
    }

    fn transfer_leader(&mut self, _replica_id: RaftNodeId) -> Result<(), RaftError> {
        Err(Self::unavailable("transfer_leader"))
    }

    fn campaign(&mut self, _timeout_ms: u64, _force: bool) -> Result<(), RaftError> {
        Err(Self::unavailable("campaign"))
    }

    fn can_serve_bounded_stale_read(&self, _max_stale_index_lag: u64) -> Result<(), RaftError> {
        Err(Self::unavailable("can_serve_bounded_stale_read"))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataRaftCommittedLogApplier {
    shard_id: ShardId,
    applied_raft_index: u64,
    applied_wal_sequence: u64,
}

impl DataRaftCommittedLogApplier {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            applied_raft_index: 0,
            applied_wal_sequence: 0,
        }
    }

    pub fn applied_raft_index(&self) -> u64 {
        self.applied_raft_index
    }

    pub fn applied_wal_sequence(&self) -> u64 {
        self.applied_wal_sequence
    }

    pub fn apply(
        &mut self,
        raft_index: u64,
        committed_log: &[u8],
        engine: &TemporalEngine,
    ) -> Result<Option<CommandResponse>, RaftError> {
        if raft_index <= self.applied_raft_index {
            return Ok(None);
        }
        let entry = parse_data_raft_log(committed_log)?;
        if entry.shard_id != self.shard_id {
            return Err(RaftError::InvalidDataRaftLog(format!(
                "shard mismatch: log={}, applier={}",
                entry.shard_id, self.shard_id
            )));
        }
        if entry.raft_index != raft_index {
            return Err(RaftError::InvalidDataRaftLog(format!(
                "raft index mismatch: header={}, apply={}",
                entry.raft_index, raft_index
            )));
        }
        let response = engine.execute_raft_apply(ExecuteRequest {
            shard_id: entry.shard_id,
            command: entry.command,
        });
        if !response.status.ok {
            return Err(RaftError::InvalidDataRaftLog(format!(
                "engine apply failed: {} {}",
                response.status.code, response.status.message
            )));
        }
        self.applied_raft_index = entry.raft_index;
        self.applied_wal_sequence = entry.wal_sequence;
        Ok(Some(response.response))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftExternalSnapshotRef {
    pub uri: String,
    pub checksum: String,
    pub byte_size: u64,
}

pub const DEFAULT_EXTERNAL_SNAPSHOT_THRESHOLD_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftSnapshotTransferMode {
    PeerStreaming,
    ExternalStore,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotTransferPolicy {
    pub external_threshold_bytes: u64,
    pub allow_peer_streaming: bool,
    pub allow_external_store: bool,
}

impl Default for RaftSnapshotTransferPolicy {
    fn default() -> Self {
        Self {
            external_threshold_bytes: DEFAULT_EXTERNAL_SNAPSHOT_THRESHOLD_BYTES,
            allow_peer_streaming: true,
            allow_external_store: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotTransferDecision {
    pub mode: RaftSnapshotTransferMode,
    pub snapshot_bytes: u64,
    pub threshold_bytes: u64,
    pub external_snapshot_ref: Option<RaftExternalSnapshotRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReplicaBootstrapPlan {
    pub shard_id: ShardId,
    pub target_id: RaftNodeId,
    pub transfer: RaftSnapshotTransferDecision,
    pub last_included_index: u64,
    pub catch_up_from_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotPublishReport {
    pub shard_id: ShardId,
    pub last_log_index: u64,
    pub raft_ref: RaftExternalSnapshotRef,
    pub meta_ref: ShardSnapshotRef,
}

/// Four times the compaction threshold: enough that an ordinary lagging follower is waited for,
/// while a peer that has stopped answering cannot hold the log open indefinitely.
fn default_max_retained_log_bytes() -> u64 {
    4 * 1024 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNodeStatus {
    pub node_id: RaftNodeId,
    pub role: RaftRole,
    pub replica_role: RaftReplicaRole,
    pub current_term: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub applied_index: u64,
    pub alive: bool,
    pub lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftClusterStatus {
    pub leader_id: RaftNodeId,
    pub current_term: u64,
    pub commit_index: u64,
    pub majority: usize,
    pub live_voters: usize,
    pub has_majority: bool,
    pub leader_lease_valid: bool,
    pub nodes: Vec<RaftNodeStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftPeerPipelineState {
    pub peer_id: RaftNodeId,
    pub role: RaftRole,
    pub replica_role: RaftReplicaRole,
    pub match_index: u64,
    pub next_index: u64,
    pub append_requests: u64,
    pub append_accepted: u64,
    pub append_rejected: u64,
    pub inflight_entries: u64,
    pub inflight_bytes: u64,
    pub append_queue_depth: u64,
    #[serde(default)]
    pub append_queue_limit: u64,
    #[serde(default)]
    pub inflight_bytes_limit: u64,
    #[serde(default)]
    pub apply_inflight_tasks: u64,
    #[serde(default)]
    pub apply_inflight_limit: u64,
    #[serde(default)]
    pub apply_queue_depth: u64,
    #[serde(default)]
    pub apply_queue_max_depth: u64,
    #[serde(default)]
    pub apply_batch_bytes_limit: u64,
    #[serde(default)]
    pub apply_backpressure_rejections: u64,
    #[serde(default)]
    pub memory_backpressure_rejections: u64,
    #[serde(default)]
    pub oversized_log_rejections: u64,
    #[serde(default)]
    pub append_queue_max_depth: u64,
    pub reorder_queue_depth: u64,
    #[serde(default)]
    pub out_of_order_append_rejections: u64,
    pub reorder_entries_accepted: u64,
    pub reorder_entries_released: u64,
    pub reorder_entries_rejected: u64,
    #[serde(default)]
    pub reorder_entry_timeouts: u64,
    #[serde(default)]
    pub reorder_dropped_packages: u64,
    #[serde(default)]
    pub stale_term_rejections: u64,
    /// Sends to this peer that failed outright, producing no response at all. Each one released a
    /// reservation that would otherwise have been held forever.
    #[serde(default)]
    pub append_send_failures: u64,
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: u64,
    pub snapshot_send_attempts: u64,
    pub snapshot_send_completed: u64,
    pub snapshot_send_failed: u64,
    pub snapshot_install_started: u64,
    pub snapshot_install_completed: u64,
    pub snapshot_install_rejected: u64,
    pub snapshot_install_rolled_back: u64,
    pub snapshot_install_received_chunks: u64,
    pub snapshot_install_total_chunks: u64,
    #[serde(default)]
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_retry_count: u64,
    #[serde(default)]
    pub snapshot_chunk_retry_count: u64,
    pub snapshot_backpressure_rejections: u64,
    #[serde(default)]
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_send_elapsed_ms: u64,
    pub snapshot_send_timeouts: u64,
    #[serde(default)]
    pub snapshot_during_membership_change: bool,
    #[serde(default)]
    pub snapshot_rejoin_after_compacted_log: bool,
    pub transfer_leader_target: bool,
    pub transfer_leader_requests: u64,
    pub transfer_leader_accepted: u64,
    pub transfer_leader_rejected: u64,
    pub transfer_leader_completed: u64,
    pub transfer_leader_elapsed_ms: u64,
    pub transfer_leader_timeouts: u64,
    pub pre_vote_rejections: u64,
    pub election_rejections: u64,
    pub offline_elapsed_ms: u64,
    pub offline_timeout_reached: bool,
    pub offline_timeout_rejections: u64,
    #[serde(default)]
    pub auto_promoted_from_learner: bool,
}

impl MatrixRaftPeerPipelineState {
    fn to_matrixraft_peer_pipeline_status(&self) -> MatrixRaftPeerPipelineStatus {
        matrixraft_peer_pipeline_status_from_observed(&MatrixRaftObservedPeerPipeline {
            peer_id: self.peer_id,
            match_index: self.match_index,
            next_index: self.next_index,
            append_requests: self.append_requests,
            append_accepted: self.append_accepted,
            append_rejected: self.append_rejected,
            inflight_entries: self.inflight_entries,
            inflight_bytes: self.inflight_bytes,
            append_queue_depth: self.append_queue_depth,
            append_queue_limit: self.append_queue_limit,
            append_queue_max_depth: self.append_queue_max_depth,
            inflight_bytes_limit: self.inflight_bytes_limit,
            apply_inflight_tasks: self.apply_inflight_tasks,
            apply_inflight_limit: self.apply_inflight_limit,
            apply_queue_depth: self.apply_queue_depth,
            apply_queue_max_depth: self.apply_queue_max_depth,
            apply_batch_bytes_limit: self.apply_batch_bytes_limit,
            apply_backpressure_rejections: self.apply_backpressure_rejections,
            memory_backpressure_rejections: self.memory_backpressure_rejections,
            oversized_log_rejections: self.oversized_log_rejections,
            reorder_queue_depth: self.reorder_queue_depth,
            out_of_order_append_rejections: self.out_of_order_append_rejections,
            reorder_entries_rejected: self.reorder_entries_rejected,
            reorder_entry_timeouts: self.reorder_entry_timeouts,
            reorder_dropped_packages: self.reorder_dropped_packages,
            stale_term_rejections: self.stale_term_rejections,
            packet_loss_events: 0,
            network_error_probe_transitions: 0,
            snapshot_sending: self.snapshot_sending,
            snapshot_installing: self.snapshot_installing,
            snapshot_installed_index: self.snapshot_installed_index,
            snapshot_send_attempts: self.snapshot_send_attempts,
            snapshot_install_total_chunks: self.snapshot_install_total_chunks,
            snapshot_install_progress_per_mille: self.snapshot_install_progress_per_mille,
            snapshot_backpressure_rejections: self.snapshot_backpressure_rejections,
            snapshot_rate_limit_rejections: self.snapshot_rate_limit_rejections,
            snapshot_install_rolled_back: self.snapshot_install_rolled_back,
            snapshot_chunk_retry_count: self.snapshot_chunk_retry_count,
            snapshot_send_timeouts: self.snapshot_send_timeouts,
            snapshot_during_membership_change: self.snapshot_during_membership_change,
            snapshot_rejoin_after_compacted_log: self.snapshot_rejoin_after_compacted_log,
            transfer_leader_target: self.transfer_leader_target,
            transfer_leader_timeouts: self.transfer_leader_timeouts,
            pre_vote_rejections: self.pre_vote_rejections,
            election_rejections: self.election_rejections,
            offline_timeout_reached: self.offline_timeout_reached,
            offline_timeout_rejections: self.offline_timeout_rejections,
            auto_promoted_from_learner: self.auto_promoted_from_learner,
            witness_quorum_required: 0,
            witness_quorum_acked: 0,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCapabilityEvidence {
    pub capability: String,
    pub ready: bool,
    pub evidence_field: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftRuntimeAdminReport {
    pub shard_id: ShardId,
    pub leader_id: RaftNodeId,
    pub commit_index: u64,
    pub leader_lease_valid: bool,
    pub read_index_validated: bool,
    pub lease_read_validated: bool,
    pub stale_follower_read_rejected: bool,
    pub stale_follower_write_rejected: bool,
    #[serde(default)]
    pub stale_leader_lease_rejected: bool,
    #[serde(default)]
    pub lagging_follower_read_rejected: bool,
    #[serde(default)]
    pub bounded_stale_read_accepted: bool,
    #[serde(default)]
    pub bounded_stale_read_rejected: bool,
    #[serde(default)]
    pub minority_partition_rejected_reads: bool,
    #[serde(default)]
    pub minority_partition_rejected_writes: bool,
    #[serde(default)]
    pub healed_follower_caught_up: bool,
    #[serde(default)]
    pub witness_membership_present: bool,
    #[serde(default)]
    pub witness_role_behavior_present: bool,
    #[serde(default)]
    pub learner_auto_promote_present: bool,
    #[serde(default)]
    pub pending_joint_consensus_present: bool,
    #[serde(default)]
    pub learner_add_present: bool,
    #[serde(default)]
    pub learner_catchup_present: bool,
    #[serde(default)]
    pub learner_promote_present: bool,
    #[serde(default)]
    pub voter_remove_present: bool,
    #[serde(default)]
    pub leader_transfer_exact_once_present: bool,
    #[serde(default)]
    pub pending_joint_consensus_restart_present: bool,
    #[serde(default)]
    pub membership_evidence: RaftMembershipRuntimeEvidence,
    pub peer_pipeline_states: Vec<MatrixRaftPeerPipelineState>,
    pub append_backpressure_enforced: bool,
    #[serde(default)]
    pub apply_backpressure_enforced: bool,
    #[serde(default)]
    pub memory_replicate_bytes_enforced: bool,
    #[serde(default)]
    pub oversized_log_rejection_present: bool,
    #[serde(default)]
    pub out_of_order_append_handling_present: bool,
    #[serde(default)]
    pub reorder_timeout_drop_present: bool,
    #[serde(default)]
    pub stale_term_rejection_present: bool,
    pub reorder_queue_enabled: bool,
    pub snapshot_sender_lifecycle_present: bool,
    pub snapshot_downloader_lifecycle_present: bool,
    pub snapshot_retry_backpressure_present: bool,
    #[serde(default)]
    pub snapshot_chunk_retry_present: bool,
    #[serde(default)]
    pub snapshot_send_timeout_present: bool,
    #[serde(default)]
    pub snapshot_rate_limit_present: bool,
    #[serde(default)]
    pub snapshot_install_progress_present: bool,
    #[serde(default)]
    pub snapshot_install_rollback_present: bool,
    #[serde(default)]
    pub snapshot_membership_change_present: bool,
    #[serde(default)]
    pub snapshot_rejoin_after_compacted_log_present: bool,
    pub wal_segment_lifecycle_present: bool,
    pub wal_segment_count: u64,
    pub wal_active_segment_id: u64,
    pub wal_first_retained_segment_id: u64,
    pub wal_last_retained_segment_id: u64,
    pub wal_total_bytes: u64,
    pub wal_active_segment_bytes: u64,
    pub wal_total_records: u64,
    pub wal_first_sequence: u64,
    pub wal_last_sequence: u64,
    #[serde(default)]
    pub wal_first_log_index: u64,
    #[serde(default)]
    pub wal_last_log_index: u64,
    #[serde(default)]
    pub wal_released_segment_count: u64,
    #[serde(default)]
    pub wal_slow_fsync_backpressure_observed: bool,
    pub pre_vote_enforced: bool,
    pub election_controls_enforced: bool,
    #[serde(default)]
    pub pre_vote_process_evidence_observed: bool,
    #[serde(default)]
    pub election_prohibition_observed: bool,
    #[serde(default)]
    pub offline_timeout_observed: bool,
    #[serde(default)]
    pub transfer_timeout_observed: bool,
    pub read_index_requests: u64,
    pub read_index_accepted: u64,
    pub read_index_rejected: u64,
    pub lease_read_requests: u64,
    pub lease_read_accepted: u64,
    pub lease_read_rejected: u64,
    #[serde(default)]
    pub stale_leader_lease_rejection_count: u64,
    #[serde(default)]
    pub lagging_follower_read_rejection_count: u64,
    #[serde(default)]
    pub bounded_stale_read_requests: u64,
    #[serde(default)]
    pub bounded_stale_read_accepted_count: u64,
    #[serde(default)]
    pub bounded_stale_read_rejected_count: u64,
    #[serde(default)]
    pub minority_partition_read_rejection_count: u64,
    #[serde(default)]
    pub minority_partition_write_rejection_count: u64,
    #[serde(default)]
    pub stale_follower_write_rejection_count: u64,
    #[serde(default)]
    pub healed_follower_catchup_count: u64,
    pub pre_vote_requests: u64,
    pub pre_vote_accepted: u64,
    pub pre_vote_rejected: u64,
    #[serde(default)]
    pub quorum_peer_progress_observed: bool,
    #[serde(default)]
    pub peer_pipeline_runtime_activity_observed: bool,
    #[serde(default)]
    pub peer_pipeline_limits_observed: bool,
    pub admin_status_surface_complete: bool,
    #[serde(default)]
    pub capability_matrix: Vec<MatrixRaftCapabilityEvidence>,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLeaderElectionParityReport {
    pub shard_id: ShardId,
    pub leader_id: RaftNodeId,
    pub current_term: u64,
    pub commit_index: u64,
    pub leader_election_ready: bool,
    pub pre_vote_ready: bool,
    pub leader_failover_observed: bool,
    pub learner_add_ready: bool,
    pub learner_catchup_ready: bool,
    pub learner_promote_ready: bool,
    pub learner_auto_promote_ready: bool,
    pub membership_ready: bool,
    pub leader_transfer_exact_once_ready: bool,
    pub pre_vote_requests: u64,
    pub pre_vote_accepted: u64,
    pub pre_vote_rejected: u64,
    pub learner_add_count: u64,
    pub learner_catchup_count: u64,
    pub learner_promote_count: u64,
    pub auto_promote_count: u64,
    pub leader_transfer_exact_once_commit_count: u64,
    pub evidence_fields: Vec<String>,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLocalPeerStatus {
    pub status: RaftNodeStatus,
    pub pipeline_state: MatrixRaftPeerPipelineState,
    pub participates_in_quorum: bool,
    pub can_serve_data: bool,
    pub can_be_leader: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftLocalStatusReport {
    pub shard_id: ShardId,
    pub leader_id: RaftNodeId,
    pub current_term: u64,
    pub commit_index: u64,
    #[serde(default)]
    pub wal_first_log_index: u64,
    #[serde(default)]
    pub wal_last_log_index: u64,
    pub has_majority: bool,
    pub leader_lease_valid: bool,
    pub pending_joint_consensus: Option<JointConsensusMembership>,
    pub witness_membership_present: bool,
    pub learner_membership_present: bool,
    pub learner_auto_promote_present: bool,
    pub peers: Vec<MatrixRaftLocalPeerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReplicaLag {
    pub node_id: RaftNodeId,
    pub lag: u64,
    pub alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftApplyLag {
    pub node_id: RaftNodeId,
    pub commit_index: u64,
    pub applied_index: u64,
    pub apply_lag: u64,
    pub alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftApplyHealth {
    pub leader_id: RaftNodeId,
    pub leader_commit_index: u64,
    pub max_allowed_apply_lag: u64,
    pub max_apply_lag: u64,
    pub fully_applied_nodes: Vec<RaftNodeId>,
    pub slow_appliers: Vec<RaftApplyLag>,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReplicationHealth {
    pub leader_id: RaftNodeId,
    pub leader_commit_index: u64,
    pub max_allowed_lag: u64,
    pub max_lag: u64,
    pub caught_up_voters: Vec<RaftNodeId>,
    pub lagging_voters: Vec<RaftReplicaLag>,
    pub live_voters: usize,
    pub majority: usize,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftHardState {
    pub current_term: u64,
    pub voted_for: Option<RaftNodeId>,
    pub commit_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembership {
    pub shard_id: ShardId,
    pub voters: Vec<RaftNodeId>,
    pub leader_id: RaftNodeId,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipRuntimeEvidence {
    #[serde(default)]
    pub learner_add_count: u64,
    #[serde(default)]
    pub learner_catchup_count: u64,
    #[serde(default)]
    pub learner_promote_count: u64,
    #[serde(default)]
    pub voter_remove_count: u64,
    #[serde(default)]
    pub witness_add_count: u64,
    #[serde(default)]
    pub auto_promote_count: u64,
    #[serde(default)]
    pub leader_transfer_write_count: u64,
    #[serde(default)]
    pub leader_transfer_exact_once_commit_count: u64,
    #[serde(default)]
    pub leader_transfer_exact_once_commit_ids: Vec<u64>,
    #[serde(default)]
    pub pending_joint_consensus_persist_count: u64,
    #[serde(default)]
    pub pending_joint_consensus_restore_count: u64,
}

impl RaftReplicaRole {
    /// Whether this is the value an absent field decodes to, so the encoder can leave it out.
    fn is_default(&self) -> bool {
        *self == RaftReplicaRole::default()
    }
}

impl RaftApplySnapshotFence {
    /// Whether this is the value an absent field decodes to, so the encoder can leave it out.
    fn is_default(&self) -> bool {
        *self == RaftApplySnapshotFence::default()
    }
}

impl RaftMembershipRuntimeEvidence {
    /// Whether this is the value an absent field decodes to, so the encoder can leave it out.
    /// This block is a dozen counters that are zero on almost every record, and spelling them
    /// out cost more than the entry the record exists to carry.
    fn is_default(&self) -> bool {
        *self == RaftMembershipRuntimeEvidence::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RaftWalRecord {
    pub hard_state: RaftHardState,
    pub membership: RaftMembership,
    #[serde(default, skip_serializing_if = "RaftReplicaRole::is_default")]
    pub replica_role: RaftReplicaRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub joint_membership: Option<JointConsensusMembership>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installed_snapshot: Option<RaftSnapshot>,
    #[serde(default, skip_serializing_if = "RaftApplySnapshotFence::is_default")]
    pub apply_snapshot_fence: RaftApplySnapshotFence,
    #[serde(default)]
    pub storage_apply_fence: RaftStorageApplyFence,
    // Volatile runtime telemetry: excluded from `raft_durable_fingerprint` because it is
    // not durability-relevant, and omitted from the encoding when it is at its default so
    // an incremental record does not spend 2.4 KB restating counters.
    #[serde(default, skip_serializing_if = "RaftPeerPipelineRuntimeState::is_default")]
    pub pipeline_state: RaftPeerPipelineRuntimeState,
    #[serde(default, skip_serializing_if = "RaftReadSafetyRuntimeState::is_default")]
    pub read_safety_state: RaftReadSafetyRuntimeState,
    #[serde(default, skip_serializing_if = "RaftMembershipRuntimeEvidence::is_default")]
    pub membership_evidence: RaftMembershipRuntimeEvidence,
    pub entries: Vec<RaftLogEntry>,
}

impl RaftPeerPipelineRuntimeState {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

impl RaftReadSafetyRuntimeState {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftPeerPipelineRuntimeState {
    pub match_index: u64,
    pub next_index: u64,
    pub append_requests: u64,
    pub append_accepted: u64,
    pub append_rejected: u64,
    pub inflight_entries: u64,
    pub inflight_bytes: u64,
    pub append_queue_depth: u64,
    /// Cluster clock reading when this node last ACCEPTED an append. Not when one last
    /// arrived: a follower stuck rejecting every entry still receives them, and counting
    /// arrivals is how a stalled follower comes to look healthy.
    #[serde(default)]
    pub last_accepted_append_ms: u64,
    /// The commit index the leader last reported here. It comes from the leader itself rather
    /// than from this process's shadow of it, which is what makes it trustworthy: a shadow does
    /// not move when a peer rejects, so it cannot tell "caught up" from "stuck".
    #[serde(default)]
    pub leader_reported_commit_index: u64,
    #[serde(default)]
    pub apply_inflight_tasks: u64,
    #[serde(default)]
    pub apply_queue_depth: u64,
    #[serde(default)]
    pub apply_queue_max_depth: u64,
    #[serde(default)]
    pub apply_backpressure_rejections: u64,
    #[serde(default)]
    pub memory_backpressure_rejections: u64,
    #[serde(default)]
    pub oversized_log_rejections: u64,
    #[serde(default)]
    pub append_queue_max_depth: u64,
    pub reorder_queue_depth: u64,
    #[serde(default)]
    pub out_of_order_append_rejections: u64,
    pub reorder_entries_accepted: u64,
    pub reorder_entries_released: u64,
    pub reorder_entries_rejected: u64,
    #[serde(default)]
    pub reorder_entry_timeouts: u64,
    #[serde(default)]
    pub reorder_dropped_packages: u64,
    #[serde(default)]
    pub stale_term_rejections: u64,
    /// Sends to this peer that failed outright, producing no response. Each released a reservation
    /// that would otherwise have been held forever.
    #[serde(default)]
    pub append_send_failures: u64,
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: u64,
    pub snapshot_send_attempts: u64,
    pub snapshot_send_completed: u64,
    pub snapshot_send_failed: u64,
    pub snapshot_install_started: u64,
    pub snapshot_install_completed: u64,
    pub snapshot_install_rejected: u64,
    pub snapshot_install_rolled_back: u64,
    pub snapshot_install_received_chunks: u64,
    /// Chunk count as of the last stall check. The send counts as progressing while this
    /// keeps changing, however long the transfer takes overall.
    pub snapshot_send_progress_mark: u64,
    pub snapshot_install_total_chunks: u64,
    #[serde(default)]
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_retry_count: u64,
    #[serde(default)]
    pub snapshot_chunk_retry_count: u64,
    pub snapshot_backpressure_rejections: u64,
    #[serde(default)]
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_send_started_ms: Option<u64>,
    pub snapshot_send_elapsed_ms: u64,
    pub snapshot_send_timeouts: u64,
    #[serde(default)]
    pub snapshot_during_membership_change: bool,
    #[serde(default)]
    pub snapshot_rejoin_after_compacted_log: bool,
    pub transfer_leader_target: bool,
    pub transfer_leader_requests: u64,
    pub transfer_leader_accepted: u64,
    pub transfer_leader_rejected: u64,
    pub transfer_leader_completed: u64,
    pub transfer_leader_started_ms: Option<u64>,
    pub transfer_leader_elapsed_ms: u64,
    pub transfer_leader_timeouts: u64,
    pub pre_vote_rejections: u64,
    pub election_rejections: u64,
    pub offline_since_ms: Option<u64>,
    pub offline_elapsed_ms: u64,
    pub offline_timeout_reached: bool,
    pub offline_timeout_rejections: u64,
    #[serde(default)]
    pub auto_promoted_from_learner: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReadSafetyRuntimeState {
    pub read_index_requests: u64,
    pub read_index_accepted: u64,
    pub read_index_rejected: u64,
    pub lease_read_requests: u64,
    pub lease_read_accepted: u64,
    pub lease_read_rejected: u64,
    pub pre_vote_requests: u64,
    pub pre_vote_accepted: u64,
    pub pre_vote_rejected: u64,
    #[serde(default)]
    pub bounded_stale_read_requests: u64,
    #[serde(default)]
    pub bounded_stale_read_accepted: u64,
    #[serde(default)]
    pub bounded_stale_read_rejected: u64,
    #[serde(default)]
    pub stale_leader_lease_rejected: u64,
    #[serde(default)]
    pub lagging_follower_read_rejected: u64,
    #[serde(default)]
    pub stale_follower_write_rejected: u64,
    #[serde(default)]
    pub minority_partition_read_rejected: u64,
    #[serde(default)]
    pub minority_partition_write_rejected: u64,
    #[serde(default)]
    pub healed_follower_catchup_observed: u64,
}

impl RaftReadSafetyRuntimeState {
    fn record_matrixraft_runtime_decision(
        &mut self,
        decision: &MatrixRaftReadSafetyRuntimeDecision,
    ) {
        if decision.stale_leader_lease_rejected {
            self.stale_leader_lease_rejected = self.stale_leader_lease_rejected.saturating_add(1);
        }
        if decision.lagging_follower_read_rejected {
            self.lagging_follower_read_rejected =
                self.lagging_follower_read_rejected.saturating_add(1);
        }
        if decision.stale_follower_write_rejected {
            self.stale_follower_write_rejected =
                self.stale_follower_write_rejected.saturating_add(1);
        }
        if decision.minority_partition_read_rejected {
            self.minority_partition_read_rejected =
                self.minority_partition_read_rejected.saturating_add(1);
        }
        if decision.minority_partition_write_rejected {
            self.minority_partition_write_rejected =
                self.minority_partition_write_rejected.saturating_add(1);
        }
        if decision.healed_follower_catchup_observed {
            self.healed_follower_catchup_observed =
                self.healed_follower_catchup_observed.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RaftWalEnvelope {
    sequence: u64,
    checksum: String,
    record: RaftWalRecord,
    /// Present on an incremental record: `record.entries` then carries ONLY the log
    /// entries appended since `from_index`, and recovery folds it onto the most recent
    /// full ("base") record. Absent on a base record, so a WAL written before this
    /// field existed still reads back as a sequence of full snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delta: Option<RaftWalEntryDelta>,
}

/// Describes how to fold an incremental WAL record onto the running base.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct RaftWalEntryDelta {
    /// The record's `entries` hold only indexes strictly greater than this.
    from_index: u64,
    /// First index the node's full log holds after this record (0 when the log is empty).
    /// Anything below it was compacted away and must be dropped while folding.
    log_first_index: u64,
    /// Last index the node's full log holds after this record (0 when the log is empty).
    log_last_index: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RaftWalSegmentRuntimeState {
    #[serde(default)]
    released_segment_count: u64,
    #[serde(default)]
    last_fsync_elapsed_ms: u64,
    #[serde(default)]
    slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RaftWalRecovery {
    pub record: Option<RaftWalRecord>,
    pub valid_records: usize,
    pub truncated_bytes: u64,
    pub corrupt_tail: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalSegmentInfo {
    pub segment_id: u64,
    pub path: String,
    pub bytes: u64,
    pub record_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    #[serde(default)]
    pub first_log_index: u64,
    #[serde(default)]
    pub last_log_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalSegmentReport {
    pub active_segment_id: u64,
    pub segments: Vec<RaftWalSegmentInfo>,
    #[serde(default)]
    pub released_segment_count: u64,
    #[serde(default)]
    pub first_retained_log_index: u64,
    #[serde(default)]
    pub last_retained_log_index: u64,
    #[serde(default)]
    pub last_fsync_elapsed_ms: u64,
    #[serde(default)]
    pub slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, Default)]
struct NodeWalCursor {
    /// Snapshot boundary of the last marker this node persisted, so an incremental record can
    /// omit an unchanged marker the way it omits already-persisted entries.
    persisted_marker_index: Option<u64>,
    next_sequence: u64,
    segments: Vec<RaftWalSegmentInfo>,
    released_segment_count: u64,
    last_fsync_elapsed_ms: u64,
    slow_fsync_backpressure_observed: bool,
    /// Encoded size of the full record that opened the active segment. Rotation waits
    /// until the segment is a multiple of it, so the one O(log) base write per segment
    /// stays a bounded fraction of the bytes that segment costs.
    base_bytes: u64,
    /// Last log index already written into the active segment, and the term at that
    /// index. An incremental record is only sound while the in-memory log still agrees
    /// with both; a conflict overwrite or a snapshot compaction re-bases instead.
    persisted_last_index: u64,
    persisted_last_term: u64,
    /// False until a full base record has been written into the active segment.
    has_base: bool,
}

/// A segment is never rotated below the size of the full record that opens it: a
/// segment that cannot hold even one base record would re-base on every append, which
/// is the O(log length) cost incremental records exist to remove. Above that floor the
/// configured `max_segment_bytes` decides rotation, so retention keeps its meaning and
/// the base cost is amortised over `max_segment_bytes / delta_size` appends.

/// How far behind a node must be, with nothing accepted, before it says so.
const RAFT_STALL_WARN_MS: u64 = 30_000;
/// How often it may repeat that. A node that is stuck stays stuck, and one line per tick would
/// bury everything else in the log.
const RAFT_STALL_REPORT_INTERVAL_MS: u64 = 60_000;
/// Consecutive failed AppendEntries before a leader marks a peer down.
const RAFT_PEER_FAILURE_THRESHOLD: u32 = 3;
/// Ticks of leader silence a follower tolerates before standing for election.
const RAFT_ELECTION_TIMEOUT_BASE_TICKS: u64 = 8;
/// Width of the random spread added to the election timeout.
const RAFT_ELECTION_TIMEOUT_SPREAD_TICKS: u64 = 12;

/// Draw a fresh election timeout. It must be re-drawn on every attempt: a timeout that is
/// merely a fixed function of the node id puts two survivors a constant distance apart, so
/// each one's campaign supersedes the other's brand-new leadership before its first
/// heartbeat can land, and they trade the term back and forth indefinitely.
pub(crate) fn randomized_election_timeout_ticks(node_id: RaftNodeId) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(node_id);
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default(),
    );
    RAFT_ELECTION_TIMEOUT_BASE_TICKS + (hasher.finish() % RAFT_ELECTION_TIMEOUT_SPREAD_TICKS)
}

/// `TS_RAFT_WAL_DELTA_ENTRIES=0` restores the legacy full-log-per-record WAL payload.
/// Note that a WAL containing incremental records cannot be read by a build that
/// predates them, so roll the binary forward before flipping this back on.
fn raft_wal_delta_entries_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("TS_RAFT_WAL_DELTA_ENTRIES").ok().as_deref(),
            Some("0") | Some("false") | Some("off")
        )
    })
}

#[derive(Debug, Clone)]
pub struct LocalRaftWal {
    root: PathBuf,
    cursors: Arc<Mutex<BTreeMap<(ShardId, RaftNodeId), NodeWalCursor>>>,
    /// One barrier gate per node log, so writers that arrive while an fsync is in flight ride it
    /// instead of queueing to take an identical one. Shared by `clone`, like the cursors: two
    /// handles to the same file must share a gate or each would take its own barrier again.
    flush_gates: Arc<crate::flush_gate::FlushRegistry>,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppendEntriesRequest {
    #[serde(default)]
    pub rpc: Option<RaftRpcMetadata>,
    pub shard_id: ShardId,
    pub term: u64,
    pub leader_id: RaftNodeId,
    pub target_id: RaftNodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteRequest {
    #[serde(default)]
    pub rpc: Option<RaftRpcMetadata>,
    pub shard_id: ShardId,
    pub term: u64,
    pub candidate_id: RaftNodeId,
    pub target_id: RaftNodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    /// Asks whether the vote WOULD be granted, without anyone acting on the question.
    ///
    /// A node that cannot reach the cluster would otherwise campaign on a timer and raise its term
    /// every time, and a leader that later hears that term must step down -- so one unreachable
    /// node drags a healthy cluster through repeated elections. Answering a pre-vote changes no
    /// state, so an unreachable node never moves its term.
    #[serde(default)]
    pub pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InstallSnapshotRequest {
    #[serde(default)]
    pub rpc: Option<RaftRpcMetadata>,
    #[serde(default)]
    pub external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    pub shard_id: ShardId,
    pub term: u64,
    pub leader_id: RaftNodeId,
    pub target_id: RaftNodeId,
    pub snapshot: RaftSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: u64,
    pub success: bool,
    pub last_included_index: u64,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotInstallReport {
    pub shard_id: ShardId,
    pub node_id: RaftNodeId,
    pub snapshot_index: u64,
    pub before_commit_index: u64,
    pub after_commit_index: u64,
    pub freeze_started: bool,
    pub flush_completed: bool,
    pub manifest_verified: bool,
    pub checksum_verified: bool,
    pub install_completed: bool,
    pub tail_replay_completed: bool,
    pub rollback_performed: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InstallSnapshotChunkRequest {
    #[serde(default)]
    pub rpc: Option<RaftRpcMetadata>,
    pub shard_id: ShardId,
    pub term: u64,
    pub leader_id: RaftNodeId,
    pub target_id: RaftNodeId,
    pub snapshot_id: String,
    pub last_included_term: u64,
    pub last_included_index: u64,
    pub chunk_index: u64,
    pub chunk_count: u64,
    pub entries: Vec<RaftLogEntry>,
    /// S2: opaque engine state image, carried on chunk 0 of an image-based snapshot (whose
    /// `entries` are empty, so it is a single chunk). Default `None` keeps entry-carrying chunks
    /// and older peers byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_image: Option<RaftSnapshotStateImage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotChunkResponse {
    pub term: u64,
    pub success: bool,
    pub snapshot_complete: bool,
    pub received_chunks: u64,
    pub last_included_index: u64,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRpcMetadata {
    pub auth_token: Option<String>,
    pub deadline_ms: Option<u64>,
    pub request_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftTickOutcome {
    LeaderAlive {
        leader_id: RaftNodeId,
    },
    ElectionPending {
        elapsed_tick: u64,
        timeout_tick: u64,
    },
    PreVoteRejected {
        candidate_id: RaftNodeId,
    },
    LeaderElected {
        leader_id: RaftNodeId,
        term: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSchedulerOptions {
    pub heartbeat_interval_tick: u64,
    pub election_timeout_min_tick: u64,
    pub election_timeout_max_tick: u64,
    pub random_seed: u64,
}

impl Default for RaftSchedulerOptions {
    fn default() -> Self {
        Self {
            heartbeat_interval_tick: 1,
            election_timeout_min_tick: 3,
            election_timeout_max_tick: 6,
            random_seed: 0x5eed,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSchedulerTick {
    pub heartbeat_due: bool,
    pub election_due: bool,
    pub elapsed_heartbeat_tick: u64,
    pub elapsed_election_tick: u64,
    pub election_timeout_tick: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftScheduler {
    options: RaftSchedulerOptions,
    elapsed_heartbeat_tick: u64,
    elapsed_election_tick: u64,
    election_timeout_tick: u64,
    rng_state: u64,
}

impl RaftScheduler {
    pub fn new(options: RaftSchedulerOptions) -> Self {
        let mut scheduler = Self {
            options: RaftSchedulerOptions {
                heartbeat_interval_tick: options.heartbeat_interval_tick.max(1),
                election_timeout_min_tick: options.election_timeout_min_tick.max(1),
                election_timeout_max_tick: options
                    .election_timeout_max_tick
                    .max(options.election_timeout_min_tick.max(1)),
                random_seed: options.random_seed,
            },
            elapsed_heartbeat_tick: 0,
            elapsed_election_tick: 0,
            election_timeout_tick: 0,
            rng_state: options.random_seed,
        };
        scheduler.election_timeout_tick = scheduler.next_election_timeout();
        scheduler
    }

    pub fn tick(&mut self, leader_alive: bool) -> RaftSchedulerTick {
        self.elapsed_heartbeat_tick += 1;
        if leader_alive {
            self.elapsed_election_tick = 0;
            self.election_timeout_tick = self.next_election_timeout();
        } else {
            self.elapsed_election_tick += 1;
        }

        let heartbeat_due =
            self.elapsed_heartbeat_tick >= self.options.heartbeat_interval_tick && leader_alive;
        if heartbeat_due {
            self.elapsed_heartbeat_tick = 0;
        }
        let election_due =
            !leader_alive && self.elapsed_election_tick >= self.election_timeout_tick;
        if election_due {
            self.elapsed_election_tick = 0;
            self.election_timeout_tick = self.next_election_timeout();
        }
        RaftSchedulerTick {
            heartbeat_due,
            election_due,
            elapsed_heartbeat_tick: self.elapsed_heartbeat_tick,
            elapsed_election_tick: self.elapsed_election_tick,
            election_timeout_tick: self.election_timeout_tick,
        }
    }

    fn next_election_timeout(&mut self) -> u64 {
        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        let min = self.options.election_timeout_min_tick;
        let max = self.options.election_timeout_max_tick;
        let span = max.saturating_sub(min).saturating_add(1);
        min + (self.rng_state % span)
    }
}

pub trait RaftTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError>;

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError>;

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError>;

    fn install_snapshot_chunk(
        &self,
        _request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        Err(RaftError::Transport(
            "snapshot chunk transport is not implemented".to_string(),
        ))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRpcRuntimeOptions {
    pub max_inflight: usize,
    pub max_retries: usize,
    pub retry_backoff_ms: u64,
    pub deadline_ms: u64,
    pub auth_token_required: bool,
}

impl Default for RaftRpcRuntimeOptions {
    fn default() -> Self {
        Self {
            max_inflight: 128,
            max_retries: 2,
            retry_backoff_ms: 10,
            deadline_ms: 1_000,
            auth_token_required: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRpcRuntimeMetrics {
    pub inflight: usize,
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub retries: u64,
    pub backpressure_rejections: u64,
}

#[derive(Debug, Clone)]
pub struct RaftRpcRuntime<T> {
    transport: T,
    options: RaftRpcRuntimeOptions,
    auth_token: Option<String>,
    inflight: Arc<Mutex<usize>>,
    metrics: Arc<Mutex<RaftRpcRuntimeMetrics>>,
    request_counter: Arc<Mutex<u64>>,
}

impl<T> RaftRpcRuntime<T> {
    pub fn new(transport: T, options: RaftRpcRuntimeOptions) -> Self {
        Self::with_auth_token(transport, options, None)
    }

    pub fn with_auth_token(
        transport: T,
        options: RaftRpcRuntimeOptions,
        auth_token: Option<String>,
    ) -> Self {
        Self {
            transport,
            options: RaftRpcRuntimeOptions {
                max_inflight: options.max_inflight.max(1),
                deadline_ms: options.deadline_ms.max(1),
                ..options
            },
            auth_token,
            inflight: Arc::default(),
            metrics: Arc::default(),
            request_counter: Arc::default(),
        }
    }

    pub fn inflight(&self) -> usize {
        *self
            .inflight
            .lock()
            .expect("raft rpc inflight lock poisoned")
    }

    pub fn metrics(&self) -> RaftRpcRuntimeMetrics {
        let mut metrics = *self.metrics.lock().expect("raft rpc metrics lock poisoned");
        metrics.inflight = self.inflight();
        metrics
    }

    fn record_metrics(&self, update: impl FnOnce(&mut RaftRpcRuntimeMetrics)) {
        let mut metrics = self.metrics.lock().expect("raft rpc metrics lock poisoned");
        update(&mut metrics);
    }

    fn acquire(&self) -> Result<RaftRpcPermit, RaftError> {
        let mut inflight = self
            .inflight
            .lock()
            .expect("raft rpc inflight lock poisoned");
        if *inflight >= self.options.max_inflight {
            self.record_metrics(|metrics| metrics.backpressure_rejections += 1);
            return Err(RaftError::Transport("raft rpc backpressure".to_string()));
        }
        *inflight += 1;
        Ok(RaftRpcPermit {
            inflight: Arc::clone(&self.inflight),
        })
    }

    fn retry<R>(&self, mut call: impl FnMut() -> Result<R, RaftError>) -> Result<R, RaftError> {
        let _permit = self.acquire()?;
        let attempts = self.options.max_retries.saturating_add(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            self.record_metrics(|metrics| metrics.attempts += 1);
            match call() {
                Ok(response) => {
                    self.record_metrics(|metrics| metrics.successes += 1);
                    return Ok(response);
                }
                Err(err @ RaftError::Transport(_)) => {
                    if attempt + 1 < attempts {
                        self.record_metrics(|metrics| metrics.retries += 1);
                    }
                    last_error = Some(err);
                    if attempt + 1 < attempts && self.options.retry_backoff_ms > 0 {
                        thread::sleep(Duration::from_millis(self.options.retry_backoff_ms));
                    }
                }
                Err(err) => {
                    self.record_metrics(|metrics| metrics.failures += 1);
                    return Err(err);
                }
            }
        }
        self.record_metrics(|metrics| metrics.failures += 1);
        Err(last_error.unwrap_or_else(|| RaftError::Transport("raft rpc failed".to_string())))
    }

    fn next_metadata(&self) -> RaftRpcMetadata {
        let mut counter = self
            .request_counter
            .lock()
            .expect("raft rpc request counter lock poisoned");
        *counter += 1;
        RaftRpcMetadata {
            auth_token: self.auth_token.clone(),
            deadline_ms: Some(self.options.deadline_ms),
            request_id: format!("raft-rpc-{}", *counter),
        }
    }
}

#[derive(Debug)]
struct RaftRpcPermit {
    inflight: Arc<Mutex<usize>>,
}

impl Drop for RaftRpcPermit {
    fn drop(&mut self) {
        let mut inflight = self
            .inflight
            .lock()
            .expect("raft rpc inflight lock poisoned");
        *inflight = inflight.saturating_sub(1);
    }
}

impl<T> RaftTransport for RaftRpcRuntime<T>
where
    T: RaftTransport,
{
    fn append_entries(
        &self,
        mut request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        request.rpc = Some(self.next_metadata());
        self.retry(|| self.transport.append_entries(request.clone()))
    }

    fn request_vote(&self, mut request: VoteRequest) -> Result<VoteResponse, RaftError> {
        request.rpc = Some(self.next_metadata());
        self.retry(|| self.transport.request_vote(request.clone()))
    }

    fn install_snapshot(
        &self,
        mut request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        request.rpc = Some(self.next_metadata());
        self.retry(|| self.transport.install_snapshot(request.clone()))
    }

    fn install_snapshot_chunk(
        &self,
        mut request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        request.rpc = Some(self.next_metadata());
        self.retry(|| self.transport.install_snapshot_chunk(request.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct AuthenticatedRaftTransport<T> {
    inner: T,
    required_token: String,
}

impl<T> AuthenticatedRaftTransport<T> {
    pub fn new(inner: T, required_token: impl Into<String>) -> Self {
        Self {
            inner,
            required_token: required_token.into(),
        }
    }

    fn check(&self, rpc: &Option<RaftRpcMetadata>) -> Result<(), RaftError> {
        validate_raft_rpc_metadata(rpc, &self.required_token)
    }
}

impl<T> RaftTransport for AuthenticatedRaftTransport<T>
where
    T: RaftTransport,
{
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.check(&request.rpc)?;
        self.inner.append_entries(request)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.check(&request.rpc)?;
        self.inner.request_vote(request)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.check(&request.rpc)?;
        self.inner.install_snapshot(request)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        self.check(&request.rpc)?;
        self.inner.install_snapshot_chunk(request)
    }
}

#[derive(Debug, Clone)]
pub struct HttpRaftTransport {
    peers: BTreeMap<RaftNodeId, String>,
    options: HttpRequestOptions,
}

impl HttpRaftTransport {
    pub fn new(peers: BTreeMap<RaftNodeId, String>) -> Self {
        Self {
            peers,
            options: HttpRequestOptions::default(),
        }
    }

    pub fn with_options(peers: BTreeMap<RaftNodeId, String>, options: HttpRequestOptions) -> Self {
        Self { peers, options }
    }

    fn peer_addr(&self, node_id: RaftNodeId) -> Result<&str, RaftError> {
        self.peers
            .get(&node_id)
            .map(String::as_str)
            .ok_or(RaftError::NodeNotFound(node_id))
    }
}

impl RaftTransport for HttpRaftTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let addr = self.peer_addr(request.target_id)?.to_string();
        if wal_proto::binary_replication_enabled() {
            let body = wal_proto::encode_append_entries(&request)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            // The response is a handful of integers either way, so it stays as it was; the size
            // that matters is the entries travelling out.
            let raw = crate::http::request_bytes_with_options(
                &addr,
                "POST",
                "/raft/append_entries",
                &body,
                "application/x-protobuf",
                self.options,
            )
            .map_err(|err| RaftError::Transport(err.to_string()))?;
            return serde_json::from_slice(&raw)
                .map_err(|err| RaftError::Transport(err.to_string()));
        }
        Ok(post_json_with_options(
            &addr,
            "/raft/append_entries",
            &request,
            self.options,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        Ok(post_json_with_options(
            self.peer_addr(request.target_id)?,
            "/raft/request_vote",
            &request,
            self.options,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        Ok(post_json_with_options(
            self.peer_addr(request.target_id)?,
            "/raft/install_snapshot",
            &request,
            self.options,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        Ok(post_json_with_options(
            self.peer_addr(request.target_id)?,
            "/raft/install_snapshot_chunk",
            &request,
            self.options,
        )
        .map_err(|err| RaftError::Transport(err.to_string()))?)
    }
}

/// Whether a request body carries the binary encoding rather than text.
pub fn is_binary_rpc(body: &[u8]) -> bool {
    wal_proto::is_binary_rpc(body)
}

/// Decode a binary replicated batch. Callers that only need a field from the header still have to
/// decode, since a binary body has no fields to read out by name.
pub fn decode_append_entries(body: &[u8]) -> std::io::Result<AppendEntriesRequest> {
    wal_proto::decode_append_entries(body)
}

pub fn handle_raft_http(cluster: &RaftCluster, request: HttpRequest) -> (u16, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        // Accept either encoding: a body starting with the magic byte is binary, and a text
        // body always starts with `{`.
        ("POST", "/raft/append_entries") if wal_proto::is_binary_rpc(&request.body) => {
            match wal_proto::decode_append_entries(&request.body) {
                Ok(req) => match cluster.receive_append_entries(req) {
                    Ok(response) => json_response(200, &response),
                    Err(err) => json_response(500, &err.to_string()),
                },
                Err(err) => json_response(400, &err.to_string()),
            }
        }
        ("POST", "/raft/append_entries") => match parse_json::<AppendEntriesRequest>(&request.body)
        {
            Ok(req) => match cluster.receive_append_entries(req) {
                Ok(response) => json_response(200, &response),
                Err(err) => json_response(500, &err.to_string()),
            },
            Err(err) => json_response(400, &err.to_string()),
        },
        ("POST", "/raft/request_vote") => match parse_json::<VoteRequest>(&request.body) {
            Ok(req) => match cluster.receive_vote_request(req) {
                Ok(response) => json_response(200, &response),
                Err(err) => json_response(500, &err.to_string()),
            },
            Err(err) => json_response(400, &err.to_string()),
        },
        ("POST", "/raft/install_snapshot") => {
            match parse_json::<InstallSnapshotRequest>(&request.body) {
                Ok(req) => match cluster.receive_install_snapshot(req) {
                    Ok(response) => json_response(200, &response),
                    Err(err) => json_response(500, &err.to_string()),
                },
                Err(err) => json_response(400, &err.to_string()),
            }
        }
        ("POST", "/raft/install_snapshot_chunk") => {
            match parse_json::<InstallSnapshotChunkRequest>(&request.body) {
                Ok(req) => match cluster.receive_install_snapshot_chunk(req) {
                    Ok(response) => json_response(200, &response),
                    Err(err) => json_response(500, &err.to_string()),
                },
                Err(err) => json_response(400, &err.to_string()),
            }
        }
        _ => json_response(404, &"unknown raft route"),
    }
}

pub fn handle_authenticated_raft_http(
    cluster: &RaftCluster,
    request: HttpRequest,
    required_token: &str,
) -> (u16, Vec<u8>) {
    let auth = match raft_rpc_metadata_for_http_request(&request) {
        Ok(Some(rpc)) => validate_raft_rpc_metadata(&Some(rpc), required_token),
        Ok(None) => Ok(()),
        Err(err) => Err(err),
    };
    match auth {
        Ok(()) => handle_raft_http(cluster, request),
        Err(err) => json_response(403, &err.to_string()),
    }
}

fn raft_rpc_metadata_for_http_request(
    request: &HttpRequest,
) -> Result<Option<RaftRpcMetadata>, RaftError> {
    match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/raft/append_entries") => {
            // A binary body carries its rpc metadata inside the message, not as a JSON field.
            // Parsing it as JSON here fails, and that failure surfaced as a 403 on EVERY binary
            // append -- the authenticated wrapper choked before dispatch ever saw the request.
            if wal_proto::is_binary_rpc(&request.body) {
                let req = wal_proto::decode_append_entries(&request.body)
                    .map_err(|err| RaftError::Transport(err.to_string()))?;
                return Ok(req.rpc);
            }
            let req = parse_json::<AppendEntriesRequest>(&request.body)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            Ok(req.rpc)
        }
        ("POST", "/raft/request_vote") => {
            let req = parse_json::<VoteRequest>(&request.body)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            Ok(req.rpc)
        }
        ("POST", "/raft/install_snapshot") => {
            let req = parse_json::<InstallSnapshotRequest>(&request.body)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            Ok(req.rpc)
        }
        ("POST", "/raft/install_snapshot_chunk") => {
            let req = parse_json::<InstallSnapshotChunkRequest>(&request.body)
                .map_err(|err| RaftError::Transport(err.to_string()))?;
            Ok(req.rpc)
        }
        _ => Ok(None),
    }
}

fn validate_raft_rpc_metadata(
    rpc: &Option<RaftRpcMetadata>,
    required_token: &str,
) -> Result<(), RaftError> {
    let Some(rpc) = rpc else {
        return Err(RaftError::Transport(
            "missing raft rpc metadata".to_string(),
        ));
    };
    if rpc.auth_token.as_deref() != Some(required_token) {
        return Err(RaftError::Transport("raft rpc auth failed".to_string()));
    }
    if rpc.deadline_ms.unwrap_or_default() == 0 {
        return Err(RaftError::Transport(
            "raft rpc deadline missing".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftProposeRequest {
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftReadRequest {
    pub node_id: RaftNodeId,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftControlLeadershipRequest {
    pub node_id: RaftNodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftCommandResponse {
    pub status: Status,
    pub response: CommandResponse,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProductionRaftEngineKind {
    TemporalRaft,
    RaftRs,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProductionRaftSecurityMode {
    Mtls,
    PlaintextForLocalChaos,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftSecurity {
    pub mode: ProductionRaftSecurityMode,
    pub auth_token: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub ca_cert_path: Option<String>,
}

impl ProductionRaftSecurity {
    pub fn mtls(
        auth_token: impl Into<String>,
        cert_path: impl Into<String>,
        key_path: impl Into<String>,
        ca_cert_path: impl Into<String>,
    ) -> Self {
        Self {
            mode: ProductionRaftSecurityMode::Mtls,
            auth_token: Some(auth_token.into()),
            cert_path: Some(cert_path.into()),
            key_path: Some(key_path.into()),
            ca_cert_path: Some(ca_cert_path.into()),
        }
    }

    pub fn plaintext_for_local_chaos(auth_token: impl Into<String>) -> Self {
        Self {
            mode: ProductionRaftSecurityMode::PlaintextForLocalChaos,
            auth_token: Some(auth_token.into()),
            cert_path: None,
            key_path: None,
            ca_cert_path: None,
        }
    }

    pub fn validate(&self, allow_plaintext_for_local_chaos: bool) -> Result<(), RaftError> {
        if self.auth_token.as_deref().unwrap_or_default().is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires an auth token".to_string(),
            ));
        }
        match self.mode {
            ProductionRaftSecurityMode::Mtls => {
                validate_nonempty_file(self.cert_path.as_deref().unwrap_or_default(), "cert_path")?;
                validate_nonempty_file(self.key_path.as_deref().unwrap_or_default(), "key_path")?;
                validate_nonempty_file(
                    self.ca_cert_path.as_deref().unwrap_or_default(),
                    "ca_cert_path",
                )?;
            }
            ProductionRaftSecurityMode::PlaintextForLocalChaos
                if !allow_plaintext_for_local_chaos =>
            {
                return Err(RaftError::InvalidConfig(
                    "plaintext raft transport is allowed only for local chaos tests".to_string(),
                ));
            }
            ProductionRaftSecurityMode::PlaintextForLocalChaos => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftSecurityEnv {
    pub security: ProductionRaftSecurity,
    pub allow_plaintext_for_local_chaos: bool,
}

pub fn production_raft_security_from_env(
    default_auth_token: impl Into<String>,
    default_allow_plaintext_for_local_chaos: bool,
) -> ProductionRaftSecurityEnv {
    production_raft_security_from_lookup(
        default_auth_token,
        default_allow_plaintext_for_local_chaos,
        |key| std::env::var(key).ok(),
    )
}

pub fn production_raft_security_from_lookup<F>(
    default_auth_token: impl Into<String>,
    default_allow_plaintext_for_local_chaos: bool,
    mut lookup: F,
) -> ProductionRaftSecurityEnv
where
    F: FnMut(&str) -> Option<String>,
{
    let auth_token = lookup("TS_RAFT_AUTH_TOKEN").unwrap_or_else(|| default_auth_token.into());
    let mode = lookup("TS_RAFT_SECURITY_MODE")
        .or_else(|| lookup("TS_RAFT_TRANSPORT_SECURITY"))
        .unwrap_or_else(|| "plaintext_for_local_chaos".to_string());
    let allow_plaintext_for_local_chaos = lookup("TS_RAFT_ALLOW_PLAINTEXT")
        .map(|raw| parse_env_bool(&raw, default_allow_plaintext_for_local_chaos))
        .unwrap_or(default_allow_plaintext_for_local_chaos);
    let security = match mode.trim().to_ascii_lowercase().as_str() {
        "mtls" | "mutual_tls" | "mutual-tls" => ProductionRaftSecurity::mtls(
            auth_token,
            lookup("TS_RAFT_CERT_PATH").unwrap_or_default(),
            lookup("TS_RAFT_KEY_PATH").unwrap_or_default(),
            lookup("TS_RAFT_CA_CERT_PATH")
                .or_else(|| lookup("TS_RAFT_CA_PATH"))
                .unwrap_or_default(),
        ),
        "plaintext" | "plaintext_for_local_chaos" | "local_chaos" => {
            ProductionRaftSecurity::plaintext_for_local_chaos(auth_token)
        }
        other => ProductionRaftSecurity::mtls(
            auth_token,
            format!("invalid-security-mode-{other}"),
            "",
            "",
        ),
    };
    ProductionRaftSecurityEnv {
        security,
        allow_plaintext_for_local_chaos,
    }
}

fn parse_env_bool(raw: &str, default: bool) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => true,
        "0" | "false" | "no" | "n" | "off" => false,
        _ => default,
    }
}

fn validate_nonempty_file(path: &str, label: &str) -> Result<(), RaftError> {
    let path = path.trim();
    if path.is_empty() {
        return Err(RaftError::InvalidConfig(format!(
            "production raft mTLS requires {label}"
        )));
    }
    let metadata = fs::metadata(path).map_err(|err| {
        RaftError::InvalidConfig(format!(
            "production raft mTLS {label} is not readable at {path}: {err}"
        ))
    })?;
    if !metadata.is_file() {
        return Err(RaftError::InvalidConfig(format!(
            "production raft mTLS {label} must point to a file: {path}"
        )));
    }
    if metadata.len() == 0 {
        return Err(RaftError::InvalidConfig(format!(
            "production raft mTLS {label} must not be empty: {path}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftNode {
    pub node_id: RaftNodeId,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftRuntimeOptions {
    pub engine: ProductionRaftEngineKind,
    pub shard_id: ShardId,
    pub local_node_id: RaftNodeId,
    pub nodes: Vec<ProductionRaftNode>,
    pub wal_dir: String,
    pub config: RaftConfig,
    pub rpc: RaftRpcRuntimeOptions,
    pub security: ProductionRaftSecurity,
    pub heartbeat_interval_ms: u64,
    pub election_tick_ms: u64,
    pub max_catchup_entries_per_heartbeat: u64,
    pub allow_plaintext_for_local_chaos: bool,
    /// How often the timer loop asks whether the applied raft log has grown past
    /// [`RaftConfig::max_applied_log_bytes`] and should be compacted into a
    /// snapshot. Zero disables the check.
    ///
    /// Nothing used to ask. `maybe_trigger_snapshot` was implemented, and the
    /// config already said to compact at a threshold, but no loop ever called
    /// it -- so the log grew for the life of the process.
    pub snapshot_check_interval_ms: u64,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftProcessSpec {
    pub node_id: RaftNodeId,
    pub addr: String,
    pub wal_dir: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionRaftChaosPlan {
    pub shard_id: ShardId,
    pub nodes: Vec<ProductionRaftProcessSpec>,
    pub partition_pairs: Vec<(RaftNodeId, RaftNodeId)>,
    pub crash_nodes: Vec<RaftNodeId>,
    pub restart_nodes: Vec<RaftNodeId>,
}

impl ProductionRaftChaosPlan {
    pub fn validate(&self) -> Result<(), RaftError> {
        if self.nodes.len() < 3 {
            return Err(RaftError::InvalidConfig(
                "multi-process chaos plan requires at least three raft nodes".to_string(),
            ));
        }
        let node_ids = self
            .nodes
            .iter()
            .map(|node| node.node_id)
            .collect::<BTreeSet<_>>();
        for node_id in self
            .crash_nodes
            .iter()
            .chain(self.restart_nodes.iter())
            .copied()
        {
            if !node_ids.contains(&node_id) {
                return Err(RaftError::NodeNotFound(node_id));
            }
        }
        for (left, right) in &self.partition_pairs {
            if !node_ids.contains(left) || !node_ids.contains(right) {
                return Err(RaftError::InvalidConfig(
                    "partition pair references an unknown raft node".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct InstantCompat(std::time::Instant);

impl InstantCompat {
    fn now() -> Self {
        Self(std::time::Instant::now())
    }

    fn elapsed(self) -> Duration {
        self.0.elapsed()
    }
}

#[cfg(feature = "temporal-raft-engine")]
pub mod temporal_raft_integration {
    use super::*;
    use std::collections::BTreeSet;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftConfig;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftLogId {
        pub term: u64,
        pub node_id: RaftNodeId,
        pub index: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub enum TemporalRaftEntryPayload {
        Blank,
        Normal(Command),
        Membership(TemporalRaftMembership),
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TemporalRaftEntry {
        pub log_id: TemporalRaftLogId,
        pub payload: TemporalRaftEntryPayload,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftNode {
        pub addr: String,
    }

    impl TemporalRaftNode {
        pub fn new(addr: String) -> Self {
            Self { addr }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftMembership {
        pub voter_sets: Vec<BTreeSet<RaftNodeId>>,
        pub nodes: BTreeMap<RaftNodeId, TemporalRaftNode>,
    }

    impl TemporalRaftMembership {
        pub fn new(
            voter_sets: Vec<BTreeSet<RaftNodeId>>,
            nodes: BTreeMap<RaftNodeId, TemporalRaftNode>,
        ) -> Self {
            Self { voter_sets, nodes }
        }

        pub fn voter_ids(&self) -> impl Iterator<Item = RaftNodeId> + '_ {
            self.voter_sets
                .first()
                .into_iter()
                .flat_map(|voters| voters.iter().copied())
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftStoredMembership {
        pub last_log_id: Option<TemporalRaftLogId>,
        pub membership: TemporalRaftMembership,
    }

    impl TemporalRaftStoredMembership {
        pub fn new(
            last_log_id: Option<TemporalRaftLogId>,
            membership: TemporalRaftMembership,
        ) -> Self {
            Self {
                last_log_id,
                membership,
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftSnapshotMeta {
        pub last_log_id: Option<TemporalRaftLogId>,
        pub last_membership: TemporalRaftStoredMembership,
        pub snapshot_id: String,
    }

    pub trait ExternalRaftEngine {
        fn propose_command(&self, command: Command) -> Result<CommandResponse, RaftError>;
        fn install_snapshot_chunks(
            &self,
            chunks: Vec<InstallSnapshotChunkRequest>,
        ) -> Result<InstallSnapshotChunkResponse, RaftError>;
        fn joint_change(
            &self,
            new_voters: Vec<RaftNodeId>,
        ) -> Result<JointConsensusMembership, RaftError>;
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    pub enum TemporalRaftRuntimeKind {
        DataNode,
        Metaserver,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TemporalRaftDurableLogRecord {
        pub log_id: u64,
        pub term: u64,
        pub leader_id: RaftNodeId,
        pub index: u64,
        pub shard_id: ShardId,
        pub command: Option<Command>,
        pub membership: Option<Vec<RaftNodeId>>,
        pub checksum: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct TemporalRaftDurableSnapshot {
        pub snapshot_id: String,
        pub last_log_index: u64,
        pub last_log_term: u64,
        pub last_leader_id: RaftNodeId,
        pub applied_index: u64,
        pub membership: Vec<RaftNodeId>,
        pub payload_checksum: String,
        pub payload_bytes: Vec<u8>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TemporalRaftDurableState {
        version: u32,
        kind: TemporalRaftRuntimeKind,
        shard_id: ShardId,
        local_node_id: RaftNodeId,
        current_term: u64,
        leader_id: RaftNodeId,
        committed_index: u64,
        applied_index: u64,
        first_index: u64,
        last_index: u64,
        voters: Vec<RaftNodeId>,
        learners: Vec<RaftNodeId>,
        log: Vec<TemporalRaftDurableLogRecord>,
        snapshot: Option<TemporalRaftDurableSnapshot>,
        #[serde(default)]
        storage_apply_fence: RaftStorageApplyFence,
    }

    impl TemporalRaftDurableState {
        fn new(
            kind: TemporalRaftRuntimeKind,
            options: &DataRaftConsensusOptions,
            voters: Vec<RaftNodeId>,
            learners: Vec<RaftNodeId>,
        ) -> Self {
            let mut voters = voters;
            if voters.is_empty() {
                voters = options
                    .peers
                    .iter()
                    .map(|peer| peer.replica_id)
                    .chain(std::iter::once(options.replica_id))
                    .collect();
            }
            voters.sort_unstable();
            voters.dedup();
            let leader_id = voters.first().copied().unwrap_or(options.replica_id);
            let mut state = Self {
                version: 1,
                kind,
                shard_id: options.shard_id,
                local_node_id: options.replica_id,
                current_term: 1,
                leader_id,
                committed_index: options.initial_applied_index,
                applied_index: options.initial_applied_index,
                first_index: options.initial_applied_index.saturating_add(1),
                last_index: options.initial_applied_index,
                voters,
                learners,
                log: Vec::new(),
                snapshot: None,
                storage_apply_fence: RaftStorageApplyFence::default(),
            };
            state.refresh_storage_apply_fence();
            state
        }

        fn snapshot_id(&self) -> Option<&str> {
            self.snapshot
                .as_ref()
                .map(|snapshot| snapshot.snapshot_id.as_str())
        }

        fn storage_epoch(&self) -> u64 {
            self.applied_index.max(
                self.snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.applied_index)
                    .unwrap_or_default(),
            )
        }

        fn refresh_storage_apply_fence(&mut self) {
            let snapshot_id = self.snapshot_id().map(str::to_string);
            let storage_epoch = self.storage_epoch();
            let checksum = raft_storage_apply_fence_checksum(
                self.shard_id,
                self.current_term,
                self.committed_index,
                self.applied_index,
                snapshot_id.as_deref(),
                storage_epoch,
            );
            self.storage_apply_fence = RaftStorageApplyFence {
                shard_id: self.shard_id,
                raft_term: self.current_term,
                committed_index: self.committed_index,
                applied_index: self.applied_index,
                snapshot_id,
                storage_epoch,
                checksum,
            };
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TemporalRaftBackendReport {
        pub kind: TemporalRaftRuntimeKind,
        pub shard_id: ShardId,
        pub local_node_id: RaftNodeId,
        pub durable_log_records: usize,
        pub snapshot_installed: bool,
        pub storage_apply_fence: RaftStorageApplyFence,
        pub storage_apply_fence_valid: bool,
        pub read_index_supported: bool,
        pub membership_change_supported: bool,
        pub leader_transfer_supported: bool,
        pub campaign_supported: bool,
        pub learner_bootstrap_supported: bool,
        pub temporal_raft_entry_boundary: String,
    }

    #[derive(Debug, Clone)]
    pub struct TemporalRaftConsensusBackend {
        options: DataRaftConsensusOptions,
        kind: TemporalRaftRuntimeKind,
        state: TemporalRaftDurableState,
        engine: Option<TemporalEngine>,
        state_path: Option<PathBuf>,
        running: bool,
    }

    impl TemporalRaftConsensusBackend {
        pub fn new_data_node(options: DataRaftConsensusOptions, engine: TemporalEngine) -> Self {
            Self::new(options, TemporalRaftRuntimeKind::DataNode, Some(engine))
        }

        pub fn new_metaserver(options: DataRaftConsensusOptions) -> Self {
            Self::new(options, TemporalRaftRuntimeKind::Metaserver, None)
        }

        fn new(
            options: DataRaftConsensusOptions,
            kind: TemporalRaftRuntimeKind,
            engine: Option<TemporalEngine>,
        ) -> Self {
            let state_path = options.wal_dir.as_ref().map(|dir| {
                dir.join(format!(
                    "temporalraft-{}-{}.json",
                    options.shard_id, options.replica_id
                ))
            });
            let mut voters = options
                .peers
                .iter()
                .filter(|peer| peer.replica_id != options.replica_id)
                .map(|peer| peer.replica_id)
                .collect::<Vec<_>>();
            let learners = if options.bootstrap_as_learner {
                vec![options.replica_id]
            } else {
                voters.push(options.replica_id);
                Vec::new()
            };
            let state = match state_path.as_ref() {
                Some(path) => Self::load_state(path)
                    .unwrap_or_else(|err| {
                        panic!("failed to load durable TemporalRaft state from {path:?}: {err}")
                    })
                    .unwrap_or_else(|| {
                        TemporalRaftDurableState::new(kind, &options, voters, learners)
                    }),
                None => TemporalRaftDurableState::new(kind, &options, voters, learners),
            };
            Self {
                options,
                kind,
                state,
                engine,
                state_path,
                running: false,
            }
        }

        pub fn report(&self) -> TemporalRaftBackendReport {
            TemporalRaftBackendReport {
                kind: self.kind,
                shard_id: self.state.shard_id,
                local_node_id: self.state.local_node_id,
                durable_log_records: self.state.log.len(),
                snapshot_installed: self.state.snapshot.is_some(),
                storage_apply_fence: self.state.storage_apply_fence.clone(),
                storage_apply_fence_valid: validate_temporal_raft_storage_apply_fence(&self.state)
                    .is_ok(),
                read_index_supported: true,
                membership_change_supported: true,
                leader_transfer_supported: true,
                campaign_supported: true,
                learner_bootstrap_supported: true,
                temporal_raft_entry_boundary: std::any::type_name::<TemporalRaftEntry>()
                    .to_string(),
            }
        }

        pub fn temporal_raft_membership(&self) -> TemporalRaftMembership {
            let voter_set = self.state.voters.iter().copied().collect::<BTreeSet<_>>();
            let mut nodes = BTreeMap::new();
            for node_id in self
                .state
                .voters
                .iter()
                .chain(self.state.learners.iter())
                .copied()
            {
                let addr = self
                    .options
                    .peers
                    .iter()
                    .find(|peer| peer.replica_id == node_id)
                    .map(|peer| peer.raft_addr.clone())
                    .unwrap_or_default();
                nodes.insert(node_id, TemporalRaftNode::new(addr));
            }
            TemporalRaftMembership::new(vec![voter_set], nodes)
        }

        pub fn temporal_raft_membership_compat(&self) -> TemporalRaftMembership {
            self.temporal_raft_membership()
        }

        pub fn build_temporal_raft_snapshot_meta(&self) -> TemporalRaftSnapshotMeta {
            let last_log_id = if self.state.last_index == 0 {
                None
            } else {
                Some(temporal_raft_log_id(
                    self.state.current_term,
                    self.state.leader_id,
                    self.state.last_index,
                ))
            };
            let membership = self.temporal_raft_membership();
            TemporalRaftSnapshotMeta {
                last_log_id: last_log_id.clone(),
                last_membership: TemporalRaftStoredMembership::new(last_log_id, membership),
                snapshot_id: self
                    .state
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.snapshot_id.clone())
                    .unwrap_or_else(|| {
                        format!("{}-{}", self.state.shard_id, self.state.last_index)
                    }),
            }
        }

        pub fn build_temporal_raft_snapshot_meta_compat(&self) -> TemporalRaftSnapshotMeta {
            self.build_temporal_raft_snapshot_meta()
        }

        fn load_state(path: &Path) -> Result<Option<TemporalRaftDurableState>, RaftError> {
            if !path.exists() {
                return Ok(None);
            }
            let bytes = fs::read(path).map_err(|err| RaftError::Wal(err.to_string()))?;
            let state = serde_json::from_slice(&bytes).map_err(|err| {
                RaftError::Wal(format!("temporal raft state decode failed: {err}"))
            })?;
            validate_temporal_raft_storage_apply_fence(&state)?;
            Ok(Some(state))
        }

        fn persist_state(&self) -> Result<(), RaftError> {
            let Some(path) = &self.state_path else {
                return Ok(());
            };
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|err| RaftError::Wal(err.to_string()))?;
            }
            let tmp = path.with_extension("json.tmp");
            let bytes = serde_json::to_vec_pretty(&self.state)
                .map_err(|err| RaftError::Wal(err.to_string()))?;
            {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp)
                    .map_err(|err| RaftError::Wal(err.to_string()))?;
                use std::io::Write as _;
                file.write_all(&bytes)
                    .map_err(|err| RaftError::Wal(err.to_string()))?;
                file.sync_all()
                    .map_err(|err| RaftError::Wal(err.to_string()))?;
            }
            fs::rename(&tmp, path).map_err(|err| RaftError::Wal(err.to_string()))?;
            if let Some(parent) = path.parent() {
                if let Ok(dir) = fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
            Ok(())
        }

        fn append_record(
            &mut self,
            command: Option<Command>,
            membership: Option<Vec<RaftNodeId>>,
        ) -> Result<u64, RaftError> {
            self.state.last_index = self.state.last_index.saturating_add(1);
            let index = self.state.last_index;
            let log_id = temporal_raft_log_id(self.state.current_term, self.state.leader_id, index);
            let temporal_raft_entry = match (&command, &membership) {
                (Some(command), _) => TemporalRaftEntry {
                    log_id,
                    payload: TemporalRaftEntryPayload::Normal(command.clone()),
                },
                (None, Some(voters)) => {
                    let membership = membership_from_voters(voters, &self.options.peers);
                    TemporalRaftEntry {
                        log_id,
                        payload: TemporalRaftEntryPayload::Membership(membership),
                    }
                }
                (None, None) => TemporalRaftEntry {
                    log_id,
                    payload: TemporalRaftEntryPayload::Blank,
                },
            };
            let checksum = checksum_temporal_raft_record(
                index,
                self.state.current_term,
                self.state.leader_id,
                &command,
                membership.as_deref(),
            )?;
            let record = TemporalRaftDurableLogRecord {
                log_id: temporal_raft_entry.log_id.index,
                term: self.state.current_term,
                leader_id: self.state.leader_id,
                index,
                shard_id: self.state.shard_id,
                command,
                membership,
                checksum,
            };
            self.state.log.push(record);
            self.state.committed_index = index;
            self.apply_committed_record(index)?;
            self.persist_state()?;
            Ok(index)
        }

        fn apply_committed_record(&mut self, index: u64) -> Result<(), RaftError> {
            let Some(record) = self.state.log.iter().find(|record| record.index == index) else {
                return Ok(());
            };
            if let Some(voters) = &record.membership {
                let mut voters = voters.clone();
                voters.sort_unstable();
                voters.dedup();
                if voters.is_empty() {
                    return Err(RaftError::CannotRemoveLastNode);
                }
                self.state.voters = voters;
            }
            if let (Some(engine), Some(command)) = (&self.engine, &record.command) {
                let response = engine.execute_raft_apply(ExecuteRequest {
                    shard_id: self.state.shard_id,
                    command: command.clone(),
                });
                if !response.status.ok {
                    return Err(RaftError::InvalidDataRaftLog(format!(
                        "temporal raft state-machine apply failed: {} {}",
                        response.status.code, response.status.message
                    )));
                }
            }
            self.state.applied_index = self.state.applied_index.max(index);
            self.state.refresh_storage_apply_fence();
            Ok(())
        }
    }

    impl DataRaftConsensusBackend for TemporalRaftConsensusBackend {
        fn start(&mut self) -> Result<(), RaftError> {
            self.running = true;
            self.persist_state()
        }

        fn stop(&mut self) {
            self.running = false;
            let _ = self.persist_state();
        }

        fn is_leader(&self) -> bool {
            self.running && self.state.leader_id == self.state.local_node_id
        }

        fn status(&self) -> Result<DataRaftStatus, RaftError> {
            Ok(DataRaftStatus {
                running: self.running,
                leader: self.is_leader(),
                learner: self.state.learners.contains(&self.state.local_node_id),
                term: self.state.current_term,
                leader_replica_id: self.state.leader_id,
                committed_index: self.state.committed_index,
                applied_index: self.state.applied_index,
                first_index: self.state.first_index,
                last_index: self.state.last_index,
                pending_config_change_index: 0,
                voter_count: self.state.voters.len() as u64,
                learner_count: self.state.learners.len() as u64,
                fatal_event_count: 0,
                snapshot_creating: false,
                snapshot_loading: false,
            })
        }

        fn propose(&mut self, serialized_entry: Vec<u8>) -> Result<u64, RaftError> {
            if !self.is_leader() {
                return Err(RaftError::NotLeader {
                    node_id: self.state.local_node_id,
                });
            }
            let entry = parse_data_raft_log(&serialized_entry)?;
            if entry.shard_id != self.state.shard_id {
                return Err(RaftError::InvalidDataRaftLog(format!(
                    "shard mismatch: entry={}, backend={}",
                    entry.shard_id, self.state.shard_id
                )));
            }
            self.append_record(Some(entry.command), None)
        }

        fn wait_for_applied_index(&self, index: u64, _timeout_ms: u64) -> Result<(), RaftError> {
            if self.state.applied_index >= index {
                Ok(())
            } else {
                Err(RaftError::AppliedIndexTimeout {
                    node_id: self.state.local_node_id,
                    applied_index: self.state.applied_index,
                    target_index: index,
                    timeout_ms: 0,
                })
            }
        }

        fn trigger_snapshot(&mut self) -> Result<u64, RaftError> {
            let payload_bytes = serde_json::to_vec(&self.state.log)
                .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
            let payload_checksum = hex::encode(Sha256::digest(&payload_bytes));
            let snapshot = TemporalRaftDurableSnapshot {
                snapshot_id: format!(
                    "temporalraft-{}-{}-{}",
                    self.state.shard_id, self.state.current_term, self.state.applied_index
                ),
                last_log_index: self.state.last_index,
                last_log_term: self.state.current_term,
                last_leader_id: self.state.leader_id,
                applied_index: self.state.applied_index,
                membership: self.state.voters.clone(),
                payload_checksum,
                payload_bytes,
            };
            self.state.snapshot = Some(snapshot);
            self.state.first_index = self.state.applied_index.saturating_add(1);
            self.state
                .log
                .retain(|record| record.index >= self.state.first_index);
            self.state.refresh_storage_apply_fence();
            self.persist_state()?;
            Ok(self.state.applied_index)
        }

        fn read_index(&self, _timeout_ms: u64) -> Result<(), RaftError> {
            if !self.running {
                return Err(RaftError::LeaderUnavailable);
            }
            if self.state.committed_index > self.state.applied_index {
                return Err(RaftError::ReplicaLagging {
                    replica_id: self.state.local_node_id,
                    replica_commit_index: self.state.applied_index,
                    leader_commit_index: self.state.committed_index,
                });
            }
            Ok(())
        }

        fn add_peer(&mut self, peer: DataRaftPeer) -> Result<(), RaftError> {
            if self.state.voters.contains(&peer.replica_id) {
                return Err(RaftError::NodeAlreadyExists(peer.replica_id));
            }
            let mut voters = self.state.voters.clone();
            voters.push(peer.replica_id);
            voters.sort_unstable();
            voters.dedup();
            self.append_record(None, Some(voters)).map(|_| ())
        }

        fn add_learner(&mut self, peer: DataRaftPeer) -> Result<(), RaftError> {
            if self.state.voters.contains(&peer.replica_id)
                || self.state.learners.contains(&peer.replica_id)
            {
                return Err(RaftError::NodeAlreadyExists(peer.replica_id));
            }
            self.state.learners.push(peer.replica_id);
            self.state.learners.sort_unstable();
            self.state.learners.dedup();
            self.persist_state()?;
            if peer.auto_promote {
                self.promote_peer(peer.replica_id)
            } else {
                Ok(())
            }
        }

        fn promote_peer(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError> {
            if !self.state.learners.contains(&replica_id) {
                return Err(RaftError::NodeNotFound(replica_id));
            }
            let mut voters = self.state.voters.clone();
            voters.push(replica_id);
            self.state.learners.retain(|learner| *learner != replica_id);
            self.append_record(None, Some(voters)).map(|_| ())
        }

        fn remove_peer(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError> {
            if !self.state.voters.contains(&replica_id) {
                self.state.learners.retain(|learner| *learner != replica_id);
                return self.persist_state();
            }
            if self.state.voters.len() == 1 {
                return Err(RaftError::CannotRemoveLastNode);
            }
            let mut voters = self.state.voters.clone();
            voters.retain(|voter| *voter != replica_id);
            self.append_record(None, Some(voters)).map(|_| ())
        }

        fn transfer_leader(&mut self, replica_id: RaftNodeId) -> Result<(), RaftError> {
            if !self.state.voters.contains(&replica_id) {
                return Err(RaftError::NodeNotFound(replica_id));
            }
            self.state.leader_id = replica_id;
            self.state.current_term = self.state.current_term.saturating_add(1);
            self.state.refresh_storage_apply_fence();
            self.persist_state()
        }

        fn campaign(&mut self, _timeout_ms: u64, force: bool) -> Result<(), RaftError> {
            if !self.running {
                return Err(RaftError::LeaderUnavailable);
            }
            if !self.state.voters.contains(&self.state.local_node_id) {
                return Err(RaftError::NodeNotFound(self.state.local_node_id));
            }
            if !force && self.state.committed_index > self.state.applied_index {
                return Err(RaftError::ReplicaLagging {
                    replica_id: self.state.local_node_id,
                    replica_commit_index: self.state.applied_index,
                    leader_commit_index: self.state.committed_index,
                });
            }
            self.state.leader_id = self.state.local_node_id;
            self.state.current_term = self.state.current_term.saturating_add(1);
            self.state.refresh_storage_apply_fence();
            self.persist_state()
        }

        fn can_serve_bounded_stale_read(&self, max_stale_index_lag: u64) -> Result<(), RaftError> {
            let lag = self
                .state
                .committed_index
                .saturating_sub(self.state.applied_index);
            if lag <= max_stale_index_lag {
                Ok(())
            } else {
                Err(RaftError::ReplicaLagging {
                    replica_id: self.state.local_node_id,
                    replica_commit_index: self.state.applied_index,
                    leader_commit_index: self.state.committed_index,
                })
            }
        }
    }

    pub fn new_temporal_raft_data_node_backend(
        options: DataRaftConsensusOptions,
        engine: TemporalEngine,
    ) -> TemporalRaftConsensusBackend {
        TemporalRaftConsensusBackend::new_data_node(options, engine)
    }

    pub fn new_temporal_raft_metaserver_backend(
        options: DataRaftConsensusOptions,
    ) -> TemporalRaftConsensusBackend {
        TemporalRaftConsensusBackend::new_metaserver(options)
    }

    fn temporal_raft_log_id(term: u64, node_id: RaftNodeId, index: u64) -> TemporalRaftLogId {
        TemporalRaftLogId {
            term,
            node_id,
            index,
        }
    }

    fn membership_from_voters(
        voters: &[RaftNodeId],
        peers: &[DataRaftPeer],
    ) -> TemporalRaftMembership {
        let voter_set = voters.iter().copied().collect::<BTreeSet<_>>();
        let mut nodes = BTreeMap::new();
        for voter in voters {
            let addr = peers
                .iter()
                .find(|peer| peer.replica_id == *voter)
                .map(|peer| peer.raft_addr.clone())
                .unwrap_or_default();
            nodes.insert(*voter, TemporalRaftNode::new(addr));
        }
        TemporalRaftMembership::new(vec![voter_set], nodes)
    }

    fn validate_temporal_raft_storage_apply_fence(
        state: &TemporalRaftDurableState,
    ) -> Result<(), RaftError> {
        let fence = &state.storage_apply_fence;
        if fence == &RaftStorageApplyFence::default()
            && (state.committed_index > 0 || state.applied_index > 0 || state.snapshot.is_some())
        {
            return Err(RaftError::ApplySnapshotFence(
                "missing temporal raft storage apply fence".to_string(),
            ));
        }
        if fence == &RaftStorageApplyFence::default() {
            return Ok(());
        }
        if fence.shard_id != state.shard_id {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence shard {} does not match state shard {}",
                fence.shard_id, state.shard_id
            )));
        }
        if fence.raft_term != state.current_term {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence term {} does not match current term {}",
                fence.raft_term, state.current_term
            )));
        }
        if fence.committed_index != state.committed_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence committed index {} does not match committed index {}",
                fence.committed_index, state.committed_index
            )));
        }
        if fence.applied_index != state.applied_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence applied index {} does not match applied index {}",
                fence.applied_index, state.applied_index
            )));
        }
        if fence.applied_index > fence.committed_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence applied index {} is ahead of committed index {}",
                fence.applied_index, fence.committed_index
            )));
        }
        let snapshot_id = state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.snapshot_id.clone());
        if fence.snapshot_id != snapshot_id {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence snapshot id {:?} does not match durable snapshot id {:?}",
                fence.snapshot_id, snapshot_id
            )));
        }
        let snapshot_index = state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.applied_index)
            .unwrap_or_default();
        if fence.storage_epoch < fence.applied_index || fence.storage_epoch < snapshot_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "temporal raft storage fence epoch {} is behind applied index {} or snapshot index {}",
                fence.storage_epoch, fence.applied_index, snapshot_index
            )));
        }
        let expected = raft_storage_apply_fence_checksum(
            fence.shard_id,
            fence.raft_term,
            fence.committed_index,
            fence.applied_index,
            fence.snapshot_id.as_deref(),
            fence.storage_epoch,
        );
        if fence.checksum != expected {
            return Err(RaftError::ApplySnapshotFence(
                "temporal raft storage fence checksum mismatch".to_string(),
            ));
        }
        Ok(())
    }

    fn checksum_temporal_raft_record(
        index: u64,
        term: u64,
        leader_id: RaftNodeId,
        command: &Option<Command>,
        membership: Option<&[RaftNodeId]>,
    ) -> Result<String, RaftError> {
        let payload = serde_json::to_vec(&(index, term, leader_id, command, membership))
            .map_err(|err| RaftError::Wal(err.to_string()))?;
        Ok(hex::encode(Sha256::digest(payload)))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftReadStrategy {
    RelaxRead,
    LeaseRead,
    ReadIndex,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReadOptions {
    pub strategy: RaftReadStrategy,
    pub enable_read_from_follower: bool,
    pub fill_cache: bool,
    pub ignore_write_intent: bool,
    pub wait_millis: u64,
}

impl Default for RaftReadOptions {
    fn default() -> Self {
        Self {
            strategy: RaftReadStrategy::RelaxRead,
            enable_read_from_follower: false,
            fill_cache: true,
            ignore_write_intent: false,
            wait_millis: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataRaftReadMode {
    Leader,
    Linearizable,
    BoundedStale,
    UnsafeAnyReplica,
}

impl FromStr for DataRaftReadMode {
    type Err = RaftError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "leader" => Ok(Self::Leader),
            "linearizable" => Ok(Self::Linearizable),
            "bounded_stale" => Ok(Self::BoundedStale),
            "unsafe_any_replica" => Ok(Self::UnsafeAnyReplica),
            _ => Err(RaftError::InvalidConfig(format!(
                "invalid data_raft_read_mode: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftReadPolicy {
    pub mode: DataRaftReadMode,
    pub bounded_stale_max_index_lag: u64,
    pub read_index_timeout_ms: u64,
}

impl Default for DataRaftReadPolicy {
    fn default() -> Self {
        Self {
            mode: DataRaftReadMode::Leader,
            bounded_stale_max_index_lag: 0,
            read_index_timeout_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftConfig {
    pub election_cycle_tick: u32,
    pub transfer_timeout_tick: u64,
    pub offline_timeout_tick: u64,
    pub lease_duration_ms: u64,
    pub last_lease_duration_ms: u64,
    pub assume_lease_when_start: bool,
    pub max_memory_replicate_log_bytes: u64,
    pub max_disk_replicate_log_num: u64,
    pub max_cache_memory_bytes: u64,
    pub max_apply_batch_bytes: u64,
    pub enable_reorder_queue: bool,
    pub reorder_timeout_us: u64,
    pub reorder_window_size: u64,
    pub max_inflights_apply_task: u64,
    pub max_inflights_replicate: u64,
    pub enable_pre_vote: bool,
    pub prohibits_election: bool,
    pub ignore_witness: bool,
    pub send_snapshot_timeout_ms: u64,
    pub raft_transport_timeout_ms: u64,
    pub wal_sync: bool,
    pub max_segment_bytes: u64,
    pub min_keep_segment_num: u64,
    pub can_trigger_snapshot: bool,
    pub max_applied_log_bytes: u64,
    /// Ceiling on the log kept for a follower that is behind, in bytes.
    ///
    /// Compaction is held while a live follower still needs the entries, so that catching it up
    /// stays a matter of sending entries rather than installing a snapshot. Past this it compacts
    /// anyway: a peer that is this far behind is cheaper to catch up with a snapshot, and one
    /// that never returns must not pin the log open. Zero disables the hold entirely.
    #[serde(default = "default_max_retained_log_bytes")]
    pub max_retained_log_bytes: u64,
    /// P2: how long `propose_distributed_one` waits for the replication quorum before returning
    /// `NoMajority`. Defaults to 5000 ms (the legacy hardcoded deadline); a config that omits the
    /// field also resolves to 5000 so behavior stays byte-identical. Lower it (e.g. 500) so a
    /// lagging/rejecting follower no longer freezes the proposer for a full 5 s.
    #[serde(default = "default_replication_deadline_ms")]
    pub replication_deadline_ms: u64,
}

fn default_replication_deadline_ms() -> u64 {
    5000
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_cycle_tick: 3,
            transfer_timeout_tick: 3,
            offline_timeout_tick: 10,
            lease_duration_ms: 0,
            last_lease_duration_ms: 0,
            assume_lease_when_start: true,
            max_memory_replicate_log_bytes: 32 * 1024,
            max_disk_replicate_log_num: 64,
            max_cache_memory_bytes: 32 * 1024 * 1024,
            max_apply_batch_bytes: 64 * 1024,
            enable_reorder_queue: true,
            reorder_timeout_us: 3_000,
            reorder_window_size: 128,
            max_inflights_apply_task: 5,
            max_inflights_replicate: 128,
            enable_pre_vote: false,
            prohibits_election: false,
            ignore_witness: false,
            // A stall budget, not a transfer budget: the clock resets on every chunk, so
            // this is how long a send may make NO progress before the peer is released. A
            // minute of complete silence is a dead transfer, and holding the peer that long
            // rejects every proposal to it for the same minute.
            send_snapshot_timeout_ms: 10_000,
            raft_transport_timeout_ms: 1_000,
            wal_sync: false,
            max_segment_bytes: 64 * 1024 * 1024,
            min_keep_segment_num: 2,
            can_trigger_snapshot: true,
            max_applied_log_bytes: 1024 * 1024 * 1024,
            max_retained_log_bytes: default_max_retained_log_bytes(),
            replication_deadline_ms: default_replication_deadline_ms(),
        }
    }
}

impl RaftConfig {
    pub fn validate(&self) -> Result<(), RaftConfigError> {
        if self.election_cycle_tick == 0 {
            return Err(RaftConfigError::InvalidValue("election_cycle_tick"));
        }
        if self.max_memory_replicate_log_bytes == 0 {
            return Err(RaftConfigError::InvalidValue(
                "max_memory_replicate_log_bytes",
            ));
        }
        if self.max_disk_replicate_log_num == 0 {
            return Err(RaftConfigError::InvalidValue("max_disk_replicate_log_num"));
        }
        if self.max_apply_batch_bytes == 0 {
            return Err(RaftConfigError::InvalidValue("max_apply_batch_bytes"));
        }
        if self.max_inflights_replicate == 0 {
            return Err(RaftConfigError::InvalidValue("max_inflights_replicate"));
        }
        if self.max_segment_bytes == 0 {
            return Err(RaftConfigError::InvalidValue("max_segment_bytes"));
        }
        if self.min_keep_segment_num == 0 {
            return Err(RaftConfigError::InvalidValue("min_keep_segment_num"));
        }
        Ok(())
    }
}

fn initial_leader_lease_deadline_ms(config: &RaftConfig) -> u64 {
    if config.lease_duration_ms == 0 {
        u64::MAX
    } else if config.assume_lease_when_start {
        config.lease_duration_ms
    } else {
        0
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftConfigError {
    #[error("invalid raft config value: {0}")]
    InvalidValue(&'static str),
}

#[derive(Debug)]
struct RaftNode {
    id: RaftNodeId,
    role: RaftRole,
    replica_role: RaftReplicaRole,
    current_term: u64,
    voted_for: Option<RaftNodeId>,
    commit_index: u64,
    alive: bool,
    log: Vec<RaftLogEntry>,
    installed_snapshot: Option<RaftSnapshot>,
    applied_index: u64,
    applied: BTreeSet<u64>,
    // Monotonic exactly-once floor: the highest raft index ever applied. It is NEVER
    // lowered by a log truncation, so an entry at or below it is never re-executed even
    // if `applied`/`applied_index` were rewound.
    max_applied_index: u64,
    engine: TemporalEngine,
    pipeline_state: RaftPeerPipelineRuntimeState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JointConsensusMembership {
    pub old_voters: Vec<RaftNodeId>,
    pub new_voters: Vec<RaftNodeId>,
}

#[derive(Debug)]
struct PendingSnapshotChunks {
    shard_id: ShardId,
    last_included_term: u64,
    last_included_index: u64,
    chunks: Vec<Option<Vec<RaftLogEntry>>>,
    // S2: opaque engine state image reassembled from the chunk stream (carried on chunk 0 of an
    // image-based snapshot). `None` for the classic entry-carrying stream.
    state_image: Option<RaftSnapshotStateImage>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftError {
    #[error("invalid raft config: {0}")]
    InvalidConfig(String),
    #[error("leader is not available")]
    LeaderUnavailable,
    #[error("not enough live replicas for majority: live={live}, required={required}")]
    NoMajority { live: usize, required: usize },
    #[error("node not found: {0}")]
    NodeNotFound(RaftNodeId),
    #[error("node already exists: {0}")]
    NodeAlreadyExists(RaftNodeId),
    #[error("cannot remove the last node")]
    CannotRemoveLastNode,
    #[error("replica {replica_id} is behind leader commit index: replica={replica_commit_index}, leader={leader_commit_index}")]
    ReplicaLagging {
        replica_id: RaftNodeId,
        replica_commit_index: u64,
        leader_commit_index: u64,
    },
    #[error("node {node_id} did not apply raft index {target_index} within {timeout_ms}ms: applied={applied_index}")]
    AppliedIndexTimeout {
        node_id: RaftNodeId,
        applied_index: u64,
        target_index: u64,
        timeout_ms: u64,
    },
    #[error("replica {replica_id} has committed but not applied up to the read frontier: applied={replica_applied_index}, required={required_index}")]
    ReplicaApplyLagging {
        replica_id: RaftNodeId,
        replica_applied_index: u64,
        required_index: u64,
    },
    #[error("snapshot shard mismatch: snapshot={snapshot_shard_id}, cluster={cluster_shard_id}")]
    SnapshotShardMismatch {
        snapshot_shard_id: ShardId,
        cluster_shard_id: ShardId,
    },
    #[error("stale snapshot cannot overwrite newer local raft state: snapshot_index={snapshot_index}, local_commit_index={local_commit_index}")]
    StaleSnapshot {
        snapshot_index: u64,
        local_commit_index: u64,
    },
    #[error("raft apply/snapshot fence is inconsistent: {0}")]
    ApplySnapshotFence(String),
    #[error("raft log entry too large: bytes={bytes}, limit={limit}")]
    LogEntryTooLarge { bytes: u64, limit: u64 },
    #[error("raft append pipeline backpressure for node {node_id}: inflight_entries={inflight_entries}, inflight_bytes={inflight_bytes}, entry_limit={entry_limit}, byte_limit={byte_limit}")]
    AppendBackpressure {
        node_id: RaftNodeId,
        inflight_entries: u64,
        inflight_bytes: u64,
        entry_limit: u64,
        byte_limit: u64,
    },
    #[error("raft snapshot sender backpressure for node {node_id}: snapshot transfer already in progress")]
    SnapshotBackpressure { node_id: RaftNodeId },
    #[error("node {node_id} is not leader")]
    NotLeader { node_id: RaftNodeId },
    #[error("election is prohibited by raft config")]
    ElectionProhibited,
    #[error("raft wal error: {0}")]
    Wal(String),
    #[error("raft transport error: {0}")]
    Transport(String),
    #[error("joint consensus is already in progress")]
    JointConsensusInProgress,
    #[error("no joint consensus is in progress")]
    NoJointConsensus,
    #[error("invalid snapshot chunk: {0}")]
    InvalidSnapshotChunk(String),
    #[error("external snapshot reference is required: snapshot_bytes={snapshot_bytes}, threshold_bytes={threshold_bytes}")]
    ExternalSnapshotRequired {
        snapshot_bytes: u64,
        threshold_bytes: u64,
    },
    #[error("snapshot serialization failed: {0}")]
    SnapshotEncoding(String),
    #[error("snapshot store error: {0}")]
    SnapshotStore(String),
    #[error("invalid data raft log: {0}")]
    InvalidDataRaftLog(String),
    #[error("invalid data raft command: {0}")]
    InvalidDataRaftCommand(String),
}

pub fn serialize_data_raft_log(entry: &DataRaftLogCodecEntry) -> Result<Vec<u8>, RaftError> {
    if entry.wal_sequence == 0 {
        return Err(RaftError::InvalidDataRaftLog(
            "wal sequence must be nonzero".to_string(),
        ));
    }
    let payload = serde_json::to_vec(&entry.command)
        .map_err(|err| RaftError::InvalidDataRaftLog(err.to_string()))?;
    if payload.is_empty() {
        return Err(RaftError::InvalidDataRaftLog(
            "command payload is empty".to_string(),
        ));
    }
    let log_size = if entry.log_size == 0 {
        payload.len() as u64
    } else {
        entry.log_size
    };
    let mut bytes = Vec::with_capacity(DATA_RAFT_LOG_HEADER_LEN + payload.len());
    push_u32_le(&mut bytes, DATA_RAFT_LOG_MAGIC);
    push_u32_le(&mut bytes, DATA_RAFT_CODEC_VERSION);
    push_u64_le(&mut bytes, entry.shard_id);
    push_u64_le(&mut bytes, entry.raft_index);
    push_u64_le(&mut bytes, entry.log_id);
    push_u64_le(&mut bytes, log_size);
    push_u64_le(&mut bytes, entry.wal_sequence);
    push_u64_le(&mut bytes, payload.len() as u64);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn parse_data_raft_log(bytes: &[u8]) -> Result<DataRaftLogCodecEntry, RaftError> {
    if bytes.len() < DATA_RAFT_LOG_HEADER_LEN {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "truncated header: bytes={}, need={}",
            bytes.len(),
            DATA_RAFT_LOG_HEADER_LEN
        )));
    }
    let magic = read_u32_le(bytes, 0)?;
    if magic != DATA_RAFT_LOG_MAGIC {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "bad magic: {magic:#x}"
        )));
    }
    let version = read_u32_le(bytes, 4)?;
    if version != DATA_RAFT_CODEC_VERSION {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "unsupported version: {version}"
        )));
    }
    let shard_id = read_u64_le(bytes, 8)?;
    let raft_index = read_u64_le(bytes, 16)?;
    let log_id = read_u64_le(bytes, 24)?;
    let log_size = read_u64_le(bytes, 32)?;
    let wal_sequence = read_u64_le(bytes, 40)?;
    let payload_size = read_u64_le(bytes, 48)?;
    if wal_sequence == 0 {
        return Err(RaftError::InvalidDataRaftLog(
            "wal sequence must be nonzero".to_string(),
        ));
    }
    if payload_size > u32::MAX as u64 {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "payload too large: {payload_size}"
        )));
    }
    let payload_size_usize = payload_size as usize;
    if bytes.len() - DATA_RAFT_LOG_HEADER_LEN != payload_size_usize {
        return Err(RaftError::InvalidDataRaftLog(format!(
            "payload size mismatch: header={}, actual={}",
            payload_size,
            bytes.len() - DATA_RAFT_LOG_HEADER_LEN
        )));
    }
    let command = serde_json::from_slice(&bytes[DATA_RAFT_LOG_HEADER_LEN..])
        .map_err(|err| RaftError::InvalidDataRaftLog(err.to_string()))?;
    Ok(DataRaftLogCodecEntry {
        shard_id,
        raft_index,
        log_id,
        log_size,
        wal_sequence,
        command,
    })
}

pub fn serialize_data_raft_command(
    entry: &DataRaftCommandCodecEntry,
) -> Result<Vec<u8>, RaftError> {
    if entry.shard_id == 0 {
        return Err(RaftError::InvalidDataRaftCommand(
            "shard id must be nonzero".to_string(),
        ));
    }
    if entry.commands.is_empty() {
        return Err(RaftError::InvalidDataRaftCommand(
            "command batch is empty".to_string(),
        ));
    }
    let payload = serde_json::to_vec(&entry.commands)
        .map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    if payload.is_empty() {
        return Err(RaftError::InvalidDataRaftCommand(
            "command payload is empty".to_string(),
        ));
    }
    let mut bytes = Vec::with_capacity(DATA_RAFT_COMMAND_HEADER_LEN + payload.len());
    push_u32_le(&mut bytes, DATA_RAFT_COMMAND_MAGIC);
    push_u32_le(&mut bytes, DATA_RAFT_CODEC_VERSION);
    push_u64_le(&mut bytes, entry.shard_id);
    push_u64_le(&mut bytes, entry.raft_index);
    push_u64_le(&mut bytes, entry.request_id);
    push_u64_le(&mut bytes, payload.len() as u64);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn parse_data_raft_command(bytes: &[u8]) -> Result<DataRaftCommandCodecEntry, RaftError> {
    if bytes.len() < DATA_RAFT_COMMAND_HEADER_LEN {
        return Err(RaftError::InvalidDataRaftCommand(format!(
            "truncated header: bytes={}, need={}",
            bytes.len(),
            DATA_RAFT_COMMAND_HEADER_LEN
        )));
    }
    let magic =
        read_u32_le(bytes, 0).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    if magic != DATA_RAFT_COMMAND_MAGIC {
        return Err(RaftError::InvalidDataRaftCommand(format!(
            "bad magic: {magic:#x}"
        )));
    }
    let version =
        read_u32_le(bytes, 4).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    if version != DATA_RAFT_CODEC_VERSION {
        return Err(RaftError::InvalidDataRaftCommand(format!(
            "unsupported version: {version}"
        )));
    }
    let shard_id =
        read_u64_le(bytes, 8).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    let raft_index =
        read_u64_le(bytes, 16).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    let request_id =
        read_u64_le(bytes, 24).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    let payload_size =
        read_u64_le(bytes, 32).map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    if shard_id == 0 {
        return Err(RaftError::InvalidDataRaftCommand(
            "shard id must be nonzero".to_string(),
        ));
    }
    let payload_size_usize = usize::try_from(payload_size).map_err(|_| {
        RaftError::InvalidDataRaftCommand(format!("payload too large: {payload_size}"))
    })?;
    if bytes.len() - DATA_RAFT_COMMAND_HEADER_LEN != payload_size_usize {
        return Err(RaftError::InvalidDataRaftCommand(format!(
            "payload size mismatch: header={}, actual={}",
            payload_size,
            bytes.len() - DATA_RAFT_COMMAND_HEADER_LEN
        )));
    }
    let commands = serde_json::from_slice(&bytes[DATA_RAFT_COMMAND_HEADER_LEN..])
        .map_err(|err| RaftError::InvalidDataRaftCommand(err.to_string()))?;
    Ok(DataRaftCommandCodecEntry {
        shard_id,
        raft_index,
        request_id,
        commands,
    })
}

fn push_u32_le(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64_le(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32, RaftError> {
    let end = offset.saturating_add(4);
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| RaftError::InvalidDataRaftLog("truncated u32".to_string()))?;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("u32 slice length"),
    ))
}

fn read_u64_le(bytes: &[u8], offset: usize) -> Result<u64, RaftError> {
    let end = offset.saturating_add(8);
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| RaftError::InvalidDataRaftLog("truncated u64".to_string()))?;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("u64 slice length"),
    ))
}

pub fn estimate_snapshot_bytes(snapshot: &RaftSnapshot) -> Result<u64, RaftError> {
    serde_json::to_vec(snapshot)
        .map(|bytes| bytes.len() as u64)
        .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))
}

pub fn decide_snapshot_transfer(
    snapshot: &RaftSnapshot,
    policy: RaftSnapshotTransferPolicy,
    external_snapshot_ref: Option<RaftExternalSnapshotRef>,
) -> Result<RaftSnapshotTransferDecision, RaftError> {
    let threshold = policy.external_threshold_bytes.max(1);
    let snapshot_bytes = estimate_snapshot_bytes(snapshot)?;
    if snapshot_bytes <= threshold && policy.allow_peer_streaming {
        return Ok(RaftSnapshotTransferDecision {
            mode: RaftSnapshotTransferMode::PeerStreaming,
            snapshot_bytes,
            threshold_bytes: threshold,
            external_snapshot_ref: None,
        });
    }
    if policy.allow_external_store {
        let Some(snapshot_ref) = external_snapshot_ref else {
            return Err(RaftError::ExternalSnapshotRequired {
                snapshot_bytes,
                threshold_bytes: threshold,
            });
        };
        return Ok(RaftSnapshotTransferDecision {
            mode: RaftSnapshotTransferMode::ExternalStore,
            snapshot_bytes,
            threshold_bytes: threshold,
            external_snapshot_ref: Some(snapshot_ref),
        });
    }
    Err(RaftError::ExternalSnapshotRequired {
        snapshot_bytes,
        threshold_bytes: threshold,
    })
}

pub fn raft_external_ref_from_snapshot_ref(snapshot_ref: &SnapshotRef) -> RaftExternalSnapshotRef {
    RaftExternalSnapshotRef {
        uri: snapshot_ref.uri.clone(),
        checksum: snapshot_ref.checksum.clone(),
        byte_size: snapshot_ref.byte_size,
    }
}

pub fn shard_snapshot_ref_from_snapshot_ref(snapshot_ref: &SnapshotRef) -> ShardSnapshotRef {
    ShardSnapshotRef {
        uri: snapshot_ref.uri.clone(),
        checksum: snapshot_ref.checksum.clone(),
        byte_size: snapshot_ref.byte_size,
        last_log_index: snapshot_ref.last_log_index,
        created_at_ms: snapshot_ref.created_at.timestamp_millis().max(0) as u64,
    }
}

fn validate_downloaded_snapshot_ref(
    expected_shard_id: ShardId,
    snapshot_ref: &ShardSnapshotRef,
    manifest: &temporalstore_snapshot::SnapshotManifest,
    index_bytes: &[u8],
) -> Result<(), RaftError> {
    if manifest.shard_id != expected_shard_id {
        return Err(RaftError::SnapshotStore(format!(
            "snapshot shard mismatch: manifest={}, ref={}",
            manifest.shard_id, expected_shard_id
        )));
    }
    if manifest.last_log_index != snapshot_ref.last_log_index {
        return Err(RaftError::SnapshotStore(format!(
            "snapshot log index mismatch: manifest={}, ref={}",
            manifest.last_log_index, snapshot_ref.last_log_index
        )));
    }
    let index_entry = manifest
        .checksums
        .iter()
        .find(|entry| entry.relative_path == "index.bin")
        .ok_or_else(|| {
            RaftError::SnapshotStore("snapshot manifest missing index.bin".to_string())
        })?;
    if index_bytes.len() as u64 != index_entry.byte_size {
        return Err(RaftError::SnapshotStore(format!(
            "snapshot index byte size mismatch: actual={}, manifest={}",
            index_bytes.len(),
            index_entry.byte_size
        )));
    }
    let index_checksum = hex::encode(Sha256::digest(index_bytes));
    if index_checksum != index_entry.sha256 {
        return Err(RaftError::SnapshotStore(format!(
            "snapshot index checksum mismatch: actual={index_checksum}, manifest={}",
            index_entry.sha256
        )));
    }

    let mut total_bytes = 0;
    let mut aggregate = Sha256::new();
    for entry in &manifest.checksums {
        total_bytes += entry.byte_size;
        aggregate.update(entry.sha256.as_bytes());
    }
    if total_bytes != snapshot_ref.byte_size {
        return Err(RaftError::SnapshotStore(format!(
            "snapshot byte size mismatch: manifest={}, ref={}",
            total_bytes, snapshot_ref.byte_size
        )));
    }
    let aggregate_checksum = hex::encode(aggregate.finalize());
    if aggregate_checksum != snapshot_ref.checksum {
        return Err(RaftError::SnapshotStore(format!(
            "snapshot checksum mismatch: manifest={aggregate_checksum}, ref={}",
            snapshot_ref.checksum
        )));
    }
    Ok(())
}

#[derive(Clone)]
pub struct RaftCluster {
    inner: Arc<RwLock<RaftClusterInner>>,
    /// The per-follower senders, built on first use and rebuilt when the peer set changes.
    /// Shared by clone: every handle to a cluster must ring the same senders.
    follower_pipeline: Arc<Mutex<Option<follower_pipeline::FollowerPipeline>>>,
    /// Bumped whenever the quorum commit advances; proposers wait on it instead of counting
    /// acknowledgements themselves.
    commit_signal: Arc<(Mutex<u64>, Condvar)>,
}

impl std::fmt::Debug for RaftCluster {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RaftCluster").finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct RaftClusterInner {
    /// Indices some proposer is waiting on; apply records their responses so each
    /// waiter gets its own command's answer rather than whichever applied last.
    response_waiters: BTreeSet<u64>,
    /// Responses captured for registered waiters, taken by index.
    pending_responses: BTreeMap<u64, CommandResponse>,
    shard_id: ShardId,
    leader_id: RaftNodeId,
    nodes: BTreeMap<RaftNodeId, RaftNode>,
    config: RaftConfig,
    wal: Option<LocalRaftWal>,
    logical_time_ms: u64,
    leader_lease_deadline_ms: u64,
    election_elapsed_tick: u64,
    joint_membership: Option<JointConsensusMembership>,
    latest_external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    pending_snapshots: BTreeMap<(RaftNodeId, String), PendingSnapshotChunks>,
    read_safety_state: RaftReadSafetyRuntimeState,
    membership_evidence: RaftMembershipRuntimeEvidence,
    /// P1: last durable fingerprint persisted per node (see `raft_durable_fingerprint`). Only
    /// consulted when `TS_RAFT_WAL_COALESCE` is on; otherwise left untouched.
    last_durable_fingerprint: BTreeMap<RaftNodeId, u64>,
    /// P2: serialize proposes into the log in order under `TS_RAFT_PROPOSE_SERIALIZE`.
    propose_serialize: Arc<Mutex<()>>,
    /// P3/P6: which node THIS process actually is, when the deployed runtime declares it.
    /// `None` for the in-process test cluster, which genuinely hosts every node -- so both of
    /// the local-only narrowings key off this and are inert there.
    local_node_id: Option<RaftNodeId>,
    /// P4: the thread currently owing durability barriers instead of taking them, set for the
    /// span of one propose. Thread-scoped ON PURPOSE. The propose path releases the cluster
    /// write lock for its network phase, so another path can persist inside this window -- and
    /// one of them is the vote grant, which must be durable BEFORE it is answered or a crash
    /// lets the same term be voted twice and elect two leaders. Only the thread that opened the
    /// deferral defers; every other path takes its real barrier as before.
    persist_deferred_owner: Option<std::thread::ThreadId>,
    /// P4: whether anything was actually deferred, so a propose that persisted nothing flushes
    /// nothing.
    persist_dirty: bool,
    /// Bumped on every accepted AppendEntries. A follower's timer loop watches it to
    /// tell "the leader is quiet" from "the leader is fine"; nothing else in the data
    /// raft path observes leader liveness at all.
    leader_contact_epoch: u64,
    /// True for the in-process cluster model, where `tick_election` may promote any
    /// node from local shadow state. The production runtime clears it: there, an
    /// election has to be won over the wire, so a local promotion of a *remote* node
    /// would just make this process disagree with the rest of the group.
    local_shadow_election: bool,
}

impl RaftCluster {
    /// Tell the cluster which node THIS process actually is. Set by the deployed production
    /// runtime, which hosts exactly one node per process; left unset by the in-process test
    /// cluster, which hosts every node. Everything scoped to the local node keys off this, so
    /// leaving it unset preserves whole-cluster behaviour exactly.
    pub fn set_local_node_id(&self, node_id: RaftNodeId) {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.local_node_id = Some(node_id);
    }

    /// Local single-shard Raft model for unit tests and validation harnesses.
    ///
    /// Production runtime/deployment paths must use the distributed production
    /// Raft runtime and are rejected by readiness if they select this local model.
    pub fn new_single_shard(
        shard_id: ShardId,
        node_ids: impl IntoIterator<Item = RaftNodeId>,
    ) -> Self {
        Self::new_single_shard_with_config(shard_id, node_ids, RaftConfig::default())
            .expect("default raft config must be valid")
    }

    /// Local single-shard Raft model with explicit config for tests/harnesses.
    pub fn new_single_shard_with_config(
        shard_id: ShardId,
        node_ids: impl IntoIterator<Item = RaftNodeId>,
        config: RaftConfig,
    ) -> Result<Self, RaftError> {
        config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        let mut nodes = BTreeMap::new();
        let mut iter = node_ids.into_iter();
        let leader_id = iter.next().unwrap_or(1);
        nodes.insert(leader_id, new_node(leader_id, RaftRole::Leader, shard_id));
        for node_id in iter {
            nodes.insert(node_id, new_node(node_id, RaftRole::Follower, shard_id));
        }
        let leader_lease_deadline_ms = initial_leader_lease_deadline_ms(&config);
        Ok(Self {
            follower_pipeline: Arc::new(Mutex::new(None)),
            commit_signal: Arc::new((Mutex::new(0), Condvar::new())),
            inner: Arc::new(RwLock::new(RaftClusterInner {
                response_waiters: BTreeSet::new(),
                pending_responses: BTreeMap::new(),
                shard_id,
                leader_id,
                nodes,
                config,
                wal: None,
                logical_time_ms: 0,
                leader_lease_deadline_ms,
                election_elapsed_tick: 0,
                joint_membership: None,
                latest_external_snapshot_ref: None,
                pending_snapshots: BTreeMap::new(),
                read_safety_state: RaftReadSafetyRuntimeState::default(),
                membership_evidence: RaftMembershipRuntimeEvidence::default(),
                last_durable_fingerprint: BTreeMap::new(),
                propose_serialize: Arc::new(Mutex::new(())),
                local_node_id: None,
                persist_deferred_owner: None,
                persist_dirty: false,
                leader_contact_epoch: 0,
                local_shadow_election: true,
            })),
        })
    }

    /// WAL-backed local Raft fixture for durability tests and harnesses only.
    pub fn new_single_shard_with_wal(
        root: impl AsRef<Path>,
        shard_id: ShardId,
        node_ids: impl IntoIterator<Item = RaftNodeId>,
        config: RaftConfig,
    ) -> Result<Self, RaftError> {
        let cluster = Self::new_single_shard_with_config(shard_id, node_ids, config)?;
        {
            let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
            inner.wal = Some(LocalRaftWal::new(root.as_ref().to_path_buf()));
            inner.persist_configured_wal()?;
        }
        Ok(cluster)
    }

    pub fn restore_single_shard_from_wal(
        root: impl AsRef<Path>,
        shard_id: ShardId,
        node_ids: impl IntoIterator<Item = RaftNodeId>,
        config: RaftConfig,
    ) -> Result<Self, RaftError> {
        config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        let wal = LocalRaftWal::new(root.as_ref().to_path_buf());
        let mut nodes = BTreeMap::new();
        let mut leader_id = None;
        let mut joint_membership = None;
        let mut latest_external_snapshot_ref = None;
        let mut read_safety_state = RaftReadSafetyRuntimeState::default();
        let mut membership_evidence = RaftMembershipRuntimeEvidence::default();
        for node_id in node_ids {
            let record = wal
                .load_node(shard_id, node_id)
                .map_err(|err| RaftError::Wal(err.to_string()))?;
            let mut node = if let Some(record) = record {
                validate_raft_apply_snapshot_fence(&record)?;
                validate_raft_storage_apply_fence(&record)?;
                let mut node = new_node(
                    node_id,
                    if record.membership.leader_id == node_id {
                        RaftRole::Leader
                    } else {
                        RaftRole::Follower
                    },
                    shard_id,
                );
                node.replica_role = record.replica_role;
                node.current_term = record.hard_state.current_term;
                node.voted_for = record.hard_state.voted_for;
                node.commit_index = record.hard_state.commit_index;
                node.pipeline_state = record.pipeline_state;
                if let Some(snapshot) = record.installed_snapshot.clone() {
                    if node.replica_role.can_serve_data() {
                        install_snapshot_state(&mut node, snapshot);
                    } else {
                        node.current_term = node.current_term.max(snapshot.last_included_term);
                        node.commit_index = node.commit_index.max(snapshot.last_included_index);
                        node.applied_index = snapshot.last_included_index;
                        node.max_applied_index = node.max_applied_index.max(snapshot.last_included_index);
                        node.installed_snapshot = Some(snapshot);
                    }
                }
                node.log = record.entries;
                if node.replica_role.can_serve_data() {
                    apply_committed(&mut node);
                }
                leader_id.get_or_insert(record.membership.leader_id);
                if joint_membership.is_none() {
                    joint_membership = record.joint_membership;
                }
                if latest_external_snapshot_ref.is_none() {
                    latest_external_snapshot_ref = record.latest_external_snapshot_ref;
                }
                read_safety_state = record.read_safety_state;
                merge_membership_evidence(&mut membership_evidence, &record.membership_evidence);
                node
            } else {
                new_node(node_id, RaftRole::Follower, shard_id)
            };
            if leader_id == Some(node_id) {
                node.role = RaftRole::Leader;
            }
            nodes.insert(node_id, node);
        }
        let leader_id = leader_id
            .or_else(|| nodes.keys().next().copied())
            .unwrap_or(1);
        if let Some(leader) = nodes.get_mut(&leader_id) {
            leader.role = RaftRole::Leader;
        }
        if joint_membership.is_some() {
            membership_evidence.pending_joint_consensus_restore_count = membership_evidence
                .pending_joint_consensus_restore_count
                .saturating_add(1);
        }
        refresh_all_pipeline_states(&mut nodes, leader_id, None, &config);
        let leader_lease_deadline_ms = initial_leader_lease_deadline_ms(&config);
        Ok(Self {
            follower_pipeline: Arc::new(Mutex::new(None)),
            commit_signal: Arc::new((Mutex::new(0), Condvar::new())),
            inner: Arc::new(RwLock::new(RaftClusterInner {
                response_waiters: BTreeSet::new(),
                pending_responses: BTreeMap::new(),
                shard_id,
                leader_id,
                nodes,
                config,
                wal: Some(wal),
                logical_time_ms: 0,
                leader_lease_deadline_ms,
                election_elapsed_tick: 0,
                joint_membership,
                latest_external_snapshot_ref,
                pending_snapshots: BTreeMap::new(),
                read_safety_state,
                membership_evidence,
                last_durable_fingerprint: BTreeMap::new(),
                propose_serialize: Arc::new(Mutex::new(())),
                local_node_id: None,
                persist_deferred_owner: None,
                persist_dirty: false,
                leader_contact_epoch: 0,
                local_shadow_election: true,
            })),
        })
    }

    /// Take the barriers a set of staged appends owes, holding NO cluster lock.
    ///
    /// The lock is needed to decide WHAT to write and to keep those writes ordered, not to make
    /// them durable: an fsync covers every byte already in the file whoever wrote it. Holding a
    /// lock here would block the next proposer for the length of an fsync and serialise exactly
    /// the writers this is meant to let share a barrier.
    fn finish_staged_unlocked(
        &self,
        staged: Vec<local_wal::StagedWalAppend>,
    ) -> Result<(), RaftError> {
        if staged.is_empty() {
            return Ok(());
        }
        let wal = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            inner.wal.clone()
        };
        let Some(wal) = wal else {
            return Ok(());
        };
        for append in staged {
            wal.finish_staged(append)
                .map_err(|err| RaftError::Wal(err.to_string()))?;
        }
        Ok(())
    }

    pub fn propose(&self, command: Command) -> Result<CommandResponse, RaftError> {
        let limit = self
            .inner
            .read()
            .expect("raft cluster lock poisoned")
            .config
            .max_memory_replicate_log_bytes;
        let chunks = match split_command_for_raft_limit(command, limit) {
            Ok(chunks) => chunks,
            Err(err @ RaftError::LogEntryTooLarge { .. }) => {
                let _ = self.record_leader_oversized_rejection();
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        // Defer the writes, then flush them with no lock held. `propose_one` takes the cluster
        // write lock for its whole body, barrier included, so proposers could never reach the
        // barrier together and each paid for one of its own -- measured flat at 1.0 barriers per
        // write from 1 to 16 concurrent writers. Deferring does not reduce how many writes there
        // are; it moves the barrier out from under the lock so writers can share one. A command
        // split across chunks takes one barrier for the whole command: no chunk is acknowledged
        // until every chunk is written and flushed.
        //
        // The deferral is a per-thread claim recorded in SHARED state, so the span from opening
        // it to staging it must not interleave with another proposer's. Unserialised, two
        // proposers race: A marks the state dirty, B opens its own deferral and clears that flag,
        // and A then stages nothing and returns success for a write no barrier ever covered. The
        // replicated path already holds this same lock for this same span. It costs no throughput
        // here -- `propose_one` serialises on the cluster write lock anyway -- and it is released
        // before the barrier, so the flushes still coalesce.
        let local_propose_gate = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            inner.propose_serialize.clone()
        };
        let local_propose_guard = local_propose_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.inner
            .write()
            .expect("raft cluster lock poisoned")
            .begin_deferred_persist();
        let mut outcome = Ok(CommandResponse::Empty);
        for chunk in chunks {
            match self.propose_one(chunk) {
                Ok(response) => outcome = Ok(response),
                Err(err) => {
                    outcome = Err(err);
                    break;
                }
            }
        }
        let staged = self
            .inner
            .write()
            .expect("raft cluster lock poisoned")
            .stage_deferred_persist();
        // The writes are done and ordered; the barrier needs no ordering, so release before it.
        drop(local_propose_guard);
        // Flush on the failure path too: a propose that failed part-way still owes a barrier for
        // whatever it already wrote, and nothing is returned to the caller before it is taken.
        let flushed = match staged {
            Ok(staged) => self.finish_staged_unlocked(staged),
            Err(err) => Err(err),
        };
        match (outcome, flushed) {
            (Ok(response), Ok(())) => Ok(response),
            (Ok(_), Err(err)) => Err(err),
            (Err(err), _) => Err(err),
        }
    }

    fn propose_one(&self, command: Command) -> Result<CommandResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.ensure_live_leader()?;
        let entry_bytes = command_size_bytes(&command);
        if entry_bytes > inner.config.max_memory_replicate_log_bytes {
            let leader_id = inner.leader_id;
            if let Some(leader) = inner.nodes.get_mut(&leader_id) {
                leader.pipeline_state.oversized_log_rejections = leader
                    .pipeline_state
                    .oversized_log_rejections
                    .saturating_add(1);
                leader.pipeline_state.memory_backpressure_rejections = leader
                    .pipeline_state
                    .memory_backpressure_rejections
                    .saturating_add(1);
            }
            inner.persist_configured_wal()?;
            return Err(RaftError::LogEntryTooLarge {
                bytes: entry_bytes,
                limit: inner.config.max_memory_replicate_log_bytes,
            });
        }
        if let Some((live, required)) = inner.joint_majority_failure() {
            return Err(RaftError::NoMajority { live, required });
        }
        let required = inner.required_majority();
        let live = inner.live_quorum_participants();
        if live < required {
            return Err(RaftError::NoMajority { live, required });
        }
        if !inner.leader_lease_valid() {
            return Err(RaftError::LeaderUnavailable);
        }

        let shard_id = inner.shard_id;
        let leader_id = inner.leader_id;
        let leader = inner
            .nodes
            .get(&leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        let entry = RaftLogEntry {
            term: leader.current_term,
            index: node_next_log_index(leader),
            shard_id,
            command,
        };

        let mut replicated = 0;
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            append_entry(node, entry.clone());
            if node.replica_role.participates_in_quorum() {
                replicated += 1;
            }
        }
        if replicated < required || inner.joint_majority_failure().is_some() {
            return Err(RaftError::NoMajority {
                live: replicated,
                required,
            });
        }

        let mut leader_response = CommandResponse::Empty;
        // P6: every `RaftNode` owns a `TemporalEngine`, so applying a committed entry into every
        // node held by this process drives one engine WAL -- and one durability barrier -- per
        // node. In a deployed process the peers are shadows: the real ones apply in their own
        // processes off AppendEntries, so these applies are pure cost. Apply only into our own
        // engine. Log and commit bookkeeping still advance for every node; only the apply and
        // its barrier are skipped. Read before the mutable borrow below.
        let apply_local_only = inner.local_node_id;
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            node.commit_index = entry.index;
            if !node.replica_role.can_serve_data() {
                continue;
            }
            if let Some(local) = apply_local_only {
                if node.id != local {
                    continue;
                }
            }
            if let Some(response) = apply_committed(node) {
                if node.id == leader_id {
                    leader_response = response;
                }
            }
        }
        if inner
            .nodes
            .values()
            .any(|node| node.pipeline_state.transfer_leader_target)
        {
            inner.membership_evidence.leader_transfer_write_count = inner
                .membership_evidence
                .leader_transfer_write_count
                .saturating_add(1);
            inner
                .membership_evidence
                .leader_transfer_exact_once_commit_count = inner
                .membership_evidence
                .leader_transfer_exact_once_commit_count
                .saturating_add(1);
            if !inner
                .membership_evidence
                .leader_transfer_exact_once_commit_ids
                .contains(&entry.index)
            {
                inner
                    .membership_evidence
                    .leader_transfer_exact_once_commit_ids
                    .push(entry.index);
            }
        }
        let config = inner.config.clone();
        let local_node_id_for_refresh = inner.local_node_id;
        refresh_all_pipeline_states(&mut inner.nodes, leader_id, local_node_id_for_refresh, &config);
        inner.renew_leader_lease();
        inner.persist_configured_wal()?;
        Ok(leader_response)
    }

    pub fn propose_distributed<T>(
        &self,
        command: Command,
        transport: &T,
    ) -> Result<CommandResponse, RaftError>
    where
        T: RaftTransport + Clone + Send + 'static,
    {
        let limit = self
            .inner
            .read()
            .expect("raft cluster lock poisoned")
            .config
            .max_memory_replicate_log_bytes;
        let chunks = match split_command_for_raft_limit(command, limit) {
            Ok(chunks) => chunks,
            Err(err @ RaftError::LogEntryTooLarge { .. }) => {
                let _ = self.record_leader_oversized_rejection();
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        let mut last_response = CommandResponse::Empty;
        for chunk in chunks {
            last_response = self.propose_distributed_one(chunk, transport)?;
        }
        Ok(last_response)
    }

    fn record_leader_oversized_rejection(&self) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader_id = inner.leader_id;
        if let Some(leader) = inner.nodes.get_mut(&leader_id) {
            leader.pipeline_state.oversized_log_rejections = leader
                .pipeline_state
                .oversized_log_rejections
                .saturating_add(1);
            leader.pipeline_state.memory_backpressure_rejections = leader
                .pipeline_state
                .memory_backpressure_rejections
                .saturating_add(1);
        }
        inner.persist_configured_wal()
    }

    fn propose_distributed_one<T>(
        &self,
        command: Command,
        transport: &T,
    ) -> Result<CommandResponse, RaftError>
    where
        T: RaftTransport + Clone + Send + 'static,
    {
        // With the senders on, proposers skip the serialize gate entirely: order to any one
        // follower is that follower's single sender thread, and entry order is the write
        // lock's index assignment. Serializing proposers here starved the senders of batches --
        // every entry paid the whole doorbell -> sender -> ack -> commit-signal chain alone,
        // which is exactly the concurrency collapse measured on small machines. =0 falls back
        // to one propose at a time through the same senders.
        if follower_pipeline::follower_pipeline_enabled()
            && raft_env_flag_default_on("TS_RAFT_PIPELINE_CONCURRENT_PROPOSE")
        {
            return self.propose_pipelined_concurrent(command, transport);
        }
        // P2: serialize proposes into the log in order. Concurrent proposers otherwise append
        // under the write lock (sequential indices) but release it before the async network phase,
        // so their AppendEntries race and can reach a follower out of order -> `prev_log` mismatch
        // -> reject -> a full-deadline stall. Holding this per-cluster lock across the whole
        // append+replicate+commit critical section forces index N to commit before N+1 begins.
        let propose_gate = if raft_propose_serialize_on() {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            Some(inner.propose_serialize.clone())
        } else {
            None
        };
        let _propose_guard = propose_gate
            .as_ref()
            .map(|gate| gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        // P4: the propose lock makes us the only proposer, so nested persists can record that a
        // barrier is owed and let one flush below cover the final state -- before the ack. Still
        // conditioned on actually holding that lock: without it there can be several proposers,
        // and a shared deferral would then have no single owner.
        let deferring = _propose_guard.is_some();
        if deferring {
            self.inner
                .write()
                .expect("raft cluster lock poisoned")
                .begin_deferred_persist();
        }
        let mut entry_barrier = None;
        // The senders replicate when the pipeline is on; this thread then appends, rings them,
        // and waits on the quorum-commit signal instead of sending to any peer itself. Branching
        // here keeps both paths inside the same deferral bookkeeping: the caller-side staging
        // and barrier-join below are what propose_pipelined leaves for its caller, exactly as
        // the fan-out body does.
        let outcome = if follower_pipeline::follower_pipeline_enabled() {
            self.propose_pipelined(command, transport, &mut entry_barrier)
        } else {
            self.propose_distributed_one_locked(command, transport, &mut entry_barrier)
        };
        if !deferring {
            // An overlapped barrier is still joined before the ack, whatever path leaves here.
            if let Some(handle) = entry_barrier {
                let joined = handle
                    .join()
                    .unwrap_or_else(|_| Err(RaftError::Wal("barrier thread panicked".to_string())));
                return match (outcome, joined) {
                    (Ok(response), Ok(())) => Ok(response),
                    (Ok(_), Err(err)) => Err(err),
                    (Err(err), _) => Err(err),
                };
            }
            return outcome;
        }
        // Write what the deferral owes while the propose lock still orders it. Records carry the
        // log, so an older record landing after a newer one would regress the log on recovery --
        // the write must stay ordered.
        //
        // With the barrier overlapped, the entry is already written and being flushed, and what
        // is owed here is only the commit index -- which is recoverable and so is left for the
        // next record to carry rather than paid for on this write's critical path.
        let staged = if entry_barrier.is_some() {
            self.inner
                .write()
                .expect("raft cluster lock poisoned")
                .discard_deferred_persist();
            Ok(Vec::new())
        } else {
            self.inner
                .write()
                .expect("raft cluster lock poisoned")
                .stage_deferred_persist()
        };
        // The barrier needs no such ordering: an fsync makes every byte already in the file
        // durable whoever wrote it. Releasing the lock here is what finally gives group commit a
        // queue to coalesce -- while the barrier was taken under this lock only one writer ever
        // reached it, and barriers per write stayed flat however many writers there were.
        // Nothing is acknowledged before its barrier: the flush still happens below, on the
        // failure path as well as the success path.
        drop(_propose_guard);
        let mut flushed = match staged {
            Ok(staged) => self.finish_staged_unlocked(staged),
            Err(err) => Err(err),
        };
        // Join the overlapped barrier before acknowledging: the entry has to be durable here, it
        // just did not have to wait for replication to start becoming so.
        if let Some(handle) = entry_barrier {
            let joined = handle
                .join()
                .unwrap_or_else(|_| Err(RaftError::Wal("barrier thread panicked".to_string())));
            if flushed.is_ok() {
                flushed = joined;
            }
        }
        // Never ack a write whose barrier failed: a flush error wins over a successful propose.
        match (outcome, flushed) {
            (Ok(response), Ok(())) => Ok(response),
            (Ok(_), Err(err)) => Err(err),
            (Err(err), _) => Err(err),
        }
    }

    /// Propose through the per-follower senders WITHOUT the propose-serialize gate.
    ///
    /// The gate exists for the fan-out, where concurrent proposers each send their own
    /// AppendEntries and the network can reorder them. The senders make it not just
    /// unnecessary but harmful: with one sender thread per follower carrying every append in
    /// log order, serializing proposers guarantees the senders never see more than one new
    /// entry at a time, so each propose pays the whole doorbell -> sender -> ack ->
    /// commit-signal chain by itself. Appending concurrently under the write lock keeps index
    /// order, the senders batch whatever has accumulated, and one wake chain carries every
    /// waiting proposer.
    ///
    /// Durability: the entry's record bytes are staged under the write lock (which orders
    /// them) and the barrier is taken on a side thread while the senders replicate, then
    /// joined before the ack -- nothing is acknowledged before its bytes are durable, and
    /// concurrent proposers share barriers through the WAL flush gate. The commit index stays
    /// recoverable and is never waited on. No deferral bookkeeping: that machinery assumes one
    /// owning proposer, which is the constraint this path removes.
    fn propose_pipelined_concurrent<T>(
        &self,
        command: Command,
        transport: &T,
    ) -> Result<CommandResponse, RaftError>
    where
        T: RaftTransport + Clone + Send + 'static,
    {
        self.ensure_follower_pipeline(transport);
        let (entry_index, staged) = {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.ensure_live_leader()?;
            let entry_bytes = command_size_bytes(&command);
            if entry_bytes > inner.config.max_memory_replicate_log_bytes {
                let leader_id = inner.leader_id;
                if let Some(leader) = inner.nodes.get_mut(&leader_id) {
                    leader.pipeline_state.oversized_log_rejections = leader
                        .pipeline_state
                        .oversized_log_rejections
                        .saturating_add(1);
                    leader.pipeline_state.memory_backpressure_rejections = leader
                        .pipeline_state
                        .memory_backpressure_rejections
                        .saturating_add(1);
                }
                inner.persist_configured_wal()?;
                return Err(RaftError::LogEntryTooLarge {
                    bytes: entry_bytes,
                    limit: inner.config.max_memory_replicate_log_bytes,
                });
            }
            if let Some((live, required)) = inner.joint_majority_failure() {
                return Err(RaftError::NoMajority { live, required });
            }
            let required = inner.required_majority();
            let live = inner.live_quorum_participants();
            if live < required {
                return Err(RaftError::NoMajority { live, required });
            }
            if !inner.leader_lease_valid() {
                return Err(RaftError::LeaderUnavailable);
            }
            let leader_id = inner.leader_id;
            let shard_id = inner.shard_id;
            let leader = inner
                .nodes
                .get(&leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            let entry = RaftLogEntry {
                term: leader.current_term,
                index: node_next_log_index(leader),
                shard_id,
                command,
            };
            let index = entry.index;
            let leader = inner
                .nodes
                .get_mut(&leader_id)
                .ok_or(RaftError::LeaderUnavailable)?;
            append_entry(leader, entry);
            inner.response_waiters.insert(index);
            // Write the record's bytes while the lock orders them; the barrier happens below,
            // off the lock, shared with every concurrent proposer through the flush gate.
            let staged = inner.stage_configured_wal()?;
            (index, staged)
        };

        // The leader's disk wait runs alongside the followers' replication, exactly like the
        // fan-out's overlapped barrier -- joined before the ack, never skipped.
        let entry_barrier = if staged.is_empty() {
            None
        } else {
            let cluster = self.clone();
            Some(thread::spawn(move || cluster.finish_staged_unlocked(staged)))
        };

        self.ring_replication();

        let deadline_ms = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let configured = inner.config.replication_deadline_ms;
            if configured == 0 {
                default_replication_deadline_ms()
            } else {
                configured
            }
        };
        let committed =
            self.wait_for_quorum_commit(entry_index, Duration::from_millis(deadline_ms));

        let outcome = {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.response_waiters.remove(&entry_index);
            if !committed {
                inner.pending_responses.remove(&entry_index);
                let required = inner.required_majority();
                let live = inner.live_quorum_participants();
                // The entry stays in the log and may commit later; the caller only learns that
                // it did not commit within the deadline, which is all a replication deadline
                // ever meant.
                Err(RaftError::NoMajority { live, required })
            } else {
                Ok(inner
                    .pending_responses
                    .remove(&entry_index)
                    .unwrap_or(CommandResponse::Empty))
            }
        };

        // Never ack a write whose barrier failed, and never report a flush error as success.
        let flushed = match entry_barrier {
            Some(handle) => handle
                .join()
                .unwrap_or_else(|_| Err(RaftError::Wal("barrier thread panicked".to_string()))),
            None => Ok(()),
        };
        match (outcome, flushed) {
            (Ok(response), Ok(())) => Ok(response),
            (Ok(_), Err(err)) => Err(err),
            (Err(err), _) => Err(err),
        }
    }

    /// Propose through the per-follower senders: append under the lock, ring the senders,
    /// wait for the quorum commit signal. This path never sends to a peer itself -- one sender
    /// per follower does, which is what keeps that follower's appends in order -- and the commit
    /// index reaches followers on their next append or heartbeat instead of a dedicated second
    /// round trip per propose.
    fn propose_pipelined<T>(
        &self,
        command: Command,
        transport: &T,
        entry_barrier: &mut Option<thread::JoinHandle<Result<(), RaftError>>>,
    ) -> Result<CommandResponse, RaftError>
    where
        T: RaftTransport + Clone + Send + 'static,
    {
        self.ensure_follower_pipeline(transport);
        let entry_index = {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.ensure_live_leader()?;
            let entry_bytes = command_size_bytes(&command);
            if entry_bytes > inner.config.max_memory_replicate_log_bytes {
                let leader_id = inner.leader_id;
                if let Some(leader) = inner.nodes.get_mut(&leader_id) {
                    leader.pipeline_state.oversized_log_rejections = leader
                        .pipeline_state
                        .oversized_log_rejections
                        .saturating_add(1);
                    leader.pipeline_state.memory_backpressure_rejections = leader
                        .pipeline_state
                        .memory_backpressure_rejections
                        .saturating_add(1);
                }
                inner.persist_configured_wal()?;
                return Err(RaftError::LogEntryTooLarge {
                    bytes: entry_bytes,
                    limit: inner.config.max_memory_replicate_log_bytes,
                });
            }
            if let Some((live, required)) = inner.joint_majority_failure() {
                return Err(RaftError::NoMajority { live, required });
            }
            let required = inner.required_majority();
            let live = inner.live_quorum_participants();
            if live < required {
                return Err(RaftError::NoMajority { live, required });
            }
            if !inner.leader_lease_valid() {
                return Err(RaftError::LeaderUnavailable);
            }
            let leader_id = inner.leader_id;
            let shard_id = inner.shard_id;
            let leader = inner
                .nodes
                .get(&leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            let entry = RaftLogEntry {
                term: leader.current_term,
                index: node_next_log_index(leader),
                shard_id,
                command,
            };
            let index = entry.index;
            let leader = inner
                .nodes
                .get_mut(&leader_id)
                .ok_or(RaftError::LeaderUnavailable)?;
            append_entry(leader, entry);
            inner.response_waiters.insert(index);
            index
        };

        // The entry is in the log. Write it and start its barrier NOW, so the leader's disk
        // wait runs alongside the followers' instead of after them. Unchanged from the fan-out
        // path: the commit index is recoverable and never has to be durable before the ack.
        if wal_proto::overlap_leader_barrier_enabled() {
            let staged = self
                .inner
                .write()
                .expect("raft cluster lock poisoned")
                .stage_deferred_persist();
            if let Ok(staged) = staged {
                if !staged.is_empty() {
                    let cluster = self.clone();
                    *entry_barrier =
                        Some(thread::spawn(move || cluster.finish_staged_unlocked(staged)));
                }
            }
            self.inner
                .write()
                .expect("raft cluster lock poisoned")
                .begin_deferred_persist();
        }

        self.ring_replication();

        let deadline_ms = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let configured = inner.config.replication_deadline_ms;
            if configured == 0 {
                default_replication_deadline_ms()
            } else {
                configured
            }
        };
        let committed =
            self.wait_for_quorum_commit(entry_index, Duration::from_millis(deadline_ms));

        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.response_waiters.remove(&entry_index);
        if !committed {
            inner.pending_responses.remove(&entry_index);
            let required = inner.required_majority();
            let live = inner.live_quorum_participants();
            // The entry stays in the log and may commit later; the caller only learns that it
            // did not commit within the deadline, which is all a replication deadline ever meant.
            return Err(RaftError::NoMajority { live, required });
        }
        let response = inner
            .pending_responses
            .remove(&entry_index)
            .unwrap_or(CommandResponse::Empty);
        // No persist here: the commit advance already persisted the record that carries the new
        // commit index, and the outer deferral flushes what this propose itself owes. A persist
        // here charged every proposer a duplicate barrier.
        Ok(response)
    }

    fn propose_distributed_one_locked<T>(
        &self,
        command: Command,
        transport: &T,
        entry_barrier: &mut Option<thread::JoinHandle<Result<(), RaftError>>>,
    ) -> Result<CommandResponse, RaftError>
    where
        T: RaftTransport + Clone + Send + 'static,
    {
        let (entry, leader_id, target_ids, required) = {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.ensure_live_leader()?;
            let entry_bytes = command_size_bytes(&command);
            if entry_bytes > inner.config.max_memory_replicate_log_bytes {
                let leader_id = inner.leader_id;
                if let Some(leader) = inner.nodes.get_mut(&leader_id) {
                    leader.pipeline_state.oversized_log_rejections = leader
                        .pipeline_state
                        .oversized_log_rejections
                        .saturating_add(1);
                    leader.pipeline_state.memory_backpressure_rejections = leader
                        .pipeline_state
                        .memory_backpressure_rejections
                        .saturating_add(1);
                }
                inner.persist_configured_wal()?;
                return Err(RaftError::LogEntryTooLarge {
                    bytes: entry_bytes,
                    limit: inner.config.max_memory_replicate_log_bytes,
                });
            }
            if let Some((live, required)) = inner.joint_majority_failure() {
                return Err(RaftError::NoMajority { live, required });
            }
            let required = inner.required_majority();
            let live = inner.live_quorum_participants();
            if live < required {
                return Err(RaftError::NoMajority { live, required });
            }
            if !inner.leader_lease_valid() {
                return Err(RaftError::LeaderUnavailable);
            }
            let leader_id = inner.leader_id;
            let shard_id = inner.shard_id;
            let leader = inner
                .nodes
                .get(&leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            let entry = RaftLogEntry {
                term: leader.current_term,
                index: node_next_log_index(leader),
                shard_id,
                command,
            };
            let leader = inner
                .nodes
                .get_mut(&leader_id)
                .ok_or(RaftError::LeaderUnavailable)?;
            append_entry(leader, entry.clone());
            let (live_target_ids, fallback_target_ids): (Vec<_>, Vec<_>) = inner
                .nodes
                .iter()
                .filter(|(node_id, _)| **node_id != leader_id)
                .map(|(node_id, node)| (*node_id, node.alive))
                .partition(|(_, alive)| *alive);
            let mut target_ids = live_target_ids;
            target_ids.extend(fallback_target_ids);
            (entry, leader_id, target_ids, required)
        };

        // The entry is in the log. Write it and start its barrier NOW, so the leader's disk wait
        // runs alongside the followers' instead of after them. The commit index is not known yet
        // and does not need to be: it is recoverable, so it never has to be durable before the
        // acknowledgement.
        if wal_proto::overlap_leader_barrier_enabled() {
            let staged = self
                .inner
                .write()
                .expect("raft cluster lock poisoned")
                .stage_deferred_persist();
            if let Ok(staged) = staged {
                if !staged.is_empty() {
                    let cluster = self.clone();
                    *entry_barrier =
                        Some(thread::spawn(move || cluster.finish_staged_unlocked(staged)));
                }
            }
            // Anything persisted from here on is the commit index, which this path discards.
            self.inner
                .write()
                .expect("raft cluster lock poisoned")
                .begin_deferred_persist();
        }

        let mut replicated = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            inner
                .nodes
                .get(&leader_id)
                .map(|node| usize::from(node.replica_role.participates_in_quorum()))
                .unwrap_or_default()
        };
        let mut successful_targets = Vec::new();
        let mut failed_targets = Vec::new();
        let mut fallback_targets = Vec::new();
        let mut live_requests = Vec::new();
        let mut live_target_ids = Vec::new();
        for (target_id, alive) in target_ids {
            if !alive {
                fallback_targets.push(target_id);
                continue;
            }
            let request = self.build_append_entries_request(target_id)?;
            live_target_ids.push(target_id);
            live_requests.push((target_id, request));
        }
        let (tx, rx) = mpsc::channel();
        for (target_id, request) in live_requests {
            let transport = transport.clone();
            let tx = tx.clone();
            thread::spawn(move || {
                let _ = tx.send((target_id, transport.append_entries(request)));
            });
        }
        drop(tx);
        // P2: configurable replication deadline (legacy hardcoded 5 s). A config that leaves the
        // field at 0 resolves to the 5000 ms default so behavior stays byte-identical.
        let replication_deadline_ms = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let configured = inner.config.replication_deadline_ms;
            if configured == 0 {
                default_replication_deadline_ms()
            } else {
                configured
            }
        };
        let replication_deadline =
            Instant::now() + Duration::from_millis(replication_deadline_ms);
        while replicated < required {
            let remaining = replication_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let Ok((target_id, result)) =
                rx.recv_timeout(remaining.min(Duration::from_millis(250)))
            else {
                if Instant::now() >= replication_deadline {
                    break;
                }
                continue;
            };
            match result {
                Ok(response) if response.success && response.match_index >= entry.index => {
                    let _ = self.record_append_entries_response(target_id, &response);
                    let counts_for_quorum = self
                        .inner
                        .read()
                        .expect("raft cluster lock poisoned")
                        .nodes
                        .get(&target_id)
                        .map(|node| node.replica_role.participates_in_quorum())
                        .unwrap_or(false);
                    if counts_for_quorum {
                        replicated += 1;
                    }
                    successful_targets.push(target_id);
                }
                Ok(response) => {
                    let _ = self.record_append_entries_response(target_id, &response);
                    failed_targets.push(target_id);
                }
                Err(_) => {
                    let _ = self.record_append_entries_send_failure(target_id);
                    failed_targets.push(target_id);
                }
            }
        }
        while let Ok((target_id, result)) = rx.try_recv() {
            match result {
                Ok(response) if response.success && response.match_index >= entry.index => {
                    let _ = self.record_append_entries_response(target_id, &response);
                    if !successful_targets.contains(&target_id) {
                        successful_targets.push(target_id);
                    }
                }
                Ok(response) => {
                    let _ = self.record_append_entries_response(target_id, &response);
                    if !failed_targets.contains(&target_id) {
                        failed_targets.push(target_id);
                    }
                }
                Err(_) => {
                    let _ = self.record_append_entries_send_failure(target_id);
                    if !failed_targets.contains(&target_id) {
                        failed_targets.push(target_id);
                    }
                }
            }
        }
        failed_targets.extend(fallback_targets);
        if replicated < required {
            for target_id in failed_targets {
                if transport
                    .install_snapshot(self.build_install_snapshot_request(target_id)?)
                    .map(|response| response.success)
                    .unwrap_or(false)
                {
                    let retry_request = {
                        let inner = self.inner.read().expect("raft cluster lock poisoned");
                        let leader = inner
                            .nodes
                            .get(&leader_id)
                            .ok_or(RaftError::LeaderUnavailable)?;
                        let prev_log_index = entry.index.saturating_sub(1);
                        let prev_log_term =
                            node_term_at_log_or_snapshot_index(leader, prev_log_index)
                                .unwrap_or_default();
                        AppendEntriesRequest {
                            rpc: None,
                            shard_id: entry.shard_id,
                            term: entry.term,
                            leader_id,
                            target_id,
                            prev_log_index,
                            prev_log_term,
                            entries: vec![entry.clone()],
                            leader_commit: entry.index,
                        }
                    };
                    if let Ok(response) = transport.append_entries(retry_request) {
                        let success = response.success && response.match_index >= entry.index;
                        let _ = self.record_append_entries_response(target_id, &response);
                        if success {
                            let counts_for_quorum = self
                                .inner
                                .read()
                                .expect("raft cluster lock poisoned")
                                .nodes
                                .get(&target_id)
                                .map(|node| node.replica_role.participates_in_quorum())
                                .unwrap_or(false);
                            if counts_for_quorum {
                                replicated += 1;
                            }
                            let _ = self.catch_up(target_id);
                            successful_targets.push(target_id);
                        }
                    }
                }
            }
        }

        if replicated < required {
            return Err(RaftError::NoMajority {
                live: replicated,
                required,
            });
        }

        let leader_response = {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            let leader = inner
                .nodes
                .get_mut(&leader_id)
                .ok_or(RaftError::LeaderUnavailable)?;
            leader.commit_index = entry.index;
            let response = if leader.replica_role.can_serve_data() {
                apply_committed(leader).unwrap_or(CommandResponse::Empty)
            } else {
                CommandResponse::Empty
            };
            inner.renew_leader_lease();
            inner.persist_configured_wal()?;
            response
        };

        for target_id in &live_target_ids {
            let request = AppendEntriesRequest {
                rpc: None,
                shard_id: entry.shard_id,
                term: entry.term,
                leader_id,
                target_id: *target_id,
                prev_log_index: entry.index,
                prev_log_term: entry.term,
                entries: Vec::new(),
                leader_commit: entry.index,
            };
            let committed = match transport.append_entries(request) {
                Ok(response) => {
                    let success = response.success;
                    let _ = self.record_append_entries_response(*target_id, &response);
                    success
                }
                Err(_) => false,
            };
            if committed {
                let _ = self.catch_up(*target_id);
            }
        }

        Ok(leader_response)
    }




}


fn joint_majority_failure(
    nodes: &BTreeMap<RaftNodeId, RaftNode>,
    membership: &JointConsensusMembership,
) -> Option<(usize, usize)> {
    let old_live = live_voters_in(nodes, &membership.old_voters);
    let old_required = majority(membership.old_voters.len());
    if old_live < old_required {
        return Some((old_live, old_required));
    }
    let new_live = live_voters_in(nodes, &membership.new_voters);
    let new_required = majority(membership.new_voters.len());
    if new_live < new_required {
        return Some((new_live, new_required));
    }
    None
}

fn live_voters_in(nodes: &BTreeMap<RaftNodeId, RaftNode>, voters: &[RaftNodeId]) -> usize {
    voters
        .iter()
        .filter(|node_id| {
            nodes
                .get(node_id)
                .map(|node| node.alive && node.replica_role.participates_in_quorum())
                .unwrap_or(false)
        })
        .count()
}

impl RaftTransport for RaftCluster {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.receive_append_entries(request)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.receive_vote_request(request)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.receive_install_snapshot(request)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        self.receive_install_snapshot_chunk(request)
    }
}

fn node_status(node: &RaftNode, leader_commit_index: u64) -> RaftNodeStatus {
    RaftNodeStatus {
        node_id: node.id,
        role: node.role,
        replica_role: node.replica_role,
        current_term: node.current_term,
        commit_index: node.commit_index,
        last_log_index: node_last_log_or_snapshot_index(node),
        applied_index: node.applied_index,
        alive: node.alive,
        lag: leader_commit_index.saturating_sub(node.commit_index),
    }
}

fn node_last_log_or_snapshot_index(node: &RaftNode) -> u64 {
    node.log
        .last()
        .map(|entry| entry.index)
        .or_else(|| {
            node.installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
        })
        .unwrap_or_default()
}

fn node_next_log_index(node: &RaftNode) -> u64 {
    node_last_log_or_snapshot_index(node).saturating_add(1)
}

fn node_term_at_log_or_snapshot_index(node: &RaftNode, index: u64) -> Option<u64> {
    if index == 0 {
        return Some(0);
    }
    node.log
        .iter()
        .find(|entry| entry.index == index)
        .map(|entry| entry.term)
        .or_else(|| {
            node.installed_snapshot
                .as_ref()
                .filter(|snapshot| snapshot.last_included_index == index)
                .map(|snapshot| snapshot.last_included_term)
        })
}

fn replication_health_from_status(
    status: RaftClusterStatus,
    max_allowed_lag: u64,
) -> RaftReplicationHealth {
    let mut max_lag = 0;
    let mut caught_up_voters = Vec::new();
    let mut lagging_voters = Vec::new();
    for node in status.nodes {
        max_lag = max_lag.max(node.lag);
        if !node.replica_role.participates_in_quorum() {
            continue;
        }
        if node.alive && node.lag <= max_allowed_lag {
            caught_up_voters.push(node.node_id);
        } else {
            lagging_voters.push(RaftReplicaLag {
                node_id: node.node_id,
                lag: node.lag,
                alive: node.alive,
            });
        }
    }
    let healthy = status.has_majority
        && status.leader_lease_valid
        && lagging_voters.is_empty()
        && caught_up_voters.len() >= status.majority;
    RaftReplicationHealth {
        leader_id: status.leader_id,
        leader_commit_index: status.commit_index,
        max_allowed_lag,
        max_lag,
        caught_up_voters,
        lagging_voters,
        live_voters: status.live_voters,
        majority: status.majority,
        healthy,
    }
}

fn raft_apply_health_from_status(
    status: RaftClusterStatus,
    max_allowed_apply_lag: u64,
) -> RaftApplyHealth {
    let mut fully_applied_nodes = Vec::new();
    let mut slow_appliers = Vec::new();
    let mut max_apply_lag = 0;
    for node in &status.nodes {
        if !node.alive || !node.replica_role.can_serve_data() {
            continue;
        }
        let apply_lag = node.commit_index.saturating_sub(node.applied_index);
        max_apply_lag = max_apply_lag.max(apply_lag);
        if apply_lag <= max_allowed_apply_lag && node.applied_index >= status.commit_index {
            fully_applied_nodes.push(node.node_id);
        }
        if apply_lag > max_allowed_apply_lag {
            slow_appliers.push(RaftApplyLag {
                node_id: node.node_id,
                commit_index: node.commit_index,
                applied_index: node.applied_index,
                apply_lag,
                alive: node.alive,
            });
        }
    }
    RaftApplyHealth {
        leader_id: status.leader_id,
        leader_commit_index: status.commit_index,
        max_allowed_apply_lag,
        max_apply_lag,
        fully_applied_nodes,
        healthy: slow_appliers.is_empty(),
        slow_appliers,
    }
}

fn raft_observer_apply_health_from_status(
    status: RaftClusterStatus,
    observer_node_id: RaftNodeId,
    max_allowed_apply_lag: u64,
) -> RaftApplyHealth {
    let Some(node) = status
        .nodes
        .iter()
        .find(|node| node.node_id == observer_node_id)
    else {
        return RaftApplyHealth {
            leader_id: status.leader_id,
            leader_commit_index: status.commit_index,
            max_allowed_apply_lag,
            max_apply_lag: status.commit_index,
            fully_applied_nodes: Vec::new(),
            slow_appliers: vec![RaftApplyLag {
                node_id: observer_node_id,
                commit_index: status.commit_index,
                applied_index: 0,
                apply_lag: status.commit_index,
                alive: false,
            }],
            healthy: false,
        };
    };
    if !node.replica_role.can_serve_data() {
        return RaftApplyHealth {
            leader_id: status.leader_id,
            leader_commit_index: status.commit_index,
            max_allowed_apply_lag,
            max_apply_lag: 0,
            fully_applied_nodes: Vec::new(),
            slow_appliers: Vec::new(),
            healthy: node.alive && status.has_majority && status.leader_lease_valid,
        };
    }
    let apply_lag = node.commit_index.saturating_sub(node.applied_index);
    let fully_applied =
        node.alive && apply_lag <= max_allowed_apply_lag && node.applied_index >= node.commit_index;
    let slow_appliers = if fully_applied {
        Vec::new()
    } else {
        vec![RaftApplyLag {
            node_id: node.node_id,
            commit_index: node.commit_index,
            applied_index: node.applied_index,
            apply_lag,
            alive: node.alive,
        }]
    };
    RaftApplyHealth {
        leader_id: status.leader_id,
        leader_commit_index: status.commit_index,
        max_allowed_apply_lag,
        max_apply_lag: apply_lag,
        fully_applied_nodes: if fully_applied {
            vec![node.node_id]
        } else {
            Vec::new()
        },
        healthy: fully_applied && status.has_majority && status.leader_lease_valid,
        slow_appliers,
    }
}

fn new_node(id: RaftNodeId, role: RaftRole, shard_id: ShardId) -> RaftNode {
    let engine = TemporalEngine::default();
    engine.load_shard(shard_id);
    RaftNode {
        id,
        role,
        replica_role: RaftReplicaRole::Voter,
        current_term: 1,
        voted_for: None,
        commit_index: 0,
        alive: true,
        log: Vec::new(),
        installed_snapshot: None,
        applied_index: 0,
        applied: BTreeSet::new(),
        max_applied_index: 0,
        engine,
        pipeline_state: RaftPeerPipelineRuntimeState {
            next_index: 1,
            ..RaftPeerPipelineRuntimeState::default()
        },
    }
}

fn merge_membership_evidence(
    target: &mut RaftMembershipRuntimeEvidence,
    source: &RaftMembershipRuntimeEvidence,
) {
    target.learner_add_count = target.learner_add_count.max(source.learner_add_count);
    target.learner_catchup_count = target
        .learner_catchup_count
        .max(source.learner_catchup_count);
    target.learner_promote_count = target
        .learner_promote_count
        .max(source.learner_promote_count);
    target.voter_remove_count = target.voter_remove_count.max(source.voter_remove_count);
    target.witness_add_count = target.witness_add_count.max(source.witness_add_count);
    target.auto_promote_count = target.auto_promote_count.max(source.auto_promote_count);
    target.leader_transfer_write_count = target
        .leader_transfer_write_count
        .max(source.leader_transfer_write_count);
    target.leader_transfer_exact_once_commit_count = target
        .leader_transfer_exact_once_commit_count
        .max(source.leader_transfer_exact_once_commit_count);
    let mut commit_ids = target
        .leader_transfer_exact_once_commit_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    commit_ids.extend(source.leader_transfer_exact_once_commit_ids.iter().copied());
    target.leader_transfer_exact_once_commit_ids = commit_ids.into_iter().collect();
    target.pending_joint_consensus_persist_count = target
        .pending_joint_consensus_persist_count
        .max(source.pending_joint_consensus_persist_count);
    target.pending_joint_consensus_restore_count = target
        .pending_joint_consensus_restore_count
        .max(source.pending_joint_consensus_restore_count);
}

fn refresh_all_pipeline_states(
    nodes: &mut BTreeMap<RaftNodeId, RaftNode>,
    leader_id: RaftNodeId,
    local_node_id: Option<RaftNodeId>,
    config: &RaftConfig,
) {
    // `local_node_id` is set only by the deployed runtime, which hosts exactly ONE node.
    let deployed = local_node_id.is_some();
    // This runs on every propose and every accepted AppendEntries. Cloning the leader's
    // whole log here and re-scanning all of it per node made each of those O(log length),
    // which is exactly the growth incremental WAL records were meant to remove. The log is
    // index-ordered, so the bytes still owed to a peer are a suffix: find where that suffix
    // starts and sum only it, against a borrowed log.
    let Some(leader) = nodes.get(&leader_id) else {
        return;
    };
    let leader_commit_index = leader.commit_index;
    let leader_next_index = node_next_log_index(leader);
    let progress = nodes
        .iter()
        .map(|(node_id, node)| {
            (
                *node_id,
                node.commit_index.max(node.pipeline_state.match_index),
            )
        })
        .collect::<Vec<_>>();
    let inflight_bytes = {
        let leader_log = &nodes
            .get(&leader_id)
            .expect("leader present, checked above")
            .log;
        progress
            .into_iter()
            .map(|(node_id, known_progress)| {
                let start = leader_log.partition_point(|entry| entry.index <= known_progress);
                let bytes = leader_log[start..]
                    .iter()
                    .map(|entry| command_size_bytes(&entry.command))
                    .sum::<u64>();
                (node_id, bytes)
            })
            .collect::<BTreeMap<_, _>>()
    };
    for node in nodes.values_mut() {
        let bytes = inflight_bytes.get(&node.id).copied().unwrap_or_default();
        refresh_node_pipeline_state(
            node,
            leader_id,
            leader_commit_index,
            bytes,
            leader_next_index,
            deployed,
            config,
        );
    }
}

fn refresh_node_pipeline_state(
    node: &mut RaftNode,
    leader_id: RaftNodeId,
    leader_commit_index: u64,
    inflight_bytes: u64,
    leader_next_index: u64,
    deployed: bool,
    config: &RaftConfig,
) {
    // How far this peer has actually got, as far as THIS leader knows. `commit_index` is
    // a shadow copy that is only ever updated for peers this process replicated to
    // itself, so a leader that was just promoted still reads 0 there and would conclude
    // its whole log is in flight -- enough to trip the backpressure limit on its first
    // append and leave the shard unable to commit. `match_index` is what AppendEntries
    // responses actually confirm, so take the better of the two.
    let known_progress = node.commit_index.max(node.pipeline_state.match_index);
    let inflight_entries = leader_commit_index.saturating_sub(known_progress);
    let snapshot_installed_index = node
        .installed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.last_included_index)
        .unwrap_or_default();
    // Never walk a confirmed cursor backwards. `commit_index` here is this process's
    // shadow copy of the peer, which is 0 for any peer it has not itself replicated to --
    // a newly promoted leader would otherwise forget everything the send/ack path had
    // already confirmed and re-ship the whole log on every heartbeat.
    node.pipeline_state.match_index = node.commit_index.max(node.pipeline_state.match_index);
    // `match_index` is only ever set from a SUCCESSFUL append, so it is authoritative:
    // the next entry to send is never below it. Deriving `next_index` purely from the
    // peer's shadow log (empty on a node this process never replicated to) reset a newly
    // promoted leader's cursor to 1, making every heartbeat look like a full-log catch-up
    // and tripping backpressure. Clamping keeps the in-process model's behaviour, where
    // the shadow log IS the peer's log.
    if deployed && node.id != leader_id {
        // Deployed, this process hosts ONE node, so a peer's `log` here is an empty
        // placeholder -- deriving `next_index` from it starts every catch-up at index 1 and
        // re-ships a log the follower already has. Raft starts a peer optimistically at the
        // leader's next index and lets rejections walk it back, so only INITIALISE it here;
        // the send/ack path owns it from then on.
        // "No progress established yet" is next_index <= 1 with nothing confirmed -- 1 is
        // the default a fresh RaftNode carries, not evidence that the follower is empty.
        if node.pipeline_state.match_index == 0 && node.pipeline_state.next_index <= 1 {
            node.pipeline_state.next_index = leader_next_index;
        }
        node.pipeline_state.next_index = node
            .pipeline_state
            .next_index
            .max(node.pipeline_state.match_index.saturating_add(1));
    } else {
        // In-process cluster: the shadow log IS the peer's log, so derive as before.
        node.pipeline_state.next_index = node_next_log_index(node)
            .max(node.pipeline_state.match_index.saturating_add(1));
    }
    node.pipeline_state.inflight_entries = inflight_entries;
    node.pipeline_state.inflight_bytes = inflight_bytes;
    node.pipeline_state.append_queue_depth = inflight_entries;
    node.pipeline_state.reorder_queue_depth = if config.enable_reorder_queue {
        node.commit_index.saturating_sub(node.applied_index)
    } else {
        0
    };
    node.pipeline_state.snapshot_sending = snapshot_installed_index > 0 && node.id != leader_id;
    node.pipeline_state.snapshot_installed_index = snapshot_installed_index;
    // transfer_leader_target is owned by begin_leader_transfer / transfer_leader /
    // refresh_leader_transfer_timeouts. The per-refresh recompute here clobbered a
    // pending transfer target (so it never timed out) and wrongly marked the leader
    // itself as its own transfer target; leave the field untouched during refresh.
    node.pipeline_state.pre_vote_rejections = if config.enable_pre_vote && !node.alive {
        node.pipeline_state.pre_vote_rejections.max(1)
    } else {
        node.pipeline_state.pre_vote_rejections
    };
    node.pipeline_state.election_rejections = if config.prohibits_election && node.id != leader_id {
        node.pipeline_state.election_rejections.max(1)
    } else {
        node.pipeline_state.election_rejections
    };
}

fn append_entry(node: &mut RaftNode, entry: RaftLogEntry) {
    if node
        .installed_snapshot
        .as_ref()
        .map(|snapshot| entry.index <= snapshot.last_included_index)
        .unwrap_or(false)
    {
        return;
    }
    // Raft §5.3: truncate ONLY on a genuine term conflict (same index, different term).
    // An entry already present with the SAME term is a no-op -- unconditionally
    // truncating on index<=last (a stale/duplicate/reordered AppendEntries that is a
    // prefix of the committed log) would delete committed suffix entries, rewind
    // applied state, and double-execute non-idempotent commands on re-apply.
    if let Some(existing) = node.log.iter().find(|existing| existing.index == entry.index) {
        if existing.term == entry.term {
            return;
        }
        node.log.retain(|existing| existing.index < entry.index);
        node.applied.retain(|applied| *applied < entry.index);
        node.applied_index = node.applied.iter().next_back().copied().unwrap_or_default();
        // max_applied_index is intentionally NOT lowered (monotonic exactly-once floor).
    }
    node.log.push(entry);
}

fn command_size_bytes(command: &Command) -> u64 {
    serde_json::to_vec(command)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or_default()
}

fn raft_log_bytes_after(log: &[RaftLogEntry], index: u64) -> u64 {
    log.iter()
        .filter(|entry| entry.index > index)
        .map(|entry| command_size_bytes(&entry.command))
        .sum()
}

fn meta_log_bytes_after(log: &[MetaLogEntry], index: u64) -> u64 {
    log.iter()
        .filter(|entry| entry.index > index)
        .map(|entry| {
            serde_json::to_vec(&entry.command)
                .map(|bytes| bytes.len() as u64)
                .unwrap_or_default()
        })
        .sum()
}

fn split_command_for_raft_limit(command: Command, limit: u64) -> Result<Vec<Command>, RaftError> {
    let bytes = command_size_bytes(&command);
    if bytes <= limit {
        return Ok(vec![command]);
    }
    match command {
        Command::SequenceAdd { key, rows } if rows.len() > 1 => {
            let mut chunks = Vec::new();
            let mut current = Vec::new();
            for row in rows {
                let mut candidate = current.clone();
                candidate.push(row.clone());
                let candidate_command = Command::SequenceAdd {
                    key: key.clone(),
                    rows: candidate,
                };
                let candidate_bytes = command_size_bytes(&candidate_command);
                if candidate_bytes <= limit {
                    current.push(row);
                    continue;
                }
                if current.is_empty() {
                    return Err(RaftError::LogEntryTooLarge {
                        bytes: candidate_bytes,
                        limit,
                    });
                }
                chunks.push(Command::SequenceAdd {
                    key: key.clone(),
                    rows: std::mem::take(&mut current),
                });
                let single_command = Command::SequenceAdd {
                    key: key.clone(),
                    rows: vec![row.clone()],
                };
                let single_bytes = command_size_bytes(&single_command);
                if single_bytes > limit {
                    return Err(RaftError::LogEntryTooLarge {
                        bytes: single_bytes,
                        limit,
                    });
                }
                current.push(row);
            }
            if !current.is_empty() {
                chunks.push(Command::SequenceAdd { key, rows: current });
            }
            Ok(chunks)
        }
        other => Err(RaftError::LogEntryTooLarge {
            bytes: command_size_bytes(&other),
            limit,
        }),
    }
}

/// Apply newly committed entries, capturing the response of every index in `waiters` so each
/// waiting proposer gets its own command's answer. The twin of [`apply_committed`] for the path
/// where the applier is a sender thread and the interested parties are elsewhere; the exactly-
/// once and cursor rules are identical, and both run under the caller's `inner` write lock.
fn apply_committed_recording(
    node: &mut RaftNode,
    waiters: &BTreeSet<u64>,
    captured: &mut BTreeMap<u64, CommandResponse>,
) {
    let start = node
        .log
        .binary_search_by_key(&node.applied_index.saturating_add(1), |entry| entry.index)
        .unwrap_or_else(|position| position);
    let mut batch = Vec::new();
    let mut batch_indexes = Vec::new();
    for entry in node.log[start..]
        .iter()
        .take_while(|entry| entry.index <= node.commit_index)
    {
        if entry.index <= node.max_applied_index {
            node.applied_index = node.applied_index.max(entry.index);
            node.applied.insert(entry.index);
            continue;
        }
        if node.applied.insert(entry.index) {
            batch.push(ExecuteRequest {
                shard_id: entry.shard_id,
                command: entry.command.clone(),
            });
            batch_indexes.push(entry.index);
        }
    }
    if !batch.is_empty() {
        let responses = node.engine.execute_raft_apply_batch(batch);
        for (index, response) in batch_indexes.into_iter().zip(responses) {
            node.applied_index = index;
            node.max_applied_index = node.max_applied_index.max(index);
            if waiters.contains(&index) {
                captured.insert(index, response.response);
            }
        }
    }
}

fn apply_committed(node: &mut RaftNode) -> Option<CommandResponse> {
    let mut last_response = None;
    let start = node
        .log
        .binary_search_by_key(&node.applied_index.saturating_add(1), |entry| entry.index)
        .unwrap_or_else(|position| position);
    // Collect the committed entries that pass the exactly-once floor (and are freshly inserted
    // into `applied`) into a single batch, then apply them via `execute_raft_apply_batch`. Under
    // TS_RAFT_APPLY_COALESCE this coalesces the batch's engine-WAL fdatasync into ONE barrier; with
    // the gate off (or a single-entry batch) the batch method degrades to the same per-entry
    // `execute_raft_apply` calls, so the exactly-once + durability semantics are byte-identical.
    // Cursor advances happen in-order exactly as before (skipped/already-applied entries update the
    // apply cursor inline; executed entries advance applied_index/max_applied_index after apply),
    // and nothing observes node state mid-loop (all under the caller's `inner` write lock).
    let mut batch = Vec::new();
    let mut batch_indexes = Vec::new();
    for entry in node.log[start..]
        .iter()
        .take_while(|entry| entry.index <= node.commit_index)
    {
        // Monotonic exactly-once floor (applied_raft_index_ guard): never
        // re-execute an index that was already applied, even if `applied`/applied_index
        // were rewound by a truncation. Still advance the apply cursor so health/lag
        // reflects that the entry IS applied -- catch-up after a rewind reconciles the
        // cursor without double-executing.
        if entry.index <= node.max_applied_index {
            node.applied_index = node.applied_index.max(entry.index);
            node.applied.insert(entry.index);
            continue;
        }
        if node.applied.insert(entry.index) {
            batch.push(ExecuteRequest {
                shard_id: entry.shard_id,
                command: entry.command.clone(),
            });
            batch_indexes.push(entry.index);
        }
    }
    if !batch.is_empty() {
        let responses = node.engine.execute_raft_apply_batch(batch);
        for (index, response) in batch_indexes.into_iter().zip(responses) {
            node.applied_index = index;
            node.max_applied_index = node.max_applied_index.max(index);
            last_response = Some(response.response);
        }
    }
    last_response
}

fn install_snapshot_state(node: &mut RaftNode, snapshot: RaftSnapshot) {
    let engine = TemporalEngine::default();
    if let Some(image) = &snapshot.state_image {
        // Reconstruct from the opaque state image in O(state): install the slabs and the served
        // index, then load the shard so the index is read in. Every path a snapshot can land on
        // must handle this -- an image snapshot fed to an entries-only installer replays nothing
        // and quietly leaves an EMPTY engine, which is exactly what happened to a restart that
        // restored an image-carrying record before this installer learned about images.
        let block_store = engine.block_store();
        for slab in &image.slabs {
            let _ = block_store.install_slab(slab.page_slab_id, &slab.bytes);
        }
        let _ = engine.install_index_bytes(snapshot.shard_id, &image.index_bytes);
        engine.load_shard(snapshot.shard_id);
    } else {
        engine.load_shard(snapshot.shard_id);
        for entry in &snapshot.entries {
            engine.execute_raft_apply(ExecuteRequest {
                shard_id: entry.shard_id,
                command: entry.command.clone(),
            });
        }
    }
    node.engine = engine;
    // votedFor is per-term (Raft Fig-2): clear a stale vote when a snapshot raises the term,
    // so a same-new-term candidate is not wrongly rejected as already_voted.
    if snapshot.last_included_term > node.current_term {
        node.voted_for = None;
    }
    node.current_term = node.current_term.max(snapshot.last_included_term);
    node.commit_index = node.commit_index.max(snapshot.last_included_index);
    node.log
        .retain(|entry| entry.index > snapshot.last_included_index);
    node.applied.clear();
    node.applied
        .extend(snapshot.entries.iter().map(|entry| entry.index));
    node.applied_index = snapshot.last_included_index;
    node.max_applied_index = node.max_applied_index.max(snapshot.last_included_index);
    node.installed_snapshot = Some(snapshot);
}

fn install_snapshot_state_for_role(node: &mut RaftNode, snapshot: RaftSnapshot) {
    if node.replica_role.can_serve_data() {
        install_snapshot_state(node, snapshot);
    } else {
        node.current_term = node.current_term.max(snapshot.last_included_term);
        node.commit_index = node.commit_index.max(snapshot.last_included_index);
        node.log
            .retain(|entry| entry.index > snapshot.last_included_index);
        node.applied.clear();
        node.applied_index = snapshot.last_included_index;
        node.max_applied_index = node.max_applied_index.max(snapshot.last_included_index);
        node.installed_snapshot = Some(snapshot);
    }
}

fn install_leader_snapshot_tail(
    node: &mut RaftNode,
    leader_snapshot: Option<RaftSnapshot>,
    leader_log: Vec<RaftLogEntry>,
    leader_commit_index: u64,
) {
    if let Some(snapshot) = leader_snapshot {
        if node_last_log_or_snapshot_index(node) < snapshot.last_included_index {
            install_snapshot_state_for_role(node, snapshot);
        }
    }
    node.log = leader_log;
    node.commit_index = leader_commit_index;
    if node.replica_role.can_serve_data() {
        apply_committed(node);
    }
}

fn raft_apply_snapshot_fence(node: &RaftNode) -> RaftApplySnapshotFence {
    let installed_snapshot_index = node
        .installed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.last_included_index)
        .unwrap_or_default();
    RaftApplySnapshotFence {
        applied_index: node.applied_index,
        commit_index: node.commit_index,
        installed_snapshot_index,
        first_retained_log_index: node
            .log
            .iter()
            .find(|entry| entry.index > installed_snapshot_index)
            .map(|entry| entry.index)
            .unwrap_or_default(),
    }
}

fn raft_storage_snapshot_id(node: &RaftNode) -> Option<String> {
    node.installed_snapshot.as_ref().map(|snapshot| {
        format!(
            "local-snapshot-{}-{}-{}",
            snapshot.shard_id, snapshot.last_included_term, snapshot.last_included_index
        )
    })
}

fn raft_storage_apply_fence(shard_id: ShardId, node: &RaftNode) -> RaftStorageApplyFence {
    let snapshot_id = raft_storage_snapshot_id(node);
    let storage_epoch = node.applied_index.max(
        node.installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or_default(),
    );
    let checksum = raft_storage_apply_fence_checksum(
        shard_id,
        node.current_term,
        node.commit_index,
        node.applied_index,
        snapshot_id.as_deref(),
        storage_epoch,
    );
    RaftStorageApplyFence {
        shard_id,
        raft_term: node.current_term,
        committed_index: node.commit_index,
        applied_index: node.applied_index,
        snapshot_id,
        storage_epoch,
        checksum,
    }
}

fn raft_storage_apply_fence_checksum(
    shard_id: ShardId,
    raft_term: u64,
    committed_index: u64,
    applied_index: u64,
    snapshot_id: Option<&str>,
    storage_epoch: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(shard_id.to_le_bytes());
    hasher.update(raft_term.to_le_bytes());
    hasher.update(committed_index.to_le_bytes());
    hasher.update(applied_index.to_le_bytes());
    hasher.update(storage_epoch.to_le_bytes());
    if let Some(snapshot_id) = snapshot_id {
        hasher.update(snapshot_id.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn validate_raft_apply_snapshot_fence(record: &RaftWalRecord) -> Result<(), RaftError> {
    let fence = &record.apply_snapshot_fence;
    if fence == &RaftApplySnapshotFence::default()
        && (record.hard_state.commit_index > 0
            || record.installed_snapshot.is_some()
            || !record.entries.is_empty())
    {
        return Ok(());
    }
    let snapshot_index = record
        .installed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.last_included_index)
        .unwrap_or_default();
    if fence.commit_index != record.hard_state.commit_index {
        return Err(RaftError::ApplySnapshotFence(format!(
            "fence commit index {} does not match hard-state commit index {}",
            fence.commit_index, record.hard_state.commit_index
        )));
    }
    if fence.applied_index > fence.commit_index {
        return Err(RaftError::ApplySnapshotFence(format!(
            "applied index {} is ahead of commit index {}",
            fence.applied_index, fence.commit_index
        )));
    }
    if fence.installed_snapshot_index != snapshot_index {
        return Err(RaftError::ApplySnapshotFence(format!(
            "fence snapshot index {} does not match installed snapshot index {}",
            fence.installed_snapshot_index, snapshot_index
        )));
    }
    if fence.applied_index < fence.installed_snapshot_index {
        return Err(RaftError::ApplySnapshotFence(format!(
            "applied index {} is behind installed snapshot index {}",
            fence.applied_index, fence.installed_snapshot_index
        )));
    }
    let first_retained_entry = record
        .entries
        .iter()
        .find(|entry| entry.index > fence.installed_snapshot_index);
    if let Some(first_entry) = first_retained_entry {
        if fence.first_retained_log_index != first_entry.index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "fence first retained log index {} does not match first entry {}",
                fence.first_retained_log_index, first_entry.index
            )));
        }
        if first_entry.index <= fence.installed_snapshot_index {
            return Err(RaftError::ApplySnapshotFence(format!(
                "first retained log index {} is not above snapshot index {}",
                first_entry.index, fence.installed_snapshot_index
            )));
        }
    } else if fence.first_retained_log_index != 0 {
        return Err(RaftError::ApplySnapshotFence(format!(
            "fence first retained log index {} is set but no log entries are retained",
            fence.first_retained_log_index
        )));
    }
    Ok(())
}

fn validate_raft_storage_apply_fence(record: &RaftWalRecord) -> Result<(), RaftError> {
    let fence = &record.storage_apply_fence;
    if fence == &RaftStorageApplyFence::default()
        && (record.hard_state.commit_index > 0
            || record.installed_snapshot.is_some()
            || !record.entries.is_empty())
    {
        return Err(RaftError::ApplySnapshotFence(
            "missing raft storage apply fence".to_string(),
        ));
    }
    if fence == &RaftStorageApplyFence::default() {
        return Ok(());
    }
    if fence.shard_id != record.membership.shard_id {
        return Err(RaftError::ApplySnapshotFence(format!(
            "storage fence shard {} does not match membership shard {}",
            fence.shard_id, record.membership.shard_id
        )));
    }
    if fence.raft_term != record.hard_state.current_term {
        return Err(RaftError::ApplySnapshotFence(format!(
            "storage fence term {} does not match hard-state term {}",
            fence.raft_term, record.hard_state.current_term
        )));
    }
    if fence.committed_index != record.hard_state.commit_index {
        return Err(RaftError::ApplySnapshotFence(format!(
            "storage fence committed index {} does not match hard-state commit index {}",
            fence.committed_index, record.hard_state.commit_index
        )));
    }
    if fence.applied_index > fence.committed_index {
        return Err(RaftError::ApplySnapshotFence(format!(
            "storage fence applied index {} is ahead of committed index {}",
            fence.applied_index, fence.committed_index
        )));
    }
    let snapshot_id = record.installed_snapshot.as_ref().map(|snapshot| {
        format!(
            "local-snapshot-{}-{}-{}",
            snapshot.shard_id, snapshot.last_included_term, snapshot.last_included_index
        )
    });
    if fence.snapshot_id != snapshot_id {
        return Err(RaftError::ApplySnapshotFence(format!(
            "storage fence snapshot id {:?} does not match installed snapshot id {:?}",
            fence.snapshot_id, snapshot_id
        )));
    }
    let snapshot_index = record
        .installed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.last_included_index)
        .unwrap_or_default();
    if fence.storage_epoch < fence.applied_index || fence.storage_epoch < snapshot_index {
        return Err(RaftError::ApplySnapshotFence(format!(
            "storage fence epoch {} is behind applied index {} or snapshot index {}",
            fence.storage_epoch, fence.applied_index, snapshot_index
        )));
    }
    let expected = raft_storage_apply_fence_checksum(
        fence.shard_id,
        fence.raft_term,
        fence.committed_index,
        fence.applied_index,
        fence.snapshot_id.as_deref(),
        fence.storage_epoch,
    );
    if fence.checksum != expected {
        return Err(RaftError::ApplySnapshotFence(
            "storage fence checksum mismatch".to_string(),
        ));
    }
    Ok(())
}

fn raft_wal_checksum(record: &RaftWalRecord) -> io::Result<String> {
    let bytes = serde_json::to_vec(record).map_err(io::Error::other)?;
    let digest = Sha256::digest(bytes);
    let mut checksum = String::with_capacity(digest.len() * 2);
    for byte in digest {
        checksum.push_str(&format!("{byte:02x}"));
    }
    Ok(checksum)
}

fn majority(replica_count: usize) -> usize {
    replica_count / 2 + 1
}

fn raft_status_prometheus(kind: &str, status: RaftClusterStatus) -> String {
    let mut out = String::new();
    out.push_str("# HELP temporalstore_raft_cluster_commit_index Current committed raft index.\n");
    out.push_str("# TYPE temporalstore_raft_cluster_commit_index gauge\n");
    push_raft_metric(
        &mut out,
        "temporalstore_raft_cluster_commit_index",
        &[("kind", kind.to_string())],
        status.commit_index,
    );
    out.push_str("# HELP temporalstore_raft_cluster_live_voters Live raft voters.\n");
    out.push_str("# TYPE temporalstore_raft_cluster_live_voters gauge\n");
    push_raft_metric(
        &mut out,
        "temporalstore_raft_cluster_live_voters",
        &[("kind", kind.to_string())],
        status.live_voters as u64,
    );
    out.push_str(
        "# HELP temporalstore_raft_cluster_has_majority Whether the raft group has majority.\n",
    );
    out.push_str("# TYPE temporalstore_raft_cluster_has_majority gauge\n");
    push_raft_metric(
        &mut out,
        "temporalstore_raft_cluster_has_majority",
        &[("kind", kind.to_string())],
        u64::from(status.has_majority),
    );
    out.push_str("# HELP temporalstore_raft_leader_lease_valid Whether local model considers leader lease valid.\n");
    out.push_str("# TYPE temporalstore_raft_leader_lease_valid gauge\n");
    push_raft_metric(
        &mut out,
        "temporalstore_raft_leader_lease_valid",
        &[("kind", kind.to_string())],
        u64::from(status.leader_lease_valid),
    );
    out.push_str("# HELP temporalstore_raft_node_commit_index Per-node committed raft index.\n");
    out.push_str("# TYPE temporalstore_raft_node_commit_index gauge\n");
    out.push_str("# HELP temporalstore_raft_node_applied_index Per-node applied raft index.\n");
    out.push_str("# TYPE temporalstore_raft_node_applied_index gauge\n");
    out.push_str("# HELP temporalstore_raft_node_lag Per-node raft commit lag behind leader.\n");
    out.push_str("# TYPE temporalstore_raft_node_lag gauge\n");
    out.push_str("# HELP temporalstore_raft_node_apply_lag Per-node commit-to-apply lag.\n");
    out.push_str("# TYPE temporalstore_raft_node_apply_lag gauge\n");
    out.push_str("# HELP temporalstore_raft_node_alive Whether a raft node is alive.\n");
    out.push_str("# TYPE temporalstore_raft_node_alive gauge\n");
    for node in status.nodes {
        let labels = &[
            ("kind", kind.to_string()),
            ("node_id", node.node_id.to_string()),
            ("role", format!("{:?}", node.role).to_ascii_lowercase()),
            (
                "replica_role",
                format!("{:?}", node.replica_role).to_ascii_lowercase(),
            ),
        ];
        push_raft_metric(
            &mut out,
            "temporalstore_raft_node_commit_index",
            labels,
            node.commit_index,
        );
        push_raft_metric(
            &mut out,
            "temporalstore_raft_node_applied_index",
            labels,
            node.applied_index,
        );
        push_raft_metric(&mut out, "temporalstore_raft_node_lag", labels, node.lag);
        push_raft_metric(
            &mut out,
            "temporalstore_raft_node_apply_lag",
            labels,
            node.commit_index.saturating_sub(node.applied_index),
        );
        push_raft_metric(
            &mut out,
            "temporalstore_raft_node_alive",
            labels,
            u64::from(node.alive),
        );
    }
    out
}

fn append_matrixraft_runtime_admin_prometheus(
    out: &mut String,
    kind: &str,
    report: MatrixRaftRuntimeAdminReport,
) {
    let matrixraft_capability_report = matrixraft_capability_report_from_matrixraft_admin(&report);
    let matrixraft_metrics = matrixraft_reference_raft_runtime_capability_prometheus(
        &matrixraft_capability_report,
        &[("kind", kind)],
    );
    out.push_str(&matrixraft_metrics.text);

    out.push_str("# HELP temporalstore_raft_matrixraft_ready Whether MatrixRaft-style production runtime evidence is complete.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_ready gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_ready",
        &[("kind", kind.to_string())],
        u64::from(report.ready),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_quorum_peer_progress_observed Whether admin readiness observed quorum peer match/next progress from runtime state.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_quorum_peer_progress_observed gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_quorum_peer_progress_observed",
        &[("kind", kind.to_string())],
        u64::from(report.quorum_peer_progress_observed),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_pipeline_runtime_activity_observed Whether per-peer pipeline state has non-vacuous runtime activity.\n");
    out.push_str(
        "# TYPE temporalstore_raft_matrixraft_peer_pipeline_runtime_activity_observed gauge\n",
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_peer_pipeline_runtime_activity_observed",
        &[("kind", kind.to_string())],
        u64::from(report.peer_pipeline_runtime_activity_observed),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_pipeline_limits_observed Whether per-peer pipeline limits are populated for admin readiness.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_pipeline_limits_observed gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_peer_pipeline_limits_observed",
        &[("kind", kind.to_string())],
        u64::from(report.peer_pipeline_limits_observed),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_capability_ready MatrixRaft-style capability readiness matrix.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_capability_ready gauge\n");
    for capability in &report.capability_matrix {
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_capability_ready",
            &[
                ("kind", kind.to_string()),
                ("capability", capability.capability.clone()),
                ("evidence_field", capability.evidence_field.clone()),
            ],
            u64::from(capability.ready),
        );
    }
    out.push_str("# HELP temporalstore_raft_matrixraft_read_index_validated Whether read-index evidence was validated.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_read_index_validated gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_read_index_validated",
        &[("kind", kind.to_string())],
        u64::from(report.read_index_validated),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_lease_read_validated Whether lease-read evidence was validated.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_lease_read_validated gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_lease_read_validated",
        &[("kind", kind.to_string())],
        u64::from(report.lease_read_validated),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_stale_follower_read_rejected Whether stale follower reads are rejected.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_stale_follower_read_rejected gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_stale_follower_read_rejected",
        &[("kind", kind.to_string())],
        u64::from(report.stale_follower_read_rejected),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_stale_follower_write_rejected Whether stale follower writes are rejected.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_stale_follower_write_rejected gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_stale_follower_write_rejected",
        &[("kind", kind.to_string())],
        u64::from(report.stale_follower_write_rejected),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_witness_membership_present",
        &[("kind", kind.to_string())],
        u64::from(report.witness_membership_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_witness_role_behavior_present",
        &[("kind", kind.to_string())],
        u64::from(report.witness_role_behavior_present),
    );
    for (name, value) in [
        (
            "temporalstore_raft_matrixraft_learner_add_present",
            report.learner_add_present,
        ),
        (
            "temporalstore_raft_matrixraft_learner_catchup_present",
            report.learner_catchup_present,
        ),
        (
            "temporalstore_raft_matrixraft_learner_promote_present",
            report.learner_promote_present,
        ),
        (
            "temporalstore_raft_matrixraft_voter_remove_present",
            report.voter_remove_present,
        ),
        (
            "temporalstore_raft_matrixraft_leader_transfer_exact_once_present",
            report.leader_transfer_exact_once_present,
        ),
        (
            "temporalstore_raft_matrixraft_pending_joint_consensus_restart_present",
            report.pending_joint_consensus_restart_present,
        ),
    ] {
        push_raft_metric(out, name, &[("kind", kind.to_string())], u64::from(value));
    }
    for (name, value) in [
        (
            "temporalstore_raft_matrixraft_membership_learner_add_count",
            report.membership_evidence.learner_add_count,
        ),
        (
            "temporalstore_raft_matrixraft_membership_learner_catchup_count",
            report.membership_evidence.learner_catchup_count,
        ),
        (
            "temporalstore_raft_matrixraft_membership_learner_promote_count",
            report.membership_evidence.learner_promote_count,
        ),
        (
            "temporalstore_raft_matrixraft_membership_voter_remove_count",
            report.membership_evidence.voter_remove_count,
        ),
        (
            "temporalstore_raft_matrixraft_membership_leader_transfer_write_count",
            report.membership_evidence.leader_transfer_write_count,
        ),
        (
            "temporalstore_raft_matrixraft_membership_leader_transfer_exact_once_commit_count",
            report
                .membership_evidence
                .leader_transfer_exact_once_commit_count,
        ),
        (
            "temporalstore_raft_matrixraft_membership_leader_transfer_exact_once_commit_id_count",
            report
                .membership_evidence
                .leader_transfer_exact_once_commit_ids
                .len() as u64,
        ),
        (
            "temporalstore_raft_matrixraft_membership_pending_joint_consensus_restore_count",
            report
                .membership_evidence
                .pending_joint_consensus_restore_count,
        ),
    ] {
        push_raft_metric(out, name, &[("kind", kind.to_string())], value);
    }
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_learner_auto_promote_present",
        &[("kind", kind.to_string())],
        u64::from(report.learner_auto_promote_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_pending_joint_consensus_present",
        &[("kind", kind.to_string())],
        u64::from(report.pending_joint_consensus_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_append_backpressure_enforced Whether append pipeline backpressure evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_append_backpressure_enforced gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_append_backpressure_enforced",
        &[("kind", kind.to_string())],
        u64::from(report.append_backpressure_enforced),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_apply_backpressure_enforced",
        &[("kind", kind.to_string())],
        u64::from(report.apply_backpressure_enforced),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_memory_replicate_bytes_enforced",
        &[("kind", kind.to_string())],
        u64::from(report.memory_replicate_bytes_enforced),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_oversized_log_rejection_present",
        &[("kind", kind.to_string())],
        u64::from(report.oversized_log_rejection_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_out_of_order_append_handling_present",
        &[("kind", kind.to_string())],
        u64::from(report.out_of_order_append_handling_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_reorder_queue_enabled Whether per-peer reorder queue evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_reorder_queue_enabled gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_reorder_queue_enabled",
        &[("kind", kind.to_string())],
        u64::from(report.reorder_queue_enabled),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_snapshot_sender_lifecycle_present Whether snapshot sender lifecycle evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_snapshot_sender_lifecycle_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_sender_lifecycle_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_sender_lifecycle_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_snapshot_downloader_lifecycle_present Whether snapshot downloader lifecycle evidence is present.\n");
    out.push_str(
        "# TYPE temporalstore_raft_matrixraft_snapshot_downloader_lifecycle_present gauge\n",
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_downloader_lifecycle_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_downloader_lifecycle_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_snapshot_retry_backpressure_present Whether snapshot retry/backpressure evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_snapshot_retry_backpressure_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_retry_backpressure_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_retry_backpressure_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_snapshot_chunk_retry_present Whether snapshot chunk retry evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_snapshot_chunk_retry_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_chunk_retry_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_chunk_retry_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_snapshot_send_timeout_present Whether snapshot send timeout evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_snapshot_send_timeout_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_send_timeout_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_send_timeout_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_rate_limit_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_rate_limit_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_install_progress_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_install_progress_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_install_rollback_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_install_rollback_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_membership_change_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_membership_change_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_snapshot_rejoin_after_compacted_log_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_rejoin_after_compacted_log_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_wal_segment_lifecycle_present Whether WAL segment lifecycle evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_segment_lifecycle_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_segment_lifecycle_present",
        &[("kind", kind.to_string())],
        u64::from(report.wal_segment_lifecycle_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_wal_segment_count WAL segments retained by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_segment_count gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_segment_count",
        &[("kind", kind.to_string())],
        report.wal_segment_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_wal_total_bytes WAL bytes retained by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_total_bytes gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_total_bytes",
        &[("kind", kind.to_string())],
        report.wal_total_bytes,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_wal_active_segment_bytes Active WAL segment bytes retained by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_active_segment_bytes gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_active_segment_bytes",
        &[("kind", kind.to_string())],
        report.wal_active_segment_bytes,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_wal_total_records WAL records retained by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_total_records gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_total_records",
        &[("kind", kind.to_string())],
        report.wal_total_records,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_wal_first_sequence First retained WAL record sequence.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_first_sequence gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_first_sequence",
        &[("kind", kind.to_string())],
        report.wal_first_sequence,
    );
    out.push_str(
        "# HELP temporalstore_raft_matrixraft_wal_last_sequence Last retained WAL record sequence.\n",
    );
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_last_sequence gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_last_sequence",
        &[("kind", kind.to_string())],
        report.wal_last_sequence,
    );
    out.push_str(
        "# HELP temporalstore_raft_matrixraft_wal_first_log_index First retained WAL log index.\n",
    );
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_first_log_index gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_first_log_index",
        &[("kind", kind.to_string())],
        report.wal_first_log_index,
    );
    out.push_str(
        "# HELP temporalstore_raft_matrixraft_wal_last_log_index Last retained WAL log index.\n",
    );
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_last_log_index gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_last_log_index",
        &[("kind", kind.to_string())],
        report.wal_last_log_index,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_wal_released_segment_count WAL segments released by the last segmented append.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_released_segment_count gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_released_segment_count",
        &[("kind", kind.to_string())],
        report.wal_released_segment_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_wal_slow_fsync_backpressure_observed Whether slow fsync backpressure evidence was observed.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_wal_slow_fsync_backpressure_observed gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_wal_slow_fsync_backpressure_observed",
        &[("kind", kind.to_string())],
        u64::from(report.wal_slow_fsync_backpressure_observed),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_read_index_requests Read-index requests observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_read_index_requests counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_read_index_requests",
        &[("kind", kind.to_string())],
        report.read_index_requests,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_read_index_accepted Read-index requests accepted by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_read_index_accepted counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_read_index_accepted",
        &[("kind", kind.to_string())],
        report.read_index_accepted,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_read_index_rejected Read-index requests rejected by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_read_index_rejected counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_read_index_rejected",
        &[("kind", kind.to_string())],
        report.read_index_rejected,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_lease_read_requests Lease-read requests observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_lease_read_requests counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_lease_read_requests",
        &[("kind", kind.to_string())],
        report.lease_read_requests,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_lease_read_accepted Lease-read requests accepted by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_lease_read_accepted counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_lease_read_accepted",
        &[("kind", kind.to_string())],
        report.lease_read_accepted,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_lease_read_rejected Lease-read requests rejected by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_lease_read_rejected counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_lease_read_rejected",
        &[("kind", kind.to_string())],
        report.lease_read_rejected,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_stale_leader_lease_rejections Stale leader lease read rejections observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_stale_leader_lease_rejections counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_stale_leader_lease_rejections",
        &[("kind", kind.to_string())],
        report.stale_leader_lease_rejection_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_lagging_follower_read_rejections Lagging follower read rejections observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_lagging_follower_read_rejections counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_lagging_follower_read_rejections",
        &[("kind", kind.to_string())],
        report.lagging_follower_read_rejection_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_bounded_stale_read_requests Bounded-stale read requests observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_bounded_stale_read_requests counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_bounded_stale_read_requests",
        &[("kind", kind.to_string())],
        report.bounded_stale_read_requests,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_bounded_stale_read_accepted Bounded-stale read requests accepted by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_bounded_stale_read_accepted counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_bounded_stale_read_accepted",
        &[("kind", kind.to_string())],
        report.bounded_stale_read_accepted_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_bounded_stale_read_rejected Bounded-stale read requests rejected by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_bounded_stale_read_rejected counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_bounded_stale_read_rejected",
        &[("kind", kind.to_string())],
        report.bounded_stale_read_rejected_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_minority_partition_read_rejections Minority-partition read rejections observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_minority_partition_read_rejections counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_minority_partition_read_rejections",
        &[("kind", kind.to_string())],
        report.minority_partition_read_rejection_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_minority_partition_write_rejections Minority-partition write rejections observed by the raft runtime.\n");
    out.push_str(
        "# TYPE temporalstore_raft_matrixraft_minority_partition_write_rejections counter\n",
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_minority_partition_write_rejections",
        &[("kind", kind.to_string())],
        report.minority_partition_write_rejection_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_healed_follower_catchup Healed follower catch-up observations by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_healed_follower_catchup counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_healed_follower_catchup",
        &[("kind", kind.to_string())],
        report.healed_follower_catchup_count,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_pre_vote_requests Pre-vote attempts observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_pre_vote_requests counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_pre_vote_requests",
        &[("kind", kind.to_string())],
        report.pre_vote_requests,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_pre_vote_accepted Pre-vote attempts accepted by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_pre_vote_accepted counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_pre_vote_accepted",
        &[("kind", kind.to_string())],
        report.pre_vote_accepted,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_pre_vote_rejected Pre-vote attempts rejected by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_pre_vote_rejected counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_pre_vote_rejected",
        &[("kind", kind.to_string())],
        report.pre_vote_rejected,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_match_index MatrixRaft-style per-peer match index.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_match_index gauge\n");
    out.push_str(
        "# HELP temporalstore_raft_matrixraft_peer_next_index MatrixRaft-style per-peer next index.\n",
    );
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_next_index gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_append_requests MatrixRaft-style per-peer append requests.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_append_requests counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_append_accepted MatrixRaft-style per-peer append requests accepted.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_append_accepted counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_append_rejected MatrixRaft-style per-peer append requests rejected by pipeline backpressure.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_append_rejected counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_inflight_entries MatrixRaft-style per-peer inflight entries.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_inflight_entries gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_inflight_bytes MatrixRaft-style per-peer inflight bytes.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_inflight_bytes gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_append_queue_depth MatrixRaft-style per-peer append queue depth.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_append_queue_depth gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_append_queue_limit MatrixRaft-style per-peer append queue limit.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_append_queue_limit gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_inflight_bytes_limit MatrixRaft-style per-peer inflight byte limit.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_inflight_bytes_limit gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_apply_inflight_limit MatrixRaft-style per-peer apply inflight task limit.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_apply_inflight_limit gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_apply_queue_depth MatrixRaft-style per-peer apply queue depth.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_apply_queue_depth gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_apply_queue_max_depth MatrixRaft-style per-peer max observed apply queue depth.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_apply_queue_max_depth gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_apply_batch_bytes_limit MatrixRaft-style per-peer apply batch byte limit.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_apply_batch_bytes_limit gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_reorder_queue_depth MatrixRaft-style per-peer reorder queue depth.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_reorder_queue_depth gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_reorder_entries_accepted MatrixRaft-style per-peer reorder entries accepted.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_reorder_entries_accepted counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_reorder_entries_released MatrixRaft-style per-peer reorder entries released to apply.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_reorder_entries_released counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_reorder_entries_rejected MatrixRaft-style per-peer reorder entries rejected by window overflow.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_reorder_entries_rejected counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_reorder_entry_timeouts MatrixRaft-style per-peer reorder entry timeout count.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_reorder_entry_timeouts counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_reorder_dropped_packages MatrixRaft-style per-peer dropped reordered append packages.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_reorder_dropped_packages counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_stale_term_rejections MatrixRaft-style per-peer stale-term append rejections.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_stale_term_rejections counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_sending Whether a peer is sending a snapshot.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_sending gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_installing Whether a peer is installing a snapshot.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_installing gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_installed_index Last installed snapshot index per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_installed_index gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_send_attempts Snapshot send attempts per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_send_attempts counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_send_completed Snapshot sends completed per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_send_completed counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_send_failed Snapshot sends failed per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_send_failed counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_install_started Snapshot installs started per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_install_started counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_install_completed Snapshot installs completed per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_install_completed counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_install_rejected Snapshot installs rejected per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_install_rejected counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_install_rolled_back Snapshot install rollbacks per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_install_rolled_back counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_install_received_chunks Snapshot chunks received per peer.\n");
    out.push_str(
        "# TYPE temporalstore_raft_matrixraft_peer_snapshot_install_received_chunks gauge\n",
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_install_total_chunks Snapshot chunks expected per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_install_total_chunks gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_retry_count Snapshot retry/rejection count per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_retry_count counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_backpressure_rejections Snapshot backpressure rejection count per peer.\n");
    out.push_str(
        "# TYPE temporalstore_raft_matrixraft_peer_snapshot_backpressure_rejections counter\n",
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_send_elapsed_ms Snapshot sender elapsed time per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_send_elapsed_ms gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_snapshot_send_timeouts Snapshot sender timeout count per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_snapshot_send_timeouts counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_transfer_leader_requests Leader-transfer requests per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_transfer_leader_requests counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_transfer_leader_accepted Leader-transfer requests accepted per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_transfer_leader_accepted counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_transfer_leader_rejected Leader-transfer requests rejected per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_transfer_leader_rejected counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_transfer_leader_completed Leader-transfer completions per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_transfer_leader_completed counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_transfer_leader_elapsed_ms Pending leader-transfer elapsed time per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_transfer_leader_elapsed_ms gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_transfer_leader_timeouts Leader-transfer timeout count per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_transfer_leader_timeouts counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_offline_elapsed_ms MatrixRaft-style offline elapsed time per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_offline_elapsed_ms gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_offline_timeout_reached Whether a peer has crossed the configured offline timeout.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_offline_timeout_reached gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_peer_offline_timeout_rejections Offline timeout transitions per peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_peer_offline_timeout_rejections counter\n");
    for peer in report.peer_pipeline_states {
        let labels = &[
            ("kind", kind.to_string()),
            ("node_id", peer.peer_id.to_string()),
            ("role", format!("{:?}", peer.role).to_ascii_lowercase()),
            (
                "replica_role",
                format!("{:?}", peer.replica_role).to_ascii_lowercase(),
            ),
        ];
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_match_index",
            labels,
            peer.match_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_next_index",
            labels,
            peer.next_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_append_requests",
            labels,
            peer.append_requests,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_append_accepted",
            labels,
            peer.append_accepted,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_append_rejected",
            labels,
            peer.append_rejected,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_inflight_entries",
            labels,
            peer.inflight_entries,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_inflight_bytes",
            labels,
            peer.inflight_bytes,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_append_queue_depth",
            labels,
            peer.append_queue_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_append_queue_limit",
            labels,
            peer.append_queue_limit,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_inflight_bytes_limit",
            labels,
            peer.inflight_bytes_limit,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_append_queue_max_depth",
            labels,
            peer.append_queue_max_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_apply_inflight_tasks",
            labels,
            peer.apply_inflight_tasks,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_apply_inflight_limit",
            labels,
            peer.apply_inflight_limit,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_apply_queue_depth",
            labels,
            peer.apply_queue_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_apply_queue_max_depth",
            labels,
            peer.apply_queue_max_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_apply_batch_bytes_limit",
            labels,
            peer.apply_batch_bytes_limit,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_apply_backpressure_rejections",
            labels,
            peer.apply_backpressure_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_memory_backpressure_rejections",
            labels,
            peer.memory_backpressure_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_oversized_log_rejections",
            labels,
            peer.oversized_log_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_reorder_queue_depth",
            labels,
            peer.reorder_queue_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_out_of_order_append_rejections",
            labels,
            peer.out_of_order_append_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_reorder_entries_accepted",
            labels,
            peer.reorder_entries_accepted,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_reorder_entries_released",
            labels,
            peer.reorder_entries_released,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_reorder_entries_rejected",
            labels,
            peer.reorder_entries_rejected,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_reorder_entry_timeouts",
            labels,
            peer.reorder_entry_timeouts,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_reorder_dropped_packages",
            labels,
            peer.reorder_dropped_packages,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_stale_term_rejections",
            labels,
            peer.stale_term_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_sending",
            labels,
            u64::from(peer.snapshot_sending),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_installing",
            labels,
            u64::from(peer.snapshot_installing),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_installed_index",
            labels,
            peer.snapshot_installed_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_send_attempts",
            labels,
            peer.snapshot_send_attempts,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_send_completed",
            labels,
            peer.snapshot_send_completed,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_send_failed",
            labels,
            peer.snapshot_send_failed,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_install_started",
            labels,
            peer.snapshot_install_started,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_install_completed",
            labels,
            peer.snapshot_install_completed,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_install_rejected",
            labels,
            peer.snapshot_install_rejected,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_install_rolled_back",
            labels,
            peer.snapshot_install_rolled_back,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_install_received_chunks",
            labels,
            peer.snapshot_install_received_chunks,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_install_total_chunks",
            labels,
            peer.snapshot_install_total_chunks,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_install_progress_per_mille",
            labels,
            peer.snapshot_install_progress_per_mille,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_retry_count",
            labels,
            peer.snapshot_retry_count,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_chunk_retry_count",
            labels,
            peer.snapshot_chunk_retry_count,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_backpressure_rejections",
            labels,
            peer.snapshot_backpressure_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_rate_limit_rejections",
            labels,
            peer.snapshot_rate_limit_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_during_membership_change",
            labels,
            u64::from(peer.snapshot_during_membership_change),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_rejoin_after_compacted_log",
            labels,
            u64::from(peer.snapshot_rejoin_after_compacted_log),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_auto_promoted_from_learner",
            labels,
            u64::from(peer.auto_promoted_from_learner),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_send_elapsed_ms",
            labels,
            peer.snapshot_send_elapsed_ms,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_snapshot_send_timeouts",
            labels,
            peer.snapshot_send_timeouts,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_transfer_leader_requests",
            labels,
            peer.transfer_leader_requests,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_transfer_leader_accepted",
            labels,
            peer.transfer_leader_accepted,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_transfer_leader_rejected",
            labels,
            peer.transfer_leader_rejected,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_transfer_leader_completed",
            labels,
            peer.transfer_leader_completed,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_transfer_leader_elapsed_ms",
            labels,
            peer.transfer_leader_elapsed_ms,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_transfer_leader_timeouts",
            labels,
            peer.transfer_leader_timeouts,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_offline_elapsed_ms",
            labels,
            peer.offline_elapsed_ms,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_offline_timeout_reached",
            labels,
            u64::from(peer.offline_timeout_reached),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_peer_offline_timeout_rejections",
            labels,
            peer.offline_timeout_rejections,
        );
    }
}

fn append_matrixraft_local_status_prometheus(
    out: &mut String,
    kind: &str,
    report: MatrixRaftLocalStatusReport,
) {
    out.push_str("# HELP temporalstore_raft_matrixraft_local_pending_joint_consensus_present Whether local status has pending joint-consensus membership.\n");
    out.push_str(
        "# TYPE temporalstore_raft_matrixraft_local_pending_joint_consensus_present gauge\n",
    );
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_local_pending_joint_consensus_present",
        &[("kind", kind.to_string())],
        u64::from(report.pending_joint_consensus.is_some()),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_local_witness_membership_present Whether local status has a witness member.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_witness_membership_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_local_witness_membership_present",
        &[("kind", kind.to_string())],
        u64::from(report.witness_membership_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_local_learner_membership_present Whether local status has a learner member.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_learner_membership_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_local_learner_membership_present",
        &[("kind", kind.to_string())],
        u64::from(report.learner_membership_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_local_learner_auto_promote_present Whether local status observed learner auto-promotion evidence.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_learner_auto_promote_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_local_learner_auto_promote_present",
        &[("kind", kind.to_string())],
        u64::from(report.learner_auto_promote_present),
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_local_wal_first_log_index First retained WAL log index visible in local status.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_wal_first_log_index gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_local_wal_first_log_index",
        &[("kind", kind.to_string())],
        report.wal_first_log_index,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_local_wal_last_log_index Last retained WAL log index visible in local status.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_wal_last_log_index gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_matrixraft_local_wal_last_log_index",
        &[("kind", kind.to_string())],
        report.wal_last_log_index,
    );
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_role MatrixRaft-style local-status peer role as a labeled gauge.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_role gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_participates_in_quorum Whether a peer participates in quorum.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_participates_in_quorum gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_can_serve_data Whether a peer can serve data reads.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_can_serve_data gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_can_be_leader Whether a peer can become leader.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_can_be_leader gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_match_index MatrixRaft-style local peer match index.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_match_index gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_next_index MatrixRaft-style local peer next index.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_next_index gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_snapshot_sending Whether local status sees a snapshot sender active for the peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_snapshot_sending gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_snapshot_installing Whether local status sees a snapshot install active for the peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_snapshot_installing gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_snapshot_installed_index Last installed snapshot index for the peer.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_snapshot_installed_index gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_transfer_leader_target Whether the peer is the active leader-transfer target.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_transfer_leader_target gauge\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_pre_vote_rejections Pre-vote rejections visible in local status.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_pre_vote_rejections counter\n");
    out.push_str("# HELP temporalstore_raft_matrixraft_local_peer_election_rejections Election rejections visible in local status.\n");
    out.push_str("# TYPE temporalstore_raft_matrixraft_local_peer_election_rejections counter\n");
    for peer in report.peers {
        let labels = &[
            ("kind", kind.to_string()),
            ("node_id", peer.status.node_id.to_string()),
            (
                "role",
                format!("{:?}", peer.status.role).to_ascii_lowercase(),
            ),
            (
                "replica_role",
                format!("{:?}", peer.status.replica_role).to_ascii_lowercase(),
            ),
        ];
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_role",
            labels,
            1,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_participates_in_quorum",
            labels,
            u64::from(peer.participates_in_quorum),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_can_serve_data",
            labels,
            u64::from(peer.can_serve_data),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_can_be_leader",
            labels,
            u64::from(peer.can_be_leader),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_match_index",
            labels,
            peer.pipeline_state.match_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_next_index",
            labels,
            peer.pipeline_state.next_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_snapshot_sending",
            labels,
            u64::from(peer.pipeline_state.snapshot_sending),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_snapshot_installing",
            labels,
            u64::from(peer.pipeline_state.snapshot_installing),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_snapshot_installed_index",
            labels,
            peer.pipeline_state.snapshot_installed_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_transfer_leader_target",
            labels,
            u64::from(peer.pipeline_state.transfer_leader_target),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_pre_vote_rejections",
            labels,
            peer.pipeline_state.pre_vote_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_matrixraft_local_peer_election_rejections",
            labels,
            peer.pipeline_state.election_rejections,
        );
    }
}

fn matrixraft_capability_report_from_matrixraft_admin(
    report: &MatrixRaftRuntimeAdminReport,
) -> MatrixRaftReferenceRaftRuntimeCapabilityReport {
    let capability_evidence = report
        .capability_matrix
        .iter()
        .map(|capability| {
            matrixraft_capability_evidence_from_fields(
                capability.capability.clone(),
                "temporalstore_matrixraft_runtime_admin_report",
                capability
                    .evidence_field
                    .split(';')
                    .map(str::trim)
                    .filter(|field| !field.is_empty())
                    .map(|field| (capability.ready, field)),
            )
        })
        .collect::<Vec<_>>();
    let mut product_blockers = report
        .blockers
        .iter()
        .map(|blocker| format!("temporalstore:blocker:{blocker}"))
        .collect::<Vec<_>>();
    if !report.ready {
        product_blockers.push("temporalstore:blocker:matrixraft_admin_report_not_ready".to_string());
    }
    matrixraft_runtime_capability_report_from_evidence(capability_evidence, product_blockers)
}

fn push_raft_metric(out: &mut String, name: &str, labels: &[(&str, String)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&value.replace('\\', "\\\\").replace('"', "\\\""));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaCommand {
    PutShardLocation(ShardLocation),
    RemoveShard(ShardId),
    ApplyMutation(MetaMutation),
    PersistSchedulerState(RaftPersistedSchedulerState),
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MetaState {
    pub shards: BTreeMap<ShardId, ShardLocation>,
    #[serde(default)]
    pub scheduler_state: Option<RaftPersistedSchedulerState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaLogEntry {
    term: u64,
    index: u64,
    command: MetaCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaRaftSnapshot {
    pub last_included_term: u64,
    pub last_included_index: u64,
    pub state: MetaState,
}

#[derive(Debug)]
struct MetaRaftNode {
    id: RaftNodeId,
    role: RaftRole,
    current_term: u64,
    commit_index: u64,
    alive: bool,
    log: Vec<MetaLogEntry>,
    applied: BTreeSet<u64>,
    installed_snapshot_index: u64,
    installed_snapshot_term: u64,
    state: MetaState,
    meta: SingleNodeMeta,
}

#[derive(Debug, Clone)]
pub struct MetaRaftCluster {
    inner: Arc<RwLock<MetaRaftClusterInner>>,
}

#[derive(Debug)]
struct MetaRaftClusterInner {
    leader_id: RaftNodeId,
    nodes: BTreeMap<RaftNodeId, MetaRaftNode>,
    config: RaftConfig,
}



fn meta_node_status(node: &MetaRaftNode, leader_commit_index: u64) -> RaftNodeStatus {
    RaftNodeStatus {
        node_id: node.id,
        role: node.role,
        replica_role: RaftReplicaRole::Voter,
        current_term: node.current_term,
        commit_index: node.commit_index,
        last_log_index: meta_node_last_log_or_snapshot_index(node),
        applied_index: node.applied.iter().next_back().copied().unwrap_or_default(),
        alive: node.alive,
        lag: leader_commit_index.saturating_sub(node.commit_index),
    }
}

fn meta_node_last_log_or_snapshot_index(node: &MetaRaftNode) -> u64 {
    node.log
        .last()
        .map(|entry| entry.index)
        .unwrap_or(node.installed_snapshot_index)
}

fn meta_node_last_log_or_snapshot_term(node: &MetaRaftNode) -> u64 {
    node.log
        .last()
        .map(|entry| entry.term)
        .unwrap_or(node.installed_snapshot_term)
}

fn new_meta_node(id: RaftNodeId, role: RaftRole) -> MetaRaftNode {
    MetaRaftNode {
        id,
        role,
        current_term: 1,
        commit_index: 0,
        alive: true,
        log: Vec::new(),
        applied: BTreeSet::new(),
        installed_snapshot_index: 0,
        installed_snapshot_term: 0,
        state: MetaState::default(),
        meta: SingleNodeMeta::default(),
    }
}

fn append_meta_entry(node: &mut MetaRaftNode, entry: MetaLogEntry) {
    if node.log.last().map(|last| last.index) >= Some(entry.index) {
        node.log.retain(|existing| existing.index < entry.index);
    }
    node.log.push(entry);
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn apply_meta_committed(node: &mut MetaRaftNode) -> Option<Status> {
    let mut last_status = None;
    for entry in node
        .log
        .iter()
        .filter(|entry| entry.index <= node.commit_index)
    {
        if node.applied.insert(entry.index) {
            match &entry.command {
                MetaCommand::PutShardLocation(location) => {
                    node.state
                        .shards
                        .insert(location.shard_id, location.clone());
                    last_status = Some(node.meta.apply_mutation(MetaMutation::RegisterShard(
                        RegisterShardRequest {
                            shard_id: location.shard_id,
                            server_addr: location.server_addr.clone(),
                        },
                    )));
                }
                MetaCommand::RemoveShard(shard_id) => {
                    node.state.shards.remove(shard_id);
                    last_status = Some(Status::ok());
                }
                MetaCommand::ApplyMutation(mutation) => {
                    last_status = Some(node.meta.apply_mutation(mutation.clone()));
                    match mutation {
                        MetaMutation::RegisterShard(request) => {
                            node.state.shards.insert(
                                request.shard_id,
                                ShardLocation {
                                    state: crate::meta::MetaEntityState::Normal,
                                    shard_id: request.shard_id,
                                    server_addr: request.server_addr.clone(),
                                    latest_snapshot: None,
                                },
                            );
                        }
                        MetaMutation::FinishLoad(request) if request.status.ok => {
                            node.state.shards.insert(
                                request.shard_id,
                                ShardLocation {
                                    state: crate::meta::MetaEntityState::Normal,
                                    shard_id: request.shard_id,
                                    server_addr: request.server_addr.clone(),
                                    latest_snapshot: None,
                                },
                            );
                        }
                        MetaMutation::PublishShardSnapshot(request) => {
                            if let Some(location) = node.state.shards.get_mut(&request.shard_id) {
                                if location.latest_snapshot.as_ref().map_or(true, |existing| {
                                    existing.last_log_index <= request.snapshot.last_log_index
                                }) {
                                    location.latest_snapshot = Some(request.snapshot.clone());
                                }
                            }
                        }
                        _ => {}
                    }
                }
                MetaCommand::PersistSchedulerState(state) => match state.validate() {
                    Ok(()) => {
                        node.state.scheduler_state = Some(state.clone());
                        last_status = Some(Status::ok());
                    }
                    Err(err) => {
                        last_status =
                            Some(Status::error("scheduler_state_rejected", err.to_string()));
                    }
                },
            }
        }
    }
    last_status
}

fn install_meta_snapshot_state(node: &mut MetaRaftNode, snapshot: MetaRaftSnapshot) {
    node.state = snapshot.state;
    node.current_term = node.current_term.max(snapshot.last_included_term);
    node.commit_index = snapshot.last_included_index;
    node.log
        .retain(|entry| entry.index > snapshot.last_included_index);
    node.applied.clear();
    node.applied.extend(1..=snapshot.last_included_index);
    node.installed_snapshot_index = snapshot.last_included_index;
    node.installed_snapshot_term = snapshot.last_included_term;
}

fn install_meta_leader_snapshot_tail(
    node: &mut MetaRaftNode,
    leader_snapshot_index: u64,
    leader_snapshot_term: u64,
    leader_log: Vec<MetaLogEntry>,
    leader_commit_index: u64,
    leader_state: MetaState,
) {
    if meta_node_last_log_or_snapshot_index(node) < leader_snapshot_index {
        node.installed_snapshot_index = leader_snapshot_index;
        node.installed_snapshot_term = leader_snapshot_term;
        node.applied.clear();
        node.applied.extend(1..=leader_snapshot_index);
    }
    node.log = leader_log;
    node.commit_index = leader_commit_index;
    node.state = leader_state;
    apply_meta_committed(node);
}

#[cfg(test)]
mod tests;
