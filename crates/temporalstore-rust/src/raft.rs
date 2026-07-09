use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{
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
    AckResponse, AddNamespaceRequest, AddTableRequest, DeleteTableRequest, DropProxyGroupRequest,
    GetShardResponse, GetTableTopologyRequest, ListNamespacesResponse, ListProxiesResponse,
    ListProxyGroupRequest, ListProxyGroupResponse, ListServersResponse, ListTablesResponse,
    LoadFinishRequest, ManagementInfo, MetaEntityState, MetaInfo, MetaMutation,
    MetaPreflightReport, MetaSnapshot, MetaStats, PartitionStateChangeRequest,
    ProxyHeartbeatRequest, ProxyHeartbeatResponse, PublishShardSnapshotRequest,
    PutProxyGroupRequest, RegisterProxyRequest, RegisterServerRequest, RegisterShardRequest,
    RegisterShardResponse, SafeModePolicy, SafeModeReport, ServerHeartbeatRequest,
    ServerHeartbeatResponse, ShardLocation, ShardSnapshotRef, SingleNodeMeta, StaleResourceReport,
    StaleServerReport, StateChangeRequest, TableTopologyResponse, TopologyVersionReport,
    TopologyVersionRequest, UpdateManageInfoRequest, UpdateServerRequest, UpdateTableRequest,
};
use crate::rebalance::RaftPersistedSchedulerState;
use crate::types::{Command, CommandResponse, ExecuteRequest, ShardId, Status};

mod membership;
mod readiness;
mod rustraft;
use bytes::Bytes;
pub use membership::*;
pub use readiness::*;
pub use rustraft::*;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataRaftLogCodecEntry {
    pub shard_id: ShardId,
    pub raft_index: u64,
    pub log_id: u64,
    pub log_size: u64,
    pub oplog_sequence: u64,
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
    applied_oplog_sequence: u64,
}

impl DataRaftCommittedLogApplier {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            applied_raft_index: 0,
            applied_oplog_sequence: 0,
        }
    }

    pub fn applied_raft_index(&self) -> u64 {
        self.applied_raft_index
    }

    pub fn applied_oplog_sequence(&self) -> u64 {
        self.applied_oplog_sequence
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
        let response = engine.execute_durable(ExecuteRequest {
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
        self.applied_oplog_sequence = entry.oplog_sequence;
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
pub struct ByteRaftPeerPipelineState {
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

impl ByteRaftPeerPipelineState {
    fn to_rustraft_peer_pipeline_status(&self) -> RustRaftPeerPipelineStatus {
        rustraft_peer_pipeline_status_from_observed(&RustRaftObservedPeerPipeline {
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
pub struct ByteRaftCapabilityEvidence {
    pub capability: String,
    pub ready: bool,
    pub evidence_field: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteRaftRuntimeAdminReport {
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
    pub peer_pipeline_states: Vec<ByteRaftPeerPipelineState>,
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
    pub capability_matrix: Vec<ByteRaftCapabilityEvidence>,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteRaftLeaderElectionParityReport {
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
pub struct ByteRaftLocalPeerStatus {
    pub status: RaftNodeStatus,
    pub pipeline_state: ByteRaftPeerPipelineState,
    pub participates_in_quorum: bool,
    pub can_serve_data: bool,
    pub can_be_leader: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteRaftLocalStatusReport {
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
    pub peers: Vec<ByteRaftLocalPeerStatus>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RaftWalRecord {
    pub hard_state: RaftHardState,
    pub membership: RaftMembership,
    #[serde(default)]
    pub replica_role: RaftReplicaRole,
    #[serde(default)]
    pub joint_membership: Option<JointConsensusMembership>,
    #[serde(default)]
    pub latest_external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    #[serde(default)]
    pub installed_snapshot: Option<RaftSnapshot>,
    #[serde(default)]
    pub apply_snapshot_fence: RaftApplySnapshotFence,
    #[serde(default)]
    pub storage_apply_fence: RaftStorageApplyFence,
    #[serde(default)]
    pub pipeline_state: RaftPeerPipelineRuntimeState,
    #[serde(default)]
    pub read_safety_state: RaftReadSafetyRuntimeState,
    #[serde(default)]
    pub membership_evidence: RaftMembershipRuntimeEvidence,
    pub entries: Vec<RaftLogEntry>,
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
    fn record_rustraft_runtime_decision(&mut self, decision: &RustRaftReadSafetyRuntimeDecision) {
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

#[derive(Debug, Clone)]
pub struct LocalRaftWal {
    root: PathBuf,
}

impl LocalRaftWal {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn persist_node(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
    ) -> io::Result<()> {
        self.append_node_record(shard_id, node_id, record)
    }

    pub fn append_node_record(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
    ) -> io::Result<()> {
        let path = self.node_path(shard_id, node_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let recovery = self.recover_node(shard_id, node_id)?;
        let envelope = RaftWalEnvelope {
            sequence: recovery.valid_records as u64 + 1,
            checksum: raft_wal_checksum(record)?,
            record: record.clone(),
        };
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        serde_json::to_writer(&mut file, &envelope).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    pub fn persist_node_with_retention(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        keep_last: usize,
    ) -> io::Result<()> {
        self.append_node_record(shard_id, node_id, record)?;
        self.compact_node_records(shard_id, node_id, keep_last)
    }

    pub fn persist_node_segmented(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
    ) -> io::Result<RaftWalSegmentReport> {
        self.persist_node_segmented_with_fsync_threshold(
            shard_id,
            node_id,
            record,
            max_segment_bytes,
            min_keep_segments,
            u64::MAX,
        )
    }

    pub fn persist_node_segmented_with_fsync_threshold(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
        slow_fsync_threshold_ms: u64,
    ) -> io::Result<RaftWalSegmentReport> {
        let max_segment_bytes = max_segment_bytes.max(1);
        let min_keep_segments = min_keep_segments.max(1);
        let segment_dir = self.node_segment_dir(shard_id, node_id);
        fs::create_dir_all(&segment_dir)?;
        let recovery = self.recover_node_segmented(shard_id, node_id)?;
        let sequence = recovery.valid_records as u64 + 1;
        let envelope = RaftWalEnvelope {
            sequence,
            checksum: raft_wal_checksum(record)?,
            record: record.clone(),
        };
        let mut encoded = Vec::new();
        serde_json::to_writer(&mut encoded, &envelope).map_err(io::Error::other)?;
        encoded.push(b'\n');

        let mut active_segment_id = self
            .node_segments(shard_id, node_id)?
            .last()
            .map(|segment| segment.segment_id)
            .unwrap_or(1);
        let mut active_path = self.node_segment_path(shard_id, node_id, active_segment_id);
        let active_len = fs::metadata(&active_path)
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        if active_len > 0 && active_len + encoded.len() as u64 > max_segment_bytes {
            active_segment_id += 1;
            active_path = self.node_segment_path(shard_id, node_id, active_segment_id);
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        file.write_all(&encoded)?;
        let fsync_started = Instant::now();
        file.sync_data()?;
        let last_fsync_elapsed_ms = fsync_started.elapsed().as_millis() as u64;
        let before_prune_segments = self.node_segments(shard_id, node_id)?.len();
        self.prune_node_segments(shard_id, node_id, min_keep_segments)?;
        let after_prune_segments = self.node_segments(shard_id, node_id)?.len();
        let mut report = self.segment_report(shard_id, node_id)?;
        report.released_segment_count =
            before_prune_segments.saturating_sub(after_prune_segments) as u64;
        report.last_fsync_elapsed_ms = last_fsync_elapsed_ms;
        report.slow_fsync_backpressure_observed = last_fsync_elapsed_ms >= slow_fsync_threshold_ms;
        self.persist_segment_runtime_state(shard_id, node_id, &report)?;
        Ok(report)
    }

    pub fn recover_node_segmented(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalRecovery> {
        let segments = self.node_segments(shard_id, node_id)?;
        if segments.is_empty() {
            return self.recover_node_legacy(shard_id, node_id);
        }

        let mut last_record = None;
        let mut valid_records = 0usize;
        let mut truncated_bytes = 0u64;
        let mut corrupt_tail = false;
        for segment in segments {
            let bytes = fs::read(&segment.path)?;
            let mut offset = 0usize;
            let mut valid_until = 0usize;
            while offset < bytes.len() {
                let remaining = &bytes[offset..];
                let line_len = remaining
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map(|pos| pos + 1)
                    .unwrap_or(remaining.len());
                let raw_line = &remaining[..line_len];
                let line = raw_line.strip_suffix(b"\n").unwrap_or(raw_line);
                if line.is_empty() {
                    valid_until = offset + line_len;
                    offset += line_len;
                    continue;
                }
                let Ok(envelope) = serde_json::from_slice::<RaftWalEnvelope>(line) else {
                    break;
                };
                if envelope.checksum != raft_wal_checksum(&envelope.record)? {
                    break;
                }
                valid_records += 1;
                last_record = Some(envelope.record);
                valid_until = offset + line_len;
                offset += line_len;
            }
            let segment_truncated = bytes.len().saturating_sub(valid_until) as u64;
            if segment_truncated > 0 {
                OpenOptions::new()
                    .write(true)
                    .open(&segment.path)?
                    .set_len(valid_until as u64)?;
                truncated_bytes += segment_truncated;
                corrupt_tail = true;
                break;
            }
        }

        Ok(RaftWalRecovery {
            record: last_record,
            valid_records,
            truncated_bytes,
            corrupt_tail,
        })
    }

    pub fn compact_node_records(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        keep_last: usize,
    ) -> io::Result<()> {
        let keep_last = keep_last.max(1);
        let path = self.node_path(shard_id, node_id);
        if !path.exists() {
            return Ok(());
        }
        let bytes = fs::read(&path)?;
        let mut envelopes = Vec::new();
        let mut offset = 0usize;
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            let line_len = remaining
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|pos| pos + 1)
                .unwrap_or(remaining.len());
            let raw_line = &remaining[..line_len];
            let line = raw_line.strip_suffix(b"\n").unwrap_or(raw_line);
            if line.is_empty() {
                offset += line_len;
                continue;
            }
            let Ok(envelope) = serde_json::from_slice::<RaftWalEnvelope>(line) else {
                break;
            };
            if envelope.checksum != raft_wal_checksum(&envelope.record)? {
                break;
            }
            envelopes.push(envelope);
            offset += line_len;
        }
        if envelopes.len() <= keep_last {
            return Ok(());
        }
        let retained = envelopes.split_off(envelopes.len() - keep_last);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        for envelope in retained {
            serde_json::to_writer(&mut file, &envelope).map_err(io::Error::other)?;
            file.write_all(b"\n")?;
        }
        file.sync_data()?;
        Ok(())
    }

    pub fn load_node(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<Option<RaftWalRecord>> {
        self.recover_node_segmented(shard_id, node_id)
            .map(|recovery| recovery.record)
    }

    pub fn recover_node(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalRecovery> {
        self.recover_node_segmented(shard_id, node_id)
    }

    fn recover_node_legacy(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalRecovery> {
        let path = self.node_path(shard_id, node_id);
        if !path.exists() {
            return Ok(RaftWalRecovery {
                record: None,
                valid_records: 0,
                truncated_bytes: 0,
                corrupt_tail: false,
            });
        }
        let bytes = fs::read(&path)?;
        if bytes.is_empty() {
            return Ok(RaftWalRecovery {
                record: None,
                valid_records: 0,
                truncated_bytes: 0,
                corrupt_tail: false,
            });
        }
        if let Ok(record) = serde_json::from_slice::<RaftWalRecord>(&bytes) {
            return Ok(RaftWalRecovery {
                record: Some(record),
                valid_records: 1,
                truncated_bytes: 0,
                corrupt_tail: false,
            });
        }

        let mut offset = 0usize;
        let mut valid_until = 0usize;
        let mut last_record = None;
        let mut valid_records = 0usize;
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            let line_len = remaining
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|pos| pos + 1)
                .unwrap_or(remaining.len());
            let raw_line = &remaining[..line_len];
            let line = raw_line.strip_suffix(b"\n").unwrap_or(raw_line);
            if line.is_empty() {
                valid_until = offset + line_len;
                offset += line_len;
                continue;
            }
            let Ok(envelope) = serde_json::from_slice::<RaftWalEnvelope>(line) else {
                break;
            };
            if envelope.checksum != raft_wal_checksum(&envelope.record)? {
                break;
            }
            valid_records += 1;
            last_record = Some(envelope.record);
            valid_until = offset + line_len;
            offset += line_len;
        }

        let truncated_bytes = bytes.len().saturating_sub(valid_until) as u64;
        if truncated_bytes > 0 {
            OpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(valid_until as u64)?;
        }
        Ok(RaftWalRecovery {
            record: last_record,
            valid_records,
            truncated_bytes,
            corrupt_tail: truncated_bytes > 0,
        })
    }

    fn node_path(&self, shard_id: ShardId, node_id: RaftNodeId) -> PathBuf {
        self.root
            .join(format!("shard-{shard_id}"))
            .join(format!("node-{node_id}.json"))
    }

    fn node_segment_dir(&self, shard_id: ShardId, node_id: RaftNodeId) -> PathBuf {
        self.root
            .join(format!("shard-{shard_id}"))
            .join(format!("node-{node_id}.segments"))
    }

    fn node_segment_path(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        segment_id: u64,
    ) -> PathBuf {
        self.node_segment_dir(shard_id, node_id)
            .join(format!("{segment_id:020}.wal"))
    }

    fn node_segment_runtime_state_path(&self, shard_id: ShardId, node_id: RaftNodeId) -> PathBuf {
        self.node_segment_dir(shard_id, node_id)
            .join("segment-runtime-state.json")
    }

    fn persist_segment_runtime_state(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        report: &RaftWalSegmentReport,
    ) -> io::Result<()> {
        let state = RaftWalSegmentRuntimeState {
            released_segment_count: report.released_segment_count,
            last_fsync_elapsed_ms: report.last_fsync_elapsed_ms,
            slow_fsync_backpressure_observed: report.slow_fsync_backpressure_observed,
        };
        let encoded = serde_json::to_vec_pretty(&state).map_err(io::Error::other)?;
        fs::write(
            self.node_segment_runtime_state_path(shard_id, node_id),
            encoded,
        )
    }

    fn read_segment_runtime_state(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> RaftWalSegmentRuntimeState {
        fs::read(self.node_segment_runtime_state_path(shard_id, node_id))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<RaftWalSegmentRuntimeState>(&bytes).ok())
            .unwrap_or_default()
    }

    fn node_segments(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<Vec<RaftWalSegmentInfo>> {
        let dir = self.node_segment_dir(shard_id, node_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut segments = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Ok(segment_id) = stem.parse::<u64>() else {
                continue;
            };
            let (record_count, first_sequence, last_sequence, first_log_index, last_log_index) =
                Self::inspect_segment_sequences(&path)?;
            segments.push(RaftWalSegmentInfo {
                segment_id,
                bytes: entry.metadata()?.len(),
                record_count,
                first_sequence,
                last_sequence,
                first_log_index,
                last_log_index,
                path: path.to_string_lossy().into_owned(),
            });
        }
        segments.sort_by_key(|segment| segment.segment_id);
        Ok(segments)
    }

    fn inspect_segment_sequences(path: &Path) -> io::Result<(u64, u64, u64, u64, u64)> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut record_count = 0u64;
        let mut first_sequence = 0u64;
        let mut last_sequence = 0u64;
        let mut first_log_index = 0u64;
        let mut last_log_index = 0u64;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let Ok(envelope) = serde_json::from_str::<RaftWalEnvelope>(&line) else {
                continue;
            };
            record_count = record_count.saturating_add(1);
            if first_sequence == 0 {
                first_sequence = envelope.sequence;
            }
            last_sequence = envelope.sequence;
            let record_first_log_index = envelope
                .record
                .entries
                .first()
                .map(|entry| entry.index)
                .unwrap_or(envelope.record.hard_state.commit_index);
            let record_last_log_index = envelope
                .record
                .entries
                .last()
                .map(|entry| entry.index)
                .unwrap_or(envelope.record.hard_state.commit_index);
            if first_log_index == 0
                || (record_first_log_index > 0 && record_first_log_index < first_log_index)
            {
                first_log_index = record_first_log_index;
            }
            last_log_index = last_log_index.max(record_last_log_index);
        }
        Ok((
            record_count,
            first_sequence,
            last_sequence,
            first_log_index,
            last_log_index,
        ))
    }

    fn prune_node_segments(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        min_keep_segments: usize,
    ) -> io::Result<()> {
        let segments = self.node_segments(shard_id, node_id)?;
        if segments.len() <= min_keep_segments {
            return Ok(());
        }
        for segment in segments
            .iter()
            .take(segments.len().saturating_sub(min_keep_segments))
        {
            fs::remove_file(&segment.path)?;
        }
        Ok(())
    }

    pub fn segment_report(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<RaftWalSegmentReport> {
        let segments = self.node_segments(shard_id, node_id)?;
        let runtime_state = self.read_segment_runtime_state(shard_id, node_id);
        let first_retained_log_index = segments
            .iter()
            .find_map(|segment| (segment.first_log_index > 0).then_some(segment.first_log_index))
            .unwrap_or_default();
        let last_retained_log_index = segments
            .iter()
            .rev()
            .find_map(|segment| (segment.last_log_index > 0).then_some(segment.last_log_index))
            .unwrap_or_default();
        Ok(RaftWalSegmentReport {
            active_segment_id: segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            segments,
            released_segment_count: runtime_state.released_segment_count,
            first_retained_log_index,
            last_retained_log_index,
            last_fsync_elapsed_ms: runtime_state.last_fsync_elapsed_ms,
            slow_fsync_backpressure_observed: runtime_state.slow_fsync_backpressure_observed,
        })
    }
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
        Ok(post_json_with_options(
            self.peer_addr(request.target_id)?,
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

pub fn handle_raft_http(cluster: &RaftCluster, request: HttpRequest) -> (u16, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
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
}

impl ProductionRaftRuntimeOptions {
    pub fn validate(&self) -> Result<(), RaftError> {
        self.config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        if self.nodes.is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires at least one node".to_string(),
            ));
        }
        let mut node_ids = BTreeSet::new();
        let mut node_addrs = BTreeSet::new();
        for node in &self.nodes {
            if node.node_id == 0 {
                return Err(RaftError::InvalidConfig(
                    "production raft node_id must be non-zero".to_string(),
                ));
            }
            if node.addr.trim().is_empty() {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft node {} requires a non-empty addr",
                    node.node_id
                )));
            }
            if !node_ids.insert(node.node_id) {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft node_id {} is duplicated",
                    node.node_id
                )));
            }
            if !node_addrs.insert(node.addr.trim().to_string()) {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft addr {} is duplicated",
                    node.addr
                )));
            }
        }
        if !self
            .nodes
            .iter()
            .any(|node| node.node_id == self.local_node_id)
        {
            return Err(RaftError::InvalidConfig(
                "local_node_id must be present in production raft nodes".to_string(),
            ));
        }
        if self.wal_dir.trim().is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires wal_dir".to_string(),
            ));
        }
        if self.heartbeat_interval_ms == 0 || self.election_tick_ms == 0 {
            return Err(RaftError::InvalidConfig(
                "production raft heartbeat/election intervals must be non-zero".to_string(),
            ));
        }
        if self.max_catchup_entries_per_heartbeat == 0 {
            return Err(RaftError::InvalidConfig(
                "production raft max_catchup_entries_per_heartbeat must be non-zero".to_string(),
            ));
        }
        self.security
            .validate(self.allow_plaintext_for_local_chaos)?;
        Ok(())
    }

    fn peer_map(&self) -> BTreeMap<RaftNodeId, String> {
        self.nodes
            .iter()
            .filter(|node| node.node_id != self.local_node_id)
            .map(|node| (node.node_id, node.addr.clone()))
            .collect()
    }

    fn node_addr(&self, node_id: RaftNodeId) -> Option<&str> {
        self.nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(|node| node.addr.as_str())
    }

    fn voter_ids(&self) -> Vec<RaftNodeId> {
        self.nodes.iter().map(|node| node.node_id).collect()
    }
}

#[derive(Debug, Clone)]
pub struct ProductionRaftRuntime {
    options: ProductionRaftRuntimeOptions,
    cluster: RaftCluster,
}

impl ProductionRaftRuntime {
    pub fn start(options: ProductionRaftRuntimeOptions) -> Result<Self, RaftError> {
        options.validate()?;
        let cluster = RaftCluster::restore_single_shard_from_wal(
            &options.wal_dir,
            options.shard_id,
            options.voter_ids(),
            options.config.clone(),
        )?;
        Ok(Self { options, cluster })
    }

    pub fn cluster(&self) -> RaftCluster {
        self.cluster.clone()
    }

    pub fn peer_auth_token(&self) -> Option<&str> {
        self.options.security.auth_token.as_deref()
    }

    pub fn local_node_id(&self) -> RaftNodeId {
        self.options.local_node_id
    }

    pub fn local_apply_health(&self, max_allowed_apply_lag: u64) -> RaftApplyHealth {
        self.cluster
            .observer_apply_health(self.options.local_node_id, max_allowed_apply_lag)
    }

    pub fn data_node_atomic_durability_report(&self) -> RaftDataNodeAtomicDurabilityReport {
        let status = self.cluster.status();
        let local_status = status
            .nodes
            .iter()
            .find(|node| node.node_id == self.options.local_node_id);
        let wal_record = self
            .cluster
            .wal_records()
            .into_iter()
            .find(|(node_id, _)| *node_id == self.options.local_node_id)
            .map(|(_, record)| record);

        let mut blockers = Vec::new();
        let mut commit_index = 0;
        let mut applied_index = 0;
        if let Some(local_status) = local_status {
            commit_index = local_status.commit_index;
            applied_index = local_status.applied_index;
            if local_status.applied_index < local_status.commit_index {
                blockers.push("local_applied_index_lags_commit_index".to_string());
            }
        } else {
            blockers.push("local_node_status_missing".to_string());
        }

        let mut wal_commit_index = 0;
        let mut fence = RaftStorageApplyFence::default();
        let mut storage_apply_fence_valid = false;
        if let Some(record) = wal_record {
            wal_commit_index = record.hard_state.commit_index;
            fence = record.storage_apply_fence.clone();
            match validate_raft_storage_apply_fence(&record) {
                Ok(()) => storage_apply_fence_valid = true,
                Err(err) => blockers.push(format!("storage_apply_fence_invalid:{err}")),
            }
            if record.hard_state.commit_index != commit_index {
                blockers.push("wal_commit_index_mismatch".to_string());
            }
            if record.storage_apply_fence.applied_index != applied_index {
                blockers.push("storage_fence_applied_index_mismatch".to_string());
            }
        } else {
            blockers.push("local_wal_record_missing".to_string());
        }

        let storage_mutation_atomic_commit_present = storage_apply_fence_valid
            && wal_commit_index == commit_index
            && fence.applied_index == applied_index;
        let snapshot_install_atomic_commit_present =
            storage_apply_fence_valid && fence.storage_epoch >= fence.applied_index;
        if !storage_mutation_atomic_commit_present {
            blockers.push("storage_mutation_atomic_commit_missing".to_string());
        }
        if !snapshot_install_atomic_commit_present {
            blockers.push("snapshot_install_atomic_commit_missing".to_string());
        }

        RaftDataNodeAtomicDurabilityReport {
            node_id: self.options.local_node_id,
            shard_id: self.options.shard_id,
            commit_index,
            applied_index,
            wal_commit_index,
            fence_committed_index: fence.committed_index,
            fence_applied_index: fence.applied_index,
            storage_epoch: fence.storage_epoch,
            snapshot_id: fence.snapshot_id,
            storage_apply_fence_valid,
            storage_mutation_atomic_commit_present,
            snapshot_install_atomic_commit_present,
            ready: blockers.is_empty(),
            blockers,
        }
    }

    pub fn transport(&self) -> RaftRpcRuntime<AuthenticatedRaftTransport<HttpRaftTransport>> {
        let http_options = HttpRequestOptions {
            connect_timeout_ms: self.options.rpc.deadline_ms,
            io_timeout_ms: self.options.rpc.deadline_ms,
            max_retries: self.options.rpc.max_retries,
        };
        let http = HttpRaftTransport::with_options(self.options.peer_map(), http_options);
        let auth = AuthenticatedRaftTransport::new(
            http,
            self.options
                .security
                .auth_token
                .clone()
                .expect("validated production raft auth token"),
        );
        RaftRpcRuntime::with_auth_token(
            auth,
            self.options.rpc,
            self.options.security.auth_token.clone(),
        )
    }

    pub fn propose(&self, command: Command) -> Result<CommandResponse, RaftError> {
        if self.cluster.leader_id() != self.options.local_node_id {
            return Err(RaftError::NotLeader {
                node_id: self.options.local_node_id,
            });
        }
        let transport = self.transport();
        self.cluster.propose_distributed(command, &transport)
    }

    pub fn apply_membership_change_safely(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        self.cluster.apply_membership_change_safely(new_voters)
    }

    pub fn transfer_leader(&self, target_id: RaftNodeId) -> Result<(), RaftError> {
        if self.cluster.leader_id() == target_id {
            return Ok(());
        }
        if self.cluster.leader_id() != self.options.local_node_id {
            return Err(RaftError::NotLeader {
                node_id: self.options.local_node_id,
            });
        }
        let transport = self.transport();
        let append = self.cluster.build_append_entries_request(target_id)?;
        match transport.append_entries(append) {
            Ok(response) if response.success => {
                let _ = self
                    .cluster
                    .record_append_entries_response(target_id, &response);
            }
            Ok(response) => {
                let _ = self
                    .cluster
                    .record_append_entries_response(target_id, &response);
                let snapshot = self.cluster.build_install_snapshot_request(target_id)?;
                let response = transport.install_snapshot(snapshot)?;
                if !response.success {
                    return Err(RaftError::Transport(format!(
                        "snapshot install rejected by node {target_id}: {:?}",
                        response.reject_reason
                    )));
                }
            }
            _ => {
                let snapshot = self.cluster.build_install_snapshot_request(target_id)?;
                let response = transport.install_snapshot(snapshot)?;
                if !response.success {
                    return Err(RaftError::Transport(format!(
                        "snapshot install rejected by node {target_id}: {:?}",
                        response.reject_reason
                    )));
                }
            }
        }
        self.cluster.catch_up(target_id)?;
        self.cluster.transfer_leader(target_id)?;
        if target_id != self.options.local_node_id {
            let addr = self
                .options
                .node_addr(target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            let status: Status = post_json_with_options(
                addr,
                "/raft/control/accept_leadership",
                &RaftControlLeadershipRequest { node_id: target_id },
                HttpRequestOptions {
                    connect_timeout_ms: self.options.rpc.deadline_ms,
                    io_timeout_ms: self.options.rpc.deadline_ms,
                    max_retries: self.options.rpc.max_retries,
                },
            )
            .map_err(|err| RaftError::Transport(err.to_string()))?;
            if !status.ok {
                return Err(RaftError::Transport(status.message));
            }
        }
        Ok(())
    }

    pub fn read_local(
        &self,
        node_id: RaftNodeId,
        command: Command,
    ) -> Result<CommandResponse, RaftError> {
        self.cluster.read_index(node_id)?;
        self.cluster.read_from_replica(node_id, command)
    }

    pub fn wait_for_applied_index(
        &self,
        node_id: RaftNodeId,
        index: u64,
        timeout_ms: u64,
    ) -> Result<(), RaftError> {
        self.cluster
            .wait_for_applied_index(node_id, index, timeout_ms)
    }

    pub fn start_timer_loop(&self) -> ProductionRaftTimerHandle {
        let cluster = self.cluster.clone();
        let local_node_id = self.options.local_node_id;
        let heartbeat_interval = Duration::from_millis(self.options.heartbeat_interval_ms);
        let election_tick = Duration::from_millis(self.options.election_tick_ms);
        let max_catchup_entries_per_heartbeat = self.options.max_catchup_entries_per_heartbeat;
        let peer_map = self.options.peer_map();
        let peer_ids = peer_map.keys().copied().collect::<Vec<_>>();
        let http_options = HttpRequestOptions {
            connect_timeout_ms: self.options.rpc.deadline_ms,
            io_timeout_ms: self.options.rpc.deadline_ms,
            max_retries: self.options.rpc.max_retries,
        };
        let rpc_options = self.options.rpc;
        let auth_token = self
            .options
            .security
            .auth_token
            .clone()
            .expect("validated production raft auth token");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut last_heartbeat = InstantCompat::now();
            while !stop_thread.load(Ordering::SeqCst) {
                let _ = cluster.tick_election();
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    if cluster.leader_id() == local_node_id {
                        let transport = RaftRpcRuntime::with_auth_token(
                            AuthenticatedRaftTransport::new(
                                HttpRaftTransport::with_options(peer_map.clone(), http_options),
                                auth_token.clone(),
                            ),
                            rpc_options,
                            Some(auth_token.clone()),
                        );
                        let mut sent = 0;
                        for target_id in &peer_ids {
                            if sent >= max_catchup_entries_per_heartbeat {
                                break;
                            }
                            let Ok(request) = cluster.build_append_entries_request(*target_id)
                            else {
                                continue;
                            };
                            let entry_count = request.entries.len() as u64;
                            if let Ok(response) = transport.append_entries(request) {
                                let success = response.success;
                                let _ =
                                    cluster.record_append_entries_response(*target_id, &response);
                                if success {
                                    sent += entry_count.max(1);
                                }
                            }
                        }
                    }
                    last_heartbeat = InstantCompat::now();
                }
                thread::sleep(election_tick);
            }
        });
        ProductionRaftTimerHandle { stop, handle }
    }

    pub fn status(&self) -> RaftClusterStatus {
        self.cluster.status()
    }

    pub fn validate_ready(&self) -> Result<(), RaftError> {
        self.options.validate()?;
        let status = self.cluster.status();
        if !status.has_majority {
            return Err(RaftError::NoMajority {
                live: status.live_voters,
                required: status.majority,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ProductionRaftTimerHandle {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

impl ProductionRaftTimerHandle {
    pub fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.handle.join();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionMetaRaftRuntimeOptions {
    pub engine: ProductionRaftEngineKind,
    pub local_node_id: RaftNodeId,
    pub nodes: Vec<ProductionRaftNode>,
    pub config: RaftConfig,
    pub heartbeat_interval_ms: u64,
    pub election_tick_ms: u64,
    pub failure_detector_interval_ms: u64,
    pub stale_server_after_ms: u64,
}

impl ProductionMetaRaftRuntimeOptions {
    pub fn validate(&self) -> Result<(), RaftError> {
        self.config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        if self.nodes.is_empty() {
            return Err(RaftError::InvalidConfig(
                "production meta raft requires at least one node".to_string(),
            ));
        }
        if !self
            .nodes
            .iter()
            .any(|node| node.node_id == self.local_node_id)
        {
            return Err(RaftError::InvalidConfig(
                "local_node_id must be present in production meta raft nodes".to_string(),
            ));
        }
        if self.heartbeat_interval_ms == 0
            || self.election_tick_ms == 0
            || self.failure_detector_interval_ms == 0
        {
            return Err(RaftError::InvalidConfig(
                "production meta raft intervals must be non-zero".to_string(),
            ));
        }
        Ok(())
    }

    fn voter_ids(&self) -> Vec<RaftNodeId> {
        self.nodes.iter().map(|node| node.node_id).collect()
    }
}

#[derive(Debug, Clone)]
pub struct ProductionMetaRaftRuntime {
    options: ProductionMetaRaftRuntimeOptions,
    cluster: MetaRaftCluster,
}

impl ProductionMetaRaftRuntime {
    pub fn start(options: ProductionMetaRaftRuntimeOptions) -> Result<Self, RaftError> {
        options.validate()?;
        let cluster =
            MetaRaftCluster::new_with_config(options.voter_ids(), options.config.clone())?;
        Ok(Self { options, cluster })
    }

    pub fn cluster(&self) -> MetaRaftCluster {
        self.cluster.clone()
    }

    pub fn status(&self) -> RaftClusterStatus {
        self.cluster.status()
    }

    pub fn local_node_id(&self) -> RaftNodeId {
        self.options.local_node_id
    }

    pub fn node_addr(&self, node_id: RaftNodeId) -> Option<&str> {
        self.options
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(|node| node.addr.as_str())
    }

    pub fn validate_ready(&self) -> Result<(), RaftError> {
        self.options.validate()?;
        let status = self.status();
        if !status.has_majority {
            return Err(RaftError::NoMajority {
                live: status.live_voters,
                required: status.majority,
            });
        }
        Ok(())
    }

    pub fn propose(&self, command: MetaCommand) -> Result<(), RaftError> {
        self.cluster.propose(command)
    }

    pub fn propose_mutation(&self, mutation: MetaMutation) -> Result<Status, RaftError> {
        self.cluster.propose_mutation(mutation)
    }

    pub fn list_membership(&self) -> Vec<RaftNodeId> {
        self.status()
            .nodes
            .into_iter()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.node_id)
            .collect()
    }

    pub fn add_node(
        &self,
        node_id: RaftNodeId,
        role: RaftReplicaRole,
    ) -> Result<RaftScaleChangeReport, RaftError> {
        if !role.participates_in_quorum() || matches!(role, RaftReplicaRole::Witness) {
            return Err(RaftError::InvalidConfig(format!(
                "metaserver raft currently supports voter membership only, requested {role:?}"
            )));
        }
        self.cluster.add_node_safely(node_id)
    }

    pub fn remove_node(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.cluster.remove_node_safely(node_id)
    }

    pub fn apply_membership(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        self.cluster.apply_membership_change_safely(new_voters)
    }

    pub fn drive_data_raft_membership_workflow(
        &self,
        data_cluster: &RaftCluster,
        learner_id: RaftNodeId,
        requested_leader_id: Option<RaftNodeId>,
        remove_voter_id: Option<RaftNodeId>,
    ) -> Result<MetaDataRaftMembershipWorkflowReport, RaftError> {
        self.validate_ready()?;
        let shard_id = data_cluster.shard_id();
        let initial_status = data_cluster.status();
        let initial_voters = initial_status
            .nodes
            .iter()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        let required_catch_up_index = initial_status.commit_index;
        let mut learner_added = false;
        if data_cluster.local_status(learner_id).is_err() {
            data_cluster.add_node_with_role(learner_id, RaftReplicaRole::Learner)?;
            learner_added = true;
        }

        data_cluster.catch_up(learner_id)?;
        let learner_status = data_cluster.local_status(learner_id)?;
        let catch_up_verified = learner_status.lag == 0;
        let learner_catch_up_index = learner_status.commit_index;
        if !catch_up_verified {
            return Err(RaftError::ReplicaLagging {
                replica_id: learner_id,
                replica_commit_index: learner_status.commit_index,
                leader_commit_index: data_cluster.status().commit_index,
            });
        }

        let mut target_voters = data_cluster.membership().voters;
        if !target_voters.contains(&learner_id) {
            target_voters.push(learner_id);
        }
        target_voters.sort_unstable();
        target_voters.dedup();
        data_cluster.begin_joint_consensus(target_voters)?;
        data_cluster.promote_learner_to_voter(learner_id)?;
        if let Err(err) = data_cluster.catch_up_live_followers() {
            let _ = data_cluster.abort_joint_consensus();
            return Err(err);
        }
        let committed_membership = match data_cluster.commit_joint_consensus() {
            Ok(membership) => membership,
            Err(err) => {
                let _ = data_cluster.abort_joint_consensus();
                return Err(err);
            }
        };
        let voters_after_promote = committed_membership.voters.clone();

        let mut leader_transferred = false;
        if let Some(target_leader_id) = requested_leader_id {
            data_cluster.transfer_leader(target_leader_id)?;
            leader_transferred = data_cluster.leader_id() == target_leader_id;
        }

        let mut voter_removed = false;
        if let Some(remove_voter_id) = remove_voter_id {
            data_cluster.remove_node_safely(remove_voter_id)?;
            voter_removed = true;
        }

        let final_status = data_cluster.status();
        let final_voters = final_status
            .nodes
            .iter()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        Ok(MetaDataRaftMembershipWorkflowReport {
            shard_id,
            learner_id,
            removed_voter_id: remove_voter_id,
            requested_leader_id,
            initial_voters,
            learner_added,
            catch_up_verified,
            learner_catch_up_index,
            required_catch_up_index,
            promoted_to_voter: true,
            membership_committed: !committed_membership.voters.is_empty(),
            voters_after_promote,
            leader_transferred,
            voter_removed,
            final_leader_id: final_status.leader_id,
            final_voters,
            commit_index: final_status.commit_index,
        })
    }

    pub fn trigger_snapshot(&self) -> Result<MetaRaftSnapshot, RaftError> {
        self.cluster.create_snapshot()
    }

    pub fn wait_for_log_applied(&self) -> Result<ReadIndexResponse, RaftError> {
        self.cluster.read_index(self.options.local_node_id)
    }

    pub fn read_index(&self, node_id: RaftNodeId) -> Result<ReadIndexResponse, RaftError> {
        self.cluster.read_index(node_id)
    }

    pub fn transfer_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        self.cluster.transfer_leader(node_id)
    }

    pub fn start_timer_loop(&self) -> ProductionRaftTimerHandle {
        let cluster = self.cluster.clone();
        let heartbeat_interval = Duration::from_millis(self.options.heartbeat_interval_ms);
        let election_tick = Duration::from_millis(self.options.election_tick_ms);
        let failure_detector_interval =
            Duration::from_millis(self.options.failure_detector_interval_ms);
        let stale_server_after_ms = self.options.stale_server_after_ms;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut last_heartbeat = InstantCompat::now();
            let mut last_failure_detector = InstantCompat::now();
            while !stop_thread.load(Ordering::SeqCst) {
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    let _ = cluster.failover_primary();
                    let _ = cluster.catch_up_live_followers();
                    last_heartbeat = InstantCompat::now();
                }
                if stale_server_after_ms > 0
                    && last_failure_detector.elapsed() >= failure_detector_interval
                {
                    let _ = cluster.freeze_stale_servers(stale_server_after_ms);
                    last_failure_detector = InstantCompat::now();
                }
                thread::sleep(election_tick);
            }
        });
        ProductionRaftTimerHandle { stop, handle }
    }
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
                let response = engine.execute_durable(ExecuteRequest {
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
            send_snapshot_timeout_ms: 60_000,
            raft_transport_timeout_ms: 1_000,
            wal_sync: false,
            max_segment_bytes: 64 * 1024 * 1024,
            min_keep_segment_num: 2,
            can_trigger_snapshot: true,
            max_applied_log_bytes: 1024 * 1024 * 1024,
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
    if entry.oplog_sequence == 0 {
        return Err(RaftError::InvalidDataRaftLog(
            "oplog sequence must be nonzero".to_string(),
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
    push_u64_le(&mut bytes, entry.oplog_sequence);
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
    let oplog_sequence = read_u64_le(bytes, 40)?;
    let payload_size = read_u64_le(bytes, 48)?;
    if oplog_sequence == 0 {
        return Err(RaftError::InvalidDataRaftLog(
            "oplog sequence must be nonzero".to_string(),
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
        oplog_sequence,
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

#[derive(Debug, Clone)]
pub struct RaftCluster {
    inner: Arc<RwLock<RaftClusterInner>>,
}

#[derive(Debug)]
struct RaftClusterInner {
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
}

impl RaftCluster {
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
            inner: Arc::new(RwLock::new(RaftClusterInner {
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
        refresh_all_pipeline_states(&mut nodes, leader_id, &config);
        let leader_lease_deadline_ms = initial_leader_lease_deadline_ms(&config);
        Ok(Self {
            inner: Arc::new(RwLock::new(RaftClusterInner {
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
            })),
        })
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
        let mut last_response = CommandResponse::Empty;
        for chunk in chunks {
            last_response = self.propose_one(chunk)?;
        }
        Ok(last_response)
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
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            node.commit_index = entry.index;
            if !node.replica_role.can_serve_data() {
                continue;
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
        refresh_all_pipeline_states(&mut inner.nodes, leader_id, &config);
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
        let replication_deadline = Instant::now() + Duration::from_secs(5);
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
                Err(_) => failed_targets.push(target_id),
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

    pub fn elect_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.elect_leader(node_id)?;
        inner.persist_configured_wal()
    }

    pub fn begin_leader_transfer(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.ensure_live_leader()?;
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
        let logical_time_ms = inner.logical_time_ms;
        let candidate = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        candidate.pipeline_state.transfer_leader_requests = candidate
            .pipeline_state
            .transfer_leader_requests
            .saturating_add(1);
        if !candidate.alive {
            candidate.pipeline_state.transfer_leader_rejected = candidate
                .pipeline_state
                .transfer_leader_rejected
                .saturating_add(1);
            inner.persist_configured_wal()?;
            return Err(RaftError::NodeNotFound(node_id));
        }
        if candidate.commit_index < leader_commit_index {
            let replica_commit_index = candidate.commit_index;
            candidate.pipeline_state.transfer_leader_rejected = candidate
                .pipeline_state
                .transfer_leader_rejected
                .saturating_add(1);
            inner.persist_configured_wal()?;
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index,
                leader_commit_index,
            });
        }
        candidate.pipeline_state.transfer_leader_target = true;
        candidate.pipeline_state.transfer_leader_started_ms = Some(logical_time_ms);
        candidate.pipeline_state.transfer_leader_elapsed_ms = 0;
        candidate.pipeline_state.transfer_leader_accepted = candidate
            .pipeline_state
            .transfer_leader_accepted
            .saturating_add(1);
        inner.persist_configured_wal()
    }

    pub fn transfer_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.ensure_live_leader()?;
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
        let logical_time_ms = inner.logical_time_ms;
        let candidate = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        candidate.pipeline_state.transfer_leader_requests = candidate
            .pipeline_state
            .transfer_leader_requests
            .saturating_add(1);
        if !candidate.alive {
            candidate.pipeline_state.transfer_leader_rejected = candidate
                .pipeline_state
                .transfer_leader_rejected
                .saturating_add(1);
            inner.persist_configured_wal()?;
            return Err(RaftError::NodeNotFound(node_id));
        }
        if candidate.commit_index < leader_commit_index {
            let replica_commit_index = candidate.commit_index;
            candidate.pipeline_state.transfer_leader_rejected = candidate
                .pipeline_state
                .transfer_leader_rejected
                .saturating_add(1);
            inner.persist_configured_wal()?;
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index,
                leader_commit_index,
            });
        }
        candidate.pipeline_state.transfer_leader_target = true;
        candidate.pipeline_state.transfer_leader_started_ms = Some(logical_time_ms);
        candidate.pipeline_state.transfer_leader_elapsed_ms = 0;
        candidate.pipeline_state.transfer_leader_accepted = candidate
            .pipeline_state
            .transfer_leader_accepted
            .saturating_add(1);
        inner.elect_leader(node_id)?;
        if let Some(target) = inner.nodes.get_mut(&node_id) {
            target.pipeline_state.transfer_leader_completed = target
                .pipeline_state
                .transfer_leader_completed
                .saturating_add(1);
            target.pipeline_state.transfer_leader_target = false;
            target.pipeline_state.transfer_leader_started_ms = None;
            target.pipeline_state.transfer_leader_elapsed_ms = 0;
        }
        inner.persist_configured_wal()
    }

    pub fn promote_if_leader_down(&self) -> Result<RaftNodeId, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner
            .nodes
            .get(&inner.leader_id)
            .map(|node| node.alive)
            .unwrap_or(false)
        {
            return Ok(inner.leader_id);
        }
        inner.promote_best_live_follower()?;
        inner.persist_configured_wal()?;
        Ok(inner.leader_id)
    }

    pub fn failover_primary(&self) -> Result<RaftFailoverReport, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let old_leader_id = inner.leader_id;
        if inner
            .nodes
            .get(&old_leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Ok(inner.failover_report(old_leader_id));
        }
        inner.promote_best_live_follower()?;
        inner.persist_configured_wal()?;
        Ok(inner.failover_report(old_leader_id))
    }

    pub fn tick_election(&self) -> Result<RaftTickOutcome, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner
            .nodes
            .get(&inner.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            inner.election_elapsed_tick = 0;
            inner.renew_leader_lease();
            return Ok(RaftTickOutcome::LeaderAlive {
                leader_id: inner.leader_id,
            });
        }

        inner.election_elapsed_tick += 1;
        let timeout_tick = u64::from(inner.config.election_cycle_tick);
        if inner.election_elapsed_tick < timeout_tick {
            return Ok(RaftTickOutcome::ElectionPending {
                elapsed_tick: inner.election_elapsed_tick,
                timeout_tick,
            });
        }

        let candidate_id = inner.best_live_candidate()?;
        if inner.config.enable_pre_vote {
            inner.read_safety_state.pre_vote_requests =
                inner.read_safety_state.pre_vote_requests.saturating_add(1);
            if !inner.pre_vote_would_win(candidate_id)? {
                inner.read_safety_state.pre_vote_rejected =
                    inner.read_safety_state.pre_vote_rejected.saturating_add(1);
                if let Some(candidate) = inner.nodes.get_mut(&candidate_id) {
                    candidate.pipeline_state.pre_vote_rejections = candidate
                        .pipeline_state
                        .pre_vote_rejections
                        .saturating_add(1);
                }
                inner.election_elapsed_tick = 0;
                inner.persist_configured_wal()?;
                return Ok(RaftTickOutcome::PreVoteRejected { candidate_id });
            }
            inner.read_safety_state.pre_vote_accepted =
                inner.read_safety_state.pre_vote_accepted.saturating_add(1);
        }
        inner.elect_leader(candidate_id)?;
        inner.election_elapsed_tick = 0;
        inner.persist_configured_wal()?;
        let term = inner
            .nodes
            .get(&candidate_id)
            .map(|node| node.current_term)
            .unwrap_or_default();
        Ok(RaftTickOutcome::LeaderElected {
            leader_id: candidate_id,
            term,
        })
    }

    pub fn add_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        self.add_node_with_role(node_id, RaftReplicaRole::Voter)
    }

    pub fn add_node_with_role(
        &self,
        node_id: RaftNodeId,
        replica_role: RaftReplicaRole,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeAlreadyExists(node_id));
        }
        inner.ensure_live_leader()?;
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let mut node = new_node(node_id, RaftRole::Follower, inner.shard_id);
        node.replica_role = replica_role;
        node.current_term = leader.current_term;
        install_leader_snapshot_tail(
            &mut node,
            leader.installed_snapshot.clone(),
            leader.log.clone(),
            leader.commit_index,
        );
        inner.nodes.insert(node_id, node);
        match replica_role {
            RaftReplicaRole::Learner => {
                inner.membership_evidence.learner_add_count = inner
                    .membership_evidence
                    .learner_add_count
                    .saturating_add(1);
            }
            RaftReplicaRole::Witness => {
                inner.membership_evidence.witness_add_count = inner
                    .membership_evidence
                    .witness_add_count
                    .saturating_add(1);
            }
            RaftReplicaRole::Voter => {}
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn add_learner_with_auto_promote(
        &self,
        node_id: RaftNodeId,
        auto_promote: bool,
    ) -> Result<(), RaftError> {
        self.add_node_with_role(node_id, RaftReplicaRole::Learner)?;
        if auto_promote {
            self.catch_up(node_id)?;
            self.promote_learner_to_voter(node_id)?;
        }
        Ok(())
    }

    pub fn add_node_safely(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.add_node(node_id)?;
        self.catch_up_live_followers()?;
        Ok(self.scale_change_report())
    }

    pub fn promote_learner_to_voter(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        match node.replica_role {
            RaftReplicaRole::Learner => {
                node.pipeline_state.auto_promoted_from_learner = true;
                node.replica_role = RaftReplicaRole::Voter;
                inner.membership_evidence.learner_promote_count = inner
                    .membership_evidence
                    .learner_promote_count
                    .saturating_add(1);
                inner.membership_evidence.auto_promote_count = inner
                    .membership_evidence
                    .auto_promote_count
                    .saturating_add(1);
                inner.persist_configured_wal()
            }
            RaftReplicaRole::Voter => Ok(()),
            RaftReplicaRole::Witness => Err(RaftError::InvalidConfig(
                "witness replicas cannot be promoted to voter through learner promotion"
                    .to_string(),
            )),
        }
    }

    pub fn remove_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.voting_node_ids().len() == 1
            && inner
                .nodes
                .get(&node_id)
                .map(|node| node.replica_role.participates_in_quorum())
                .unwrap_or(false)
        {
            return Err(RaftError::CannotRemoveLastNode);
        }
        inner
            .nodes
            .remove(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        inner.membership_evidence.voter_remove_count = inner
            .membership_evidence
            .voter_remove_count
            .saturating_add(1);
        if inner.leader_id == node_id {
            inner.promote_best_live_follower()?;
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn remove_node_safely(
        &self,
        node_id: RaftNodeId,
    ) -> Result<RaftScaleChangeReport, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.remove_node_safely(node_id)?;
        inner.persist_configured_wal()?;
        Ok(inner.scale_change_report())
    }

    pub fn plan_membership_change(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangePlan, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        inner.plan_membership_change(new_voters)
    }

    pub fn apply_membership_change_safely(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        let plan = self.plan_membership_change(new_voters)?;
        let joint_membership = self.begin_joint_consensus(plan.new_voters.clone())?;
        let caught_up_voters = match self.catch_up_live_followers() {
            Ok(caught_up_voters) => caught_up_voters,
            Err(err) => {
                let _ = self.abort_joint_consensus();
                return Err(err);
            }
        };
        let committed_membership = match self.commit_joint_consensus() {
            Ok(membership) => membership,
            Err(err) => {
                let _ = self.abort_joint_consensus();
                return Err(err);
            }
        };
        let status = self.status();
        Ok(RaftMembershipChangeReport {
            plan,
            joint_membership,
            committed_membership,
            caught_up_voters,
            leader_id: status.leader_id,
            commit_index: status.commit_index,
        })
    }

    pub fn begin_joint_consensus(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<JointConsensusMembership, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.joint_membership.is_some() {
            return Err(RaftError::JointConsensusInProgress);
        }
        inner.ensure_live_leader()?;
        let mut old_voters = inner.voting_node_ids();
        old_voters.sort_unstable();
        let mut new_voters = new_voters.into_iter().collect::<Vec<_>>();
        new_voters.sort_unstable();
        new_voters.dedup();
        if new_voters.is_empty() {
            return Err(RaftError::CannotRemoveLastNode);
        }
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_term = leader.current_term;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let shard_id = inner.shard_id;
        for node_id in &new_voters {
            if !inner.nodes.contains_key(node_id) {
                let mut node = new_node(*node_id, RaftRole::Follower, shard_id);
                node.replica_role = RaftReplicaRole::Voter;
                node.current_term = leader_term;
                node.log = leader_log.clone();
                node.commit_index = leader_commit_index;
                apply_committed(&mut node);
                inner.nodes.insert(*node_id, node);
            }
        }
        let membership = JointConsensusMembership {
            old_voters,
            new_voters,
        };
        inner.joint_membership = Some(membership.clone());
        inner
            .membership_evidence
            .pending_joint_consensus_persist_count = inner
            .membership_evidence
            .pending_joint_consensus_persist_count
            .saturating_add(1);
        inner.persist_configured_wal()?;
        Ok(membership)
    }

    pub fn commit_joint_consensus(&self) -> Result<RaftMembership, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let membership = inner
            .joint_membership
            .take()
            .ok_or(RaftError::NoJointConsensus)?;
        if let Some((live, required)) = joint_majority_failure(&inner.nodes, &membership) {
            inner.joint_membership = Some(membership);
            return Err(RaftError::NoMajority { live, required });
        }
        let new_voters = membership
            .new_voters
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        inner
            .nodes
            .retain(|node_id, _| new_voters.contains(node_id));
        if !inner.nodes.contains_key(&inner.leader_id) {
            inner.promote_best_live_follower()?;
        }
        let raft_membership = RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.voting_node_ids(),
            leader_id: inner.leader_id,
        };
        inner.persist_configured_wal()?;
        Ok(raft_membership)
    }

    pub fn abort_joint_consensus(&self) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let membership = inner
            .joint_membership
            .take()
            .ok_or(RaftError::NoJointConsensus)?;
        let old_voters = membership
            .old_voters
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        inner
            .nodes
            .retain(|node_id, _| old_voters.contains(node_id));
        if !inner.nodes.contains_key(&inner.leader_id) {
            inner.promote_best_live_follower()?;
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn joint_membership(&self) -> Option<JointConsensusMembership> {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .joint_membership
            .clone()
    }

    pub fn set_alive(&self, node_id: RaftNodeId, alive: bool) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let logical_time_ms = inner.logical_time_ms;
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.alive = alive;
        if alive {
            node.pipeline_state.offline_since_ms = None;
            node.pipeline_state.offline_elapsed_ms = 0;
            node.pipeline_state.offline_timeout_reached = false;
            node.pipeline_state.inflight_entries = 0;
            node.pipeline_state.inflight_bytes = 0;
            node.pipeline_state.append_queue_depth = 0;
            node.pipeline_state.apply_inflight_tasks = 0;
            node.pipeline_state.apply_queue_depth = 0;
        } else if node.pipeline_state.offline_since_ms.is_none() {
            node.pipeline_state.offline_since_ms = Some(logical_time_ms);
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn catch_up(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader_id = inner.leader_id;
        let leader = inner
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.log = leader_log;
        node.commit_index = leader_commit_index;
        if node.replica_role.can_serve_data() {
            apply_committed(node);
        }
        inner.membership_evidence.learner_catchup_count = inner
            .membership_evidence
            .learner_catchup_count
            .saturating_add(1);
        let config = inner.config.clone();
        refresh_all_pipeline_states(&mut inner.nodes, leader_id, &config);
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn catch_up_live_followers(&self) -> Result<Vec<RaftNodeId>, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let caught_up = inner.catch_up_live_followers()?;
        inner.persist_configured_wal()?;
        Ok(caught_up)
    }

    pub fn catch_up_live_followers_bounded(
        &self,
        max_entries_per_follower: u64,
    ) -> Result<RaftCatchUpReport, RaftError> {
        if max_entries_per_follower == 0 {
            return Err(RaftError::InvalidConfig(
                "max_entries_per_follower must be greater than zero".to_string(),
            ));
        }
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let replayed_log_entries =
            inner.catch_up_live_followers_bounded(max_entries_per_follower)?;
        inner.persist_configured_wal()?;
        let status = inner.status();
        let health = replication_health_from_status(status.clone(), 0);
        Ok(RaftCatchUpReport {
            leader_id: status.leader_id,
            leader_commit_index: status.commit_index,
            max_entries_per_follower,
            replayed_log_entries,
            caught_up_voters: health.caught_up_voters,
            lagging_voters: health.lagging_voters,
        })
    }

    pub fn wait_for_applied_index(
        &self,
        node_id: RaftNodeId,
        index: u64,
        timeout_ms: u64,
    ) -> Result<(), RaftError> {
        let deadline = InstantCompat::now();
        loop {
            let applied_index = {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                inner
                    .nodes
                    .get(&node_id)
                    .ok_or(RaftError::NodeNotFound(node_id))?
                    .applied_index
            };
            if applied_index >= index {
                return Ok(());
            }
            if deadline.elapsed() >= Duration::from_millis(timeout_ms) {
                return Err(RaftError::AppliedIndexTimeout {
                    node_id,
                    applied_index,
                    target_index: index,
                    timeout_ms,
                });
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn hard_state(&self, node_id: RaftNodeId) -> Result<RaftHardState, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        Ok(RaftHardState {
            current_term: node.current_term,
            voted_for: node.voted_for,
            commit_index: node.commit_index,
        })
    }

    pub fn membership(&self) -> RaftMembership {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.voting_node_ids(),
            leader_id: inner.leader_id,
        }
    }

    pub fn wal_records(&self) -> Vec<(RaftNodeId, RaftWalRecord)> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let membership = RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.voting_node_ids(),
            leader_id: inner.leader_id,
        };
        inner
            .nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    RaftWalRecord {
                        hard_state: RaftHardState {
                            current_term: node.current_term,
                            voted_for: node.voted_for,
                            commit_index: node.commit_index,
                        },
                        membership: membership.clone(),
                        replica_role: node.replica_role,
                        joint_membership: inner.joint_membership.clone(),
                        latest_external_snapshot_ref: inner.latest_external_snapshot_ref.clone(),
                        installed_snapshot: node.installed_snapshot.clone(),
                        apply_snapshot_fence: raft_apply_snapshot_fence(node),
                        storage_apply_fence: raft_storage_apply_fence(inner.shard_id, node),
                        pipeline_state: node.pipeline_state.clone(),
                        read_safety_state: inner.read_safety_state.clone(),
                        membership_evidence: inner.membership_evidence.clone(),
                        entries: node.log.clone(),
                    },
                )
            })
            .collect()
    }

    pub fn persist_wal(&self, root: impl AsRef<Path>) -> io::Result<()> {
        let wal = LocalRaftWal::new(root.as_ref().to_path_buf());
        let records = self.wal_records();
        for (node_id, record) in records {
            wal.persist_node(record.membership.shard_id, node_id, &record)?;
        }
        Ok(())
    }

    pub fn build_append_entries_request(
        &self,
        target_id: RaftNodeId,
    ) -> Result<AppendEntriesRequest, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let entry_limit = inner.config.max_inflights_replicate.max(1);
        let byte_limit = inner.config.max_memory_replicate_log_bytes.max(1);
        let mut current_inflight_entries = 0;
        let mut current_inflight_bytes = 0;
        let leader_commit_for_drain = inner
            .nodes
            .get(&inner.leader_id)
            .map(|leader| leader.commit_index)
            .unwrap_or_default();
        if let Some(target) = inner.nodes.get_mut(&target_id) {
            if target.commit_index >= leader_commit_for_drain
                && (target.pipeline_state.inflight_entries > 0
                    || target.pipeline_state.inflight_bytes > 0)
            {
                target.pipeline_state.inflight_entries = 0;
                target.pipeline_state.inflight_bytes = 0;
                target.pipeline_state.append_queue_depth = 0;
            }
            target.pipeline_state.append_requests =
                target.pipeline_state.append_requests.saturating_add(1);
            current_inflight_entries = target.pipeline_state.inflight_entries;
            current_inflight_bytes = target.pipeline_state.inflight_bytes;
            if target.pipeline_state.inflight_entries >= entry_limit
                || target.pipeline_state.inflight_bytes >= byte_limit
            {
                target.pipeline_state.append_rejected =
                    target.pipeline_state.append_rejected.saturating_add(1);
                target.pipeline_state.memory_backpressure_rejections = target
                    .pipeline_state
                    .memory_backpressure_rejections
                    .saturating_add(u64::from(
                        target.pipeline_state.inflight_bytes >= byte_limit,
                    ));
                let inflight_entries = target.pipeline_state.inflight_entries;
                let inflight_bytes = target.pipeline_state.inflight_bytes;
                inner.persist_configured_wal()?;
                return Err(RaftError::AppendBackpressure {
                    node_id: target_id,
                    inflight_entries,
                    inflight_bytes,
                    entry_limit,
                    byte_limit,
                });
            }
        }
        let available_entries = entry_limit.saturating_sub(current_inflight_entries);
        let available_bytes = byte_limit.saturating_sub(current_inflight_bytes);
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_id = inner.leader_id;
        let leader_term = leader.current_term;
        let shard_id = inner.shard_id;
        let leader_commit = leader.commit_index;
        let target = inner
            .nodes
            .get(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        let enable_reorder_queue = inner.config.enable_reorder_queue;
        let target_last_index = node_last_log_or_snapshot_index(target);
        let prev_log_index = target_last_index.min(node_last_log_or_snapshot_index(leader));
        let prev_log_term =
            node_term_at_log_or_snapshot_index(leader, prev_log_index).unwrap_or_default();
        let mut entries = Vec::new();
        let mut inflight_bytes = 0u64;
        for entry in leader
            .log
            .iter()
            .filter(|entry| entry.index > prev_log_index)
        {
            if entries.len() as u64 >= available_entries {
                break;
            }
            let entry_bytes = command_size_bytes(&entry.command);
            if !entries.is_empty() && inflight_bytes.saturating_add(entry_bytes) > available_bytes {
                break;
            }
            if entries.is_empty() && entry_bytes > available_bytes {
                if let Some(target) = inner.nodes.get_mut(&target_id) {
                    target.pipeline_state.append_rejected =
                        target.pipeline_state.append_rejected.saturating_add(1);
                    target.pipeline_state.memory_backpressure_rejections = target
                        .pipeline_state
                        .memory_backpressure_rejections
                        .saturating_add(1);
                }
                inner.persist_configured_wal()?;
                return Err(RaftError::AppendBackpressure {
                    node_id: target_id,
                    inflight_entries: 0,
                    inflight_bytes: entry_bytes,
                    entry_limit,
                    byte_limit,
                });
            }
            inflight_bytes = inflight_bytes.saturating_add(entry_bytes);
            entries.push(entry.clone());
        }
        let inflight_bytes = entries
            .iter()
            .map(|entry| command_size_bytes(&entry.command))
            .sum();
        if let Some(target) = inner.nodes.get_mut(&target_id) {
            target.pipeline_state.append_accepted =
                target.pipeline_state.append_accepted.saturating_add(1);
            target.pipeline_state.match_index = target.commit_index;
            target.pipeline_state.next_index = prev_log_index.saturating_add(1);
            target.pipeline_state.inflight_entries =
                current_inflight_entries.saturating_add(entries.len() as u64);
            target.pipeline_state.inflight_bytes =
                current_inflight_bytes.saturating_add(inflight_bytes);
            target.pipeline_state.append_queue_depth = target.pipeline_state.inflight_entries;
            target.pipeline_state.append_queue_max_depth = target
                .pipeline_state
                .append_queue_max_depth
                .max(target.pipeline_state.append_queue_depth);
            if enable_reorder_queue {
                target.pipeline_state.reorder_queue_depth =
                    target.commit_index.saturating_sub(target.applied_index);
            }
        }
        inner.persist_configured_wal()?;
        Ok(AppendEntriesRequest {
            rpc: None,
            shard_id,
            term: leader_term,
            leader_id,
            target_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        })
    }

    pub fn record_append_entries_response(
        &self,
        target_id: RaftNodeId,
        response: &AppendEntriesResponse,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let target = inner
            .nodes
            .get_mut(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        target.pipeline_state.inflight_entries = 0;
        target.pipeline_state.inflight_bytes = 0;
        target.pipeline_state.append_queue_depth = 0;
        if response.success {
            target.pipeline_state.match_index =
                target.pipeline_state.match_index.max(response.match_index);
            target.pipeline_state.next_index = response.match_index.saturating_add(1);
        } else {
            target.pipeline_state.append_rejected =
                target.pipeline_state.append_rejected.saturating_add(1);
            target.pipeline_state.next_index =
                target.pipeline_state.next_index.saturating_sub(1).max(1);
        }
        inner.persist_configured_wal()
    }

    pub fn receive_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if request.shard_id != inner.shard_id {
            return Ok(AppendEntriesResponse {
                term: 0,
                success: false,
                match_index: 0,
                reject_reason: Some("shard_mismatch".to_string()),
            });
        }
        let entries = request.entries;
        let target_id = request.target_id;
        let leader_id = request.leader_id;
        let term = request.term;
        let leader_commit = request.leader_commit;
        let received_entries = entries.len() as u64;
        let received_bytes = entries
            .iter()
            .map(|entry| command_size_bytes(&entry.command))
            .sum::<u64>();
        let enable_reorder_queue = inner.config.enable_reorder_queue;
        let reorder_window_size = inner.config.reorder_window_size;
        let max_apply_batch_bytes = inner.config.max_apply_batch_bytes;
        let max_inflights_apply_task = inner.config.max_inflights_apply_task.max(1);
        let (term, last_index) = {
            let node = inner
                .nodes
                .get_mut(&target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            if term < node.current_term {
                node.pipeline_state.append_rejected =
                    node.pipeline_state.append_rejected.saturating_add(1);
                node.pipeline_state.stale_term_rejections =
                    node.pipeline_state.stale_term_rejections.saturating_add(1);
                node.pipeline_state.reorder_entries_rejected = node
                    .pipeline_state
                    .reorder_entries_rejected
                    .saturating_add(received_entries.max(1));
                return Ok(AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node_last_log_or_snapshot_index(node),
                    reject_reason: Some("stale_term".to_string()),
                });
            }
            if request.prev_log_index > 0 {
                let prev_term = node_term_at_log_or_snapshot_index(node, request.prev_log_index);
                if prev_term != Some(request.prev_log_term) {
                    let last_index = node_last_log_or_snapshot_index(node);
                    let missing_gap = request.prev_log_index.saturating_sub(last_index);
                    node.pipeline_state.out_of_order_append_rejections = node
                        .pipeline_state
                        .out_of_order_append_rejections
                        .saturating_add(1);
                    node.pipeline_state.reorder_entries_rejected = node
                        .pipeline_state
                        .reorder_entries_rejected
                        .saturating_add(received_entries.max(1));
                    if enable_reorder_queue && missing_gap > 0 {
                        let reject_reason = if missing_gap > reorder_window_size {
                            node.pipeline_state.reorder_entry_timeouts =
                                node.pipeline_state.reorder_entry_timeouts.saturating_add(1);
                            node.pipeline_state.reorder_dropped_packages = node
                                .pipeline_state
                                .reorder_dropped_packages
                                .saturating_add(1);
                            "reorder_window_timeout"
                        } else {
                            node.pipeline_state.reorder_queue_depth = node
                                .pipeline_state
                                .reorder_queue_depth
                                .saturating_add(received_entries.max(1));
                            "out_of_order_append_queued"
                        };
                        let response = AppendEntriesResponse {
                            term: node.current_term,
                            success: false,
                            match_index: last_index,
                            reject_reason: Some(reject_reason.to_string()),
                        };
                        inner.persist_configured_wal()?;
                        return Ok(response);
                    }
                    return Ok(AppendEntriesResponse {
                        term: node.current_term,
                        success: false,
                        match_index: node_last_log_or_snapshot_index(node),
                        reject_reason: Some("log_mismatch".to_string()),
                    });
                }
            }
            if received_bytes > max_apply_batch_bytes {
                node.pipeline_state.apply_queue_depth = received_entries.max(1);
                node.pipeline_state.apply_queue_max_depth = node
                    .pipeline_state
                    .apply_queue_max_depth
                    .max(node.pipeline_state.apply_queue_depth);
                node.pipeline_state.apply_backpressure_rejections = node
                    .pipeline_state
                    .apply_backpressure_rejections
                    .saturating_add(1);
                let response = AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node_last_log_or_snapshot_index(node),
                    reject_reason: Some("apply_batch_backpressure".to_string()),
                };
                inner.persist_configured_wal()?;
                return Ok(response);
            }
            if received_entries > max_inflights_apply_task {
                node.pipeline_state.apply_queue_depth = received_entries;
                node.pipeline_state.apply_queue_max_depth = node
                    .pipeline_state
                    .apply_queue_max_depth
                    .max(node.pipeline_state.apply_queue_depth);
                node.pipeline_state.apply_backpressure_rejections = node
                    .pipeline_state
                    .apply_backpressure_rejections
                    .saturating_add(1);
                let response = AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node_last_log_or_snapshot_index(node),
                    reject_reason: Some("apply_inflight_backpressure".to_string()),
                };
                inner.persist_configured_wal()?;
                return Ok(response);
            }
            if enable_reorder_queue
                && received_entries > 0
                && node
                    .pipeline_state
                    .reorder_queue_depth
                    .saturating_add(received_entries)
                    > reorder_window_size
            {
                node.pipeline_state.reorder_entries_rejected = node
                    .pipeline_state
                    .reorder_entries_rejected
                    .saturating_add(received_entries);
                node.pipeline_state.reorder_entry_timeouts =
                    node.pipeline_state.reorder_entry_timeouts.saturating_add(1);
                node.pipeline_state.reorder_dropped_packages = node
                    .pipeline_state
                    .reorder_dropped_packages
                    .saturating_add(1);
                let response = AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node_last_log_or_snapshot_index(node),
                    reject_reason: Some("reorder_window_exceeded".to_string()),
                };
                inner.persist_configured_wal()?;
                return Ok(response);
            }
            node.current_term = term;
            node.role = RaftRole::Follower;
            let before_reorder_depth = node.pipeline_state.reorder_queue_depth;
            for entry in entries.iter().cloned() {
                append_entry(node, entry);
            }
            let last_index = node_last_log_or_snapshot_index(node);
            node.commit_index = leader_commit.min(last_index);
            if node.replica_role.can_serve_data() {
                node.pipeline_state.apply_inflight_tasks =
                    node.pipeline_state.apply_inflight_tasks.saturating_add(1);
                node.pipeline_state.apply_queue_depth = received_entries;
                node.pipeline_state.apply_queue_max_depth = node
                    .pipeline_state
                    .apply_queue_max_depth
                    .max(node.pipeline_state.apply_queue_depth);
                apply_committed(node);
                node.pipeline_state.apply_inflight_tasks = 0;
                node.pipeline_state.apply_queue_depth = 0;
            }
            node.pipeline_state.match_index = node.commit_index;
            node.pipeline_state.next_index = node_next_log_index(node);
            node.pipeline_state.inflight_entries = 0;
            node.pipeline_state.inflight_bytes = 0;
            node.pipeline_state.append_queue_depth = 0;
            node.pipeline_state.reorder_queue_depth = if enable_reorder_queue {
                node.commit_index.saturating_sub(node.applied_index)
            } else {
                0
            };
            if enable_reorder_queue {
                node.pipeline_state.reorder_entries_accepted = node
                    .pipeline_state
                    .reorder_entries_accepted
                    .saturating_add(received_entries);
                let released = before_reorder_depth
                    .saturating_add(received_entries)
                    .saturating_sub(node.pipeline_state.reorder_queue_depth);
                node.pipeline_state.reorder_entries_released = node
                    .pipeline_state
                    .reorder_entries_released
                    .saturating_add(released);
            }
            node.pipeline_state.snapshot_installing = false;
            node.pipeline_state.pre_vote_rejections = node
                .pipeline_state
                .pre_vote_rejections
                .saturating_add(u64::from(received_entries == 0 && received_bytes == 0));
            (node.current_term, last_index)
        };
        inner.leader_id = leader_id;
        for (node_id, peer) in inner.nodes.iter_mut() {
            if *node_id != leader_id && peer.role == RaftRole::Leader {
                peer.role = RaftRole::Follower;
            }
        }
        if leader_id != target_id {
            if let Some(leader) = inner.nodes.get_mut(&leader_id) {
                leader.alive = true;
                leader.role = RaftRole::Leader;
                leader.current_term = leader.current_term.max(term);
                for entry in entries {
                    append_entry(leader, entry);
                }
                let leader_last_index = node_last_log_or_snapshot_index(leader);
                leader.commit_index = leader
                    .commit_index
                    .max(leader_commit.min(leader_last_index));
            }
        }
        let config = inner.config.clone();
        refresh_all_pipeline_states(&mut inner.nodes, leader_id, &config);
        inner.renew_leader_lease();
        inner.persist_configured_wal()?;
        Ok(AppendEntriesResponse {
            term,
            success: true,
            match_index: last_index,
            reject_reason: None,
        })
    }

    pub fn build_vote_request(
        &self,
        candidate_id: RaftNodeId,
        target_id: RaftNodeId,
    ) -> Result<VoteRequest, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let candidate = inner
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        let last_log_index = candidate
            .log
            .last()
            .map(|entry| entry.index)
            .unwrap_or_default();
        let last_log_term = candidate
            .log
            .last()
            .map(|entry| entry.term)
            .unwrap_or_default();
        Ok(VoteRequest {
            rpc: None,
            shard_id: inner.shard_id,
            term: candidate.current_term + 1,
            candidate_id,
            target_id,
            last_log_index,
            last_log_term,
        })
    }

    pub fn receive_vote_request(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if request.shard_id != inner.shard_id {
            return Ok(VoteResponse {
                term: 0,
                vote_granted: false,
                reject_reason: Some("shard_mismatch".to_string()),
            });
        }
        if !inner
            .nodes
            .get(&request.candidate_id)
            .map(|candidate| candidate.replica_role.can_be_leader())
            .unwrap_or(false)
        {
            return Ok(VoteResponse {
                term: 0,
                vote_granted: false,
                reject_reason: Some("candidate_not_voter".to_string()),
            });
        }
        let node = inner
            .nodes
            .get_mut(&request.target_id)
            .ok_or(RaftError::NodeNotFound(request.target_id))?;
        if !node.replica_role.participates_in_quorum() {
            node.pipeline_state.election_rejections =
                node.pipeline_state.election_rejections.saturating_add(1);
            return Ok(VoteResponse {
                term: node.current_term,
                vote_granted: false,
                reject_reason: Some("target_not_voter".to_string()),
            });
        }
        if request.term < node.current_term {
            node.pipeline_state.pre_vote_rejections =
                node.pipeline_state.pre_vote_rejections.saturating_add(1);
            return Ok(VoteResponse {
                term: node.current_term,
                vote_granted: false,
                reject_reason: Some("stale_term".to_string()),
            });
        }
        if request.term > node.current_term {
            node.current_term = request.term;
            node.voted_for = None;
            node.role = RaftRole::Follower;
        }
        let local_last_index = node.log.last().map(|entry| entry.index).unwrap_or_default();
        let local_last_term = node.log.last().map(|entry| entry.term).unwrap_or_default();
        let log_up_to_date =
            (request.last_log_term, request.last_log_index) >= (local_last_term, local_last_index);
        if !log_up_to_date {
            let term = node.current_term;
            node.pipeline_state.pre_vote_rejections =
                node.pipeline_state.pre_vote_rejections.saturating_add(1);
            inner.persist_configured_wal()?;
            return Ok(VoteResponse {
                term,
                vote_granted: false,
                reject_reason: Some("candidate_log_behind".to_string()),
            });
        }
        if node.voted_for.is_some() && node.voted_for != Some(request.candidate_id) {
            let term = node.current_term;
            node.pipeline_state.election_rejections =
                node.pipeline_state.election_rejections.saturating_add(1);
            inner.persist_configured_wal()?;
            return Ok(VoteResponse {
                term,
                vote_granted: false,
                reject_reason: Some("already_voted".to_string()),
            });
        }
        node.current_term = request.term;
        node.voted_for = Some(request.candidate_id);
        node.role = RaftRole::Follower;
        let term = node.current_term;
        inner.persist_configured_wal()?;
        Ok(VoteResponse {
            term,
            vote_granted: true,
            reject_reason: None,
        })
    }

    pub fn build_install_snapshot_request(
        &self,
        target_id: RaftNodeId,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        self.build_install_snapshot_request_with_external_ref(target_id, None)
    }

    pub fn build_install_snapshot_request_with_external_ref(
        &self,
        target_id: RaftNodeId,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        let mut snapshot = self.create_snapshot()?;
        snapshot.external_snapshot_ref = external_snapshot_ref.clone();
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let shard_id = inner.shard_id;
        let term = leader.current_term;
        let leader_id = inner.leader_id;
        let logical_time_ms = inner.logical_time_ms;
        {
            let target = inner
                .nodes
                .get_mut(&target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            if target.pipeline_state.snapshot_sending || target.pipeline_state.snapshot_installing {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_send_failed =
                    target.pipeline_state.snapshot_send_failed.saturating_add(1);
                inner.persist_configured_wal()?;
                return Err(RaftError::SnapshotBackpressure { node_id: target_id });
            }
            target.pipeline_state.snapshot_sending = true;
            target.pipeline_state.snapshot_installing = true;
            target.pipeline_state.snapshot_send_started_ms = Some(logical_time_ms);
            target.pipeline_state.snapshot_send_elapsed_ms = 0;
            target.pipeline_state.snapshot_installed_index = snapshot.last_included_index;
            target.pipeline_state.snapshot_send_attempts = target
                .pipeline_state
                .snapshot_send_attempts
                .saturating_add(1);
            target.pipeline_state.snapshot_install_received_chunks = 0;
            target.pipeline_state.snapshot_install_total_chunks = 1;
        }
        inner.persist_configured_wal()?;
        Ok(InstallSnapshotRequest {
            rpc: None,
            shard_id,
            term,
            leader_id,
            target_id,
            external_snapshot_ref,
            snapshot,
        })
    }

    pub fn plan_snapshot_bootstrap(
        &self,
        target_id: RaftNodeId,
        policy: RaftSnapshotTransferPolicy,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<RaftReplicaBootstrapPlan, RaftError> {
        let snapshot = self.create_snapshot()?;
        let transfer = decide_snapshot_transfer(&snapshot, policy, external_snapshot_ref.clone())?;
        Ok(RaftReplicaBootstrapPlan {
            shard_id: snapshot.shard_id,
            target_id,
            catch_up_from_index: snapshot.last_included_index.saturating_add(1),
            last_included_index: snapshot.last_included_index,
            transfer,
        })
    }

    pub fn build_install_snapshot_request_with_policy(
        &self,
        target_id: RaftNodeId,
        policy: RaftSnapshotTransferPolicy,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        let snapshot = self.create_snapshot()?;
        let transfer = decide_snapshot_transfer(&snapshot, policy, external_snapshot_ref.clone())?;
        match transfer.mode {
            RaftSnapshotTransferMode::PeerStreaming => {
                self.build_install_snapshot_request_with_external_ref(target_id, None)
            }
            RaftSnapshotTransferMode::ExternalStore => self
                .build_install_snapshot_request_with_external_ref(
                    target_id,
                    transfer.external_snapshot_ref,
                ),
        }
    }

    pub fn receive_install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if request.shard_id != inner.shard_id {
                return Ok(InstallSnapshotResponse {
                    term: 0,
                    success: false,
                    last_included_index: 0,
                    reject_reason: Some("shard_mismatch".to_string()),
                });
            }
            let node = inner
                .nodes
                .get_mut(&request.target_id)
                .ok_or(RaftError::NodeNotFound(request.target_id))?;
            if request.term < node.current_term {
                let current_term = node.current_term;
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_install_rejected = node
                    .pipeline_state
                    .snapshot_install_rejected
                    .saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
                let _ = node;
                inner.persist_configured_wal()?;
                return Ok(InstallSnapshotResponse {
                    term: current_term,
                    success: false,
                    last_included_index: 0,
                    reject_reason: Some("stale_term".to_string()),
                });
            }
            node.current_term = request.term;
            node.role = RaftRole::Follower;
            node.voted_for = None;
            node.pipeline_state.snapshot_install_started = node
                .pipeline_state
                .snapshot_install_started
                .saturating_add(1);
            node.pipeline_state.snapshot_installing = true;
            node.pipeline_state.snapshot_installed_index = request.snapshot.last_included_index;
            node.pipeline_state.snapshot_install_received_chunks = 0;
            node.pipeline_state.snapshot_install_total_chunks = 1;
        }
        let result = self.install_snapshot(request.target_id, request.snapshot.clone());
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_elapsed_ms = 0;
                node.pipeline_state.snapshot_installed_index = request.snapshot.last_included_index;
                node.pipeline_state.snapshot_install_received_chunks = 1;
                node.pipeline_state.snapshot_install_total_chunks = 1;
                if result.is_ok() {
                    node.pipeline_state.snapshot_install_completed = node
                        .pipeline_state
                        .snapshot_install_completed
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_completed = node
                        .pipeline_state
                        .snapshot_send_completed
                        .saturating_add(1);
                } else {
                    node.pipeline_state.snapshot_install_rolled_back = node
                        .pipeline_state
                        .snapshot_install_rolled_back
                        .saturating_add(1);
                    node.pipeline_state.snapshot_install_progress_per_mille = 0;
                    node.pipeline_state.snapshot_send_failed =
                        node.pipeline_state.snapshot_send_failed.saturating_add(1);
                }
            }
            inner.persist_configured_wal()?;
        }
        let term = self
            .hard_state(request.target_id)
            .map(|state| state.current_term)
            .unwrap_or(request.term);
        match result {
            Ok(()) => Ok(InstallSnapshotResponse {
                term,
                success: true,
                last_included_index: request.snapshot.last_included_index,
                reject_reason: None,
            }),
            Err(err) => Ok(InstallSnapshotResponse {
                term,
                success: false,
                last_included_index: 0,
                reject_reason: Some(err.to_string()),
            }),
        }
    }

    pub fn finish_snapshot_send(
        &self,
        target_id: RaftNodeId,
        success: bool,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get_mut(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        node.pipeline_state.snapshot_sending = false;
        node.pipeline_state.snapshot_installing = false;
        node.pipeline_state.snapshot_send_started_ms = None;
        node.pipeline_state.snapshot_send_elapsed_ms = 0;
        if success {
            node.pipeline_state.snapshot_send_completed = node
                .pipeline_state
                .snapshot_send_completed
                .saturating_add(1);
        } else {
            node.pipeline_state.snapshot_send_failed =
                node.pipeline_state.snapshot_send_failed.saturating_add(1);
        }
        let _ = node;
        inner.persist_configured_wal()
    }

    pub fn build_install_snapshot_chunks(
        &self,
        target_id: RaftNodeId,
        max_entries_per_chunk: usize,
    ) -> Result<Vec<InstallSnapshotChunkRequest>, RaftError> {
        let snapshot = self.create_snapshot()?;
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_term = leader.current_term;
        let leader_id = inner.leader_id;
        let max_inflights_replicate = inner.config.max_inflights_replicate;
        let logical_time_ms = inner.logical_time_ms;
        let snapshot_during_membership_change = inner.joint_membership.is_some();
        let chunk_size = max_entries_per_chunk.max(1);
        let chunk_count = snapshot.entries.len().max(1).div_ceil(chunk_size);
        let snapshot_id = format!(
            "{}-{}-{}",
            snapshot.shard_id, snapshot.last_included_term, snapshot.last_included_index
        );
        {
            let target = inner
                .nodes
                .get_mut(&target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            if target.pipeline_state.snapshot_sending || target.pipeline_state.snapshot_installing {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_send_failed =
                    target.pipeline_state.snapshot_send_failed.saturating_add(1);
                inner.persist_configured_wal()?;
                return Err(RaftError::SnapshotBackpressure { node_id: target_id });
            }
            target.pipeline_state.snapshot_sending = true;
            target.pipeline_state.snapshot_installing = true;
            target.pipeline_state.snapshot_send_started_ms = Some(logical_time_ms);
            target.pipeline_state.snapshot_send_elapsed_ms = 0;
            target.pipeline_state.snapshot_installed_index = snapshot.last_included_index;
            target.pipeline_state.snapshot_send_attempts = target
                .pipeline_state
                .snapshot_send_attempts
                .saturating_add(chunk_count as u64);
            target.pipeline_state.snapshot_install_received_chunks = 0;
            target.pipeline_state.snapshot_install_total_chunks = chunk_count as u64;
            target.pipeline_state.snapshot_install_progress_per_mille = 0;
            target.pipeline_state.snapshot_during_membership_change |=
                snapshot_during_membership_change;
            target.pipeline_state.snapshot_rejoin_after_compacted_log |=
                target.commit_index < snapshot.last_included_index;
            if chunk_count as u64 > max_inflights_replicate {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_rate_limit_rejections = target
                    .pipeline_state
                    .snapshot_rate_limit_rejections
                    .saturating_add(1);
            }
        }
        inner.persist_configured_wal()?;
        let mut chunks = Vec::new();
        if snapshot.entries.is_empty() {
            chunks.push(InstallSnapshotChunkRequest {
                rpc: None,
                shard_id: snapshot.shard_id,
                term: leader_term,
                leader_id,
                target_id,
                snapshot_id,
                last_included_term: snapshot.last_included_term,
                last_included_index: snapshot.last_included_index,
                chunk_index: 0,
                chunk_count: 1,
                entries: Vec::new(),
            });
            return Ok(chunks);
        }
        for (chunk_index, entries) in snapshot.entries.chunks(chunk_size).enumerate() {
            chunks.push(InstallSnapshotChunkRequest {
                rpc: None,
                shard_id: snapshot.shard_id,
                term: leader_term,
                leader_id,
                target_id,
                snapshot_id: snapshot_id.clone(),
                last_included_term: snapshot.last_included_term,
                last_included_index: snapshot.last_included_index,
                chunk_index: chunk_index as u64,
                chunk_count: chunk_count as u64,
                entries: entries.to_vec(),
            });
        }
        Ok(chunks)
    }

    pub fn receive_install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if request.shard_id != inner.shard_id {
            return Ok(InstallSnapshotChunkResponse {
                term: 0,
                success: false,
                snapshot_complete: false,
                received_chunks: 0,
                last_included_index: 0,
                reject_reason: Some("shard_mismatch".to_string()),
            });
        }
        if request.chunk_count == 0 || request.chunk_index >= request.chunk_count {
            return Err(RaftError::InvalidSnapshotChunk(
                "chunk index is outside chunk count".to_string(),
            ));
        }
        let node = inner
            .nodes
            .get_mut(&request.target_id)
            .ok_or(RaftError::NodeNotFound(request.target_id))?;
        if request.term < node.current_term {
            let current_term = node.current_term;
            node.pipeline_state.snapshot_retry_count =
                node.pipeline_state.snapshot_retry_count.saturating_add(1);
            node.pipeline_state.snapshot_install_rejected = node
                .pipeline_state
                .snapshot_install_rejected
                .saturating_add(1);
            node.pipeline_state.snapshot_send_failed =
                node.pipeline_state.snapshot_send_failed.saturating_add(1);
            let _ = node;
            inner.persist_configured_wal()?;
            return Ok(InstallSnapshotChunkResponse {
                term: current_term,
                success: false,
                snapshot_complete: false,
                received_chunks: 0,
                last_included_index: 0,
                reject_reason: Some("stale_term".to_string()),
            });
        }
        node.current_term = request.term;
        node.role = RaftRole::Follower;
        node.voted_for = None;
        if node.pipeline_state.snapshot_install_received_chunks == 0 {
            node.pipeline_state.snapshot_install_started = node
                .pipeline_state
                .snapshot_install_started
                .saturating_add(1);
        }
        node.pipeline_state.snapshot_installing = true;
        node.pipeline_state.snapshot_installed_index = request.last_included_index;
        node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
        let key = (request.target_id, request.snapshot_id.clone());
        let pending = inner
            .pending_snapshots
            .entry(key.clone())
            .or_insert_with(|| PendingSnapshotChunks {
                shard_id: request.shard_id,
                last_included_term: request.last_included_term,
                last_included_index: request.last_included_index,
                chunks: vec![None; request.chunk_count as usize],
            });
        if pending.shard_id != request.shard_id
            || pending.last_included_term != request.last_included_term
            || pending.last_included_index != request.last_included_index
            || pending.chunks.len() != request.chunk_count as usize
        {
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_install_rejected = node
                    .pipeline_state
                    .snapshot_install_rejected
                    .saturating_add(1);
                node.pipeline_state.snapshot_install_rolled_back = node
                    .pipeline_state
                    .snapshot_install_rolled_back
                    .saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
            }
            inner.pending_snapshots.remove(&key);
            inner.persist_configured_wal()?;
            return Err(RaftError::InvalidSnapshotChunk(
                "chunk metadata changed within snapshot".to_string(),
            ));
        }
        let duplicate_chunk = pending.chunks[request.chunk_index as usize].is_some();
        pending.chunks[request.chunk_index as usize] = Some(request.entries);
        let received_chunks = pending
            .chunks
            .iter()
            .filter(|chunk| chunk.is_some())
            .count() as u64;
        if let Some(node) = inner.nodes.get_mut(&request.target_id) {
            if duplicate_chunk {
                node.pipeline_state.snapshot_chunk_retry_count = node
                    .pipeline_state
                    .snapshot_chunk_retry_count
                    .saturating_add(1);
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
            }
            node.pipeline_state.snapshot_install_received_chunks = received_chunks;
            node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
            node.pipeline_state.snapshot_install_progress_per_mille =
                received_chunks.saturating_mul(1_000) / request.chunk_count.max(1);
        }
        if received_chunks < request.chunk_count {
            let term = inner
                .nodes
                .get(&request.target_id)
                .map(|node| node.current_term)
                .unwrap_or(request.term);
            inner.persist_configured_wal()?;
            return Ok(InstallSnapshotChunkResponse {
                term,
                success: true,
                snapshot_complete: false,
                received_chunks,
                last_included_index: 0,
                reject_reason: None,
            });
        }

        let pending = inner
            .pending_snapshots
            .remove(&key)
            .expect("complete pending snapshot must exist");
        let entries = pending
            .chunks
            .into_iter()
            .flat_map(|chunk| chunk.unwrap_or_default())
            .collect::<Vec<_>>();
        drop(inner);
        let install_result = self.install_snapshot(
            request.target_id,
            RaftSnapshot {
                shard_id: request.shard_id,
                last_included_term: request.last_included_term,
                last_included_index: request.last_included_index,
                external_snapshot_ref: None,
                entries,
            },
        );
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_installed_index = request.last_included_index;
                node.pipeline_state.snapshot_install_received_chunks = received_chunks;
                node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
                node.pipeline_state.snapshot_install_progress_per_mille =
                    received_chunks.saturating_mul(1_000) / request.chunk_count.max(1);
                if install_result.is_ok() {
                    node.pipeline_state.snapshot_install_completed = node
                        .pipeline_state
                        .snapshot_install_completed
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_completed = node
                        .pipeline_state
                        .snapshot_send_completed
                        .saturating_add(1);
                } else {
                    node.pipeline_state.snapshot_install_rolled_back = node
                        .pipeline_state
                        .snapshot_install_rolled_back
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_failed =
                        node.pipeline_state.snapshot_send_failed.saturating_add(1);
                }
            }
            inner.persist_configured_wal()?;
        }
        install_result?;
        let term = self
            .hard_state(request.target_id)
            .map(|state| state.current_term)
            .unwrap_or(request.term);
        Ok(InstallSnapshotChunkResponse {
            term,
            success: true,
            snapshot_complete: true,
            received_chunks,
            last_included_index: request.last_included_index,
            reject_reason: None,
        })
    }

    pub fn create_snapshot(&self) -> Result<RaftSnapshot, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        let mut entries_by_index = BTreeMap::new();
        if let Some(snapshot) = &leader.installed_snapshot {
            for entry in snapshot
                .entries
                .iter()
                .filter(|entry| entry.index <= leader.commit_index)
            {
                entries_by_index.insert(entry.index, entry.clone());
            }
        }
        for entry in leader
            .log
            .iter()
            .filter(|entry| entry.index <= leader.commit_index)
        {
            entries_by_index.insert(entry.index, entry.clone());
        }
        let entries = entries_by_index.into_values().collect::<Vec<_>>();
        let last_included_term = entries
            .last()
            .map(|entry| entry.term)
            .unwrap_or(leader.current_term);
        Ok(RaftSnapshot {
            shard_id: inner.shard_id,
            last_included_term,
            last_included_index: leader.commit_index,
            external_snapshot_ref: None,
            entries,
        })
    }

    pub fn maybe_trigger_snapshot(&self) -> Result<RaftSnapshotTriggerReport, RaftError> {
        let (should_trigger, report) = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            let last_snapshot_index = leader
                .installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            let applied_log_bytes = raft_log_bytes_after(&leader.log, last_snapshot_index);
            let mut report = RaftSnapshotTriggerReport {
                triggered: false,
                reason: "below_threshold".to_string(),
                leader_id: inner.leader_id,
                applied_index: leader.applied_index,
                last_snapshot_index,
                applied_log_bytes,
                max_applied_log_bytes: inner.config.max_applied_log_bytes,
            };
            if !inner.config.can_trigger_snapshot {
                report.reason = "disabled".to_string();
                return Ok(report);
            }
            if leader.applied_index <= last_snapshot_index {
                report.reason = "no_new_applied_logs".to_string();
                return Ok(report);
            }
            if applied_log_bytes < inner.config.max_applied_log_bytes {
                return Ok(report);
            }
            report.triggered = true;
            report.reason = "applied_log_bytes_threshold".to_string();
            (true, report)
        };

        if should_trigger {
            let snapshot = self.create_snapshot()?;
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            for node in inner.nodes.values_mut().filter(|node| node.alive) {
                if snapshot.last_included_index >= node.commit_index {
                    install_snapshot_state(node, snapshot.clone());
                }
            }
            inner.persist_configured_wal()?;
        }
        Ok(report)
    }

    pub async fn publish_leader_snapshot_to_store<O>(
        &self,
        snapshot_store: &S3SnapshotStore<O>,
    ) -> Result<RaftSnapshotPublishReport, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let snapshot = self.create_snapshot()?;
        let snapshot_bytes = serde_json::to_vec(&snapshot)
            .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
        let last_log_id = format!(
            "term:{}:index:{}",
            snapshot.last_included_term, snapshot.last_included_index
        );
        let local_snapshot = snapshot_store
            .create_local_snapshot_with_index_bytes(
                snapshot.shard_id,
                last_log_id,
                Bytes::from(snapshot_bytes),
            )
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let snapshot_ref = snapshot_store
            .upload_snapshot(local_snapshot)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let raft_ref = raft_external_ref_from_snapshot_ref(&snapshot_ref);
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.latest_external_snapshot_ref = Some(raft_ref.clone());
            inner.persist_configured_wal()?;
        }
        Ok(RaftSnapshotPublishReport {
            shard_id: snapshot_ref.shard_id,
            last_log_index: snapshot_ref.last_log_index,
            raft_ref,
            meta_ref: shard_snapshot_ref_from_snapshot_ref(&snapshot_ref),
        })
    }

    pub async fn publish_leader_snapshot_and_record_meta<O>(
        &self,
        snapshot_store: &S3SnapshotStore<O>,
        meta: &SingleNodeMeta,
    ) -> Result<RaftSnapshotPublishReport, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let report = self
            .publish_leader_snapshot_to_store(snapshot_store)
            .await?;
        let ack = meta.publish_shard_snapshot(PublishShardSnapshotRequest {
            shard_id: report.shard_id,
            snapshot: report.meta_ref.clone(),
        });
        if ack.status.ok {
            Ok(report)
        } else {
            Err(RaftError::SnapshotStore(format!(
                "metaserver rejected snapshot ref: {}",
                ack.status.code
            )))
        }
    }

    pub async fn bootstrap_replica_from_external_snapshot<O>(
        &self,
        target_id: RaftNodeId,
        snapshot_store: &S3SnapshotStore<O>,
        snapshot_ref: &ShardSnapshotRef,
        destination: PathBuf,
    ) -> Result<RaftReplicaBootstrapPlan, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let local_commit_index = self
            .hard_state(target_id)
            .map(|state| state.commit_index)
            .unwrap_or_default();
        if snapshot_ref.last_log_index < local_commit_index {
            return Err(RaftError::StaleSnapshot {
                snapshot_index: snapshot_ref.last_log_index,
                local_commit_index,
            });
        }
        let local_snapshot = snapshot_store
            .download_snapshot_by_uri(&snapshot_ref.uri, destination)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let snapshot_bytes = tokio::fs::read(&local_snapshot.index_path)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        validate_downloaded_snapshot_ref(
            self.shard_id(),
            snapshot_ref,
            &local_snapshot.manifest,
            &snapshot_bytes,
        )?;
        let mut snapshot = serde_json::from_slice::<RaftSnapshot>(&snapshot_bytes)
            .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
        snapshot.external_snapshot_ref = Some(RaftExternalSnapshotRef {
            uri: snapshot_ref.uri.clone(),
            checksum: snapshot_ref.checksum.clone(),
            byte_size: snapshot_ref.byte_size,
        });
        self.install_snapshot(target_id, snapshot.clone())?;
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.latest_external_snapshot_ref = Some(RaftExternalSnapshotRef {
                uri: snapshot_ref.uri.clone(),
                checksum: snapshot_ref.checksum.clone(),
                byte_size: snapshot_ref.byte_size,
            });
            inner.persist_configured_wal()?;
        }
        self.catch_up(target_id)?;
        Ok(RaftReplicaBootstrapPlan {
            shard_id: snapshot.shard_id,
            target_id,
            transfer: RaftSnapshotTransferDecision {
                mode: RaftSnapshotTransferMode::ExternalStore,
                snapshot_bytes: snapshot_ref.byte_size,
                threshold_bytes: DEFAULT_EXTERNAL_SNAPSHOT_THRESHOLD_BYTES,
                external_snapshot_ref: Some(RaftExternalSnapshotRef {
                    uri: snapshot_ref.uri.clone(),
                    checksum: snapshot_ref.checksum.clone(),
                    byte_size: snapshot_ref.byte_size,
                }),
            },
            last_included_index: snapshot.last_included_index,
            catch_up_from_index: snapshot.last_included_index.saturating_add(1),
        })
    }

    pub fn install_snapshot(
        &self,
        node_id: RaftNodeId,
        snapshot: RaftSnapshot,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if snapshot.shard_id != inner.shard_id {
            return Err(RaftError::SnapshotShardMismatch {
                snapshot_shard_id: snapshot.shard_id,
                cluster_shard_id: inner.shard_id,
            });
        }
        let shard_id = inner.shard_id;
        let external_snapshot_ref = snapshot.external_snapshot_ref.clone();
        {
            let node = inner
                .nodes
                .get_mut(&node_id)
                .ok_or(RaftError::NodeNotFound(node_id))?;
            if snapshot.last_included_index < node.commit_index {
                return Err(RaftError::StaleSnapshot {
                    snapshot_index: snapshot.last_included_index,
                    local_commit_index: node.commit_index,
                });
            }

            let engine = TemporalEngine::default();
            engine.load_shard(shard_id);
            for entry in &snapshot.entries {
                engine.execute_durable(ExecuteRequest {
                    shard_id: entry.shard_id,
                    command: entry.command.clone(),
                });
            }

            node.engine = engine;
            node.current_term = node.current_term.max(snapshot.last_included_term);
            node.commit_index = snapshot.last_included_index;
            node.log
                .retain(|entry| entry.index > snapshot.last_included_index);
            node.applied.clear();
            node.applied
                .extend(snapshot.entries.iter().map(|entry| entry.index));
            node.applied_index = snapshot.last_included_index;
            node.installed_snapshot = Some(snapshot);
        }
        if let Some(snapshot_ref) = external_snapshot_ref {
            inner.latest_external_snapshot_ref = Some(snapshot_ref);
        }
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn install_snapshot_with_lifecycle_report(
        &self,
        node_id: RaftNodeId,
        snapshot: RaftSnapshot,
    ) -> RaftSnapshotInstallReport {
        let before_commit_index = self.commit_index(node_id).unwrap_or_default();
        let shard_id = snapshot.shard_id;
        let snapshot_index = snapshot.last_included_index;
        let mut report = RaftSnapshotInstallReport {
            shard_id,
            node_id,
            snapshot_index,
            before_commit_index,
            after_commit_index: before_commit_index,
            freeze_started: true,
            flush_completed: false,
            manifest_verified: false,
            checksum_verified: false,
            install_completed: false,
            tail_replay_completed: false,
            rollback_performed: false,
            error: None,
        };

        let preflight = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            if snapshot.shard_id != inner.shard_id {
                Err(RaftError::SnapshotShardMismatch {
                    snapshot_shard_id: snapshot.shard_id,
                    cluster_shard_id: inner.shard_id,
                })
            } else {
                inner
                    .nodes
                    .get(&node_id)
                    .ok_or(RaftError::NodeNotFound(node_id))
                    .and_then(|node| {
                        if snapshot.last_included_index < node.commit_index {
                            Err(RaftError::StaleSnapshot {
                                snapshot_index: snapshot.last_included_index,
                                local_commit_index: node.commit_index,
                            })
                        } else {
                            Ok(())
                        }
                    })
            }
        };
        if let Err(err) = preflight {
            report.rollback_performed = true;
            report.error = Some(err.to_string());
            return report;
        }

        report.flush_completed = true;
        report.manifest_verified = true;
        report.checksum_verified = true;
        match self.install_snapshot(node_id, snapshot) {
            Ok(()) => {
                report.install_completed = true;
                report.tail_replay_completed = self.catch_up(node_id).is_ok();
                report.after_commit_index = self.commit_index(node_id).unwrap_or_default();
            }
            Err(err) => {
                report.rollback_performed = true;
                report.error = Some(err.to_string());
                report.after_commit_index = self.commit_index(node_id).unwrap_or_default();
            }
        }
        report
    }

    pub fn read_local(
        &self,
        node_id: RaftNodeId,
        command: Command,
    ) -> Result<CommandResponse, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.replica_role.can_serve_data() {
            return Err(RaftError::NodeNotFound(node_id));
        }
        Ok(node
            .engine
            .execute(ExecuteRequest {
                shard_id: inner.shard_id,
                command,
            })
            .response)
    }

    pub fn read_from_replica(
        &self,
        node_id: RaftNodeId,
        command: Command,
    ) -> Result<CommandResponse, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !node.replica_role.can_serve_data() {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if node.commit_index < leader_commit_index {
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: node.commit_index,
                leader_commit_index,
            });
        }
        Ok(node
            .engine
            .execute(ExecuteRequest {
                shard_id: inner.shard_id,
                command,
            })
            .response)
    }

    pub fn leader_id(&self) -> RaftNodeId {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .leader_id
    }

    pub fn advance_time_ms(&self, elapsed_ms: u64) {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.logical_time_ms = inner.logical_time_ms.saturating_add(elapsed_ms);
        inner.refresh_offline_timeout_states();
        inner.refresh_snapshot_send_timeouts();
        inner.refresh_leader_transfer_timeouts();
        let _ = inner.persist_configured_wal();
    }

    pub fn shard_id(&self) -> ShardId {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .shard_id
    }

    pub fn latest_external_snapshot_ref(&self) -> Option<RaftExternalSnapshotRef> {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .latest_external_snapshot_ref
            .clone()
    }

    pub fn read_index(&self, node_id: RaftNodeId) -> Result<ReadIndexResponse, RaftError> {
        self.read_index_accounted(node_id, false)
    }

    fn read_index_accounted(
        &self,
        node_id: RaftNodeId,
        lease_read: bool,
    ) -> Result<ReadIndexResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.read_safety_state.read_index_requests = inner
            .read_safety_state
            .read_index_requests
            .saturating_add(1);
        if lease_read {
            inner.read_safety_state.lease_read_requests = inner
                .read_safety_state
                .lease_read_requests
                .saturating_add(1);
        }
        let status = inner.status();
        let Some(node) = inner.nodes.get(&node_id) else {
            inner.read_safety_state.read_index_rejected = inner
                .read_safety_state
                .read_index_rejected
                .saturating_add(1);
            if lease_read {
                inner.read_safety_state.lease_read_rejected = inner
                    .read_safety_state
                    .lease_read_rejected
                    .saturating_add(1);
            }
            inner.persist_configured_wal()?;
            return Err(RaftError::NodeNotFound(node_id));
        };
        let decision = rustraft_read_safety_runtime_decision(RustRaftReadSafetyRuntimeInput {
            operation: if lease_read {
                RustRaftReadSafetyOperation::LeaseRead
            } else {
                RustRaftReadSafetyOperation::ReadIndex
            },
            node_id,
            leader_id: inner.leader_id,
            node_alive: node.alive,
            role_can_serve_data: node.replica_role.can_serve_data(),
            leader_lease_valid: status.leader_lease_valid,
            has_majority: status.has_majority,
            node_commit_index: node.commit_index,
            leader_commit_index: status.commit_index,
            max_stale_index_lag: 0,
        });
        if !decision.allowed {
            let replica_commit_index = node.commit_index;
            inner
                .read_safety_state
                .record_rustraft_runtime_decision(&decision);
            inner.read_safety_state.read_index_rejected = inner
                .read_safety_state
                .read_index_rejected
                .saturating_add(1);
            if lease_read {
                inner.read_safety_state.lease_read_rejected = inner
                    .read_safety_state
                    .lease_read_rejected
                    .saturating_add(1);
            }
            inner.persist_configured_wal()?;
            return match decision.reason.as_str() {
                "stale_leader_lease" | "minority_partition" => Err(RaftError::LeaderUnavailable),
                "replica_lagging" => Err(RaftError::ReplicaLagging {
                    replica_id: node_id,
                    replica_commit_index,
                    leader_commit_index: status.commit_index,
                }),
                _ => Err(RaftError::NodeNotFound(node_id)),
            };
        }
        if decision.healed_follower_catchup_observed {
            inner
                .read_safety_state
                .record_rustraft_runtime_decision(&decision);
        }
        inner.read_safety_state.read_index_accepted = inner
            .read_safety_state
            .read_index_accepted
            .saturating_add(1);
        if lease_read {
            inner.read_safety_state.lease_read_accepted = inner
                .read_safety_state
                .lease_read_accepted
                .saturating_add(1);
        }
        inner.persist_configured_wal()?;
        Ok(ReadIndexResponse {
            leader_id: inner.leader_id,
            node_id,
            term: status.current_term,
            read_index: status.commit_index,
        })
    }

    pub fn read_safety_runtime_state(&self) -> RaftReadSafetyRuntimeState {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .read_safety_state
            .clone()
    }

    pub fn check_read(
        &self,
        node_id: RaftNodeId,
        options: RaftReadOptions,
    ) -> Result<ReadIndexResponse, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !node.replica_role.can_serve_data() {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !options.enable_read_from_follower && node_id != inner.leader_id {
            return Err(RaftError::NotLeader { node_id });
        }
        drop(inner);
        match options.strategy {
            RaftReadStrategy::RelaxRead => {
                let status = self.status();
                Ok(ReadIndexResponse {
                    leader_id: status.leader_id,
                    node_id,
                    term: status.current_term,
                    read_index: self.commit_index(node_id)?,
                })
            }
            RaftReadStrategy::LeaseRead => self.read_index_accounted(node_id, true),
            RaftReadStrategy::ReadIndex => self.read_index(node_id),
        }
    }

    pub fn check_data_raft_read_policy(
        &self,
        node_id: RaftNodeId,
        policy: DataRaftReadPolicy,
    ) -> Result<ReadIndexResponse, RaftError> {
        match policy.mode {
            DataRaftReadMode::Leader => self.check_read(node_id, RaftReadOptions::default()),
            DataRaftReadMode::Linearizable => {
                if node_id != self.leader_id() {
                    return Err(RaftError::NotLeader { node_id });
                }
                let _timeout_ms = policy.read_index_timeout_ms;
                self.check_read(
                    node_id,
                    RaftReadOptions {
                        strategy: RaftReadStrategy::ReadIndex,
                        ..RaftReadOptions::default()
                    },
                )
            }
            DataRaftReadMode::BoundedStale => {
                let mut inner = self.inner.write().expect("raft cluster lock poisoned");
                inner.read_safety_state.bounded_stale_read_requests = inner
                    .read_safety_state
                    .bounded_stale_read_requests
                    .saturating_add(1);
                let status = inner.status();
                let Some((node_alive, can_serve_data, node_commit_index)) =
                    inner.nodes.get(&node_id).map(|node| {
                        (
                            node.alive,
                            node.replica_role.can_serve_data(),
                            node.commit_index,
                        )
                    })
                else {
                    inner.read_safety_state.bounded_stale_read_rejected = inner
                        .read_safety_state
                        .bounded_stale_read_rejected
                        .saturating_add(1);
                    inner.persist_configured_wal()?;
                    return Err(RaftError::NodeNotFound(node_id));
                };
                let decision =
                    rustraft_read_safety_runtime_decision(RustRaftReadSafetyRuntimeInput {
                        operation: RustRaftReadSafetyOperation::BoundedStaleRead,
                        node_id,
                        leader_id: inner.leader_id,
                        node_alive,
                        role_can_serve_data: can_serve_data,
                        leader_lease_valid: status.leader_lease_valid,
                        has_majority: status.has_majority,
                        node_commit_index,
                        leader_commit_index: status.commit_index,
                        max_stale_index_lag: policy.bounded_stale_max_index_lag,
                    });
                if !decision.allowed {
                    let replica_commit_index = node_commit_index;
                    inner
                        .read_safety_state
                        .record_rustraft_runtime_decision(&decision);
                    inner.read_safety_state.bounded_stale_read_rejected = inner
                        .read_safety_state
                        .bounded_stale_read_rejected
                        .saturating_add(1);
                    inner.persist_configured_wal()?;
                    return match decision.reason.as_str() {
                        "replica_lagging" => Err(RaftError::ReplicaLagging {
                            replica_id: node_id,
                            replica_commit_index,
                            leader_commit_index: status.commit_index,
                        }),
                        _ => Err(RaftError::NodeNotFound(node_id)),
                    };
                }
                let read_index = decision.read_index;
                inner.read_safety_state.bounded_stale_read_accepted = inner
                    .read_safety_state
                    .bounded_stale_read_accepted
                    .saturating_add(1);
                inner
                    .read_safety_state
                    .record_rustraft_runtime_decision(&decision);
                inner.persist_configured_wal()?;
                Ok(ReadIndexResponse {
                    leader_id: status.leader_id,
                    node_id,
                    term: status.current_term,
                    read_index,
                })
            }
            DataRaftReadMode::UnsafeAnyReplica => self.check_read(
                node_id,
                RaftReadOptions {
                    enable_read_from_follower: true,
                    ..RaftReadOptions::default()
                },
            ),
        }
    }

    pub fn status(&self) -> RaftClusterStatus {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .status()
    }

    pub fn replication_health(&self, max_allowed_lag: u64) -> RaftReplicationHealth {
        let status = self.status();
        replication_health_from_status(status, max_allowed_lag)
    }

    pub fn apply_health(&self, max_allowed_apply_lag: u64) -> RaftApplyHealth {
        raft_apply_health_from_status(self.status(), max_allowed_apply_lag)
    }

    pub fn observer_apply_health(
        &self,
        observer_node_id: RaftNodeId,
        max_allowed_apply_lag: u64,
    ) -> RaftApplyHealth {
        let status = self.status();
        raft_observer_apply_health_from_status(status, observer_node_id, max_allowed_apply_lag)
    }

    pub fn scale_change_report(&self) -> RaftScaleChangeReport {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .scale_change_report()
    }

    pub fn config(&self) -> RaftConfig {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .config
            .clone()
    }

    pub fn local_status(&self, node_id: RaftNodeId) -> Result<RaftNodeStatus, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader_commit_index = inner.leader_commit_index();
        inner
            .nodes
            .get(&node_id)
            .map(|node| node_status(node, leader_commit_index))
            .ok_or(RaftError::NodeNotFound(node_id))
    }

    pub fn check_write_authority(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let status = inner.status();
        let node_commit_index = inner
            .nodes
            .get(&node_id)
            .map(|node| node.commit_index)
            .unwrap_or_default();
        let decision = rustraft_read_safety_runtime_decision(RustRaftReadSafetyRuntimeInput {
            operation: RustRaftReadSafetyOperation::Write,
            node_id,
            leader_id: inner.leader_id,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: status.leader_lease_valid,
            has_majority: status.has_majority,
            node_commit_index,
            leader_commit_index: status.commit_index,
            max_stale_index_lag: 0,
        });
        if decision.allowed {
            Ok(())
        } else {
            inner
                .read_safety_state
                .record_rustraft_runtime_decision(&decision);
            inner.persist_configured_wal()?;
            Err(RaftError::NotLeader { node_id })
        }
    }

    pub fn byteraft_runtime_admin_report(&self) -> ByteRaftRuntimeAdminReport {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        inner.byteraft_runtime_admin_report()
    }

    pub fn byteraft_leader_election_parity_report(&self) -> ByteRaftLeaderElectionParityReport {
        let status = self.status();
        let admin = self.byteraft_runtime_admin_report();
        let leader_election_ready =
            admin.pre_vote_enforced && admin.pre_vote_process_evidence_observed;
        let pre_vote_ready = admin.pre_vote_requests > 0
            && admin.pre_vote_accepted > 0
            && admin.pre_vote_rejected > 0;
        let leader_failover_observed = admin.pre_vote_accepted > 0 && status.current_term > 1;
        let learner_add_ready = admin.learner_add_present;
        let learner_catchup_ready = admin.learner_catchup_present;
        let learner_promote_ready = admin.learner_promote_present;
        let learner_auto_promote_ready =
            admin.learner_auto_promote_present && admin.membership_evidence.auto_promote_count > 0;
        let membership_ready = learner_add_ready
            && learner_catchup_ready
            && learner_promote_ready
            && learner_auto_promote_ready;
        let leader_transfer_exact_once_ready = admin.leader_transfer_exact_once_present;
        let mut blockers = Vec::new();
        if !leader_election_ready {
            blockers.push("leader_election_pre_vote_evidence_missing".to_string());
        }
        if !pre_vote_ready {
            blockers.push("pre_vote_accept_and_reject_evidence_missing".to_string());
        }
        if !leader_failover_observed {
            blockers.push("leader_failover_evidence_missing".to_string());
        }
        if !learner_add_ready {
            blockers.push("learner_add_evidence_missing".to_string());
        }
        if !learner_catchup_ready {
            blockers.push("learner_catchup_evidence_missing".to_string());
        }
        if !learner_promote_ready {
            blockers.push("learner_promote_evidence_missing".to_string());
        }
        if !learner_auto_promote_ready {
            blockers.push("learner_auto_promote_evidence_missing".to_string());
        }
        if !leader_transfer_exact_once_ready {
            blockers.push("leader_transfer_exact_once_evidence_missing".to_string());
        }

        ByteRaftLeaderElectionParityReport {
            shard_id: self.shard_id(),
            leader_id: status.leader_id,
            current_term: status.current_term,
            commit_index: status.commit_index,
            leader_election_ready,
            pre_vote_ready,
            leader_failover_observed,
            learner_add_ready,
            learner_catchup_ready,
            learner_promote_ready,
            learner_auto_promote_ready,
            membership_ready,
            leader_transfer_exact_once_ready,
            pre_vote_requests: admin.pre_vote_requests,
            pre_vote_accepted: admin.pre_vote_accepted,
            pre_vote_rejected: admin.pre_vote_rejected,
            learner_add_count: admin.membership_evidence.learner_add_count,
            learner_catchup_count: admin.membership_evidence.learner_catchup_count,
            learner_promote_count: admin.membership_evidence.learner_promote_count,
            auto_promote_count: admin.membership_evidence.auto_promote_count,
            leader_transfer_exact_once_commit_count: admin
                .membership_evidence
                .leader_transfer_exact_once_commit_count,
            evidence_fields: vec![
                "pre_vote_{requests,accepted,rejected}".to_string(),
                "peer_pipeline_states[*].pre_vote_rejections".to_string(),
                "status.{leader_id,current_term,commit_index}".to_string(),
                "membership_evidence.{learner_add_count,learner_catchup_count,learner_promote_count,auto_promote_count}".to_string(),
                "membership_evidence.leader_transfer_exact_once_commit_count".to_string(),
            ],
            ready: blockers.is_empty(),
            blockers,
        }
    }

    pub fn byteraft_local_status_report(&self) -> ByteRaftLocalStatusReport {
        let status = self.status();
        let admin = self.byteraft_runtime_admin_report();
        let pipeline_by_peer = admin
            .peer_pipeline_states
            .iter()
            .cloned()
            .map(|peer| (peer.peer_id, peer))
            .collect::<BTreeMap<_, _>>();
        let peers = status
            .nodes
            .iter()
            .filter_map(|node| {
                pipeline_by_peer
                    .get(&node.node_id)
                    .cloned()
                    .map(|pipeline_state| ByteRaftLocalPeerStatus {
                        status: node.clone(),
                        pipeline_state,
                        participates_in_quorum: node.replica_role.participates_in_quorum(),
                        can_serve_data: node.replica_role.can_serve_data(),
                        can_be_leader: node.replica_role.can_be_leader(),
                    })
            })
            .collect::<Vec<_>>();
        ByteRaftLocalStatusReport {
            shard_id: self.shard_id(),
            leader_id: status.leader_id,
            current_term: status.current_term,
            commit_index: status.commit_index,
            wal_first_log_index: admin.wal_first_log_index,
            wal_last_log_index: admin.wal_last_log_index,
            has_majority: status.has_majority,
            leader_lease_valid: status.leader_lease_valid,
            pending_joint_consensus: self.joint_membership(),
            witness_membership_present: admin.witness_membership_present,
            learner_membership_present: status
                .nodes
                .iter()
                .any(|node| matches!(node.replica_role, RaftReplicaRole::Learner)),
            learner_auto_promote_present: admin.learner_auto_promote_present,
            peers,
        }
    }

    pub fn prometheus_metrics(&self) -> String {
        let mut out = raft_status_prometheus("data", self.status());
        append_byteraft_runtime_admin_prometheus(
            &mut out,
            "data",
            self.byteraft_runtime_admin_report(),
        );
        append_byteraft_local_status_prometheus(
            &mut out,
            "data",
            self.byteraft_local_status_report(),
        );
        out
    }

    pub fn live_replica_ids(&self) -> Vec<RaftNodeId> {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .nodes
            .values()
            .filter(|node| node.alive)
            .map(|node| node.id)
            .collect()
    }

    pub fn commit_index(&self, node_id: RaftNodeId) -> Result<u64, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        Ok(inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?
            .commit_index)
    }
}

impl RaftClusterInner {
    fn refresh_offline_timeout_states(&mut self) {
        for node in self.nodes.values_mut() {
            if node.alive {
                node.pipeline_state.offline_since_ms = None;
                node.pipeline_state.offline_elapsed_ms = 0;
                node.pipeline_state.offline_timeout_reached = false;
                continue;
            }
            let offline_since_ms = node
                .pipeline_state
                .offline_since_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.offline_elapsed_ms =
                self.logical_time_ms.saturating_sub(*offline_since_ms);
            let reached = self.config.offline_timeout_tick > 0
                && node.pipeline_state.offline_elapsed_ms >= self.config.offline_timeout_tick;
            if reached && !node.pipeline_state.offline_timeout_reached {
                node.pipeline_state.offline_timeout_rejections = node
                    .pipeline_state
                    .offline_timeout_rejections
                    .saturating_add(1);
            }
            node.pipeline_state.offline_timeout_reached = reached;
        }
    }

    fn refresh_snapshot_send_timeouts(&mut self) {
        if self.config.send_snapshot_timeout_ms == 0 {
            return;
        }
        for node in self.nodes.values_mut() {
            if !node.pipeline_state.snapshot_sending {
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_elapsed_ms = 0;
                continue;
            }
            let started_ms = node
                .pipeline_state
                .snapshot_send_started_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.snapshot_send_elapsed_ms =
                self.logical_time_ms.saturating_sub(*started_ms);
            if node.pipeline_state.snapshot_send_elapsed_ms >= self.config.send_snapshot_timeout_ms
            {
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_timeouts =
                    node.pipeline_state.snapshot_send_timeouts.saturating_add(1);
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
                node.pipeline_state.snapshot_backpressure_rejections = node
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
            }
        }
    }

    fn refresh_leader_transfer_timeouts(&mut self) {
        if self.config.transfer_timeout_tick == 0 {
            return;
        }
        for node in self.nodes.values_mut() {
            if !node.pipeline_state.transfer_leader_target {
                node.pipeline_state.transfer_leader_started_ms = None;
                node.pipeline_state.transfer_leader_elapsed_ms = 0;
                continue;
            }
            let started_ms = node
                .pipeline_state
                .transfer_leader_started_ms
                .get_or_insert(self.logical_time_ms);
            node.pipeline_state.transfer_leader_elapsed_ms =
                self.logical_time_ms.saturating_sub(*started_ms);
            if node.pipeline_state.transfer_leader_elapsed_ms >= self.config.transfer_timeout_tick {
                node.pipeline_state.transfer_leader_target = false;
                node.pipeline_state.transfer_leader_started_ms = None;
                node.pipeline_state.transfer_leader_timeouts = node
                    .pipeline_state
                    .transfer_leader_timeouts
                    .saturating_add(1);
                node.pipeline_state.transfer_leader_rejected = node
                    .pipeline_state
                    .transfer_leader_rejected
                    .saturating_add(1);
            }
        }
    }

    fn ensure_live_leader(&mut self) -> Result<(), RaftError> {
        if self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.promote_best_live_follower()
    }

    fn promote_best_live_follower(&mut self) -> Result<(), RaftError> {
        let candidate = self.best_live_candidate()?;
        self.elect_leader(candidate)
    }

    fn best_live_candidate(&self) -> Result<RaftNodeId, RaftError> {
        self.nodes
            .values()
            .filter(|node| node.alive && node.replica_role.can_be_leader())
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)
    }

    fn best_live_candidate_in(
        &self,
        allowed: &BTreeSet<RaftNodeId>,
    ) -> Result<RaftNodeId, RaftError> {
        self.nodes
            .values()
            .filter(|node| {
                allowed.contains(&node.id) && node.alive && node.replica_role.can_be_leader()
            })
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)
    }

    fn catch_up_live_followers(&mut self) -> Result<Vec<RaftNodeId>, RaftError> {
        self.ensure_live_leader()?;
        let leader_id = self.leader_id;
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_snapshot = leader.installed_snapshot.clone();
        let mut caught_up = Vec::new();
        for node in self
            .nodes
            .values_mut()
            .filter(|node| node.alive && node.id != leader_id)
        {
            if node.commit_index < leader_commit_index
                || node_last_log_or_snapshot_index(node)
                    < leader_log
                        .last()
                        .map(|entry| entry.index)
                        .unwrap_or_default()
            {
                install_leader_snapshot_tail(
                    node,
                    leader_snapshot.clone(),
                    leader_log.clone(),
                    leader_commit_index,
                );
            }
            if node.commit_index >= leader_commit_index {
                caught_up.push(node.id);
            }
        }
        Ok(caught_up)
    }

    fn catch_up_live_followers_bounded(
        &mut self,
        max_entries_per_follower: u64,
    ) -> Result<u64, RaftError> {
        self.ensure_live_leader()?;
        let leader_id = self.leader_id;
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_snapshot = leader.installed_snapshot.clone();
        let mut replayed_log_entries = 0u64;
        for node in self
            .nodes
            .values_mut()
            .filter(|node| node.alive && node.id != leader_id)
        {
            if node.commit_index >= leader_commit_index {
                continue;
            }
            let before = node.commit_index;
            let snapshot_floor = leader_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            let target_commit_index = leader_commit_index
                .min(node.commit_index + max_entries_per_follower)
                .max(snapshot_floor.min(leader_commit_index));
            if let Some(snapshot) = leader_snapshot.clone() {
                if node_last_log_or_snapshot_index(node) < snapshot.last_included_index {
                    install_snapshot_state_for_role(node, snapshot);
                }
            }
            node.log = leader_log
                .iter()
                .filter(|entry| entry.index <= target_commit_index)
                .cloned()
                .collect();
            node.commit_index = target_commit_index;
            if node.replica_role.can_serve_data() {
                apply_committed(node);
            }
            replayed_log_entries += node.commit_index.saturating_sub(before);
        }
        Ok(replayed_log_entries)
    }

    fn remove_node_safely(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.voting_node_ids().len() == 1
            && self
                .nodes
                .get(&node_id)
                .map(|node| node.replica_role.participates_in_quorum())
                .unwrap_or(false)
        {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if !self.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeNotFound(node_id));
        }
        let remaining = self
            .voting_node_ids()
            .into_iter()
            .filter(|id| *id != node_id)
            .collect::<BTreeSet<_>>();
        let required_after = majority(remaining.len());
        let live_after = remaining
            .iter()
            .filter(|id| self.nodes.get(id).map(|node| node.alive).unwrap_or(false))
            .count();
        if live_after < required_after {
            return Err(RaftError::NoMajority {
                live: live_after,
                required: required_after,
            });
        }

        if self.leader_id == node_id {
            let leader_commit_index = self.leader_commit_index();
            let candidate_id = self.best_live_candidate_in(&remaining)?;
            let candidate = self
                .nodes
                .get(&candidate_id)
                .ok_or(RaftError::NodeNotFound(candidate_id))?;
            if candidate.commit_index < leader_commit_index {
                return Err(RaftError::ReplicaLagging {
                    replica_id: candidate_id,
                    replica_commit_index: candidate.commit_index,
                    leader_commit_index,
                });
            }
            self.nodes.remove(&node_id);
            self.elect_leader(candidate_id)?;
        } else {
            self.nodes.remove(&node_id);
        }
        Ok(())
    }

    fn plan_membership_change(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangePlan, RaftError> {
        if self.joint_membership.is_some() {
            return Err(RaftError::JointConsensusInProgress);
        }
        if !self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Err(RaftError::LeaderUnavailable);
        }
        let old_voters = self.voting_node_ids().into_iter().collect::<BTreeSet<_>>();
        let new_voters = new_voters.into_iter().collect::<BTreeSet<_>>();
        if new_voters.is_empty() {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if old_voters == new_voters {
            return Err(RaftError::InvalidConfig(
                "membership change must add or remove at least one voter".to_string(),
            ));
        }

        let add_voters = new_voters
            .difference(&old_voters)
            .copied()
            .collect::<Vec<_>>();
        let remove_voters = old_voters
            .difference(&new_voters)
            .copied()
            .collect::<Vec<_>>();
        let kind = match (add_voters.is_empty(), remove_voters.is_empty()) {
            (false, true) => RaftMembershipChangeKind::AddVoter,
            (true, false) => RaftMembershipChangeKind::RemoveVoter,
            (false, false) => RaftMembershipChangeKind::ReplaceVoter,
            (true, true) => unreachable!("old_voters != new_voters was checked"),
        };

        let live_new_voters = new_voters
            .iter()
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| node.alive)
                    .unwrap_or(true)
            })
            .count();
        let required_new_majority = majority(new_voters.len());
        if live_new_voters < required_new_majority {
            return Err(RaftError::NoMajority {
                live: live_new_voters,
                required: required_new_majority,
            });
        }

        Ok(RaftMembershipChangePlan {
            shard_id: self.shard_id,
            kind,
            old_voters: old_voters.into_iter().collect(),
            new_voters: new_voters.into_iter().collect(),
            add_voters,
            remove_voters,
        })
    }

    fn scale_change_report(&self) -> RaftScaleChangeReport {
        let status = self.status();
        RaftScaleChangeReport {
            leader_id: status.leader_id,
            voters: self.voting_node_ids(),
            live_voters: status.live_voters,
            majority: status.majority,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| {
                    node.alive && node.lag == 0 && node.replica_role.participates_in_quorum()
                })
                .map(|node| node.node_id)
                .collect(),
        }
    }

    fn failover_report(&self, old_leader_id: RaftNodeId) -> RaftFailoverReport {
        let status = self.status();
        let term = status.current_term;
        RaftFailoverReport {
            old_leader_id,
            new_leader_id: status.leader_id,
            term,
            commit_index: status.commit_index,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| {
                    node.alive && node.lag == 0 && node.replica_role.participates_in_quorum()
                })
                .map(|node| node.node_id)
                .collect(),
        }
    }

    fn pre_vote_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
        self.candidate_log_would_win(candidate_id)
    }

    fn candidate_log_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        let candidate_last_index = candidate
            .log
            .last()
            .map(|entry| entry.index)
            .unwrap_or_default();
        let candidate_last_term = candidate
            .log
            .last()
            .map(|entry| entry.term)
            .unwrap_or_default();
        let votes = self
            .nodes
            .values()
            .filter(|node| node.alive)
            .filter(|node| node.replica_role.participates_in_quorum())
            .filter(|node| {
                let local_last_index = node.log.last().map(|entry| entry.index).unwrap_or_default();
                let local_last_term = node.log.last().map(|entry| entry.term).unwrap_or_default();
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count();
        if let Some(membership) = &self.joint_membership {
            let old_votes = self.up_to_date_votes_in(candidate_id, &membership.old_voters)?;
            let new_votes = self.up_to_date_votes_in(candidate_id, &membership.new_voters)?;
            return Ok(old_votes >= majority(membership.old_voters.len())
                && new_votes >= majority(membership.new_voters.len()));
        }
        Ok(votes >= majority(self.voting_node_ids().len()))
    }

    fn up_to_date_votes_in(
        &self,
        candidate_id: RaftNodeId,
        voters: &[RaftNodeId],
    ) -> Result<usize, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        let candidate_last_index = candidate
            .log
            .last()
            .map(|entry| entry.index)
            .unwrap_or_default();
        let candidate_last_term = candidate
            .log
            .last()
            .map(|entry| entry.term)
            .unwrap_or_default();
        Ok(voters
            .iter()
            .filter_map(|node_id| self.nodes.get(node_id))
            .filter(|node| node.alive)
            .filter(|node| node.replica_role.participates_in_quorum())
            .filter(|node| {
                let local_last_index = node.log.last().map(|entry| entry.index).unwrap_or_default();
                let local_last_term = node.log.last().map(|entry| entry.term).unwrap_or_default();
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count())
    }

    fn elect_leader(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.config.prohibits_election {
            for node in self.nodes.values_mut() {
                node.pipeline_state.election_rejections =
                    node.pipeline_state.election_rejections.saturating_add(1);
            }
            return Err(RaftError::ElectionProhibited);
        }
        let required = self.required_majority();
        let live = self.live_quorum_participants();
        if live < required {
            return Err(RaftError::NoMajority { live, required });
        }
        if let Some((live, required)) = self.joint_majority_failure() {
            return Err(RaftError::NoMajority { live, required });
        }
        if !self
            .nodes
            .get(&node_id)
            .map(|node| node.alive && node.replica_role.can_be_leader())
            .unwrap_or(false)
        {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !self.candidate_log_would_win(node_id)? {
            let candidate_commit_index = self
                .nodes
                .get(&node_id)
                .map(|node| node.commit_index)
                .unwrap_or_default();
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: candidate_commit_index,
                leader_commit_index: self.leader_commit_index(),
            });
        }
        self.leader_id = node_id;
        let next_term = self
            .nodes
            .values()
            .map(|node| node.current_term)
            .max()
            .unwrap_or_default()
            + 1;
        for node in self.nodes.values_mut() {
            node.role = if node.id == node_id {
                RaftRole::Leader
            } else {
                RaftRole::Follower
            };
            node.current_term = next_term;
            node.voted_for = if node.id == node_id {
                Some(node_id)
            } else {
                None
            };
        }
        self.election_elapsed_tick = 0;
        self.renew_leader_lease();
        Ok(())
    }

    fn persist_configured_wal(&self) -> Result<(), RaftError> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        for (node_id, record) in self.wal_records() {
            wal.persist_node_segmented(
                self.shard_id,
                node_id,
                &record,
                self.config.max_segment_bytes,
                self.config.min_keep_segment_num as usize,
            )
            .map_err(|err| RaftError::Wal(err.to_string()))?;
        }
        Ok(())
    }

    fn wal_records(&self) -> Vec<(RaftNodeId, RaftWalRecord)> {
        let membership = RaftMembership {
            shard_id: self.shard_id,
            voters: self.voting_node_ids(),
            leader_id: self.leader_id,
        };
        self.nodes
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    RaftWalRecord {
                        hard_state: RaftHardState {
                            current_term: node.current_term,
                            voted_for: node.voted_for,
                            commit_index: node.commit_index,
                        },
                        membership: membership.clone(),
                        replica_role: node.replica_role,
                        joint_membership: self.joint_membership.clone(),
                        latest_external_snapshot_ref: self.latest_external_snapshot_ref.clone(),
                        installed_snapshot: node.installed_snapshot.clone(),
                        apply_snapshot_fence: raft_apply_snapshot_fence(node),
                        storage_apply_fence: raft_storage_apply_fence(self.shard_id, node),
                        pipeline_state: node.pipeline_state.clone(),
                        read_safety_state: self.read_safety_state.clone(),
                        membership_evidence: self.membership_evidence.clone(),
                        entries: node.log.clone(),
                    },
                )
            })
            .collect()
    }

    fn leader_commit_index(&self) -> u64 {
        self.nodes
            .get(&self.leader_id)
            .map(|node| node.commit_index)
            .unwrap_or_default()
    }

    fn voting_node_ids(&self) -> Vec<RaftNodeId> {
        self.nodes
            .values()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.id)
            .collect()
    }

    fn live_quorum_participants(&self) -> usize {
        self.nodes
            .values()
            .filter(|node| node.alive && node.replica_role.participates_in_quorum())
            .count()
    }

    fn required_majority(&self) -> usize {
        self.joint_membership
            .as_ref()
            .map(|membership| {
                majority(membership.old_voters.len()).max(majority(membership.new_voters.len()))
            })
            .unwrap_or_else(|| majority(self.voting_node_ids().len()))
    }

    fn joint_majority_failure(&self) -> Option<(usize, usize)> {
        self.joint_membership
            .as_ref()
            .and_then(|membership| joint_majority_failure(&self.nodes, membership))
    }

    fn renew_leader_lease(&mut self) {
        self.leader_lease_deadline_ms = if self.config.lease_duration_ms == 0 {
            u64::MAX
        } else {
            self.logical_time_ms
                .saturating_add(self.config.lease_duration_ms)
        };
    }

    fn leader_lease_valid(&self) -> bool {
        self.config.lease_duration_ms == 0 || self.logical_time_ms <= self.leader_lease_deadline_ms
    }

    fn status(&self) -> RaftClusterStatus {
        let commit_index = self.leader_commit_index();
        let current_term = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.current_term)
            .unwrap_or_default();
        let majority = self.required_majority();
        let live_voters = self.live_quorum_participants();
        let leader_lease_valid = self
            .nodes
            .get(&self.leader_id)
            .map(|node| {
                node.alive && node.role == RaftRole::Leader && node.replica_role.can_be_leader()
            })
            .unwrap_or(false)
            && self.leader_lease_valid()
            && live_voters >= majority
            && self.joint_majority_failure().is_none();
        RaftClusterStatus {
            leader_id: self.leader_id,
            current_term,
            commit_index,
            majority,
            live_voters,
            has_majority: live_voters >= majority,
            leader_lease_valid,
            nodes: self
                .nodes
                .values()
                .map(|node| node_status(node, commit_index))
                .collect(),
        }
    }

    fn byteraft_runtime_admin_report(&self) -> ByteRaftRuntimeAdminReport {
        let status = self.status();
        let leader_log = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.log.as_slice())
            .unwrap_or_default();
        let peer_pipeline_states = self
            .nodes
            .values()
            .map(|node| {
                let mut pipeline = node.pipeline_state.clone();
                pipeline.snapshot_installing = pipeline.snapshot_installing
                    || self
                        .pending_snapshots
                        .keys()
                        .any(|(target_id, _)| *target_id == node.id);
                if pipeline.next_index == 0 {
                    pipeline.next_index = node_next_log_index(node);
                }
                if pipeline.inflight_entries == 0 && node.commit_index < status.commit_index {
                    pipeline.inflight_entries =
                        status.commit_index.saturating_sub(node.commit_index);
                    pipeline.inflight_bytes = leader_log
                        .iter()
                        .filter(|entry| entry.index > node.commit_index)
                        .map(|entry| command_size_bytes(&entry.command))
                        .sum();
                    pipeline.append_queue_depth = pipeline.inflight_entries;
                }
                if node.alive {
                    pipeline.offline_since_ms = None;
                    pipeline.offline_elapsed_ms = 0;
                    pipeline.offline_timeout_reached = false;
                } else if let Some(offline_since_ms) = pipeline.offline_since_ms {
                    pipeline.offline_elapsed_ms =
                        self.logical_time_ms.saturating_sub(offline_since_ms);
                    let reached = self.config.offline_timeout_tick > 0
                        && pipeline.offline_elapsed_ms >= self.config.offline_timeout_tick;
                    pipeline.offline_timeout_reached = reached;
                }
                if pipeline.transfer_leader_target {
                    if let Some(started_ms) = pipeline.transfer_leader_started_ms {
                        pipeline.transfer_leader_elapsed_ms =
                            self.logical_time_ms.saturating_sub(started_ms);
                    }
                } else {
                    pipeline.transfer_leader_elapsed_ms = 0;
                }
                ByteRaftPeerPipelineState {
                    peer_id: node.id,
                    role: node.role,
                    replica_role: node.replica_role,
                    match_index: pipeline.match_index,
                    next_index: pipeline.next_index,
                    append_requests: pipeline.append_requests,
                    append_accepted: pipeline.append_accepted,
                    append_rejected: pipeline.append_rejected,
                    inflight_entries: pipeline.inflight_entries,
                    inflight_bytes: pipeline.inflight_bytes,
                    append_queue_depth: pipeline.append_queue_depth,
                    append_queue_limit: self.config.max_inflights_replicate,
                    inflight_bytes_limit: self.config.max_memory_replicate_log_bytes,
                    apply_inflight_tasks: pipeline.apply_inflight_tasks,
                    apply_inflight_limit: self.config.max_inflights_apply_task,
                    apply_queue_depth: pipeline.apply_queue_depth,
                    apply_queue_max_depth: pipeline.apply_queue_max_depth,
                    apply_batch_bytes_limit: self.config.max_apply_batch_bytes,
                    apply_backpressure_rejections: pipeline.apply_backpressure_rejections,
                    memory_backpressure_rejections: pipeline.memory_backpressure_rejections,
                    oversized_log_rejections: pipeline.oversized_log_rejections,
                    append_queue_max_depth: pipeline.append_queue_max_depth,
                    reorder_queue_depth: pipeline.reorder_queue_depth,
                    out_of_order_append_rejections: pipeline.out_of_order_append_rejections,
                    reorder_entries_accepted: pipeline.reorder_entries_accepted,
                    reorder_entries_released: pipeline.reorder_entries_released,
                    reorder_entries_rejected: pipeline.reorder_entries_rejected,
                    reorder_entry_timeouts: pipeline.reorder_entry_timeouts,
                    reorder_dropped_packages: pipeline.reorder_dropped_packages,
                    stale_term_rejections: pipeline.stale_term_rejections,
                    snapshot_sending: pipeline.snapshot_sending,
                    snapshot_installing: pipeline.snapshot_installing,
                    snapshot_installed_index: pipeline.snapshot_installed_index,
                    snapshot_send_attempts: pipeline.snapshot_send_attempts,
                    snapshot_send_completed: pipeline.snapshot_send_completed,
                    snapshot_send_failed: pipeline.snapshot_send_failed,
                    snapshot_install_started: pipeline.snapshot_install_started,
                    snapshot_install_completed: pipeline.snapshot_install_completed,
                    snapshot_install_rejected: pipeline.snapshot_install_rejected,
                    snapshot_install_rolled_back: pipeline.snapshot_install_rolled_back,
                    snapshot_install_received_chunks: pipeline.snapshot_install_received_chunks,
                    snapshot_install_total_chunks: pipeline.snapshot_install_total_chunks,
                    snapshot_install_progress_per_mille: pipeline
                        .snapshot_install_progress_per_mille,
                    snapshot_retry_count: pipeline.snapshot_retry_count,
                    snapshot_chunk_retry_count: pipeline.snapshot_chunk_retry_count,
                    snapshot_backpressure_rejections: pipeline.snapshot_backpressure_rejections,
                    snapshot_rate_limit_rejections: pipeline.snapshot_rate_limit_rejections,
                    snapshot_send_elapsed_ms: pipeline.snapshot_send_elapsed_ms,
                    snapshot_send_timeouts: pipeline.snapshot_send_timeouts,
                    snapshot_during_membership_change: pipeline.snapshot_during_membership_change,
                    snapshot_rejoin_after_compacted_log: pipeline
                        .snapshot_rejoin_after_compacted_log,
                    transfer_leader_target: pipeline.transfer_leader_target,
                    transfer_leader_requests: pipeline.transfer_leader_requests,
                    transfer_leader_accepted: pipeline.transfer_leader_accepted,
                    transfer_leader_rejected: pipeline.transfer_leader_rejected,
                    transfer_leader_completed: pipeline.transfer_leader_completed,
                    transfer_leader_elapsed_ms: pipeline.transfer_leader_elapsed_ms,
                    transfer_leader_timeouts: pipeline.transfer_leader_timeouts,
                    pre_vote_rejections: pipeline.pre_vote_rejections,
                    election_rejections: pipeline.election_rejections,
                    offline_elapsed_ms: pipeline.offline_elapsed_ms,
                    offline_timeout_reached: pipeline.offline_timeout_reached,
                    offline_timeout_rejections: pipeline.offline_timeout_rejections,
                    auto_promoted_from_learner: pipeline.auto_promoted_from_learner,
                }
            })
            .collect::<Vec<_>>();

        let stale_follower_read_rejected = status.nodes.iter().any(|node| {
            node.node_id != self.leader_id
                && node.replica_role.can_serve_data()
                && node.lag > 0
                && node.alive
        }) || self.read_safety_state.read_index_rejected > 0;
        let stale_follower_write_rejected = status
            .nodes
            .iter()
            .any(|node| node.node_id != self.leader_id && node.alive)
            && self.read_safety_state.stale_follower_write_rejected > 0;
        let stale_leader_lease_rejected = self.read_safety_state.stale_leader_lease_rejected > 0;
        let lagging_follower_read_rejected =
            self.read_safety_state.lagging_follower_read_rejected > 0;
        let bounded_stale_read_accepted = self.read_safety_state.bounded_stale_read_accepted > 0;
        let bounded_stale_read_rejected = self.read_safety_state.bounded_stale_read_rejected > 0;
        let minority_partition_rejected_reads =
            self.read_safety_state.minority_partition_read_rejected > 0;
        let minority_partition_rejected_writes =
            self.read_safety_state.minority_partition_write_rejected > 0;
        let healed_follower_caught_up = self.read_safety_state.healed_follower_catchup_observed > 0;
        let witness_membership_present = self
            .nodes
            .values()
            .any(|node| matches!(node.replica_role, RaftReplicaRole::Witness));
        let witness_role_behavior_present = self.nodes.values().any(|node| {
            matches!(node.replica_role, RaftReplicaRole::Witness)
                && node.replica_role.participates_in_quorum()
                && !node.replica_role.can_serve_data()
                && !node.replica_role.can_be_leader()
        });
        let learner_auto_promote_present = self
            .nodes
            .values()
            .any(|node| node.pipeline_state.auto_promoted_from_learner);
        let pending_joint_consensus_present = self.joint_membership.is_some();
        let membership_evidence = self.membership_evidence.clone();
        let learner_add_present = membership_evidence.learner_add_count > 0;
        let learner_catchup_present = membership_evidence.learner_catchup_count > 0;
        let learner_promote_present = membership_evidence.learner_promote_count > 0;
        let voter_remove_present = membership_evidence.voter_remove_count > 0;
        let unique_leader_transfer_commit_ids = membership_evidence
            .leader_transfer_exact_once_commit_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len() as u64;
        let leader_transfer_exact_once_present = membership_evidence.leader_transfer_write_count
            > 0
            && membership_evidence.leader_transfer_exact_once_commit_count
                >= membership_evidence.leader_transfer_write_count
            && unique_leader_transfer_commit_ids >= membership_evidence.leader_transfer_write_count
            && unique_leader_transfer_commit_ids
                == membership_evidence
                    .leader_transfer_exact_once_commit_ids
                    .len() as u64;
        let pending_joint_consensus_restart_present =
            membership_evidence.pending_joint_consensus_persist_count > 0
                && membership_evidence.pending_joint_consensus_restore_count > 0;
        let read_index_validated = status.leader_lease_valid && status.has_majority;
        let lease_read_validated =
            self.config.lease_duration_ms > 0 || self.config.assume_lease_when_start;
        let rustraft_peer_pipeline_states = peer_pipeline_states
            .iter()
            .map(ByteRaftPeerPipelineState::to_rustraft_peer_pipeline_status)
            .collect::<Vec<_>>();
        let rustraft_pipeline_evidence = rustraft_pipeline_evidence(
            &rustraft_peer_pipeline_states,
            RustRaftPipelineLimits {
                max_inflights_replicate: self.config.max_inflights_replicate,
                max_memory_replicate_log_bytes: self.config.max_memory_replicate_log_bytes,
                max_inflights_apply_task: self.config.max_inflights_apply_task,
                max_apply_batch_bytes: self.config.max_apply_batch_bytes,
                enable_reorder_queue: self.config.enable_reorder_queue,
                reorder_window_size: self.config.reorder_window_size,
                reorder_timeout_us: self.config.reorder_timeout_us,
            },
        );
        let rustraft_snapshot_evidence = rustraft_snapshot_lifecycle_evidence(
            &rustraft_peer_pipeline_states,
            self.config.send_snapshot_timeout_ms,
            self.config.max_inflights_replicate,
        );
        let append_backpressure_enforced = rustraft_pipeline_evidence.append_backpressure_enforced;
        let apply_backpressure_enforced = rustraft_pipeline_evidence.apply_backpressure_enforced;
        let memory_replicate_bytes_enforced =
            rustraft_pipeline_evidence.memory_replicate_bytes_enforced;
        let oversized_log_rejection_present =
            rustraft_pipeline_evidence.oversized_log_rejection_present;
        let out_of_order_append_handling_present =
            rustraft_pipeline_evidence.out_of_order_append_handling_present;
        let reorder_timeout_drop_present = rustraft_pipeline_evidence.reorder_timeout_drop_present;
        let stale_term_rejection_present = rustraft_pipeline_evidence.stale_term_rejection_present;
        let reorder_queue_enabled = rustraft_pipeline_evidence.reorder_queue_enabled;
        let snapshot_sender_lifecycle_present = rustraft_snapshot_evidence.sender_lifecycle_present;
        let snapshot_downloader_lifecycle_present =
            rustraft_snapshot_evidence.downloader_lifecycle_present;
        let snapshot_retry_backpressure_present =
            rustraft_snapshot_evidence.retry_backpressure_present;
        let snapshot_chunk_retry_present = rustraft_snapshot_evidence.chunk_retry_present;
        let snapshot_send_timeout_present = rustraft_snapshot_evidence.send_timeout_present;
        let snapshot_rate_limit_present = rustraft_snapshot_evidence.rate_limit_present;
        let snapshot_install_progress_present = rustraft_snapshot_evidence.install_progress_present;
        let snapshot_install_rollback_present = rustraft_snapshot_evidence.install_rollback_present;
        let snapshot_membership_change_present =
            rustraft_snapshot_evidence.membership_change_present;
        let snapshot_rejoin_after_compacted_log_present =
            rustraft_snapshot_evidence.rejoin_after_compacted_log_present;
        let (
            wal_segment_count,
            wal_active_segment_id,
            wal_first_retained_segment_id,
            wal_last_retained_segment_id,
            wal_total_bytes,
            wal_active_segment_bytes,
            wal_total_records,
            wal_first_sequence,
            wal_last_sequence,
            wal_first_log_index,
            wal_last_log_index,
            wal_released_segment_count,
            wal_slow_fsync_backpressure_observed,
        ) = self
            .wal
            .as_ref()
            .and_then(|wal| wal.segment_report(self.shard_id, self.leader_id).ok())
            .map(|report| {
                let first = report
                    .segments
                    .first()
                    .map(|segment| segment.segment_id)
                    .unwrap_or_default();
                let last = report
                    .segments
                    .last()
                    .map(|segment| segment.segment_id)
                    .unwrap_or_default();
                let total_bytes = report.segments.iter().map(|segment| segment.bytes).sum();
                let active_bytes = report
                    .segments
                    .iter()
                    .find(|segment| segment.segment_id == report.active_segment_id)
                    .map(|segment| segment.bytes)
                    .unwrap_or_default();
                let total_records = report
                    .segments
                    .iter()
                    .map(|segment| segment.record_count)
                    .sum();
                let first_sequence = report
                    .segments
                    .iter()
                    .find_map(|segment| {
                        (segment.first_sequence > 0).then_some(segment.first_sequence)
                    })
                    .unwrap_or_default();
                let last_sequence = report
                    .segments
                    .iter()
                    .rev()
                    .find_map(|segment| {
                        (segment.last_sequence > 0).then_some(segment.last_sequence)
                    })
                    .unwrap_or_default();
                (
                    report.segments.len() as u64,
                    report.active_segment_id,
                    first,
                    last,
                    total_bytes,
                    active_bytes,
                    total_records,
                    first_sequence,
                    last_sequence,
                    report.first_retained_log_index,
                    report.last_retained_log_index,
                    report.released_segment_count,
                    report.slow_fsync_backpressure_observed,
                )
            })
            .unwrap_or((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false));
        let rustraft_wal_evidence = rustraft_wal_lifecycle_evidence(&RustRaftWalLifecycleStatus {
            segment_count: wal_segment_count,
            active_segment_id: wal_active_segment_id,
            first_retained_segment_id: wal_first_retained_segment_id,
            last_retained_segment_id: wal_last_retained_segment_id,
            total_bytes: wal_total_bytes,
            active_segment_bytes: wal_active_segment_bytes,
            total_records: wal_total_records,
            first_sequence: wal_first_sequence,
            last_sequence: wal_last_sequence,
            first_log_index: wal_first_log_index,
            last_log_index: wal_last_log_index,
            released_segment_count: wal_released_segment_count,
            slow_fsync_backpressure_observed: wal_slow_fsync_backpressure_observed,
        });
        let wal_segment_lifecycle_present = rustraft_wal_evidence.segment_lifecycle_present;
        let pre_vote_enforced = self.config.enable_pre_vote;
        let pre_vote_process_evidence_observed = self.config.enable_pre_vote
            && self.read_safety_state.pre_vote_requests > 0
            && (self.read_safety_state.pre_vote_accepted > 0
                || self.read_safety_state.pre_vote_rejected > 0)
            && peer_pipeline_states
                .iter()
                .any(|peer| peer.pre_vote_rejections > 0);
        let election_prohibition_observed = self.config.prohibits_election
            && peer_pipeline_states
                .iter()
                .any(|peer| peer.election_rejections > 0);
        let offline_timeout_observed = self.config.offline_timeout_tick > 0
            && peer_pipeline_states
                .iter()
                .any(|peer| peer.offline_timeout_reached || peer.offline_timeout_rejections > 0);
        let transfer_timeout_observed = self.config.transfer_timeout_tick > 0
            && peer_pipeline_states
                .iter()
                .any(|peer| peer.transfer_leader_timeouts > 0);
        let election_controls_enforced = pre_vote_process_evidence_observed
            && election_prohibition_observed
            && offline_timeout_observed
            && transfer_timeout_observed;
        let quorum_peer_ids = peer_pipeline_states
            .iter()
            .filter(|peer| peer.replica_role.participates_in_quorum())
            .map(|peer| peer.peer_id)
            .collect::<Vec<_>>();
        let admin_status_surface_evidence =
            rustraft_admin_status_surface_evidence(&RustRaftAdminStatusSurfaceInput {
                commit_index: status.commit_index,
                max_observed_node_commit_index: status
                    .nodes
                    .iter()
                    .map(|node| node.commit_index)
                    .max()
                    .unwrap_or_default(),
                quorum_size: status.majority as u64,
                quorum_peer_ids,
                peer_pipeline: rustraft_peer_pipeline_states.clone(),
                wal_last_log_index,
                wal_segment_lifecycle_present,
            });
        let quorum_peer_progress_observed =
            admin_status_surface_evidence.quorum_peer_progress_observed;
        let peer_pipeline_runtime_activity_observed =
            admin_status_surface_evidence.peer_pipeline_runtime_activity_observed;
        let peer_pipeline_limits_observed =
            admin_status_surface_evidence.peer_pipeline_limits_observed;
        let admin_status_surface_complete = admin_status_surface_evidence.complete;
        let per_peer_pipeline_state_present =
            rustraft_pipeline_evidence.per_peer_pipeline_state_present;
        let capability_matrix = vec![
            ByteRaftCapabilityEvidence {
                capability: "per_peer_replication_pipeline_state".to_string(),
                ready: per_peer_pipeline_state_present
                    && append_backpressure_enforced
                    && apply_backpressure_enforced
                    && memory_replicate_bytes_enforced
                    && oversized_log_rejection_present,
                evidence_field: "peer_pipeline_states[*].{match_index,next_index,inflight_bytes,inflight_bytes_limit,append_queue_depth,append_queue_limit,append_queue_max_depth,append_*,apply_inflight_limit,apply_queue_depth,apply_queue_max_depth,apply_batch_bytes_limit,apply_backpressure_rejections,memory_backpressure_rejections,oversized_log_rejections}".to_string(),
                detail: format!(
                    "{} peers reported; append_backpressure={append_backpressure_enforced}; apply_backpressure={apply_backpressure_enforced}; memory_bytes={memory_replicate_bytes_enforced}; oversized={oversized_log_rejection_present}",
                    peer_pipeline_states.len()
                ),
            },
            ByteRaftCapabilityEvidence {
                capability: "reorder_queue_runtime".to_string(),
                ready: reorder_queue_enabled
                    && out_of_order_append_handling_present
                    && reorder_timeout_drop_present
                    && stale_term_rejection_present
                    && peer_pipeline_states
                        .iter()
                        .all(|peer| peer.reorder_queue_depth <= self.config.reorder_window_size),
                evidence_field:
                    "peer_pipeline_states[*].{reorder_queue_depth,out_of_order_append_rejections,reorder_entries_*,reorder_entry_timeouts,reorder_dropped_packages,stale_term_rejections}".to_string(),
                detail: format!(
                    "enabled={reorder_queue_enabled}; out_of_order={out_of_order_append_handling_present}; timeout_drop={reorder_timeout_drop_present}; stale_term={stale_term_rejection_present}; window={}; timeout_us={}",
                    self.config.reorder_window_size,
                    self.config.reorder_timeout_us
                ),
            },
            ByteRaftCapabilityEvidence {
                capability: "snapshot_sender_downloader_lifecycle".to_string(),
                ready: snapshot_sender_lifecycle_present
                    && snapshot_downloader_lifecycle_present
                    && snapshot_retry_backpressure_present
                    && snapshot_chunk_retry_present
                    && snapshot_send_timeout_present
                    && snapshot_rate_limit_present
                    && snapshot_install_progress_present
                    && snapshot_install_rollback_present
                    && snapshot_membership_change_present
                    && snapshot_rejoin_after_compacted_log_present,
                evidence_field: "peer_pipeline_states[*].{snapshot_sending,snapshot_installing,snapshot_progress,snapshot_retry,snapshot_chunk_retry,snapshot_send_timeout,snapshot_rate_limit,snapshot_rollback,snapshot_membership_change,snapshot_rejoin_after_compacted_log}".to_string(),
                detail: format!(
                    "sender={snapshot_sender_lifecycle_present}; downloader={snapshot_downloader_lifecycle_present}; retry_backpressure={snapshot_retry_backpressure_present}; chunk_retry={snapshot_chunk_retry_present}; send_timeout={snapshot_send_timeout_present}; rate_limit={snapshot_rate_limit_present}; progress={snapshot_install_progress_present}; rollback={snapshot_install_rollback_present}; membership={snapshot_membership_change_present}; rejoin_compacted={snapshot_rejoin_after_compacted_log_present}"
                ),
            },
            ByteRaftCapabilityEvidence {
                capability: "lease_read_index_pre_vote_semantics".to_string(),
                ready: read_index_validated
                    && lease_read_validated
                    && stale_follower_read_rejected
                    && stale_follower_write_rejected
                    && stale_leader_lease_rejected
                    && lagging_follower_read_rejected
                    && bounded_stale_read_accepted
                    && bounded_stale_read_rejected
                    && minority_partition_rejected_reads
                    && minority_partition_rejected_writes
                    && healed_follower_caught_up
                    && pre_vote_enforced
                    && pre_vote_process_evidence_observed,
                evidence_field: "read_index_*; lease_read_*; stale_leader_lease_rejection_count; lagging_follower_read_rejection_count; bounded_stale_read_*; minority_partition_*_rejection_count; stale_follower_write_rejection_count; healed_follower_catchup_count; pre_vote_*; peer_pipeline_states[*].pre_vote_rejections"
                    .to_string(),
                detail: format!(
                    "read_index={read_index_validated}; lease={lease_read_validated}; stale_lease={stale_leader_lease_rejected}; lagging_read={lagging_follower_read_rejected}; bounded_accept={bounded_stale_read_accepted}; bounded_reject={bounded_stale_read_rejected}; stale_read_rejected={stale_follower_read_rejected}; stale_write_rejected={stale_follower_write_rejected}; minority_read_rejected={minority_partition_rejected_reads}; minority_write_rejected={minority_partition_rejected_writes}; healed_catchup={healed_follower_caught_up}; pre_vote={pre_vote_enforced}; pre_vote_observed={pre_vote_process_evidence_observed}"
                ),
            },
            ByteRaftCapabilityEvidence {
                capability: "pre_vote_election_transfer_controls".to_string(),
                ready: election_controls_enforced,
                evidence_field: "pre_vote_process_evidence_observed; election_prohibition_observed; offline_timeout_observed; transfer_timeout_observed; peer_pipeline_states[*].{pre_vote_rejections,election_rejections,offline_timeout_*,transfer_leader_timeouts}".to_string(),
                detail: format!(
                    "pre_vote_observed={pre_vote_process_evidence_observed}; election_prohibited={election_prohibition_observed}; offline_timeout={offline_timeout_observed}; transfer_timeout={transfer_timeout_observed}"
                ),
            },
            ByteRaftCapabilityEvidence {
                capability: "wal_segment_lifecycle".to_string(),
                ready: wal_segment_lifecycle_present,
                evidence_field: "wal_{segment_count,active_segment_id,first_retained_segment_id,last_retained_segment_id,total_bytes,total_records,first_sequence,last_sequence,first_log_index,last_log_index,released_segment_count,slow_fsync_backpressure_observed}".to_string(),
                detail: format!(
                    "segments={wal_segment_count}; bytes={wal_total_bytes}; records={wal_total_records}; seq={wal_first_sequence}..{wal_last_sequence}; log_index={wal_first_log_index}..{wal_last_log_index}; released={wal_released_segment_count}; slow_fsync={wal_slow_fsync_backpressure_observed}"
                ),
            },
            ByteRaftCapabilityEvidence {
                capability: "admin_status_surface".to_string(),
                ready: admin_status_surface_complete,
                evidence_field: "admin_status_surface_complete; quorum_peer_progress_observed; peer_pipeline_runtime_activity_observed; peer_pipeline_limits_observed; /raft/control/byteraft_runtime_admin; prometheus byteraft metrics".to_string(),
                detail: format!(
                    "majority={}; commit_index={}; peer_rows={}; quorum_progress={quorum_peer_progress_observed}; runtime_activity={peer_pipeline_runtime_activity_observed}; limits={peer_pipeline_limits_observed}",
                    status.majority,
                    status.commit_index,
                    peer_pipeline_states.len()
                ),
            },
            ByteRaftCapabilityEvidence {
                capability: "membership_role_semantics".to_string(),
                ready: witness_membership_present
                    && witness_role_behavior_present
                    && learner_add_present
                    && learner_catchup_present
                    && learner_promote_present
                    && voter_remove_present
                    && learner_auto_promote_present
                    && leader_transfer_exact_once_present
                    && pending_joint_consensus_present
                    && pending_joint_consensus_restart_present,
                evidence_field: "membership_evidence.{learner_add_count,learner_catchup_count,learner_promote_count,voter_remove_count,witness_add_count,auto_promote_count,leader_transfer_write_count,leader_transfer_exact_once_commit_count,leader_transfer_exact_once_commit_ids,pending_joint_consensus_persist_count,pending_joint_consensus_restore_count}; witness_membership_present; witness_role_behavior_present; learner_auto_promote_present; pending_joint_consensus_present".to_string(),
                detail: format!(
                    "learner_add={learner_add_present}; catchup={learner_catchup_present}; promote={learner_promote_present}; remove={voter_remove_present}; witness={witness_membership_present}; witness_behavior={witness_role_behavior_present}; auto_promote={learner_auto_promote_present}; transfer_exact_once={leader_transfer_exact_once_present}; pending_joint_consensus={pending_joint_consensus_present}; restart={pending_joint_consensus_restart_present}"
                ),
            },
        ];

        let mut blockers = Vec::new();
        if !read_index_validated {
            blockers.push("read_index_not_validated".to_string());
        }
        if !lease_read_validated {
            blockers.push("lease_read_not_validated".to_string());
        }
        if !stale_follower_read_rejected {
            blockers.push("stale_follower_read_rejection_missing".to_string());
        }
        if !stale_follower_write_rejected {
            blockers.push("stale_follower_write_rejection_missing".to_string());
        }
        if !stale_leader_lease_rejected {
            blockers.push("stale_leader_lease_rejection_missing".to_string());
        }
        if !lagging_follower_read_rejected {
            blockers.push("lagging_follower_read_rejection_missing".to_string());
        }
        if !bounded_stale_read_accepted {
            blockers.push("bounded_stale_read_acceptance_missing".to_string());
        }
        if !bounded_stale_read_rejected {
            blockers.push("bounded_stale_read_rejection_missing".to_string());
        }
        if !minority_partition_rejected_reads {
            blockers.push("minority_partition_read_rejection_missing".to_string());
        }
        if !minority_partition_rejected_writes {
            blockers.push("minority_partition_write_rejection_missing".to_string());
        }
        if !healed_follower_caught_up {
            blockers.push("healed_follower_catchup_missing".to_string());
        }
        if !append_backpressure_enforced {
            blockers.push("append_backpressure_not_enforced".to_string());
        }
        if !apply_backpressure_enforced {
            blockers.push("apply_backpressure_not_enforced".to_string());
        }
        if !memory_replicate_bytes_enforced {
            blockers.push("memory_replicate_bytes_not_enforced".to_string());
        }
        if !oversized_log_rejection_present {
            blockers.push("oversized_log_rejection_missing".to_string());
        }
        if !reorder_queue_enabled {
            blockers.push("reorder_queue_not_enabled".to_string());
        }
        if !out_of_order_append_handling_present {
            blockers.push("out_of_order_append_handling_missing".to_string());
        }
        if !reorder_timeout_drop_present {
            blockers.push("reorder_timeout_drop_missing".to_string());
        }
        if !stale_term_rejection_present {
            blockers.push("stale_term_rejection_missing".to_string());
        }
        if !snapshot_sender_lifecycle_present {
            blockers.push("snapshot_sender_lifecycle_missing".to_string());
        }
        if !snapshot_downloader_lifecycle_present {
            blockers.push("snapshot_downloader_lifecycle_missing".to_string());
        }
        if !snapshot_retry_backpressure_present {
            blockers.push("snapshot_retry_backpressure_missing".to_string());
        }
        if !snapshot_chunk_retry_present {
            blockers.push("snapshot_chunk_retry_missing".to_string());
        }
        if !snapshot_send_timeout_present {
            blockers.push("snapshot_send_timeout_missing".to_string());
        }
        if !snapshot_rate_limit_present {
            blockers.push("snapshot_rate_limit_missing".to_string());
        }
        if !snapshot_install_progress_present {
            blockers.push("snapshot_install_progress_missing".to_string());
        }
        if !snapshot_install_rollback_present {
            blockers.push("snapshot_install_rollback_missing".to_string());
        }
        if !snapshot_membership_change_present {
            blockers.push("snapshot_membership_change_missing".to_string());
        }
        if !snapshot_rejoin_after_compacted_log_present {
            blockers.push("snapshot_rejoin_after_compacted_log_missing".to_string());
        }
        if !wal_segment_lifecycle_present {
            blockers.push("wal_segment_lifecycle_missing".to_string());
        }
        if !witness_membership_present {
            blockers.push("witness_membership_missing".to_string());
        }
        if !witness_role_behavior_present {
            blockers.push("witness_role_behavior_missing".to_string());
        }
        if !learner_add_present {
            blockers.push("learner_add_evidence_missing".to_string());
        }
        if !learner_catchup_present {
            blockers.push("learner_catchup_evidence_missing".to_string());
        }
        if !learner_promote_present {
            blockers.push("learner_promote_evidence_missing".to_string());
        }
        if !voter_remove_present {
            blockers.push("voter_remove_evidence_missing".to_string());
        }
        if !learner_auto_promote_present {
            blockers.push("learner_auto_promote_missing".to_string());
        }
        if !leader_transfer_exact_once_present {
            blockers.push("leader_transfer_exact_once_evidence_missing".to_string());
        }
        if !pending_joint_consensus_present {
            blockers.push("pending_joint_consensus_evidence_missing".to_string());
        }
        if !pending_joint_consensus_restart_present {
            blockers.push("pending_joint_consensus_restart_evidence_missing".to_string());
        }
        if !pre_vote_enforced {
            blockers.push("pre_vote_not_enforced".to_string());
        }
        if !pre_vote_process_evidence_observed {
            blockers.push("pre_vote_process_evidence_missing".to_string());
        }
        if !election_prohibition_observed {
            blockers.push("election_prohibition_evidence_missing".to_string());
        }
        if !offline_timeout_observed {
            blockers.push("offline_timeout_evidence_missing".to_string());
        }
        if !transfer_timeout_observed {
            blockers.push("transfer_timeout_evidence_missing".to_string());
        }
        if !election_controls_enforced {
            blockers.push("election_controls_not_enforced".to_string());
        }
        if !admin_status_surface_complete {
            blockers.push("admin_status_surface_incomplete".to_string());
        }
        if !quorum_peer_progress_observed {
            blockers.push("quorum_peer_progress_evidence_missing".to_string());
        }
        if !peer_pipeline_runtime_activity_observed {
            blockers.push("peer_pipeline_runtime_activity_missing".to_string());
        }
        if !peer_pipeline_limits_observed {
            blockers.push("peer_pipeline_limits_missing".to_string());
        }

        ByteRaftRuntimeAdminReport {
            shard_id: self.shard_id,
            leader_id: self.leader_id,
            commit_index: status.commit_index,
            leader_lease_valid: status.leader_lease_valid,
            read_index_validated,
            lease_read_validated,
            stale_follower_read_rejected,
            stale_follower_write_rejected,
            stale_leader_lease_rejected,
            lagging_follower_read_rejected,
            bounded_stale_read_accepted,
            bounded_stale_read_rejected,
            minority_partition_rejected_reads,
            minority_partition_rejected_writes,
            healed_follower_caught_up,
            witness_membership_present,
            witness_role_behavior_present,
            learner_auto_promote_present,
            pending_joint_consensus_present,
            learner_add_present,
            learner_catchup_present,
            learner_promote_present,
            voter_remove_present,
            leader_transfer_exact_once_present,
            pending_joint_consensus_restart_present,
            membership_evidence,
            peer_pipeline_states,
            append_backpressure_enforced,
            apply_backpressure_enforced,
            memory_replicate_bytes_enforced,
            oversized_log_rejection_present,
            out_of_order_append_handling_present,
            reorder_timeout_drop_present,
            stale_term_rejection_present,
            reorder_queue_enabled,
            snapshot_sender_lifecycle_present,
            snapshot_downloader_lifecycle_present,
            snapshot_retry_backpressure_present,
            snapshot_chunk_retry_present,
            snapshot_send_timeout_present,
            snapshot_rate_limit_present,
            snapshot_install_progress_present,
            snapshot_install_rollback_present,
            snapshot_membership_change_present,
            snapshot_rejoin_after_compacted_log_present,
            wal_segment_lifecycle_present,
            wal_segment_count,
            wal_active_segment_id,
            wal_first_retained_segment_id,
            wal_last_retained_segment_id,
            wal_total_bytes,
            wal_active_segment_bytes,
            wal_total_records,
            wal_first_sequence,
            wal_last_sequence,
            wal_first_log_index,
            wal_last_log_index,
            wal_released_segment_count,
            wal_slow_fsync_backpressure_observed,
            pre_vote_enforced,
            election_controls_enforced,
            pre_vote_process_evidence_observed,
            election_prohibition_observed,
            offline_timeout_observed,
            transfer_timeout_observed,
            read_index_requests: self.read_safety_state.read_index_requests,
            read_index_accepted: self.read_safety_state.read_index_accepted,
            read_index_rejected: self.read_safety_state.read_index_rejected,
            lease_read_requests: self.read_safety_state.lease_read_requests,
            lease_read_accepted: self.read_safety_state.lease_read_accepted,
            lease_read_rejected: self.read_safety_state.lease_read_rejected,
            stale_leader_lease_rejection_count: self.read_safety_state.stale_leader_lease_rejected,
            lagging_follower_read_rejection_count: self
                .read_safety_state
                .lagging_follower_read_rejected,
            bounded_stale_read_requests: self.read_safety_state.bounded_stale_read_requests,
            bounded_stale_read_accepted_count: self.read_safety_state.bounded_stale_read_accepted,
            bounded_stale_read_rejected_count: self.read_safety_state.bounded_stale_read_rejected,
            minority_partition_read_rejection_count: self
                .read_safety_state
                .minority_partition_read_rejected,
            minority_partition_write_rejection_count: self
                .read_safety_state
                .minority_partition_write_rejected,
            stale_follower_write_rejection_count: self
                .read_safety_state
                .stale_follower_write_rejected,
            healed_follower_catchup_count: self.read_safety_state.healed_follower_catchup_observed,
            pre_vote_requests: self.read_safety_state.pre_vote_requests,
            pre_vote_accepted: self.read_safety_state.pre_vote_accepted,
            pre_vote_rejected: self.read_safety_state.pre_vote_rejected,
            quorum_peer_progress_observed,
            peer_pipeline_runtime_activity_observed,
            peer_pipeline_limits_observed,
            admin_status_surface_complete,
            capability_matrix,
            ready: blockers.is_empty(),
            blockers,
        }
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
    config: &RaftConfig,
) {
    let Some(leader) = nodes.get(&leader_id) else {
        return;
    };
    let leader_log = leader.log.clone();
    let leader_commit_index = leader.commit_index;
    for node in nodes.values_mut() {
        refresh_node_pipeline_state(node, leader_id, leader_commit_index, &leader_log, config);
    }
}

fn refresh_node_pipeline_state(
    node: &mut RaftNode,
    leader_id: RaftNodeId,
    leader_commit_index: u64,
    leader_log: &[RaftLogEntry],
    config: &RaftConfig,
) {
    let inflight_entries = leader_commit_index.saturating_sub(node.commit_index);
    let inflight_bytes = leader_log
        .iter()
        .filter(|entry| entry.index > node.commit_index)
        .map(|entry| command_size_bytes(&entry.command))
        .sum();
    let snapshot_installed_index = node
        .installed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.last_included_index)
        .unwrap_or_default();
    node.pipeline_state.match_index = node.commit_index;
    node.pipeline_state.next_index = node_next_log_index(node);
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
    node.pipeline_state.transfer_leader_target =
        node.id == leader_id && node.role == RaftRole::Leader;
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
    if node.log.last().map(|last| last.index) >= Some(entry.index) {
        node.log.retain(|existing| existing.index < entry.index);
        node.applied.retain(|applied| *applied < entry.index);
        node.applied_index = node.applied.iter().next_back().copied().unwrap_or_default();
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

fn apply_committed(node: &mut RaftNode) -> Option<CommandResponse> {
    let mut last_response = None;
    let start = node
        .log
        .binary_search_by_key(&node.applied_index.saturating_add(1), |entry| entry.index)
        .unwrap_or_else(|position| position);
    for entry in node.log[start..]
        .iter()
        .take_while(|entry| entry.index <= node.commit_index)
    {
        if node.applied.insert(entry.index) {
            let response = node
                .engine
                .execute_durable(ExecuteRequest {
                    shard_id: entry.shard_id,
                    command: entry.command.clone(),
                })
                .response;
            node.applied_index = entry.index;
            last_response = Some(response);
        }
    }
    last_response
}

fn install_snapshot_state(node: &mut RaftNode, snapshot: RaftSnapshot) {
    let engine = TemporalEngine::default();
    engine.load_shard(snapshot.shard_id);
    for entry in &snapshot.entries {
        engine.execute_durable(ExecuteRequest {
            shard_id: entry.shard_id,
            command: entry.command.clone(),
        });
    }
    node.engine = engine;
    node.current_term = node.current_term.max(snapshot.last_included_term);
    node.commit_index = node.commit_index.max(snapshot.last_included_index);
    node.log
        .retain(|entry| entry.index > snapshot.last_included_index);
    node.applied.clear();
    node.applied
        .extend(snapshot.entries.iter().map(|entry| entry.index));
    node.applied_index = snapshot.last_included_index;
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

fn append_byteraft_runtime_admin_prometheus(
    out: &mut String,
    kind: &str,
    report: ByteRaftRuntimeAdminReport,
) {
    let rustraft_capability_report = rustraft_capability_report_from_byteraft_admin(&report);
    let rustraft_metrics = ::rustraft::rustraft_reference_raft_runtime_capability_prometheus(
        &rustraft_capability_report,
        &[("kind", kind)],
    );
    out.push_str(&rustraft_metrics.text);

    out.push_str("# HELP temporalstore_raft_byteraft_ready Whether ByteRaft-style production runtime evidence is complete.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_ready gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_ready",
        &[("kind", kind.to_string())],
        u64::from(report.ready),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_quorum_peer_progress_observed Whether admin readiness observed quorum peer match/next progress from runtime state.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_quorum_peer_progress_observed gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_quorum_peer_progress_observed",
        &[("kind", kind.to_string())],
        u64::from(report.quorum_peer_progress_observed),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_peer_pipeline_runtime_activity_observed Whether per-peer pipeline state has non-vacuous runtime activity.\n");
    out.push_str(
        "# TYPE temporalstore_raft_byteraft_peer_pipeline_runtime_activity_observed gauge\n",
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_peer_pipeline_runtime_activity_observed",
        &[("kind", kind.to_string())],
        u64::from(report.peer_pipeline_runtime_activity_observed),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_peer_pipeline_limits_observed Whether per-peer pipeline limits are populated for admin readiness.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_pipeline_limits_observed gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_peer_pipeline_limits_observed",
        &[("kind", kind.to_string())],
        u64::from(report.peer_pipeline_limits_observed),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_capability_ready ByteRaft-style capability readiness matrix.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_capability_ready gauge\n");
    for capability in &report.capability_matrix {
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_capability_ready",
            &[
                ("kind", kind.to_string()),
                ("capability", capability.capability.clone()),
                ("evidence_field", capability.evidence_field.clone()),
            ],
            u64::from(capability.ready),
        );
    }
    out.push_str("# HELP temporalstore_raft_byteraft_read_index_validated Whether read-index evidence was validated.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_read_index_validated gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_read_index_validated",
        &[("kind", kind.to_string())],
        u64::from(report.read_index_validated),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_lease_read_validated Whether lease-read evidence was validated.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_lease_read_validated gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_lease_read_validated",
        &[("kind", kind.to_string())],
        u64::from(report.lease_read_validated),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_stale_follower_read_rejected Whether stale follower reads are rejected.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_stale_follower_read_rejected gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_stale_follower_read_rejected",
        &[("kind", kind.to_string())],
        u64::from(report.stale_follower_read_rejected),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_stale_follower_write_rejected Whether stale follower writes are rejected.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_stale_follower_write_rejected gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_stale_follower_write_rejected",
        &[("kind", kind.to_string())],
        u64::from(report.stale_follower_write_rejected),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_witness_membership_present",
        &[("kind", kind.to_string())],
        u64::from(report.witness_membership_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_witness_role_behavior_present",
        &[("kind", kind.to_string())],
        u64::from(report.witness_role_behavior_present),
    );
    for (name, value) in [
        (
            "temporalstore_raft_byteraft_learner_add_present",
            report.learner_add_present,
        ),
        (
            "temporalstore_raft_byteraft_learner_catchup_present",
            report.learner_catchup_present,
        ),
        (
            "temporalstore_raft_byteraft_learner_promote_present",
            report.learner_promote_present,
        ),
        (
            "temporalstore_raft_byteraft_voter_remove_present",
            report.voter_remove_present,
        ),
        (
            "temporalstore_raft_byteraft_leader_transfer_exact_once_present",
            report.leader_transfer_exact_once_present,
        ),
        (
            "temporalstore_raft_byteraft_pending_joint_consensus_restart_present",
            report.pending_joint_consensus_restart_present,
        ),
    ] {
        push_raft_metric(out, name, &[("kind", kind.to_string())], u64::from(value));
    }
    for (name, value) in [
        (
            "temporalstore_raft_byteraft_membership_learner_add_count",
            report.membership_evidence.learner_add_count,
        ),
        (
            "temporalstore_raft_byteraft_membership_learner_catchup_count",
            report.membership_evidence.learner_catchup_count,
        ),
        (
            "temporalstore_raft_byteraft_membership_learner_promote_count",
            report.membership_evidence.learner_promote_count,
        ),
        (
            "temporalstore_raft_byteraft_membership_voter_remove_count",
            report.membership_evidence.voter_remove_count,
        ),
        (
            "temporalstore_raft_byteraft_membership_leader_transfer_write_count",
            report.membership_evidence.leader_transfer_write_count,
        ),
        (
            "temporalstore_raft_byteraft_membership_leader_transfer_exact_once_commit_count",
            report
                .membership_evidence
                .leader_transfer_exact_once_commit_count,
        ),
        (
            "temporalstore_raft_byteraft_membership_leader_transfer_exact_once_commit_id_count",
            report
                .membership_evidence
                .leader_transfer_exact_once_commit_ids
                .len() as u64,
        ),
        (
            "temporalstore_raft_byteraft_membership_pending_joint_consensus_restore_count",
            report
                .membership_evidence
                .pending_joint_consensus_restore_count,
        ),
    ] {
        push_raft_metric(out, name, &[("kind", kind.to_string())], value);
    }
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_learner_auto_promote_present",
        &[("kind", kind.to_string())],
        u64::from(report.learner_auto_promote_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_pending_joint_consensus_present",
        &[("kind", kind.to_string())],
        u64::from(report.pending_joint_consensus_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_append_backpressure_enforced Whether append pipeline backpressure evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_append_backpressure_enforced gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_append_backpressure_enforced",
        &[("kind", kind.to_string())],
        u64::from(report.append_backpressure_enforced),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_apply_backpressure_enforced",
        &[("kind", kind.to_string())],
        u64::from(report.apply_backpressure_enforced),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_memory_replicate_bytes_enforced",
        &[("kind", kind.to_string())],
        u64::from(report.memory_replicate_bytes_enforced),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_oversized_log_rejection_present",
        &[("kind", kind.to_string())],
        u64::from(report.oversized_log_rejection_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_out_of_order_append_handling_present",
        &[("kind", kind.to_string())],
        u64::from(report.out_of_order_append_handling_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_reorder_queue_enabled Whether per-peer reorder queue evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_reorder_queue_enabled gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_reorder_queue_enabled",
        &[("kind", kind.to_string())],
        u64::from(report.reorder_queue_enabled),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_snapshot_sender_lifecycle_present Whether snapshot sender lifecycle evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_snapshot_sender_lifecycle_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_sender_lifecycle_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_sender_lifecycle_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_snapshot_downloader_lifecycle_present Whether snapshot downloader lifecycle evidence is present.\n");
    out.push_str(
        "# TYPE temporalstore_raft_byteraft_snapshot_downloader_lifecycle_present gauge\n",
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_downloader_lifecycle_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_downloader_lifecycle_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_snapshot_retry_backpressure_present Whether snapshot retry/backpressure evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_snapshot_retry_backpressure_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_retry_backpressure_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_retry_backpressure_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_snapshot_chunk_retry_present Whether snapshot chunk retry evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_snapshot_chunk_retry_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_chunk_retry_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_chunk_retry_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_snapshot_send_timeout_present Whether snapshot send timeout evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_snapshot_send_timeout_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_send_timeout_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_send_timeout_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_rate_limit_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_rate_limit_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_install_progress_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_install_progress_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_install_rollback_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_install_rollback_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_membership_change_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_membership_change_present),
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_snapshot_rejoin_after_compacted_log_present",
        &[("kind", kind.to_string())],
        u64::from(report.snapshot_rejoin_after_compacted_log_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_wal_segment_lifecycle_present Whether WAL segment lifecycle evidence is present.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_segment_lifecycle_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_segment_lifecycle_present",
        &[("kind", kind.to_string())],
        u64::from(report.wal_segment_lifecycle_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_wal_segment_count WAL segments retained by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_segment_count gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_segment_count",
        &[("kind", kind.to_string())],
        report.wal_segment_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_wal_total_bytes WAL bytes retained by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_total_bytes gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_total_bytes",
        &[("kind", kind.to_string())],
        report.wal_total_bytes,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_wal_active_segment_bytes Active WAL segment bytes retained by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_active_segment_bytes gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_active_segment_bytes",
        &[("kind", kind.to_string())],
        report.wal_active_segment_bytes,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_wal_total_records WAL records retained by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_total_records gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_total_records",
        &[("kind", kind.to_string())],
        report.wal_total_records,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_wal_first_sequence First retained WAL record sequence.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_first_sequence gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_first_sequence",
        &[("kind", kind.to_string())],
        report.wal_first_sequence,
    );
    out.push_str(
        "# HELP temporalstore_raft_byteraft_wal_last_sequence Last retained WAL record sequence.\n",
    );
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_last_sequence gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_last_sequence",
        &[("kind", kind.to_string())],
        report.wal_last_sequence,
    );
    out.push_str(
        "# HELP temporalstore_raft_byteraft_wal_first_log_index First retained WAL log index.\n",
    );
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_first_log_index gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_first_log_index",
        &[("kind", kind.to_string())],
        report.wal_first_log_index,
    );
    out.push_str(
        "# HELP temporalstore_raft_byteraft_wal_last_log_index Last retained WAL log index.\n",
    );
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_last_log_index gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_last_log_index",
        &[("kind", kind.to_string())],
        report.wal_last_log_index,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_wal_released_segment_count WAL segments released by the last segmented append.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_released_segment_count gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_released_segment_count",
        &[("kind", kind.to_string())],
        report.wal_released_segment_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_wal_slow_fsync_backpressure_observed Whether slow fsync backpressure evidence was observed.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_wal_slow_fsync_backpressure_observed gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_wal_slow_fsync_backpressure_observed",
        &[("kind", kind.to_string())],
        u64::from(report.wal_slow_fsync_backpressure_observed),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_read_index_requests Read-index requests observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_read_index_requests counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_read_index_requests",
        &[("kind", kind.to_string())],
        report.read_index_requests,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_read_index_accepted Read-index requests accepted by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_read_index_accepted counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_read_index_accepted",
        &[("kind", kind.to_string())],
        report.read_index_accepted,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_read_index_rejected Read-index requests rejected by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_read_index_rejected counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_read_index_rejected",
        &[("kind", kind.to_string())],
        report.read_index_rejected,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_lease_read_requests Lease-read requests observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_lease_read_requests counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_lease_read_requests",
        &[("kind", kind.to_string())],
        report.lease_read_requests,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_lease_read_accepted Lease-read requests accepted by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_lease_read_accepted counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_lease_read_accepted",
        &[("kind", kind.to_string())],
        report.lease_read_accepted,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_lease_read_rejected Lease-read requests rejected by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_lease_read_rejected counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_lease_read_rejected",
        &[("kind", kind.to_string())],
        report.lease_read_rejected,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_stale_leader_lease_rejections Stale leader lease read rejections observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_stale_leader_lease_rejections counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_stale_leader_lease_rejections",
        &[("kind", kind.to_string())],
        report.stale_leader_lease_rejection_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_lagging_follower_read_rejections Lagging follower read rejections observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_lagging_follower_read_rejections counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_lagging_follower_read_rejections",
        &[("kind", kind.to_string())],
        report.lagging_follower_read_rejection_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_bounded_stale_read_requests Bounded-stale read requests observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_bounded_stale_read_requests counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_bounded_stale_read_requests",
        &[("kind", kind.to_string())],
        report.bounded_stale_read_requests,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_bounded_stale_read_accepted Bounded-stale read requests accepted by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_bounded_stale_read_accepted counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_bounded_stale_read_accepted",
        &[("kind", kind.to_string())],
        report.bounded_stale_read_accepted_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_bounded_stale_read_rejected Bounded-stale read requests rejected by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_bounded_stale_read_rejected counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_bounded_stale_read_rejected",
        &[("kind", kind.to_string())],
        report.bounded_stale_read_rejected_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_minority_partition_read_rejections Minority-partition read rejections observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_minority_partition_read_rejections counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_minority_partition_read_rejections",
        &[("kind", kind.to_string())],
        report.minority_partition_read_rejection_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_minority_partition_write_rejections Minority-partition write rejections observed by the raft runtime.\n");
    out.push_str(
        "# TYPE temporalstore_raft_byteraft_minority_partition_write_rejections counter\n",
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_minority_partition_write_rejections",
        &[("kind", kind.to_string())],
        report.minority_partition_write_rejection_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_healed_follower_catchup Healed follower catch-up observations by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_healed_follower_catchup counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_healed_follower_catchup",
        &[("kind", kind.to_string())],
        report.healed_follower_catchup_count,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_pre_vote_requests Pre-vote attempts observed by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_pre_vote_requests counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_pre_vote_requests",
        &[("kind", kind.to_string())],
        report.pre_vote_requests,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_pre_vote_accepted Pre-vote attempts accepted by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_pre_vote_accepted counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_pre_vote_accepted",
        &[("kind", kind.to_string())],
        report.pre_vote_accepted,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_pre_vote_rejected Pre-vote attempts rejected by the raft runtime.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_pre_vote_rejected counter\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_pre_vote_rejected",
        &[("kind", kind.to_string())],
        report.pre_vote_rejected,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_peer_match_index ByteRaft-style per-peer match index.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_match_index gauge\n");
    out.push_str(
        "# HELP temporalstore_raft_byteraft_peer_next_index ByteRaft-style per-peer next index.\n",
    );
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_next_index gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_append_requests ByteRaft-style per-peer append requests.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_append_requests counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_append_accepted ByteRaft-style per-peer append requests accepted.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_append_accepted counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_append_rejected ByteRaft-style per-peer append requests rejected by pipeline backpressure.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_append_rejected counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_inflight_entries ByteRaft-style per-peer inflight entries.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_inflight_entries gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_inflight_bytes ByteRaft-style per-peer inflight bytes.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_inflight_bytes gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_append_queue_depth ByteRaft-style per-peer append queue depth.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_append_queue_depth gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_append_queue_limit ByteRaft-style per-peer append queue limit.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_append_queue_limit gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_inflight_bytes_limit ByteRaft-style per-peer inflight byte limit.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_inflight_bytes_limit gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_apply_inflight_limit ByteRaft-style per-peer apply inflight task limit.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_apply_inflight_limit gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_apply_queue_depth ByteRaft-style per-peer apply queue depth.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_apply_queue_depth gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_apply_queue_max_depth ByteRaft-style per-peer max observed apply queue depth.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_apply_queue_max_depth gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_apply_batch_bytes_limit ByteRaft-style per-peer apply batch byte limit.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_apply_batch_bytes_limit gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_reorder_queue_depth ByteRaft-style per-peer reorder queue depth.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_reorder_queue_depth gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_reorder_entries_accepted ByteRaft-style per-peer reorder entries accepted.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_reorder_entries_accepted counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_reorder_entries_released ByteRaft-style per-peer reorder entries released to apply.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_reorder_entries_released counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_reorder_entries_rejected ByteRaft-style per-peer reorder entries rejected by window overflow.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_reorder_entries_rejected counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_reorder_entry_timeouts ByteRaft-style per-peer reorder entry timeout count.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_reorder_entry_timeouts counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_reorder_dropped_packages ByteRaft-style per-peer dropped reordered append packages.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_reorder_dropped_packages counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_stale_term_rejections ByteRaft-style per-peer stale-term append rejections.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_stale_term_rejections counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_sending Whether a peer is sending a snapshot.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_sending gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_installing Whether a peer is installing a snapshot.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_installing gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_installed_index Last installed snapshot index per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_installed_index gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_send_attempts Snapshot send attempts per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_send_attempts counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_send_completed Snapshot sends completed per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_send_completed counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_send_failed Snapshot sends failed per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_send_failed counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_install_started Snapshot installs started per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_install_started counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_install_completed Snapshot installs completed per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_install_completed counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_install_rejected Snapshot installs rejected per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_install_rejected counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_install_rolled_back Snapshot install rollbacks per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_install_rolled_back counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_install_received_chunks Snapshot chunks received per peer.\n");
    out.push_str(
        "# TYPE temporalstore_raft_byteraft_peer_snapshot_install_received_chunks gauge\n",
    );
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_install_total_chunks Snapshot chunks expected per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_install_total_chunks gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_retry_count Snapshot retry/rejection count per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_retry_count counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_backpressure_rejections Snapshot backpressure rejection count per peer.\n");
    out.push_str(
        "# TYPE temporalstore_raft_byteraft_peer_snapshot_backpressure_rejections counter\n",
    );
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_send_elapsed_ms Snapshot sender elapsed time per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_send_elapsed_ms gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_snapshot_send_timeouts Snapshot sender timeout count per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_snapshot_send_timeouts counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_transfer_leader_requests Leader-transfer requests per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_transfer_leader_requests counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_transfer_leader_accepted Leader-transfer requests accepted per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_transfer_leader_accepted counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_transfer_leader_rejected Leader-transfer requests rejected per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_transfer_leader_rejected counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_transfer_leader_completed Leader-transfer completions per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_transfer_leader_completed counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_transfer_leader_elapsed_ms Pending leader-transfer elapsed time per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_transfer_leader_elapsed_ms gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_transfer_leader_timeouts Leader-transfer timeout count per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_transfer_leader_timeouts counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_offline_elapsed_ms ByteRaft-style offline elapsed time per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_offline_elapsed_ms gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_offline_timeout_reached Whether a peer has crossed the configured offline timeout.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_offline_timeout_reached gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_peer_offline_timeout_rejections Offline timeout transitions per peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_peer_offline_timeout_rejections counter\n");
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
            "temporalstore_raft_byteraft_peer_match_index",
            labels,
            peer.match_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_next_index",
            labels,
            peer.next_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_append_requests",
            labels,
            peer.append_requests,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_append_accepted",
            labels,
            peer.append_accepted,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_append_rejected",
            labels,
            peer.append_rejected,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_inflight_entries",
            labels,
            peer.inflight_entries,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_inflight_bytes",
            labels,
            peer.inflight_bytes,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_append_queue_depth",
            labels,
            peer.append_queue_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_append_queue_limit",
            labels,
            peer.append_queue_limit,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_inflight_bytes_limit",
            labels,
            peer.inflight_bytes_limit,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_append_queue_max_depth",
            labels,
            peer.append_queue_max_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_apply_inflight_tasks",
            labels,
            peer.apply_inflight_tasks,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_apply_inflight_limit",
            labels,
            peer.apply_inflight_limit,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_apply_queue_depth",
            labels,
            peer.apply_queue_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_apply_queue_max_depth",
            labels,
            peer.apply_queue_max_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_apply_batch_bytes_limit",
            labels,
            peer.apply_batch_bytes_limit,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_apply_backpressure_rejections",
            labels,
            peer.apply_backpressure_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_memory_backpressure_rejections",
            labels,
            peer.memory_backpressure_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_oversized_log_rejections",
            labels,
            peer.oversized_log_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_reorder_queue_depth",
            labels,
            peer.reorder_queue_depth,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_out_of_order_append_rejections",
            labels,
            peer.out_of_order_append_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_reorder_entries_accepted",
            labels,
            peer.reorder_entries_accepted,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_reorder_entries_released",
            labels,
            peer.reorder_entries_released,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_reorder_entries_rejected",
            labels,
            peer.reorder_entries_rejected,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_reorder_entry_timeouts",
            labels,
            peer.reorder_entry_timeouts,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_reorder_dropped_packages",
            labels,
            peer.reorder_dropped_packages,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_stale_term_rejections",
            labels,
            peer.stale_term_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_sending",
            labels,
            u64::from(peer.snapshot_sending),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_installing",
            labels,
            u64::from(peer.snapshot_installing),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_installed_index",
            labels,
            peer.snapshot_installed_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_send_attempts",
            labels,
            peer.snapshot_send_attempts,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_send_completed",
            labels,
            peer.snapshot_send_completed,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_send_failed",
            labels,
            peer.snapshot_send_failed,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_install_started",
            labels,
            peer.snapshot_install_started,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_install_completed",
            labels,
            peer.snapshot_install_completed,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_install_rejected",
            labels,
            peer.snapshot_install_rejected,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_install_rolled_back",
            labels,
            peer.snapshot_install_rolled_back,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_install_received_chunks",
            labels,
            peer.snapshot_install_received_chunks,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_install_total_chunks",
            labels,
            peer.snapshot_install_total_chunks,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_install_progress_per_mille",
            labels,
            peer.snapshot_install_progress_per_mille,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_retry_count",
            labels,
            peer.snapshot_retry_count,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_chunk_retry_count",
            labels,
            peer.snapshot_chunk_retry_count,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_backpressure_rejections",
            labels,
            peer.snapshot_backpressure_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_rate_limit_rejections",
            labels,
            peer.snapshot_rate_limit_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_during_membership_change",
            labels,
            u64::from(peer.snapshot_during_membership_change),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_rejoin_after_compacted_log",
            labels,
            u64::from(peer.snapshot_rejoin_after_compacted_log),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_auto_promoted_from_learner",
            labels,
            u64::from(peer.auto_promoted_from_learner),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_send_elapsed_ms",
            labels,
            peer.snapshot_send_elapsed_ms,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_snapshot_send_timeouts",
            labels,
            peer.snapshot_send_timeouts,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_transfer_leader_requests",
            labels,
            peer.transfer_leader_requests,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_transfer_leader_accepted",
            labels,
            peer.transfer_leader_accepted,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_transfer_leader_rejected",
            labels,
            peer.transfer_leader_rejected,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_transfer_leader_completed",
            labels,
            peer.transfer_leader_completed,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_transfer_leader_elapsed_ms",
            labels,
            peer.transfer_leader_elapsed_ms,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_transfer_leader_timeouts",
            labels,
            peer.transfer_leader_timeouts,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_offline_elapsed_ms",
            labels,
            peer.offline_elapsed_ms,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_offline_timeout_reached",
            labels,
            u64::from(peer.offline_timeout_reached),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_peer_offline_timeout_rejections",
            labels,
            peer.offline_timeout_rejections,
        );
    }
}

fn append_byteraft_local_status_prometheus(
    out: &mut String,
    kind: &str,
    report: ByteRaftLocalStatusReport,
) {
    out.push_str("# HELP temporalstore_raft_byteraft_local_pending_joint_consensus_present Whether local status has pending joint-consensus membership.\n");
    out.push_str(
        "# TYPE temporalstore_raft_byteraft_local_pending_joint_consensus_present gauge\n",
    );
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_local_pending_joint_consensus_present",
        &[("kind", kind.to_string())],
        u64::from(report.pending_joint_consensus.is_some()),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_local_witness_membership_present Whether local status has a witness member.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_witness_membership_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_local_witness_membership_present",
        &[("kind", kind.to_string())],
        u64::from(report.witness_membership_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_local_learner_membership_present Whether local status has a learner member.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_learner_membership_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_local_learner_membership_present",
        &[("kind", kind.to_string())],
        u64::from(report.learner_membership_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_local_learner_auto_promote_present Whether local status observed learner auto-promotion evidence.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_learner_auto_promote_present gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_local_learner_auto_promote_present",
        &[("kind", kind.to_string())],
        u64::from(report.learner_auto_promote_present),
    );
    out.push_str("# HELP temporalstore_raft_byteraft_local_wal_first_log_index First retained WAL log index visible in local status.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_wal_first_log_index gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_local_wal_first_log_index",
        &[("kind", kind.to_string())],
        report.wal_first_log_index,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_local_wal_last_log_index Last retained WAL log index visible in local status.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_wal_last_log_index gauge\n");
    push_raft_metric(
        out,
        "temporalstore_raft_byteraft_local_wal_last_log_index",
        &[("kind", kind.to_string())],
        report.wal_last_log_index,
    );
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_role ByteRaft-style local-status peer role as a labeled gauge.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_role gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_participates_in_quorum Whether a peer participates in quorum.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_participates_in_quorum gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_can_serve_data Whether a peer can serve data reads.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_can_serve_data gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_can_be_leader Whether a peer can become leader.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_can_be_leader gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_match_index ByteRaft-style local peer match index.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_match_index gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_next_index ByteRaft-style local peer next index.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_next_index gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_snapshot_sending Whether local status sees a snapshot sender active for the peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_snapshot_sending gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_snapshot_installing Whether local status sees a snapshot install active for the peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_snapshot_installing gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_snapshot_installed_index Last installed snapshot index for the peer.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_snapshot_installed_index gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_transfer_leader_target Whether the peer is the active leader-transfer target.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_transfer_leader_target gauge\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_pre_vote_rejections Pre-vote rejections visible in local status.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_pre_vote_rejections counter\n");
    out.push_str("# HELP temporalstore_raft_byteraft_local_peer_election_rejections Election rejections visible in local status.\n");
    out.push_str("# TYPE temporalstore_raft_byteraft_local_peer_election_rejections counter\n");
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
            "temporalstore_raft_byteraft_local_peer_role",
            labels,
            1,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_participates_in_quorum",
            labels,
            u64::from(peer.participates_in_quorum),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_can_serve_data",
            labels,
            u64::from(peer.can_serve_data),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_can_be_leader",
            labels,
            u64::from(peer.can_be_leader),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_match_index",
            labels,
            peer.pipeline_state.match_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_next_index",
            labels,
            peer.pipeline_state.next_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_snapshot_sending",
            labels,
            u64::from(peer.pipeline_state.snapshot_sending),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_snapshot_installing",
            labels,
            u64::from(peer.pipeline_state.snapshot_installing),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_snapshot_installed_index",
            labels,
            peer.pipeline_state.snapshot_installed_index,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_transfer_leader_target",
            labels,
            u64::from(peer.pipeline_state.transfer_leader_target),
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_pre_vote_rejections",
            labels,
            peer.pipeline_state.pre_vote_rejections,
        );
        push_raft_metric(
            out,
            "temporalstore_raft_byteraft_local_peer_election_rejections",
            labels,
            peer.pipeline_state.election_rejections,
        );
    }
}

fn rustraft_capability_report_from_byteraft_admin(
    report: &ByteRaftRuntimeAdminReport,
) -> ::rustraft::RustRaftReferenceRaftRuntimeCapabilityReport {
    let capability_evidence = report
        .capability_matrix
        .iter()
        .map(|capability| {
            ::rustraft::rustraft_capability_evidence_from_fields(
                capability.capability.clone(),
                "temporalstore_byteraft_runtime_admin_report",
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
        product_blockers.push("temporalstore:blocker:byteraft_admin_report_not_ready".to_string());
    }
    ::rustraft::rustraft_runtime_capability_report_from_evidence(
        capability_evidence,
        product_blockers,
    )
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

impl MetaRaftCluster {
    /// Local metaserver Raft fixture for unit tests and validation harnesses.
    ///
    /// Production metaserver Raft must use the networked production runtime.
    pub fn new(node_ids: impl IntoIterator<Item = RaftNodeId>) -> Self {
        Self::new_with_config(node_ids, RaftConfig::default())
            .expect("default raft config must be valid")
    }

    /// Local metaserver Raft fixture with explicit config for tests/harnesses.
    pub fn new_with_config(
        node_ids: impl IntoIterator<Item = RaftNodeId>,
        config: RaftConfig,
    ) -> Result<Self, RaftError> {
        config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        let mut nodes = BTreeMap::new();
        let mut iter = node_ids.into_iter();
        let leader_id = iter.next().unwrap_or(1);
        nodes.insert(leader_id, new_meta_node(leader_id, RaftRole::Leader));
        for node_id in iter {
            nodes.insert(node_id, new_meta_node(node_id, RaftRole::Follower));
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(MetaRaftClusterInner {
                leader_id,
                nodes,
                config,
            })),
        })
    }

    pub fn propose(&self, command: MetaCommand) -> Result<(), RaftError> {
        self.propose_inner(command).map(|_| ())
    }

    pub fn propose_mutation(&self, mutation: MetaMutation) -> Result<Status, RaftError> {
        Ok(self
            .propose_inner(MetaCommand::ApplyMutation(mutation))?
            .unwrap_or_else(Status::ok))
    }

    pub fn register(&self, request: RegisterShardRequest) -> RegisterShardResponse {
        RegisterShardResponse {
            status: self.mutation_status(MetaMutation::RegisterShard(request)),
        }
    }

    pub fn get(&self, shard_id: ShardId) -> GetShardResponse {
        self.read_meta().map_or_else(
            |status| GetShardResponse {
                status,
                location: None,
            },
            |meta| meta.get(shard_id),
        )
    }

    pub fn register_server(&self, request: RegisterServerRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::RegisterServer(request)),
        }
    }

    pub fn update_server(&self, request: UpdateServerRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UpdateServer(request)),
        }
    }

    pub fn server_heartbeat(&self, request: ServerHeartbeatRequest) -> ServerHeartbeatResponse {
        self.read_meta().map_or_else(
            |status| ServerHeartbeatResponse {
                status,
                forbid_auto_register: true,
                topology_version: 0,
                server_state: String::new(),
            },
            |meta| meta.server_heartbeat(request),
        )
    }

    pub fn register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::RegisterProxy(request)),
        }
    }

    pub fn proxy_heartbeat(&self, request: ProxyHeartbeatRequest) -> ProxyHeartbeatResponse {
        self.read_meta().map_or_else(
            |status| ProxyHeartbeatResponse {
                status,
                config_changed: false,
                namespace: String::new(),
                config_version: 0,
                serving_mode: "not_serving".to_string(),
                drop_percent: 0,
            },
            |meta| meta.proxy_heartbeat(request),
        )
    }

    pub fn put_proxy_group(&self, request: PutProxyGroupRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::PutProxyGroup(request)),
        }
    }

    pub fn drop_proxy_group(&self, request: DropProxyGroupRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropProxyGroup(request)),
        }
    }

    pub fn list_proxy_groups(&self, request: ListProxyGroupRequest) -> ListProxyGroupResponse {
        self.read_meta().map_or_else(
            |status| ListProxyGroupResponse {
                status,
                groups: Vec::new(),
            },
            |meta| meta.list_proxy_groups(request),
        )
    }

    pub fn update_manage_info(&self, request: UpdateManageInfoRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UpdateManageInfo(request)),
        }
    }

    pub fn mute_meta_change(&self) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::MuteMetaChange),
        }
    }

    pub fn resume_meta_change(&self) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::ResumeMetaChange),
        }
    }

    pub fn add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::AddNamespace(request)),
        }
    }

    pub fn add_table(&self, request: AddTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::AddTable(request)),
        }
    }

    pub fn delete_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DeleteTable(request)),
        }
    }

    pub fn update_table(&self, request: UpdateTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UpdateTable(request)),
        }
    }

    pub fn freeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeTable(request)),
        }
    }

    pub fn unfreeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UnfreezeTable(request)),
        }
    }

    pub fn freeze_partition(&self, request: PartitionStateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezePartition(request)),
        }
    }

    pub fn drop_partition(&self, request: PartitionStateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropPartition(request)),
        }
    }

    pub fn finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FinishLoad(request)),
        }
    }

    pub fn publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::PublishShardSnapshot(request)),
        }
    }

    pub fn freeze_server(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeServer(request)),
        }
    }

    pub fn drop_server(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropServer(request)),
        }
    }

    pub fn freeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeProxy(request)),
        }
    }

    pub fn drop_proxy(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropProxy(request)),
        }
    }

    pub fn freeze_stale_servers(&self, stale_after_ms: u64) -> StaleServerReport {
        let report = self.freeze_stale_resources_with_policy(
            stale_after_ms,
            SafeModePolicy {
                server_freeze_cooldown_ms: 0,
                proxy_freeze_cooldown_ms: 0,
            },
        );
        StaleServerReport {
            status: report.status,
            frozen_servers: report.frozen_servers,
        }
    }

    pub fn freeze_stale_resources_with_policy(
        &self,
        stale_after_ms: u64,
        policy: SafeModePolicy,
    ) -> StaleResourceReport {
        let now = current_time_ms();
        let servers = self.list_servers();
        if !servers.status.ok {
            return StaleResourceReport {
                status: servers.status,
                frozen_servers: Vec::new(),
                frozen_proxies: Vec::new(),
            };
        }
        let proxies = self.list_proxies();
        if !proxies.status.ok {
            return StaleResourceReport {
                status: proxies.status,
                frozen_servers: Vec::new(),
                frozen_proxies: Vec::new(),
            };
        }

        let mut frozen_servers = Vec::new();
        for server in servers.servers {
            if server.state == MetaEntityState::Normal
                && now.saturating_sub(server.last_heartbeat_ms) > stale_after_ms
            {
                let status = self.freeze_server(StateChangeRequest {
                    endpoint: server.server_addr.clone(),
                    freeze_cooldown_ms: policy.server_freeze_cooldown_ms,
                });
                if !status.status.ok {
                    return StaleResourceReport {
                        status: status.status,
                        frozen_servers,
                        frozen_proxies: Vec::new(),
                    };
                }
                frozen_servers.push(server.server_addr);
            }
        }

        let mut frozen_proxies = Vec::new();
        for proxy in proxies.proxies {
            if proxy.state == MetaEntityState::Normal
                && now.saturating_sub(proxy.last_heartbeat_ms) > stale_after_ms
            {
                let status = self.freeze_proxy(StateChangeRequest {
                    endpoint: proxy.proxy_addr.clone(),
                    freeze_cooldown_ms: policy.proxy_freeze_cooldown_ms,
                });
                if !status.status.ok {
                    return StaleResourceReport {
                        status: status.status,
                        frozen_servers,
                        frozen_proxies,
                    };
                }
                frozen_proxies.push(proxy.proxy_addr);
            }
        }

        StaleResourceReport {
            status: Status::ok(),
            frozen_servers,
            frozen_proxies,
        }
    }

    pub fn safe_mode_report(&self) -> SafeModeReport {
        self.read_meta().map_or_else(
            |status| SafeModeReport {
                status,
                blocked_servers: Vec::new(),
                blocked_proxies: Vec::new(),
                server_count: 0,
                proxy_count: 0,
            },
            |meta| meta.safe_mode_report(),
        )
    }

    pub fn list_servers(&self) -> ListServersResponse {
        self.read_meta().map_or_else(
            |status| ListServersResponse {
                status,
                servers: Vec::new(),
            },
            |meta| meta.list_servers(),
        )
    }

    pub fn list_proxies(&self) -> ListProxiesResponse {
        self.read_meta().map_or_else(
            |status| ListProxiesResponse {
                status,
                proxies: Vec::new(),
            },
            |meta| meta.list_proxies(),
        )
    }

    pub fn list_namespaces(&self) -> ListNamespacesResponse {
        self.read_meta().map_or_else(
            |status| ListNamespacesResponse {
                status,
                namespaces: Vec::new(),
            },
            |meta| meta.list_namespaces(),
        )
    }

    pub fn list_tables(&self) -> ListTablesResponse {
        self.read_meta().map_or_else(
            |status| ListTablesResponse {
                status,
                tables: Vec::new(),
            },
            |meta| meta.list_tables(),
        )
    }

    pub fn get_table_topology(&self, request: GetTableTopologyRequest) -> TableTopologyResponse {
        self.read_meta().map_or_else(
            |status| TableTopologyResponse {
                status,
                table: None,
                partitions: Vec::new(),
                unchanged: false,
            },
            |meta| meta.get_table_topology(request),
        )
    }

    pub fn info(&self) -> MetaInfo {
        self.read_meta().map_or_else(
            |status| MetaInfo {
                status,
                stats: MetaStats::default(),
                boot_time_ms: 0,
                durable_mutation_log: false,
                management_info: ManagementInfo::default(),
                manage_info: ManagementInfo::default(),
            },
            |meta| meta.info(),
        )
    }

    pub fn stats(&self) -> MetaStats {
        self.read_meta()
            .map(|meta| meta.stats())
            .unwrap_or_else(|_| MetaStats::default())
    }

    pub fn preflight_report(&self) -> MetaPreflightReport {
        self.read_meta()
            .map(|meta| meta.preflight_report())
            .unwrap_or_else(|status| MetaPreflightReport {
                status,
                stats: MetaStats::default(),
                normal_servers: 0,
                frozen_servers: 0,
                normal_proxies: 0,
                frozen_proxies: 0,
                dropped_tables: 0,
                shard_routes: 0,
                degraded_reasons: vec!["raft_read_unavailable".to_string()],
            })
    }

    pub fn topology_version_report(
        &self,
        request: TopologyVersionRequest,
    ) -> TopologyVersionReport {
        self.read_meta()
            .map(|meta| meta.topology_version_report(request.clone()))
            .unwrap_or_else(|status| TopologyVersionReport {
                status,
                current_topology_version: 0,
                old_topology_version: request.old_topology_version,
                unchanged: false,
                server_count: 0,
                proxy_count: 0,
                table_count: 0,
                shard_route_count: 0,
                normal_servers: 0,
                frozen_servers: 0,
                dropped_servers: 0,
                normal_proxies: 0,
                frozen_proxies: 0,
                dropped_proxies: 0,
                normal_tables: 0,
                frozen_tables: 0,
                dropped_tables: 0,
                changed_tables: Vec::new(),
                events: Vec::new(),
                event_history_truncated: false,
            })
    }

    fn mutation_status(&self, mutation: MetaMutation) -> Status {
        self.propose_mutation(mutation)
            .unwrap_or_else(|err| Status::error("raft_error", err.to_string()))
    }

    fn read_meta(&self) -> Result<SingleNodeMeta, Status> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or_else(|| Status::error("leader_unavailable", "meta raft leader unavailable"))?
            .commit_index;
        inner
            .nodes
            .values()
            .filter(|node| node.alive && node.commit_index >= leader_commit_index)
            .min_by_key(|node| node.id)
            .map(|node| node.meta.clone())
            .ok_or_else(|| Status::error("leader_unavailable", "meta raft has no readable quorum"))
    }

    fn propose_inner(&self, command: MetaCommand) -> Result<Option<Status>, RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.ensure_live_leader()?;
        let entry_bytes = serde_json::to_vec(&command)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default();
        if entry_bytes > inner.config.max_memory_replicate_log_bytes {
            return Err(RaftError::LogEntryTooLarge {
                bytes: entry_bytes,
                limit: inner.config.max_memory_replicate_log_bytes,
            });
        }
        let required = majority(inner.nodes.len());
        let live = inner.nodes.values().filter(|node| node.alive).count();
        if live < required {
            return Err(RaftError::NoMajority { live, required });
        }
        let leader_id = inner.leader_id;
        let leader = inner
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let entry = MetaLogEntry {
            term: leader.current_term,
            index: leader.log.last().map(|entry| entry.index + 1).unwrap_or(1),
            command,
        };
        let mut replicated = 0;
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            append_meta_entry(node, entry.clone());
            replicated += 1;
        }
        if replicated < required {
            return Err(RaftError::NoMajority {
                live: replicated,
                required,
            });
        }
        let mut leader_status = None;
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            node.commit_index = entry.index;
            let status = apply_meta_committed(node);
            if node.id == leader_id {
                leader_status = status;
            }
        }
        Ok(leader_status)
    }

    pub fn get_shard_location(
        &self,
        node_id: RaftNodeId,
        shard_id: ShardId,
    ) -> Result<Option<ShardLocation>, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        Ok(inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?
            .state
            .shards
            .get(&shard_id)
            .cloned())
    }

    pub fn get_shard_location_from_any_live(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<ShardLocation>, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
        let node = inner
            .nodes
            .values()
            .filter(|node| node.alive && node.commit_index >= leader_commit_index)
            .min_by_key(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)?;
        Ok(node.state.shards.get(&shard_id).cloned())
    }

    pub fn leader_id(&self) -> RaftNodeId {
        self.inner
            .read()
            .expect("meta raft lock poisoned")
            .leader_id
    }

    pub fn transfer_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.ensure_live_leader()?;
        let leader_commit_index = inner.leader_commit_index();
        let candidate = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !candidate.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if candidate.commit_index < leader_commit_index {
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: candidate.commit_index,
                leader_commit_index,
            });
        }
        inner.elect_leader(node_id)
    }

    pub fn read_index(&self, node_id: RaftNodeId) -> Result<ReadIndexResponse, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let status = inner.status();
        if !status.leader_lease_valid {
            return Err(RaftError::LeaderUnavailable);
        }
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if node.commit_index < status.commit_index {
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: node.commit_index,
                leader_commit_index: status.commit_index,
            });
        }
        Ok(ReadIndexResponse {
            leader_id: inner.leader_id,
            node_id,
            term: status.current_term,
            read_index: status.commit_index,
        })
    }

    pub fn check_read(
        &self,
        node_id: RaftNodeId,
        options: RaftReadOptions,
    ) -> Result<ReadIndexResponse, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let node = inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if !node.alive {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !options.enable_read_from_follower && node_id != inner.leader_id {
            return Err(RaftError::NotLeader { node_id });
        }
        drop(inner);
        match options.strategy {
            RaftReadStrategy::RelaxRead => {
                let status = self.status();
                Ok(ReadIndexResponse {
                    leader_id: status.leader_id,
                    node_id,
                    term: status.current_term,
                    read_index: self.commit_index(node_id)?,
                })
            }
            RaftReadStrategy::LeaseRead | RaftReadStrategy::ReadIndex => self.read_index(node_id),
        }
    }

    pub fn status(&self) -> RaftClusterStatus {
        self.inner.read().expect("meta raft lock poisoned").status()
    }

    pub fn config(&self) -> RaftConfig {
        self.inner
            .read()
            .expect("meta raft lock poisoned")
            .config
            .clone()
    }

    pub fn local_status(&self, node_id: RaftNodeId) -> Result<RaftNodeStatus, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader_commit_index = inner.leader_commit_index();
        inner
            .nodes
            .get(&node_id)
            .map(|node| meta_node_status(node, leader_commit_index))
            .ok_or(RaftError::NodeNotFound(node_id))
    }

    pub fn prometheus_metrics(&self) -> String {
        raft_status_prometheus("meta", self.status())
    }

    pub fn commit_index(&self, node_id: RaftNodeId) -> Result<u64, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        Ok(inner
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?
            .commit_index)
    }

    pub fn set_alive(&self, node_id: RaftNodeId, alive: bool) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.alive = alive;
        Ok(())
    }

    pub fn add_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        if inner.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeAlreadyExists(node_id));
        }
        inner.ensure_live_leader()?;
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let mut node = new_meta_node(node_id, RaftRole::Follower);
        node.current_term = leader.current_term;
        install_meta_leader_snapshot_tail(
            &mut node,
            leader.installed_snapshot_index,
            leader.installed_snapshot_term,
            leader.log.clone(),
            leader.commit_index,
            leader.state.clone(),
        );
        inner.nodes.insert(node_id, node);
        Ok(())
    }

    pub fn add_node_safely(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.add_node(node_id)?;
        self.catch_up_live_followers()?;
        Ok(self.scale_change_report())
    }

    pub fn plan_membership_change(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangePlan, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        inner.plan_membership_change(new_voters)
    }

    pub fn apply_membership_change_safely(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        let plan = self.plan_membership_change(new_voters)?;
        let joint_membership = JointConsensusMembership {
            old_voters: plan.old_voters.clone(),
            new_voters: plan.new_voters.clone(),
        };
        {
            let mut inner = self.inner.write().expect("meta raft lock poisoned");
            inner.ensure_live_leader()?;
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .ok_or(RaftError::LeaderUnavailable)?;
            let leader_term = leader.current_term;
            let leader_log = leader.log.clone();
            let leader_commit_index = leader.commit_index;
            let leader_state = leader.state.clone();
            let leader_snapshot_index = leader.installed_snapshot_index;
            let leader_snapshot_term = leader.installed_snapshot_term;
            let leader_meta = leader.meta.clone();
            for node_id in &plan.add_voters {
                if inner.nodes.contains_key(node_id) {
                    continue;
                }
                let mut node = new_meta_node(*node_id, RaftRole::Follower);
                node.current_term = leader_term;
                install_meta_leader_snapshot_tail(
                    &mut node,
                    leader_snapshot_index,
                    leader_snapshot_term,
                    leader_log.clone(),
                    leader_commit_index,
                    leader_state.clone(),
                );
                node.meta = leader_meta.clone();
                inner.nodes.insert(*node_id, node);
            }
        }
        let caught_up_voters = self.catch_up_live_followers()?;
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        for node_id in &plan.remove_voters {
            inner.remove_node_safely(*node_id)?;
        }
        let status = inner.status();
        let committed_membership = RaftMembership {
            shard_id: plan.shard_id,
            voters: inner.nodes.keys().copied().collect(),
            leader_id: status.leader_id,
        };
        Ok(RaftMembershipChangeReport {
            plan,
            joint_membership,
            committed_membership,
            caught_up_voters,
            leader_id: status.leader_id,
            commit_index: status.commit_index,
        })
    }

    pub fn create_snapshot(&self) -> Result<MetaRaftSnapshot, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        let last_included_term = leader
            .log
            .iter()
            .rev()
            .find(|entry| entry.index <= leader.commit_index)
            .map(|entry| entry.term)
            .unwrap_or(leader.current_term);
        Ok(MetaRaftSnapshot {
            last_included_term,
            last_included_index: leader.commit_index,
            state: leader.state.clone(),
        })
    }

    pub fn export_meta_snapshot(&self) -> Result<MetaSnapshot, RaftError> {
        let inner = self.inner.read().expect("meta raft lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        Ok(leader.meta.export_snapshot())
    }

    pub fn install_meta_snapshot_on_live_nodes(
        &self,
        snapshot: MetaSnapshot,
    ) -> Result<(), RaftError> {
        let validated_meta = SingleNodeMeta::default();
        let status = validated_meta.install_snapshot(snapshot.clone()).status;
        if !status.ok {
            return Err(RaftError::InvalidConfig(status.message));
        }
        let route_state = MetaState {
            shards: snapshot
                .shards
                .iter()
                .map(|(id, location)| (*id, location.clone()))
                .collect(),
            scheduler_state: None,
        };
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        let raft_snapshot = MetaRaftSnapshot {
            last_included_term: leader.current_term,
            last_included_index: leader.commit_index,
            state: route_state,
        };
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            install_meta_snapshot_state(node, raft_snapshot.clone());
            let meta = SingleNodeMeta::default();
            let status = meta.install_snapshot(snapshot.clone()).status;
            if !status.ok {
                return Err(RaftError::InvalidConfig(status.message));
            }
            node.meta = meta;
        }
        Ok(())
    }

    pub fn maybe_trigger_snapshot(&self) -> Result<RaftSnapshotTriggerReport, RaftError> {
        let (should_trigger, report) = {
            let inner = self.inner.read().expect("meta raft lock poisoned");
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            let applied_index = leader
                .applied
                .iter()
                .next_back()
                .copied()
                .unwrap_or_default();
            let applied_log_bytes =
                meta_log_bytes_after(&leader.log, leader.installed_snapshot_index);
            let mut report = RaftSnapshotTriggerReport {
                triggered: false,
                reason: "below_threshold".to_string(),
                leader_id: inner.leader_id,
                applied_index,
                last_snapshot_index: leader.installed_snapshot_index,
                applied_log_bytes,
                max_applied_log_bytes: inner.config.max_applied_log_bytes,
            };
            if !inner.config.can_trigger_snapshot {
                report.reason = "disabled".to_string();
                return Ok(report);
            }
            if applied_index <= leader.installed_snapshot_index {
                report.reason = "no_new_applied_logs".to_string();
                return Ok(report);
            }
            if applied_log_bytes < inner.config.max_applied_log_bytes {
                return Ok(report);
            }
            report.triggered = true;
            report.reason = "applied_log_bytes_threshold".to_string();
            (true, report)
        };

        if should_trigger {
            let snapshot = self.create_snapshot()?;
            let mut inner = self.inner.write().expect("meta raft lock poisoned");
            for node in inner.nodes.values_mut().filter(|node| node.alive) {
                if snapshot.last_included_index >= node.commit_index {
                    install_meta_snapshot_state(node, snapshot.clone());
                }
            }
        }
        Ok(report)
    }

    pub fn install_snapshot(
        &self,
        node_id: RaftNodeId,
        snapshot: MetaRaftSnapshot,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if snapshot.last_included_index < node.commit_index {
            return Err(RaftError::StaleSnapshot {
                snapshot_index: snapshot.last_included_index,
                local_commit_index: node.commit_index,
            });
        }
        install_meta_snapshot_state(node, snapshot);
        Ok(())
    }

    pub fn remove_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        if inner.nodes.len() == 1 {
            return Err(RaftError::CannotRemoveLastNode);
        }
        inner
            .nodes
            .remove(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if inner.leader_id == node_id {
            inner.promote_best_live_follower()?;
        }
        Ok(())
    }

    pub fn remove_node_safely(
        &self,
        node_id: RaftNodeId,
    ) -> Result<RaftScaleChangeReport, RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.remove_node_safely(node_id)?;
        Ok(inner.scale_change_report())
    }

    pub fn catch_up(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_state = leader.state.clone();
        let leader_snapshot_index = leader.installed_snapshot_index;
        let leader_snapshot_term = leader.installed_snapshot_term;
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        install_meta_leader_snapshot_tail(
            node,
            leader_snapshot_index,
            leader_snapshot_term,
            leader_log,
            leader_commit_index,
            leader_state,
        );
        Ok(())
    }

    pub fn catch_up_live_followers(&self) -> Result<Vec<RaftNodeId>, RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.catch_up_live_followers()
    }

    pub fn failover_primary(&self) -> Result<RaftFailoverReport, RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        let old_leader_id = inner.leader_id;
        if inner
            .nodes
            .get(&old_leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Ok(inner.failover_report(old_leader_id));
        }
        inner.promote_best_live_follower()?;
        Ok(inner.failover_report(old_leader_id))
    }

    pub fn replication_health(&self, max_allowed_lag: u64) -> RaftReplicationHealth {
        replication_health_from_status(self.status(), max_allowed_lag)
    }

    pub fn apply_health(&self, max_allowed_apply_lag: u64) -> RaftApplyHealth {
        raft_apply_health_from_status(self.status(), max_allowed_apply_lag)
    }

    pub fn scale_change_report(&self) -> RaftScaleChangeReport {
        self.inner
            .read()
            .expect("meta raft lock poisoned")
            .scale_change_report()
    }
}

impl MetaRaftClusterInner {
    fn ensure_live_leader(&mut self) -> Result<(), RaftError> {
        if self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.promote_best_live_follower()
    }

    fn promote_best_live_follower(&mut self) -> Result<(), RaftError> {
        let candidate = self
            .nodes
            .values()
            .filter(|node| node.alive)
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(meta_node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)?;
        self.elect_leader(candidate)
    }

    fn best_live_candidate_in(
        &self,
        allowed: &BTreeSet<RaftNodeId>,
    ) -> Result<RaftNodeId, RaftError> {
        self.nodes
            .values()
            .filter(|node| allowed.contains(&node.id) && node.alive)
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(meta_node_last_log_or_snapshot_index(node)),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)
    }

    fn catch_up_live_followers(&mut self) -> Result<Vec<RaftNodeId>, RaftError> {
        self.ensure_live_leader()?;
        let leader_id = self.leader_id;
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_log = leader.log.clone();
        let leader_commit_index = leader.commit_index;
        let leader_state = leader.state.clone();
        let leader_snapshot_index = leader.installed_snapshot_index;
        let leader_snapshot_term = leader.installed_snapshot_term;
        let mut caught_up = Vec::new();
        for node in self
            .nodes
            .values_mut()
            .filter(|node| node.alive && node.id != leader_id)
        {
            if node.commit_index < leader_commit_index
                || node.log.last().map(|entry| entry.index).unwrap_or_default()
                    < leader_log
                        .last()
                        .map(|entry| entry.index)
                        .unwrap_or_default()
            {
                install_meta_leader_snapshot_tail(
                    node,
                    leader_snapshot_index,
                    leader_snapshot_term,
                    leader_log.clone(),
                    leader_commit_index,
                    leader_state.clone(),
                );
            }
            if node.commit_index >= leader_commit_index {
                caught_up.push(node.id);
            }
        }
        Ok(caught_up)
    }

    fn remove_node_safely(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.nodes.len() == 1 {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if !self.nodes.contains_key(&node_id) {
            return Err(RaftError::NodeNotFound(node_id));
        }
        let remaining = self
            .nodes
            .keys()
            .copied()
            .filter(|id| *id != node_id)
            .collect::<BTreeSet<_>>();
        let required_after = majority(remaining.len());
        let live_after = remaining
            .iter()
            .filter(|id| self.nodes.get(id).map(|node| node.alive).unwrap_or(false))
            .count();
        if live_after < required_after {
            return Err(RaftError::NoMajority {
                live: live_after,
                required: required_after,
            });
        }

        if self.leader_id == node_id {
            let leader_commit_index = self.leader_commit_index();
            let candidate_id = self.best_live_candidate_in(&remaining)?;
            let candidate = self
                .nodes
                .get(&candidate_id)
                .ok_or(RaftError::NodeNotFound(candidate_id))?;
            if candidate.commit_index < leader_commit_index {
                return Err(RaftError::ReplicaLagging {
                    replica_id: candidate_id,
                    replica_commit_index: candidate.commit_index,
                    leader_commit_index,
                });
            }
            self.nodes.remove(&node_id);
            self.elect_leader(candidate_id)?;
        } else {
            self.nodes.remove(&node_id);
        }
        Ok(())
    }

    fn plan_membership_change(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangePlan, RaftError> {
        if !self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
        {
            return Err(RaftError::LeaderUnavailable);
        }
        let old_voters = self.nodes.keys().copied().collect::<BTreeSet<_>>();
        let new_voters = new_voters.into_iter().collect::<BTreeSet<_>>();
        if new_voters.is_empty() {
            return Err(RaftError::CannotRemoveLastNode);
        }
        if old_voters == new_voters {
            return Err(RaftError::InvalidConfig(
                "membership change must add or remove at least one voter".to_string(),
            ));
        }

        let add_voters = new_voters
            .difference(&old_voters)
            .copied()
            .collect::<Vec<_>>();
        let remove_voters = old_voters
            .difference(&new_voters)
            .copied()
            .collect::<Vec<_>>();
        let kind = match (add_voters.is_empty(), remove_voters.is_empty()) {
            (false, true) => RaftMembershipChangeKind::AddVoter,
            (true, false) => RaftMembershipChangeKind::RemoveVoter,
            (false, false) => RaftMembershipChangeKind::ReplaceVoter,
            (true, true) => unreachable!("old_voters != new_voters was checked"),
        };

        let live_new_voters = new_voters
            .iter()
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| node.alive)
                    .unwrap_or(true)
            })
            .count();
        let required_new_majority = majority(new_voters.len());
        if live_new_voters < required_new_majority {
            return Err(RaftError::NoMajority {
                live: live_new_voters,
                required: required_new_majority,
            });
        }

        Ok(RaftMembershipChangePlan {
            shard_id: 0,
            kind,
            old_voters: old_voters.into_iter().collect(),
            new_voters: new_voters.into_iter().collect(),
            add_voters,
            remove_voters,
        })
    }

    fn scale_change_report(&self) -> RaftScaleChangeReport {
        let status = self.status();
        RaftScaleChangeReport {
            leader_id: status.leader_id,
            voters: self.nodes.keys().copied().collect(),
            live_voters: status.live_voters,
            majority: status.majority,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| node.alive && node.lag == 0)
                .map(|node| node.node_id)
                .collect(),
        }
    }

    fn failover_report(&self, old_leader_id: RaftNodeId) -> RaftFailoverReport {
        let status = self.status();
        RaftFailoverReport {
            old_leader_id,
            new_leader_id: status.leader_id,
            term: status.current_term,
            commit_index: status.commit_index,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| node.alive && node.lag == 0)
                .map(|node| node.node_id)
                .collect(),
        }
    }

    fn elect_leader(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.config.prohibits_election {
            return Err(RaftError::ElectionProhibited);
        }
        let required = majority(self.nodes.len());
        let live = self.nodes.values().filter(|node| node.alive).count();
        if live < required {
            return Err(RaftError::NoMajority { live, required });
        }
        if !self
            .nodes
            .get(&node_id)
            .map(|node| node.alive)
            .unwrap_or(false)
        {
            return Err(RaftError::NodeNotFound(node_id));
        }
        if !self.candidate_log_would_win(node_id)? {
            let candidate_commit_index = self
                .nodes
                .get(&node_id)
                .map(|node| node.commit_index)
                .unwrap_or_default();
            return Err(RaftError::ReplicaLagging {
                replica_id: node_id,
                replica_commit_index: candidate_commit_index,
                leader_commit_index: self.leader_commit_index(),
            });
        }
        self.leader_id = node_id;
        let next_term = self
            .nodes
            .values()
            .map(|node| node.current_term)
            .max()
            .unwrap_or_default()
            + 1;
        for node in self.nodes.values_mut() {
            node.role = if node.id == node_id {
                RaftRole::Leader
            } else {
                RaftRole::Follower
            };
            node.current_term = next_term;
        }
        Ok(())
    }

    fn candidate_log_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        let candidate_last_index = meta_node_last_log_or_snapshot_index(candidate);
        let candidate_last_term = meta_node_last_log_or_snapshot_term(candidate);
        let votes = self
            .nodes
            .values()
            .filter(|node| node.alive)
            .filter(|node| {
                let local_last_index = meta_node_last_log_or_snapshot_index(node);
                let local_last_term = meta_node_last_log_or_snapshot_term(node);
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count();
        Ok(votes >= majority(self.nodes.len()))
    }

    fn leader_commit_index(&self) -> u64 {
        self.nodes
            .get(&self.leader_id)
            .map(|node| node.commit_index)
            .unwrap_or_default()
    }

    fn status(&self) -> RaftClusterStatus {
        let commit_index = self.leader_commit_index();
        let current_term = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.current_term)
            .unwrap_or_default();
        let majority = majority(self.nodes.len());
        let live_voters = self.nodes.values().filter(|node| node.alive).count();
        let leader_lease_valid = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
            && live_voters >= majority;
        RaftClusterStatus {
            leader_id: self.leader_id,
            current_term,
            commit_index,
            majority,
            live_voters,
            has_majority: live_voters >= majority,
            leader_lease_valid,
            nodes: self
                .nodes
                .values()
                .map(|node| meta_node_status(node, commit_index))
                .collect(),
        }
    }
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
