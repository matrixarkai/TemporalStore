use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::engine::TemporalEngine;
use crate::http::{
    json_response, parse_json, post_json_with_options, HttpRequest, HttpRequestOptions,
};
use crate::meta::ShardLocation;
use crate::types::{Command, CommandResponse, ExecuteRequest, ShardId};

pub type RaftNodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Leader,
    Follower,
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
    pub entries: Vec<RaftLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNodeStatus {
    pub node_id: RaftNodeId,
    pub role: RaftRole,
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
pub struct ReadIndexResponse {
    pub leader_id: RaftNodeId,
    pub node_id: RaftNodeId,
    pub term: u64,
    pub read_index: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RaftWalRecord {
    pub hard_state: RaftHardState,
    pub membership: RaftMembership,
    pub entries: Vec<RaftLogEntry>,
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
        let path = self.node_path(shard_id, node_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(record).map_err(io::Error::other)?;
        fs::write(&tmp, bytes)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn load_node(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
    ) -> io::Result<Option<RaftWalRecord>> {
        let path = self.node_path(shard_id, node_id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other)
    }

    fn node_path(&self, shard_id: ShardId, node_id: RaftNodeId) -> PathBuf {
        self.root
            .join(format!("shard-{shard_id}"))
            .join(format!("node-{node_id}.json"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppendEntriesRequest {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InstallSnapshotChunkRequest {
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
}

impl Default for RaftRpcRuntimeOptions {
    fn default() -> Self {
        Self {
            max_inflight: 128,
            max_retries: 2,
            retry_backoff_ms: 10,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftRpcRuntime<T> {
    transport: T,
    options: RaftRpcRuntimeOptions,
    inflight: Arc<Mutex<usize>>,
}

impl<T> RaftRpcRuntime<T> {
    pub fn new(transport: T, options: RaftRpcRuntimeOptions) -> Self {
        Self {
            transport,
            options: RaftRpcRuntimeOptions {
                max_inflight: options.max_inflight.max(1),
                ..options
            },
            inflight: Arc::default(),
        }
    }

    pub fn inflight(&self) -> usize {
        *self
            .inflight
            .lock()
            .expect("raft rpc inflight lock poisoned")
    }

    fn acquire(&self) -> Result<RaftRpcPermit, RaftError> {
        let mut inflight = self
            .inflight
            .lock()
            .expect("raft rpc inflight lock poisoned");
        if *inflight >= self.options.max_inflight {
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
            match call() {
                Ok(response) => return Ok(response),
                Err(err @ RaftError::Transport(_)) => {
                    last_error = Some(err);
                    if attempt + 1 < attempts && self.options.retry_backoff_ms > 0 {
                        thread::sleep(Duration::from_millis(self.options.retry_backoff_ms));
                    }
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_error.unwrap_or_else(|| RaftError::Transport("raft rpc failed".to_string())))
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
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.retry(|| self.transport.append_entries(request.clone()))
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.retry(|| self.transport.request_vote(request.clone()))
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.retry(|| self.transport.install_snapshot(request.clone()))
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        self.retry(|| self.transport.install_snapshot_chunk(request.clone()))
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftDistributedReadiness {
    pub complete: bool,
    pub local_model_tested: bool,
    pub transport_contracts_present: bool,
    pub http_transport_tested: bool,
    pub timer_election_tested: bool,
    pub missing: Vec<String>,
}

pub fn distributed_raft_readiness() -> RaftDistributedReadiness {
    let missing = vec![
        "full OpenRaft or raft-rs consensus engine swap".to_string(),
        "production Raft RPC runtime connection pooling and auth".to_string(),
        "durable metaserver HTTP mutation log and recovery".to_string(),
        "production randomized election timers and heartbeat scheduler".to_string(),
        "external multi-process crash/restart and network partition test harness".to_string(),
    ];
    RaftDistributedReadiness {
        complete: missing.is_empty(),
        local_model_tested: true,
        transport_contracts_present: true,
        http_transport_tested: true,
        timer_election_tested: true,
        missing,
    }
}

#[cfg(feature = "openraft-engine")]
pub mod openraft_integration {
    use super::*;

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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftConfigError {
    #[error("invalid raft config value: {0}")]
    InvalidValue(&'static str),
}

#[derive(Debug)]
struct RaftNode {
    id: RaftNodeId,
    role: RaftRole,
    current_term: u64,
    voted_for: Option<RaftNodeId>,
    commit_index: u64,
    alive: bool,
    log: Vec<RaftLogEntry>,
    applied: BTreeSet<u64>,
    engine: TemporalEngine,
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
    #[error("raft log entry too large: bytes={bytes}, limit={limit}")]
    LogEntryTooLarge { bytes: u64, limit: u64 },
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
    election_elapsed_tick: u64,
    joint_membership: Option<JointConsensusMembership>,
    pending_snapshots: BTreeMap<(RaftNodeId, String), PendingSnapshotChunks>,
}

impl RaftCluster {
    pub fn new_single_shard(
        shard_id: ShardId,
        node_ids: impl IntoIterator<Item = RaftNodeId>,
    ) -> Self {
        Self::new_single_shard_with_config(shard_id, node_ids, RaftConfig::default())
            .expect("default raft config must be valid")
    }

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
        Ok(Self {
            inner: Arc::new(RwLock::new(RaftClusterInner {
                shard_id,
                leader_id,
                nodes,
                config,
                election_elapsed_tick: 0,
                joint_membership: None,
                pending_snapshots: BTreeMap::new(),
            })),
        })
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
        for node_id in node_ids {
            let record = wal
                .load_node(shard_id, node_id)
                .map_err(|err| RaftError::Wal(err.to_string()))?;
            let mut node = if let Some(record) = record {
                let mut node = new_node(
                    node_id,
                    if record.membership.leader_id == node_id {
                        RaftRole::Leader
                    } else {
                        RaftRole::Follower
                    },
                    shard_id,
                );
                node.current_term = record.hard_state.current_term;
                node.voted_for = record.hard_state.voted_for;
                node.commit_index = record.hard_state.commit_index;
                node.log = record.entries;
                apply_committed(&mut node);
                leader_id.get_or_insert(record.membership.leader_id);
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
        Ok(Self {
            inner: Arc::new(RwLock::new(RaftClusterInner {
                shard_id,
                leader_id,
                nodes,
                config,
                election_elapsed_tick: 0,
                joint_membership: None,
                pending_snapshots: BTreeMap::new(),
            })),
        })
    }

    pub fn propose(&self, command: Command) -> Result<CommandResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
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
        if let Some((live, required)) = inner.joint_majority_failure() {
            return Err(RaftError::NoMajority { live, required });
        }
        let required = inner.required_majority();
        let live = inner.nodes.values().filter(|node| node.alive).count();
        if live < required {
            return Err(RaftError::NoMajority { live, required });
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
            index: leader.log.last().map(|entry| entry.index + 1).unwrap_or(1),
            shard_id,
            command,
        };

        let mut replicated = 0;
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            append_entry(node, entry.clone());
            replicated += 1;
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
            if let Some(response) = apply_committed(node) {
                if node.id == leader_id {
                    leader_response = response;
                }
            }
        }
        Ok(leader_response)
    }

    pub fn elect_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.elect_leader(node_id)
    }

    pub fn transfer_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.ensure_live_leader()?;
        let leader_commit_index = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?
            .commit_index;
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
        Ok(inner.leader_id)
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
        if inner.config.enable_pre_vote && !inner.pre_vote_would_win(candidate_id)? {
            inner.election_elapsed_tick = 0;
            return Ok(RaftTickOutcome::PreVoteRejected { candidate_id });
        }
        inner.elect_leader(candidate_id)?;
        inner.election_elapsed_tick = 0;
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
        node.current_term = leader.current_term;
        node.log = leader.log.clone();
        node.commit_index = leader.commit_index;
        apply_committed(&mut node);
        inner.nodes.insert(node_id, node);
        Ok(())
    }

    pub fn remove_node(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
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

    pub fn begin_joint_consensus(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<JointConsensusMembership, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if inner.joint_membership.is_some() {
            return Err(RaftError::JointConsensusInProgress);
        }
        inner.ensure_live_leader()?;
        let mut old_voters = inner.nodes.keys().copied().collect::<Vec<_>>();
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
        Ok(RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.nodes.keys().copied().collect(),
            leader_id: inner.leader_id,
        })
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
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.alive = alive;
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
        apply_committed(node);
        Ok(())
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
            voters: inner.nodes.keys().copied().collect(),
            leader_id: inner.leader_id,
        }
    }

    pub fn wal_records(&self) -> Vec<(RaftNodeId, RaftWalRecord)> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let membership = RaftMembership {
            shard_id: inner.shard_id,
            voters: inner.nodes.keys().copied().collect(),
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
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let target_last_index = inner
            .nodes
            .get(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?
            .log
            .last()
            .map(|entry| entry.index)
            .unwrap_or_default();
        let prev_log_index = target_last_index.min(leader.log.len() as u64);
        let prev_log_term = leader
            .log
            .iter()
            .find(|entry| entry.index == prev_log_index)
            .map(|entry| entry.term)
            .unwrap_or_default();
        let entries = leader
            .log
            .iter()
            .filter(|entry| entry.index > prev_log_index)
            .cloned()
            .collect();
        Ok(AppendEntriesRequest {
            shard_id: inner.shard_id,
            term: leader.current_term,
            leader_id: inner.leader_id,
            target_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: leader.commit_index,
        })
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
        let node = inner
            .nodes
            .get_mut(&request.target_id)
            .ok_or(RaftError::NodeNotFound(request.target_id))?;
        if request.term < node.current_term {
            return Ok(AppendEntriesResponse {
                term: node.current_term,
                success: false,
                match_index: node.log.last().map(|entry| entry.index).unwrap_or_default(),
                reject_reason: Some("stale_term".to_string()),
            });
        }
        if request.prev_log_index > 0 {
            let prev_term = node
                .log
                .iter()
                .find(|entry| entry.index == request.prev_log_index)
                .map(|entry| entry.term);
            if prev_term != Some(request.prev_log_term) {
                return Ok(AppendEntriesResponse {
                    term: node.current_term,
                    success: false,
                    match_index: node.log.last().map(|entry| entry.index).unwrap_or_default(),
                    reject_reason: Some("log_mismatch".to_string()),
                });
            }
        }
        node.current_term = request.term;
        node.role = RaftRole::Follower;
        for entry in request.entries {
            append_entry(node, entry);
        }
        let last_index = node.log.last().map(|entry| entry.index).unwrap_or_default();
        node.commit_index = request.leader_commit.min(last_index);
        apply_committed(node);
        Ok(AppendEntriesResponse {
            term: node.current_term,
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
        let node = inner
            .nodes
            .get_mut(&request.target_id)
            .ok_or(RaftError::NodeNotFound(request.target_id))?;
        if request.term < node.current_term {
            return Ok(VoteResponse {
                term: node.current_term,
                vote_granted: false,
                reject_reason: Some("stale_term".to_string()),
            });
        }
        let local_last_index = node.log.last().map(|entry| entry.index).unwrap_or_default();
        let local_last_term = node.log.last().map(|entry| entry.term).unwrap_or_default();
        let log_up_to_date =
            (request.last_log_term, request.last_log_index) >= (local_last_term, local_last_index);
        if !log_up_to_date {
            return Ok(VoteResponse {
                term: node.current_term,
                vote_granted: false,
                reject_reason: Some("candidate_log_behind".to_string()),
            });
        }
        if node.voted_for.is_some() && node.voted_for != Some(request.candidate_id) {
            return Ok(VoteResponse {
                term: node.current_term,
                vote_granted: false,
                reject_reason: Some("already_voted".to_string()),
            });
        }
        node.current_term = request.term;
        node.voted_for = Some(request.candidate_id);
        node.role = RaftRole::Follower;
        Ok(VoteResponse {
            term: node.current_term,
            vote_granted: true,
            reject_reason: None,
        })
    }

    pub fn build_install_snapshot_request(
        &self,
        target_id: RaftNodeId,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        Ok(InstallSnapshotRequest {
            shard_id: inner.shard_id,
            term: leader.current_term,
            leader_id: inner.leader_id,
            target_id,
            snapshot: self.create_snapshot()?,
        })
    }

    pub fn receive_install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        let result = self.install_snapshot(request.target_id, request.snapshot.clone());
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

    pub fn build_install_snapshot_chunks(
        &self,
        target_id: RaftNodeId,
        max_entries_per_chunk: usize,
    ) -> Result<Vec<InstallSnapshotChunkRequest>, RaftError> {
        let snapshot = self.create_snapshot()?;
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let chunk_size = max_entries_per_chunk.max(1);
        let chunk_count = snapshot.entries.len().max(1).div_ceil(chunk_size);
        let snapshot_id = format!(
            "{}-{}-{}",
            snapshot.shard_id, snapshot.last_included_term, snapshot.last_included_index
        );
        let mut chunks = Vec::new();
        if snapshot.entries.is_empty() {
            chunks.push(InstallSnapshotChunkRequest {
                shard_id: snapshot.shard_id,
                term: leader.current_term,
                leader_id: inner.leader_id,
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
                shard_id: snapshot.shard_id,
                term: leader.current_term,
                leader_id: inner.leader_id,
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
            return Err(RaftError::SnapshotShardMismatch {
                snapshot_shard_id: request.shard_id,
                cluster_shard_id: inner.shard_id,
            });
        }
        if request.chunk_count == 0 || request.chunk_index >= request.chunk_count {
            return Err(RaftError::InvalidSnapshotChunk(
                "chunk index is outside chunk count".to_string(),
            ));
        }
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
            return Err(RaftError::InvalidSnapshotChunk(
                "chunk metadata changed within snapshot".to_string(),
            ));
        }
        pending.chunks[request.chunk_index as usize] = Some(request.entries);
        let received_chunks = pending
            .chunks
            .iter()
            .filter(|chunk| chunk.is_some())
            .count() as u64;
        if received_chunks < request.chunk_count {
            let term = inner
                .nodes
                .get(&request.target_id)
                .map(|node| node.current_term)
                .unwrap_or(request.term);
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
        self.install_snapshot(
            request.target_id,
            RaftSnapshot {
                shard_id: request.shard_id,
                last_included_term: request.last_included_term,
                last_included_index: request.last_included_index,
                entries,
            },
        )?;
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
        let entries = leader
            .log
            .iter()
            .filter(|entry| entry.index <= leader.commit_index)
            .cloned()
            .collect::<Vec<_>>();
        let last_included_term = entries
            .last()
            .map(|entry| entry.term)
            .unwrap_or(leader.current_term);
        Ok(RaftSnapshot {
            shard_id: inner.shard_id,
            last_included_term,
            last_included_index: leader.commit_index,
            entries,
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
            engine.execute(ExecuteRequest {
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
        Ok(())
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

    pub fn read_index(&self, node_id: RaftNodeId) -> Result<ReadIndexResponse, RaftError> {
        let inner = self.inner.read().expect("raft cluster lock poisoned");
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
        let inner = self.inner.read().expect("raft cluster lock poisoned");
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
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .status()
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

    pub fn prometheus_metrics(&self) -> String {
        raft_status_prometheus("data", self.status())
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
            .filter(|node| node.alive)
            .min_by_key(|node| {
                (
                    std::cmp::Reverse(node.commit_index),
                    std::cmp::Reverse(node.log.last().map(|entry| entry.index).unwrap_or_default()),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)
    }

    fn pre_vote_would_win(&self, candidate_id: RaftNodeId) -> Result<bool, RaftError> {
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
            .filter(|node| {
                let local_last_index = node.log.last().map(|entry| entry.index).unwrap_or_default();
                let local_last_term = node.log.last().map(|entry| entry.term).unwrap_or_default();
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count();
        Ok(votes >= majority(self.nodes.len()))
    }

    fn elect_leader(&mut self, node_id: RaftNodeId) -> Result<(), RaftError> {
        if self.config.prohibits_election {
            return Err(RaftError::ElectionProhibited);
        }
        let required = self.required_majority();
        let live = self.nodes.values().filter(|node| node.alive).count();
        if live < required {
            return Err(RaftError::NoMajority { live, required });
        }
        if let Some((live, required)) = self.joint_majority_failure() {
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
        Ok(())
    }

    fn leader_commit_index(&self) -> u64 {
        self.nodes
            .get(&self.leader_id)
            .map(|node| node.commit_index)
            .unwrap_or_default()
    }

    fn required_majority(&self) -> usize {
        self.joint_membership
            .as_ref()
            .map(|membership| {
                majority(membership.old_voters.len()).max(majority(membership.new_voters.len()))
            })
            .unwrap_or_else(|| majority(self.nodes.len()))
    }

    fn joint_majority_failure(&self) -> Option<(usize, usize)> {
        self.joint_membership
            .as_ref()
            .and_then(|membership| joint_majority_failure(&self.nodes, membership))
    }

    fn status(&self) -> RaftClusterStatus {
        let commit_index = self.leader_commit_index();
        let current_term = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.current_term)
            .unwrap_or_default();
        let majority = self.required_majority();
        let live_voters = self.nodes.values().filter(|node| node.alive).count();
        let leader_lease_valid = self
            .nodes
            .get(&self.leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false)
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
        .filter(|node_id| nodes.get(node_id).map(|node| node.alive).unwrap_or(false))
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
        current_term: node.current_term,
        commit_index: node.commit_index,
        last_log_index: node.log.last().map(|entry| entry.index).unwrap_or_default(),
        applied_index: node.applied.iter().next_back().copied().unwrap_or_default(),
        alive: node.alive,
        lag: leader_commit_index.saturating_sub(node.commit_index),
    }
}

fn new_node(id: RaftNodeId, role: RaftRole, shard_id: ShardId) -> RaftNode {
    let engine = TemporalEngine::default();
    engine.load_shard(shard_id);
    RaftNode {
        id,
        role,
        current_term: 1,
        voted_for: None,
        commit_index: 0,
        alive: true,
        log: Vec::new(),
        applied: BTreeSet::new(),
        engine,
    }
}

fn append_entry(node: &mut RaftNode, entry: RaftLogEntry) {
    if node.log.last().map(|last| last.index) >= Some(entry.index) {
        node.log.retain(|existing| existing.index < entry.index);
    }
    node.log.push(entry);
}

fn apply_committed(node: &mut RaftNode) -> Option<CommandResponse> {
    let mut last_response = None;
    for entry in node
        .log
        .iter()
        .filter(|entry| entry.index <= node.commit_index)
    {
        if node.applied.insert(entry.index) {
            let response = node
                .engine
                .execute(ExecuteRequest {
                    shard_id: entry.shard_id,
                    command: entry.command.clone(),
                })
                .response;
            last_response = Some(response);
        }
    }
    last_response
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
    out.push_str("# HELP temporalstore_raft_node_lag Per-node raft commit lag behind leader.\n");
    out.push_str("# TYPE temporalstore_raft_node_lag gauge\n");
    out.push_str("# HELP temporalstore_raft_node_alive Whether a raft node is alive.\n");
    out.push_str("# TYPE temporalstore_raft_node_alive gauge\n");
    for node in status.nodes {
        let labels = &[
            ("kind", kind.to_string()),
            ("node_id", node.node_id.to_string()),
            ("role", format!("{:?}", node.role).to_ascii_lowercase()),
        ];
        push_raft_metric(
            &mut out,
            "temporalstore_raft_node_commit_index",
            labels,
            node.commit_index,
        );
        push_raft_metric(&mut out, "temporalstore_raft_node_lag", labels, node.lag);
        push_raft_metric(
            &mut out,
            "temporalstore_raft_node_alive",
            labels,
            u64::from(node.alive),
        );
    }
    out
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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MetaState {
    pub shards: BTreeMap<ShardId, ShardLocation>,
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
    state: MetaState,
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
    pub fn new(node_ids: impl IntoIterator<Item = RaftNodeId>) -> Self {
        Self::new_with_config(node_ids, RaftConfig::default())
            .expect("default raft config must be valid")
    }

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
        for node in inner.nodes.values_mut().filter(|node| node.alive) {
            node.commit_index = entry.index;
            apply_meta_committed(node);
        }
        Ok(())
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
        node.log = leader.log.clone();
        node.commit_index = leader.commit_index;
        apply_meta_committed(&mut node);
        inner.nodes.insert(node_id, node);
        Ok(())
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
        node.state = snapshot.state;
        node.current_term = node.current_term.max(snapshot.last_included_term);
        node.commit_index = snapshot.last_included_index;
        node.log
            .retain(|entry| entry.index > snapshot.last_included_index);
        node.applied.clear();
        node.applied.extend(1..=snapshot.last_included_index);
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
                    std::cmp::Reverse(node.log.last().map(|entry| entry.index).unwrap_or_default()),
                    node.id,
                )
            })
            .map(|node| node.id)
            .ok_or(RaftError::LeaderUnavailable)?;
        self.elect_leader(candidate)
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
        current_term: node.current_term,
        commit_index: node.commit_index,
        last_log_index: node.log.last().map(|entry| entry.index).unwrap_or_default(),
        applied_index: node.applied.iter().next_back().copied().unwrap_or_default(),
        alive: node.alive,
        lag: leader_commit_index.saturating_sub(node.commit_index),
    }
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
        state: MetaState::default(),
    }
}

fn append_meta_entry(node: &mut MetaRaftNode, entry: MetaLogEntry) {
    if node.log.last().map(|last| last.index) >= Some(entry.index) {
        node.log.retain(|existing| existing.index < entry.index);
    }
    node.log.push(entry);
}

fn apply_meta_committed(node: &mut MetaRaftNode) {
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
                }
                MetaCommand::RemoveShard(shard_id) => {
                    node.state.shards.remove(shard_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::serve;
    use crate::types::Command;
    use std::time::{Duration, Instant};

    #[test]
    fn raft_replicates_committed_write_to_majority_and_followers() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();

        for node_id in [1, 2, 3] {
            let response = cluster
                .read_local(
                    node_id,
                    Command::StringGet {
                        key: "k".to_string(),
                    },
                )
                .unwrap();
            assert_eq!(
                response,
                CommandResponse::Bytes {
                    value: Some(b"v".to_vec())
                }
            );
            assert_eq!(cluster.commit_index(node_id).unwrap(), 1);
        }
    }

    #[test]
    fn raft_rejects_write_without_majority() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(2, false).unwrap();
        cluster.set_alive(3, false).unwrap();

        let err = cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap_err();
        assert_eq!(
            err,
            RaftError::NoMajority {
                live: 1,
                required: 2
            }
        );
    }

    #[test]
    fn raft_follower_catches_up_after_outage() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v1".to_vec(),
            })
            .unwrap();

        assert_eq!(cluster.commit_index(3).unwrap(), 0);
        cluster.set_alive(3, true).unwrap();
        cluster.catch_up(3).unwrap();
        let response = cluster
            .read_local(
                3,
                Command::StringGet {
                    key: "k".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            response,
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );
        assert_eq!(cluster.commit_index(3).unwrap(), 1);
    }

    #[test]
    fn raft_transport_append_entries_catches_up_lagging_replica() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"transport-value".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();

        let request = cluster.build_append_entries_request(3).unwrap();
        assert_eq!(request.leader_id, 1);
        assert_eq!(request.target_id, 3);
        assert_eq!(request.entries.len(), 1);
        let response = cluster.append_entries(request).unwrap();
        assert!(response.success);
        assert_eq!(response.match_index, 1);
        assert_eq!(
            cluster
                .read_from_replica(
                    3,
                    Command::StringGet {
                        key: "k".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"transport-value".to_vec())
            }
        );
    }

    #[test]
    fn raft_transport_rejects_stale_append_entries_and_behind_vote() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();
        cluster.elect_leader(2).unwrap();
        let stale_append = AppendEntriesRequest {
            shard_id: 1,
            term: 1,
            leader_id: 1,
            target_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };
        let append_response = cluster.append_entries(stale_append).unwrap();
        assert!(!append_response.success);
        assert_eq!(append_response.reject_reason.as_deref(), Some("stale_term"));

        let vote_response = cluster
            .request_vote(VoteRequest {
                shard_id: 1,
                term: cluster.hard_state(2).unwrap().current_term + 1,
                candidate_id: 3,
                target_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            })
            .unwrap();
        assert!(!vote_response.vote_granted);
        assert_eq!(
            vote_response.reject_reason.as_deref(),
            Some("candidate_log_behind")
        );
    }

    #[test]
    fn raft_hard_state_membership_and_snapshot_transport_are_exposed() {
        let cluster = RaftCluster::new_single_shard(9, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "snap".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();
        let hard_state = cluster.hard_state(1).unwrap();
        assert_eq!(hard_state.current_term, 1);
        assert_eq!(hard_state.commit_index, 1);
        assert_eq!(
            cluster.membership(),
            RaftMembership {
                shard_id: 9,
                voters: vec![1, 2, 3],
                leader_id: 1,
            }
        );
        let request = cluster.build_install_snapshot_request(3).unwrap();
        let response = RaftTransport::install_snapshot(&cluster, request).unwrap();
        assert!(response.success);
        assert_eq!(response.last_included_index, 1);
    }

    #[test]
    fn http_raft_transport_sends_append_vote_and_snapshot_over_tcp() {
        let cluster = RaftCluster::new_single_shard(11, [1, 2, 3]);
        let addr = "127.0.0.1:18431".to_string();
        std::thread::spawn({
            let cluster = cluster.clone();
            let addr = addr.clone();
            move || serve(&addr, move |request| handle_raft_http(&cluster, request)).unwrap()
        });
        wait_for_http(&addr);

        let mut peers = BTreeMap::new();
        peers.insert(2, addr.clone());
        peers.insert(3, addr.clone());
        let transport = HttpRaftTransport::with_options(
            peers,
            HttpRequestOptions {
                connect_timeout_ms: 200,
                io_timeout_ms: 500,
                max_retries: 1,
            },
        );

        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "network".to_string(),
                value: b"append".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();

        let append = cluster.build_append_entries_request(3).unwrap();
        let append_response = transport.append_entries(append).unwrap();
        assert!(append_response.success);
        assert_eq!(append_response.match_index, 1);
        assert_eq!(
            cluster.read_local(
                3,
                Command::StringGet {
                    key: "network".to_string()
                },
            ),
            Ok(CommandResponse::Bytes {
                value: Some(b"append".to_vec())
            })
        );

        let vote = cluster.build_vote_request(2, 3).unwrap();
        let vote_response = transport.request_vote(vote).unwrap();
        assert!(vote_response.vote_granted);

        cluster
            .propose(Command::StringSet {
                key: "snapshot".to_string(),
                value: b"installed".to_vec(),
            })
            .unwrap();
        let snapshot = cluster.build_install_snapshot_request(3).unwrap();
        let snapshot_response = transport.install_snapshot(snapshot).unwrap();
        assert!(snapshot_response.success);
        assert_eq!(snapshot_response.last_included_index, 2);
    }

    #[test]
    fn streaming_snapshot_chunks_install_only_after_all_chunks_arrive() {
        let cluster = RaftCluster::new_single_shard(21, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        for index in 0..5 {
            cluster
                .propose(Command::StringSet {
                    key: format!("k{index}"),
                    value: vec![index as u8],
                })
                .unwrap();
        }
        cluster.set_alive(3, true).unwrap();
        let chunks = cluster.build_install_snapshot_chunks(3, 2).unwrap();
        assert_eq!(chunks.len(), 3);

        let first = cluster
            .receive_install_snapshot_chunk(chunks[0].clone())
            .unwrap();
        assert!(first.success);
        assert!(!first.snapshot_complete);
        assert_eq!(cluster.commit_index(3).unwrap(), 0);

        let second = cluster
            .receive_install_snapshot_chunk(chunks[1].clone())
            .unwrap();
        assert!(second.success);
        assert!(!second.snapshot_complete);
        assert_eq!(cluster.commit_index(3).unwrap(), 0);

        let final_chunk = cluster
            .receive_install_snapshot_chunk(chunks[2].clone())
            .unwrap();
        assert!(final_chunk.snapshot_complete);
        assert_eq!(final_chunk.last_included_index, 5);
        assert_eq!(cluster.commit_index(3).unwrap(), 5);
        assert_eq!(
            cluster
                .read_local(
                    3,
                    Command::StringGet {
                        key: "k4".to_string(),
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(vec![4])
            }
        );
    }

    #[test]
    fn joint_consensus_requires_old_and_new_majorities_before_commit_or_write() {
        let cluster = RaftCluster::new_single_shard(22, [1, 2, 3]);
        let membership = cluster
            .begin_joint_consensus([1, 2, 3, 4, 5, 6, 7])
            .unwrap();
        assert_eq!(membership.old_voters, vec![1, 2, 3]);
        assert_eq!(membership.new_voters, vec![1, 2, 3, 4, 5, 6, 7]);
        for node_id in [4, 5, 6, 7] {
            cluster.set_alive(node_id, false).unwrap();
        }
        assert_eq!(
            cluster
                .propose(Command::StringSet {
                    key: "blocked".to_string(),
                    value: b"v".to_vec(),
                })
                .unwrap_err(),
            RaftError::NoMajority {
                live: 3,
                required: 4,
            }
        );
        assert_eq!(
            cluster.commit_joint_consensus().unwrap_err(),
            RaftError::NoMajority {
                live: 3,
                required: 4,
            }
        );

        cluster.set_alive(4, true).unwrap();
        cluster.commit_joint_consensus().unwrap();
        assert_eq!(cluster.membership().voters, vec![1, 2, 3, 4, 5, 6, 7]);
        cluster
            .propose(Command::StringSet {
                key: "after".to_string(),
                value: b"ok".to_vec(),
            })
            .unwrap();
    }

    #[test]
    fn raft_rpc_runtime_retries_transport_errors_and_releases_inflight() {
        let transport = FlakyTransport {
            cluster: RaftCluster::new_single_shard(23, [1, 2, 3]),
            failures_left: Arc::new(Mutex::new(1)),
        };
        let runtime = RaftRpcRuntime::new(
            transport.clone(),
            RaftRpcRuntimeOptions {
                max_inflight: 1,
                max_retries: 2,
                retry_backoff_ms: 0,
            },
        );
        let response = runtime
            .append_entries(transport.cluster.build_append_entries_request(2).unwrap())
            .unwrap();
        assert!(response.success);
        assert_eq!(runtime.inflight(), 0);
    }

    #[test]
    fn partition_chaos_majority_side_continues_and_healed_replica_catches_up() {
        let cluster = RaftCluster::new_single_shard(24, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        for index in 0..3 {
            cluster
                .propose(Command::StringSet {
                    key: format!("majority-{index}"),
                    value: vec![index],
                })
                .unwrap();
        }
        assert_eq!(
            cluster.read_from_replica(
                3,
                Command::StringGet {
                    key: "majority-2".to_string(),
                },
            ),
            Err(RaftError::NodeNotFound(3))
        );

        cluster.set_alive(3, true).unwrap();
        cluster.catch_up(3).unwrap();
        assert_eq!(
            cluster.commit_index(3).unwrap(),
            cluster.status().commit_index
        );
        assert_eq!(
            cluster
                .read_from_replica(
                    3,
                    Command::StringGet {
                        key: "majority-2".to_string(),
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(vec![2])
            }
        );
    }

    #[derive(Debug, Clone)]
    struct FlakyTransport {
        cluster: RaftCluster,
        failures_left: Arc<Mutex<usize>>,
    }

    impl RaftTransport for FlakyTransport {
        fn append_entries(
            &self,
            request: AppendEntriesRequest,
        ) -> Result<AppendEntriesResponse, RaftError> {
            let mut failures_left = self
                .failures_left
                .lock()
                .expect("flaky transport lock poisoned");
            if *failures_left > 0 {
                *failures_left -= 1;
                return Err(RaftError::Transport("injected retry".to_string()));
            }
            drop(failures_left);
            self.cluster.receive_append_entries(request)
        }

        fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
            self.cluster.receive_vote_request(request)
        }

        fn install_snapshot(
            &self,
            request: InstallSnapshotRequest,
        ) -> Result<InstallSnapshotResponse, RaftError> {
            self.cluster.receive_install_snapshot(request)
        }

        fn install_snapshot_chunk(
            &self,
            request: InstallSnapshotChunkRequest,
        ) -> Result<InstallSnapshotChunkResponse, RaftError> {
            self.cluster.receive_install_snapshot_chunk(request)
        }
    }

    #[test]
    fn distributed_raft_readiness_reports_remaining_production_gaps() {
        let readiness = distributed_raft_readiness();
        assert!(!readiness.complete);
        assert!(readiness.local_model_tested);
        assert!(readiness.transport_contracts_present);
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("OpenRaft") || item.contains("raft-rs")));
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("metaserver") && item.contains("recovery")));
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("connection pooling") && item.contains("auth")));
    }

    fn wait_for_http(addr: &str) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("raft http server {addr} did not start");
    }

    #[test]
    fn raft_replica_read_rejects_lagging_follower_and_succeeds_after_catchup() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();

        assert_eq!(
            cluster
                .read_from_replica(
                    3,
                    Command::StringGet {
                        key: "k".to_string()
                    },
                )
                .unwrap_err(),
            RaftError::ReplicaLagging {
                replica_id: 3,
                replica_commit_index: 0,
                leader_commit_index: 1,
            }
        );

        cluster.catch_up(3).unwrap();
        assert_eq!(
            cluster
                .read_from_replica(
                    3,
                    Command::StringGet {
                        key: "k".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
    }

    #[test]
    fn raft_can_elect_new_leader_and_continue() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(1, false).unwrap();
        cluster.elect_leader(2).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v2".to_vec(),
            })
            .unwrap();

        let response = cluster
            .read_local(
                2,
                Command::StringGet {
                    key: "k".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            response,
            CommandResponse::Bytes {
                value: Some(b"v2".to_vec())
            }
        );
        assert_eq!(cluster.commit_index(2).unwrap(), 1);
        assert_eq!(cluster.commit_index(3).unwrap(), 1);
    }

    #[test]
    fn raft_tick_election_waits_for_timeout_and_prevotes_before_promotion() {
        let cluster = RaftCluster::new_single_shard_with_config(
            1,
            [1, 2, 3],
            RaftConfig {
                election_cycle_tick: 3,
                enable_pre_vote: true,
                ..RaftConfig::default()
            },
        )
        .unwrap();
        assert_eq!(
            cluster.tick_election().unwrap(),
            RaftTickOutcome::LeaderAlive { leader_id: 1 }
        );
        cluster.set_alive(1, false).unwrap();
        assert_eq!(
            cluster.tick_election().unwrap(),
            RaftTickOutcome::ElectionPending {
                elapsed_tick: 1,
                timeout_tick: 3,
            }
        );
        assert_eq!(
            cluster.tick_election().unwrap(),
            RaftTickOutcome::ElectionPending {
                elapsed_tick: 2,
                timeout_tick: 3,
            }
        );
        assert_eq!(
            cluster.tick_election().unwrap(),
            RaftTickOutcome::LeaderElected {
                leader_id: 2,
                term: 2,
            }
        );
        assert_eq!(cluster.leader_id(), 2);
    }

    #[test]
    fn raft_prevote_rejects_candidate_without_quorum() {
        let cluster = RaftCluster::new_single_shard_with_config(
            1,
            [1, 2, 3],
            RaftConfig {
                election_cycle_tick: 1,
                enable_pre_vote: true,
                ..RaftConfig::default()
            },
        )
        .unwrap();
        cluster.set_alive(1, false).unwrap();
        cluster.set_alive(3, false).unwrap();
        assert_eq!(
            cluster.tick_election().unwrap(),
            RaftTickOutcome::PreVoteRejected { candidate_id: 2 }
        );
        assert_eq!(cluster.leader_id(), 1);
    }

    #[test]
    fn raft_status_read_index_and_transfer_leader_match_engine_control_shape() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();

        let status = cluster.status();
        assert_eq!(status.leader_id, 1);
        assert_eq!(status.commit_index, 1);
        assert_eq!(status.majority, 2);
        assert!(status.has_majority);
        assert!(status.leader_lease_valid);
        assert_eq!(status.nodes.len(), 3);
        assert!(status.nodes.iter().all(|node| node.lag == 0));

        let read_index = cluster.read_index(2).unwrap();
        assert_eq!(read_index.leader_id, 1);
        assert_eq!(read_index.node_id, 2);
        assert_eq!(read_index.read_index, 1);

        cluster.transfer_leader(2).unwrap();
        assert_eq!(cluster.leader_id(), 2);
        let local = cluster.local_status(2).unwrap();
        assert_eq!(local.role, RaftRole::Leader);
        assert_eq!(local.commit_index, 1);

        let metrics = cluster.prometheus_metrics();
        assert!(metrics.contains("temporalstore_raft_cluster_commit_index{kind=\"data\"} 1"));
        assert!(metrics.contains("temporalstore_raft_node_lag"));
    }

    #[test]
    fn raft_config_matches_cpp_defaults_and_validates_required_limits() {
        let config = RaftConfig::default();
        assert_eq!(config.election_cycle_tick, 3);
        assert_eq!(config.max_apply_batch_bytes, 64 * 1024);
        assert_eq!(config.max_cache_memory_bytes, 32 * 1024 * 1024);
        assert_eq!(config.raft_transport_timeout_ms, 1_000);
        assert_eq!(config.max_segment_bytes, 64 * 1024 * 1024);
        assert!(!config.wal_sync);
        assert!(config.assume_lease_when_start);
        assert!(config.can_trigger_snapshot);
        assert!(config.validate().is_ok());

        let mut invalid = config;
        invalid.max_memory_replicate_log_bytes = 0;
        assert_eq!(
            invalid.validate(),
            Err(RaftConfigError::InvalidValue(
                "max_memory_replicate_log_bytes"
            ))
        );
    }

    #[test]
    fn raft_config_rejects_oversized_log_entries_and_prohibited_elections() {
        let mut config = RaftConfig {
            max_memory_replicate_log_bytes: 16,
            ..RaftConfig::default()
        };
        let cluster =
            RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config.clone()).unwrap();
        let err = cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: vec![b'x'; 128],
            })
            .unwrap_err();
        assert!(matches!(err, RaftError::LogEntryTooLarge { .. }));

        config.max_memory_replicate_log_bytes = 1024;
        config.prohibits_election = true;
        let cluster = RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config).unwrap();
        assert_eq!(cluster.elect_leader(2), Err(RaftError::ElectionProhibited));
    }

    #[test]
    fn raft_read_options_enforce_leader_and_follower_read_paths() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();

        assert_eq!(
            cluster.check_read(2, RaftReadOptions::default()),
            Err(RaftError::NotLeader { node_id: 2 })
        );
        assert!(cluster
            .check_read(
                2,
                RaftReadOptions {
                    enable_read_from_follower: true,
                    strategy: RaftReadStrategy::ReadIndex,
                    ..RaftReadOptions::default()
                },
            )
            .is_ok());
        assert!(cluster
            .check_read(
                1,
                RaftReadOptions {
                    strategy: RaftReadStrategy::LeaseRead,
                    ..RaftReadOptions::default()
                },
            )
            .is_ok());
    }

    #[test]
    fn raft_read_index_and_transfer_reject_lagging_replica() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();

        assert_eq!(
            cluster.read_index(3).unwrap_err(),
            RaftError::ReplicaLagging {
                replica_id: 3,
                replica_commit_index: 0,
                leader_commit_index: 1,
            }
        );
        assert_eq!(
            cluster.transfer_leader(3).unwrap_err(),
            RaftError::ReplicaLagging {
                replica_id: 3,
                replica_commit_index: 0,
                leader_commit_index: 1,
            }
        );
        cluster.catch_up(3).unwrap();
        assert!(cluster.read_index(3).is_ok());
        assert!(cluster.transfer_leader(3).is_ok());
    }

    #[test]
    fn secondary_is_promoted_automatically_when_primary_is_down() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(1, false).unwrap();
        let promoted = cluster.promote_if_leader_down().unwrap();
        assert_eq!(promoted, 2);
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"after-promotion".to_vec(),
            })
            .unwrap();
        let response = cluster
            .read_local(
                2,
                Command::StringGet {
                    key: "k".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            response,
            CommandResponse::Bytes {
                value: Some(b"after-promotion".to_vec())
            }
        );
    }

    #[test]
    fn scale_up_adds_caught_up_replica() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"before-scale-up".to_vec(),
            })
            .unwrap();
        cluster.add_node(4).unwrap();
        assert_eq!(cluster.commit_index(4).unwrap(), 1);
        let response = cluster
            .read_local(
                4,
                Command::StringGet {
                    key: "k".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            response,
            CommandResponse::Bytes {
                value: Some(b"before-scale-up".to_vec())
            }
        );
    }

    #[test]
    fn scale_down_removes_replica_and_continues_with_majority() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.remove_node(3).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"after-scale-down".to_vec(),
            })
            .unwrap();
        assert_eq!(cluster.commit_index(1).unwrap(), 1);
        assert_eq!(cluster.commit_index(2).unwrap(), 1);
        assert_eq!(cluster.commit_index(3), Err(RaftError::NodeNotFound(3)));
    }

    #[test]
    fn raft_snapshot_bootstraps_lagging_data_replica_then_catches_up_logs() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "k1".to_string(),
                value: b"snapshot-value".to_vec(),
            })
            .unwrap();
        let snapshot = cluster.create_snapshot().unwrap();

        cluster.set_alive(3, true).unwrap();
        cluster.install_snapshot(3, snapshot).unwrap();
        assert_eq!(cluster.commit_index(3).unwrap(), 1);
        assert_eq!(
            cluster
                .read_local(
                    3,
                    Command::StringGet {
                        key: "k1".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"snapshot-value".to_vec())
            }
        );

        cluster
            .propose(Command::StringSet {
                key: "k2".to_string(),
                value: b"post-snapshot-log".to_vec(),
            })
            .unwrap();
        cluster.catch_up(3).unwrap();
        assert_eq!(
            cluster
                .read_from_replica(
                    3,
                    Command::StringGet {
                        key: "k2".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"post-snapshot-log".to_vec())
            }
        );
    }

    #[test]
    fn raft_snapshot_cannot_overwrite_newer_data_state() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "k1".to_string(),
                value: b"v1".to_vec(),
            })
            .unwrap();
        let snapshot = cluster.create_snapshot().unwrap();
        cluster
            .propose(Command::StringSet {
                key: "k2".to_string(),
                value: b"v2".to_vec(),
            })
            .unwrap();

        assert_eq!(
            cluster.install_snapshot(2, snapshot).unwrap_err(),
            RaftError::StaleSnapshot {
                snapshot_index: 1,
                local_commit_index: 2,
            }
        );
    }

    #[test]
    fn raft_election_does_not_depend_on_snapshot_availability() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "before".to_string(),
                value: b"leader-local".to_vec(),
            })
            .unwrap();
        cluster.set_alive(1, false).unwrap();
        assert_eq!(cluster.promote_if_leader_down().unwrap(), 2);
        cluster
            .propose(Command::StringSet {
                key: "after".to_string(),
                value: b"new-leader".to_vec(),
            })
            .unwrap();
        assert_eq!(cluster.commit_index(2).unwrap(), 2);
    }

    #[test]
    fn metaserver_raft_replicates_shard_location_metadata() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        let location = ShardLocation {
            shard_id: 1,
            server_addr: "127.0.0.1:17002".to_string(),
        };
        meta.propose(MetaCommand::PutShardLocation(location.clone()))
            .unwrap();
        for node_id in [10, 11, 12] {
            assert_eq!(
                meta.get_shard_location(node_id, 1).unwrap(),
                Some(location.clone())
            );
        }
    }

    #[test]
    fn metaserver_raft_can_read_from_any_live_committed_replica() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 7,
            server_addr: "server-a".to_string(),
        }))
        .unwrap();
        assert_eq!(meta.commit_index(11).unwrap(), 1);
        meta.set_alive(10, false).unwrap();

        assert_eq!(
            meta.get_shard_location_from_any_live(7).unwrap(),
            Some(ShardLocation {
                shard_id: 7,
                server_addr: "server-a".to_string(),
            })
        );
    }

    #[test]
    fn metaserver_raft_supports_promotion_and_membership_changes() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.set_alive(10, false).unwrap();
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 2,
            server_addr: "server-b".to_string(),
        }))
        .unwrap();
        meta.add_node(13).unwrap();
        assert_eq!(
            meta.get_shard_location(13, 2).unwrap(),
            Some(ShardLocation {
                shard_id: 2,
                server_addr: "server-b".to_string()
            })
        );
        meta.remove_node(12).unwrap();
        meta.propose(MetaCommand::RemoveShard(2)).unwrap();
        assert_eq!(meta.get_shard_location(11, 2).unwrap(), None);
        assert_eq!(meta.get_shard_location(13, 2).unwrap(), None);
    }

    #[test]
    fn metaserver_raft_status_read_index_and_transfer_leader_work() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 7,
            server_addr: "127.0.0.1:17002".to_string(),
        }))
        .unwrap();

        let status = meta.status();
        assert_eq!(status.leader_id, 10);
        assert_eq!(status.commit_index, 1);
        assert!(status.leader_lease_valid);
        assert_eq!(status.nodes.len(), 3);

        let read_index = meta.read_index(11).unwrap();
        assert_eq!(read_index.leader_id, 10);
        assert_eq!(read_index.node_id, 11);
        assert_eq!(read_index.read_index, 1);

        meta.transfer_leader(11).unwrap();
        assert_eq!(meta.leader_id(), 11);
        assert_eq!(meta.local_status(11).unwrap().role, RaftRole::Leader);
        assert!(meta
            .prometheus_metrics()
            .contains("temporalstore_raft_cluster_commit_index{kind=\"meta\"} 1"));
    }

    #[test]
    fn metaserver_raft_promotes_follower_after_leader_failure_and_keeps_metadata_available() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 7,
            server_addr: "server-before-failover".to_string(),
        }))
        .unwrap();

        meta.set_alive(10, false).unwrap();
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 8,
            server_addr: "server-after-failover".to_string(),
        }))
        .unwrap();

        let status = meta.status();
        assert_ne!(status.leader_id, 10);
        assert!(status.has_majority);
        assert_eq!(
            meta.get_shard_location(status.leader_id, 7).unwrap(),
            Some(ShardLocation {
                shard_id: 7,
                server_addr: "server-before-failover".to_string()
            })
        );
        assert_eq!(
            meta.get_shard_location(status.leader_id, 8).unwrap(),
            Some(ShardLocation {
                shard_id: 8,
                server_addr: "server-after-failover".to_string()
            })
        );
    }

    #[test]
    fn metaserver_raft_rejects_reads_and_writes_without_majority() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 7,
            server_addr: "server-before-quorum-loss".to_string(),
        }))
        .unwrap();

        meta.set_alive(11, false).unwrap();
        meta.set_alive(12, false).unwrap();

        let status = meta.status();
        assert!(!status.has_majority);
        assert!(!status.leader_lease_valid);
        assert_eq!(meta.read_index(10), Err(RaftError::LeaderUnavailable));
        assert_eq!(
            meta.propose(MetaCommand::PutShardLocation(ShardLocation {
                shard_id: 8,
                server_addr: "server-without-quorum".to_string(),
            })),
            Err(RaftError::NoMajority {
                live: 1,
                required: 2
            })
        );
    }

    #[test]
    fn metaserver_snapshot_bootstraps_lagging_meta_replica() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.set_alive(12, false).unwrap();
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 9,
            server_addr: "server-snapshot".to_string(),
        }))
        .unwrap();
        let snapshot = meta.create_snapshot().unwrap();

        meta.set_alive(12, true).unwrap();
        meta.install_snapshot(12, snapshot).unwrap();
        assert_eq!(
            meta.get_shard_location(12, 9).unwrap(),
            Some(ShardLocation {
                shard_id: 9,
                server_addr: "server-snapshot".to_string()
            })
        );
        assert_eq!(meta.commit_index(12).unwrap(), 1);
    }

    #[test]
    fn metaserver_snapshot_cannot_overwrite_newer_meta_state() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 1,
            server_addr: "server-a".to_string(),
        }))
        .unwrap();
        let snapshot = meta.create_snapshot().unwrap();
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 2,
            server_addr: "server-b".to_string(),
        }))
        .unwrap();

        assert_eq!(
            meta.install_snapshot(11, snapshot).unwrap_err(),
            RaftError::StaleSnapshot {
                snapshot_index: 1,
                local_commit_index: 2,
            }
        );
    }

    #[test]
    fn local_raft_wal_persists_hard_state_membership_and_entries() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "wal-key".to_string(),
                value: b"wal-value".to_vec(),
            })
            .unwrap();
        cluster.persist_wal(dir.path()).unwrap();

        let wal = LocalRaftWal::new(dir.path());
        let record = wal.load_node(7, 1).unwrap().unwrap();
        assert_eq!(record.hard_state.commit_index, 1);
        assert_eq!(record.membership.shard_id, 7);
        assert_eq!(record.membership.voters, vec![1, 2, 3]);
        assert_eq!(record.entries.len(), 1);
        assert_eq!(record.entries[0].index, 1);
    }

    #[test]
    fn raft_cluster_recovers_committed_state_from_local_wal() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "recovered".to_string(),
                value: b"from-wal".to_vec(),
            })
            .unwrap();
        cluster.transfer_leader(2).unwrap();
        cluster.persist_wal(dir.path()).unwrap();

        let restored = RaftCluster::restore_single_shard_from_wal(
            dir.path(),
            7,
            [1, 2, 3],
            RaftConfig::default(),
        )
        .unwrap();
        assert_eq!(restored.leader_id(), 2);
        assert_eq!(restored.commit_index(1).unwrap(), 1);
        assert_eq!(
            restored.read_local(
                3,
                Command::StringGet {
                    key: "recovered".to_string()
                },
            ),
            Ok(CommandResponse::Bytes {
                value: Some(b"from-wal".to_vec())
            })
        );
    }
}
