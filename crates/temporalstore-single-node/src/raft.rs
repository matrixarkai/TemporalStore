use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::engine::TemporalEngine;
use crate::meta::ShardLocation;
use crate::types::{Command, CommandResponse, ExecuteRequest, ShardId};

pub type RaftNodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub shard_id: ShardId,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    commit_index: u64,
    alive: bool,
    log: Vec<RaftLogEntry>,
    applied: BTreeSet<u64>,
    engine: TemporalEngine,
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
        let required = majority(inner.nodes.len());
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
        if replicated < required {
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
                .map(|node| node_status(node, commit_index))
                .collect(),
        }
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
    use crate::types::Command;

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
}
