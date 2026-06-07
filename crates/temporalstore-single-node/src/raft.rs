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
}

impl RaftCluster {
    pub fn new_single_shard(
        shard_id: ShardId,
        node_ids: impl IntoIterator<Item = RaftNodeId>,
    ) -> Self {
        let mut nodes = BTreeMap::new();
        let mut iter = node_ids.into_iter();
        let leader_id = iter.next().unwrap_or(1);
        nodes.insert(leader_id, new_node(leader_id, RaftRole::Leader, shard_id));
        for node_id in iter {
            nodes.insert(node_id, new_node(node_id, RaftRole::Follower, shard_id));
        }
        Self {
            inner: Arc::new(RwLock::new(RaftClusterInner {
                shard_id,
                leader_id,
                nodes,
            })),
        }
    }

    pub fn propose(&self, command: Command) -> Result<CommandResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.ensure_live_leader()?;
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
}

impl MetaRaftCluster {
    pub fn new(node_ids: impl IntoIterator<Item = RaftNodeId>) -> Self {
        let mut nodes = BTreeMap::new();
        let mut iter = node_ids.into_iter();
        let leader_id = iter.next().unwrap_or(1);
        nodes.insert(leader_id, new_meta_node(leader_id, RaftRole::Leader));
        for node_id in iter {
            nodes.insert(node_id, new_meta_node(node_id, RaftRole::Follower));
        }
        Self {
            inner: Arc::new(RwLock::new(MetaRaftClusterInner { leader_id, nodes })),
        }
    }

    pub fn propose(&self, command: MetaCommand) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        inner.ensure_live_leader()?;
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
}
