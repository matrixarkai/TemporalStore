use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, RwLock,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::engine::TemporalEngine;
use crate::http::{
    json_response, parse_json, post_json_with_options, HttpRequest, HttpRequestOptions,
};
use crate::meta::{
    AckResponse, AddNamespaceRequest, AddTableRequest, GetShardResponse, GetTableTopologyRequest,
    ListNamespacesResponse, ListProxiesResponse, ListServersResponse, ListTablesResponse,
    LoadFinishRequest, MetaEntityState, MetaInfo, MetaMutation, MetaStats, ProxyHeartbeatRequest,
    ProxyHeartbeatResponse, RegisterProxyRequest, RegisterServerRequest, RegisterShardRequest,
    RegisterShardResponse, ServerHeartbeatRequest, ServerHeartbeatResponse, ShardLocation,
    SingleNodeMeta, StaleServerReport, StateChangeRequest, TableTopologyResponse,
};
use crate::types::{Command, CommandResponse, ExecuteRequest, ShardId, Status};

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
pub struct RaftExternalSnapshotRef {
    pub uri: String,
    pub checksum: String,
    pub byte_size: u64,
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
pub struct RaftReplicaLag {
    pub node_id: RaftNodeId,
    pub lag: u64,
    pub alive: bool,
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
pub struct RaftScaleChangeReport {
    pub leader_id: RaftNodeId,
    pub voters: Vec<RaftNodeId>,
    pub live_voters: usize,
    pub majority: usize,
    pub caught_up_voters: Vec<RaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftMembershipChangeKind {
    AddVoter,
    RemoveVoter,
    ReplaceVoter,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipChangePlan {
    pub shard_id: ShardId,
    pub kind: RaftMembershipChangeKind,
    pub old_voters: Vec<RaftNodeId>,
    pub new_voters: Vec<RaftNodeId>,
    pub add_voters: Vec<RaftNodeId>,
    pub remove_voters: Vec<RaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipChangeReport {
    pub plan: RaftMembershipChangePlan,
    pub joint_membership: JointConsensusMembership,
    pub committed_membership: RaftMembership,
    pub caught_up_voters: Vec<RaftNodeId>,
    pub leader_id: RaftNodeId,
    pub commit_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFailoverReport {
    pub old_leader_id: RaftNodeId,
    pub new_leader_id: RaftNodeId,
    pub term: u64,
    pub commit_index: u64,
    pub caught_up_voters: Vec<RaftNodeId>,
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
    #[serde(default)]
    pub joint_membership: Option<JointConsensusMembership>,
    pub entries: Vec<RaftLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct RaftWalEnvelope {
    sequence: u64,
    checksum: String,
    record: RaftWalRecord,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalSegmentReport {
    pub active_segment_id: u64,
    pub segments: Vec<RaftWalSegmentInfo>,
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
        file.sync_data()?;
        self.prune_node_segments(shard_id, node_id, min_keep_segments)?;
        self.segment_report(shard_id, node_id)
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
            segments.push(RaftWalSegmentInfo {
                segment_id,
                bytes: entry.metadata()?.len(),
                path: path.to_string_lossy().into_owned(),
            });
        }
        segments.sort_by_key(|segment| segment.segment_id);
        Ok(segments)
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
        Ok(RaftWalSegmentReport {
            active_segment_id: segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            segments,
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
        let Some(rpc) = rpc else {
            return Err(RaftError::Transport(
                "missing raft rpc metadata".to_string(),
            ));
        };
        if rpc.auth_token.as_deref() != Some(self.required_token.as_str()) {
            return Err(RaftError::Transport("raft rpc auth failed".to_string()));
        }
        if rpc.deadline_ms.unwrap_or_default() == 0 {
            return Err(RaftError::Transport(
                "raft rpc deadline missing".to_string(),
            ));
        }
        Ok(())
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftProposeRequest {
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftReadRequest {
    pub node_id: RaftNodeId,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedRaftCommandResponse {
    pub status: Status,
    pub response: CommandResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftDistributedReadiness {
    pub complete: bool,
    pub production_ready: bool,
    pub mode: RaftDeploymentMode,
    pub local_model_tested: bool,
    pub transport_contracts_present: bool,
    pub http_transport_tested: bool,
    pub rpc_runtime_observability_present: bool,
    pub external_snapshot_refs_present: bool,
    pub timer_election_tested: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftDeploymentMode {
    LocalModel,
    ProductionDistributed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftProductionReadinessError {
    pub mode: RaftDeploymentMode,
    pub message: String,
    pub missing: Vec<String>,
}

impl std::fmt::Display for RaftProductionReadinessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)?;
        if !self.missing.is_empty() {
            write!(formatter, ": {}", self.missing.join("; "))?;
        }
        Ok(())
    }
}

impl std::error::Error for RaftProductionReadinessError {}

pub fn distributed_raft_readiness() -> RaftDistributedReadiness {
    let missing = vec![
        "replace local consensus model with OpenRaft or raft-rs FSM/storage integration"
            .to_string(),
        "wire data-node Raft snapshots to real engine snapshot create/download/install".to_string(),
        "run external multi-process crash/restart, partition, slow follower, and rolling restart tests"
            .to_string(),
        "add production mTLS transport implementation instead of validation-only config".to_string(),
        "integrate metaserver shard membership changes with networked Raft groups".to_string(),
    ];
    RaftDistributedReadiness {
        complete: missing.is_empty(),
        production_ready: false,
        mode: RaftDeploymentMode::LocalModel,
        local_model_tested: true,
        transport_contracts_present: true,
        http_transport_tested: true,
        rpc_runtime_observability_present: true,
        external_snapshot_refs_present: true,
        timer_election_tested: true,
        missing,
    }
}

pub fn validate_raft_deployment_mode(
    mode: RaftDeploymentMode,
) -> Result<RaftDistributedReadiness, RaftProductionReadinessError> {
    let readiness = distributed_raft_readiness();
    match mode {
        RaftDeploymentMode::LocalModel => Ok(readiness),
        RaftDeploymentMode::ProductionDistributed if readiness.production_ready => Ok(readiness),
        RaftDeploymentMode::ProductionDistributed => Err(RaftProductionReadinessError {
            mode,
            message: "distributed Raft is not production-ready".to_string(),
            missing: readiness.missing,
        }),
    }
}

pub fn require_production_raft_ready() -> Result<(), RaftProductionReadinessError> {
    validate_raft_deployment_mode(RaftDeploymentMode::ProductionDistributed).map(|_| ())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProductionRaftEngineKind {
    OpenRaft,
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
                if self.cert_path.as_deref().unwrap_or_default().is_empty()
                    || self.key_path.as_deref().unwrap_or_default().is_empty()
                    || self.ca_cert_path.as_deref().unwrap_or_default().is_empty()
                {
                    return Err(RaftError::InvalidConfig(
                        "production raft mTLS requires cert_path, key_path, and ca_cert_path"
                            .to_string(),
                    ));
                }
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
        if !self
            .nodes
            .iter()
            .any(|node| node.node_id == self.local_node_id)
        {
            return Err(RaftError::InvalidConfig(
                "local_node_id must be present in production raft nodes".to_string(),
            ));
        }
        if self.wal_dir.is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires wal_dir".to_string(),
            ));
        }
        if self.heartbeat_interval_ms == 0 || self.election_tick_ms == 0 {
            return Err(RaftError::InvalidConfig(
                "production raft heartbeat/election intervals must be non-zero".to_string(),
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
        let transport = self.transport();
        self.cluster.propose_distributed(command, &transport)
    }

    pub fn apply_membership_change_safely(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        self.cluster.apply_membership_change_safely(new_voters)
    }

    pub fn read_local(
        &self,
        node_id: RaftNodeId,
        command: Command,
    ) -> Result<CommandResponse, RaftError> {
        self.cluster.read_from_replica(node_id, command)
    }

    pub fn start_timer_loop(&self) -> ProductionRaftTimerHandle {
        let cluster = self.cluster.clone();
        let heartbeat_interval = Duration::from_millis(self.options.heartbeat_interval_ms);
        let election_tick = Duration::from_millis(self.options.election_tick_ms);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut last_heartbeat = InstantCompat::now();
            while !stop_thread.load(Ordering::SeqCst) {
                let _ = cluster.tick_election();
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    let _ = cluster.catch_up_live_followers();
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
    applied_index: u64,
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
    wal: Option<LocalRaftWal>,
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
                wal: None,
                election_elapsed_tick: 0,
                joint_membership: None,
                pending_snapshots: BTreeMap::new(),
            })),
        })
    }

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
                if joint_membership.is_none() {
                    joint_membership = record.joint_membership;
                }
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
                wal: Some(wal),
                election_elapsed_tick: 0,
                joint_membership,
                pending_snapshots: BTreeMap::new(),
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
        let chunks = split_command_for_raft_limit(command, limit)?;
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
        inner.persist_configured_wal()?;
        Ok(leader_response)
    }

    pub fn propose_distributed<T: RaftTransport>(
        &self,
        command: Command,
        transport: &T,
    ) -> Result<CommandResponse, RaftError> {
        let limit = self
            .inner
            .read()
            .expect("raft cluster lock poisoned")
            .config
            .max_memory_replicate_log_bytes;
        let chunks = split_command_for_raft_limit(command, limit)?;
        let mut last_response = CommandResponse::Empty;
        for chunk in chunks {
            last_response = self.propose_distributed_one(chunk, transport)?;
        }
        Ok(last_response)
    }

    fn propose_distributed_one<T: RaftTransport>(
        &self,
        command: Command,
        transport: &T,
    ) -> Result<CommandResponse, RaftError> {
        let (entry, leader_id, target_ids, required) = {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.ensure_live_leader()?;
            let entry_bytes = command_size_bytes(&command);
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
            let leader_id = inner.leader_id;
            let shard_id = inner.shard_id;
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
            let leader = inner
                .nodes
                .get_mut(&leader_id)
                .ok_or(RaftError::LeaderUnavailable)?;
            append_entry(leader, entry.clone());
            let target_ids = inner
                .nodes
                .keys()
                .copied()
                .filter(|node_id| *node_id != leader_id)
                .collect::<Vec<_>>();
            (entry, leader_id, target_ids, required)
        };

        let mut replicated = 1usize;
        let mut successful_targets = Vec::new();
        for target_id in target_ids {
            let request = self.build_append_entries_request(target_id)?;
            match transport.append_entries(request) {
                Ok(response) if response.success && response.match_index >= entry.index => {
                    replicated += 1;
                    successful_targets.push(target_id);
                }
                _ => {}
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
            let response = apply_committed(leader).unwrap_or(CommandResponse::Empty);
            inner.persist_configured_wal()?;
            response
        };

        for target_id in &successful_targets {
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
            let _ = transport.append_entries(request);
            let _ = self.catch_up(*target_id);
        }

        Ok(leader_response)
    }

    pub fn elect_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.elect_leader(node_id)?;
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
        inner.elect_leader(node_id)?;
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
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn add_node_safely(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.add_node(node_id)?;
        self.catch_up_live_followers()?;
        Ok(self.scale_change_report())
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
            voters: inner.nodes.keys().copied().collect(),
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
        inner.persist_configured_wal()?;
        Ok(())
    }

    pub fn catch_up_live_followers(&self) -> Result<Vec<RaftNodeId>, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let caught_up = inner.catch_up_live_followers()?;
        inner.persist_configured_wal()?;
        Ok(caught_up)
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
                        joint_membership: inner.joint_membership.clone(),
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
            rpc: None,
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
        let term = node.current_term;
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
            inner.persist_configured_wal()?;
            return Ok(VoteResponse {
                term,
                vote_granted: false,
                reject_reason: Some("candidate_log_behind".to_string()),
            });
        }
        if node.voted_for.is_some() && node.voted_for != Some(request.candidate_id) {
            let term = node.current_term;
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
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        Ok(InstallSnapshotRequest {
            rpc: None,
            shard_id: inner.shard_id,
            term: leader.current_term,
            leader_id: inner.leader_id,
            target_id,
            external_snapshot_ref,
            snapshot: self.create_snapshot()?,
        })
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
                return Ok(InstallSnapshotResponse {
                    term: node.current_term,
                    success: false,
                    last_included_index: 0,
                    reject_reason: Some("stale_term".to_string()),
                });
            }
            node.current_term = request.term;
            node.role = RaftRole::Follower;
            node.voted_for = None;
        }
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
                rpc: None,
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
                rpc: None,
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
            return Ok(InstallSnapshotChunkResponse {
                term: node.current_term,
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
        node.applied_index = snapshot.last_included_index;
        inner.persist_configured_wal()?;
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

    pub fn replication_health(&self, max_allowed_lag: u64) -> RaftReplicationHealth {
        let status = self.status();
        replication_health_from_status(status, max_allowed_lag)
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
                    std::cmp::Reverse(node.log.last().map(|entry| entry.index).unwrap_or_default()),
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
                node.log = leader_log.clone();
                node.commit_index = leader_commit_index;
                apply_committed(node);
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
        let term = status.current_term;
        RaftFailoverReport {
            old_leader_id,
            new_leader_id: status.leader_id,
            term,
            commit_index: status.commit_index,
            caught_up_voters: status
                .nodes
                .into_iter()
                .filter(|node| node.alive && node.lag == 0)
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
        Ok(votes >= majority(self.nodes.len()))
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
            .filter(|node| {
                let local_last_index = node.log.last().map(|entry| entry.index).unwrap_or_default();
                let local_last_term = node.log.last().map(|entry| entry.term).unwrap_or_default();
                (candidate_last_term, candidate_last_index) >= (local_last_term, local_last_index)
            })
            .count())
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
            voters: self.nodes.keys().copied().collect(),
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
                        joint_membership: self.joint_membership.clone(),
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
        applied_index: node.applied_index,
        alive: node.alive,
        lag: leader_commit_index.saturating_sub(node.commit_index),
    }
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
        applied_index: 0,
        applied: BTreeSet::new(),
        engine,
    }
}

fn append_entry(node: &mut RaftNode, entry: RaftLogEntry) {
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
                .execute(ExecuteRequest {
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
    ApplyMutation(MetaMutation),
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

    pub fn server_heartbeat(&self, request: ServerHeartbeatRequest) -> ServerHeartbeatResponse {
        self.read_meta().map_or_else(
            |status| ServerHeartbeatResponse {
                status,
                forbid_auto_register: true,
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
            },
            |meta| meta.proxy_heartbeat(request),
        )
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

    pub fn finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FinishLoad(request)),
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
        let now = current_time_ms();
        let servers = self.list_servers();
        if !servers.status.ok {
            return StaleServerReport {
                status: servers.status,
                frozen_servers: Vec::new(),
            };
        }

        let mut frozen_servers = Vec::new();
        for server in servers.servers {
            if server.state == MetaEntityState::Normal
                && now.saturating_sub(server.last_heartbeat_ms) > stale_after_ms
            {
                let status = self.freeze_server(StateChangeRequest {
                    endpoint: server.server_addr.clone(),
                });
                if !status.status.ok {
                    return StaleServerReport {
                        status: status.status,
                        frozen_servers,
                    };
                }
                frozen_servers.push(server.server_addr);
            }
        }
        StaleServerReport {
            status: Status::ok(),
            frozen_servers,
        }
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
            },
            |meta| meta.info(),
        )
    }

    pub fn stats(&self) -> MetaStats {
        self.read_meta()
            .map(|meta| meta.stats())
            .unwrap_or_else(|_| MetaStats::default())
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
        node.log = leader.log.clone();
        node.commit_index = leader.commit_index;
        apply_meta_committed(&mut node);
        inner.nodes.insert(node_id, node);
        Ok(())
    }

    pub fn add_node_safely(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.add_node(node_id)?;
        self.catch_up_live_followers()?;
        Ok(self.scale_change_report())
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
        let node = inner
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.log = leader_log;
        node.commit_index = leader_commit_index;
        node.state = leader_state;
        apply_meta_committed(node);
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
                    std::cmp::Reverse(node.log.last().map(|entry| entry.index).unwrap_or_default()),
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
                    std::cmp::Reverse(node.log.last().map(|entry| entry.index).unwrap_or_default()),
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
                node.log = leader_log.clone();
                node.commit_index = leader_commit_index;
                node.state = leader_state.clone();
                apply_meta_committed(node);
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
                                },
                            );
                        }
                        MetaMutation::FinishLoad(request) if request.status.ok => {
                            node.state.shards.insert(
                                request.shard_id,
                                ShardLocation {
                                    shard_id: request.shard_id,
                                    server_addr: request.server_addr.clone(),
                                },
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    last_status
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{
        json_response, parse_json, post_json_with_options, serve, HttpRequestOptions,
    };
    use crate::types::{Command, FeatureFilter, FeatureFilterOp, SequenceFeatureRow};
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
    fn raft_rejects_electing_stale_replica_until_it_catches_up() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "stale-election".to_string(),
                value: b"committed".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();

        assert_eq!(
            cluster.elect_leader(3).unwrap_err(),
            RaftError::ReplicaLagging {
                replica_id: 3,
                replica_commit_index: 0,
                leader_commit_index: 1,
            }
        );
        assert_eq!(cluster.leader_id(), 1);

        cluster.catch_up(3).unwrap();
        cluster.elect_leader(3).unwrap();
        assert_eq!(cluster.leader_id(), 3);
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
            rpc: None,
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
                rpc: None,
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
    fn request_vote_higher_term_resets_prior_vote_before_decision() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.elect_leader(2).unwrap();
        assert_eq!(cluster.hard_state(1).unwrap().voted_for, None);

        let first_vote = cluster
            .request_vote(VoteRequest {
                rpc: None,
                shard_id: 1,
                term: 3,
                candidate_id: 2,
                target_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            })
            .unwrap();
        assert!(first_vote.vote_granted);
        assert_eq!(cluster.hard_state(1).unwrap().voted_for, Some(2));

        let higher_term_vote = cluster
            .request_vote(VoteRequest {
                rpc: None,
                shard_id: 1,
                term: 4,
                candidate_id: 3,
                target_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            })
            .unwrap();
        assert!(higher_term_vote.vote_granted);
        let hard_state = cluster.hard_state(1).unwrap();
        assert_eq!(hard_state.current_term, 4);
        assert_eq!(hard_state.voted_for, Some(3));
    }

    #[test]
    fn request_vote_higher_term_updates_term_even_when_candidate_log_is_behind() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "vote-term".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();
        let response = cluster
            .request_vote(VoteRequest {
                rpc: None,
                shard_id: 1,
                term: 5,
                candidate_id: 3,
                target_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            })
            .unwrap();
        assert!(!response.vote_granted);
        assert_eq!(response.term, 5);
        assert_eq!(
            response.reject_reason.as_deref(),
            Some("candidate_log_behind")
        );
        let hard_state = cluster.hard_state(1).unwrap();
        assert_eq!(hard_state.current_term, 5);
        assert_eq!(hard_state.voted_for, None);
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
    fn raft_install_snapshot_request_carries_external_snapshot_reference() {
        let cluster = RaftCluster::new_single_shard(19, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "external-snapshot".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();
        let snapshot_ref = RaftExternalSnapshotRef {
            uri: "s3://temporalstore-test/cluster-a/shards/19/snapshots/1-1-snap/manifest.json"
                .to_string(),
            checksum: "sha256:abc123".to_string(),
            byte_size: 512 * 1024 * 1024,
        };
        let request = cluster
            .build_install_snapshot_request_with_external_ref(3, Some(snapshot_ref.clone()))
            .unwrap();
        assert_eq!(request.external_snapshot_ref, Some(snapshot_ref));
        let response = cluster.receive_install_snapshot(request).unwrap();
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

        cluster.elect_leader(2).unwrap();
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
    fn raft_snapshot_transport_rejects_stale_term_before_install() {
        let cluster = RaftCluster::new_single_shard(211, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "snapshot-term".to_string(),
                value: b"leader-value".to_vec(),
            })
            .unwrap();
        cluster.elect_leader(2).unwrap();
        let mut request = cluster.build_install_snapshot_request(3).unwrap();
        request.term = 1;

        let response = cluster.receive_install_snapshot(request).unwrap();
        assert!(!response.success);
        assert_eq!(response.reject_reason.as_deref(), Some("stale_term"));
        assert_eq!(cluster.commit_index(3).unwrap(), 1);
        assert_eq!(cluster.hard_state(3).unwrap().current_term, 2);
    }

    #[test]
    fn raft_snapshot_chunk_transport_rejects_stale_term_before_buffering() {
        let cluster = RaftCluster::new_single_shard(212, [1, 2, 3]);
        for index in 0..3 {
            cluster
                .propose(Command::StringSet {
                    key: format!("snapshot-chunk-term-{index}"),
                    value: vec![index as u8],
                })
                .unwrap();
        }
        cluster.elect_leader(2).unwrap();
        let mut chunks = cluster.build_install_snapshot_chunks(3, 1).unwrap();
        chunks[0].term = 1;

        let response = cluster
            .receive_install_snapshot_chunk(chunks[0].clone())
            .unwrap();
        assert!(!response.success);
        assert!(!response.snapshot_complete);
        assert_eq!(response.reject_reason.as_deref(), Some("stale_term"));
        assert_eq!(cluster.hard_state(3).unwrap().current_term, 2);

        chunks[0].term = 2;
        let response = cluster
            .receive_install_snapshot_chunk(chunks[0].clone())
            .unwrap();
        assert!(response.success);
        assert!(!response.snapshot_complete);
        assert_eq!(response.received_chunks, 1);
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
    fn joint_consensus_state_survives_wal_restore_and_still_requires_both_majorities() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard(222, [1, 2, 3]);
        cluster.begin_joint_consensus([1, 2, 3, 4, 5]).unwrap();
        cluster.persist_wal(dir.path()).unwrap();

        let restored = RaftCluster::restore_single_shard_from_wal(
            dir.path(),
            222,
            [1, 2, 3, 4, 5],
            RaftConfig::default(),
        )
        .unwrap();
        assert_eq!(
            restored.joint_membership(),
            Some(JointConsensusMembership {
                old_voters: vec![1, 2, 3],
                new_voters: vec![1, 2, 3, 4, 5],
            })
        );

        restored.set_alive(2, false).unwrap();
        restored.set_alive(4, false).unwrap();
        restored.set_alive(5, false).unwrap();
        assert_eq!(
            restored
                .propose(Command::StringSet {
                    key: "blocked-after-restore".to_string(),
                    value: b"v".to_vec(),
                })
                .unwrap_err(),
            RaftError::NoMajority {
                live: 2,
                required: 3,
            }
        );

        restored.set_alive(4, true).unwrap();
        restored.commit_joint_consensus().unwrap();
        assert_eq!(restored.membership().voters, vec![1, 2, 3, 4, 5]);
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
                deadline_ms: 100,
                auth_token_required: false,
            },
        );
        let response = runtime
            .append_entries(transport.cluster.build_append_entries_request(2).unwrap())
            .unwrap();
        assert!(response.success);
        assert_eq!(runtime.inflight(), 0);
        let metrics = runtime.metrics();
        assert_eq!(metrics.attempts, 2);
        assert_eq!(metrics.retries, 1);
        assert_eq!(metrics.successes, 1);
        assert_eq!(metrics.failures, 0);
        assert_eq!(metrics.inflight, 0);

        let _permit = runtime.acquire().unwrap();
        assert!(matches!(
            runtime
                .append_entries(transport.cluster.build_append_entries_request(2).unwrap())
                .unwrap_err(),
            RaftError::Transport(message) if message.contains("backpressure")
        ));
        assert_eq!(runtime.metrics().backpressure_rejections, 1);
    }

    #[test]
    fn raft_rpc_runtime_attaches_auth_and_deadline_metadata() {
        let cluster = RaftCluster::new_single_shard(25, [1, 2, 3]);
        let authenticated = AuthenticatedRaftTransport::new(cluster.clone(), "secret");
        let unauthenticated_runtime = RaftRpcRuntime::new(
            authenticated.clone(),
            RaftRpcRuntimeOptions {
                max_inflight: 1,
                max_retries: 0,
                retry_backoff_ms: 0,
                deadline_ms: 250,
                auth_token_required: true,
            },
        );
        assert!(matches!(
            unauthenticated_runtime
                .append_entries(cluster.build_append_entries_request(2).unwrap())
                .unwrap_err(),
            RaftError::Transport(message) if message.contains("auth")
        ));

        let authenticated_runtime = RaftRpcRuntime::with_auth_token(
            authenticated,
            RaftRpcRuntimeOptions {
                max_inflight: 1,
                max_retries: 0,
                retry_backoff_ms: 0,
                deadline_ms: 250,
                auth_token_required: true,
            },
            Some("secret".to_string()),
        );
        let response = authenticated_runtime
            .append_entries(cluster.build_append_entries_request(2).unwrap())
            .unwrap();
        assert!(response.success);
    }

    #[test]
    fn raft_scheduler_randomizes_election_timeout_and_emits_heartbeats() {
        let mut scheduler = RaftScheduler::new(RaftSchedulerOptions {
            heartbeat_interval_tick: 2,
            election_timeout_min_tick: 3,
            election_timeout_max_tick: 5,
            random_seed: 7,
        });
        assert!(!scheduler.tick(true).heartbeat_due);
        assert!(scheduler.tick(true).heartbeat_due);

        let mut election_due_at = None;
        for tick in 1..=8 {
            let event = scheduler.tick(false);
            assert!((3..=5).contains(&event.election_timeout_tick));
            if event.election_due {
                election_due_at = Some(tick);
                break;
            }
        }
        assert!((3..=5).contains(&election_due_at.expect("election should become due")));
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
    fn distributed_raft_readiness_reports_remaining_production_blockers() {
        let readiness = distributed_raft_readiness();
        assert!(!readiness.complete);
        assert!(!readiness.production_ready);
        assert_eq!(readiness.mode, RaftDeploymentMode::LocalModel);
        assert!(readiness.local_model_tested);
        assert!(readiness.transport_contracts_present);
        assert!(readiness.rpc_runtime_observability_present);
        assert!(readiness.external_snapshot_refs_present);
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("OpenRaft") || item.contains("raft-rs")));
        assert!(readiness.missing.iter().any(|item| item.contains("mTLS")));
    }

    #[test]
    fn production_raft_mode_is_blocked_until_real_engine_and_chaos_exist() {
        assert!(validate_raft_deployment_mode(RaftDeploymentMode::LocalModel).is_ok());
        let err =
            validate_raft_deployment_mode(RaftDeploymentMode::ProductionDistributed).unwrap_err();
        assert_eq!(err.mode, RaftDeploymentMode::ProductionDistributed);
        assert!(err
            .missing
            .iter()
            .any(|item| item.contains("OpenRaft") || item.contains("raft-rs")));
        assert_eq!(require_production_raft_ready().unwrap_err(), err);
    }

    #[test]
    fn production_raft_runtime_validates_security_timer_and_chaos_contracts() {
        let dir = tempfile::tempdir().unwrap();
        let options = ProductionRaftRuntimeOptions {
            engine: ProductionRaftEngineKind::OpenRaft,
            shard_id: 91,
            local_node_id: 1,
            nodes: vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:19101".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:19102".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:19103".to_string(),
                },
            ],
            wal_dir: dir.path().display().to_string(),
            config: RaftConfig::default(),
            rpc: RaftRpcRuntimeOptions {
                max_retries: 1,
                deadline_ms: 50,
                ..RaftRpcRuntimeOptions::default()
            },
            security: ProductionRaftSecurity::plaintext_for_local_chaos("token"),
            heartbeat_interval_ms: 5,
            election_tick_ms: 1,
            allow_plaintext_for_local_chaos: true,
        };

        let runtime = ProductionRaftRuntime::start(options.clone()).unwrap();
        runtime.validate_ready().unwrap();
        assert_eq!(runtime.status().leader_id, 1);
        let transport = runtime.transport();
        assert_eq!(transport.metrics().inflight, 0);

        let timer = runtime.start_timer_loop();
        runtime
            .cluster()
            .propose(Command::StringSet {
                key: "production-raft".to_string(),
                value: b"ok".to_vec(),
            })
            .unwrap();
        thread::sleep(Duration::from_millis(20));
        timer.stop();
        assert!(runtime
            .cluster()
            .replication_health(0)
            .caught_up_voters
            .contains(&2));

        let chaos = ProductionRaftChaosPlan {
            shard_id: 91,
            nodes: options
                .nodes
                .iter()
                .map(|node| ProductionRaftProcessSpec {
                    node_id: node.node_id,
                    addr: node.addr.clone(),
                    wal_dir: format!("{}/node-{}", options.wal_dir, node.node_id),
                    command: "temporalstore-raft-node".to_string(),
                    args: vec!["--serve".to_string()],
                    env: BTreeMap::new(),
                })
                .collect(),
            partition_pairs: vec![(1, 3)],
            crash_nodes: vec![1],
            restart_nodes: vec![1],
        };
        chaos.validate().unwrap();

        let mut invalid = options;
        invalid.allow_plaintext_for_local_chaos = false;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn production_raft_runtime_replicates_over_separate_http_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let addr1 = free_local_addr();
        let addr2 = free_local_addr();
        let addr3 = free_local_addr();
        let nodes = vec![
            ProductionRaftNode {
                node_id: 1,
                addr: addr1,
            },
            ProductionRaftNode {
                node_id: 2,
                addr: addr2,
            },
            ProductionRaftNode {
                node_id: 3,
                addr: addr3,
            },
        ];
        let mut runtimes = Vec::new();
        for node in &nodes {
            let runtime = ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
                engine: ProductionRaftEngineKind::OpenRaft,
                shard_id: 193,
                local_node_id: node.node_id,
                nodes: nodes.clone(),
                wal_dir: dir
                    .path()
                    .join(format!("node-{}", node.node_id))
                    .display()
                    .to_string(),
                config: RaftConfig::default(),
                rpc: RaftRpcRuntimeOptions {
                    max_retries: 2,
                    deadline_ms: 200,
                    ..RaftRpcRuntimeOptions::default()
                },
                security: ProductionRaftSecurity::plaintext_for_local_chaos("token"),
                heartbeat_interval_ms: 20,
                election_tick_ms: 5,
                allow_plaintext_for_local_chaos: true,
            })
            .unwrap();
            let addr = node.addr.clone();
            let runtime_for_server = runtime.clone();
            thread::spawn(move || {
                serve(&addr, move |request| {
                    match (request.method.as_str(), request.path.as_str()) {
                        ("POST", "/raft/propose") => {
                            match parse_json::<DistributedRaftProposeRequest>(&request.body) {
                                Ok(req) => match runtime_for_server.propose(req.command) {
                                    Ok(response) => json_response(
                                        200,
                                        &DistributedRaftCommandResponse {
                                            status: Status::ok(),
                                            response,
                                        },
                                    ),
                                    Err(err) => json_response(
                                        200,
                                        &DistributedRaftCommandResponse {
                                            status: Status::error("raft_error", err.to_string()),
                                            response: CommandResponse::Empty,
                                        },
                                    ),
                                },
                                Err(err) => json_response(
                                    400,
                                    &Status::error("bad_request", err.to_string()),
                                ),
                            }
                        }
                        _ => handle_raft_http(&runtime_for_server.cluster(), request),
                    }
                })
                .unwrap()
            });
            runtimes.push(runtime);
        }
        for node in &nodes {
            wait_for_http(&node.addr);
        }

        let response: DistributedRaftCommandResponse = post_json_with_options(
            &nodes[0].addr,
            "/raft/propose",
            &DistributedRaftProposeRequest {
                command: Command::StringSet {
                    key: "separate-node".to_string(),
                    value: b"ready".to_vec(),
                },
            },
            HttpRequestOptions {
                connect_timeout_ms: 1_000,
                io_timeout_ms: 1_000,
                max_retries: 3,
            },
        )
        .unwrap();
        assert!(response.status.ok);

        assert_eq!(
            runtimes[1]
                .cluster()
                .read_from_replica(
                    2,
                    Command::StringGet {
                        key: "separate-node".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"ready".to_vec())
            }
        );
        assert_eq!(
            runtimes[2]
                .cluster()
                .read_from_replica(
                    3,
                    Command::StringGet {
                        key: "separate-node".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"ready".to_vec())
            }
        );
    }

    fn free_local_addr() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    fn long_sequence_rows(count: usize) -> Vec<SequenceFeatureRow> {
        (0..count)
            .map(|offset| SequenceFeatureRow {
                timestamp_ms: 1_700_000_000_000 + offset as u64,
                gid: offset as u64,
                action_type: (offset % 8) as u32,
                duration: (offset % 600) as u32,
                author_id: 42_000_000 + offset as u64,
            })
            .collect()
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
    fn raft_chunks_large_sequence_add_under_default_entry_limit() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        let rows = long_sequence_rows(5_000);

        cluster
            .propose(Command::SequenceAdd {
                key: "long-sequence".to_string(),
                rows,
            })
            .unwrap();

        assert!(cluster.commit_index(1).unwrap() > 1);
        assert_eq!(
            cluster
                .read_from_replica(
                    2,
                    Command::SequenceQuery {
                        key: "long-sequence".to_string(),
                        start_ms: 1_700_000_000_000,
                        end_ms: 1_700_000_999_999,
                        count: 5_000,
                        filters: vec![FeatureFilter {
                            field: "action_type".to_string(),
                            op: FeatureFilterOp::GreaterThan,
                            value: 2,
                        }],
                    },
                )
                .unwrap(),
            CommandResponse::SequenceRows {
                rows: long_sequence_rows(5_000)
                    .into_iter()
                    .filter(|row| row.action_type > 2)
                    .collect()
            }
        );
    }

    #[test]
    fn distributed_raft_chunks_large_sequence_add_under_default_entry_limit() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        let rows = long_sequence_rows(5_000);

        cluster
            .propose_distributed(
                Command::SequenceAdd {
                    key: "distributed-long-sequence".to_string(),
                    rows,
                },
                &cluster,
            )
            .unwrap();

        assert!(cluster.commit_index(1).unwrap() > 1);
        assert_eq!(
            cluster
                .read_from_replica(
                    3,
                    Command::SequenceQuery {
                        key: "distributed-long-sequence".to_string(),
                        start_ms: 1_700_000_000_000,
                        end_ms: 1_700_000_999_999,
                        count: 5_000,
                        filters: vec![FeatureFilter {
                            field: "duration".to_string(),
                            op: FeatureFilterOp::LessThan,
                            value: 10,
                        }],
                    },
                )
                .unwrap(),
            CommandResponse::SequenceRows {
                rows: long_sequence_rows(5_000)
                    .into_iter()
                    .filter(|row| row.duration < 10)
                    .collect()
            }
        );
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
    fn replication_health_reports_lag_and_heartbeat_catches_up_secondary() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "lag-key".to_string(),
                value: b"v1".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();

        let health = cluster.replication_health(0);
        assert!(!health.healthy);
        assert_eq!(health.max_lag, 1);
        assert_eq!(
            health.lagging_voters,
            vec![RaftReplicaLag {
                node_id: 3,
                lag: 1,
                alive: true,
            }]
        );

        let caught_up = cluster.catch_up_live_followers().unwrap();
        assert_eq!(caught_up, vec![2, 3]);
        let health = cluster.replication_health(0);
        assert!(health.healthy);
        assert_eq!(health.caught_up_voters, vec![1, 2, 3]);
        assert_eq!(
            cluster
                .read_from_replica(
                    3,
                    Command::StringGet {
                        key: "lag-key".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );
    }

    #[test]
    fn safe_scale_up_adds_replica_only_after_catchup() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "scale-up-safe".to_string(),
                value: b"ready".to_vec(),
            })
            .unwrap();

        let report = cluster.add_node_safely(4).unwrap();
        assert_eq!(report.voters, vec![1, 2, 3, 4]);
        assert_eq!(report.majority, 3);
        assert_eq!(report.caught_up_voters, vec![1, 2, 3, 4]);
        assert_eq!(
            cluster.commit_index(4).unwrap(),
            cluster.status().commit_index
        );
        assert!(cluster.read_index(4).is_ok());
    }

    #[test]
    fn safe_membership_change_adds_voter_through_joint_consensus() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "membership-add".to_string(),
                value: b"before".to_vec(),
            })
            .unwrap();

        let plan = cluster.plan_membership_change([1, 2, 3, 4]).unwrap();
        assert_eq!(plan.kind, RaftMembershipChangeKind::AddVoter);
        assert_eq!(plan.old_voters, vec![1, 2, 3]);
        assert_eq!(plan.new_voters, vec![1, 2, 3, 4]);
        assert_eq!(plan.add_voters, vec![4]);
        assert!(plan.remove_voters.is_empty());

        let report = cluster
            .apply_membership_change_safely([1, 2, 3, 4])
            .unwrap();
        assert_eq!(report.plan, plan);
        assert_eq!(report.joint_membership.old_voters, vec![1, 2, 3]);
        assert_eq!(report.joint_membership.new_voters, vec![1, 2, 3, 4]);
        assert_eq!(report.committed_membership.voters, vec![1, 2, 3, 4]);
        assert_eq!(report.caught_up_voters, vec![2, 3, 4]);
        assert_eq!(
            cluster.read_from_replica(
                4,
                Command::StringGet {
                    key: "membership-add".to_string()
                }
            ),
            Ok(CommandResponse::Bytes {
                value: Some(b"before".to_vec())
            })
        );
    }

    #[test]
    fn safe_membership_change_removes_leader_after_caught_up_successor_exists() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "membership-remove-leader".to_string(),
                value: b"before".to_vec(),
            })
            .unwrap();

        let report = cluster.apply_membership_change_safely([2, 3]).unwrap();
        assert_eq!(report.plan.kind, RaftMembershipChangeKind::RemoveVoter);
        assert_eq!(report.plan.remove_voters, vec![1]);
        assert_eq!(report.committed_membership.voters, vec![2, 3]);
        assert_ne!(report.leader_id, 1);
        assert_eq!(cluster.commit_index(1), Err(RaftError::NodeNotFound(1)));
        cluster
            .propose(Command::StringSet {
                key: "membership-after-leader-remove".to_string(),
                value: b"after".to_vec(),
            })
            .unwrap();
    }

    #[test]
    fn safe_membership_change_replaces_voter_and_rejects_invalid_targets() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);

        assert!(matches!(
            cluster.plan_membership_change([1, 2, 3]).unwrap_err(),
            RaftError::InvalidConfig(_)
        ));
        assert_eq!(
            cluster.plan_membership_change([]).unwrap_err(),
            RaftError::CannotRemoveLastNode
        );

        cluster.set_alive(2, false).unwrap();
        assert_eq!(
            cluster.apply_membership_change_safely([2, 3]).unwrap_err(),
            RaftError::NoMajority {
                live: 1,
                required: 2
            }
        );
        cluster.set_alive(2, true).unwrap();

        let report = cluster.apply_membership_change_safely([1, 2, 4]).unwrap();
        assert_eq!(report.plan.kind, RaftMembershipChangeKind::ReplaceVoter);
        assert_eq!(report.plan.add_voters, vec![4]);
        assert_eq!(report.plan.remove_voters, vec![3]);
        assert_eq!(report.committed_membership.voters, vec![1, 2, 4]);
        assert_eq!(cluster.commit_index(3), Err(RaftError::NodeNotFound(3)));
        assert!(cluster.read_index(4).is_ok());
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
    fn safe_scale_down_rejects_quorum_loss_and_promotes_caught_up_leader_successor() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster.set_alive(2, false).unwrap();
        assert_eq!(
            cluster.remove_node_safely(3).unwrap_err(),
            RaftError::NoMajority {
                live: 1,
                required: 2,
            }
        );

        cluster.set_alive(2, true).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "before-leader-remove".to_string(),
                value: b"ok".to_vec(),
            })
            .unwrap();
        let report = cluster.remove_node_safely(1).unwrap();
        assert_ne!(report.leader_id, 1);
        assert_eq!(report.voters, vec![2, 3]);
        assert_eq!(report.majority, 2);
        assert!(report.caught_up_voters.contains(&report.leader_id));
        cluster
            .propose(Command::StringSet {
                key: "after-leader-remove".to_string(),
                value: b"still-ok".to_vec(),
            })
            .unwrap();
    }

    #[test]
    fn primary_crash_promotes_caught_up_secondary_and_old_primary_recovers() {
        let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "before-crash".to_string(),
                value: b"v1".to_vec(),
            })
            .unwrap();
        cluster.set_alive(1, false).unwrap();

        let report = cluster.failover_primary().unwrap();
        assert_eq!(report.old_leader_id, 1);
        assert_eq!(report.new_leader_id, 2);
        assert_eq!(report.commit_index, 1);
        assert_eq!(cluster.leader_id(), 2);
        cluster
            .propose(Command::StringSet {
                key: "after-crash".to_string(),
                value: b"v2".to_vec(),
            })
            .unwrap();

        cluster.set_alive(1, true).unwrap();
        assert_eq!(cluster.local_status(1).unwrap().lag, 1);
        cluster.catch_up_live_followers().unwrap();
        assert_eq!(cluster.local_status(1).unwrap().lag, 0);
        assert_eq!(
            cluster
                .read_from_replica(
                    1,
                    Command::StringGet {
                        key: "after-crash".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"v2".to_vec())
            }
        );
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
    fn metaserver_raft_replicates_full_metadata_mutation_api() {
        let meta = MetaRaftCluster::new([10, 11, 12]);

        assert!(
            meta.register_server(RegisterServerRequest {
                server_addr: "server-a".to_string(),
                node_id: 1,
                location: "az-a".to_string(),
                binary_version: "test".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.add_namespace(AddNamespaceRequest {
                namespace: "feature".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "feature".to_string(),
                table_name: "user_seq".to_string(),
                first_shard_id: 100,
                shard_count: 2,
                replica_count: 1,
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                shard_id: 100,
                server_addr: "server-a".to_string(),
            })
            .status
            .ok
        );

        for node_id in [10, 11, 12] {
            assert_eq!(meta.commit_index(node_id).unwrap(), 4);
        }

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "feature".to_string(),
            table_name: "user_seq".to_string(),
            old_topology_version: 0,
        });
        assert!(topology.status.ok);
        assert_eq!(topology.table.unwrap().shard_count, 2);
        assert_eq!(topology.partitions.len(), 2);
        assert_eq!(topology.partitions[0].primary.as_deref(), Some("server-a"));

        let duplicate = meta.add_table(AddTableRequest {
            namespace: "feature".to_string(),
            table_name: "user_seq".to_string(),
            first_shard_id: 100,
            shard_count: 2,
            replica_count: 1,
        });
        assert!(!duplicate.status.ok);
        assert_eq!(duplicate.status.code, "already_exists");
    }

    #[test]
    fn metaserver_raft_freeze_stale_server_is_replicated_mutation() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.register_server(RegisterServerRequest {
            server_addr: "server-stale".to_string(),
            node_id: 1,
            location: "az-a".to_string(),
            binary_version: "test".to_string(),
        });
        thread::sleep(Duration::from_millis(2));

        let report = meta.freeze_stale_servers(0);
        assert!(report.status.ok);
        assert_eq!(report.frozen_servers, vec!["server-stale".to_string()]);

        let servers = meta.list_servers();
        assert!(servers.status.ok);
        assert_eq!(servers.servers[0].state, MetaEntityState::Frozen);
        for node_id in [10, 11, 12] {
            assert_eq!(meta.commit_index(node_id).unwrap(), 2);
        }
    }

    #[test]
    fn metaserver_raft_mutation_api_rejects_without_majority() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.set_alive(11, false).unwrap();
        meta.set_alive(12, false).unwrap();

        let response = meta.add_namespace(AddNamespaceRequest {
            namespace: "feature".to_string(),
        });
        assert!(!response.status.ok);
        assert_eq!(response.status.code, "raft_error");
        assert!(response.status.message.contains("majority"));
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
    fn metaserver_raft_health_catchup_safe_scale_and_failover_work() {
        let meta = MetaRaftCluster::new([10, 11, 12]);
        meta.set_alive(12, false).unwrap();
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 42,
            server_addr: "server-before-meta-lag".to_string(),
        }))
        .unwrap();
        meta.set_alive(12, true).unwrap();

        let health = meta.replication_health(0);
        assert!(!health.healthy);
        assert_eq!(
            health.lagging_voters,
            vec![RaftReplicaLag {
                node_id: 12,
                lag: 1,
                alive: true,
            }]
        );
        assert_eq!(meta.catch_up_live_followers().unwrap(), vec![11, 12]);
        assert!(meta.replication_health(0).healthy);

        let report = meta.add_node_safely(13).unwrap();
        assert_eq!(report.voters, vec![10, 11, 12, 13]);
        assert_eq!(report.caught_up_voters, vec![10, 11, 12, 13]);

        meta.set_alive(11, false).unwrap();
        meta.set_alive(12, false).unwrap();
        assert_eq!(
            meta.remove_node_safely(13).unwrap_err(),
            RaftError::NoMajority {
                live: 1,
                required: 2,
            }
        );
        meta.set_alive(11, true).unwrap();
        meta.set_alive(12, true).unwrap();
        meta.catch_up_live_followers().unwrap();

        meta.set_alive(10, false).unwrap();
        let failover = meta.failover_primary().unwrap();
        assert_eq!(failover.old_leader_id, 10);
        assert_ne!(failover.new_leader_id, 10);
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id: 43,
            server_addr: "server-after-meta-failover".to_string(),
        }))
        .unwrap();
        assert_eq!(
            meta.get_shard_location(failover.new_leader_id, 43).unwrap(),
            Some(ShardLocation {
                shard_id: 43,
                server_addr: "server-after-meta-failover".to_string()
            })
        );
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
    fn local_raft_wal_recovers_latest_valid_record_and_truncates_corrupt_tail() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
        cluster
            .propose(Command::StringSet {
                key: "wal-crash".to_string(),
                value: b"v1".to_vec(),
            })
            .unwrap();
        cluster.persist_wal(dir.path()).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "wal-crash".to_string(),
                value: b"v2".to_vec(),
            })
            .unwrap();
        cluster.persist_wal(dir.path()).unwrap();

        let wal = LocalRaftWal::new(dir.path());
        let path = wal.node_path(7, 1);
        let before_corruption = fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"sequence\":3,\"checksum\":\"bad\"")
            .unwrap();
        file.sync_data().unwrap();

        let recovery = wal.recover_node(7, 1).unwrap();
        assert!(recovery.corrupt_tail);
        assert!(recovery.truncated_bytes > 0);
        assert_eq!(recovery.valid_records, 2);
        assert_eq!(fs::metadata(&path).unwrap().len(), before_corruption);
        let record = recovery.record.unwrap();
        assert_eq!(record.hard_state.commit_index, 2);
        assert_eq!(record.entries.len(), 2);
    }

    #[test]
    fn local_raft_wal_segments_roll_retain_and_recover_latest_state() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
        let wal = LocalRaftWal::new(dir.path());
        for index in 0..8 {
            cluster
                .propose(Command::StringSet {
                    key: "segmented-wal".to_string(),
                    value: format!("v{index}").into_bytes(),
                })
                .unwrap();
            for (node_id, record) in cluster.wal_records() {
                wal.persist_node_segmented(7, node_id, &record, 256, 2)
                    .unwrap();
            }
        }

        let report = wal.segment_report(7, 1).unwrap();
        assert_eq!(report.segments.len(), 2);
        assert!(report.active_segment_id >= 2);
        assert!(report.segments.iter().all(|segment| segment.bytes > 0));

        let recovery = wal.recover_node(7, 1).unwrap();
        let record = recovery.record.unwrap();
        assert_eq!(record.hard_state.commit_index, 8);
        assert_eq!(record.entries.len(), 8);
    }

    #[test]
    fn local_raft_wal_segment_recovery_truncates_corrupt_tail_only() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
        let wal = LocalRaftWal::new(dir.path());
        for index in 0..3 {
            cluster
                .propose(Command::StringSet {
                    key: "segmented-crash".to_string(),
                    value: format!("v{index}").into_bytes(),
                })
                .unwrap();
            let record = cluster
                .wal_records()
                .into_iter()
                .find(|(node_id, _)| *node_id == 1)
                .unwrap()
                .1;
            wal.persist_node_segmented(7, 1, &record, 1024, 2).unwrap();
        }
        let report = wal.segment_report(7, 1).unwrap();
        let active = report.segments.last().unwrap();
        let before_corruption = fs::metadata(&active.path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&active.path).unwrap();
        file.write_all(b"{\"sequence\":99,\"checksum\":\"bad\"")
            .unwrap();
        file.sync_data().unwrap();

        let recovery = wal.recover_node(7, 1).unwrap();
        assert!(recovery.corrupt_tail);
        assert!(recovery.truncated_bytes > 0);
        assert_eq!(fs::metadata(&active.path).unwrap().len(), before_corruption);
        assert_eq!(recovery.record.unwrap().hard_state.commit_index, 3);
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

    #[test]
    fn wal_backed_raft_cluster_auto_persists_commits_leadership_and_membership() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = RaftCluster::new_single_shard_with_wal(
            dir.path(),
            77,
            [1, 2, 3],
            RaftConfig::default(),
        )
        .unwrap();
        cluster
            .propose(Command::StringSet {
                key: "auto-wal".to_string(),
                value: b"committed".to_vec(),
            })
            .unwrap();
        cluster.transfer_leader(2).unwrap();
        cluster.begin_joint_consensus([1, 2, 3, 4]).unwrap();

        let restored = RaftCluster::restore_single_shard_from_wal(
            dir.path(),
            77,
            [1, 2, 3, 4],
            RaftConfig::default(),
        )
        .unwrap();
        assert_eq!(restored.leader_id(), 2);
        assert_eq!(restored.commit_index(1).unwrap(), 1);
        assert_eq!(
            restored.joint_membership(),
            Some(JointConsensusMembership {
                old_voters: vec![1, 2, 3],
                new_voters: vec![1, 2, 3, 4],
            })
        );
        assert_eq!(
            restored.read_local(
                3,
                Command::StringGet {
                    key: "auto-wal".to_string()
                },
            ),
            Ok(CommandResponse::Bytes {
                value: Some(b"committed".to_vec())
            })
        );
        restored.commit_joint_consensus().unwrap();

        let rerestored = RaftCluster::restore_single_shard_from_wal(
            dir.path(),
            77,
            [1, 2, 3, 4],
            RaftConfig::default(),
        )
        .unwrap();
        assert_eq!(rerestored.joint_membership(), None);
        assert_eq!(rerestored.membership().voters, vec![1, 2, 3, 4]);
    }

    #[test]
    fn wal_backed_raft_cluster_compacts_wal_tail_but_recovers_latest_state() {
        let dir = tempfile::tempdir().unwrap();
        let config = RaftConfig {
            max_segment_bytes: 512,
            min_keep_segment_num: 2,
            ..RaftConfig::default()
        };
        let cluster =
            RaftCluster::new_single_shard_with_wal(dir.path(), 78, [1, 2, 3], config).unwrap();
        for index in 0..8 {
            cluster
                .propose(Command::StringSet {
                    key: "compact-wal".to_string(),
                    value: format!("v{index}").into_bytes(),
                })
                .unwrap();
        }

        let wal = LocalRaftWal::new(dir.path());
        let recovery = wal.recover_node(78, 1).unwrap();
        let report = wal.segment_report(78, 1).unwrap();
        assert_eq!(report.segments.len(), 2);
        let record = recovery.record.unwrap();
        assert_eq!(record.hard_state.commit_index, 8);
        assert_eq!(record.entries.len(), 8);

        let restored = RaftCluster::restore_single_shard_from_wal(
            dir.path(),
            78,
            [1, 2, 3],
            RaftConfig::default(),
        )
        .unwrap();
        assert_eq!(restored.commit_index(1).unwrap(), 8);
        assert_eq!(
            restored.read_local(
                2,
                Command::StringGet {
                    key: "compact-wal".to_string()
                },
            ),
            Ok(CommandResponse::Bytes {
                value: Some(b"v7".to_vec())
            })
        );
    }
}
