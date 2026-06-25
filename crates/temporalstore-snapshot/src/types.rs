use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

pub type ShardId = u64;
pub type NodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionFormat {
    None,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageSegmentManifest {
    pub page_segment_id: String,
    pub relative_path: String,
    pub byte_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksumEntry {
    pub relative_path: String,
    pub sha256: String,
    pub byte_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub cluster_id: String,
    pub shard_id: ShardId,
    pub snapshot_id: String,
    pub raft_term: u64,
    pub last_log_index: u64,
    pub last_applied_log_id: String,
    pub created_at: DateTime<Utc>,
    pub engine_version: String,
    pub page_segments: Vec<PageSegmentManifest>,
    pub object_count: u64,
    pub record_count: u64,
    pub checksums: Vec<ChecksumEntry>,
    pub compression: CompressionFormat,
}

impl SnapshotManifest {
    pub fn stable_prefix(&self) -> String {
        format!(
            "{}/shards/{}/snapshots/{}-{}-{}/",
            self.cluster_id, self.shard_id, self.raft_term, self.last_log_index, self.snapshot_id
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSnapshot {
    pub manifest: SnapshotManifest,
    pub root_dir: PathBuf,
    pub index_path: PathBuf,
    pub checksums_path: PathBuf,
    pub page_segments: Vec<PathBuf>,
}

impl LocalSnapshot {
    pub fn new(
        cluster_id: impl Into<String>,
        shard_id: ShardId,
        raft_term: u64,
        last_log_index: u64,
        last_applied_log_id: impl Into<String>,
        root_dir: PathBuf,
        index_path: PathBuf,
        checksums_path: PathBuf,
        page_segments: Vec<PathBuf>,
    ) -> Self {
        let snapshot_id = Uuid::new_v4().to_string();
        let manifest = SnapshotManifest {
            cluster_id: cluster_id.into(),
            shard_id,
            snapshot_id,
            raft_term,
            last_log_index,
            last_applied_log_id: last_applied_log_id.into(),
            created_at: Utc::now(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            page_segments: Vec::new(),
            object_count: 0,
            record_count: 0,
            checksums: Vec::new(),
            compression: CompressionFormat::None,
        };

        Self {
            manifest,
            root_dir,
            index_path,
            checksums_path,
            page_segments,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub cluster_id: String,
    pub shard_id: ShardId,
    pub snapshot_id: String,
    pub raft_term: u64,
    pub last_log_index: u64,
    pub uri: String,
    pub byte_size: u64,
    pub checksum: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRetention {
    pub keep_last: usize,
    pub keep_newer_than_secs: u64,
}

impl Default for SnapshotRetention {
    fn default() -> Self {
        Self {
            keep_last: 3,
            keep_newer_than_secs: 7 * 24 * 60 * 60,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SnapshotStatus {
    Success,
    Failure,
}

impl SnapshotStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SnapshotStatus::Success => "success",
            SnapshotStatus::Failure => "failure",
        }
    }
}
