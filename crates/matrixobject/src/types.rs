// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum MatrixObjectError {
    #[error("namespace not found: {0}")]
    NamespaceNotFound(NamespaceId),
    #[error("volume not found: {0}")]
    VolumeNotFound(VolumeId),
    #[error("segment not found: {0}")]
    SegmentNotFound(SegmentId),
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),
    #[error("segment is frozen: {0}")]
    SegmentFrozen(SegmentId),
    #[error("segment already exists: {0}")]
    SegmentAlreadyExists(SegmentId),
    #[error("node not found: {0}")]
    NodeNotFound(u64),
    #[error("request exceeds configured limit: {0}")]
    RequestTooLarge(String),
    #[error("admission control rejected request: {0}")]
    AdmissionControl(String),
    #[error("pipeline failed at {stage}: {message}")]
    Pipeline { stage: String, message: String },
    #[error("replication failed: {0}")]
    Replication(String),
    #[error("shared store failed: {0}")]
    SharedStore(String),
    #[error("shared store queue is full")]
    SharedStoreQueueFull,
    #[error("request timed out after {millis} ms")]
    Timeout { millis: u64 },
    #[error("stale open version for {segment_id}: expected {expected}, got {actual}")]
    StaleOpenVersion {
        segment_id: SegmentId,
        expected: u64,
        actual: u64,
    },
    #[error("invalid range offset={offset} length={length}")]
    InvalidRange { offset: u64, length: u64 },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, MatrixObjectError>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NamespaceId {
    pub tenant_id: String,
}

impl NamespaceId {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }

    pub fn path_key(&self) -> String {
        clean_path_part(&self.tenant_id)
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.tenant_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VolumeId {
    pub tenant_id: String,
    pub volume_id: String,
}

impl VolumeId {
    pub fn new(tenant_id: impl Into<String>, volume_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            volume_id: volume_id.into(),
        }
    }

    pub fn namespace_id(&self) -> NamespaceId {
        NamespaceId::new(self.tenant_id.clone())
    }

    pub fn path_key(&self) -> String {
        format!(
            "{}/{}",
            clean_path_part(&self.tenant_id),
            clean_path_part(&self.volume_id)
        )
    }
}

impl fmt::Display for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.tenant_id, self.volume_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentId {
    pub tenant_id: String,
    pub volume_id: String,
    pub segment_id: u64,
}

impl SegmentId {
    pub fn new(
        tenant_id: impl Into<String>,
        volume_id: impl Into<String>,
        segment_id: u64,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            volume_id: volume_id.into(),
            segment_id,
        }
    }

    pub fn path_key(&self) -> String {
        format!(
            "{}/{}/{}",
            clean_path_part(&self.tenant_id),
            clean_path_part(&self.volume_id),
            self.segment_id
        )
    }

    pub fn namespace_id(&self) -> NamespaceId {
        NamespaceId::new(self.tenant_id.clone())
    }

    pub fn volume_key(&self) -> VolumeId {
        VolumeId::new(self.tenant_id.clone(), self.volume_id.clone())
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.tenant_id, self.volume_id, self.segment_id
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentStatus {
    Open,
    Frozen,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteDurability {
    Async,
    SyncData,
    SyncAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageCommitMode {
    LocalOnly,
    SharedStoreAsync,
    SharedStoreSync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionKind {
    None,
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientDesc {
    pub client_id: u64,
    pub client_epoch: u64,
    pub auth_token: Option<String>,
}

impl Default for ClientDesc {
    fn default() -> Self {
        Self {
            client_id: 0,
            client_epoch: 0,
            auth_token: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QoSRequest {
    pub priority: IoPriority,
    pub deadline_ms: Option<u64>,
}

impl Default for QoSRequest {
    fn default() -> Self {
        Self {
            priority: IoPriority::Normal,
            deadline_ms: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IoPriority {
    Background,
    Normal,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenSegmentRequest {
    pub segment_id: SegmentId,
    pub expected_open_version: Option<u64>,
    pub create_if_missing: bool,
    pub client: ClientDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenSegmentResponse {
    pub open_version: u64,
    pub status: SegmentStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseSegmentRequest {
    pub segment_id: SegmentId,
    pub sync: bool,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseSegmentResponse {
    pub open_version: u64,
    pub status: SegmentStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateSegmentRequest {
    pub segment_id: SegmentId,
    pub status: Option<SegmentStatus>,
    pub logical_size: Option<u64>,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListSegmentsResponse {
    pub segments: Vec<SegmentSpace>,
    pub total_logical_size: u64,
    pub total_physical_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteRequest {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub data: Bytes,
    pub durability: WriteDurability,
    pub sequence_id: u64,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
    pub qos: QoSRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteResponse {
    pub written: u64,
    pub crc32: u32,
    pub open_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchWriteRequest {
    pub writes: Vec<WriteRequest>,
    pub ordered: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchWriteResponse {
    pub responses: Vec<WriteResponse>,
    pub total_written: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadRequest {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub length: u64,
    pub sequence_id: u64,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
    pub qos: QoSRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadResponse {
    pub data: Bytes,
    pub crc32: u32,
    pub open_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawSegmentWriteRequest {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub data: Bytes,
    pub durability: WriteDurability,
    pub sequence_id: u64,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
    pub qos: QoSRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawSegmentWriteResponse {
    pub written: u64,
    pub crc32: u32,
    pub open_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawSegmentReadRequest {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub length: u64,
    pub sequence_id: u64,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
    pub qos: QoSRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawSegmentReadResponse {
    pub data: Bytes,
    pub crc32: u32,
    pub open_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadVectorRequest {
    pub reads: Vec<ReadRequest>,
    pub ordered: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadVectorResponse {
    pub responses: Vec<ReadResponse>,
    pub total_read: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscardRequest {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub length: u64,
    pub sequence_id: u64,
    pub open_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub snapshot_id: String,
    pub segment_id: SegmentId,
    pub open_version: u64,
    pub logical_size: u64,
    pub chunk_count: usize,
    pub manifest_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub reference: SnapshotRef,
    pub manifest: SegmentManifest,
    pub physical_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotListResponse {
    pub snapshots: Vec<SnapshotRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaDiff {
    pub segment_id: SegmentId,
    pub old_open_version: u64,
    pub new_open_version: u64,
    pub logical_size_changed: bool,
    pub status_changed: bool,
    pub added_chunks: Vec<u64>,
    pub removed_chunks: Vec<u64>,
    pub changed_chunks: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub chunk_id: Uuid,
    pub chunk_index: u64,
    pub logical_offset: u64,
    pub logical_len: u64,
    #[serde(default)]
    pub physical_len: u64,
    pub physical_path: String,
    pub version: u32,
    pub crc32: u32,
    pub frozen: bool,
    #[serde(default)]
    pub flags: ChunkFlags,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkFlags {
    pub bits: u32,
}

impl ChunkFlags {
    pub const RECYCLING: u32 = 1 << 0;
    pub const DECOMMISSIONING: u32 = 1 << 1;
    pub const PINNED: u32 = 1 << 2;
    pub const SCRUB_REQUIRED: u32 = 1 << 3;

    pub fn contains(self, bit: u32) -> bool {
        self.bits & bit != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentManifest {
    pub segment_id: SegmentId,
    pub status: SegmentStatus,
    pub open_version: u64,
    pub logical_size: u64,
    pub chunk_size: u64,
    pub compression: CompressionKind,
    pub chunks: Vec<ChunkMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentSpace {
    pub segment_id: SegmentId,
    pub logical_space: u64,
    pub physical_space: u64,
    pub chunk_count: usize,
    pub open_version: u64,
    pub status: SegmentStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceDescriptor {
    pub namespace_id: NamespaceId,
    pub created_at_micros: u64,
    pub updated_at_micros: u64,
    pub serviceable: bool,
    pub max_volumes: Option<usize>,
    pub max_logical_bytes: Option<u64>,
    pub volume_count: usize,
    pub segment_count: usize,
    pub logical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeDescriptor {
    pub volume_id: VolumeId,
    pub created_at_micros: u64,
    pub updated_at_micros: u64,
    pub serviceable: bool,
    pub max_segments: Option<usize>,
    pub max_logical_bytes: Option<u64>,
    pub segment_count: usize,
    pub logical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<NamespaceDescriptor>,
    pub total_volumes: usize,
    pub total_segments: usize,
    pub total_logical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListVolumesResponse {
    pub volumes: Vec<VolumeDescriptor>,
    pub total_segments: usize,
    pub total_logical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskStatus {
    pub disk_id: u32,
    pub root: PathBuf,
    pub serviceable: bool,
    pub approximate_used_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskMediaType {
    Unknown,
    Hdd,
    Ssd,
    Nvme,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskPowerStatus {
    Online,
    Offline,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskLoadState {
    Init,
    Preparing,
    Replaying,
    Done,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskDescriptor {
    pub disk_id: u32,
    pub root: PathBuf,
    pub media_type: DiskMediaType,
    pub slot_id: Option<String>,
    pub block_device_path: Option<PathBuf>,
    pub power_status: DiskPowerStatus,
    pub load_state: DiskLoadState,
    pub load_started_at_micros: Option<u64>,
    pub load_cost_micros: Option<u64>,
    pub serviceable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddDiskRequest {
    pub disk_id: u32,
    pub root: PathBuf,
    pub media_type: DiskMediaType,
    pub slot_id: Option<String>,
    pub block_device_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveDiskRequest {
    pub disk_id: u32,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskLoadInfo {
    pub disk_id: u32,
    pub load_state: DiskLoadState,
    pub started_at_micros: Option<u64>,
    pub cost_micros: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreIoStats {
    pub read_ops: u64,
    pub read_bytes: u64,
    pub write_ops: u64,
    pub write_bytes: u64,
    pub discard_ops: u64,
    pub discard_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub range_read_ops: u64,
    pub range_read_bytes: u64,
    pub checksum_failures: u64,
    pub throttled_ops: u64,
    pub throttled_micros: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheStats {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub entry_count: usize,
    pub lru_len: usize,
    pub evictions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheWarmupReport {
    pub segment_id: SegmentId,
    pub warmed_chunks: usize,
    pub warmed_bytes: u64,
    pub skipped_chunks: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedStoreStats {
    pub enabled: bool,
    pub mode: Option<StorageCommitMode>,
    pub enqueued_writes: u64,
    pub committed_writes: u64,
    pub failed_writes: u64,
    pub in_flight_writes: u64,
    pub enqueued_bytes: u64,
    pub committed_bytes: u64,
    pub failed_bytes: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMeta {
    pub segment_id: SegmentId,
    pub chunk_index: u64,
    pub physical_path: String,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub crc32: u32,
    pub stored_crc32: u32,
    pub version: u32,
    pub exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubResult {
    pub segment_id: SegmentId,
    pub chunk_index: u64,
    pub ok: bool,
    pub expected_crc32: u32,
    pub actual_crc32: Option<u32>,
    pub expected_logical_len: u64,
    pub actual_physical_len: Option<u64>,
    pub error_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentScrubReport {
    pub segment_id: SegmentId,
    pub checked_chunks: usize,
    pub failed_chunks: usize,
    pub results: Vec<ScrubResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundThroughputOptions {
    pub read_bytes_per_sec: Option<u64>,
    pub write_bytes_per_sec: Option<u64>,
    pub priority: IoPriority,
}

impl Default for BackgroundThroughputOptions {
    fn default() -> Self {
        Self {
            read_bytes_per_sec: None,
            write_bytes_per_sec: None,
            priority: IoPriority::Background,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDeleteResult {
    pub chunk_index: u64,
    pub chunk_version: u32,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaleChunkVersion {
    pub chunk_index: u64,
    pub max_delete_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecycleBinEntry {
    pub recycle_id: String,
    pub segment_id: SegmentId,
    pub chunk: ChunkMeta,
    pub original_physical_path: String,
    pub recycled_physical_path: String,
    pub deleted_at_micros: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreRecycleBinRequest {
    pub recycle_ids: Vec<String>,
    pub client: ClientDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreRecycleBinResponse {
    pub restored: Vec<RecycleBinEntry>,
    pub not_found: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverDecommissionResponse {
    pub recovered_chunks: Vec<ChunkMeta>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenancePolicy {
    pub recycle_grace_micros: u64,
    pub max_recycle_entries_to_reclaim: usize,
    pub shared_store_min_keep_records: usize,
    pub shared_store_max_records_per_segment: usize,
}

impl Default for MaintenancePolicy {
    fn default() -> Self {
        Self {
            recycle_grace_micros: 24 * 60 * 60 * 1_000_000,
            max_recycle_entries_to_reclaim: 1024,
            shared_store_min_keep_records: 1,
            shared_store_max_records_per_segment: 16 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceReport {
    pub reclaimed_recycle_entries: usize,
    pub reclaimed_recycle_bytes: u64,
    pub trimmed_shared_store_records: usize,
    pub trimmed_shared_store_bytes: u64,
    pub compacted_oplogs: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundMaintenanceOptions {
    pub interval_micros: u64,
    pub policy: MaintenancePolicy,
}

impl Default for BackgroundMaintenanceOptions {
    fn default() -> Self {
        Self {
            interval_micros: 60 * 1_000_000,
            policy: MaintenancePolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundMaintenanceStatus {
    pub enabled: bool,
    pub interval_micros: u64,
    pub epoch: u64,
    pub runs: u64,
    pub failures: u64,
    pub last_started_at_micros: Option<u64>,
    pub last_finished_at_micros: Option<u64>,
    pub last_report: Option<MaintenanceReport>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFlag {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetRuntimeFlagRequest {
    pub name: String,
    pub value: String,
    pub client: ClientDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetRuntimeFlagResponse {
    pub name: String,
    pub value: Option<String>,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetChunkFlagsRequest {
    pub segment_id: SegmentId,
    pub chunk_index: u64,
    pub flags: ChunkFlags,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicatePriority {
    Low,
    Normal,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicateTaskStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicateChunkKey {
    pub segment_id: SegmentId,
    pub chunk_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyReplicateRequest {
    pub task_id: String,
    pub chunk: ReplicateChunkKey,
    pub source_node_id: u64,
    pub target_node_id: u64,
    pub priority: ReplicatePriority,
    pub expected_version: Option<u32>,
    pub client: ClientDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyReplicateResponse {
    pub task_id: String,
    pub status: ReplicateTaskStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicateChunkInfo {
    pub task_id: String,
    pub chunk: ReplicateChunkKey,
    pub status: ReplicateTaskStatus,
    pub chunk_meta: Option<ChunkMeta>,
    pub error_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckReplicateStatusResponse {
    pub chunk_infos: Vec<ReplicateChunkInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelReplicateResponse {
    pub cancelled: Vec<String>,
    pub not_found: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateChunkRequest {
    pub segment_id: SegmentId,
    pub chunk_index: u64,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateChunkResponse {
    pub chunk: ChunkMeta,
    pub open_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawChunkWriteRequest {
    pub segment_id: SegmentId,
    pub chunk_index: u64,
    pub offset: u64,
    pub data: Bytes,
    pub durability: WriteDurability,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
    pub qos: QoSRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawChunkWriteResponse {
    pub chunk: ChunkMeta,
    pub written: u64,
    pub crc32: u32,
    pub open_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawChunkReadRequest {
    pub segment_id: SegmentId,
    pub chunk_index: u64,
    pub offset: u64,
    pub length: u64,
    pub open_version: Option<u64>,
    pub client: ClientDesc,
    pub qos: QoSRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawChunkReadResponse {
    pub chunk: ChunkMeta,
    pub data: Bytes,
    pub crc32: u32,
    pub open_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawChunkDiscardRequest {
    pub segment_id: SegmentId,
    pub chunk_index: u64,
    pub offset: u64,
    pub length: u64,
    pub open_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardLinkChunkRequest {
    pub source_segment_id: SegmentId,
    pub source_chunk_index: u64,
    pub dest_segment_id: SegmentId,
    pub dest_chunk_index: u64,
    pub open_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub node_id: u64,
    pub address: String,
    pub zone: String,
    pub rack: String,
    pub disk_status: Vec<DiskStatus>,
    pub serviceable: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLoadReport {
    pub node_id: u64,
    pub observed_at_micros: u64,
    pub serviceable: bool,
    pub read_qps: u64,
    pub write_qps: u64,
    pub in_flight: u64,
    pub used_bytes: u64,
    pub free_bytes: Option<u64>,
    pub cache_hit_per_million: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportNodeLoadRequest {
    pub report: NodeLoadReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeFailureDetectorPolicy {
    pub heartbeat_timeout_micros: u64,
    pub rebalance_on_failure: bool,
}

impl Default for NodeFailureDetectorPolicy {
    fn default() -> Self {
        Self {
            heartbeat_timeout_micros: 30 * 1_000_000,
            rebalance_on_failure: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeFailureDetectorReport {
    pub checked_nodes: usize,
    pub stale_nodes: Vec<u64>,
    pub affected_segments: usize,
    pub rebalance_plan: RebalancePlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundFailureDetectorOptions {
    pub interval_micros: u64,
    pub policy: NodeFailureDetectorPolicy,
}

impl Default for BackgroundFailureDetectorOptions {
    fn default() -> Self {
        Self {
            interval_micros: 5 * 1_000_000,
            policy: NodeFailureDetectorPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundFailureDetectorStatus {
    pub enabled: bool,
    pub interval_micros: u64,
    pub epoch: u64,
    pub runs: u64,
    pub failures: u64,
    pub last_started_at_micros: Option<u64>,
    pub last_finished_at_micros: Option<u64>,
    pub last_report: Option<NodeFailureDetectorReport>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentReplicaRole {
    Primary,
    Secondary,
    Witness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentReplicaDescriptor {
    pub node_id: u64,
    pub role: SegmentReplicaRole,
    pub lag_versions: u64,
    pub serviceable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub status: SegmentStatus,
    pub open_version: u64,
    pub logical_size: u64,
    pub replicas: Vec<SegmentReplicaDescriptor>,
    pub snapshots: Vec<SnapshotRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceReason {
    ScaleUp,
    ScaleDown,
    NodeFailure,
    UnderReplicated,
    ZoneSpread,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RebalanceAction {
    AddReplica {
        segment_id: SegmentId,
        node_id: u64,
        reason: RebalanceReason,
    },
    RemoveReplica {
        segment_id: SegmentId,
        node_id: u64,
        reason: RebalanceReason,
    },
    PromoteReplica {
        segment_id: SegmentId,
        node_id: u64,
        reason: RebalanceReason,
    },
    MarkReplicaUnserviceable {
        segment_id: SegmentId,
        node_id: u64,
        reason: RebalanceReason,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalancePlan {
    pub actions: Vec<RebalanceAction>,
    pub affected_segments: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SniffSegmentsResponse {
    pub segments: Vec<SegmentDescriptor>,
    pub total_logical_size: u64,
    pub total_replicas: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotDiff {
    pub segment_id: SegmentId,
    pub old_snapshot_id: String,
    pub new_snapshot_id: String,
    pub changed_chunks: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentExport {
    pub manifest: SegmentManifest,
    pub chunk_payloads: Vec<ChunkPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPayload {
    pub meta: ChunkMeta,
    pub data: Bytes,
}

fn clean_path_part(part: &str) -> String {
    part.chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => ch,
        })
        .collect()
}
