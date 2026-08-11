// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::pipeline::{
    decode_compressed, encode_compressed, MatrixObjectPipeline, PipelineRead, PipelineWrite,
};
use crate::types::*;
use bytes::Bytes;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct MatrixObjectConfig {
    pub root: PathBuf,
    pub disk_id: u32,
    pub chunk_size: u64,
    pub max_io_bytes: u64,
    pub compression: CompressionKind,
    pub read_cache_bytes: usize,
    pub verify_checksums_on_read: bool,
    pub storage_commit_mode: StorageCommitMode,
    pub shared_store_root: Option<PathBuf>,
    pub shared_store_queue_depth: usize,
    pub max_open_segments: Option<usize>,
    pub max_logical_bytes: Option<u64>,
    pub max_physical_bytes: Option<u64>,
}

impl MatrixObjectConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            disk_id: 0,
            chunk_size: 4 * 1024 * 1024,
            max_io_bytes: 64 * 1024 * 1024,
            compression: CompressionKind::None,
            read_cache_bytes: 256 * 1024 * 1024,
            verify_checksums_on_read: true,
            storage_commit_mode: StorageCommitMode::LocalOnly,
            shared_store_root: None,
            shared_store_queue_depth: 8192,
            max_open_segments: None,
            max_logical_bytes: None,
            max_physical_bytes: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalMatrixObjectStore {
    config: MatrixObjectConfig,
    pipeline: MatrixObjectPipeline,
    state: Arc<RwLock<BTreeMap<SegmentId, SegmentState>>>,
    read_cache: Arc<ChunkReadCache>,
    shared_writer: Option<SharedStoreWriter>,
    runtime: Arc<RwLock<RuntimeState>>,
    stats: Arc<StoreIoCounters>,
}

#[derive(Debug, Clone)]
struct SegmentState {
    manifest: SegmentManifest,
}

#[derive(Debug, Clone)]
struct RuntimeState {
    serviceable: bool,
    background_throughput: BackgroundThroughputOptions,
    flags: BTreeMap<String, String>,
    replication_tasks: BTreeMap<String, ReplicateChunkInfo>,
    disks: BTreeMap<u32, DiskDescriptor>,
    recycle_bin: BTreeMap<String, RecycleBinEntry>,
    verify_checksums_on_read: bool,
    maintenance_enabled: bool,
    maintenance_options: BackgroundMaintenanceOptions,
    maintenance_epoch: u64,
    maintenance_runs: u64,
    maintenance_failures: u64,
    maintenance_last_started_at_micros: Option<u64>,
    maintenance_last_finished_at_micros: Option<u64>,
    maintenance_last_report: Option<MaintenanceReport>,
    maintenance_last_error: Option<String>,
}

impl RuntimeState {
    fn new(config: &MatrixObjectConfig) -> Self {
        let mut flags = BTreeMap::new();
        flags.insert("allow_writes".to_owned(), "true".to_owned());
        flags.insert("allow_reads".to_owned(), "true".to_owned());
        flags.insert("allow_background_io".to_owned(), "true".to_owned());
        flags.insert("allow_shared_store_async".to_owned(), "true".to_owned());
        let mut disks = BTreeMap::new();
        disks.insert(
            config.disk_id,
            DiskDescriptor {
                disk_id: config.disk_id,
                root: config.root.clone(),
                media_type: DiskMediaType::Unknown,
                slot_id: None,
                block_device_path: None,
                power_status: DiskPowerStatus::Online,
                load_state: DiskLoadState::Done,
                load_started_at_micros: Some(now_micros()),
                load_cost_micros: Some(0),
                serviceable: true,
            },
        );
        Self {
            serviceable: true,
            background_throughput: BackgroundThroughputOptions::default(),
            flags,
            replication_tasks: BTreeMap::new(),
            disks,
            recycle_bin: BTreeMap::new(),
            verify_checksums_on_read: config.verify_checksums_on_read,
            maintenance_enabled: false,
            maintenance_options: BackgroundMaintenanceOptions::default(),
            maintenance_epoch: 0,
            maintenance_runs: 0,
            maintenance_failures: 0,
            maintenance_last_started_at_micros: None,
            maintenance_last_finished_at_micros: None,
            maintenance_last_report: None,
            maintenance_last_error: None,
        }
    }
}

#[derive(Debug, Default)]
struct StoreIoCounters {
    read_ops: AtomicU64,
    read_bytes: AtomicU64,
    write_ops: AtomicU64,
    write_bytes: AtomicU64,
    discard_ops: AtomicU64,
    discard_bytes: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    range_read_ops: AtomicU64,
    range_read_bytes: AtomicU64,
    checksum_failures: AtomicU64,
    throttled_ops: AtomicU64,
    throttled_micros: AtomicU64,
}

impl StoreIoCounters {
    fn snapshot(&self) -> StoreIoStats {
        StoreIoStats {
            read_ops: self.read_ops.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_ops: self.write_ops.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            discard_ops: self.discard_ops.load(Ordering::Relaxed),
            discard_bytes: self.discard_bytes.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            range_read_ops: self.range_read_ops.load(Ordering::Relaxed),
            range_read_bytes: self.range_read_bytes.load(Ordering::Relaxed),
            checksum_failures: self.checksum_failures.load(Ordering::Relaxed),
            throttled_ops: self.throttled_ops.load(Ordering::Relaxed),
            throttled_micros: self.throttled_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
struct SharedStoreWriter {
    root: PathBuf,
    mode: StorageCommitMode,
    tx: Option<mpsc::Sender<SharedStoreWrite>>,
    stats: Arc<SharedStoreCounters>,
}

#[derive(Debug, Clone)]
struct SharedStoreWrite {
    segment_id: SegmentId,
    offset: u64,
    data: Bytes,
    sequence_id: u64,
    open_version: u64,
    crc32: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedStoreOplogRecord {
    tenant_id: String,
    volume_id: String,
    segment_id: u64,
    offset: u64,
    length: u64,
    sequence_id: u64,
    open_version: u64,
    crc32: u32,
    payload_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedStoreCheckpointRecord {
    tenant_id: String,
    volume_id: String,
    segment_id: u64,
    open_version: u64,
    sequence_id: u64,
    created_at_micros: u64,
    export_path: String,
    crc32: u32,
}

#[derive(Debug, Default)]
struct SharedStoreCounters {
    enqueued_writes: AtomicU64,
    committed_writes: AtomicU64,
    failed_writes: AtomicU64,
    in_flight_writes: AtomicUsize,
    enqueued_bytes: AtomicU64,
    committed_bytes: AtomicU64,
    failed_bytes: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl SharedStoreCounters {
    fn snapshot(&self, enabled: bool, mode: StorageCommitMode) -> SharedStoreStats {
        SharedStoreStats {
            enabled,
            mode: Some(mode),
            enqueued_writes: self.enqueued_writes.load(Ordering::Relaxed),
            committed_writes: self.committed_writes.load(Ordering::Relaxed),
            failed_writes: self.failed_writes.load(Ordering::Relaxed),
            in_flight_writes: self.in_flight_writes.load(Ordering::Relaxed) as u64,
            enqueued_bytes: self.enqueued_bytes.load(Ordering::Relaxed),
            committed_bytes: self.committed_bytes.load(Ordering::Relaxed),
            failed_bytes: self.failed_bytes.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .lock()
                .expect("shared store last_error mutex poisoned")
                .clone(),
        }
    }

    fn record_enqueue(&self, bytes: u64) {
        self.enqueued_writes.fetch_add(1, Ordering::Relaxed);
        self.enqueued_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.in_flight_writes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_commit(&self, bytes: u64) {
        self.committed_writes.fetch_add(1, Ordering::Relaxed);
        self.committed_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.in_flight_writes.fetch_sub(1, Ordering::Relaxed);
    }

    fn record_failure(&self, bytes: u64, error: String) {
        self.failed_writes.fetch_add(1, Ordering::Relaxed);
        self.failed_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.in_flight_writes.fetch_sub(1, Ordering::Relaxed);
        *self
            .last_error
            .lock()
            .expect("shared store last_error mutex poisoned") = Some(error);
    }
}

impl SharedStoreWriter {
    fn open(config: &MatrixObjectConfig) -> Option<Self> {
        let root = config.shared_store_root.clone()?;
        let stats = Arc::new(SharedStoreCounters::default());
        match config.storage_commit_mode {
            StorageCommitMode::LocalOnly => None,
            StorageCommitMode::SharedStoreSync => Some(Self {
                root,
                mode: config.storage_commit_mode,
                tx: None,
                stats,
            }),
            StorageCommitMode::SharedStoreAsync => {
                let queue_depth = config.shared_store_queue_depth.max(1);
                let (tx, mut rx) = mpsc::channel::<SharedStoreWrite>(queue_depth);
                let worker_root = root.clone();
                let worker_stats = stats.clone();
                tokio::spawn(async move {
                    while let Some(write) = rx.recv().await {
                        let bytes = write.data.len() as u64;
                        match persist_shared_store_write(&worker_root, &write).await {
                            Ok(()) => worker_stats.record_commit(bytes),
                            Err(err) => worker_stats.record_failure(bytes, err.to_string()),
                        }
                    }
                });
                Some(Self {
                    root,
                    mode: config.storage_commit_mode,
                    tx: Some(tx),
                    stats,
                })
            }
        }
    }

    async fn commit(&self, write: SharedStoreWrite) -> Result<()> {
        let bytes = write.data.len() as u64;
        self.stats.record_enqueue(bytes);
        match self.mode {
            StorageCommitMode::LocalOnly => {
                self.stats.record_commit(bytes);
                Ok(())
            }
            StorageCommitMode::SharedStoreSync => {
                match persist_shared_store_write(&self.root, &write).await {
                    Ok(()) => {
                        self.stats.record_commit(bytes);
                        Ok(())
                    }
                    Err(err) => {
                        let message = err.to_string();
                        self.stats.record_failure(bytes, message);
                        Err(err)
                    }
                }
            }
            StorageCommitMode::SharedStoreAsync => {
                let Some(tx) = &self.tx else {
                    self.stats
                        .record_failure(bytes, "async shared store writer missing".to_owned());
                    return Err(MatrixObjectError::SharedStore(
                        "async shared store writer missing".to_owned(),
                    ));
                };
                if tx.try_send(write).is_err() {
                    self.stats
                        .record_failure(bytes, "shared store queue is full".to_owned());
                    return Err(MatrixObjectError::SharedStoreQueueFull);
                }
                Ok(())
            }
        }
    }

    fn stats(&self) -> SharedStoreStats {
        self.stats.snapshot(true, self.mode)
    }

    async fn flush(&self, timeout: Duration) -> Result<SharedStoreStats> {
        let started_at = now_micros();
        loop {
            if self.stats.in_flight_writes.load(Ordering::Relaxed) == 0 {
                return Ok(self.stats());
            }
            if now_micros().saturating_sub(started_at) >= timeout.as_micros() as u64 {
                return Err(MatrixObjectError::SharedStore(format!(
                    "timed out flushing shared store with {} writes still in flight",
                    self.stats.in_flight_writes.load(Ordering::Relaxed)
                )));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

#[derive(Debug)]
struct ChunkReadCache {
    capacity_bytes: usize,
    inner: Mutex<ChunkReadCacheState>,
    evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct ChunkReadCacheState {
    used_bytes: usize,
    entries: HashMap<String, Bytes>,
    lru: VecDeque<String>,
}

impl ChunkReadCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            inner: Mutex::new(ChunkReadCacheState::default()),
            evictions: AtomicU64::new(0),
        }
    }

    fn get(&self, key: &str) -> Option<Bytes> {
        if self.capacity_bytes == 0 {
            return None;
        }
        let mut inner = self.inner.lock().expect("chunk cache mutex poisoned");
        let value = inner.entries.get(key).cloned()?;
        inner.lru.retain(|old| old != key);
        inner.lru.push_back(key.to_owned());
        Some(value)
    }

    fn insert(&self, key: String, data: Bytes) {
        if self.capacity_bytes == 0 || data.len() > self.capacity_bytes {
            self.invalidate(&key);
            return;
        }

        let mut inner = self.inner.lock().expect("chunk cache mutex poisoned");
        if let Some(old) = inner.entries.remove(&key) {
            inner.used_bytes = inner.used_bytes.saturating_sub(old.len());
        }
        inner.lru.retain(|old| old != &key);
        inner.used_bytes += data.len();
        inner.entries.insert(key.clone(), data);
        inner.lru.push_back(key);

        while inner.used_bytes > self.capacity_bytes {
            let Some(victim) = inner.lru.pop_front() else {
                break;
            };
            if let Some(removed) = inner.entries.remove(&victim) {
                inner.used_bytes = inner.used_bytes.saturating_sub(removed.len());
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn invalidate(&self, key: &str) {
        let mut inner = self.inner.lock().expect("chunk cache mutex poisoned");
        if let Some(old) = inner.entries.remove(key) {
            inner.used_bytes = inner.used_bytes.saturating_sub(old.len());
        }
        inner.lru.retain(|old| old != key);
    }

    fn clear(&self) -> CacheStats {
        let mut inner = self.inner.lock().expect("chunk cache mutex poisoned");
        inner.entries.clear();
        inner.lru.clear();
        inner.used_bytes = 0;
        self.snapshot_locked(&inner)
    }

    fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().expect("chunk cache mutex poisoned");
        self.snapshot_locked(&inner)
    }

    fn snapshot_locked(&self, inner: &ChunkReadCacheState) -> CacheStats {
        CacheStats {
            capacity_bytes: self.capacity_bytes as u64,
            used_bytes: inner.used_bytes as u64,
            entry_count: inner.entries.len(),
            lru_len: inner.lru.len(),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

impl LocalMatrixObjectStore {
    pub async fn open(config: MatrixObjectConfig) -> Result<Self> {
        tokio::fs::create_dir_all(config.root.join("segments")).await?;
        tokio::fs::create_dir_all(config.root.join("snapshots")).await?;
        if let Some(root) = &config.shared_store_root {
            tokio::fs::create_dir_all(root.join("oplog")).await?;
        }
        let shared_writer = SharedStoreWriter::open(&config);
        let runtime = Arc::new(RwLock::new(RuntimeState::new(&config)));
        Ok(Self {
            pipeline: MatrixObjectPipeline::byte_store_with_compression(config.compression),
            read_cache: Arc::new(ChunkReadCache::new(config.read_cache_bytes)),
            shared_writer,
            config,
            state: Arc::new(RwLock::new(BTreeMap::new())),
            runtime,
            stats: Arc::new(StoreIoCounters::default()),
        })
    }

    pub async fn open_with_pipeline(
        config: MatrixObjectConfig,
        pipeline: MatrixObjectPipeline,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(config.root.join("segments")).await?;
        tokio::fs::create_dir_all(config.root.join("snapshots")).await?;
        if let Some(root) = &config.shared_store_root {
            tokio::fs::create_dir_all(root.join("oplog")).await?;
        }
        let shared_writer = SharedStoreWriter::open(&config);
        let runtime = Arc::new(RwLock::new(RuntimeState::new(&config)));
        Ok(Self {
            pipeline,
            read_cache: Arc::new(ChunkReadCache::new(config.read_cache_bytes)),
            shared_writer,
            config,
            state: Arc::new(RwLock::new(BTreeMap::new())),
            runtime,
            stats: Arc::new(StoreIoCounters::default()),
        })
    }

    pub async fn open_segment(&self, req: OpenSegmentRequest) -> Result<OpenSegmentResponse> {
        let mut state = self.state.write().await;
        let path = self.segment_manifest_path(&req.segment_id);
        if let Some(segment) = state.get(&req.segment_id) {
            check_open_version(
                &req.segment_id,
                segment.manifest.open_version,
                req.expected_open_version,
            )?;
            return Ok(OpenSegmentResponse {
                open_version: segment.manifest.open_version,
                status: segment.manifest.status,
            });
        }

        if path.exists() {
            let manifest = read_manifest(&path).await?;
            check_open_version(
                &req.segment_id,
                manifest.open_version,
                req.expected_open_version,
            )?;
            let resp = OpenSegmentResponse {
                open_version: manifest.open_version,
                status: manifest.status,
            };
            state.insert(req.segment_id, SegmentState { manifest });
            return Ok(resp);
        }

        if !req.create_if_missing {
            return Err(MatrixObjectError::SegmentNotFound(req.segment_id));
        }
        self.ensure_can_create_segment(&state, &req.segment_id)?;

        let manifest = SegmentManifest {
            segment_id: req.segment_id.clone(),
            status: SegmentStatus::Open,
            open_version: 1,
            logical_size: 0,
            chunk_size: self.config.chunk_size,
            compression: self.config.compression,
            chunks: Vec::new(),
        };
        self.persist_manifest(&manifest).await?;
        state.insert(req.segment_id, SegmentState { manifest });
        Ok(OpenSegmentResponse {
            open_version: 1,
            status: SegmentStatus::Open,
        })
    }

    pub async fn close_segment(&self, req: CloseSegmentRequest) -> Result<CloseSegmentResponse> {
        if req.sync {
            self.sync_segment(&req.segment_id).await?;
        }
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, &req.segment_id).await?;
        check_open_version(
            &req.segment_id,
            segment.manifest.open_version,
            req.open_version,
        )?;
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        Ok(CloseSegmentResponse {
            open_version: segment.manifest.open_version,
            status: segment.manifest.status,
        })
    }

    pub async fn update_segment(&self, req: UpdateSegmentRequest) -> Result<OpenSegmentResponse> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, &req.segment_id).await?;
        check_open_version(
            &req.segment_id,
            segment.manifest.open_version,
            req.open_version,
        )?;
        if let Some(status) = req.status {
            segment.manifest.status = status;
        }
        if let Some(logical_size) = req.logical_size {
            segment.manifest.logical_size = logical_size;
        }
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        Ok(OpenSegmentResponse {
            open_version: segment.manifest.open_version,
            status: segment.manifest.status,
        })
    }

    pub async fn write(&self, req: WriteRequest) -> Result<WriteResponse> {
        self.ensure_runtime_flag("allow_writes").await?;
        if req.data.len() as u64 > self.config.max_io_bytes {
            return Err(MatrixObjectError::RequestTooLarge(format!(
                "{} > {}",
                req.data.len(),
                self.config.max_io_bytes
            )));
        }
        validate_range(req.offset, req.data.len() as u64)?;

        let prepared = self
            .pipeline
            .prepare_write(PipelineWrite {
                segment_id: req.segment_id.clone(),
                offset: req.offset,
                data: req.data,
                attrs: Default::default(),
            })
            .await?;
        let crc32 = crc32(&prepared.data);
        self.throttle_background_write(&req.qos, prepared.data.len() as u64)
            .await;
        let committed_open_version = {
            let mut state = self.state.write().await;
            let admission_manifest = {
                let segment = self.segment_mut(&mut state, &req.segment_id).await?;
                if segment.manifest.status == SegmentStatus::Frozen {
                    return Err(MatrixObjectError::SegmentFrozen(req.segment_id.clone()));
                }
                check_open_version(
                    &req.segment_id,
                    segment.manifest.open_version,
                    req.open_version,
                )?;
                segment.manifest.clone()
            };
            self.ensure_can_write(
                &state,
                &admission_manifest,
                prepared.offset,
                prepared.data.len() as u64,
            )?;
            let segment = self.segment_mut(&mut state, &req.segment_id).await?;
            self.write_bytes(
                &mut segment.manifest,
                prepared.offset,
                &prepared.data,
                req.durability,
            )
            .await?;
            segment.manifest.logical_size = segment
                .manifest
                .logical_size
                .max(prepared.offset + prepared.data.len() as u64);
            segment.manifest.open_version += 1;
            self.persist_manifest(&segment.manifest).await?;
            segment.manifest.open_version
        };
        if let Some(shared_writer) = &self.shared_writer {
            shared_writer
                .commit(SharedStoreWrite {
                    segment_id: req.segment_id.clone(),
                    offset: prepared.offset,
                    data: prepared.data.clone(),
                    sequence_id: req.sequence_id,
                    open_version: committed_open_version,
                    crc32,
                })
                .await?;
        }
        self.stats.write_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .write_bytes
            .fetch_add(prepared.data.len() as u64, Ordering::Relaxed);

        Ok(WriteResponse {
            written: prepared.data.len() as u64,
            crc32,
            open_version: committed_open_version,
        })
    }

    pub async fn write_batch(&self, req: BatchWriteRequest) -> Result<BatchWriteResponse> {
        let mut writes = req.writes;
        if !req.ordered {
            writes.sort_by(|left, right| {
                left.segment_id
                    .cmp(&right.segment_id)
                    .then(left.offset.cmp(&right.offset))
            });
        }

        let mut responses = Vec::with_capacity(writes.len());
        let mut total_written = 0;
        for write in writes {
            let response = self.write(write).await?;
            total_written += response.written;
            responses.push(response);
        }

        Ok(BatchWriteResponse {
            responses,
            total_written,
        })
    }

    pub async fn raw_write(&self, req: RawSegmentWriteRequest) -> Result<RawSegmentWriteResponse> {
        self.ensure_runtime_flag("allow_writes").await?;
        if req.data.len() as u64 > self.config.max_io_bytes {
            return Err(MatrixObjectError::RequestTooLarge(format!(
                "{} > {}",
                req.data.len(),
                self.config.max_io_bytes
            )));
        }
        validate_range(req.offset, req.data.len() as u64)?;

        let crc32 = crc32(&req.data);
        self.throttle_background_write(&req.qos, req.data.len() as u64)
            .await;
        let committed_open_version = {
            let mut state = self.state.write().await;
            let admission_manifest = {
                let segment = self.segment_mut(&mut state, &req.segment_id).await?;
                if segment.manifest.status == SegmentStatus::Frozen {
                    return Err(MatrixObjectError::SegmentFrozen(req.segment_id.clone()));
                }
                check_open_version(
                    &req.segment_id,
                    segment.manifest.open_version,
                    req.open_version,
                )?;
                segment.manifest.clone()
            };
            self.ensure_can_write(
                &state,
                &admission_manifest,
                req.offset,
                req.data.len() as u64,
            )?;
            let segment = self.segment_mut(&mut state, &req.segment_id).await?;
            self.write_bytes(&mut segment.manifest, req.offset, &req.data, req.durability)
                .await?;
            segment.manifest.logical_size = segment
                .manifest
                .logical_size
                .max(req.offset + req.data.len() as u64);
            segment.manifest.open_version += 1;
            self.persist_manifest(&segment.manifest).await?;
            segment.manifest.open_version
        };
        if let Some(shared_writer) = &self.shared_writer {
            shared_writer
                .commit(SharedStoreWrite {
                    segment_id: req.segment_id.clone(),
                    offset: req.offset,
                    data: req.data.clone(),
                    sequence_id: req.sequence_id,
                    open_version: committed_open_version,
                    crc32,
                })
                .await?;
        }
        self.stats.write_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .write_bytes
            .fetch_add(req.data.len() as u64, Ordering::Relaxed);

        Ok(RawSegmentWriteResponse {
            written: req.data.len() as u64,
            crc32,
            open_version: committed_open_version,
        })
    }

    pub async fn read(&self, req: ReadRequest) -> Result<ReadResponse> {
        self.ensure_runtime_flag("allow_reads").await?;
        if req.length > self.config.max_io_bytes {
            return Err(MatrixObjectError::RequestTooLarge(format!(
                "{} > {}",
                req.length, self.config.max_io_bytes
            )));
        }
        validate_range(req.offset, req.length)?;

        let manifest = self.segment_manifest_snapshot(&req.segment_id).await?;
        check_open_version(&req.segment_id, manifest.open_version, req.open_version)?;

        let read_req = self
            .pipeline
            .prepare_read(PipelineRead {
                segment_id: req.segment_id.clone(),
                offset: req.offset,
                length: req.length,
                attrs: Default::default(),
            })
            .await?;

        let data = self
            .read_bytes(&manifest, read_req.offset, read_req.length)
            .await?;
        self.throttle_background_read(&req.qos, data.len() as u64)
            .await;
        let crc32 = crc32(&data);
        self.stats.read_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .read_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(ReadResponse {
            data: Bytes::from(data),
            crc32,
            open_version: manifest.open_version,
        })
    }

    pub async fn raw_read(&self, req: RawSegmentReadRequest) -> Result<RawSegmentReadResponse> {
        self.ensure_runtime_flag("allow_reads").await?;
        if req.length > self.config.max_io_bytes {
            return Err(MatrixObjectError::RequestTooLarge(format!(
                "{} > {}",
                req.length, self.config.max_io_bytes
            )));
        }
        validate_range(req.offset, req.length)?;

        let manifest = self.segment_manifest_snapshot(&req.segment_id).await?;
        check_open_version(&req.segment_id, manifest.open_version, req.open_version)?;
        let data = self.read_bytes(&manifest, req.offset, req.length).await?;
        self.throttle_background_read(&req.qos, data.len() as u64)
            .await;
        let crc32 = crc32(&data);
        self.stats.read_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .read_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(RawSegmentReadResponse {
            data: Bytes::from(data),
            crc32,
            open_version: manifest.open_version,
        })
    }

    pub async fn readv(&self, req: ReadVectorRequest) -> Result<ReadVectorResponse> {
        let mut reads = req.reads;
        if !req.ordered {
            reads.sort_by(|left, right| {
                left.segment_id
                    .cmp(&right.segment_id)
                    .then(left.offset.cmp(&right.offset))
            });
        }

        let mut responses = Vec::with_capacity(reads.len());
        let mut total_read = 0;
        for read in reads {
            let response = self.read(read).await?;
            total_read += response.data.len() as u64;
            responses.push(response);
        }

        Ok(ReadVectorResponse {
            responses,
            total_read,
        })
    }

    pub async fn discard(&self, req: DiscardRequest) -> Result<()> {
        validate_range(req.offset, req.length)?;
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, &req.segment_id).await?;
        if segment.manifest.status == SegmentStatus::Frozen {
            return Err(MatrixObjectError::SegmentFrozen(req.segment_id));
        }
        check_open_version(
            &req.segment_id,
            segment.manifest.open_version,
            req.open_version,
        )?;

        let zeros = vec![0u8; req.length as usize];
        self.write_bytes(
            &mut segment.manifest,
            req.offset,
            &zeros,
            WriteDurability::Async,
        )
        .await?;
        segment.manifest.open_version += 1;
        self.stats.discard_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .discard_bytes
            .fetch_add(req.length, Ordering::Relaxed);
        self.persist_manifest(&segment.manifest).await
    }

    pub async fn sync_segment(&self, segment_id: &SegmentId) -> Result<()> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        for chunk in &segment.manifest.chunks {
            let path = self.config.root.join(&chunk.physical_path);
            sync_file(&path, WriteDurability::SyncAll).await?;
        }
        self.persist_manifest(&segment.manifest).await
    }

    pub async fn create_chunk(&self, req: CreateChunkRequest) -> Result<CreateChunkResponse> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, &req.segment_id).await?;
        check_open_version(
            &req.segment_id,
            segment.manifest.open_version,
            req.open_version,
        )?;
        let chunk = ensure_chunk(self, &mut segment.manifest, req.chunk_index)
            .await?
            .clone();
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        Ok(CreateChunkResponse {
            chunk,
            open_version: segment.manifest.open_version,
        })
    }

    pub async fn raw_write_chunk(
        &self,
        req: RawChunkWriteRequest,
    ) -> Result<RawChunkWriteResponse> {
        self.ensure_runtime_flag("allow_writes").await?;
        if req.data.len() as u64 > self.config.max_io_bytes {
            return Err(MatrixObjectError::RequestTooLarge(format!(
                "{} > {}",
                req.data.len(),
                self.config.max_io_bytes
            )));
        }
        validate_range(req.offset, req.data.len() as u64)?;

        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, &req.segment_id).await?;
        if segment.manifest.status == SegmentStatus::Frozen {
            return Err(MatrixObjectError::SegmentFrozen(req.segment_id));
        }
        check_open_version(
            &req.segment_id,
            segment.manifest.open_version,
            req.open_version,
        )?;
        let chunk_size = segment.manifest.chunk_size;
        let compression = segment.manifest.compression;
        if req.offset + req.data.len() as u64 > chunk_size {
            return Err(MatrixObjectError::InvalidRange {
                offset: req.offset,
                length: req.data.len() as u64,
            });
        }

        let chunk = ensure_chunk(self, &mut segment.manifest, req.chunk_index).await?;
        let path = self.config.root.join(&chunk.physical_path);
        self.throttle_background_write(&req.qos, req.data.len() as u64)
            .await;
        let chunk_bytes = update_chunk_logical_slice(
            &path,
            compression,
            chunk_size,
            req.offset,
            &req.data,
            req.durability,
        )
        .await?;
        let crc32 = crc32(&chunk_bytes);
        chunk.crc32 = crc32;
        chunk.logical_len = chunk
            .logical_len
            .max(req.offset + req.data.len() as u64)
            .max((chunk_bytes.len() as u64).min(chunk_size));
        chunk.physical_len = file_len(&path).await?;
        chunk.version += 1;
        self.read_cache
            .insert(chunk.physical_path.clone(), chunk_bytes);
        let chunk = chunk.clone();
        segment.manifest.logical_size = segment
            .manifest
            .logical_size
            .max(chunk.logical_offset + chunk.logical_len);
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        self.stats.write_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .write_bytes
            .fetch_add(req.data.len() as u64, Ordering::Relaxed);
        Ok(RawChunkWriteResponse {
            chunk,
            written: req.data.len() as u64,
            crc32,
            open_version: segment.manifest.open_version,
        })
    }

    pub async fn raw_read_chunk(&self, req: RawChunkReadRequest) -> Result<RawChunkReadResponse> {
        self.ensure_runtime_flag("allow_reads").await?;
        validate_range(req.offset, req.length)?;
        let manifest = self.segment_manifest_snapshot(&req.segment_id).await?;
        check_open_version(&req.segment_id, manifest.open_version, req.open_version)?;
        if req.offset + req.length > manifest.chunk_size {
            return Err(MatrixObjectError::InvalidRange {
                offset: req.offset,
                length: req.length,
            });
        }
        let chunk = manifest
            .chunks
            .iter()
            .find(|chunk| chunk.chunk_index == req.chunk_index)
            .cloned()
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(req.segment_id.clone()))?;
        let data = self
            .read_chunk_range(&chunk, manifest.compression, req.offset, req.length)
            .await?
            .unwrap_or_default();
        self.throttle_background_read(&req.qos, data.len() as u64)
            .await;
        let crc32 = crc32(&data);
        self.stats.read_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .read_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(RawChunkReadResponse {
            chunk,
            data,
            crc32,
            open_version: manifest.open_version,
        })
    }

    pub async fn discard_chunk(
        &self,
        req: RawChunkDiscardRequest,
    ) -> Result<RawChunkWriteResponse> {
        self.stats.discard_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .discard_bytes
            .fetch_add(req.length, Ordering::Relaxed);
        let zeros = Bytes::from(vec![0u8; req.length as usize]);
        self.raw_write_chunk(RawChunkWriteRequest {
            segment_id: req.segment_id,
            chunk_index: req.chunk_index,
            offset: req.offset,
            data: zeros,
            durability: WriteDurability::Async,
            open_version: req.open_version,
            client: ClientDesc::default(),
            qos: QoSRequest::default(),
        })
        .await
    }

    pub async fn sync_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ChunkMeta> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        let chunk = segment
            .manifest
            .chunks
            .iter()
            .find(|chunk| chunk.chunk_index == chunk_index)
            .cloned()
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        let path = self.config.root.join(&chunk.physical_path);
        sync_file(&path, WriteDurability::SyncAll).await?;
        Ok(chunk)
    }

    pub async fn freeze_chunk(
        &self,
        segment_id: &SegmentId,
        chunk_index: u64,
    ) -> Result<ChunkMeta> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        let chunk = segment
            .manifest
            .chunks
            .iter_mut()
            .find(|chunk| chunk.chunk_index == chunk_index)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        chunk.frozen = true;
        chunk.version += 1;
        let chunk = chunk.clone();
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        Ok(chunk)
    }

    pub async fn set_chunk_flags(&self, req: SetChunkFlagsRequest) -> Result<ChunkMeta> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, &req.segment_id).await?;
        check_open_version(
            &req.segment_id,
            segment.manifest.open_version,
            req.open_version,
        )?;
        let chunk = segment
            .manifest
            .chunks
            .iter_mut()
            .find(|chunk| chunk.chunk_index == req.chunk_index)
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(req.segment_id.clone()))?;
        chunk.flags = req.flags;
        chunk.version += 1;
        let chunk = chunk.clone();
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        Ok(chunk)
    }

    pub async fn hard_link_chunk(&self, req: HardLinkChunkRequest) -> Result<ChunkMeta> {
        let mut state = self.state.write().await;
        let source_manifest = self
            .segment_mut(&mut state, &req.source_segment_id)
            .await?
            .manifest
            .clone();
        let source_chunk = source_manifest
            .chunks
            .iter()
            .find(|chunk| chunk.chunk_index == req.source_chunk_index)
            .cloned()
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(req.source_segment_id.clone()))?;

        let dest = self.segment_mut(&mut state, &req.dest_segment_id).await?;
        check_open_version(
            &req.dest_segment_id,
            dest.manifest.open_version,
            req.open_version,
        )?;
        if let Some(pos) = dest
            .manifest
            .chunks
            .iter()
            .position(|chunk| chunk.chunk_index == req.dest_chunk_index)
        {
            let old = dest.manifest.chunks.remove(pos);
            self.read_cache.invalidate(&old.physical_path);
        }

        let mut linked = source_chunk;
        linked.chunk_id = Uuid::new_v4();
        linked.chunk_index = req.dest_chunk_index;
        linked.logical_offset = req.dest_chunk_index * dest.manifest.chunk_size;
        linked.physical_path =
            self.chunk_path(&req.dest_segment_id, req.dest_chunk_index, linked.chunk_id);
        linked.frozen = false;
        linked.version = 0;
        linked.flags = ChunkFlags::default();
        let src_path = self.config.root.join(
            source_manifest
                .chunks
                .iter()
                .find(|chunk| chunk.chunk_index == req.source_chunk_index)
                .expect("source chunk checked above")
                .physical_path
                .clone(),
        );
        let dest_path = self.config.root.join(&linked.physical_path);
        copy_or_link(&src_path, &dest_path).await?;
        if let Ok(data) = tokio::fs::read(&dest_path).await {
            self.read_cache
                .insert(linked.physical_path.clone(), Bytes::from(data));
        }

        dest.manifest.logical_size = dest
            .manifest
            .logical_size
            .max(linked.logical_offset + linked.logical_len);
        dest.manifest.open_version += 1;
        dest.manifest.chunks.push(linked.clone());
        dest.manifest.chunks.sort_by_key(|chunk| chunk.chunk_index);
        self.persist_manifest(&dest.manifest).await?;
        Ok(linked)
    }

    pub async fn freeze_segment(&self, segment_id: &SegmentId, sync: bool) -> Result<u64> {
        if sync {
            self.sync_segment(segment_id).await?;
        }
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        segment.manifest.status = SegmentStatus::Frozen;
        segment.manifest.open_version += 1;
        for chunk in &mut segment.manifest.chunks {
            chunk.frozen = true;
        }
        self.persist_manifest(&segment.manifest).await?;
        Ok(segment.manifest.open_version)
    }

    pub async fn delete_segment(&self, segment_id: &SegmentId, delete_chunks: bool) -> Result<()> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        segment.manifest.status = SegmentStatus::Deleted;
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        let chunks_to_delete = if delete_chunks {
            segment.manifest.chunks.clone()
        } else {
            Vec::new()
        };

        state.remove(segment_id);
        drop(state);

        for chunk in &chunks_to_delete {
            self.hard_delete_chunk_file(chunk).await?;
        }
        let manifest_path = self.segment_manifest_path(segment_id);
        if manifest_path.exists() {
            tokio::fs::remove_file(manifest_path).await?;
        }
        Ok(())
    }

    pub async fn delete_chunks(
        &self,
        segment_id: &SegmentId,
        chunk_indices: &[u64],
    ) -> Result<Vec<ChunkDeleteResult>> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        let mut results = Vec::with_capacity(chunk_indices.len());
        let mut chunks_to_delete = Vec::new();
        for chunk_index in chunk_indices {
            if let Some(pos) = segment
                .manifest
                .chunks
                .iter()
                .position(|chunk| chunk.chunk_index == *chunk_index)
            {
                let chunk = segment.manifest.chunks.remove(pos);
                let chunk_version = chunk.version;
                chunks_to_delete.push(chunk);
                results.push(ChunkDeleteResult {
                    chunk_index: *chunk_index,
                    chunk_version,
                    deleted: true,
                });
            } else {
                results.push(ChunkDeleteResult {
                    chunk_index: *chunk_index,
                    chunk_version: 0,
                    deleted: false,
                });
            }
        }
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        drop(state);

        for chunk in &chunks_to_delete {
            self.recycle_chunk_file(segment_id, chunk, "delete_chunks")
                .await?;
        }
        Ok(results)
    }

    pub async fn delete_stale_chunks(
        &self,
        segment_id: &SegmentId,
        stale_versions: &[StaleChunkVersion],
    ) -> Result<Vec<ChunkDeleteResult>> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        let mut results = Vec::with_capacity(stale_versions.len());
        let mut chunks_to_delete = Vec::new();
        for stale in stale_versions {
            if let Some(pos) = segment.manifest.chunks.iter().position(|chunk| {
                chunk.chunk_index == stale.chunk_index && chunk.version <= stale.max_delete_version
            }) {
                let chunk = segment.manifest.chunks.remove(pos);
                let chunk_version = chunk.version;
                chunks_to_delete.push(chunk);
                results.push(ChunkDeleteResult {
                    chunk_index: stale.chunk_index,
                    chunk_version,
                    deleted: true,
                });
            } else {
                results.push(ChunkDeleteResult {
                    chunk_index: stale.chunk_index,
                    chunk_version: 0,
                    deleted: false,
                });
            }
        }
        segment.manifest.open_version += 1;
        self.persist_manifest(&segment.manifest).await?;
        drop(state);

        for chunk in &chunks_to_delete {
            self.recycle_chunk_file(segment_id, chunk, "delete_stale_chunks")
                .await?;
        }
        Ok(results)
    }

    pub async fn create_snapshot(&self, segment_id: &SegmentId) -> Result<SnapshotRef> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        let snapshot_id = Uuid::new_v4().to_string();
        let snapshot_dir = self
            .config
            .root
            .join("snapshots")
            .join(segment_id.path_key())
            .join(&snapshot_id);
        tokio::fs::create_dir_all(snapshot_dir.join("chunks")).await?;

        let mut snapshot_manifest = segment.manifest.clone();
        for chunk in &mut snapshot_manifest.chunks {
            let src = self.config.root.join(&chunk.physical_path);
            let dest_rel = format!(
                "snapshots/{}/{}/chunks/{}.chunk",
                segment_id.path_key(),
                snapshot_id,
                chunk.chunk_id
            );
            let dest = self.config.root.join(&dest_rel);
            copy_or_link(&src, &dest).await?;
            chunk.physical_path = dest_rel;
            chunk.frozen = true;
        }

        let manifest_path = self.snapshot_manifest_path(segment_id, &snapshot_id);
        write_manifest_path(&manifest_path, &snapshot_manifest).await?;
        Ok(self.snapshot_ref(snapshot_id, &snapshot_manifest, manifest_path))
    }

    pub async fn clone_snapshot(
        &self,
        snapshot_id: &str,
        src_segment_id: &SegmentId,
        dest_segment_id: SegmentId,
    ) -> Result<OpenSegmentResponse> {
        let snapshot_manifest_path = self.snapshot_manifest_path(src_segment_id, snapshot_id);
        if !snapshot_manifest_path.exists() {
            return Err(MatrixObjectError::SnapshotNotFound(snapshot_id.to_string()));
        }

        let mut manifest = read_manifest(&snapshot_manifest_path).await?;
        manifest.segment_id = dest_segment_id.clone();
        manifest.status = SegmentStatus::Open;
        manifest.open_version = 1;
        for chunk in &mut manifest.chunks {
            let src = self.config.root.join(&chunk.physical_path);
            chunk.chunk_id = Uuid::new_v4();
            chunk.physical_path =
                self.chunk_path(&dest_segment_id, chunk.chunk_index, chunk.chunk_id);
            chunk.frozen = false;
            chunk.flags = ChunkFlags::default();
            let dest = self.config.root.join(&chunk.physical_path);
            copy_or_link(&src, &dest).await?;
        }

        self.persist_manifest(&manifest).await?;
        let mut state = self.state.write().await;
        state.insert(dest_segment_id, SegmentState { manifest });
        Ok(OpenSegmentResponse {
            open_version: 1,
            status: SegmentStatus::Open,
        })
    }

    pub async fn delete_snapshot(&self, segment_id: &SegmentId, snapshot_id: &str) -> Result<()> {
        let snapshot_dir = self
            .config
            .root
            .join("snapshots")
            .join(segment_id.path_key())
            .join(snapshot_id);
        if !snapshot_dir.exists() {
            return Err(MatrixObjectError::SnapshotNotFound(snapshot_id.to_owned()));
        }
        tokio::fs::remove_dir_all(snapshot_dir).await?;
        Ok(())
    }

    pub async fn list_snapshots(&self, segment_id: &SegmentId) -> Result<SnapshotListResponse> {
        let snapshot_root = self
            .config
            .root
            .join("snapshots")
            .join(segment_id.path_key());
        if !snapshot_root.exists() {
            return Ok(SnapshotListResponse {
                snapshots: Vec::new(),
            });
        }
        let mut snapshots = Vec::new();
        for entry in std::fs::read_dir(snapshot_root)? {
            let entry = entry?;
            if !entry.path().is_dir() {
                continue;
            }
            let snapshot_id = entry.file_name().to_string_lossy().to_string();
            let manifest_path = entry.path().join("manifest.json");
            if manifest_path.exists() {
                let manifest = read_manifest(&manifest_path).await?;
                snapshots.push(self.snapshot_ref(snapshot_id, &manifest, manifest_path));
            }
        }
        snapshots.sort_by(|left, right| {
            left.open_version
                .cmp(&right.open_version)
                .then(left.snapshot_id.cmp(&right.snapshot_id))
        });
        Ok(SnapshotListResponse { snapshots })
    }

    pub async fn get_snapshot_info(
        &self,
        segment_id: &SegmentId,
        snapshot_id: &str,
    ) -> Result<SnapshotInfo> {
        let manifest_path = self.snapshot_manifest_path(segment_id, snapshot_id);
        if !manifest_path.exists() {
            return Err(MatrixObjectError::SnapshotNotFound(snapshot_id.to_owned()));
        }
        let manifest = read_manifest(&manifest_path).await?;
        let mut physical_size = 0;
        for chunk in &manifest.chunks {
            if let Ok(meta) = tokio::fs::metadata(self.config.root.join(&chunk.physical_path)).await
            {
                physical_size += meta.len();
            }
        }
        Ok(SnapshotInfo {
            reference: self.snapshot_ref(snapshot_id.to_owned(), &manifest, manifest_path),
            manifest,
            physical_size,
        })
    }

    pub async fn rollback_snapshot(
        &self,
        snapshot_id: &str,
        segment_id: &SegmentId,
    ) -> Result<OpenSegmentResponse> {
        let snapshot_manifest_path = self.snapshot_manifest_path(segment_id, snapshot_id);
        if !snapshot_manifest_path.exists() {
            return Err(MatrixObjectError::SnapshotNotFound(snapshot_id.to_owned()));
        }
        let snapshot_manifest = read_manifest(&snapshot_manifest_path).await?;
        let mut state = self.state.write().await;
        let current = self.segment_mut(&mut state, segment_id).await?;
        let old_chunks = current.manifest.chunks.clone();

        let mut manifest = snapshot_manifest;
        manifest.segment_id = segment_id.clone();
        manifest.status = SegmentStatus::Open;
        manifest.open_version = current.manifest.open_version + 1;
        for chunk in &mut manifest.chunks {
            let src = self.config.root.join(&chunk.physical_path);
            chunk.chunk_id = Uuid::new_v4();
            chunk.physical_path = self.chunk_path(segment_id, chunk.chunk_index, chunk.chunk_id);
            chunk.frozen = false;
            chunk.flags = ChunkFlags::default();
            let dest = self.config.root.join(&chunk.physical_path);
            copy_or_link(&src, &dest).await?;
        }
        current.manifest = manifest.clone();
        self.persist_manifest(&manifest).await?;
        drop(state);

        for old in &old_chunks {
            self.recycle_chunk_file(segment_id, old, "rollback_snapshot")
                .await?;
        }
        Ok(OpenSegmentResponse {
            open_version: manifest.open_version,
            status: manifest.status,
        })
    }

    pub async fn get_snapshot_diff(
        &self,
        segment_id: &SegmentId,
        old_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> Result<SnapshotDiff> {
        let old_manifest = read_manifest(&self.snapshot_manifest_path(segment_id, old_snapshot_id))
            .await
            .map_err(|_| MatrixObjectError::SnapshotNotFound(old_snapshot_id.to_owned()))?;
        let new_manifest = read_manifest(&self.snapshot_manifest_path(segment_id, new_snapshot_id))
            .await
            .map_err(|_| MatrixObjectError::SnapshotNotFound(new_snapshot_id.to_owned()))?;
        Ok(SnapshotDiff {
            segment_id: segment_id.clone(),
            old_snapshot_id: old_snapshot_id.to_owned(),
            new_snapshot_id: new_snapshot_id.to_owned(),
            changed_chunks: changed_chunk_indices(&old_manifest, &new_manifest),
        })
    }

    pub async fn get_meta_diff(
        &self,
        segment_id: &SegmentId,
        old_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> Result<MetaDiff> {
        let old_manifest = read_manifest(&self.snapshot_manifest_path(segment_id, old_snapshot_id))
            .await
            .map_err(|_| MatrixObjectError::SnapshotNotFound(old_snapshot_id.to_owned()))?;
        let new_manifest = read_manifest(&self.snapshot_manifest_path(segment_id, new_snapshot_id))
            .await
            .map_err(|_| MatrixObjectError::SnapshotNotFound(new_snapshot_id.to_owned()))?;
        Ok(meta_diff(segment_id.clone(), &old_manifest, &new_manifest))
    }

    pub async fn rebase_segment(
        &self,
        segment_id: &SegmentId,
        base_snapshot_id: &str,
    ) -> Result<MetaDiff> {
        let current = self.segment_manifest_snapshot(segment_id).await?;
        let base = read_manifest(&self.snapshot_manifest_path(segment_id, base_snapshot_id))
            .await
            .map_err(|_| MatrixObjectError::SnapshotNotFound(base_snapshot_id.to_owned()))?;
        Ok(meta_diff(segment_id.clone(), &base, &current))
    }

    pub async fn stat_segment(&self, segment_id: &SegmentId) -> Result<SegmentSpace> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        let mut physical_space = 0;
        for chunk in &segment.manifest.chunks {
            let path = self.config.root.join(&chunk.physical_path);
            if let Ok(meta) = tokio::fs::metadata(path).await {
                physical_space += meta.len();
            }
        }
        Ok(SegmentSpace {
            segment_id: segment_id.clone(),
            logical_space: segment.manifest.logical_size,
            physical_space,
            chunk_count: segment.manifest.chunks.len(),
            open_version: segment.manifest.open_version,
            status: segment.manifest.status,
        })
    }

    pub async fn list_segments(&self) -> Result<ListSegmentsResponse> {
        self.load_persisted_manifests().await?;
        let segment_ids = {
            let state = self.state.read().await;
            state.keys().cloned().collect::<Vec<_>>()
        };
        let mut segments = Vec::with_capacity(segment_ids.len());
        let mut total_logical_size = 0;
        let mut total_physical_size = 0;
        for segment_id in segment_ids {
            let segment = self.stat_segment(&segment_id).await?;
            total_logical_size += segment.logical_space;
            total_physical_size += segment.physical_space;
            segments.push(segment);
        }
        segments.sort_by(|left, right| left.segment_id.cmp(&right.segment_id));
        Ok(ListSegmentsResponse {
            segments,
            total_logical_size,
            total_physical_size,
        })
    }

    pub async fn get_chunk_meta(&self, segment_id: &SegmentId) -> Result<Vec<ChunkMeta>> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        Ok(segment.manifest.chunks.clone())
    }

    pub fn cache_stats(&self) -> CacheStats {
        self.read_cache.stats()
    }

    pub fn clear_cache(&self) -> CacheStats {
        self.read_cache.clear()
    }

    pub async fn invalidate_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheStats> {
        let manifest = self.segment_manifest_snapshot(segment_id).await?;
        for chunk in &manifest.chunks {
            self.read_cache.invalidate(&chunk.physical_path);
        }
        Ok(self.read_cache.stats())
    }

    pub async fn warm_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheWarmupReport> {
        let manifest = self.segment_manifest_snapshot(segment_id).await?;
        let mut warmed_chunks = 0usize;
        let mut warmed_bytes = 0u64;
        let mut skipped_chunks = 0usize;
        for chunk in &manifest.chunks {
            if self.read_cache.get(&chunk.physical_path).is_some() {
                skipped_chunks += 1;
                continue;
            }
            match self.read_chunk_cached(chunk, manifest.compression).await? {
                Some(data) => {
                    warmed_chunks += 1;
                    warmed_bytes = warmed_bytes.saturating_add(data.len() as u64);
                }
                None => skipped_chunks += 1,
            }
        }
        Ok(CacheWarmupReport {
            segment_id: segment_id.clone(),
            warmed_chunks,
            warmed_bytes,
            skipped_chunks,
        })
    }

    pub async fn collect_chunk_metas(&self) -> Result<Vec<ChunkMeta>> {
        let mut out = Vec::new();
        let state = self.state.read().await;
        for segment in state.values() {
            out.extend(segment.manifest.chunks.clone());
        }
        Ok(out)
    }

    pub async fn collect_chunk_metas_after(
        &self,
        last_chunk_index: Option<u64>,
        batch: usize,
    ) -> Result<Vec<ChunkMeta>> {
        let mut chunks = self.collect_chunk_metas().await?;
        chunks.sort_by_key(|chunk| chunk.chunk_index);
        if let Some(last) = last_chunk_index {
            chunks.retain(|chunk| chunk.chunk_index > last);
        }
        chunks.truncate(batch);
        Ok(chunks)
    }

    pub async fn get_storage_meta(
        &self,
        segment_id: &SegmentId,
        chunk_index: u64,
    ) -> Result<StorageMeta> {
        let manifest = self.segment_manifest_snapshot(segment_id).await?;
        let chunk = manifest
            .chunks
            .iter()
            .find(|chunk| chunk.chunk_index == chunk_index)
            .cloned()
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        let path = self.config.root.join(&chunk.physical_path);
        let exists = path.exists();
        let data = if exists {
            tokio::fs::read(&path).await?
        } else {
            Vec::new()
        };
        let logical = if exists {
            decode_compressed(manifest.compression, Bytes::from(data.clone()))?
        } else {
            Bytes::new()
        };
        Ok(StorageMeta {
            segment_id: segment_id.clone(),
            chunk_index,
            physical_path: chunk.physical_path,
            physical_bytes: if chunk.physical_len == 0 {
                data.len() as u64
            } else {
                chunk.physical_len
            },
            logical_bytes: chunk.logical_len,
            crc32: crc32(&logical),
            stored_crc32: chunk.crc32,
            version: chunk.version,
            exists,
        })
    }

    pub async fn scrub_chunk(
        &self,
        segment_id: &SegmentId,
        chunk_index: u64,
    ) -> Result<ScrubResult> {
        let manifest = self.segment_manifest_snapshot(segment_id).await?;
        let chunk = manifest
            .chunks
            .iter()
            .find(|chunk| chunk.chunk_index == chunk_index)
            .cloned()
            .ok_or_else(|| MatrixObjectError::SegmentNotFound(segment_id.clone()))?;
        Ok(self
            .scrub_chunk_meta(segment_id, manifest.compression, &chunk)
            .await)
    }

    pub async fn scrub_segment(&self, segment_id: &SegmentId) -> Result<SegmentScrubReport> {
        let manifest = self.segment_manifest_snapshot(segment_id).await?;
        let mut results = Vec::with_capacity(manifest.chunks.len());
        for chunk in &manifest.chunks {
            results.push(
                self.scrub_chunk_meta(segment_id, manifest.compression, chunk)
                    .await,
            );
        }
        let failed_chunks = results.iter().filter(|result| !result.ok).count();
        Ok(SegmentScrubReport {
            segment_id: segment_id.clone(),
            checked_chunks: results.len(),
            failed_chunks,
            results,
        })
    }

    pub async fn scrub_store(&self) -> Result<Vec<SegmentScrubReport>> {
        self.load_persisted_manifests().await?;
        let segment_ids = {
            let state = self.state.read().await;
            state.keys().cloned().collect::<Vec<_>>()
        };
        let mut reports = Vec::with_capacity(segment_ids.len());
        for segment_id in segment_ids {
            reports.push(self.scrub_segment(&segment_id).await?);
        }
        Ok(reports)
    }

    pub async fn export_segment(&self, segment_id: &SegmentId) -> Result<SegmentExport> {
        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        let manifest = segment.manifest.clone();
        let mut chunk_payloads = Vec::with_capacity(manifest.chunks.len());
        for chunk in &manifest.chunks {
            let path = self.config.root.join(&chunk.physical_path);
            let data = if path.exists() {
                Bytes::from(tokio::fs::read(path).await?)
            } else {
                Bytes::new()
            };
            chunk_payloads.push(ChunkPayload {
                meta: chunk.clone(),
                data,
            });
        }
        Ok(SegmentExport {
            manifest,
            chunk_payloads,
        })
    }

    pub async fn import_segment(&self, export: SegmentExport) -> Result<OpenSegmentResponse> {
        let mut manifest = export.manifest;
        for payload in export.chunk_payloads {
            let path = self.config.root.join(&payload.meta.physical_path);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let cache_key = payload.meta.physical_path.clone();
            let data = payload.data;
            let physical_len = data.len() as u64;
            tokio::fs::write(&path, &data).await?;
            let decoded = decode_compressed(manifest.compression, data)?;
            self.read_cache.insert(cache_key.clone(), decoded);
            if let Some(chunk) = manifest
                .chunks
                .iter_mut()
                .find(|chunk| chunk.physical_path == cache_key)
            {
                chunk.physical_len = physical_len;
            }
        }
        manifest.open_version += 1;
        self.persist_manifest(&manifest).await?;
        let response = OpenSegmentResponse {
            open_version: manifest.open_version,
            status: manifest.status,
        };
        let mut state = self.state.write().await;
        state.insert(manifest.segment_id.clone(), SegmentState { manifest });
        Ok(response)
    }

    pub async fn restore_segment_from_shared_store(
        &self,
        segment_id: SegmentId,
    ) -> Result<OpenSegmentResponse> {
        let root = self
            .config
            .shared_store_root
            .as_ref()
            .ok_or_else(|| MatrixObjectError::SharedStore("shared store root is unset".to_owned()))?
            .clone();
        let checkpoint = read_latest_shared_store_checkpoint(&root, &segment_id).await?;
        let mut records = read_shared_store_records(&root, &segment_id).await?;
        records.sort_by(|left, right| {
            left.open_version
                .cmp(&right.open_version)
                .then(left.sequence_id.cmp(&right.sequence_id))
        });
        if checkpoint.is_none() && records.is_empty() {
            return Err(MatrixObjectError::SegmentNotFound(segment_id));
        }

        let mut manifest = if let Some(checkpoint) = checkpoint {
            let export_bytes = tokio::fs::read(root.join(&checkpoint.export_path)).await?;
            let actual_crc32 = crc32(&export_bytes);
            if actual_crc32 != checkpoint.crc32 {
                return Err(MatrixObjectError::SharedStore(format!(
                    "checkpoint checksum mismatch for {}",
                    checkpoint.export_path
                )));
            }
            let export: SegmentExport = serde_json::from_slice(&export_bytes)?;
            let response = self.import_segment(export).await?;
            let mut manifest = self.segment_manifest_snapshot(&segment_id).await?;
            manifest.open_version = manifest.open_version.max(response.open_version);
            records.retain(|record| {
                (record.open_version, record.sequence_id)
                    > (checkpoint.open_version, checkpoint.sequence_id)
            });
            manifest
        } else {
            SegmentManifest {
                segment_id: segment_id.clone(),
                status: SegmentStatus::Open,
                open_version: 1,
                logical_size: 0,
                chunk_size: self.config.chunk_size,
                compression: self.config.compression,
                chunks: Vec::new(),
            }
        };

        for record in records {
            let payload = tokio::fs::read(root.join(&record.payload_path)).await?;
            if payload.len() as u64 != record.length {
                return Err(MatrixObjectError::SharedStore(format!(
                    "payload length mismatch for {}",
                    record.payload_path
                )));
            }
            let actual_crc32 = crc32(&payload);
            if actual_crc32 != record.crc32 {
                return Err(MatrixObjectError::SharedStore(format!(
                    "payload checksum mismatch for {}",
                    record.payload_path
                )));
            }
            self.write_bytes(
                &mut manifest,
                record.offset,
                &payload,
                WriteDurability::Async,
            )
            .await?;
            manifest.logical_size = manifest
                .logical_size
                .max(record.offset + payload.len() as u64);
            manifest.open_version = manifest.open_version.max(record.open_version);
        }

        manifest.open_version += 1;
        self.persist_manifest(&manifest).await?;
        let response = OpenSegmentResponse {
            open_version: manifest.open_version,
            status: manifest.status,
        };
        let mut state = self.state.write().await;
        state.insert(manifest.segment_id.clone(), SegmentState { manifest });
        Ok(response)
    }

    pub async fn disk_status(&self) -> Result<DiskStatus> {
        let state = self.state.read().await;
        let approximate_used_bytes = state
            .values()
            .map(|segment| {
                segment
                    .manifest
                    .chunks
                    .iter()
                    .map(|c| c.logical_len)
                    .sum::<u64>()
            })
            .sum();
        Ok(DiskStatus {
            disk_id: self.config.disk_id,
            root: self.config.root.clone(),
            serviceable: self
                .runtime
                .read()
                .await
                .disks
                .get(&self.config.disk_id)
                .map(|disk| {
                    disk.serviceable && matches!(disk.power_status, DiskPowerStatus::Online)
                })
                .unwrap_or(false),
            approximate_used_bytes,
        })
    }

    pub async fn list_disks(&self) -> Vec<DiskDescriptor> {
        self.runtime.read().await.disks.values().cloned().collect()
    }

    pub async fn add_disk(&self, req: AddDiskRequest) -> Result<DiskDescriptor> {
        tokio::fs::create_dir_all(req.root.join("segments")).await?;
        tokio::fs::create_dir_all(req.root.join("snapshots")).await?;
        let descriptor = DiskDescriptor {
            disk_id: req.disk_id,
            root: req.root,
            media_type: req.media_type,
            slot_id: req.slot_id,
            block_device_path: req.block_device_path,
            power_status: DiskPowerStatus::Online,
            load_state: DiskLoadState::Done,
            load_started_at_micros: Some(now_micros()),
            load_cost_micros: Some(0),
            serviceable: true,
        };
        self.runtime
            .write()
            .await
            .disks
            .insert(descriptor.disk_id, descriptor.clone());
        Ok(descriptor)
    }

    pub async fn remove_disk(&self, req: RemoveDiskRequest) -> Result<Option<DiskDescriptor>> {
        if req.disk_id == self.config.disk_id && !req.force {
            return Err(MatrixObjectError::SharedStore(
                "refusing to remove configured primary disk without force".to_owned(),
            ));
        }
        Ok(self.runtime.write().await.disks.remove(&req.disk_id))
    }

    pub async fn set_disk_power_status(
        &self,
        disk_id: u32,
        power_status: DiskPowerStatus,
    ) -> Result<DiskDescriptor> {
        let mut runtime = self.runtime.write().await;
        let disk = runtime
            .disks
            .get_mut(&disk_id)
            .ok_or(MatrixObjectError::NodeNotFound(disk_id as u64))?;
        disk.power_status = power_status;
        disk.serviceable = matches!(power_status, DiskPowerStatus::Online);
        Ok(disk.clone())
    }

    pub async fn set_disk_load_state(
        &self,
        disk_id: u32,
        load_state: DiskLoadState,
    ) -> Result<DiskLoadInfo> {
        let mut runtime = self.runtime.write().await;
        let disk = runtime
            .disks
            .get_mut(&disk_id)
            .ok_or(MatrixObjectError::NodeNotFound(disk_id as u64))?;
        let now = now_micros();
        if matches!(load_state, DiskLoadState::Preparing) {
            disk.load_started_at_micros = Some(now);
            disk.load_cost_micros = None;
        }
        if matches!(load_state, DiskLoadState::Done) {
            disk.load_cost_micros = disk
                .load_started_at_micros
                .map(|started| now.saturating_sub(started));
        }
        disk.load_state = load_state;
        Ok(DiskLoadInfo {
            disk_id,
            load_state: disk.load_state,
            started_at_micros: disk.load_started_at_micros,
            cost_micros: disk.load_cost_micros,
        })
    }

    pub async fn get_disk_load_state(&self, disk_ids: &[u32]) -> Vec<DiskLoadInfo> {
        let runtime = self.runtime.read().await;
        let disks = if disk_ids.is_empty() {
            runtime.disks.values().collect::<Vec<_>>()
        } else {
            disk_ids
                .iter()
                .filter_map(|disk_id| runtime.disks.get(disk_id))
                .collect::<Vec<_>>()
        };
        disks
            .into_iter()
            .map(|disk| DiskLoadInfo {
                disk_id: disk.disk_id,
                load_state: disk.load_state,
                started_at_micros: disk.load_started_at_micros,
                cost_micros: disk.load_cost_micros,
            })
            .collect()
    }

    pub async fn set_serviceable(&self, serviceable: bool) {
        let mut runtime = self.runtime.write().await;
        runtime.serviceable = serviceable;
        for disk in runtime.disks.values_mut() {
            disk.serviceable = serviceable && matches!(disk.power_status, DiskPowerStatus::Online);
        }
    }

    pub async fn is_serviceable(&self) -> bool {
        self.runtime.read().await.serviceable
    }

    pub async fn set_background_throughput(&self, options: BackgroundThroughputOptions) {
        self.runtime.write().await.background_throughput = options;
    }

    pub async fn background_throughput(&self) -> BackgroundThroughputOptions {
        self.runtime.read().await.background_throughput
    }

    pub fn io_stats(&self) -> StoreIoStats {
        self.stats.snapshot()
    }

    pub fn shared_store_stats(&self) -> SharedStoreStats {
        self.shared_writer
            .as_ref()
            .map(SharedStoreWriter::stats)
            .unwrap_or_default()
    }

    pub async fn flush_shared_store(&self, timeout: Duration) -> Result<SharedStoreStats> {
        match &self.shared_writer {
            Some(shared_writer) => shared_writer.flush(timeout).await,
            None => Ok(SharedStoreStats::default()),
        }
    }

    pub async fn set_verify_checksums_on_read(&self, enabled: bool) {
        self.runtime.write().await.verify_checksums_on_read = enabled;
    }

    pub async fn verify_checksums_on_read(&self) -> bool {
        self.runtime.read().await.verify_checksums_on_read
    }

    pub async fn set_runtime_flag(&self, req: SetRuntimeFlagRequest) {
        self.runtime.write().await.flags.insert(req.name, req.value);
    }

    pub async fn get_runtime_flag(&self, name: &str) -> GetRuntimeFlagResponse {
        let runtime = self.runtime.read().await;
        GetRuntimeFlagResponse {
            name: name.to_owned(),
            value: runtime.flags.get(name).cloned(),
            default_value: default_runtime_flag(name).map(ToOwned::to_owned),
        }
    }

    pub async fn list_runtime_flags(&self) -> Vec<RuntimeFlag> {
        self.runtime
            .read()
            .await
            .flags
            .iter()
            .map(|(name, value)| RuntimeFlag {
                name: name.clone(),
                value: value.clone(),
            })
            .collect()
    }

    pub async fn notify_replicate(
        &self,
        req: NotifyReplicateRequest,
    ) -> Result<NotifyReplicateResponse> {
        let chunk_meta = self
            .find_chunk_meta(&req.chunk.segment_id, req.chunk.chunk_index)
            .await;
        let (status, error_context) = match &chunk_meta {
            Ok(Some(chunk))
                if req
                    .expected_version
                    .map(|version| version == chunk.version)
                    .unwrap_or(true) =>
            {
                (ReplicateTaskStatus::Succeeded, None)
            }
            Ok(Some(chunk)) => (
                ReplicateTaskStatus::Failed,
                Some(format!(
                    "chunk version mismatch: expected {:?}, got {}",
                    req.expected_version, chunk.version
                )),
            ),
            Ok(None) => (
                ReplicateTaskStatus::Pending,
                Some("chunk metadata not present locally".to_owned()),
            ),
            Err(err) => (ReplicateTaskStatus::Failed, Some(err.to_string())),
        };
        let info = ReplicateChunkInfo {
            task_id: req.task_id.clone(),
            chunk: req.chunk,
            status,
            chunk_meta: chunk_meta.ok().flatten(),
            error_context,
        };
        self.runtime
            .write()
            .await
            .replication_tasks
            .insert(req.task_id.clone(), info);
        Ok(NotifyReplicateResponse {
            task_id: req.task_id,
            status,
        })
    }

    pub async fn check_replicate_status(
        &self,
        task_ids: &[String],
    ) -> CheckReplicateStatusResponse {
        let runtime = self.runtime.read().await;
        let chunk_infos = if task_ids.is_empty() {
            runtime.replication_tasks.values().cloned().collect()
        } else {
            task_ids
                .iter()
                .filter_map(|task_id| runtime.replication_tasks.get(task_id).cloned())
                .collect()
        };
        CheckReplicateStatusResponse { chunk_infos }
    }

    pub async fn cancel_replicate(
        &self,
        task_ids: &[String],
        force: bool,
    ) -> CancelReplicateResponse {
        let mut runtime = self.runtime.write().await;
        let mut cancelled = Vec::new();
        let mut not_found = Vec::new();
        let ids = if task_ids.is_empty() {
            runtime
                .replication_tasks
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        } else {
            task_ids.to_vec()
        };
        for task_id in ids {
            if let Some(info) = runtime.replication_tasks.get_mut(&task_id) {
                if force
                    || matches!(
                        info.status,
                        ReplicateTaskStatus::Pending | ReplicateTaskStatus::Running
                    )
                {
                    info.status = ReplicateTaskStatus::Cancelled;
                    info.error_context = Some("replication task cancelled".to_owned());
                    cancelled.push(task_id);
                }
            } else {
                not_found.push(task_id);
            }
        }
        CancelReplicateResponse {
            cancelled,
            not_found,
        }
    }

    pub async fn list_recycle_bin(&self) -> Vec<RecycleBinEntry> {
        self.runtime
            .read()
            .await
            .recycle_bin
            .values()
            .cloned()
            .collect()
    }

    pub async fn restore_recycle_bin(
        &self,
        req: RestoreRecycleBinRequest,
    ) -> Result<RestoreRecycleBinResponse> {
        let ids = if req.recycle_ids.is_empty() {
            self.runtime
                .read()
                .await
                .recycle_bin
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        } else {
            req.recycle_ids
        };
        let mut restored = Vec::new();
        let mut not_found = Vec::new();
        for recycle_id in ids {
            let entry = self.runtime.write().await.recycle_bin.remove(&recycle_id);
            let Some(entry) = entry else {
                not_found.push(recycle_id);
                continue;
            };

            let src = self.config.root.join(&entry.recycled_physical_path);
            let dest = self.config.root.join(&entry.original_physical_path);
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if src.exists() {
                tokio::fs::rename(&src, &dest).await?;
            }

            let mut state = self.state.write().await;
            let segment = self.segment_mut(&mut state, &entry.segment_id).await?;
            if let Some(pos) = segment
                .manifest
                .chunks
                .iter()
                .position(|chunk| chunk.chunk_index == entry.chunk.chunk_index)
            {
                segment.manifest.chunks[pos] = entry.chunk.clone();
            } else {
                segment.manifest.chunks.push(entry.chunk.clone());
                segment
                    .manifest
                    .chunks
                    .sort_by_key(|chunk| chunk.chunk_index);
            }
            segment.manifest.open_version += 1;
            self.persist_manifest(&segment.manifest).await?;
            drop(state);

            if let Ok(data) = tokio::fs::read(&dest).await {
                self.read_cache
                    .insert(entry.original_physical_path.clone(), Bytes::from(data));
            }
            restored.push(entry);
        }
        Ok(RestoreRecycleBinResponse {
            restored,
            not_found,
        })
    }

    pub async fn recover_decommission(&self) -> Result<RecoverDecommissionResponse> {
        let ids = self
            .runtime
            .read()
            .await
            .recycle_bin
            .values()
            .filter(|entry| entry.chunk.flags.contains(ChunkFlags::DECOMMISSIONING))
            .map(|entry| entry.recycle_id.clone())
            .collect::<Vec<_>>();
        let response = self
            .restore_recycle_bin(RestoreRecycleBinRequest {
                recycle_ids: ids,
                client: ClientDesc::default(),
            })
            .await?;
        Ok(RecoverDecommissionResponse {
            recovered_chunks: response
                .restored
                .into_iter()
                .map(|entry| entry.chunk)
                .collect(),
        })
    }

    pub async fn run_maintenance(&self, policy: MaintenancePolicy) -> Result<MaintenanceReport> {
        self.ensure_runtime_flag("allow_background_io").await?;
        let mut report = self.reclaim_recycle_bin(policy).await?;
        let shared_report = self.compact_shared_store_oplogs(policy).await?;
        report.trimmed_shared_store_records = shared_report.trimmed_shared_store_records;
        report.trimmed_shared_store_bytes = shared_report.trimmed_shared_store_bytes;
        report.compacted_oplogs = shared_report.compacted_oplogs;
        Ok(report)
    }

    pub async fn start_background_maintenance(&self, options: BackgroundMaintenanceOptions) {
        let epoch = {
            let mut runtime = self.runtime.write().await;
            runtime.maintenance_epoch = runtime.maintenance_epoch.saturating_add(1);
            runtime.maintenance_enabled = true;
            runtime.maintenance_options = options;
            runtime.maintenance_last_error = None;
            runtime.maintenance_epoch
        };
        let store = self.clone();
        tokio::spawn(async move {
            store.background_maintenance_loop(epoch).await;
        });
    }

    pub async fn stop_background_maintenance(&self) {
        let mut runtime = self.runtime.write().await;
        runtime.maintenance_enabled = false;
        runtime.maintenance_epoch = runtime.maintenance_epoch.saturating_add(1);
    }

    pub async fn background_maintenance_status(&self) -> BackgroundMaintenanceStatus {
        let runtime = self.runtime.read().await;
        BackgroundMaintenanceStatus {
            enabled: runtime.maintenance_enabled,
            interval_micros: runtime.maintenance_options.interval_micros,
            epoch: runtime.maintenance_epoch,
            runs: runtime.maintenance_runs,
            failures: runtime.maintenance_failures,
            last_started_at_micros: runtime.maintenance_last_started_at_micros,
            last_finished_at_micros: runtime.maintenance_last_finished_at_micros,
            last_report: runtime.maintenance_last_report.clone(),
            last_error: runtime.maintenance_last_error.clone(),
        }
    }

    async fn background_maintenance_loop(self, epoch: u64) {
        loop {
            let options = {
                let runtime = self.runtime.read().await;
                if !runtime.maintenance_enabled || runtime.maintenance_epoch != epoch {
                    return;
                }
                runtime.maintenance_options
            };
            let sleep_micros = options.interval_micros.max(1);
            tokio::time::sleep(Duration::from_micros(sleep_micros)).await;
            let should_run = {
                let mut runtime = self.runtime.write().await;
                if !runtime.maintenance_enabled || runtime.maintenance_epoch != epoch {
                    return;
                }
                runtime.maintenance_last_started_at_micros = Some(now_micros());
                true
            };
            if !should_run {
                continue;
            }
            let result = self.run_maintenance(options.policy).await;
            let mut runtime = self.runtime.write().await;
            if !runtime.maintenance_enabled || runtime.maintenance_epoch != epoch {
                return;
            }
            runtime.maintenance_last_finished_at_micros = Some(now_micros());
            match result {
                Ok(report) => {
                    runtime.maintenance_runs = runtime.maintenance_runs.saturating_add(1);
                    runtime.maintenance_last_report = Some(report);
                    runtime.maintenance_last_error = None;
                }
                Err(err) => {
                    runtime.maintenance_failures = runtime.maintenance_failures.saturating_add(1);
                    runtime.maintenance_last_error = Some(err.to_string());
                }
            }
        }
    }

    async fn reclaim_recycle_bin(&self, policy: MaintenancePolicy) -> Result<MaintenanceReport> {
        let cutoff = now_micros().saturating_sub(policy.recycle_grace_micros);
        let mut candidates = self
            .runtime
            .read()
            .await
            .recycle_bin
            .values()
            .filter(|entry| entry.deleted_at_micros <= cutoff)
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|entry| entry.deleted_at_micros);
        candidates.truncate(policy.max_recycle_entries_to_reclaim);

        let mut report = MaintenanceReport::default();
        for entry in candidates {
            let removed = self
                .runtime
                .write()
                .await
                .recycle_bin
                .remove(&entry.recycle_id);
            if removed.is_none() {
                continue;
            }
            let path = self.config.root.join(&entry.recycled_physical_path);
            if path.exists() {
                let len = tokio::fs::metadata(&path)
                    .await
                    .map(|meta| meta.len())
                    .unwrap_or(0);
                tokio::fs::remove_file(path).await?;
                report.reclaimed_recycle_bytes = report.reclaimed_recycle_bytes.saturating_add(len);
            }
            self.read_cache.invalidate(&entry.original_physical_path);
            self.read_cache.invalidate(&entry.recycled_physical_path);
            report.reclaimed_recycle_entries += 1;
        }
        Ok(report)
    }

    async fn compact_shared_store_oplogs(
        &self,
        policy: MaintenancePolicy,
    ) -> Result<MaintenanceReport> {
        let Some(root) = &self.config.shared_store_root else {
            return Ok(MaintenanceReport::default());
        };
        if policy.shared_store_max_records_per_segment == 0 {
            return Ok(MaintenanceReport::default());
        }
        let oplog_paths = collect_shared_store_oplog_paths(&root.join("oplog"))?;
        let mut report = MaintenanceReport::default();
        for oplog_path in oplog_paths {
            let bytes = tokio::fs::read(&oplog_path).await?;
            let mut records = Vec::new();
            for line in bytes.split(|byte| *byte == b'\n') {
                if line.is_empty() {
                    continue;
                }
                records.push(serde_json::from_slice::<SharedStoreOplogRecord>(line)?);
            }
            if records.len() <= policy.shared_store_max_records_per_segment {
                continue;
            }
            let keep = policy.shared_store_min_keep_records.max(
                policy
                    .shared_store_max_records_per_segment
                    .min(records.len()),
            );
            let split_at = records.len().saturating_sub(keep);
            let (old_records, kept_records) = records.split_at(split_at);
            if let Some(last_old) = old_records.last() {
                let segment_id = SegmentId::new(
                    last_old.tenant_id.clone(),
                    last_old.volume_id.clone(),
                    last_old.segment_id,
                );
                write_shared_store_checkpoint(
                    root,
                    &segment_id,
                    old_records,
                    self.config.chunk_size,
                    self.config.compression,
                )
                .await?;
            }
            for record in old_records {
                let payload_path = root.join(&record.payload_path);
                if payload_path.exists() {
                    let len = tokio::fs::metadata(&payload_path)
                        .await
                        .map(|meta| meta.len())
                        .unwrap_or(0);
                    tokio::fs::remove_file(payload_path).await?;
                    report.trimmed_shared_store_bytes =
                        report.trimmed_shared_store_bytes.saturating_add(len);
                }
            }
            let mut new_bytes = Vec::new();
            for record in kept_records {
                new_bytes.extend_from_slice(&serde_json::to_vec(record)?);
                new_bytes.push(b'\n');
            }
            replace_file_sync(&oplog_path, new_bytes, WriteDurability::SyncData).await?;
            report.trimmed_shared_store_records += old_records.len();
            report.compacted_oplogs += 1;
        }
        Ok(report)
    }

    async fn segment_mut<'a>(
        &self,
        state: &'a mut BTreeMap<SegmentId, SegmentState>,
        segment_id: &SegmentId,
    ) -> Result<&'a mut SegmentState> {
        if !state.contains_key(segment_id) {
            let path = self.segment_manifest_path(segment_id);
            if !path.exists() {
                return Err(MatrixObjectError::SegmentNotFound(segment_id.clone()));
            }
            let manifest = read_manifest(&path).await?;
            state.insert(segment_id.clone(), SegmentState { manifest });
        }
        Ok(state.get_mut(segment_id).expect("segment inserted above"))
    }

    async fn segment_manifest_snapshot(&self, segment_id: &SegmentId) -> Result<SegmentManifest> {
        {
            let state = self.state.read().await;
            if let Some(segment) = state.get(segment_id) {
                return Ok(segment.manifest.clone());
            }
        }

        let mut state = self.state.write().await;
        let segment = self.segment_mut(&mut state, segment_id).await?;
        Ok(segment.manifest.clone())
    }

    async fn load_persisted_manifests(&self) -> Result<()> {
        let manifest_paths = collect_manifest_paths(&self.config.root.join("segments"))?;
        let mut state = self.state.write().await;
        for path in manifest_paths {
            let manifest = read_manifest(&path).await?;
            state
                .entry(manifest.segment_id.clone())
                .or_insert(SegmentState { manifest });
        }
        Ok(())
    }

    fn ensure_can_create_segment(
        &self,
        state: &BTreeMap<SegmentId, SegmentState>,
        segment_id: &SegmentId,
    ) -> Result<()> {
        if let Some(max_open_segments) = self.config.max_open_segments {
            let open_segments = state
                .values()
                .filter(|segment| segment.manifest.status != SegmentStatus::Deleted)
                .count();
            if open_segments >= max_open_segments {
                return Err(MatrixObjectError::AdmissionControl(format!(
                    "segment limit reached while creating {segment_id}: {open_segments} >= {max_open_segments}"
                )));
            }
        }
        Ok(())
    }

    fn ensure_can_write(
        &self,
        state: &BTreeMap<SegmentId, SegmentState>,
        manifest: &SegmentManifest,
        offset: u64,
        write_len: u64,
    ) -> Result<()> {
        let new_segment_logical = manifest.logical_size.max(offset.saturating_add(write_len));
        let old_segment_physical = manifest_physical_bytes(manifest);
        let estimated_segment_physical = old_segment_physical.saturating_add(write_len);
        let mut total_logical = 0u64;
        let mut total_physical = 0u64;
        for segment in state.values() {
            if segment.manifest.status == SegmentStatus::Deleted {
                continue;
            }
            if segment.manifest.segment_id == manifest.segment_id {
                total_logical = total_logical.saturating_add(new_segment_logical);
                total_physical = total_physical.saturating_add(estimated_segment_physical);
            } else {
                total_logical = total_logical.saturating_add(segment.manifest.logical_size);
                total_physical =
                    total_physical.saturating_add(manifest_physical_bytes(&segment.manifest));
            }
        }

        if let Some(max_logical_bytes) = self.config.max_logical_bytes {
            if total_logical > max_logical_bytes {
                return Err(MatrixObjectError::AdmissionControl(format!(
                    "logical byte limit exceeded by write to {}: {} > {}",
                    manifest.segment_id, total_logical, max_logical_bytes
                )));
            }
        }
        if let Some(max_physical_bytes) = self.config.max_physical_bytes {
            if total_physical > max_physical_bytes {
                return Err(MatrixObjectError::AdmissionControl(format!(
                    "physical byte limit exceeded by write to {}: {} > {}",
                    manifest.segment_id, total_physical, max_physical_bytes
                )));
            }
        }
        Ok(())
    }

    async fn throttle_background_read(&self, qos: &QoSRequest, bytes: u64) {
        let rate = if qos.priority == IoPriority::Background {
            self.runtime
                .read()
                .await
                .background_throughput
                .read_bytes_per_sec
        } else {
            None
        };
        self.throttle_background(rate, bytes).await;
    }

    async fn throttle_background_write(&self, qos: &QoSRequest, bytes: u64) {
        let rate = if qos.priority == IoPriority::Background {
            self.runtime
                .read()
                .await
                .background_throughput
                .write_bytes_per_sec
        } else {
            None
        };
        self.throttle_background(rate, bytes).await;
    }

    async fn throttle_background(&self, rate: Option<u64>, bytes: u64) {
        if self
            .ensure_runtime_flag("allow_background_io")
            .await
            .is_err()
        {
            return;
        }
        let Some(rate) = rate else {
            return;
        };
        if rate == 0 || bytes == 0 {
            return;
        }
        let micros = bytes.saturating_mul(1_000_000) / rate;
        if micros == 0 {
            return;
        }
        self.stats.throttled_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .throttled_micros
            .fetch_add(micros, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_micros(micros)).await;
    }

    async fn ensure_runtime_flag(&self, name: &str) -> Result<()> {
        let runtime = self.runtime.read().await;
        let value = runtime
            .flags
            .get(name)
            .map(String::as_str)
            .or_else(|| default_runtime_flag(name))
            .unwrap_or("true");
        if matches!(value, "false" | "0" | "off" | "disabled") {
            return Err(MatrixObjectError::Pipeline {
                stage: "runtime_flag".to_owned(),
                message: format!("{name} is disabled"),
            });
        }
        Ok(())
    }

    async fn find_chunk_meta(
        &self,
        segment_id: &SegmentId,
        chunk_index: u64,
    ) -> Result<Option<ChunkMeta>> {
        let manifest = self.segment_manifest_snapshot(segment_id).await?;
        Ok(manifest
            .chunks
            .iter()
            .find(|chunk| chunk.chunk_index == chunk_index)
            .cloned())
    }

    async fn write_bytes(
        &self,
        manifest: &mut SegmentManifest,
        offset: u64,
        bytes: &[u8],
        durability: WriteDurability,
    ) -> Result<()> {
        let mut remaining = bytes;
        let mut cursor = offset;
        while !remaining.is_empty() {
            let chunk_size = manifest.chunk_size;
            let compression = manifest.compression;
            let chunk_index = cursor / chunk_size;
            let in_chunk_offset = cursor % chunk_size;
            let write_len = remaining.len().min((chunk_size - in_chunk_offset) as usize);
            let chunk = ensure_chunk(self, manifest, chunk_index).await?;
            let path = self.config.root.join(&chunk.physical_path);
            let logical_chunk_bytes = update_chunk_logical_slice(
                &path,
                compression,
                chunk_size,
                in_chunk_offset,
                &remaining[..write_len],
                durability,
            )
            .await?;
            let chunk_crc32 = crc32(&logical_chunk_bytes);
            let chunk_len = logical_chunk_bytes.len() as u64;
            self.read_cache
                .insert(chunk.physical_path.clone(), logical_chunk_bytes);
            chunk.crc32 = chunk_crc32;
            chunk.logical_len = chunk
                .logical_len
                .max(in_chunk_offset + write_len as u64)
                .max(chunk_len.min(chunk_size));
            chunk.physical_len = file_len(&path).await?;
            chunk.version += 1;

            cursor += write_len as u64;
            remaining = &remaining[write_len..];
        }
        Ok(())
    }

    async fn read_bytes(
        &self,
        manifest: &SegmentManifest,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let mut out = vec![0u8; length as usize];
        if length == 0 {
            return Ok(out);
        }

        let mut remaining = length;
        let mut cursor = offset;
        let mut out_pos = 0usize;
        while remaining > 0 {
            let chunk_index = cursor / manifest.chunk_size;
            let in_chunk_offset = cursor % manifest.chunk_size;
            let read_len = remaining.min(manifest.chunk_size - in_chunk_offset);
            if let Some(chunk) = manifest
                .chunks
                .iter()
                .find(|c| c.chunk_index == chunk_index)
            {
                if let Some(data) = self
                    .read_chunk_range(chunk, manifest.compression, in_chunk_offset, read_len)
                    .await?
                {
                    let copy_len = data.len().min(read_len as usize);
                    out[out_pos..out_pos + copy_len].copy_from_slice(&data[..copy_len]);
                }
            }
            cursor += read_len;
            remaining -= read_len;
            out_pos += read_len as usize;
        }
        Ok(out)
    }

    async fn read_chunk_range(
        &self,
        chunk: &ChunkMeta,
        compression: CompressionKind,
        offset: u64,
        length: u64,
    ) -> Result<Option<Bytes>> {
        if let Some(data) = self.read_cache.get(&chunk.physical_path) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            let available = data.len().saturating_sub(offset as usize);
            let copy_len = available.min(length as usize);
            return Ok(Some(Bytes::copy_from_slice(
                &data[offset as usize..offset as usize + copy_len],
            )));
        }
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

        if compression == CompressionKind::None && !self.verify_checksums_on_read().await {
            let path = self.config.root.join(&chunk.physical_path);
            if !path.exists() {
                return Ok(None);
            }
            let read_len = length.min(chunk.logical_len.saturating_sub(offset));
            if read_len == 0 {
                return Ok(Some(Bytes::new()));
            }
            let data = read_file_range(&path, offset, read_len).await?;
            self.stats.range_read_ops.fetch_add(1, Ordering::Relaxed);
            self.stats
                .range_read_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            return Ok(Some(data));
        }

        let data = self.read_chunk_uncached_full(chunk, compression).await?;
        let Some(data) = data else {
            return Ok(None);
        };
        self.read_cache
            .insert(chunk.physical_path.clone(), data.clone());
        let available = data.len().saturating_sub(offset as usize);
        let copy_len = available.min(length as usize);
        Ok(Some(Bytes::copy_from_slice(
            &data[offset as usize..offset as usize + copy_len],
        )))
    }

    async fn read_chunk_cached(
        &self,
        chunk: &ChunkMeta,
        compression: CompressionKind,
    ) -> Result<Option<Bytes>> {
        if let Some(data) = self.read_cache.get(&chunk.physical_path) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(data));
        }
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        let data = self.read_chunk_uncached_full(chunk, compression).await?;
        if let Some(data) = &data {
            self.read_cache
                .insert(chunk.physical_path.clone(), data.clone());
        }
        Ok(data)
    }

    async fn read_chunk_uncached_full(
        &self,
        chunk: &ChunkMeta,
        compression: CompressionKind,
    ) -> Result<Option<Bytes>> {
        let path = self.config.root.join(&chunk.physical_path);
        if !path.exists() {
            return Ok(None);
        }
        let encoded = Bytes::from(tokio::fs::read(path).await?);
        let data = decode_compressed(compression, encoded)?;
        if self.verify_checksums_on_read().await {
            let actual_crc32 = crc32(&data);
            if actual_crc32 != chunk.crc32 {
                self.stats.checksum_failures.fetch_add(1, Ordering::Relaxed);
                return Err(MatrixObjectError::Pipeline {
                    stage: "checksum".to_owned(),
                    message: format!(
                        "chunk {} checksum mismatch: expected {:08x}, got {:08x}",
                        chunk.chunk_index, chunk.crc32, actual_crc32
                    ),
                });
            }
            if data.len() < chunk.logical_len as usize {
                self.stats.checksum_failures.fetch_add(1, Ordering::Relaxed);
                return Err(MatrixObjectError::Pipeline {
                    stage: "checksum".to_owned(),
                    message: format!(
                        "chunk {} length mismatch: expected at least {}, got {}",
                        chunk.chunk_index,
                        chunk.logical_len,
                        data.len()
                    ),
                });
            }
        }
        Ok(Some(data))
    }

    async fn scrub_chunk_meta(
        &self,
        segment_id: &SegmentId,
        compression: CompressionKind,
        chunk: &ChunkMeta,
    ) -> ScrubResult {
        let path = self.config.root.join(&chunk.physical_path);
        if !path.exists() {
            return ScrubResult {
                segment_id: segment_id.clone(),
                chunk_index: chunk.chunk_index,
                ok: false,
                expected_crc32: chunk.crc32,
                actual_crc32: None,
                expected_logical_len: chunk.logical_len,
                actual_physical_len: None,
                error_context: Some("chunk file missing".to_owned()),
            };
        }
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let actual_physical_len = data.len() as u64;
                let decoded = match decode_compressed(compression, Bytes::from(data)) {
                    Ok(decoded) => decoded,
                    Err(err) => {
                        return ScrubResult {
                            segment_id: segment_id.clone(),
                            chunk_index: chunk.chunk_index,
                            ok: false,
                            expected_crc32: chunk.crc32,
                            actual_crc32: None,
                            expected_logical_len: chunk.logical_len,
                            actual_physical_len: Some(actual_physical_len),
                            error_context: Some(err.to_string()),
                        }
                    }
                };
                let actual_crc32 = crc32(&decoded);
                let actual_len = decoded.len() as u64;
                let ok = actual_crc32 == chunk.crc32 && actual_len >= chunk.logical_len;
                ScrubResult {
                    segment_id: segment_id.clone(),
                    chunk_index: chunk.chunk_index,
                    ok,
                    expected_crc32: chunk.crc32,
                    actual_crc32: Some(actual_crc32),
                    expected_logical_len: chunk.logical_len,
                    actual_physical_len: Some(actual_physical_len),
                    error_context: if ok {
                        None
                    } else {
                        Some("chunk checksum or length mismatch".to_owned())
                    },
                }
            }
            Err(err) => ScrubResult {
                segment_id: segment_id.clone(),
                chunk_index: chunk.chunk_index,
                ok: false,
                expected_crc32: chunk.crc32,
                actual_crc32: None,
                expected_logical_len: chunk.logical_len,
                actual_physical_len: None,
                error_context: Some(err.to_string()),
            },
        }
    }

    async fn hard_delete_chunk_file(&self, chunk: &ChunkMeta) -> Result<()> {
        self.read_cache.invalidate(&chunk.physical_path);
        let path = self.config.root.join(&chunk.physical_path);
        if path.exists() {
            tokio::fs::remove_file(path).await?;
        }
        Ok(())
    }

    async fn recycle_chunk_file(
        &self,
        segment_id: &SegmentId,
        chunk: &ChunkMeta,
        reason: &str,
    ) -> Result<RecycleBinEntry> {
        self.read_cache.invalidate(&chunk.physical_path);
        let recycle_id = Uuid::new_v4().to_string();
        let recycled_physical_path = format!(
            "recycle_bin/{}/{}/{}-{:016x}.chunk",
            segment_id.path_key(),
            recycle_id,
            chunk.chunk_id,
            chunk.chunk_index
        );
        let src = self.config.root.join(&chunk.physical_path);
        let dest = self.config.root.join(&recycled_physical_path);
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if src.exists() {
            tokio::fs::rename(&src, &dest).await?;
        }
        let entry = RecycleBinEntry {
            recycle_id: recycle_id.clone(),
            segment_id: segment_id.clone(),
            chunk: chunk.clone(),
            original_physical_path: chunk.physical_path.clone(),
            recycled_physical_path,
            deleted_at_micros: now_micros(),
            reason: reason.to_owned(),
        };
        self.runtime
            .write()
            .await
            .recycle_bin
            .insert(recycle_id, entry.clone());
        Ok(entry)
    }

    fn segment_manifest_path(&self, segment_id: &SegmentId) -> PathBuf {
        self.config
            .root
            .join("segments")
            .join(segment_id.path_key())
            .join("manifest.json")
    }

    fn snapshot_manifest_path(&self, segment_id: &SegmentId, snapshot_id: &str) -> PathBuf {
        self.config
            .root
            .join("snapshots")
            .join(segment_id.path_key())
            .join(snapshot_id)
            .join("manifest.json")
    }

    fn snapshot_ref(
        &self,
        snapshot_id: String,
        manifest: &SegmentManifest,
        manifest_path: PathBuf,
    ) -> SnapshotRef {
        SnapshotRef {
            snapshot_id,
            segment_id: manifest.segment_id.clone(),
            open_version: manifest.open_version,
            logical_size: manifest.logical_size,
            chunk_count: manifest.chunks.len(),
            manifest_path,
        }
    }

    fn chunk_path(&self, segment_id: &SegmentId, chunk_index: u64, chunk_id: Uuid) -> String {
        format!(
            "segments/{}/chunks/{:016x}-{}.chunk",
            segment_id.path_key(),
            chunk_index,
            chunk_id
        )
    }

    async fn persist_manifest(&self, manifest: &SegmentManifest) -> Result<()> {
        write_manifest_path(&self.segment_manifest_path(&manifest.segment_id), manifest).await
    }
}

async fn ensure_chunk<'a>(
    store: &LocalMatrixObjectStore,
    manifest: &'a mut SegmentManifest,
    chunk_index: u64,
) -> Result<&'a mut ChunkMeta> {
    if let Some(pos) = manifest
        .chunks
        .iter()
        .position(|chunk| chunk.chunk_index == chunk_index)
    {
        return Ok(&mut manifest.chunks[pos]);
    }

    let chunk_id = Uuid::new_v4();
    let physical_path = store.chunk_path(&manifest.segment_id, chunk_index, chunk_id);
    let path = store.config.root.join(&physical_path);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&path, &[]).await?;
    manifest.chunks.push(ChunkMeta {
        chunk_id,
        chunk_index,
        logical_offset: chunk_index * manifest.chunk_size,
        logical_len: 0,
        physical_len: 0,
        physical_path,
        version: 0,
        crc32: 0,
        frozen: false,
        flags: ChunkFlags::default(),
    });
    manifest.chunks.sort_by_key(|chunk| chunk.chunk_index);
    let pos = manifest
        .chunks
        .iter()
        .position(|chunk| chunk.chunk_index == chunk_index)
        .expect("chunk inserted above");
    Ok(&mut manifest.chunks[pos])
}

async fn update_chunk_logical_slice(
    path: &Path,
    compression: CompressionKind,
    chunk_size: u64,
    offset: u64,
    bytes: &[u8],
    durability: WriteDurability,
) -> Result<Bytes> {
    let mut logical = if path.exists() {
        let encoded = tokio::fs::read(path).await?;
        if encoded.is_empty() {
            Vec::new()
        } else {
            decode_compressed(compression, Bytes::from(encoded))?.to_vec()
        }
    } else {
        Vec::new()
    };
    let end = offset
        .checked_add(bytes.len() as u64)
        .ok_or(MatrixObjectError::InvalidRange {
            offset,
            length: bytes.len() as u64,
        })?;
    if end > chunk_size {
        return Err(MatrixObjectError::InvalidRange {
            offset,
            length: bytes.len() as u64,
        });
    }
    if logical.len() < end as usize {
        logical.resize(end as usize, 0);
    }
    logical[offset as usize..end as usize].copy_from_slice(bytes);
    let encoded = encode_compressed(compression, Bytes::from(logical.clone()))?;
    replace_file_sync(path, encoded.to_vec(), durability).await?;
    Ok(Bytes::from(logical))
}

async fn file_len(path: &Path) -> Result<u64> {
    Ok(tokio::fs::metadata(path).await.map(|meta| meta.len())?)
}

async fn read_file_range(path: &Path, offset: u64, length: u64) -> Result<Bytes> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<Bytes> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::OpenOptions::new().read(true).open(&path)?;
        let file_len = file.metadata()?.len();
        if offset >= file_len || length == 0 {
            return Ok(Bytes::new());
        }
        let read_len = length.min(file_len - offset) as usize;
        let mut out = vec![0u8; read_len];
        file.seek(SeekFrom::Start(offset))?;
        let mut filled = 0usize;
        while filled < read_len {
            let read = file.read(&mut out[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        out.truncate(filled);
        Ok(Bytes::from(out))
    })
    .await
    .expect("blocking range read panicked")
}

async fn replace_file_sync(path: &Path, bytes: Vec<u8>, durability: WriteDurability) -> Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        file.write_all(&bytes)?;
        match durability {
            WriteDurability::Async => {}
            WriteDurability::SyncData => file.sync_data()?,
            WriteDurability::SyncAll => file.sync_all()?,
        }
        Ok(())
    })
    .await
    .expect("blocking chunk replace panicked")
}

async fn sync_file(path: &Path, durability: WriteDurability) -> Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let file = std::fs::OpenOptions::new().read(true).open(&path)?;
        match durability {
            WriteDurability::Async => {}
            WriteDurability::SyncData => file.sync_data()?,
            WriteDurability::SyncAll => file.sync_all()?,
        }
        Ok(())
    })
    .await
    .expect("blocking file sync panicked")
}

async fn persist_shared_store_write(root: &Path, write: &SharedStoreWrite) -> Result<()> {
    let payload_rel = format!(
        "objects/{}/{:020}-{:020}.bin",
        write.segment_id.path_key(),
        write.open_version,
        write.sequence_id
    );
    let payload_path = root.join(&payload_rel);
    if let Some(parent) = payload_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&payload_path, &write.data).await?;
    sync_file(&payload_path, WriteDurability::SyncData).await?;

    let record = SharedStoreOplogRecord {
        tenant_id: write.segment_id.tenant_id.clone(),
        volume_id: write.segment_id.volume_id.clone(),
        segment_id: write.segment_id.segment_id,
        offset: write.offset,
        length: write.data.len() as u64,
        sequence_id: write.sequence_id,
        open_version: write.open_version,
        crc32: write.crc32,
        payload_path: payload_rel,
    };
    let mut record_bytes = serde_json::to_vec(&record)?;
    record_bytes.push(b'\n');

    let oplog_path = root
        .join("oplog")
        .join(write.segment_id.path_key())
        .join("writes.jsonl");
    if let Some(parent) = oplog_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    append_file_sync(&oplog_path, record_bytes, WriteDurability::SyncData).await
}

async fn read_shared_store_records(
    root: &Path,
    segment_id: &SegmentId,
) -> Result<Vec<SharedStoreOplogRecord>> {
    let oplog_path = root
        .join("oplog")
        .join(segment_id.path_key())
        .join("writes.jsonl");
    if !oplog_path.exists() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(oplog_path).await?;
    let mut records = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let record: SharedStoreOplogRecord = serde_json::from_slice(line)?;
        if record.tenant_id != segment_id.tenant_id
            || record.volume_id != segment_id.volume_id
            || record.segment_id != segment_id.segment_id
        {
            return Err(MatrixObjectError::SharedStore(format!(
                "oplog record belongs to a different segment: {}/{}/{}",
                record.tenant_id, record.volume_id, record.segment_id
            )));
        }
        records.push(record);
    }
    Ok(records)
}

async fn read_latest_shared_store_checkpoint(
    root: &Path,
    segment_id: &SegmentId,
) -> Result<Option<SharedStoreCheckpointRecord>> {
    let checkpoint_path = root
        .join("checkpoints")
        .join(segment_id.path_key())
        .join("checkpoint.json");
    if !checkpoint_path.exists() {
        return Ok(None);
    }
    let checkpoint: SharedStoreCheckpointRecord =
        serde_json::from_slice(&tokio::fs::read(checkpoint_path).await?)?;
    if checkpoint.tenant_id != segment_id.tenant_id
        || checkpoint.volume_id != segment_id.volume_id
        || checkpoint.segment_id != segment_id.segment_id
    {
        return Err(MatrixObjectError::SharedStore(format!(
            "checkpoint belongs to a different segment: {}/{}/{}",
            checkpoint.tenant_id, checkpoint.volume_id, checkpoint.segment_id
        )));
    }
    Ok(Some(checkpoint))
}

async fn write_shared_store_checkpoint(
    root: &Path,
    segment_id: &SegmentId,
    records: &[SharedStoreOplogRecord],
    chunk_size: u64,
    compression: CompressionKind,
) -> Result<SharedStoreCheckpointRecord> {
    let Some(last_record) = records.last() else {
        return Err(MatrixObjectError::SharedStore(
            "cannot checkpoint an empty oplog range".to_owned(),
        ));
    };

    let mut logical_chunks = BTreeMap::<u64, Vec<u8>>::new();
    let mut logical_size = 0u64;
    let mut open_version = 1u64;
    if let Some(existing) = read_latest_shared_store_checkpoint(root, segment_id).await? {
        let export_bytes = tokio::fs::read(root.join(&existing.export_path)).await?;
        if crc32(&export_bytes) != existing.crc32 {
            return Err(MatrixObjectError::SharedStore(format!(
                "checkpoint checksum mismatch for {}",
                existing.export_path
            )));
        }
        let export: SegmentExport = serde_json::from_slice(&export_bytes)?;
        logical_size = export.manifest.logical_size;
        open_version = export.manifest.open_version;
        for payload in export.chunk_payloads {
            let logical = decode_compressed(export.manifest.compression, payload.data)?;
            logical_chunks.insert(payload.meta.chunk_index, logical.to_vec());
        }
    }

    let mut sorted_records = records.to_vec();
    sorted_records.sort_by(|left, right| {
        left.open_version
            .cmp(&right.open_version)
            .then(left.sequence_id.cmp(&right.sequence_id))
    });
    for record in &sorted_records {
        let payload = tokio::fs::read(root.join(&record.payload_path)).await?;
        if payload.len() as u64 != record.length {
            return Err(MatrixObjectError::SharedStore(format!(
                "payload length mismatch for {}",
                record.payload_path
            )));
        }
        if crc32(&payload) != record.crc32 {
            return Err(MatrixObjectError::SharedStore(format!(
                "payload checksum mismatch for {}",
                record.payload_path
            )));
        }
        apply_logical_write(&mut logical_chunks, chunk_size, record.offset, &payload)?;
        logical_size = logical_size.max(record.offset + payload.len() as u64);
        open_version = open_version.max(record.open_version);
    }

    let mut chunks = Vec::new();
    let mut chunk_payloads = Vec::new();
    for (chunk_index, logical) in logical_chunks {
        let chunk_id = Uuid::new_v4();
        let encoded = encode_compressed(compression, Bytes::from(logical.clone()))?;
        let physical_path = format!(
            "segments/{}/chunks/{:016x}-{}.chunk",
            segment_id.path_key(),
            chunk_index,
            chunk_id
        );
        let meta = ChunkMeta {
            chunk_id,
            chunk_index,
            logical_offset: chunk_index * chunk_size,
            logical_len: logical.len() as u64,
            physical_len: encoded.len() as u64,
            physical_path,
            version: 1,
            crc32: crc32(&logical),
            frozen: false,
            flags: ChunkFlags::default(),
        };
        chunks.push(meta.clone());
        chunk_payloads.push(ChunkPayload {
            meta,
            data: encoded,
        });
    }
    chunks.sort_by_key(|chunk| chunk.chunk_index);
    chunk_payloads.sort_by_key(|payload| payload.meta.chunk_index);
    let export = SegmentExport {
        manifest: SegmentManifest {
            segment_id: segment_id.clone(),
            status: SegmentStatus::Open,
            open_version,
            logical_size,
            chunk_size,
            compression,
            chunks,
        },
        chunk_payloads,
    };
    let export_rel = format!(
        "checkpoints/{}/exports/{:020}-{:020}-{}.json",
        segment_id.path_key(),
        last_record.open_version,
        last_record.sequence_id,
        Uuid::new_v4()
    );
    let mut export_bytes = serde_json::to_vec(&export)?;
    export_bytes.push(b'\n');
    let export_crc32 = crc32(&export_bytes);
    let export_path = root.join(&export_rel);
    if let Some(parent) = export_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    replace_file_sync(&export_path, export_bytes, WriteDurability::SyncData).await?;

    let checkpoint = SharedStoreCheckpointRecord {
        tenant_id: segment_id.tenant_id.clone(),
        volume_id: segment_id.volume_id.clone(),
        segment_id: segment_id.segment_id,
        open_version: last_record.open_version,
        sequence_id: last_record.sequence_id,
        created_at_micros: now_micros(),
        export_path: export_rel,
        crc32: export_crc32,
    };
    let mut checkpoint_bytes = serde_json::to_vec_pretty(&checkpoint)?;
    checkpoint_bytes.push(b'\n');
    let checkpoint_path = root
        .join("checkpoints")
        .join(segment_id.path_key())
        .join("checkpoint.json");
    if let Some(parent) = checkpoint_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    replace_file_sync(
        &checkpoint_path,
        checkpoint_bytes,
        WriteDurability::SyncData,
    )
    .await?;
    Ok(checkpoint)
}

fn apply_logical_write(
    chunks: &mut BTreeMap<u64, Vec<u8>>,
    chunk_size: u64,
    offset: u64,
    payload: &[u8],
) -> Result<()> {
    let mut cursor = offset;
    let mut remaining = payload;
    while !remaining.is_empty() {
        let chunk_index = cursor / chunk_size;
        let in_chunk_offset = cursor % chunk_size;
        let write_len = remaining.len().min((chunk_size - in_chunk_offset) as usize);
        let chunk = chunks.entry(chunk_index).or_default();
        let end = in_chunk_offset as usize + write_len;
        if chunk.len() < end {
            chunk.resize(end, 0);
        }
        chunk[in_chunk_offset as usize..end].copy_from_slice(&remaining[..write_len]);
        cursor += write_len as u64;
        remaining = &remaining[write_len..];
    }
    Ok(())
}

async fn append_file_sync(path: &Path, bytes: Vec<u8>, durability: WriteDurability) -> Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        file.write_all(&bytes)?;
        match durability {
            WriteDurability::Async => {}
            WriteDurability::SyncData => file.sync_data()?,
            WriteDurability::SyncAll => file.sync_all()?,
        }
        Ok(())
    })
    .await
    .expect("blocking append panicked")
}

async fn copy_or_link(src: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::hard_link(src, dest).await {
        Ok(()) => Ok(()),
        Err(_) => {
            tokio::fs::copy(src, dest).await?;
            Ok(())
        }
    }
}

async fn read_manifest(path: &Path) -> Result<SegmentManifest> {
    let bytes = tokio::fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn collect_manifest_paths(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    collect_manifest_paths_inner(root, &mut paths)?;
    Ok(paths)
}

fn collect_manifest_paths_inner(path: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_manifest_paths_inner(&entry_path, paths)?;
        } else if entry_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "manifest.json")
        {
            paths.push(entry_path);
        }
    }
    Ok(())
}

fn collect_shared_store_oplog_paths(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    collect_shared_store_oplog_paths_inner(root, &mut paths)?;
    Ok(paths)
}

fn collect_shared_store_oplog_paths_inner(path: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_shared_store_oplog_paths_inner(&entry_path, paths)?;
        } else if entry_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "writes.jsonl")
        {
            paths.push(entry_path);
        }
    }
    Ok(())
}

fn manifest_physical_bytes(manifest: &SegmentManifest) -> u64 {
    manifest
        .chunks
        .iter()
        .map(|chunk| {
            if chunk.physical_len == 0 {
                chunk.logical_len
            } else {
                chunk.physical_len
            }
        })
        .sum()
}

fn changed_chunk_indices(old: &SegmentManifest, new: &SegmentManifest) -> Vec<u64> {
    let mut changed = Vec::new();
    let old_chunks = old
        .chunks
        .iter()
        .map(|chunk| {
            (
                chunk.chunk_index,
                (
                    chunk.version,
                    chunk.crc32,
                    chunk.logical_len,
                    chunk.physical_len,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let new_chunks = new
        .chunks
        .iter()
        .map(|chunk| {
            (
                chunk.chunk_index,
                (
                    chunk.version,
                    chunk.crc32,
                    chunk.logical_len,
                    chunk.physical_len,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for chunk_index in old_chunks.keys().chain(new_chunks.keys()) {
        if old_chunks.get(chunk_index) != new_chunks.get(chunk_index)
            && !changed.contains(chunk_index)
        {
            changed.push(*chunk_index);
        }
    }
    changed.sort_unstable();
    changed
}

fn meta_diff(segment_id: SegmentId, old: &SegmentManifest, new: &SegmentManifest) -> MetaDiff {
    let old_chunks = old
        .chunks
        .iter()
        .map(|chunk| chunk.chunk_index)
        .collect::<std::collections::BTreeSet<_>>();
    let new_chunks = new
        .chunks
        .iter()
        .map(|chunk| chunk.chunk_index)
        .collect::<std::collections::BTreeSet<_>>();
    let added_chunks = new_chunks.difference(&old_chunks).copied().collect();
    let removed_chunks = old_chunks.difference(&new_chunks).copied().collect();
    MetaDiff {
        segment_id,
        old_open_version: old.open_version,
        new_open_version: new.open_version,
        logical_size_changed: old.logical_size != new.logical_size,
        status_changed: old.status != new.status,
        added_chunks,
        removed_chunks,
        changed_chunks: changed_chunk_indices(old, new),
    }
}

fn default_runtime_flag(name: &str) -> Option<&'static str> {
    match name {
        "allow_writes" | "allow_reads" | "allow_background_io" | "allow_shared_store_async" => {
            Some("true")
        }
        _ => None,
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

async fn write_manifest_path(path: &Path, manifest: &SegmentManifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(manifest)?;
    tokio::fs::write(&tmp, bytes).await?;
    sync_file(&tmp, WriteDurability::SyncAll).await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

fn validate_range(offset: u64, length: u64) -> Result<()> {
    offset
        .checked_add(length)
        .ok_or(MatrixObjectError::InvalidRange { offset, length })?;
    Ok(())
}

fn check_open_version(segment_id: &SegmentId, expected: u64, actual: Option<u64>) -> Result<()> {
    if let Some(actual) = actual {
        if actual != expected {
            return Err(MatrixObjectError::StaleOpenVersion {
                segment_id: segment_id.clone(),
                expected,
                actual,
            });
        }
    }
    Ok(())
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn range_write_read_discard_snapshot_clone() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = MatrixObjectConfig::new(dir.path());
        config.chunk_size = 4;
        let store = LocalMatrixObjectStore::open(config).await.unwrap();
        let segment = SegmentId::new("tenant", "volume", 7);

        store
            .open_segment(OpenSegmentRequest {
                segment_id: segment.clone(),
                expected_open_version: None,
                create_if_missing: true,
                client: ClientDesc::default(),
            })
            .await
            .unwrap();

        store
            .write(WriteRequest {
                segment_id: segment.clone(),
                offset: 2,
                data: Bytes::from_static(b"abcdefgh"),
                durability: WriteDurability::SyncAll,
                sequence_id: 1,
                open_version: None,
                client: ClientDesc::default(),
                qos: QoSRequest::default(),
            })
            .await
            .unwrap();
        let read = store
            .read(ReadRequest {
                segment_id: segment.clone(),
                offset: 0,
                length: 12,
                sequence_id: 2,
                open_version: None,
                client: ClientDesc::default(),
                qos: QoSRequest::default(),
            })
            .await
            .unwrap();
        assert_eq!(&read.data[..], b"\0\0abcdefgh\0\0");

        store
            .discard(DiscardRequest {
                segment_id: segment.clone(),
                offset: 4,
                length: 3,
                sequence_id: 3,
                open_version: None,
            })
            .await
            .unwrap();
        let snapshot = store.create_snapshot(&segment).await.unwrap();
        let clone = SegmentId::new("tenant", "volume", 8);
        store
            .clone_snapshot(&snapshot.snapshot_id, &segment, clone.clone())
            .await
            .unwrap();
        let cloned = store
            .read(ReadRequest {
                segment_id: clone,
                offset: 0,
                length: 10,
                sequence_id: 4,
                open_version: None,
                client: ClientDesc::default(),
                qos: QoSRequest::default(),
            })
            .await
            .unwrap();
        assert_eq!(&cloned.data[..], b"\0\0ab\0\0\0fgh");
    }
}
