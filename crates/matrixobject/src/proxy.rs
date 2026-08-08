use crate::local::LocalMatrixObjectStore;
use crate::meta::{MatrixObjectMetaService, PlacementPolicy};
use crate::replication::SegmentReplicationReport;
use crate::service::{
    MatrixObjectAdminService, MatrixObjectBlockService, MatrixObjectChunkService,
};
use crate::types::*;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyReadPolicy {
    PrimaryOnly,
    ReplicaPreferred { max_lag_versions: u64 },
    AnyServiceable { max_lag_versions: u64 },
}

impl Default for ProxyReadPolicy {
    fn default() -> Self {
        Self::PrimaryOnly
    }
}

#[derive(Debug, Clone)]
pub struct MatrixObjectProxyOptions {
    pub placement: PlacementPolicy,
    pub read_policy: ProxyReadPolicy,
    pub sync_secondary_count: usize,
    pub async_secondary_count: usize,
    pub async_replication_queue_depth: usize,
}

impl Default for MatrixObjectProxyOptions {
    fn default() -> Self {
        Self {
            placement: PlacementPolicy::default(),
            read_policy: ProxyReadPolicy::default(),
            sync_secondary_count: 0,
            async_secondary_count: 0,
            async_replication_queue_depth: 8192,
        }
    }
}

#[derive(Clone)]
pub struct MatrixObjectProxy {
    meta: MatrixObjectMetaService,
    stores: Arc<RwLock<BTreeMap<u64, Arc<LocalMatrixObjectStore>>>>,
    options: MatrixObjectProxyOptions,
    async_replication_tx: Option<mpsc::Sender<SegmentId>>,
    replication_counters: Arc<ProxyReplicationCounters>,
    pending_async_replication: Arc<Mutex<BTreeSet<SegmentId>>>,
    dirty_async_replication: Arc<Mutex<BTreeSet<SegmentId>>>,
    failure_detector: Arc<Mutex<ProxyFailureDetectorState>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProxyReplicationStatus {
    pub async_enabled: bool,
    pub queued_segments: u64,
    pub completed_segments: u64,
    pub failed_segments: u64,
    pub dropped_segments: u64,
    pub coalesced_segments: u64,
    pub rerun_segments: u64,
    pub in_flight_segments: u64,
}

#[derive(Debug, Default)]
struct ProxyReplicationCounters {
    queued_segments: AtomicU64,
    completed_segments: AtomicU64,
    failed_segments: AtomicU64,
    dropped_segments: AtomicU64,
    coalesced_segments: AtomicU64,
    rerun_segments: AtomicU64,
    in_flight_segments: AtomicU64,
}

#[derive(Debug)]
struct ProxyFailureDetectorState {
    enabled: bool,
    options: BackgroundFailureDetectorOptions,
    epoch: u64,
    runs: u64,
    failures: u64,
    last_started_at_micros: Option<u64>,
    last_finished_at_micros: Option<u64>,
    last_report: Option<NodeFailureDetectorReport>,
    last_error: Option<String>,
}

impl Default for ProxyFailureDetectorState {
    fn default() -> Self {
        Self {
            enabled: false,
            options: BackgroundFailureDetectorOptions::default(),
            epoch: 0,
            runs: 0,
            failures: 0,
            last_started_at_micros: None,
            last_finished_at_micros: None,
            last_report: None,
            last_error: None,
        }
    }
}

impl ProxyFailureDetectorState {
    fn status(&self) -> BackgroundFailureDetectorStatus {
        BackgroundFailureDetectorStatus {
            enabled: self.enabled,
            interval_micros: self.options.interval_micros,
            epoch: self.epoch,
            runs: self.runs,
            failures: self.failures,
            last_started_at_micros: self.last_started_at_micros,
            last_finished_at_micros: self.last_finished_at_micros,
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

impl ProxyReplicationCounters {
    fn snapshot(&self, async_enabled: bool) -> ProxyReplicationStatus {
        ProxyReplicationStatus {
            async_enabled,
            queued_segments: self.queued_segments.load(Ordering::Relaxed),
            completed_segments: self.completed_segments.load(Ordering::Relaxed),
            failed_segments: self.failed_segments.load(Ordering::Relaxed),
            dropped_segments: self.dropped_segments.load(Ordering::Relaxed),
            coalesced_segments: self.coalesced_segments.load(Ordering::Relaxed),
            rerun_segments: self.rerun_segments.load(Ordering::Relaxed),
            in_flight_segments: self.in_flight_segments.load(Ordering::Relaxed),
        }
    }
}

impl MatrixObjectProxy {
    pub fn new(meta: MatrixObjectMetaService, options: MatrixObjectProxyOptions) -> Self {
        let counters = Arc::new(ProxyReplicationCounters::default());
        let (async_replication_tx, async_replication_rx) = if options.async_secondary_count > 0 {
            let (tx, rx) = mpsc::channel(options.async_replication_queue_depth.max(1));
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let proxy = Self {
            meta,
            stores: Arc::new(RwLock::new(BTreeMap::new())),
            options,
            async_replication_tx,
            replication_counters: counters,
            pending_async_replication: Arc::new(Mutex::new(BTreeSet::new())),
            dirty_async_replication: Arc::new(Mutex::new(BTreeSet::new())),
            failure_detector: Arc::new(Mutex::new(ProxyFailureDetectorState::default())),
        };
        if let Some(rx) = async_replication_rx {
            proxy.spawn_async_replication_worker(rx);
        }
        proxy
    }

    pub async fn register_store(
        &self,
        node: NodeDescriptor,
        store: Arc<LocalMatrixObjectStore>,
    ) -> Result<()> {
        let node_id = node.node_id;
        self.meta.register_node(node).await?;
        self.stores.write().await.insert(node_id, store);
        Ok(())
    }

    pub async fn remove_store(&self, node_id: u64) -> Option<Arc<LocalMatrixObjectStore>> {
        self.stores.write().await.remove(&node_id)
    }

    pub async fn open_segment(&self, req: OpenSegmentRequest) -> Result<SegmentDescriptor> {
        let descriptor = self
            .meta
            .open_segment(
                req.segment_id.clone(),
                self.options.placement.clone(),
                req.create_if_missing,
            )
            .await?;
        if let Some(primary) = self.primary_store(&descriptor).await? {
            primary.open_segment(req).await?;
        }
        Ok(descriptor)
    }

    pub async fn write(&self, req: WriteRequest) -> Result<WriteResponse> {
        let descriptor = self.meta.get_segment(&req.segment_id).await?;
        let response = self
            .require_primary_store(&descriptor)
            .await?
            .write(req.clone())
            .await?;
        self.record_primary_version(&descriptor, response.open_version)
            .await?;
        self.replicate_after_primary_write(&req.segment_id).await?;
        Ok(response)
    }

    pub async fn raw_write(&self, req: RawSegmentWriteRequest) -> Result<RawSegmentWriteResponse> {
        let descriptor = self.meta.get_segment(&req.segment_id).await?;
        let segment_id = req.segment_id.clone();
        let response = self
            .require_primary_store(&descriptor)
            .await?
            .raw_write(req)
            .await?;
        self.record_primary_version(&descriptor, response.open_version)
            .await?;
        self.replicate_after_primary_write(&segment_id).await?;
        Ok(response)
    }

    pub async fn write_batch(&self, req: BatchWriteRequest) -> Result<BatchWriteResponse> {
        let mut responses = Vec::with_capacity(req.writes.len());
        let mut total_written = 0;
        for write in req.writes {
            let response = self.write(write).await?;
            total_written += response.written;
            responses.push(response);
        }
        Ok(BatchWriteResponse {
            responses,
            total_written,
        })
    }

    pub async fn read(&self, req: ReadRequest) -> Result<ReadResponse> {
        let descriptor = self.meta.get_segment(&req.segment_id).await?;
        self.read_store(&descriptor).await?.read(req).await
    }

    pub async fn raw_read(&self, req: RawSegmentReadRequest) -> Result<RawSegmentReadResponse> {
        let descriptor = self.meta.get_segment(&req.segment_id).await?;
        self.read_store(&descriptor).await?.raw_read(req).await
    }

    pub async fn readv(&self, req: ReadVectorRequest) -> Result<ReadVectorResponse> {
        let mut responses = Vec::with_capacity(req.reads.len());
        let mut total_read = 0;
        for read in req.reads {
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
        let descriptor = self.meta.get_segment(&req.segment_id).await?;
        self.require_primary_store(&descriptor)
            .await?
            .discard(req.clone())
            .await?;
        let primary_version = self
            .require_primary_store(&descriptor)
            .await?
            .stat_segment(&req.segment_id)
            .await?
            .open_version;
        self.record_primary_version(&descriptor, primary_version)
            .await?;
        self.replicate_after_primary_write(&req.segment_id).await
    }

    pub async fn create_snapshot(&self, segment_id: &SegmentId) -> Result<SnapshotRef> {
        let descriptor = self.meta.get_segment(segment_id).await?;
        let snapshot = self
            .require_primary_store(&descriptor)
            .await?
            .create_snapshot(segment_id)
            .await?;
        self.meta.record_snapshot(snapshot.clone()).await?;
        Ok(snapshot)
    }

    pub async fn rebalance(&self) -> Result<RebalancePlan> {
        self.meta.rebalance(self.options.placement.clone()).await
    }

    pub async fn drain_node(&self, node_id: u64) -> Result<RebalancePlan> {
        self.meta
            .drain_node(node_id, self.options.placement.clone())
            .await
    }

    pub async fn fail_node(&self, node_id: u64) -> Result<RebalancePlan> {
        self.meta
            .fail_node(node_id, self.options.placement.clone())
            .await
    }

    pub async fn sweep_node_failures(
        &self,
        policy: NodeFailureDetectorPolicy,
    ) -> Result<NodeFailureDetectorReport> {
        self.meta
            .sweep_node_failures(policy, self.options.placement.clone())
            .await
    }

    pub fn start_background_failure_detector(&self, options: BackgroundFailureDetectorOptions) {
        let epoch = {
            let mut state = self
                .failure_detector
                .lock()
                .expect("proxy failure detector mutex poisoned");
            state.enabled = true;
            state.options = options;
            state.epoch = state.epoch.saturating_add(1);
            state.last_error = None;
            state.epoch
        };
        let proxy = self.clone();
        tokio::spawn(async move {
            proxy.background_failure_detector_loop(epoch).await;
        });
    }

    pub fn stop_background_failure_detector(&self) {
        let mut state = self
            .failure_detector
            .lock()
            .expect("proxy failure detector mutex poisoned");
        state.enabled = false;
        state.epoch = state.epoch.saturating_add(1);
    }

    pub fn background_failure_detector_status(&self) -> BackgroundFailureDetectorStatus {
        self.failure_detector
            .lock()
            .expect("proxy failure detector mutex poisoned")
            .status()
    }

    pub async fn route_segment(&self, segment_id: &SegmentId) -> Result<SegmentDescriptor> {
        self.meta.get_segment(segment_id).await
    }

    pub fn replication_status(&self) -> ProxyReplicationStatus {
        self.replication_counters
            .snapshot(self.async_replication_tx.is_some())
    }

    pub async fn replicate_segment(
        &self,
        segment_id: &SegmentId,
    ) -> Result<SegmentReplicationReport> {
        self.replicate_segment_with_limit(segment_id, self.options.sync_secondary_count)
            .await
    }

    async fn replicate_segment_with_limit(
        &self,
        segment_id: &SegmentId,
        secondary_count: usize,
    ) -> Result<SegmentReplicationReport> {
        let descriptor = self.meta.get_segment(segment_id).await?;
        let primary = self.require_primary_store(&descriptor).await?;
        let export = primary.export_segment(segment_id).await?;
        let primary_version = export.manifest.open_version;
        let stores = self.stores.read().await;
        let mut replica_versions = Vec::new();
        for replica in descriptor
            .replicas
            .iter()
            .filter(|replica| replica.role == SegmentReplicaRole::Secondary)
            .take(secondary_count)
        {
            let Some(store) = stores.get(&replica.node_id).cloned() else {
                self.meta
                    .record_replica_failure(segment_id, replica.node_id)
                    .await?;
                continue;
            };
            match store.import_segment(export.clone()).await {
                Ok(response) => {
                    self.meta
                        .record_replica_version(segment_id, replica.node_id, response.open_version)
                        .await?;
                    replica_versions.push((replica.node_id, response.open_version));
                }
                Err(_) => {
                    self.meta
                        .record_replica_failure(segment_id, replica.node_id)
                        .await?;
                }
            }
        }
        let max_lag_versions = replica_versions
            .iter()
            .map(|(_, version)| primary_version.saturating_sub(*version))
            .max()
            .unwrap_or(0);
        Ok(SegmentReplicationReport {
            segment_id: segment_id.clone(),
            primary_version,
            replica_versions,
            max_lag_versions,
        })
    }

    async fn replicate_after_primary_write(&self, segment_id: &SegmentId) -> Result<()> {
        if self.options.sync_secondary_count > 0 {
            self.replicate_segment_with_limit(segment_id, self.options.sync_secondary_count)
                .await?;
        }
        if self.options.async_secondary_count > 0 {
            self.enqueue_async_replication(segment_id.clone());
        }
        Ok(())
    }

    fn enqueue_async_replication(&self, segment_id: SegmentId) {
        let Some(tx) = &self.async_replication_tx else {
            return;
        };
        {
            let mut pending = self
                .pending_async_replication
                .lock()
                .expect("proxy pending replication mutex poisoned");
            if !pending.insert(segment_id.clone()) {
                self.dirty_async_replication
                    .lock()
                    .expect("proxy dirty replication mutex poisoned")
                    .insert(segment_id);
                self.replication_counters
                    .coalesced_segments
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        match tx.try_send(segment_id.clone()) {
            Ok(()) => {
                self.replication_counters
                    .queued_segments
                    .fetch_add(1, Ordering::Relaxed);
                self.replication_counters
                    .in_flight_segments
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.pending_async_replication
                    .lock()
                    .expect("proxy pending replication mutex poisoned")
                    .remove(&segment_id);
                self.replication_counters
                    .dropped_segments
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn spawn_async_replication_worker(&self, mut rx: mpsc::Receiver<SegmentId>) {
        let proxy = self.clone();
        tokio::spawn(async move {
            while let Some(segment_id) = rx.recv().await {
                let result = proxy
                    .replicate_segment_with_limit(&segment_id, proxy.options.async_secondary_count)
                    .await;
                proxy
                    .pending_async_replication
                    .lock()
                    .expect("proxy pending replication mutex poisoned")
                    .remove(&segment_id);
                proxy
                    .replication_counters
                    .in_flight_segments
                    .fetch_sub(1, Ordering::Relaxed);
                let rerun = proxy
                    .dirty_async_replication
                    .lock()
                    .expect("proxy dirty replication mutex poisoned")
                    .remove(&segment_id);
                match result {
                    Ok(_) => {
                        proxy
                            .replication_counters
                            .completed_segments
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        proxy
                            .replication_counters
                            .failed_segments
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                if rerun {
                    proxy
                        .replication_counters
                        .rerun_segments
                        .fetch_add(1, Ordering::Relaxed);
                    proxy.enqueue_async_replication(segment_id);
                }
            }
        });
    }

    async fn background_failure_detector_loop(self, epoch: u64) {
        loop {
            let options = {
                let state = self
                    .failure_detector
                    .lock()
                    .expect("proxy failure detector mutex poisoned");
                if !state.enabled || state.epoch != epoch {
                    return;
                }
                state.options
            };
            tokio::time::sleep(Duration::from_micros(options.interval_micros.max(1))).await;
            {
                let mut state = self
                    .failure_detector
                    .lock()
                    .expect("proxy failure detector mutex poisoned");
                if !state.enabled || state.epoch != epoch {
                    return;
                }
                state.last_started_at_micros = Some(now_micros());
            }

            let result = self.sweep_node_failures(options.policy).await;
            let mut state = self
                .failure_detector
                .lock()
                .expect("proxy failure detector mutex poisoned");
            if !state.enabled || state.epoch != epoch {
                return;
            }
            state.last_finished_at_micros = Some(now_micros());
            match result {
                Ok(report) => {
                    state.runs = state.runs.saturating_add(1);
                    state.last_report = Some(report);
                    state.last_error = None;
                }
                Err(err) => {
                    state.failures = state.failures.saturating_add(1);
                    state.last_error = Some(err.to_string());
                }
            }
        }
    }

    async fn record_primary_version(
        &self,
        descriptor: &SegmentDescriptor,
        open_version: u64,
    ) -> Result<()> {
        if let Some(primary) = descriptor
            .replicas
            .iter()
            .find(|replica| replica.role == SegmentReplicaRole::Primary)
        {
            self.meta
                .record_replica_version(&descriptor.segment_id, primary.node_id, open_version)
                .await?;
        }
        Ok(())
    }

    async fn primary_store_for_segment(
        &self,
        segment_id: &SegmentId,
    ) -> Result<Arc<LocalMatrixObjectStore>> {
        let descriptor = self.meta.get_segment(segment_id).await?;
        self.require_primary_store(&descriptor).await
    }

    async fn first_store(&self) -> Result<Arc<LocalMatrixObjectStore>> {
        self.stores
            .read()
            .await
            .values()
            .next()
            .cloned()
            .ok_or_else(|| {
                MatrixObjectError::Replication("proxy has no registered stores".to_owned())
            })
    }

    async fn all_stores(&self) -> Vec<Arc<LocalMatrixObjectStore>> {
        self.stores.read().await.values().cloned().collect()
    }

    async fn require_primary_store(
        &self,
        descriptor: &SegmentDescriptor,
    ) -> Result<Arc<LocalMatrixObjectStore>> {
        self.primary_store(descriptor).await?.ok_or_else(|| {
            MatrixObjectError::Replication(format!(
                "segment {} has no primary replica",
                descriptor.segment_id
            ))
        })
    }

    async fn primary_store(
        &self,
        descriptor: &SegmentDescriptor,
    ) -> Result<Option<Arc<LocalMatrixObjectStore>>> {
        let Some(primary) = descriptor
            .replicas
            .iter()
            .find(|replica| replica.role == SegmentReplicaRole::Primary && replica.serviceable)
        else {
            return Ok(None);
        };
        Ok(self.stores.read().await.get(&primary.node_id).cloned())
    }

    async fn read_store(
        &self,
        descriptor: &SegmentDescriptor,
    ) -> Result<Arc<LocalMatrixObjectStore>> {
        match self.options.read_policy {
            ProxyReadPolicy::PrimaryOnly => self.require_primary_store(descriptor).await,
            ProxyReadPolicy::ReplicaPreferred { max_lag_versions } => {
                if let Some(store) = self
                    .serviceable_replica_store(descriptor, false, max_lag_versions)
                    .await?
                {
                    return Ok(store);
                }
                self.require_primary_store(descriptor).await
            }
            ProxyReadPolicy::AnyServiceable { max_lag_versions } => {
                if let Some(store) = self
                    .serviceable_replica_store(descriptor, true, max_lag_versions)
                    .await?
                {
                    return Ok(store);
                }
                self.require_primary_store(descriptor).await
            }
        }
    }

    async fn serviceable_replica_store(
        &self,
        descriptor: &SegmentDescriptor,
        include_primary: bool,
        max_lag_versions: u64,
    ) -> Result<Option<Arc<LocalMatrixObjectStore>>> {
        let stores = self.stores.read().await;
        for replica in descriptor
            .replicas
            .iter()
            .filter(|replica| replica.serviceable && replica.lag_versions <= max_lag_versions)
        {
            if !include_primary && replica.role == SegmentReplicaRole::Primary {
                continue;
            }
            if let Some(store) = stores.get(&replica.node_id) {
                if store.is_serviceable().await {
                    return Ok(Some(store.clone()));
                }
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl MatrixObjectAdminService for MatrixObjectProxy {
    async fn disk_status(&self) -> Result<DiskStatus> {
        let stores = self.all_stores().await;
        let mut approximate_used_bytes = 0u64;
        let mut serviceable = !stores.is_empty();
        for store in stores {
            let status = store.disk_status().await?;
            approximate_used_bytes =
                approximate_used_bytes.saturating_add(status.approximate_used_bytes);
            serviceable &= status.serviceable;
        }
        Ok(DiskStatus {
            disk_id: 0,
            root: PathBuf::from("matrixobject-proxy"),
            serviceable,
            approximate_used_bytes,
        })
    }

    async fn set_serviceable(&self, serviceable: bool) {
        for store in self.all_stores().await {
            store.set_serviceable(serviceable).await;
        }
    }

    async fn is_serviceable(&self) -> bool {
        let stores = self.all_stores().await;
        if stores.is_empty() {
            return false;
        }
        for store in stores {
            if store.is_serviceable().await {
                return true;
            }
        }
        false
    }

    async fn list_disks(&self) -> Vec<DiskDescriptor> {
        let mut disks = Vec::new();
        for store in self.all_stores().await {
            disks.extend(store.list_disks().await);
        }
        disks
    }

    async fn add_disk(&self, req: AddDiskRequest) -> Result<DiskDescriptor> {
        self.first_store().await?.add_disk(req).await
    }

    async fn remove_disk(&self, req: RemoveDiskRequest) -> Result<Option<DiskDescriptor>> {
        self.first_store().await?.remove_disk(req).await
    }

    async fn set_disk_power_status(
        &self,
        disk_id: u32,
        power_status: DiskPowerStatus,
    ) -> Result<DiskDescriptor> {
        self.first_store()
            .await?
            .set_disk_power_status(disk_id, power_status)
            .await
    }

    async fn set_disk_load_state(
        &self,
        disk_id: u32,
        load_state: DiskLoadState,
    ) -> Result<DiskLoadInfo> {
        self.first_store()
            .await?
            .set_disk_load_state(disk_id, load_state)
            .await
    }

    async fn get_disk_load_state(&self, disk_ids: &[u32]) -> Vec<DiskLoadInfo> {
        let mut out = Vec::new();
        for store in self.all_stores().await {
            out.extend(store.get_disk_load_state(disk_ids).await);
        }
        out
    }

    async fn set_background_throughput(&self, options: BackgroundThroughputOptions) {
        for store in self.all_stores().await {
            store.set_background_throughput(options).await;
        }
    }

    async fn background_throughput(&self) -> BackgroundThroughputOptions {
        match self.first_store().await {
            Ok(store) => store.background_throughput().await,
            Err(_) => BackgroundThroughputOptions::default(),
        }
    }

    async fn io_stats(&self) -> StoreIoStats {
        let mut out = StoreIoStats::default();
        for store in self.all_stores().await {
            let stats = store.io_stats();
            out.read_ops = out.read_ops.saturating_add(stats.read_ops);
            out.read_bytes = out.read_bytes.saturating_add(stats.read_bytes);
            out.write_ops = out.write_ops.saturating_add(stats.write_ops);
            out.write_bytes = out.write_bytes.saturating_add(stats.write_bytes);
            out.discard_ops = out.discard_ops.saturating_add(stats.discard_ops);
            out.discard_bytes = out.discard_bytes.saturating_add(stats.discard_bytes);
            out.cache_hits = out.cache_hits.saturating_add(stats.cache_hits);
            out.cache_misses = out.cache_misses.saturating_add(stats.cache_misses);
            out.range_read_ops = out.range_read_ops.saturating_add(stats.range_read_ops);
            out.range_read_bytes = out.range_read_bytes.saturating_add(stats.range_read_bytes);
            out.checksum_failures = out
                .checksum_failures
                .saturating_add(stats.checksum_failures);
            out.throttled_ops = out.throttled_ops.saturating_add(stats.throttled_ops);
            out.throttled_micros = out.throttled_micros.saturating_add(stats.throttled_micros);
        }
        out
    }

    async fn cache_stats(&self) -> CacheStats {
        let mut out = CacheStats::default();
        for store in self.all_stores().await {
            let stats = store.cache_stats();
            out.capacity_bytes = out.capacity_bytes.saturating_add(stats.capacity_bytes);
            out.used_bytes = out.used_bytes.saturating_add(stats.used_bytes);
            out.entry_count = out.entry_count.saturating_add(stats.entry_count);
            out.lru_len = out.lru_len.saturating_add(stats.lru_len);
            out.evictions = out.evictions.saturating_add(stats.evictions);
        }
        out
    }

    async fn clear_cache(&self) -> CacheStats {
        for store in self.all_stores().await {
            store.clear_cache();
        }
        self.cache_stats().await
    }

    async fn invalidate_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheStats> {
        let descriptor = self.meta.get_segment(segment_id).await?;
        let stores = self.stores.read().await;
        for replica in descriptor.replicas {
            if let Some(store) = stores.get(&replica.node_id) {
                store.invalidate_segment_cache(segment_id).await?;
            }
        }
        drop(stores);
        Ok(self.cache_stats().await)
    }

    async fn warm_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheWarmupReport> {
        self.primary_store_for_segment(segment_id)
            .await?
            .warm_segment_cache(segment_id)
            .await
    }

    async fn shared_store_stats(&self) -> SharedStoreStats {
        match self.first_store().await {
            Ok(store) => store.shared_store_stats(),
            Err(_) => SharedStoreStats::default(),
        }
    }

    async fn flush_shared_store(&self, timeout: Duration) -> Result<SharedStoreStats> {
        let mut out = SharedStoreStats::default();
        for store in self.all_stores().await {
            let stats = store.flush_shared_store(timeout).await?;
            out.enabled |= stats.enabled;
            out.mode = out.mode.or(stats.mode);
            out.enqueued_writes = out.enqueued_writes.saturating_add(stats.enqueued_writes);
            out.committed_writes = out.committed_writes.saturating_add(stats.committed_writes);
            out.failed_writes = out.failed_writes.saturating_add(stats.failed_writes);
            out.in_flight_writes = out.in_flight_writes.saturating_add(stats.in_flight_writes);
            out.enqueued_bytes = out.enqueued_bytes.saturating_add(stats.enqueued_bytes);
            out.committed_bytes = out.committed_bytes.saturating_add(stats.committed_bytes);
            out.failed_bytes = out.failed_bytes.saturating_add(stats.failed_bytes);
            out.last_error = out.last_error.or(stats.last_error);
        }
        Ok(out)
    }

    async fn scrub_store(&self) -> Result<Vec<SegmentScrubReport>> {
        let mut reports = Vec::new();
        for store in self.all_stores().await {
            reports.extend(store.scrub_store().await?);
        }
        Ok(reports)
    }

    async fn set_verify_checksums_on_read(&self, enabled: bool) {
        for store in self.all_stores().await {
            store.set_verify_checksums_on_read(enabled).await;
        }
    }

    async fn verify_checksums_on_read(&self) -> bool {
        match self.first_store().await {
            Ok(store) => store.verify_checksums_on_read().await,
            Err(_) => false,
        }
    }

    async fn set_runtime_flag(&self, req: SetRuntimeFlagRequest) {
        for store in self.all_stores().await {
            store.set_runtime_flag(req.clone()).await;
        }
    }

    async fn get_runtime_flag(&self, name: &str) -> GetRuntimeFlagResponse {
        match self.first_store().await {
            Ok(store) => store.get_runtime_flag(name).await,
            Err(_) => GetRuntimeFlagResponse {
                name: name.to_owned(),
                value: None,
                default_value: None,
            },
        }
    }

    async fn list_runtime_flags(&self) -> Vec<RuntimeFlag> {
        match self.first_store().await {
            Ok(store) => store.list_runtime_flags().await,
            Err(_) => Vec::new(),
        }
    }

    async fn notify_replicate(
        &self,
        req: NotifyReplicateRequest,
    ) -> Result<NotifyReplicateResponse> {
        self.primary_store_for_segment(&req.chunk.segment_id)
            .await?
            .notify_replicate(req)
            .await
    }

    async fn check_replicate_status(&self, task_ids: &[String]) -> CheckReplicateStatusResponse {
        let mut response = CheckReplicateStatusResponse {
            chunk_infos: Vec::new(),
        };
        for store in self.all_stores().await {
            let store_response = store.check_replicate_status(task_ids).await;
            response.chunk_infos.extend(store_response.chunk_infos);
        }
        response
    }

    async fn cancel_replicate(&self, task_ids: &[String], force: bool) -> CancelReplicateResponse {
        let mut response = CancelReplicateResponse {
            cancelled: Vec::new(),
            not_found: Vec::new(),
        };
        for store in self.all_stores().await {
            let store_response = store.cancel_replicate(task_ids, force).await;
            response.cancelled.extend(store_response.cancelled);
            response.not_found.extend(store_response.not_found);
        }
        response
    }

    async fn list_recycle_bin(&self) -> Vec<RecycleBinEntry> {
        let mut entries = Vec::new();
        for store in self.all_stores().await {
            entries.extend(store.list_recycle_bin().await);
        }
        entries
    }

    async fn restore_recycle_bin(
        &self,
        req: RestoreRecycleBinRequest,
    ) -> Result<RestoreRecycleBinResponse> {
        let mut response = RestoreRecycleBinResponse {
            restored: Vec::new(),
            not_found: Vec::new(),
        };
        for store in self.all_stores().await {
            let store_response = store.restore_recycle_bin(req.clone()).await?;
            response.restored.extend(store_response.restored);
            response.not_found.extend(store_response.not_found);
        }
        Ok(response)
    }

    async fn recover_decommission(&self) -> Result<RecoverDecommissionResponse> {
        let mut response = RecoverDecommissionResponse {
            recovered_chunks: Vec::new(),
        };
        for store in self.all_stores().await {
            response
                .recovered_chunks
                .extend(store.recover_decommission().await?.recovered_chunks);
        }
        Ok(response)
    }

    async fn run_maintenance(&self, policy: MaintenancePolicy) -> Result<MaintenanceReport> {
        let mut out = MaintenanceReport::default();
        for store in self.all_stores().await {
            let report = store.run_maintenance(policy).await?;
            out.reclaimed_recycle_entries = out
                .reclaimed_recycle_entries
                .saturating_add(report.reclaimed_recycle_entries);
            out.reclaimed_recycle_bytes = out
                .reclaimed_recycle_bytes
                .saturating_add(report.reclaimed_recycle_bytes);
            out.trimmed_shared_store_records = out
                .trimmed_shared_store_records
                .saturating_add(report.trimmed_shared_store_records);
            out.trimmed_shared_store_bytes = out
                .trimmed_shared_store_bytes
                .saturating_add(report.trimmed_shared_store_bytes);
            out.compacted_oplogs = out.compacted_oplogs.saturating_add(report.compacted_oplogs);
        }
        Ok(out)
    }

    async fn start_background_maintenance(&self, options: BackgroundMaintenanceOptions) {
        for store in self.all_stores().await {
            store.start_background_maintenance(options).await;
        }
    }

    async fn stop_background_maintenance(&self) {
        for store in self.all_stores().await {
            store.stop_background_maintenance().await;
        }
    }

    async fn background_maintenance_status(&self) -> BackgroundMaintenanceStatus {
        match self.first_store().await {
            Ok(store) => store.background_maintenance_status().await,
            Err(_) => BackgroundMaintenanceStatus::default(),
        }
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[async_trait]
impl MatrixObjectBlockService for MatrixObjectProxy {
    async fn open_segment(&self, req: OpenSegmentRequest) -> Result<OpenSegmentResponse> {
        let segment_id = req.segment_id.clone();
        MatrixObjectProxy::open_segment(self, req).await?;
        let descriptor = self.meta.get_segment(&segment_id).await?;
        Ok(OpenSegmentResponse {
            open_version: descriptor.open_version,
            status: descriptor.status,
        })
    }

    async fn close_segment(&self, req: CloseSegmentRequest) -> Result<CloseSegmentResponse> {
        let store = self.primary_store_for_segment(&req.segment_id).await?;
        let response = store.close_segment(req.clone()).await?;
        self.meta.close_segment(&req.segment_id).await?;
        Ok(response)
    }

    async fn update_segment(&self, req: UpdateSegmentRequest) -> Result<OpenSegmentResponse> {
        self.primary_store_for_segment(&req.segment_id)
            .await?
            .update_segment(req)
            .await
    }

    async fn delete_segment(&self, segment_id: &SegmentId, delete_chunks: bool) -> Result<()> {
        if let Ok(store) = self.primary_store_for_segment(segment_id).await {
            store.delete_segment(segment_id, delete_chunks).await?;
        }
        self.meta.delete_segment(segment_id).await?;
        Ok(())
    }

    async fn clone_snapshot(
        &self,
        snapshot_id: &str,
        source_segment_id: &SegmentId,
        dest_segment_id: SegmentId,
    ) -> Result<OpenSegmentResponse> {
        self.primary_store_for_segment(source_segment_id)
            .await?
            .clone_snapshot(snapshot_id, source_segment_id, dest_segment_id)
            .await
    }

    async fn stat_segment(&self, segment_id: &SegmentId) -> Result<SegmentSpace> {
        self.primary_store_for_segment(segment_id)
            .await?
            .stat_segment(segment_id)
            .await
    }

    async fn list_segments(&self) -> Result<ListSegmentsResponse> {
        let sniff = self.meta.sniff_segments(None).await;
        let mut segments = Vec::new();
        let mut total_logical_size = 0;
        let mut total_physical_size = 0;
        for descriptor in sniff.segments {
            if let Ok(store) = self.require_primary_store(&descriptor).await {
                let segment = store.stat_segment(&descriptor.segment_id).await?;
                total_logical_size += segment.logical_space;
                total_physical_size += segment.physical_space;
                segments.push(segment);
            }
        }
        Ok(ListSegmentsResponse {
            segments,
            total_logical_size,
            total_physical_size,
        })
    }

    async fn create_snapshot(&self, segment_id: &SegmentId) -> Result<SnapshotRef> {
        MatrixObjectProxy::create_snapshot(self, segment_id).await
    }

    async fn delete_snapshot(&self, segment_id: &SegmentId, snapshot_id: &str) -> Result<()> {
        self.primary_store_for_segment(segment_id)
            .await?
            .delete_snapshot(segment_id, snapshot_id)
            .await
    }

    async fn rollback_snapshot(
        &self,
        snapshot_id: &str,
        segment_id: &SegmentId,
    ) -> Result<OpenSegmentResponse> {
        self.primary_store_for_segment(segment_id)
            .await?
            .rollback_snapshot(snapshot_id, segment_id)
            .await
    }

    async fn get_snapshot_info(
        &self,
        segment_id: &SegmentId,
        snapshot_id: &str,
    ) -> Result<SnapshotInfo> {
        self.primary_store_for_segment(segment_id)
            .await?
            .get_snapshot_info(segment_id, snapshot_id)
            .await
    }

    async fn list_snapshots(&self, segment_id: &SegmentId) -> Result<SnapshotListResponse> {
        self.primary_store_for_segment(segment_id)
            .await?
            .list_snapshots(segment_id)
            .await
    }

    async fn get_snapshot_diff(
        &self,
        segment_id: &SegmentId,
        old_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> Result<SnapshotDiff> {
        self.primary_store_for_segment(segment_id)
            .await?
            .get_snapshot_diff(segment_id, old_snapshot_id, new_snapshot_id)
            .await
    }

    async fn get_meta_diff(
        &self,
        segment_id: &SegmentId,
        old_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> Result<MetaDiff> {
        self.primary_store_for_segment(segment_id)
            .await?
            .get_meta_diff(segment_id, old_snapshot_id, new_snapshot_id)
            .await
    }

    async fn rebase_segment(
        &self,
        segment_id: &SegmentId,
        base_snapshot_id: &str,
    ) -> Result<MetaDiff> {
        self.primary_store_for_segment(segment_id)
            .await?
            .rebase_segment(segment_id, base_snapshot_id)
            .await
    }

    async fn read(&self, req: ReadRequest) -> Result<ReadResponse> {
        MatrixObjectProxy::read(self, req).await
    }

    async fn raw_read(&self, req: RawSegmentReadRequest) -> Result<RawSegmentReadResponse> {
        MatrixObjectProxy::raw_read(self, req).await
    }

    async fn readv(&self, req: ReadVectorRequest) -> Result<ReadVectorResponse> {
        MatrixObjectProxy::readv(self, req).await
    }

    async fn write(&self, req: WriteRequest) -> Result<WriteResponse> {
        MatrixObjectProxy::write(self, req).await
    }

    async fn raw_write(&self, req: RawSegmentWriteRequest) -> Result<RawSegmentWriteResponse> {
        MatrixObjectProxy::raw_write(self, req).await
    }

    async fn write_batch(&self, req: BatchWriteRequest) -> Result<BatchWriteResponse> {
        MatrixObjectProxy::write_batch(self, req).await
    }

    async fn discard(&self, req: DiscardRequest) -> Result<()> {
        MatrixObjectProxy::discard(self, req).await
    }
}

#[async_trait]
impl MatrixObjectChunkService for MatrixObjectProxy {
    async fn create_chunk(&self, req: CreateChunkRequest) -> Result<CreateChunkResponse> {
        let segment_id = req.segment_id.clone();
        let response = self
            .primary_store_for_segment(&req.segment_id)
            .await?
            .create_chunk(req)
            .await?;
        self.replicate_after_primary_write(&segment_id).await?;
        Ok(response)
    }

    async fn raw_write_chunk(&self, req: RawChunkWriteRequest) -> Result<RawChunkWriteResponse> {
        let segment_id = req.segment_id.clone();
        let response = self
            .primary_store_for_segment(&req.segment_id)
            .await?
            .raw_write_chunk(req)
            .await?;
        self.replicate_after_primary_write(&segment_id).await?;
        Ok(response)
    }

    async fn raw_read_chunk(&self, req: RawChunkReadRequest) -> Result<RawChunkReadResponse> {
        let descriptor = self.meta.get_segment(&req.segment_id).await?;
        self.read_store(&descriptor)
            .await?
            .raw_read_chunk(req)
            .await
    }

    async fn discard_chunk(&self, req: RawChunkDiscardRequest) -> Result<RawChunkWriteResponse> {
        let segment_id = req.segment_id.clone();
        let response = self
            .primary_store_for_segment(&req.segment_id)
            .await?
            .discard_chunk(req)
            .await?;
        self.replicate_after_primary_write(&segment_id).await?;
        Ok(response)
    }

    async fn sync_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ChunkMeta> {
        self.primary_store_for_segment(segment_id)
            .await?
            .sync_chunk(segment_id, chunk_index)
            .await
    }

    async fn freeze_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ChunkMeta> {
        let chunk = self
            .primary_store_for_segment(segment_id)
            .await?
            .freeze_chunk(segment_id, chunk_index)
            .await?;
        self.replicate_after_primary_write(segment_id).await?;
        Ok(chunk)
    }

    async fn hard_link_chunk(&self, req: HardLinkChunkRequest) -> Result<ChunkMeta> {
        let dest_segment_id = req.dest_segment_id.clone();
        let chunk = self
            .primary_store_for_segment(&req.dest_segment_id)
            .await?
            .hard_link_chunk(req)
            .await?;
        self.replicate_after_primary_write(&dest_segment_id).await?;
        Ok(chunk)
    }

    async fn set_chunk_flags(&self, req: SetChunkFlagsRequest) -> Result<ChunkMeta> {
        let segment_id = req.segment_id.clone();
        let chunk = self
            .primary_store_for_segment(&req.segment_id)
            .await?
            .set_chunk_flags(req)
            .await?;
        self.replicate_after_primary_write(&segment_id).await?;
        Ok(chunk)
    }

    async fn get_chunk_meta(&self, segment_id: &SegmentId) -> Result<Vec<ChunkMeta>> {
        self.primary_store_for_segment(segment_id)
            .await?
            .get_chunk_meta(segment_id)
            .await
    }

    async fn get_storage_meta(
        &self,
        segment_id: &SegmentId,
        chunk_index: u64,
    ) -> Result<StorageMeta> {
        self.primary_store_for_segment(segment_id)
            .await?
            .get_storage_meta(segment_id, chunk_index)
            .await
    }

    async fn scrub_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ScrubResult> {
        self.primary_store_for_segment(segment_id)
            .await?
            .scrub_chunk(segment_id, chunk_index)
            .await
    }

    async fn scrub_segment(&self, segment_id: &SegmentId) -> Result<SegmentScrubReport> {
        self.primary_store_for_segment(segment_id)
            .await?
            .scrub_segment(segment_id)
            .await
    }

    async fn collect_chunk_metas(&self) -> Result<Vec<ChunkMeta>> {
        let stores = self.stores.read().await;
        let mut chunks = Vec::new();
        for store in stores.values() {
            chunks.extend(store.collect_chunk_metas().await?);
        }
        Ok(chunks)
    }

    async fn collect_chunk_metas_after(
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

    async fn delete_chunks(
        &self,
        segment_id: &SegmentId,
        chunk_indices: &[u64],
    ) -> Result<Vec<ChunkDeleteResult>> {
        let response = self
            .primary_store_for_segment(segment_id)
            .await?
            .delete_chunks(segment_id, chunk_indices)
            .await?;
        self.replicate_after_primary_write(segment_id).await?;
        Ok(response)
    }

    async fn delete_stale_chunks(
        &self,
        segment_id: &SegmentId,
        stale_versions: &[StaleChunkVersion],
    ) -> Result<Vec<ChunkDeleteResult>> {
        let response = self
            .primary_store_for_segment(segment_id)
            .await?
            .delete_stale_chunks(segment_id, stale_versions)
            .await?;
        self.replicate_after_primary_write(segment_id).await?;
        Ok(response)
    }
}
