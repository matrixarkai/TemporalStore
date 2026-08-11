// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::local::LocalMatrixObjectStore;
use crate::types::*;
use bytes::Bytes;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadTarget {
    Primary,
    ReplicaPreferred,
    Any,
}

#[derive(Debug, Clone)]
pub struct MatrixObjectClientOptions {
    pub default_timeout: Duration,
    pub default_durability: WriteDurability,
    pub read_target: ReadTarget,
    pub max_in_flight: usize,
    pub client: ClientDesc,
    pub qos: QoSRequest,
}

impl Default for MatrixObjectClientOptions {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(3),
            default_durability: WriteDurability::Async,
            read_target: ReadTarget::Primary,
            max_in_flight: 1024,
            client: ClientDesc::default(),
            qos: QoSRequest::default(),
        }
    }
}

#[derive(Clone)]
pub struct MatrixObjectClient {
    primary: Arc<LocalMatrixObjectStore>,
    replicas: Arc<Vec<Arc<LocalMatrixObjectStore>>>,
    options: MatrixObjectClientOptions,
    in_flight: Arc<Semaphore>,
    next_sequence: Arc<AtomicU64>,
    next_replica: Arc<AtomicUsize>,
}

impl MatrixObjectClient {
    pub fn new(primary: Arc<LocalMatrixObjectStore>, options: MatrixObjectClientOptions) -> Self {
        let max_in_flight = options.max_in_flight.max(1);
        Self {
            primary,
            replicas: Arc::new(Vec::new()),
            options,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            next_sequence: Arc::new(AtomicU64::new(1)),
            next_replica: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_replicas(
        primary: Arc<LocalMatrixObjectStore>,
        replicas: Vec<Arc<LocalMatrixObjectStore>>,
        options: MatrixObjectClientOptions,
    ) -> Self {
        let max_in_flight = options.max_in_flight.max(1);
        Self {
            primary,
            replicas: Arc::new(replicas),
            options,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            next_sequence: Arc::new(AtomicU64::new(1)),
            next_replica: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn open_segment(&self, segment_id: SegmentId) -> Result<OpenSegmentResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.open_segment(OpenSegmentRequest {
            segment_id,
            expected_open_version: None,
            create_if_missing: true,
            client: self.options.client.clone(),
        }))
        .await
    }

    pub async fn close_segment(
        &self,
        segment_id: SegmentId,
        sync: bool,
    ) -> Result<CloseSegmentResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.close_segment(CloseSegmentRequest {
            segment_id,
            sync,
            open_version: None,
            client: self.options.client.clone(),
        }))
        .await
    }

    pub async fn stat_segment(&self, segment_id: &SegmentId) -> Result<SegmentSpace> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.stat_segment(segment_id))
            .await
    }

    pub async fn list_segments(&self) -> Result<ListSegmentsResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.list_segments()).await
    }

    pub async fn write(
        &self,
        segment_id: SegmentId,
        offset: u64,
        data: Bytes,
    ) -> Result<WriteResponse> {
        self.write_with_durability(segment_id, offset, data, self.options.default_durability)
            .await
    }

    pub async fn write_sync(
        &self,
        segment_id: SegmentId,
        offset: u64,
        data: Bytes,
    ) -> Result<WriteResponse> {
        self.write_with_durability(segment_id, offset, data, WriteDurability::SyncAll)
            .await
    }

    pub async fn write_with_durability(
        &self,
        segment_id: SegmentId,
        offset: u64,
        data: Bytes,
        durability: WriteDurability,
    ) -> Result<WriteResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.write(WriteRequest {
            segment_id,
            offset,
            data,
            durability,
            sequence_id: self.next_sequence(),
            open_version: None,
            client: self.options.client.clone(),
            qos: self.options.qos.clone(),
        }))
        .await
    }

    pub async fn write_batch(
        &self,
        writes: Vec<(SegmentId, u64, Bytes)>,
        ordered: bool,
    ) -> Result<BatchWriteResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        let req = BatchWriteRequest {
            writes: writes
                .into_iter()
                .map(|(segment_id, offset, data)| WriteRequest {
                    segment_id,
                    offset,
                    data,
                    durability: self.options.default_durability,
                    sequence_id: self.next_sequence(),
                    open_version: None,
                    client: self.options.client.clone(),
                    qos: self.options.qos.clone(),
                })
                .collect(),
            ordered,
        };
        self.with_timeout(self.primary.write_batch(req)).await
    }

    pub async fn raw_write(
        &self,
        segment_id: SegmentId,
        offset: u64,
        data: Bytes,
    ) -> Result<RawSegmentWriteResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.raw_write(RawSegmentWriteRequest {
            segment_id,
            offset,
            data,
            durability: self.options.default_durability,
            sequence_id: self.next_sequence(),
            open_version: None,
            client: self.options.client.clone(),
            qos: self.options.qos.clone(),
        }))
        .await
    }

    pub async fn read(
        &self,
        segment_id: SegmentId,
        offset: u64,
        length: u64,
    ) -> Result<ReadResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        let store = self.read_store().await;
        self.with_timeout(store.read(ReadRequest {
            segment_id,
            offset,
            length,
            sequence_id: self.next_sequence(),
            open_version: None,
            client: self.options.client.clone(),
            qos: self.options.qos.clone(),
        }))
        .await
    }

    pub async fn raw_read(
        &self,
        segment_id: SegmentId,
        offset: u64,
        length: u64,
    ) -> Result<RawSegmentReadResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        let store = self.read_store().await;
        self.with_timeout(store.raw_read(RawSegmentReadRequest {
            segment_id,
            offset,
            length,
            sequence_id: self.next_sequence(),
            open_version: None,
            client: self.options.client.clone(),
            qos: self.options.qos.clone(),
        }))
        .await
    }

    pub async fn readv(
        &self,
        reads: Vec<(SegmentId, u64, u64)>,
        ordered: bool,
    ) -> Result<ReadVectorResponse> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        let store = self.read_store().await;
        let req = ReadVectorRequest {
            reads: reads
                .into_iter()
                .map(|(segment_id, offset, length)| ReadRequest {
                    segment_id,
                    offset,
                    length,
                    sequence_id: self.next_sequence(),
                    open_version: None,
                    client: self.options.client.clone(),
                    qos: self.options.qos.clone(),
                })
                .collect(),
            ordered,
        };
        self.with_timeout(store.readv(req)).await
    }

    pub async fn discard(&self, segment_id: SegmentId, offset: u64, length: u64) -> Result<()> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.discard(DiscardRequest {
            segment_id,
            offset,
            length,
            sequence_id: self.next_sequence(),
            open_version: None,
        }))
        .await
    }

    pub async fn freeze_segment(&self, segment_id: &SegmentId, sync: bool) -> Result<u64> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.freeze_segment(segment_id, sync))
            .await
    }

    pub async fn snapshot(&self, segment_id: &SegmentId) -> Result<SnapshotRef> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.create_snapshot(segment_id))
            .await
    }

    pub async fn flush_shared_store(&self, timeout: Duration) -> Result<SharedStoreStats> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.flush_shared_store(timeout))
            .await
    }

    pub async fn run_maintenance(&self, policy: MaintenancePolicy) -> Result<MaintenanceReport> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.run_maintenance(policy))
            .await
    }

    pub async fn start_background_maintenance(&self, options: BackgroundMaintenanceOptions) {
        self.primary.start_background_maintenance(options).await
    }

    pub async fn stop_background_maintenance(&self) {
        self.primary.stop_background_maintenance().await
    }

    pub async fn background_maintenance_status(&self) -> BackgroundMaintenanceStatus {
        self.primary.background_maintenance_status().await
    }

    pub fn io_stats(&self) -> StoreIoStats {
        self.primary.io_stats()
    }

    pub fn cache_stats(&self) -> CacheStats {
        self.primary.cache_stats()
    }

    pub fn clear_cache(&self) -> CacheStats {
        self.primary.clear_cache()
    }

    pub async fn invalidate_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheStats> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.invalidate_segment_cache(segment_id))
            .await
    }

    pub async fn warm_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheWarmupReport> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .expect("matrixobject semaphore closed");
        self.with_timeout(self.primary.warm_segment_cache(segment_id))
            .await
    }

    pub fn shared_store_stats(&self) -> SharedStoreStats {
        self.primary.shared_store_stats()
    }

    async fn read_store(&self) -> Arc<LocalMatrixObjectStore> {
        match self.options.read_target {
            ReadTarget::Primary => self.primary.clone(),
            ReadTarget::ReplicaPreferred => self
                .next_serviceable_replica()
                .await
                .unwrap_or_else(|| self.primary.clone()),
            ReadTarget::Any => {
                if self.primary.is_serviceable().await {
                    self.primary.clone()
                } else {
                    self.next_serviceable_replica()
                        .await
                        .unwrap_or_else(|| self.primary.clone())
                }
            }
        }
    }

    async fn next_serviceable_replica(&self) -> Option<Arc<LocalMatrixObjectStore>> {
        if self.replicas.is_empty() {
            return None;
        }
        let start = self.next_replica.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.replicas.len() {
            let replica = self.replicas[(start + offset) % self.replicas.len()].clone();
            if replica.is_serviceable().await {
                return Some(replica);
            }
        }
        None
    }

    fn next_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::Relaxed)
    }

    async fn with_timeout<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::time::timeout(self.options.default_timeout, future)
            .await
            .map_err(|_| MatrixObjectError::Timeout {
                millis: self.options.default_timeout.as_millis() as u64,
            })?
    }
}
