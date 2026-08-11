// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::local::LocalMatrixObjectStore;
use crate::types::*;
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait MatrixObjectBlockService: Send + Sync {
    async fn open_segment(&self, req: OpenSegmentRequest) -> Result<OpenSegmentResponse>;
    async fn close_segment(&self, req: CloseSegmentRequest) -> Result<CloseSegmentResponse>;
    async fn update_segment(&self, req: UpdateSegmentRequest) -> Result<OpenSegmentResponse>;
    async fn delete_segment(&self, segment_id: &SegmentId, delete_chunks: bool) -> Result<()>;
    async fn clone_snapshot(
        &self,
        snapshot_id: &str,
        source_segment_id: &SegmentId,
        dest_segment_id: SegmentId,
    ) -> Result<OpenSegmentResponse>;
    async fn stat_segment(&self, segment_id: &SegmentId) -> Result<SegmentSpace>;
    async fn list_segments(&self) -> Result<ListSegmentsResponse>;
    async fn create_snapshot(&self, segment_id: &SegmentId) -> Result<SnapshotRef>;
    async fn delete_snapshot(&self, segment_id: &SegmentId, snapshot_id: &str) -> Result<()>;
    async fn rollback_snapshot(
        &self,
        snapshot_id: &str,
        segment_id: &SegmentId,
    ) -> Result<OpenSegmentResponse>;
    async fn get_snapshot_info(
        &self,
        segment_id: &SegmentId,
        snapshot_id: &str,
    ) -> Result<SnapshotInfo>;
    async fn list_snapshots(&self, segment_id: &SegmentId) -> Result<SnapshotListResponse>;
    async fn get_snapshot_diff(
        &self,
        segment_id: &SegmentId,
        old_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> Result<SnapshotDiff>;
    async fn get_meta_diff(
        &self,
        segment_id: &SegmentId,
        old_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> Result<MetaDiff>;
    async fn rebase_segment(
        &self,
        segment_id: &SegmentId,
        base_snapshot_id: &str,
    ) -> Result<MetaDiff>;
    async fn read(&self, req: ReadRequest) -> Result<ReadResponse>;
    async fn raw_read(&self, req: RawSegmentReadRequest) -> Result<RawSegmentReadResponse>;
    async fn readv(&self, req: ReadVectorRequest) -> Result<ReadVectorResponse>;
    async fn write(&self, req: WriteRequest) -> Result<WriteResponse>;
    async fn raw_write(&self, req: RawSegmentWriteRequest) -> Result<RawSegmentWriteResponse>;
    async fn write_batch(&self, req: BatchWriteRequest) -> Result<BatchWriteResponse>;
    async fn discard(&self, req: DiscardRequest) -> Result<()>;
}

#[async_trait]
pub trait MatrixObjectChunkService: Send + Sync {
    async fn create_chunk(&self, req: CreateChunkRequest) -> Result<CreateChunkResponse>;
    async fn raw_write_chunk(&self, req: RawChunkWriteRequest) -> Result<RawChunkWriteResponse>;
    async fn raw_read_chunk(&self, req: RawChunkReadRequest) -> Result<RawChunkReadResponse>;
    async fn discard_chunk(&self, req: RawChunkDiscardRequest) -> Result<RawChunkWriteResponse>;
    async fn sync_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ChunkMeta>;
    async fn freeze_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ChunkMeta>;
    async fn hard_link_chunk(&self, req: HardLinkChunkRequest) -> Result<ChunkMeta>;
    async fn set_chunk_flags(&self, req: SetChunkFlagsRequest) -> Result<ChunkMeta>;
    async fn get_chunk_meta(&self, segment_id: &SegmentId) -> Result<Vec<ChunkMeta>>;
    async fn get_storage_meta(
        &self,
        segment_id: &SegmentId,
        chunk_index: u64,
    ) -> Result<StorageMeta>;
    async fn scrub_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ScrubResult>;
    async fn scrub_segment(&self, segment_id: &SegmentId) -> Result<SegmentScrubReport>;
    async fn collect_chunk_metas(&self) -> Result<Vec<ChunkMeta>>;
    async fn collect_chunk_metas_after(
        &self,
        last_chunk_index: Option<u64>,
        batch: usize,
    ) -> Result<Vec<ChunkMeta>>;
    async fn delete_chunks(
        &self,
        segment_id: &SegmentId,
        chunk_indices: &[u64],
    ) -> Result<Vec<ChunkDeleteResult>>;
    async fn delete_stale_chunks(
        &self,
        segment_id: &SegmentId,
        stale_versions: &[StaleChunkVersion],
    ) -> Result<Vec<ChunkDeleteResult>>;
}

#[async_trait]
pub trait MatrixObjectAdminService: Send + Sync {
    async fn disk_status(&self) -> Result<DiskStatus>;
    async fn set_serviceable(&self, serviceable: bool);
    async fn is_serviceable(&self) -> bool;
    async fn list_disks(&self) -> Vec<DiskDescriptor>;
    async fn add_disk(&self, req: AddDiskRequest) -> Result<DiskDescriptor>;
    async fn remove_disk(&self, req: RemoveDiskRequest) -> Result<Option<DiskDescriptor>>;
    async fn set_disk_power_status(
        &self,
        disk_id: u32,
        power_status: DiskPowerStatus,
    ) -> Result<DiskDescriptor>;
    async fn set_disk_load_state(
        &self,
        disk_id: u32,
        load_state: DiskLoadState,
    ) -> Result<DiskLoadInfo>;
    async fn get_disk_load_state(&self, disk_ids: &[u32]) -> Vec<DiskLoadInfo>;
    async fn set_background_throughput(&self, options: BackgroundThroughputOptions);
    async fn background_throughput(&self) -> BackgroundThroughputOptions;
    async fn io_stats(&self) -> StoreIoStats;
    async fn cache_stats(&self) -> CacheStats;
    async fn clear_cache(&self) -> CacheStats;
    async fn invalidate_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheStats>;
    async fn warm_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheWarmupReport>;
    async fn shared_store_stats(&self) -> SharedStoreStats;
    async fn flush_shared_store(&self, timeout: Duration) -> Result<SharedStoreStats>;
    async fn scrub_store(&self) -> Result<Vec<SegmentScrubReport>>;
    async fn set_verify_checksums_on_read(&self, enabled: bool);
    async fn verify_checksums_on_read(&self) -> bool;
    async fn set_runtime_flag(&self, req: SetRuntimeFlagRequest);
    async fn get_runtime_flag(&self, name: &str) -> GetRuntimeFlagResponse;
    async fn list_runtime_flags(&self) -> Vec<RuntimeFlag>;
    async fn notify_replicate(
        &self,
        req: NotifyReplicateRequest,
    ) -> Result<NotifyReplicateResponse>;
    async fn check_replicate_status(&self, task_ids: &[String]) -> CheckReplicateStatusResponse;
    async fn cancel_replicate(&self, task_ids: &[String], force: bool) -> CancelReplicateResponse;
    async fn list_recycle_bin(&self) -> Vec<RecycleBinEntry>;
    async fn restore_recycle_bin(
        &self,
        req: RestoreRecycleBinRequest,
    ) -> Result<RestoreRecycleBinResponse>;
    async fn recover_decommission(&self) -> Result<RecoverDecommissionResponse>;
    async fn run_maintenance(&self, policy: MaintenancePolicy) -> Result<MaintenanceReport>;
    async fn start_background_maintenance(&self, options: BackgroundMaintenanceOptions);
    async fn stop_background_maintenance(&self);
    async fn background_maintenance_status(&self) -> BackgroundMaintenanceStatus;
}

#[async_trait]
impl MatrixObjectBlockService for LocalMatrixObjectStore {
    async fn open_segment(&self, req: OpenSegmentRequest) -> Result<OpenSegmentResponse> {
        LocalMatrixObjectStore::open_segment(self, req).await
    }

    async fn close_segment(&self, req: CloseSegmentRequest) -> Result<CloseSegmentResponse> {
        LocalMatrixObjectStore::close_segment(self, req).await
    }

    async fn update_segment(&self, req: UpdateSegmentRequest) -> Result<OpenSegmentResponse> {
        LocalMatrixObjectStore::update_segment(self, req).await
    }

    async fn delete_segment(&self, segment_id: &SegmentId, delete_chunks: bool) -> Result<()> {
        LocalMatrixObjectStore::delete_segment(self, segment_id, delete_chunks).await
    }

    async fn clone_snapshot(
        &self,
        snapshot_id: &str,
        source_segment_id: &SegmentId,
        dest_segment_id: SegmentId,
    ) -> Result<OpenSegmentResponse> {
        LocalMatrixObjectStore::clone_snapshot(
            self,
            snapshot_id,
            source_segment_id,
            dest_segment_id,
        )
        .await
    }

    async fn stat_segment(&self, segment_id: &SegmentId) -> Result<SegmentSpace> {
        LocalMatrixObjectStore::stat_segment(self, segment_id).await
    }

    async fn list_segments(&self) -> Result<ListSegmentsResponse> {
        LocalMatrixObjectStore::list_segments(self).await
    }

    async fn create_snapshot(&self, segment_id: &SegmentId) -> Result<SnapshotRef> {
        LocalMatrixObjectStore::create_snapshot(self, segment_id).await
    }

    async fn delete_snapshot(&self, segment_id: &SegmentId, snapshot_id: &str) -> Result<()> {
        LocalMatrixObjectStore::delete_snapshot(self, segment_id, snapshot_id).await
    }

    async fn rollback_snapshot(
        &self,
        snapshot_id: &str,
        segment_id: &SegmentId,
    ) -> Result<OpenSegmentResponse> {
        LocalMatrixObjectStore::rollback_snapshot(self, snapshot_id, segment_id).await
    }

    async fn get_snapshot_info(
        &self,
        segment_id: &SegmentId,
        snapshot_id: &str,
    ) -> Result<SnapshotInfo> {
        LocalMatrixObjectStore::get_snapshot_info(self, segment_id, snapshot_id).await
    }

    async fn list_snapshots(&self, segment_id: &SegmentId) -> Result<SnapshotListResponse> {
        LocalMatrixObjectStore::list_snapshots(self, segment_id).await
    }

    async fn get_snapshot_diff(
        &self,
        segment_id: &SegmentId,
        old_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> Result<SnapshotDiff> {
        LocalMatrixObjectStore::get_snapshot_diff(
            self,
            segment_id,
            old_snapshot_id,
            new_snapshot_id,
        )
        .await
    }

    async fn get_meta_diff(
        &self,
        segment_id: &SegmentId,
        old_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> Result<MetaDiff> {
        LocalMatrixObjectStore::get_meta_diff(self, segment_id, old_snapshot_id, new_snapshot_id)
            .await
    }

    async fn rebase_segment(
        &self,
        segment_id: &SegmentId,
        base_snapshot_id: &str,
    ) -> Result<MetaDiff> {
        LocalMatrixObjectStore::rebase_segment(self, segment_id, base_snapshot_id).await
    }

    async fn read(&self, req: ReadRequest) -> Result<ReadResponse> {
        LocalMatrixObjectStore::read(self, req).await
    }

    async fn raw_read(&self, req: RawSegmentReadRequest) -> Result<RawSegmentReadResponse> {
        LocalMatrixObjectStore::raw_read(self, req).await
    }

    async fn readv(&self, req: ReadVectorRequest) -> Result<ReadVectorResponse> {
        LocalMatrixObjectStore::readv(self, req).await
    }

    async fn write(&self, req: WriteRequest) -> Result<WriteResponse> {
        LocalMatrixObjectStore::write(self, req).await
    }

    async fn raw_write(&self, req: RawSegmentWriteRequest) -> Result<RawSegmentWriteResponse> {
        LocalMatrixObjectStore::raw_write(self, req).await
    }

    async fn write_batch(&self, req: BatchWriteRequest) -> Result<BatchWriteResponse> {
        LocalMatrixObjectStore::write_batch(self, req).await
    }

    async fn discard(&self, req: DiscardRequest) -> Result<()> {
        LocalMatrixObjectStore::discard(self, req).await
    }
}

#[async_trait]
impl MatrixObjectChunkService for LocalMatrixObjectStore {
    async fn create_chunk(&self, req: CreateChunkRequest) -> Result<CreateChunkResponse> {
        LocalMatrixObjectStore::create_chunk(self, req).await
    }

    async fn raw_write_chunk(&self, req: RawChunkWriteRequest) -> Result<RawChunkWriteResponse> {
        LocalMatrixObjectStore::raw_write_chunk(self, req).await
    }

    async fn raw_read_chunk(&self, req: RawChunkReadRequest) -> Result<RawChunkReadResponse> {
        LocalMatrixObjectStore::raw_read_chunk(self, req).await
    }

    async fn discard_chunk(&self, req: RawChunkDiscardRequest) -> Result<RawChunkWriteResponse> {
        LocalMatrixObjectStore::discard_chunk(self, req).await
    }

    async fn sync_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ChunkMeta> {
        LocalMatrixObjectStore::sync_chunk(self, segment_id, chunk_index).await
    }

    async fn freeze_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ChunkMeta> {
        LocalMatrixObjectStore::freeze_chunk(self, segment_id, chunk_index).await
    }

    async fn hard_link_chunk(&self, req: HardLinkChunkRequest) -> Result<ChunkMeta> {
        LocalMatrixObjectStore::hard_link_chunk(self, req).await
    }

    async fn set_chunk_flags(&self, req: SetChunkFlagsRequest) -> Result<ChunkMeta> {
        LocalMatrixObjectStore::set_chunk_flags(self, req).await
    }

    async fn get_chunk_meta(&self, segment_id: &SegmentId) -> Result<Vec<ChunkMeta>> {
        LocalMatrixObjectStore::get_chunk_meta(self, segment_id).await
    }

    async fn get_storage_meta(
        &self,
        segment_id: &SegmentId,
        chunk_index: u64,
    ) -> Result<StorageMeta> {
        LocalMatrixObjectStore::get_storage_meta(self, segment_id, chunk_index).await
    }

    async fn scrub_chunk(&self, segment_id: &SegmentId, chunk_index: u64) -> Result<ScrubResult> {
        LocalMatrixObjectStore::scrub_chunk(self, segment_id, chunk_index).await
    }

    async fn scrub_segment(&self, segment_id: &SegmentId) -> Result<SegmentScrubReport> {
        LocalMatrixObjectStore::scrub_segment(self, segment_id).await
    }

    async fn collect_chunk_metas(&self) -> Result<Vec<ChunkMeta>> {
        LocalMatrixObjectStore::collect_chunk_metas(self).await
    }

    async fn collect_chunk_metas_after(
        &self,
        last_chunk_index: Option<u64>,
        batch: usize,
    ) -> Result<Vec<ChunkMeta>> {
        LocalMatrixObjectStore::collect_chunk_metas_after(self, last_chunk_index, batch).await
    }

    async fn delete_chunks(
        &self,
        segment_id: &SegmentId,
        chunk_indices: &[u64],
    ) -> Result<Vec<ChunkDeleteResult>> {
        LocalMatrixObjectStore::delete_chunks(self, segment_id, chunk_indices).await
    }

    async fn delete_stale_chunks(
        &self,
        segment_id: &SegmentId,
        stale_versions: &[StaleChunkVersion],
    ) -> Result<Vec<ChunkDeleteResult>> {
        LocalMatrixObjectStore::delete_stale_chunks(self, segment_id, stale_versions).await
    }
}

#[async_trait]
impl MatrixObjectAdminService for LocalMatrixObjectStore {
    async fn disk_status(&self) -> Result<DiskStatus> {
        LocalMatrixObjectStore::disk_status(self).await
    }

    async fn set_serviceable(&self, serviceable: bool) {
        LocalMatrixObjectStore::set_serviceable(self, serviceable).await
    }

    async fn is_serviceable(&self) -> bool {
        LocalMatrixObjectStore::is_serviceable(self).await
    }

    async fn list_disks(&self) -> Vec<DiskDescriptor> {
        LocalMatrixObjectStore::list_disks(self).await
    }

    async fn add_disk(&self, req: AddDiskRequest) -> Result<DiskDescriptor> {
        LocalMatrixObjectStore::add_disk(self, req).await
    }

    async fn remove_disk(&self, req: RemoveDiskRequest) -> Result<Option<DiskDescriptor>> {
        LocalMatrixObjectStore::remove_disk(self, req).await
    }

    async fn set_disk_power_status(
        &self,
        disk_id: u32,
        power_status: DiskPowerStatus,
    ) -> Result<DiskDescriptor> {
        LocalMatrixObjectStore::set_disk_power_status(self, disk_id, power_status).await
    }

    async fn set_disk_load_state(
        &self,
        disk_id: u32,
        load_state: DiskLoadState,
    ) -> Result<DiskLoadInfo> {
        LocalMatrixObjectStore::set_disk_load_state(self, disk_id, load_state).await
    }

    async fn get_disk_load_state(&self, disk_ids: &[u32]) -> Vec<DiskLoadInfo> {
        LocalMatrixObjectStore::get_disk_load_state(self, disk_ids).await
    }

    async fn set_background_throughput(&self, options: BackgroundThroughputOptions) {
        LocalMatrixObjectStore::set_background_throughput(self, options).await
    }

    async fn background_throughput(&self) -> BackgroundThroughputOptions {
        LocalMatrixObjectStore::background_throughput(self).await
    }

    async fn io_stats(&self) -> StoreIoStats {
        LocalMatrixObjectStore::io_stats(self)
    }

    async fn cache_stats(&self) -> CacheStats {
        LocalMatrixObjectStore::cache_stats(self)
    }

    async fn clear_cache(&self) -> CacheStats {
        LocalMatrixObjectStore::clear_cache(self)
    }

    async fn invalidate_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheStats> {
        LocalMatrixObjectStore::invalidate_segment_cache(self, segment_id).await
    }

    async fn warm_segment_cache(&self, segment_id: &SegmentId) -> Result<CacheWarmupReport> {
        LocalMatrixObjectStore::warm_segment_cache(self, segment_id).await
    }

    async fn shared_store_stats(&self) -> SharedStoreStats {
        LocalMatrixObjectStore::shared_store_stats(self)
    }

    async fn flush_shared_store(&self, timeout: Duration) -> Result<SharedStoreStats> {
        LocalMatrixObjectStore::flush_shared_store(self, timeout).await
    }

    async fn scrub_store(&self) -> Result<Vec<SegmentScrubReport>> {
        LocalMatrixObjectStore::scrub_store(self).await
    }

    async fn set_verify_checksums_on_read(&self, enabled: bool) {
        LocalMatrixObjectStore::set_verify_checksums_on_read(self, enabled).await
    }

    async fn verify_checksums_on_read(&self) -> bool {
        LocalMatrixObjectStore::verify_checksums_on_read(self).await
    }

    async fn set_runtime_flag(&self, req: SetRuntimeFlagRequest) {
        LocalMatrixObjectStore::set_runtime_flag(self, req).await
    }

    async fn get_runtime_flag(&self, name: &str) -> GetRuntimeFlagResponse {
        LocalMatrixObjectStore::get_runtime_flag(self, name).await
    }

    async fn list_runtime_flags(&self) -> Vec<RuntimeFlag> {
        LocalMatrixObjectStore::list_runtime_flags(self).await
    }

    async fn notify_replicate(
        &self,
        req: NotifyReplicateRequest,
    ) -> Result<NotifyReplicateResponse> {
        LocalMatrixObjectStore::notify_replicate(self, req).await
    }

    async fn check_replicate_status(&self, task_ids: &[String]) -> CheckReplicateStatusResponse {
        LocalMatrixObjectStore::check_replicate_status(self, task_ids).await
    }

    async fn cancel_replicate(&self, task_ids: &[String], force: bool) -> CancelReplicateResponse {
        LocalMatrixObjectStore::cancel_replicate(self, task_ids, force).await
    }

    async fn list_recycle_bin(&self) -> Vec<RecycleBinEntry> {
        LocalMatrixObjectStore::list_recycle_bin(self).await
    }

    async fn restore_recycle_bin(
        &self,
        req: RestoreRecycleBinRequest,
    ) -> Result<RestoreRecycleBinResponse> {
        LocalMatrixObjectStore::restore_recycle_bin(self, req).await
    }

    async fn recover_decommission(&self) -> Result<RecoverDecommissionResponse> {
        LocalMatrixObjectStore::recover_decommission(self).await
    }

    async fn run_maintenance(&self, policy: MaintenancePolicy) -> Result<MaintenanceReport> {
        LocalMatrixObjectStore::run_maintenance(self, policy).await
    }

    async fn start_background_maintenance(&self, options: BackgroundMaintenanceOptions) {
        LocalMatrixObjectStore::start_background_maintenance(self, options).await
    }

    async fn stop_background_maintenance(&self) {
        LocalMatrixObjectStore::stop_background_maintenance(self).await
    }

    async fn background_maintenance_status(&self) -> BackgroundMaintenanceStatus {
        LocalMatrixObjectStore::background_maintenance_status(self).await
    }
}
