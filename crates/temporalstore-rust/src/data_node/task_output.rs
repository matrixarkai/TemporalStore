// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Task cancellation guard + task output/status helpers, extracted from data_node.rs.

use super::*;

pub(super) struct TaskCancellation<'a> {
    pub(super) inner: &'a DataNodeRuntimeInner,
    pub(super) job_id: u64,
}

impl TaskCancellation<'_> {
    pub(super) fn is_requested(&self) -> bool {
        is_cancel_requested(self.inner, self.job_id)
    }
}

pub(super) fn task_timeout_output(kind: DataNodeTaskKind) -> DataNodeTaskOutput {
    let status = Status::error("deadline_exceeded", "data node task deadline exceeded");
    match kind {
        DataNodeTaskKind::Load => DataNodeTaskOutput::Load(LoadShardResponse { status }),
        DataNodeTaskKind::Reload => DataNodeTaskOutput::Reload(LoadShardResponse { status }),
        DataNodeTaskKind::Unload => DataNodeTaskOutput::Unload(UnloadShardResponse { status }),
        DataNodeTaskKind::Execute => DataNodeTaskOutput::Execute(ExecuteResponse {
            status,
            response: CommandResponse::Empty,
        }),
        DataNodeTaskKind::CheckedExecute => {
            DataNodeTaskOutput::CheckedExecute(CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            })
        }
        DataNodeTaskKind::Dump => DataNodeTaskOutput::Dump(DumpShardResponse {
            status,
            shard_id: 0,
            index_bytes: 0,
            dirty_objects_flushed: 0,
            bucket_dump_manifest: None,
        }),
        DataNodeTaskKind::Compact => DataNodeTaskOutput::Compact(CompactionResponse {
            status,
            shard_id: 0,
            compacted_objects: 0,
            rewritten_object_pages: 0,
            tombstoned_object_ids_before: 0,
            tombstoned_object_ids_after: 0,
            model_layouts: Vec::new(),
            previous_page_slab_id: 0,
            compacted_page_slab_id: 0,
            stale_page_slab_ids: Vec::new(),
            before: ShardCompactionUtilityReport::default(),
            after: ShardCompactionUtilityReport::default(),
        }),
        DataNodeTaskKind::Gc => DataNodeTaskOutput::Gc(GcResponse {
            status,
            shard_id: 0,
            collected_objects: 0,
            cache_entries_removed: 0,
            cache_disk_bytes_removed: 0,
            wal_records_removed: 0,
            index_log_records_removed: 0,
            page_slabs_removed: 0,
            page_slabs_removed_physical_bytes: 0,
            page_slabs_retained_physical_bytes: 0,
            page_slabs_retained_live: 0,
            page_slabs_retained_live_physical_bytes: 0,
            gc_durable_index_backed: false,
            wal_gc_clamped_by_durable_index: false,
            index_log_gc_clamped_by_durable_index: false,
            lifecycle_plan: None,
        }),
        DataNodeTaskKind::StorageManager => {
            DataNodeTaskOutput::StorageManager(StorageManagerResponse {
                status,
                report: StorageManagerCycleReport::default(),
            })
        }
    }
}

pub(super) fn task_canceled_output(task: &QueuedTask, message: &str) -> DataNodeTaskOutput {
    let status = Status::error("job_canceled", message);
    let shard_id = task.request.shard_id();
    match task.kind {
        DataNodeTaskKind::Load => DataNodeTaskOutput::Load(LoadShardResponse { status }),
        DataNodeTaskKind::Reload => DataNodeTaskOutput::Reload(LoadShardResponse { status }),
        DataNodeTaskKind::Unload => DataNodeTaskOutput::Unload(UnloadShardResponse { status }),
        DataNodeTaskKind::Execute => DataNodeTaskOutput::Execute(ExecuteResponse {
            status,
            response: CommandResponse::Empty,
        }),
        DataNodeTaskKind::CheckedExecute => {
            DataNodeTaskOutput::CheckedExecute(CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            })
        }
        DataNodeTaskKind::Dump => DataNodeTaskOutput::Dump(DumpShardResponse {
            status,
            shard_id,
            index_bytes: 0,
            dirty_objects_flushed: 0,
            bucket_dump_manifest: None,
        }),
        DataNodeTaskKind::Compact => DataNodeTaskOutput::Compact(CompactionResponse {
            status,
            shard_id,
            compacted_objects: 0,
            rewritten_object_pages: 0,
            tombstoned_object_ids_before: 0,
            tombstoned_object_ids_after: 0,
            model_layouts: Vec::new(),
            previous_page_slab_id: 0,
            compacted_page_slab_id: 0,
            stale_page_slab_ids: Vec::new(),
            before: ShardCompactionUtilityReport::default(),
            after: ShardCompactionUtilityReport::default(),
        }),
        DataNodeTaskKind::Gc => DataNodeTaskOutput::Gc(GcResponse {
            status,
            shard_id,
            collected_objects: 0,
            cache_entries_removed: 0,
            cache_disk_bytes_removed: 0,
            wal_records_removed: 0,
            index_log_records_removed: 0,
            page_slabs_removed: 0,
            page_slabs_removed_physical_bytes: 0,
            page_slabs_retained_physical_bytes: 0,
            page_slabs_retained_live: 0,
            page_slabs_retained_live_physical_bytes: 0,
            gc_durable_index_backed: false,
            wal_gc_clamped_by_durable_index: false,
            index_log_gc_clamped_by_durable_index: false,
            lifecycle_plan: None,
        }),
        DataNodeTaskKind::StorageManager => {
            DataNodeTaskOutput::StorageManager(StorageManagerResponse {
                status,
                report: StorageManagerCycleReport {
                    shard_id,
                    ..StorageManagerCycleReport::default()
                },
            })
        }
    }
}

pub(super) fn is_cancel_requested(inner: &DataNodeRuntimeInner, job_id: u64) -> bool {
    inner
        .canceled
        .lock()
        .expect("runtime cancellation lock poisoned")
        .contains(&job_id)
}

pub(super) fn take_canceled(inner: &DataNodeRuntimeInner, job_id: u64) -> bool {
    inner
        .canceled
        .lock()
        .expect("runtime cancellation lock poisoned")
        .remove(&job_id)
}

pub(super) fn task_output_status(output: &DataNodeTaskOutput) -> Status {
    match output {
        DataNodeTaskOutput::Load(response) => response.status.clone(),
        DataNodeTaskOutput::Reload(response) => response.status.clone(),
        DataNodeTaskOutput::Unload(response) => response.status.clone(),
        DataNodeTaskOutput::Execute(response) => response.status.clone(),
        DataNodeTaskOutput::CheckedExecute(response) => response.status.clone(),
        DataNodeTaskOutput::Dump(response) => response.status.clone(),
        DataNodeTaskOutput::Compact(response) => response.status.clone(),
        DataNodeTaskOutput::Gc(response) => response.status.clone(),
        DataNodeTaskOutput::StorageManager(response) => response.status.clone(),
    }
}

