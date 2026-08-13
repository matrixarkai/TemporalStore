// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compaction/GC task execution, extracted from data_node.rs.

use super::*;

pub(super) fn run_compaction_inner(
    inner: &DataNodeRuntimeInner,
    request: CompactionRequest,
) -> CompactionResponse {
    let compaction = inner.engine.compact_shard_pages(request.shard_id);
    let (
        status,
        compacted_objects,
        rewritten_object_pages,
        tombstoned_object_ids_before,
        tombstoned_object_ids_after,
        model_layouts,
        previous_page_slab_id,
        compacted_page_slab_id,
        stale_page_slab_ids,
        before,
        after,
    ) = match compaction {
        Ok(report) => (
            Status::ok(),
            report.rewritten_page_refs,
            report.rewritten_object_pages,
            report.tombstoned_object_ids_before,
            report.tombstoned_object_ids_after,
            report.model_layouts,
            report.previous_page_slab_id,
            report.compacted_page_slab_id,
            report.stale_page_slab_ids,
            report.before,
            report.after,
        ),
        Err(status) => (
            status,
            0,
            0,
            0,
            0,
            Vec::new(),
            0,
            0,
            Vec::new(),
            ShardCompactionUtilityReport::default(),
            ShardCompactionUtilityReport::default(),
        ),
    };
    inner
        .stats
        .lock()
        .expect("runtime stats lock poisoned")
        .compaction_runs += 1;
    CompactionResponse {
        status,
        shard_id: request.shard_id,
        compacted_objects,
        rewritten_object_pages,
        tombstoned_object_ids_before,
        tombstoned_object_ids_after,
        model_layouts,
        previous_page_slab_id,
        compacted_page_slab_id,
        stale_page_slab_ids,
        before,
        after,
    }
}

pub(super) fn run_gc_inner(inner: &DataNodeRuntimeInner, request: GcRequest) -> GcResponse {
    // GC must NOT touch the dirty-scheduling tracker. GC (block/index reclaim) never
    // clears dirty buckets -- a bucket leaves the dirty set only via a completed
    // dump/replay that clears its dirty flag. Clearing it here (at task start, before
    // any GC work and regardless of whether GC then fails) dropped the re-dump scheduling
    // state, so schedule_dirty_shard_dumps would stop scheduling those still-undumped objects.
    let collected_objects = 0;
    let mut status = Status::ok();
    let mut cache_entries_removed = 0;
    let mut cache_disk_bytes_removed = 0;
    let mut wal_records_removed = 0;
    let mut index_log_records_removed = 0;
    let mut page_slabs_removed = 0;
    let mut page_slabs_removed_physical_bytes = 0;
    let mut page_slabs_retained_physical_bytes = 0;
    let mut page_slabs_retained_live = 0;
    let mut page_slabs_retained_live_physical_bytes = 0;
    match inner.engine.cache().invalidate_shard(request.shard_id) {
        Ok(report) => {
            cache_entries_removed = report.memory_entries_removed;
            cache_disk_bytes_removed = report.disk_bytes_removed;
        }
        Err(err) => {
            status = Status::error("cache_gc_failed", &err.to_string());
        }
    }
    if let Some(retain_from_sequence) = request.retain_wal_from_sequence {
        if status.ok {
            match inner
                .engine
                .write_ahead_log_store()
                .gc_before_sequence(request.shard_id, retain_from_sequence)
            {
                Ok(report) => wal_records_removed = report.records_removed,
                Err(err) => {
                    status = Status::error("wal_gc_failed", &err.to_string());
                }
            }
        }
    }
    if status.ok {
        if let Some(retain_from_sequence) = request.retain_index_log_from_sequence {
            match inner
                .engine
                .index_log_store()
                .gc_before_sequence(request.shard_id, retain_from_sequence)
            {
                Ok(report) => index_log_records_removed = report.records_removed,
                Err(err) => {
                    status = Status::error("index_log_gc_failed", &err.to_string());
                }
            }
        }
    }
    if status.ok {
        if let Some(retain_from_page_slab_id) = request.retain_page_slabs_from_id {
            let mut live_page_slab_ids = inner.engine.live_page_slab_ids(request.shard_id);
            // Retain any page slab still referenced by a durable bucket-dump manifest. The
            // operator /gc RPC must not delete a slab a retained manifest needs: a lagging
            // follower's replay or a snapshot-install reads it, and deleting it makes the
            // manifest uninstallable (replica data loss). The gated storage-manager cycle
            // already blocks this via storage_page_gc_dependency_plan; mirror that manifest
            // guard here so the operator path cannot bypass it.
            for manifest in inner.engine.list_bucket_dump_manifests(request.shard_id) {
                live_page_slab_ids.extend(manifest.page_slab_ids.iter().copied());
            }
            match inner
                .engine
                .block_store()
                .gc_slabs_before_with_live_refs(
                    retain_from_page_slab_id,
                    live_page_slab_ids,
                ) {
                Ok(report) => {
                    page_slabs_removed = report.removed_page_slab_ids.len();
                    page_slabs_removed_physical_bytes = report.removed_physical_bytes;
                    page_slabs_retained_physical_bytes = report.retained_physical_bytes;
                    page_slabs_retained_live = report.retained_live_page_slab_ids.len();
                    page_slabs_retained_live_physical_bytes =
                        report.retained_live_physical_bytes;
                }
                Err(err) => {
                    status = Status::error("block_store_gc_failed", &err.to_string());
                }
            }
        }
    }
    let lifecycle_plan = Some(
        inner
            .engine
            .storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id: request.shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: 0,
                min_undumped_wal_records: 0,
                purge_delayed_destroy: false,
                prune_bucket_dump_manifests: false,
                roll_forward_bucket_dump_installs: false,
                follower_replay_cursors: Vec::new(),
                page_gc_shared_store_cursors: Vec::new(),
                page_gc_raft_snapshot_refs: Vec::new(),
                page_gc_checkpoint_floor_slab_id: None,
                page_gc_raft_install_floor_slab_id: None,
                page_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            }),
    );
    inner
        .stats
        .lock()
        .expect("runtime stats lock poisoned")
        .gc_runs += 1;
    GcResponse {
        status,
        shard_id: request.shard_id,
        collected_objects,
        cache_entries_removed,
        cache_disk_bytes_removed,
        wal_records_removed,
        index_log_records_removed,
        page_slabs_removed,
        page_slabs_removed_physical_bytes,
        page_slabs_retained_physical_bytes,
        page_slabs_retained_live,
        page_slabs_retained_live_physical_bytes,
        lifecycle_plan,
    }
}
