// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compaction/GC task execution, extracted from data_node.rs.

use super::*;

pub(super) fn run_compaction_inner(
    inner: &DataNodeRuntimeInner,
    request: CompactionRequest,
) -> CompactionResponse {
    run_compaction_inner_draining(inner, request, None)
}

/// The same task, optionally restricted to the slabs the caller wants emptied.
///
/// `None` relocates every live page, which is what a queued compaction task and the operator
/// RPC mean. `Some` is what the periodic maintenance round issues: it already knows which
/// slabs carry dead space that objects still hold pages on, and those are the only slabs a
/// relocation can recover anything for.
pub(super) fn run_compaction_inner_draining(
    inner: &DataNodeRuntimeInner,
    request: CompactionRequest,
    drain_block_slab_ids: Option<BTreeSet<u64>>,
) -> CompactionResponse {
    let compaction = match drain_block_slab_ids {
        Some(drain_block_slab_ids) => inner
            .engine
            .compact_shard_blocks_draining(request.shard_id, drain_block_slab_ids),
        None => inner.engine.compact_shard_blocks(request.shard_id),
    };
    let (
        status,
        compacted_objects,
        relocated_bytes,
        rewritten_object_blocks,
        delete_marked_object_ids_before,
        delete_marked_object_ids_after,
        model_layouts,
        previous_block_slab_id,
        compacted_block_slab_id,
        stale_block_slab_ids,
        before,
        after,
    ) = match compaction {
        Ok(report) => (
            Status::ok(),
            report.rewritten_block_refs,
            report.relocated_bytes,
            report.rewritten_object_blocks,
            report.delete_marked_object_ids_before,
            report.delete_marked_object_ids_after,
            report.model_layouts,
            report.previous_block_slab_id,
            report.compacted_block_slab_id,
            report.stale_block_slab_ids,
            report.before,
            report.after,
        ),
        Err(status) => (
            status,
            0,
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
        relocated_bytes,
        rewritten_object_blocks,
        delete_marked_object_ids_before,
        delete_marked_object_ids_after,
        model_layouts,
        previous_block_slab_id,
        compacted_block_slab_id,
        stale_block_slab_ids,
        before,
        after,
    }
}

pub(super) fn run_gc_inner(inner: &DataNodeRuntimeInner, request: GcRequest) -> GcResponse {
    // REFUSE WHILE THE SHARD IS RECOVERING (WAL replay still to run, or running).
    //
    // This is the guard the storage-manager cycle has always had, on the reclaim that actually
    // ships. Both callers of this function can reach a recovering shard:
    //
    //   1. the periodic loop -- run_storage_manager_once, driven by the storage-manager
    //      scheduler, which iterates loaded_shard_ids(). That reads the shards map, and
    //      load_shard_with inserts into the shards map BEFORE it replays the WAL, precisely so
    //      a concurrent writer can observe the shard and be refused. loaded_shard_ids() does not
    //      consult the recovery flag, so a recovering shard is picked up like any other.
    //   2. the operator reclaim RPC -- an operator can call it at any moment, including during
    //      a restart, and nothing between the route and this function looks at recovery.
    //
    // What goes wrong, measured on this tree rather than reasoned about: the LOG is the sharper
    // exposure, not the slabs. This function also reclaims the write-ahead log, and a shard that
    // has never dumped gets an unproven durable anchor whose through_sequence is u64::MAX, so
    // nothing clamps the ask. Taken during the recovery window that deletes the records replay
    // has not applied yet -- 31 of 32 in the characterization test below -- and replay then
    // aborts on the hole it made, which unwinds load_shard_with and refuses the shard outright.
    // The log has no quarantine of any kind.
    //
    // The slabs are exposed the same way and more mildly: the live set comes from
    // live_block_slab_ids_all_shards(), derived from the in-memory index, so during the window a
    // slab whose referents are still waiting in the log reads as DEAD. The periodic loop asks
    // for delayed destroy and the purge re-checks liveness, so that path can be restored from
    // quarantine -- but the operator RPC leaves delayed destroy off and unlinks there and then.
    //
    // Refused rather than silently skipped: the caller asked for a reclaim and did not get one,
    // and a reclaim that reports ok while doing nothing is how a maintenance loop goes quiet
    // without anyone noticing. The window is bounded by replay, so the next round runs normally.
    if inner.engine.shard_is_recovering(request.shard_id) {
        return GcResponse {
            status: Status::error(
                "shard_recovering",
                format!(
                    "gc refused for shard {}: recovery (WAL replay) is in progress; reclaim must not interleave with replay",
                    request.shard_id
                ),
            ),
            shard_id: request.shard_id,
            collected_objects: 0,
            cache_entries_removed: 0,
            cache_disk_bytes_removed: 0,
            wal_records_removed: 0,
            index_log_records_removed: 0,
            block_slabs_removed: 0,
            block_slabs_removed_physical_bytes: 0,
            block_slabs_retained_physical_bytes: 0,
            block_slabs_retained_live: 0,
            block_slabs_retained_live_physical_bytes: 0,
            gc_durable_index_backed: false,
            wal_gc_clamped_by_durable_index: false,
            index_log_gc_clamped_by_durable_index: false,
            // No plan: building one reads the same half-reconstructed index this is refusing
            // to act on, so a plan here would be a measurement of the partial state.
            lifecycle_plan: None,
        };
    }
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
    let mut gc_durable_index_backed = false;
    let mut wal_gc_clamped_by_durable_index = false;
    let mut index_log_gc_clamped_by_durable_index = false;
    let mut index_log_records_removed = 0;
    let mut block_slabs_removed = 0;
    let mut block_slabs_removed_physical_bytes = 0;
    let mut block_slabs_retained_physical_bytes = 0;
    let mut block_slabs_retained_live = 0;
    let mut block_slabs_retained_live_physical_bytes = 0;
    // Dropping the whole shard's cache has to happen BEFORE the collection when it happens at
    // all: it cannot know what will be reclaimed, so it takes everything. Scoping it to what was
    // actually reclaimed means doing it after, which is why this is a branch here and a second
    // block below rather than a narrower call in the same place.
    if !request.block_gc_invalidate_removed_slabs_only {
        match inner.engine.cache().invalidate_shard(request.shard_id) {
            Ok(report) => {
                cache_entries_removed = report.memory_entries_removed;
                cache_disk_bytes_removed = report.disk_bytes_removed;
            }
            Err(err) => {
                status = Status::error("cache_gc_failed", &err.to_string());
            }
        }
    }
    // Both reclaims below delete durable log records on an operator's say-so, and until now
    // neither checked that anything durable could replace what it was about to drop. The
    // served index is rewritten per write but its barrier is deferred, so a crash shortly after
    // a generous /gc could lose the index update while the records backing it were already
    // gone. One plan answers both -- resolved once, and only when something is actually asked
    // for, since it deserializes every retained manifest.
    let reclaim_plan = (request.retain_wal_from_sequence.is_some()
        || request.retain_index_log_from_sequence.is_some())
    .then(|| {
        inner
            .engine
            .storage_wal_reclaim_plan(request.shard_id, Vec::new(), Vec::new())
    });
    if let Some(plan) = reclaim_plan.as_ref() {
        gc_durable_index_backed = plan.safe_to_reclaim;
    }
    if let Some(retain_from_sequence) = request.retain_wal_from_sequence {
        if status.ok {
            // Anchor the ask to what the bucket-dump manifests actually prove. A shard that has
            // never dumped proves nothing, and narrowing to a frontier of zero would quietly
            // turn this endpoint into a no-op -- so that case reclaims as it always has, and
            // the response says the reclaim was trusted rather than proven.
            let durable_index = match reclaim_plan.as_ref() {
                Some(plan) if plan.safe_to_reclaim => {
                    crate::wal::DurableIndexAnchor::proven_durable_through(
                        request.shard_id,
                        plan.durable_bucket_generation_frontier_wal_sequence,
                    )
                }
                _ => crate::wal::DurableIndexAnchor::unproven(request.shard_id),
            };
            match inner.engine.write_ahead_log_store().gc_before_sequence(
                request.shard_id,
                retain_from_sequence,
                &durable_index,
            ) {
                Ok(report) => {
                    wal_records_removed = report.records_removed;
                    wal_gc_clamped_by_durable_index = report.clamped_by_durable_index;
                }
                Err(err) => {
                    status = Status::error("wal_gc_failed", &err.to_string());
                }
            }
        }
    }
    if status.ok {
        if let Some(retain_from_sequence) = request.retain_index_log_from_sequence {
            // The same exposure as the WAL half, on the log that holds the ADDRESSES. An
            // index-log record names where a block's bytes live; dropping one the durable state
            // does not yet reflect loses the LOCATION of data that is still sitting on disk,
            // which reads as missing rather than as corruption. Bound it by the plan's
            // index-log frontier where the plan proves one, on the same terms as above.
            let ceiling = match reclaim_plan.as_ref() {
                Some(plan) if plan.safe_to_reclaim => plan.retain_from_index_log_sequence,
                _ => u64::MAX,
            };
            index_log_gc_clamped_by_durable_index = retain_from_sequence > ceiling;
            match inner
                .engine
                .index_log_store()
                .gc_before_sequence(request.shard_id, retain_from_sequence.min(ceiling))
            {
                Ok(report) => index_log_records_removed = report.records_removed,
                Err(err) => {
                    status = Status::error("index_log_gc_failed", &err.to_string());
                }
            }
        }
    }
    if status.ok {
        if let Some(retain_from_block_slab_id) = request.retain_block_slabs_from_id {
            // One engine shares a single page_store across every shard it hosts, so a slab can
            // hold pages from multiple shards. Retain slabs live in ANY loaded shard, not just
            // this request's shard; a per-shard live set would delete another shard's live pages.
            let mut live_block_slab_ids = inner.engine.live_block_slab_ids_all_shards();
            // Retain any page slab still referenced by a durable bucket-dump manifest. The
            // operator /gc RPC must not delete a slab a retained manifest needs: a lagging
            // follower's replay or a snapshot-install reads it, and deleting it makes the
            // manifest uninstallable (replica data loss). The gated storage-manager cycle
            // already blocks this via storage_block_gc_dependency_plan; mirror that manifest
            // guard here so the operator path cannot bypass it.
            for manifest in inner.engine.list_bucket_dump_manifests(request.shard_id) {
                live_block_slab_ids.extend(manifest.block_slab_ids.iter().copied());
            }
            // Quarantine or unlink, nothing else: both entries take the same path and differ
            // only in what they do with a slab once it has been selected, so a request that does
            // not ask for quarantine runs exactly the code it ran before.
            let gc_result = if request.block_gc_delayed_destroy {
                inner
                    .engine
                    .block_store()
                    .gc_slabs_before_with_live_refs_delayed_destroy(
                        retain_from_block_slab_id,
                        live_block_slab_ids,
                    )
            } else {
                inner
                    .engine
                    .block_store()
                    .gc_slabs_before_with_live_refs(retain_from_block_slab_id, live_block_slab_ids)
            };
            match gc_result {
                Ok(report) => {
                    if request.block_gc_invalidate_removed_slabs_only {
                        // The entries that went stale are the ones whose slab went away, and
                        // nothing else in the shard did.
                        //
                        // `removed_block_slab_ids` alone, NOT chained with the delayed-destroy
                        // list: a quarantined slab is pushed onto BOTH, so chaining them -- as the
                        // cycle does -- visits it twice. Harmless for the invalidation itself,
                        // which is idempotent, but it would double-count the totals reported here.
                        for block_slab_id in report.removed_block_slab_ids.iter() {
                            match inner
                                .engine
                                .cache()
                                .invalidate_page_segment(request.shard_id, *block_slab_id)
                            {
                                Ok(cache_report) => {
                                    cache_entries_removed = cache_entries_removed
                                        .saturating_add(cache_report.memory_entries_removed);
                                    cache_disk_bytes_removed = cache_disk_bytes_removed
                                        .saturating_add(cache_report.disk_bytes_removed);
                                }
                                Err(err) => {
                                    status =
                                        Status::error("cache_gc_failed", &err.to_string());
                                }
                            }
                        }
                    }
                    block_slabs_removed = report.removed_block_slab_ids.len();
                    block_slabs_removed_physical_bytes = report.removed_physical_bytes;
                    block_slabs_retained_physical_bytes = report.retained_physical_bytes;
                    block_slabs_retained_live = report.retained_live_block_slab_ids.len();
                    block_slabs_retained_live_physical_bytes =
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
                min_undumped_wal_bytes: 0,
                purge_delayed_destroy: false,
                purge_delayed_destroy_slab_ids: None,
                prune_bucket_dump_manifests: false,
                roll_forward_bucket_dump_installs: false,
                follower_replay_cursors: Vec::new(),
                block_gc_shared_store_cursors: Vec::new(),
                block_gc_raft_snapshot_refs: Vec::new(),
                block_gc_checkpoint_floor_slab_id: None,
                block_gc_raft_install_floor_slab_id: None,
                block_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            }),
    );
    inner
        .stats
        .lock()
        .expect("runtime stats lock poisoned")
        .gc_runs += 1;
    // Hand back what this just released. `free` returns memory to the allocator, not to the kernel:
    // measured here, dropping 46 MB returned 2.3% of it, and one trim returned 95% of the rest. GC
    // is where a lot is released at once and nothing is waiting on the result.
    let _ = crate::memory_trim::release_free_heap_to_os();
    GcResponse {
        status,
        shard_id: request.shard_id,
        collected_objects,
        cache_entries_removed,
        cache_disk_bytes_removed,
        wal_records_removed,
        index_log_records_removed,
        block_slabs_removed,
        block_slabs_removed_physical_bytes,
        block_slabs_retained_physical_bytes,
        block_slabs_retained_live,
        block_slabs_retained_live_physical_bytes,
        gc_durable_index_backed,
        wal_gc_clamped_by_durable_index,
        index_log_gc_clamped_by_durable_index,
        lifecycle_plan,
    }
}
