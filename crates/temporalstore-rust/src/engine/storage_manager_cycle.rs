// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage-manager cycle + merged-dump policy report for TemporalEngine, split from engine.rs.
//!
//! CROSS-SHARD RECLAIM, always on. Page-slab reclaim retains any slab referenced by ANY shard
//! loaded into this engine, not only the shard whose cycle is running: one engine shares a single
//! page_store across all its shards with a global slab cursor, so a slab can hold committed pages
//! from multiple shards, and driving GC off a single shard's live set deletes another shard's live
//! pages (silent data loss). `TS_CROSS_SHARD_RECLAIM_GUARD` used to be able to restore the legacy
//! per-shard live set; nothing selected that position, and it was unsafe under multi-shard hosting
//! by construction. For a single loaded shard the union equals that shard's live set, so
//! single-shard behavior was byte-identical either way.

use super::*;

/// How many live pages one maintenance round checks for readability.
///
/// The round used to check every one. A bound makes the cost of the check independent of the
/// store's size, and 512 is chosen the way the other round bounds were: large enough that a
/// small store is still fully checked every round, small enough that a large one pays a fixed
/// price. At the measured ~18 microseconds per page read that is about 9 ms.
pub(super) const RECOVERY_READABLE_PROBE_PER_ROUND: usize = 512;

impl TemporalEngine {
    pub fn run_storage_manager_cycle(
        &self,
        request: StorageManagerCycleRequest,
    ) -> StorageManagerCycleReport {
        let cycle_started_unix_ms = now_ms();
        // The same instant, on the clock that cannot be adjusted. The pair is deliberate: one
        // says when, the other says how long.
        let cycle_started_at = std::time::Instant::now();
        // Each stage is timed from the end of the previous one, so the durations tile the cycle
        // instead of overlapping it. Read and restart in the same expression, exactly once per
        // stage, where that stage's report is built.
        //
        // Deliberately not a closure. A `move` closure capturing only an `Instant` captures a Copy
        // type, which makes the closure itself Copy -- the restart then applies to a copy and every
        // stage reports the time since the CYCLE began instead of since the previous stage. That
        // was the first version here, and the totals gave it away: eight stages summing to eight
        // times the wall clock of the call that produced them.
        let mut stage_clock = std::time::Instant::now();
        // Short-circuit while the shard is RECOVERING (WAL replay in progress). The cycle
        // mutates shard state (eviction / page + WAL reclaim / compaction); a GC or compaction
        // round interleaved with an in-flight replay would observe a half-reconstructed bucket
        // index and could mis-reclaim a page that is still live -> silent durable loss. The
        // synchronous POST /load path calls load_shard_with directly (it never registers in
        // running_shards), so `recovering` is the reliable, path-independent guard: it is set
        // for the whole replay window by both the async and the synchronous load. A recovering
        // shard yields an inert (no-op) report; the next cycle after recovery runs normally.
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.recovering)
            .unwrap_or(false)
        {
            return StorageManagerCycleReport {
                shard_id: request.shard_id,
                dry_run: request.dry_run,
                completed: false,
                errors: vec![format!(
                    "storage manager cycle skipped for shard {}: recovery (WAL replay) is in progress; GC/compaction must not interleave with replay",
                    request.shard_id
                )],
                ..StorageManagerCycleReport::default()
            };
        }
        let native_stage_order = [
            "prepare",
            "reclaim_wal",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let plan_request = StorageLifecycleRequest {
            shard_id: request.shard_id,
            selected_dump_buckets: request.selected_dump_buckets.clone(),
            max_dump_buckets_per_round: request.max_dump_buckets_per_round,
            min_undumped_wal_records: if request.enable_wal_reclaim {
                request.min_undumped_wal_records
            } else {
                u64::MAX
            },
            // Unreachable when reclaim is off, exactly as the record threshold is. A byte
            // threshold left live here would release a dump the operator switched off -- the
            // two thresholds are alternatives for WHEN to dump, not for WHETHER to.
            min_undumped_wal_bytes: if request.enable_wal_reclaim {
                request.min_undumped_wal_bytes
            } else {
                u64::MAX
            },
            purge_delayed_destroy: request.enable_block_reclaim,
            purge_delayed_destroy_slab_ids: None,
            prune_bucket_dump_manifests: request.enable_index_gc,
            roll_forward_bucket_dump_installs: request.enable_index_gc,
            follower_replay_cursors: request.follower_replay_cursors.clone(),
            block_gc_shared_store_cursors: request.block_gc_shared_store_cursors.clone(),
            block_gc_raft_snapshot_refs: request.raft_snapshot_refs.clone(),
            block_gc_checkpoint_floor_slab_id: request.block_gc_checkpoint_floor_slab_id,
            block_gc_raft_install_floor_slab_id: request.block_gc_raft_install_floor_slab_id,
            block_gc_delayed_destroy_grace_ms: request.block_gc_delayed_destroy_grace_ms,
            invalidate_cache: false,
            warm_cache: request.warm_cache,
        };
        let plan = self.storage_lifecycle_plan(plan_request.clone());
        let block_gc_dependency_plan = self.storage_block_gc_dependency_plan(
            request.shard_id,
            plan.reclaim_candidates
                .iter()
                .map(|candidate| candidate.block_slab_id),
            request.block_gc_shared_store_cursors.clone(),
            request.raft_snapshot_refs.clone(),
            request.block_gc_checkpoint_floor_slab_id,
            request.block_gc_raft_install_floor_slab_id,
            request.block_gc_delayed_destroy_grace_ms,
        );
        // THE ONE PLACE "did not look" COLLAPSES TO A ZERO IN THIS FUNCTION.
        //
        // `plan.bucket_summaries` is `None` when the round skipped the whole-shard walk because
        // no bucket was dirty. That is NOT "the shard holds nothing": a settled shard with
        // millions of live pages reports `None` every round. The stage figures below are derived
        // from it and therefore read 0 on such a round.
        //
        // What keeps that 0 honest is that the plan travels WITH them:
        // `StorageManagerCycleReport.plan.bucket_summaries` is the same `Option`, so a reader of
        // any of these byte or count figures can tell an unmeasured 0 from a measured one without
        // a second source. `live_page_summaries_measured` below is that test, named once.
        let bucket_summaries = plan.bucket_summaries.as_deref();
        let live_block_summaries_measured = bucket_summaries.is_some();
        let bucket_logical_bytes = bucket_summaries
            .map(|summaries| {
                summaries
                    .iter()
                    .map(|summary| summary.logical_bytes)
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let bucket_physical_bytes = bucket_summaries
            .map(|summaries| {
                summaries
                    .iter()
                    .map(|summary| summary.physical_bytes)
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let bucket_block_ref_count = bucket_summaries
            .map(|summaries| {
                summaries
                    .iter()
                    .map(|summary| summary.block_ref_count)
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let bucket_count = bucket_summaries.map(|summaries| summaries.len()).unwrap_or(0);
        let reclaim_live_bytes = plan
            .reclaim_candidates
            .iter()
            .map(|candidate| candidate.live_physical_bytes)
            .sum::<u64>();
        let reclaim_stale_bytes = plan
            .reclaim_candidates
            .iter()
            .map(|candidate| candidate.stale_physical_bytes)
            .sum::<u64>();
        let reclaim_candidate_count = plan.reclaim_candidates.len();
        let reclaim_skipped_count = plan
            .stale_block_slab_ids
            .len()
            .saturating_sub(reclaim_candidate_count);
        let log_pressure = self.storage_log_compatibility_report(request.shard_id);
        let cache_pressure = self.storage_cache_inspection_report(request.shard_id);
        let block_slab_total_bytes = reclaim_live_bytes.saturating_add(reclaim_stale_bytes);
        let block_slab_stale_density_basis_points = if block_slab_total_bytes == 0 {
            0
        } else {
            reclaim_stale_bytes.saturating_mul(10_000) / block_slab_total_bytes
        };
        let delayed_destroy_slab_count = plan.delayed_destroy_block_slab_ids.len();
        let delayed_destroy_bytes = plan
            .reclaim_candidates
            .iter()
            .filter(|candidate| candidate.reason == "delayed_destroy")
            .map(|candidate| candidate.physical_bytes)
            .sum::<u64>();
        let expired_bucket_object_scan_debt = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
            .map(|shard| shard.expires_at_ms.len())
            .unwrap_or_default();
        let compaction_utility = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
            .map(|shard| compaction_utility_report(&self.block_store, shard))
            .unwrap_or_default();
        let compaction_debt_model_count = compaction_utility
            .model_policies
            .iter()
            .filter(|policy| {
                policy.stale_block_estimate > 0
                    || policy.stale_density_basis_points > 0
                    || policy.delete_marker_density_basis_points > 0
            })
            .count()
            .max(usize::from(block_slab_stale_density_basis_points > 0));
        let compaction_debt_score = compaction_utility
            .model_policies
            .iter()
            .map(|policy| {
                policy
                    .stale_block_estimate
                    .saturating_add(policy.stale_density_basis_points)
                    .saturating_add(policy.delete_marker_density_basis_points)
            })
            .sum::<u64>()
            .saturating_add(compaction_utility.stale_block_estimate)
            .saturating_add(reclaim_stale_bytes)
            .saturating_add(block_slab_stale_density_basis_points);
        let retention_prune_plan = self.bucket_dump_manifest_prune_plan_with_retention_refs(
            request.shard_id,
            request.follower_replay_cursors.clone(),
            request.raft_snapshot_refs.clone(),
        );
        let manifest_retention_blockers = retention_prune_plan
            .follower_blocks
            .len()
            .saturating_add(retention_prune_plan.raft_snapshot_blocks.len());
        let memory_cache_pressure_score = cache_pressure
            .stats
            .memory_bytes
            .saturating_add(cache_pressure.stats.pinned_bytes)
            .saturating_add(cache_pressure.stats.async_writeback_queue_bytes)
            .saturating_add(cache_pressure.stats.async_writeback_queue_depth);
        // The bucket index, which no memory term above can see. Kept out of
        // `memory_cache_pressure_score` on purpose -- see the field doc on
        // `StorageManagerPressureSignals::bucket_index_resident_bytes`: that score is re-derived
        // after the warm, by subtracting itself out of the total, and anything folded into it
        // would be subtracted away with it.
        let bucket_index_resident_bytes = self.bucket_index_resident_bytes(request.shard_id);
        let total_pressure_score = plan
            .dirty_buckets
            .len()
            .saturating_add(expired_bucket_object_scan_debt)
            .saturating_add(delayed_destroy_slab_count)
            .saturating_add(compaction_debt_model_count) as u64
            + plan.undumped_wal_records
            + log_pressure.wal_bytes
            + log_pressure.index_log_bytes
            + reclaim_stale_bytes
            + cache_pressure.stats.disk_bytes
            + memory_cache_pressure_score
            + bucket_index_resident_bytes
            + delayed_destroy_bytes
            + manifest_retention_blockers as u64
            + compaction_debt_score;
        let mut pressure_signals = StorageManagerPressureSignals {
            live_block_summaries_measured,
            dirty_bucket_count: plan.dirty_buckets.len(),
            undumped_wal_records: plan.undumped_wal_records,
            wal_bytes: log_pressure.wal_bytes,
            index_log_bytes: log_pressure.index_log_bytes,
            stale_block_bytes: reclaim_stale_bytes,
            live_block_bytes: reclaim_live_bytes,
            block_slab_stale_density_basis_points,
            memory_cache_bytes: cache_pressure.stats.memory_bytes,
            disk_cache_bytes: cache_pressure.stats.disk_bytes,
            bucket_index_resident_bytes,
            memory_cache_pressure_score,
            expired_bucket_object_scan_debt,
            delayed_destroy_slab_count,
            delayed_destroy_bytes,
            follower_cursor_retention_blockers: retention_prune_plan.follower_blocks.len(),
            raft_snapshot_retention_blockers: retention_prune_plan.raft_snapshot_blocks.len(),
            compaction_debt_model_count,
            compaction_debt_score,
            total_pressure_score,
        };
        let mut stages = Vec::new();
        let mut errors = Vec::new();
        // Everything above gets its own stage, for the reason mx#1435 gave for the dump-load
        // policy report: `stage_clock` runs from the previous stage's push, and this is the FIRST
        // stage boundary in the round. So `storage_lifecycle_plan` (which surveys the shard),
        // `storage_block_gc_dependency_plan` and the pressure-snapshot arithmetic were all being
        // charged to `prepare` -- which pre-allocates the next slab and surveys nothing, yet
        // reported 328 ms at 32,000 records.
        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "plan".to_string(),
            enabled: true,
            applied: true,
            skipped: false,
            reason: "lifecycle plan, page-GC dependency plan and the pressure snapshot"
                .to_string(),
            ..StorageManagerStageReport::default()
        });
        // PREPARE ROLLS THE NEXT DATA SLAB, which is the only thing this stage has to do that
        // nothing else in the round does.
        //
        // Rolling is not cheap: it fsyncs the outgoing slab, scans the slab directory to pick
        // the next id, creates and fsyncs the new file, fsyncs the parent directory and
        // persists the slab manifest. Left to `append`, all of that lands on one unlucky client
        // write as a latency outlier unrelated to the size of the write that triggered it.
        //
        // TWO ROUND DRIVERS, AND ONLY ONE OF THEM DID THIS. `run_storage_manager_once` has
        // pre-allocated here since the stage was written; this one -- the driver the data-node
        // worker and the embedded proxy both run -- reported `applied: true` for a stage whose
        // body was a report and nothing else. A deployment driven by this cycle therefore paid
        // the inline roll on the write path on every slab boundary, which is the exact cost the
        // stage exists to move off it.
        //
        // A FAILURE IS RECORDED AND DOES NOT STOP THE ROUND, matching the other driver: the
        // inline roll in `append` is still there, so the only consequence of a failure here is
        // that the next append pays for the roll exactly as it does today. That is a weaker
        // reaction than the design being followed, whose `Prepare` returns before any other
        // stage runs -- but there the failure is a stream error that also invalidates the
        // stages after it, and here it is not: nothing else in this round reads the active
        // slab's identity.
        //
        // A no-op while the active slab is under target, so the ordinary round pays one lock
        // and one comparison.
        let mut prepared_block_slab_id = None;
        if request.enable_prepare && !request.dry_run {
            let slab_target_bytes = if request.prepare_slab_target_bytes == 0 {
                crate::storage_config::effective_block_slab_target_bytes()
            } else {
                request.prepare_slab_target_bytes
            };
            match self
                .block_store()
                .prepare_next_slab_with_target(slab_target_bytes)
            {
                Ok(Some(roll)) => prepared_block_slab_id = Some(roll.new_block_slab_id),
                Ok(None) => {}
                Err(error) => errors.push(format!("prepare: {error}")),
            }
        }
        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "prepare".to_string(),
            enabled: request.enable_prepare,
            applied: request.enable_prepare && !request.dry_run,
            skipped: !request.enable_prepare,
            reason: if request.enable_prepare {
                "prepared storage lifecycle pressure view and index/page-store metadata".to_string()
            } else {
                "prepare disabled".to_string()
            },
            selected_block_slab_ids: plan.live_block_slab_ids.clone(),
            prepared_block_slab_id,
            pressure_signal:
                "dirty_slots+wal_bytes+index_log_bytes+stale_density+cache_pressure+expire_debt+delayed_destroy+retention_blockers+model_compaction_debt"
                    .to_string(),
            pressure_score: pressure_signals.total_pressure_score,
            pressure_threshold: 1,
            pressure_triggered: pressure_signals.total_pressure_score > 0,
            candidate_count: reclaim_candidate_count,
            skipped_count: reclaim_skipped_count,
            before_bytes: bucket_physical_bytes,
            after_bytes: bucket_physical_bytes,
            live_bytes: reclaim_live_bytes,
            stale_bytes: reclaim_stale_bytes,
            dirty_bucket_count: plan.dirty_buckets.len(),
            undumped_wal_records: plan.undumped_wal_records,
            metrics_bucket_count: bucket_count,
            metrics_block_ref_count: bucket_block_ref_count,
            ..StorageManagerStageReport::default()
        });

        let mut expiry_report = None;
        if request.enable_expire && !request.dry_run {
            match self.sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: request.shard_id,
                hot_cursor: request.expire_hot_cursor.clone(),
                cold_cursor: request.expire_cold_cursor.clone(),
                max_hot_buckets_per_round: request.max_expire_hot_buckets_per_round,
                max_cold_buckets_per_round: request.max_expire_cold_buckets_per_round,
                load_cold_buckets: request.load_cold_buckets_for_expire,
            }) {
                Ok(report) => expiry_report = Some(report),
                Err(err) => errors.push(format!("expire: {}", err.message)),
            }
        }

        // ONE PINNED SLAB NO LONGER SUPPRESSES THE REST.
        //
        // The dependency plan already works out, per slab, which candidates are pinned and which
        // are free, and `reclaimable_block_slab_ids` has always named the free subset. Both
        // reclaim stages then threw that away and consulted `safe_to_reclaim` -- a single
        // store-wide boolean that is false whenever ANY candidate is blocked. A shard with one
        // slab behind a lagging follower's replay cursor collected nothing at all, for as long
        // as that follower stayed behind, however many other slabs were free.
        //
        // Gating per slab instead requires the purge below to take a LIST, and the two changes
        // only make sense together: a purge that ran because some slab was free, but still swept
        // the whole trash directory, would destroy exactly the quarantined slabs the plan had
        // blocked. That is a loss path, not an inefficiency, which is why the collector's
        // selection and the purge's selection move in the same commit.
        // HAND THE COLLECTOR THE TALLY BEFORE IT PLANS.
        //
        // Without a publish the block store falls back to its older `used_bytes` figure, which
        // sums the file sizes of the NEIGHBOURING slabs that are not collectable -- a quantity
        // that is zero for every candidate by construction and would be wrong rather than zero if
        // it ever were not. Published, `used_bytes` is the live page bytes on the slab itself.
        //
        // Costs the number of slabs, not the number of pages: the shards keep the tally running
        // on their own mutation path and this only sums them.
        self.publish_block_slab_live_bytes();
        let reclaimable_block_slab_ids = block_gc_dependency_plan
            .reclaimable_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if request.enable_block_reclaim
            && !request.dry_run
            && !plan.reclaim_candidates.is_empty()
            && !reclaimable_block_slab_ids.is_empty()
        {
            let retain_from_block_slab_id = plan
                .reclaim_candidates
                .iter()
                .map(|candidate| candidate.block_slab_id)
                .max()
                .unwrap_or_default()
                .saturating_add(1);
            // Retain slabs live in ANY shard sharing this engine's page_store, not just the
            // shard being cycled (see the module header): otherwise a slab whose pages belong to
            // another shard is absent from this shard's live set and gets deleted.
            let reclaim_live_refs = self.live_block_slab_ids_all_shards();
            match self.block_store.gc_slabs_before_with_live_refs_policy_limited(
                retain_from_block_slab_id,
                reclaim_live_refs,
                // garbage-ratio victim selection: collect the highest-garbage slabs first,
                // keeping slabs below the garbage floor. Floor 0 (the default) collects every
                // eligible slab as before.
                BlockStoreGcPolicy::with_slab_garbage_floor(
                    request.block_gc_min_slab_garbage_basis_points,
                    None,
                ),
                true,
                Some(reclaimable_block_slab_ids.clone()),
            ) {
                Ok(report) => {
                    for block_slab_id in report
                        .removed_block_slab_ids
                        .iter()
                        .chain(report.delayed_destroy_block_slab_ids.iter())
                    {
                        let _ = self
                            .cache
                            .invalidate_page_segment(request.shard_id, *block_slab_id);
                    }
                }
                Err(err) => errors.push(format!("reclaim_page: {err}")),
            }
        }

        let lifecycle_report = if request.dry_run {
            None
        } else {
            let mut lifecycle_request = plan_request.clone();
            // The other half of the per-slab gate. The purge now runs whenever ANY quarantined
            // slab is free -- but it is handed the free set, so the slabs the plan blocked are
            // passed over rather than swept up by a round that was let in on someone else's
            // behalf. Flipping the condition without passing the list would turn a suppressed
            // reclaim into a destroyed dependency.
            lifecycle_request.purge_delayed_destroy = lifecycle_request.purge_delayed_destroy
                && !reclaimable_block_slab_ids.is_empty();
            lifecycle_request.purge_delayed_destroy_slab_ids =
                Some(reclaimable_block_slab_ids.iter().copied().collect());
            let report = Some(self.apply_storage_lifecycle(lifecycle_request));
            // AND AGAIN AFTERWARDS. The round just relocated pages onto a fresh slab and
            // destroyed others; the tally published above describes the state before it. A caller
            // that inspects the store after the round -- which is what every reclaim assertion
            // does -- should read what the round left, not what it found.
            self.publish_block_slab_live_bytes();
            report
        };
        // apply_storage_lifecycle's warm phase brings freshly-dumped pages into DRAM.
        // The pressure snapshot above was captured during prepare (pre-warm), so
        // re-measure memory residency here so the emitted snapshot reflects the warmed
        // cache. Only the memory fields are refreshed; disk_cache_bytes stays at its
        // prepare value (warmed pages also write through to SSD, and downstream
        // reclamation assertions compare against that pre-warm disk figure).
        if !request.dry_run {
            let warmed_cache = self.storage_cache_inspection_report(request.shard_id).stats;
            let warmed_memory_pressure = warmed_cache
                .memory_bytes
                .saturating_add(warmed_cache.pinned_bytes)
                .saturating_add(warmed_cache.async_writeback_queue_bytes)
                .saturating_add(warmed_cache.async_writeback_queue_depth);
            pressure_signals.total_pressure_score = pressure_signals
                .total_pressure_score
                .saturating_sub(pressure_signals.memory_cache_pressure_score)
                .saturating_add(warmed_memory_pressure);
            pressure_signals.memory_cache_bytes = warmed_cache.memory_bytes;
            pressure_signals.memory_cache_pressure_score = warmed_memory_pressure;
        }
        let eviction_report = if request.enable_evict {
            Some(if request.dry_run {
                StorageEvictionReport {
                    shard_id: request.shard_id,
                    mode: if request.eviction_delete_drop {
                        "delete_drop"
                    } else {
                        "evict_cache"
                    }
                    .to_string(),
                    pressure_before: pressure_signals
                        .memory_cache_pressure_score
                        .saturating_add(pressure_signals.disk_cache_bytes),
                    pressure_after: pressure_signals
                        .memory_cache_pressure_score
                        .saturating_add(pressure_signals.disk_cache_bytes),
                    memory_pressure_threshold: request.eviction_memory_pressure_threshold,
                    batch_limit: request.eviction_batch_limit,
                    dump_before_evict: request.eviction_dump_before_evict,
                    skipped_reason: "dry_run".to_string(),
                    ..StorageEvictionReport::default()
                }
            } else {
                self.apply_storage_eviction(
                    request.shard_id,
                    request.eviction_memory_pressure_threshold,
                    request.eviction_batch_limit,
                    request.eviction_dump_before_evict,
                    request.eviction_delete_drop,
                )
            })
        } else {
            None
        };
        // Pages whose only durable copy is a WAL record leave the log HERE, before the reclaim
        // plan is computed, because `min_registered_sequence` pins retention to the LOWEST
        // registration: a page still resident holds the floor down and no retention policy can
        // pass it. That is the order this stage's counterpart uses in the design this
        // follows -- dump the dirty slots, advance the dumped-log id, and only then reclaim.
        //
        // Until this ran, the only caller that moved a page out of the log was the write path,
        // which sweeps once registrations pass `TS_WAL_RESIDENT_PAGES` and therefore stops the
        // moment writes stop. An idle shard kept every page it had ever written that way.
        // Measured on 48 async writes: a flush left all 48 registered, and so did a full
        // eight-stage cycle, with the log floor still at sequence 1.
        //
        // Bounded per cycle at `max_dump_buckets_per_round`, so a shard sitting at the write
        // path's ceiling drains over several cycles instead of turning one into a long pause.
        // Oldest first, because the oldest registration is the one holding the floor down. The
        // bytes are already durable in the log, so this moves a copy that is safe either way.
        let wal_resident_blocks_before = self.wal_resident_block_count(request.shard_id);
        let wal_resident_blocks_materialised = if request.enable_wal_reclaim
            && !request.dry_run
            && wal_resident_blocks_before > 0
        {
            // Zero is how this request spells "no bound" -- its own default is 0 while the
            // scheduler's is 64 -- so a cycle asked for no bound drains the shard rather than
            // one page per pass.
            let per_round = if request.max_dump_buckets_per_round == 0 {
                usize::MAX
            } else {
                request.max_dump_buckets_per_round
            };
            self.materialize_oldest_resident_blocks(
                request.shard_id,
                wal_resident_blocks_before.saturating_sub(per_round),
            )
        } else {
            0
        };

        let wal_reclaim_plan = self.storage_wal_reclaim_plan(
            request.shard_id,
            request.follower_replay_cursors.clone(),
            request.raft_snapshot_refs.clone(),
        );
        let wal_reclaim_report = if request.enable_wal_reclaim {
            Some(if request.dry_run {
                StorageWalReclaimReport {
                    plan: wal_reclaim_plan.clone(),
                    applied: false,
                    ..StorageWalReclaimReport::default()
                }
            } else {
                self.apply_storage_wal_reclaim(wal_reclaim_plan.clone())
            })
        } else {
            None
        };
        let index_gc_report = Some(self.storage_index_gc_report(
            &plan,
            &wal_reclaim_plan,
            lifecycle_report.as_ref(),
            &request,
        ));

        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "reclaim_wal".to_string(),
            wal_resident_blocks_materialised,
            enabled: request.enable_wal_reclaim,
            applied: wal_reclaim_report
                .as_ref()
                .map(|report| report.applied)
                .unwrap_or(false),
            skipped: !request.enable_wal_reclaim
                || plan.dump_delayed
                || wal_reclaim_report
                    .as_ref()
                    .map(|report| !report.plan.safe_to_reclaim)
                    .unwrap_or(true),
            reason: if !request.enable_wal_reclaim {
                "wal reclaim disabled".to_string()
            } else if plan.dump_delayed {
                "dirty slot dump delayed until the configured undumped log threshold is reached"
                    .to_string()
            } else if wal_reclaim_report
                .as_ref()
                .map(|report| !report.plan.safe_to_reclaim)
                .unwrap_or(true)
            {
                format!(
                    "WAL/index-log reclaim blocked until durable slot generations and retention cursors allow it: {}",
                    wal_reclaim_report
                        .as_ref()
                        .map(|report| report.plan.blocker_reasons.join(","))
                        .unwrap_or_default()
                )
            } else {
                "reclaimed WAL/index-log through the slot-generation durable dump frontier"
                    .to_string()
            },
            selected_buckets: plan.selected_dump_buckets.clone(),
            pressure_signal:
                "durable_slot_generation_frontier+follower_snapshot_retention+wal_bytes+index_log_bytes"
                    .to_string(),
            pressure_score: pressure_signals
                .undumped_wal_records
                .saturating_add(pressure_signals.wal_bytes)
                .saturating_add(pressure_signals.index_log_bytes),
            pressure_threshold: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_wal_sequence)
                .unwrap_or(request.min_undumped_wal_records),
            pressure_triggered: request.enable_wal_reclaim
                && wal_reclaim_report
                    .as_ref()
                    .map(|report| report.plan.safe_to_reclaim)
                    .unwrap_or(false),
            candidate_count: plan.dirty_buckets.len(),
            skipped_count: plan
                .dirty_buckets
                .len()
                .saturating_sub(plan.selected_dump_buckets.len()),
            before_bytes: bucket_logical_bytes,
            after_bytes: bucket_logical_bytes,
            live_bytes: bucket_logical_bytes,
            dirty_bucket_count: plan.dirty_buckets.len(),
            undumped_wal_records: plan.undumped_wal_records,
            dumped_bucket_count: lifecycle_report
                .as_ref()
                .and_then(|report| report.dump_manifest.as_ref())
                .map(|manifest| manifest.bucket_ids.len())
                .unwrap_or_default(),
            wal_records_removed: wal_reclaim_report
                .as_ref()
                .map(|report| report.wal_records_removed)
                .unwrap_or_default(),
            index_log_records_removed: wal_reclaim_report
                .as_ref()
                .map(|report| report.index_log_records_removed)
                .unwrap_or_default(),
            retention_blockers: wal_reclaim_report
                .as_ref()
                .map(|report| {
                    report
                        .plan
                        .follower_cursor_block_count
                        .saturating_add(report.plan.raft_snapshot_block_count)
                })
                .unwrap_or_default(),
            retain_from_wal_sequence: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_wal_sequence)
                .unwrap_or_default(),
            retain_from_index_log_sequence: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_index_log_sequence)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "expire".to_string(),
            enabled: request.enable_expire,
            applied: request.enable_expire && !request.dry_run,
            skipped: !request.enable_expire,
            reason: if request.enable_expire {
                "swept expired logical records and persisted index updates".to_string()
            } else {
                "expire disabled".to_string()
            },
            pressure_signal: "expired_hot_slots+cold_slots+scan_cursors+load_on_expire_debt"
                .to_string(),
            pressure_score: expiry_report
                .as_ref()
                .map(|report| report.expired_records_removed as u64)
                .unwrap_or(pressure_signals.expired_bucket_object_scan_debt as u64),
            pressure_threshold: 1,
            pressure_triggered: pressure_signals.expired_bucket_object_scan_debt > 0
                || expiry_report
                    .as_ref()
                    .map(|report| report.expired_records_removed > 0)
                    .unwrap_or(false),
            before_bytes: bucket_logical_bytes,
            after_bytes: bucket_logical_bytes,
            expired_records_removed: expiry_report
                .as_ref()
                .map(|report| report.expired_records_removed)
                .unwrap_or_default(),
            candidate_count: expiry_report
                .as_ref()
                .map(|report| report.scanned_records)
                .unwrap_or(pressure_signals.expired_bucket_object_scan_debt),
            skipped_count: expiry_report
                .as_ref()
                .map(|report| report.skipped_records)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "evict".to_string(),
            enabled: request.enable_evict,
            applied: eviction_report
                .as_ref()
                .map(|report| {
                    report.cache_entries_removed > 0
                        || report.cache_disk_bytes_removed > 0
                        || report.dropped_object_count > 0
                })
                .unwrap_or(false),
            skipped: !request.enable_evict
                || eviction_report
                    .as_ref()
                    .map(|report| !report.pressure_gate_open)
                    .unwrap_or(true),
            reason: if !request.enable_evict {
                "evict disabled".to_string()
            } else if eviction_report
                .as_ref()
                .map(|report| !report.pressure_gate_open)
                .unwrap_or(false)
            {
                "eviction skipped because memory/cache pressure is below threshold".to_string()
            } else if eviction_report
                .as_ref()
                .map(|report| report.cooldown)
                .unwrap_or(false)
            {
                "eviction entered cooldown because pressure did not decrease".to_string()
            } else {
                "evicted weighted slot/object victims under memory/cache pressure".to_string()
            },
            pressure_signal:
                "weighted_slot_object_eviction+memory_pressure_gate+bucket_index_resident+batch_limit"
                    .to_string(),
            // The index term belongs HERE and only here among the stages: evict is the one stage
            // that can release it. `reclaim_memory` invalidates cached pages and leaves the index
            // exactly where it was, so reporting index bytes as its pressure would name a debt it
            // has no way to pay.
            pressure_score: pressure_signals
                .memory_cache_pressure_score
                .saturating_add(pressure_signals.disk_cache_bytes)
                .saturating_add(pressure_signals.bucket_index_resident_bytes)
                .saturating_add(
                    eviction_report
                        .as_ref()
                        .map(|report| {
                            report.cache_entries_removed as u64 + report.cache_disk_bytes_removed
                        })
                        .unwrap_or_default(),
                ),
            pressure_threshold: request.eviction_memory_pressure_threshold,
            pressure_triggered: pressure_signals.memory_cache_pressure_score > 0
                || pressure_signals.disk_cache_bytes > 0
                || pressure_signals.bucket_index_resident_bytes > 0
                || eviction_report
                    .as_ref()
                    .map(|report| {
                        report.cache_entries_removed > 0 || report.cache_disk_bytes_removed > 0
                    })
                    .unwrap_or(false),
            before_bytes: eviction_report
                .as_ref()
                .map(|report| report.pressure_before)
                .unwrap_or_default(),
            after_bytes: eviction_report
                .as_ref()
                .map(|report| report.pressure_after)
                .unwrap_or_default(),
            cache_entries_removed: eviction_report
                .as_ref()
                .map(|report| report.cache_entries_removed)
                .unwrap_or_default(),
            cache_disk_bytes_removed: eviction_report
                .as_ref()
                .map(|report| report.cache_disk_bytes_removed)
                .unwrap_or_default(),
            selected_buckets: eviction_report
                .as_ref()
                .map(|report| {
                    report
                        .selected_victims
                        .iter()
                        .map(|victim| victim.routing_bucket)
                        .collect()
                })
                .unwrap_or_default(),
            candidate_count: eviction_report
                .as_ref()
                .map(|report| report.selected_victims.len())
                .unwrap_or_default(),
            eviction_pressure_before: eviction_report
                .as_ref()
                .map(|report| report.pressure_before)
                .unwrap_or_default(),
            eviction_pressure_after: eviction_report
                .as_ref()
                .map(|report| report.pressure_after)
                .unwrap_or_default(),
            eviction_cooldown: eviction_report
                .as_ref()
                .map(|report| report.cooldown)
                .unwrap_or(false),
            dropped_object_count: eviction_report
                .as_ref()
                .map(|report| report.dropped_object_count)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "reclaim_page".to_string(),
            enabled: request.enable_block_reclaim,
            applied: request.enable_block_reclaim
                && !request.dry_run
                && block_gc_dependency_plan.safe_to_reclaim
                && lifecycle_report
                    .as_ref()
                    .map(|report| !report.delayed_destroy_purged_slabs.is_empty())
                    .unwrap_or(false),
            skipped: !request.enable_block_reclaim
                || plan.reclaim_candidates.is_empty()
                || !block_gc_dependency_plan.safe_to_reclaim,
            reason: if !request.enable_block_reclaim {
                "page reclaim disabled".to_string()
            } else if plan.reclaim_candidates.is_empty() {
                "no stale or delayed-destroy page segments are reclaimable".to_string()
            } else if !block_gc_dependency_plan.safe_to_reclaim {
                format!(
                    "page GC refused because retained dependencies remain: {}",
                    block_gc_dependency_plan.blocker_reasons.join(",")
                )
            } else {
                "reclaimed delayed-destroy page segments selected by stale-byte pressure"
                    .to_string()
            },
            pressure_signal:
                "stale_page_bytes+delayed_destroy_backlog+stale_density+dependency_retention"
                    .to_string(),
            pressure_score: pressure_signals
                .stale_block_bytes
                .saturating_add(pressure_signals.delayed_destroy_bytes)
                .saturating_add(pressure_signals.block_slab_stale_density_basis_points),
            pressure_threshold: 1,
            pressure_triggered: request.enable_block_reclaim
                && !plan.reclaim_candidates.is_empty()
                && block_gc_dependency_plan.safe_to_reclaim,
            candidate_count: reclaim_candidate_count,
            skipped_count: reclaim_skipped_count
                .saturating_add(block_gc_dependency_plan.blocked_block_slab_ids.len()),
            before_bytes: reclaim_live_bytes + reclaim_stale_bytes,
            after_bytes: reclaim_live_bytes,
            live_bytes: reclaim_live_bytes,
            stale_bytes: reclaim_stale_bytes,
            selected_block_slab_ids: block_gc_dependency_plan.reclaimable_block_slab_ids.clone(),
            block_slabs_reclaimed: lifecycle_report
                .as_ref()
                .map(|report| report.delayed_destroy_purged_slabs.len())
                .unwrap_or_default(),
            block_bytes_reclaimed: lifecycle_report
                .as_ref()
                .map(|report| report.delayed_destroy_purged_bytes)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "index_gc".to_string(),
            enabled: request.enable_index_gc,
            applied: request.enable_index_gc
                && !request.dry_run
                && (lifecycle_report
                    .as_ref()
                    .and_then(|report| report.manifest_prune_report.as_ref())
                    .map(|report| {
                        !report.removed_manifest_ids.is_empty() || report.removed_marker_files > 0
                    })
                    .unwrap_or(false)
                    || index_gc_report
                        .as_ref()
                        .map(|report| report.applied)
                        .unwrap_or(false)),
            skipped: !request.enable_index_gc,
            reason: if request.enable_index_gc {
                "pruned obsolete manifests, rolled forward safe install markers, and applied thresholded index-log GC"
                    .to_string()
            } else {
                "index GC disabled".to_string()
            },
            pressure_signal: "obsolete_manifests+install_markers+index_log_bytes+usage_ratio+max_entries"
                .to_string(),
            pressure_score: lifecycle_report
                .as_ref()
                .map(|report| {
                    report
                        .manifest_prune_report
                        .as_ref()
                        .map(|prune| prune.removed_manifest_ids.len() + prune.removed_marker_files)
                        .unwrap_or_default()
                        + report.install_roll_forward_reports.len()
                })
                .unwrap_or_default() as u64
                + pressure_signals.follower_cursor_retention_blockers as u64
                + pressure_signals.raft_snapshot_retention_blockers as u64
                + index_gc_report
                    .as_ref()
                    .map(|report| {
                        report
                            .bytes_before
                            .saturating_add(report.usage_ratio_basis_points)
                    })
                    .unwrap_or_default(),
            pressure_threshold: index_gc_report
                .as_ref()
                .map(|report| {
                    report
                        .bytes_threshold
                        .max(report.usage_ratio_trigger_basis_points)
                })
                .unwrap_or(1),
            pressure_triggered: request.enable_index_gc
                && (lifecycle_report
                    .as_ref()
                    .map(|report| {
                        report.manifest_prune_report.is_some()
                            || !report.install_roll_forward_reports.is_empty()
                    })
                    .unwrap_or(false)
                    || index_gc_report
                        .as_ref()
                        .map(|report| report.threshold_triggered || report.usage_ratio_triggered)
                        .unwrap_or(false)),
            candidate_count: lifecycle_report
                .as_ref()
                .map(|report| {
                    report
                        .manifest_prune_report
                        .as_ref()
                        .map(|prune| prune.removed_manifest_ids.len() + prune.removed_marker_files)
                        .unwrap_or_default()
                        + report.install_roll_forward_reports.len()
                })
                .unwrap_or_default()
                + index_gc_report
                    .as_ref()
                    .map(|report| report.removable_records_before_budget)
                    .unwrap_or_default(),
            skipped_count: lifecycle_report
                .as_ref()
                .map(|report| report.manifest_prune_plan.blocked_manifest_ids.len())
                .unwrap_or_default()
                + index_gc_report
                    .as_ref()
                    .map(|report| {
                        report
                            .removable_records_before_budget
                            .saturating_sub(report.records_removed)
                    })
                    .unwrap_or_default(),
            before_bytes: index_gc_report
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            after_bytes: index_gc_report
                .as_ref()
                .map(|report| report.bytes_after)
                .unwrap_or_default(),
            manifest_pruned_count: lifecycle_report
                .as_ref()
                .and_then(|report| report.manifest_prune_report.as_ref())
                .map(|report| report.removed_manifest_ids.len() + report.removed_marker_files)
                .unwrap_or_default(),
            install_roll_forward_count: lifecycle_report
                .as_ref()
                .map(|report| report.install_roll_forward_reports.len())
                .unwrap_or_default(),
            index_log_records_removed: index_gc_report
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        let mut merged_dump_load_policy =
            self.storage_merged_dump_load_policy_report(StorageMergedDumpLoadPolicyRequest {
                lifecycle: plan_request.clone(),
                create_dump_manifest: request.enable_wal_reclaim,
                install_dump_manifest: false,
            });
        // Its own stage, because it is its own work: this report runs a lifecycle plan, an apply
        // and a recovery-boundary report (which reads every live page).
        //
        // `stage_clock` measures from the PREVIOUS stage's push, so before this entry existed
        // every millisecond of that landed on `compact`. Compaction then read as the most
        // expensive stage of the round at 32,000 records -- about 1.7 s -- while reporting
        // `skipped=true` and moving zero pages, because it was being charged for work it does
        // not do. A stage report that names the wrong stage is worse than no stage report: it
        // sends whoever reads it to optimise the wrong function, which is exactly what it did.
        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "merged_dump_load_policy".to_string(),
            enabled: true,
            applied: true,
            skipped: false,
            reason: "dump-load policy: plan, apply and recovery boundary".to_string(),
            ..StorageManagerStageReport::default()
        });

        let should_compact = request.enable_block_compaction
            && !request.dry_run
            && (!plan.reclaim_candidates.is_empty() || plan.live_block_slab_ids.len() > 1);
        let compaction_report = if should_compact {
            match self.compact_shard_blocks(request.shard_id) {
                Ok(report) => Some(report),
                Err(err) => {
                    errors.push(format!("compact: {}", err.message));
                    None
                }
            }
        } else {
            None
        };
        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "compact".to_string(),
            enabled: request.enable_block_compaction,
            applied: compaction_report.is_some(),
            skipped: !request.enable_block_compaction
                || request.dry_run
                || (plan.reclaim_candidates.is_empty() && plan.live_block_slab_ids.len() <= 1),
            reason: if !request.enable_block_compaction {
                "page compaction disabled".to_string()
            } else if request.dry_run {
                "dry run reports compaction pressure without rewriting pages".to_string()
            } else if plan.reclaim_candidates.is_empty() && plan.live_block_slab_ids.len() <= 1 {
                "compaction skipped because page density does not show stale-segment pressure"
                    .to_string()
            } else {
                "rewrote live model page references into a fresh compacted segment".to_string()
            },
            pressure_signal: "model_layout_compaction_debt+stale_segment_density".to_string(),
            pressure_score: pressure_signals
                .compaction_debt_score
                .saturating_add(pressure_signals.block_slab_stale_density_basis_points),
            pressure_threshold: 1,
            pressure_triggered: should_compact,
            candidate_count: plan.stale_block_slab_ids.len(),
            skipped_count: plan.stale_block_slab_ids.len().saturating_sub(
                compaction_report
                    .as_ref()
                    .map(|report| report.stale_block_slab_ids.len())
                    .unwrap_or_default(),
            ),
            before_bytes: bucket_physical_bytes,
            after_bytes: compaction_report
                .as_ref()
                .map(|_| bucket_logical_bytes)
                .unwrap_or(bucket_physical_bytes),
            live_bytes: bucket_logical_bytes,
            stale_bytes: reclaim_stale_bytes,
            selected_block_slab_ids: compaction_report
                .as_ref()
                .map(|report| report.stale_block_slab_ids.clone())
                .unwrap_or_default(),
            compacted_block_slab_id: compaction_report
                .as_ref()
                .map(|report| report.compacted_block_slab_id),
            rewritten_block_refs: compaction_report
                .as_ref()
                .map(|report| report.rewritten_block_refs)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        let compaction_policy_applied =
            request.dry_run || plan.reclaim_candidates.is_empty() || compaction_report.is_some();
        if compaction_policy_applied {
            merged_dump_load_policy
                .blockers
                .retain(|blocker| blocker != "compaction");
        } else if !merged_dump_load_policy
            .blockers
            .iter()
            .any(|blocker| blocker == "compaction")
        {
            merged_dump_load_policy
                .blockers
                .push("compaction".to_string());
        }
        merged_dump_load_policy.policy_ready = merged_dump_load_policy.blockers.is_empty();

        stages.push(StorageManagerStageReport {
            duration_ms: {
                let elapsed = stage_clock.elapsed().as_millis() as u64;
                stage_clock = std::time::Instant::now();
                elapsed
            },
            stage: "reap_metrics".to_string(),
            enabled: true,
            applied: !request.dry_run,
            // An unmeasured round says so HERE, in the stage an operator reads for these
            // numbers, rather than leaving them to be read as a shard that emptied itself.
            reason: if live_block_summaries_measured {
                "reported slot/page/cache pressure metrics for the completed cycle".to_string()
            } else {
                "no slot was dirty, so the round did not walk the live pages: the slot and page \
counts on this stage are NOT MEASURED and read 0, they are not a measurement of zero"
                    .to_string()
            },
            skipped: !live_block_summaries_measured,
            pressure_signal: "slot_page_cache_metrics".to_string(),
            pressure_score: bucket_count as u64 + bucket_block_ref_count,
            pressure_threshold: 1,
            pressure_triggered: bucket_summaries
                .map(|summaries| !summaries.is_empty())
                .unwrap_or(false),
            before_bytes: bucket_physical_bytes,
            after_bytes: bucket_physical_bytes,
            live_bytes: bucket_logical_bytes,
            stale_bytes: reclaim_stale_bytes,
            metrics_bucket_count: bucket_count,
            metrics_block_ref_count: bucket_block_ref_count,
            ..StorageManagerStageReport::default()
        });
        let phase_executor =
            StorageManagerPhaseExecutor::new(cycle_started_unix_ms, cycle_started_at);
        let round_duration_ms = phase_executor.annotate_reports(
            &mut stages,
            &errors,
            pressure_signals.follower_cursor_retention_blockers
                + pressure_signals.raft_snapshot_retention_blockers,
        );

        // MANIFEST-CONFORMANCE FOLD threshold dump (gate on only, never on a dry run): if the undumped
        // index-log gap has crossed `index_dump_wal_gap_bytes`, materialize the base index +
        // fold the slab/zone catalog into an index-log anchor here, in the background cycle --
        // mirroring this design's background `StorageManager` dump-on-WAL-gap cadence, never
        // per write. No-op with the gate off, so the cycle stays byte-identical when the fold is
        // not enabled.
        if !request.dry_run {
            self.maybe_dump_index_catalog(request.shard_id);
        }

        let production_parity_slice = errors.is_empty()
            && native_stage_order
                .iter()
                .all(|stage| stages.iter().any(|report| &report.stage == stage))
            && stages.iter().all(|stage| stage.enabled)
            && merged_dump_load_policy.policy_ready;
        StorageManagerCycleReport {
            shard_id: request.shard_id,
            duration_ms: round_duration_ms,
            dry_run: request.dry_run,
            native_stage_order,
            completed: errors.is_empty(),
            production_parity_slice,
            pressure_snapshot: pressure_signals.clone(),
            pressure_signals,
            stages,
            plan,
            merged_dump_load_policy,
            lifecycle_report,
            expiry_report,
            compaction_report,
            wal_reclaim_report,
            index_gc_report,
            eviction_report,
            block_gc_dependency_plan,
            errors,
        }
    }

    pub fn storage_merged_dump_load_policy_report(
        &self,
        request: StorageMergedDumpLoadPolicyRequest,
    ) -> StorageMergedDumpLoadPolicyReport {
        let mut lifecycle_request = request.lifecycle.clone();
        lifecycle_request.roll_forward_bucket_dump_installs = true;
        let lifecycle = if request.create_dump_manifest {
            self.apply_storage_lifecycle(lifecycle_request.clone())
        } else {
            let plan = self.storage_lifecycle_plan(lifecycle_request.clone());
            let manifest_prune_plan = self.bucket_dump_manifest_prune_plan_with_follower_cursors(
                lifecycle_request.shard_id,
                lifecycle_request.follower_replay_cursors.clone(),
            );
            StorageLifecycleReport {
                shard_id: lifecycle_request.shard_id,
                plan,
                manifest_prune_plan,
                install_roll_forward_reports: self
                    .bucket_dump_install_roll_forward_reports(lifecycle_request.shard_id),
                object_lifecycle: self
                    .storage_object_lifecycle_snapshot(lifecycle_request.shard_id),
                ..StorageLifecycleReport::default()
            }
        };
        let manifest = lifecycle
            .dump_manifest
            .clone()
            .or_else(|| latest_bucket_dump_manifest_at(&self.index_dir, lifecycle_request.shard_id));
        // Sampled, not the whole store. This report is here for `manifest_chain_issues`, the two
        // dump sequences and the two replay sequences, none of which reads a page -- but it also
        // CARRIES `stale_index_page_refs` / `unreadable_page_bytes`, so the readability check is
        // bounded rather than skipped: corruption is still found, across rounds instead of all in
        // one. Measured at 32,000 records the unbounded version was 575 ms and 32,000 reads,
        // about a fifth of the round.
        let boundary = self.storage_recovery_boundary_report_sampled(
            lifecycle_request.shard_id,
            RECOVERY_READABLE_PROBE_PER_ROUND,
        );
        let manifest_prune_plan = self.bucket_dump_manifest_prune_plan_with_follower_cursors(
            lifecycle_request.shard_id,
            lifecycle_request.follower_replay_cursors.clone(),
        );
        // Report the roll-forward recoveries that the lifecycle pass above actually
        // performed. Re-deriving them from the current interrupted-install set here
        // would return empty, because apply_storage_lifecycle already rolled the
        // interrupted installs forward (and cleared their markers).
        let install_roll_forward_reports = lifecycle.install_roll_forward_reports.clone();
        // Sampled, because this round is not installing anything: `install_dump_manifest` is
        // false here, the result feeds a reported readiness flag and gates nothing destructive,
        // and the unbounded form read every page the manifest names -- the last whole-store pass
        // left in a round. A real install still uses the unbounded one, where a sample could let
        // a dump with unreadable pages through.
        let load_preflight = manifest.as_ref().map(|manifest| {
            if request.install_dump_manifest {
                self.bucket_dump_install_preflight_report(manifest)
            } else {
                self.bucket_dump_install_preflight_report_sampled(
                    manifest,
                    RECOVERY_READABLE_PROBE_PER_ROUND,
                )
            }
        });
        let install_status = if request.install_dump_manifest {
            manifest
                .as_ref()
                .map(|manifest| match self.install_bucket_dump_manifest(manifest) {
                    Ok(()) => Status::ok(),
                    Err(status) => status,
                })
        } else {
            None
        };
        let manifest_chain_valid = boundary.manifest_chain_issues.is_empty();
        // Safe means nothing is left unservable, NOT that no cursor exists. A cursor anchored on
        // a manifest the plan retains is served by that manifest. Only one that precedes every
        // manifest has nothing to replay from. Emptiness stopped being the right test the moment
        // every cursor is recorded rather than only those that kept an extra manifest -- read as
        // emptiness, index GC would be blocked by the mere existence of a healthy follower.
        let follower_retention_safe = !manifest_prune_plan.follower_blocks.iter().any(|block| {
            block.reason == crate::engine::bucket_dump_io::FOLLOWER_PRECEDES_EVERY_MANIFEST
        }) && !manifest_prune_plan.raft_snapshot_blocks.iter().any(|block| {
            block.reason == crate::engine::bucket_dump_io::RAFT_SNAPSHOT_PRECEDES_EVERY_MANIFEST
        });
        let load_preflight_safe = load_preflight
            .as_ref()
            .map(|preflight| preflight.install_safe)
            .unwrap_or(false);
        let load_installed = install_status
            .as_ref()
            .map(|status| status.ok)
            .unwrap_or(!request.install_dump_manifest);
        let replay_boundary_safe = manifest
            .as_ref()
            .map(|manifest| {
                // The replay base is the dumped checkpoint (which covers up to its own
                // sequence) plus the retained WAL tail above it. Once the WAL is
                // legitimately reclaimed past a dump (the dumped-log-id anchor + WAL
                // truncate step), latest_safe_* -- and thus selected_replay_* -- can sit
                // below the manifest; the manifest checkpoint still covers it, so gate on
                // the dump frontier rather than the possibly-reclaimed WAL tail.
                boundary.latest_dump_wal_sequence >= manifest.wal_sequence
                    && boundary.latest_dump_index_log_sequence >= manifest.index_log_sequence
            })
            .unwrap_or(false);
        let index_gc_ready = install_roll_forward_reports.iter().all(|report| {
            report.can_roll_forward || report.can_retry_install || report.reason == "commit_ready"
        }) && manifest_chain_valid;

        let mut blockers = Vec::new();
        if manifest.is_none() {
            blockers.push("missing_dump_manifest".to_string());
        }
        if !load_preflight_safe {
            blockers.push("load_preflight_unsafe".to_string());
        }
        if !load_installed {
            blockers.push("load_install_failed".to_string());
        }
        if !replay_boundary_safe {
            blockers.push("replay_boundary_before_dump_manifest".to_string());
        }
        if !manifest_chain_valid {
            blockers.push("broken_manifest_chain".to_string());
        }
        if !follower_retention_safe {
            blockers.push("retention_cursor_blocks_index_gc".to_string());
        }
        if !index_gc_ready {
            blockers.push("index_gc_not_ready".to_string());
        }
        let policy_ready = blockers.is_empty();
        let (
            manifest_id,
            manifest_bucket_ids,
            manifest_block_slab_ids,
            manifest_wal_sequence,
            manifest_index_log_sequence,
        ) = manifest
            .as_ref()
            .map(|manifest| {
                (
                    Some(manifest.manifest_id.clone()),
                    manifest.bucket_ids.clone(),
                    manifest.block_slab_ids.clone(),
                    manifest.wal_sequence,
                    manifest.index_log_sequence,
                )
            })
            .unwrap_or_default();

        StorageMergedDumpLoadPolicyReport {
            shard_id: lifecycle_request.shard_id,
            policy_ready,
            dump_manifest_created: lifecycle.dump_manifest.is_some(),
            load_preflight_safe,
            load_installed,
            replay_boundary_safe,
            manifest_chain_valid,
            follower_retention_safe,
            index_gc_ready,
            manifest_id,
            manifest_bucket_ids,
            manifest_block_slab_ids,
            manifest_wal_sequence,
            manifest_index_log_sequence,
            selected_replay_wal_sequence: boundary.selected_replay_wal_sequence,
            selected_replay_index_log_sequence: boundary.selected_replay_index_log_sequence,
            lifecycle,
            load_preflight,
            install_status,
            boundary,
            manifest_prune_plan,
            install_roll_forward_reports,
            evidence: vec![
                "merged dump/load policy coordinates dirty-slot dump selection, manifest checksum/generation validation, load preflight, recovery replay boundary, roll-forward markers, and follower-safe manifest retention".to_string(),
                "policy report fails closed when manifest, load, replay, chain, retention, or index-GC evidence is unsafe".to_string(),
            ],
            blockers,
        }
    }
}
