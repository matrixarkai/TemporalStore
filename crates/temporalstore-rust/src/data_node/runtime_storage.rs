// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! DataNodeRuntime storage-manager scheduling/lifecycle methods, extracted from data_node.rs.

use super::round_budget::{
    options_for_slot, BoundedStage, HeavySlot, PageCursor, ShardRoundState, StageBudget,
    StageContext, StageProgress, StageStep,
};
use super::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// Every input the loop's WAL-reclaim stage reads cheaply before it decides whether a reclaim
/// could do anything.
///
/// A shard nobody writes to asks the stage the same question every round and gets the same
/// answer, and answering it builds the reclaim plan, which walks every live block in the shard.
/// When the last round that asked found nothing to reclaim, this is what the next round compares
/// against: if none of it has moved, the answer cannot have either.
///
/// A WRITE moves `wal_last_sequence`. A truncate of the write-ahead log moves
/// `wal_persistent_bytes`. A dump clears dirty buckets, which moves `dirty_buckets` and
/// `undumped_wal_records`. Moving a block out of the log moves `resident_blocks`, which lowers the
/// retention floor without touching any sequence. A change to the cursors or snapshot references
/// the caller passes moves `retention_inputs`.
///
/// THE INDEX LOG'S LENGTH IS NOT PART OF THE KEY. It was, and a compaction step that moves blocks
/// appends one small index record per round, so on a shard that is draining stale slabs the key
/// moved every round and the stage built the reclaim plan, which walks every live block, every
/// round: the log grew by 53 bytes a round and the stage paid five whole-shard walks for it.
/// Nothing that decides whether the write-ahead log is reclaimable lives in the index log's tail:
/// the floors come from the write-ahead log, the retained dumps and the resident blocks, and the
/// index log is truncated by its own stage.
///
/// `quiet_minute` is the backstop for any input this key does not see: a remembered answer
/// is good for at most one minute (until the wall clock's minute changes), then the stage asks
/// again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QuietWalReclaimKey {
    dump_wal_sequence: u64,
    wal_last_sequence: u64,
    wal_persistent_bytes: u64,
    resident_blocks: usize,
    dirty_buckets: usize,
    undumped_wal_records: u64,
    retention_inputs: u64,
    quiet_minute: u64,
}

/// How long a remembered "nothing to reclaim" stays good, in seconds.
pub(super) const QUIET_WAL_RECLAIM_MAX_AGE_SECS: u64 = 60;

impl QuietWalReclaimKey {
    /// The key from its inputs. Kept apart from the reads so a test can move one input at a time.
    pub(super) fn from_inputs(
        dump_wal_sequence: u64,
        wal_last_sequence: u64,
        wal_persistent_bytes: u64,
        resident_blocks: usize,
        dirty_buckets: usize,
        undumped_wal_records: u64,
        retention_inputs: u64,
        now_unix_secs: u64,
    ) -> Self {
        Self {
            dump_wal_sequence,
            wal_last_sequence,
            wal_persistent_bytes,
            resident_blocks,
            dirty_buckets,
            undumped_wal_records,
            retention_inputs,
            quiet_minute: now_unix_secs / QUIET_WAL_RECLAIM_MAX_AGE_SECS,
        }
    }
}

fn record_purge_work(
    report: &mut Option<StorageLifecycleReport>,
    plan: &StorageLifecyclePlan,
    purge: crate::block_store::BlockStorePurgeDelayedDestroyReport,
    detailed_report: bool,
) {
    if !detailed_report
        || (purge.purged_block_slab_ids.is_empty() && purge.restored_block_slab_ids.is_empty())
    {
        return;
    }
    // Use the plan already computed for this round; reporting must not run maintenance again.
    let report = report.get_or_insert_with(|| StorageLifecycleReport {
        shard_id: plan.shard_id,
        plan: plan.clone(),
        ..StorageLifecycleReport::default()
    });
    report.delayed_destroy_purged_slabs.extend(purge.purged_block_slab_ids);
    report.delayed_destroy_purged_bytes = report.delayed_destroy_purged_bytes
        .saturating_add(purge.purged_physical_bytes);
    report.delayed_destroy_restored_slabs.extend(purge.restored_block_slab_ids);
    report.delayed_destroy_restored_bytes = report.delayed_destroy_restored_bytes
        .saturating_add(purge.restored_physical_bytes);
    report.refresh_public_lifecycle_metrics();
}

#[cfg(test)]
mod purge_report_tests {
    use super::*;

    #[test]
    fn compact_scheduler_reporting_does_not_build_a_lifecycle_report() {
        let mut report = None;
        record_purge_work(
            &mut report,
            &StorageLifecyclePlan::default(),
            crate::block_store::BlockStorePurgeDelayedDestroyReport {
                purged_block_slab_ids: vec![1], purged_physical_bytes: 128,
                ..Default::default()
            },
            false,
        );
        assert!(report.is_none());
    }
}

impl DataNodeRuntime {
    fn quiet_wal_reclaim_key(
        &self,
        shard_id: ShardId,
        options: &StorageManagerOptions,
        dirty_buckets: usize,
        undumped_wal_records: u64,
    ) -> QuietWalReclaimKey {
        use std::hash::{Hash, Hasher};
        // `stats` answers the last sequence with a scan of the active piece; the cached variant
        // scans once per shard per process and is already trusted for the per-write index anchor.
        let wal = self
            .inner
            .engine
            .write_ahead_log_store()
            .stats_without_rescan(shard_id);
        let retention_inputs = if options.follower_replay_cursors.is_empty()
            && options.raft_snapshot_refs.is_empty()
        {
            0
        } else {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            format!(
                "{:?}|{:?}",
                options.follower_replay_cursors, options.raft_snapshot_refs
            )
            .hash(&mut hasher);
            hasher.finish()
        };
        QuietWalReclaimKey::from_inputs(
            self.inner.engine.storage_dump_wal_sequence(shard_id).unwrap_or(0),
            wal.last_sequence,
            wal.persistent_bytes,
            self.inner.engine.wal_resident_block_count(shard_id),
            dirty_buckets,
            undumped_wal_records,
            retention_inputs,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs())
                .unwrap_or(0),
        )
    }
}

impl DataNodeRuntime {
    fn reclaim_page_log(
        &self,
        shard_id: ShardId,
        options: &StorageManagerOptions,
        pressure: &StorageManagerPressureSnapshot,
    ) -> Option<crate::engine::reports::StorageWalReclaimReport> {
        if self.inner.engine.storage_dump_wal_sequence(shard_id).unwrap_or(0) == 0 {
            return None;
        }
        let key = self.quiet_wal_reclaim_key(
            shard_id, options, pressure.dirty_bucket_count, pressure.undumped_wal_records,
        );
        let readers = self.inner.engine.has_registered_replay_readers(shard_id);
        {
            let mut memo = self.inner.quiet_wal_reclaim.lock().expect("quiet wal reclaim lock poisoned");
            if readers {
                memo.remove(&shard_id);
            } else if memo.get(&shard_id).is_some_and(|(held, _)| held == &key) {
                return None;
            }
        }
        if self.inner.engine.periodic_wal_reclaim_is_pinned(shard_id) {
            return None;
        }
        let plan = self.inner.engine.storage_wal_reclaim_plan(
            shard_id, options.follower_replay_cursors.clone(), options.raft_snapshot_refs.clone(),
        );
        let report = self.inner.engine.apply_storage_wal_reclaim(plan);
        let unchanged = report.wal_records_removed == 0 && report.wal_segments_dropped == 0;
        let mut memo = self.inner.quiet_wal_reclaim.lock().expect("quiet wal reclaim lock poisoned");
        // Failed collection must retry. Reader release must also cause a fresh decision.
        if !readers && (!report.plan.safe_to_reclaim || (report.applied && unchanged)) {
            memo.insert(shard_id, (key, report.plan.safe_to_reclaim));
        } else {
            memo.remove(&shard_id);
        }
        Some(report)
    }
}

#[cfg(test)]
mod quiet_wal_reclaim_key_tests {
    use super::*;
    const NOW: u64 = 1_800_000_000;

    fn base() -> QuietWalReclaimKey {
        QuietWalReclaimKey::from_inputs(9, 10, 4096, 3, 0, 0, 0, NOW)
    }

    #[test]
    fn each_input_the_key_keeps_moves_it_and_nothing_else_does() {
        assert_eq!(base(), base());
        let moved = [
            ("dump_wal_sequence", QuietWalReclaimKey::from_inputs(10, 10, 4096, 3, 0, 0, 0, NOW)),
            ("wal_last_sequence", QuietWalReclaimKey::from_inputs(9, 11, 4096, 3, 0, 0, 0, NOW)),
            ("wal_persistent_bytes", QuietWalReclaimKey::from_inputs(9, 10, 8192, 3, 0, 0, 0, NOW)),
            ("resident_blocks", QuietWalReclaimKey::from_inputs(9, 10, 4096, 2, 0, 0, 0, NOW)),
            ("dirty_buckets", QuietWalReclaimKey::from_inputs(9, 10, 4096, 3, 1, 0, 0, NOW)),
            ("undumped_wal_records", QuietWalReclaimKey::from_inputs(9, 10, 4096, 3, 0, 1, 0, NOW)),
            ("retention_inputs", QuietWalReclaimKey::from_inputs(9, 10, 4096, 3, 0, 0, 7, NOW)),
        ];
        for (name, key) in moved {
            assert_ne!(base(), key, "{name} must move the key");
        }
    }

    #[test]
    fn a_remembered_answer_is_good_for_a_minute_and_no_longer() {
        let start = (NOW / QUIET_WAL_RECLAIM_MAX_AGE_SECS) * QUIET_WAL_RECLAIM_MAX_AGE_SECS;
        let at = |secs: u64| QuietWalReclaimKey::from_inputs(9, 10, 4096, 3, 0, 0, 0, start + secs);
        assert_eq!(at(0), at(QUIET_WAL_RECLAIM_MAX_AGE_SECS - 1));
        assert_ne!(at(0), at(QUIET_WAL_RECLAIM_MAX_AGE_SECS));
    }
}

/// The four terms of the storage-eviction loop's continue-decision, named so the count and the
/// vacuity of individual terms can be asserted instead of described in prose.
///
/// Two of the four are constants at the shipped default threshold of 0; see the comment at the
/// loop in `run_storage_manager_round_inner` for which and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EvictionLoopTerms {
    /// `apply_storage_eviction` did not take its `pressure_before <= threshold` early return.
    pub(crate) pressure_gate_open: bool,
    /// The per-call victim count budget has room left.
    pub(crate) more_budget: bool,
    /// Usage exceeds the configured limit, or this is an explicit forced batch (threshold zero).
    pub(crate) still_over: bool,
    /// The last batch took victims and was not in cooldown.
    pub(crate) made_progress: bool,
}

/// The terms that can be `false` at `threshold`, i.e. the ones that can actually stop the loop.
///
/// At a threshold of 0 the two threshold-derived terms are constants -- no `u64` is below 0, so
/// `apply_storage_eviction` cannot take its early return and `pressure_after >= 0` cannot be
/// false -- and only the budget and the progress checks remain. This is the DENOMINATOR for the
/// guard: 2 of 4 live at the shipped default, 4 of 4 once an operator raises the threshold.
pub(crate) fn eviction_loop_live_stopping_conditions(threshold: u64) -> &'static [&'static str] {
    if threshold == 0 {
        &["more_budget", "made_progress"]
    } else {
        &[
            "pressure_gate_open",
            "more_budget",
            "still_over",
            "made_progress",
        ]
    }
}

/// How many ROUNDS the evict stage may take, derived from the victim-count budget.
///
/// A round that is doing its job takes at least one victim, so after `count_limit` rounds the
/// victim budget is spent anyway and this bound has changed nothing. It exists for the round
/// that takes NONE -- see `eviction_loop_terms_for_batch`.
pub(crate) fn eviction_loop_round_limit(count_limit: usize) -> usize {
    count_limit
}

/// The four terms, computed FROM a batch report -- the wiring, as opposed to the predicate.
///
/// WHY THIS IS A FUNCTION. The predicate had unit tests, and they construct `EvictionLoopTerms`
/// directly: nothing exercised the step that turns a report into terms, and that step is where
/// the loop could fail to terminate. Forcing `made_progress` true there spins the stage for ever
/// -- 27 of 29 tests in the area still passed, two ran unbounded, and libtest has no per-test
/// timeout, so the run was CANCELLED rather than failed. A cancelled run names no test.
///
/// WHY `more_budget` COUNTS ROUNDS AS WELL AS VICTIMS. `taken` is `victims.len()`, and victims
/// only grow when a batch returns some. A batch that returns NONE leaves `taken` exactly where
/// it was, so at the shipped budget of 100 the term `count_limit > taken` never goes false
/// however many times the loop goes round: the budget bounds WORK TAKEN, not WORK ATTEMPTED.
/// With `still_over` and `pressure_gate_open` both vacuous at the shipped threshold of 0, that
/// left `made_progress` as the entire termination argument -- one term, untested at this
/// boundary. The round bound makes the budget bound iterations too, so the worst a broken
/// progress term can now do is run `count_limit` rounds and stop, which a test can assert on.
pub(crate) fn eviction_loop_terms_for_batch(
    report: &crate::engine::reports::StorageEvictionReport,
    threshold: u64,
    count_limit: usize,
    taken: usize,
    rounds: usize,
) -> EvictionLoopTerms {
    EvictionLoopTerms {
        pressure_gate_open: report.pressure_gate_open,
        more_budget: count_limit > taken && rounds < eviction_loop_round_limit(count_limit),
        still_over: threshold == 0 || report.pressure_after > threshold,
        // A batch that freed nothing, or found nobody, means the next will not do better.
        made_progress: !report.cooldown && !report.selected_victims.is_empty(),
    }
}

/// What the evict STAGE did, accumulated across its batches.
///
/// The stage reports what the STAGE did, not what its last batch did. Returning the final report
/// alone would describe the batch that stopped the loop -- usually the unproductive one that
/// tripped `cooldown` -- and hide every victim the earlier batches took, which is the shape of
/// misreporting #1453-#1455 fixed elsewhere.
pub(crate) struct EvictionStageTally {
    pub(crate) victims: Vec<crate::engine::reports::StorageEvictionVictim>,
    pub(crate) dump_manifest_ids: Vec<String>,
    pub(crate) cache_entries_removed: usize,
    pub(crate) cache_disk_bytes_removed: u64,
    pub(crate) dropped_object_count: usize,
    pub(crate) bucket_index_buckets_released: usize,
    pub(crate) bucket_index_blocks_released: usize,
    pub(crate) bucket_index_release_refused: usize,
    pub(crate) first_bucket_index_bytes_before: u64,
    pub(crate) first_batch_limit: usize,
    pub(crate) pressure_gate_open: bool,
    pub(crate) dump_before_evict: bool,
    /// Batches taken AFTER the first. Bounded by `eviction_loop_round_limit`, and the number a
    /// termination guard asserts on.
    pub(crate) rounds: usize,
    pub(crate) first_pressure_before: u64,
    pub(crate) last: crate::engine::reports::StorageEvictionReport,
}

impl EvictionStageTally {
    /// The stage's report: counts accumulated, `pressure_before` from the FIRST batch and
    /// `pressure_after` from the last.
    pub(crate) fn into_stage_report(self) -> crate::engine::reports::StorageEvictionReport {
        let first_pressure_before = self.first_pressure_before;
        let skipped_reason = if self.pressure_gate_open {
            String::new()
        } else {
            self.last.skipped_reason.clone()
        };
        crate::engine::reports::StorageEvictionReport {
            pressure_before: first_pressure_before,
            selected_victims: self.victims,
            dump_manifest_ids: self.dump_manifest_ids,
            cache_entries_removed: self.cache_entries_removed,
            cache_disk_bytes_removed: self.cache_disk_bytes_removed,
            dropped_object_count: self.dropped_object_count,
            bucket_index_buckets_released: self.bucket_index_buckets_released,
            bucket_index_blocks_released: self.bucket_index_blocks_released,
            bucket_index_release_refused: self.bucket_index_release_refused,
            bucket_index_bytes_before: self.first_bucket_index_bytes_before,
            batch_limit: self.first_batch_limit,
            pressure_gate_open: self.pressure_gate_open,
            dump_before_evict: self.dump_before_evict,
            skipped_reason,
            // `cooldown` means the STAGE freed nothing, not that its last batch did.
            cooldown: self.last.pressure_after >= first_pressure_before,
            ..self.last
        }
    }
}

/// Take batches until the pressure is back under the threshold, the budget is spent, or a batch
/// stops helping. `next_batch` is handed the remaining selection budget and returns the next batch.
///
/// The loop lives here, apart from the engine, so that a test can drive it with a batch sequence
/// of its own and assert how many ROUNDS it ran. Driving the real control rather than a copy of
/// it is the point: a test that re-implemented the decision would agree with itself whatever the
/// stage did.
pub(crate) fn run_eviction_stage<F>(
    threshold: u64,
    count_limit: usize,
    first: crate::engine::reports::StorageEvictionReport,
    mut next_batch: F,
) -> EvictionStageTally
where
    F: FnMut(usize) -> crate::engine::reports::StorageEvictionReport,
{
    let mut tally = EvictionStageTally {
        victims: first.selected_victims.clone(),
        dump_manifest_ids: first.dump_manifest_ids.clone(),
        cache_entries_removed: first.cache_entries_removed,
        cache_disk_bytes_removed: first.cache_disk_bytes_removed,
        dropped_object_count: first.dropped_object_count,
        bucket_index_buckets_released: first.bucket_index_buckets_released,
        bucket_index_blocks_released: first.bucket_index_blocks_released,
        bucket_index_release_refused: first.bucket_index_release_refused,
        first_bucket_index_bytes_before: first.bucket_index_bytes_before,
        first_batch_limit: first.batch_limit,
        pressure_gate_open: first.pressure_gate_open,
        dump_before_evict: first.dump_before_evict,
        rounds: 0,
        first_pressure_before: first.pressure_before,
        last: first,
    };
    let round_limit = eviction_loop_round_limit(count_limit);
    // Selection work is charged by the requested batch, not by the number of eligible victims.
    // Keep a minimum charge for callers using the low-level zero (unbounded) batch convention.
    let mut charged = tally.last.batch_limit.max(1).min(count_limit);
    loop {
        let terms =
            eviction_loop_terms_for_batch(&tally.last, threshold, count_limit, charged, tally.rounds);
        debug_assert!(
            terms.stops_only_on_live_conditions(threshold),
            "eviction loop stopped on a condition that cannot fire at threshold {threshold}: {terms:?}",
        );
        if !terms.should_continue() {
            break;
        }
        debug_assert!(
            tally.rounds < round_limit,
            "the eviction loop ran {} rounds against a limit of {round_limit}",
            tally.rounds,
        );
        let remaining = count_limit.saturating_sub(charged);
        let report = next_batch(remaining);
        charged = charged.saturating_add(report.batch_limit.max(1).min(remaining));
        tally.rounds = tally.rounds.saturating_add(1);
        tally.victims.extend(report.selected_victims.iter().cloned());
        tally
            .dump_manifest_ids
            .extend(report.dump_manifest_ids.iter().cloned());
        tally.cache_entries_removed = tally
            .cache_entries_removed
            .saturating_add(report.cache_entries_removed);
        tally.cache_disk_bytes_removed = tally
            .cache_disk_bytes_removed
            .saturating_add(report.cache_disk_bytes_removed);
        tally.dropped_object_count = tally
            .dropped_object_count
            .saturating_add(report.dropped_object_count);
        tally.bucket_index_buckets_released = tally.bucket_index_buckets_released
            .saturating_add(report.bucket_index_buckets_released);
        tally.bucket_index_blocks_released = tally.bucket_index_blocks_released
            .saturating_add(report.bucket_index_blocks_released);
        tally.bucket_index_release_refused = tally.bucket_index_release_refused
            .saturating_add(report.bucket_index_release_refused);
        tally.pressure_gate_open |= report.pressure_gate_open;
        tally.dump_before_evict |= report.dump_before_evict;
        tally.last = report;
    }
    tally
}

impl EvictionLoopTerms {
    /// Take another batch only if every term holds.
    pub(crate) fn should_continue(self) -> bool {
        self.pressure_gate_open && self.more_budget && self.still_over && self.made_progress
    }

    /// True unless the loop is stopping ONLY because of a term that cannot fire at `threshold`.
    ///
    /// A stated precondition that is printed but not enforced is not one, so this is checked at
    /// the loop rather than left to the comment. At a threshold of 0 a stop must be attributable
    /// to `more_budget` or `made_progress`; a stop attributable only to `still_over` or
    /// `pressure_gate_open` means someone has made a vacuous term load-bearing.
    pub(crate) fn stops_only_on_live_conditions(self, threshold: u64) -> bool {
        if self.should_continue() {
            return true;
        }
        let live = eviction_loop_live_stopping_conditions(threshold);
        live.iter().any(|name| match *name {
            "pressure_gate_open" => !self.pressure_gate_open,
            "more_budget" => !self.more_budget,
            "still_over" => !self.still_over,
            "made_progress" => !self.made_progress,
            other => unreachable!("unknown eviction stopping condition {other}"),
        })
    }
}

/// Stage-by-stage clock for `run_storage_manager_once`. One `Instant` read per stage; the span's
/// microseconds and its walk charge are both taken at the same boundary, so the rows tile the
/// round. A row is closed AFTER the stage's branch chain, which is where its work ends: a stage's
/// own gate probe (the reclaim_wal reclaimability check, the reclaim_index trigger) is charged to
/// that stage and not to the one before it.
struct LoopSpans {
    started: std::time::Instant,
    clock: std::time::Instant,
    rows: Vec<StorageManagerLoopStageSpan>,
    io: super::stage_io_probe::StageIoProbe,
}

impl LoopSpans {
    fn start() -> Self {
        crate::engine::reset_stage_walk_charges();
        let io = super::stage_io_probe::StageIoProbe::from_environment();
        let now = std::time::Instant::now();
        Self {
            started: now,
            clock: now,
            rows: Vec::new(),
            io,
        }
    }

    /// A stage ran when its name is the last thing pushed on `executed`; stages push exactly once
    /// and in order, so no earlier entry can be mistaken for it.
    fn close(&mut self, stage: &str, executed: &[String]) {
        self.io.mark();
        let elapsed = self.clock.elapsed();
        let start_offset = self.clock.saturating_duration_since(self.started);
        self.clock = std::time::Instant::now();
        self.rows.push(StorageManagerLoopStageSpan {
            stage: stage.to_string(),
            executed: executed.last().map(String::as_str) == Some(stage),
            start_offset_us: start_offset.as_micros() as u64,
            duration_us: elapsed.as_micros() as u64,
            walk: crate::engine::take_stage_walk_charges(),
        });
    }
}
impl DataNodeRuntime {
    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        self.inner.engine.storage_lifecycle_plan(request)
    }

    pub fn storage_manager_pressure_snapshot(
        &self,
        shard_id: ShardId,
        options: &StorageManagerOptions,
    ) -> (StorageManagerPressureSnapshot, StorageLifecyclePlan) {
        self.storage_manager_pressure_snapshot_with_report_details(shard_id, options, true)
    }

    pub(super) fn storage_manager_pressure_snapshot_with_report_details(
        &self,
        shard_id: ShardId,
        options: &StorageManagerOptions,
        detailed_report: bool,
    ) -> (StorageManagerPressureSnapshot, StorageLifecyclePlan) {
        let (plan, undumped_wal_bytes) = self
            .inner
            .engine
            .storage_lifecycle_pressure_plan(StorageLifecycleRequest {
                item_amplification_full_dump_ppm: 0,
                shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: options.max_dump_buckets_per_round,
                min_undumped_wal_records: options.min_undumped_wal_records,
                min_undumped_wal_bytes: options.min_undumped_wal_bytes,
                purge_delayed_destroy: options.enable_block_gc,
                purge_delayed_destroy_slab_ids: None,
                prune_bucket_dump_manifests: options.enable_index_gc,
                roll_forward_bucket_dump_installs: options.enable_index_gc,
                follower_replay_cursors: options.follower_replay_cursors.clone(),
                block_gc_shared_store_cursors: options.block_gc_shared_store_cursors.clone(),
                block_gc_raft_snapshot_refs: options.raft_snapshot_refs.clone(),
                block_gc_checkpoint_floor_slab_id: options.block_gc_checkpoint_floor_slab_id,
                block_gc_raft_install_floor_slab_id: options.block_gc_raft_install_floor_slab_id,
                // The grace the apply holds, so the plan's purgeable figure is not an upper bound
                // the purge never reaches.
                block_gc_delayed_destroy_grace_ms:
                    crate::engine::reports::DEFAULT_BLOCK_GC_DELAYED_DESTROY_GRACE_MS,
                invalidate_cache: false,
                warm_cache: false,
            }, detailed_report);
        let cache = self.inner.engine.cache().stats();
        // THE NUMBER THAT WAS MISSING. Every memory term below was a cache term, so a shard whose
        // bucket index is large and whose cache is small -- the exact shape eviction exists for --
        // read as carrying no memory and was never asked to give any back.
        //
        // The moving figure (nodes + per-block entries), not the node-only floor, because the
        // per-block entries are what a release frees: the pressure this reports falls when the
        // evict stage acts on it, which is what makes it a gate and not a klaxon.
        let bucket_index_resident_bytes = self.inner.engine.bucket_index_resident_bytes(shard_id);
        // Mirrors `apply_storage_eviction`'s own gate term for term, so the reported signal and
        // the real decision cannot drift apart. ONE TERM, because that gate now reads one term:
        // this shard's resident index. The four cache terms it used to carry came from
        // `MultiLayerCache::stats()`, which is not filtered by shard, so what this field published
        // as "the shard's eviction pressure" was mostly other shards' cache -- 99.93% of it on a
        // ten-key shard beside an eight-thousand-key one, measured in
        // `the_evict_gates_cache_terms_are_process_wide_not_per_shard`.
        let eviction_memory_pressure_bytes = bucket_index_resident_bytes;
        // THE NODE MEMORY SCORE KEEPS THE WIDE COMPOSITION, and is now a different number from the
        // evict gate's input rather than an alias of it. It is published for an operator watching
        // the node, and `reclaim_memory` -- the stage that invalidates cached blocks -- is the thing
        // it describes; narrowing it to the index would make it report a debt that stage cannot pay.
        // The two were one field only because the gate used to read the wide number.
        let memory_cache_pressure_score = cache
            .memory_bytes
            .saturating_add(cache.disk_bytes)
            .saturating_add(cache.async_writeback_queue_bytes)
            .saturating_add(cache.async_writeback_queue_depth)
            .saturating_add(bucket_index_resident_bytes);
        let log_pressure = self.inner.engine.storage_log_pressure_report(shard_id);
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        (
            StorageManagerPressureSnapshot {
                shard_id,
                dirty_bucket_count: plan.dirty_buckets.len(),
                selected_dirty_bucket_count: plan.selected_dump_buckets.len(),
                undumped_wal_records: plan.undumped_wal_records,
                // BYTES, as the field says. This was the record count copied into a field named for
                // bytes, which is what the byte gate would have read had anything read it.
                wal_bytes: undumped_wal_bytes,
                index_log_bytes: log_pressure.index_log_bytes,
                stale_block_slab_count: plan.stale_block_slab_ids.len(),
                reclaim_candidate_count: plan.reclaim_candidates.len(),
                reclaimable_physical_bytes: plan.reclaimable_physical_bytes,
                block_slab_stale_density_basis_points: 0,
                cache_memory_bytes: cache.memory_bytes,
                cache_disk_bytes: cache.disk_bytes,
                bucket_index_resident_bytes,
                eviction_memory_pressure_bytes,
                memory_cache_pressure_score,
                live_block_summaries_measured: plan.bucket_summaries.is_some(),
                // 0 when the round did not walk. The flag above is what says which 0 this is.
                expired_bucket_object_scan_debt: plan
                    .bucket_summaries
                    .as_ref()
                    .map(|summaries| summaries.len())
                    .unwrap_or(0),
                delayed_destroy_slab_count: plan.delayed_destroy_block_slab_ids.len(),
                delayed_destroy_bytes: plan.reclaimable_physical_bytes,
                follower_cursor_retention_blockers: 0,
                raft_snapshot_retention_blockers: 0,
                compaction_debt_model_count: usize::from(!plan.stale_block_slab_ids.is_empty()),
                compaction_debt_score: plan.reclaimable_physical_bytes,
                total_pressure_score: plan
                    .dirty_buckets
                    .len()
                    .saturating_add(plan.selected_dump_buckets.len())
                    as u64
                    + plan.undumped_wal_records
                    + log_pressure.index_log_bytes
                    + plan.reclaimable_physical_bytes
                    + cache.memory_bytes
                    + cache.disk_bytes
                    + bucket_index_resident_bytes,
                background_queue_depth: queue.background_queued_total,
                foreground_queue_depth: queue
                    .queued_total
                    .saturating_sub(queue.background_queued_total),
            },
            plan,
        )
    }

    pub fn run_storage_manager_once(
        &self,
        shard_id: ShardId,
        options: StorageManagerOptions,
    ) -> StorageManagerLoopReport {
        self.run_storage_manager_once_with_report_details(shard_id, options, true)
    }

    pub(crate) fn run_storage_manager_once_with_report_details(
        &self,
        shard_id: ShardId,
        options: StorageManagerOptions,
        detailed_report: bool,
    ) -> StorageManagerLoopReport {
        self.run_storage_manager_round(shard_id, options, detailed_report, true).0
    }

    fn run_storage_manager_round(
        &self,
        shard_id: ShardId,
        options: StorageManagerOptions,
        detailed_report: bool,
        scan_page_slabs: bool,
    ) -> (StorageManagerLoopReport, StageProgress) {
        // A SHARD STILL REPLAYING ITS LOG GETS NO ROUND. The all-shards scheduler visits every
        // key of `shards`, and a shard is inserted there BEFORE its replay and flagged
        // `recovering` until the replay ends. The plan, the dump, compaction and the index-log
        // truncation below all read a half-reconstructed index in that window: a dump would
        // persist it, compaction would relocate against it, and a block the replay has not yet
        // re-filed looks dead. The cycle path and the block GC already decline such a shard;
        // this loop did not, and the stages it runs are the ones that change the most.
        // Declined, not failed: the next round after recovery runs normally.
        if self.inner.engine.shard_is_recovering(shard_id) {
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_stage_skips += 1;
            return (StorageManagerLoopReport {
                shard_id,
                pressure: StorageManagerPressureSnapshot::default(),
                executed_stages: Vec::new(),
                skipped_stages: vec!["shard_recovering".to_string()],
                pressure_decisions: Vec::new(),
                lifecycle_plan: StorageLifecyclePlan::default(),
                lifecycle_report: None,
                expired_records_removed: 0,
                metrics_reap: None,
                compaction_report: None,
                gc_report: None,
                eviction: None,
                status: Status::ok(),
                stage_spans: Vec::new(),
                duration_us: 0,
                wal_reclaim_round: StorageManagerWalReclaimRound::default(),
                page_log_reclaim: None,
            }, StageProgress::Idle);
        }
        // The stage walk counters are read-and-cleared at each stage boundary, so the first span
        // must not inherit whatever ran before this round.
        let mut spans = LoopSpans::start();
        let (pressure, lifecycle_plan) = self.storage_manager_pressure_snapshot_with_report_details(
            shard_id, &options, detailed_report,
        );
        {
            // The backlog the loop just read, kept where a scrape can see it.
            let mut stats = self.inner.stats.lock().expect("runtime stats lock poisoned");
            stats.storage_manager_loop_wal_bytes = pressure.wal_bytes;
            stats.storage_manager_loop_undumped_wal_records = pressure.undumped_wal_records;
            stats.storage_manager_loop_dirty_buckets = pressure.dirty_bucket_count as u64;
            stats.storage_manager_loop_index_log_bytes = pressure.index_log_bytes;
        }
        spans.close("pressure_snapshot", &["pressure_snapshot".to_string()]);
        let mut executed_stages = Vec::new();
        let mut skipped_stages = Vec::new();
        let mut pressure_decisions = Vec::new();
        let mut lifecycle_report = None;
        let mut block_purge_report = None;
        let mut compaction_report = None;
        let mut gc_report = None;
        let mut expired_records_removed = 0;
        let mut status = Status::ok();

        if options.enable_prepare {
            executed_stages.push("prepare".to_string());
            // Pre-allocate the next data slab so a client append never has to roll inline.
            //
            // Rolling costs several fsyncs plus a slab-directory scan (see
            // BlockStore::prepare_next_slab). Left to the write path it lands on one
            // unlucky write as a latency outlier unrelated to that write's size. Doing it
            // here -- the stage that already exists and is already named "prepare" -- is what
            // this design does with PrepareNewZone in the same position of
            // its background cycle.
            //
            // A no-op while the active slab has room, so this costs a lock and a comparison
            // on the overwhelming majority of cycles. A failure is recorded and does not stop
            // the cycle: the inline roll in `append` remains the fallback, so the only
            // consequence is that the next append pays for the roll, exactly as it does today.
            match self.inner.engine.block_store().prepare_next_slab_for_shard(
                shard_id,
                crate::storage_config::effective_block_slab_target_bytes(),
            ) {
                Ok(Some(roll)) => {
                    tracing::debug!(
                        previous_block_slab_id = roll.previous_block_slab_id,
                        new_block_slab_id = roll.new_block_slab_id,
                        "storage manager pre-allocated the next data slab"
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "storage manager could not pre-allocate the next data slab;                          the next append will roll inline"
                    );
                }
            }
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_prepare_runs += 1;
        } else {
            skipped_stages.push("prepare_disabled".to_string());
        }
        spans.close("prepare", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "prepare",
            options.enable_prepare,
            true,
            options.enable_prepare,
            vec![
                storage_manager_pressure_signal(
                    "dirty_slot_count",
                    pressure.dirty_bucket_count as u64,
                    options.dirty_bucket_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "stale_page_segment_count",
                    pressure.stale_block_slab_count as u64,
                    options.stale_block_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "background_queue_depth",
                    pressure.background_queue_depth as u64,
                    1,
                ),
            ],
            vec!["continuous_prepare_plan_refresh".to_string()],
            (!options.enable_prepare).then(|| "prepare_disabled".to_string()),
        ));

        // THE LOG BYTES ARE A TRIGGER OF THEIR OWN, and on a default deployment the only one.
        // Until the undumped log reaches the byte gate nothing here needs to look at a dirty bucket.
        // This stage used to run its dump, two plans and a reclaim on every round that found one
        // dirty bucket, because `dirty_bucket_pressure` shipped at 1.
        let wal_bytes_pressure = options.min_undumped_wal_bytes > 0
            && pressure.wal_bytes >= options.min_undumped_wal_bytes;
        let dump_pressure = pressure.dirty_bucket_count >= options.dirty_bucket_pressure.max(1)
            || pressure.undumped_wal_records >= options.min_undumped_wal_records.max(1)
            || wal_bytes_pressure;
        let cache_pressure = pressure.cache_memory_bytes
            >= options.cache_memory_bytes_pressure.max(1)
            || pressure.cache_disk_bytes >= options.cache_disk_bytes_pressure.max(1);
        self.inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .storage_manager_retention_owners_configured =
            options.retention_owner_input_count() as u64;
        let stale_block_pressure =
            super::compaction_stage::stale_block_pressure(&pressure, &options);

        // This stage publishes dumps. Physical log retirement belongs to page collection.
        let mut wal_reclaim_round = StorageManagerWalReclaimRound::default();
        if options.enable_wal_reclaim && dump_pressure {
            let dump_started = std::time::Instant::now();
            // Publish relocated addresses before the dump captures their generations.
            let resident = self.inner.engine.wal_resident_block_count(shard_id);
            let moved_blocks = if resident > 0 {
                self.inner.engine.materialize_oldest_resident_blocks(
                    shard_id,
                    resident.saturating_sub(if options.max_dump_buckets_per_round == 0 {
                        usize::MAX
                    } else {
                        options.max_dump_buckets_per_round
                    }),
                )
            } else {
                0
            };
            wal_reclaim_round.moved_blocks = moved_blocks;
            let response = self.apply_storage_lifecycle_with_report_details(StorageLifecycleRequest {
                item_amplification_full_dump_ppm: 0,
                shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: options.max_dump_buckets_per_round,
                min_undumped_wal_records: options.min_undumped_wal_records,
                min_undumped_wal_bytes: options.min_undumped_wal_bytes,
                purge_delayed_destroy: false,
                purge_delayed_destroy_slab_ids: None,
                prune_bucket_dump_manifests: false,
                roll_forward_bucket_dump_installs: false,
                follower_replay_cursors: options.follower_replay_cursors.clone(),
                block_gc_shared_store_cursors: options.block_gc_shared_store_cursors.clone(),
                block_gc_raft_snapshot_refs: options.raft_snapshot_refs.clone(),
                block_gc_checkpoint_floor_slab_id: options.block_gc_checkpoint_floor_slab_id,
                block_gc_raft_install_floor_slab_id: options.block_gc_raft_install_floor_slab_id,
                block_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            }, detailed_report);
            if let Some(manifest) = response.report.dump_manifest.as_ref() {
                clear_dirty_shard_buckets(
                    &self.inner.dirty,
                    &self.inner.engine,
                    shard_id,
                    &manifest.bucket_ids,
                );
                let mut stats = self
                    .inner
                    .stats
                    .lock()
                    .expect("runtime stats lock poisoned");
                stats.dump_runs += 1;
                stats.storage_manager_reclaim_wal_runs += 1;
                let dump_us = dump_started.elapsed().as_micros() as u64;
                stats.storage_manager_dump_buckets = stats
                    .storage_manager_dump_buckets
                    .saturating_add(manifest.bucket_ids.len() as u64);
                stats.storage_manager_dump_logical_bytes = stats
                    .storage_manager_dump_logical_bytes
                    .saturating_add(manifest.logical_bytes);
                stats.storage_manager_dump_us_total =
                    stats.storage_manager_dump_us_total.saturating_add(dump_us);
                stats.storage_manager_dump_us_max = stats.storage_manager_dump_us_max.max(dump_us);
            }
            wal_reclaim_round.dumped = response.report.dump_manifest.is_some();
            lifecycle_report = Some(response.report);
            executed_stages.push("reclaim_wal".to_string());
        } else if !options.enable_wal_reclaim {
            skipped_stages.push("reclaim_wal_disabled".to_string());
        } else {
            skipped_stages.push("reclaim_wal_no_pressure".to_string());
        }
        spans.close("reclaim_wal", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_wal",
            options.enable_wal_reclaim,
            dump_pressure,
            options.enable_wal_reclaim && dump_pressure,
            vec![
                storage_manager_pressure_signal(
                    "dirty_slot_count",
                    pressure.dirty_bucket_count as u64,
                    options.dirty_bucket_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "undumped_wal_records",
                    pressure.undumped_wal_records,
                    options.min_undumped_wal_records.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (
                    pressure.dirty_bucket_count >= options.dirty_bucket_pressure.max(1),
                    "dirty_slot_pressure",
                ),
                (
                    pressure.undumped_wal_records >= options.min_undumped_wal_records.max(1),
                    "undumped_wal_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_wal_reclaim,
                dump_pressure,
                "reclaim_wal",
            ),
        ));

        if options.enable_expire {
            expired_records_removed = self.sweep_expired_records_bounded(
                shard_id,
                options.max_expire_hot_buckets_per_round,
                options.max_expire_cold_buckets_per_round,
            );
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_expire_runs += 1;
            executed_stages.push("expire".to_string());
        } else {
            skipped_stages.push("expire_disabled".to_string());
        }
        spans.close("expire", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "expire",
            options.enable_expire,
            expired_records_removed > 0,
            options.enable_expire,
            vec![storage_manager_pressure_signal(
                "expired_records_removed",
                expired_records_removed as u64,
                1,
            )],
            if expired_records_removed > 0 {
                vec!["expired_records_present".to_string()]
            } else {
                vec!["continuous_expiry_sweep".to_string()]
            },
            (!options.enable_expire).then(|| "expire_disabled".to_string()),
        ));

        // Evict, when the operator has asked for it -- beside expire, and before the stages
        // that rewrite blocks.
        //
        // Expire drops what has died of old age and evict drops what is merely cold; they are two
        // halves of relieving memory, and the design runs them together at this point in the
        // round. This stage ran LAST, after the metrics reap, which put it after every stage that
        // rewrites blocks: it decided what to drop from a state compaction had just churned, and
        // the memory it frees arrived too late to spare those stages any work. It has no data
        // dependency on any of them -- it reads options and engine state only -- so the position
        // was incidental.
        //
        // reclaim_memory above relieves memory by invalidating cached blocks, which frees the cache
        // and leaves the index untouched. This is the stage that can actually free a bucket -- and
        // dump it first, so freeing does not strand the log.
        // Keep taking batches until the pressure is back under the threshold, the count budget
        // is spent, or a batch stops helping.
        //
        // The stage used to take ONE batch and return, however far the pressure still was from the
        // threshold -- 16 victims freeing about 4,800 bytes against a 1,775,000-byte overage, so
        // roughly 370 rounds. The design being followed loops instead and stops on a per-call
        // count budget.
        //
        // FOUR terms decide whether the loop takes another batch, and at the shipped default
        // threshold of 0 only TWO of them can ever be false. Counting them as "three stopping
        // conditions, all three needed" -- which is what this comment used to say -- reads the
        // guard as stronger than it is, so the terms are named in `EvictionLoopTerms` and the
        // count is asserted rather than described.
        //
        // Both limits measure resident index bytes. Zero disables a limit; either configured
        // limit can open the stage, and the enable flag controls both eviction routes.
        let configured_limit = crate::engine::reports::configured_eviction_memory_limit(
            options.eviction_memory_pressure_threshold, options.index_resident_limit_bytes,
        );
        let index_bound_is_the_gate = options.index_resident_limit_bytes > 0
            && (options.eviction_memory_pressure_threshold == 0
                || options.index_resident_limit_bytes < options.eviction_memory_pressure_threshold);
        let eviction = options.enable_evict.then(|| {
            let Some(stage_threshold) = configured_limit else {
                return crate::engine::reports::StorageEvictionReport {
                    shard_id,
                    mode: "evict_cache".to_string(),
                    pressure_before: pressure.eviction_memory_pressure_bytes,
                    pressure_after: pressure.eviction_memory_pressure_bytes,
                    bucket_index_bytes_before: pressure.eviction_memory_pressure_bytes,
                    bucket_index_bytes_after: pressure.eviction_memory_pressure_bytes,
                    batch_limit: options.eviction_batch_limit,
                    dump_before_evict: options.eviction_dump_before_evict,
                    skipped_reason: "no_memory_limit".to_string(),
                    ..Default::default()
                };
            };
            let engine = &self.inner.engine;
            if options.eviction_count_limit == 0 || options.eviction_batch_limit == 0 {
                let resident = engine.bucket_index_resident_bytes(shard_id);
                let gate_open = resident > stage_threshold;
                return crate::engine::reports::StorageEvictionReport {
                    shard_id,
                    mode: if index_bound_is_the_gate { "index_resident_bound" } else { "evict_cache" }.to_string(),
                    pressure_before: resident,
                    pressure_after: resident,
                    bucket_index_bytes_before: resident,
                    bucket_index_bytes_after: resident,
                    memory_pressure_threshold: stage_threshold,
                    pressure_gate_open: gate_open,
                    batch_limit: options.eviction_batch_limit,
                    dump_before_evict: options.eviction_dump_before_evict,
                    cooldown: gate_open,
                    skipped_reason: if gate_open && options.eviction_count_limit == 0 {
                        "eviction_count_limit"
                    } else if gate_open {
                        "eviction_batch_limit"
                    } else if index_bound_is_the_gate {
                        "index_resident_below_bound"
                    } else {
                        "memory_pressure_below_threshold"
                    }.to_string(),
                    ..Default::default()
                };
            }
            // ONE batch function for both arms, so the repeat path cannot call a different gate
            // from the first one. It did not, and could not, when there was only one gate; with
            // two it is the obvious way to get a stage that opens on one and loops on the other.
            let batch = |budget: usize| {
                if index_bound_is_the_gate {
                    engine.apply_index_resident_bound(
                        shard_id,
                        options.index_resident_limit_bytes,
                        budget,
                    )
                } else {
                    engine.apply_storage_eviction(
                        shard_id,
                        options.eviction_memory_pressure_threshold,
                        budget,
                        options.eviction_dump_before_evict,
                        // NEVER, AND THERE IS NO LONGER A SETTING THAT COULD MAKE IT TRUE. This
                        // took `options.eviction_delete_drop`, and `delete_drop` calls
                        // `delete_record` on every live object in its victim buckets and then
                        // writes a `CommonDelete` tombstone per key -- so it does not drop a
                        // resident copy, it removes the model-map entry that is the only thing
                        // pointing at the payload block, and makes the removal survive replay.
                        // Measured on 600 acknowledged keys: 18 deleted, 18 missing cold, still
                        // missing after a reload. Relieving memory pressure must never destroy the
                        // only reachable copy of live data. The literal is the route's absence.
                        false,
                    )
                }
            };
            // The bar `still_over` is read against has to be the one the gate used, or the stage
            // keeps taking batches against a number nobody set. At the bound this term stops being
            // vacuous for the first time: a non-zero bar can actually be reached.
            let bounded_batch = |remaining_budget: usize| {
                let limit = options.eviction_batch_limit.min(remaining_budget);
                batch(limit)
            };
            let first = bounded_batch(options.eviction_count_limit);
            run_eviction_stage(
                stage_threshold,
                options.eviction_count_limit,
                first,
                bounded_batch,
            )
            .into_stage_report()
        });
        if eviction.is_some() {
            executed_stages.push("evict".to_string());
            self.inner
                .stats
                .lock()
                .expect("data node stats lock poisoned")
                .storage_manager_evict_runs += 1;
        } else {
            skipped_stages.push("evict_disabled".to_string());
        }
        spans.close("evict", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "evict",
            eviction.is_some(),
            eviction.as_ref().is_some_and(|report| report.pressure_gate_open),
            eviction.is_some(),
            configured_limit.map(|limit| vec![
                storage_manager_pressure_signal(
                    "eviction_memory_pressure_bytes",
                    pressure.eviction_memory_pressure_bytes, limit,
                ),
                storage_manager_pressure_signal(
                    "bucket_index_resident_bytes",
                    pressure.bucket_index_resident_bytes, limit,
                ),
                storage_manager_pressure_signal(
                    "index_resident_limit_bytes",
                    pressure.bucket_index_resident_bytes, limit,
                ),
            ]).unwrap_or_default(),
            vec![if index_bound_is_the_gate {
                "index_resident_bound_set".to_string()
            } else {
                "operator_enabled_eviction".to_string()
            }],
            if eviction.is_none() {
                Some("evict_disabled".to_string())
            } else {
                eviction.as_ref().filter(|report| !report.skipped_reason.is_empty())
                    .map(|report| report.skipped_reason.clone())
            },
        ));

        // Expire and evict run BEFORE the cache invalidation below, not after it.
        //
        // `reclaim_memory` is a blanket whole-shard cache invalidation. Eviction's gate
        // compares its threshold against exactly that cache, so running it afterwards handed
        // it a zero: measured over ten rounds, the gate never opened once and every round
        // skipped with `memory_pressure_below_threshold`, whatever the threshold was set to.
        // Eviction had therefore never evicted anything on a default server, and
        // `executed_stages` said "evict" either way, which is why it went unnoticed.
        //
        // The name collision is what makes this confusing. The `ReclaimMemory` this stage is
        // named after CONTAINS expire and evict and invalidates nothing -- there, those two
        // ARE the memory relief. Our blanket invalidation is a separate mechanism with no
        // counterpart, so there is no ordering to match, only an input not to destroy.
        //
        // `enable_evict` now ships TRUE, so this ordering is load-bearing on a default server
        // rather than only for an operator who turned the stage on. The shipped round opens the
        // bound rather than the wide gate, and the bound reads `bucket_index_resident_bytes`,
        // which a cache invalidation does not touch -- but the ordering stays as it is, because
        // lowering the bound below the threshold lands on the wide arm, whose input this
        // invalidation would zero.
        if options.enable_memory_reclaim && cache_pressure {
            // NO DUMP AND NO PLAN. This stage used to run a whole lifecycle request with
            // `invalidate_cache: true`: a plan walking the shard, then up to
            // `max_dump_buckets_per_round` bucket dumps, then the invalidation. The invalidation
            // drops COPIES -- every cached block is also in the resident index or a slab, and every
            // write is in the log whether or not its bucket was dumped -- so the dump bought
            // nothing here and the plan only named what it would dump. Relieving memory by
            // dumping is what the reclaim_wal stage's bound is for.
            //
            // The retain floors of the stages after this one read the last `lifecycle_report`'s
            // dump manifest. This stage no longer replaces it, so they read an older manifest:
            // a lower floor, which retains more and never less.
            self.inner.engine.invalidate_shard_cache(shard_id);
            let mut stats = self
                .inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned");
            stats.storage_manager_reclaim_memory_runs += 1;
            executed_stages.push("reclaim_memory".to_string());
        } else if !options.enable_memory_reclaim {
            skipped_stages.push("reclaim_memory_disabled".to_string());
        } else {
            skipped_stages.push("reclaim_memory_no_pressure".to_string());
        }
        spans.close("reclaim_memory", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_memory",
            options.enable_memory_reclaim,
            cache_pressure,
            options.enable_memory_reclaim && cache_pressure,
            vec![
                storage_manager_pressure_signal(
                    "cache_memory_bytes",
                    pressure.cache_memory_bytes,
                    options.cache_memory_bytes_pressure.max(1),
                ),
                storage_manager_pressure_signal(
                    "cache_disk_bytes",
                    pressure.cache_disk_bytes,
                    options.cache_disk_bytes_pressure.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (
                    pressure.cache_memory_bytes >= options.cache_memory_bytes_pressure.max(1),
                    "cache_memory_pressure",
                ),
                (
                    pressure.cache_disk_bytes >= options.cache_disk_bytes_pressure.max(1),
                    "cache_disk_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_memory_reclaim,
                cache_pressure,
                "reclaim_memory",
            ),
        ));


        if options.enable_block_gc && scan_page_slabs && lifecycle_plan.reasons.iter()
            .any(|reason| reason == "slot_dump_refresh_after_relocation")
        {
            // Publish relocated addresses before deciding which old segments can be retired.
            let response = self.apply_storage_lifecycle_with_report_details(
                StorageLifecycleRequest {
                    shard_id,
                    selected_dump_buckets: lifecycle_plan.selected_dump_buckets.clone(),
                    prune_bucket_dump_manifests: true,
                    roll_forward_bucket_dump_installs: true,
                    follower_replay_cursors: options.follower_replay_cursors.clone(),
                    block_gc_shared_store_cursors: options.block_gc_shared_store_cursors.clone(),
                    block_gc_raft_snapshot_refs: options.raft_snapshot_refs.clone(),
                    block_gc_checkpoint_floor_slab_id: options.block_gc_checkpoint_floor_slab_id,
                    block_gc_raft_install_floor_slab_id: options.block_gc_raft_install_floor_slab_id,
                    ..StorageLifecycleRequest::default()
                }, detailed_report,
            );
            if !response.status.ok {
                status = response.status.clone();
            }
            lifecycle_report = Some(response.report);
            self.inner.quiet_wal_reclaim.lock().expect("quiet wal reclaim lock poisoned")
                .remove(&shard_id);
        }
        let page_log_reclaim = if options.enable_block_gc {
            self.reclaim_page_log(shard_id, &options, &pressure)
        } else {
            None
        };
        if options.enable_block_gc && scan_page_slabs && stale_block_pressure {
            // The floor is a MIN over the stale slabs, so a stale slab that can never be deleted
            // freezes it -- and one always can be. `run_gc_inner` treats every slab named by a
            // bucket dump manifest as live, so the moment a dump exists, the slab it names is
            // stale (its blocks have moved) and retained (the manifest needs it). `min` then sits
            // on that slab for ever, the floor never advances, and every slab compaction rolls
            // afterwards is above it and permanently out of reach.
            //
            // Measured before this: the floor froze at 2 while stale slabs climbed to nine and the
            // collector removed nothing, on a store nobody was writing to. That is the same shape
            // as #1516, where a reclaim frontier taken as a min over every bucket was pinned for
            // ever by one clean bucket -- a min over a set with an immovable member.
            //
            // So take the min over the slabs that are actually candidates: stale AND not pinned by
            // a manifest. Excluding them does not make them deletable -- `gc_slabs_before_..`
            // checks `is_live` itself and still refuses them -- it only stops them dictating how
            // far back the round is allowed to look.
            let manifest_pinned_block_slab_ids = self
                .inner
                .engine
                .list_bucket_dump_manifests(shard_id)
                .into_iter()
                .flat_map(|manifest| manifest.block_slab_ids.into_iter())
                .collect::<std::collections::BTreeSet<_>>();
            // THE HIGHEST COLLECTABLE STALE SLAB, NOT THE LOWEST, and this is what lets a
            // per-round budget mean anything here.
            //
            // The predicate downstream is `block_slab_id < retain_from_block_slab_id`
            // (`block_store_gc.rs`), so a floor of `min + 1` admits EXACTLY ONE slab -- the
            // lowest -- however many are collectable. Every per-round bound on this stage was
            // therefore unreachable on this path at every setting: the candidate set could not
            // exceed one, so `max_destroy_slabs` (500 shipped), the physical-byte ceiling and the
            // garbage floor had nothing to bound, and `skipped_by_budget_count` could not be
            // anything but 0.
            //
            // MEASURED in `the_block_gc_budget_reaches_the_periodic_loop`: against 6 collectable
            // slabs, a round bounded at one slab removed 1 and a round with NO bound also removed
            // 1. A bound that cannot be told from its own absence is not a bound.
            //
            // The storage-manager cycle has always taken `max` over its candidates
            // (`storage_manager_cycle.rs`), so this is the two pipelines agreeing rather than a
            // new policy.
            //
            // WHY `min` WAS HERE, AND WHY `max` DOES NOT BRING THAT BACK. `min` was chosen so one
            // immovable slab could not freeze the floor: a stale slab named by a durable
            // bucket-dump manifest is retained for ever, and a `max` over a set containing it
            // would sit on it. The `filter` above already removes the manifest-pinned slabs from
            // the set being reduced, so they cannot dictate the ceiling whichever reduction is
            // used -- the filter is what answers that objection, not the choice of `min`.
            //
            // AND RAISING THE FLOOR ONLY WIDENS WHAT IS OFFERED, never what is destroyed.
            // `gc_slabs_before_with_live_refs_selected` re-checks `!is_current`, `!is_live` and
            // the published live-bytes tally immediately before each irreversible step, and
            // `run_gc_inner` puts every slab a retained manifest names into the live set. So a
            // live, current or manifest-pinned slab below the new floor is refused exactly as it
            // was before.
            let retain_block_slabs_from_id = lifecycle_plan
                .stale_block_slab_ids
                .iter()
                .filter(|slab_id| !manifest_pinned_block_slab_ids.contains(slab_id))
                .max()
                .map(|slab_id| slab_id.saturating_add(1))
                // Every stale slab is manifest-pinned: nothing to collect this round, and the
                // floor must not fall back to 0 -- that would read as "retain nothing".
                .or_else(|| {
                    lifecycle_plan
                        .stale_block_slab_ids
                        .iter()
                        .max()
                        .map(|slab_id| slab_id.saturating_add(1))
                });
            let response = run_gc_inner(
                &self.inner,
                GcRequest {
                    shard_id,
                    // Log retirement was handled above with the caller's retention inputs.
                    retain_wal_from_sequence: None,
                    retain_index_log_from_sequence: None,
                    retain_block_slabs_from_id,
                    // Quarantine rather than unlink, as the cycle has always done. This stage
                    // deleted a slab outright, so if the retain floor above was ever computed too
                    // generously there was nothing left to recover from. This stage also purges
                    // what it sets aside, after the grace period.
                    block_gc_delayed_destroy: true,
                    // This stage ran every round block pressure held, and each run emptied the
                    // shard's cache outright -- memory, pmem and disk tiers alike -- however few
                    // slabs it went on to reclaim. Invalidate what was reclaimed instead.
                    block_gc_invalidate_removed_slabs_only: true,
                    block_gc_shared_store_cursors: options.block_gc_shared_store_cursors.clone(),
                    block_gc_raft_snapshot_refs: options.raft_snapshot_refs.clone(),
                    block_gc_checkpoint_floor_slab_id: options.block_gc_checkpoint_floor_slab_id,
                    block_gc_raft_install_floor_slab_id: options.block_gc_raft_install_floor_slab_id,
                    // THE THREE BOUNDS THIS STAGE RAN WITHOUT. They existed on the cycle request
                    // and reached `BlockStoreGcPolicy` from there; this request had no field for
                    // them, so the stage called the policy-free reclaim entries and destroyed
                    // every eligible slab on every round, at every setting an operator could
                    // choose. `the_block_gc_budget_reaches_the_periodic_loop` measures a round
                    // bounded at one slab taking one of several collectable ones.
                    block_gc_max_destroy_slabs_per_round: options
                        .block_gc_max_destroy_slabs_per_round,
                    block_gc_max_destroy_physical_bytes_per_round: options
                        .block_gc_max_destroy_physical_bytes_per_round,
                    block_gc_min_slab_garbage_basis_points: options
                        .block_gc_min_slab_garbage_basis_points,
                    block_gc_max_scan_slabs_per_round: options
                        .block_gc_scan_window
                        .map_or(0, |window| window.max_slabs),
                    block_gc_scan_after_slab_id: options
                        .block_gc_scan_window
                        .and_then(|window| window.after_block_slab_id),
                    include_lifecycle_plan: false,
                },
            );
            if !response.status.ok {
                status = response.status.clone();
            }
            gc_report = Some(response);
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_reclaim_block_runs += 1;
            executed_stages.push("reclaim_page".to_string());
        } else if !options.enable_block_gc {
            skipped_stages.push("reclaim_page_disabled".to_string());
        } else {
            skipped_stages.push("reclaim_page_no_pressure".to_string());
        }
        // THE PURGE BELONGS TO THIS STAGE, and to no other: it destroys slabs this stage set aside.
        //
        // A QUARANTINED SLAB IS WORK ONLY WHEN IT CAN BE ACTED ON. The plan lists every slab in
        // quarantine, but the purge destroys none before the grace has passed and restores one only
        // when the live set names it; a store that has already checked the quarantine against the
        // live set, with nothing old enough, is asked nothing the purge would answer differently, so
        // the call (and the walk behind it) is skipped. Under steady overwrite load a quarantine is
        // never empty, so reading "any slab" as work would walk the shard every round. A retention
        // owner can name a quarantined slab with no new arrival, so with one configured the purge
        // always runs, and when a slab CAN mature the restore check still reads the walked live set.
        if options.enable_block_gc {
            let quarantine_waits = options.block_gc_shared_store_cursors.is_empty()
                && !self.inner.engine.has_registered_replay_readers(shard_id)
                && options.raft_snapshot_refs.is_empty()
                && options.block_gc_checkpoint_floor_slab_id.is_none()
                && options.block_gc_raft_install_floor_slab_id.is_none()
                && self.inner.engine.quarantine_waits_for_age(
                    crate::engine::reports::DEFAULT_BLOCK_GC_DELAYED_DESTROY_GRACE_MS,
                );
            if !quarantine_waits {
                let purge_started = std::time::Instant::now();
                let purge = self
                    .inner
                    .engine
                    .purge_storage_delayed_destroy(&StorageLifecycleRequest {
                        shard_id,
                        purge_delayed_destroy: true,
                        follower_replay_cursors: options.follower_replay_cursors.clone(),
                        block_gc_shared_store_cursors: options.block_gc_shared_store_cursors.clone(),
                        block_gc_raft_snapshot_refs: options.raft_snapshot_refs.clone(),
                        block_gc_checkpoint_floor_slab_id: options.block_gc_checkpoint_floor_slab_id,
                        block_gc_raft_install_floor_slab_id: options
                            .block_gc_raft_install_floor_slab_id,
                        block_gc_delayed_destroy_grace_ms:
                            crate::engine::reports::DEFAULT_BLOCK_GC_DELAYED_DESTROY_GRACE_MS,
                        ..StorageLifecycleRequest::default()
                    });
                super::task_execution::record_gc_phase(&self.inner, "purge", purge_started);
                block_purge_report = Some(purge);
                if !executed_stages.iter().any(|stage| stage == "reclaim_page") {
                    executed_stages.push("reclaim_page".to_string());
                    skipped_stages.retain(|stage| stage != "reclaim_page_no_pressure");
                    self.inner.stats.lock().expect("runtime stats lock poisoned")
                        .storage_manager_reclaim_block_runs += 1;
                }
            }
        }
        if page_log_reclaim.is_some()
            && !executed_stages.iter().any(|stage| stage == "reclaim_page")
        {
            executed_stages.push("reclaim_page".to_string());
            skipped_stages.retain(|stage| stage != "reclaim_page_no_pressure");
            self.inner.stats.lock().expect("runtime stats lock poisoned")
                .storage_manager_reclaim_block_runs += 1;
        }
        let block_gc_work = (scan_page_slabs && stale_block_pressure) || block_purge_report.is_some()
            || page_log_reclaim.is_some();
        spans.close("reclaim_page", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_page",
            options.enable_block_gc,
            block_gc_work,
            options.enable_block_gc && block_gc_work,
            vec![
                storage_manager_pressure_signal(
                    "published_log_check", u64::from(page_log_reclaim.is_some()), 1,
                ),
                storage_manager_pressure_signal(
                    "quarantine_check_due",
                    u64::from(block_purge_report.is_some()),
                    1,
                ),
                storage_manager_pressure_signal(
                    "stale_page_segment_count",
                    pressure.stale_block_slab_count as u64,
                    options.stale_block_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "reclaim_candidate_count",
                    pressure.reclaim_candidate_count as u64,
                    options.stale_block_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "reclaimable_physical_bytes",
                    pressure.reclaimable_physical_bytes,
                    options.reclaimable_physical_bytes_pressure.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (page_log_reclaim.is_some(), "published_log_check"),
                (block_purge_report.is_some(), "quarantine_check_due"),
                (
                    pressure.stale_block_slab_count >= options.stale_block_slab_pressure.max(1),
                    "stale_page_segment_pressure",
                ),
                (
                    pressure.reclaim_candidate_count >= options.stale_block_slab_pressure.max(1),
                    "reclaim_candidate_pressure",
                ),
                (
                    pressure.reclaimable_physical_bytes
                        >= options.reclaimable_physical_bytes_pressure.max(1),
                    "reclaimable_physical_bytes_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_block_gc,
                block_gc_work,
                "reclaim_page",
            ),
        ));

        let index_gc_pressure = lifecycle_plan.reasons.iter().any(|reason| {
            reason == "slot_dump_manifest_prune" || reason == "slot_dump_install_roll_forward_check"
        });
        // THE CHEAP QUESTIONS FIRST, answered from what this round already holds.
        //
        // This stage dumps the dirty set, prunes manifests, rolls installs forward, purges delayed
        // destroys, and then asks whether the index log is large enough to collect -- and it asked
        // that LAST, after two plans that each walk every live block. On a shard where none of those
        // has any work, every round paid O(keys) to establish that nothing needed doing.
        //
        // The shard has no work for this stage exactly when: nothing is dirty and nothing is
        // undumped (so the dump takes nothing), no manifest-prune or install-roll-forward reason
        // stands (so the prune does nothing), no slab is waiting to be destroyed, and the index log
        // is shorter than the byte floor the collector declines below (so the collector declines).
        // The floor is read from the same default the collector itself uses, so the two cannot drift.
        //
        // Counted as a run of the stage, like the WAL stage's answer from memory: the stage list is
        // how an operator reads whether maintenance is alive.
        //
        // The plan this reads was taken at the top of the round. An earlier stage that retires a slab
        // in this same round is therefore seen one round later, which is a delay of one period and
        // not a missed purge: the next round's plan lists it.
        let index_log_floor = StorageManagerCycleRequest::default().index_gc_index_log_bytes_threshold;
        //
        // NO DUMP TO MAKE has two forms: nothing dirty and nothing undumped, or a dirty shard that
        // the byte gate holds back. The second is the busy shard under the gate, which used to pay
        // three whole-shard plan builds a round to learn that the gate would decline. The decision
        // is the one the plan itself makes (`dump_gate_delays`), read off the round's own counters.
        // A log below the floor also rules out the forced dump, which needs at least the floor.
        let index_dump_held_back = crate::engine::dump_gate_delays(
            options.min_undumped_wal_records,
            options.min_undumped_wal_bytes,
            pressure.undumped_wal_records,
            pressure.wal_bytes,
        );
        let index_gc_idle_apart_from_quarantine = options.enable_index_gc
            && index_log_floor != 0
            && ((pressure.dirty_bucket_count == 0 && pressure.undumped_wal_records == 0)
                || index_dump_held_back)
            && !index_gc_pressure
            && self
                .inner
                .engine
                .index_log_store()
                .log_len_bytes(shard_id)
                < index_log_floor;
        // The quarantine is the block stage's: this stage no longer purges, so a waiting slab is not its work.
        let index_gc_has_no_work = index_gc_idle_apart_from_quarantine;
        if index_gc_has_no_work {
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_index_gc_runs += 1;
            executed_stages.push("reclaim_index".to_string());
        } else if options.enable_index_gc {
            // THE DUMP IS GATED LIKE THE WAL STAGE'S, NOT FORCED EVERY ROUND.
            //
            // A manifest embeds the whole shard index, so every dump this stage forced cost
            // O(shard) bytes however few buckets were dirty: measured per-SET bytes written
            // grew with run length (one dump per round, each larger than the last) while the
            // per-op families stayed flat. So the shipped byte gate decides here, as it does
            // in the WAL stage, and a dump is forced only when the index log itself has
            // outgrown the floor the collector declines below, which is the one case where
            // waiting would let that log grow without bound (its truncation rides on the
            // dump frontier). Replay bound: the index log can reach max(1 MiB, last image)
            // before a forced dump.
            //
            // The floor scales with the last image. A dump costs about one image of bytes, so
            // forcing one per fixed 1 MiB of index log made bytes written per SET grow with the
            // key count again. Waiting until the log is as long as the image keeps the dump to
            // at most one byte written per index-log byte, which is flat per operation.
            let last_image_bytes = self
                .inner
                .engine
                .latest_bucket_dump_image_bytes(shard_id);
            let index_log_over_floor = index_log_forces_dump(
                self.inner.engine.index_log_store().log_len_bytes(shard_id),
                index_log_floor,
                last_image_bytes,
            );
            let (dump_min_records, dump_min_bytes) = if index_log_over_floor {
                (0, 0)
            } else {
                (options.min_undumped_wal_records, options.min_undumped_wal_bytes)
            };
            let index_gc_request = StorageLifecycleRequest {
                item_amplification_full_dump_ppm: 0,
                shard_id,
                selected_dump_buckets: Vec::new(),
                // UNBOUNDED, and load-bearing -- do not cap this.
                //
                // Capping it was tried and it stopped index-log reclaim dead: 16,000 records
                // before a round, 16,000 after. The reason is COVERAGE, not the commit check:
                // `wal_plan.safe_to_reclaim` needs a durable manifest for every live generation,
                // and this whole-dirty-set dump is what produces one. Bound it and the frontier
                // never advances -- the #1439 finding seen from the other side.
                //
                // The consequence is that `max_dump_buckets_per_round` bounds the reclaim_wal
                // and reclaim_memory stages, not a ROUND: this stage still dumps the whole dirty
                // set by default.
                //
                // #1516 fixed the frozen frontier that made a bounded dump fail to advance the
                // floor, so the cap is now reachable and its effect is measurable rather than
                // assumed. Defaults to 0, which is exactly the behaviour above.
                max_dump_buckets_per_round: options.index_gc_max_dump_buckets_per_round,
                min_undumped_wal_records: dump_min_records,
                min_undumped_wal_bytes: dump_min_bytes,
                purge_delayed_destroy: false,
                purge_delayed_destroy_slab_ids: None,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                // THE SITE THAT MATTERS MOST OF THE FOUR, because this is the one that PRUNES.
                // Two of the others only ever cost a clamp on the reclaim floor: the pressure
                // snapshot builds a PLAN and applies nothing, and the `reclaim_wal` stage applies
                // with `prune_bucket_dump_manifests: false`. Here it costs a dump: with no cursor
                // to anchor, `bucket_dump_manifest_prune_plan_at` retains only what local coverage
                // needs and the older manifests go. A reader that was relying on one to restore
                // from -- `POST /server/storage/dumps/install` is how a node is rebuilt from a
                // dump -- loses the thing it would have restored from, and unlike a reclaimed log
                // record nothing regenerates it.
                follower_replay_cursors: options.follower_replay_cursors.clone(),
                block_gc_shared_store_cursors: options.block_gc_shared_store_cursors.clone(),
                block_gc_raft_snapshot_refs: options.raft_snapshot_refs.clone(),
                block_gc_checkpoint_floor_slab_id: options.block_gc_checkpoint_floor_slab_id,
                block_gc_raft_install_floor_slab_id: options.block_gc_raft_install_floor_slab_id,
                // A DAY, NOT THE STORE'S ONE-HOUR FLOOR. This was 0, which the purge raises to
                // `DELAYED_DESTROY_MIN_AGE_MS` (one hour), so a quarantined slab was destroyed an
                // hour after it was set aside. The cycle request already defaulted to this
                // constant; only the periodic loop, the path a server actually runs, did not.
                block_gc_delayed_destroy_grace_ms:
                    crate::engine::reports::DEFAULT_BLOCK_GC_DELAYED_DESTROY_GRACE_MS,
                invalidate_cache: false,
                warm_cache: false,
            };
            let response = self.apply_storage_lifecycle_with_report_details(
                index_gc_request.clone(), detailed_report,
            );
            // Pruning manifests and rolling forward installs make the index log RECLAIMABLE.
            // Neither frees a byte of it: `storage_index_gc_report` is what truncates, and until
            // now nothing on this loop reached it -- so a server started as shipped grew its index
            // log for ever, exactly as it grew its write-ahead log before #1470.
            //
            // The same request this stage just applied, so the plan the safety check reads is the
            // one this stage's dump produced; and this stage's OWN report, so a dump that
            // committed dirty buckets is visible to
            // `index_gc_commit_dirty_buckets_before_truncation`.
            let index_gc = self
                .inner
                .engine
                .apply_periodic_index_gc(
                    index_gc_request,
                    Some(&response.report),
                    options.index_gc_max_entries_per_round,
                );
            if index_gc.applied {
                tracing::debug!(
                    shard_id,
                    index_log_records_removed = index_gc.records_removed,
                    "storage manager reclaimed index-log records"
                );
            }
            // A round's report names the manifest the round wrote, whichever stage wrote it: this
            // stage no longer dumps below its gate, so its own report would otherwise erase the
            // manifest the WAL stage just produced.
            let mut stage_report = response.report;
            if stage_report.dump_manifest.is_none() {
                if let Some(earlier) = lifecycle_report
                    .as_mut()
                    .and_then(|earlier| earlier.dump_manifest.take())
                {
                    stage_report.dump_manifest = Some(earlier);
                }
            }
            lifecycle_report = Some(stage_report);
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_index_gc_runs += 1;
            executed_stages.push("reclaim_index".to_string());
        } else {
            skipped_stages.push("reclaim_index_disabled".to_string());
        }
        spans.close("reclaim_index", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_index",
            options.enable_index_gc,
            index_gc_pressure,
            options.enable_index_gc,
            vec![
                storage_manager_pressure_signal(
                    "manifest_prune_reasons",
                    lifecycle_plan
                        .reasons
                        .iter()
                        .filter(|reason| reason.as_str() == "slot_dump_manifest_prune")
                        .count() as u64,
                    1,
                ),
                storage_manager_pressure_signal(
                    "install_roll_forward_reasons",
                    lifecycle_plan
                        .reasons
                        .iter()
                        .filter(|reason| reason.as_str() == "slot_dump_install_roll_forward_check")
                        .count() as u64,
                    1,
                ),
            ],
            if index_gc_pressure {
                lifecycle_plan
                    .reasons
                    .iter()
                    .filter(|reason| {
                        reason.as_str() == "slot_dump_manifest_prune"
                            || reason.as_str() == "slot_dump_install_roll_forward_check"
                    })
                    .cloned()
                    .collect()
            } else {
                vec!["continuous_index_gc_safety_check".to_string()]
            },
            (!options.enable_index_gc).then(|| "reclaim_index_disabled".to_string()),
        ));

        // Reclaim the index log BEFORE rewriting blocks, not after, so the stages run in the
        // order the design intends: reclaim memory, reclaim blocks, reclaim the index, compact.
        //
        // This is an ALIGNMENT, not a throughput fix, and the probe beside it is what settles
        // that. `what_the_stage_order_buys` runs the same eight-round overwrite fixture under
        // both arrangements, with compaction firing in all eight rounds either way, and the two
        // are identical to the record: 1,873 left in the log, 1,765 removed per round.
        //
        // The reason they are identical is worth keeping, because the opposite is intuitive:
        // compaction rewrites live records into fresh blocks and appends an index record for
        // each, so running it first looks like it should hand the reclaim a log it just grew.
        // It does -- but those records are above the floor the reclaim computed, and a record
        // above the floor is protected whichever order the two stages run in. Moving the reclaim
        // earlier cannot collect them any sooner; it only stops them from being written until
        // after it has finished. Neither order is unsafe and neither collects more.
        // WHAT WOULD THIS ROUND RELOCATE? Asked before issuing a compaction, and not the same
        // question as `stale_block_pressure`.
        //
        // `stale_block_pressure` counts reclaim candidates, and a candidate is any slab carrying
        // dead space. That lumps together the slab compaction must empty and the slab compaction
        // has ALREADY emptied -- and the second is its own residue: a round relocates a slab's
        // live blocks onto a fresh one, and the vacated slab stays a candidate until the collector
        // destroys it, which is the pressure that runs the next round. Measured on an idle
        // 8,000-record shard: `compact_pages` executed in all twelve rounds with nobody writing,
        // the whole live set rewritten every four rounds, and the index log growing 63 bytes a
        // round for ever -- about 181 KB a day per idle shard at this cadence -- because each of
        // those rounds persists a fresh index record. With compaction off over the identical
        // fixture the index log grew by zero, which is what attributes it.
        //
        // The question below is answered PER OBJECT: does any object still hold a block on a slab
        // someone wants emptied? A slab holding nothing but dead space answers no -- no object has
        // a block there, nothing can be relocated off it, and destroying it is `reclaim_page`'s
        // job, not this stage's. That is the distinction three shard-wide predicates could not
        // make, which is why they were each refused by the suite (#1550, #1553): they asked about
        // the shard's staleness, and the shard is stale either way.
        //
        // Costs no walk: `live_page_refs` per candidate is already in the plan this round built.
        let compaction_relocatable_block_refs =
            crate::engine::compaction_relocatable_block_refs(&lifecycle_plan.reclaim_candidates);
        let compaction_has_work = compaction_relocatable_block_refs > 0;
        // AND THE ROUND MOVES THAT, AND NOT THE REST OF THE SHARD.
        //
        // The count above and the set below are the same answer read two ways off the same plan:
        // the count is what makes this round worth starting, the set is what the round is then
        // allowed to touch. They were not always the same answer. The gate has asked the
        // per-object question since the self-retrigger fix, but the round it issued relocated
        // every live block in the shard -- so one overwritten record anywhere cost a rewrite of
        // the WHOLE live set, over as many rounds of shard write lock as the ref budget needed,
        // to recover that one block.
        //
        // Relocating a block off a slab that carries no dead space recovers nothing.
        // `compact_block_addresses` copies its bytes verbatim, so it lands byte for byte what
        // it was, on a slab that is now the one carrying the dead space -- same live bytes, one
        // more emptied slab for the collector, one more share of an index record.
        //
        // Taking both off ONE plan snapshot also removes a smaller divergence: the gate and the
        // selection used to be computed from different views of the shard, so a round could be
        // started for a slab it then had no particular interest in draining.
        let compaction_drain_block_slab_ids =
            crate::engine::compaction_drain_block_slab_ids(&lifecycle_plan.reclaim_candidates);
        // AND WHO IS ASKING? A node that does not lead this shard rewrites its full live set
        // onto a fresh slab, copying every byte verbatim, for a layout its clients cannot see.
        //
        // The rewrite is node-local and it is NOT divergent -- compaction emits no replicated
        // record and does not advance `applied_wal_sequence` (#1620), so the relocated addresses
        // exist only in this node's durable index; replication ships commands, and a snapshot
        // install builds a FRESH engine and replaces this one wholesale, so a follower's slab
        // ids can neither collide with nor leak into the leader's. It is pure waste, and it
        // scales with the shard rather than with the traffic.
        //
        // THREE states, and the third is the one that could break this. The server constructs
        // this scheduler about eighty lines before it constructs raft, so for the first rounds
        // -- and for ever on a standalone node, which is the shipped default -- there is nothing
        // to ask. `is_known_not_leading` is therefore a POSITIVE test on the one state that is
        // evidence: `Unknown` compacts, exactly as every deployment did before this existed. The
        // negation (`!= Leading`) would have read "unknown" as "follower" and switched
        // compaction off on every single-node store in the world, silently.
        let leadership = self.shard_leadership(shard_id);
        let leadership_permits_compaction = !leadership.is_known_not_leading();
        if options.enable_block_compaction
            && leadership_permits_compaction
            && stale_block_pressure
            && compaction_has_work
        {
            if detailed_report {
                let response = run_compaction_inner_draining(
                    &self.inner,
                    CompactionRequest { shard_id },
                    Some(compaction_drain_block_slab_ids),
                );
                if !response.status.ok {
                    status = response.status.clone();
                }
                compaction_report = Some(response);
            } else {
                let compaction_status = task_execution::run_compaction_background_draining(
                    &self.inner, shard_id, compaction_drain_block_slab_ids,
                );
                if !compaction_status.ok {
                    status = compaction_status;
                }
            }
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_compact_runs += 1;
            executed_stages.push("compact_pages".to_string());
        } else if !options.enable_block_compaction {
            skipped_stages.push("compact_pages_disabled".to_string());
        } else if !leadership_permits_compaction {
            skipped_stages.push("compact_pages_not_leading".to_string());
        } else if !stale_block_pressure {
            skipped_stages.push("compact_pages_no_pressure".to_string());
        } else {
            skipped_stages.push("compact_pages_nothing_to_relocate".to_string());
        }
        spans.close("compact_pages", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "compact_pages",
            options.enable_block_compaction && leadership_permits_compaction,
            stale_block_pressure,
            options.enable_block_compaction
                && leadership_permits_compaction
                && stale_block_pressure
                && compaction_has_work,
            vec![
                storage_manager_pressure_signal(
                    "relocatable_block_refs",
                    compaction_relocatable_block_refs,
                    1,
                ),
                // 1 for the two states that compact, 0 for the one that does not. Carried so an
                // operator can tell "skipped because this node follows" from the three other
                // reasons a round skips, without correlating against raft's own report.
                storage_manager_pressure_signal(
                    "leadership_permits_compaction",
                    u64::from(leadership_permits_compaction),
                    1,
                ),
                storage_manager_pressure_signal(
                    "stale_page_segment_count",
                    pressure.stale_block_slab_count as u64,
                    options.stale_block_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "reclaim_candidate_count",
                    pressure.reclaim_candidate_count as u64,
                    options.stale_block_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "reclaimable_physical_bytes",
                    pressure.reclaimable_physical_bytes,
                    options.reclaimable_physical_bytes_pressure.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (
                    pressure.stale_block_slab_count >= options.stale_block_slab_pressure.max(1),
                    "stale_page_segment_pressure",
                ),
                (
                    pressure.reclaim_candidate_count >= options.stale_block_slab_pressure.max(1),
                    "reclaim_candidate_pressure",
                ),
                (
                    pressure.reclaimable_physical_bytes
                        >= options.reclaimable_physical_bytes_pressure.max(1),
                    "reclaimable_physical_bytes_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_block_compaction,
                stale_block_pressure,
                "compact_pages",
            )
            .or_else(|| {
                (!leadership_permits_compaction).then(|| {
                    format!("compact_pages_not_leading (role={})", leadership.as_str())
                })
            })
            .or_else(|| {
                (!compaction_has_work)
                    .then(|| "compact_pages_nothing_to_relocate".to_string())
            }),
        ));


        // Gather what the stage has always claimed to gather. Deliberately a snapshot and not
        // a reset: `durability_metrics` documents its counters as process-wide and monotonic,
        // and tests reset them to isolate a measurement window -- a background cycle clearing
        // them would pull the floor out from under anyone diffing two points in time.
        // The per-bucket gauges are a BOUNDED, resumable slice: the cursor says where the last round
        // stopped, so the cost of a round is the buckets it reads and not the size of the shard.
        let bucket_sweep = (options.enable_metrics_reap
            && options.reap_metrics_max_buckets_per_round > 0)
            .then(|| {
                let after = self
                    .inner
                    .reap_cursors
                    .lock()
                    .expect("reap cursor lock poisoned")
                    .get(&shard_id)
                    .copied()
                    .flatten();
                let sweep = self.inner.engine.bucket_gauge_sweep(
                    shard_id,
                    after,
                    options.reap_metrics_max_buckets_per_round,
                );
                self.inner
                    .reap_cursors
                    .lock()
                    .expect("reap cursor lock poisoned")
                    .insert(shard_id, sweep.next_after);
                sweep
            })
            .unwrap_or_default();
        let metrics_reap = options.enable_metrics_reap.then(|| {
            let durability_barriers: std::collections::BTreeMap<String, u64> =
                crate::durability_metrics::snapshot()
                    .into_iter()
                    .map(|(site, count)| (site.to_string(), count))
                    .collect();
            StorageMetricsReapReport {
                durability_barriers_total: durability_barriers.values().copied().sum(),
                durability_barriers,
                // From the cache the append path keeps. `stats` answers the last sequence by
                // scanning the active piece, and the loop reaps every round.
                wal: self
                    .inner
                    .engine
                    .write_ahead_log_store()
                    .stats_without_rescan(shard_id),
                undumped_wal_records: pressure.undumped_wal_records,
                wal_bytes: pressure.wal_bytes,
                index_log_bytes: pressure.index_log_bytes,
                cache_memory_bytes: pressure.cache_memory_bytes,
                bucket_index_resident_bytes: pressure.bucket_index_resident_bytes,
                cache_memory_pressure_threshold_bytes: options.cache_memory_bytes_pressure,
                bucket_gauges: bucket_sweep.gauges,
                bucket_sweep_next_after: bucket_sweep.next_after,
                buckets_total: bucket_sweep.buckets_total,
            }
        });
        if metrics_reap.is_some() {
            executed_stages.push("reap_metrics".to_string());
        } else {
            skipped_stages.push("reap_metrics_disabled".to_string());
        }
        spans.close("reap_metrics", &executed_stages);
        pressure_decisions.push(storage_manager_pressure_decision(
            "reap_metrics",
            options.enable_metrics_reap,
            true,
            options.enable_metrics_reap,
            vec![
                storage_manager_pressure_signal(
                    "foreground_queue_depth",
                    pressure.foreground_queue_depth as u64,
                    1,
                ),
                storage_manager_pressure_signal(
                    "background_queue_depth",
                    pressure.background_queue_depth as u64,
                    1,
                ),
            ],
            vec!["continuous_metrics_reap".to_string()],
            (!options.enable_metrics_reap).then(|| "reap_metrics_disabled".to_string()),
        ));


        if lifecycle_report.as_ref()
            .and_then(|report| report.manifest_prune_report.as_ref())
            .is_some_and(|report| !report.removed_manifest_ids.is_empty())
        {
            self.inner.quiet_wal_reclaim.lock().expect("quiet wal reclaim lock poisoned")
                .remove(&shard_id);
        }
        // Later index work may replace its lifecycle report. Attach physical purge work last.
        let cleanup_progress = if block_purge_report.as_ref().is_some_and(|purge| purge.budget_exhausted) {
            StageProgress::More
        } else if block_purge_report.as_ref().is_some_and(|purge| purge.processed_block_slabs > 0)
            || page_log_reclaim.as_ref().is_some_and(|reclaim| {
                reclaim.wal_records_removed > 0 || reclaim.wal_segments_dropped > 0
            })
        {
            StageProgress::Done
        } else {
            StageProgress::Idle
        };
        if let Some(purge) = block_purge_report {
            record_purge_work(&mut lifecycle_report, &lifecycle_plan, purge, detailed_report);
        }
        let completed_round = {
            // Recorded beside the round counter, so every caller gets it: the per-shard
            // scheduler, the all-shards scheduler, and the cycle endpoint.
            let mut stats = self.inner.stats.lock().expect("runtime stats lock poisoned");
            // A round here is a run of the storage manager, and has to be counted as one.
            //
            // The queued path counts both (`worker.rs`, storage_manager_runs then
            // storage_manager_loops). This one counted only the round, and
            // `storage_manager_runs` is the field the scrape exports as
            // temporalstore_data_node_runtime_jobs_total{kind="storage_manager"} -- while
            // `storage_manager_loops` is exported nowhere. A server started as shipped drives
            // maintenance from THIS path and never the queued one, so the only exported sign
            // that maintenance runs at all sat at zero while it ran every thirty seconds, and
            // an operator reading it could not tell a working loop from a stopped one.
            stats.storage_manager_runs += 1;
            stats.storage_manager_loops += 1;
            stats.storage_manager_last_shard_id = Some(shard_id);
            // What this round declined to do and whether it failed, kept on the runtime: the
            // all-shards scheduler throws the report away, and before this the only trace of a
            // failed or skipped stage on a shipped server was a log line.
            stats.storage_manager_stage_skips += skipped_stages.len() as u64;
            if !status.ok {
                stats.storage_manager_failed_rounds += 1;
            }
            let round_us = spans.started.elapsed().as_micros() as u64;
            stats.storage_manager_round_us_total =
                stats.storage_manager_round_us_total.saturating_add(round_us);
            stats.storage_manager_round_us_max = stats.storage_manager_round_us_max.max(round_us);
            for row in spans.rows.iter().filter(|row| row.executed) {
                let timing = stats
                    .storage_manager_stage_timing
                    .entry(row.stage.clone())
                    .or_default();
                timing.rounds_executed += 1;
                timing.total_us = timing.total_us.saturating_add(row.duration_us);
                timing.max_us = timing.max_us.max(row.duration_us);
            }
            stats.storage_manager_loops.saturating_sub(1)
        };
        spans.io.emit(completed_round, shard_id, &spans.rows);

        // Compact execution reports above carry the dump manifest needed by index-GC fences.
        // Never expose their uncollected diagnostic fields as a complete lifecycle report.
        if !detailed_report {
            lifecycle_report = None;
        }
        let duration_us = spans.started.elapsed().as_micros() as u64;
        (StorageManagerLoopReport {
            shard_id,
            pressure,
            executed_stages,
            skipped_stages,
            pressure_decisions,
            lifecycle_plan,
            lifecycle_report,
            expired_records_removed,
            metrics_reap,
            compaction_report,
            gc_report,
            eviction,
            status,
            stage_spans: spans.rows,
            duration_us,
            wal_reclaim_round,
            page_log_reclaim,
        }, cleanup_progress)
    }

    /// Run a maintenance cycle for EVERY loaded shard, forever, on an interval.
    ///
    /// The per-shard scheduler below needs to be told which shard it serves, so a server that
    /// does not know its shards up front could not start one -- which is why nothing started
    /// one. This asks the engine each tick instead, so a shard loaded later is picked up without
    /// restarting anything and a shard unloaded in between is simply not visited.
    ///
    /// A cycle that fails is not fatal to the loop. The failure is already recorded in the
    /// report the cycle returns and in the runtime's stats; stopping the loop over one bad round
    /// would turn a transient error into a store that never reclaims again, which is the state
    /// this exists to end.
    pub fn start_storage_manager_scheduler_for_all_shards(
        &self,
        interval: Duration,
        options: StorageManagerOptions,
    ) -> StorageLifecycleScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                for shard_id in runtime.inner.engine.loaded_shard_ids() {
                    if scheduler_stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let started = std::time::Instant::now();
                    runtime.run_storage_manager_once_with_report_details(shard_id, options.clone(), false);
                    runtime.note_round_overrun(started.elapsed(), sleep_interval);
                }
            }
        });
        StorageLifecycleScheduler {
            stop,
            handle: Some(handle),
        }
    }

    /// The loop as short ticks. Every `tick` it does the cheap work for each loaded shard, and at
    /// most one heavy stage per shard per tick, each heavy stage falling due once per `period`
    /// and the five spread across the period (see `round_budget`). Where
    /// `start_storage_manager_scheduler_for_all_shards` runs every stage to completion in one
    /// round every interval, a request here can wait behind at most ONE stage, not all of them.
    pub fn start_storage_manager_tick_scheduler_for_all_shards(
        &self,
        tick: Duration,
        period: Duration,
        options: StorageManagerOptions,
    ) -> StorageLifecycleScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let tick = tick.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            let mut states: std::collections::HashMap<ShardId, ShardRoundState> =
                std::collections::HashMap::new();
            while !scheduler_stop.load(Ordering::Relaxed) {
                let started = std::time::Instant::now();
                let shard_ids = runtime.inner.engine.loaded_shard_ids();
                states.retain(|shard_id, _| shard_ids.contains(shard_id));
                for shard_id in shard_ids {
                    if scheduler_stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let state = states
                        .entry(shard_id)
                        .or_insert_with(|| ShardRoundState::new(started, period));
                    runtime.run_storage_manager_tick(shard_id, &options, state, period);
                }
                // The next tick is one `tick` after this one STARTED, not after it ended, so a
                // tick that did a stage does not push the cheap work of every other shard back.
                if let Some(rest) = tick.checked_sub(started.elapsed()) {
                    thread::sleep(rest);
                }
            }
        });
        StorageLifecycleScheduler {
            stop,
            handle: Some(handle),
        }
    }

    /// One tick for one shard: the cheap work, then the heavy slot that is due, if any. Returns
    /// the slot's round report when one ran.
    ///
    /// A shard still replaying its log gets nothing. The cheap work builds no plan: it is the
    /// slab pre-allocation, which is a lock and a comparison, and one bounded expiry sweep of this
    /// shard.
    pub(crate) fn run_storage_manager_tick(
        &self,
        shard_id: ShardId,
        options: &StorageManagerOptions,
        state: &mut ShardRoundState,
        period: Duration,
    ) -> Option<(HeavySlot, StorageManagerLoopReport)> {
        if self.inner.engine.shard_is_recovering(shard_id) {
            return None;
        }
        if options.enable_prepare {
            if let Err(error) = self.inner.engine.block_store().prepare_next_slab_for_shard(
                shard_id,
                crate::storage_config::effective_block_slab_target_bytes(),
            ) {
                tracing::warn!(
                    %error,
                    "storage manager could not pre-allocate the next data slab; \
                     the next append will roll inline"
                );
            }
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_prepare_runs += 1;
        }
        if options.enable_expire {
            let removed = self
                .inner
                .engine
                .sweep_expired_records_for_maintenance(crate::engine::reports::ShardExpirySweepRequest {
                    shard_id,
                    max_hot_buckets_per_round: options.max_expire_hot_buckets_per_round,
                    max_cold_buckets_per_round: options.max_expire_cold_buckets_per_round,
                    load_cold_buckets: true,
                })
                .map(|report| report.expired_records_removed)
                .unwrap_or(0);
            let mut stats = self.inner.stats.lock().expect("runtime stats lock poisoned");
            stats.storage_manager_expire_runs += 1;
            stats.expired_records_removed += removed as u64;
        }
        let slot = state.due_slot(std::time::Instant::now())?;
        if slot == HeavySlot::Compaction {
            return Some((slot, self.run_compaction_step(shard_id, options, state, period)));
        }
        let started = std::time::Instant::now();
        let mut resume = false;
        let report = if slot == HeavySlot::PageGc {
            let stage = PageGcStage { runtime: self };
            let mut budget = StageBudget::new(PAGE_GC_STEP_TIME, u64::MAX);
            let context = StageContext { options, plan: None };
            let (step, report) =
                stage.run(shard_id, &mut state.cursors.page, &mut budget, &context);
            resume = step.progress == StageProgress::More;
            report
        } else {
            self.run_storage_manager_once_with_report_details(
                shard_id,
                options_for_slot(options, slot),
                false,
            )
        };
        self.note_round_overrun(started.elapsed(), period);
        if resume {
            state.resume_next_tick(slot, std::time::Instant::now());
        } else {
            state.finished(slot, std::time::Instant::now(), period);
        }
        Some((slot, report))
    }

    /// A round that outlasts the interval means the loop never idles: the next round starts the
    /// moment the sleep after it ends, and the period is `round + interval`, not `interval`.
    /// Counted so that state is a number an operator can read.
    pub(crate) fn note_round_overrun(&self, took: Duration, interval: Duration) {
        if took > interval {
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_overrun_rounds += 1;
        }
    }

    pub fn start_storage_manager_scheduler(
        &self,
        interval: Duration,
        shard_id: ShardId,
        options: StorageManagerOptions,
    ) -> StorageLifecycleScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                let started = std::time::Instant::now();
                runtime.run_storage_manager_once_with_report_details(shard_id, options.clone(), false);
                runtime.note_round_overrun(started.elapsed(), sleep_interval);
            }
        });
        StorageLifecycleScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn storage_production_readiness_report(
        &self,
        shard_id: ShardId,
    ) -> StorageProductionReadinessReport {
        self.inner
            .engine
            .storage_production_readiness_report(shard_id)
    }

    pub fn storage_production_readiness_report_with_policy(
        &self,
        shard_id: ShardId,
        policy: StorageProductionReadinessPolicy,
    ) -> StorageProductionReadinessReport {
        self.inner
            .engine
            .storage_production_readiness_report_with_policy(shard_id, policy)
    }

    pub fn apply_storage_lifecycle(
        &self,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleResponse {
        self.apply_storage_lifecycle_with_report_details(request, true)
    }

    fn apply_storage_lifecycle_with_report_details(
        &self,
        request: StorageLifecycleRequest,
        detailed_report: bool,
    ) -> StorageLifecycleResponse {
        let report = self.inner.engine.apply_storage_lifecycle_with_report_details(request, detailed_report);
        self.inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .storage_lifecycle_runs += 1;
        StorageLifecycleResponse {
            status: Status::ok(),
            report,
        }
    }

    pub fn start_dirty_dump_scheduler(
        &self,
        interval: Duration,
        controller: RequestController,
    ) -> DirtyDumpScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.schedule_dirty_shard_dumps(controller);
            }
        });
        DirtyDumpScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn start_storage_lifecycle_scheduler(
        &self,
        interval: Duration,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.apply_storage_lifecycle_with_report_details(request.clone(), false);
            }
        });
        StorageLifecycleScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn start_storage_manager_cycle_scheduler(
        &self,
        interval: Duration,
        request: StorageManagerCycleRequest,
        controller: RequestController,
    ) -> StorageManagerScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                let should_submit = !runtime
                    .inner
                    .queue
                    .lock()
                    .expect("runtime queue lock poisoned")
                    .has_pending_storage_manager(request.shard_id);
                if should_submit {
                    runtime.submit_storage_manager_cycle(request.clone(), controller);
                }
            }
        });
        StorageManagerScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn start_storage_manager_runtime(
        &self,
        options: StorageManagerRuntimeOptions,
    ) -> StorageManagerRuntime {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let report = Arc::new(Mutex::new(storage_manager_runtime_initial_report(&options)));
        let thread_stop = Arc::clone(&stop);
        let thread_paused = Arc::clone(&paused);
        let thread_report = Arc::clone(&report);
        let handle = thread::spawn(move || {
            let mut round = 0u64;
            let mut current_backoff_ms = options.initial_backoff_ms;
            // Cycles whose completion outlived the short wait below. Their reports are collected
            // on a later tick instead of being thrown away -- see the comment at the wait.
            //
            // A LIST, not a single slot. With one slot the report was lost every time: the
            // top-of-tick poll saw the cycle still running, the cycle finished later in that same
            // tick, `has_pending` then went false so a new cycle was submitted, and submitting
            // overwrote the slot before anything polled the finished job again. Every cycle was
            // abandoned exactly one tick before it completed, so `last_completed_cycle` stayed
            // None forever and every derived figure -- bytes reclaimed, pressure after, WAL and
            // index-log floors -- stayed at zero while the manager was doing real work.
            //
            // Bounded by `has_pending_storage_manager`, which allows only one cycle in flight per
            // shard, so this holds a couple of entries at most; the cap is a backstop, not a
            // budget.
            let mut uncollected_cycle_jobs: Vec<u64> = Vec::new();
            const MAX_UNCOLLECTED_CYCLE_JOBS: usize = 64;
            loop {
                let delay_ms =
                    storage_manager_runtime_delay_ms(&options, round, current_backoff_ms);
                {
                    let mut report = thread_report
                        .lock()
                        .expect("storage manager runtime report lock poisoned");
                    report.last_delay_ms = delay_ms;
                    report.current_backoff_ms = current_backoff_ms;
                }
                if sleep_until_storage_manager_runtime_round(&thread_stop, delay_ms) {
                    break;
                }
                if thread_stop.load(Ordering::Relaxed) {
                    break;
                }
                round = round.saturating_add(1);
                if thread_paused.load(Ordering::Relaxed) {
                    let mut report = thread_report
                        .lock()
                        .expect("storage manager runtime report lock poisoned");
                    report.rounds_skipped_paused = report.rounds_skipped_paused.saturating_add(1);
                    report.paused = true;
                    continue;
                }
                {
                    let mut report = thread_report
                        .lock()
                        .expect("storage manager runtime report lock poisoned");
                    report.paused = false;
                    report.rounds_attempted = report.rounds_attempted.saturating_add(1);
                }
                // Collect a cycle that finished after we stopped waiting for it. Without this
                // the runtime report only ever reflected cycles that completed inside the very
                // short budget below -- which are precisely the cycles that found nothing to
                // do. Any cycle that actually dumped buckets or reclaimed WAL outlived the
                // budget and had its report dropped, so the reported dirty-bucket count,
                // selected buckets and reclaim floors sat at zero while the manager was busy.
                if !uncollected_cycle_jobs.is_empty() {
                    let mut still_running = Vec::with_capacity(uncollected_cycle_jobs.len());
                    for job_id in std::mem::take(&mut uncollected_cycle_jobs) {
                        match runtime.job_status(job_id) {
                            Some(status) if status.finished_at_ms.is_some() => {
                                // Finished: collect the report if it carried one, and stop
                                // tracking either way so a job that finished without a usable
                                // output cannot pin a slot forever.
                                if let Some(DataNodeTaskOutput::StorageManager(response)) =
                                    status.output
                                {
                                    let mut report = thread_report
                                        .lock()
                                        .expect("storage manager runtime report lock poisoned");
                                    apply_storage_manager_cycle_to_runtime_report(
                                        &mut report,
                                        response.report,
                                    );
                                }
                            }
                            Some(_) => still_running.push(job_id),
                            // Gone from the jobs map (pruned): nothing left to collect.
                            None => {}
                        }
                    }
                    uncollected_cycle_jobs = still_running;
                }
                let should_submit = !runtime
                    .inner
                    .queue
                    .lock()
                    .expect("runtime queue lock poisoned")
                    .has_pending_storage_manager(options.request.shard_id);
                if !should_submit {
                    let mut report = thread_report
                        .lock()
                        .expect("storage manager runtime report lock poisoned");
                    report.rounds_skipped_pending = report.rounds_skipped_pending.saturating_add(1);
                    continue;
                }
                // Same question, the other periodic driver. This one does not call
                // `run_storage_manager_once`; it posts a cycle to the worker, and the engine
                // that runs it has no way to ask about leadership. Masking the request here is
                // the only place in this path that can see both. An on-demand cycle posted by
                // an operator is untouched -- that is an instruction, not a background round.
                let mut cycle_request = options.request.clone();
                if runtime
                    .shard_leadership(cycle_request.shard_id)
                    .is_known_not_leading()
                {
                    cycle_request.enable_block_compaction = false;
                }
                let submitted =
                    runtime.submit_storage_manager_cycle(cycle_request, options.controller);
                let mut report = thread_report
                    .lock()
                    .expect("storage manager runtime report lock poisoned");
                report.last_job_id = Some(submitted.job_id);
                report.last_status = Some(submitted.status.clone());
                if submitted.status.ok {
                    report.rounds_submitted = report.rounds_submitted.saturating_add(1);
                    current_backoff_ms = options.initial_backoff_ms;
                } else {
                    report.submit_failures = report.submit_failures.saturating_add(1);
                    current_backoff_ms = storage_manager_runtime_next_backoff_ms(
                        current_backoff_ms,
                        options.initial_backoff_ms,
                        options.max_backoff_ms,
                    );
                }
                drop(report);
                if submitted.status.ok {
                    let wait_budget_ms = options
                        .controller
                        .timeout_ms
                        .max(50)
                        .min(delay_ms.saturating_mul(10).max(50));
                    // The budget is deliberately tied to the loop interval, not to
                    // `controller.timeout_ms`, so pause/stop stay responsive. That means a
                    // working cycle routinely finishes AFTER we stop waiting -- remember it and
                    // collect it at the top of a later tick rather than losing its report.
                    if let Some(cycle) = wait_for_storage_manager_cycle_completion(
                        &runtime,
                        submitted.job_id,
                        wait_budget_ms,
                    ) {
                        let mut report = thread_report
                            .lock()
                            .expect("storage manager runtime report lock poisoned");
                        apply_storage_manager_cycle_to_runtime_report(&mut report, cycle);
                    } else {
                        if uncollected_cycle_jobs.len() >= MAX_UNCOLLECTED_CYCLE_JOBS {
                            uncollected_cycle_jobs.remove(0);
                        }
                        uncollected_cycle_jobs.push(submitted.job_id);
                    }
                }
            }
            let mut report = thread_report
                .lock()
                .expect("storage manager runtime report lock poisoned");
            report.running = false;
            report.stopped = true;
        });
        StorageManagerRuntime {
            stop,
            paused,
            report,
            handle: Some(handle),
        }
    }

    /// Sweep every loaded shard with a per-round bound.
    ///
    /// `hot_limit` / `cold_limit` of 0 disable that residency class. A round takes at most that many
    /// due keys from the front of the deadline-ordered view, so there is nothing to resume: what
    /// is due is always the front, and a key not reached this round is still there next round.
    pub fn sweep_expired_records_bounded(
        &self,
        shard_id: ShardId,
        hot_limit: usize,
        cold_limit: usize,
    ) -> usize {
        // THIS SHARD ONLY. The storage manager runs one round per shard, and this used to sweep
        // every loaded shard from each of them: with S shards a tick took S sweeps of the whole
        // table, each under the shard-table write guard, and the removals of the other shards
        // were counted in this shard's report.
        let removed = self
            .inner
            .engine
            .sweep_expired_records_for_maintenance(crate::engine::reports::ShardExpirySweepRequest {
                shard_id,
                max_hot_buckets_per_round: hot_limit,
                max_cold_buckets_per_round: cold_limit,
                load_cold_buckets: true,
            })
            .map(|report| report.expired_records_removed)
            .unwrap_or(0);
        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned");
        stats.expiry_sweeps += 1;
        stats.expired_records_removed += removed as u64;
        removed
    }

    /// Test-only: sweeps every loaded shard, unbounded. The loop's expire stage sweeps its own
    /// shard through `sweep_expired_records_bounded`.
    pub fn sweep_expired_records(&self) -> usize {
        let reports = self.inner.engine.sweep_all_expired_records();
        let removed = reports
            .iter()
            .map(|report| report.expired_records_removed)
            .sum::<usize>();
        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned");
        stats.expiry_sweeps += 1;
        stats.expired_records_removed += removed as u64;
        removed
    }

    /// Test-only: a second driver that sweeps every loaded shard on its own timer. Nothing in the
    /// server starts it; the storage manager's expire stage is the driver.
    pub fn start_expiry_sweep_scheduler(&self, interval: Duration) -> ExpirySweepScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.sweep_expired_records();
            }
        });
        ExpirySweepScheduler {
            stop,
            handle: Some(handle),
        }
    }
}

/// Whether the index stage must dump now, whatever the byte gate says: the index log has grown to
/// the larger of the collector's floor and the last dump's image.
///
/// The invariant is that a dump costs about one image of bytes, so waiting until the log is as long
/// as the image keeps the bytes written per index-log byte at or below one. A fixed floor alone
/// would force a dump per floor's worth of log however large the image had grown, which makes bytes
/// written per operation grow with the key count. Below the floor the collector declines anyway, so
/// the floor is the lower bound and the image the scaling term.
fn index_log_forces_dump(index_log_bytes: u64, floor: u64, last_image_bytes: u64) -> bool {
    index_log_bytes >= floor.max(last_image_bytes)
}

#[cfg(test)]
mod index_log_forces_dump_guards {
    use super::index_log_forces_dump;

    const FLOOR: u64 = 1 << 20;

    /// A log between the floor and a larger image must not force a dump; one at the image must.
    ///
    /// Planted fault: comparing against the floor alone (`index_log_bytes >= floor`) makes the
    /// first assertion fail, because the log is past the floor and the image is not yet matched.
    #[test]
    fn a_log_shorter_than_the_last_image_does_not_force_a_dump() {
        let image = 8 * FLOOR;
        assert!(
            !index_log_forces_dump(2 * FLOOR, FLOOR, image),
            "a log past the floor but shorter than the image forced a dump"
        );
        assert!(!index_log_forces_dump(image - 1, FLOOR, image));
        assert!(index_log_forces_dump(image, FLOOR, image));
    }

    /// With no image (or a small one) the floor decides, so a log below it never forces a dump.
    ///
    /// Planted fault: comparing against the image alone makes a zero image force a dump on an
    /// empty log, and this fails on the first assertion.
    #[test]
    fn the_floor_bounds_the_decision_from_below() {
        assert!(!index_log_forces_dump(0, FLOOR, 0));
        assert!(!index_log_forces_dump(FLOOR - 1, FLOOR, 0));
        assert!(index_log_forces_dump(FLOOR, FLOOR, 0));
        assert!(!index_log_forces_dump(FLOOR - 1, FLOOR, FLOOR / 2));
    }
}

#[cfg(test)]
#[path = "maintenance_report_tests.rs"]
mod maintenance_report_tests;

#[cfg(test)]
mod eviction_loop_terms_tests {
    use super::{eviction_loop_live_stopping_conditions, EvictionLoopTerms};
    use crate::data_node::StorageManagerOptions;

    fn all_true() -> EvictionLoopTerms {
        EvictionLoopTerms {
            pressure_gate_open: true,
            more_budget: true,
            still_over: true,
            made_progress: true,
        }
    }

    #[test]
    fn the_batch_loop_stops_exactly_at_its_configured_limit() {
        let report = crate::engine::reports::StorageEvictionReport {
            pressure_gate_open: true,
            pressure_after: 64,
            ..Default::default()
        };
        let terms = super::eviction_loop_terms_for_batch(&report, 64, 100, 1, 1);
        assert!(!terms.still_over);
        assert!(!terms.should_continue());
        assert!(super::eviction_loop_terms_for_batch(&report, 63, 100, 1, 1).still_over);
    }

    #[test]
    fn the_shipped_configuration_has_no_implicit_memory_limit() {
        let options = StorageManagerOptions::default();
        assert!(options.enable_evict);
        assert_eq!(options.eviction_memory_pressure_threshold, 0);
        assert_eq!(options.index_resident_limit_bytes, 0);
        assert_eq!(crate::engine::reports::configured_eviction_memory_limit(
            options.eviction_memory_pressure_threshold, options.index_resident_limit_bytes,
        ), None);
    }

    /// The denominator: 2 of the 4 terms can stop the loop at the shipped default, 4 of 4 above
    /// it. Printing the count is the point -- a guard that says "the conditions are checked"
    /// without saying how many are live is the shape this replaces.
    #[test]
    fn live_stopping_condition_count_is_two_of_four_at_the_shipped_default() {
        let at_zero = eviction_loop_live_stopping_conditions(0);
        let above_zero = eviction_loop_live_stopping_conditions(1);
        assert_eq!(
            at_zero.len(),
            2,
            "expected 2 of 4 live stopping conditions at threshold 0, got {at_zero:?}",
        );
        assert_eq!(
            above_zero.len(),
            4,
            "expected 4 of 4 live stopping conditions above 0, got {above_zero:?}",
        );
        assert!(!at_zero.contains(&"still_over"));
        assert!(!at_zero.contains(&"pressure_gate_open"));
        assert!(above_zero.contains(&"still_over"));
        assert!(above_zero.contains(&"pressure_gate_open"));
    }

    /// `still_over` is `pressure_after >= threshold` on two `u64`s. At a threshold of 0 there is
    /// no value of `pressure_after` -- including `u64::MAX` and 0 itself -- that makes it false.
    #[test]
    fn still_over_cannot_be_false_at_threshold_zero() {
        let threshold_zero: u64 = 0;
        let mut false_cases = 0usize;
        let probes = [0u64, 1, 4_800, 1_775_000, u64::MAX];
        for pressure_after in probes {
            if !(pressure_after >= threshold_zero) {
                false_cases += 1;
            }
        }
        assert_eq!(
            false_cases, 0,
            "still_over fired at threshold 0 -- the documented vacuity is wrong",
        );
        // POSITIVE CONTROL over the same probe set: above 0 the comparison is a real test, so
        // the probes are proving something rather than passing because the loop body never ran.
        let threshold_raised: u64 = 1_775_000;
        let mut raised_false_cases = 0usize;
        for pressure_after in probes {
            if !(pressure_after >= threshold_raised) {
                raised_false_cases += 1;
            }
        }
        assert_eq!(
            raised_false_cases, 3,
            "expected 3 of {} probes below a raised threshold",
            probes.len(),
        );
    }

    /// The loop must not be able to terminate on a vacuous term at the shipped default. With the
    /// two live terms satisfied, the decision is `continue` no matter what the two vacuous terms
    /// would have said had they been reachable.
    #[test]
    fn at_threshold_zero_the_loop_stops_only_on_budget_or_progress() {
        let terms = all_true();
        assert!(terms.should_continue());
        assert!(terms.stops_only_on_live_conditions(0));

        let out_of_budget = EvictionLoopTerms {
            more_budget: false,
            ..all_true()
        };
        assert!(!out_of_budget.should_continue());
        assert!(
            out_of_budget.stops_only_on_live_conditions(0),
            "the budget is a live stopping condition at every threshold",
        );

        let no_progress = EvictionLoopTerms {
            made_progress: false,
            ..all_true()
        };
        assert!(!no_progress.should_continue());
        assert!(
            no_progress.stops_only_on_live_conditions(0),
            "progress is a live stopping condition at every threshold",
        );

        // The guard itself: a stop attributable ONLY to a term that cannot fire at 0.
        let leaned_on_still_over = EvictionLoopTerms {
            still_over: false,
            ..all_true()
        };
        assert!(!leaned_on_still_over.should_continue());
        assert!(
            !leaned_on_still_over.stops_only_on_live_conditions(0),
            "a loop that stops only because still_over went false at threshold 0 is relying on a \
             condition that cannot fire there",
        );

        let leaned_on_gate = EvictionLoopTerms {
            pressure_gate_open: false,
            ..all_true()
        };
        assert!(
            !leaned_on_gate.stops_only_on_live_conditions(0),
            "pressure_gate_open cannot be false at threshold 0",
        );

        // ... and above 0 both become legitimate stops. Same inputs, different threshold: this is
        // what makes the assertion above about the threshold and not about the struct.
        assert!(leaned_on_still_over.stops_only_on_live_conditions(1));
        assert!(leaned_on_gate.stops_only_on_live_conditions(1));
    }

    /// Every term is load-bearing in `should_continue`: dropping any one of the four flips the
    /// decision for at least one input. Keeps a future edit from quietly deleting a term.
    #[test]
    fn each_of_the_four_terms_can_stop_the_loop() {
        let mut stopped_by = 0usize;
        assert!(!EvictionLoopTerms {
            pressure_gate_open: false,
            ..all_true()
        }
        .should_continue());
        stopped_by += 1;
        assert!(!EvictionLoopTerms {
            more_budget: false,
            ..all_true()
        }
        .should_continue());
        stopped_by += 1;
        assert!(!EvictionLoopTerms {
            still_over: false,
            ..all_true()
        }
        .should_continue());
        stopped_by += 1;
        assert!(!EvictionLoopTerms {
            made_progress: false,
            ..all_true()
        }
        .should_continue());
        stopped_by += 1;
        assert_eq!(
            stopped_by, 4,
            "all four terms must be able to stop the loop once the threshold is raised",
        );
    }
}


/// Guards that the evict stage's batch loop TERMINATES, and in how many rounds.
///
/// WHY THESE EXIST. Forcing `made_progress` true at the point the terms are computed from a
/// batch report made this loop run for ever. 27 of 29 tests in the area still passed; two ran
/// unbounded. libtest has no per-test timeout, so nothing FAILED -- the run was cancelled, which
/// reads as infrastructure trouble rather than a defect, and names no test. The test written to
/// cover the loop's stopping conditions passed too: it catches "stopped too early" and never
/// "never stops".
///
/// The predicate `EvictionLoopTerms` had unit tests already. They construct the struct directly,
/// so the step that turns a report INTO terms -- where the spin lives -- was not reached by any
/// of them. These guards drive the real loop through the real wiring and assert the round count.
#[cfg(test)]
mod eviction_loop_termination_guards {
    use super::{eviction_loop_round_limit, eviction_loop_terms_for_batch, run_eviction_stage};
    use crate::engine::reports::{StorageEvictionReport, StorageEvictionVictim};

    /// The shipped victim budget, which is also the round limit derived from it.
    const BUDGET: usize = crate::data_node::DEFAULT_EVICTION_COUNT_LIMIT;

    #[test]
    fn the_terminal_batch_keeps_all_resident_release_work() {
        for gate_closed in [false, true] {
            let first = StorageEvictionReport {
                pressure_before: 1100, pressure_after: 700,
                bucket_index_bytes_before: 1000, bucket_index_bytes_after: 700,
                bucket_index_buckets_released: 2, bucket_index_blocks_released: 4,
                bucket_index_release_refused: 1, batch_limit: 10, dump_before_evict: true,
                ..batch(2)
            };
            let mut calls = 0;
            let stage = run_eviction_stage(400, 100, first, |_| {
                calls += 1;
                match calls {
                    1 => StorageEvictionReport {
                        pressure_before: 700, pressure_after: 500,
                        bucket_index_bytes_before: 700, bucket_index_bytes_after: 500,
                        bucket_index_buckets_released: 3, bucket_index_blocks_released: 7,
                        bucket_index_release_refused: 2, batch_limit: 5, ..batch(3)
                    },
                    2 => StorageEvictionReport {
                        pressure_before: if gate_closed { 301 } else { 500 },
                        pressure_after: if gate_closed { 301 } else { 500 },
                        bucket_index_bytes_before: if gate_closed { 300 } else { 500 },
                        bucket_index_bytes_after: if gate_closed { 300 } else { 500 },
                        pressure_gate_open: !gate_closed,
                        cooldown: !gate_closed,
                        skipped_reason: if gate_closed { "memory_pressure_below_threshold" } else { "" }.into(),
                        batch_limit: 5, ..Default::default()
                    },
                    _ => panic!("the terminal batch must stop the stage"),
                }
            }).into_stage_report();
            assert_eq!(calls, 2);
            assert_eq!(stage.selected_victims.len(), 5);
            assert_eq!(stage.bucket_index_buckets_released, 5);
            assert_eq!(stage.bucket_index_blocks_released, 11);
            assert_eq!(stage.bucket_index_release_refused, 3);
            assert_eq!(stage.bucket_index_bytes_before, 1000);
            assert_eq!(stage.bucket_index_bytes_after, if gate_closed { 300 } else { 500 });
            assert_eq!(stage.pressure_before, 1100);
            assert_eq!(stage.pressure_after, if gate_closed { 301 } else { 500 });
            assert_eq!(stage.batch_limit, 10);
            assert!(stage.pressure_gate_open);
            assert!(stage.dump_before_evict);
            assert!(!stage.cooldown);
            assert!(stage.skipped_reason.is_empty());
        }
    }

    #[test]
    fn accumulated_resident_release_counts_saturate() {
        let first = StorageEvictionReport {
            bucket_index_buckets_released: usize::MAX,
            bucket_index_blocks_released: usize::MAX,
            bucket_index_release_refused: usize::MAX,
            ..batch(1)
        };
        let stage = run_eviction_stage(0, 2, first, |_| StorageEvictionReport {
            bucket_index_buckets_released: 1,
            bucket_index_blocks_released: 1,
            bucket_index_release_refused: 1,
            ..batch(1)
        }).into_stage_report();
        assert_eq!(stage.bucket_index_buckets_released, usize::MAX);
        assert_eq!(stage.bucket_index_blocks_released, usize::MAX);
        assert_eq!(stage.bucket_index_release_refused, usize::MAX);
    }

    /// A batch that took `victims` victims, was not in cooldown, and did not take the pressure
    /// gate's early return -- i.e. every term except `made_progress` says continue.
    fn batch(victims: usize) -> StorageEvictionReport {
        StorageEvictionReport {
            batch_limit: victims.max(1),
            pressure_gate_open: true,
            cooldown: false,
            selected_victims: vec![StorageEvictionVictim::default(); victims],
            ..StorageEvictionReport::default()
        }
    }

    /// THE TERMINATION GUARD. A batch that finds nobody must stop the stage, in a stated number
    /// of rounds. Forcing `made_progress` true is what this exists to catch: before the round
    /// bound that mutation spun here instead of failing, and an unbounded test is a cancelled CI
    /// run with no named failure. Now the same mutation stops at the round limit and this fails
    /// on the number.
    #[test]
    fn a_batch_that_takes_no_victims_stops_the_stage_without_another_round() {
        let mut calls = 0usize;
        let tally = run_eviction_stage(0, BUDGET, batch(0), |_remaining| {
            calls += 1;
            batch(0)
        });
        assert_eq!(
            tally.rounds, 0,
            "expected 0 further rounds after a batch that found nobody, ran {}",
            tally.rounds,
        );
        assert_eq!(calls, 0, "the stage took {calls} more batches after finding nobody");
        assert!(tally.victims.is_empty(), "{} victims from empty batches", tally.victims.len());
    }

    /// A batch that freed nothing stops it too, by the other half of the same term.
    #[test]
    fn a_batch_in_cooldown_stops_the_stage_without_another_round() {
        let mut calls = 0usize;
        let seed = StorageEvictionReport { cooldown: true, ..batch(1) };
        let tally = run_eviction_stage(0, BUDGET, seed, |_remaining| {
            calls += 1;
            batch(1)
        });
        assert_eq!(tally.rounds, 0, "expected 0 further rounds after a cooldown batch");
        assert_eq!(calls, 0, "the stage took {calls} more batches after a cooldown batch");
    }

    /// The productive case, on a number: a stage whose every batch takes one victim spends the
    /// budget exactly and stops. This is the denominator for the two guards above -- without it
    /// they would pass on a loop that never ran a round at all.
    #[test]
    fn the_stage_spends_its_victim_budget_and_then_stops() {
        let mut calls = 0usize;
        let tally = run_eviction_stage(0, BUDGET, batch(1), |_remaining| {
            calls += 1;
            batch(1)
        });
        assert_eq!(
            tally.victims.len(),
            BUDGET,
            "expected the stage to take exactly its budget of {BUDGET} victims",
        );
        assert_eq!(
            tally.rounds,
            BUDGET - 1,
            "expected {} rounds after the seed batch, ran {}",
            BUDGET - 1,
            tally.rounds,
        );
        assert_eq!(calls, BUDGET - 1, "batches taken and rounds counted disagree");
    }

    #[test]
    fn sparse_selection_spends_the_requested_batch_budget() {
        for (budget, requested, expected_remaining) in [
            (100, 10, vec![90, 80, 70, 60, 50, 40, 30, 20, 10]),
            (25, 10, vec![15, 5]),
            (10, 10, vec![]),
            (0, 10, vec![]),
            (usize::MAX, usize::MAX, vec![]),
        ] {
            let mut remaining_budgets = Vec::new();
            let first = StorageEvictionReport { batch_limit: requested, ..batch(1) };
            let tally = run_eviction_stage(0, budget, first, |remaining| {
                remaining_budgets.push(remaining);
                StorageEvictionReport { batch_limit: requested.min(remaining), ..batch(1) }
            });
            assert_eq!(remaining_budgets, expected_remaining, "budget={budget}");
            assert_eq!(tally.rounds, expected_remaining.len());
            assert_eq!(tally.victims.len(), expected_remaining.len() + 1);
        }
    }

    /// The ROUND half of `more_budget`, asserted where the victim half cannot reach it: `taken`
    /// has not moved, so `count_limit > taken` is still true and only the round count can stop
    /// the loop. This is the term that turns a spin into a bounded run.
    #[test]
    fn the_round_bound_stops_a_loop_whose_victim_budget_never_advances() {
        let productive = batch(1);

        let below = eviction_loop_terms_for_batch(&productive, 0, BUDGET, 0, BUDGET - 1);
        assert!(
            below.more_budget,
            "round {} of {BUDGET} must still have budget",
            BUDGET - 1,
        );
        assert!(below.should_continue());

        let at_limit = eviction_loop_terms_for_batch(&productive, 0, BUDGET, 0, BUDGET);
        assert!(
            !at_limit.more_budget,
            "round {BUDGET} of {BUDGET} still had budget with 0 victims taken -- the budget \
             bounds victims only, which is what let the stage spin",
        );
        assert!(!at_limit.should_continue());
        assert_eq!(
            eviction_loop_round_limit(BUDGET),
            BUDGET,
            "the round limit is derived from the victim budget",
        );
    }

    /// THE WIRING. Exactly one of three batch shapes reads as progress. The predicate's own unit
    /// tests construct `EvictionLoopTerms` directly and so cannot see this step at all.
    #[test]
    fn exactly_one_of_three_batch_shapes_reads_as_progress() {
        let probes: [(&str, StorageEvictionReport, bool); 3] = [
            ("took a victim", batch(1), true),
            ("found nobody", batch(0), false),
            (
                "in cooldown",
                StorageEvictionReport { cooldown: true, ..batch(1) },
                false,
            ),
        ];
        let progressed = probes
            .iter()
            .filter(|(_, report, _)| {
                eviction_loop_terms_for_batch(report, 0, BUDGET, 0, 0).made_progress
            })
            .count();
        assert_eq!(
            progressed,
            1,
            "expected exactly 1 of {} batch shapes to read as progress, got {progressed}",
            probes.len(),
        );
        for (name, report, expected) in &probes {
            assert_eq!(
                eviction_loop_terms_for_batch(report, 0, BUDGET, 0, 0).made_progress,
                *expected,
                "batch shape {name} read the wrong way",
            );
        }
    }

    /// The accumulation the stage report is built from, so a bounded loop cannot buy termination
    /// by losing what the earlier batches did.
    #[test]
    fn the_stage_report_accumulates_every_batch_not_only_the_last() {
        let mut calls = 0usize;
        let seed = StorageEvictionReport {
            pressure_before: 1_775_000,
            cache_entries_removed: 3,
            ..batch(1)
        };
        let tally = run_eviction_stage(0, 4, seed, |_remaining| {
            calls += 1;
            StorageEvictionReport { cache_entries_removed: 2, ..batch(1) }
        });
        assert_eq!(tally.rounds, 3, "expected 3 rounds against a budget of 4");
        let report = tally.into_stage_report();
        assert_eq!(report.selected_victims.len(), 4, "the stage lost victims");
        assert_eq!(
            report.cache_entries_removed, 9,
            "expected 3 + 3 * 2 cache entries across the stage",
        );
        assert_eq!(
            report.pressure_before, 1_775_000,
            "pressure_before must come from the FIRST batch",
        );
    }
}


/// The longest a bounded page-GC step is meant to run. The window of candidates a round examines is
/// the real bound; this is the time budget the driver hands the step.
const PAGE_GC_STEP_TIME: Duration = Duration::from_millis(10);

/// Block collection as a bounded, resumable stage: each step examines a window of candidate slabs
/// starting behind the cursor, and reports whether more remain.
pub(crate) struct PageGcStage<'a> {
    runtime: &'a DataNodeRuntime,
}

impl PageGcStage<'_> {
    /// One bounded step, with the loop report the legacy tick also returns.
    pub(crate) fn run(
        &self,
        shard: ShardId,
        cursor: &mut PageCursor,
        budget: &mut StageBudget,
        context: &StageContext<'_>,
    ) -> (StageStep, StorageManagerLoopReport) {
        let mut options = options_for_slot(context.options, HeavySlot::PageGc);
        // The work budget can only shrink the window, and never to nothing: a step that examines
        // no candidate could not make progress.
        let window = match context.options.block_gc_max_scan_slabs_per_round {
            0 => 0,
            cap => cap.min(budget.work_left().min(usize::MAX as u64) as usize).max(1),
        };
        options.block_gc_scan_window = Some(super::BlockGcScanWindow {
            after_block_slab_id: cursor.after_slab_id,
            max_slabs: window,
        });
        // A round that starts a fresh pass asks the gate first: refresh the per-slab tally, then ask
        // whether the live fraction over noncurrent slabs is under the threshold. Both are O(slabs)
        // with no file opened. A pass already underway finishes; an unpublished tally reads as due.
        let mut scan_page_slabs = true;
        if options.enable_block_gc && cursor.after_slab_id.is_none() {
            // TIMED AS ITS OWN ROW. A round the gate declines runs no collection and is charged to no
            // other row, so without this a stage that is asked every tick and always declines leaves
            // no trace in the per-stage timing table. The row says how often the gate was asked and
            // what asking cost; its absence next to a `reclaim_page` row says nothing was collected.
            let gate_started = std::time::Instant::now();
            let engine = &self.runtime.inner.engine;
            engine.publish_block_slab_live_bytes();
            let gate = engine
                .block_store()
                .reclaim_gate(crate::block_store::DEFAULT_RECLAIM_UTILITY_THRESHOLD_BASIS_POINTS);
            scan_page_slabs = gate.due;
            super::task_execution::record_gc_phase(&self.runtime.inner, "gate", gate_started);
        }
        // The picker gate applies only to new slab collection. Eligible log
        // retirement and delayed destruction keep their own retention checks.
        let (report, cleanup_progress) = self.runtime
            .run_storage_manager_round(shard, options, false, scan_page_slabs);
        let (progress, work_done, status) = match report.gc_report.as_ref() {
            None => {
                *cursor = PageCursor::default();
                (cleanup_progress, 0, report.status.clone())
            }
            Some(gc) => {
                let examined = gc.block_gc_scan_examined_slabs as u64;
                budget.spend(examined);
                let more = !gc.block_gc_scan_complete && gc.block_gc_scan_last_block_slab_id.is_some();
                *cursor = if more {
                    PageCursor { after_slab_id: gc.block_gc_scan_last_block_slab_id }
                } else {
                    PageCursor::default()
                };
                (
                    if more { StageProgress::More } else { StageProgress::Done },
                    examined,
                    gc.status.clone(),
                )
            }
        };
        (StageStep { progress, work_done, status }, report)
    }
}

impl BoundedStage for PageGcStage<'_> {
    type Cursor = PageCursor;
    const NAME: &'static str = "reclaim_page";

    fn step(
        &self,
        shard: ShardId,
        cursor: &mut PageCursor,
        budget: &mut StageBudget,
        context: &StageContext<'_>,
    ) -> StageStep {
        self.run(shard, cursor, budget, context).0
    }
}

#[cfg(test)]
mod maintenance_expiry_budget_routes {
    use super::*;

    fn fixture(root: &std::path::Path) -> (TemporalEngine, DataNodeRuntime) {
        let engine = TemporalEngine::with_local_dirs(1 << 20,
            root.join("cache"), root.join("blocks"), root.join("indexes"));
        engine.load_shard(1);
        for index in 0..8 {
            assert!(engine.execute(crate::types::ExecuteRequest { shard_id: 1,
                command: crate::types::Command::StringSetEx {
                    key: format!("budget-due-{index}"), value: vec![index; 16], ttl_ms: 1,
                },
            }).status.ok);
        }
        engine.write_ahead_log_store().flush(1).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        let runtime = DataNodeRuntime::new_without_workers_with_options(engine.clone(),
            DataNodeRuntimeOptions { worker_threads: 0, max_queue_depth: 4,
                max_background_queue_depth: 2 });
        (engine, runtime)
    }

    fn options() -> StorageManagerOptions {
        StorageManagerOptions {
            enable_prepare: false, enable_wal_reclaim: false, enable_memory_reclaim: false,
            enable_expire: true, enable_block_gc: false, enable_block_compaction: false,
            enable_index_gc: false, enable_metrics_reap: false, enable_evict: false,
            max_expire_hot_buckets_per_round: 0, max_expire_cold_buckets_per_round: 0,
            ..Default::default()
        }
    }

    #[test]
    fn full_maintenance_round_preserves_due_records_at_zero_expiry_budgets() {
        let root = tempfile::tempdir().unwrap();
        let (engine, runtime) = fixture(root.path());
        let writes = engine.write_ahead_log_store().stats(1).writes;
        let report = runtime.run_storage_manager_once(1, options());
        assert!(report.status.ok, "{report:?}");
        assert_eq!(report.expired_records_removed, 0);
        assert_eq!(runtime.stats().storage_manager_expire_runs, 1);
        assert_eq!(engine.write_ahead_log_store().stats(1).writes, writes);
        assert_eq!(engine.sweep_expired_records(1).unwrap().expired_records_removed, 8);
    }

    #[test]
    fn cheap_maintenance_tick_preserves_due_records_at_zero_expiry_budgets() {
        let root = tempfile::tempdir().unwrap();
        let (engine, runtime) = fixture(root.path());
        let writes = engine.write_ahead_log_store().stats(1).writes;
        let period = Duration::from_secs(3600);
        let mut state = ShardRoundState::new(std::time::Instant::now(), period);
        assert!(runtime.run_storage_manager_tick(1, &options(), &mut state, period).is_none());
        assert_eq!(runtime.stats().storage_manager_expire_runs, 1);
        assert_eq!(runtime.stats().expired_records_removed, 0);
        assert_eq!(engine.write_ahead_log_store().stats(1).writes, writes);
        assert_eq!(engine.sweep_expired_records(1).unwrap().expired_records_removed, 8);
    }
}

#[cfg(test)]
mod page_gate_is_timed {
    use super::*;

    #[test]
    fn a_closed_slab_gate_still_purges_mature_quarantine() {
        let root = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024, root.path().join("cache"),
            root.path().join("blocks"), root.path().join("indexes"),
        );
        engine.load_shard(1);
        let store = engine.block_store();
        let victim = store.append_block_of_object_for_shard(1, &[17; 128], None, None, 0)
            .unwrap().block_slab_id();
        let roll = store.roll_slab_for_shard(1).unwrap();
        assert_eq!(roll.previous_block_slab_id, victim);
        let quarantined = store.gc_slabs_before_with_live_refs_policy_limited(
            roll.new_block_slab_id, Vec::<u64>::new(),
            crate::block_store::BlockStoreGcPolicy::max_slabs(1), true,
            Some(std::collections::BTreeSet::from([victim])),
        ).unwrap();
        assert_eq!(quarantined.delayed_destroy_block_slab_ids, vec![victim]);
        engine.publish_block_slab_live_bytes();
        let gate = store.reclaim_gate(crate::block_store::DEFAULT_RECLAIM_UTILITY_THRESHOLD_BASIS_POINTS);
        assert!(!gate.due, "{gate:?}");

        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine.clone(),
            DataNodeRuntimeOptions { worker_threads: 0, max_queue_depth: 4, max_background_queue_depth: 2 },
        );
        let step = |enabled| {
            let options = StorageManagerOptions { enable_block_gc: enabled, ..Default::default() };
            let context = StageContext { options: &options, plan: None };
            let (step, report) = PageGcStage { runtime: &runtime }.run(
                1, &mut PageCursor::default(), &mut StageBudget::new(PAGE_GC_STEP_TIME, u64::MAX),
                &context,
            );
            assert!(report.status.ok, "{report:?}");
            assert!(report.gc_report.is_none(), "the physical picker must remain closed");
            (step, report)
        };
        let _ = step(true);
        assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![victim], "grace must hold");
        assert_eq!(store.backdate_delayed_destroy_stamps_for_test(
            crate::engine::reports::DEFAULT_BLOCK_GC_DELAYED_DESTROY_GRACE_MS.saturating_add(60_000),
        ).unwrap(), 1);
        let _ = step(false);
        assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![victim], "explicit disable must hold");
        let (result, report) = step(true);
        assert!(matches!(result.progress, StageProgress::Done));
        assert!(report.executed_stages.iter().any(|name| name == "reclaim_page"));
        assert!(store.delayed_destroy_slab_ids().unwrap().is_empty());
        assert!(!store.slab_ids().unwrap().contains(&victim));
    }


    #[test]
    fn a_closed_slab_gate_retires_rolled_wal_after_follower_release() {
        use crate::engine::reports::BucketDumpFollowerReplayCursor;
        use crate::types::{Command, CommandResponse, ExecuteRequest};

        const CHILD_ROOT: &str = "TS_TEST_PAGE_GATE_WAL_REOPEN";
        let check_values = |engine: &TemporalEngine| {
            for index in 0..64u8 {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1, command: Command::StringGet { key: format!("owner-{index:04}") },
                });
                assert!(response.status.ok, "{:?}", response.status);
                assert_eq!(response.response, CommandResponse::Bytes { value: Some(vec![index + 1; 64]) });
            }
        };
        let open = |root: &std::path::Path, cache: &str| {
            let engine = TemporalEngine::with_local_dirs(
                64 * 1024 * 1024, root.join(cache), root.join("blocks"), root.join("indexes"),
            );
            engine.load_shard(1);
            engine
        };
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let engine = open(std::path::Path::new(&root), "child-cache");
            check_values(&engine);
            eprintln!("verified 64 Page GC WAL survivor values after reopen");
            return;
        }

        struct RestoreWalRoll;
        impl Drop for RestoreWalRoll {
            fn drop(&mut self) {
                crate::wal::set_wal_segment_bytes_for_test(None);
            }
        }
        let _roll = RestoreWalRoll;
        crate::wal::set_wal_segment_bytes_for_test(Some(1024));
        let root = tempfile::tempdir().unwrap();
        let engine = open(root.path(), "seed-cache");
        assert!(engine.set_config(crate::control::SetConfigRequest {
            shard_id: 1,
            config: crate::control::Config { version: 2, async_storage: true, ..Default::default() },
        }).ok);
        for version in 0..2 {
            for index in 0..64u8 {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("owner-{index:04}"),
                        value: vec![if version == 0 { 0 } else { index + 1 }; 64],
                    },
                });
                assert!(response.status.ok, "{:?}", response.status);
            }
        }
        let wal = engine.write_ahead_log_store();
        wal.flush(1).unwrap();
        let info = wal.info(1).unwrap();
        let pieces = crate::wal::wal_piece_extents_for_test(info.path.parent().unwrap(), 1);
        assert!(pieces.iter().filter(|(_, named, _, _)| named.is_some()).count() >= 2);

        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine.clone(),
            DataNodeRuntimeOptions { worker_threads: 0, max_queue_depth: 4, max_background_queue_depth: 2 },
        );
        let dump_options = StorageManagerOptions {
            enable_wal_reclaim: true, min_undumped_wal_records: 0, min_undumped_wal_bytes: 0,
            max_dump_buckets_per_round: 0, ..Default::default()
        };
        let dump = runtime.run_storage_manager_once_with_report_details(
            1, options_for_slot(&dump_options, HeavySlot::WalReclaim), false,
        );
        assert!(dump.status.ok && dump.wal_reclaim_round.dumped, "{dump:?}");
        assert_eq!(engine.storage_dump_wal_sequence(1), Some(128));
        assert!(pieces.iter().all(|(path, _, _, _)| path.is_file()));

        engine.publish_block_slab_live_bytes();
        let gate = engine.block_store().reclaim_gate(crate::block_store::DEFAULT_RECLAIM_UTILITY_THRESHOLD_BASIS_POINTS);
        assert!(gate.ready && !gate.due, "{gate:?}");
        let step = |options: &StorageManagerOptions| {
            let context = StageContext { options, plan: None };
            let (step, report) = PageGcStage { runtime: &runtime }.run(
                1, &mut PageCursor::default(), &mut StageBudget::new(PAGE_GC_STEP_TIME, u64::MAX),
                &context,
            );
            assert!(report.status.ok, "{report:?}");
            assert!(report.gc_report.is_none(), "WAL cleanup must not open the slab picker");
            (step, report)
        };
        let mut options = StorageManagerOptions { enable_block_gc: true, ..Default::default() };
        options.follower_replay_cursors.push(BucketDumpFollowerReplayCursor {
            shard_id: 1, follower_id: "page-gate-reader".into(), ..Default::default()
        });
        let (_, held) = step(&options);
        let held = held.page_log_reclaim.unwrap();
        assert!(held.plan.follower_cursor_block_count > 0);
        assert_eq!(held.wal_records_removed, 0);
        assert_eq!(held.wal_segments_dropped, 0);
        assert!(pieces.iter().all(|(path, _, _, _)| path.is_file()));

        options.follower_replay_cursors.clear();
        let (progress, released) = step(&options);
        let reclaimed = released.page_log_reclaim.unwrap();
        assert!(reclaimed.applied && reclaimed.wal_segments_dropped > 0, "{reclaimed:?}");
        assert!(matches!(progress.progress, StageProgress::Done));
        assert!(pieces.iter().any(|(path, named, _, _)| named.is_some() && !path.exists()));
        drop(wal);
        drop(runtime);
        drop(engine);

        let (_, module) = module_path!().split_once("::").unwrap();
        let selector = format!("{module}::a_closed_slab_gate_retires_rolled_wal_after_follower_release");
        let stdout_path = root.path().join("child-stdout.log");
        let stderr_path = root.path().join("child-stderr.log");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &selector, "--nocapture"])
            .env(CHILD_ROOT, root.path())
            .stdin(std::process::Stdio::null())
            .stdout(std::fs::File::create(&stdout_path).unwrap())
            .stderr(std::fs::File::create(&stderr_path).unwrap())
            .spawn().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let result = loop {
            match child.try_wait() {
                Ok(Some(status)) => break Ok(status),
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(None) => break Err("child reopen timed out".to_string()),
                Err(error) => break Err(format!("child reopen wait failed: {error}")),
            }
        };
        if result.is_err() {
            let _ = child.kill();
            child.wait().expect("reap the child after a failed reopen wait");
        }
        let stdout = std::fs::read_to_string(stdout_path).unwrap();
        let stderr = std::fs::read_to_string(stderr_path).unwrap();
        assert!(result.as_ref().is_ok_and(|status| status.success())
            && stderr.contains("verified 64 Page GC WAL survivor values after reopen"),
            "reopen result={result:?} stdout={stdout} stderr={stderr}");
    }



    /// A tick page step on a store with nothing to collect must still be visible: the gate that
    /// declined it is charged to `reclaim_page.gate`, one round per fresh pass, and no collection
    /// row appears (the control: the gate really did decline). Planted fault: drop the
    /// `record_gc_phase` call and the row is absent.
    #[test]
    fn a_declined_page_step_reports_its_gate() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions { worker_threads: 0, max_queue_depth: 4, max_background_queue_depth: 2 },
        );
        assert!(
            runtime.stats().storage_manager_stage_timing.keys().all(|name| !name.starts_with("reclaim_page")),
            "a page row existed before any step ran"
        );
        let options = StorageManagerOptions::default();
        for round in 1..=3u64 {
            let mut cursor = PageCursor::default();
            let mut budget = StageBudget::new(PAGE_GC_STEP_TIME, u64::MAX);
            let context = StageContext { options: &options, plan: None };
            let (_, report) = PageGcStage { runtime: &runtime }.run(1, &mut cursor, &mut budget, &context);
            assert!(report.gc_report.is_none(), "the gate was expected to decline: {report:?}");
            let timing = runtime.stats().storage_manager_stage_timing;
            let gate = timing.get("reclaim_page.gate").expect("a declined step reports its gate");
            assert_eq!(gate.rounds_executed, round);
            assert!(gate.max_us <= gate.total_us, "{gate:?}");
            assert!(timing.get("reclaim_page").is_none(), "a declined step must not claim a collection row");
        }
    }
}
