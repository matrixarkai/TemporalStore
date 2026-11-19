// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Compaction as a bounded stage: a step per tick instead of a whole pass per due time.
//!
//! A step does its share of one sliced pass (`engine/compaction_slices.rs`) and returns. The pass
//! itself lives in the engine, one slot per shard, so the cursor this stage keeps is only the set of
//! slabs the pass was started to drain: the stage chooses it once, from the plan, and every later
//! step resumes the same pass without planning again.
//!
//! WHAT A STEP COSTS. A slice costs the keys it moves, and a step runs slices until its budget is
//! spent: [`COMPACTION_ITEMS_PER_STEP`] keys or [`COMPACTION_STEP_TIME`], whichever ends first, and
//! the budget is checked between slices and never inside one, so a step overruns by at most one
//! slice. The first step of a pass also pays the pass's setup, and the last pays its finish; both
//! still read the whole shard. They are bounded by the shard, not by the budget, and a measurement
//! of this stage reports them apart from the slices.

use super::round_budget::{BoundedStage, StageBudget, StageContext, StageProgress, StageStep};
use super::*;
use std::collections::BTreeSet;
use std::time::Duration;

/// Keys one step may move: the cap a round of this stage has always had.
pub(crate) const COMPACTION_ITEMS_PER_STEP: u64 = 200;

/// Time one step may take, checked between slices.
pub(crate) const COMPACTION_STEP_TIME: Duration = Duration::from_millis(10);

/// Where a pass is: the slabs it drains once it has started, nothing before.
#[derive(Default)]
pub(crate) struct CompactionCursor {
    drain_block_slab_ids: Option<BTreeSet<u64>>,
}

/// Whether the shard's slab pressure calls for compaction. One definition, read by the whole round
/// and by this stage, so the two cannot disagree about when a pass is worth starting.
pub(super) fn stale_block_pressure(
    pressure: &StorageManagerPressureSnapshot,
    options: &StorageManagerOptions,
) -> bool {
    pressure.stale_block_slab_count >= options.stale_block_slab_pressure.max(1)
        || pressure.reclaim_candidate_count >= options.stale_block_slab_pressure.max(1)
        || pressure.reclaimable_physical_bytes >= options.reclaimable_physical_bytes_pressure.max(1)
}

pub(crate) struct CompactionStage<'a> {
    pub(crate) runtime: &'a DataNodeRuntime,
    /// Keys a slice moves. The budget is spent in these.
    pub(crate) keys_per_slice: usize,
}

impl<'a> CompactionStage<'a> {
    pub(crate) fn new(runtime: &'a DataNodeRuntime) -> Self {
        Self {
            runtime,
            keys_per_slice: crate::engine::COMPACTION_SLICE_KEYS,
        }
    }

    fn idle() -> StageStep {
        StageStep {
            progress: StageProgress::Idle,
            work_done: 0,
            status: Status::ok(),
        }
    }

    /// The slabs to drain, or `None` when this shard needs no pass: compaction is off, the node
    /// follows the shard, there is no slab pressure, or no object holds a block on a slab that is
    /// being emptied. The same four questions the whole round asks, in the same order.
    fn drain_set(&self, shard: ShardId, options: &StorageManagerOptions) -> Option<BTreeSet<u64>> {
        if !options.enable_block_compaction
            || self.runtime.shard_leadership(shard).is_known_not_leading()
        {
            return None;
        }
        let (pressure, plan) = self.runtime.storage_manager_pressure_snapshot(shard, options);
        if !stale_block_pressure(&pressure, options)
            || crate::engine::compaction_relocatable_block_refs(&plan.reclaim_candidates) == 0
        {
            return None;
        }
        Some(crate::engine::compaction_drain_block_slab_ids(&plan.reclaim_candidates))
    }
}

impl BoundedStage for CompactionStage<'_> {
    type Cursor = CompactionCursor;
    const NAME: &'static str = "compact_pages";

    fn step(
        &self,
        shard: ShardId,
        cursor: &mut CompactionCursor,
        budget: &mut StageBudget,
        context: &StageContext<'_>,
    ) -> StageStep {
        if cursor.drain_block_slab_ids.is_none() {
            // A pass the engine still holds from a step whose cursor was lost resumes under the
            // slabs it began with, so the set chosen here only matters for a pass that starts now.
            match self.drain_set(shard, context.options) {
                Some(drain) => cursor.drain_block_slab_ids = Some(drain),
                None => return Self::idle(),
            }
        }
        let drain = cursor
            .drain_block_slab_ids
            .clone()
            .expect("the drain set was just chosen");
        let mut work_done = 0_u64;
        loop {
            match self
                .runtime
                .inner
                .engine
                .compact_shard_blocks_slice(shard, &drain, self.keys_per_slice)
            {
                Ok(crate::engine::SliceProgress::Done) => {
                    *cursor = CompactionCursor::default();
                    return StageStep {
                        progress: StageProgress::Done,
                        work_done,
                        status: Status::ok(),
                    };
                }
                Ok(crate::engine::SliceProgress::More) => {
                    work_done += self.keys_per_slice as u64;
                    budget.spend(self.keys_per_slice as u64);
                    if budget.exhausted() {
                        return StageStep {
                            progress: StageProgress::More,
                            work_done,
                            status: Status::ok(),
                        };
                    }
                }
                Err(status) => {
                    // The failed pass has published what it moved and kept its anchor; the next
                    // pass starts from a fresh plan.
                    *cursor = CompactionCursor::default();
                    return StageStep {
                        progress: StageProgress::Done,
                        work_done,
                        status,
                    };
                }
            }
        }
    }
}

impl DataNodeRuntime {
    /// The compaction slot of a tick: one bounded step, and the slot stays due while the pass has
    /// work left, so the next tick resumes it instead of waiting a period.
    pub(crate) fn run_compaction_step(
        &self,
        shard_id: ShardId,
        options: &StorageManagerOptions,
        state: &mut round_budget::ShardRoundState,
        period: Duration,
    ) -> StorageManagerLoopReport {
        let started = std::time::Instant::now();
        let mut budget = StageBudget::new(COMPACTION_STEP_TIME, COMPACTION_ITEMS_PER_STEP);
        let context = StageContext { options, plan: None };
        let step = CompactionStage::new(self).step(
            shard_id,
            &mut state.cursors.compaction,
            &mut budget,
            &context,
        );
        if step.progress != StageProgress::Idle {
            let mut stats = self.inner.stats.lock().expect("runtime stats lock poisoned");
            stats.storage_manager_compact_runs += 1;
            if step.progress == StageProgress::Done {
                stats.compaction_runs += 1;
            }
        }
        let now = std::time::Instant::now();
        match step.progress {
            StageProgress::More => state.resume_next_tick(round_budget::HeavySlot::Compaction, now),
            _ => state.finished(round_budget::HeavySlot::Compaction, now, period),
        }
        self.note_round_overrun(started.elapsed(), period);
        let (executed_stages, skipped_stages) = if step.progress == StageProgress::Idle {
            (Vec::new(), vec!["compact_pages_idle".to_string()])
        } else {
            (vec!["compact_pages".to_string()], Vec::new())
        };
        StorageManagerLoopReport {
            shard_id,
            pressure: StorageManagerPressureSnapshot::default(),
            executed_stages,
            skipped_stages,
            pressure_decisions: Vec::new(),
            lifecycle_plan: StorageLifecyclePlan::default(),
            lifecycle_report: None,
            expired_records_removed: 0,
            metrics_reap: None,
            compaction_report: None,
            gc_report: None,
            eviction: None,
            status: step.status,
            stage_spans: Vec::new(),
            duration_us: started.elapsed().as_micros() as u64,
        }
    }
}
