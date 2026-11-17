// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The storage loop as short ticks, one heavy stage at a time, each stage able to be bounded.
//!
//! A whole storage round reads every live block of the shard several times to plan itself, then
//! runs every stage to completion. On a 100k-key shard that is several seconds in one piece, and
//! everything that queues behind it waits. This module splits the loop two ways.
//!
//! * A TICK is short. Every tick does the cheap work (pre-allocating the next slab, one bounded
//!   expiry sweep) and AT MOST ONE heavy stage.
//! * A heavy stage runs when it is DUE, once per period, and the five stages are staggered across
//!   the period so no two share a tick.
//!
//! The types below are the contract a stage implements to stop being "a whole pass per due time"
//! and become "a bounded step per tick": it is handed a [`StageBudget`], keeps its own cursor, and
//! reports whether it finished a pass, ran out of budget, or had nothing to do. A stage that has
//! not adopted the contract runs through the legacy path in `runtime_storage.rs`: the existing
//! whole-pass function with only its own stage enabled, at its own due time.

use super::*;
use std::time::{Duration, Instant};

/// The tick period, in milliseconds, the server runs the storage manager with when the
/// environment does not set one. Zero is the whole-round loop.
///
/// The tick loop is opt-in until a comparison of the two on the same fixture, showing whole-round
/// latency and loop CPU share, is part of the change that makes it the default.
pub const DEFAULT_STORAGE_MANAGER_TICK_MS: u64 = 0;

/// The tick period for the value of `MATRIXARK_STORAGE_MANAGER_TICK_MS`, if it is set.
/// Zero selects the whole-round loop; anything unparseable falls back to the default.
pub fn storage_manager_tick_ms(raw: Option<&str>) -> u64 {
    raw.and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_STORAGE_MANAGER_TICK_MS)
}

/// What one tick hands a stage: a deadline and a count of work items it may touch.
///
/// Spent through `spend`, checked through `exhausted` BETWEEN work items. Never checked while a
/// lock is held, so the longest a stage can overrun is one item.
#[allow(dead_code)]
pub(crate) struct StageBudget {
    deadline: Instant,
    work_left: u64,
}

#[allow(dead_code)]
impl StageBudget {
    pub(crate) fn new(time: Duration, work_units: u64) -> Self {
        Self {
            deadline: Instant::now() + time,
            work_left: work_units,
        }
    }

    pub(crate) fn exhausted(&self) -> bool {
        self.work_left == 0 || Instant::now() >= self.deadline
    }

    pub(crate) fn spend(&mut self, units: u64) {
        self.work_left = self.work_left.saturating_sub(units);
    }

    pub(crate) fn time_left(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

/// How a bounded step ended.
#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum StageProgress {
    /// Nothing to do, decided from counters. The cursor is not kept.
    Idle,
    /// A pass finished inside the budget. The cursor is reset.
    Done,
    /// The budget ran out first. The cursor is kept and the stage is called again next tick.
    More,
}

/// The result of one bounded step.
#[allow(dead_code)]
pub(crate) struct StageStep {
    pub(crate) progress: StageProgress,
    /// Items touched, in the stage's own unit.
    pub(crate) work_done: u64,
    pub(crate) status: Status,
}

/// A stage that can be called with a budget and resumed from its own cursor.
///
/// The cursor is a hint, identified by key or id and never by position: writes and other stages
/// change the target between ticks, and a cursor that no longer resolves restarts the pass. It
/// lives in memory only; a restart starts a new pass, which is safe because a pass is idempotent.
#[allow(dead_code)]
pub(crate) trait BoundedStage {
    type Cursor: Default + Send;
    /// The existing stage label, as the loop's per-stage series spell it.
    const NAME: &'static str;
    fn step(
        &self,
        shard: ShardId,
        cursor: &mut Self::Cursor,
        budget: &mut StageBudget,
        context: &StageContext<'_>,
    ) -> StageStep;
}

/// What the driver lends a stage for one step. `options` is the SAME struct the loop was started
/// with, not a trimmed copy, so retention owners and grace periods reach every stage; `plan` is
/// the shared round plan once the driver builds one per round (None until then, and a stage must
/// work without it).
#[allow(dead_code)]
pub(crate) struct StageContext<'a> {
    pub(crate) options: &'a StorageManagerOptions,
    pub(crate) plan: Option<&'a RoundPlan>,
}

/// Everything the loop computes once per round and lends read-only to the stages, so none of
/// them walks the shard to plan itself. A stage that needs one more input from that walk adds a
/// field here in its own change.
#[allow(dead_code)]
pub(crate) struct RoundPlan {
    pub(crate) lifecycle: StorageLifecyclePlan,
}

/// The resume positions of the stages that have adopted [`BoundedStage`]. Each stage's owner adds
/// one field here, of that stage's cursor type, and nothing else in this module.
#[derive(Default)]
pub(crate) struct StageCursors {}

/// One heavy unit of the loop. Each is a whole stage, or the group of cheap stages that share the
/// plan the loop builds before any stage runs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum HeavySlot {
    WalReclaim,
    Memory,
    PageGc,
    IndexGc,
    Compaction,
}

impl HeavySlot {
    pub(crate) const ALL: [HeavySlot; 5] = [
        HeavySlot::WalReclaim,
        HeavySlot::Memory,
        HeavySlot::PageGc,
        HeavySlot::IndexGc,
        HeavySlot::Compaction,
    ];

    fn ordinal(self) -> usize {
        Self::ALL
            .iter()
            .position(|slot| *slot == self)
            .expect("every slot is in ALL")
    }
}

/// `base` with everything switched off except the stages of `slot`, and each of those only if
/// `base` had it on. The legacy whole-pass function then runs just that slot.
pub(crate) fn options_for_slot(
    base: &StorageManagerOptions,
    slot: HeavySlot,
) -> StorageManagerOptions {
    let mut options = base.clone();
    options.enable_prepare = false;
    options.enable_expire = false;
    options.enable_wal_reclaim = false;
    options.enable_memory_reclaim = false;
    options.enable_evict = false;
    options.enable_block_gc = false;
    options.enable_index_gc = false;
    options.enable_block_compaction = false;
    options.enable_metrics_reap = false;
    match slot {
        HeavySlot::WalReclaim => options.enable_wal_reclaim = base.enable_wal_reclaim,
        HeavySlot::Memory => {
            options.enable_memory_reclaim = base.enable_memory_reclaim;
            options.enable_evict = base.enable_evict;
            options.enable_metrics_reap = base.enable_metrics_reap;
        }
        HeavySlot::PageGc => options.enable_block_gc = base.enable_block_gc,
        HeavySlot::IndexGc => options.enable_index_gc = base.enable_index_gc,
        HeavySlot::Compaction => options.enable_block_compaction = base.enable_block_compaction,
    }
    options
}

/// What the tick scheduler remembers about one shard between ticks.
pub(crate) struct ShardRoundState {
    next_due: [Instant; HeavySlot::ALL.len()],
    #[allow(dead_code)]
    pub(crate) cursors: StageCursors,
}

impl ShardRoundState {
    /// Slot `i` first falls due `(i + 1) / (slots + 1)` of a period from `now`, so the slots are
    /// spread across the first period instead of all falling due together.
    pub(crate) fn new(now: Instant, period: Duration) -> Self {
        let slots = HeavySlot::ALL.len() as u32;
        let mut next_due = [now; HeavySlot::ALL.len()];
        for slot in HeavySlot::ALL {
            let index = slot.ordinal() as u32;
            next_due[slot.ordinal()] = now + period * (index + 1) / (slots + 1);
        }
        Self {
            next_due,
            cursors: StageCursors::default(),
        }
    }

    /// The most overdue slot, if any is due. One per tick.
    pub(crate) fn due_slot(&self, now: Instant) -> Option<HeavySlot> {
        HeavySlot::ALL
            .into_iter()
            .filter(|slot| self.next_due[slot.ordinal()] <= now)
            .min_by_key(|slot| self.next_due[slot.ordinal()])
    }

    /// The slot ran and ended at `now`: its next turn is a period later.
    pub(crate) fn finished(&mut self, slot: HeavySlot, now: Instant, period: Duration) {
        self.next_due[slot.ordinal()] = now + period;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The server starts the whole-round loop unless the tick period is set, and an unusable
    /// setting does not switch the tick loop on by accident.
    #[test]
    fn the_whole_round_loop_is_the_default_and_the_tick_loop_is_opt_in() {
        assert_eq!(storage_manager_tick_ms(None), 0, "unset must run the whole-round loop");
        assert_eq!(storage_manager_tick_ms(Some("not a number")), 0);
        assert_eq!(storage_manager_tick_ms(Some("")), 0);
        assert_eq!(storage_manager_tick_ms(Some(" 30 ")), 30);
        assert_eq!(storage_manager_tick_ms(Some("0")), 0);
    }

    #[test]
    fn the_slots_are_spread_over_the_period_and_each_runs_once_per_period() {
        let start = Instant::now();
        let period = Duration::from_secs(10);
        let tick = Duration::from_millis(30);
        let mut state = ShardRoundState::new(start, period);
        let mut runs: Vec<Vec<Duration>> = vec![Vec::new(); HeavySlot::ALL.len()];
        let mut now = start;
        // 35 simulated seconds at one tick every 30 ms; a heavy stage takes 1 s.
        while now.duration_since(start) < Duration::from_secs(35) {
            if let Some(slot) = state.due_slot(now) {
                runs[slot.ordinal()].push(now.duration_since(start));
                state.finished(slot, now + Duration::from_secs(1), period);
                now += Duration::from_secs(1);
            } else {
                now += tick;
            }
        }
        for (ordinal, times) in runs.iter().enumerate() {
            assert!(
                times.len() >= 3,
                "slot {ordinal} ran {} times in 35 s at a 10 s period",
                times.len()
            );
            for pair in times.windows(2) {
                assert!(
                    pair[1] - pair[0] >= period,
                    "slot {ordinal} ran twice inside one period: {pair:?}"
                );
            }
        }
        let firsts: Vec<Duration> = runs.iter().map(|times| times[0]).collect();
        let mut sorted = firsts.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            firsts.len(),
            "no two slots may first run in the same moment: {firsts:?}"
        );
    }

    #[test]
    fn a_slot_keeps_only_its_own_stages_and_only_those_the_base_had_on() {
        let base = StorageManagerOptions::default();
        for slot in HeavySlot::ALL {
            let options = options_for_slot(&base, slot);
            let on = [
                options.enable_prepare,
                options.enable_expire,
                options.enable_wal_reclaim,
                options.enable_memory_reclaim,
                options.enable_evict,
                options.enable_block_gc,
                options.enable_index_gc,
                options.enable_block_compaction,
                options.enable_metrics_reap,
            ];
            let count = on.iter().filter(|flag| **flag).count();
            let expected = match slot {
                HeavySlot::Memory => 1 + base.enable_evict as usize + 1,
                _ => 1,
            };
            assert_eq!(count, expected, "{slot:?} left {on:?} switched on");
        }
        let mut off = StorageManagerOptions::default();
        off.enable_wal_reclaim = false;
        let options = options_for_slot(&off, HeavySlot::WalReclaim);
        assert!(!options.enable_wal_reclaim, "a stage the base had off stays off");
    }

    /// A tick runs the cheap work and at most ONE heavy stage, and over the five ticks the five
    /// slots fall due in, each stage runs exactly once and only the stages of its own slot.
    #[test]
    fn a_tick_runs_one_heavy_slot_and_each_slot_runs_only_its_own_stages() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for index in 0..16 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("tick-{index:02}"),
                    value: vec![b'v'; 32],
                },
            });
            assert!(response.status.ok);
        }
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let options = StorageManagerOptions::default();
        let period = Duration::from_secs(600);
        // Every slot already overdue.
        let mut state = ShardRoundState::new(Instant::now() - period * 2, period);
        let mut seen = Vec::new();
        for _ in 0..HeavySlot::ALL.len() {
            let (slot, report) = runtime
                .run_storage_manager_tick(1, &options, &mut state, period)
                .expect("an overdue slot must run on the tick");
            let allowed: &[&str] = match slot {
                HeavySlot::WalReclaim => &["reclaim_wal"],
                HeavySlot::Memory => &["reclaim_memory", "evict", "reap_metrics"],
                HeavySlot::PageGc => &["reclaim_page"],
                HeavySlot::IndexGc => &["reclaim_index"],
                HeavySlot::Compaction => &["compact_pages"],
            };
            for stage in &report.executed_stages {
                assert!(
                    allowed.contains(&stage.as_str()),
                    "{slot:?} ran {stage}, which belongs to another slot: {:?}",
                    report.executed_stages
                );
            }
            assert!(!seen.contains(&slot), "{slot:?} ran twice in five ticks");
            seen.push(slot);
        }
        assert_eq!(seen.len(), HeavySlot::ALL.len());
        assert!(
            runtime
                .run_storage_manager_tick(1, &options, &mut state, period)
                .is_none(),
            "every slot just ran, so the next tick has only the cheap work"
        );
    }

    #[test]
    fn a_budget_is_spent_by_work_and_by_time() {
        let mut budget = StageBudget::new(Duration::from_secs(60), 3);
        assert!(!budget.exhausted());
        budget.spend(2);
        assert!(!budget.exhausted());
        budget.spend(5);
        assert!(budget.exhausted(), "work is spent");
        let late = StageBudget::new(Duration::ZERO, 100);
        assert!(late.exhausted(), "time is spent");
        assert_eq!(late.time_left(), Duration::ZERO);
    }
}
