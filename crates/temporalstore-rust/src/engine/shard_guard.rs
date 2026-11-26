// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Shard-local state access with opt-in per-call-site hold measurements.
//! Registry membership is independent of each owner's read/write lock.

use super::*;

use std::cell::Cell;
use std::panic::Location;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

/// Sites kept in the table. A call site beyond this is counted in `sites_dropped`, not recorded.
pub const MAX_SITES: usize = 256;

static PROBE: AtomicBool = AtomicBool::new(false);
static SITES_DROPPED: AtomicU64 = AtomicU64::new(0);
static TABLE: Mutex<Vec<SiteStats>> = Mutex::new(Vec::new());

thread_local! {
    /// Entries the most recent `with_*` accessor hold on THIS thread reported. For tests.
    static LAST_HOLD_ENTRIES: Cell<u64> = const { Cell::new(0) };
    /// Shard-table guards THIS thread holds right now (read or write).
    static DEPTH: Cell<u32> = const { Cell::new(0) };
    /// Block-store mutex acquisitions by the instrumented store readers made while a shard-table
    /// guard was held on this thread. For the lock-order guard.
    static STORE_READS_UNDER_GUARD: Cell<u64> = const { Cell::new(0) };
}

/// Called by the block store's scalar readers (`stats`, `slab_summary`) as they take the store
/// mutex. Counts the call when a shard-table guard is held on this thread: the nesting
/// (shard lock, then store mutex) that makes a scrape stall every writer behind a disk-adjacent lock.
pub fn note_store_read() {
    if DEPTH.with(|d| d.get()) > 0 {
        STORE_READS_UNDER_GUARD.with(|c| c.set(c.get() + 1));
    }
}

/// Store-mutex reads made under a shard-table guard on this thread since the last reset.
pub fn store_reads_under_guard() -> u64 {
    STORE_READS_UNDER_GUARD.with(|c| c.get())
}

pub fn reset_store_reads_under_guard() {
    STORE_READS_UNDER_GUARD.with(|c| c.set(0));
}

fn enter() {
    DEPTH.with(|d| d.set(d.get() + 1));
}

fn leave() {
    DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
}

/// Turn the per-call-site hold histogram on or off. Off by default.
pub fn set_hold_probe(on: bool) {
    PROBE.store(on, Ordering::Relaxed);
}

/// Whether the hold probe is recording.
pub fn hold_probe_on() -> bool {
    PROBE.load(Ordering::Relaxed)
}

/// Entries the last `with_shard` / `with_all_shards_read` hold on this thread said it visited.
pub fn last_hold_entries() -> u64 {
    LAST_HOLD_ENTRIES.with(|cell| cell.get())
}

/// Forget every recorded hold.
pub fn reset_holds() {
    TABLE.lock().unwrap_or_else(PoisonError::into_inner).clear();
    SITES_DROPPED.store(0, Ordering::Relaxed);
}

/// A call site that took the lock, and what its holds looked like.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLockHold {
    pub file: &'static str,
    pub line: u32,
    pub write: bool,
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
    /// Upper bound of the power-of-two bucket the median / 99th percentile hold fell in.
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub entries_total: u64,
    pub entries_max: u64,
}

/// Every recorded site, longest max hold first, and how many sites did not fit the table.
pub fn hold_report() -> (Vec<ShardLockHold>, u64) {
    let table = TABLE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut report: Vec<ShardLockHold> = table
        .iter()
        .map(|site| ShardLockHold {
            file: site.file,
            line: site.line,
            write: site.write,
            count: site.count,
            total_ns: site.total_ns,
            max_ns: site.max_ns,
            p50_ns: site.percentile_ns(50),
            p99_ns: site.percentile_ns(99),
            entries_total: site.entries_total,
            entries_max: site.entries_max,
        })
        .collect();
    report.sort_by(|a, b| b.max_ns.cmp(&a.max_ns));
    (report, SITES_DROPPED.load(Ordering::Relaxed))
}

struct SiteStats {
    file: &'static str,
    line: u32,
    write: bool,
    count: u64,
    total_ns: u64,
    max_ns: u64,
    entries_total: u64,
    entries_max: u64,
    /// `buckets[i]` counts holds with `2^(i-1) <= ns < 2^i` (bucket 0 is exactly 0 ns).
    buckets: [u64; 65],
}

impl SiteStats {
    fn percentile_ns(&self, percent: u64) -> u64 {
        let target = (self.count.saturating_mul(percent) + 99) / 100;
        let mut seen = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= target.max(1) {
                return if index == 0 { 0 } else if index >= 64 { u64::MAX } else { 1u64 << index };
            }
        }
        self.max_ns
    }
}

fn record_ns(site: &'static Location<'static>, write: bool, ns: u64, entries: Option<u64>) {
    let mut table = TABLE.lock().unwrap_or_else(PoisonError::into_inner);
    let index = match table
        .iter()
        .position(|s| s.line == site.line() && s.write == write && s.file == site.file())
    {
        Some(index) => index,
        None => {
            if table.len() >= MAX_SITES {
                SITES_DROPPED.fetch_add(1, Ordering::Relaxed);
                return;
            }
            table.push(SiteStats {
                file: site.file(),
                line: site.line(),
                write,
                count: 0,
                total_ns: 0,
                max_ns: 0,
                entries_total: 0,
                entries_max: 0,
                buckets: [0; 65],
            });
            table.len() - 1
        }
    };
    let stats = &mut table[index];
    stats.count += 1;
    stats.total_ns = stats.total_ns.saturating_add(ns);
    stats.max_ns = stats.max_ns.max(ns);
    stats.buckets[(64 - ns.leading_zeros()) as usize] += 1;
    if let Some(entries) = entries {
        stats.entries_total = stats.entries_total.saturating_add(entries);
        stats.entries_max = stats.entries_max.max(entries);
    }
}

/// How many entries one guarded closure touched. The closure adds; the accessor records.
#[derive(Debug, Default)]
pub struct EntriesVisited(Cell<u64>);

impl EntriesVisited {
    pub fn add(&self, entries: u64) {
        self.0.set(self.0.get().saturating_add(entries));
    }

    pub fn get(&self) -> u64 {
        self.0.get()
    }
}

use super::shard_state_registry::{ShardStateHandle, ShardStateRegistry};

pub(super) type ShardHandle = ShardStateHandle<ShardState>;

pub(super) struct ShardsLock {
    owners: ShardStateRegistry<ShardState>,
}

struct OwnerObservation {
    site: &'static Location<'static>,
    write: bool,
    enabled: bool,
    acquired: Cell<Option<Instant>>,
    released: Cell<Option<Instant>>,
    entries: Cell<Option<u64>>,
}

impl OwnerObservation {
    fn new(site: &'static Location<'static>, write: bool) -> Self {
        Self {
            site, write, enabled: hold_probe_on(),
            acquired: Cell::new(None), released: Cell::new(None), entries: Cell::new(None),
        }
    }

    fn enter(&self) -> ActiveOwner<'_> {
        enter();
        if self.write {
            super::shard_write_guard::entered();
        } else {
            super::shard_write_guard::entered_read();
        }
        if self.enabled {
            self.acquired.set(Some(Instant::now()));
        }
        ActiveOwner(self)
    }

    fn note_entries(&self, entries: u64) {
        LAST_HOLD_ENTRIES.with(|cell| cell.set(entries));
        self.entries.set(Some(entries));
    }
}

struct ActiveOwner<'a>(&'a OwnerObservation);

impl Drop for ActiveOwner<'_> {
    fn drop(&mut self) {
        if self.0.enabled {
            self.0.released.set(Some(Instant::now()));
        }
        if self.0.write {
            super::shard_write_guard::left();
        } else {
            super::shard_write_guard::left_read();
        }
        leave();
    }
}

impl Drop for OwnerObservation {
    fn drop(&mut self) {
        if let (Some(begin), Some(end)) = (self.acquired.get(), self.released.get()) {
            let ns = end.duration_since(begin).as_nanos().min(u64::MAX as u128) as u64;
            // This observer outlives the registry call, including its local guard.
            record_ns(self.site, self.write, ns, self.entries.get());
        }
    }
}

impl ShardsLock {
    pub(crate) fn new(map: HashMap<ShardId, ShardState>) -> Self {
        let owners = ShardStateRegistry::default();
        for (id, state) in map {
            owners.replace(id, state);
        }
        Self { owners }
    }

    pub(crate) fn handle(&self, shard_id: ShardId) -> Option<ShardHandle> {
        self.owners.handle(shard_id)
    }

    pub(crate) fn shard_ids(&self) -> Vec<ShardId> {
        self.owners.shard_ids()
    }

    pub(crate) fn install(&self, shard_id: ShardId, state: ShardState) -> Option<ShardState> {
        self.owners.replace(shard_id, state)
    }

    pub(crate) fn retire_if_current(&self, handle: &ShardHandle) -> Option<ShardState> {
        self.owners.retire_if_current(handle)
    }

    #[track_caller]
    pub(crate) fn with_handle_state<R>(
        &self,
        handle: &ShardHandle,
        read: impl FnOnce(Option<&ShardState>) -> R,
    ) -> R {
        let observation = OwnerObservation::new(Location::caller(), false);
        handle.with_read(|state| {
            let _held = state.is_some().then(|| observation.enter());
            read(state)
        })
    }

    #[track_caller]
    pub(crate) fn with_handle_state_mut<R>(
        &self,
        handle: &ShardHandle,
        write: impl FnOnce(Option<&mut ShardState>) -> R,
    ) -> R {
        let observation = OwnerObservation::new(Location::caller(), true);
        handle.with_write(|state| {
            let _held = state.is_some().then(|| observation.enter());
            write(state)
        })
    }

    #[track_caller]
    pub(crate) fn with_state<R>(
        &self,
        shard_id: ShardId,
        read: impl FnOnce(Option<&ShardState>) -> R,
    ) -> R {
        let observation = OwnerObservation::new(Location::caller(), false);
        self.owners.with_shard(shard_id, |state| {
            let _held = state.is_some().then(|| observation.enter());
            read(state)
        })
    }

    #[track_caller]
    pub(crate) fn with_state_mut<R>(
        &self,
        shard_id: ShardId,
        write: impl FnOnce(Option<&mut ShardState>) -> R,
    ) -> R {
        let observation = OwnerObservation::new(Location::caller(), true);
        self.owners.with_shard_mut(shard_id, |state| {
            let _held = state.is_some().then(|| observation.enter());
            write(state)
        })
    }

    #[track_caller]
    pub(crate) fn with_all_states<R>(
        &self,
        read: impl FnOnce(&BTreeMap<ShardId, &ShardState>) -> R,
    ) -> R {
        let observation = OwnerObservation::new(Location::caller(), false);
        self.owners.with_all_shards_read(|states| {
            let _held = observation.enter();
            read(states)
        })
    }

    #[track_caller]
    pub(crate) fn with_all_states_mut<R>(
        &self,
        write: impl FnOnce(&mut BTreeMap<ShardId, &mut ShardState>) -> R,
    ) -> R {
        let observation = OwnerObservation::new(Location::caller(), true);
        self.owners.with_all_shards_mut(|states| {
            let _held = observation.enter();
            write(states)
        })
    }

    #[track_caller]
    pub(crate) fn with_shard<R>(
        &self,
        shard_id: ShardId,
        read: impl FnOnce(Option<&ShardState>, &EntriesVisited) -> R,
    ) -> R {
        let observation = OwnerObservation::new(Location::caller(), false);
        self.owners.with_shard(shard_id, |state| {
            let _held = state.is_some().then(|| observation.enter());
            let visited = EntriesVisited::default();
            let result = read(state, &visited);
            observation.note_entries(visited.get());
            result
        })
    }

    #[track_caller]
    pub(crate) fn with_all_shards_read<R>(
        &self,
        read: impl FnOnce(&BTreeMap<ShardId, &ShardState>, &EntriesVisited) -> R,
    ) -> R {
        let observation = OwnerObservation::new(Location::caller(), false);
        self.owners.with_all_shards_read(|states| {
            let _held = observation.enter();
            let visited = EntriesVisited::default();
            let result = read(states, &visited);
            observation.note_entries(visited.get());
            result
        })
    }
}

impl Default for ShardsLock {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

impl std::fmt::Debug for ShardsLock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ShardsLock").field("ownership", &"per-shard").finish_non_exhaustive()
    }
}
