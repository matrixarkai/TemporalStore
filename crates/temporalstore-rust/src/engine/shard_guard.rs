// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SHARD TABLE'S LOCK, WITH EVERY HOLD ATTRIBUTED TO THE LINE THAT TOOK IT.
//!
//! `TemporalEngine.shards` is one engine-wide `RwLock`, and a guard on it stalls every writer for as
//! long as it is held. Before this module only the nine sites that used a "marked" guard could be
//! seen by the lock-hold counters; the other seventy-odd were invisible, so "which site is the
//! longest hold" had no answer that did not start with reading the source.
//!
//! `ShardsLock` has the same `read()`/`write()` as `RwLock` (and the same poisoning behaviour), so no
//! call site changes. Each call is `#[track_caller]`, and when the probe is ON the guard's `Drop`
//! adds the hold time to a per-call-site histogram. The probe is OFF by default: a guard then pays one
//! relaxed load and no clock read.
//!
//! The accessors `with_shard` and `with_all_shards_read` are the seam the later stages convert call
//! sites onto. Their closure receives an [`EntriesVisited`] handle and says how many entries it
//! touched; that count is recorded per hold, so a guard can state a bound on ENTRIES (flat from 1k
//! to 64k keys) instead of on time, which is a flaky thing to assert.
//!
//! rust-internal: measures this crate's own lock holds, no product behaviour

use super::*;

use std::cell::Cell;
use std::panic::Location;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{LockResult, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
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

fn record(site: &'static Location<'static>, write: bool, started: Instant, entries: Option<u64>) {
    let ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
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

/// The shard table. Same `read()`/`write()` as `RwLock`, plus per-call-site hold attribution.
pub(crate) struct ShardsLock {
    inner: RwLock<HashMap<ShardId, ShardState>>,
}

impl ShardsLock {
    pub(crate) fn new(map: HashMap<ShardId, ShardState>) -> Self {
        Self { inner: RwLock::new(map) }
    }

    #[track_caller]
    pub(crate) fn read(&self) -> LockResult<ShardsReadGuard<'_>> {
        let site = Location::caller();
        match self.inner.read() {
            Ok(guard) => Ok(ShardsReadGuard::new(guard, site)),
            Err(poisoned) => Err(PoisonError::new(ShardsReadGuard::new(poisoned.into_inner(), site))),
        }
    }

    #[track_caller]
    pub(crate) fn write(&self) -> LockResult<ShardsWriteGuard<'_>> {
        let site = Location::caller();
        match self.inner.write() {
            Ok(guard) => Ok(ShardsWriteGuard::new(guard, site)),
            Err(poisoned) => Err(PoisonError::new(ShardsWriteGuard::new(poisoned.into_inner(), site))),
        }
    }

    /// Run `f` on one shard under the shared guard. `f` reports the entries it touched.
    #[track_caller]
    pub(crate) fn with_shard<R>(
        &self,
        shard_id: ShardId,
        f: impl FnOnce(Option<&ShardState>, &EntriesVisited) -> R,
    ) -> R {
        let guard = self.read().expect("engine lock poisoned");
        let visited = EntriesVisited::default();
        let result = f(guard.get(&shard_id), &visited);
        guard.note_entries(visited.get());
        result
    }

    /// Run `f` over every shard under the shared guard. `f` reports the entries it touched.
    #[track_caller]
    pub(crate) fn with_all_shards_read<R>(
        &self,
        f: impl FnOnce(&HashMap<ShardId, ShardState>, &EntriesVisited) -> R,
    ) -> R {
        let guard = self.read().expect("engine lock poisoned");
        let visited = EntriesVisited::default();
        let result = f(&guard, &visited);
        guard.note_entries(visited.get());
        result
    }
}

pub(crate) struct ShardsReadGuard<'a> {
    guard: RwLockReadGuard<'a, HashMap<ShardId, ShardState>>,
    site: &'static Location<'static>,
    started: Option<Instant>,
    entries: Cell<Option<u64>>,
}

impl<'a> ShardsReadGuard<'a> {
    fn new(guard: RwLockReadGuard<'a, HashMap<ShardId, ShardState>>, site: &'static Location<'static>) -> Self {
        enter();
        let started = hold_probe_on().then(Instant::now);
        Self { guard, site, started, entries: Cell::new(None) }
    }

    fn note_entries(&self, entries: u64) {
        LAST_HOLD_ENTRIES.with(|cell| cell.set(entries));
        self.entries.set(Some(entries));
    }
}

impl std::ops::Deref for ShardsReadGuard<'_> {
    type Target = HashMap<ShardId, ShardState>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl Drop for ShardsReadGuard<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            record(self.site, false, started, self.entries.get());
        }
        leave();
    }
}

pub(crate) struct ShardsWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, HashMap<ShardId, ShardState>>,
    site: &'static Location<'static>,
    started: Option<Instant>,
}

impl<'a> ShardsWriteGuard<'a> {
    fn new(guard: RwLockWriteGuard<'a, HashMap<ShardId, ShardState>>, site: &'static Location<'static>) -> Self {
        enter();
        let started = hold_probe_on().then(Instant::now);
        Self { guard, site, started }
    }
}

impl std::ops::Deref for ShardsWriteGuard<'_> {
    type Target = HashMap<ShardId, ShardState>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for ShardsWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for ShardsWriteGuard<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            record(self.site, true, started, None);
        }
        leave();
    }
}

impl Default for ShardsLock {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}
