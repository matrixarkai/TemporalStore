// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cell::{Cell, RefCell};
use std::ops::{Deref, DerefMut};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};
use std::time::Instant;

/// Physical lock sites, separate from the existing callback hold probe.
#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardLockSite {
    RegistryLookup,
    RegistryLifecycle,
    OwnerAccess,
    OwnerLifecycle,
    CoherentRegistry,
    CoherentTry,
    CoherentWait,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardLockTimes {
    pub acquired: u64,
    pub would_block: u64,
    /// Bucket zero counts 0 ns; bucket i counts 2^(i-1)..2^i ns, exclusive at the upper end.
    pub wait_buckets: [u64; 65],
    pub hold_buckets: [u64; 65],
    pub wait_total_ns: u64,
    pub hold_total_ns: u64,
    pub wait_max_ns: u64,
    pub hold_max_ns: u64,
}

impl ShardLockTimes {
    const EMPTY: Self = Self {
        acquired: 0,
        would_block: 0,
        wait_buckets: [0; 65],
        hold_buckets: [0; 65],
        wait_total_ns: 0,
        hold_total_ns: 0,
        wait_max_ns: 0,
        hold_max_ns: 0,
    };
}

/// One thread's measurements. Enable separately on each worker being measured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardLockDiagnostics {
    sites: [[ShardLockTimes; 2]; ShardLockSite::CoherentWait as usize + 1],
    active_guards: u64,
    pub busy_retries: u64,
    pub membership_retries: u64,
}

impl ShardLockDiagnostics {
    const EMPTY: Self = Self {
        sites: [const { [const { ShardLockTimes::EMPTY }; 2] }; ShardLockSite::CoherentWait as usize + 1],
        active_guards: 0,
        busy_retries: 0,
        membership_retries: 0,
    };

    pub fn times(&self, site: ShardLockSite, write: bool) -> &ShardLockTimes {
        &self.sites[site as usize][usize::from(write)]
    }
}

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static REPORT: RefCell<Option<Box<ShardLockDiagnostics>>> = const { RefCell::new(None) };
}

/// Measure physical acquisitions and guard lifetimes on the calling thread for one operation.
/// This scope is independent of the callback hold probe. Nested scopes on one thread are invalid.
/// Start the scope outside the operation's locks. Registry guards stay inside the registry.
/// The scope rejects an operation if a measured guard is still alive when it returns.
pub fn measure_shard_locks<R>(run: impl FnOnce() -> R) -> (R, ShardLockDiagnostics) {
    assert!(!ENABLED.with(Cell::get), "nested shard lock diagnostics");
    REPORT.with(|report| *report.borrow_mut() = Some(Box::new(ShardLockDiagnostics::EMPTY)));
    ENABLED.with(|enabled| enabled.set(true));
    struct Disable;
    impl Drop for Disable {
        fn drop(&mut self) {
            ENABLED.with(|enabled| enabled.set(false));
            REPORT.with(|report| drop(report.borrow_mut().take()));
        }
    }
    let scope = Disable;
    let value = run();
    let report = REPORT.with(|report| {
        let mut report = report.borrow_mut();
        assert_eq!(report.as_ref().expect("active diagnostics").active_guards, 0,
            "measured shard lock escaped its diagnostics scope");
        report.take().expect("active diagnostics")
    });
    drop(scope);
    (value, *report)
}

#[inline]
fn start() -> Option<Instant> {
    ENABLED.with(Cell::get).then(Instant::now)
}

fn record(site: ShardLockSite, write: bool, acquired: bool, update: impl FnOnce(&mut ShardLockTimes)) {
    REPORT.with(|report| {
        if let Some(report) = report.borrow_mut().as_mut() {
            if acquired {
                report.active_guards += 1;
            } else {
                report.active_guards -= 1;
            }
            update(&mut report.sites[site as usize][usize::from(write)]);
        }
    });
}

fn ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn bucket(ns: u64) -> usize {
    (u64::BITS - ns.leading_zeros()) as usize
}

pub(super) fn retry(busy: bool) {
    if ENABLED.with(Cell::get) {
        REPORT.with(|report| {
            let mut report = report.borrow_mut();
            let report = report.as_mut().expect("active diagnostics");
            let count = if busy { &mut report.busy_retries } else { &mut report.membership_retries };
            *count = count.saturating_add(1);
        });
    }
}

pub(super) struct Observed<G> {
    guard: Option<G>,
    timing: Option<(Instant, ShardLockSite, bool)>,
}

impl<G> Observed<G> {
    fn new(guard: G, attempt: Option<Instant>, site: ShardLockSite, write: bool) -> Self {
        let timing = attempt.map(|attempt| {
            let acquired = Instant::now();
            let elapsed = ns(acquired.duration_since(attempt));
            record(site, write, true, |times| {
                times.acquired = times.acquired.saturating_add(1);
                times.wait_total_ns = times.wait_total_ns.saturating_add(elapsed);
                times.wait_max_ns = times.wait_max_ns.max(elapsed);
                let count = &mut times.wait_buckets[bucket(elapsed)];
                *count = count.saturating_add(1);
            });
            (acquired, site, write)
        });
        Self { guard: Some(guard), timing }
    }
}

impl<G: Deref> Deref for Observed<G> {
    type Target = G::Target;

    fn deref(&self) -> &Self::Target {
        self.guard.as_deref().expect("live lock guard")
    }
}

impl<G: DerefMut> DerefMut for Observed<G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_deref_mut().expect("live lock guard")
    }
}

impl<G> Drop for Observed<G> {
    fn drop(&mut self) {
        drop(self.guard.take());
        if let Some((acquired, site, write)) = self.timing {
            let elapsed = ns(acquired.elapsed());
            record(site, write, false, |times| {
                times.hold_total_ns = times.hold_total_ns.saturating_add(elapsed);
                times.hold_max_ns = times.hold_max_ns.max(elapsed);
                let count = &mut times.hold_buckets[bucket(elapsed)];
                *count = count.saturating_add(1);
            });
        }
    }
}

#[inline]
pub(super) fn read<T>(lock: &RwLock<T>, site: ShardLockSite) -> Observed<RwLockReadGuard<'_, T>> {
    let attempt = start();
    let guard = lock.read().expect(poison_message(site));
    Observed::new(guard, attempt, site, false)
}

#[inline]
pub(super) fn write<T>(lock: &RwLock<T>, site: ShardLockSite) -> Observed<RwLockWriteGuard<'_, T>> {
    let attempt = start();
    let guard = lock.write().expect(poison_message(site));
    Observed::new(guard, attempt, site, true)
}

fn poison_message(site: ShardLockSite) -> &'static str {
    match site {
        ShardLockSite::RegistryLookup | ShardLockSite::RegistryLifecycle | ShardLockSite::CoherentRegistry =>
            "shard registry lock poisoned",
        _ => "shard state lock poisoned",
    }
}

pub(super) fn try_read<T>(lock: &RwLock<T>) -> Option<Observed<RwLockReadGuard<'_, T>>> {
    let attempt = start();
    match lock.try_read() {
        Ok(guard) => Some(Observed::new(guard, attempt, ShardLockSite::CoherentTry, false)),
        Err(TryLockError::WouldBlock) => {
            would_block(attempt, false);
            None
        }
        Err(TryLockError::Poisoned(_)) => panic!("shard state lock poisoned"),
    }
}

pub(super) fn try_write<T>(lock: &RwLock<T>) -> Option<Observed<RwLockWriteGuard<'_, T>>> {
    let attempt = start();
    match lock.try_write() {
        Ok(guard) => Some(Observed::new(guard, attempt, ShardLockSite::CoherentTry, true)),
        Err(TryLockError::WouldBlock) => {
            would_block(attempt, true);
            None
        }
        Err(TryLockError::Poisoned(_)) => panic!("shard state lock poisoned"),
    }
}

fn would_block(attempt: Option<Instant>, write: bool) {
    if attempt.is_some() {
        REPORT.with(|report| {
            let mut report = report.borrow_mut();
            let report = report.as_mut().expect("active diagnostics");
            let times = &mut report.sites[ShardLockSite::CoherentTry as usize][usize::from(write)];
            times.would_block = times.would_block.saturating_add(1);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_diagnostics_take_no_timestamp_or_report_buffer() {
        let lock = RwLock::new(7);
        assert!(start().is_none());
        let guard = read(&lock, ShardLockSite::OwnerAccess);
        assert!(guard.timing.is_none());
        assert_eq!(*guard, 7);
        drop(guard);
        assert!(REPORT.with(|report| report.borrow().is_none()));
    }

    #[test]
    fn diagnostic_scope_resets_after_panic_and_between_measurements() {
        let lock = RwLock::new(7);
        let aborted = std::panic::catch_unwind(|| measure_shard_locks(|| {
            let _guard = read(&lock, ShardLockSite::OwnerAccess);
            panic!("interrupt measured callback");
        }));
        assert!(aborted.is_err());
        assert!(start().is_none());
        assert!(REPORT.with(|report| report.borrow().is_none()));
        let (value, first) = measure_shard_locks(|| *read(&lock, ShardLockSite::OwnerAccess));
        assert_eq!(value, 7);
        assert_eq!(first.times(ShardLockSite::OwnerAccess, false).acquired, 1);
        let (_, empty) = measure_shard_locks(|| ());
        assert_eq!(empty, ShardLockDiagnostics::EMPTY);
        let (_, second) = measure_shard_locks(|| {
            *write(&lock, ShardLockSite::OwnerAccess) = 8;
        });
        assert_eq!(second.times(ShardLockSite::OwnerAccess, false).acquired, 0);
        assert_eq!(second.times(ShardLockSite::OwnerAccess, true).acquired, 1);
    }

    #[test]
    fn diagnostic_scope_rejects_an_escaping_guard() {
        let lock = RwLock::new(7);
        let escaped = std::panic::catch_unwind(|| {
            measure_shard_locks(|| read(&lock, ShardLockSite::OwnerAccess))
        });
        let panic = escaped.err().expect("guard escaped its measured scope");
        assert!(panic.downcast_ref::<String>().is_some_and(|message| {
            message.contains("measured shard lock escaped its diagnostics scope")
        }));
        assert!(lock.try_write().is_ok());
        assert!(REPORT.with(|report| report.borrow().is_none()));
    }
}
