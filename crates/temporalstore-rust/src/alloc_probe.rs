// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Counting allocator, tests only.
//!
//! Process RSS conflates three different things: memory allocated and still held, memory freed but
//! retained by the allocator, and memory that never came from the heap at all. Measured on the
//! proxy, 71% of RSS was allocator retention rather than live data -- so an RSS delta cannot say
//! whether a change reduced what a request holds, and a change that removes real allocations can
//! show up as nothing at all.
//!
//! This counts the calls and the bytes directly, which is what "this path allocates N times per
//! candidate" actually means. Counts do not move with machine load either, which matters on a box
//! that sits between load 5 and 30 for hours.
//!
//! Wired under `cfg(test)` only: a global allocator wrapper adds two atomic increments to every
//! allocation in the process, which is not something to put on a serving path.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

pub static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
pub static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
pub static FREE_CALLS: AtomicU64 = AtomicU64::new(0);
pub static FREE_BYTES: AtomicU64 = AtomicU64::new(0);

pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREE_CALLS.fetch_add(1, Ordering::Relaxed);
        FREE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }

    // realloc and alloc_zeroed have default implementations in terms of alloc/dealloc, but the
    // default realloc copies; forwarding to System::realloc keeps growth cheap and still counts the
    // net change, which is what a Vec push storm actually costs.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        if new_size > layout.size() {
            ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
        }
        System.realloc(ptr, layout, new_size)
    }
}

/// What happened between `Probe::start()` and `Probe::stop()`.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocCounts {
    pub allocs: u64,
    pub alloc_bytes: u64,
    pub frees: u64,
    pub free_bytes: u64,
}

impl AllocCounts {
    /// Allocations that were still outstanding when the probe stopped.
    ///
    /// This is a difference of counts, not a live-heap measurement: a path that frees everything it
    /// takes reports zero here while still having done the work, which is why `allocs` is reported
    /// alongside and is usually the number that matters for latency.
    pub fn outstanding(&self) -> i64 {
        self.allocs as i64 - self.frees as i64
    }

    pub fn per(&self, n: usize) -> f64 {
        if n == 0 {
            0.0
        } else {
            self.allocs as f64 / n as f64
        }
    }
}

/// Span counter. Single-threaded use only: the counters are process-global, so a probe running
/// while another thread allocates attributes that thread's work to this span.
pub struct Probe {
    allocs: u64,
    alloc_bytes: u64,
    frees: u64,
    free_bytes: u64,
}

impl Probe {
    pub fn start() -> Self {
        Probe {
            allocs: ALLOC_CALLS.load(Ordering::Relaxed),
            alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
            frees: FREE_CALLS.load(Ordering::Relaxed),
            free_bytes: FREE_BYTES.load(Ordering::Relaxed),
        }
    }

    pub fn stop(&self) -> AllocCounts {
        AllocCounts {
            allocs: ALLOC_CALLS.load(Ordering::Relaxed).saturating_sub(self.allocs),
            alloc_bytes: ALLOC_BYTES
                .load(Ordering::Relaxed)
                .saturating_sub(self.alloc_bytes),
            frees: FREE_CALLS.load(Ordering::Relaxed).saturating_sub(self.frees),
            free_bytes: FREE_BYTES
                .load(Ordering::Relaxed)
                .saturating_sub(self.free_bytes),
        }
    }
}

// These assert the counters MOVE, which they only do when this module is actually installed as the
// global allocator -- and it is installed only under the `alloc-probe` feature. Without that gate
// they fail in every ordinary `cargo test`, asserting a property the build deliberately does not
// have.
#[cfg(all(test, feature = "alloc-probe"))]
mod tests {
    use super::*;

    #[test]
    fn the_probe_counts_an_allocation_it_can_see() {
        // A probe that reports zero for a known allocation is worse than no probe: it reads as
        // "this path does not allocate". Prove it moves on something unmistakable before trusting
        // it on something subtle.
        let probe = Probe::start();
        let v: Vec<u8> = Vec::with_capacity(4096);
        let counts = probe.stop();
        assert!(
            counts.allocs >= 1,
            "a 4 KB Vec must register at least one allocation, saw {}",
            counts.allocs
        );
        assert!(
            counts.alloc_bytes >= 4096,
            "expected at least the 4096 bytes asked for, saw {}",
            counts.alloc_bytes
        );
        drop(v);
    }

    #[test]
    fn a_span_that_frees_what_it_takes_still_reports_the_work() {
        let probe = Probe::start();
        for _ in 0..64 {
            let v: Vec<u8> = Vec::with_capacity(1024);
            drop(v);
        }
        let counts = probe.stop();
        assert!(
            counts.allocs >= 64,
            "64 allocate/free pairs must count as 64 allocations, saw {}",
            counts.allocs
        );
        // Outstanding nets out; the work does not. This is the distinction the whole probe exists
        // for, so it is asserted rather than left as a comment.
        assert!(
            counts.outstanding().abs() <= 8,
            "everything taken was given back, so outstanding should be near zero, saw {}",
            counts.outstanding()
        );
    }
}
