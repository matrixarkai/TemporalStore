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

/// The allocation counters, or `None` when the counting allocator is not installed.
///
/// `Probe` and the statics behind it are for a `#[test]` that is itself gated on `alloc-probe`,
/// and `every_counting_allocator_probe_is_gated_on_the_feature_that_installs_it` enforces exactly
/// that -- because a test reading them without the feature measures a process where they never
/// move and reports a table of zeros.
///
/// A probe that lives at a PRODUCTION call site and is read by tests on both sides of the feature
/// cannot take that shape: it has no enclosing `#[test]` to gate. What it needs instead is to be
/// able to tell "this span allocated nothing" from "nothing was counting", and reading the
/// statics directly cannot -- both answer zero. This says which, so the caller can record it
/// alongside its numbers and a reader is never shown a zero that means the opposite.
///
/// `restore_phase_probe` in `engine/lifecycle.rs` is the caller this exists for.
pub fn counted_now() -> Option<(u64, u64)> {
    #[cfg(feature = "alloc-probe")]
    {
        Some((
            ALLOC_CALLS.load(Ordering::Relaxed),
            ALLOC_BYTES.load(Ordering::Relaxed),
        ))
    }
    #[cfg(not(feature = "alloc-probe"))]
    {
        None
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
// The counting allocator is installed only under `alloc-probe`, so a test that reads these counters
// without that feature measures a process where they never move. Twenty-eight such tests carried
// `#[cfg(feature = "alloc-probe")]` and thirty-one did not: the thirty-one compiled into every
// build, appeared in `--ignored --list`, and twenty-nine of them aborted on their first line with
// "the counting allocator is not installed". The other two printed a table of zeros instead, which
// is the same defect with the alarm taken out. Running the ignored set on a default build therefore
// produced failures that meant nothing, which teaches everyone to discount that run.
//
// Same class of probe, so: same gate. This asserts it stays that way.
#[cfg(test)]
mod counting_allocator_gate {
    use std::path::Path;

    const GATE: &str = "#[cfg(feature = \"alloc-probe\")]";
    const MARKERS: [&str; 6] = [
        "alloc_probe::Probe::start",
        "alloc_probe::ALLOC_CALLS",
        "alloc_probe::ALLOC_BYTES",
        "alloc_probe::FREE_CALLS",
        "alloc_probe::FREE_BYTES",
        "alloc_probe::AllocCounts",
    ];

    /// Every test that reads the counting allocator carries the feature gate that installs it.
    #[test]
    fn every_counting_allocator_probe_is_gated_on_the_feature_that_installs_it() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut pending = vec![root];
        let mut files_read = 0_usize;
        let mut excised = 0_usize;
        let mut probe_tests = 0_usize;
        let mut violations: Vec<String> = Vec::new();

        while let Some(path) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    pending.push(entry_path);
                    continue;
                }
                if entry_path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                // This file holds the marker strings themselves, so it is taken out of its own
                // haystack -- and the removal is asserted below. If this module is ever moved,
                // the scan reads its own literals, finds them under an ungated `#[test]`, and
                // FAILS. A guard that stops excluding itself should get louder, not quieter.
                if entry_path.file_name().and_then(|n| n.to_str()) == Some("alloc_probe.rs") {
                    excised += 1;
                    continue;
                }
                let Ok(text) = std::fs::read_to_string(&entry_path) else {
                    continue;
                };
                files_read += 1;
                let lines: Vec<&str> = text.lines().collect();
                let mut anchors_seen: Vec<usize> = Vec::new();
                for (index, line) in lines.iter().enumerate() {
                    if !MARKERS.iter().any(|marker| line.contains(marker)) {
                        continue;
                    }
                    // Walk BACK to the enclosing `#[test]`. Deliberately not brace counting: a
                    // `{` inside a string literal drifts a depth counter, and this crate writes
                    // plenty of them. A first pass at this guard did count braces, saw 23 of the
                    // 82 probe tests, and named two functions that do not touch the probe at all.
                    let mut anchor = index as isize;
                    while anchor >= 0 && lines[anchor as usize].trim() != "#[test]" {
                        anchor -= 1;
                    }
                    if anchor < 0 {
                        violations.push(format!(
                            "{}:{}: the counting allocator is read outside any test",
                            entry_path.display(),
                            index + 1
                        ));
                        continue;
                    }
                    let anchor = anchor as usize;
                    if anchors_seen.contains(&anchor) {
                        continue;
                    }
                    anchors_seen.push(anchor);
                    probe_tests += 1;
                    let mut top = anchor;
                    while top > 0 && lines[top - 1].trim().starts_with("#[") {
                        top -= 1;
                    }
                    let mut bottom = anchor;
                    while bottom + 1 < lines.len() && lines[bottom + 1].trim().starts_with("#[") {
                        bottom += 1;
                    }
                    if !lines[top..=bottom].iter().any(|line| line.trim() == GATE) {
                        violations.push(format!(
                            "{}:{}: reads the counting allocator and is not gated on \
                             `alloc-probe`, so it compiles into every build and measures a \
                             process where the counters never move",
                            entry_path.display(),
                            anchor + 1
                        ));
                    }
                }
            }
        }

        // Denominators first. Every assertion below is about a set, and a set that came back empty
        // satisfies all of them.
        assert!(
            files_read > 100,
            "the source walk found {files_read} files; this guard would pass over nothing"
        );
        assert_eq!(
            1, excised,
            "expected to take exactly one file out of the scan (this one); took {excised}"
        );
        assert!(
            probe_tests >= 60,
            "found only {probe_tests} tests reading the counting allocator across {files_read} \
             files; there were 82 when this was written, so the scan has broken rather than the \
             tests having gone away"
        );
        assert!(
            violations.is_empty(),
            "{} of {probe_tests} counting-allocator tests are not gated on the feature that \
             installs the allocator:\n{}",
            violations.len(),
            violations.join("\n")
        );
    }
}
