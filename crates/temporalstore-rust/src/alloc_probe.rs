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
        charge_class_alloc(layout.size());
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREE_CALLS.fetch_add(1, Ordering::Relaxed);
        FREE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        charge_class_free(layout.size());
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
        // Charged the same way the whole-process counter above is, so the class rows and the span
        // total treat a grow identically and a `Vec` push storm cannot land in one and not the
        // other.
        charge_class_alloc(new_size.saturating_sub(layout.size()));
        System.realloc(ptr, layout, new_size)
    }
}

// ---------------------------------------------------------------------------------------------
// WHAT THE MEMORY WAS SPENT ON
// ---------------------------------------------------------------------------------------------
//
// One undifferentiated total answers "how much" and never "on what". At four thousand records
// "6.0 allocations per record" is a summary; at a large corpus the same number describes a store
// whose index is flat per record and one whose index is outgrowing the store, and those two want
// opposite fixes. The counters below split the same allocations by the sink they land in.
//
// THE CLASSES WERE MEASURED, NOT CHOSEN. They are the nine sinks a value write was measured to
// land in on this engine, each with a primitive that owns it; the run and the full decomposition
// are in `engine::tests::alloc_class_scale`. Candidates that did not survive the measurement are
// named there too, with the reason, so the set is a finding rather than a taxonomy.
//
// WHERE THE CHARGE SITS. Inside the primitive that owns the sink, never at its call sites. Six
// counters in this crate have been declared and then seen a fraction of their call sites or none
// of them; the two most recent corrections (#1882, #1899) both moved the charge into the callee
// for exactly this reason, and the exploratory pass behind this module hit the same wall from the
// other side -- a scope placed at the single-command call site of `execute_on_shard` read ZERO on
// a batch ingest, because the batch path is a second live caller of the same function.
//
// What placing the charge in the callee does NOT cover is a NEW primitive that allocates into a
// sink without going through the existing one. `every_alloc_class_has_a_production_scope` fails on
// a class nothing enters; nothing can fail on a sink nobody has written yet. The pull request that
// introduced this says so rather than implying the cover is total.

/// What a span of allocation was spent on, in this engine's own terms.
///
/// A PAGE is what a write stores, a SLAB is the file it lands in, a BUCKET is the set of keys a
/// routing bucket owns, and an OUTCOME is what a write puts aside for its log record -- the words
/// this tree already uses for its own parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AllocClass {
    /// The stored value's own bytes: the block-record payload encode, and the copy the in-memory
    /// page holds on the asynchronous arm. The largest class by bytes after the log record.
    PageBytes,
    /// The copy of those same bytes that the write-ahead record CARRIES, so an ack promises the
    /// page and not only the command that produced it. Separate from `PageBytes` because a
    /// durability decision put it there and reclaim is what takes it away again.
    CarriedPage,
    /// Appending that page to a slab, minus the payload itself: the record header, the framing,
    /// the address, and what the block store keeps about where it went.
    SlabAppend,
    /// The per-routing-bucket page index: the bucket node, its page entries, the object-id set,
    /// and the object/component lookup that resolves a key to its pages. This is the row a store
    /// whose index is outgrowing its data shows it in.
    BucketIndex,
    /// The dirty-object index: which keys a later storage round has to sweep, under which bucket.
    /// Index BOOKKEEPING rather than index, and it is reported apart from `BucketIndex` because a
    /// sweep that falls behind grows this one and not that one.
    DirtyObjects,
    /// The index outcome a write stages for its log record -- which object, under which kind, and
    /// where its page landed -- and the buffer those outcomes accumulate in.
    StagedOutcome,
    /// Encoding the write-ahead record itself. Few allocations and the most bytes of any class:
    /// the record is built as whole buffers, so counting calls alone reports it as nearly free.
    LogRecord,
    /// The index-log delta a write appends: the items describing the pages it changed, and the
    /// record they are written in.
    IndexLogDelta,
    /// Invalidating the serving cache's entry for a written key. The largest class by allocation
    /// COUNT and a middling one by bytes, which is the pair of facts either column alone hides.
    CacheInvalidation,
    /// ASKING THE SERVING CACHE FOR AN ANSWER: the lookup itself, inside the three production
    /// primitives that make one. The only class on the READ side, and it was added because the
    /// other nine are all charged inside WRITE primitives, so a serving read reconciled to a
    /// classified share of exactly zero -- every allocation it made landed in the residual and
    /// nothing could say what any of them were for.
    ///
    /// Charged around the cache call and nothing else. The key is built by the caller and the
    /// answer is decoded by it, which is the same boundary `CacheInvalidation` draws around
    /// `invalidate_cache_key`, so the two cache-side classes are measured the same way and can be
    /// read against each other.
    CacheRead,
}

const CLASS_COUNT: usize = 10;

impl AllocClass {
    /// Every class, in slot order. `alloc_class_slots_are_dense_and_in_order` holds that.
    pub const ALL: [AllocClass; CLASS_COUNT] = [
        AllocClass::PageBytes,
        AllocClass::CarriedPage,
        AllocClass::SlabAppend,
        AllocClass::BucketIndex,
        AllocClass::DirtyObjects,
        AllocClass::StagedOutcome,
        AllocClass::LogRecord,
        AllocClass::IndexLogDelta,
        AllocClass::CacheInvalidation,
        AllocClass::CacheRead,
    ];

    /// Which counter row this class owns.
    ///
    /// THIS MATCH IS THE COMPILE ERROR. A class added to the enum without a row here does not
    /// build, and neither does one without a `label`. That is the half of "declared but never
    /// incremented" a compiler can catch; the other half -- declared, given a row, and then never
    /// entered by any production code -- is caught by `every_alloc_class_has_a_production_scope`,
    /// which fails by name on a class no production file enters.
    pub(crate) const fn slot(self) -> usize {
        match self {
            AllocClass::PageBytes => 0,
            AllocClass::CarriedPage => 1,
            AllocClass::SlabAppend => 2,
            AllocClass::BucketIndex => 3,
            AllocClass::DirtyObjects => 4,
            AllocClass::StagedOutcome => 5,
            AllocClass::LogRecord => 6,
            AllocClass::IndexLogDelta => 7,
            AllocClass::CacheInvalidation => 8,
            AllocClass::CacheRead => 9,
        }
    }

    /// The name a report prints.
    pub const fn label(self) -> &'static str {
        match self {
            AllocClass::PageBytes => "page_bytes",
            AllocClass::CarriedPage => "carried_page",
            AllocClass::SlabAppend => "slab_append",
            AllocClass::BucketIndex => "bucket_index",
            AllocClass::DirtyObjects => "dirty_objects",
            AllocClass::StagedOutcome => "staged_outcome",
            AllocClass::LogRecord => "log_record",
            AllocClass::IndexLogDelta => "index_log_delta",
            AllocClass::CacheInvalidation => "cache_invalidation",
            AllocClass::CacheRead => "cache_read",
        }
    }
}

// A class whose slot is past the end of the table would index out of bounds on its first use.
// Held here so the mismatch is a build failure rather than a panic in whichever test ran first.
const _: () = assert!(AllocClass::ALL.len() == CLASS_COUNT);

struct ClassSlot {
    allocs: AtomicU64,
    alloc_bytes: AtomicU64,
    frees: AtomicU64,
    free_bytes: AtomicU64,
}

impl ClassSlot {
    const fn new() -> Self {
        ClassSlot {
            allocs: AtomicU64::new(0),
            alloc_bytes: AtomicU64::new(0),
            frees: AtomicU64::new(0),
            free_bytes: AtomicU64::new(0),
        }
    }
}

static CLASS_SLOTS: [ClassSlot; CLASS_COUNT] = [
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
];

/// No class in scope.
///
/// Deliberately NOT a variant of `AllocClass`: unclassified is a residual that the reconciliation
/// measures against an independent total, never a bucket anything can be charged to on purpose.
const UNCLASSIFIED: u8 = u8::MAX;

thread_local! {
    /// The class the running code is inside, or `UNCLASSIFIED`.
    ///
    /// `const`-initialised, and a `Cell<u8>` has no destructor: the allocator reads this on every
    /// allocation, and a thread-local that allocated to initialise itself would recurse through
    /// the allocator that is reading it.
    static CURRENT_CLASS: std::cell::Cell<u8> = const { std::cell::Cell::new(UNCLASSIFIED) };
}

#[inline]
fn charge_class_alloc(size: usize) {
    let _ = CURRENT_CLASS.try_with(|cell| {
        let slot = cell.get() as usize;
        if slot < CLASS_COUNT {
            CLASS_SLOTS[slot].allocs.fetch_add(1, Ordering::Relaxed);
            CLASS_SLOTS[slot]
                .alloc_bytes
                .fetch_add(size as u64, Ordering::Relaxed);
        }
    });
}

#[inline]
fn charge_class_free(size: usize) {
    let _ = CURRENT_CLASS.try_with(|cell| {
        let slot = cell.get() as usize;
        if slot < CLASS_COUNT {
            CLASS_SLOTS[slot].frees.fetch_add(1, Ordering::Relaxed);
            CLASS_SLOTS[slot]
                .free_bytes
                .fetch_add(size as u64, Ordering::Relaxed);
        }
    });
}

/// Run `work` with everything it allocates charged to `class`.
///
/// The only way to attribute an allocation, and it is called INSIDE the primitive that owns the
/// sink. Nesting is allowed and the innermost scope wins, which is what lets the carried-page copy
/// be told apart from the slab append it happens inside. Entering the same class twice, nested, is
/// idempotent.
///
/// WITHOUT `alloc-probe` THIS IS `work()` AND NOTHING ELSE -- no atomic, no thread-local, no
/// branch, the closure inlined through. The scopes therefore sit in production code at no cost to
/// a production build, which is the only reason it is acceptable to put them there at all.
#[inline]
pub fn in_class<T>(class: AllocClass, work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "alloc-probe")]
    {
        let _scope = ClassScope::enter(class);
        work()
    }
    #[cfg(not(feature = "alloc-probe"))]
    {
        let _ = class;
        work()
    }
}

#[cfg(feature = "alloc-probe")]
struct ClassScope {
    previous: u8,
}

#[cfg(feature = "alloc-probe")]
impl ClassScope {
    fn enter(class: AllocClass) -> Self {
        let previous = CURRENT_CLASS
            .try_with(|cell| cell.replace(class.slot() as u8))
            .unwrap_or(UNCLASSIFIED);
        ClassScope { previous }
    }
}

#[cfg(feature = "alloc-probe")]
impl Drop for ClassScope {
    /// Restored on unwind as well as on return. A panic inside a scoped primitive that left the
    /// class set would charge the whole rest of that thread's life to it.
    fn drop(&mut self) {
        let _ = CURRENT_CLASS.try_with(|cell| cell.set(self.previous));
    }
}

/// One class's share of what a span allocated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassCounts {
    pub allocs: u64,
    pub alloc_bytes: u64,
    pub frees: u64,
    pub free_bytes: u64,
}

impl ClassCounts {
    /// Allocations made inside this class and not given back inside it.
    ///
    /// READ THIS CAREFULLY. A free is charged to the class in scope WHEN THE FREE HAPPENS, which
    /// is not necessarily the class that allocated the block: the allocator sees a pointer and a
    /// layout, not a history. Inside a narrow synchronous primitive almost everything freed was
    /// allocated there too, so this separates a class that CHURNS memory from one that KEEPS it --
    /// but a block allocated in one class and released in another is charged to the releasing one,
    /// and no assertion in this crate is built on this number alone.
    pub fn outstanding(&self) -> i64 {
        self.allocs as i64 - self.frees as i64
    }

    fn since(&self, earlier: &ClassCounts) -> ClassCounts {
        ClassCounts {
            allocs: self.allocs.saturating_sub(earlier.allocs),
            alloc_bytes: self.alloc_bytes.saturating_sub(earlier.alloc_bytes),
            frees: self.frees.saturating_sub(earlier.frees),
            free_bytes: self.free_bytes.saturating_sub(earlier.free_bytes),
        }
    }

    fn plus(&self, other: &ClassCounts) -> ClassCounts {
        ClassCounts {
            allocs: self.allocs + other.allocs,
            alloc_bytes: self.alloc_bytes + other.alloc_bytes,
            frees: self.frees + other.frees,
            free_bytes: self.free_bytes + other.free_bytes,
        }
    }
}

/// Every class's counters as of one moment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassLedger {
    rows: [ClassCounts; CLASS_COUNT],
}

impl ClassLedger {
    pub fn row(&self, class: AllocClass) -> ClassCounts {
        self.rows[class.slot()]
    }

    /// The rows added up.
    ///
    /// NOT the total for an operation, and never to be used as one: see `ClassifiedCounts`, whose
    /// residual is the difference between this and a counter the rows do not feed.
    pub fn summed(&self) -> ClassCounts {
        self.rows
            .iter()
            .fold(ClassCounts::default(), |acc, row| acc.plus(row))
    }

    /// Two spans' rows added together.
    ///
    /// So a harness can open a span per batch and build the next batch BETWEEN spans: a probe that
    /// assembles its fixture inside the measured window charges the store for it.
    pub fn plus(&self, other: &ClassLedger) -> ClassLedger {
        let mut rows = [ClassCounts::default(); CLASS_COUNT];
        for slot in 0..CLASS_COUNT {
            rows[slot] = self.rows[slot].plus(&other.rows[slot]);
        }
        ClassLedger { rows }
    }

    fn since(&self, earlier: &ClassLedger) -> ClassLedger {
        let mut rows = [ClassCounts::default(); CLASS_COUNT];
        for slot in 0..CLASS_COUNT {
            rows[slot] = self.rows[slot].since(&earlier.rows[slot]);
        }
        ClassLedger { rows }
    }

    /// `class=allocs/bytes` pairs, biggest first, for a one-line report.
    pub fn report_line(&self) -> String {
        let mut rows: Vec<(AllocClass, ClassCounts)> = AllocClass::ALL
            .iter()
            .map(|class| (*class, self.row(*class)))
            .collect();
        rows.sort_by(|left, right| right.1.alloc_bytes.cmp(&left.1.alloc_bytes));
        rows.iter()
            .map(|(class, counts)| {
                format!("{}={}/{}", class.label(), counts.allocs, counts.alloc_bytes)
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Every class's counters right now, or `None` when the counting allocator is not installed.
///
/// The `Option` is the whole point. Without `alloc-probe` every row reads zero, and a zero row is
/// indistinguishable from a class that was counted and cost nothing -- which is how an
/// uninstrumented build masquerades as a clean measurement. Handling the `None` is not optional at
/// the call site, because the type will not let it be.
///
/// There is deliberately no metrics emitter over this. A production build does not install the
/// counting allocator, so a periodic reap would publish nine rows of zeros on every scrape -- the
/// exact failure this `Option` exists to prevent, wearing a gauge.
pub fn classified_now() -> Option<ClassLedger> {
    #[cfg(feature = "alloc-probe")]
    {
        let mut rows = [ClassCounts::default(); CLASS_COUNT];
        for slot in 0..CLASS_COUNT {
            rows[slot] = ClassCounts {
                allocs: CLASS_SLOTS[slot].allocs.load(Ordering::Relaxed),
                alloc_bytes: CLASS_SLOTS[slot].alloc_bytes.load(Ordering::Relaxed),
                frees: CLASS_SLOTS[slot].frees.load(Ordering::Relaxed),
                free_bytes: CLASS_SLOTS[slot].free_bytes.load(Ordering::Relaxed),
            };
        }
        Some(ClassLedger { rows })
    }
    #[cfg(not(feature = "alloc-probe"))]
    {
        None
    }
}

/// A span measured BOTH ways at once: the whole-span counter, and the per-class ledger inside it.
///
/// THE RESIDUAL IS A MEASUREMENT, NOT AN IDENTITY. The span total comes from `Probe`, which the
/// class rows do not feed, so the residual can be wrong and can be seen to be wrong. This crate
/// shipped a residual computed from the rows it audited -- in `restore_scale`, where it read zero
/// "by construction" -- and had to correct it. A residual that cannot fail is not a check.
pub struct ClassSpan {
    probe: Probe,
    opened: ClassLedger,
}

impl ClassSpan {
    pub fn open() -> Self {
        ClassSpan {
            probe: Probe::start(),
            opened: classified_now().unwrap_or_default(),
        }
    }

    /// What the span cost, split by class, or `None` when nothing was counting.
    pub fn close(self) -> Option<ClassifiedCounts> {
        let closed = classified_now()?;
        Some(ClassifiedCounts {
            total: self.probe.stop(),
            classes: closed.since(&self.opened),
        })
    }
}

/// What a span cost, with the classes and the independent total side by side.
#[derive(Debug, Clone, Copy)]
pub struct ClassifiedCounts {
    /// The whole span, from the process-wide counter. Nothing below feeds this.
    pub total: AllocCounts,
    /// The same span, split by what the allocations were spent on.
    pub classes: ClassLedger,
}

impl ClassifiedCounts {
    /// Allocations the span made that no class claimed.
    ///
    /// Signed: a negative residual means the rows claim more than the span made, which is what
    /// double counting looks like and is worth failing on rather than clamping away.
    pub fn residual_allocs(&self) -> i64 {
        self.total.allocs as i64 - self.classes.summed().allocs as i64
    }

    pub fn residual_bytes(&self) -> i64 {
        self.total.alloc_bytes as i64 - self.classes.summed().alloc_bytes as i64
    }

    /// The share of the span's allocated bytes that landed in a named class.
    pub fn classified_byte_share(&self) -> f64 {
        if self.total.alloc_bytes == 0 {
            return 0.0;
        }
        self.classes.summed().alloc_bytes as f64 / self.total.alloc_bytes as f64
    }

    /// The share of the span's allocation CALLS that landed in a named class.
    pub fn classified_call_share(&self) -> f64 {
        if self.total.allocs == 0 {
            return 0.0;
        }
        self.classes.summed().allocs as f64 / self.total.allocs as f64
    }

    /// Two spans added together, rows and independent total alike.
    ///
    /// The total is summed from the same `Probe` readings it always was, so a sum of spans keeps
    /// the property the residual depends on: the total still does not come from the rows.
    pub fn plus(&self, other: &ClassifiedCounts) -> ClassifiedCounts {
        ClassifiedCounts {
            total: AllocCounts {
                allocs: self.total.allocs + other.total.allocs,
                alloc_bytes: self.total.alloc_bytes + other.total.alloc_bytes,
                frees: self.total.frees + other.total.frees,
                free_bytes: self.total.free_bytes + other.total.free_bytes,
            },
            classes: self.classes.plus(&other.classes),
        }
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
