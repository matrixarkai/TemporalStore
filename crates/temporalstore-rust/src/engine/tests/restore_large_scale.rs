// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT BRINGING A LARGE SHARD BACK COSTS, at 20,000 and 200,000 records, in both regimes.
//!
//! Every restore figure in this crate stops at 20,000 records. `restore_scale.rs` cuts a restart
//! into its five phases at 2,000 and 20,000 and reports the whole thing flat per record; this
//! file starts where that one stops and goes an order of magnitude past it. It also turns the
//! knob that decides more about a restart than its size does: HOW MUCH OF THE LOG IS UNREFLECTED
//! when the shard comes back.
//!
//! Two regimes, and they are different restores rather than two sizes of one:
//!
//!   * REFLECTED -- the shard dumped a checkpoint and then went away without taking another
//!     write, so nothing is past the checkpoint. The post-replay fold then does not run AT ALL:
//!     two allocations, at both corpus sizes. That is asserted exactly, because it is what makes
//!     this a second regime rather than a second fixture.
//!   * UNREFLECTED -- half the shard's writes landed after the last checkpoint.
//!
//! THE BILL, per stored record. 128-byte incompressible values, one checkpoint, three slabs with
//! a disjoint key range in each, two log pieces, and the store path 15 characters in every arm --
//! allocation counts move with the path length, so it is held equal and asserted:
//!
//! ```text
//!                              REFLECTED                     UNREFLECTED
//!                       20,000   200,000  ratio        20,000    200,000  ratio
//!   allocations          22.034    21.710  0.985        34.363     37.250  1.084
//!    manifest+base index 18.458    18.448  0.999         9.233      9.225  0.999
//!    publish_and_seed     3.109     3.107  0.999         3.109      3.107  0.999
//!    wal_replay           0.467     0.156  0.333         7.661     10.566  1.379
//!    index_fold          2 allocations, not a rate       14.360     14.352  0.999
//!   promotion-check pages 1.0000    1.0000  1.000        1.5000     1.5000  1.000
//!   manifest bytes read 190.28    191.23   1.005        93.535     95.421  1.020
//!   the log store's own
//!     bytes_read           0         0      --         150.892    150.905  1.000
//!   heap bytes not freed 897.6     833.0   0.928       837.3      808.5    0.966
//!   KERNEL rchar         431.06    396.96  0.921       757.44    5073.40   6.698
//!    of which no reader
//!    in this file claims 240.77    205.73  0.854       513.02    4827.08   9.409
//!   replay windows (n)       1         1                    6         59
//! ```
//!
//! WHAT IS FLAT. Everything the engine can see. Allocations per stored record in the reflected
//! regime, 22.03 -> 21.71. The checkpoint decode, 18.458 -> 18.448 and 9.233 -> 9.225. The seed,
//! 3.109 -> 3.107. The fold, 14.360 -> 14.352. The promotion check's walk, 1.0000 and 1.5000
//! live model-map pages per stored record to four decimal places -- which is the count the 4.0
//! allocations a page and 6.0 a stored record of #1884 and #1891 are rates over, and it has not
//! moved at ten times the store. Manifest bytes read, 190.28 -> 191.23. Two of the nine
//! allocation classes of #1922, 1.2309 -> 1.2289 and 1.0007 -> 1.0001. And the part of the span
//! no phase row accounts for: 8 or 9 allocations, at every size and in every regime, which is a
//! fixed edge cost and not a phase nobody wrote down.
//!
//! WHAT GROWS. Replay, and only replay -- 7.661 -> 10.566 allocations and 2,948 -> 11,183
//! allocated bytes per stored record, which carries the whole restore's allocated bytes from
//! 8,660 to 16,519 a record. And, far larger than any of it, what the KERNEL says the process
//! read.
//!
//! THE FINDING, AND IT IS ONLY VISIBLE FROM OUTSIDE THE ENGINE. Bringing back a 200,000-record
//! shard with half its writes past the checkpoint reads 1,014,680,664 bytes off disk -- a
//! gigabyte, to rebuild an 87.9 MB store. The log store's own counter says 30,180,893, the
//! manifest reader says 19,084,120, and nothing else in this engine claims a byte: 965,415,651
//! of that gigabyte is read by a reader with no counter on it. Per stored record the kernel's
//! number goes 757.44 -> 5,073.40 for ten times the store, and the unattributed part of it
//! 513.02 -> 4,827.08, while EVERY counter inside the engine reports the same restore as
//! perfectly flat -- the log store's own `bytes_read` moves 1.000.
//!
//! The mechanism is the replay WINDOW COUNT, which is the one thing that grew: 6 windows at
//! 20,000 records and 59 at 200,000, each re-reading the pieces behind it. `restore_scale.rs`
//! recorded the same shape at 2,000 and 20,000 from an `strace` and called it inherited and
//! bounded. At 200,000 it is 1.01 GB, it is 89% of what a restart reads, and it is paid at
//! exactly the moment a node is trying to return to service. Nothing here fixes it; what this
//! file adds is a number for it and a guard that fails if it stops being true.
//!
//! AND IN THE OTHER REGIME IT IS NOT THERE AT ALL. A shard dumped immediately before it went
//! away reads 396.96 bytes per stored record at 200,000 against 431.06 at 20,000 -- flat, and
//! slightly better. One window, no replay, no fold. A guard that measured only this regime would
//! report a healthy system at any size, which is why both are measured and the control arm is
//! asserted by name.
//!
//! WHAT A REFLECTED RESTART COSTS ANYWAY, which is the other half of the regime story: 21.710
//! allocations and 6,165 allocated bytes per stored record, 18.7 seconds and a peak of 1.60 GiB,
//! for a shard with NOTHING to recover. 85% of it is one phase -- decoding the checkpoint's
//! embedded whole-shard index image, 38,246,262 bytes of it, and re-serialising all 38,246,198
//! of them to verify a checksum. That cost is priced by the STORE while the work in front of it
//! is zero, which is the shape three of this campaign's findings already have.
//!
//! WALL TIME AND PEAK MEMORY -- the numbers nobody had. A 200,000-record shard comes back in
//! 18.7 s reflected and 23.8 s unreflected, holding a peak of 1,672,388 and 1,649,228 KiB. THE
//! BOX WAS LOADED: `/proc/loadavg` between 8.4 and 9.5 with four other build jobs on it, printed
//! beside every arm in the report. Those are UPPER BOUNDS and not clean figures. Every count in
//! the table above is not: three independent runs agreed to four decimal places on every
//! allocation column, and every comparison in this file rests on counts. The time is here
//! because a node's return to service is measured in seconds, and twenty-four of them is one
//! shard of 200,000 records.
//!
//! PEAK RSS UNDERSTATES A RESTORE and is reported anyway, beside something that does not: the
//! restore runs in a process that has just built the corpus, so the allocator serves most of it
//! out of memory it already holds. The span's allocated-minus-freed bytes are the same question
//! asked of a counter that cannot be fooled that way, and they are flat: 897.6 -> 833.0 and
//! 837.3 -> 808.5 bytes per stored record.
//!
//! THE CATEGORISED ACCOUNTING OF #1922 DOES NOT FIT THIS PATH, and the measured share says so:
//! 6.0% of a replaying restore's allocation calls and 2.4% of its bytes land in a named class,
//! and 0.0% of a reflected one's. Eight of the nine classes are charged inside WRITE primitives;
//! a restore's dominant costs -- decoding a checkpoint's index image, and rebuilding the bucket
//! index in the fold -- are inside none of them. The two classes it does enter, `bucket_index`
//! and `staged_outcome`, are reported above and are flat. Extending the classes to cover the
//! load path would be a change to nine production primitives and is not attempted here.
//!
//! THE RESIDUAL IS INDEPENDENT, and it is the only row here that could have caught the finding.
//! It is the kernel's `rchar` for this process across the `load_shard_with` call, minus the four
//! readers this file names -- never the sum of the rows it audits, which is a residual that
//! cannot fail. The instrument has its own control:
//! `the_kernel_byte_instrument_recovers_a_planted_read_exactly` plants 1,000,003 and 3,000,009
//! bytes in front of it and asserts it hands back exactly those numbers, and that an empty span
//! reads exactly zero, so a residual of zero provably comes from an instrument that can see a
//! hit. The instrument subtracts its own reading's bytes rather than estimating them, which is
//! what makes "exactly" available at all.
//!
//! WHAT STOPPED THIS AT 200,000: TIME, not disk. The two large corpora take 108.8 s and 88.0 s
//! to build and the four-arm run 330.7 s, on a box shared with four other threads; 43 GB of disk
//! were still free when it finished. A 2,000,000-record arm would build for something over
//! twenty minutes and then, on the window growth measured here, read on the order of a hundred
//! gigabytes to come back.
//!
//! No production code changes.

#![allow(clippy::all)]
// Most of this file only exists with `alloc-probe`, so its sizes and helpers go unread in an
// ordinary build.
#![allow(dead_code)]
use super::*;
use crate::alloc_probe::AllocClass;
use crate::engine::lifecycle::restore_phase_probe;
use crate::engine::storage_bucket_internals::{
    promote_model_map_check_counts, reset_promote_model_map_check_counts,
};

/// The size every existing restore figure stops at, kept as the small arm so this file starts
/// where `restore_scale.rs` ends.
const BASE: usize = 20_000;
/// An order of magnitude past it.
const BIG: usize = 200_000;
/// What the two ordinary-gate guards build. Large enough to roll more than one log piece and
/// more than one slab, small enough to belong in a gate.
const GATE: usize = 20_000;
const VALUE_BYTES: usize = 128;
/// Records per seeded batch, and therefore per LOG RECORD: the walk decodes batches, not keys.
const BATCH: usize = 100;
/// Spread across the whole key space, so a restore that recovered only the dumped half fails
/// here rather than being reported as a cheap recovery.
const PROBE_READS: usize = 64;

/// How much of the log is UNREFLECTED at load time -- the knob, and the reason this file measures
/// two regimes rather than one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Suffix {
    /// The shard dumped a checkpoint and then went away without taking another write. Nothing
    /// past the checkpoint; replay has no tail to apply.
    Reflected,
    /// Half the shard's writes landed after the last checkpoint. A long unreflected suffix.
    Unreflected,
}

impl Suffix {
    fn label(self) -> &'static str {
        match self {
            Suffix::Reflected => "REFLECTED (dumped immediately before the shard went away)",
            Suffix::Unreflected => "UNREFLECTED (half the writes are past the last checkpoint)",
        }
    }

    fn short(self) -> &'static str {
        match self {
            Suffix::Reflected => "reflected",
            Suffix::Unreflected => "unreflected",
        }
    }

    fn dumped(self, records: usize) -> usize {
        match self {
            Suffix::Reflected => records,
            Suffix::Unreflected => records / 2,
        }
    }
}

/// xorshift64*, seeded by index so runs repeat and no two records share a payload.
///
/// A repeated byte compresses, and a log built from one would hold a fraction of the bytes its
/// record count suggests.
fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT: the kernel's own byte counter for this process.
//
// `rchar` is bytes handed to this process by read-family syscalls, maintained by the kernel.
// Nothing in the engine feeds it, so a residual computed against it CAN be wrong and can be seen
// to be wrong -- which is the property a residual computed from the rows it audits does not have.
//
// Reading the counter is itself a read, and the kernel charges it. The reading therefore carries
// the LENGTH of the text it was read from, so the span's own instrument can be subtracted exactly
// rather than estimated: see `IoReading`.
// ---------------------------------------------------------------------------------------------

/// One reading of `/proc/self/io`, with the size of the text it came from.
#[derive(Debug, Clone, Copy)]
struct IoReading {
    rchar: u64,
    wchar: u64,
    /// Bytes this reading's own `read` was handed. `rchar` above excludes them -- procfs fills
    /// the buffer before the kernel charges for it -- so they land inside any span this reading
    /// OPENS, and subtracting this number removes the instrument from its own measurement.
    self_bytes: u64,
}

fn read_proc_io() -> Option<IoReading> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    let field = |name: &str| -> Option<u64> {
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix(name) {
                if let Some(value) = rest.strip_prefix(':') {
                    return value.trim().parse().ok();
                }
            }
        }
        None
    };
    Some(IoReading {
        rchar: field("rchar")?,
        wchar: field("wchar")?,
        self_bytes: text.len() as u64,
    })
}

/// What the kernel says a span of work read and wrote, with this instrument's own reads removed.
#[derive(Debug, Clone, Copy, Default)]
struct KernelIo {
    rchar: u64,
    wchar: u64,
}

/// Run `work` between two readings of the kernel's counters.
///
/// `None` when `/proc` said nothing, so "the kernel was not counting" cannot be read as "the
/// kernel counted zero" -- the same rule the allocation probe follows.
fn kernel_io_span<T>(work: impl FnOnce() -> T) -> (T, Option<KernelIo>) {
    let before = read_proc_io();
    let value = work();
    let after = read_proc_io();
    let io = before.zip(after).map(|(before, after)| KernelIo {
        // The opening reading's own bytes fall inside the span; nothing else of this
        // instrument's does, because the closing reading is charged after it is taken.
        rchar: (after.rchar - before.rchar).saturating_sub(before.self_bytes),
        wchar: after.wchar - before.wchar,
    });
    (value, io)
}

fn vm_status_kb(field: &str) -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            if let Some(value) = rest.strip_prefix(':') {
                return value.trim().trim_end_matches("kB").trim().parse::<u64>().ok();
            }
        }
    }
    None
}

/// Reset this process's peak-RSS high-water mark, so the peak reported for a restore is the
/// restore's and not the corpus build's.
///
/// `5` to `clear_refs` is the kernel's "reset VmHWM" request. It is best-effort: where it is not
/// permitted the peak stays where it was, which shows up as a peak no smaller than the resident
/// size before the restore and is reported as measured rather than corrected.
fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

// ---------------------------------------------------------------------------------------------
// THE CORPUS
// ---------------------------------------------------------------------------------------------

fn dir_bytes(root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            total = total.saturating_add(dir_bytes(&entry.path()));
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    total
}

fn files_in(root: &std::path::Path) -> (usize, u64) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return (0, 0);
    };
    let mut count = 0usize;
    let mut bytes = 0u64;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_file() {
            count += 1;
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    (count, bytes)
}

/// What a shard that went away without unloading left on disk.
#[derive(Debug, Clone)]
struct Corpus {
    records: usize,
    value_bytes: usize,
    suffix: Suffix,
    /// Records the last dump manifest covers. The rest are the unreflected suffix a restore
    /// has to replay.
    dumped: usize,
    tail: usize,
    wal_bytes: u64,
    wal_pieces: usize,
    manifest_files: usize,
    manifest_bytes: u64,
    index_log_bytes: u64,
    page_bytes: u64,
    store_bytes: u64,
    /// The `wal_sequence` each checkpoint anchored at, in the order they were taken. The pick
    /// takes a maximum over exactly this field, so a fixture whose manifests all carried the
    /// same value could not tell a correct pick from a constant.
    manifest_anchors: Vec<u64>,
    /// Slabs on disk after the writes, and how many of them hold live bytes. Both, because a
    /// store whose every live page sits in slab 0 compares `{0}` against `{0}`.
    slabs_on_disk: usize,
    slabs_holding_live_bytes: usize,
    /// Distinct keys the corpus actually wrote, counted from the strings rather than assumed.
    distinct_keys: usize,
    /// Length of the store's root path. Allocation counts move with it, so it is held equal
    /// across arms and asserted rather than assumed.
    store_path_len: usize,
}

impl Corpus {
    fn anchor(&self) -> u64 {
        self.manifest_anchors.last().copied().unwrap_or(0)
    }
}

/// A shard that took `records` writes, dumped `dumps` checkpoints over its first `dumped` of
/// them, and then went away.
///
/// The engine is DROPPED rather than unloaded. Unloading materialises the base index, which is
/// exactly the durable checkpoint a crash does not leave behind.
fn build_corpus(
    dir: &std::path::Path,
    records: usize,
    value_bytes: usize,
    dumps: usize,
    dumped: usize,
    suffix: Suffix,
) -> Corpus {
    let index_dir = dir.join("indexes");
    let mut manifest_anchors = Vec::new();
    let (slabs_on_disk, slabs_holding_live_bytes) = {
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.join("cache"),
            dir.join("pages"),
            index_dir.clone(),
        );
        engine.load_shard(1);
        // WHERE THE SLABS ROLL. Deliberately NOT the checkpoint boundary: a fixture whose slab
        // edges sat on the dumped/unreflected split would confound the two, and one whose every
        // live page sat in slab 0 would compare `{0}` against `{0}`. A quarter and three
        // quarters puts a DISJOINT key range in each of three slabs and crosses the checkpoint
        // boundary at neither.
        let roll_at = [records / 4, records * 3 / 4];
        let mut taken = 0usize;
        while taken < dumps {
            seed(
                &engine,
                dumped * taken / dumps.max(1),
                dumped * (taken + 1) / dumps.max(1),
                value_bytes,
                &roll_at,
            );
            let manifest = engine
                .create_bucket_dump_manifest(1, Vec::<u32>::new())
                .expect("the corpus needs a durable dump manifest to recover from");
            manifest_anchors.push(manifest.wal_sequence);
            taken += 1;
        }
        seed(&engine, dumped, records, value_bytes, &roll_at);
        let store = engine.block_store();
        let on_disk = store.slab_ids().map(|ids| ids.len()).unwrap_or(0);
        let live = store
            .slab_usage()
            .iter()
            .filter(|usage| usage.live_bytes > 0)
            .count();
        (on_disk, live)
    };
    let (manifest_files, manifest_bytes) = files_in(&index_dir.join("slot-dumps").join("shard-1"));
    let (wal_pieces, wal_bytes) = files_in(&index_dir.join("wals"));
    let distinct_keys = (0..records)
        .map(|index| format!("k-{index:08}"))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    Corpus {
        records,
        value_bytes,
        suffix,
        dumped,
        tail: records - dumped,
        wal_bytes,
        wal_pieces,
        manifest_files,
        manifest_bytes,
        index_log_bytes: dir_bytes(&index_dir.join("indexlogs")),
        page_bytes: dir_bytes(&dir.join("pages")),
        store_bytes: dir_bytes(dir),
        manifest_anchors,
        slabs_on_disk,
        slabs_holding_live_bytes,
        distinct_keys,
        store_path_len: dir.as_os_str().len(),
    }
}

fn seed(
    engine: &TemporalEngine,
    from: usize,
    to: usize,
    value_bytes: usize,
    roll_at: &[usize],
) {
    let mut index = from;
    while index < to {
        let end = (index + BATCH).min(to);
        // Roll the active slab when the corpus crosses a roll point, so the pages this shard
        // will be rebuilt from are spread over more than one slab.
        if roll_at
            .iter()
            .any(|point| *point > index && *point <= end && index > 0)
        {
            engine
                .block_store()
                .roll_slab()
                .expect("the fixture must be able to roll a slab");
        }
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: incompressible(value_bytes, cursor as u64),
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

// ---------------------------------------------------------------------------------------------
// THE MEASUREMENT
// ---------------------------------------------------------------------------------------------

/// What the restart cost.
#[derive(Debug, Clone)]
struct Restart {
    corpus: Corpus,
    cost: restore_phase_probe::RestoreCost,
    /// Allocations and bytes across the WHOLE `load_shard_with` call, split by what they were
    /// spent on, with an independent span total the class rows do not feed. `None` when nothing
    /// was counting.
    classified: Option<crate::alloc_probe::ClassifiedCounts>,
    wal_window_walks: u64,
    wal_bytes_read: u64,
    index_log_bytes_read: u64,
    index_log_reads: u64,
    /// Page bytes the block store handed back during the restore, and the reads that fetched
    /// them. The fourth named reader, and the one the first draft of this file was missing --
    /// which is what a residual that can be wrong is for.
    page_bytes_read: u64,
    page_reads: u64,
    /// The promotion check's borrow-only walk: how many times it ran, how many of those
    /// REBUILT, and how many live model-map pages it walked. The 4.0 allocations a page and
    /// 6.0 a stored record that #1884 and #1891 left behind are rates over exactly this
    /// count, and it is what makes them checkable at a size nobody has run them at.
    promote_checks: u64,
    promote_rebuilds: u64,
    promote_pages: u64,
    /// Allocations the span made and did not free, and the bytes of them. A restore runs in
    /// a process that has just built the corpus, so the allocator serves most of it out of
    /// memory it already holds and process RSS understates what the restore takes. This is
    /// the same question asked of the counter that cannot be fooled that way.
    net_heap_allocs: i64,
    net_heap_bytes: i64,
    manifest_file_reads: u64,
    manifest_bytes_read: u64,
    manifest_checksum_bytes: u64,
    /// The kernel's own byte counters across the restore, or `None` where `/proc` said nothing.
    kernel: Option<KernelIo>,
    replayed_from: u64,
    readable_after: usize,
    wall_ms: u64,
    build_ms: u64,
    /// Resident set before the restore, and the peak reached during it, in KiB.
    rss_before_kb: u64,
    rss_after_kb: u64,
    peak_rss_kb: u64,
}

impl Restart {
    fn span_allocs(&self) -> Option<u64> {
        self.classified.map(|counts| counts.total.allocs)
    }

    fn span_alloc_bytes(&self) -> Option<u64> {
        self.classified.map(|counts| counts.total.alloc_bytes)
    }

    /// Allocations inside the restore that no PHASE row accounts for: the span counter either
    /// side of the whole call, minus the rows.
    fn phase_residual_allocs(&self) -> Option<i64> {
        self.span_allocs()
            .map(|span| span as i64 - self.cost.allocs() as i64)
    }

    fn phase_allocs(&self, name: &str) -> u64 {
        self.cost.phase(name).allocs
    }

    /// Bytes the restore read that this file attributes to a named reader.
    fn attributed_bytes_read(&self) -> u64 {
        self.wal_bytes_read
            + self.manifest_bytes_read
            + self.index_log_bytes_read
            + self.page_bytes_read
    }

    /// THE INDEPENDENT RESIDUAL: what the kernel handed this process during the restore, minus
    /// the rows above. Nothing in the engine feeds `rchar`, so this can be wrong.
    fn read_residual_bytes(&self) -> Option<i64> {
        self.kernel
            .map(|kernel| kernel.rchar as i64 - self.attributed_bytes_read() as i64)
    }

    fn per_record(&self, value: u64) -> f64 {
        value as f64 / self.corpus.records as f64
    }
}

/// Restore the shard the corpus left behind, in this process, with every counter zeroed first.
fn restore(dir: &std::path::Path, corpus: Corpus) -> Restart {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache-restore"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    crate::engine::reset_bucket_dump_manifest_io_counts();
    let index_log_before = engine.index_log_store().stats(1);
    let pages_before = engine.block_store().stats();
    reset_promote_model_map_check_counts();
    reset_peak_rss();
    let rss_before_kb = vm_status_kb("VmRSS").unwrap_or(0);
    let started = std::time::Instant::now();
    let span = crate::alloc_probe::ClassSpan::open();
    let ((response, classified), kernel) = kernel_io_span(|| {
        let response = engine.load_shard_with(LoadShardRequest {
            shard_id: 1,
            load_version: 1,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
            readonly: false,
            table_name: "t".to_string(),
        });
        // Read BEFORE `finish()`, which allocates a report the restore did not.
        let classified = span.close();
        (response, classified)
    });
    let wall_ms = started.elapsed().as_millis() as u64;
    let peak_rss_kb = vm_status_kb("VmHWM").unwrap_or(0);
    let rss_after_kb = vm_status_kb("VmRSS").unwrap_or(0);
    let cost = restore_phase_probe::finish();
    assert!(
        response.status.ok,
        "the restore under measurement must succeed, or every number below describes a failure: \
         {:?}",
        response.status
    );
    let manifest_counts = crate::engine::bucket_dump_manifest_io_counts();
    let wal = engine.write_ahead_log_store().raw_stats(1);
    let index_log_after = engine.index_log_store().stats(1);
    // BEFORE the probe reads below, which would charge their own page reads to the restore.
    let pages_after = engine.block_store().stats();
    let (promote_checks, promote_rebuilds, promote_pages) = promote_model_map_check_counts();
    let mut readable = 0usize;
    let mut probe = 0usize;
    while probe < PROBE_READS {
        let index = probe * corpus.records / PROBE_READS;
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("k-{index:08}"),
            },
        });
        let recovered = matches!(
            &response.response,
            CommandResponse::Bytes { value }
                if value.as_ref().map(|bytes| bytes.len()) == Some(corpus.value_bytes)
        );
        if response.status.ok && recovered {
            readable += 1;
        }
        probe += 1;
    }
    Restart {
        corpus,
        cost,
        classified,
        wal_window_walks: wal.scans,
        wal_bytes_read: wal.bytes_read,
        index_log_bytes_read: index_log_after
            .bytes_read
            .saturating_sub(index_log_before.bytes_read),
        index_log_reads: index_log_after.reads.saturating_sub(index_log_before.reads),
        page_bytes_read: pages_after
            .bytes_read
            .saturating_sub(pages_before.bytes_read),
        page_reads: pages_after.reads.saturating_sub(pages_before.reads),
        promote_checks,
        promote_rebuilds,
        promote_pages,
        net_heap_allocs: classified.map(|c| c.total.outstanding()).unwrap_or(0),
        net_heap_bytes: classified
            .map(|c| c.total.alloc_bytes as i64 - c.total.free_bytes as i64)
            .unwrap_or(0),
        manifest_file_reads: manifest_counts.file_reads,
        manifest_bytes_read: manifest_counts.bytes_read,
        manifest_checksum_bytes: manifest_counts.checksum_bytes,
        kernel,
        replayed_from: crate::engine::lifecycle::LAST_REPLAY_WATERMARK
            .load(std::sync::atomic::Ordering::SeqCst),
        readable_after: readable,
        wall_ms,
        build_ms: 0,
        rss_before_kb,
        rss_after_kb,
        peak_rss_kb,
    }
}

fn measure(records: usize, suffix: Suffix, dumps: usize) -> Restart {
    let dir = tempfile::tempdir().expect("tempdir");
    let built = std::time::Instant::now();
    let corpus = build_corpus(
        dir.path(),
        records,
        VALUE_BYTES,
        dumps,
        suffix.dumped(records),
        suffix,
    );
    let build_ms = built.elapsed().as_millis() as u64;
    let mut restart = restore(dir.path(), corpus);
    restart.build_ms = build_ms;
    restart
}

// ---------------------------------------------------------------------------------------------
// THE REPORT -- written to a FILE as well as stdout.
//
// `cargo test` captures stderr for PASSING tests and the `--nocapture` path has its own traps, so
// the numbers land somewhere a run that was interrupted still leaves them.
// ---------------------------------------------------------------------------------------------

struct Report {
    path: std::path::PathBuf,
    lines: Vec<String>,
}

impl Report {
    fn open(name: &str) -> Report {
        Report {
            path: std::env::temp_dir().join(format!("ts-{name}.txt")),
            lines: Vec::new(),
        }
    }

    fn say(&mut self, line: String) {
        println!("{line}");
        self.lines.push(line);
        let _ = std::fs::write(&self.path, self.lines.join("\n"));
    }
}

fn describe(report: &mut Report, label: &str, restart: &Restart) {
    let corpus = &restart.corpus;
    report.say(format!(
        "\n{label}  --  {}\n  corpus: {} records of {} B, {} dumped, {} unreflected; store {} B \
         in total -- log {} B in {} piece(s), {} manifest file(s) {} B, index-log {} B, pages {} \
         B; {} slab(s) on disk, {} holding live bytes; store path {} chars; built in {} ms",
        corpus.suffix.label(),
        corpus.records,
        corpus.value_bytes,
        corpus.dumped,
        corpus.tail,
        corpus.store_bytes,
        corpus.wal_bytes,
        corpus.wal_pieces,
        corpus.manifest_files,
        corpus.manifest_bytes,
        corpus.index_log_bytes,
        corpus.page_bytes,
        corpus.slabs_on_disk,
        corpus.slabs_holding_live_bytes,
        corpus.store_path_len,
        restart.build_ms,
    ));
    report.say(format!(
        "  replayed from wal_sequence {} (last checkpoint anchor {}), {} of {} probed keys \
         readable, restore wall {} ms",
        restart.replayed_from,
        corpus.anchor(),
        restart.readable_after,
        PROBE_READS,
        restart.wall_ms,
    ));
    report.say(
        "  phase                       allocs     alloc_bytes        ms      allocs/record"
            .to_string(),
    );
    for phase in &restart.cost.phases {
        report.say(format!(
            "  {:<24} {:>10} {:>15} {:>9.1} {:>18.3}",
            phase.phase,
            phase.allocs,
            phase.alloc_bytes,
            phase.nanos as f64 / 1e6,
            phase.allocs as f64 / corpus.records as f64,
        ));
    }
    report.say(format!(
        "  {:<24} {:>10} {:>15} {:>9.1} {:>18.3}   <- every phase, summed",
        "ALL PHASES",
        restart.cost.allocs(),
        restart.cost.alloc_bytes(),
        restart.cost.nanos() as f64 / 1e6,
        restart.cost.allocs() as f64 / corpus.records as f64,
    ));
    match (restart.span_allocs(), restart.phase_residual_allocs()) {
        (Some(span), Some(residual)) => report.say(format!(
            "  {:<24} {:>10} {:>15}            <- the whole call; the rows miss {} of it",
            "SPAN (independent)",
            span,
            restart.span_alloc_bytes().unwrap_or(0),
            residual,
        )),
        _ => report
            .say("  (the allocation columns read zero: built without `alloc-probe`)".to_string()),
    }
    if let Some(counts) = restart.classified {
        report.say(format!(
            "  CLASSES  {:.1}% of allocation calls and {:.1}% of allocated bytes landed in a \
             named class",
            100.0 * counts.classified_call_share(),
            100.0 * counts.classified_byte_share(),
        ));
        for class in AllocClass::ALL {
            let row = counts.classes.row(class);
            if row.allocs == 0 && row.alloc_bytes == 0 {
                continue;
            }
            report.say(format!(
                "    {:<18} {:>10} allocs {:>14} B   {:>8.3} allocs/record",
                class.label(),
                row.allocs,
                row.alloc_bytes,
                row.allocs as f64 / corpus.records as f64,
            ));
        }
    }
    report.say(format!(
        "  READS    kernel rchar {} B; attributed: log {} B in {} window walk(s), manifest {} B \
         in {} file read(s), index-log {} B in {} read(s), pages {} B in {} read(s); RESIDUAL {} B",
        restart.kernel.map(|io| io.rchar as i64).unwrap_or(-1),
        restart.wal_bytes_read,
        restart.wal_window_walks,
        restart.manifest_bytes_read,
        restart.manifest_file_reads,
        restart.index_log_bytes_read,
        restart.index_log_reads,
        restart.page_bytes_read,
        restart.page_reads,
        restart.read_residual_bytes().unwrap_or(-1),
    ));
    report.say(format!(
        "  WRITES   kernel wchar {} B; manifest re-serialised for a checksum {} B",
        restart.kernel.map(|io| io.wchar as i64).unwrap_or(-1),
        restart.manifest_checksum_bytes,
    ));
    report.say(format!(
        "  MEMORY   resident {} KiB before the restore, {} KiB after, peak {} KiB during; \
         the span allocated {} bytes it did not free, in {} allocations it did not free",
        restart.rss_before_kb,
        restart.rss_after_kb,
        restart.peak_rss_kb,
        restart.net_heap_bytes,
        restart.net_heap_allocs,
    ));
    report.say(format!(
        "  PROMOTE  the promotion check ran {} time(s), rebuilt {} time(s), and walked {} \
         live model-map pages -- {:.4} per stored record",
        restart.promote_checks,
        restart.promote_rebuilds,
        restart.promote_pages,
        restart.promote_pages as f64 / corpus.records as f64,
    ));
    report.say(format!(
        "  WALK     decoded {} records, {} of them ({:.0}%) already covered by the checkpoint",
        restart.cost.records_decoded,
        restart.cost.records_behind_checkpoint,
        100.0 * restart.cost.records_behind_checkpoint as f64
            / restart.cost.records_decoded.max(1) as f64,
    ));
    report.say(format!(
        "  LOCK     {} write syscall(s) under the engine lock, {} after releasing it",
        restart.cost.writes_under_engine_lock, restart.cost.writes_after_engine_lock,
    ));
}

/// The box this ran on, printed beside every wall-time figure.
fn load_line() -> String {
    let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    format!("  BOX /proc/loadavg {}", load.trim())
}

// ---------------------------------------------------------------------------------------------
// WHAT THE FIXTURE HAS TO BE ABLE TO EXPRESS
// ---------------------------------------------------------------------------------------------

/// A fixture whose items look alike cannot tell a correct decision from a constant.
///
/// Asserted on EVERY arm of the headline as well as in the ordinary-gate guard, so a size that
/// collapses back into one slab or one log piece fails where it is measured and not only where
/// it is checked.
fn assert_fixture_can_express(restart: &Restart) {
    let corpus = &restart.corpus;
    assert!(
        corpus.wal_pieces > 1,
        "the log must span more than one piece, or the windowed walk this measures has one \
         window by construction: {} piece(s) at {} records",
        corpus.wal_pieces,
        corpus.records
    );
    assert!(
        corpus.slabs_on_disk > 1,
        "the store must span more than one slab: {} slab(s) at {} records",
        corpus.slabs_on_disk,
        corpus.records
    );
    assert!(
        corpus.slabs_holding_live_bytes > 1,
        "and the LIVE pages must span more than one of them -- a store whose every live page \
         sits in slab 0 compares one slab against itself: {} of {} slab(s) hold live bytes",
        corpus.slabs_holding_live_bytes,
        corpus.slabs_on_disk
    );
    assert_eq!(
        corpus.distinct_keys, corpus.records,
        "every record must name a DISTINCT key -- the field the index keys on -- or a restore \
         that recovered one of them would read back as a restore that recovered all: {} distinct \
         keys for {} records",
        corpus.distinct_keys, corpus.records
    );
    assert_eq!(
        restart.readable_after, PROBE_READS,
        "and every probed key, spread across the whole key space, must read back after the \
         restore: {} of {}",
        restart.readable_after, PROBE_READS
    );
}

// ---------------------------------------------------------------------------------------------
// THE HEADLINE
// ---------------------------------------------------------------------------------------------

/// WHAT A LARGE RESTART COSTS, AT TWO CORPUS SIZES AND IN BOTH SUFFIX REGIMES.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "builds a 200,000-record store; run by name under --features alloc-probe"]
fn what_bringing_a_large_shard_back_costs_at_two_corpus_sizes() {
    let mut report = Report::open("restore-large-scale");
    report.say(load_line());
    let mut arms: Vec<Restart> = Vec::new();
    for (records, suffix) in [
        (BASE, Suffix::Reflected),
        (BASE, Suffix::Unreflected),
        (BIG, Suffix::Reflected),
        (BIG, Suffix::Unreflected),
    ] {
        let restart = measure(records, suffix, 1);
        describe(
            &mut report,
            &format!("{records} RECORDS, {}", suffix.short()),
            &restart,
        );
        report.say(load_line());
        arms.push(restart);
    }

    // BEFORE ANY NUMBER IS READ: the counting allocator is installed, the kernel counter is
    // readable, and every arm's fixture can express what it is being asked about. Without the
    // first, every allocation column is a placeholder and every ratio is zero against zero.
    for arm in &arms {
        assert!(
            arm.cost.allocations_were_counted,
            "the counting allocator must be installed for this test to measure anything"
        );
        assert!(
            arm.classified.is_some(),
            "the classified span must have counted something"
        );
        assert!(
            arm.kernel.is_some(),
            "the kernel byte counter must be readable, or the residual below audits nothing"
        );
        assert_fixture_can_express(arm);
    }
    summarise(&mut report, &arms);

    // -----------------------------------------------------------------------------------------
    // WHAT IS HELD. Every claim the header makes, asserted where it was measured.
    // -----------------------------------------------------------------------------------------
    let find = |records: usize, suffix: Suffix| -> &Restart {
        arms.iter()
            .find(|arm| arm.corpus.records == records && arm.corpus.suffix == suffix)
            .expect("arm")
    };

    // THE CONTROL ARM, with its own failure message so it cannot be chosen by accident later.
    for records in [BASE, BIG] {
        let reflected = find(records, Suffix::Reflected);
        assert_eq!(
            reflected.corpus.tail, 0,
            "the CONTROL arm at {records} records is a shard dumped immediately before it went \
             away: it must have no unreflected suffix at all, and it has {}",
            reflected.corpus.tail
        );
        assert_eq!(
            reflected.phase_allocs("index_fold"),
            2,
            "and with nothing to replay the post-replay fold must not run at all -- that is what \
             makes the two regimes different restores rather than two sizes of one; it cost {} \
             allocations at {records} records",
            reflected.phase_allocs("index_fold")
        );
        let unreflected = find(records, Suffix::Unreflected);
        assert_eq!(
            unreflected.corpus.tail,
            records / 2,
            "the TREATMENT arm at {records} records must carry half the store as an unreflected \
             suffix; it carries {}",
            unreflected.corpus.tail
        );
        assert!(
            unreflected.cost.records_behind_checkpoint > 0
                && unreflected.cost.records_behind_checkpoint < unreflected.cost.records_decoded,
            "and the walk must decode log records on BOTH sides of the checkpoint anchor, or its \
             sequence test has one answer by construction: {} of {} at {records} records",
            unreflected.cost.records_behind_checkpoint,
            unreflected.cost.records_decoded
        );
    }

    // THE PHASE TABLE IS A BILL, NOT A SELECTION: five rows, and the part of the span they miss
    // is a FIXED edge cost rather than one that grows with the store.
    for arm in arms.iter() {
        assert_eq!(
            arm.cost.phases.len(),
            5,
            "a restore is five phases; at {} records, {} regime, it reported {:?}",
            arm.corpus.records,
            arm.corpus.suffix.short(),
            arm.cost
                .phases
                .iter()
                .map(|phase| phase.phase.clone())
                .collect::<Vec<_>>()
        );
        let residual = arm.phase_residual_allocs().expect("counted");
        assert!(
            (0..=32).contains(&residual),
            "the allocations no phase row accounts for must be a FIXED edge cost: it is {residual} \
             at {} records in the {} regime, and a residual that grows with the store is a phase \
             boundary sitting past work nobody wrote a row for",
            arm.corpus.records,
            arm.corpus.suffix.short()
        );
    }

    // FLAT, PER STORED RECORD, FROM 20,000 TO 200,000 -- in BOTH regimes. Each of these is a
    // rate a change to the phase behind it has to move.
    let flat = |name: &str, small: f64, large: f64, band: f64| {
        let ratio = large / small.max(f64::MIN_POSITIVE);
        assert!(
            (1.0 - band..=1.0 + band).contains(&ratio),
            "{name} must be flat per stored record from {BASE} to {BIG}: {small:.4} against \
             {large:.4}, a ratio of {ratio:.3}"
        );
    };
    for suffix in [Suffix::Reflected, Suffix::Unreflected] {
        let (small, large) = (find(BASE, suffix), find(BIG, suffix));
        // The fold is a rate only where it RUNS. In the reflected regime it is a fixed 2
        // allocations -- asserted exactly, above -- and a fixed cost divided by ten times
        // the records is a tenth, which is not a rate that moved.
        let phases: &[&str] = match suffix {
            Suffix::Reflected => &["manifest_and_base_index", "publish_and_seed"],
            Suffix::Unreflected => {
                &["manifest_and_base_index", "publish_and_seed", "index_fold"]
            }
        };
        for phase in phases.iter().copied() {
            flat(
                &format!("{phase} allocations ({})", suffix.short()),
                small.per_record(small.phase_allocs(phase)),
                large.per_record(large.phase_allocs(phase)),
                0.10,
            );
        }
        flat(
            &format!("manifest bytes read ({})", suffix.short()),
            small.per_record(small.manifest_bytes_read),
            large.per_record(large.manifest_bytes_read),
            0.10,
        );
        flat(
            &format!("the promotion check's pages walked ({})", suffix.short()),
            small.per_record(small.promote_pages),
            large.per_record(large.promote_pages),
            0.10,
        );
        // THE ENGINE'S OWN VIEW OF WHAT REPLAY READ -- the half of the finding that makes the
        // other half worth reporting. In the unreflected regime it is flat per stored record at
        // both sizes while the kernel's number for the same span is not; in the reflected regime
        // it is ZERO at both sizes while the kernel says the process read tens of megabytes.
        // Neither shape is visible from inside the engine.
        match suffix {
            Suffix::Unreflected => flat(
                "the log store's own bytes_read (unreflected)",
                small.per_record(small.wal_bytes_read),
                large.per_record(large.wal_bytes_read),
                0.10,
            ),
            Suffix::Reflected => {
                assert_eq!(
                    (small.wal_bytes_read, large.wal_bytes_read),
                    (0, 0),
                    "a restore with nothing past its checkpoint reports ZERO log bytes read \
                     through the log store's own counter: {} at {BASE} records and {} at {BIG}",
                    small.wal_bytes_read,
                    large.wal_bytes_read
                );
                for (records, arm) in [(BASE, small), (BIG, large)] {
                    assert!(
                        arm.kernel.expect("io").rchar > arm.corpus.wal_bytes,
                        "while the kernel says the same span read more than the whole log: {} B \
                         against a {} B log at {records} records. A counter reading zero is not \
                         the same thing as a reader that did not run.",
                        arm.kernel.expect("io").rchar,
                        arm.corpus.wal_bytes
                    );
                }
            }
        }
    }

    // THE REFLECTED REGIME IS FLAT ALL THE WAY DOWN, INCLUDING THE KERNEL'S NUMBER. This is the
    // regime that reports a healthy system, and it is asserted separately so the growth below
    // cannot be mistaken for a property of restore in general.
    let (small, large) = (find(BASE, Suffix::Reflected), find(BIG, Suffix::Reflected));
    flat(
        "a reflected restore's allocations",
        small.per_record(small.cost.allocs()),
        large.per_record(large.cost.allocs()),
        0.10,
    );
    flat(
        "a reflected restore's kernel rchar",
        small.per_record(small.kernel.expect("io").rchar),
        large.per_record(large.kernel.expect("io").rchar),
        0.20,
    );

    // AND THE UNREFLECTED REGIME IS NOT. The kernel says a restore with a suffix to replay reads
    // several times more per stored record at ten times the size, while every counter inside the
    // engine says it reads the same. Asserted as a FLOOR on the growth, so a fix that removes it
    // fails here and has to come back and rewrite the header.
    let (small, large) = (find(BASE, Suffix::Unreflected), find(BIG, Suffix::Unreflected));
    let kernel_growth = large.per_record(large.kernel.expect("io").rchar)
        / small.per_record(small.kernel.expect("io").rchar);
    let residual_growth = (large.read_residual_bytes().expect("io") as f64 / BIG as f64)
        / (small.read_residual_bytes().expect("io") as f64 / BASE as f64);
    report.say(format!(
        "\n  WHAT THE ENGINE CANNOT SEE: ten times the store multiplies the kernel's bytes per stored record by \
         {kernel_growth:.2} and the unattributed part of them by {residual_growth:.2}, while the \
         log store's own bytes_read per stored record moves {:.3} -- {} replay windows against {}",
        large.per_record(large.wal_bytes_read) / small.per_record(small.wal_bytes_read),
        small.wal_window_walks,
        large.wal_window_walks,
    ));
    assert!(
        kernel_growth > 3.0,
        "ten times the store must multiply what the kernel hands a replaying restore per stored \
         record by more than three: it multiplied it by {kernel_growth:.2} ({:.1} B/record at \
         {BASE} against {:.1} at {BIG})",
        small.per_record(small.kernel.expect("io").rchar),
        large.per_record(large.kernel.expect("io").rchar),
    );
    assert!(
        residual_growth > 3.0,
        "and the part of it no named reader claims must grow with it: {residual_growth:.2}x"
    );
    assert!(
        large.wal_window_walks >= small.wal_window_walks * 5,
        "the mechanism is the WINDOW COUNT, so it must be the thing that grew: {} windows at \
         {BASE} records against {} at {BIG}",
        small.wal_window_walks,
        large.wal_window_walks
    );
    // NON-VACUOUS: the growth is not an artefact of the small arm reading almost nothing.
    assert!(
        small.kernel.expect("io").rchar > small.attributed_bytes_read(),
        "the small arm must already read more than its rows name, or the ratio above is a \
         division by an empty measurement: {} B against {} B attributed",
        small.kernel.expect("io").rchar,
        small.attributed_bytes_read()
    );
}

/// Every per-record quantity, at both sizes, in both regimes.
fn summarise(report: &mut Report, arms: &[Restart]) {
    let find = |records: usize, suffix: Suffix| -> &Restart {
        arms.iter()
            .find(|arm| arm.corpus.records == records && arm.corpus.suffix == suffix)
            .expect("arm")
    };
    for suffix in [Suffix::Reflected, Suffix::Unreflected] {
        let small = find(BASE, suffix);
        let large = find(BIG, suffix);
        assert_eq!(
            small.corpus.store_path_len, large.corpus.store_path_len,
            "the store path length is held CONSTANT across arms -- allocation counts move with \
             it -- and it is {} characters at {BASE} records against {} at {BIG}",
            small.corpus.store_path_len, large.corpus.store_path_len
        );
        report.say(format!(
            "\n\nPER RECORD, {} -- {BASE} against {BIG} records, store path {} chars in both\
             \n  {:<34} {:>16} {:>16} {:>10}",
            suffix.label(),
            small.corpus.store_path_len,
            "quantity",
            BASE,
            BIG,
            "ratio",
        ));
        let mut row = |name: &str, a: u64, b: u64| {
            let (pa, pb) = (small.per_record(a), large.per_record(b));
            report.say(format!(
                "  {:<34} {:>16.4} {:>16.4} {:>10.3}",
                name,
                pa,
                pb,
                if pa == 0.0 { 0.0 } else { pb / pa },
            ));
        };
        row("allocations", small.cost.allocs(), large.cost.allocs());
        row(
            "allocated bytes",
            small.cost.alloc_bytes(),
            large.cost.alloc_bytes(),
        );
        for phase in [
            "manifest_and_base_index",
            "publish_and_seed",
            "wal_replay",
            "index_fold",
            "open_for_serving",
        ] {
            row(
                &format!("  {phase} allocs"),
                small.phase_allocs(phase),
                large.phase_allocs(phase),
            );
        }
        row(
            "span allocations (independent)",
            small.span_allocs().unwrap_or(0),
            large.span_allocs().unwrap_or(0),
        );
        for class in AllocClass::ALL {
            let (a, b) = (
                small.classified.map(|c| c.classes.row(class).allocs).unwrap_or(0),
                large.classified.map(|c| c.classes.row(class).allocs).unwrap_or(0),
            );
            if a == 0 && b == 0 {
                continue;
            }
            row(&format!("  class {}", class.label()), a, b);
        }
        row("log bytes on disk", small.corpus.wal_bytes, large.corpus.wal_bytes);
        row("log bytes READ by replay", small.wal_bytes_read, large.wal_bytes_read);
        row("log window walks", small.wal_window_walks, large.wal_window_walks);
        row("log pieces on disk", small.corpus.wal_pieces as u64, large.corpus.wal_pieces as u64);
        row("records the walk decoded", small.cost.records_decoded, large.cost.records_decoded);
        row("manifest bytes read", small.manifest_bytes_read, large.manifest_bytes_read);
        row(
            "manifest checksum bytes",
            small.manifest_checksum_bytes,
            large.manifest_checksum_bytes,
        );
        row("index-log bytes read", small.index_log_bytes_read, large.index_log_bytes_read);
        row("page bytes read", small.page_bytes_read, large.page_bytes_read);
        row("page reads", small.page_reads, large.page_reads);
        row(
            "bytes read, attributed",
            small.attributed_bytes_read(),
            large.attributed_bytes_read(),
        );
        row("kernel rchar", small.kernel.map(|io| io.rchar).unwrap_or(0), large.kernel.map(|io| io.rchar).unwrap_or(0));
        row("kernel wchar", small.kernel.map(|io| io.wchar).unwrap_or(0), large.kernel.map(|io| io.wchar).unwrap_or(0));
        row("store bytes on disk", small.corpus.store_bytes, large.corpus.store_bytes);
        row(
            "log + index-log bytes on disk",
            small.corpus.wal_bytes + small.corpus.index_log_bytes,
            large.corpus.wal_bytes + large.corpus.index_log_bytes,
        );
        row("promotion-check pages walked", small.promote_pages, large.promote_pages);
        row(
            "net heap bytes not freed",
            small.net_heap_bytes.max(0) as u64,
            large.net_heap_bytes.max(0) as u64,
        );
        report.say(format!(
            "  {:<34} {:>16} {:>16} {:>10.3}   <- wall ms, an UPPER BOUND on a loaded box",
            "restore wall ms (not per record)",
            small.wall_ms,
            large.wall_ms,
            large.wall_ms as f64 / small.wall_ms.max(1) as f64,
        ));
        report.say(format!(
            "  {:<34} {:>16} {:>16} {:>10.3}   <- peak resident KiB during the restore",
            "peak RSS KiB (not per record)",
            small.peak_rss_kb,
            large.peak_rss_kb,
            large.peak_rss_kb as f64 / small.peak_rss_kb.max(1) as f64,
        ));
        report.say(format!(
            "  {:<34} {:>16.4} {:>16.4}              <- INDEPENDENT residual, bytes per record",
            "read residual B/record",
            small.read_residual_bytes().unwrap_or(0) as f64 / BASE as f64,
            large.read_residual_bytes().unwrap_or(0) as f64 / BIG as f64,
        ));
    }
}

// ---------------------------------------------------------------------------------------------
// THE ORDINARY-GATE GUARDS
// ---------------------------------------------------------------------------------------------

/// THE INSTRUMENT THE RESIDUAL IS COMPUTED AGAINST CAN SEE A HIT, AND RECOVERS IT EXACTLY.
///
/// A residual of zero from an instrument that cannot see anything is indistinguishable from a
/// residual of zero from one that can. This plants known quantities in front of the kernel
/// counter and asserts it hands them back to the byte.
#[test]
fn the_kernel_byte_instrument_recovers_a_planted_read_exactly() {
    let dir = tempfile::tempdir().expect("tempdir");
    const PLANTED_SMALL: usize = 1_000_003;
    const PLANTED_LARGE: usize = 3_000_009;
    let small_path = dir.path().join("planted-small");
    let large_path = dir.path().join("planted-large");
    std::fs::write(&small_path, incompressible(PLANTED_SMALL, 1)).expect("plant");
    std::fs::write(&large_path, incompressible(PLANTED_LARGE, 2)).expect("plant");

    // THE CONTROL, FIRST: a span with nothing in it reads zero. If this were not zero the two
    // recoveries below would each carry an unnamed offset and "exactly" would mean nothing.
    let (_, empty) = kernel_io_span(|| ());
    let empty = empty.expect("/proc/self/io must be readable for the residual to audit anything");
    assert_eq!(
        empty.rchar, 0,
        "an empty span must read zero bytes once this instrument's own reading is subtracted; \
         it read {}",
        empty.rchar
    );

    let (small_len, small_io) = kernel_io_span(|| std::fs::read(&small_path).expect("read").len());
    let (large_len, large_io) = kernel_io_span(|| std::fs::read(&large_path).expect("read").len());
    let (small_io, large_io) = (small_io.expect("io"), large_io.expect("io"));
    println!(
        "  planted {PLANTED_SMALL} B -> kernel {} B;  planted {PLANTED_LARGE} B -> kernel {} B;  \
         empty span {} B",
        small_io.rchar, large_io.rchar, empty.rchar
    );
    assert_eq!(
        (small_len, large_len),
        (PLANTED_SMALL, PLANTED_LARGE),
        "the plants must be the sizes they claim, or the recovery below compares two wrong \
         numbers that agree"
    );
    assert_eq!(
        small_io.rchar, PLANTED_SMALL as u64,
        "the kernel counter must recover a planted {PLANTED_SMALL} B read exactly; it reported {}",
        small_io.rchar
    );
    assert_eq!(
        large_io.rchar, PLANTED_LARGE as u64,
        "and a planted {PLANTED_LARGE} B read exactly; it reported {}",
        large_io.rchar
    );
    // And the DIFFERENCE of the two, which survives any fixed offset the two spans might share.
    assert_eq!(
        large_io.rchar - small_io.rchar,
        (PLANTED_LARGE - PLANTED_SMALL) as u64,
        "three times the planted bytes must move the counter by exactly the difference"
    );
    // The write side of the same instrument, planted the same way.
    let (_, written) = kernel_io_span(|| {
        std::fs::write(dir.path().join("planted-write"), incompressible(PLANTED_SMALL, 3))
            .expect("plant")
    });
    let written = written.expect("io");
    assert_eq!(
        written.wchar, PLANTED_SMALL as u64,
        "and the write counter must recover a planted {PLANTED_SMALL} B write exactly; it \
         reported {}",
        written.wchar
    );
}

/// THE TWO REGIMES ARE DIFFERENT RESTORES, AND THE CONTROL ARM SAYS SO BY NAME.
///
/// The knob is how much of the log is unreflected when the shard comes back. A store dumped
/// immediately before it went away has nothing past the checkpoint; one that took half its writes
/// after the checkpoint has a long suffix to replay. A guard that measured only one of them would
/// report a healthy system in the regime that happens to be flat.
#[test]
fn a_reflected_suffix_and_an_unreflected_one_are_different_restores() {
    let reflected = measure(GATE, Suffix::Reflected, 1);
    let unreflected = measure(GATE, Suffix::Unreflected, 1);
    let mut report = Report::open("restore-large-regimes");
    describe(&mut report, &format!("{GATE} RECORDS, reflected"), &reflected);
    describe(&mut report, &format!("{GATE} RECORDS, unreflected"), &unreflected);
    report.say(load_line());

    // The fixture can express what is being asked of it, in BOTH arms.
    assert_fixture_can_express(&reflected);
    assert_fixture_can_express(&unreflected);

    // THE CONTROL ARM, ASSERTED WITH ITS OWN FAILURE MESSAGE so it cannot be chosen by accident
    // later: the reflected arm really does have nothing past its checkpoint.
    assert_eq!(
        reflected.corpus.tail, 0,
        "the CONTROL arm is a shard dumped immediately before it went away: it must have no \
         unreflected suffix at all, and it has {} records of one",
        reflected.corpus.tail
    );
    assert_eq!(
        (reflected.cost.records_decoded, reflected.cost.records_behind_checkpoint),
        (0, 0),
        "and so the walk in the control arm decodes NOTHING -- not 'nothing new', nothing at \
         all: it decoded {} log records, {} of them already covered by the checkpoint. Asserted \
         as the pair, because a census that reads 0 of 0 would satisfy 'all of them are already \
         durable' without the walk having looked at anything",
        reflected.cost.records_decoded,
        reflected.cost.records_behind_checkpoint
    );

    // THE TREATMENT ARM: half the store is past the checkpoint, and the walk's sequence test --
    // the dimension the decision keys on -- has BOTH answers in front of it rather than one.
    assert_eq!(
        unreflected.corpus.tail,
        GATE / 2,
        "the treatment arm must carry half the store as an unreflected suffix; it carries {}",
        unreflected.corpus.tail
    );
    assert!(
        unreflected.cost.records_behind_checkpoint > 0
            && unreflected.cost.records_behind_checkpoint < unreflected.cost.records_decoded,
        "and the walk must decode records on BOTH sides of the checkpoint anchor -- STRICTLY \
         between none and all of them -- or the sequence test it makes has one answer by \
         construction and a census that answered it the same way every time would read exactly \
         like a census that answered it correctly: {} of {} were already covered",
        unreflected.cost.records_behind_checkpoint,
        unreflected.cost.records_decoded
    );
    // A log RECORD is one seeded batch, so a tail of `tail` records is `tail / BATCH` of them.
    let applied = unreflected.cost.records_decoded - unreflected.cost.records_behind_checkpoint;
    assert!(
        applied >= (unreflected.corpus.tail / BATCH) as u64,
        "the treatment arm must actually replay its suffix: the walk decoded {} log records, {} \
         of them past the checkpoint, for a {} record tail written {BATCH} to a log record",
        unreflected.cost.records_decoded,
        applied,
        unreflected.corpus.tail
    );

    // BOTH arms recovered from the checkpoint they were given, rather than from nothing.
    for arm in [&reflected, &unreflected] {
        assert!(
            arm.replayed_from > 0 && arm.replayed_from == arm.corpus.anchor(),
            "the restore must start from the checkpoint's own anchor ({}), not from {}",
            arm.corpus.anchor(),
            arm.replayed_from
        );
    }
}

/// THE CHECKPOINT PICK TAKES THE MAXIMUM OVER ANCHORS THAT ARE ACTUALLY DIFFERENT.
///
/// The load chooses which durable checkpoint to rebuild the shard from by taking a maximum over
/// every manifest's `wal_sequence`. A fixture whose manifests all carried the same anchor could
/// not tell that maximum from a constant, from a minimum, or from "whichever was listed first",
/// so this one asserts the three anchors are DISTINCT in exactly the field the pick compares
/// before asserting which of them the restore recovered from.
#[test]
fn the_checkpoint_pick_takes_the_highest_of_three_distinct_anchors() {
    let restart = measure(GATE, Suffix::Reflected, 3);
    let mut report = Report::open("restore-large-checkpoint-pick");
    describe(&mut report, &format!("{GATE} RECORDS, three checkpoints"), &restart);
    report.say(load_line());
    assert_fixture_can_express(&restart);

    let anchors = &restart.corpus.manifest_anchors;
    assert_eq!(
        anchors.len(),
        3,
        "the fixture must take three checkpoints; it took {:?}",
        anchors
    );
    assert_eq!(
        restart.corpus.manifest_files, 3,
        "and leave all three on disk for the pick to read: {} file(s)",
        restart.corpus.manifest_files
    );
    let distinct: std::collections::BTreeSet<u64> = anchors.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        anchors.len(),
        "the three checkpoints must anchor at DISTINCT wal_sequences -- the field the pick takes \
         a maximum over -- or a pick that took the minimum, or the first, would read exactly like \
         a pick that took the maximum: {anchors:?}"
    );
    let highest = anchors.iter().copied().max().expect("three anchors");
    assert!(
        highest > anchors.iter().copied().min().expect("three anchors"),
        "and the maximum must not be the minimum: {anchors:?}"
    );
    assert_eq!(
        restart.replayed_from, highest,
        "the restore must recover from the HIGHEST anchor of the three; it recovered from {} and \
         the anchors were {anchors:?}",
        restart.replayed_from
    );
    // And having read three, it used one: the other two were read whole, parsed whole and
    // re-serialised whole for one integer each.
    assert_eq!(
        restart.manifest_file_reads, 3,
        "every manifest on disk is read whole to pick one of them: {} read(s)",
        restart.manifest_file_reads
    );
    assert!(
        restart.manifest_checksum_bytes >= restart.manifest_bytes_read * 9 / 10,
        "and put back through serde to verify a checksum: {} B re-serialised against {} B read",
        restart.manifest_checksum_bytes,
        restart.manifest_bytes_read
    );
}
