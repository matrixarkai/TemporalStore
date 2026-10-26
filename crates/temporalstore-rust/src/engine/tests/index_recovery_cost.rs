// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A RECOVERY READS AT EACH INDEX-LOG SPACE BUDGET -- THE OTHER HALF OF THE TRADE.
//!
//! # THE NUMBER THIS MODULE EXISTS TO STATE
//!
//! The parent module establishes that the dump threshold IS a space budget: it is
//! `base_index / divisor`, so the index log may stand at `1/divisor` of the base,
//! `b = 1 + 1/divisor`, and the measured write amplification sits on `a <= b/(b-1)` to within 1%
//! at every point of an eight-fold sweep of `b`. There is no slack left in that mechanism, so the
//! only remaining lever is the constant -- and nobody had measured what the other side of the
//! trade costs. This is that side, on one corpus, at five budgets:
//!
//! ```text
//!   4,000 objects, shipped routing range 0..1023, scattered keys, 1 KiB rolling threshold
//!
//!   divisor      b       a   a/bound   index log      base      WAL   INDEX-LOG     NAMED  wall
//!                                       on disk      read      read     read        read    ms
//!        2  1.3563   3.810    1.0009     143,669   286,242   763,392   143,669  1,193,303  1671
//!        4  1.2125   5.715    1.0016      61,768   243,537   566,784    61,768    872,089   405
//!        8  1.1159   9.658    1.0030      28,807   223,389   493,056    28,807    745,252   252
//!       16  1.0609  17.511    1.0056      14,409   213,124   468,480    14,409    696,013   249
//!       32  1.0347  30.083    1.0099       7,245   208,503   443,904     7,245    659,652   228
//!
//!   the same five stores with NOTHING past the checkpoint -- the reflected control
//!        2  1.0002      --        --          69   286,242     3,760        69    290,071   193
//!        4  1.0003      --        --          69   243,537     3,796        69    247,402   147
//!        8  1.0003      --        --          69   223,389     3,706        69    227,164   124
//!       16  1.0003      --        --          69   213,124     3,736        69    216,929   135
//!       32  1.0003      --        --          69   208,503     3,776        69    212,348   128
//! ```
//!
//! # THE FINDING: A RECOVERY READS THE WHOLE INDEX LOG, AND IT IS NOT THE FOLD THAT READS IT
//!
//! `index_log_replay_reach` establishes that the DEFAULT load path does not FOLD the index log:
//! `load_shard_with` branches on `wal_single_barrier()`, which is `!wal_legacy_recovery()` and so
//! TRUE unless an operator sets `TS_WAL_LEGACY_RECOVERY`, and that arm calls
//! `load_index_base_only` -- `fold_deltas: false`. `the_fold_reaching_loaders_stay_gated` guards
//! the call-site set that keeps it closed. Both are correct and neither is re-derived here.
//!
//! DOES NOT FOLD IS NOT DOES NOT READ, and nobody had asked the second question. The default arm
//! reads EVERY BYTE of the index log, in the same `load_shard_with` call, through
//! `latest_slab_catalog`: the manifest-conformance seed that recovers the slab catalog. The
//! column above is exact rather than approximate -- the bytes charged inside the read EQUAL the
//! bytes on disk, in all ten readings, to the byte:
//!
//! ```text
//!   143,669 = 143,669    61,768 = 61,768    28,807 = 28,807    14,409 = 14,409
//!     7,245 =   7,245        69 =     69  (the reflected control, at all five budgets)
//! ```
//!
//! AND THE OBVIOUS FIX WAS MEASURED AND DOES NOT WORK, WHICH IS WHY THE READ IS A REAL COST AND
//! NOT AN OVERSIGHT. `latest_slab_catalog` wants the LAST record in the log carrying a slab
//! catalog, so it calls `for_each_delta_record`, which passes base anchor 0 -- and a zero anchor
//! declines nothing. The piece-name decline
//! `index_log_scale::what_an_index_log_replay_reads_at_two_corpus_sizes` measured, and
//! `the_load_path_fold_is_the_one_that_declines` placed, is on the OTHER entry point,
//! `for_each_delta_record_above_anchor` -- so the first thing to try is handing the catalog walk
//! the base anchor. IN THE STATE A BUDGET'S CRASH LEAVES, THAT BUYS NOTHING. The sweep behind the
//! last dump has already dropped every piece the base reflects, so what remains IS the
//! unreflected suffix and there is no piece left for an anchor to decline: driven over the same
//! files, the fold's shape reads the same 25,885 B and hands back the same 19 records as the
//! anchor-less walk, while a control anchor above every record in the log declines pieces and
//! reads strictly less. `the_whole_log_read_at_recovery_is_the_slab_catalog_walk` is those three
//! arms, and the control is there because "the decline declined nothing" and "the decline is
//! broken" read identically without it.
//!
//! So the budget's footprint is read back IN FULL at every restart, irreducibly under this
//! reader, and the relationship is a one-for-one identity rather than a slope with a residual:
//!
//! ```text
//!   index-log bytes a recovery reads  =  (b - 1) * base index on disk
//! ```
//!
//! What WOULD make it smaller is a different property of the same walk: it needs only the LAST
//! record carrying a catalog and reads every record in the log to find it. That is not attempted
//! here; what is recorded is that the budget is currently priced against an unbounded read.
//!
//! # AND THE WAL TAIL MOVES WITH THE BUDGET TOO, ON TOP OF THAT
//!
//! The dump the threshold fires is also what advances the DURABLE RECOVERY WATERMARK --
//! `flush_shard_index` fsyncs every block before advancing it, and
//! `durable_recovery_bucket_dump_manifest_at` is what the default arm reads. So a looser budget
//! dumps less often and leaves a longer unreflected write-ahead tail, and the replay reads
//! 1.69x to 2.91x the bytes the log holds on disk because each window re-reads the pieces behind
//! it -- the term `restore_large_scale` priced at 89% of a 200,000-record restart and measured
//! going 6.70x per stored record for a 10x store. Here the WAL read runs 443,904 B at the tight
//! end to 763,392 B at the loose one, off a log that is 262,144 B on disk in EVERY arm.
//!
//! # THE SAME BUDGET AT A LARGER STORE, AND WHICH TERM IS SCALE-FREE
//!
//! `the_recovery_a_budget_buys_grows_with_the_store` holds the shipped divisor and varies the
//! corpus. The index-log term is scale-free BY CONSTRUCTION and measured to be so -- the
//! threshold is a fraction of the base index, so the footprint is `1/divisor` of it at any size --
//! while the terms it is traded against are not:
//!
//! ```text
//!   objects   base index   index log   log/base    WAL read      NAMED   idx share   wall ms
//!     4,950      223,389      28,807     0.1290     493,056    745,252       3.9%        272
//!    20,700    1,156,918     145,160     0.1255     763,392  2,065,470       7.0%      1,167
//!             (5.179x)      (5.039x)               (1.548x)   (2.772x)
//! ```
//!
//! So a looser budget costs a FIXED FRACTION of the base image in index-log reads at every size,
//! and the base image is itself the largest single term once a store is real: 56% of what the
//! larger recovery reads, against 30% at the smaller one. WHAT DOMINATES therefore has two answers
//! and they are in different units. In BYTES it is the base image at a real store size and the WAL
//! replay at fixture scale. In TIME it is the base image's decode at every size -- 454-517 ms of
//! the 590-1,167 ms above -- while the whole-log read the budget pays for is 73-92 ms of it. The
//! index-log fold is not on this path and reads nothing.
//!
//! # THE SHAPE OF THE WHOLE TRADE, IN ONE LINE EACH
//!
//! ```text
//!   writes:   a  =  b/(b-1)  ~=  divisor + 1          paid on every byte of index logged
//!   recovery: (b-1) * base of index log, read whole   paid once per restart, plus a WAL tail
//!                                                     that grows with the same threshold
//! ```
//!
//! The two are reciprocal in `(b-1)`, so their product is near constant and the measurement
//! identifies no interior optimum on its own. What breaks the symmetry is that the recovery side
//! is the one with a term that is superlinear in its own size, and it is paid at the moment a node
//! is trying to return to service.
//!
//! # EVERY FIGURE'S DENOMINATOR, AND WHICH SURFACE IT IS ON
//!
//! - `base_index` -- `shard-{id}.index.json` on disk. The `TSIDX\x01` container, which is a ZSTD
//!   FRAME: a COMPRESSED size that cannot name a field, and one that moves in its third decimal
//!   place between runs because the index carries wall-clock values. `load_index_base_only` reads
//!   it with one whole-file `fs::read`, so the file's length IS what that reader read; it is
//!   reported as the base-image term rather than estimated.
//! - `index_log` -- `indexlogs/shard-{id}.indexlog*.bin`, the store's own binary framing over a
//!   json payload. UNCOMPRESSED, so it is not comparable byte for byte with the base index. The
//!   only ratio taken between the two is the space budget `b`, which is the quantity the mechanism
//!   is defined in.
//! - `wal` -- `wals/`, which lives UNDER the index root (`with_cache_block_store_and_index_dir`).
//!   The parent module excludes it from the write numerator by PATH for that reason and reports it
//!   beside; here it is a term under measurement, so the same path split is used in the other
//!   direction and the classes are asserted to PARTITION the root.
//! - what a recovery READ is never a directory walk. It is five readers, each with a counter
//!   charged INSIDE its own read: the base image's own file length,
//!   `wal_piece_read_counts_on_this_thread`, `index_log_piece_read_counts_on_this_thread`,
//!   `bucket_dump_manifest_io_counts` and the block store's `stats().bytes_read`. The two logs'
//!   own `stats().bytes_read` is NOT used as a cost: it counts what a scan HANDS BACK, and here
//!   the index log hands back 0 while reading 143,669 B, and the WAL hands back 112,320 B while
//!   reading 763,392. Both are printed, so the difference stays visible.
//!
//! # THE SECOND BOOK, TWICE
//!
//! - THE CRASH STATE: this module's own directory walk against the two stores' own byte counters,
//!   `log_len_bytes` and `probe_all_segment_bytes`, asserted EQUAL TO THE BYTE rather than close.
//! - WHAT THE RECOVERY READ: the kernel. `/proc/thread-self/io`'s `rchar` across the same span,
//!   less the five named readers, is the residual -- a total this module does not maintain, so it
//!   cannot be a sum of the rows it audits. It comes back at 52 B in every reflected arm and
//!   417-440 B in every at-budget arm: 0.04% of the span, flat, and the size of the probe's own
//!   handful of `statx` calls rather than a reader nobody wrote down.
//!   `the_recovery_byte_instrument_recovers_a_planted_read_exactly` plants reads of two known
//!   lengths in front of the same instrument and requires them back exactly, and requires an empty
//!   span to read exactly zero, so that residual is a reading rather than an identity.
//!
//! # THE CONTROLS
//!
//! - THE REFLECTED ARM. Every arm is recovered TWICE off the same store: once straight after the
//!   dump the round fired, with nothing accrued since, and once at the budget's high-water mark.
//!   An idle store holds no tail whatever the threshold says, so its WAL read must not move with
//!   the budget -- it runs 3,706 to 3,796 B across the sweep, a 90-byte spread against the
//!   319,488 B the at-budget arms move across. A measurement taken only in that regime would
//!   report a budget with almost no recovery cost at all, which is the fixture a careless run
//!   picks.
//! - THE IDLE CONTROL in its literal form: the index root is walked twice across an interval in
//!   which nothing is written and the two walks must agree file by file, so a byte that appears
//!   after the accrual is attributable to the accrual.
//! - THE BUDGET IS ASSERTED REACHED before anything is reported: the measured `(base + log)/base`
//!   at the crash state has to be the `1 + 1/divisor` the arm claims. An arm that stopped short
//!   measures a footprint the budget did not choose.
//! - THE AMORTISED REGIME, by the parent's `assert_amortised_regime` on the shipped routing range
//!   `0..1023` -- 7.81 objects a bucket, not the one-per-bucket shape `0..u32::MAX` leaves.
//! - THE RECOVERY SERVED THE CORPUS. A cheap recovery and a lossy one read the same, so every arm
//!   probes the recovered store across the whole key range and reads the WITHHELD ordinal at the
//!   same moment, which has to come back absent.
//! - AND ONE EXPECTATION THAT WAS WRONG, KEPT HERE RATHER THAN QUIETLY DROPPED: the page term was
//!   first required NON-ZERO, on the argument that a load warms the cache. It is ZERO at every
//!   budget, in both regimes, at both store sizes. The expectation was wrong, not the instrument;
//!   see the comment at the term itself.
//!
//! # WHAT IS ASSERTED AND WHAT IS ONLY PRINTED
//!
//! `LAST_REPLAY_WATERMARK`, `restore_phase_probe`'s record counts and `probe::fold_pieces_declined`
//! are PROCESS-GLOBAL, and this suite runs many threads at once, so a neighbouring load can
//! overwrite them between this load returning and this module reading them. They are printed and
//! never asserted. Everything asserted comes from a per-thread counter, a per-store counter or
//! this module's own walk. The watermark the tail is computed from is this module's own, derived
//! from the files by the same rule the default arm uses, and the engine's record is printed beside
//! it -- the two agreed in all ten readings of the sweep.
//!
//! # WHAT PLANTING EACH CLAUSE SAID ABOUT IT
//!
//! Ten plants, one clause at a time, each a production mutation a real regression could produce.
//! Seven CAUGHT. Three did not, and what they say is recorded here rather than left out:
//!
//! - THE CLAUSE WITH REDUNDANT SUPPORT. "A looser budget costs more to recover" survives the loss
//!   of `replay_start_after_sequence`'s piece skip -- planted, the tree still passed. The ordering
//!   across budgets has TWO maintainers: where in the retained log a replay starts, and how many
//!   windows the records it applies need. Removing one leaves the other, so a single-removal plant
//!   cannot isolate that clause, and "not caught" here means REDUNDANT rather than wrong.
//! - A PLANT THAT MADE THE FIXTURE QUADRATIC INSTEAD OF FAILING. Making `index_log_total_bytes`
//!   report the active piece -- the regression `log_len_bytes` documents -- also starves the dump
//!   cadence, which reads that same function, so the round never fires and the fixture writes
//!   100,000 records against a growing directory listing. The clause is defended by the drift
//!   planted inside `log_len_bytes` alone instead, which the cadence does not read: that one is
//!   CAUGHT, by the walk-against-the-counter assertion, at one byte of drift.
//! - A VOID, WHICH IS NOT A NEGATIVE. The checkpoint pick (`durable_recovery_bucket_dump_manifest_at`)
//!   cannot be reached from this fixture at all: it writes NO bucket dump manifests, which the
//!   manifest reader's own count says -- 0 file reads in all ten readings, in both regimes. So the
//!   reflected arm's idleness does not rest on the manifest pick; it rests on the base index's own
//!   anchor and on the WAL reclaim behind the dump. Planting THAT reclaim away is CAUGHT.
//!
//! Wall time and the per-phase microseconds are printed because a node's return to service is
//! measured in seconds and nobody had the number. NOTHING IS ASSERTED ON THEM: this box carries
//! other tenants, `/proc/loadavg` ran from 1.59 to 4.94 across the five arms above, and the same
//! round has been seen at 490 ms and at 1,077 ms. Every assertion in this file rests on a count.
#![allow(clippy::all)]
use super::*;

/// The store size the budget sweep runs, and the parent budget sweep's size -- so the `a` and `b`
/// measured per arm here are on the same corpus as the published write-side table, and the two
/// halves of the trade are one table rather than two.
const SWEEP_OBJECTS: usize = 4_000;

/// The second size, for the shape question. A dimensionless budget needs a RANGE of budgets and
/// not a large absolute store, which is why the sweep is at one size; what a second size answers
/// is whether the recovery term the budget buys grows with the store as well as with the budget.
const LARGE_OBJECTS: usize = 16_000;

/// Key reads spread across the whole corpus on every arm. A recovery that came back holding only
/// the dumped half reads exactly like a cheap one, so the SPREAD matters more than the count.
const PROBE_READS: usize = 64;

/// Which recovery is being measured. Two regimes rather than two fixtures: a store with nothing
/// past its checkpoint has no tail to replay WHATEVER the threshold says, which is what makes the
/// reflected arm a control on the budget rather than a smaller version of the same reading.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Regime {
    /// The dump has just fired and nothing has been written since. The idle control.
    Reflected,
    /// The index log stands at the budget's high-water mark -- the worst moment the budget admits,
    /// and the state a crash leaves at the instant before a dump.
    AtTheBudget,
}

impl Regime {
    fn short(self) -> &'static str {
        match self {
            Regime::Reflected => "reflected",
            Regime::AtTheBudget => "at-budget",
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT
// ---------------------------------------------------------------------------------------------

/// One reading of THIS THREAD's kernel read counter, and the length of the text it came from.
///
/// The length is carried because taking the reading is itself a read: the span below subtracts the
/// bytes of its own opening sample, which is what makes "exactly" available to the planted-read
/// control.
#[derive(Clone, Copy, Debug)]
struct KernelRead {
    rchar: u64,
    own_bytes: u64,
}

/// `/proc/thread-self/io`, not `/proc/self/io`.
///
/// A restore runs on the thread that called it; this suite runs many threads at once. The process
/// counter would charge this span whatever a neighbouring test happened to read, and taking the
/// total from outside the rows it audits is the whole point of the residual.
fn kernel_read_now() -> Option<KernelRead> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("rchar:") {
            return Some(KernelRead {
                rchar: rest.trim().parse().ok()?,
                own_bytes: text.len() as u64,
            });
        }
    }
    None
}

/// Bytes the kernel charged this thread across `work`, with the opening sample's own bytes taken
/// back out.
///
/// `None` when the file cannot be read, which every caller turns into an APPARATUS failure rather
/// than a zero: a residual of zero and a residual that was never measured read identically.
fn kernel_read_span<T>(work: impl FnOnce() -> T) -> (T, Option<u64>) {
    let before = kernel_read_now();
    let value = work();
    let after = kernel_read_now();
    let charged = before
        .zip(after)
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.own_bytes));
    (value, charged)
}

// ---------------------------------------------------------------------------------------------
// THE CRASH STATE
// ---------------------------------------------------------------------------------------------

type Tree = std::collections::BTreeMap<std::path::PathBuf, (u64, std::time::SystemTime)>;

/// Total length of every write-ahead log piece in a snapshot, by the same path split the parent
/// module excludes the log with -- used here in the other direction.
fn wal_total(tree: &Tree) -> u64 {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::WriteAheadLog)
        .map(|(_, (len, _))| *len)
        .sum()
}

fn base_total(tree: &Tree) -> u64 {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::Base)
        .map(|(_, (len, _))| *len)
        .sum()
}

fn other_total(tree: &Tree) -> u64 {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::Other)
        .map(|(_, (len, _))| *len)
        .sum()
}

/// Index-log pieces in a snapshot, counted off the filesystem rather than asked of the store.
fn log_pieces(tree: &Tree) -> usize {
    tree.iter()
        .filter(|(path, _)| classify(path) == IndexFile::Log)
        .count()
}

/// The watermark a recovery over these files WILL replay above, computed from the files by the
/// same rule `load_shard_with`'s default arm uses: the base index's own anchor, raised by the
/// newest durable dump manifest when that is ahead of it.
///
/// This module's OWN book. The engine records its decision in `LAST_REPLAY_WATERMARK`, which is
/// process-global and therefore not assertable under a parallel suite; it is printed beside this.
fn durable_watermark(dir: &std::path::Path) -> u64 {
    let index_dir = dir.join("indexes");
    let base = std::fs::read(index_dir.join(format!("shard-{SHARD}.index.json")))
        .ok()
        .and_then(|bytes| crate::engine::decode_index_bytes(&bytes).ok())
        .and_then(|state| state.applied_wal_sequence)
        .unwrap_or(0);
    match crate::engine::bucket_dump_io::durable_recovery_bucket_dump_manifest_at(&index_dir, SHARD)
    {
        Some(manifest) if manifest.wal_sequence > base => manifest.wal_sequence,
        _ => base,
    }
}

/// Copy a store directory, so the crash state can be recovered from WITHOUT being consumed.
///
/// A recovery is not read-only -- the load settles and stamps the routing range -- so recovering
/// twice from one directory would measure the second recovery against a store the first changed.
/// Both regimes of one arm are recovered off copies of the same files, which is what lets the two
/// readings be compared at all.
fn copy_tree(from: &std::path::Path, to: &std::path::Path) -> u64 {
    std::fs::create_dir_all(to).expect("APPARATUS: the crash-state copy needs a directory");
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(from)
        .expect("APPARATUS: the crash state must be readable to be copied")
        .flatten()
    {
        let src = entry.path();
        let dst = to.join(entry.file_name());
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            bytes += copy_tree(&src, &dst);
        } else {
            bytes += std::fs::copy(&src, &dst).expect("APPARATUS: a crash-state file must copy");
        }
    }
    bytes
}

/// An engine over `dir` that has NOT loaded the shard yet.
///
/// The parent's `shipped_range_engine` constructs and loads in one call, and the load is the span
/// under measurement here, so it has to be issued separately. The range is the same shipped
/// `0..1023` and is named rather than defaulted, for the same reason the parent names it.
fn unloaded_engine(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache-recover"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

/// What the store held when the engine went away, and what the recovery over it read.
#[derive(Debug, Clone)]
struct Recovery {
    regime: Regime,
    divisor: u64,
    objects: usize,
    // ---- THE CRASH STATE, by the file each byte lives in
    /// `shard-{id}.index.json`. The `TSIDX\x01` container -- a zstd frame, a compressed size.
    base_index_bytes: u64,
    /// `indexlogs/`, the store's binary framing over json. Uncompressed.
    index_log_bytes: u64,
    index_log_pieces: usize,
    /// `wals/`, which lives under the index root and is no part of any index figure.
    wal_bytes: u64,
    // ---- WHAT THE RECOVERY READ, by reader, each counter charged inside its own read
    base_image_read: u64,
    manifest_bytes_read: u64,
    manifest_file_reads: u64,
    wal_read_bytes: u64,
    wal_reads: u64,
    index_log_read_bytes: u64,
    index_log_reads: u64,
    page_read_bytes: u64,
    page_reads: u64,
    /// The two stores' OWN `bytes_read`, which counts what a scan handed back rather than what it
    /// read off disk. Printed beside the physical counters so the difference stays visible; never
    /// used as a cost.
    wal_handed_back: u64,
    index_log_handed_back: u64,
    // ---- THE SECOND BOOK
    kernel_rchar: Option<u64>,
    // ---- WHAT THE REPLAY HAD TO DO
    /// This module's own watermark, from the files.
    watermark: u64,
    /// The engine's own record of the same decision. PROCESS-GLOBAL, printed, never asserted.
    engine_watermark: u64,
    wal_last_sequence: u64,
    /// Process-global, printed, never asserted.
    records_decoded: u64,
    records_behind_checkpoint: u64,
    /// Pieces a fold declined by name. Only `fold_index_log_deltas` passes a non-zero anchor, so
    /// this moves for no other entry point -- which is why it is the column that says the whole-log
    /// read was NOT the delta fold. PROCESS-GLOBAL, printed, never asserted.
    fold_pieces_declined: u64,
    replay_scans: u64,
    // ---- TIME. REPORTED, NOT CLAIMED.
    wall_ms: u64,
    nanos_base: u64,
    /// `publish_and_seed`, the phase that holds `latest_slab_catalog` -- the whole-log read.
    nanos_seed: u64,
    nanos_replay: u64,
    nanos_fold: u64,
    nanos_total: u64,
    // ---- AND WHETHER IT CAME BACK WITH THE DATA
    probed: usize,
    /// Of those positions, the ones the fixture actually wrote. COUNTED in the loop.
    live: usize,
    readable: usize,
    withheld_absent: bool,
}

impl Recovery {
    /// Every reader this module names. Never summed into the residual's total.
    fn named(&self) -> u64 {
        self.base_image_read
            + self.manifest_bytes_read
            + self.wal_read_bytes
            + self.index_log_read_bytes
            + self.page_read_bytes
    }

    /// THE SUBJECT: the kernel's total less the rows above.
    fn residual(&self) -> i64 {
        self.kernel_rchar.unwrap_or(0) as i64 - self.named() as i64
    }

    /// The space budget this crash state actually stood at.
    fn b(&self) -> f64 {
        assert!(
            self.base_index_bytes > 0,
            "APPARATUS: the {} arm at divisor {} left no base index on disk, so there is nothing \
             for the space figure to be a multiple of",
            self.regime.short(),
            self.divisor
        );
        (self.base_index_bytes + self.index_log_bytes) as f64 / self.base_index_bytes as f64
    }

    /// WAL records the recovery had to replay: the tail the budget left unreflected, from this
    /// module's own watermark.
    fn tail_records(&self) -> u64 {
        self.wal_last_sequence.saturating_sub(self.watermark)
    }
}

/// Write in batches until the PRODUCTION cadence would fire, and stop there WITHOUT firing it.
///
/// That instant is the budget's high-water mark and the worst moment a crash can pick, which is
/// what a budget has to be priced at. The gate is asked on every batch below the threshold and
/// must REFUSE, exactly as `run_round` asks it, so a gate that fires early is an apparatus failure
/// here too. It is deliberately NOT asked at or above the threshold, because asking is what fires
/// it; the pure predicate `should_dump_index_catalog` is asserted instead, which is the same
/// decision without the dump.
fn accrue_to_the_budget(
    engine: &TemporalEngine,
    next_ordinal: &mut usize,
    shape: KeyShape,
    divisor: u64,
    base_index_bytes: u64,
) -> u64 {
    let store = engine.index_log_store();
    let threshold_bytes = crate::index_log::effective_index_dump_threshold_bytes(
        FLOOR_BYTES,
        base_index_bytes,
        divisor,
    );
    assert!(
        threshold_bytes > FLOOR_BYTES,
        "FIXTURE: the accrual at divisor {divisor} held out for {threshold_bytes} B, which is the \
         configured floor {FLOOR_BYTES} B rather than a threshold read off the {base_index_bytes} \
         B base index. The relative term does not bind here, so the budget is not what decides \
         this tail"
    );
    for batch in 0..MAX_BATCHES {
        let accrued = store.undumped_len_since_dump(SHARD);
        if accrued >= threshold_bytes {
            assert!(
                crate::index_log::should_dump_index_catalog(accrued, threshold_bytes),
                "APPARATUS: {accrued} B accrued against a {threshold_bytes} B threshold and the \
                 production predicate says do not dump -- the stopping point and the cadence \
                 disagree, so this tail is not the one the budget chose"
            );
            return threshold_bytes;
        }
        let refused = engine
            .maybe_dump_and_reclaim_with_threshold_and_divisor_for_test(
                SHARD,
                FLOOR_BYTES,
                0,
                0,
                divisor,
            )
            .is_none();
        assert!(
            refused,
            "APPARATUS: the cadence FIRED on {accrued} B accrued, below the {threshold_bytes} B \
             `effective_index_dump_threshold_bytes` says it holds out for -- so the state this \
             accrual leaves is not the budget's high-water mark"
        );
        assert!(
            batch + 1 < MAX_BATCHES,
            "APPARATUS: an accrual at divisor {divisor} never reached its threshold: \
             {MAX_BATCHES} batches of {BATCH} against {threshold_bytes} B, with {accrued} B \
             accrued"
        );
        seed_range(engine, *next_ordinal, *next_ordinal + BATCH, shape);
        *next_ordinal += BATCH;
    }
    unreachable!("the loop above either returns or asserts");
}

/// Recover a crash state and count what the recovery read.
///
/// The engine is CONSTRUCTED first and the counters are taken around the LOAD alone, so nothing
/// the constructor reads -- the slab manifest among it -- is charged to the recovery.
fn recover(
    dir: &std::path::Path,
    regime: Regime,
    divisor: u64,
    objects: usize,
    shape: KeyShape,
) -> Recovery {
    let tree = index_tree(&dir.join("indexes"));
    let base_index_bytes = base_total(&tree);
    let index_log_bytes = log_total(&tree);
    let index_log_pieces = log_pieces(&tree);
    let wal_bytes = wal_total(&tree);
    // THE THREE CLASSES PARTITION THE ROOT. Without this the WAL could be inside the index-log
    // figure, which is how the parent module once read 263,8xx B a round as index.
    assert_eq!(
        base_index_bytes + index_log_bytes + wal_bytes + other_total(&tree),
        tree.values().map(|(len, _)| *len).sum::<u64>(),
        "APPARATUS: the four path classes do not partition the index root, so one of the three \
         figures above is carrying bytes that belong to another"
    );
    assert_eq!(
        base_index_bytes,
        base_index_on_disk(dir),
        "APPARATUS: the walk found {base_index_bytes} B of base index and the direct read of \
         `shard-{SHARD}.index.json` found {} B, so there is more than one file in the Base class \
         and the base-image term names the wrong one",
        base_index_on_disk(dir),
    );
    let watermark = durable_watermark(dir);

    let engine = unloaded_engine(dir);
    crate::engine::reset_bucket_dump_manifest_io_counts();
    let (wal_before, wal_reads_before) = crate::wal::wal_piece_read_counts_on_this_thread();
    let (index_before, index_reads_before) =
        crate::index_log::index_log_piece_read_counts_on_this_thread();
    let pages_before = engine.block_store().stats();
    let started = std::time::Instant::now();

    let (response, kernel_rchar) = kernel_read_span(|| {
        engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: SHARD,
            table_name: "index-recovery-cost".to_string(),
            shard_uri: "local://index-recovery-cost/1".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket: SHIPPED_END_BUCKET,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        })
    });
    let wall_ms = started.elapsed().as_millis() as u64;
    let cost = crate::engine::lifecycle::restore_phase_probe::finish();
    assert!(
        response.status.ok,
        "APPARATUS: the recovery under measurement must succeed, or every number here describes a \
         failure: {:?}",
        response.status
    );

    let (wal_after, wal_reads_after) = crate::wal::wal_piece_read_counts_on_this_thread();
    let (index_after, index_reads_after) =
        crate::index_log::index_log_piece_read_counts_on_this_thread();
    // BEFORE the readback below, which would charge its own page reads to the recovery.
    let pages_after = engine.block_store().stats();
    let manifest = crate::engine::bucket_dump_manifest_io_counts();
    // `stats`, not `raw_stats`: the cached `last_sequence` is set by an APPEND, and a store
    // that only replayed never appended, so `raw_stats` answered 0 here for every arm and the
    // tail it is subtracted from read as zero everywhere. `stats` scans for it, which costs a
    // read -- taken AFTER the counters above are captured, so the scan is not charged to the
    // recovery.
    let wal_stats = engine.write_ahead_log_store().stats(SHARD);
    let index_stats = engine.index_log_store().stats(SHARD);

    let mut readable = 0usize;
    let mut live = 0usize;
    let mut probed = 0usize;
    while probed < PROBE_READS {
        let ordinal = probed * objects / PROBE_READS;
        probed += 1;
        // COUNTED, not derived. The step is `objects / PROBE_READS`, so whether it ever lands on
        // the withheld ordinal depends on the corpus size -- at these sizes it does not, and an
        // expected count of `probed - 1` was wrong by exactly that for every arm.
        if ordinal == WITHHELD_ORDINAL {
            continue;
        }
        live += 1;
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet {
                key: key_text(ordinal, shape),
            },
        });
        let got = matches!(
            &response.response,
            CommandResponse::Bytes { value }
                if value.as_ref().map(|bytes| bytes.len()) == Some(VALUE_LEN)
        );
        if response.status.ok && got {
            readable += 1;
        }
    }
    // THE NEGATIVE CONTROL, same build and same moment as the positive reads above.
    let withheld = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet {
            key: key_text(WITHHELD_ORDINAL, shape),
        },
    });
    let withheld_absent = matches!(
        &withheld.response,
        CommandResponse::Bytes { value } if value.is_none()
    );

    Recovery {
        regime,
        divisor,
        objects,
        base_index_bytes,
        index_log_bytes,
        index_log_pieces,
        wal_bytes,
        base_image_read: base_index_bytes,
        manifest_bytes_read: manifest.bytes_read,
        manifest_file_reads: manifest.file_reads,
        wal_read_bytes: wal_after - wal_before,
        wal_reads: wal_reads_after - wal_reads_before,
        index_log_read_bytes: index_after - index_before,
        index_log_reads: index_reads_after - index_reads_before,
        page_read_bytes: pages_after
            .bytes_read
            .saturating_sub(pages_before.bytes_read),
        page_reads: pages_after.reads.saturating_sub(pages_before.reads),
        wal_handed_back: wal_stats.bytes_read,
        index_log_handed_back: index_stats.bytes_read,
        kernel_rchar,
        watermark,
        engine_watermark: crate::engine::lifecycle::LAST_REPLAY_WATERMARK
            .load(std::sync::atomic::Ordering::SeqCst),
        wal_last_sequence: wal_stats.last_sequence,
        records_decoded: cost.records_decoded,
        records_behind_checkpoint: cost.records_behind_checkpoint,
        fold_pieces_declined: crate::index_log::probe::fold_pieces_declined(),
        replay_scans: wal_stats.scans,
        wall_ms,
        nanos_base: cost.phase("manifest_and_base_index").nanos,
        nanos_seed: cost.phase("publish_and_seed").nanos,
        nanos_replay: cost.phase("wal_replay").nanos,
        nanos_fold: cost.phase("index_fold").nanos,
        nanos_total: cost.nanos(),
        probed,
        live,
        readable,
        withheld_absent,
    }
}

/// Everything that says a recovery reading is a reading of what it claims to be.
fn assert_recovery_can_express_it(reading: &Recovery) {
    let tag = format!("the {} arm at divisor {}", reading.regime.short(), reading.divisor);
    assert!(
        reading.base_index_bytes > 0,
        "FIXTURE: {tag} crashed with no base index on disk, so the recovery has no checkpoint to \
         read and is not the recovery a budget is paid for"
    );
    assert!(
        reading.wal_bytes > 0,
        "APPARATUS: {tag} attributed ZERO bytes to `wals/`. The write-ahead log shares the index \
         root and every write here goes through it, so a zero means the path split has stopped \
         matching anything -- and those bytes are inside the index-log figure, where the parent \
         module once read 263,8xx B a round"
    );
    assert!(
        reading.base_image_read > 0,
        "APPARATUS: {tag} has a zero base-image term, so the one reader every recovery has is \
         missing from the accounting"
    );
    // NO ASSERTION ON THE PAGE TERM, AND THE REASON IS A MEASUREMENT THAT CONTRADICTED AN
    // EXPECTATION. This file first required it NON-ZERO, on the argument that the load warms
    // the cache through `reconcile_secondary_views_from_bucket_index`. It is ZERO -- at every
    // budget, in both regimes, at both store sizes. The expectation was wrong rather than the
    // instrument: a recovery over a base image plus a WAL tail rebuilds the secondary views
    // from the decoded index and from the records it replays, and asks the block store for
    // nothing. The term stays in the accounting by NAME so that a recovery which does start
    // reading pages appears as a row rather than inside the residual, and the residual beside
    // it is what would catch those bytes if the counter were the thing at fault.
    assert!(
        reading.kernel_rchar.is_some(),
        "APPARATUS: {tag} found no rchar line in /proc/thread-self/io, so the second book was \
         never opened and its residual is not a reading"
    );
    assert!(
        reading.live > PROBE_READS / 2,
        "APPARATUS: {tag} probed {} position(s) of which only {} were ordinals the fixture wrote, \
         so the readback below is over almost nothing",
        reading.probed,
        reading.live,
    );
    assert_eq!(
        reading.readable, reading.live,
        "READBACK: {tag} came back serving {} of the {} live ordinals probed across the corpus. A \
         recovery that lost data reads exactly like a cheap one",
        reading.readable,
        reading.live,
    );
    assert!(
        reading.withheld_absent,
        "NEGATIVE CONTROL: {tag} serves the withheld ordinal {WITHHELD_ORDINAL}, which the \
         fixture never wrote -- so a present read anywhere in that readback proves nothing"
    );
}

fn show_recovery(reading: &Recovery) {
    eprintln!(
        "  d={:<3} {:<10} CRASH STATE base={:9} B  log={:8} B in {:3} piece(s)  wal={:9} B   \
         (b={:.4})",
        reading.divisor,
        reading.regime.short(),
        reading.base_index_bytes,
        reading.index_log_bytes,
        reading.index_log_pieces,
        reading.wal_bytes,
        reading.b(),
    );
    eprintln!(
        "        READ base_image={:9} manifest={:8} ({:2} file(s))  WAL={:10} ({:4} read(s), {:3} \
         scan(s))  INDEX-LOG={:8} ({:3} read(s))  pages={:9} ({:4} read(s))  = NAMED {:10} B",
        reading.base_image_read,
        reading.manifest_bytes_read,
        reading.manifest_file_reads,
        reading.wal_read_bytes,
        reading.wal_reads,
        reading.replay_scans,
        reading.index_log_read_bytes,
        reading.index_log_reads,
        reading.page_read_bytes,
        reading.page_reads,
        reading.named(),
    );
    eprintln!(
        "        KERNEL rchar={:10} B  RESIDUAL={:10} B  | handed back (not a cost): wal={:9} \
         index-log={:7}",
        reading.kernel_rchar.unwrap_or(0),
        reading.residual(),
        reading.wal_handed_back,
        reading.index_log_handed_back,
    );
    eprintln!(
        "        REPLAY above {:7} of {:7} = {:6} record(s) of tail (engine's own record: {:7}; \
         decoded {:6}, {:6} already covered -- process-global, printed only)",
        reading.watermark,
        reading.wal_last_sequence,
        reading.tail_records(),
        reading.engine_watermark,
        reading.records_decoded,
        reading.records_behind_checkpoint,
    );
    eprintln!(
        "        pieces a FOLD declined by name: {} -- process-global, printed only. Only \
         `fold_index_log_deltas` passes an anchor, so this is what says the whole-log read above \
         was not the delta fold",
        reading.fold_pieces_declined,
    );
    eprintln!(
        "        TIME {:5} ms wall (base {:8} us, seed {:8} us, replay {:8} us, fold {:8} us, \
         total {:8} us) -- reported, not claimed. The whole-log read is inside SEED",
        reading.wall_ms,
        reading.nanos_base / 1_000,
        reading.nanos_seed / 1_000,
        reading.nanos_replay / 1_000,
        reading.nanos_fold / 1_000,
        reading.nanos_total / 1_000,
    );
}

/// One arm: build a store at `divisor`, price its round, and recover it in BOTH regimes.
///
/// Both regimes come off ONE store, so the reflected control and the at-budget reading are about
/// the same files and the same records rather than two fixtures that happen to differ.
fn arm(objects: usize, divisor: u64, shape: KeyShape) -> (Round, Recovery, Recovery) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = shipped_range_engine(dir.path());
    let acked = seed_range(&engine, 0, objects, shape);
    assert_amortised_regime(&engine, acked);
    engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");

    let mut next_ordinal = objects;
    let round = run_round(&engine, dir.path(), &mut next_ordinal, shape, divisor);
    assert_round_can_express_it(&round);
    show_round(&format!("d={divisor}"), &round);

    // THE IDLE CONTROL, in its literal form: the round has just dumped and nothing is being
    // written. Two walks across that interval must agree file by file, or a byte measured after
    // the accrual below is not attributable to the accrual.
    let indexes = dir.path().join("indexes");
    let idle_before = index_tree(&indexes);
    let idle_after = index_tree(&indexes);
    let drifted: Vec<String> = idle_after
        .iter()
        .filter(|(path, entry)| idle_before.get(*path) != Some(*entry))
        .map(|(path, (len, _))| format!("{} ({len} B)", path.display()))
        .collect();
    assert!(
        drifted.is_empty() && idle_before.len() == idle_after.len(),
        "IDLE CONTROL: {} file(s) changed across an interval in which nothing was written -- {:?} \
         -- so no byte measured after the accrual is attributable to it",
        drifted.len(),
        drifted,
    );

    // THE SECOND BOOK ON THE CRASH STATE: this module's walk against the two stores' own byte
    // counters, to the byte.
    let walked_log = log_total(&idle_after);
    let store_log = engine.index_log_store().log_len_bytes(SHARD);
    assert_eq!(
        walked_log, store_log,
        "APPARATUS: the walk measured {walked_log} B of index log and the store's own \
         `log_len_bytes` reports {store_log} B. The two books disagree, so the space figure is \
         one measurement believed on its own"
    );
    let walked_wal = wal_total(&idle_after);
    let store_wal = engine
        .write_ahead_log_store()
        .probe_all_segment_bytes(SHARD);
    assert_eq!(
        walked_wal, store_wal,
        "APPARATUS: the walk measured {walked_wal} B of write-ahead log and the store's own \
         `probe_all_segment_bytes` reports {store_wal} B, so the term the budget's recovery cost \
         is paid in has no agreeing second book"
    );

    // REGIME ONE, off a copy: the store as it stands immediately after the dump.
    let reflected_dir = tempfile::tempdir().expect("tempdir");
    let copied = copy_tree(dir.path(), reflected_dir.path());
    assert!(
        copied > 0,
        "APPARATUS: the crash-state copy carried no bytes, so the recovery below runs over an \
         empty directory"
    );
    let reflected = recover(
        reflected_dir.path(),
        Regime::Reflected,
        divisor,
        next_ordinal,
        shape,
    );
    assert_recovery_can_express_it(&reflected);
    show_recovery(&reflected);

    // ACCRUE TO THE BUDGET, then REGIME TWO off a second copy.
    let threshold = accrue_to_the_budget(
        &engine,
        &mut next_ordinal,
        shape,
        divisor,
        base_index_on_disk(dir.path()),
    );
    let budget_dir = tempfile::tempdir().expect("tempdir");
    let copied_at_budget = copy_tree(dir.path(), budget_dir.path());
    assert!(
        copied_at_budget > copied,
        "APPARATUS: the at-budget crash state is {copied_at_budget} B against the reflected \
         state's {copied} B, so the accrual to {threshold} B put nothing on disk"
    );
    let at_budget = recover(
        budget_dir.path(),
        Regime::AtTheBudget,
        divisor,
        next_ordinal,
        shape,
    );
    assert_recovery_can_express_it(&at_budget);
    show_recovery(&at_budget);

    // THE BUDGET WAS REACHED. Without this an arm that stopped short reports a tail the budget did
    // not choose, and every figure below is about the fixture rather than the knob. The band is
    // one batch of overshoot: the accrual stops at the first check at or above the threshold.
    let claimed = 1.0 + 1.0 / divisor as f64;
    let measured = at_budget.b();
    assert!(
        (measured - claimed).abs() < (claimed - 1.0) * 0.25,
        "FIXTURE: the arm at divisor {divisor} crashed at space amplification {measured:.4} \
         against the {claimed:.4} the budget 1 + 1/{divisor} claims, so the tail it is about is \
         not the one the threshold chose"
    );
    assert!(
        reflected.b() < measured,
        "CONTROL: the reflected arm at divisor {divisor} stood at {:.4} and the at-budget arm at \
         {measured:.4}. The control must hold LESS log than the budget admits, or the two readings \
         are of one state and the comparison between them says nothing",
        reflected.b()
    );
    (round, reflected, at_budget)
}

// ---------------------------------------------------------------------------------------------
// THE DELIVERABLE
// ---------------------------------------------------------------------------------------------

/// AT BUDGET b, WRITE AMPLIFICATION IS a AND RECOVERY COSTS X.
///
/// The table this module exists to state, with both halves measured in the same arm off the same
/// store, every denominator named and the regime asserted before anything is reported.
///
/// WHAT IT SAYS, and why the constant can now be chosen rather than inherited:
///
/// - THE INDEX-LOG TERM OF A RECOVERY IS ZERO AT EVERY BUDGET. The default load path does not fold
///   the log, so the footprint a looser budget buys is never read back. The premise that a larger
///   budget "costs a longer index log to read at recovery" is REFUTED on the path a deployment
///   runs. `the_index_log_read_counter_registers_a_fold_of_the_same_crash_state` is why that zero
///   is a fact about the load path rather than about the counter.
/// - WHAT THE BUDGET COSTS INSTEAD IS THE WAL TAIL, because the dump its threshold fires is what
///   advances the durable recovery watermark. The tail and the WAL bytes a recovery reads both
///   track `1/divisor` across the sweep.
/// - WHAT DOMINATES A RECOVERY is the replay. The base image is one whole-file read that does not
///   move with the budget at all, the fold reads nothing, and the replay is the only term the
///   budget can change.
///
/// rust-internal: prices the engine's own recovery read in bytes at each dump cadence, no product
/// behaviour
#[test]
fn what_a_recovery_reads_across_the_budget_sweep() {
    let _rolling = roll_at(ROLL_BYTES);
    let shape = KeyShape::Scattered;
    let mut rows: Vec<(Round, Recovery, Recovery)> = Vec::new();

    for divisor in DIVISOR_SWEEP {
        let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
        eprintln!(
            "\n=== divisor {divisor} (budget b = 1 + 1/{divisor}), {SWEEP_OBJECTS} objects, load \
             {} ===",
            load.trim()
        );
        rows.push(arm(SWEEP_OBJECTS, divisor, shape));
    }

    eprintln!(
        "\n================ THE TRADE, {SWEEP_OBJECTS} objects on the shipped range \
         0..{SHIPPED_END_BUCKET} ================"
    );
    eprintln!(
        "  AT THE BUDGET -- the log at its high-water mark, the worst moment the budget admits"
    );
    eprintln!(
        "  {:>7} {:>8} {:>8} {:>8} {:>9} {:>10} {:>9} {:>11} {:>9} {:>10} {:>8}",
        "divisor", "b", "a", "a/bnd", "log B", "tail recs", "base read", "WAL read", "idx read",
        "named B", "wall ms"
    );
    for (round, _reflected, at_budget) in &rows {
        eprintln!(
            "  {:>7} {:>8.4} {:>8.3} {:>8.4} {:>9} {:>10} {:>9} {:>11} {:>9} {:>10} {:>8}",
            at_budget.divisor,
            round.b(),
            round.a(),
            round.a() / round.bound(),
            at_budget.index_log_bytes,
            at_budget.tail_records(),
            at_budget.base_image_read,
            at_budget.wal_read_bytes,
            at_budget.index_log_read_bytes,
            at_budget.named(),
            at_budget.wall_ms,
        );
    }
    eprintln!("  THE REFLECTED CONTROL -- the same stores with nothing past the checkpoint");
    eprintln!(
        "  {:>7} {:>8} {:>8} {:>8} {:>9} {:>10} {:>9} {:>11} {:>9} {:>10} {:>8}",
        "divisor", "b", "a", "a/bnd", "log B", "tail recs", "base read", "WAL read", "idx read",
        "named B", "wall ms"
    );
    for (_round, reflected, _at_budget) in &rows {
        eprintln!(
            "  {:>7} {:>8.4} {:>8} {:>8} {:>9} {:>10} {:>9} {:>11} {:>9} {:>10} {:>8}",
            reflected.divisor,
            reflected.b(),
            "--",
            "--",
            reflected.index_log_bytes,
            reflected.tail_records(),
            reflected.base_image_read,
            reflected.wal_read_bytes,
            reflected.index_log_read_bytes,
            reflected.named(),
            reflected.wall_ms,
        );
    }

    // ---- HALF ONE, AND IT COMES FIRST BECAUSE IT IS THE FINDING. The recovery reads the WHOLE
    // index log, at every budget and in both regimes, and the identity is EXACT rather than
    // approximate -- the bytes charged inside the read equal the bytes on disk. An equality rather
    // than a bound: a bound would pass for a read that had started declining pieces, which is
    // precisely the change that would make the budget cheap to recover and is the thing this
    // assertion exists to notice.
    for (_round, reflected, at_budget) in &rows {
        for reading in [reflected, at_budget] {
            assert_eq!(
                reading.index_log_read_bytes, reading.index_log_bytes,
                "the {} recovery at divisor {} read {} B of index log off disk in {} read(s) \
                 against the {} B the log holds. The budget's footprint is read back IN FULL by \
                 `latest_slab_catalog`, so these two are one number; a difference means the read \
                 has started declining pieces -- or has started reading more than the log",
                reading.regime.short(),
                reading.divisor,
                reading.index_log_read_bytes,
                reading.index_log_reads,
                reading.index_log_bytes,
            );
        }
    }

    // ---- HALF TWO: WHAT THE BUDGET DOES BUY A RECOVERY IS A TAIL, AND THE TAIL TRACKS 1/d.
    let loose = &rows.first().expect("the sweep has arms").2;
    let tight = &rows.last().expect("the sweep has arms").2;
    let budget_range = tight.divisor as f64 / loose.divisor as f64;
    assert!(
        loose.index_log_bytes >= tight.index_log_bytes * 4,
        "the crash state held {} B of index log at divisor {} and {} B at divisor {} -- a \
         {budget_range:.0}x change in the budget that did not move the footprint, so the sweep is \
         not driving the knob it names",
        loose.index_log_bytes,
        loose.divisor,
        tight.index_log_bytes,
        tight.divisor,
    );
    // THE INDEX-LOG READ IS THE FOOTPRINT, so this clause is the same 4x in the term the
    // recovery actually pays. Stated separately from the footprint clause above because the two
    // being equal is HALF ONE's claim and must not be assumed by the clause that uses it.
    assert!(
        loose.index_log_read_bytes >= tight.index_log_read_bytes * 4,
        "the recovery read {} B of index log at divisor {} and {} B at divisor {} -- a \
         {budget_range:.0}x change in the budget that did not move the term it is paid in",
        loose.index_log_read_bytes,
        loose.divisor,
        tight.index_log_read_bytes,
        tight.divisor,
    );
    assert!(
        loose.wal_read_bytes > tight.wal_read_bytes,
        "the recovery read {} B of WAL at divisor {} and {} B at divisor {} -- the looser budget \
         did not cost more to recover, so there is no second half to the trade",
        loose.wal_read_bytes,
        loose.divisor,
        tight.wal_read_bytes,
        tight.divisor,
    );

    // ---- HALF THREE: THE SLOPE, NOT A DIVISION. Bytes the recovery read against the index-log
    // footprint the budget admitted, fitted across the whole sweep, beside the division at each
    // end -- which is named as a division.
    let points: Vec<(f64, f64)> = rows
        .iter()
        .map(|(_, _, at_budget)| (at_budget.index_log_bytes as f64, at_budget.named() as f64))
        .collect();
    let fitted = slope(&points);
    let wal_points: Vec<(f64, f64)> = rows
        .iter()
        .map(|(_, _, at_budget)| {
            (
                at_budget.index_log_bytes as f64,
                at_budget.wal_read_bytes as f64,
            )
        })
        .collect();
    let fitted_wal = slope(&wal_points);
    let per_log_byte =
        |reading: &Recovery| reading.wal_read_bytes as f64 / reading.index_log_bytes.max(1) as f64;
    let per_tail_record =
        |reading: &Recovery| reading.wal_read_bytes as f64 / reading.tail_records().max(1) as f64;
    eprintln!(
        "\n  SLOPES, fitted over {} budgets spanning {:.0}x of index-log footprint:\n\
         \x20   {fitted:.3} B of TOTAL named read per byte of index log the budget admits\n\
         \x20   {fitted_wal:.3} B of WAL read per byte of index log the budget admits\n\
         \x20   1.000   B of INDEX-LOG read per byte of index log -- not a fit, an IDENTITY, \
         asserted to the byte above\n\
         \x20 THE DIVISIONS at each end, named as divisions and not slopes: WAL read is {:.3} of \
         the footprint at divisor {} and {:.3} at divisor {}; per record of tail {:.1} B at \
         divisor {} and {:.1} B at divisor {} -- the tight end is dearer per record because a \
         replay has a fixed part, the piece header and the first window, that a short tail divides \
         by less.",
        points.len(),
        loose.index_log_bytes as f64 / tight.index_log_bytes.max(1) as f64,
        per_log_byte(loose),
        loose.divisor,
        per_log_byte(tight),
        tight.divisor,
        per_tail_record(loose),
        loose.divisor,
        per_tail_record(tight),
        tight.divisor,
    );
    assert!(
        fitted > 1.0 && fitted_wal > 0.0,
        "APPARATUS: the fitted slopes against the index-log footprint are {fitted:.3} total and \
         {fitted_wal:.3} WAL. The total must exceed 1.0 -- the log itself is read whole, which is \
         one byte per byte before any other reader -- so a total at or below it means the sweep \
         does not show a recovery getting dearer as the budget loosens and the sign of the whole \
         trade is unestablished"
    );

    // ---- HALF FOUR, THE CONTROL: with nothing past the checkpoint there is no tail at ANY
    // budget, and the figure must not move with the budget. A measurement taken only in this
    // regime would report a budget with no recovery cost at all.
    for (_round, reflected, at_budget) in &rows {
        assert!(
            reflected.wal_bytes * 10 < at_budget.wal_bytes,
            "CONTROL: the reflected arm at divisor {} held {} B of write-ahead log against the \
             at-budget arm's {} B. It dumped and took no write after, so a log the same size \
             means the dump did not reclaim and the control is not idle",
            reflected.divisor,
            reflected.wal_bytes,
            at_budget.wal_bytes,
        );
        assert!(
            reflected.wal_read_bytes * 50 < at_budget.wal_read_bytes,
            "CONTROL: the reflected recovery at divisor {} read {} B of write-ahead log against \
             the at-budget recovery's {} B. With nothing past the checkpoint there is no tail to \
             replay, so a comparable read means the control cannot bound the at-budget reading",
            reflected.divisor,
            reflected.wal_read_bytes,
            at_budget.wal_read_bytes,
        );
    }
    let reflected_wal: Vec<u64> = rows
        .iter()
        .map(|(_, reflected, _)| reflected.wal_read_bytes)
        .collect();
    let widest = *reflected_wal.iter().max().expect("the sweep has arms");
    let tightest = *reflected_wal.iter().min().expect("the sweep has arms");
    assert!(
        widest <= tightest + loose.wal_read_bytes / 4,
        "CONTROL: the reflected recovery read between {tightest} and {widest} B of WAL across the \
         sweep, against {} B at the loose end of the at-budget arms. An idle store holds no tail \
         whatever the threshold says, so a reflected figure that moved with the budget means the \
         budget is being read somewhere this accounting does not name",
        loose.wal_read_bytes,
    );

    // ---- HALF FIVE: THE BASE IMAGE IS NOT WHAT THE BUDGET MOVES, and it is the term the looser
    // budget was supposed to be traded against. Same corpus in every arm, so it must be flat.
    let (base_low, base_high) = rows
        .iter()
        .map(|(_, _, at_budget)| at_budget.base_image_read)
        .fold((u64::MAX, 0u64), |(lo, hi), value| {
            (lo.min(value), hi.max(value))
        });
    let base_spread = base_high as f64 / base_low.max(1) as f64;
    let log_spread =
        loose.index_log_read_bytes as f64 / tight.index_log_read_bytes.max(1) as f64;
    // NOT A FLATNESS CLAIM, AND THE REASON IS WORTH WRITING DOWN: the base image DOES move across
    // the sweep, because a looser budget's round writes more objects into the same store before it
    // dumps, so the arms differ in corpus as well as in budget. What is asserted is the thing that
    // is actually true -- the budget's cost is in the LOG term, which moves an order of magnitude
    // further than the base term over the same sweep.
    assert!(
        log_spread >= base_spread * 4.0,
        "the index-log read moved {log_spread:.2}x across the sweep and the base image moved \
         {base_spread:.2}x. If the base term moves comparably then the arms differ in corpus as \
         much as in budget, and the trade this table states cannot be attributed to the knob"
    );
    eprintln!(
        "\n  WHAT DOMINATES, AND WHAT THE BUDGET MOVES. The base image ran {base_low} to \
         {base_high} B ({base_spread:.2}x) -- it moves only because a looser budget's round adds \
         more objects before it dumps, not because of the threshold. The index-log read moved \
         {log_spread:.2}x over the same sweep, one byte for every byte the budget admits. The WAL \
         read moved {:.2}x off a log that is {} B on disk in EVERY arm, which is the window \
         re-read `restore_large_scale` measures going superlinear at deployment scale. What reads \
         the index log is `latest_slab_catalog`, and \
         `the_whole_log_read_at_recovery_is_the_slab_catalog_walk` measures why giving that walk \
         the base anchor would not make it smaller.",
        loose.wal_read_bytes as f64 / tight.wal_read_bytes.max(1) as f64,
        loose.wal_bytes,
    );
}

/// THE SIZE QUESTION: DOES THE RECOVERY TERM THE BUDGET BUYS GROW WITH THE STORE TOO.
///
/// The sweep above holds the store and varies the budget, which is the right shape for a
/// dimensionless knob. This holds the budget at the shipped divisor and varies the STORE, because
/// the figure a deployment cares about is the one at its own size -- and the term under
/// measurement is the WAL tail, whose cost `restore_large_scale` already measured going
/// SUPERLINEAR at 200,000 records: 6.70x the kernel bytes per stored record for a 10x store,
/// because the replay window count grows and each window re-reads the pieces behind it.
///
/// What this adds is the budget's part in that: the threshold is a FRACTION of the base index, so
/// the tail it admits grows WITH the store, which is what puts a budget on that curve rather than
/// at a point on it.
///
/// rust-internal: prices the engine's own recovery read at two store sizes, no product behaviour
#[test]
fn the_recovery_a_budget_buys_grows_with_the_store() {
    let _rolling = roll_at(ROLL_BYTES);
    let shape = KeyShape::Scattered;
    let divisor = crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR;

    eprintln!("\n=== {SWEEP_OBJECTS} objects at the shipped divisor {divisor} ===");
    let (_, _, small) = arm(SWEEP_OBJECTS, divisor, shape);
    eprintln!("\n=== {LARGE_OBJECTS} objects at the shipped divisor {divisor} ===");
    let (_, _, large) = arm(LARGE_OBJECTS, divisor, shape);

    let corpus_ratio = LARGE_OBJECTS as f64 / SWEEP_OBJECTS as f64;
    assert!(
        large.base_index_bytes >= small.base_index_bytes * 2,
        "FIXTURE: the base index read {} B at {SWEEP_OBJECTS} objects and {} B at {LARGE_OBJECTS}. \
         A {corpus_ratio:.0}x corpus that did not move the base index is not a size sweep, and the \
         threshold is a fraction of exactly that number",
        small.base_index_bytes,
        large.base_index_bytes,
    );
    assert!(
        large.tail_records() > small.tail_records(),
        "the tail the shipped budget admits was {} record(s) at {SWEEP_OBJECTS} objects and {} at \
         {LARGE_OBJECTS}. The threshold is `base_index / {divisor}` and the base index grew, so a \
         tail that did not grow is the budget failing to be a fraction",
        small.tail_records(),
        large.tail_records(),
    );

    let per_tail_record =
        |reading: &Recovery| reading.wal_read_bytes as f64 / reading.tail_records().max(1) as f64;
    let per_object = |reading: &Recovery| reading.named() as f64 / reading.objects.max(1) as f64;
    eprintln!(
        "\n================ THE SHAPE AT TWO STORE SIZES, shipped divisor {divisor} ================"
    );
    eprintln!(
        "  {:>8} {:>11} {:>9} {:>10} {:>11} {:>11} {:>12} {:>12} {:>8}",
        "objects", "base index", "log B", "tail recs", "WAL read", "named B", "B/tail rec",
        "named/obj", "wall ms"
    );
    for reading in [&small, &large] {
        eprintln!(
            "  {:>8} {:>11} {:>9} {:>10} {:>11} {:>11} {:>12.1} {:>12.2} {:>8}",
            reading.objects,
            reading.base_index_bytes,
            reading.index_log_bytes,
            reading.tail_records(),
            reading.wal_read_bytes,
            reading.named(),
            per_tail_record(reading),
            per_object(reading),
            reading.wall_ms,
        );
    }
    eprintln!(
        "  OVER A {corpus_ratio:.0}x CORPUS: base index {:.3}x, index-log footprint {:.3}x, tail \
         {:.3}x, WAL read {:.3}x, named {:.3}x.\n  TWO POINTS, so these are DIVISIONS and not \
         slopes: {:.1} -> {:.1} B of WAL read a tail record, {:.2} -> {:.2} B named a stored \
         object.",
        large.base_index_bytes as f64 / small.base_index_bytes.max(1) as f64,
        large.index_log_bytes as f64 / small.index_log_bytes.max(1) as f64,
        large.tail_records() as f64 / small.tail_records().max(1) as f64,
        large.wal_read_bytes as f64 / small.wal_read_bytes.max(1) as f64,
        large.named() as f64 / small.named().max(1) as f64,
        per_tail_record(&small),
        per_tail_record(&large),
        per_object(&small),
        per_object(&large),
    );

    // THE IDENTITY HOLDS AT BOTH SIZES: the recovery reads the budget's whole footprint, and the
    // footprint is a FRACTION of the base index, so the recovery's index-log term is `1/divisor`
    // of the base image at every store size. That is what makes the trade this module states
    // scale-free in its index-log term, and it is the clause a store-size change would break.
    for reading in [&small, &large] {
        assert_eq!(
            reading.index_log_read_bytes, reading.index_log_bytes,
            "the recovery at {} objects read {} B of index log against the {} B the log holds",
            reading.objects,
            reading.index_log_read_bytes,
            reading.index_log_bytes,
        );
        let share = reading.index_log_bytes as f64 / reading.base_index_bytes as f64;
        let claimed = 1.0 / divisor as f64;
        assert!(
            (share - claimed).abs() < claimed * 0.25,
            "at {} objects the index log stood at {share:.4} of the base index against the \
             {claimed:.4} the budget 1/{divisor} names. The threshold is a FRACTION, so a share \
             that moves with the store size means the recovery term this budget buys is not \
             scale-free and the two sizes are not comparable",
            reading.objects,
        );
    }
    eprintln!(
        "  AND THE INDEX-LOG TERM IS SCALE-FREE: the log stood at {:.4} of the base index at {} \
         objects and {:.4} at {}, against the {:.4} the shipped budget 1/{divisor} names -- and \
         the recovery read all of it in both cases. What is NOT scale-free is the rest: the base \
         image went {:.3}x and the WAL read {:.3}x, so the index-log term rose from {:.1}% to \
         {:.1}% of what a recovery reads as the store grew.",
        small.index_log_bytes as f64 / small.base_index_bytes as f64,
        small.objects,
        large.index_log_bytes as f64 / large.base_index_bytes as f64,
        large.objects,
        1.0 / divisor as f64,
        large.base_index_bytes as f64 / small.base_index_bytes.max(1) as f64,
        large.wal_read_bytes as f64 / small.wal_read_bytes.max(1) as f64,
        100.0 * small.index_log_read_bytes as f64 / small.named().max(1) as f64,
        100.0 * large.index_log_read_bytes as f64 / large.named().max(1) as f64,
    );
}

/// WHICH WALK READ THE LOG, AND WHY NO DECLINE WOULD HAVE MADE IT SMALLER.
///
/// The sweep measures a whole-log read on the default recovery path and attributes it to
/// `latest_slab_catalog`. That is an attribution, and an attribution taken from the mechanism
/// rather than from the call site has been wrong by 100% in this crate before, so it is measured:
/// the walk is driven alone over ONE crash state and its byte count compared with the recovery's.
///
/// AND THE OBVIOUS FIX IS MEASURED HERE TOO, BECAUSE IT DOES NOT WORK. The piece-name decline
/// `index_log_scale::what_an_index_log_replay_reads_at_two_corpus_sizes` measured is on
/// `for_each_delta_record_above_anchor`, which `latest_slab_catalog` does not use -- so the first
/// thing to try is giving the catalog walk the base anchor. In the state a budget's crash leaves,
/// THAT BUYS NOTHING: the sweep behind the last dump has already dropped every piece the base
/// reflects, so what remains IS the unreflected suffix and there is no piece for an anchor to
/// decline. Driven over the same files, the fold's shape reads the same bytes and hands back the
/// same records as the anchor-less walk.
///
/// A POSITIVE CONTROL SAYS THAT IS A FACT ABOUT THE STATE AND NOT A BROKEN DECLINE: the same walk
/// given an anchor ABOVE every record in the log declines pieces and reads strictly less off the
/// same files. Without that arm, "the decline declined nothing" and "the decline is not working"
/// read identically.
///
/// WHAT FOLLOWS, and it is what the budget decision rests on: the whole-log read is the budget's
/// FOOTPRINT, read back in full, and not an artefact a decline could remove. What could remove it
/// is a different property of the same walk -- `latest_slab_catalog` wants only the LAST record
/// carrying a catalog and reads every record to find it.
///
/// This does NOT set `TS_WAL_LEGACY_RECOVERY` to reach the fold through the engine. That variable
/// is read from the environment of the whole process, which this suite shares, so flipping it here
/// would change the load path under every test running beside this one.
///
/// rust-internal: compares the engine's own index-log walks over one store, no product behaviour
#[test]
fn the_whole_log_read_at_recovery_is_the_slab_catalog_walk() {
    let _rolling = roll_at(ROLL_BYTES);
    let divisor = crate::index_log::INDEX_DUMP_BASE_FRACTION_DIVISOR;
    let shape = KeyShape::Scattered;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = shipped_range_engine(dir.path());
    let acked = seed_range(&engine, 0, SWEEP_OBJECTS, shape);
    assert_amortised_regime(&engine, acked);
    engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the priming dump did not complete");
    let mut next_ordinal = SWEEP_OBJECTS;
    accrue_to_the_budget(
        &engine,
        &mut next_ordinal,
        shape,
        divisor,
        base_index_on_disk(dir.path()),
    );

    let tree = index_tree(&dir.path().join("indexes"));
    let on_disk = log_total(&tree);
    let pieces = log_pieces(&tree);
    // THE DENOMINATOR: a log in one piece cannot show a per-piece decision at all, so neither the
    // negative reading below nor the positive control would mean anything there.
    assert!(
        pieces >= 4,
        "FIXTURE: the crash state holds the log in {pieces} piece(s), too few for a decision that \
         works by dropping whole pieces to be visible either way"
    );
    assert!(
        on_disk > 0,
        "APPARATUS: the crash state holds no index log at all, so no walk could read a byte for \
         any reason"
    );
    let anchor = durable_watermark(dir.path());
    let store = engine.index_log_store();

    /// One walk's cost and what it handed back.
    fn walk(
        store: &crate::index_log::LocalIndexLogStore,
        anchor: u64,
    ) -> (u64, u64, usize, u64) {
        let (before, reads_before) =
            crate::index_log::index_log_piece_read_counts_on_this_thread();
        let mut records = 0usize;
        let mut highest = 0u64;
        store
            .for_each_delta_record_above_anchor(SHARD, 0, anchor, |record| {
                records += 1;
                if let Some(sequence) = record.applied_wal_sequence {
                    highest = highest.max(sequence);
                }
            })
            .expect("the walk under measurement must succeed");
        let (after, reads_after) = crate::index_log::index_log_piece_read_counts_on_this_thread();
        (after - before, reads_after - reads_before, records, highest)
    }

    // WALK ONE, the shape the recovery runs: anchor 0, which is what `for_each_delta_record` --
    // and therefore `latest_slab_catalog` -- passes.
    let (open_read, open_reads, open_records, highest) = walk(&store, 0);
    // WALK TWO, the fold's shape over the same files, with the anchor the base image carries.
    let (fold_read, fold_reads, fold_records, _) = walk(&store, anchor);
    // WALK THREE, THE POSITIVE CONTROL: an anchor above every record in the log. If the decline
    // works on these files at all, this is the arm in which it fires.
    let (high_read, high_reads, high_records, _) = walk(&store, highest.saturating_add(1));

    // And the catalog walk itself, through its own entry point, so the attribution is to a CALL
    // SITE and not to a shape that resembles one.
    let (before, reads_before) = crate::index_log::index_log_piece_read_counts_on_this_thread();
    let catalog = store
        .latest_slab_catalog(SHARD)
        .expect("the catalog walk under measurement must succeed");
    let (after, reads_after) = crate::index_log::index_log_piece_read_counts_on_this_thread();
    let catalog_read = after - before;
    let catalog_reads = reads_after - reads_before;

    eprintln!(
        "  one crash state: {on_disk} B of index log in {pieces} piece(s), base anchor {anchor}, \
         highest record anchor {highest}\n\
         \x20   `latest_slab_catalog`          read {catalog_read:8} B in {catalog_reads:3} \
         read(s)   -- catalog present: {}\n\
         \x20   the same walk, anchor 0        read {open_read:8} B in {open_reads:3} read(s)   \
         -- {open_records} record(s)\n\
         \x20   the FOLD's shape, anchor {anchor:<5} read {fold_read:8} B in {fold_reads:3} \
         read(s)   -- {fold_records} record(s): the decline has nothing to decline, because the \
         sweep already dropped every reflected piece\n\
         \x20   CONTROL, anchor {:<13} read {high_read:8} B in {high_reads:3} read(s)   -- \
         {high_records} record(s): the decline DOES work on these files",
        catalog.is_some(),
        highest + 1,
    );

    assert_eq!(
        catalog_read, on_disk,
        "the catalog walk read {catalog_read} B of a {on_disk} B log. The sweep attributes the \
         recovery's whole-log read to this call site because the two are the same number; if this \
         walk no longer reads the whole log then that attribution is stale"
    );
    assert_eq!(
        catalog_read, open_read,
        "the catalog walk read {catalog_read} B and the anchor-0 walk {open_read} B off the same \
         files. `latest_slab_catalog` IS `for_each_delta_record`, which is the anchor-0 walk, so a \
         difference means it has grown a reader of its own that this accounting does not name"
    );
    assert!(
        catalog.is_some(),
        "FIXTURE: the log carries no slab-catalog record, so `latest_slab_catalog` walked it all \
         and found nothing. It reads the whole log either way, which is the point -- but a fixture \
         that cannot produce the record is not the state a recovery meets"
    );
    // THE NEGATIVE READING: in this state the fold's anchor buys nothing.
    assert_eq!(
        (fold_read, fold_records),
        (open_read, open_records),
        "the fold's shape read {fold_read} B and {fold_records} record(s) against the anchor-0 \
         walk's {open_read} B and {open_records}. They are equal in the state a budget's crash \
         leaves because the sweep has already dropped every reflected piece; a difference here \
         means that is no longer true, and the whole-log read in the sweep would then be partly an \
         artefact of the entry point rather than the budget's footprint"
    );
    // THE POSITIVE CONTROL, which is what stops the line above from being a broken instrument.
    assert!(
        high_read < open_read && high_records < open_records,
        "CONTROL: an anchor above every record in the log read {high_read} B and {high_records} \
         record(s) against the anchor-0 walk's {open_read} B and {open_records}. The decline did \
         not fire even when nothing in the log is unreflected, so 'the decline declined nothing' \
         above cannot be told apart from 'the decline is not working'"
    );
    eprintln!(
        "  SO: the recovery's {on_disk} B index-log read is the budget's FOOTPRINT read back whole, \
         not a decline that was left off. What would make it smaller is that \
         `latest_slab_catalog` needs only the LAST record carrying a catalog and reads every one \
         of the {open_records} to find it."
    );
}

/// THE RESIDUAL IS A READING, NOT AN IDENTITY.
///
/// Zero is exactly what an instrument that always answered zero would say. An empty span must read
/// exactly zero and a span holding a read of a known length must read exactly that length, so the
/// residual printed beside every arm above is a number that could have been wrong.
///
/// rust-internal: exercises this module's own kernel byte instrument, no product behaviour
#[test]
fn the_recovery_byte_instrument_recovers_a_planted_read_exactly() {
    /// Two sizes, so a correct reading is not one constant, and odd, so a reading rounded to a
    /// block cannot match by accident.
    const PLANTS: [usize; 2] = [1_000_003, 3_000_009];

    let (_, empty) = kernel_read_span(|| ());
    assert_eq!(
        empty,
        Some(0),
        "APPARATUS: an empty span read {empty:?} bytes. The instrument charges itself, so every \
         residual beside it is offset by exactly that much"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    for (index, size) in PLANTS.iter().enumerate() {
        let path = dir.path().join(format!("plant-{index}.bin"));
        std::fs::write(&path, vec![b'p'; *size]).expect("the plant must be writable");
        let (len, charged) = kernel_read_span(|| {
            std::fs::read(&path)
                .expect("the planted read must succeed")
                .len()
        });
        assert_eq!(len, *size, "APPARATUS: the plant did not read back whole");
        assert_eq!(
            charged,
            Some(*size as u64),
            "the instrument charged {charged:?} for a planted read of exactly {size} bytes, so \
             the residual it produces is not a measurement of what a span read"
        );
    }
    eprintln!(
        "  the kernel byte instrument: an empty span reads 0, and planted reads of {PLANTS:?} come \
         back exactly."
    );
}
