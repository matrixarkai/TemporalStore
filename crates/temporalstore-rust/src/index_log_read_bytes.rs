// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the index-log store READS, against what it says it read.
//!
//! #1936 took a restore's total from the kernel and subtracted the readers the engine names.
//! 965,415,651 bytes -- 95% of that restore -- belonged to a reader with no counter on it, while
//! every in-engine counter reported the restore as perfectly flat. It fixed the largest one and
//! NAMED this one: the index-log store reads 8,119,256 bytes bringing back a 200,000-record
//! shard, in 1,069 reads, against an `index_log_store().stats()` answering `bytes_read: 0`.
//!
//! Reproduced here before anything was changed, at 2,000 and 8,000 records:
//!
//! ```text
//!                                     2,000 records   8,000 records    ratio
//!   pieces                                       20              80    4.000
//!   records the fold handed back              2,000           8,000    4.000
//!   the log on disk                         163,236         655,236    4.014
//!   what the STORE said it read                   0               0       --
//!   what the KERNEL charged                 163,236         655,236    4.014
//!   UNATTRIBUTED                            163,236         655,236    4.014
//! ```
//!
//! ALL of it. Not 0.8% -- at this shape the fold's whole reading was unattributed, because the
//! fold fed no counter at all. A READER WITH NO COUNTER CONTRIBUTES ZERO TO EVERY REPORT, WHICH
//! IS INDISTINGUISHABLE FROM A READER DOING NO WORK, and that is how a 95% blind spot survived
//! every previous measurement of this path.
//!
//! Every number here is a COUNT, never a duration: this box has measured the same round at 490 ms
//! and at 1,077 ms on two afternoons, and a count does not move with the neighbours.

#![cfg(test)]

use super::*;

/// Two sizes and the ratio between them. Four times the log, so a quantity flat per record and
/// one that grows with the log are four times apart and cannot be confused.
const SMALL: usize = 2_000;
const BIG: usize = 8_000;

/// Rolled rather than one piece, so the fold enumerates and opens tens of pieces and the counter
/// is charged across piece boundaries rather than once.
const SEGMENT_BYTES: u64 = 8 * 1024;

const SHARD: ShardId = 21;

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT: the kernel's own byte counter, for THIS THREAD.
//
// `/proc/self/io` is the whole PROCESS, and a suite running tests beside each other charges this
// span whatever another thread happened to read -- it once charged an operation 3,194 bytes a
// background thread wrote. `/proc/thread-self/io` is this thread, and a fold runs on one.
//
// Reading the counter is itself a read the kernel charges, so a reading carries the LENGTH of the
// text it came from and the span subtracts it exactly rather than estimating it. That is what
// makes "exactly" available at the end instead of "about".
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct IoReading {
    rchar: u64,
    self_bytes: u64,
}

fn read_thread_io() -> Option<IoReading> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    let mut rchar = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("rchar:") {
            rchar = rest.trim().parse().ok();
        }
    }
    Some(IoReading {
        rchar: rchar?,
        self_bytes: text.len() as u64,
    })
}

/// What the kernel says this THREAD read across a span, with the instrument's own reads removed.
///
/// `None` when `/proc` said nothing, so "the kernel was not counting" can never be read as "the
/// kernel counted zero".
fn kernel_read_span<T>(work: impl FnOnce() -> T) -> (T, Option<u64>) {
    let before = read_thread_io();
    let value = work();
    let after = read_thread_io();
    let rchar = before
        .zip(after)
        // The opening reading's own bytes fall inside the span; the closing reading's are charged
        // after it is taken.
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes));
    (value, rchar)
}

// ---------------------------------------------------------------------------------------------
// THE FIXTURE
// ---------------------------------------------------------------------------------------------

fn small_item(bucket: u32, key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Page,
        routing_bucket: bucket,
        block_ref_key: key.to_string(),
        object_key: key.to_string(),
        model_id: "m".to_string(),
        component: None,
        object_id: 1,
        block_id: 0,
        address: None,
        size: 8,
        in_log: false,
        deleted: false,
    }
}

/// Set the rolling threshold for THIS THREAD and put it back on drop, panic included.
struct RollingThreshold;
impl Drop for RollingThreshold {
    fn drop(&mut self) {
        set_index_log_segment_bytes_for_test(None);
    }
}
fn roll_at(bytes: u64) -> RollingThreshold {
    set_index_log_segment_bytes_for_test(Some(bytes));
    RollingThreshold
}

/// A log of `records` anchored deltas whose anchors CLIMB one per record.
fn build_log(dir: &std::path::Path, records: usize) {
    let store = LocalIndexLogStore::new(dir);
    for value in 0..records {
        store
            .append_delta(
                SHARD,
                vec![small_item(
                    (value % 64) as u32,
                    // Zero-padded to eight digits at BOTH corpus sizes, so the only field that
                    // differs between the two fixtures is the one being varied.
                    &format!("tenant/1/object/{value:08}"),
                )],
                Vec::new(),
                Some(value as u64 + 1),
                None,
                false,
                false,
            )
            .unwrap();
    }
}

/// On-disk bytes of every piece the fold will open, from `stat` and not from the fold.
///
/// A THIRD row, independent of both the kernel's and the counter's: with nothing declined, a fold
/// that reads each piece exactly once must read exactly this.
fn log_bytes_on_disk(dir: &std::path::Path) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut pieces = 0usize;
    for path in index_log_segment_paths(dir, SHARD) {
        let Ok(metadata) = path.metadata() else {
            continue;
        };
        pieces += 1;
        bytes = bytes.saturating_add(metadata.len());
    }
    (bytes, pieces)
}

/// What one load-path fold of a whole log cost.
#[derive(Debug)]
struct Fold {
    pieces: usize,
    /// Every piece's size from `stat`: what a fold reading each piece once must read.
    on_disk: u64,
    /// Records the fold handed back.
    applied: usize,
    /// What the STORE's record-facing row says -- the one that reported zero.
    store_bytes: u64,
    /// What the counter INSIDE the read says.
    counted_bytes: u64,
    counted_reads: u64,
    /// What the KERNEL charged this thread across the same span -- the independent total.
    kernel_rchar: Option<u64>,
}

impl Fold {
    /// The kernel's number minus the reader that is now counted. Nothing in this engine feeds
    /// `rchar`, so this CAN be wrong and can be SEEN to be wrong -- which is the property a
    /// residual summed from the rows it audits does not have.
    fn residual(&self) -> Option<i64> {
        self.kernel_rchar
            .map(|rchar| rchar as i64 - self.counted_bytes as i64)
    }
}

/// Fold a whole log the way the load path does, with nothing declined.
fn fold(dir: &std::path::Path) -> Fold {
    let (on_disk, pieces) = log_bytes_on_disk(dir);
    let store = LocalIndexLogStore::new(dir);
    let store_before = store.stats(SHARD).bytes_read;
    let (bytes_before, reads_before) = index_log_piece_read_counts_on_this_thread();
    let mut applied = 0usize;
    let (_, kernel_rchar) = kernel_read_span(|| {
        store
            .for_each_delta_record_above_anchor(SHARD, 0, 0, |_record| {
                applied += 1;
            })
            .expect("the fold under measurement must succeed");
    });
    let (bytes_after, reads_after) = index_log_piece_read_counts_on_this_thread();
    let store_after = store.stats(SHARD).bytes_read;
    Fold {
        pieces,
        on_disk,
        applied,
        store_bytes: store_after.saturating_sub(store_before),
        counted_bytes: bytes_after - bytes_before,
        counted_reads: reads_after - reads_before,
        kernel_rchar,
    }
}

// ---------------------------------------------------------------------------------------------
// THE MEASUREMENT
// ---------------------------------------------------------------------------------------------

/// WHAT THE LOAD-PATH FOLD READS, AND WHETHER THE COUNTER RECOVERS THE KERNEL'S FIGURE EXACTLY.
///
/// Three rows, from three independent places: `stat` on the pieces, the counter inside the read,
/// and `/proc/thread-self/io`. A counter that merely LOOKED plausible would satisfy an assertion
/// about a ratio; these must agree TO THE BYTE.
#[test]
fn the_fold_byte_counter_recovers_the_kernels_figure_at_two_corpus_sizes() {
    let _rolling = roll_at(SEGMENT_BYTES);

    let small_dir = tempfile::tempdir().unwrap();
    let big_dir = tempfile::tempdir().unwrap();
    build_log(small_dir.path(), SMALL);
    build_log(big_dir.path(), BIG);

    let small = fold(small_dir.path());
    let big = fold(big_dir.path());

    println!(
        "index-log fold: what it reads, against what three instruments say it read\n\
         {:>36}{:>16}{:>16}{:>10}\n\
         {:>36}{:>16}{:>16}{:>10.3}\n\
         {:>36}{:>16}{:>16}{:>10.3}\n\
         {:>36}{:>16}{:>16}{:>10.3}\n\
         {:>36}{:>16}{:>16}{:>10.3}\n\
         {:>36}{:>16}{:>16}{:>10.3}\n\
         {:>36}{:>16}{:>16}{:>10.3}\n\
         {:>36}{:>16}{:>16}\n",
        "", format!("{SMALL} records"), format!("{BIG} records"), "ratio",
        "pieces", small.pieces, big.pieces, big.pieces as f64 / small.pieces as f64,
        "records the fold handed back", small.applied, big.applied,
        big.applied as f64 / small.applied as f64,
        "the log on disk (stat)", small.on_disk, big.on_disk,
        big.on_disk as f64 / small.on_disk as f64,
        "the store's record-facing row", small.store_bytes, big.store_bytes, 0.0,
        "THE COUNTER INSIDE THE READ", small.counted_bytes, big.counted_bytes,
        big.counted_bytes as f64 / small.counted_bytes.max(1) as f64,
        "reads it was charged", small.counted_reads, big.counted_reads,
        big.counted_reads as f64 / small.counted_reads.max(1) as f64,
        "UNATTRIBUTED (kernel less counter)",
        small.residual().unwrap_or(i64::MIN), big.residual().unwrap_or(i64::MIN),
    );

    // DENOMINATORS FIRST. A fixture that quietly produced one piece, or folded no record, would
    // report a flat cost and be believed.
    assert!(
        small.pieces >= 10 && big.pieces >= 40,
        "APPARATUS: fixtures rolled into {} and {} pieces -- too few to charge across boundaries",
        small.pieces,
        big.pieces
    );
    assert_eq!(small.applied, SMALL, "APPARATUS: the small fold applied {}", small.applied);
    assert_eq!(big.applied, BIG, "APPARATUS: the big fold applied {}", big.applied);
    assert!(
        small.kernel_rchar.is_some() && big.kernel_rchar.is_some(),
        "APPARATUS: /proc/thread-self/io carried no rchar line"
    );
    assert!(
        big.on_disk > small.on_disk * 3,
        "APPARATUS: the two logs are {} and {} bytes -- not a corpus difference",
        small.on_disk,
        big.on_disk
    );

    // THE CLAIM. The counter recovers the kernel's figure EXACTLY, at both sizes. Not within a
    // tolerance and not as a ratio: a counter that agrees with the kernel to the byte is
    // evidence, and one that looks plausible is not.
    for (label, cost) in [("small", &small), ("big", &big)] {
        assert_eq!(
            cost.counted_bytes,
            cost.kernel_rchar.unwrap(),
            "{label}: the counter says {} bytes and the kernel charged this thread {} over the \
             same span",
            cost.counted_bytes,
            cost.kernel_rchar.unwrap()
        );
        assert_eq!(
            cost.counted_bytes, cost.on_disk,
            "{label}: the counter says {} and the pieces on disk hold {} -- a fold with nothing \
             declined reads each piece exactly once",
            cost.counted_bytes, cost.on_disk
        );
        assert_eq!(
            cost.residual(),
            Some(0),
            "{label}: the fold left an unattributed remainder of {:?}",
            cost.residual()
        );
        assert!(
            cost.counted_reads > 0,
            "APPARATUS: {label} charged {} reads",
            cost.counted_reads
        );
    }

    // AND THE ROW THAT REPORTED ZERO STILL DOES, because it is a different noun and says so.
    // Asserted rather than left implicit: if it ever starts moving, the two numbers have been
    // conflated and the doc on `IndexLogStats::bytes_read` is wrong.
    assert_eq!(
        (small.store_bytes, big.store_bytes),
        (0, 0),
        "the store's record-facing row moved: {} and {}",
        small.store_bytes,
        big.store_bytes
    );
}

/// THE COUNTER'S OWN PLANTED-MARKER CONTROL.
///
/// A counter that always answered zero, or always answered the file's length, would satisfy the
/// agreement above on a fixture built to match it. This plants reads of a size THIS TEST chose,
/// in front of the counter, and requires it to hand back exactly those numbers.
#[test]
fn the_piece_byte_counter_recovers_a_planted_read_exactly() {
    const PLANTED_SMALL: usize = 100_003;
    const PLANTED_LARGE: usize = 300_009;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("planted.bin");
    std::fs::write(&path, vec![7u8; PLANTED_LARGE]).unwrap();

    // An EMPTY span reads exactly zero. Without this, a counter that never moved would pass
    // every difference below.
    let before_empty = index_log_piece_read_counts_on_this_thread();
    let after_empty = index_log_piece_read_counts_on_this_thread();
    assert_eq!(
        before_empty, after_empty,
        "APPARATUS: an empty span charged {before_empty:?} -> {after_empty:?}"
    );

    let mut charged = Vec::new();
    for planted in [PLANTED_SMALL, PLANTED_LARGE] {
        let (bytes_before, reads_before) = index_log_piece_read_counts_on_this_thread();
        let (read, kernel) = kernel_read_span(|| {
            read_index_log_piece_for_test(&path, 0, planted).expect("the planted read must succeed")
        });
        let (bytes_after, reads_after) = index_log_piece_read_counts_on_this_thread();

        assert_eq!(
            read.len(),
            planted,
            "the planted read must hand back what it asked for"
        );
        assert_eq!(
            bytes_after - bytes_before,
            planted as u64,
            "the counter must give back exactly the {planted} bytes planted in front of it"
        );
        assert!(
            reads_after > reads_before,
            "a planted read must be charged at least one read"
        );
        if let Some(rchar) = kernel {
            assert_eq!(
                rchar, planted as u64,
                "the kernel charged this thread {rchar} bytes for a planted read of {planted}"
            );
        }
        charged.push(bytes_after - bytes_before);
    }

    // The DIFFERENCE too, so a counter charging a constant could not pass by charging it twice.
    assert_eq!(
        charged[1] - charged[0],
        (PLANTED_LARGE - PLANTED_SMALL) as u64,
        "the two planted reads differ by {} where they were planted {} apart",
        charged[1] - charged[0],
        PLANTED_LARGE - PLANTED_SMALL
    );
}

/// THE RESIDUAL READS ZERO, AND THAT IS THE ANSWER THAT HAS TO BE DISTRUSTED.
///
/// An instrument that always answered zero would say exactly what the test above says. So this
/// runs the same span again with a read of a file the counter CANNOT see planted inside it, and
/// requires the residual to come back as exactly that many bytes. Zero then means "every byte the
/// kernel charged this thread is claimed", which is a reading rather than an identity.
#[test]
fn the_residual_a_fold_leaves_is_zero_and_a_planted_read_proves_it_is_a_reading() {
    const PLANTED: usize = 1_000_003;

    let _rolling = roll_at(SEGMENT_BYTES);
    let dir = tempfile::tempdir().unwrap();
    build_log(dir.path(), SMALL);

    // Not under the log's root, and not an index-log piece: the counter has no way to see it.
    let unseen_dir = tempfile::tempdir().unwrap();
    let unseen = unseen_dir.path().join("unseen.bin");
    std::fs::write(&unseen, vec![3u8; PLANTED]).unwrap();

    let store = LocalIndexLogStore::new(dir.path());

    let run = |plant: bool| {
        let (bytes_before, _) = index_log_piece_read_counts_on_this_thread();
        let mut applied = 0usize;
        let (_, kernel_rchar) = kernel_read_span(|| {
            store
                .for_each_delta_record_above_anchor(SHARD, 0, 0, |_| applied += 1)
                .expect("the fold must succeed");
            if plant {
                let read = std::fs::read(&unseen).expect("the planted read must succeed");
                assert_eq!(read.len(), PLANTED, "the planted file changed size");
            }
        });
        let (bytes_after, _) = index_log_piece_read_counts_on_this_thread();
        let counted = bytes_after - bytes_before;
        (
            applied,
            counted,
            kernel_rchar.map(|rchar| rchar as i64 - counted as i64),
        )
    };

    let (plain_applied, plain_counted, plain_residual) = run(false);
    let (planted_applied, planted_counted, planted_residual) = run(true);

    println!(
        "the residual a fold leaves, without and with a {PLANTED}-byte read planted inside it\n\
         {:>28}{:>14}{:>14}\n\
         {:>28}{:>14}{:>14}\n\
         {:>28}{:>14?}{:>14?}\n",
        "", "plain", "planted",
        "bytes the counter claimed", plain_counted, planted_counted,
        "residual", plain_residual, planted_residual,
    );

    assert_eq!(
        (plain_applied, planted_applied),
        (SMALL, SMALL),
        "APPARATUS: the two folds applied {plain_applied} and {planted_applied} of {SMALL}"
    );
    assert!(
        plain_counted > 0,
        "APPARATUS: the fold under measurement claimed no byte at all"
    );
    assert_eq!(
        plain_residual,
        Some(0),
        "the fold left {plain_residual:?} unattributed"
    );
    assert_eq!(
        planted_residual,
        Some(PLANTED as i64),
        "a {PLANTED}-byte read planted in the span came back as {planted_residual:?} -- the \
         residual is not a reading"
    );
    assert_eq!(
        plain_counted, planted_counted,
        "the planted read must not be charged to the counter: {plain_counted} against \
         {planted_counted}"
    );
}

/// THE COUNTED HANDLE IS THE ONLY READABLE HANDLE ON A PIECE, AND THIS IS WHAT HOLDS IT SO.
///
/// The compiler enforces most of it: `std::fs::File` is not imported into `index_log.rs`, so a
/// read written the old way -- `File::open(&path)` -- does not resolve. What the compiler cannot
/// refuse is somebody spelling the path out in full, so that spelling is enumerated here and
/// every one of them is a WRITE or a directory handle.
///
/// The negative control is `a_new_uncounted_read_would_fail_this_guard` below.
#[test]
fn every_production_read_of_a_piece_goes_through_the_counted_handle() {
    let source = include_str!("index_log.rs");
    let (production, _tests) = split_at_test_modules(source);

    assert!(
        !production.contains("use std::fs::{self, File, OpenOptions};"),
        "`File` is back in this module's namespace -- a read written the old way would compile"
    );
    assert!(
        production.len() > 40_000,
        "APPARATUS: the production half of index_log.rs came out at {} bytes",
        production.len()
    );

    let offenders = uncounted_reads_in(production);
    assert!(
        offenders.is_empty(),
        "these open a piece for reading without a tally:\n{}",
        offenders.join("\n")
    );

    // THE DENOMINATOR: the guard has to be looking at something. A fold, a scan and a trim at
    // the very least.
    let counted = production.matches("open_index_log_read(").count()
        + production.matches("open_index_log_repair(").count();
    assert!(
        counted >= 8,
        "only {counted} opens go through the counted handle -- the guard is reading the wrong \
         file or the handle has been bypassed wholesale"
    );
}

/// THE NEGATIVE CONTROL: the guard above must go red on an input that deserves it.
///
/// This campaign has found seven checks that could not fail. A guard whose ability to fail is
/// untested is a guard that says nothing.
#[test]
fn a_new_uncounted_read_would_fail_this_guard() {
    let clean = "    let mut reader = BufReader::new(open_index_log_read(&path, &mut tally)?);\n";
    assert!(
        uncounted_reads_in(clean).is_empty(),
        "APPARATUS: the guard flagged a counted read"
    );

    for planted in [
        "        let file = std::fs::File::open(&path)?;\n",
        "        let mut file = OpenOptions::new().read(true).open(&path)?;\n",
        "        let bytes = std::fs::read(&path)?;\n",
    ] {
        assert!(
            !uncounted_reads_in(planted).is_empty(),
            "the guard did not flag {planted:?} -- it cannot go red, so it says nothing"
        );
    }
}

/// Everything in `index_log.rs` before its first `#[cfg(test)]` module.
fn split_at_test_modules(source: &str) -> (&str, &str) {
    // The first two `#[cfg(test)]` in this file are on a probe module and on the counted handle's
    // test-only reader, both of which sit inside the production half. The one that begins the
    // tests is the first `#[cfg(test)]\nmod tests`.
    match source.find("#[cfg(test)]\nmod tests") {
        Some(at) => source.split_at(at),
        None => (source, ""),
    }
}

/// Lines that open something for reading without handing over a tally.
fn uncounted_reads_in(source: &str) -> Vec<String> {
    let mut offenders = Vec::new();
    for (number, line) in source.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") || trimmed.starts_with("///") {
            continue;
        }
        let reads = (line.contains("File::open") && !line.contains("open(parent)"))
            || line.contains(".read(true)")
            || line.contains("fs::read(")
            || line.contains("fs::read_to_string(");
        if !reads {
            continue;
        }
        // The two openers are where the counting lives; everything else must go through them.
        if line.contains("open_index_log_read") || line.contains("open_index_log_repair") {
            continue;
        }
        if line.contains("file: std::fs::File::open(path)?")
            || line.contains("file: OpenOptions::new().read(true).write(true).open(path)?")
        {
            continue;
        }
        offenders.push(format!("  line {}: {}", number + 1, line.trim()));
    }
    offenders
}
