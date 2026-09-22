// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a replay costs in PIECE METADATA, and why that grew faster than the log.
//!
//! `wal_scale` asked what an append costs and what a replay costs, and found the replay
//! superlinear: 4.01x the bytes for 12.45x the `statx`. This module is that aside taken as its own
//! subject -- the quantity named, the fixture regime that decides whether it is visible at all,
//! and the shape the walk holds now.
//!
//! THE QUANTITY. A replay reads the log in bounded windows, and the walk resumed its RECORDS but
//! never its PIECE LIST. Every window took the piece list from a fresh `read_dir`, then walked it
//! from the FRONT, opening each piece and reading its header to learn where the piece begins --
//! so it could conclude the piece was behind the window and step over it. Both the number of
//! windows and the number of pieces grow linearly with the log's bytes, so the metadata probes
//! grew with their PRODUCT:
//!
//! ```text
//!   probes  ~=  windows x pieces  =  bytes/WAL_REPLAY_WINDOW_BYTES x bytes/TS_WAL_SEGMENT_BYTES
//! ```
//!
//! and a replay of a log twice as long cost four times the metadata. The number the walk went to
//! disk for was in the piece's NAME the whole time: `sealed_wal_path` names a sealed piece for the
//! log id its contents start at, and `wal_segment_paths` already parsed that number to SORT by it
//! before discarding it. A piece is behind the window when the NEXT piece's name says it is.
//!
//! Measured with `strace -f -c` on `wal_cost_scale_harness`, 128 incompressible bytes per record,
//! production constants (512 KiB windows, 256 KiB pieces), whole-log replay:
//!
//! ```text
//!   records   log bytes  pieces  windows  records     statx      total syscalls
//!                                         replayed  before  after   before  after
//!    20,000   3,403,490      14        7    20,000     248     75    1,320    983
//!    80,000  13,663,490      54       27    80,000   3,088    255    8,976  3,341
//!   ratio        4.01x                                12.45x  3.40x
//! ```
//!
//! Same windows, same records replayed, same bytes: what changed is only the metadata. At 80,000
//! records the walk had been spending more syscalls on piece metadata than on reading records.
//!
//! THE FIXTURE REGIME, which decides whether any of that is visible. A replay reads the SUFFIX of
//! the log that the loaded index does not already reflect, and the knob is how that suffix is
//! sized against the log:
//!
//! * PROPORTIONAL SUFFIX -- a watermark of zero, the whole log replayed. Windows AND pieces both
//!   grow, the product is quadratic, and the defect is visible against any denominator.
//! * FIXED SUFFIX -- a watermark near the end, which is what a node that has been dumping its
//!   index actually restores from. The replayed work is CONSTANT: one or two pieces, one window.
//!   The metadata cost was still linear in the WHOLE log, because that single window still walked
//!   every piece from the front. Against BYTES that reads as 4.01x for 4.01x -- perfectly linear,
//!   no defect -- and a measurement taken only in this regime, against that denominator, would
//!   have closed the question. Against the work actually done it is unbounded: four times the log
//!   for the same handful of records.
//!
//! Both regimes are measured and asserted below, so neither can be picked by accident later.
//!
//! REACHABILITY, stated before any saving. `load_shard_with` calls `replay_wal_into_shard` on
//! every shard load, in both barrier arms, with no flag in front of it, and rolling is on by
//! default (`DEFAULT_WAL_SEGMENT_BYTES`, 256 KiB) so a production log is in many pieces. The
//! narrow part is the REGIME, not the path: the fixed-suffix arm is the one a real restore takes,
//! and it is the arm where the old cost was linear in the whole retained log rather than
//! quadratic. The quadratic arm is reached by a shard with no usable index checkpoint -- a fresh
//! or async-only shard, or one whose manifest is older than its log.
//!
//! DIRECTION. Replay is the restore path: reading too FEW records is silent data loss, reading too
//! many is merely slow. A count assertion passes while replaying the wrong records, so the
//! identity test below compares the SEQUENCE the walk hands back, element by element, against the
//! same records taken from a walk that skips nothing.

#![cfg(test)]

use crate::types::Command;
use crate::wal::{
    set_wal_segment_bytes_for_test, wal_piece_extents_for_test, LocalWriteAheadLogStore,
    DEFAULT_WAL_SEGMENT_BYTES, WAL_PIECES_SKIPPED_BY_NAME, WAL_PIECE_BODY_READS,
    WAL_PIECE_TAIL_READS, WAL_READ_FILE_OPENS, WAL_SEGMENT_HEADER_READS,
    WAL_SEGMENT_LISTINGS, WAL_SEGMENT_LISTING_ENTRIES,
};

/// The window the engine replays with (`engine::lifecycle::WAL_REPLAY_WINDOW_BYTES`) and the size
/// it rolls at (`DEFAULT_WAL_SEGMENT_BYTES`), both scaled down by eight so a test log of a few
/// hundred kilobytes still crosses several windows and several pieces. The shape being measured is
/// windows-against-pieces, and that shape is set by the RATIO of these two, which is preserved:
/// 512 KiB / 256 KiB in production, 64 KiB / 32 KiB here.
const TEST_WINDOW_BYTES: u64 = 64 * 1024;
const TEST_SEGMENT_BYTES: u64 = 32 * 1024;

/// xorshift64*, seeded per record. Records are compressed, so a corpus of repeated bytes holds a
/// fraction of the bytes its value size implies and moves neither the piece count nor the window
/// count where it is meant to.
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

fn build_log(dir: &std::path::Path, records: usize, value_bytes: usize) -> LocalWriteAheadLogStore {
    let store = LocalWriteAheadLogStore::new(dir);
    let mut index = 0usize;
    while index < records {
        store
            .append(
                1,
                Command::StringSet {
                    key: format!("k{index:08}"),
                    value: incompressible(value_bytes, index as u64),
                },
            )
            .unwrap();
        index += 1;
    }
    store
}

fn reset_counters() {
    WAL_SEGMENT_LISTINGS.with(|value| value.set(0));
    WAL_SEGMENT_LISTING_ENTRIES.with(|value| value.set(0));
    WAL_SEGMENT_HEADER_READS.with(|value| value.set(0));
    WAL_PIECES_SKIPPED_BY_NAME.with(|value| value.set(0));
    WAL_PIECE_BODY_READS.with(|value| value.set(0));
    WAL_PIECE_TAIL_READS.with(|value| value.set(0));
    WAL_READ_FILE_OPENS.with(|value| value.set(0));
}

/// Every counter, as it stands.
#[derive(Clone, Copy, Default)]
struct Counters {
    listings: u64,
    /// Directory ENTRIES the listings walked, summed. The work, as against the call count.
    listing_entries: u64,
    header_reads: u64,
    skipped_by_name: u64,
    body_reads: u64,
    tail_reads: u64,
    read_opens: u64,
}

fn counters() -> Counters {
    Counters {
        listings: WAL_SEGMENT_LISTINGS.with(|value| value.get()),
        listing_entries: WAL_SEGMENT_LISTING_ENTRIES.with(|value| value.get()),
        header_reads: WAL_SEGMENT_HEADER_READS.with(|value| value.get()),
        skipped_by_name: WAL_PIECES_SKIPPED_BY_NAME.with(|value| value.get()),
        body_reads: WAL_PIECE_BODY_READS.with(|value| value.get()),
        tail_reads: WAL_PIECE_TAIL_READS.with(|value| value.get()),
        read_opens: WAL_READ_FILE_OPENS.with(|value| value.get()),
    }
}

/// What one whole replay cost, and what it handed back.
///
/// A replay is TWO walks with two different shapes, and adding them up hides both. The PRE-WALK
/// (`log_id_after_sequence`) runs once and asks each piece for its last SEQUENCE. The WINDOWED
/// SCAN then runs once per window and asks each piece where it starts, which is a LOG ID. Only
/// the second is per-window, so only the second was ever the product; they are kept apart here
/// so a change to one cannot be read as a change to the other.
struct ReplayCost {
    windows: u64,
    /// `(log id, sequence)` for every record the replay was handed, IN ORDER. The identity test
    /// compares these, not their length: a walk that replayed the wrong records has the right
    /// count.
    handed_back: Vec<(u64, u64)>,
    /// What the pre-walk cost, on its own.
    prewalk: Counters,
    /// The whole replay, pre-walk included.
    total: Counters,
}

impl ReplayCost {
    fn records(&self) -> u64 {
        self.handed_back.len() as u64
    }

    /// Header reads taken by the WINDOWED SCAN -- the per-window walk, which is the subject.
    fn scan_header_reads(&self) -> u64 {
        self.total.header_reads - self.prewalk.header_reads
    }

    /// What the windowed scan's header reads WOULD have been without the name-skip,
    /// reconstructed rather than remembered.
    ///
    /// A piece skipped on its name is exactly a piece the old walk opened, read a header from, and
    /// then stepped over on the length test -- one header read each, no more and no fewer. So the
    /// old cost is this sum, at any fixture, with no recorded constant that could go stale. It is
    /// checked against the numbers `wal_scale` printed before the change: 45 and 466.
    fn scan_header_reads_before_the_skip(&self) -> u64 {
        self.scan_header_reads() + self.total.skipped_by_name
    }
}

/// Replay `store` the way `replay_wal_into_shard_windowed` does: start past the pieces the
/// watermark already covers, then bounded windows resuming where the last stopped, verifying the
/// tail only on the first.
fn replay_cost(store: &LocalWriteAheadLogStore, watermark: u64) -> ReplayCost {
    reset_counters();
    let start_at = store
        .replay_start_after_sequence(1, watermark)
        .unwrap_or_else(|_| crate::wal::ReplayPosition::at_start_of_log());
    let prewalk = counters();
    let mut window_start = start_at;
    let mut verify_tail = true;
    let mut windows = 0u64;
    let mut handed_back = Vec::new();
    loop {
        let (scanned, more_to_come, resume_at) = store
            .scan_decoded_window(1, window_start, TEST_WINDOW_BYTES, verify_tail)
            .unwrap();
        verify_tail = false;
        windows += 1;
        for (log_id, record) in &scanned {
            handed_back.push((*log_id, record.sequence));
        }
        if !more_to_come {
            break;
        }
        window_start = resume_at;
    }
    ReplayCost { windows, handed_back, prewalk, total: counters() }
}

/// How many pieces the log is in, and how many records it holds.
fn piece_count(store: &LocalWriteAheadLogStore, dir: &std::path::Path) -> usize {
    let _ = store;
    wal_piece_extents_for_test(dir, 1).len()
}

fn print_cost(label: &str, bytes: u64, pieces: usize, cost: &ReplayCost) {
    println!(
        "    {label:<8} {bytes:>9} B, {pieces:>3} pieces, {:>3} windows, {:>6} records | \
         SCAN {:>5} header reads ({:>5} before the skip), {:>5} skipped by name, {:>4} bodies | \
         PRE-WALK {:>4} piece tails, {:>4} header reads | {:>5} read opens, {:>4} listings",
        cost.windows,
        cost.records(),
        cost.scan_header_reads(),
        cost.scan_header_reads_before_the_skip(),
        cost.total.skipped_by_name,
        cost.total.body_reads,
        cost.prewalk.tail_reads,
        cost.prewalk.header_reads,
        cost.total.read_opens,
        cost.total.listings
    );
}

/// The apparatus and the regime, asserted before anything is concluded from a ratio.
///
/// A cost measured where it cannot occur reads exactly like a low cost: a log in ONE piece has no
/// piece to step over, a replay of an EMPTY suffix walks nothing, and both would report a flat,
/// perfect result about nothing at all.
fn assert_in_regime(label: &str, pieces: usize, cost: &ReplayCost) {
    assert!(
        pieces > 1,
        "{label}: the log is in {pieces} piece(s). A walk that steps over pieces behind the window \
         cannot cost anything on a log that has none, so every figure here would be a zero about \
         nothing"
    );
    assert!(
        cost.records() > 0,
        "{label}: the replay was handed {} records. An empty suffix is not a replay",
        cost.records()
    );
    assert!(
        cost.windows >= 1 && cost.total.body_reads >= 1,
        "{label}: {} windows and {} pieces opened for records; this is not timing a replay",
        cost.windows,
        cost.total.body_reads
    );
    assert!(
        cost.total.listings >= cost.windows,
        "{label}: {} listings for {} windows -- the listing counter is not counting, so the \
         residual below means nothing",
        cost.total.listings,
        cost.windows
    );
}

/// PROPORTIONAL SUFFIX: the whole log replayed, so the replayed portion grows with the store.
///
/// This is the regime `wal_scale`'s aside was measured in, and the one where the old shape is
/// unmissable: four times the bytes cost 10.36x the piece-header reads in-process and 12.45x the
/// `statx` on the real binary. Windows and pieces both grow, and the walk paid their product.
///
/// What is asserted now is that the product is gone -- the header reads track the PIECES, which
/// track the bytes -- with the pre-fix quantity reconstructed from the skip counter beside it so
/// the test says what it fixed rather than only what it holds.
#[test]
fn a_whole_log_replay_reads_each_pieces_header_about_once() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small_records = 2_000usize;
    let large_records = 8_000usize;

    let small_dir = tempfile::tempdir().unwrap();
    let small = build_log(small_dir.path(), small_records, 128);
    let small_bytes = small.raw_stats(1).bytes_written;
    let small_pieces = piece_count(&small, small_dir.path());
    let small_cost = replay_cost(&small, 0);

    let large_dir = tempfile::tempdir().unwrap();
    let large = build_log(large_dir.path(), large_records, 128);
    let large_bytes = large.raw_stats(1).bytes_written;
    let large_pieces = piece_count(&large, large_dir.path());
    let large_cost = replay_cost(&large, 0);
    set_wal_segment_bytes_for_test(None);

    let byte_ratio = large_bytes as f64 / small_bytes.max(1) as f64;
    let piece_ratio = large_pieces as f64 / small_pieces.max(1) as f64;
    let walk_ratio = large_cost.scan_header_reads() as f64
        / small_cost.scan_header_reads().max(1) as f64;
    let before_ratio = large_cost.scan_header_reads_before_the_skip() as f64
        / small_cost.scan_header_reads_before_the_skip().max(1) as f64;

    println!("  PROPORTIONAL SUFFIX -- watermark 0, the whole log replayed");
    print_cost("small", small_bytes, small_pieces, &small_cost);
    print_cost("large", large_bytes, large_pieces, &large_cost);
    println!(
        "    ratio: {byte_ratio:.2}x bytes, {piece_ratio:.2}x pieces, \
         {walk_ratio:.2}x scan header reads, {before_ratio:.2}x before the skip"
    );
    println!(
        "    per window: {:.1} scan header reads small, {:.1} large",
        small_cost.scan_header_reads() as f64 / small_cost.windows.max(1) as f64,
        large_cost.scan_header_reads() as f64 / large_cost.windows.max(1) as f64
    );

    assert_in_regime("small", small_pieces, &small_cost);
    assert_in_regime("large", large_pieces, &large_cost);
    assert!(
        small_cost.windows >= 2 && large_cost.windows >= 4,
        "the two replays took {} and {} windows; a replay that fits in one window cannot show \
         anything about a PER-WINDOW cost -- and, as the fixed-suffix test beside this one shows, \
         it cannot show the superlinearity either, because one window times P pieces is P",
        small_cost.windows,
        large_cost.windows
    );
    assert!(
        large_cost.total.skipped_by_name > 0,
        "no piece was skipped on its name over {} windows and {large_pieces} pieces. The skip is \
         the thing under test and it did not happen, so the flat ratio below is about nothing",
        large_cost.windows
    );

    // The treatment was applied: the log really is four times longer, in four times the pieces.
    assert!(
        byte_ratio > 3.5 && byte_ratio < 4.5,
        "the two logs differ by {byte_ratio:.2}x in bytes, not the 4x this fixture is sized for"
    );

    // THE RESULT. The header reads track the PIECES, not windows-times-pieces. The bound is
    // deliberately loose enough that ordinary work does not move it and tight enough that the
    // product cannot hide inside it: at this fixture the product would be ~10x.
    assert!(
        walk_ratio < byte_ratio * 1.5,
        "the scan's header reads grew {walk_ratio:.2}x for {byte_ratio:.2}x the bytes. That is the \
         windows-times-pieces shape coming back: every window is walking the piece list from the \
         front again instead of stepping over the pieces the next piece's NAME says are behind it"
    );
    assert!(
        large_cost.scan_header_reads() <= (large_pieces as u64 + large_cost.windows) * 2,
        "the scan read {} headers over {large_pieces} pieces in {} windows. One header read per \
         piece is the shape; a multiple of the WINDOWS on top of it is the old product returning",
        large_cost.scan_header_reads(),
        large_cost.windows
    );
    // The per-window cost is FLAT. This is the identity the ratio above is a consequence of, and
    // it is the one that does not depend on the fixture being sized 4x.
    let small_per_window = small_cost.scan_header_reads() as f64 / small_cost.windows as f64;
    let large_per_window = large_cost.scan_header_reads() as f64 / large_cost.windows as f64;
    assert!(
        large_per_window < small_per_window * 1.5,
        "a window of the four-times-longer log read {large_per_window:.1} piece headers against \
         {small_per_window:.1}. What a window costs must not depend on how much log is in front \
         of it; that dependence IS the product"
    );

    // And what it replaced, reconstructed from the skip counter rather than remembered as a
    // constant. This is the measurement, and it is why the loose bound above is not vacuous.
    assert!(
        before_ratio > byte_ratio * 1.5,
        "without the name-skip this walk would have grown {before_ratio:.2}x for {byte_ratio:.2}x \
         the bytes, which is not the superlinear shape this fixture is supposed to produce. \
         Either the fixture stopped crossing enough pieces or the skip counter is counting \
         something else, and in both cases the {walk_ratio:.2}x above proves nothing"
    );
}

/// FIXED SUFFIX: a watermark near the end, which is the regime a real restore is in -- AND THE
/// REGIME THAT HIDES THE SUPERLINEARITY COMPLETELY.
///
/// This is the trap, and it is the same one `wal_scale`'s proportional fixture would have fallen
/// into with the arms swapped. A node that has been dumping its index restores from a watermark
/// close to the log's tail, so the replayed WORK is a constant -- a piece or two, ONE window --
/// however long the retained log has grown. One window times P pieces is P. The product that makes
/// a whole-log replay quadratic collapses to a straight line, and the numbers come out:
///
/// ```text
///                                   scan header reads   ratio      verdict
///   FIXED SUFFIX   (this test)             9 ->    40   4.44x   "linear in the bytes, healthy"
///   PROPORTIONAL   (the test above)       45 ->   466  10.36x   "superlinear, defective"
/// ```
///
/// Same code, same store, same corpus ratio, opposite conclusions. A measurement taken only here,
/// against bytes, closes the question and reports no defect -- so the knob is the RATIO OF THE
/// REPLAYED SUFFIX TO THE LOG, and this test exists so that neither setting of it can be picked by
/// accident later.
///
/// What is still true in this regime, and what this test asserts against the OTHER denominator:
/// the work does not move and the cost did, four-fold, because a single window walked every piece
/// in front of it. The name-skip removes that. What it does NOT remove is the pre-walk beside it:
/// `log_id_after_sequence` asks every piece for its last SEQUENCE, and a sequence is not in any
/// piece's name. That walk is LINEAR in the log for constant work, it is measured here rather than
/// folded in, and it is deliberately not touched -- see the module header.
#[test]
fn a_fixed_tail_replay_no_longer_pays_for_the_log_in_front_of_it() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small_records = 2_000usize;
    let large_records = 8_000usize;

    let small_dir = tempfile::tempdir().unwrap();
    let small = build_log(small_dir.path(), small_records, 128);
    let small_bytes = small.raw_stats(1).bytes_written;
    let small_pieces = piece_count(&small, small_dir.path());
    // Everything but the last 200 records already covered by the index. `log_id_after_sequence`
    // is conservative -- it starts at the first piece the watermark does not cover -- so the
    // replayed portion is a piece or so at BOTH sizes, which is the point.
    let small_cost = replay_cost(&small, small_records as u64 - 200);

    let large_dir = tempfile::tempdir().unwrap();
    let large = build_log(large_dir.path(), large_records, 128);
    let large_bytes = large.raw_stats(1).bytes_written;
    let large_pieces = piece_count(&large, large_dir.path());
    let large_cost = replay_cost(&large, large_records as u64 - 200);
    set_wal_segment_bytes_for_test(None);

    let byte_ratio = large_bytes as f64 / small_bytes.max(1) as f64;
    let work_ratio = large_cost.records() as f64 / small_cost.records().max(1) as f64;
    let walk_ratio = large_cost.scan_header_reads() as f64
        / small_cost.scan_header_reads().max(1) as f64;
    let before_ratio = large_cost.scan_header_reads_before_the_skip() as f64
        / small_cost.scan_header_reads_before_the_skip().max(1) as f64;
    let prewalk_ratio =
        large_cost.prewalk.tail_reads as f64 / small_cost.prewalk.tail_reads.max(1) as f64;

    println!("  FIXED SUFFIX -- the last 200 records replayed, however long the log is");
    print_cost("small", small_bytes, small_pieces, &small_cost);
    print_cost("large", large_bytes, large_pieces, &large_cost);
    println!(
        "    ratio: {byte_ratio:.2}x bytes, {work_ratio:.2}x records replayed, \
         {walk_ratio:.2}x scan header reads, {before_ratio:.2}x before the skip, \
         {prewalk_ratio:.2}x pre-walk piece tails"
    );
    println!(
        "    SCAN header reads PER REPLAYED RECORD: {:.4} small, {:.4} large \
         (before the skip: {:.4} and {:.4})",
        small_cost.scan_header_reads() as f64 / small_cost.records().max(1) as f64,
        large_cost.scan_header_reads() as f64 / large_cost.records().max(1) as f64,
        small_cost.scan_header_reads_before_the_skip() as f64 / small_cost.records().max(1) as f64,
        large_cost.scan_header_reads_before_the_skip() as f64 / large_cost.records().max(1) as f64
    );

    assert_in_regime("small", small_pieces, &small_cost);
    assert_in_regime("large", large_pieces, &large_cost);

    // THE CONTROL HELD: the log really did grow four-fold.
    assert!(
        byte_ratio > 3.5 && byte_ratio < 4.5,
        "the two logs differ by {byte_ratio:.2}x in bytes, not the 4x this fixture is sized for"
    );
    // AND THE WORK REALLY DID NOT: this is a FIXED suffix, not a proportional one. Without this
    // the test is the proportional arm again under a different name.
    assert!(
        work_ratio < 1.6,
        "the large replay handed back {} records against {} -- {work_ratio:.2}x. This fixture is \
         supposed to hold the replayed work FIXED while the log grows; it did not, so it is \
         measuring the proportional regime",
        large_cost.records(),
        small_cost.records()
    );
    // AND IT REALLY IS ONE WINDOW, which is what makes this the regime that hides the product.
    assert_eq!(
        (small_cost.windows, large_cost.windows),
        (1, 1),
        "this fixture took {} and {} windows. The hiding is the arithmetic of ONE window: windows \
         times pieces is what grows quadratically, and at one window it is just the pieces",
        small_cost.windows,
        large_cost.windows
    );

    // THE RESULT, against the denominator that does NOT hide it: the same work costs the same
    // metadata, whatever is in front of it.
    assert!(
        walk_ratio < 2.0,
        "four times the log, the same {} records to replay, and {walk_ratio:.2}x the scan's header \
         reads. The window is still walking the pieces in front of it one header at a time, which \
         makes a restore slower the longer the node has been running -- the worst place for it",
        large_cost.records()
    );
    assert!(
        large_cost.scan_header_reads() <= 8,
        "{} header reads to scan {} records out of a {large_pieces}-piece log. A fixed tail \
         should touch the pieces it reads and the one after them, not the log",
        large_cost.scan_header_reads(),
        large_cost.records()
    );

    // THE EXACT IDENTITY, at both sizes: EVERY piece in front of the one the window starts in is
    // stepped over on its name. Two are not -- the piece the window starts in, and the piece being
    // written, whose name carries no number for the one before it to read. A bound would be
    // satisfied by a walk that skipped one piece fewer at every window; this is not.
    assert_eq!(
        (small_cost.total.skipped_by_name as usize, large_cost.total.skipped_by_name as usize),
        (small_pieces - 2, large_pieces - 2),
        "the walk skipped {} of {small_pieces} pieces and {} of {large_pieces} on their names. \
         The window starts exactly at a piece boundary here -- `log_id_after_sequence` answers a \
         piece start -- so every piece before it ends AT the window and must be stepped over. One \
         short at each size is a boundary that stopped being inclusive; more than that is a piece \
         being opened that nothing needs",
        small_cost.total.skipped_by_name,
        large_cost.total.skipped_by_name
    );

    // AND THE TRAP, asserted so it cannot be walked into again. In THIS regime the pre-fix cost
    // grew at the rate the BYTES did -- perfectly linear, the shape a measurement taken here would
    // have called healthy and closed the question on. The defect is only visible against the work,
    // which did not move at all.
    assert!(
        (before_ratio - byte_ratio).abs() < byte_ratio * 0.5,
        "before the skip this fixed-tail replay's cost grew {before_ratio:.2}x against \
         {byte_ratio:.2}x the bytes. Those two being the SAME number is the whole trap: in this \
         regime the old walk reads as perfectly linear and reports no defect, and it is only \
         against the {work_ratio:.2}x of work actually done that it is visible at all. If the \
         fixture stops producing that, it has stopped standing in for a real restore"
    );

    // THE PART THIS CHANGE DOES NOT FIX, measured rather than mentioned. `log_id_after_sequence`
    // asks every piece for its last SEQUENCE, and no piece's name carries a sequence, so the name
    // cannot answer it. That walk is LINEAR in the log for a constant amount of replayed work --
    // a real cost on a real restore, a DIFFERENT quantity from the product this change removes,
    // and one whose fix lands on the single decision that, taken wrong, starts a restore too late
    // and loses acknowledged writes. It gets its own change. Asserted here so it cannot quietly
    // get worse, and so nobody reads the saving above as covering it.
    assert!(
        prewalk_ratio > 2.5,
        "the pre-walk read {} piece tails against {} -- {prewalk_ratio:.2}x for {byte_ratio:.2}x \
         the bytes. This is recorded as LINEAR-IN-THE-LOG and declined on purpose; if it is no \
         longer linear, something changed `log_id_after_sequence` and this comment is now wrong",
        large_cost.prewalk.tail_reads,
        small_cost.prewalk.tail_reads
    );
    assert!(
        large_cost.prewalk.tail_reads as usize >= large_pieces - 1,
        "the pre-walk read {} piece tails out of {large_pieces} pieces; it is supposed to walk \
         essentially the whole log to find where a near-the-end watermark starts, and that is the \
         cost being priced",
        large_cost.prewalk.tail_reads
    );
}

/// Every read-side open a replay makes is accounted for, at both sizes, per record.
///
/// The total comes from `WAL_READ_FILE_OPENS`, which counts inside `open_wal_read` -- the one
/// primitive every read-side open in the log goes through -- and NOT from adding the rows up. The
/// rows (piece headers, piece bodies) are subtracted from it, so an open belonging to no row
/// survives as residual instead of vanishing.
///
/// The assertion is on the residual PER RECORD at the two sizes. Fixed overhead -- the tail
/// verification, the pre-walk that finds where to start, the process's own startup -- divides away
/// across a four-fold corpus; anything that grows with the log does not.
#[test]
fn every_read_open_in_a_replay_is_accounted_for_at_both_sizes() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small_records = 2_000usize;
    let large_records = 8_000usize;

    let small_dir = tempfile::tempdir().unwrap();
    let small = build_log(small_dir.path(), small_records, 128);
    let small_pieces = piece_count(&small, small_dir.path());
    let large_dir = tempfile::tempdir().unwrap();
    let large = build_log(large_dir.path(), large_records, 128);
    let large_pieces = piece_count(&large, large_dir.path());

    // BOTH regimes, because they walk different things and a residual taken in only one of them
    // audits only one of them. The fixed-tail arm is the one whose pre-walk dominates.
    let arms: [(&str, u64, u64); 2] = [
        ("proportional", 0, 0),
        ("fixed tail", small_records as u64 - 200, large_records as u64 - 200),
    ];
    let mut checked = 0usize;
    for (regime, small_watermark, large_watermark) in arms {
        let small_cost = replay_cost(&small, small_watermark);
        let large_cost = replay_cost(&large, large_watermark);

        // The rows: every read-side open belongs to a piece header, a piece body, or a piece tail.
        // Summed here to be SUBTRACTED from the outer total, never to stand in for it.
        let small_rows = small_cost.total.header_reads
            + small_cost.total.body_reads
            + small_cost.total.tail_reads;
        let large_rows = large_cost.total.header_reads
            + large_cost.total.body_reads
            + large_cost.total.tail_reads;
        let small_residual = small_cost.total.read_opens as i64 - small_rows as i64;
        let large_residual = large_cost.total.read_opens as i64 - large_rows as i64;
        let small_per_record = small_residual as f64 / small_cost.records().max(1) as f64;
        let large_per_record = large_residual as f64 / large_cost.records().max(1) as f64;

        println!("  {regime}: read-side opens, and what no row accounts for");
        print_cost("small", small.raw_stats(1).bytes_written, small_pieces, &small_cost);
        print_cost("large", large.raw_stats(1).bytes_written, large_pieces, &large_cost);
        println!(
            "    small: {} opens - {small_rows} attributed = {small_residual} residual over {} \
             records, {small_per_record:.5} per record",
            small_cost.total.read_opens,
            small_cost.records()
        );
        println!(
            "    large: {} opens - {large_rows} attributed = {large_residual} residual over {} \
             records, {large_per_record:.5} per record",
            large_cost.total.read_opens,
            large_cost.records()
        );

        assert_in_regime("small", small_pieces, &small_cost);
        assert_in_regime("large", large_pieces, &large_cost);
        assert!(
            small_cost.total.read_opens > 0 && large_cost.total.read_opens > 0,
            "{regime}: the outer counter saw {} and {} opens for replays that opened pieces; it is \
             not counting, and a residual taken against zero is a number about nothing",
            small_cost.total.read_opens,
            large_cost.total.read_opens
        );

        // The outer total is a TOTAL: it contains the rows, so it cannot come out below them.
        assert!(
            small_residual >= 0 && large_residual >= 0,
            "{regime}: the attributed rows came to more than the total opens ({small_rows} and \
             {large_rows} against {} and {}). The rows are an audit OF the total, so one of them \
             is counting somewhere no open happens",
            small_cost.total.read_opens,
            large_cost.total.read_opens
        );

        // THE RESULT: what is left over does not grow with the corpus. Fixed overhead -- the tail
        // verification, the process's own startup -- divides away across a four-fold record count;
        // anything that scales with the log does not.
        assert!(
            (large_per_record - small_per_record).abs() < 0.01,
            "{regime}: the unattributed read opens came to {small_per_record:.5} per record at \
             {small_records} and {large_per_record:.5} at {large_records}. A residual that CLIMBS \
             per record is work inside the replay that none of the named rows is watching -- which \
             is exactly how the piece-header walk went unnoticed in the first place"
        );
        checked += 1;
    }
    set_wal_segment_bytes_for_test(None);

    assert_eq!(checked, arms.len(), "{checked} of {} regimes audited", arms.len());
    assert!(checked >= 2, "both regimes have to be audited, not {checked}");
}

/// A replay hands back the same records, in the same order, as a walk that skips nothing.
///
/// THE DIRECTION ARGUMENT. On a restore path, replaying too FEW records loses acknowledged writes
/// and is invisible -- the shard loads clean and the value is simply absent -- while replaying too
/// many is merely slow. Every assertion about a COUNT passes just as happily on a walk that
/// replayed the wrong records, so this compares the `(log id, sequence)` sequence itself.
///
/// The control is an UNWINDOWED walk of the whole log -- one `scan_decoded` call from log id zero
/// with no budget. It is skip-free by construction and not by assumption: the skip fires on
/// `next_start <= start_offset`, and a `start_offset` of zero needs a piece after the first to
/// start at log id zero, which would mean two pieces owning the same address. The skip counter is
/// asserted to be zero on the control arm so that stays a fact rather than an argument. A WINDOWED
/// walk would not do: its second and later windows resume at a non-zero offset and skip like any
/// other, which is exactly what a control must not share with its subject.
#[test]
fn a_windowed_replay_hands_back_exactly_the_records_at_or_after_its_start() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let records = 4_000usize;
    let dir = tempfile::tempdir().unwrap();
    let store = build_log(dir.path(), records, 128);
    let pieces = piece_count(&store, dir.path());

    // The control: every record in the log, from ONE unwindowed walk, in which nothing is skipped.
    reset_counters();
    let (all_records, control_truncated) = store.scan_decoded(1, 0, u64::MAX, u64::MAX).unwrap();
    let control_skips = WAL_PIECES_SKIPPED_BY_NAME.with(|value| value.get());
    let whole_handed_back: Vec<(u64, u64)> = all_records
        .iter()
        .map(|(log_id, record)| (*log_id, record.sequence))
        .collect();

    // Watermarks spread across the log, so the skip fires at a different depth each time and a
    // rule that is off by one piece cannot hide in a single lucky alignment.
    let watermarks = [0u64, 1, 500, 1_999, 2_000, 3_500, records as u64 - 1];
    let mut checked = 0usize;
    for watermark in watermarks {
        let start_at = store.log_id_after_sequence(1, watermark).unwrap();
        let tail = replay_cost(&store, watermark);
        let expected: Vec<(u64, u64)> = whole_handed_back
            .iter()
            .copied()
            .filter(|(log_id, _)| *log_id >= start_at)
            .collect();
        println!(
            "    watermark {watermark:>5}: start_at {start_at:>8}, {} records handed back, \
             {} expected, {} pieces skipped by name",
            tail.handed_back.len(),
            expected.len(),
            tail.total.skipped_by_name
        );
        assert_eq!(
            tail.handed_back, expected,
            "at watermark {watermark} the windowed replay handed back a DIFFERENT SEQUENCE of \
             records from the whole-log walk filtered to log ids at or after {start_at}. On a \
             restore path that is either a lost acknowledged write or a record applied twice, and \
             neither shows up in a count"
        );
        assert!(
            !expected.is_empty(),
            "watermark {watermark} left nothing to replay, so the comparison above is between two \
             empty vectors and proves nothing"
        );
        checked += 1;
    }
    set_wal_segment_bytes_for_test(None);

    println!(
        "  {checked} watermarks checked over a {pieces}-piece log of {} records; the unwindowed \
         control walk skipped {control_skips} pieces by name",
        whole_handed_back.len()
    );

    // FLOORED against the literal list above, so a watermark deleted from it fails here rather
    // than quietly narrowing the test.
    assert_eq!(
        checked,
        watermarks.len(),
        "{checked} of {} watermarks were checked",
        watermarks.len()
    );
    assert!(
        checked >= 7 && pieces > 4,
        "{checked} watermarks over a {pieces}-piece log: this is meant to cross several pieces at \
         several depths"
    );
    assert_eq!(
        control_skips, 0,
        "the unwindowed control walk skipped {control_skips} pieces on their names. The control is \
         supposed to be the arm the skip cannot touch; if it can, it is not a control"
    );
    assert!(
        !control_truncated,
        "the control walk ran out of budget and returned a PREFIX of the log; every comparison \
         above would then be against a truncated expectation"
    );
    assert_eq!(
        whole_handed_back.len(),
        records,
        "the control walk was handed {} of {records} records",
        whole_handed_back.len()
    );
}

/// The two claims the name-skip rests on, asserted on a real log rather than argued from the code.
///
/// A sealed piece's NAME agrees with its HEADER, and no piece's contents reach into the next
/// piece's range. The skip reads the first and concludes the second: if the piece after this one
/// starts at or below the window, this one ends at or below it too. Break either claim and the
/// walk steps over a piece that still holds records the caller asked for -- silently, on the
/// restore path.
#[test]
fn a_sealed_pieces_name_agrees_with_its_header_and_pieces_do_not_overlap() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let dir = tempfile::tempdir().unwrap();
    let store = build_log(dir.path(), 4_000, 128);
    let extents = wal_piece_extents_for_test(dir.path(), 1);
    set_wal_segment_bytes_for_test(None);
    let _ = store.raw_stats(1);

    let named = extents.iter().filter(|(_, name, _, _)| name.is_some()).count();
    println!("  {} pieces, {named} of them sealed and named", extents.len());
    for (path, name, base, end) in &extents {
        println!(
            "    {:<44} name {:>10} header base {:>10} ends {:>10}",
            path.file_name().unwrap().to_string_lossy(),
            name.map(|value| value.to_string()).unwrap_or_else(|| "-".to_string()),
            base,
            end
        );
    }

    assert!(
        named >= 4,
        "{named} sealed pieces: with fewer than a few there is no adjacency to check and this \
         test passes on a log the skip could never fire on"
    );

    for (path, name, base, _) in &extents {
        if let Some(name) = name {
            assert_eq!(
                *name, *base,
                "{} is named for log id {name} and its header says {base}. The skip decides from \
                 the NAME what the header would have said, so a piece whose name lies is a piece \
                 the walk steps over while it still holds records",
                path.display()
            );
        }
    }

    for window in extents.windows(2) {
        let (path, _, _, end) = &window[0];
        let (next_path, next_name, next_base, _) = &window[1];
        let next_start = next_name.unwrap_or(*next_base);
        assert!(
            *end <= next_start,
            "{} ends at log id {end} and {} starts at {next_start}. Overlapping pieces would hand \
             the same log id to two different records, and the skip -- which takes the next \
             piece's start as this piece's end -- would step over live records",
            path.display(),
            next_path.display()
        );
    }
}

/// The path is on the DEFAULT load, and the default constants put a log in many pieces.
///
/// Stated as a test rather than as prose in the header, because a saving that only lands on a
/// non-default configuration is a saving nobody gets, and the two numbers that decide it here are
/// ordinary constants somebody could change.
#[test]
fn the_default_rolling_threshold_puts_a_replayed_log_in_many_pieces() {
    // The engine's own window, not a scaled one: this is about the shipped configuration.
    const PRODUCTION_WINDOW_BYTES: u64 = 512 * 1024;
    assert!(
        DEFAULT_WAL_SEGMENT_BYTES > 0,
        "rolling is OFF by default ({DEFAULT_WAL_SEGMENT_BYTES}), which would put a default log in \
         ONE piece -- and this whole subject would then be reachable only on a non-default setting"
    );
    let log_bytes = 64u64 * 1024 * 1024;
    let pieces = log_bytes / DEFAULT_WAL_SEGMENT_BYTES;
    let windows = log_bytes / PRODUCTION_WINDOW_BYTES;
    println!(
        "  at the shipped defaults a {log_bytes} B log is {pieces} pieces replayed in {windows} \
         windows: {} piece-header reads before the name-skip, about {pieces} after",
        pieces * windows / 2
    );
    assert!(
        pieces > 100 && windows > 50,
        "a {log_bytes} B log comes to {pieces} pieces in {windows} windows at the defaults; the \
         product this change removes is only worth removing if both of them grow"
    );
}

/// THE PRODUCT THAT IS STILL BEING PAID, one directory entry at a time.
///
/// #1917 stopped a window re-reading the HEADER of every piece behind it, and #1936 stopped it
/// re-reading their CONTENTS. Both of those were the same product -- windows x pieces -- and both
/// are gone. What neither touched is the step in front of them: every window takes its piece list
/// from a fresh `read_dir` of the log's directory, and that listing walks, name-matches and sorts
/// EVERY piece in the log. The walk is resumed in its records and in its skip decision; the
/// LISTING is not resumed at all.
///
/// So the product survives, in the entries:
///
/// ```text
///   entries  ~=  windows x pieces  =  bytes/WAL_REPLAY_WINDOW_BYTES x bytes/TS_WAL_SEGMENT_BYTES
/// ```
///
/// WHY NO MEASUREMENT HAS SEEN IT. Two reasons, and they compound.
///
/// First, the counter. `WAL_SEGMENT_LISTINGS` counts the CALL, and a walk that lists once per
/// window makes exactly as many calls as it has windows -- linear, healthy, and silent about the
/// directory behind each one. The entries were never counted, so the only in-process number for
/// this quantity reported the wrong shape.
///
/// Second, the syscalls. One `getdents64` returns as many entries as fit in the kernel's buffer,
/// which for these names is several hundred, so on a log of 14, 54 or 67 pieces -- every size this
/// log has ever been measured at -- the whole directory comes back in one call and the syscall
/// count is flat. The entries behind it are not, and a corpus large enough to need a second
/// `getdents64` is the first place the two numbers separate.
///
/// The assertion is therefore on the ENTRIES against the LISTINGS at two sizes: the listings track
/// the windows, the entries track their product with the pieces, and a test that watched only the
/// listings would call this flat.
#[test]
fn every_replay_window_lists_the_whole_directory_again() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small_records = 2_000usize;
    let large_records = 8_000usize;

    let small_dir = tempfile::tempdir().unwrap();
    let small = build_log(small_dir.path(), small_records, 128);
    let small_bytes = small.raw_stats(1).bytes_written;
    let small_pieces = piece_count(&small, small_dir.path());
    let small_cost = replay_cost(&small, 0);

    let large_dir = tempfile::tempdir().unwrap();
    let large = build_log(large_dir.path(), large_records, 128);
    let large_bytes = large.raw_stats(1).bytes_written;
    let large_pieces = piece_count(&large, large_dir.path());
    let large_cost = replay_cost(&large, 0);

    println!("  every_replay_window_lists_the_whole_directory_again:");
    print_cost("small", small_bytes, small_pieces, &small_cost);
    print_cost("large", large_bytes, large_pieces, &large_cost);

    assert_in_regime("small", small_pieces, &small_cost);
    assert_in_regime("large", large_pieces, &large_cost);

    // THE APPARATUS, before any ratio is read off it. A counter that never moved would make every
    // number below a zero about nothing, and a directory that fits one `getdents64` at BOTH sizes
    // is the regime in which this defect is invisible -- so the fixture has to leave it.
    assert!(
        small_cost.total.listing_entries > 0 && large_cost.total.listing_entries > 0,
        "the entry counter read {} and {}; it is not counting, so nothing below means anything",
        small_cost.total.listing_entries,
        large_cost.total.listing_entries
    );
    assert!(
        large_pieces > small_pieces * 2,
        "{small_pieces} pieces against {large_pieces}: the piece count barely moved, so a product \
         with it cannot be told apart from a constant"
    );

    // THE IDENTITY. Every listing walks every FILE the directory holds at that moment, so the
    // entries are the listings times the directory's size -- not a bound, an equality. The
    // denominator is the directory as `read_dir` sees it, not `piece_count`: the latter reports
    // what parses as a piece and answers one fewer here, and an identity checked against the
    // wrong denominator is off by a constant that looks exactly like a partial walk.
    let dir_entries = |dir: &std::path::Path| std::fs::read_dir(dir).unwrap().count() as u64;
    for (label, cost, files) in [
        ("small", &small_cost, dir_entries(small_dir.path())),
        ("large", &large_cost, dir_entries(large_dir.path())),
    ] {
        let expected = cost.total.listings * files;
        assert_eq!(
            cost.total.listing_entries, expected,
            "{label}: {} entries walked over {} listings of a {files}-file directory. The product \
             is the claim; if a listing has started walking fewer than every file, this test is \
             the thing that has gone stale",
            cost.total.listing_entries, cost.total.listings
        );
    }

    // THE SHAPE, at the two sizes. The listings grow like the windows -- linear. The entries grow
    // like the product -- and the two ratios being DIFFERENT numbers is the whole finding.
    let listing_ratio =
        large_cost.total.listings as f64 / small_cost.total.listings.max(1) as f64;
    let entry_ratio =
        large_cost.total.listing_entries as f64 / small_cost.total.listing_entries.max(1) as f64;
    let byte_ratio = large_bytes as f64 / small_bytes.max(1) as f64;
    println!(
        "    {byte_ratio:.2}x bytes -> {listing_ratio:.2}x listings, {entry_ratio:.2}x entries \
         ({} -> {})",
        small_cost.total.listing_entries, large_cost.total.listing_entries
    );

    assert!(
        (listing_ratio - byte_ratio).abs() < byte_ratio * 0.5,
        "the listings grew {listing_ratio:.2}x for {byte_ratio:.2}x the bytes. They are supposed \
         to track the WINDOWS, which track the bytes; if they no longer do, the walk has stopped \
         listing once per window and the comparison below is against the wrong thing"
    );
    assert!(
        entry_ratio > listing_ratio * 2.0,
        "the entries grew {entry_ratio:.2}x against {listing_ratio:.2}x the listings. Those two \
         being the same number would mean the listing had stopped walking the whole directory -- \
         which is the fix, and would make this test the record of it rather than a passing guard"
    );
    assert!(
        entry_ratio > byte_ratio * 2.0,
        "the entries grew {entry_ratio:.2}x for {byte_ratio:.2}x the bytes. A per-window listing \
         over a directory that grows with the log is quadratic in the log; anything close to \
         linear here means the fixture has stopped producing the regime"
    );

    // AND WHAT THE OLD COUNTER WOULD HAVE SAID, so the reason this went unseen is on the record
    // rather than in a commit message. Reading the listings alone reports a walk growing exactly
    // as fast as the log -- the same healthy, linear answer a measurement of the piece-header
    // reads now correctly gives, about a quantity that is not linear at all.
    assert!(
        listing_ratio < entry_ratio,
        "the call count and the entry count grew at the same rate, so the call count was never \
         hiding anything and this test has no subject"
    );
}
