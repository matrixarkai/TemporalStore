// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What an append costs per record, and what a replay costs per log.
//!
//! The write-ahead log's CORRECTNESS is the best covered of any component here -- it killed 17 of
//! 18 mutants. What nobody had measured is what it COSTS, and the two questions a log has to
//! answer at scale are separate: does one append get more expensive as the log grows, and does a
//! replay cost the number of records or the number of bytes.
//!
//! Counted, never timed. This machine sits between load 1 and 30 for hours at a stretch, so a
//! stopwatch here measures the other tenants; and the quantities that actually move -- syscalls,
//! directory listings, header reads -- do not move with load at all. Each test therefore runs the
//! same work at TWO sizes and asserts the RATIO, with both denominators printed, so a counter that
//! has quietly stopped incrementing reads as a failure rather than as a perfect result.
//!
//! Measured outside the suite with `strace -c`, on the real binary, at 2,000 / 20,000 / 80,000
//! records of 128 incompressible bytes:
//!
//! ```text
//!   APPEND, per record, and the whole of it -- residual 0:
//!     9 statx   3 openat   3 close   2 flock   2 lseek   1 read   1 write   1 fdatasync  = 22
//!   total syscalls 44,167 -> 440,411 for 2,000 -> 20,000 records: 9.97x against a 10.00x
//!   record ratio. FLAT PER RECORD. Two of the 22 do the durable work; twenty are metadata.
//!
//!   REPLAY, whole-log, 512 KiB windows:
//!     records    log bytes    windows  pieces   statx   total syscalls
//!      2,000       339,873        1       2        24       269
//!     20,000     3,403,490        7      13       248     1,230
//!     80,000    13,663,490       27      53     3,088     8,106
//!   20,000 -> 80,000 is 4.01x the bytes and 12.45x the statx. SUPERLINEAR.
//!
//!   And the same 3.4 MB of log written as 2,000 records instead of 20,000 -- ten times fewer
//!   records, same bytes -- cost 1,348 syscalls against 1,230. Replay tracks the log's BYTES.
//! ```
//!
//! These tests are the in-process halves of those measurements: they count the same mechanisms
//! (`WAL_SEGMENT_LISTINGS`, `WAL_SEGMENT_HEADER_READS`, `WAL_FILE_OPENS`, the named durability
//! barriers) rather than syscalls, because a counter can be asserted and a syscall total cannot.
//!
//! A scale measurement is not a regression guard, and these do not pretend to be one: they pin
//! the SHAPE (flat / superlinear / bytes-not-records) and the one exact identity the walk has
//! (one directory listing per window), not a constant that ordinary work would move.

#![cfg(test)]

use crate::types::Command;
use crate::wal::{
    set_wal_segment_bytes_for_test, LocalWriteAheadLogStore, WAL_SEGMENT_HEADER_READS,
    WAL_SEGMENT_LISTINGS,
};

/// The window the engine replays with -- `engine::lifecycle::WAL_REPLAY_WINDOW_BYTES`, scaled
/// down with the piece size below so a test log of a few hundred kilobytes still crosses several
/// windows and several pieces. The SHAPE being measured is windows-against-pieces, and that shape
/// is set by the ratio of these two numbers, which is preserved: 512 KiB / 256 KiB in production,
/// 64 KiB / 32 KiB here.
const TEST_WINDOW_BYTES: u64 = 64 * 1024;
const TEST_SEGMENT_BYTES: u64 = 32 * 1024;

// Per-record framing overhead, measured on the real writer: a 128-byte value under a 9-byte key
// wrote 170 bytes, and a 1,530-byte value under the same key wrote 1,572. Both are 33 bytes over
// key plus value, which is what lets the two corpora in
// `a_replay_costs_the_logs_bytes_and_not_its_records` be sized to the same byte total from
// different record counts.

/// xorshift64*, seeded per record.
///
/// A repeated byte compresses to almost nothing, and the records are compressed. A corpus built
/// from `vec![7u8; n]` therefore holds a fraction of the bytes its value size implies -- measured,
/// 2,000 records of 1,700 repeated bytes wrote 116,843 bytes where 2,000 records of 128 random
/// ones wrote 339,873. The large-value corpus exists precisely to move bytes without moving
/// records, so it has to be incompressible or it moves neither.
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

fn reset_walk_counters() {
    WAL_SEGMENT_LISTINGS.with(|listings| listings.set(0));
    WAL_SEGMENT_HEADER_READS.with(|reads| reads.set(0));
}

fn listings() -> u64 {
    WAL_SEGMENT_LISTINGS.with(|listings| listings.get())
}

fn header_reads() -> u64 {
    WAL_SEGMENT_HEADER_READS.with(|reads| reads.get())
}

/// Write `records` records of `value_bytes` each into a fresh log, and report the log's root.
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

/// What one whole-log replay costs, walked the way `replay_wal_into_shard_windowed` walks it:
/// bounded windows, resuming where the last one stopped, verifying the tail only on the first.
struct ReplayCost {
    windows: u64,
    records: u64,
    listings: u64,
    header_reads: u64,
}

fn replay_cost(store: &LocalWriteAheadLogStore) -> ReplayCost {
    reset_walk_counters();
    let start_at = store.log_id_after_sequence(1, 0).unwrap_or(0);
    let mut window_start = start_at;
    let mut verify_tail = true;
    let mut windows = 0u64;
    let mut records = 0u64;
    loop {
        let (scanned, more_to_come, resume_at) = store
            .scan_decoded_window(1, window_start, TEST_WINDOW_BYTES, verify_tail)
            .unwrap();
        verify_tail = false;
        windows += 1;
        records += scanned.len() as u64;
        if !more_to_come {
            break;
        }
        window_start = resume_at;
    }
    ReplayCost {
        windows,
        records,
        listings: listings(),
        header_reads: header_reads(),
    }
}

/// An append costs the same whether the log holds two thousand records or twenty thousand.
///
/// This is the property a log lives or dies by, and the one that is invisible at test size: an
/// append that re-reads the log to find its own end is perfectly correct, passes every
/// correctness test, and turns an ingest quadratic. The warm sequence cache is what stops it, and
/// what this asserts is that the cache HOLDS as the log grows -- ten times the records for ten
/// times the file opens and no more.
///
/// The control arm is the point of the test. A counter that had simply stopped incrementing would
/// report a perfect ratio, so the same measurement is taken with the cache turned off, where the
/// full-scan count is required to track the write volume. Without that arm this test passes
/// against a WAL with no cache at all and against one with no counter at all.
#[test]
fn what_an_append_costs_per_record_at_two_log_sizes() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small_records = 2_000usize;
    let large_records = 20_000usize;

    let small_dir = tempfile::tempdir().unwrap();
    let small = build_log(small_dir.path(), small_records, 128);
    let small_stats = small.raw_stats(1);

    let large_dir = tempfile::tempdir().unwrap();
    let large = build_log(large_dir.path(), large_records, 128);
    let large_stats = large.raw_stats(1);
    set_wal_segment_bytes_for_test(None);

    // The control: the same twenty thousand appends with the warm cache refused, which is what
    // this code did before the cache existed.
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let control_dir = tempfile::tempdir().unwrap();
    let control = LocalWriteAheadLogStore::new(control_dir.path());
    control.rescan_on_every_append_for_test();
    let control_records = 2_000usize;
    let mut index = 0usize;
    while index < control_records {
        control
            .append(
                1,
                Command::StringSet {
                    key: format!("k{index:08}"),
                    value: incompressible(128, index as u64),
                },
            )
            .unwrap();
        index += 1;
    }
    let control_stats = control.raw_stats(1);
    set_wal_segment_bytes_for_test(None);

    let record_ratio = large_records as f64 / small_records as f64;
    let bytes_ratio = large_stats.bytes_written as f64 / small_stats.bytes_written as f64;
    let scan_ratio = large_stats.append_full_scans as f64 / small_stats.append_full_scans.max(1) as f64;

    println!("  what an append costs, at two log sizes");
    println!(
        "    {:>8} records: {:>10} B written, {:>6} full walks of the log, {:>6} syncs",
        small_records,
        small_stats.bytes_written,
        small_stats.append_full_scans,
        small_stats.syncs
    );
    println!(
        "    {:>8} records: {:>10} B written, {:>6} full walks of the log, {:>6} syncs",
        large_records,
        large_stats.bytes_written,
        large_stats.append_full_scans,
        large_stats.syncs
    );
    println!(
        "    ratio           {record_ratio:.2}x records, {bytes_ratio:.2}x bytes, \
         {scan_ratio:.2}x full walks"
    );
    println!(
        "    CONTROL, cache refused: {control_records} records cost \
         {} full walks of the log",
        control_stats.append_full_scans
    );

    // Vacuity first, and the apparatus before the result. A run that wrote nothing, or a counter
    // that never moved, would make every ratio below meaningless.
    assert!(
        small_stats.bytes_written > 0 && large_stats.bytes_written > 0,
        "neither log was written ({} B and {} B), so there is nothing to take a ratio of",
        small_stats.bytes_written,
        large_stats.bytes_written
    );
    assert!(
        control_stats.append_full_scans >= control_records as u64,
        "the CONTROL walked the log {} times for {control_records} appends with the warm cache \
         refused; it is supposed to walk once per append, so the counter is not counting walks \
         and the flat figures below mean nothing",
        control_stats.append_full_scans
    );

    // One durable barrier per synchronous append, at both sizes -- and the same number at both,
    // per record. An append that took two would double every write's latency.
    assert_eq!(
        small_stats.syncs, small_records as u64,
        "{} barriers for {small_records} synchronous appends",
        small_stats.syncs
    );
    assert_eq!(
        large_stats.syncs, large_records as u64,
        "{} barriers for {large_records} synchronous appends",
        large_stats.syncs
    );

    // The result. Ten times the records for the same per-record cost: the bytes track the
    // records, and the number of times the append path reads the whole log does NOT.
    assert!(
        (bytes_ratio - record_ratio).abs() < record_ratio * 0.05,
        "ten times the records wrote {bytes_ratio:.2}x the bytes; per-record framing should make \
         those the same number, so something is growing with the log"
    );
    assert!(
        large_stats.append_full_scans <= 2,
        "the append path walked the whole log {} times over {large_records} appends. It learns the \
         end once and carries it; a walk per append is the shape that makes an ingest quadratic",
        large_stats.append_full_scans
    );
    assert!(
        large_stats.append_full_scans <= small_stats.append_full_scans,
        "the log that is ten times longer walked itself {} times against {}; the cost of finding \
         the end must not grow with the log",
        large_stats.append_full_scans,
        small_stats.append_full_scans
    );
}

/// What an append and a replayed record ALLOCATE, at two log sizes.
///
/// The counted companion to the syscall measurement above, and the one that answers the first of
/// the three shapes a scale problem takes here: a whole structure held in memory, or a clone per
/// operation. An append that allocated in proportion to the log would be invisible in the syscall
/// counts and fatal in a long-running writer.
///
/// `#[ignore]`d and gated on the feature that installs the counting allocator, like every other
/// probe in this crate: the allocator adds two atomic increments to every allocation in the
/// process, which is not something the ordinary suite should carry. Run it with
/// `cargo test --features alloc-probe --lib wal_scale -- --ignored --nocapture --test-threads=1`.
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore]
fn what_an_append_and_a_replayed_record_allocate_at_two_log_sizes() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small_records = 2_000usize;
    let large_records = 20_000usize;

    let measure_append = |records: usize| -> (f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        // The payloads are built OUTSIDE the probe: generating a random value allocates, and
        // attributing the harness's own Vec to the append would report the corpus rather than
        // the writer.
        let payloads: Vec<(String, Vec<u8>)> = (0..records)
            .map(|index| (format!("k{index:08}"), incompressible(128, index as u64)))
            .collect();
        let probe = crate::alloc_probe::Probe::start();
        for (key, value) in &payloads {
            store
                .append(1, Command::StringSet { key: key.clone(), value: value.clone() })
                .unwrap();
        }
        let counts = probe.stop();
        (counts.per(records), counts.alloc_bytes as f64 / records as f64)
    };

    let (small_allocs, small_bytes) = measure_append(small_records);
    let (large_allocs, large_bytes) = measure_append(large_records);

    // The replay side, per record recovered.
    let replay_dir = tempfile::tempdir().unwrap();
    let replay_store = build_log(replay_dir.path(), large_records, 128);
    reset_walk_counters();
    let probe = crate::alloc_probe::Probe::start();
    let cost = replay_cost(&replay_store);
    let replay_counts = probe.stop();
    set_wal_segment_bytes_for_test(None);

    let alloc_ratio = large_allocs / small_allocs.max(f64::MIN_POSITIVE);

    println!("  what an append allocates, at two log sizes");
    println!("    {small_records:>6} records: {small_allocs:>6.2} allocations, {small_bytes:>8.1} B per append");
    println!("    {large_records:>6} records: {large_allocs:>6.2} allocations, {large_bytes:>8.1} B per append");
    println!("    ratio            {alloc_ratio:.3}x allocations per append for 10.00x the log");
    println!(
        "  what a replay allocates: {:.2} allocations and {:.1} B per record over {} records \
         in {} windows",
        replay_counts.per(cost.records.max(1) as usize),
        replay_counts.alloc_bytes as f64 / cost.records.max(1) as f64,
        cost.records,
        cost.windows
    );

    // Vacuity first: a probe reporting zero for a path that unmistakably allocates is worse than
    // no probe, because it reads as a perfect result.
    assert!(
        small_allocs >= 1.0 && replay_counts.allocs >= cost.records,
        "the probe saw {small_allocs:.2} allocations per append and {} for {} replayed records; \
         the counting allocator is not installed, so every figure above is a zero about nothing",
        replay_counts.allocs,
        cost.records
    );

    // The result: what one append allocates does not grow with the log behind it.
    assert!(
        alloc_ratio < 1.20,
        "an append into the ten-times-longer log allocated {alloc_ratio:.2}x as much \
         ({small_allocs:.2} -> {large_allocs:.2} per append). A per-append cost that grows with \
         the log is the shape that makes a long-lived writer degrade in place"
    );
}

/// A replay costs the log's BYTES, not its records.
///
/// The two quantities move independently -- reclaim took a real log from 1,344,852 bytes to 33,413
/// on one idle round without changing what it held -- so "replay is expensive" has two different
/// fixes depending on which one it tracks, and guessing has a fifty percent success rate.
///
/// The experiment holds the bytes fixed and varies the records ten-fold. Both arms are asserted
/// BEFORE the comparison: that the byte totals really did stay level (otherwise nothing is
/// controlled) and that the record counts really did differ (otherwise nothing was varied). A test
/// that skipped either would pass on two identical corpora.
#[test]
fn a_replay_costs_the_logs_bytes_and_not_its_records() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));

    // Sized to the same byte total from the measured framing overhead: 3,000 x (64 + 9 + 33) and
    // 300 x (1024 + 9 + 33) are 318,000 and 319,800 bytes.
    let many_records = 3_000usize;
    let many_value = 64usize;
    let few_records = 300usize;
    let few_value = 1_024usize;

    let many_dir = tempfile::tempdir().unwrap();
    let many = build_log(many_dir.path(), many_records, many_value);
    let many_bytes = many.raw_stats(1).bytes_written;
    let many_cost = replay_cost(&many);

    let few_dir = tempfile::tempdir().unwrap();
    let few = build_log(few_dir.path(), few_records, few_value);
    let few_bytes = few.raw_stats(1).bytes_written;
    let few_cost = replay_cost(&few);
    set_wal_segment_bytes_for_test(None);

    let record_ratio = many_cost.records as f64 / few_cost.records.max(1) as f64;
    let byte_ratio = many_bytes as f64 / few_bytes.max(1) as f64;
    let window_ratio = many_cost.windows as f64 / few_cost.windows.max(1) as f64;
    let walk_ratio = many_cost.header_reads as f64 / few_cost.header_reads.max(1) as f64;

    println!("  what a replay costs, at the same bytes and ten times the records");
    println!(
        "    {:>6} records of {:>5} B: {:>8} B of log, {:>4} windows, {:>5} listings, \
         {:>5} header reads",
        many_cost.records, many_value, many_bytes, many_cost.windows, many_cost.listings,
        many_cost.header_reads
    );
    println!(
        "    {:>6} records of {:>5} B: {:>8} B of log, {:>4} windows, {:>5} listings, \
         {:>5} header reads",
        few_cost.records, few_value, few_bytes, few_cost.windows, few_cost.listings,
        few_cost.header_reads
    );
    println!(
        "    ratio: {record_ratio:.2}x records, {byte_ratio:.2}x bytes, \
         {window_ratio:.2}x windows, {walk_ratio:.2}x header reads"
    );

    // Vacuity and apparatus, before anything is concluded from them.
    assert!(
        many_cost.records >= many_records as u64 && few_cost.records >= few_records as u64,
        "the walk returned {} and {} records for logs of {many_records} and {few_records}; a walk \
         that lost records is measuring a different log from the one that was written",
        many_cost.records,
        few_cost.records
    );
    assert!(
        few_cost.header_reads >= 1 && many_cost.header_reads >= 1,
        "the walk read {} and {} piece headers; the counter is not counting, so the ratio below \
         means nothing",
        many_cost.header_reads,
        few_cost.header_reads
    );

    // The control held: the two logs are the same size in bytes.
    assert!(
        (byte_ratio - 1.0).abs() < 0.15,
        "the two corpora were meant to hold the same number of bytes and differ by \
         {byte_ratio:.2}x ({many_bytes} B against {few_bytes} B); nothing below is controlled"
    );
    // The treatment was applied: the record counts really do differ ten-fold.
    assert!(
        record_ratio > 5.0,
        "the two corpora differ by only {record_ratio:.2}x in records, so the thing this test \
         varies was not varied"
    );

    // The result. Ten times the records, over the same bytes, costs the same walk.
    assert!(
        walk_ratio < 1.5,
        "ten times the records over the same bytes cost {walk_ratio:.2}x the piece-header reads. \
         A replay that tracked RECORDS would show about {record_ratio:.2}x here, and the fix for \
         an expensive replay would then be fewer records rather than fewer bytes"
    );
    assert_eq!(
        many_cost.windows, few_cost.windows,
        "the same bytes took {} windows one way and {} the other; the window budget is a byte \
         budget, so it must not notice how the bytes were divided into records",
        many_cost.windows, few_cost.windows
    );
}

/// Every replay window lists the log's directory again, and re-reads the header of every piece
/// behind it.
///
/// The walk is resumable in its RECORDS -- each window picks up exactly where the last stopped --
/// and not in anything else. It takes the log's piece list from a fresh `read_dir` every time, and
/// then walks that list from the FRONT, reading each piece's header to learn where the piece
/// begins so it can decide the piece is behind the window and step over it. So a piece one window
/// stepped over is read again by every window after it, and both the number of windows and the
/// number of pieces grow with the log.
///
/// Measured on the real binary at 512 KiB windows and 256 KiB pieces: 20,000 records cost 248
/// `statx`, and 80,000 -- four times the bytes -- cost 3,088. That is 12.45x for 4.01x, and at
/// 80,000 records the walk spent more syscalls on piece metadata (3,018 on sealed pieces alone)
/// than it did reading records (2,722).
///
/// The exact identity is the listing: one per window, no more and no fewer, with the two walks
/// that bracket a replay accounted for by name. A residual of zero is what makes this a
/// measurement of the mechanism rather than of a number that happened to come out.
#[test]
fn a_replay_window_re_reads_the_pieces_behind_it() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));

    let small_records = 2_000usize;
    let large_records = 8_000usize;

    let small_dir = tempfile::tempdir().unwrap();
    let small = build_log(small_dir.path(), small_records, 128);
    let small_bytes = small.raw_stats(1).bytes_written;
    let small_cost = replay_cost(&small);

    let large_dir = tempfile::tempdir().unwrap();
    let large = build_log(large_dir.path(), large_records, 128);
    let large_bytes = large.raw_stats(1).bytes_written;
    let large_cost = replay_cost(&large);
    set_wal_segment_bytes_for_test(None);

    let byte_ratio = large_bytes as f64 / small_bytes.max(1) as f64;
    let window_ratio = large_cost.windows as f64 / small_cost.windows.max(1) as f64;
    let walk_ratio = large_cost.header_reads as f64 / small_cost.header_reads.max(1) as f64;

    // Every listing is either a replay window's own, or one of the two walks that bracket the
    // replay: `log_id_after_sequence` before it, and the tail verification inside the first
    // window. Printed as a RESIDUAL so the count is accounted for rather than merely bounded.
    let small_residual = small_cost.listings as i64 - small_cost.windows as i64;
    let large_residual = large_cost.listings as i64 - large_cost.windows as i64;

    println!("  what a replay costs, at two log sizes");
    println!(
        "    {:>6} records, {:>9} B: {:>4} windows, {:>5} listings, {:>6} header reads, \
         residual {:>3}",
        small_cost.records, small_bytes, small_cost.windows, small_cost.listings,
        small_cost.header_reads, small_residual
    );
    println!(
        "    {:>6} records, {:>9} B: {:>4} windows, {:>5} listings, {:>6} header reads, \
         residual {:>3}",
        large_cost.records, large_bytes, large_cost.windows, large_cost.listings,
        large_cost.header_reads, large_residual
    );
    println!(
        "    ratio: {byte_ratio:.2}x bytes, {window_ratio:.2}x windows, \
         {walk_ratio:.2}x header reads"
    );
    println!(
        "    per window: {:.1} header reads at the small size, {:.1} at the large one",
        small_cost.header_reads as f64 / small_cost.windows.max(1) as f64,
        large_cost.header_reads as f64 / large_cost.windows.max(1) as f64
    );

    // Vacuity and apparatus first.
    assert!(
        small_cost.windows >= 2 && large_cost.windows >= 4,
        "the two replays took {} and {} windows; a replay that fits in one window cannot show \
         anything about per-window cost",
        small_cost.windows,
        large_cost.windows
    );
    assert!(
        small_cost.header_reads > 0 && small_cost.listings > 0,
        "the walk listed {} times and read {} headers; the counters are not counting",
        small_cost.listings,
        small_cost.header_reads
    );

    // The exact identity, with a residual that accounts for the total: the listings are the
    // windows plus the two bracketing walks, at BOTH sizes, so the count is explained and not
    // merely bounded.
    assert_eq!(
        small_residual, 2,
        "at {small_records} records the walk listed the directory {} times for {} windows, \
         leaving {small_residual} unaccounted; the two expected extras are the pre-walk that \
         finds where to start and the tail verification inside the first window",
        small_cost.listings, small_cost.windows
    );
    assert_eq!(
        large_residual, 2,
        "at {large_records} records the walk listed the directory {} times for {} windows, \
         leaving {large_residual} unaccounted",
        large_cost.listings, large_cost.windows
    );

    // The result: the per-window walk is not flat, so the whole replay is superlinear in the
    // log's bytes. Four times the bytes costs four times the windows AND four times the pieces
    // each window walks past.
    assert!(
        walk_ratio > byte_ratio * 1.5,
        "the header reads grew {walk_ratio:.2}x for {byte_ratio:.2}x the bytes. If that ever \
         becomes linear this test is reporting a FIX -- the walk would have started resuming its \
         piece list the way it already resumes its records -- and should be rewritten to hold the \
         new shape, not deleted"
    );
}

/// The append lock spans the write, and not one durable barrier.
///
/// Every append takes the log's mutex and a cross-process `flock`, writes its record under both,
/// and releases them BEFORE asking for durability, so that concurrent writers coalesce onto one
/// barrier instead of queueing behind each other's. That is the whole design, and the thing that
/// would silently undo it is a barrier creeping back inside the critical section -- which reads
/// as no change at all on one thread.
///
/// The two sites report themselves under different names, so the split is measured rather than
/// argued. The zero is asserted in the SAME run that is asserted to have made every one of its
/// records durable: a run that synced nothing would report zero barriers under the lock too, and
/// mean nothing by it.
#[test]
fn no_durable_barrier_is_taken_while_the_append_lock_is_held() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let records = 500usize;
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    // Created and warmed before the counters are read: the first append makes the file and its
    // directory entry durable, which is not per-record work.
    store
        .append(1, Command::StringSet { key: "warm".to_string(), value: vec![1] })
        .unwrap();

    crate::durability_metrics::reset();
    let mut index = 0usize;
    while index < records {
        store
            .append(
                1,
                Command::StringSet {
                    key: format!("k{index:08}"),
                    value: incompressible(128, index as u64),
                },
            )
            .unwrap();
        index += 1;
    }
    let stats = store.raw_stats(1);
    set_wal_segment_bytes_for_test(None);

    let snapshot = crate::durability_metrics::snapshot();
    let under_the_lock = snapshot.get("engine_wal_append").copied().unwrap_or(0);
    let after_release = snapshot.get("engine_wal_group_commit").copied().unwrap_or(0);

    println!("  where an append's durable barriers are taken, over {records} appends");
    println!("    under the append lock     {under_the_lock}");
    println!("    after releasing it        {after_release}");
    println!("    records made durable      {}", stats.syncs);
    for (site, count) in &snapshot {
        println!("      {site:<42} {count}");
    }

    // Non-vacuity: this run really did make every record durable. Without this the zero below is
    // satisfied by a run that never synced anything.
    assert!(
        after_release >= records as u64,
        "{records} synchronous appends took only {after_release} barriers after releasing the \
         lock; if the writes were not made durable here then the zero below is a zero about \
         nothing"
    );

    // The result.
    assert_eq!(
        under_the_lock, 0,
        "{under_the_lock} durable barriers were taken while the append lock was held, over \
         {records} appends that took {after_release} barriers after releasing it. A barrier under \
         the lock is milliseconds of held mutex on the write path, and on one thread it costs \
         nothing measurable -- it only shows up as every concurrent writer queueing"
    );
}
