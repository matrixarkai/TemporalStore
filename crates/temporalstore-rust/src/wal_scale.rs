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
//!   20,000 -> 80,000 is 4.01x the bytes and 12.45x the statx. SUPERLINEAR -- that walk has since
//!   been given the piece's own NAME to read instead of its header, and the same measurement now
//!   reports 75 -> 255 statx, 3.40x for 4.01x, with the same windows and the same records
//!   replayed. See `wal_replay_scale`, which took that aside as its subject; the table above is
//!   left as the BEFORE it was measured as.
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
    set_wal_segment_bytes_for_test, LocalWriteAheadLogStore, WAL_PATH_BUILDS, WAL_PATH_LEN_ASKS,
    WAL_SEGMENT_HEADER_READS, WAL_SEGMENT_LISTINGS,
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

/// Every replay window lists the log's directory again, and steps over the pieces behind it
/// WITHOUT reading them.
///
/// This test used to assert the opposite, and it said what to do if the shape ever changed: "If
/// that ever becomes linear this test is reporting a FIX -- the walk would have started resuming
/// its piece list the way it already resumes its records -- and should be rewritten to hold the
/// new shape, not deleted." That is what happened, and this is that rewrite.
///
/// The walk was resumable in its RECORDS -- each window picks up exactly where the last stopped --
/// and in nothing else. It took the log's piece list from a fresh `read_dir` every time and walked
/// it from the FRONT, reading each piece's header to learn where the piece begins so it could
/// decide the piece was behind the window and step over it. Both the windows and the pieces grow
/// with the log, so the header reads grew with their PRODUCT: 45 at 2,000 records and 466 at
/// 8,000, which is 10.36x for 4.00x the bytes, and 12.45x the `statx` for 4.01x on the real binary.
///
/// A sealed piece is named for the log id its contents start at, so the NEXT piece's name already
/// said where this one stops; `scan_collect` now reads that instead of the file. What this test
/// holds is the part that did not change -- the listing identity, one per window with the two
/// bracketing walks named -- and, in place of the product, that a window's cost no longer depends
/// on how much log is in front of it. The superlinear shape itself, and the regime in which it is
/// invisible, are measured in `wal_replay_scale`.
#[test]
fn a_replay_window_steps_over_the_pieces_behind_it() {
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

    // The result: what ONE WINDOW costs in piece headers does not depend on how much log is in
    // front of it. That is the property; the ratio below is its consequence, and it is stated as a
    // per-window figure so it does not quietly depend on this fixture being sized four-fold.
    let small_per_window = small_cost.header_reads as f64 / small_cost.windows as f64;
    let large_per_window = large_cost.header_reads as f64 / large_cost.windows as f64;
    assert!(
        large_per_window < small_per_window * 1.5,
        "a window of the four-times-longer log read {large_per_window:.1} piece headers against \
         {small_per_window:.1}. Every window walking the piece list from the front again is what \
         made this superlinear -- 7.5 against 22.2 per window, 10.36x the header reads for 4.00x \
         the bytes -- and it is back"
    );
    assert!(
        walk_ratio < byte_ratio * 1.5,
        "the header reads grew {walk_ratio:.2}x for {byte_ratio:.2}x the bytes, which is the \
         windows-times-pieces product again rather than the pieces alone"
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

/// Append `records` records into a log that is ALREADY being written, and report how many times
/// the append path asked the filesystem for the active piece's length, how many times it rebuilt
/// that piece's name, how many records the log took, and how many files it ended up in.
///
/// The first append is outside the measurement deliberately. It creates the file, takes the one
/// full tail scan this process pays, and makes the directory entry durable -- none of which is
/// per-record work, and all of which would be divided into the per-append figure as though it
/// were. Everything counted here is a steady-state append into a non-empty log.
fn metadata_asks_over_steady_appends(records: usize) -> (u64, u64, u64, usize) {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    store
        .append(
            1,
            Command::StringSet { key: "warm".to_string(), value: vec![1u8; 128] },
        )
        .unwrap();

    WAL_PATH_LEN_ASKS.with(|asks| asks.set(0));
    WAL_PATH_BUILDS.with(|builds| builds.set(0));
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
    let asks = WAL_PATH_LEN_ASKS.with(|asks| asks.get());
    let builds = WAL_PATH_BUILDS.with(|builds| builds.get());

    let written = store.raw_stats(1).writes;
    let files = std::fs::read_dir(dir.path()).unwrap().count();
    (asks, builds, written, files)
}

/// What a steady-state append asks the FILESYSTEM, as a number that does not move with the log.
///
/// Measured outside the suite with `strace -f -c` on the release binary, at 2,000 and 20,000
/// records of 128 incompressible bytes: an append issues 22 syscalls, of which ONE `write` and
/// ONE `fdatasync` are the durable work. Nine of the other twenty were `statx`, every one of them
/// a question about the same file, asked under the same append lock, inside the same append.
///
/// Three of those nine were the same question asked twice. `ensure_active_wal_segment` asked
/// `metadata()` for the length and `exists()` for the presence -- one `statx` each, back to back,
/// about one file -- where absent and empty take the same branch and one answer decides both.
/// `group_commit_sync` rebuilt the piece's NAME, which costs a `statx` because the name is chosen
/// by asking which candidate exists, and then asked `exists()` again before an open that answers
/// the same question itself. Removing the three took the release binary from 44,167 to 38,167
/// syscalls at 2,000 records and from 440,411 to 380,411 at 20,000: 3.000 fewer per append at
/// both sizes, all three of them `statx`.
///
/// This is the in-process half of that. It counts the asks rather than the syscalls because a
/// count can be asserted and a syscall total cannot, and it asserts the count PER APPEND at two
/// log sizes: fixed process overhead divides away, and an ask that started scaling with the log
/// reads as a failure rather than as a larger constant.
#[test]
fn a_steady_state_append_asks_for_the_active_pieces_length_a_fixed_number_of_times() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let small = 200usize;
    let large = 2_000usize;
    let (small_asks, _, small_written, small_files) = metadata_asks_over_steady_appends(small);
    let (large_asks, _, large_written, large_files) = metadata_asks_over_steady_appends(large);
    set_wal_segment_bytes_for_test(None);

    let small_per = small_asks as f64 / small as f64;
    let large_per = large_asks as f64 / large as f64;
    println!("  what an append asks the filesystem for the active piece's length");
    println!("    records   asks     per append   files   records written");
    println!("    {small:>7}   {small_asks:>6}   {small_per:>10.3}   {small_files:>5}   {small_written}");
    println!("    {large:>7}   {large_asks:>6}   {large_per:>10.3}   {large_files:>5}   {large_written}");

    // The fixture is in the regime being measured. Both runs appended into a log that already
    // had records in it, both appended more than one record, and both ROLLED -- a log of one
    // piece never enters `roll_wal_segment_if_due`'s sealing branch, and a fresh-file append
    // takes the full-scan path rather than the steady-state one. More than two files is a
    // rolled log: the piece being written, the lock, and at least one sealed piece.
    assert!(
        small_written > small as u64 && large_written > large as u64,
        "the warm append is missing: {small_written} and {large_written} records written for \
         {small} and {large} measured appends"
    );
    assert!(
        small_files > 2 && large_files > 2,
        "neither run rolled ({small_files} and {large_files} files), so this measures a log that \
         never seals a piece and not the one the engine writes"
    );
    assert!(
        small_asks > 0 && large_asks > 0,
        "no asks were counted at all, so every assertion below is about a counter that is not \
         being incremented"
    );

    // The result: the per-append cost does not move with the log.
    assert_eq!(
        small_asks as usize, small * 3,
        "{small} steady-state appends asked for the active piece's length {small_asks} times, \
         not {} -- an append asks three times: once to check the piece exists, once to confirm \
         no other writer moved its end, and once to decide whether it is full. \
         There used to be a fourth, taken by the group-commit barrier to record how much of the \
         piece it had just made durable. It asked the wrong question: under preallocation the \
         file's length is the RESERVATION, not the records, so the barrier recorded a durable \
         byte count up to a whole 256 KiB chunk above what the log actually held. The record end \
         is already in hand on that path, so the answer cost a `statx` that was not needed even \
         to be wrong",
        small * 3
    );
    assert_eq!(
        large_asks as usize, large * 3,
        "{large} steady-state appends asked {large_asks} times, not {}", large * 3
    );
    assert_eq!(
        small_per, large_per,
        "the per-append ask count moved between a {small}-record log and a {large}-record one \
         ({small_per} against {large_per}). A quantity that grows with the log is a scan wearing \
         a constant's clothes"
    );
}

/// A steady-state append never rebuilds the log's NAME.
///
/// `write_ahead_log_path` chooses between the current piece name and the one a store written
/// before the rename still uses, and it chooses by asking whether each exists -- so every call is
/// a `statx`, sometimes two. The append path holds the answer in `active_path_by_shard` and the
/// name cannot move while the log is open: a roll seals the outgoing piece under a NUMBERED name
/// and recreates this one. `group_commit_sync` rebuilt it once per barrier anyway.
///
/// That was also the more dangerous of the two questions, and the reason this test asserts zero
/// rather than one. The record is written to the CACHED path; a barrier has to cover the file the
/// record went into, not whatever the name resolves to by the time the barrier runs. Two
/// independent answers to "which file is the log" on one durability path agree by coincidence.
/// One answer agrees by construction.
///
/// The zero is asserted in a run that rolls, because a roll is the one moment the name could
/// plausibly need re-deriving -- and then the counter is proved to work by planting exactly one
/// rebuild and asserting exactly one is recovered. Without that, a counter wired to nothing
/// reports the same zero.
#[test]
fn a_steady_state_append_never_rebuilds_the_logs_name() {
    set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES));
    let records = 2_000usize;
    let (_, builds, written, files) = metadata_asks_over_steady_appends(records);

    println!("  name rebuilds over {records} steady-state appends: {builds}");
    println!("    records written {written}, files {files}");

    assert!(
        written > records as u64 && files > 2,
        "{written} records in {files} files: this run did not roll, so the zero below is a zero \
         about a log that never sealed a piece"
    );
    assert_eq!(
        builds, 0,
        "{records} appends rebuilt the log's name {builds} times. Each rebuild is a `statx` to \
         re-derive a name that cannot change while the log is open, and a second opinion about \
         which file the log IS on a path that is about to make it durable"
    );

    // The counter is wired to something. Exactly one rebuild is planted -- `base_offset` builds
    // the name from the root, which is the one thing being counted -- and exactly one has to come
    // back. A counter that reports zero because nothing increments it fails here and passes above.
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    store
        .append(1, Command::StringSet { key: "one".to_string(), value: vec![9u8; 64] })
        .unwrap();
    WAL_PATH_BUILDS.with(|builds| builds.set(0));
    let _ = store.base_offset(1).unwrap();
    let planted = WAL_PATH_BUILDS.with(|builds| builds.get());
    set_wal_segment_bytes_for_test(None);
    assert_eq!(
        planted, 1,
        "one name rebuild was planted and {planted} were counted, so the zero above says nothing \
         about whether appends rebuild the name"
    );
}
