// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Whether the batch append rolls the piece it is writing, and what starts working when it does.
//!
//! ## What was found
//!
//! #1936 took a restore's kernel reads from 1,034,158,338 to 81,232,374 bytes and, in doing so,
//! named the reason that quadratic was catastrophic rather than merely bad: at 200,000 records
//! the shard's log was **a single 32,768,000-byte file**. It measured that and did not fix it.
//!
//! `append_batch_as_one_record` -- the path the engine takes for every batch that produced
//! outcomes, and `TS_WAL_DATA_ONLY` defaults ON -- was the one append entry point that never
//! asked `roll_wal_segment_if_due` whether the piece was full. Every other one asks:
//! `append_with_sync_inner` after its record, `append_for_group_commit` after its record, and
//! `append_batch_atomic` after its batch, with a comment saying exactly why it is after.
//!
//! The roll was not suppressed and it was not on a bypassed path. It was **absent**.
//!
//! ## Why it is not a size problem
//!
//! Almost every log-walking cost in this engine is expressed per PIECE, and several of the
//! mechanisms decide what to read from a piece's NAME. One enormous piece defeats all of them at
//! once, and each of the three was measured here with rolling off and rolling on:
//!
//! * #1917 declines to open a piece by reading the name of the piece after it -- a one-piece log
//!   has no piece after, so there is nothing to decline;
//! * #1920 found nothing bounds the pieces a log retains except a completed dump -- reclaim
//!   unlinks whole pieces, and a one-piece log has no whole piece it may unlink;
//! * #1936's own pre-walk asks every piece for its last sequence, which is cheap over 125 pieces
//!   and is the entire answer over one -- a one-piece log answers log id ZERO, so the replay
//!   starts at the beginning and decodes every record the watermark then throws away.
//!
//! ## The two-line change, and why it is two
//!
//! Adding the roll ALONE is not safe. `append_batch_as_one_record` also skipped
//! `ensure_active_wal_segment`, which every other entry point calls, and which was free to skip
//! for exactly as long as this path never rolled: a log that is always one piece has no sealed
//! pieces for a headerless new piece to collide with. The roll creates them. A crash between the
//! seal-rename and the new piece's header then leaves the next batch writing into a file with no
//! base header, which reads as starting at log id ZERO -- an address the sealed pieces already
//! own. `a_crash_inside_the_roll_leaves_no_two_records_sharing_a_log_id` drives both fault points
//! and asserts no log id is handed out twice.
//!
//! ## DIRECTION
//!
//! A log that rolls too eagerly costs a few extra files. A log that loses or reorders a record on
//! a piece boundary is silent data loss, and it loads clean. So the boundary is asserted in the
//! strong form: `the_records_a_rolled_batch_log_replays_are_the_ones_an_unrolled_one_holds`
//! compares the `(log id, sequence, record bytes)` SEQUENCE a windowed replay applies, element by
//! element, against the same replay of a log holding the same batches in one piece -- at six
//! watermarks, with the boundary asserted to fall inside the compared span.
//!
//! That comparison is available at all because a roll preserves log ids exactly: a log id is
//! cumulative RECORD bytes, the base header is not a record, and a piece starts at
//! `base + holds`. A rolled log and an unrolled log holding the same records address them
//! identically, so "the same records" can be asserted as equality rather than as a count.

#![cfg(test)]

use crate::fault::{self, FaultAction};
use crate::types::Command;
use crate::wal::{
    set_wal_segment_bytes_for_test, wal_piece_read_counts_on_this_thread, LocalWriteAheadLogStore,
    ReplayPosition, WalOutcomeItem,
};

/// The size the engine rolls at (`DEFAULT_WAL_SEGMENT_BYTES`, 256 KiB) and the window the engine
/// replays with (`engine::lifecycle::WAL_REPLAY_WINDOW_BYTES`, 512 KiB), both scaled down by
/// eight so a test log of a few hundred kilobytes still crosses several pieces and several
/// windows. What is measured here is pieces against the log, and that is set by the RATIO.
const TEST_SEGMENT_BYTES: u64 = 32 * 1024;
const TEST_WINDOW_BYTES: u64 = 64 * 1024;

/// Two corpus sizes and the ratio between them. Four times the corpus, so a quantity that is flat
/// and one that grows with the log are four times apart and cannot be confused for each other.
const SMALL: usize = 250;
const BIG: usize = 1_000;
const RATIO: usize = BIG / SMALL;

/// Items in a batch, and the bytes each one carries. Eight is the size #1936's own batch-cost
/// measurement used.
const ITEMS: usize = 8;
const VALUE_BYTES: usize = 128;

const SHARD: crate::types::ShardId = 1;

/// Restores the rolling threshold when it drops, including while a panic unwinds.
///
/// The override is per THREAD and the gate runs with `--test-threads=1`, so a test that set it
/// and then panicked would leave every test after it on that thread rolling at this size.
struct SegmentBytes;

impl SegmentBytes {
    fn set(threshold: u64) -> Self {
        set_wal_segment_bytes_for_test(Some(threshold));
        SegmentBytes
    }
}

impl Drop for SegmentBytes {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(None);
    }
}

/// xorshift64*, seeded per item. Records are compressed, so a corpus of repeated bytes holds a
/// fraction of the bytes its value size implies and moves the piece count where it is not meant
/// to go. Each item needs its OWN payload: sixteen identical payloads in ONE record compress
/// just as well as one, which is the correction #1936's batch-cost measurement had to make.
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

fn outcomes(batch: usize) -> Vec<WalOutcomeItem> {
    (0..ITEMS)
        .map(|item| {
            let id = (batch * ITEMS + item) as u64;
            WalOutcomeItem {
                kind: "string".to_string(),
                object_key: format!("batch-{batch:08}-item-{item:03}"),
                component: None,
                object_id: id,
                routing_bucket: item as u32,
                address: None,
                value: Some(incompressible(VALUE_BYTES, id)),
                ttl: None,
                deleted: false,
                meta: false,
            }
        })
        .collect()
}

/// Which append entry point writes the corpus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Writer {
    /// `append_batch_as_one_record`: the subject. The engine's data-only batch path.
    Batch,
    /// `append`: the control. Rolling has always worked here, so an arm that fails to roll on
    /// THIS path is a fixture that never set the threshold, not a defect in the subject.
    Single,
}

impl Writer {
    fn label(self) -> &'static str {
        match self {
            Writer::Batch => "append_batch_as_one_record",
            Writer::Single => "append (control)",
        }
    }

    fn write(self, store: &LocalWriteAheadLogStore, batch: usize) {
        match self {
            Writer::Batch => {
                store
                    .append_batch_as_one_record(SHARD, outcomes(batch), Vec::new(), false)
                    .expect("the fixture's batch must land");
            }
            Writer::Single => {
                // One record of about the same size as a batch record, so the two arms lay down
                // comparable numbers of bytes and the piece counts are comparable too.
                store
                    .append(
                        SHARD,
                        Command::StringSet {
                            key: format!("single-{batch:08}"),
                            value: incompressible(ITEMS * VALUE_BYTES, batch as u64),
                        },
                    )
                    .expect("the fixture's write must land");
            }
        }
    }
}

fn build(dir: &std::path::Path, batches: usize, writer: Writer) -> LocalWriteAheadLogStore {
    let store = LocalWriteAheadLogStore::new(dir);
    for batch in 0..batches {
        writer.write(&store, batch);
    }
    store
}

// ---------------------------------------------------------------------------------------------
// WHAT IS ON DISK, READ BACK FROM IT
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Pieces {
    /// Every piece of the log, the one being written included. Counted from the DIRECTORY, so a
    /// piece the extent listing declined to report would still be seen here.
    count: usize,
    /// Pieces that have been sealed -- the ones with a log id in their name, and the only ones
    /// reclaim may unlink and a walk may decline by name.
    sealed: usize,
    /// RECORD bytes, summed over the pieces. Not file bytes: under preallocation the piece being
    /// WRITTEN is longer than its records by up to a whole reservation chunk, which charges a
    /// small corpus and a large one the same constant and makes a linear total read as
    /// sub-linear. Sealed pieces are trimmed to their contents (`roll_wal_segment_if_due` does it
    /// before the rename, so that a sealed piece's length IS its contents), so only the active
    /// piece differs -- and log ids are record bytes, so this is the quantity every per-piece
    /// mechanism here is expressed in anyway.
    total_bytes: u64,
    /// The most record bytes any one piece holds. The subject of the FLAT claim.
    largest_bytes: u64,
    /// What the files actually occupy, reservation included. Reported rather than asserted on.
    file_bytes: u64,
}

impl Pieces {
    /// `record_end` is one past the log's last record byte -- `log id + length` of the last
    /// record, which is the definition of where the records stop.
    fn of(dir: &std::path::Path, record_end: u64) -> Self {
        let mut count = 0usize;
        let mut sealed = 0usize;
        let mut file_bytes = 0u64;
        let active = format!("shard-{SHARD}.wal.bin");
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.contains(".wal") || name.ends_with(".lock") {
                continue;
            }
            file_bytes += entry.metadata().map(|meta| meta.len()).unwrap_or(0);
            count += 1;
            if name != active {
                sealed += 1;
            }
        }
        let extents = crate::wal::wal_piece_extents_for_test(dir, SHARD);
        assert_eq!(
            extents.len(),
            count,
            "the extent listing and the directory must agree on how many pieces there are"
        );
        // Where the RECORDS stop, taken from the records. The extent listing reports the active
        // piece's FILE length, which under preallocation runs past its records by up to a whole
        // reservation chunk -- a constant, charged to every corpus alike, which turns a linear
        // total into a sub-linear one and a flat largest piece into a growing one.
        //
        // A piece holds everything from its own base to the next piece's base, and the last piece
        // holds everything from its base to where the records stop. Sealed pieces are trimmed to
        // their contents before the rename, so for those the two agree; this is the only piece
        // where they can differ, and it is where the reservation lives.
        let mut bases: Vec<u64> = extents.iter().map(|(_, _, base, _)| *base).collect();
        bases.sort_unstable();
        let mut total_bytes = 0u64;
        let mut largest_bytes = 0u64;
        for (index, base) in bases.iter().enumerate() {
            let end = bases.get(index + 1).copied().unwrap_or(record_end);
            let holds = end.saturating_sub(*base);
            total_bytes += holds;
            largest_bytes = largest_bytes.max(holds);
        }
        assert_eq!(
            total_bytes, record_end,
            "the pieces must tile the log's record bytes with no hole and no overlap"
        );
        Pieces {
            count,
            sealed,
            total_bytes,
            largest_bytes,
            file_bytes,
        }
    }
}

/// A record as a reader sees it: where it lives, what it is called, and its exact bytes.
///
/// The element of the element-by-element comparison. A count is not enough -- a walk that drops
/// one record and keeps an extra reports the same count as a correct one -- and neither is a
/// sequence on its own, which a walk replaying the wrong records still gets right.
type Element = (u64, u64, Vec<u8>);

fn elements(store: &LocalWriteAheadLogStore) -> Vec<Element> {
    let raw = store
        .scan(SHARD, 0, u64::MAX, u64::MAX)
        .expect("the fixture's own log must scan");
    let (decoded, truncated) = store
        .scan_decoded(SHARD, 0, u64::MAX, u64::MAX)
        .expect("the fixture's own log must decode");
    assert!(
        !truncated,
        "the fixture's whole log must fit one unbounded walk or the control is a prefix"
    );
    assert_eq!(
        raw.len(),
        decoded.len(),
        "the byte walk and the decoded walk must see the same records"
    );
    raw.into_iter()
        .zip(decoded)
        .map(|((log_id, bytes), (decoded_log_id, record))| {
            assert_eq!(
                log_id, decoded_log_id,
                "the two walks must address the same record identically"
            );
            (log_id, record.sequence, bytes)
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT: the kernel's own byte counter, for THIS THREAD.
//
// `/proc/self/io` is the whole PROCESS, and a suite running tests beside each other charges this
// span whatever another thread happened to read. Reading the counter is itself a read the kernel
// charges, so the reading carries the LENGTH of the text it came from and the span subtracts it
// exactly rather than estimating it.
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
/// `None` when `/proc` said nothing, so "the kernel was not counting" cannot be read as "the
/// kernel counted zero".
fn kernel_read_span<T>(work: impl FnOnce() -> T) -> (T, Option<u64>) {
    let before = read_thread_io();
    let value = work();
    let after = read_thread_io();
    let rchar = before
        .zip(after)
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes));
    (value, rchar)
}

// ---------------------------------------------------------------------------------------------
// THE REPLAY UNDER MEASUREMENT
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Replay {
    windows: u64,
    /// Where the replay started reading -- #1936's pre-walk answer. Zero means "the beginning of
    /// the log", which is what a one-piece log always answers.
    started_at: u64,
    /// `(log id, sequence)` for every record the replay would APPLY, in order. The bytes are
    /// not here: fetching them needs a second walk, and a second walk inside the measured span
    /// declines the same pieces again and doubles what #1917's counter reports. The
    /// element-by-element comparison takes them from `replay_with_bytes`, outside the span.
    applied: Vec<(u64, u64)>,
    /// Records the walk decoded and the watermark then threw away. The RELATION between this and
    /// `applied` is what says whether the replay started in the right place.
    decoded_and_dropped: u64,
    /// Pieces the walk declined to open by reading the next piece's NAME -- #1917's mechanism.
    pieces_declined_by_name: u64,
    /// Bytes pulled off log pieces, from the counter inside the read itself (#1936's).
    piece_bytes: u64,
    kernel_rchar: Option<u64>,
}

fn counters() -> (u64, u64, u64) {
    let (bytes, _reads) = wal_piece_read_counts_on_this_thread();
    let declined = crate::wal::WAL_PIECES_SKIPPED_BY_NAME.with(|skipped| skipped.get());
    let bodies = crate::wal::WAL_PIECE_BODY_READS.with(|reads| reads.get());
    (bytes, declined, bodies)
}

/// Replay `store` the way `replay_wal_into_shard_windowed` does: start past the pieces the
/// watermark already covers, then bounded windows resuming where the last one stopped.
fn replay(store: &LocalWriteAheadLogStore, watermark: u64, window_bytes: u64) -> Replay {
    let (bytes_before, declined_before, _) = counters();
    let mut windows = 0u64;
    let mut applied: Vec<(u64, u64)> = Vec::new();
    let mut decoded_and_dropped = 0u64;
    let mut started_at = 0u64;
    let (_, kernel_rchar) = kernel_read_span(|| {
        let mut from = store
            .replay_start_after_sequence(SHARD, watermark)
            .expect("a replay must be able to find where to start");
        started_at = from.log_id();
        let mut verify_tail = true;
        loop {
            let (scanned, more_to_come, resume_at) = store
                .scan_decoded_window(SHARD, from, window_bytes, verify_tail)
                .expect("the walk under measurement must succeed");
            verify_tail = false;
            windows += 1;
            for (log_id, record) in &scanned {
                if record.sequence > watermark {
                    applied.push((*log_id, record.sequence));
                } else {
                    decoded_and_dropped += 1;
                }
            }
            if !more_to_come {
                break;
            }
            assert!(
                resume_at.log_id() > from.log_id(),
                "a window that did not advance would spin: resumed at {} from {}",
                resume_at.log_id(),
                from.log_id()
            );
            from = resume_at;
        }
    });
    let (bytes_after, declined_after, _) = counters();
    Replay {
        windows,
        started_at,
        applied,
        decoded_and_dropped,
        pieces_declined_by_name: declined_after - declined_before,
        piece_bytes: bytes_after - bytes_before,
        kernel_rchar,
    }
}

/// The same windowed replay, carrying each record's BYTES -- run OUTSIDE any measured span.
///
/// Getting the bytes needs a second walk over the same window, and a second walk inside the span
/// declines the same pieces again, which doubles what #1917's counter reports for the replay. The
/// bytes are wanted for the element-by-element comparison and the counters are wanted for the
/// table, so the two runs are separate and neither pollutes the other.
fn replay_with_bytes(
    store: &LocalWriteAheadLogStore,
    watermark: u64,
    window_bytes: u64,
) -> Vec<Element> {
    let mut applied: Vec<Element> = Vec::new();
    let mut from = store
        .replay_start_after_sequence(SHARD, watermark)
        .expect("a replay must be able to find where to start");
    let mut verify_tail = true;
    loop {
        let (decoded, more_to_come, resume_at) = store
            .scan_decoded_window(SHARD, from, window_bytes, verify_tail)
            .expect("the walk must succeed");
        let raw = store
            .scan(SHARD, from.log_id(), u64::MAX, window_bytes)
            .expect("the byte walk over the same window must succeed");
        let by_log_id: std::collections::HashMap<u64, Vec<u8>> = raw.into_iter().collect();
        verify_tail = false;
        for (log_id, record) in decoded {
            let bytes = by_log_id.get(&log_id).cloned().expect(
                "every record the decoded walk returns must have bytes at the same log id",
            );
            if record.sequence > watermark {
                applied.push((log_id, record.sequence, bytes));
            }
        }
        if !more_to_come {
            break;
        }
        from = resume_at;
    }
    applied
}

/// One past the log's last record byte. A log id IS a byte position in the log's history, so
/// the last record's log id plus its length is where the records stop -- derived from the
/// records rather than from any file's length.
fn record_end(store: &LocalWriteAheadLogStore) -> u64 {
    elements(store)
        .last()
        .map(|(log_id, _, bytes)| log_id + bytes.len() as u64)
        .expect("the fixture must have written something")
}

fn last_sequence(store: &LocalWriteAheadLogStore) -> u64 {
    elements(store)
        .last()
        .map(|(_, sequence, _)| *sequence)
        .expect("the fixture must have written something")
}

/// Compare two record sequences element by element, reporting the FIRST place they differ.
///
/// `assert_eq!` on two thousand-element vectors of kilobyte records prints both of them: the
/// assertion is right and twenty megabytes of hex is not a failure anyone can read. This says
/// which element, at which log id, and how -- and asserts the lengths after, so a prefix that
/// matches and then stops is not read as agreement.
fn same_records(subject: &[Element], control: &[Element], context: &str) {
    for (index, (left, right)) in subject.iter().zip(control).enumerate() {
        assert_eq!(
            (left.0, left.1, left.2.len()),
            (right.0, right.1, right.2.len()),
            "{context}: record {index} differs -- (log id, sequence, bytes) {:?} against {:?}",
            (left.0, left.1, left.2.len()),
            (right.0, right.1, right.2.len())
        );
        assert!(
            left.2 == right.2,
            "{context}: record {index} at log id {} (seq {}) has the same length and different \
             bytes",
            left.0,
            left.1
        );
    }
    assert_eq!(
        subject.len(),
        control.len(),
        "{context}: the sequences agree for {} records and then one of them stops",
        subject.len().min(control.len())
    );
}

/// The same records, from a walk that skips nothing: one unbounded window from log id zero.
///
/// The control the boundary comparison is against, and it is the SAME LOG -- so the comparison
/// is byte for byte, and what it isolates is the windowed walk crossing a piece boundary rather
/// than any difference between two corpora.
fn skip_free(store: &LocalWriteAheadLogStore, watermark: u64) -> Vec<Element> {
    elements(store)
        .into_iter()
        .filter(|(_, sequence, _)| *sequence > watermark)
        .collect()
}

/// Which log id and which sequence each record has, and how long it is.
///
/// What a roll could change, without the one thing it cannot: a record carries
/// `timestamp_ms: current_time_ms()`, so two logs built from the same batches at different
/// moments hold different bytes for the same record. That is a property of the record.
fn addressing(records: &[Element]) -> Vec<(u64, u64, usize)> {
    records
        .iter()
        .map(|(log_id, sequence, bytes)| (*log_id, *sequence, bytes.len()))
        .collect()
}

fn thousands(value: u64) -> String {
    let text = value.to_string();
    let mut out = String::new();
    for (index, ch) in text.chars().enumerate() {
        if index > 0 && (text.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

// ---------------------------------------------------------------------------------------------
// 1. THE BEHAVIOUR, AT TWO CORPUS SIZES
// ---------------------------------------------------------------------------------------------

/// The batch path rolls, and the piece it writes stays the size the threshold says.
///
/// Every quantity is marked FLAT or GROWING against the 4x corpus ratio, and the ordinary append
/// path runs beside it as a control: rolling has always worked there, so an arm where NEITHER
/// path rolls is a fixture that failed to set the threshold rather than a defect in the subject.
#[test]
fn a_batch_append_rolls_the_log_piece_at_two_corpus_sizes() {
    let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);
    let mut report = String::from("\n  rolling at 32,768 B\n");
    for writer in [Writer::Batch, Writer::Single] {
        let mut arms: Vec<(usize, Pieces, usize)> = Vec::new();
        for batches in [SMALL, BIG] {
            let dir = tempfile::tempdir().expect("tempdir");
            let store = build(dir.path(), batches, writer);
            let on_disk = Pieces::of(dir.path(), record_end(&store));
            let records = elements(&store).len();
            // Full scans of the log taken by the append fast path's fallback. A roll replaces the
            // piece being written, so it also replaces the record-end that the next append's
            // O(1) check is against; both are updated by the roll, and a writer that then put the
            // SEALED piece's length back would make the check disagree with the file and send
            // every post-roll append through a whole-log scan instead.
            //
            // Correctness survives that -- the fallback repairs the cache before the record is
            // placed -- so nothing about the records would show it. This is the quantity that
            // does. `flat_append` is on by default, so the fast path being measured is live.
            let full_scans = store.stats(SHARD).append_full_scans;

            // VACUITY FLOORS. Each of these, absent, makes every assertion below pass on a
            // fixture that measured nothing.
            assert_eq!(
                records, batches,
                "{}: the fixture must have written one record per batch",
                writer.label()
            );
            assert!(
                on_disk.total_bytes > TEST_SEGMENT_BYTES * 2,
                "{} at {batches}: the corpus must be several pieces' worth of bytes or there is \
                 nothing to roll -- {} B",
                writer.label(),
                on_disk.total_bytes
            );
            assert!(
                on_disk.count > 1,
                "{} at {batches}: THE LOG MUST ROLL -- found {} piece(s) holding {} B",
                writer.label(),
                on_disk.count,
                thousands(on_disk.total_bytes)
            );
            // The piece being written is never sealed, so a rolled log has count - 1 sealed --
            // and the sealed ones are the only ones reclaim may unlink and a walk may decline.
            assert_eq!(
                on_disk.sealed,
                on_disk.count - 1,
                "{} at {batches}: every piece but the one being written must be sealed",
                writer.label()
            );
            assert!(
                on_disk.sealed > 2,
                "{} at {batches}: the scan-count assertion below needs several rolls to be about \
                 anything -- {} sealed piece(s)",
                writer.label(),
                on_disk.sealed
            );
            assert!(
                full_scans <= 2,
                "{} at {batches}: {} rolls cost {full_scans} whole-log scans on the append fast \
                 path -- a roll must leave the record-end cache describing the piece it STARTED, \
                 not the one it sealed",
                writer.label(),
                on_disk.sealed,
                full_scans = full_scans
            );
            arms.push((batches, on_disk, records));
        }

        let (small_batches, small, _) = arms[0].clone();
        let (big_batches, big, _) = arms[1].clone();
        report.push_str(&format!(
            "  {:<28} {:>6} recs {:>4} pieces  largest {:>9} B  records {:>11} B  \
files {:>11} B\n\
             \x20 {:<28} {:>6} recs {:>4} pieces  largest {:>9} B  records {:>11} B  \
files {:>11} B\n\
             \x20 {:<28} {:>11} {:>12} {:>18} {:>19} {:>18}\n",
            writer.label(),
            small_batches,
            small.count,
            thousands(small.largest_bytes),
            thousands(small.total_bytes),
            thousands(small.file_bytes),
            "",
            big_batches,
            big.count,
            thousands(big.largest_bytes),
            thousands(big.total_bytes),
            thousands(big.file_bytes),
            "RATIO",
            "",
            format!("{:.2}x", big.count as f64 / small.count as f64),
            format!("{:.2}x", big.largest_bytes as f64 / small.largest_bytes as f64),
            format!("{:.2}x", big.total_bytes as f64 / small.total_bytes as f64),
            format!("{:.2}x", big.file_bytes as f64 / small.file_bytes as f64),
        ));

        // GROWING: the bytes, and the pieces with them.
        assert!(
            big.total_bytes as f64 / small.total_bytes as f64 > RATIO as f64 * 0.8,
            "{}: the log's BYTES must grow with the corpus -- {} -> {}",
            writer.label(),
            small.total_bytes,
            big.total_bytes
        );
        assert!(
            big.count as f64 / small.count as f64 > RATIO as f64 * 0.8,
            "{}: the PIECE COUNT must grow with the corpus, or the pieces are growing instead -- \
             {} -> {}",
            writer.label(),
            small.count,
            big.count
        );

        // FLAT: the largest piece. This is the whole claim. One batch record can straddle the
        // threshold -- the roll is taken AFTER the record lands, so the piece that seals holds
        // the record that filled it -- and that is the only slack allowed.
        let record_bytes = big.total_bytes / big_batches as u64;
        let ceiling = TEST_SEGMENT_BYTES + record_bytes * 2;
        for (batches, on_disk, _) in &arms {
            assert!(
                on_disk.largest_bytes <= ceiling,
                "{} at {batches}: the LARGEST PIECE must stay at the threshold, not grow with \
                 the corpus -- {} B against a ceiling of {} B",
                writer.label(),
                thousands(on_disk.largest_bytes),
                thousands(ceiling)
            );
        }
        assert!(
            big.largest_bytes as f64 / small.largest_bytes as f64 <= 1.25,
            "{}: the largest piece must be FLAT across a {RATIO}x corpus -- {} -> {}",
            writer.label(),
            small.largest_bytes,
            big.largest_bytes
        );
    }
    println!("{report}");
}

/// Never rolling is still reachable, and it is what the batch path used to do unconditionally.
///
/// The negative control for the test above. `TS_WAL_SEGMENT_BYTES=0` means "never roll", and a
/// threshold that stopped being read would make the rolling assertions above pass for the wrong
/// reason -- so this asserts the same fixture, with rolling off, produces exactly ONE growing
/// piece.
#[test]
fn with_rolling_off_the_batch_path_writes_one_piece_that_grows_with_the_corpus() {
    let _threshold = SegmentBytes::set(0);
    let mut largest = Vec::new();
    for batches in [SMALL, BIG] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = build(dir.path(), batches, Writer::Batch);
        let on_disk = Pieces::of(dir.path(), record_end(&store));
        assert_eq!(
            elements(&store).len(),
            batches,
            "the fixture must have written one record per batch"
        );
        assert_eq!(
            on_disk.count, 1,
            "with rolling off the log must be ONE piece -- found {}",
            on_disk.count
        );
        assert_eq!(
            on_disk.sealed, 0,
            "with rolling off nothing may be sealed -- found {} sealed",
            on_disk.sealed
        );
        println!(
            "  rolling off, {batches} batches: 1 piece holding {} B of records ({} B on disk)",
            thousands(on_disk.largest_bytes),
            thousands(on_disk.file_bytes)
        );
        largest.push(on_disk.largest_bytes);
    }
    assert!(
        largest[1] as f64 / largest[0] as f64 > RATIO as f64 * 0.8,
        "with rolling off the ONE piece must grow with the corpus -- {} -> {}",
        largest[0],
        largest[1]
    );
}

// ---------------------------------------------------------------------------------------------
// 2. THE BATCH STILL LANDS WHOLE
// ---------------------------------------------------------------------------------------------

/// A batch record is never split across two pieces, and the piece that sealed holds it whole.
///
/// The correctness constraint the fix has to respect. A batch written this way IS one record, so
/// there is no inside to roll in -- but "there is no inside" is an argument, and this is the
/// measurement: every piece parses into whole frames from its header to its last byte, every
/// record lives in exactly one piece, and the records concatenated across pieces in piece order
/// are the log's records in log order.
#[test]
fn a_batch_record_lands_whole_inside_one_piece() {
    let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);
    let dir = tempfile::tempdir().expect("tempdir");
    let store = build(dir.path(), BIG, Writer::Batch);
    let on_disk = Pieces::of(dir.path(), record_end(&store));
    assert!(
        on_disk.count > 2,
        "this test needs several boundaries to be about anything -- found {} piece(s)",
        on_disk.count
    );

    let extents = crate::wal::wal_piece_extents_for_test(dir.path(), SHARD);
    assert_eq!(
        extents.len(),
        on_disk.count,
        "the extent listing must see every piece the directory holds"
    );

    // Every record, and which piece's log-id range it falls in. A record that straddled a
    // boundary would start inside one piece's range and end past it.
    let records = elements(&store);
    assert_eq!(records.len(), BIG, "one record per batch");
    let mut per_piece = vec![0usize; extents.len()];
    for (log_id, sequence, bytes) in &records {
        let end = log_id + bytes.len() as u64;
        let home = extents
            .iter()
            .position(|(_, _, base, piece_end)| *log_id >= *base && *log_id < *piece_end)
            .unwrap_or_else(|| {
                panic!("record seq {sequence} at log id {log_id} lives in no piece's range")
            });
        let (path, named_start, base, piece_end) = &extents[home];
        assert!(
            end <= *piece_end,
            "record seq {sequence} at log id {log_id} is {} B and runs {} B past the end of \
             {} -- a batch record was SPLIT across a piece boundary",
            bytes.len(),
            end - piece_end,
            path.display()
        );
        // A SEALED piece's NAME still describes its contents: the number in the name is the log
        // id the piece's records begin at, and it agrees with the header the piece carries. Every
        // walk that declines a piece by name rests on this.
        if let Some(named_start) = named_start {
            assert_eq!(
                named_start, base,
                "the name of {} says its contents start at {named_start} and its header says \
                 {base}",
                path.display()
            );
        }
        per_piece[home] += 1;
    }
    let placed: usize = per_piece.iter().sum();
    assert_eq!(
        placed,
        records.len(),
        "every record must be placed in exactly one piece"
    );
    assert!(
        per_piece.iter().filter(|count| **count > 0).count() > 2,
        "the records must be spread over several pieces or no boundary was crossed -- {per_piece:?}"
    );

    // The roll fires AFTER the record, so every sealed piece holds at least the threshold. A roll
    // taken BEFORE the record would seal pieces one record short of it.
    for (path, _, base, piece_end) in extents.iter().take(extents.len() - 1) {
        assert!(
            piece_end - base >= TEST_SEGMENT_BYTES,
            "sealed piece {} holds {} B, under the {TEST_SEGMENT_BYTES} B threshold -- the roll \
             was taken BEFORE the record that filled it",
            path.display(),
            piece_end - base
        );
    }
}

// ---------------------------------------------------------------------------------------------
// 3. DURABILITY ACROSS THE NEW BOUNDARY
// ---------------------------------------------------------------------------------------------

/// A crash inside the roll leaves no two records answering to one log id.
///
/// Rolling is a seal-rename and then a create-and-header, two steps with a crash window between
/// them, and the roll is what makes this path have sealed pieces at all. A headerless piece reads
/// as starting at log id ZERO, which the sealed pieces already own -- so without
/// `ensure_active_wal_segment` the batch after the crash is addressed at an id another record
/// already has, and a walk resolving that id reaches whichever it finds first.
///
/// Both fault points are driven, and each is asserted to have been REACHED: a crash test that
/// never reaches its point passes while testing nothing.
#[test]
fn a_crash_inside_the_roll_leaves_no_two_records_sharing_a_log_id() {
    for point in ["wal/roll/after_rename", "wal/roll/after_create"] {
        let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);
        let dir = tempfile::tempdir().expect("tempdir");

        // Write with the point armed, until one of the appends reaches it.
        let mut crashed_at = None;
        {
            let store = build(dir.path(), 1, Writer::Batch);
            let mut batch = 1usize;
            // The `Stop` action is a panic and the default hook prints it. This span expects
            // exactly one, so the hook is silenced across it and restored after.
            let previous_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            loop {
                assert!(batch < 10_000, "the fixture must reach a roll");
                let armed = fault::arm(point, FaultAction::Stop);
                assert!(fault::is_armed(point), "the point must be armed");
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    store
                        .append_batch_as_one_record(SHARD, outcomes(batch), Vec::new(), false)
                        .map(|_| ())
                }));
                drop(armed);
                batch += 1;
                if outcome.is_err() {
                    crashed_at = Some(batch);
                    break;
                }
                outcome.expect("caught").expect("the batch must land");
            }
            std::panic::set_hook(previous_hook);
            // The panic unwound through the roll while the `inner` mutex guard was held, so this
            // store's lock is poisoned and every later call on it panics with that instead of
            // with what this test is asking about. A crash loses the process anyway; the log is
            // read back through a fresh store below, which is what a restart does.
        }
        let crashed_at = crashed_at.expect("the fixture must have reached the fault point");
        assert!(
            crashed_at > 2,
            "{point}: the crash must land after several batches, not on the first"
        );

        // REOPEN, as a restart would. What the log holds now is what the crash left: the record
        // that triggered the roll is on disk, because the roll runs after it.
        let store = LocalWriteAheadLogStore::new(dir.path());
        let before_crash = elements(&store);
        for batch in crashed_at..(crashed_at + 40) {
            store
                .append_batch_as_one_record(SHARD, outcomes(batch), Vec::new(), false)
                .expect("the batch after the crash must land");
        }
        let after = elements(&store);

        // NO DUPLICATED ADDRESS. This is the failure a missing piece header produces, and it is
        // silent: the log reads back clean and one of the two records is unreachable.
        let mut seen = std::collections::HashSet::new();
        for (log_id, sequence, _) in &after {
            assert!(
                seen.insert(*log_id),
                "{point}: log id {log_id} (seq {sequence}) is held by two records"
            );
        }
        // ORDERING, which replay depends on: log ids and sequences both strictly ascending.
        for pair in after.windows(2) {
            assert!(
                pair[1].0 > pair[0].0,
                "{point}: log ids must ascend -- {} then {}",
                pair[0].0,
                pair[1].0
            );
            assert!(
                pair[1].1 > pair[0].1,
                "{point}: sequences must ascend across the boundary -- {} then {}",
                pair[0].1,
                pair[1].1
            );
        }
        // EVERY RECORD FROM BEFORE THE CRASH, unchanged, element by element.
        assert!(
            !before_crash.is_empty(),
            "{point}: the pre-crash log must not be empty or this comparison is vacuous"
        );
        assert!(
            after.len() > before_crash.len(),
            "{point}: the post-crash writes must have landed"
        );
        same_records(
            &after[..before_crash.len()],
            &before_crash,
            &format!("{point}: the log as it stood after the crash, against what it holds now"),
        );
    }
}

/// The records a replay of a ROLLED batch log applies are the ones an UNROLLED one holds --
/// element by element, across the boundary.
///
/// Direction: replaying too few records is silent data loss; replaying too many is merely slow.
/// Every assertion about a COUNT passes just as happily on a walk that replayed the wrong
/// records, so this compares the `(log id, sequence, record bytes)` SEQUENCE.
///
/// Log ids are comparable across the two shapes because a roll preserves them exactly: a log id
/// is cumulative RECORD bytes, the base header is not a record, and a piece starts at
/// `base + holds`. That is the property that makes "the same records" an equality.
#[test]
fn the_records_a_rolled_batch_log_replays_are_the_ones_an_unrolled_one_holds() {
    let dir_rolled = tempfile::tempdir().expect("tempdir");
    let dir_flat = tempfile::tempdir().expect("tempdir");
    let (rolled, flat, rolled_pieces) = {
        let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);
        let rolled = build(dir_rolled.path(), BIG, Writer::Batch);
        let pieces = Pieces::of(dir_rolled.path(), record_end(&rolled));
        drop(_threshold);
        let _flat_threshold = SegmentBytes::set(0);
        let flat = build(dir_flat.path(), BIG, Writer::Batch);
        assert_eq!(
            Pieces::of(dir_flat.path(), record_end(&flat)).count,
            1,
            "the control must be ONE piece"
        );
        drop(_flat_threshold);
        (rolled, flat, pieces)
    };
    assert!(
        rolled_pieces.count > 4,
        "the subject must cross several boundaries -- {} piece(s)",
        rolled_pieces.count
    );
    let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);

    // The whole logs first: the two shapes must hold the same records at the same addresses.
    let whole_rolled = elements(&rolled);
    let whole_flat = elements(&flat);
    assert!(!whole_flat.is_empty(), "the control must not be empty");
    assert_eq!(
        whole_rolled.len(),
        whole_flat.len(),
        "the two shapes must hold the same number of records"
    );
    // WHAT A ROLL PRESERVES: the records, their order, their sequences and their lengths.
    let rolled_addressing = addressing(&whole_rolled);
    let flat_addressing = addressing(&whole_flat);
    for (index, (left, right)) in rolled_addressing.iter().zip(&flat_addressing).enumerate() {
        assert_eq!(
            (left.1, left.2),
            (right.1, right.2),
            "a roll must not change which record is where in the order: record {index} is \
             (sequence, bytes) ({}, {}) in the rolled log and ({}, {}) in the one-piece log",
            left.1,
            left.2,
            right.1,
            right.2
        );
    }
    assert_eq!(
        rolled_addressing.len(),
        flat_addressing.len(),
        "the two shapes agree for {} records and then one of them stops",
        rolled_addressing.len().min(flat_addressing.len())
    );

    // WHAT IT DOES NOT PRESERVE, measured rather than assumed. A log id is cumulative record
    // bytes plus whatever BLOCK padding the piece accumulated: a piece is written in
    // `WAL_BLOCK_BYTES` (128 KiB) blocks, each ending in a footer slot, and a record that would
    // cross that slot starts in the next block. A piece that rolls before it reaches 128 KiB
    // never reaches a boundary, so the rolled log's records are exactly contiguous and the
    // one-piece log's are not.
    //
    // Production rolls at 256 KiB against 128 KiB blocks, so a production piece still crosses
    // one boundary. This fixture's 32 KiB pieces do not, which is a property of its scale.
    for pair in whole_rolled.windows(2) {
        assert_eq!(
            pair[1].0,
            pair[0].0 + pair[0].2.len() as u64,
            "in a log whose pieces roll below the block size the records are contiguous: \
             seq {} at log id {} is {} B and seq {} starts at {}",
            pair[0].1,
            pair[0].0,
            pair[0].2.len(),
            pair[1].1,
            pair[1].0
        );
    }
    let flat_padding: u64 = whole_flat
        .windows(2)
        .map(|pair| pair[1].0 - (pair[0].0 + pair[0].2.len() as u64))
        .sum();
    assert!(
        flat_padding > 0,
        "the one-piece control must cross a block boundary at this corpus, or the difference \
         being explained here is absent and the explanation is untested"
    );
    println!(
        "\n  a roll does not preserve log ids: the one-piece control padded {} B past block \
         boundaries over {} records; the rolled log padded 0\n",
        thousands(flat_padding),
        whole_flat.len()
    );
    // Padding only ever pushes a record FORWARD, so the one-piece log addresses every record at
    // or past where the rolled log does, and the difference never shrinks.
    let mut previous_shift = 0u64;
    for (rolled, flat) in whole_rolled.iter().zip(&whole_flat) {
        assert!(
            flat.0 >= rolled.0,
            "block padding can only push a record forward: seq {} is at {} in the one-piece log \
             and {} in the rolled one",
            rolled.1,
            flat.0,
            rolled.0
        );
        let shift = flat.0 - rolled.0;
        assert!(
            shift >= previous_shift,
            "the difference between the two shapes must never shrink: {shift} after \
             {previous_shift}"
        );
        previous_shift = shift;
    }

    // EVERY RECORD ADDRESSED EXACTLY ONCE, ASCENDING, IN BOTH. This is what replay depends on,
    // and it is the property the log ids being different does not touch.
    for (label, records) in [("rolled", &whole_rolled), ("one piece", &whole_flat)] {
        let mut seen = std::collections::HashSet::new();
        for (log_id, sequence, _) in records.iter() {
            assert!(
                seen.insert(*log_id),
                "{label}: log id {log_id} (seq {sequence}) is held by two records"
            );
        }
        for pair in records.windows(2) {
            assert!(
                pair[1].0 > pair[0].0 && pair[1].1 > pair[0].1,
                "{label}: log ids and sequences must both ascend -- ({}, {}) then ({}, {})",
                pair[0].0,
                pair[0].1,
                pair[1].0,
                pair[1].1
            );
        }
    }

    let last = last_sequence(&flat);
    let boundaries: Vec<u64> = crate::wal::wal_piece_extents_for_test(dir_rolled.path(), SHARD)
        .into_iter()
        .filter_map(|(_, named_start, _, _)| named_start)
        .filter(|start| *start > 0)
        .collect();
    assert!(
        boundaries.len() > 3,
        "the subject must have several piece boundaries -- {boundaries:?}"
    );

    let mut suffix_lengths = std::collections::HashSet::new();
    let mut compared = 0usize;
    for numerator in 0..6u64 {
        let watermark = last * numerator / 6;
        for window in [TEST_WINDOW_BYTES, TEST_WINDOW_BYTES / 2, TEST_WINDOW_BYTES / 4] {
            let subject = replay(&rolled, watermark, window);
            let subject_bytes = replay_with_bytes(&rolled, watermark, window);
            assert_eq!(
                subject.applied.len(),
                subject_bytes.len(),
                "the measured walk and the byte-carrying walk must apply the same records"
            );
            // The control is a walk over the SAME log that skips nothing, so the comparison is
            // byte for byte and what it isolates is the windowed walk crossing a boundary.
            let control = skip_free(&rolled, watermark);

            assert!(
                !control.is_empty(),
                "the control must hold something at watermark {watermark}, or the comparison \
                 passes on two empty lists"
            );
            // THE BOUNDARY MUST FALL INSIDE THE COMPARED SPAN, or this is a comparison that
            // never crosses the thing it exists to check.
            let first = subject.applied.first().map(|(log_id, _)| *log_id).unwrap();
            let last_applied = subject.applied.last().map(|(log_id, _)| *log_id).unwrap();
            assert!(
                boundaries
                    .iter()
                    .any(|start| *start > first && *start <= last_applied),
                "no piece boundary falls between log ids {first} and {last_applied} at \
                 watermark {watermark}: {boundaries:?}"
            );

            // THE COMPARISON. Element by element, not counts.
            same_records(
                &subject_bytes,
                &control,
                &format!(
                    "at watermark {watermark}, window {window}: the windowed replay of the \
                     rolled log against a walk of it that skips nothing"
                ),
            );
            // The subject must actually have been WINDOWED, or a single window that read the
            // whole log is being compared against a whole-log walk and agrees trivially.
            assert!(
                subject.windows > 1,
                "at watermark {watermark}, window {window}: the replay took one window, so no \
                 boundary between windows was crossed"
            );
            suffix_lengths.insert(control.len());
            compared += 1;
        }
    }
    assert_eq!(compared, 18, "every watermark and window must have been compared");
    assert!(
        suffix_lengths.len() >= 5,
        "the watermarks must produce different suffix lengths or the test has ONE answer by \
         construction -- {suffix_lengths:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// 4. WHAT ROLLING GIVES BACK
// ---------------------------------------------------------------------------------------------

/// The three per-piece mechanisms, with rolling off and rolling on, at two corpus sizes.
///
/// Rolling is not a size win -- the bytes on disk are the same records either way. It is what
/// makes every mechanism that is expressed PER PIECE have more than one piece to work with.
#[test]
fn rolling_gives_the_per_piece_mechanisms_their_numbers_back() {
    let mut report = String::from(
        "\n  batches   rolling   pieces  replay starts at  decoded+dropped  declined by name  \
         reclaim unlinks  bytes copied\n",
    );
    let mut rows = Vec::new();
    for batches in [SMALL, BIG] {
        for threshold in [0u64, TEST_SEGMENT_BYTES] {
            let _guard = SegmentBytes::set(threshold);
            let dir = tempfile::tempdir().expect("tempdir");
            let store = build(dir.path(), batches, Writer::Batch);
            let on_disk = Pieces::of(dir.path(), record_end(&store));
            let last = last_sequence(&store);
            let watermark = last / 2;

            // #1936's pre-walk answer, and #1917's name-decline, over one whole replay.
            let measured = replay(&store, watermark, TEST_WINDOW_BYTES);
            assert!(
                !measured.applied.is_empty(),
                "the replay must apply something at {batches} batches"
            );

            // #1920's retention: what a reclaim can unlink without reading.
            let gc = store
                .gc_before_sequence_unchecked(SHARD, watermark)
                .expect("the reclaim must run");

            rows.push((
                batches,
                threshold,
                on_disk.count,
                measured.started_at,
                measured.decoded_and_dropped,
                measured.pieces_declined_by_name,
                gc.dropped_segments,
                gc.bytes_copied,
            ));
            report.push_str(&format!(
                "  {:>7}   {:>7}   {:>6}  {:>16}  {:>15}  {:>16}  {:>15}  {:>12}\n",
                batches,
                if threshold == 0 { "off" } else { "on" },
                on_disk.count,
                thousands(measured.started_at),
                measured.decoded_and_dropped,
                measured.pieces_declined_by_name,
                gc.dropped_segments,
                thousands(gc.bytes_copied),
            ));
        }
    }
    println!("{report}");

    // Rows are (small/off, small/on, big/off, big/on).
    let off_small = &rows[0];
    let on_small = &rows[1];
    let off_big = &rows[2];
    let on_big = &rows[3];

    for off in [off_small, off_big] {
        assert_eq!(off.2, 1, "rolling off must leave one piece");
        assert_eq!(
            off.3, 0,
            "a one-piece log answers the pre-walk ZERO: the replay starts at the beginning"
        );
        assert_eq!(
            off.5, 0,
            "#1917's name-decline has nothing to decline in a one-piece log"
        );
        assert_eq!(
            off.6, 0,
            "#1920's reclaim can unlink no whole piece when there is only the one being written"
        );
    }
    for on in [on_small, on_big] {
        assert!(on.2 > 1, "rolling on must produce several pieces");
        assert!(
            on.3 > 0,
            "the pre-walk must be able to start past the pieces the watermark covers"
        );
        assert!(
            on.5 > 0,
            "#1917's name-decline must decline pieces once there are pieces to decline"
        );
        assert!(
            on.6 > 0,
            "#1920's reclaim must unlink whole pieces once there are whole pieces"
        );
    }

    // GROWING, off: the records a replay decodes and throws away grows with the corpus, because
    // a one-piece log always starts at the beginning.
    assert!(
        off_big.4 as f64 / off_small.4.max(1) as f64 > RATIO as f64 * 0.8,
        "with rolling off the wasted decode must GROW with the corpus -- {} -> {}",
        off_small.4,
        off_big.4
    );
    // And rolling caps it at the piece the watermark lands in, which is flat.
    assert!(
        on_big.4 <= off_big.4 / 4,
        "rolling must cut the wasted decode by more than 4x -- {} against {}",
        on_big.4,
        off_big.4
    );
    // #1920: reclaim copies what it keeps. With rolling on it unlinks whole pieces instead.
    assert!(
        on_big.7 < off_big.7,
        "reclaim must copy fewer bytes once it can unlink whole pieces -- {} against {}",
        on_big.7,
        off_big.7
    );
}

// ---------------------------------------------------------------------------------------------
// 5. THE RESIDUAL
// ---------------------------------------------------------------------------------------------

/// Every byte the kernel charges a replay of a rolled batch log is claimed by the piece counter,
/// and a planted read the counter cannot see comes back as exactly its own size.
///
/// A residual that is always zero and an instrument that always answers zero look identical. The
/// planted marker is what separates them.
#[test]
fn the_residual_a_rolled_batch_replay_leaves_is_zero_and_a_planted_read_proves_it_is_a_reading() {
    const PLANTED: usize = 1_000_003;
    let _threshold = SegmentBytes::set(TEST_SEGMENT_BYTES);
    for batches in [SMALL, BIG] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = build(dir.path(), batches, Writer::Batch);
        let on_disk = Pieces::of(dir.path(), record_end(&store));
        assert!(on_disk.count > 1, "the log must have rolled");
        let last = last_sequence(&store);

        let measured = replay(&store, last / 2, TEST_WINDOW_BYTES);
        assert!(measured.windows > 1, "the replay must take several windows");
        let kernel = measured
            .kernel_rchar
            .expect("/proc/thread-self/io must be readable");
        let residual = kernel as i64 - measured.piece_bytes as i64;
        assert_eq!(
            residual, 0,
            "at {batches} batches the kernel charged this thread {kernel} B and the piece \
             counter claimed {} -- the log is {} B in {} pieces and nothing else is read in \
             this span",
            measured.piece_bytes,
            thousands(on_disk.total_bytes),
            on_disk.count
        );

        // The same span with a read the counter cannot see planted inside it.
        let plant = dir.path().join("planted-marker.bin");
        std::fs::write(&plant, vec![0x5Au8; PLANTED]).expect("the marker must be written");
        let (bytes_before, _, _) = counters();
        let (planted_bytes, planted_kernel) = kernel_read_span(|| {
            let mut from = store
                .replay_start_after_sequence(SHARD, last / 2)
                .expect("a replay must find where to start");
            let mut verify_tail = true;
            loop {
                let (_, more, resume) = store
                    .scan_decoded_window(SHARD, from, TEST_WINDOW_BYTES, verify_tail)
                    .expect("the walk must succeed");
                verify_tail = false;
                if !more {
                    break;
                }
                from = resume;
            }
            std::fs::read(&plant).expect("the planted read must succeed").len()
        });
        let (bytes_after, _, _) = counters();
        assert_eq!(planted_bytes, PLANTED, "the planted read must hand back the whole file");
        let planted_residual = planted_kernel.expect("/proc/thread-self/io must be readable")
            as i64
            - (bytes_after - bytes_before) as i64;
        assert_eq!(
            planted_residual, PLANTED as i64,
            "at {batches} batches a planted {PLANTED}-byte read must come back as exactly \
             {PLANTED} bytes of residual"
        );
        std::fs::remove_file(&plant).ok();
    }
}
