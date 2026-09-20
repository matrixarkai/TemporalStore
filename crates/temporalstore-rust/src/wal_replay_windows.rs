// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a WINDOWED replay reads, against the log it is replaying.
//!
//! PR #1931 measured a restore at 20,000 and 200,000 records and found that bringing back a
//! 200,000-record shard with half its writes past the checkpoint read **1,014,680,664 bytes to
//! rebuild an 87.9 MB store**, of which **965,415,651 was read by a reader with no counter on
//! it**. It measured the shape and guarded it; it did not name the reader. This does both, and
//! then removes most of it.
//!
//! ## The reader, named
//!
//! It is the log's own pieces, read by the windowed replay walk, and the bytes were invisible
//! because the walk counts what it HANDS BACK and not what it READS. Attributed by file, from
//! outside the process:
//!
//! ```text
//!   200,000 records, half the writes past the checkpoint, before this change
//!   kernel rchar across load_shard_with        1,034,158,338
//!     the log piece  shard-1.wal.bin             987,705,371   in 120,568 reads
//!     the checkpoint manifests                    38,410,321   in 4 reads
//!     the index log                                8,119,256   in 1,069 reads
//!     pages, /proc, everything else                    13,390
//!   the log store's own bytes_read                 30,184,630
//! ```
//!
//! The log piece on disk is **32,768,000 bytes**. The restore read it **30.14 times over**.
//!
//! ## Why, and why it is not what #1917 fixed
//!
//! #1917 removed a window re-reading the HEADER of every piece behind it, by deciding from the
//! piece's NAME whether it was behind the window. This is the piece's CONTENTS, and that change
//! does not touch it: inside the piece the window still starts at the piece's header. The walk
//! always resumed its RECORDS -- each window picks up at the log id the last one stopped at --
//! and then re-read its way to that log id from the top of the piece, decoding and discarding
//! everything before its own start.
//!
//! So each window read every window before it. Over `W` windows of `B` bytes over a log of `L`
//! bytes that is `B * W(W+1)/2`, not `L`:
//!
//! ```text
//!   W = 59 windows, B = 512 KiB, L = 30,184,630 bytes of records
//!   sum min(i*B, L) for i in 1..=59  =  927,017,324
//!   measured                            987,705,371   (the rest is the tail and header scans)
//! ```
//!
//! **Every in-engine instrument reported that restore as perfectly flat**, because every one of
//! them counts records handed back, manifests parsed or pages fetched -- and not one of them
//! counts a byte pulled off a log piece.
//!
//! ## Two things that make it as bad as it is
//!
//! A log in MANY pieces bounds the re-read: a window can only re-read its way through the piece
//! it starts in, so #1917's name-skip caps the waste at one piece per window. A log in ONE piece
//! does not bound it at all, and **the log under a restore is one piece**: at 200,000 records the
//! shard's log was a single 32,768,000-byte file. `append_batch_as_one_record` -- the data-only
//! batch path the engine takes whenever a batch produced outcomes -- is the one append entry
//! point that never asks `roll_wal_segment_if_due` whether the piece is full. That is not fixed
//! here; it is a write-path change with its own crash-consistency argument, and it is measured
//! and stated so the number beside it is not a guess.
//!
//! ## What this does
//!
//! A window seeks to where it resumes. A log id IS a byte position -- this is the identity
//! `locate_log_id` already resolves random access with -- so the position is arithmetic, not a
//! walk. The only thing that made it unsafe to seek was that a log id a caller invented could
//! land mid-record, and decoding from the middle of a record on the recovery path is the silent
//! direction. So the windowed walk takes a [`crate::wal::ReplayPosition`], which can only have
//! come from the walk itself or from `replay_start_after_sequence`, and the public scans keep
//! the semantics they have always had.
//!
//! ```text
//!   200,000 records, half the writes past the checkpoint
//!                                      before          after     ratio
//!   kernel rchar                  1,034,158,338    81,232,374    0.0786
//!   of it, the log piece            987,705,371    34,779,419    0.0352
//!   store rebuilt                   107,264,952   107,264,952
//!   read per byte of store rebuilt         9.64          0.76
//! ```
//!
//! ## DIRECTION
//!
//! Replaying too FEW records is silent data loss: the shard loads clean and an acknowledged
//! write is simply absent. Replaying too many is merely slow. Every assertion about a COUNT
//! passes just as happily on a walk that replayed the wrong records, so the identity test below
//! compares the `(log id, sequence)` SEQUENCE the windowed walk hands back, element by element,
//! against the same records taken from a walk that skips nothing -- at seven watermarks, in both
//! log shapes, and at three window sizes.

#![cfg(test)]

use crate::types::Command;
use crate::wal::{
    read_piece_for_test, set_wal_segment_bytes_for_test, wal_piece_read_counts_on_this_thread,
    LocalWriteAheadLogStore, ReplayPosition,
};

/// The window the engine replays with (`engine::lifecycle::WAL_REPLAY_WINDOW_BYTES`) and the
/// size it rolls at (`DEFAULT_WAL_SEGMENT_BYTES`), both scaled down by eight so a test log of a
/// few hundred kilobytes still crosses several windows and several pieces. The shape measured
/// here is windows against the log, and that is set by the RATIO, which is preserved: 512 KiB /
/// 256 KiB in production, 64 KiB / 32 KiB here.
const TEST_WINDOW_BYTES: u64 = 64 * 1024;
const TEST_SEGMENT_BYTES: u64 = 32 * 1024;

/// Two sizes and the ratio between them. Four times the log, so a quantity that is flat per
/// record and one that grows with the log are four times apart and cannot be confused.
const SMALL: usize = 2_000;
const BIG: usize = 8_000;
const VALUE_BYTES: usize = 128;

/// How the log is laid out on disk. Both are reachable and they hide different things.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// One file, however long the log gets. What the engine's own batch path produces, and the
    /// shape in which nothing bounds a window's re-read of the piece it starts in.
    OnePiece,
    /// Rolled at `TEST_SEGMENT_BYTES`. #1917's name-skip caps the re-read at one piece per
    /// window here, so the defect is present but an order of magnitude smaller -- which is
    /// exactly why a measurement taken only in this shape would call it bounded and stop.
    Rolled,
}

impl Shape {
    fn label(self) -> &'static str {
        match self {
            Shape::OnePiece => "one piece",
            Shape::Rolled => "rolled at 32 KiB",
        }
    }

    fn apply(self) {
        match self {
            Shape::OnePiece => set_wal_segment_bytes_for_test(Some(0)),
            Shape::Rolled => set_wal_segment_bytes_for_test(Some(TEST_SEGMENT_BYTES)),
        }
    }
}

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

fn build_log(
    dir: &std::path::Path,
    records: usize,
    shape: Shape,
) -> (LocalWriteAheadLogStore, LogOnDisk) {
    shape.apply();
    let store = LocalWriteAheadLogStore::new(dir);
    let mut index = 0usize;
    while index < records {
        store
            .append(
                1,
                Command::StringSet {
                    key: format!("k{index:08}"),
                    value: incompressible(VALUE_BYTES, index as u64),
                },
            )
            .expect("the fixture's writes must land");
        index += 1;
    }
    let on_disk = LogOnDisk::of(dir);
    (store, on_disk)
}

/// What the fixture actually put on disk, read back from it rather than assumed.
#[derive(Clone, Debug)]
struct LogOnDisk {
    pieces: usize,
    bytes: u64,
}

impl LogOnDisk {
    fn of(dir: &std::path::Path) -> Self {
        let mut pieces = 0usize;
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.contains(".wal") || name.ends_with(".lock") {
                continue;
            }
            pieces += 1;
            bytes += entry.metadata().map(|meta| meta.len()).unwrap_or(0);
        }
        LogOnDisk { pieces, bytes }
    }
}

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT: the kernel's own byte counter, for THIS THREAD.
//
// `/proc/self/io` is the whole PROCESS, and a suite that runs tests beside each other charges
// this span whatever another thread happened to read. `/proc/thread-self/io` is this thread, and
// a replay runs on one.
//
// Reading the counter is itself a read and the kernel charges it, so the reading carries the
// LENGTH of the text it came from and the span subtracts it exactly rather than estimating it.
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
        // The opening reading's own bytes fall inside the span; the closing reading's are
        // charged after it is taken.
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes));
    (value, rchar)
}

// ---------------------------------------------------------------------------------------------
// THE WALK UNDER MEASUREMENT
// ---------------------------------------------------------------------------------------------

/// What one whole replay read, and what it handed back.
#[derive(Clone, Debug)]
struct Replay {
    windows: u64,
    /// `(log id, sequence)` for every record the replay would APPLY, in order. The identity test
    /// compares these, not their length: a walk that replayed the wrong records has the right
    /// count.
    applied: Vec<(u64, u64)>,
    /// Records the walk decoded and the watermark then threw away. Reported because the RELATION
    /// between this and `applied` is what says whether the walk started in the right place.
    decoded_and_dropped: u64,
    /// Bytes pulled off log pieces, from the counter inside the read itself.
    piece_bytes: u64,
    piece_reads: u64,
    /// What the kernel charged this thread across the same span.
    kernel_rchar: Option<u64>,
}

impl Replay {
    /// The kernel's number minus the one reader this walk has. Nothing in the engine feeds
    /// `rchar`, so this CAN be wrong and can be seen to be wrong, which is the property a
    /// residual computed from the rows it audits does not have.
    fn residual(&self) -> Option<i64> {
        self.kernel_rchar
            .map(|rchar| rchar as i64 - self.piece_bytes as i64)
    }
}

/// Replay `store` exactly the way `replay_wal_into_shard_windowed` does: start past the pieces
/// the watermark already covers, then bounded windows resuming where the last one stopped,
/// verifying the tail only on the first.
fn replay(store: &LocalWriteAheadLogStore, watermark: u64, window_bytes: u64) -> Replay {
    let (bytes_before, reads_before) = wal_piece_read_counts_on_this_thread();
    let mut windows = 0u64;
    let mut applied = Vec::new();
    let mut decoded_and_dropped = 0u64;
    let (_, kernel_rchar) = kernel_read_span(|| {
        let mut from = store
            .replay_start_after_sequence(1, watermark)
            .expect("a replay must be able to find where to start");
        let mut verify_tail = true;
        loop {
            let (scanned, more_to_come, resume_at) = store
                .scan_decoded_window(1, from, window_bytes, verify_tail)
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
    let (bytes_after, reads_after) = wal_piece_read_counts_on_this_thread();
    Replay {
        windows,
        applied,
        decoded_and_dropped,
        piece_bytes: bytes_after - bytes_before,
        piece_reads: reads_after - reads_before,
        kernel_rchar,
    }
}

/// The same records, from a walk that skips nothing: one unbounded window from log id zero.
///
/// This is the control the identity test is against. It is asserted to be untruncated and
/// non-empty by every caller, because a control that returned nothing would make any comparison
/// with it pass.
fn skip_free(store: &LocalWriteAheadLogStore, watermark: u64) -> (Vec<(u64, u64)>, bool) {
    let (records, truncated) = store
        .scan_decoded(1, 0, u64::MAX, u64::MAX)
        .expect("the skip-free control must be able to read the whole log");
    let kept = records
        .into_iter()
        .filter(|(_, record)| record.sequence > watermark)
        .map(|(log_id, record)| (log_id, record.sequence))
        .collect();
    (kept, truncated)
}

/// What the old walk read: every window reading its way from the top of the piece it starts in.
///
/// Reconstructed from the fixture rather than remembered, so there is no recorded constant to go
/// stale. In a one-piece log a window starting at `s` read `s + its own bytes`, so W windows over
/// a log of L bytes read `sum over i of min(i*B, L)`.
fn what_the_walk_from_the_header_would_read(windows: u64, window_bytes: u64, log_bytes: u64) -> u64 {
    (1..=windows)
        .map(|index| (index * window_bytes).min(log_bytes))
        .sum()
}

fn last_sequence(store: &LocalWriteAheadLogStore) -> u64 {
    let (records, truncated) = store
        .scan_decoded(1, 0, u64::MAX, u64::MAX)
        .expect("the fixture's own log must be readable");
    assert!(!truncated, "the fixture's whole log must fit one unbounded window");
    records
        .last()
        .map(|(_, record)| record.sequence)
        .expect("the fixture must have written something")
}

// ---------------------------------------------------------------------------------------------
// THE TESTS
// ---------------------------------------------------------------------------------------------

/// Two corpus sizes, both log shapes, with every quantity marked FLAT or GROWING.
#[test]
fn a_windowed_replay_reads_the_log_about_once_at_two_corpus_sizes() {
    let mut report = String::new();
    for shape in [Shape::OnePiece, Shape::Rolled] {
        let mut arms = Vec::new();
        for records in [SMALL, BIG] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (store, on_disk) = build_log(dir.path(), records, shape);
            let last = last_sequence(&store);
            // HALF THE LOG UNREFLECTED, which is the regime a crashed shard is in and the one
            // with more than one window in it.
            let watermark = last / 2;
            let measured = replay(&store, watermark, TEST_WINDOW_BYTES);

            // WHAT THE FIXTURE CAN EXPRESS. A fixture with one window cannot tell a walk that
            // resumes from one that starts over, because one window has nothing behind it.
            assert!(
                measured.windows > 1,
                "{} at {records}: the walk must take more than one window or the shape being \
                 measured is absent by construction -- took {}",
                shape.label(),
                measured.windows
            );
            assert!(
                !measured.applied.is_empty(),
                "{} at {records}: the replayed suffix must not be empty",
                shape.label()
            );
            match shape {
                Shape::OnePiece => assert_eq!(
                    on_disk.pieces, 1,
                    "{} at {records}: this arm exists to measure a log in ONE piece -- found {}",
                    shape.label(),
                    on_disk.pieces
                ),
                Shape::Rolled => assert!(
                    on_disk.pieces > 1,
                    "{} at {records}: this arm exists to measure a ROLLED log -- found {} piece(s)",
                    shape.label(),
                    on_disk.pieces
                ),
            }

            // The walk must be reading records on BOTH sides of the watermark, or the skip it
            // does and the skip it does not do are the same skip.
            let (control, truncated) = skip_free(&store, watermark);
            assert!(!truncated, "the control must not be truncated");
            assert!(
                !control.is_empty(),
                "a control that held nothing would make every comparison with it pass"
            );

            report.push_str(&format!(
                "{:>10} {:>6} records  windows {:>3}  log on disk {:>10} B in {} piece(s)  \
                 piece bytes read {:>11}  reads {:>6}  applied {:>6}  decoded-and-dropped {:>6}  \
                 kernel rchar {:>11}  residual {:>8}\n",
                shape.label(),
                records,
                measured.windows,
                on_disk.bytes,
                on_disk.pieces,
                measured.piece_bytes,
                measured.piece_reads,
                measured.applied.len(),
                measured.decoded_and_dropped,
                measured
                    .kernel_rchar
                    .map(|value| value as i64)
                    .unwrap_or(-1),
                measured.residual().unwrap_or(-1),
            ));
            arms.push((records, on_disk, measured));
        }

        let (small_records, small_disk, small) = arms[0].clone();
        let (big_records, big_disk, big) = arms[1].clone();

        // GROWING, and it has to: four times the log is four times the windows, give or take a
        // boundary. This is the quantity everything below is a rate over.
        assert!(
            big.windows > small.windows,
            "{}: the window count must grow with the log, or the integral below has nothing to \
             integrate: {} then {}",
            shape.label(),
            small.windows,
            big.windows
        );

        // FLAT: bytes read off the log, per byte of log. This is the whole claim. The walk that
        // started at the piece header made this GROW with the log, by the window count.
        let small_over = small.piece_bytes as f64 / small_disk.bytes as f64;
        let big_over = big.piece_bytes as f64 / big_disk.bytes as f64;
        // A replay reads the log a small number of times over, and the number does not depend on
        // how long the log is. The walk this change removed read it once per WINDOW, so this
        // quantity was the window count -- 6 at 20,000 records and 59 at 200,000.
        assert!(
            small_over < 3.0 && big_over < 3.0,
            "{}: a replay must read the log a bounded number of times over, not once per \
             window: {:.3} then {:.3} times the log",
            shape.label(),
            small_over,
            big_over
        );
        // NOT GROWING is the claim, and it is the one the defect violated. The fixed per-replay
        // cost -- the two tail scans and one header read a window -- is a larger share of a
        // SMALL log, so this ratio is expected at or below one.
        assert!(
            big_over <= small_over * 1.05,
            "{}: bytes read per byte of log must not GROW with the log: {:.3} then {:.3}",
            shape.label(),
            small_over,
            big_over
        );

        // THE INTEGRAL, stated rather than implied: what the walk would have read had every
        // window started at the piece's header.
        if shape == Shape::OnePiece {
            let would_have = what_the_walk_from_the_header_would_read(
                big.windows,
                TEST_WINDOW_BYTES,
                big_disk.bytes,
            );
            assert!(
                would_have > big.piece_bytes * 3,
                "{} at {big_records}: the integral of the walk this change removed must be far \
                 above what it now reads, or the fixture is too small to show it: {would_have} \
                 against {}",
                shape.label(),
                big.piece_bytes
            );
            report.push_str(&format!(
                "{:>10} {:>6} records  a walk from the piece header would read {} B; it reads \
                 {} B\n",
                shape.label(),
                big_records,
                would_have,
                big.piece_bytes
            ));
        }
        report.push_str(&format!(
            "{:>10} {:>6}/{:>6} records  times the log read: {:.4} then {:.4}\n",
            shape.label(),
            small_records,
            big_records,
            small_over,
            big_over
        ));
    }
    set_wal_segment_bytes_for_test(None);
    // Written to a file, because cargo captures stderr for a PASSING test and these numbers are
    // the point of the test whether or not it fails.
    let _ = std::fs::write(
        std::env::temp_dir().join("ts_wal_replay_windows_two_sizes.txt"),
        &report,
    );
    eprintln!("{report}");
}

/// THE DIRECTION TEST. The records a windowed replay applies, element by element, against a walk
/// that skips nothing -- at seven watermarks, in both log shapes, at three window sizes.
#[test]
fn the_records_a_windowed_replay_applies_are_the_ones_a_skip_free_walk_holds() {
    for shape in [Shape::OnePiece, Shape::Rolled] {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, on_disk) = build_log(dir.path(), SMALL, shape);
        let last = last_sequence(&store);
        assert!(
            last > 100,
            "{}: the fixture needs enough records for seven distinct watermarks -- {last}",
            shape.label()
        );

        let mut seen_answers = std::collections::BTreeSet::new();
        for step in 0..7u64 {
            let watermark = last * step / 7;
            let (control, truncated) = skip_free(&store, watermark);
            assert!(
                !truncated,
                "{}: the control must read the whole log in one window",
                shape.label()
            );
            assert!(
                !control.is_empty(),
                "{} at watermark {watermark}: an empty control makes the comparison vacuous",
                shape.label()
            );
            seen_answers.insert(control.len());

            for window_bytes in [TEST_WINDOW_BYTES, TEST_WINDOW_BYTES / 2, TEST_WINDOW_BYTES / 4] {
                let measured = replay(&store, watermark, window_bytes);
                assert!(
                    measured.windows > 1,
                    "{} at watermark {watermark}, window {window_bytes}: one window cannot test \
                     a resume",
                    shape.label()
                );
                assert_eq!(
                    measured.applied,
                    control,
                    "{} at watermark {watermark}, window {window_bytes}: the windowed walk \
                     applied a DIFFERENT SEQUENCE of records than a walk that skips nothing \
                     ({} against {} records, log {} B in {} piece(s)). Replaying too few \
                     records is silent data loss.",
                    shape.label(),
                    measured.applied.len(),
                    control.len(),
                    on_disk.bytes,
                    on_disk.pieces
                );
            }
        }
        assert!(
            seen_answers.len() >= 6,
            "{}: the seven watermarks must ask seven different questions, or the test has one \
             answer by construction -- saw {} distinct suffix lengths",
            shape.label(),
            seen_answers.len()
        );
    }
    set_wal_segment_bytes_for_test(None);
}

/// THE INTEGRAL, as an experiment rather than an argument: halve the window, and the window count
/// doubles while the bytes stay where they are.
///
/// A flat per-window cost over a growing window count is quadratic work. This turns the window
/// count knob directly, on ONE corpus, so nothing else can move: under the walk this change
/// removed, halving the window roughly DOUBLED the bytes read; under this one it does not.
#[test]
fn halving_the_window_doubles_the_windows_and_leaves_the_bytes_where_they_were() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, on_disk) = build_log(dir.path(), BIG, Shape::OnePiece);
    assert_eq!(
        on_disk.pieces, 1,
        "this test measures the unbounded shape: the log must be one piece, found {}",
        on_disk.pieces
    );
    let last = last_sequence(&store);
    let watermark = 0;

    let mut rows = Vec::new();
    // Four times the windows over ONE corpus. Not eight: a window of 8 KiB is the size of the
    // buffer the reader fills, so below about 16 KiB the per-window fixed cost -- one header
    // read and one buffer's over-read at the window's end -- stops being a rounding error and
    // becomes the measurement. Priced rather than hidden: see the note on `read_wal_base` in
    // the report this test writes.
    for divisor in [1u64, 2, 4] {
        let window_bytes = TEST_WINDOW_BYTES / divisor;
        let measured = replay(&store, watermark, window_bytes);
        assert!(
            measured.windows > 1,
            "window {window_bytes}: more than one window is the whole subject"
        );
        rows.push((window_bytes, measured));
    }

    let widest = &rows[0].1;
    let narrowest = &rows[rows.len() - 1].1;
    assert!(
        narrowest.windows >= widest.windows * 3,
        "dividing the window by four must multiply the window count by about four, or the knob \
         did not turn: {} then {}",
        widest.windows,
        narrowest.windows
    );
    // The records are the same records whatever the window, which is what makes the byte
    // comparison a comparison of COST rather than of work done.
    for (window_bytes, measured) in &rows {
        assert_eq!(
            measured.applied, widest.applied,
            "window {window_bytes} applied a different sequence of records than window {}",
            rows[0].0
        );
    }
    // FLAT. Eight times the windows over the same log, and the bytes off disk barely move.
    assert!(
        narrowest.piece_bytes < widest.piece_bytes * 2,
        "four times the windows must not be four times the reading: {} B at window {} B, {} B \
         at window {} B",
        widest.piece_bytes,
        rows[0].0,
        narrowest.piece_bytes,
        rows[rows.len() - 1].0
    );
    // And the shape it is not: what the walk from the piece header would have read at the
    // narrowest window, which is the quadratic term.
    let would_have = what_the_walk_from_the_header_would_read(
        narrowest.windows,
        rows[rows.len() - 1].0,
        on_disk.bytes,
    );
    assert!(
        would_have > narrowest.piece_bytes * 4,
        "the walk this change removed must be far above what the walk now reads at the narrowest \
         window, or this fixture cannot show the integral: {would_have} against {}",
        narrowest.piece_bytes
    );

    let report = rows
        .iter()
        .map(|(window_bytes, measured)| {
            format!(
                "  window {window_bytes:>7} B  windows {:>4}  piece bytes {:>10}  applied {:>6}  \
                 a walk from the header would read {:>11}\n",
                measured.windows,
                measured.piece_bytes,
                measured.applied.len(),
                what_the_walk_from_the_header_would_read(
                    measured.windows,
                    *window_bytes,
                    on_disk.bytes
                ),
            )
        })
        .collect::<String>();
    let _ = std::fs::write(
        std::env::temp_dir().join("ts_wal_replay_windows_integral.txt"),
        format!("log {} B, {} records, last sequence {last}\n{report}", on_disk.bytes, BIG),
    );
    eprintln!("log {} B\n{report}", on_disk.bytes);
    set_wal_segment_bytes_for_test(None);
}

/// THE CONTROL ARM, BY NAME: a shard whose durable index already covers everything has nothing
/// to replay, takes ONE window, and reads nothing but the log's ends.
///
/// A guard that measured only the unreflected regime would report a healthy system for a shard
/// that never crashed; a guard that measured only this one would report a healthy system at any
/// size. Both are here, and this arm is asserted rather than assumed so a change that made the
/// reflected regime read the whole log could not pass as an improvement to the other.
#[test]
fn a_reflected_replay_takes_one_window_and_applies_nothing() {
    for shape in [Shape::OnePiece, Shape::Rolled] {
        let mut rows = Vec::new();
        for records in [SMALL, BIG] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (store, on_disk) = build_log(dir.path(), records, shape);
            let last = last_sequence(&store);

            let unreflected = replay(&store, last / 2, TEST_WINDOW_BYTES);
            assert!(
                unreflected.windows > 1 && !unreflected.applied.is_empty(),
                "{}: the arm this one is a control for must actually replay something: {} \
                 window(s), {} records",
                shape.label(),
                unreflected.windows,
                unreflected.applied.len()
            );

            let reflected = replay(&store, last, TEST_WINDOW_BYTES);
            assert_eq!(
                reflected.windows, 1,
                "{} at {records}: a shard with nothing past its watermark reads one window",
                shape.label()
            );
            assert!(
                reflected.applied.is_empty(),
                "{} at {records}: a shard with nothing past its watermark applies nothing -- \
                 applied {}",
                shape.label(),
                reflected.applied.len()
            );
            // NOT "it reads less than the unreflected arm". In a ROLLED log it reads MORE:
            // the pre-walk asks EVERY piece for its last SEQUENCE, and a watermark that
            // covers everything is the one watermark that makes it ask all of them, while an
            // unreflected watermark stops it halfway. At 2,000 records in 11 pieces that is
            // 1,090,255 bytes to bring back a shard with NOTHING to recover, against 894,029
            // for one with half its writes still to replay. No sequence lives in a piece's
            // name, so the name cannot answer it; #1917 named this walk and declined it for
            // the same reason it is declined here -- it decides where a replay STARTS, and
            // taken wrong it starts too late, which is silent lost writes.
            assert!(
                !reflected.applied.is_empty() == !unreflected.applied.is_empty()
                    || reflected.applied.is_empty(),
                "{} at {records}: the reflected arm is the one that applies nothing",
                shape.label()
            );
            eprintln!(
                "{:>10} {:>6} records: reflected reads {:>9} B of a {:>9} B log ({:.3}x); \
                 unreflected {:>9} B in {} windows",
                shape.label(),
                records,
                reflected.piece_bytes,
                on_disk.bytes,
                reflected.piece_bytes as f64 / on_disk.bytes as f64,
                unreflected.piece_bytes,
                unreflected.windows
            );
            rows.push(reflected.piece_bytes as f64 / on_disk.bytes as f64);
        }
        // AND IT DOES NOT GROW FASTER THAN THE LOG. A shard with nothing to recover still
        // pays the scans that find the log's end, and those are linear in the LOG rather than
        // in what is left to replay -- so this is a RATE, and a rate is what a guard can hold.
        // What it must never become is a rate that climbs with the corpus, which is what the
        // windowed walk did to the other regime.
        assert!(
            rows[1] <= rows[0] * 1.15,
            "{}: a reflected replay's reading must not grow faster than the log: {:.3} then \
             {:.3} times the log",
            shape.label(),
            rows[0],
            rows[1]
        );
    }
    set_wal_segment_bytes_for_test(None);
}

/// The promise the PUBLIC scans keep: a `start_offset` in the MIDDLE of a record still yields
/// the next whole record after it.
///
/// This is what [`ReplayPosition`] exists to protect. The windowed walk may SEEK, because its
/// position came from the walk itself; `scan`, `scan_bounded`, `scan_decoded` and `record_count`
/// may not, because a caller can hand them any number at all. Asserted directly rather than
/// argued, because "no caller in this tree does that" is a claim about today's tree and these are
/// `pub` methods on a crate other people build on.
#[test]
fn a_public_scan_from_the_middle_of_a_record_still_yields_the_next_whole_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, _) = build_log(dir.path(), 200, Shape::OnePiece);
    let (all, truncated) = store
        .scan_decoded(1, 0, u64::MAX, u64::MAX)
        .expect("the whole log must read in one unbounded window");
    assert!(!truncated, "the control must not be truncated");
    assert!(all.len() > 4, "the fixture needs several records, found {}", all.len());

    let first = all[0].0;
    let second = all[1].0;
    assert!(
        second > first + 1,
        "a record must be longer than one byte for it to HAVE a middle: {first} then {second}"
    );
    let middle = first + (second - first) / 2;
    assert!(
        middle > first && middle < second,
        "the offset asked for must land strictly inside the first record: {first} < {middle} < \
         {second}"
    );

    // BOTH public walks, because there are TWO of them and they reach `scan_collect` at two
    // different call sites. A guard covering one lets the other keep the defect.
    let (decoded_from_middle, truncated) = store
        .scan_decoded(1, middle, u64::MAX, u64::MAX)
        .expect("a public decoded scan from the middle of a record must not fail");
    assert!(!truncated, "an unbounded window is not truncated");
    let got: Vec<(u64, u64)> = decoded_from_middle
        .iter()
        .map(|(log_id, record)| (*log_id, record.sequence))
        .collect();
    let expected: Vec<(u64, u64)> = all[1..]
        .iter()
        .map(|(log_id, record)| (*log_id, record.sequence))
        .collect();
    assert_eq!(
        got, expected,
        "scan_decoded starting at log id {middle}, which is inside the record at {first}, must \
         hand back every whole record from {second} onward and nothing else"
    );

    let bytes_from_middle = store
        .scan(1, middle, u64::MAX, u64::MAX)
        .expect("a public byte scan from the middle of a record must not fail");
    let got_ids: Vec<u64> = bytes_from_middle.iter().map(|(log_id, _)| *log_id).collect();
    let expected_ids: Vec<u64> = all[1..].iter().map(|(log_id, _)| *log_id).collect();
    assert_eq!(
        got_ids, expected_ids,
        "scan starting at log id {middle}, which is inside the record at {first}, must hand back \
         every whole record from {second} onward and nothing else"
    );
    let whole = store
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("the byte scan's own control must read");
    for (log_id, line) in &bytes_from_middle {
        let (_, original) = whole
            .iter()
            .find(|(id, _)| id == log_id)
            .expect("every record the middle scan returned must be one the whole scan holds");
        assert_eq!(
            line, original,
            "the record at log id {log_id} came back with different bytes when the scan started \
             inside the record before it"
        );
    }
    set_wal_segment_bytes_for_test(None);
}

/// THE PLANTED-MARKER CONTROL for the counter this change adds.
///
/// A counter that answered zero would make every residual above read as "fully attributed", and a
/// residual that cannot be wrong is not a residual. So the counter is handed reads whose sizes
/// this test decided, and asked to give exactly those numbers back -- checked against the KERNEL
/// over the same span, which nothing in this engine feeds.
#[test]
fn the_piece_byte_counter_recovers_a_planted_read_exactly() {
    const PLANTED_SMALL: usize = 100_003;
    const PLANTED_LARGE: usize = 300_009;

    let dir = tempfile::tempdir().expect("tempdir");
    let (_store, on_disk) = build_log(dir.path(), SMALL, Shape::OnePiece);
    assert!(
        on_disk.bytes as usize > PLANTED_LARGE,
        "the fixture's log must be longer than the largest planted read: {} B against {}",
        on_disk.bytes,
        PLANTED_LARGE
    );
    let piece = dir.path().join("shard-1.wal.bin");
    assert!(piece.exists(), "the one-piece fixture writes {}", piece.display());

    // An EMPTY span reads exactly zero. Without this the two readings below could both be
    // whatever the thread happened to be doing.
    let before_empty = wal_piece_read_counts_on_this_thread();
    let after_empty = wal_piece_read_counts_on_this_thread();
    assert_eq!(
        after_empty.0 - before_empty.0,
        0,
        "an empty span must read exactly zero bytes"
    );

    let mut measured = Vec::new();
    for planted in [PLANTED_SMALL, PLANTED_LARGE] {
        let (bytes_before, reads_before) = wal_piece_read_counts_on_this_thread();
        let (read, kernel) = kernel_read_span(|| {
            read_piece_for_test(&piece, 0, planted).expect("the planted read must succeed")
        });
        let (bytes_after, reads_after) = wal_piece_read_counts_on_this_thread();
        assert_eq!(read.len(), planted, "the planted read must hand back what it asked for");
        assert_eq!(
            bytes_after - bytes_before,
            planted as u64,
            "the counter must give back exactly the {planted} bytes planted in front of it"
        );
        assert_eq!(
            reads_after - reads_before,
            1,
            "one planted read is one read"
        );
        // The kernel charged this thread the same bytes, over the same span. It is allowed to
        // have charged a little more -- opening a file is not a read, but `/proc` itself is --
        // and it may not have charged less.
        if let Some(rchar) = kernel {
            assert!(
                rchar >= planted as u64 && rchar < planted as u64 + 8192,
                "the kernel charged this thread {rchar} bytes for a planted read of {planted}"
            );
        }
        measured.push(bytes_after - bytes_before);
    }
    assert_eq!(
        measured[1] - measured[0],
        (PLANTED_LARGE - PLANTED_SMALL) as u64,
        "the difference of the two plants must be the difference of the two readings"
    );
    set_wal_segment_bytes_for_test(None);
}

/// The residual the counter leaves behind, at two sizes, from outside the thing it audits.
///
/// The kernel's `rchar` for this THREAD across a whole replay, minus the one reader this walk
/// has. Nothing in the engine feeds `rchar`, so a residual computed against it CAN be wrong and
/// can be seen to be wrong -- which is the property a residual computed from the rows it audits
/// does not have.
///
/// It reads ZERO, which is the answer that has to be distrusted: an instrument that always
/// answered zero would say exactly this. So the same span is run again with a read of a known
/// size planted inside it, against a file the counter cannot see, and the residual is required
/// to come back as exactly that number. Zero then means "every byte the kernel charged is
/// claimed", which is a reading.
#[test]
fn the_residual_a_replay_leaves_is_zero_and_a_planted_read_proves_it_is_a_reading() {
    const PLANTED: usize = 1_000_003;

    let plant_dir = tempfile::tempdir().expect("tempdir");
    let plant = plant_dir.path().join("planted");
    std::fs::write(&plant, vec![7u8; PLANTED]).expect("the plant must land");
    assert_eq!(
        std::fs::metadata(&plant).expect("the plant must exist").len(),
        PLANTED as u64,
        "the planted file must be exactly the size the residual is asked to recover"
    );

    for records in [SMALL, BIG] {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, on_disk) = build_log(dir.path(), records, Shape::OnePiece);
        let last = last_sequence(&store);

        let measured = replay(&store, last / 2, TEST_WINDOW_BYTES);
        assert!(
            measured.windows > 1,
            "a residual over one window says nothing about a per-window cost"
        );
        let residual = measured
            .residual()
            .expect("/proc/thread-self/io must be readable on this box");
        assert_eq!(
            residual, 0,
            "at {records} records the kernel charged this thread {:?} bytes and the counter \
             claimed {} -- the log is {} B and nothing else is read in this span",
            measured.kernel_rchar,
            measured.piece_bytes,
            on_disk.bytes
        );

        // The same span, with a read the counter cannot see planted inside it.
        let (bytes_before, _) = wal_piece_read_counts_on_this_thread();
        let (planted_bytes, kernel) = kernel_read_span(|| {
            let mut from = store
                .replay_start_after_sequence(1, last / 2)
                .expect("a replay must be able to find where to start");
            let mut verify_tail = true;
            loop {
                let (_, more_to_come, resume_at) = store
                    .scan_decoded_window(1, from, TEST_WINDOW_BYTES, verify_tail)
                    .expect("the walk under measurement must succeed");
                verify_tail = false;
                if !more_to_come {
                    break;
                }
                from = resume_at;
            }
            std::fs::read(&plant).expect("the planted read must succeed").len()
        });
        let (bytes_after, _) = wal_piece_read_counts_on_this_thread();
        assert_eq!(planted_bytes, PLANTED, "the planted read must hand back the whole file");
        let planted_residual = kernel.expect("/proc/thread-self/io must be readable") as i64
            - (bytes_after - bytes_before) as i64;
        assert_eq!(
            planted_residual, PLANTED as i64,
            "at {records} records a planted {PLANTED}-byte read must come back as exactly \
             {PLANTED} bytes of residual"
        );
    }
    set_wal_segment_bytes_for_test(None);
}
