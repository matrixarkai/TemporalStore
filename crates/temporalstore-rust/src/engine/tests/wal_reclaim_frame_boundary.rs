// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Why the write-ahead-log half of a maintenance round freed nothing, decided at the byte.
//!
//! #1928 measured a dump at 20,000 and 200,000 records and reported, as a side result it
//! deliberately did not explain, that the write-ahead-log half released 0 B in 0 records at
//! both sizes -- while a single-command control arm in the same fixture released 261,933 B.
//! It named three candidates (how many records the batch path wrote, the total bytes in the
//! log, a newline byte inside the payload), drove each, and ruled each out.
//!
//! The decision is here, and it is none of the three. It is the LAST byte of the payload.
//!
//! A record is written as a binary frame -- marker, a varint length, a four-byte digest, then
//! exactly that many payload bytes -- and `TS_WAL_BINARY_FRAME` is ON unless it is switched off,
//! so this is what every record is. A binary frame carries no delimiter: it ends where its
//! declared length ends. The reclaim walk in `gc_before_sequence_unchecked` took what
//! `read_raw_record` handed it and ran `strip_suffix(b"\n")` over it before decoding, which is
//! right for a text record and wrong for a frame. When the payload's final byte happened to be
//! `0x0A` the strip took it, `next_frame` then saw one byte fewer than the frame declared,
//! called it a torn tail, and `decode_line` turned that into `binary record is incomplete` --
//! refusing a sweep over records that were entirely intact on disk.
//!
//! The error then reached `dump_and_reclaim_index_logs_with_min_reclaimable`, which drops the
//! sweep's `Result` with `.ok()` and reads every field through `unwrap_or_default()`. So the
//! refusal arrived at the operator as three zeros: identical, to the byte, to a store whose log
//! held nothing at all. That is the rendering #1928 shipped a guard for, and it is why the
//! failure survived being measured twice.
//!
//! Why the three candidates could not find it: the last byte of one of these payloads is a
//! trailing varint field, so it is a deterministic function of the record's CONTENT and moves
//! with the batch's shape rather than with the number of records, the bytes in the log, or the
//! bytes inside the payload. `b'\n'` written as a VALUE -- candidate three -- lands in the
//! middle of the payload and changes nothing, which is exactly what #1928 measured.
//!
//! The same rule was already written down, correctly, in the sibling walk a few hundred lines
//! away in `wal.rs` (the one that fills in a piece's record count), with a comment saying a
//! binary frame ends where its declared length ends and trimming it would take a byte of the
//! payload. The reclaim walk did not have it. Two copies of one rule, one of them right.
//!
//! DIRECTION. Freeing too much is silent data loss; freeing too little is space held. This
//! defect freed too little -- the sweep refused before it rewrote anything -- so nothing was
//! lost. The strong form is asserted anyway, element by element against a control that never
//! reclaimed, because a count cannot see a sweep that drops one record and keeps an extra.

use super::*;
use crate::wal::{decode_wal_line, LocalWriteAheadLogStore, WalOutcomeItem};
use std::path::Path;

const SHARD: ShardId = 1;

fn release_engine(dir: &Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

/// Where the search for a batch shape starts. 100 items per batch encoded to a payload ending
/// in `0x0A` when this was written, so the search finds it on the first try; the search exists
/// so that a change to the encoding moves the fixture instead of breaking it, and the assertion
/// that one was FOUND is what stops it quietly testing nothing.
const SEARCH_FROM: usize = 100;
const SEARCH_TRIES: usize = 512;

fn outcomes(count: usize) -> Vec<WalOutcomeItem> {
    (0..count)
        .map(|index| WalOutcomeItem {
            kind: "string".to_string(),
            object_key: format!("batch-key-{index:09}"),
            component: None,
            object_id: index as u64,
            routing_bucket: index as u32,
            address: None,
            value: Some(vec![b'v'; 8]),
            ttl: None,
            deleted: false,
            meta: false,
        })
        .collect()
}

/// The last byte of the payload a batch of `items` encodes to.
///
/// Measured from the file rather than predicted: this is the quantity the whole defect turns on,
/// and the fixture below has to be able to say which side of it each arm sits on.
fn trailing_payload_byte(items: usize) -> u8 {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalWriteAheadLogStore::new(dir.path());
    seed_batches(&store, 1, items);
    frames_of(&active_log(dir.path()))
        .first()
        .expect("one batch wrote one frame")
        .last_payload_byte
}

/// The first batch size at or after `SEARCH_FROM` whose payload ends in the delimiter byte, and
/// the first that does not. Both, or the test says so rather than running a one-sided compare.
fn batch_sizes_either_side_of_the_delimiter() -> (usize, usize) {
    let mut ends_in_delimiter = None;
    let mut does_not = None;
    for items in SEARCH_FROM..(SEARCH_FROM + SEARCH_TRIES) {
        let byte = trailing_payload_byte(items);
        if byte == b'\n' && ends_in_delimiter.is_none() {
            ends_in_delimiter = Some(items);
        } else if byte != b'\n' && does_not.is_none() {
            does_not = Some(items);
        }
        if ends_in_delimiter.is_some() && does_not.is_some() {
            break;
        }
    }
    let subject = ends_in_delimiter.unwrap_or_else(|| {
        panic!(
            "APPARATUS: no batch size in {SEARCH_FROM}..{} encoded to a payload ending in 0x0A, \
             so this module cannot build the fixture it measures. The encoding moved; widen the \
             search rather than deleting the arm",
            SEARCH_FROM + SEARCH_TRIES
        )
    });
    let control = does_not.expect("APPARATUS: every batch size ended in 0x0A, which cannot be");
    (subject, control)
}

/// Every piece of the log, oldest first. A log rolls into sealed pieces once it outgrows one, and
/// a measurement that looks only at the active piece stops seeing most of it.
fn log_pieces(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found: Vec<std::path::PathBuf> = std::fs::read_dir(root)
        .expect("the log directory is readable")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .filter(|path| path.extension().map(|ext| ext != "lock").unwrap_or(true))
        .collect();
    found.sort();
    found
}

/// Bytes the log occupies on disk, summed over its pieces.
///
/// The independent quantity in the rounds test below: it comes from the filesystem, never from
/// the sweep's own report, so a sweep that miscounts cannot supply the figure that checks it.
/// It also counts whole pieces the sweep unlinks, which `bytes_before`/`bytes_after` do not --
/// those describe the active piece and report dropped pieces in a separate field.
fn log_bytes(root: &std::path::Path) -> u64 {
    log_pieces(root)
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|meta| meta.len())
        .sum()
}

fn active_log(root: &std::path::Path) -> std::path::PathBuf {
    let mut found: Vec<std::path::PathBuf> = std::fs::read_dir(root)
        .expect("the log directory is readable")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .filter(|path| path.extension().map(|ext| ext != "lock").unwrap_or(true))
        .collect();
    found.sort();
    found.into_iter().next().expect("an active log piece")
}

/// What a walker sees: every frame's start offset, its total length, and its payload's last byte.
///
/// Read from the file's own bytes by this module, never from anything the reclaim reports, so a
/// reclaim that miscounts cannot also supply the count that checks it.
///
/// Walks records only, and so stops at the first block footer: a log is laid out in 128 KiB
/// blocks with a footer slot at the end of each, and stepping over one is the engine's own
/// business. That is enough for the fixtures below, which are a few hundred frames inside the
/// first block, and it is why the rounds test measures bytes off the filesystem instead.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Frame {
    at: u64,
    total: u64,
    payload_len: u64,
    last_payload_byte: u8,
}

fn frames_of(path: &std::path::Path) -> Vec<Frame> {
    let bytes = std::fs::read(path).expect("the log piece is readable");
    let mut frames = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        // A zero where a record would start is preallocated room, never a record.
        if bytes[cursor] == 0 || bytes[cursor] != 0xB3 {
            break;
        }
        let mut declared: u64 = 0;
        let mut shift = 0u32;
        let mut at = cursor + 1;
        loop {
            let byte = bytes[at];
            at += 1;
            declared |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        let payload_end = at + 4 + declared as usize;
        if payload_end > bytes.len() {
            break;
        }
        frames.push(Frame {
            at: cursor as u64,
            total: (payload_end - cursor) as u64,
            payload_len: declared,
            last_payload_byte: bytes[payload_end - 1],
        });
        cursor = payload_end;
    }
    frames
}

fn seed_batches(store: &LocalWriteAheadLogStore, batches: usize, items: usize) {
    for _ in 0..batches {
        store
            .append_batch_as_one_record(SHARD, outcomes(items), Vec::new(), true)
            .expect("the batch appends");
    }
}

/// Every record the log holds, in order, as (sequence, the exact bytes on disk).
///
/// The element-by-element subject of the strong-form comparison below. Counts are not enough:
/// a sweep that drops one record and keeps an extra reports the same count as a correct one.
fn records_in_order(store: &LocalWriteAheadLogStore) -> Vec<(u64, Vec<u8>)> {
    store
        .scan(SHARD, 0, u64::MAX, u64::MAX)
        .expect("the log scans")
        .into_iter()
        .map(|(_, raw)| {
            let sequence = decode_wal_line(crate::log_framing::record_body(&raw))
                .expect("every record the log serves decodes")
                .sequence;
            (sequence, raw)
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// THE DECISION
// ---------------------------------------------------------------------------------------------

/// The defect, isolated to the one byte that causes it.
///
/// Two fixtures differing only in how many items each batch carried. The test asserts which
/// side of the byte each landed on -- proving the fixture can express the thing measured -- and
/// then that BOTH reclaim. Before the fix the subject arm returned
/// `binary record is incomplete` and the control arm swept cleanly.
/// rust-internal: drives the write-ahead log's own reclaim, no product behaviour
#[test]
fn a_reclaim_sweeps_a_log_whose_frames_end_in_the_delimiter_byte() {
    let (subject_items, control_items) = batch_sizes_either_side_of_the_delimiter();
    eprintln!(
        "RECLAIM FRAME fixture: {subject_items} items per batch encodes to a payload ending in \
         0x0A, {control_items} items does not"
    );
    let mut swept = Vec::new();
    for (label, items) in [("SUBJECT", subject_items), ("CONTROL", control_items)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalWriteAheadLogStore::new(dir.path());
        seed_batches(&store, 6, items);

        let frames = frames_of(&active_log(dir.path()));
        let ending_in_delimiter = frames
            .iter()
            .filter(|frame| frame.last_payload_byte == b'\n')
            .count();

        eprintln!(
            "RECLAIM FRAME {label}: {} frames, {} of them with a payload ending in 0x0A, \
             last bytes {:02X?}",
            frames.len(),
            ending_in_delimiter,
            frames
                .iter()
                .map(|frame| frame.last_payload_byte)
                .collect::<Vec<_>>(),
        );

        // FIXTURE PROOF, both directions. A stage with nothing to do reports zero and that reads
        // exactly like a stage that refused, so each arm must be shown to be the arm it claims.
        assert!(
            frames.len() >= 6,
            "APPARATUS {label}: the log holds {} frames, so there is nothing for a sweep to \
             decide about",
            frames.len()
        );
        if label == "SUBJECT" {
            assert_eq!(
                ending_in_delimiter,
                frames.len(),
                "APPARATUS SUBJECT: {ending_in_delimiter} of {} frames end in 0x0A. This arm \
                 exists to hold frames on that side of the byte; if the encoding moved, find a \
                 batch shape that puts them there again rather than dropping the arm",
                frames.len()
            );
        } else {
            assert_eq!(
                ending_in_delimiter, 0,
                "APPARATUS CONTROL: {ending_in_delimiter} of {} frames end in 0x0A, so this arm \
                 is not the other side of the byte and the comparison says nothing",
                frames.len()
            );
        }

        let before = records_in_order(&store);
        let report = store
            .gc_before_sequence_unchecked(SHARD, 4)
            .unwrap_or_else(|err| {
                panic!(
                    "{label}: the reclaim refused a log of {} intact frames: {err}. This is the \
                     defect -- the walk stripped a delimiter off a frame that declares its own \
                     length, and the frame then read one byte short of what it says it is",
                    frames.len()
                )
            });

        eprintln!(
            "RECLAIM FRAME {label}: swept before={} after={} removed={} bytes {} -> {} \
             (skipped_not_worth_rewrite={} clamped_by_block_retention={})",
            report.records_before,
            report.records_after,
            report.records_removed,
            report.bytes_before,
            report.bytes_after,
            report.skipped_not_worth_rewrite,
            report.clamped_by_block_retention,
        );

        assert!(
            report.records_removed > 0,
            "{label}: the sweep removed no records from a log of {} frames with a retain floor \
             of 4",
            frames.len()
        );

        // STRONG FORM. Freeing too much is silent, so compare the survivors element by element
        // against what the log held before, not their count.
        let after = records_in_order(&store);
        let expected: Vec<(u64, Vec<u8>)> = before
            .iter()
            .filter(|(sequence, _)| *sequence >= 4)
            .cloned()
            .collect();
        assert_eq!(
            after.len(),
            expected.len(),
            "{label}: the log serves {} records after the sweep where {} sit at or above the \
             retain floor",
            after.len(),
            expected.len()
        );
        for (index, (survivor, wanted)) in after.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                survivor.0, wanted.0,
                "{label}: survivor {index} is sequence {} where {} was retained",
                survivor.0, wanted.0
            );
            assert_eq!(
                survivor.1, wanted.1,
                "{label}: survivor {index} (sequence {}) does not match the bytes it was written \
                 with",
                survivor.0
            );
        }
        swept.push((label, report.records_removed, report.bytes_before - report.bytes_after));
    }

    // The two arms agree, which is the claim: the last byte of a payload must not change what a
    // reclaim does.
    assert_eq!(
        swept[0].1, swept[1].1,
        "the two arms removed {} and {} records from identically-shaped logs; the payload's last \
         byte is still deciding the sweep",
        swept[0].1, swept[1].1
    );
    eprintln!(
        "RECLAIM FRAME both arms removed {} records ({} B and {} B freed)",
        swept[0].1, swept[0].2, swept[1].2
    );
}

/// The rule itself, at the level it lives at, in both directions.
///
/// A text record's delimiter comes off; a frame's last byte -- `0x0A` or not -- does not.
/// rust-internal: reads the log framing layer directly, no product behaviour
#[test]
fn only_a_delimited_record_gives_up_its_trailing_byte() {
    let framed_ending_in_delimiter = crate::log_framing::encode_frame(b"payload ending in \n");
    assert_eq!(
        crate::log_framing::record_body(&framed_ending_in_delimiter),
        framed_ending_in_delimiter.as_slice(),
        "a binary frame declares its own length, so nothing may come off the end of it"
    );
    assert!(
        crate::log_framing::decode_line(crate::log_framing::record_body(
            &framed_ending_in_delimiter
        ))
        .is_ok(),
        "a frame whose payload ends in 0x0A must decode after passing through the walk's trimmer"
    );

    // The negative control: the same bytes with the delimiter actually taken off are exactly the
    // fragment the defect produced, and they must still be refused.
    let damaged = &framed_ending_in_delimiter[..framed_ending_in_delimiter.len() - 1];
    assert!(
        crate::log_framing::decode_line(damaged).is_err(),
        "a frame one byte short of its declared length is a torn frame and must be refused; if \
         this passes, the decode stopped checking the length and the guard above means nothing"
    );

    let delimited = b"#tsf2 2 00000000 {}\n";
    assert_eq!(
        crate::log_framing::record_body(delimited),
        &delimited[..delimited.len() - 1],
        "a text record's trailing delimiter still comes off"
    );
}

/// The refusal term is now sayable, which is the part that does not depend on the trigger.
///
/// An errored sweep and a sweep with nothing to do both report `(0, 0, 0)` for records, bytes
/// before and bytes after, because the `Result` is dropped with `.ok()` and every field read
/// through `unwrap_or_default()`. `wal_sweep_failed` is what tells them apart. Eleven refusal
/// terms behind one number is a report that cannot be tested; this one names which fired.
/// rust-internal: reads the engine's own maintenance entry point, no product behaviour
#[test]
fn a_failed_sweep_and_an_empty_one_no_longer_report_the_same_thing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = release_engine(dir.path());
    let report = engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the dump did not complete, so there is no report to read");

    eprintln!(
        "RECLAIM TERM empty shard: wal_sweep_failed={} index_log_sweep_failed={} \
         wal_records_removed={} wal_bytes_before={} wal_bytes_after={}",
        report.wal_sweep_failed,
        report.index_log_sweep_failed,
        report.wal_records_removed,
        report.wal_bytes_before,
        report.wal_bytes_after,
    );

    assert!(
        !report.wal_sweep_failed,
        "an untouched shard's sweep reports a failure; the term is reading something other than \
         the sweep's own outcome"
    );
    assert!(
        !report.index_log_sweep_failed,
        "an untouched shard's index-log sweep reports a failure"
    );
    // The zeros this arm reports are honest, and the term is what says so.
    assert_eq!(
        (report.wal_records_removed, report.wal_bytes_before),
        (0, 0),
        "APPARATUS: this arm is meant to be the nothing-to-do case"
    );
}

// ---------------------------------------------------------------------------------------------
// CAN IT KEEP UP, AND IS ANYTHING BOUNDED PER CYCLE
// ---------------------------------------------------------------------------------------------

/// Rounds of "write, then reclaim" at two corpus sizes, on the half that now frees something.
///
/// The question a bound has to answer is not whether it frees bytes but whether it frees them
/// faster than they accrue. Each round writes `DELTA` batches and then sweeps, and the test
/// records what accrued against what was released.
///
/// TWELVE ROUNDS, and twelve rounds cannot establish an asymptote -- that limit is stated here
/// rather than argued past. What twelve do establish is that the release is not a start-up
/// transient and that the log returns to a floor every round rather than ratcheting.
///
/// rust-internal: drives the write-ahead log's own reclaim, no product behaviour
#[test]
fn reclaim_frees_more_than_accrues_at_both_corpus_sizes() {
    const ROUNDS: usize = 12;
    const DELTA: usize = 8;
    const ITEMS: usize = 64;
    let mut shape = Vec::new();

    for corpus in [64usize, 640] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalWriteAheadLogStore::new(dir.path());
        seed_batches(&store, corpus, ITEMS);

        // FIXTURE. A stage with nothing to do reports zero, which reads exactly like a stage that
        // refused, so the log has to be shown to hold records before any zero below means
        // anything.
        let seeded_bytes = log_bytes(dir.path());
        let seeded_sequence = store.stats(SHARD).last_sequence;
        assert!(
            seeded_sequence >= corpus as u64 && seeded_bytes > 0,
            "APPARATUS corpus={corpus}: the log reached sequence {seeded_sequence} in \
             {seeded_bytes} B after {corpus} batches, so the sweep below has less to do than this \
             arm claims"
        );
        eprintln!(
            "RECLAIM ROUND corpus={corpus}: seeded to sequence {seeded_sequence} across {} pieces, \
             {seeded_bytes} B",
            log_pieces(dir.path()).len(),
        );

        let mut accrued_total = 0u64;
        let mut released_total = 0u64;
        let mut floors = Vec::new();
        // The log's length after the previous round's sweep. What the writer adds on top of it
        // is what accrued, and it is measured from the file rather than from anything the sweep
        // reports -- a sweep that miscounts must not also supply the figure that checks it.
        let mut settled = log_bytes(dir.path());
        for round in 0..ROUNDS {
            seed_batches(&store, DELTA, ITEMS);
            let grown = log_bytes(dir.path());
            let accrued = grown.saturating_sub(settled);

            // Retain only the newest DELTA records, so every round has the same work to do. The
            // retain point is a SEQUENCE, which keeps ascending across rounds; the frame count
            // does not, because each sweep rewrites the piece.
            let last_sequence = store.stats(SHARD).last_sequence;
            let report = store
                .gc_before_sequence_unchecked(
                    SHARD,
                    last_sequence.saturating_sub(DELTA as u64).saturating_add(1),
                )
                .expect("the sweep runs");
            // Released and floor from the FILESYSTEM, not from the report: whole pieces the sweep
            // unlinks never appear in `bytes_before`/`bytes_after`, and this is also the residual
            // that keeps the sweep from marking its own work.
            let floor = log_bytes(dir.path());
            let released = grown.saturating_sub(floor);
            settled = floor;
            accrued_total += accrued;
            released_total += released;
            floors.push(floor);

            if round < 3 || round == ROUNDS - 1 {
                eprintln!(
                    "RECLAIM ROUND corpus={corpus} round={round} last_sequence={last_sequence} \
                     accrued={accrued} B released={released} B floor={floor} B removed={} records \
                     copied={} B pieces_dropped={} ({} B)",
                    report.records_removed,
                    report.bytes_copied,
                    report.dropped_segments,
                    report.dropped_segment_bytes,
                );
            }
        }

        // The bound is REACHABLE: the log comes back to the same floor every round instead of
        // ratcheting upward. Asserted on the last three rather than on all twelve, because the
        // first rounds still carry the seeded corpus down.
        // Within a band rather than to the byte: a record carries a varint timestamp, so the last
        // digits of these counts move between rounds and the claim does not rest on them. What is
        // asserted is that the floor does not RATCHET -- the spread stays under a percent, and
        // round eleven does not sit above round two.
        let tail = &floors[floors.len() - 3..];
        let spread = tail.iter().max().unwrap() - tail.iter().min().unwrap();
        let mean = tail.iter().sum::<u64>() / tail.len() as u64;
        assert!(
            spread * 100 <= mean,
            "corpus={corpus}: the log settled at {tail:?} over the last three rounds -- a spread \
             of {spread} B against a mean of {mean} B, so the reclaim is not returning it to a \
             floor"
        );
        assert!(
            *floors.last().unwrap() * 10 <= floors[2] * 11,
            "corpus={corpus}: the floor moved from {} B at round 2 to {} B at round {}, so it is \
             ratcheting rather than settling",
            floors[2],
            floors.last().unwrap(),
            ROUNDS - 1
        );
        assert!(
            released_total >= accrued_total,
            "corpus={corpus}: {ROUNDS} rounds released {released_total} B against \
             {accrued_total} B accrued, so the sweep does not keep up with the writer"
        );
        eprintln!(
            "RECLAIM ROUND corpus={corpus}: {ROUNDS} rounds, accrued {accrued_total} B, released \
             {released_total} B, settled floor {} B",
            floors[floors.len() - 1]
        );
        shape.push((corpus, floors[floors.len() - 1], released_total / ROUNDS as u64));
    }

    // FLAT or GROWING, both quantities, with the ratio over a corpus factor of 10.
    let (small_corpus, small_floor, small_release) = shape[0];
    let (large_corpus, large_floor, large_release) = shape[1];
    eprintln!(
        "RECLAIM ROUND corpus {small_corpus} -> {large_corpus} (10x): settled floor {small_floor} \
         -> {large_floor} B, mean release per round {small_release} -> {large_release} B",
    );
    let floor_ratio = large_floor as f64 / small_floor.max(1) as f64;
    assert!(
        (0.95..1.05).contains(&floor_ratio),
        "the floor the log settles at is {small_floor} B at {small_corpus} batches and \
         {large_floor} B at {large_corpus} -- {floor_ratio:.4}x over a corpus factor of 10. The \
         floor is what the sweep RETAINS, which is the same DELTA records either way, so it is \
         expected FLAT in the corpus"
    );
    // The release per round is priced by the round's own work, not by the store: the same DELTA
    // records cost the same to free at ten times the corpus. Within 5%, because the last digits
    // of a byte count move between runs.
    let ratio = large_release as f64 / small_release.max(1) as f64;
    assert!(
        (0.95..1.05).contains(&ratio),
        "the mean release per round moved by {ratio:.4}x over a corpus factor of 10, so it is \
         priced by the store rather than by the round"
    );
    eprintln!("RECLAIM ROUND release per round is FLAT in the corpus: ratio {ratio:.4}x");
}

/// What bounds a single pass, named. The answer is NOT "nothing".
///
/// Two mechanisms, both real, both in `wal.rs`:
///
/// * `TS_WAL_RECLAIM_MAX_SEGMENTS_PER_PASS` (default 1,024) caps how many whole sealed pieces
///   one pass unlinks, explicitly so the lock every append needs is not held for a whole
///   backlog. Pieces go from the front in order, so stopping early leaves the rest exactly where
///   the next pass looks.
/// * `reclaim_is_worth_rewriting` declines the rewrite entirely when it would copy more than
///   `TS_WAL_RECLAIM_MIN_COPY_BYTES` (default 64 MiB) to free less than
///   `TS_WAL_RECLAIM_MIN_FREED_PERCENT` (default 25%) of what it copies, reporting
///   `skipped_not_worth_rewrite`.
///
/// The second is a WORTH gate rather than a bound -- it decides whether a pass runs, not how
/// much it does -- and the active piece's rewrite itself is unbounded: it copies every retained
/// byte. So one pass is bounded in the pieces it unlinks and unbounded in the bytes it copies.
/// This test holds the observable half of that: the refusal terms are separately named, so a
/// caller can say which one fired.
///
/// rust-internal: reads the write-ahead log's own reclaim report, no product behaviour
#[test]
fn a_pass_names_which_refusal_term_fired() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalWriteAheadLogStore::new(dir.path());
    seed_batches(&store, 8, 64);

    // A sweep that does its work names no refusal.
    let worked = store
        .gc_before_sequence_unchecked(SHARD, 4)
        .expect("the sweep runs");
    assert!(worked.records_removed > 0, "APPARATUS: the sweep had no work");
    assert!(!worked.skipped_not_worth_rewrite);
    assert!(!worked.clamped_by_block_retention);
    assert!(!worked.clamped_by_durable_index);

    // The block-retention floor, driven on its own. It is the term that stops a reclaim removing
    // the only copy of a page the served index still points at.
    store.set_block_retention_floor(SHARD, 1);
    let clamped = store
        .gc_before_sequence_unchecked(SHARD, 8)
        .expect("the sweep runs");
    eprintln!(
        "RECLAIM TERM block-retention floor: clamped={} removed={} effective_retain={}",
        clamped.clamped_by_block_retention,
        clamped.records_removed,
        clamped.effective_retain_from_sequence,
    );
    assert!(
        clamped.clamped_by_block_retention,
        "a retain point of 8 above a floor of 1 did not report the clamp, so the term that says \
         WHY a pass kept records is not being set"
    );
    assert_eq!(
        clamped.records_removed, 0,
        "the floor is at sequence 1, so nothing above it may go"
    );

    // And the durable-index clamp, which is the anchored wrapper's own term.
    store.clear_block_retention_floor(SHARD);
    let anchor = crate::wal::DurableIndexAnchor::proven_durable_through(SHARD, 2);
    let anchored = store
        .gc_before_sequence(SHARD, 9, &anchor)
        .expect("the sweep runs");
    eprintln!(
        "RECLAIM TERM durable-index anchor: clamped={} removed={} retain_asked={} effective={}",
        anchored.clamped_by_durable_index,
        anchored.records_removed,
        anchored.retain_from_sequence,
        anchored.effective_retain_from_sequence,
    );
    assert!(
        anchored.clamped_by_durable_index,
        "a retain point past what the anchor proves did not report the clamp"
    );
    assert_eq!(
        anchored.retain_from_sequence, 9,
        "the report must carry the sequence the caller ASKED for, so a narrowed reclaim is \
         visible as narrowed rather than as a smaller request that succeeded exactly"
    );
}

/// THE POSITIVE ARM. A sweep that genuinely fails must SAY so.
///
/// The test above shows the term is not stuck on; this one shows it is not stuck off. Without
/// both, `wal_sweep_failed: false` hard-coded would pass everything -- a term that can only ever
/// be observed in its quiet state is not a term.
///
/// The failure is manufactured by damaging a record's payload after it was written, which is
/// what `next_frame` is there to catch: a complete frame whose digest does not match its payload
/// is committed corruption and returns `Err`, and that `Err` is what the caller drops with
/// `.ok()`.
///
/// rust-internal: reads the engine's own maintenance entry point, no product behaviour
#[test]
fn a_sweep_that_fails_says_so_instead_of_reporting_three_zeros() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = release_engine(dir.path());
    for index in 0..400usize {
        let _ = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("k-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
    }

    let piece = find_wal_piece(dir.path()).expect("APPARATUS: the engine wrote no log piece");
    let frames = frames_of(&piece);
    assert!(
        frames.len() >= 8,
        "APPARATUS: the log holds {} frames, so there is nothing for a sweep to do",
        frames.len()
    );

    // FAIL THE SWEEP AND NOTHING ELSE.
    //
    // Damaging the log's BYTES was tried first and is the wrong lever: the dump's step 1 flushes
    // the write-ahead log before it materializes anything, that flush reads the log end to end,
    // and a damaged record stops the DUMP rather than the sweep -- measured, `flush` and the
    // sweep both refused and the dump answered `None`, so there was no report to read at all.
    // (That the dump refuses a log it cannot read is the right behaviour; it is simply not this
    // test's subject.)
    //
    // So block the one file only the sweep writes. The reclaim rewrites survivors into
    // `<piece>.jsonl.tmp` and renames it into place; a DIRECTORY sitting on that name fails the
    // `File::create` and nothing before it. The walk runs, the decision is made, and the sweep
    // then returns `Err` at the moment it tries to commit -- which is exactly the shape the
    // caller's `.ok()` discards.
    let blocked = piece.with_extension("jsonl.tmp");
    std::fs::create_dir(&blocked).expect("the blocker is creatable");
    eprintln!(
        "RECLAIM TERM blocked the sweep's temp path {} ({} frames in the log)",
        blocked.display(),
        frames.len()
    );

    // APPARATUS, both halves: the flush the dump depends on must still SUCCEED, and the sweep
    // must actually FAIL. Without the first the dump answers None; without the second this arm
    // is measuring a sweep that worked.
    assert!(
        engine.write_ahead_log_store().flush(SHARD).is_ok(),
        "APPARATUS: the flush the dump takes before anything else failed, so a None below would \
         be the dump bailing rather than the sweep refusing"
    );

    let report = engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the dump itself did not complete, so there is no report to read");

    eprintln!(
        "RECLAIM TERM refused sweep: wal_sweep_failed={} wal_records_removed={} \
         wal_bytes_before={} wal_bytes_after={}",
        report.wal_sweep_failed,
        report.wal_records_removed,
        report.wal_bytes_before,
        report.wal_bytes_after,
    );

    assert!(
        report.wal_sweep_failed,
        "the sweep refused a damaged log and the report says it did not fail. Its three release \
         fields are {} / {} / {} -- exactly what an empty log reports, which is the rendering \
         this term exists to break",
        report.wal_records_removed, report.wal_bytes_before, report.wal_bytes_after
    );
    // And the reason the term is needed at all: the three numbers really are the empty-log ones.
    assert_eq!(
        (
            report.wal_records_removed,
            report.wal_bytes_before,
            report.wal_bytes_after
        ),
        (0, 0, 0),
        "APPARATUS: a failed sweep no longer reports defaults, so the term is guarding a \
         rendering that has stopped happening -- check `.ok()` / `unwrap_or_default()` in \
         `dump_and_reclaim_index_logs_with_min_reclaimable`"
    );
}

/// The shard's active log piece, wherever the engine put it.
fn find_wal_piece(root: &Path) -> Option<std::path::PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("shard-1.wal") && !name.ends_with(".lock"))
                .unwrap_or(false)
            {
                return Some(path);
            }
        }
    }
    None
}

/// THE INSTRUMENT, CONTROLLED. A blind instrument reads zero exactly like an exact one does.
///
/// `log_bytes` is what the rounds test measures accrual and release with, and it is independent
/// of the sweep only if it is actually reading the filesystem. So plant a known number of bytes
/// where it has to see them and require it back EXACTLY -- not "more than before", which a
/// stuck-open instrument would also satisfy.
///
/// The residual beside it is the other direction: bytes written where the walk cannot see them
/// must NOT be counted. `/proc/thread-self/io` is the kernel's own figure for this thread --
/// per-thread and not `/proc/self/io`, which is process-wide and has charged a measured span
/// 3,194 bytes a background thread wrote under a full-suite run.
///
/// rust-internal: measures the test's own byte instrument, no product behaviour
#[test]
fn the_byte_instrument_recovers_exactly_what_was_planted() {
    const PLANTED: u64 = 4_096;
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalWriteAheadLogStore::new(dir.path());
    seed_batches(&store, 4, 16);

    let before = log_bytes(dir.path());
    assert!(
        before > 0,
        "APPARATUS: the log measured 0 B before anything was planted, so a recovery of exactly \
         {PLANTED} below would be an instrument reading one file rather than the log"
    );

    let thread_io_before = thread_bytes_written();
    // Planted INSIDE the log directory, where the walk must see it.
    std::fs::write(dir.path().join("planted.bin"), vec![0u8; PLANTED as usize])
        .expect("the marker is writable");
    let after = log_bytes(dir.path());
    let thread_io_after = thread_bytes_written();

    let recovered = after.saturating_sub(before);
    eprintln!(
        "RECLAIM INSTRUMENT planted {PLANTED} B, walk recovered {recovered} B \
         (log {before} -> {after} B); this thread's kernel wchar moved {} B",
        thread_io_after.saturating_sub(thread_io_before)
    );
    assert_eq!(
        recovered, PLANTED,
        "the walk recovered {recovered} B of a planted {PLANTED} B. An instrument that cannot \
         recover a known write cannot be trusted when it reports a zero"
    );

    // And the negative direction: a file planted OUTSIDE the log directory must not be counted,
    // or the instrument is measuring the machine rather than the log.
    let elsewhere = tempfile::tempdir().expect("tempdir");
    std::fs::write(elsewhere.path().join("unrelated.bin"), vec![0u8; PLANTED as usize])
        .expect("the decoy is writable");
    assert_eq!(
        log_bytes(dir.path()),
        after,
        "a file written outside the log directory changed the log's measured size, so the \
         instrument is not bounded by what it claims to measure"
    );

    // The kernel's own figure has to have moved at all, or it is an identity rather than a
    // reading -- a residual computed against a counter that never moves is always 0.
    assert!(
        thread_io_after > thread_io_before,
        "this thread's kernel wchar did not move across two {PLANTED} B writes ({} -> {}), so it \
         is not being read from where this thread's writes are counted",
        thread_io_before,
        thread_io_after
    );
}

/// Bytes THIS THREAD has written, per the kernel.
///
/// `/proc/thread-self/io`, never `/proc/self/io`: the process-wide counter includes every other
/// thread in the test binary, and under a full-suite run that has charged a measured span bytes
/// a background thread an earlier test left running wrote.
fn thread_bytes_written() -> u64 {
    std::fs::read_to_string("/proc/thread-self/io")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("wchar: ")?.trim().parse::<u64>().ok())
        })
        .unwrap_or(0)
}
