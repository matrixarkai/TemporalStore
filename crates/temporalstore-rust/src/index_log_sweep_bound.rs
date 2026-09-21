// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT AN INDEX-LOG SWEEP'S PER-ROUND ENTRY BUDGET ACTUALLY BOUNDS.
//!
//! Two production sweeps walk this log and only one of them takes a per-round entry budget:
//!
//! - [`LocalIndexLogStore::gc_before_sequence_limited`] takes `max_entries_per_round`, and the
//!   storage-manager cycle passes 256 (`StorageManagerCycleRequest::default`);
//! - [`LocalIndexLogStore::gc_reflected_before_anchor`] takes no such parameter at all and
//!   constructs its report with `max_entries_per_round: 0`.
//!
//! That asymmetry reads as an oversight. It is not, and the numbers below are why.
//!
//! WHAT `0` MEANS, READ FROM THE CONSUMER. The only code that consumes the quantity is the
//! record loop in `gc_before_sequence_limited`, which guards it as `max_entries_per_round > 0 &&
//! removed_this_round >= max_entries_per_round`, and the `budget_exhausted` term, guarded the
//! same way. So `0` in the PARAMETER means "no budget". In `gc_reflected_before_anchor` there is
//! no parameter and no consumer: the `0` is a REPORT FIELD, and its value is already
//! `IndexLogGcReport::default()`'s. It reports honestly that the sweep applied no entry budget.
//!
//! AND AN ENTRY BUDGET IS NOT A BOUND ON WORK. When the budget is reached the record is pushed
//! onto `retained` -- the loop does not break. So the budget:
//!
//! - does NOT bound what the sweep READS (both loops read the piece being written to its end);
//! - does NOT bound how long the sweep holds `inner`, which is the lock `append_json` takes;
//! - does bound what the sweep REMOVES, and therefore makes the rewrite COPY MORE, not less.
//!
//! That is the inversion the note on the parameter already describes at 40,000 records in
//! milliseconds. [`an_entry_budget_copies_more_and_reads_exactly_as_much`] states it in counts.
//!
//! WHAT DOES BOUND EITHER SWEEP is the rolling threshold. Whole sealed pieces are unlinked by
//! name without being opened, so the walk and the rewrite see only the piece being written --
//! at most `TS_INDEX_LOG_SEGMENT_BYTES`, 64 KiB by default. That is a bound on what the sweep
//! READS, which the campaign's survey found the engine has exactly one of.
//!
//! Counted, never timed: the box these run on carries other tenants.

use super::*;
use std::collections::BTreeSet;

const SHARD: ShardId = 11;
/// The two corpus sizes. Ten times apart, and the large one must roll into many pieces or every
/// per-piece claim below is vacuous.
const SMALL: usize = 2_000;
const LARGE: usize = 20_000;

/// Set the rolling threshold for this thread and put it back on drop, panic included.
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

/// Put the removal probe back however the test leaves.
struct ArmedRemovals;
impl Drop for ArmedRemovals {
    fn drop(&mut self) {
        probe::disarm_removals();
    }
}
fn arm_removals() -> ArmedRemovals {
    probe::arm_removals();
    ArmedRemovals
}

fn item(key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Page,
        routing_bucket: 0,
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

/// `records` delta records, every one anchored at WAL sequence 1, into a fresh store.
///
/// Anchored so the post-dump sweep finds every one of them reflected by a base at anchor 1:
/// the two sweeps are then being asked to remove the SAME set, which is what makes their costs
/// comparable at all.
fn seed(dir: &std::path::Path, records: usize) -> LocalIndexLogStore {
    let store = LocalIndexLogStore::new(dir);
    for value in 0..records {
        store
            .append_delta(
                SHARD,
                // Zero-padded: an unpadded key is one character longer at 20,000 records than at
                // 2,000 and would put fixture bytes into a number measuring the code.
                vec![item(&format!("tenant/1/object/{value:08}"))],
                Vec::new(),
                Some(1),
                None,
                false,
                false,
            )
            .unwrap();
    }
    store
}

/// What one sweep cost, read from the probes rather than from the report it is auditing.
struct SweepCost {
    records: usize,
    pieces_before: usize,
    frames_read: u64,
    frames_copied: u64,
    dir_listings: u64,
    piece_paths: u64,
    records_removed: usize,
    dropped_segments: usize,
}

impl SweepCost {
    /// Frames the sweep read per record OF STORE. This is the denominator that has hidden every
    /// defect of this shape -- it falls as the corpus grows whatever the sweep does.
    fn read_per_record_of_store(&self) -> f64 {
        self.frames_read as f64 / self.records as f64
    }
    /// Frames the sweep read per record OF WORK -- per record it actually removed. This one does
    /// not fall for free.
    fn read_per_record_of_work(&self) -> f64 {
        if self.records_removed == 0 {
            f64::INFINITY
        } else {
            self.frames_read as f64 / self.records_removed as f64
        }
    }
    fn piece_paths_per_record_of_store(&self) -> f64 {
        self.piece_paths as f64 / self.records as f64
    }
}

fn show(tag: &str, cost: &SweepCost) {
    println!(
        "  {tag:<28} records {:>7} | pieces {:>5} | frames read {:>6} | frames copied {:>6} | \
         dir listings {:>4} | piece paths {:>6} | removed {:>7} | pieces unlinked {:>5} | \
         read/record-of-store {:>8.5} | read/record-of-work {:>8.4} | piece-paths/record {:>8.5}",
        cost.records,
        cost.pieces_before,
        cost.frames_read,
        cost.frames_copied,
        cost.dir_listings,
        cost.piece_paths,
        cost.records_removed,
        cost.dropped_segments,
        cost.read_per_record_of_store(),
        cost.read_per_record_of_work(),
        cost.piece_paths_per_record_of_store(),
    );
}

/// Seed `records` and run the post-dump sweep once, counting what it cost.
fn measure_post_dump(dir: &std::path::Path, records: usize) -> SweepCost {
    let store = seed(dir, records);
    let pieces_before = store.piece_count(SHARD);
    probe::reset();
    let report = store
        .gc_reflected_before_anchor(SHARD, 1, records as u64 + 1, 0)
        .unwrap();
    SweepCost {
        records,
        pieces_before,
        frames_read: probe::sweep_frames_read(),
        frames_copied: probe::sweep_frames_copied(),
        dir_listings: probe::dir_listings(),
        piece_paths: probe::piece_paths(),
        records_removed: report.records_removed,
        dropped_segments: report.dropped_segments,
    }
}

/// WHAT THE UNBOUNDED SWEEP COSTS, AT TWO CORPUS SIZES, UNDER BOTH DENOMINATORS.
///
/// The sweep with no entry budget is NOT unbounded in the corpus. What it reads is the piece
/// being written, which the rolling threshold caps; the sealed pieces below the floor go by name
/// and are never opened. So frames read is FLAT at ten times the records, and read per record of
/// STORE falls by ten -- which is exactly the shape that would let an unbounded walk hide, so the
/// per record of WORK column is asserted beside it and is ALSO flat.
///
/// The term that does grow is the piece enumeration: ten times the pieces, ten times the paths
/// each listing hands back. That is the real per-round cost of a large store here, and an entry
/// budget does not touch it either.
#[test]
fn what_the_unbounded_post_dump_sweep_costs_at_two_corpus_sizes() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    let small_dir = tempfile::tempdir().unwrap();
    let large_dir = tempfile::tempdir().unwrap();
    let small = measure_post_dump(small_dir.path(), SMALL);
    let large = measure_post_dump(large_dir.path(), LARGE);

    println!("post-dump sweep (no entry budget), rolling at {DEFAULT_INDEX_LOG_SEGMENT_BYTES} B:");
    show("small", &small);
    show("large", &large);

    // THE DENOMINATOR, FIRST. Every claim below is about a log in many pieces; a fixture that
    // never rolled would report a flat per-piece cost for the uninteresting reason.
    assert!(
        large.pieces_before >= 10 && large.pieces_before >= small.pieces_before * 5,
        "the large fixture did not roll into many more pieces than the small one: {} against {}",
        large.pieces_before,
        small.pieces_before
    );
    assert!(
        small.records_removed > 0 && large.records_removed > 0,
        "a sweep removed nothing, so every cost below is a cost of doing nothing: {} and {}",
        small.records_removed,
        large.records_removed
    );
    assert!(
        small.dropped_segments > 0 && large.dropped_segments > 0,
        "a sweep unlinked no whole piece ({} and {}), so the walk was never the minority of it",
        small.dropped_segments,
        large.dropped_segments
    );

    // WHAT IT READS IS CAPPED BY THE PIECE, NOT BY THE CORPUS. Ten times the records, and the
    // walk reads no more frames -- within one piece's worth either way.
    assert!(
        large.frames_read <= small.frames_read.saturating_mul(2).saturating_add(64),
        "frames read grew with the corpus: {} at {} records against {} at {} records -- the walk \
         is reading sealed pieces again",
        large.frames_read,
        large.records,
        small.frames_read,
        small.records,
    );
    // And it is genuinely a small fraction of the store at the LARGE size, which is the claim
    // that would fail if the roll stopped working.
    assert!(
        large.frames_read * 10 < large.records as u64,
        "the walk read {} of {} records -- that is not a piece, that is the log",
        large.frames_read,
        large.records
    );

    // PER RECORD OF WORK, which the store denominator would have hidden. Flat, so the sweep is
    // not paying more per record removed as the store grows.
    assert!(
        large.read_per_record_of_work() <= small.read_per_record_of_work() * 2.0 + 1.0,
        "frames read per record REMOVED grew with the corpus: {:.4} against {:.4}",
        large.read_per_record_of_work(),
        small.read_per_record_of_work(),
    );

    // THE TERM THAT DOES GROW. Piece paths enumerated is linear in the pieces, and an entry
    // budget bounds none of it: the enumeration happens before the record loop is reached.
    assert!(
        large.piece_paths > small.piece_paths * 3,
        "piece paths did not grow with the piece count: {} against {} for {} against {} pieces -- \
         the per-piece term this measurement exists to name has gone missing",
        large.piece_paths,
        small.piece_paths,
        large.pieces_before,
        small.pieces_before,
    );
    // Flat in LISTINGS though: the cost is per piece handed back, not per listing taken.
    assert!(
        large.dir_listings <= small.dir_listings + 2,
        "directory listings grew with the corpus: {} against {}",
        large.dir_listings,
        small.dir_listings
    );
}

/// AN ENTRY BUDGET READS EXACTLY AS MUCH AND COPIES MORE.
///
/// The same log, swept twice: once with no budget and once with the storage-manager cycle's
/// shipped 256. The budgeted round reads the identical number of frames -- the loop has no
/// break -- removes fewer, and therefore RETAINS more, and every retained record is re-framed
/// and written out. So the budget buys nothing on the read, nothing on the lock hold, and costs
/// strictly more on the write.
///
/// THE CONTROL ARM IS A THIRD ROUND whose removable count is BELOW the budget, where the budget
/// cannot bite. It must come back identical to the unbudgeted round on every column. If that arm
/// ever goes flat with the others -- if all three rounds agree -- the fixture has stopped
/// reaching the budget at all and the comparison above is vacuous.
#[test]
fn an_entry_budget_copies_more_and_reads_exactly_as_much() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    // Rolling OFF for the subject arms: this test is about the record loop, and a log in pieces
    // would have the budget compete with the piece unlinking for the same records. One piece
    // makes the loop the whole of the round.
    let _one_piece = roll_at(0);
    const BUDGET: usize = 256;
    const RECORDS: usize = 2_000;
    // Every record below this floor is removable, so the removable count is 1,500 -- comfortably
    // above the 256 the budget is. CHECKED below rather than assumed: a fixture shrunk to afford
    // more mutants would make the threshold under test inert.
    const RETAIN_FROM: u64 = 1_501;

    let mut arms = Vec::new();
    for (label, budget, retain_from) in [
        ("no budget", 0usize, RETAIN_FROM),
        ("budget 256", BUDGET, RETAIN_FROM),
        // The control: only 100 records are removable, which no budget of 256 can reach.
        ("control, under budget", BUDGET, 101),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = seed(dir.path(), RECORDS);
        let pieces_before = store.piece_count(SHARD);
        probe::reset();
        let report = store
            .gc_before_sequence_limited(SHARD, retain_from, budget)
            .unwrap();
        let cost = SweepCost {
            records: RECORDS,
            pieces_before,
            frames_read: probe::sweep_frames_read(),
            frames_copied: probe::sweep_frames_copied(),
            dir_listings: probe::dir_listings(),
            piece_paths: probe::piece_paths(),
            records_removed: report.records_removed,
            dropped_segments: report.dropped_segments,
        };
        show(label, &cost);
        arms.push((label, budget, report, cost));
    }

    let (_, _, unbudgeted_report, unbudgeted) = &arms[0];
    let (_, _, budgeted_report, budgeted) = &arms[1];
    let (_, _, control_report, control) = &arms[2];

    // THE FIXTURE REACHES THE THRESHOLD UNDER TEST. Without this the whole comparison is
    // "256 is bigger than everything here", which is not what it claims to measure.
    assert!(
        unbudgeted_report.removable_records_before_budget > BUDGET * 4,
        "the fixture offers {} removable records against a budget of {BUDGET}; the budget cannot \
         bite and nothing below means anything",
        unbudgeted_report.removable_records_before_budget
    );
    assert_eq!(
        pieces_are_one(unbudgeted, budgeted, control),
        true,
        "an arm rolled into pieces ({}, {}, {}); the budget is then competing with the unlinking",
        unbudgeted.pieces_before,
        budgeted.pieces_before,
        control.pieces_before
    );

    // READ: IDENTICAL. Not "similar" -- the loop reads every frame in the piece either way, so
    // the two counts are the same integer.
    assert_eq!(
        budgeted.frames_read, unbudgeted.frames_read,
        "a budget of {BUDGET} changed what the sweep READ: {} frames against {} -- if this ever \
         becomes true the loop has learnt to break and the claim here is stale",
        budgeted.frames_read, unbudgeted.frames_read,
    );

    // REMOVES: FEWER, and exactly the budget.
    assert_eq!(
        budgeted_report.records_removed, BUDGET,
        "the budgeted round removed {} records, not the {BUDGET} it was budgeted",
        budgeted_report.records_removed
    );
    assert!(
        unbudgeted_report.records_removed > budgeted_report.records_removed,
        "the unbudgeted round removed {} and the budgeted one {}; the budget did nothing",
        unbudgeted_report.records_removed,
        budgeted_report.records_removed
    );
    assert!(
        budgeted_report.budget_exhausted,
        "the budgeted round did not report its budget exhausted, so it never reached it"
    );

    // COPIES: STRICTLY MORE. This is the whole finding. Asserted as a strict inequality on
    // counts and echoed on bytes, which is what the operator pays.
    assert!(
        budgeted.frames_copied > unbudgeted.frames_copied,
        "the budgeted round copied {} frames and the unbudgeted one {}; the inversion this test \
         exists to name has gone",
        budgeted.frames_copied,
        unbudgeted.frames_copied
    );
    assert!(
        budgeted_report.bytes_copied > unbudgeted_report.bytes_copied,
        "the budgeted round copied {} B and the unbudgeted one {} B",
        budgeted_report.bytes_copied,
        unbudgeted_report.bytes_copied
    );
    println!(
        "  INVERSION: budget {BUDGET} removed {} records ({} fewer) and copied {} B \
         ({} B MORE) than no budget at all, having read the same {} frames",
        budgeted_report.records_removed,
        unbudgeted_report
            .records_removed
            .saturating_sub(budgeted_report.records_removed),
        budgeted_report.bytes_copied,
        budgeted_report
            .bytes_copied
            .saturating_sub(unbudgeted_report.bytes_copied),
        budgeted.frames_read,
    );

    // THE CONTROL ARM, asserted by name with its own failure message. Below the budget the
    // budgeted call and the unbudgeted one must be the same round -- and it must NOT report its
    // budget exhausted, which is what says the arm is really below it.
    assert!(
        !control_report.budget_exhausted,
        "the control arm exhausted its budget, so it is not a control: it offered {} removable \
         records against {BUDGET}",
        control_report.removable_records_before_budget
    );
    assert!(
        control_report.removable_records_before_budget > 0,
        "the control arm had nothing to remove, so it agrees with everything and guards nothing"
    );
    assert_eq!(
        control_report.records_removed, control_report.removable_records_before_budget,
        "the control arm removed {} of {} removable records -- a budget it cannot reach took \
         something anyway",
        control_report.records_removed, control_report.removable_records_before_budget,
    );
    // And the arm must not be flat with the subject: if the control's numbers ever match the
    // budgeted arm's, the fixture stopped separating them.
    assert!(
        control_report.records_removed != budgeted_report.records_removed,
        "the control arm and the budgeted arm removed the same {} records; the arms have gone \
         flat and this test can no longer fail for the right reason",
        control_report.records_removed
    );
}

fn pieces_are_one(a: &SweepCost, b: &SweepCost, c: &SweepCost) -> bool {
    a.pieces_before == 1 && b.pieces_before == 1 && c.pieces_before == 1
}

/// THE SWEEP REMOVES EXACTLY THE REFLECTED SET, ELEMENT BY ELEMENT.
///
/// A count cannot see a sweep that removes the right NUMBER of the wrong records, and removing
/// the wrong record here is silent data loss: a delta carries an eviction that lives nowhere
/// else. So the removal sequence the sweep actually took is compared, sequence by sequence,
/// against a control the test computes itself by decoding the log before the sweep and applying
/// the retention rule independently.
///
/// The control is built from the FILE, not from the report being audited.
#[test]
fn the_post_dump_sweep_removes_exactly_the_set_a_control_computes() {
    let _one_piece = roll_at(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());

    // A mixture, so the predicate has something to decide: records the base reflects, records
    // above its anchor, and an anchor-less record that CARRIES CONTENT and must survive.
    let mut expected_removed = BTreeSet::new();
    let mut sequence = 0u64;
    for value in 0..120usize {
        let anchor = match value % 3 {
            0 => Some(1u64),  // reflected by a base at anchor 1
            1 => Some(9u64),  // above it, must survive
            _ => None,        // anchor-less WITH content, must survive
        };
        sequence = store
            .append_delta(
                SHARD,
                vec![item(&format!("tenant/1/object/{value:08}"))],
                Vec::new(),
                anchor,
                None,
                false,
                false,
            )
            .unwrap();
        if anchor == Some(1) {
            expected_removed.insert(sequence);
        }
    }
    let meta_sequence = sequence + 1;

    let _armed = arm_removals();
    probe::reset();
    probe::arm_removals();
    let report = store
        .gc_reflected_before_anchor(SHARD, 1, meta_sequence, 0)
        .unwrap();
    let actual_removed = probe::removals()
        .into_iter()
        .map(|(_, seq)| seq)
        .collect::<BTreeSet<_>>();

    // DENOMINATOR: the control must be a real set, and a PROPER SUBSET -- a control that expects
    // everything removed cannot tell a correct sweep from one that removes the log.
    assert!(
        expected_removed.len() >= 20,
        "the control expects only {} removals; too few to distinguish anything",
        expected_removed.len()
    );
    assert!(
        expected_removed.len() < 120,
        "the control expects every record removed, so it cannot fail on an over-removing sweep"
    );

    // ELEMENT BY ELEMENT, both directions named separately: what the sweep took that the control
    // did not (data loss), and what the control expected that the sweep left (space held).
    let over: Vec<u64> = actual_removed.difference(&expected_removed).copied().collect();
    let under: Vec<u64> = expected_removed.difference(&actual_removed).copied().collect();
    assert!(
        over.is_empty(),
        "the sweep removed {} record(s) the control says the base does NOT reflect -- that is \
         silent data loss: sequences {:?}",
        over.len(),
        over
    );
    assert!(
        under.is_empty(),
        "the sweep left {} record(s) the control says were reflected: sequences {:?}",
        under.len(),
        under
    );
    assert_eq!(
        report.records_removed,
        expected_removed.len(),
        "the report says {} removed and the control set holds {}",
        report.records_removed,
        expected_removed.len()
    );
}

/// THE REMOVAL PROBE RECOVERS EXACTLY WHAT WAS PLANTED, AND IS BOUNDED BY WHAT IT MEASURES.
///
/// A blind probe reads 0 exactly like an exact one, and every claim above is a probe reading.
/// Three ways, the zero last:
///
/// - a known number of removable records planted; the probe must return EXACTLY that many, not
///   "more than none", which a stuck-open probe would also satisfy;
/// - records planted that the sweep must NOT remove leave the probe unmoved;
/// - disarmed, the probe records nothing, so an armed reading is the arming and not the static.
#[test]
fn the_sweep_probes_recover_exactly_what_was_planted() {
    let _one_piece = roll_at(0);
    const PLANTED_REMOVABLE: usize = 37;
    const PLANTED_SURVIVING: usize = 13;

    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for value in 0..PLANTED_REMOVABLE {
        store
            .append_delta(
                SHARD,
                vec![item(&format!("removable/{value:08}"))],
                Vec::new(),
                Some(1),
                None,
                false,
                false,
            )
            .unwrap();
    }
    let mut last = 0u64;
    for value in 0..PLANTED_SURVIVING {
        last = store
            .append_delta(
                SHARD,
                vec![item(&format!("surviving/{value:08}"))],
                Vec::new(),
                Some(99),
                None,
                false,
                false,
            )
            .unwrap();
    }

    // DISARMED FIRST: the probe must be silent, so the reading below is the arming.
    probe::reset();
    probe::disarm_removals();
    let peek = LocalIndexLogStore::new(dir.path());
    let _ = peek.gc_reflected_before_anchor(SHARD, 0, last + 1, u64::MAX);
    assert!(
        probe::removals().is_empty(),
        "the removal probe recorded {} entries while disarmed",
        probe::removals().len()
    );

    let _armed = arm_removals();
    probe::reset();
    probe::arm_removals();
    let report = store
        .gc_reflected_before_anchor(SHARD, 1, last + 1, 0)
        .unwrap();

    // EXACTLY what was planted. Both probes, and the report beside them.
    assert_eq!(
        probe::removals().len(),
        PLANTED_REMOVABLE,
        "planted {PLANTED_REMOVABLE} removable records and the probe recovered {}",
        probe::removals().len()
    );
    assert_eq!(
        probe::sweep_frames_read(),
        (PLANTED_REMOVABLE + PLANTED_SURVIVING) as u64,
        "planted {} records in one piece and the read probe recovered {}",
        PLANTED_REMOVABLE + PLANTED_SURVIVING,
        probe::sweep_frames_read()
    );
    assert_eq!(
        probe::sweep_frames_copied(),
        PLANTED_SURVIVING as u64,
        "planted {PLANTED_SURVIVING} surviving records and the copy probe recovered {}",
        probe::sweep_frames_copied()
    );
    assert_eq!(
        report.records_removed, PLANTED_REMOVABLE,
        "the report says {} removed against {PLANTED_REMOVABLE} planted",
        report.records_removed
    );
    // The entry counter, planted the same way: one call, one entry.
    assert_eq!(
        probe::sweep_entries_post_dump(),
        1,
        "one post-dump sweep call and the entry counter recovered {}",
        probe::sweep_entries_post_dump()
    );
    assert_eq!(
        probe::sweep_entries_budgeted(),
        0,
        "no budgeted sweep was called and the entry counter recovered {}",
        probe::sweep_entries_budgeted()
    );
}

/// THE SWEEP HOLDS THE LOCK EVERY APPEND NEEDS, FOR THE WHOLE ROUND.
///
/// Direction decides what this is worth. A sweep that stops early leaves space held -- visible,
/// recoverable, merely slower. A sweep that stalls every writer on the shard while it runs is a
/// latency spike on a shared path, and that is what an unbounded round would be here IF the round
/// were unbounded in the corpus. It is not (see the cost measurement above), but the hold is real
/// and is measured rather than reasoned about.
///
/// NO TIMING IN IT. The first version of this compared appends landed during the sweep against a
/// control window of the same measured length -- and read 485x on an idle box and 11x on a busy
/// one, off identical code, because the leakage between sampling a counter and the sweep taking
/// the lock is a FIXED COUNT while the sweep's duration is not. That test would have joined the
/// load-dependent ones.
///
/// The instrument instead reads a counter of COMPLETED APPENDS -- incremented beside
/// `inner.stats.writes` at all three append sites, so it moves only while an appender holds the
/// very lock in question -- at the moment the sweep TAKES the lock and again at the last statement
/// before it drops it (`probe::SweepLockSpan`, declared after the guard so it drops before it).
/// If the lock is held across the round those two readings are EQUAL, exactly.
///
/// BOTH CONTROLS, because `0 == 0` is what a dead counter reports too:
///
/// - the appender must have completed appends over the test, so the counter is live;
/// - and the same two readings taken across a window where the lock is NOT held must DIFFER, so
///   the instrument is shown able to see movement before it is believed about the absence of it.
#[test]
fn a_sweep_holds_the_lock_every_append_needs() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let _one_piece = roll_at(0);
    let dir = tempfile::tempdir().unwrap();
    // Big enough that the sweep really walks, and every record removable.
    let store = Arc::new(seed(dir.path(), 8_000));
    let last = store.stats(SHARD).last_sequence;

    probe::reset();
    let stop = Arc::new(AtomicBool::new(false));
    let handle = {
        let (store, stop) = (Arc::clone(&store), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = store.append_delta(
                    SHARD,
                    vec![item("writer/subject")],
                    Vec::new(),
                    Some(1),
                    None,
                    false,
                    false,
                );
            }
        })
    };
    // Wait for the writer to be going, rather than sleeping a fixed span and hoping.
    let mut waited = 0;
    while probe::appends_completed() == 0 && waited < 600 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        waited += 1;
    }
    let before_window = probe::appends_completed();
    assert!(
        before_window > 0,
        "the appender completed nothing in {} ms, so it cannot say anything about what the sweep \
         blocks",
        waited * 10
    );

    // THE NEGATIVE CONTROL, FIRST AND WITH ITS OWN MESSAGE: the same two readings across a window
    // in which NO sweep holds the lock must DIFFER. Without this, the zero below is equally well
    // explained by a counter that never moves.
    let control_open = probe::appends_completed();
    std::thread::sleep(std::time::Duration::from_millis(30));
    let control_close = probe::appends_completed();
    let control_landed = control_close - control_open;
    assert!(
        control_landed > 0,
        "the counter did not move across a 30 ms window with nothing holding the lock, so a zero \
         across the sweep would say nothing at all"
    );

    // THE SUBJECT.
    let report = store.gc_before_sequence_limited(SHARD, last + 1, 0).unwrap();
    let during = probe::appends_during_the_sweep_lock();
    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();
    let total = probe::appends_completed();

    println!(
        "  lock hold: the sweep read {} frames and removed {} records; {during} append(s)          completed between the lock being taken and released. Control: {control_landed} append(s)          in a 30 ms window with nothing holding it; {total} appends completed over the test.",
        report.records_before, report.records_removed,
    );

    // The sweep did real work -- otherwise the zero is a claim about an instant.
    assert!(
        report.records_removed > 6_000,
        "the sweep removed only {} records, so it did not hold the lock long enough to say \
         anything",
        report.records_removed
    );
    // And the appender kept going THROUGH the sweep, so it was there to be blocked.
    assert!(
        total > before_window,
        "the appender completed nothing after the window opened ({total} against \
         {before_window}); it stopped rather than being blocked"
    );

    // EXACTLY ZERO. No ratio, no window, no duration.
    assert_eq!(
        during, 0,
        "{during} append(s) completed between the sweep taking the lock and releasing it; the \
         sweep no longer holds the lock across its walk, which changes what a bound on it would \
         be FOR"
    );
}

/// THE INTEGRAL, SUMMED OVER ROUNDS RATHER THAN ASSUMED FROM ONE.
///
/// A flat per-round figure over a subject the round itself grows is not health. The question is
/// whether round N reads N pieces' worth or one piece's worth, so eight rounds of "write, sweep"
/// are run and the frames read are SUMMED. Linear in rounds is the healthy answer; S(S+1)/2 is
/// the one that would make an unbounded sweep a real cost on a long-lived store.
#[test]
fn the_unbounded_sweep_integrates_linearly_in_rounds() {
    let _rolling = roll_at(DEFAULT_INDEX_LOG_SEGMENT_BYTES);
    const ROUNDS: usize = 8;
    const PER_ROUND: usize = 1_000;

    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    let mut per_round_read = Vec::new();
    let mut per_round_paths = Vec::new();
    let mut total_read = 0u64;
    let mut written = 0usize;
    for round in 0..ROUNDS {
        for value in 0..PER_ROUND {
            store
                .append_delta(
                    SHARD,
                    vec![item(&format!("tenant/1/object/{:08}", written + value))],
                    Vec::new(),
                    Some(round as u64 + 1),
                    None,
                    false,
                    false,
                )
                .unwrap();
        }
        written += PER_ROUND;
        let last = store.stats(SHARD).last_sequence;
        probe::reset();
        let report = store
            .gc_reflected_before_anchor(SHARD, round as u64 + 1, last + 1, 0)
            .unwrap();
        per_round_read.push(probe::sweep_frames_read());
        per_round_paths.push(probe::piece_paths());
        total_read += probe::sweep_frames_read();
        println!(
            "  round {:>2}: store {:>6} records | frames read {:>5} | piece paths {:>5} | \
             removed {:>5} | pieces unlinked {:>4} | log {:>8} -> {:>8} B",
            round + 1,
            written,
            per_round_read[round],
            per_round_paths[round],
            report.records_removed,
            report.dropped_segments,
            report.bytes_before,
            report.bytes_after,
        );
    }

    let first = per_round_read[0];
    let last_round = per_round_read[ROUNDS - 1];
    let linear = first.saturating_mul(ROUNDS as u64);
    let quadratic = first.saturating_mul((ROUNDS * (ROUNDS + 1) / 2) as u64);
    println!(
        "  INTEGRAL over {ROUNDS} rounds: frames read summed {total_read} | linear would be \
         {linear} | S(S+1)/2 would be {quadratic} | round 1 read {first}, round {ROUNDS} read \
         {last_round}"
    );

    // DENOMINATOR: the store really did grow, and every round really swept.
    assert_eq!(written, ROUNDS * PER_ROUND);
    assert!(
        first > 0,
        "round 1 read no frame at all, so the integral is a sum of zeros"
    );

    // The per-round figure does not ratchet with the store: round 8 reads what round 1 did.
    assert!(
        last_round <= first.saturating_mul(2).saturating_add(64),
        "round {ROUNDS} read {last_round} frames against round 1's {first}; the per-round walk is \
         ratcheting with the store"
    );
    // And the sum is linear, not quadratic. Named against BOTH shapes so the assertion says
    // which one it rules out.
    assert!(
        total_read < quadratic / 2,
        "frames read summed over {ROUNDS} rounds is {total_read}, which is on the S(S+1)/2 curve \
         ({quadratic}) rather than the linear one ({linear})"
    );
}

/// WHICH SWEEP EACH PRODUCTION DRIVER REACHES, COUNTED AT THE ENTRY POINT.
///
/// The two sweeps have different cadences, and the cadences run the OPPOSITE way from the bounds:
///
/// - `gc_reflected_before_anchor` -- NO entry budget -- is reached from
///   `maybe_dump_and_reclaim_index_logs`, which the embedded proxy polls on a BACKGROUND TIMER,
///   `MATRIXARK_RUST_PROXY_LOG_RECLAIM_INTERVAL_MS`, default 1,000 ms. Unattended, once a second,
///   for the life of the process.
/// - `gc_before_sequence_limited` -- budgeted at 256 -- is reached from
///   `run_storage_manager_cycle`, which in that same proxy is the `storage_manager_cycle` REQUEST
///   OP ("Exposed as an op, not a background thread: it rewrites durable structures, so it runs
///   when asked"), and in the server and the data-node is a maintenance cycle.
///
/// So the bound sits on the sweep that runs when asked, and the once-a-second background sweep
/// has none. Counted rather than read off the call graph: arguments of exactly this shape have
/// been overturned by a counter.
///
/// AND THE BUDGETED SWEEP IS GATED THREE MORE TIMES ON TOP. `storage_index_gc_report` calls it
/// only when the log is past `index_gc_index_log_bytes_threshold` (768 KiB), at least
/// `index_gc_usage_ratio_trigger_basis_points` (40%) of it is removable, and the WAL frontier is
/// safe. The post-dump sweep has none of those: past its dump cadence it runs. Both regimes are
/// driven below -- shipped triggers, where the cycle declines and NAMES which trigger; and
/// triggers opened with the budget left at its default, where it reaches the sweep.
///
/// A SEPARATE ENGINE PER DRIVER. Sharing one would have the first driver reclaim the log the
/// second is supposed to find, and the second would then read 0 for a reason that has nothing to
/// do with reachability. This test did exactly that before it was read: 0 entries, because the
/// log was 79 bytes by the time the cycle saw it.
///
/// PLANTED AND RECOVERED EXACTLY. A known number of driver calls is made and the counter must
/// return that number, not "more than none" -- and the OTHER counter must stay where it was,
/// which is what says a driver reaches one sweep and not both.
#[test]
fn each_production_driver_reaches_exactly_one_of_the_two_sweeps() {
    const DUMP_DRIVER_CALLS: u64 = 5;
    const CYCLE_DRIVER_CALLS: u64 = 3;
    const SEED: usize = 2_000;

    // ---------------------------------------------------------------- the background timer
    let timer_dir = tempfile::tempdir().unwrap();
    let timer_engine = seeded_engine(timer_dir.path(), SEED);
    probe::reset();
    for _ in 0..DUMP_DRIVER_CALLS {
        timer_engine.dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0);
    }
    let post_dump_after_timer = probe::sweep_entries_post_dump();
    let budgeted_after_timer = probe::sweep_entries_budgeted();

    // ------------------------------------------- the operator's cycle, SHIPPED triggers
    let shipped_dir = tempfile::tempdir().unwrap();
    let shipped_engine = seeded_engine(shipped_dir.path(), SEED);
    probe::reset();
    let mut shipped_reason = String::new();
    for _ in 0..CYCLE_DRIVER_CALLS {
        let report = shipped_engine.run_storage_manager_cycle(
            crate::engine::reports::StorageManagerCycleRequest {
                shard_id: SHARD,
                ..Default::default()
            },
        );
        shipped_reason = report
            .index_gc_report
            .as_ref()
            .map(|gc| gc.skipped_reason.clone())
            .unwrap_or_default();
    }
    let budgeted_under_shipped_triggers = probe::sweep_entries_budgeted();

    // ------------------------------------------- the operator's cycle, triggers OPENED
    // The BUDGET is left at its default; only the two triggers that decide whether the sweep is
    // called at all are opened, so what is measured is which function the driver reaches.
    let open_dir = tempfile::tempdir().unwrap();
    let open_engine = seeded_engine(open_dir.path(), SEED);
    probe::reset();
    for _ in 0..CYCLE_DRIVER_CALLS {
        open_engine.run_storage_manager_cycle(
            crate::engine::reports::StorageManagerCycleRequest {
                shard_id: SHARD,
                index_gc_index_log_bytes_threshold: 0,
                index_gc_usage_ratio_trigger_basis_points: 0,
                index_gc_commit_dirty_buckets_before_truncation: false,
                ..Default::default()
            },
        );
    }
    let post_dump_after_cycle = probe::sweep_entries_post_dump();
    let budgeted_after_cycle = probe::sweep_entries_budgeted();

    println!(
        "  reachability, {SEED} records, a separate engine per driver:\n             {DUMP_DRIVER_CALLS} background-timer call(s) -> post-dump sweep {post_dump_after_timer}x,          budgeted sweep {budgeted_after_timer}x\n             {CYCLE_DRIVER_CALLS} cycle call(s), SHIPPED triggers -> budgeted sweep          {budgeted_under_shipped_triggers}x, declined: {shipped_reason:?}\n             {CYCLE_DRIVER_CALLS} cycle call(s), triggers opened -> budgeted sweep          {budgeted_after_cycle}x, post-dump sweep {post_dump_after_cycle}x"
    );

    // PLANTED AND RECOVERED EXACTLY, both ways round.
    assert_eq!(
        post_dump_after_timer, DUMP_DRIVER_CALLS,
        "planted {DUMP_DRIVER_CALLS} background-timer driver call(s); the post-dump sweep's entry \
         counter recovered {post_dump_after_timer}"
    );
    assert_eq!(
        budgeted_after_timer, 0,
        "the background-timer driver reached the BUDGETED sweep {budgeted_after_timer} time(s); \
         the two paths are no longer distinct and the cadence claim above is stale"
    );
    assert_eq!(
        budgeted_after_cycle, CYCLE_DRIVER_CALLS,
        "planted {CYCLE_DRIVER_CALLS} storage-manager-cycle driver call(s) with the triggers \
         opened; the budgeted sweep's entry counter recovered {budgeted_after_cycle}"
    );
    assert_eq!(
        post_dump_after_cycle, 0,
        "the storage-manager cycle reached the POST-DUMP sweep {post_dump_after_cycle} time(s)"
    );

    // THE SHIPPED-TRIGGER ARM, asserted by name with its own message: at this corpus the cycle
    // does not reach the sweep at all, and it says which trigger stopped it. This is not the
    // fixture being too small -- 768 KiB of index log is about 19,000 records at the 40.05 B per
    // record `dump_release_scale` measures, so a store under that size never reaches the budgeted
    // sweep however often an operator asks.
    assert_eq!(
        budgeted_under_shipped_triggers, 0,
        "under shipped triggers the cycle reached the budgeted sweep \
         {budgeted_under_shipped_triggers} time(s) at {SEED} records; the extra gates named above \
         have moved"
    );
    assert_eq!(
        shipped_reason, "index-log byte threshold not reached",
        "the shipped-trigger arm declined for {shipped_reason:?}; this arm is only a control for \
         the trigger it names"
    );

    // NEGATIVE CONTROL: with no driver called at all, both counters must read zero. A counter
    // incremented somewhere else in the fixture would make every number above a coincidence.
    probe::reset();
    assert_eq!(
        (probe::sweep_entries_post_dump(), probe::sweep_entries_budgeted()),
        (0, 0),
        "the sweep entry counters are non-zero with no driver called"
    );
}

/// An engine with `records` string writes in it, on its own directory.
fn seeded_engine(dir: &std::path::Path, records: usize) -> crate::engine::TemporalEngine {
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    for index in 0..records {
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id: SHARD,
            command: crate::types::Command::StringSet {
                key: format!("k-{index:08}"),
                value: vec![b'v'; 96],
            },
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
    engine
}

/// THE REPO'S OWN BOUNDEDNESS GUARD PASSES AND SAYS NOTHING ABOUT THE SWEEP THAT RUNS ONCE A
/// SECOND.
///
/// `every_maintenance_round_is_bounded_by_default` pins `index_gc_max_entries_per_round` to 256
/// and asserts it is non-zero, on the reasoning that "zero is NO bound: ... the index-log sweep
/// removes every removable record". That is true of the field it reads, and the field it reads
/// is on `StorageManagerCycleRequest` -- the operator-driven path. The background sweep does not
/// take that field, has no field of its own, and is not named by that guard at all.
///
/// Asserted here rather than left as prose, so the two facts stay attached to each other: the
/// default really is 256, and the post-dump sweep really does run with no entry budget.
#[test]
fn the_default_entry_budget_belongs_to_the_path_that_runs_when_asked() {
    assert_eq!(
        crate::engine::reports::StorageManagerCycleRequest::default()
            .index_gc_max_entries_per_round,
        256,
        "the storage-manager cycle's default entry budget moved; the note above names 256"
    );
    assert_eq!(
        POST_DUMP_SWEEP_ENTRY_BUDGET, 0,
        "the post-dump sweep grew an entry budget; see the note on the constant for why it had \
         none, and re-measure before keeping this"
    );
    // And 0 there is not a smaller bound than 256 -- it is the absence of one, which is what the
    // consumer's own guard says. Stated against the CONSUMER so a reader does not have to take
    // the constant's word for it.
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for value in 0..64usize {
        store
            .append_delta(
                SHARD,
                vec![item(&format!("budget/{value:08}"))],
                Vec::new(),
                Some(1),
                None,
                false,
                false,
            )
            .unwrap();
    }
    let unbounded = store
        .gc_before_sequence_limited(SHARD, 33, POST_DUMP_SWEEP_ENTRY_BUDGET)
        .unwrap();
    assert_eq!(
        unbounded.records_removed, 32,
        "a budget of {POST_DUMP_SWEEP_ENTRY_BUDGET} removed {} of 32 removable records, so it is \
         being read as a bound of that size rather than as no bound",
        unbounded.records_removed
    );
    assert!(
        !unbounded.budget_exhausted,
        "a budget of {POST_DUMP_SWEEP_ENTRY_BUDGET} reported itself exhausted"
    );
}
