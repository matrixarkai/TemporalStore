// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a reclaim pass holds the append mutex for.
//!
//! The pass used to hold it across the directory listing, three walks of the active piece, the
//! copy of the survivors, their fsync and the rename. It now holds it for a decision and a swap.
//! These tests count what happens inside each hold, with the pass's own phase hook, and plant the
//! old behaviour to show the count would catch it.

use super::*;

struct SegmentPolicy(Option<u64>);

impl SegmentPolicy {
    fn set(threshold: u64) -> Self {
        Self(SEGMENT_BYTES_OVERRIDE.with(|value| value.replace(Some(threshold))))
    }
}

impl Drop for SegmentPolicy {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(self.0);
    }
}

/// Clears the hooks however the test ends.
struct Hooks;

impl Drop for Hooks {
    fn drop(&mut self) {
        GC_PHASE_HOOK.with(|hook| *hook.borrow_mut() = None);
        GC_PLAN_FORCES_FULL_SCAN.with(|force| force.set(false));
    }
}

fn put(store: &LocalWriteAheadLogStore, index: usize) {
    store
        .append(
            3,
            Command::StringSet {
                key: format!("key-{index}"),
                value: vec![b'v'; 64],
            },
        )
        .unwrap();
}

fn sequences(store: &LocalWriteAheadLogStore) -> Vec<u64> {
    store
        .scan(3, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .map(|(_, line)| decode_wal_line(line).unwrap().sequence)
        .collect()
}

/// Reads and statx calls made while the append mutex was held by one reclaim pass:
/// the plan step (start to "planned") plus the swap step ("scanned" to the end).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Held {
    opens: u64,
    stats: u64,
}

fn counters() -> (u64, u64) {
    (
        WAL_READ_FILE_OPENS.with(|opens| opens.get()),
        WAL_PATH_LEN_ASKS.with(|asks| asks.get()),
    )
}

/// Runs one pass and returns its report, what the two holds did, and what happened between them.
fn measured_pass(
    store: &LocalWriteAheadLogStore,
    retain: u64,
    between: impl FnMut() + 'static,
) -> (WriteAheadLogGcReport, Held, Held) {
    use std::cell::RefCell;
    use std::rc::Rc;
    let marks: Rc<RefCell<Vec<(&'static str, (u64, u64))>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&marks);
    let mut between = between;
    GC_PHASE_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |phase| {
            sink.borrow_mut().push((phase, counters()));
            if phase == "scanned" {
                between();
                // What `between` did is the writers' work, not the pass's: re-mark after it.
                sink.borrow_mut().push(("resumed", counters()));
            }
        }));
    });
    let start = counters();
    let report = store.gc_before_sequence_unchecked(3, retain).unwrap();
    let end = counters();
    let marks = marks.borrow();
    let at = |name: &str| marks.iter().find(|(phase, _)| *phase == name).map(|(_, c)| *c);
    let planned = at("planned").expect("the plan step ran");
    let plan_hold = Held { opens: planned.0 - start.0, stats: planned.1 - start.1 };
    // A pass that declined before the swap has no second hold.
    let swap_hold = match at("resumed") {
        Some(resumed) => Held { opens: end.0 - resumed.0, stats: end.1 - resumed.1 },
        None => Held { opens: 0, stats: 0 },
    };
    (report, plan_hold, swap_hold)
}

fn log_of(records: usize) -> (tempfile::TempDir, LocalWriteAheadLogStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    for index in 0..records {
        put(&store, index);
    }
    (dir, store)
}

#[test]
fn the_hold_does_not_grow_with_the_log() {
    let _policy = SegmentPolicy::set(0);
    let _hooks = Hooks;
    let mut holds = Vec::new();
    // One log three times the size of the other: the 100k -> 300k shape at a size a test can write.
    for records in [400usize, 1200, 3600] {
        let (_dir, store) = log_of(records);
        let retain = (records as u64) / 2;
        let (report, plan_hold, swap_hold) = measured_pass(&store, retain, || {});
        assert!(!report.skipped_raced && !report.skipped_not_worth_rewrite);
        assert_eq!(report.records_before, records);
        assert_eq!(report.records_after, records - (retain as usize - 1));
        // Nothing was walked under the mutex, and nothing was appended meanwhile to copy.
        assert_eq!(report.full_scans_under_mutex, 0, "{report:?}");
        assert_eq!(report.bytes_copied_under_mutex, 0);
        assert!(report.bytes_copied > 0, "the pass did copy, outside the mutex");
        holds.push((plan_hold, swap_hold));
    }
    // Flat: the same number of file opens and stats for a log three times as large, and the plan
    // opened the piece once and the swap opened nothing.
    assert_eq!(holds[0], holds[1]);
    assert_eq!(holds[1], holds[2]);
    // The piece is opened once, plus the one-byte read that vouches for a preallocated reservation.
    assert!(holds[0].0.opens <= 2, "{holds:?}");
    assert_eq!(holds[0].1.opens, 0, "{holds:?}");
}

#[test]
fn a_planted_whole_scan_in_the_plan_is_seen() {
    let _policy = SegmentPolicy::set(0);
    let _hooks = Hooks;
    let (_clean_dir, clean_store) = log_of(600);
    let (clean, clean_hold, _) = measured_pass(&clean_store, 300, || {});
    assert_eq!(clean.full_scans_under_mutex, 0);
    let (_dir, store) = log_of(600);
    GC_PLAN_FORCES_FULL_SCAN.with(|force| force.set(true));
    let (report, plan_hold, _) = measured_pass(&store, 300, || {});
    // The planted fault walks the log under the mutex: the guard's two figures both turn red.
    assert!(report.full_scans_under_mutex >= 1, "{report:?}");
    assert!(plan_hold.opens > clean_hold.opens, "{plan_hold:?} vs {clean_hold:?}");
}

#[test]
fn records_appended_while_the_pass_runs_survive_it() {
    let _policy = SegmentPolicy::set(0);
    let _hooks = Hooks;
    let (dir, store) = log_of(300);
    let writer = store.clone();
    let (report, _, swap_hold) = measured_pass(&store, 100, move || {
        for index in 300..312 {
            put(&writer, index);
        }
    });
    assert!(!report.skipped_raced);
    // Only what the writers added went through the swap, and it is a small fixed amount.
    assert!(report.bytes_copied_under_mutex > 0);
    assert!(report.bytes_copied_under_mutex < report.bytes_copied);
    assert_eq!(swap_hold.opens, 1, "the swap opens the live piece once, to read the new tail");
    let expected: Vec<u64> = (100..=312).collect();
    assert_eq!(sequences(&store), expected);
    // And a process that reopens the directory sees the same log.
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    assert_eq!(sequences(&reopened), expected);
    put(&store, 999);
    assert_eq!(store.stats(3).last_sequence, 313);
    assert_eq!(*sequences(&store).last().unwrap(), 313);
    // The seam with a warm append path that trusts its cached record end: the record written after
    // the swap must sit at the swapped piece's real end, so a fresh store reads every record.
    drop(store);
    let again = LocalWriteAheadLogStore::new(dir.path());
    let mut expected_again = expected.clone();
    expected_again.push(313);
    assert_eq!(sequences(&again), expected_again);
}

#[test]
fn a_roll_during_the_pass_refuses_the_swap_and_loses_nothing() {
    let _policy = SegmentPolicy::set(4 * 1024);
    let _hooks = Hooks;
    let (dir, store) = log_of(40);
    let before = sequences(&store);
    let writer = store.clone();
    let (report, _, _) = measured_pass(&store, 20, move || {
        // Enough to roll the piece the pass planned against.
        for index in 0..200 {
            put(&writer, 1000 + index);
        }
    });
    assert!(report.skipped_raced, "{report:?}");
    assert_eq!(report.records_removed, 0);
    let temp = dir.path().join("shard-3.wal.jsonl.tmp");
    assert!(!temp.exists(), "the refused pass leaves no temp file behind");
    let after = sequences(&store);
    assert!(before.iter().all(|sequence| after.contains(sequence)) || report.dropped_segments > 0);
    assert_eq!(*after.last().unwrap(), 240);
    // The next pass runs from the new piece and succeeds.
    let next = store.gc_before_sequence_unchecked(3, 230).unwrap();
    assert!(!next.skipped_raced, "{next:?}");
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    assert_eq!(*sequences(&reopened).last().unwrap(), 240);
}

#[test]
fn one_pass_per_shard_at_a_time() {
    let _policy = SegmentPolicy::set(0);
    let _hooks = Hooks;
    let (_dir, store) = log_of(200);
    let rival = store.clone();
    let seen = std::rc::Rc::new(std::cell::Cell::new(false));
    let flag = std::rc::Rc::clone(&seen);
    let (report, _, _) = measured_pass(&store, 100, move || {
        let declined = rival.gc_before_sequence_unchecked(3, 150).unwrap();
        flag.set(declined.skipped_raced && declined.records_removed == 0);
    });
    assert!(seen.get(), "a second pass during the first is declined, not run");
    assert!(!report.skipped_raced);
    // The mark is cleared on the way out: a later pass runs.
    let again = store.gc_before_sequence_unchecked(3, 150).unwrap();
    assert!(!again.skipped_raced, "{again:?}");
}

#[test]
fn a_reader_that_appears_during_the_pass_stops_the_swap() {
    let _policy = SegmentPolicy::set(0);
    let _hooks = Hooks;
    let (dir, store) = log_of(200);
    let registrar = store.clone();
    let (report, _, _) = measured_pass(&store, 150, move || {
        // A block-retention floor below what the pass planned to drop.
        registrar.set_block_retention_floor(3, 40);
    });
    assert!(report.skipped_raced, "{report:?}");
    assert_eq!(sequences(&store).first().copied(), Some(1));
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    assert_eq!(sequences(&reopened).first().copied(), Some(1));
}
