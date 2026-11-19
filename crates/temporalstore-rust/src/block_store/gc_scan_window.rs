// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A bounded GC round examines a bounded window of candidate slabs and resumes where it stopped.

use super::*;

fn store_with_stale_slabs(count: u64) -> (tempfile::TempDir, BlockStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    for slab in 0..count {
        store.install_slab(slab, format!("stale-slab-{slab}").as_bytes()).unwrap();
    }
    // The newest slab is the current one and is never a candidate.
    store.install_slab(count, b"current").unwrap();
    (dir, store)
}

fn policy(max_scan_slabs: usize, scan_after_slab_id: Option<u64>) -> BlockStoreGcPolicy {
    BlockStoreGcPolicy {
        max_scan_slabs,
        scan_after_slab_id,
        max_destroy_slabs: 0,
        max_destroy_physical_bytes: 0,
        max_utility_score: None,
        min_age_ms: None,
        min_slab_garbage_basis_points: None,
    }
}

/// THE WORK OF A ROUND IS BOUNDED BY THE WINDOW, NOT BY HOW MANY STALE SLABS THE STORE HOLDS.
///
/// Ten stale slabs, a window of four: three rounds, each examining at most four, together
/// covering every slab exactly once, the last one reporting the scan complete. The denominator
/// is asserted too: the unbounded plan over the same store must offer all ten, or "four per
/// round" proves nothing about a window.
#[test]
fn a_bounded_scan_examines_a_window_and_resumes_behind_it() {
    let (_dir, store) = store_with_stale_slabs(10);
    let current = 10;

    let unbounded = store.gc_policy_plan(current, Vec::<u64>::new(), &policy(0, None)).unwrap();
    assert_eq!(unbounded.candidate_count, 10, "DENOMINATOR: {unbounded:?}");
    assert!(unbounded.scan_complete, "an unbounded scan has nothing left: {unbounded:?}");

    let mut seen = Vec::new();
    let mut after = None;
    let mut rounds = 0;
    loop {
        let plan = store.gc_policy_plan(current, Vec::<u64>::new(), &policy(4, after)).unwrap();
        assert!(plan.candidate_count <= 4, "a round examined more than its window: {plan:?}");
        seen.extend(plan.candidates.iter().map(|candidate| candidate.block_slab_id));
        rounds += 1;
        if plan.scan_complete {
            break;
        }
        assert_eq!(
            plan.candidate_count, 4,
            "a round that is not complete must have used its whole window: {plan:?}"
        );
        after = plan.scan_last_block_slab_id;
        assert!(after.is_some(), "an incomplete round must say where it stopped: {plan:?}");
        assert!(rounds < 10, "the scan is not making progress");
    }
    assert_eq!(rounds, 3, "ten candidates in windows of four is three rounds");
    seen.sort_unstable();
    assert_eq!(seen, (0..10).collect::<Vec<u64>>(), "every slab examined exactly once");
}

/// A resume point past every candidate finds nothing and says the scan is complete.
#[test]
fn a_resume_point_past_the_last_candidate_is_an_empty_complete_scan() {
    let (_dir, store) = store_with_stale_slabs(5);
    let plan = store.gc_policy_plan(5, Vec::<u64>::new(), &policy(4, Some(4))).unwrap();
    assert_eq!(plan.candidate_count, 0, "{plan:?}");
    assert!(plan.scan_complete, "{plan:?}");
    assert_eq!(plan.scan_last_block_slab_id, None, "{plan:?}");
}

/// THE GATE IS DECIDED FROM TWO TOTALS AND ONLY EVER ERRS TOWARD COLLECTING.
///
/// Each arm is one way to be wrong that the others do not catch: a gate that treated an unknown
/// total as "nothing to do" would switch collection off; one that used `<=` would collect a store
/// sitting exactly at the threshold; one that trusted live bytes above logical bytes would call a
/// counting artefact utility.
#[test]
fn the_reclaim_gate_is_due_below_the_threshold_and_never_closed_by_an_unknown() {
    use super::{evaluate_reclaim_gate as gate, DEFAULT_RECLAIM_UTILITY_THRESHOLD_BASIS_POINTS as T};

    // Not ready is due, whatever the totals say.
    assert!(gate(1_000, 1_000, false, T).due, "an unknown total must never close the gate");
    assert!(gate(0, 0, false, T).due);

    // Ready with nothing logical has nothing to collect.
    assert!(!gate(0, 0, true, T).due);

    // Strictly below the threshold is due; at it is not.
    assert!(gate(499, 1_000, true, 5_000).due, "49.9% live is below 50%");
    assert!(!gate(500, 1_000, true, 5_000).due, "exactly at the threshold is left alone");
    assert!(!gate(900, 1_000, true, 5_000).due);

    // Live above logical is clamped, not counted as extra utility.
    assert!(!gate(5_000, 1_000, true, 5_000).due);

    // A zero threshold is the gate switched off, which is due.
    assert!(gate(1_000, 1_000, true, 0).due);

    // The totals are the widest the type holds: no overflow, no wrap.
    assert!(!gate(u64::MAX, u64::MAX, true, 5_000).due);
    assert!(gate(1, u64::MAX, true, 5_000).due);

    // It reports the inputs it decided from.
    let reported = gate(7, 9, true, T);
    assert_eq!((reported.live_bytes, reported.logical_bytes, reported.ready), (7, 9, true));
}

#[test]
fn the_store_gate_reads_published_live_bytes_against_slab_logical_bytes() {
    let (_dir, store) = store_with_stale_slabs(4);
    // Nothing published: an unknown, so due, and never a reason to skip a round.
    let unknown = store.reclaim_gate(DEFAULT_RECLAIM_UTILITY_THRESHOLD_BASIS_POINTS);
    assert!(!unknown.ready && unknown.due);
    assert!(unknown.logical_bytes > 0, "slab descriptors carry logical bytes");

    let publish = |fraction_percent: u64| {
        let mut live = BTreeMap::new();
        for slab in 0..=4_u64 {
            let logical = store.slab_live_fractions().unwrap().iter()
                .find(|row| row.block_slab_id == slab).map(|row| row.logical_bytes).unwrap_or(0);
            live.insert(slab, BlockStoreSlabLive { live_block_refs: 1, live_bytes: logical * fraction_percent / 100 });
        }
        store.publish_live_block_bytes(live);
    };
    publish(10);
    let mostly_garbage = store.reclaim_gate(DEFAULT_RECLAIM_UTILITY_THRESHOLD_BASIS_POINTS);
    assert!(mostly_garbage.ready && mostly_garbage.due, "{mostly_garbage:?}");
    publish(90);
    let mostly_live = store.reclaim_gate(DEFAULT_RECLAIM_UTILITY_THRESHOLD_BASIS_POINTS);
    assert!(mostly_live.ready && !mostly_live.due, "{mostly_live:?}");
    assert_eq!(mostly_live.logical_bytes, unknown.logical_bytes);
}
