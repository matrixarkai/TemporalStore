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
