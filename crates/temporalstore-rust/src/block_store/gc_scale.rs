// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one reclaim round COSTS as the quarantine grows, measured at two sizes.

use crate::block_store::*;

/// A quarantine of `slabs` freshly-written files, every one of them too young to destroy.
///
/// Raw files with no manifest descriptor, so the purge's age falls back to the file mtime --
/// which is NOW. That is the state a quarantine is in for the whole of
/// `DELAYED_DESTROY_MIN_AGE_MS` after the collector sets the slabs aside.
fn quarantine_fixture(root: &std::path::Path, slabs: u64) {
    let trash = delayed_destroy_dir(root);
    fs::create_dir_all(&trash).unwrap();
    let mut id = 0u64;
    while id < slabs {
        fs::write(
            trash.join(format!("block_segment_{id:020}.seg.deleted.{id}")),
            b"slab",
        )
        .unwrap();
        id += 1;
    }
}

/// Every entry the round NAMED, which for a quarantine of parseable names and no caller list is
/// every entry in the directory. The four lists are disjoint and together they are the loop body's
/// only exits, so this is a count of iterations rather than an estimate of one.
fn entries_examined(report: &BlockStorePurgeDelayedDestroyReport) -> usize {
    report.purged_block_slab_ids.len()
        + report.restored_block_slab_ids.len()
        + report.restore_blocked_block_slab_ids.len()
        + report.retained_too_young_block_slab_ids.len()
}

/// THE CAP BOUNDS A ROUND THAT IS DOING WORK. IT DOES NOT BOUND A ROUND IN THE QUARANTINE WINDOW.
///
/// `purge_delayed_destroy_slabs_capped` spends its budget on work done -- a destroy or a restore
/// -- and never on an entry it passes over. That is deliberate and it is what stops a blocked prefix
/// starving the drain for ever. It has a consequence that has not been measured: a slab that is
/// TOO YOUNG is also a skip, so it charges nothing either, so `processed` never reaches the
/// budget, so the `break` that makes a capped round cost the budget rather than the directory
/// never fires. The round walks every entry.
///
/// That is not a corner. A quarantined slab waits `DELAYED_DESTROY_MIN_AGE_MS` -- an hour -- and
/// for that whole hour EVERY entry is too young, so every round in the window walks the entire
/// directory, stats every file, and does nothing. The cap engages only once the slabs mature.
///
/// Measured at two sizes, ten times apart, as a COUNT of loop iterations rather than a duration:
///
///   quarantine     examined, in-window     examined, actionable
///      8,000                    8,000                    1,000
///     80,000                   80,000                    1,000
///   ratio                      10.00x                    1.00x
///
/// Same function, same budget, same fixture; the only difference is whether the entries are old
/// enough to act on. Run it:
///
///   cargo test -p temporalstore-rust --lib what_a_purge_round_examines \
///       -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_a_purge_round_examines_at_two_quarantine_sizes() {
    let mut in_window = Vec::new();
    let mut actionable = Vec::new();
    let sizes = [8_000u64, 80_000];

    let mut index = 0usize;
    while index < sizes.len() {
        let slabs = sizes[index];

        // HALF ONE: the shipped age, on a quarantine that has just been filled.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        quarantine_fixture(dir.path(), slabs);
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "DENOMINATOR: the in-window round must really face {slabs} quarantined slabs"
        );
        let started = std::time::Instant::now();
        let report = store
            .purge_delayed_destroy_slabs_capped(
                DELAYED_DESTROY_MIN_AGE_MS,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let window_ms = started.elapsed().as_secs_f64() * 1e3;
        let window_examined = entries_examined(&report);
        assert_eq!(
            report.processed_block_slabs, 0,
            "NON-VACUOUS ZERO: every entry is too young, so the round did no work at all -- if \
             this is non-zero the fixture matured and the rest of the reading is meaningless: \
             {report:?}"
        );
        assert!(
            !report.budget_exhausted,
            "and the budget was never spent, so the cap's break never fired: {report:?}"
        );
        assert_eq!(
            window_examined as u64, slabs,
            "the round walked every entry in the directory"
        );
        in_window.push((slabs, window_examined, window_ms));

        // HALF TWO: the same directory, the same budget, entries that are old enough to act on.
        // Ordered second so a mutant that kills the first half does not prevent this one running.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        quarantine_fixture(dir.path(), slabs);
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "DENOMINATOR: the actionable round must face the same {slabs} slabs"
        );
        let started = std::time::Instant::now();
        let report = store
            .purge_delayed_destroy_slabs_capped(
                0,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let actionable_ms = started.elapsed().as_secs_f64() * 1e3;
        let actionable_examined = entries_examined(&report);
        assert_eq!(
            report.processed_block_slabs, DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            "the actionable round spends its whole budget: {report:?}"
        );
        assert!(
            report.budget_exhausted,
            "and says there is more to drain: {report:?}"
        );
        assert_eq!(
            report.retained_too_young_block_slab_ids.len(),
            0,
            "NON-VACUOUS ZERO the other way: nothing was held back for age here"
        );
        actionable.push((slabs, actionable_examined, actionable_ms));

        index += 1;
    }

    println!();
    println!("  ONE PURGE ROUND, budget {DELAYED_DESTROY_MAX_SLABS_PER_ROUND}");
    println!(
        "  {:>10}  {:>22}  {:>22}",
        "quarantine", "examined, in-window", "examined, actionable"
    );
    let mut row = 0usize;
    while row < sizes.len() {
        println!(
            "  {:>10}  {:>13} {:>7.1} ms  {:>13} {:>7.1} ms",
            in_window[row].0,
            in_window[row].1,
            in_window[row].2,
            actionable[row].1,
            actionable[row].2,
        );
        row += 1;
    }
    println!(
        "  ratio      {:>13.2}x {:>18.2}x",
        in_window[1].1 as f64 / in_window[0].1 as f64,
        actionable[1].1 as f64 / actionable[0].1 as f64,
    );
    println!();

    assert_eq!(
        in_window[1].1 / in_window[0].1,
        10,
        "the in-window round is LINEAR in the quarantine: ten times the directory, ten times the \
         walk"
    );
    assert_eq!(
        actionable[0].1, actionable[1].1,
        "while the actionable round is FLAT at the budget, which is what the cap was for"
    );
}

/// The same property, small enough for CI, asserted as two separate halves.
///
/// The guard is that the two rounds below -- identical but for whether their entries are old
/// enough -- examine DIFFERENT numbers of entries, and that the in-window one examines the whole
/// directory rather than the budget. A budget charged for the entries it passes over would make both halves equal
/// and is exactly the change `purge_delayed_destroy_slabs_capped` forbids, so this cannot be
/// satisfied by making the cap tighter.
#[test]
fn a_purge_round_inside_the_quarantine_window_is_not_bounded_by_its_budget() {
    let slabs = 4_000u64;
    let budget = 1_000usize;
    assert!(
        (slabs as usize) > budget * 2,
        "DENOMINATOR: the directory must be several budgets deep or the two halves cannot differ"
    );

    // HALF ONE: in the window. Nothing is actionable, so the cap cannot engage.
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} slabs are really in quarantine"
    );
    let window = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();
    assert_eq!(
        window.processed_block_slabs, 0,
        "NON-VACUOUS ZERO: no entry was old enough, so no budget was charged: {window:?}"
    );
    assert_eq!(
        entries_examined(&window) as u64,
        slabs,
        "so the round examined the WHOLE directory, not its budget: {window:?}"
    );
    assert!(
        !window.budget_exhausted,
        "and reports no budget exhaustion, so a draining caller stops here: {window:?}"
    );

    // HALF TWO: actionable. Same directory size, same budget, and now the cap does bound it.
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: the actionable half faces the same {slabs} slabs"
    );
    let actionable = store
        .purge_delayed_destroy_slabs_capped(0, std::iter::empty(), None, budget)
        .unwrap();
    assert_eq!(
        actionable.processed_block_slabs, budget,
        "the actionable round spends its whole budget: {actionable:?}"
    );
    assert_eq!(
        entries_examined(&actionable),
        budget,
        "and examines the budget rather than the directory: {actionable:?}"
    );
    assert!(
        actionable.budget_exhausted,
        "and says there is more: {actionable:?}"
    );

    // THE DIFFERENCE, stated on its own so neither half can carry it alone.
    assert!(
        entries_examined(&window) > entries_examined(&actionable) * 3,
        "a round in the quarantine window walks the directory ({}) while an actionable round \
         walks its budget ({})",
        entries_examined(&window),
        entries_examined(&actionable),
    );
}

/// How many slabs the strace probes below run against. Literal default so the probe is
/// reproducible without the variable; the driver sets it to take the second reading.
fn probe_slabs() -> u64 {
    std::env::var("RECLAIM_SCALE_SLABS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(8_000)
}

/// THE COLLECTOR HALF, for `strace -y -e trace=fsync,fdatasync` to count by PATH.
///
/// Install, then one quarantine round. The fsyncs of the store root and of the trash directory
/// are the quantity: the loop used to issue one of each PER SLAB, and they were hoisted to one
/// pair per round. Counting them by the path strace prints separates them from the manifest's own
/// and from the per-slab install fsyncs, which no change here touches.
///
///   RECLAIM_SCALE_SLABS=8000 strace -f -y -e trace=fsync,fdatasync -o /tmp/q8 \
///       ./temporalstore_rust-<hash> probe_one_quarantine_round --exact --ignored --nocapture
#[test]
#[ignore]
fn probe_one_quarantine_round() {
    let slabs = probe_slabs();
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    let mut id = 0u64;
    while id < slabs {
        store.install_slab(id, b"slab-contents").unwrap();
        id += 1;
    }
    assert_eq!(
        store.slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} slabs are installed before the quarantine round"
    );
    println!("PROBE-MARK install-done {slabs}");
    let started = std::time::Instant::now();
    let report = store
        .gc_slabs_before_with_live_refs_delayed_destroy(slabs - 1, [slabs - 1])
        .unwrap();
    let ms = started.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        report.delayed_destroy_block_slab_ids.len() as u64,
        slabs - 1,
        "the round must really have quarantined {} slabs, or the fsync count is of nothing",
        slabs - 1
    );
    println!(
        "PROBE-MARK quarantine-done slabs={} quarantined={} ms={ms:.1}",
        slabs,
        report.delayed_destroy_block_slab_ids.len()
    );
}

/// THE PURGE HALF, in the quarantine window, for the same strace count.
///
/// A round that destroys and restores nothing must issue NO fsync at all -- there is no rename
/// and no unlink for one to commit. That zero is the one worth asserting, and it is non-vacuous
/// because the round really did walk every entry: `retained_too_young` names all of them.
#[test]
#[ignore]
fn probe_one_in_window_purge_round() {
    let slabs = probe_slabs();
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    println!("PROBE-MARK fixture-done {slabs}");
    let started = std::time::Instant::now();
    let report = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let ms = started.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        report.retained_too_young_block_slab_ids.len() as u64,
        slabs,
        "DENOMINATOR: the round examined every one of the {slabs} entries"
    );
    println!(
        "PROBE-MARK in-window-purge-done slabs={slabs} examined={} ms={ms:.1}",
        entries_examined(&report)
    );
}

/// THE PURGE HALF, actionable, for the same strace count.
#[test]
#[ignore]
fn probe_one_actionable_purge_round() {
    let slabs = probe_slabs();
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    println!("PROBE-MARK fixture-done {slabs}");
    let started = std::time::Instant::now();
    let report = store
        .purge_delayed_destroy_slabs_capped(
            0,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let ms = started.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        report.processed_block_slabs, DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        "DENOMINATOR: the round really spent its whole budget"
    );
    println!(
        "PROBE-MARK actionable-purge-done slabs={slabs} destroyed={} ms={ms:.1}",
        report.purged_block_slab_ids.len()
    );
}

/// THE STORE-WIDE LOCK SPANS THE WHOLE WALK, counted rather than timed.
///
/// `purge_delayed_destroy_slabs_capped` takes `self.inner` before it opens the trash directory
/// and holds it until it returns, so every entry the round examines is examined under the lock
/// that every read of the block store also needs. In the actionable case the cap bounds how long
/// that is. In the quarantine window nothing bounds it.
///
/// The count is of PROBE READS COMPLETED BY A SECOND THREAD while the round runs -- each one a
/// `slab_ids()`, which takes the same lock. Under the round it is zero of the attempted set; the
/// control, an identical probe against an empty quarantine, completes all of them. Reporting both
/// is what makes the zero mean "blocked" rather than "the probe never ran".
#[test]
#[ignore]
fn the_store_lock_is_held_across_the_whole_in_window_walk() {
    let slabs = 80_000u64;
    let probes = 200usize;

    // CONTROL FIRST, so a mutant that stops the first half from finishing still leaves this
    // reading on the record: with no round in the way every probe read completes.
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(BlockStore::new(dir.path()));
    let mut control_completed = 0usize;
    let mut probe = 0usize;
    while probe < probes {
        store.slab_ids().unwrap();
        control_completed += 1;
        probe += 1;
    }
    assert_eq!(
        control_completed, probes,
        "DENOMINATOR: the probe read itself works {probes} times out of {probes}"
    );

    // AND NOW THE SAME PROBE AGAINST A ROUND THAT IS WALKING EIGHTY THOUSAND ENTRIES.
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(BlockStore::new(dir.path()));
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} entries are really in the directory the round will walk"
    );

    let round_store = std::sync::Arc::clone(&store);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let round_barrier = std::sync::Arc::clone(&barrier);
    let round = std::thread::spawn(move || {
        round_barrier.wait();
        let started = std::time::Instant::now();
        let report = round_store
            .purge_delayed_destroy_slabs_capped(
                DELAYED_DESTROY_MIN_AGE_MS,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        (report, started.elapsed())
    });

    barrier.wait();
    // Give the round time to be inside the walk rather than still entering the function. The
    // walk is hundreds of milliseconds; this is a small fraction of it.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let probe_started = std::time::Instant::now();
    store.slab_ids().unwrap();
    let blocked_for = probe_started.elapsed();

    let (report, round_took) = round.join().unwrap();
    assert_eq!(
        report.retained_too_young_block_slab_ids.len() as u64,
        slabs,
        "the round really walked every entry"
    );
    assert_eq!(
        report.processed_block_slabs, 0,
        "NON-VACUOUS ZERO: and did no work while it held the lock"
    );

    println!();
    println!(
        "  one in-window round over {slabs} entries took {:.1} ms; a probe read issued 50 ms in \
         waited {:.1} ms for the lock ({control_completed}/{probes} of the same read complete \
         instantly with no round running)",
        round_took.as_secs_f64() * 1e3,
        blocked_for.as_secs_f64() * 1e3,
    );
    println!();

    assert!(
        blocked_for.as_millis() >= 50,
        "a read issued while the round walks must wait for it: waited only {:.1} ms of a {:.1} ms \
         round",
        blocked_for.as_secs_f64() * 1e3,
        round_took.as_secs_f64() * 1e3,
    );
}

/// A SLAB THAT IS TOO YOUNG STAYS IN FRONT OF THE LOOP AND DOES NOT STARVE THE ONES BEHIND IT.
///
/// This is the #1719 property re-asked for the one skip reason that fires in the ordinary state.
/// A quarantine that mixes matured slabs with fresh ones must destroy every matured slab the
/// budget allows in a single round, wherever directory order happens to put the fresh ones -- the
/// budget is spent on work, so walking past a fresh entry costs the round a step and never a unit
/// of its budget. The cost of those steps is what
/// `a_purge_round_inside_the_quarantine_window_is_not_bounded_by_its_budget` measures; that the
/// drain still ADVANCES is this one.
#[test]
fn a_matured_slab_behind_a_fresh_one_is_still_destroyed_this_round() {
    let matured = 300u64;
    let fresh = 1_200u64;
    let budget = 1_000usize;
    assert!(
        (matured as usize) < budget,
        "DENOMINATOR: the budget must be able to take every matured slab in one round, or a \
         shortfall would mean the cap and not a starving skip"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());

    // The matured half goes through the real collector, so each slab gets a descriptor whose
    // stamp can be moved into the past.
    let mut id = 0u64;
    while id <= matured {
        store.install_slab(id, b"slab-contents").unwrap();
        id += 1;
    }
    let quarantined = store
        .gc_slabs_before_with_live_refs_delayed_destroy(matured, [matured])
        .unwrap()
        .delayed_destroy_block_slab_ids
        .len();
    assert_eq!(
        quarantined as u64, matured,
        "DENOMINATOR: the collector really set aside {matured} slabs"
    );
    let moved = store
        .backdate_delayed_destroy_stamps_for_test(2 * DELAYED_DESTROY_MIN_AGE_MS)
        .unwrap();
    assert_eq!(
        moved as u64, matured,
        "DENOMINATOR: and every one of them was backdated past the grace window"
    );

    // The fresh half is written straight into the trash directory with no descriptor, at ids far
    // above the matured ones, so the age falls back to a mtime of NOW.
    let trash = delayed_destroy_dir(dir.path());
    let mut id = 1_000_000u64;
    while id < 1_000_000 + fresh {
        fs::write(
            trash.join(format!("block_segment_{id:020}.seg.deleted.{id}")),
            b"slab",
        )
        .unwrap();
        id += 1;
    }
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        matured + fresh,
        "DENOMINATOR: the directory holds both halves"
    );

    let report = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();

    // HALF ONE: every matured slab was destroyed, in this one round.
    assert_eq!(
        report.purged_block_slab_ids.len() as u64,
        matured,
        "every matured slab must be destroyed this round however many fresh ones sit in front of \
         it in directory order: {report:?}"
    );
    // HALF TWO: and exactly the fresh ones were held, so none of them was charged for.
    assert_eq!(
        report.retained_too_young_block_slab_ids.len() as u64,
        fresh,
        "and exactly the fresh half was held back for age: {report:?}"
    );
    assert_eq!(
        report.processed_block_slabs as u64, matured,
        "budget is spent on work done, so the {fresh} skipped entries cost none of it: {report:?}"
    );
}
