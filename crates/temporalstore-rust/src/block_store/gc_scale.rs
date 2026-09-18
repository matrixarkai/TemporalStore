// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one reclaim round COSTS as the quarantine grows, measured at two sizes.

use crate::block_store::*;

/// A quarantine of `slabs` freshly-written files, every one of them too young to destroy.
///
/// Raw files with NO MANIFEST DESCRIPTOR, so the purge's age falls back to the file mtime --
/// which is NOW. This is the CRASH shape, not the ordinary one, and the distinction has since
/// earned its keep: the collector quarantines through `set_slab_state`, which stamps a descriptor,
/// so a quarantine the collector built is fully described. A quarantine with files the manifest
/// never learned about is what a process that died between the renames and the manifest persist
/// leaves behind. `quarantine_fixture_the_collector_shape` is the other one, and a round can
/// decline to walk only the second.
fn quarantine_fixture(root: &std::path::Path, slabs: u64) {
    quarantine_files_from(root, 0, slabs);
}

/// `slabs` quarantined files starting at `first_id`.
///
/// THE FIRST ID IS NOT COSMETIC once a store is opened on top of the directory. Slab 0 is the id
/// a fresh store takes for its own ACTIVE slab, and the open-time reconcile will not let a
/// folded lifecycle state overwrite the active slab's -- so a fixture that starts at 0 comes back
/// one descriptor short of its own file count, which reads as the adoption having half failed. A
/// fixture that needs every file described starts at 1.
fn quarantine_files_from(root: &std::path::Path, first_id: u64, slabs: u64) {
    let trash = delayed_destroy_dir(root);
    fs::create_dir_all(&trash).unwrap();
    let mut id = first_id;
    while id < first_id + slabs {
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
///
/// IT IS A SUM OF CLASSIFICATIONS, WHICH IS NOT THE SAME THING AS A COUNT OF ITERATIONS, and the
/// sentence above holds only while every iteration ends in one of the four. Turning the budget's
/// `break` into a `continue` breaks exactly that: the entries walked past after the budget is
/// spent are classified as nothing and this sum cannot see them. `examined_block_slabs` on the
/// report is the real count and is what the guards below assert; this stays because the older
/// measurements are quoted against it and the two agreeing is itself worth asserting.
fn entries_examined(report: &BlockStorePurgeDelayedDestroyReport) -> usize {
    report.purged_block_slab_ids.len()
        + report.restored_block_slab_ids.len()
        + report.restore_blocked_block_slab_ids.len()
        + report.retained_too_young_block_slab_ids.len()
}

/// The quarantine THE COLLECTOR leaves: every file in it described by a manifest descriptor whose
/// `updated_unix_ms` records when the slab was set aside.
///
/// Built by writing the files and opening the store AFTERWARDS, so the open-time reconcile adopts
/// each one -- the same route a restarted process takes, and far cheaper at eighty thousand slabs
/// than installing and collecting each. The caller must assert the denominator the fixture returns
/// before reading anything else: a store whose descriptors did not get adopted would let a purge
/// round walk for reasons that have nothing to do with what is being measured.
fn quarantine_fixture_the_collector_shape(root: &std::path::Path, slabs: u64) -> BlockStore {
    quarantine_files_from(root, 1, slabs);
    BlockStore::new(root)
}

/// How many quarantined slabs this store has a stamped descriptor for.
fn quarantined_with_a_stamp(store: &BlockStore) -> usize {
    store
        .slab_descriptors()
        .into_iter()
        .filter(|slab| {
            slab.state == BlockStoreSlabState::DelayedDestroy && slab.updated_unix_ms.is_some()
        })
        .count()
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
        .and_then(|raw| raw.trim().parse::<u64>().ok())
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

/// THE SECOND ROUND IN THE WINDOW DOES NOT OPEN THE DIRECTORY.
///
/// The first one has to: nothing in a fresh process knows what is in quarantine, and finding out
/// is the walk. What it also does, on its way past every entry, is note the earliest moment any
/// of them arrived -- and from that the next round can answer the only question it had without
/// looking, because if the oldest slab in there is not old enough then no slab in there is.
///
/// Both rounds are run BEFORE either is asserted about. The second round is the claim and it
/// depends on the first having happened, so it cannot be moved in front of it; running them both
/// first is the next best thing, and it means a mutant that breaks the first round's reading
/// still leaves the second round's reading on the record.
#[test]
fn a_second_round_inside_the_window_declines_without_opening_the_directory() {
    let slabs = 4_000u64;
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: every one of the {slabs} quarantined slabs must be described and stamped, \
         or a round would be walking for a reason this test is not about"
    );

    let first = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let second = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let still_quarantined = store.delayed_destroy_slab_ids().unwrap().len();

    // HALF ONE: the first round pays the full walk, which is what there is to remove.
    assert_eq!(
        first.examined_block_slabs as u64, slabs,
        "the first round walks every entry: {first:?}"
    );
    assert_eq!(
        first.processed_block_slabs, 0,
        "NON-VACUOUS: and does no work at all while doing it, so the whole walk is waste: {first:?}"
    );
    assert!(
        !first.declined_without_examining,
        "and it cannot have declined -- a fresh store knows nothing yet: {first:?}"
    );

    // HALF TWO: the second round pays nothing, and the quarantine it declined to walk is still
    // there. A zero examined count against an empty directory would mean nothing.
    assert_eq!(
        still_quarantined as u64, slabs,
        "NON-VACUOUS ZERO: all {slabs} slabs are still in quarantine after both rounds"
    );
    assert!(
        second.declined_without_examining,
        "so the second round declines outright: {second:?}"
    );
    assert_eq!(
        second.examined_block_slabs, 0,
        "and examines nothing to do it: {second:?}"
    );
    assert_eq!(
        second.processed_block_slabs, 0,
        "having done, as the first round also did, no work: {second:?}"
    );
}

/// A LIVE SLAB IN QUARANTINE IS STILL RESCUED, AND THE DECLINE IS WHAT HAD TO ASK.
///
/// The liveness re-check restores a quarantined slab the caller still calls live, and it does so
/// at ANY age -- it runs before the age is consulted, because a live slab is not too young to
/// destroy, it is not for destroying. A round that declines on age alone would defer that restore
/// by up to the whole grace window, and the restore is the repair for a reader that cannot reach
/// its slab. So the decline asks first.
///
/// The third round is the control and it is what makes the second one mean something: same store,
/// same moment, same everything but the live set, and it declines. Without it a second round that
/// walked would be indistinguishable from a decline that never became available.
#[test]
fn a_live_quarantined_slab_is_restored_by_a_round_that_could_otherwise_have_declined() {
    let slabs = 2_000u64;
    let rescued = 7u64;
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described, stamped, quarantined slabs"
    );

    // Round one earns the decline for every round after it.
    let first = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    // Round two is offered the decline and must refuse it, because the caller names a live slab
    // that is sitting in the trash directory.
    let with_a_live_slab = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            [rescued],
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    // Round three is the control: nothing live, and the decline is taken.
    let control = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();

    assert_eq!(
        first.examined_block_slabs as u64, slabs,
        "DENOMINATOR: the first round really walked the directory: {first:?}"
    );

    // HALF ONE: the live slab came back.
    assert_eq!(
        with_a_live_slab.restored_block_slab_ids,
        vec![rescued],
        "the round the caller told about a live quarantined slab must restore it rather than \
         decline on age: {with_a_live_slab:?}"
    );
    assert!(
        !with_a_live_slab.declined_without_examining,
        "which means it did not decline: {with_a_live_slab:?}"
    );
    assert_eq!(
        with_a_live_slab.examined_block_slabs as u64, slabs,
        "and paid for the walk that found it: {with_a_live_slab:?}"
    );

    // HALF TWO, THE CONTROL: the decline was there to be taken, and only the live set withheld it.
    assert!(
        control.declined_without_examining,
        "the same round with nothing live declines, so it was the live slab and not the absence \
         of a decline that made the round above walk: {control:?}"
    );
    assert_eq!(
        control.examined_block_slabs, 0,
        "examining nothing: {control:?}"
    );
}

/// AN ENTRY THE MANIFEST DOES NOT KNOW ABOUT STOPS THE ROUND DECLINING, FOR EVER IF NEED BE.
///
/// A file in quarantine with no descriptor is what a crash between the collector's renames and
/// its manifest persist leaves behind, and it is the one entry whose liveness the decline cannot
/// test -- the test reads slab state out of the manifest, and there is no entry there to read.
/// Rather than decline on an incomplete picture, a round that meets one withdraws its opinion and
/// the next round walks. That is this fix declining to help in the one shape where helping would
/// mean guessing, and it is worth a guard of its own because the alternative fails SILENTLY: a
/// deferred restore looks exactly like no restore being needed.
#[test]
fn a_quarantined_file_with_no_descriptor_keeps_every_round_walking() {
    let slabs = 1_000u64;
    let dir = tempfile::tempdir().unwrap();
    // The store is opened FIRST and the files are written behind its back, so nothing reconciles
    // them and they stay undescribed.
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} files really are in the trash directory"
    );
    assert_eq!(
        quarantined_with_a_stamp(&store),
        0,
        "DENOMINATOR, the other way: and NONE of them is described, which is the whole premise"
    );

    let first = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let second = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();

    assert_eq!(
        first.examined_block_slabs as u64, slabs,
        "the first round walks: {first:?}"
    );
    assert!(
        !second.declined_without_examining,
        "and so does the second -- an undescribed entry withdraws the round's opinion rather than \
         letting it decline on a picture with a hole in it: {second:?}"
    );
    assert_eq!(
        second.examined_block_slabs as u64, slabs,
        "which costs the whole directory again: {second:?}"
    );
}

/// THE BUDGET'S `break` COSTS THE BUDGET AND NOT THE DIRECTORY, ASSERTED ON THE COUNT THAT MOVES.
///
/// Until `examined_block_slabs` existed nothing in the crate could see this. The budget check
/// stops the round; turning that `break` into a `continue` leaves a round that walks every
/// remaining entry doing nothing -- and reports the same four lists, the same
/// `processed_block_slabs`, the same `budget_exhausted`, and passes. The directory is four
/// budgets deep here so the two readings cannot be confused for each other.
#[test]
fn a_round_that_stops_on_its_budget_examines_the_budget_and_not_the_directory() {
    let slabs = 4_000u64;
    let budget = 1_000usize;
    assert!(
        slabs as usize >= budget * 4,
        "DENOMINATOR: the directory must be several budgets deep or the two readings coincide"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), slabs);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        slabs,
        "DENOMINATOR: {slabs} slabs are in quarantine"
    );

    // Actionable, so every entry the round reaches charges the budget and the cap engages.
    let report = store
        .purge_delayed_destroy_slabs_capped(0, std::iter::empty(), None, budget)
        .unwrap();

    // HALF ONE: the round did a budget's worth of work and said there was more.
    assert_eq!(
        report.processed_block_slabs, budget,
        "the round spends its whole budget: {report:?}"
    );
    assert!(
        report.budget_exhausted,
        "and says the drain continues: {report:?}"
    );
    // HALF TWO: and it stopped rather than walking on. This is the half that pins the `break`,
    // and it is stated second so a mutant that breaks the first does not keep it from running.
    assert_eq!(
        report.examined_block_slabs,
        budget + 1,
        "a round that stops on its budget must EXAMINE its budget and the one entry that found \
         the budget already spent -- walking the remaining {} entries to classify none of them \
         would report every other number above unchanged: {report:?}",
        slabs as usize - budget - 1,
    );
    assert_eq!(
        entries_examined(&report) + 1,
        report.examined_block_slabs,
        "and that ONE is the whole difference between what the round looked at and what it \
         reached a decision about, which is how a stopped round is told from a walked one: \
         {report:?}"
    );
}

/// A ROUND THAT STOPPED ON ITS BUDGET IS NOT ENTITLED TO AN OPINION, SO THE NEXT ONE WALKS.
///
/// The entries behind the `break` are precisely the ones the round knows nothing about, and an
/// earliest arrival that does not cover them is an earliest arrival that could be wrong in the
/// one direction this cache must never be wrong in. So a capped-out round publishes nothing.
#[test]
fn a_round_that_stopped_on_its_budget_leaves_the_next_round_to_walk() {
    let matured = 1_500u64;
    let fresh = 1_000u64;
    let budget = 500usize;
    assert!(
        (matured as usize) > budget,
        "DENOMINATOR: the matured half must outlast the budget or the round would not stop early"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), matured);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        matured,
        "DENOMINATOR: {matured} described slabs to age"
    );
    let moved = store
        .backdate_delayed_destroy_stamps_for_test(2 * DELAYED_DESTROY_MIN_AGE_MS)
        .unwrap();
    assert_eq!(
        moved as u64, matured,
        "DENOMINATOR: and every one of them moved past the window"
    );
    // A fresh, described half on top, so the directory still holds in-window entries after the
    // matured ones are gone.
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

    let stopped = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();
    let after = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();

    // HALF ONE: the first round really did stop early.
    assert!(
        stopped.budget_exhausted,
        "the first round must stop on its budget: {stopped:?}"
    );
    assert_eq!(
        stopped.purged_block_slab_ids.len(),
        budget,
        "having destroyed exactly a budget's worth: {stopped:?}"
    );
    // HALF TWO: and left nothing behind for the next round to decline on.
    assert!(
        !after.declined_without_examining,
        "so the round after it must walk -- the entries behind the break were never seen and an \
         earliest arrival that does not cover them cannot be published: {after:?}"
    );
    assert!(
        after.examined_block_slabs > 0,
        "which means it examined entries: {after:?}"
    );
}

/// A CALLER THAT ASKS FOR AN IMMEDIATE PURGE ALWAYS WALKS.
///
/// `min_age_ms == 0` makes the decline's first condition -- `now - earliest < min_age_ms` --
/// false whatever the clock says and whatever is cached, which is why every test written against
/// this function before the cache existed is unaffected by it. Worth a guard rather than a
/// remark: the arithmetic that makes it true is one `<` and a mutant can turn it into a `<=`.
#[test]
fn an_immediate_purge_walks_however_recently_the_round_before_it_declined() {
    let slabs = 1_500u64;
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described, stamped, quarantined slabs"
    );

    let warm = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            0,
        )
        .unwrap();
    let declined = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            0,
        )
        .unwrap();
    let immediate = store
        .purge_delayed_destroy_slabs_capped(0, std::iter::empty(), None, 0)
        .unwrap();

    // HALF ONE: the decline really was in force at the moment the immediate purge ran.
    assert_eq!(
        warm.examined_block_slabs as u64, slabs,
        "DENOMINATOR: the warming round walked: {warm:?}"
    );
    assert!(
        declined.declined_without_examining,
        "NON-VACUOUS: and the round after it declined, so the cache was live: {declined:?}"
    );
    // HALF TWO: and the immediate purge went through it anyway, and destroyed everything.
    assert!(
        !immediate.declined_without_examining,
        "a purge asked for at zero age must never decline: {immediate:?}"
    );
    assert_eq!(
        immediate.purged_block_slab_ids.len() as u64,
        slabs,
        "and must destroy every quarantined slab: {immediate:?}"
    );
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len(),
        0,
        "leaving the trash directory empty"
    );
}

/// WHAT THE DECLINE REMOVES FROM AN IN-WINDOW ROUND, at two quarantine sizes ten times apart.
///
/// The first round in a fresh process walks whatever the quarantine holds; every round after it,
/// for the rest of the hour, examines nothing. Against an hourly grace window and a periodic
/// cycle measured in seconds that is almost all of them.
///
/// Counted, not timed, for the reason the sibling measurement above gives: the count is the claim
/// and a duration only agrees with it.
///
///   quarantine    examined, round 1    examined, round 2    examined, actionable
///      8,000                 8,000                    0                   1,000
///     80,000                80,000                    0                   1,000
///
/// Run it:
///
///   cargo test -p temporalstore-rust --lib what_the_decline_removes \
///       -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_the_decline_removes_from_an_in_window_round_at_two_quarantine_sizes() {
    let mut rows = Vec::new();
    let sizes = [8_000u64, 80_000];

    let mut index = 0usize;
    while index < sizes.len() {
        let slabs = sizes[index];
        let dir = tempfile::tempdir().unwrap();
        let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
        assert_eq!(
            quarantined_with_a_stamp(&store) as u64,
            slabs,
            "DENOMINATOR: the round must face {slabs} described, quarantined slabs"
        );

        let started = std::time::Instant::now();
        let first = store
            .purge_delayed_destroy_slabs_capped(
                DELAYED_DESTROY_MIN_AGE_MS,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let first_ms = started.elapsed().as_secs_f64() * 1e3;
        let started = std::time::Instant::now();
        let second = store
            .purge_delayed_destroy_slabs_capped(
                DELAYED_DESTROY_MIN_AGE_MS,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let second_ms = started.elapsed().as_secs_f64() * 1e3;

        assert_eq!(
            first.processed_block_slabs, 0,
            "NON-VACUOUS ZERO: the first round did no work, so its whole walk is what is being \
             removed: {first:?}"
        );
        assert_eq!(
            first.examined_block_slabs as u64, slabs,
            "and it walked every entry: {first:?}"
        );
        assert!(
            second.declined_without_examining,
            "the second round declines: {second:?}"
        );
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "NON-VACUOUS ZERO: with all {slabs} slabs still in the directory it declined to walk"
        );

        // The same directory, the same budget, entries that are old enough to act on. Ordered
        // last so a mutant that kills either reading above does not prevent it running.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        quarantine_fixture(dir.path(), slabs);
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "DENOMINATOR: the actionable round faces the same {slabs} slabs"
        );
        let started = std::time::Instant::now();
        let actionable = store
            .purge_delayed_destroy_slabs_capped(
                0,
                std::iter::empty(),
                None,
                DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            )
            .unwrap();
        let actionable_ms = started.elapsed().as_secs_f64() * 1e3;
        assert_eq!(
            actionable.processed_block_slabs, DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            "the actionable round spends its whole budget: {actionable:?}"
        );

        rows.push((
            slabs,
            first.examined_block_slabs,
            first_ms,
            second.examined_block_slabs,
            second_ms,
            actionable.examined_block_slabs,
            actionable_ms,
        ));
        index += 1;
    }

    println!();
    println!("  ONE PURGE ROUND, budget {DELAYED_DESTROY_MAX_SLABS_PER_ROUND}");
    println!(
        "  {:>10}  {:>20}  {:>20}  {:>22}",
        "quarantine", "examined, round 1", "examined, round 2", "examined, actionable"
    );
    let mut row = 0usize;
    while row < rows.len() {
        println!(
            "  {:>10}  {:>11} {:>7.1} ms  {:>11} {:>7.1} ms  {:>13} {:>7.1} ms",
            rows[row].0,
            rows[row].1,
            rows[row].2,
            rows[row].3,
            rows[row].4,
            rows[row].5,
            rows[row].6,
        );
        row += 1;
    }
    println!();

    assert_eq!(
        rows[1].1 / rows[0].1,
        10,
        "the first round is LINEAR in the quarantine: ten times the directory, ten times the walk"
    );
    assert_eq!(
        (rows[0].3, rows[1].3),
        (0, 0),
        "and the second round examines nothing at either size, so what it costs does not depend \
         on the quarantine at all"
    );
    assert_eq!(
        (rows[0].5, rows[1].5),
        (
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND
        ),
        "while an actionable round is still flat at the budget, which the cap was for"
    );
}

/// THE STORE-WIDE LOCK IS NOT HELD ACROSS A ROUND THAT DECLINES.
///
/// The sibling measurement above counts probe reads blocked by a round that walks eighty thousand
/// entries under the lock. This one asks the same question of the round that follows it. The
/// probe is a `slab_ids()`, which needs the same lock, and the reading is how long it waits.
#[test]
#[ignore]
fn a_round_that_declines_does_not_hold_the_store_lock() {
    let slabs = 80_000u64;
    let probes = 200usize;

    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(quarantine_fixture_the_collector_shape(dir.path(), slabs));
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described entries for the round to decline on"
    );

    // CONTROL FIRST, so a mutant that stops the measured half finishing still leaves this on the
    // record: with no round in the way every probe read completes.
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

    // ROUND ONE pays the walk and earns the decline. Timed, because it is the before reading.
    let started = std::time::Instant::now();
    let first = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let first_ms = started.elapsed().as_secs_f64() * 1e3;
    assert_eq!(
        first.examined_block_slabs as u64, slabs,
        "DENOMINATOR: the first round really walked all {slabs}: {first:?}"
    );

    // AND NOW THE SAME PROBE AGAINST THE ROUND THAT FOLLOWS IT.
    let round_store = std::sync::Arc::clone(&store);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let round_barrier = std::sync::Arc::clone(&barrier);
    let round = std::thread::spawn(move || {
        round_barrier.wait();
        let started = std::time::Instant::now();
        let mut rounds = 0usize;
        let mut declined = 0usize;
        // One round returns in microseconds now, which is too short to aim a probe at. A thousand
        // of them is a fair stand-in for the hour of rounds this replaces, and every one must
        // decline or the reading is of something else.
        while rounds < 1_000 {
            let report = round_store
                .purge_delayed_destroy_slabs_capped(
                    DELAYED_DESTROY_MIN_AGE_MS,
                    std::iter::empty(),
                    None,
                    DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
                )
                .unwrap();
            if report.declined_without_examining {
                declined += 1;
            }
            rounds += 1;
        }
        (rounds, declined, started.elapsed())
    });

    barrier.wait();
    let mut blocked_completed = 0usize;
    let mut worst = std::time::Duration::ZERO;
    let mut probe = 0usize;
    while probe < probes {
        let probe_started = std::time::Instant::now();
        store.slab_ids().unwrap();
        worst = worst.max(probe_started.elapsed());
        blocked_completed += 1;
        probe += 1;
    }
    let (rounds, declined, rounds_took) = round.join().unwrap();

    println!();
    println!(
        "  before: one in-window round over {slabs} entries took {first_ms:.1} ms with the store \
         lock held throughout"
    );
    println!(
        "  after:  {declined}/{rounds} declining rounds took {:.1} ms TOGETHER, while \
         {blocked_completed}/{probes} probe reads ran alongside them, the slowest waiting \
         {:.1} ms",
        rounds_took.as_secs_f64() * 1e3,
        worst.as_secs_f64() * 1e3,
    );
    println!();

    assert_eq!(
        (declined, rounds),
        (1_000, 1_000),
        "NON-VACUOUS: every one of the rounds the probes ran against declined"
    );
    assert_eq!(
        blocked_completed, probes,
        "and every probe read completed alongside them: {blocked_completed}/{probes}"
    );
    assert!(
        rounds_took.as_secs_f64() * 1e3 < first_ms,
        "a thousand declining rounds must cost less than the one walking round they replace: \
         {:.1} ms against {first_ms:.1} ms",
        rounds_took.as_secs_f64() * 1e3,
    );
}

/// HOW MANY TIMES ONE STORAGE ROUND OPENS THE TRASH DIRECTORY.
///
/// A periodic round reaches `delayed_destroy_slab_reports()` TWICE before the purge opens the
/// same directory for itself -- once from the lifecycle plan and once from the block-GC
/// dependency plan, at `engine::storage_lifecycle_methods` lines 288 and 428 on the commit this
/// was written against. Each of those walks stats every entry and builds an owned report per
/// slab; the purge's own walk then does it a third time.
///
/// The round is reproduced here at the block-store boundary, which is where all three walks
/// actually happen, and counted by the tally each walk now reports itself to. The decline removes
/// the THIRD one and nothing else: the two plan walks are the engine's and are untouched.
///
/// Run it:
///
///   cargo test -p temporalstore-rust --lib what_one_round_costs_the_trash_directory \
///       -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn what_one_round_costs_the_trash_directory() {
    let slabs = 8_000u64;
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described, stamped, quarantined slabs"
    );
    // The first round earns the decline for the rounds after it, and is not part of either
    // reading below.
    store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();

    let walks = |before: u64| {
        crate::durability_metrics::snapshot()
            .get("block_store_trash_dir_walk")
            .copied()
            .unwrap_or_default()
            - before
    };
    let started = crate::durability_metrics::snapshot()
        .get("block_store_trash_dir_walk")
        .copied()
        .unwrap_or_default();

    // POSITIVE CONTROL FIRST: one plan walk on its own must move the tally by exactly one, or
    // every number below is measuring a counter that does not count.
    store.delayed_destroy_slab_reports().unwrap();
    let one_walk = walks(started);
    assert_eq!(
        one_walk, 1,
        "POSITIVE CONTROL: a single plan walk must register as one walk, got {one_walk}"
    );

    // THE ROUND AS IT RUNS: two plan walks, then the purge.
    let started = crate::durability_metrics::snapshot()
        .get("block_store_trash_dir_walk")
        .copied()
        .unwrap_or_default();
    store.delayed_destroy_slab_reports().unwrap();
    store.delayed_destroy_slab_reports().unwrap();
    let declining = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
        .unwrap();
    let with_decline = walks(started);

    // AND THE SAME ROUND WITH THE PURGE WALKING, which is what it did every time before.
    let started = crate::durability_metrics::snapshot()
        .get("block_store_trash_dir_walk")
        .copied()
        .unwrap_or_default();
    store.delayed_destroy_slab_reports().unwrap();
    store.delayed_destroy_slab_reports().unwrap();
    // `min_age_ms == 0` is the one thing that always walks, so this is the same round with the
    // decline taken away and nothing else changed.
    let walking = store
        .purge_delayed_destroy_slabs_capped(0, std::iter::empty(), None, 0)
        .unwrap();
    let without_decline = walks(started);

    println!();
    println!("  ONE STORAGE ROUND, {slabs} slabs in quarantine");
    println!("  trash-directory walks, purge walking:   {without_decline}");
    println!("  trash-directory walks, purge declining: {with_decline}");
    println!();

    assert!(
        declining.declined_without_examining,
        "NON-VACUOUS: the declining round really declined: {declining:?}"
    );
    assert_eq!(
        walking.purged_block_slab_ids.len() as u64,
        slabs,
        "NON-VACUOUS the other way: the walking round really walked and destroyed everything"
    );
    assert_eq!(
        without_decline, 3,
        "a round whose purge walks opens the trash directory three times"
    );
    assert_eq!(
        with_decline, 2,
        "and the decline removes the purge's own walk, and only that one -- the two plan walks \
         belong to the engine and are untouched here"
    );
}

/// THE CACHE HOLDS THE EARLIEST ARRIVAL AND NOT THE LATEST, AND THE DIFFERENCE IS THE WHOLE SAFETY.
///
/// A cache holding a moment EARLIER than the truth makes `now - earliest` larger, so the round
/// walks when it need not: a wasted walk, which is what every round in the window cost before any
/// of this. A cache holding a moment LATER makes the round decline while something in there has
/// in fact matured, and that costs a delayed destroy. Only one of those two is allowed, and
/// nothing in a quarantine whose slabs all arrived together can tell them apart -- the minimum and
/// the maximum are the same number.
///
/// So half the quarantine is given an older arrival, and the round that follows asks for an age
/// the older half has reached and the younger half has not. The right answer is to walk and
/// destroy the older half. A cache holding the LATEST arrival answers that nothing can have
/// matured, declines, and destroys nothing -- and reports success while doing it, which is why
/// this needs a guard rather than a comment.
#[test]
fn the_cached_arrival_is_the_earliest_one_so_a_matured_half_is_not_declined_away() {
    let older = 600u64;
    let younger = 900u64;
    let slabs = older + younger;
    // The older half is moved back to within a minute of maturing; the younger half stays where
    // it is. The round below then asks for an age one minute short of the window, which the older
    // half has reached and the younger half has not.
    let older_by = DELAYED_DESTROY_MIN_AGE_MS - 60_000;
    let asked_for = DELAYED_DESTROY_MIN_AGE_MS - 120_000;
    assert!(
        asked_for < older_by && older_by < DELAYED_DESTROY_MIN_AGE_MS,
        "DENOMINATOR: the age asked for must sit between the two halves' ages, or the two halves \
         are not actually different"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described, stamped, quarantined slabs"
    );
    // Ids run from 1, so the first `older` of them are the older half.
    let older_ids = (1..=older).collect::<BTreeSet<u64>>();
    let moved = store
        .backdate_named_delayed_destroy_stamps_for_test(Some(&older_ids), older_by)
        .unwrap();
    assert_eq!(
        moved as u64, older,
        "DENOMINATOR: exactly the older half moved, leaving {younger} at their original age"
    );

    // The warming round walks and records what it saw. It must find nothing actionable at the
    // shipped age, or the two halves are not both inside the window and the reading below is of
    // something else.
    let warm = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            0,
        )
        .unwrap();
    let asked = store
        .purge_delayed_destroy_slabs_capped(asked_for, std::iter::empty(), None, 0)
        .unwrap();

    // HALF ONE: both halves really were in the window when the cache was written.
    assert_eq!(
        warm.examined_block_slabs as u64, slabs,
        "DENOMINATOR: the warming round walked every entry: {warm:?}"
    );
    assert_eq!(
        warm.processed_block_slabs, 0,
        "NON-VACUOUS ZERO: and found nothing old enough at the shipped age, so what it cached \
         covers two halves that are both still quarantined: {warm:?}"
    );

    // HALF TWO: and the round that asks for the older half's age gets it. Stated second so a
    // mutant that breaks the first does not keep this one from running.
    assert!(
        !asked.declined_without_examining,
        "a round asking for an age the OLDEST quarantined slab has reached must not decline -- a \
         cache holding the latest arrival rather than the earliest would decline here and destroy \
         nothing: {asked:?}"
    );
    assert_eq!(
        asked.purged_block_slab_ids.len() as u64,
        older,
        "and must destroy exactly the older half: {asked:?}"
    );
    assert_eq!(
        asked.retained_too_young_block_slab_ids.len() as u64,
        younger,
        "holding back exactly the younger half, which is what makes the count above a division \
         of this quarantine rather than a coincidence: {asked:?}"
    );
}


/// WHAT A ROUND IS ENTITLED TO PUBLISH, ASSERTED ON THE CLAIM ITSELF.
///
/// The behavioural guards above ask what a round DOES. This one asks what the round leaves
/// behind, because the publish rule has three arms and only one of them has a consequence that
/// can be provoked on demand:
///
///   * a round that walked the whole listing and read a stamp for every entry that stayed may
///     publish the earliest of them;
///   * a round that STOPPED ON ITS BUDGET may not, because the entries behind the `break` are
///     precisely the ones it knows nothing about, and an earliest that does not cover them can
///     name a moment LATER than the truth -- the one direction this cache is not allowed to be
///     wrong in, and the one that costs a delayed destroy rather than a wasted walk;
///   * a round that met an entry the manifest does not describe may not either.
///
/// Provoking the second arm's consequence needs an older entry to fall BEHIND the break, and
/// where the break lands depends on the order the directory listing returns -- which is not the
/// order of the ids and is not stable across the writes a test makes between two rounds. A guard
/// resting on that would be measuring the filesystem. The rule rests on neither, so it is asserted
/// as the rule.
#[test]
fn what_a_round_may_publish_about_the_earliest_arrival() {
    let matured = 400u64;
    let fresh = 400u64;
    let budget = 100usize;
    assert!(
        (matured as usize) > budget,
        "DENOMINATOR: the matured half must outlast the budget or no round here stops early"
    );

    // ARM ONE: a round that walked it all, with every entry described. It may publish, and what
    // it publishes must be an arrival it actually saw rather than the moment it ran.
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), fresh);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        fresh,
        "DENOMINATOR: {fresh} described, stamped, quarantined slabs"
    );
    assert_eq!(
        store.delayed_destroy_earliest_for_test(),
        None,
        "DENOMINATOR: a store that has run no round claims nothing"
    );
    let walked = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            0,
        )
        .unwrap();
    let published = store.delayed_destroy_earliest_for_test();
    assert_eq!(
        walked.examined_block_slabs as u64, fresh,
        "DENOMINATOR: the round walked every entry: {walked:?}"
    );
    assert_eq!(
        walked.retained_too_young_block_slab_ids.len() as u64,
        fresh,
        "NON-VACUOUS: and every entry STAYED, so there was something for it to publish about"
    );
    assert!(
        published.is_some(),
        "a round that saw every entry and a stamp for each may publish an earliest arrival"
    );

    // ARM TWO: a round that stopped on its budget. Same fixture shape, but with enough matured
    // entries to spend the budget before the listing runs out.
    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), matured);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        matured,
        "DENOMINATOR: {matured} described slabs to age"
    );
    let moved = store
        .backdate_delayed_destroy_stamps_for_test(2 * DELAYED_DESTROY_MIN_AGE_MS)
        .unwrap();
    assert_eq!(
        moved as u64, matured,
        "DENOMINATOR: and every one of them moved past the window"
    );
    let stopped = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            budget,
        )
        .unwrap();
    assert!(
        stopped.budget_exhausted,
        "DENOMINATOR: the round really stopped on its budget: {stopped:?}"
    );
    assert!(
        stopped.examined_block_slabs < matured as usize,
        "NON-VACUOUS: leaving entries it never pulled off the listing, which is the whole reason \
         it may not publish: {stopped:?}"
    );
    assert_eq!(
        store.delayed_destroy_earliest_for_test(),
        None,
        "so it must publish NOTHING -- an earliest that covers only the prefix it walked can name \
         a moment later than an arrival behind the break, and the round after it would decline \
         while that entry had already matured"
    );

    // ARM THREE: a round that met an entry the manifest does not describe. Ordered last so a
    // mutant that kills either arm above does not prevent it running.
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    quarantine_fixture(dir.path(), fresh);
    assert_eq!(
        store.delayed_destroy_slab_ids().unwrap().len() as u64,
        fresh,
        "DENOMINATOR: {fresh} files in the trash directory"
    );
    assert_eq!(
        quarantined_with_a_stamp(&store),
        0,
        "DENOMINATOR, the other way: and not one of them described"
    );
    let undescribed = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            None,
            0,
        )
        .unwrap();
    assert_eq!(
        undescribed.examined_block_slabs as u64, fresh,
        "DENOMINATOR: the round walked every entry: {undescribed:?}"
    );
    assert_eq!(
        store.delayed_destroy_earliest_for_test(),
        None,
        "and published nothing: an undescribed entry is one the decline's liveness test cannot \
         see, so the round withdraws its opinion rather than decline on a picture with a hole in it"
    );
}

/// Ages part of a quarantine and returns the age a later round should ask for to reach it.
///
/// The older part is moved to within a minute of maturing and the age asked for is two minutes
/// short of the window, so it sits strictly between the two halves: the older half has reached it
/// and the younger half has not. Both halves stay inside the grace window at the shipped age, so
/// a round run at `DELAYED_DESTROY_MIN_AGE_MS` still finds nothing to do and every entry stays.
fn age_part_of_the_quarantine(store: &BlockStore, older: &BTreeSet<u64>) -> u64 {
    let older_by = DELAYED_DESTROY_MIN_AGE_MS - 60_000;
    let moved = store
        .backdate_named_delayed_destroy_stamps_for_test(Some(older), older_by)
        .unwrap();
    assert_eq!(
        moved,
        older.len(),
        "DENOMINATOR: exactly the older part moved"
    );
    DELAYED_DESTROY_MIN_AGE_MS - 120_000
}

/// AN ENTRY THE CALLER DID NOT NAME STILL BOUNDS WHAT THE NEXT ROUND MAY DECLINE ON.
///
/// A slab left out of the caller's list is not this round's business and the round is right to
/// pass over it -- but it STAYS IN THE DIRECTORY, so it is very much the next round's business.
/// Leaving it out of the earliest arrival would publish a moment later than the truth, and the
/// round after would decline while that slab had already matured. This matters more than it
/// sounds: the periodic storage cycle is the caller that passes a list, so the narrowed round is
/// the ordinary one and not the exception.
///
/// The round below is UNCAPPED, so it reaches every entry whatever order the listing returns them
/// in. Nothing here depends on where the older half happens to sit.
#[test]
fn an_entry_left_out_of_the_callers_list_still_bounds_the_next_rounds_decline() {
    let older = 300u64;
    let younger = 500u64;
    let slabs = older + younger;

    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described, stamped, quarantined slabs"
    );
    // Ids run from 1. The OLDER half is the one the caller will leave out.
    let older_ids = (1..=older).collect::<BTreeSet<u64>>();
    let selected = (older + 1..=slabs).collect::<BTreeSet<u64>>();
    let asked_for = age_part_of_the_quarantine(&store, &older_ids);

    let narrowed = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            std::iter::empty(),
            Some(selected),
            0,
        )
        .unwrap();
    let after = store
        .purge_delayed_destroy_slabs_capped(asked_for, std::iter::empty(), None, 0)
        .unwrap();

    // HALF ONE: the narrowed round really passed over the older half without classifying it.
    assert_eq!(
        narrowed.examined_block_slabs as u64, slabs,
        "DENOMINATOR: the round pulled every entry off the listing: {narrowed:?}"
    );
    assert_eq!(
        narrowed.retained_too_young_block_slab_ids.len() as u64,
        younger,
        "NON-VACUOUS: and reported only the half it was told about as too young -- the other half \
         was not its business and is in none of its lists: {narrowed:?}"
    );
    assert_eq!(
        narrowed.processed_block_slabs, 0,
        "having done no work: {narrowed:?}"
    );

    // HALF TWO: and the round after it can still reach the half that was passed over.
    assert!(
        !after.declined_without_examining,
        "the next round must not decline -- the half left out of the list has matured, and an \
         earliest arrival that counted only the named half would name a later moment and miss it: \
         {after:?}"
    );
    assert_eq!(
        after.purged_block_slab_ids.len() as u64,
        older,
        "and must destroy exactly that half: {after:?}"
    );
}

/// A LIVE SLAB THAT COULD NOT BE PUT BACK STILL BOUNDS WHAT THE NEXT ROUND MAY DECLINE ON.
///
/// A restore that finds a file already occupying the slab id moves nothing, so the slab stays in
/// quarantine: not destroyed, not restored, and charged no budget. It is the third way an entry
/// survives a round, and like the other two it has to be counted, or the earliest arrival the
/// round publishes will not cover it.
#[test]
fn a_blocked_restore_still_bounds_the_next_rounds_decline() {
    let blocked = 200u64;
    let rest = 600u64;
    let slabs = blocked + rest;

    let dir = tempfile::tempdir().unwrap();
    let store = quarantine_fixture_the_collector_shape(dir.path(), slabs);
    assert_eq!(
        quarantined_with_a_stamp(&store) as u64,
        slabs,
        "DENOMINATOR: {slabs} described, stamped, quarantined slabs"
    );
    // Ids run from 1. The blocked half is also the OLDER half.
    let blocked_ids = (1..=blocked).collect::<BTreeSet<u64>>();
    let asked_for = age_part_of_the_quarantine(&store, &blocked_ids);
    // Occupy each of their slab ids in the store, so the restore has nowhere to put them back.
    let mut id = 1u64;
    while id <= blocked {
        fs::write(slab_path(dir.path(), id), b"occupied").unwrap();
        id += 1;
    }

    let round = store
        .purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS,
            blocked_ids.iter().copied(),
            None,
            0,
        )
        .unwrap();
    let after = store
        .purge_delayed_destroy_slabs_capped(asked_for, std::iter::empty(), None, 0)
        .unwrap();

    // HALF ONE: every one of them really was blocked rather than restored.
    assert_eq!(
        round.restore_blocked_block_slab_ids.len() as u64,
        blocked,
        "NON-VACUOUS: all {blocked} restores must be blocked, or this is a test about restores: \
         {round:?}"
    );
    assert!(
        round.restored_block_slab_ids.is_empty(),
        "and none of them moved: {round:?}"
    );
    assert_eq!(
        round.processed_block_slabs, 0,
        "a blocked restore is not work, so none of them was charged: {round:?}"
    );

    // HALF TWO: and the round after it can still reach them.
    assert!(
        !after.declined_without_examining,
        "the next round must not decline -- the blocked slabs stayed in the directory and have \
         matured, and an earliest arrival that left them out would name a later moment: {after:?}"
    );
    assert_eq!(
        after.purged_block_slab_ids.len() as u64,
        blocked,
        "and must destroy exactly them: {after:?}"
    );
}
