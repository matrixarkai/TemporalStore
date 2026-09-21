// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a snapshot READS for each byte of applied log it DISCARDS, and what bounds it.
//!
//! #1939 measured a snapshot at 25,000 and 100,000 records and recorded, guarded but unfixed: a
//! snapshot is triggered by the LOG and reads the STORE, and those coincide exactly once -- the
//! first snapshot on a fresh store. On a 100,000-record store, discarding ONE log entry reads
//! 14,000,140 slab bytes. Per record of STORE everything is flat at 140, and that flatness is the
//! camouflage; per record of WORK it is 14,000,140.
//!
//! The READ is irreducible for the same kind of reason #1941 found the index dump's WRITE to be.
//! `a_receiving_peer_cannot_apply_an_increment` establishes it from the installers rather than by
//! assumption: every path that consumes a state image -- `rebuild_snapshot_engine`,
//! `build_installed_engine`, the restart restore -- starts from `TemporalEngine::default()` and
//! REPLACES, and none of them merges into what the receiver already holds. An image carrying only
//! what changed would therefore install as a shard containing only what changed, and the receiver
//! would serve a store with the rest silently missing. Incremental is not available without
//! changing what a receiver does, and `the_slab_read_is_what_a_snapshot_is` prices the read at
//! essentially all of the operation, so there is no second term to attack either.
//!
//! What is NOT irreducible is the CADENCE. Bytes read per byte discarded is a ratio and only its
//! numerator is the store; the denominator was a constant with no relation to the image a snapshot
//! must read to free it. `RaftConfig::snapshot_image_fraction_divisor` makes the configured
//! `max_applied_log_bytes` a FLOOR, raised to `last_image_bytes / divisor`, so a snapshot never
//! reads more than `divisor` bytes of image per byte of log it frees.
//!
//! ```text
//!   SLAB BYTES READ PER BYTE OF APPLIED LOG DISCARDED, store 10,000 -> 40,000
//!     FIXED    (control, divisor 0 -- today's cadence)   GROWS with the store
//!     RELATIVE (subject,  divisor 8)                     FLAT
//!     DISABLED (can_trigger_snapshot = false)            nothing read, nothing released
//! ```
//!
//! COUNTED, never timed, for the reason `snapshot_cost` gives: this box varies about 2.4x in wall
//! time across a day and these counters do not move with load at all.
//!
//! THE CONTROL ARM IS #1939 ITSELF. `force_the_threshold` now pins `snapshot_image_fraction_divisor
//! = 0`, so every measurement in `snapshot_cost` and `snapshot_large_store` goes on measuring the
//! constant cadence it recorded -- a recording of a defect must go on measuring the defect. Those
//! files are the FIXED arm at 25,000 and 100,000 records; this file is the RELATIVE arm beside it.

use super::snapshot_cost::{
    assert_the_fixture_is_populated, cluster_with, deployed_follower_with,
    force_the_threshold,
};
use super::*;
use crate::raft::cluster_snapshot::{
    effective_max_applied_log_bytes, SNAPSHOT_IMAGE_FRACTION_DIVISOR,
};
use crate::snapshot_probe::{self, SnapshotCounts};

/// Records of STORE, at the two sizes every measurement here is taken at. A 4.00x step, as
/// `snapshot_cost` and `snapshot_large_store` use, so a ratio is unambiguous and a per-record
/// identity is worth three significant figures.
///
/// Smaller than #1939's 25,000 / 100,000 because this file runs four rounds per size rather than
/// two snapshots, and the RELATIVE arm must write `image / divisor` bytes of log before its
/// snapshot is admitted -- which is the whole point of it. #1939's own sizes are still measured,
/// by #1939's own module, which is this file's control arm.
const STORE_SMALL: usize = 10_000;
const STORE_LARGE: usize = 40_000;

/// The configured floor every arm here runs with. A round number well above the noise of a single
/// batch and far enough below the crossover that the FIXED arm is genuinely constant at both
/// sizes: at 10,000 records the last image is about 2.0 MB and an eighth of it is about 258,000
/// bytes, so the relative term binds at both sizes and the floor binds at neither.
const FIXED_FLOOR_BYTES: u64 = 16_384;

/// Records appended between one ask of the production cadence and the next.
const BATCH: usize = 250;

/// A ceiling on how many batches one round may write before its cadence fires. A round that never
/// fires is a fixture failure, not a result, and it must say so rather than run forever.
const MAX_BATCHES_PER_ROUND: usize = 400;

/// Slab bytes a record of the fixture corpus occupies. `snapshot_cost` measures 140 at 1,000 and
/// 4,000; `snapshot_large_store` measures 140 at 25,000 and 100,000. Asserted here too.
const SLAB_BYTES_PER_RECORD: u64 = 140;

/// Which cadence a round ran under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cadence {
    /// Today's shipped behaviour: a constant byte threshold, unrelated to the image a snapshot
    /// will read. This is the CONTROL, and it fails below if it ever goes flat.
    Fixed,
    /// The subject: the configured floor raised to `last_image_bytes / divisor`.
    Relative,
}

impl Cadence {
    fn divisor(self) -> u64 {
        match self {
            Cadence::Fixed => 0,
            Cadence::Relative => SNAPSHOT_IMAGE_FRACTION_DIVISOR,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Cadence::Fixed => "FIXED   ",
            Cadence::Relative => "RELATIVE",
        }
    }
}

/// One round: write until the production cadence fires, and price the snapshot it fired.
#[derive(Debug, Clone, Copy)]
struct Round {
    cadence: Cadence,
    /// Records of STORE the shard held when the snapshot fired.
    store_records: u64,
    /// Bytes of applied log the snapshot discarded, read off the trigger report.
    released_bytes: u64,
    /// Log ENTRIES the snapshot discarded -- `applied_index - last_snapshot_index`.
    released_entries: u64,
    /// The threshold that actually decided, read off the trigger report.
    threshold_bytes: u64,
    /// The highest the applied log stood at any ask during this round. This is the trade.
    log_high_water_bytes: u64,
    counts: SnapshotCounts,
    image_payload_bytes: u64,
    image_index_bytes: u64,
    image_slabs: u64,
}

impl Round {
    /// The figure this file is about: slab bytes READ for each byte of applied log DISCARDED.
    fn read_per_byte_released(&self) -> f64 {
        self.counts.slab_read_bytes as f64 / self.released_bytes.max(1) as f64
    }

    fn read_per_record_of_store(&self) -> f64 {
        self.counts.slab_read_bytes as f64 / self.store_records.max(1) as f64
    }

    fn read_per_record_of_work(&self) -> f64 {
        self.counts.slab_read_bytes as f64 / self.released_entries.max(1) as f64
    }

    fn print(&self) {
        println!(
            "  {} store={:>7} threshold={:>10} released={:>9}B/{:>6}e read={:>10}B \
             index={:>9}B image={:>10}B slabs={:>3} log_hw={:>9}B rebuilds={} publishes={} \
             image_builds={}",
            self.cadence.label(),
            self.store_records,
            self.threshold_bytes,
            self.released_bytes,
            self.released_entries,
            self.counts.slab_read_bytes,
            self.image_index_bytes,
            self.image_payload_bytes,
            self.image_slabs,
            self.log_high_water_bytes,
            self.counts.engine_rebuilds,
            self.counts.engine_publishes,
            self.counts.image_builds,
        );
        println!(
            "            READ PER BYTE RELEASED = {:>12.3}   per record of STORE = {:>9.3}   \
             per record of WORK = {:>14.3}",
            self.read_per_byte_released(),
            self.read_per_record_of_store(),
            self.read_per_record_of_work(),
        );
    }
}

/// Append `count` further records, keeping the key shape -- and therefore the key LENGTH --
/// identical to `cluster_with`'s. A record's slab bytes carry its key, so an arm that widened the
/// key by one character would move every byte row without anything on the snapshot path changing.
fn append_records(cluster: &RaftCluster, from: usize, count: usize) {
    for index in from..from + count {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: vec![b'x'; 128],
            })
            .expect("propose must succeed");
    }
}

/// Put the cluster on `cadence` with the shared floor, and take the catch-up hold out of the way.
///
/// The hold (`max_retained_log_bytes`) is forced off exactly as `snapshot_cost` forces it off: a
/// lagging peer otherwise holds compaction and the fixture measures "nothing happened". It is a
/// production behaviour and it is not what is under test here.
fn set_cadence(cluster: &RaftCluster, cadence: Cadence, floor: u64) {
    let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
    inner.config.can_trigger_snapshot = true;
    inner.config.max_applied_log_bytes = floor;
    inner.config.max_retained_log_bytes = 0;
    inner.config.snapshot_image_fraction_divisor = cadence.divisor();
}

/// Read the image the snapshot just published, off the node it was published into.
fn installed_image(cluster: &RaftCluster) -> (u64, u64, u64) {
    let installed = cluster
        .inner
        .read()
        .expect("raft cluster lock poisoned")
        .nodes
        .get(&1)
        .and_then(|node| node.installed_snapshot.clone())
        .expect("a snapshot that triggered must leave an installed snapshot on the leader");
    let image = installed
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test");
    (
        image.payload_bytes() as u64,
        image.index_bytes.len() as u64,
        image.slabs.len() as u64,
    )
}

/// Write in batches of `BATCH` until the PRODUCTION cadence fires, then price the snapshot it
/// fired. `written` is how many records the store already holds; the new total is returned.
fn run_round(
    cluster: &RaftCluster,
    cadence: Cadence,
    floor: u64,
    written: &mut usize,
) -> (Round, usize) {
    set_cadence(cluster, cadence, floor);
    let mut high_water = 0u64;
    let mut batches = 0usize;
    loop {
        assert!(
            batches < MAX_BATCHES_PER_ROUND,
            "{} round wrote {} records without its cadence firing. That is a fixture failure, \
             not a result: every figure below divides by a snapshot that never happened",
            cadence.label(),
            batches * BATCH
        );
        append_records(cluster, *written, BATCH);
        *written += BATCH;
        batches += 1;
        // Reset immediately before the ask, so the counters hold exactly the snapshot this ask
        // performs. An ask that refuses builds no image and reads no slab, so the resets before
        // the refusals cost nothing and discard nothing.
        snapshot_probe::reset();
        let report = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        high_water = high_water.max(report.applied_log_bytes);
        if report.triggered {
            let counts = snapshot_probe::counts();
            let (payload, index_bytes, slabs) = installed_image(cluster);
            let round = Round {
                cadence,
                store_records: *written as u64,
                released_bytes: report.applied_log_bytes,
                released_entries: report
                    .applied_index
                    .checked_sub(report.last_snapshot_index)
                    .expect("applied_index must be at or above the last snapshot index"),
                threshold_bytes: report.max_applied_log_bytes,
                log_high_water_bytes: high_water,
                counts,
                image_payload_bytes: payload,
                image_index_bytes: index_bytes,
                image_slabs: slabs,
            };
            round.print();
            return (round, batches);
        }
    }
}

/// Every under-guard result #1912 and #1914 established, re-derived on this round rather than
/// inherited, plus the vacuity check that stops a zero meaning "nothing installed at all".
fn assert_the_guard_results_hold(round: &Round, where_: &str) {
    assert_eq!(
        round.counts.engine_rebuilds_under_guard, 0,
        "{where_}: #1914's result must hold under this cadence -- no engine may be rebuilt under \
         the cluster write guard, and {} were",
        round.counts.engine_rebuilds_under_guard
    );
    assert_eq!(
        round.counts.slab_install_bytes_under_guard, 0,
        "{where_}: #1912's result must hold under this cadence -- no slab byte may be written \
         under the cluster write guard, and {} were",
        round.counts.slab_install_bytes_under_guard
    );
    assert_eq!(
        round.counts.image_clone_bytes_under_guard, 0,
        "{where_}: #1912's result must hold under this cadence -- no image byte may be copied \
         under the cluster write guard, and {} were",
        round.counts.image_clone_bytes_under_guard
    );
    assert!(
        round.counts.engine_rebuilds > 0 && round.counts.engine_publishes > 0,
        "{where_}: {} rebuilds and {} publishes. A snapshot that installed into nobody would \
         satisfy every under-guard assertion above by doing nothing at all",
        round.counts.engine_rebuilds,
        round.counts.engine_publishes
    );
    assert_eq!(
        round.counts.image_builds, 1,
        "{where_}: one snapshot must build exactly one state image. More than one means the \
         three-attempt loop in create_state_image_snapshot RETRIED -- a path #1912 and #1939 both \
         measured as never taken anywhere in the tree, and one this change could reach by making \
         the build rarer and larger. If this fires, that path is UNTESTED and must be said so: {}",
        round.counts.image_builds
    );
    assert_eq!(
        round.counts.slab_reads, round.image_slabs,
        "{where_}: the build must read each carried slab exactly once -- {} reads for {} slabs",
        round.counts.slab_reads, round.image_slabs
    );
    assert_eq!(
        round.counts.slab_read_bytes,
        SLAB_BYTES_PER_RECORD * round.store_records,
        "{where_}: slab bytes read must still be exactly {SLAB_BYTES_PER_RECORD} per record of \
         STORE, and were {}. The cadence changes HOW OFTEN a snapshot runs and nothing about what \
         one reads; if this moves, the read has changed and every ratio here is against a \
         different subject",
        round.counts.slab_read_bytes
    );
}

/// THE MEASUREMENT. Both cadences, both sizes, in ABBA order on ONE store per size.
///
/// FIXED, RELATIVE, RELATIVE, FIXED -- so the store's growth through the sequence falls on both
/// cadences rather than on whichever went last, and both are priced against the same fixture.
///
/// The FIXED arm is the CONTROL and it is asserted to GROW. A control that has gone flat is an
/// apparatus that can no longer show the defect, and it would report the subject healthy for the
/// wrong reason.
#[test]
fn what_a_snapshot_reads_per_byte_of_log_it_discards_under_each_cadence() {
    let mut fixed_first = Vec::new();
    let mut relative = Vec::new();

    for store_records in [STORE_SMALL, STORE_LARGE] {
        println!("=== store={store_records} ===");
        let cluster = cluster_with(store_records);
        let fixture = assert_the_fixture_is_populated(&cluster, store_records);
        println!(
            "  fixture payload={} slabs={} alive_nodes={}",
            fixture.payload_bytes, fixture.slabs, fixture.alive_nodes
        );

        // Prime: one snapshot under the FIXED cadence, so every round below is a ROUTINE
        // snapshot with an installed image behind it. Without this the first round would be the
        // WHOLE regime -- the one case where the log discarded IS the store -- and the two
        // cadences would be measured on different regimes.
        force_the_threshold(&cluster);
        let priming = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        assert!(
            priming.triggered,
            "the priming snapshot did not fire ({}), so every round below would measure the \
             WHOLE regime rather than the routine one",
            priming.reason
        );

        let mut written = store_records;
        let mut rounds = Vec::new();
        for cadence in [
            Cadence::Fixed,
            Cadence::Relative,
            Cadence::Relative,
            Cadence::Fixed,
        ] {
            let (round, batches) = run_round(&cluster, cadence, FIXED_FLOOR_BYTES, &mut written);
            println!("            (batches written this round: {batches})");
            assert_the_guard_results_hold(
                &round,
                &format!("{} at store={}", cadence.label(), round.store_records),
            );
            rounds.push(round);
        }

        // The live set must still be more than one slab AFTER an install, because every round
        // here measures a REBUILT engine and a rebuild that collapsed the store into one slab
        // would not be expressing what the fixture was built to express. #1939's own note.
        let live_after = cluster
            .node_engine_for_test(1)
            .expect("the leader serves an engine")
            .live_block_slab_ids(1);
        println!("  live slab ids after the rounds = {live_after:?}");
        assert!(
            live_after.len() > 1,
            "the shard's live slab set after these installs is {live_after:?}: with one slab a \
             per-slab cost and a per-snapshot cost are the same number"
        );
        let tallies = cluster
            .node_engine_for_test(1)
            .expect("the leader serves an engine")
            .block_slab_live_tallies(1)
            .expect("the leader's live tally must be derived");
        let holding = tallies.iter().filter(|(_, _, bytes)| *bytes > 0).count();
        println!("  slabs holding live bytes       = {holding} of {}", tallies.len());
        assert!(
            holding > 1,
            "only {holding} slab holds live bytes: a cost spread over slabs cannot be told from \
             a cost concentrated in one"
        );

        fixed_first.push(rounds[0]);
        relative.push(rounds[1]);
    }

    let (fixed_small, fixed_large) = (fixed_first[0], fixed_first[1]);
    let (rel_small, rel_large) = (relative[0], relative[1]);

    println!("=== READ PER BYTE RELEASED, store {STORE_SMALL} -> {STORE_LARGE} ===");
    let fixed_ratio = fixed_large.read_per_byte_released() / fixed_small.read_per_byte_released();
    let rel_ratio = rel_large.read_per_byte_released() / rel_small.read_per_byte_released();
    println!(
        "  FIXED    (control)  {:>10.3} -> {:>10.3}   ({fixed_ratio:.3}x)",
        fixed_small.read_per_byte_released(),
        fixed_large.read_per_byte_released()
    );
    println!(
        "  RELATIVE (subject)  {:>10.3} -> {:>10.3}   ({rel_ratio:.3}x), divisor {}",
        rel_small.read_per_byte_released(),
        rel_large.read_per_byte_released(),
        SNAPSHOT_IMAGE_FRACTION_DIVISOR
    );

    // ---- EVERY QUANTITY, FLAT OR GROWING ----
    println!("=== EVERY QUANTITY, FLAT OR GROWING (small -> large) ===");
    for (label, small, large) in [
        (
            "slab bytes read          ",
            fixed_small.counts.slab_read_bytes,
            fixed_large.counts.slab_read_bytes,
        ),
        (
            "addresses walked         ",
            fixed_small.counts.live_slab_scan_addresses,
            fixed_large.counts.live_slab_scan_addresses,
        ),
        (
            "slab reads               ",
            fixed_small.counts.slab_reads,
            fixed_large.counts.slab_reads,
        ),
        (
            "slab directory listings  ",
            fixed_small.counts.slab_dir_listings,
            fixed_large.counts.slab_dir_listings,
        ),
        (
            "state images built       ",
            fixed_small.counts.image_builds,
            fixed_large.counts.image_builds,
        ),
        (
            "engine rebuilds          ",
            fixed_small.counts.engine_rebuilds,
            fixed_large.counts.engine_rebuilds,
        ),
        (
            "engine publishes         ",
            fixed_small.counts.engine_publishes,
            fixed_large.counts.engine_publishes,
        ),
        (
            "rebuilds under the guard ",
            fixed_small.counts.engine_rebuilds_under_guard,
            fixed_large.counts.engine_rebuilds_under_guard,
        ),
        (
            "slab bytes under guard   ",
            fixed_small.counts.slab_install_bytes_under_guard,
            fixed_large.counts.slab_install_bytes_under_guard,
        ),
        (
            "image bytes under guard  ",
            fixed_small.counts.image_clone_bytes_under_guard,
            fixed_large.counts.image_clone_bytes_under_guard,
        ),
        (
            "FIXED    released bytes  ",
            fixed_small.released_bytes,
            fixed_large.released_bytes,
        ),
        (
            "FIXED    threshold       ",
            fixed_small.threshold_bytes,
            fixed_large.threshold_bytes,
        ),
        (
            "RELATIVE released bytes  ",
            rel_small.released_bytes,
            rel_large.released_bytes,
        ),
        (
            "RELATIVE threshold       ",
            rel_small.threshold_bytes,
            rel_large.threshold_bytes,
        ),
        (
            "RELATIVE log high water  ",
            rel_small.log_high_water_bytes,
            rel_large.log_high_water_bytes,
        ),
        (
            "FIXED    log high water  ",
            fixed_small.log_high_water_bytes,
            fixed_large.log_high_water_bytes,
        ),
    ] {
        let verdict = if small == large { "FLAT" } else { "GROWING" };
        println!("  {label} {small:>12} -> {large:>12}   {verdict}");
    }
    println!(
        "  FIXED    read per byte released  {:>12.3} -> {:>12.3}   GROWING",
        fixed_small.read_per_byte_released(),
        fixed_large.read_per_byte_released()
    );
    println!(
        "  RELATIVE read per byte released  {:>12.3} -> {:>12.3}   FLAT",
        rel_small.read_per_byte_released(),
        rel_large.read_per_byte_released()
    );

    // ---- THE CONTROL ARM, WHICH FAILS IF IT GOES FLAT ----
    assert!(
        fixed_ratio > 2.0,
        "CONTROL ARM FIXED did not grow: {:.3} at store={} against {:.3} at store={}, a ratio of \
         {fixed_ratio:.3} over a {:.3}x store. This arm IS #1939's finding, kept alive so the \
         subject's flat reading keeps meaning something. A control that has gone flat is an \
         apparatus that can no longer show the defect, and the subject below would then read \
         healthy for the wrong reason",
        fixed_small.read_per_byte_released(),
        fixed_small.store_records,
        fixed_large.read_per_byte_released(),
        fixed_large.store_records,
        fixed_large.store_records as f64 / fixed_small.store_records as f64,
    );
    assert_eq!(
        fixed_small.threshold_bytes, FIXED_FLOOR_BYTES,
        "the FIXED arm's threshold must be the configured constant and nothing else: {}",
        fixed_small.threshold_bytes
    );
    assert_eq!(
        fixed_large.threshold_bytes, FIXED_FLOOR_BYTES,
        "the FIXED arm's threshold must be the configured constant and nothing else: {}",
        fixed_large.threshold_bytes
    );

    // ---- THE SUBJECT ----
    assert!(
        rel_ratio > 0.8 && rel_ratio < 1.25,
        "RELATIVE is not flat across a {:.3}x store: {:.3} -> {:.3}, a ratio of {rel_ratio:.3}. \
         The claim of this change is that the figure stops moving with the store, and it moved",
        rel_large.store_records as f64 / rel_small.store_records as f64,
        rel_small.read_per_byte_released(),
        rel_large.read_per_byte_released(),
    );
    for round in [rel_small, rel_large] {
        assert!(
            round.threshold_bytes > FIXED_FLOOR_BYTES,
            "the RELATIVE arm at store={} was decided by the configured floor ({}), not by the \
             relative term. Below the crossover this change does nothing at all, which is correct \
             behaviour and a vacuous measurement: this arm must be ABOVE it",
            round.store_records,
            round.threshold_bytes
        );
        // The image is read BEFORE the round, and the store grows by the round's own writes
        // while the threshold that admitted it stays where it was -- so the cost being divided is
        // the larger one and the figure sits a little above the divisor rather than at it. What
        // matters is that it does not move with the store, which is asserted above.
        assert!(
            round.read_per_byte_released() < (SNAPSHOT_IMAGE_FRACTION_DIVISOR * 2) as f64,
            "the RELATIVE arm at store={} read {:.3} slab bytes per byte released, against a \
             divisor of {SNAPSHOT_IMAGE_FRACTION_DIVISOR}. The bound is on the IMAGE, of which \
             the slab bytes are a part, so this figure must sit below the divisor plus the \
             round's own growth -- well inside twice it",
            round.store_records,
            round.read_per_byte_released(),
        );
    }
    assert!(
        fixed_large.read_per_byte_released() > rel_large.read_per_byte_released() * 2.0,
        "at store={} the FIXED cadence read {:.3} bytes per byte released and the RELATIVE one \
         {:.3}. With the two arms that close together this fixture is not expressing the \
         difference it was built to express",
        fixed_large.store_records,
        fixed_large.read_per_byte_released(),
        rel_large.read_per_byte_released(),
    );
}

/// WHAT THE SLAB READ IS AS A FRACTION OF THE OPERATION -- the question that decides which fix is
/// available.
///
/// #1941 found its subject was 99.43% of the dump, which is what made "cut the cadence, not the
/// work" the right call there. The same accounting here, in bytes, at both sizes: everything a
/// snapshot moves, and the share of it that is priced by the STORE rather than by the ROUND.
#[test]
fn the_slab_read_is_what_a_snapshot_is() {
    let mut rows = Vec::new();
    for store_records in [STORE_SMALL, STORE_LARGE] {
        let cluster = cluster_with(store_records);
        assert_the_fixture_is_populated(&cluster, store_records);
        force_the_threshold(&cluster);
        snapshot_probe::reset();
        let report = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        assert!(report.triggered, "the snapshot did not fire: {}", report.reason);
        let counts = snapshot_probe::counts();
        let (payload, index_bytes, slabs) = installed_image(&cluster);

        // Everything a snapshot MOVES, in bytes. The read is the build's half; the installs are
        // what the rebuilt engines write; the copies are the per-node images.
        let read = counts.slab_read_bytes;
        let encoded = index_bytes;
        let installed = counts.slab_install_bytes;
        let copied = counts.image_clone_bytes;
        let total = read + encoded + installed + copied;
        println!("=== WHAT A SNAPSHOT MOVES, store={store_records} ===");
        println!("  {read:>12} B  slab bytes READ by the build");
        println!("  {encoded:>12} B  served index ENCODED by the build");
        println!("  {installed:>12} B  slab bytes INSTALLED into the rebuilt engines");
        println!("  {copied:>12} B  image bytes COPIED per install target");
        println!("  {total:>12} B  total");
        println!(
            "  THE BUILD'S WHOLE-STORE READ IS {:.2}% OF THE BYTES, AND THE SLAB READ ALONE IS \
             {:.2}%",
            (read + encoded) as f64 * 100.0 / total.max(1) as f64,
            read as f64 * 100.0 / total.max(1) as f64,
        );
        println!(
            "  image payload = {payload} B over {slabs} slabs; every one of these four terms is \
             priced by the STORE"
        );
        assert!(total > 0, "a snapshot that moved no bytes measures nothing");
        assert_eq!(
            read,
            SLAB_BYTES_PER_RECORD * store_records as u64,
            "the read must be {SLAB_BYTES_PER_RECORD} slab bytes per record of store"
        );
        rows.push((store_records as u64, read, encoded, installed, copied, total));
    }

    // Every term scales with the store, which is the finding: there is no second term bounded by
    // the round, so there is nothing to remove from the work. Asserted as a ratio against the
    // store ratio rather than as a trend.
    let (small, large) = (rows[0], rows[1]);
    let store_ratio = large.0 as f64 / small.0 as f64;
    for (label, s, l) in [
        ("slab read     ", small.1, large.1),
        ("index encode  ", small.2, large.2),
        ("slab installs ", small.3, large.3),
        ("image copies  ", small.4, large.4),
        ("total         ", small.5, large.5),
    ] {
        let ratio = l as f64 / s.max(1) as f64;
        println!("  {label} {s:>12} -> {l:>12}   {ratio:.3}x against a store ratio of {store_ratio:.3}x");
        assert!(
            ratio > store_ratio * 0.75,
            "{label} grew only {ratio:.3}x over a {store_ratio:.3}x store. If some term of a \
             snapshot were bounded by the ROUND rather than by the store there would be something \
             to remove from the work, and this file's argument for cutting the cadence instead \
             would not hold"
        );
    }
    assert_eq!(
        small.1 * large.0,
        large.1 * small.0,
        "the slab read is not exactly proportional to the store: {} at {} against {} at {}",
        small.1,
        small.0,
        large.1,
        large.0
    );
}

/// WHY INCREMENTAL IS NOT AVAILABLE -- established from the installers, not assumed.
///
/// Every path that consumes a state image builds a FRESH `TemporalEngine` and replaces with it;
/// none merges into what the receiver already holds. So an image carrying only what changed
/// installs as a shard containing only what changed, and the receiver serves a store with the
/// rest silently missing -- which is precisely the failure a snapshot exists to prevent and the
/// one nothing would report.
///
/// Constructed rather than argued: a snapshot is built, its image is reduced to a PROPER SUBSET of
/// its slabs, an engine is rebuilt from it, and the records in the dropped slab are asked for.
#[test]
fn a_receiving_peer_cannot_apply_an_increment() {
    let records = 2_000usize;
    let cluster = cluster_with(records);
    assert_the_fixture_is_populated(&cluster, records);

    let whole = cluster.create_snapshot().expect("create_snapshot must succeed");
    let image = whole
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test");
    assert!(
        image.slabs.len() > 1,
        "the image carries {} slab(s): a PROPER subset of one slab is the empty set, and this \
         test would be dropping everything rather than dropping an increment's complement",
        image.slabs.len()
    );

    // A peer built from the WHOLE image serves every record. This is the positive control: it is
    // what makes the negative below a statement about the missing slab rather than about the
    // rebuild being broken in general.
    let whole_peer = crate::raft::rebuild_snapshot_engine(&whole);
    let mut served_by_whole = 0usize;
    for index in 0..records {
        if read_back(&whole_peer, &format!("key-{index:08}")).is_some() {
            served_by_whole += 1;
        }
    }
    println!("  whole image: {served_by_whole} of {records} records served");
    assert_eq!(
        served_by_whole, records,
        "a peer rebuilt from the WHOLE image serves only {served_by_whole} of {records} records, \
         so nothing below can be attributed to the slab that was dropped"
    );

    // Now drop ONE slab -- the smallest possible "carry only what changed" -- and rebuild.
    let mut partial = whole.clone();
    let dropped = partial
        .state_image
        .as_mut()
        .expect("the image is present")
        .slabs
        .pop()
        .expect("more than one slab, so one can be dropped");
    println!(
        "  dropped slab {} of {} bytes from the image",
        dropped.block_slab_id,
        dropped.bytes.len()
    );
    let partial_peer = crate::raft::rebuild_snapshot_engine(&partial);
    let mut served_by_partial = 0usize;
    for index in 0..records {
        if read_back(&partial_peer, &format!("key-{index:08}")).is_some() {
            served_by_partial += 1;
        }
    }
    println!("  partial image: {served_by_partial} of {records} records served");
    assert!(
        served_by_partial < served_by_whole,
        "a peer rebuilt from an image missing one slab served the same {served_by_partial} \
         records as one rebuilt from the whole image. If that were so, an image could carry only \
         what changed and this change would have been the wrong shape"
    );
    println!(
        "  INCREMENTAL IS NOT AVAILABLE: the install REPLACES, so an image carrying only what \
         changed loses {} records and says nothing about it",
        served_by_whole - served_by_partial
    );
}

/// Read one key back off an engine, element by element rather than by count.
fn read_back(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: key.to_string(),
            },
        })
        .response
    {
        CommandResponse::Bytes { value } => value,
        other => panic!("a StringGet must answer with bytes, and answered {other:?}"),
    }
}

/// CORRECTNESS IS THE BINDING CONSTRAINT.
///
/// A snapshot exists so a peer can catch up, and a snapshot that omits something a peer needs is
/// SILENT: the peer diverges and nothing says so. So the strong form is asserted -- a peer is
/// built from what the paced cadence produced and its served state is compared to the leader's
/// ELEMENT BY ELEMENT, at several points, including immediately after a restart.
#[test]
fn a_peer_built_from_a_paced_snapshot_serves_the_leaders_elements_exactly() {
    let records = 3_000usize;
    let cluster = cluster_with(records);
    assert_the_fixture_is_populated(&cluster, records);
    force_the_threshold(&cluster);
    assert!(
        cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed")
            .triggered,
        "the priming snapshot must fire"
    );

    let mut written = records;
    let mut checkpoints = 0usize;
    for round in 0..3 {
        let (measured, _) = run_round(&cluster, Cadence::Relative, FIXED_FLOOR_BYTES, &mut written);
        assert_the_guard_results_hold(&measured, &format!("correctness round {round}"));

        // The leader, and a peer that took the very snapshot this round produced.
        let (leader, peer) = {
            let inner = cluster.inner.read().expect("raft cluster lock poisoned");
            (
                inner.nodes.get(&1).expect("node 1").engine.clone(),
                inner.nodes.get(&2).expect("node 2").engine.clone(),
            )
        };
        compare_element_by_element(&leader, &peer, written, &format!("round {round}"));
        checkpoints += 1;

        // AND AFTER A RESTART. The image is what a restart restores from, so a snapshot that is
        // right on the wire and wrong on disk would pass everything above.
        let restored =
            crate::raft::rebuild_snapshot_engine(
                cluster
                    .inner
                    .read()
                    .expect("raft cluster lock poisoned")
                    .nodes
                    .get(&2)
                    .expect("node 2")
                    .installed_snapshot
                    .as_ref()
                    .expect("the peer holds the snapshot it installed"),
            );
        compare_element_by_element(&leader, &restored, written, &format!("round {round} restart"));
        checkpoints += 1;
    }
    assert_eq!(
        checkpoints, 6,
        "the comparison must have run at every checkpoint; it ran {checkpoints} times"
    );
}

/// Compare two engines ELEMENT BY ELEMENT -- every key's value, and then the live structure that
/// would reveal an element the leader does not have.
fn compare_element_by_element(
    leader: &TemporalEngine,
    peer: &TemporalEngine,
    records: usize,
    where_: &str,
) {
    let mut compared = 0usize;
    for index in 0..records {
        let key = format!("key-{index:08}");
        let want = read_back(leader, &key);
        let got = read_back(peer, &key);
        assert_eq!(
            want, got,
            "{where_}: the peer does not serve key {key} as the leader does. A snapshot that \
             omits something a peer needs is SILENT -- the peer diverges and nothing reports it"
        );
        assert!(
            want.is_some(),
            "{where_}: the LEADER does not serve key {key} either, so this comparison is between \
             two absences and would pass on an empty pair of engines"
        );
        compared += 1;
    }
    assert_eq!(
        compared, records,
        "{where_}: only {compared} of {records} elements were compared"
    );

    // And the other direction: an element the peer has and the leader does not would not show up
    // above. The live tally is the structure the served index maintains, per slab.
    let leader_live = leader
        .block_slab_live_tallies(1)
        .expect("the leader's live tally must be derived");
    let peer_live = peer
        .block_slab_live_tallies(1)
        .expect("the peer's live tally must be derived");
    let leader_refs: u64 = leader_live.iter().map(|(_, refs, _)| *refs).sum();
    let peer_refs: u64 = peer_live.iter().map(|(_, refs, _)| *refs).sum();
    let leader_bytes: u64 = leader_live.iter().map(|(_, _, bytes)| *bytes).sum();
    let peer_bytes: u64 = peer_live.iter().map(|(_, _, bytes)| *bytes).sum();
    println!(
        "  {where_}: compared {compared} elements; live refs {leader_refs} vs {peer_refs}, live \
         bytes {leader_bytes} vs {peer_bytes}"
    );
    assert!(
        leader_refs > 0 && leader_bytes > 0,
        "{where_}: the leader's live tally is {leader_refs} refs / {leader_bytes} bytes, so an \
         equality against it would be an equality of two zeroes"
    );
    assert_eq!(
        leader_refs, peer_refs,
        "{where_}: the peer holds {peer_refs} live page refs against the leader's {leader_refs}. \
         An element the peer has and the leader does not cannot be found by asking for the \
         leader's keys, and this is where it shows"
    );
    assert_eq!(
        leader_bytes, peer_bytes,
        "{where_}: the peer holds {peer_bytes} live bytes against the leader's {leader_bytes}"
    );
}

/// THE INTEGRAL: what a store READS in total to take the same work, under each cadence, with a
/// DISABLED arm so the subject is not graded against nothing.
///
/// Three arms write exactly the same records past an identical store, ask the production cadence
/// after every batch, and sum every slab byte every snapshot read.
#[test]
fn what_a_store_reads_in_total_for_the_same_work_under_each_cadence() {
    const BASE: usize = 5_000;
    const WORK: usize = 8_000;
    // Chosen so the relative term BINDS over this whole range rather than only at the end of
    // it: an eighth of the image at 5,000 records is about 124,000 bytes, so a floor above that
    // would leave the first rounds of both arms running the same cadence and the comparison
    // would be of a cadence against itself over part of its range. Below the crossover this
    // change does nothing at all, which is correct behaviour and a vacuous measurement.
    const INTEGRAL_FLOOR: u64 = 32_768;

    #[derive(Debug, Clone, Copy)]
    struct Arm {
        snapshots: u64,
        read_bytes: u64,
        released_bytes: u64,
        log_high_water: u64,
    }

    let mut arms = Vec::new();
    for (label, cadence, enabled) in [
        ("FIXED    (today)", Cadence::Fixed, true),
        ("RELATIVE (change)", Cadence::Relative, true),
        ("DISABLED         ", Cadence::Fixed, false),
    ] {
        let cluster = cluster_with(BASE);
        assert_the_fixture_is_populated(&cluster, BASE);
        // PRIME THE ENABLED ARMS. The first snapshot on a fresh store is the WHOLE regime -- it
        // discards one entry per record of store, so it releases the whole base corpus in one go
        // and sets a log high-water mark neither cadence will reach again. It is the same
        // snapshot in both arms and would add the same constant to both totals; leaving it in
        // hides the steady-state footprint, which is the TRADE this change is bought with. The
        // DISABLED arm is deliberately NOT primed: its whole point is that nothing is ever
        // released, and its log therefore still holds the base corpus.
        if enabled {
            force_the_threshold(&cluster);
            assert!(
                cluster
                    .maybe_trigger_snapshot()
                    .expect("maybe_trigger_snapshot must succeed")
                    .triggered,
                "the priming snapshot for the {label} arm did not fire, so its rounds below                  would not all be routine ones"
            );
        }
        set_cadence(&cluster, cadence, INTEGRAL_FLOOR);
        {
            let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
            inner.config.can_trigger_snapshot = enabled;
        }
        let mut snapshots = 0u64;
        let mut read_bytes = 0u64;
        let mut released_bytes = 0u64;
        let mut log_high_water = 0u64;
        let mut written = BASE;
        while written < BASE + WORK {
            append_records(&cluster, written, BATCH);
            written += BATCH;
            snapshot_probe::reset();
            let report = cluster
                .maybe_trigger_snapshot()
                .expect("maybe_trigger_snapshot must succeed");
            log_high_water = log_high_water.max(report.applied_log_bytes);
            if report.triggered {
                snapshots += 1;
                read_bytes += snapshot_probe::counts().slab_read_bytes;
                released_bytes += report.applied_log_bytes;
            }
        }
        let arm = Arm {
            snapshots,
            read_bytes,
            released_bytes,
            log_high_water,
        };
        println!(
            "  {label}  snapshots={:>3}  read={:>12} B  released={:>10} B  cumulative read per \
             byte released={:>10.3}  log high water={:>10} B",
            arm.snapshots,
            arm.read_bytes,
            arm.released_bytes,
            arm.read_bytes as f64 / arm.released_bytes.max(1) as f64,
            arm.log_high_water,
        );
        arms.push(arm);
    }

    let (fixed, relative, disabled) = (arms[0], arms[1], arms[2]);
    assert_eq!(
        disabled.snapshots, 0,
        "the DISABLED arm fired {} snapshots; it is here to show what the absence looks like",
        disabled.snapshots
    );
    assert_eq!(
        disabled.read_bytes, 0,
        "the DISABLED arm read {} slab bytes with snapshots off",
        disabled.read_bytes
    );
    assert!(
        disabled.log_high_water > relative.log_high_water,
        "the DISABLED arm's log stood at {} B against the RELATIVE arm's {} B. With nothing \
         shedding it must stand higher than either cadence, and if it does not, the arms are not \
         doing the same work",
        disabled.log_high_water,
        relative.log_high_water
    );
    assert!(
        fixed.snapshots > relative.snapshots,
        "FIXED fired {} snapshots and RELATIVE {} over the same work. The whole mechanism is that \
         the relative cadence fires less often as the store grows",
        fixed.snapshots,
        relative.snapshots
    );
    assert!(
        fixed.read_bytes > relative.read_bytes * 2,
        "over the same {WORK} records of work past the same {BASE}-record store, FIXED read {} B \
         and RELATIVE {} B. Less than a 2x separation and this fixture is not expressing the \
         difference",
        fixed.read_bytes,
        relative.read_bytes
    );
    println!(
        "  RELATIVE reads {:.2}x fewer bytes for the same work, bought with a log standing at {} \
         B between snapshots instead of {} B",
        fixed.read_bytes as f64 / relative.read_bytes.max(1) as f64,
        relative.log_high_water,
        fixed.log_high_water,
    );
    assert!(
        relative.log_high_water > fixed.log_high_water,
        "the RELATIVE arm's log stood at {} B and the FIXED arm's at {} B. Holding MORE log \
         between snapshots is what this change is bought with; an arm that does not is not \
         paying the price this file claims it pays, and the trade is being reported wrong",
        relative.log_high_water,
        fixed.log_high_water
    );

    // #1939's closed form, asserted in the tree against its own constant rather than quoted. At
    // one snapshot per `step` records, reaching `step * R` records costs
    // `140 * step * R * (R + 1) / 2` slab bytes across R snapshots.
    for (step, rounds, expected) in [(1_000u64, 10u64, 7_700_000u64), (1_000, 20, 29_400_000)] {
        let closed = SLAB_BYTES_PER_RECORD * step * rounds * (rounds + 1) / 2;
        let summed: u64 = (1..=rounds).map(|r| SLAB_BYTES_PER_RECORD * step * r).sum();
        println!(
            "  integral check: {rounds} rounds of {step} -> closed form {closed}, summed {summed}"
        );
        assert_eq!(
            closed, expected,
            "#1939's published integral for {rounds} rounds of {step} records is {expected}; the \
             closed form evaluates to {closed}. A sibling published this identity wrong by a \
             factor of 1,000 and it stood in two reports, so it is arithmetic in the tree here"
        );
        assert_eq!(
            summed, closed,
            "the summed series and the closed form disagree: {summed} against {closed}"
        );
    }
}

/// THE PROPERTIES OF THE BOUND, each a way this could have been wrong, each asserted and each
/// mutated.
#[test]
fn a_cadence_an_operator_pinned_stays_pinned_at_every_image_size() {
    // PIN THE DIVISOR. Every arm in this file reads the constant for both the treatment and the
    // expectation, so without this pin a change to it would move both sides and no assertion
    // could see it go. The divisor IS the trade: halving it doubles the retained log.
    assert_eq!(
        SNAPSHOT_IMAGE_FRACTION_DIVISOR, 8,
        "SNAPSHOT_IMAGE_FRACTION_DIVISOR is the TRADE this change makes: a snapshot reads at most \
         this many bytes of image per byte of log it frees, and the log stands at up to \
         image/this between snapshots. Changing it moves the measured footprint in this file's \
         integral arm, which must be re-read beside it"
    );
    assert_eq!(
        RaftConfig::default().snapshot_image_fraction_divisor,
        SNAPSHOT_IMAGE_FRACTION_DIVISOR,
        "the shipped default must be the constant, or the measurements here are of a cadence \
         nothing runs"
    );

    // A FLOOR AND NEVER A CEILING. The relative term can only ever ask for MORE.
    for floor in [1u64, 16_384, 1 << 20, 1 << 30] {
        for image in [0u64, 1, 1 << 10, 1 << 20, 1 << 30, u64::MAX] {
            let effective = effective_max_applied_log_bytes(floor, image, 8);
            assert!(
                effective >= floor,
                "floor {floor} with image {image} produced {effective}, which is BELOW the \
                 configured floor. The relative term may only ever make a snapshot wait for more"
            );
        }
    }

    // A ZERO CONFIGURED FLOOR IS AN OPERATOR PINNING THE CADENCE, and it stays pinned at every
    // image size. Zero means "fire on any new applied entry": the comparison it feeds is
    // `applied_log_bytes < threshold`, which no byte count is below at zero. Without this guard
    // the relative term -- a max against a quantity that grows without bound -- would silently
    // STOP that operator's snapshots on exactly the largest shards.
    for image in [0u64, 1 << 10, 1 << 20, 1 << 30, u64::MAX] {
        assert_eq!(
            effective_max_applied_log_bytes(0, image, 8),
            0,
            "a zero configured floor with an image of {image} produced a non-zero threshold, so \
             an operator who pinned the cadence has had it un-pinned by the size of their store"
        );
    }

    // A ZERO DIVISOR RESTORES THE CONSTANT THRESHOLD EXACTLY. This is what the FIXED control arm
    // runs, and what `force_the_threshold` pins so #1939's recording goes on measuring #1939's
    // defect.
    for floor in [1u64, 16_384, 1 << 30] {
        for image in [0u64, 1 << 20, u64::MAX] {
            assert_eq!(
                effective_max_applied_log_bytes(floor, image, 0),
                floor,
                "a zero divisor must return the configured floor {floor} unchanged, whatever the \
                 image; it is how the control arm is constructed"
            );
        }
    }

    // AND IT DIVIDES. A term that never binds would satisfy every assertion above.
    assert_eq!(
        effective_max_applied_log_bytes(16_384, 20_660_688, 8),
        2_582_586,
        "the relative term must be image/divisor once it exceeds the floor. 20,660,688 is the \
         whole image bytes #1939 measured at 100,000 records"
    );
    assert_eq!(
        effective_max_applied_log_bytes(16_384, 5_151_207, 8),
        643_900,
        "the same at #1939's 25,000-record image of 5,151,207 bytes"
    );
    assert!(
        effective_max_applied_log_bytes(1 << 20, 5_151_207, 8) == 1 << 20,
        "a 1 MiB floor is above an eighth of a 5,151,207-byte image, so the floor must decide and \
         nothing below the crossover changes behaviour at all"
    );
}

/// AN INDEPENDENT RESIDUAL, with planted-marker recovery.
///
/// The counters above are bumped inside the primitives a snapshot uses, so they cannot see work a
/// snapshot does through some path nobody instrumented. This reads the kernel's own tally for
/// THIS THREAD -- `/proc/thread-self/io`, not `/proc/self/io`, which is process-wide and would
/// carry every other test's reads on a parallel run -- and asks whether the bytes the counters
/// attribute to the build account for the bytes the thread actually read.
///
/// A zero here would be an identity dressed as a result, so a marker is planted for it: a file of
/// known size is read inside a fresh measured span, and the instrument must recover exactly it.
#[test]
fn an_independent_residual_recovers_a_planted_marker() {
    fn thread_rchar() -> u64 {
        let text = std::fs::read_to_string("/proc/thread-self/io")
            .expect("this box must expose /proc/thread-self/io");
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("rchar: ") {
                return rest.trim().parse().expect("rchar must be a number");
            }
        }
        panic!("no rchar line in /proc/thread-self/io");
    }

    // THE CONTROL FIRST, so it cannot be skipped by an earlier failure. A marker of known size is
    // read inside a measured span and must be recovered to the byte.
    const MARKER_BYTES: usize = 350_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let marker = dir.path().join("planted-marker.bin");
    std::fs::write(&marker, vec![b'm'; MARKER_BYTES]).expect("write the marker");
    let before = thread_rchar();
    let read = std::fs::read(&marker).expect("read the marker");
    let after = thread_rchar();
    assert_eq!(read.len(), MARKER_BYTES);
    let recovered = after - before;
    println!("  planted marker: {MARKER_BYTES} B, instrument recovered {recovered} B");
    assert!(
        recovered >= MARKER_BYTES as u64,
        "the planted {MARKER_BYTES}-byte read was recovered as {recovered} bytes. An instrument \
         that cannot see a read it was handed cannot be believed when it reads zero"
    );
    assert!(
        recovered < MARKER_BYTES as u64 * 2,
        "the instrument attributed {recovered} bytes to a {MARKER_BYTES}-byte read, so it is \
         carrying work that is not the subject's"
    );

    // NOW THE SUBJECT. One snapshot, with both instruments around the same span.
    let records = 4_000usize;
    let cluster = cluster_with(records);
    assert_the_fixture_is_populated(&cluster, records);
    force_the_threshold(&cluster);
    snapshot_probe::reset();
    let before = thread_rchar();
    let report = cluster
        .maybe_trigger_snapshot()
        .expect("maybe_trigger_snapshot must succeed");
    let after = thread_rchar();
    assert!(report.triggered, "the snapshot did not fire: {}", report.reason);
    let counts = snapshot_probe::counts();
    let kernel = after - before;
    let counted = counts.slab_read_bytes;
    println!(
        "  snapshot at {records} records: kernel rchar {kernel} B, counted slab reads {counted} B"
    );
    assert!(
        kernel > 0,
        "the kernel attributed ZERO bytes to a snapshot that read {counted} counted bytes. That \
         is the reading this campaign says to distrust, and the planted marker above shows the \
         instrument works, so a zero here is a span in the wrong place"
    );
    assert!(
        kernel >= counted,
        "the kernel saw {kernel} bytes and the counters claim {counted}. The counters cannot \
         exceed the kernel's own tally of the same thread's reads; if they do, they are counting \
         something that was never read"
    );
    let residual = kernel - counted;
    println!(
        "  residual (kernel - counted) = {residual} B, {:.2}% of the counted read",
        residual as f64 * 100.0 / counted.max(1) as f64
    );

    // AND THE RESIDUAL IS ATTRIBUTED, not merely printed. A residual left standing is a theory
    // nobody tested. The hypothesis: every engine the install rebuilds calls `load_shard` after
    // `install_index_bytes`, which READS that served index back in -- so the bytes the slab
    // counter does not cover should be about one index per rebuilt engine, and NOT a second walk
    // of the corpus, which would add at least another `counted` bytes on top.
    let (_, index_bytes, _) = installed_image(&cluster);
    let predicted = index_bytes * counts.engine_rebuilds;
    println!(
        "  attribution: {} rebuilt engines x {index_bytes} B of served index = {predicted} B,          against a residual of {residual} B ({:.3}x)",
        counts.engine_rebuilds,
        residual as f64 / predicted.max(1) as f64
    );
    assert!(
        counts.engine_rebuilds > 1 && index_bytes > 0,
        "the attribution divides by {} rebuilds and {index_bytes} index bytes, so it would be an          identity rather than a prediction",
        counts.engine_rebuilds
    );
    assert!(
        residual > predicted * 7 / 10 && residual < predicted * 3 / 2,
        "the residual is {residual} B against a predicted {predicted} B -- one served-index read          per rebuilt engine. Outside that band the residual is something else, and in particular          a residual near {} B would be a SECOND walk of the corpus that none of the counters in          this file can see",
        predicted + counted
    );
}

/// THE SECOND LIVE COPY. A deployed FOLLOWER compacts its own log on the same threshold, off its
/// own engine, and pays the identical whole-shard read for it -- so a bound applied to only one of
/// the two call sites would leave the other with the defect. #1914 had to fix the follower's
/// rebuild separately for exactly this reason.
///
/// The threshold is not assumed here, it is READ: after the first compaction the follower's own
/// installed image is measured, an eighth of it is computed, and the follower is then asked once
/// per small batch. It must refuse every ask below that figure -- including asks well above the
/// constant threshold, which is what proves the relative term and not the floor is deciding --
/// and fire at the first ask at or above it.
#[test]
fn a_deployed_followers_compaction_is_paced_by_the_same_bound() {
    const ENTRIES: u64 = 2_000;
    const FLOOR: u64 = 4_096;
    const STEP: u64 = 10;

    let (_dir, follower) = deployed_follower_with(ENTRIES);
    {
        let mut inner = follower.inner.write().expect("raft cluster lock poisoned");
        inner.config.max_applied_log_bytes = FLOOR;
        inner.config.max_retained_log_bytes = 0;
        inner.config.snapshot_image_fraction_divisor = SNAPSHOT_IMAGE_FRACTION_DIVISOR;
    }

    // The FIRST compaction on a node with no installed image: the floor decides alone, exactly as
    // it does today. This is also the vacuity floor -- a follower that never compacts at all
    // satisfies every refusal below.
    let first = follower
        .maybe_trigger_snapshot()
        .expect("maybe_trigger_snapshot must succeed");
    assert!(
        first.triggered && first.reason == "follower_applied_log_bytes_threshold",
        "the first follower compaction did not fire ({}), so nothing below is measuring this \
         branch at all",
        first.reason
    );
    let compacted_to = follower_snapshot_index(&follower);
    assert_eq!(
        compacted_to, ENTRIES,
        "the follower must compact to its applied index, and compacted to {compacted_to}"
    );

    // Read the bound rather than assume it.
    let image_bytes = follower_image_bytes(&follower);
    let expected = effective_max_applied_log_bytes(
        FLOOR,
        image_bytes,
        SNAPSHOT_IMAGE_FRACTION_DIVISOR,
    );
    println!(
        "  follower image {image_bytes} B -> threshold {expected} B against a floor of {FLOOR} B"
    );
    assert!(
        expected > FLOOR * 4,
        "the follower's relative threshold is {expected} B against a floor of {FLOOR} B. Below \
         the crossover the floor decides and this test would be measuring the floor"
    );

    let mut next = ENTRIES + 1;
    let mut refusals_above_the_floor = 0usize;
    let mut fired_at = None;
    while next <= ENTRIES + 40 * STEP {
        append_to_follower(&follower, next, STEP);
        next += STEP;
        let log_bytes = follower_log_bytes(&follower);
        let before = follower_snapshot_index(&follower);
        let _ = follower.maybe_trigger_snapshot();
        let after = follower_snapshot_index(&follower);
        if after > before {
            println!("  FIRED at log {log_bytes} B (threshold {expected} B)");
            assert!(
                log_bytes >= expected,
                "the follower compacted with only {log_bytes} B of log against a threshold of \
                 {expected} B, so the bound is not the one it is being asked for"
            );
            fired_at = Some(log_bytes);
            break;
        }
        assert!(
            log_bytes < expected,
            "the follower REFUSED at {log_bytes} B, which is at or above its {expected} B \
             threshold. A bound that refuses above itself is not a floor being raised, it is a \
             cadence that has stopped"
        );
        if log_bytes > FLOOR {
            refusals_above_the_floor += 1;
        }
    }
    let fired_at = fired_at.expect(
        "the follower never compacted again within 40 batches. A cadence that never fires is not \
         a paced one",
    );
    println!(
        "  refusals above the constant floor: {refusals_above_the_floor}, fired at {fired_at} B"
    );
    assert!(
        refusals_above_the_floor > 5,
        "only {refusals_above_the_floor} ask(s) were refused while the log stood above the \
         constant {FLOOR} B floor. Those refusals ARE the change on this call site: without them \
         the floor and the relative term are indistinguishable here"
    );
}

/// Append `count` further entries to a deployed follower by RPC, keeping the key shape -- and so
/// the key LENGTH -- identical to `deployed_follower_with`'s.
fn append_to_follower(follower: &RaftCluster, from: u64, count: u64) {
    for index in from..from + count {
        let response = follower
            .receive_append_entries(AppendEntriesRequest {
                rpc: None,
                shard_id: 1,
                term: 1,
                leader_id: 1,
                target_id: 2,
                prev_log_index: index - 1,
                prev_log_term: 1,
                entries: vec![RaftLogEntry {
                    leader_time_ms: 0,
                    term: 1,
                    index,
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("follower-{index:04}"),
                        value: vec![b'y'; 128],
                    },
                }],
                leader_commit: index,
            })
            .expect("append must succeed");
        assert!(response.success, "append {index} was rejected");
    }
}

/// The index the follower has compacted to, read off its own installed snapshot.
fn follower_snapshot_index(follower: &RaftCluster) -> u64 {
    follower
        .inner
        .read()
        .expect("raft cluster lock poisoned")
        .nodes
        .get(&2)
        .expect("node 2")
        .installed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.last_included_index)
        .unwrap_or_default()
}

/// The payload of the image the follower's own installed snapshot carries.
fn follower_image_bytes(follower: &RaftCluster) -> u64 {
    follower
        .inner
        .read()
        .expect("raft cluster lock poisoned")
        .nodes
        .get(&2)
        .expect("node 2")
        .installed_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.state_image.as_ref())
        .map(|image| image.payload_bytes() as u64)
        .unwrap_or_default()
}

/// The follower's applied log above what it has already compacted -- the quantity its own cadence
/// weighs, computed here the same way the cadence computes it.
///
/// Which is NOT the logical command bytes. A node that keeps a WAL judges the threshold by the log
/// ON DISK, because the logical measure understates the footprint by the whole encoding overhead,
/// and it takes the LARGER of the two. Calibrating against the logical measure alone reads a
/// compaction that fired exactly on its bound as one that fired below it.
fn follower_log_bytes(follower: &RaftCluster) -> u64 {
    let inner = follower.inner.read().expect("raft cluster lock poisoned");
    let node = inner.nodes.get(&2).expect("node 2");
    let last = node
        .installed_snapshot
        .as_ref()
        .map(|snapshot| snapshot.last_included_index)
        .unwrap_or_default();
    let logical = crate::raft::raft_log_bytes_after(&node.log, last);
    inner
        .wal
        .as_ref()
        .map(|wal| wal.node_log_bytes_after(inner.shard_id, 2, last))
        .unwrap_or(0)
        .max(logical)
}
