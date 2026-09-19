// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one raft snapshot COSTS at scale, counted inside the primitives that do the work.
//!
//! A snapshot here is a STATE IMAGE: the shard's served index plus every slab it references,
//! materialised as owned bytes. Its size is set by the STORE, not by the request, which makes the
//! interesting questions about it questions of multiplicity -- how many times is the corpus
//! walked, how many copies of the shard exist at once, and which of those happen while the raft
//! cluster write guard is held. That guard is the half of the lock every `propose` needs, so work
//! under it is not merely slow, it is unavailable.
//!
//! COUNTED, never timed. The same test on this box varies about 2.4x in wall time across one day
//! and the box sits anywhere between load 1 and 30; the quantities below -- walks, copies, reads,
//! rebuilds -- do not move with load at all. Everything runs at TWO corpus sizes and the RATIO is
//! asserted with both denominators printed, so a counter that has quietly stopped incrementing
//! reads as a failure and not as a perfect result.
//!
//! The in-order trace of one `maybe_trigger_snapshot` on a 3-node in-process cluster, measured
//! here at 2,000 and 8,000 records of 128 bytes:
//!
//! ```text
//!                                       2,000        8,000     ratio   per record
//!   BUILD, holding no cluster guard
//!     state image builds                    1            1      1.00
//!     corpus walks for live slab ids        1            1      1.00
//!     block addresses visited           2,000        8,000      4.00        1.000
//!     served-index encodes                  1            1      1.00
//!     index bytes                     100,875      498,593      4.94
//!     slab directory listings               1            1      1.00
//!     slab reads                            1            1      1.00
//!     slab bytes read                 280,000    1,120,000      4.00          140
//!     encodes under a shard guard           0            0         -
//!   INSTALL, all of it under the cluster WRITE guard
//!     whole-shard image copies              3            3      1.00
//!     image bytes copied            1,142,625    4,855,779      4.25
//!     engine rebuilds                       3            3      1.00
//!     slab bytes re-installed         840,000    3,360,000      4.00
//! ```
//!
//! The build is one pass: one walk, one encode, one listing, one read per slab, nothing under any
//! guard. The INSTALL was three whole copies of the shard and three whole engine rebuilds, every
//! byte of it inside the cluster write guard. One of those three copies was pure waste -- the
//! snapshot is dropped the moment the loop ends, so the last install can have the original -- and
//! in a DEPLOYED process, which installs into exactly one node, that one was the only copy there
//! was. Moving it is what `the_last_install_takes_the_image_instead_of_copying_it` guards.

use super::*;
use crate::snapshot_probe::{self, SnapshotCounts};

/// Records per corpus. The pair is a 4.00x step, small enough that the whole file runs inside the
/// ordinary gate on a loaded box and large enough that a per-record identity is worth three
/// significant figures.
const SMALL: usize = 1_000;
const LARGE: usize = 4_000;

/// How many slabs the corpus is spread over.
///
/// The slab target is a gibibyte, so a corpus any test can afford to write lands entirely in slab
/// zero -- and then the live set is `{0}`, the store's set is `{0}`, "one read per slab" is "one
/// read", and every per-slab multiplicity below is indistinguishable from a per-snapshot one. The
/// records are rolled onto fresh slabs as they are written so the image genuinely spans several,
/// and `assert_the_fixture_is_populated` refuses to measure a fixture that collapsed back to one.
const SLABS: usize = 4;

/// A leader with `records` committed and applied, each a 128-byte value under a 12-byte key,
/// spread over `SLABS` slabs.
fn cluster_with(records: usize) -> RaftCluster {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let roll_every = (records / SLABS).max(1);
    let mut index = 0usize;
    while index < records {
        if index > 0 && index % roll_every == 0 {
            // Target 1 byte: the production target is a gibibyte and is reachable only through a
            // process-wide env var, which is exactly what `prepare_next_slab_with_target` exists
            // to avoid mutating. `Some` is required, so a roll that silently did not happen fails
            // here rather than showing up as a one-slab image three assertions later.
            assert!(
                cluster
                    .node_engine_for_test(1)
                    .expect("the leader serves an engine")
                    .block_store()
                    .prepare_next_slab_with_target(1)
                    .expect("rolling onto a fresh slab must succeed")
                    .is_some(),
                "the slab did not roll at record {index}, so the corpus would land in one slab"
            );
        }
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:08}"),
                value: vec![b'x'; 128],
            })
            .expect("propose must succeed");
        index += 1;
    }
    cluster
}

/// Make the compaction threshold fire, and stop the catch-up hold from pre-empting it.
///
/// Both are deliberate production behaviours that would otherwise make this fixture measure
/// "nothing happened": the byte threshold is a gibibyte, and the retained-bytes ceiling holds
/// compaction while a live peer is still behind.
fn force_the_threshold(cluster: &RaftCluster) {
    let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
    inner.config.max_applied_log_bytes = 1;
    inner.config.max_retained_log_bytes = 0;
}

/// Everything the snapshot path is about to be measured against, asserted NON-EMPTY.
///
/// The most expensive mistake available here is measuring a structure nothing filled: an image
/// with no slabs walks no slabs, and a cost that never occurred reads exactly like a low one. So
/// every denominator the rows below divide by is checked here, against the same cluster the
/// measurement runs on, and printed.
fn assert_the_fixture_is_populated(cluster: &RaftCluster, records: usize) -> Fixture {
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let image = snapshot
        .state_image
        .as_ref()
        .expect("the state-image path is the one under test; an entry snapshot measures nothing");
    let payload = image.payload_bytes();
    println!("  fixture records        = {records}");
    println!("  fixture image bytes    = {payload}");
    println!("  fixture index bytes    = {}", image.index_bytes.len());
    println!("  fixture image slabs    = {}", image.slabs.len());
    assert!(
        image.index_bytes.len() > 1_024,
        "served index is {} bytes: nothing was indexed, so an index encode measures nothing",
        image.index_bytes.len()
    );
    assert!(
        image.slabs.len() > 1,
        "the image carries {} slab(s). With zero, every slab read, copy and re-install below is a \
         count of zero that would pass any bound put on it; with one, a per-slab cost and a \
         per-snapshot cost are the same number and neither can be told from the other",
        image.slabs.len()
    );
    let slab_bytes = image
        .slabs
        .iter()
        .map(|slab| slab.bytes.len())
        .sum::<usize>();
    assert!(
        slab_bytes > 0,
        "the image's slabs are all empty: {} slabs, 0 bytes",
        image.slabs.len()
    );

    // The scope of the image is the shard's LIVE slab set intersected with what the store holds.
    // An EMPTY live set is not an empty image -- it is the fallback that carries the whole store
    // -- so a fixture whose live set is empty is measuring the other branch entirely.
    let engine = cluster
        .node_engine_for_test(1)
        .expect("the leader serves an engine");
    let live = engine.live_block_slab_ids(1);
    println!("  fixture live slab ids  = {live:?}");
    assert!(
        live.len() > 1,
        "the shard's live slab set is {live:?}. Empty means the image took the carry-the-whole-\
         store fallback and none of the scoping being measured happened; a single id means the \
         set cannot distinguish a walk of the live set from a walk of the store"
    );

    // And the node set the install loop walks. One alive node makes "copies per node" and "copies"
    // the same number and the multiplicity being measured invisible.
    let alive = cluster
        .inner
        .read()
        .expect("raft cluster lock poisoned")
        .nodes
        .values()
        .filter(|node| node.alive)
        .count();
    println!("  fixture alive nodes    = {alive}");
    assert!(
        alive > 1,
        "only {alive} alive node: a per-node cost cannot be distinguished from a per-snapshot one"
    );
    Fixture {
        payload_bytes: payload,
        slabs: image.slabs.len(),
        alive_nodes: alive,
    }
}

/// The denominators one measurement divides by, read off the fixture it is about to run on.
#[derive(Debug, Clone, Copy)]
struct Fixture {
    payload_bytes: usize,
    slabs: usize,
    alive_nodes: usize,
}

fn print_counts(label: &str, counts: &SnapshotCounts) {
    println!("  {label}: {counts:?}");
}

/// The BUILD is one pass over the corpus, and it holds no guard.
///
/// What is asserted is the SHAPE at two sizes, not a constant: the walk visits exactly one block
/// address per record at both sizes (so it is O(corpus) and nothing walks it twice), the store is
/// listed once and each slab read once however big the corpus is, and no served-index encode
/// happens while a shard guard is held.
#[test]
fn a_snapshot_build_walks_the_corpus_once_and_holds_no_guard() {
    let mut rows = Vec::new();
    for records in [SMALL, LARGE] {
        let cluster = cluster_with(records);
        println!("=== BUILD records={records} ===");
        let fixture = assert_the_fixture_is_populated(&cluster, records);

        snapshot_probe::reset();
        crate::engine::shard_write_guard::reset_index_encode_counts();
        let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
        let counts = snapshot_probe::counts();
        let encodes = crate::engine::shard_write_guard::index_encode_counts();
        print_counts("build", &counts);
        println!("  encodes: {encodes:?}");

        assert_eq!(
            counts.image_builds, 1,
            "one create_snapshot must build exactly one state image"
        );
        assert_eq!(
            counts.live_slab_scans, 1,
            "one create_snapshot must walk the corpus for live slab ids exactly once"
        );
        assert_eq!(
            counts.live_slab_scan_addresses, records as u64,
            "the live-slab walk must visit exactly one block address per record: {} for {records} \
             records",
            counts.live_slab_scan_addresses
        );
        assert_eq!(
            counts.slab_dir_listings, 1,
            "the build must list the store's slab directory exactly once"
        );
        assert_eq!(
            counts.slab_reads,
            snapshot
                .state_image
                .as_ref()
                .expect("state image")
                .slabs
                .len() as u64,
            "the build must read each carried slab exactly once"
        );
        assert_eq!(
            counts.image_clones, 0,
            "building a snapshot must not copy the image it just built"
        );
        assert_eq!(
            encodes.encodes_total, 1,
            "the build must encode the served index exactly once"
        );
        assert_eq!(
            encodes.encodes_under_guard, 0,
            "the served-index encode must not run while a shard-table guard is held"
        );
        rows.push((records, fixture, counts));
    }

    let (small_records, small_fixture, small) = &rows[0];
    let (large_records, large_fixture, large) = &rows[1];
    let corpus_ratio = *large_records as f64 / *small_records as f64;
    println!("=== BUILD ratio, {small_records} -> {large_records} ===");
    println!("  corpus ratio                 = {corpus_ratio:.3}");
    println!(
        "  addresses walked per record  = {:.3} -> {:.3}",
        small.live_slab_scan_addresses as f64 / *small_records as f64,
        large.live_slab_scan_addresses as f64 / *large_records as f64
    );
    println!(
        "  slab bytes read ratio        = {:.3}",
        large.slab_read_bytes as f64 / small.slab_read_bytes.max(1) as f64
    );
    println!(
        "  image bytes ratio            = {:.3}",
        large_fixture.payload_bytes as f64 / small_fixture.payload_bytes as f64
    );
    assert!(
        small.slab_read_bytes > 0 && large.slab_read_bytes > 0,
        "slab bytes read is zero at one of the two sizes, so the ratio below is vacuous: \
         {} and {}",
        small.slab_read_bytes,
        large.slab_read_bytes
    );
    // The walk is the finding: it is EXACTLY linear in the corpus, with no second pass hiding in
    // it. Asserted as an identity per record rather than as a bound, because a bound would also
    // be satisfied by a walk that had stopped happening.
    assert_eq!(
        small.live_slab_scan_addresses * (*large_records as u64),
        large.live_slab_scan_addresses * (*small_records as u64),
        "the live-slab walk must cost the same per record at both sizes: {} per {small_records} \
         against {} per {large_records}",
        small.live_slab_scan_addresses,
        large.live_slab_scan_addresses
    );
    // And the metadata around it does NOT grow with the corpus.
    assert_eq!(
        small.slab_dir_listings, large.slab_dir_listings,
        "the number of slab directory listings must not grow with the corpus"
    );
}

/// The INSTALL half, and what it holds the cluster write guard across.
///
/// This is the row the whole file exists for. `maybe_trigger_snapshot` is the ROUTINE snapshot --
/// the periodic compaction tick a deployed node runs, not the rare catch-up send -- and it does
/// its installs inside the cluster write guard. Each install rebuilds a whole engine from the
/// image, and each install but the last needs its own copy of it.
#[test]
fn a_routine_snapshot_rebuilds_the_whole_shard_under_the_cluster_write_guard() {
    let mut rows = Vec::new();
    for records in [SMALL, LARGE] {
        let cluster = cluster_with(records);
        println!("=== TRIGGER records={records} ===");
        let fixture = assert_the_fixture_is_populated(&cluster, records);
        force_the_threshold(&cluster);

        snapshot_probe::reset();
        let report = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        let counts = snapshot_probe::counts();
        println!("  report: {report:?}");
        print_counts("trigger", &counts);

        // VACUITY FLOOR. Every bound below is on work done under the guard; a trigger that did
        // not fire does none of it and would satisfy all of them.
        assert!(
            report.triggered,
            "the trigger did not fire ({}), so every count below is zero for the wrong reason",
            report.reason
        );
        assert_eq!(
            counts.image_builds, 1,
            "a routine snapshot must build exactly one state image"
        );
        assert_eq!(
            counts.engine_rebuilds, fixture.alive_nodes as u64,
            "the in-process cluster rebuilds one engine per alive node: {} rebuilds for {} nodes",
            counts.engine_rebuilds, fixture.alive_nodes
        );
        // The per-SLAB multiplicity, which the four-slab fixture is what makes visible: each
        // install writes every slab in the image again.
        assert_eq!(
            counts.slab_installs,
            counts.engine_rebuilds * fixture.slabs as u64,
            "each of the {} installs must re-write all {} slabs: {} installs seen",
            counts.engine_rebuilds,
            fixture.slabs,
            counts.slab_installs
        );

        // THE FINDING, stated as an identity rather than as a bound: every engine rebuild a
        // routine snapshot performs happens inside the cluster write guard.
        assert_eq!(
            counts.engine_rebuilds_under_guard, counts.engine_rebuilds,
            "every engine rebuild on this path is under the cluster write guard: {} of {}",
            counts.engine_rebuilds_under_guard, counts.engine_rebuilds
        );
        assert_eq!(
            counts.slab_install_bytes_under_guard, counts.slab_install_bytes,
            "every slab re-installed by a routine snapshot is written under the cluster write \
             guard: {} of {} bytes",
            counts.slab_install_bytes_under_guard, counts.slab_install_bytes
        );
        rows.push((records, fixture, counts));
    }

    let (small_records, small_fixture, small) = &rows[0];
    let (large_records, large_fixture, large) = &rows[1];
    println!("=== TRIGGER ratio, {small_records} -> {large_records} ===");
    println!("  corpus ratio            = {:.3}", *large_records as f64 / *small_records as f64);
    println!(
        "  image bytes             = {} -> {}",
        small_fixture.payload_bytes, large_fixture.payload_bytes
    );
    println!(
        "  image copies            = {} -> {}",
        small.image_clones, large.image_clones
    );
    println!(
        "  image bytes copied      = {} -> {} (ratio {:.3})",
        small.image_clone_bytes,
        large.image_clone_bytes,
        large.image_clone_bytes as f64 / small.image_clone_bytes.max(1) as f64
    );
    println!(
        "  engine rebuilds         = {} -> {}",
        small.engine_rebuilds, large.engine_rebuilds
    );
    println!(
        "  slab bytes re-installed = {} -> {} (ratio {:.3})",
        small.slab_install_bytes,
        large.slab_install_bytes,
        large.slab_install_bytes as f64 / small.slab_install_bytes.max(1) as f64
    );

    // The MULTIPLICITY is flat -- it is set by the node count, not by the corpus -- while the
    // BYTES move with the corpus. Both halves are asserted: a copy count that grew with the
    // corpus would be a different and worse finding, and a byte total that did not would mean the
    // counter had stopped.
    assert_eq!(
        small.engine_rebuilds, large.engine_rebuilds,
        "engine rebuilds must be set by the node count, not by the corpus: {} against {}",
        small.engine_rebuilds, large.engine_rebuilds
    );
    assert_eq!(
        small.image_clones, large.image_clones,
        "image copies must be set by the node count, not by the corpus: {} against {}",
        small.image_clones, large.image_clones
    );
    assert!(
        large.slab_install_bytes > small.slab_install_bytes,
        "slab bytes re-installed under the guard must grow with the corpus: {} against {}",
        small.slab_install_bytes,
        large.slab_install_bytes
    );
}

/// The one copy that was never needed: the snapshot dies with the loop, so the LAST install can
/// have the original.
///
/// Measured before the move, on a 3-node in-process cluster: 3 copies and 1,142,625 bytes at
/// 2,000 records, 3 copies and 4,855,779 bytes at 8,000 -- every byte inside the cluster write
/// guard. After it, 2 copies, and a DEPLOYED process (below) copies nothing at all.
///
/// Stated against the install count measured in the same run rather than against a constant, so
/// it holds at any node count and cannot be satisfied by a fixture that shrank.
#[test]
fn the_last_install_takes_the_image_instead_of_copying_it() {
    let cluster = cluster_with(SMALL);
    println!("=== LAST INSTALL TAKES ===");
    let fixture = assert_the_fixture_is_populated(&cluster, SMALL);
    force_the_threshold(&cluster);

    snapshot_probe::reset();
    let report = cluster
        .maybe_trigger_snapshot()
        .expect("maybe_trigger_snapshot must succeed");
    let counts = snapshot_probe::counts();
    println!("  report: {report:?}");
    print_counts("trigger", &counts);
    let payload = fixture.payload_bytes;
    println!("  image bytes: {payload}");

    assert!(report.triggered, "the trigger did not fire: {}", report.reason);
    assert!(
        counts.engine_rebuilds >= 2,
        "{} installs: with fewer than two there is no copy to save and this guard proves nothing",
        counts.engine_rebuilds
    );
    assert_eq!(
        counts.image_clones,
        counts.engine_rebuilds - 1,
        "n installs must cost n-1 copies of the image, not n: {} copies for {} installs",
        counts.image_clones,
        counts.engine_rebuilds
    );
    // The direction that matters. This check can only fail one way -- a copy too MANY -- because
    // a path that lost the image would also copy less; so the image is read back out of every
    // node that installed it, which is what stops "copied nothing" from passing.
    let inner = cluster.inner.read().expect("raft cluster lock poisoned");
    let shard_id = inner.shard_id;
    let mut served = 0usize;
    for node in inner.nodes.values() {
        let Some(installed) = node.installed_snapshot.as_ref() else {
            continue;
        };
        let image = installed
            .state_image
            .as_ref()
            .expect("an installed image snapshot must carry its image");
        assert_eq!(
            image.payload_bytes(),
            payload,
            "node {} installed a {}-byte image where the built one was {payload} bytes",
            node.id,
            image.payload_bytes()
        );
        assert_eq!(
            node.engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringGet {
                        key: "key-00000777".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(vec![b'x'; 128])
            },
            "node {} must serve a record from the image it installed",
            node.id
        );
        served += 1;
    }
    println!("  nodes serving from their installed image: {served}");
    assert!(
        served >= 2,
        "only {served} node served from an installed image: the read-back above is what stops a \
         path that dropped the image from satisfying the copy bound"
    );
}

/// The DEPLOYED shape: one node, and therefore no copy of the shard at all.
///
/// A deployed process owns one node and keeps shadows of its peers, so the install loop reaches
/// exactly one node. Before the move that single install still took a copy -- the only copy there
/// was, all of it under the cluster write guard, one whole shard per compaction tick.
#[test]
fn a_deployed_node_copies_the_image_not_at_all() {
    let cluster = cluster_with(SMALL);
    println!("=== DEPLOYED SHAPE ===");
    let fixture = assert_the_fixture_is_populated(&cluster, SMALL);
    // What `RaftProductionRuntime::start` does with `options.local_node_id`.
    cluster.set_local_node_id(1);
    force_the_threshold(&cluster);

    snapshot_probe::reset();
    let report = cluster
        .maybe_trigger_snapshot()
        .expect("maybe_trigger_snapshot must succeed");
    let counts = snapshot_probe::counts();
    println!("  report: {report:?}");
    print_counts("trigger", &counts);
    let payload = fixture.payload_bytes;
    println!("  image bytes: {payload}");

    assert!(report.triggered, "the trigger did not fire: {}", report.reason);
    assert_eq!(
        counts.engine_rebuilds, 1,
        "a deployed process installs into exactly one node: {} rebuilds",
        counts.engine_rebuilds
    );
    assert_eq!(
        counts.image_clones, 0,
        "the single install must TAKE the image: it copied {} bytes of shard under the cluster \
         write guard",
        counts.image_clone_bytes
    );
    // Vacuity: the install has to have HAPPENED for the zero above to mean anything.
    assert!(
        counts.slab_install_bytes > 0,
        "no slab was installed, so the zero-copy assertion above is satisfied by an install that \
         did not run"
    );
    let inner = cluster.inner.read().expect("raft cluster lock poisoned");
    let installed = inner
        .nodes
        .get(&1)
        .expect("node 1")
        .installed_snapshot
        .as_ref()
        .expect("the local node must have installed the snapshot");
    assert_eq!(
        installed
            .state_image
            .as_ref()
            .expect("the installed snapshot must carry its image")
            .payload_bytes(),
        fixture.payload_bytes,
        "the moved image must arrive whole"
    );
}

/// The transfer-policy entry point must build ONE state image, not two.
///
/// It built the image once to decide how to ship it and then threw that one away and built it
/// again -- two corpus walks, two served-index encodes and two reads of every slab for one send.
#[test]
fn the_transfer_policy_entry_point_builds_one_state_image() {
    let cluster = cluster_with(SMALL);
    println!("=== TRANSFER POLICY ===");
    let fixture = assert_the_fixture_is_populated(&cluster, SMALL);

    snapshot_probe::reset();
    let request = cluster
        .build_install_snapshot_request_with_policy(3, RaftSnapshotTransferPolicy::default(), None)
        .expect("building the request must succeed");
    let counts = snapshot_probe::counts();
    print_counts("policy", &counts);

    assert_eq!(
        counts.image_builds, 1,
        "deciding how to transfer a snapshot and then building it must cost ONE state image"
    );
    assert_eq!(
        counts.live_slab_scans, 1,
        "one send must walk the corpus for live slab ids once, not once per decision"
    );
    assert_eq!(
        counts.slab_dir_listings, 1,
        "one send must list the slab directory once"
    );
    // The image still ARRIVES: building it once and losing it would satisfy every count above.
    assert_eq!(
        request
            .snapshot
            .state_image
            .as_ref()
            .expect("the request must carry the state image")
            .payload_bytes(),
        fixture.payload_bytes,
        "the single built image must be the one the request carries, whole"
    );
}

/// `Clone for RaftSnapshotStateImage` is hand-written so the copy can be charged. A hand-written
/// clone that forgets a field compiles and fails silently, so it is checked against the source.
#[test]
fn a_cloned_state_image_equals_its_source() {
    let cluster = cluster_with(64);
    let snapshot = cluster.create_snapshot().expect("create_snapshot");
    let image = snapshot.state_image.as_ref().expect("state image");
    assert!(
        image.payload_bytes() > 0 && !image.slabs.is_empty(),
        "an empty image would compare equal to a clone that copied nothing: {} bytes, {} slabs",
        image.payload_bytes(),
        image.slabs.len()
    );
    snapshot_probe::reset();
    let copy = image.clone();
    let counts = snapshot_probe::counts();
    assert_eq!(
        counts.image_clones, 1,
        "the clone must charge itself exactly once"
    );
    assert_eq!(
        counts.image_clone_bytes,
        image.payload_bytes() as u64,
        "the clone must charge the whole payload"
    );
    assert_eq!(&copy, image, "a clone must equal its source, field for field");
}

/// An INDEPENDENT total, minus the rows this file attributes -- and the residual asserted ACROSS
/// the two sizes rather than against a constant.
///
/// The total is the allocator's own byte counter placed either side of the whole operation. It is
/// emphatically NOT the sum of the rows it audits: a residual computed from those rows could not
/// fail. What it can catch is a cost that moved OUT of the counted primitives, because the
/// residual would then start growing with the corpus while the attributed rows did not.
///
/// So the assertion is on the residual PER RECORD at the two sizes. Fixed overhead divides away
/// and falls; a drifted boundary -- work that used to be attributed and now is not -- shows up as
/// a residual per record that climbs.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_unattributed_remainder_of_a_snapshot_does_not_grow_with_the_corpus() {
    let mut rows = Vec::new();
    for records in [SMALL, LARGE] {
        let cluster = cluster_with(records);
        println!("=== RESIDUAL records={records} ===");
        let fixture = assert_the_fixture_is_populated(&cluster, records);
        let payload = fixture.payload_bytes as u64;
        force_the_threshold(&cluster);

        snapshot_probe::reset();
        crate::engine::shard_write_guard::reset_index_encode_counts();
        let probe = crate::alloc_probe::Probe::start();
        let report = cluster
            .maybe_trigger_snapshot()
            .expect("maybe_trigger_snapshot must succeed");
        let total = probe.stop().alloc_bytes;
        let counts = snapshot_probe::counts();
        let encodes = crate::engine::shard_write_guard::index_encode_counts();
        assert!(report.triggered, "the trigger did not fire: {}", report.reason);

        // The attributed rows: everything this file names, each measured by its own counter.
        let attributed = counts.image_clone_bytes
            + counts.slab_read_bytes
            + counts.slab_install_bytes
            + encodes.encode_bytes_total
            // Each engine rebuild reads one whole image back in, which is the input it is sized
            // by. Named here rather than left in the remainder so the remainder is not simply
            // "the biggest thing nobody counted".
            + counts.engine_rebuilds * payload;
        let residual = total.saturating_sub(attributed);
        println!("  allocator bytes, whole op = {total}");
        println!("  attributed rows           = {attributed}");
        println!("  residual                  = {residual}");
        println!(
            "  residual per record       = {:.1}",
            residual as f64 / records as f64
        );
        assert!(
            total > 0 && attributed > 0,
            "vacuity floor: allocator total {total}, attributed {attributed} -- a residual off \
             two zeroes audits nothing"
        );
        rows.push((records, residual as f64 / records as f64));
    }

    let (small_records, small_per) = rows[0];
    let (large_records, large_per) = rows[1];
    println!("=== RESIDUAL ratio, {small_records} -> {large_records} ===");
    println!("  residual per record = {small_per:.1} -> {large_per:.1}");
    assert!(
        large_per <= small_per * 2.0,
        "the unattributed remainder grew from {small_per:.1} to {large_per:.1} bytes per record \
         over a {:.2}x corpus: something that scales with the store is no longer being counted by \
         the rows this file attributes",
        large_records as f64 / small_records as f64
    );
}
