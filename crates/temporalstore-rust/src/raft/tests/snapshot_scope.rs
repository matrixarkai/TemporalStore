// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT a per-shard snapshot carries, and WHERE the install does its work.
//!
//! Two properties, measured rather than read:
//!
//! 1. A snapshot for shard A must carry shard A's slabs. `BlockStore` has no shard concept --
//!    `slab_ids()` lists a directory -- so the walk that built the image answered for the whole
//!    NODE. The tests below build one engine hosting several shards and compare the image for
//!    one shard against the image for another. Before the scope, the two were the same set: the
//!    image was sized by the node's shard count, and the receiving replica installed slabs for
//!    shards it does not host.
//!
//! 2. The install must not do corpus-sized work under the cluster WRITE guard, which excludes
//!    readers as well as writers. The engine an install publishes is private until the moment
//!    it is published, so the slab installs and the applied-set fill belong outside the guard.
//!    That is asserted by COUNTING the work attributed to the guarded span, not by timing it:
//!    this box's load moves between runs and a count does not.
//!
//! Every counted claim here carries its own positive control, because a probe that never fires
//! and a subject that does no work are otherwise the same measurement.

use super::*;
use crate::raft::cluster_snapshot::{build_state_image, install_probe};
use std::collections::BTreeSet;

/// 256-byte records, so a slab holds a countable number of them rather than thousands.
fn write_records(engine: &TemporalEngine, shard_id: ShardId, tag: &str, count: usize) {
    let mut index = 0usize;
    while index < count {
        engine.execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: format!("{tag}-{index:05}"),
                value: vec![b'v'; 256],
            },
        });
        index += 1;
    }
}

fn slabs_on_the_node(engine: &TemporalEngine) -> BTreeSet<u64> {
    engine
        .block_store()
        .slab_ids()
        .expect("the block store must list its slabs")
        .into_iter()
        .collect()
}

fn carried_slabs(engine: &TemporalEngine, shard_id: ShardId) -> BTreeSet<u64> {
    build_state_image(engine, shard_id)
        .expect("the engine must be able to serve a state image for a loaded shard")
        .slabs
        .iter()
        .map(|slab| slab.block_slab_id)
        .collect()
}

/// One engine hosting `shards` shards, each shard's writes separated by a slab roll so the
/// store ends up holding several shards' slabs at once. Shard 2 is deliberately given a roll
/// mid-window, so it owns a DIFFERENT NUMBER of slabs from every other shard -- that is the
/// control: a scoped image answers with a different count per shard, an unscoped one cannot.
fn engine_hosting(shards: &[ShardId]) -> TemporalEngine {
    let engine = TemporalEngine::default();
    for shard_id in shards {
        engine.load_shard(*shard_id);
    }
    for shard_id in shards {
        let tag = format!("shard{shard_id}");
        if *shard_id == 2 {
            write_records(&engine, *shard_id, &tag, 40);
            engine
                .block_store()
                .roll_slab()
                .expect("rolling the active slab must succeed");
            write_records(&engine, *shard_id, &format!("{tag}b"), 40);
        } else {
            write_records(&engine, *shard_id, &tag, 40);
        }
        engine
            .block_store()
            .roll_slab()
            .expect("rolling the active slab must succeed");
    }
    engine
}

/// The denominators, printed, plus the floors that stop any of this being vacuous.
fn report_scope(label: &str, engine: &TemporalEngine, shards: &[ShardId]) -> (usize, usize) {
    let present = slabs_on_the_node(engine);
    let first = carried_slabs(engine, shards[0]);
    println!("--- {label} ---");
    println!("shards_hosted        = {}", shards.len());
    println!("slabs_on_the_node    = {}", present.len());
    println!("slabs_carried_shard1 = {}", first.len());

    assert!(
        present.len() >= shards.len(),
        "fixture did not produce a slab per shard: the node holds {} slabs for {} shards, so \
         there is nothing for an unscoped walk to over-carry and this measures nothing",
        present.len(),
        shards.len()
    );
    assert!(
        !first.is_empty(),
        "the image for shard {} carries NO slabs, so every comparison below is vacuous",
        shards[0]
    );
    (present.len(), first.len())
}

/// FINDING 1, at two shards. The image for shard 1 must not be the image for shard 2.
#[test]
fn a_shard_snapshot_carries_only_its_own_slabs_at_two_shards() {
    let shards = [1 as ShardId, 2 as ShardId];
    let engine = engine_hosting(&shards);
    let (present, carried_one) = report_scope("two shards", &engine, &shards);

    let first = carried_slabs(&engine, 1);
    let second = carried_slabs(&engine, 2);
    println!("slabs_carried_shard2 = {}", second.len());
    println!(
        "over_carry_ratio     = {:.2}x",
        present as f64 / carried_one as f64
    );

    // THE CONTROL, and it is the whole refutation of "the walk is implicitly shard-scoped":
    // two shards of one engine must not be answered with the same slab set. Shard 2 was given
    // an extra roll, so a scoped answer differs in COUNT and not only in membership.
    assert_ne!(
        first, second,
        "shard 1 and shard 2 of the same engine were handed the IDENTICAL slab set, so the walk \
         is not shard-scoped: {first:?}"
    );
    assert_ne!(
        first.len(),
        second.len(),
        "the two shards were handed the same NUMBER of slabs ({}), so this fixture cannot tell \
         a scoped walk from an unscoped one",
        first.len()
    );

    // THE CLAIM: shard 1's image is a strict subset of the node's slabs, i.e. the other shard's
    // bytes were actually dropped rather than merely reordered.
    assert!(
        carried_one < present,
        "shard 1's image carries all {present} slabs on the node, so it is still carrying shard \
         2's bytes"
    );
    assert!(
        !first.contains(second.iter().next().expect("shard 2 owns a slab")),
        "shard 1's image carries a slab that shard 2's image also claims exclusively"
    );
}

/// FINDING 1, at four shards. Same property; the point of the second size is the RATIO, which
/// grows with the shard count because the over-carry was the whole store every time.
#[test]
fn a_shard_snapshot_carries_only_its_own_slabs_at_four_shards() {
    let shards = [1 as ShardId, 2 as ShardId, 3 as ShardId, 4 as ShardId];
    let engine = engine_hosting(&shards);
    let (present, carried_one) = report_scope("four shards", &engine, &shards);
    println!(
        "over_carry_ratio     = {:.2}x",
        present as f64 / carried_one as f64
    );

    let first = carried_slabs(&engine, 1);
    let fourth = carried_slabs(&engine, 4);
    assert_ne!(
        first, fourth,
        "shard 1 and shard 4 of the same engine were handed the IDENTICAL slab set"
    );
    assert!(
        carried_one < present,
        "shard 1's image carries all {present} slabs on the node at four shards"
    );

    // The over-carry the scope removes grows with the shard count: at four shards the node
    // holds strictly more slabs than at two, while shard 1 still owns what it owned.
    let two_shard_present = slabs_on_the_node(&engine_hosting(&[1 as ShardId, 2 as ShardId])).len();
    println!("slabs_on_the_node@2  = {two_shard_present}");
    assert!(
        present > two_shard_present,
        "four shards did not put more slabs on the node than two ({present} vs \
         {two_shard_present}), so the multiplier claim is untested"
    );
}

/// The scope must not drop a slab the installed index can still address. This is the property
/// that matters and it is checked END TO END rather than against the same formula the scope
/// uses: install shard 1's image into a fresh engine and read every record back.
#[test]
fn a_scoped_image_still_serves_every_record_the_shard_held() {
    let shards = [1 as ShardId, 2 as ShardId, 3 as ShardId, 4 as ShardId];
    let engine = engine_hosting(&shards);
    let image = build_state_image(&engine, 1).expect("shard 1 must serve a state image");
    println!("slabs_carried_shard1 = {}", image.slabs.len());
    assert!(
        !image.slabs.is_empty(),
        "nothing was carried, so serving the records back proves nothing"
    );

    // Exactly what the install does with an image, on a fresh private engine.
    let restored = TemporalEngine::default();
    let block_store = restored.block_store();
    for slab in &image.slabs {
        block_store
            .install_slab(slab.block_slab_id, &slab.bytes)
            .expect("installing a carried slab must succeed");
    }
    restored
        .install_index_bytes(1, &image.index_bytes)
        .expect("installing the served index must succeed");
    restored.load_shard(1);

    let mut index = 0usize;
    while index < 40 {
        assert_eq!(
            restored
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: format!("shard1-{index:05}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(vec![b'v'; 256])
            },
            "the scoped image dropped a slab shard 1's index still addresses: key \
             shard1-{index:05} did not come back"
        );
        index += 1;
    }
}

/// How much of finding 1 is LIVE rather than latent, stated as a measurement rather than as a
/// reassurance. Every engine a raft node serves from is built by this crate with exactly one
/// shard loaded into its own scratch block store, so today the over-carry multiplier on the
/// serving path is 1.0 and the scope above is what keeps it there.
#[test]
fn a_raft_node_engine_hosts_exactly_one_shard_today() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let mut index = 0usize;
    while index < 64 {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:05}"),
                value: vec![b'v'; 256],
            })
            .expect("propose must succeed");
        index += 1;
    }

    let engine = cluster
        .node_engine_for_test(1)
        .expect("the leader must have an engine");
    let this_shard = engine.live_block_slab_ids(1);
    let whole_node = engine.live_block_slab_ids_all_shards();
    println!("live_slabs_shard1    = {}", this_shard.len());
    println!("live_slabs_whole_node= {}", whole_node.len());

    assert!(
        !this_shard.is_empty(),
        "the leader engine holds no live slabs for its own shard, so this comparison is vacuous"
    );
    assert_eq!(
        this_shard, whole_node,
        "a raft node's engine is hosting a shard other than its own, so the over-carry this \
         file scopes away is LIVE on the serving path and not merely latent"
    );
}

/// FINDING 2, first half: no slab is installed while the cluster write guard is held.
#[test]
fn installing_a_snapshot_does_no_slab_work_under_the_cluster_write_guard() {
    // POSITIVE CONTROL FIRST, so it cannot be skipped by an earlier failure. It proves the
    // detector can attribute work to the guarded span at all -- without it, a `GuardMark` that
    // never arms and an install that never runs under the guard are the same measurement --
    // and that `Drop` disarms it again.
    install_probe::reset();
    {
        let _mark = install_probe::GuardMark::new();
        install_probe::note_slab_install();
    }
    install_probe::note_slab_install();
    let control = install_probe::counts();
    println!("control_total        = {}", control.slabs_total);
    println!("control_under_guard  = {}", control.slabs_under_guard);
    assert_eq!(
        control.slabs_total, 2,
        "the probe did not count both noted installs, so it cannot be trusted below"
    );
    assert_eq!(
        control.slabs_under_guard, 1,
        "the probe cannot tell work inside the guarded span from work outside it, so a zero \
         below would mean nothing"
    );

    // Rolled between batches so the image carries SEVERAL slabs: "0 of 1" would be a much
    // weaker statement than "0 of n", and one slab is within noise of an install that happens
    // to carry none.
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let leader = cluster
        .node_engine_for_test(1)
        .expect("the leader must have an engine");
    let mut batch = 0usize;
    while batch < 4 {
        let mut index = 0usize;
        while index < 200 {
            cluster
                .propose(Command::StringSet {
                    key: format!("key-{batch}-{index:05}"),
                    value: vec![b'v'; 256],
                })
                .expect("propose must succeed");
            index += 1;
        }
        leader
            .block_store()
            .roll_slab()
            .expect("rolling the active slab must succeed");
        batch += 1;
    }
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let slabs_in_image = snapshot
        .state_image
        .as_ref()
        .expect("the state-image path must be the one under test")
        .slabs
        .len() as u64;

    install_probe::reset();
    cluster
        .install_snapshot(3, snapshot)
        .expect("install must succeed");
    let counts = install_probe::counts();
    println!("slabs_in_image       = {slabs_in_image}");
    println!("slabs_installed      = {}", counts.slabs_total);
    println!("slabs_under_guard    = {}", counts.slabs_under_guard);

    // VACUITY FLOOR: there must have been slab work to place on one side or the other.
    assert!(
        slabs_in_image >= 2,
        "the image carries {slabs_in_image} slab(s); below 2 an install that does no slab work \
         under the guard is not distinguishable from one with no slab work to do"
    );
    assert_eq!(
        counts.slabs_total, slabs_in_image,
        "the install did not install every slab the image carries"
    );

    // THE CLAIM.
    assert_eq!(
        counts.slabs_under_guard, 0,
        "{} of {slabs_in_image} slab installs ran while the cluster WRITE guard was held, which \
         excludes every reader and proposer for the length of the install",
        counts.slabs_under_guard
    );
}

/// FINDING 2, second half, asserted separately: the applied-set fill is the O(history) step
/// inside an otherwise O(state) install -- one element per committed index -- and it must not
/// run under the write guard either.
#[test]
fn installing_a_snapshot_fills_the_applied_set_off_the_cluster_write_guard() {
    install_probe::reset();
    {
        let _mark = install_probe::GuardMark::new();
        install_probe::note_applied_fill(5);
    }
    install_probe::note_applied_fill(3);
    let control = install_probe::counts();
    println!("control_total        = {}", control.applied_total);
    println!("control_under_guard  = {}", control.applied_under_guard);
    assert_eq!(
        control.applied_total, 8,
        "the probe did not count both noted fills, so it cannot be trusted below"
    );
    assert_eq!(
        control.applied_under_guard, 5,
        "the probe cannot tell a fill inside the guarded span from one outside it"
    );

    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let mut index = 0usize;
    while index < 400 {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:05}"),
                value: vec![b'v'; 64],
            })
            .expect("propose must succeed");
        index += 1;
    }
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let last_included = snapshot.last_included_index;

    install_probe::reset();
    cluster
        .install_snapshot(3, snapshot)
        .expect("install must succeed");
    let counts = install_probe::counts();
    println!("last_included_index  = {last_included}");
    println!("applied_filled       = {}", counts.applied_total);
    println!("applied_under_guard  = {}", counts.applied_under_guard);

    assert!(
        last_included > 0,
        "the snapshot covers no indexes, so there is no fill to place on either side"
    );
    assert_eq!(
        counts.applied_total, last_included,
        "the fill did not cover the snapshot's index range"
    );
    assert_eq!(
        counts.applied_under_guard, 0,
        "{} applied-set elements were inserted under the cluster WRITE guard; that count grows \
         with the committed HISTORY, not with the state the install is supposed to cost",
        counts.applied_under_guard
    );
}

/// M8. `create_state_image_snapshot` builds the image OFF the cluster lock and then re-reads
/// the leader's applied index to decide whether the image it just built still describes one
/// state. That equality is the entire justification for building off the lock: invert it and
/// every returned snapshot is still correct -- the retry loop falls through to a fresh build
/// under the read lock -- so no correctness test can see it. What it costs is BUILDS, and a
/// build reads the whole served index and every carried slab.
#[test]
fn a_quiescent_cluster_builds_the_state_image_exactly_once() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let mut index = 0usize;
    while index < 32 {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:05}"),
                value: vec![b'v'; 128],
            })
            .expect("propose must succeed");
        index += 1;
    }

    // Nothing else proposes from here, so the watermark cannot move under the build.
    install_probe::reset();
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let builds = install_probe::counts().state_image_builds;
    println!("state_image_builds   = {builds}");

    assert!(
        snapshot.state_image.is_some(),
        "the entry-carrying path was taken, so no image was built and the count below is vacuous"
    );
    assert!(
        builds > 0,
        "the probe counted no builds at all, so it cannot detect a repeated one"
    );
    assert_eq!(
        builds, 1,
        "a quiescent cluster built the state image {builds} times for one snapshot: the \
         watermark re-check accepted nothing, so every build was discarded and rebuilt"
    );
}

/// M9. The applied set is seeded from the range the snapshot COVERS. `last_included_index` is
/// applied -- it is the index the snapshot was taken AT -- so the range is inclusive of it, and
/// `applied_index` is set to exactly that value beside it. An exclusive range leaves the set one
/// element short of the scalar that summarises it, which nothing else in the install notices.
#[test]
fn the_applied_set_covers_the_snapshot_index_it_was_installed_at() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let mut index = 0usize;
    while index < 24 {
        cluster
            .propose(Command::StringSet {
                key: format!("key-{index:05}"),
                value: vec![b'v'; 64],
            })
            .expect("propose must succeed");
        index += 1;
    }
    let snapshot = cluster.create_snapshot().expect("create_snapshot must succeed");
    let last_included = snapshot.last_included_index;
    assert!(
        snapshot.state_image.is_some(),
        "the entry-carrying path was taken, so the range fill under test never ran"
    );
    assert!(
        last_included >= 2,
        "the snapshot covers {last_included} index(es); an off-by-one in the fill is not \
         resolvable below 2"
    );

    cluster
        .install_snapshot(3, snapshot)
        .expect("install must succeed");

    let applied_len = cluster.applied_set_len_for_test(3);
    println!("last_included_index  = {last_included}");
    println!("applied_set_len      = {applied_len}");
    assert_eq!(
        applied_len, last_included as usize,
        "the applied set holds {applied_len} indexes for a snapshot covering {last_included}"
    );
    assert!(
        cluster.applied_set_contains_for_test(3, last_included),
        "the applied set omits index {last_included}, the very index the snapshot was taken at, \
         while applied_index was set to it"
    );
}
