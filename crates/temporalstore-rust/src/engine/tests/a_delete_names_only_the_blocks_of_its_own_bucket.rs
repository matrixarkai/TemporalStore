// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A DELETE NAMES THE BLOCKS OF ITS OWN BUCKET, NOT THE BLOCKS OF THE SHARD.
//!
//! Removing a key settles the bucket its blocks sit in, and settling it needs to know which
//! object each block of that bucket belongs to. The only join from a bucket to its blocks' owners
//! was the objects' own refs, so naming one bucket's blocks visited every object of every kind in
//! the shard: the cost of one delete grew with the shard. Measured on a 20,000-key shard a
//! maintenance round removing 100 due keys took 42 ms, and 229 ms at 100,000 keys: the same 100
//! removals, five times the keys, five times the time.
//!
//! The per-bucket owners record which objects hold a ref in each bucket, so a bucket is named from
//! its own objects.
//!
//! TWO ARMS, BECAUSE THEY GUARD DIFFERENT FAILURES.
//!   * `a_delete_reads_none_of_the_bucket_it_leaves_behind` counts the objects visited
//!     per delete at two shard sizes and requires the count not to grow with the shard, with a
//!     control that switches the owners off and requires the count to grow with it, so the counter
//!     is known to be able to see the dimension.
//!   * `the_owners_record_every_object_that_holds_a_ref` states the property naming relies on: after
//!     writes, overwrites, deletes and a rebuild, every object that holds a ref in a bucket is
//!     recorded as its owner.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

fn engine_on(dir: &std::path::Path, buckets: u32) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    // A BUCKET COUNT THE CALLER SETS, so each bucket holds hundreds of blocks (past the small-bucket projection) at every shard size.
    engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: buckets - 1,
        readonly: false,
        table_name: String::new(),
    });
    engine
}

fn run(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn fill(engine: &TemporalEngine, keys: usize) {
    for index in 0..keys {
        run(
            engine,
            Command::StringSet {
                key: format!("k-{index:07}"),
                value: vec![b'v'; 16],
            },
        );
    }
}

/// Objects visited per delete, for `deletes` keys removed from a shard of `keys` string keys held in
/// four buckets (so a bucket grows with the shard). `object_local` false takes the path that names
/// the whole bucket, which is the control.
fn visits_per_delete(keys: usize, deletes: usize, object_local: bool) -> f64 {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), 4);
    fill(&engine, keys);
    crate::engine::disable_object_local_delete_for_test(!object_local);
    let (_, visits) = crate::engine::state::measure_block_identity_visits_for_test(|| {
        for index in 0..deletes {
            run(
                &engine,
                Command::CommonDelete {
                    key: format!("k-{:07}", (index * 7919) % keys),
                },
            );
        }
    });
    crate::engine::disable_object_local_delete_for_test(false);
    visits as f64 / deletes as f64
}

#[test]
fn a_delete_reads_none_of_the_bucket_it_leaves_behind() {
    const SMALL: usize = 2_000;
    const LARGE: usize = 16_000;
    const DELETES: usize = 40;

    let small = visits_per_delete(SMALL, DELETES, true);
    let large = visits_per_delete(LARGE, DELETES, true);
    // THE CONTROL: the path that names the whole bucket visits the bucket, and the bucket grows
    // with the shard here, so a count that stayed flat on this path would prove nothing.
    let named_small = visits_per_delete(SMALL, DELETES, false);
    let named_large = visits_per_delete(LARGE, DELETES, false);
    println!(
        "  objects visited per delete: by handle {small:.1} at {SMALL} keys, {large:.1} at {LARGE};          naming the bucket {named_small:.1} at {SMALL}, {named_large:.1} at {LARGE}"
    );
    assert!(
        named_large > 4.0 * named_small && named_large > 1_000.0,
        "the control is blind to the bucket's size: naming it visited {named_small:.1} per delete at          {SMALL} keys and {named_large:.1} at {LARGE}, so a count that stayed flat would prove nothing"
    );
    assert!(
        small <= 1.0 && large <= 1.0,
        "a delete by handle visited {small:.1} objects at {SMALL} keys and {large:.1} at {LARGE}: it is          reading the bucket again"
    );
}

/// What a bucket holds after the same deletes, by handle and by naming the bucket.
fn buckets_after_deletes(object_local: bool) -> Vec<(u32, usize, Vec<u64>)> {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), 4);
    fill(&engine, 1_200);
    crate::engine::disable_object_local_delete_for_test(!object_local);
    for index in 0..1_200 {
        if index % 3 == 0 {
            run(
                &engine,
                Command::StringSet {
                    key: format!("k-{index:07}"),
                    value: vec![b'w'; 300],
                },
            );
        }
        if index % 4 == 0 {
            run(
                &engine,
                Command::CommonDelete {
                    key: format!("k-{index:07}"),
                },
            );
        }
    }
    crate::engine::disable_object_local_delete_for_test(false);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(bucket, node)| {
            let mut ids: Vec<u64> = node.object_index.iter().copied().collect();
            ids.sort_unstable();
            (*bucket, node.block_index.len(), ids)
        })
        .collect()
}

#[test]
fn removing_by_handle_leaves_each_bucket_as_naming_it_does() {
    let by_handle = buckets_after_deletes(true);
    let by_naming = buckets_after_deletes(false);
    assert!(!by_handle.is_empty(), "the fixture left no bucket to compare");
    assert_eq!(
        by_handle, by_naming,
        "the same deletes left the buckets different: (bucket, blocks, object ids) by handle against by naming"
    );
}

#[test]
fn the_owners_record_every_object_that_holds_a_ref() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), 2);
    fill(&engine, 600);
    // Overwrites, deletes and a container, so refs move and leave.
    for index in 0..600 {
        if index % 3 == 0 {
            run(
                &engine,
                Command::StringSet {
                    key: format!("k-{index:07}"),
                    value: vec![b'w'; 300],
                },
            );
        }
        if index % 5 == 0 {
            run(
                &engine,
                Command::CommonDelete {
                    key: format!("k-{index:07}"),
                },
            );
        }
    }
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    assert!(
        shard.bucket_index.bucket_owners.get().is_some(),
        "an installed shard has its owners built"
    );
    shard.bucket_index.assert_bucket_owners_cover_every_ref();
}

/// The footprint of the owners, per object, on a shard of 20,000 string keys. Run by hand:
/// `cargo test --release --lib the_owners_footprint -- --ignored --nocapture`.
#[test]
#[ignore = "a measurement, not a guard"]
fn the_owners_footprint_per_object() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path(), 40);
    fill(&engine, 20_000);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let owners = shard.bucket_index.bucket_owners.get().expect("owners are built");
    let objects: usize = shard.bucket_index.objects.values().map(|table| table.len()).sum();
    println!(
        "  owners: {} entries over {objects} objects, {} reserved bytes = {:.1} B per object",
        owners.entries(),
        owners.reserved_bytes(),
        owners.reserved_bytes() as f64 / objects as f64
    );
}
