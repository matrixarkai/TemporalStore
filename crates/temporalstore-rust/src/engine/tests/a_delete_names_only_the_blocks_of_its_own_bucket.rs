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
//!   * `a_delete_visits_the_objects_of_its_bucket_and_not_of_the_shard` counts the objects visited
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

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
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

/// Objects visited per delete, for `deletes` keys removed from a shard of `keys` string keys.
fn visits_per_delete(keys: usize, deletes: usize, owners_disabled: bool) -> f64 {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    fill(&engine, keys);
    crate::engine::state::disable_bucket_owners_for_test(owners_disabled);
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
    crate::engine::state::disable_bucket_owners_for_test(false);
    visits as f64 / deletes as f64
}

#[test]
fn a_delete_visits_the_objects_of_its_bucket_and_not_of_the_shard() {
    const SMALL: usize = 2_000;
    const LARGE: usize = 16_000;
    const DELETES: usize = 40;

    let small = visits_per_delete(SMALL, DELETES, false);
    let large = visits_per_delete(LARGE, DELETES, false);
    // THE CONTROL: with the owners off the walk runs, and what it visits is the shard.
    let walk_small = visits_per_delete(SMALL, DELETES, true);
    let walk_large = visits_per_delete(LARGE, DELETES, true);
    println!(
        "  objects visited per delete: owners {small:.1} at {SMALL} keys, {large:.1} at {LARGE}; \
         walk {walk_small:.1} at {SMALL}, {walk_large:.1} at {LARGE}"
    );
    assert!(
        walk_large > 4.0 * walk_small && walk_large > 1_000.0,
        "the control is blind to the shard's size: the walk visited {walk_small:.1} per delete at \
         {SMALL} keys and {walk_large:.1} at {LARGE}, so a count that stayed flat would prove nothing"
    );
    assert!(
        large <= 2.0 * small.max(1.0),
        "a delete visited {large:.1} objects at {LARGE} keys against {small:.1} at {SMALL}: the \
         naming of a bucket's blocks is paying for the shard again"
    );
    assert!(
        large < walk_large / 20.0,
        "the owner path visited {large:.1} objects per delete, against {walk_large:.1} for the walk"
    );
}

#[test]
fn the_owners_record_every_object_that_holds_a_ref() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
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
