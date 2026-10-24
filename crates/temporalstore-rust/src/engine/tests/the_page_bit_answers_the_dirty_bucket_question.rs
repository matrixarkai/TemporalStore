// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAGE BIT ANSWERS THE DIRTY-BUCKET QUESTION, so the object-keyed set need not.
//!
//! # WHAT THIS IS FOR
//!
//! The design being followed keys objects directly and keeps no per-object dirty set. Going there
//! has one blocker: `object_manager_stats` walks `dirty_objects` to find buckets that hold a dirty
//! object's *blocks* while not being that object's *routing* bucket. Every other reader of the set
//! wants dirty BUCKETS, which `BucketNode` already records itself.
//!
//! **The replacement is already in the sibling arm of that same function** — the one taken when the
//! object lookup is empty:
//!
//! ```text
//!   bucket.dirty() || bucket.block_index.values().any(|page| page.dirty)
//! ```
//!
//! and it is exact rather than approximate, because `push_object_dirty_onto_its_existing_pages`
//! runs at **both** sites that mark an object dirty. The invariant is already pinned elsewhere in
//! these words: *a mark that does not reach its pages leaves the bucket reading clean.* So every
//! page of a dirty object carries `page.dirty`, and every bucket holding one of those pages has a
//! dirty page.
//!
//! # BUT THAT IS AN ARGUMENT, AND AN ARGUMENT IS NOT A MEASUREMENT
//!
//! This computes the dirty-bucket set **both ways** on the same shard and asserts they agree. The
//! fixture puts an object's blocks in buckets other than its routing bucket, because that is the
//! only case where the two could differ — a fixture where every object sits alone in its routing
//! bucket would agree trivially and prove nothing.
//!
//! Deleting the object-keyed set is safe exactly as far as this holds, and if it ever stops
//! holding, this fails with both sets printed rather than the removal being found later as a
//! bucket that stopped being dumped.
//!
//! rust-internal: reads the engine's own index structures, no product behaviour

use std::collections::BTreeSet;

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 48;

/// The dirty buckets, found the way the object-keyed set finds them.
fn by_object_set(engine: &crate::TemporalEngine) -> BTreeSet<u32> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let mut buckets: BTreeSet<u32> = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter_map(|(id, bucket)| bucket.dirty().then_some(*id))
        .collect();
    for object_key in shard.dirty_objects.iter() {
        buckets.extend(crate::engine::bucket_index_target_buckets_for_object_key(
            shard, object_key,
        ));
    }
    buckets
}

/// The dirty buckets, found the way the sibling arm finds them: the bucket's own bit, or any page
/// of it carrying one.
fn by_page_bit(engine: &crate::TemporalEngine) -> BTreeSet<u32> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .filter_map(|(id, bucket)| {
            let dirty = bucket.dirty() || bucket.block_index.values().any(|page| page.dirty);
            dirty.then_some(*id)
        })
        .collect()
}

#[test]
fn the_page_bit_answers_the_dirty_bucket_question() {
    // ---- A NARROW ROUTING RANGE, so keys SHARE buckets. On the default range a key lands alone
    // ---- by construction, every object's blocks sit in its own routing bucket, and the two
    // ---- methods would agree without the case that distinguishes them ever arising.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "dirty-bucket-equivalence".to_string(),
        shard_uri: "local://dirty-bucket-equivalence/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 15,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        loaded.status.ok,
        "the fixture could not load shard 1 on 0..15: {:?}",
        loaded.status
    );

    for index in 0..KEYS {
        let key = format!("dirty-probe-{index:04}");
        for command in [
            Command::StringSet { key: key.clone(), value: vec![b'v'; 96] },
            Command::HashSet {
                key: key.clone(),
                field: "f".to_string(),
                value: vec![b'h'; 96],
            },
            Command::SetAdd { key: key.clone(), member: vec![b's'; 96] },
        ] {
            let out = engine.execute(ExecuteRequest { shard_id: 1, command });
            assert!(out.status.ok, "fixture write {index}: {:?}", out.status);
        }
    }

    // ---- CLEAR DIRTINESS FIRST, then dirty a FEW. Comparing the two methods while every
    // ---- bucket is dirty would agree trivially: both return the whole map, and a method that
    // ---- always answered "all buckets" would pass. The case this arm exists for is a bucket
    // ---- that is dirty ONLY because a page of some other bucket's object lives in it, and that
    // ---- case is invisible until most buckets are clean.
    let _dumped = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });

    let (after_dump_objects, after_dump_dirty) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        (
            shard.dirty_objects.len(),
            shard.bucket_index.bucket_map.values().filter(|b| b.dirty()).count(),
        )
    };
    println!("    after the dump: {after_dump_objects} dirty object(s), {after_dump_dirty} dirty bucket(s)");

    // ---- NOW DIRTY A FEW, chosen to land in different buckets.
    for index in [1usize, 5, 9] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("dirty-probe-{index:04}"),
                value: vec![b'w'; 96],
            },
        });
        assert!(out.status.ok, "re-dirtying {index}: {:?}", out.status);
    }

    let via_objects = by_object_set(&engine);
    let via_pages = by_page_bit(&engine);

    let (object_count, bucket_count, pages) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        (
            shard.dirty_objects.len(),
            shard.bucket_index.bucket_map.len(),
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|b| b.block_index.len())
                .sum::<usize>(),
        )
    };

    println!();
    println!("  {KEYS} keys on a 16-bucket routing range");
    println!();
    println!("    dirty objects in the set      {object_count:>5}");
    println!("    buckets in the map            {bucket_count:>5}");
    println!("    pages across those buckets    {pages:>5}");
    println!("    dirty buckets via the SET     {:>5}", via_objects.len());
    println!("    dirty buckets via the PAGE    {:>5}", via_pages.len());
    println!();

    // ---- THE FIXTURE HAS TO REACH THE CASE. With one bucket per object the two agree trivially.
    assert!(
        bucket_count < KEYS,
        "{KEYS} keys landed in {bucket_count} buckets, so each key may still have a bucket to \
         itself and the blocks-outside-the-routing-bucket case this arm exists for never arose"
    );
    assert!(
        object_count > 0,
        "nothing is dirty, so both methods return the empty set and agreeing means nothing"
    );
    assert!(
        !via_objects.is_empty(),
        "the object-keyed method found no dirty bucket, so there is nothing to agree about"
    );
    // ---- AND NOT EVERY BUCKET, or both methods are answering "all of them" and a method that
    // ---- always said so would pass. This is the assertion that makes the agreement mean
    // ---- something.
    assert!(
        via_objects.len() < bucket_count,
        "every one of the {bucket_count} buckets reads dirty, so both methods return the whole          map and agreeing proves nothing. The dump above was supposed to leave most of them          clean -- check what it actually cleared before trusting the equality below"
    );

    // ---- AND THEY MUST AGREE. This is the whole claim: the page bit is sufficient, so the
    // ---- object-keyed set is not load-bearing for this question and can be removed.
    assert_eq!(
        via_objects, via_pages,
        "the two methods disagree.\n  via the object set: {via_objects:?}\n  via the page bit:   \
         {via_pages:?}\n\nThe page bit is only sufficient while \
         `push_object_dirty_onto_its_existing_pages` runs at EVERY site that marks an object dirty. \
         A disagreement means a marking site stopped pushing, and the object-keyed set is covering \
         for it -- fix the push rather than keeping the set, because a bucket missing from the page \
         view is a bucket that stops being dumped"
    );
}
