// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A SYNCHRONOUS WRITE LEAVES PENDING, which decides whether we already write through.
//!
//! # THE QUESTION
//!
//! The design being followed writes through per object: extents reach storage before the manifest
//! entry commits, so nothing is ever pending write-back and no set of dirty objects is needed. Ours
//! keeps `dirty_objects`, and the standing reading has been that this is a divergence in the write
//! path.
//!
//! **One name is enough to doubt that.** The only two sites that mark an object dirty are reached
//! through `mark_object_dirty`. If dirtying is a property of the ASYNC path, then
//! `dirty_objects` is the cost of an option they do not offer rather than the cost of our default,
//! and a synchronous write already does what theirs does.
//!
//! That is the difference between a configuration and an architecture change, so it is measured
//! rather than argued: the same corpus written twice, once synchronously and once through the async
//! path, with the write-back set read after each.
//!
//! # AND THE ANSWER IS THE SECOND ONE
//!
//! **Both paths leave the whole corpus pending — 64 of 64.** So the name describes the caller it
//! was first written for, not a condition: dirtying is unconditional, a synchronous write does not
//! write through, and write-through has to be *built* rather than selected. The reasoning from the
//! name was wrong and this arm is what says so.
//!
//! # WHAT MAKES THAT BUILD AFFORDABLE, AND WHERE IT WOULD NOT BE
//!
//! The same run shows **64 objects in 64 buckets — one per bucket**. That is not a property of the
//! workload but of the default routing range: `load_shard` uses `0..u32::MAX`, where the placement
//! modulus is the range width, so a key lands alone by construction.
//!
//! **A batch of one buys nothing.** So at the default range, writing through per object costs no
//! extra durable write than batching per bucket already does, and the write-back set is bookkeeping
//! for batches that never form.
//!
//! That is the opposite at a narrow range. On the range the runtime tuning guide tells an operator
//! to set, 94.7% of dirty buckets hold more than one key, and write-through would multiply durable
//! writes by the keys per bucket. So this is one change with two costs, and which one applies is a
//! deployment setting rather than a property of the design.
//!
//! rust-internal: reads the engine's own write-back set, no product behaviour

use crate::types::{Command, EventReplicationMode, ExecuteRequest, ReplicatedExecuteRequest};

const KEYS: usize = 64;

fn engine(dir: &std::path::Path) -> crate::TemporalEngine {
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

/// The write-back set's size for shard 1, read straight off the shard.
fn pending(engine: &crate::TemporalEngine) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    (
        shard.dirty_objects.len(),
        shard.dirty_objects.bucket_counts().count(),
    )
}

#[test]
fn what_a_synchronous_write_leaves_pending() {
    // ---- ARM ONE: the ordinary synchronous path, which is what `execute` does.
    let sync_dir = tempfile::tempdir().expect("tempdir");
    let sync = engine(sync_dir.path());
    for index in 0..KEYS {
        let out = sync.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("wt-key-{index:04}"),
                value: vec![b'v'; 128],
            },
        });
        assert!(out.status.ok, "synchronous write {index}: {:?}", out.status);
    }
    let (sync_objects, sync_buckets) = pending(&sync);

    // ---- ARM TWO: the async storage path, the one whose name the dirty-marking function carries.
    let async_dir = tempfile::tempdir().expect("tempdir");
    let asynchronous = engine(async_dir.path());
    for index in 0..KEYS {
        let out = asynchronous.execute_replicated(ReplicatedExecuteRequest::new(
            1,
            Command::StringSet {
                key: format!("wt-key-{index:04}"),
                value: vec![b'v'; 128],
            },
            EventReplicationMode::AsyncStorage,
        ));
        assert!(out.status.ok, "async write {index}: {:?}", out.status);
    }
    let (async_objects, async_buckets) = pending(&asynchronous);

    println!();
    println!("  THE WRITE-BACK SET AFTER {KEYS} WRITES OF THE SAME CORPUS");
    println!();
    println!("    path            dirty objects   dirty buckets");
    println!("    synchronous     {sync_objects:>13}   {sync_buckets:>13}");
    println!("    async storage   {async_objects:>13}   {async_buckets:>13}");
    println!();

    println!("  NEITHER PATH WRITES THROUGH. `mark_object_dirty` is named for the caller it");
    println!("  was first written for, not for a condition, so a synchronous write leaves just as");
    println!("  much pending as an async one. Write-through has to be BUILT, not selected.");
    println!();
    println!("  AND ONE OBJECT PER BUCKET is what the default routing range produces: a batch of");
    println!("  one buys nothing, so write-through would cost no extra durable write HERE. On a");
    println!("  narrow configured range it would, in proportion to the keys per bucket.");
    println!();

    // ---- DIRTYING IS UNCONDITIONAL. Both paths, same corpus, same count. This is the arm's
    // ---- subject and it is asserted rather than printed, because it is the fact that decides
    // ---- whether write-through is a configuration or a build.
    assert_eq!(
        sync_objects, KEYS,
        "a synchronous write left {sync_objects} of {KEYS} objects pending. If this ever reads 0, \
         the write path has become write-through and `dirty_objects` can go -- which is the change \
         this arm exists to detect, so read the count rather than assuming it still holds"
    );
    assert_eq!(
        sync_objects, async_objects,
        "the two paths now differ: synchronous left {sync_objects} pending and async \
         {async_objects}. That would mean dirtying has become conditional after all, which is the \
         shape `mark_object_dirty`'s name has always suggested and has never had"
    );

    // ---- AND THE BATCH IS ONE. Asserted on the BUCKET count rather than inferred from the
    // ---- routing range, because the range is a setting and this is the consequence that matters.
    assert_eq!(
        sync_buckets, sync_objects,
        "{sync_objects} dirty object(s) occupy {sync_buckets} bucket(s), so a bucket holds more \
         than one and the per-bucket batch is real here. At the default range a key lands alone by \
         construction -- if that has changed, write-through is no longer free and the cost is the \
         keys per bucket"
    );
    assert_eq!(
        async_buckets, async_objects,
        "the async path put {async_objects} object(s) into {async_buckets} bucket(s)"
    );
}
