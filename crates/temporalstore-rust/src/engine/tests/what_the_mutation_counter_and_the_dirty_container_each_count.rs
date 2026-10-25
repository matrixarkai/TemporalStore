// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE MUTATION COUNTER AND THE DIRTY CONTAINER COUNT DIFFERENT THINGS, and the dump cadence
//! already reads the counter.
//!
//! # WHAT THIS IS FOR
//!
//! Dirty bookkeeping in this store is moving toward scalars. Three questions are asked of it, and
//! this measures which of them a scalar already answers, because two of the three turn out to be
//! answered by one already and the third is answered by neither.
//!
//!     is anything dirty          `BucketNode::dirty`, one record per bucket
//!     how much since the dump    `undumped_objects_by_shard`, one u64 per shard in the log
//!     WHICH objects are dirty    `dirty_objects`, a container of keys
//!
//! The first moved to the node's own flag (#2338, #2340) and the presence question now has ONE
//! record. The second is already a scalar: the log keeps a per-shard count of object mutations
//! appended since the last dump, reset only once a dump manifest is durably written, and the dump
//! cadence is a predicate over it and two sequence numbers -- no container is consulted. That is
//! pinned below rather than read off the code.
//!
//! THE THIRD IS THE OPEN ONE, and the point of this measurement is that the second cannot stand in
//! for it. A count of MUTATIONS and a count of DISTINCT DIRTY OBJECTS are different numbers, by
//! whatever factor the writer batches -- so replacing the container's count with the mutation
//! counter is not a substitution of one spelling for another, it is a change of question. The
//! factor is measured here on a fixture built to separate them, rather than left as the remark at
//! `StorageLifecyclePlan::undumped_wal_objects` that the two "disagree by whatever factor the
//! writer batches".
//!
//! # WHY THIS IS NOT THE COUNT BEING DERIVED
//!
//! `whether_a_bucket_can_count_its_own_dirty_objects` measures the per-bucket dirty-object count to
//! be NOT derivable -- a page walk finds 3 of 11 marks at 17x the work. Nothing here derives it.
//! These are two independently maintained counters being compared so that the difference between
//! what they count is a figure, which is what a later change replacing one with the other would
//! need before it could be priced.
//!
//! rust-internal: reads the engine's own index structures and log counters, no product behaviour

use std::collections::BTreeSet;

use crate::types::{Command, ExecuteRequest};

/// Few keys, many writes each, so a count of mutations and a count of objects cannot coincide.
const DISTINCT_KEYS: usize = 6;
const WRITES_PER_KEY: usize = 9;
const END_ROUTING_BUCKET: u32 = 15;

#[test]
fn what_the_mutation_counter_and_the_dirty_container_each_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "mutation-counter".to_string(),
        shard_uri: "local://mutation-counter/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: END_ROUTING_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        loaded.status.ok,
        "the fixture could not load shard 1: {:?}",
        loaded.status
    );

    // ---- OVERWRITE THE SAME FEW KEYS REPEATEDLY. A fixture that writes each key ONCE makes a
    // ---- mutation count and a distinct-object count the same number, and then this test cannot
    // ---- tell one from the other -- which is the whole question.
    for round in 0..WRITES_PER_KEY {
        for index in 0..DISTINCT_KEYS {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("mutation-counter-{index:04}"),
                    value: vec![b'a' + (round as u8 % 26); 96],
                },
            });
            assert!(out.status.ok, "write {index}/{round}: {:?}", out.status);
        }
    }

    // ---- THE THREE FIGURES, each read from the thing that owns it.
    let plan = engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    let mutations = plan.undumped_wal_objects;
    let log_records = plan.undumped_wal_records;
    let (distinct_dirty_objects, dirty_buckets, bucket_total) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        let dirty: BTreeSet<u32> = shard
            .bucket_index
            .bucket_map
            .iter()
            .filter(|(_bucket, node)| node.dirty())
            .map(|(bucket, _node)| *bucket)
            .collect();
        (
            shard.dirty_objects.len(),
            dirty.len(),
            shard.bucket_index.bucket_map.len(),
        )
    };

    println!();
    println!(
        "  {DISTINCT_KEYS} distinct keys, {WRITES_PER_KEY} writes each = {} writes issued",
        DISTINCT_KEYS * WRITES_PER_KEY
    );
    println!();
    println!("    object mutations since the dump (a SCALAR in the log) {mutations:>8}");
    println!("    log records since the dump      (two SEQUENCES)      {log_records:>8}");
    println!("    distinct dirty objects          (the CONTAINER)      {distinct_dirty_objects:>8}");
    println!("    dirty buckets                   (one FLAG each)      {dirty_buckets:>8}");
    println!("    buckets in the map                                   {bucket_total:>8}");
    println!();

    // ---- THE FIXTURE HAS TO REACH THE REGIME WHERE THE TWO COULD DIFFER, asserted first and one
    // ---- clause at a time: a probe that never enters the contested regime agrees with whatever
    // ---- it is asked.
    assert!(
        mutations > 0,
        "[clause: regime_counter_moved] the log recorded 0 object mutations, so the scalar never \
         moved and comparing it against anything says nothing"
    );
    assert!(
        distinct_dirty_objects > 0,
        "[clause: regime_container_filled] the dirty container holds nothing, so there is no \
         count to compare the scalar against"
    );
    assert!(
        mutations > distinct_dirty_objects as u64,
        "[clause: regime_mutations_exceed_objects] {mutations} mutation(s) against \
         {distinct_dirty_objects} distinct dirty object(s), so this fixture did NOT write any key \
         more than once and the two counts coincide by construction. The comparison below cannot \
         tell a mutation count from an object count -- raise WRITES_PER_KEY"
    );

    // ---- AND THE CONCLUSIONS, one per block.

    // THE TWO COUNT DIFFERENT THINGS, and the factor is the figure that matters: it is the ratio a
    // replacement would be wrong by, and it is set by how much the writer batches, not by anything
    // this store could correct for.
    assert_ne!(
        mutations, distinct_dirty_objects as u64,
        "[clause: counter_is_not_the_object_count] the mutation counter and the distinct-dirty-object \
         count have become the same number ({mutations}), so this fixture no longer separates them \
         and nothing here establishes that one cannot stand in for the other"
    );
    println!(
        "    the mutation counter reads {mutations} where the container holds \
         {distinct_dirty_objects} distinct object(s): a factor of {:.2}x, set by how much the \
         writer batches",
        mutations as f64 / distinct_dirty_objects as f64
    );

    // THE DIRTY-BUCKET VIEW IS O(BUCKETS) AND BOUNDED BY THE ROUTING RANGE, which is what makes
    // the presence question cheap to answer per bucket while the count is not.
    assert!(
        dirty_buckets <= bucket_total,
        "[clause: dirty_buckets_bounded_by_the_map] {dirty_buckets} dirty bucket(s) in a map of \
         {bucket_total}, which cannot happen: the dirty view is a flag per bucket and is bounded \
         by the map holding them"
    );
    assert!(
        (bucket_total as u32) <= END_ROUTING_BUCKET + 1,
        "[clause: dirty_buckets_bounded_by_the_range] the map holds {bucket_total} bucket(s) on a \
         routing range of {}, so the per-bucket view is not bounded by the range after all",
        END_ROUTING_BUCKET + 1
    );

    // AND THE CADENCE ALREADY READS THE SCALAR, NOT THE CONTAINER. Two plans over the SAME shard
    // state, differing only in the configured threshold: one above the mutation count, one at 0.
    // The container is identical across both, so a predicate consulting it could not tell them
    // apart.
    let delayed = engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        min_undumped_wal_records: log_records.saturating_add(1_000),
        min_undumped_wal_bytes: u64::MAX,
        ..Default::default()
    });
    let released = engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        min_undumped_wal_records: 0,
        ..Default::default()
    });
    println!(
        "    threshold above the count -> dump_delayed {}   threshold 0 -> dump_delayed {}",
        delayed.dump_delayed, released.dump_delayed
    );
    assert!(
        delayed.dump_delayed,
        "[clause: cadence_waits_below_the_threshold] with the record threshold set \
         {} above the {log_records} undumped record(s) the dump was NOT delayed, so the cadence is \
         not reading the undumped-record scalar it is configured against",
        1_000
    );
    assert!(
        !released.dump_delayed,
        "[clause: cadence_releases_at_no_threshold] with no record threshold configured the dump \
         was still delayed, so the cadence is holding on something other than its scalars"
    );
}
