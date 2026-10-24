// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH OF THE TWO DIRTY RECORDS EACH PRESENCE CONSUMER READS -- forced apart so the answer is
//! observable.
//!
//! # WHAT THIS IS FOR
//!
//! Three consumers asked `BucketStorageSummary::dirty_object_count > 0`, which is a dirty-BUCKET
//! question answered from a count of dirty OBJECTS. The bucket records that fact itself, on its
//! own node, and `whether_a_bucket_can_count_its_own_dirty_objects` measures the two records
//! agreeing on every bucket in both directions -- including the ones carrying dirt no page walk
//! can see -- and measures the node's flag to be an INDEPENDENT record rather than a cache of the
//! set, by neutralising each of its three maintainers in turn.
//!
//! That agreement is what makes the move safe. It is also what makes the move INVISIBLE to an
//! ordinary test: while the two records agree on every bucket, a consumer reading either one
//! satisfies any assertion that compares the consumer against the truth. So this test FORCES THEM
//! APART and asks each consumer again.
//!
//! The divergence is planted in the FIXTURE, never in the code under test. Two buckets are picked
//! out of the live shard and their nodes' flags assigned directly:
//!
//!     forced_dirty   node reads dirty, the object-keyed set holds NOTHING for it
//!     forced_clean   node reads clean, the object-keyed set holds a mark for it
//!
//! Each is a state the two records cannot both describe, so every consumer now reports one answer
//! or the other and cannot report both. That is only legitimate because the two records are
//! independent -- a flag derived from the set could not be held apart from it, and the measurement
//! above is what establishes that it is not.
//!
//! # THE THREE CONSUMERS, AND WHAT EACH ONE'S ERROR COSTS
//!
//!     storage_lifecycle_plan          dump selection      an extra bucket is dumped
//!     storage_recovery_boundary       which buckets the newest dump does not cover
//!     storage_physical_index_report   a per-bucket report flag
//!
//! None of the three decides durability. Dump selection over-reporting costs a dump; the other two
//! are report fields. The two WAL-reclaim checks are NOT here and do not move with them: there
//! `dirty_object_count == 0` is the arm that counts a bucket as covered and contributing no log
//! floor, so a bucket wrongly reading dirty skips that arm, cannot name a claim because its
//! `first_dirty_wal_sequence` was cleared alongside `dirty = false`, and blocks the whole reclaim
//! plan. That move needs the over-reporting direction measured empty on a shard that has actually
//! dumped, which is a different experiment from this one.
//!
//! # AND ONE OF THE THREE WAS ALREADY DEAD, WHICH IS MEASURED HERE RATHER THAN ASSUMED
//!
//! `storage_physical_index_report` built each row with `dirty: summary.dirty_object_count > 0` and
//! then, in a LATER loop over the same `bucket_map`, assigned `bucket.dirty = node.dirty()` over
//! the top of it. So the count reached that field only for a bucket the summaries name and
//! `bucket_map` does not -- and such a bucket has no node for a mark to create, so the set loop
//! contributes no count to it and the initializer could only ever have written `false`. The field
//! has been reading the node all along. That is asserted below as the unreachability it rests on,
//! so the claim is a property of the code rather than a reading of it, and the change at that site
//! is a dead initializer stopping naming a figure it never passed on.
//!
//! rust-internal: reads and writes the engine's own index structures, no product behaviour

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 48;
const END_ROUTING_BUCKET: u32 = 15;

/// Every bucket's two records of the same fact: `(node.dirty(), set count)`.
fn both_records(engine: &crate::TemporalEngine) -> BTreeMap<u32, (bool, u64)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| {
            (
                *routing_bucket,
                (bucket.dirty(), shard.dirty_objects.bucket_count(*routing_bucket)),
            )
        })
        .collect()
}

#[test]
fn which_record_the_dirty_bucket_consumers_read() {
    // ---- A NARROW ROUTING RANGE, so keys SHARE buckets and the removals' non-page-backed records
    // ---- route to buckets of their own. On the default whole-keyspace range every key gets a
    // ---- bucket to itself and the populations stop overlapping.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let loaded = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "which-dirty-record".to_string(),
        shard_uri: "local://which-dirty-record/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: END_ROUTING_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        loaded.status.ok,
        "the fixture could not load shard 1 on 0..{END_ROUTING_BUCKET}: {:?}",
        loaded.status
    );

    for index in 0..KEYS {
        let key = format!("which-record-{index:04}");
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

    // ---- DUMP CLEAN FIRST, then dirty a FEW, so most buckets are clean and there is somewhere to
    // ---- plant a node-dirty bucket the set knows nothing about.
    let _dumped = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    for index in [1usize, 5, 9] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("which-record-{index:04}"),
                value: vec![b'w'; 96],
            },
        });
        assert!(out.status.ok, "re-dirtying {index}: {:?}", out.status);
    }
    for index in [2usize, 6] {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete { key: format!("which-record-{index:04}") },
        });
        assert!(out.status.ok, "removing {index}: {:?}", out.status);
    }

    // ---- THEN A DUMP OF ONE BUCKET, so the NEWEST manifest names a PROPER SUBSET of the dirty
    // ---- buckets. Without this the newest manifest names every bucket the first dump covered,
    // ---- the dump-coverage report is empty whichever record it reads, and the clause below would
    // ---- be asserting nothing. A partial dump is also the shape `max_dump_buckets_per_round`
    // ---- asks for in production, so this is the ordinary case rather than a contrived one.
    let one_dirty_bucket = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        shard
            .bucket_index
            .bucket_map
            .iter()
            .find(|(_routing_bucket, bucket)| bucket.dirty())
            .map(|(routing_bucket, _bucket)| *routing_bucket)
            .expect("the writes above left at least one bucket dirty")
    };
    let _partial = engine.apply_storage_lifecycle(crate::StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: vec![one_dirty_bucket],
        ..Default::default()
    });

    // ---- WHAT THE SHARD LOOKS LIKE BEFORE ANYTHING IS FORCED, which is the state production
    // ---- reaches and the state in which the two records agree.
    let natural = both_records(&engine);
    let naturally_dirty: BTreeSet<u32> = natural
        .iter()
        .filter(|(_bucket, (node_dirty, _count))| *node_dirty)
        .map(|(bucket, _)| *bucket)
        .collect();
    let naturally_counted: BTreeSet<u32> = natural
        .iter()
        .filter(|(_bucket, (_node_dirty, count))| *count > 0)
        .map(|(bucket, _)| *bucket)
        .collect();

    // ---- THE TWO BUCKETS TO FORCE APART, chosen from the live shard rather than named as
    // ---- literals, so the fixture does not depend on where this routing range happens to put a
    // ---- key.
    let forced_dirty = natural
        .iter()
        .find(|(_bucket, (node_dirty, count))| !*node_dirty && *count == 0)
        .map(|(bucket, _)| *bucket);
    let forced_clean = natural
        .iter()
        .find(|(_bucket, (node_dirty, count))| *node_dirty && *count > 0)
        .map(|(bucket, _)| *bucket);

    println!();
    println!("  {KEYS} keys on a {}-bucket routing range", END_ROUTING_BUCKET + 1);
    println!();
    println!("    buckets in the map                      {:>6}", natural.len());
    println!("    node reads dirty                        {:>6}", naturally_dirty.len());
    println!("    the set holds a mark                     {:>6}", naturally_counted.len());
    println!("    the two records agree on every bucket    {:>6}", naturally_dirty == naturally_counted);
    println!();
    println!("    bucket to force DIRTY (set holds none)  {forced_dirty:>6?}");
    println!("    bucket to force CLEAN (set holds a mark){forced_clean:>6?}");
    println!();

    // ---- THE FIXTURE HAS TO REACH THE REGIME. Each clause stands alone so a failure says which
    // ---- one did not hold, and all of them come before any conclusion: a plant stops at the
    // ---- first failing assertion, so a conclusion placed above a regime clause it overlaps would
    // ---- hide it.
    assert!(
        natural.len() < KEYS,
        "[clause: regime_shared_buckets] {KEYS} keys landed in {} buckets, so each key may still have a bucket to itself and the \
         shared-bucket case never arose",
        natural.len()
    );
    assert_eq!(
        naturally_dirty, naturally_counted,
        "[clause: regime_records_agree] the node's flag and the object-keyed set DISAGREE on this shard before anything is \
         forced: node {naturally_dirty:?} against set {naturally_counted:?}. This test's whole \
         method is that they agree naturally and have to be forced apart; a natural disagreement \
         means the premise of the move has changed, and \
         `whether_a_bucket_can_count_its_own_dirty_objects` is the test that owns it"
    );
    assert!(
        !naturally_dirty.is_empty(),
        "[clause: regime_something_dirty] no bucket reads dirty, so nothing is dirty and every consumer below reports nothing \
         whichever record it reads"
    );
    assert!(
        naturally_dirty.len() < natural.len(),
        "[clause: regime_some_clean] all {} buckets read dirty, so there is no clean bucket to force dirty and a consumer that \
         always said \"every bucket\" would pass. The dump above was supposed to leave most of \
         them clean",
        natural.len()
    );
    let Some(forced_dirty) = forced_dirty else {
        panic!(
            "[clause: regime_room_to_force_dirty] every bucket in the map either reads dirty or holds a mark, so there is nowhere to \
             plant a node-dirty bucket the set knows nothing about and the OVER-reporting \
             direction cannot be put to any consumer"
        )
    };
    let Some(forced_clean) = forced_clean else {
        panic!(
            "[clause: regime_room_to_force_clean] no bucket both reads dirty and holds a mark, so there is nowhere to plant a \
             node-clean bucket the set still marks and the UNDER-reporting direction cannot be put \
             to any consumer"
        )
    };

    // ---- NOW FORCE THEM APART. Assigned straight onto the node, which is legitimate here only
    // ---- because the flag is an independent record: a flag derived from the set would be
    // ---- recomputed back into agreement and this plant would measure nothing.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");
        shard
            .bucket_index
            .bucket_map
            .get_mut(&forced_dirty)
            .expect("the bucket chosen above is in the map")
            .set_dirty(true);
        shard
            .bucket_index
            .bucket_map
            .get_mut(&forced_clean)
            .expect("the bucket chosen above is in the map")
            .set_dirty(false);
    }

    // ---- ASK ALL THREE CONSUMERS. `storage_lifecycle_plan` PLANS and does not apply: applying
    // ---- would dump, and a dump clears the very flags that were just forced.
    let plan = engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    let plan_dirty_buckets: BTreeSet<u32> = plan.dirty_buckets.iter().copied().collect();
    let boundary = engine.storage_recovery_boundary_report(1);
    let missing_dump_buckets: BTreeSet<u32> =
        boundary.missing_dump_bucket_ids.iter().copied().collect();
    let physical = engine.storage_physical_index_report(1);
    let physical_dirty: BTreeMap<u32, bool> = physical
        .bucket_nodes
        .iter()
        .map(|bucket| (bucket.routing_bucket, bucket.dirty))
        .collect();
    let summaries_by_bucket: BTreeMap<u32, u64> = engine
        .bucket_storage_summaries(1)
        .into_iter()
        .map(|summary| (summary.routing_bucket, summary.dirty_object_count))
        .collect();

    // ---- AND RE-READ THE SHARD, because a plant is evidence only once it is known to have
    // ---- landed. Any of the three calls above could have folded the flag back into step with
    // ---- the set -- two sites do exactly that on other paths -- and then the answers below would
    // ---- be about a shard that no longer holds the divergence.
    let after = both_records(&engine);
    let node_dirty_now: BTreeSet<u32> = after
        .iter()
        .filter(|(_bucket, (node_dirty, _count))| *node_dirty)
        .map(|(bucket, _)| *bucket)
        .collect();
    let set_marked_now: BTreeSet<u32> = after
        .iter()
        .filter(|(_bucket, (_node_dirty, count))| *count > 0)
        .map(|(bucket, _)| *bucket)
        .collect();

    println!("    bucket   node.dirty   set count   plan   missing-dump   report.dirty");
    let every_bucket: BTreeSet<u32> = after
        .keys()
        .copied()
        .chain(plan_dirty_buckets.iter().copied())
        .chain(missing_dump_buckets.iter().copied())
        .chain(physical_dirty.keys().copied())
        .collect();
    for bucket in &every_bucket {
        let (node_dirty, count) = after.get(bucket).copied().unwrap_or((false, 0));
        println!(
            "    {bucket:>6} {node_dirty:>12} {count:>11} {:>6} {:>14} {:>14}",
            plan_dirty_buckets.contains(bucket),
            missing_dump_buckets.contains(bucket),
            physical_dirty
                .get(bucket)
                .map(|dirty| dirty.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
    }
    println!();
    println!("    node reads dirty, set holds nothing: {:?}", node_dirty_now.difference(&set_marked_now).collect::<Vec<_>>());
    println!("    set holds a mark, node reads clean:  {:?}", set_marked_now.difference(&node_dirty_now).collect::<Vec<_>>());
    println!();

    // ---- THE PLANT LANDED, asserted before any conclusion is read. Both directions, each on its
    // ---- own, because one surviving and the other not is a different fixture from the one the
    // ---- conclusions are about.
    assert_eq!(
        after.get(&forced_dirty).copied().map(|(node_dirty, _)| node_dirty),
        Some(true),
        "[clause: plant_forced_dirty_landed] bucket {forced_dirty} was forced to read dirty and no longer does, so something between \
         the plant and here folded the flag back into step with the set. The OVER-reporting \
         direction is not present in the shard the consumers below were asked about"
    );
    assert_eq!(
        after.get(&forced_dirty).copied().map(|(_node_dirty, count)| count),
        Some(0),
        "[clause: plant_forced_dirty_set_empty] bucket {forced_dirty} was chosen because the object-keyed set held nothing for it and the \
         set now holds {:?} mark(s), so this bucket no longer separates the two records",
        after.get(&forced_dirty).copied().map(|(_node_dirty, count)| count)
    );
    assert_eq!(
        after.get(&forced_clean).copied().map(|(node_dirty, _)| node_dirty),
        Some(false),
        "[clause: plant_forced_clean_landed] bucket {forced_clean} was forced to read clean and no longer does, so something between \
         the plant and here folded the flag back into step with the set -- which is what the two \
         `has_bucket`/`contains` OR terms do on the paths that run them. The UNDER-reporting \
         direction is not present in the shard the consumers below were asked about"
    );
    assert!(
        after.get(&forced_clean).copied().map(|(_node_dirty, count)| count).unwrap_or(0) > 0,
        "[clause: plant_forced_clean_set_marked] bucket {forced_clean} was chosen because the object-keyed set held a mark for it and the \
         set now holds none, so this bucket no longer separates the two records"
    );

    // ---- AND THE CONCLUSIONS, one consumer per block so a failure names the consumer that moved
    // ---- rather than reporting that "a consumer changed".

    // DUMP SELECTION. Over-reporting here costs an extra dump; under-reporting is undumped state
    // no round would pick up, which is why this reads the record that cannot miss a bucket.
    assert!(
        plan_dirty_buckets.contains(&forced_dirty),
        "[clause: dump_filter_reads_node_over] dump selection did not select bucket {forced_dirty}, whose own node reads DIRTY while the \
         object-keyed set holds nothing for it. So the filter is still reading \
         `dirty_object_count > 0` rather than the bucket's own flag.\n\n  node reads dirty: \
         {node_dirty_now:?}\n  set holds a mark: {set_marked_now:?}\n  selected:         \
         {plan_dirty_buckets:?}"
    );
    assert!(
        !plan_dirty_buckets.contains(&forced_clean),
        "[clause: dump_filter_reads_node_under] dump selection selected bucket {forced_clean}, whose own node reads CLEAN while the \
         object-keyed set still holds a mark for it. So the filter is still reading \
         `dirty_object_count > 0`.\n\n  node reads dirty: {node_dirty_now:?}\n  set holds a mark: \
         {set_marked_now:?}\n  selected:         {plan_dirty_buckets:?}"
    );
    assert_eq!(
        plan_dirty_buckets, node_dirty_now,
        "[clause: dump_filter_whole_set] dump selection and the node's own flag name different bucket sets. The two clauses above \
         pin the forced pair; this one pins every other bucket too, so a filter that read the node \
         for some buckets and the set for others cannot pass"
    );

    // WHICH BUCKETS THE NEWEST DUMP DOES NOT COVER. This one is filtered against the newest
    // manifest's bucket list as well, so what it reports is an INTERSECTION and cannot be compared
    // to the node's set as an equality: a node-dirty bucket the manifest happens to name is
    // excluded for a second reason. The containment is the direction that still pins it -- nothing
    // may be reported that the node does not read dirty -- and the forced-clean clause after it is
    // the one a count-reading consumer fails.
    assert!(
        missing_dump_buckets.is_subset(&node_dirty_now),
        "[clause: dump_coverage_subset_of_node] the dump-coverage report names bucket(s) {:?} that the node does not read dirty, so it is \
         still deriving presence from `dirty_object_count > 0`.\n\n  reported missing: \
         {missing_dump_buckets:?}\n  node reads dirty: {node_dirty_now:?}\n  set holds a mark: \
         {set_marked_now:?}",
        missing_dump_buckets.difference(&node_dirty_now).collect::<Vec<_>>()
    );
    assert!(
        !missing_dump_buckets.contains(&forced_clean),
        "[clause: dump_coverage_reads_node_under] the dump-coverage report names bucket {forced_clean}, whose own node reads CLEAN while the \
         object-keyed set still holds a mark for it. So that report is still reading \
         `dirty_object_count > 0`"
    );

    // THE PER-BUCKET REPORT FLAG, and the unreachability that makes its initializer dead. The
    // field is assigned from the node in a later loop over the same map, so the count reached it
    // only for a bucket the summaries name and `bucket_map` does not -- and this clause is what
    // says no such bucket can carry a count.
    for (bucket, count) in &summaries_by_bucket {
        if *count == 0 {
            continue;
        }
        assert!(
            after.contains_key(bucket),
            "[clause: report_initializer_unreachable] bucket {bucket} carries a dirty-object count of {count} in its summary while \
             `bucket_map` holds no node for it. The count can then reach \
             `StoragePhysicalBucketNode::dirty` through the row initializer, which the later \
             assignment from the node does not overwrite for a bucket it cannot find -- so that \
             initializer is no longer the dead code this module's header measures it to be, and \
             the report field has a second source again"
        );
    }
    for (bucket, dirty) in &physical_dirty {
        let node_dirty = after.get(bucket).copied().map(|(node_dirty, _)| node_dirty);
        if node_dirty.is_none() {
            continue;
        }
        assert_eq!(
            Some(*dirty),
            node_dirty,
            "[clause: report_flag_follows_node] the physical-index report reads bucket {bucket} as dirty={dirty} where its own node \
             reads {node_dirty:?}"
        );
    }
}
