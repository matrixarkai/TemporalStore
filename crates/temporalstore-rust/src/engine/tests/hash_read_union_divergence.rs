// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// THE HASH WHOLE-OBJECT READ SERVES BOTH SOURCES, AND SAYS SO WHEN THEY DISAGREE
// =================================================================================================
//
//! WHY THIS MODULE EXISTS, AND WHAT IT REPLACES.
//!
//! #2078 set out to prove that the durable container `shard.hashes` holds every field the fold
//! installs, by comparing it against the page index after a reload. IT DOES NOT PROVE THAT. The
//! LOAD PATH BUILDS A HASH FIELD MAP BY WALKING THE PAGE INDEX AND MERGES IT INTO THE CONTAINER,
//! from BOTH of the two functions in `storage_bucket_internals` that do it:
//! `rebuild_unserialized_model_maps_from_bucket_index` and
//! `reconcile_secondary_views_from_bucket_index`, each through
//! `RecordedHashContainer::reconcile_from_durable`. So after a
//! reload the container names everything the index names BY CONSTRUCTION, and the fixture compares
//! the index against a structure derived from it. Disabling the container carry outright leaves
//! that test passing. Measured while establishing this: `2 checks, 0 REBUILDS, 6 pages walked`, so
//! the reverse mechanism -- the index being rebuilt from the map -- did not fire either and a
//! decoded index does not rescue the comparison.
//!
//! THE CITATION THIS MODULE WAS FIRST WRITTEN WITH WAS WRONG TWICE, and is recorded here because
//! the wrong one is the plausible one. It named `reconcile_secondary_views_from_bucket_index`
//! alone, and it named the merge `shard.hashes = fill_absent_elements(hashes, persisted, ..)`.
//! There is no such expression: `fill_absent_elements` is assigned to `shard.lists`, `shard.zsets`
//! and `shard.sets` (`:5195`, `:5204`, `:5213`) and never to `hashes`, which takes the merge
//! through `reconcile_from_durable` instead. The refutation does not depend on either detail --
//! it is stronger without them, because it holds on BOTH load paths rather than one.
//!
//! AND ONE TRAP WORTH NAMING, because grepping for the wrong expression leads straight into it.
//! `recorded_hash_container_invariant.rs` asserts that
//! `assignments("    shard.hashes = fill_absent_elements(")` is NOT empty, which reads like
//! evidence that the tree contains such an assignment. It is not: the argument is a PLANTED
//! STRING LITERAL and the assertion is a self-test on the matcher, checking it can still see a
//! multi-line assignment. A sweep of `engine/` for `shard.hashes = ` finds only comments and test
//! prose. Read a guard's matcher and its arguments, not the shape of its name.
//!
//! WHAT IS HERE INSTEAD. `Command::HashGetAll` resolves field names from the container, still
//! consults the page index, and serves the UNION; and it counts, at read time, each field the
//! INDEX named that the CONTAINER lacked. That is a run-time measurement against real traffic,
//! which is strictly stronger than a fixture in one specific way: it samples whatever route each
//! request took, including routes no fixture drives, and there is no load path between the two
//! sources to fill one from the other.
//!
//! WHAT A ZERO FROM THAT COUNTER MEANS, AND WHAT IT DOES NOT. A zero means NO DIVERGENCE ON THE
//! ROUTES THE TRAFFIC TOOK. IT IS NOT PROOF OF COMPLETENESS ON THE ROUTES IT DID NOT TAKE. An
//! object no request read is an object this counter never looked at. Saying otherwise would
//! manufacture a second #2078.
//!
//! WHAT THESE TESTS DO, IN ORDER, AND WHY THE ORDER IS THE POINT. A divergence counter that has
//! never been seen to move is not evidence of anything -- it reads identically to a counter whose
//! increment was deleted. So the arms are: a NEGATIVE CONTROL on a store whose two sources agree
//! (the counter must not move), then a PLANT that removes one field from the container alone (the
//! counter must move, and the union must still serve that field correctly), then a plant in the
//! OPPOSITE direction that takes a field off the index alone (the union must serve it from the
//! container, and it must NOT be counted, because that direction loses no read).

#![allow(clippy::all)]
use super::*;

const PLANTED_KEY: &str = "tenant/1/union-divergence/object";

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "hash-read-union-divergence".to_string(),
        shard_uri: "local://hash-read-union-divergence/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: crate::DEFAULT_END_ROUTING_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

/// What the served path answers, as (field, value) pairs sorted so the comparison does not depend
/// on either source's iteration order. A `HashMap`'s order is randomised per instance, so an
/// ordered comparison here would be a flake with a plausible-looking message.
fn served(engine: &TemporalEngine, key: &str) -> Vec<(String, Vec<u8>)> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGetAll {
                key: key.to_string(),
            },
        })
        .response
    {
        CommandResponse::HashEntries { mut entries } => {
            entries.sort();
            entries
        }
        other => panic!("HashGetAll answered {other:?}"),
    }
}

/// The counter, read off the report surface rather than off the static, so these tests exercise
/// the plumbing a reader would actually use. If the field stops being published the tests stop
/// compiling.
fn divergences(engine: &TemporalEngine) -> u64 {
    engine
        .hash_read_divergence_report()
        .index_named_fields_the_container_lacked
}

fn expected(fields: &[&str]) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = fields
        .iter()
        .map(|field| (field.to_string(), format!("value-of-{field}").into_bytes()))
        .collect();
    out.sort();
    out
}

fn seed(engine: &TemporalEngine, fields: &[&str]) {
    for field in fields {
        write(
            engine,
            Command::HashSet {
                key: PLANTED_KEY.to_string(),
                field: field.to_string(),
                value: format!("value-of-{field}").into_bytes(),
            },
        );
    }
}

/// A FIELD THE PAGE INDEX NO LONGER NAMES IS STILL SERVED, AND NOTHING IS COUNTED.
///
/// RESTATED DOWN TO ITS ONE REMAINING DIRECTION, AND THE OTHER TWO WERE NOT DROPPED -- THEY ARE
/// HELD ELSEWHERE. This was `the_hash_read_serves_both_sources_and_counts_what_only_the_index_names`
/// and it ran three arms over one store: a negative control, a plant that takes a field off the
/// CONTAINER and leaves it on the index, and a plant in the opposite direction. The first two are
/// both about a field the INDEX names that the container lacks, and an entry has no field to name.
/// So the divergence is unreachable BY CONSTRUCTION rather than absent by circumstance: the
/// control's "the counter did not move" could only ever pass, and the plant's "the counter moved"
/// could only ever fail -- which is how it failed, at "the divergence counter did not move (0 ->
/// 0); a counter that is never seen to move is not evidence". Its own in-function comment had
/// already diagnosed that and held the arm on "the ungated route". The one-entry-a-page flag is
/// retired, so there is no ungated route left to hold it on.
///
/// THOSE TWO ARE HELD, WITH THE REASONS THAT MAKE THEM MEAN SOMETHING, BY
/// `under_the_gate_a_hash_entry_names_no_field_so_the_divergence_is_unreachable` below: it asserts
/// the zero TOGETHER WITH a live-entry floor, so the zero cannot be read as an empty store; that
/// the field taken off the container is GONE from the served answer; and that no field named `""`
/// is served, which is the phantom the collapse had to fix on the read path. Repeating them here
/// would be two arms making one structural claim, one of them framed around a retired flag.
///
/// WHAT IS LEFT IS THE DIRECTION THAT STILL HAS TWO OUTCOMES, and it matters more now than it did.
/// With the container the sole source of a hash's field names, an index entry marked deleted must
/// lose NOTHING -- and this arm marks EVERY live entry of the object deleted, so the index names
/// nothing live for it at all and the container is answering alone. It is not counted either,
/// because this direction loses no read and counting it would make the figure unreadable as a risk
/// number.
///
/// THE UNION IS ONE-SIDED NOW, AND THAT IS THE FINDING THIS ARM CARRIES. The module header still
/// describes `HashGetAll` as serving the union of two sources. Its index half answers from
/// `shard.hashes` -- it was one of the four consumers that had to move before the element name
/// could come off the entry -- so the index contributes no NAMES, only the divergence observation
/// that can no longer observe anything. What protects the single remaining source is
/// `fold_hash_map_completeness`, which compares the durable map against the pages' own payloads.
///
/// rust-internal: drives the engine's own served path and its own report surface
#[test]
fn an_index_entry_marked_deleted_loses_no_field_and_is_not_counted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let fields = ["alpha", "beta", "gamma"];
    seed(&engine, &fields);
    assert_eq!(
        expected(&fields),
        served(&engine, PLANTED_KEY),
        "the fixture does not serve its own three fields, so nothing below means anything"
    );

    // THE DENOMINATOR, BEFORE THE PLANT. A plant that marked nothing would leave the answer
    // trivially unchanged, and this arm would then be measuring an untouched store.
    let live_before = live_hash_entries(&engine, PLANTED_KEY);
    assert_eq!(
        fields.len(),
        live_before,
        "the object holds {live_before} live index entries for {} fields written; every element \
         gets its own page on the write path, so this arm is not in the state it says",
        fields.len()
    );

    // THE PLANT: every live entry of the object marked deleted, so the index names nothing live for
    // it. The blocks are untouched, so the container's own addresses still resolve.
    //
    // IT CANNOT SELECT BY FIELD NAME, AND IT NO LONGER NEEDS TO. It read
    // `entry.component.as_deref() == Some(INDEX_PLANTED_FIELD)` and then, after the field left the
    // entry, selected "the object's one live entry" on the belief that one entry a page meant one
    // entry an OBJECT. It does not: each element is written to its own page, so this object holds
    // three. Marking all of them is the stronger plant anyway -- it leaves the container answering
    // with no help from the index at all -- and the count below pins it to exactly the three above
    // rather than to whatever the walk happened to find.
    let marked = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        // THE HANDLES FIRST, THEN THE MARK. `BlockIndexMap` has no `values_mut` -- it is an enum
        // over an inline entry and a sorted vector -- so the matching handles are collected under
        // the read side of the walk and marked through `get_mut` one at a time.
        let targets: Vec<(u32, u64)> = shard
            .bucket_index
            .bucket_map
            .iter()
            .flat_map(|(routing_bucket, bucket)| {
                bucket
                    .block_index
                    .iter()
                    .filter(|(_, page)| {
                        !page.deleted
                            && page.model_id.as_str() == "hash"
                            && &*page.object_key == PLANTED_KEY
                    })
                    .map(|(handle, _)| (*routing_bucket, *handle))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut marked = 0usize;
        for (routing_bucket, handle) in targets {
            let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
                continue;
            };
            let Some(page) = bucket.block_index.get_mut(&handle) else {
                continue;
            };
            if !page.deleted {
                page.deleted = true;
                marked += 1;
            }
        }
        marked
    };
    assert_eq!(
        live_before, marked,
        "the plant marked {marked} of {live_before} live entries; a partial plant leaves the index \
         naming some of the object and this arm would not be about the container answering alone"
    );
    assert_eq!(
        0,
        live_hash_entries(&engine, PLANTED_KEY),
        "the object still holds live index entries after the plant"
    );

    let before = divergences(&engine);
    let served_after = served(&engine, PLANTED_KEY);
    let after = divergences(&engine);

    // THE CONTAINER ANSWERS ALONE, AND IT ANSWERS IN FULL.
    assert_eq!(
        expected(&fields),
        served_after,
        "with no live index entry for the object the read served {served_after:?}. The container \
         holds all three fields and their addresses, so it must answer all three -- a short answer \
         here means the read still depends on the index naming something"
    );

    // AND NOTHING WAS COUNTED, because the container-side direction loses no read.
    assert_eq!(
        before, after,
        "the container-only direction counted {} divergences; the counter is for a field the INDEX \
         names that the container lacks, and mixing the two makes it unreadable as a risk figure",
        after - before
    );

    // THE DENOMINATOR IS PUBLISHED AND NON-ZERO, because a count without one is unreadable.
    let report = engine.hash_read_divergence_report();
    assert!(
        report.reads_served >= 2,
        "the report says {} reads were served over {} divergences; a zero or missing denominator \
         makes the count say nothing either way",
        report.reads_served,
        report.index_named_fields_the_container_lacked
    );
}

/// The live hash page entries this object holds.
fn live_hash_entries(engine: &TemporalEngine, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut live = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if !page.deleted && page.model_id.as_str() == "hash" && &*page.object_key == key {
                live += 1;
            }
        }
    }
    live
}

/// Holds the one-entry-a-page gate OFF and puts back whatever was there -- on a normal drop AND
/// while unwinding, so a failing arm cannot leak it into every later test in the process.
///
/// `remove_var` IS THE CORRECT RESTORE when the variable was absent: the gate reads through
/// `env_flag_default_on`, so UNSET MEANS ON and removing it restores the shipped default rather
/// than turning the gate off for the rest of the binary.



/// UNDER THE GATE THERE IS NOTHING FOR THIS COUNTER TO COUNT, AND THAT IS ASSERTED.
///
/// The arm above holds the gate OFF because that is the only route on which a hash page entry still
/// names a field. This states the other half: with the gate at its shipped default every hash entry
/// is page-named, so the same plant -- a field taken off the container and left on the index --
/// produces NO named-only field, the read serves the container's remaining fields and nothing else,
/// and the counter cannot move.
///
/// WRITTEN AS A TRIPWIRE RATHER THAN A ZERO. A counter that does not move is also what a broken
/// instrument looks like, so this does not merely assert zero: it asserts that the index names no
/// field for this object AT ALL, which is the reason the zero is correct, and it asserts that the
/// planted field is GONE from the served answer -- because serving it from a nameless entry is the
/// phantom `""` field the collapse had to fix on the read path.
///
/// rust-internal: drives the engine's own served path and its own report surface
#[test]
fn under_the_gate_a_hash_entry_names_no_field_so_the_divergence_is_unreachable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let fields = ["alpha", "beta", "gamma"];
    seed(&engine, &fields);
    assert_eq!(
        expected(&fields),
        served(&engine, PLANTED_KEY),
        "the fixture does not serve its own three fields, so nothing below means anything"
    );

    // FLOOR: the object really does hold live hash entries. The arm this guarded -- a `named`
    // counter asserted to zero -- is gone, because an entry has no field name to count and the
    // zero could only ever pass. What is left is the denominator it was a denominator FOR, kept
    // because the serving assertions below are about an index that holds something.
    let live = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let mut live = 0usize;
        for bucket in shard.bucket_index.bucket_map.values() {
            for page in bucket.block_index.values() {
                if page.deleted
                    || page.model_id.as_str() != "hash"
                    || &*page.object_key != PLANTED_KEY
                {
                    continue;
                }
                live += 1;
            }
        }
        live
    };
    assert!(
        live > 0,
        "the index holds no live hash entry for this object, so a zero below says nothing about \
         naming"
    );
    // THE `named == 0` ARM IS GONE: an entry has no field name to count, so it could only pass.
    // The `live > 0` floor above is what the arms below actually need -- an index that holds
    // something for this object -- and it is kept.

    const PLANTED_FIELD: &str = "beta";
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        let fields_of = shard
            .hashes
            .elements_mut_for_test(PLANTED_KEY)
            .expect("the container holds the seeded object");
        assert!(
            fields_of.remove(PLANTED_FIELD).is_some(),
            "the container did not hold `{PLANTED_FIELD}`, so this test plants nothing"
        );
    }

    let before = divergences(&engine);
    let served_after = served(&engine, PLANTED_KEY);
    let after = divergences(&engine);

    assert_eq!(
        before, after,
        "the counter moved by {} under the gate, where no entry names a field for it to diverge \
         from",
        after - before
    );
    assert!(
        !served_after.iter().any(|(name, _)| name == PLANTED_FIELD),
        "the planted field is still served after being taken off the container: {served_after:?}. \
         Under the gate the only entry that could serve it names no field, so serving it means a \
         nameless entry was defaulted to a name -- the phantom-field defect on the read path"
    );
    assert!(
        !served_after.iter().any(|(name, _)| name.is_empty()),
        "a field named `\"\"` is being served: {served_after:?}. That is a nameless page entry \
         defaulted through `unwrap_or_default()`, whose value is the raw page frame"
    );
}
