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

/// THE WHOLE CLAIM, IN THE ORDER THAT MAKES IT EVIDENCE.
///
/// One test and not three, because the three arms share a store and the second and third arms are
/// only meaningful against the first: a counter that moves on a planted divergence proves nothing
/// unless it stayed still when there was none. Split across `#[test]`s they would be three
/// separate readings of a PROCESS-WIDE counter, and the deltas would be at the mercy of whichever
/// order the harness chose.
///
/// WHY THE COUNTER IS READ AS A DELTA AND ASSERTED AS A BOUND. It is process-wide and monotonic,
/// so another test in the same binary that served a hash read contributes to it. A delta removes
/// the history; a `>=` rather than `==` on the planted arm removes the last of the coupling, and
/// what pins the delta to THIS test is `last_divergence` naming this module's own key and field.
///
/// rust-internal: drives the engine's own served path and its own report surface
#[test]
fn the_hash_read_serves_both_sources_and_counts_what_only_the_index_names() {
    // HELD AT GATE OFF, AND THAT IS THE SUBJECT RATHER THAN A WORKAROUND.
    //
    // This counter is about a field the PAGE INDEX names that the container does not. Under one
    // entry a page a hash entry names NO field at all, so there is no named-only field for a plant
    // to create and the divergence is unreachable by construction -- the arm below asserts exactly
    // that, so the structural zero is stated rather than mistaken for a clean store. The route
    // where the counter can still move is the ungated one, which is what this arm holds.
    let _gate = GateHeldOff::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let fields = ["alpha", "beta", "gamma"];
    seed(&engine, &fields);

    // -------------------------------------------------------------------------------------------
    // ARM 1 -- THE NEGATIVE CONTROL. The two sources agree, so the counter must not move. Without
    // this arm, arm 2 cannot tell a counter that detects a divergence from one that counts reads.
    // -------------------------------------------------------------------------------------------
    let before_control = divergences(&engine);
    assert_eq!(
        expected(&fields),
        served(&engine, PLANTED_KEY),
        "the fixture does not serve its own three fields, so nothing below means anything"
    );
    let after_control = divergences(&engine);
    assert_eq!(
        before_control, after_control,
        "a read of an object whose two sources AGREE moved the divergence counter by {}; the \
         counter is counting something other than a disagreement",
        after_control - before_control
    );

    // -------------------------------------------------------------------------------------------
    // ARM 2 -- THE PLANT THE COUNTER EXISTS FOR. One field is taken off the CONTAINER and left on
    // the page index, which is exactly the state that would lose a read once the index stops being
    // consulted.
    // -------------------------------------------------------------------------------------------
    const PLANTED_FIELD: &str = "beta";
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        let fields_of = shard
            .hashes
            .elements_mut_for_test(PLANTED_KEY)
            .expect("the container holds the seeded object");
        let removed = fields_of.remove(PLANTED_FIELD);
        assert!(
            removed.is_some(),
            "the container did not hold `{PLANTED_FIELD}` to begin with, so this test plants \
             nothing and its green is vacuous"
        );
        assert_eq!(
            2,
            fields_of.len(),
            "the container should hold the other two fields after the plant"
        );
    }

    let before_plant = divergences(&engine);
    let served_after_plant = served(&engine, PLANTED_KEY);
    let after_plant = divergences(&engine);

    // (a) THE COUNTER MOVED.
    assert!(
        after_plant > before_plant,
        "the page index names `{PLANTED_FIELD}` and the container no longer holds it, and the \
         divergence counter did not move ({before_plant} -> {after_plant}); a counter that is \
         never seen to move is not evidence"
    );

    // (b) IT MOVED FOR THIS FIELD, which is what ties the delta to this test rather than to
    //     whatever else the binary served.
    let sample = engine
        .hash_read_divergence_report()
        .last_divergence
        .expect("a divergence was counted, so a sample must have been recorded");
    assert_eq!(
        (PLANTED_KEY.to_string(), PLANTED_FIELD.to_string()),
        (sample.object_key.clone(), sample.field.clone()),
        "the counter moved but the sample names {sample:?}, not the planted field"
    );

    // (c) AND THE UNION STILL SERVED THE RIGHT ANSWER -- all three fields with their own values,
    //     the planted one answered from the index. This is what makes observing the divergence
    //     safe: it is observed rather than suffered.
    assert_eq!(
        expected(&fields),
        served_after_plant,
        "a field missing from the container alone changed what the read serves; the union is \
         supposed to make a divergence observable WITHOUT losing a read"
    );

    // (d) THE DENOMINATOR IS PUBLISHED AND NON-ZERO, because a count without it is unreadable.
    let report = engine.hash_read_divergence_report();
    assert!(
        report.reads_served >= 2,
        "the report says {} reads were served over {} divergences; a zero or missing denominator \
         makes the count say nothing either way",
        report.reads_served,
        report.index_named_fields_the_container_lacked
    );

    // -------------------------------------------------------------------------------------------
    // ARM 3 -- THE OPPOSITE DIRECTION, which the union rescues and which is deliberately NOT
    // counted. A field is taken off the PAGE INDEX and left in the container. Marking the index
    // entry deleted is how the index stops naming it; the block itself is untouched, so the
    // container's address still resolves.
    // -------------------------------------------------------------------------------------------
    const INDEX_PLANTED_FIELD: &str = "gamma";
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        let located: Vec<(u32, u64)> = shard
            .bucket_index
            .object_block_refs("hash", PLANTED_KEY)
            .map(|refs| {
                refs.all_refs()
                    .map(|block_ref| (block_ref.routing_bucket, block_ref.block_ref_key))
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            !located.is_empty(),
            "the object lookup names no blocks for the seeded object, so this arm cannot plant"
        );
        let mut marked = 0usize;
        for (routing_bucket, block_ref_key) in located {
            let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
                continue;
            };
            let Some(entry) = bucket.block_index.get_mut(&block_ref_key) else {
                continue;
            };
            if entry.component.as_deref() == Some(INDEX_PLANTED_FIELD) && !entry.deleted {
                entry.deleted = true;
                marked += 1;
            }
        }
        assert_eq!(
            1, marked,
            "the plant marked {marked} index entries for `{INDEX_PLANTED_FIELD}`, not one; the \
             arm below would be measuring a different store than it says"
        );
    }

    let before_rescue = divergences(&engine);
    let served_after_rescue = served(&engine, PLANTED_KEY);
    let after_rescue = divergences(&engine);

    // The container still holds `gamma`, so the union must still serve it. `beta` is still missing
    // from the container and still named by the index, so it is still served from there: the
    // answer is unchanged in BOTH directions at once, which is the union doing its whole job.
    assert_eq!(
        expected(&fields),
        served_after_rescue,
        "a field the page index no longer names was not served from the container; the union is \
         not serving both sources"
    );

    // AND IT COUNTED ONLY THE OTHER DIRECTION. `beta` is still divergent index-side and still
    // counted; `gamma` is divergent container-side and must not be, because the union already
    // answers it and mixing the two would make the count unreadable as a risk figure.
    assert_eq!(
        1,
        after_rescue - before_rescue,
        "one read of an object with one index-only field and one container-only field counted {} \
         divergences, not 1; the container-only direction is being counted too",
        after_rescue - before_rescue
    );
}

/// Holds the one-entry-a-page gate OFF and puts back whatever was there -- on a normal drop AND
/// while unwinding, so a failing arm cannot leak it into every later test in the process.
///
/// `remove_var` IS THE CORRECT RESTORE when the variable was absent: the gate reads through
/// `env_flag_default_on`, so UNSET MEANS ON and removing it restores the shipped default rather
/// than turning the gate off for the rest of the binary.
struct GateHeldOff {
    restore: Option<String>,
}

impl GateHeldOff {
    fn new() -> Self {
        let restore = std::env::var(crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE).ok();
        std::env::set_var(crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE, "0");
        Self { restore }
    }
}

impl Drop for GateHeldOff {
    fn drop(&mut self) {
        match self.restore.take() {
            Some(previous) => {
                std::env::set_var(crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE, previous)
            }
            None => std::env::remove_var(crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE),
        }
    }
}

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

    // FLOOR: the index really does name no field for this object, which is WHY the counter cannot
    // move. Without this the zero below is satisfied by an index with no entries at all.
    let (live, named) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let mut live = 0usize;
        let mut named = 0usize;
        for bucket in shard.bucket_index.bucket_map.values() {
            for page in bucket.block_index.values() {
                if page.deleted
                    || page.model_id.as_str() != "hash"
                    || &*page.object_key != PLANTED_KEY
                {
                    continue;
                }
                live += 1;
                if page.component.is_some() {
                    named += 1;
                }
            }
        }
        (live, named)
    };
    assert!(
        live > 0,
        "the index holds no live hash entry for this object, so a zero below says nothing about \
         naming"
    );
    assert_eq!(
        0, named,
        "{named} of {live} live hash entries name a field under the gate; the collapse has not \
         reached this kind and the arm above is the one to read"
    );

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
