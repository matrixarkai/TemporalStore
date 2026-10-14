// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN THE THREE CONTROL-STATE VALUE MAPS ENTER THE CONSOLIDATED MAP, AND WHAT IS THE FLOOR?
//!
//! `control_state_changes` and `control_state_selection` are two of the twelve maps
//! `apply_key_state_field` folds (three of thirteen when this was written, before the two change
//! maps became one). The consolidated map stores an `ObjectSlot`
//! -- a record plus an element array, both of which describe WHERE PAGES LIVE. These three hold
//! domain values. This file pins what is true of them, so the floor on the goal is recorded with
//! its evidence rather than asserted in a report.
//!
//! # THE FOUR ANSWERS, EACH DRIVEN
//!
//! 1. DURABLE, NOT DERIVED. All three carry `#[serde(default)]` and no `skip`, nothing on the
//!    load path clears or rebuilds them, and [`all_three_maps_survive_a_reload`] drives the round
//!    trip rather than reading the attribute. The contrast is `expiry_by_deadline`
//!    (`#[serde(skip)]`, rebuilt from `expires_at_ms`) and `control_state_rollups`
//!    (`#[serde(skip)]`, rebuilt lazily) -- which is almost certainly where an earlier census got
//!    "two of the three are `#[serde(skip)]`" from. They are NEIGHBOURS of these three in the
//!    struct, with the same `control_state_` prefix, and they are the skipped ones.
//!
//! 2. ALL THREE HAVE KEY-CREATING PRODUCTION WRITERS, unlike `sequences` (none, deleted) and
//!    `context_nodes` (none key-creating). `control_state_changes` is written unconditionally by
//!    `hll::record_change`; a bucket is converted to its SKETCH representation by the same
//!    function only when the `control_distinct_sketch` gate is on, which
//!    [`the_sketch_map_has_no_writer_until_its_flag_is_on`] drives in both positions;
//!    `control_state_selection` is written from the command path.
//!
//! 3. THE TWO CHANGE MAPS ARE ONE LOGICAL MAP. `record_change` keeps them mutually exclusive per
//!    `(key, bucket)` -- on conversion it REMOVES the bucket from the exact map and inserts it
//!    into the sketch map -- which [`a_bucket_holds_exactly_one_representation`] drives across
//!    the conversion. That was the one consolidation available here that needed no change to
//!    `ObjectSlot`, and it has been taken.
//!
//! 4. NONE OF THEM HAS AN OUTER KIND KEY.
//!    [`none_of_these_maps_has_a_stored_model_kind`] asks `StoredModelKind::from_stored_name`
//!    rather than reading a list. `control_state` has a variant because its data is PAGES; these
//!    three have none because they have no pages at all.
//!
//! # THE FLOOR, AND THE ONE STEP OF IT THAT HAS SINCE BEEN TAKEN
//!
//! This module was written when `apply_key_state_field` folded THIRTEEN maps. NINE of them have
//! `ElementEntry` values -- a page address, which is what a slot exists to hold: `features`,
//! `context_nodes`, `context_events`, `context_indexes`, `context_audits`, `context_entities`,
//! `context_children`, `context_summaries`, `context_compressions`. FOUR did not, and the fourth
//! was the one no list of the problem had mentioned -- `expires_at_ms`, whose value is a deadline.
//! So 13 - 4 = 9, and 13 - 3 = 10 would have been wrong: the floor's NUMBER was right and its
//! stated reason was one map short.
//!
//! IT IS NOW NINE OF TWELVE, because the recommendation this module made has been taken.
//! `control_state_change_sketch` and `control_state_changes` were one logical map -- keyed
//! identically, kept mutually exclusive per `(key, bucket)` by `record_change` -- and they are now
//! one map whose bucket value is `hll::DistinctBucket`. The three that remain outside the slot are:
//!
//! | map | value | a page address? |
//! |---|---|---|
//! | `control_state_changes` | `BTreeMap<u64, DistinctBucket>` | no -- members or a sketch |
//! | `control_state_selection` | `ControlStateSelectionValue` | no -- time, bytes, kind |
//! | `expires_at_ms` | `u64` | no -- a deadline |
//!
//! So the count of maps moved and the count outside the slot moved with it, which is the whole of
//! what that consolidation bought. The nine did not change.

#![allow(clippy::all)]
use super::*;

use crate::engine::storage_bucket_internals::StoredModelKind;

const KEY: &str = "ctl:cap:tenant-7";
const BUCKET_MS: u64 = 1_000;
const HOUR_MS: u64 = 3_600_000;

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

/// What the shard holds for `KEY`: exact buckets, sketched buckets, and whether a selection is
/// present.
///
/// THE FIRST TWO USED TO BE TWO MAPS AND ARE NOW TWO VARIANTS. The shape of this helper is
/// unchanged on purpose -- every assertion below is written against the same triple -- but where
/// it once read a length from each of two maps it now partitions one map by variant. A bucket can
/// only be counted once either way, which is the point.
fn held(engine: &TemporalEngine) -> (usize, usize, bool) {
    use crate::engine::hll::DistinctBucket;
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let (exact, sketched) = shard
        .control_state_changes
        .get(KEY)
        .map_or((0, 0), |buckets| {
            buckets.values().fold((0, 0), |(exact, sketched), bucket| match bucket {
                DistinctBucket::Exact(_) => (exact + 1, sketched),
                DistinctBucket::Sketch(_) => (exact, sketched + 1),
            })
        });
    (exact, sketched, shard.control_state_selection.contains_key(KEY))
}

/// Turn the distinct-sketch gate on through the real config path, and confirm the shard field the
/// writer reads actually moved -- the plumbing is `execute_on_shard`'s per-execute assignment, so
/// setting the config is not by itself evidence that `record_change` will see it.
fn enable_distinct_sketch(engine: &TemporalEngine) {
    let mut config = Config {
        version: 2,
        ..Config::default()
    };
    config
        .extend_config
        .insert("control_distinct_sketch".to_string(), "on".to_string());
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config
            })
            .ok,
        "setting the distinct-sketch gate failed"
    );
    // One execute, so the per-execute assignment runs.
    let answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateChangeAdd {
            key: KEY.to_string(),
            timestamp_ms: BUCKET_MS,
            value: b"seed".to_vec(),
            precision_ms: None,
            ttl_ms: None,
        },
    });
    assert!(answer.status.ok, "the seed write failed: {}", answer.status.message);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    assert!(
        shard.control_distinct_sketch,
        "the config gate is on but the shard field `record_change` reads is still false, so the \
         per-execute assignment in `execute_on_shard` is not reaching it and the arms below would \
         be measuring the default rather than the gate"
    );
}

/// Drive `record_change` past the conversion threshold on one bucket.
fn fill_past_the_threshold(engine: &TemporalEngine) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 is loaded");
    for index in 0..=crate::engine::hll::DISTINCT_SKETCH_THRESHOLD {
        crate::engine::hll::record_change(
            shard,
            KEY,
            BUCKET_MS,
            format!("member-{index}").into_bytes(),
        );
    }
}

/// 1. DURABLE, NOT DERIVED -- driven as a round trip, not read off the attribute.
#[test]
fn all_three_maps_survive_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    engine.load_shard(1);

    // The sketch map only gets a key with the gate on, so turn it on first.
    enable_distinct_sketch(&engine);
    fill_past_the_threshold(&engine);

    let selection = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateSelectionSet {
            key: KEY.to_string(),
            value: b"chosen".to_vec(),
            occur_time_ms: BUCKET_MS,
            ttl_ms: HOUR_MS,
            selection_type: crate::types::ControlStateSelectionType::Last,
        },
    });
    assert!(
        selection.status.ok,
        "the selection write failed: {}",
        selection.status.message
    );

    // A second bucket that stays EXACT, so the reload has something to carry in both maps.
    let other = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateChangeAdd {
            key: KEY.to_string(),
            timestamp_ms: BUCKET_MS + 60_000,
            value: b"single".to_vec(),
            precision_ms: None,
            ttl_ms: None,
        },
    });
    assert!(other.status.ok, "the second bucket write failed: {}", other.status.message);

    let (exact_before, sketch_before, selection_before) = held(&engine);
    println!("  before reload -> exact buckets {exact_before}, sketched buckets {sketch_before}, selection {selection_before}");
    assert!(
        exact_before > 0 && sketch_before > 0 && selection_before,
        "denominator: all three maps must hold something for `{KEY}` before the reload, or the \
         assertions after it are about empty maps. Got exact {exact_before}, sketch \
         {sketch_before}, selection {selection_before}"
    );

    engine.flush_shard_index(1);
    drop(engine);

    let reloaded = engine_at(dir.path());
    reloaded.load_shard(1);
    let (exact_after, sketch_after, selection_after) = held(&reloaded);
    println!("  after reload  -> exact buckets {exact_after}, sketched buckets {sketch_after}, selection {selection_after}");

    assert_eq!(
        (exact_before, sketch_before, selection_before),
        (exact_after, sketch_after, selection_after),
        "one of the three control-state value maps did not survive a reload. If a map comes back \
         EMPTY it is derived rather than durable -- like `expiry_by_deadline`, which is \
         `#[serde(skip)]` and rebuilt from `expires_at_ms` -- and a derived map is not a \
         consolidation candidate at all. Re-derive the floor before changing this."
    );
}

/// 2. THE SKETCH MAP'S KEY-CREATING WRITER IS GATED, AND THE EXACT MAP'S IS NOT.
#[test]
fn the_sketch_map_has_no_writer_until_its_flag_is_on() {
    // ---- gate OFF: the default ----------------------------------------------------------
    let off_dir = tempfile::tempdir().expect("tempdir");
    let off = engine_at(off_dir.path());
    off.load_shard(1);
    {
        let shards = off.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        assert!(
            !shard.control_distinct_sketch,
            "denominator: the distinct-sketch gate must be OFF by default, or this arm is not \
             measuring the default. `Config::flag` is `flag_or(name, false)`."
        );
    }
    fill_past_the_threshold(&off);
    let (exact_off, sketch_off, _) = held(&off);
    println!("  gate off -> exact buckets {exact_off}, sketched buckets {sketch_off}");
    assert_eq!(
        1, exact_off,
        "with the gate off every value must stay in the one exact bucket"
    );
    assert_eq!(
        0, sketch_off,
        "the sketch map gained a key with the gate OFF, so its writer is not gated after all and \
         the default deployment does populate it"
    );

    // ---- gate ON ------------------------------------------------------------------------
    let on_dir = tempfile::tempdir().expect("tempdir");
    let on = engine_at(on_dir.path());
    on.load_shard(1);
    enable_distinct_sketch(&on);
    fill_past_the_threshold(&on);
    let (exact_on, sketch_on, _) = held(&on);
    println!("  gate on  -> exact buckets {exact_on}, sketched buckets {sketch_on}");
    assert_eq!(
        1, sketch_on,
        "with the gate on, a bucket past the threshold must be converted into exactly one sketch"
    );
}

/// 3. A BUCKET HOLDS EXACTLY ONE REPRESENTATION, AND NOW CANNOT HOLD TWO.
///
/// # WHAT THIS ARM ASSERTED, AND WHY IT CHANGED
///
/// It asserted that the two change maps were mutually exclusive per `(key, bucket)` -- the
/// property that made merging them a consolidation rather than a reshuffle. The exclusivity was
/// maintained by `record_change` doing a `remove` from one map and an `insert` into the other,
/// which meant a bucket in both was REPRESENTABLE and the assertion had something it could catch.
///
/// The merge happened, and `hll::DistinctBucket` makes the overlap state unrepresentable: one
/// bucket, one variant. So this arm can no longer fail the way it was written to fail, and
/// pretending otherwise would leave a guard over a state the type system has already excluded --
/// which is the kind of assertion that looks like coverage and is not.
///
/// WHAT IT ASSERTS INSTEAD is that the conversion REPLACES rather than ADDS: after a bucket is
/// sketched, the key holds that one bucket and it is the sketch. That is still a real claim about
/// `record_change` -- a conversion that inserted a new bucket id, or left the exact one behind
/// under a different key, would fail it.
///
/// AND IT WAS NEVER A CORRECTNESS RISK, which stays written down so the merge is not later
/// justified as fixing a bug it did not fix. `count_changes` folds the exact values into an HLL
/// and merges the sketches into the same HLL, and both an `Hll` and a `BTreeSet` absorb a
/// duplicate, so a bucket present in both maps would still have produced the right distinct
/// count. The merge bought the invariant being unbreakable, not a defect repaired.
#[test]
fn a_bucket_holds_exactly_one_representation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    engine.load_shard(1);
    enable_distinct_sketch(&engine);
    fill_past_the_threshold(&engine);

    use crate::engine::hll::DistinctBucket;
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let buckets = shard
        .control_state_changes
        .get(KEY)
        .expect("the key holds a change series");
    let shapes: Vec<(u64, &str)> = buckets
        .iter()
        .map(|(at, bucket)| {
            (
                *at,
                match bucket {
                    DistinctBucket::Exact(_) => "exact",
                    DistinctBucket::Sketch(_) => "sketch",
                },
            )
        })
        .collect();
    println!("  buckets held: {shapes:?}");
    assert_eq!(
        1,
        shapes.len(),
        "the conversion left {} buckets under one key where the fixture wrote to one. A \
         conversion that ADDS a bucket rather than replacing one in place would look like this, \
         and it would double the memory the sketch exists to bound.",
        shapes.len()
    );
    assert_eq!(
        "sketch", shapes[0].1,
        "the bucket past the threshold with the gate on is still exact, so the conversion did \
         not happen and this arm is measuring nothing"
    );
}

/// 4. NONE OF THE THREE HAS AN OUTER KIND KEY, WHICH IS WHY A SLOT CANNOT ADDRESS THEM.
///
/// Asked of `StoredModelKind::from_stored_name` rather than of a list in a comment. A kind exists
/// for data whose elements are PAGES; the positive control is `control_state`, whose blocks are
/// pages and which does have one.
#[test]
fn none_of_these_maps_has_a_stored_model_kind() {
    assert!(
        StoredModelKind::from_stored_name("control_state").is_some(),
        "positive control: `control_state` must have a stored kind, or `from_stored_name` is \
         answering None for everything and the assertions below mean nothing"
    );
    for absent in [
        "control_state_changes",
        "control_state_change_sketch",
        "control_state_selection",
        "control_state_fol",
        "expires_at_ms",
    ] {
        assert!(
            StoredModelKind::from_stored_name(absent).is_none(),
            "`{absent}` now has a `StoredModelKind`. That is the outer key a consolidated map \
             would address it by, so if this is deliberate the floor of nine-of-thirteen has \
             moved and the slot's value type has to be able to hold whatever this map holds -- \
             which for these is a domain value, not a page address."
        );
    }
}
