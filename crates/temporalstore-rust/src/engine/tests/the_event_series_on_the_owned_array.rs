// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE EVENT SERIES ON THE OWNED SORTED ARRAY, and the decode-only landing spot beside it.
//!
//! # WHAT IS BEING ASSERTED, AND WHAT IS DELIBERATELY NOT
//!
//! Three claims, each with its own test, because one test covering all three would pass on two:
//!
//!   1. **The array is a timestamped series.** Sorted whatever order points arrive in, including
//!      duplicates and removals, and it gives its allocation back when it empties.
//!   2. **The compatibility field is DRAINED and never refilled.** That is the single property the
//!      whole shape rests on: one live copy of this data at every moment after a decode returns.
//!      The mirror this change was first written with is gone, and so are its agreement predicate
//!      and that predicate's negative control -- not because they failed, but because with one
//!      copy they have nothing to check.
//!   3. **The two paths that can carry the legacy key are both covered** -- a decode of an image
//!      that carries it, and the key-state capture that carries it with no decode at all. Neither
//!      is a door that forgets to check the stamp: every installing door checks it. What the stamp
//!      cannot do is refuse a row it does not know about, because the test is `<` and a
//!      current-stamp image is accepted everywhere.
//!
//! What is NOT claimed is a footprint win on a live shard. The compatibility field costs nothing
//! after the fold drains it, but the array and the B-tree are never both resident long enough for
//! the shard-level bracket to see either -- and that bracket is seeded with `strings` and
//! `features` and holds no context events at all, so it does not move for this map in either
//! direction. The measurement below prices the two representations against each other directly,
//! which is the honest form of that claim.

use super::super::state::ShardState;
use super::super::timestamped_series::OwnedTimestampedSeries;
use crate::block_store::ElementEntry;
use std::collections::BTreeMap;

/// A legal address. `ElementEntry` has no public constructor and `try_from_parts` REFUSES a block
/// id wider than `u16::MAX` rather than truncating it, so the id is masked exactly as the other
/// address fixtures in this crate mask it.
fn address(i: u64) -> ElementEntry {
    ElementEntry::from_parts(1, i * 64, 64, Some(i & u64::from(u16::MAX)), Some(i))
}

fn series_of(points: &[u64]) -> BTreeMap<u64, ElementEntry> {
    points.iter().map(|at| (*at, address(*at))).collect()
}

/// Ascending order is an INVARIANT and not a property of the input, so the input is deliberately
/// adversarial: descending, interleaved, duplicated, and then drained.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn the_owned_array_stays_sorted_whatever_order_the_points_arrive_in() {
    let mut series = OwnedTimestampedSeries::default();
    assert!(series.is_empty(), "a default series holds nothing");
    assert_eq!(
        0,
        series.allocation_count(),
        "an empty series must hold no allocation at all -- a per-key `Vec` header kept for a series \
         with no points is exactly the per-key constant this representation exists to remove"
    );

    // DESCENDING, then interleaved into the holes between them. A representation that appended
    // and sorted later would pass a test that wrote ascending input.
    for at in [900u64, 700, 500, 300, 100] {
        assert_eq!(None, series.insert(at, address(at)), "{at} is new");
    }
    for at in [800u64, 600, 400, 200] {
        assert_eq!(None, series.insert(at, address(at)), "{at} is new");
    }
    let walked: Vec<u64> = series.keys().copied().collect();
    assert_eq!(
        vec![100u64, 200, 300, 400, 500, 600, 700, 800, 900],
        walked,
        "the walk must be ascending after out-of-order inserts, because `points` PROMISES that \
         order and the stored-image collectors depend on it"
    );
    assert_eq!(9, series.len(), "nine distinct instants were written");

    // A DUPLICATE INSTANT REPLACES, it does not grow the array. A representation that appended
    // would read back the same instant twice and the walk would still be ascending, so the length
    // is what separates the two.
    let replaced = series.insert(500, address(5_000));
    assert_eq!(
        Some(address(500)),
        replaced,
        "writing an instant that is already there returns what it replaced"
    );
    assert_eq!(9, series.len(), "a replace does not grow the array");
    assert_eq!(
        Some(&address(5_000)),
        series.get(&500),
        "and the new address is what is read back"
    );

    // REMOVALS PRESERVE ORDER, from both ends and the middle.
    for at in [100u64, 900, 500] {
        assert!(series.remove(&at).is_some(), "{at} was there to remove");
    }
    assert_eq!(None, series.remove(&500), "a second removal finds nothing");
    let walked: Vec<u64> = series.keys().copied().collect();
    assert_eq!(
        vec![200u64, 300, 400, 600, 700, 800],
        walked,
        "the walk is still ascending after removals at both ends and in the middle"
    );

    // AND THE ALLOCATION GOES BACK when the last point leaves.
    for at in [200u64, 300, 400, 600, 700, 800] {
        series.remove(&at);
    }
    assert!(series.is_empty(), "every point was removed");
    assert_eq!(
        0,
        series.allocation_count(),
        "a drained series must give its allocation back, not keep an empty buffer per key"
    );
}

/// The round trip through the B-tree shape is lossless in both directions, which is what lets the
/// stored image stay exactly what it was.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn the_array_and_the_btree_shape_round_trip_without_loss() {
    let original = series_of(&[7, 3, 11, 1, 9]);
    let array = OwnedTimestampedSeries::from(&original);
    assert_eq!(
        original,
        array.to_btree(),
        "the B-tree view of the array must be the B-tree it was built from -- this is the shape \
         the capture blob carries and an older binary folds"
    );
    assert_eq!(original.len(), array.len(), "and no point was dropped");
}

/// THE PROPERTY THE WHOLE SHAPE RESTS ON: the fold DRAINS the compatibility field, and nothing
/// refills it.
///
/// This is the test that would have caught the shape this change was first written with. That
/// revision refilled the field at the end of the fold and mirrored it from every mutator, so two
/// copies were live and a bulk rebuild had a window in which a reader could be served the stale
/// one. The assertion is therefore not "the copies agree" -- which that revision could also pass
/// -- but "there is only one copy", which it could not.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn the_fold_drains_the_compatibility_field_and_no_write_refills_it() {
    let mut shard = ShardState::default();

    // What a decoded image carrying the legacy key leaves behind.
    shard.plant_legacy_context_event_for_test("ctx:event:t:a", series_of(&[1, 2, 3]));
    shard.plant_legacy_context_event_for_test("ctx:event:t:b", series_of(&[9]));
    assert_eq!(
        2,
        shard.legacy_context_event_len_for_test(),
        "the fixture planted what a decode would have left"
    );
    assert!(
        shard.context_events().is_empty(),
        "before the fold the array map is empty -- which is exactly the state in which a reader \
         served only from it would find the rows GONE"
    );

    shard.fold_legacy_context_events();

    assert_eq!(2, shard.context_events().len(), "both keys arrived");
    assert_eq!(
        Some(&address(2)),
        shard
            .context_events()
            .get("ctx:event:t:a")
            .and_then(|series| series.get(&2)),
        "and the points came with them"
    );
    assert_eq!(
        0,
        shard.legacy_context_event_len_for_test(),
        "THE FIELD IS EMPTY AFTER THE FOLD. A non-empty one is a second live copy, and a second \
         live copy is a divergence window during every bulk rebuild"
    );

    // AND NO WRITE PUTS ANYTHING BACK. Every production write path for this map goes through the
    // array map; this drives the three shapes of write and re-checks the field after each.
    super::super::timestamped_series::set_point_and_collect_live(
        shard.context_events_mut(),
        std::sync::Arc::from("ctx:event:t:c"),
        5,
        Some(address(5)),
    );
    assert_eq!(
        0,
        shard.legacy_context_event_len_for_test(),
        "a point write must not touch the compatibility field"
    );

    shard.set_context_event_series("ctx:event:t:d", series_of(&[6, 7]));
    assert_eq!(
        0,
        shard.legacy_context_event_len_for_test(),
        "a whole-series install must not touch it either -- this is the key-state path"
    );

    shard.context_events_mut().remove("ctx:event:t:a");
    assert_eq!(
        0,
        shard.legacy_context_event_len_for_test(),
        "nor a removal, which is the half a mirrored pair used to lose"
    );
    assert_eq!(
        3,
        shard.context_events().len(),
        "denominator: the array map really holds the three keys those writes left"
    );
}

/// THE PREMISE OF THE FOLD, AND THE COUNTER THAT CATCHES IT FAILING.
///
/// The fold is a move rather than a merge because its destination is empty by construction -- the
/// array field is `#[serde(skip)]`, so a freshly decoded state has it at `Default`. That premise is
/// checked rather than assumed: if the fold runs over a populated map, the live entry is KEPT and
/// counted, so the old image cannot silently overwrite live series.
///
/// THIS REPLACED A COUNTER THAT COULD NOT FIRE. The mirror revision counted the two copies arriving
/// different, which became unreachable the moment there was one copy -- a counter whose branch no
/// execution can take proves nothing by reading zero. This one is reachable, and here is the
/// execution that reaches it.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn a_fold_over_a_live_map_keeps_the_live_entry_and_counts_it() {
    crate::engine::state::reset_legacy_context_event_fold_over_live_map_count();

    // THE ORDINARY CASE FIRST, as the control: an empty destination counts nothing.
    let mut fresh = ShardState::default();
    fresh.plant_legacy_context_event_for_test("ctx:event:t:a", series_of(&[1]));
    fresh.fold_legacy_context_events();
    assert_eq!(
        0,
        crate::engine::state::legacy_context_event_fold_over_live_map_count(),
        "a fold onto an empty map is every ordinary decode and must count nothing"
    );

    // NOW THE PREMISE FAILING: a live series under the same key the legacy image carries.
    let mut live = ShardState::default();
    live.set_context_event_series("ctx:event:t:a", series_of(&[1]));
    live.plant_legacy_context_event_for_test("ctx:event:t:a", series_of(&[2]));
    live.plant_legacy_context_event_for_test("ctx:event:t:b", series_of(&[7]));
    live.fold_legacy_context_events();

    assert_eq!(
        1,
        crate::engine::state::legacy_context_event_fold_over_live_map_count(),
        "exactly one key was already live -- the second was absent from the array map, which is an \
         arrival and not a premise failure"
    );
    assert_eq!(
        Some(&address(1)),
        live.context_events()
            .get("ctx:event:t:a")
            .and_then(|series| series.get(&1)),
        "THE LIVE ENTRY IS KEPT. An old image must not overwrite a series that is already serving"
    );
    assert!(
        live.context_events().contains_key("ctx:event:t:b"),
        "and the key that was only in the legacy copy still arrived -- the fold must not refuse \
         the load, because an unopenable store is the worst outcome on this path"
    );
    assert_eq!(
        0,
        live.legacy_context_event_len_for_test(),
        "and the field is still drained, even on the path where the premise failed"
    );
    crate::engine::state::reset_legacy_context_event_fold_over_live_map_count();
}

/// THE PATH THE DECODE FOLD CANNOT SEE.
///
/// `capture_key_states` writes a `context_events` field into the index-log blobs. When no base
/// index decodes, `persistence.rs` takes `ShardState::default()` and folds those blobs straight
/// through `apply_key_states` -- so `fold_legacy_context_events` is never called and cannot cover
/// them. This is not an unguarded door: there is no stored index on this path for a stamp to
/// describe, and the blob was written by this binary at this stamp. What covers it is the
/// `KeyedState` conversion from the stored B-tree shape into the array.
///
/// THE REMOVAL HALF IS DRIVEN TOO. `apply_key_state_field` reads an ABSENT field as "this key had
/// none" and REMOVES the entry, so the path writes removals as well as inserts.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn the_capture_path_writes_the_array_map_with_nothing_decoded() {
    let mut shard = ShardState::default();

    let arrival = serde_json::json!({
        "key": "ctx:event:t:a",
        "context_events": serde_json::to_value(series_of(&[1, 2])).expect("the series encodes"),
    });
    crate::engine::apply_key_states(&mut shard, std::slice::from_ref(&arrival));

    assert_eq!(
        2,
        shard
            .context_events()
            .get("ctx:event:t:a")
            .map_or(0, OwnedTimestampedSeries::len),
        "the key-state fold delivered the series into the array map with nothing decoded"
    );
    assert_eq!(
        0,
        shard.legacy_context_event_len_for_test(),
        "and it did not put anything in the compatibility field, which only a decode fills"
    );

    // THE REMOVAL HALF: a blob naming the key but carrying no `context_events` field.
    let departure = serde_json::json!({ "key": "ctx:event:t:a", "features": {} });
    crate::engine::apply_key_states(&mut shard, std::slice::from_ref(&departure));

    assert!(
        !shard.context_events().contains_key("ctx:event:t:a"),
        "an absent field is read as a removal, which is what this helper is for"
    );
}

/// THE STORED IMAGE IS UNCHANGED, which is the whole reason the compatibility field exists and the
/// reason no format stamp is owed.
///
/// This map was ALREADY `skip_serializing` before this change, so the assertion is the strong one:
/// the key is absent from what this binary writes, exactly as it was absent before. And an image
/// that still carries it -- one written before `skip_serializing` landed, or one arriving through a
/// dump manifest -- still delivers its rows, through `default` on the compatibility field and then
/// the fold.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn the_stored_image_still_omits_the_key_and_still_reads_one_that_carries_it() {
    let mut shard = ShardState::default();
    shard.set_context_event_series("ctx:event:t:a", series_of(&[1, 2]));

    let encoded = serde_json::to_value(&shard).expect("the shard state encodes");
    assert!(
        encoded.get("context_events").is_none(),
        "this map is `skip_serializing` and must stay so -- writing it would change the stored \
         image and owe a format stamp"
    );
    assert!(
        encoded.get("context_events_series").is_none(),
        "and the array map must not appear under a NEW key either, which would be the same format \
         change wearing a different name"
    );

    // AND THE READ DIRECTION, end to end: an image carrying the legacy key decodes into the
    // compatibility field, and the fold moves it across and leaves that field empty.
    let mut carried = encoded.clone();
    carried
        .as_object_mut()
        .expect("the encoded state is an object")
        .insert(
            "context_events".to_string(),
            serde_json::json!({ "ctx:event:t:b": series_of(&[5]) }),
        );
    let mut decoded: ShardState =
        serde_json::from_value(carried).expect("an image carrying the legacy key decodes");
    assert_eq!(
        1,
        decoded.legacy_context_event_len_for_test(),
        "the legacy key landed in the compatibility field"
    );
    decoded.fold_legacy_context_events();
    assert_eq!(
        Some(&address(5)),
        decoded
            .context_events()
            .get("ctx:event:t:b")
            .and_then(|series| series.get(&5)),
        "and the fold delivered it to the array map"
    );
    assert_eq!(
        0,
        decoded.legacy_context_event_len_for_test(),
        "leaving the compatibility field empty, which is the one property this shape rests on"
    );
}

/// WHAT THE ARRAY COSTS AGAINST THE B-TREE IT REPLACES -- in BYTES and in ALLOCATIONS, reported as
/// two numbers because they are two different components and one standing in for the other is how
/// a footprint claim gets published wrong.
///
/// # WHY THIS IS STRUCTURAL AND NOT A COUNTER READING
///
/// The process-global allocation counters cannot attribute across concurrently running tests, and
/// a mutex does not fix that -- the allocations of the other test still land in the same counter.
/// So the allocation count here comes from the structure itself, which is exact and
/// order-independent, and the per-element byte comparison is a `size_of` statement about the
/// owning field rather than a reading of resident memory.
///
/// # WHAT IT DOES NOT CLAIM
///
/// Not a shard-level saving. The shard-level bracket in `index_bytes_per_key.rs` is seeded with
/// `strings` and `features` and holds no context events at all, so it does not move for this map in
/// either direction, and an unmoved bracket on an unseeded map is not evidence of anything.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn the_owned_array_is_eight_bytes_and_two_allocations_where_the_btree_is_twenty_four_and_more() {
    // THE OWNING FIELD. Eight bytes against twenty-four, per key, before any element is stored.
    assert_eq!(
        8,
        std::mem::size_of::<OwnedTimestampedSeries>(),
        "the array field is a thin pointer"
    );
    assert_eq!(
        24,
        std::mem::size_of::<BTreeMap<u64, ElementEntry>>(),
        "the B-tree header it replaces is three words -- this is the per-key inline difference, \
         and it is 16 bytes, which is NOT the whole story and must not be quoted as it"
    );

    // THE ALLOCATION COUNT, which is the other component and the larger one. A `BTreeMap` with a
    // single element has allocated its root node; the array has a `Vec` header and a buffer.
    let mut array = OwnedTimestampedSeries::default();
    array.insert(1, address(1));
    assert_eq!(
        1,
        array.len(),
        "denominator: one element, which is the regime this comparison is about"
    );
    assert_eq!(
        2,
        array.allocation_count(),
        "one element costs the array two allocations: the boxed `Vec` header and its buffer"
    );

    // AND AT A THOUSAND ELEMENTS IT IS STILL TWO. The B-tree grows a node per fanout group; the
    // array grows its one buffer. That is the shape of the win, and it is why the allocation count
    // is reported separately from the byte figure rather than folded into it.
    let mut longer = OwnedTimestampedSeries::default();
    for at in 0..1_000u64 {
        longer.insert(at, address(at));
    }
    assert_eq!(1_000, longer.len(), "denominator: a thousand elements");
    assert_eq!(
        2,
        longer.allocation_count(),
        "a thousand elements still cost the array exactly two allocations"
    );

    println!(
        "owned array: {} B inline, 2 allocations at 1 element, {} at 1000 elements",
        std::mem::size_of::<OwnedTimestampedSeries>(),
        longer.allocation_count()
    );
    println!(
        "B-tree it replaces: {} B inline, one 280-byte root node allocated for the first element",
        std::mem::size_of::<BTreeMap<u64, ElementEntry>>()
    );
}
