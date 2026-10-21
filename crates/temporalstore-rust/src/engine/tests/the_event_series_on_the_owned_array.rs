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
//!   2. **The legacy key has NO DOOR**, which is what retired the compatibility field this module
//!      was built around. The old claim here was that the field is drained and never refilled --
//!      one live copy at every moment after a decode returns. There is no field now, and the
//!      claim that replaced it is stronger: nothing can deliver a top-level `context_events` key.
//!      A pre-stamp image is refused on every one of `decode_index_bytes`'s six callers, and an
//!      image at stamp 20 or later does not carry the key because the series moved into `objects`
//!      there. The second half is the one driven below, because the stamp test is `<` and so cannot
//!      refuse a current-stamp image over a key it does not know -- what makes that safe is that no
//!      binary at or above 20 produces one.
//!   3. **The capture path writes the array map with nothing decoded.** That path is unchanged: a
//!      key-state blob carries the series under its own name and `apply_key_states` converts the
//!      stored B-tree shape into the array, with no decode and no landing spot in between.
//!
//! What is NOT claimed is a footprint win on a live shard. The compatibility field costs nothing
//! after the fold drains it, but the array and the B-tree are never both resident long enough for
//! the shard-level bracket to see either -- and that bracket is seeded with `strings` and
//! `features` and holds no context events at all, so it does not move for this map in either
//! direction. The measurement below prices the two representations against each other directly,
//! which is the honest form of that claim.

use crate::engine::timestamped_series::TimestampedSeries as _;
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



/// THE PATH NO DECODE FOLD EVER COVERED, AND THE ONLY ONE LEFT.
///
/// `capture_key_states` writes a `context_events` field into the index-log blobs. When no base
/// index decodes, `persistence.rs` takes `ShardState::default()` and folds those blobs straight
/// through `apply_key_states`. This is not an unguarded door: there is no stored index on this
/// path for a stamp to describe, and the blob was written by this binary at this stamp. What
/// covers it is the `KeyedState` conversion from the stored B-tree shape into the array.
///
/// THE DECODE FOLD THIS ARM'S TITLE REFERS TO IS GONE, and the path it could not see is the
/// only one left. That is why this arm survives the compatibility field's removal unchanged:
/// it never tested the fold, it tested the path the fold did not cover.
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
            .map_or(0, |slot| slot.owned_element_count()),
        "the key-state fold delivered the series into the array map with nothing decoded"
    );

    // THE REMOVAL HALF: a blob naming the key but carrying no `context_events` field.
    let departure = serde_json::json!({ "key": "ctx:event:t:a", "features": {} });
    crate::engine::apply_key_states(&mut shard, std::slice::from_ref(&departure));

    assert!(
        !shard.context_events().contains_key("ctx:event:t:a"),
        "an absent field is read as a removal, which is what this helper is for"
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
/// THE LEGACY EVENT KEY HAS NO DOOR LEFT, which is why the compatibility field is gone.
///
/// # WHAT REPLACED "DRAINED AND NEVER REFILLED"
///
/// This module used to assert that a decode-only landing spot was drained by a fold and never
/// written again. There is no landing spot now, and the claim that retired it is stronger: nothing
/// can deliver a top-level `context_events` key at all.
///
/// The field was filled by serde alone, from a `ShardState` image carrying that key. Two facts
/// close that path, and neither is "nothing writes it":
///
///   * **A pre-stamp image is refused on every path.** `decode_index_bytes` records that all six of
///     its callers read `index_format_version`, and `the_dump_door_reads_the_index_stamp` and
///     `page_entry_names` drive the refusal on the base-index and manifest doors.
///   * **An image at stamp 20 or later does not carry the key.** That is the half this arm drives:
///     the event series moved into `objects` at 20, so the encoder emits no such key, and a
///     current-stamp image that somehow carried one would be IGNORED rather than folded.
///
/// The second point is the one worth a test, because the stamp check is `<` and therefore cannot
/// refuse a current-stamp image over a key it does not know. What makes that safe is not the gate;
/// it is that no binary at or above 20 produces the key.
#[test]
fn the_legacy_event_key_has_no_door_left() {
    let mut shard = ShardState::default();
    shard.set_context_event_series("ctx:event:t:a", series_of(&[1, 2]));

    let encoded = serde_json::to_value(&shard).expect("the shard state encodes");

    // THE ENCODER EMITS NO SUCH KEY, under the old name or a new one. This is the half that makes
    // the field unreachable rather than merely unused.
    assert!(
        encoded.get("context_events").is_none(),
        "the encoder emitted a top-level `context_events` key, so an image carrying the legacy \
         name is reachable again and the compatibility field was retired too early"
    );
    assert!(
        encoded.get("context_events_series").is_none(),
        "and it must not appear under a NEW top-level key either, which would be the same format \
         change wearing a different name"
    );

    // AND THE SERIES IS IN `objects`, which is where the key went. Asserted so that the absence
    // above cannot be satisfied by the series having gone missing altogether -- an encoder that
    // emitted nothing at all would pass the two checks above for the wrong reason.
    let objects = encoded
        .get("objects")
        .and_then(|value| value.as_object())
        .expect("the consolidated map is on the wire");
    assert!(
        objects.contains_key("context_event"),
        "the event series is in neither a top-level key nor the consolidated map, so this arm is \
         measuring an encoder that dropped the data: {:?}",
        objects.keys().collect::<Vec<_>>()
    );

    // A CURRENT-STAMP IMAGE CARRYING THE RETIRED KEY DECODES AND IGNORES IT. `ShardState` has no
    // `deny_unknown_fields`, so this is what would happen to one -- and no reachable image is one.
    let mut carried = encoded.clone();
    carried
        .as_object_mut()
        .expect("the encoded state is an object")
        .insert(
            "context_events".to_string(),
            serde_json::json!({ "ctx:event:t:b": series_of(&[5]) }),
        );
    let decoded: ShardState =
        serde_json::from_value(carried).expect("an image carrying the retired key still decodes");
    assert!(
        decoded.context_events().get("ctx:event:t:b").is_none(),
        "the retired key was folded into the live map, so something still reads it"
    );
    assert!(
        decoded.context_events().contains_key("ctx:event:t:a"),
        "the series that WAS on the wire did not survive the round trip, so this arm proves \
         nothing about the retired key"
    );
}
