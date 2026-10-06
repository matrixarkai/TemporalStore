// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SLOT NAMES ONE OBJECT ACROSS A RELOAD, which is what makes it storable at all.
//!
//! #2057 made a slot stable against the three mutations that invalidated a sorted run's position:
//! `insert` takes the first free slot, `remove` leaves a placeholder, and `valid` is tracked apart
//! from the array length. Those rules held IN MEMORY, and that was the whole of it. Its own test
//! said so in as many words -- "a load re-files what is written through `insert`, so slots are
//! handed out afresh on every load and a slot is a RESIDENT fact" -- and named this step as the one
//! that would make the slot durable and take the next format stamp.
//!
//! # THE TWO THINGS THAT RE-HANDED A SLOT, AND BOTH ARE CLOSED HERE
//!
//!   * THE WIRE. `Serialize` wrote `sorted_ids()`: ascending, holes omitted. A load collected that
//!     through `insert`, which reproduces the written ORDER and not the written POSITIONS. It
//!     writes the slot array now, in slot order, with `null` for a placeholder, and reads it back
//!     by position.
//!   * THE REBUILD. `update_bucket_layout` assigned `bucket.object_index = live_object_ids.into()`
//!     from a `BTreeSet`, so every call re-handed every slot in ascending id order -- and it has
//!     eleven production callers, so a write touching any page in a bucket renumbered every object
//!     in it. It reconciles now, and what it reconciles is counted rather than overwritten.
//!
//! # WHY EACH TEST HERE CARRIES A CONTROL THAT WOULD FAIL UNDER THE OLD BEHAVIOUR
//!
//! Every assertion below is satisfied by an ascending fixture under the OLD code too, so a fixture
//! whose filing order happens to be ascending would pass for the wrong reason. Each test therefore
//! asserts its fixture is NOT in ascending order before asserting anything about it, and the round
//! trip tests additionally drive what the old form would have loaded to, so the difference is
//! measured rather than described.
//!
//! rust-internal: drives this crate's own serde impls and one maintenance path, no external surface

#![allow(clippy::all)]
use super::*;
use crate::engine::state::ObjectIndex;

/// Filed in an order whose slot order is NOT ascending, which is the only order that can tell a
/// positional round trip from an order-preserving one.
const FILED: [u64; 6] = [900, 5, 700, 1, 800, 0];

fn filed_index() -> ObjectIndex {
    let mut index = ObjectIndex::default();
    for id in FILED {
        assert!(index.insert(id), "the fixture filed {id} twice");
    }
    index
}

fn round_trip(index: &ObjectIndex) -> ObjectIndex {
    let written = serde_json::to_string(index).expect("an object list serializes");
    serde_json::from_str(&written).expect("an object list loads")
}

/// THE WHOLE POINT: every slot names the same object after a round trip through the stored form.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn a_slot_names_the_same_object_after_a_round_trip_of_the_stored_index() {
    let index = filed_index();
    assert!(
        matches!(index, ObjectIndex::Many(_)),
        "the fixture must reach the slot-array arm or it is not exercising slots at all"
    );

    let before: Vec<(u64, usize)> = FILED
        .iter()
        .map(|id| (*id, index.slot_of(id).expect("a filed id has a slot")))
        .collect();
    let slot_order: Vec<u64> = index.iter().copied().collect();
    let mut ascending = FILED.to_vec();
    ascending.sort_unstable();

    println!("\n=== what the stored index now carries ===");
    println!("  filed in   : {FILED:?}");
    println!("  slot order : {slot_order:?}");
    println!("  written    : {}", serde_json::to_string(&index).expect("serializes"));

    // THE CONTROL THAT MAKES THE REST MEAN SOMETHING. If the fixture's slot order were already
    // ascending, the old order-preserving load would reproduce these positions too.
    assert_ne!(
        slot_order, ascending,
        "the fixture's slot order is already ascending, so a load that re-handed slots in ascending \
         order would reproduce it and this test would pass under the behaviour it exists to forbid"
    );

    let loaded = round_trip(&index);
    for (id, slot) in &before {
        assert_eq!(
            Some(*slot),
            loaded.slot_of(id),
            "object {id} was in slot {slot} and came back in {:?}; a slot that moves across a round \
             trip cannot be stored on a page entry, which is the only reason this change exists",
            loaded.slot_of(id)
        );
        assert_eq!(
            Some(*id),
            loaded.id_at(*slot),
            "slot {slot} named object {id} and names {:?} after a round trip",
            loaded.id_at(*slot)
        );
    }
    assert_eq!(
        index.object_count(),
        loaded.object_count(),
        "the round trip changed the object count"
    );
    assert_eq!(
        index.slot_count(),
        loaded.slot_count(),
        "the round trip changed the LENGTH of the slot array, so a slot number is bounded \
         differently after a load than before one"
    );

    // AND THE OLD FORM WOULD NOT HAVE DONE THIS, driven rather than asserted about. An ascending
    // sequence of bare ids is exactly what `Serialize` used to write for this fixture.
    let old_form = serde_json::to_string(&ascending).expect("the old shape serializes");
    let from_old_form: ObjectIndex =
        serde_json::from_str(&old_form).expect("the old shape still loads");
    assert_ne!(
        from_old_form.slot_of(&FILED[0]),
        index.slot_of(&FILED[0]),
        "loading the OLD written form reproduces this fixture's slots, so the two forms are not \
         distinguishable and the stamp this change takes would be paying for nothing"
    );
}

/// A PLACEHOLDER IS PART OF THE STORED SHAPE, because closing a hole is what renumbers.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn a_placeholder_survives_the_stored_form_so_the_slots_above_it_do_not_move() {
    let mut index = filed_index();
    let evicted = FILED[1];
    let hole = index.slot_of(&evicted).expect("the id to remove has a slot");
    let above: Vec<(u64, usize)> = FILED
        .iter()
        .filter(|id| **id != evicted)
        .map(|id| (*id, index.slot_of(id).expect("a filed id has a slot")))
        .collect();
    assert!(
        above.iter().any(|(_, slot)| *slot > hole),
        "nothing is filed above the hole, so this fixture cannot show that a hole keeps the slots \
         above it where they are"
    );
    assert!(index.remove(&evicted), "the id to remove was not held");
    assert_eq!(None, index.id_at(hole), "the removal did not leave a placeholder");

    let written = serde_json::to_string(&index).expect("serializes");
    println!("\n=== a stored index carrying a hole ===");
    println!("  removed {evicted} from slot {hole}; written {written}");
    assert!(
        written.contains("null"),
        "the written form is {written} and carries no null, so the placeholder is not stored and \
         every slot above it closes up on the next load"
    );

    let loaded = round_trip(&index);
    assert_eq!(
        None,
        loaded.id_at(hole),
        "slot {hole} came back holding {:?} rather than staying a placeholder",
        loaded.id_at(hole)
    );
    for (id, slot) in &above {
        assert_eq!(
            Some(*slot),
            loaded.slot_of(id),
            "object {id} sat in slot {slot} above the hole and came back in {:?}",
            loaded.slot_of(id)
        );
    }

    // THE NEGATIVE CONTROL: the same ids written WITHOUT the hole -- which is what the old form
    // did -- loads one slot shorter, and that shortening is the renumber.
    let without_the_hole: Vec<u64> = above.iter().map(|(id, _)| *id).collect();
    let closed: ObjectIndex = serde_json::from_str(
        &serde_json::to_string(&without_the_hole).expect("serializes"),
    )
    .expect("loads");
    assert!(
        closed.slot_count() < loaded.slot_count(),
        "writing the same ids without the placeholder gives a {} slot array against {} with it, so \
         storing the hole changes nothing and this test is not measuring the renumber",
        closed.slot_count(),
        loaded.slot_count()
    );
}

/// AN INDEX WRITTEN BEFORE THIS CHANGE LOADS WHERE THE OLD LOADER WOULD HAVE PUT IT.
///
/// DEFENCE IN DEPTH, AND NOT THE UPGRADE PATH, which is worth being exact about. On upgrade the
/// stored stamp is below the constant and `persistence.rs` compares with `<`, so an older index is
/// REFUSED and rebuilt from the log; it is never decoded. What this test pins is that IF it is
/// decoded -- the one shape where a decode runs before the stamp check -- it lands where the old
/// loader would have put it rather than somewhere new. Asserted against an index built the old way
/// rather than against a literal.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn an_index_written_before_this_change_loads_to_the_slots_the_old_loader_would_have_given() {
    let mut ascending = FILED.to_vec();
    ascending.sort_unstable();
    let old_form = serde_json::to_string(&ascending).expect("the old written shape serializes");
    println!("\n=== an index written before this change ===");
    println!("  old form: {old_form}");

    let loaded: ObjectIndex = serde_json::from_str(&old_form).expect("an old index still loads");

    // What the OLD loader did: collect the written sequence through `insert`.
    let mut as_the_old_loader_would: ObjectIndex = ObjectIndex::default();
    for id in &ascending {
        as_the_old_loader_would.insert(*id);
    }

    for id in &ascending {
        assert_eq!(
            as_the_old_loader_would.slot_of(id),
            loaded.slot_of(id),
            "object {id} loads into slot {:?} and the old loader would have put it in {:?}; an \
             upgrade that moves a slot on a store already on disk is the one migration this change \
             must not need",
            loaded.slot_of(id),
            as_the_old_loader_would.slot_of(id)
        );
    }
    assert_eq!(
        as_the_old_loader_would.slot_count(),
        loaded.slot_count(),
        "the old form loads to a different array length than the old loader produced"
    );
    // AND THE SENSITIVITY CONTROL: the agreement above is a property of ASCENDING input, not of
    // any input.
    //
    // CORRECTED FROM A CONTROL THAT COULD NOT FAIL. This re-filed the positionally-loaded array
    // through `insert` IN SLOT ORDER and asserted the positions differed -- but inserting ids in
    // slot order reproduces slot order, so the two agreed for every fixture and the control was
    // vacuous. It failed on the first run, which is the only reason it is right now.
    //
    // What discriminates is the order the OLD WRITER wrote: ascending. A non-ascending array stored
    // positionally must load to DIFFERENT slots than the same ids written ascending, or the two
    // written forms are indistinguishable and the stamp pays for nothing.
    let filed = filed_index();
    let positional = round_trip(&filed);
    let as_the_old_writer_wrote: ObjectIndex = serde_json::from_str(
        &serde_json::to_string(&filed.sorted_ids()).expect("the old written shape serializes"),
    )
    .expect("loads");
    let moved: Vec<u64> = FILED
        .iter()
        .copied()
        .filter(|id| positional.slot_of(id) != as_the_old_writer_wrote.slot_of(id))
        .collect();
    println!("  ids whose slot differs between the two written forms: {moved:?}");
    assert!(
        !moved.is_empty(),
        "every id lands in the same slot whether the array is written positionally or ascending, \
         so the two written forms are indistinguishable and this change moves no stored shape"
    );
}

/// A REPEATED ID IN A STORED ARRAY IS PLACEHOLDERED WHERE IT REPEATS, AND COUNTED.
///
/// Dropping it would shorten the array and move every id above it, which is the renumber the whole
/// change exists to prevent -- so the first position wins, because it is the one anything already
/// stored would be naming.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn a_stored_array_that_names_one_object_twice_keeps_the_first_slot_and_counts_the_repeat() {
    let before = crate::engine::state::stored_slots_repeated_an_id();
    // Position 1 repeats what position 0 holds; position 2 is above both and must not move.
    let doubled: Vec<Option<u64>> = vec![Some(77), Some(77), Some(42)];
    let loaded: ObjectIndex = serde_json::from_str(
        &serde_json::to_string(&doubled).expect("serializes"),
    )
    .expect("loads");
    let after = crate::engine::state::stored_slots_repeated_an_id();

    println!("\n=== a stored array naming one object twice ===");
    println!("  stored {doubled:?} -> slot 0 {:?}, slot 1 {:?}, slot 2 {:?}", loaded.id_at(0), loaded.id_at(1), loaded.id_at(2));

    assert_eq!(Some(77), loaded.id_at(0), "the FIRST slot naming the id must win");
    assert_eq!(None, loaded.id_at(1), "the repeat must become a placeholder, not be dropped");
    assert_eq!(
        Some(42),
        loaded.id_at(2),
        "the id above the repeat moved, so the repeat was closed rather than placeholdered"
    );
    assert_eq!(2, loaded.object_count(), "the repeat was counted as a second object");
    assert_eq!(3, loaded.slot_count(), "the array length changed, which renumbers");
    assert!(
        after > before,
        "the repeat counter read {before} before and {after} after, so a stored array disagreeing \
         with itself is accepted silently -- which is the reporting hole this counter exists to close"
    );
}

/// THE DIVERGENCE COUNTER MOVES ON A PLANTED DISAGREEMENT, and this exists before any zero is read.
///
/// The maintenance scan used to ASSIGN the object list from the pages, so a list that disagreed was
/// overwritten and the disagreement left no trace. "8,679 rebuilds, 0 divergences" was therefore a
/// statement about a comparison nobody made. The scan reconciles now and counts what it reconciles
/// -- and a counter that has never moved cannot be told from one that is not wired up, which is why
/// the POSITIVE control comes first here and the clean arm is only meaningful after it.
///
/// BOTH ARMS IN ONE BODY, because the counters are process-wide: a delta taken across two `#[test]`
/// functions cannot be attributed, and this tree has a recorded case of exactly that. Within one
/// body nothing else is running at `--test-threads=1`, so the subtraction is sound.
///
/// rust-internal: drives one maintenance path and its counters, no external surface
#[test]
fn the_divergence_counter_moves_on_a_planted_disagreement_and_not_on_a_clean_rebuild() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "object-list-divergence".to_string(),
                shard_uri: "local://object-list-divergence/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: 63,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    for index in 0..60 {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("div-str-{index}"),
                value: vec![b'v'; 48],
            },
        });
    }

    // THE DENOMINATOR IS REACHED, AND NOT BY THE WORKLOAD.
    //
    // CORRECTED FROM A FLOOR THAT FAILED, and the failure is a fact worth keeping: a plain write
    // does NOT reach `update_bucket_layout`. The write path classifies the layout in place and
    // skips the rescan, so sixty writes moved `rebuilds` not at all. The scan runs on the load,
    // release and delete paths and from the flag-refresh sweep. So the floor is on a scan this
    // test causes itself, asserted as a delta rather than as a level.
    let seeded = engine.object_index_divergence_report();
    println!("\n=== after the workload ===\n  {seeded:?}");
    //
    // AND NOT AN ABSOLUTE LEVEL. This asserted the counter read ZERO here, and that passed alone
    // and FAILED in the suite: these counters are process-wide and monotonic, so every test that
    // ran before this one in the same process has already moved them. A level is not attributable;
    // only a delta taken inside this body is. Every assertion below is therefore a subtraction
    // between two readings taken around one action.

    // --- ARM 1, THE POSITIVE CONTROL: plant an id no live page names, then rebuild that bucket. ---
    const PLANTED: u64 = 0xD15A_6E_ED_BEEF_0001u64;
    let (routing_bucket, slots_before) = {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        let routing_bucket = *shard
            .bucket_index
            .bucket_map
            .iter()
            .find(|(_, bucket)| !bucket.block_index.is_empty())
            .expect("the workload filed at least one page")
            .0;
        let bucket = shard
            .bucket_index
            .bucket_map
            .get_mut(&routing_bucket)
            .expect("the bucket just found");
        let before: Vec<Option<u64>> = (0..bucket.object_index.slot_count())
            .map(|slot| bucket.object_index.id_at(slot))
            .collect();
        assert!(
            bucket.object_index.insert(PLANTED),
            "the planted id was already held, so this arm plants nothing"
        );
        (routing_bucket, before)
    };

    let before_rebuild = engine.object_index_divergence_report();
    {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .get_mut(&routing_bucket)
            .expect("the planted bucket");
        crate::engine::storage_bucket_internals::update_bucket_layout(1, bucket);
    }
    let after_plant = engine.object_index_divergence_report();
    println!("  after the planted rebuild: {after_plant:?}");

    assert_eq!(
        before_rebuild.list_held_an_object_no_page_names + 1,
        after_plant.list_held_an_object_no_page_names,
        "the planted id was not counted: the counter read {} before the rebuild and {} after. A \
         counter that does not move on a disagreement cannot certify the absence of one",
        before_rebuild.list_held_an_object_no_page_names,
        after_plant.list_held_an_object_no_page_names
    );
    let sample = after_plant
        .last_divergence
        .as_ref()
        .expect("a counted divergence must name the row it found");
    assert_eq!(
        PLANTED, sample.object_id,
        "the last divergence names object {} rather than the planted {PLANTED}, so the delta above \
         could have come from any other bucket in this process",
        sample.object_id
    );
    assert!(
        after_plant.rebuilds > before_rebuild.rebuilds,
        "the rebuild denominator did not move, so the scan that reported the divergence did not run"
    );

    // AND THE PLANTED ID IS GONE WITHOUT MOVING WHAT STAYED -- the reconcile, not an assignment.
    {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .expect("the planted bucket");
        assert_eq!(
            None,
            bucket.object_index.slot_of(&PLANTED),
            "the planted id survived the reconcile"
        );
        let after: Vec<Option<u64>> = (0..bucket.object_index.slot_count())
            .map(|slot| bucket.object_index.id_at(slot))
            .collect();
        assert_eq!(
            slots_before, after,
            "the reconcile left the bucket at {after:?} where it was {slots_before:?}. Removing an \
             id it should never have held moved the ids that belonged there"
        );
    }

    // --- ARM 2, THE NEGATIVE CONTROL: the same rebuild with nothing planted moves nothing. ---
    let before_clean = engine.object_index_divergence_report();
    {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .get_mut(&routing_bucket)
            .expect("the planted bucket");
        crate::engine::storage_bucket_internals::update_bucket_layout(1, bucket);
    }
    let after_clean = engine.object_index_divergence_report();
    println!("  after a clean rebuild of the same bucket: {after_clean:?}");
    assert!(
        after_clean.rebuilds > before_clean.rebuilds,
        "the clean arm's rebuild did not run, so its zero is the absence and not an agreement"
    );
    assert_eq!(
        before_clean.list_held_an_object_no_page_names,
        after_clean.list_held_an_object_no_page_names,
        "a rebuild with nothing planted counted a divergence, so the counter moves on agreement too \
         and arm 1 proved nothing"
    );
    assert_eq!(
        before_clean.pages_named_an_object_the_list_lacked,
        after_clean.pages_named_an_object_the_list_lacked,
        "a rebuild with nothing planted found a page naming an object its list lacked"
    );
    println!(
        "\n  VERDICT: the counter moved on the plant and not on the clean rebuild, over \
         {} rebuilds",
        after_clean.rebuilds
    );
}
