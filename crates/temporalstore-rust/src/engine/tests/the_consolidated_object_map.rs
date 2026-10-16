// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The consolidated object map: that its two levels behave, that its outer order is stable, and
//! that an unknown kind spelling is refused rather than aborting.
//!
//! THE MIRROR THIS MODULE WAS WRITTEN AROUND IS GONE. The control-state blocks live in
//! `ShardState::objects`, keyed kind then object key. A compatibility copy of the old top-level
//! key was written beside it, because every image at the then-current stamp carried that key and
//! dropping it would have lost those rows with every door shut. Its stated expiry was a change
//! that moves `SHARD_INDEX_FORMAT_VERSION` -- which makes those images refuse as stale and fall
//! back to replay -- and that change removed it.
//!
//! So the mirror, its decode fold, its agreement predicate, its disagreement counter, its
//! test-only injector and the tripwire that held it to an expiry condition are all gone, and what
//! is left here is the consolidated map on its own terms.
//!
//! rust-internal: measures and guards this crate's own resident structures, no product behaviour

use crate::engine::object_record::{ObjectRecord, ObjectSlot, OwnedElementTenancy};
use crate::engine::state::ShardState;
use crate::engine::storage_bucket_internals::StoredModelKind;

/// A slot pointing at a page, built the way production builds one -- through `set_address` rather
/// than by hand, so a fixture cannot be the one place the narrow door is bypassed.
fn slot_at(slab: u64, offset: u64, length: u64) -> ObjectSlot {
    // ORDINAL PRESENT AND ZERO, which is what a single-page object carries by construction --
    // and what the record's inline arm can hold. The inline arm keeps the ordinal's PRESENCE
    // and not its value, so a fixture passing a nonzero ordinal would not round-trip and the
    // test would be measuring that rather than the pair.
    let entry = crate::block_store::ElementEntry::from_parts(
        slab,
        offset,
        length,
        Some(0),
        Some(11),
    );
    let mut record = ObjectRecord::default();
    record.set_address(&entry);
    ObjectSlot::from_record(record)
}

// =================================================================================================
// THE MUTATOR PAIR
// =================================================================================================

/// THE CONTROL-STATE MUTATORS KEEP THE CONSOLIDATED MAP RIGHT THROUGH A REMOVAL SEQUENCE.
///
/// # WHAT THIS TEST USED TO BE, AND WHAT LEFT WITH THE MIRROR
///
/// It drove this sequence and asserted after EVERY step that the consolidated map and a
/// compatibility mirror agreed, with a negative control that forced them apart to prove the
/// agreement check could fail. The mirror is gone -- removed by the change that moved the format
/// stamp, which is what its own expiry condition asked for -- so the per-step agreement check and
/// its negative control are gone with it. There is no second copy to drift.
///
/// THE SEQUENCE IS KEPT, and that is deliberate rather than inertia. Its value was never only the
/// mirror: it drives REPLACE, REMOVE, REMOVE-ABSENT, IN-PLACE MUTATION, MUTATE-ABSENT and
/// WHOLE-MAP REPLACEMENT against the one map, and each of those still has its own assertion about
/// what the authoritative map holds afterwards. The remove side in particular is the half that
/// gets forgotten, which was true of a mirrored pair and is just as true of a two-level map where
/// a removal has to reach into the right kind's inner table.
#[test]
fn the_control_state_mutators_keep_the_consolidated_map_right_through_removals() {
    let mut shard = ShardState::default();
    let a = slot_at(1, 0, 32);
    let b = slot_at(1, 64, 48);
    let c = slot_at(2, 128, 16);

    shard.insert_control_state_block("k1".into(), a.clone());

    shard.insert_control_state_block("k2".into(), b.clone());

    // REPLACING an existing key, which is a write to a key both copies already hold.
    shard.insert_control_state_block("k1".into(), c.clone());
    assert_eq!(
        shard.control_state_blocks().get("k1"),
        Some(&c),
        "the replacement did not take in the authoritative map"
    );

    // THE REMOVE SIDE.
    let removed = shard.remove_control_state_block("k2");
    assert_eq!(removed, Some(b), "remove must hand back what it removed");
    assert!(
        !shard.control_state_blocks().contains_key("k2"),
        "k2 survived its own removal in the authoritative map"
    );

    // REMOVING A KEY THAT WAS NEVER THERE must not invent one in either copy.
    assert_eq!(
        shard.remove_control_state_block("never-existed"),
        None,
        "removing an absent key must answer None"
    );

    // IN-PLACE MUTATION through the closure, which is the member that would otherwise hand out a
    // `&mut` to one copy alone.
    let other = crate::block_store::ElementEntry::from_parts(9, 512, 64, Some(0), Some(11));
    let touched = shard.with_control_state_block_mut("k1", |slot| {
        slot.set_address(&other);
    });
    assert!(touched.is_some(), "the in-place mutation did not find k1");
    assert_eq!(
        shard.control_state_blocks().get("k1").map(|slot| slot.address()),
        Some(other),
        "the in-place mutation did not reach the authoritative map"
    );

    // WHAT THE INLINE ARM DOES WITH A NONZERO ORDINAL, PINNED HERE BECAUSE THE TWO PATHS
    // DISAGREE. The record's inline arm holds the ordinal's PRESENCE and not its value -- correct,
    // and measured: a single-page object has page zero by construction, and 400 of 400 live
    // addresses carried ordinal 0. The CONVERSION path refuses a nonzero one by name
    // (`NonZeroPageOrdinalDoesNotFitInline`). `set_address` does not refuse it; it writes the
    // presence bit and drops the value.
    //
    // That asymmetry is correct today only because the maps holding records are single-page. It
    // is asserted rather than trusted, so that the day a multi-page object reaches this door the
    // behaviour is a stated fact with a test on it rather than a surprise -- and the owned element
    // array is where such an object belongs.
    let nonzero_ordinal =
        crate::block_store::ElementEntry::from_parts(1, 0, 16, Some(7), Some(11));
    let mut probe = ObjectRecord::default();
    probe.set_address(&nonzero_ordinal);
    assert_eq!(
        probe.address().block_id(),
        Some(0),
        "the inline arm now round-trips a nonzero page ordinal. If that is deliberate, this          assertion and the note above are stale; if it is not, a page ordinal is being silently          zeroed on the compactor write-back path"
    );

    // Mutating a key that is not there must change nothing and say so.
    assert!(
        shard.with_control_state_block_mut("absent", |_| ()).is_none(),
        "mutating an absent key must answer None"
    );

    // THE WHOLE-MAP REPLACEMENT, which is where a field assignment would have left the mirror
    // holding every key the replacement dropped.
    let mut replacement = std::collections::HashMap::new();
    replacement.insert(std::sync::Arc::<str>::from("fresh"), a.clone());
    shard.set_all_control_state_blocks(replacement);
    assert_eq!(
        shard.control_state_blocks().len(),
        1,
        "the whole-map replacement left keys the replacement did not contain"
    );
    assert!(
        !shard.control_state_blocks().contains_key("k1"),
        "k1 survived a whole-map replacement that did not include it -- a ghost entry"
    );

}

// =================================================================================================
// THE OUTER MAP'S ORDER
// =================================================================================================

/// THE OUTER MAP ITERATES IN ONE ORDER WHATEVER ORDER ITS KINDS ARRIVED IN.
///
/// # WHY THIS IS NOT A TEST ABOUT A CONTAINER CHOICE
///
/// The dump manifest feeds its digest from an iteration, key by key, and recomputes and compares
/// it on restore. So the digest depends on iteration order -- and the only existing test over that
/// key list uses `.any()`, which is order-INSENSITIVE. An ordering regression there is therefore
/// silent today, and it would invalidate every manifest written before it.
///
/// This is the first thing in the tree asserting that order is stable at all, which is why it is
/// its own named test rather than an assertion buried inside a migration test.
///
/// WHAT IT STOPS, CONCRETELY. Swapping the outer `BTreeMap` for a `HashMap` on the entirely
/// reasonable grounds that there are only seventeen kinds, so the container cannot matter for
/// lookup cost. It does not matter for cost. It decides the digest.
#[test]
fn the_consolidated_outer_map_iterates_in_one_order_whatever_the_insertion_order() {
    let slot = slot_at(3, 96, 24);

    // The same four kinds, inserted in two deliberately different orders.
    let forward = [
        StoredModelKind::String,
        StoredModelKind::Feature,
        StoredModelKind::ControlState,
        StoredModelKind::ContextEvent,
    ];
    let mut backward = forward;
    backward.reverse();

    let build = |kinds: &[StoredModelKind]| {
        let mut shard = ShardState::default();
        for (n, kind) in kinds.iter().enumerate() {
            shard
                .objects
                .entry(*kind)
                .or_default()
                .insert(std::sync::Arc::from(format!("k{n}").as_str()), slot.clone());
        }
        shard.objects.keys().copied().collect::<Vec<_>>()
    };

    let one = build(&forward);
    let other = build(&backward);

    assert_eq!(
        one.len(),
        forward.len(),
        "the fixture did not install every kind, so this compares a shorter list than intended"
    );
    assert_eq!(
        one, other,
        "the outer map iterated {one:?} when the kinds arrived one way and {other:?} when they \
         arrived the other. The dump manifest digest is fed from this iteration and recomputed on \
         restore, so an order that depends on insertion order silently invalidates every manifest \
         written before the change. If the container was swapped for a HashMap because seventeen \
         entries cannot matter for lookup cost -- that is true, and it is not what this order is \
         for"
    );

    // And the order is the kinds' own, not an accident of how they were inserted.
    let mut sorted = one.clone();
    sorted.sort();
    assert_eq!(
        one, sorted,
        "the outer map's iteration order is not its key order, so it is not the deterministic \
         order the digest needs"
    );
}

// =================================================================================================
// THE DECODE FOLD
// =================================================================================================

// =================================================================================================
// THE UNKNOWN SPELLING
// =================================================================================================

/// AN UNKNOWN MODEL-KIND SPELLING IS REFUSED AND DOES NOT ABORT.
///
/// # WHY THIS TEST HAD TO BE WRITTEN BEFORE THE BEHAVIOUR COULD BE TRUSTED
///
/// `StoredModelKind`'s decoder carried a doc comment titled "AN UNKNOWN SPELLING FAILS LOUDLY,
/// NAMING IT", claiming it "refuses the same way `model_report_code` refuses". It did not refuse;
/// it called `panic!`. Nothing ever fed it an unknown spelling, so the divergence between the documented
/// behaviour and the actual behaviour was invisible -- which is exactly how a panic survives under
/// a doc asserting the opposite.
///
/// THE DIFFERENCE IS NOT COSMETIC. Every caller of this decode is a load path. A panic there makes
/// a store UNOPENABLE; an `Err` makes it fall back to replaying the operation log, which is slower
/// and correct. That is the trade `ObjectRecord::try_from` states in capitals and takes
/// deliberately, and the trade every stamp refusal in this tree takes.
#[test]
fn an_unknown_model_kind_spelling_is_refused_instead_of_aborting() {
    // A kind spelling neither half of the registry declares.
    let refusal = serde_json::from_str::<StoredModelKind>("\"a_kind_nobody_declared\"")
        .expect_err("an undeclared spelling must not decode successfully");
    let text = refusal.to_string();
    assert!(
        text.contains("a_kind_nobody_declared"),
        "the refusal does not name the spelling it refused, so a log line from it cannot say \
         which kind the store and the engine disagree about: {text}"
    );
    assert!(
        text.contains("model_kind_registry"),
        "the refusal does not say where to declare the missing spelling, which is the instruction \
         that makes it actionable: {text}"
    );

    // THE POSITIVE CONTROL. A declared spelling still decodes, and to the right variant -- so the
    // refusal above is attributable to the spelling being unknown and not to the decoder being
    // broken for every input.
    let known = serde_json::from_str::<StoredModelKind>("\"control_state\"")
        .expect("a declared spelling must still decode");
    assert_eq!(
        known,
        StoredModelKind::ControlState,
        "a declared spelling decoded to the wrong variant"
    );
}

// =================================================================================================
// THE TRIPWIRE
// =================================================================================================

// =================================================================================================
// WHAT THE DUPLICATION COSTS
// =================================================================================================

// =================================================================================================
// THE SLOT AS A SERIES
// =================================================================================================

/// THE OWNED ARRAY STAYS SORTED, AND WHAT THAT PROTECTS.
///
/// The array is written through `set`, which binary-searches for the insertion position, and read
/// through `get`, which binary-searches for the value. The two agree only while the array is
/// SORTED -- so an insert that appended blindly would make `get` miss points that are present,
/// which a caller reads as a deletion rather than as a bug.
///
/// So this writes points OUT OF ORDER on purpose, and then asserts three separate things: that
/// `points()` comes back ascending, that `get` finds every one of them, and that the array really
/// is stored sorted rather than merely iterated sorted.
///
/// AND THE EMPTY STATE HAS ONE SPELLING. An emptied array gives up its allocation, so
/// `many.is_none()` is the only way to say "no points". A slot holding `Some(empty)` would answer
/// `len() == 0` while still carrying a heap allocation -- a cost and a second representation of one
/// state, which is the kind of pair that later disagrees.
#[test]
fn the_slots_owned_array_stays_sorted_through_out_of_order_writes() {
    use crate::engine::timestamped_series::{TimestampedSeries, TimestampedSeriesWrite};

    let mut slot = ObjectSlot::default();
    assert_eq!(TimestampedSeries::len(&slot), 0, "a fresh slot holds no points");
    assert!(slot.owned_elements().is_none(), "a fresh slot holds no allocation either");

    // Deliberately out of order, with a duplicate instant last to exercise the replace arm.
    let written: [(u64, u64); 6] = [(50, 1), (10, 2), (90, 3), (30, 4), (70, 5), (30, 6)];
    for (at, slab) in written {
        slot.set(at, crate::block_store::ElementEntry::from_parts(slab, 0, 32, Some(0), Some(11)));
    }

    // FIVE distinct instants, because the sixth write replaced instant 30 rather than adding it.
    assert_eq!(
        TimestampedSeries::len(&slot),
        5,
        "a second write at one instant must replace, not append -- the array would hold two \
         points at the same instant and the binary search could find either"
    );

    let instants = slot.points().map(|(at, _)| at).collect::<Vec<_>>();
    assert_eq!(
        instants,
        vec![10, 30, 50, 70, 90],
        "points() must come back ascending; it is the order `get` binary-searches and the order a \
         stored image is collected in"
    );

    // EVERY point findable, which is what an unsorted array would silently break.
    for at in [10u64, 30, 50, 70, 90] {
        assert!(
            TimestampedSeries::get(&slot, at).is_some(),
            "get missed instant {at}, which is present -- a caller reads that as a deletion"
        );
    }
    assert!(
        TimestampedSeries::get(&slot, 60).is_none(),
        "get found an instant that was never written"
    );

    // The replacement took, so the duplicate wrote the LAST value and not the first.
    assert_eq!(
        TimestampedSeries::get(&slot, 30).map(|address| address.block_slab_id()),
        Some(6),
        "the replacing write at instant 30 did not overwrite the earlier one"
    );

    // STORED sorted, not merely iterated sorted -- read the array directly rather than through
    // the accessor that could be sorting on the way out.
    let stored = slot.owned_elements().expect("points are held").to_vec();
    let mut expected = stored.clone();
    expected.sort_by_key(|(at, _)| *at);
    assert_eq!(
        stored, expected,
        "the array is not stored in ascending order, so `get`'s binary search is searching an \
         unsorted slice and will miss points that are present"
    );

    // --- REMOVAL, including the last point giving up the allocation ---
    slot.clear(50);
    assert_eq!(TimestampedSeries::len(&slot), 4, "clear did not remove the point");
    assert!(TimestampedSeries::get(&slot, 50).is_none(), "the cleared point is still findable");
    assert_eq!(
        slot.points().map(|(at, _)| at).collect::<Vec<_>>(),
        vec![10, 30, 70, 90],
        "a removal left the array unsorted or shifted a neighbour"
    );

    slot.clear(999);
    assert_eq!(
        TimestampedSeries::len(&slot),
        4,
        "clearing an instant that is absent changed the array"
    );

    for at in [10u64, 30, 70, 90] {
        slot.clear(at);
    }
    assert_eq!(TimestampedSeries::len(&slot), 0, "the array still holds points");
    assert!(
        slot.owned_elements().is_none(),
        "an emptied array kept its allocation, so `len() == 0` now has two spellings -- \
         `None` and `Some(empty)` -- and a later reader can distinguish states that mean the same \
         thing"
    );
}

// =================================================================================================
// THE PREMISE THE TENANCY WITNESS RESTS ON
// =================================================================================================

/// THE OWNED ELEMENT ARRAY REACHES NO STORED IMAGE -- MEASURED, AND THIS IS WHERE THAT STOPS.
///
/// # WHAT THIS IS FOR
///
/// `ObjectSlot::many` is `#[serde(skip)]` under a `#[serde(transparent)]` struct, so a slot's
/// stored image is exactly the bare record's and the owned array serializes to nothing. The
/// tenancy witness on `set_owned_elements` refuses a durable per-element tenant on that basis
/// alone. A premise asserted only in prose goes stale silently; this one is driven.
///
/// # THE TWO LAYERS, AND WHY BOTH
///
/// The byte-identity half pins what `#[serde(transparent)]` is claiming -- that wrapping the
/// record cost the record nothing on the wire. The round-trip half goes through
/// `encode_index_bytes` and `decode_index_bytes`, which is the layer that DECIDES: that is the
/// codec a dump manifest's index actually carries, so an attribute change a slot-level
/// `serde_json` call happened not to reach would still be caught here.
///
/// # THE POSITIVE CONTROL IS THE FIRST ASSERTION, NOT A COURTESY
///
/// A fixture whose array never got populated round-trips to zero elements and passes everything
/// below while proving nothing -- the same vacuity as a test filter that selects nothing and
/// prints `ok`. So the count is asserted non-zero BEFORE the round-trip, and the rows go in out
/// of order so the door's sort is exercised rather than assumed.
///
/// # WHEN THIS FAILS, ITS MESSAGE IS THE INSTRUCTION
///
/// A surviving array means the stored form has landed, and the witness is now refusing a
/// registration that has become correct.
///
/// rust-internal: guards this crate's own stored shape, no product behaviour
#[test]
fn the_owned_element_array_reaches_no_stored_image() {
    fn element(offset: u64) -> crate::block_store::ElementEntry {
        crate::block_store::ElementEntry::from_parts(3, offset, 16, Some(0), Some(11))
    }

    // THE SAFE TENANCY, which is the one that compiles: `context_index` is `skip_serializing`
    // already, so an empty stored image costs it a rebuild and nothing else.
    let mut slot = slot_at(1, 0, 32);
    slot.set_owned_elements(
        OwnedElementTenancy::RebuiltAtLoad(StoredModelKind::ContextIndex),
        vec![(30, element(192)), (10, element(64)), (20, element(128))],
    );

    // --- POSITIVE CONTROL. Both halves below are vacuous over an empty array.
    assert_eq!(
        slot.owned_element_count(),
        3,
        "the door did not populate the owned element array, so every assertion below would pass \
         over an empty array and measure nothing"
    );

    // --- (a) THE SLOT'S STORED IMAGE IS THE BARE RECORD'S, BYTE FOR BYTE.
    let with_elements = serde_json::to_vec(&slot).expect("a slot serializes");
    let bare = serde_json::to_vec(&slot_at(1, 0, 32)).expect("a bare slot serializes");
    assert_eq!(
        with_elements, bare,
        "a slot carrying three owned elements no longer serializes identically to the same slot \
         carrying none, so the owned array has reached the wire and the tenancy witness in \
         `object_record.rs` is now refusing a registration that has become correct. Read \
         `OwnedElementWireForm`'s doc: it lists what to delete."
    );

    // --- (b) THROUGH THE CODEC A DUMP MANIFEST ACTUALLY CARRIES.
    let mut shard = ShardState::default();
    shard.insert_control_state_block("k1".into(), slot.clone());
    let bytes = crate::engine::encode_index_bytes(&shard);
    let restored =
        crate::engine::decode_index_bytes(&bytes).expect("the index this test just wrote decodes");
    let restored_slot = restored
        .control_state_blocks()
        .get("k1")
        .expect("the key round-tripped away, so this measures the codec and not the array");

    // THE RECORD DID SURVIVE. Without this, a codec that dropped the whole slot would satisfy
    // the element assertion below and read as the premise holding.
    assert_eq!(
        restored_slot.record(),
        slot.record(),
        "the fixed-width record did not survive the round-trip, so the element count below is \
         measuring a broken codec rather than the owned array's absence from the wire"
    );

    assert_eq!(
        restored_slot.owned_element_count(),
        0,
        "THE OWNED ELEMENT ARRAY NOW SURVIVES A ROUND-TRIP THROUGH THE INDEX CODEC, so the \
         premise the tenancy witness rests on is gone and the witness must go with it -- in this \
         change, not later. A durable per-element tenant is now safe to register, while \
         `OwnedElementTenancy::DurableOnTheWire` still cannot be spelled, which refuses correct \
         code. Delete, all in one change: `OwnedElementWireForm`; the `wire_form` field of \
         `DurableOnTheWire`; the empty match arm in `ObjectSlot::set_owned_elements`; the pointer \
         at `ShardState::objects`' declaration in `state.rs`; and this test. See #2165."
    );
}
