// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The consolidated object map: that its two levels behave, that its mirror cannot drift, and
//! that the compatibility key it still writes has a stated reason to exist and a trigger to stop.
//!
//! WHY A MIRROR EXISTS AT ALL, stated once here so each test below can be read without it. The
//! control-state blocks live in `ShardState::objects`, keyed kind then object key, which is
//! authoritative. The old top-level key is still written beside it -- and the reason is the STAMP,
//! not the doors.
//!
//! All six callers of `decode_index_bytes` read `index_format_version` now, so every PRE-stamp
//! image is refused on every path. Images at the CURRENT stamp are the problem: the old key is
//! serialized unconditionally, so every one of them carries it, none is stale, `objects` defaults
//! to empty, and a removed field is silently ignored. Dropping the key today loses those rows with
//! every door shut. Only a stamp bump makes them refuse.
//!
//! rust-internal: measures and guards this crate's own resident structures, no product behaviour

use crate::engine::object_record::{ObjectRecord, ObjectSlot};
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

/// THE TWO COPIES STAY IN STEP THROUGH A SEQUENCE THAT INCLUDES REMOVALS, CHECKED AFTER EVERY STEP.
///
/// # WHY REMOVALS, AND WHY NOT ONLY AT THE END
///
/// A mirrored pair of maps diverges on the REMOVE side far more often than the insert side. The
/// insert gets mirrored because it is the code being written; the remove lives somewhere else and
/// gets forgotten, and what it leaves behind is a ghost entry in the copy nobody updated, which
/// reads as live data. An insert-only test passes straight over that bug, so this drives removals,
/// a removal of a key that was never there, an in-place mutation, and a whole-map replacement.
///
/// And the check runs after EVERY step rather than once at the end, because a pair that diverges
/// and then reconverges passes an end-state assertion while a reader looking in between sees the
/// wrong thing. The end-state version would have been the comfortable one and would prove less.
///
/// # THE NEGATIVE CONTROL IS THE POINT OF THE SECOND HALF
///
/// `control_state_copies_agree` returning `true` after every step proves nothing unless something
/// can make it return `false`. So the second half forces the copies apart through the test-only
/// injector and asserts the check NOTICES. Without that, this test would pass identically if the
/// check were `fn control_state_copies_agree(&self) -> bool { true }`.
#[test]
fn the_mutator_pair_keeps_both_copies_in_step_through_removals() {
    let mut shard = ShardState::default();
    let a = slot_at(1, 0, 32);
    let b = slot_at(1, 64, 48);
    let c = slot_at(2, 128, 16);

    // Every step below is followed by the same assertion, with a label so a failure says WHICH
    // step broke the pair rather than only that it is broken.
    let mut step = 0usize;
    let mut check = |shard: &ShardState, what: &str, step: &mut usize| {
        *step += 1;
        assert!(
            shard.control_state_copies_agree(),
            "step {} ({what}) left the two copies of the control-state blocks disagreeing: the \
             consolidated map holds {} and the compatibility copy holds {}. A write reached one \
             and not the other, which is the ghost-entry divergence the mutator pair exists to \
             prevent",
            *step,
            shard.control_state_blocks().len(),
            shard.legacy_control_state_len_for_test()
        );
    };

    check(&shard, "empty", &mut step);

    shard.insert_control_state_block("k1".into(), a.clone());
    check(&shard, "insert k1", &mut step);

    shard.insert_control_state_block("k2".into(), b.clone());
    check(&shard, "insert k2", &mut step);

    // REPLACING an existing key, which is a write to a key both copies already hold.
    shard.insert_control_state_block("k1".into(), c.clone());
    check(&shard, "replace k1", &mut step);
    assert_eq!(
        shard.control_state_blocks().get("k1"),
        Some(&c),
        "the replacement did not take in the authoritative map"
    );

    // THE REMOVE SIDE.
    let removed = shard.remove_control_state_block("k2");
    check(&shard, "remove k2", &mut step);
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
    check(&shard, "remove absent key", &mut step);

    // IN-PLACE MUTATION through the closure, which is the member that would otherwise hand out a
    // `&mut` to one copy alone.
    let other = crate::block_store::ElementEntry::from_parts(9, 512, 64, Some(0), Some(11));
    let touched = shard.with_control_state_block_mut("k1", |slot| {
        slot.set_address(&other);
    });
    assert!(touched.is_some(), "the in-place mutation did not find k1");
    check(&shard, "mutate k1 in place", &mut step);
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
    check(&shard, "mutate absent key", &mut step);

    // THE WHOLE-MAP REPLACEMENT, which is where a field assignment would have left the mirror
    // holding every key the replacement dropped.
    let mut replacement = std::collections::HashMap::new();
    replacement.insert(Box::<str>::from("fresh"), a.clone());
    shard.set_all_control_state_blocks(replacement);
    check(&shard, "replace the whole map", &mut step);
    assert_eq!(
        shard.control_state_blocks().len(),
        1,
        "the whole-map replacement left keys the replacement did not contain"
    );
    assert!(
        !shard.control_state_blocks().contains_key("k1"),
        "k1 survived a whole-map replacement that did not include it -- a ghost entry"
    );

    assert!(step >= 9, "the sequence ran {step} steps, which is fewer than it was written with");

    // --- THE NEGATIVE CONTROL. Force the copies apart and require the check to catch it.
    shard.force_control_state_copies_apart_for_test("fresh");
    assert!(
        !shard.control_state_copies_agree(),
        "the agreement check did NOT notice a key removed from one copy alone, so every \
         assertion above is vacuous -- the check cannot distinguish agreement from disagreement"
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
                .insert(format!("k{n}").into_boxed_str(), slot.clone());
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

/// A LEGACY-ONLY IMAGE FOLDS FORWARD, AND A WELL-FORMED ONE REPORTS NO DISAGREEMENT.
///
/// The fold is what makes removing readers of the old key safe while doors remain unguarded: an
/// image carrying only the top-level key has its rows moved into the consolidated map, so a reader
/// that looks only at the consolidated map still finds them.
///
/// THE DISAGREEMENT COUNTER IS ASSERTED AT ZERO, because a counter nothing asserts on is a counter
/// nobody reads. Both copies present and EQUAL is the expected steady state and must not count;
/// this test is what proves it does not.
#[test]
fn a_legacy_only_control_state_image_folds_into_the_consolidated_map() {
    crate::engine::state::reset_legacy_control_state_disagreements();

    let slot = slot_at(4, 32, 128);
    let mut shard = ShardState::default();

    // Exactly what a pre-consolidation image carries: the top-level key populated, the
    // consolidated map absent.
    shard.plant_legacy_control_state_for_test("legacy-key".into(), slot.clone());
    assert!(
        shard.control_state_blocks().is_empty(),
        "the fixture is not a legacy-only image -- the consolidated map already holds something, \
         so a fold that did nothing would still look like it worked"
    );

    shard.fold_legacy_control_state_blocks();

    assert_eq!(
        shard.control_state_blocks().get("legacy-key"),
        Some(&slot),
        "the fold did not move a legacy-only row into the consolidated map, which is the row a \
         reader of the consolidated map alone would have lost"
    );
    assert!(
        shard.control_state_copies_agree(),
        "the fold left the two copies disagreeing"
    );
    assert_eq!(
        crate::engine::state::legacy_control_state_disagreements(),
        0,
        "a well-formed image reported a disagreement. Both copies present and EQUAL is the \
         expected steady state under the compatibility write and must not be counted, or the \
         counter means only that an old image was read"
    );

    // AND A SECOND FOLD IS A NO-OP, so a path that decodes twice cannot double-count or disturb
    // the agreement.
    shard.fold_legacy_control_state_blocks();
    assert!(shard.control_state_copies_agree(), "a second fold broke the pair");
    assert_eq!(
        crate::engine::state::legacy_control_state_disagreements(),
        0,
        "a second fold over an already-folded image counted a disagreement"
    );
}

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

/// THE COMPATIBILITY KEY STILL HAS A REASON TO EXIST -- AND THIS FAILS WHEN IT STOPS.
///
/// # READ THE FAILURE MESSAGE. IT IS THE INSTRUCTION.
///
/// The compatibility copy of the control-state blocks is written and never read. That is a
/// deliberate cost with a precondition for removal, and a write-only field with no expiry
/// condition is how the retired sequence map became something a later reader mistook for dead
/// code and queued for deletion on a premise that turned out false.
///
/// # WHAT THE PRECONDITION ACTUALLY IS, AND WHY IT IS NOT THE DOORS
///
/// The first version of this guard asserted that a door still trusted a stored index without
/// reading its stamp. That was wrong, and the fix is worth stating because the wrong version
/// looked more thorough.
///
/// Shutting the doors makes every PRE-STAMP image refused on every path. It does nothing about
/// images at the CURRENT stamp -- and those are the ones that matter, because
/// `control_state_blocks` is serialized with `#[serde(default)]` and NO `skip_serializing_if`, so
/// the key is written unconditionally and EVERY image at this stamp carries it. Such an image is
/// not stale, so no door refuses it; `objects` defaults to empty; and a reader of the consolidated
/// map alone would find nothing. The rows would be gone with every door shut.
///
/// So the precondition is the STAMP, not the doors. The key can depart the stored image only in a
/// change that moves `SHARD_INDEX_FORMAT_VERSION`, which is what makes every image carrying the
/// old key refuse as stale and fall back to replay. The doors being shut is necessary -- it is
/// what makes that refusal happen on all six decode callers rather than one -- and it is already
/// true. The stamp is the half still outstanding, so the stamp is what this asserts.
///
/// rust-internal: guards this crate's own stored shape, no product behaviour
#[test]
fn the_compatibility_key_still_has_a_reason_to_exist() {
    assert_eq!(
        crate::engine::SHARD_INDEX_FORMAT_VERSION,
        14,
        "THE FORMAT STAMP HAS MOVED, so the compatibility key can go -- and it must go in the          change that moved it, not later.
         
         Why this is now safe: every image carrying the top-level `control_state_blocks` key was          written at stamp 14 or earlier. A stamp above 14 makes all of them refuse as stale on          every one of the six `decode_index_bytes` callers (all of which read the stamp as of          #2150), and a refusal falls back to replaying the operation log, which rebuilds the          consolidated map correctly.
         
         What to remove, all of it in one change:
           1. `ShardState::control_state_blocks_compat`, including its               `#[serde(rename = \"control_state_blocks\")]`. This is what takes the key out of               the stored image, and it is why the stamp had to move.
           2. `ShardState::fold_legacy_control_state_blocks` and its call in               `decode_index_bytes`.
           3. The mirroring half of `insert_control_state_block`,               `remove_control_state_block`, `with_control_state_block_mut` and               `set_all_control_state_blocks`. They keep their names and write only `objects`.
           4. `control_state_copies_agree`, the disagreement counter and its accessors, the               test-only injector, and this test.
         
         IF YOU BUMPED THE STAMP FOR AN UNRELATED REASON: removing this shim is still correct and          still safe, and leaving it is still a cost -- 119.4 B per control-state key, measured on          a seeded map. Do it in your change or file it, but do not relax this assertion to the          new number and move on, because that puts the shim back to having no expiry condition,          which is the state this guard exists to prevent."
    );
}

// =================================================================================================
// WHAT THE DUPLICATION COSTS
// =================================================================================================

/// WHAT THE COMPATIBILITY COPY COSTS, MEASURED ON A MAP THAT IS NOT EMPTY.
///
/// # WHY THE FIXTURE IS SEEDED, AND WHY THAT IS THE WHOLE POINT
///
/// The footprint bracket's fixture seeds single-page objects and feature series only, so
/// `control_state_blocks` is EMPTY in it. A duplication cost measured there is zero -- and zero
/// would read as evidence that duplication is free, which is the same vacuity as a test filter
/// that selects nothing and prints `ok`. A number that cannot discriminate the outcomes is worse
/// than no number, because it looks like evidence.
///
/// So this seeds the map and prices the second copy directly, and the figure is reported against a
/// NAMED denominator rather than as a total.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
fn what_the_compatibility_copy_costs_on_a_map_that_is_not_empty() {
    /// Enough keys that per-key arithmetic is not dominated by one table growth step.
    const SEEDED_KEYS: usize = 2_000;
    /// The key width the campaign's other figures were taken at, so this one is comparable.
    const KEY_WIDTH: usize = 19;

    let mut shard = ShardState::default();
    for n in 0..SEEDED_KEYS {
        let key = format!("{n:0KEY_WIDTH$}", KEY_WIDTH = KEY_WIDTH).into_boxed_str();
        shard.insert_control_state_block(key, slot_at(1, (n as u64) * 64, 32));
    }

    // --- DENOMINATORS FIRST, asserted, because a zero divides into anything and an empty map
    // --- would make the figure below zero and meaningless.
    let authoritative_keys = shard.control_state_blocks().len();
    let mirrored_keys = shard.legacy_control_state_len_for_test();
    assert_eq!(
        SEEDED_KEYS, authoritative_keys,
        "the fixture seeded {SEEDED_KEYS} control-state blocks and the consolidated map holds \
         {authoritative_keys} -- a cost measured over a map this short is not the cost claimed"
    );
    assert_eq!(
        SEEDED_KEYS, mirrored_keys,
        "the compatibility copy holds {mirrored_keys} of {SEEDED_KEYS} seeded keys, so the pair \
         diverged while seeding and this measures the wrong thing"
    );
    assert!(
        shard.control_state_copies_agree(),
        "the two copies disagree, so the figure below prices two different maps"
    );

    // numerator: bytes the heap gives up to hold a deep copy of the COMPATIBILITY map, which is
    // what that map itself holds. denominator: the 2,000 control-state keys it mirrors. This is
    // the cost of the SECOND copy -- the price of going early, before the stamp has moved.
    let mirror_bytes = compatibility_copy_heap_bytes(&shard);
    let bytes_per_mirrored_key = mirror_bytes as f64 / mirrored_keys as f64;

    println!(
        "=== the compatibility copy, on a SEEDED map ===\n  \
         {mirror_bytes} B over {mirrored_keys} mirrored control-state keys at {KEY_WIDTH} \
         characters = {bytes_per_mirrored_key:.1} B per mirrored key"
    );

    assert!(
        mirror_bytes > 0,
        "the compatibility copy measured as costing nothing over {mirrored_keys} keys, which is a \
         broken measurement and not a free mirror"
    );
    // A LOOSE CEILING, and loose on purpose. The point is to catch the mirror costing an order
    // more than the slot and key it holds -- 32 bytes of slot plus a 16-byte key handle plus its
    // characters and a table slot -- not to pin an exact figure that would make this test a
    // restatement of the allocator.
    assert!(
        bytes_per_mirrored_key < 400.0,
        "the compatibility copy costs {bytes_per_mirrored_key:.1} B per mirrored key, which is \
         far more than the 32-byte slot and {KEY_WIDTH}-character key it mirrors. The duplication \
         was accepted as a one-off cost of migrating before the stamp moved; at this price it is \
         not one"
    );
}

/// Bytes the heap gives up to hold a deep copy of the compatibility map alone.
///
/// Cloning the whole `ShardState` would price every other field too, so this clones just the one
/// map -- reached through the test-only accessor, since the field itself is private.
#[cfg(feature = "alloc-probe")]
fn compatibility_copy_heap_bytes(shard: &ShardState) -> u64 {
    let copy_source = shard.clone_legacy_control_state_for_test();
    let probe = crate::alloc_probe::Probe::start();
    let copy = copy_source.clone();
    std::hint::black_box(&copy);
    let counts = probe.stop();
    drop(copy);
    counts.alloc_bytes
}
