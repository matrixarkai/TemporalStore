// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHERE AN OBJECT KEY'S CHARACTERS ARE DURABLY HELD, ASKED ONE MODEL KIND AT A TIME.
//!
//! THE QUESTION THE THREE PREVIOUS REFUSALS DID NOT ASK.
//! [`super::the_entry_cannot_name_its_object_by_slot`] refuses a bucket-relative ordinal on Ground
//! 2 -- the entry is the only durable home of the key's characters -- and
//! [`super::an_assigned_durable_id_defeats_ground_one_only`] prices an assigned id and finds it
//! orthogonal to that ground. BOTH WERE ARGUED ABOUT THE PAGE ENTRY, AND THE ENTRY HAS SINCE LOST
//! THE FIELD: `BlockIndex` is `model_id`, `address`, `dirty`, `deleted` and nothing else,
//! const-asserted at 24 bytes. So the refusals' subject is gone and their ground has MOVED one
//! structure outwards, to `ObjectBlockLookup` -- which is keyed by `Arc<str>` and from which
//! `impl Serialize for CoreIndex` composes every page's written key through
//! `PageKeysByPosition::from_lookup`.
//!
//! WHICH MAKES THE OPEN QUESTION NARROWER AND ANSWERABLE: if the key's characters are ALREADY held
//! durably, per kind, by a map that exists for its own reasons, then re-addressing the lookup adds
//! no second index -- it is a different addressing of one that is already there. This module
//! answers that, per kind, by measurement, and the answer is NOT uniform: it is yes for thirteen
//! of the fifteen live kinds and no for one, with one of the thirteen qualified by a hole this
//! module constructs rather than caveats.
//!
//! # WHAT "RECOVERABLE" HAS TO MEAN HERE, AND WHY EACH TERM IS IN IT
//!
//! Three terms, and dropping any one of them makes the answer yes for every kind and worthless:
//!
//!   * DURABLE -- it survives the index being written and read back. A `#[serde(skip)]` or
//!     `skip_serializing` map is resident state, not a durable home, and the window between dumps
//!     is exactly the lifetime `an_assigned_durable_id_defeats_ground_one_only`'s Claim 3 warns
//!     about for anything whose only home is a log item.
//!   * NOT THE LOOKUP -- the structure being re-addressed cannot be its own source. Asserted
//!     rather than assumed: [`the_lookup_is_empty_after_a_round_trip_so_it_cannot_be_the_source`]
//!     measures that `object_block_lookup` comes back EMPTY, which is what makes every other arm
//!     here a statement about some OTHER structure.
//!   * WITHOUT A PAGE READ -- the measurement is taken on the decode alone. `ShardState`'s serde
//!     impls are the whole apparatus; there is no block store in this module, so a recovery that
//!     needed one could not register as a success. That matters because the kinds that refuse are
//!     precisely the ones whose maps `reconcile_secondary_views_from_bucket_index` restores by
//!     READING BLOCKS, which is a page read and is also downstream of the index's own page keys.
//!
//! # THE ANSWER, PER KIND
//!
//! ```text
//!     kind                  durable structure holding the key's characters   serde      verdict
//!     string                ShardState::strings                             written    YES
//!     hash                  ShardState::hashes                              default    YES
//!     set                   ShardState::sets                                default    YES
//!     zset                  ShardState::zsets                               default    YES
//!     list                  ShardState::lists                               default    YES
//!     feature               ShardState::features                            written    YES
//!     control_state         ShardState::control_state_blocks                default    YES
//!     context_node          ShardState::context_nodes                       default    YES
//!     context_audit         ShardState::context_audits                      default    YES
//!     context_child         ShardState::context_children                    default    YES
//!     context_summary       ShardState::context_summaries                   default    YES
//!     context_compression   ShardState::context_compressions                default    YES
//!     context_entity        ShardState::context_entities, BY COMPOSITION    default    YES
//!     context_event         ShardState::context_event_timeline, NOT its own SKIP_SER   QUALIFIED
//!     context_index         nothing                                         SKIP_SER   NO
//! ```
//!
//! TWELVE PLAIN YES, ONE BY COMPOSITION, ONE QUALIFIED AND ONE FLAT NO. The twelve are the kinds
//! whose own map is keyed by the characters and is written; the walk that derives the index from
//! those maps (`visit_model_live_blocks`) is what pins "the map's key IS the lookup's object key"
//! for each one, and the arms below measure the round trip rather than reading the attribute.
//!
//! CONTEXT_ENTITY IS A YES BY COMPOSITION AND THAT IS NOT A WEAKER ANSWER. Its map is keyed by the
//! node's COLLECTION key and its inner `u64` is the entity hash, while the lookup's object key is
//! `format!("{collection_key}:{entity_hash}")` -- composed in the walk's own entity arm. Both
//! terms are durable and the composition is the walk's, so the characters are recoverable exactly;
//! the field's own doc says the load path "splits back into (collection key, entity hash)", which
//! is this composition read in the other direction.
//!
//! CONTEXT_EVENT IS THE INTERESTING ONE AND ITS ANSWER IS CONDITIONAL. `context_events` is
//! `#[serde(default, skip_serializing)]`, so the primary map is NOT a durable home. But
//! `context_event_timeline` is `#[serde(default)]` -- written -- and is keyed by the SAME object
//! key: `execute_on_shard` files `context_event_timeline.entry(object_key.clone())` in the same
//! block that inserts the event, and `reconcile_secondary_views_from_bucket_index`'s own retain
//! reads `shard.context_events.get(object_key)` with the timeline's key, which is what says the
//! two outer key domains are one. So the characters ARE durable for this kind -- in the time index
//! rather than in the primary map.
//!
//! AND IT IS A QUALIFIED YES RATHER THAN A YES, FOR A REASON THIS MODULE MEASURES RATHER THAN
//! WAVES AT. The timeline's key set is a SUBSET of the event map's by construction -- the
//! reconcile's `retain` drops a timeline whose events are gone, and `lifecycle`'s fold removes the
//! timeline entry when its last slot goes -- so an event object whose timeline went empty while
//! its events did not would be a key the timeline cannot name.
//! [`an_event_object_with_no_timeline_entry_is_a_key_the_time_index_cannot_name`] CONSTRUCTS that
//! state and shows the recovery failing, so the qualification is a measured hole and not a caveat.
//! Whether production can reach it is NOT settled here and must not be inferred from this module.
//!
//! CONTEXT_INDEX IS A FLAT NO AND HAS NO SIBLING TO RESCUE IT. `context_indexes` is
//! `skip_serializing` and there is no second durable map keyed by its object key -- the shape that
//! saves `context_event` simply does not exist for it. `released_model_kind_is_addressable`'s own
//! first term says the same thing from the other side: these two maps "are rebuilt FROM the bucket
//! index on load", so for them the index's stored page key is the only durable home of the
//! characters, and an index that stopped storing characters would leave them nothing. That is
//! Ground 2, intact, for one kind out of fifteen.
//!
//! # THE CONTROLS, BECAUSE A KEY-PRESENCE SCAN IS THE EASIEST THING TO GET VACUOUSLY RIGHT
//!
//! Every arm carries both:
//!
//!   * POSITIVE -- a key the fixture certainly wrote must be found, or the scan proves nothing
//!     about the store. This is the control that caught the recorded detector failure in the slot
//!     module, where a raw-byte scan found a key in none of fourteen files because the index is
//!     COMPRESSED and read as "the characters are not durable at all".
//!   * NEGATIVE -- a key of the same SHAPE that was never written must be found nowhere, or the
//!     matcher matches anything.
//!
//! The negative control is a sibling spelling of the positive one differing only in its LAST
//! character, so a prefix match, a substring match, or a hash of the first n bytes fails it rather
//! than passing both arms.
//!
//! NO PRODUCTION CODE CHANGES HERE AND NO STORED SHAPE MOVES, so no `SHARD_INDEX_FORMAT_VERSION`
//! is owed. This module is the enumeration the leg's fourth attempt is gated on.
//!
//! rust-internal: drives this crate's own index codec, no external surface

#![allow(clippy::all)]

use super::*;
use std::collections::BTreeMap;

use crate::block_store::ElementEntry;
use crate::engine::state::ShardState;
use crate::engine::{decode_index_bytes, encode_index_bytes};

/// One address, shaped like any other. Nothing in this module reads its fields: it exists so a map
/// entry can exist at all, because an empty map answers every question here with "absent".
fn address() -> ElementEntry {
    ElementEntry::from_parts(1, 64, 32, Some(9), Some(11))
}

/// THE KEY THE FIXTURE WRITES, and the sibling spelling it never writes.
const WRITTEN_SUFFIX: &str = "written";
const NEVER_WRITTEN_SUFFIX: &str = "writteN";

fn key_for(kind: &str, suffix: &str) -> String {
    format!("tenant/1/{kind}/object/{suffix}")
}

const ENTITY_HASH: u64 = 0x5ead_beef;
const TIMELINE_KEY: u64 = 1_700_000_000_000;
const EVENT_ID_HASH: u64 = 0x1234_5678;

fn entity_collection_key(suffix: &str) -> String {
    format!("ctx:entity:tenant1:{suffix}")
}

/// A shard carrying ONE object of every live kind, each under that kind's own key spelling.
///
/// Built through the maps rather than through the engine on purpose: the question is what the
/// DECODE hands back, and an engine fixture would also populate the bucket index and the lookup,
/// which is the structure under suspicion. A shard with model maps and an empty bucket index is
/// exactly the state in which "the characters are somewhere else" is a checkable claim.
fn shard_with_one_object_of_every_kind(suffix: &str) -> ShardState {
    let mut shard = ShardState::default();

    shard
        .strings
        .insert(key_for("string", suffix).as_str().into(), address());
    shard
        .hashes
        .insert_element_for_test(&key_for("hash", suffix), "field-a".to_string(), address());
    shard
        .sets
        .insert_element_for_test(&key_for("set", suffix), b"member-a".to_vec(), address());
    shard.zsets.insert_element_for_test(
        &key_for("zset", suffix),
        b"member-a".to_vec(),
        (7, address()),
    );
    shard
        .lists
        .insert_element_for_test(&key_for("list", suffix), 1i64, address());

    let mut series = BTreeMap::new();
    series.insert(1_000u64, address());
    shard
        .features
        .insert(key_for("feature", suffix).into(), series.clone().into());
    shard.insert_control_state_block(
        key_for("control_state", suffix).as_str().into(),
        {
            // Through `set_address`, like production -- see the note on that method.
            let mut record = crate::engine::object_record::ObjectRecord::default();
            record.set_address(&address());
            record.into()
        },
    );
    shard
        .context_nodes
        .insert(key_for("context_node", suffix).as_str().into(), address());
    shard
        .context_audits
        .insert(key_for("context_audit", suffix).into(), series.clone());
    shard
        .context_children
        .insert(key_for("context_child", suffix).into(), series.clone());
    shard
        .context_summaries
        .insert(key_for("context_summary", suffix).into(), series.clone());
    shard
        .context_compressions
        .insert(key_for("context_compression", suffix).into(), series.clone());

    // THE ENTITY ARM IS KEYED BY THE COLLECTION AND NOT BY THE COMPOSED KEY, which is the whole
    // reason it is a separate verdict below. The inner u64 is the ENTITY HASH, not a timestamp.
    let mut entities = BTreeMap::new();
    entities.insert(ENTITY_HASH, address());
    shard
        .context_entities
        .insert(entity_collection_key(suffix).into(), entities);

    // THE TWO THAT ARE NOT WRITTEN. Populated anyway, so the arms below measure a map that HAD the
    // key and lost it rather than a map that never held one -- which are different claims and only
    // one of them is about durability.
    shard.set_context_event_series(&key_for("context_event", suffix), series.clone());
    shard
        .context_indexes
        .insert(key_for("context_index", suffix).into(), series.clone());
    // The time index, keyed by the same object key the event map is keyed by.
    let mut timeline = BTreeMap::new();
    timeline.insert(TIMELINE_KEY, EVENT_ID_HASH);
    shard
        .context_event_timeline
        .insert(key_for("context_event", suffix).into(), timeline);

    shard
}

/// The round trip the whole module is taken across: the real encoder and the real decoder, the
/// same pair `persistence` writes and reads.
fn round_trip(shard: &ShardState) -> ShardState {
    let bytes = encode_index_bytes(shard);
    decode_index_bytes(&bytes).expect("a shard this engine wrote should decode")
}

// =================================================================================================
// THE PRECONDITION: THE STRUCTURE UNDER SUSPICION IS NOT A SOURCE
// =================================================================================================

/// THE LOOKUP COMES BACK EMPTY, SO NOTHING BELOW IS MEASURING IT.
///
/// `object_block_lookup` is `skip_serializing` and `CoreIndex`'s encoder writes ONE field. If this
/// arm failed -- if a decode handed back a populated lookup -- then every "the characters are
/// durable" result below could be the lookup answering about itself, which is the shape this
/// campaign has shipped before: a detector replaced by a comparison of a value against itself.
///
/// THE POSITIVE CONTROL IS THAT THE SHARD WAS NOT EMPTY. A round trip of nothing also produces an
/// empty lookup, so the model maps are asserted populated on BOTH sides before the emptiness is
/// read as meaning anything.
#[test]
fn the_lookup_is_empty_after_a_round_trip_so_it_cannot_be_the_source() {
    let shard = shard_with_one_object_of_every_kind(WRITTEN_SUFFIX);
    assert!(
        !shard.strings.is_empty(),
        "fixture must be populated or the round trip is of nothing"
    );

    let reloaded = round_trip(&shard);

    assert!(
        reloaded.bucket_index.object_block_lookup.is_empty(),
        "the lookup is skip_serializing, so a decode must not hand one back -- if it does, every \
         durability result in this module may be the lookup answering about itself"
    );
    assert!(
        !reloaded.strings.is_empty(),
        "positive control: the round trip must carry SOMETHING, or the empty lookup above is \
         simply an empty shard"
    );
}

// =================================================================================================
// THE CENSUS: ONE VERDICT PER KIND, MEASURED ACROSS THE REAL CODEC
// =================================================================================================

/// Whether the shard's own map for this kind names this key.
///
/// ONE ARM PER KIND AND NO DEFAULT ARM THAT ANSWERS. A kind added to `model_kind_registry` and not
/// to this match PANICS with the instruction, which is the only way this census cannot go stale
/// beside the registry it is a census OF.
fn kind_map_holds(shard: &ShardState, kind: &str, suffix: &str) -> bool {
    let key = key_for(kind, suffix);
    match kind {
        "string" => shard.strings.contains_key(key.as_str()),
        "hash" => shard.hashes.contains_key(&key),
        "set" => shard.sets.contains_key(&key),
        "zset" => shard.zsets.contains_key(&key),
        "list" => shard.lists.contains_key(&key),
        "feature" => shard.features.contains_key(&key[..]),
        "control_state" => shard.control_state_blocks().contains_key(key.as_str()),
        "context_node" => shard.context_nodes.contains_key(key.as_str()),
        "context_audit" => shard.context_audits.contains_key(&key[..]),
        "context_child" => shard.context_children.contains_key(&key[..]),
        "context_summary" => shard.context_summaries.contains_key(&key[..]),
        "context_compression" => shard.context_compressions.contains_key(&key[..]),
        "context_event" => shard.context_events().contains_key(key.as_str()),
        "context_index" => shard.context_indexes.contains_key(&key[..]),
        other => panic!(
            "no census arm for model kind {other:?}. Every kind `model_kind_registry` declares \
             live needs a verdict here, or this census silently stops covering one."
        ),
    }
}

/// THE KINDS WHOSE OWN MAP IS THE DURABLE HOME.
///
/// `context_entity` is absent because its map is not keyed by the lookup's object key; it has its
/// own arm. `context_event` and `context_index` are absent because they are the refusals.
const KINDS_WHOSE_OWN_MAP_IS_DURABLE: &[&str] = &[
    "string",
    "hash",
    "set",
    "zset",
    "list",
    "feature",
    "control_state",
    "context_node",
    "context_audit",
    "context_child",
    "context_summary",
    "context_compression",
];

/// TWELVE KINDS RECOVER THEIR KEY FROM THEIR OWN DURABLE MAP, MEASURED ONE AT A TIME.
///
/// Each kind is asserted on BOTH controls, so a kind that passes passes because its map carried
/// the characters and not because `contains_key` answers true for everything.
#[test]
fn each_durable_kind_hands_back_its_object_keys_characters() {
    let written = shard_with_one_object_of_every_kind(WRITTEN_SUFFIX);
    let reloaded = round_trip(&written);

    for kind in KINDS_WHOSE_OWN_MAP_IS_DURABLE {
        assert!(
            kind_map_holds(&written, kind, WRITTEN_SUFFIX),
            "fixture failed to write kind {kind}: the round trip below would measure nothing"
        );
        assert!(
            kind_map_holds(&reloaded, kind, WRITTEN_SUFFIX),
            "kind {kind} lost its object key across an index round trip, so its characters are \
             NOT durably held outside the lookup and this kind refuses"
        );
        assert!(
            !kind_map_holds(&reloaded, kind, NEVER_WRITTEN_SUFFIX),
            "negative control for kind {kind}: a sibling spelling that was never written was \
             found, so the matcher answers for keys the store does not hold"
        );
    }

    assert_eq!(
        KINDS_WHOSE_OWN_MAP_IS_DURABLE.len(),
        12,
        "the count is part of the claim: twelve kinds recover from their own map, one recovers by \
         composition, one is qualified and one refuses"
    );
}

/// THE COMPOSED KEY IS RECOVERABLE EXACTLY, AND IT IS COMPARED AGAINST THE WALK'S OWN SPELLING.
///
/// `context_entity`'s map is keyed by the collection key with the entity hash as the inner `u64`,
/// and the object key the lookup is keyed by is the two joined with a colon --
/// `visit_model_live_blocks`'s entity arm composes exactly that. So the recovery is a composition
/// of two durable terms, which is a different route to the same answer and not a weaker one.
#[test]
fn a_composed_entity_key_is_recoverable_from_its_two_durable_terms() {
    let written = shard_with_one_object_of_every_kind(WRITTEN_SUFFIX);
    assert!(
        !written.context_entities.is_empty(),
        "fixture must hold an entity or this arm measures nothing"
    );
    let reloaded = round_trip(&written);

    let mut recomposed: Vec<String> = Vec::new();
    for (collection_key, series) in &reloaded.context_entities {
        for (entity_hash, _address) in series.iter() {
            recomposed.push(format!("{collection_key}:{entity_hash}"));
        }
    }

    let expected = format!("{}:{}", entity_collection_key(WRITTEN_SUFFIX), ENTITY_HASH);
    assert!(
        recomposed.contains(&expected),
        "the composed entity object key was not recoverable from the two durable terms; \
         recomposed {recomposed:?}"
    );
    assert!(
        !recomposed.contains(&format!(
            "{}:{}",
            entity_collection_key(NEVER_WRITTEN_SUFFIX),
            ENTITY_HASH
        )),
        "negative control: a collection key that was never written was recomposed anyway"
    );
}

/// CONTEXT_EVENT'S CHARACTERS SURVIVE -- IN THE TIME INDEX AND NOT IN THE EVENT MAP.
///
/// Both halves are asserted, because either alone is misleading. The primary map coming back empty
/// is what makes this kind a candidate refusal; the timeline coming back holding the SAME key is
/// what rescues it. A test that only checked the timeline would pass just as well if the primary
/// map were durable too, and would then be saying nothing about where the characters live.
#[test]
fn a_context_event_key_survives_in_the_time_index_only() {
    let written = shard_with_one_object_of_every_kind(WRITTEN_SUFFIX);
    let key = key_for("context_event", WRITTEN_SUFFIX);
    assert!(
        written.context_events().contains_key(key.as_str()),
        "fixture must write the event map, or its emptiness after the trip is not a loss"
    );
    assert!(
        written.context_event_timeline.contains_key(key.as_str()),
        "fixture must write the timeline under the SAME key, which is the claim under test"
    );

    let reloaded = round_trip(&written);

    assert!(
        !reloaded.context_events().contains_key(key.as_str()),
        "context_events is skip_serializing: if it came back, this kind's verdict is not what \
         this module says it is"
    );
    assert!(
        reloaded.context_event_timeline.contains_key(key.as_str()),
        "the time index is the durable home of a context_event object key's characters, and it \
         did not carry them"
    );
    assert!(
        !reloaded
            .context_event_timeline
            .contains_key(key_for("context_event", NEVER_WRITTEN_SUFFIX).as_str()),
        "negative control: the time index named a key that was never written"
    );
}

/// AND THE QUALIFICATION, CONSTRUCTED RATHER THAN CAVEATED.
///
/// The timeline's key set is a subset of the event map's: the reconcile's `retain` drops a
/// timeline whose events are gone, and the fold removes a timeline entry when its last slot goes.
/// So an event object with no timeline entry is representable, and for THAT object the rescue
/// above does not apply.
///
/// This arm builds that state directly and shows the recovery failing. It is a statement about
/// what the structure permits, NOT a claim that production reaches it -- that would need the write
/// and fold paths driven, and this module does not drive them.
#[test]
fn an_event_object_with_no_timeline_entry_is_a_key_the_time_index_cannot_name() {
    let mut written = shard_with_one_object_of_every_kind(WRITTEN_SUFFIX);
    let key = key_for("context_event", WRITTEN_SUFFIX);
    // The event stays; its timeline entry goes.
    written.context_event_timeline.remove(key.as_str());
    assert!(
        written.context_events().contains_key(key.as_str()),
        "the event must still be there, or this arm is about an object that does not exist"
    );

    let reloaded = round_trip(&written);

    assert!(
        !reloaded.context_events().contains_key(key.as_str()),
        "the event map is still not durable"
    );
    assert!(
        !reloaded.context_event_timeline.contains_key(key.as_str()),
        "this is the hole: an event object whose timeline entry is absent has NO durable home for \
         its characters, and the rescue in the arm above does not cover it"
    );
}

/// CONTEXT_INDEX REFUSES, AND THE REFUSAL IS THAT NO DURABLE MAP IN THE SHARD NAMES ITS KEY.
///
/// Measured as a whole-shard statement rather than as an attribute read: after the round trip, the
/// key is searched for across EVERY map a `ShardState` carries that is keyed by an object key, by
/// the same `contains_key` the census uses, and found in none. The positive control is in the same
/// scan -- a `string` key written by the same fixture IS found by the same loop -- so a zero means
/// absent rather than meaning the scan looked nowhere.
#[test]
fn a_context_index_object_key_is_in_no_durable_structure_after_a_reload() {
    let written = shard_with_one_object_of_every_kind(WRITTEN_SUFFIX);
    let refused = key_for("context_index", WRITTEN_SUFFIX);
    assert!(
        written.context_indexes.contains_key(&refused[..]),
        "fixture must write the context_index map first"
    );
    assert!(
        !maps_naming(&written, &refused).is_empty(),
        "before the trip the key must be found by this very scan, or the scan is what is absent"
    );

    let reloaded = round_trip(&written);

    let found_in = maps_naming(&reloaded, &refused);
    assert!(
        found_in.is_empty(),
        "context_index's key was expected in no durable map and was found in {found_in:?}"
    );

    // POSITIVE CONTROL, THROUGH THE SAME SCAN ON THE SAME RELOADED SHARD.
    let present = key_for("string", WRITTEN_SUFFIX);
    let control = maps_naming(&reloaded, &present);
    assert_eq!(
        control,
        vec!["strings"],
        "the same scan must find a key the store does hold, or its silence about context_index \
         means nothing"
    );
}

/// Which of the shard's object-key-keyed maps name this exact key, by name, in declaration order.
///
/// `context_events` and `context_indexes` are deliberately IN this list even though they are
/// `skip_serializing`: the scan is run on a RELOADED shard, so including them is what makes "found
/// in none" a measurement rather than a tautology produced by leaving them out.
fn maps_naming(shard: &ShardState, key: &str) -> Vec<&'static str> {
    let mut found = Vec::new();
    if shard.strings.contains_key(key) {
        found.push("strings");
    }
    if shard.hashes.contains_key(key) {
        found.push("hashes");
    }
    if shard.sets.contains_key(key) {
        found.push("sets");
    }
    if shard.zsets.contains_key(key) {
        found.push("zsets");
    }
    if shard.lists.contains_key(key) {
        found.push("lists");
    }
    if shard.features.contains_key(key) {
        found.push("features");
    }
    if shard.control_state_blocks().contains_key(key) {
        found.push("control_state_blocks");
    }
    if shard.context_nodes.contains_key(key) {
        found.push("context_nodes");
    }
    if shard.context_audits.contains_key(key) {
        found.push("context_audits");
    }
    if shard.context_children.contains_key(key) {
        found.push("context_children");
    }
    if shard.context_summaries.contains_key(key) {
        found.push("context_summaries");
    }
    if shard.context_compressions.contains_key(key) {
        found.push("context_compressions");
    }
    if shard.context_events().contains_key(key) {
        found.push("context_events");
    }
    if shard.context_indexes.contains_key(key) {
        found.push("context_indexes");
    }
    if shard.context_event_timeline.contains_key(key) {
        found.push("context_event_timeline");
    }
    if shard.context_entities.contains_key(key) {
        found.push("context_entities");
    }
    found
}

/// THE CENSUS COVERS EVERY LIVE KIND, AND THE COMPILER IS WHAT SAYS SO.
///
/// The verdict sets must between them name every kind `model_kind_registry` declares live. A kind
/// added to the registry and to no verdict set fails here, which is the only thing standing
/// between this module and a census that silently stops covering a kind.
#[test]
fn every_live_model_kind_has_a_verdict_in_this_module() {
    use crate::engine::storage_bucket_internals::ModelKind;

    const RECOVERS_BY_COMPOSITION: &[&str] = &["context_entity"];
    const QUALIFIED: &[&str] = &["context_event"];
    const REFUSES: &[&str] = &["context_index"];

    let mut covered: Vec<&str> = Vec::new();
    covered.extend_from_slice(KINDS_WHOSE_OWN_MAP_IS_DURABLE);
    covered.extend_from_slice(RECOVERS_BY_COMPOSITION);
    covered.extend_from_slice(QUALIFIED);
    covered.extend_from_slice(REFUSES);

    let live: Vec<&str> = ModelKind::ALL.iter().map(|kind| kind.as_str()).collect();
    assert!(
        !live.is_empty(),
        "the registry must declare live kinds or this check is vacuous"
    );

    for kind in &live {
        assert!(
            covered.contains(kind),
            "live model kind {kind} has no verdict in this module's census. Add it to \
             KINDS_WHOSE_OWN_MAP_IS_DURABLE, RECOVERS_BY_COMPOSITION, QUALIFIED or REFUSES -- and \
             to `kind_map_holds` -- with the measurement that says which."
        );
    }

    assert_eq!(
        covered.len(),
        live.len(),
        "the verdict sets and the live registry must be the same size: covered {covered:?} \
         against live {live:?}"
    );

    // AND THE HEADLINE NUMBERS, SPELLED SO THEY CANNOT DRIFT FROM THE PROSE ABOVE.
    assert_eq!(live.len(), 15, "fifteen live kinds");
    assert_eq!(REFUSES.len(), 1, "one refuses outright");
    assert_eq!(QUALIFIED.len(), 1, "one is qualified by a constructed hole");
}
