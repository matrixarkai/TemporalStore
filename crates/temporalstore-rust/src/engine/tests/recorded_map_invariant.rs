// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// CAN A MODEL-MAP MUTATION HAPPEN WITHOUT A DURABLE RECORD?
//
// One suite, because there is now one type. `recorded_hash_container_invariant` and
// `recorded_set_container_invariant` are gone with the two containers they guarded, and what they
// each asserted is asserted here once, parameterised over the kinds.
//
// Two mechanisms, as before:
//
//   * THE COMPILER holds the OUTSIDE. `RecordedMap::entries` is private to its module, so no writer
//     elsewhere can obtain `&mut` to an inner map -- for ANY kind, which is one of the things
//     collapsing the two types bought. This tree carries no compile-fail harness (no `trybuild`, no
//     `.stderr` fixtures), so that negative is DOCUMENTED rather than executed.
//   * THIS MODULE holds the INSIDE: a new `pub(super)` accessor added inside that module is the one
//     way the invariant can be lost, and the compiler cannot object to it.
// =================================================================================================

#![allow(clippy::all)]
use super::*;

use crate::engine::recorded_map::{
    drop_object, install_element, install_element_staged_only, remove_element, HashKind, ListKind,
    RecordedMap, RepacksAfterDecode, ReplaysInstallsUnrecorded, SetKind, ZSetKind,
};
use crate::engine::storage_bucket_internals::mark_bucket_index_object_deleted_filed;

const SOURCE: &str = include_str!("../recorded_map.rs");

/// The part of the module that exists in a SHIPPED binary.
///
/// The hatch matcher runs over this rather than the whole file, and that distinction was driven: a
/// `#[cfg(test)]` fixture returning `Option<&mut ..>` made the first run of the set container's
/// version of this test fail. A fixture cannot be an escape hatch in a binary it is not compiled
/// into -- but it IS a `&mut` accessor, so the split has to be made rather than the needle dropped.
fn shipped(source: &str) -> &str {
    match source.find("\n#[cfg(test)]") {
        Some(at) => &source[..at],
        None => source,
    }
}

const PLANTED_HATCH: &str = "\
pub(super) fn entries_mut(&mut self) -> &mut HashMap<String, K::Elements> {
    &mut self.entries
}
";

const PLANTED_HATCH_WRAPPED: &str = "\
pub(super) fn entries_mut(
    &mut self,
) -> &mut HashMap<String, K::Elements> {
    &mut self.entries
}
";

const PLANTED_PUBLIC_FIELD: &str = "\
pub(super) struct RecordedMap<K: RecordedKind> {
    pub(super) entries: HashMap<String, K::Elements>,
}
";

/// THE MATCHER, anchored on the arrow so a `rustfmt` line break cannot hide a return type.
fn mutable_borrows_of_the_inner_map(source: &str) -> Vec<String> {
    let mut found = Vec::new();
    for needle in [
        "-> &mut HashMap",
        "-> &mut std::collections::HashMap",
        "-> &'a mut HashMap",
        "-> Option<&mut HashMap>",
        "-> &mut K::Elements",
        "-> Option<&mut K::Elements>",
    ] {
        if source.contains(needle) {
            found.push(needle.to_string());
        }
    }
    found
}

fn visible_declarations_of_the_field(source: &str) -> Vec<String> {
    source
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with("pub")
                && !line.contains("fn ")
                && line.contains("entries: HashMap<String, K::Elements>")
                && line.ends_with(",")
        })
        .map(str::to_string)
        .collect()
}

fn exposed_surface(source: &str) -> Vec<String> {
    shipped(source)
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with("pub(super) fn ")
                || line.starts_with("pub(super) struct ")
                || line.starts_with("pub(super) trait ")
        })
        .map(|line| {
            let after = line
                .trim_start_matches("pub(super) ")
                .trim_start_matches("fn ")
                .trim_start_matches("struct ")
                .trim_start_matches("trait ");
            let end = after
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .unwrap_or(after.len());
            after[..end].to_string()
        })
        .collect()
}

/// THE SURFACE IS THE INVARIANT, SO THE SURFACE IS PINNED BY NAME -- ONCE, FOR EVERY KIND.
#[test]
fn the_recorded_map_exposes_no_way_to_mutate_any_kind_without_a_record() {
    // CONTROLS on the matchers.
    assert!(
        !mutable_borrows_of_the_inner_map(PLANTED_HATCH).is_empty(),
        "the matcher cannot see a one-line hatch, so its silence on the real module means nothing"
    );
    assert!(
        !mutable_borrows_of_the_inner_map(PLANTED_HATCH_WRAPPED).is_empty(),
        "the matcher cannot see a wrapped hatch, so `rustfmt` alone would blind it"
    );
    assert!(
        !visible_declarations_of_the_field(PLANTED_PUBLIC_FIELD).is_empty(),
        "the matcher cannot see the field made visible, which needs no accessor at all"
    );
    assert!(
        SOURCE.contains("pub(super) fn install_element"),
        "the source read is not `recorded_map.rs`: it has no `install_element`"
    );
    assert!(
        SOURCE.len() > 10_000,
        "the source read is {} bytes, too small to be the module",
        SOURCE.len()
    );
    // CONTROL ON THE SPLIT: the test half must contain a `&mut` accessor, or `shipped()` trims
    // nothing and the subject assertion is over the whole file by accident.
    assert!(
        !mutable_borrows_of_the_inner_map(SOURCE).is_empty(),
        "the module has no `&mut` accessor anywhere, so the shipped/test split is not exercised"
    );

    // THE SUBJECT, over the shipped half only.
    assert_eq!(
        mutable_borrows_of_the_inner_map(shipped(SOURCE)),
        Vec::<String>::new(),
        "`recorded_map` hands out a mutable borrow of an inner model map. That is the escape hatch \
         this type exists to remove -- and because there is now ONE type, one hatch would open \
         every kind at once."
    );
    assert_eq!(
        visible_declarations_of_the_field(shipped(SOURCE)),
        Vec::<String>::new(),
        "`RecordedMap::entries` has been given a visibility. The field being private to its module \
         IS the invariant."
    );

    // THE MUTATORS ARE PRIVATE, which is the half the one-call surface rests on.
    for private in [
        "\n    fn install(&mut self, recorded: RecordedElement<K>)",
        "\n    fn remove_element(&mut self, recorded: RecordedElementRemoval<K>)",
        "\n    fn remove_object(&mut self, recorded: RecordedObjectRemoval<K>)",
    ] {
        assert!(
            SOURCE.contains(private),
            "a recorded mutator is no longer a PRIVATE method: {private:?}. The one-call surface \
             only narrows enforcement if the mutators behind it are unreachable from outside."
        );
    }

    let expected: Vec<&str> = vec![
        // The one type, and what distinguishes a kind.
        "RecordedKind",
        "ReplaysInstallsUnrecorded",
        "RepacksAfterDecode",
        "RecordedMap",
        // The proofs. Generic, each carrying the identity of what was recorded.
        "RecordedElement",
        "RecordedElementRemoval",
        "RecordedObjectRemoval",
        // THE OPERATIONS, written once for every kind.
        "install_element",
        "install_element_staged_only",
        "remove_element",
        "drop_object",
        // What a level-2 container must supply, declared beside the operation that needs it.
        "RemovableElementMap",
        // Reads, all borrows.
        "get",
        "contains_key",
        "len",
        "is_empty",
        "keys",
        "values",
        "iter",
        "entries",
        // The five shared exceptions.
        "from_decoded",
        "reconcile_from_durable",
        "fold_carried_elements",
        "replay_remove_element",
        "element_values_mut",
        // The two PER-KIND exceptions, each gated by its marker trait.
        "replay_install_element",
        "repack_decoded",
        // The traits the two gated exceptions need of a level-2 container.
        "RepackableElementMap",
        "ElementValuesMut",
        "iter_mut",
        "IterableMutElementMap",
        // NOTE on what this list does and does not contain: the matcher collects items carrying an
        // explicit `pub(super)`, so a TRAIT METHOD declaration -- which has no visibility modifier
        // of its own -- is not collected. `RemovableElementMap::remove_element`,
        // `RepackableElementMap::repack` and `IterableMutElementMap::iter_values_mut` are therefore
        // absent by construction, while `ElementValuesMut::iter_mut` is present because it is an
        // inherent `pub(super) fn`. That is the matcher being consistent rather than blind: a trait
        // method cannot be added without its trait appearing above, and the traits ARE pinned.
        // The kinds themselves -- ALL FOUR container kinds now, which is the whole point: adding
        // one is a `RecordedKind` impl plus markers, and it appears HERE rather than as a new type.
        "HashKind",
        "SetKind",
        "ZSetKind",
        "ListKind",
    ];
    let actual = exposed_surface(SOURCE);
    assert_eq!(
        actual, expected,
        "the surface of `recorded_map` changed. If an entry was ADDED, say in this list what it is \
         for and whether it can mutate a map without a record.\n  expected: {expected:?}\n  \
         actual:   {actual:?}"
    );
}

/// THE EXCEPTION ASYMMETRY IS DATA, AND THE COMPILER READS IT.
///
/// # WHY THIS IS NOT A COMMENT
///
/// Five paths mutate without recording. THREE are shared by every kind. TWO are not, and that is
/// the single thing two concrete containers established that one could not have:
///
///   * hash has a recovery arm that installs an element and files NO outcome record -- the replayed
///     log item IS the record for `write_context_node` -- so it needs `replay_install_element`. The
///     set replay arm re-files its block, so it does not. (This said the node "is never registered
///     in the bucket index", which is false: the index holds an entry for the block. The asymmetry
///     is about the outcome record, not the index.)
///   * set's level-2 container is a B-tree that a decode leaves half empty, so it needs
///     `repack_decoded`. `HashFieldMap` is a sorted vector whose length is its capacity, so it has
///     nothing to pack.
///
/// Flattening those into one surface, or into a sentence saying "only hash does this", would have
/// discarded exactly that. They are MARKER TRAITS instead, so the asymmetry is two `impl` lines and
/// a kind that has not declared one CANNOT CALL the method -- which the two functions below assert
/// at compile time by existing at all.
#[test]
fn the_per_kind_exceptions_are_declared_per_kind_and_the_compiler_enforces_it() {
    // COMPILE-TIME: these two functions accept only a kind that declares the marker. They are the
    // positive half, and they are checked by the crate compiling at all.
    fn only_a_kind_that_replays_unrecorded<K: ReplaysInstallsUnrecorded>() {}
    fn only_a_kind_that_repacks<K: RepacksAfterDecode>() {}
    only_a_kind_that_replays_unrecorded::<HashKind>();
    only_a_kind_that_repacks::<SetKind>();
    only_a_kind_that_repacks::<ZSetKind>();
    only_a_kind_that_repacks::<ListKind>();

    // THE NEGATIVE HALF CANNOT BE WRITTEN IN THE LANGUAGE -- Rust has no negative bounds, so
    // "`SetKind` does NOT replay unrecorded" is not expressible as a type check. It is expressible
    // as DATA about the module, which is what this reads: exactly one declaration of each marker,
    // and the kind each names.
    let replays: Vec<&str> = SOURCE
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("impl ReplaysInstallsUnrecorded for "))
        .collect();
    let repacks: Vec<&str> = SOURCE
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("impl RepacksAfterDecode for "))
        .collect();
    println!("[asymmetry] replay-install declared by: {replays:?}");
    println!("[asymmetry] decode-repack declared by:  {repacks:?}");

    assert_eq!(
        replays,
        vec!["impl ReplaysInstallsUnrecorded for HashKind {}"],
        "the set of kinds with an UNRECORDED replay install changed. A new one means a recovery arm \
         that installs an element and files nothing -- the shape hash's census found in \
         `write_context_node` -- and it has to arrive with the reason written down, not as an \
         `impl` line."
    );
    assert_eq!(
        repacks,
        vec![
            "impl RepacksAfterDecode for SetKind {}",
            "impl RepacksAfterDecode for ZSetKind {}",
            "impl RepacksAfterDecode for ListKind {}",
        ],
        "the set of kinds that repack after a decode changed. THREE of the four declare it, and the \
         one that does not is the point: `HashFieldMap` is a sorted vector whose length is its \
         capacity, so it has nothing to pack, while `sets`, `zsets` and `lists` are all B-trees a \
         decode fills by ascending insertion. Adding a B-tree kind here is routine; adding \
         `HashKind` would repack a vector on every load for no benefit, and removing one would \
         leave that kind's trees half empty after every decode."
    );

    // AND THE TWO SETS ARE DISJOINT, which is the asymmetry itself rather than two counts that
    // happen to be one.
    // THE OTHER THREE KINDS MUST NOT CLAIM THE UNRECORDED REPLAY INSTALL. Each of their replay
    // arms re-files its block through the bucket index before installing -- read from the arms
    // rather than assumed -- so hash remains the only kind whose recovery installs and files
    // nothing. `SetKind` is named separately because `ZSetKind` contains it as a substring.
    assert!(
        !replays.iter().any(|line| line.contains("for SetKind")),
        "`SetKind` now declares the unrecorded replay install. Its replay arm re-files its block, \
         so if that changed the arm changed."
    );
    for kind in ["ZSetKind", "ListKind"] {
        assert!(
            !replays.iter().any(|line| line.contains(kind)),
            "`{kind}` now declares the unrecorded replay install, but its replay arm re-files its \
             block. Either the arm changed or the marker is wrong; both need saying out loud."
        );
    }
    assert!(
        !repacks.iter().any(|line| line.contains("HashKind")),
        "`HashKind` now declares the decode repack, which would repack a sorted vector on every \
         load for no benefit."
    );
}

const TEST_SHARD: ShardId = 13;

fn shard_for_records() -> ShardState {
    let mut shard = ShardState::default();
    shard.set_routing_range(0, crate::DEFAULT_END_ROUTING_BUCKET);
    shard.set_shard_id(TEST_SHARD);
    shard
}

fn an_address() -> crate::ElementEntry {
    crate::ElementEntry::from_parts(3, 128, 64, Some(11), Some(22))
}

fn drain_records() {
    let _ = crate::engine::block_in_wal::take_outcomes();
    let _ = crate::engine::block_in_wal::take_staged();
}

fn staged() -> usize {
    crate::engine::block_in_wal::staged_outcome_count()
}

/// ONE MUTATION, ONE RECORD, COUNTED -- FOR EVERY KIND THROUGH THE ONE OPERATION.
///
/// # THIS WAS DRIVEN TO FAIL, TWICE, AND THE TWO FAILURES ARE DIFFERENT
///
/// Deleting the emission from `install_element` does not compile: the proof requires a `BlockFiled`
/// witness that only the emitter can mint, so the error is `cannot find value `filed` in this
/// scope`. Keeping the witness and undoing the record makes this assertion report
/// `staged 0 records, not 1`. Both were run and the source restored byte-identically each time.
///
/// THE WITNESS EXISTS BECAUSE OF A MISTAKE THIS CHANGE'S LINEAGE ALREADY MADE. The first version of
/// the one-call surface built the proof from the CALLER'S OWN ARGUMENTS; deleting the emission
/// compiled, and only this count noticed, at run time, while the module's doc claimed the compiler
/// would. That is why the witness lives in the emitter's module behind a private field.
#[test]
fn every_kind_records_exactly_once_through_the_one_operation() {
    drain_records();
    let mut shard = shard_for_records();

    // HASH: element is a `String` field name, which IS its component.
    let before = staged();
    install_element::<HashKind>(
        &mut shard,
        TEST_SHARD,
        "rec-hash",
        Some("field-a".to_string()),
        "field-a".to_string(),
        an_address(),
        true,
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one HASH element staged {} records, not 1",
        staged() - before
    );
    assert_eq!(shard.hashes.get("rec-hash").map(|e| e.len()), Some(1));

    // SET: element is the member BYTES, and its component is those bytes in hex -- a different
    // rendering of one identity, which is why both are passed rather than one derived here.
    let member = b"member-a".to_vec();
    let before = staged();
    install_element::<SetKind>(
        &mut shard,
        TEST_SHARD,
        "rec-set",
        Some(hex::encode(&member)),
        member.clone(),
        an_address(),
        true,
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one SET element staged {} records, not 1",
        staged() - before
    );
    assert_eq!(shard.sets.get("rec-set").map(|e| e.len()), Some(1));

    // ZSET: the element is the member bytes and the VALUE carries the score, which is why this
    // kind needed no composite key and no widened trait.
    let zmember = b"zmember".to_vec();
    let before = staged();
    install_element::<ZSetKind>(
        &mut shard,
        TEST_SHARD,
        "rec-zset",
        Some(format!("{:016x}{}", 7u64, hex::encode(&zmember))),
        zmember.clone(),
        (7u64, an_address()),
        true,
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one ZSET element staged {} records, not 1",
        staged() - before
    );
    assert_eq!(shard.zsets.get("rec-zset").map(|e| e.len()), Some(1));

    // LIST: the element is an `i64` sequence, so this kind pays no name at all -- which is why its
    // row cost does not move with an element name the way a hash field's does.
    let before = staged();
    install_element::<ListKind>(
        &mut shard,
        TEST_SHARD,
        "rec-list",
        Some(format!("{:016x}", 0u64)),
        0i64,
        an_address(),
        true,
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one LIST element staged {} records, not 1",
        staged() - before
    );
    assert_eq!(shard.lists.get("rec-list").map(|e| e.len()), Some(1));

    // THE STAGED-ONLY RECORD, which is a context node: never filed in the bucket index, so its
    // record is the staged outcome alone. One record, counted the same way.
    let before = staged();
    install_element_staged_only::<HashKind>(
        &mut shard,
        TEST_SHARD,
        "context_node",
        "rec-node",
        "meta",
        7,
        "meta".to_string(),
        an_address(),
        an_address(),
    );
    assert_eq!(
        staged() - before,
        1,
        "installing one context node staged {} records, not 1",
        staged() - before
    );

    // THE OBJECT DELETION, whose record is the bucket index's own deleted-object set rather than a
    // staged outcome -- so the staged count must NOT move, and ONE mark authorises BOTH kinds.
    let deleted_before: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|b| b.deleted_object_index.object_count())
        .sum();
    let staged_before = staged();
    let (marked, deletion) = mark_bucket_index_object_deleted_filed(&mut shard, "rec-hash");
    let deleted_after: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|b| b.deleted_object_index.object_count())
        .sum();
    assert_eq!(
        deleted_after - deleted_before,
        1,
        "deleting the object recorded {} deleted object ids, not 1",
        deleted_after - deleted_before
    );
    assert_eq!(
        staged(),
        staged_before,
        "the object deletion staged a WAL outcome; its record is the deleted-object set and a \
         staged outcome as well would have a load apply it twice"
    );
    // ONE record, TWO drops -- which is why the witness is `Clone` and why this is the one call
    // site in the engine where a token is threaded.
    let dropped_hash = drop_object::<HashKind>(&mut shard, "rec-hash", deletion.clone());
    let dropped_set = drop_object::<SetKind>(&mut shard, "rec-hash", deletion);
    assert!(
        marked | dropped_hash | dropped_set,
        "the recorded object deletion reported no change at all"
    );
    assert!(!shard.hashes.contains_key("rec-hash"));

    drain_records();
}

/// THE READS ARE STILL BORROWS, FOR EVERY KIND.
///
/// Pointer identity rather than an allocation count: the counting allocator is behind the
/// `alloc-probe` feature and a test reading its statics without it reports a table of zeros.
#[test]
fn reading_any_kind_hands_back_borrows_and_not_copies() {
    drain_records();
    let mut shard = shard_for_records();
    install_element::<HashKind>(
        &mut shard,
        TEST_SHARD,
        "borrowed",
        Some("f".to_string()),
        "f".to_string(),
        an_address(),
        true,
        an_address(),
    );
    let member = b"m".to_vec();
    install_element::<SetKind>(
        &mut shard,
        TEST_SHARD,
        "borrowed",
        Some(hex::encode(&member)),
        member,
        an_address(),
        true,
        an_address(),
    );

    // Compile-time: an owned return would fail to coerce to these.
    let hash_point: &crate::engine::hash_field_map::HashFieldMap =
        shard.hashes.get("borrowed").expect("installed");
    let hash_walk: &crate::engine::hash_field_map::HashFieldMap =
        shard.hashes.values().next().expect("one key");
    let set_point: &std::collections::BTreeMap<Vec<u8>, crate::ElementEntry> =
        shard.sets.get("borrowed").expect("installed");
    let set_walk = shard.sets.values().next().expect("one key");

    // Runtime: same resident entry, so nothing copied.
    assert!(
        std::ptr::eq(hash_point, hash_walk),
        "the hash point lookup and walk returned different addresses, so one copied the entry"
    );
    assert!(
        std::ptr::eq(set_point, set_walk),
        "the set point lookup and walk returned different addresses, so one copied the entry"
    );
    assert_eq!(shard.hashes.len(), 1);
    assert_eq!(shard.sets.len(), 1);
    drain_records();
}

/// EVERY KIND SURVIVES A ROUND TRIP THROUGH ITS OWN CODEC, AND A PLAIN MAP IS NOT A DEFAULT.
///
/// # WHY THIS TEST EXISTS
///
/// The first version of the one generic map had a single blanket `Serialize` doing
/// `self.entries.serialize(..)`. That is right for `hashes`, whose field was a plain
/// `#[serde(default)]` map. It is WRONG for `sets`, whose field carried
/// `with = "super::set_index_serde"` precisely because a member is `Vec<u8>` and a JSON object key
/// is a string -- so a set's map stopped surviving a reload, `set_index_serde` was left referenced
/// only from comments, and three reload tests failed:
/// `conformance_random_sequences_never_panic_and_survive_reload`,
/// `container_page_element_key::a_framed_page_reads_back_after_a_reload` and
/// `container_page_ordinal::a_reloaded_container_still_reads_every_element`.
///
/// Those three caught it, and they are the right kind of guard to have -- but they caught it
/// DOWNSTREAM, as a reload that lost data, and the message named neither the codec nor the kind.
/// This asserts the mechanism directly, per kind, so the next person sees "the set codec does not
/// round-trip" rather than "a framed page did not read back".
#[test]
fn every_kind_round_trip_uses_the_codec_its_field_always_used() {
    // HASH: a plain map, which is what the field always was.
    let mut hash_elements = crate::engine::hash_field_map::HashFieldMap::default();
    hash_elements.insert("field-a".to_string(), an_address());
    hash_elements.insert("field-b".to_string(), an_address());
    let mut hashes = RecordedMap::<HashKind>::default();
    hashes.insert_elements_for_test("h", hash_elements);

    let hash_json = serde_json::to_vec(&hashes).expect("the hash map serializes");
    let hash_back: RecordedMap<HashKind> =
        serde_json::from_slice(&hash_json).expect("the hash map round-trips");
    assert_eq!(hash_back, hashes, "the hash codec does not round-trip");
    assert_eq!(hash_back.get("h").map(|e| e.len()), Some(2));

    // SET: `Vec<u8>` members, which a plain map CANNOT encode as JSON object keys. This is the
    // assertion the regression failed.
    let mut set_elements = std::collections::BTreeMap::new();
    set_elements.insert(b"member-a".to_vec(), an_address());
    set_elements.insert(vec![0u8, 1, 2, 255], an_address());
    let mut sets = RecordedMap::<SetKind>::default();
    sets.insert_elements_for_test("s", set_elements);

    let set_json = serde_json::to_vec(&sets).expect(
        "the set map serializes -- a blanket plain-map impl fails HERE, because a `Vec<u8>` key \
         has no JSON object-key representation",
    );
    let set_back: RecordedMap<SetKind> =
        serde_json::from_slice(&set_json).expect("the set map round-trips");
    assert_eq!(
        set_back, sets,
        "the set codec does not round-trip. `SetKind::serialize_entries` must delegate to \
         `set_index_serde`, which encodes each set's members as a SEQUENCE of pairs; a plain map \
         encoding loses them."
    );
    assert_eq!(set_back.get("s").map(|e| e.len()), Some(2));
    // The non-UTF8 member specifically, because that is the one a string key could never carry.
    assert!(
        set_back
            .get("s")
            .expect("present")
            .contains_key(&vec![0u8, 1, 2, 255]),
        "the non-UTF8 member did not survive the round trip, which is the exact shape a plain map \
         encoding cannot represent"
    );

    // ZSET: `Vec<u8>` members again, so `zset_index_serde` -- STATED by the kind, not inherited.
    let mut z = std::collections::BTreeMap::new();
    z.insert(b"zm".to_vec(), (3u64, an_address()));
    z.insert(vec![0u8, 254, 255], (9u64, an_address()));
    let mut zsets = RecordedMap::<ZSetKind>::default();
    zsets.insert_elements_for_test("z", z);
    let z_json = serde_json::to_vec(&zsets).expect(
        "the zset map serializes -- a plain-map impl fails HERE for the same reason it does for a \
         set: a `Vec<u8>` key has no JSON object-key representation",
    );
    let z_back: RecordedMap<ZSetKind> =
        serde_json::from_slice(&z_json).expect("the zset map round-trips");
    assert_eq!(z_back, zsets, "the zset codec does not round-trip");
    assert!(
        z_back
            .get("z")
            .expect("present")
            .contains_key(&vec![0u8, 254, 255]),
        "the non-UTF8 zset member did not survive the round trip"
    );

    // LIST: an `i64` key, which a JSON object CAN carry as a string -- so this kind legitimately
    // uses the plain map, and says so rather than defaulting to it.
    let mut l = std::collections::BTreeMap::new();
    l.insert(-5i64, an_address());
    l.insert(0i64, an_address());
    l.insert(9_223_372_036_854_775_807i64, an_address());
    let mut lists = RecordedMap::<ListKind>::default();
    lists.insert_elements_for_test("l", l);
    let l_json = serde_json::to_vec(&lists).expect("the list map serializes");
    let l_back: RecordedMap<ListKind> =
        serde_json::from_slice(&l_json).expect("the list map round-trips");
    assert_eq!(l_back, lists, "the list codec does not round-trip");
    assert!(
        l_back
            .get("l")
            .expect("present")
            .contains_key(&9_223_372_036_854_775_807i64),
        "the extreme sequence did not survive the round trip, which is the value a narrower key \
         type would have lost"
    );

    // CONTROL: the comparison detects a planted difference, so agreement above is not vacuous.
    let mut planted = RecordedMap::<SetKind>::default();
    planted.insert_elements_for_test(
        "s",
        std::collections::BTreeMap::from([(b"member-a".to_vec(), an_address())]),
    );
    assert_ne!(
        planted, sets,
        "the equality used above cannot tell two different maps apart, so it proves nothing"
    );
}

/// #2087 IS CLOSED BY CONSTRUCTION: READING A LIST'S NEXT SEQUENCE CANNOT CREATE THE LIST.
///
/// # THE DEFECT
///
/// `ListPush` was the only container command that created its map entry BEFORE the append that
/// justifies it: it called `shard.lists.entry(key).or_default()` purely to read the next sequence
/// number. A failed append therefore left an empty `BTreeMap` under the key, and
/// `record_exists_exact` ORs each model map's `contains_key` into its answer -- so the key reported
/// EXISTS=1 and TYPE=list with no block behind it, and `CommonExpire`, which gates on the same
/// function, accepted a deadline for it.
///
/// # WHAT THIS TEST ASSERTS, AND WHAT IT DOES NOT
///
/// It asserts the two halves of the structural fix:
///
///   1. a READ of an absent key does not create it, and the sequence computation `ListPush` now
///      uses is a read;
///   2. the arm contains no `entry(` call against `lists` at all, so the old shape is not merely
///      unused but absent.
///
/// THE TREE-LEVEL DRIVE IS A COMPILE ERROR, WHICH IS THE STRONGEST FORM THIS COULD TAKE. Planting
/// the literal old shape -- `shard.lists.entry(key.clone()).or_default()` -- does not fail this
/// test, it fails the BUILD:
///
/// ```text
/// error[E0599]: no method named `entry` found for struct `RecordedMap<ListKind>`
/// ```
///
/// So the second assertion below is belt-and-braces: it guards the residual risk that someone adds
/// an `entry`-like accessor to the container later, at which point the arm could reacquire the shape
/// without the compiler objecting. Its own matcher is driven against a planted string instead, since
/// the tree cannot hold the defect for it to find.
///
/// IT DOES NOT SIMULATE A FAILED APPEND, and that is deliberate rather than an omission. Forcing
/// `append_value_of_object` to fail needs a block store that cannot write, and this tree has no
/// helper for that -- a permissions fixture on a shared machine can leave state behind, which is a
/// worse trade than the narrower assertion. The reason the narrower one suffices is that the
/// container removes the *expressibility*: `entries` is private, so the only way a key can come
/// into being is `install_element`, and that is called only inside `if let Ok(address) = append…`.
/// A failed append has no path to the map left to take.
#[test]
fn reading_a_lists_next_sequence_cannot_create_the_list() {
    let mut shard = shard_for_records();

    // 1. THE READ DOES NOT CREATE. This is the exact shape `ListPush` now uses.
    assert!(shard.lists.get("never-pushed").is_none());
    let seq = match shard.lists.get("never-pushed") {
        Some(list) => list.keys().next().copied().map_or(0, |first| first - 1),
        None => 0,
    };
    assert_eq!(seq, 0, "the empty-list sequence is still 0");
    assert!(
        !shard.lists.contains_key("never-pushed"),
        "reading the next sequence CREATED the key. `record_exists_exact` ORs this very \
         `contains_key` into its answer, so that is an EXISTS=1 for a list with no block behind it \
         -- which is #2087."
    );
    assert_eq!(shard.lists.len(), 0, "the map gained an entry from a read");

    // 2. AND THE OLD SHAPE IS ABSENT FROM THE ARM, not merely unused. The matcher is scoped to the
    //    `ListPush` arm rather than the file, because `entry(` against other maps is legitimate.
    let execute = include_str!("../execute_on_shard.rs");
    let at = execute
        .find("Command::ListPush { key, member, left } => {")
        .expect("the ListPush arm is findable by name");
    let rest = &execute[at..];
    let arm = match rest.find("\n        Command::") {
        Some(end) => &rest[..end],
        None => rest,
    };
    assert!(
        arm.len() > 400,
        "the ListPush arm parsed to {} characters, which is not its body",
        arm.len()
    );
    // CONTROL: the slice really is that arm.
    assert!(
        arm.contains("install_element::<super::recorded_map::ListKind>("),
        "the slice taken is not the ListPush arm -- it does not install a list element"
    );
    // COMMENT LINES COME OUT FIRST, because the note explaining the fix QUOTES the shape it
    // removed -- `entry().or_default()` -- and the first run of this test fired on that very
    // sentence. A guard that fires on its own documentation has to be relaxed to survive, and a
    // relaxed guard is not one; stripping comments keeps both needles exact.
    let arm_code: String = arm
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("//"))
        .collect::<Vec<_>>()
        .join("
");
    // CONTROL ON THE MATCHER ITSELF, against a planted string -- because the tree cannot hold the
    // defect any more, so the matcher has nothing real to find and its silence would otherwise be
    // unearned. These are the two shapes it exists to catch if an `entry`-like accessor ever
    // returns to the container.
    let planted = "let list = shard.lists.entry(key.clone()).or_default();";
    assert!(
        planted.contains(".lists.entry(") && planted.contains("or_default()"),
        "the needles this test asserts the ABSENCE of do not match the shape they were written          for, so their absence from the arm proves nothing"
    );

    // CONTROL on the strip: the comment WAS there, so the filter is doing work rather than
    // matching nothing.
    assert!(
        arm.contains("entry().or_default()") && !arm_code.contains("entry().or_default()"),
        "the comment strip removed nothing, so these assertions are not being tested against the          shape they are written for"
    );
    assert!(
        !arm_code.contains(".lists.entry("),
        "`ListPush` reaches `shard.lists` with `entry(` again. That is how #2087 arose: the entry \
         is created before the append that justifies it, and a failed append leaves a phantom list."
    );
    assert!(
        !arm_code.contains("or_default()"),
        "`ListPush` has an `or_default()` again, which is the other half of the shape that created \
         an entry a failed append then left behind."
    );
}
