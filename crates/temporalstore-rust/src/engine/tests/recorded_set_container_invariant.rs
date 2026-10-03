// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// CAN A SET MODEL-MAP MUTATION HAPPEN WITHOUT A DURABLE RECORD?
//
// The same two-mechanism answer the hash container has, held here for `sets`:
//
//   * THE COMPILER holds the OUTSIDE. `RecordedSetContainer::entries` is private to its module, so
//     no writer elsewhere can obtain `&mut` to the inner map. There is nothing for a test to assert
//     there -- a violation is a compile error, and this tree carries no compile-fail harness (no
//     `trybuild`, no `.stderr` fixtures), so the negative is DOCUMENTED rather than executed.
//   * THIS MODULE holds the INSIDE. The compiler cannot object to a new `pub(super)` accessor added
//     inside that module, which is the one way the invariant can be lost.
// =================================================================================================

#![allow(clippy::all)]
use super::*;

use crate::engine::recorded_set_container::{install_set_member, remove_set_member};
use crate::engine::storage_bucket_internals::mark_bucket_index_object_deleted_filed;

const SOURCE: &str = include_str!("../recorded_set_container.rs");

/// The part of the module that exists in a SHIPPED binary.
///
/// The hatch matcher runs over this rather than the whole file, and that distinction was driven: a
/// `#[cfg(test)]` fixture returning `Option<&mut SetMemberMap>` made the first run of this test
/// fail. A test fixture cannot be an escape hatch in a shipped binary because it is not compiled
/// into one -- but it IS a `&mut` accessor, so the split has to be made rather than the needle
/// dropped. Dropping the needle is what the hash equivalent of this matcher effectively did, and
/// it would therefore miss a SHIPPED `-> Option<&mut HashFieldMap>`, which is worth closing
/// when that module is next touched.
fn shipped(source: &str) -> &str {
    match source.find("
#[cfg(test)]") {
        Some(at) => &source[..at],
        None => source,
    }
}

/// What a hatch would look like. A CONTROL on the matcher: one that cannot see this cannot see the
/// real thing either.
const PLANTED_HATCH: &str = "\
pub(super) fn entries_mut(&mut self) -> &mut HashMap<String, SetMemberMap> {
    &mut self.entries
}
";

const PLANTED_HATCH_WRAPPED: &str = "\
pub(super) fn entries_mut(
    &mut self,
) -> &mut HashMap<String, SetMemberMap> {
    &mut self.entries
}
";

const PLANTED_PUBLIC_FIELD: &str = "\
pub(super) struct RecordedSetContainer {
    pub(super) entries: HashMap<String, SetMemberMap>,
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
        "-> &mut SetMemberMap",
        "-> Option<&mut SetMemberMap>",
    ] {
        if source.contains(needle) {
            found.push(needle.to_string());
        }
    }
    found
}

/// The field given any visibility at all, anchored on the DECLARATION rather than the name -- the
/// hash equivalent of this matcher fired on a `_for_test` setter's PARAMETER before it was anchored.
fn visible_declarations_of_the_field(source: &str) -> Vec<String> {
    source
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with("pub")
                && !line.contains("fn ")
                && line.contains("entries: HashMap<String, SetMemberMap>")
                && line.ends_with(",")
        })
        .map(str::to_string)
        .collect()
}

fn exposed_surface(source: &str) -> Vec<String> {
    let shipped = match source.find("\n#[cfg(test)]") {
        Some(at) => &source[..at],
        None => source,
    };
    shipped
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("pub(super) fn ") || line.starts_with("pub(super) struct "))
        .map(|line| {
            let after = line
                .trim_start_matches("pub(super) ")
                .trim_start_matches("fn ")
                .trim_start_matches("struct ");
            let end = after
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .unwrap_or(after.len());
            after[..end].to_string()
        })
        .collect()
}

/// THE SURFACE IS THE INVARIANT, SO THE SURFACE IS PINNED BY NAME.
#[test]
fn the_set_container_exposes_no_way_to_mutate_the_map_without_a_record() {
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
    // CONTROLS that the source read is the module it thinks it is.
    assert!(
        SOURCE.contains("pub(super) fn install_set_member"),
        "the source read is not `recorded_set_container.rs`: it has no `install_set_member`"
    );
    assert!(
        SOURCE.len() > 8_000,
        "the source read is {} bytes, too small to be the module",
        SOURCE.len()
    );

    // CONTROL ON THE SPLIT: the test half must actually contain a `&mut` accessor, or `shipped()`
    // is trimming nothing and the assertion below is over the whole file by accident.
    assert!(
        !mutable_borrows_of_the_inner_map(SOURCE).is_empty(),
        "the module has no `&mut` accessor anywhere, so the shipped/test split below is not being          exercised and would pass whether it worked or not"
    );
    // AND EVERY ONE OF THEM IS A NAMED FIXTURE. A `&mut` accessor under `#[cfg(test)]` is not an
    // escape hatch in a shipped binary, but one that is NOT named `_for_test` reads like ordinary
    // surface to anyone skimming the module.
    for line in SOURCE.lines().map(str::trim) {
        if line.starts_with("pub(super) fn") && line.contains("-> Option<&mut SetMemberMap>") {
            assert!(
                line.contains("_for_test"),
                "a `&mut` accessor that is not a named test fixture: {line}"
            );
        }
    }

    // THE SUBJECT, over the shipped half only.
    assert_eq!(
        mutable_borrows_of_the_inner_map(shipped(SOURCE)),
        Vec::<String>::new(),
        "`recorded_set_container` hands out a mutable borrow of the set model map. That is the \
         escape hatch this type exists to remove."
    );
    assert_eq!(
        visible_declarations_of_the_field(shipped(SOURCE)),
        Vec::<String>::new(),
        "`RecordedSetContainer::entries` has been given a visibility. The field being private to \
         its module IS the invariant."
    );

    // AND THE MUTATORS ARE PRIVATE, which is the half the one-call surface rests on: a caller that
    // never holds a proof cannot reach the map, and these are why.
    for private in [
        "\n    fn install(&mut self, recorded: RecordedSetMember)",
        "\n    fn remove_member(&mut self, recorded: RecordedSetMemberRemoval)",
        "\n    fn remove_object(&mut self, recorded: RecordedSetObjectRemoval)",
    ] {
        assert!(
            SOURCE.contains(private),
            "a recorded mutator is no longer a PRIVATE method: {private:?}. The one-call surface \
             only narrows enforcement if the mutators behind it are unreachable from outside."
        );
    }
    for private in [
        "\nstruct RecordedSetMember {",
        "\nstruct RecordedSetMemberRemoval {",
        "\nstruct RecordedSetObjectRemoval {",
    ] {
        assert!(
            SOURCE.contains(private),
            "a proof type is no longer private to this module: {private:?}"
        );
    }

    let expected: Vec<&str> = vec![
        "RecordedSetContainer",
        // THE THREE OPERATIONS. Each records and then mutates in one call. `drop_set_object` TAKES
        // the deletion witness rather than minting it, because one object deletion authorises a
        // drop in every recorded container and minting per kind would file a second record of one
        // deletion.
        "install_set_member",
        "remove_set_member",
        "drop_set_object",
        // Reads, all borrows.
        "get",
        "contains_key",
        "len",
        "is_empty",
        "keys",
        "values",
        "iter",
        // The five exceptions, where the record is the source and not the sink.
        "reconcile_from_durable",
        "fold_carried_elements",
        "replay_remove_member",
        "repack_decoded",
        "member_addresses_mut",
        // The narrowed lend the compaction exception walks with: addresses, never membership.
        "MemberAddressesMut",
        "iter_mut",
    ];
    let actual = exposed_surface(SOURCE);
    assert_eq!(
        actual, expected,
        "the surface of `recorded_set_container` changed. If an entry was ADDED, say in this list \
         what it is for and whether it can mutate the map without a record.\n  expected: \
         {expected:?}\n  actual:   {actual:?}"
    );

    assert!(
        SOURCE.contains("entries: HashMap<String, SetMemberMap>"),
        "the inner map is no longer a bare private field, so the privacy argument may no longer be \
         the one that holds"
    );
}

const TEST_SHARD: ShardId = 11;

fn shard_for_records() -> ShardState {
    let mut shard = ShardState::default();
    shard.set_routing_range(0, crate::DEFAULT_END_ROUTING_BUCKET);
    shard.set_shard_id(TEST_SHARD);
    shard
}

fn an_address() -> crate::BlockAddress {
    crate::BlockAddress::from_parts(3, 128, 64, Some(11), Some(22))
}

fn drain_records() {
    let _ = crate::engine::block_in_wal::take_outcomes();
    let _ = crate::engine::block_in_wal::take_staged();
}

fn staged() -> usize {
    crate::engine::block_in_wal::staged_outcome_count()
}

/// ONE MUTATION, ONE RECORD, COUNTED.
///
/// # THIS TEST WAS DRIVEN TO FAIL
///
/// The emission was removed from `install_set_member`. Because the proof requires a `BlockFiled`
/// witness that only the emitter can mint, that deletion does not compile -- so the drive was done
/// in two steps and both are recorded in the body: the witness makes it a COMPILE error, and with
/// the witness threaded through a hand-made value instead the count assertion below reports 0.
#[test]
fn each_recorded_set_mutator_emits_exactly_one_record() {
    drain_records();

    // 1. INSTALLING A MEMBER. The record is the bucket-index entry, which stages one outcome.
    let mut shard = shard_for_records();
    let member = b"member-a".to_vec();
    let component = hex::encode(&member);
    let before = staged();
    install_set_member(
        &mut shard,
        TEST_SHARD,
        "recorded-key",
        component,
        member.clone(),
        an_address(),
        true,
    );
    let after = staged();
    assert_eq!(
        after - before,
        1,
        "installing one set member staged {} records, not 1. Zero means the mutation can happen \
         unrecorded, which is the defect class this type removes; more than one means a replay \
         installs the page more than once.",
        after - before
    );
    assert_eq!(
        shard.sets.get("recorded-key").map(|members| members.len()),
        Some(1),
        "the recorded member did not reach the resident map"
    );

    // 2. DELETING THE WHOLE OBJECT. The mark stages NOTHING -- its record is the bucket index's own
    //    deleted-object set -- so the count asserted is deleted object ids, and the staged count is
    //    asserted UNMOVED, because a second record would have a load apply the deletion twice.
    let deleted_before: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.deleted_object_index.object_count())
        .sum();
    let staged_before = staged();
    let (marked, deletion) = mark_bucket_index_object_deleted_filed(&mut shard, "recorded-key");
    let deleted_after: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.deleted_object_index.object_count())
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
        "the object deletion staged a WAL outcome. Its record is the bucket index's \
         deleted-object set; a staged outcome as well would have a load apply it twice."
    );
    assert!(
        marked | crate::engine::recorded_set_container::drop_set_object(
            &mut shard,
            "recorded-key",
            deletion
        ),
        "the recorded object deletion reported no change"
    );
    assert!(
        !shard.sets.contains_key("recorded-key"),
        "the set is still resident after a recorded object deletion"
    );

    drain_records();
}

/// THE READS ARE STILL BORROWS, WHICH IS WHY THEY STILL COST NOTHING.
///
/// Pointer identity rather than an allocation count: the counting allocator is behind the
/// `alloc-probe` feature and a test reading its statics without it reports a table of zeros, which
/// `every_counting_allocator_probe_is_gated_on_the_feature_that_installs_it` exists to stop.
#[test]
fn reading_the_set_container_hands_back_borrows_and_not_copies() {
    drain_records();
    let mut shard = shard_for_records();
    let member = b"borrowed".to_vec();
    install_set_member(
        &mut shard,
        TEST_SHARD,
        "borrowed-key",
        hex::encode(&member),
        member,
        an_address(),
        true,
    );

    // Compile-time: each read is a borrow. An owned return would fail to coerce here.
    let point: Option<&crate::engine::recorded_set_container::SetMemberMap> =
        shard.sets.get("borrowed-key");
    let point = point.expect("the fixture installed this key");
    let walked: &crate::engine::recorded_set_container::SetMemberMap =
        shard.sets.values().next().expect("one key");
    let paired: (&String, &crate::engine::recorded_set_container::SetMemberMap) =
        shard.sets.iter().next().expect("one entry");

    // Runtime: all three name the SAME resident entry, so none of them copied it.
    assert!(
        std::ptr::eq(point, walked),
        "the point lookup and the walk returned different addresses, so one of them copied the \
         entry -- the read path has become an allocation"
    );
    assert!(
        std::ptr::eq(point, paired.1),
        "the point lookup and `iter` returned different addresses, so `iter` copies"
    );
    assert_eq!(shard.sets.len(), 1);
    assert!(!shard.sets.is_empty());
    assert_eq!(shard.sets.keys().count(), 1);
    drain_records();
}

/// THE WRAPPER ENCODES TO THE BYTES THE BARE MAP ENCODED TO.
///
/// # WHY ROUND-TRIP AND NOT A DIGEST, AND WHY THE CHOICE WAS AVAILABLE HERE
///
/// For `hashes` a byte digest cannot prove this: a `HashMap`'s iteration order is randomised per
/// INSTANCE, so two encodings of identical content differ by key order alone and a digest compares
/// orders rather than bytes. For `sets` it is different -- `set_index_serde::serialize` collects
/// into a `BTreeMap`, so the outer key order of this field's encoding is SORTED and a digest would
/// have worked. This still uses the canonical round-trip, for uniformity across the four kinds and
/// because it exercises the decode direction as well, which a digest does not.
///
/// The claim is held against the SAME functions the field used to name, which is why the encoding
/// is identical by construction rather than by resemblance.
#[test]
fn the_set_container_encodes_to_the_same_bytes_as_the_bare_map() {
    let mut narrow = std::collections::BTreeMap::new();
    narrow.insert(b"member-a".to_vec(), an_address());
    narrow.insert(
        b"member-b".to_vec(),
        crate::BlockAddress::from_parts(4, 256, 128, None, None),
    );
    let mut wide = std::collections::BTreeMap::new();
    for index in 0..9u64 {
        wide.insert(
            format!("wide-{index}").into_bytes(),
            crate::BlockAddress::from_parts(5, index * 64, 64, Some(index), Some(index * 3)),
        );
    }
    let bare: std::collections::HashMap<String, crate::engine::recorded_set_container::SetMemberMap> =
        [
            ("narrow".to_string(), narrow),
            ("wide".to_string(), wide),
            ("empty".to_string(), std::collections::BTreeMap::new()),
        ]
        .into_iter()
        .collect();
    let container =
        crate::engine::recorded_set_container::RecordedSetContainer::from(bare.clone());

    // The bare map's own wire, produced by the functions the field used to name.
    #[derive(serde::Serialize)]
    struct BareWire<'a> {
        #[serde(with = "crate::engine::set_index_serde")]
        sets: &'a std::collections::HashMap<
            String,
            crate::engine::recorded_set_container::SetMemberMap,
        >,
    }
    #[derive(serde::Serialize)]
    struct WrappedWire<'a> {
        sets: &'a crate::engine::recorded_set_container::RecordedSetContainer,
    }

    let bare_value = serde_json::to_value(BareWire { sets: &bare }).expect("bare serializes");
    let wrapped_value =
        serde_json::to_value(WrappedWire { sets: &container }).expect("container serializes");
    assert_eq!(
        wrapped_value, bare_value,
        "the container's encoding differs from the bare map's in shape, keys or values, so this \
         change moves stored bytes and needs a format stamp after all"
    );
    let bare_json = serde_json::to_vec(&bare_value).expect("canonical");
    assert_eq!(
        serde_json::to_vec(&wrapped_value).expect("canonical"),
        bare_json,
        "the canonical encodings differ byte for byte"
    );

    // CONTROL: the comparison detects a planted difference -- an added key AND a changed address at
    // an existing key, because a key-set check alone would miss the second.
    let mut planted = bare.clone();
    planted.insert(
        "planted".to_string(),
        std::collections::BTreeMap::from([(b"p".to_vec(), an_address())]),
    );
    assert_ne!(
        serde_json::to_value(BareWire { sets: &planted }).expect("planted"),
        bare_value,
        "the comparison does not notice an added key, so the agreement above proves nothing"
    );
    let mut moved = bare.clone();
    moved.insert(
        "narrow".to_string(),
        std::collections::BTreeMap::from([(
            b"member-a".to_vec(),
            crate::BlockAddress::from_parts(9, 9, 9, None, None),
        )]),
    );
    assert_ne!(
        serde_json::to_value(BareWire { sets: &moved }).expect("moved"),
        bare_value,
        "the comparison does not notice a changed address at an existing key"
    );

    // AND IT DECODES THE BARE MAP'S OWN BYTES, which is the direction a stored index takes.
    #[derive(serde::Deserialize)]
    struct WrappedIn {
        sets: crate::engine::recorded_set_container::RecordedSetContainer,
    }
    let decoded: WrappedIn =
        serde_json::from_slice(&bare_json).expect("the container decodes the bare map's bytes");
    assert_eq!(decoded.sets.len(), 3);
    assert_eq!(
        decoded.sets.get("wide").map(|members| members.len()),
        Some(9),
        "the container decoded the bare map's bytes but lost a member"
    );
}
