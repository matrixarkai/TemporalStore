// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// CAN A HASH MODEL-MAP MUTATION HAPPEN WITHOUT A DURABLE RECORD?
//
// The answer this module holds is "not without editing `engine::recorded_hash_container`", and it
// is held in two halves because two different mechanisms do the work:
//
//   * THE COMPILER holds the OUTSIDE. `RecordedHashContainer::entries` is private to its module, so
//     no writer anywhere else in the crate can obtain `&mut` to the inner map. There is nothing for
//     a test to assert there -- a violation is a compile error, and this tree carries no
//     compile-fail harness to point at one (no `trybuild` dependency, no `.stderr` fixtures), so
//     the negative is DOCUMENTED here rather than executed.
//   * THIS MODULE holds the INSIDE. The compiler cannot object to a new `pub(super)` accessor added
//     INSIDE that module, and that is the one way the invariant can be lost. So the first test
//     below reads the module's own source and fails when its exposed surface changes -- with a
//     matcher that is itself checked against a planted hatch, because a guard that has never been
//     shown to fire on its own subject is not evidence.
//
// The second test holds that each recorded mutator emits exactly ONE record, counted before and
// after rather than asserted in prose.
// =================================================================================================

#![allow(clippy::all)]
use super::*;

use crate::engine::recorded_hash_container::{
    record_context_node_element, record_hash_element, record_hash_object_removal,
};

/// The module whose surface is the invariant.
const SOURCE: &str = include_str!("../recorded_hash_container.rs");

/// What a hatch would look like if someone added one. Used as a CONTROL on the matcher below: a
/// matcher that cannot see this cannot see the real thing either.
const PLANTED_HATCH: &str = "\
pub(super) fn entries_mut(&mut self) -> &mut HashMap<String, HashFieldMap> {
    &mut self.entries
}
";

/// A second planted shape, broken across lines the way `rustfmt` would break a long signature --
/// so the matcher is shown to be robust to the formatting, not just to the one-line spelling.
const PLANTED_HATCH_WRAPPED: &str = "\
pub(super) fn entries_mut(
    &mut self,
) -> &mut HashMap<String, HashFieldMap> {
    &mut self.entries
}
";

/// A third: the field itself made visible, which needs no accessor at all.
const PLANTED_PUBLIC_FIELD: &str = "\
pub(super) struct RecordedHashContainer {
    pub(super) entries: HashMap<String, HashFieldMap>,
}
";

/// THE MATCHER, and it asks CONTAINS rather than IS.
///
/// It looks for a mutable borrow of the inner map in a RETURN position, which is the only shape
/// that can carry `&mut` to the map out of the module. `->` and the type it names stay adjacent
/// through every line break `rustfmt` can introduce, which is why the substring is anchored on the
/// arrow rather than on a line start.
fn mutable_borrows_of_the_inner_map(source: &str) -> Vec<String> {
    let mut found = Vec::new();
    for needle in [
        "-> &mut HashMap",
        "-> &mut std::collections::HashMap",
        "-> &'a mut HashMap",
        "-> Option<&mut HashMap>",
        "-> &mut HashFieldMap",
    ] {
        if source.contains(needle) {
            found.push(needle.to_string());
        }
    }
    found
}

/// The second shape: the field given any visibility at all.
///
/// ANCHORED ON THE DECLARATION, NOT ON THE NAME. Asking only for a `pub` line containing
/// `entries:` matched `pub(super) fn restore_for_test(&mut self, entries: HashMap<..>)` -- a
/// PARAMETER carrying the field's name -- and the guard failed on a line that exposes nothing. That
/// was driven: this matcher reported one offender before it was anchored. So it now requires the
/// struct-field shape: a visibility, the field with its declared type, a trailing comma, no `fn`.
fn visible_declarations_of_the_field(source: &str) -> Vec<String> {
    source
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.starts_with("pub")
                && !line.contains("fn ")
                && line.contains("entries: HashMap<String, HashFieldMap>")
                && line.ends_with(",")
        })
        .map(str::to_string)
        .collect()
}

/// Everything the module exposes to the rest of `engine`, in source order, excluding whatever sits
/// under a `#[cfg(test)]` at column zero.
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
///
/// # WHY A PINNED LIST AND NOT JUST THE TWO MATCHERS
///
/// The matchers catch the two shapes that hand the map out. They cannot catch a THIRD shape nobody
/// has thought of. The pinned list catches every shape, at the cost of going stale -- and here the
/// staleness is the feature: a new name on this module forces a human to come to this list and say
/// what the new entry is for, which is precisely the review the four pieces of evidence behind this
/// type show did not happen when the mutation and its record were two statements at a call site.
///
/// # WHAT EACH ENTRY IS FOR
///
/// Three emitters mint a proof after emitting a record; three mutators consume one. Four exceptions
/// mutate without a record, each because on that path the record is the SOURCE of the change --
/// they are named in the module and the reasons are there, not here. The rest are reads.
#[test]
fn the_hash_container_exposes_no_way_to_mutate_the_map_without_a_record() {
    // CONTROL 1: the matcher sees a planted hatch, in both spellings, and the planted public field.
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
    // CONTROL 2: the matcher is reading the module it thinks it is reading. A token that MUST be
    // present, so an empty or wrong `include_str!` cannot score clean.
    assert!(
        SOURCE.contains("pub(super) fn install"),
        "the source read is not `recorded_hash_container.rs`: it has no `install`"
    );
    assert!(
        SOURCE.len() > 8_000,
        "the source read is {} bytes, which is too small to be the module",
        SOURCE.len()
    );

    // THE SUBJECT.
    assert_eq!(
        mutable_borrows_of_the_inner_map(SOURCE),
        Vec::<String>::new(),
        "`recorded_hash_container` hands out a mutable borrow of the hash model map. That is the \
         escape hatch this type exists to remove: with it, a writer can mutate the map without \
         emitting the durable record, and the four questions this campaign asked about \
         `shard.hashes` become unanswerable again."
    );
    assert_eq!(
        visible_declarations_of_the_field(SOURCE),
        Vec::<String>::new(),
        "`RecordedHashContainer::entries` has been given a visibility. The field being private to \
         its module IS the invariant -- every other guarantee here rests on `rustc` refusing \
         `&mut shard.hashes.entries` from any other module."
    );

    let expected: Vec<&str> = vec![
        // The container itself.
        "RecordedHashContainer",
        // The proofs. Each is minted only by an emitter below.
        "RecordedHashElement",
        "RecordedHashFieldRemoval",
        "RecordedHashObjectRemoval",
        // The emitters. Each emits the durable record, then returns the proof.
        "record_hash_element",
        "record_context_node_element",
        "record_hash_field_removal",
        "record_hash_object_removal",
        // Reads, all borrows.
        "get",
        "contains_key",
        "len",
        "is_empty",
        "keys",
        "values",
        "iter",
        // The recorded mutators. Each consumes a proof.
        "install",
        "remove_field",
        "remove_object",
        // The five exceptions, where the record is the source and not the sink.
        "reconcile_from_durable",
        "fold_carried_elements",
        "replay_remove_field",
        "replay_install_element",
        "element_addresses_mut",
        // The narrowed lend exception 5 walks with: addresses, never membership.
        "ElementAddressesMut",
        "iter_mut",
    ];
    let actual = exposed_surface(SOURCE);
    assert_eq!(
        actual,
        expected,
        "the surface of `recorded_hash_container` changed. If an entry was ADDED, say in this list \
         what it is for and whether it can mutate the map without a record -- that is the review \
         the type exists to force. If one was REMOVED, take it out of this list too.\n  \
         expected: {expected:?}\n  actual:   {actual:?}"
    );

    // AND THE NEGATIVE THAT IS DOCUMENTED RATHER THAN EXECUTED. There is no compile-fail harness
    // in this tree -- no `trybuild` dependency and no `.stderr` fixtures -- so the statement
    // "`&mut shard.hashes.entries` does not compile outside the module" is held by `rustc`'s
    // privacy rules and by the absence of any accessor above, not by a test that runs it. Adding
    // one to see it fail would mean adding a dev-dependency and a second compilation of the crate.
    assert!(
        SOURCE.contains("entries: HashMap<String, HashFieldMap>"),
        "the inner map is no longer a bare private field, so the privacy argument above may no \
         longer be the one that holds"
    );
}

/// THE ONE HOLE PRIVACY CANNOT CLOSE, CLOSED BY A SWEEP INSTEAD.
///
/// `ShardState::hashes` is `pub(super)`, so any module in `engine` can REPLACE THE WHOLE FIELD --
/// `shard.hashes = Default::default()` wipes the map, unrecorded, and `rustc` has no objection
/// because the inner field is never named. Privacy guards the INSIDE of the container; it cannot
/// guard the binding that holds it.
///
/// Removing `Default` would close it, and cannot be done: `#[serde(default)]` on the field needs it
/// so that an index written before `hashes` became durable still decodes with an empty map.
///
/// So this sweeps instead. It is a weaker instrument than the borrow checker and is written to say
/// so: it prints its own denominator, and it is checked against a planted line.
#[test]
fn nothing_outside_the_container_assigns_the_hash_field_wholesale() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let module = root.join("engine").join("recorded_hash_container.rs");

    fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let entries = std::fs::read_dir(dir).unwrap_or_else(|err| {
            panic!("the sweep cannot read {}: {err}", dir.display())
        });
        for entry in entries {
            let path = entry.expect("a readable directory entry").path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }

    /// THE MATCHER, AND IT IS ANCHORED ON THE DOT.
    ///
    /// Asking for `hashes =` reported 48 sites, every one of them a LOCAL named `node_hashes`,
    /// `tenant_hashes` or `hashes` -- the word is ordinary in this crate, and that over-report was
    /// driven before the anchor was added. What makes a line a wholesale write of the model map is
    /// that it assigns through a FIELD ACCESS: `.hashes =`. Comparisons are excluded because
    /// `.hashes ==` is a read.
    ///
    /// Comment lines are dropped because this tree's prose quotes the old `shard.hashes = hashes`
    /// shape in seven places as HISTORY, and a guard that fired on its own documentation would have
    /// to be relaxed, which is how a matcher dies.
    ///
    /// AND IT REQUIRES THE STATEMENT SHAPE. Dropping comments was not enough: one of those seven
    /// quotations lives inside a `assert!` MESSAGE -- a string-literal continuation whose trimmed
    /// text starts with the prose itself and ends with a backslash. That was driven too. An
    /// assignment statement ends with `;`, and a multi-line one ends with the `(` that opens its
    /// right-hand side, so those two endings are the filter and prose is not one of them.
    fn assignments(source: &str) -> Vec<String> {
        source
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with("//"))
            .filter(|line| line.ends_with(";") || line.ends_with("("))
            .filter(|line| {
                (line.contains(".hashes =") && !line.contains(".hashes =="))
                    || line.contains(".hashes=")
                    || (line.contains("mem::take(&mut ") && line.contains(".hashes)"))
            })
            .map(str::to_string)
            .collect()
    }

    // CONTROL: the matcher sees a planted assignment and a planted take.
    assert!(
        !assignments("    shard.hashes = Default::default();").is_empty(),
        "the matcher cannot see a wholesale assignment, so its silence means nothing"
    );
    assert!(
        !assignments("    let held = std::mem::take(&mut shard.hashes);").is_empty(),
        "the matcher cannot see a wholesale take"
    );
    // CONTROL: it does NOT fire on a read, or on the prose that quotes the old shape.
    assert!(
        assignments("    if shard.hashes == other {}").is_empty(),
        "the matcher fires on a comparison, which is a read"
    );
    assert!(
        assignments("    // the flag gates `shard.hashes = hashes`, a wholesale assignment")
            .is_empty(),
        "the matcher fires on a comment, so this tree's own history would force it to be relaxed"
    );
    // CONTROL: it does NOT fire on an ordinary local whose name ends in `hashes`. Without the dot
    // anchor it fired on 48 of these, which is how a guard gets relaxed into uselessness.
    assert!(
        assignments("    let mut node_hashes = Vec::new();").is_empty(),
        "the matcher fires on a local named `node_hashes`, which is not the model map"
    );
    assert!(
        assignments("    node_hashes = prefiltered;").is_empty(),
        "the matcher fires on an assignment to a local, not to the field"
    );
    // CONTROL: it does NOT fire on the shape quoted inside an assertion MESSAGE, which is how this
    // tree records what the old wholesale assignment used to look like.
    assert!(
        assignments("merge is reverted to `shard.hashes = hashes`, or if `insert_element_if_absent` stops \\")
            .is_empty(),
        "the matcher fires on prose inside a string literal, so this tree's own records of the old \
         shape would force it to be relaxed"
    );
    // CONTROL, THE OTHER WAY: the statement filter must not silence a REAL multi-line assignment.
    assert!(
        !assignments("    shard.hashes = fill_absent_elements(").is_empty(),
        "the statement filter silenced a multi-line assignment, which is the shape this change just \
         removed from two sites and the one most likely to come back"
    );

    // THE SCOPE IS `engine/`, and that is the right scope rather than a convenience: `hashes` is
    // `pub(super)` on `ShardState`, so `engine` and its descendants are exactly the modules that
    // can name it at all. A sweep over the whole crate would read 440,000 lines that cannot express
    // the thing being looked for, and a denominator padded with lines that cannot fail is the shape
    // that makes a sweep look thorough while proving less.
    let mut files = Vec::new();
    rust_files(&root.join("engine"), &mut files);
    files.push(root.join("engine.rs"));
    let scanned = files.len();
    assert!(
        scanned > 40,
        "the sweep scanned only {scanned} files, which is not the engine subtree -- an empty denominator scores clean and proves nothing"
    );

    let mut offenders = Vec::new();
    let mut checked_lines = 0usize;
    let mut skipped = 0usize;
    for path in &files {
        // The container itself, whose whole job is to own the field, and THIS file, which carries
        // the planted shapes above as string literals. Both are counted and named rather than
        // filtered silently.
        if path == &module || path.ends_with("recorded_hash_container_invariant.rs") {
            skipped += 1;
            continue;
        }
        let source = std::fs::read_to_string(path).expect("a readable source file");
        checked_lines += source.lines().count();
        for line in assignments(&source) {
            offenders.push(format!("{}: {line}", path.display()));
        }
    }
    assert_eq!(
        skipped, 2,
        "the sweep skipped {skipped} files, not the two it names"
    );
    // The denominator, measured AFTER the last filter.
    assert!(
        checked_lines > 50_000,
        "the sweep read {checked_lines} lines over {scanned} files, which is too few to be the engine subtree"
    );

    assert_eq!(
        offenders,
        Vec::<String>::new(),
        "{} site(s) replace or empty the hash model map wholesale, which is a mutation with no          record and which privacy cannot catch because it never names the inner field. Route it          through a named method on `RecordedHashContainer` instead. Scanned {checked_lines} lines          over {scanned} files.\n  {}",
        offenders.len(),
        offenders.join("\n  ")
    );
}

// =================================================================================================
// DOES EACH RECORDED MUTATOR EMIT EXACTLY ONE RECORD?
// =================================================================================================

const TEST_SHARD: ShardId = 9;

fn shard_for_records() -> ShardState {
    let mut shard = ShardState::default();
    shard.set_routing_range(0, crate::DEFAULT_END_ROUTING_BUCKET);
    shard.set_shard_id(TEST_SHARD);
    shard
}

fn an_address() -> crate::BlockAddress {
    crate::BlockAddress::from_parts(3, 128, 64, Some(11), Some(22))
}

/// Clear the thread-local staging area so a delta is this test's and not a neighbour's.
fn drain_records() {
    let _ = crate::engine::block_in_wal::take_outcomes();
    let _ = crate::engine::block_in_wal::take_staged();
}

fn staged() -> usize {
    crate::engine::block_in_wal::staged_outcome_count()
}

/// ONE MUTATION, ONE RECORD, COUNTED.
///
/// # WHY A COUNT AND NOT A PRESENCE CHECK
///
/// "A record was emitted" goes green when two are emitted, and two is a real defect on this path:
/// a replay that installs the same page twice under two kinds is exactly what
/// `upsert_bucket_index_block_with(.., stage: false)` exists to prevent. So the assertion is on the
/// NUMBER, taken as a delta across the call.
///
/// # THIS TEST WAS DRIVEN TO FAIL
///
/// The emission line was removed from `record_hash_element` and this test was run: it failed with
/// `0 != 1` at the first assertion below. It was then restored and the test passes. A guard that
/// has never failed on its own subject is not evidence, and this one has.
#[test]
fn each_recorded_hash_mutator_emits_exactly_one_record() {
    drain_records();

    // 1. INSTALLING AN ELEMENT. The record is the bucket-index entry, which stages one outcome.
    let mut shard = shard_for_records();
    let before = staged();
    let recorded = record_hash_element(
        &mut shard,
        TEST_SHARD,
        "recorded-key",
        "field-a".to_string(),
        an_address(),
        true,
    );
    let after = staged();
    assert_eq!(
        after - before,
        1,
        "installing one hash element staged {} records, not 1. Zero means the mutation can happen \
         unrecorded, which is the whole defect class this type removes; more than one means a \
         replay installs the page more than once.",
        after - before
    );
    // And the mutation is not possible without that proof: this line consumes it.
    shard.hashes.install(recorded);
    assert_eq!(
        shard.hashes.get("recorded-key").map(|fields| fields.len()),
        Some(1),
        "the recorded element did not reach the resident map"
    );

    // 2. INSTALLING A CONTEXT NODE, whose record is DELIBERATELY the staged outcome alone and not
    //    a bucket-index entry -- so it is a separate emitter, and it too emits exactly one.
    let before = staged();
    let recorded = record_context_node_element(
        TEST_SHARD,
        "context_node",
        "recorded-node",
        "meta",
        7,
        an_address(),
    );
    let after = staged();
    assert_eq!(
        after - before,
        1,
        "installing one context node staged {} records, not 1",
        after - before
    );
    shard.hashes.install(recorded);

    // 3. DELETING THE WHOLE OBJECT. This one stages NOTHING, and that is correct rather than a
    //    hole: its durable record is the bucket index's own deleted-object set, which a dump
    //    carries. So the count asserted here is the count of DELETED OBJECT IDS, and the staged
    //    count is asserted to be unmoved -- a second record here would make a load see the
    //    deletion twice.
    let deleted_before: usize = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.deleted_object_index.object_count())
        .sum();
    let staged_before = staged();
    let (_marked, removal) = record_hash_object_removal(&mut shard, "recorded-key");
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
        "the object deletion staged a WAL outcome. Its record is the bucket index's deleted-object \
         set; a staged outcome as well would have a load apply the deletion twice."
    );
    assert!(
        shard.hashes.remove_object(removal),
        "the recorded object deletion did not drop the resident hash"
    );
    assert!(
        !shard.hashes.contains_key("recorded-key"),
        "the hash is still resident after a recorded object deletion"
    );

    drain_records();
}

/// THE WRAPPER ENCODES TO THE BYTES THE BARE MAP ENCODED TO, IN BOTH CODECS THE INDEX USES.
///
/// This is the claim that no stored byte moved and that no format stamp has to. It is held here
/// rather than argued, and it is held at the FIELD because the field is the only thing this change
/// altered: `ShardState`'s other seventeen model maps, its bucket index and its counters are
/// untouched, so if this field's bytes are identical the index's bytes are identical.
///
/// `#[serde(transparent)]` over exactly one field is what makes it true -- the derived impls forward
/// verbatim to `HashMap<String, HashFieldMap>` -- and that is a property of an attribute, which is
/// exactly the kind of property that is removed by an edit nobody notices. Hence a test.
#[test]
fn the_container_encodes_to_the_same_bytes_as_the_bare_map() {
    let mut fields = crate::engine::hash_field_map::HashFieldMap::default();
    fields.insert("field-a".to_string(), an_address());
    fields.insert("field-b".to_string(), crate::BlockAddress::from_parts(4, 256, 128, None, None));
    let mut wide = crate::engine::hash_field_map::HashFieldMap::default();
    for index in 0..9u64 {
        wide.insert(
            format!("wide-{index}"),
            crate::BlockAddress::from_parts(5, index * 64, 64, Some(index), Some(index * 3)),
        );
    }
    let bare: std::collections::HashMap<String, crate::engine::hash_field_map::HashFieldMap> =
        [
            ("narrow".to_string(), fields),
            ("wide".to_string(), wide),
            ("empty".to_string(), crate::engine::hash_field_map::HashFieldMap::default()),
        ]
        .into_iter()
        .collect();
    let container =
        crate::engine::recorded_hash_container::RecordedHashContainer::from(bare.clone());

    // RAW `to_vec` OF TWO MAP INSTANCES CANNOT BE COMPARED, AND THIS TEST FAILED THAT WAY FIRST.
    //
    // A `HashMap`'s iteration order is randomized per INSTANCE, and `HashFieldMap` serializes
    // through a `HashMap` too -- so two serializations of two instances holding identical contents
    // differ in key order and in nothing else. The first version of this test compared them
    // directly and failed with two byte vectors whose only difference was the order of `wide-0`
    // through `wide-8`. That is comparing orders, not bytes.
    //
    // So the comparison is CANONICAL: through `serde_json::Value`, whose object is key-ordered, so
    // the shape and every key and value are compared and the instance's order is not.
    let bare_value = serde_json::to_value(&bare).expect("the bare map serializes");
    let wrapped_value = serde_json::to_value(&container).expect("the container serializes");
    assert_eq!(
        wrapped_value, bare_value,
        "the container's JSON encoding differs from the bare map's in shape, keys or values, so \
         this change moves stored bytes and needs a format stamp after all"
    );
    let bare_json = serde_json::to_vec(&bare_value).expect("canonical bytes");
    let wrapped_json = serde_json::to_vec(&wrapped_value).expect("canonical bytes");
    assert_eq!(
        wrapped_json, bare_json,
        "the canonical encodings differ byte for byte"
    );

    // msgpack, the index's other codec, through the same canonical form.
    let bare_pack = rmp_serde::to_vec_named(&bare_value).expect("the bare map packs");
    let wrapped_pack = rmp_serde::to_vec_named(&wrapped_value).expect("the container packs");
    assert_eq!(
        wrapped_pack, bare_pack,
        "the container's msgpack encoding differs from the bare map's"
    );

    // CONTROL: the comparison detects a planted difference. Without this, agreement above could
    // mean the encoders are producing nothing, or that `Value` equality is vacuous.
    let mut planted = bare.clone();
    planted.insert(
        "planted".to_string(),
        [("f".to_string(), an_address())].into_iter().collect(),
    );
    let planted_value = serde_json::to_value(&planted).expect("planted serializes");
    assert_ne!(
        planted_value, bare_value,
        "`Value` equality does not notice an added key, so the agreement above proves nothing"
    );
    assert_ne!(
        serde_json::to_vec(&planted_value).expect("canonical"),
        bare_json,
        "the canonical JSON bytes do not move when a key is added"
    );
    assert_ne!(
        rmp_serde::to_vec_named(&planted_value).expect("canonical"),
        bare_pack,
        "the canonical msgpack bytes do not move when a key is added"
    );
    // CONTROL, the subtler one: a changed VALUE at an existing key must also be seen, not just a
    // changed key set.
    let mut moved = bare.clone();
    moved.insert(
        "narrow".to_string(),
        [("field-a".to_string(), crate::BlockAddress::from_parts(9, 9, 9, None, None))]
            .into_iter()
            .collect(),
    );
    assert_ne!(
        serde_json::to_value(&moved).expect("moved serializes"),
        bare_value,
        "the comparison does not notice a changed address at an existing key"
    );

    // AND IT DECODES THE BARE MAP'S OWN BYTES, which is the direction an index written by a binary
    // that had the bare field takes.
    let decoded: crate::engine::recorded_hash_container::RecordedHashContainer =
        serde_json::from_slice(&serde_json::to_vec(&bare).expect("bytes"))
            .expect("the container decodes the bare map's bytes");
    assert_eq!(decoded.len(), 3);
    assert_eq!(
        decoded.get("wide").map(|fields| fields.len()),
        Some(9),
        "the container decoded the bare map's bytes but lost a field"
    );
    assert_eq!(
        decoded.get("narrow").map(|fields| fields.len()),
        Some(2),
        "the container decoded the bare map's bytes but lost a field of the narrow key"
    );
}

/// THE READS ARE STILL BORROWS, WHICH IS WHY THEY STILL COST NOTHING.
///
/// # WHY POINTER IDENTITY AND NOT AN ALLOCATION COUNT
///
/// The counting allocator lives behind the `alloc-probe` feature, and a test reading its statics
/// without the feature measures a process where they never move and reports a table of zeros --
/// `every_counting_allocator_probe_is_gated_on_the_feature_that_installs_it` exists to stop exactly
/// that. So the assertion here is one that holds under the default feature set and cannot report a
/// false zero: a read hands back a BORROW OF THE RESIDENT ENTRY. Two reads of one key return the
/// same pointer, and the pointer a walk yields is the pointer a point lookup yields. A read that
/// had become a clone could not satisfy either, because a clone lives somewhere else.
///
/// The typed bindings below are the compile-time half: if `get` or `values` ever returned an owned
/// value, these lines would not compile.
#[test]
fn reading_the_hash_container_hands_back_borrows_and_not_copies() {
    drain_records();
    let mut shard = shard_for_records();
    let recorded = record_hash_element(
        &mut shard,
        TEST_SHARD,
        "borrowed-key",
        "field-a".to_string(),
        an_address(),
        true,
    );
    shard.hashes.install(recorded);

    // Compile-time: each read is a borrow. An owned return would fail to coerce here.
    let point: Option<&crate::engine::hash_field_map::HashFieldMap> =
        shard.hashes.get("borrowed-key");
    let point = point.expect("the fixture installed this key");
    let walked: &crate::engine::hash_field_map::HashFieldMap = shard
        .hashes
        .values()
        .next()
        .expect("the fixture installed one key");
    let paired: (&String, &crate::engine::hash_field_map::HashFieldMap) =
        shard.hashes.iter().next().expect("one entry");

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
    assert!(
        std::ptr::eq(point, shard.hashes.get("borrowed-key").expect("still there")),
        "two point lookups of one key returned different addresses"
    );
    assert_eq!(shard.hashes.len(), 1);
    assert!(!shard.hashes.is_empty());
    assert_eq!(shard.hashes.keys().count(), 1);
    drain_records();
}
