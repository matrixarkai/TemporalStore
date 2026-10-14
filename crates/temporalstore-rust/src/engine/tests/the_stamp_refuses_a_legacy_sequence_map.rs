// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT MAKES DELETING `ShardState::sequences` SAFE, DRIVEN RATHER THAN ARGUED.
//!
//! # THE ARGUMENT THAT IS NOT ENOUGH
//!
//! "Nothing writes it" is true and insufficient. Every `insert` and `entry` on that map lived in
//! `engine/tests/conformance.rs`, a test model -- but the field WAS ON THE WIRE when non-empty,
//! and `ShardState` sets no `deny_unknown_fields`. So an index that still carried a sequence
//! series would decode CLEANLY against a struct without the field, and the series would be
//! dropped with no error raised anywhere. A deletion justified only by the absence of a writer
//! loses that data silently, which is the worst available failure mode.
//!
//! # WHAT IS ACTUALLY LOAD-BEARING: THE STAMP, AND THE ORDER
//!
//! `load_index_inner` refuses any index stamped below `SHARD_INDEX_FORMAT_VERSION` and answers
//! `Ok(None)`, and it does so BEFORE anything reads the decoded maps. So reaching the point where
//! a sequence series could matter implies the stamp is current, and a current-stamped index was
//! written by a binary that never wrote the field. The data the decode drops is data no reachable
//! load path would have looked at.
//!
//! That is a claim about CONTROL FLOW, so it is driven here instead of reasoned about.
//!
//! # THE TWO ARMS, AND WHY THE SECOND IS REQUIRED
//!
//! A refusal arm on its own passes for the wrong reason as soon as the fixture stops reaching the
//! load path at all -- a store the loader never opens refuses everything. So the SAME image is
//! loaded twice, once stamped below the constant and once stamped AT it, and the second must be
//! ACCEPTED. One arm says the stale image is refused; the other says the image is loadable.
//!
//! # WHY THE RETURN VALUE IS THE EVIDENCE AND THE COUNTERS ARE CORROBORATION
//!
//! `index_load_path_counts` is four process-global atomics. Another test loading a shard in
//! another thread moves them, so a zero asserted on one of them is a cross-test flake waiting to
//! happen and a non-zero can be somebody else's load. What `load_index_checked` RETURNS is scoped
//! to this call and nothing else: `Ok(None)` is the refusal, `Ok(Some(state))` is the acceptance.
//! The counters are printed, and asserted only in the direction that can misreport as a pass
//! rather than as a failure.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

use crate::block_store::ElementEntry;

const SHARD: ShardId = 9;

/// The object key of the series the legacy map carries.
const SERIES_KEY: &str = "seq/a-series-written-before-the-fold";
/// Its one row's timestamp.
const ROW_TIMESTAMP_MS: u64 = 1_700_000_000_123;

/// A whole shard index whose `sequences` map holds one non-empty series, at the given stamp.
///
/// BUILT THROUGH THE REAL TYPES, NOT SPELLED AS A STRING LITERAL. `ShardState` has 48 fields and
/// most of them are required on the wire, so a hand-written JSON fixture would go stale the next
/// time one is added and would then fail as a PARSE error -- which reads exactly like the refusal
/// this module is trying to measure. Serializing a `ShardState::default()` cannot go stale.
///
/// THE SERIES IS SERIALIZED OUT OF `features` AND MOVED ACROSS. `features` and the retired
/// `sequences` map had IDENTICAL types -- `HashMap<String, BTreeMap<u64, ElementEntry>>` -- so a
/// series serialized from one is byte-for-byte what a pre-fold binary wrote under the other. That
/// is also why the fixture cannot be built by setting the field: this binary no longer has it.
fn index_image_carrying_a_legacy_sequence_map(stamp: u32) -> Vec<u8> {
    let mut state = ShardState::default();
    state.index_format_version = stamp;
    let mut series = BTreeMap::new();
    series.insert(
        ROW_TIMESTAMP_MS,
        ElementEntry::from_parts(1, 4_096, 128, Some(3), Some(7)),
    );
    state.features.insert(SERIES_KEY.to_string(), series);

    let mut value = serde_json::to_value(&state).expect("a ShardState serializes");
    let object = value
        .as_object_mut()
        .expect("a serialized ShardState is a JSON object");
    let carried = object
        .remove("features")
        .expect("`features` is on the wire -- it is where the series was just built");
    object.insert("features".to_string(), serde_json::json!({}));
    // THE INSERT'S RETURN IS A GUARD, NOT A FORMALITY: `None` means this binary's `ShardState`
    // really has no `sequences` field, so the image is the legacy-only shape the module is about.
    // A `Some` here would mean the field came back and both arms below are measuring nothing.
    assert!(
        object.insert("sequences".to_string(), carried).is_none(),
        "this binary still serializes a `sequences` field, so the fixture is not the \
         legacy-only shape these guards are about"
    );
    serde_json::to_vec(&value).expect("the image serializes")
}

/// Plant that image where the loader looks, and hand back an engine over it.
///
/// The `TempDir` is returned so the caller holds it: dropped, it takes the planted index with it.
fn engine_over_a_planted_image(stamp: u32) -> (tempfile::TempDir, TemporalEngine, Vec<u8>) {
    let image = index_image_carrying_a_legacy_sequence_map(stamp);
    let dir = tempfile::tempdir().expect("tempdir");
    let indexes = dir.path().join("indexes");
    std::fs::create_dir_all(&indexes).expect("mkdir indexes");
    std::fs::write(indexes.join(format!("shard-{SHARD}.index.json")), &image)
        .expect("plant the index image");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        indexes,
    );
    (dir, engine, image)
}

/// The image really does carry the retired map, asserted on the BYTES.
///
/// On the bytes and not on a decode, because what the decode can still see of that field is
/// exactly what this change takes away: after the deletion the decode drops it, so a decoded
/// fixture would look empty whether the fixture carried a series or never had one.
fn assert_the_image_spells_the_legacy_map(image: &[u8]) {
    let text = std::str::from_utf8(image).expect("the image is JSON text");
    for spelling in ["\"sequences\"", SERIES_KEY] {
        assert!(
            text.contains(spelling),
            "the planted image does not spell {spelling}, so it is not an index carrying a \
             legacy sequence series and these guards are about nothing"
        );
    }
}

/// A STALE-STAMPED INDEX CARRYING A SEQUENCE SERIES IS REFUSED, NOT FOLDED.
///
/// If this ever reports `Ok(Some(..))`, the control-flow reading above is wrong: an index that
/// still carries sequence data would be reaching code that cannot see it, and deleting the field
/// loses that data silently. That is the one outcome here that must stop the change.
#[test]
fn a_stale_stamped_index_carrying_a_sequence_map_is_refused_before_anything_reads_it() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let (_dir, engine, image) = engine_over_a_planted_image(stale);
    assert_the_image_spells_the_legacy_map(&image);

    // IT DECODES. A refusal that was really a parse failure would prove nothing about the stamp,
    // and it is the clean decode that makes the stamp the only thing standing between a reader
    // and the dropped series.
    let decoded = crate::engine::decode_index_bytes(&image).expect(
        "an index written before the fold must still DECODE -- `ShardState` sets no \
         `deny_unknown_fields`, which is precisely why the STAMP has to be what stops it",
    );
    assert_eq!(
        stale, decoded.index_format_version,
        "the planted image is stamped {} rather than {stale}, so it is not the stale index this \
         guard is about",
        decoded.index_format_version
    );
    assert!(
        decoded.features.is_empty(),
        "the decode put {} series into `features` on its own, so this fixture cannot tell a \
         refusal from a fold",
        decoded.features.len()
    );

    crate::engine::persistence::reset_index_load_path_counts();
    let loaded = engine
        .load_index_checked(SHARD, false)
        .expect("the load must not error -- a refusal is Ok(None), not Err");
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    println!(
        "\n=== stamp {stale} against a current {SHARD_INDEX_FORMAT_VERSION} ===\n  \
         load path -> accepted {accepted}, refused-stale {refused_stale}, absent {absent}, \
         undecodable {undecodable}"
    );

    if let Some(state) = loaded {
        panic!(
            "THE STAMP DID NOT REFUSE IT. The load returned a ShardState holding {} feature \
             series from an index stamped {stale}. Deleting `ShardState::sequences` is NOT safe: \
             a reachable load path reads an index that can still carry sequence data, and the \
             decode drops that data with no error. Counters: accepted {accepted}, refused-stale \
             {refused_stale}, absent {absent}, undecodable {undecodable}",
            state.features.len()
        );
    }
    // Corroboration only, and in the direction that can only ever misreport as a PASS: these are
    // process-global counters and a parallel test's load moves them too.
    assert!(
        refused_stale > 0,
        "the load returned None and yet NOTHING was counted as a stale-stamp refusal: accepted \
         {accepted}, refused-stale {refused_stale}, absent {absent}, undecodable {undecodable}. \
         An index the loader never opened also returns None, and that is not what this asserts"
    );
}

/// THE POSITIVE CONTROL: the same image at the CURRENT stamp is accepted.
///
/// Without this, the arm above passes for a fixture the loader never opens -- and a guard that
/// refuses everything refuses correctly for the wrong reason. This says the image is loadable, so
/// the refusal above is the stamp doing its job rather than the fixture missing the path.
#[test]
fn the_same_image_at_the_current_stamp_is_accepted_by_the_same_load_path() {
    let (_dir, engine, image) = engine_over_a_planted_image(SHARD_INDEX_FORMAT_VERSION);
    assert_the_image_spells_the_legacy_map(&image);

    crate::engine::persistence::reset_index_load_path_counts();
    let loaded = engine
        .load_index_checked(SHARD, false)
        .expect("the load must not error");
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    println!(
        "\n=== the same image stamped {SHARD_INDEX_FORMAT_VERSION} ===\n  \
         load path -> accepted {accepted}, refused-stale {refused_stale}, absent {absent}, \
         undecodable {undecodable}"
    );
    let state = loaded.expect(
        "the SAME image at the CURRENT stamp was not accepted. The refusal arm beside this one \
         is then about a fixture the loader never reaches, and it proves nothing about the stamp",
    );
    assert!(
        accepted > 0,
        "the load returned a ShardState and yet nothing was counted as accepted: accepted \
         {accepted}, refused-stale {refused_stale}, absent {absent}, undecodable {undecodable}"
    );

    // AND THE RETIRED FIELD'S DATA IS DROPPED RATHER THAN FOLDED -- stated here because it is the
    // hazard this whole module exists to bound. An ACCEPTED index carrying `sequences` loses that
    // series, silently. What makes the deletion safe is that no index a binary stamped at the
    // current version can carry one: this fixture is hand-made, and no writer produces it.
    assert!(
        state.features.is_empty(),
        "the accepted load folded {} series out of the retired map into `features`, so the field \
         is still being read somewhere and this module's premise is wrong",
        state.features.len()
    );
}
