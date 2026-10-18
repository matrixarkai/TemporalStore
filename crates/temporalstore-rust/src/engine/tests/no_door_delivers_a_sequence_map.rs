// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! NO DOOR CAN DELIVER A `sequences` MAP, AND THAT IS WHAT MAKES THE FIELD SAFE TO DELETE.
//!
//! # THE ARGUMENT THAT IS NOT ENOUGH
//!
//! "Nothing writes it" is true -- every `insert` and `entry` on that map lived in
//! `engine/tests/conformance.rs`, a test model -- and on its own it is wrong, because the field
//! WAS on the wire when non-empty and `ShardState` carries no `deny_unknown_fields`. An index
//! still holding a series decodes *cleanly* against a struct without the field, and the series is
//! dropped with no error raised anywhere. A deletion resting on the absence of a writer loses that
//! data silently.
//!
//! So the question is not "who writes it" but "what can still ARRIVE carrying it". That is a
//! question about doors, and it took three passes to enumerate them. Each pass found the previous
//! answer had been a claim about one function reported as a claim about the system.
//!
//! # THE THREE DOORS, AND WHY EACH IS SHUT
//!
//! **Door 1 -- the base index file.** `load_index_inner` decodes it and refuses any stamp below
//! `SHARD_INDEX_FORMAT_VERSION`, before anything reads the decoded maps. Arms 1 and 2.
//!
//! **Door 2 -- the dump manifest.** `decode_index_bytes` has six production callers and for a long
//! time only `load_index_inner` compared the stamp; the version check inside `decode_index_bytes`
//! is on the msgpack-container arm, which a plain-JSON index never meets. A stamp-13 image
//! installed through `install_bucket_dump_manifest` reached a live shard with all four
//! `index_load_path_counts` counters at ZERO, and the install re-serialized the decoded state --
//! so with this field deleted the rows were written away irrecoverably. That hole is what the
//! change before this one closed, and arm 3 is the arm that failed before it and passes now.
//!
//! **Door 3 -- the base index file being ABSENT.** `load_index_inner`'s `Err(_)` arm takes
//! `ShardState::default()` and never reaches the stamp comparison at all, and
//! `fold_index_log_deltas` then runs with `base_anchor` 0 and folds the whole index log. Nothing
//! decodes on that path, so neither the stamp nor any decode-time fold fires there. Arms 5 and 6.
//!
//! Door 3 is shut by what the log can CARRY rather than by a gate, and that is checked at the
//! layer that decides rather than argued:
//!
//!   * `capture_key_states` -- the writer -- emits exactly thirteen field names
//!     (`features`, `expires_at_ms`, `control_state_changes`,
//!     `control_state_selection`, `context_nodes`, `context_events`, `context_indexes`,
//!     `context_audits`, `context_children`, `context_summaries`, `context_compressions`,
//!     `context_entities`) and `sequences` is not one of them, so no index log in existence
//!     carries such an entry;
//!   * `apply_key_states` -- the reader -- has exactly thirteen `apply_key_state_field` calls over
//!     those same names and never consults a `sequences` key, so even a hand-made log record
//!     naming it delivers nothing. Arm 5 drives that;
//!   * the live-block arm of the fold cannot produce one either: `visit_model_live_blocks` walks
//!     fifteen maps and `sequences` is not among them, which is consistent with
//!     `model_kind_registry` declaring `Sequence` under `retired` with "no arm emits it". That is
//!     also why this removal cannot make a block reclaimable: a block addressed only by that map
//!     was already invisible to the liveness walk.
//!
//! # EVERY ARM HAS A CONTROL, BECAUSE A CLOSED DOOR AND AN EMPTY FIXTURE READ ALIKE
//!
//! A "nothing arrived" from a fixture that planted nothing is indistinguishable from a door that
//! is shut. So each refusal arm is paired with the same fixture delivering the same series through
//! the same door under a map the struct still has (`features`), and that must ARRIVE.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

use crate::block_store::ElementEntry;

const SHARD: ShardId = 13;
const SERIES_KEY: &str = "seq/a-series-written-before-the-fold";
const ROW_TIMESTAMP_MS: u64 = 1_700_000_000_123;

/// THE SERIES' ROWS ALONE, as JSON, built through the real value type.
///
/// This is the shape a KEY-STATE BLOB carries: `apply_key_state_field` deserializes the field's
/// value as the map's VALUE type and inserts it under the blob's `key`, so the value is the
/// series for that one key -- not a map of keys to series. A first version of the door-3 arm
/// passed the whole-map shape and its CONTROL refused to arrive, which is what caught it: the
/// retired name delivering nothing meant nothing while the live name delivered nothing either.
fn one_series_rows_as_json() -> serde_json::Value {
    let mut rows = BTreeMap::new();
    rows.insert(
        ROW_TIMESTAMP_MS,
        ElementEntry::from_parts(1, 4_096, 128, Some(3), Some(7)),
    );
    serde_json::to_value(&rows).expect("the series rows serialize")
}

/// One series, as JSON, built through the real value type.
///
/// Lifted out of a throwaway `ShardState`'s `features` map: `features` and the retired
/// `sequences` map had IDENTICAL types, so this is byte-for-byte what a pre-fold binary wrote
/// under the other name. It cannot be built by setting the field -- this binary has not got one.
fn one_legacy_series_as_json() -> serde_json::Value {
    // Built on `one_series_rows_as_json` so the two shapes this module uses -- the whole MAP for
    // a serialized index field, and the ROWS alone for a key-state blob -- cannot drift apart.
    serde_json::json!({ SERIES_KEY: one_series_rows_as_json() })
}

/// A whole index image at `stamp`, carrying the series under `field`.
///
/// `field` is the variable the paired arms differ in: `"sequences"` is the retired name this
/// binary no longer has, `"features"` the live one it does. Everything else is identical, which
/// is what makes the pair an attribution control rather than two unrelated fixtures.
fn index_image_carrying(field: &str, stamp: u32) -> Vec<u8> {
    let mut state = ShardState::default();
    state.index_format_version = stamp;
    let mut value = serde_json::to_value(&state).expect("a ShardState serializes");
    let object = value
        .as_object_mut()
        .expect("a serialized ShardState is a JSON object");
    let previous = object.insert(field.to_string(), one_legacy_series_as_json());
    if field == "sequences" {
        // THE INSERT'S RETURN IS A GUARD: `None` means this binary really has no `sequences`
        // field, so the image is the legacy-only shape these arms are about. A `Some` would mean
        // the field came back and every refusal arm below is measuring nothing.
        assert!(
            previous.is_none(),
            "this binary still serializes a `sequences` field, so the fixture is not the \
             legacy-only shape these guards are about"
        );
    } else {
        assert!(
            previous.is_some(),
            "`{field}` was not already on the wire, so this control is not the pair of the \
             refusal arm beside it"
        );
    }
    serde_json::to_vec(&value).expect("the image serializes")
}

fn engine_over_dirs(dir: &std::path::Path, index_subdir: &str) -> (TemporalEngine, std::path::PathBuf) {
    let index_dir = dir.join(index_subdir);
    std::fs::create_dir_all(&index_dir).expect("mkdir index dir");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("cache"),
        dir.join("pages"),
        &index_dir,
    );
    (engine, index_dir)
}

// =============================================================================================
// DOOR 1: the base index file.
// =============================================================================================

/// A stale-stamped base index carrying the retired map is refused before anything reads it.
#[test]
fn door_one_the_base_index_refuses_a_stale_stamp_carrying_the_retired_map() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let image = index_image_carrying("sequences", stale);
    assert!(
        String::from_utf8_lossy(&image).contains("\"sequences\""),
        "the fixture does not spell `sequences`, so this arm is about nothing"
    );
    // IT DECODES -- so the refusal below is the stamp and not a parse failure, and it is the clean
    // decode that makes the stamp the only thing standing between a reader and the dropped series.
    let decoded = crate::engine::decode_index_bytes(&image).expect(
        "a pre-fold index must still DECODE -- `ShardState` sets no `deny_unknown_fields`, which \
         is precisely why a GATE has to be what stops it",
    );
    assert_eq!(stale, decoded.index_format_version);
    assert!(
        decoded.features.is_empty(),
        "the decode folded the retired map into `features` by itself, so this fixture cannot tell \
         a refusal from a fold"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let (engine, index_dir) = engine_over_dirs(dir.path(), "indexes");
    std::fs::write(index_dir.join(format!("shard-{SHARD}.index.json")), &image).expect("plant");
    crate::engine::persistence::reset_index_load_path_counts();
    let loaded = engine
        .load_index_checked(SHARD, false)
        .expect("a refusal is Ok(None), not Err");
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    println!(
        "\n=== DOOR 1, stamp {stale} ===\n  accepted {accepted}, refused-stale {refused_stale}, \
         absent {absent}, undecodable {undecodable}"
    );
    assert!(
        loaded.is_none(),
        "the base-index door ACCEPTED a stale-stamped image carrying the retired map; the rows \
         are then dropped at the decode with no error and this deletion loses data"
    );
    assert!(refused_stale > 0, "the refusal was not counted");
}

/// The control: the same image at the current stamp reaches the load path and is accepted.
#[test]
fn door_one_control_the_same_image_at_the_current_stamp_is_accepted() {
    let image = index_image_carrying("features", SHARD_INDEX_FORMAT_VERSION);
    let dir = tempfile::tempdir().expect("tempdir");
    let (engine, index_dir) = engine_over_dirs(dir.path(), "indexes");
    std::fs::write(index_dir.join(format!("shard-{SHARD}.index.json")), &image).expect("plant");
    let state = engine
        .load_index_checked(SHARD, false)
        .expect("the load must not error")
        .expect("a current-stamped image must be ACCEPTED, or the refusal arm beside this one is \
                 about a fixture the loader never opens");
    assert!(
        state.features.contains_key(SERIES_KEY),
        "the accepted image did not carry the series, so this control is about nothing"
    );
}

// =============================================================================================
// DOOR 2: the dump manifest. THE ARM THAT FAILED BEFORE THE GATE.
// =============================================================================================

/// A real dump manifest, re-stamped, with the series under `field`.
fn a_manifest_carrying(
    field: &str,
    stamp: u32,
) -> (tempfile::TempDir, crate::engine::reports::BucketDumpManifest) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (engine, _index_dir) = engine_over_dirs(dir.path(), "indexes");
    engine.load_shard(SHARD);
    for i in 0..4 {
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id: SHARD,
            command: crate::types::Command::StringSet {
                key: format!("seed-{i}"),
                value: format!("seed-value-{i}").into_bytes(),
            },
        });
        assert!(response.status.ok, "seeding write {i}: {response:?}");
    }
    let mut manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("a dump manifest over a seeded shard");
    let mut restored = crate::engine::decode_index_bytes(&manifest.index_bytes)
        .expect("a manifest this binary wrote must decode");
    restored.index_format_version = stamp;
    let mut value = serde_json::to_value(&restored).expect("the manifest index serializes");
    let object = value.as_object_mut().expect("a JSON object");
    let previous = object.insert(field.to_string(), one_legacy_series_as_json());
    if field == "sequences" {
        assert!(previous.is_none(), "this binary still has a `sequences` field");
    } else {
        assert!(previous.is_some(), "`{field}` was not already on the wire");
    }
    // Plain serde, so the stamp set above survives: `stamp_index_format_version` exists to
    // OVERRIDE it with the current constant, which is the opposite of what this needs.
    manifest.index_bytes = serde_json::to_vec(&value).expect("the image serializes");
    manifest.index_sha256 = sha256_hex_bytes(&manifest.index_bytes);
    manifest.dump_generation_id = bucket_dump_generation_id(&manifest);
    manifest.checksum =
        bucket_dump_manifest_checksum(&manifest).expect("the manifest checksum recomputes");
    assert_eq!(
        stamp,
        crate::engine::decode_index_bytes(&manifest.index_bytes)
            .expect("the re-stamped image decodes")
            .index_format_version,
        "the re-stamped image did not keep its stamp, so a refusal arm would test an acceptance"
    );
    (dir, manifest)
}

/// THE DUMP DOOR REFUSES IT, AND REFUSES BEFORE IT CAN WRITE THE ROWS AWAY.
///
/// This arm FAILED before the stamp gate landed on this door: the install accepted a stamp-13
/// image, re-serialized the decoded state into the durable base index, and the rows -- dropped at
/// the decode because the struct no longer had the field -- were written away irrecoverably, with
/// every index-load counter at zero.
#[test]
fn door_two_the_dump_manifest_refuses_a_stale_stamp_carrying_the_retired_map() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let (dir, manifest) = a_manifest_carrying("sequences", stale);
    let (target, index_dir) = engine_over_dirs(dir.path(), "restore-indexes");

    crate::engine::persistence::reset_index_load_path_counts();
    let installed = target.install_bucket_dump_manifest(&manifest);
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    println!(
        "\n=== DOOR 2, stamp {stale} through install_bucket_dump_manifest ===\n  \
         install -> {installed:?}\n  accepted {accepted}, refused-stale {refused_stale}, absent \
         {absent}, undecodable {undecodable}"
    );
    let status = installed.expect_err(
        "THE DUMP DOOR ACCEPTED IT. This is the hole that blocked this removal once already: the \
         install re-serializes the decoded state, so a `sequences` payload the decode dropped is \
         written away with no error anywhere. The field must not be deleted while this door is \
         open",
    );
    assert_eq!("slot_dump_stale_index_stamp", status.code, "refused for some other reason: {status:?}");
    assert!(refused_stale > 0, "the refusal was not counted");
    assert!(
        !index_dir.join(format!("shard-{SHARD}.index.json")).exists(),
        "the refused install still wrote a base index, so the refusal is not before the write"
    );
}

/// The control: the same manifest at the current stamp installs and its series survives.
#[test]
fn door_two_control_the_same_manifest_at_the_current_stamp_installs() {
    let (dir, manifest) = a_manifest_carrying("features", SHARD_INDEX_FORMAT_VERSION);
    let (target, _index_dir) = engine_over_dirs(dir.path(), "restore-indexes");
    target
        .install_bucket_dump_manifest(&manifest)
        .expect("a current-stamped manifest must still install, or the refusal arm beside this \
                 one is about a manifest that could never have installed at all");
    target.load_shard(SHARD);
    let present = {
        let shards = target.shards.read().expect("engine lock poisoned");
        shards
            .get(&SHARD)
            .expect("the shard is loaded")
            .features
            .contains_key(SERIES_KEY)
    };
    println!("\n=== DOOR 2 control, stamp {SHARD_INDEX_FORMAT_VERSION} ===\n  series present after install+load: {present}");
    assert!(
        present,
        "the current-stamped manifest installed and its series did not survive, so door 2's \
         refusal is not attributable to the stamp"
    );
}

// =============================================================================================
// DOOR 3: the base index ABSENT, and the whole index log folded with no decode anywhere.
// =============================================================================================

/// THE LOG FOLD CANNOT DELIVER THE RETIRED MAP, DRIVEN AT THE LAYER THAT DECIDES.
///
/// `apply_key_states` is the key-state arm of the fold. It is called with two blobs that differ
/// only in the field name they carry, and the pair is the whole argument: the live name arrives,
/// the retired one cannot, so door 3 delivers nothing however the log was written.
///
/// ONE OBSERVATION WORTH RECORDING RATHER THAN ASSERTING AWAY: a blob naming only its key and a
/// field `apply_key_states` does not read is still treated as a key-state CAPTURE, and
/// `apply_key_state_field` reads an absent field as "this key had none" and removes the entry. So
/// such a blob does not merely fail to deliver -- it CLEARS the thirteen captured fields for that
/// key. That is printed below. It is not reachable from any writer (`capture_key_states` emits
/// only the thirteen names), and it is not what this removal changes, but it is the behaviour a
/// future field rename would meet.
#[test]
fn door_three_the_index_log_fold_cannot_deliver_the_retired_map() {
    // ARM: the retired name, with prior state under the SAME key so the clearing behaviour is
    // observable rather than inferred.
    let mut shard = ShardState::default();
    shard.features.insert(
        SERIES_KEY.into(),
        BTreeMap::from([(1u64, ElementEntry::from_parts(1, 0, 8, Some(0), Some(0)))]).into(),
    );
    assert!(
        shard.features.contains_key(SERIES_KEY),
        "DENOMINATOR: the prior state was not seeded"
    );
    crate::engine::apply_key_states(
        &mut shard,
        &[serde_json::json!({ "key": SERIES_KEY, "sequences": one_series_rows_as_json() })],
    );
    let retired_rows_arrived = shard
        .features
        .get(SERIES_KEY)
        .map(|rows| rows.contains_key(&ROW_TIMESTAMP_MS))
        .unwrap_or(false);
    let prior_state_cleared = !shard.features.contains_key(SERIES_KEY);

    // CONTROL: the live name, same blob shape, same call, same value.
    let mut control = ShardState::default();
    crate::engine::apply_key_states(
        &mut control,
        &[serde_json::json!({ "key": SERIES_KEY, "features": one_series_rows_as_json() })],
    );
    let live_rows_arrived = control
        .features
        .get(SERIES_KEY)
        .map(|rows| rows.contains_key(&ROW_TIMESTAMP_MS))
        .unwrap_or(false);

    println!(
        "
=== DOOR 3, the key-state arm of the log fold ===
  \n         a blob naming `sequences` delivered the row: {retired_rows_arrived}
  \n         a blob naming `features`  delivered the row: {live_rows_arrived}
  \n         (observation, not asserted: the `sequences` blob is still read as a CAPTURE, so it \n         cleared this key's prior `features` state: {prior_state_cleared})"
    );
    assert!(
        live_rows_arrived,
        "the CONTROL did not arrive, so this fixture never reaches the fold and the arm below \n         proves nothing: a `features` blob through `apply_key_states` must land"
    );
    assert!(
        !retired_rows_arrived,
        "a log record naming `sequences` DELIVERED a series through the fold. Door 3 is open: an \n         absent base index skips the stamp entirely, so this would arrive on a store with no \n         base file and the field must not be deleted while that is true"
    );
}

/// THE END-TO-END CONTROL FOR DOOR 3: the door really is open, and really is reached.
///
/// Arm 5 is a unit call. This drives the whole path -- a shard with an index log, its base index
/// file DELETED, reloaded -- and asserts the fold put the log's state back. Without it, arm 5
/// could be a true statement about a function nothing calls on a path nothing takes.
#[test]
fn door_three_control_an_absent_base_index_still_folds_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (engine, index_dir) = engine_over_dirs(dir.path(), "indexes");
    engine.load_shard(SHARD);
    for i in 0..4 {
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id: SHARD,
            command: crate::types::Command::StringSet {
                key: format!("logged-{i}"),
                value: format!("logged-value-{i}").into_bytes(),
            },
        });
        assert!(response.status.ok, "seeding write {i}: {response:?}");
    }
    engine.unload_shard(SHARD);

    // DELETE THE BASE INDEX. This is door 3: `load_index_inner`'s read fails, the `Err(_)` arm
    // takes `ShardState::default()` without ever reaching the stamp comparison, and the fold runs
    // from anchor 0 over the whole index log.
    let base = index_dir.join(format!("shard-{SHARD}.index.json"));
    let existed = base.exists();
    if existed {
        std::fs::remove_file(&base).expect("delete the base index");
    }
    assert!(
        existed && !base.exists(),
        "DENOMINATOR: there was no base index file to delete (existed {existed}), so this arm is \
         not driving the absent-base door"
    );

    let (reloaded, _) = engine_over_dirs(dir.path(), "indexes");
    crate::engine::persistence::reset_index_load_path_counts();
    reloaded.load_shard(SHARD);
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    let served = matches!(
        reloaded
            .execute(crate::types::ExecuteRequest {
                shard_id: SHARD,
                command: crate::types::Command::StringGet {
                    key: "logged-0".to_string(),
                },
            })
            .response,
        crate::types::CommandResponse::Bytes { value: Some(_) }
    );
    println!(
        "\n=== DOOR 3 control, base index absent ===\n  accepted {accepted}, refused-stale \
         {refused_stale}, absent {absent}, undecodable {undecodable}\n  \
         the reload served a logged write: {served}"
    );
    assert!(
        absent > 0,
        "the load did not take the ABSENT arm (accepted {accepted}, refused-stale \
         {refused_stale}, absent {absent}, undecodable {undecodable}), so this is not the door \
         arm 5 is about"
    );
    assert!(
        served,
        "with the base index deleted the reload recovered nothing, so this door delivers nothing \
         for anyone and arm 5's negative is about a path that carries no data at all"
    );
}
