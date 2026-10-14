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

// ===========================================================================================
// ARMS 3 AND 4: THE SECOND DOOR.
//
// Arms 1 and 2 above cover ONE loader. `decode_index_bytes` has six production callers and
// `persistence.rs:307` is the only site in the tree that compares `index_format_version`
// against the constant. The other five are the dump-manifest restore family
// (`bucket_dump_manifest_methods.rs:83/668/928/1437`, `lifecycle.rs:993`,
// `storage_lifecycle_methods.rs:1161`) and none of them reads the stamp. The version check
// INSIDE `decode_index_bytes` is on the msgpack-container arm only, so a plain-JSON image
// never meets it.
//
// So "the stamp refuses every index that could carry the data" is a claim about a FUNCTION
// reported as a claim about the SYSTEM. These two arms drive the other door.
// ===========================================================================================

/// One `sequences`-shaped series, as JSON, built through the real value type.
///
/// Lifted out of a throwaway `ShardState`'s `features` map: `features` and the retired
/// `sequences` map had IDENTICAL types, so this is byte-for-byte what a pre-fold binary wrote
/// under the other name. It cannot be built by setting the field -- this binary has not got one.
fn one_legacy_series_as_json() -> serde_json::Value {
    let mut probe = ShardState::default();
    let mut series = BTreeMap::new();
    series.insert(
        ROW_TIMESTAMP_MS,
        ElementEntry::from_parts(1, 4_096, 128, Some(3), Some(7)),
    );
    probe.features.insert(SERIES_KEY.to_string(), series);
    serde_json::to_value(&probe)
        .expect("a ShardState serializes")
        .as_object_mut()
        .expect("a serialized ShardState is a JSON object")
        .remove("features")
        .expect("`features` is on the wire -- it is where the series was just built")
}

/// A source shard with real pages, and a real dump manifest over it.
struct DumpFixture {
    dir: tempfile::TempDir,
    manifest: crate::engine::reports::BucketDumpManifest,
}

fn a_real_dump_manifest() -> DumpFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    for i in 0..6 {
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id: SHARD,
            command: crate::types::Command::StringSet {
                key: format!("seed-{i}"),
                value: format!("seed-value-{i}").into_bytes(),
            },
        });
        assert!(response.status.ok, "seeding write {i}: {response:?}");
    }
    let manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("a dump manifest over a seeded shard");
    // DENOMINATOR: a manifest with no index image would make both arms below vacuous.
    assert!(
        !manifest.index_bytes.is_empty(),
        "the manifest carries no index image, so there is nothing for the install to decode"
    );
    DumpFixture { dir, manifest }
}

/// Down-stamp a real manifest's index image and inject a non-empty `sequences` map into it.
///
/// Everything else about the manifest stays real -- its pages, its slab ids, its sequences --
/// so the install's validation and preflight pass for the same reasons they do in production.
/// Only the stamp and the one injected key differ, which is what makes this arm about the stamp.
fn down_stamp_and_inject_a_sequence_map(
    manifest: &mut crate::engine::reports::BucketDumpManifest,
    stamp: u32,
) {
    let mut restored = crate::engine::decode_index_bytes(&manifest.index_bytes)
        .expect("a manifest this binary wrote must decode");
    restored.index_format_version = stamp;
    let mut value = serde_json::to_value(&restored).expect("the manifest index serializes");
    let object = value
        .as_object_mut()
        .expect("a serialized ShardState is a JSON object");
    assert!(
        object
            .insert("sequences".to_string(), one_legacy_series_as_json())
            .is_none(),
        "this binary still serializes a `sequences` field, so the image is not the legacy-only \
         shape this arm is about"
    );
    // Written as plain JSON deliberately: `encode_index_bytes` goes through the struct, which
    // cannot carry a field the struct has not got, and the container arm is the ONLY arm whose
    // decode checks a version at all.
    manifest.index_bytes = serde_json::to_vec(&value).expect("the image serializes");
    manifest.index_sha256 = sha256_hex_bytes(&manifest.index_bytes);
    manifest.dump_generation_id = bucket_dump_generation_id(manifest);
    manifest.checksum =
        bucket_dump_manifest_checksum(manifest).expect("the manifest checksum recomputes");
}

/// A restore target over the SAME pages, with its own index dir and its shard NOT loaded.
///
/// Not loaded on purpose: that is the install-then-load order the comment inside
/// `install_bucket_dump_manifest` calls "the production order, reached over
/// `POST /server/storage/dumps/install`", and it takes the branch that persists the index
/// without a WAL replay -- so what lands on disk is exactly what the install decided.
fn a_restore_target(dir: &std::path::Path) -> TemporalEngine {
    let index_dir = dir.join("restore-indexes");
    std::fs::create_dir_all(&index_dir).expect("mkdir restore-indexes");
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("restore-cache"),
        dir.join("pages"),
        index_dir,
    )
}

/// The persisted base index as JSON TEXT, unwrapping the container if there is one.
///
/// WHY THIS EXISTS, AND IT IS A CORRECTION. A first version of arm 4 asked whether the persisted
/// index FILE still spelled `"sequences"` and read `false`. That was a measurement error: a shard
/// whose stamp is stale takes `serialize_index`'s `wrap_index_json` branch, so the bytes on disk
/// are zstd inside a `TSIDX` container and no key appears as text in them at all. Arm 5 caught
/// it -- the control's series had demonstrably survived and the same text search still said
/// `false`.
///
/// `decode_index_bytes` is no use for this question either: it drops a field the struct has not
/// got, which is the very thing being asked about. So the container is unwrapped by hand.
fn persisted_index_json(target: &TemporalEngine) -> String {
    let bytes =
        std::fs::read(target.index_path(SHARD)).expect("the install persisted a base index");
    if bytes.starts_with(crate::engine::INDEX_CONTAINER_MAGIC) {
        let body = &bytes[crate::engine::INDEX_CONTAINER_MAGIC.len()..];
        let (codec, payload) = body.split_first().expect("the container carries a codec byte");
        assert_eq!(
            crate::engine::INDEX_CODEC_ZSTD_JSON, *codec,
            "the persisted index uses payload codec {codec}, which this probe cannot read as text"
        );
        let json = zstd::stream::decode_all(payload).expect("the payload decompresses");
        String::from_utf8(json).expect("the payload is JSON text")
    } else {
        String::from_utf8(bytes).expect("a bare index is JSON text")
    }
}

/// ARM 3: THE DUMP-MANIFEST DOOR DOES NOT MEET THE REFUSAL.
///
/// This is the arm that refutes "the stamp refuses every index that could carry the data". A
/// stamp below the constant is installed through `install_bucket_dump_manifest` and the install
/// SUCCEEDS -- no `RefusedStaleStamp`, no error, the image reaches a persisted index.
#[test]
fn a_stale_stamped_image_installs_through_the_dump_manifest_without_meeting_the_refusal() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let mut fixture = a_real_dump_manifest();
    down_stamp_and_inject_a_sequence_map(&mut fixture.manifest, stale);
    assert_the_image_spells_the_legacy_map(&fixture.manifest.index_bytes);

    let target = a_restore_target(fixture.dir.path());
    crate::engine::persistence::reset_index_load_path_counts();
    let installed = target.install_bucket_dump_manifest(&fixture.manifest);
    let (accepted, refused_stale, absent, undecodable) =
        crate::engine::persistence::index_load_path_counts();
    println!(
        "\n=== a stamp-{stale} image through install_bucket_dump_manifest ===\n  \
         install -> {:?}\n  index-load counters -> accepted {accepted}, refused-stale \
         {refused_stale}, absent {absent}, undecodable {undecodable}",
        installed.as_ref().map(|_| "Ok(())")
    );
    installed.expect(
        "the install REFUSED a stale-stamped image -- if this is now a refusal, the dump door is \
         gated after all and the arm below is moot; check WHICH code refused before believing it",
    );
    assert_eq!(
        0, refused_stale,
        "the install counted a stale-stamp refusal, so this path DOES meet the gate at \
         persistence.rs:307 and the premise it was thought to break is intact"
    );
}

/// ARM 4: AND THE SERIES MUST SURVIVE THAT INSTALL.
///
/// THIS IS THE DATA-LOSS QUESTION, AND IT IS THE ONE THAT DECIDES WHETHER THE FIELD MAY GO.
///
/// The install decodes the manifest's image, rebuilds over it, and re-serializes the result with
/// `serialize_index` into the durable base index. With the field present, a non-empty
/// `sequences` map survives that round trip (it is `skip_serializing_if` EMPTY, and this one is
/// not empty) and `serialize_index` re-stamps the image to the current version on the way out --
/// so the next `load_index_inner` ACCEPTS it and the drain folds the rows into `features`. That
/// is a working recovery route for a pre-fold dump.
///
/// With the field deleted there is no such route: the decode drops the key, the re-serialize
/// writes it away, and the rows are gone from the only copy. So this arm asserts the SAFETY
/// PROPERTY rather than the current behaviour -- the series must still be reachable after the
/// install -- and a failure here means the deletion loses data through this door.
#[test]
fn a_sequence_series_survives_the_dump_manifest_install_and_the_load_after_it() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let mut fixture = a_real_dump_manifest();
    down_stamp_and_inject_a_sequence_map(&mut fixture.manifest, stale);
    assert_the_image_spells_the_legacy_map(&fixture.manifest.index_bytes);
    let image_in = fixture.manifest.index_bytes.clone();

    let target = a_restore_target(fixture.dir.path());
    target
        .install_bucket_dump_manifest(&fixture.manifest)
        .expect("the install must succeed -- arm 3 is what establishes that it does");

    // --- WHAT THE INSTALL PUT ON DISK, read through the container, not as raw bytes. ---
    let persisted_text = persisted_index_json(&target);
    let went_in = String::from_utf8_lossy(&image_in).contains("\"sequences\"");
    let came_out = persisted_text.contains("\"sequences\"");
    println!(
        "\n=== the series across install_bucket_dump_manifest ===\n  \
         image IN  spells \"sequences\": {went_in}\n  \
         index OUT spells \"sequences\": {came_out}  ({} chars of index JSON)",
        persisted_text.len()
    );

    // --- AND WHAT A LOAD OF THAT INDEX RECOVERS. ---
    target.load_shard(SHARD);
    let folded = {
        let shards = target.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("the shard is loaded");
        shard.features.contains_key(SERIES_KEY)
    };
    println!("  the load after it put the series into `features`: {folded}");

    // THE AUTHORITATIVE OBSERVATION IS `folded`, NOT `came_out`. Both are printed, and the
    // on-disk one is secondary deliberately: whether the FILE still spells the key says where
    // the rows went, while whether the shard holds them after a load says whether they are
    // RECOVERABLE, which is the question that decides whether the field may be deleted.
    assert!(
        folded,
        "THE SERIES DID NOT SURVIVE. A stamp-{stale} image carrying a non-empty `sequences` map \
         went into `install_bucket_dump_manifest`; the persisted base index spells `sequences`: \
         {came_out}; and the load after it recovered NOTHING into `features`. That path never \
         meets the stamp refusal (arm 3 drives it: all four index-load counters stay at zero), \
         and arm 5 shows the SAME install preserves the SAME series under `features`, so the \
         difference is the field itself. Deleting `ShardState::sequences` loses a pre-fold dump \
         manifest sequence rows silently, at the decode, with no error anywhere. DO NOT DELETE \
         THE FIELD: the dump door needs a stamp check first"
    );
}

/// ARM 5: THE ATTRIBUTION CONTROL FOR ARM 4.
///
/// Arm 4 shows a `sequences` series going into `install_bucket_dump_manifest` and not coming out.
/// On its own that is ambiguous: it could mean the install discards object-keyed series maps in
/// general -- in which case a pre-fold dump already lost these rows before this change and the
/// field's removal costs nothing -- or it could mean the install preserves such a map and the loss
/// is caused precisely by the field no longer existing to decode into.
///
/// So the SAME install is driven with the series injected under `features`, a map of the IDENTICAL
/// type that the struct still has. If that survives and `sequences` does not, the difference is
/// the field, and arm 4's loss is attributable to deleting it.
#[test]
fn the_same_install_preserves_the_same_series_under_a_map_the_struct_still_has() {
    let stale = SHARD_INDEX_FORMAT_VERSION - 1;
    let mut fixture = a_real_dump_manifest();

    // Identical to `down_stamp_and_inject_a_sequence_map`, except the key written is `features`.
    {
        let manifest = &mut fixture.manifest;
        let mut restored = crate::engine::decode_index_bytes(&manifest.index_bytes)
            .expect("a manifest this binary wrote must decode");
        restored.index_format_version = stale;
        let mut value = serde_json::to_value(&restored).expect("the manifest index serializes");
        let object = value
            .as_object_mut()
            .expect("a serialized ShardState is a JSON object");
        // REPLACED, not inserted: `features` is already on the wire, and this arm is about the
        // map's CONTENTS surviving rather than about adding an unknown key.
        assert!(
            object
                .insert("features".to_string(), one_legacy_series_as_json())
                .is_some(),
            "`features` was not already on the wire, so this is not the control arm 4 needs"
        );
        manifest.index_bytes = serde_json::to_vec(&value).expect("the image serializes");
        manifest.index_sha256 = sha256_hex_bytes(&manifest.index_bytes);
        manifest.dump_generation_id = bucket_dump_generation_id(manifest);
        manifest.checksum =
            bucket_dump_manifest_checksum(manifest).expect("the manifest checksum recomputes");
    }
    let went_in = String::from_utf8_lossy(&fixture.manifest.index_bytes)
        .contains(SERIES_KEY);
    assert!(
        went_in,
        "the control's image does not spell the series key, so it is about nothing"
    );

    let target = a_restore_target(fixture.dir.path());
    target
        .install_bucket_dump_manifest(&fixture.manifest)
        .expect("the install must succeed");

    let persisted_text = persisted_index_json(&target);
    let came_out = persisted_text.contains(SERIES_KEY);
    target.load_shard(SHARD);
    let present = {
        let shards = target.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("the shard is loaded");
        shard.features.contains_key(SERIES_KEY)
    };
    println!(
        "\n=== the SAME series under `features`, through the SAME install ===\n  \
         image IN  spells the series key: {went_in}\n  \
         index OUT spells the series key: {came_out}  ({} chars of index JSON)\n  \
         present in `features` after the load: {present}",
        persisted_text.len()
    );

    assert!(
        came_out && present,
        "the install did not preserve the series under `features` either (on disk {came_out}, \
         loaded {present}). Then arm 4's loss is NOT attributable to deleting the field -- this \
         install path discards object-keyed series maps whatever they are called, and that is a \
         separate and larger finding that has to be reported on its own terms"
    );
}
