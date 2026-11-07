// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE BUCKET-DUMP MANIFEST AND INSTALL MARKER ARE READ IN EITHER SHAPE, AND THE CHECKSUM KNOWS
//! WHICH BYTES IT WAS TAKEN OVER.
//!
//! The writers are binary and have NO JSON encode path, so every LEGACY (JSON) document below is
//! built with `serde_json` here and PLANTED straight into the directory or object store the real
//! readers use. A test that round-tripped through a writer would have had nothing left to drive
//! its JSON arm after the flip, and would quietly have become a test of the binary arm twice over.
//!
//! # WHY THE DIGEST HAS ITS OWN TESTS
//!
//! The manifest's checksum is the one thing that stops a half-written or rotted manifest from being
//! trusted as a recovery base. It used to be defined over JSON bytes. A binary manifest cannot reuse
//! that, and an existing manifest must keep verifying, so there are two derivations selected by
//! `checksum_version`. The failure to fear is a verifier that accepts a document it should not, so
//! every corruption below is made at a layer the NEXT layer accepts: the file still decodes as a
//! valid manifest, with one stored id or count changed. Flipping the last byte of a frame would
//! only prove the decoder rejects truncation, which says nothing about the digest.
#![allow(clippy::all)]
use super::*;
use crate::engine::bucket_dump_codec::{
    bucket_dump_protobuf_digest, bucket_dump_protobuf_digest_payload,
    encode_bucket_dump_install_marker, encode_bucket_dump_manifest, BUCKET_DUMP_CHECKSUM_PROTOBUF,
    BUCKET_DUMP_CONTAINER_PROTOBUF,
};
use crate::engine::reports::{
    BucketDumpInstallMarker, BucketDumpLoadVersionHandoff, BucketDumpManifest,
    BucketDumpSourceCoverage, BucketStorageSummary, StorageObjectLifecycleReport,
};
use prost::Message;

const SHARD: ShardId = 7;

/// A manifest with EVERY field populated by a value no default would produce, so a field the codec
/// drops reads back as its default and the equality assertion names it.
fn a_manifest_with_every_field_set() -> BucketDumpManifest {
    BucketDumpManifest {
        version: 3,
        shard_id: SHARD,
        manifest_id: "7-4242-1700000000123".to_string(),
        manifest_kind: "merged_slot_dump".to_string(),
        dump_generation_id: "generation-abc".to_string(),
        source_manifest_ids: vec!["7-1-100".to_string(), "7-2-200".to_string()],
        source_manifest_coverage: vec![
            BucketDumpSourceCoverage {
                manifest_id: "7-1-100".to_string(),
                bucket_ids: vec![3, 5, 8],
            },
            BucketDumpSourceCoverage {
                manifest_id: "7-2-200".to_string(),
                bucket_ids: vec![],
            },
        ],
        // Named and EMPTY: not the same as no parent, and the codec has to keep them apart.
        parent_manifest_id: Some(String::new()),
        load_version_handoff: Some(BucketDumpLoadVersionHandoff {
            previous_load_version: 11,
            next_load_version: 12,
            applied: true,
        }),
        created_unix_ms: 1_700_000_000_123,
        bucket_ids: vec![0, 3, 5, 8, 4_000_000_000],
        block_slab_ids: vec![9, 10, u64::MAX - 3],
        wal_sequence: 77,
        index_log_sequence: 4242,
        live_block_refs: 31,
        logical_bytes: 1_234_567,
        physical_bytes: 2_345_678,
        bucket_summaries: vec![
            BucketStorageSummary {
                routing_bucket: 3,
                object_count: 4,
                block_ref_count: 5,
                logical_bytes: 6,
                physical_bytes: 7,
                dirty_object_count: 8,
                dirty_generation: 9,
                last_dump_sequence: 10,
                block_slab_ids: vec![11, 12],
                // Present AND ZERO: slab 0 is a slab, and absent is a different statement.
                last_compacted_slab: Some(0),
            },
            BucketStorageSummary {
                routing_bucket: 5,
                object_count: 1,
                block_slab_ids: vec![],
                last_compacted_slab: None,
                ..BucketStorageSummary::default()
            },
        ],
        object_lifecycle: StorageObjectLifecycleReport {
            live_object_ids: 21,
            live_block_refs: 22,
            stale_object_ids: 23,
            delete_marked_object_ids: 24,
            reused_object_id_conflicts: 25,
            missing_owner_block_refs: 26,
            owner_mismatch_block_refs: 27,
            reused_object_ids: vec![101, 102],
            delete_marked_object_keys: vec!["gone/a".to_string(), "gone/b".to_string()],
        },
        index_bytes: (0..2048u32).map(|i| (i % 251) as u8).collect(),
        index_sha256: "ab".repeat(32),
        checksum_version: BUCKET_DUMP_CHECKSUM_PROTOBUF,
        checksum: String::new(),
    }
}

/// A REAL manifest over a seeded shard, plus the engine and index dir it was written into.
struct Real {
    _dir: tempfile::TempDir,
    engine: TemporalEngine,
    manifest: BucketDumpManifest,
}

fn a_real_manifest() -> Real {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
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
    assert!(
        !manifest.index_bytes.is_empty() && !manifest.bucket_summaries.is_empty(),
        "the fixture manifest is empty, so every assertion below would be about nothing"
    );
    Real {
        _dir: dir,
        engine,
        manifest,
    }
}

/// `m`, re-filed as a distinct BINARY-digest manifest: new id, later in the index log, and a
/// version-1 checksum computed over its protobuf encoding.
fn as_a_binary_digest_manifest(m: &BucketDumpManifest, suffix: &str) -> BucketDumpManifest {
    let mut planted = m.clone();
    planted.manifest_id = format!("{}-{suffix}", m.manifest_id);
    planted.index_log_sequence = m.index_log_sequence + 1;
    resign(&mut planted);
    planted
}

/// Make `m` a self-consistent version-1 manifest again after a field was edited: the generation id
/// hashes the sequences and the checksum covers the generation id, so both are recomputed, in that
/// order. Without it `validate_bucket_dump_manifest` would fail on the generation, and a test
/// asserting a clean validation would be about the fixture.
fn resign(m: &mut BucketDumpManifest) {
    m.checksum_version = BUCKET_DUMP_CHECKSUM_PROTOBUF;
    m.dump_generation_id = bucket_dump_generation_id(m);
    m.checksum = bucket_dump_protobuf_digest(m).0;
}

/// `m`, re-filed as a LEGACY manifest: new id, a version-0 checksum taken over its canonical JSON.
/// The generation id is recomputed first because it hashes the sequences and the checksum covers it.
fn as_a_legacy_json_manifest(m: &BucketDumpManifest, suffix: &str) -> BucketDumpManifest {
    let mut legacy = m.clone();
    legacy.manifest_id = format!("{}-{suffix}", m.manifest_id);
    legacy.index_log_sequence = m.index_log_sequence + 1;
    crate::engine::bucket_dump_codec::into_legacy_json_manifest_for_test(legacy)
}

/// Write `m` into the manifest directory as the JSON older builds wrote.
fn plant_json(engine: &TemporalEngine, m: &BucketDumpManifest) {
    let path = bucket_dump_manifest_path(&engine.index_dir, m.shard_id, &m.manifest_id);
    let bytes = serde_json::to_vec(m).expect("the JSON shape must still build");
    assert_eq!(
        bytes[0], b'{',
        "the planted legacy manifest is not the JSON shape, so the arm kept for the installed base \
         is being driven on the wrong bytes"
    );
    std::fs::write(&path, bytes).expect("plant the legacy manifest");
    assert_eq!(
        std::fs::read(&path).unwrap()[0],
        b'{',
        "the plant did not take, so nothing below reads a JSON manifest"
    );
}

fn plant_binary(engine: &TemporalEngine, m: &BucketDumpManifest) -> std::path::PathBuf {
    let path = bucket_dump_manifest_path(&engine.index_dir, m.shard_id, &m.manifest_id);
    let bytes = encode_bucket_dump_manifest(m);
    assert_eq!(
        bytes[0], BUCKET_DUMP_CONTAINER_PROTOBUF,
        "the planted document is not the binary shape"
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&bytes).is_err(),
        "the planted document parses as JSON, so the binary arm is not what is being driven"
    );
    std::fs::write(&path, bytes).expect("plant the binary manifest");
    path
}

fn listed_ids(engine: &TemporalEngine) -> Vec<String> {
    let mut ids = engine
        .list_bucket_dump_manifests(SHARD)
        .into_iter()
        .map(|m| m.manifest_id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

/// BOTH SHAPES CARRY EVERY FIELD, AND THE STREAMED DIGEST PAYLOAD IS PROST'S OWN ENCODING.
///
/// The second half is the claim the zero-copy digest rests on: the payload is assembled from a head
/// plus a hand-written index-image field so the whole-shard image is never copied into a message,
/// and that is only a digest "over the deterministic encoding" if the assembled bytes ARE what prost
/// writes for the message. The comparison is against prost encoding the DECODED message, which is
/// code the digest path does not call.
#[test]
fn the_manifest_codec_reads_both_shapes_and_the_digest_payload_is_the_encoding() {
    let mut original = a_manifest_with_every_field_set();
    original.checksum = bucket_dump_protobuf_digest(&original).0;

    // ---- JSON: the shape every manifest written before this had.
    let json = serde_json::to_vec(&original).unwrap();
    assert_eq!(json[0], b'{', "the JSON fixture is not JSON");
    let from_json = crate::engine::bucket_dump_codec::decode_bucket_dump_manifest(&json)
        .expect("the JSON shape must still decode");
    assert_eq!(from_json, original, "the JSON arm changed the manifest");

    // ---- binary.
    let binary = encode_bucket_dump_manifest(&original);
    assert_eq!(binary[0], BUCKET_DUMP_CONTAINER_PROTOBUF);
    assert!(serde_json::from_slice::<serde_json::Value>(&binary).is_err());
    let from_binary = crate::engine::bucket_dump_codec::decode_bucket_dump_manifest(&binary)
        .expect("the binary shape must decode");
    assert_eq!(
        from_binary, original,
        "the binary arm dropped or changed a field"
    );

    // Named-and-empty parent, present-and-zero slab, and an absent compaction slab all survive.
    assert_eq!(from_binary.parent_manifest_id, Some(String::new()));
    assert_eq!(from_binary.bucket_summaries[0].last_compacted_slab, Some(0));
    assert_eq!(from_binary.bucket_summaries[1].last_compacted_slab, None);

    // ---- an all-zero lifecycle report is absent on the wire and zero when read.
    let mut zero_lifecycle = original.clone();
    zero_lifecycle.object_lifecycle = StorageObjectLifecycleReport::default();
    let back = crate::engine::bucket_dump_codec::decode_bucket_dump_manifest(
        &encode_bucket_dump_manifest(&zero_lifecycle),
    )
    .unwrap();
    assert_eq!(back.object_lifecycle, StorageObjectLifecycleReport::default());

    // ---- the digest payload equals prost's encoding of the whole message, checksum empty.
    let mut unsigned = original.clone();
    unsigned.checksum.clear();
    let payload = bucket_dump_protobuf_digest_payload(&unsigned);
    let whole = crate::sdk::v1::DurableBucketDumpManifest::decode(
        &encode_bucket_dump_manifest(&unsigned)[1..],
    )
    .expect("an unsigned binary manifest decodes as the message")
    .encode_to_vec();
    assert_eq!(
        payload, whole,
        "the streamed digest payload is not the encoding of the message; a field was added above \
         the index image's tag, or the head and the hand-written field disagree"
    );
    assert!(
        payload.len() > unsigned.index_bytes.len(),
        "the payload is shorter than the index image, so the image was not hashed"
    );
    let (digest, hashed) = bucket_dump_protobuf_digest(&unsigned);
    assert_eq!(hashed as usize, payload.len());
    assert_eq!(digest.len(), 64);
}

/// EACH DERIVATION VERIFIES ITS OWN MANIFESTS AND NEITHER VERIFIES THE OTHER'S.
///
/// A real manifest, whose checksum was taken over its JSON by the writer that runs today. The same
/// document re-versioned to 1 must NOT verify against that checksum (the bytes differ), and with a
/// checksum recomputed under version 1 it must verify -- and then must not verify if relabelled 0.
/// A version this build does not know is an error, not a default.
#[test]
fn each_checksum_version_verifies_its_own_manifests_and_never_the_others() {
    let real = a_real_manifest();
    assert_eq!(
        real.manifest.checksum_version, BUCKET_DUMP_CHECKSUM_PROTOBUF,
        "the writer must mint version-1 manifests: a new manifest on the JSON derivation would \
         keep that verifier on the write path for ever"
    );
    real.engine
        .validate_bucket_dump_manifest(&real.manifest)
        .expect("a manifest the writer just minted must verify under the protobuf derivation");
    // The installed base: a manifest whose checksum was taken over its canonical JSON.
    let legacy = as_a_legacy_json_manifest(&real.manifest, "legacy");
    assert_eq!(legacy.checksum_version, 0);
    assert_ne!(
        legacy.checksum, real.manifest.checksum,
        "the two derivations agree on this manifest, so the checks below could not tell them apart"
    );
    real.engine
        .validate_bucket_dump_manifest(&legacy)
        .expect("a legacy manifest must keep verifying under the JSON derivation");

    // Same document, labelled version 1, checksum NOT recomputed: must fail, and the failure is
    // specifically the checksum.
    let mut relabelled = legacy.clone();
    relabelled.checksum_version = BUCKET_DUMP_CHECKSUM_PROTOBUF;
    assert_eq!(
        real.engine
            .validate_bucket_dump_manifest(&relabelled)
            .unwrap_err()
            .code,
        "slot_dump_checksum_mismatch",
        "a version-1 label over a JSON-derived checksum verified"
    );

    // Recomputed under version 1: verifies. The two checksums differ, or the test is vacuous.
    let binary = as_a_binary_digest_manifest(&legacy, "v1");
    assert_ne!(binary.checksum, legacy.checksum);
    assert_ne!(binary.manifest_id, legacy.manifest_id);
    real.engine
        .validate_bucket_dump_manifest(&binary)
        .expect("a version-1 manifest must verify under the protobuf derivation");

    // The version-1 checksum, carried by a manifest relabelled 0: the JSON derivation runs and
    // does not land on it.
    let mut downgraded = binary.clone();
    downgraded.checksum_version = 0;
    assert_eq!(
        real.engine
            .validate_bucket_dump_manifest(&downgraded)
            .unwrap_err()
            .code,
        "slot_dump_checksum_mismatch",
        "a version-0 label over a protobuf-derived checksum verified"
    );

    // An unknown version is refused outright.
    let mut unknown = binary.clone();
    unknown.checksum_version = 7;
    assert!(
        bucket_dump_manifest_checksum(&unknown).is_err(),
        "an unknown checksum version produced a digest instead of an error"
    );
    assert!(real.engine.validate_bucket_dump_manifest(&unknown).is_err());
}

/// A BINARY MANIFEST IS LISTED BESIDE A JSON ONE, AND A CHANGED ID OR COUNT IS NOT TRUSTED.
///
/// Driven through the real directory listing, `bucket_dump_manifest_at` and the recovery picker,
/// because those are what a running node uses. The corruptions decode cleanly -- one stored id and
/// one stored count each change -- so the only thing that can refuse them is the digest.
#[test]
fn a_binary_manifest_is_listed_beside_a_json_one_and_a_changed_field_is_not_trusted() {
    // The listing memo is keyed on file identity; this test rewrites files, so run the control arm.
    let _no_memo = crate::engine::bucket_dump_io::ManifestParseMemoOff::new();
    let real = a_real_manifest();
    let written_id = real.manifest.manifest_id.clone();

    // ---- what the writer produced is BINARY and is listed.
    let on_disk = std::fs::read(bucket_dump_manifest_path(
        &real.engine.index_dir,
        SHARD,
        &written_id,
    ))
    .unwrap();
    assert_eq!(
        on_disk[0], BUCKET_DUMP_CONTAINER_PROTOBUF,
        "the manifest writer did not write the binary shape"
    );
    assert!(serde_json::from_slice::<serde_json::Value>(&on_disk).is_err());
    assert_eq!(listed_ids(&real.engine), vec![written_id.clone()]);

    // ---- the INSTALLED BASE: a JSON manifest planted beside it, because no writer produces one.
    let legacy = as_a_legacy_json_manifest(&real.manifest, "legacy");
    plant_json(&real.engine, &legacy);
    let mut base = vec![written_id.clone(), legacy.manifest_id.clone()];
    base.sort();
    assert_eq!(
        listed_ids(&real.engine),
        base,
        "the planted JSON manifest was not listed beside the binary one"
    );
    let legacy_back = bucket_dump_manifest_at(&real.engine.index_dir, SHARD, &legacy.manifest_id)
        .unwrap()
        .unwrap();
    assert_eq!(legacy_back, legacy, "the JSON arm changed the legacy manifest");

    // ---- a binary manifest beside it. Its wal_sequence is a one-byte varint (40), so a change
    // ---- to it is a one-byte change that still decodes.
    let mut binary = as_a_binary_digest_manifest(&real.manifest, "bin");
    binary.wal_sequence = 40;
    resign(&mut binary);
    let path = plant_binary(&real.engine, &binary);

    let mut want = base.clone();
    want.push(binary.manifest_id.clone());
    want.sort();
    assert_eq!(
        listed_ids(&real.engine),
        want,
        "the binary manifest was not listed beside the JSON one"
    );
    let listed = real
        .engine
        .list_bucket_dump_manifests(SHARD)
        .into_iter()
        .find(|m| m.manifest_id == binary.manifest_id)
        .unwrap();
    assert_eq!(listed, binary, "the listing changed the binary manifest");
    assert_eq!(
        bucket_dump_manifest_at(&real.engine.index_dir, SHARD, &binary.manifest_id)
            .unwrap()
            .unwrap(),
        binary,
        "bucket_dump_manifest_at disagrees with the listing"
    );

    // ---- the binary manifest with ONE COUNT changed. Locate the byte by encoding the unsigned
    // ---- manifest at two adjacent values and taking the one position that differs.
    let at = |wal: u64| {
        let mut m = binary.clone();
        m.wal_sequence = wal;
        m.checksum.clear();
        encode_bucket_dump_manifest(&m)
    };
    let (a, b) = (at(40), at(41));
    assert_eq!(a.len(), b.len());
    let differing = (0..a.len()).filter(|&i| a[i] != b[i]).collect::<Vec<_>>();
    assert_eq!(
        differing.len(),
        1,
        "the count is not a single byte on the wire, so the flip below is not what it claims"
    );
    let mut counted = std::fs::read(&path).unwrap();
    assert_eq!(counted[differing[0]], 40);
    counted[differing[0]] = 41;
    std::fs::write(&path, &counted).unwrap();
    assert!(
        crate::engine::bucket_dump_codec::decode_bucket_dump_manifest(&counted)
            .is_ok_and(|m| m.wal_sequence == 41),
        "the corrupted file no longer decodes, so only the decoder is being tested"
    );
    assert_eq!(
        listed_ids(&real.engine),
        base.clone(),
        "a manifest whose stored count changed was still trusted"
    );

    // ---- CONTROL: the same bytes with the count put back list again, so the exclusion above was
    // ---- the changed count and not the fixture.
    counted[differing[0]] = 40;
    std::fs::write(&path, &counted).unwrap();
    assert_eq!(listed_ids(&real.engine), want);

    // ---- one STORED ID changed: the manifest id's last character, still valid UTF-8.
    let mut id_bytes = std::fs::read(&path).unwrap();
    let needle = binary.manifest_id.as_bytes();
    let pos = id_bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("the manifest id is in the file");
    let last = pos + needle.len() - 1;
    assert!(id_bytes[last].is_ascii_alphanumeric());
    id_bytes[last] = if id_bytes[last] == b'z' { b'y' } else { b'z' };
    std::fs::write(&path, &id_bytes).unwrap();
    assert!(
        crate::engine::bucket_dump_codec::decode_bucket_dump_manifest(&id_bytes)
            .is_ok_and(|m| m.manifest_id != binary.manifest_id),
        "the id-corrupted file no longer decodes, so only the decoder is being tested"
    );
    assert_eq!(
        listed_ids(&real.engine),
        base.clone(),
        "a manifest whose stored id changed was still trusted"
    );
    std::fs::write(&path, encode_bucket_dump_manifest(&binary)).unwrap();
    assert_eq!(listed_ids(&real.engine), want);

    // ---- one byte INSIDE THE INDEX IMAGE, which is the field the digest streams instead of
    // ---- encoding and so the one a shortcut would most plausibly leave out of the hash.
    let mut image = encode_bucket_dump_manifest(&binary);
    let image_head = &binary.index_bytes[..32];
    let start = image
        .windows(image_head.len())
        .position(|w| w == image_head)
        .expect("the index image is in the file");
    image[start + 100] ^= 0x01;
    std::fs::write(&path, &image).unwrap();
    assert!(
        crate::engine::bucket_dump_codec::decode_bucket_dump_manifest(&image)
            .is_ok_and(|m| m.index_bytes != binary.index_bytes),
        "the image-corrupted file no longer decodes, so only the decoder is being tested"
    );
    assert_eq!(
        listed_ids(&real.engine),
        base.clone(),
        "a manifest whose index image changed by one bit was still trusted"
    );
    std::fs::write(&path, encode_bucket_dump_manifest(&binary)).unwrap();
    assert_eq!(listed_ids(&real.engine), want);

    // ---- an unknown checksum version is dropped from the listing, not defaulted.
    let mut unknown = binary.clone();
    unknown.checksum_version = 9;
    std::fs::write(&path, encode_bucket_dump_manifest(&unknown)).unwrap();
    assert_eq!(
        listed_ids(&real.engine),
        base,
        "a manifest with an unknown checksum version was listed"
    );

    // ---- the recovery picker reads the same shapes: the binary manifest has the HIGHER wal
    // ---- anchor once its count is put back above the legacy one's.
    let mut high = as_a_binary_digest_manifest(&real.manifest, "high");
    high.wal_sequence = real.manifest.wal_sequence + 1_000;
    resign(&mut high);
    plant_binary(&real.engine, &high);
    let picked = durable_recovery_bucket_dump_manifest_at(&real.engine.index_dir, SHARD)
        .expect("a manifest to recover from");
    assert_eq!(
        picked.manifest_id, high.manifest_id,
        "recovery did not pick the binary manifest with the highest WAL anchor"
    );
}

/// AN INSTALL MARKER IS WRITTEN BINARY AND READ IN EITHER SHAPE.
///
/// The writer's marker is binary; the JSON one is the installed base and is PLANTED. Their values
/// are disjoint, so each can only have come from its own arm.
#[test]
fn an_install_marker_is_read_in_either_shape() {
    let real = a_real_manifest();
    real.engine
        .persist_bucket_dump_install_marker(&real.manifest, "prepare")
        .unwrap();
    let files = bucket_dump_install_marker_files_at(&real.engine.index_dir, SHARD).unwrap();
    assert_eq!(files.len(), 1);
    let written = std::fs::read(&files[0].1).unwrap();
    assert_eq!(
        written[0], BUCKET_DUMP_CONTAINER_PROTOBUF,
        "the marker writer did not write the binary shape"
    );
    assert!(serde_json::from_slice::<serde_json::Value>(&written).is_err());
    assert_eq!(files[0].0.manifest_id, real.manifest.manifest_id);
    assert_eq!(files[0].0.phase, "prepare");

    let legacy = BucketDumpInstallMarker {
        shard_id: SHARD,
        manifest_id: "planted-legacy-manifest".to_string(),
        phase: "install".to_string(),
        wal_sequence: 9_001,
        index_log_sequence: 9_002,
        created_unix_ms: 1_999_999_999_999,
    };
    let bytes = serde_json::to_vec_pretty(&legacy).unwrap();
    assert_eq!(bytes[0], b'{', "the planted marker is not the JSON shape");
    let path = bucket_dump_install_marker_path(&real.engine.index_dir, &legacy);
    std::fs::write(&path, bytes).unwrap();
    assert_eq!(std::fs::read(&path).unwrap()[0], b'{', "the plant did not take");

    let markers = list_bucket_dump_install_markers_at(&real.engine.index_dir, SHARD).unwrap();
    assert_eq!(markers.len(), 2, "the JSON marker was not listed: {markers:?}");
    assert!(
        markers.contains(&legacy),
        "the JSON marker was read as something else: {markers:?}"
    );
    assert!(markers.iter().any(|m| m.manifest_id == real.manifest.manifest_id));
}

/// NO JSON ENCODE PATH IS LEFT FOR EITHER WRITER, AND THE FILES THEY WRITE ARE NOT JSON.
///
/// The behavioural half is in the tests above (they assert the first byte of what the real writers
/// put on disk). This is the half that sees an encoder nobody calls YET: a retired format comes
/// back when a later branch finds the function and calls it, and the first byte of today's output
/// says nothing about that. The scan looks at each writer function's own code with comment lines
/// removed, because a doc comment naming `serde_json` is not a call to it, and it is anchored on the
/// function's declaration up to the next one.
///
/// The POSITIVE CONTROL is the checksum verifier, which does still call `serde_json` (it is the
/// legacy verifier). A scan that cannot see that call would pass over any writer.
#[test]
fn no_json_encode_path_is_left_for_a_manifest_or_a_marker() {
    fn body(source: &'static str, from: &str, to: &str) -> String {
        let start = source
            .find(from)
            .unwrap_or_else(|| panic!("no declaration {from}"));
        let rest = &source[start..];
        let end = rest[from.len()..]
            .find(to)
            .unwrap_or_else(|| panic!("no declaration {to} after {from}"))
            + from.len();
        rest[..end]
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    let persistence = include_str!("../engine_persistence.rs");
    let io = include_str!("../bucket_dump_io.rs");
    let shared = include_str!("../../shared_store.rs");

    let writers = [
        (
            "persist_bucket_dump_manifest",
            body(
                persistence,
                "fn persist_bucket_dump_manifest(",
                "fn persist_bucket_dump_install_marker(",
            ),
            "encode_bucket_dump_manifest(",
        ),
        (
            "write_bucket_dump_install_marker",
            body(
                io,
                "fn write_bucket_dump_install_marker(",
                "fn bucket_dump_install_marker_files_at(",
            ),
            "encode_bucket_dump_install_marker(",
        ),
        (
            "publish_bucket_dump_manifests",
            body(
                shared,
                "pub async fn publish_bucket_dump_manifests(",
                "pub async fn restore_bucket_dump_manifests(",
            ),
            "encode_bucket_dump_manifest(",
        ),
    ];
    // DENOMINATOR: three writers, each located and each calling the binary encoder.
    assert_eq!(writers.len(), 3);
    for (name, code, encoder) in &writers {
        assert!(
            code.len() > 100,
            "{name}: the scan found almost nothing, so it proves nothing"
        );
        assert!(
            code.contains(encoder),
            "{name} does not call {encoder}, so it is not the writer this scan thinks it is"
        );
        assert!(
            !code.contains("serde_json"),
            "{name} still reaches for serde_json: a JSON encode path survives for a manifest or \
             marker. Delete it; the decoder keeps its JSON arm for the files already on disk."
        );
    }

    let verifier = body(
        io,
        "fn finish_bucket_dump_manifest_checksum(",
        "pub(super) fn bucket_dump_fault_scenario(",
    );
    assert!(
        verifier.contains("serde_json::to_vec"),
        "the control no longer sees the legacy verifier's serde_json call, so this scan could pass \
         over a writer it cannot see"
    );
}
