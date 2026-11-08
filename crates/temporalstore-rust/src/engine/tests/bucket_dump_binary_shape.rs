// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE BUCKET-DUMP MANIFEST AND INSTALL MARKER ARE BARE PROTOBUF, AND THE CHECKSUM COVERS THE
//! ENCODING.
//!
//! There is one shape and one digest. The first release carries no installed base, so no test here
//! plants an older shape to prove it still loads: the JSON document the writers used to produce is
//! the thing that must now be REFUSED.
//!
//! # WHY THE DIGEST HAS ITS OWN TESTS
//!
//! The manifest's checksum is the one thing that stops a half-written or rotted manifest from being
//! trusted as a recovery base. The failure to fear is a verifier that accepts a document it should
//! not, so every corruption below is made at a layer the NEXT layer accepts: the file still decodes
//! as a valid manifest, with one stored id or count changed. Flipping the last byte of a frame would
//! only prove the decoder rejects truncation, which says nothing about the digest.
#![allow(clippy::all)]
use super::*;
use crate::engine::bucket_dump_codec::{
    bucket_dump_protobuf_digest, bucket_dump_protobuf_digest_payload,
    decode_bucket_dump_install_marker, decode_bucket_dump_manifest, encode_bucket_dump_install_marker,
    encode_bucket_dump_manifest,
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

/// `m`, re-filed as a distinct manifest: new id, later in the index log, and a checksum computed
/// over its protobuf encoding.
fn as_a_distinct_manifest(m: &BucketDumpManifest, suffix: &str) -> BucketDumpManifest {
    let mut planted = m.clone();
    planted.manifest_id = format!("{}-{suffix}", m.manifest_id);
    planted.index_log_sequence = m.index_log_sequence + 1;
    resign(&mut planted);
    planted
}

/// Make `m` a self-consistent manifest again after a field was edited: the generation id hashes the
/// sequences and the checksum covers the generation id, so both are recomputed, in that order.
/// Without it `validate_bucket_dump_manifest` would fail on the generation, and a test asserting a
/// clean validation would be about the fixture.
fn resign(m: &mut BucketDumpManifest) {
    m.dump_generation_id = bucket_dump_generation_id(m);
    m.checksum = bucket_dump_protobuf_digest(m).0;
}

fn plant(engine: &TemporalEngine, m: &BucketDumpManifest) -> std::path::PathBuf {
    let path = bucket_dump_manifest_path(&engine.index_dir, m.shard_id, &m.manifest_id);
    let bytes = encode_bucket_dump_manifest(m);
    assert!(
        serde_json::from_slice::<serde_json::Value>(&bytes).is_err(),
        "the planted document parses as JSON, so the protobuf shape is not what is being driven"
    );
    std::fs::write(&path, bytes).expect("plant the manifest");
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

/// THE CODEC CARRIES EVERY FIELD, REFUSES EVERYTHING ELSE, AND THE STREAMED DIGEST PAYLOAD IS
/// PROST'S OWN ENCODING.
///
/// The last claim is the one the zero-copy digest rests on: the payload is assembled from a head
/// plus a hand-written index-image field so the whole-shard image is never copied into a message,
/// and that is only a digest "over the deterministic encoding" if the assembled bytes ARE what prost
/// writes for the message. The comparison is against prost encoding the DECODED message, which is
/// code the digest path does not call.
#[test]
fn the_manifest_codec_reads_protobuf_refuses_everything_else_and_the_digest_payload_is_the_encoding() {
    let mut original = a_manifest_with_every_field_set();
    original.checksum = bucket_dump_protobuf_digest(&original).0;

    let binary = encode_bucket_dump_manifest(&original);
    assert!(serde_json::from_slice::<serde_json::Value>(&binary).is_err());
    let from_binary = decode_bucket_dump_manifest(&binary).expect("the manifest must decode");
    assert_eq!(from_binary, original, "the codec dropped or changed a field");

    // Named-and-empty parent, present-and-zero slab, and an absent compaction slab all survive.
    assert_eq!(from_binary.parent_manifest_id, Some(String::new()));
    assert_eq!(from_binary.bucket_summaries[0].last_compacted_slab, Some(0));
    assert_eq!(from_binary.bucket_summaries[1].last_compacted_slab, None);

    // ---- an all-zero lifecycle report is absent on the wire and zero when read.
    let mut zero_lifecycle = original.clone();
    zero_lifecycle.object_lifecycle = StorageObjectLifecycleReport::default();
    let back = decode_bucket_dump_manifest(&encode_bucket_dump_manifest(&zero_lifecycle)).unwrap();
    assert_eq!(back.object_lifecycle, StorageObjectLifecycleReport::default());

    // ---- everything below is a REFUSAL.
    let refused = |bytes: &[u8], what: &str| {
        let error = decode_bucket_dump_manifest(bytes)
            .expect_err(&format!("{what} was read instead of refused"));
        assert!(!error.to_string().is_empty(), "{what}: the refusal says nothing");
    };
    // THE JSON DOCUMENT THE WRITERS USED TO PRODUCE IS NOT A MANIFEST ANY MORE.
    refused(&serde_json::to_vec(&original).unwrap(), "the old JSON manifest");
    refused(b"", "an empty body");
    refused(b"not a manifest", "plain text");
    refused(&binary[..binary.len() / 2], "a truncated manifest");
    // A message of only an unknown field decodes to a default manifest naming nothing: damage.
    refused(&[0x78, 0x01], "a message of only an unknown field");

    // ---- the digest payload equals prost's encoding of the whole message, checksum empty.
    let mut unsigned = original.clone();
    unsigned.checksum.clear();
    let payload = bucket_dump_protobuf_digest_payload(&unsigned);
    let whole = crate::sdk::v1::DurableBucketDumpManifest::decode(
        encode_bucket_dump_manifest(&unsigned).as_slice(),
    )
    .expect("an unsigned manifest decodes as the message")
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

/// A MANIFEST THE WRITER MINTED VERIFIES, AND ONE CHANGED FIELD DOES NOT.
///
/// A real manifest, whose checksum was taken by the writer that runs today. Re-filing it under a new
/// id with a recomputed checksum verifies; the same document with one field edited and the checksum
/// NOT recomputed fails, and the failure is specifically the checksum.
#[test]
fn a_manifest_verifies_under_the_one_digest_and_a_changed_field_does_not() {
    let real = a_real_manifest();
    real.engine
        .validate_bucket_dump_manifest(&real.manifest)
        .expect("a manifest the writer just minted must verify");
    let refiled = as_a_distinct_manifest(&real.manifest, "refiled");
    assert_ne!(refiled.checksum, real.manifest.checksum);
    real.engine
        .validate_bucket_dump_manifest(&refiled)
        .expect("a re-signed manifest must verify");

    let mut tampered = refiled.clone();
    tampered.logical_bytes += 1;
    assert_eq!(
        real.engine
            .validate_bucket_dump_manifest(&tampered)
            .unwrap_err()
            .code,
        "slot_dump_checksum_mismatch",
        "a manifest with one changed field and the old checksum verified"
    );
}

/// A MANIFEST IS LISTED, A JSON ONE IS NOT, AND A CHANGED ID OR COUNT IS NOT TRUSTED.
///
/// Driven through the real directory listing, `bucket_dump_manifest_at` and the recovery picker,
/// because those are what a running node uses. The corruptions decode cleanly -- one stored id and
/// one stored count each change -- so the only thing that can refuse them is the digest.
#[test]
fn a_manifest_is_listed_a_json_one_is_not_and_a_changed_field_is_not_trusted() {
    // The listing memo is keyed on file identity; this test rewrites files, so run the control arm.
    let _no_memo = crate::engine::bucket_dump_io::ManifestParseMemoOff::new();
    let real = a_real_manifest();
    let written_id = real.manifest.manifest_id.clone();

    // ---- what the writer produced is a protobuf file named for what it is, and is listed.
    let path_written = bucket_dump_manifest_path(&real.engine.index_dir, SHARD, &written_id);
    assert_eq!(path_written.extension().and_then(|e| e.to_str()), Some("manifest"));
    let on_disk = std::fs::read(&path_written).unwrap();
    assert_eq!(on_disk, encode_bucket_dump_manifest(&real.manifest));
    assert!(serde_json::from_slice::<serde_json::Value>(&on_disk).is_err());
    let base = vec![written_id.clone()];
    assert_eq!(listed_ids(&real.engine), base);

    // ---- the JSON document the writers used to produce, planted beside it: NOT listed.
    let mut json_manifest = as_a_distinct_manifest(&real.manifest, "json");
    resign(&mut json_manifest);
    let json_path =
        bucket_dump_manifest_path(&real.engine.index_dir, SHARD, &json_manifest.manifest_id);
    std::fs::write(&json_path, serde_json::to_vec(&json_manifest).unwrap()).unwrap();
    assert_eq!(std::fs::read(&json_path).unwrap()[0], b'{', "the plant did not take");
    assert_eq!(
        listed_ids(&real.engine),
        base,
        "a JSON manifest was listed: the JSON arm is back"
    );
    assert!(
        bucket_dump_manifest_at(&real.engine.index_dir, SHARD, &json_manifest.manifest_id).is_err(),
        "bucket_dump_manifest_at read a JSON manifest"
    );
    std::fs::remove_file(&json_path).unwrap();

    // ---- a second manifest beside it. Its wal_sequence is a one-byte varint (40), so a change to
    // ---- it is a one-byte change that still decodes.
    let mut second = as_a_distinct_manifest(&real.manifest, "bin");
    second.wal_sequence = 40;
    resign(&mut second);
    let path = plant(&real.engine, &second);

    let mut want = base.clone();
    want.push(second.manifest_id.clone());
    want.sort();
    assert_eq!(
        listed_ids(&real.engine),
        want,
        "the planted manifest was not listed beside the written one"
    );
    let listed = real
        .engine
        .list_bucket_dump_manifests(SHARD)
        .into_iter()
        .find(|m| m.manifest_id == second.manifest_id)
        .unwrap();
    assert_eq!(listed, second, "the listing changed the manifest");
    assert_eq!(
        bucket_dump_manifest_at(&real.engine.index_dir, SHARD, &second.manifest_id)
            .unwrap()
            .unwrap(),
        second,
        "bucket_dump_manifest_at disagrees with the listing"
    );

    // ---- the manifest with ONE COUNT changed. Locate the byte by encoding the unsigned manifest at
    // ---- two adjacent values and taking the one position that differs.
    let at = |wal: u64| {
        let mut m = second.clone();
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
        decode_bucket_dump_manifest(&counted).is_ok_and(|m| m.wal_sequence == 41),
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
    let needle = second.manifest_id.as_bytes();
    let pos = id_bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("the manifest id is in the file");
    let last = pos + needle.len() - 1;
    assert!(id_bytes[last].is_ascii_alphanumeric());
    id_bytes[last] = if id_bytes[last] == b'z' { b'y' } else { b'z' };
    std::fs::write(&path, &id_bytes).unwrap();
    assert!(
        decode_bucket_dump_manifest(&id_bytes).is_ok_and(|m| m.manifest_id != second.manifest_id),
        "the id-corrupted file no longer decodes, so only the decoder is being tested"
    );
    assert_eq!(
        listed_ids(&real.engine),
        base.clone(),
        "a manifest whose stored id changed was still trusted"
    );
    std::fs::write(&path, encode_bucket_dump_manifest(&second)).unwrap();
    assert_eq!(listed_ids(&real.engine), want);

    // ---- one byte INSIDE THE INDEX IMAGE, which is the field the digest streams instead of
    // ---- encoding and so the one a shortcut would most plausibly leave out of the hash.
    let mut image = encode_bucket_dump_manifest(&second);
    let image_head = &second.index_bytes[..32];
    let start = image
        .windows(image_head.len())
        .position(|w| w == image_head)
        .expect("the index image is in the file");
    image[start + 100] ^= 0x01;
    std::fs::write(&path, &image).unwrap();
    assert!(
        decode_bucket_dump_manifest(&image).is_ok_and(|m| m.index_bytes != second.index_bytes),
        "the image-corrupted file no longer decodes, so only the decoder is being tested"
    );
    assert_eq!(
        listed_ids(&real.engine),
        base,
        "a manifest whose index image changed by one bit was still trusted"
    );
    std::fs::write(&path, encode_bucket_dump_manifest(&second)).unwrap();
    assert_eq!(listed_ids(&real.engine), want);

    // ---- the recovery picker reads the same files: the manifest with the HIGHER wal anchor wins.
    let mut high = as_a_distinct_manifest(&real.manifest, "high");
    high.wal_sequence = real.manifest.wal_sequence + 1_000;
    resign(&mut high);
    plant(&real.engine, &high);
    let picked = durable_recovery_bucket_dump_manifest_at(&real.engine.index_dir, SHARD)
        .expect("a manifest to recover from");
    assert_eq!(
        picked.manifest_id, high.manifest_id,
        "recovery did not pick the manifest with the highest WAL anchor"
    );
}

/// AN INSTALL MARKER IS PROTOBUF ONLY, AND A JSON ONE IS REFUSED.
///
/// The writer's marker is read back through the real listing. A planted JSON marker, and one that
/// decodes to a message naming no manifest, are each refused by the listing rather than read as a
/// marker; the written marker is the control.
#[test]
fn an_install_marker_is_protobuf_only() {
    let real = a_real_manifest();
    real.engine
        .persist_bucket_dump_install_marker(&real.manifest, "prepare")
        .unwrap();
    let files = bucket_dump_install_marker_files_at(&real.engine.index_dir, SHARD).unwrap();
    assert_eq!(files.len(), 1);
    let written = std::fs::read(&files[0].1).unwrap();
    assert_eq!(written, encode_bucket_dump_install_marker(&files[0].0));
    assert!(serde_json::from_slice::<serde_json::Value>(&written).is_err());
    assert_eq!(files[0].0.manifest_id, real.manifest.manifest_id);
    assert_eq!(files[0].0.phase, "prepare");

    let refused = |bytes: &[u8], what: &str| {
        assert!(
            decode_bucket_dump_install_marker(bytes).is_err(),
            "{what} was read instead of refused"
        );
    };
    let other = BucketDumpInstallMarker {
        shard_id: SHARD,
        manifest_id: "planted-other-manifest".to_string(),
        phase: "install".to_string(),
        wal_sequence: 9_001,
        index_log_sequence: 9_002,
        created_unix_ms: 1_999_999_999_999,
    };
    refused(&serde_json::to_vec_pretty(&other).unwrap(), "the old JSON marker");
    refused(b"", "an empty marker");
    refused(&[0x78, 0x01], "a marker of only an unknown field");
    // CONTROL: the same marker in the shape the writer produces is read.
    assert_eq!(
        decode_bucket_dump_install_marker(&encode_bucket_dump_install_marker(&other)).unwrap(),
        other
    );

    // Through the real listing: a JSON file in the marker's place makes the listing fail rather
    // than yield a marker.
    let path = bucket_dump_install_marker_path(&real.engine.index_dir, &other);
    std::fs::write(&path, serde_json::to_vec_pretty(&other).unwrap()).unwrap();
    assert!(
        list_bucket_dump_install_markers_at(&real.engine.index_dir, SHARD).is_err(),
        "the listing read a JSON marker"
    );
    std::fs::remove_file(&path).unwrap();
    assert_eq!(
        list_bucket_dump_install_markers_at(&real.engine.index_dir, SHARD)
            .unwrap()
            .len(),
        1
    );
}

/// NO JSON PATH IS LEFT IN THE MANIFEST OR MARKER CODE.
///
/// The behavioural half is in the tests above (they assert what the real writers put on disk). This
/// is the half that sees an encoder nobody calls YET: a retired format comes back when a later branch
/// finds the function and calls it. The scan looks at each function's own code with comment lines
/// removed, because a doc comment naming `serde_json` is not a call to it, and it is anchored on the
/// function's declaration up to the next one. The CONTROL runs the same scan over a string that does
/// name it, so a scan that could not see the token would fail here instead of passing over a writer.
#[test]
fn no_json_path_is_left_for_a_manifest_or_a_marker() {
    fn body(source: &str, from: &str, to: &str) -> String {
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
    // control
    assert!(
        body("fn a() {\n// a comment\nlet v = serde_json::to_vec(&x);\n}\nfn b", "fn a(", "fn b")
            .contains("serde_json"),
        "the scan cannot see a serde_json call"
    );
    let persistence = include_str!("../engine_persistence.rs");
    let io = include_str!("../bucket_dump_io.rs");
    let shared = include_str!("../../shared_store.rs");
    let codec = include_str!("../bucket_dump_codec.rs");

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
    // DENOMINATOR: three writers, each located and each calling the encoder.
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
            "{name} still reaches for serde_json: a JSON encode path survives for a manifest or marker"
        );
    }

    // The readers and the digest: none may name serde_json either. The listing and the single
    // reader are in `bucket_dump_io`, the decoders and the digest in the codec, and the restore in
    // the shared store.
    let readers = [
        ("the codec", body(codec, "use super::*;", "#[cfg(test)]\npub(crate) fn bucket_dump_protobuf_digest_payload(")),
        (
            "bucket_dump_manifest_at",
            body(io, "pub(super) fn bucket_dump_manifest_at(", "pub(super) fn bucket_dump_install_marker_path("),
        ),
        (
            "the checksum",
            body(io, "fn finish_bucket_dump_manifest_checksum(", "pub(super) fn bucket_dump_fault_scenario("),
        ),
        (
            "restore_bucket_dump_manifests",
            body(shared, "pub async fn restore_bucket_dump_manifests(", "pub async fn restore_checkpoint("),
        ),
    ];
    for (name, code) in &readers {
        assert!(code.len() > 100, "{name}: the scan found almost nothing, so it proves nothing");
        assert!(
            !code.contains("serde_json"),
            "{name} still names serde_json: a JSON arm survives for a manifest or marker"
        );
    }
}
