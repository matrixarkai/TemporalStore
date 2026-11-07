// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The binary shape of the bucket-dump manifest and the install marker, and the checksum
//! derivation that goes with it.
//!
//! # TWO SHAPES, DECIDED PER FILE FROM THE FIRST BYTE
//!
//! A manifest or marker on disk is either the JSON document older builds wrote or a protobuf body
//! behind one container byte. `BUCKET_DUMP_CONTAINER_PROTOBUF` is 0x01, which is not whitespace
//! and not valid JSON in any position, so the first byte settles it with no chance of reading
//! either shape as the other. The decision is never taken from a setting: a node that has never
//! written a JSON manifest must still read one, because the hold is on the installed base.
//!
//! # THE CHECKSUM IS DEFINED OVER BYTES, SO IT HAS A VERSION
//!
//! `BucketDumpManifest::checksum` is a SHA-256 over a byte string, and which byte string it is
//! depends on when the manifest was written:
//!
//!   * version 0 -- the manifest's canonical JSON with `checksum` emptied. Every manifest written
//!     before this module existed. It is kept as the VERIFIER for those manifests, and nothing
//!     else: a derivation that is part of the verification of a stored file cannot be removed
//!     while that file can exist, and it cannot be re-derived from the protobuf shape.
//!   * version 1 -- the manifest's deterministic protobuf encoding with `checksum` empty.
//!
//! The version travels IN the manifest (`checksum_version`), and for version 1 it is inside the
//! digested bytes, so flipping it on a stored manifest changes which derivation runs AND what it
//! is run over; neither can land on the stored checksum. A version this build does not know is an
//! error, never a default.
//!
//! # WHY A PROTOBUF DIGEST IS DETERMINISTIC HERE
//!
//! prost writes fields in tag order, writes no unknown fields, and writes a proto3 scalar at its
//! default as nothing. The message has no `map` field (a map has no defined iteration order), so
//! one manifest value has exactly one encoding. The digest is taken over the DECODED value
//! re-encoded, the same way the JSON derivation re-serialises the decoded struct, so a byte that
//! decodes to a different value changes the digest, and a byte the decoder normalises away is a
//! byte that carried no value.
//!
//! # THE INDEX IMAGE IS STREAMED, NOT COPIED
//!
//! `index_bytes` is the whole-shard index and the largest field by orders of magnitude. It has
//! the second-highest tag, so the encoding is `[every other field][tag, length, index bytes]
//! [checksum]`, and the digest feeds the hasher those pieces from the borrowed slice instead of
//! building a message that owns a second copy. The test below that compares this against
//! `prost`'s own encoding of the whole message is what keeps that claim honest.
use super::*;
use crate::sdk::v1;
use prost::Message;

/// First byte of a binary manifest or marker file. See the module note.
pub(crate) const BUCKET_DUMP_CONTAINER_PROTOBUF: u8 = 0x01;

/// `checksum_version` of a manifest whose checksum is a SHA-256 over its canonical JSON.
pub(crate) const BUCKET_DUMP_CHECKSUM_LEGACY_JSON: u32 = 0;
/// `checksum_version` of a manifest whose checksum is a SHA-256 over its protobuf encoding.
pub(crate) const BUCKET_DUMP_CHECKSUM_PROTOBUF: u32 = 1;

/// Field numbers the digest streams by hand. Declared in `temporalstore.proto`, where the note on
/// `DurableBucketDumpManifest` says why they must stay the two highest.
const INDEX_BYTES_FIELD: u32 = 22;
const CHECKSUM_FIELD: u32 = 23;

fn invalid_data(err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
}

/// The protobuf body of a binary file, or `None` for the JSON shape.
///
/// THE ONLY PLACE THAT DECIDES, so the two shapes cannot be told apart two different ways.
fn bucket_dump_protobuf_body(bytes: &[u8]) -> Option<&[u8]> {
    match bytes.split_first() {
        Some((&BUCKET_DUMP_CONTAINER_PROTOBUF, body)) => Some(body),
        _ => None,
    }
}

fn summary_to_proto(summary: &BucketStorageSummary) -> v1::DurableBucketStorageSummary {
    v1::DurableBucketStorageSummary {
        routing_bucket: summary.routing_bucket,
        object_count: summary.object_count,
        block_ref_count: summary.block_ref_count,
        logical_bytes: summary.logical_bytes,
        physical_bytes: summary.physical_bytes,
        dirty_object_count: summary.dirty_object_count,
        dirty_generation: summary.dirty_generation,
        last_dump_sequence: summary.last_dump_sequence,
        block_slab_ids: summary.block_slab_ids.clone(),
        last_compacted_slab: summary.last_compacted_slab,
    }
}

fn summary_from_proto(summary: v1::DurableBucketStorageSummary) -> BucketStorageSummary {
    BucketStorageSummary {
        routing_bucket: summary.routing_bucket,
        object_count: summary.object_count,
        block_ref_count: summary.block_ref_count,
        logical_bytes: summary.logical_bytes,
        physical_bytes: summary.physical_bytes,
        dirty_object_count: summary.dirty_object_count,
        dirty_generation: summary.dirty_generation,
        last_dump_sequence: summary.last_dump_sequence,
        block_slab_ids: summary.block_slab_ids,
        last_compacted_slab: summary.last_compacted_slab,
    }
}

fn lifecycle_to_proto(report: &StorageObjectLifecycleReport) -> v1::DurableStorageObjectLifecycle {
    v1::DurableStorageObjectLifecycle {
        live_object_ids: report.live_object_ids,
        live_block_refs: report.live_block_refs,
        stale_object_ids: report.stale_object_ids,
        delete_marked_object_ids: report.delete_marked_object_ids,
        reused_object_id_conflicts: report.reused_object_id_conflicts,
        missing_owner_block_refs: report.missing_owner_block_refs,
        owner_mismatch_block_refs: report.owner_mismatch_block_refs,
        reused_object_ids: report.reused_object_ids.clone(),
        delete_marked_object_keys: report.delete_marked_object_keys.clone(),
    }
}

fn lifecycle_from_proto(report: v1::DurableStorageObjectLifecycle) -> StorageObjectLifecycleReport {
    StorageObjectLifecycleReport {
        live_object_ids: report.live_object_ids,
        live_block_refs: report.live_block_refs,
        stale_object_ids: report.stale_object_ids,
        delete_marked_object_ids: report.delete_marked_object_ids,
        reused_object_id_conflicts: report.reused_object_id_conflicts,
        missing_owner_block_refs: report.missing_owner_block_refs,
        owner_mismatch_block_refs: report.owner_mismatch_block_refs,
        reused_object_ids: report.reused_object_ids,
        delete_marked_object_keys: report.delete_marked_object_keys,
    }
}

/// Everything in the manifest EXCEPT the index image and the checksum, as a message.
///
/// Those two are the fields the digest and the writer append by hand (see the module note), so
/// leaving them empty here is what keeps the whole-shard image from being copied into a message.
fn manifest_head_to_proto(manifest: &BucketDumpManifest) -> v1::DurableBucketDumpManifest {
    v1::DurableBucketDumpManifest {
        version: manifest.version,
        shard_id: manifest.shard_id,
        manifest_id: manifest.manifest_id.clone(),
        manifest_kind: manifest.manifest_kind.clone(),
        dump_generation_id: manifest.dump_generation_id.clone(),
        source_manifest_ids: manifest.source_manifest_ids.clone(),
        source_manifest_coverage: manifest
            .source_manifest_coverage
            .iter()
            .map(|source| v1::DurableBucketDumpSourceCoverage {
                manifest_id: source.manifest_id.clone(),
                bucket_ids: source.bucket_ids.clone(),
            })
            .collect(),
        parent_manifest_id: manifest.parent_manifest_id.clone(),
        load_version_handoff: manifest.load_version_handoff.as_ref().map(|handoff| {
            v1::DurableBucketDumpLoadVersionHandoff {
                previous_load_version: handoff.previous_load_version,
                next_load_version: handoff.next_load_version,
                applied: handoff.applied,
            }
        }),
        created_unix_ms: manifest.created_unix_ms,
        bucket_ids: manifest.bucket_ids.clone(),
        block_slab_ids: manifest.block_slab_ids.clone(),
        wal_sequence: manifest.wal_sequence,
        index_log_sequence: manifest.index_log_sequence,
        live_block_refs: manifest.live_block_refs,
        logical_bytes: manifest.logical_bytes,
        physical_bytes: manifest.physical_bytes,
        bucket_summaries: manifest
            .bucket_summaries
            .iter()
            .map(summary_to_proto)
            .collect(),
        // The all-zero report is ABSENT, which is what a manifest without one has always meant.
        object_lifecycle: (manifest.object_lifecycle != StorageObjectLifecycleReport::default())
            .then(|| lifecycle_to_proto(&manifest.object_lifecycle)),
        index_sha256: manifest.index_sha256.clone(),
        checksum_version: manifest.checksum_version,
        index_bytes: Vec::new(),
        checksum: String::new(),
    }
}

fn manifest_from_proto(manifest: v1::DurableBucketDumpManifest) -> BucketDumpManifest {
    BucketDumpManifest {
        version: manifest.version,
        shard_id: manifest.shard_id,
        manifest_id: manifest.manifest_id,
        manifest_kind: manifest.manifest_kind,
        dump_generation_id: manifest.dump_generation_id,
        source_manifest_ids: manifest.source_manifest_ids,
        source_manifest_coverage: manifest
            .source_manifest_coverage
            .into_iter()
            .map(|source| BucketDumpSourceCoverage {
                manifest_id: source.manifest_id,
                bucket_ids: source.bucket_ids,
            })
            .collect(),
        parent_manifest_id: manifest.parent_manifest_id,
        load_version_handoff: manifest.load_version_handoff.map(|handoff| {
            BucketDumpLoadVersionHandoff {
                previous_load_version: handoff.previous_load_version,
                next_load_version: handoff.next_load_version,
                applied: handoff.applied,
            }
        }),
        created_unix_ms: manifest.created_unix_ms,
        bucket_ids: manifest.bucket_ids,
        block_slab_ids: manifest.block_slab_ids,
        wal_sequence: manifest.wal_sequence,
        index_log_sequence: manifest.index_log_sequence,
        live_block_refs: manifest.live_block_refs,
        logical_bytes: manifest.logical_bytes,
        physical_bytes: manifest.physical_bytes,
        bucket_summaries: manifest
            .bucket_summaries
            .into_iter()
            .map(summary_from_proto)
            .collect(),
        object_lifecycle: manifest
            .object_lifecycle
            .map(lifecycle_from_proto)
            .unwrap_or_default(),
        index_bytes: manifest.index_bytes,
        index_sha256: manifest.index_sha256,
        checksum_version: manifest.checksum_version,
        checksum: manifest.checksum,
    }
}

/// The tag and length prefix of the index image field, as prost would write it.
fn index_bytes_field_header(len: usize) -> Vec<u8> {
    let mut header = Vec::with_capacity(8);
    prost::encoding::encode_key(
        INDEX_BYTES_FIELD,
        prost::encoding::WireType::LengthDelimited,
        &mut header,
    );
    prost::encoding::encode_varint(len as u64, &mut header);
    header
}

/// A manifest in EITHER shape.
///
/// Every reader of a manifest file or object goes through this. The JSON arm is permanent: it is
/// the shape of every manifest written before the binary one existed, and "which writer runs
/// today" is the wrong question for a reader -- the hold is on what is on disk.
pub(crate) fn decode_bucket_dump_manifest(
    bytes: &[u8],
) -> Result<BucketDumpManifest, std::io::Error> {
    match bucket_dump_protobuf_body(bytes) {
        Some(body) => v1::DurableBucketDumpManifest::decode(body)
            .map(manifest_from_proto)
            .map_err(invalid_data),
        None => serde_json::from_slice(bytes).map_err(invalid_data),
    }
}

/// A marker in EITHER shape. See `decode_bucket_dump_manifest` for why the JSON arm stays.
pub(crate) fn decode_bucket_dump_install_marker(
    bytes: &[u8],
) -> Result<BucketDumpInstallMarker, std::io::Error> {
    match bucket_dump_protobuf_body(bytes) {
        Some(body) => v1::DurableBucketDumpInstallMarker::decode(body)
            .map(|marker| BucketDumpInstallMarker {
                shard_id: marker.shard_id,
                manifest_id: marker.manifest_id,
                phase: marker.phase,
                wal_sequence: marker.wal_sequence,
                index_log_sequence: marker.index_log_sequence,
                created_unix_ms: marker.created_unix_ms,
            })
            .map_err(invalid_data),
        None => serde_json::from_slice(bytes).map_err(invalid_data),
    }
}

/// The version-1 checksum: SHA-256 over the manifest's protobuf encoding with `checksum` empty,
/// and how many bytes were hashed.
///
/// Streamed in the order prost writes the message: the head (every field below the index image),
/// then the image's tag and length and the image itself from the borrowed slice. `checksum` is the
/// highest tag and is empty here, so it contributes nothing.
pub(crate) fn bucket_dump_protobuf_digest(manifest: &BucketDumpManifest) -> (String, u64) {
    let head = manifest_head_to_proto(manifest).encode_to_vec();
    let mut hasher = Sha256::new();
    hasher.update(&head);
    let mut hashed = head.len() as u64;
    if !manifest.index_bytes.is_empty() {
        let header = index_bytes_field_header(manifest.index_bytes.len());
        hasher.update(&header);
        hasher.update(&manifest.index_bytes);
        hashed += (header.len() + manifest.index_bytes.len()) as u64;
    }
    let hex = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    (hex, hashed)
}

/// The exact bytes `bucket_dump_protobuf_digest` hashes, materialised. For the test that compares
/// the streamed form against prost's own encoding of the whole message.
#[cfg(test)]
pub(crate) fn bucket_dump_protobuf_digest_payload(manifest: &BucketDumpManifest) -> Vec<u8> {
    let mut payload = manifest_head_to_proto(manifest).encode_to_vec();
    if !manifest.index_bytes.is_empty() {
        payload.extend(index_bytes_field_header(manifest.index_bytes.len()));
        payload.extend_from_slice(&manifest.index_bytes);
    }
    payload
}

/// A manifest as the binary file: the container byte, the head, the index image, the checksum.
///
/// THE ONLY ENCODER. Every writer of a manifest -- the local index dir and the shared object
/// store -- calls this, and no JSON encode path is left for either. `BucketDumpManifest` still
/// derives `Serialize`, so a test can build the legacy shape to plant it; what is gone is any
/// path a running node can take to write it. An encoder nobody reaches is how a retired format
/// comes back (a later branch finds the function and calls it); a decoder nobody in this tree
/// calls is still reached by every manifest already on disk. Opposite reasons, so the encoder is
/// deleted and `decode_bucket_dump_manifest` keeps its JSON arm.
pub(crate) fn encode_bucket_dump_manifest(manifest: &BucketDumpManifest) -> Vec<u8> {
    let mut out = vec![BUCKET_DUMP_CONTAINER_PROTOBUF];
    out.extend(manifest_head_to_proto(manifest).encode_to_vec());
    if !manifest.index_bytes.is_empty() {
        out.extend(index_bytes_field_header(manifest.index_bytes.len()));
        out.extend_from_slice(&manifest.index_bytes);
    }
    if !manifest.checksum.is_empty() {
        prost::encoding::encode_key(
            CHECKSUM_FIELD,
            prost::encoding::WireType::LengthDelimited,
            &mut out,
        );
        prost::encoding::encode_varint(manifest.checksum.len() as u64, &mut out);
        out.extend_from_slice(manifest.checksum.as_bytes());
    }
    out
}

/// A marker as the binary file. The only encoder, for the reasons on `encode_bucket_dump_manifest`.
pub(crate) fn encode_bucket_dump_install_marker(marker: &BucketDumpInstallMarker) -> Vec<u8> {
    let mut out = vec![BUCKET_DUMP_CONTAINER_PROTOBUF];
    out.extend(
        v1::DurableBucketDumpInstallMarker {
            shard_id: marker.shard_id,
            manifest_id: marker.manifest_id.clone(),
            phase: marker.phase.clone(),
            wal_sequence: marker.wal_sequence,
            index_log_sequence: marker.index_log_sequence,
            created_unix_ms: marker.created_unix_ms,
        }
        .encode_to_vec(),
    );
    out
}

/// A manifest re-derived as a LEGACY one: version-0 checksum over its canonical JSON, with the
/// generation id refreshed first because it hashes the sequences and the checksum covers it.
///
/// For tests that plant the installed base. No writer produces this shape any more, which is why
/// there is a test-only way to build it at all.
#[cfg(test)]
pub(crate) fn into_legacy_json_manifest_for_test(mut manifest: BucketDumpManifest) -> BucketDumpManifest {
    manifest.checksum_version = BUCKET_DUMP_CHECKSUM_LEGACY_JSON;
    manifest.dump_generation_id = bucket_dump_generation_id(&manifest);
    manifest.checksum =
        bucket_dump_manifest_checksum(&manifest).expect("the JSON derivation runs");
    manifest
}
