// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The binary shape of the bucket-dump manifest and the install marker, and the checksum
//! derivation that goes with it.
//!
//! # ONE SHAPE
//!
//! A manifest or marker file is a bare protobuf message: no container byte, no JSON arm and no
//! first-byte sniff. The first release carries no installed base, so there is no older shape to tell
//! apart from this one. A body that does not decode, or that decodes to a message naming no manifest
//! (which is also what damage that decodes to nothing looks like), is an error and never a default.
//!
//! # THE CHECKSUM IS A SHA-256 OVER THE ENCODING
//!
//! `BucketDumpManifest::checksum` is a SHA-256 over the manifest's deterministic protobuf encoding
//! with `checksum` empty. There is one derivation, so there is no version to carry: the digest is
//! taken over the DECODED value re-encoded, which is what makes a byte that decodes to a different
//! value change the digest.
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

/// Field numbers the digest streams by hand. Declared in `temporalstore.proto`, where the note on
/// `DurableBucketDumpManifest` says why they must stay the two highest.
const INDEX_BYTES_FIELD: u32 = 22;
const CHECKSUM_FIELD: u32 = 23;

fn invalid_data(err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
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

/// A manifest file or object. THE ONLY READER of one.
///
/// Every reader of a manifest goes through this. An empty body is an error, and so is a message that
/// names no manifest: with no container byte, a body that decodes to nothing is indistinguishable
/// from damage, and a default manifest would read as a real one with an unmatched checksum.
pub(crate) fn decode_bucket_dump_manifest(
    bytes: &[u8],
) -> Result<BucketDumpManifest, std::io::Error> {
    if bytes.is_empty() {
        return Err(invalid_data("the manifest body is empty"));
    }
    let manifest = v1::DurableBucketDumpManifest::decode(bytes).map_err(invalid_data)?;
    if manifest.manifest_id.is_empty() {
        return Err(invalid_data("the manifest body names no manifest"));
    }
    Ok(manifest_from_proto(manifest))
}

/// A marker file. THE ONLY READER of one; refuses an empty body and a message naming no manifest
/// for the reasons on `decode_bucket_dump_manifest`.
pub(crate) fn decode_bucket_dump_install_marker(
    bytes: &[u8],
) -> Result<BucketDumpInstallMarker, std::io::Error> {
    if bytes.is_empty() {
        return Err(invalid_data("the marker body is empty"));
    }
    let marker = v1::DurableBucketDumpInstallMarker::decode(bytes).map_err(invalid_data)?;
    if marker.manifest_id.is_empty() {
        return Err(invalid_data("the marker body names no manifest"));
    }
    Ok(BucketDumpInstallMarker {
        shard_id: marker.shard_id,
        manifest_id: marker.manifest_id,
        phase: marker.phase,
        wal_sequence: marker.wal_sequence,
        index_log_sequence: marker.index_log_sequence,
        created_unix_ms: marker.created_unix_ms,
    })
}

/// The manifest checksum: SHA-256 over the manifest's protobuf encoding with `checksum` empty,
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

/// A manifest as the file: the head, the index image, the checksum.
///
/// THE ONLY ENCODER. Every writer of a manifest -- the local index dir and the shared object store --
/// calls this.
pub(crate) fn encode_bucket_dump_manifest(manifest: &BucketDumpManifest) -> Vec<u8> {
    let mut out = manifest_head_to_proto(manifest).encode_to_vec();
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

/// A marker as the file. The only encoder.
pub(crate) fn encode_bucket_dump_install_marker(marker: &BucketDumpInstallMarker) -> Vec<u8> {
    v1::DurableBucketDumpInstallMarker {
        shard_id: marker.shard_id,
        manifest_id: marker.manifest_id.clone(),
        phase: marker.phase.clone(),
        wal_sequence: marker.wal_sequence,
        index_log_sequence: marker.index_log_sequence,
        created_unix_ms: marker.created_unix_ms,
    }
    .encode_to_vec()
}
