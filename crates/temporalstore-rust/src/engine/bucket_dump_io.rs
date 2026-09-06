// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bucket-dump manifest/install-marker file I/O helpers, split from engine.rs.
use super::*;

pub(super) fn bucket_dump_manifest_dir(index_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    index_dir
        .join("slot-dumps")
        .join(format!("shard-{shard_id}"))
}

pub(super) fn bucket_dump_manifest_path(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> PathBuf {
    bucket_dump_manifest_dir(index_dir, shard_id).join(format!("{manifest_id}.json"))
}

pub(super) fn bucket_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<Option<BucketDumpManifest>, std::io::Error> {
    let path = bucket_dump_manifest_path(index_dir, shard_id, manifest_id);
    if !path.exists() {
        return Ok(None);
    }
    serde_json::from_slice::<BucketDumpManifest>(&fs::read(path)?)
        .map(Some)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

pub(super) fn bucket_dump_install_marker_path(
    index_dir: &std::path::Path,
    marker: &BucketDumpInstallMarker,
) -> PathBuf {
    bucket_dump_manifest_dir(index_dir, marker.shard_id).join(format!(
        "{}.{}.{}.marker",
        marker.manifest_id, marker.phase, marker.created_unix_ms
    ))
}

pub(super) fn write_bucket_dump_install_marker(
    index_dir: &std::path::Path,
    marker: &BucketDumpInstallMarker,
) -> Result<(), std::io::Error> {
    let path = bucket_dump_install_marker_path(index_dir, marker);
    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    // Install roll-forward recovery keys off these markers; write durably + atomically
    // so a crash cannot leave a torn/lost commit-phase marker (was a bare fs::write).
    atomic_write_bytes(&path, &bytes)
}

pub(super) fn bucket_dump_install_marker_files_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<(BucketDumpInstallMarker, PathBuf)>, std::io::Error> {
    let dir = bucket_dump_manifest_dir(index_dir, shard_id);
    let mut markers = Vec::new();
    if !dir.exists() {
        return Ok(markers);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "marker")
            .unwrap_or(false)
        {
            continue;
        }
        let path = entry.path();
        let marker = serde_json::from_slice::<BucketDumpInstallMarker>(&fs::read(&path)?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        markers.push((marker, path));
    }
    markers.sort_by_key(|(marker, _)| {
        (
            marker.index_log_sequence,
            marker.created_unix_ms,
            bucket_dump_install_phase_rank(&marker.phase),
        )
    });
    Ok(markers)
}

pub(super) fn list_bucket_dump_install_markers_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<BucketDumpInstallMarker>, std::io::Error> {
    Ok(bucket_dump_install_marker_files_at(index_dir, shard_id)?
        .into_iter()
        .map(|(marker, _)| marker)
        .collect())
}

pub(super) fn interrupted_bucket_dump_installs_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<BucketDumpInstallMarker>, std::io::Error> {
    let mut latest_by_manifest = BTreeMap::<String, BucketDumpInstallMarker>::new();
    for marker in list_bucket_dump_install_markers_at(index_dir, shard_id)? {
        let replace = latest_by_manifest
            .get(&marker.manifest_id)
            .map(|existing| {
                bucket_dump_install_phase_rank(&marker.phase)
                    > bucket_dump_install_phase_rank(&existing.phase)
                    || (bucket_dump_install_phase_rank(&marker.phase)
                        == bucket_dump_install_phase_rank(&existing.phase)
                        && marker.created_unix_ms > existing.created_unix_ms)
            })
            .unwrap_or(true);
        if replace {
            latest_by_manifest.insert(marker.manifest_id.clone(), marker);
        }
    }
    Ok(latest_by_manifest
        .into_values()
        .filter(|marker| marker.phase != "commit")
        .collect())
}

pub(super) fn remove_obsolete_bucket_dump_install_markers(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<usize, std::io::Error> {
    let mut removed = 0usize;
    for (marker, path) in bucket_dump_install_marker_files_at(index_dir, shard_id)? {
        if marker.manifest_id == manifest_id
            && (marker.phase == "prepare" || marker.phase == "install")
            && fs::remove_file(path).is_ok()
        {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

pub(super) fn bucket_dump_install_phase_counts(markers: &[BucketDumpInstallMarker]) -> (usize, usize, usize) {
    let mut prepared = 0usize;
    let mut installed = 0usize;
    let mut unknown = 0usize;
    for marker in markers {
        match marker.phase.as_str() {
            "prepare" => prepared = prepared.saturating_add(1),
            "install" => installed = installed.saturating_add(1),
            _ => unknown = unknown.saturating_add(1),
        }
    }
    (prepared, installed, unknown)
}

pub(super) fn bucket_dump_install_phase_rank(phase: &str) -> u8 {
    match phase {
        "prepare" => 1,
        "install" => 2,
        "commit" => 3,
        _ => 0,
    }
}

pub(super) fn bucket_dump_manifest_chain_issues(
    manifests: &[BucketDumpManifest],
) -> Vec<BucketDumpManifestChainIssue> {
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    manifests
        .iter()
        .filter_map(|manifest| {
            let parent = manifest.parent_manifest_id.as_ref()?;
            (!manifest_ids.contains(parent)).then(|| BucketDumpManifestChainIssue {
                manifest_id: manifest.manifest_id.clone(),
                parent_manifest_id: Some(parent.clone()),
                reason: "missing_parent_manifest".to_string(),
            })
        })
        .collect()
}

/// Manifests retained unconditionally: the NEWEST one, and nothing else.
///
/// Each manifest embeds a complete, self-contained index (`index_bytes`), and installing one
/// never walks the parent chain to reconstruct anything -- the chain is lineage metadata, not
/// a recovery path. So an ancestor is not needed to recover from its descendant.
///
/// This used to walk the whole parent chain. Because every manifest is created with its parent
/// set to the previous one, that retained every manifest ever written: `prunable` was always
/// empty, dump manifests grew without bound on disk, and the follower-cursor / raft-snapshot
/// guards were inert -- they can only hold back a manifest that would otherwise be pruned, so
/// their block counters were structurally stuck at zero.
///
/// Anything older is now prunable unless a follower cursor or raft snapshot ref pins it, which
/// is exactly what those cursors are for.
pub(super) fn retained_bucket_dump_manifest_ids(manifests: &[BucketDumpManifest]) -> BTreeSet<String> {
    // The newest manifest, PLUS any older one that is still the only dump covering some bucket.
    //
    // Reclaim advances its frontier only for buckets that have a manifest matching their current
    // generation. Keeping just the newest meant an ordinary cycle -- which dumps only the few
    // DIRTY buckets -- deleted the manifest covering everything else, and coverage collapsed to
    // those few. Seen on a live store: a round covering all 3,190 buckets let reclaim apply and the
    // log fall 806 -> 478 MB, then an ordinary cycle within the hour left `covered_slot_count: 0`
    // and a durable frontier of 0, so the log grew back and every cold start paid for it.
    //
    // Self-limiting: walking newest first, once a manifest covers everything, older ones add no
    // coverage and are pruned exactly as before.
    let mut ordered = manifests.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|manifest| {
        std::cmp::Reverse((manifest.index_log_sequence, manifest.created_unix_ms))
    });
    let mut retained = BTreeSet::new();
    let mut covered = BTreeSet::<u32>::new();
    for manifest in ordered {
        let adds_coverage = manifest
            .bucket_ids
            .iter()
            .any(|bucket| !covered.contains(bucket));
        // `retained.is_empty()` keeps the newest even when it covers nothing new, so the previous
        // guarantee -- there is always a most recent dump to anchor on -- is unchanged.
        if retained.is_empty() || adds_coverage {
            retained.insert(manifest.manifest_id.clone());
            covered.extend(manifest.bucket_ids.iter().copied());
        }
    }
    retained
}

/// A cursor older than every manifest we kept: nothing retained can serve it. Named because the
/// prune plan produces it and the index-GC gate reads it, and a typo between the two would read as
/// "safe" rather than as a mistake.
pub(super) const FOLLOWER_PRECEDES_EVERY_MANIFEST: &str = "follower_cursor_precedes_every_manifest";
/// The same for a raft snapshot reference.
pub(super) const RAFT_SNAPSHOT_PRECEDES_EVERY_MANIFEST: &str = "raft_snapshot_precedes_every_manifest";

pub(super) fn bucket_dump_manifest_prune_plan_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    follower_cursors: &[BucketDumpFollowerReplayCursor],
    raft_snapshot_refs: &[BucketDumpRaftSnapshotRef],
) -> Result<BucketDumpManifestPrunePlan, std::io::Error> {
    let manifests = list_bucket_dump_manifests_at(index_dir, shard_id)?;
    let mut retained = retained_bucket_dump_manifest_ids(&manifests);
    let mut follower_blocks = Vec::new();
    let mut raft_snapshot_blocks = Vec::new();
    for cursor in follower_cursors
        .iter()
        .filter(|cursor| cursor.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.wal_sequence <= cursor.wal_sequence
                && manifest.index_log_sequence <= cursor.index_log_sequence
        }) else {
            // Behind every manifest: nothing kept can serve this follower. Pruning here throws
            // away its last chance of catching up from a dump and says nothing about it, so keep
            // the oldest -- the only one that could ever help -- and report it. Unconditionally:
            // the point is that the follower is unservable, not that anything extra was kept.
            if let Some(oldest) = manifests
                .iter()
                .min_by_key(|manifest| (manifest.wal_sequence, manifest.index_log_sequence))
            {
                retained.insert(oldest.manifest_id.clone());
                follower_blocks.push(BucketDumpFollowerRetentionBlock {
                    follower_id: cursor.follower_id.clone(),
                    manifest_id: oldest.manifest_id.clone(),
                    manifest_wal_sequence: oldest.wal_sequence,
                    manifest_index_log_sequence: oldest.index_log_sequence,
                    cursor_wal_sequence: cursor.wal_sequence,
                    cursor_index_log_sequence: cursor.index_log_sequence,
                    reason: FOLLOWER_PRECEDES_EVERY_MANIFEST.to_string(),
                });
            }
            continue;
        };
        // Record the dependency whether or not the manifest was already being kept. Gating this
        // push on `insert` answered "did this cursor keep something extra?" while every reader
        // takes it for "which cursors depend on a retained dump". The second cursor to anchor one
        // manifest got `false` and vanished -- and so did EVERY cursor anchored on the newest
        // manifest, which is retained unconditionally, which is the ordinary case. Worse, the two
        // loops share `retained`, so a follower and a snapshot anchoring the same manifest had
        // whichever ran first swallow the other, making the record depend on iteration order.
        //
        // `follower_cursor_retention_floor` is the MINIMUM cursor sequence across these blocks. An
        // omitted cursor reports a floor above the truth, and omitting them all reports 0. A floor
        // that forgets the follower furthest behind is worse than no floor at all.
        retained.insert(anchor.manifest_id.clone());
        follower_blocks.push(BucketDumpFollowerRetentionBlock {
            follower_id: cursor.follower_id.clone(),
            manifest_id: anchor.manifest_id.clone(),
            manifest_wal_sequence: anchor.wal_sequence,
            manifest_index_log_sequence: anchor.index_log_sequence,
            cursor_wal_sequence: cursor.wal_sequence,
            cursor_index_log_sequence: cursor.index_log_sequence,
            reason: "follower_cursor_anchor".to_string(),
        });
    }
    for snapshot in raft_snapshot_refs
        .iter()
        .filter(|snapshot| snapshot.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.wal_sequence <= snapshot.wal_sequence
                && manifest.index_log_sequence <= snapshot.index_log_sequence
        }) else {
            // As above: a snapshot reference older than every manifest cannot be served by any of
            // them, and the operator needs to know that rather than have it pruned in silence.
            if let Some(oldest) = manifests
                .iter()
                .min_by_key(|manifest| (manifest.wal_sequence, manifest.index_log_sequence))
            {
                retained.insert(oldest.manifest_id.clone());
                raft_snapshot_blocks.push(BucketDumpRaftSnapshotRetentionBlock {
                    snapshot_id: snapshot.snapshot_id.clone(),
                    manifest_id: oldest.manifest_id.clone(),
                    manifest_wal_sequence: oldest.wal_sequence,
                    manifest_index_log_sequence: oldest.index_log_sequence,
                    snapshot_wal_sequence: snapshot.wal_sequence,
                    snapshot_index_log_sequence: snapshot.index_log_sequence,
                    last_included_index: snapshot.last_included_index,
                    last_included_term: snapshot.last_included_term,
                    reason: RAFT_SNAPSHOT_PRECEDES_EVERY_MANIFEST.to_string(),
                });
            }
            continue;
        };
        // As above: the record is of which snapshot references depend on a retained dump, not
        // of which ones kept an extra one.
        retained.insert(anchor.manifest_id.clone());
        raft_snapshot_blocks.push(BucketDumpRaftSnapshotRetentionBlock {
            snapshot_id: snapshot.snapshot_id.clone(),
            manifest_id: anchor.manifest_id.clone(),
            manifest_wal_sequence: anchor.wal_sequence,
            manifest_index_log_sequence: anchor.index_log_sequence,
            snapshot_wal_sequence: snapshot.wal_sequence,
            snapshot_index_log_sequence: snapshot.index_log_sequence,
            last_included_index: snapshot.last_included_index,
            last_included_term: snapshot.last_included_term,
            reason: "raft_snapshot_anchor".to_string(),
        });
    }
    let interrupted = interrupted_bucket_dump_installs_at(index_dir, shard_id)?
        .into_iter()
        .map(|marker| marker.manifest_id)
        .collect::<BTreeSet<_>>();
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    let mut prunable_manifest_ids = Vec::new();
    let mut blocked_manifest_ids = Vec::new();
    for manifest in &manifests {
        if retained.contains(&manifest.manifest_id) {
            continue;
        }
        if interrupted.contains(&manifest.manifest_id) {
            blocked_manifest_ids.push(manifest.manifest_id.clone());
        } else {
            prunable_manifest_ids.push(manifest.manifest_id.clone());
        }
    }
    let prunable_marker_manifest_ids = list_bucket_dump_install_markers_at(index_dir, shard_id)?
        .into_iter()
        .map(|marker| marker.manifest_id)
        .filter(|manifest_id| {
            !retained.contains(manifest_id)
                && !interrupted.contains(manifest_id)
                && (prunable_manifest_ids.iter().any(|id| id == manifest_id)
                    || !manifest_ids.contains(manifest_id))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut reasons = Vec::new();
    if !prunable_manifest_ids.is_empty() {
        reasons.push("obsolete_slot_dump_manifest".to_string());
    }
    if !prunable_marker_manifest_ids.is_empty() {
        reasons.push("obsolete_slot_dump_marker".to_string());
    }
    if !blocked_manifest_ids.is_empty() {
        reasons.push("interrupted_install_blocks_prune".to_string());
    }
    if !follower_blocks.is_empty() {
        reasons.push("follower_cursor_blocks_prune".to_string());
    }
    if !raft_snapshot_blocks.is_empty() {
        reasons.push("raft_snapshot_blocks_prune".to_string());
    }
    Ok(BucketDumpManifestPrunePlan {
        shard_id,
        retained_manifest_ids: retained.into_iter().collect(),
        prunable_manifest_ids,
        prunable_marker_manifest_ids,
        blocked_manifest_ids,
        follower_blocks,
        raft_snapshot_blocks,
        reasons,
    })
}

pub(super) fn list_bucket_dump_manifests_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<BucketDumpManifest>, std::io::Error> {
    let dir = bucket_dump_manifest_dir(index_dir, shard_id);
    let mut manifests = Vec::new();
    if !dir.exists() {
        return Ok(manifests);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "json")
            .unwrap_or(false)
        {
            continue;
        }
        // A torn (crash-interrupted) or bit-rotted manifest must NOT fail the whole
        // listing -- previously `?` on parse turned one bad file into an empty listing,
        // which on load silently dropped all dumped state. Skip unreadable/unparseable
        // files and reject checksum mismatches, keeping every valid manifest.
        let bytes = match fs::read(entry.path()) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let Ok(manifest) = serde_json::from_slice::<BucketDumpManifest>(&bytes) else {
            continue;
        };
        if !manifest.checksum.is_empty() {
            match bucket_dump_manifest_checksum(&manifest) {
                Ok(expected) if expected == manifest.checksum => {}
                _ => continue,
            }
        }
        manifests.push(manifest);
    }
    manifests.sort_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms));
    Ok(manifests)
}

pub(super) fn latest_bucket_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Option<BucketDumpManifest> {
    list_bucket_dump_manifests_at(index_dir, shard_id)
        .ok()?
        .into_iter()
        .last()
}

pub(super) fn bucket_dump_manifest_checksum(manifest: &BucketDumpManifest) -> Result<String, Status> {
    let mut payload = manifest.clone();
    payload.checksum.clear();
    serde_json::to_vec(&payload)
        .map(|bytes| sha256_hex_bytes(&bytes))
        .map_err(|err| Status::error("slot_dump_checksum_failed", err.to_string()))
}

pub(super) fn bucket_dump_fault_scenario(
    scenario: impl Into<String>,
    expected_code: impl Into<String>,
    actual_code: impl Into<String>,
    blockers: Vec<String>,
    install_safe: bool,
) -> BucketDumpFaultScenarioReport {
    let expected_code = expected_code.into();
    let actual_code = actual_code.into();
    BucketDumpFaultScenarioReport {
        scenario: scenario.into(),
        passed: actual_code == expected_code,
        expected_code,
        actual_code,
        blockers,
        install_safe,
    }
}

pub(super) fn bucket_dump_generation_id(manifest: &BucketDumpManifest) -> String {
    let mut digest = Sha256::new();
    digest.update(manifest.shard_id.to_le_bytes());
    digest.update(manifest.wal_sequence.to_le_bytes());
    digest.update(manifest.index_log_sequence.to_le_bytes());
    for bucket_id in &manifest.bucket_ids {
        digest.update(bucket_id.to_le_bytes());
    }
    for page_slab_id in &manifest.page_slab_ids {
        digest.update(page_slab_id.to_le_bytes());
    }
    digest.update(manifest.index_sha256.as_bytes());
    if manifest.version >= 3 {
        digest.update(manifest.object_lifecycle.live_object_ids.to_le_bytes());
        digest.update(manifest.object_lifecycle.live_page_refs.to_le_bytes());
        digest.update(manifest.object_lifecycle.stale_object_ids.to_le_bytes());
        digest.update(
            manifest
                .object_lifecycle
                .tombstoned_object_ids
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .reused_object_id_conflicts
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .missing_owner_page_refs
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .owner_mismatch_page_refs
                .to_le_bytes(),
        );
        for object_id in &manifest.object_lifecycle.reused_object_ids {
            digest.update(object_id.to_le_bytes());
        }
        for key in &manifest.object_lifecycle.tombstoned_object_keys {
            digest.update(key.as_bytes());
            digest.update([0]);
        }
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
