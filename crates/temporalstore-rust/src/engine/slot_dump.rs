use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use sha2::{Digest, Sha256};

use super::reports::*;
use crate::types::{ShardId, Status};

pub(super) fn slot_dump_manifest_dir(index_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    index_dir
        .join("slot-dumps")
        .join(format!("shard-{shard_id}"))
}

pub(super) fn slot_dump_manifest_path(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> PathBuf {
    slot_dump_manifest_dir(index_dir, shard_id).join(format!("{manifest_id}.json"))
}

pub(super) fn slot_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<Option<SlotDumpManifest>, std::io::Error> {
    let path = slot_dump_manifest_path(index_dir, shard_id, manifest_id);
    if !path.exists() {
        return Ok(None);
    }
    serde_json::from_slice::<SlotDumpManifest>(&fs::read(path)?)
        .map(Some)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

pub(super) fn slot_dump_install_marker_path(
    index_dir: &std::path::Path,
    marker: &SlotDumpInstallMarker,
) -> PathBuf {
    slot_dump_manifest_dir(index_dir, marker.shard_id).join(format!(
        "{}.{}.{}.marker",
        marker.manifest_id, marker.phase, marker.created_unix_ms
    ))
}

pub(super) fn write_slot_dump_install_marker(
    index_dir: &std::path::Path,
    marker: &SlotDumpInstallMarker,
) -> Result<(), std::io::Error> {
    let path = slot_dump_install_marker_path(index_dir, marker);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    fs::write(path, bytes)
}

pub(super) fn slot_dump_install_marker_files_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<(SlotDumpInstallMarker, PathBuf)>, std::io::Error> {
    let dir = slot_dump_manifest_dir(index_dir, shard_id);
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
        let marker = serde_json::from_slice::<SlotDumpInstallMarker>(&fs::read(&path)?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        markers.push((marker, path));
    }
    markers.sort_by_key(|(marker, _)| {
        (
            marker.index_log_sequence,
            marker.created_unix_ms,
            slot_dump_install_phase_rank(&marker.phase),
        )
    });
    Ok(markers)
}

pub(super) fn list_slot_dump_install_markers_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpInstallMarker>, std::io::Error> {
    Ok(slot_dump_install_marker_files_at(index_dir, shard_id)?
        .into_iter()
        .map(|(marker, _)| marker)
        .collect())
}

pub(super) fn interrupted_slot_dump_installs_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpInstallMarker>, std::io::Error> {
    let mut latest_by_manifest = BTreeMap::<String, SlotDumpInstallMarker>::new();
    for marker in list_slot_dump_install_markers_at(index_dir, shard_id)? {
        let replace = latest_by_manifest
            .get(&marker.manifest_id)
            .map(|existing| {
                slot_dump_install_phase_rank(&marker.phase)
                    > slot_dump_install_phase_rank(&existing.phase)
                    || (slot_dump_install_phase_rank(&marker.phase)
                        == slot_dump_install_phase_rank(&existing.phase)
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

pub(super) fn remove_obsolete_slot_dump_install_markers(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<usize, std::io::Error> {
    let mut removed = 0usize;
    for (marker, path) in slot_dump_install_marker_files_at(index_dir, shard_id)? {
        if marker.manifest_id == manifest_id
            && (marker.phase == "prepare" || marker.phase == "install")
            && fs::remove_file(path).is_ok()
        {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

pub(super) fn slot_dump_install_phase_counts(
    markers: &[SlotDumpInstallMarker],
) -> (usize, usize, usize) {
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

pub(super) fn slot_dump_install_phase_rank(phase: &str) -> u8 {
    match phase {
        "prepare" => 1,
        "install" => 2,
        "commit" => 3,
        _ => 0,
    }
}

pub(super) fn slot_dump_manifest_chain_issues(
    manifests: &[SlotDumpManifest],
) -> Vec<SlotDumpManifestChainIssue> {
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    manifests
        .iter()
        .filter_map(|manifest| {
            let parent = manifest.parent_manifest_id.as_ref()?;
            (!manifest_ids.contains(parent)).then(|| SlotDumpManifestChainIssue {
                manifest_id: manifest.manifest_id.clone(),
                parent_manifest_id: Some(parent.clone()),
                reason: "missing_parent_manifest".to_string(),
            })
        })
        .collect()
}

pub(super) fn retained_slot_dump_manifest_ids(manifests: &[SlotDumpManifest]) -> BTreeSet<String> {
    let by_id = manifests
        .iter()
        .map(|manifest| (manifest.manifest_id.clone(), manifest))
        .collect::<BTreeMap<_, _>>();
    let mut retained = BTreeSet::new();
    let mut cursor = manifests
        .iter()
        .max_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms))
        .map(|manifest| manifest.manifest_id.clone());
    while let Some(manifest_id) = cursor {
        if !retained.insert(manifest_id.clone()) {
            break;
        }
        cursor = by_id
            .get(&manifest_id)
            .and_then(|manifest| manifest.parent_manifest_id.clone());
    }
    retained
}

pub(super) fn slot_dump_manifest_prune_plan_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    follower_cursors: &[SlotDumpFollowerReplayCursor],
    raft_snapshot_refs: &[SlotDumpRaftSnapshotRef],
) -> Result<SlotDumpManifestPrunePlan, std::io::Error> {
    let manifests = list_slot_dump_manifests_at(index_dir, shard_id)?;
    let mut retained = retained_slot_dump_manifest_ids(&manifests);
    let mut follower_blocks = Vec::new();
    let mut raft_snapshot_blocks = Vec::new();
    for cursor in follower_cursors
        .iter()
        .filter(|cursor| cursor.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.oplog_sequence <= cursor.oplog_sequence
                && manifest.index_log_sequence <= cursor.index_log_sequence
        }) else {
            continue;
        };
        if retained.insert(anchor.manifest_id.clone()) {
            follower_blocks.push(SlotDumpFollowerRetentionBlock {
                follower_id: cursor.follower_id.clone(),
                manifest_id: anchor.manifest_id.clone(),
                manifest_oplog_sequence: anchor.oplog_sequence,
                manifest_index_log_sequence: anchor.index_log_sequence,
                cursor_oplog_sequence: cursor.oplog_sequence,
                cursor_index_log_sequence: cursor.index_log_sequence,
                reason: "follower_cursor_anchor".to_string(),
            });
        }
    }
    for snapshot in raft_snapshot_refs
        .iter()
        .filter(|snapshot| snapshot.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.oplog_sequence <= snapshot.oplog_sequence
                && manifest.index_log_sequence <= snapshot.index_log_sequence
        }) else {
            continue;
        };
        if retained.insert(anchor.manifest_id.clone()) {
            raft_snapshot_blocks.push(SlotDumpRaftSnapshotRetentionBlock {
                snapshot_id: snapshot.snapshot_id.clone(),
                manifest_id: anchor.manifest_id.clone(),
                manifest_oplog_sequence: anchor.oplog_sequence,
                manifest_index_log_sequence: anchor.index_log_sequence,
                snapshot_oplog_sequence: snapshot.oplog_sequence,
                snapshot_index_log_sequence: snapshot.index_log_sequence,
                last_included_index: snapshot.last_included_index,
                last_included_term: snapshot.last_included_term,
                reason: "raft_snapshot_anchor".to_string(),
            });
        }
    }
    let interrupted = interrupted_slot_dump_installs_at(index_dir, shard_id)?
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
    let prunable_marker_manifest_ids = list_slot_dump_install_markers_at(index_dir, shard_id)?
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
    Ok(SlotDumpManifestPrunePlan {
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

pub(super) fn list_slot_dump_manifests_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpManifest>, std::io::Error> {
    let dir = slot_dump_manifest_dir(index_dir, shard_id);
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
        let manifest = serde_json::from_slice::<SlotDumpManifest>(&fs::read(entry.path())?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        manifests.push(manifest);
    }
    manifests.sort_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms));
    Ok(manifests)
}

pub(super) fn latest_slot_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Option<SlotDumpManifest> {
    list_slot_dump_manifests_at(index_dir, shard_id)
        .ok()?
        .into_iter()
        .last()
}

pub(super) fn slot_dump_manifest_checksum(manifest: &SlotDumpManifest) -> Result<String, Status> {
    let mut payload = manifest.clone();
    payload.checksum.clear();
    serde_json::to_vec(&payload)
        .map(|bytes| sha256_hex_bytes(&bytes))
        .map_err(|err| Status::error("slot_dump_checksum_failed", err.to_string()))
}

pub(super) fn slot_dump_fault_scenario(
    scenario: impl Into<String>,
    expected_code: impl Into<String>,
    actual_code: impl Into<String>,
    blockers: Vec<String>,
    install_safe: bool,
) -> SlotDumpFaultScenarioReport {
    let expected_code = expected_code.into();
    let actual_code = actual_code.into();
    SlotDumpFaultScenarioReport {
        scenario: scenario.into(),
        passed: actual_code == expected_code,
        expected_code,
        actual_code,
        blockers,
        install_safe,
    }
}

pub(super) fn slot_dump_generation_id(manifest: &SlotDumpManifest) -> String {
    let mut digest = Sha256::new();
    digest.update(manifest.shard_id.to_le_bytes());
    digest.update(manifest.oplog_sequence.to_le_bytes());
    digest.update(manifest.index_log_sequence.to_le_bytes());
    for slot_id in &manifest.slot_ids {
        digest.update(slot_id.to_le_bytes());
    }
    for page_segment_id in &manifest.page_segment_ids {
        digest.update(page_segment_id.to_le_bytes());
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

pub(super) fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
