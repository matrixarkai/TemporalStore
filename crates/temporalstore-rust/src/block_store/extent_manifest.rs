//! Extent manifest load/rebuild/reconcile/persist + extent descriptor maintenance, extracted from block_store.rs.

use super::*;
use std::fs;
use std::path::Path;

pub(super) fn load_extent_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreExtentDescriptor>, BlockStoreError> {
    let current_path = extent_manifest_path(root);
    let legacy_path = legacy_zone_manifest_path(root);
    let path = if current_path.exists() {
        current_path
    } else {
        legacy_path
    };
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let manifest: BlockStoreExtentManifest =
        serde_json::from_slice(&fs::read(path)?).map_err(|err| {
            BlockStoreError::CorruptPageEnvelope {
                page_segment_id: 0,
                offset: 0,
                reason: format!("corrupt extent manifest: {err}"),
            }
        })?;
    Ok(manifest
        .extents
        .into_iter()
        .map(|extent| (extent.page_segment_id, extent))
        .collect())
}

pub(super) fn rebuild_extent_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreExtentDescriptor>, BlockStoreError> {
    let mut extents = BTreeMap::new();
    let latest = latest_segment_id_at(root)?;
    for page_segment_id in segment_ids_at(root)? {
        let path = segment_path(root, page_segment_id);
        let bytes = fs::read(&path)?;
        let report = inspect_segment(&bytes, page_segment_id);
        extents.insert(
            page_segment_id,
            BlockStoreExtentDescriptor {
                extent_id: extent_id_for_segment(page_segment_id),
                page_segment_id,
                state: if page_segment_id == latest {
                    BlockStoreExtentState::Active
                } else {
                    BlockStoreExtentState::Sealed
                },
                physical_bytes: bytes.len() as u64,
                logical_bytes: report.logical_bytes,
                created_unix_ms: file_created_unix_ms(&path)
                    .or_else(|| file_modified_unix_ms(&path)),
                updated_unix_ms: file_modified_unix_ms(&path)
                    .or_else(|| file_created_unix_ms(&path)),
                first_page_id: report.first_page_id,
                last_page_id: report.last_page_id,
                readable_prefix_physical_bytes: report.readable_prefix_physical_bytes,
                has_corruption: report.has_corruption,
                first_error_offset: report.first_error_offset,
                first_error: report.first_error,
            },
        );
    }
    for delayed in delayed_destroy_segment_reports_at(root)? {
        extents
            .entry(delayed.page_segment_id)
            .and_modify(|extent| {
                extent.state = BlockStoreExtentState::DelayedDestroy;
                extent.updated_unix_ms = delayed.modified_unix_ms;
                extent.physical_bytes = delayed.physical_bytes;
            })
            .or_insert(BlockStoreExtentDescriptor {
                extent_id: extent_id_for_segment(delayed.page_segment_id),
                page_segment_id: delayed.page_segment_id,
                state: BlockStoreExtentState::DelayedDestroy,
                physical_bytes: delayed.physical_bytes,
                logical_bytes: 0,
                created_unix_ms: delayed.modified_unix_ms,
                updated_unix_ms: delayed.modified_unix_ms,
                first_page_id: None,
                last_page_id: None,
                readable_prefix_physical_bytes: 0,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            });
    }
    Ok(extents)
}

pub(super) fn reconcile_extent_manifest_with_disk(
    root: &Path,
    extents: &mut BTreeMap<u64, BlockStoreExtentDescriptor>,
) -> Result<bool, BlockStoreError> {
    let mut changed = false;
    let live_segment_ids = segment_ids_at(root)?.into_iter().collect::<BTreeSet<_>>();
    let delayed_segments = delayed_destroy_segment_reports_at(root)?
        .into_iter()
        .map(|report| (report.page_segment_id, report))
        .collect::<BTreeMap<_, _>>();
    let latest = live_segment_ids
        .iter()
        .next_back()
        .copied()
        .unwrap_or_default();

    for page_segment_id in &live_segment_ids {
        let path = segment_path(root, *page_segment_id);
        let bytes = fs::read(&path)?;
        let report = inspect_segment(&bytes, *page_segment_id);
        let desired_state = if *page_segment_id == latest {
            BlockStoreExtentState::Active
        } else {
            BlockStoreExtentState::Sealed
        };
        let created_unix_ms = file_created_unix_ms(&path).or_else(|| file_modified_unix_ms(&path));
        let updated_unix_ms = file_modified_unix_ms(&path).or_else(|| file_created_unix_ms(&path));
        match extents.get_mut(page_segment_id) {
            Some(extent) => {
                let old = extent.clone();
                let content_changed = extent.extent_id != extent_id_for_segment(*page_segment_id)
                    || extent.page_segment_id != *page_segment_id
                    || extent.state != desired_state
                    || extent.physical_bytes != bytes.len() as u64
                    || extent.logical_bytes != report.logical_bytes
                    || extent.first_page_id != report.first_page_id
                    || extent.last_page_id != report.last_page_id
                    || extent.readable_prefix_physical_bytes
                        != report.readable_prefix_physical_bytes
                    || extent.has_corruption != report.has_corruption
                    || extent.first_error_offset != report.first_error_offset
                    || extent.first_error != report.first_error;
                extent.extent_id = extent_id_for_segment(*page_segment_id);
                extent.page_segment_id = *page_segment_id;
                extent.state = desired_state;
                extent.physical_bytes = bytes.len() as u64;
                extent.logical_bytes = report.logical_bytes;
                extent.created_unix_ms = extent.created_unix_ms.or(created_unix_ms);
                if content_changed {
                    extent.updated_unix_ms = updated_unix_ms;
                }
                extent.first_page_id = report.first_page_id;
                extent.last_page_id = report.last_page_id;
                extent.readable_prefix_physical_bytes = report.readable_prefix_physical_bytes;
                extent.has_corruption = report.has_corruption;
                extent.first_error_offset = report.first_error_offset;
                extent.first_error = report.first_error;
                changed |= *extent != old;
            }
            None => {
                extents.insert(
                    *page_segment_id,
                    BlockStoreExtentDescriptor {
                        extent_id: extent_id_for_segment(*page_segment_id),
                        page_segment_id: *page_segment_id,
                        state: desired_state,
                        physical_bytes: bytes.len() as u64,
                        logical_bytes: report.logical_bytes,
                        created_unix_ms,
                        updated_unix_ms,
                        first_page_id: report.first_page_id,
                        last_page_id: report.last_page_id,
                        readable_prefix_physical_bytes: report.readable_prefix_physical_bytes,
                        has_corruption: report.has_corruption,
                        first_error_offset: report.first_error_offset,
                        first_error: report.first_error,
                    },
                );
                changed = true;
            }
        }
    }

    for (page_segment_id, report) in &delayed_segments {
        let old = extents.get(page_segment_id).cloned();
        extents.insert(
            *page_segment_id,
            BlockStoreExtentDescriptor {
                extent_id: extent_id_for_segment(*page_segment_id),
                page_segment_id: *page_segment_id,
                state: BlockStoreExtentState::DelayedDestroy,
                physical_bytes: report.physical_bytes,
                logical_bytes: old.as_ref().map(|extent| extent.logical_bytes).unwrap_or(0),
                created_unix_ms: old
                    .as_ref()
                    .and_then(|extent| extent.created_unix_ms)
                    .or(report.modified_unix_ms),
                updated_unix_ms: report.modified_unix_ms,
                first_page_id: old.as_ref().and_then(|extent| extent.first_page_id),
                last_page_id: old.as_ref().and_then(|extent| extent.last_page_id),
                readable_prefix_physical_bytes: 0,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        changed |= extents.get(page_segment_id) != old.as_ref();
    }

    let known_ids = extents.keys().copied().collect::<Vec<_>>();
    for page_segment_id in known_ids {
        if live_segment_ids.contains(&page_segment_id)
            || delayed_segments.contains_key(&page_segment_id)
        {
            continue;
        }
        if let Some(extent) = extents.get_mut(&page_segment_id) {
            if extent.state != BlockStoreExtentState::Purged {
                extent.state = BlockStoreExtentState::Purged;
                extent.updated_unix_ms = Some(now_unix_ms());
                changed = true;
            }
        }
    }

    Ok(changed)
}

pub(super) fn persist_extent_manifest(
    root: &Path,
    extents: &BTreeMap<u64, BlockStoreExtentDescriptor>,
) -> Result<(), BlockStoreError> {
    fs::create_dir_all(root)?;
    let path = extent_manifest_path(root);
    let temp_path = path.with_extension(format!(
        "json.tmp.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ));
    let manifest = BlockStoreExtentManifest {
        version: 1,
        extents: extents.values().cloned().collect(),
    };
    {
        let mut temp = File::create(&temp_path)?;
        serde_json::to_writer_pretty(&mut temp, &manifest).map_err(|err| {
            BlockStoreError::CorruptPageEnvelope {
                page_segment_id: 0,
                offset: 0,
                reason: format!("serialize extent manifest: {err}"),
            }
        })?;
        temp.write_all(b"\n")?;
        temp.flush()?;
        temp.sync_all()?;
    }
    fs::rename(&temp_path, &path)?;
    sync_parent_dir(&path)?;
    Ok(())
}

pub(super) fn summarize_extents(
    extents: &BTreeMap<u64, BlockStoreExtentDescriptor>,
) -> BlockStoreExtentSummary {
    let mut summary = BlockStoreExtentSummary::default();
    let now = now_unix_ms();
    for extent in extents.values() {
        update_oldest_extent_timestamp(&mut summary.oldest_known_extent_unix_ms, extent);
        summary.total_known_physical_bytes = summary
            .total_known_physical_bytes
            .saturating_add(extent.physical_bytes);
        match extent.state {
            BlockStoreExtentState::Active => {
                update_oldest_extent_timestamp(&mut summary.oldest_live_extent_unix_ms, extent);
                summary.active_extents = summary.active_extents.saturating_add(1);
                summary.active_physical_bytes = summary
                    .active_physical_bytes
                    .saturating_add(extent.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(extent.physical_bytes);
            }
            BlockStoreExtentState::Sealed => {
                update_oldest_extent_timestamp(&mut summary.oldest_live_extent_unix_ms, extent);
                summary.sealed_extents = summary.sealed_extents.saturating_add(1);
                summary.sealed_physical_bytes = summary
                    .sealed_physical_bytes
                    .saturating_add(extent.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(extent.physical_bytes);
            }
            BlockStoreExtentState::DelayedDestroy => {
                update_oldest_extent_timestamp(
                    &mut summary.oldest_reclaimable_extent_unix_ms,
                    extent,
                );
                summary.delayed_destroy_extents = summary.delayed_destroy_extents.saturating_add(1);
                summary.delayed_destroy_physical_bytes = summary
                    .delayed_destroy_physical_bytes
                    .saturating_add(extent.physical_bytes);
                summary.reclaimable_physical_bytes = summary
                    .reclaimable_physical_bytes
                    .saturating_add(extent.physical_bytes);
            }
            BlockStoreExtentState::Purged => {
                summary.purged_extents = summary.purged_extents.saturating_add(1);
                summary.purged_physical_bytes = summary
                    .purged_physical_bytes
                    .saturating_add(extent.physical_bytes);
            }
        }
    }
    summary.oldest_known_extent_age_ms = summary
        .oldest_known_extent_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_live_extent_age_ms = summary
        .oldest_live_extent_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_reclaimable_extent_age_ms = summary
        .oldest_reclaimable_extent_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary
}

pub(super) fn update_oldest_extent_timestamp(target: &mut Option<u64>, extent: &BlockStoreExtentDescriptor) {
    let Some(timestamp) = extent.updated_unix_ms.or(extent.created_unix_ms) else {
        return;
    };
    if target.map(|current| timestamp < current).unwrap_or(true) {
        *target = Some(timestamp);
    }
}

pub(super) fn ensure_extent_descriptor(
    extents: &mut BTreeMap<u64, BlockStoreExtentDescriptor>,
    root: &Path,
    page_segment_id: u64,
    state: BlockStoreExtentState,
) {
    extents.entry(page_segment_id).or_insert_with(|| {
        let physical_bytes = segment_path(root, page_segment_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        BlockStoreExtentDescriptor {
            extent_id: extent_id_for_segment(page_segment_id),
            page_segment_id,
            state,
            physical_bytes,
            logical_bytes: physical_bytes,
            created_unix_ms: file_created_unix_ms(&segment_path(root, page_segment_id))
                .or_else(|| file_modified_unix_ms(&segment_path(root, page_segment_id))),
            updated_unix_ms: file_modified_unix_ms(&segment_path(root, page_segment_id)),
            first_page_id: None,
            last_page_id: None,
            readable_prefix_physical_bytes: physical_bytes,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        }
    });
    let transition_unix_ms = now_unix_ms();
    for extent in extents.values_mut() {
        if extent.page_segment_id == page_segment_id {
            extent.state = state;
            extent.updated_unix_ms = Some(transition_unix_ms);
        } else if extent.state == BlockStoreExtentState::Active {
            extent.state = BlockStoreExtentState::Sealed;
            extent.updated_unix_ms = Some(transition_unix_ms);
        }
    }
}

pub(super) fn upsert_extent_after_append(
    extents: &mut BTreeMap<u64, BlockStoreExtentDescriptor>,
    page_segment_id: u64,
    physical_bytes: u64,
    logical_bytes_written: u64,
    page_id: u64,
) {
    let extent = extents
        .entry(page_segment_id)
        .or_insert(BlockStoreExtentDescriptor {
            extent_id: extent_id_for_segment(page_segment_id),
            page_segment_id,
            state: BlockStoreExtentState::Active,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_page_id: Some(page_id),
            last_page_id: Some(page_id),
            readable_prefix_physical_bytes: 0,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        });
    let updated_unix_ms = now_unix_ms();
    extent.state = BlockStoreExtentState::Active;
    extent.physical_bytes = physical_bytes;
    extent.readable_prefix_physical_bytes = physical_bytes;
    extent.has_corruption = false;
    extent.first_error_offset = None;
    extent.first_error = None;
    extent.logical_bytes = extent.logical_bytes.saturating_add(logical_bytes_written);
    if extent.created_unix_ms.is_none() {
        extent.created_unix_ms = Some(updated_unix_ms);
    }
    extent.updated_unix_ms = Some(updated_unix_ms);
    extent.first_page_id = Some(
        extent
            .first_page_id
            .map_or(page_id, |first| first.min(page_id)),
    );
    extent.last_page_id = Some(
        extent
            .last_page_id
            .map_or(page_id, |last| last.max(page_id)),
    );
}

pub(super) fn set_extent_state(
    extents: &mut BTreeMap<u64, BlockStoreExtentDescriptor>,
    page_segment_id: u64,
    state: BlockStoreExtentState,
) {
    extents
        .entry(page_segment_id)
        .and_modify(|extent| {
            extent.state = state;
            extent.updated_unix_ms = Some(now_unix_ms());
        })
        .or_insert(BlockStoreExtentDescriptor {
            extent_id: extent_id_for_segment(page_segment_id),
            page_segment_id,
            state,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_page_id: None,
            last_page_id: None,
            readable_prefix_physical_bytes: 0,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        });
}

