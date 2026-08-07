//! Band manifest load/rebuild/reconcile/persist + band descriptor maintenance, extracted from block_store.rs.

use super::*;
use super::slab_ids::*;
use std::fs::{self, File};
use std::path::Path;

pub(super) fn load_band_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreBandDescriptor>, BlockStoreError> {
    let current_path = band_manifest_path(root);
    let legacy_path = legacy_zone_manifest_path(root);
    let path = if current_path.exists() {
        current_path
    } else {
        legacy_path
    };
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let manifest: BlockStoreBandManifest =
        serde_json::from_slice(&fs::read(path)?).map_err(|err| {
            BlockStoreError::CorruptPageEnvelope {
                page_slab_id: 0,
                offset: 0,
                reason: format!("corrupt band manifest: {err}"),
            }
        })?;
    Ok(manifest
        .bands
        .into_iter()
        .map(|band| (band.page_slab_id, band))
        .collect())
}

pub(super) fn rebuild_band_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreBandDescriptor>, BlockStoreError> {
    let mut bands = BTreeMap::new();
    let latest = latest_slab_id_at(root)?;
    for page_slab_id in slab_ids_at(root)? {
        let path = slab_path(root, page_slab_id);
        let bytes = fs::read(&path)?;
        let report = inspect_slab(&bytes, page_slab_id);
        bands.insert(
            page_slab_id,
            BlockStoreBandDescriptor {
                band_id: band_id_for_slab(page_slab_id),
                page_slab_id,
                state: if page_slab_id == latest {
                    BlockStoreBandState::Active
                } else {
                    BlockStoreBandState::Sealed
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
    for delayed in delayed_destroy_slab_reports_at(root)? {
        bands
            .entry(delayed.page_slab_id)
            .and_modify(|band| {
                band.state = BlockStoreBandState::DelayedDestroy;
                band.updated_unix_ms = delayed.modified_unix_ms;
                band.physical_bytes = delayed.physical_bytes;
            })
            .or_insert(BlockStoreBandDescriptor {
                band_id: band_id_for_slab(delayed.page_slab_id),
                page_slab_id: delayed.page_slab_id,
                state: BlockStoreBandState::DelayedDestroy,
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
    Ok(bands)
}

pub(super) fn reconcile_band_manifest_with_disk(
    root: &Path,
    bands: &mut BTreeMap<u64, BlockStoreBandDescriptor>,
) -> Result<bool, BlockStoreError> {
    let mut changed = false;
    let live_slab_ids = slab_ids_at(root)?.into_iter().collect::<BTreeSet<_>>();
    let delayed_slabs = delayed_destroy_slab_reports_at(root)?
        .into_iter()
        .map(|report| (report.page_slab_id, report))
        .collect::<BTreeMap<_, _>>();
    let latest = live_slab_ids
        .iter()
        .next_back()
        .copied()
        .unwrap_or_default();

    for page_slab_id in &live_slab_ids {
        let path = slab_path(root, *page_slab_id);
        let bytes = fs::read(&path)?;
        let report = inspect_slab(&bytes, *page_slab_id);
        let desired_state = if *page_slab_id == latest {
            BlockStoreBandState::Active
        } else {
            BlockStoreBandState::Sealed
        };
        let created_unix_ms = file_created_unix_ms(&path).or_else(|| file_modified_unix_ms(&path));
        let updated_unix_ms = file_modified_unix_ms(&path).or_else(|| file_created_unix_ms(&path));
        match bands.get_mut(page_slab_id) {
            Some(band) => {
                let old = band.clone();
                let content_changed = band.band_id != band_id_for_slab(*page_slab_id)
                    || band.page_slab_id != *page_slab_id
                    || band.state != desired_state
                    || band.physical_bytes != bytes.len() as u64
                    || band.logical_bytes != report.logical_bytes
                    || band.first_page_id != report.first_page_id
                    || band.last_page_id != report.last_page_id
                    || band.readable_prefix_physical_bytes
                        != report.readable_prefix_physical_bytes
                    || band.has_corruption != report.has_corruption
                    || band.first_error_offset != report.first_error_offset
                    || band.first_error != report.first_error;
                band.band_id = band_id_for_slab(*page_slab_id);
                band.page_slab_id = *page_slab_id;
                band.state = desired_state;
                band.physical_bytes = bytes.len() as u64;
                band.logical_bytes = report.logical_bytes;
                band.created_unix_ms = band.created_unix_ms.or(created_unix_ms);
                if content_changed {
                    band.updated_unix_ms = updated_unix_ms;
                }
                band.first_page_id = report.first_page_id;
                band.last_page_id = report.last_page_id;
                band.readable_prefix_physical_bytes = report.readable_prefix_physical_bytes;
                band.has_corruption = report.has_corruption;
                band.first_error_offset = report.first_error_offset;
                band.first_error = report.first_error;
                changed |= *band != old;
            }
            None => {
                bands.insert(
                    *page_slab_id,
                    BlockStoreBandDescriptor {
                        band_id: band_id_for_slab(*page_slab_id),
                        page_slab_id: *page_slab_id,
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

    for (page_slab_id, report) in &delayed_slabs {
        let old = bands.get(page_slab_id).cloned();
        bands.insert(
            *page_slab_id,
            BlockStoreBandDescriptor {
                band_id: band_id_for_slab(*page_slab_id),
                page_slab_id: *page_slab_id,
                state: BlockStoreBandState::DelayedDestroy,
                physical_bytes: report.physical_bytes,
                logical_bytes: old.as_ref().map(|band| band.logical_bytes).unwrap_or(0),
                created_unix_ms: old
                    .as_ref()
                    .and_then(|band| band.created_unix_ms)
                    .or(report.modified_unix_ms),
                updated_unix_ms: report.modified_unix_ms,
                first_page_id: old.as_ref().and_then(|band| band.first_page_id),
                last_page_id: old.as_ref().and_then(|band| band.last_page_id),
                readable_prefix_physical_bytes: 0,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        changed |= bands.get(page_slab_id) != old.as_ref();
    }

    let known_ids = bands.keys().copied().collect::<Vec<_>>();
    for page_slab_id in known_ids {
        if live_slab_ids.contains(&page_slab_id)
            || delayed_slabs.contains_key(&page_slab_id)
        {
            continue;
        }
        if let Some(band) = bands.get_mut(&page_slab_id) {
            if band.state != BlockStoreBandState::Purged {
                band.state = BlockStoreBandState::Purged;
                band.updated_unix_ms = Some(now_unix_ms());
                changed = true;
            }
        }
    }

    Ok(changed)
}

pub(super) fn persist_band_manifest(
    root: &Path,
    bands: &BTreeMap<u64, BlockStoreBandDescriptor>,
) -> Result<(), BlockStoreError> {
    fs::create_dir_all(root)?;
    let path = band_manifest_path(root);
    let temp_path = path.with_extension(format!(
        "json.tmp.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ));
    let manifest = BlockStoreBandManifest {
        version: 1,
        bands: bands.values().cloned().collect(),
    };
    {
        let mut temp = File::create(&temp_path)?;
        serde_json::to_writer_pretty(&mut temp, &manifest).map_err(|err| {
            BlockStoreError::CorruptPageEnvelope {
                page_slab_id: 0,
                offset: 0,
                reason: format!("serialize band manifest: {err}"),
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

pub(super) fn summarize_bands(
    bands: &BTreeMap<u64, BlockStoreBandDescriptor>,
) -> BlockStoreBandSummary {
    let mut summary = BlockStoreBandSummary::default();
    let now = now_unix_ms();
    for band in bands.values() {
        update_oldest_band_timestamp(&mut summary.oldest_known_band_unix_ms, band);
        summary.total_known_physical_bytes = summary
            .total_known_physical_bytes
            .saturating_add(band.physical_bytes);
        match band.state {
            BlockStoreBandState::Active => {
                update_oldest_band_timestamp(&mut summary.oldest_live_band_unix_ms, band);
                summary.active_bands = summary.active_bands.saturating_add(1);
                summary.active_physical_bytes = summary
                    .active_physical_bytes
                    .saturating_add(band.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(band.physical_bytes);
            }
            BlockStoreBandState::Sealed => {
                update_oldest_band_timestamp(&mut summary.oldest_live_band_unix_ms, band);
                summary.sealed_bands = summary.sealed_bands.saturating_add(1);
                summary.sealed_physical_bytes = summary
                    .sealed_physical_bytes
                    .saturating_add(band.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(band.physical_bytes);
            }
            BlockStoreBandState::DelayedDestroy => {
                update_oldest_band_timestamp(
                    &mut summary.oldest_reclaimable_band_unix_ms,
                    band,
                );
                summary.delayed_destroy_bands = summary.delayed_destroy_bands.saturating_add(1);
                summary.delayed_destroy_physical_bytes = summary
                    .delayed_destroy_physical_bytes
                    .saturating_add(band.physical_bytes);
                summary.reclaimable_physical_bytes = summary
                    .reclaimable_physical_bytes
                    .saturating_add(band.physical_bytes);
            }
            BlockStoreBandState::Purged => {
                summary.purged_bands = summary.purged_bands.saturating_add(1);
                summary.purged_physical_bytes = summary
                    .purged_physical_bytes
                    .saturating_add(band.physical_bytes);
            }
        }
    }
    summary.oldest_known_band_age_ms = summary
        .oldest_known_band_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_live_band_age_ms = summary
        .oldest_live_band_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_reclaimable_band_age_ms = summary
        .oldest_reclaimable_band_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary
}

pub(super) fn update_oldest_band_timestamp(target: &mut Option<u64>, band: &BlockStoreBandDescriptor) {
    let Some(timestamp) = band.updated_unix_ms.or(band.created_unix_ms) else {
        return;
    };
    if target.map(|current| timestamp < current).unwrap_or(true) {
        *target = Some(timestamp);
    }
}

pub(super) fn ensure_band_descriptor(
    bands: &mut BTreeMap<u64, BlockStoreBandDescriptor>,
    root: &Path,
    page_slab_id: u64,
    state: BlockStoreBandState,
) {
    bands.entry(page_slab_id).or_insert_with(|| {
        let physical_bytes = slab_path(root, page_slab_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        BlockStoreBandDescriptor {
            band_id: band_id_for_slab(page_slab_id),
            page_slab_id,
            state,
            physical_bytes,
            logical_bytes: physical_bytes,
            created_unix_ms: file_created_unix_ms(&slab_path(root, page_slab_id))
                .or_else(|| file_modified_unix_ms(&slab_path(root, page_slab_id))),
            updated_unix_ms: file_modified_unix_ms(&slab_path(root, page_slab_id)),
            first_page_id: None,
            last_page_id: None,
            readable_prefix_physical_bytes: physical_bytes,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        }
    });
    let transition_unix_ms = now_unix_ms();
    for band in bands.values_mut() {
        if band.page_slab_id == page_slab_id {
            band.state = state;
            band.updated_unix_ms = Some(transition_unix_ms);
        } else if band.state == BlockStoreBandState::Active {
            band.state = BlockStoreBandState::Sealed;
            band.updated_unix_ms = Some(transition_unix_ms);
        }
    }
}

pub(super) fn upsert_band_after_append(
    bands: &mut BTreeMap<u64, BlockStoreBandDescriptor>,
    page_slab_id: u64,
    physical_bytes: u64,
    logical_bytes_written: u64,
    page_id: u64,
) {
    let band = bands
        .entry(page_slab_id)
        .or_insert(BlockStoreBandDescriptor {
            band_id: band_id_for_slab(page_slab_id),
            page_slab_id,
            state: BlockStoreBandState::Active,
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
    band.state = BlockStoreBandState::Active;
    band.physical_bytes = physical_bytes;
    band.readable_prefix_physical_bytes = physical_bytes;
    band.has_corruption = false;
    band.first_error_offset = None;
    band.first_error = None;
    band.logical_bytes = band.logical_bytes.saturating_add(logical_bytes_written);
    if band.created_unix_ms.is_none() {
        band.created_unix_ms = Some(updated_unix_ms);
    }
    band.updated_unix_ms = Some(updated_unix_ms);
    band.first_page_id = Some(
        band
            .first_page_id
            .map_or(page_id, |first| first.min(page_id)),
    );
    band.last_page_id = Some(
        band
            .last_page_id
            .map_or(page_id, |last| last.max(page_id)),
    );
}

pub(super) fn set_band_state(
    bands: &mut BTreeMap<u64, BlockStoreBandDescriptor>,
    page_slab_id: u64,
    state: BlockStoreBandState,
) {
    bands
        .entry(page_slab_id)
        .and_modify(|band| {
            band.state = state;
            band.updated_unix_ms = Some(now_unix_ms());
        })
        .or_insert(BlockStoreBandDescriptor {
            band_id: band_id_for_slab(page_slab_id),
            page_slab_id,
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

