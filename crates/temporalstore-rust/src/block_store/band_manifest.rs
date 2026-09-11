// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Band manifest load/rebuild/reconcile/persist + band descriptor maintenance, extracted from block_store.rs.

use super::*;
use super::slab_ids::*;
use std::fs::{self, File};
use std::path::Path;

pub(super) fn load_band_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreSlabDescriptor>, BlockStoreError> {
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
    let manifest: BlockStoreSlabManifest =
        serde_json::from_slice(&fs::read(path)?).map_err(|err| {
            BlockStoreError::CorruptPageEnvelope {
                block_slab_id: 0,
                offset: 0,
                reason: format!("corrupt band manifest: {err}"),
            }
        })?;
    Ok(manifest
        .bands
        .into_iter()
        .map(|band| (band.block_slab_id, band))
        .collect())
}

pub(super) fn rebuild_band_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreSlabDescriptor>, BlockStoreError> {
    let mut bands = BTreeMap::new();
    let latest = latest_slab_id_at(root)?;
    for block_slab_id in slab_ids_at(root)? {
        let path = slab_path(root, block_slab_id);
        let bytes = fs::read(&path)?;
        let report = inspect_slab(&bytes, block_slab_id);
        bands.insert(
            block_slab_id,
            BlockStoreSlabDescriptor {
                band_id: band_id_for_slab(block_slab_id),
                block_slab_id,
                state: if block_slab_id == latest {
                    BlockStoreSlabState::Active
                } else {
                    BlockStoreSlabState::Sealed
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
                // This path inspected the slab, so record the identity it was verified against;
                // leaving it empty makes the next reconcile re-read a slab this one just proved.
                verified_source_mtime_unix_ms: file_modified_unix_ms(&path)
                    .or_else(|| file_created_unix_ms(&path)),
                has_corruption: report.has_corruption,
                first_error_offset: report.first_error_offset,
                first_error: report.first_error,
            },
        );
    }
    for delayed in delayed_destroy_slab_reports_at(root)? {
        bands
            .entry(delayed.block_slab_id)
            .and_modify(|band| {
                band.state = BlockStoreSlabState::DelayedDestroy;
                band.updated_unix_ms = delayed.modified_unix_ms;
                band.physical_bytes = delayed.physical_bytes;
            })
            .or_insert(BlockStoreSlabDescriptor {
                band_id: band_id_for_slab(delayed.block_slab_id),
                block_slab_id: delayed.block_slab_id,
                state: BlockStoreSlabState::DelayedDestroy,
                physical_bytes: delayed.physical_bytes,
                logical_bytes: 0,
                created_unix_ms: delayed.modified_unix_ms,
                updated_unix_ms: delayed.modified_unix_ms,
                first_page_id: None,
                last_page_id: None,
                readable_prefix_physical_bytes: 0,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            });
    }
    Ok(bands)
}

/// Re-verify every slab on every open, as before this was made skippable.
///
/// The escape hatch for a deployment that suspects its slabs: it costs the full cold-start CPU
/// again, which is the point.
fn reverify_all_slabs() -> bool {
    std::env::var("TS_REVERIFY_ALL_SLABS")
        .map(|value| matches!(value.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

pub(super) fn reconcile_band_manifest_with_disk(
    root: &Path,
    bands: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> Result<bool, BlockStoreError> {
    let mut changed = false;
    let live_slab_ids = slab_ids_at(root)?.into_iter().collect::<BTreeSet<_>>();
    let delayed_slabs = delayed_destroy_slab_reports_at(root)?
        .into_iter()
        .map(|report| (report.block_slab_id, report))
        .collect::<BTreeMap<_, _>>();
    let latest = live_slab_ids
        .iter()
        .next_back()
        .copied()
        .unwrap_or_default();

    // Read and hash the slabs in parallel, then apply the band updates in the original order.
    //
    // Inspecting a slab re-verifies the sha256 of every page record in it, and this runs at EVERY
    // engine open. Measured on a live store: ~950 MB of slabs took about 70 s of a cold start
    // (13.5 MB/s, against hundreds of MB/s for sha256) with the process pinned at 99% of one core.
    // Only the reads and hashing are shared out; the update loop below is unchanged and still walks
    // `live_slab_ids` in order, so it makes the same decisions in the same sequence.
    let mut ordered_slab_ids = live_slab_ids.iter().copied().collect::<Vec<_>>();
    // Leave out the sealed slabs whose file is still exactly what their descriptor was verified
    // against. Inspecting one re-reads it and re-hashes every record in it, which is the bulk of a
    // cold open -- 30.7 s of CPU in a 32.0 s restart on this store, 96% CPU-bound -- and for a slab
    // nobody has touched it recomputes an answer already on disk.
    //
    // Deliberately narrow. Only a SEALED slab already in the manifest, not marked corrupt, already
    // in the state it should be in, whose size AND mtime both still match what was verified. The
    // active slab is always inspected: it is the one being appended to. Anything unreadable or
    // unrecorded falls through to a full inspection, so the skip can only ever be taken on evidence.
    if !reverify_all_slabs() {
        ordered_slab_ids.retain(|block_slab_id| {
            if *block_slab_id == latest {
                return true;
            }
            let Some(band) = bands.get(block_slab_id) else {
                return true;
            };
            if band.has_corruption || band.state != BlockStoreSlabState::Sealed {
                return true;
            }
            let Some(verified_mtime) = band.verified_source_mtime_unix_ms else {
                return true;
            };
            let path = slab_path(root, *block_slab_id);
            let Ok(meta) = fs::metadata(&path) else {
                return true;
            };
            if meta.len() != band.physical_bytes {
                return true;
            }
            file_modified_unix_ms(&path) != Some(verified_mtime)
        });
    }
    let workers = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .clamp(1, 8)
        .min(ordered_slab_ids.len().max(1));
    type InspectedSlab = (u64, BlockStoreSlabReport, Option<u64>, Option<u64>);
    // A BATCH at a time. Fanning out over the whole list first was the same 2x, but held every
    // slab's report until the update loop ran and took peak RSS from 385 MB to ~960 MB. Worker
    // count made no difference to that, which is the tell: it is the retained reports, not the slab
    // buffers. One batch in flight keeps the speed and the memory.
    for batch in ordered_slab_ids.chunks(workers.max(1)) {
        let mut inspected: Vec<Option<Result<InspectedSlab, std::io::Error>>> =
            (0..batch.len()).map(|_| None).collect();
        std::thread::scope(|scope| {
            for (slot, block_slab_id) in inspected.iter_mut().zip(batch.iter()) {
                scope.spawn(move || {
                    let path = slab_path(root, *block_slab_id);
                    *slot = Some(fs::read(&path).map(|bytes| {
                        let report = inspect_slab(&bytes, *block_slab_id);
                        let created =
                            file_created_unix_ms(&path).or_else(|| file_modified_unix_ms(&path));
                        let updated =
                            file_modified_unix_ms(&path).or_else(|| file_created_unix_ms(&path));
                        (bytes.len() as u64, report, created, updated)
                    }));
                });
            }
        });

        for (slot, block_slab_id) in inspected.iter_mut().zip(batch.iter()) {
            let (physical_bytes, report, created_unix_ms, updated_unix_ms) = slot
                .take()
                .expect("every slab in the batch is inspected exactly once")?;
            let desired_state = if *block_slab_id == latest {
            BlockStoreSlabState::Active
        } else {
            BlockStoreSlabState::Sealed
        };
        match bands.get_mut(block_slab_id) {
            Some(band) => {
                let old = band.clone();
                let content_changed = band.band_id != band_id_for_slab(*block_slab_id)
                    || band.block_slab_id != *block_slab_id
                    || band.state != desired_state
                    || band.physical_bytes != physical_bytes
                    || band.logical_bytes != report.logical_bytes
                    || band.first_page_id != report.first_page_id
                    || band.last_page_id != report.last_page_id
                    || band.readable_prefix_physical_bytes
                        != report.readable_prefix_physical_bytes
                    || band.has_corruption != report.has_corruption
                    || band.first_error_offset != report.first_error_offset
                    || band.first_error != report.first_error;
                band.band_id = band_id_for_slab(*block_slab_id);
                band.block_slab_id = *block_slab_id;
                band.state = desired_state;
                band.physical_bytes = physical_bytes;
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
                // What this descriptor has now been verified against, so the next open can tell
                // whether the file still matches without reading it.
                band.verified_source_mtime_unix_ms = updated_unix_ms;
                changed |= *band != old;
            }
            None => {
                bands.insert(
                    *block_slab_id,
                    BlockStoreSlabDescriptor {
                        band_id: band_id_for_slab(*block_slab_id),
                        block_slab_id: *block_slab_id,
                        state: desired_state,
                        physical_bytes,
                        logical_bytes: report.logical_bytes,
                        created_unix_ms,
                        updated_unix_ms,
                        first_page_id: report.first_page_id,
                        last_page_id: report.last_page_id,
                        readable_prefix_physical_bytes: report.readable_prefix_physical_bytes,
                        // Just inspected, so record what it was verified against; otherwise the
                        // next open re-reads and re-hashes a slab this one already proved.
                        verified_source_mtime_unix_ms: updated_unix_ms,
                        has_corruption: report.has_corruption,
                        first_error_offset: report.first_error_offset,
                        first_error: report.first_error,
                    },
                );
                changed = true;
            }
            }
        }
    }

    for (block_slab_id, report) in &delayed_slabs {
        let old = bands.get(block_slab_id).cloned();
        bands.insert(
            *block_slab_id,
            BlockStoreSlabDescriptor {
                band_id: band_id_for_slab(*block_slab_id),
                block_slab_id: *block_slab_id,
                state: BlockStoreSlabState::DelayedDestroy,
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
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        changed |= bands.get(block_slab_id) != old.as_ref();
    }

    let known_ids = bands.keys().copied().collect::<Vec<_>>();
    for block_slab_id in known_ids {
        if live_slab_ids.contains(&block_slab_id)
            || delayed_slabs.contains_key(&block_slab_id)
        {
            continue;
        }
        if let Some(band) = bands.get_mut(&block_slab_id) {
            if band.state != BlockStoreSlabState::Purged {
                band.state = BlockStoreSlabState::Purged;
                band.updated_unix_ms = Some(now_unix_ms());
                changed = true;
            }
        }
    }

    Ok(changed)
}

pub(super) fn persist_band_manifest(
    root: &Path,
    bands: &BTreeMap<u64, BlockStoreSlabDescriptor>,
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
    let manifest = BlockStoreSlabManifest {
        version: 1,
        bands: bands.values().cloned().collect(),
    };
    {
        let mut temp = File::create(&temp_path)?;
        serde_json::to_writer_pretty(&mut temp, &manifest).map_err(|err| {
            BlockStoreError::CorruptPageEnvelope {
                block_slab_id: 0,
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
    bands: &BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> BlockStoreSlabSummary {
    let mut summary = BlockStoreSlabSummary::default();
    let now = now_unix_ms();
    for band in bands.values() {
        update_oldest_band_timestamp(&mut summary.oldest_known_slab_unix_ms, band);
        summary.total_known_physical_bytes = summary
            .total_known_physical_bytes
            .saturating_add(band.physical_bytes);
        match band.state {
            BlockStoreSlabState::Active => {
                update_oldest_band_timestamp(&mut summary.oldest_live_band_unix_ms, band);
                summary.active_slabs = summary.active_slabs.saturating_add(1);
                summary.active_physical_bytes = summary
                    .active_physical_bytes
                    .saturating_add(band.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(band.physical_bytes);
            }
            BlockStoreSlabState::Sealed => {
                update_oldest_band_timestamp(&mut summary.oldest_live_band_unix_ms, band);
                summary.sealed_slabs = summary.sealed_slabs.saturating_add(1);
                summary.sealed_physical_bytes = summary
                    .sealed_physical_bytes
                    .saturating_add(band.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(band.physical_bytes);
            }
            BlockStoreSlabState::DelayedDestroy => {
                update_oldest_band_timestamp(
                    &mut summary.oldest_reclaimable_band_unix_ms,
                    band,
                );
                summary.delayed_destroy_slabs = summary.delayed_destroy_slabs.saturating_add(1);
                summary.delayed_destroy_physical_bytes = summary
                    .delayed_destroy_physical_bytes
                    .saturating_add(band.physical_bytes);
                summary.reclaimable_physical_bytes = summary
                    .reclaimable_physical_bytes
                    .saturating_add(band.physical_bytes);
            }
            BlockStoreSlabState::Purged => {
                summary.purged_slabs = summary.purged_slabs.saturating_add(1);
                summary.purged_physical_bytes = summary
                    .purged_physical_bytes
                    .saturating_add(band.physical_bytes);
            }
        }
    }
    summary.oldest_known_slab_age_ms = summary
        .oldest_known_slab_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_live_band_age_ms = summary
        .oldest_live_band_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_reclaimable_band_age_ms = summary
        .oldest_reclaimable_band_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary
}

pub(super) fn update_oldest_band_timestamp(target: &mut Option<u64>, band: &BlockStoreSlabDescriptor) {
    let Some(timestamp) = band.updated_unix_ms.or(band.created_unix_ms) else {
        return;
    };
    if target.map(|current| timestamp < current).unwrap_or(true) {
        *target = Some(timestamp);
    }
}

pub(super) fn ensure_band_descriptor(
    bands: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
    root: &Path,
    block_slab_id: u64,
    state: BlockStoreSlabState,
) {
    bands.entry(block_slab_id).or_insert_with(|| {
        let physical_bytes = slab_path(root, block_slab_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        BlockStoreSlabDescriptor {
            band_id: band_id_for_slab(block_slab_id),
            block_slab_id,
            state,
            physical_bytes,
            logical_bytes: physical_bytes,
            created_unix_ms: file_created_unix_ms(&slab_path(root, block_slab_id))
                .or_else(|| file_modified_unix_ms(&slab_path(root, block_slab_id))),
            updated_unix_ms: file_modified_unix_ms(&slab_path(root, block_slab_id)),
            first_page_id: None,
            last_page_id: None,
            readable_prefix_physical_bytes: physical_bytes,
            verified_source_mtime_unix_ms: None,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        }
    });
    let transition_unix_ms = now_unix_ms();
    for band in bands.values_mut() {
        if band.block_slab_id == block_slab_id {
            band.state = state;
            band.updated_unix_ms = Some(transition_unix_ms);
        } else if band.state == BlockStoreSlabState::Active {
            band.state = BlockStoreSlabState::Sealed;
            band.updated_unix_ms = Some(transition_unix_ms);
        }
    }
}

pub(super) fn upsert_band_after_append(
    bands: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
    block_slab_id: u64,
    physical_bytes: u64,
    logical_bytes_written: u64,
    page_id: u64,
) {
    let band = bands
        .entry(block_slab_id)
        .or_insert(BlockStoreSlabDescriptor {
            band_id: band_id_for_slab(block_slab_id),
            block_slab_id,
            state: BlockStoreSlabState::Active,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_page_id: Some(page_id),
            last_page_id: Some(page_id),
            readable_prefix_physical_bytes: 0,
            verified_source_mtime_unix_ms: None,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        });
    let updated_unix_ms = now_unix_ms();
    band.state = BlockStoreSlabState::Active;
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
    bands: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
    block_slab_id: u64,
    state: BlockStoreSlabState,
) {
    bands
        .entry(block_slab_id)
        .and_modify(|band| {
            band.state = state;
            band.updated_unix_ms = Some(now_unix_ms());
        })
        .or_insert(BlockStoreSlabDescriptor {
            band_id: band_id_for_slab(block_slab_id),
            block_slab_id,
            state,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_page_id: None,
            last_page_id: None,
            readable_prefix_physical_bytes: 0,
            verified_source_mtime_unix_ms: None,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        });
}

