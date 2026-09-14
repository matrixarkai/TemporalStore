// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Slab manifest load/rebuild/reconcile/persist + slab descriptor maintenance, extracted from block_store.rs.

use super::*;
use super::slab_ids::*;
use std::fs::{self, File};
use std::path::Path;

pub(super) fn load_slab_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreSlabDescriptor>, BlockStoreError> {
    let current_path = slab_manifest_path(root);
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
            BlockStoreError::CorruptBlockEnvelope {
                block_slab_id: 0,
                offset: 0,
                reason: format!("corrupt slab manifest: {err}"),
            }
        })?;
    Ok(manifest
        .slabs
        .into_iter()
        .map(|slab| (slab.block_slab_id, slab))
        .collect())
}

pub(super) fn rebuild_slab_manifest_at(
    root: &Path,
) -> Result<BTreeMap<u64, BlockStoreSlabDescriptor>, BlockStoreError> {
    let mut slabs = BTreeMap::new();
    let latest = latest_slab_id_at(root)?;
    for block_slab_id in slab_ids_at(root)? {
        let path = slab_path(root, block_slab_id);
        let bytes = fs::read(&path)?;
        let report = inspect_slab(&bytes, block_slab_id);
        slabs.insert(
            block_slab_id,
            BlockStoreSlabDescriptor {
                stored_slab_id: block_slab_id,
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
                first_block_id: report.first_block_id,
                last_block_id: report.last_block_id,
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
        slabs
            .entry(delayed.block_slab_id)
            .and_modify(|slab| {
                slab.state = BlockStoreSlabState::DelayedDestroy;
                slab.updated_unix_ms = delayed.modified_unix_ms;
                slab.physical_bytes = delayed.physical_bytes;
            })
            .or_insert(BlockStoreSlabDescriptor {
                stored_slab_id: delayed.block_slab_id,
                block_slab_id: delayed.block_slab_id,
                state: BlockStoreSlabState::DelayedDestroy,
                physical_bytes: delayed.physical_bytes,
                logical_bytes: 0,
                created_unix_ms: delayed.modified_unix_ms,
                updated_unix_ms: delayed.modified_unix_ms,
                first_block_id: None,
                last_block_id: None,
                readable_prefix_physical_bytes: 0,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            });
    }
    Ok(slabs)
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

/// What one reconcile did, beyond whether it changed anything.
///
/// `slabs_skipped_reinspection` is the DENOMINATOR for the skip route. The skip is on by default
/// (`TS_REVERIFY_ALL_SLABS` unset) but needs a slab that is sealed AND already carries a
/// `verified_source_mtime_unix_ms` matching the file -- which the open that stamped it only wrote
/// out afterwards. A guard that opens the store twice therefore skips NOTHING and tests nothing;
/// it takes a third open. Reporting the count is what lets such a guard prove the route ran
/// before it asserts anything about it.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SlabManifestReconcileOutcome {
    pub changed: bool,
    pub slabs_skipped_reinspection: usize,
}

pub(super) fn reconcile_slab_manifest_with_disk(
    root: &Path,
    slabs: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> Result<SlabManifestReconcileOutcome, BlockStoreError> {
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

    // Read and hash the slabs in parallel, then apply the slab updates in the original order.
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
            let Some(slab) = slabs.get(block_slab_id) else {
                return true;
            };
            if slab.has_corruption || slab.state != BlockStoreSlabState::Sealed {
                return true;
            }
            let Some(verified_mtime) = slab.verified_source_mtime_unix_ms else {
                return true;
            };
            let path = slab_path(root, *block_slab_id);
            let Ok(meta) = fs::metadata(&path) else {
                return true;
            };
            if meta.len() != slab.physical_bytes {
                return true;
            }
            file_modified_unix_ms(&path) != Some(verified_mtime)
        });
    }
    let slabs_skipped_reinspection = live_slab_ids.len().saturating_sub(ordered_slab_ids.len());
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
        match slabs.get_mut(block_slab_id) {
            Some(slab) => {
                let old = slab.clone();
                let content_changed = slab.stored_slab_id != *block_slab_id
                    || slab.block_slab_id != *block_slab_id
                    || slab.state != desired_state
                    || slab.physical_bytes != physical_bytes
                    || slab.logical_bytes != report.logical_bytes
                    || slab.first_block_id != report.first_block_id
                    || slab.last_block_id != report.last_block_id
                    || slab.readable_prefix_physical_bytes
                        != report.readable_prefix_physical_bytes
                    || slab.has_corruption != report.has_corruption
                    || slab.first_error_offset != report.first_error_offset
                    || slab.first_error != report.first_error;
                slab.stored_slab_id = *block_slab_id;
                slab.block_slab_id = *block_slab_id;
                slab.state = desired_state;
                slab.physical_bytes = physical_bytes;
                slab.logical_bytes = report.logical_bytes;
                slab.created_unix_ms = slab.created_unix_ms.or(created_unix_ms);
                if content_changed {
                    slab.updated_unix_ms = updated_unix_ms;
                }
                slab.first_block_id = report.first_block_id;
                slab.last_block_id = report.last_block_id;
                slab.readable_prefix_physical_bytes = report.readable_prefix_physical_bytes;
                slab.has_corruption = report.has_corruption;
                slab.first_error_offset = report.first_error_offset;
                slab.first_error = report.first_error;
                // What this descriptor has now been verified against, so the next open can tell
                // whether the file still matches without reading it.
                slab.verified_source_mtime_unix_ms = updated_unix_ms;
                changed |= *slab != old;
            }
            None => {
                slabs.insert(
                    *block_slab_id,
                    BlockStoreSlabDescriptor {
                        stored_slab_id: *block_slab_id,
                        block_slab_id: *block_slab_id,
                        state: desired_state,
                        physical_bytes,
                        logical_bytes: report.logical_bytes,
                        created_unix_ms,
                        updated_unix_ms,
                        first_block_id: report.first_block_id,
                        last_block_id: report.last_block_id,
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
        let old = slabs.get(block_slab_id).cloned();
        slabs.insert(
            *block_slab_id,
            BlockStoreSlabDescriptor {
                stored_slab_id: *block_slab_id,
                block_slab_id: *block_slab_id,
                state: BlockStoreSlabState::DelayedDestroy,
                physical_bytes: report.physical_bytes,
                logical_bytes: old.as_ref().map(|slab| slab.logical_bytes).unwrap_or(0),
                created_unix_ms: old
                    .as_ref()
                    .and_then(|slab| slab.created_unix_ms)
                    .or(report.modified_unix_ms),
                updated_unix_ms: report.modified_unix_ms,
                first_block_id: old.as_ref().and_then(|slab| slab.first_block_id),
                last_block_id: old.as_ref().and_then(|slab| slab.last_block_id),
                readable_prefix_physical_bytes: 0,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        changed |= slabs.get(block_slab_id) != old.as_ref();
    }

    let known_ids = slabs.keys().copied().collect::<Vec<_>>();
    for block_slab_id in known_ids {
        if live_slab_ids.contains(&block_slab_id)
            || delayed_slabs.contains_key(&block_slab_id)
        {
            continue;
        }
        if let Some(slab) = slabs.get_mut(&block_slab_id) {
            if slab.state != BlockStoreSlabState::Purged {
                slab.state = BlockStoreSlabState::Purged;
                slab.updated_unix_ms = Some(now_unix_ms());
                changed = true;
            }
        }
    }

    // NORMALISE THE GROUPING ID ON EVERY DESCRIPTOR, not only on the ones this open re-read.
    //
    // The stored id IS the slab id, and every path that WRITES one now writes the slab id itself,
    // so a descriptor this process builds cannot diverge. The manifest is the one way a different
    // number enters: `stored_slab_id` serializes -- under the older `band_id` key -- and
    // `load_slab_manifest_at` keeps whatever the file carried. The inspect loop above rewrites
    // it, but only for the slabs it actually inspected, and by default this open deliberately
    // SKIPS every sealed slab whose size and mtime still match what it was verified against. A
    // descriptor on that skip route, and one for a slab no longer on disk at all, never reached
    // that write.
    //
    // It matters because consumers read the STORED value instead of recomputing it:
    // `gc_utility_candidates` groups slabs by it in two places, and `compute_slab_usage`
    // keys its per-slab usage rows by it. Two slabs carrying one id there are summed together, so
    // each slab's GC utility is scored against the other slab's bytes. Enforcing the invariant
    // once, here, at the only point a stored id enters the process, is what makes every consumer
    // -- including the next one written -- correct without having to know about this.
    for (block_slab_id, slab) in slabs.iter_mut() {
        let normalised_slab_id = *block_slab_id;
        if slab.stored_slab_id != normalised_slab_id || slab.block_slab_id != *block_slab_id {
            slab.stored_slab_id = normalised_slab_id;
            slab.block_slab_id = *block_slab_id;
            changed = true;
        }
    }

    Ok(SlabManifestReconcileOutcome {
        changed,
        slabs_skipped_reinspection,
    })
}

pub(super) fn persist_slab_manifest(
    root: &Path,
    slabs: &BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> Result<(), BlockStoreError> {
    fs::create_dir_all(root)?;
    let path = slab_manifest_path(root);
    let temp_path = path.with_extension(format!(
        "json.tmp.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ));
    let manifest = BlockStoreSlabManifest {
        version: 1,
        slabs: slabs.values().cloned().collect(),
    };
    {
        let mut temp = File::create(&temp_path)?;
        serde_json::to_writer_pretty(&mut temp, &manifest).map_err(|err| {
            BlockStoreError::CorruptBlockEnvelope {
                block_slab_id: 0,
                offset: 0,
                reason: format!("serialize slab manifest: {err}"),
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

pub(super) fn summarize_slabs(
    slabs: &BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> BlockStoreSlabSummary {
    let mut summary = BlockStoreSlabSummary::default();
    let now = now_unix_ms();
    for slab in slabs.values() {
        update_oldest_slab_timestamp(&mut summary.oldest_known_slab_unix_ms, slab);
        summary.total_known_physical_bytes = summary
            .total_known_physical_bytes
            .saturating_add(slab.physical_bytes);
        match slab.state {
            BlockStoreSlabState::Active => {
                update_oldest_slab_timestamp(&mut summary.oldest_live_slab_unix_ms, slab);
                summary.active_slabs = summary.active_slabs.saturating_add(1);
                summary.active_physical_bytes = summary
                    .active_physical_bytes
                    .saturating_add(slab.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(slab.physical_bytes);
            }
            BlockStoreSlabState::Sealed => {
                update_oldest_slab_timestamp(&mut summary.oldest_live_slab_unix_ms, slab);
                summary.sealed_slabs = summary.sealed_slabs.saturating_add(1);
                summary.sealed_physical_bytes = summary
                    .sealed_physical_bytes
                    .saturating_add(slab.physical_bytes);
                summary.live_physical_bytes = summary
                    .live_physical_bytes
                    .saturating_add(slab.physical_bytes);
            }
            BlockStoreSlabState::DelayedDestroy => {
                update_oldest_slab_timestamp(
                    &mut summary.oldest_reclaimable_slab_unix_ms,
                    slab,
                );
                summary.delayed_destroy_slabs = summary.delayed_destroy_slabs.saturating_add(1);
                summary.delayed_destroy_physical_bytes = summary
                    .delayed_destroy_physical_bytes
                    .saturating_add(slab.physical_bytes);
                summary.reclaimable_physical_bytes = summary
                    .reclaimable_physical_bytes
                    .saturating_add(slab.physical_bytes);
            }
            BlockStoreSlabState::Purged => {
                summary.purged_slabs = summary.purged_slabs.saturating_add(1);
                summary.purged_physical_bytes = summary
                    .purged_physical_bytes
                    .saturating_add(slab.physical_bytes);
            }
        }
    }
    summary.oldest_known_slab_age_ms = summary
        .oldest_known_slab_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_live_slab_age_ms = summary
        .oldest_live_slab_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary.oldest_reclaimable_slab_age_ms = summary
        .oldest_reclaimable_slab_unix_ms
        .map(|timestamp| now.saturating_sub(timestamp));
    summary
}

pub(super) fn update_oldest_slab_timestamp(
    target: &mut Option<u64>,
    slab: &BlockStoreSlabDescriptor,
) {
    let Some(timestamp) = slab.updated_unix_ms.or(slab.created_unix_ms) else {
        return;
    };
    if target.map(|current| timestamp < current).unwrap_or(true) {
        *target = Some(timestamp);
    }
}

pub(super) fn ensure_slab_descriptor(
    slabs: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
    root: &Path,
    block_slab_id: u64,
    state: BlockStoreSlabState,
) {
    slabs.entry(block_slab_id).or_insert_with(|| {
        let physical_bytes = slab_path(root, block_slab_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        BlockStoreSlabDescriptor {
            stored_slab_id: block_slab_id,
            block_slab_id,
            state,
            physical_bytes,
            logical_bytes: physical_bytes,
            created_unix_ms: file_created_unix_ms(&slab_path(root, block_slab_id))
                .or_else(|| file_modified_unix_ms(&slab_path(root, block_slab_id))),
            updated_unix_ms: file_modified_unix_ms(&slab_path(root, block_slab_id)),
            first_block_id: None,
            last_block_id: None,
            readable_prefix_physical_bytes: physical_bytes,
            verified_source_mtime_unix_ms: None,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        }
    });
    let transition_unix_ms = now_unix_ms();
    for slab in slabs.values_mut() {
        if slab.block_slab_id == block_slab_id {
            slab.state = state;
            slab.updated_unix_ms = Some(transition_unix_ms);
        } else if slab.state == BlockStoreSlabState::Active {
            slab.state = BlockStoreSlabState::Sealed;
            slab.updated_unix_ms = Some(transition_unix_ms);
        }
    }
}

pub(super) fn upsert_slab_after_append(
    slabs: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
    block_slab_id: u64,
    physical_bytes: u64,
    logical_bytes_written: u64,
    page_id: u64,
) {
    let slab = slabs
        .entry(block_slab_id)
        .or_insert(BlockStoreSlabDescriptor {
            stored_slab_id: block_slab_id,
            block_slab_id,
            state: BlockStoreSlabState::Active,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_block_id: Some(page_id),
            last_block_id: Some(page_id),
            readable_prefix_physical_bytes: 0,
            verified_source_mtime_unix_ms: None,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        });
    let updated_unix_ms = now_unix_ms();
    slab.state = BlockStoreSlabState::Active;
    slab.physical_bytes = physical_bytes;
    slab.readable_prefix_physical_bytes = physical_bytes;
    slab.has_corruption = false;
    slab.first_error_offset = None;
    slab.first_error = None;
    slab.logical_bytes = slab.logical_bytes.saturating_add(logical_bytes_written);
    if slab.created_unix_ms.is_none() {
        slab.created_unix_ms = Some(updated_unix_ms);
    }
    slab.updated_unix_ms = Some(updated_unix_ms);
    slab.first_block_id = Some(
        slab
            .first_block_id
            .map_or(page_id, |first| first.min(page_id)),
    );
    slab.last_block_id = Some(
        slab
            .last_block_id
            .map_or(page_id, |last| last.max(page_id)),
    );
}

pub(super) fn set_slab_state(
    slabs: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
    block_slab_id: u64,
    state: BlockStoreSlabState,
) {
    slabs
        .entry(block_slab_id)
        .and_modify(|slab| {
            slab.state = state;
            slab.updated_unix_ms = Some(now_unix_ms());
        })
        .or_insert(BlockStoreSlabDescriptor {
            stored_slab_id: block_slab_id,
            block_slab_id,
            state,
            physical_bytes: 0,
            logical_bytes: 0,
            created_unix_ms: Some(now_unix_ms()),
            updated_unix_ms: Some(now_unix_ms()),
            first_block_id: None,
            last_block_id: None,
            readable_prefix_physical_bytes: 0,
            verified_source_mtime_unix_ms: None,
            has_corruption: false,
            first_error_offset: None,
            first_error: None,
        });
}

