// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BlockStore slab descriptor/summary + stream-backed slab runtime report, extracted from block_store.rs.

use super::*;

/// Map a slab descriptor's lifecycle state to the index-log `SlabCatalogState` (1:1). Kept a free fn
/// so both directions of the MANIFEST-CONFORMANCE FOLD conversion share one mapping.
fn slab_state_to_catalog_state(state: BlockStoreSlabState) -> crate::index_log::SlabCatalogState {
    match state {
        BlockStoreSlabState::Active => crate::index_log::SlabCatalogState::Active,
        BlockStoreSlabState::Sealed => crate::index_log::SlabCatalogState::Sealed,
        BlockStoreSlabState::DelayedDestroy => crate::index_log::SlabCatalogState::DelayedDestroy,
        BlockStoreSlabState::Purged => crate::index_log::SlabCatalogState::Purged,
    }
}

fn catalog_state_to_slab_state(state: crate::index_log::SlabCatalogState) -> BlockStoreSlabState {
    match state {
        crate::index_log::SlabCatalogState::Active => BlockStoreSlabState::Active,
        crate::index_log::SlabCatalogState::Sealed => BlockStoreSlabState::Sealed,
        crate::index_log::SlabCatalogState::DelayedDestroy => BlockStoreSlabState::DelayedDestroy,
        crate::index_log::SlabCatalogState::Purged => BlockStoreSlabState::Purged,
    }
}

impl BlockStore {
    pub fn slab_descriptors(&self) -> Vec<BlockStoreSlabDescriptor> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .slabs
            .values()
            .cloned()
            .collect()
    }

    /// How many sealed slabs this open kept from the manifest WITHOUT re-reading them.
    ///
    /// The skip is the default (`TS_REVERIFY_ALL_SLABS` unset) and it is where a descriptor is
    /// carried across an open untouched -- so it is the route worth guarding, and the one easiest
    /// to guard vacuously. Arming it takes THREE opens: the open that inspects a slab stamps
    /// `verified_source_mtime_unix_ms` on its descriptor and only then writes the manifest out,
    /// so the second open is the first that can read a stamped descriptor back and the third is
    /// the first where the stamp is already on disk when a test edits the manifest around it.
    /// A guard that asserts a property of the skip route without first asserting this count is
    /// non-zero is asserting it of a route that never ran.
    pub fn slabs_skipped_reinspection_on_open(&self) -> usize {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .slabs_skipped_reinspection_on_open
    }

    /// MANIFEST-CONFORMANCE FOLD: project the in-memory slab catalog into the DURABLE `SlabCatalogEntry`
    /// subset kept in the index-log slab catalog. Only the durable fields ride in
    /// the fold; the slab descriptor's diagnostic fields (readable_prefix / corruption / errors)
    /// are deliberately dropped -- they are recomputed on load by scanning the slab, exactly as
    /// this design does not persist them. `slab_version` stamps every entry so a folded anchor
    /// carries a monotonically-versioned snapshot.
    pub fn slab_catalog(&self, slab_version: u64) -> Vec<crate::index_log::SlabCatalogEntry> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .slabs
            .values()
            .map(|slab| crate::index_log::SlabCatalogEntry {
                block_slab_id: slab.block_slab_id,
                state: slab_state_to_catalog_state(slab.state),
                physical_bytes: slab.physical_bytes,
                logical_bytes: slab.logical_bytes,
                created_unix_ms: slab.created_unix_ms,
                updated_unix_ms: slab.updated_unix_ms,
                first_block_id: slab.first_block_id,
                last_block_id: slab.last_block_id,
                version: slab_version,
            })
            .collect()
    }

    /// MANIFEST-CONFORMANCE FOLD recovery: seed the slab catalog from a folded `SlabCatalogEntry` snapshot
    /// recovered from the index-log MetaItem. Applied on load AFTER the block store has already
    /// reconciled from durable pages (reconcile stays authoritative for on-disk physical bytes
    /// and diagnostics), so this only RESTORES the catalog fields a pure disk scan cannot infer:
    /// the exact lifecycle state, the creation/update timestamps, the logical byte count, and the
    /// first/last page-id range. It never deletes a slab reconcile found on disk and never
    /// downgrades physical bytes below what the slab actually holds -- so it cannot lose durable
    /// state; it is a metadata refinement layered on the lossless disk-derived catalog. Persists
    /// the merged manifest once. Returns whether anything changed.
    pub fn install_slab_catalog(
        &self,
        catalog: &[crate::index_log::SlabCatalogEntry],
    ) -> Result<bool, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let active = inner.block_slab_id;
        let mut changed = false;
        for entry in catalog {
            let state = catalog_state_to_slab_state(entry.state);
            match inner.slabs.get_mut(&entry.block_slab_id) {
                Some(slab) => {
                    let before = slab.clone();
                    // Never override the live ACTIVE slab's disk-derived state (it holds the open
                    // write frontier); for every other slab adopt the folded lifecycle state.
                    if entry.block_slab_id != active {
                        slab.state = state;
                    }
                    slab.created_unix_ms = slab.created_unix_ms.or(entry.created_unix_ms);
                    if slab.updated_unix_ms.is_none() {
                        slab.updated_unix_ms = entry.updated_unix_ms;
                    }
                    if slab.logical_bytes == 0 {
                        slab.logical_bytes = entry.logical_bytes;
                    }
                    slab.first_block_id = slab.first_block_id.or(entry.first_block_id);
                    slab.last_block_id = slab.last_block_id.or(entry.last_block_id);
                    changed |= *slab != before;
                }
                None => {
                    // A slab the disk scan did not surface (e.g. a purged/reclaimed slab with no
                    // live file): install it from the fold so accounting/GC see the full history.
                    inner.slabs.insert(
                        entry.block_slab_id,
                        BlockStoreSlabDescriptor {
                            stored_slab_id: entry.block_slab_id,
                            block_slab_id: entry.block_slab_id,
                            state,
                            physical_bytes: entry.physical_bytes,
                            logical_bytes: entry.logical_bytes,
                            created_unix_ms: entry.created_unix_ms,
                            updated_unix_ms: entry.updated_unix_ms,
                            first_block_id: entry.first_block_id,
                            last_block_id: entry.last_block_id,
                            readable_prefix_physical_bytes: entry.physical_bytes,
                            verified_source_mtime_unix_ms: None,
                            has_corruption: false,
                            first_error_offset: None,
                            first_error: None,
                        },
                    );
                    changed = true;
                }
            }
        }
        if changed {
            let root = inner.root.clone();
            persist_slab_manifest(&root, &inner.slabs)?;
        }
        Ok(changed)
    }

    pub fn slab_summary(&self) -> BlockStoreSlabSummary {
        summarize_slabs(
            &self
                .inner
                .lock()
                .expect("block store lock poisoned")
                .slabs,
        )
    }

    pub fn stream_backed_slab_runtime_report(
        &self,
    ) -> Result<StreamBackedSlabRuntimeReport, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let slabs = inner.slabs.clone();
        let root = inner.root.clone();
        let options = inner.options;
        let stats = inner.stats;
        let slab_manifest_reconciled_on_open = inner.slab_manifest_reconciled_on_open;
        drop(inner);

        let summary = summarize_slabs(&slabs);
        let slab_usage = compute_slab_usage(&slabs);
        let slab_stats_ready = slab_usage.iter().all(|slab| {
            slab.stored_slab_id == slab.block_slab_id
                && slab.block_store_used_bytes
                    == slab
                        .live_block_store_used_bytes
                        .saturating_add(slab.reclaimable_block_store_used_bytes)
                        .saturating_add(slab.purged_block_store_used_bytes)
        });
        let slab_reports = {
            let mut reports = Vec::new();
            for id in slab_ids_at(&root)? {
                reports.push(inspect_slab(&fs::read(slab_path(&root, id))?, id));
            }
            reports
        };
        let stream_slab_count = slab_reports
            .iter()
            .filter(|report| report.block_count > 0 || report.physical_bytes > 0)
            .count() as u64;
        let live_slab_ids = slab_reports
            .iter()
            .map(|report| report.block_slab_id)
            .collect::<BTreeSet<_>>();
        let delayed_slab_ids = delayed_destroy_slab_reports_at(&root)?
            .into_iter()
            .map(|report| report.block_slab_id)
            .collect::<BTreeSet<_>>();
        let manifest_missing_stream_slabs = slabs
            .values()
            .filter(|slab| {
                !matches!(slab.state, BlockStoreSlabState::Purged)
                    && !live_slab_ids.contains(&slab.block_slab_id)
                    && !delayed_slab_ids.contains(&slab.block_slab_id)
            })
            .count() as u64;
        let manifest_extra_stream_slabs = live_slab_ids
            .iter()
            .filter(|block_slab_id| !slabs.contains_key(block_slab_id))
            .count() as u64;
        let slab_manifest_disk_consistent =
            manifest_missing_stream_slabs == 0 && manifest_extra_stream_slabs == 0;
        let physical_bytes = slab_reports
            .iter()
            .map(|report| report.physical_bytes)
            .sum::<u64>();
        let logical_bytes = slab_reports
            .iter()
            .map(|report| report.logical_bytes)
            .sum::<u64>();
        let stream_record_count = slab_reports
            .iter()
            .map(|report| report.block_count)
            .sum::<u64>();
        let corrupt_slab_count = slab_reports
            .iter()
            .filter(|report| report.has_corruption)
            .count() as u64;
        let partial_slab_count = slab_reports
            .iter()
            .filter(|report| {
                report.has_corruption
                    && report.readable_prefix_physical_bytes > 0
                    && report.readable_prefix_physical_bytes < report.physical_bytes
            })
            .count() as u64;
        let readable_prefix_physical_bytes = slab_reports
            .iter()
            .map(|report| report.readable_prefix_physical_bytes)
            .sum::<u64>();
        let first_block_id = slab_reports
            .iter()
            .filter_map(|report| report.first_block_id)
            .min();
        let last_block_id = slab_reports
            .iter()
            .filter_map(|report| report.last_block_id)
            .max();
        // Block ids are indexes INSIDE an object now, not a run of numbers handed out across
        // the store, so "first to last covers exactly this many records" is no longer a
        // property the store has: two objects in one slab both start at block 0. What still
        // holds is that a slab holding records reports the range it holds.
        let block_id_continuity_ready = match (first_block_id, last_block_id) {
            (Some(first), Some(last)) => stream_record_count > 0 && last >= first,
            _ => stream_record_count == 0,
        };
        let logical_stream_read_ready = slab_reports.iter().any(|report| report.block_count > 0);
        let append_roll_ready = summary.active_slabs == 1
            && summary
                .sealed_slabs
                .saturating_add(summary.delayed_destroy_slabs)
                .saturating_add(summary.purged_slabs)
                > 0;
        let slab_manifest_ready = slab_manifest_path(&root).exists()
            && !slabs.is_empty()
            && slabs
                .values()
                .all(|slab| slab.stored_slab_id == slab.block_slab_id);
        let slab_manifest_rebuild_ready = slab_manifest_ready
            && slab_reports.iter().all(|report| {
                slabs
                    .get(&report.block_slab_id)
                    .map(|slab| {
                        slab.first_block_id == report.first_block_id
                            && slab.last_block_id == report.last_block_id
                            && slab.logical_bytes == report.logical_bytes
                            && slab.readable_prefix_physical_bytes
                                == report.readable_prefix_physical_bytes
                            && slab.has_corruption == report.has_corruption
                    })
                    .unwrap_or(false)
            });
        let partial_slab_recovery_ready = corrupt_slab_count == 0
            || slab_reports
                .iter()
                .filter(|report| report.has_corruption)
                .all(|report| {
                    slabs
                        .get(&report.block_slab_id)
                        .map(|slab| {
                            slab.has_corruption
                                && slab.first_error_offset == report.first_error_offset
                                && slab.readable_prefix_physical_bytes
                                    == report.readable_prefix_physical_bytes
                                && slab.first_block_id == report.first_block_id
                                && slab.last_block_id == report.last_block_id
                        })
                        .unwrap_or(false)
                });
        let envelope_checksum_ready = slab_reports
            .iter()
            .filter(|report| report.block_count > 0)
            .all(|report| !report.has_corruption && report.logical_bytes > 0);
        let compression_stream_ready = options.compression_enabled
            && slab_reports
                .iter()
                .any(|report| report.compressed_records > 0);
        let delayed_destroy_ready =
            summary.delayed_destroy_slabs > 0 || summary.purged_slabs > 0;
        let purge_lifecycle_ready = summary.purged_slabs > 0;
        let slab_lifecycle_states = slab_lifecycle_states(&summary);
        let slab_state_transition_count = [
            summary.active_slabs,
            summary.sealed_slabs,
            summary.delayed_destroy_slabs,
            summary.purged_slabs,
        ]
        .into_iter()
        .filter(|count| *count > 0)
        .count() as u64;

        let mut blockers = Vec::new();
        if !logical_stream_read_ready {
            blockers.push("no readable block stream records found".to_string());
        }
        if !append_roll_ready {
            blockers.push(
                "append/roll band lifecycle has not produced active plus sealed bands"
                    .to_string(),
            );
        }
        if !slab_manifest_ready {
            blockers.push("band manifest is missing or inconsistent".to_string());
        }
        if !slab_manifest_rebuild_ready {
            blockers.push("band manifest does not match what the slabs hold".to_string());
        }
        if !slab_manifest_disk_consistent {
            blockers.push(
                "band manifest still diverges from live/delayed-destroy stream files".to_string(),
            );
        }
        if !slab_stats_ready {
            blockers.push("page-store band usage accounting is inconsistent".to_string());
        }
        if !envelope_checksum_ready {
            blockers.push("stream record envelope/checksum inspection is not clean".to_string());
        }
        if corrupt_slab_count > 0 && partial_slab_recovery_ready {
            blockers.push(
                "corrupt stream band detected; readable prefix was preserved in rebuilt manifest"
                    .to_string(),
            );
        }
        if !block_id_continuity_ready {
            blockers.push("stream page ids are not contiguous across bands".to_string());
        }

        let runtime_ready = blockers.is_empty();
        Ok(StreamBackedSlabRuntimeReport {
            runtime_ready,
            slab_lifecycle_states,
            slab_count: slabs.len() as u64,
            active_slabs: summary.active_slabs,
            sealed_slabs: summary.sealed_slabs,
            delayed_destroy_slabs: summary.delayed_destroy_slabs,
            purged_slabs: summary.purged_slabs,
            slab_stats_ready,
            slab_usage,
            stream_slab_count,
            physical_bytes,
            logical_bytes,
            stream_record_count,
            first_block_id,
            last_block_id,
            block_id_continuity_ready,
            logical_stream_bytes_read: stats.logical_bytes_read,
            slab_state_transition_count,
            logical_stream_read_ready,
            append_roll_ready,
            slab_manifest_ready,
            slab_manifest_rebuild_ready,
            slab_manifest_reconciled_on_open,
            slab_manifest_disk_consistent,
            manifest_missing_stream_slabs,
            manifest_extra_stream_slabs,
            corrupt_slab_count,
            partial_slab_count,
            readable_prefix_physical_bytes,
            partial_slab_recovery_ready,
            envelope_checksum_ready,
            compression_stream_ready,
            delayed_destroy_ready,
            purge_lifecycle_ready,
            blockers,
            evidence: vec![
                "block records are appended as self-describing stream envelopes".to_string(),
                "logical stream reads span records while skipping envelopes and decompression"
                    .to_string(),
                "slab roll seals the previous band and opens a new active band".to_string(),
                "band manifest persists active/sealed/delayed-destroy/purged lifecycle state"
                    .to_string(),
                "stream runtime reports page-id continuity and logical read byte evidence"
                    .to_string(),
                "band manifest descriptors are validated against inspected stream boundaries"
                    .to_string(),
                "open-time reconciliation repairs manifest/live stream divergence like band updates"
                    .to_string(),
                "band usage reports map band ids to page-store used bytes like SlabStats"
                    .to_string(),
            ],
        })
    }
}
