// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! LocalBlockStore band descriptor/summary + stream-backed band runtime report, extracted from block_store.rs.

use super::*;

/// Map a band descriptor's lifecycle state to the index-log `ZoneState` (1:1). Kept a free fn
/// so both directions of the MANIFEST-PARITY FOLD conversion share one mapping.
fn band_state_to_zone_state(state: BlockStoreBandState) -> crate::index_log::ZoneState {
    match state {
        BlockStoreBandState::Active => crate::index_log::ZoneState::Active,
        BlockStoreBandState::Sealed => crate::index_log::ZoneState::Sealed,
        BlockStoreBandState::DelayedDestroy => crate::index_log::ZoneState::DelayedDestroy,
        BlockStoreBandState::Purged => crate::index_log::ZoneState::Purged,
    }
}

fn zone_state_to_band_state(state: crate::index_log::ZoneState) -> BlockStoreBandState {
    match state {
        crate::index_log::ZoneState::Active => BlockStoreBandState::Active,
        crate::index_log::ZoneState::Sealed => BlockStoreBandState::Sealed,
        crate::index_log::ZoneState::DelayedDestroy => BlockStoreBandState::DelayedDestroy,
        crate::index_log::ZoneState::Purged => BlockStoreBandState::Purged,
    }
}

impl LocalBlockStore {
    pub fn band_descriptors(&self) -> Vec<BlockStoreBandDescriptor> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .bands
            .values()
            .cloned()
            .collect()
    }

    /// MANIFEST-PARITY FOLD: project the in-memory band catalog into the DURABLE `ZoneInfo`
    /// subset the reference keeps in `IndexLog.MetaItem.zones`. Only the durable fields ride in
    /// the fold; the band descriptor's diagnostic fields (readable_prefix / corruption / errors)
    /// are deliberately dropped -- they are recomputed on load by scanning the slab, exactly as
    /// the reference does not persist them. `zone_version` stamps every entry so a folded anchor
    /// carries a monotonically-versioned snapshot.
    pub fn zone_catalog(&self, zone_version: u64) -> Vec<crate::index_log::ZoneInfo> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .bands
            .values()
            .map(|band| crate::index_log::ZoneInfo {
                page_slab_id: band.page_slab_id,
                state: band_state_to_zone_state(band.state),
                physical_bytes: band.physical_bytes,
                logical_bytes: band.logical_bytes,
                created_unix_ms: band.created_unix_ms,
                updated_unix_ms: band.updated_unix_ms,
                first_page_id: band.first_page_id,
                last_page_id: band.last_page_id,
                version: zone_version,
            })
            .collect()
    }

    /// MANIFEST-PARITY FOLD recovery: seed the band catalog from a folded `ZoneInfo` snapshot
    /// recovered from the index-log MetaItem. Applied on load AFTER the block store has already
    /// reconciled from durable pages (reconcile stays authoritative for on-disk physical bytes
    /// and diagnostics), so this only RESTORES the catalog fields a pure disk scan cannot infer:
    /// the exact lifecycle state, the creation/update timestamps, the logical byte count, and the
    /// first/last page-id range. It never deletes a band reconcile found on disk and never
    /// downgrades physical bytes below what the slab actually holds -- so it cannot lose durable
    /// state; it is a metadata refinement layered on the lossless disk-derived catalog. Persists
    /// the merged manifest once. Returns whether anything changed.
    pub fn install_zone_catalog(
        &self,
        zones: &[crate::index_log::ZoneInfo],
    ) -> Result<bool, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let active = inner.page_slab_id;
        let mut changed = false;
        for zone in zones {
            let state = zone_state_to_band_state(zone.state);
            match inner.bands.get_mut(&zone.page_slab_id) {
                Some(band) => {
                    let before = band.clone();
                    // Never override the live ACTIVE slab's disk-derived state (it holds the open
                    // write frontier); for every other slab adopt the folded lifecycle state.
                    if zone.page_slab_id != active {
                        band.state = state;
                    }
                    band.created_unix_ms = band.created_unix_ms.or(zone.created_unix_ms);
                    if band.updated_unix_ms.is_none() {
                        band.updated_unix_ms = zone.updated_unix_ms;
                    }
                    if band.logical_bytes == 0 {
                        band.logical_bytes = zone.logical_bytes;
                    }
                    band.first_page_id = band.first_page_id.or(zone.first_page_id);
                    band.last_page_id = band.last_page_id.or(zone.last_page_id);
                    changed |= *band != before;
                }
                None => {
                    // A band the disk scan did not surface (e.g. a purged/reclaimed slab with no
                    // live file): install it from the fold so accounting/GC see the full history.
                    inner.bands.insert(
                        zone.page_slab_id,
                        BlockStoreBandDescriptor {
                            band_id: band_id_for_slab(zone.page_slab_id),
                            page_slab_id: zone.page_slab_id,
                            state,
                            physical_bytes: zone.physical_bytes,
                            logical_bytes: zone.logical_bytes,
                            created_unix_ms: zone.created_unix_ms,
                            updated_unix_ms: zone.updated_unix_ms,
                            first_page_id: zone.first_page_id,
                            last_page_id: zone.last_page_id,
                            readable_prefix_physical_bytes: zone.physical_bytes,
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
            persist_band_manifest(&root, &inner.bands)?;
        }
        Ok(changed)
    }

    pub fn band_summary(&self) -> BlockStoreBandSummary {
        summarize_bands(
            &self
                .inner
                .lock()
                .expect("block store lock poisoned")
                .bands,
        )
    }

    pub fn stream_backed_band_runtime_report(
        &self,
    ) -> Result<StreamBackedBandRuntimeReport, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let bands = inner.bands.clone();
        let root = inner.root.clone();
        let options = inner.options;
        let stats = inner.stats;
        let band_manifest_reconciled_on_open = inner.band_manifest_reconciled_on_open;
        drop(inner);

        let summary = summarize_bands(&bands);
        let zone_usage = band_zone_usage(&bands);
        let zone_stats_ready = zone_usage.iter().all(|zone| {
            zone.band_id == band_id_for_slab(zone.page_slab_id)
                && zone.page_store_used_bytes
                    == zone
                        .live_page_store_used_bytes
                        .saturating_add(zone.reclaimable_page_store_used_bytes)
                        .saturating_add(zone.purged_page_store_used_bytes)
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
            .filter(|report| report.page_count > 0 || report.physical_bytes > 0)
            .count() as u64;
        let live_slab_ids = slab_reports
            .iter()
            .map(|report| report.page_slab_id)
            .collect::<BTreeSet<_>>();
        let delayed_slab_ids = delayed_destroy_slab_reports_at(&root)?
            .into_iter()
            .map(|report| report.page_slab_id)
            .collect::<BTreeSet<_>>();
        let manifest_missing_stream_bands = bands
            .values()
            .filter(|band| {
                !matches!(band.state, BlockStoreBandState::Purged)
                    && !live_slab_ids.contains(&band.page_slab_id)
                    && !delayed_slab_ids.contains(&band.page_slab_id)
            })
            .count() as u64;
        let manifest_extra_stream_bands = live_slab_ids
            .iter()
            .filter(|page_slab_id| !bands.contains_key(page_slab_id))
            .count() as u64;
        let band_manifest_disk_consistent =
            manifest_missing_stream_bands == 0 && manifest_extra_stream_bands == 0;
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
            .map(|report| report.page_count)
            .sum::<u64>();
        let corrupt_band_count = slab_reports
            .iter()
            .filter(|report| report.has_corruption)
            .count() as u64;
        let partial_band_count = slab_reports
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
        let first_page_id = slab_reports
            .iter()
            .filter_map(|report| report.first_page_id)
            .min();
        let last_page_id = slab_reports
            .iter()
            .filter_map(|report| report.last_page_id)
            .max();
        let page_id_continuity_ready = match (first_page_id, last_page_id) {
            (Some(first), Some(last)) => {
                stream_record_count > 0
                    && last >= first
                    && last.saturating_sub(first).saturating_add(1) == stream_record_count
            }
            _ => stream_record_count == 0,
        };
        let logical_stream_read_ready = slab_reports.iter().any(|report| report.page_count > 0);
        let append_roll_ready = summary.active_bands == 1
            && summary
                .sealed_bands
                .saturating_add(summary.delayed_destroy_bands)
                .saturating_add(summary.purged_bands)
                > 0;
        let band_manifest_ready = band_manifest_path(&root).exists()
            && !bands.is_empty()
            && bands
                .values()
                .all(|band| band.band_id == band_id_for_slab(band.page_slab_id));
        let band_manifest_rebuild_ready = band_manifest_ready
            && slab_reports.iter().all(|report| {
                bands
                    .get(&report.page_slab_id)
                    .map(|band| {
                        band.first_page_id == report.first_page_id
                            && band.last_page_id == report.last_page_id
                            && band.logical_bytes == report.logical_bytes
                            && band.readable_prefix_physical_bytes
                                == report.readable_prefix_physical_bytes
                            && band.has_corruption == report.has_corruption
                    })
                    .unwrap_or(false)
            });
        let partial_band_recovery_ready = corrupt_band_count == 0
            || slab_reports
                .iter()
                .filter(|report| report.has_corruption)
                .all(|report| {
                    bands
                        .get(&report.page_slab_id)
                        .map(|band| {
                            band.has_corruption
                                && band.first_error_offset == report.first_error_offset
                                && band.readable_prefix_physical_bytes
                                    == report.readable_prefix_physical_bytes
                                && band.first_page_id == report.first_page_id
                                && band.last_page_id == report.last_page_id
                        })
                        .unwrap_or(false)
                });
        let envelope_checksum_ready = slab_reports
            .iter()
            .filter(|report| report.page_count > 0)
            .all(|report| !report.has_corruption && report.logical_bytes > 0);
        let compression_stream_ready = options.compression_enabled
            && slab_reports
                .iter()
                .any(|report| report.compressed_records > 0);
        let delayed_destroy_ready =
            summary.delayed_destroy_bands > 0 || summary.purged_bands > 0;
        let purge_lifecycle_ready = summary.purged_bands > 0;
        let band_lifecycle_states = band_lifecycle_states(&summary);
        let band_state_transition_count = [
            summary.active_bands,
            summary.sealed_bands,
            summary.delayed_destroy_bands,
            summary.purged_bands,
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
        if !band_manifest_ready {
            blockers.push("band manifest is missing or inconsistent".to_string());
        }
        if !band_manifest_rebuild_ready {
            blockers.push("band manifest does not match stream page-id boundaries".to_string());
        }
        if !band_manifest_disk_consistent {
            blockers.push(
                "band manifest still diverges from live/delayed-destroy stream files".to_string(),
            );
        }
        if !zone_stats_ready {
            blockers.push("page-store zone usage accounting is inconsistent".to_string());
        }
        if !envelope_checksum_ready {
            blockers.push("stream record envelope/checksum inspection is not clean".to_string());
        }
        if corrupt_band_count > 0 && partial_band_recovery_ready {
            blockers.push(
                "corrupt stream band detected; readable prefix was preserved in rebuilt manifest"
                    .to_string(),
            );
        }
        if !page_id_continuity_ready {
            blockers.push("stream page ids are not contiguous across bands".to_string());
        }

        let runtime_ready = blockers.is_empty();
        Ok(StreamBackedBandRuntimeReport {
            runtime_ready,
            band_lifecycle_states,
            band_count: bands.len() as u64,
            active_bands: summary.active_bands,
            sealed_bands: summary.sealed_bands,
            delayed_destroy_bands: summary.delayed_destroy_bands,
            purged_bands: summary.purged_bands,
            zone_stats_ready,
            zone_usage,
            stream_slab_count,
            physical_bytes,
            logical_bytes,
            stream_record_count,
            first_page_id,
            last_page_id,
            page_id_continuity_ready,
            logical_stream_bytes_read: stats.logical_bytes_read,
            band_state_transition_count,
            logical_stream_read_ready,
            append_roll_ready,
            band_manifest_ready,
            band_manifest_rebuild_ready,
            band_manifest_reconciled_on_open,
            band_manifest_disk_consistent,
            manifest_missing_stream_bands,
            manifest_extra_stream_bands,
            corrupt_band_count,
            partial_band_count,
            readable_prefix_physical_bytes,
            partial_band_recovery_ready,
            envelope_checksum_ready,
            compression_stream_ready,
            delayed_destroy_ready,
            purge_lifecycle_ready,
            blockers,
            evidence: vec![
                "block records are appended as self-describing stream envelopes".to_string(),
                "logical stream reads span records while skipping envelopes and decompression"
                    .to_string(),
                "segment roll seals the previous band and opens a new active band".to_string(),
                "band manifest persists active/sealed/delayed-destroy/purged lifecycle state"
                    .to_string(),
                "stream runtime reports page-id continuity and logical read byte evidence"
                    .to_string(),
                "band manifest descriptors are validated against inspected stream boundaries"
                    .to_string(),
                "open-time reconciliation repairs manifest/live stream divergence like zone updates"
                    .to_string(),
                "zone usage reports map band ids to page-store used bytes like ZoneStats"
                    .to_string(),
            ],
        })
    }
}
