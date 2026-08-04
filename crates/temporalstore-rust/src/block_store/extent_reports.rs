//! LocalBlockStore extent descriptor/summary + stream-backed extent runtime report, extracted from block_store.rs.

use super::*;

impl LocalBlockStore {
    pub fn extent_descriptors(&self) -> Vec<BlockStoreExtentDescriptor> {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .extents
            .values()
            .cloned()
            .collect()
    }

    pub fn extent_summary(&self) -> BlockStoreExtentSummary {
        summarize_extents(
            &self
                .inner
                .lock()
                .expect("block store lock poisoned")
                .extents,
        )
    }

    pub fn stream_backed_extent_runtime_report(
        &self,
    ) -> Result<StreamBackedExtentRuntimeReport, BlockStoreError> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let extents = inner.extents.clone();
        let root = inner.root.clone();
        let options = inner.options;
        let stats = inner.stats;
        let extent_manifest_reconciled_on_open = inner.extent_manifest_reconciled_on_open;
        drop(inner);

        let summary = summarize_extents(&extents);
        let zone_usage = extent_zone_usage(&extents);
        let zone_stats_ready = zone_usage.iter().all(|zone| {
            zone.extent_id == extent_id_for_segment(zone.page_segment_id)
                && zone.page_store_used_bytes
                    == zone
                        .live_page_store_used_bytes
                        .saturating_add(zone.reclaimable_page_store_used_bytes)
                        .saturating_add(zone.purged_page_store_used_bytes)
        });
        let segment_reports = {
            let mut reports = Vec::new();
            for id in segment_ids_at(&root)? {
                reports.push(inspect_segment(&fs::read(segment_path(&root, id))?, id));
            }
            reports
        };
        let stream_segment_count = segment_reports
            .iter()
            .filter(|report| report.page_count > 0 || report.physical_bytes > 0)
            .count() as u64;
        let live_segment_ids = segment_reports
            .iter()
            .map(|report| report.page_segment_id)
            .collect::<BTreeSet<_>>();
        let delayed_segment_ids = delayed_destroy_segment_reports_at(&root)?
            .into_iter()
            .map(|report| report.page_segment_id)
            .collect::<BTreeSet<_>>();
        let manifest_missing_stream_extents = extents
            .values()
            .filter(|extent| {
                !matches!(extent.state, BlockStoreExtentState::Purged)
                    && !live_segment_ids.contains(&extent.page_segment_id)
                    && !delayed_segment_ids.contains(&extent.page_segment_id)
            })
            .count() as u64;
        let manifest_extra_stream_extents = live_segment_ids
            .iter()
            .filter(|page_segment_id| !extents.contains_key(page_segment_id))
            .count() as u64;
        let extent_manifest_disk_consistent =
            manifest_missing_stream_extents == 0 && manifest_extra_stream_extents == 0;
        let physical_bytes = segment_reports
            .iter()
            .map(|report| report.physical_bytes)
            .sum::<u64>();
        let logical_bytes = segment_reports
            .iter()
            .map(|report| report.logical_bytes)
            .sum::<u64>();
        let stream_record_count = segment_reports
            .iter()
            .map(|report| report.page_count)
            .sum::<u64>();
        let corrupt_extent_count = segment_reports
            .iter()
            .filter(|report| report.has_corruption)
            .count() as u64;
        let partial_extent_count = segment_reports
            .iter()
            .filter(|report| {
                report.has_corruption
                    && report.readable_prefix_physical_bytes > 0
                    && report.readable_prefix_physical_bytes < report.physical_bytes
            })
            .count() as u64;
        let readable_prefix_physical_bytes = segment_reports
            .iter()
            .map(|report| report.readable_prefix_physical_bytes)
            .sum::<u64>();
        let first_page_id = segment_reports
            .iter()
            .filter_map(|report| report.first_page_id)
            .min();
        let last_page_id = segment_reports
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
        let logical_stream_read_ready = segment_reports.iter().any(|report| report.page_count > 0);
        let append_roll_ready = summary.active_extents == 1
            && summary
                .sealed_extents
                .saturating_add(summary.delayed_destroy_extents)
                .saturating_add(summary.purged_extents)
                > 0;
        let extent_manifest_ready = extent_manifest_path(&root).exists()
            && !extents.is_empty()
            && extents
                .values()
                .all(|extent| extent.extent_id == extent_id_for_segment(extent.page_segment_id));
        let extent_manifest_rebuild_ready = extent_manifest_ready
            && segment_reports.iter().all(|report| {
                extents
                    .get(&report.page_segment_id)
                    .map(|extent| {
                        extent.first_page_id == report.first_page_id
                            && extent.last_page_id == report.last_page_id
                            && extent.logical_bytes == report.logical_bytes
                            && extent.readable_prefix_physical_bytes
                                == report.readable_prefix_physical_bytes
                            && extent.has_corruption == report.has_corruption
                    })
                    .unwrap_or(false)
            });
        let partial_extent_recovery_ready = corrupt_extent_count == 0
            || segment_reports
                .iter()
                .filter(|report| report.has_corruption)
                .all(|report| {
                    extents
                        .get(&report.page_segment_id)
                        .map(|extent| {
                            extent.has_corruption
                                && extent.first_error_offset == report.first_error_offset
                                && extent.readable_prefix_physical_bytes
                                    == report.readable_prefix_physical_bytes
                                && extent.first_page_id == report.first_page_id
                                && extent.last_page_id == report.last_page_id
                        })
                        .unwrap_or(false)
                });
        let envelope_checksum_ready = segment_reports
            .iter()
            .filter(|report| report.page_count > 0)
            .all(|report| !report.has_corruption && report.logical_bytes > 0);
        let compression_stream_ready = options.compression_enabled
            && segment_reports
                .iter()
                .any(|report| report.compressed_records > 0);
        let delayed_destroy_ready =
            summary.delayed_destroy_extents > 0 || summary.purged_extents > 0;
        let purge_lifecycle_ready = summary.purged_extents > 0;
        let extent_lifecycle_states = extent_lifecycle_states(&summary);
        let extent_state_transition_count = [
            summary.active_extents,
            summary.sealed_extents,
            summary.delayed_destroy_extents,
            summary.purged_extents,
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
                "append/roll extent lifecycle has not produced active plus sealed extents"
                    .to_string(),
            );
        }
        if !extent_manifest_ready {
            blockers.push("extent manifest is missing or inconsistent".to_string());
        }
        if !extent_manifest_rebuild_ready {
            blockers.push("extent manifest does not match stream page-id boundaries".to_string());
        }
        if !extent_manifest_disk_consistent {
            blockers.push(
                "extent manifest still diverges from live/delayed-destroy stream files".to_string(),
            );
        }
        if !zone_stats_ready {
            blockers.push("page-store zone usage accounting is inconsistent".to_string());
        }
        if !envelope_checksum_ready {
            blockers.push("stream record envelope/checksum inspection is not clean".to_string());
        }
        if corrupt_extent_count > 0 && partial_extent_recovery_ready {
            blockers.push(
                "corrupt stream extent detected; readable prefix was preserved in rebuilt manifest"
                    .to_string(),
            );
        }
        if !page_id_continuity_ready {
            blockers.push("stream page ids are not contiguous across extents".to_string());
        }

        let runtime_ready = blockers.is_empty();
        Ok(StreamBackedExtentRuntimeReport {
            runtime_ready,
            extent_lifecycle_states,
            extent_count: extents.len() as u64,
            active_extents: summary.active_extents,
            sealed_extents: summary.sealed_extents,
            delayed_destroy_extents: summary.delayed_destroy_extents,
            purged_extents: summary.purged_extents,
            zone_stats_ready,
            zone_usage,
            stream_segment_count,
            physical_bytes,
            logical_bytes,
            stream_record_count,
            first_page_id,
            last_page_id,
            page_id_continuity_ready,
            logical_stream_bytes_read: stats.logical_bytes_read,
            extent_state_transition_count,
            logical_stream_read_ready,
            append_roll_ready,
            extent_manifest_ready,
            extent_manifest_rebuild_ready,
            extent_manifest_reconciled_on_open,
            extent_manifest_disk_consistent,
            manifest_missing_stream_extents,
            manifest_extra_stream_extents,
            corrupt_extent_count,
            partial_extent_count,
            readable_prefix_physical_bytes,
            partial_extent_recovery_ready,
            envelope_checksum_ready,
            compression_stream_ready,
            delayed_destroy_ready,
            purge_lifecycle_ready,
            blockers,
            evidence: vec![
                "block records are appended as self-describing stream envelopes".to_string(),
                "logical stream reads span records while skipping envelopes and decompression"
                    .to_string(),
                "segment roll seals the previous extent and opens a new active extent".to_string(),
                "extent manifest persists active/sealed/delayed-destroy/purged lifecycle state"
                    .to_string(),
                "stream runtime reports page-id continuity and logical read byte evidence"
                    .to_string(),
                "extent manifest descriptors are validated against inspected stream boundaries"
                    .to_string(),
                "open-time reconciliation repairs manifest/live stream divergence like C++ zone updates"
                    .to_string(),
                "zone usage reports map extent ids to page-store used bytes like C++ ZoneStats"
                    .to_string(),
            ],
        })
    }
}
