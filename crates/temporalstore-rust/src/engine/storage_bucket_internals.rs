// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage snapshot/sample reporting + bucket-index maintenance internals, split from engine.rs.
use super::*;

pub(super) fn storage_slab_integrity_report(
    shard_id: ShardId,
    recovery: &StorageRecoveryReport,
    boundary: &StorageRecoveryBoundaryReport,
) -> StorageSlabIntegrityReport {
    let indexed_page_slab_count = recovery.active_page_slab_ids.len();
    let discovered_page_slab_count = recovery.page_slab_reports.len();
    let live_page_slab_count = recovery.live_page_slab_ids.len();
    let orphan_page_slab_count = boundary.orphan_page_slab_ids.len();
    let stale_page_ref_count = boundary.stale_index_page_refs.len();
    let corrupt_page_slab_count = boundary.corrupt_page_slab_ids.len();
    let unreadable_page_ref_count = recovery.unreadable_page_refs.len();
    let unreadable_page_bytes = boundary.unreadable_page_bytes;
    let owner_mismatch_page_ref_count = boundary.owner_mismatch_page_refs.len();
    let missing_owner_page_ref_count = boundary.missing_owner_page_refs;
    let reclaim_required = orphan_page_slab_count > 0
        || recovery
            .page_slab_live_reports
            .iter()
            .any(|report| report.stale_page_estimate > 0);
    let integrity_ok = stale_page_ref_count == 0
        && corrupt_page_slab_count == 0
        && unreadable_page_ref_count == 0
        && unreadable_page_bytes == 0
        && owner_mismatch_page_ref_count == 0
        && missing_owner_page_ref_count == 0
        && recovery.all_live_pages_readable;

    StorageSlabIntegrityReport {
        shard_id,
        indexed_page_slab_count,
        discovered_page_slab_count,
        live_page_slab_count,
        orphan_page_slab_count,
        stale_page_ref_count,
        corrupt_page_slab_count,
        unreadable_page_ref_count,
        unreadable_page_bytes,
        owner_mismatch_page_ref_count,
        missing_owner_page_ref_count,
        reclaim_required,
        integrity_ok,
    }
}

pub(super) fn storage_reclaim_candidates_from_recovery(
    recovery: &StorageRecoveryReport,
    fully_stale_slab_ids: &BTreeSet<u64>,
) -> Vec<StorageReclaimCandidate> {
    let mut candidates = recovery
        .page_slab_live_reports
        .iter()
        .filter_map(|report| {
            let fully_stale = fully_stale_slab_ids.contains(&report.page_slab_id);
            let stale_page_estimate = if fully_stale {
                report.page_count
            } else {
                report.stale_page_estimate
            };
            let stale_physical_bytes = if fully_stale {
                report.physical_bytes
            } else {
                report
                    .physical_bytes
                    .saturating_sub(report.live_physical_bytes)
            };
            if stale_page_estimate == 0 && stale_physical_bytes == 0 {
                return None;
            }
            let reclaim_score = stale_physical_bytes
                .saturating_mul(10_000_u64.saturating_sub(report.live_ref_density_basis_points))
                .saturating_div(10_000)
                .saturating_add(stale_page_estimate);
            Some(StorageReclaimCandidate {
                page_slab_id: report.page_slab_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: report.live_physical_bytes,
                stale_physical_bytes,
                page_count: report.page_count,
                live_page_refs: report.live_page_refs,
                stale_page_estimate,
                live_ref_density_basis_points: report.live_ref_density_basis_points,
                reclaim_score,
                reason: if fully_stale {
                    "orphan_segment".to_string()
                } else {
                    "low_live_density".to_string()
                },
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .reclaim_score
            .cmp(&left.reclaim_score)
            .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
            .then_with(|| left.page_slab_id.cmp(&right.page_slab_id))
    });
    candidates
}

pub(super) fn annotate_storage_manager_admin_stage_fields(
    stages: &mut [StorageManagerStageReport],
    last_run_unix_ms: u64,
    duration_ms: u64,
    errors: &[String],
    retention_blockers: usize,
) {
    // `duration_ms` is deliberately NOT set here. It used to be, to the whole round's duration, for
    // every stage -- so all eight phases reported the same number and the published
    // `..._phase_duration_ms` series could not say which phase was slow, which is the only question
    // a per-phase duration exists to answer. Each stage now times itself as it is recorded, and the
    // round total lives on the cycle report instead of being copied across the stages.
    let _ = duration_ms;
    for stage in stages {
        stage.last_run_unix_ms = last_run_unix_ms;
        if stage.skipped && stage.skipped_reason.is_empty() {
            stage.skipped_reason = stage.reason.clone();
        }
        if !errors.is_empty() {
            let prefix = format!("{}:", stage.stage);
            stage.errors = errors
                .iter()
                .filter(|error| error.starts_with(&prefix))
                .cloned()
                .collect();
        }
        stage.bytes_reclaimed = stage
            .page_bytes_reclaimed
            .max(stage.cache_disk_bytes_removed)
            .max(stage.before_bytes.saturating_sub(stage.after_bytes));
        stage.pages_compacted = stage.rewritten_page_refs;
        if stage.wal_floor_sequence == 0 {
            stage.wal_floor_sequence = stage.retain_from_wal_sequence;
        }
        if stage.index_log_floor_sequence == 0 {
            stage.index_log_floor_sequence = stage.retain_from_index_log_sequence;
        }
        if stage.retention_blockers == 0 {
            stage.retention_blockers = retention_blockers;
        }
        if stage.pressure_before == 0 {
            stage.pressure_before = stage.eviction_pressure_before.max(stage.before_bytes);
        }
        if stage.pressure_after == 0 {
            stage.pressure_after = stage.eviction_pressure_after.max(stage.after_bytes);
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct StorageManagerPhaseExecutor {
    round_started_unix_ms: u64,
    /// Monotonic start, for the DURATION.
    ///
    /// The unix millisecond above says WHEN the round began and is comparable across processes.
    /// It cannot say how LONG the round took: the clock behind it is adjustable, and an
    /// adjustment mid-round is indistinguishable from work -- which showed up as a round whose
    /// reported duration was shorter than the stages inside it.
    ///
    /// Passed in rather than captured here, because this executor is constructed at the END of a
    /// cycle. Capturing it here measures the finish and reports a round of zero, which is how the
    /// first version of this fix was caught.
    round_started_at: std::time::Instant,
}

impl StorageManagerPhaseExecutor {
    pub(super) fn new(round_started_unix_ms: u64, round_started_at: std::time::Instant) -> Self {
        Self {
            round_started_unix_ms,
            round_started_at,
        }
    }

    pub(super) fn annotate_reports(
        &self,
        stages: &mut [StorageManagerStageReport],
        errors: &[String],
        retention_blockers: usize,
    ) -> u64 {
        let round_duration_ms = self.round_started_at.elapsed().as_millis() as u64;
        annotate_storage_manager_admin_stage_fields(
            stages,
            self.round_started_unix_ms,
            round_duration_ms,
            errors,
            retention_blockers,
        );
        round_duration_ms
    }
}

#[derive(Debug, Clone)]
pub(super) struct LivePageEntry {
    pub(super) object_key: String,
    pub(super) kind: String,
    pub(super) component: Option<String>,
    pub(super) address: BlockAddress,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
    pub(super) log_backed: bool,
}

#[derive(Debug, Default)]
pub(super) struct StoragePageOwnershipValidation {
    pub(super) mismatches: Vec<StorageRecoveryPageOwnerMismatch>,
    pub(super) missing_owner_page_refs: usize,
}

pub(super) fn live_page_entry(
    object_key: impl Into<String>,
    kind: impl Into<String>,
    component: Option<String>,
    address: BlockAddress,
) -> LivePageEntry {
    LivePageEntry {
        object_key: object_key.into(),
        kind: kind.into(),
        component,
        // A page materialized in the block store carries a real page_id; a page
        // backed only by the hot/append-log buffer does not. Evaluate before the
        // `address` field moves it.
        log_backed: address.page_id.is_none(),
        address,
        dirty: false,
        deleted: false,
    }
}

pub(super) fn storage_page_address_sample(
    shard_id: ShardId,
    address: &BlockAddress,
) -> StoragePageAddressSample {
    StoragePageAddressSample {
        shard_id,
        zone_id: address.band_id.unwrap_or(address.page_slab_id),
        slab_id: address.page_slab_id,
        page_id: address.page_id.unwrap_or(address.page_slab_id),
        offset: address.offset,
        length: address.length,
        generation: address.object_id.unwrap_or(0),
    }
}

pub(super) fn storage_block_address_sample(
    shard_id: ShardId,
    address: &BlockAddress,
) -> StorageBlockAddressSample {
    StorageBlockAddressSample {
        shard_id,
        zone_id: address.band_id.unwrap_or(address.page_slab_id),
        block_id: address.page_slab_id,
        offset: address.offset,
        length: address.length,
        checksum: address.sha256.clone().unwrap_or_default(),
    }
}

pub(super) fn storage_index_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    mut snapshot: StorageIndexSnapshot,
) -> StorageIndexSnapshot {
    let mut entries = collect_live_page_entries(shard);
    entries.sort_by(|left, right| {
        (
            left.kind.as_str(),
            left.object_key.as_str(),
            left.component.as_deref().unwrap_or(""),
            left.address.page_slab_id,
            left.address.offset,
        )
            .cmp(&(
                right.kind.as_str(),
                right.object_key.as_str(),
                right.component.as_deref().unwrap_or(""),
                right.address.page_slab_id,
                right.address.offset,
            ))
    });

    const MAX_STORAGE_INDEX_SAMPLES: usize = 8;
    snapshot.page_index_entry_samples = entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .map(|entry| {
            let page_address = storage_page_address_sample(shard_id, &entry.address);
            StoragePageIndexEntrySample {
                logical_key: entry.object_key.clone(),
                timestamp_range: None,
                page_addresses: vec![page_address],
                append_watermark: entry.address.offset,
                generation: entry.address.object_id.unwrap_or(0),
            }
        })
        .collect();
    snapshot.block_index_entry_samples = entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .map(|entry| {
            let page_address = storage_page_address_sample(shard_id, &entry.address);
            let block_address = storage_block_address_sample(shard_id, &entry.address);
            StorageBlockIndexEntrySample {
                band: entry
                    .address
                    .band_id
                    .unwrap_or(entry.address.page_slab_id),
                checksum: entry.address.sha256.clone().unwrap_or_default(),
                generation: entry.address.object_id.unwrap_or(0),
                page_address,
                block_address,
            }
        })
        .collect();

    let mut object_entries: BTreeMap<(String, String, String), StorageObjectIndexEntrySample> =
        BTreeMap::new();
    for entry in entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES.saturating_mul(4))
    {
        let key = (
            entry.kind.clone(),
            entry.kind.clone(),
            entry.object_key.clone(),
        );
        let sample = object_entries
            .entry(key)
            .or_insert_with(|| StorageObjectIndexEntrySample {
                model: entry.kind.clone(),
                table: entry.kind.clone(),
                object_key: entry.object_key.clone(),
                page_chain: Vec::new(),
                tombstone: entry.deleted,
                generation: entry.address.object_id.unwrap_or(0),
            });
        if sample.page_chain.len() < MAX_STORAGE_INDEX_SAMPLES {
            sample
                .page_chain
                .push(storage_page_address_sample(shard_id, &entry.address));
        }
        sample.tombstone |= entry.deleted;
        sample.generation = sample.generation.max(entry.address.object_id.unwrap_or(0));
    }
    snapshot.object_index_entry_samples = object_entries
        .into_iter()
        .map(|(_, sample)| sample)
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .collect();
    snapshot
}

pub(super) fn storage_gc_ref(entry: &LivePageEntry) -> String {
    match entry.component.as_deref() {
        Some(component) if !component.is_empty() => {
            format!("{}:{}:{}", entry.kind, entry.object_key, component)
        }
        _ => format!("{}:{}", entry.kind, entry.object_key),
    }
}

pub(super) fn storage_watermark_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    mut snapshot: StorageWatermarkSnapshot,
) -> StorageWatermarkSnapshot {
    const MAX_STORAGE_WATERMARK_SAMPLES: usize = 8;
    let timestamp_ms = now_ms();
    let mut bucket_watermarks = BTreeMap::<u32, u64>::new();

    for (bucket_id, runtime_bucket) in &shard.bucket_index.bucket_map {
        bucket_watermarks.insert(*bucket_id, runtime_bucket.dirty_generation);
    }
    for entry in collect_live_page_entries(shard) {
        let bucket_id = entry
            .address
            .routing_bucket
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        let generation = entry.address.object_id.unwrap_or(0);
        bucket_watermarks
            .entry(bucket_id)
            .and_modify(|current| *current = (*current).max(generation))
            .or_insert(generation);
    }

    snapshot.append_watermark_samples = bucket_watermarks
        .iter()
        .take(MAX_STORAGE_WATERMARK_SAMPLES)
        .map(|(bucket_id, generation)| StorageAppendWatermarkSample {
            shard_id,
            bucket_id: *bucket_id,
            log_index: (*generation).max(snapshot.append_watermark),
            timestamp_ms,
        })
        .collect();
    if snapshot.append_watermark_samples.is_empty() && snapshot.append_watermark > 0 {
        snapshot
            .append_watermark_samples
            .push(StorageAppendWatermarkSample {
                shard_id,
                bucket_id: 0,
                log_index: snapshot.append_watermark,
                timestamp_ms,
            });
    }

    snapshot.compaction_watermark_samples = vec![StorageCompactionWatermarkSample {
        shard_id,
        safe_generation: snapshot.compaction_watermark,
        safe_timestamp_ms: snapshot.follower_cursor_safe_watermark,
        follower_floor: snapshot.follower_cursor_retention_floor,
    }];
    snapshot
}

pub(super) fn storage_gc_snapshot_with_samples(
    _shard_id: ShardId,
    shard: &ShardState,
    mut snapshot: StorageGcSnapshot,
) -> StorageGcSnapshot {
    let mut entries = collect_live_page_entries(shard);
    entries.sort_by(|left, right| {
        (
            left.deleted,
            left.kind.as_str(),
            left.object_key.as_str(),
            left.component.as_deref().unwrap_or(""),
            left.address.page_slab_id,
            left.address.offset,
        )
            .cmp(&(
                right.deleted,
                right.kind.as_str(),
                right.object_key.as_str(),
                right.component.as_deref().unwrap_or(""),
                right.address.page_slab_id,
                right.address.offset,
            ))
    });

    const MAX_STORAGE_GC_SAMPLES: usize = 8;
    let now = now_ms();
    snapshot.tombstone_samples = entries
        .iter()
        .filter(|entry| entry.deleted)
        .take(MAX_STORAGE_GC_SAMPLES)
        .map(|entry| StorageTombstoneSample {
            ref_id: storage_gc_ref(entry),
            generation: entry.address.object_id.unwrap_or(0),
            deleted_at_ms: now,
            reason: "object_tombstone".to_string(),
        })
        .collect();

    let follower_safe = snapshot.follower_cursor_safe_to_reclaim;
    let mut eligibility_samples: Vec<StorageGcEligibilitySample> = entries
        .iter()
        .filter_map(|entry| {
            let eligible_after_ms = shard
                .expires_at_ms
                .get(&entry.object_key)
                .copied()
                .unwrap_or(0);
            let has_tombstone = entry.deleted;
            let ttl_eligible = eligible_after_ms > 0 && eligible_after_ms <= now;
            if !has_tombstone && !ttl_eligible {
                return None;
            }
            Some(StorageGcEligibilitySample {
                ref_id: storage_gc_ref(entry),
                eligible_after_ms,
                has_tombstone,
                follower_safe,
                reclaimable_bytes: if follower_safe {
                    entry.address.length
                } else {
                    0
                },
            })
        })
        .take(MAX_STORAGE_GC_SAMPLES)
        .collect();

    if eligibility_samples.is_empty() && snapshot.gc_eligible_record_count > 0 {
        eligibility_samples.push(StorageGcEligibilitySample {
            ref_id: "aggregate:gc_eligible_records".to_string(),
            eligible_after_ms: 0,
            has_tombstone: snapshot.tombstone_records > 0,
            follower_safe,
            reclaimable_bytes: if follower_safe {
                snapshot.reclaimable_bytes
            } else {
                0
            },
        });
    }
    snapshot.gc_eligibility_samples = eligibility_samples;

    snapshot.follower_cursor_safety_samples = vec![StorageFollowerCursorSafetySample {
        min_follower_cursor: snapshot.follower_cursor_retention_floor,
        blocked_reclaim_bytes: if follower_safe {
            0
        } else {
            snapshot.reclaimable_bytes
        },
        safe_to_reclaim: follower_safe,
    }];
    snapshot
}

pub(super) fn storage_topology_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    mut snapshot: StorageTopologySnapshot,
) -> StorageTopologySnapshot {
    let mut entries = collect_live_page_entries(shard);
    entries.sort_by(|left, right| {
        (
            left.address
                .band_id
                .unwrap_or(left.address.page_slab_id),
            left.address.page_slab_id,
            left.address.offset,
            left.kind.as_str(),
            left.object_key.as_str(),
        )
            .cmp(&(
                right
                    .address
                    .band_id
                    .unwrap_or(right.address.page_slab_id),
                right.address.page_slab_id,
                right.address.offset,
                right.kind.as_str(),
                right.object_key.as_str(),
            ))
    });

    const MAX_STORAGE_TOPOLOGY_SAMPLES: usize = 8;
    #[derive(Default)]
    struct ZoneAcc {
        used_bytes: u64,
        stale_bytes: u64,
        slabs: BTreeSet<u64>,
        generation: u64,
    }
    #[derive(Default)]
    struct SlabAcc {
        band_id: u64,
        start_offset: u64,
        generation: u64,
        deleted_refs: u64,
        live_refs: u64,
    }
    #[derive(Default)]
    struct BandAcc {
        min_offset: u64,
        max_offset: u64,
        generation: u64,
        deleted_refs: u64,
        live_refs: u64,
    }
    #[derive(Default)]
    struct BucketAcc {
        dirty_generation: u64,
        object_refs: BTreeSet<u64>,
        page_refs: Vec<StoragePageAddressSample>,
        tombstones: BTreeSet<String>,
    }

    let mut zones = BTreeMap::<u64, ZoneAcc>::new();
    let mut slabs = BTreeMap::<u64, SlabAcc>::new();
    let mut bands = BTreeMap::<u64, BandAcc>::new();
    let mut buckets = BTreeMap::<u32, BucketAcc>::new();

    for entry in &entries {
        let zone_id = entry
            .address
            .band_id
            .unwrap_or(entry.address.page_slab_id);
        let slab_id = entry.address.page_slab_id;
        let generation = entry.address.object_id.unwrap_or(0);
        let zone = zones.entry(zone_id).or_default();
        zone.slabs.insert(slab_id);
        zone.generation = zone.generation.max(generation);
        if entry.deleted {
            zone.stale_bytes = zone.stale_bytes.saturating_add(entry.address.length);
        } else {
            zone.used_bytes = zone.used_bytes.saturating_add(entry.address.length);
        }

        let slab = slabs.entry(slab_id).or_insert_with(|| SlabAcc {
            band_id: zone_id,
            start_offset: entry.address.offset,
            ..SlabAcc::default()
        });
        slab.start_offset = slab.start_offset.min(entry.address.offset);
        slab.generation = slab.generation.max(generation);
        if entry.deleted {
            slab.deleted_refs = slab.deleted_refs.saturating_add(1);
        } else {
            slab.live_refs = slab.live_refs.saturating_add(1);
        }

        let band = bands.entry(zone_id).or_insert_with(|| BandAcc {
            min_offset: entry.address.offset,
            max_offset: entry.address.offset.saturating_add(entry.address.length),
            ..BandAcc::default()
        });
        band.min_offset = band.min_offset.min(entry.address.offset);
        band.max_offset = band
            .max_offset
            .max(entry.address.offset.saturating_add(entry.address.length));
        band.generation = band.generation.max(generation);
        if entry.deleted {
            band.deleted_refs = band.deleted_refs.saturating_add(1);
        } else {
            band.live_refs = band.live_refs.saturating_add(1);
        }

        let bucket_id = entry
            .address
            .routing_bucket
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        let bucket = buckets.entry(bucket_id).or_default();
        bucket.dirty_generation = bucket.dirty_generation.max(generation);
        bucket.object_refs.insert(generation);
        if bucket.page_refs.len() < MAX_STORAGE_TOPOLOGY_SAMPLES {
            bucket.page_refs
                .push(storage_page_address_sample(shard_id, &entry.address));
        }
        if entry.deleted {
            bucket.tombstones.insert(storage_gc_ref(entry));
        }
    }

    for (bucket_id, runtime_bucket) in &shard.bucket_index.bucket_map {
        let bucket = buckets.entry(*bucket_id).or_default();
        bucket.dirty_generation = bucket.dirty_generation.max(runtime_bucket.dirty_generation);
        bucket.object_refs
            .extend(runtime_bucket.object_index.iter().copied());
        for page in runtime_bucket.page_index.values() {
            if bucket.page_refs.len() >= MAX_STORAGE_TOPOLOGY_SAMPLES {
                break;
            }
            bucket.page_refs
                .push(storage_page_address_sample(shard_id, &page.address));
            if page.deleted {
                bucket.tombstones
                    .insert(format!("{}:{}", page.model_id, page.object_key));
            }
        }
    }

    snapshot.storage_zone_samples = zones
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(zone_id, zone)| StorageZoneSample {
            zone_id,
            total_bytes: zone.used_bytes.saturating_add(zone.stale_bytes),
            used_bytes: zone.used_bytes,
            stale_bytes: zone.stale_bytes,
            slabs: zone.slabs.into_iter().collect(),
        })
        .collect();
    let stream_slabs = slabs.keys().copied().collect::<Vec<_>>();
    snapshot.stream_samples = (!stream_slabs.is_empty())
        .then(|| StorageStreamSample {
            stream_id: format!("shard:{shard_id}:page_stream"),
            rollover_count: snapshot.slab_open_count.saturating_sub(1),
            sealed_slab_count: snapshot.slab_sealed_count,
            slabs: stream_slabs
                .iter()
                .copied()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
        })
        .into_iter()
        .collect();
    snapshot.slab_samples = slabs
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(slab_id, slab)| StorageSlabSample {
            slab_id,
            band: slab.band_id,
            start_offset: slab.start_offset,
            sealed: slab.live_refs == 0 || slab.deleted_refs > 0,
            generation: slab.generation,
        })
        .collect();
    snapshot.band_samples = bands
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(band_id, band)| StorageBandSample {
            band: band_id,
            block_range: vec![band.min_offset, band.max_offset],
            reclaim_state: if band.deleted_refs > 0 && band.live_refs == 0 {
                "reclaimable".to_string()
            } else if band.deleted_refs > 0 {
                "mixed_live_stale".to_string()
            } else {
                "live".to_string()
            },
            generation: band.generation,
        })
        .collect();
    snapshot.bucket_samples = buckets
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(bucket_id, bucket)| StorageBucketSample {
            bucket_id,
            dirty_generation: bucket.dirty_generation,
            object_refs: bucket
                .object_refs
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            page_refs: bucket
                .page_refs
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            tombstones: bucket
                .tombstones
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            owner_mismatch_count: 0,
        })
        .collect();
    snapshot
}

/// Running total of live-page entries materialized by [`collect_live_page_entries`].
///
/// This walk is `O(live pages)` and clones two strings per entry, and several callers run it on
/// a background loop, so its cost is easy to introduce and hard to notice. The counter makes it
/// measurable: a test can assert that a code path's scan volume does not grow with the store.
static LIVE_PAGE_SCAN_ENTRIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Live-page entries materialized since the last reset.
pub fn live_page_scan_entries() -> u64 {
    LIVE_PAGE_SCAN_ENTRIES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the scan counter. For tests measuring one operation's scan volume.
pub fn reset_live_page_scan_entries() {
    LIVE_PAGE_SCAN_ENTRIES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Running total of bucket `page_index` entries visited by the bucket-maintenance walks.
///
/// Distinct from [`LIVE_PAGE_SCAN_ENTRIES`], which counts materialized live-page entries. This
/// one counts the cheaper-looking `bucket.page_index.values()` passes -- `update_bucket_layout`
/// and the per-object dirty-state clear. Each is `O(pages in the bucket)` and they run inside
/// loops over buckets, so their cost is a product, not a sum, and does not show up in any single
/// obvious place.
static BUCKET_PAGE_INDEX_VISITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Bucket `page_index` entries visited since the last reset.
pub fn bucket_page_index_visits() -> u64 {
    BUCKET_PAGE_INDEX_VISITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the bucket-visit counter. For tests measuring one operation's maintenance volume.
pub fn reset_bucket_page_index_visits() {
    BUCKET_PAGE_INDEX_VISITS.store(0, std::sync::atomic::Ordering::Relaxed);
}

fn note_bucket_page_visits(count: usize) {
    BUCKET_PAGE_INDEX_VISITS.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
}

pub(super) fn collect_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    let entries = if !shard.bucket_index.bucket_map.is_empty() {
        collect_bucket_index_live_page_entries(shard)
    } else {
        collect_model_live_page_entries(shard)
    };
    LIVE_PAGE_SCAN_ENTRIES.fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
    entries
}

pub(super) fn mark_async_dirty_object(
    shard: &mut ShardState,
    object_key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    let routing_bucket = page_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    shard.dirty_objects.insert(object_key.to_string());
    let bucket = shard
        .bucket_index
        .bucket_map
        .entry(routing_bucket)
        .or_insert_with(|| BucketNode {
            routing_bucket,
            meta_loaded: true,
            ..BucketNode::default()
        });
    bucket.dirty = true;
    bucket.dirty_generation = bucket.dirty_generation.saturating_add(1).max(1);
}

pub(super) fn rebuild_bucket_page_ownership(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    // Preserve the durable per-bucket watermarks across the clear+rebuild. load_index keeps
    // dirty_generation / last_dump_sequence; rebuilding with a fresh BucketNode::default()
    // would zero them, making a restored shard (e.g. after a manifest install) mismatch its
    // own dump-manifest generation and forcing unnecessary re-dumps (and mis-driving the
    // index/GC reclaim watermarks). Carry the prior values over via the snapshot below.
    let preserved_bucket_watermarks: HashMap<u32, (u64, u64)> = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| {
            (
                *routing_bucket,
                (bucket.dirty_generation, bucket.last_dump_sequence),
            )
        })
        .collect();
    shard.bucket_index.bucket_map.clear();
    for entry in collect_model_live_page_entries(shard) {
        let routing_bucket = entry.address.routing_bucket.unwrap_or_else(|| {
            page_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket)
        });
        if routing_bucket < start_routing_bucket || routing_bucket > end_routing_bucket {
            continue;
        }
        let object_id = entry.address.object_id.unwrap_or_else(|| {
            stable_page_object_id(
                shard_id,
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
            )
        });
        let bucket = shard
            .bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| {
                let (dirty_generation, last_dump_sequence) = preserved_bucket_watermarks
                    .get(&routing_bucket)
                    .copied()
                    .unwrap_or_default();
                BucketNode {
                    routing_bucket,
                    meta_loaded: true,
                    in_memory: true,
                    dirty_generation,
                    last_dump_sequence,
                    ..BucketNode::default()
                }
            });
        bucket.object_index.insert(object_id);
        bucket.page_index.insert(
            format!(
                "{}:{}:{}:{}:{}",
                entry.kind,
                entry.object_key,
                entry.component.clone().unwrap_or_default(),
                entry.address.page_slab_id,
                entry.address.offset
            ),
            PageIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component,
                object_id,
                address: entry.address,
                dirty: entry.dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
        );
    }
    shard.bucket_index.rebuild_object_page_lookup();
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        bucket.meta_loaded = true;
        bucket.loading = false;
        bucket.in_memory = !bucket.page_index.is_empty();
        bucket.deleted =
            !bucket.page_index.is_empty() && bucket.page_index.values().all(|page| page.deleted);
        update_bucket_layout(bucket);
    }
}

pub(super) fn promote_model_maps_to_bucket_index_authority(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> bool {
    let model_entries = collect_model_live_page_entries(shard);
    if model_entries.is_empty() {
        return false;
    }
    let bucket_index_missing_entry = shard.bucket_index.bucket_map.is_empty()
        || model_entries.iter().any(|entry| {
            !shard.bucket_index.contains_object_page_address(
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
                &entry.address,
            )
        });
    if !bucket_index_missing_entry {
        return false;
    }
    rebuild_bucket_page_ownership(shard_id, shard, start_routing_bucket, end_routing_bucket);
    refresh_bucket_runtime_flags(shard);
    true
}

/// Rebuild ONLY the model maps that are `skip_serializing` -- today just `hashes` -- from the
/// durable bucket index. A freshly deserialized index never carries them, so anything deriving
/// state from the model maps would see a shard with no hash objects at all.
///
/// Deliberately narrow: the serialized maps are left exactly as decoded. Rebuilding those from
/// the bucket index too would overwrite whatever the index actually said, which is precisely
/// the disagreement a manifest cross-check exists to detect -- a tampered `strings` object_id
/// would be silently repaired instead of rejected.
pub(super) fn rebuild_unserialized_model_maps_from_bucket_index(shard: &mut ShardState) {
    if shard.bucket_index.bucket_map.is_empty() {
        return;
    }
    let mut hashes = HashMap::<String, HashMap<String, BlockAddress>>::new();
    for entry in collect_bucket_index_live_page_entries(shard) {
        if entry.deleted || entry.kind != "hash" {
            continue;
        }
        hashes
            .entry(entry.object_key)
            .or_default()
            .insert(entry.component.unwrap_or_default(), entry.address);
    }
    if !hashes.is_empty() {
        shard.hashes = hashes;
    }
}

pub(super) fn collect_bucket_index_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    let mut entries = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.page_index.values() {
            entries.push(LivePageEntry {
                object_key: page.object_key.clone(),
                kind: page.model_id.clone(),
                component: page.component.clone(),
                address: page.address.clone(),
                dirty: page.dirty,
                deleted: page.deleted,
                log_backed: page.log_backed,
            });
        }
    }
    entries
}

/// Cheap O(1)-per-map check for whether the shard holds ANY live model-map entry that
/// `collect_model_live_page_entries` would enumerate. Used to avoid latching the phase-1
/// `promote_scan_done` fast-skip flag before the shard has any state to reconcile. Short-circuits
/// on the first non-empty map; never clones.
pub(super) fn shard_has_model_entries(shard: &ShardState) -> bool {
    !shard.strings.is_empty()
        || !shard.hashes.is_empty()
        || !shard.sets.is_empty()
        || !shard.lists.is_empty()
        || !shard.zsets.is_empty()
        || !shard.features.is_empty()
        || !shard.control_state_pages.is_empty()
        || !shard.context_nodes.is_empty()
        || !shard.context_events.is_empty()
        || !shard.context_indexes.is_empty()
        || !shard.context_audits.is_empty()
        || !shard.context_entities.is_empty()
        || !shard.context_children.is_empty()
        || !shard.context_summaries.is_empty()
        || !shard.context_compressions.is_empty()
}

pub(super) fn collect_model_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    let mut entries = Vec::new();
    entries.extend(
        shard
            .strings
            .iter()
            .map(|(key, address)| live_page_entry(key.clone(), "string", None, address.clone())),
    );
    for (key, fields) in &shard.hashes {
        entries.extend(fields.iter().map(|(field, address)| {
            live_page_entry(key.clone(), "hash", Some(field.clone()), address.clone())
        }));
    }
    for (key, members) in &shard.zsets {
        entries.extend(members.iter().map(|(member, (biased, address))| {
            live_page_entry(
                key.clone(),
                "zset",
                Some(format!("{biased:016x}{}", hex::encode(member))),
                address.clone(),
            )
        }));
    }
    for (key, elements) in &shard.lists {
        entries.extend(elements.iter().map(|(seq, address)| {
            live_page_entry(
                key.clone(),
                "list",
                Some(format!("{:016x}", (*seq as u64).wrapping_sub(i64::MIN as u64))),
                address.clone(),
            )
        }));
    }
    for (key, members) in &shard.sets {
        entries.extend(members.iter().map(|(member, address)| {
            live_page_entry(
                key.clone(),
                "set",
                Some(hex::encode(member)),
                address.clone(),
            )
        }));
    }
    for (key, series) in &shard.features {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "feature", None, address)),
        );
    }
    entries.extend(
        shard
            .control_state_pages
            .iter()
            .map(|(key, address)| live_page_entry(key.clone(), "control_state", None, address.clone())),
    );
    entries.extend(
        shard.context_nodes.iter().map(|(key, address)| {
            live_page_entry(key.clone(), "context_node", None, address.clone())
        }),
    );
    for (key, series) in &shard.context_events {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_event", None, address)),
        );
    }
    for (key, series) in &shard.context_indexes {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_index", None, address)),
        );
    }
    for (key, series) in &shard.context_audits {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_audit", None, address)),
        );
    }
    // Entities live grouped by node in memory but persist one entry per entity, under the same
    // `ctx:entity:{tenant}:{node}:{entity_hash}` key as before the fold -- the collection key
    // plus the BTree key reproduce it exactly. Keeping the on-disk key per entity is what makes
    // this change format-compatible in both directions.
    for (collection_key, series) in &shard.context_entities {
        entries.extend(series.iter().map(|(entity_hash, address)| {
            live_page_entry(
                format!("{collection_key}:{entity_hash}"),
                "context_entity",
                None,
                address.clone(),
            )
        }));
    }
    for (key, series) in &shard.context_children {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_child", None, address)),
        );
    }
    for (key, series) in &shard.context_summaries {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_summary", None, address)),
        );
    }
    for (key, series) in &shard.context_compressions {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_compression", None, address)),
        );
    }
    entries
}

pub(super) fn page_index_ref_key(entry: &LivePageEntry) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:{}",
        entry.kind,
        entry.object_key,
        entry.component.as_deref().unwrap_or(""),
        entry.address.page_slab_id,
        entry.address.offset,
        entry.address.length,
        entry.address.page_id.unwrap_or_default(),
        entry.address.generation.unwrap_or_default()
    )
}

pub(super) fn page_physical_identity_key(
    address: &BlockAddress,
) -> (
    u64,
    u64,
    u64,
    Option<u64>,
    Option<u64>,
    Option<u32>,
    Option<u64>,
) {
    (
        address.page_slab_id,
        address.offset,
        address.length,
        address.page_id,
        address.object_id,
        address.routing_bucket,
        address.generation,
    )
}

pub(super) fn upsert_bucket_index_page(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    component: Option<String>,
    address: BlockAddress,
    dirty: bool,
) {
    let routing_bucket = address
        .routing_bucket
        .unwrap_or_else(|| page_routing_bucket(object_key, 0, u32::MAX));
    let object_id = address
        .object_id
        .unwrap_or_else(|| stable_page_object_id(shard_id, kind, object_key, component.as_deref()));
    // This IS the outcome: an object, its identity, and where its page ended up. Put it aside
    // for the record, so replay has the option of installing it instead of re-running the
    // command that produced it.
    if crate::wal::wal_outcome_items_enabled() {
        super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
            kind: kind.to_string(),
            object_key: object_key.to_string(),
            component: component.clone(),
            object_id,
            routing_bucket,
            address: Some(address.clone()),
            value: None,
            ttl: None,
            deleted: false,
            meta: false,
        });
    }
    let entry = LivePageEntry {
        object_key: object_key.to_string(),
        kind: kind.to_string(),
        component,
        log_backed: address.page_id.is_none(),
        address,
        dirty,
        deleted: false,
    };
    let lookup_enabled = !shard.bucket_index.object_page_lookup.is_empty();
    let direct_page_refs = if lookup_enabled {
        shard
            .bucket_index
            .object_page_lookup
            .get(&object_page_lookup_key(
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
            ))
            .cloned()
    } else {
        None
    };
    shard.bucket_index.remove_object_page_lookup_entry(
        &entry.kind,
        &entry.object_key,
        entry.component.as_deref(),
    );
    if let Some(page_refs) = direct_page_refs {
        for page_ref in page_refs {
            let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&page_ref.routing_bucket) else {
                continue;
            };
            let removed_object_id = bucket
                .page_index
                .remove(&page_ref.page_ref_key)
                .map(|page| page.object_id);
            if let Some(removed_object_id) = removed_object_id {
                if !bucket
                    .page_index
                    .values()
                    .any(|page| page.object_id == removed_object_id)
                {
                    bucket.object_index.remove(&removed_object_id);
                }
                update_bucket_layout(bucket);
            }
        }
    } else if !lookup_enabled {
        for bucket in shard.bucket_index.bucket_map.values_mut() {
            bucket.page_index.retain(|_, page| {
                !(page.object_key == entry.object_key
                    && page.model_id == entry.kind
                    && page.component == entry.component)
            });
            if !bucket
                .page_index
                .values()
                .any(|page| page.object_id == object_id)
            {
                bucket.object_index.remove(&object_id);
            }
            update_bucket_layout(bucket);
        }
    }
    let page_ref_key = page_index_ref_key(&entry);
    let page_index = PageIndex {
        object_key: entry.object_key,
        model_id: entry.kind,
        component: entry.component,
        object_id,
        address: entry.address,
        dirty: entry.dirty,
        deleted: entry.deleted,
        log_backed: entry.log_backed,
    };
    {
        let bucket = shard
            .bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket,
                meta_loaded: true,
                in_memory: true,
                ..BucketNode::default()
            });
        bucket.dirty |= dirty;
        bucket.deleted = false;
        if dirty {
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
        }
        bucket.in_memory = true;
        bucket.object_index.insert(object_id);
        bucket.page_index
            .insert(page_ref_key.clone(), page_index.clone());
        update_bucket_layout(bucket);
    }
    shard
        .bucket_index
        .insert_object_page_lookup(routing_bucket, page_ref_key, &page_index);
}

pub(super) fn sync_bucket_index_object_pages(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    addresses: Vec<BlockAddress>,
    dirty: bool,
) {
    let mut touched_buckets = BTreeSet::new();
    let mut removed_any = false;
    let target_buckets = if shard.bucket_index.object_component_lookup.is_empty() {
        shard
            .bucket_index
            .bucket_map
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
    } else {
        shard
            .bucket_index
            .object_component_lookup
            .get(&object_component_lookup_key(kind, object_key))
            .map(|page_refs| {
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.routing_bucket)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default()
    };
    for routing_bucket in target_buckets {
        let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
            continue;
        };
        let before = bucket.page_index.len();
        bucket.page_index
            .retain(|_, page| !(page.model_id == kind && page.object_key == object_key));
        if bucket.page_index.len() != before {
            removed_any = true;
            touched_buckets.insert(routing_bucket);
            bucket.dirty |= dirty;
            bucket.deleted = bucket.page_index.is_empty();
            if dirty {
                bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
            }
            bucket.in_memory = !bucket.page_index.is_empty();
            update_bucket_layout(bucket);
        }
    }

    let mut unique_addresses = BTreeMap::<
        (
            u64,
            u64,
            u64,
            Option<u64>,
            Option<u64>,
            Option<u32>,
            Option<u64>,
        ),
        BlockAddress,
    >::new();
    for address in addresses {
        unique_addresses.insert(page_physical_identity_key(&address), address);
    }

    for address in unique_addresses.into_values() {
        let routing_bucket = address
            .routing_bucket
            .unwrap_or_else(|| page_routing_bucket(object_key, 0, u32::MAX));
        let object_id = address
            .object_id
            .unwrap_or_else(|| stable_page_object_id(shard_id, kind, object_key, None));
        let entry = LivePageEntry {
            object_key: object_key.to_string(),
            kind: kind.to_string(),
            component: None,
            log_backed: address.page_id.is_none(),
            address,
            dirty,
            deleted: false,
        };
        let bucket = shard
            .bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket,
                meta_loaded: true,
                in_memory: true,
                ..BucketNode::default()
            });
        bucket.dirty |= dirty;
        bucket.deleted = false;
        if dirty || touched_buckets.insert(routing_bucket) {
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
        }
        bucket.meta_loaded = true;
        bucket.loading = false;
        bucket.in_memory = true;
        bucket.object_index.insert(object_id);
        bucket.deleted_object_index.remove(&object_id);
        bucket.page_index.insert(
            page_index_ref_key(&entry),
            PageIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component,
                object_id,
                address: entry.address,
                dirty: entry.dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
        );
        update_bucket_layout(bucket);
    }

    if removed_any || dirty {
        shard
            .bucket_index
            .bucket_map
            .retain(|_, bucket| !bucket.page_index.is_empty() || !bucket.object_index.is_empty());
    }
    shard.bucket_index.rebuild_object_page_lookup();
}

pub(super) fn classify_bucket_layout(object_count: usize, page_ref_count: usize) -> BucketLayoutState {
    match (object_count, page_ref_count) {
        (0, _) => BucketLayoutState::Empty,
        (1, 0) => BucketLayoutState::SingleObject,
        (_, 0) => BucketLayoutState::Empty,
        (1, 1) => BucketLayoutState::SinglePageObject,
        (1, _) => BucketLayoutState::MultiPageObject,
        _ => BucketLayoutState::MultiObject,
    }
}

pub(super) fn bucket_layout_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object",
        BucketLayoutState::SinglePageObject => "single_page_object",
        BucketLayoutState::MultiPageObject => "multi_page_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

pub(super) fn update_bucket_layout(bucket: &mut BucketNode) {
    note_bucket_page_visits(bucket.page_index.len());
    let live_object_ids: BTreeSet<u64> = bucket
        .page_index
        .values()
        .filter(|page| !page.deleted)
        .map(|page| page.object_id)
        .collect();
    if !live_object_ids.is_empty() {
        bucket.object_index = live_object_ids;
    } else if !bucket.page_index.is_empty() {
        bucket.object_index.clear();
    }
    bucket.layout = classify_bucket_layout(bucket.object_index.len(), bucket.page_index.len());
}

pub(super) fn refresh_bucket_runtime_flags(shard: &mut ShardState) {
    let now = now_ms();
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        bucket.meta_loaded = true;
        bucket.loading = false;
        bucket.in_memory = !bucket.page_index.is_empty();
        bucket.deleted =
            !bucket.page_index.is_empty() && bucket.page_index.values().all(|page| page.deleted);
        bucket.dirty |= bucket
            .page_index
            .values()
            .any(|page| page.dirty || shard.dirty_objects.contains(&page.object_key));
        bucket.ttl_ms = bucket
            .page_index
            .values()
            .filter_map(|page| shard.expires_at_ms.get(&page.object_key).copied())
            .map(|expires_at| expires_at.saturating_sub(now))
            .min();
        update_bucket_layout(bucket);
    }
}

pub(super) fn object_still_has_hot_page(shard: &ShardState, object_key: &str) -> bool {
    shard
        .strings
        .get(object_key)
        .map(|address| crate::wal_record::is_wal_resident(address.page_slab_id))
        .unwrap_or(false)
        || shard
            .hashes
            .get(object_key)
            .map(|fields| {
                fields
                    .values()
                    .any(|address| crate::wal_record::is_wal_resident(address.page_slab_id))
            })
            .unwrap_or(false)
}

pub(super) fn clear_published_object_dirty_state(shard: &mut ShardState, object_key: &str) {
    if object_still_has_hot_page(shard, object_key) {
        return;
    }
    shard.dirty_objects.remove(object_key);
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        note_bucket_page_visits(bucket.page_index.len());
        let mut touched = false;
        for page in bucket.page_index.values_mut() {
            if page.object_key == object_key {
                page.dirty = false;
                touched = true;
            }
        }
        if touched {
            note_bucket_page_visits(bucket.page_index.len());
            bucket.dirty = bucket
                .page_index
                .values()
                .any(|page| page.dirty || shard.dirty_objects.contains(&page.object_key));
            update_bucket_layout(bucket);
        }
    }
}

pub(super) fn rebuild_bucket_first_index(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    // Preserve tombstone (deleted) object ids across the rebuild. A delete removes the object
    // from the model maps (strings/hashes/...), so collect_model_live_page_entries no longer
    // sees it, but the object manager must keep reporting it as a tombstone until GC reclaims
    // the slot. The deserialize + reconcile load path keeps deleted_object_index; a
    // promote/rebuild reconstruct (flush or the WAL-replay tail) would otherwise silently drop
    // it, undercounting objects after a reconstruct-based reload.
    let prior_deleted_object_index: BTreeMap<u32, ObjectIndex> = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| !bucket.deleted_object_index.is_empty())
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.deleted_object_index.clone()))
        .collect();
    let mut bucket_index = CoreIndex::default();
    for entry in collect_model_live_page_entries(shard) {
        let routing_bucket = entry.address.routing_bucket.unwrap_or_else(|| {
            page_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket)
        });
        let object_id = entry.address.object_id.unwrap_or_else(|| {
            stable_page_object_id(
                shard_id,
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
            )
        });
        let bucket = bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket,
                meta_loaded: true,
                in_memory: true,
                ..BucketNode::default()
            });
        let page_dirty = shard.dirty_objects.contains(&entry.object_key) || entry.dirty;
        bucket.dirty |= page_dirty;
        if page_dirty {
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
        }
        bucket.in_memory |= true;
        bucket.object_index.insert(object_id);
        bucket.page_index.insert(
            page_index_ref_key(&entry),
            PageIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component,
                object_id,
                address: entry.address,
                dirty: page_dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
        );
        update_bucket_layout(bucket);
    }
    // Re-attach the tombstone ids captured above. Keep them in object_index too so the object
    // manager's object_count matches the deserialize/reconcile load path (which never dropped
    // them); a live page entry re-adding the same id is a no-op (BTreeSet).
    for (routing_bucket, deleted) in prior_deleted_object_index {
        let bucket = bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket,
                meta_loaded: true,
                ..BucketNode::default()
            });
        for object_id in &deleted {
            bucket.object_index.insert(*object_id);
        }
        bucket.deleted_object_index.extend(deleted);
    }
    bucket_index.rebuild_object_page_lookup();
    shard.bucket_index = bucket_index;
}

/// Merge a page-derived timestamped-series view against the pre-existing (deserialized /
/// in-memory) model map, which is AUTHORITATIVE for membership. reconcile re-reads packed
/// pages, but a page physically holds timestamps that may have been evicted (feature
/// max_size trim) from the model map, and a page read can transiently fail. So:
///  - a key present in the persisted map keeps EXACTLY its persisted timestamps (no
///    resurrection of evicted points, no loss on a failed page read), refreshing each
///    address from the page-derived view when available;
///  - a key absent from the persisted map is rebuilt from the page (the legitimate
///    rebuild-from-bucket-index case, e.g. a bucket_index entry with no model-map counterpart).
/// This is why `promote` never clears the model maps: they remain the membership source.
fn reconcile_timestamped_series_membership(
    persisted: &HashMap<String, BTreeMap<u64, BlockAddress>>,
    page_derived: HashMap<String, BTreeMap<u64, BlockAddress>>,
) -> HashMap<String, BTreeMap<u64, BlockAddress>> {
    let mut result: HashMap<String, BTreeMap<u64, BlockAddress>> = HashMap::new();
    for (key, page_series) in page_derived {
        match persisted.get(&key) {
            Some(persisted_series) => {
                let merged = persisted_series
                    .iter()
                    .map(|(timestamp_ms, persisted_address)| {
                        let address = page_series
                            .get(timestamp_ms)
                            .cloned()
                            .unwrap_or_else(|| persisted_address.clone());
                        (*timestamp_ms, address)
                    })
                    .collect();
                result.insert(key, merged);
            }
            None => {
                result.insert(key, page_series);
            }
        }
    }
    // Preserve persisted keys entirely absent from the page-derived view (page unreadable or
    // not in bucket_index) so a transient read failure never drops a durable series.
    for (key, persisted_series) in persisted {
        result
            .entry(key.clone())
            .or_insert_with(|| persisted_series.clone());
    }
    result
}

pub(super) fn reconcile_secondary_views_from_bucket_index(
    page_store: &LocalBlockStore,
    shard: &mut ShardState,
    warm: Option<(&MultiLayerCache, ShardId)>,
) {
    if shard.bucket_index.bucket_map.is_empty() {
        return;
    }

    let entries = collect_bucket_index_live_page_entries(shard)
        .into_iter()
        .filter(|entry| !entry.deleted)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        return;
    }

    // Disk->memory promotion accumulator (normal restart). When warming, each page
    // read below also collects (cache_key, bytes) here; a single cache.put_batch()
    // at the end promotes them all under one lock instead of one lock cycle per page.
    let warm_shard = warm.map(|(_, shard_id)| shard_id);
    let mut warm_batch: Vec<(CacheKey, Vec<u8>)> = Vec::new();

    let mut saw_strings = false;
    let mut saw_hashes = false;
    let mut saw_sets = false;
    let mut saw_lists = false;
    let mut saw_zsets = false;
    let mut saw_features = false;
    let mut saw_control_state = false;
    let mut saw_context_events = false;
    let mut saw_context_indexes = false;
    let mut saw_context_audits = false;
    let mut saw_context_entities = false;
    let mut saw_context_children = false;
    let mut saw_context_summaries = false;
    let mut saw_context_compressions = false;

    let mut strings = HashMap::new();
    let mut hashes = HashMap::<String, HashMap<String, BlockAddress>>::new();
    let mut sets = HashMap::<String, BTreeMap<Vec<u8>, BlockAddress>>::new();
    let mut lists = HashMap::<String, BTreeMap<i64, BlockAddress>>::new();
    let mut zsets = HashMap::<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>::new();
    let mut features = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();
    let mut control_state = HashMap::<String, BTreeMap<u64, i64>>::new();
    let mut control_state_pages = HashMap::new();
    let mut context_events = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();
    let mut context_event_timeline = HashMap::<String, BTreeMap<u64, u64>>::new();
    let mut context_indexes = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();
    let mut context_audits = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();
    let mut context_entities = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();
    let mut context_children = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();
    let mut context_summaries = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();
    let mut context_compressions = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();

    for entry in entries {
        match entry.kind.as_str() {
            "string" => {
                saw_strings = true;
                strings.insert(entry.object_key, entry.address);
            }
            "hash" => {
                saw_hashes = true;
                hashes
                    .entry(entry.object_key)
                    .or_default()
                    .insert(entry.component.unwrap_or_default(), entry.address);
            }
            "set" => {
                saw_sets = true;
                let member = entry
                    .component
                    .as_deref()
                    .and_then(|component| hex::decode(component).ok())
                    .unwrap_or_default();
                sets.entry(entry.object_key)
                    .or_default()
                    .insert(member, entry.address);
            }
            "zset" => {
                saw_zsets = true;
                if let Some(component) = entry.component.as_deref() {
                    if component.len() > 16 {
                        if let (Ok(biased), Ok(member)) = (
                            u64::from_str_radix(&component[..16], 16),
                            hex::decode(&component[16..]),
                        ) {
                            zsets
                                .entry(entry.object_key)
                                .or_default()
                                .insert(member, (biased, entry.address));
                        }
                    }
                }
            }
            "list" => {
                saw_lists = true;
                let seq = entry
                    .component
                    .as_deref()
                    .and_then(|component| u64::from_str_radix(component, 16).ok())
                    .map(|biased| biased.wrapping_add(i64::MIN as u64) as i64)
                    .unwrap_or_default();
                lists
                    .entry(entry.object_key)
                    .or_default()
                    .insert(seq, entry.address);
            }
            "feature" => {
                saw_features = true;
                insert_timestamped_secondary_view(
                    page_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut features,
                    entry.object_key,
                    entry.address,
                );
            }
            "sequence" => {
                saw_features = true;
                insert_timestamped_secondary_view(
                    page_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut features,
                    entry.object_key,
                    entry.address,
                );
            }
            "control_state" => {
                saw_control_state = true;
                if let Ok(bytes) = page_store.read(&entry.address) {
                    if let Some(shard_id) = warm_shard {
                        let key = CacheKey::page_with_slot(
                            shard_id,
                            entry.address.page_slab_id,
                            entry.address.offset,
                            entry.address.length,
                            entry.address.routing_bucket,
                        );
                        warm_batch.push((key, bytes.clone()));
                    }
                    if let Ok(series) = serde_json::from_slice::<BTreeMap<u64, i64>>(&bytes) {
                        control_state.insert(entry.object_key.clone(), series);
                    }
                }
                control_state_pages.insert(entry.object_key, entry.address);
            }
            "context_event" => {
                saw_context_events = true;
                insert_context_event_views(
                    page_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_events,
                    &mut context_event_timeline,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_index" => {
                saw_context_indexes = true;
                insert_timestamped_secondary_view(
                    page_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_indexes,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_audit" => {
                saw_context_audits = true;
                insert_timestamped_secondary_view(
                    page_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_audits,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_entity" => {
                saw_context_entities = true;
                if let Some((collection_key, entity_hash)) =
                    split_context_entity_key(&entry.object_key)
                {
                    context_entities
                        .entry(collection_key)
                        .or_insert_with(BTreeMap::new)
                        .insert(entity_hash, entry.address);
                }
            }
            "context_child" => {
                saw_context_children = true;
                insert_timestamped_secondary_view(
                    page_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_children,
                    entry.object_key,
                    entry.address,
                );
            }
            // "context_embedding" entries from pre-retirement indexes fall through to the
            // ignore arm below: the rows they addressed have no readers left.
            "context_summary" => {
                saw_context_summaries = true;
                insert_timestamped_secondary_view(
                    page_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_summaries,
                    entry.object_key,
                    entry.address,
                );
            }
            "context_compression" => {
                saw_context_compressions = true;
                insert_timestamped_secondary_view(
                    page_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_compressions,
                    entry.object_key,
                    entry.address,
                );
            }
            _ => {}
        }
    }

    if saw_strings {
        shard.strings = strings;
    }
    if saw_hashes {
        shard.hashes = hashes;
    }
    if saw_lists {
        shard.lists = lists;
    }
    if saw_zsets {
        shard.zsets = zsets;
    }
    if saw_sets {
        shard.sets = sets;
    }
    if saw_features {
        let persisted = std::mem::take(&mut shard.features);
        shard.features = reconcile_timestamped_series_membership(&persisted, features);
        // The feature numeric view + rollup are derived from the feature series; drop them so
        // they rebuild lazily from the series reconcile just materialized.
        super::control_rollup::feature_clear_all(shard);
    }
    if saw_control_state {
        // The serialized i64 series is authoritative (the page is a copy of it): keep the
        // persisted series where present and use the page-derived series only for keys the
        // persisted map does not have, so a transient page-read failure never drops a durable
        // control-state key.
        let persisted = std::mem::take(&mut shard.control_state);
        let mut merged = control_state;
        merged.extend(persisted);
        shard.control_state = merged;
        shard.control_state_pages = control_state_pages;
        // The rollup ladder is a derived view of control_state; drop it so it rebuilds
        // lazily from the series reconcile just materialized.
        super::control_rollup::clear_all(shard);
    }
    if saw_context_events {
        let persisted = std::mem::take(&mut shard.context_events);
        shard.context_events = reconcile_timestamped_series_membership(&persisted, context_events);
        // The time index is derived state: rebuild it wholesale from what the pages actually
        // carried rather than reconciling it, so it can never reference an event id that
        // membership reconciliation just dropped from the primary map.
        shard.context_event_timeline = context_event_timeline;
        shard.context_event_timeline.retain(|object_key, index| {
            match shard.context_events.get(object_key) {
                None => false,
                Some(series) => {
                    index.retain(|_, event_id_hash| series.contains_key(event_id_hash));
                    !index.is_empty()
                }
            }
        });
    }
    if saw_context_indexes {
        let persisted = std::mem::take(&mut shard.context_indexes);
        shard.context_indexes =
            reconcile_timestamped_series_membership(&persisted, context_indexes);
    }
    if saw_context_audits {
        let persisted = std::mem::take(&mut shard.context_audits);
        shard.context_audits = reconcile_timestamped_series_membership(&persisted, context_audits);
    }
    if saw_context_entities {
        shard.context_entities = context_entities;
    }
    if saw_context_children {
        let persisted = std::mem::take(&mut shard.context_children);
        shard.context_children =
            reconcile_timestamped_series_membership(&persisted, context_children);
    }
    if saw_context_summaries {
        let persisted = std::mem::take(&mut shard.context_summaries);
        shard.context_summaries =
            reconcile_timestamped_series_membership(&persisted, context_summaries);
    }
    if saw_context_compressions {
        let persisted = std::mem::take(&mut shard.context_compressions);
        shard.context_compressions =
            reconcile_timestamped_series_membership(&persisted, context_compressions);
    }

    for bucket in shard.bucket_index.bucket_map.values_mut() {
        update_bucket_layout(bucket);
    }

    // Promote all pages read above into the cache tier in a single batched put (one
    // lock acquire + one eviction drain vs one per page). No-op when not warming.
    if let Some((cache, _)) = warm {
        if !warm_batch.is_empty() {
            let _ = cache.put_batch(warm_batch);
        }
    }
}

pub(super) fn insert_timestamped_secondary_view(
    page_store: &LocalBlockStore,
    warm_shard: Option<ShardId>,
    warm_batch: &mut Vec<(CacheKey, Vec<u8>)>,
    target: &mut HashMap<String, BTreeMap<u64, BlockAddress>>,
    object_key: String,
    address: BlockAddress,
) {
    let bytes = page_store.read(&address).ok();
    // Fold the disk->memory promotion into the load read we already perform here.
    // page_store.read is mutex-serialized, so a separate post-load warm pass would
    // re-read every page under the same lock; collect the bytes we just read for a
    // single batched cache.put_batch() at the end of reconcile (24k individual
    // cache.put lock cycles -> one). The key MUST match the retrieval read path
    // (read_page_bytes) or the entries never get hit.
    if let (Some(shard_id), Some(bytes)) = (warm_shard, bytes.as_ref()) {
        let key = CacheKey::page_with_slot(
            shard_id,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket,
        );
        warm_batch.push((key, bytes.clone()));
    }
    let timestamps = bytes
        .and_then(|bytes| match decode_feature_page_strict(&bytes) {
            PackedFeaturePageDecode::Packed(points) => Some(
                points
                    .into_iter()
                    .map(|point| point.timestamp_ms)
                    .collect::<Vec<_>>(),
            ),
            PackedFeaturePageDecode::Legacy | PackedFeaturePageDecode::Corrupt(_) => None,
        })
        .unwrap_or_default();
    let series = target.entry(object_key).or_default();
    for timestamp_ms in timestamps {
        // A timestamp can physically live in MORE THAN ONE page: overwriting a timestamped point
        // with a new value writes a NEW page (higher, monotonic page_id/generation) while the OLD
        // page still physically contains that timestamp (kept live by its other points, so its
        // bucket-index entry is not removed). Reconstruction visits pages in slab/offset order --
        // NOT write order -- so an unconditional insert let a STALE older page clobber the newer
        // one for a shared timestamp, and the value silently reverted to the old page's bytes on
        // reload. Keep the NEWEST page (highest address generation) per timestamp.
        match series.entry(timestamp_ms) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(address.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                if address.generation.unwrap_or(0) >= slot.get().generation.unwrap_or(0) {
                    slot.insert(address.clone());
                }
            }
        }
    }
}

/// Rebuild the event primary map AND its time index from a physical page.
///
/// Events are keyed by event id hash, which -- unlike a timestamp -- is not recoverable from the
/// packed point header. It lives inside the encoded ContextEvent, so this decodes each point's
/// value rather than reading only its timestamp. The alternative, keying recovered events by
/// timeline key, would rebuild a map the read path can no longer address and silently strand
/// every event after a page-recovery load.
///
/// Newest-page-wins is preserved for the same reason it exists in the timestamped view: one
/// logical record can physically live in several pages, and reconstruction visits pages in
/// slab/offset order, not write order.
#[allow(clippy::too_many_arguments)]
pub(super) fn insert_context_event_views(
    page_store: &LocalBlockStore,
    warm_shard: Option<ShardId>,
    warm_batch: &mut Vec<(CacheKey, Vec<u8>)>,
    events: &mut HashMap<String, BTreeMap<u64, BlockAddress>>,
    timeline: &mut HashMap<String, BTreeMap<u64, u64>>,
    object_key: String,
    address: BlockAddress,
) {
    let bytes = page_store.read(&address).ok();
    if let (Some(shard_id), Some(bytes)) = (warm_shard, bytes.as_ref()) {
        let key = CacheKey::page_with_slot(
            shard_id,
            address.page_slab_id,
            address.offset,
            address.length,
            address.routing_bucket,
        );
        warm_batch.push((key, bytes.clone()));
    }
    let points = bytes
        .and_then(|bytes| match decode_feature_page_strict(&bytes) {
            PackedFeaturePageDecode::Packed(points) => Some(points),
            PackedFeaturePageDecode::Legacy | PackedFeaturePageDecode::Corrupt(_) => None,
        })
        .unwrap_or_default();
    let series = events.entry(object_key.clone()).or_default();
    let index = timeline.entry(object_key).or_default();
    for point in points {
        let Some(event) = super::context::context_from_bytes::<ContextEvent>(&point.value) else {
            continue;
        };
        index.insert(point.timestamp_ms, event.event_id_hash);
        match series.entry(event.event_id_hash) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(address.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                if address.generation.unwrap_or(0) >= slot.get().generation.unwrap_or(0) {
                    slot.insert(address.clone());
                }
            }
        }
    }
}

pub(super) fn expected_live_page_object_id(shard_id: ShardId, entry: &LivePageEntry) -> u64 {
    stable_page_object_id(
        shard_id,
        &entry.kind,
        &entry.object_key,
        entry.component.as_deref(),
    )
}

pub(super) fn validate_bucket_ownership_index(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> StoragePageOwnershipValidation {
    let mut validation = StoragePageOwnershipValidation::default();
    for entry in collect_live_page_entries(shard) {
        let expected_object_id = expected_live_page_object_id(shard_id, &entry);
        let expected_routing_bucket =
            page_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket);
        let expected_page_id = entry.address.page_id;
        let object_mismatch = entry
            .address
            .object_id
            .is_some_and(|actual| actual != expected_object_id);
        let bucket_mismatch = entry
            .address
            .routing_bucket
            .is_some_and(|actual| actual != expected_routing_bucket);
        if entry.address.object_id.is_none() || entry.address.routing_bucket.is_none() {
            validation.missing_owner_page_refs =
                validation.missing_owner_page_refs.saturating_add(1);
        }
        let bucket_page_present = shard
            .bucket_index
            .bucket_map
            .get(&expected_routing_bucket)
            .is_some_and(|bucket| {
                bucket.object_index.contains(&expected_object_id)
                    && bucket.page_index.values().any(|page| {
                        page.address.page_slab_id == entry.address.page_slab_id
                            && page.address.offset == entry.address.offset
                            && page.address.length == entry.address.length
                            && page.address.page_id == expected_page_id
                            && page.model_id == entry.kind
                    })
            });
        if !bucket_page_present {
            validation.missing_owner_page_refs =
                validation.missing_owner_page_refs.saturating_add(1);
        }
        if object_mismatch || bucket_mismatch {
            validation
                .mismatches
                .push(StorageRecoveryPageOwnerMismatch {
                    object_key: entry.object_key,
                    page_slab_id: entry.address.page_slab_id,
                    offset: entry.address.offset,
                    expected_object_id,
                    actual_object_id: entry.address.object_id,
                    expected_routing_bucket,
                    actual_routing_bucket: entry.address.routing_bucket,
                });
        }
    }
    validation
}
