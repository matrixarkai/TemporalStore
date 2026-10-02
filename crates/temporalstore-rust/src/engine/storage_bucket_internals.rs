// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage snapshot/sample reporting + bucket-index maintenance internals, split from engine.rs.
use super::*;
use crate::engine::reports::StageWalkCharges;
use std::sync::Arc;


/// Drift checks run so far, and how many of them found a disagreement.
///
/// Process-wide and monotonic, so a soak can ask whether the maintained tally has EVER been wrong
/// without a report having to be plumbed anywhere. Reset only by the tests that read them.
pub static BLOCK_SLAB_LIVE_RECONCILES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static BLOCK_SLAB_LIVE_DRIFTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The per-slab live tally AS THE WALK SEES IT. This is the DEFINITION; the maintained counter is
/// held to it.
///
/// Deliberately built from `collect_live_block_entries` and not from the model maps: that walk is
/// what every existing per-slab live figure is built from -- the bucket index, plus the model-map
/// supplement for released buckets -- so the two sides of the drift check are the same question
/// asked twice, not two different questions that happen to be close.
pub(super) fn recompute_block_slab_live(shard: &ShardState) -> BTreeMap<u64, SlabLiveTally> {
    let mut tallies: BTreeMap<u64, SlabLiveTally> = BTreeMap::new();
    for entry in collect_live_block_entries(shard) {
        let tally = tallies.entry(entry.address.block_slab_id()).or_default();
        tally.block_refs = tally.block_refs.saturating_add(1);
        tally.bytes = tally.bytes.saturating_add(entry.address.length());
    }
    tallies
}

/// Compare the maintained tally against the walk, CORRECT the maintained one, and REPORT.
///
/// Three things in a fixed order, and the order is the design:
///
///   1. RECOMPUTE. The walk is the definition.
///   2. CORRECT. Recomputation wins. A maintained counter that has drifted is worse than one that
///      was never maintained, because everything downstream believes it; leaving it wrong to
///      preserve the evidence would be preserving evidence in the serving path.
///   3. REPORT. The difference is returned and tallied in `BLOCK_SLAB_LIVE_DRIFTS`. Never an
///      assert: a counting bug must not become an outage.
///
/// Called where the index is rebuilt wholesale -- a load, a manifest install, a bucket-ownership
/// rebuild -- which is where a walk is already being paid for, and on demand by the guard.
pub(super) fn reconcile_block_slab_live(shard: &mut ShardState) -> BlockSlabLiveDriftReport {
    let recomputed = recompute_block_slab_live(shard);
    let was_ready = shard.bucket_index.block_slab_live.is_ready();
    let mut report = BlockSlabLiveDriftReport {
        was_ready,
        ..BlockSlabLiveDriftReport::default()
    };
    let mut slabs: BTreeSet<u64> = recomputed.keys().copied().collect();
    slabs.extend(shard.bucket_index.block_slab_live.iter().map(|(id, _)| id));
    report.slabs_compared = slabs.len() as u64;
    if was_ready {
        let mut worst_bytes = 0_i64;
        for block_slab_id in slabs {
            let maintained = shard.bucket_index.block_slab_live.tally(block_slab_id);
            let walked = recomputed.get(&block_slab_id).copied().unwrap_or_default();
            let refs = maintained.block_refs as i64 - walked.block_refs as i64;
            let bytes = maintained.bytes as i64 - walked.bytes as i64;
            if refs == 0 && bytes == 0 {
                continue;
            }
            report.drifted_slabs = report.drifted_slabs.saturating_add(1);
            report.block_ref_drift = report.block_ref_drift.saturating_add(refs);
            report.byte_drift = report.byte_drift.saturating_add(bytes);
            if bytes.abs() > worst_bytes.abs() || report.worst_block_slab_id.is_none() {
                worst_bytes = bytes;
                report.worst_block_slab_id = Some(block_slab_id);
            }
        }
        BLOCK_SLAB_LIVE_RECONCILES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if !report.is_clean() {
            BLOCK_SLAB_LIVE_DRIFTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    shard.bucket_index.block_slab_live.reset_from(recomputed);
    report
}

/// Seed the maintained tally from the index, without reporting anything.
///
/// For the load and rebuild paths, where there is nothing to have drifted from: the index has just
/// been built or replaced, so a comparison against the walk would compare the walk with itself.
pub(super) fn seed_block_slab_live(shard: &mut ShardState) {
    let recomputed = recompute_block_slab_live(shard);
    shard.bucket_index.block_slab_live.reset_from(recomputed);
}

pub(super) fn storage_slab_integrity_report(
    shard_id: ShardId,
    recovery: &StorageRecoveryReport,
    boundary: &StorageRecoveryBoundaryReport,
) -> StorageSlabIntegrityReport {
    let indexed_block_slab_count = recovery.active_block_slab_ids.len();
    let discovered_block_slab_count = recovery.block_slab_reports.len();
    let live_block_slab_count = recovery.live_block_slab_ids.len();
    let orphan_block_slab_count = boundary.orphan_block_slab_ids.len();
    let stale_block_ref_count = boundary.stale_index_block_refs.len();
    let corrupt_block_slab_count = boundary.corrupt_block_slab_ids.len();
    let unreadable_block_ref_count = recovery.unreadable_block_refs.len();
    let unreadable_block_bytes = boundary.unreadable_block_bytes;
    let owner_mismatch_block_ref_count = boundary.owner_mismatch_block_refs.len();
    let missing_owner_block_ref_count = boundary.missing_owner_block_refs;
    let reclaim_required = orphan_block_slab_count > 0
        || recovery
            .block_slab_live_reports
            .iter()
            .any(|report| report.stale_block_estimate > 0);
    let integrity_ok = stale_block_ref_count == 0
        && corrupt_block_slab_count == 0
        && unreadable_block_ref_count == 0
        && unreadable_block_bytes == 0
        && owner_mismatch_block_ref_count == 0
        && missing_owner_block_ref_count == 0
        && recovery.all_live_blocks_readable;

    StorageSlabIntegrityReport {
        shard_id,
        indexed_block_slab_count,
        discovered_block_slab_count,
        live_block_slab_count,
        orphan_block_slab_count,
        stale_block_ref_count,
        corrupt_block_slab_count,
        unreadable_block_ref_count,
        unreadable_block_bytes,
        owner_mismatch_block_ref_count,
        missing_owner_block_ref_count,
        reclaim_required,
        integrity_ok,
    }
}

/// The slabs worth reclaiming, chosen from the per-slab live/stale tally.
///
/// This took a whole `StorageRecoveryReport` and read one field off it. Taking that field
/// directly is what lets the planner build the tally without the report's whole-store block
/// scan -- see `storage_reclaim_slab_reports`.
pub(super) fn storage_reclaim_candidates_from_slab_reports(
    block_slab_live_reports: &[StorageRecoverySlabLiveReport],
    fully_stale_slab_ids: &BTreeSet<u64>,
) -> Vec<StorageReclaimCandidate> {
    let mut candidates = block_slab_live_reports
        .iter()
        .filter_map(|report| {
            let fully_stale = fully_stale_slab_ids.contains(&report.block_slab_id);
            let stale_block_estimate = if fully_stale {
                report.block_count
            } else {
                report.stale_block_estimate
            };
            let stale_physical_bytes = if fully_stale {
                report.physical_bytes
            } else {
                report
                    .physical_bytes
                    .saturating_sub(report.live_physical_bytes)
            };
            if stale_block_estimate == 0 && stale_physical_bytes == 0 {
                return None;
            }
            let reclaim_score = stale_physical_bytes
                .saturating_mul(10_000_u64.saturating_sub(report.live_ref_density_basis_points))
                .saturating_div(10_000)
                .saturating_add(stale_block_estimate);
            Some(StorageReclaimCandidate {
                block_slab_id: report.block_slab_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: report.live_physical_bytes,
                stale_physical_bytes,
                block_count: report.block_count,
                live_block_refs: report.live_block_refs,
                stale_block_estimate,
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
            .then_with(|| left.block_slab_id.cmp(&right.block_slab_id))
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
            .block_bytes_reclaimed
            .max(stage.cache_disk_bytes_removed)
            .max(stage.before_bytes.saturating_sub(stage.after_bytes));
        stage.blocks_compacted = stage.rewritten_block_refs;
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
pub(super) struct LiveBlockEntry {
    pub(super) object_key: Arc<str>,
    pub(super) kind: StoredModelKind,
    pub(super) component: Option<Arc<str>>,
    pub(super) address: BlockAddress,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
    pub(super) log_backed: bool,
    /// THE BUCKET THIS BLOCK IS ACTUALLY FILED UNDER, when the walk that produced the entry knew
    /// it -- which is whenever the entry came out of the bucket index.
    ///
    /// An address carries no routing bucket of its own: the wire slot `rs` is retired from
    /// `BlockAddressWire` entirely, so every index decodes into blocks that carry none -- one
    /// written before the field existed, one written while it did, and one written now all alike
    /// -- and `rebuild_bucket_first_index` stamps only the
    /// object id onto an address, so a block comes out of a reconstruct still unrouted. Five
    /// readers then had to answer "which bucket is this block in?" for themselves, and each
    /// answered it with `block_routing_bucket(key, 0, u32::MAX)` -- a hash over the WHOLE range,
    /// which is the bucket the block is filed under only when the shard is loaded on the whole
    /// range too. On a shard loaded `0..1023` it names a bucket the shard does not hold.
    ///
    /// `collect_bucket_index_live_block_entries` walks `bucket_map` and therefore HAS the answer;
    /// it was iterating `.values()` and discarding the key. Carrying it costs NOTHING -- see the
    /// note on the two-field shape below -- and removes the guess. An unset `filing_is_known`
    /// means the entry came from the model maps, where there is no filing to report; those
    /// callers keep the fallback they had.
    ///
    /// NOT the same question as `address.routing_bucket()`, which is what the BLOCK claims. This
    /// is where the INDEX has it. `validate_bucket_ownership_index_from_entries` exists to
    /// report when those two disagree, so the two must stay separately answerable.
    ///
    /// A BARE `u32` AND A FLAG RATHER THAN AN `Option<u32>`, AND THE DIFFERENCE IS 8 BYTES PER
    /// LIVE BLOCK. This struct aligns to 8 -- three `Arc` pointers and a `BlockAddress` -- so its
    /// three flags sat in three bytes with five of tail padding. A `u32` and a fourth flag fit
    /// that padding; an `Option<u32>` is eight bytes of its own and took the struct from 104 to
    /// 112. `a_live_page_entry_carries_pointers_not_text_and_the_hoist_lowered_the_peak` caught
    /// that, and it is the guard on a walk that materializes EVERY live block in the shard, so the
    /// right answer was to stop paying the eight bytes rather than to widen the bound.
    ///
    /// Read through [`LiveBlockEntry::filed_bucket`], never as the two fields.
    pub(super) filed_routing_bucket: u32,
    pub(super) filing_is_known: bool,
}

impl LiveBlockEntry {
    /// The bucket the INDEX has this block under, when the walk that produced the entry knew it.
    pub(super) fn filed_bucket(&self) -> Option<u32> {
        self.filing_is_known.then_some(self.filed_routing_bucket)
    }
}

#[derive(Debug, Default)]
pub(super) struct StorageBlockOwnershipValidation {
    pub(super) mismatches: Vec<StorageRecoveryBlockOwnerMismatch>,
    pub(super) missing_owner_block_refs: usize,
}

pub(super) fn live_block_entry(
    object_key: impl Into<String>,
    kind: impl AsRef<str>,
    component: Option<String>,
    address: BlockAddress,
) -> LiveBlockEntry {
    LiveBlockEntry {
        object_key: Arc::from(object_key.into()),
        kind: stored_model_kind(kind.as_ref()),
        component: component.map(Arc::from),
        // A block materialized in the block store carries a real page_id; a block
        // backed only by the hot/append-log buffer does not. Evaluate before the
        // `address` field moves it.
        log_backed: address.block_id().is_none(),
        address,
        dirty: false,
        deleted: false,
        // A model-map walk reads the blocks an object owns, not the index that files them, so
        // this walk genuinely does not know. Says so rather than guessing.
        filed_routing_bucket: 0,
        filing_is_known: false,
    }
}

/// The same entry from a walk that DOES know the filing.
///
/// A bucket-scoped model-map walk selected this block BY its bucket, so it knows the answer as
/// exactly as the bucket-index walk does -- and an entry that reported "not filed" would send the
/// five `filed_bucket()` readers to a hash over the WHOLE keyspace, which names a bucket a shard
/// loaded on a narrow range does not hold. That fallback used to be unreachable because the address
/// answered first; it is reachable now, so the walks that know have to say so.
pub(super) fn live_block_entry_filed(
    object_key: impl Into<String>,
    kind: impl AsRef<str>,
    component: Option<String>,
    address: BlockAddress,
    routing_bucket: u32,
) -> LiveBlockEntry {
    let mut entry = live_block_entry(object_key, kind, component, address);
    entry.filed_routing_bucket = routing_bucket;
    entry.filing_is_known = true;
    entry
}

pub(super) fn storage_page_address_sample(
    shard_id: ShardId,
    address: &BlockAddress,
) -> StoragePageAddressSample {
    StoragePageAddressSample {
        shard_id,
        stored_slab_id: address.block_slab_id(),
        slab_id: address.block_slab_id(),
        block_id: address.block_id().unwrap_or(address.block_slab_id()),
        offset: address.offset(),
        length: address.length(),
        generation: address.generation().unwrap_or(0),
    }
}

pub(super) fn storage_block_address_sample(
    shard_id: ShardId,
    address: &BlockAddress,
) -> StorageBlockAddressSample {
    StorageBlockAddressSample {
        shard_id,
        stored_slab_id: address.block_slab_id(),
        block_id: address.block_slab_id(),
        offset: address.offset(),
        length: address.length(),
        // Not carried in the index any more; the block envelope holds it.
        checksum: String::new(),
    }
}

pub(super) fn storage_index_snapshot_with_samples(
    shard_id: ShardId,
    shard: &ShardState,
    snapshot: StorageIndexSnapshot,
) -> StorageIndexSnapshot {
    storage_index_snapshot_with_samples_from_entries(
        shard_id,
        &collect_live_block_entries(shard),
        snapshot,
    )
}

/// The same samples, from live-block entries the caller ALREADY has.
///
/// All four `*_snapshot_with_samples` builders run back to back inside ONE read lock in
/// `apply_storage_lifecycle`, and each was calling `collect_live_block_entries` for its own copy of
/// every live block -- then sorting all of it to take EIGHT samples. `who_walks_the_shard` measured
/// the four at 1.0x the shard apiece.
///
/// They differ only in sort order, so one walk serves all four and each sorts a vector of
/// REFERENCES rather than owning entries -- the shared walk is not traded for four clones.
///
/// Safe for the same reason as #1586: one lock, an unchanged `&ShardState`, nothing mutating
/// between them. And these feed report SAMPLES rather than a decision, so unlike the freshness
/// walk in `clear_dumped_bucket_dirty_state` (#1607) a shared snapshot cannot change what the
/// system DOES -- it only makes the four samples describe one moment instead of four.
pub(super) fn storage_index_snapshot_with_samples_from_entries(
    shard_id: ShardId,
    entries: &[LiveBlockEntry],
    mut snapshot: StorageIndexSnapshot,
) -> StorageIndexSnapshot {
    let mut entries: Vec<&LiveBlockEntry> = entries.iter().collect();
    entries.sort_by(|left, right| {
        (
            left.kind.as_str(),
            left.object_key.as_ref(),
            left.component.as_deref().unwrap_or(""),
            left.address.block_slab_id(),
            left.address.offset(),
        )
            .cmp(&(
                right.kind.as_str(),
                right.object_key.as_ref(),
                right.component.as_deref().unwrap_or(""),
                right.address.block_slab_id(),
                right.address.offset(),
            ))
    });

    const MAX_STORAGE_INDEX_SAMPLES: usize = 8;
    snapshot.page_index_entry_samples = entries
        .iter()
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .map(|entry| {
            let page_address = storage_page_address_sample(shard_id, &entry.address);
            StoragePageIndexEntrySample {
                logical_key: entry.object_key.clone().to_string(),
                timestamp_range: None,
                block_addresses: vec![page_address],
                append_watermark: entry.address.offset(),
                generation: expected_live_block_object_id(shard_id, entry),
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
                stored_slab_id: entry
                    .address
                    .slab_id()
                    .unwrap_or(entry.address.block_slab_id()),
                checksum: String::new(),
                generation: expected_live_block_object_id(shard_id, entry),
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
            entry.kind.to_string(),
            entry.kind.to_string(),
            entry.object_key.to_string(),
        );
        let sample = object_entries
            .entry(key)
            .or_insert_with(|| StorageObjectIndexEntrySample {
                model: entry.kind.to_string(),
                table: entry.kind.to_string(),
                object_key: entry.object_key.to_string(),
                block_chain: Vec::new(),
                delete_marker: entry.deleted,
                generation: expected_live_block_object_id(shard_id, entry),
            });
        if sample.block_chain.len() < MAX_STORAGE_INDEX_SAMPLES {
            sample
                .block_chain
                .push(storage_page_address_sample(shard_id, &entry.address));
        }
        sample.delete_marker |= entry.deleted;
        sample.generation = sample.generation.max(expected_live_block_object_id(shard_id, entry));
    }
    snapshot.object_index_entry_samples = object_entries
        .into_iter()
        .map(|(_, sample)| sample)
        .take(MAX_STORAGE_INDEX_SAMPLES)
        .collect();
    snapshot
}

pub(super) fn storage_gc_ref(entry: &LiveBlockEntry) -> String {
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
    snapshot: StorageWatermarkSnapshot,
) -> StorageWatermarkSnapshot {
    storage_watermark_snapshot_with_samples_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        snapshot,
    )
}

/// The same samples, from live-block entries the caller ALREADY has. See
/// `storage_index_snapshot_with_samples_from_entries` for why sharing one walk across the four
/// sampling builders is safe.
///
/// This one is shaped differently from the other three and the difference is worth keeping in
/// view: it does NOT sort. It folds every entry into a per-bucket watermark map, so it reads the
/// entries once in whatever order they arrive. Anything that rewrites these four as a group has to
/// notice that -- treating them as one template is how a bulk edit of this set goes wrong.
pub(super) fn storage_watermark_snapshot_with_samples_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    mut snapshot: StorageWatermarkSnapshot,
) -> StorageWatermarkSnapshot {
    const MAX_STORAGE_WATERMARK_SAMPLES: usize = 8;
    let timestamp_ms = now_ms();
    let mut bucket_watermarks = BTreeMap::<u32, u64>::new();

    for (bucket_id, runtime_bucket) in &shard.bucket_index.bucket_map {
        bucket_watermarks.insert(*bucket_id, runtime_bucket.dirty_generation);
    }
    for entry in entries {
        let bucket_id = entry
            .filed_bucket()
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        let generation = expected_live_block_object_id(shard_id, entry);
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
    shard_id: ShardId,
    shard: &ShardState,
    snapshot: StorageGcSnapshot,
) -> StorageGcSnapshot {
    storage_gc_snapshot_with_samples_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        snapshot,
    )
}

/// The same samples, from live-block entries the caller ALREADY has. See
/// `storage_index_snapshot_with_samples_from_entries` for why sharing one walk across the four
/// sampling builders is safe: one lock, an unchanged `&ShardState`, and these feed report SAMPLES
/// rather than a decision.
pub(super) fn storage_gc_snapshot_with_samples_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    mut snapshot: StorageGcSnapshot,
) -> StorageGcSnapshot {
    let mut entries: Vec<&LiveBlockEntry> = entries.iter().collect();
    entries.sort_by(|left, right| {
        (
            left.deleted,
            left.kind.as_str(),
            left.object_key.as_ref(),
            left.component.as_deref().unwrap_or(""),
            left.address.block_slab_id(),
            left.address.offset(),
        )
            .cmp(&(
                right.deleted,
                right.kind.as_str(),
                right.object_key.as_ref(),
                right.component.as_deref().unwrap_or(""),
                right.address.block_slab_id(),
                right.address.offset(),
            ))
    });

    const MAX_STORAGE_GC_SAMPLES: usize = 8;
    let now = now_ms();
    snapshot.delete_marker_samples = entries
        .iter()
        .filter(|entry| entry.deleted)
        .take(MAX_STORAGE_GC_SAMPLES)
        .map(|entry| StorageDeleteMarkerSample {
            ref_id: storage_gc_ref(entry),
            generation: expected_live_block_object_id(shard_id, entry),
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
                .get(entry.object_key.as_ref())
                .copied()
                .unwrap_or(0);
            let has_delete_marker = entry.deleted;
            let ttl_eligible = eligible_after_ms > 0 && eligible_after_ms <= now;
            if !has_delete_marker && !ttl_eligible {
                return None;
            }
            Some(StorageGcEligibilitySample {
                ref_id: storage_gc_ref(entry),
                eligible_after_ms,
                has_delete_marker,
                follower_safe,
                reclaimable_bytes: if follower_safe {
                    entry.address.length()
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
            has_delete_marker: snapshot.delete_marker_records > 0,
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
    snapshot: StorageTopologySnapshot,
) -> StorageTopologySnapshot {
    storage_topology_snapshot_with_samples_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        snapshot,
    )
}

/// The same samples, from live-block entries the caller ALREADY has. See
/// `storage_index_snapshot_with_samples_from_entries` for why sharing one walk across the four
/// sampling builders is safe: one lock, an unchanged `&ShardState`, and these feed report SAMPLES
/// rather than a decision.
pub(super) fn storage_topology_snapshot_with_samples_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    mut snapshot: StorageTopologySnapshot,
) -> StorageTopologySnapshot {
    let mut entries: Vec<&LiveBlockEntry> = entries.iter().collect();
    entries.sort_by(|left, right| {
        (
            left.address
                .slab_id()
                .unwrap_or(left.address.block_slab_id()),
            left.address.block_slab_id(),
            left.address.offset(),
            left.kind.as_str(),
            left.object_key.as_ref(),
        )
            .cmp(&(
                right
                    .address
                    .slab_id()
                    .unwrap_or(right.address.block_slab_id()),
                right.address.block_slab_id(),
                right.address.offset(),
                right.kind.as_str(),
                right.object_key.as_ref(),
            ))
    });

    const MAX_STORAGE_TOPOLOGY_SAMPLES: usize = 8;
    #[derive(Default)]
    struct SlabUsageAcc {
        used_bytes: u64,
        stale_bytes: u64,
        slabs: BTreeSet<u64>,
        generation: u64,
    }
    #[derive(Default)]
    struct SlabAcc {
        stored_slab_id: u64,
        start_offset: u64,
        generation: u64,
        deleted_refs: u64,
        live_refs: u64,
    }
    #[derive(Default)]
    struct SlabAccumulator {
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
        block_refs: Vec<StoragePageAddressSample>,
        delete_markers: BTreeSet<String>,
    }

    let mut slabs_usage = BTreeMap::<u64, SlabUsageAcc>::new();
    let mut slabs = BTreeMap::<u64, SlabAcc>::new();
    let mut slab_ranges = BTreeMap::<u64, SlabAccumulator>::new();
    let mut buckets = BTreeMap::<u32, BucketAcc>::new();

    for entry in &entries {
        let stored_slab_id = entry
            .address
            .slab_id()
            .unwrap_or(entry.address.block_slab_id());
        let slab_id = entry.address.block_slab_id();
        let generation = expected_live_block_object_id(shard_id, entry);
        let usage = slabs_usage.entry(stored_slab_id).or_default();
        usage.slabs.insert(slab_id);
        usage.generation = usage.generation.max(generation);
        if entry.deleted {
            usage.stale_bytes = usage.stale_bytes.saturating_add(entry.address.length());
        } else {
            usage.used_bytes = usage.used_bytes.saturating_add(entry.address.length());
        }

        let slab = slabs.entry(slab_id).or_insert_with(|| SlabAcc {
            stored_slab_id,
            start_offset: entry.address.offset(),
            ..SlabAcc::default()
        });
        slab.start_offset = slab.start_offset.min(entry.address.offset());
        slab.generation = slab.generation.max(generation);
        if entry.deleted {
            slab.deleted_refs = slab.deleted_refs.saturating_add(1);
        } else {
            slab.live_refs = slab.live_refs.saturating_add(1);
        }

        let range = slab_ranges
            .entry(stored_slab_id)
            .or_insert_with(|| SlabAccumulator {
                min_offset: entry.address.offset(),
                max_offset: entry.address.offset().saturating_add(entry.address.length()),
                ..SlabAccumulator::default()
            });
        range.min_offset = range.min_offset.min(entry.address.offset());
        range.max_offset = range
            .max_offset
            .max(entry.address.offset().saturating_add(entry.address.length()));
        range.generation = range.generation.max(generation);
        if entry.deleted {
            range.deleted_refs = range.deleted_refs.saturating_add(1);
        } else {
            range.live_refs = range.live_refs.saturating_add(1);
        }

        let bucket_id = entry
            .filed_bucket()
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        let bucket = buckets.entry(bucket_id).or_default();
        bucket.dirty_generation = bucket.dirty_generation.max(generation);
        bucket.object_refs.insert(generation);
        if bucket.block_refs.len() < MAX_STORAGE_TOPOLOGY_SAMPLES {
            bucket.block_refs
                .push(storage_page_address_sample(shard_id, &entry.address));
        }
        if entry.deleted {
            bucket.delete_markers.insert(storage_gc_ref(entry));
        }
    }

    for (bucket_id, runtime_bucket) in &shard.bucket_index.bucket_map {
        let bucket = buckets.entry(*bucket_id).or_default();
        bucket.dirty_generation = bucket.dirty_generation.max(runtime_bucket.dirty_generation);
        bucket.object_refs
            .extend(runtime_bucket.object_index.iter().copied());
        for page in runtime_bucket.block_index.values() {
            if bucket.block_refs.len() >= MAX_STORAGE_TOPOLOGY_SAMPLES {
                break;
            }
            bucket.block_refs
                .push(storage_page_address_sample(shard_id, &page.address));
            if page.deleted {
                bucket.delete_markers
                    .insert(format!("{}:{}", page.model_id, page.object_key));
            }
        }
    }

    snapshot.storage_slab_usage_samples = slabs_usage
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(stored_slab_id, usage)| StorageSlabUsageSample {
            stored_slab_id,
            total_bytes: usage.used_bytes.saturating_add(usage.stale_bytes),
            used_bytes: usage.used_bytes,
            stale_bytes: usage.stale_bytes,
            slabs: usage.slabs.into_iter().collect(),
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
            stored_slab_id: slab.stored_slab_id,
            start_offset: slab.start_offset,
            sealed: slab.live_refs == 0 || slab.deleted_refs > 0,
            generation: slab.generation,
        })
        .collect();
    snapshot.slab_range_samples = slab_ranges
        .into_iter()
        .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
        .map(|(stored_slab_id, range)| StorageSlabSlabSample {
            stored_slab_id,
            block_range: vec![range.min_offset, range.max_offset],
            reclaim_state: if range.deleted_refs > 0 && range.live_refs == 0 {
                "reclaimable".to_string()
            } else if range.deleted_refs > 0 {
                "mixed_live_stale".to_string()
            } else {
                "live".to_string()
            },
            generation: range.generation,
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
            block_refs: bucket
                .block_refs
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            delete_markers: bucket
                .delete_markers
                .into_iter()
                .take(MAX_STORAGE_TOPOLOGY_SAMPLES)
                .collect(),
            owner_mismatch_count: 0,
        })
        .collect();
    snapshot
}

/// Running total of live-block entries materialized by [`collect_live_block_entries`].
///
/// This walk is `O(live pages)` and clones two strings per entry, and several callers run it on
/// a background loop, so its cost is easy to introduce and hard to notice. The counter makes it
/// measurable: a test can assert that a code path's scan volume does not grow with the store.
static LIVE_BLOCK_SCAN_ENTRIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Live-block entries materialized since the last reset.
pub fn live_block_scan_entries() -> u64 {
    LIVE_BLOCK_SCAN_ENTRIES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the scan counter. For tests measuring one operation's scan volume.
pub fn reset_live_block_scan_entries() {
    LIVE_BLOCK_SCAN_ENTRIES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Live-block entries MATERIALIZED by the two bucket-scoped model-map walks, across every call.
///
/// [`release_bucket_blocks`] and [`reload_released_bucket`] each ask one question of the model
/// maps about a NAMED set of buckets. Nothing indexes the model maps by routing bucket, so both
/// have to walk them to ask it -- but neither has to build an owned entry for every live block in
/// the store on the way past, and until this counter existed nothing said which they did.
///
/// Distinct from [`LIVE_BLOCK_SCAN_ENTRIES`], which counts only what the wrapper
/// `collect_live_block_entries` materializes. Both of these paths call
/// `collect_model_live_block_entries` DIRECTLY and are invisible to that counter, which is why a
/// round whose release allocated four times per object in the store could report zero entries
/// scanned.
///
/// COUNTED, not timed, and independent of the counting allocator: a guard can assert this number
/// tracks the victims without `alloc-probe` being on. Process-wide, so a reader must reset it
/// immediately before the call it is measuring, and the suite it is read in runs single-threaded.
static BUCKET_SCOPED_MODEL_ENTRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Entries materialized by the bucket-scoped model-map walks since the last reset.
pub fn bucket_scoped_model_entries() -> u64 {
    BUCKET_SCOPED_MODEL_ENTRIES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the bucket-scoped walk counter. Pairs with [`bucket_scoped_model_entries`].
pub fn reset_bucket_scoped_model_entries() {
    BUCKET_SCOPED_MODEL_ENTRIES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Model-map addresses a walk LOOKED AT, across every tally.
///
/// [`BUCKET_SCOPED_MODEL_ENTRIES`] counts what a bucket-scoped walk EMITS, and that is bounded by
/// the buckets it was asked about. The pass underneath it is not: the maps are keyed by object
/// and the routing bucket is a field of the ADDRESS, so `accept` runs on every live address in
/// the shard before any of them is filtered away. A release of four buckets and a release of four
/// buckets on a store ten times the size therefore report the same emitted count while doing ten
/// times the work, and until this counter existed nothing in the tree could tell them apart.
///
/// Charged once per walk from a local tally, so the walk pays one atomic and not one per address.
/// Process-wide like its neighbours: reset immediately before the call being measured, and read
/// it in a single-threaded run.
static MODEL_MAP_ADDRESSES_VISITED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Model-map addresses visited since the last reset.
pub fn model_map_addresses_visited() -> u64 {
    MODEL_MAP_ADDRESSES_VISITED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the model-map visit counter. Pairs with [`model_map_addresses_visited`].
pub fn reset_model_map_addresses_visited() {
    MODEL_MAP_ADDRESSES_VISITED.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Times [`release_bucket_blocks`] actually ran its whole-store derivation.
///
/// [`MODEL_MAP_ADDRESSES_VISITED`] says how big the pass was; this says whether it happened. The
/// two are different questions and only one of them survives a store of any particular size: a
/// release that never reaches the model-map comparison visits zero addresses, and zero is also
/// what a counter wired to nothing reads. Counting the derivations separately means a guard can
/// assert the pass did NOT run without that assertion being satisfied by the counter being dead.
///
/// At most one per `release_bucket_blocks` call by construction -- the derivation covers the whole
/// candidate set in one pass and is memoized for the rest of the batch -- so the identity a guard
/// pins is `derivations == 1 if any candidate reached the comparison else 0`, whatever the batch
/// size and whatever the store size.
static BUCKET_RELEASE_MODEL_DERIVATIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Whole-store derivations run by the bucket release since the last reset.
pub fn bucket_release_model_derivations() -> u64 {
    BUCKET_RELEASE_MODEL_DERIVATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the release-derivation counter. Pairs with [`bucket_release_model_derivations`].
pub fn reset_bucket_release_model_derivations() {
    BUCKET_RELEASE_MODEL_DERIVATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Bucket-index entries visited by [`bucket_index_resident_bytes`]: one per node plus one per
/// resident block.
///
/// Deliberately its own counter rather than a site on [`BUCKET_BLOCK_INDEX_VISITS`]. That total
/// is asserted on by existing guards around maintenance rounds, and folding a new walk into it
/// would move their numbers without any of them being about this walk. What this one is for is
/// the eviction round, which reads resident bytes twice -- once before its gate and once after
/// its actuator -- and pays the whole bucket index each time.
static BUCKET_INDEX_RESIDENT_BYTES_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Bucket-index entries visited by the resident-bytes walk since the last reset.
pub fn bucket_index_resident_bytes_visits() -> u64 {
    BUCKET_INDEX_RESIDENT_BYTES_VISITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the resident-bytes walk counter. Pairs with [`bucket_index_resident_bytes_visits`].
pub fn reset_bucket_index_resident_bytes_visits() {
    BUCKET_INDEX_RESIDENT_BYTES_VISITS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Running total of bucket `page_index` entries visited by the bucket-maintenance walks.
///
/// Distinct from [`LIVE_BLOCK_SCAN_ENTRIES`], which counts materialized live-block entries. This
/// one counts the cheaper-looking `bucket.page_index.values()` passes -- `update_bucket_layout`
/// and the per-object dirty-state clear. Each is `O(pages in the bucket)` and they run inside
/// loops over buckets, so their cost is a product, not a sum, and does not show up in any single
/// obvious place.
static BUCKET_BLOCK_INDEX_VISITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Bucket `page_index` entries visited since the last reset.
pub fn bucket_block_index_visits() -> u64 {
    BUCKET_BLOCK_INDEX_VISITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the bucket-visit counter. For tests measuring one operation's maintenance volume.
pub fn reset_bucket_block_index_visits() {
    BUCKET_BLOCK_INDEX_VISITS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// The same two walks, accumulated for the STAGE of a maintenance round that is running.
///
/// Separate atomics rather than a second read of the two above, because the two questions have
/// different spans. [`LIVE_BLOCK_SCAN_ENTRIES`] answers "how much did this whole call walk", and
/// a reader resets it once around the call. These answer "how much did THIS STAGE walk", and are
/// read-and-cleared at every stage boundary -- the same discipline the round's `stage_clock`
/// already uses for `duration_ms`, so the stage rows tile the round rather than overlapping it.
///
/// KEEPING THEM SEPARATE IS THE POINT, not an accident of implementation. A residual computed as
/// "the whole call minus the stages" is only a reading if the two sides are measured by different
/// instruments; if the stage rows were slices of the same counter the subtraction would be an
/// identity, and an identity cannot notice a stage boundary that has drifted or a walk made
/// outside every stage. Both are charged by the same primitives below, and neither is derived
/// from the other.
static STAGE_LIVE_BLOCK_SCAN_ENTRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static STAGE_BUCKET_BLOCK_INDEX_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Served-index encodes charged to the stage that is running, and their bytes.
///
/// A WHOLE-STORE COST NEITHER WALK COUNTER CAN SEE. `serialize_index` encodes the entire served
/// index for the shard; its cost is the store, and it materialises no live-block entry and visits
/// no bucket `page_index`, so both counters above read zero across it. It is charged here so the
/// round's own rows account for it -- and so the part of the round that does it OUTSIDE every
/// stage shows up as a residual instead of as nothing.
static STAGE_INDEX_ENCODES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static STAGE_INDEX_ENCODE_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Charge one served-index encode to the running stage. Called by `note_index_encode`.
pub(super) fn note_stage_index_encode(bytes: usize) {
    STAGE_INDEX_ENCODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    STAGE_INDEX_ENCODE_BYTES.fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Read the current stage's walk charges and clear them for the next stage.
///
/// Read-and-clear in one call, so a stage cannot be charged twice and the next stage cannot
/// inherit this one's total.
pub fn take_stage_walk_charges() -> StageWalkCharges {
    StageWalkCharges {
        live_block_entries: STAGE_LIVE_BLOCK_SCAN_ENTRIES.swap(0, std::sync::atomic::Ordering::Relaxed),
        bucket_block_index_visits: STAGE_BUCKET_BLOCK_INDEX_VISITS
            .swap(0, std::sync::atomic::Ordering::Relaxed),
        index_encodes: STAGE_INDEX_ENCODES.swap(0, std::sync::atomic::Ordering::Relaxed),
        index_encode_bytes: STAGE_INDEX_ENCODE_BYTES
            .swap(0, std::sync::atomic::Ordering::Relaxed),
    }
}

/// Clear both stage accumulators without reporting them, so the first stage of a round is not
/// charged for whatever ran before the round started.
pub fn reset_stage_walk_charges() {
    STAGE_LIVE_BLOCK_SCAN_ENTRIES.store(0, std::sync::atomic::Ordering::Relaxed);
    STAGE_BUCKET_BLOCK_INDEX_VISITS.store(0, std::sync::atomic::Ordering::Relaxed);
    STAGE_INDEX_ENCODES.store(0, std::sync::atomic::Ordering::Relaxed);
    STAGE_INDEX_ENCODE_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Charge both the whole-call and the per-stage live-block counters by `count`.
///
/// Tests only, and it exists for one job: planting a known quantity into the residual instrument
/// so a test can prove the subtraction recovers it exactly, rather than trusting that a zero
/// means nothing was missed.
#[cfg(test)]
pub fn plant_live_block_scan_entries_for_test(count: u64) {
    LIVE_BLOCK_SCAN_ENTRIES.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    STAGE_LIVE_BLOCK_SCAN_ENTRIES.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
}

/// Which tally a model-map walk is charged to.
///
/// Named by the caller, applied by [`visit_model_live_blocks`] itself. The charge happens where
/// the blocks are EMITTED, not at the call site, because a call site that counts is a call site
/// the next caller forgets: [`LIVE_BLOCK_SCAN_ENTRIES`] was charged in exactly one place --
/// `collect_live_block_entries` -- while seven production call sites reached the same two walks
/// directly and were charged nothing at all.
///
/// There is deliberately NO uncounted variant. Every way of walking the model maps names one of
/// these three, so a walk added later cannot compile without saying where it is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ModelWalkTally {
    /// Materializing every live block in the shard: [`LIVE_BLOCK_SCAN_ENTRIES`].
    WholeShardEntries,
    /// Materializing the blocks of NAMED routing buckets: [`BUCKET_SCOPED_MODEL_ENTRIES`].
    BucketScopedEntries,
    /// The promotion check's borrow-only pass: `PROMOTE_MODEL_MAP_PAGES`.
    PromotionCheckPages,
}

/// Charge live-block entries to the scan counter AND to the caller that asked for them.
///
/// `#[track_caller]` all the way down from the public walks, so a charge moved inward still
/// attributes to the same source line it did when the wrapper counted.
#[track_caller]
fn note_live_block_scan(count: usize) {
    LIVE_BLOCK_SCAN_ENTRIES.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    STAGE_LIVE_BLOCK_SCAN_ENTRIES.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    let caller = std::panic::Location::caller();
    if let Ok(mut sites) = live_block_scan_sites().lock() {
        *sites
            .entry(format!("{}:{}", caller.file(), caller.line()))
            .or_insert(0) += count as u64;
    }
}

fn note_bucket_block_visits(count: usize) {
    BUCKET_BLOCK_INDEX_VISITS.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    STAGE_BUCKET_BLOCK_INDEX_VISITS.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Per-site attribution for [`BUCKET_BLOCK_INDEX_VISITS`], so a scaling result names the walk that
/// caused it rather than leaving it to be inferred from arithmetic.
pub mod bucket_visit_sites {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub(super) static LAYOUT: AtomicU64 = AtomicU64::new(0);
    pub(super) static CLEAR_DIRTY: AtomicU64 = AtomicU64::new(0);
    pub(super) static REFRESH_FLAGS: AtomicU64 = AtomicU64::new(0);
    pub(super) static REMOVE_ALL_BUCKETS: AtomicU64 = AtomicU64::new(0);

    /// `(layout, clear_dirty, refresh_flags, remove_all_buckets)` visits since the last reset.
    pub fn snapshot() -> (u64, u64, u64, u64) {
        (
            LAYOUT.load(Ordering::Relaxed),
            CLEAR_DIRTY.load(Ordering::Relaxed),
            REFRESH_FLAGS.load(Ordering::Relaxed),
            REMOVE_ALL_BUCKETS.load(Ordering::Relaxed),
        )
    }

    pub fn reset() {
        for counter in [&LAYOUT, &CLEAR_DIRTY, &REFRESH_FLAGS, &REMOVE_ALL_BUCKETS] {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

fn note_site(site: &std::sync::atomic::AtomicU64, count: usize) {
    site.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
    note_bucket_block_visits(count);
}

/// Per-CALLER attribution for [`LIVE_BLOCK_SCAN_ENTRIES`], keyed by the source location that asked.
///
/// The total alone says a round walks the shard N times; it does not say WHO. Attributing it by
/// hand hit a wall: `apply_storage_lifecycle` measures 10.0x while every one of its callees sums
/// to 6.0x, and the remaining four walks are inside the body where no probe row can reach them.
/// Two earlier attributions failed the same way and were only closed by finding a call that no
/// grep had matched -- once a bare expression at the end of a function.
///
/// `#[track_caller]` gives the answer with NO call-site changes, which matters because this
/// function has about twenty of them and a threaded-through label would have to be right at every
/// one to be trustworthy. The location is resolved at compile time; the cost here is one map
/// update per CALL, on a path that is already walking every live block in the shard.
fn live_block_scan_sites() -> &'static std::sync::Mutex<std::collections::BTreeMap<String, u64>> {
    static SITES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::BTreeMap<String, u64>>,
    > = std::sync::OnceLock::new();
    SITES.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

/// Entries materialized per calling site since the last reset, as `file:line -> entries`.
pub fn live_block_scan_sites_snapshot() -> std::collections::BTreeMap<String, u64> {
    live_block_scan_sites()
        .lock()
        .map(|sites| sites.clone())
        .unwrap_or_default()
}

/// Clear the per-site tallies. Pairs with [`reset_live_block_scan_entries`].
pub fn reset_live_block_scan_sites() {
    if let Ok(mut sites) = live_block_scan_sites().lock() {
        sites.clear();
    }
}

/// Both arms charge themselves, so this wrapper is no longer the only counted way in. It stays
/// `#[track_caller]` so a charge made further down still attributes to ITS caller.
#[track_caller]
pub(super) fn collect_live_block_entries(shard: &ShardState) -> Vec<LiveBlockEntry> {
    if !shard.bucket_index.bucket_map.is_empty() {
        collect_bucket_index_live_block_entries(shard)
    } else {
        collect_model_live_block_entries(shard)
    }
}

pub(super) fn mark_async_dirty_object(
    shard: &mut ShardState,
    object_key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    let routing_bucket = block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    // Recorded WITH the bucket that was just computed for it. Every consumer that asks which
    // bucket a dirty object belongs to used to recompute this hash for itself.
    shard.dirty_objects.insert(object_key, routing_bucket);
    let bucket = shard
        .bucket_index
        .bucket_map
        .entry(routing_bucket)
        .or_insert_with(|| BucketNode {
            routing_bucket,
            flags: BucketFlags::default().with(BucketFlags::META_LOADED, true),
            ..BucketNode::default()
        });
    bucket.set_dirty(true);
    bucket.dirty_generation = bucket.dirty_generation.saturating_add(1).max(1);
    note_bucket_flags_stale(shard, routing_bucket);
}

pub(super) fn rebuild_bucket_block_ownership(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) {
    // Preserve the durable per-bucket watermark across the clear+rebuild. load_index keeps
    // dirty_generation; rebuilding with a fresh BucketNode::default() would zero it, making a
    // restored shard (e.g. after a manifest install) mismatch its own dump-manifest generation
    // and forcing unnecessary re-dumps (and mis-driving the index/GC reclaim watermarks). Carry
    // the prior value over via the snapshot below.
    //
    // ONE watermark, where this carried two. `last_dump_sequence` used to ride along and no
    // longer exists on the node: it was read into two reports and nothing else, and the report
    // takes it from the newest dump manifest instead. Nothing here has to preserve a figure a
    // manifest already holds.
    let preserved_dirty_generations: HashMap<u32, u64> = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.dirty_generation))
        .collect();
    // AND THE TOMBSTONE ENTRIES, WHICH THE MODEL MAPS CANNOT RE-DERIVE.
    //
    // This function rebuilds every bucket from `collect_model_live_block_entries`, and those maps hold
    // only LIVE elements -- a removed one is gone from `shard.sets` by construction. So a rebuild
    // produces one entry per live element and NONE for a removal, and the entry that keeps a removal's
    // tombstone page reachable is erased by any round that rebuilds. `compact_shard_blocks` calls this
    // directly, twice, and `promote_model_maps_to_bucket_index_authority` calls it as well; so before
    // this, a removal stopped being recorded in the pages at the next compaction round and a
    // page-derived membership would have resurrected the element.
    //
    // THE ENTRIES ARE CARRIED OVER RATHER THAN RE-DERIVED, because they cannot be re-derived: nothing
    // outside the index knows a removal happened except the WAL, and a rebuild is not a replay. They
    // are filed back after the live rebuild, by the same function the removal path uses, so the
    // bucket accounting and the lookup skip are the ones that own those rules.
    //
    // Serialization is what makes this enough on the reload path: `block_index` carries `deleted` on
    // the wire, so a tombstone entry read back from a stored index survives, and a REPLAY re-files it
    // from the outcome's address instead. Between them the two paths cover every way an index arrives.
    let preserved_tombstones: Vec<(u32, BlockIndex)> = shard
        .bucket_index
        .bucket_map
        .iter()
        .flat_map(|(routing_bucket, bucket)| {
            bucket
                .block_index
                .values()
                .filter(|page| page.deleted)
                .map(move |page| (*routing_bucket, page.clone()))
        })
        .collect();
    shard.bucket_index.bucket_map.clear();
    // The tally counts the map that was just emptied. Emptied with it, and re-earned by the
    // charges the inserts below make -- not by a walk afterwards, which is the walk this whole
    // change exists to remove and which a compaction round would pay twice.
    shard.bucket_index.block_slab_live.clear();
    // The rebuild re-derives every bucket from the model maps, which is what a reload does one
    // bucket at a time. Nothing is released afterwards, and a registry that outlived the map it
    // names would make the block walk supplement buckets that are already whole.
    shard.bucket_index.released_buckets.clear();
    for entry in collect_model_live_block_entries(shard) {
        let routing_bucket =
            block_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket);
        // THE OUT-OF-RANGE FILTER THAT STOOD HERE CANNOT FIRE ANY MORE, so it is gone rather than
        // kept as a `continue` nothing reaches.
        //
        // It read `if routing_bucket < start || routing_bucket > end { continue; }` and its only
        // possible input was an EXPLICIT bucket carried on the address -- a block whose stored bucket
        // fell outside the range the shard was loaded on. There are no explicit buckets: the line
        // above DERIVES the bucket as `start + FNV-1a-64(key) % (end - start + 1)`, which is inside
        // `start..=end` by construction (and `start` itself for a degenerate range).
        // `a_derived_bucket_is_always_inside_the_range_it_was_derived_on` drives that over many keys
        // and several ranges rather than leaving it as arithmetic in a comment.
        //
        // What this removes is not a check but a HAZARD: mx#1974 measured this filter dropping a
        // block from the index entirely when an explicit bucket sat outside the range, and bounded it
        // by showing the engine does not produce that state. It now cannot be produced at all.
        let object_id = expected_live_block_object_id(shard_id, &entry);
        let bucket = shard
            .bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| {
                let dirty_generation = preserved_dirty_generations
                    .get(&routing_bucket)
                    .copied()
                    .unwrap_or_default();
                BucketNode {
                    routing_bucket,
                    flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
                    dirty_generation,
                    ..BucketNode::default()
                }
            });
        bucket.object_index.insert(object_id);
        // Charged as it goes, and then replaced wholesale by the `seed_block_slab_live` at the end
        // of this rebuild. Both, deliberately: the charge keeps this site honest if the shape of
        // the function changes, and the seed is what makes the result independent of whatever the
        // tally held before `bucket_map.clear()` above.
        bucket.block_index.insert(
            BlockIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component.clone(),
                address: entry.address,
                dirty: entry.dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
            &mut shard.bucket_index.block_slab_live,
        );
    }
    // FILE THE TOMBSTONE ENTRIES BACK, after the live rebuild and before the flag pass below, so the
    // `every_page_deleted` computation sees the finished bucket rather than a half-built one.
    //
    // Into the bucket each was FILED IN, not one recomputed from the key: the removal recorded that
    // bucket deliberately, because `block_routing_bucket(key, 0, u32::MAX)` -- the form the WAL outcome
    // uses -- is a different number from the shard's own range, and filing a tombstone under it put the
    // entry in a bucket holding nothing else for its object. That made the bucket all-tombstone (so a
    // twelve-member set reported as a deleted object) and made ownership validation refuse a whole
    // compaction round. Carrying the recorded bucket cannot reintroduce either.
    //
    // A tombstone whose element is LIVE AGAIN is dropped rather than filed. A re-add clears the
    // tombstone through `upsert_bucket_index_block_inner`'s retain, and a rebuild has to reach the same
    // state or it would resurrect a removal the store has already undone -- the mirror image of the
    // defect this whole change is about, and the one direction nothing else here would catch.
    let mut tombstones_refiled = 0usize;
    for (routing_bucket, tombstone) in preserved_tombstones {
        let live_again = shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .is_some_and(|bucket| {
                bucket.block_index.values().any(|page| {
                    !page.deleted
                        && page.model_id == tombstone.model_id
                        && page.object_key == tombstone.object_key
                        && page.component.as_deref() == tombstone.component.as_deref()
                })
            });
        if live_again {
            continue;
        }
        let bucket = shard
            .bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| {
                let dirty_generation = preserved_dirty_generations
                    .get(&routing_bucket)
                    .copied()
                    .unwrap_or_default();
                BucketNode {
                    routing_bucket,
                    flags: BucketFlags::default()
                        .with(BucketFlags::META_LOADED, true)
                        .with(BucketFlags::IN_MEMORY, true),
                    dirty_generation,
                    ..BucketNode::default()
                }
            });
        bucket
            .block_index
            .insert(tombstone, &mut shard.bucket_index.block_slab_live);
        tombstones_refiled += 1;
    }
    note_tombstones_refiled(tombstones_refiled);
    shard.bucket_index.rebuild_object_block_lookup();
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        bucket.set_meta_loaded(true);
        bucket.set_loading(false);
        bucket.set_in_memory(!bucket.block_index.is_empty());
        let every_page_deleted =
            !bucket.block_index.is_empty() && bucket.block_index.values().all(|page| page.deleted);
        bucket.set_deleted(every_page_deleted);
        update_bucket_layout(shard_id, bucket);
    }
    // Every block above was charged as it was filed, and the tally started empty, so it now
    // describes exactly what `bucket_map` holds. Declaring that is the last step; confirming it
    // with a walk would cost a compaction round two whole-shard scans it does not need.
    shard.bucket_index.block_slab_live.mark_ready();
}

/// Tombstone entries carried across an index rebuild.
///
/// A COUNTER AND NOT A DERIVED FIGURE, for the reason every counter in this area exists: "the tombstone
/// count did not change" is also what a rebuild that never ran reports, and a guard needs to tell the
/// two apart. A rebuild that carried none over a store that had removals is the defect; a rebuild that
/// carried none because a re-add had cleared them is correct.
static TOMBSTONES_REFILED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn note_tombstones_refiled(count: usize) {
    TOMBSTONES_REFILED.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Tombstone entries an index rebuild has carried over since the last reset.
pub fn tombstones_refiled_count() -> u64 {
    TOMBSTONES_REFILED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own rebuilds.
pub fn reset_tombstones_refiled_count() {
    TOMBSTONES_REFILED.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// What the promotion check decided, since the last reset.
///
/// #1888 reported that on a restore the answer is always "the index already names everything",
/// so the whole walk is spent returning `false`. That was MEASURED in its configurations, which
/// is not the same as the positive arm being unreachable -- and a precondition whose positive arm
/// nobody can construct is a precondition nobody has tested. These make the question answerable
/// from a guard rather than from a reading of the four call sites.
///
/// `PAGES` is what the walk actually offered the test. A guard asserting the check is cheap needs
/// it: a walk that visited nothing is cheap for the wrong reason, and would pass the same guard.
///
/// COUNTED, not timed, and independent of the counting allocator. Process-wide and monotonic, so
/// a reader resets immediately before the call it is measuring and reads it in a single-threaded
/// suite -- the same contract as [`bucket_scoped_model_entries`] above.
static PROMOTE_MODEL_MAP_CHECKS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PROMOTE_MODEL_MAP_REBUILDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PROMOTE_MODEL_MAP_PAGES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Promotion checks run, promotions that REBUILT, and model-map blocks the checks walked.
pub(super) fn promote_model_map_check_counts() -> (u64, u64, u64) {
    (
        PROMOTE_MODEL_MAP_CHECKS.load(std::sync::atomic::Ordering::Relaxed),
        PROMOTE_MODEL_MAP_REBUILDS.load(std::sync::atomic::Ordering::Relaxed),
        PROMOTE_MODEL_MAP_PAGES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Reset the promotion-check counters. Pairs with [`promote_model_map_check_counts`].
pub(super) fn reset_promote_model_map_check_counts() {
    PROMOTE_MODEL_MAP_CHECKS.store(0, std::sync::atomic::Ordering::Relaxed);
    PROMOTE_MODEL_MAP_REBUILDS.store(0, std::sync::atomic::Ordering::Relaxed);
    PROMOTE_MODEL_MAP_PAGES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Make the bucket index name every live model-map block, if it does not already.
///
/// A PRECONDITION, and preconditions on this path have teeth: #1822 was the default recovery arm
/// calling `rebuild_bucket_block_ownership` without one of these in front of it, and a restart
/// served 0 of 6 hash fields against 6 of 6 strings. So WHAT THIS DECIDES is fixed, and the note
/// below about cost changes none of it.
///
/// The decision, in three parts:
///
///   1. no live model-map block at all -> `false`, having established nothing. (The per-execute
///      caller in `engine.rs` reads exactly this to know not to latch its fast-skip flag.)
///   2. otherwise: is `bucket_map` empty, OR is there a live block the index does not name at the
///      same address? A block routing to a RELEASED bucket is absent on purpose and is not one.
///   3. only if so, rebuild ownership over the routing range, refresh the runtime flags, `true`.
///
/// WHAT IS NOT MATERIALISED, and why that changes nothing above. This opened with
/// `collect_model_live_block_entries(shard)`, which walks the same blocks this walks and turns
/// every one of them into an owned `LiveBlockEntry` -- an owned key and an owned kind, each built
/// as a `String` and then copied into an `Arc<str>`, plus the vector holding them -- to ask an
/// `any()` a question answerable from the borrowed fields the walk is already holding. Since
/// step 2's answer is normally "no", the vector was built in full to return `false`: 4.0
/// allocations per record the shard holds, 22% of a restore's index fold and 10% of the whole
/// restore, and again on the per-execute path in `engine.rs` whenever its fast-skip is not
/// latched.
///
/// `visit_model_live_blocks` offers `emit` the kind, key, component and address as borrows, and
/// those are exactly the four arguments `contains_object_block_address` takes -- the same values
/// the old `any()` tested, in the same order, block for block. The walk IS the check and stays
/// whole; only the vector goes.
///
/// It does not stop the WALK at the first missing block. `any()` stopped there, and the visitor
/// four callers share has no way to be told to stop; what it does stop is the LOOKUP, which is
/// the part that costs more than a field read. On the path this was measured on nothing is ever
/// missing, so the early exit was never reached and removing it costs nothing measured.
pub(super) fn promote_model_maps_to_bucket_index_authority(
    shard_id: ShardId,
    shard: &mut ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> bool {
    PROMOTE_MODEL_MAP_CHECKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (saw_model_entry, bucket_index_missing_entry) = {
        let shard: &ShardState = shard;
        // Read once, ahead of the walk, because the old form was
        // `bucket_map.is_empty() || entries.iter().any(..)` and the left arm short-circuited the
        // right one. An empty index names nothing, so every block would test missing anyway --
        // asking per block would be an index lookup per block for an answer already in hand.
        let bucket_map_empty = shard.bucket_index.bucket_map.is_empty();
        let mut saw_model_entry = false;
        let mut missing_entry = false;
        visit_model_live_blocks(
            shard,
            ModelWalkTally::PromotionCheckPages,
            |_, _| true,
            |kind, object_key, component, address| {
                saw_model_entry = true;
                // The early return below bypasses the LOOKUP, not the count. `visit_model_live_blocks`
                // charges every block it emits before this body runs at all, so a walk over a large
                // shard can no longer read as a walk over a small one by returning early -- and the
                // guard that reads the block count can no longer pass because the check looked cheap.
                if bucket_map_empty || missing_entry {
                    return;
                }
                // A RELEASED bucket is absent on purpose. Without this the first command after a
                // release would find every released block "missing" from the index and rebuild the
                // whole shard -- which is a correct index and a release that never survives one
                // execute.
                let released = shard.bucket_index.released_buckets.contains(
                    &block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket),
                );
                if !released
                    && !shard.bucket_index.contains_object_block_address(
                        kind.as_str(),
                        object_key,
                        component,
                        address,
                    )
                {
                    missing_entry = true;
                }
            },
        );
        (saw_model_entry, bucket_map_empty || missing_entry)
    };
    if !saw_model_entry {
        return false;
    }
    if !bucket_index_missing_entry {
        return false;
    }
    PROMOTE_MODEL_MAP_REBUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    rebuild_bucket_block_ownership(shard_id, shard, start_routing_bucket, end_routing_bucket);
    refresh_bucket_runtime_flags(shard);
    true
}

/// Rebuild ONLY the model maps that are `skip_serializing`, from the durable bucket index. A
/// freshly deserialized index never carries them, so anything deriving state from the model maps
/// would see a shard with no objects of those kinds at all.
///
/// THIS COVERS ONE OF THREE, and the sentence here used to say there was only one. Checked field
/// by field against `collect_model_live_block_entries`, which is what the manifest cross-check
/// reads: of the twelve maps it walks, three are `skip_serializing` --
///
///   * `hashes`            rebuilt below
///   * `context_events`    NOT rebuilt
///   * `context_indexes`   NOT rebuilt
///
/// so a slot dump whose buckets hold context-event or context-index blocks decodes with those maps
/// empty, the model-map derivation misses every one of those blocks, the bucket-index derivation
/// does not, and `install_bucket_dump_manifest` rejects a perfectly good manifest with
/// `slot_dump_object_lifecycle_mismatch`. That is the live failure of
/// `rust_executes_temporalstore_corpus` and
/// `rust_storage_replays_migration_corpus_across_lifecycle_paths`, both on case
/// `native_logical_storage_models_packed_timestamped_pages` -- timestamped being the shape of
/// exactly these two maps, which are keyed by `u64`.
///
/// NOT fixed here, because the two obvious repairs are not equivalent and the choice is not a
/// detail:
///
///   1. Rebuild them too. But these maps are keyed by TIMESTAMP and the bucket-index entries for
///      them carry `component: None` (see the `context_event` arm of
///      `collect_model_live_block_entries`), so the keys are not recoverable from the index.
///      Synthesising keys would make the cross-check agree while putting invented timestamps into
///      a time-keyed map, which is worse than the failure it cures.
///   2. Have the cross-check compare only the maps that SURVIVE serialization. A map the manifest
///      does not carry cannot disagree with the manifest, so it is outside what the check is for
///      -- its stated purpose is a manifest whose serialized model maps contradict its bucket
///      index. On this reading the `hashes` rebuild below is also unnecessary for the check.
///
/// The second looks right, but it narrows a durability check, so it wants the owner of the dump
/// format rather than an inference from a failing test.
///
/// Deliberately narrow: the serialized maps are left exactly as decoded. Rebuilding those from
/// the bucket index too would overwrite whatever the index actually said, which is precisely
/// the disagreement a manifest cross-check exists to detect -- a tampered `strings` object_id
/// would be silently repaired instead of rejected.
pub(super) fn rebuild_unserialized_model_maps_from_bucket_index(shard: &mut ShardState) {
    if shard.bucket_index.bucket_map.is_empty() {
        return;
    }
    let mut hashes = HashMap::<String, super::hash_field_map::HashFieldMap>::new();
    // Block entries that named no field, over the one kind this function derives. See the arm below
    // for why this is a skip and not a default; counted so it is not silent, the way
    // `reconcile_secondary_views_from_bucket_index` counts the same thing for the other three.
    let mut unreadable_names = 0usize;
    // THE SAME LIVE-ADDRESS SET THE RECONCILE BUILDS, and built from the same walk the derived view
    // below is built from, so the filter can only ever remove what the derived view also lacks.
    let walked = collect_bucket_index_live_block_entries(shard);
    let live_pages_by_address: std::collections::HashSet<super::LiveBlockKey> = walked
        .iter()
        .filter(|entry| !entry.deleted)
        .map(|entry| super::live_page_key(&entry.address))
        .collect();
    for entry in walked {
        if entry.deleted || entry.kind.as_str() != "hash" {
            continue;
        }
        // SKIPPED, NOT DEFAULTED. This was `entry.component.unwrap_or_default()`, which turns a block
        // that names NO field into a field named `""` -- a real, addressable field name, which then
        // collides with a genuine empty-named field and takes its address. An absent name names
        // nothing.
        //
        // AND THIS ARM NOW HAS A DURABLE MAP BEHIND IT, which is what changed. It used to say what
        // the other three arms could not: `hashes` was `skip_serializing`, so nothing was written,
        // there was no map to outrank a wrong answer, and a phantom field was the only answer the
        // shard had. `hashes` carries `#[serde(default)]` now, so this arm says exactly what the
        // other three say -- "the durable map still holds the element, so skipping loses it from the
        // derived view and not from the store" -- and the merge below is what makes that true.
        //
        // The name is not recoverable from anywhere else, so inventing one is the only alternative
        // to skipping. For a hash the component IS the field name, decoded by nothing (#2009), and
        // #2013's per-element `StagedBlock` does not supply it either: that registry is keyed BY the
        // component and is live-path state that is never persisted, so it cannot be asked which
        // field an unnamed entry was -- the object id alone is not a discriminator between an
        // object's own blocks, which is the finding #2013 landed.
        match entry.component {
            Some(field) => {
                hashes
                    .entry(entry.object_key.to_string())
                    .or_default()
                    .insert(field.to_string(), entry.address);
            }
            None => unreadable_names += 1,
        }
    }
    if unreadable_names > 0 {
        // SAID OUT LOUD, in the same words the reconcile uses, so an operator sees a derived view
        // that is short of a field rather than silently getting a field nobody wrote.
        eprintln!(
            "rebuild_unserialized_model_maps: {unreadable_names} hash page(s) named no field and \
             were skipped rather than defaulted to the empty field name"
        );
    }
    // MERGED, NOT ASSIGNED, AND THE `is_empty` GATE WENT WITH THE ASSIGNMENT. While `hashes` was
    // `skip_serializing` there was nothing in `shard.hashes` to protect, so overwriting it whenever
    // the index said anything was harmless and the gate only avoided clearing it to empty. It is
    // durable now, so a wholesale assignment here would discard exactly the elements this function's
    // own header warns about discarding -- a field whose block the index cannot NAME is a field the
    // durable map is now the only record of. The merge keeps it; the live-address filter still drops
    // a durable element whose page the settled index does not hold.
    let mut resurrections_refused = 0usize;
    let persisted = std::mem::take(&mut shard.hashes);
    shard.hashes = fill_absent_elements(
        hashes,
        persisted,
        &live_pages_by_address,
        &mut resurrections_refused,
    );
    if resurrections_refused > 0 {
        eprintln!(
            "rebuild_unserialized_model_maps: {resurrections_refused} persisted hash field(s) named              a page the bucket index does not hold and were not restored"
        );
    }
}

#[track_caller]
pub(super) fn collect_bucket_index_live_block_entries(shard: &ShardState) -> Vec<LiveBlockEntry> {
    let mut entries = Vec::new();
    // Charged here rather than by whoever called: three production callers reach this walk
    // without going through `collect_live_block_entries`, and the wrapper's charge could not see
    // the supplement walk below either -- it charged what was RETURNED, while the walk
    // materializes the indexed blocks and then, whenever anything is released, the whole shard.
    let mut from_index = 0usize;
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        for page in bucket.block_index.values() {
            from_index += 1;
            entries.push(LiveBlockEntry {
                object_key: page.object_key.clone(),
                kind: page.model_id,
                // Both sides are `Option<Arc<str>>`; going through a String allocated the text
                // twice per block to arrive at the same pointer a clone hands back for free.
                component: page.component.clone(),
                address: page.address.clone(),
                dirty: page.dirty,
                deleted: page.deleted,
                log_backed: page.log_backed(),
                // The key of the map being walked. This walk always knew it; it was iterating
                // `.values()` and throwing it away, which is why five readers downstream had to
                // guess it back out of the object key.
                filed_routing_bucket: *routing_bucket,
                filing_is_known: true,
            });
        }
    }
    note_live_block_scan(from_index);
    // A RELEASED bucket holds no block entries, and every caller of this walk -- the dump
    // manifest, WAL reclaim, compaction, the GC snapshot -- reads "no entries" as "no live
    // blocks". Left alone that is not a cheaper index, it is a block whose backing record may be
    // reclaimed. So the released buckets are supplemented from the model maps, which is the same
    // source `reload_released_bucket` would rebuild them from: what this returns is what the
    // bucket index WOULD say if nothing were released.
    //
    // Exact, not approximate, because a release refuses any bucket holding a block whose KEY does
    // not route to it -- `BucketReleaseRefusal::BlockRoutingMismatch` -- so the bucket computed
    // below is the bucket the released node held the block under, and cannot claim a block for the
    // wrong bucket.
    if !shard.bucket_index.released_buckets.is_empty() {
        let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
        for mut entry in collect_model_live_block_entries(shard) {
            let routing_bucket = block_routing_bucket(
                &entry.object_key,
                start_routing_bucket,
                end_routing_bucket,
            );
            if shard.bucket_index.released_buckets.contains(&routing_bucket) {
                // The filing is known exactly here too, and a supplemented entry must not read as
                // "not filed": the five readers downstream would then fall back to a hash over the
                // WHOLE keyspace and name a bucket a narrow shard does not hold.
                entry.filed_routing_bucket = routing_bucket;
                entry.filing_is_known = true;
                entries.push(entry);
            }
        }
    }
    entries
}

/// The identity a released block is compared by, so a release can prove it is reversible.
///
/// `object_id` is not part of it, and no longer COULD be: an address does not carry one. The reason
/// it was excluded while it did is worth keeping, because it is why removing the field was safe
/// here -- the bucket index stamped an id into the address it filed while the model map's copy of
/// the same block did not, so comparing on it would have refused every release over a difference
/// reload reproduces on its own. Two stored copies that could disagree became one derivation that
/// cannot, which is the same conclusion arrived at for free.
type ReleasedBlockIdentity = (String, String, Option<String>, u64, u64, u64, Option<u64>, Option<u64>);

fn released_block_identity(
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
    address: &BlockAddress,
) -> ReleasedBlockIdentity {
    released_block_identity_owned(
        model_id.to_string(),
        object_key.to_string(),
        component.map(str::to_string),
        address,
    )
}

/// The same identity, for a caller that already owns its strings.
///
/// The derivation below is handed owned keys by the walk and would otherwise copy each one a
/// second time to hand it to the borrowing form. One definition of the tuple, so the resident side
/// and the derived side cannot drift into comparing differently-ordered fields.
fn released_block_identity_owned(
    model_id: String,
    object_key: String,
    component: Option<String>,
    address: &BlockAddress,
) -> ReleasedBlockIdentity {
    (
        model_id,
        object_key,
        component,
        address.block_slab_id(),
        address.offset(),
        address.length(),
        address.block_id(),
        address.generation(),
    )
}

/// Model kinds a bucket may be released while holding.
///
/// An ALLOW-list, not a deny-list, and deliberately short. Two independent things have to be true
/// of a kind before a bucket holding it can lose its block entries, and both are properties of the
/// kind rather than of the bucket:
///
///   1. ITS MAP MUST SURVIVE SERIALIZATION. `context_events` and `context_indexes` are
///      `skip_serializing` on `ShardState` and are rebuilt FROM the bucket index on load, so a
///      released bucket of one of those kinds would have nothing to rebuild from the moment the
///      index was written and read back. `hashes` WAS in that list and is not any more -- it is
///      durable now -- so this term no longer holds it out. Term 2 does, and it is the only thing
///      holding it out, which is worth knowing before anyone reads this list as settled.
///   2. A READ MUST STILL RESOLVE IT. `bucket_index_block_address` -- the slow read path -- looks
///      an address up THROUGH the bucket index, so a released block has to be findable in its model
///      map by `(kind, object_key, component)` alone. `model_map_block_address` is that lookup, and
///      it is a point lookup, not a scan. Kinds whose objects span components or timestamps
///      (`set`, `zset`, `list`, `feature`, the context series) are also read whole through
///      `bucket_index_component_block_addresses`, which has no equivalent point lookup, so they
///      stay out until one exists.
///
/// That leaves the two component-less, single-block, serialized kinds -- which is also where the
/// index cost being reclaimed actually is: one block and one node per key.
fn released_model_kind_is_addressable(kind: &str) -> bool {
    matches!(kind, "string" | "context_node")
}

/// The address of a block held by a RELEASED bucket, from the model map the block lives in.
///
/// The counterpart to `bucket_index_block_address`: same question, asked of the maps instead of the
/// index. Only the kinds `released_model_kind_is_addressable` admits are answerable here, and that
/// is not a coincidence -- it is the same list, for this reason.
pub(super) fn model_map_block_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<BlockAddress> {
    match (model_id, component) {
        ("string", None) => shard.strings.get(object_key).cloned(),
        ("context_node", None) => shard.context_nodes.get(object_key).cloned(),
        _ => None,
    }
}

/// The same, but only when the block really does belong to a released bucket.
///
/// The guard matters: without it this would answer for a block whose bucket is resident and whose
/// index entry is absent for some other reason -- which is a disagreement the promote reconcile
/// exists to find and repair, not one to paper over on the read path.
pub(super) fn released_bucket_block_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<BlockAddress> {
    if shard.bucket_index.released_buckets.is_empty() {
        return None;
    }
    let address = model_map_block_address(shard, model_id, object_key, component)?;
    // THE CONTAINER'S ABSENCE IS THE CONDITION; THE BUCKET ID IS STILL KNOWN. This asks whether a
    // SPECIFIC bucket is released, and which bucket that is comes from the key, not from the block:
    // `released_buckets` is keyed by bucket id and the key's bucket is what a release recorded.
    // The address used to carry it and an address carrying none returned `None` here, which was a
    // second answer to a question the key already answers.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let routing_bucket =
        block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    shard
        .bucket_index
        .released_buckets
        .contains(&routing_bucket)
        .then_some(address)
}

/// The model kinds a released bucket may hold, in the form the delete path needs them.
///
/// `released_model_kind_is_addressable` answers the question one kind at a time, which is what the
/// read path asks. A whole-object delete names a key and no kind at all, so settling the released
/// side of it means asking each releasable kind whether this key is one of its blocks. Same list,
/// same reasons, stated once.
pub(super) const RELEASABLE_MODEL_KINDS: [&str; 2] = ["string", "context_node"];

/// Drop a deleted object's id from its RELEASED bucket's object index.
///
/// `release_bucket_blocks` empties `page_index` and KEEPS `object_index`; the keeping is the only
/// thing that tells a released bucket from one legitimately holding nothing, and since
/// `classify_bucket_layout` was corrected the object count is the sole authority for whether a
/// bucket is empty at all. Both delete paths remove an object by walking `page_index` -- which a
/// release has already emptied -- so a delete arriving while the bucket is released dropped the
/// block from the model map and left the id claimed. The node then reported an object that no
/// longer existed, and reported it as LIVE: the resident path tombstones what it removes in
/// `deleted_object_index`, and none of that ran either.
///
/// `reload_released_bucket` re-derives the set from the model maps and settles it -- but only
/// whenever a reload happens, and the point of a release is that one may not for a long time. So
/// this is that same re-derivation, for the one id, performed at the delete. It must run BEFORE
/// the model map entry goes, which is the order every delete path already uses, because the map
/// is where the block's address -- and with it the routing bucket and the object id -- is read.
///
/// TWO THINGS ARE DELIBERATELY NOT DONE HERE.
///
///   * The node is left in the map when its last object goes. `reload_released_bucket` removes a
///     node it finds no blocks for, and bringing that removal forward would take a bucket out of
///     the reclaim plan's view earlier than anything has asked for. Leaving it costs one node and
///     the bucket now reports `empty`, which is true.
///   * No tombstone is written. The resident path keeps the id and records it in
///     `deleted_object_index` because the id stays; here the id goes, and a tombstone for an
///     absent id is an entry nothing would ever read. A reload derives no tombstone either, which
///     is the state this is bringing forward.
pub(super) fn settle_released_bucket_object_delete(
    shard: &mut ShardState,
    object_key: &str,
) -> bool {
    if shard.bucket_index.released_buckets.is_empty() {
        return false;
    }
    // NO SHARD, NO DERIVATION. A state that never entered the engine has no id to derive with, and
    // there is no id that is safe to guess here: a zero would name a member of a different shard.
    let Some(shard_id) = shard.shard_id() else {
        return false;
    };
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let mut settled = false;
    for model_id in RELEASABLE_MODEL_KINDS {
        // Answers only for a block whose bucket really is released -- a resident bucket with a
        // missing index entry is a disagreement for the promote reconcile, not for a delete.
        let Some(address) = released_bucket_block_address(shard, model_id, object_key, None) else {
            continue;
        };
        // The bucket is the KEY's bucket -- `released_bucket_block_address` above answered for
        // exactly that bucket, so asking again here gets the same number.
        //
        // THE ID IS NOW DERIVED RATHER THAN SKIPPED, and that IS the behaviour it always had. The
        // skip existed because an address could carry no object id; an address carries none at all
        // now, so skipping unconditionally would mean never removing a member -- which is a
        // behaviour change, where deriving is not. Every address this path saw was written by a
        // write path that put the derived id on it, so the derivation answers what the field held.
        let routing_bucket =
            block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
        let object_id = stable_block_object_id(shard_id, model_id, object_key);
        let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
            continue;
        };
        if bucket.object_index.remove(&object_id) {
            classify_bucket_layout_in_place(bucket);
            settled = true;
        }
    }
    settled
}

/// WHICH precondition a refusal failed, at the granularity of the term that decided it.
///
/// `refused_buckets` is one number for eleven different answers, and that is not enough to
/// assert on: a test that pins it to 1 passes when the WRONG term fired, so every term was
/// verifiable only by however pure the scenario a test happened to build was. Each term counts
/// itself here, and each has a guard that fails on its own number.
///
/// Refusal is attributed to the FIRST failing term, in the order the function tests them. A
/// bucket that is both dirty and deleted is one refusal, counted as dirty, and the totals stay
/// equal to `refused_buckets` -- which is the invariant `refusals_total_matches_refused_buckets`
/// pins.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct BucketReleaseRefusals {
    /// Not resident, so there is no resident block list to drop.
    pub(super) not_in_memory: usize,
    /// A load is in flight and the block list is not settled.
    pub(super) loading: usize,
    /// The bucket still pins the log. Releasing it while `eviction_dump_before_evict` is false
    /// would strand undumped writes with nothing to rebuild them from.
    pub(super) bucket_dirty: usize,
    /// Deleted: its blocks are not a set a reload should rebuild.
    pub(super) bucket_deleted: usize,
    /// Nothing resident to release.
    pub(super) empty_block_index: usize,
    /// A block carries an unwritten change the model maps do not record.
    pub(super) block_dirty: usize,
    /// A block is delete-marked, which the model maps do not record either.
    pub(super) block_deleted: usize,
    /// A block's address does not name THIS bucket, so which entries are this bucket's could
    /// only be answered by a hash fallback.
    pub(super) block_routing_mismatch: usize,
    /// A block's kind is not one `released_model_kind_is_addressable` admits.
    pub(super) block_kind_not_addressable: usize,
    /// A block's lookup refs are held by some other bucket.
    pub(super) lookup_not_local: usize,
    /// The model maps would not rebuild what is resident.
    pub(super) model_map_disagreement: usize,
}

impl BucketReleaseRefusals {
    /// Every refusal counted, whatever the reason. Equal to `refused_buckets` by construction.
    pub(super) fn total(&self) -> usize {
        self.not_in_memory
            + self.loading
            + self.bucket_dirty
            + self.bucket_deleted
            + self.empty_block_index
            + self.block_dirty
            + self.block_deleted
            + self.block_routing_mismatch
            + self.block_kind_not_addressable
            + self.lookup_not_local
            + self.model_map_disagreement
    }
}

/// The term a refusal failed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BucketReleaseRefusal {
    NotInMemory,
    Loading,
    BucketDirty,
    BucketDeleted,
    EmptyBlockIndex,
    BlockDirty,
    BlockDeleted,
    BlockRoutingMismatch,
    BlockKindNotAddressable,
    LookupNotLocal,
    ModelMapDisagreement,
}

/// What one call to [`release_bucket_blocks`] managed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct BucketReleaseOutcome {
    pub(super) released_buckets: Vec<u32>,
    pub(super) released_blocks: usize,
    /// Candidates that failed a precondition. A release that quietly did nothing and a release
    /// that was refused are different answers, and the eviction report publishes both.
    pub(super) refused_buckets: usize,
    /// The same number, broken out by the term that decided it.
    pub(super) refusals: BucketReleaseRefusals,
}

impl BucketReleaseOutcome {
    /// Record one refusal against the term that caused it.
    fn refuse(&mut self, reason: BucketReleaseRefusal) {
        self.refused_buckets = self.refused_buckets.saturating_add(1);
        let counter = match reason {
            BucketReleaseRefusal::NotInMemory => &mut self.refusals.not_in_memory,
            BucketReleaseRefusal::Loading => &mut self.refusals.loading,
            BucketReleaseRefusal::BucketDirty => &mut self.refusals.bucket_dirty,
            BucketReleaseRefusal::BucketDeleted => &mut self.refusals.bucket_deleted,
            BucketReleaseRefusal::EmptyBlockIndex => &mut self.refusals.empty_block_index,
            BucketReleaseRefusal::BlockDirty => &mut self.refusals.block_dirty,
            BucketReleaseRefusal::BlockDeleted => &mut self.refusals.block_deleted,
            BucketReleaseRefusal::BlockRoutingMismatch => {
                &mut self.refusals.block_routing_mismatch
            }
            BucketReleaseRefusal::BlockKindNotAddressable => {
                &mut self.refusals.block_kind_not_addressable
            }
            BucketReleaseRefusal::LookupNotLocal => &mut self.refusals.lookup_not_local,
            BucketReleaseRefusal::ModelMapDisagreement => {
                &mut self.refusals.model_map_disagreement
            }
        };
        *counter = counter.saturating_add(1);
    }
}

/// Dump-and-release: drop the named buckets' resident block lists, keeping the nodes routable.
///
/// This is the half that was missing. `evict_cache` drops CACHED BLOCKS and leaves every
/// `BucketNode` whole, so eviction could free only what the cache held and the index -- the part
/// that actually grows one entry per record -- was untouchable. Releasing a bucket frees its
/// `page_index` and its lookup refs while the node, its `object_index` and its durable watermarks
/// stay, so the next read through `reload_released_bucket` rebuilds exactly what was dropped.
///
/// Every precondition is CHECKED here, against this shard's live state, rather than assumed from
/// how the caller chose its candidates. See the residency-flag doc on `BucketNode` for why each
/// one is needed.
pub(super) fn release_bucket_blocks(
    shard: &mut ShardState,
    candidates: &[u32],
) -> BucketReleaseOutcome {
    let mut outcome = BucketReleaseOutcome::default();
    if candidates.is_empty() {
        return outcome;
    }
    let wanted: BTreeSet<u32> = candidates.iter().copied().collect();
    // One model-map walk for the whole batch, not one per bucket. This is the set a reload would
    // rebuild from, so comparing the resident blocks against it is the proof the release is
    // reversible.
    // WHAT A RELOAD WOULD REBUILD for each wanted bucket, and nothing else. Comparing the
    // resident blocks against it is the proof the release is reversible.
    //
    // This walks the model maps and it has to: the maps are keyed by object, the routing bucket
    // is a field of the ADDRESS, and no index runs the other way. What the walk no longer does is
    // build an owned entry for every live block in the store before throwing all but the wanted
    // ones away -- four allocations per object in the store, to release four buckets.
    //
    // THE REMAINING COST IS THE VISIT, AND IT IS PAID ONLY WHEN A CANDIDATE ASKS FOR IT. #1884
    // removed what this pass ALLOCATES; the pass still runs `accept` on every live address in the
    // shard, which is 200,000 field reads at 200,000 records to answer a question about at most
    // `batch_limit` buckets, and no allocation table can see it because the walk is borrow-only.
    // Every term tested below the derivation -- the five residency terms, the four per-block
    // terms, and the lookup-locality term -- reads the CANDIDATE's own state and nothing the
    // derivation produces, so a candidate that fails one of them never consults the derived map.
    // At the shipped `eviction_dump_before_evict` false a freshly written bucket is dirty and
    // every candidate is refused on `bucket_dirty`, so the whole-store pass was run and then
    // entirely discarded, once per round, for ever.
    //
    // WHAT IS NOT CHANGED, and the reason this is a memoization rather than a reorder: the terms
    // keep their order relative to one another, the derivation still covers the WHOLE candidate
    // set in one pass, and it is still computed before any bucket is mutated -- the first
    // candidate that reaches the comparison has not released anything yet, and a release touches
    // only `bucket_index`, which the derivation does not read. So the map built here is the map
    // that was built eagerly, at the same shard state, and every outcome field is identical.
    // `refuse` is a set of counters with no ordering, and `released_buckets` is still pushed in
    // candidate order.
    let mut derived: Option<BTreeMap<u32, BTreeSet<ReleasedBlockIdentity>>> = None;
    let lookup_established = !shard.bucket_index.object_block_lookup.is_empty();
    // The shard's own range, for the per-block routing term below.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    // Iterated by reference rather than consumed, because the derivation below is handed the same
    // set and is now reached from inside the loop. Same order, same elements, no allocation.
    for routing_bucket in wanted.iter().copied() {
        let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
            continue;
        };
        // The residency terms, tested in order so a refusal can name the one that decided it.
        // Same set, same order, same answer as the `||` chain this replaces -- what is new is
        // that the outcome can say WHICH, which is what makes one guard per term possible.
        let residency_refusal = if !bucket.in_memory() {
            Some(BucketReleaseRefusal::NotInMemory)
        } else if bucket.loading() {
            Some(BucketReleaseRefusal::Loading)
        } else if bucket.dirty() {
            Some(BucketReleaseRefusal::BucketDirty)
        } else if bucket.deleted() {
            Some(BucketReleaseRefusal::BucketDeleted)
        } else if bucket.block_index.is_empty() {
            Some(BucketReleaseRefusal::EmptyBlockIndex)
        } else {
            None
        };
        if let Some(reason) = residency_refusal {
            outcome.refuse(reason);
            continue;
        }
        // The per-block terms, likewise. `find_map` stops at the first block that fails, which
        // is the same set as the `all` it replaces: `all` is false exactly when `find_map` is
        // Some.
        let block_refusal = bucket.block_index.values().find_map(|block| {
            if block.dirty {
                Some(BucketReleaseRefusal::BlockDirty)
            } else if block.deleted {
                Some(BucketReleaseRefusal::BlockDeleted)
            } else if block_routing_bucket(
                &block.object_key,
                start_routing_bucket,
                end_routing_bucket,
            ) != routing_bucket
            {
                // STILL A CHECK THAT CAN FAIL, and it now compares two INDEPENDENT things rather
                // than a block's copy of its bucket against the bucket holding it. Where the block
                // IS (the key of the map being walked) against where its KEY routes: a block filed
                // under a stale range, or moved by hand, fails here. The old form compared a field
                // the filing site had just written against the filing site's own key.
                Some(BucketReleaseRefusal::BlockRoutingMismatch)
            } else if !released_model_kind_is_addressable(block.model_id.as_str()) {
                Some(BucketReleaseRefusal::BlockKindNotAddressable)
            } else {
                None
            }
        });
        if let Some(reason) = block_refusal {
            outcome.refuse(reason);
            continue;
        }
        // The lookup refs for each block must point at THIS bucket, or dropping the block's lookup
        // entry below would also drop a ref some other bucket still owns.
        let lookup_is_local = !lookup_established
            || bucket.block_index.values().all(|page| {
                shard
                    .bucket_index
                    .block_refs_for(page.model_id.as_str(), &page.object_key, page.component.as_deref())
                    .map(|refs| refs.iter().all(|block_ref| block_ref.routing_bucket == routing_bucket))
                    .unwrap_or(false)
            });
        if !lookup_is_local {
            outcome.refuse(BucketReleaseRefusal::LookupNotLocal);
            continue;
        }
        let resident: BTreeSet<ReleasedBlockIdentity> = bucket
            .block_index
            .values()
            .map(|page| {
                released_block_identity(
                    page.model_id.as_str(),
                    &page.object_key,
                    page.component.as_deref(),
                    &page.address,
                )
            })
            .collect();
        // THE WHOLE-STORE PASS, on first demand and once. Nothing above this line consulted it.
        let derived = derived.get_or_insert_with(|| {
            BUCKET_RELEASE_MODEL_DERIVATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            derive_released_block_identities(shard, &wanted)
        });
        if derived.get(&routing_bucket) != Some(&resident) {
            // The model maps would not rebuild what is resident. Whatever the disagreement is,
            // it is not this function's to resolve -- and releasing across it would lose blocks.
            outcome.refuse(BucketReleaseRefusal::ModelMapDisagreement);
            continue;
        }
        let dropped: Vec<(StoredModelKind, Arc<str>, Option<Arc<str>>)> = bucket
            .block_index
            .values()
            .map(|page| {
                (
                    page.model_id,
                    page.object_key.clone(),
                    page.component.clone(),
                )
            })
            .collect();
        let block_count = dropped.len();
        if lookup_established {
            for (model_id, object_key, component) in &dropped {
                shard.bucket_index.remove_object_block_lookup_entry(
                    model_id.as_str(),
                    object_key,
                    component.as_deref(),
                );
            }
        }
        let bucket = shard
            .bucket_index
            .bucket_map
            .get_mut(&routing_bucket)
            .expect("bucket read immutably above");
        bucket.block_index = crate::engine::state::BlockIndexMap::Empty;
        bucket.set_meta_loaded(true);
        bucket.set_loading(false);
        bucket.set_in_memory(false);
        // `object_index` is deliberately kept: it is what keeps the bucket countable and is the
        // only thing distinguishing a released bucket from one that legitimately holds nothing.
        bucket.layout = classify_bucket_layout(bucket.object_index.object_count(), 0);
        shard.bucket_index.released_buckets.insert(routing_bucket);
        outcome.released_buckets.push(routing_bucket);
        outcome.released_blocks = outcome.released_blocks.saturating_add(block_count);
    }
    outcome
}

/// Load a released bucket's block list back, from the maps a read already resolves through.
///
/// The mirror of [`release_bucket_blocks`], and the reason releasing is safe. Returns false when
/// the bucket is not released -- an already-resident bucket is a no-op, not an error, which is
/// what lets every mutation site call this unconditionally.
pub(super) fn reload_released_bucket(
    shard: &mut ShardState,
    shard_id: ShardId,
    routing_bucket: u32,
) -> bool {
    if !shard.bucket_index.released_buckets.contains(&routing_bucket) {
        return false;
    }
    if let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) {
        // Held across the derivation below. Under the shard write lock nothing can observe it
        // today; it is the state a queued concurrent loader would wait on if this ever became
        // asynchronous, and setting it is what makes that a wiring change rather than a design.
        bucket.set_loading(true);
    } else {
        shard.bucket_index.released_buckets.remove(&routing_bucket);
        return false;
    }
    let mut pages: Vec<(BlockIndex, u64)> = Vec::new();
    // ONE bucket's blocks, filtered inside the walk. This used to materialize every live block in
    // the shard and then drop all but this bucket's -- a batch reload over N released buckets
    // would have paid the whole store N times. The walk now filters by bucket, so one reload
    // costs one bucket's blocks.
    for entry in collect_model_live_block_entries_in_bucket(shard, routing_bucket) {
        let object_id = expected_live_block_object_id(shard_id, &entry);
        let address = entry.address;
        pages.push((
            BlockIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component,
                address,
                // The model maps carry no per-block dirty/deleted bit, which is exactly why
                // release refuses a bucket holding either. Reloaded blocks are clean and live,
                // which is the state they were released in.
                dirty: false,
                deleted: false,
                log_backed: entry.log_backed,
            },
            object_id,
        ));
    }
    let bucket = shard
        .bucket_index
        .bucket_map
        .get_mut(&routing_bucket)
        .expect("bucket present: checked above and the shard is locked");
    let mut installed: Vec<(u64, BlockIndex)> = Vec::with_capacity(pages.len());
    for (page, object_id) in pages {
        bucket.object_index.insert(object_id);
        // NOT charged. `release_bucket_blocks` did not discharge these -- the blocks stayed live
        // the whole time it held them out of the index -- so counting them here would double
        // every released bucket the first time anything touched it again.
        let handle = bucket.block_index.insert_released(page.clone());
        installed.push((handle, page));
    }
    bucket.set_meta_loaded(true);
    bucket.set_in_memory(!bucket.block_index.is_empty());
    bucket.set_loading(false);
    if bucket.block_index.is_empty() {
        // Everything the bucket held was deleted while it was released. Nothing routes here any
        // more, so the node goes rather than lingering with a stale object index.
        shard.bucket_index.bucket_map.remove(&routing_bucket);
        shard.bucket_index.released_buckets.remove(&routing_bucket);
        return true;
    }
    update_bucket_layout(shard_id, bucket);
    for (handle, page) in installed {
        shard
            .bucket_index
            .insert_object_block_lookup(routing_bucket, handle, &page);
    }
    shard.bucket_index.released_buckets.remove(&routing_bucket);
    note_bucket_flags_stale(shard, routing_bucket);
    true
}

/// The node-only floor: one `BucketNode` width per resident bucket.
///
/// ONE AUTHORITY, BECAUSE THERE ARE TWO READERS AND THEY MUST NOT DRIFT. `persistence` publishes
/// this as `bucket_index_resident_bytes_floor` and [`bucket_index_resident_bytes`] uses it as its
/// own first term; both used to spell `bucket_map.len() * size_of::<BucketNode>()` out for
/// themselves. Two copies of one product agree until one of them is edited, and the failure that
/// produces is the published floor ceasing to be a floor of the published total -- which nothing
/// would have failed on, because each site would still be internally consistent.
/// `the_published_floor_is_the_node_term_of_the_published_total` is the standing assertion that
/// the two are one quantity.
pub(super) fn bucket_index_node_bytes(shard: &ShardState) -> u64 {
    (shard.bucket_index.bucket_map.len() as u64)
        .saturating_mul(std::mem::size_of::<BucketNode>() as u64)
}

/// What the resident bucket index costs: one node per bucket plus what that bucket's block index
/// owns on the heap.
///
/// The published `bucket_index_resident_bytes_floor` counts NODES only, so it cannot move when a
/// bucket is released -- the node stays. The per-block entries are the part that grows with the
/// corpus and the part a release actually frees, so the eviction gate needs this number and not
/// that one.
///
/// THE BLOCK TERM IS ASKED OF THE CONTAINER, NOT COMPUTED FROM A BLOCK COUNT. It was
/// `pages * size_of::<BlockIndex>()`, and measured against the counting allocator that is right on
/// one of the three arms and wrong on the one the shipped routing range puts every bucket into:
/// 0.6817 of what the allocator held at 4,000 records on `0..1023` and 0.8542 at 40,000, because
/// a `Many` arm's element is the eight-byte-wider PAIR and its buffer is owned at CAPACITY. The
/// arithmetic now lives on `BlockIndexMap::resident_heap_bytes`, beside the arms, where each term
/// is the width of the type that arm holds. `the_resident_index_report_is_measured_against_the_allocator`
/// measures this against the allocator at two corpus sizes, both routing ranges and both block
/// populations, and `the_resident_index_report_reconstructs_from_the_widths_the_containers_declare`
/// fails if a field changes width without this moving.
///
/// WHY IT MATTERS MORE THAN A DASHBOARD ROW. This is a term in the eviction pressure score
/// `storage_manager_cycle` reads. A figure that reads low is an engine holding an index it should
/// have released.
///
/// WHAT IT STILL CANNOT SEE, STATED RATHER THAN IMPLIED. Four things, and all four push the same
/// way -- this remains a FLOOR:
///
///   1. REQUEST BYTES, NOT CHUNK BYTES. Every term here is what the container asked for.
///      `ALLOC_CHUNK_BYTES` reads `malloc_usable_size`, which is a floor above the request and not
///      an equality (#1969: a 104-byte request read 128). Asking the allocator is not something a
///      serving binary can do per figure, and modelling a rounding rule here would make this a
///      claim about the platform's allocator rather than about this engine's containers.
///   2. THE BUCKET MAP'S OWN SPINE. `nodes * size_of::<BucketNode>()` charges the node's WIDTH.
///      The `BTreeMap` that holds it allocates in whole nodes sized for eleven entries whether or
///      not they fill, and that overhead is not here.
///   3. THE OTHER TWO INDEXES ON EVERY NODE. `object_index` and `deleted_object_index` are inside
///      `size_of::<BucketNode>()` as containers, so whatever THEY hold out of line is not counted.
///   4. THE SHARED NAMES, AND THIS ONE IS DELIBERATE. `resident_heap_bytes` says why: measured,
///      this index never owns them, so charging them here would double-count the model maps that
///      do.
pub(super) fn bucket_index_resident_bytes(shard: &ShardState) -> u64 {
    let nodes = shard.bucket_index.bucket_map.len() as u64;
    let mut pages = 0u64;
    let mut page_heap = 0u64;
    for bucket in shard.bucket_index.bucket_map.values() {
        pages = pages.saturating_add(bucket.block_index.len() as u64);
        page_heap = page_heap.saturating_add(bucket.block_index.resident_heap_bytes());
    }
    // One entry per node plus one per resident block, which is what the walk above touched.
    // Charged here rather than at the call sites: the eviction round reaches this twice per
    // round and the lifecycle plan reaches it again, and a charge at any one of those would
    // describe a fraction of the walking that actually happens.
    //
    // UNCHANGED BY THE BLOCK-TERM FIX, ON PURPOSE. Both the old arithmetic and this one visit one
    // node per bucket and read a length that is O(1) on every arm; neither has ever walked the
    // blocks one by one. The charge names the entries the index HOLDS, which this change does not
    // move, so the eviction round's pinned visit counts stay comparable across it.
    BUCKET_INDEX_RESIDENT_BYTES_VISITS
        .fetch_add(nodes.saturating_add(pages), std::sync::atomic::Ordering::Relaxed);
    bucket_index_node_bytes(shard).saturating_add(page_heap)
}

/// Cheap O(1)-per-map check for whether the shard holds ANY live model-map entry that
/// `collect_model_live_block_entries` would enumerate. Used to avoid latching the phase-1
/// `promote_scan_done` fast-skip flag before the shard has any state to reconcile. Short-circuits
/// on the first non-empty map; never clones.
pub(super) fn shard_has_model_entries(shard: &ShardState) -> bool {
    !shard.strings.is_empty()
        || !shard.hashes.is_empty()
        || !shard.sets.is_empty()
        || !shard.lists.is_empty()
        || !shard.zsets.is_empty()
        || !shard.features.is_empty()
        || !shard.control_state_blocks.is_empty()
        || !shard.context_nodes.is_empty()
        || !shard.context_events.is_empty()
        || !shard.context_indexes.is_empty()
        || !shard.context_audits.is_empty()
        || !shard.context_entities.is_empty()
        || !shard.context_children.is_empty()
        || !shard.context_summaries.is_empty()
        || !shard.context_compressions.is_empty()
}

/// Bring the bucket index up to date for ONE object key, across every context kind.
///
/// A context write does not register its block. The shard rebuilds the whole first-index afterwards
/// instead -- `rebuild_bucket_first_index`, which walks every live block in the store -- and with
/// several context writes per add that was the last term in an add that grows with the corpus.
/// Measured before coalescing: 5 762 400 block visits across 600 adds, per-add cost doubling as the
/// corpus doubled.
///
/// Feature and Sequence writes already maintain the index this way on the write path, and REPLAY
/// already does it for these very kinds (`lifecycle.rs`, via the same `sync_bucket_index_object_blocks`).
/// The context write path was the one that did not.
///
/// The kinds and the maps below mirror `collect_model_live_block_entries` arm for arm, deliberately:
/// maintenance and rebuild then derive from the same source and cannot disagree about which kind a
/// block belongs to. `context_entity` composes its key from the collection key and the entity hash,
/// which is exactly the sort of detail a hand-written command-to-kind mapping gets wrong.
///
/// Returns whether anything was synced, so the caller can fall back to a rebuild for a write this
/// does not cover rather than silently leaving the index stale.
/// Keys `sync_context_blocks_for_object` found nothing for, recorded so they can be named.
///
/// One uncovered key forces a rebuild for the whole write, so what matters is WHICH keys are
/// uncovered, not how many. Reading the command list to guess at them has already been wrong more
/// than once in this area.
#[cfg(test)]
pub mod uncovered_maintenance {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    pub(super) static UNCOVERED_MAINTENANCE_KEYS: Mutex<Option<BTreeSet<String>>> =
        Mutex::new(None);

    pub(super) fn note(object_key: &str) {
        let mut guard = UNCOVERED_MAINTENANCE_KEYS.lock().expect("uncovered key tally poisoned");
        guard.get_or_insert_with(BTreeSet::new).insert(object_key.to_string());
    }

    pub fn reset() {
        *UNCOVERED_MAINTENANCE_KEYS.lock().expect("uncovered key tally poisoned") =
            Some(BTreeSet::new());
    }

    pub fn snapshot() -> Vec<String> {
        UNCOVERED_MAINTENANCE_KEYS
            .lock()
            .expect("uncovered key tally poisoned")
            .as_ref()
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }
}

/// Sync only the components a command actually wrote.
///
/// [`sync_context_blocks_for_object`] is given an object key and nothing else, so it re-upserts
/// EVERY field that object holds. For a record hash carrying one field per record that is one
/// upsert per stored record on every write, and each upsert removes and reinserts an entry in the
/// object's component vector -- two `Vec` shifts whose tails are the whole object. The cost is
/// quadratic in the object's field count.
///
/// Measured on a production-corpus one-box at a 260 MB store: a single-message ingest cost 40.1 s
/// of datanode CPU out of a 52 s wall, and a DWARF-unwound profile put 60.9% of engine CPU in
/// memmove under `remove_object_block_lookup_entry`, reached from here. With this path taken the
/// same ingest cost 0.13 s of datanode CPU.
///
/// The batch path already knows the exact `(kind, object_key, component)` a command wrote --
/// `command_upsert_components` computes it and the index-log delta has been built from it all
/// along. Given that list the maintenance is one upsert per WRITTEN component instead of one per
/// STORED component.
///
/// Returns false if any component's address is not in the shard maps; the caller then runs the
/// whole-object sync exactly as before, so a shape this does not model costs a fallback rather
/// than a stale index.
pub(super) fn sync_blocks_for_written_components(
    shard: &mut ShardState,
    shard_id: ShardId,
    components: &[(&'static str, String, Option<String>)],
) -> bool {
    if components.is_empty() {
        return false;
    }
    for (kind, object_key, component) in components {
        // Read the address back from the map the write just updated, exactly as
        // `collect_upsert_index_items` does, so the block filed here is the block a reload serves.
        let address = match (*kind, component.as_deref()) {
            ("hash", Some(field)) => shard
                .hashes
                .get(object_key)
                .and_then(|fields| fields.get(field))
                .cloned(),
            ("string", None) => shard.strings.get(object_key.as_str()).cloned(),
            _ => None,
        };
        let Some(address) = address else {
            return false;
        };
        // dirty: true, stage: false -- the flags the whole-object sync uses for a hash field: the
        // write staged its own outcome already and a second would have replay install the same
        // block twice.
        upsert_bucket_index_block_with(
            shard,
            shard_id,
            kind,
            object_key,
            component.clone(),
            address,
            true,
            false,
        );
    }
    true
}

pub(super) fn sync_context_blocks_for_object(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
) -> bool {
    // Read every kind's live addresses first, so the shared borrow ends before the sync below
    // takes a mutable one.
    let mut groups: Vec<(&'static str, String, Vec<BlockAddress>)> = Vec::new();

    if let Some(address) = shard.context_nodes.get(object_key) {
        groups.push(("context_node", object_key.to_string(), vec![address.clone()]));
    }
    for (kind, series) in [
        ("context_event", &shard.context_events),
        ("context_index", &shard.context_indexes),
        ("context_audit", &shard.context_audits),
        ("context_child", &shard.context_children),
        ("context_summary", &shard.context_summaries),
        ("context_compression", &shard.context_compressions),
    ] {
        if let Some(points) = series.get(object_key) {
            // Only the NEWEST block is filed. The loop below pops the last address and drops the
            // rest -- these kinds carry no component, so the index holds one ref per object -- so
            // finding the maximum is all this needs.
            //
            // Collecting every address the series ever held, into a HashSet and then a sorted Vec,
            // made a write carry the node's whole history: 8,322 bytes to add a summary to a node
            // holding 20, and 78,626 to add one to a node holding 320 -- 90.7% of the write, and
            // still climbing. The same defect was already fixed one level down, in the loop that
            // files these groups; the list it stopped filing was still being built here.
            //
            // The maximum of the series equals the last element of the deduplicated, sorted list,
            // so this is the same address by the same ordering, without the set or the sort.
            if let Some(newest) = points
                .values()
                .max_by(|left, right| {
                    left.block_slab_id()
                        .cmp(&right.block_slab_id())
                        .then(left.offset().cmp(&right.offset()))
                        .then(left.length().cmp(&right.length()))
                })
                .cloned()
            {
                groups.push((kind, object_key.to_string(), vec![newest]));
            }
        }
    }
    // Entities live grouped by node but index one entry per entity, under the composed key.
    if let Some(series) = shard.context_entities.get(object_key) {
        for (entity_hash, address) in series.iter() {
            groups.push((
                "context_entity",
                format!("{object_key}:{entity_hash}"),
                vec![address.clone()],
            ));
        }
    } else if let Some((collection_key, entity_hash)) = split_context_entity_key(object_key) {
        // A PERSISTED per-entity key names one entity inside a node's collection, and that is
        // what an entity write reports as its object key. The index groups entities under the
        // COLLECTION key, so the lookup above finds nothing, the object reports as uncovered,
        // and the caller rebuilds the whole bucket index -- on every entity upsert. Measured at
        // 4.27 MB per upsert into a 3,200-entity store, 99.8% of the write, all of it the
        // rebuild and its flag refresh.
        //
        // File the ONE block this key names. The composed key above is
        // `{collection_key}:{entity_hash}`, which IS this key, so both paths file the same shape.
        if let Some(address) = shard
            .context_entities
            .get(&collection_key)
            .and_then(|series| series.get(&entity_hash))
        {
            groups.push((
                "context_entity",
                object_key.to_string(),
                vec![address.clone()],
            ));
        }
    }

    // A context node's block lives in `shard.hashes` under a single field, so the rebuild derives
    // it as kind "hash" with that field as the component -- a different shape from the kinds
    // above, which carry no component. It is filed here the same way the rebuild would file it.
    //
    // Only the fields whose block is not already filed. This ran on every write and re-filed
    // EVERY field of the object each time -- cloning each field name to do it -- so writing a
    // hash cost work proportional to the fields it already had: 800 allocations per write at 100
    // fields, 8,388 at 1,600. Filtering before the clone makes the ordinary case, where the write
    // path already registered its own block, cost nothing here.
    //
    // `had_hash_blocks` still asks whether the object HAS hash blocks, not how many needed filing.
    // Those differ once the filter can empty the list, and answering the second question would
    // report an already-synced object as uncovered -- which sends the caller into a full rebuild.
    let had_hash_blocks = shard
        .hashes
        .get(object_key)
        .is_some_and(|fields| !fields.is_empty());
    let hash_fields: Vec<(String, BlockAddress)> = shard
        .hashes
        .get(object_key)
        .map(|fields| {
            fields
                .iter()
                .filter(|(field, address)| {
                    !shard.bucket_index.contains_object_block_address(
                        "hash",
                        object_key,
                        Some(field.as_str()),
                        address,
                    )
                })
                .map(|(field, address)| (field.clone(), address.clone()))
                .collect()
        })
        .unwrap_or_default();
    for (field, address) in hash_fields {
        // `stage: false` -- the write staged its own outcome under its own kind already, and a
        // second one would have replay install the same block twice.
        upsert_bucket_index_block_with(
            shard,
            shard_id,
            "hash",
            object_key,
            Some(field),
            address,
            true,
            false,
        );
    }

    if groups.is_empty() && !had_hash_blocks {
        #[cfg(test)]
        uncovered_maintenance::note(object_key);
        return false;
    }
    for (kind, key, mut live) in groups {
        // File the newest block, not every block the object has ever had.
        //
        // These kinds carry no component, so all of an object's blocks file under the same
        // (kind, key, None). `upsert_bucket_index_block_with` drops that entry's existing refs
        // before inserting, so filing a list leaves only its last element -- the other entries
        // are removed again on the way past. The index holds ONE ref per object here either way;
        // this reaches it without the removals and inserts in between.
        //
        // That is worth stating plainly because the list is the object's whole series and this
        // runs on every write: a node holding 850 events re-filed 850 blocks to add its 851st, so
        // filling a node cost the square of its length -- 2,072 allocations per message at 50
        // events, 23,822 at 800.
        //
        // Whether the index SHOULD hold every block of a series rather than the newest is a
        // separate question. It holds one today, and this keeps that.
        if live.len() > 1 {
            let newest = live.pop().expect("length checked");
            live.clear();
            live.push(newest);
        }
        sync_bucket_index_object_blocks(shard, shard_id, kind, &key, live, true);
    }
    true
}

/// THE MODEL KINDS A LIVE-BLOCK WALK CAN EMIT, AND THE BYTE EACH ONE PACKS AS, DECLARED ONCE.
///
/// WHAT THIS REPLACES, AND THE TWO SETS THAT WERE NEVER COMPARED. `storage_model_code` in
/// `storage_reporting.rs` hand-listed fifteen `&str` arms beside the walk below, which emits a
/// DIFFERENT fifteen. Both differences mattered, and in opposite ways:
///
///   * `zset` and `list` ARE emitted by the arms below and had no entry, so both fell through the
///     list's `_ => 0` arm. Zero is also what that arm handed a model id the reporting path
///     cannot name at all, so the packed block-index byte could not tell a zset block from a block
///     whose kind the engine does not recognise. Three different answers behind one byte.
///   * `sequence` and `context_embedding` had entries and are NOT emitted -- and those two are
///     not drift. Both are RETIRED spellings a stored index can still carry:
///     `ShardState::sequences` exists only to fold a pre-fold index's sequence map into
///     `features` at load, and `restore_model_maps_from_bucket_index` says in as many words that
///     `context_embedding` entries from pre-retirement indexes fall through to its ignore arm. An
///     index written before either retirement still loads, so a report over one still has to be
///     able to name what it finds. They are declared as retired rather than deleted, each with
///     its reason beside it, because an exemption list whose entries are not justified is a
///     hiding place.
///
/// HOW THE SET IS CLOSED AT COMPILE TIME, which is the part a table beside a walk cannot do.
/// [`visit_model_live_blocks`] hands `emit` a `ModelKind`, not a `&str`. So an arm added to the
/// walk cannot name a kind this declaration does not have -- that is a type error, not a missing
/// table row -- and each variant's report code comes off the same declaration as the variant, so
/// a kind that exists has a code by construction. Model ids are `&'static str` literals and this
/// is the only place they are written, which is what makes the set genuinely closed rather than
/// merely tidy.
///
/// THE ONE DIRECTION THE COMPILER CANNOT SEE is a variant no arm emits, and
/// `the_walk_emits_every_model_kind_the_registry_declares` drives it on a shard holding one block
/// in each map, comparing SETS by name rather than counting.
///
/// AND AN UNKNOWN NAME FAILS LOUDLY. [`model_report_code`] panics naming the model id rather
/// than returning 0. Every model id that reaches it came either from an arm below or from a
/// stored index this engine opened, so a name it cannot place is the engine and the store
/// disagreeing about what kinds exist -- and 0 is now reserved for the one thing the packed byte
/// still has to be able to say, which is that the bucket names no block at all.
macro_rules! model_kind_registry {
    (
        live { $($variant:ident = $name:literal @ $code:literal,)+ }
        retired { $($retired_variant:ident = $retired_name:literal @ $retired_code:literal,)+ }
    ) => {
        /// One model kind, spelled as the stored index spells it. Closed by declaration.
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub(super) enum ModelKind {
            $($variant,)+
        }

        impl ModelKind {
            /// Every kind that exists, in declaration order. Derived, so a variant cannot be
            /// missing from it.
            pub(super) const ALL: &'static [ModelKind] = &[$(ModelKind::$variant,)+];

            /// The stored spelling. This IS the `model_id` the index writes.
            pub(super) const fn as_str(self) -> &'static str {
                match self {
                    $(ModelKind::$variant => $name,)+
                }
            }

            /// The byte the packed reporting path writes for this kind.
            pub(super) const fn report_code(self) -> u8 {
                match self {
                    $(ModelKind::$variant => $code,)+
                }
            }

            /// The kind a stored `model_id` names, or `None` for a name no live arm emits.
            pub(super) fn from_stored_name(model_id: &str) -> Option<Self> {
                match model_id {
                    $($name => Some(ModelKind::$variant),)+
                    _ => None,
                }
            }
        }

        /// Spellings a stored index can still carry that no live arm emits, with the code each
        /// one already had. Not an escape hatch: adding a row here is a claim that an older
        /// index can hold the name, and each row carries the reason it can.
        pub(super) const RETIRED_MODEL_REPORT_CODES: &[(&str, u8)] =
            &[$(($retired_name, $retired_code),)+];

        /// EVERY SPELLING A STORED BLOCK ENTRY CAN CARRY, live and retired, in ONE BYTE.
        ///
        /// `ModelKind` is the LIVE walk's kind: `emit` hands one out, so it deliberately has no
        /// variant for a spelling no arm emits, and that is what makes the walk's set closed.
        /// A BLOCK ENTRY asks a different question. It is read back off a store this engine may
        /// not have written, and the two retired spellings are precisely the names such a store
        /// can still hold -- so an entry typed as `ModelKind` would be unable to represent a
        /// store that loads today. This enum is the entry's type: the SAME declaration, both
        /// halves, and a live kind converts into it infallibly.
        ///
        /// ONE BYTE, floored below, which is the whole point -- the entry used to spend sixteen
        /// on a fat pointer to a string drawn from this seventeen-element set.
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub(super) enum StoredModelKind {
            $($variant,)+
            $($retired_variant,)+
        }

        impl StoredModelKind {
            /// Every spelling that exists, live then retired, in declaration order.
            pub(super) const ALL: &'static [StoredModelKind] =
                &[$(StoredModelKind::$variant,)+ $(StoredModelKind::$retired_variant,)+];

            /// The stored spelling. This IS what the index writes and reads back.
            pub(super) const fn as_str(self) -> &'static str {
                match self {
                    $(StoredModelKind::$variant => $name,)+
                    $(StoredModelKind::$retired_variant => $retired_name,)+
                }
            }

            /// The byte the packed reporting path writes for this spelling.
            pub(super) const fn report_code(self) -> u8 {
                match self {
                    $(StoredModelKind::$variant => $code,)+
                    $(StoredModelKind::$retired_variant => $retired_code,)+
                }
            }

            /// The spelling a stored `model_id` names, or `None` for one NEITHER half declares.
            /// Every caller turns that `None` into a refusal that names the spelling.
            pub(super) fn from_stored_name(model_id: &str) -> Option<Self> {
                match model_id {
                    $($name => Some(StoredModelKind::$variant),)+
                    $($retired_name => Some(StoredModelKind::$retired_variant),)+
                    _ => None,
                }
            }

            /// Whether NO live arm emits this spelling. Derived as the complement of the live
            /// half rather than listed, so it cannot disagree with the declaration.
            pub(super) const fn is_retired(self) -> bool {
                match self {
                    $(StoredModelKind::$variant => false,)+
                    $(StoredModelKind::$retired_variant => true,)+
                }
            }
        }

        /// A live kind IS a stored spelling. Infallible, and exhaustive over `ModelKind`, so a
        /// variant added to the walk cannot fail to have an entry spelling.
        impl From<ModelKind> for StoredModelKind {
            fn from(kind: ModelKind) -> Self {
                match kind {
                    $(ModelKind::$variant => StoredModelKind::$variant,)+
                }
            }
        }

        /// FLOORS ON THE DERIVATION. The point of deriving the registry is that it cannot go
        /// stale beside the walk; the point of these is that it cannot go EMPTY either. A
        /// declaration that lost rows would otherwise compile, and a registry of one kind maps
        /// every other kind onto the panic below rather than onto a code.
        const _: () = assert!(ModelKind::ALL.len() >= 15);
        const _: () = assert!(RETIRED_MODEL_REPORT_CODES.len() >= 2);
        /// The entry's spelling is BOTH halves, so its count is floored against the live half
        /// rather than on its own: a retired row lost would otherwise still satisfy a bare
        /// `>= 17`, and a store that carries the name would stop loading.
        const _: () = assert!(StoredModelKind::ALL.len() == ModelKind::ALL.len() + RETIRED_MODEL_REPORT_CODES.len());
        const _: () = assert!(StoredModelKind::ALL.len() >= 17);
        /// ONE BYTE. The reason the entry can hold it where it held a fat pointer.
        const _: () = assert!(std::mem::size_of::<StoredModelKind>() == 1);

        /// NO CODE IS 0, AND NO TWO CODES COLLIDE -- live and retired counted together, because
        /// the reporting path reads one byte and does not know which list answered. A duplicate
        /// would put two kinds back behind one value, which is the defect this registry exists
        /// to remove; a 0 would collide with "this bucket names no block".
        const _: () = {
            let codes: &[u8] = &[$($code,)+ $($retired_code,)+];
            let mut left = 0;
            while left < codes.len() {
                assert!(codes[left] != 0);
                let mut right = left + 1;
                while right < codes.len() {
                    assert!(codes[left] != codes[right]);
                    right += 1;
                }
                left += 1;
            }
        };
    };
}

model_kind_registry! {
    live {
        String = "string" @ 1,
        Hash = "hash" @ 2,
        Set = "set" @ 3,
        Feature = "feature" @ 4,
        ControlState = "control_state" @ 7,
        ContextNode = "context_node" @ 8,
        ContextEvent = "context_event" @ 9,
        ContextIndex = "context_index" @ 10,
        ContextAudit = "context_audit" @ 11,
        ContextEntity = "context_entity" @ 13,
        ContextChild = "context_child" @ 14,
        ContextSummary = "context_summary" @ 16,
        ContextCompression = "context_compression" @ 17,
        // THE TWO THE PACKED REPORT COULD NOT NAME. 6 and 12 are the two codes the old table
        // skipped, so every code any engine has ever written keeps the meaning it had and these
        // two stop sharing a byte with "unknown".
        Zset = "zset" @ 6,
        List = "list" @ 12,
    }
    retired {
        // Sequence is Feature with a typed row codec over identical timestamped-KV storage; the
        // fold moved its data into `features` and left `ShardState::sequences` behind only to
        // fold a pre-fold on-disk index at load. No arm emits it; an index older than the fold
        // still names it.
        Sequence = "sequence" @ 5,
        // The rows `context_embedding` addressed have no readers left, and
        // `restore_model_maps_from_bucket_index` drops the entries on purpose. An index written
        // before that retirement still carries them, and a report over one still has to name
        // the kind rather than calling it unknown.
        ContextEmbedding = "context_embedding" @ 15,
    }
}

/// The one-byte spelling for a kind THIS ENGINE spells itself, refusing loudly if the registry
/// does not declare it.
///
/// Every caller passes a `&'static str` literal from a command arm or a walk, so a refusal here
/// is a kind the engine writes and the registry has never heard of -- the same disagreement
/// [`model_report_code`] refuses, caught at the write rather than at the report. Returning some
/// default variant instead would file the block under a kind it does not have.
pub(super) fn stored_model_kind(kind: &str) -> StoredModelKind {
    StoredModelKind::from_stored_name(kind).unwrap_or_else(|| {
        panic!(
            "no stored model kind for model id {kind:?}. This spelling is written by this engine \
             and the registry does not declare it, so the page would be filed under a kind it \
             does not have. Declare it in `model_kind_registry`: under `live` if an arm of \
             `visit_model_live_blocks` emits it, under `retired` -- with the reason an older \
             index can carry it -- if not."
        )
    })
}

/// Reads as the stored spelling, so a message that used to interpolate the `Arc<str>` still
/// says the same word.
impl std::fmt::Display for StoredModelKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// THE WIRE DOES NOT MOVE. A block entry's model spelling is written as the STRING it has always
/// been written as, and read back as one; only the in-memory width changes, from a sixteen-byte
/// fat pointer to one byte.
///
/// That is deliberate and it is the cheap half of the change. #1969 found the index log packs
/// POSITIONALLY -- a plain `rmp_serde::Serializer`, no field names -- so every field after a
/// changed one shifts, and a store written by an older engine would mis-parse. Keeping the
/// spelling on the wire means there is no migration to get wrong, no decoy field to pay for, and
/// `core_index_loads_legacy_bucket_page_field_names` keeps asserting what it always asserted.
impl serde::Serialize for StoredModelKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

/// AND AN UNKNOWN SPELLING FAILS LOUDLY, NAMING IT.
///
/// This is the one direction that must not be quiet. A block entry names the object a block belongs
/// to; a spelling silently mapped onto some default variant would file the block under a kind it
/// does not have, and a block filed under the wrong kind is still on its slab and nothing looks
/// for it. That is silent corruption, not a failing test.
///
/// It refuses the same way [`model_report_code`] refuses, for the same reason and with the same
/// instruction: a spelling neither half of `model_kind_registry` declares is the engine and the
/// store disagreeing about which kinds exist, and the fix is a declared row, not a fallback.
impl<'de> serde::Deserialize<'de> for StoredModelKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let spelling = <std::borrow::Cow<'de, str> as serde::Deserialize>::deserialize(deserializer)?;
        match StoredModelKind::from_stored_name(&spelling) {
            Some(kind) => Ok(kind),
            None => panic!(
                "no stored model kind for model id {spelling:?}. A page entry's model spelling \
                 comes either from a live-page walk arm or from a stored index this engine \
                 opened, so a name that cannot be placed is the engine and the store disagreeing \
                 about which kinds exist -- and mapping it onto a kind it does not have would \
                 file the page under the wrong object. Declare it in `model_kind_registry`: \
                 under `live` if an arm of `visit_model_live_blocks` emits it, under `retired` -- \
                 with the reason an older index can carry it -- if not."
            ),
        }
    }
}

/// The byte the packed reporting path writes for a stored `model_id`.
///
/// ONE LOOKUP OVER BOTH HALVES, then a refusal that NAMES the id. The refusal is the behaviour
/// change #1970 made: this used to be a `_ => 0` arm, and 0 is the same byte the packed node writes
/// for a bucket that holds no block at all, so an unrecognised kind was reported as an absence. A
/// caller cannot handle what it cannot see.
///
/// IT USED TO BE TWO LOOKUPS -- `ModelKind::from_stored_name` and then a linear scan of
/// `RETIRED_MODEL_REPORT_CODES` -- because the live set and the retired set were two different
/// shapes. `StoredModelKind` is both halves of the declaration in one closed enum, so this is now a
/// single match over seventeen spellings and the retired half is no longer a scan.
///
/// A MUTATION RUN IS WHY THIS CHANGED. Mutating `StoredModelKind::report_code` so every spelling
/// packed as the same byte killed nothing, because at the time this function read
/// `ModelKind::report_code` and the new accessor had no production caller at all -- a second
/// derivation of the report byte that nothing reached. Routing the report through it makes the
/// accessor reachable, and `a_zset_and_a_list_page_pack_as_their_own_kind_and_not_as_the_empty_code`
/// then kills that mutant: it derives its expectation from `ModelKind::report_code`, which is a
/// genuinely independent derivation rather than the same function compared against itself.
pub(super) fn model_report_code(model_id: &str) -> u8 {
    if let Some(kind) = StoredModelKind::from_stored_name(model_id) {
        return kind.report_code();
    }
    panic!(
        "no packed report code for model id {model_id:?}. Every model id reaching here comes \
         either from a live-page walk arm or from a stored index this engine opened, so a name \
         that cannot be placed is the engine and the store disagreeing about which kinds exist. \
         Declare it in `model_kind_registry`: under `live` if an arm of `visit_model_live_blocks` \
         emits it, under `retired` -- with the reason an older index can carry it -- if not."
    );
}

/// Offer every live model-map block in the shard to `accept`, and hand the accepted ones to
/// `emit`.
///
/// ONE arm list, for every caller that asks the model maps what blocks are live.
/// `collect_model_live_block_entries` is this walk with `accept` always true; the release and
/// reload paths pass a routing-bucket filter. Sharing the arms matters more than it looks: a kind
/// present in one hand-written arm list and missing from another would make the release's "what a
/// reload would rebuild" derivation disagree with what the reload actually rebuilds, and it would
/// disagree in the direction that silently ALLOWS a release rather than refusing one.
///
/// `accept` is given the OBJECT KEY AND THE ADDRESS, and the key is what a routing-bucket filter
/// needs now that a block's bucket is `block_routing_bucket(object_key, ..)` rather than a field on
/// the address. Everything owned is still built after it: several arms compose their component with
/// `format!` or `hex::encode`, and a `LiveBlockEntry` costs four allocations -- an owned key and an
/// owned kind, each built as a `String` and then copied into an `Arc<str>`. None of that runs for a
/// block the caller is not going to keep.
///
/// ONE ARM PAYS FOR THE KEY EARLIER THAN IT DID. `context_entities` emits a COMPOSED key
/// (`{collection_key}:{entity_hash}`), and that string is what the block is filed under -- so a
/// bucket filter has to see it. The `format!` therefore moves ahead of `accept` on that arm only,
/// which means a bucket-scoped walk now builds one `String` for an entity it goes on to reject.
/// Every other arm's key is borrowed from the map and costs nothing.
///
/// The timestamped-series kinds dedup and sort their addresses, and that helper allocates, so the
/// series is first asked -- without allocating -- whether ANY of its addresses is accepted. The
/// addresses emitted, and their order within a series, are unchanged.
/// Walk every live model-map block, and CHARGE what the walk materializes.
///
/// The charge is here, not at the call sites, and `tally` has no uncounted variant -- so a new
/// way of walking the model maps cannot compile without saying which counter it belongs to.
/// That is the whole point: [`LIVE_BLOCK_SCAN_ENTRIES`] was charged by one wrapper while seven
/// production call sites reached the same walks around it, and nothing failed.
#[track_caller]
fn visit_model_live_blocks(
    shard: &ShardState,
    tally: ModelWalkTally,
    accept: impl Fn(&str, &BlockAddress) -> bool,
    mut emit: impl FnMut(ModelKind, &str, Option<&str>, &BlockAddress),
) {
    // THE ARM LIST. Nested so that `visit_model_live_blocks` is the only thing in the tree that
    // can reach it, and so the count wraps all fifteen arms at once instead of being repeated
    // in each -- an arm added below is charged without being told to be.
    //
    // The kind is a `ModelKind`, not a string literal: see `model_kind_registry` above for why
    // an arm that could name its own kind is an arm the reporting registry can fall behind.
    fn arms(
        shard: &ShardState,
        accept: impl Fn(&str, &BlockAddress) -> bool,
        mut emit: impl FnMut(ModelKind, &str, Option<&str>, &BlockAddress),
    ) {
        for (key, address) in &shard.strings {
            if accept(key, address) {
                emit(ModelKind::String, key, None, address);
            }
        }
        for (key, fields) in &shard.hashes {
            for (field, address) in fields.iter() {
                if accept(key, address) {
                    emit(ModelKind::Hash, key, Some(field.as_str()), address);
                }
            }
        }
        for (key, members) in &shard.zsets {
            for (member, (biased, address)) in members.iter() {
                if accept(key, address) {
                    let component = format!("{biased:016x}{}", hex::encode(member));
                    emit(ModelKind::Zset, key, Some(component.as_str()), address);
                }
            }
        }
        for (key, elements) in &shard.lists {
            for (seq, address) in elements.iter() {
                if accept(key, address) {
                    let component = format!("{:016x}", (*seq as u64).wrapping_sub(i64::MIN as u64));
                    emit(ModelKind::List, key, Some(component.as_str()), address);
                }
            }
        }
        for (key, members) in &shard.sets {
            for (member, address) in members.iter() {
                if accept(key, address) {
                    let component = hex::encode(member);
                    emit(ModelKind::Set, key, Some(component.as_str()), address);
                }
            }
        }
        visit_timestamped_series(&shard.features, ModelKind::Feature, &accept, &mut emit);
        for (key, address) in &shard.control_state_blocks {
            if accept(key, address) {
                emit(ModelKind::ControlState, key, None, address);
            }
        }
        for (key, address) in &shard.context_nodes {
            if accept(key, address) {
                emit(ModelKind::ContextNode, key, None, address);
            }
        }
        visit_timestamped_series(
            &shard.context_events,
            ModelKind::ContextEvent,
            &accept,
            &mut emit,
        );
        visit_timestamped_series(
            &shard.context_indexes,
            ModelKind::ContextIndex,
            &accept,
            &mut emit,
        );
        visit_timestamped_series(
            &shard.context_audits,
            ModelKind::ContextAudit,
            &accept,
            &mut emit,
        );
        // Entities live grouped by node in memory but persist one entry per entity, under the same
        // `ctx:entity:{tenant}:{node}:{entity_hash}` key as before the fold -- the collection key
        // plus the BTree key reproduce it exactly. Keeping the on-disk key per entity is what makes
        // this change format-compatible in both directions.
        for (collection_key, series) in &shard.context_entities {
            for (entity_hash, address) in series.iter() {
                // COMPOSED BEFORE ACCEPT, and only on this arm. The block is filed under this
                // composed key, so it is the string a bucket filter has to hash; see the note on
                // this function for what that costs.
                let composed = format!("{collection_key}:{entity_hash}");
                if accept(composed.as_str(), address) {
                    emit(ModelKind::ContextEntity, composed.as_str(), None, address);
                }
            }
        }
        visit_timestamped_series(
            &shard.context_children,
            ModelKind::ContextChild,
            &accept,
            &mut emit,
        );
        visit_timestamped_series(
            &shard.context_summaries,
            ModelKind::ContextSummary,
            &accept,
            &mut emit,
        );
        visit_timestamped_series(
            &shard.context_compressions,
            ModelKind::ContextCompression,
            &accept,
            &mut emit,
        );
    }

    // WHAT THE WALK LOOKED AT, as opposed to what it kept. `accept` runs on every live address
    // in the shard whatever the caller asked for, so this is the term that tracks the store; the
    // three tallies below track what came out. Counted in a local cell and charged once, so the
    // walk pays one atomic rather than one per address.
    let visited = std::cell::Cell::new(0u64);
    let counting_accept = |object_key: &str, address: &BlockAddress| {
        visited.set(visited.get().saturating_add(1));
        accept(object_key, address)
    };
    let mut emitted = 0usize;
    arms(
        shard,
        counting_accept,
        |kind, object_key, component, address| {
            emitted += 1;
            emit(kind, object_key, component, address);
        },
    );
    MODEL_MAP_ADDRESSES_VISITED.fetch_add(visited.get(), std::sync::atomic::Ordering::Relaxed);
    match tally {
        ModelWalkTally::WholeShardEntries => note_live_block_scan(emitted),
        ModelWalkTally::BucketScopedEntries => {
            BUCKET_SCOPED_MODEL_ENTRIES
                .fetch_add(emitted as u64, std::sync::atomic::Ordering::Relaxed);
        }
        ModelWalkTally::PromotionCheckPages => {
            PROMOTE_MODEL_MAP_PAGES
                .fetch_add(emitted as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// One timestamped-series map's arm of [`visit_model_live_blocks`].
///
/// `unique_timestamped_kv_block_addresses` builds a set and a sorted `Vec` for the series, so it
/// is reached only once the series is known to hold at least one accepted address. Asking that
/// first costs a field read per point and no allocation, and the answer it gates on is exactly the
/// answer the caller would otherwise reach after allocating.
fn visit_timestamped_series(
    map: &HashMap<String, BTreeMap<u64, BlockAddress>>,
    kind: ModelKind,
    accept: &impl Fn(&str, &BlockAddress) -> bool,
    emit: &mut impl FnMut(ModelKind, &str, Option<&str>, &BlockAddress),
) {
    for (key, series) in map {
        if !series.values().any(|address| accept(key, address)) {
            continue;
        }
        for address in unique_timestamped_kv_block_addresses(series) {
            if accept(key, &address) {
                emit(kind, key, None, &address);
            }
        }
    }
}

#[track_caller]
pub(super) fn collect_model_live_block_entries(shard: &ShardState) -> Vec<LiveBlockEntry> {
    let mut entries = Vec::new();
    visit_model_live_blocks(
        shard,
        ModelWalkTally::WholeShardEntries,
        |_, _| true,
        |kind, object_key, component, address| {
            entries.push(live_block_entry(
                object_key.to_string(),
                kind.as_str(),
                component.map(str::to_string),
                address.clone(),
            ));
        },
    );
    entries
}

/// The live model-map blocks routing to ONE bucket.
///
/// What [`reload_released_bucket`] needs, and all it ever needed: it walked the whole shard into
/// owned entries and then dropped every one that did not route here. The walk is still the whole
/// shard -- see [`visit_model_live_blocks`] for why nothing can answer this from an index -- but
/// what it MATERIALIZES is this bucket's blocks.
pub(super) fn collect_model_live_block_entries_in_bucket(
    shard: &ShardState,
    routing_bucket: u32,
) -> Vec<LiveBlockEntry> {
    let mut entries = Vec::new();
    // THE SHARD'S OWN RANGE, so that "routes to this bucket" means the same thing here as it does
    // at the site that FILED the block. This used to read the bucket off the address; the address
    // does not carry one, and the range a store is loaded on is the range it was built on -- a
    // disagreeing stamp is refused before the decode, so this is the same number the writer
    // stamped rather than a second opinion about it.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    visit_model_live_blocks(
        shard,
        ModelWalkTally::BucketScopedEntries,
        |object_key, _| {
            block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket)
                == routing_bucket
        },
        |kind, object_key, component, address| {
            entries.push(live_block_entry_filed(
                object_key.to_string(),
                kind.as_str(),
                component.map(str::to_string),
                address.clone(),
                routing_bucket,
            ));
        },
    );
    entries
}

/// What a reload would rebuild for each of the WANTED buckets, and nothing else.
///
/// The set [`release_bucket_blocks`] compares each candidate's resident blocks against. Two
/// directions are being asked at once, and only one of them is answerable per victim:
///
///   * resident is contained in derived -- every resident block is still live in the model maps at
///     the same address. A per-victim question: look each resident block up in its own map.
///   * derived is contained in resident -- no live model-map block routes to this bucket without
///     being resident in it. NOT a per-victim question. The maps are keyed by object key; the
///     routing bucket is a field of the address; `object_block_lookup` is derived from the very
///     block index being checked and so answers with it rather than about it. Finding a block that
///     routes here and is absent from the block index means looking at blocks the block index does
///     not name, and the only place they are is the maps.
///
/// So the walk stays. What goes is materializing the store to do it: `accept` runs on the address,
/// before any key, component or entry is built, and only a wanted block is ever turned into an
/// identity.
fn derive_released_block_identities(
    shard: &ShardState,
    wanted: &BTreeSet<u32>,
) -> BTreeMap<u32, BTreeSet<ReleasedBlockIdentity>> {
    let mut derived: BTreeMap<u32, BTreeSet<ReleasedBlockIdentity>> = BTreeMap::new();
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    visit_model_live_blocks(
        shard,
        ModelWalkTally::BucketScopedEntries,
        |object_key, _| {
            wanted.contains(&block_routing_bucket(
                object_key,
                start_routing_bucket,
                end_routing_bucket,
            ))
        },
        |kind, object_key, component, address| {
            let routing_bucket =
                block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
            derived
                .entry(routing_bucket)
                .or_default()
                .insert(released_block_identity_owned(
                    kind.as_str().to_string(),
                    object_key.to_string(),
                    component.map(str::to_string),
                    address,
                ));
        },
    );
    derived
}

/// SIX TERMS, NOT SEVEN. The routing bucket left the address, and with it this key's copy of it.
///
/// It was never discriminating here: this key dedupes the addresses of ONE object key inside one
/// publish, and every address of one key routes to one bucket. A term equal across every element of
/// the set it partitions cannot split it.
pub(super) fn block_physical_identity_key(
    address: &BlockAddress,
) -> (u64, u64, u64, Option<u64>, Option<u64>) {
    (
        address.block_slab_id(),
        address.offset(),
        address.length(),
        address.block_id(),
        // The object id was a term here and is not one any more: an address does not carry one, and
        // this key dedupes the addresses of ONE object key inside one publish, so a term equal
        // across every member of the set it partitions could not split it either way.
        address.generation(),
    )
}

pub(super) fn upsert_bucket_index_block(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    component: Option<String>,
    address: BlockAddress,
    dirty: bool,
) {
    upsert_bucket_index_block_with(shard, shard_id, kind, object_key, component, address, dirty, true)
}

/// File the entry that keeps a removal's TOMBSTONE PAGE reachable.
///
/// # WHY NOT [`upsert_bucket_index_block`]
///
/// Three of the things that function does are wrong for a tombstone, and each would be a defect
/// rather than an inefficiency:
///
///   * it STAGES AN OUTCOME. `mark_bucket_index_block_deleted_recording` has already staged one for
///     this removal, carrying this very address; a second would replay as a page installed twice.
///   * it CLEARS `deleted_object_index` for the object. A removal is not a re-add -- that line
///     exists because writing a member back must clear the tombstone the removal filed -- so a
///     tombstone page clearing it would undo the object deletion the removal just recorded.
///   * it DROPS the object's existing entry for this component first. There is none left; the
///     `retain` in the caller took it, which is the whole reason this runs afterwards.
///
/// # AND IT DOES NOT ENTER `object_block_lookup`
///
/// `insert_object_block_lookup` returns early on a deleted page and always has, so calling it would
/// be a no-op -- it is called anyway, deliberately, so that the skip lives in the one function that
/// owns the lookup's rules rather than being a condition this site remembers. What follows from the
/// skip is the load-bearing part: the fast per-object reader `bucket_index_component_block_addresses`
/// resolves THROUGH that lookup, so a tombstone entry is invisible to it and every index answer is
/// unchanged. A derivation therefore has to walk `block_index` itself, which is what
/// `container_membership` does and what makes its walk O(pages) rather than O(elements of one
/// object).
/// `routing_bucket` IS THE ONE THE REMOVED ENTRY WAS FILED IN, handed in by the caller that took it.
/// It must not be recomputed here: `block_routing_bucket(key, 0, u32::MAX)` -- the form the removal's
/// WAL outcome uses -- is a DIFFERENT number from the shard's own range, and filing the tombstone
/// under it put the entry in a bucket holding nothing else for the object. That made the bucket
/// all-tombstone (so a twelve-member set reported as a deleted object) and made its filed bucket
/// disagree with the one its key routes to (so a compaction round refused with
/// `page_compaction_owner_mismatch` on any container that had had a removal). Both were driven.
pub(super) fn insert_container_tombstone_entry(
    shard: &mut ShardState,
    kind: &str,
    object_key: &str,
    component: &str,
    address: BlockAddress,
    routing_bucket: u32,
) {
    // THE ADDRESS NO LONGER CARRIES AN IDENTITY TO STAMP. This stood here as
    // `address.set_object_id(Some(stable_block_object_id(shard_id, kind, object_key)))`, so that
    // `object_id()` on the entry could read straight through to the field. The entry derives it
    // now, from `model_id` and `object_key` -- which on this path are `kind` and `object_key`, the
    // same two terms that fed the stamp -- so the value is unchanged by construction and the shard
    // is no longer needed here to produce it.
    let page = BlockIndex {
        object_key: std::sync::Arc::from(object_key),
        model_id: stored_model_kind(kind),
        component: Some(std::sync::Arc::from(component)),
        address,
        dirty: true,
        deleted: true,
        log_backed: false,
    };
    let bucket = shard
        .bucket_index
        .bucket_map
        .entry(routing_bucket)
        .or_insert_with(|| BucketNode {
            routing_bucket,
            flags: BucketFlags::default()
                .with(BucketFlags::META_LOADED, true)
                .with(BucketFlags::IN_MEMORY, true),
            ..BucketNode::default()
        });
    bucket.set_dirty(true);
    // THE BUCKET IS NOT EMPTY ANY MORE. The caller ran `set_deleted(block_index.is_empty())` before
    // this entry existed, so a bucket whose last live entry was the one just removed is marked
    // deleted while it is about to hold a page. A deleted bucket is not dumped, and the tombstone
    // would not survive a reload -- which is the one place it has to survive.
    bucket.set_deleted(false);
    bucket.set_in_memory(true);
    bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
    let block_ref_key = bucket
        .block_index
        .insert(page.clone(), &mut shard.bucket_index.block_slab_live);
    if let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) {
        classify_bucket_layout_in_place(bucket);
    }
    shard
        .bucket_index
        .insert_object_block_lookup(routing_bucket, block_ref_key, &page);
    shard.buckets_pending_flag_refresh.insert(routing_bucket);
}

/// The same, with a say over whether an outcome is staged for the record.
///
/// A block write produces an outcome, and this is where that outcome is produced -- so a caller
/// that WRITES a block wants `stage: true`, which is every existing caller.
///
/// Maintenance is different: the context write has already staged its own outcome, under its own
/// kind. Registering the block it produced must not put a SECOND outcome in the log, because replay
/// would then install the same block twice under two kinds. `stage: false` says "file this block in
/// the index; the record already knows about it".
#[allow(clippy::too_many_arguments)]
pub(super) fn upsert_bucket_index_block_with(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    component: Option<String>,
    address: BlockAddress,
    dirty: bool,
    stage: bool,
) {
    // Every single-block writer reaches the bucket index through here, so the charge sits here and
    // not at the arms. A new command arm that files a block is counted because this function counts
    // it; the outcome staged in the middle re-tags itself, so the two do not overlap.
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::BucketIndex, || {
        upsert_bucket_index_block_inner(
            shard,
            shard_id,
            kind,
            object_key,
            component,
            address,
            dirty,
            stage,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn upsert_bucket_index_block_inner(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    component: Option<String>,
    address: BlockAddress,
    dirty: bool,
    stage: bool,
) {
    // THE SHARD'S OWN RANGE, carried on the shard. This site PLACES: `routing_bucket` below is
    // the KEY this block is filed under, not a filter over an answer already decided. Under the
    // whole range an unrouted block went into a bucket a `0..1023` shard does not hold -- filed
    // where nothing scoped to the shard will look for it. An unstamped state still answers the
    // whole range, which is what this line passed unconditionally before.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let routing_bucket = block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    // Filing a block into a RELEASED bucket would leave the node holding one block and claiming to
    // be resident, with the rest of its blocks still only in the model maps -- neither released
    // nor whole. Load it back first; a no-op for every bucket that was never released.
    reload_released_bucket(shard, shard_id, routing_bucket);
    let object_id = stable_block_object_id(shard_id, kind, object_key);
    // This IS the outcome: an object, its identity, and where its block ended up. Put it aside
    // for the record, so replay has the option of installing it instead of re-running the
    // command that produced it.
    if stage {
        // The item is BUILT here and staged there, and both halves belong to the record rather
        // than to the index -- so the class covers the construction too. Without this the three
        // owned strings an outcome carries would be charged to the bucket index, which is the
        // shape of mis-attribution that makes an index look like it is growing when a log is.
        crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::StagedOutcome, || {
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
        });
    }
    // One allocation of this object's identity, shared by the block entry, the lookup, and every
    // OTHER block already filed under the same object. Taken before the `&mut` borrows below.
    //
    // Only the first block of an object allocates. A container key's hundredth field now points at
    // the copy its first field made, where before each of the hundred made its own.
    let shared_object_key = shard
        .bucket_index
        .object_block_lookup
        .shared_object_key(kind, object_key)
        .unwrap_or_else(|| Arc::from(object_key));
    let entry = LiveBlockEntry {
        object_key: shared_object_key,
        kind: stored_model_kind(kind),
        component: component
            .map(|name| crate::engine::state::intern_shared(&mut shard.bucket_index.kind_pool, &name)),
        log_backed: address.block_id().is_none(),
        address,
        dirty,
        deleted: false,
        // Where this upsert is about to file it, computed ten lines up.
        filed_routing_bucket: routing_bucket,
        filing_is_known: true,
    };
    // Buckets whose blocks this upsert disturbs. Collected while the bucket borrows are live and
    // recorded once they end, so the per-write refresh can skip the rest of the shard.
    let mut touched_buckets: Vec<u32> = Vec::new();
    let lookup_enabled = !shard.bucket_index.object_block_lookup.is_empty();
    let direct_block_refs = if lookup_enabled {
        shard
            .bucket_index
            .block_refs_for(entry.kind.as_str(), &entry.object_key, entry.component.as_deref())
            .map(<[crate::engine::state::BlockLookupRef]>::to_vec)
    } else {
        None
    };
    shard.bucket_index.remove_object_block_lookup_entry(
        entry.kind.as_str(),
        &entry.object_key,
        entry.component.as_deref(),
    );
    if let Some(block_refs) = direct_block_refs {
        for block_ref in block_refs {
            let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&block_ref.routing_bucket) else {
                continue;
            };
            touched_buckets.push(block_ref.routing_bucket);
            let removed_object_id = bucket
                .block_index
                .remove(&block_ref.block_ref_key, &mut shard.bucket_index.block_slab_live)
                .map(|page| page.object_id(shard_id));
            if let Some(removed_object_id) = removed_object_id {
                if !bucket
                    .block_index
                    .values()
                    .any(|page| page.object_id(shard_id) == removed_object_id)
                {
                    bucket.object_index.remove(&removed_object_id);
                }
                classify_bucket_layout_in_place(bucket);
            }
        }
    } else if !lookup_enabled {
        let CoreIndex {
            bucket_map,
            block_slab_live: live,
            ..
        } = &mut shard.bucket_index;
        for (routing_bucket, bucket) in bucket_map.iter_mut() {
            note_site(&bucket_visit_sites::REMOVE_ALL_BUCKETS, bucket.block_index.len());
            touched_buckets.push(*routing_bucket);
            bucket.block_index.retain(&mut *live, |_, page| {
                !(page.object_key == entry.object_key
                    && page.model_id == entry.kind
                    && page.component.as_deref() == entry.component.as_deref())
            });
            if !bucket
                .block_index
                .values()
                .any(|page| page.object_id(shard_id) == object_id)
            {
                bucket.object_index.remove(&object_id);
            }
            classify_bucket_layout_in_place(bucket);
        }
    }
    // AND THE TOMBSTONE FOR THIS COMPONENT, WHICH NEITHER BRANCH ABOVE CAN REACH.
    //
    // UNCONDITIONAL, AND THE CONDITIONAL VERSIONS WERE BOTH WRONG. A re-add must clear the tombstone
    // its element left, and that is what bounds the cost of retaining one at ONE ENTRY PER DISTINCT
    // ELEMENT REMOVED rather than one per removal. There are THREE cases above, not two:
    //
    //   * lookup established AND it names a live ref for this component -> the first branch removes
    //     that ref, and a tombstone is never IN the lookup (`insert_object_block_lookup` returns early
    //     on a deleted page), so the tombstone survives it;
    //   * lookup NOT established -> the second branch's `retain` matches on the component and does
    //     take the tombstone with it;
    //   * LOOKUP ESTABLISHED AND IT NAMES NOTHING for this component -- which is EXACTLY the re-add
    //     case, because the removal dropped the live ref -- so `direct_block_refs` is `None`, the
    //     `else if !lookup_enabled` guard is false, and NEITHER BRANCH RUNS AT ALL.
    //
    // The third case is the one that was leaking, and putting the sweep inside the first branch did
    // not fix it: I tried that and the guard stayed red, which is what showed there was a third case
    // rather than two. So it runs unconditionally, where its own predicate is the only condition.
    //
    // Swept in the TARGET bucket only: a tombstone is filed in the bucket its live entry was removed
    // from, and that is the bucket this write routes to.
    //
    // ASKED BEFORE IT IS DONE, because this runs on EVERY page write. `BlockIndexMap::retain` walks
    // the bucket and then `shrink`s it, which can reallocate, and paying that per write for a
    // tombstone that is almost never there would be a new cost on the hot path. A short-circuiting
    // `any` is the same ORDER as the two `!any(|page| page.object_id() == ..)` scans the branches
    // above already do over the same bucket, so the common case adds a scan and not an allocation.
    let sweep = shard
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .is_some_and(|bucket| {
            bucket.block_index.values().any(|page| {
                page.deleted
                    && page.object_key == entry.object_key
                    && page.model_id == entry.kind
                    && page.component.as_deref() == entry.component.as_deref()
            })
        });
    if sweep {
        if let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) {
            bucket
                .block_index
                .retain(&mut shard.bucket_index.block_slab_live, |_, page| {
                    !(page.deleted
                        && page.object_key == entry.object_key
                        && page.model_id == entry.kind
                        && page.component.as_deref() == entry.component.as_deref())
                });
            touched_buckets.push(routing_bucket);
            classify_bucket_layout_in_place(bucket);
        }
    }
    let mut block_ref_key: u64 = 0;
    // The address carries no object id to be given: the entry derives one from its own terms, so
    // there is no second copy to keep in step.
    let address = entry.address;
    let block_index = BlockIndex {
        object_key: entry.object_key,
        model_id: entry.kind,
        component: entry.component.clone(),
        address,
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
                flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
                ..BucketNode::default()
            });
        bucket.set_dirty(bucket.dirty() | dirty);
        bucket.set_deleted(false);
        if dirty {
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
        }
        bucket.set_in_memory(true);
        bucket.object_index.insert(object_id);
        // A RE-ADD CLEARS THE TOMBSTONE, and this was the one door that did not.
        //
        // Every per-element removal -- `ZSetRemove`, `SetRemove`, `ListPop`, `HashDelete` -- goes
        // through `mark_bucket_index_block_deleted`, which drops the block and files the object id
        // in `deleted_object_index`. Writing the member back files a LIVE block here. Leave the id
        // behind and `object_manager::runtime_report` asks `deleted_object_index.contains` beside
        // that live block, calls the object deleted, and counts its block as a deleted block ref
        // instead of a hot one -- which leaves the shard through the public report as
        // `tombstone_object_count` on a store whose only block is live.
        //
        // The whole-object restate path has always cleared it, one line after the same
        // `object_index.insert` (`sync_bucket_index_object_blocks_with_mode`). The asymmetry was
        // the whole defect; this is the same line, on the per-element door.
        bucket.deleted_object_index.remove(&object_id);
        // The handle the map assigns is what the lookup records, so the two cannot disagree.
        block_ref_key = bucket.block_index.insert(block_index.clone(), &mut shard.bucket_index.block_slab_live);
        classify_bucket_layout_in_place(bucket);
        touched_buckets.push(routing_bucket);
    }
    shard
        .bucket_index
        .insert_object_block_lookup(routing_bucket, block_ref_key, &block_index);
    shard.buckets_pending_flag_refresh.extend(touched_buckets);
}

pub(super) fn sync_bucket_index_object_blocks(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    addresses: Vec<BlockAddress>,
    dirty: bool,
) {
    sync_bucket_index_object_blocks_with_mode(shard, shard_id, kind, object_key, addresses, dirty, true)
}

/// Publish blocks for an object into the bucket index.
///
/// `replace_existing` is the whole cost of this function. With it true the object's blocks are
/// dropped and rebuilt from `addresses`, which is the only way to express "the live set is
/// exactly this" -- and it costs the object's whole block count on every call, however few blocks
/// the write actually touched.
///
/// A pure APPEND does not need that. Nothing was removed, and the blocks already filed are still
/// correct, so publishing only the new addresses leaves the index in the same state for a
/// fraction of the work. Callers may pass false ONLY when both hold:
///
///   * nothing was evicted (a trim that dropped points must re-state the live set), and
///   * no appended key REPLACED an existing one (a replacement must drop the superseded block,
///     or the object would carry two entries for one key).
///
/// Both are decidable at the call site: `BTreeMap::insert` reports the value it displaced, and
/// the trim reports whether it removed anything.
pub(super) fn sync_bucket_index_object_blocks_with_mode(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    addresses: Vec<BlockAddress>,
    dirty: bool,
    replace_existing: bool,
) {
    let mut touched_buckets = BTreeSet::new();
    let mut removed_any = false;
    // Read before the first `&mut` borrow of the index below, and for the same reason as the
    // upsert: this site PLACES. What it REMOVES is decided by `object_block_refs` and by the
    // bucket map itself -- where the blocks actually are -- so narrowing this cannot drop one.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    // Same reason as `upsert_bucket_index_block_with`: publish into a released bucket and the node
    // is left half-resident. Reload every bucket these addresses land in first.
    if !shard.bucket_index.released_buckets.is_empty() {
        // One bucket, not a set gathered from the addresses: every address published here belongs
        // to ONE object key, so they all land in the key's bucket. The set was a set of one.
        let landing = [block_routing_bucket(
            object_key,
            start_routing_bucket,
            end_routing_bucket,
        )];
        for routing_bucket in landing {
            reload_released_bucket(shard, shard_id, routing_bucket);
        }
    }
    // An empty lookup means "not established yet", which callers read as a signal to fall back to
    // scanning. Establishing it still walks the buckets; maintaining an established one must not.
    //
    // The ref total counts as part of being established. Only a rebuild can set it -- a count that
    // starts at "unknown" cannot be incremented into a right answer -- and the load path fills the
    // lookup without it, so a shard can come up with entries and no total. The wholesale rebuild
    // this replaces re-established the total on every series write, which hid that. Tie the two
    // together instead: either both are established or the next write establishes both.
    let lookup_needs_establishing = shard.bucket_index.object_block_lookup.is_empty()
        || shard.bucket_index.object_component_block_refs.is_none();
    // Components whose blocks this call drops, so the lookup can be corrected for exactly those
    // instead of being rebuilt from every block in the shard.
    let mut removed_components: BTreeSet<Option<Arc<str>>> = BTreeSet::new();
    let target_buckets = if lookup_needs_establishing {
        shard
            .bucket_index
            .bucket_map
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
    } else {
        shard
            .bucket_index
            .object_block_refs(kind, object_key)
            .map(|block_refs| {
                block_refs
                    .all_refs()
                    .map(|block_ref| block_ref.routing_bucket)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default()
    };
    for routing_bucket in target_buckets {
        if !replace_existing {
            // Additive publish: nothing is being superseded, so the blocks already filed stay.
            break;
        }
        let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
            continue;
        };
        let before = bucket.block_index.len();
        bucket.block_index.retain(&mut shard.bucket_index.block_slab_live, |_, page| {
            let matches_object = page.model_id.as_str() == kind && &*page.object_key == object_key;
            if matches_object {
                removed_components.insert(page.component.clone());
            }
            !matches_object
        });
        if bucket.block_index.len() != before {
            removed_any = true;
            touched_buckets.insert(routing_bucket);
            bucket.set_dirty(bucket.dirty() | dirty);
            bucket.set_deleted(bucket.block_index.is_empty());
            if dirty {
                bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
            }
            bucket.set_in_memory(!bucket.block_index.is_empty());
            update_bucket_layout(shard_id, bucket);
        }
    }

    if !lookup_needs_establishing {
        // Only this object's own entries were dropped above, so only they need correcting.
        for component in &removed_components {
            shard.bucket_index.remove_object_block_lookup_entry(
                kind,
                object_key,
                component.as_deref(),
            );
        }
    }

    let mut unique_addresses = BTreeMap::<
        (u64, u64, u64, Option<u64>, Option<u64>),
        BlockAddress,
    >::new();
    for address in addresses {
        unique_addresses.insert(block_physical_identity_key(&address), address);
    }

    // Hoisted: neither changes across iterations, and each cost a String plus an Arc that copies
    // it -- four allocations per address published, for two values. Cloning an Arc is a refcount
    // bump.
    let object_key_arc: Arc<str> = Arc::from(object_key);
    let entry_kind = stored_model_kind(kind);
    for address in unique_addresses.into_values() {
        let routing_bucket =
            block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
        let object_id = stable_block_object_id(shard_id, kind, object_key);
        let entry = LiveBlockEntry {
            object_key: Arc::clone(&object_key_arc),
            kind: entry_kind,
            component: None,
            log_backed: address.block_id().is_none(),
            address,
            dirty,
            deleted: false,
            // Where this publish is about to file it, computed five lines up.
            filed_routing_bucket: routing_bucket,
            filing_is_known: true,
        };
        let bucket = shard
            .bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket,
                flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
                ..BucketNode::default()
            });
        bucket.set_dirty(bucket.dirty() | dirty);
        bucket.set_deleted(false);
        if dirty || touched_buckets.insert(routing_bucket) {
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
        }
        bucket.set_meta_loaded(true);
        bucket.set_loading(false);
        bucket.set_in_memory(true);
        bucket.object_index.insert(object_id);
        bucket.deleted_object_index.remove(&object_id);
        let mut block_ref_key: u64 = 0;
        let page = BlockIndex {
            object_key: entry.object_key,
            model_id: entry.kind,
            component: entry.component.clone(),
            address: {
                let mut address = entry.address;
                    address
            },
            dirty: entry.dirty,
            deleted: entry.deleted,
            log_backed: entry.log_backed,
        };
        // The map assigns the handle; the lookup records the same one.
        let block_ref_key = bucket.block_index.insert(page.clone(), &mut shard.bucket_index.block_slab_live);
        // `object_index` was just given this object id above, so the set is already correct and only
        // the label needs re-deriving. `update_bucket_layout` would rebuild the set by walking every
        // block in the bucket -- once per address published, which is what made a write cost the
        // whole object rather than the part of it being written.
        classify_bucket_layout_in_place(bucket);
        // The bucket borrow has to end before the lookup, which borrows the index itself.
        if !lookup_needs_establishing {
            shard
                .bucket_index
                .insert_object_block_lookup(routing_bucket, block_ref_key, &page);
        }
    }

    // Drop buckets this call emptied -- BY NAME, not by walking the map.
    //
    // This was `bucket_map.retain(..)`, which is O(buckets) and ran on every write because
    // `dirty` is true for one. At the default routing-slot range every key gets its own slot, so
    // the map holds one bucket per record and the walk is O(corpus) per command: measured at
    // 94.2% of the datanode's self time under a message-ingest profile, and an 8-hour run
    // degraded from 7 ms to 105 ms per message as `corpus^0.96` -- linear per write, quadratic
    // overall.
    //
    // Only a bucket whose `page_index` actually shrank above can have newly become empty, and
    // that set is `touched_buckets`. Buckets the publish loop inserted into gained a block, and
    // buckets this call never opened are unchanged. So the same buckets are removed, without
    // reading the ones that cannot have changed.
    if removed_any {
        for routing_bucket in &touched_buckets {
            let now_empty = shard
                .bucket_index
                .bucket_map
                .get(routing_bucket)
                .is_some_and(|bucket| {
                    bucket.block_index.is_empty() && bucket.object_index.is_empty()
                });
            if now_empty {
                shard.bucket_index.bucket_map.remove(routing_bucket);
            }
        }
    }
    if lookup_needs_establishing {
        shard.bucket_index.rebuild_object_block_lookup();
    }
}

/// The label a bucket wears, from how many objects it holds and how many blocks are resident.
///
/// EMPTY IS ABOUT OBJECTS, NOT BLOCKS. A bucket with no resident blocks is not thereby empty: a
/// RELEASED bucket is exactly that shape -- `release_bucket_blocks` clears `page_index` and
/// deliberately keeps `object_index`, which is the only thing distinguishing a released bucket
/// from one that genuinely holds nothing -- and a bucket whose blocks live only in the model maps
/// is still holding every object it held before.
///
/// A zero block count used to answer `Empty` for two or more objects while answering
/// `SingleObject` for exactly one, so the two halves of the same question disagreed: one
/// released bucket reported what it held and the next reported nothing. `Empty` is also the
/// derive default, so the mislabel also made a populated bucket read as one nothing had ever
/// classified. The block count now only chooses BETWEEN the non-empty labels, and the object
/// count alone decides whether the bucket is empty at all.
pub(super) fn classify_bucket_layout(object_count: usize, block_ref_count: usize) -> BucketLayoutState {
    match (object_count, block_ref_count) {
        (0, _) => BucketLayoutState::Empty,
        (1, 0) => BucketLayoutState::SingleObject,
        (1, 1) => BucketLayoutState::SingleBlockObject,
        (1, _) => BucketLayoutState::MultiBlockObject,
        _ => BucketLayoutState::MultiObject,
    }
}

pub(super) fn bucket_layout_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object",
        BucketLayoutState::SingleBlockObject => "single_page_object",
        BucketLayoutState::MultiBlockObject => "multi_page_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

/// Re-derive only the layout label, taking `object_index` as already correct.
///
/// `update_bucket_layout` rebuilds that set by scanning every block in the bucket. On the write
/// path the scan is redundant: an insert has just added its object id and a removal has just
/// dropped one, so the scan re-derives what is already stored -- and being the last pass without
/// a short-circuit, it is the whole of what makes a write cost more as the store grows.
///
/// `bucket_object_index_already_matches_a_from_scratch_recompute` holds that invariant across
/// inserts, superseding overwrites, expiries and deletes. Reconstruct paths, which build
/// `bucket_map` from block entries where nothing maintained the set, keep the full rebuild.
fn classify_bucket_layout_in_place(bucket: &mut BucketNode) {
    bucket.layout = classify_bucket_layout(bucket.object_index.object_count(), bucket.block_index.len());
}

/// Blocks visited by `update_bucket_layout`, attributed to the CALL SITE that asked for it.
///
/// The visit counter lives inside the function, so it reports how much work was done and not who
/// caused it -- and with ten callers that is the difference between a fix and a guess. Two guesses
/// were spent on the wrong site before this existed: the per-insert rebuild (fixing it moved the
/// counter by exactly zero) and narrowing the rebuild's bucket range (the range is only a hashing
/// input; the function walks the whole shard regardless).
///
/// Only written under `#[cfg(test)]` -- it takes a lock, and `update_bucket_layout` is on a write
/// path.
pub mod layout_by_caller {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    pub(super) static LAYOUT_BY_CALLER: Mutex<Option<BTreeMap<String, u64>>> = Mutex::new(None);

    #[cfg(test)]
    pub(super) fn note(caller: &std::panic::Location<'static>, pages: usize) {
        let mut guard = LAYOUT_BY_CALLER.lock().expect("layout caller tally poisoned");
        *guard
            .get_or_insert_with(BTreeMap::new)
            .entry(format!("{}:{}", caller.file(), caller.line()))
            .or_insert(0) += pages as u64;
    }

    pub fn reset() {
        *LAYOUT_BY_CALLER.lock().expect("layout caller tally poisoned") = Some(BTreeMap::new());
    }

    /// Call sites and the blocks each has caused to be visited, largest first.
    pub fn snapshot() -> Vec<(String, u64)> {
        let guard = LAYOUT_BY_CALLER.lock().expect("layout caller tally poisoned");
        let mut rows: Vec<(String, u64)> = guard
            .as_ref()
            .map(|map| map.iter().map(|(k, v)| (k.clone(), *v)).collect())
            .unwrap_or_default();
        rows.sort_by(|a, b| b.1.cmp(&a.1));
        rows
    }
}

// `Location::caller()` in a function that is not `#[track_caller]` returns the location of the
// `caller()` call itself, so the by-caller breakdown below reported this one line for all ten
// callers -- it looked like attribution and was not. Applied only under `cfg(test)`, because the
// attribute adds a hidden location argument at every call site and this is a write path.
#[cfg_attr(test, track_caller)]
/// Rebuild one bucket's live-object set from its pages, then classify the layout.
///
/// THE SHARD IS A PARAMETER BECAUSE THE SET IS DERIVED PER PAGE. A page's object id is
/// `stable_block_object_id(shard, kind, key)`, so this cannot be done without a shard and must not
/// be done with a guessed one -- a zero derives a well-formed id belonging to a different shard.
///
/// AND THE PRUNING IS THE POINT, which is what an earlier version of this change lost. It asked the
/// bucket for the ids it ALREADY held rather than deriving them, on the reasoning that the function
/// had no shard. That kept every id for as long as any page in the bucket was live, so an object
/// whose last page was deleted was never dropped: `object_index` grew monotonically, the count fed
/// to `classify_bucket_layout` was wrong, and the collect below grew with the corpus. Measured on the
/// allocation probe, `SetAdd` went from 5,784 -> 5,785 bytes (1.00x flat) across a sixteen-fold
/// corpus to 5,723 -> 9,631 (1.68x). A width pin could not have found that and neither could a
/// compile; the scaling control did.
///
/// A caller that genuinely has no shard uses [`classify_bucket_layout_in_place`] instead, which
/// takes `object_index` as already correct. That function is also what the write path uses to skip
/// this rescan, so the two reasons for not rebuilding -- cannot, and need not -- share one
/// implementation; a second name for the identical body was briefly added here and removed again.
pub(super) fn update_bucket_layout(shard_id: ShardId, bucket: &mut BucketNode) {
    note_site(&bucket_visit_sites::LAYOUT, bucket.block_index.len());
    // Tests only: this takes a lock, and `update_bucket_layout` is on a write path. It exists
    // because the visit counter lives INSIDE this function and so reports how much work happened
    // without saying who asked for it -- with ten callers, that was the difference between a fix
    // and a guess.
    #[cfg(test)]
    layout_by_caller::note(std::panic::Location::caller(), bucket.block_index.len());
    let live_object_ids: BTreeSet<u64> = bucket
        .block_index
        .values()
        .filter(|page| !page.deleted)
        .map(|page| page.object_id(shard_id))
        .collect();
    if !live_object_ids.is_empty() {
        bucket.object_index = live_object_ids.into();
    } else if !bucket.block_index.is_empty() {
        bucket.object_index.clear();
    }
    bucket.layout = classify_bucket_layout(bucket.object_index.object_count(), bucket.block_index.len());
}

/// Note that a bucket's derived runtime flags may be stale.
///
/// Called where the routing bucket is already in hand. Recording it is cheap; the alternative --
/// deriving it from the object key later -- is not sound, because a stored address may carry an
/// explicit routing bucket that disagrees with `block_routing_bucket`.
pub(super) fn note_bucket_flags_stale(shard: &mut ShardState, routing_bucket: u32) {
    shard.buckets_pending_flag_refresh.insert(routing_bucket);
}

/// Recompute one bucket's derived flags. The whole body of the sweep, for a single bucket.
///
/// `rebuild_object_index` decides whether the live-object set is recomputed by scanning every
/// block, or taken as already correct and only re-classified.
///
/// The mutation sites maintain that set themselves -- a block insert adds its object id, a removal
/// drops the id once no live block carries it -- so on the write path the scan finds exactly what
/// is already stored and is pure overhead. It is also the LAST guaranteed full pass in bucket
/// maintenance (`deleted` and `dirty` both short-circuit; the TTL pass is skipped when nothing
/// expires), so it is the whole of what still scales with the corpus.
///
/// The load, recovery and reconstruct paths are a different matter: they rebuild `bucket_map`
/// from block entries, where the set has NOT been maintained and must be derived. Those keep the
/// scan. `bucket_object_index_already_matches_a_from_scratch_recompute` is the evidence for
/// dropping it everywhere else.
#[cfg_attr(test, track_caller)]
fn refresh_one_bucket_runtime_flags(
    // `None` for a state that carries no shard id. The object-index rebuild below needs one to
    // derive a page's object with, and there is no id that is safe to guess, so an unstamped state
    // RECLASSIFIES what it already holds rather than rebuilding against a different shard's terms.
    // In production a served state is always stamped -- `install_shard_state` is the only place
    // `set_shard_id` is called -- so the rebuild always runs where it matters.
    shard_id: Option<ShardId>,
    bucket: &mut BucketNode,
    now: u64,
    dirty_objects: &DirtyObjectIndex,
    expires_at_ms: &BTreeMap<String, u64>,
    rebuild_object_index: bool,
) {
    bucket.set_meta_loaded(true);
    bucket.set_loading(false);
    bucket.set_in_memory(!bucket.block_index.is_empty());
    // `all` and `any` stop at the first block that decides the answer, so neither is a reliable
    // full pass; during ingest the dirty check in particular answers on block one.
    let every_page_deleted =
        !bucket.block_index.is_empty() && bucket.block_index.values().all(|page| page.deleted);
    bucket.set_deleted(every_page_deleted);
    let any_page_dirty = bucket
        .block_index
        .values()
        .any(|page| page.dirty || dirty_objects.contains(page.object_key.as_ref()));
    bucket.set_dirty(bucket.dirty() | any_page_dirty);
    // The TTL is the one guaranteed full pass: a minimum has to look at every block, and each
    // look is a map lookup keyed by the block's object key. When nothing in the shard has an
    // expiry that whole pass is dead work -- the minimum over an empty selection is None, which
    // is exactly what the field already holds. A store that never sets a TTL is the common case
    // for bulk ingest, and this is where its per-bucket cost was going.
    if expires_at_ms.is_empty() {
        bucket.ttl_ms = BucketTtl::ABSENT;
    } else {
        note_site(&bucket_visit_sites::REFRESH_FLAGS, bucket.block_index.len());
        bucket.ttl_ms = BucketTtl::from_ms(
            bucket
                .block_index
                .values()
                .filter_map(|page| expires_at_ms.get(page.object_key.as_ref()).copied())
                .map(|expires_at| expires_at.saturating_sub(now))
                .min(),
        );
    }
    match (rebuild_object_index, shard_id) {
        (true, Some(shard_id)) => update_bucket_layout(shard_id, bucket),
        _ => classify_bucket_layout_in_place(bucket),
    }
}

/// Refresh EVERY bucket in the shard. `O(total pages)`.
///
/// Correct everywhere and the right thing after a load, a recovery or a reconstruct, where the
/// set of changed buckets is not known. On the per-write path use
/// [`refresh_pending_bucket_runtime_flags`] instead -- this sweep on every write is what made
/// ingestion quadratic in the corpus.
#[cfg_attr(test, track_caller)]
pub(super) fn refresh_bucket_runtime_flags(shard: &mut ShardState) {
    refresh_all_bucket_runtime_flags(shard, true);
}

/// The sweep, for a caller that has just rebuilt the bucket index from the block entries.
///
/// [`rebuild_bucket_first_index`] recomputes every bucket's object index and layout by scanning
/// that bucket's block index. Running the full sweep with the rebuild still switched on immediately
/// afterwards scans exactly the same blocks a second time, from the same source, with nothing in
/// between that could change the answer. The two showed up in the per-add attribution as a pair of
/// counters that were equal at every corpus size -- 45 300 each over 150 adds, 180 600 each over
/// 300, 721 200 each over 600 -- which is what the same scan run twice looks like.
///
/// The flags themselves (dirty, deleted, in_memory, ttl) are still refreshed; only the redundant
/// object-index rescan is skipped.
#[cfg_attr(test, track_caller)]
pub(super) fn refresh_bucket_runtime_flags_after_reconstruct(shard: &mut ShardState) {
    refresh_all_bucket_runtime_flags(shard, false);
}

/// The sweep, with the object-index rebuild made optional. See
/// [`refresh_one_bucket_runtime_flags`] for when it can be skipped.
#[cfg_attr(test, track_caller)]
fn refresh_all_bucket_runtime_flags(shard: &mut ShardState, rebuild_object_index: bool) {
    let now = now_ms();
    // Resolved ONCE, outside the loop: it is a property of the shard, not of a bucket.
    let shard_id = shard.shard_id();
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        refresh_one_bucket_runtime_flags(
            shard_id,
            bucket,
            now,
            &shard.dirty_objects,
            &shard.expires_at_ms,
            rebuild_object_index,
        );
    }
    // The sweep covered everything, so nothing is left outstanding.
    shard.buckets_pending_flag_refresh.clear();
}

/// Refresh only the buckets recorded as touched, and clear the record.
///
/// Equivalent to the full sweep for the buckets that changed; an untouched bucket's flags are a
/// function of its own blocks plus the two shard-wide maps, and both of those are noted against the
/// buckets they affect. `bucket_runtime_flags_match_full_sweep` in the engine tests checks that
/// equivalence against a real workload rather than leaving it as an argument.
#[cfg_attr(test, track_caller)]
pub(super) fn refresh_pending_bucket_runtime_flags(shard: &mut ShardState) {
    if shard.buckets_pending_flag_refresh.is_empty() {
        return;
    }
    // Refreshing bucket-by-bucket costs a map lookup each, where the full sweep is one ordered
    // pass. That only pays while the touched set is a small share of the shard's buckets.
    //
    // It is not always small. With a wide routing range every key lands in its own bucket, so a
    // batch touches a few hundred of millions and the targeted path wins outright. With a narrow
    // range -- `TS_SHARD_END_ROUTING_SLOT=1023`, the setting that cuts resident memory 45% and is
    // the one to run in production -- there are only 1024 buckets and a 500-command batch hashes
    // across essentially all of them. The targeted path then visits exactly the same blocks as the
    // sweep and adds a lookup per bucket on top: measured 1.6-2.2x SLOWER over 200k and 400k
    // records, in four runs out of four.
    //
    // So choose. Measured at default slots the targeted path is 1.3-2.7x faster; at 1023 slots
    // this guard hands the work back to the sweep, which is where it belongs.
    let bucket_count = shard.bucket_index.bucket_map.len();
    if shard.buckets_pending_flag_refresh.len().saturating_mul(2) >= bucket_count {
        // Still the write path, so the object-index scan stays off; only the traversal changes.
        refresh_all_bucket_runtime_flags(shard, false);
        return;
    }
    let now = now_ms();
    let shard_id = shard.shard_id();
    let pending = std::mem::take(&mut shard.buckets_pending_flag_refresh);
    for routing_bucket in pending {
        let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) else {
            continue;
        };
        refresh_one_bucket_runtime_flags(
            shard_id,
            bucket,
            now,
            &shard.dirty_objects,
            &shard.expires_at_ms,
            false,
        );
    }
}

pub(super) fn object_still_has_hot_block(shard: &ShardState, object_key: &str) -> bool {
    shard
        .strings
        .get(object_key)
        .map(|address| crate::wal_record::is_wal_resident(address.block_slab_id()))
        .unwrap_or(false)
        || shard
            .hashes
            .get(object_key)
            .map(|fields| {
                fields
                    .values()
                    .any(|address| crate::wal_record::is_wal_resident(address.block_slab_id()))
            })
            .unwrap_or(false)
}

pub(super) fn clear_published_object_dirty_state(shard: &mut ShardState, object_key: &str) {
    if object_still_has_hot_block(shard, object_key) {
        return;
    }
    // Clearing a dirty bit needs no shard; re-deriving the object index does. Resolved here so the
    // loop below can rebuild when the state is stamped and reclassify when it is not.
    let shard_id = shard.shard_id();
    shard.dirty_objects.remove(object_key);
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        note_site(&bucket_visit_sites::CLEAR_DIRTY, bucket.block_index.len());
        let mut touched = false;
        for page in bucket.block_index.blocks_mut_unaccounted() {
            if &*page.object_key == object_key {
                page.dirty = false;
                touched = true;
            }
        }
        if touched {
            note_site(&bucket_visit_sites::CLEAR_DIRTY, bucket.block_index.len());
            let any_page_dirty = bucket
                .block_index
                .values()
                .any(|page| page.dirty || shard.dirty_objects.contains(page.object_key.as_ref()));
            bucket.set_dirty(any_page_dirty);
            match shard_id {
                Some(shard_id) => update_bucket_layout(shard_id, bucket),
                None => classify_bucket_layout_in_place(bucket),
            }
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
    // from the model maps (strings/hashes/...), so collect_model_live_block_entries no longer
    // sees it, but the object manager must keep reporting it as a tombstone until GC reclaims
    // the slot. The deserialize + reconcile load path keeps deleted_object_index; a
    // promote/rebuild reconstruct (flush or the WAL-replay tail) would otherwise silently drop
    // it, undercounting objects after a reconstruct-based reload.
    let prior_deleted_object_index: BTreeMap<u32, DeletedObjectIndex> = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| !bucket.deleted_object_index.is_empty())
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.deleted_object_index.clone()))
        .collect();
    // AND THE TOMBSTONE ENTRIES, FOR THE SAME REASON ONE STEP DOWN.
    //
    // The paragraph above is the precedent and it states the rule exactly: a delete takes the object
    // out of the model maps, so a rebuild that reads those maps cannot re-derive it, and a
    // reconstruct-based reload would silently drop what the deserialize path keeps. That was written
    // about a deleted OBJECT's id. A per-element removal now keeps an ENTRY -- pointing at the page
    // that records the removal, so a membership derived from the pages does not resurrect the element
    // -- and it is invisible to `collect_model_live_block_entries` for precisely the same reason.
    //
    // WITHOUT THIS, A COMPACTION ROUND ERASED EVERY REMOVAL FROM THE PAGES. This function runs first
    // in the round and `rebuild_bucket_block_ownership` runs after it; both rebuild from the model
    // maps, and the tombstone count went to zero at this one. Driven by
    // `a_removal_retains_one_entry_and_nothing_yet_collects_it`, whose denominator is
    // `tombstones_refiled_count` so that "nothing was dropped" cannot be confused with "nothing was
    // rebuilt".
    //
    // The bucket each was FILED IN travels with it rather than being recomputed from the key: the
    // removal recorded that bucket because the outcome's `block_routing_bucket(key, 0, u32::MAX)` is a
    // different number from the shard's own range, and filing a tombstone under the wrong one made a
    // bucket all-tombstone and made ownership validation refuse the round.
    let prior_tombstone_entries: Vec<(u32, BlockIndex)> = shard
        .bucket_index
        .bucket_map
        .iter()
        .flat_map(|(routing_bucket, bucket)| {
            bucket
                .block_index
                .values()
                .filter(|page| page.deleted)
                .map(move |page| (*routing_bucket, page.clone()))
        })
        .collect();
    let mut bucket_index = CoreIndex::default();
    for entry in collect_model_live_block_entries(shard) {
        let routing_bucket =
            block_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket);
        let object_id = expected_live_block_object_id(shard_id, &entry);
        let bucket = bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket,
                flags: BucketFlags::default().with(BucketFlags::META_LOADED, true).with(BucketFlags::IN_MEMORY, true),
                ..BucketNode::default()
            });
        let block_dirty = shard.dirty_objects.contains(entry.object_key.as_ref()) || entry.dirty;
        bucket.set_dirty(bucket.dirty() | block_dirty);
        if block_dirty {
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(1);
        }
        bucket.set_in_memory(bucket.in_memory() | true);
        bucket.object_index.insert(object_id);
        bucket.block_index.insert(
            BlockIndex {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component.clone(),
                address: entry.address,
                dirty: block_dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
            &mut bucket_index.block_slab_live,
        );
        update_bucket_layout(shard_id, bucket);
    }
    // Re-attach the tombstone ids captured above. Keep them in object_index too so the object
    // manager's object_count matches the deserialize/reconcile load path (which never dropped
    // them); a live block entry re-adding the same id is a no-op (BTreeSet).
    for (routing_bucket, deleted) in prior_deleted_object_index {
        let bucket = bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket,
                flags: BucketFlags::default().with(BucketFlags::META_LOADED, true),
                ..BucketNode::default()
            });
        for object_id in &deleted {
            bucket.object_index.insert(*object_id);
        }
        bucket.deleted_object_index.extend(deleted);
    }
    // Re-attach the tombstone ENTRIES captured above, skipping any whose element the rebuild has just
    // filed as LIVE. A re-add clears its tombstone through the upsert's retain, and a rebuild has to
    // reach the same state or it would restate a removal the store has already undone -- the mirror
    // image of the defect this exists to prevent, and the direction nothing else here would catch.
    let mut refiled = 0usize;
    for (routing_bucket, tombstone) in prior_tombstone_entries {
        let live_again = bucket_index
            .bucket_map
            .get(&routing_bucket)
            .is_some_and(|bucket| {
                bucket.block_index.values().any(|page| {
                    !page.deleted
                        && page.model_id == tombstone.model_id
                        && page.object_key == tombstone.object_key
                        && page.component.as_deref() == tombstone.component.as_deref()
                })
            });
        if live_again {
            continue;
        }
        let bucket = bucket_index
            .bucket_map
            .entry(routing_bucket)
            .or_insert_with(|| BucketNode {
                routing_bucket,
                flags: BucketFlags::default().with(BucketFlags::META_LOADED, true),
                ..BucketNode::default()
            });
        bucket.set_dirty(true);
        bucket
            .block_index
            .insert(tombstone, &mut bucket_index.block_slab_live);
        refiled += 1;
        // THE SHARD COMES FROM THE CALLER. `rebuild_bucket_first_index` takes `shard_id`, and this
        // is a RECONSTRUCT path -- `object_index` is being rebuilt from block entries, where no
        // mutation site maintained it -- so this is one of the sites that must keep the full
        // rescan rather than reclassify what it already holds.
        update_bucket_layout(shard_id, bucket);
    }
    note_tombstones_refiled(refiled);
    bucket_index.rebuild_object_block_lookup();
    shard.bucket_index = bucket_index;
    // The local index charged every block it filed, and it arrived empty, so the tally travelled
    // here with it and is already right. Same reason as `rebuild_bucket_block_ownership`: a walk to
    // confirm it is the walk being removed.
    shard.bucket_index.block_slab_live.mark_ready();
}

/// Merge a block-derived timestamped-series view against the pre-existing (deserialized /
/// in-memory) model map, which is AUTHORITATIVE for membership. reconcile re-reads packed
/// blocks, but a block physically holds timestamps that may have been evicted (feature
/// max_size trim) from the model map, and a block read can transiently fail. So:
///  - a key present in the persisted map keeps EXACTLY its persisted timestamps (no
///    resurrection of evicted points, no loss on a failed block read), refreshing each
///    address from the block-derived view when available;
///  - a key absent from the persisted map is rebuilt from the block (the legitimate
///    rebuild-from-bucket-index case, e.g. a bucket_index entry with no model-map counterpart).
/// This is why `promote` never clears the model maps: they remain the membership source.
fn reconcile_timestamped_series_membership(
    persisted: &HashMap<String, BTreeMap<u64, BlockAddress>>,
    block_derived: HashMap<String, BTreeMap<u64, BlockAddress>>,
) -> HashMap<String, BTreeMap<u64, BlockAddress>> {
    let mut result: HashMap<String, BTreeMap<u64, BlockAddress>> = HashMap::new();
    for (key, block_series) in block_derived {
        match persisted.get(&key) {
            Some(persisted_series) => {
                let merged = persisted_series
                    .iter()
                    .map(|(timestamp_ms, persisted_address)| {
                        let address = block_series
                            .get(timestamp_ms)
                            .cloned()
                            .unwrap_or_else(|| persisted_address.clone());
                        (*timestamp_ms, address)
                    })
                    .collect();
                result.insert(key, merged);
            }
            None => {
                result.insert(key, block_series);
            }
        }
    }
    // Preserve persisted keys entirely absent from the block-derived view (block unreadable or
    // not in bucket_index) so a transient read failure never drops a durable series.
    for (key, persisted_series) in persisted {
        result
            .entry(key.clone())
            .or_insert_with(|| persisted_series.clone());
    }
    result
}

/// The derived view, with every element the DURABLE map holds and it does not AND WHOSE BLOCK IS
/// STILL THERE.
///
/// One rule for the three kinds whose element identity is spelled into a component name. The derived
/// view wins where both have an element -- it reflects the delta fold, which the persisted map does
/// not, because `apply_key_states` folds `features` and the control-state maps and not these three.
/// The persisted map keeps anything the derived view could not produce, which is what makes skipping
/// an unreadable name safe: the element stays, it just does not come back through the name.
///
/// Per ELEMENT rather than per KEY. A per-key rule -- which is what the `control_state` arm uses, for
/// a reason that holds there and not here -- would drop every element the fold added to a key the
/// persisted map already had.
///
/// # AND THE PERSISTED MAP IS ONLY AS NEW AS THE SNAPSHOT IT CAME OUT OF
///
/// `set_index_serde` and its siblings persist these maps as part of the BASE INDEX, written at
/// compaction or unload. The bucket index this function's derived view is built from is newer than
/// that: `fold_index_log_deltas` has already replayed the delta suffix over it by the time the
/// reconcile runs (`load_index_inner` folds at one statement and reconciles at the next), and
/// `fold_delta_block_items` makes the delta authoritative for every key it covers -- "every existing
/// live block entry for a covered key is removed first ... then the delta's live items are inserted".
///
/// So a member removed after the last base-index write is GONE from the block index and PRESENT in
/// the persisted map, and this function used to hand it back. #2017 drove exactly that: resident map
/// 2 members, live block index 1. It is the reason a set listing could not be served from
/// `shard.sets`.
///
/// THE QUESTION ASKED IS #2005's, AND IT IS ASKED OF THE OTHER INPUT NOW. #2005 fixed a resurrection
/// on this same function from the CARRY side -- `fold_carried_container_elements` applies the delta
/// records' carried elements once, after the block index settles, keeping only those whose block is
/// still at the carried address. That fix was complete for the carry and never looked at the
/// persisted map, which is a different input reaching the same merge: one is built during the load,
/// the other is read off disk and is simply older. Both answer `live_page_key` against the finished
/// index now.
///
/// WHY THIS KEEPS #1989's ELEMENT AND DROPS #2017's, which is what makes it the right question
/// rather than a narrowing of the function's job. #1989's case is a block whose component cannot be
/// decoded: the derived view cannot NAME the element while its block sits in the index, so the block is
/// live at the persisted address and the element is kept -- the merge still does the job it was added
/// for. #2017's case is a block that is not in the index at all, so nothing is live at that address
/// and the element is dropped. The rule holds for a key the delta covered and one it did not: an
/// untouched key's blocks are still exactly where the base index put them.
///
/// A PERSISTED KEY WITH NO SURVIVING ELEMENT NOW GETS NO ENTRY AT ALL. This used to run
/// `derived.entry(key).or_default()` before looking at a single element, so a persisted key whose
/// every block had gone -- and a persisted key holding an empty map -- installed an EMPTY inner map
/// under a live key. `record_exists_exact` reads `contains_key` on these maps, so that was a key
/// EXISTS answered 1 for and every listing answered empty for, arriving by reload rather than by
/// `SetRemove`. The entry is created only where an element survives.
fn fill_absent_elements<K, M>(
    mut derived: std::collections::HashMap<K, M>,
    persisted: std::collections::HashMap<K, M>,
    live: &std::collections::HashSet<super::LiveBlockKey>,
    resurrections_refused: &mut usize,
) -> std::collections::HashMap<K, M>
where
    K: std::hash::Hash + Eq + Clone,
    M: super::ElementMap
        + IntoIterator<
            Item = (
                <M as super::ElementMap>::Element,
                <M as super::ElementMap>::Value,
            ),
        >,
    <M as super::ElementMap>::Value: super::CarriedValue,
{
    for (key, elements) in persisted {
        for (element, value) in elements {
            if !live.contains(&super::live_page_key(value.carried_address())) {
                *resurrections_refused += 1;
                continue;
            }
            // `insert_element_if_absent` and NOT `insert_element`: the derived value wins where it
            // exists. The two merges over `ElementMap` want opposite things from a collision -- see
            // the trait -- and this is the one that must not let the older durable map overwrite a
            // newer derived address.
            derived
                .entry(key.clone())
                .or_default()
                .insert_element_if_absent(element, value);
        }
    }
    derived
}

pub(super) fn reconcile_secondary_views_from_bucket_index(
    block_store: &BlockStore,
    shard: &mut ShardState,
    warm: Option<(&MultiLayerCache, ShardId)>,
) {
    if shard.bucket_index.bucket_map.is_empty() {
        return;
    }

    let entries = collect_bucket_index_live_block_entries(shard)
        .into_iter()
        .filter(|entry| !entry.deleted)
        .collect::<Vec<_>>();

    // THE BLOCKS THE FINISHED INDEX STILL HOLDS, by address, for the three merges below. This is the
    // question `fold_carried_container_elements` asks of the fold's carried elements, asked of the
    // merge's other input -- the persisted map, which is the older of the two.
    //
    // BUILT FROM `entries`, AND THAT IS THE WHOLE CARE IN IT, not a convenience. The obvious source
    // is a walk of `shard.bucket_index.bucket_map`, and it is WRONG: a RELEASED bucket's blocks are
    // absent from `bucket_map` ON PURPOSE while the elements are still live, and
    // `collect_bucket_index_live_block_entries` supplements exactly those back in from the model
    // maps -- "what this returns is what the bucket index WOULD say if nothing were released".
    // Filtering against the raw `bucket_map` would therefore DROP every container element in a
    // released bucket, on any of the five reconcile sites a release can be followed by. Taking the
    // set from the same `entries` the derived view is built from means the filter and its subject
    // read one population, so the filter can only ever remove what the derived view also lacks.
    let live_pages_by_address: std::collections::HashSet<super::LiveBlockKey> = entries
        .iter()
        .map(|entry| super::live_page_key(&entry.address))
        .collect();

    if entries.is_empty() {
        return;
    }

    // Disk->memory promotion accumulator (normal restart). When warming, each block
    // read below also collects (cache_key, bytes) here; a single cache.put_batch()
    // at the end promotes them all under one lock instead of one lock cycle per block.
    let warm_shard = warm.map(|(_, shard_id)| shard_id);
    let mut warm_batch: Vec<(CacheKey, Vec<u8>)> = Vec::new();
    // THE KEY A BLOCK IS WARMED UNDER MUST BE THE KEY THE READ PATH BUILDS, and the read path now
    // derives the bucket from the object key over the shard's range. Deriving it the same way here
    // is what keeps a warmed block findable; taking the bucket from where the block is FILED would
    // differ for a block filed under a stale range, and the miss would be silent -- a cold read
    // that still answers, which no test can see as a failure.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();

    let mut saw_strings = false;
    // No `saw_sets` / `saw_lists` / `saw_zsets`: those three merges are unconditional now. See the
    // note at the merges for why the flags were not a protection for these arms -- each was set
    // before its own decode, so it only ever said "the index mentions this kind".
    let mut saw_features = false;
    let mut saw_control_state = false;
    let mut saw_context_events = false;
    let mut saw_context_indexes = false;
    let mut saw_context_audits = false;
    let mut saw_context_entities = false;
    let mut saw_context_children = false;
    let mut saw_context_summaries = false;
    let mut saw_context_compressions = false;

    // Component names this code could not read, over the FOUR kinds whose element identity is
    // spelled into one. Counted rather than defaulted: each of these used to become a real value --
    // the empty member, sequence zero, or the empty field name -- and take a genuine element's
    // address.
    //
    // Four and not three: the `hash` arm was the last one still defaulting. It was also, until
    // `hashes` became durable, the one arm with no durable map behind it to outrank the phantom it
    // produced -- which is why the skip mattered more here than anywhere else, and why it matters
    // less now: the merge below restores the field the name could not spell.
    let mut unreadable_names = 0usize;
    // Scores the DURABLE map supplied because the name's disagreed, and scores taken from the name
    // because the durable map did not hold the member. Both are printed, because "the durable map
    // won" and "the durable map did not hold this element" are different states with the same
    // outcome. (All four kinds have a durable map now, so the second no longer ever means "there
    // was no map at all" -- it means the map was silent about this element.)
    let mut outranked_scores = 0usize;
    let mut derived_scores = 0usize;

    let mut strings = HashMap::new();
    let mut hashes = HashMap::<String, super::hash_field_map::HashFieldMap>::new();
    let mut sets = HashMap::<String, BTreeMap<Vec<u8>, BlockAddress>>::new();
    let mut lists = HashMap::<String, BTreeMap<i64, BlockAddress>>::new();
    let mut zsets = HashMap::<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>::new();
    let mut features = HashMap::<String, BTreeMap<u64, BlockAddress>>::new();
    let mut control_state = HashMap::<String, BTreeMap<u64, i64>>::new();
    let mut control_state_blocks = HashMap::new();
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
                strings.insert(entry.object_key.as_ref().into(), entry.address);
            }
            "hash" => {
                // SKIPPED, NOT DEFAULTED, and the fourth arm to need it. This was
                // `entry.component.unwrap_or_default()`, which turns a block that names NO field into
                // a field named `""` -- a real, addressable field name, which then collides with a
                // genuine empty-named field and takes its address. An empty hash FIELD NAME is
                // legal, which is exactly why the absent one must not spell it.
                //
                // THE CONSOLATION THE OTHER THREE ARMS RELY ON DOES NOT EXIST HERE. Each of those
                // says "the durable map below still holds the element", and each is MERGED through
                // `fill_absent_elements` for that reason. `hashes` is `skip_serializing`
                // (`state.rs`), so nothing is written, this arm ASSIGNS rather than merges, and
                // there is no durable map to outrank a wrong answer. A phantom field here is the
                // only answer the shard has -- and for a context node, whose block is filed under the
                // single constant `CONTEXT_NODE_FIELD`, a phantom `""` is not merely a wrong name:
                // the seven readers that spell `"meta"` back find nothing and the node reads as
                // ABSENT.
                //
                // `saw_hashes` MOVED IN HERE WITH IT, and that is part of the fix rather than tidying.
                // The flag gates `shard.hashes = hashes`, a wholesale assignment; set outside the
                // match, an index whose hash entries ALL named nothing would derive an empty map and
                // assign it over a live one. The other three arms are safe from that without the flag
                // because their merge KEEPS a persisted element the derived view could not produce,
                // and an unreadable name is exactly that case: the block is still in the index, so the
                // element is still live at its persisted address and the merge keeps it. (That used to
                // read "returns the persisted map when the derived one is empty", which stopped being
                // true when the merge began filtering the persisted map on whether each element's block
                // is still there -- a whole-map passthrough is not what it does, and the reason those
                // arms are safe is the per-element one above.) This arm has no merge at all, so the
                // flag has to do that work. "Saw a hash" now means the index said something about a
                // hash this code could use.
                match entry.component {
                    Some(field) => {
                        hashes
                            .entry(entry.object_key.to_string())
                            .or_default()
                            .insert(field.to_string(), entry.address);
                    }
                    None => unreadable_names += 1,
                }
            }
            "set" => {
                // SKIPPED, NOT DEFAULTED. This was
                // `.and_then(|c| hex::decode(c).ok()).unwrap_or_default()`, which turns a name this
                // code cannot read into the EMPTY member -- a real member value, which then collides
                // with any genuine empty member and takes its address. A name that cannot be read
                // names nothing; the durable map below still holds the element, so skipping loses
                // it from the derived view and not from the store.
                match entry.component.as_deref().and_then(|c| hex::decode(c).ok()) {
                    Some(member) => {
                        sets.entry(entry.object_key.to_string())
                            .or_default()
                            .insert(member, entry.address);
                    }
                    None => unreadable_names += 1,
                }
            }
            "zset" => {
                let parsed = entry.component.as_deref().and_then(|component| {
                    // SIXTEEN CHARACTERS IS A WHOLE COMPONENT, NOT A TRUNCATED ONE.
                    //
                    // `zset_component` is `{biased:016x}` followed by `hex::encode(member)`, so a
                    // member of zero bytes -- which nothing on the write path rejects -- spells
                    // EXACTLY sixteen characters. This read `<= 16`, so that component decoded to
                    // nothing, the element was counted as unreadable and skipped, and on the one
                    // door where the durable map does not already hold it (the delta fold, whose
                    // records carry elements written after the base snapshot) the member was
                    // silently gone on reload.
                    //
                    // The engine already spells the boundary the other way where it replays the
                    // same name: both the insert and the removal arm of `apply_outcome_item` ask
                    // `component.len() < 16`. Two readers of one encoding disagreeing about its
                    // shortest legal form is the defect; this is the side that was wrong, because
                    // sixteen characters is a complete score with an empty member after it and
                    // `hex::decode("")` is `Ok(vec![])`.
                    if component.len() < 16 {
                        return None;
                    }
                    match (
                        u64::from_str_radix(&component[..16], 16),
                        hex::decode(&component[16..]),
                    ) {
                        (Ok(biased), Ok(member)) => Some((biased, member)),
                        _ => None,
                    }
                });
                match parsed {
                    Some((named_score, member)) => {
                        // THE DURABLE MAP OUTRANKS THE NAME FOR THE SCORE.
                        //
                        // `zset_index_serde` persists this map as (member bytes, (score, address)),
                        // so the score is a stored value and the name is a second copy of it
                        // rendered as text. Where the durable map holds this member its score wins;
                        // the name's is the fallback for a member the durable map does not have,
                        // which is how an element folded out of the delta log arrives.
                        let score = shard
                            .zsets
                            .get(entry.object_key.as_ref())
                            .and_then(|members| members.get(&member))
                            .map(|(stored, _)| *stored)
                            .unwrap_or_else(|| {
                                derived_scores += 1;
                                named_score
                            });
                        if score != named_score {
                            outranked_scores += 1;
                        }
                        zsets
                            .entry(entry.object_key.to_string())
                            .or_default()
                            .insert(member, (score, entry.address));
                    }
                    None => unreadable_names += 1,
                }
            }
            "list" => {
                // SKIPPED, NOT DEFAULTED. This ended `.unwrap_or_default()`, so a name this code
                // cannot read became SEQUENCE ZERO -- a real position in the list, whose entry it
                // then overwrote.
                match entry
                    .component
                    .as_deref()
                    .and_then(|component| u64::from_str_radix(component, 16).ok())
                    .map(|biased| biased.wrapping_add(i64::MIN as u64) as i64)
                {
                    Some(seq) => {
                        lists
                            .entry(entry.object_key.to_string())
                            .or_default()
                            .insert(seq, entry.address);
                    }
                    None => unreadable_names += 1,
                }
            }
            "feature" => {
                saw_features = true;
                insert_timestamped_secondary_view(
                    block_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut features,
                    entry.object_key.to_string(),
                    entry.address,
                    Some(block_routing_bucket(
                        &entry.object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                );
            }
            "sequence" => {
                saw_features = true;
                insert_timestamped_secondary_view(
                    block_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut features,
                    entry.object_key.to_string(),
                    entry.address,
                    Some(block_routing_bucket(
                        &entry.object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                );
            }
            "control_state" => {
                saw_control_state = true;
                if let Ok(bytes) = block_store.read(&entry.address) {
                    if let Some(shard_id) = warm_shard {
                        let key = CacheKey::page_with_slot(
                            shard_id,
                            entry.address.block_slab_id(),
                            entry.address.offset(),
                            entry.address.length(),
                            Some(block_routing_bucket(
                                &entry.object_key,
                                start_routing_bucket,
                                end_routing_bucket,
                            )),
                        );
                        warm_batch.push((key, bytes.clone()));
                    }
                    if let Ok(series) = serde_json::from_slice::<BTreeMap<u64, i64>>(&bytes) {
                        control_state.insert(entry.object_key.clone().to_string(), series);
                    }
                }
                control_state_blocks.insert(entry.object_key.as_ref().into(), entry.address);
            }
            "context_event" => {
                saw_context_events = true;
                insert_context_event_views(
                    block_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_events,
                    &mut context_event_timeline,
                    entry.object_key.to_string(),
                    entry.address,
                    Some(block_routing_bucket(
                        &entry.object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                );
            }
            "context_index" => {
                saw_context_indexes = true;
                insert_timestamped_secondary_view(
                    block_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_indexes,
                    entry.object_key.to_string(),
                    entry.address,
                    Some(block_routing_bucket(
                        &entry.object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                );
            }
            "context_audit" => {
                saw_context_audits = true;
                insert_timestamped_secondary_view(
                    block_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_audits,
                    entry.object_key.to_string(),
                    entry.address,
                    Some(block_routing_bucket(
                        &entry.object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
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
                    block_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_children,
                    entry.object_key.to_string(),
                    entry.address,
                    Some(block_routing_bucket(
                        &entry.object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                );
            }
            // "context_embedding" entries from pre-retirement indexes fall through to the
            // ignore arm below: the rows they addressed have no readers left.
            "context_summary" => {
                saw_context_summaries = true;
                insert_timestamped_secondary_view(
                    block_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_summaries,
                    entry.object_key.to_string(),
                    entry.address,
                    Some(block_routing_bucket(
                        &entry.object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                );
            }
            "context_compression" => {
                saw_context_compressions = true;
                insert_timestamped_secondary_view(
                    block_store,
                    warm_shard,
                    &mut warm_batch,
                    &mut context_compressions,
                    entry.object_key.to_string(),
                    entry.address,
                    Some(block_routing_bucket(
                        &entry.object_key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                );
            }
            _ => {}
        }
    }

    if saw_strings {
        shard.strings = strings;
    }
    // MERGED, NOT ASSIGNED, for the three kinds whose element identity is spelled into a component
    // name. These three used to read `shard.zsets = zsets;` and so on, which throws a DURABLE map
    // away in favour of a view rebuilt by parsing those names: a store written with one score came
    // back with another when only the name was changed, and the persisted map -- which
    // `zset_index_serde` writes as (member, (score, address)) -- had no say.
    //
    // NOT the per-key rule the `control_state` arm below uses, and the difference matters.
    // `control_state` keeps the persisted series wholesale for every key it has, because its block is
    // a copy of the series. These three cannot: `apply_key_states` folds `features` and the
    // control-state maps out of the delta log and NOT `sets`, `zsets` or `lists`, so the derived view
    // is the only path by which a folded element reaches them. Taking the durable map wholesale per
    // key would drop exactly those.
    //
    // So the rule is per ELEMENT: the derived view decides which elements exist and which block backs
    // each, because it reflects the fold; the durable map supplies what the name merely re-spells,
    // and keeps any element the derived view could not produce.
    let mut resurrections_refused = 0usize;
    // UNCONDITIONAL, WHERE THESE THREE USED TO BE GATED ON `saw_lists` / `saw_zsets` / `saw_sets`,
    // and that is part of the change rather than tidying.
    //
    // Each flag is set at the TOP of its arm, BEFORE the component decode. `saw_hashes` -- which
    // #2016 moved INSIDE its match, for a reason that applied to an arm with no durable map behind
    // it -- is GONE entirely now that the hash arm merges like the others, so that contrast is
    // history rather than a live difference. So `saw_sets == false` does not mean "no set entry
    // decoded"; it means the settled
    // block index holds NO SET BLOCK AT ALL. Skipping the merge there left the deserialized persisted
    // map standing WHOLE, unfiltered: a store whose every set block was removed after its last base
    // index write reloaded with a full resident map and an empty block index, which is the
    // over-complete state again by the one route the merge never saw.
    //
    // Running the merge with an empty derived view is safe here for the reason the filter is safe at
    // all: it drops only a persisted element whose address matches no live block in the finished
    // index. That trusts the index exactly as far as this function already trusts it two arms up,
    // where `shard.strings = strings` and `shard.hashes = hashes` assign the derived view WHOLESALE.
    {
        let persisted = std::mem::take(&mut shard.lists);
        shard.lists = fill_absent_elements(
            lists,
            persisted,
            &live_pages_by_address,
            &mut resurrections_refused,
        );
    }
    {
        let persisted = std::mem::take(&mut shard.zsets);
        shard.zsets = fill_absent_elements(
            zsets,
            persisted,
            &live_pages_by_address,
            &mut resurrections_refused,
        );
    }
    {
        let persisted = std::mem::take(&mut shard.sets);
        shard.sets = fill_absent_elements(
            sets,
            persisted,
            &live_pages_by_address,
            &mut resurrections_refused,
        );
    }
    // THE FOURTH KIND, AND THE REASON IT WAS THE ODD ONE OUT IS GONE. `hashes` is durable now
    // (`state.rs` carries `#[serde(default)]`, not `skip_serializing`), so there is a persisted map
    // to outrank a name this code could not read -- which is the whole consolation the other three
    // arms had and this one did not. The `saw_hashes` gate went with it: that flag existed ONLY to
    // stop a wholesale assignment of a derived-empty map over a live one, and a merge cannot do
    // that. Leaving the flag would have reproduced, for `hashes`, exactly the hole the note above
    // describes for the other three -- a skipped merge leaves the deserialized persisted map
    // standing WHOLE and unfiltered.
    {
        let persisted = std::mem::take(&mut shard.hashes);
        shard.hashes = fill_absent_elements(
            hashes,
            persisted,
            &live_pages_by_address,
            &mut resurrections_refused,
        );
    }
    if resurrections_refused > 0 {
        // Said out loud, because a refusal here is the merge declining to serve something the
        // durable map still lists. The same sentence `fold_carried_container_elements` prints for
        // the other input to this merge.
        eprintln!(
            "reconcile: {resurrections_refused} persisted container element(s) named a page the \
             finished index does not hold and were not restored, over {} live page(s)",
            live_pages_by_address.len()
        );
    }
    let (page_read_failures, page_decode_failures) = view_rebuild_page_failure_counts();
    if page_read_failures > 0 || page_decode_failures > 0 {
        // SAID OUT LOUD, like the line below it. These were the quietest of the lot: an
        // `.unwrap_or_default()` inside a helper, with no counter and no message, on the only copy
        // two kinds have.
        eprintln!(
            "reconcile: {page_read_failures} page(s) could not be read and {page_decode_failures} \
             could not be decoded while rebuilding the timestamped views; neither contributed an \
             empty series"
        );
    }
    if unreadable_names > 0 || outranked_scores > 0 {
        // SAID OUT LOUD. Each of these was silent, and each names a stored value that disagreed with
        // the name derived from it.
        eprintln!(
            "reconcile: {unreadable_names} component name(s) could not be read and were skipped \
             rather than defaulted; {outranked_scores} score(s) came from the durable map because \
             the name disagreed; {derived_scores} came from a name because the durable map did not \
             hold the member"
        );
    }
    if saw_features {
        let persisted = std::mem::take(&mut shard.features);
        shard.features = reconcile_timestamped_series_membership(&persisted, features);
        // The feature numeric view + rollup are derived from the feature series; drop them so
        // they rebuild lazily from the series reconcile just materialized.
        super::control_rollup::feature_clear_all(shard);
    }
    if saw_control_state {
        // The serialized i64 series is authoritative (the block is a copy of it): keep the
        // persisted series where present and use the block-derived series only for keys the
        // persisted map does not have, so a transient block-read failure never drops a durable
        // control-state key.
        let persisted = std::mem::take(&mut shard.control_state);
        let mut merged = control_state;
        merged.extend(persisted);
        shard.control_state = merged;
        shard.control_state_blocks = control_state_blocks;
        // The rollup ladder is a derived view of control_state; drop it so it rebuilds
        // lazily from the series reconcile just materialized.
        super::control_rollup::clear_all(shard);
    }
    if saw_context_events {
        let persisted = std::mem::take(&mut shard.context_events);
        shard.context_events = reconcile_timestamped_series_membership(&persisted, context_events);
        // The time index is derived state: rebuild it wholesale from what the blocks actually
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

    // THE SHARD, FROM WHICHEVER OF THE TWO PLACES HAS IT, resolved before the mutable borrow below.
    // `warm` carries one when this reconcile is warming the cache; a state that entered the engine
    // carries its own. Neither is a guess. If neither is present the layout is reclassified from the
    // set the mutation sites already maintain rather than rebuilt against an invented shard.
    let shard_id = warm.map(|(_, shard_id)| shard_id).or_else(|| shard.shard_id());
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        match shard_id {
            Some(shard_id) => update_bucket_layout(shard_id, bucket),
            None => classify_bucket_layout_in_place(bucket),
        }
    }

    // Promote all blocks read above into the cache tier in a single batched put (one
    // lock acquire + one eviction drain vs one per block). No-op when not warming.
    if let Some((cache, _)) = warm {
        if !warm_batch.is_empty() {
            let _ = cache.put_batch(warm_batch);
        }
    }
}

/// Blocks a load-path view rebuild could not READ, and blocks it read and could not DECODE.
///
/// TWO COUNTERS AND NOT ONE, because the `.unwrap_or_default()` these replace made THREE different
/// outcomes into the same empty series: a read that failed, a payload in the pre-packed format, and a
/// payload that is packed and corrupt. The middle one is a legitimate thing to find in an old store;
/// the other two are faults. A single counter over all three cannot be floored at zero on a corpus
/// that contains the middle one, so it would have to be floored at "whatever it was", which is not a
/// claim about anything.
pub static VIEW_REBUILD_PAGE_READ_FAILURES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static VIEW_REBUILD_PAGE_DECODE_FAILURES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many blocks a view rebuild could not read, and how many it could not decode, in this process.
pub fn view_rebuild_page_failure_counts() -> (u64, u64) {
    (
        VIEW_REBUILD_PAGE_READ_FAILURES.load(std::sync::atomic::Ordering::Relaxed),
        VIEW_REBUILD_PAGE_DECODE_FAILURES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Forget both counts, so a test measures its own load.
pub fn reset_view_rebuild_page_failure_counts() {
    VIEW_REBUILD_PAGE_READ_FAILURES.store(0, std::sync::atomic::Ordering::Relaxed);
    VIEW_REBUILD_PAGE_DECODE_FAILURES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// `block_store.read`, with the failure COUNTED instead of flattened into an empty series.
///
/// NO TEST SEAM, DELIBERATELY, and the reason is a mutation result rather than a matter of style. A seam
/// was written here first, modelled on `fail_compaction_block_read_after_for_test`. A mutant that
/// deleted the counter from the REAL `Err(_)` arm then SURVIVED, because every fixture failed its
/// reads through the seam and nothing exercised the arm that runs in production. A torn block is
/// producible on demand -- truncate the slab files the store was written into -- so the seam was
/// buying a `cfg(test)` hook to cover a path it is not on. The guards tear the files instead.
fn read_page_for_view_rebuild(block_store: &BlockStore, address: &BlockAddress) -> Option<Vec<u8>> {
    match block_store.read(address) {
        Ok(bytes) => Some(bytes),
        Err(_) => {
            VIEW_REBUILD_PAGE_READ_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn insert_timestamped_secondary_view(
    block_store: &BlockStore,
    warm_shard: Option<ShardId>,
    warm_batch: &mut Vec<(CacheKey, Vec<u8>)>,
    target: &mut HashMap<String, BTreeMap<u64, BlockAddress>>,
    object_key: String,
    address: BlockAddress,
    routing_bucket: Option<u32>,
) {
    // THROUGH THE SEAM, AND A FAILURE IS COUNTED RATHER THAN DEFAULTED TO AN EMPTY SERIES.
    //
    // This was `block_store.read(&address).ok()` feeding an `.unwrap_or_default()` thirty lines
    // below, so a block that could not be read contributed NO timestamps and the key still got an
    // entry -- an empty series under a live key, which is #2017's over-complete shape arriving by
    // reload instead of by removal.
    //
    // WHY IT IS LOSS HERE AND DEGRADATION ELSEWHERE, which is the reason this arm is the one that
    // had to change. `reconcile_timestamped_series_membership` keeps a persisted series the derived
    // view could not produce, and says so: "a transient read failure never drops a durable series".
    // That consolation is real for `features`, whose map IS serialized. It does not exist for
    // `context_events` or `context_indexes`: both are `skip_serializing` on `ShardState`
    // (`state.rs`), so the persisted map is EMPTY by construction and this derived view is the only
    // copy. A swallowed read there does not degrade an answer, it removes the events.
    let bytes = read_page_for_view_rebuild(block_store, &address);
    // Fold the disk->memory promotion into the load read we already perform here.
    // page_store.read is mutex-serialized, so a separate post-load warm pass would
    // re-read every block under the same lock; collect the bytes we just read for a
    // single batched cache.put_batch() at the end of reconcile (24k individual
    // cache.put lock cycles -> one). The key MUST match the retrieval read path
    // (read_block_bytes) or the entries never get hit.
    if let (Some(shard_id), Some(bytes)) = (warm_shard, bytes.as_ref()) {
        let key = CacheKey::page_with_slot(
            shard_id,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            routing_bucket,
        );
        warm_batch.push((key, bytes.clone()));
    }
    // THREE OUTCOMES, THREE ANSWERS, where there used to be one empty vector for all of them.
    let Some(bytes) = bytes else {
        // Counted inside the read. No entry is created: a key whose block could not be read is left
        // for the merge to supply from the durable map, which is exactly what the merge is for, and
        // an empty entry would tell `record_exists_exact` the key is here with nothing in it.
        return;
    };
    let timestamps = match decode_feature_block_strict(&bytes) {
        PackedFeatureBlockDecode::Packed(points) => points
            .into_iter()
            .map(|point| point.timestamp_ms)
            .collect::<Vec<_>>(),
        // A block from before the packed format. Not a fault and not counted as one -- it simply
        // names no timestamps, so it contributes none.
        PackedFeatureBlockDecode::Legacy => Vec::new(),
        PackedFeatureBlockDecode::Corrupt(_) => {
            VIEW_REBUILD_PAGE_DECODE_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };
    if timestamps.is_empty() {
        // THE ENTRY IS CREATED ONLY WHERE A POINT SURVIVES, which is `fill_absent_elements`'s rule
        // after #2016 and holds here for the same reason: `target.entry(..).or_default()` ran before
        // a single point was looked at, so a block naming nothing installed an empty inner map under
        // a live key.
        return;
    }
    let series = target.entry(object_key).or_default();
    for timestamp_ms in timestamps {
        // A timestamp can physically live in MORE THAN ONE block: overwriting a timestamped point
        // with a new value writes a NEW block (higher, monotonic page_id/generation) while the OLD
        // block still physically contains that timestamp (kept live by its other points, so its
        // bucket-index entry is not removed). Reconstruction visits blocks in slab/offset order --
        // NOT write order -- so an unconditional insert let a STALE older block clobber the newer
        // one for a shared timestamp, and the value silently reverted to the old block's bytes on
        // reload. Keep the NEWEST block (highest address generation) per timestamp.
        match series.entry(timestamp_ms) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(address.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                if address.generation().unwrap_or(0) >= slot.get().generation().unwrap_or(0) {
                    slot.insert(address.clone());
                }
            }
        }
    }
}

/// Rebuild the event primary map AND its time index from a physical block.
///
/// Events are keyed by event id hash, which -- unlike a timestamp -- is not recoverable from the
/// packed point header. It lives inside the encoded ContextEvent, so this decodes each point's
/// value rather than reading only its timestamp. The alternative, keying recovered events by
/// timeline key, would rebuild a map the read path can no longer address and silently strand
/// every event after a block-recovery load.
///
/// Newest-block-wins is preserved for the same reason it exists in the timestamped view: one
/// logical record can physically live in several blocks, and reconstruction visits blocks in
/// slab/offset order, not write order.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
pub(super) fn insert_context_event_views(
    block_store: &BlockStore,
    warm_shard: Option<ShardId>,
    warm_batch: &mut Vec<(CacheKey, Vec<u8>)>,
    events: &mut HashMap<String, BTreeMap<u64, BlockAddress>>,
    timeline: &mut HashMap<String, BTreeMap<u64, u64>>,
    object_key: String,
    address: BlockAddress,
    routing_bucket: Option<u32>,
) {
    // Through the same seam, counted the same way, and for the sharper reason: `context_events`
    // and `context_event_timeline` are rebuilt here and the primary map is `skip_serializing`, so
    // there is no durable copy behind this derived one.
    let bytes = read_page_for_view_rebuild(block_store, &address);
    if let (Some(shard_id), Some(bytes)) = (warm_shard, bytes.as_ref()) {
        let key = CacheKey::page_with_slot(
            shard_id,
            address.block_slab_id(),
            address.offset(),
            address.length(),
            routing_bucket,
        );
        warm_batch.push((key, bytes.clone()));
    }
    let Some(bytes) = bytes else {
        return;
    };
    let points = match decode_feature_block_strict(&bytes) {
        PackedFeatureBlockDecode::Packed(points) => points,
        PackedFeatureBlockDecode::Legacy => Vec::new(),
        PackedFeatureBlockDecode::Corrupt(_) => {
            VIEW_REBUILD_PAGE_DECODE_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };
    if points.is_empty() {
        return;
    }
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
                if address.generation().unwrap_or(0) >= slot.get().generation().unwrap_or(0) {
                    slot.insert(address.clone());
                }
            }
        }
    }
}

pub(super) fn expected_live_block_object_id(shard_id: ShardId, entry: &LiveBlockEntry) -> u64 {
    stable_block_object_id(
        shard_id,
        entry.kind.as_str(),
        &entry.object_key,
    )
}

pub(super) fn validate_bucket_ownership_index(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> StorageBlockOwnershipValidation {
    validate_bucket_ownership_index_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        start_routing_bucket,
        end_routing_bucket,
    )
}

/// The same validation against live-block entries the caller ALREADY has.
///
/// `collect_live_block_entries` materializes every live block in the shard, and callers that need
/// several derived reports were each walking for their own copy. Taking a slice lets one walk
/// serve all of them. The wrapper above keeps the old signature for callers with nothing to share.
pub(super) fn validate_bucket_ownership_index_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> StorageBlockOwnershipValidation {
    let mut validation = StorageBlockOwnershipValidation::default();
    for entry in entries {
        let expected_object_id = expected_live_block_object_id(shard_id, entry);
        let expected_routing_bucket =
            block_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket);
        let expected_block_id = entry.address.block_id();
        // THE OBJECT HALF OF THIS COMPARISON IS GONE, AND DELETING IT IS THE POINT. It compared
        // the id an address CARRIED against the id the terms DERIVE. An address carries none now,
        // so both sides would come from `expected_live_block_object_id` and the test could only
        // ever compare a value with itself -- a check that cannot fail, which reads exactly like a
        // check that passes. The filing comparison below is unaffected: `filed_bucket()` is the key
        // of the map the walk was iterating, so it remains two independent answers.
        // WHERE THE BLOCK IS, against where its KEY routes -- two independent answers, which is what
        // this report has to compare. It used to read the bucket off the ADDRESS, and an address's
        // bucket was written by the same expression `expected_routing_bucket` is: the comparison
        // was between a value and a copy of itself, and only an address carrying NONE could make it
        // fire. `filed_bucket()` is the key of the map the walk was iterating, so a block filed in a
        // bucket its key does not route to -- a stale range, a hand-moved entry -- fails here.
        let bucket_mismatch = entry
            .filed_bucket()
            .is_some_and(|actual| actual != expected_routing_bucket);
        if entry.filed_bucket().is_none() {
            validation.missing_owner_block_refs =
                validation.missing_owner_block_refs.saturating_add(1);
        }
        let bucket_block_present = shard
            .bucket_index
            .bucket_map
            .get(&expected_routing_bucket)
            .is_some_and(|bucket| {
                bucket.object_index.contains(&expected_object_id)
                    && bucket.block_index.values().any(|page| {
                        page.address.block_slab_id() == entry.address.block_slab_id()
                            && page.address.offset() == entry.address.offset()
                            && page.address.length() == entry.address.length()
                            && page.address.block_id() == expected_block_id
                            && page.model_id == entry.kind
                    })
            });
        if !bucket_block_present {
            validation.missing_owner_block_refs =
                validation.missing_owner_block_refs.saturating_add(1);
        }
        if bucket_mismatch {
            validation
                .mismatches
                .push(StorageRecoveryBlockOwnerMismatch {
                    object_key: entry.object_key.to_string(),
                    block_slab_id: entry.address.block_slab_id(),
                    offset: entry.address.offset(),
                    expected_object_id,
                    // An address carries no object id to disagree, so a row here is always a
                    // FILING mismatch. Reported absent rather than echoing `expected_object_id`,
                    // which would read as agreement that was never tested.
                    actual_object_id: None,
                    expected_routing_bucket,
                    actual_routing_bucket: entry.filed_bucket(),
                });
        }
    }
    validation
}


/// What the live-block walk reports for a RELEASED bucket, including the FILING.
///
/// WHY THIS EXISTS. mx#1949 gave `LiveBlockEntry` a `filed_bucket()` so five readers could stop
/// guessing a block's bucket out of its key, and mutated the two lines that set it on the
/// released-bucket supplement. That mutant SURVIVED, and mx#1949 reported it as unkillable by
/// construction: the supplement admits a block only when `address.routing_bucket()` is `Some`, so
/// every reader's `address.routing_bucket().or(entry.filed_bucket())` short-circuits before the
/// filing is ever consulted.
///
/// The first half of that is right and is asserted below -- no reader of the five can see this
/// field on this path, so the supplement's filing is PRODUCTION-INERT. The second half does not
/// follow. `filed_bucket()` is the walk's own answer to "which bucket is this block in", and
/// `collect_bucket_index_live_block_entries` states its contract in place: what it returns is
/// what the bucket index WOULD say if nothing were released. A released bucket's block reporting
/// no filing breaks that contract at the walk's own output, whether or not a reader looks --
/// and the next reader added, or an existing one that stops finding an explicit bucket on the
/// address, reads bucket 0 instead of the bucket the block is in.
///
/// So the mutant is killable, by comparing the walk's answer against the SAME walk before the
/// release rather than against a reader downstream of it.
#[cfg(test)]
mod released_supplement_guards {
    use super::release_refusal_guards::releasable_bucket;
    use super::{collect_live_block_entries, release_bucket_blocks};
    use crate::engine::state::ShardState;
    use std::collections::BTreeMap;

    /// The walk's answer, keyed by block. Element by element, never a count: a count is equal on
    /// a walk that returns the right number of blocks with the wrong answers on all of them.
    fn filings(shard: &ShardState) -> BTreeMap<String, Option<u32>> {
        collect_live_block_entries(shard)
            .iter()
            .map(|entry| (entry.object_key.to_string(), entry.filed_bucket()))
            .collect()
    }

    /// THE CONTROL, and it fires on the plant and on nothing else.
    ///
    /// `filings` has to be able to report `None`, or the equality in the test below holds
    /// because the comparison cannot express a difference. The model-map arm of the same walk --
    /// taken when `bucket_map` is empty -- genuinely does not know a filing and answers `None`
    /// for every block, which is the plant. The bucket-index arm on the same blocks answers
    /// `Some`, which is what says the `None`s are the arm and not the fixture.
    #[test]
    fn the_filing_comparison_can_report_a_missing_filing() {
        let mut shard = ShardState::default();
        let control_key = releasable_bucket(&mut shard, 7, "control-key");

        let from_the_index = filings(&shard);
        assert_eq!(
            from_the_index.get(&control_key),
            Some(&Some(7)),
            "the bucket-index arm must report a filing, or the plant below proves nothing",
        );

        // The plant: empty the bucket index so the walk takes its model-map arm, which has no
        // index to read a filing out of.
        shard.bucket_index.bucket_map.clear();
        let from_the_model_maps = filings(&shard);
        assert_eq!(
            from_the_model_maps.len(),
            1,
            "the model-map arm produced {} pages; if it produces none this control measures \
             nothing",
            from_the_model_maps.len(),
        );
        assert_eq!(
            from_the_model_maps.get(&control_key),
            Some(&None),
            "the comparison used below cannot express a missing filing, so its equality is \
             satisfied by a walk that reports nothing at all",
        );
    }

    /// A RELEASED bucket's blocks report the bucket they are filed in, exactly as before the
    /// release.
    ///
    /// The assertion is SET EQUALITY against the walk's own answer taken before the release --
    /// not a count, and not "every filing is non-empty". Doing too little here is silent: a block
    /// whose filing has been dropped is still returned, still live, still the right block, and
    /// reads as a block that simply has no bucket.
    #[test]
    fn a_released_buckets_pages_still_report_the_bucket_they_are_filed_in() {
        let mut shard = ShardState::default();
        let released_key = releasable_bucket(&mut shard, 7, "released-key");
        let resident_key = releasable_bucket(&mut shard, 9, "resident-key");
        assert_ne!(
            released_key, resident_key,
            "the two fixture keys must differ, or one page overwrote the other",
        );

        let before = filings(&shard);
        assert_eq!(
            before,
            BTreeMap::from([
                (released_key.clone(), Some(7u32)),
                (resident_key.clone(), Some(9u32)),
            ]),
            "the fixture does not start in the state this test compares against",
        );

        let outcome = release_bucket_blocks(&mut shard, &[7]);

        // THE DENOMINATOR. Every assertion below is about the supplement, and the supplement
        // runs only for a bucket that was actually released.
        assert_eq!(outcome.released_buckets, vec![7], "{outcome:?}");
        assert!(
            shard.bucket_index.released_buckets.contains(&7),
            "nothing was released, so the supplement walk never runs: {outcome:?}",
        );
        assert!(
            shard
                .bucket_index
                .bucket_map
                .get(&7)
                .expect("node kept")
                .block_index
                .is_empty(),
            "bucket 7 still holds its pages in the index, so they come from the index arm and \
             not from the supplement this test is about",
        );

        let after = filings(&shard);
        assert_eq!(
            after, before,
            "the walk's answer changed across a release. Its contract is that what it returns \
             is what the bucket index WOULD say if nothing were released -- the filing included",
        );
        // Named separately, so a failure says which half moved rather than printing two maps.
        assert_eq!(
            after.get(&released_key),
            Some(&Some(7)),
            "the page of the RELEASED bucket lost the bucket it is filed in",
        );
        assert_eq!(
            after.get(&resident_key),
            Some(&Some(9)),
            "the page of the bucket that was NOT released moved, which is a different defect",
        );
    }

    /// AND THE SUPPLEMENT'S FILING IS NOW WHAT EVERY READER GETS -- it was production-inert, and
    /// that is exactly what changed.
    ///
    /// mx#1949's five readers spelled `address.routing_bucket().or(entry.filed_bucket())`, and the
    /// supplement admitted a block only when the address carried a bucket -- so the left side always
    /// answered, the right side was never evaluated, and a mutant that broke the supplement's filing
    /// survived by construction.
    ///
    /// AN ADDRESS CARRIES NO BUCKET. The five readers are `entry.filed_bucket()` with a whole-range
    /// hash as the last resort, so the supplement's filing is the ONLY thing standing between a
    /// released bucket's block and a bucket the shard does not hold. What was inert is load-bearing,
    /// and this test is inverted to say so: it asserts the reader's own expression answers the
    /// SUPPLEMENT'S filing and not the fallback.
    #[test]
    fn every_reader_now_gets_the_supplements_filing_because_nothing_answers_before_it() {
        let mut shard = ShardState::default();
        let _inert_key = releasable_bucket(&mut shard, 7, "inert-key");
        let outcome = release_bucket_blocks(&mut shard, &[7]);
        assert_eq!(outcome.released_buckets, vec![7], "{outcome:?}");

        let entries = collect_live_block_entries(&shard);
        assert_eq!(entries.len(), 1, "the supplement returned {} pages", entries.len());
        for entry in &entries {
            assert_eq!(
                entry.filed_bucket(),
                Some(7),
                "a supplemented page that reports no filing sends every one of the five readers to \
                 `bucket_for_object(key, 0, u32::MAX)`, which on a shard loaded on a narrow range \
                 names a bucket it does not hold. This walk is the only answer now.",
            );
            // The reader's own expression, spelled out. There is no left side any more, which is
            // the statement: the fallback below is reached only when the walk does not know.
            assert_ne!(
                entry.filed_bucket(),
                None,
                "the five readers' whole-keyspace fallback is reachable on the supplement path",
            );
        }
    }
}

/// Guards for the preconditions [`release_bucket_blocks`] refuses on.
///
/// WHY THESE EXIST. The refusal outcome was produced and never checked. Across the whole crate
/// `bucket_index_release_refused` occurred exactly once outside its own definition and
/// assignment -- as a format argument inside another assertion's failure message -- so every
/// precondition in this function could be deleted with the suite still green. Four were: the
/// dirty-bucket term, the dirty-block term, the addressable-kind term and the model-map
/// agreement term.
///
/// HOW THEY ARE BUILT. Every guard starts from `releasable_bucket`, which satisfies EVERY
/// precondition and is released -- asserted by `a_bucket_that_satisfies_every_precondition_is_released`,
/// which is this module's denominator. A guard then breaks exactly ONE term and asserts both
/// halves: that the release did NOT happen, and that the refusal is attributed to ITS term and
/// to no other. Asserting only "a refusal happened" would pass when the wrong term fired, which
/// is how eleven conditions came to share one unassertable number.
#[cfg(test)]
mod release_refusal_guards {
    use super::{
        release_bucket_blocks, released_model_kind_is_addressable, BucketReleaseRefusals,
    };
    use crate::block_store::BlockAddress;
    use crate::engine::state::{BlockIndex, BlockIndexMap, BucketFlags, BucketNode, ObjectIndex, ShardState};
    use std::sync::Arc;

    const OBJECT_ID: u64 = 22;

    /// THE RANGE THE FIXTURE STAMPS, AND WHY IT IS NARROW.
    ///
    /// A block's bucket is `block_routing_bucket(object_key, start, end)`. The guards here choose the
    /// BUCKET -- they assert on it by number -- so the KEY is what has to be chosen to match, and a
    /// search for one terminates in about `end + 1` tries. Over the whole keyspace that is four
    /// billion. 128 buckets is enough for every bucket id these guards name.
    const RELEASE_RANGE_END: u32 = 127;

    /// A key that routes to `routing_bucket` on [`RELEASE_RANGE_END`], found by trying suffixes.
    ///
    /// WHY THE FIXTURE HAS TO DO THIS. `release_bucket_blocks` refuses a bucket holding a block whose
    /// key does not route to it (`BlockRoutingMismatch`), and it has to: a release is reversible only
    /// because `reload_released_bucket` re-derives that bucket's blocks from the model maps by the SAME
    /// expression, so a block filed where its key does not route would simply be lost by the reload.
    /// The fixture used to stamp the bucket onto the block's address, which is how it could name a
    /// bucket and a key independently; an address carries no bucket now.
    fn key_routing_to(prefix: &str, routing_bucket: u32) -> String {
        assert!(
            routing_bucket <= RELEASE_RANGE_END,
            "bucket {routing_bucket} is outside the fixture's own range 0..{RELEASE_RANGE_END}, so \
             no key can route to it and the search below would spin"
        );
        for suffix in 0..100_000u32 {
            let key = format!("{prefix}-{suffix}");
            if crate::engine::hashing::block_routing_bucket(&key, 0, RELEASE_RANGE_END)
                == routing_bucket
            {
                return key;
            }
        }
        panic!("no key with prefix {prefix} routes to bucket {routing_bucket} in 100,000 tries");
    }

    fn address(routing_bucket: u32, length: u64) -> BlockAddress {
        BlockAddress::from_parts(
            3,
            128,
            length,
            Some(11),
            Some(OBJECT_ID),
        )
    }

    fn block(key: &str, model_id: &str, component: Option<&str>, address: BlockAddress) -> BlockIndex {
        BlockIndex {
            object_key: Arc::from(key),
            model_id: super::stored_model_kind(model_id),
            component: component.map(Arc::from),
            address,
            dirty: false,
            deleted: false,
            log_backed: false,
        }
    }

    fn node(routing_bucket: u32, held: BlockIndex) -> BucketNode {
        BucketNode {
            routing_bucket,
            flags: BucketFlags::default().with(BucketFlags::DIRTY, false).with(BucketFlags::DELETED, false).with(BucketFlags::META_LOADED, true).with(BucketFlags::LOADING, false).with(BucketFlags::IN_MEMORY, true),
            object_index: ObjectIndex::One(OBJECT_ID),
            // The single-block arm, which holds its entry behind a POINTER rather than inline.
            block_index: BlockIndexMap::One(1, Box::new(held)),
            ..BucketNode::default()
        }
    }

    /// One bucket that satisfies every precondition: resident, not loading, clean, undeleted,
    /// holding one clean, undeleted, correctly routed `string` block that the model maps derive
    /// exactly. Anything a guard changes is a change from THIS.
    ///
    /// RETURNS THE KEY IT USED, which the caller names by prefix rather than in full: the key has to
    /// route to `routing_bucket` and only a search can produce one. See [`key_routing_to`].
    pub(super) fn releasable_bucket(
        shard: &mut ShardState,
        routing_bucket: u32,
        key_prefix: &str,
    ) -> String {
        // The shard must carry the range the key was chosen on, or `release_bucket_blocks` derives
        // the block's bucket over the whole keyspace and refuses every fixture here.
        shard.set_routing_range(0, RELEASE_RANGE_END);
        let key = key_routing_to(key_prefix, routing_bucket);
        let held = address(routing_bucket, 64);
        shard.strings.insert(key.as_str().into(), held.clone());
        shard
            .bucket_index
            .bucket_map
            .insert(routing_bucket, node(routing_bucket, block(&key, "string", None, held)));
        key
    }

    /// A releasable bucket for a key that is GIVEN, with the bucket DERIVED and returned.
    ///
    /// The scale fixtures want N distinct buckets and do not care which; searching for a key per
    /// chosen bucket would cost a coupon-collector's sweep over thousands of them. So they give the
    /// key and take the bucket -- and they take it over the WHOLE keyspace, where four thousand keys
    /// land in four thousand distinct buckets almost surely, which is what those fixtures assert.
    ///
    /// The refusal guards cannot use this: they name a bucket by number and assert on it, so for them
    /// the key is what has to be chosen. See [`key_routing_to`].
    pub(super) fn releasable_bucket_for_key(shard: &mut ShardState, key: &str) -> u32 {
        shard.set_routing_range(0, u32::MAX);
        let routing_bucket = crate::engine::hashing::block_routing_bucket(key, 0, u32::MAX);
        let held = address(routing_bucket, 64);
        shard.strings.insert(key.to_string().into_boxed_str(), held.clone());
        shard
            .bucket_index
            .bucket_map
            .insert(routing_bucket, node(routing_bucket, block(key, "string", None, held)));
        routing_bucket
    }

    fn releasable_shard(routing_bucket: u32, key_prefix: &str) -> (ShardState, String) {
        let mut shard = ShardState::default();
        let key = releasable_bucket(&mut shard, routing_bucket, key_prefix);
        (shard, key)
    }

    /// The same bucket, whose one block carries an unwritten change. The model maps still derive
    /// it -- `dirty` is not part of a block's identity -- so the map-agreement term is satisfied
    /// and the dirty-block term is the only one left to refuse on.
    fn bucket_holding_a_dirty_block(
        shard: &mut ShardState,
        routing_bucket: u32,
        key_prefix: &str,
    ) -> String {
        shard.set_routing_range(0, RELEASE_RANGE_END);
        let key = key_routing_to(key_prefix, routing_bucket);
        let held = address(routing_bucket, 64);
        shard.strings.insert(key.as_str().into(), held.clone());
        let dirty = BlockIndex {
            dirty: true,
            ..block(&key, "string", None, held)
        };
        shard
            .bucket_index
            .bucket_map
            .insert(routing_bucket, node(routing_bucket, dirty));
        key
    }

    /// THE DENOMINATOR. Without this every guard below could pass because the fixture never
    /// released anything, which is the shape that makes a refusal guard worthless.
    #[test]
    fn a_bucket_that_satisfies_every_precondition_is_released() {
        let (mut shard, _key) = releasable_shard(7, "denominator-key");
        let outcome = release_bucket_blocks(&mut shard, &[7]);

        assert_eq!(outcome.released_buckets, vec![7], "{outcome:?}");
        assert_eq!(outcome.released_blocks, 1, "{outcome:?}");
        assert_eq!(outcome.refused_buckets, 0, "{outcome:?}");
        assert_eq!(outcome.refusals, BucketReleaseRefusals::default(), "{outcome:?}");

        // What release is FOR, and what makes a released bucket ineligible for re-selection.
        let bucket = shard.bucket_index.bucket_map.get(&7).expect("node kept");
        assert!(bucket.block_index.is_empty(), "the block index was not cleared");
        assert!(!bucket.in_memory(), "a released bucket must not read as resident");
        assert_eq!(
            bucket.object_index.object_count(),
            1,
            "object_index is what keeps a released bucket countable and must survive",
        );
        assert!(shard.bucket_index.released_buckets.contains(&7));
    }

    /// TERM: `bucket.dirty`. A dirty bucket still pins the log, and releasing it while
    /// `eviction_dump_before_evict` is false leaves undumped writes with nothing to rebuild
    /// them from.
    #[test]
    fn a_dirty_bucket_is_refused_and_the_refusal_names_the_dirty_bucket_term() {
        let (mut shard, _key) = releasable_shard(7, "dirty-bucket-key");
        shard
            .bucket_index
            .bucket_map
            .get_mut(&7)
            .expect("fixture bucket")
            .set_dirty(true);

        let outcome = release_bucket_blocks(&mut shard, &[7]);

        assert!(outcome.released_buckets.is_empty(), "a dirty bucket was released: {outcome:?}");
        assert_eq!(outcome.released_blocks, 0, "{outcome:?}");
        assert_eq!(outcome.refused_buckets, 1, "{outcome:?}");
        assert_eq!(
            outcome.refusals,
            BucketReleaseRefusals { bucket_dirty: 1, ..BucketReleaseRefusals::default() },
            "the refusal was not attributed to the dirty-bucket term",
        );
        assert!(
            shard.bucket_index.bucket_map.get(&7).expect("node kept").in_memory(),
            "a refused bucket must stay resident and re-selectable",
        );
    }

    /// TERM: `!block.dirty`. The model maps carry no per-block dirty bit, so a release that had
    /// to restore one could not.
    #[test]
    fn a_dirty_block_is_refused_and_the_refusal_names_the_dirty_block_term() {
        let mut shard = ShardState::default();
        let _key = bucket_holding_a_dirty_block(&mut shard, 7, "dirty-block-key");

        let outcome = release_bucket_blocks(&mut shard, &[7]);

        assert!(
            outcome.released_buckets.is_empty(),
            "a bucket holding a dirty block was released: {outcome:?}",
        );
        assert_eq!(outcome.released_blocks, 0, "{outcome:?}");
        assert_eq!(outcome.refused_buckets, 1, "{outcome:?}");
        assert_eq!(
            outcome.refusals,
            BucketReleaseRefusals { block_dirty: 1, ..BucketReleaseRefusals::default() },
            "the refusal was not attributed to the dirty-block term",
        );
    }

    /// TERM: `released_model_kind_is_addressable`. A `hash` block is derivable by the model walk
    /// -- so the map-agreement term is SATISFIED here, and the kind term is the only thing left
    /// to refuse on. A hash is still refused, but the REASON narrowed: `hashes` is durable now, so
    /// precondition 1 (its map must survive serialization) no longer excludes it. Precondition 2
    /// does: a hash is read WHOLE through `bucket_index_component_block_addresses` and
    /// `model_map_block_address` offers no point lookup for it, so a released hash bucket would
    /// answer `HashGetAll` and the length with nothing.
    #[test]
    fn an_unaddressable_kind_is_refused_and_the_refusal_names_the_kind_term() {
        let mut shard = ShardState::default();
        shard.set_routing_range(0, RELEASE_RANGE_END);
        let key = key_routing_to("hash-kind-key", 7);
        let held = address(7, 64);
        shard
            .hashes
            .entry(key.clone())
            .or_default()
            .insert("field".to_string(), held.clone());
        shard.bucket_index.bucket_map.insert(
            7,
            node(7, block(&key, "hash", Some("field"), held)),
        );

        let outcome = release_bucket_blocks(&mut shard, &[7]);

        assert!(
            outcome.released_buckets.is_empty(),
            "a bucket holding an unaddressable kind was released: {outcome:?}",
        );
        assert_eq!(outcome.released_blocks, 0, "{outcome:?}");
        assert_eq!(outcome.refused_buckets, 1, "{outcome:?}");
        assert_eq!(
            outcome.refusals,
            BucketReleaseRefusals {
                block_kind_not_addressable: 1,
                ..BucketReleaseRefusals::default()
            },
            "the refusal was not attributed to the kind term -- if this says \
             model_map_disagreement instead, the fixture stopped being derivable and the guard \
             would pass for the wrong reason",
        );
    }

    /// TERM: the block's KEY must route to the bucket holding it.
    ///
    /// THE TERM THAT REPLACED A COMPARISON WITH ITSELF, and the one guard this change owes. It used to
    /// read `block.address.routing_bucket() != Some(routing_bucket)` -- the block's own copy of its
    /// bucket against the bucket holding it, written by the same expression that filed it, so only an
    /// address carrying NONE could make it fire. It now compares where the block IS against where its
    /// KEY routes, which are two independent answers.
    ///
    /// AND IT IS LOAD-BEARING, not decorative: a release is reversible only because
    /// `reload_released_bucket` re-derives the bucket's blocks from the model maps by that same
    /// expression. A block filed where its key does not route would simply not be re-derived, so the
    /// release would lose it -- which is why the refusal has to fire and why this is the guard that
    /// says so.
    ///
    /// THE FIXTURE MOVES THE BLOCK, not the address: there is no address field left to tamper with.
    #[test]
    fn a_page_whose_key_does_not_route_here_is_refused_and_the_refusal_names_the_routing_term() {
        let mut shard = ShardState::default();
        let key = releasable_bucket(&mut shard, 7, "misfiled-key");

        // DENOMINATOR: the fixture starts releasable, which is what makes the move below the only
        // difference. `a_bucket_that_satisfies_every_precondition_is_released` is this module's
        // denominator for that, and this is the same fixture.
        assert_eq!(
            7,
            crate::engine::hashing::block_routing_bucket(&key, 0, RELEASE_RANGE_END),
            "the fixture's key must route to bucket 7 before it is moved, or the refusal below is \
             about the fixture and not about the move",
        );

        // THE MOVE: file the very same node under a bucket the key does not route to.
        let node = shard
            .bucket_index
            .bucket_map
            .remove(&7)
            .expect("the fixture bucket");
        let elsewhere = (0..=RELEASE_RANGE_END)
            .find(|candidate| {
                *candidate != crate::engine::hashing::block_routing_bucket(&key, 0, RELEASE_RANGE_END)
            })
            .expect("some bucket is not the key's");
        shard.bucket_index.bucket_map.insert(
            elsewhere,
            BucketNode { routing_bucket: elsewhere, ..node },
        );

        let outcome = release_bucket_blocks(&mut shard, &[elsewhere]);

        assert!(
            outcome.released_buckets.is_empty(),
            "a bucket holding a page whose key routes elsewhere was released: {outcome:?}",
        );
        assert_eq!(outcome.released_blocks, 0, "{outcome:?}");
        assert_eq!(outcome.refused_buckets, 1, "{outcome:?}");
        assert_eq!(
            outcome.refusals,
            BucketReleaseRefusals {
                block_routing_mismatch: 1,
                ..BucketReleaseRefusals::default()
            },
            "the refusal was not attributed to the routing term -- if it says \
             model_map_disagreement instead, the move broke the map agreement too and the guard \
             would pass for the wrong reason",
        );
    }

    /// The allow-list itself, on a NUMBER. Widening it to every kind -- which is what deleting
    /// the term amounts to -- changes 2 to 8.
    #[test]
    fn exactly_two_of_the_probed_model_kinds_are_releasable() {
        let probes = [
            "string",
            "context_node",
            "hash",
            "set",
            "zset",
            "list",
            "feature",
            "context_event",
        ];
        let admitted = probes
            .iter()
            .filter(|kind| released_model_kind_is_addressable(kind))
            .count();
        assert_eq!(
            admitted,
            2,
            "expected 2 releasable kinds of {} probed, got {}: {:?}",
            probes.len(),
            admitted,
            probes
                .iter()
                .filter(|kind| released_model_kind_is_addressable(kind))
                .collect::<Vec<_>>(),
        );
        assert!(released_model_kind_is_addressable("string"));
        assert!(released_model_kind_is_addressable("context_node"));
    }

    /// TERM: the resident set must EQUAL what the model maps derive. Here the map holds the same
    /// block at a different length, so a reload would rebuild a different address than the one
    /// released -- the disagreement this term exists to refuse across.
    #[test]
    fn a_model_map_disagreement_is_refused_and_the_refusal_names_the_map_term() {
        let (mut shard, key) = releasable_shard(7, "disagreement-key");
        shard.strings.insert(key.into_boxed_str(), address(7, 4_096));

        let outcome = release_bucket_blocks(&mut shard, &[7]);

        assert!(
            outcome.released_buckets.is_empty(),
            "released across a model-map disagreement: {outcome:?}",
        );
        assert_eq!(outcome.released_blocks, 0, "{outcome:?}");
        assert_eq!(outcome.refused_buckets, 1, "{outcome:?}");
        assert_eq!(
            outcome.refusals,
            BucketReleaseRefusals {
                model_map_disagreement: 1,
                ..BucketReleaseRefusals::default()
            },
            "the refusal was not attributed to the model-map term",
        );
    }

    /// All four in ONE batch beside a releasable bucket. This is the guard that the four terms
    /// are distinguishable from each other rather than four names for whichever fired first,
    /// and it pins the totals: 5 candidates in, 1 released, 4 refused, 4 attributed.
    #[test]
    fn four_refusal_terms_and_one_release_are_counted_separately_in_one_batch() {
        let mut shard = ShardState::default();
        let _releasable = releasable_bucket(&mut shard, 1, "batch-releasable");
        let _dirty_bucket = releasable_bucket(&mut shard, 2, "batch-dirty-bucket");
        let _dirty_block = bucket_holding_a_dirty_block(&mut shard, 3, "batch-dirty-block");
        let disagreement_key = releasable_bucket(&mut shard, 5, "batch-disagreement");

        shard.bucket_index.bucket_map.get_mut(&2).expect("fixture").set_dirty(true);
        let kind_key = key_routing_to("batch-kind", 4);
        let held = address(4, 64);
        shard
            .hashes
            .entry(kind_key.clone())
            .or_default()
            .insert("field".to_string(), held.clone());
        shard
            .bucket_index
            .bucket_map
            .insert(4, node(4, block(&kind_key, "hash", Some("field"), held)));
        shard.strings.insert(disagreement_key.into_boxed_str(), address(5, 4_096));

        let candidates = [1u32, 2, 3, 4, 5];
        let outcome = release_bucket_blocks(&mut shard, &candidates);

        assert_eq!(
            outcome.released_buckets,
            vec![1],
            "expected exactly the releasable bucket of {} candidates: {outcome:?}",
            candidates.len(),
        );
        assert_eq!(outcome.released_blocks, 1, "{outcome:?}");
        assert_eq!(
            outcome.refused_buckets,
            4,
            "expected 4 refusals of {} candidates: {outcome:?}",
            candidates.len(),
        );
        assert_eq!(
            outcome.refusals,
            BucketReleaseRefusals {
                bucket_dirty: 1,
                block_dirty: 1,
                block_kind_not_addressable: 1,
                model_map_disagreement: 1,
                ..BucketReleaseRefusals::default()
            },
            "the four refusals were not attributed one to each term",
        );
        assert_eq!(
            outcome.refusals.total(),
            outcome.refused_buckets,
            "the breakdown and the total disagree, so one of them is not counting refusals",
        );
    }
}


/// What releasing four buckets MATERIALIZES as the store grows.
///
/// Counted, not timed, and counted without the counting allocator: `BUCKET_SCOPED_MODEL_ENTRIES`
/// is a number the release path publishes about itself, so these guards run on the ordinary gate
/// rather than only under `alloc-probe`. That separation is the point. The eviction round's
/// allocation table in `storage_lifecycle_methods` MEASURES this cost; it asserts its own
/// apparatus and a vacuity floor, and it passes whether the release walks the store or not. These
/// two assert the behaviour.
///
/// The fixture is the refusal guards' own `releasable_bucket`, so every candidate here really is
/// released -- a store of refusals would derive the same entries and release nothing, and the
/// control below fails on exactly that.
#[cfg(test)]
mod release_walk_scale {
    use super::release_refusal_guards::releasable_bucket_for_key;
    use super::{
        bucket_scoped_model_entries, release_bucket_blocks, reload_released_bucket,
        reset_bucket_scoped_model_entries,
    };
    use crate::engine::state::ShardState;

    /// `buckets` releasable buckets, one live `string` block each, and THE BUCKETS THEY LANDED IN.
    ///
    /// The bucket is derived from the key rather than chosen, because a block filed where its key does
    /// not route is a bucket a release REFUSES -- see `releasable_bucket_for_key`. The caller takes its
    /// victims from the returned list rather than assuming they are numbered from one.
    fn shard_with(buckets: u32) -> (ShardState, Vec<u32>) {
        let mut shard = ShardState::default();
        let mut landed = Vec::with_capacity(buckets as usize);
        let mut index = 1u32;
        while index <= buckets {
            landed.push(releasable_bucket_for_key(
                &mut shard,
                &format!("walk-scale-key-{index}"),
            ));
            index += 1;
        }
        // DENOMINATOR: the keys really did land in distinct buckets. Over the whole keyspace a
        // collision is a one-in-two-million accident at this size, and if one ever happens the
        // per-bucket claims below would be measuring a bucket holding two blocks.
        let distinct: std::collections::BTreeSet<u32> = landed.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            landed.len(),
            "{} of {} keys collided into a shared bucket, so the fixture no longer holds one page \
             per bucket",
            landed.len() - distinct.len(),
            landed.len(),
        );
        (shard, landed)
    }

    /// What one release of the SAME four victims costs out of a store of `buckets`.
    ///
    /// Returns `(entries materialized, live blocks in the store, blocks released, buckets
    /// released)`. The live-block count is the denominator: it is what the walk used to
    /// materialize, and a fixture whose two sizes did not differ in it would make the claim below
    /// a comparison of a store with itself.
    fn release_four_of(buckets: u32) -> (u64, usize, usize, usize) {
        let (mut shard, landed) = shard_with(buckets);
        let live_pages = shard.strings.len();
        let candidates = [landed[0], landed[1], landed[2], landed[3]];

        reset_bucket_scoped_model_entries();
        let outcome = release_bucket_blocks(&mut shard, &candidates);
        let materialized = bucket_scoped_model_entries();

        assert_eq!(
            outcome.refused_buckets, 0,
            "a refused candidate makes this a different measurement: {outcome:?}",
        );
        (
            materialized,
            live_pages,
            outcome.released_blocks,
            outcome.released_buckets.len(),
        )
    }

    /// THE CONTROL, and it runs as its own test so a failure here cannot stop the claim below
    /// being made.
    ///
    /// The derivation must materialize the four victims' blocks. A counter wired to nothing, and a
    /// derivation that built nothing at all, both read zero at every store size -- and zero
    /// satisfies "does not grow with the store" perfectly while measuring nothing.
    #[test]
    fn the_release_derivation_materializes_the_four_victims_pages() {
        let (materialized, live_pages, released_blocks, released_buckets) = release_four_of(64);

        assert_eq!(
            released_buckets, 4,
            "the four victims must have been released, or there was nothing to derive for",
        );
        assert_eq!(released_blocks, 4, "one page per victim");
        assert!(
            live_pages > released_blocks,
            "the store must hold more than the victims, or the claim below is untestable; \
             {live_pages} live pages against {released_blocks} released",
        );
        assert_eq!(
            materialized, 4,
            "the derivation must materialize the four victims' pages; it materialized \
             {materialized} out of {live_pages} live in the store",
        );
    }

    /// THE CLAIM: releasing four buckets costs the four buckets, not the store.
    ///
    /// EXACT EQUALITY across an eight-fold store, not "sublinear" and not "fewer than the store".
    /// The defect this replaces materialized one entry per live block in the shard and then threw
    /// all but the victims' away, so a bound like "under the live-block count" would have passed on
    /// a walk that had merely got cheaper per entry.
    ///
    /// The whole-shard WALK remains, and remains necessary: `derive_released_block_identities`
    /// says why. What is asserted here is what it materializes.
    #[test]
    fn releasing_four_buckets_materializes_the_same_entries_however_large_the_store() {
        const SMALL: u32 = 500;
        const LARGE: u32 = 4_000;

        let (small, small_live, small_blocks, small_buckets) = release_four_of(SMALL);
        let (large, large_live, large_blocks, large_buckets) = release_four_of(LARGE);

        // VACUITY FLOOR, on the measured denominator rather than on the constants.
        assert!(
            large_live > small_live,
            "the two stores must differ in live pages, got {small_live} and {large_live}",
        );
        assert_eq!(
            (small_buckets, large_buckets),
            (4, 4),
            "both sizes must release the same four victims to be compared at all",
        );
        assert_eq!(
            (small_blocks, large_blocks),
            (4, 4),
            "both sizes must release the same four pages",
        );
        assert!(
            small > 0 && large > 0,
            "a derivation that materializes nothing at either size measured nothing",
        );
        assert_eq!(
            small, large,
            "releasing four buckets materialized {small} entries out of {small_live} live pages \
             and {large} out of {large_live}; the release must cost the victims, not the store",
        );
    }

    /// The same claim for the other half of the pair.
    ///
    /// `reload_released_bucket` used to walk the whole shard into owned entries for ONE bucket --
    /// a batch reload over N released buckets would have paid the store N times. The control is
    /// inside the assertion: a reload that installed nothing would materialize nothing, and the
    /// released-block count it is compared against is 1.
    #[test]
    fn reloading_one_bucket_materializes_only_that_buckets_pages() {
        const SMALL: u32 = 500;
        const LARGE: u32 = 4_000;

        fn reload_one_of(buckets: u32) -> (u64, usize) {
            let (mut shard, landed) = shard_with(buckets);
            let victim = landed[0];
            let live_pages = shard.strings.len();
            let outcome = release_bucket_blocks(&mut shard, &[victim]);
            assert_eq!(outcome.released_buckets, vec![victim], "{outcome:?}");

            reset_bucket_scoped_model_entries();
            let reloaded = reload_released_bucket(&mut shard, 1, victim);
            let materialized = bucket_scoped_model_entries();

            assert!(reloaded, "the bucket must have been reloaded, or nothing was walked for");
            assert_eq!(
                shard
                    .bucket_index
                    .bucket_map
                    .get(&victim)
                    .map(|bucket| bucket.block_index.len()),
                Some(1),
                "the reload must have installed the page it released",
            );
            (materialized, live_pages)
        }

        let (small, small_live) = reload_one_of(SMALL);
        let (large, large_live) = reload_one_of(LARGE);

        assert!(
            large_live > small_live,
            "the two stores must differ in live pages, got {small_live} and {large_live}",
        );
        assert_eq!(
            small, 1,
            "reloading one bucket materialized {small} entries out of {small_live} live pages",
        );
        assert_eq!(
            small, large,
            "reloading one bucket materialized {small} entries out of {small_live} live pages and \
             {large} out of {large_live}; a reload must cost the bucket, not the store",
        );
    }
}


/// Which live-block walks the scan counter can see.
///
/// [`LIVE_BLOCK_SCAN_ENTRIES`] is charged where the ENTRIES ARE BUILT, not where a wrapper
/// happens to be called, so a walk that materializes the shard is visible however it was
/// reached. These guards name each way in and assert it is charged; before the counting moved
/// down, the wrapper was the only charged route and every direct call to the two walks read
/// zero.
///
/// Process-wide counters: each guard resets immediately before the call it measures, and the
/// suite is read single-threaded -- the same contract as [`bucket_scoped_model_entries`].
#[cfg(test)]
mod live_block_scan_coverage {
    use super::release_refusal_guards::releasable_bucket_for_key;
    use super::{
        collect_bucket_index_live_block_entries, collect_live_block_entries,
        collect_model_live_block_entries, live_block_scan_entries, release_bucket_blocks,
        reset_live_block_scan_entries,
    };
    use crate::engine::state::ShardState;

    const BUCKETS: u32 = 64;

    /// `BUCKETS` buckets, one live `string` block each, and THE BUCKETS THEY LANDED IN.
    ///
    /// Derived rather than chosen, for the reason `releasable_bucket_for_key` gives: a block filed
    /// where its key does not route is a bucket a release refuses.
    fn shard_with(buckets: u32) -> (ShardState, Vec<u32>) {
        let mut shard = ShardState::default();
        let mut landed = Vec::with_capacity(buckets as usize);
        let mut index = 1u32;
        while index <= buckets {
            landed.push(releasable_bucket_for_key(
                &mut shard,
                &format!("scan-coverage-key-{index}"),
            ));
            index += 1;
        }
        let distinct: std::collections::BTreeSet<u32> = landed.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            landed.len(),
            "the fixture's keys collided into a shared bucket, so it no longer holds one page per \
             bucket",
        );
        (shard, landed)
    }

    /// THE FIXTURE'S OWN DENOMINATOR, as its own test so a failure here cannot stop the claims
    /// below being made. A shard whose blocks all sat in one bucket could not tell a whole-shard
    /// walk from a bucket-scoped one.
    #[test]
    fn the_fixture_spreads_its_pages_over_many_buckets() {
        let (shard, _landed) = shard_with(BUCKETS);

        assert_eq!(
            shard.strings.len(),
            BUCKETS as usize,
            "one live page per bucket",
        );
        assert_eq!(
            shard.bucket_index.bucket_map.len(),
            BUCKETS as usize,
            "the pages must occupy more than one bucket, or a bucket-scoped walk and a \
             whole-shard walk would materialize the same thing",
        );
        assert!(BUCKETS > 1, "a single-bucket fixture cannot express the defect");
    }

    /// A whole-shard model-map walk reached DIRECTLY, as five production callers reach it.
    #[test]
    fn a_direct_whole_shard_model_walk_is_counted() {
        let (shard, _landed) = shard_with(BUCKETS);

        reset_live_block_scan_entries();
        let entries = collect_model_live_block_entries(&shard);
        let counted = live_block_scan_entries();

        assert_eq!(
            entries.len(),
            BUCKETS as usize,
            "the walk must materialize the whole store, or there is nothing to charge for",
        );
        assert_eq!(
            counted,
            entries.len() as u64,
            "a direct whole-shard model walk materialized {} entries and the counter saw \
             {counted}",
            entries.len(),
        );
    }

    /// A bucket-index walk reached DIRECTLY, as three production callers reach it.
    #[test]
    fn a_direct_bucket_index_walk_is_counted() {
        let (shard, _landed) = shard_with(BUCKETS);

        reset_live_block_scan_entries();
        let entries = collect_bucket_index_live_block_entries(&shard);
        let counted = live_block_scan_entries();

        assert_eq!(
            entries.len(),
            BUCKETS as usize,
            "the walk must materialize every indexed page, or there is nothing to charge for",
        );
        assert_eq!(
            counted,
            entries.len() as u64,
            "a direct bucket-index walk materialized {} entries and the counter saw {counted}",
            entries.len(),
        );
    }

    /// The supplement a released bucket forces, on the WRAPPER's own path.
    ///
    /// `collect_bucket_index_live_block_entries` walks the whole shard a SECOND time whenever any
    /// bucket is released, to supplement the blocks the emptied index no longer names. Charging
    /// the wrapper's return value could not see that second walk: it reports the blocks returned,
    /// while the walk built the indexed blocks AND the whole shard. The two diverge by one block
    /// per block in the store, so the miss grows with the store rather than being a fixed offset.
    #[test]
    fn the_released_bucket_supplement_is_counted() {
        let (mut shard, landed) = shard_with(BUCKETS);
        let victim = landed[0];
        let outcome = release_bucket_blocks(&mut shard, &[victim]);
        assert_eq!(outcome.released_buckets, vec![victim], "{outcome:?}");

        let live_pages = shard.strings.len();
        let indexed_pages = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.block_index.len())
            .sum::<usize>();

        // DENOMINATOR: the release must have emptied exactly one bucket's index, or the second
        // walk below never runs and this guard measures nothing.
        assert_eq!(live_pages, BUCKETS as usize, "the model maps keep every page");
        assert_eq!(
            indexed_pages,
            live_pages - 1,
            "exactly one bucket's index must have been emptied by the release",
        );
        assert_eq!(
            shard.bucket_index.released_buckets.len(),
            1,
            "a released bucket is what triggers the supplement walk",
        );

        reset_live_block_scan_entries();
        let entries = collect_live_block_entries(&shard);
        let counted = live_block_scan_entries();

        assert_eq!(
            entries.len(),
            live_pages,
            "the supplement must put the released page back into the answer",
        );
        assert_eq!(
            counted,
            (indexed_pages + live_pages) as u64,
            "with one bucket released the wrapper materialized {indexed_pages} indexed pages \
             and then the whole {live_pages}-page shard again, and the counter saw {counted}",
        );
    }

    /// HOW MUCH the old charge missed, as a number that grows with the store.
    ///
    /// Charging the wrapper's RETURN value cost exactly the supplement walk: with one bucket
    /// released the walk materializes `2n - 1` entries for a store of `n` live blocks and returns
    /// `n` of them, so what a return-value charge could not see was `n - 1` -- 499 entries at 500
    /// blocks and 3,999 at 4,000. Not a fixed offset to be lived with: it is the store.
    #[test]
    fn what_a_return_value_charge_could_not_see_grows_with_the_store() {
        fn charged_and_returned(buckets: u32) -> (u64, usize) {
            let (mut shard, landed) = shard_with(buckets);
            let victim = landed[0];
            let outcome = release_bucket_blocks(&mut shard, &[victim]);
            assert_eq!(outcome.released_buckets, vec![victim], "{outcome:?}");

            reset_live_block_scan_entries();
            let entries = collect_live_block_entries(&shard);
            (live_block_scan_entries(), entries.len())
        }

        const SMALL: u32 = 500;
        const LARGE: u32 = 4_000;
        let (small_charged, small_returned) = charged_and_returned(SMALL);
        let (large_charged, large_returned) = charged_and_returned(LARGE);

        // VACUITY FLOOR on the measured denominator, not on the constants.
        assert!(
            large_returned > small_returned,
            "the two stores must differ in pages returned, got {small_returned} and \
             {large_returned}",
        );
        assert!(
            small_charged > 0 && large_charged > 0,
            "a walk charged nothing at either size measured nothing",
        );

        assert_eq!(
            small_charged,
            (2 * small_returned - 1) as u64,
            "at {small_returned} live pages the walk materializes the indexed pages and the \
             whole shard; it charged {small_charged}",
        );
        assert_eq!(
            large_charged,
            (2 * large_returned - 1) as u64,
            "at {large_returned} live pages the walk materializes the indexed pages and the \
             whole shard; it charged {large_charged}",
        );

        let small_unseen = small_charged - small_returned as u64;
        let large_unseen = large_charged - large_returned as u64;
        assert!(
            large_unseen > small_unseen * 4,
            "what a return-value charge misses must grow with the store, not sit at a fixed \
             offset: {small_unseen} unseen at {small_returned} pages and {large_unseen} at \
             {large_returned}",
        );
    }
}

/// THE MODEL-KIND REGISTRY AGAINST THE WALK THAT IS ITS AUTHORITY.
///
/// The registry is derived from one declaration and the walk's `emit` takes a `ModelKind`, so the
/// direction "an arm names a kind the registry does not have" is a type error and needs no test.
/// What a compiler cannot see is the other direction and the report codes, and that is all this
/// module is:
///
///   1. every variant the registry declares is actually EMITTED by a walk over a shard holding
///      one block in each map -- set equality by name, with the count floored on
///      `ModelKind::ALL.len()` so a derivation that produced fewer would fail rather than pass
///      vacuously;
///   2. `zset` and `list`, the two kinds that used to fall through to 0, now pack as their own
///      codes, driven through `storage_physical_index_report` rather than asserted against the
///      table;
///   3. an unrecognised model id REFUSES and names itself, with a retired spelling as the
///      control that the refusal is not simply "anything unusual panics";
///   4. no code is 0 and no two collide, live and retired together.
#[cfg(test)]
mod model_kind_registry_guards {
    use super::{
        collect_model_live_block_entries, model_report_code, ModelKind,
        RETIRED_MODEL_REPORT_CODES,
    };
    use crate::block_store::BlockAddress;
    use crate::engine::state::ShardState;
    use std::collections::{BTreeMap, BTreeSet};

    fn address(routing_bucket: u32) -> BlockAddress {
        BlockAddress::from_parts(3, 128, 64, Some(11), Some(22))
    }

    /// ONE BLOCK IN EVERY MAP THE WALK READS.
    ///
    /// Written map by map rather than through commands on purpose: a command path that stopped
    /// filing one of these kinds would make the completeness check below pass by holding fewer
    /// kinds, which is the failure it exists to catch. Every field here is a field of
    /// `ShardState` that an arm of `visit_model_live_blocks` reads.
    fn shard_holding_one_page_of_every_kind() -> ShardState {
        let mut shard = ShardState::default();
        let at = address(7);
        shard.strings.insert("s".into(), at.clone());
        shard
            .hashes
            .insert("h".to_string(), [("f".to_string(), at.clone())].into_iter().collect());
        shard
            .zsets
            .insert("z".to_string(), BTreeMap::from([(vec![1u8], (9u64, at.clone()))]));
        shard
            .lists
            .insert("l".to_string(), BTreeMap::from([(0i64, at.clone())]));
        shard
            .sets
            .insert("t".to_string(), BTreeMap::from([(vec![2u8], at.clone())]));
        shard
            .features
            .insert("f".to_string(), BTreeMap::from([(1u64, at.clone())]));
        shard.control_state_blocks.insert("c".into(), at.clone());
        shard.context_nodes.insert("ctx:node:t:n".into(), at.clone());
        shard
            .context_events
            .insert("ctx:event:t:n".to_string(), BTreeMap::from([(1u64, at.clone())]));
        shard
            .context_indexes
            .insert("ctx:index:t:n".to_string(), BTreeMap::from([(1u64, at.clone())]));
        shard
            .context_audits
            .insert("ctx:audit:t:n".to_string(), BTreeMap::from([(1u64, at.clone())]));
        shard
            .context_entities
            .insert("ctx:entity:t:n".to_string(), BTreeMap::from([(1u64, at.clone())]));
        shard
            .context_children
            .insert("ctx:child:t:n".to_string(), BTreeMap::from([(1u64, at.clone())]));
        shard
            .context_summaries
            .insert("ctx:summary:t:n".to_string(), BTreeMap::from([(1u64, at.clone())]));
        shard
            .context_compressions
            .insert("ctx:compression:t:n".to_string(), BTreeMap::from([(1u64, at)]));
        shard
    }

    /// EVERY DECLARED KIND IS EMITTED, and the comparison is a set of NAMES.
    ///
    /// A count would be satisfied by fifteen blocks of one kind. The floor is separate from the
    /// equality and states the denominator: a registry that had silently shrunk would make the
    /// equality trivially true against a walk that had shrunk with it, and
    /// `ModelKind::ALL.len() >= 15` in the declaration is the other half of that.
    #[test]
    fn the_walk_emits_every_model_kind_the_registry_declares() {
        let shard = shard_holding_one_page_of_every_kind();
        let entries = collect_model_live_block_entries(&shard);

        let mut per_kind: BTreeMap<String, usize> = BTreeMap::new();
        for entry in &entries {
            *per_kind.entry(entry.kind.to_string()).or_default() += 1;
        }
        println!("\n=== what the walk emitted, per kind ===");
        println!("  {:<24} {:>6}", "kind", "pages");
        for (kind, count) in &per_kind {
            println!("  {kind:<24} {count:>6}");
        }
        println!("  {:<24} {:>6}", "TOTAL", entries.len());
        println!("  declared kinds: {}", ModelKind::ALL.len());

        let emitted: BTreeSet<String> = per_kind.keys().cloned().collect();
        let declared: BTreeSet<String> = ModelKind::ALL
            .iter()
            .map(|kind| kind.as_str().to_string())
            .collect();

        assert_eq!(
            declared.len(),
            ModelKind::ALL.len(),
            "two variants of `ModelKind` share a stored spelling, so the registry cannot map a \
             stored name back to one kind: {declared:?}",
        );
        assert!(
            emitted.len() >= ModelKind::ALL.len(),
            "the walk emitted {} distinct kinds over a shard holding one page in every map, and \
             the registry declares {}; with fewer the equality below can hold while both sides \
             have shrunk",
            emitted.len(),
            ModelKind::ALL.len(),
        );
        assert_eq!(
            declared, emitted,
            "the registry and the walk name different kinds. Declared but not emitted: {:?}. \
             Emitted but not declared: {:?}. The second is impossible while `emit` takes a \
             `ModelKind`; the first is a variant no arm reaches, which gives the reporting path a \
             code for a kind that cannot occur.",
            declared.difference(&emitted).collect::<Vec<_>>(),
            emitted.difference(&declared).collect::<Vec<_>>(),
        );
    }

    /// THE TWO RETIRED SPELLINGS ARE RETIRED, and the walk is what says so.
    ///
    /// If either ever became live again this would fail, which is the point: a retired row whose
    /// kind is emitted is a kind whose code is reachable from two lists.
    #[test]
    fn the_retired_model_spellings_are_not_emitted_by_any_arm() {
        let shard = shard_holding_one_page_of_every_kind();
        let emitted: BTreeSet<String> = collect_model_live_block_entries(&shard)
            .iter()
            .map(|entry| entry.kind.to_string())
            .collect();

        println!("\n=== retired spellings ===");
        println!("  {:<24} {:>5}  emitted?", "name", "code");
        for (name, code) in RETIRED_MODEL_REPORT_CODES {
            println!("  {name:<24} {code:>5}  {}", emitted.contains(*name));
            assert!(
                !emitted.contains(*name),
                "`{name}` is declared retired and an arm emits it; it belongs under `live`",
            );
            assert!(
                ModelKind::from_stored_name(name).is_none(),
                "`{name}` is declared retired and is also a live variant",
            );
            assert_eq!(
                model_report_code(name), *code,
                "a retired spelling must still report the code it had, or a report over an older \
                 index renames the kind it finds",
            );
        }
        assert_eq!(5, model_report_code("sequence"));
        assert_eq!(15, model_report_code("context_embedding"));
    }

    /// NO CODE IS 0 AND NO TWO COLLIDE -- the runtime mirror of the declaration's const asserts,
    /// printed, because a const assert that fires says nothing about which two rows collided.
    #[test]
    fn no_two_model_report_codes_collide_and_none_is_the_empty_bucket_code() {
        let mut by_code: BTreeMap<u8, Vec<String>> = BTreeMap::new();
        for kind in ModelKind::ALL {
            by_code
                .entry(kind.report_code())
                .or_default()
                .push(format!("{} (live)", kind.as_str()));
        }
        for (name, code) in RETIRED_MODEL_REPORT_CODES {
            by_code
                .entry(*code)
                .or_default()
                .push(format!("{name} (retired)"));
        }
        println!("\n=== the packed model byte, code by code ===");
        println!("  {:>5}  {}", "code", "kinds");
        for (code, names) in &by_code {
            println!("  {code:>5}  {}", names.join(", "));
        }
        let rows = ModelKind::ALL.len() + RETIRED_MODEL_REPORT_CODES.len();
        assert_eq!(
            rows,
            by_code.len(),
            "{rows} declared rows share {} codes, so at least two kinds pack as one byte",
            by_code.len(),
        );
        assert!(
            !by_code.contains_key(&0),
            "a kind packs as 0, which is the byte a bucket holding no page writes",
        );
    }

    /// AN UNRECOGNISED MODEL ID REFUSES, AND THE REFUSAL NAMES IT.
    ///
    /// The old arm returned 0 -- the same byte as an empty bucket -- so a store the engine did
    /// not understand reported as a store with nothing in it.
    #[test]
    #[should_panic(expected = "no packed report code for model id \"not_a_model_kind\"")]
    fn an_unrecognised_model_id_refuses_rather_than_reporting_the_empty_bucket_code() {
        model_report_code("not_a_model_kind");
    }

    /// THE LIVE DEFECT, DRIVEN THROUGH THE REPORT RATHER THAN AGAINST THE TABLE.
    ///
    /// `zset` and `list` blocks reached `native_packed_block_index_bytes` and came out with byte 1
    /// set to 0 -- the value an unrecognised kind got. This walks the shard into the physical
    /// index report the way the reporting path does and reads the byte back out of the published
    /// hex, so it fails if the registry is right and the packing stops using it.
    #[test]
    fn a_zset_and_a_list_page_pack_as_their_own_kind_and_not_as_the_empty_code() {
        let shard = shard_holding_one_page_of_every_kind();
        let report = crate::engine::storage_reporting::storage_physical_index_report(
            1,
            &shard,
            Vec::new(),
        );

        let mut packed: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for bucket in &report.bucket_nodes {
            for page in &bucket.block_indexes {
                let bytes = hex::decode(&page.native_packed_block_index_hex)
                    .expect("the report publishes hex");
                packed
                    .entry(page.model_id.clone())
                    .or_default()
                    .push(bytes[1]);
            }
        }
        println!("\n=== the packed model byte, per kind, off the report ===");
        println!("  {:<24} {:>6}  {}", "model_id", "pages", "byte 1");
        for (model_id, codes) in &packed {
            println!("  {model_id:<24} {:>6}  {codes:?}", codes.len());
        }

        assert_eq!(
            packed.len(),
            ModelKind::ALL.len(),
            "the report published {} kinds where the registry declares {}; a report that does \
             not carry both of the kinds under test cannot show their byte",
            packed.len(),
            ModelKind::ALL.len(),
        );
        for name in ["zset", "list"] {
            let codes = packed
                .get(name)
                .unwrap_or_else(|| panic!("the report carries no `{name}` page"));
            let expected = ModelKind::from_stored_name(name)
                .expect("both are live kinds")
                .report_code();
            assert!(
                codes.iter().all(|code| *code == expected),
                "a `{name}` page packs as {codes:?}, not {expected}; 0 is the byte an empty \
                 bucket writes and is what this kind used to get",
            );
        }
        // The denominator: no block in the whole report packs as the empty-bucket byte.
        let zeroes: Vec<&String> = packed
            .iter()
            .filter(|(_, codes)| codes.iter().any(|code| *code == 0))
            .map(|(name, _)| name)
            .collect();
        assert!(
            zeroes.is_empty(),
            "these kinds still pack as 0, which is the byte that means the bucket names no page: \
             {zeroes:?}",
        );
    }

    /// THE CONTROL ON THAT REFUSAL: the names that must NOT panic.
    ///
    /// Without this the guard above is satisfied by a `model_report_code` that panics on
    /// everything, which would refuse every live kind as well.
    #[test]
    fn every_live_and_retired_spelling_answers_without_refusing() {
        println!("\n=== live kinds ===");
        for kind in ModelKind::ALL {
            let code = model_report_code(kind.as_str());
            println!("  {:<24} {:>3}", kind.as_str(), code);
            assert_eq!(
                code,
                kind.report_code(),
                "`{}` reports a different code through the stored-name lookup than off the \
                 variant, so the two halves of the derivation disagree",
                kind.as_str(),
            );
        }
        for (name, code) in RETIRED_MODEL_REPORT_CODES {
            assert_eq!(model_report_code(name), *code);
        }
    }
}
