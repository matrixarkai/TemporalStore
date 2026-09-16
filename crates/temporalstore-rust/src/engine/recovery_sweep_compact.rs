// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Index install/recovery + expiry sweep + page compaction methods for TemporalEngine, split from engine.rs.
use super::*;

/// OBSERVATION SEAM: was the shard-table write guard still held when a round PUBLISHED its
/// durable index?
///
/// `the_compaction_flush_stays_inside_its_write_guard` counts served-index ENCODES taken under
/// the guard, and the encode is only the first third of the flush. Dropping the guard between the
/// encode and `persist_index_bytes` leaves that count untouched -- the encode still happened
/// inside the region -- while opening the entire window the invariant exists to close: from the
/// moment the guard drops, `storage_lifecycle_plan` can take its own read lock, derive stale
/// slabs from a live set that no longer names the slabs this round vacated, and reclaim them,
/// while the durable index on disk is still the PRE-compaction one that does name them.
/// Compaction advances no `applied_wal_sequence` and emits no record, so nothing replays that
/// back.
///
/// Measured rather than asserted structurally because the guard is an RAII value: whether it is
/// still alive at a given statement is a fact about the generated code, and reading the
/// write-guard depth at the publish point is the only way to ask.
#[cfg(test)]
thread_local! {
    static FLUSH_PUBLISHED_UNDER_GUARD: std::cell::Cell<Option<bool>> =
        const { std::cell::Cell::new(None) };
}

/// Record the write-guard depth at the moment the round publishes. Called once per round.
#[cfg(test)]
fn note_compaction_flush_publish() {
    let held = super::shard_write_guard::held();
    FLUSH_PUBLISHED_UNDER_GUARD.with(|cell| cell.set(Some(held)));
}

/// `None` until a round has published since the last reset; then whether the guard was held.
#[cfg(test)]
pub(crate) fn compaction_flush_published_under_guard_for_test() -> Option<bool> {
    FLUSH_PUBLISHED_UNDER_GUARD.with(|cell| cell.get())
}

#[cfg(test)]
pub(crate) fn reset_compaction_flush_publish_observation_for_test() {
    FLUSH_PUBLISHED_UNDER_GUARD.with(|cell| cell.set(None));
}

/// The same predicate the seam reads, exposed so a test can show it is capable of saying `false`.
#[cfg(test)]
pub(crate) fn shard_write_guard_held_for_test() -> bool {
    super::shard_write_guard::held()
}

/// What an expiry round hands to its flush: a description of what the round REMOVED, or the
/// whole served index.
///
/// Two shapes rather than one because the whole-index write is the measurement arm's subject --
/// see `expiry_index_flush_whole`. Production only ever builds the delta.
enum ExpiryIndexCheckpoint {
    /// O(what changed): one index-log delta record naming the keys this round removed.
    Delta(Box<ExpiryIndexDelta>),
    /// O(shard): the entire served index, re-encoded and rewritten.
    Whole(Box<ShardState>),
}

/// The delta record an expiry round writes, built under the shard write guard and appended after
/// it drops.
struct ExpiryIndexDelta {
    /// The page items the record carries. Always empty: the round's deletes leave no page for a
    /// covered key, and an empty list against a covered key is how the fold spells a removal.
    /// Kept as a field because it is what the record's wire shape has, and because a future
    /// checkpoint that DOES have pages to name would fill it rather than grow a second path.
    items: Vec<crate::index_log::IndexItem>,
    /// One blob per covered key, carrying no map fields: the fold reads an absent field as a
    /// removal, so these are tombstones across every per-key map, `expires_at_ms` included.
    key_states: Vec<serde_json::Value>,
    /// The WAL sequence this checkpoint reflects -- anchored AFTER the round's tombstones, so
    /// folding the anchor and folding the deletions are the same act.
    applied_wal_sequence: Option<u64>,
}

impl TemporalEngine {
    pub fn install_index_bytes(
        &self,
        shard_id: ShardId,
        bytes: &[u8],
    ) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        fs::write(self.index_path(shard_id), bytes)
    }

    pub fn storage_recovery_report(&self, shard_id: ShardId) -> StorageRecoveryReport {
        let mut report = self.storage_recovery_report_without_boundary(shard_id);
        report.boundary = self.storage_recovery_boundary_report(shard_id);
        report.slab_integrity =
            storage_slab_integrity_report(shard_id, &report, &report.boundary);
        report
    }

    /// The reclaim candidates, built once from the narrow view and once from the full report.
    ///
    /// Same selection function, two sources for the tally it reads. The narrow view leaves the
    /// read-dependent fields at zero, so this is the check that the planner never looked at
    /// them.
    #[cfg(test)]
    pub(crate) fn reclaim_candidates_two_ways_for_test(
        &self,
        shard_id: ShardId,
    ) -> (Vec<StorageReclaimCandidate>, Vec<StorageReclaimCandidate>) {
        let live = self
            .live_block_slab_ids(shard_id)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let stale = self
            .block_store
            .slab_ids()
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !live.contains(id))
            .collect::<BTreeSet<_>>();
        let narrow = storage_reclaim_candidates_from_slab_reports(
            &self.storage_reclaim_slab_reports(shard_id),
            &stale,
        );
        let full = storage_reclaim_candidates_from_slab_reports(
            &self.storage_recovery_report(shard_id).block_slab_live_reports,
            &stale,
        );
        (narrow, full)
    }

    /// Each dirty bucket's first undumped write sequence, for the test that pins it.
    #[cfg(test)]
    pub(crate) fn first_dirty_sequences_for_test(&self, shard_id: ShardId) -> Vec<(u32, u64)> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return Vec::new();
        };
        let mut out = shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.first_dirty_wal_sequence))
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    /// The per-slab live/stale tally the reclaim planner reads, without the whole-store scan.
    ///
    /// `storage_reclaim_candidates_from_slab_reports` consumes seven fields off each of these:
    /// the slab id, its physical bytes and page count, its live page refs and live physical
    /// bytes, and the two figures derived from those. Not one of them needs the page itself --
    /// a page's physical size is in the address that names it. The recovery report supplied
    /// them anyway, and supplied them by reading every live page off the block store to fill
    /// in `live_logical_bytes`, which the planner never reads.
    ///
    /// The fields that DO require the read -- `live_logical_bytes`, `readable_live_page_refs`,
    /// `unreadable_live_page_refs` -- are left at zero here, so this is not a drop-in for the
    /// report: it is the planner's view, and `the_reclaim_planner_sees_the_same_candidates`
    /// pins it to the answer the report produced.
    pub(super) fn storage_reclaim_slab_reports(
        &self,
        shard_id: ShardId,
    ) -> Vec<StorageRecoverySlabLiveReport> {
        // Counted by header walk, not `slab_reports()`. That function calls
        // `decode_block_record` on every record in every slab -- a CRC32C verify and a
        // decompress each -- which is a full integrity pass over the whole store, and the
        // selection below reads two fields out of it: the slab's size and its block count.
        // Measured at 32,000 records: 12.95 ms against 0.37 ms, 35x, with identical counts.
        //
        // `logical_bytes` is left at zero here for the same reason as the read-dependent
        // fields: `storage_reclaim_candidates_from_slab_reports` does not read it, and
        // `the_reclaim_planner_sees_the_same_candidates` is what holds that true.
        let block_slab_counts = self.block_store.slab_block_counts().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        // THE MAINTAINED TALLY FIRST, AND THE WALK ONLY IF THERE ISN'T ONE.
        //
        // Everything below builds a per-slab live/stale view out of `live_page_refs` and
        // `live_physical_bytes`, and those two are exactly what the shard now keeps a running
        // total of. Taking them from the tally makes this round cost the number of SLABS; walking
        // for them costs the number of live BLOCKS, on a loop that ticks every thirty seconds per
        // shard for the life of the process.
        //
        // The fallback is not decoration. A shard whose tally has never been derived reports
        // `is_ready() == false`, and an empty tally is indistinguishable from a shard holding no
        // live pages at all -- which would make every slab look like pure garbage. So an
        // underived tally costs the old cost rather than producing a confident wrong answer.
        //
        // `live_object_count` and `live_routing_bucket_count` are left at zero on the maintained
        // path, joining the read-dependent fields already documented above:
        // `storage_reclaim_candidates_from_slab_reports` reads neither, and
        // `the_reclaim_planner_sees_the_same_candidates` is what holds that true.
        let maintained = shards
            .get(&shard_id)
            .filter(|shard| shard.bucket_index.block_slab_live.is_ready())
            .map(|shard| shard.bucket_index.block_slab_live.iter().collect::<Vec<_>>());
        let addresses = match maintained {
            Some(_) => Vec::new(),
            None => shards
                .get(&shard_id)
                .map(collect_live_block_addresses)
                .unwrap_or_default(),
        };
        let mut reports = block_slab_counts
            .iter()
            .map(|(block_slab_id, physical_bytes, block_count)| {
                (
                    *block_slab_id,
                    StorageRecoverySlabLiveReport {
                        block_slab_id: *block_slab_id,
                        physical_bytes: *physical_bytes,
                        page_count: *block_count,
                        ..StorageRecoverySlabLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        if let Some(maintained) = maintained {
            for (block_slab_id, tally) in maintained {
                let slab_report =
                    reports
                        .entry(block_slab_id)
                        .or_insert(StorageRecoverySlabLiveReport {
                            block_slab_id,
                            ..StorageRecoverySlabLiveReport::default()
                        });
                slab_report.live_block_refs = tally.block_refs;
                slab_report.live_physical_bytes = tally.bytes;
            }
        }
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_buckets = BTreeMap::<u64, BTreeSet<u32>>::new();
        for address in &addresses {
            let slab_report = reports.entry(address.block_slab_id).or_insert(
                StorageRecoverySlabLiveReport {
                    block_slab_id: address.block_slab_id,
                    ..StorageRecoverySlabLiveReport::default()
                },
            );
            slab_report.live_block_refs = slab_report.live_block_refs.saturating_add(1);
            slab_report.live_physical_bytes = slab_report
                .live_physical_bytes
                .saturating_add(address.length);
            if let Some(object_id) = address.object_id() {
                let objects = live_object_ids.entry(address.block_slab_id).or_default();
                objects.insert(object_id);
                slab_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_bucket) = address.routing_bucket() {
                let buckets = live_routing_buckets
                    .entry(address.block_slab_id)
                    .or_default();
                buckets.insert(routing_bucket);
                slab_report.live_routing_bucket_count = buckets.len() as u64;
            }
        }
        reports
            .into_values()
            .map(|mut report| {
                report.stale_block_estimate =
                    report.page_count.saturating_sub(report.live_block_refs);
                report.live_ref_density_basis_points = if report.page_count == 0 {
                    0
                } else {
                    report.live_block_refs.saturating_mul(10_000) / report.page_count
                };
                report
            })
            .collect()
    }

    /// The object-lifecycle view on its own, without the whole-store scan around it.
    ///
    /// The recovery report produces this field as a by-product of reading EVERY live page off
    /// disk, which is how it counts the readable ones. Two callers on the maintenance path want
    /// nothing else from that report, so they were paying a full-store read to get it -- on a
    /// loop that runs every thirty seconds, for the life of the process.
    ///
    /// Nothing here reads a page. Every count comes from the shard's own maps, the ownership
    /// validation and the slab reports, which is all the field was ever made of:
    ///
    /// | at 32,000 live pages | the report | this |
    /// |---|---|---|
    /// | wall time | ~840 ms | ~45 ms |
    /// | pages read from the block store | 32,000 | 0 |
    ///
    /// `object_lifecycle_snapshot_matches_the_recovery_report` holds the two to the same answer,
    /// so a change to either that separates them fails there rather than in a shipped round.
    /// The snapshot, and the live page count, for the tests that hold it to the report.
    #[cfg(test)]
    pub(crate) fn storage_object_lifecycle_snapshot_for_test(
        &self,
        shard_id: ShardId,
    ) -> StorageObjectLifecycleReport {
        self.storage_object_lifecycle_snapshot(shard_id)
    }

    #[cfg(test)]
    pub(crate) fn live_block_count_for_test(&self, shard_id: ShardId) -> usize {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .get(&shard_id)
            .map(|shard| collect_live_block_addresses(shard).len())
            .unwrap_or_default()
    }

    pub(super) fn storage_object_lifecycle_snapshot(
        &self,
        shard_id: ShardId,
    ) -> StorageObjectLifecycleReport {
        // Header walk, not a full decode of every record -- see `storage_reclaim_slab_reports`.
        // Only the per-slab block count is read below.
        let block_slab_counts = self.block_store.slab_block_counts().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return StorageObjectLifecycleReport::default();
        };
        // ONE walk, three consumers.
        //
        // This used to walk the shard THREE times over identical, immutable state: once inside
        // `validate_shard_block_ownership`, once inside `storage_object_lifecycle_report`, and once
        // in `collect_live_block_addresses`. Each call to `collect_live_block_entries` materializes
        // every live page in the shard into a fresh Vec, and measured at 4,000 objects this single
        // function accounted for 3.0x the shard -- the largest piece of `apply_storage_lifecycle`,
        // itself 12.0x (`what_each_plan_call_walks`).
        //
        // Hoisting is safe HERE in a way it is not across a maintenance round: the read lock is
        // held for all three, `&ShardState` is immutable throughout, and nothing between them can
        // change what a walk would find. Elsewhere in the round the stages genuinely mutate
        // between calls, which is why this is a hoist and not a cache.
        //
        // Order matters only because the report CONSUMES the entries: borrow for the two cheap
        // derivations first, then hand the Vec over last.
        let entries = collect_live_block_entries(shard);

        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let ownership = validate_bucket_ownership_index_from_entries(
            shard_id,
            shard,
            &entries,
            start_routing_bucket,
            end_routing_bucket,
        );

        // stale_object_ids is the per-slab shortfall of live refs against the slab's own page
        // count, summed. An address naming a slab the store has no report for contributes
        // nothing (the report path gives it page_count 0, so its shortfall saturates to 0).
        let mut live_block_refs_by_slab = BTreeMap::<u64, u64>::new();
        for entry in &entries {
            *live_block_refs_by_slab
                .entry(entry.address.block_slab_id)
                .or_default() += 1;
        }

        let mut report = object_lifecycle_report_from_entries(
            shard_id,
            shard,
            entries,
            &BTreeSet::new(),
            |_| 0,
        );
        report.owner_mismatch_block_refs = ownership.mismatches.len() as u64;
        report.missing_owner_block_refs = ownership.missing_owner_block_refs as u64;
        report.stale_object_ids = block_slab_counts
            .iter()
            .map(|(block_slab_id, _physical_bytes, block_count)| {
                block_count.saturating_sub(
                    live_block_refs_by_slab
                        .get(block_slab_id)
                        .copied()
                        .unwrap_or_default(),
                )
            })
            .sum();
        report
    }

    pub(super) fn storage_recovery_report_without_boundary(&self, shard_id: ShardId) -> StorageRecoveryReport {
        self.storage_recovery_report_without_boundary_sampled(shard_id, 0)
    }

    /// The same report, reading at most `readable_probe_limit` live pages this call.
    ///
    /// The readability check is the only part of this report that reads a page, and it reads
    /// EVERY live one: measured at 32,000 records it is 575 ms and 32,000 reads, about a fifth
    /// of a maintenance round, growing with the store.
    ///
    /// The maintenance cycle wants this report for `manifest_chain_issues`, the two dump
    /// sequences and the two replay sequences -- none of which reads a page. It never consults
    /// `unreadable_page_refs` or `unreadable_page_bytes`, but it does CARRY them in the report it
    /// returns, so simply not filling them in would be a silent lie to whoever reads that report.
    /// Sampling is the honest version: corruption is still found, over rounds rather than all in
    /// one, and `readable_probe_limit` says how much of the store this particular call looked at.
    ///
    /// "Over rounds" needs the sample to MOVE, which is the part that was missing. A bounded
    /// call read `addresses[0 .. limit]` and began at the front again next round, so the pages
    /// past the first window were never read by the periodic loop -- on a shard with more live
    /// pages than the budget, corruption outside that prefix was undiscoverable, in any number
    /// of rounds, while the report kept saying the pages it had read were fine. A bounded call
    /// now resumes at `recovery_probe_cursors` and wraps, so the rounds together cover the whole
    /// shard; `readable_probe_cursor` on the report says where this one started.
    ///
    /// 0 means no bound, matching every other round bound here. The diagnostic endpoint and the
    /// harnesses keep passing 0 and so keep scanning everything.
    pub(super) fn storage_recovery_report_without_boundary_sampled(
        &self,
        shard_id: ShardId,
        readable_probe_limit: usize,
    ) -> StorageRecoveryReport {
        // Durable served-index size. The base is materialized only at compaction, so a fresh
        // crash-recovered shard has no base file yet -- the durable served index is the base
        // folded with the index-log deltas, whose reconstructed size we report (via the
        // served-index funnel) so recovery diagnostics reflect real durable state.
        let base_index_bytes = self
            .index_path(shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let index_bytes = if base_index_bytes == 0 {
            self.load_served_index_bytes(shard_id)
                .map(|bytes| bytes.len() as u64)
                .unwrap_or_default()
        } else {
            base_index_bytes
        };
        // Counted, not collected. Both of these used to read their whole log into a vector
        // and take its length -- on the plan path of every maintenance round.
        let wal_records = self.wal_store.record_count(shard_id).unwrap_or_default();
        let index_log_records = self.index_log_store.record_count(shard_id).unwrap_or_default();
        let active_block_slab_ids = self.block_store.slab_ids().unwrap_or_default();
        let slab_descriptors = self.block_store.slab_descriptors();
        let slab_summary = self.block_store.slab_summary();
        let block_slab_reports = self.block_store.slab_reports().unwrap_or_default();
        let shards = self.shards_read_marked();
        let addresses = shards
            .get(&shard_id)
            .map(collect_live_block_addresses)
            .unwrap_or_default();
        let total_block_refs = addresses.len();
        // WHERE this call reads, not just how much.
        //
        // A bounded call used to read `addresses[0 .. limit]` and nothing else, every round. The
        // budget made the cost constant; starting from the front every time made the COVERAGE
        // constant too, so a live page at an index past the budget was never read by the
        // periodic loop at all. The doc above promises corruption is "still found, over rounds
        // rather than all in one", and that promise needs the window to MOVE.
        //
        // So a bounded call resumes where the last one stopped and wraps at the end. Unbounded
        // calls (`readable_probe_limit == 0`) read everything, so they start at 0 and leave the
        // stored position untouched -- a diagnostic call must not shift the loop's window.
        //
        // The position is clamped rather than trusted: the live-page vector is rebuilt each
        // round and can shrink (compaction, eviction, expiry), and a stale index past its end
        // would otherwise skip the whole round.
        let probe_window_start = if readable_probe_limit > 0 && total_block_refs > 0 {
            self.recovery_probe_cursors
                .read()
                .expect("recovery probe cursor lock poisoned")
                .get(&shard_id)
                .copied()
                .filter(|start| *start < total_block_refs)
                .unwrap_or(0)
        } else {
            0
        };
        let probe_window_len = if readable_probe_limit == 0 {
            total_block_refs
        } else {
            readable_probe_limit.min(total_block_refs)
        };
        // Whether index `position` falls in the window `[start, start + len)` taken modulo the
        // live-page count. Written as a distance from the start so the wrap needs no second
        // range and no branch on whether the window crosses the end.
        let in_probe_window = |position: usize| -> bool {
            if probe_window_len == 0 {
                return false;
            }
            if probe_window_len >= total_block_refs {
                return true;
            }
            let distance = if position >= probe_window_start {
                position - probe_window_start
            } else {
                position + total_block_refs - probe_window_start
            };
            distance < probe_window_len
        };
        let mut readable_block_refs = 0usize;
        let mut probed_block_refs = 0usize;
        let mut unreadable_block_refs = Vec::new();
        let mut owner_mismatch_block_refs = Vec::new();
        let mut missing_owner_block_refs = 0usize;
        let mut object_lifecycle = StorageObjectLifecycleReport::default();
        let mut feature_block_layout = StorageFeatureBlockLayoutReport::default();
        let mut block_slab_live_reports = block_slab_reports
            .iter()
            .map(|report| {
                (
                    report.block_slab_id,
                    StorageRecoverySlabLiveReport {
                        block_slab_id: report.block_slab_id,
                        physical_bytes: report.physical_bytes,
                        logical_bytes: report.logical_bytes,
                        page_count: report.page_count,
                        ..StorageRecoverySlabLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_buckets = BTreeMap::<u64, BTreeSet<u32>>::new();
        for (position, address) in addresses.iter().enumerate() {
            let slab_report = block_slab_live_reports
                .entry(address.block_slab_id)
                .or_insert(StorageRecoverySlabLiveReport {
                    block_slab_id: address.block_slab_id,
                    ..StorageRecoverySlabLiveReport::default()
                });
            slab_report.live_block_refs = slab_report.live_block_refs.saturating_add(1);
            slab_report.live_physical_bytes = slab_report
                .live_physical_bytes
                .saturating_add(address.length);
            if let Some(object_id) = address.object_id() {
                let objects = live_object_ids.entry(address.block_slab_id).or_default();
                objects.insert(object_id);
                slab_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_bucket) = address.routing_bucket() {
                let buckets = live_routing_buckets
                    .entry(address.block_slab_id)
                    .or_default();
                buckets.insert(routing_bucket);
                slab_report.live_routing_bucket_count = buckets.len() as u64;
            }
            // Outside this call's window it stops READING, and keeps everything above that does
            // not need a read -- the per-slab live tallies are what the reclaim planner and the
            // object-lifecycle report are built from, and they must stay complete whatever the
            // window is. Only the reads below are sampled.
            //
            // This used to test `probed_page_refs >= readable_probe_limit`, which is the same
            // budget but anchored at the front: it always admitted the first `limit` entries and
            // never any other. The window test admits `limit` entries too -- so the per-round
            // cost is unchanged -- but a different `limit` of them each round.
            if !in_probe_window(position) {
                continue;
            }
            probed_block_refs += 1;
            // Counted against the shard-table guard. This probe DOES read under the read guard,
            // and unlike the warm-up it is bounded -- `readable_probe_limit` stops the reads
            // while the per-slab tallies above keep going. Routed through the counter so the
            // measurement covers both of the engine's maintenance page readers and a claim about
            // one of them is made against a total that includes the other.
            match self.read_block_counted(address) {
                Ok(bytes) => {
                    readable_block_refs += 1;
                    slab_report.readable_live_block_refs =
                        slab_report.readable_live_block_refs.saturating_add(1);
                    slab_report.live_logical_bytes = slab_report
                        .live_logical_bytes
                        .saturating_add(bytes.len() as u64);
                }
                Err(err) => {
                    slab_report.unreadable_live_block_refs =
                        slab_report.unreadable_live_block_refs.saturating_add(1);
                    unreadable_block_refs.push(StorageRecoveryBlockError {
                        block_slab_id: address.block_slab_id,
                        offset: address.offset,
                        length: address.length,
                        error: err.to_string(),
                    });
                }
            }
        }
        if let Some(shard) = shards.get(&shard_id) {
            let ownership = self.validate_shard_block_ownership(shard_id, shard);
            owner_mismatch_block_refs = ownership.mismatches;
            missing_owner_block_refs = ownership.missing_owner_block_refs;
            object_lifecycle = storage_object_lifecycle_report(shard_id, shard);
            object_lifecycle.owner_mismatch_block_refs = owner_mismatch_block_refs.len() as u64;
            object_lifecycle.missing_owner_block_refs = missing_owner_block_refs as u64;
            feature_block_layout = storage_feature_block_layout_report(&self.block_store, shard);
        }
        let block_slab_live_reports = block_slab_live_reports
            .into_values()
            .map(|mut report| {
                report.stale_block_estimate =
                    report.page_count.saturating_sub(report.live_block_refs);
                report.live_ref_density_basis_points = if report.page_count == 0 {
                    0
                } else {
                    report.live_block_refs.saturating_mul(10_000) / report.page_count
                };
                report
            })
            .collect::<Vec<_>>();
        object_lifecycle.stale_object_ids = block_slab_live_reports
            .iter()
            .map(|report| report.stale_block_estimate)
            .sum();
        let mut live_block_slab_ids = addresses
            .iter()
            .map(|address| address.block_slab_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        live_block_slab_ids.sort_unstable();
        // Hand the next round the position after this window, wrapping at the end.
        //
        // Advanced by the window LENGTH rather than by `probed_page_refs` so a round that found
        // fewer readable pages than it looked at still moves on. Those are the same number today
        // -- every entry in the window is probed -- but tying the advance to a success count is
        // how a sampler gets stuck re-reading the region it is failing on.
        //
        // Only bounded callers write it, for the reason on the field itself: an unbounded call
        // has already read the whole shard, and moving the position would make the periodic
        // loop skip a window it had not covered.
        if readable_probe_limit > 0 && total_block_refs > 0 {
            let next = (probe_window_start + probe_window_len) % total_block_refs;
            self.recovery_probe_cursors
                .write()
                .expect("recovery probe cursor lock poisoned")
                .insert(shard_id, next);
        }
        StorageRecoveryReport {
            shard_id,
            index_bytes,
            index_write_atomic: true,
            wal_records,
            index_log_records,
            active_block_slab_ids,
            live_block_slab_ids,
            slab_descriptors,
            slab_summary,
            block_slab_reports,
            block_slab_live_reports,
            total_block_refs,
            readable_block_refs,
            probed_block_refs,
            readable_probe_cursor: probe_window_start,
            unreadable_block_refs,
            owner_mismatch_block_refs,
            missing_owner_block_refs,
            object_lifecycle,
            // Against what was PROBED, not against every live page. With a sample budget the
            // two differ, and reading it as "every page is readable" when only some were tried
            // is exactly the false assurance this field exists to avoid.
            all_live_blocks_readable: probed_block_refs == readable_block_refs,
            boundary: StorageRecoveryBoundaryReport::default(),
            slab_integrity: StorageSlabIntegrityReport::default(),
            feature_block_layout,
        }
    }

    /// Hand the block store the per-slab live tally this engine's shards maintain.
    ///
    /// ONE ENGINE, ONE PAGE STORE, MANY SHARDS -- and two shards' pages can land in the same
    /// slab, so the figure the store needs is the UNION. Returns how many shards contributed.
    ///
    /// Publishes only when EVERY loaded shard has a derived tally. A partial union would
    /// understate the live bytes of any slab a not-yet-derived shard points into, and while that
    /// direction cannot delete anything (the live slab id set gates deletion and is computed
    /// fresh), a figure that is wrong for a reason nobody can see is not worth publishing.
    ///
    /// Costs SLABS, not pages -- which is the whole point of maintaining the tally.
    pub fn publish_block_slab_live_bytes(&self) -> usize {
        let shards = self.shards.read().expect("engine lock poisoned");
        if shards.is_empty() {
            return 0;
        }
        let mut live: BTreeMap<u64, BlockStoreSlabLive> = BTreeMap::new();
        let mut ready_shards = 0_usize;
        for shard in shards.values() {
            if !shard.bucket_index.block_slab_live.is_ready() {
                continue;
            }
            ready_shards = ready_shards.saturating_add(1);
            for (block_slab_id, tally) in shard.bucket_index.block_slab_live.iter() {
                let entry = live.entry(block_slab_id).or_default();
                entry.live_block_refs = entry.live_block_refs.saturating_add(tally.block_refs);
                entry.live_bytes = entry.live_bytes.saturating_add(tally.bytes);
            }
        }
        if ready_shards != shards.len() {
            return ready_shards;
        }
        drop(shards);
        self.block_store.publish_live_block_bytes(live);
        ready_shards
    }

    /// Withhold this shard's maintained tally, so the next consumer takes its walk fallback.
    ///
    /// For the A arm of the measurement only. Nothing repairs it except a reconcile or a seed.
    #[cfg(test)]
    pub(crate) fn forget_block_slab_live_for_test(&self, shard_id: ShardId) {
        let mut shards = self.shards_write_marked();
        if let Some(shard) = shards.get_mut(&shard_id) {
            shard.bucket_index.block_slab_live.forget_for_test();
        }
    }

    /// The per-slab live/stale tally the reclaim planner reads, exposed for the measurement.
    #[cfg(test)]
    pub(crate) fn storage_reclaim_slab_reports_for_test(
        &self,
        shard_id: ShardId,
    ) -> Vec<StorageRecoverySlabLiveReport> {
        self.storage_reclaim_slab_reports(shard_id)
    }

    /// Compare the maintained per-slab live tally against the walk, correct it, and report.
    ///
    /// The guard's entry point, and an operator's. Running it costs the walk it exists to
    /// replace, so nothing on the serving path calls it; the wholesale-rebuild paths seed instead.
    pub fn block_slab_live_drift_check(&self, shard_id: ShardId) -> BlockSlabLiveDriftReport {
        let mut shards = self.shards_write_marked();
        match shards.get_mut(&shard_id) {
            Some(shard) => crate::engine::storage_bucket_internals::reconcile_block_slab_live(shard),
            None => BlockSlabLiveDriftReport::default(),
        }
    }

    /// The maintained tally as this shard holds it: `(block_slab_id, live page refs, live bytes)`.
    ///
    /// For the guards and the measurements. `None` when the shard is absent or its tally has
    /// never been derived -- the second is not the same as "no live pages" and must not read as
    /// it.
    pub fn block_slab_live_tallies(&self, shard_id: ShardId) -> Option<Vec<(u64, u64, u64)>> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&shard_id)?;
        if !shard.bucket_index.block_slab_live.is_ready() {
            return None;
        }
        Some(
            shard
                .bucket_index
                .block_slab_live
                .iter()
                .map(|(block_slab_id, tally)| (block_slab_id, tally.block_refs, tally.bytes))
                .collect(),
        )
    }

    pub fn live_block_slab_ids(&self, shard_id: ShardId) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .get(&shard_id)
            .map(collect_live_block_slab_ids)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Union of live page-slab ids across EVERY shard currently loaded into this engine.
    ///
    /// One engine owns a single `page_store` shared by all shards it hosts, and the current
    /// append cursor + slab counter are global, so two shards' pages can land in the same slab.
    /// Any slab referenced by *any* loaded shard is live and must not be reclaimed. A single
    /// shard's live set is therefore an unsafe basis for GC: a slab live only in shard B looks
    /// stale to shard A's cycle and would be deleted, silently destroying B's committed pages.
    /// Reclaim must be driven by this union so a slab referenced by any shard is retained.
    ///
    /// For a single loaded shard this equals `live_block_slab_ids(that_shard)`, so single-shard
    /// callers are unaffected.
    pub fn live_block_slab_ids_all_shards(&self) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .values()
            .flat_map(collect_live_block_slab_ids)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

/// How far a round may walk to fill its window.
///
/// The window is the useful work; the budget stops a long run of keys in the other category from
/// turning a bounded round back into a walk of everything. Zero limits mean no limit, and then
/// there is nothing to bound.
fn expiry_scan_budget(limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }
    limit.saturating_mul(8).max(64)
}

    pub fn sweep_expired_records(
        &self,
        shard_id: ShardId,
    ) -> Result<ShardExpirySweepReport, Status> {
        self.sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id,
            load_cold_buckets: true,
            ..ShardExpirySweepRequest::default()
        })
    }

    pub fn sweep_expired_records_with_request(
        &self,
        request: ShardExpirySweepRequest,
    ) -> Result<ShardExpirySweepReport, Status> {
        let mut shards = self.shards_write_marked();
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let now = now_ms();
        // Read each window from where its cursor left off. Asking about every deadline to find
        // a window of sixteen made a round cost the size of the whole set, on every cycle.
        let hot_limit = request.max_hot_buckets_per_round;
        let cold_limit = request.max_cold_buckets_per_round;
        let scan_budget = Self::expiry_scan_budget(hot_limit.max(cold_limit));
        // Read the DUE keys, not a window of the keyspace that might contain some.
        //
        // These windows used to walk `expires_at_ms` in KEY order from a cursor, testing each
        // deadline as they went, so a key that was not due was still walked and charged against
        // the budget. That made time-to-expire a function of the keyspace: measured at
        // keyspace/scan_budget rounds, with ten expired keys behind 10,000 live ones surviving
        // more than sixty rounds.
        //
        // `expiry_by_deadline` orders the same deadlines by deadline, so the due keys are a
        // PREFIX and the walk stops at the first one in the future. The cursors are no longer
        // needed for correctness -- a due key is at the front, not somewhere ahead of a cursor --
        // and the request/report fields are kept so the wire format does not change.
        crate::engine::ensure_expiry_order(shard);
        let hot_selected =
            crate::engine::due_window(shard, now, hot_limit, scan_budget, |key| {
                record_exists(shard, key)
            });
        let cold_selected =
            crate::engine::due_window(shard, now, cold_limit, scan_budget, |key| {
                !record_exists(shard, key)
            });
        let next_hot_cursor: Option<String> = None;
        let next_cold_cursor: Option<String> = None;
        let mut expired_records_removed = 0;
        let mut skipped_records = 0usize;
        let mut loaded_for_expire = 0usize;
        let mut expired_keys: Vec<String> = Vec::new();
        let mut pending_index_flush: Option<ExpiryIndexCheckpoint> = None;
        for (key, expires_at) in hot_selected.iter() {
            if *expires_at <= now {
                if delete_record(shard, key) {
                    invalidate_record_all(&self.cache, request.shard_id, key);
                    expired_records_removed += 1;
                    expired_keys.push(key.clone());
                }
            } else {
                skipped_records = skipped_records.saturating_add(1);
            }
        }
        for (key, expires_at) in cold_selected.iter() {
            if *expires_at <= now {
                if request.load_cold_buckets {
                    loaded_for_expire = loaded_for_expire.saturating_add(1);
                    if delete_record(shard, key) {
                        invalidate_record_all(&self.cache, request.shard_id, key);
                        expired_records_removed += 1;
                        expired_keys.push(key.clone());
                    } else {
                        crate::engine::clear_expiry(shard, key);
                    }
                } else {
                    skipped_records = skipped_records.saturating_add(1);
                }
            } else {
                skipped_records = skipped_records.saturating_add(1);
            }
        }
        if expired_records_removed > 0 {
            // Expiry IS a logged,
            // replicated delete. Emit a WAL tombstone per expired key -- buffered and
            // unfsynced, mirroring the fire-and-forget commit -- so followers and WAL
            // replay observe the deletion instead of relying on each node running its own
            // sweep with its own clock/enable_expire. Then anchor the served snapshot past
            // the tombstones so a restart does not resurrect the key by replaying the
            // earlier SET/EXPIRE records.
            if !replaying_wal() {
                // ONE mirror lookup for the whole run, taken before the loop.
                //
                // The per-key form takes the mirror lock and bumps an Arc refcount for every
                // tombstone, and this loop runs inside the shard-table WRITE guard -- the one
                // lock that excludes every reader and writer on the shard -- so a round removing
                // N keys took N of them in the worst place to take a lock. One lookup also gives
                // the whole run ONE destination, where a sink swapped mid-loop would split a
                // single round's tombstones across two mirrors and leave neither complete.
                let mirror = self.maintenance_mirror_sink();
                for key in &expired_keys {
                    let command = Command::CommonDelete { key: key.clone() };
                    let appended = self
                        .wal_store
                        .append_with_sync(request.shard_id, command.clone(), false);
                    // An expiry is a real deletion, so it has to reach every log that a
                    // successor might replay -- not only this node's.
                    if appended.is_ok() {
                        if let Some(sink) = mirror.as_ref() {
                            sink.record_write(request.shard_id, &command);
                        }
                    }
                }
                shard.applied_wal_sequence =
                    Some(self.wal_store.stats(request.shard_id).last_sequence);
            }
            // The checkpoint is BUILT under this lock and WRITTEN after it drops. The lock is
            // needed for the deletes and for anchoring `applied_wal_sequence`; it is not needed
            // for the write, and the write used to be the expensive part of the round -- the
            // shard's whole index through serde and then zstd, then two file writes, all of it
            // scaling with the STORE while the round's own work is bounded by
            // `expiry_scan_budget`. Moving it out came first; making it proportional to the
            // round came second, and both are still claims worth holding, so both have arms in
            // `the_expiry_sweep_flush_waits_for_the_write_guard_to_drop`.
            //
            // WHY THIS ONE IS SAFE TO MOVE AND THE COMPACTOR'S IS NOT. Between the guard
            // dropping and the write landing there is a window in which this checkpoint is
            // stale: a concurrent writer can publish a newer one that this then follows, and a
            // concurrent storage cycle can see pages these deletes freed, call their slabs
            // stale and reclaim them while the durable index still names them. Both are
            // recoverable HERE and only here, because an expiry IS a logged delete: every key
            // above emitted a `CommonDelete` to the WAL before this point, so a checkpoint that
            // lands stale (or never lands at all) leaves an anchor BEHIND the tombstones, and
            // replay re-derives exactly the deletions it describes. Landing behind a newer
            // checkpoint rewinds the anchor, which holds more log than needed and never less.
            // Compaction's relocations are in no log -- see the note at its own flush.
            shard.index_format_version = super::SHARD_INDEX_FORMAT_VERSION;
            // WHAT THE ROUND WRITES IS WHAT THE ROUND CHANGED.
            //
            // This used to clone the whole shard and hand it to an encode of the ENTIRE served
            // index. The round's own work is bounded -- `due_window` looks at the due set, ten
            // records whether the shard holds two thousand live keys or a hundred thousand --
            // but the checkpoint that followed it was not: measured, ten due keys cost 109 ms at
            // 2,000 live and 922 ms at 20,000, the same ten records looked at both times. The
            // cost tracked LIVE KEYS. Moving that encode off the write guard (#1620) changed who
            // waited for it, not how much of it there was.
            //
            // The index log already carries the record shape this wants: a DELTA naming the keys
            // a write touched, which `fold_index_log_deltas` folds onto the base on load. The
            // ordinary delete path has written exactly this shape for every `CommonDelete` it
            // applies; expiry IS a logged delete, so it writes the same one. Each expired key
            // contributes one key-state blob carrying NO map fields, which the fold reads as "this
            // key is in none of these maps" -- a tombstone in all thirteen, `expires_at_ms`
            // included -- and `delta_record_covered_keys` reads the covered set from those blobs,
            // so the page wipe reaches the keys even though they contribute no page items.
            //
            // SAFETY, AND IT IS THE SAME ARGUMENT #1620 MADE. The anchor must never move AHEAD of
            // the tombstones. The delta carries `applied_wal_sequence` -- the sequence anchored
            // just above, after every `CommonDelete` this round appended -- so the anchor and the
            // description of the deletions move TOGETHER in one record: a reader that folds the
            // anchor has folded the deletions. A delta that lands late or never leaves the anchor
            // BEHIND the tombstones, and WAL replay re-derives exactly these deletions. Neither
            // direction can leave a key resurrected.
            pending_index_flush = Some(
                if self
                    .expiry_index_flush_whole
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    ExpiryIndexCheckpoint::Whole(Box::new(shard.clone()))
                } else {
                    // `delete_record` removed each expired key AND every record key associated
                    // with it (the control-state families), so the delta has to cover the same
                    // set or the fold would restore what the round deleted.
                    let delta_keys: Vec<String> = expired_keys
                        .iter()
                        .flat_map(|key| super::associated_record_keys(key))
                        .collect::<std::collections::BTreeSet<_>>()
                        .into_iter()
                        .collect();
                    ExpiryIndexCheckpoint::Delta(Box::new(ExpiryIndexDelta {
                        // NO PAGE ITEMS, and that is the whole content of the record's page half.
                        //
                        // `mark_bucket_index_object_deleted` has already run for each of these
                        // keys, and it retains every page carrying the key OUT of every bucket
                        // that holds one -- the buckets come from `object_block_refs` across every
                        // model kind, or from the whole bucket map when the lookup is not
                        // established. So by the time this record is built there is no page left
                        // to describe, and an empty item list against a covered key is exactly
                        // how the fold spells a removal: `fold_delta_block_items` wipes every page
                        // of every covered key and then restores the items carried, which is
                        // none.
                        //
                        // This was `collect_command_index_items_for`, collecting rather than
                        // asserting -- and it cost: that helper walks the ENTIRE page index of
                        // every bucket the keys hash into, so a round clearing 8,000 due keys
                        // walked a large part of the shard to build a list that is empty by
                        // construction. Measured, it took the per-key cost of a round from
                        // linear in the due set to 3.11x between 1,000 and 8,000 due keys.
                        //
                        // The invariant is not assumed silently: `the_expiry_delta_names_what_
                        // the_round_removed_and_anchors_no_further` asserts the record carries no
                        // items, so a delete that starts leaving pages behind fails there rather
                        // than quietly wiping them on the next fold.
                        items: Vec::new(),
                        key_states: super::capture_key_states(shard, &delta_keys),
                        applied_wal_sequence: shard.applied_wal_sequence,
                    }))
                },
            );
        }
        // The control arm of `the_expiry_sweep_flush_waits_for_the_write_guard_to_drop` keeps the
        // flush inside the region, so the guard has a positive control to compare against.
        if self
            .expiry_index_flush_under_lock
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(checkpoint) = pending_index_flush.take() {
                self.flush_expiry_index(request.shard_id, checkpoint)?;
            }
        }
        drop(shards);
        if let Some(checkpoint) = pending_index_flush {
            self.flush_expiry_index(request.shard_id, checkpoint)?;
        }
        Ok(ShardExpirySweepReport {
            shard_id: request.shard_id,
            expired_records_removed,
            hot_buckets_scanned: hot_selected.len(),
            cold_buckets_scanned: cold_selected.len(),
            scanned_records: hot_selected.len().saturating_add(cold_selected.len()),
            skipped_records,
            loaded_for_expire,
            next_hot_cursor,
            next_cold_cursor,
            round_limit: hot_limit.saturating_add(cold_limit),
            load_on_expire_only_when_needed: true,
        })
    }

    /// Write the expiry sweep's served-index checkpoint.
    ///
    /// One body, called from both arms of the flush, so the arm that ships and the arm the guard
    /// measures against cannot drift into doing different work.
    fn flush_expiry_index(
        &self,
        shard_id: ShardId,
        checkpoint: ExpiryIndexCheckpoint,
    ) -> Result<(), Status> {
        match checkpoint {
            ExpiryIndexCheckpoint::Delta(delta) => {
                let delta = *delta;
                // `durable` fsyncs the record before returning, decided by the same rule the
                // ordinary write path's delta uses. Deferring it is sound here for the reason
                // stated at the call site: the WAL tombstones are already appended, so a lost
                // delta tail leaves the anchor behind them and replay re-derives the deletions.
                let durable = !super::raft_applying() && !super::wal_single_barrier();
                let _ = self.index_log_store.append_delta(
                    shard_id,
                    delta.items,
                    delta.key_states,
                    delta.applied_wal_sequence,
                    None,
                    // NOT an upsert. An upsert record replaces each item's own predecessor and
                    // leaves everything else for the key in place, which for a round whose items
                    // are empty would remove nothing at all. The snapshot shape wipes every page
                    // of every covered key and restores only the items carried, which is what
                    // makes an empty item list a deletion.
                    false,
                    durable,
                );
                Ok(())
            }
            ExpiryIndexCheckpoint::Whole(snapshot) => {
                let index_bytes = super::serialize_index(&snapshot);
                self.persist_index_bytes(shard_id, &index_bytes)
                    .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
                let _ = self
                    .index_log_store
                    .append_index_bytes(shard_id, &index_bytes);
                Ok(())
            }
        }
    }

    pub fn sweep_all_expired_records(&self) -> Vec<ShardExpirySweepReport> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.sweep_expired_records(shard_id).ok())
            .collect()
    }

    pub(super) fn validate_shard_block_ownership(
        &self,
        shard_id: ShardId,
        shard: &ShardState,
    ) -> StorageBlockOwnershipValidation {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        validate_bucket_ownership_index(shard_id, shard, start_routing_bucket, end_routing_bucket)
    }

    /// What a compaction round would relocate on this shard, asked one object at a time.
    ///
    /// Takes the reclaim candidates rather than computing them, because every caller already has
    /// a lifecycle plan holding them and recomputing means another whole-shard walk.
    ///
    /// NOT what `compact_shard_blocks` consults. A direct compaction -- the operator RPC, the
    /// on-demand cycle, and the suite -- is an instruction, not a suggestion, and still relocates
    /// everything. This is what the PERIODIC loop asks before deciding to issue one.
    pub fn compaction_relocation_hint(
        &self,
        shard_id: ShardId,
        reclaim_candidates: &[StorageReclaimCandidate],
    ) -> ShardCompactionRelocationHint {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return ShardCompactionRelocationHint {
                shard_id,
                ..ShardCompactionRelocationHint::default()
            };
        };
        compaction_relocation_hint_per_object(shard_id, shard, reclaim_candidates)
    }

    pub fn compact_shard_blocks(&self, shard_id: ShardId) -> Result<ShardCompactionReport, Status> {
        self.compact_shard_blocks_with_budgets(
            shard_id,
            COMPACTION_ROUND_BYTES,
            COMPACTION_ROUND_BLOCK_REFS,
        )
    }

    /// Compact, relocating ONLY the pages that sit on `drain_block_slab_ids`.
    ///
    /// This is what the PERIODIC loop issues, and it is the round the relocation hint describes.
    /// `compact_shard_blocks` above relocates every live page, which is what a direct
    /// instruction has always meant -- but for the loop that is far more work than the result
    /// needs. A relocation recovers space only for the slab it VACATES:
    /// `compact_block_addresses` copies a page's bytes verbatim and appends them elsewhere, so
    /// a page moved off a slab with no dead space comes out byte for byte what it went in as, on
    /// a slab that is now the one carrying the dead space. The live bytes are unchanged and there
    /// is one more emptied slab for the collector to destroy.
    ///
    /// So a single overwritten record anywhere in the shard used to cost a rewrite of the WHOLE
    /// live set, spread over `live_page_refs / COMPACTION_ROUND_BLOCK_REFS` rounds of shard
    /// write lock, to recover one page. Naming the drain set bounds the work by the size of the
    /// holed slabs instead of by the size of the store.
    ///
    /// The set costs no walk: `compaction_drain_block_slab_ids` reads it off the reclaim plan
    /// the maintenance round has already built, and it is the SAME plan the round's gate consults
    /// -- so what the round is allowed to move and what it was started for now come from one
    /// snapshot instead of two.
    pub fn compact_shard_blocks_draining(
        &self,
        shard_id: ShardId,
        drain_block_slab_ids: BTreeSet<u64>,
    ) -> Result<ShardCompactionReport, Status> {
        self.compact_shard_blocks_relocating(
            shard_id,
            COMPACTION_ROUND_BYTES,
            COMPACTION_ROUND_BLOCK_REFS,
            Some(drain_block_slab_ids),
        )
    }

    /// Compact, relocating at most `budget_bytes` of pages this round.
    ///
    /// The budget is a parameter and not only a constant so a test can force MANY rounds over a
    /// handful of pages. A boundary that only appears once a store passes 256 MiB is a boundary
    /// no test would reach, and the rules that make a bounded round correct -- a round resumes
    /// onto the slab it was filling rather than rolling again, and rounds together still move
    /// every page -- all live at that boundary.
    /// Compact with a byte budget only, leaving the ref count unbounded.
    ///
    /// This is what the byte-budget probe measures and what every existing caller wants: adding
    /// a ref bound here would silently change what those measurements mean.
    pub(crate) fn compact_shard_blocks_with_budget(
        &self,
        shard_id: ShardId,
        budget_bytes: u64,
    ) -> Result<ShardCompactionReport, Status> {
        self.compact_shard_blocks_with_budgets(shard_id, budget_bytes, usize::MAX)
    }

    pub(crate) fn compact_shard_blocks_with_budgets(
        &self,
        shard_id: ShardId,
        budget_bytes: u64,
        budget_block_refs: usize,
    ) -> Result<ShardCompactionReport, Status> {
        self.compact_shard_blocks_relocating(shard_id, budget_bytes, budget_block_refs, None)
    }

    /// The round itself. `drain_block_slab_ids` is `None` for "every live page" -- a
    /// direct instruction -- and `Some` for "only the slabs named", which is what the
    /// periodic loop asks for. Everything else about a round is identical either way, so the two
    /// share one body and one set of rules about resuming, budgets and the write guard.
    fn compact_shard_blocks_relocating(
        &self,
        shard_id: ShardId,
        budget_bytes: u64,
        budget_block_refs: usize,
        drain_block_slab_ids: Option<BTreeSet<u64>>,
    ) -> Result<ShardCompactionReport, Status> {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("shard info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards_write_marked();
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        // ONE walk for the preamble's live-page consumers.
        //
        // Before any budget is consulted, this stage builds several whole-shard reports, and each
        // one called `collect_live_block_entries` for its own copy -- measured by
        // `what_the_compaction_preamble_walks` as 5.0x the shard. All of them run under the same
        // WRITE lock on an unchanged `&ShardState`, so every read and write on the shard queues
        // behind the lot. Three of them take the same live-page set and now share one walk.
        //
        // `object_manager_runtime_report` (2.0x) and `collect_live_block_slab_ids` still walk on
        // their own: the first needs `_from_entries` forms of two nested reports, and the second
        // walks the model maps directly rather than the live-page set, so it is not the same walk
        // and cannot share this one.
        //
        // Order is forced by the last consumer taking the Vec BY VALUE: the two that borrow go
        // first, so nothing is cloned.
        let entries = collect_live_block_entries(shard);
        let ownership = validate_bucket_ownership_index_from_entries(
            shard_id,
            shard,
            &entries,
            start_routing_bucket,
            end_routing_bucket,
        );
        if !ownership.mismatches.is_empty() {
            return Err(Status::error(
                "page_compaction_owner_mismatch",
                format!(
                    "refusing compaction because {} live page refs disagree with object/page/slot ownership",
                    ownership.mismatches.len()
                ),
            ));
        }
        let before_slabs = collect_live_block_slab_ids(shard);
        let before = compaction_utility_report_from_entries(&self.block_store, shard, &entries);
        let model_layouts_before = compaction_model_layout_reports(&self.block_store, shard);
        // Ordered so the ONE consumer that takes the Vec by value goes last. These are read-only
        // reports over unchanged state, so the order among them carries no meaning beyond that.
        let object_manager_before = object_manager_runtime_report_from_entries(
            shard_id,
            shard,
            &entries,
            start_routing_bucket,
            end_routing_bucket,
        );
        let delete_marked_object_ids_before =
            object_lifecycle_report_from_entries(shard_id, shard, entries, &BTreeSet::new(), |_| 0)
                .delete_marked_object_ids;
        let bucket_layout_transition_count_before = object_manager_before.layout_transition_count;
        // Start a round, or continue the one a budget cut short.
        //
        // A round rolls a fresh slab and relocates live pages onto it. Rolling AGAIN while a
        // round is unfinished would re-move everything the last round moved -- the pages it just
        // relocated would no longer be on the newest slab -- so a bounded round would shuffle
        // rather than progress. Continuing to fill the same slab is what makes each round move
        // pages that have not moved yet.
        let resumed = self
            .compaction_rounds
            .read()
            .expect("compaction round lock poisoned")
            .get(&shard_id)
            .copied();
        let (previous_block_slab_id, target_block_slab_id) = match resumed {
            Some(round) => round,
            None => {
                let roll = self
                    .block_store
                    .roll_slab()
                    .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
                (roll.previous_block_slab_id, roll.new_block_slab_id)
            }
        };
        let mut rewrite_stats = match drain_block_slab_ids {
            Some(drain_block_slab_ids) => CompactionRewriteStats::for_drain_round(
                target_block_slab_id,
                budget_bytes,
                budget_block_refs,
                drain_block_slab_ids,
            ),
            None => CompactionRewriteStats::for_round(
                target_block_slab_id,
                budget_bytes,
                budget_block_refs,
            ),
        };

        // Relocate every model's live pages onto the freshly rolled slab. A mid-way failure
        // (append ENOSPC / an unreadable torn page) is caught below so we can durably commit the
        // consistent partial state instead of leaving the volatile index half-advanced but
        // unpersisted -- see the `if let Err(err)` handler after this block for why.
        let relocation_result: Result<(), Status> = (|| {
        compact_block_addresses(
            &self.block_store,
            &self.cache,
            shard_id,
            "string",
            shard.strings.values_mut(),
            &mut rewrite_stats,
        )?;
        for fields in shard.hashes.values_mut() {
            compact_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "hash",
                fields.values_mut(),
                &mut rewrite_stats,
            )?;
        }
        for members in shard.zsets.values_mut() {
            compact_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "zset",
                members.values_mut().map(|entry| &mut entry.1),
                &mut rewrite_stats,
            )?;
        }
        for elements in shard.lists.values_mut() {
            compact_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "list",
                elements.values_mut(),
                &mut rewrite_stats,
            )?;
        }
        for members in shard.sets.values_mut() {
            compact_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "set",
                members.values_mut(),
                &mut rewrite_stats,
            )?;
        }
        for series in shard.features.values_mut() {
            compact_feature_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "feature",
                series,
                &mut rewrite_stats,
            )?;
        }
        compact_block_addresses(
            &self.block_store,
            &self.cache,
            shard_id,
            "control_state",
            shard.control_state_blocks.values_mut(),
            &mut rewrite_stats,
        )?;
        compact_block_addresses(
            &self.block_store,
            &self.cache,
            shard_id,
            "context_node",
            shard.context_nodes.values_mut(),
            &mut rewrite_stats,
        )?;
        for series in shard.context_events.values_mut() {
            compact_feature_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "context_event",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_indexes.values_mut() {
            compact_feature_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "context_index",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_audits.values_mut() {
            compact_feature_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "context_audit",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_children.values_mut() {
            compact_feature_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "context_child",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_summaries.values_mut() {
            compact_feature_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "context_summary",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_compressions.values_mut() {
            compact_feature_block_addresses(
                &self.block_store,
                &self.cache,
                shard_id,
                "context_compression",
                series,
                &mut rewrite_stats,
            )?;
        }
        compact_block_addresses(
            &self.block_store,
            &self.cache,
            shard_id,
            "context_entity",
            shard
                .context_entities
                .values_mut()
                .flat_map(|series| series.values_mut()),
            &mut rewrite_stats,
        )?;
            Ok(())
        })();
        if let Err(err) = relocation_result {
            // A relocation failed partway. The in-memory index is now a CONSISTENT partial
            // snapshot -- pages already moved point at the fresh durable slab, the rest still point
            // at their old slabs -- but it has DIVERGED from the on-disk index, which still
            // references the now-vacated old slabs. Returning here without persisting (the old
            // behavior) let the independent next-cycle reclaim trust this volatile index, see a
            // fully-vacated old slab as stale, quarantine+purge it, and a later reload of the STALE
            // on-disk index would then dangle at the deleted slab -> silent durable data loss.
            // A compactor that leaves the index untouched on failure and commits the rewrite
            // atomically avoids the desync structurally. We instead
            // durably commit the consistent partial: rebuild the secondary views so the serialized
            // index is internally consistent, fsync the relocated bytes so the index never names a
            // non-durable page, then persist -- leaving volatile == durable so reclaim is safe --
            // and propagate the original error so the caller knows compaction did not fully
            // complete (a later run retries the not-yet-moved pages).
            rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX);
            refresh_bucket_runtime_flags(shard);
            rebuild_bucket_block_ownership(shard_id, shard, start_routing_bucket, end_routing_bucket);
            self.block_store.sync_durable().map_err(|barrier| {
                Status::error(
                    "page_compaction_failed",
                    format!(
                        "durability barrier failed while committing a partial compaction: {barrier}"
                    ),
                )
            })?;
            let partial_index_bytes = Ok::<_, serde_json::Error>(super::serialize_index_stamped(shard))
                .map_err(|serialize| Status::error("page_compaction_failed", serialize.to_string()))?;
            self.persist_index_bytes(shard_id, &partial_index_bytes)
                .map_err(|persist| Status::error("page_compaction_failed", persist.to_string()))?;
            let _ = self.index_log_store.append_index_bytes(shard_id, &partial_index_bytes);
            // Keep the round OPEN. A relocation that failed partway left work behind by
            // definition -- the pages it had not reached yet are still on their old slabs -- and
            // this used to return without recording the anchor, so the next round read
            // `resumed = None`, rolled a SECOND fresh slab, and re-moved every page this one had
            // already moved. That is exactly the shuffle the note above the resume read exists to
            // rule out, and the failure path was the one way into it. It costs repeated work and
            // one extra slab per failure rather than data loss, which is why it survived.
            //
            // Recorded UNCONDITIONALLY here, not under `left_work_behind()`: that flag is
            // `skipped_by_budget > 0`, which a failed READ never sets, so gating on it would
            // leave the anchor unwritten in precisely the case this handler exists for.
            self.compaction_rounds
                .write()
                .expect("compaction round lock poisoned")
                .insert(shard_id, (previous_block_slab_id, target_block_slab_id));
            return Err(err);
        }

        // A round that spent its budget stays open, so the next one fills the same slab instead
        // of rolling a new one and re-moving what this one moved. A round that relocated
        // everything closes, and the next starts fresh.
        {
            let mut rounds = self
                .compaction_rounds
                .write()
                .expect("compaction round lock poisoned");
            if rewrite_stats.left_work_behind() {
                rounds.insert(shard_id, (previous_block_slab_id, target_block_slab_id));
            } else {
                rounds.remove(&shard_id);
            }
        }
        rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX);
        refresh_bucket_runtime_flags(shard);
        let after_slabs = collect_live_block_slab_ids(shard);
        let after = compaction_utility_report(&self.block_store, shard);
        rebuild_bucket_block_ownership(shard_id, shard, start_routing_bucket, end_routing_bucket);
        let delete_marked_object_ids_after =
            storage_object_lifecycle_report(shard_id, shard).delete_marked_object_ids;
        let object_manager_after =
            object_manager_runtime_report(shard_id, shard, start_routing_bucket, end_routing_bucket);
        let bucket_layout_transition_count_after = object_manager_after.layout_transition_count;
        let bucket_layout_states_after = object_manager_after.layout_states;
        let stale_block_slab_ids = before_slabs
            .difference(&after_slabs)
            .copied()
            .collect::<Vec<_>>();
        let reclaimable_stale_block_slab_count = stale_block_slab_ids.len();
        let model_policy_family_count = before.model_policies.len();
        let delete_marker_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.delete_marker_compaction_triggered)
            .count();
        let stale_density_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.stale_density_triggered)
            .count();
        let layout_aware_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.layout_aware_rewrite_required)
            .count();
        // Durability barrier BEFORE publishing the base index that names the relocated pages.
        // Under deferred-fsync modes (bulk / block_wal_single_barrier -> append.rs
        // defer_data_sync) compaction relocates pages fsync-deferred, so the moved
        // bytes may still be in the page cache. Persisting a base index that references them
        // and crashing before the next barrier would leave dangling references at an un-synced
        // slab (compaction does not advance applied_wal_sequence, so WAL replay does not
        // re-derive them) = permanent silent loss. The partial-failure path above already
        // syncs here; the success path must too. Unconditional, matching that path.
        self.block_store.sync_durable().map_err(|barrier| {
            Status::error(
                "page_compaction_failed",
                format!(
                    "durability barrier failed before publishing the compacted base index: {barrier}"
                ),
            )
        })?;
        // THIS FLUSH STAYS UNDER THE WRITE GUARD. DO NOT GIVE IT THE EVICTION/EXPIRY
        // TREATMENT.
        //
        // It is the same shape as the two flushes that were moved out -- encode the whole index,
        // write two files -- and `the_expiry_sweep_flush_waits_for_the_write_guard_to_drop`
        // measures it still running inside the region, so it looks like the next one to move.
        // It is not, and the difference is not about this function: it is that the work it is
        // publishing exists NOWHERE ELSE.
        //
        // An expiry sweep and a delete_drop eviction both write a WAL tombstone per key before
        // they flush, so a flush that lands late, lands stale, or never lands is re-derived by
        // replay. Compaction deliberately does not advance `applied_wal_sequence` and emits no
        // record: relocating a page changes only the volatile index and this file. From the
        // moment the guard drops, `storage_lifecycle_plan` can take its own read lock, derive
        // stale slabs as `page_store.slab_ids()` minus the volatile live set -- which now
        // excludes the slabs this round just vacated -- and reclaim them. Crash in that window
        // and the durable index is the PRE-compaction one, still naming slabs that have been
        // destroyed, with no log to replay them back. That is the silent durable loss the
        // partial-failure handler above exists to avoid, reached by a different route.
        //
        // Holding the guard across the encode is what makes that window not exist: no other
        // thread can observe the vacated volatile index until the durable one names the new
        // slab. The cost is real and measured; it buys the invariant.
        let index_bytes = Ok::<_, serde_json::Error>(super::serialize_index_stamped(shard))
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        // The PUBLISH, not just the encode, is what has to happen inside the region. See
        // `FLUSH_PUBLISHED_UNDER_GUARD`.
        #[cfg(test)]
        note_compaction_flush_publish();
        self.persist_index_bytes(shard_id, &index_bytes)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = self.index_log_store.append_index_bytes(shard_id, &index_bytes);
        let rewritten_object_blocks = rewrite_stats.rewritten_block_refs;
        let bucket_layout_transition_count =
            bucket_layout_transition_count_after.saturating_sub(bucket_layout_transition_count_before);
        let has_model_layouts = !model_layouts_before.is_empty();
        let preserves_delete_markers = delete_marked_object_ids_after >= delete_marked_object_ids_before;
        let improves_density =
            before.live_ref_density_basis_points <= after.live_ref_density_basis_points;
        let has_layout_transitions = bucket_layout_transition_count > 0
            || bucket_layout_states_after
                .iter()
                .any(|state| state.object_count > 0);
        let mut model_layout_compaction_blockers = Vec::new();
        if rewritten_object_blocks == 0 {
            model_layout_compaction_blockers.push("no live page refs were rewritten".to_string());
        }
        if !has_model_layouts {
            model_layout_compaction_blockers.push("model layout report is empty".to_string());
        }
        if !preserves_delete_markers {
            model_layout_compaction_blockers
                .push("tombstone object count decreased during compaction".to_string());
        }
        if !improves_density {
            model_layout_compaction_blockers
                .push("live-ref density did not improve or remain stable".to_string());
        }
        if !has_layout_transitions {
            model_layout_compaction_blockers
                .push("slot layout transition evidence is missing".to_string());
        }
        Ok(ShardCompactionReport {
            shard_id,
            model_layout_compaction_ready: model_layout_compaction_blockers.is_empty(),
            model_layout_compaction_evidence: vec![
                "compaction rewrites live refs by model layout".to_string(),
                "packed timestamped model layouts preserve shared page refs".to_string(),
                "tombstone object ids are preserved across compaction".to_string(),
                "stale page density is removed from the compacted live set".to_string(),
                "slot layout transition counts and states are reported after compaction"
                    .to_string(),
                "per-model policies expose tombstone density, stale-page density, object-page packing, and cold-page rewrite eligibility".to_string(),
                "stale segments left behind by moved indexes are reported as reclaimable".to_string(),
            ],
            model_layout_compaction_blockers,
            previous_block_slab_id,
            compacted_block_slab_id: target_block_slab_id,
            blocks_left_by_budget: rewrite_stats.skipped_by_budget,
            bytes_left_by_budget: rewrite_stats.skipped_by_budget_bytes,
            blocks_left_off_drain_set: rewrite_stats.skipped_off_drain_set,
            rewritten_block_refs: rewrite_stats.rewritten_block_refs,
            relocated_bytes: rewrite_stats.relocated_bytes,
            cold_block_rewrite_refs: rewrite_stats.cold_block_rewrite_refs,
            object_block_pack_group_count: before
                .model_policies
                .iter()
                .map(|policy| policy.object_block_pack_group_count as usize)
                .sum(),
            stale_block_slab_ids,
            reclaimable_stale_block_slab_count,
            model_policy_family_count,
            delete_marker_policy_model_count,
            stale_density_policy_model_count,
            layout_aware_policy_model_count,
            model_rewrite_policies: rewrite_stats.into_reports(&before),
            rewritten_object_blocks,
            bucket_layout_transition_count,
            bucket_layout_states_after,
            delete_marked_object_ids_before,
            delete_marked_object_ids_after,
            model_layouts: model_layouts_before,
            before,
            after,
        })
    }
}
