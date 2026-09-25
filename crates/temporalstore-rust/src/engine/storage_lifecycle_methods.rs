// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage lifecycle / WAL-reclaim / eviction methods for TemporalEngine, split from engine.rs.
use super::*;

/// How many dirty-object keys the dump drain has looked at, across every call.
///
/// The drain's cost is not visible from outside -- it is a closure inside a `retain` -- and
/// deriving it as |dirty objects| x |buckets| is arithmetic about the code rather than a
/// measurement of it. This counts what actually happens, which is the only version that keeps
/// being true after someone changes the loop.
pub(crate) static DIRTY_DRAIN_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Bucket-recency entries COPIED on the way into an eviction round, across every call.
///
/// The round used to clone the whole `bucket_recency` map before looking at which selection path
/// was going to run, and only the EXHAUSTIVE path ever read it. Under the shipped default -- the
/// sampled path -- every round copied one entry per bucket the shard had ever touched and dropped
/// the copy unread. That is a cost that tracks the store sitting in front of a selection path
/// whose whole purpose is to cost the batch instead.
///
/// COUNTED, not timed, and counted in ENTRIES rather than bytes: the question is whether the
/// round touches a per-bucket structure at all, and an entry count answers that identically on an
/// idle box and a loaded one. Process-wide, so a reader must reset it immediately before the call
/// it is measuring, and the suite it is read in runs single-threaded.
pub(crate) static EVICTION_RECENCY_ENTRIES_CLONED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// WAL records a `delete_drop` eviction round appends WHILE HOLDING the shard-table write guard.
///
/// The round's index flush was already moved out of the guard -- a stamped clone carries the
/// state out and the encode and both file writes happen unlocked. The per-key WAL tombstones were
/// not, and the comment beside them says so. This counts them, because the shape has been
/// mis-read twice in this engine by argument and settled twice by a counter.
///
/// Incremented at the append itself, which is lexically inside the `shards.write()` block and
/// before the `drop(shards)` below it, so a record counted here was appended under the guard by
/// construction rather than by reading. Process-wide: a reader resets it immediately before the
/// round it is measuring, and the suite it is read in runs single-threaded.
pub(crate) static EVICTION_DELETE_DROP_WAL_APPENDS_UNDER_GUARD: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Where a `delete_drop` round's time under the shard-table write guard actually goes, split by
/// phase, in nanoseconds.
///
/// The append counter above establishes that the tombstone loop runs once per dropped key. It
/// does NOT establish that the loop is worth moving: lifting a phase that is 3% of the hold buys
/// 3%, and the cost of moving it is an ordering argument about `applied_wal_sequence` that nobody
/// has written down. These rows are what makes that question answerable instead of arguable.
///
/// Read as FRACTIONS of one round's own hold, never as absolute times. A fraction is a ratio of
/// two clocks taken microseconds apart on the same thread, so it survives a loaded box in a way
/// an absolute figure does not -- which is why the probe that reads these PRINTS them and asserts
/// only on counts.
///
/// `total` is taken by its OWN clock spanning the whole guarded region -- from the write guard
/// being acquired to it being dropped -- rather than by summing the rows. So `total` minus the
/// rows is an independent residual and not an identity: work that drifts out of every phase lands
/// in it, where a self-summing table would still balance.
pub(crate) struct DeleteDropGuardNanos {
    /// The whole hold, by its own clock.
    pub total: std::sync::atomic::AtomicU64,
    /// `collect_live_block_entries` + the victim-bucket filter: O(shard).
    pub collect: std::sync::atomic::AtomicU64,
    /// `delete_record`, once per key: the in-memory removal from the shard index. This is the
    /// only half of the drop loop that actually needs the shard guard.
    pub delete: std::sync::atomic::AtomicU64,
    /// `invalidate_record_all`, once per key. Takes `&MultiLayerCache` and a key and touches
    /// NOTHING on the shard -- so what it costs is charged to a guard it does not need.
    pub invalidate: std::sync::atomic::AtomicU64,
    /// The per-key WAL tombstone loop, including the mirror hand-off: once per dropped key.
    pub wal_append: std::sync::atomic::AtomicU64,
    /// Reading the sequence the round anchors `applied_wal_sequence` to: once per round.
    pub anchor: std::sync::atomic::AtomicU64,
    /// `shard.clone()`, the stamped snapshot the unlocked index flush is handed: once per round.
    pub snapshot: std::sync::atomic::AtomicU64,
}

impl DeleteDropGuardNanos {
    const fn zeroed() -> Self {
        Self {
            total: std::sync::atomic::AtomicU64::new(0),
            collect: std::sync::atomic::AtomicU64::new(0),
            delete: std::sync::atomic::AtomicU64::new(0),
            invalidate: std::sync::atomic::AtomicU64::new(0),
            wal_append: std::sync::atomic::AtomicU64::new(0),
            anchor: std::sync::atomic::AtomicU64::new(0),
            snapshot: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Process-wide, so a reader resets immediately before the round it is measuring, and the
    /// suite it is read in runs single-threaded.
    pub(crate) fn reset(&self) {
        for cell in self.cells() {
            cell.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn cells(&self) -> [&std::sync::atomic::AtomicU64; 7] {
        [
            &self.total,
            &self.collect,
            &self.delete,
            &self.invalidate,
            &self.wal_append,
            &self.anchor,
            &self.snapshot,
        ]
    }

    /// `(total, collect, delete, invalidate, wal_append, anchor, snapshot)` in nanoseconds.
    pub(crate) fn read(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        let [total, collect, delete, invalidate, wal_append, anchor, snapshot] = self
            .cells()
            .map(|cell| cell.load(std::sync::atomic::Ordering::Relaxed));
        (
            total, collect, delete, invalidate, wal_append, anchor, snapshot,
        )
    }

    /// The hold this round did not attribute to any phase above. Saturating, so an ordering
    /// surprise reads as zero rather than as an enormous number.
    pub(crate) fn unattributed(&self) -> u64 {
        let (total, collect, delete, invalidate, wal_append, anchor, snapshot) = self.read();
        total
            .saturating_sub(collect)
            .saturating_sub(delete)
            .saturating_sub(invalidate)
            .saturating_sub(wal_append)
            .saturating_sub(anchor)
            .saturating_sub(snapshot)
    }

    fn add_since(&self, cell: &std::sync::atomic::AtomicU64, since: std::time::Instant) {
        cell.fetch_add(
            since.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

pub(crate) static DELETE_DROP_GUARD_NANOS: DeleteDropGuardNanos = DeleteDropGuardNanos::zeroed();

/// How many O(shard) plans one round builds, counted rather than timed.
///
/// A round builds these in several stages and each walks the shard. Whether that is duplicated
/// work is a question about COUNTS, and a count is immune to whatever else is running on the box
/// -- timing it beside another build would measure the machine, not the loop.
pub(crate) static LIFECYCLE_PLAN_BUILDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The same, for the WAL reclaim plan. Separate because the two have different costs and a
/// single total would hide which one a change moved.
pub(crate) static WAL_RECLAIM_PLAN_BUILDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Both counters, for a probe measuring one round.
pub fn storage_plan_build_counts() -> (u64, u64) {
    (
        LIFECYCLE_PLAN_BUILDS.load(std::sync::atomic::Ordering::Relaxed),
        WAL_RECLAIM_PLAN_BUILDS.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Zero both, so a probe measures a round rather than a process.
pub fn reset_storage_plan_build_counts() {
    LIFECYCLE_PLAN_BUILDS.store(0, std::sync::atomic::Ordering::Relaxed);
    WAL_RECLAIM_PLAN_BUILDS.store(0, std::sync::atomic::Ordering::Relaxed);
}

impl TemporalEngine {
    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        LIFECYCLE_PLAN_BUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // DO NOT WALK THE SHARD TO FIND OUT THERE IS NOTHING TO DO.
        //
        // `bucket_storage_summaries` materialises every live page in the shard. This call sat at
        // the top of the plan unconditionally, so a settled shard that nobody had written to paid
        // a whole-shard walk on every maintenance round, for ever.
        //
        // It used to be load-bearing: dump selection read `dirty_object_count` off the summaries.
        // #1709 ended that. `dirty_object_count` is now populated from exactly ONE place --
        // `shard.dirty_objects.bucket_counts()`, the last loop in `bucket_storage_summaries` --
        // and every other loop in that function leaves it at its `Default` of 0. So
        //
        //     "some summary has dirty_object_count > 0"   IFF   "the dirty index is non-empty"
        //
        // exactly, and the right-hand side is a `BTreeMap::is_empty` that touches no pages. When
        // the index is empty the filter below selects nothing, `dirty_buckets` is empty, and the
        // walk's only remaining product is the report field -- which is why it survived, and why
        // that field is now an `Option` rather than a vector that reads as a measured zero.
        //
        // The walk is NOT deleted, and one caller can still demand it below:
        // `dump_refreshes_a_vacated_slab` selects a dump with no bucket dirty, and needs the full
        // bucket list to do it. That branch takes the walk itself, so skipping here cannot starve
        // it -- see the `get_or_insert_with` at its site.
        let shard_has_dirty_objects = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&request.shard_id)
            .map(|shard| !shard.dirty_objects.is_empty())
            .unwrap_or(false);
        let mut bucket_summaries: Option<Vec<BucketStorageSummary>> = shard_has_dirty_objects
            .then(|| self.bucket_storage_summaries(request.shard_id));
        // Select the least-recently-dumped (most overdue) dirty buckets first, matching the
        // WAL-reclaim routine's oldest-first-dirty ordering (dirty buckets are consumed
        // in non-decreasing first-dirty-log-id order).
        // bucket_summaries arrives in ascending routing_bucket order (a BTreeMap), so truncating to
        // max_dump_buckets_per_round always dropped the same high-id buckets -- a bucket dirtied
        // once could be starved forever by low-id buckets re-dirtied every round, never
        // checkpointed and pinning the WAL reclaim floor. Ordering by last_dump_sequence
        // ascending makes an overdue bucket rise to the top and guarantees every dirty bucket is
        // eventually selected; routing_bucket is a stable tiebreaker.
        //
        // WHICH `last_dump_sequence` THIS IS, because there are two and they are not the same
        // number. The sort below reads `BucketStorageSummary::last_dump_sequence`, and
        // `bucket_storage_summaries` does not fill that one from the bucket node at all: the node
        // carries its own `last_dump_sequence`, written from `manifest.wal_sequence` by
        // `clear_dumped_bucket_dirty_state`, and the summary's is written by
        // `merge_last_dump_sequence` from the NEWEST manifest's `index_log_sequence` -- the same
        // value for every bucket that manifest names, and 0 for every bucket it does not. So what
        // this key actually separates is "covered by the newest dump" from "not covered by it",
        // not "dumped recently" from "dumped long ago", and the node's own figure never reaches
        // it. `the_summary_last_dump_sequence_comes_from_the_manifest_not_from_the_node` pins
        // that in both directions.
        //
        // Left as it is rather than repointed at the node: this is a TIEBREAKER behind
        // `first_dirty_rank` below, which is the key that decides the order for every bucket that
        // can name its oldest undumped write.
        // Empty EXACTLY when the dirty index is empty, which is the case that skipped the walk
        // above -- so this selects the same buckets it always did, without a fallback that could
        // mistake "did not look" for "looked and found none".
        let dirty_bucket_source: &[BucketStorageSummary] = bucket_summaries.as_deref().unwrap_or(&[]);
        let mut dirty_bucket_summaries = dirty_bucket_source
            .iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .collect::<Vec<_>>();
        // Oldest undumped write first: the bucket that has held the log longest is dumped first.
        //
        // `last_dump_sequence` answers "is this bucket in the newest dump manifest" (see the note
        // above it), which is a proxy for "most overdue" and not the same question. What holds
        // the log is the bucket's OLDEST UNDUMPED WRITE, and `first_dirty_wal_sequence` is that
        // -- so ordering by it dumps the bucket pinning the log's floor first, and no bucket can
        // be starved indefinitely while older ones keep being re-dirtied.
        //
        // Read from the bucket node rather than added to `BucketStorageSummary`: a summary is
        // stored in the dump manifest and compared field-by-field when a manifest is validated,
        // so a new field there is a durability contract, not a report field.
        //
        // 0 means no claim recorded, which sorts LAST: a bucket we cannot place in the log is not
        // evidence of being old, and the ones we can place are the ones whose dump moves the
        // floor.
        let first_dirty_by_bucket = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&request.shard_id)
            .map(|shard| {
                shard
                    .bucket_index
                    .bucket_map
                    .iter()
                    .map(|(routing_bucket, bucket)| {
                        (*routing_bucket, bucket.first_dirty_wal_sequence)
                    })
                    .collect::<std::collections::HashMap<u32, u64>>()
            })
            .unwrap_or_default();
        let first_dirty_rank = |routing_bucket: u32| -> u64 {
            match first_dirty_by_bucket.get(&routing_bucket).copied() {
                Some(0) | None => u64::MAX,
                Some(sequence) => sequence,
            }
        };
        dirty_bucket_summaries.sort_by(|left, right| {
            first_dirty_rank(left.routing_bucket)
                .cmp(&first_dirty_rank(right.routing_bucket))
                .then_with(|| left.last_dump_sequence.cmp(&right.last_dump_sequence))
                .then_with(|| left.routing_bucket.cmp(&right.routing_bucket))
        });
        let dirty_buckets = dirty_bucket_summaries
            .iter()
            .map(|summary| summary.routing_bucket)
            .collect::<Vec<_>>();
        let latest_bucket_dump_manifest =
            latest_bucket_dump_manifest_shared_at(&self.index_dir, request.shard_id);
        let latest_dump_wal_sequence = latest_bucket_dump_manifest
            .as_ref()
            .map(|manifest| manifest.wal_sequence)
            .unwrap_or_default();
        let wal_stats = self.wal_store.stats(request.shard_id);
        let current_wal_sequence = wal_stats.last_sequence;
        let undumped_wal_records =
            current_wal_sequence.saturating_sub(latest_dump_wal_sequence);
        let explicit_buckets = !request.selected_dump_buckets.is_empty();
        // Durable bytes that are UNDUMPED, not the log's size. `persistent_bytes` is the whole
        // log across every segment, so a log that is large but fully dumped cleared this
        // threshold on every round: a shard that had written one record since its last dump
        // earned another whole-index serialize, which is the cost this cadence exists to avoid.
        let undumped_wal_bytes = self.wal_store.undumped_len_since_dump(request.shard_id);
        // Reported, not compared. See `StorageLifecyclePlan::undumped_wal_objects`.
        let undumped_wal_objects = self.wal_store.undumped_objects_since_dump(request.shard_id);
        // Each threshold can only RELEASE the dump, never hold it: a delay needs both to agree
        // there is not enough yet. Requiring both to be CROSSED instead would let the byte
        // threshold suppress a dump the record count had already earned, which is the opposite
        // of bounding the log.
        let records_say_wait = request.min_undumped_wal_records > 0
            && undumped_wal_records < request.min_undumped_wal_records;
        let bytes_say_wait = request.min_undumped_wal_bytes == 0
            || undumped_wal_bytes < request.min_undumped_wal_bytes;
        let dump_delayed = !explicit_buckets && records_say_wait && bytes_say_wait;
        let mut selected_dump_buckets = if explicit_buckets {
            request.selected_dump_buckets.clone()
        } else if dump_delayed {
            Vec::new()
        } else {
            dirty_buckets.clone()
        };
        if request.max_dump_buckets_per_round > 0
            && selected_dump_buckets.len() > request.max_dump_buckets_per_round
        {
            selected_dump_buckets.truncate(request.max_dump_buckets_per_round);
        }
        let live_block_slab_ids = self.live_block_slab_ids(request.shard_id);
        let live_block_slab_set = live_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let stale_block_slab_ids = self
            .block_store
            .slab_ids()
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !live_block_slab_set.contains(id))
            .collect::<Vec<_>>();
        let reclaim_slab_reports = self.storage_reclaim_slab_reports(request.shard_id);
        let stale_block_slab_set = stale_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        // REFRESH A DUMP THAT STILL NAMES A SLAB COMPACTION HAS EMPTIED.
        //
        // `run_gc_inner` and `storage_block_gc_dependency_plan` both hold back every slab a bucket
        // dump manifest names, and they are right to: installing that dump READS those pages, so
        // destroying the slab makes the dump uninstallable and a lagging replica unservable.
        // #1565 stopped such a slab freezing the retain FLOOR; it deliberately did not make the
        // slab deletable, and said so.
        //
        // What that leaves is a dead slab standing for ever on an IDLE shard. Compaction copies
        // the live pages onto a fresh slab, and the newest manifest goes on naming the slab it
        // emptied -- because a manifest is only ever replaced by a DUMP, a dump is selected from
        // DIRTY buckets, and an idle shard has none. Measured on a settled shard that nobody is
        // writing to, in EVERY settled round, at 8,000 records and again at 80,000:
        // slabs 2, stale 1, reclaim_candidates 1, relocatable 0.
        //
        // So ask for the dump rather than keeping the slab. It re-exports the index compaction
        // has already persisted, the replacement manifest names only slabs that are live, the
        // superseded one stops adding coverage and is pruned, and the vacated slab is collected
        // on the next round. Nothing is destroyed while a manifest still needs it: the order is
        // dump, prune, then collect, each in its own round.
        //
        // TERMINATING BY CONSTRUCTION, which is what keeps this off a busy shard and off a loop.
        // The condition reads the NEWEST manifest only, and the dump it selects BECOMES the
        // newest and names live slabs alone -- so it cannot ask twice for the same vacated slab.
        // An OLDER manifest kept because it is the only dump covering some bucket can still hold
        // a slab back; that is a retention decision this does not override, and it does not
        // re-arm this either.
        //
        // RE-CHECKED after `block_slab_ids` was widened to every slab the manifest's whole-shard
        // index can install, rather than only the dumped buckets'. The argument turns on one
        // premise -- that a fresh dump names LIVE slabs only -- and widening makes that premise
        // exact rather than weakening it: the widened set is derived from the live page refs of
        // the index the dump exports, which IS the shard's live slab set at that moment, so every
        // id in it is live by construction. What changes is how often this fires, not whether it
        // ends: a slab holding nothing but unnamed-bucket pages is now named, so vacating it now
        // arms this where before it armed nothing and the slab was destroyed under a manifest
        // that needed it. Each firing still replaces the newest manifest with one naming live
        // slabs alone, so it still cannot fire twice for the same vacated slab.
        let latest_manifest_names_a_vacated_slab = latest_bucket_dump_manifest
            .as_ref()
            .map(|manifest| {
                manifest
                    .block_slab_ids
                    .iter()
                    .any(|block_slab_id| stale_block_slab_set.contains(block_slab_id))
            })
            .unwrap_or(false);
        let dump_refreshes_a_vacated_slab = !explicit_buckets
            && selected_dump_buckets.is_empty()
            && latest_manifest_names_a_vacated_slab;
        if dump_refreshes_a_vacated_slab {
            // Every bucket the shard holds now, and NOT truncated to
            // `max_dump_buckets_per_round`. A replacement that covers less than what it
            // supersedes supersedes nothing: the older manifest stays retained for the coverage
            // it alone holds, the slab stays pinned, and the round above will not ask again. The
            // cap costs nothing to skip here -- `create_bucket_dump_manifest` exports the whole
            // index whatever the selection is, so a narrower one would be the same work for a
            // result that releases nothing.
            //
            // THIS BRANCH FIRES ON AN IDLE SHARD BY DESIGN -- no bucket is dirty, which is
            // precisely the state that skipped the walk at the top of this function. So take it
            // here. `get_or_insert_with` walks only if the plan has not already, which keeps a
            // dirty round at one walk and makes this the only round that pays for a second.
            selected_dump_buckets = bucket_summaries
                .get_or_insert_with(|| self.bucket_storage_summaries(request.shard_id))
                .iter()
                .map(|summary| summary.routing_bucket)
                .collect::<Vec<_>>();
        }
        let mut reclaim_candidates = storage_reclaim_candidates_from_slab_reports(
            &reclaim_slab_reports,
            &stale_block_slab_set,
        );
        let delayed_destroy_reports = self
            .block_store
            .delayed_destroy_slab_reports()
            .unwrap_or_default();
        reclaim_candidates.extend(delayed_destroy_reports.iter().map(|report| {
            StorageReclaimCandidate {
                block_slab_id: report.block_slab_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: 0,
                stale_physical_bytes: report.physical_bytes,
                reclaim_score: report.physical_bytes.saturating_mul(2),
                reason: "delayed_destroy".to_string(),
                ..StorageReclaimCandidate::default()
            }
        }));
        reclaim_candidates.sort_by(|left, right| {
            right
                .reclaim_score
                .cmp(&left.reclaim_score)
                .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
                .then_with(|| left.block_slab_id.cmp(&right.block_slab_id))
        });
        let mut reasons = Vec::new();
        if dump_refreshes_a_vacated_slab {
            // A distinct reason, not "dirty_slot_dump": no bucket is dirty, and an operator
            // reading the plan needs to see that this dump is the collector's precondition
            // rather than a write being checkpointed.
            reasons.push("slot_dump_refresh_after_relocation".to_string());
        } else if !selected_dump_buckets.is_empty() {
            reasons.push("dirty_slot_dump".to_string());
        } else if dump_delayed && !dirty_buckets.is_empty() {
            reasons.push("dirty_slot_dump_delayed".to_string());
        }
        if !stale_block_slab_ids.is_empty() {
            reasons.push("stale_page_segment_gc".to_string());
        }
        if !reclaim_candidates.is_empty() {
            reasons.push("ranked_reclaim_candidates".to_string());
        }
        if request.purge_delayed_destroy && !delayed_destroy_reports.is_empty() {
            reasons.push("delayed_destroy_purge".to_string());
        }
        let block_gc_dependency_plan = self.storage_block_gc_dependency_plan(
            request.shard_id,
            reclaim_candidates
                .iter()
                .map(|candidate| candidate.block_slab_id),
            request.block_gc_shared_store_cursors.clone(),
            request.block_gc_raft_snapshot_refs.clone(),
            request.block_gc_checkpoint_floor_slab_id,
            request.block_gc_raft_install_floor_slab_id,
            request.block_gc_delayed_destroy_grace_ms,
        );
        if !block_gc_dependency_plan
            .candidate_block_slab_ids
            .is_empty()
            && !block_gc_dependency_plan.safe_to_reclaim
        {
            reasons.push("page_gc_dependency_blocked".to_string());
        }
        let manifest_prune_plan = self.bucket_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        if !manifest_prune_plan.prunable_manifest_ids.is_empty()
            || !manifest_prune_plan.prunable_marker_manifest_ids.is_empty()
        {
            reasons.push("slot_dump_manifest_prune".to_string());
        }
        if !self
            .interrupted_bucket_dump_installs(request.shard_id)
            .is_empty()
        {
            reasons.push("slot_dump_install_roll_forward_check".to_string());
        }
        if request.invalidate_cache {
            reasons.push("cache_invalidation".to_string());
        }
        StorageLifecyclePlan {
            shard_id: request.shard_id,
            dirty_buckets,
            selected_dump_buckets,
            undumped_wal_records,
            undumped_wal_objects,
            dump_delayed,
            bucket_summaries,
            live_block_slab_ids,
            stale_block_slab_ids,
            reclaim_candidates,
            delayed_destroy_block_slab_ids: delayed_destroy_reports
                .iter()
                .map(|report| report.block_slab_id)
                .collect(),
            reclaimable_physical_bytes: delayed_destroy_reports
                .iter()
                .map(|report| report.physical_bytes)
                .sum(),
            reasons,
        }
    }

    pub fn storage_block_gc_dependency_plan(
        &self,
        shard_id: ShardId,
        candidate_block_slab_ids: impl IntoIterator<Item = u64>,
        shared_store_cursors: impl IntoIterator<Item = StorageBlockGcReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
        checkpoint_snapshot_floor: Option<u64>,
        raft_snapshot_install_floor: Option<u64>,
        delayed_destroy_grace_ms: u64,
    ) -> StorageBlockGcDependencyPlan {
        let mut candidate_block_slab_ids = candidate_block_slab_ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        candidate_block_slab_ids.sort_unstable();
        let candidate_set = candidate_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let live_block_slab_ids = self.live_block_slab_ids(shard_id);
        let live_set = live_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let manifests =
            list_bucket_dump_manifests_shared_at(&self.index_dir, shard_id).unwrap_or_default();
        let mut manifest_block_slab_ids = manifests
            .iter()
            .flat_map(|manifest| manifest.block_slab_ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        manifest_block_slab_ids.sort_unstable();
        let manifest_set = manifest_block_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let shared_store_cursors = shared_store_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let delayed_destroy_reports = self
            .block_store
            .delayed_destroy_slab_reports()
            .unwrap_or_default();
        let delayed_destroy_modified = delayed_destroy_reports
            .iter()
            .map(|report| (report.block_slab_id, report.modified_unix_ms))
            .collect::<BTreeMap<_, _>>();
        let now = now_ms();
        let mut dependency_blocks = Vec::new();
        for block_slab_id in &candidate_block_slab_ids {
            if live_set.contains(block_slab_id) {
                dependency_blocks.push(StorageBlockGcDependencyBlock {
                    block_slab_id: *block_slab_id,
                    dependency: "live_page_ref".to_string(),
                    owner_id: format!("shard:{shard_id}"),
                    reason: "indexed live page references still point at this page segment"
                        .to_string(),
                    ..StorageBlockGcDependencyBlock::default()
                });
            }
            if manifest_set.contains(block_slab_id) {
                let owner_id = manifests
                    .iter()
                    .filter(|manifest| manifest.block_slab_ids.contains(block_slab_id))
                    .map(|manifest| manifest.manifest_id.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                dependency_blocks.push(StorageBlockGcDependencyBlock {
                    block_slab_id: *block_slab_id,
                    dependency: "slot_dump_manifest".to_string(),
                    owner_id,
                    reason: "slot dump manifest still names this page segment".to_string(),
                    ..StorageBlockGcDependencyBlock::default()
                });
            }
            for cursor in shared_store_cursors
                .iter()
                .filter(|cursor| cursor.shard_id == shard_id)
            {
                if *block_slab_id >= cursor.retain_from_block_slab_id {
                    dependency_blocks.push(StorageBlockGcDependencyBlock {
                        block_slab_id: *block_slab_id,
                        dependency: "shared_store_replay_cursor".to_string(),
                        owner_id: cursor.cursor_id.clone(),
                        retain_from_block_slab_id: Some(cursor.retain_from_block_slab_id),
                        reason: if cursor.reason.is_empty() {
                            "shared-store replay cursor has not advanced past this page segment"
                                .to_string()
                        } else {
                            cursor.reason.clone()
                        },
                        ..StorageBlockGcDependencyBlock::default()
                    });
                }
            }
            for snapshot in raft_snapshot_refs
                .iter()
                .filter(|snapshot| snapshot.shard_id == shard_id)
            {
                if *block_slab_id >= snapshot.index_log_sequence {
                    dependency_blocks.push(StorageBlockGcDependencyBlock {
                        block_slab_id: *block_slab_id,
                        dependency: "raft_snapshot_ref".to_string(),
                        owner_id: snapshot.snapshot_id.clone(),
                        retain_from_block_slab_id: Some(snapshot.index_log_sequence),
                        reason: "Raft snapshot reference has not released this page segment floor"
                            .to_string(),
                        ..StorageBlockGcDependencyBlock::default()
                    });
                }
            }
            if checkpoint_snapshot_floor
                .map(|floor| *block_slab_id >= floor)
                .unwrap_or(false)
            {
                dependency_blocks.push(StorageBlockGcDependencyBlock {
                    block_slab_id: *block_slab_id,
                    dependency: "checkpoint_snapshot_floor".to_string(),
                    owner_id: format!("checkpoint:{shard_id}"),
                    retain_from_block_slab_id: checkpoint_snapshot_floor,
                    reason: "checkpoint/snapshot floor still retains this page segment".to_string(),
                    ..StorageBlockGcDependencyBlock::default()
                });
            }
            if raft_snapshot_install_floor
                .map(|floor| *block_slab_id >= floor)
                .unwrap_or(false)
            {
                dependency_blocks.push(StorageBlockGcDependencyBlock {
                    block_slab_id: *block_slab_id,
                    dependency: "raft_snapshot_install_floor".to_string(),
                    owner_id: format!("raft-install:{shard_id}"),
                    retain_from_block_slab_id: raft_snapshot_install_floor,
                    reason: "Raft snapshot install floor still retains this page segment"
                        .to_string(),
                    ..StorageBlockGcDependencyBlock::default()
                });
            }
            if delayed_destroy_grace_ms > 0 {
                if let Some(modified_unix_ms) = delayed_destroy_modified
                    .get(block_slab_id)
                    .and_then(|modified| *modified)
                {
                    let retain_until = modified_unix_ms.saturating_add(delayed_destroy_grace_ms);
                    if now < retain_until {
                        dependency_blocks.push(StorageBlockGcDependencyBlock {
                            block_slab_id: *block_slab_id,
                            dependency: "delayed_destroy_grace_period".to_string(),
                            owner_id: format!("delayed-destroy:{block_slab_id}"),
                            retain_until_unix_ms: Some(retain_until),
                            reason:
                                "delayed-destroy grace period has not elapsed for this page segment"
                                    .to_string(),
                            ..StorageBlockGcDependencyBlock::default()
                        });
                    }
                }
            }
        }
        let blocked_block_slab_ids = dependency_blocks
            .iter()
            .map(|block| block.block_slab_id)
            .collect::<BTreeSet<_>>();
        let dependency_count = |dependency: &str| {
            dependency_blocks
                .iter()
                .filter(|block| block.dependency == dependency)
                .count()
        };
        let live_ref_block_count = dependency_count("live_page_ref");
        let bucket_dump_manifest_block_count = dependency_count("slot_dump_manifest");
        let shared_store_cursor_block_count = dependency_count("shared_store_replay_cursor");
        let raft_snapshot_ref_block_count = dependency_count("raft_snapshot_ref");
        let checkpoint_snapshot_floor_block_count = dependency_count("checkpoint_snapshot_floor");
        let raft_snapshot_install_floor_block_count =
            dependency_count("raft_snapshot_install_floor");
        let delayed_destroy_grace_block_count = dependency_count("delayed_destroy_grace_period");
        let reclaimable_block_slab_ids = candidate_block_slab_ids
            .iter()
            .copied()
            .filter(|id| !blocked_block_slab_ids.contains(id))
            .collect::<Vec<_>>();
        let blocked_block_slab_ids = blocked_block_slab_ids.into_iter().collect::<Vec<_>>();
        let mut blocker_reasons = dependency_blocks
            .iter()
            .map(|block| block.dependency.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if candidate_set.is_empty() {
            blocker_reasons.clear();
        }
        StorageBlockGcDependencyPlan {
            shard_id,
            safe_to_reclaim: !candidate_set.is_empty() && dependency_blocks.is_empty(),
            candidate_block_slab_ids,
            reclaimable_block_slab_ids,
            blocked_block_slab_ids,
            live_block_slab_ids,
            manifest_block_slab_ids,
            shared_store_cursor_count: shared_store_cursors
                .iter()
                .filter(|cursor| cursor.shard_id == shard_id)
                .count(),
            checkpoint_snapshot_floor,
            raft_snapshot_install_floor,
            delayed_destroy_grace_ms,
            live_ref_block_count,
            bucket_dump_manifest_block_count,
            shared_store_cursor_block_count,
            raft_snapshot_ref_block_count,
            checkpoint_snapshot_floor_block_count,
            raft_snapshot_install_floor_block_count,
            delayed_destroy_grace_block_count,
            dependency_blocks,
            blocker_reasons,
        }
    }

    /// Clear the dirty state of buckets just captured by `manifest` (a dumped
    /// bucket has its dirty flag cleared), so the storage cycle does not
    /// re-select and re-dump them every round. A bucket re-dirtied since the manifest
    /// snapshot (its current derived generation no longer equals the captured one) is
    /// left dirty, so reclaim never advances past an undumped write. The bucket's
    /// generation is held at the captured (derived) value and its live pages are
    /// untouched, so bucket_dump_summary_matches_current_generation keeps matching and
    /// WAL/index reclaim gating is unchanged.
    pub(super) fn clear_dumped_bucket_dirty_state(
        &self,
        shard_id: ShardId,
        manifest: &BucketDumpManifest,
    ) {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return;
        };
        // Per-bucket derived generation the manifest captured (base + dirty count) --
        // exactly what the reclaim fingerprint compares.
        let captured: std::collections::HashMap<u32, u64> = manifest
            .bucket_summaries
            .iter()
            .map(|summary| (summary.routing_bucket, summary.dirty_generation))
            .collect();
        // Current derived generation BEFORE mutation: detects writes that landed after
        // the dump snapshot (those buckets must stay dirty for the next dump).
        //
        // DO NOT HOIST THIS INTO A SHARED SNAPSHOT. `who_walks_the_shard` shows
        // `bucket_storage_summaries` entered THREE times in one `apply_storage_lifecycle` -- here,
        // in `storage_lifecycle_plan`, and in `create_bucket_dump_manifest` -- and three identical
        // whole-shard walks in one operation look exactly like something to share. Two of them
        // can be. This one cannot, and the reason is the line above rather than anything about
        // locking.
        //
        // This walk exists to be FRESH. It is compared against the generation the manifest
        // CAPTURED, and a bucket whose generation has moved since is skipped so it stays dirty for
        // the next dump. Feed it a snapshot taken when the plan ran and both sides become equal by
        // construction: a bucket written to between the dump and this clear compares equal, is
        // cleared, stops being dirty, is never dumped, and reclaim is then free to advance past the
        // records it still needed. That is silent data loss, discovered on a later restart.
        //
        // The test that would catch it is not obvious either: any fixture that does not write
        // CONCURRENTLY with a dump will pass a hoisted version happily.
        let current: std::collections::HashMap<u32, u64> =
            bucket_storage_summaries(shard, start_routing_bucket, end_routing_bucket)
                .into_iter()
                .map(|summary| (summary.routing_bucket, summary.dirty_generation))
                .collect();
        // Buckets this dump actually clears. Collected first so the dirty set is walked ONCE.
        //
        // The retain used to sit inside this loop, so every qualifying bucket walked every dirty
        // object and re-hashed its key to recompute a routing bucket -- a bucket the caller
        // already knew, and one that `mark_async_dirty_object` had computed on the line above the
        // insert and thrown away. The work was |dirty objects| x |buckets|, to remove at most
        // |dirty objects| entries: measured at 4 040 000 closure calls to clear 4 000 objects
        // across 1 010 buckets, a 1010x amplification.
        //
        // Nothing in the per-bucket body depends on the dirty set having been cleared, and
        // `current` was captured before any mutation, so hoisting the walk out is the same answer
        // in one pass.
        let mut cleared_buckets: Vec<u32> = Vec::new();
        for bucket_id in manifest.bucket_ids.iter().copied() {
            let Some(&captured_generation) = captured.get(&bucket_id) else {
                continue;
            };
            if current.get(&bucket_id).copied().unwrap_or_default() != captured_generation {
                continue;
            }
            cleared_buckets.push(bucket_id);
            if let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&bucket_id) {
                // Hold the generation at the captured (derived) value so the reclaim
                // fingerprint still matches once the dirty objects are cleared.
                bucket.dirty_generation = bucket.dirty_generation.max(captured_generation);
                // Record the dumped-log sequence (informational; not part of the fingerprint).
                bucket.last_dump_sequence = bucket.last_dump_sequence.max(manifest.wal_sequence);
                bucket.set_dirty(false);
                // The dump captured everything this bucket had, so it holds no claim over the
                // log until it is written to again.
                bucket.first_dirty_wal_sequence = 0;
                bucket.first_dirty_index_log_sequence = 0;
                for page in bucket.block_index.blocks_mut_unaccounted() {
                    page.dirty = false;
                }
            }
        }
        if !cleared_buckets.is_empty() {
            // STRAIGHT TO THE CLEARED BUCKETS' KEYS.
            //
            // The retain this replaces walked the WHOLE dirty set and re-hashed every key to
            // recompute a routing bucket, whether or not that key belonged to a bucket this dump
            // cleared. A round that dumps a slice of the shard -- which is what
            // `max_dump_buckets_per_round` asks for -- paid for the whole set to drop a slice of
            // it. The dirty index is keyed by bucket, so the keys to drop are addressable and
            // the rest are not touched.
            //
            // The counter still measures what the drain LOOKS AT, which is now exactly what it
            // removes: it visits the cleared buckets' key sets and nothing else.
            let dropped = shard.dirty_objects.drain_buckets(&cleared_buckets);
            DIRTY_DRAIN_VISITS.fetch_add(dropped as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn apply_storage_lifecycle(
        &self,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleReport {
        let plan = self.storage_lifecycle_plan(request.clone());
        let dump_manifest = if plan.selected_dump_buckets.is_empty() {
            None
        } else {
            self.create_bucket_dump_manifest(request.shard_id, plan.selected_dump_buckets.clone())
                .ok()
        };
        if let Some(manifest) = &dump_manifest {
            // Once a bucket is dumped its dirty flag is cleared, so the storage cycle
            // stops re-selecting and re-dumping it every round. Dumped state is anchored
            // by the WAL watermark (last_dump_sequence / manifest.wal_sequence, the
            // dumped-log sequence), not by the dirty flag.
            self.clear_dumped_bucket_dirty_state(request.shard_id, manifest);
        }
        let (cache_entries_removed, cache_disk_bytes_removed) = if request.invalidate_cache {
            self.cache
                .invalidate_shard(request.shard_id)
                .map(|report| (report.memory_entries_removed, report.disk_bytes_removed))
                .unwrap_or_default()
        } else {
            (0, 0)
        };
        let cache_warmup = if request.warm_cache {
            self.storage_cache_warmup_report(request.shard_id, plan.selected_dump_buckets.clone())
        } else {
            StorageCacheWarmupReport {
                shard_id: request.shard_id,
                selected_buckets: plan.selected_dump_buckets.clone(),
                ..StorageCacheWarmupReport::default()
            }
        };
        let cache_warmup_block_refs = cache_warmup.warmed_block_refs;
        let purge_report = if request.purge_delayed_destroy {
            // THE LIVE SET AS IT IS NOW, not as it was when the collector quarantined these
            // slabs. The destroy below is irreversible and the two moments are whole rounds
            // apart; between them this same round may have written a dump manifest whose
            // embedded index installs pages in a slab that was unreferenced when the collector
            // looked. Assembled exactly as the operator `/gc` path assembles it -- live page
            // refs across every loaded shard, widened by every slab a retained manifest's index
            // can install -- so the two reclaim paths cannot disagree about what is still needed.
            let mut purge_live_block_slab_ids = self.live_block_slab_ids_all_shards();
            for manifest in
                list_bucket_dump_manifests_shared_at(&self.index_dir, request.shard_id)
                    .unwrap_or_default()
            {
                purge_live_block_slab_ids.extend(manifest.block_slab_ids.iter().copied());
            }
            self.block_store
                .purge_delayed_destroy_slabs_selected(
                    crate::block_store::DELAYED_DESTROY_MIN_AGE_MS,
                    purge_live_block_slab_ids,
                    request
                        .purge_delayed_destroy_slab_ids
                        .as_ref()
                        .map(|ids| ids.iter().copied().collect::<BTreeSet<_>>()),
                )
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let manifest_prune_plan = self.bucket_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        // Roll forward interrupted installs BEFORE pruning: prune removes obsolete
        // install markers, which would otherwise drop an interrupted install before it
        // can be recovered (leaving install_roll_forward_reports empty even though an
        // interrupted install was present).
        let install_roll_forward_reports = if request.roll_forward_bucket_dump_installs {
            self.roll_forward_bucket_dump_installs(request.shard_id)
        } else {
            self.bucket_dump_install_roll_forward_reports(request.shard_id)
        };
        let manifest_prune_report = request.prune_bucket_dump_manifests.then(|| {
            self.apply_bucket_dump_manifest_prune_with_follower_cursors(
                request.shard_id,
                request.follower_replay_cursors.clone(),
            )
        });
        let object_lifecycle = self.storage_object_lifecycle_snapshot(request.shard_id);
        let mut report = StorageLifecycleReport {
            shard_id: request.shard_id,
            public_storage_contract: Default::default(),
            public_storage_feature_shapes: Default::default(),
            effective_storage_tuning: effective_storage_tuning_from_env(),
            storage_lifecycle_metrics: default_storage_lifecycle_metrics(),
            storage_write_contract: default_storage_write_contract_empty(),
            storage_read_contract: default_storage_read_contract_empty(),
            storage_cold_scan_contract: default_storage_cold_scan_contract_empty(),
            storage_manager_contract: default_storage_manager_contract_empty(),
            storage_index_contract: default_storage_index_contract_empty(),
            storage_cache_contract: default_storage_cache_contract_empty(),
            storage_reclaim_contract: default_storage_reclaim_contract_empty(),
            storage_safety_snapshot: Default::default(),
            storage_watermark_snapshot: Default::default(),
            storage_gc_snapshot: Default::default(),
            storage_index_snapshot: Default::default(),
            storage_topology_snapshot: Default::default(),
            storage_write_sequence: default_storage_write_sequence(),
            storage_read_sequence: default_storage_read_sequence(),
            storage_cold_scan_sequence: default_storage_cold_scan_sequence(),
            storage_lifecycle_phases: default_storage_lifecycle_phases(),
            storage_cache_layers: default_storage_cache_layers(),
            storage_cache_semantics: default_storage_cache_semantics(),
            storage_reclaim_semantics: default_storage_reclaim_semantics(),
            storage_reclaim_scope: Default::default(),
            plan,
            dump_manifest,
            cache_entries_removed,
            cache_disk_bytes_removed,
            cache_warmup_block_refs,
            cache_warmup,
            delayed_destroy_purged_slabs: purge_report.purged_block_slab_ids,
            delayed_destroy_purged_bytes: purge_report.purged_physical_bytes,
            delayed_destroy_restored_slabs: purge_report.restored_block_slab_ids,
            delayed_destroy_restored_bytes: purge_report.restored_physical_bytes,
            manifest_prune_plan,
            manifest_prune_report,
            install_roll_forward_reports,
            object_lifecycle,
        };
        report.refresh_public_lifecycle_metrics();
        if let Some(shard) = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
        {
            // ONE walk for all four sampling snapshots.
            //
            // Each of these built its own `collect_live_block_entries` -- a fresh Vec of every live
            // page in the shard -- and then sorted all of it to take EIGHT samples. Four walks and
            // four full sorts, for about thirty-two sample rows. `who_walks_the_shard` measured
            // them at 1.0x the shard apiece, 4.0x of `apply_storage_lifecycle`'s 10.0x.
            //
            // Safe here in the way #1586 was and the dirty-state walk in #1607 was NOT: the read
            // lock above covers all four, `&ShardState` is unchanged throughout, and these produce
            // report SAMPLES rather than a decision. A shared snapshot cannot change what the
            // system does; it only makes the four samples describe one moment instead of four.
            let sampling_entries = collect_live_block_entries(shard);
            report.storage_index_snapshot = storage_index_snapshot_with_samples_from_entries(
                request.shard_id,
                &sampling_entries,
                report.storage_index_snapshot,
            );
            report.storage_watermark_snapshot =
                storage_watermark_snapshot_with_samples_from_entries(
                    request.shard_id,
                    shard,
                    &sampling_entries,
                    report.storage_watermark_snapshot,
                );
            report.storage_gc_snapshot = storage_gc_snapshot_with_samples_from_entries(
                request.shard_id,
                shard,
                &sampling_entries,
                report.storage_gc_snapshot,
            );
            report.storage_topology_snapshot = storage_topology_snapshot_with_samples_from_entries(
                request.shard_id,
                shard,
                &sampling_entries,
                report.storage_topology_snapshot,
            );
        }
        report
    }

    pub fn storage_wal_reclaim_plan(
        &self,
        shard_id: ShardId,
        follower_replay_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
    ) -> StorageWalReclaimPlan {
        WAL_RECLAIM_PLAN_BUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let follower_replay_cursors = follower_replay_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let current_wal_sequence = self.write_ahead_log_store().stats(shard_id).last_sequence;
        let current_index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let bucket_summaries = self.bucket_storage_summaries(shard_id);
        let current_bucket_fingerprints = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&shard_id)
            .map(bucket_generation_fingerprints_by_bucket)
            .unwrap_or_default();
        // What each bucket holds over the two logs when no manifest covers it.
        //
        // A dirty bucket with no durable dump manifest is captured nowhere, so the only answer
        // the plan could give for it was "retain everything" -- floor 0. These two claims say
        // where the bucket's oldest undumped write actually sits, so it can hold the logs from
        // there instead of from the beginning.
        let bucket_claims = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&shard_id)
            .map(|shard| {
                shard
                    .bucket_index
                    .bucket_map
                    .iter()
                    .map(|(routing_bucket, bucket)| {
                        (
                            *routing_bucket,
                            (
                                bucket.first_dirty_wal_sequence,
                                bucket.first_dirty_index_log_sequence,
                            ),
                        )
                    })
                    .collect::<std::collections::HashMap<u32, (u64, u64)>>()
            })
            .unwrap_or_default();
        let manifests =
            list_bucket_dump_manifests_shared_at(&self.index_dir, shard_id).unwrap_or_default();
        let mut missing_bucket_generations = Vec::new();
        let mut retained_manifest_ids = BTreeSet::<String>::new();
        let mut durable_wal_frontier = u64::MAX;
        let mut durable_index_log_frontier = u64::MAX;
        let mut covered_bucket_count = 0usize;

        // Decode each manifest's index ONCE, not once per bucket.
        //
        // This sat inside the loop below, so every bucket re-deserialized every manifest's
        // WHOLE shard index out of JSON and rebuilt the generation fingerprints for every
        // bucket in it -- identical work, repeated once per bucket. The manifests are read
        // before the loop and do not change while the plan is computed, so one pass produces
        // the same answer every visit would have.
        //
        // The cost was quadratic in the corpus: a real (non-dry-run) WAL reclaim took 6.5s at
        // 1k records, 23s at 2k and 100s at 4k -- x4 per doubling -- and a 40k shard did not
        // finish in ten minutes with a core pegged at 100%. Reclaim is the only thing that
        // removes WAL and index-log bytes, so a shard large enough to need it was a shard on
        // which it could not run.
        // Fingerprint a manifest only when a bucket actually reaches it.
        //
        // This was an EAGER pass over every retained manifest, and each entry is a full
        // shard-sized walk: `bucket_generation_fingerprints_by_bucket` calls
        // `collect_live_block_entries`. So the plan paid one whole-shard walk PER RETAINED
        // MANIFEST, every round, before knowing whether any bucket would consult them --
        // measured by `which_stages_walk_every_live_block` as the bulk of reclaim_wal's 16x.
        //
        // The search below is newest-first and short-circuits, and most buckets match the
        // newest manifest, so nearly all of those walks were computed and thrown away.
        //
        // `None` = not computed yet. `Some(None)` = computed, and its index would not decode
        // (which matched nothing before and matches nothing now). `Some(Some(map))` = computed.
        // The memo lives for THIS call only, so there is no cross-round staleness question: a
        // manifest's index bytes cannot change while one plan is being built.
        let mut manifest_fingerprints: Vec<
            Option<Option<BTreeMap<u32, BTreeSet<String>>>>,
        > = (0..manifests.len()).map(|_| None).collect();

        // Index each manifest's bucket summaries BY ROUTING BUCKET, once.
        //
        // The loop below asked `manifest.bucket_summaries.iter().any(..)` per bucket, so a shard
        // with N buckets and a manifest carrying N of them ran N*N comparisons -- and
        // `bucket_dump_summary_matches_current_generation` CLONES AND SORTS both slab vectors on
        // every call. At 32,000 buckets that is about a billion of them: measured, reclaim_wal
        // was 21,448 ms of a 23,310 ms round, on a loop whose period is 30 s.
        //
        // Equivalent by construction: the predicate's FIRST condition is
        // `manifest_summary.routing_bucket == current_summary.routing_bucket`, so only summaries
        // sharing a routing bucket could ever match. Duplicates under one bucket are kept in a
        // vector and still tried with `any`, so a manifest carrying two summaries for one bucket
        // behaves as it did.
        let manifest_summaries_by_bucket = manifests
            .iter()
            .map(|manifest| {
                let mut by_bucket =
                    std::collections::HashMap::<u32, Vec<&BucketStorageSummary>>::new();
                for manifest_summary in &manifest.bucket_summaries {
                    by_bucket
                        .entry(manifest_summary.routing_bucket)
                        .or_default()
                        .push(manifest_summary);
                }
                by_bucket
            })
            .collect::<Vec<_>>();

        for summary in &bucket_summaries {
            // Same search as before -- newest manifest first, first match wins -- written as a
            // loop rather than `find` so the fingerprint memo above can be filled on demand.
            //
            // The CHEAP test now comes first. A manifest carrying no summary for this bucket
            // could never match (the predicate's first condition is that the routing buckets are
            // equal), so asking that before fingerprinting skips the walk entirely for every
            // manifest that does not cover this bucket.
            let mut matching_manifest = None;
            for (manifest_index, manifest) in manifests.iter().enumerate().rev() {
                let Some(candidates) =
                    manifest_summaries_by_bucket[manifest_index].get(&summary.routing_bucket)
                else {
                    continue;
                };
                if manifest_fingerprints[manifest_index].is_none() {
                    manifest_fingerprints[manifest_index] = Some(
                        crate::engine::decode_index_bytes(&manifest.index_bytes)
                            .ok()
                            .map(|manifest_state| {
                                bucket_generation_fingerprints_by_bucket(&manifest_state)
                            }),
                    );
                }
                // A manifest whose index will not decode matched nothing before and matches
                // nothing now.
                let Some(manifest_bucket_fingerprints) = manifest_fingerprints[manifest_index]
                    .as_ref()
                    .expect("fingerprint memo filled above")
                    .as_ref()
                else {
                    continue;
                };
                if candidates.iter().any(|manifest_summary| {
                    bucket_dump_summary_matches_current_generation(
                        manifest_summary,
                        summary,
                        manifest_bucket_fingerprints,
                        &current_bucket_fingerprints,
                    )
                }) {
                    matching_manifest = Some(manifest);
                    break;
                }
            }
            let Some(manifest) = matching_manifest else {
                // No manifest covers this bucket. It is still allowed to hold the logs only from
                // its own oldest undumped write, PROVIDED it can name that point in both logs.
                //
                // `retain_from_* = frontier + 1`, so a bucket needing everything from `F` onward
                // contributes `F - 1`: records at or below that are reclaimable, `F` and above
                // are kept.
                //
                // BOTH claims are required. They count in different sequences -- one in the
                // write-ahead log, one in the index log -- and a bucket that can place itself in
                // one but not the other has said nothing about the second. A zero in either means
                // NO CLAIM RECORDED, and the only safe reading of "unknown" is the old one:
                // block, and retain everything. Treating unknown as "nothing to retain" is the
                // direction that loses committed records.
                //
                // A CLEAN bucket is the exception, and getting it wrong is what stopped the
                // reclaim entirely. `dirty_object_count == 0` means the bucket holds no undumped
                // write at all, so there is nothing for the log to retain on its behalf and no
                // claim for it to name -- `first_dirty_wal_sequence` is cleared to 0 in the same
                // breath as `dirty = false`, on a DURABLE dump manifest (see
                // `apply_storage_lifecycle`), and a bucket loaded from disk is durable by
                // construction. Reading that 0 as "cannot name its claim" put every clean bucket
                // into `missing_bucket_generations`, which blocks the whole plan.
                //
                // This is the shape #1516 removed from the manifest branch above -- a bucket that
                // needs NOTHING deciding what the log may drop -- surviving in this branch, where
                // it does not merely pin the floor but refuses outright. On an idle shard every
                // bucket ends up here, so the plan reported
                // `slot_generation_without_durable_dump` for all of them and the log could never
                // be reclaimed again: measured on 8,000 records over twelve rounds, 0 of 10,666
                // records freed and `persistent_bytes` flat at 1,344,852 for ever.
                //
                // Counted as covered and contributing no floor, exactly as the manifest branch
                // treats a clean bucket it does have a manifest for.
                if summary.dirty_object_count == 0 {
                    covered_bucket_count = covered_bucket_count.saturating_add(1);
                    continue;
                }
                let (wal_claim, index_log_claim) = bucket_claims
                    .get(&summary.routing_bucket)
                    .copied()
                    .unwrap_or((0, 0));
                if wal_claim > 0 && index_log_claim > 0 {
                    durable_wal_frontier =
                        durable_wal_frontier.min(wal_claim.saturating_sub(1));
                    durable_index_log_frontier =
                        durable_index_log_frontier.min(index_log_claim.saturating_sub(1));
                    covered_bucket_count = covered_bucket_count.saturating_add(1);
                } else {
                    missing_bucket_generations.push(summary.routing_bucket);
                }
                continue;
            };
            retained_manifest_ids.insert(manifest.manifest_id.clone());
            covered_bucket_count = covered_bucket_count.saturating_add(1);
            // EXPERIMENT: a bucket that is CLEAN has no undumped write, so it needs nothing
            // retained on its behalf and must not hold the floor.
            if summary.dirty_object_count > 0 {
                durable_wal_frontier = durable_wal_frontier.min(manifest.wal_sequence);
                durable_index_log_frontier =
                    durable_index_log_frontier.min(manifest.index_log_sequence);
            }
        }

        let mut blocker_reasons = Vec::new();
        if bucket_summaries.is_empty() {
            blocker_reasons.push("no_slot_generations_to_anchor_reclaim".to_string());
            durable_wal_frontier = 0;
            durable_index_log_frontier = 0;
        }
        if !missing_bucket_generations.is_empty() {
            blocker_reasons.push("slot_generation_without_durable_dump".to_string());
        }

        if durable_wal_frontier == u64::MAX {
            // EXPERIMENT 2: no bucket held the floor, which means every bucket is dumped and
            // nothing needs the log retained -- so the floor is the CURRENT position, not zero.
            // Zero here reads as "retain everything" and is what took the plan unsafe in the
            // first experiment.
            //
            // THE INVARIANT THIS LINE HAS TO KEEP, and the one place in the plan that could
            // break it. Everywhere else the frontier is a MINIMUM over bucket dump manifests,
            // and `load_shard_with` raises its own replay point to the LATEST of those same
            // manifests -- a minimum over a set cannot exceed a member of it, so the floor can
            // never climb above the point a load starts replaying from. Here the frontier comes
            // from the current log position instead, which no load path consults. It is safe
            // because a cycle DUMPS (`prepare`) before it RECLAIMS (`reclaim_wal`), so a
            // manifest at this position already exists by the time this is read.
            //
            // What makes that load-bearing rather than incidental: the default load path folds
            // no index-log deltas (#1644), so the expiry round's delta (#1633) can advance the
            // served anchor well past the base index FILE's -- measured 9 against 1 -- and
            // reclaim now drops whole segment files (#1622). A floor above the replay point
            // would free records that a load still has to replay, with nothing else holding
            // them. `wal_reclaim_never_frees_what_the_default_load_path_replays`
            // (engine/tests/expiry_scale.rs) asserts the relation against a real load's
            // recorded watermark at every state a production cycle passes through.
            durable_wal_frontier = current_wal_sequence;
        }
        if durable_index_log_frontier == u64::MAX {
            // EXPERIMENT 2, the index-log half, same reasoning.
            durable_index_log_frontier = current_index_log_sequence;
        }
        let mut follower_cursor_block_count = 0usize;
        for cursor in follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
        {
            if cursor.wal_sequence < durable_wal_frontier
                || cursor.index_log_sequence < durable_index_log_frontier
            {
                follower_cursor_block_count = follower_cursor_block_count.saturating_add(1);
                blocker_reasons.push(format!(
                    "follower_cursor_retains_logs:{}",
                    cursor.follower_id
                ));
            }
        }

        let mut raft_snapshot_block_count = 0usize;
        for snapshot in raft_snapshot_refs
            .iter()
            .filter(|snapshot| snapshot.shard_id == shard_id)
        {
            if snapshot.wal_sequence < durable_wal_frontier
                || snapshot.index_log_sequence < durable_index_log_frontier
            {
                raft_snapshot_block_count = raft_snapshot_block_count.saturating_add(1);
                blocker_reasons.push(format!(
                    "raft_snapshot_retains_logs:{}",
                    snapshot.snapshot_id
                ));
            }
        }
        // Two different questions were being answered by one boolean.
        //
        // WHETHER the frontier can be trusted: every live generation needs a durable dump behind
        // it, or the lowest manifest sequence does not describe what is actually on disk. These
        // stay absolute -- there is no safe partial answer to a frontier that is wrong.
        let generations_durable = missing_bucket_generations.is_empty()
            && covered_bucket_count == bucket_summaries.len()
            && durable_wal_frontier > 0
            && durable_index_log_frontier > 0;

        // HOW FAR it may be followed: a retention cursor marks what some reader has still to
        // consume. Everything at or below the SLOWEST cursor is behind every reader and can go
        // whether or not that cursor ever advances. Refusing at the cursor instead of clamping to
        // it meant one lagging follower pinned the entire log for as long as it lagged, and the
        // log grew without bound underneath it.
        //
        // The floor is a minimum over followers AND snapshot refs together: they are separate
        // lists but the same question, and taking them apart would let one advance past the other
        // and drop a log the slower one still needs.
        let cursor_wal_floor = follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
            .map(|cursor| cursor.wal_sequence)
            .chain(
                raft_snapshot_refs
                    .iter()
                    .filter(|snapshot| snapshot.shard_id == shard_id)
                    .map(|snapshot| snapshot.wal_sequence),
            )
            .min();
        let cursor_index_log_floor = follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
            .map(|cursor| cursor.index_log_sequence)
            .chain(
                raft_snapshot_refs
                    .iter()
                    .filter(|snapshot| snapshot.shard_id == shard_id)
                    .map(|snapshot| snapshot.index_log_sequence),
            )
            .min();

        // Never above the durable frontier, and never above the slowest cursor. With no cursors at
        // all the frontier stands unchanged, which is what it did before.
        let effective_wal_frontier = cursor_wal_floor
            .map_or(durable_wal_frontier, |floor| durable_wal_frontier.min(floor));
        let effective_index_log_frontier = cursor_index_log_floor.map_or(
            durable_index_log_frontier,
            |floor| durable_index_log_frontier.min(floor),
        );

        // A clamp to zero reclaims nothing, which is the right answer for a cursor that has never
        // advanced -- the win here is exactly the span a reader has already consumed, and for a
        // permanently stuck follower that span is empty.
        let safe_to_reclaim =
            generations_durable && effective_wal_frontier > 0 && effective_index_log_frontier > 0;
        let retain_from_wal_sequence = if safe_to_reclaim {
            effective_wal_frontier.saturating_add(1)
        } else {
            0
        };
        let retain_from_index_log_sequence = if safe_to_reclaim {
            effective_index_log_frontier.saturating_add(1)
        } else {
            0
        };

        StorageWalReclaimPlan {
            shard_id,
            safe_to_reclaim,
            durable_bucket_generation_frontier_wal_sequence: durable_wal_frontier,
            durable_bucket_generation_frontier_index_log_sequence: durable_index_log_frontier,
            retain_from_wal_sequence,
            retain_from_index_log_sequence,
            current_wal_sequence,
            current_index_log_sequence,
            covered_bucket_count,
            uncovered_bucket_count: missing_bucket_generations.len(),
            follower_cursor_block_count,
            raft_snapshot_block_count,
            missing_bucket_generations,
            retained_manifest_ids: retained_manifest_ids.into_iter().collect(),
            blocker_reasons,
        }
    }

    pub fn apply_storage_wal_reclaim(
        &self,
        plan: StorageWalReclaimPlan,
    ) -> StorageWalReclaimReport {
        if !plan.safe_to_reclaim {
            return StorageWalReclaimReport {
                plan,
                applied: false,
                ..StorageWalReclaimReport::default()
            };
        }
        // The plan reached `safe_to_reclaim` by finding a durable bucket-dump manifest for
        // every live generation and taking the LOWEST wal sequence among them, so the durable
        // index reflects everything at or below that frontier.
        //
        // This stays the DURABLE frontier, not the cursor-clamped one. The anchor is an upper
        // bound on what may be dropped; `retain_from_wal_sequence` may sit below it because a
        // retention cursor clamped it, and dropping less than the anchor permits is safe. Passing
        // the clamped value here would prove less durability than has actually been established
        // and would be the wrong number for a different reason.
        let durable_index = crate::wal::DurableIndexAnchor::proven_durable_through(
            plan.shard_id,
            plan.durable_bucket_generation_frontier_wal_sequence,
        );
        let wal_gc = self
            .write_ahead_log_store()
            .gc_before_sequence(plan.shard_id, plan.retain_from_wal_sequence, &durable_index)
            .ok();
        StorageWalReclaimReport {
            applied: wal_gc.is_some(),
            wal_records_removed: wal_gc
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            wal_bytes_before: wal_gc
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            wal_bytes_after: wal_gc
                .as_ref()
                .map(|report| report.bytes_after)
                .unwrap_or_default(),
            index_log_records_removed: 0,
            index_log_bytes_before: self.index_log_store.stats(plan.shard_id).bytes_written,
            index_log_bytes_after: self.index_log_store.stats(plan.shard_id).bytes_written,
            // Whole segments dropped by this pass. The gc report has measured them all along;
            // stopping here meant a pass that unlinked 64 files and freed 28 MB reported
            // `wal_records_removed: 0` and byte counts covering only the active segment.
            wal_segments_dropped: wal_gc
                .as_ref()
                .map(|report| report.dropped_segments)
                .unwrap_or_default(),
            wal_segment_bytes_dropped: wal_gc
                .as_ref()
                .map(|report| report.dropped_segment_bytes)
                .unwrap_or_default(),
            plan,
        }
    }

    /// Reclaim the index log on the same terms the background cycle uses.
    ///
    /// [`Self::storage_index_gc_report`] is engine-internal and takes a cycle request for its
    /// thresholds, which is why the periodic scheduler could not reach it: the index log was
    /// truncated only by the cycle endpoint, an explicit `/gc` request, or the embedded proxy's
    /// own reclaim thread. This is the entry that closes that, and it takes the CYCLE's defaults
    /// rather than inventing a second policy -- including
    /// `index_gc_commit_dirty_buckets_before_truncation`, which defaults TRUE and is the safe
    /// order: an index-log record names where a block's bytes live, so dropping one the durable
    /// state does not yet reflect loses the LOCATION of data that is still on disk, which reads
    /// as missing rather than as corruption.
    ///
    /// The plan is recomputed from `request` AFTER the caller's dump has run, which is the order
    /// that makes the safety check meaningful: if that dump cleared the dirty set, the plan now
    /// selects nothing and truncation is safe on its own terms. A stale plan would ask about
    /// buckets that have already been committed.
    pub fn apply_periodic_index_gc(
        &self,
        request: StorageLifecycleRequest,
        lifecycle_report: Option<&StorageLifecycleReport>,
        max_entries_per_round: usize,
    ) -> StorageIndexGcReport {
        let shard_id = request.shard_id;
        // The cursors the CALLER supplied, not an empty pair.
        //
        // This read `Vec::new(), Vec::new()` while holding a request that carries both lists, so a
        // caller that knew about a reader had it silently dropped for the index-log decision --
        // and only for that decision, since the prune below this does honour the same request's
        // cursors. One request, two answers, disagreeing about who is still reading.
        //
        // `retain_from_index_log_sequence` off this plan is what `storage_index_gc_report` hands
        // to `gc_before_sequence_limited`, so the dropped clamp was the difference between keeping
        // an index-log prefix and unlinking it.
        //
        // Cloned before `request` moves into the plan below. `page_gc_raft_snapshot_refs` is where
        // the cycle puts its `raft_snapshot_refs` when it builds this request
        // (`storage_manager_cycle.rs`), so it is the same list under the name this struct uses.
        let follower_replay_cursors = request.follower_replay_cursors.clone();
        let raft_snapshot_refs = request.block_gc_raft_snapshot_refs.clone();
        let plan = self.storage_lifecycle_plan(request);
        let wal_plan =
            self.storage_wal_reclaim_plan(shard_id, follower_replay_cursors, raft_snapshot_refs);
        self.storage_index_gc_report(
            &plan,
            &wal_plan,
            lifecycle_report,
            &StorageManagerCycleRequest {
                shard_id,
                index_gc_max_entries_per_round: max_entries_per_round,
                ..StorageManagerCycleRequest::default()
            },
        )
    }

    pub(super) fn storage_index_gc_report(
        &self,
        plan: &StorageLifecyclePlan,
        wal_plan: &StorageWalReclaimPlan,
        lifecycle_report: Option<&StorageLifecycleReport>,
        request: &StorageManagerCycleRequest,
    ) -> StorageIndexGcReport {
        // THE CHEAP QUESTIONS FIRST.
        //
        // Everything below this point measures the index log, and this report is built on EVERY
        // maintenance round -- every 30 s on the periodic loop -- whether or not the collector can
        // possibly run. With the shipped defaults (`enable_index_gc: true`, a 768 KiB byte
        // threshold) a shard whose index log is under that threshold paid for those measurements
        // every round purely to establish that it was never eligible.
        // `what_the_index_gc_gate_costs` measures that round.
        //
        // The measurement below is no longer a whole-log scan (see `gate_summary`), but this test
        // is still worth keeping in front of it: it is one `read_dir` and a `stat` per piece, and
        // it skips even that piece walk on a log too small to collect.
        //
        // WHY THE CHEAP BYTE TEST IS SOUND, which is the part worth checking rather than
        // assuming. `log_len_bytes` sums the on-disk length of every piece, and `bytes_before`
        // below is now that same sum taken by `gate_summary` -- they are the SAME quantity, so
        // this test and the one further down cannot disagree about a log. It used to be an
        // inequality (the file length against the sum of the record frames a scan handed back,
        // where the file can only ever be the larger), and it was sound then for that reason;
        // it is sound now by equality. Do NOT reintroduce a second way of measuring this log.
        //
        // ONLY TWO CONDITIONS SKIP, deliberately. `dry_run` does NOT: a dry run exists to report
        // what a real round WOULD do, so it still pays for the numbers it was asked for. Nor does
        // an unsafe WAL/index frontier: those counts are the diagnosis for why the frontier is
        // stuck. The two below are the cases where nobody is asking for a number -- the feature is
        // off, or the log is too small to be worth collecting.
        let quick_bytes = self.index_log_store.log_len_bytes(request.shard_id);
        let quick_threshold_missed = request.index_gc_index_log_bytes_threshold != 0
            && quick_bytes < request.index_gc_index_log_bytes_threshold;
        let quick_skip_reason = if !request.enable_index_gc {
            "index GC disabled"
        } else if quick_threshold_missed {
            "index-log byte threshold not reached"
        } else {
            ""
        };
        if !quick_skip_reason.is_empty() {
            return StorageIndexGcReport {
                shard_id: request.shard_id,
                enabled: request.enable_index_gc,
                bytes_threshold: request.index_gc_index_log_bytes_threshold,
                usage_ratio_trigger_basis_points: request
                    .index_gc_usage_ratio_trigger_basis_points,
                max_entries_per_round: request.index_gc_max_entries_per_round,
                retain_from_index_log_sequence: wal_plan.retain_from_index_log_sequence,
                bytes_before: quick_bytes,
                bytes_after: quick_bytes,
                threshold_triggered: !quick_threshold_missed,
                skipped_reason: quick_skip_reason.to_string(),
                ..StorageIndexGcReport::default()
            };
        }

        // THE RATIO, WITHOUT READING THE LOG.
        //
        // This used to be `scan(0, u64::MAX, u64::MAX)` -- the whole log into a vector -- followed
        // by a decode of every record to count the ones below the floor. #1634 made reclaim itself
        // cost what it REMOVES by unlinking whole pieces and deciding from their names, and left
        // this behind: the round still decoded every record to decide whether to call it, so the
        // cost moved from the collector to the gate and the round stayed O(log size).
        //
        // `gate_summary` asks the same question of the same pieces. A sealed piece's name carries
        // `start` and `end`, sequences have no holes, so it holds `end - start` records and
        // `min(end, floor) - start` of them are removable -- the identical arithmetic
        // `drop_covered_index_segments` reports its removals with, which is why the gate and the
        // collector cannot now disagree about a piece. Only the piece being WRITTEN is opened, and
        // it is at most the rolling threshold.
        //
        // `bytes_before` is now the log's on-disk length rather than the sum of the record frames
        // the scan handed back. Those are the same number for an intact log -- the file IS the
        // concatenation of the frames -- and the on-disk length is already what
        // `IndexLogGcReport::bytes_before` reports, so the two halves of a round's report now
        // measure the log the same way instead of two ways that happen to agree.
        let gate = self
            .index_log_store
            .gate_summary(request.shard_id, wal_plan.retain_from_index_log_sequence);
        let records_before = gate.records;
        let bytes_before = gate.bytes;
        let removable_records_before_budget = gate.removable_records;
        let usage_ratio_basis_points = if records_before == 0 {
            0
        } else {
            (removable_records_before_budget as u64).saturating_mul(10_000) / records_before as u64
        };
        let threshold_triggered = request.index_gc_index_log_bytes_threshold == 0
            || bytes_before >= request.index_gc_index_log_bytes_threshold;
        let usage_ratio_triggered = request.index_gc_usage_ratio_trigger_basis_points == 0
            || usage_ratio_basis_points >= request.index_gc_usage_ratio_trigger_basis_points;
        let dirty_buckets_committed_before_truncate = plan.selected_dump_buckets.is_empty()
            || lifecycle_report
                .and_then(|report| report.dump_manifest.as_ref())
                .map(|manifest| !manifest.bucket_ids.is_empty())
                .unwrap_or(false);
        let safe_to_truncate = wal_plan.safe_to_reclaim
            && removable_records_before_budget > 0
            && (!request.index_gc_commit_dirty_buckets_before_truncation
                || dirty_buckets_committed_before_truncate);
        let should_apply = request.enable_index_gc
            && !request.dry_run
            && safe_to_truncate
            && threshold_triggered
            && usage_ratio_triggered;
        let gc = should_apply
            .then(|| {
                self.index_log_store
                    .gc_before_sequence_limited(
                        request.shard_id,
                        wal_plan.retain_from_index_log_sequence,
                        request.index_gc_max_entries_per_round,
                    )
                    .ok()
            })
            .flatten();
        let bytes_after = gc
            .as_ref()
            .map(|report| report.bytes_after)
            .unwrap_or(bytes_before);
        let records_after = gc
            .as_ref()
            .map(|report| report.records_after)
            .unwrap_or(records_before);
        let skipped_reason = if !request.enable_index_gc {
            "index GC disabled"
        } else if request.dry_run {
            "dry_run"
        } else if !wal_plan.safe_to_reclaim {
            "durable WAL/index frontier not safe"
        } else if removable_records_before_budget == 0 {
            "no reclaimable index-log entries"
        } else if request.index_gc_commit_dirty_buckets_before_truncation
            && !dirty_buckets_committed_before_truncate
        {
            "dirty slots not committed before truncation"
        } else if !threshold_triggered {
            "index-log byte threshold not reached"
        } else if !usage_ratio_triggered {
            "index-log usage ratio trigger not reached"
        } else if gc.is_none() {
            "index-log truncation failed"
        } else {
            ""
        }
        .to_string();
        StorageIndexGcReport {
            shard_id: request.shard_id,
            enabled: request.enable_index_gc,
            applied: gc
                .as_ref()
                .map(|report| report.records_removed > 0)
                .unwrap_or(false),
            dirty_buckets_committed_before_truncate,
            bytes_threshold: request.index_gc_index_log_bytes_threshold,
            usage_ratio_trigger_basis_points: request.index_gc_usage_ratio_trigger_basis_points,
            usage_ratio_basis_points,
            max_entries_per_round: request.index_gc_max_entries_per_round,
            retain_from_index_log_sequence: wal_plan.retain_from_index_log_sequence,
            records_before,
            records_after,
            records_removed: gc
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            removable_records_before_budget,
            budget_exhausted: gc
                .as_ref()
                .map(|report| report.budget_exhausted)
                .unwrap_or(false),
            bytes_before,
            bytes_after,
            threshold_triggered,
            usage_ratio_triggered,
            safe_to_truncate,
            skipped_reason,
        }
    }

    /// Pick victims with a bounded sampled scan instead of enumerating every bucket.
    ///
    /// Reads recency and eligibility straight off the bucket index -- the same signals the full
    /// scan derives, but without materializing every live page -- and computes byte totals only
    /// for the buckets actually chosen, which is at most `batch_limit` of them.
    pub(super) fn sampled_eviction_victims(
        &self,
        shard_id: ShardId,
        batch_limit: usize,
        cache_by_bucket: &BTreeMap<u32, crate::StorageCacheBucketSummary>,
    ) -> Vec<StorageEvictionVictim> {
        use super::eviction_sampler::{select_victims, BucketSample, BucketSource, ScanResult};

        /// Bucket-index-backed source. Scanning ranges over the ordered map from the cursor, so
        /// a pass touches only the window it is budgeted for.
        struct IndexSource<'a> {
            buckets: &'a super::state::BucketMap,
            recency: &'a std::collections::HashMap<u32, u64>,
        }

        impl<'a> IndexSource<'a> {
            fn sample(&self, routing_bucket: u32, bucket: &super::state::BucketNode) -> BucketSample {
                BucketSample {
                    routing_bucket,
                    // Evicting a bucket only frees something if it is resident and still holds
                    // live objects, which is the same eligibility the full scan applies via its
                    // weight filter.
                    eligible: bucket.in_memory()
                        && !bucket.deleted()
                        && !bucket.object_index.is_empty(),
                    last_used_ms: self.recency.get(&routing_bucket).copied().unwrap_or(0),
                }
            }
        }

        impl<'a> BucketSource for IndexSource<'a> {
            fn bucket_count(&self) -> usize {
                self.buckets.len()
            }

            fn scan(
                &self,
                cursor: Option<u32>,
                budget: usize,
                visit: &mut dyn FnMut(&BucketSample) -> bool,
            ) -> ScanResult {
                if self.buckets.is_empty() || budget == 0 {
                    return ScanResult::default();
                }
                let mut scanned = 0usize;
                let mut wrapped = false;
                let mut next_cursor = None;
                let mut keep_going = true;

                let start = cursor.unwrap_or(0);
                // Two ranges rather than one, so the scan wraps past the end of the bucket space
                // back to the beginning without materializing the map.
                for pass in 0..2 {
                    let iter: Box<dyn Iterator<Item = (&u32, &super::state::BucketNode)>> =
                        if pass == 0 {
                            Box::new(self.buckets.range(start..))
                        } else {
                            wrapped = true;
                            Box::new(self.buckets.range(..start))
                        };
                    for (routing_bucket, bucket) in iter {
                        if scanned >= budget || !keep_going || scanned >= self.buckets.len() {
                            next_cursor = Some(*routing_bucket);
                            break;
                        }
                        let sample = self.sample(*routing_bucket, bucket);
                        scanned += 1;
                        keep_going = visit(&sample);
                        next_cursor = Some(routing_bucket.saturating_add(1));
                    }
                    if scanned >= budget || !keep_going || scanned >= self.buckets.len() {
                        break;
                    }
                }

                ScanResult {
                    scanned,
                    // Covering the whole store restarts from the top next pass.
                    next_cursor: if scanned >= self.buckets.len() {
                        None
                    } else {
                        next_cursor
                    },
                    wrapped,
                }
            }

            fn lookup(&self, routing_bucket: u32) -> Option<BucketSample> {
                self.buckets
                    .get(&routing_bucket)
                    .map(|bucket| self.sample(routing_bucket, bucket))
            }
        }

        let config = super::evict_sampler_config();
        let mut shards = self.shards.write().expect("shards lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Vec::new();
        };
        // Take the sampler state out so the scan can borrow the bucket index immutably.
        let mut sampler = std::mem::take(&mut shard.evict_sampler);
        let selected = {
            let source = IndexSource {
                buckets: &shard.bucket_index.bucket_map,
                recency: &shard.bucket_recency,
            };
            select_victims(&mut sampler, config, batch_limit, source).victims
        };
        shard.evict_sampler = sampler;

        // Byte totals for the chosen buckets only. Bounded by batch_limit, not by store size.
        selected
            .into_iter()
            .filter_map(|routing_bucket| {
                let bucket = shard.bucket_index.bucket_map.get(&routing_bucket)?;
                let mut logical_bytes = 0u64;
                let mut physical_bytes = 0u64;
                let mut dirty_object_count = 0u64;
                for page in bucket.block_index.values() {
                    if page.deleted {
                        continue;
                    }
                    logical_bytes = logical_bytes.saturating_add(page.address.length());
                    physical_bytes = physical_bytes.saturating_add(page.address.length());
                    if page.dirty {
                        dirty_object_count = dirty_object_count.saturating_add(1);
                    }
                }
                let cache = cache_by_bucket.get(&routing_bucket);
                let cache_memory_bytes = cache.map(|cache| cache.memory_bytes).unwrap_or_default();
                let cache_disk_bytes = cache.map(|cache| cache.disk_bytes).unwrap_or_default();
                Some(StorageEvictionVictim {
                    routing_bucket,
                    object_count: bucket.object_index.len() as u64,
                    logical_bytes,
                    physical_bytes,
                    cache_memory_bytes,
                    cache_disk_bytes,
                    dirty_object_count,
                    weight: cache_memory_bytes
                        .saturating_mul(4)
                        .saturating_add(cache_disk_bytes.saturating_mul(2))
                        .saturating_add(physical_bytes)
                        .saturating_add(dirty_object_count.saturating_mul(1024)),
                    last_touched_ms: shard
                        .bucket_recency
                        .get(&routing_bucket)
                        .copied()
                        .unwrap_or(0),
                })
            })
            .collect()
    }

    pub fn apply_storage_eviction(
        &self,
        shard_id: ShardId,
        memory_pressure_threshold: u64,
        batch_limit: usize,
        dump_before_evict: bool,
        delete_drop: bool,
    ) -> StorageEvictionReport {
        let before_cache = self.storage_cache_inspection_report(shard_id);
        // THE SIGNAL. Every memory number this gate used to read was the CACHE's, and the bucket
        // index is in none of them: at 8,000 records the gate saw ~600 KB of cache while the index
        // held 8,000 entries it could not see, was never evicted, and cost ~760 B a record. A gate
        // that cannot see a cost cannot relieve it, so the resident index is part of the pressure
        // now -- and it is part of it only because the release path above can actually reduce it.
        let bucket_index_bytes_before = self.bucket_index_resident_bytes(shard_id);
        let pressure_before = before_cache
            .stats
            .memory_bytes
            .saturating_add(before_cache.stats.disk_bytes)
            .saturating_add(before_cache.stats.async_writeback_queue_bytes)
            .saturating_add(before_cache.stats.async_writeback_queue_depth)
            .saturating_add(bucket_index_bytes_before);
        if pressure_before < memory_pressure_threshold {
            return StorageEvictionReport {
                shard_id,
                mode: if delete_drop {
                    "delete_drop"
                } else {
                    "evict_cache"
                }
                .to_string(),
                pressure_before,
                pressure_after: pressure_before,
                memory_pressure_threshold,
                batch_limit,
                dump_before_evict,
                skipped_reason: "memory_pressure_below_threshold".to_string(),
                ..StorageEvictionReport::default()
            };
        }
        let cache_by_bucket = before_cache
            .bucket_summaries
            .iter()
            .map(|summary| (summary.routing_bucket, summary.clone()))
            .collect::<BTreeMap<_, _>>();
        let victims = if self
            .evict_sampled_lru
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.sampled_eviction_victims(shard_id, batch_limit, &cache_by_bucket)
        } else {
            // BUILT IN THE ARM THAT READS IT. This clone used to be taken before the branch, and
            // the sampled arm never touched it: a whole-store copy on the way into the one
            // selection path written to avoid whole-store work. `EVICTION_RECENCY_ENTRIES_CLONED`
            // is what says which arm pays it.
            let recency_by_bucket = {
                let shards = self.shards.read().expect("engine lock poisoned");
                let recency = shards
                    .get(&shard_id)
                    .map(|shard| shard.bucket_recency.clone())
                    .unwrap_or_default();
                EVICTION_RECENCY_ENTRIES_CLONED.fetch_add(
                    recency.len() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                recency
            };
            let mut victims = self
                .bucket_storage_summaries(shard_id)
                .into_iter()
                .map(|summary| {
                    let cache = cache_by_bucket.get(&summary.routing_bucket);
                    let cache_memory_bytes =
                        cache.map(|cache| cache.memory_bytes).unwrap_or_default();
                    let cache_disk_bytes = cache.map(|cache| cache.disk_bytes).unwrap_or_default();
                    StorageEvictionVictim {
                        routing_bucket: summary.routing_bucket,
                        object_count: summary.object_count,
                        logical_bytes: summary.logical_bytes,
                        physical_bytes: summary.physical_bytes,
                        cache_memory_bytes,
                        cache_disk_bytes,
                        dirty_object_count: summary.dirty_object_count,
                        weight: cache_memory_bytes
                            .saturating_mul(4)
                            .saturating_add(cache_disk_bytes.saturating_mul(2))
                            .saturating_add(summary.physical_bytes)
                            .saturating_add(summary.dirty_object_count.saturating_mul(1024)),
                        last_touched_ms: recency_by_bucket
                            .get(&summary.routing_bucket)
                            .copied()
                            .unwrap_or(0),
                    }
                })
                .filter(|victim| victim.weight > 0)
                .collect::<Vec<_>>();
            // The LRU policy sorts candidates by last-used time, then evicts
            // least-recently-used buckets first. Never-touched buckets (last_touched_ms ==
            // 0) are coldest and go first; ties fall back to the heavier bucket, then the
            // lower routing_bucket for determinism.
            victims.sort_by(|left, right| {
                left.last_touched_ms
                    .cmp(&right.last_touched_ms)
                    .then_with(|| right.weight.cmp(&left.weight))
                    .then_with(|| left.routing_bucket.cmp(&right.routing_bucket))
            });
            if batch_limit > 0 && victims.len() > batch_limit {
                victims.truncate(batch_limit);
            }
            victims
        };
        let mut dump_manifest_ids = Vec::new();
        if dump_before_evict {
            let dirty_buckets = victims
                .iter()
                .filter(|victim| victim.dirty_object_count > 0)
                .map(|victim| victim.routing_bucket)
                .collect::<Vec<_>>();
            if !dirty_buckets.is_empty() {
                if let Ok(manifest) = self.create_bucket_dump_manifest(shard_id, dirty_buckets) {
                    dump_manifest_ids.push(manifest.manifest_id.clone());
                    // Dumped means dumped. `apply_storage_lifecycle` has always paired the
                    // manifest with this clear; the eviction path created the manifest and left
                    // every bucket marked dirty, so "dump before evict" dumped and then evicted
                    // nothing it had dumped. It also makes the release below possible at all --
                    // a dirty bucket is refused, because the model maps carry no per-page dirty
                    // bit for a reload to restore.
                    self.clear_dumped_bucket_dirty_state(shard_id, &manifest);
                }
            }
        }
        let mut cache_entries_removed = 0usize;
        let mut cache_disk_bytes_removed = 0u64;
        for victim in &victims {
            if let Ok(report) = self.cache.invalidate_slot(shard_id, victim.routing_bucket) {
                cache_entries_removed =
                    cache_entries_removed.saturating_add(report.memory_entries_removed);
                cache_disk_bytes_removed =
                    cache_disk_bytes_removed.saturating_add(report.disk_bytes_removed);
            }
        }
        // THE ACTUATOR. `invalidate_slot` above drops cached pages and leaves every `BucketNode`
        // whole, so the only mode that ever shrank the index was `delete_drop` -- which does it by
        // DESTROYING data. Releasing a victim's page list shrinks the index without losing
        // anything: the node stays routable and the next read loads its pages back from the model
        // maps. Not attempted under `delete_drop`, where the victim's data is about to go.
        let mut release = crate::engine::storage_bucket_internals::BucketReleaseOutcome::default();
        if !delete_drop && !victims.is_empty() {
            let candidates = victims
                .iter()
                .map(|victim| victim.routing_bucket)
                .collect::<Vec<_>>();
            let mut shards = self.shards.write().expect("shards lock poisoned");
            if let Some(shard) = shards.get_mut(&shard_id) {
                release = crate::engine::storage_bucket_internals::release_bucket_blocks(
                    shard,
                    &candidates,
                );
            }
        }
        let mut dropped_object_count = 0usize;
        if delete_drop && !victims.is_empty() {
            let victim_buckets = victims
                .iter()
                .map(|victim| victim.routing_bucket)
                .collect::<BTreeSet<_>>();
            // Encoding the served index and writing it out are the expensive part of this
            // flush -- measured at 42 ms of encode alone for a 2,000-key shard, plus two file
            // writes -- and all of it used to happen while this write lock was held, so every
            // read and write on the shard queued behind it. The lock is only needed for the
            // mutations; a stamped CLONE (9 ms) carries the exact state out, and the encode and
            // the writes happen after the guard is dropped.
            let mut pending_index_flush = None;
            let mut shards = self.shards.write().expect("shards lock poisoned");
            // THE HOLD, by its own clock. Started after the guard is in hand and read after it is
            // dropped, so it spans exactly the interval a serving read would queue behind -- and
            // it is a SEPARATE clock from the phase rows below, which is what makes the residual
            // between them mean something.
            let guard_held = std::time::Instant::now();
            if let Some(shard) = shards.get_mut(&shard_id) {
                let phase = std::time::Instant::now();
                let object_keys = collect_live_block_entries(shard)
                    .into_iter()
                    .filter_map(|entry| {
                        let bucket = entry
                            .address
                            .routing_bucket()
                            .or(entry.filed_bucket())
                            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
                        victim_buckets.contains(&bucket).then_some(entry.object_key)
                    })
                    .collect::<BTreeSet<_>>();
                DELETE_DROP_GUARD_NANOS.add_since(&DELETE_DROP_GUARD_NANOS.collect, phase);
                // The two halves of the drop loop are timed SEPARATELY because they need
                // different things: `delete_record` mutates the shard and cannot leave this
                // guard, while `invalidate_record_all` is handed `&self.cache` and a key and
                // never touches `shard` at all.
                let mut deleted_keys = Vec::new();
                for key in object_keys {
                    let phase = std::time::Instant::now();
                    let removed = delete_record(shard, &key);
                    DELETE_DROP_GUARD_NANOS.add_since(&DELETE_DROP_GUARD_NANOS.delete, phase);
                    if removed {
                        dropped_object_count = dropped_object_count.saturating_add(1);
                        deleted_keys.push(key);
                    }
                }
                // ONE PASS OVER THE CACHE FOR THE WHOLE ROUND, and STILL INSIDE THIS GUARD.
                //
                // This used to be `invalidate_record_all` per dropped key, and each of those
                // made two `MultiLayerCache::invalidate_record` calls, each of which chains the
                // key sets of all three cache tiers and filters -- so the loop above walked the
                // whole cache twice per key. #1907 measured that at 15,996,000 cache-entry visits
                // for one 4,000-key round on a warm store, inside this guard.
                //
                // The swept namespaces do not vary by key, so the walk does not have to be
                // repeated: `invalidate_records_all_batched` lists the shard's cache once and
                // tests each entry against this round's dropped-key set. It drops exactly the
                // same entries -- `one_batched_pass_walks_the_cache_once_instead_of_twice_per_\
                // dropped_key` proves that as a set equality against the per-key arm on a matched
                // fixture.
                //
                // NOT DEFERRED PAST THE GUARD. #1907 priced that and refused it: `cached_response`
                // is cache-first and a record `CacheKey` carries no generation, sequence or
                // version stamp, so a reader in the window is answered with a value the shard has
                // already deleted -- 8 of 533 reads, measured. The pass moved; the guard did not.
                {
                    let phase = std::time::Instant::now();
                    invalidate_records_all_batched(
                        &self.cache,
                        shard_id,
                        &deleted_keys,
                        &CACHE_SWEEP_COUNTS,
                    );
                    DELETE_DROP_GUARD_NANOS.add_since(&DELETE_DROP_GUARD_NANOS.invalidate, phase);
                }
                if dropped_object_count > 0 {
                    // A delete_drop eviction is a LOGICAL delete, so it must follow the same
                    // durability discipline as the expiry sweep (recovery_sweep_compact.rs), which
                    // is the codebase's other logged deletion. Emitting no WAL tombstone here left
                    // the deletion (a) invisible to followers (never replicated), and (b) under
                    // MATRIXARK_BULK_INGEST -- where persist_index_bytes is a no-op -- neither
                    // persisted NOR recoverable from the WAL, so the key resurrected on reload when
                    // replay reapplied the earlier SET. Emit a CommonDelete tombstone per dropped
                    // key (buffered/unfsynced, mirroring the expiry sweep) and anchor
                    // applied_wal_sequence past them so replay observes the deletion instead of the
                    // stale write. (eviction never deletes -- it only dumps and drops from cache -- so
                    // there is no analog; this aligns the Rust-only delete_drop path with the
                    // engine's own tombstone discipline.)
                    if !replaying_wal() {
                        let phase = std::time::Instant::now();
                        // ONE mirror lookup for the whole run -- same reasoning as the expiry
                        // sweep, and this loop is inside the shard-table write guard too.
                        let mirror = self.maintenance_mirror_sink();
                        for key in &deleted_keys {
                            let command = Command::CommonDelete { key: key.clone().to_string() };
                            let appended =
                                self.wal_store
                                    .append_with_sync(shard_id, command.clone(), false);
                            // COUNTED HERE, under the guard, one per dropped key.
                            EVICTION_DELETE_DROP_WAL_APPENDS_UNDER_GUARD
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // Same reasoning as the expiry sweep: a drop that deletes is a
                            // deletion, and it has to reach every log a successor may replay.
                            if appended.is_ok() {
                                if let Some(sink) = mirror.as_ref() {
                                    sink.record_write(shard_id, &command);
                                }
                            }
                        }
                        DELETE_DROP_GUARD_NANOS
                            .add_since(&DELETE_DROP_GUARD_NANOS.wal_append, phase);
                        let phase = std::time::Instant::now();
                        // Anchor off the O(1) CACHED last sequence, not `stats()`. `stats()`
                        // takes a full-file `last_wal_sequence_at` rescan plus a walk of every
                        // sealed piece, and it was taking them HERE -- inside the shard-table
                        // write guard, after this round's own appends, where a serving read waits
                        // on it. The write path stopped doing exactly this (engine.rs, and the
                        // batch path in stream_batch_methods.rs) and reads the cached value under
                        // flat append; the two logged-deletion paths did not follow.
                        //
                        // The value is the same one: the cache is advanced by every append on
                        // this store, the appends above are this round's own, and no other writer
                        // can have appended in between because appending takes this same guard
                        // (engine.rs: "WAL sequence order still equals in-memory apply order
                        // because the reservation + byte-append stay under this same lock").
                        // Without flat append the exact `stats()` value is kept, which is the
                        // same conditional the write path uses.
                        shard.applied_wal_sequence = Some(if self.wal_store.flat_append() {
                            self.wal_store.cached_last_sequence(shard_id)
                        } else {
                            self.wal_store.stats(shard_id).last_sequence
                        });
                        DELETE_DROP_GUARD_NANOS.add_since(&DELETE_DROP_GUARD_NANOS.anchor, phase);
                    }
                    // Stamp here so the clone carries the current on-disk shape, then hand
                    // the snapshot out; the encode and both writes happen below, unlocked.
                    shard.index_format_version = super::SHARD_INDEX_FORMAT_VERSION;
                    let phase = std::time::Instant::now();
                    pending_index_flush = Some(shard.clone());
                    DELETE_DROP_GUARD_NANOS.add_since(&DELETE_DROP_GUARD_NANOS.snapshot, phase);
                }
            }
            drop(shards);
            DELETE_DROP_GUARD_NANOS.add_since(&DELETE_DROP_GUARD_NANOS.total, guard_held);
            if let Some(snapshot) = pending_index_flush {
                let index_bytes = super::serialize_index(&snapshot);
                let _ = self.persist_index_bytes(shard_id, &index_bytes);
                let _ = self.index_log_store.append_index_bytes(shard_id, &index_bytes);
            }
        }
        let after_cache = self.storage_cache_inspection_report(shard_id);
        let bucket_index_bytes_after = self.bucket_index_resident_bytes(shard_id);
        let pressure_after = after_cache
            .stats
            .memory_bytes
            .saturating_add(after_cache.stats.disk_bytes)
            .saturating_add(after_cache.stats.async_writeback_queue_bytes)
            .saturating_add(after_cache.stats.async_writeback_queue_depth)
            .saturating_add(bucket_index_bytes_after);
        StorageEvictionReport {
            shard_id,
            mode: if delete_drop {
                "delete_drop"
            } else {
                "evict_cache"
            }
            .to_string(),
            pressure_before,
            pressure_after,
            memory_pressure_threshold,
            pressure_gate_open: true,
            batch_limit,
            dump_before_evict,
            dump_manifest_ids,
            selected_victims: victims,
            cache_entries_removed,
            cache_disk_bytes_removed,
            dropped_object_count,
            bucket_index_buckets_released: release.released_buckets.len(),
            bucket_index_blocks_released: release.released_blocks,
            bucket_index_release_refused: release.refused_buckets,
            bucket_index_bytes_before,
            bucket_index_bytes_after,
            cooldown: pressure_after >= pressure_before,
            skipped_reason: String::new(),
        }
    }
}

#[cfg(test)]
mod eviction_round_scale {
    use super::EVICTION_RECENCY_ENTRIES_CLONED;
    use crate::engine::collect_live_block_entries;
    use crate::engine::TemporalEngine;
    use crate::{Command, CommandResponse, ExecuteRequest};
    use std::sync::atomic::Ordering;

    fn engine_with(objects: usize) -> (tempfile::TempDir, TemporalEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..objects {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("evict-scale-key-{index}"),
                    value: vec![b'v'; 64],
                },
            });
        }
        (dir, engine)
    }

    /// THE CONTROL, and it runs first as its own test so a failure here cannot stop the claim
    /// below from being made. The exhaustive arm READS the recency map, so it must still copy it:
    /// a counter that reads zero everywhere would satisfy the claim below while measuring nothing.
    ///
    /// The counter is process-wide and this reads a delta around one call, which is why the gate
    /// runs `--test-threads=1`.
    #[test]
    fn the_exhaustive_eviction_arm_still_copies_the_recency_map() {
        let (_dir, engine) = engine_with(400);
        engine.use_full_scan_eviction_for_test();

        EVICTION_RECENCY_ENTRIES_CLONED.store(0, Ordering::Relaxed);
        // Threshold 0 so the pressure gate admits and selection actually runs.
        let report = engine.apply_storage_eviction(1, 0, 4, false, false);
        let copied = EVICTION_RECENCY_ENTRIES_CLONED.load(Ordering::Relaxed);

        assert!(
            report.pressure_gate_open,
            "the round must have got past the pressure gate, or nothing was measured"
        );
        assert!(
            copied > 0,
            "the arm that reads the recency map must still copy it; copied {copied} entries"
        );
    }

    /// A sampled round must not copy the recency map at all.
    ///
    /// The sampler reads recency straight off the shard inside its own bounded scan. The copy the
    /// round used to take before choosing an arm was read only by the exhaustive arm, so under the
    /// shipped default it was one entry per bucket, every round, dropped unread.
    ///
    /// ZERO is asserted rather than "fewer", because there is no reason for this arm to touch a
    /// per-bucket structure at all, and a bound like "less than the bucket count" would pass on a
    /// copy that had merely got smaller.
    #[test]
    fn a_sampled_eviction_round_copies_no_recency_entries() {
        let (_dir, engine) = engine_with(400);
        engine.use_sampled_eviction_for_test();

        EVICTION_RECENCY_ENTRIES_CLONED.store(0, Ordering::Relaxed);
        let report = engine.apply_storage_eviction(1, 0, 4, false, false);
        let copied = EVICTION_RECENCY_ENTRIES_CLONED.load(Ordering::Relaxed);

        assert!(
            report.pressure_gate_open,
            "the round must have got past the pressure gate, or nothing was measured"
        );
        assert!(
            !report.selected_victims.is_empty(),
            "the round must have selected victims, or it did no choosing to measure"
        );
        assert_eq!(
            copied, 0,
            "a sampled round copied {copied} recency entries; the sampler exists so that choosing \
             costs the batch rather than the store"
        );
    }

    /// WHAT ONE EVICTION ROUND COSTS AS THE STORE GROWS, if eviction were turned on.
    ///
    /// Eviction is dark in production -- `enable_evict` defaults to false -- so this is
    /// conditional by construction and says so. It measures allocation CALLS and BYTES across one round at two
    /// corpus sizes and prints the ratio beside the corpus ratio, so a cost that tracks the store
    /// and a cost that does not are told apart by a number rather than by reading the code.
    ///
    /// Allocations rather than a clock: this box sits between load 4 and 30 for hours, and a wall
    /// time taken on it says more about the box than about the round.
    ///
    /// Only compiled with `alloc-probe`: without the counting allocator every count reads zero,
    /// and a ratio of zeroes would print a reassuring 1.00x while measuring nothing. The canary
    /// asserts the allocator is installed before any number is believed.
    #[cfg(feature = "alloc-probe")]
    #[test]
    fn what_a_sampled_eviction_round_costs_as_the_store_grows() {
        let canary = crate::alloc_probe::Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "counting allocator not installed despite the feature being on"
        );
        drop(sink);

        fn round_cost(
            objects: usize,
        ) -> (u64, u64, u64, u64, usize, usize, u64, u64, u64, u64) {
            let (_dir, engine) = engine_with(objects);
            engine.use_sampled_eviction_for_test();
            let buckets = engine.bucket_storage_summaries(1).len();

            // A round that is BELOW the threshold and returns early. This is the ordinary shape
            // of a loop that is running and finding nothing to do.
            let idle = crate::alloc_probe::Probe::start();
            let skipped = engine.apply_storage_eviction(1, u64::MAX, 4, false, false);
            let idle = idle.stop();
            assert!(
                !skipped.skipped_reason.is_empty(),
                "the idle arm must actually have been skipped"
            );

            crate::engine::reset_live_block_scan_entries();
            let probe = crate::alloc_probe::Probe::start();
            let report = engine.apply_storage_eviction(1, 0, 4, false, false);
            let counts = probe.stop();
            let scanned = crate::engine::live_block_scan_entries();
            assert!(
                report.pressure_gate_open,
                "the working arm must have got past the pressure gate"
            );

            // ATTRIBUTION. Each phase probed on its own, and a RESIDUAL row so the table has to
            // add up: a phase that is linear and not named here shows as a growing residual
            // rather than as nothing.
            let inspect = crate::alloc_probe::Probe::start();
            let cache_report = engine.storage_cache_inspection_report(1);
            let inspect = inspect.stop().allocs;

            let resident = crate::alloc_probe::Probe::start();
            let _ = engine.bucket_index_resident_bytes(1);
            let resident = resident.stop().allocs;

            let map_build = crate::alloc_probe::Probe::start();
            let cache_by_bucket = cache_report
                .bucket_summaries
                .iter()
                .map(|summary| (summary.routing_bucket, summary.clone()))
                .collect::<std::collections::BTreeMap<_, _>>();
            let map_build = map_build.stop().allocs;

            let select = crate::alloc_probe::Probe::start();
            let picked = engine.sampled_eviction_victims(1, 4, &cache_by_bucket);
            let select = select.stop().allocs;
            assert!(
                !picked.is_empty(),
                "the selection phase must still pick victims when probed on its own"
            );

            (
                counts.allocs,
                counts.alloc_bytes,
                idle.allocs,
                scanned,
                buckets,
                report.selected_victims.len(),
                inspect,
                resident,
                map_build,
                select,
            )
        }

        // The two ACTUATOR phases, on a fresh store each time so neither has already run.
        // The release's own outcome comes back with the cost, and the two sizes are asserted to
        // agree on it below: an actuator that released four buckets and one that refused four
        // both cost something, and a table that did not say which would report the second as
        // progress.
        fn actuator_cost(
            objects: usize,
        ) -> (
            u64,
            u64,
            usize,
            usize,
            usize,
            crate::engine::storage_bucket_internals::BucketReleaseRefusals,
        ) {
            let (_dir, engine) = engine_with(objects);
            engine.use_sampled_eviction_for_test();
            let cache_report = engine.storage_cache_inspection_report(1);
            let cache_by_bucket = cache_report
                .bucket_summaries
                .iter()
                .map(|summary| (summary.routing_bucket, summary.clone()))
                .collect::<std::collections::BTreeMap<_, _>>();
            let victims = engine.sampled_eviction_victims(1, 4, &cache_by_bucket);
            let candidates = victims
                .iter()
                .map(|victim| victim.routing_bucket)
                .collect::<Vec<_>>();
            assert!(!candidates.is_empty(), "nothing to actuate on");

            let invalidate = crate::alloc_probe::Probe::start();
            for routing_bucket in &candidates {
                let _ = engine.cache.invalidate_slot(1, *routing_bucket);
            }
            let invalidate = invalidate.stop().allocs;

            let mut outcome = None;
            let release = crate::alloc_probe::Probe::start();
            {
                let mut shards = engine.shards.write().expect("shards lock poisoned");
                if let Some(shard) = shards.get_mut(&1) {
                    outcome = Some(crate::engine::storage_bucket_internals::release_bucket_blocks(
                        shard,
                        &candidates,
                    ));
                }
            }
            let release = release.stop().allocs;
            let outcome = outcome.expect("the shard must be present to release anything");
            assert_eq!(
                outcome.released_buckets.len() + outcome.refused_buckets,
                candidates.len(),
                "every candidate must have been acted on, or the probe timed a release that \
                 returned early: {outcome:?}",
            );
            (
                invalidate,
                release,
                candidates.len(),
                outcome.released_buckets.len(),
                outcome.refused_buckets,
                outcome.refusals.clone(),
            )
        }

        const SMALL: usize = 500;
        const LARGE: usize = 4000;
        let (
            small_allocs,
            small_bytes,
            small_idle,
            small_scanned,
            small_buckets,
            small_victims,
            small_inspect,
            small_resident,
            small_map,
            small_select,
        ) = round_cost(SMALL);
        let (
            large_allocs,
            large_bytes,
            large_idle,
            large_scanned,
            large_buckets,
            large_victims,
            large_inspect,
            large_resident,
            large_map,
            large_select,
        ) = round_cost(LARGE);
        let (
            small_invalidate,
            small_release,
            small_candidates,
            small_released,
            small_refused,
            small_refusals,
        ) = actuator_cost(SMALL);
        let (
            large_invalidate,
            large_release,
            large_candidates,
            large_released,
            large_refused,
            large_refusals,
        ) = actuator_cost(LARGE);
        assert_eq!(
            small_candidates, large_candidates,
            "both actuator probes must act on the same number of victims"
        );
        // WHICH TERM, named rather than inferred. Eleven terms share `refused_buckets`, so a
        // table saying only that four candidates were refused says nothing about why.
        println!("  release outcome, {SMALL} objects: {small_refusals:?}");
        println!("  release outcome, {LARGE} objects: {large_refusals:?}");
        assert_eq!(
            small_refusals, large_refusals,
            "the two release probes refused on different terms, so their costs are not comparable",
        );
        assert_eq!(
            (small_released, small_refused),
            (large_released, large_refused),
            "the two release probes did different things -- {small_released} released and \
             {small_refused} refused at {SMALL} objects against {large_released} and \
             {large_refused} at {LARGE} -- so their costs are not comparable",
        );
        let small_named = small_inspect
            + small_resident
            + small_map
            + small_select
            + small_invalidate
            + small_release;
        let large_named = large_inspect
            + large_resident
            + large_map
            + large_select
            + large_invalidate
            + large_release;
        let small_residual = small_allocs as i64 - small_named as i64;
        let large_residual = large_allocs as i64 - large_named as i64;

        let ratio = |small: u64, large: u64| {
            if small == 0 {
                0.0
            } else {
                large as f64 / small as f64
            }
        };
        println!(
            "\n  ONE SAMPLED EVICTION ROUND, at two corpus sizes (eviction defaults OFF; this is what \
             it WOULD cost)\n\
             \n                                {SMALL:>10} objects {LARGE:>10} objects      ratio\n\
               buckets                     {small_buckets:>10} {large_buckets:>18}   {:>8.2}x\n\
               victims chosen              {small_victims:>10} {large_victims:>18}\n\
               live-page entries scanned   {small_scanned:>10} {large_scanned:>18}   {:>8.2}x\n\
               allocations, working round  {small_allocs:>10} {large_allocs:>18}   {:>8.2}x\n\
               alloc bytes, working round  {small_bytes:>10} {large_bytes:>18}   {:>8.2}x\n\
               allocations, IDLE round     {small_idle:>10} {large_idle:>18}   {:>8.2}x\n\
             \n  where the working round's allocations go\n\
               cache inspection report     {small_inspect:>10} {large_inspect:>18}   {:>8.2}x\n\
               bucket index resident bytes {small_resident:>10} {large_resident:>18}   {:>8.2}x\n\
               cache_by_bucket map build   {small_map:>10} {large_map:>18}   {:>8.2}x\n\
               sampled victim selection    {small_select:>10} {large_select:>18}   {:>8.2}x\n\
               cache invalidate, 4 victims {small_invalidate:>10} {large_invalidate:>18}   {:>8.2}x\n\
               bucket release, 4 victims   {small_release:>10} {large_release:>18}   {:>8.2}x\n\
               (of {small_candidates} candidates: {small_released} released, {small_refused} refused, at both sizes)\n\
               RESIDUAL (unattributed)     {small_residual:>10} {large_residual:>18}\n",
            ratio(small_buckets as u64, large_buckets as u64),
            ratio(small_scanned, large_scanned),
            ratio(small_allocs, large_allocs),
            ratio(small_bytes, large_bytes),
            ratio(small_idle, large_idle),
            ratio(small_inspect, large_inspect),
            ratio(small_resident, large_resident),
            ratio(small_map, large_map),
            ratio(small_select, large_select),
            ratio(small_invalidate, large_invalidate),
            ratio(small_release, large_release),
        );

        // VACUITY FLOOR. Two corpus sizes that produced the same number of buckets would make
        // every ratio above a comparison of a store with itself.
        assert!(
            large_buckets > small_buckets,
            "the two corpora must differ in bucket count, got {small_buckets} and {large_buckets}"
        );
        assert!(
            small_allocs > 0 && large_allocs > 0,
            "a round that allocates nothing at either size means the probe measured nothing"
        );
    }

    /// WHAT A `delete_drop` ROUND APPENDS TO THE WAL WHILE HOLDING THE SHARD WRITE GUARD.
    ///
    /// Counted, not argued, and not inferred from the shape of the loop: this engine has had two
    /// "N under a guard" findings settled by a counter after being mis-read by argument, and the
    /// comment beside the loop acknowledging the hold is not a measurement of how many.
    ///
    /// `delete_drop` is OFF by default in both places it can be set. It is not unreachable: both
    /// are `#[serde(default)]` fields on deserialized structs -- `StorageManagerRequest`
    /// (`engine/reports.rs`), which the on-demand cycle takes from a caller, and the data node's
    /// own options (`data_node.rs`), which a deployment configures. Either can turn it on with no
    /// code change, so what it costs when on is worth a number.
    ///
    /// `batch_limit` 0 is "no limit", documented as such on the request and read that way by both
    /// selection arms since it was corrected. A round at that setting takes every bucket, so the
    /// loop runs once per key in the shard -- which is what makes this a term that tracks the
    /// store rather than the batch.
    ///
    /// NOT FIXED, AND NOW WITH A REASON RATHER THAN AN ABSENCE OF ONE. The open question was
    /// whether the appends could move out of the guard, which needs an ordering argument about
    /// `applied_wal_sequence`. The argument does not hold, and this is where it is written down.
    ///
    /// WHAT THE SEQUENCE GUARANTEES. Replay keeps records whose `sequence` is strictly above the
    /// durable anchor and then SORTS them by sequence (`engine/lifecycle.rs`), so a key's final
    /// state is decided by the highest-sequence record naming it. WHERE IT IS ASSIGNED: inside
    /// `append_with_sync_inner`, under the log's own mutex, at the moment the bytes are written --
    /// `seq = last_sequence + 1`. There is no way to take a sequence now and write its bytes
    /// later: `append_for_group_commit` reserves a sequence and writes the bytes in the SAME
    /// critical section, deferring only the fsync. WHERE IT BECOMES DURABLE: later than either --
    /// these tombstones append with `sync` false, so the bytes are in the page cache and the
    /// deletion becomes durable when a subsequent barrier reaches them, or when the index
    /// snapshot written below lands carrying an anchor above them.
    ///
    /// WHAT AN INTERLEAVED RE-WRITE DOES. Today it cannot interleave. `delete_record` and this
    /// append are in one `shards.write()` section, and an ordinary write takes that same guard to
    /// apply AND to append -- the engine states the invariant where it moved the fsync out and
    /// deliberately left the byte-append in ("WAL sequence order still equals in-memory apply
    /// order because the reservation + byte-append stay under this same lock", `engine.rs`). So a
    /// re-write of a dropped key is wholly before the drop (its SET sorts below the tombstone;
    /// the key stays deleted, which is right) or wholly after (its SET sorts above; the key comes
    /// back with the new value, which is also right).
    ///
    /// Move the appends out and that stops being true. The in-memory delete happens under the
    /// guard, the guard drops, a writer takes it and re-writes the key at sequence S_w, and the
    /// deferred tombstone then appends at S_d > S_w. Replay sorts and applies the SET and then the
    /// DELETE: a write that was acked after the drop is silently gone. Assigning the tombstone a
    /// lower sequence under the guard does not rescue it -- a sequence is assigned at the
    /// byte-append, and the log recovers its next sequence by scanning the file
    /// (`last_wal_sequence_at`) and clamps reclaim to keep the highest-sequence record, both of
    /// which read the file as monotonic in sequence.
    ///
    /// So the append IS the ordering, and the one order-independent thing (the fsync) is already
    /// out -- these records take none. What remains available is BATCHING, which keeps every
    /// record under the guard and takes one sequence for the round rather than N; that needs a
    /// multi-key delete command, and there is none, so it is a replicated wire-format change and
    /// not a tidy-up. Priced before being declined: with the loop moved out anyway, as a mutant,
    /// the hold fell from 769 ms to 540 ms at 4,000 keys -- 29.8%.
    ///
    /// And the loop is not where the hold mostly goes:
    /// `where_a_delete_drop_rounds_shard_guard_hold_actually_goes` splits it, and the largest
    /// single term is `invalidate_record_all`, which takes `&MultiLayerCache` and a key and
    /// touches no shard state at all.
    #[test]
    fn what_a_delete_drop_round_appends_to_the_wal_under_the_shard_write_guard() {
        use super::EVICTION_DELETE_DROP_WAL_APPENDS_UNDER_GUARD as APPENDS;

        /// `(appends under the guard, objects written, keys the round dropped)`
        fn round(objects: usize) -> (u64, usize, usize) {
            let (_dir, engine) = engine_with(objects);
            engine.use_sampled_eviction_for_test();
            APPENDS.store(0, Ordering::Relaxed);
            // threshold 0 so the gate admits; batch_limit 0 is "no limit", so every bucket is a
            // victim and the drop loop covers the shard.
            let report = engine.apply_storage_eviction(1, 0, 0, false, true);
            let appends = APPENDS.load(Ordering::Relaxed);
            assert!(
                report.pressure_gate_open,
                "the round must have got past the pressure gate, or nothing was measured"
            );
            (appends, objects, report.dropped_object_count)
        }

        const SMALL: usize = 500;
        const LARGE: usize = 4000;
        let (small_appends, small_objects, small_dropped) = round(SMALL);
        let (large_appends, large_objects, large_dropped) = round(LARGE);
        let ratio = if small_appends == 0 {
            0.0
        } else {
            large_appends as f64 / small_appends as f64
        };
        println!(
            "\n  A delete_drop EVICTION ROUND, WAL records appended INSIDE the shard write guard\n\
             \n                                {SMALL:>10} objects {LARGE:>10} objects      ratio\n\
               objects written             {small_objects:>10} {large_objects:>18}\n\
               keys the round dropped      {small_dropped:>10} {large_dropped:>18}\n\
               WAL appends UNDER the guard {small_appends:>10} {large_appends:>18}   {ratio:>8.2}x\n",
        );

        // VACUITY FLOOR, on the measured denominator: a round that dropped nothing appends
        // nothing, and nothing over nothing is a flat that means the probe never ran.
        assert!(
            small_dropped > 0 && large_dropped > 0,
            "the round must have dropped keys at both sizes to have appended anything; \
             dropped {small_dropped} and {large_dropped}",
        );
        assert!(
            large_dropped > small_dropped,
            "the two corpora must differ in dropped keys, got {small_dropped} and {large_dropped}",
        );
        // ONE RECORD PER DROPPED KEY, asserted as equality rather than as a bound: the claim is
        // that this loop appends per key, and "at least one" would also pass on a single record.
        assert_eq!(
            small_appends, small_dropped as u64,
            "a delete_drop round appended {small_appends} WAL records under the guard for \
             {small_dropped} dropped keys",
        );
        assert_eq!(
            large_appends, large_dropped as u64,
            "a delete_drop round appended {large_appends} WAL records under the guard for \
             {large_dropped} dropped keys",
        );
    }

    /// WHERE THE HOLD ACTUALLY GOES -- the in-order account of one `delete_drop` round.
    ///
    /// The probe above says the tombstone loop runs once per dropped key. That number on its own
    /// argues for nothing: a loop that is 3% of the hold is worth 3%, and moving it costs an
    /// ordering argument about `applied_wal_sequence` that is not free to make. This splits the
    /// hold so the question can be settled before it is argued.
    ///
    /// WHAT IS ASSERTED AND WHAT IS ONLY PRINTED. The nanosecond rows are PRINTED. They are a
    /// ratio of two clocks taken on one thread, which is the only form in which a timing on this
    /// box means anything, and an assertion on them would be an assertion about the box's load.
    /// What is ASSERTED is counts -- the WAL's own `writes`, `bytes_written`, `syncs`,
    /// `append_full_scans` and `stats_full_scans`, read either side of the WHOLE call.
    ///
    /// IS THE PATH REACHED? Measured, not read. The note beside the probe above argues from two
    /// `#[serde(default)]` fields that `delete_drop` is configurable; that is an argument about
    /// what COULD happen. A counter at the `apply_storage_eviction` entry point, run over the
    /// whole `--lib` suite single-threaded, says what DOES: 88 eviction rounds, 7 of them with
    /// `delete_drop` true. Not zero, so the cost below is paid by something that runs.
    ///
    /// THE RESIDUAL IS INDEPENDENT. `wal_writes` is maintained inside `append_record_locked` --
    /// the primitive that writes the bytes -- and is read here from OUTSIDE
    /// `apply_storage_eviction`, so `wal_writes - appends` is records this round put in the log
    /// through some path the tombstone counter does not see. It is asserted EQUAL ACROSS THE TWO
    /// CORPUS SIZES rather than against a constant: equal means fixed per-round overhead, and a
    /// residual that grew with the corpus would be a per-key path in no row, which is exactly the
    /// drift a constant would have hidden.
    #[test]
    fn where_a_delete_drop_rounds_shard_guard_hold_actually_goes() {
        use super::DELETE_DROP_GUARD_NANOS as NANOS;
        use super::EVICTION_DELETE_DROP_WAL_APPENDS_UNDER_GUARD as APPENDS;

        #[derive(Debug)]
        struct Round {
            dropped: usize,
            appends: u64,
            wal_writes: u64,
            wal_bytes: u64,
            wal_syncs: u64,
            append_scans: u64,
            stats_scans: u64,
            hold_ns: u64,
            collect_ns: u64,
            delete_ns: u64,
            invalidate_ns: u64,
            wal_ns: u64,
            anchor_ns: u64,
            snapshot_ns: u64,
            unattributed_ns: u64,
        }

        impl Round {
            /// Records this round appended that the tombstone counter did not see.
            fn wal_residual(&self) -> u64 {
                self.wal_writes.saturating_sub(self.appends)
            }
            fn pct(&self, part: u64) -> f64 {
                if self.hold_ns == 0 {
                    0.0
                } else {
                    part as f64 * 100.0 / self.hold_ns as f64
                }
            }
        }

        fn round(objects: usize) -> Round {
            let (_dir, engine) = engine_with(objects);
            engine.use_sampled_eviction_for_test();
            APPENDS.store(0, Ordering::Relaxed);
            NANOS.reset();
            // OUTER counters, taken from the WAL's own primitive either side of the whole call.
            // `raw_stats` is the non-scanning read, so taking it does not itself move
            // `stats_full_scans` -- a probe whose apparatus perturbed the quantity it reads would
            // be measuring itself.
            let before = engine.wal_store.raw_stats(1);
            let report = engine.apply_storage_eviction(1, 0, 0, false, true);
            let after = engine.wal_store.raw_stats(1);
            assert!(
                report.pressure_gate_open,
                "the round must have got past the pressure gate, or nothing was measured"
            );
            let (hold_ns, collect_ns, delete_ns, invalidate_ns, wal_ns, anchor_ns, snapshot_ns) =
                NANOS.read();
            Round {
                dropped: report.dropped_object_count,
                appends: APPENDS.load(Ordering::Relaxed),
                wal_writes: after.writes.saturating_sub(before.writes),
                wal_bytes: after.bytes_written.saturating_sub(before.bytes_written),
                wal_syncs: after.syncs.saturating_sub(before.syncs),
                append_scans: after
                    .append_full_scans
                    .saturating_sub(before.append_full_scans),
                stats_scans: after.stats_full_scans.saturating_sub(before.stats_full_scans),
                hold_ns,
                collect_ns,
                delete_ns,
                invalidate_ns,
                wal_ns,
                anchor_ns,
                snapshot_ns,
                unattributed_ns: NANOS.unattributed(),
            }
        }

        const SMALL: usize = 500;
        const LARGE: usize = 4000;
        let small = round(SMALL);
        let large = round(LARGE);

        let ratio = |s: u64, l: u64| if s == 0 { 0.0 } else { l as f64 / s as f64 };
        let per_key = |value: u64, dropped: usize| {
            if dropped == 0 {
                0.0
            } else {
                value as f64 / dropped as f64
            }
        };
        println!(
            "\n  ONE delete_drop ROUND, IN ORDER -- counts either side of the whole call, and the\n  \
               shard-table write guard's hold split by phase\n\
             \n                                    {SMALL:>10} objects {LARGE:>10} objects      ratio\n\
               keys the round dropped          {:>10} {:>18}   {:>8.2}x\n\
               WAL appends UNDER the guard     {:>10} {:>18}   {:>8.2}x\n\
               WAL records written (OUTER)     {:>10} {:>18}   {:>8.2}x\n\
               .. residual, outer minus rows   {:>10} {:>18}\n\
               WAL bytes written               {:>10} {:>18}   {:>8.2}x\n\
               .. bytes per dropped key        {:>10.1} {:>18.1}\n\
               fsync / fdatasync barriers      {:>10} {:>18}\n\
               append-path full log rescans    {:>10} {:>18}\n\
               stats() full log rescans        {:>10} {:>18}\n\
             \n  THE HOLD, SPLIT (printed, not asserted -- a time on this box is a fact about the box)\n\
             \n                                    {SMALL:>10} objects {LARGE:>10} objects\n\
               guard held, total us            {:>10.0} {:>18.0}\n\
               collect live block entries      {:>9.1}% {:>17.1}%\n\
               delete_record (NEEDS the guard) {:>9.1}% {:>17.1}%\n\
               invalidate_record_all (cache)   {:>9.1}% {:>17.1}%\n\
               WAL tombstone loop              {:>9.1}% {:>17.1}%\n\
               anchor applied_wal_sequence     {:>9.1}% {:>17.1}%\n\
               shard.clone() snapshot          {:>9.1}% {:>17.1}%\n\
               unattributed (independent)      {:>9.1}% {:>17.1}%\n",
            small.dropped,
            large.dropped,
            ratio(small.dropped as u64, large.dropped as u64),
            small.appends,
            large.appends,
            ratio(small.appends, large.appends),
            small.wal_writes,
            large.wal_writes,
            ratio(small.wal_writes, large.wal_writes),
            small.wal_residual(),
            large.wal_residual(),
            small.wal_bytes,
            large.wal_bytes,
            ratio(small.wal_bytes, large.wal_bytes),
            per_key(small.wal_bytes, small.dropped),
            per_key(large.wal_bytes, large.dropped),
            small.wal_syncs,
            large.wal_syncs,
            small.append_scans,
            large.append_scans,
            small.stats_scans,
            large.stats_scans,
            small.hold_ns as f64 / 1000.0,
            large.hold_ns as f64 / 1000.0,
            small.pct(small.collect_ns),
            large.pct(large.collect_ns),
            small.pct(small.delete_ns),
            large.pct(large.delete_ns),
            small.pct(small.invalidate_ns),
            large.pct(large.invalidate_ns),
            small.pct(small.wal_ns),
            large.pct(large.wal_ns),
            small.pct(small.anchor_ns),
            large.pct(large.anchor_ns),
            small.pct(small.snapshot_ns),
            large.pct(large.snapshot_ns),
            small.pct(small.unattributed_ns),
            large.pct(large.unattributed_ns),
        );

        // VACUITY FLOOR, on the measured denominator. A round that dropped nothing appended
        // nothing and held the guard for a branch it did not take.
        assert!(
            small.dropped > 0 && large.dropped > 0,
            "the round must have dropped keys at both sizes; dropped {} and {}",
            small.dropped,
            large.dropped,
        );
        assert!(
            large.dropped > small.dropped,
            "the two corpora must differ in dropped keys, got {} and {}",
            small.dropped,
            large.dropped,
        );
        assert!(
            small.hold_ns > 0 && large.hold_ns > 0,
            "a hold of zero at either size means the phase clocks never ran: {small:?} {large:?}",
        );

        // THE APPARATUS HAS TO ACCOUNT FOR THE HOLD IT SPLITS. Each row is a sub-interval of the
        // hold measured on the same thread, so the rows summing to nearly all of it is a
        // STRUCTURAL fact, not a timing one: the box's load stretches the rows and the total
        // together and cancels out of the ratio. Ten percent is twenty times the half-percent
        // measured, and a row that stopped accumulating would take its whole share into the
        // residual -- which is how a split table quietly stops splitting anything.
        for (label, round) in [("500", &small), ("4000", &large)] {
            assert!(
                round.unattributed_ns.saturating_mul(10) <= round.hold_ns,
                "the phase rows must account for the hold they split; at {label} objects \
                 {} ns of {} ns landed in no row",
                round.unattributed_ns,
                round.hold_ns,
            );
        }

        // THE INDEPENDENT RESIDUAL, asserted ACROSS THE SIZES rather than against a constant.
        // Equal at both sizes means the records this round appends beyond the tombstone loop are
        // fixed per-round overhead. Unequal means a per-key WAL path exists that the tombstone
        // counter does not see -- which is precisely the drift an assertion against a constant
        // would have let through.
        assert_eq!(
            small.wal_residual(),
            large.wal_residual(),
            "WAL records this round appended outside the counted tombstone loop must be fixed \
             per-round overhead, not per-key: {} at {SMALL} objects and {} at {LARGE}",
            small.wal_residual(),
            large.wal_residual(),
        );

        // The tombstones are appended with sync=false, so the round takes NO durable barrier of
        // its own. Stated as an equality at both sizes: "few" would also pass on a barrier per
        // key, which is the shape this is here to rule out.
        assert_eq!(
            (small.wal_syncs, large.wal_syncs),
            (0, 0),
            "a delete_drop round's tombstones are unsynced by construction, so the round must \
             take no fsync of its own",
        );

        // BYTES PER DROPPED KEY, identical at both sizes. The record is one key's tombstone and
        // nothing that tracks the store, so the per-key figure is the whole of what the WAL grows
        // by -- and a per-key figure that moved between the sizes would say the record carries
        // something it should not.
        let small_bytes_per_key = per_key(small.wal_bytes, small.dropped);
        let large_bytes_per_key = per_key(large.wal_bytes, large.dropped);
        assert!(
            (small_bytes_per_key - large_bytes_per_key).abs() < 1.0,
            "a tombstone's size must not track the store: {small_bytes_per_key:.1} B/key at \
             {SMALL} objects and {large_bytes_per_key:.1} B/key at {LARGE}",
        );

        // THE ANCHOR TAKES NO FULL-LOG RESCAN UNDER THE GUARD. It used to: the round anchored
        // through `stats()`, which walks every sealed piece and then rescans the active log
        // end-to-end, and it did that inside this write guard after its own appends. Under flat
        // append the anchor now reads the O(1) cached sequence, exactly as the write path does.
        //
        // Asserted at BOTH sizes, because the cost it replaced was a fixed one -- about 6 ms a
        // round here -- and a bound of "few" would pass on a scan that came back.
        assert_eq!(
            (small.stats_scans, large.stats_scans),
            (0, 0),
            "the round must anchor without a full-log rescan under the guard",
        );
    }

    /// THE CONTROL for the anchor above, and it is a control in the strict sense: it drives the
    /// SAME round through the SAME probe and differs only in the one condition the narrowing is
    /// allowed to depend on.
    ///
    /// Without flat append the log's cached sequence is not authoritative, so the anchor must
    /// still take the exact `stats()` value -- rescan and all. A narrowing that dropped the scan
    /// unconditionally would satisfy the assertion above while being wrong here, and a probe that
    /// only ever ran the flat arm could not tell the two apart.
    #[test]
    fn without_flat_append_the_delete_drop_anchor_still_takes_the_exact_sequence() {
        let (_dir, engine) = engine_with(200);
        engine.use_sampled_eviction_for_test();
        engine.wal_store.rescan_on_every_append_for_test();

        let before = engine.wal_store.raw_stats(1);
        let report = engine.apply_storage_eviction(1, 0, 0, false, true);
        let after = engine.wal_store.raw_stats(1);
        let stats_scans = after.stats_full_scans.saturating_sub(before.stats_full_scans);

        assert!(
            report.pressure_gate_open && report.dropped_object_count > 0,
            "the control must have dropped keys, or it constrains nothing: {report:?}",
        );
        assert_eq!(
            stats_scans, 1,
            "with the log's length cache refused, the anchor must fall back to the exact \
             scanning stats() read; took {stats_scans} scans for {} dropped keys",
            report.dropped_object_count,
        );
        // And the anchor must actually name this round's own tombstones, in either arm. A
        // narrowing that reads a cheaper number is only equivalent if it is the SAME number.
        let shards = engine.shards.read().expect("engine lock poisoned");
        let anchored = shards
            .get(&1)
            .and_then(|shard| shard.applied_wal_sequence)
            .expect("the round must have anchored");
        drop(shards);
        assert_eq!(
            anchored,
            engine.wal_store.stats(1).last_sequence,
            "the anchor must equal the log's own last sequence",
        );
    }

    /// THE SECOND COPY. `delete_drop` is not the only logged deletion: the expiry sweep in
    /// `recovery_sweep_compact.rs` writes the same per-key `CommonDelete` tombstones under the
    /// same write guard and anchored through the same scanning `stats()` read. A guard that
    /// covered only the round above would leave the sweep holding the cost it was written to
    /// remove, which is how one of two live copies keeps a defect.
    #[test]
    fn the_expiry_sweeps_anchor_takes_no_full_log_rescan_either() {
        let (_dir, engine) = engine_with(400);
        // Expire everything, so the sweep has keys to tombstone.
        for index in 0..400 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: format!("evict-scale-key-{index}"),
                    ttl_ms: 1,
                },
            });
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        // The SWEEP itself, not the storage-manager cycle that wraps it. A cycle runs eight other
        // stages that each read `stats()` for their own reasons, and measuring the sweep through
        // one would put this claim's denominator in other stages' hands.
        let before = engine.wal_store.raw_stats(1);
        let report = engine
            .sweep_expired_records(1)
            .expect("the sweep must run on a loaded shard");
        let after = engine.wal_store.raw_stats(1);
        let stats_scans = after.stats_full_scans.saturating_sub(before.stats_full_scans);
        let expired = report.expired_records_removed;

        println!(
            "\n  THE EXPIRY SWEEP, the other logged deletion\n    \
               keys the sweep expired        {expired:>8}\n    \
               stats() full log rescans      {stats_scans:>8}\n",
        );

        // VACUITY FLOOR. A sweep that expired nothing takes no anchor at all, and a zero scan
        // count would then mean the probe never reached the code it is about.
        assert!(
            expired > 0,
            "the sweep must have expired keys to have anchored anything: {report:?}",
        );
        assert_eq!(
            stats_scans, 0,
            "the expiry sweep must anchor without a full-log rescan under the guard; took \
             {stats_scans} scans for {expired} expired keys",
        );
    }

    /// THE CONTROL FOR THE SECOND COPY, and it exists because its absence was found by a mutant
    /// rather than by reading. `delete_drop` had a non-flat control from the start; the expiry
    /// sweep did not, so a mutation that dropped the sweep's `stats()` fallback entirely -- making
    /// it read a sequence the log does not vouch for when the length cache is refused -- passed
    /// the whole selection. One of two copies guarded is how the other copy keeps the defect.
    #[test]
    fn without_flat_append_the_expiry_sweeps_anchor_still_takes_the_exact_sequence() {
        let (_dir, engine) = engine_with(200);
        for index in 0..200 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::CommonExpire {
                    key: format!("evict-scale-key-{index}"),
                    ttl_ms: 1,
                },
            });
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        engine.wal_store.rescan_on_every_append_for_test();

        let before = engine.wal_store.raw_stats(1);
        let report = engine
            .sweep_expired_records(1)
            .expect("the sweep must run on a loaded shard");
        let after = engine.wal_store.raw_stats(1);
        let stats_scans = after.stats_full_scans.saturating_sub(before.stats_full_scans);

        assert!(
            report.expired_records_removed > 0,
            "the control must have expired keys, or it constrains nothing: {report:?}",
        );
        assert_eq!(
            stats_scans, 1,
            "with the log's length cache refused, the sweep's anchor must fall back to the exact \
             scanning stats() read; took {stats_scans} scans for {} expired keys",
            report.expired_records_removed,
        );
        let shards = engine.shards.read().expect("engine lock poisoned");
        let anchored = shards
            .get(&1)
            .and_then(|shard| shard.applied_wal_sequence)
            .expect("the sweep must have anchored");
        drop(shards);
        assert_eq!(
            anchored,
            engine.wal_store.stats(1).last_sequence,
            "the anchor must equal the log's own last sequence",
        );
    }

    /// The same corpus, READ BACK.
    ///
    /// A read is what populates the record caches -- `the_cache_namespaces_a_record_can_actually_use`
    /// (engine/tests/part4.rs) establishes that, and says it in as many words: "a read is what
    /// populates the record caches, so writing alone would leave every namespace empty". So
    /// `engine_with` on its own is a COLD-cache fixture, and the one knob between the two arms
    /// below is this loop.
    fn engine_with_warm_cache(objects: usize) -> (tempfile::TempDir, TemporalEngine) {
        let (dir, engine) = engine_with(objects);
        for index in 0..objects {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("evict-scale-key-{index}"),
                },
            });
            assert!(out.status.ok, "a warming read failed: {:?}", out.status);
        }
        (dir, engine)
    }

    /// One `delete_drop` round, measured for what its per-key cache sweep WALKS.
    #[derive(Debug)]
    struct SweepRound {
        objects: usize,
        warm: bool,
        dropped: usize,
        /// Counted INSIDE `invalidate_record_all` -- the PER-KEY primitive, which the drop
        /// loop no longer calls. Kept because eight other call sites still do, and because zero
        /// here is what says the drop loop stopped.
        calls: u64,
        sweeps: u64,
        entries_walked: u64,
        named: u64,
        /// Counted INSIDE `invalidate_records_all_batched` -- the ONE pass the round now makes.
        batched_calls: u64,
        keys_batched: u64,
        listings: u64,
        entries_listed: u64,
        entries_returned: u64,
        /// Counted OUTSIDE, off the cache itself, either side of the whole call.
        cache_entries_before: usize,
        cache_entries_after: usize,
        hold_ns: u64,
        delete_ns: u64,
        invalidate_ns: u64,
        wal_ns: u64,
        unattributed_ns: u64,
    }

    impl SweepRound {
        /// Keys this round's cache pass was handed that its own dropped-key count does not
        /// explain.
        ///
        /// INDEPENDENT: `keys_batched` is counted inside `invalidate_records_all_batched`, the
        /// primitive that does the invalidating, and `dropped` is the number
        /// `apply_storage_eviction` RETURNS to its caller. Neither is derived from the other, so
        /// this is not an identity -- a second invalidating path inside the round would show up
        /// here. It is asserted ACROSS the two corpus sizes, not against a constant.
        fn sweep_residual(&self) -> u64 {
            self.keys_batched.saturating_sub(self.dropped as u64)
        }

        fn pct(&self, part: u64) -> f64 {
            if self.hold_ns == 0 {
                0.0
            } else {
                part as f64 * 100.0 / self.hold_ns as f64
            }
        }

        fn walked_per_key(&self) -> f64 {
            if self.dropped == 0 {
                0.0
            } else {
                self.entries_walked as f64 / self.dropped as f64
            }
        }

        /// Cache entries the round's ONE pass stepped over, per key it dropped. The quantity
        /// #1907 measured at 499 and 3,999 -- twice the cache size, growing with the store.
        fn listed_per_key(&self) -> f64 {
            if self.dropped == 0 {
                0.0
            } else {
                self.entries_listed as f64 / self.dropped as f64
            }
        }

        fn label(&self) -> String {
            format!(
                "{} objects, {}",
                self.objects,
                if self.warm { "WARM" } else { "cold" }
            )
        }
    }

    /// `armed` turns on the per-sweep tier-length read that produces `entries_walked`. Every arm
    /// is run BOTH ways: the counts are read from the armed run and the timings from the
    /// disarmed one, so the apparatus that explains the hold is never inside the hold it explains.
    fn sweep_round(objects: usize, warm: bool, armed: bool) -> SweepRound {
        use super::CACHE_SWEEP_COUNTS as SWEEP;
        use super::DELETE_DROP_GUARD_NANOS as NANOS;

        let (_dir, engine) = if warm {
            engine_with_warm_cache(objects)
        } else {
            engine_with(objects)
        };
        engine.use_sampled_eviction_for_test();
        let cache_entries_before = engine.cache.entries_for_shard(1).len();
        SWEEP.reset();
        SWEEP.set_armed(armed);
        NANOS.reset();
        let report = engine.apply_storage_eviction(1, 0, 0, false, true);
        SWEEP.set_armed(false);
        let cache_entries_after = engine.cache.entries_for_shard(1).len();
        assert!(
            report.pressure_gate_open,
            "the round must have got past the pressure gate, or nothing was measured"
        );
        let (calls, sweeps, entries_walked, named) = SWEEP.read();
        let (batched_calls, keys_batched, listings, entries_listed, entries_returned) =
            SWEEP.read_batched();
        let (hold_ns, _collect_ns, delete_ns, invalidate_ns, wal_ns, _anchor_ns, _snapshot_ns) =
            NANOS.read();
        SweepRound {
            objects,
            warm,
            dropped: report.dropped_object_count,
            calls,
            sweeps,
            entries_walked,
            named,
            batched_calls,
            keys_batched,
            listings,
            entries_listed,
            entries_returned,
            cache_entries_before,
            cache_entries_after,
            hold_ns,
            delete_ns,
            invalidate_ns,
            wal_ns,
            unattributed_ns: NANOS.unattributed(),
        }
    }

    /// WHAT THE PER-KEY CACHE SWEEP WALKS, COLD AND WARM.
    ///
    /// `where_a_delete_drop_rounds_shard_guard_hold_actually_goes` puts `invalidate_record_all` at
    /// the top of the hold. That measurement was taken on a fixture that writes a corpus and never
    /// reads it back, and a cache is populated by READS -- so the share it reported is a FLOOR,
    /// not the value. This establishes the value.
    ///
    /// WHY THIS IS A COUNT AND NOT A TIME. One `invalidate_record_all` makes two
    /// `MultiLayerCache::invalidate_record` calls, and each of those chains the key sets of all
    /// three cache tiers and filters:
    ///
    ///     inner.memory.keys().chain(inner.pmem.keys()).chain(inner.disk_index.keys())
    ///         .filter(|key| key.shard_id == shard_id && key.namespace == namespace && ...)
    ///
    /// So the work is ENTRIES WALKED, it is exactly the sum of the three tier lengths, and it is
    /// countable. `entries_walked` is taken immediately before each walk, from those same three
    /// lengths, inside the primitive -- so a new call site cannot compile without saying where its
    /// sweeps are counted, and a walk cannot happen without being sized.
    ///
    /// The nanosecond rows are PRINTED, not asserted, for the reason the probe above gives: a
    /// time on this box is a fact about the box. What is asserted is counts.
    #[test]
    fn what_a_delete_drop_rounds_cache_sweep_walks_cold_and_warm() {
        const SMALL: usize = 500;
        const LARGE: usize = 4000;

        // COUNTS from the armed runs; TIMINGS from the disarmed ones. Same fixture, same round.
        let cold_small = sweep_round(SMALL, false, true);
        let warm_small = sweep_round(SMALL, true, true);
        let cold_large = sweep_round(LARGE, false, true);
        let warm_large = sweep_round(LARGE, true, true);
        let t_cold_small = sweep_round(SMALL, false, false);
        let t_warm_small = sweep_round(SMALL, true, false);
        let t_cold_large = sweep_round(LARGE, false, false);
        let t_warm_large = sweep_round(LARGE, true, false);

        let ratio = |cold: f64, warm: f64| if cold == 0.0 { 0.0 } else { warm / cold };
        println!(
            "\n  THE PER-KEY CACHE SWEEP of one delete_drop round, COLD cache vs WARM cache\n  \
               (cold = the corpus written and never read; warm = the same corpus READ BACK)\n\
             \n                                  {:>12} {:>12} {:>12} {:>12}\n\
               corpus objects              {:>12} {:>12} {:>12} {:>12}\n\
               cache entries BEFORE (outer){:>12} {:>12} {:>12} {:>12}\n\
               cache entries AFTER  (outer){:>12} {:>12} {:>12} {:>12}\n\
               keys the round dropped      {:>12} {:>12} {:>12} {:>12}\n\
               batched cache passes        {:>12} {:>12} {:>12} {:>12}\n\
               keys handed to the pass     {:>12} {:>12} {:>12} {:>12}\n\
               .. residual, keys - dropped {:>12} {:>12} {:>12} {:>12}\n\
               listings those passes made  {:>12} {:>12} {:>12} {:>12}\n\
               PER-KEY invalidate_record_all{:>11} {:>12} {:>12} {:>12}\n\
               PER-KEY invalidate_record   {:>12} {:>12} {:>12} {:>12}\n\
               named-key invalidations     {:>12} {:>12} {:>12} {:>12}\n\
               CACHE ENTRIES LISTED        {:>12} {:>12} {:>12} {:>12}\n\
               .. listed per dropped key   {:>12.1} {:>12.1} {:>12.1} {:>12.1}\n\
               entries the listing RETURNED{:>12} {:>12} {:>12} {:>12}\n\
               CACHE ENTRIES WALKED        {:>12} {:>12} {:>12} {:>12}\n",
            "cold 500", "WARM 500", "cold 4000", "WARM 4000",
            cold_small.objects, warm_small.objects, cold_large.objects, warm_large.objects,
            cold_small.cache_entries_before, warm_small.cache_entries_before,
            cold_large.cache_entries_before, warm_large.cache_entries_before,
            cold_small.cache_entries_after, warm_small.cache_entries_after,
            cold_large.cache_entries_after, warm_large.cache_entries_after,
            cold_small.dropped, warm_small.dropped, cold_large.dropped, warm_large.dropped,
            cold_small.batched_calls, warm_small.batched_calls,
            cold_large.batched_calls, warm_large.batched_calls,
            cold_small.keys_batched, warm_small.keys_batched,
            cold_large.keys_batched, warm_large.keys_batched,
            cold_small.sweep_residual(), warm_small.sweep_residual(),
            cold_large.sweep_residual(), warm_large.sweep_residual(),
            cold_small.listings, warm_small.listings, cold_large.listings, warm_large.listings,
            cold_small.calls, warm_small.calls, cold_large.calls, warm_large.calls,
            cold_small.sweeps, warm_small.sweeps, cold_large.sweeps, warm_large.sweeps,
            cold_small.named, warm_small.named, cold_large.named, warm_large.named,
            cold_small.entries_listed, warm_small.entries_listed,
            cold_large.entries_listed, warm_large.entries_listed,
            cold_small.listed_per_key(), warm_small.listed_per_key(),
            cold_large.listed_per_key(), warm_large.listed_per_key(),
            cold_small.entries_returned, warm_small.entries_returned,
            cold_large.entries_returned, warm_large.entries_returned,
            cold_small.entries_walked, warm_small.entries_walked,
            cold_large.entries_walked, warm_large.entries_walked,
        );

        println!(
            "  THE HOLD, SPLIT, from the DISARMED runs (printed -- a time here is a fact about \
             the box)\n\
             \n                                  {:>12} {:>12} {:>12} {:>12}\n\
               guard held, total us        {:>12.0} {:>12.0} {:>12.0} {:>12.0}\n\
               delete_record (NEEDS guard) {:>11.1}% {:>11.1}% {:>11.1}% {:>11.1}%\n\
               the round's cache pass      {:>11.1}% {:>11.1}% {:>11.1}% {:>11.1}%\n\
               WAL tombstone loop          {:>11.1}% {:>11.1}% {:>11.1}% {:>11.1}%\n\
               unattributed (independent)  {:>11.1}% {:>11.1}% {:>11.1}% {:>11.1}%\n\
             \n  WARM / COLD on the pass's share of the hold:   {:>5.2}x at {SMALL}, \
             {:>5.2}x at {LARGE}\n\
               entries listed at {SMALL:>4}, cold -> WARM  {:>9} -> {:<10}\n\
               entries listed at {LARGE:>4}, cold -> WARM  {:>9} -> {:<10}\n\
               (no ratio is printed: the cold arm lists NONE, and that is the finding)\n",
            "cold 500", "WARM 500", "cold 4000", "WARM 4000",
            t_cold_small.hold_ns as f64 / 1000.0, t_warm_small.hold_ns as f64 / 1000.0,
            t_cold_large.hold_ns as f64 / 1000.0, t_warm_large.hold_ns as f64 / 1000.0,
            t_cold_small.pct(t_cold_small.delete_ns), t_warm_small.pct(t_warm_small.delete_ns),
            t_cold_large.pct(t_cold_large.delete_ns), t_warm_large.pct(t_warm_large.delete_ns),
            t_cold_small.pct(t_cold_small.invalidate_ns),
            t_warm_small.pct(t_warm_small.invalidate_ns),
            t_cold_large.pct(t_cold_large.invalidate_ns),
            t_warm_large.pct(t_warm_large.invalidate_ns),
            t_cold_small.pct(t_cold_small.wal_ns), t_warm_small.pct(t_warm_small.wal_ns),
            t_cold_large.pct(t_cold_large.wal_ns), t_warm_large.pct(t_warm_large.wal_ns),
            t_cold_small.pct(t_cold_small.unattributed_ns),
            t_warm_small.pct(t_warm_small.unattributed_ns),
            t_cold_large.pct(t_cold_large.unattributed_ns),
            t_warm_large.pct(t_warm_large.unattributed_ns),
            ratio(
                t_cold_small.pct(t_cold_small.invalidate_ns),
                t_warm_small.pct(t_warm_small.invalidate_ns)
            ),
            ratio(
                t_cold_large.pct(t_cold_large.invalidate_ns),
                t_warm_large.pct(t_warm_large.invalidate_ns)
            ),
            cold_small.entries_listed,
            warm_small.entries_listed,
            cold_large.entries_listed,
            warm_large.entries_listed,
        );

        let armed = [&cold_small, &warm_small, &cold_large, &warm_large];

        // VACUITY FLOOR, on the measured denominator, at every arm. A round that dropped nothing
        // swept nothing, and every ratio below would then be a ratio of zeroes.
        for round in armed {
            assert!(
                round.dropped > 0,
                "{} dropped no keys, so nothing was swept: {round:?}",
                round.label(),
            );
        }
        assert!(
            cold_large.dropped > cold_small.dropped && warm_large.dropped > warm_small.dropped,
            "the two corpora must differ in dropped keys at both temperatures: \
             cold {} vs {}, warm {} vs {}",
            cold_small.dropped,
            cold_large.dropped,
            warm_small.dropped,
            warm_large.dropped,
        );

        // THE POSITIVE CONTROL, and everything below depends on it: the WARM arm has to actually
        // be warmer. `cache_entries_before` is read off the cache itself, outside the round, so
        // it is not the counter vouching for its own fixture. Without this a reading loop that
        // cached nothing would make both arms identical and every ratio below a flat 1.00x --
        // which reads exactly like a refutation.
        for (cold, warm) in [(&cold_small, &warm_small), (&cold_large, &warm_large)] {
            assert!(
                warm.cache_entries_before > cold.cache_entries_before,
                "the warm arm must hold more cache entries than the cold one at {} objects, \
                 or the two arms are the same experiment: cold {} vs warm {}",
                cold.objects,
                cold.cache_entries_before,
                warm.cache_entries_before,
            );
        }

        // THE COUNTER COUNTS WHERE THE WORK IS ASKED FOR. ONE pass per round, one listing in
        // it, one entry in the dropped set per dropped key, and two named invalidations per key.
        // Equalities, not bounds: "at least one listing" would also pass on a pass that had gone
        // back to listing per key, which is the whole thing this change removes.
        for round in armed {
            assert_eq!(
                round.batched_calls, 1,
                "{}: the round must make exactly ONE batched cache pass, whatever it dropped; \
                 it made {} for {} keys",
                round.label(),
                round.batched_calls,
                round.dropped,
            );
            assert_eq!(
                round.listings, round.batched_calls,
                "{}: the pass must list the cache exactly once; {} listings in {} passes",
                round.label(),
                round.listings,
                round.batched_calls,
            );
            assert_eq!(
                round.keys_batched, round.dropped as u64,
                "{}: every dropped key must reach the pass; {} keys for {} dropped",
                round.label(),
                round.keys_batched,
                round.dropped,
            );
            assert_eq!(
                round.named,
                round.keys_batched.saturating_mul(2),
                "{}: each key names two entries (string, set/members); {} for {} keys",
                round.label(),
                round.named,
                round.keys_batched,
            );
            // AND THE PER-KEY SWEEP IS GONE FROM THIS PATH. `invalidate_record_all` is still
            // reached from eight other call sites -- six single-key command paths in
            // execute_on_shard.rs and the expiry sweep's own two in recovery_sweep_compact.rs --
            // so this is a claim about the drop loop, not about the function: a round that still
            // made one call per key would fail here.
            assert_eq!(
                round.calls, 0,
                "{}: the drop loop must make no PER-KEY invalidate_record_all calls at all; \
                 it made {}",
                round.label(),
                round.calls,
            );
            assert_eq!(
                round.sweeps, 0,
                "{}: the drop loop must make no PER-KEY invalidate_record sweeps at all; \
                 it made {}",
                round.label(),
                round.sweeps,
            );
            assert_eq!(
                round.entries_walked, 0,
                "{}: with no per-key sweeps there is nothing for the sweep sizer to size; \
                 it walked {}",
                round.label(),
                round.entries_walked,
            );
        }

        // THE INDEPENDENT RESIDUAL, asserted ACROSS THE SIZES rather than against a constant.
        // `keys_batched` comes from inside `invalidate_records_all_batched`; `dropped` is what
        // `apply_storage_eviction` returns. Equal across the sizes means whatever invalidating
        // the round does beyond its drop loop is fixed per round. A residual that GREW with the
        // corpus would be a second per-key invalidating path in no row of this table, which is
        // exactly the drift an assertion against a constant would have absorbed.
        assert_eq!(
            cold_small.sweep_residual(),
            cold_large.sweep_residual(),
            "cold: keys reaching the cache pass beyond the drop loop must be fixed per round, \
             not per key: {} at {SMALL} and {} at {LARGE}",
            cold_small.sweep_residual(),
            cold_large.sweep_residual(),
        );
        assert_eq!(
            warm_small.sweep_residual(),
            warm_large.sweep_residual(),
            "warm: keys reaching the cache pass beyond the drop loop must be fixed per round, \
             not per key: {} at {SMALL} and {} at {LARGE}",
            warm_small.sweep_residual(),
            warm_large.sweep_residual(),
        );

        // THE CLAIM, FIRST HALF. A pass steps over the cache, so a warm cache is stepped over
        // and a cold one is not. The cold arm is the FLOOR the earlier split reported; the warm
        // arm is what a serving store pays. Asserted as a strict inequality on a COUNT at both
        // sizes -- the timings above only illustrate it.
        for (cold, warm) in [(&cold_small, &warm_small), (&cold_large, &warm_large)] {
            assert!(
                warm.entries_listed > cold.entries_listed,
                "the pass must step over more of a warm cache than of a cold one at {} objects: \
                 cold listed {} entries, warm listed {}",
                cold.objects,
                cold.entries_listed,
                warm.entries_listed,
            );
        }

        // AND THE WALK IS THE CACHE, ONCE PER ROUND, whatever the round dropped. Bounded by
        // the cache size read from OUTSIDE the round, either side of it.
        //
        // WHY THE UPPER BOUND IS THREE CACHE LENGTHS AND NOT ONE. `entries_listed` is the sum of
        // the three TIER lengths -- the same definition `entries_walked` uses, which is what
        // makes the two commensurable -- while `cache_entries_before` is the DEDUPLICATED listing
        // of the shard, and an entry present in two tiers is counted twice in the first and once
        // in the second. Three tiers is the ceiling on that. What the bound excludes is the shape
        // this change removed: one listing per dropped key would put `entries_listed` at N cache
        // lengths, and N is in the hundreds at {SMALL} objects and the thousands at {LARGE}.
        for round in armed {
            let ceiling = 3u64.saturating_mul(round.cache_entries_before as u64);
            assert!(
                round.entries_listed >= round.cache_entries_after as u64
                    && round.entries_listed <= ceiling,
                "{}: entries listed ({}) must lie between the cache size after ({}) and three \
                 cache lengths ({ceiling}) -- ONE pass over the cache, not one per dropped key \
                 ({} of them)",
                round.label(),
                round.entries_listed,
                round.cache_entries_after,
                round.dropped,
            );
        }

        // THE CLAIM, SECOND HALF, AND IT IS THE POINT OF THE CHANGE. What #1907 measured was a
        // per-key cost that GREW WITH THE STORE: 499 entries walked per dropped key at 500
        // objects and 3,999 at 4,000, because each key walked two whole cache lengths. One pass
        // spreads a single cache length over every key the round drops, so the per-key figure
        // stops tracking the corpus. An eightfold corpus must not bring an eightfold per-key
        // cost with it.
        let small_per_key = warm_small.listed_per_key();
        let large_per_key = warm_large.listed_per_key();
        assert!(
            small_per_key > 0.0 && large_per_key > 0.0,
            "both warm arms must have listed something per key, or the ratio below is a ratio \
             of zeroes: {small_per_key:.2} at {SMALL} and {large_per_key:.2} at {LARGE}",
        );
        assert!(
            large_per_key <= small_per_key * 1.5,
            "entries listed per dropped key must not grow with the corpus: {small_per_key:.2} \
             at {SMALL} objects and {large_per_key:.2} at {LARGE}; the per-key shape this \
             replaced went from 499 to 3,999 over the same two sizes",
        );
    }

    /// THE CONTROL FOR THE APPARATUS, and it can fail in both directions.
    ///
    /// `entries_walked` is the only figure above that costs anything to collect -- three
    /// uncontended tier-length reads per sweep -- so it sits behind a flag, and the timings in the
    /// probe above are read from rounds with the flag OFF. That split is only honest if the flag
    /// actually gates the work: armed must produce a size and disarmed must produce none, on the
    /// same round, with the sweep count identical either way.
    ///
    /// A one-directional check would not do. "Armed produces a size" passes on a counter that is
    /// always on, and the timings would then be measuring their own apparatus.
    #[test]
    fn the_sweep_size_counter_is_off_unless_it_is_armed() {
        let armed = sweep_round(300, true, true);
        let disarmed = sweep_round(300, true, false);

        assert!(
            armed.listings > 0 && disarmed.listings > 0,
            "both arms must have listed the cache, or the flag is being tested on nothing: \
             {armed:?} {disarmed:?}",
        );
        assert_eq!(
            armed.listings, disarmed.listings,
            "arming must not change how many listings happen, only whether they are sized: \
             {} armed vs {} disarmed",
            armed.listings, disarmed.listings,
        );
        assert_eq!(
            armed.entries_returned, disarmed.entries_returned,
            "arming must not change what the listings return either: {} armed vs {} disarmed",
            armed.entries_returned, disarmed.entries_returned,
        );
        assert!(
            armed.entries_listed > 0,
            "the armed arm must size its pass; listed {}",
            armed.entries_listed,
        );
        assert_eq!(
            disarmed.entries_listed, 0,
            "the disarmed arm must do no sizing at all, or the timings it is used for include \
             the apparatus that explains them; listed {}",
            disarmed.entries_listed,
        );
    }

    /// THE READ-VISIBILITY ARGUMENT, as a test rather than as prose.
    ///
    /// `where_a_delete_drop_rounds_shard_guard_hold_actually_goes` put `invalidate_record_all` at
    /// the top of the hold, and the probe above puts it higher still once the cache is warm. The
    /// obvious move is to defer it: the sweep is handed `&MultiLayerCache` and a key, the compiler
    /// says it borrows nothing from `shard` (removing each of its three parameters in turn names
    /// four lines in `engine.rs` and no others), and hoisting the call past `drop(shards)` compiles
    /// unchanged. Priced as a mutant it takes a 4,000-key warm round's hold from 1.67 s to 0.39 s.
    ///
    /// It is still wrong, and this is the interleaving that makes it wrong.
    ///
    /// WHICH READER, WHICH CACHE, WHICH MOMENT, SEEING WHAT. A `StringGet` is served by
    /// `cached_response` (`engine/command_validation.rs`), which is CACHE-FIRST and consults
    /// nothing else:
    ///
    ///     if let Ok(Some(bytes)) = cache.get(&key) { ... return response; }
    ///     let response = source();     // <- the shard, reached only on a miss
    ///
    /// `CacheKey::string(shard_id, key)` is `{shard, "string", key, "value"}`. It carries no
    /// generation, no sequence and no version stamp -- `page` keys have a generation selector,
    /// record keys do not -- so there is nothing in the key or in the read path that could notice
    /// that the shard has moved on. Nothing else closes the window either: the round's earlier
    /// `invalidate_slot` call filters on a routing slot, and a `string` key's selector is
    /// `"value"`, which has no slot.
    ///
    /// So the moment is this. With the sweep deferred, `delete_record` removes the key under the
    /// `shards` write guard, the tombstone is appended and `applied_wal_sequence` anchored past it
    /// -- the deletion is now durable and replicated -- and the guard is dropped. Until the
    /// deferred sweep reaches that key, any reader that takes the `shards` lock and asks for it is
    /// answered out of the cache with the value the shard no longer has. The window is not a few
    /// instructions wide: it is the whole sweep, which is the quantity this change exists to
    /// shrink, and it grows with the cache.
    ///
    /// IN WHICH DIRECTION IT FAILS. Deferring an invalidation can only ever leave the cache MORE
    /// populated than the shard, never less -- it delays removals and adds nothing. So the single
    /// observable failure is STALE-ALIVE: a read answers with a value for a key the shard has
    /// already deleted. The opposite direction, a read reporting missing for a live key, is not
    /// reachable this way at all. A check that only asked "is the cache eventually empty of this
    /// key" would therefore pass under the unsafe mutant, because it is a question about the end
    /// state and the defect is entirely in the middle. This attacks the observable direction: it
    /// reads keys the shard has ALREADY dropped, while the round is still running.
    ///
    /// WHY IT NEEDS A SECOND THREAD. The window opens and closes inside one call, so a
    /// single-threaded fixture cannot be inside it -- which is why the suite had nothing that
    /// could see this. The reader takes the `shards` lock to decide a key is gone, so under the
    /// shipped code it cannot observe that until the guarded section that both deletes and sweeps
    /// has completed; under the mutant it observes it the moment the guard drops.
    ///
    /// NO FALSE POSITIVE IS POSSIBLE. A key absent from the live block entries stays absent -- the
    /// round only removes -- so a candidate chosen from one snapshot is still a valid candidate
    /// when it is read. And under the shipped code the delete and the sweep for a given key are in
    /// the same `shards.write()` section, so no `shards` reader can stand between them.
    #[test]
    fn a_key_the_shard_has_dropped_is_never_still_answered_out_of_the_cache() {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        use std::sync::Arc;

        const OBJECTS: usize = 800;

        // WARM, because a cold cache holds nothing to serve stale: the probe above measures the
        // `engine_with` fixture at ZERO cache entries. On a cold cache this test would be looking
        // for a stale answer that could not exist, and would pass against any mutant at all.
        let (_dir, engine) = engine_with_warm_cache(OBJECTS);
        engine.use_sampled_eviction_for_test();

        let stop = Arc::new(AtomicBool::new(false));
        let stale_answers = Arc::new(AtomicU64::new(0));
        let dropped_keys_read = Arc::new(AtomicU64::new(0));

        let reader = {
            let engine = engine.clone();
            let stop = Arc::clone(&stop);
            let stale_answers = Arc::clone(&stale_answers);
            let dropped_keys_read = Arc::clone(&dropped_keys_read);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    // Which keys the SHARD has already let go of, read under the shard lock.
                    let live = {
                        let shards = engine.shards.read().expect("engine lock poisoned");
                        match shards.get(&1) {
                            Some(shard) => collect_live_block_entries(shard)
                                .into_iter()
                                .map(|entry| entry.object_key)
                                .collect::<std::collections::BTreeSet<_>>(),
                            None => break,
                        }
                    };
                    for index in 0..OBJECTS {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let key = format!("evict-scale-key-{index}");
                        if live.contains(key.as_str()) {
                            continue;
                        }
                        // The shard has dropped this key. Ask for it anyway.
                        dropped_keys_read.fetch_add(1, Ordering::Relaxed);
                        let out = engine.execute(ExecuteRequest {
                            shard_id: 1,
                            command: Command::StringGet { key },
                        });
                        if let CommandResponse::Bytes { value: Some(_) } = out.response {
                            stale_answers.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        };

        let report = engine.apply_storage_eviction(1, 0, 0, false, true);
        // One more full pass after the round, so the reader is guaranteed to have seen the final
        // state as well as the middle of it.
        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        reader.join().expect("the reader thread must not panic");

        let stale = stale_answers.load(Ordering::Relaxed);
        let read = dropped_keys_read.load(Ordering::Relaxed);
        println!(
            "\n  READS OF KEYS THE SHARD HAD ALREADY DROPPED, during a delete_drop round\n    \
               keys the round dropped          {:>8}\n    \
               reads issued for dropped keys   {read:>8}\n    \
               ANSWERED WITH A VALUE ANYWAY    {stale:>8}\n",
            report.dropped_object_count,
        );

        // VACUITY FLOOR. A round that dropped nothing, or a reader that never caught a key in the
        // dropped state, makes the claim below an assertion about an empty set.
        assert!(
            report.pressure_gate_open && report.dropped_object_count > 0,
            "the round must have dropped keys, or there was nothing to read stale: {report:?}",
        );
        assert!(
            read > 0,
            "the reader never found a key the shard had dropped, so it never entered the window \
             this test exists to watch; it issued {read} reads",
        );

        // THE CLAIM, in the one direction a deferred invalidation can fail.
        assert_eq!(
            stale, 0,
            "{stale} of {read} reads of already-dropped keys were answered out of the cache with \
             a value the shard no longer holds; the sweep that drops those entries has to stay in \
             the same shards.write() section as the delete that makes them stale",
        );
    }

    /// THE TWO MOST EXPENSIVE LINES IN THE HOLD WERE THE TWO NOTHING WAS WATCHING.
    ///
    /// `invalidate_record_all` sweeps `hash` and `feature`, and those two sweeps are the whole of
    /// what makes it O(cache) per key -- the probe above measures them walking 3,999 entries per
    /// dropped key on a warm 4,000-object store. Mutating each one to sweep for a key that cannot
    /// exist leaves them doing nothing, and BOTH mutants passed the entire 107-test
    /// eviction/invalidation/cache selection. So the lines that cost the most were also the lines
    /// no test could tell were working.
    ///
    /// This kills both, and it is the equivalence check any future narrowing of those sweeps has
    /// to pass: whatever it does instead, a key the round drops must keep no cached entry in any
    /// namespace the sweep covers.
    ///
    /// THE POSITIVE CONTROL RUNS FIRST AND IS NOT OPTIONAL. Everything asserted after the round is
    /// an emptiness claim, and an empty cache satisfies every emptiness claim at once. Writing a
    /// hash field or a feature point is not enough to populate its cache entry either -- a READ is
    /// what does that (`the_cache_namespaces_a_record_can_actually_use`) -- so the control has to
    /// assert the entries are really there before the round, per namespace, not in aggregate.
    #[test]
    fn a_delete_drop_round_clears_every_swept_namespace_of_the_keys_it_drops() {
        const OBJECTS: usize = 300;
        const MARKED: [usize; 3] = [3, 17, 42];

        let (_dir, engine) = engine_with(OBJECTS);
        let marked_keys: Vec<String> = MARKED
            .iter()
            .map(|index| format!("evict-scale-key-{index}"))
            .collect();

        for key in &marked_keys {
            // Write, then READ, in each namespace the sweep covers plus the two it names.
            for command in [
                Command::HashSet {
                    key: key.clone(),
                    field: "sweep-field".to_string(),
                    value: b"hash-value".to_vec(),
                },
                Command::HashGet {
                    key: key.clone(),
                    field: "sweep-field".to_string(),
                },
                Command::SetAdd {
                    key: key.clone(),
                    member: b"sweep-member".to_vec(),
                },
                Command::SetMembers { key: key.clone() },
                Command::FeatureAppend {
                    key: key.clone(),
                    points: vec![crate::types::FeaturePoint {
                        timestamp_ms: 1_000,
                        value: b"feature-value".to_vec(),
                    }],
                },
                Command::FeatureQuery {
                    key: key.clone(),
                    start_ms: 0,
                    end_ms: 10_000,
                    count: None,
                },
                Command::StringGet { key: key.clone() },
            ] {
                let out = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command,
                });
                assert!(out.status.ok, "fixture command failed: {:?}", out.status);
            }
        }
        engine.use_sampled_eviction_for_test();

        let occupied = |entries: &[matrixcache::CacheEntryInfo], namespace: &str| -> Vec<String> {
            entries
                .iter()
                .filter(|entry| {
                    entry.namespace == namespace
                        && marked_keys.iter().any(|key| key == &entry.record_key)
                })
                .map(|entry| format!("{}/{}", entry.record_key, entry.selector))
                .collect()
        };

        let before = engine.cache.entries_for_shard(1);
        // DERIVED FROM THE AUTHORITY, not written out beside it. `SWEPT_RECORD_NAMESPACES` and
        // `named_record_keys` are what both the per-key primitive and the batched pass read, so a
        // namespace added to either is covered by this test the moment it is added -- a
        // hand-written copy of the list goes stale and nothing fails.
        let swept: Vec<String> = super::SWEPT_RECORD_NAMESPACES
            .iter()
            .map(|namespace| namespace.to_string())
            .collect();
        let named: Vec<String> = super::named_record_keys(1, "probe")
            .iter()
            .map(|key| key.namespace.to_string())
            .collect();
        // AND FLOORED, because deriving cuts both ways: a change that removes a namespace from
        // the authority removes it from this test in the same moment, and the test then passes
        // by checking one thing fewer. Adding a namespace is free; losing one fails here.
        for required in ["hash", "feature", "string", "set"] {
            assert!(
                swept.iter().chain(named.iter()).any(|name| name == required),
                "`{required}` is no longer in the namespace list this test derives from the \
                 production authority, so nothing here checks it any more: swept {swept:?}, \
                 named {named:?}",
            );
        }
        println!(
            "\n  CACHE ENTRIES FOR THE MARKED KEYS, BEFORE THE ROUND\n    \
               hash    {:>4}\n    feature {:>4}\n    string  {:>4}\n    set     {:>4}\n",
            occupied(&before, "hash").len(),
            occupied(&before, "feature").len(),
            occupied(&before, "string").len(),
            occupied(&before, "set").len(),
        );

        // THE POSITIVE CONTROL, per namespace.
        for namespace in swept.iter().chain(named.iter()) {
            assert!(
                !occupied(&before, namespace).is_empty(),
                "no `{namespace}` entry was cached for the marked keys, so the emptiness claim \
                 below would hold whatever the sweep did",
            );
        }

        let report = engine.apply_storage_eviction(1, 0, 0, false, true);
        assert!(
            report.pressure_gate_open && report.dropped_object_count > 0,
            "the round must have dropped keys: {report:?}",
        );

        let after = engine.cache.entries_for_shard(1);
        let live_after = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("the shard must still be loaded");
            collect_live_block_entries(shard)
                .into_iter()
                .map(|entry| entry.object_key)
                .collect::<std::collections::BTreeSet<_>>()
        };
        // Only keys the round actually dropped are covered by the claim. A marked key the round
        // left alone must KEEP its cache entries, and asserting emptiness for it would be
        // asserting the wrong thing.
        let dropped_marked: Vec<&String> = marked_keys
            .iter()
            .filter(|key| !live_after.contains(key.as_str()))
            .collect();
        assert!(
            !dropped_marked.is_empty(),
            "the round dropped none of the marked keys, so it swept none of their namespaces; \
             it dropped {} keys in total",
            report.dropped_object_count,
        );

        for namespace in swept.iter().chain(named.iter()) {
            let left = after
                .iter()
                .filter(|entry| {
                    entry.namespace == *namespace
                        && dropped_marked.iter().any(|key| *key == &entry.record_key)
                })
                .map(|entry| format!("{}/{}", entry.record_key, entry.selector))
                .collect::<Vec<_>>();
            assert!(
                left.is_empty(),
                "the round dropped {dropped_marked:?} but left {} `{namespace}` entries cached \
                 for them: {left:?}",
                left.len(),
            );
        }
    }

    /// WHAT ONE LISTING OF THE SHARD'S CACHE COSTS IN SYSCALLS.
    ///
    /// #1907 measured the shipped sweep at 3,999 cache entries walked PER DROPPED KEY on a warm
    /// 4,000-object store -- 15,996,000 entry visits for one round, inside the `shards` write
    /// guard. One batched pass would visit the cache once instead of twice per key, but the only
    /// listing `MultiLayerCache` exposes is `entries_for_shard`, and for every entry that is NOT
    /// in the disk index it falls through to a filesystem call:
    ///
    ///     let disk_bytes = inner.disk_index.get(&key).copied().unwrap_or_else(|| {
    ///         inner.disk_path(&key).metadata().map(|m| m.len()).unwrap_or_default()
    ///     });
    ///
    /// So the batched pass trades N cache walks for one cache walk plus C syscalls, and C is a
    /// property of the fixture, not a constant. This measures C.
    ///
    /// HOW. The count is a DIFFERENCE taken from outside the process: the same test binary is run
    /// under `strace -f -c` twice, once with `TS_PROBE_LIST_REPEATS=0` and once with a positive
    /// repeat count, and the per-listing syscall count is the difference divided by the repeats.
    /// Everything else the test does -- building the corpus, warming the cache, the assertion's
    /// own listing -- is identical in both arms and cancels. Nothing in this process counts its
    /// own syscalls.
    ///
    /// THE CACHE MUST BE POPULATED. `engine_with` writes a corpus and never reads it back, and a
    /// READ is what populates a record cache, so the cold fixture lists ZERO entries -- which
    /// would put C at zero and read exactly like a free listing. The fixture here is the warm one
    /// and the entry count is asserted non-zero and printed.
    #[test]
    fn what_one_listing_of_the_shards_cache_costs_in_syscalls() {
        let objects: usize = std::env::var("TS_PROBE_OBJECTS")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(500);
        let repeats: usize = std::env::var("TS_PROBE_LIST_REPEATS")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);

        let (_dir, engine) = engine_with_warm_cache(objects);
        let listing = engine.cache.entries_for_shard(1);
        let entries = listing.len();
        assert!(
            entries > 0,
            "the fixture's cache is empty, so a listing of it would cost nothing and this probe \
             would report a free operation; objects {objects}",
        );
        let mut by_namespace = std::collections::BTreeMap::<String, usize>::new();
        for entry in &listing {
            *by_namespace.entry(entry.namespace.clone()).or_default() += 1;
        }
        let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
        println!(
            "PROBE-LISTING objects={objects} entries={entries} repeats={repeats} \
             namespaces={by_namespace:?} load={}",
            load.split_whitespace().next().unwrap_or("?"),
        );
        let started = std::time::Instant::now();
        let mut listed = 0usize;
        for _ in 0..repeats {
            listed = listed.saturating_add(engine.cache.entries_for_shard(1).len());
        }
        let elapsed = started.elapsed().as_micros();
        // The listing must be deterministic, or the syscall difference this probe is driven for
        // is a difference between two different listings.
        assert_eq!(
            engine.cache.entries_for_shard(1).len(),
            entries,
            "two listings of an untouched cache returned different lengths",
        );
        println!(
            "PROBE-LISTING listed_total={listed} elapsed_us={elapsed} per_listing_us={:.1}",
            if repeats == 0 {
                0.0
            } else {
                elapsed as f64 / repeats as f64
            },
        );
    }

    /// A warm cache with SEVERAL entries per marked key in every namespace the invalidation
    /// covers, so the two arms below have something to disagree about.
    ///
    /// A READ is what populates a record cache -- `the_cache_namespaces_a_record_can_actually_use`
    /// (engine/tests/part4.rs) says so in as many words -- so every write here is followed by the
    /// read that caches it. Three hash fields and three feature windows per key, because the
    /// namespaces that get SWEPT are exactly the ones that hold more than one entry per key: a
    /// hash caches one entry per field and a feature one per query window. A fixture with one
    /// entry each would make "drop the entry" and "drop every entry" the same assertion.
    fn engine_with_marked_record_entries(
        objects: usize,
        marked: &[String],
    ) -> (tempfile::TempDir, TemporalEngine) {
        let (dir, engine) = engine_with_warm_cache(objects);
        let mut run = |command: Command| {
            let out = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(out.status.ok, "fixture command failed: {:?}", out.status);
        };
        for key in marked {
            for field in ["alpha", "beta", "gamma"] {
                run(Command::HashSet {
                    key: key.clone(),
                    field: field.to_string(),
                    value: b"hash-value".to_vec(),
                });
                run(Command::HashGet {
                    key: key.clone(),
                    field: field.to_string(),
                });
            }
            run(Command::SetAdd {
                key: key.clone(),
                member: b"sweep-member".to_vec(),
            });
            run(Command::SetMembers { key: key.clone() });
            run(Command::FeatureAppend {
                key: key.clone(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_000,
                    value: b"feature-value".to_vec(),
                }],
            });
            for (start_ms, end_ms) in [(0u64, 10_000u64), (0, 20_000), (500, 9_000)] {
                run(Command::FeatureQuery {
                    key: key.clone(),
                    start_ms,
                    end_ms,
                    count: None,
                });
            }
            run(Command::StringGet { key: key.clone() });
        }
        (dir, engine)
    }

    /// What one arm of the comparison did, and what it left in the cache.
    #[derive(Debug)]
    struct CachePassArm {
        arm: &'static str,
        objects: usize,
        dropped: usize,
        /// Cache entries the arm stepped over. SAME DEFINITION in both arms -- the sum of the
        /// three tier lengths, read inside the primitive immediately before each walk, by
        /// `note_sweep` in one arm and `note_listing` in the other.
        stepped: u64,
        /// Walks: `invalidate_record` calls in the per-key arm, `entries_for_shard` listings in
        /// the batched one.
        walks: u64,
        /// Entries the batched arm's listing RETURNED. Zero in the per-key arm, which lists
        /// nothing at all. This is the upper bound on the arm's filesystem `metadata()` calls:
        /// `entries_for_shard` makes one for every entry it returns that the disk index does not
        /// hold, and for no others.
        returned: u64,
        /// The shard's whole cache, `namespace/record_key/selector`, before and after the arm.
        /// `entries_for_shard` already sorts by those three fields, so these compare directly.
        before: Vec<String>,
        after: Vec<String>,
    }

    impl CachePassArm {
        fn stepped_per_key(&self) -> f64 {
            if self.dropped == 0 {
                0.0
            } else {
                self.stepped as f64 / self.dropped as f64
            }
        }

        fn occupied(&self, listing: &[String], namespace: &str, key: &str) -> usize {
            let prefix = format!("{namespace}/{key}/");
            listing
                .iter()
                .filter(|entry| entry.starts_with(&prefix))
                .count()
        }
    }

    fn cache_listing(engine: &TemporalEngine) -> Vec<String> {
        engine
            .cache
            .entries_for_shard(1)
            .into_iter()
            .map(|entry| format!("{}/{}/{}", entry.namespace, entry.record_key, entry.selector))
            .collect()
    }

    /// The two arms, on two fixtures built by the same function from the same corpus size, handed
    /// the same set of dropped keys.
    fn cache_pass_arms(objects: usize) -> (CachePassArm, CachePassArm, Vec<String>, Vec<String>) {
        use super::CACHE_SWEEP_COUNTS as SWEEP;

        let marked: Vec<String> = [3usize, 17, objects - 1]
            .iter()
            .map(|index| format!("evict-scale-key-{index}"))
            .collect();
        // The keys a round would have dropped: the first half of the corpus. That holds two of
        // the three marked keys and leaves the third -- the last key in the corpus -- alone, so
        // the OVER-invalidation direction has a subject.
        let drop_keys: Vec<String> = (0..objects / 2)
            .map(|index| format!("evict-scale-key-{index}"))
            .collect();

        let per_key = {
            let (_dir, engine) = engine_with_marked_record_entries(objects, &marked);
            let before = cache_listing(&engine);
            SWEEP.reset();
            SWEEP.set_armed(true);
            for key in &drop_keys {
                super::invalidate_record_all(&engine.cache, 1, key, &SWEEP);
            }
            SWEEP.set_armed(false);
            let (_calls, walks, stepped, _named) = SWEEP.read();
            CachePassArm {
                arm: "N per-key sweeps",
                objects,
                dropped: drop_keys.len(),
                stepped,
                walks,
                returned: 0,
                before,
                after: cache_listing(&engine),
            }
        };

        let batched = {
            let (_dir, engine) = engine_with_marked_record_entries(objects, &marked);
            let before = cache_listing(&engine);
            let keys: Vec<std::sync::Arc<str>> = drop_keys
                .iter()
                .map(|key| std::sync::Arc::from(key.as_str()))
                .collect();
            SWEEP.reset();
            SWEEP.set_armed(true);
            super::invalidate_records_all_batched(&engine.cache, 1, &keys, &SWEEP);
            SWEEP.set_armed(false);
            let (_batched_calls, _keys_batched, walks, stepped, returned) = SWEEP.read_batched();
            CachePassArm {
                arm: "ONE batched pass",
                objects,
                dropped: drop_keys.len(),
                stepped,
                walks,
                returned,
                before,
                after: cache_listing(&engine),
            }
        };

        (per_key, batched, marked, drop_keys)
    }

    /// ONE PASS OVER THE CACHE, PRICED AGAINST THE N IT REPLACES, AND PROVED TO DROP THE SAME SET.
    ///
    /// #1907 established what the shipped shape costs and refused the obvious way out of it. Its
    /// closing paragraph names this change and states the obstacle: one batched pass inside the
    /// guard needs no visibility argument, but the only listing `MultiLayerCache` exposes is
    /// `entries_for_shard`, which does a filesystem `metadata()` per entry the disk index does not
    /// hold -- so it trades N cache walks for one cache walk plus C syscalls, inside the guard.
    /// This is that measurement, and the equivalence that has to come with it.
    ///
    /// THE TWO ARMS, ON MATCHED FIXTURES. Both are built by `engine_with_marked_record_entries`
    /// from the same corpus size and handed the same dropped-key set, and both are measured in
    /// the SAME unit: cache entries stepped over, read from the three tier lengths inside the
    /// primitive immediately before each walk. `note_sweep` does it for `invalidate_record` and
    /// `note_listing` does it for `entries_for_shard`, from the same three counters, so the ratio
    /// between the two arms is a ratio and not a comparison of two different quantities.
    ///
    /// EXACTLY THE SAME SET, and this is the correctness core rather than a sanity check. What is
    /// compared is not "did the dropped keys go" -- an arm that emptied the whole cache would pass
    /// that -- but the WHOLE shard listing afterwards, entry for entry, in `namespace/record_key/
    /// selector` form. Both arms match:
    ///
    ///   - `CacheKey::string(shard, key)`, selector "value"        -- named, O(1), per dropped key
    ///   - `CacheKey::set_members(shard, key)`, selector "members" -- named, O(1), per dropped key
    ///   - namespace `hash`, any selector, dropped record key      -- SWEPT
    ///   - namespace `feature`, any selector, dropped record key   -- SWEPT
    ///
    /// and nothing else. Both take the named pair from `named_record_keys` and the swept list
    /// from `SWEPT_RECORD_NAMESPACES`, so the enumeration is one list read twice rather than two
    /// lists that agree today.
    ///
    /// The set the batched arm builds is the same set because `entries_for_shard` chains the SAME
    /// three tier key sets that `invalidate_record` chains and applies the SAME shard filter,
    /// leaving the namespace and record-key halves of the filter to the predicate here; and
    /// because `CacheEntryInfo` carries `shard_id`, `namespace`, `record_key` and `selector`,
    /// which is every field of a `CacheKey`, so a matched entry is rebuilt into the key that was
    /// in the tier. At the layer below, `MultiLayerCache::invalidate` and `invalidate_batch` both
    /// end in `invalidate_keys_locked` -- the single-key form calls it with a one-element slice --
    /// so a batch is the same per-key removal under one lock acquisition instead of 4N.
    ///
    /// KEYS THE ROUND DID NOT DROP KEEP THEIR ENTRIES. `evict-scale-key-{objects - 1}` is marked,
    /// so it is cached in every namespace, and it is NOT in the dropped set. Both arms must leave
    /// every one of its entries alone, and the count is asserted equal to what it was before the
    /// arm ran -- not merely non-zero. This is the OVER-invalidation direction, and it is the one
    /// a batched predicate can fail: a pass that matched on namespace and forgot the record key
    /// would empty the whole namespace and still satisfy every "the dropped keys are gone" claim.
    ///
    /// THE POSITIVE CONTROL RUNS FIRST. Everything asserted after the arms is an emptiness or an
    /// equality claim, and two empty listings are equal. The entries have to be there first, per
    /// namespace, in both fixtures.
    #[test]
    fn one_batched_pass_walks_the_cache_once_instead_of_twice_per_dropped_key() {
        const SMALL: usize = 500;
        const LARGE: usize = 4000;

        let (small_per_key, small_batched, small_marked, small_dropped) = cache_pass_arms(SMALL);
        let (large_per_key, large_batched, large_marked, large_dropped) = cache_pass_arms(LARGE);

        let namespaces: Vec<String> = super::SWEPT_RECORD_NAMESPACES
            .iter()
            .map(|namespace| namespace.to_string())
            .chain(
                super::named_record_keys(1, "probe")
                    .iter()
                    .map(|key| key.namespace.to_string()),
            )
            .collect();


        // THE FLOOR ON THE DERIVED LIST, and it is not optional. Deriving the subject list from
        // the production authority is what stops it going stale -- but it also means a change
        // that REMOVES a namespace from the authority removes it from this test at the same
        // moment, and the test then passes by checking one thing fewer. So the list is derived
        // AND floored: these four have to be in it, and the count has to be at least four.
        // Adding a namespace is free; losing one fails here.
        for required in ["hash", "feature", "string", "set"] {
            assert!(
                namespaces.iter().any(|namespace| namespace == required),
                "`{required}` is no longer in the namespace list this test derives from \
                 SWEPT_RECORD_NAMESPACES and named_record_keys, so nothing here checks it any \
                 more; the list is {namespaces:?}",
            );
        }
        assert!(
            namespaces.len() >= 4,
            "the derived namespace list must cover at least the four the round has always \
             covered; it is {namespaces:?}",
        );

        let ratio = |per_key: &CachePassArm, batched: &CachePassArm| {
            if batched.stepped == 0 {
                0.0
            } else {
                per_key.stepped as f64 / batched.stepped as f64
            }
        };

        println!(
            "\n  ONE PASS OVER THE CACHE vs N PER-KEY SWEEPS, same fixture, same dropped keys\n\
             \n                                  {:>12} {:>12} {:>12} {:>12}\n\
               corpus objects              {:>12} {:>12} {:>12} {:>12}\n\
               cache entries BEFORE (outer){:>12} {:>12} {:>12} {:>12}\n\
               keys dropped                {:>12} {:>12} {:>12} {:>12}\n\
               walks over the cache        {:>12} {:>12} {:>12} {:>12}\n\
               CACHE ENTRIES STEPPED OVER  {:>12} {:>12} {:>12} {:>12}\n\
               .. per dropped key          {:>12.1} {:>12.1} {:>12.1} {:>12.1}\n\
               entries the listing RETURNED{:>12} {:>12} {:>12} {:>12}\n\
               cache entries AFTER  (outer){:>12} {:>12} {:>12} {:>12}\n\
             \n  ENTRY VISITS REMOVED: {:>8.1}x at {SMALL} objects, {:>8.1}x at {LARGE}\n",
            "per-key 500", "BATCHED 500", "per-key 4000", "BATCHED 4000",
            small_per_key.objects, small_batched.objects,
            large_per_key.objects, large_batched.objects,
            small_per_key.before.len(), small_batched.before.len(),
            large_per_key.before.len(), large_batched.before.len(),
            small_per_key.dropped, small_batched.dropped,
            large_per_key.dropped, large_batched.dropped,
            small_per_key.walks, small_batched.walks, large_per_key.walks, large_batched.walks,
            small_per_key.stepped, small_batched.stepped,
            large_per_key.stepped, large_batched.stepped,
            small_per_key.stepped_per_key(), small_batched.stepped_per_key(),
            large_per_key.stepped_per_key(), large_batched.stepped_per_key(),
            small_per_key.returned, small_batched.returned,
            large_per_key.returned, large_batched.returned,
            small_per_key.after.len(), small_batched.after.len(),
            large_per_key.after.len(), large_batched.after.len(),
            ratio(&small_per_key, &small_batched),
            ratio(&large_per_key, &large_batched),
        );

        let arms = [
            (&small_per_key, &small_batched, &small_marked, &small_dropped, SMALL),
            (&large_per_key, &large_batched, &large_marked, &large_dropped, LARGE),
        ];

        // THE POSITIVE CONTROL, per namespace, per arm, BEFORE anything is claimed about
        // emptiness. Two empty listings are equal, and a fixture that cached nothing would make
        // every claim below hold whatever either arm did.
        for (per_key, batched, marked, dropped, objects) in arms {
            for arm in [per_key, batched] {
                for namespace in &namespaces {
                    let held = arm.occupied(&arm.before, namespace, &marked[0]);
                    assert!(
                        held > 0,
                        "{objects} objects, {}: no `{namespace}` entry was cached for {}, so the \
                         claims below would hold whatever this arm did",
                        arm.arm,
                        marked[0],
                    );
                }
            }
            assert!(
                !dropped.is_empty() && dropped.contains(&marked[0]) && dropped.contains(&marked[1]),
                "the dropped set must contain the marked keys the emptiness claims are about",
            );
            assert!(
                !dropped.contains(&marked[2]),
                "the marked key {} must NOT be in the dropped set, or the over-invalidation \
                 direction has no subject",
                marked[2],
            );
        }

        // THE EQUIVALENCE. Not "the dropped keys are gone" -- an arm that emptied the cache would
        // satisfy that -- but the whole shard listing afterwards, entry for entry.
        for (per_key, batched, _marked, _dropped, objects) in arms {
            assert_eq!(
                per_key.before, batched.before,
                "{objects} objects: the two fixtures must start identical, or the listings \
                 compared below are of two different caches",
            );
            assert_eq!(
                per_key.after, batched.after,
                "{objects} objects: one batched pass must leave the cache in exactly the state \
                 {} per-key sweeps leave it in; {} entries vs {}",
                per_key.dropped,
                per_key.after.len(),
                batched.after.len(),
            );
            assert!(
                per_key.after.len() < per_key.before.len(),
                "{objects} objects: the arms must have removed something, or the equality above \
                 is an equality between two untouched caches: {} before, {} after",
                per_key.before.len(),
                per_key.after.len(),
            );
        }

        // WHAT WENT: every namespace the invalidation covers, for every dropped key, in BOTH
        // arms. Checked on the marked keys, which are the ones the fixture cached more than one
        // entry for.
        for (per_key, batched, marked, _dropped, objects) in arms {
            for arm in [per_key, batched] {
                for dropped_key in [&marked[0], &marked[1]] {
                    for namespace in &namespaces {
                        let left = arm.occupied(&arm.after, namespace, dropped_key);
                        assert_eq!(
                            left, 0,
                            "{objects} objects, {}: {left} `{namespace}` entries left cached for \
                             {dropped_key}, which the round dropped",
                            arm.arm,
                        );
                    }
                }
            }
        }

        // AND WHAT STAYED. The over-invalidation direction: a key the round did NOT drop keeps
        // every entry it had, in every namespace, in both arms. Asserted as EQUAL to the count
        // before the arm ran, not as non-zero -- an arm that dropped two of its three hash fields
        // would pass a non-zero check.
        for (per_key, batched, marked, _dropped, objects) in arms {
            for arm in [per_key, batched] {
                for namespace in &namespaces {
                    let before = arm.occupied(&arm.before, namespace, &marked[2]);
                    let after = arm.occupied(&arm.after, namespace, &marked[2]);
                    assert_eq!(
                        before, after,
                        "{objects} objects, {}: {} was not dropped, so its `{namespace}` entries \
                         must survive untouched; {before} before and {after} after",
                        arm.arm, marked[2],
                    );
                }
            }
        }

        // THE COST. One walk per round against two per dropped key, in the same unit, on matched
        // fixtures. Asserted as counts at both sizes; no time is involved.
        for (per_key, batched, _marked, _dropped, objects) in arms {
            assert_eq!(
                per_key.walks,
                (per_key.dropped as u64).saturating_mul(2),
                "{objects} objects: the per-key arm walks the cache twice per dropped key; \
                 {} walks for {} keys",
                per_key.walks,
                per_key.dropped,
            );
            assert_eq!(
                batched.walks, 1,
                "{objects} objects: the batched arm walks the cache exactly once, whatever it \
                 was handed; it walked {} times for {} keys",
                batched.walks,
                batched.dropped,
            );
            assert!(
                batched.stepped > 0 && per_key.stepped > 0,
                "{objects} objects: both arms must have stepped over something, or the ratio is \
                 a ratio of zeroes: per-key {} and batched {}",
                per_key.stepped,
                batched.stepped,
            );
            assert!(
                per_key.stepped > batched.stepped.saturating_mul(100),
                "{objects} objects: one pass must step over at least a hundredth of what {} \
                 per-key sweeps step over; per-key {} and batched {}",
                per_key.dropped,
                per_key.stepped,
                batched.stepped,
            );
        }

        // AND THE SHAPE OF THE SAVING, across the two sizes. The per-key arm's cost per dropped
        // key grows with the corpus -- that is what makes it a property of the STORE rather than
        // of the round -- and the batched arm's does not.
        assert!(
            large_per_key.stepped_per_key() > small_per_key.stepped_per_key() * 2.0,
            "the per-key arm's cost per dropped key must grow with the corpus: {:.1} at {SMALL} \
             and {:.1} at {LARGE}",
            small_per_key.stepped_per_key(),
            large_per_key.stepped_per_key(),
        );
        assert!(
            large_batched.stepped_per_key() <= small_batched.stepped_per_key() * 1.5,
            "one pass's cost per dropped key must NOT grow with the corpus: {:.1} at {SMALL} and \
             {:.1} at {LARGE}",
            small_batched.stepped_per_key(),
            large_batched.stepped_per_key(),
        );

        // THE SYSCALLS THE PASS BUYS THE SAVING WITH, bounded by a count from the same run.
        // `entries_for_shard` makes one filesystem `metadata()` for every entry it returns that
        // the disk index does not hold, so `returned` is the ceiling on them, and it is ONE cache
        // length per round rather than anything per key.
        for (_per_key, batched, _marked, _dropped, objects) in arms {
            assert!(
                batched.returned > 0,
                "{objects} objects: the listing returned nothing, so this probe has priced an \
                 operation on an empty cache",
            );
            assert!(
                batched.returned <= batched.before.len() as u64,
                "{objects} objects: the listing cannot return more entries than the shard's \
                 cache holds: returned {} of {}",
                batched.returned,
                batched.before.len(),
            );
        }
    }
}
