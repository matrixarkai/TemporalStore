// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A compaction pass in slices: the shard write guard is held for one slice, not for the pass.
//!
//! A whole round holds the guard from validation to the published index, so every writer of the
//! shard waits for all of it. A sliced pass does the same work in steps and lets go of the guard
//! between them:
//!
//!   * SETUP (one guard hold): validate ownership, name the keys that own a block on a drain slab,
//!     roll the slab the pass fills, pin the slabs the durable index still names, make the bucket
//!     index complete.
//!   * EACH SLICE (one guard hold): move the blocks of the next `keys_per_slice` keys and update the
//!     bucket index for the blocks moved.
//!   * FINISH (one guard hold): make the bucket index complete again, fsync the moved bytes, publish
//!     the index, release the pin.
//!
//! THE INVARIANT AT EVERY POINT WHERE THE GUARD IS FREE: every key reads at its old address or its
//! new one, and the bucket index, the object block lookup and the per-slab live tally agree with
//! the model maps. A slice ends only after the in-place update for the blocks it moved, so a reader
//! or a writer that arrives between slices finds a shard no different from one between two writes.
//!
//! WHAT KEEPS THE DURABLE IMAGE SAFE WHILE THE GUARD IS FREE: the durable index still names the old
//! slabs and the volatile one does not, so reclaim must not purge them. The pin
//! (`unpublished_compaction_slabs`) holds every slab the durable index may name from setup until the
//! finish has published. A crash between slices loses nothing: the old slabs exist and the old
//! index names only them.
//!
//! WHAT A WRITER CAN DO BETWEEN SLICES, AND WHY IT IS HARMLESS. A write appends to the slab the pass
//! fills or a later one and files its own index entry; it moves no block of the drain set. A key
//! deleted between slices simply has no blocks left for a later slice to find. A container element
//! rewritten between slices gets a fresh registration of its own in the log, and the finish retires
//! only the registrations a slice saw and that are still the ones the slice saw.

use super::*;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// How far a slice got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SliceProgress {
    /// Work remains; the guard is free until the next slice.
    More,
    /// The pass is over: the index is published and the pin released, or there was nothing to move.
    Done,
}

/// One line per phase of a pass, to stderr, when `TS_COMPACTION_PHASE_TIMING=1`: the duration of the
/// setup, of each slice and of the finish, each of which holds the shard write guard once. Off by
/// default; the read is one cached env lookup.
fn phase_timing(phase: &str, shard_id: ShardId, started: std::time::Instant) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let on = *ON.get_or_init(|| {
        matches!(std::env::var("TS_COMPACTION_PHASE_TIMING").as_deref(), Ok("1") | Ok("true"))
    });
    if on {
        eprintln!(
            "COMPACTION_PHASE shard={shard_id} phase={phase} us={}",
            started.elapsed().as_micros()
        );
    }
}

/// Keys a slice moves when the caller does not say.
pub(crate) const COMPACTION_SLICE_KEYS: usize = 128;

/// One shard's pass in flight. Memory only: a restart starts a new pass, which is safe because a
/// pass only moves blocks that are still on a drain slab.
#[derive(Debug)]
pub(crate) struct SlicedPass {
    target_block_slab_id: u64,
    drain_block_slab_ids: BTreeSet<u64>,
    /// Every slab the durable index may name when the pass began.
    pre_round_slabs: BTreeSet<u64>,
    /// Keys still to move, the next ones at the end. Used only by a pass that could not take the
    /// bucket cursor (see `by_bucket`).
    remaining_keys: Vec<crate::engine::state::ModelKey>,
    /// THE CURSOR PASS. When the per-slab tally is ready and no bucket is released, the pass names no
    /// key up front: each slice walks the next buckets after `bucket_cursor`, picks the keys that own
    /// a block on a drain slab, validates those buckets' ownership and moves the keys. Setup is then
    /// O(slabs), not O(shard), and a restart of the pass resumes from a bucket id, not a key list.
    by_bucket: bool,
    bucket_cursor: Option<u32>,
    pending_bucket_owners: Vec<(crate::engine::state::ModelKey, bool)>,
    buckets_exhausted: bool,
    /// Container blocks that were in the log before a slice moved them, and the placement each had
    /// then. The finish retires a registration only if it is still that placement.
    retired_container_wal_blocks: Vec<(u64, String)>,
    wal_placements_seen: HashMap<u64, (u64, u64)>,
    /// Every block a slice moved that the log was holding, any address kind, with the placement it
    /// had at that slice. The finish passes on the ones whose placement is still that one.
    relocated_wal_blocks: Vec<super::compaction::RelocatedWalBlock>,
    rewritten_block_refs: usize,
    bytes_written_before_relocation: u64,
}

pub(crate) type PassSlot = Arc<Mutex<Option<SlicedPass>>>;

#[cfg(test)]
thread_local! {
    /// Called with the engine at each slice boundary, with no guard held.
    static SLICE_BOUNDARY: std::cell::RefCell<Option<Box<dyn FnMut(&TemporalEngine)>>> =
        std::cell::RefCell::new(None);
}

/// Install (or clear) the callback run between slices of the passes this thread drives.
#[cfg(test)]
pub(crate) fn set_slice_boundary_for_test(hook: Option<Box<dyn FnMut(&TemporalEngine)>>) {
    SLICE_BOUNDARY.with(|cell| *cell.borrow_mut() = hook);
}

impl TemporalEngine {
    fn pass_slot(&self, shard_id: ShardId) -> PassSlot {
        Arc::clone(
            self.compaction_passes
                .lock()
                .expect("compaction pass registry poisoned")
                .entry(shard_id)
                .or_default(),
        )
    }

    /// Whether a sliced pass is in flight on this shard, for a caller that steps one.
    #[cfg(test)]
    pub(crate) fn compaction_pass_in_flight(&self, shard_id: ShardId) -> bool {
        self.sliced_pass_active(shard_id)
    }

    /// Whether a full promote scan has already confirmed this shard's bucket index in step with its
    /// model maps AND the write path keeps it so: the same test the per-command scan uses to skip
    /// itself (`flat_append` and `promote_scan_done`). `promote_scan_done` is not serialized, so a
    /// freshly loaded shard answers `false` until its first command pays one scan; a compaction that
    /// meets such a shard still walks, as before.
    fn bucket_index_is_confirmed_authoritative(&self, shard: &ShardState) -> bool {
        self.wal_store.flat_append() && shard.promote_scan_done
    }

    /// Whether a sliced pass owns this shard's compaction, so a whole round must not start.
    pub(super) fn sliced_pass_active(&self, shard_id: ShardId) -> bool {
        let slot = self.pass_slot(shard_id);
        let active = slot.try_lock().map(|pass| pass.is_some()).unwrap_or(true);
        active
    }

    /// Drain the named slabs in slices of `keys_per_slice` keys, releasing the shard write guard
    /// between slices. Returns when the pass has published, or on the first failure.
    pub(crate) fn compact_shard_blocks_draining_sliced(
        &self,
        shard_id: ShardId,
        drain_block_slab_ids: BTreeSet<u64>,
        keys_per_slice: usize,
    ) -> Result<(), Status> {
        loop {
            match self.compact_shard_blocks_slice(shard_id, &drain_block_slab_ids, keys_per_slice)? {
                SliceProgress::Done => return Ok(()),
                SliceProgress::More => {
                    #[cfg(test)]
                    {
                        let hook = SLICE_BOUNDARY.with(|cell| cell.borrow_mut().take());
                        if let Some(mut hook) = hook {
                            hook(self);
                            SLICE_BOUNDARY.with(|cell| *cell.borrow_mut() = Some(hook));
                        }
                    }
                    // A yield alone can reacquire the lock before a woken writer runs. Give
                    // queued foreground work a scheduling window between blocking-pass slices.
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
            }
        }
    }

    /// One step of a pass: setup if none is in flight, otherwise the next keys; the finish when
    /// none remain.
    pub(crate) fn compact_shard_blocks_slice(
        &self,
        shard_id: ShardId,
        drain_block_slab_ids: &BTreeSet<u64>,
        keys_per_slice: usize,
    ) -> Result<SliceProgress, Status> {
        let slot = self.pass_slot(shard_id);
        let mut slot = slot.lock().expect("compaction pass lock poisoned");
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("shard info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards_write_marked();
        let Some(shard) = shards.get_mut(&shard_id) else {
            *slot = None;
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        if slot.is_none() {
            let started = std::time::Instant::now();
            let begun = self.begin_pass(
                shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
                drain_block_slab_ids,
            )?;
            phase_timing("setup", shard_id, started);
            match begun {
                Some(pass) => *slot = Some(pass),
                None => return Ok(SliceProgress::Done),
            }
        }
        let slice_started = std::time::Instant::now();
        let pass = slot.as_mut().expect("a pass was just begun");
        let mut stats = CompactionRewriteStats::for_drain_round(
            pass.target_block_slab_id,
            u64::MAX,
            usize::MAX,
            pass.drain_block_slab_ids.clone(),
        );
        let moved = self.run_slice(
            shard_id,
            shard,
            start_routing_bucket,
            end_routing_bucket,
            pass,
            keys_per_slice,
            &mut stats,
        );
        // The index is brought up to date for what the slice moved even when it failed part way:
        // the blocks it did move are where the maps say, and the index must say the same.
        update_compacted_bucket_views(
            shard_id, shard, start_routing_bucket, end_routing_bucket, &stats.moved,
            CompactionIndexRefresh::ChangedBuckets,
        );
        pass.note_slice(shard_id, shard, stats);
        phase_timing("slice", shard_id, slice_started);
        let pass_ended = moved.is_err()
            || if pass.by_bucket {
                pass.buckets_exhausted
            } else {
                pass.remaining_keys.is_empty()
            };
        if !pass_ended {
            return Ok(SliceProgress::More);
        }
        let pass = slot.take().expect("the pass is in flight");
        match moved {
            Err(err) => Err(self.fail_pass(shard_id, shard, pass, err)),
            Ok(()) => {
                let started = std::time::Instant::now();
                let finished = self
                    .finish_pass(shard_id, shard, pass, start_routing_bucket, end_routing_bucket)
                    .map(|()| SliceProgress::Done);
                phase_timing("finish", shard_id, started);
                finished
            }
        }
    }

    /// Validate, name the keys, roll or resume the slab, pin. `None` when no block sits on a drain
    /// slab, which needs no pass at all.
    fn begin_pass(
        &self,
        shard_id: ShardId,
        shard: &mut ShardState,
        start_routing_bucket: u32,
        end_routing_bucket: u32,
        drain_block_slab_ids: &BTreeSet<u64>,
    ) -> Result<Option<SlicedPass>, Status> {
        let bytes_written_before_relocation = self.block_store.stats().bytes_written;
        let by_bucket = shard.bucket_index.block_slab_live.is_ready()
            && shard.bucket_index.released_buckets.is_empty()
            && !shard.bucket_index.bucket_map.is_empty();
        if by_bucket { shard.bucket_index.ensure_bucket_owners(); }
        let (pre_round_slabs, remaining_keys) = if by_bucket {
            // THE GATE AND THE PIN COME FROM THE MAINTAINED TALLY: O(slabs). No block is walked
            // here; the slices walk the buckets they reach.
            let tally = &shard.bucket_index.block_slab_live;
            if !drain_block_slab_ids
                .iter()
                .any(|slab| tally.tally(*slab).block_refs > 0)
            {
                return Ok(None);
            }
            // THE TALLY ONLY SKIPS WORK, IT NEVER AUTHORISES ONE. An under-counted tally would leave
            // a slab the durable index names out of the pin, and reclaim could then purge it. So the
            // pin is the tally's slabs UNION every slab the block store holds: a superset, O(slabs).
            let mut pinned = tally.live_slab_ids().collect::<BTreeSet<u64>>();
            pinned.extend(self.block_store.slab_ids().unwrap_or_default());
            (pinned, Vec::new())
        } else {
            let entries = collect_live_block_entries(shard);
            let ownership = validate_bucket_ownership_index_from_entries(
                shard_id,
                shard,
                &entries,
                start_routing_bucket,
                end_routing_bucket,
            );
            if !ownership.mismatches.is_empty() {
                return Err(Self::owner_mismatch_failure(ownership.mismatches.len()));
            }
            if !entries
                .iter()
                .any(|entry| drain_block_slab_ids.contains(&entry.address.block_slab_id()))
            {
                return Ok(None);
            }
            let pre_round_slabs = entries
                .iter()
                .map(|entry| entry.address.block_slab_id())
                .collect::<BTreeSet<u64>>();
            let mut remaining_keys = entries
                .iter()
                .filter(|entry| {
                    drain_block_slab_ids.contains(&entry.address.block_slab_id())
                })
                .map(|entry| entry.object_key.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            remaining_keys.sort();
            remaining_keys.reverse();
            (pre_round_slabs, remaining_keys)
        };
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
                    .map_err(|err| Status::error("block_compaction_failed", err.to_string()))?;
                (roll.previous_block_slab_id, roll.new_block_slab_id)
            }
        };
        self.compaction_rounds
            .write()
            .expect("compaction round lock poisoned")
            .insert(shard_id, (previous_block_slab_id, target_block_slab_id));
        self.pin_unpublished_compaction(shard_id, &pre_round_slabs);
        // The bucket index must name every live block before any block moves; see the whole round.
        // The check walks every live block of the shard, so it is skipped while the write path has
        // already confirmed the index authoritative (`bucket_index_is_confirmed_authoritative`).
        if !self.bucket_index_is_confirmed_authoritative(shard) {
            storage_bucket_internals::promote_model_maps_to_bucket_index_authority(
                shard_id, shard, start_routing_bucket, end_routing_bucket,
            );
        }
        Ok(Some(SlicedPass {
            target_block_slab_id,
            drain_block_slab_ids: drain_block_slab_ids.clone(),
            pre_round_slabs,
            remaining_keys,
            by_bucket,
            bucket_cursor: None,
            pending_bucket_owners: Vec::new(),
            buckets_exhausted: false,
            retired_container_wal_blocks: Vec::new(),
            wal_placements_seen: HashMap::new(),
            relocated_wal_blocks: Vec::new(),
            rewritten_block_refs: 0,
            bytes_written_before_relocation,
        }))
    }

    /// Move the selected owners across every model, without a whole-shard first slice.
    #[allow(clippy::too_many_arguments)]
    fn run_slice(
        &self,
        shard_id: ShardId,
        shard: &mut ShardState,
        start_routing_bucket: u32,
        end_routing_bucket: u32,
        pass: &mut SlicedPass,
        keys_per_slice: usize,
        stats: &mut CompactionRewriteStats,
    ) -> Result<(), Status> {
        let keys: HashSet<crate::engine::state::ModelKey> = if pass.by_bucket {
            Self::next_bucket_keys(
                shard_id, shard, start_routing_bucket, end_routing_bucket, pass, keys_per_slice,
            )?
        } else {
            let take = pass.remaining_keys.len().min(keys_per_slice.max(1));
            pass.remaining_keys
                .split_off(pass.remaining_keys.len() - take)
                .into_iter()
                .collect()
        };
        if keys.is_empty() {
            return Ok(());
        }
        self.relocate_keyed_blocks(
            shard_id,
            shard,
            start_routing_bucket,
            end_routing_bucket,
            Some(&keys),
            stats,
        )?;
        self.relocate_unkeyed_blocks(
            shard_id, shard, start_routing_bucket, end_routing_bucket, Some(&keys), stats,
        )?;
        Self::unreadable_blocks_failure(stats)
    }

    fn owner_mismatch_failure(mismatches: usize) -> Status {
        Status::error(
            "block_compaction_owner_mismatch",
            format!(
                "refusing compaction because {mismatches} live block refs disagree with object/block/slot ownership"
            ),
        )
    }

    /// Examine at most the requested number of owners, retaining a bucket's unvisited owners
    /// for later slices. Validate each bucket before moving any of its blocks.
    fn next_bucket_keys(
        shard_id: ShardId,
        shard: &ShardState,
        start_routing_bucket: u32,
        end_routing_bucket: u32,
        pass: &mut SlicedPass,
        keys_per_slice: usize,
    ) -> Result<HashSet<crate::engine::state::ModelKey>, Status> {
        let budget = keys_per_slice.max(1);
        let mut examined = 0usize;
        let mut keys = HashSet::new();
        while examined < budget {
            if let Some((key, on_drain)) = pass.pending_bucket_owners.pop() {
                examined += 1;
                if on_drain { keys.insert(key); }
                continue;
            }
            let Some((routing_bucket, bucket)) = shard
                .bucket_index
                .bucket_map
                .range_after(pass.bucket_cursor)
                .next()
            else {
                pass.buckets_exhausted = true;
                break;
            };
            pass.bucket_cursor = Some(*routing_bucket);
            let mut entries = Vec::new();
            storage_bucket_internals::visit_owned_bucket_live_block_entries(
                shard, routing_bucket, bucket,
                |entry| entries.push(entry),
            );
            let ownership = validate_bucket_ownership_index_from_entries(
                shard_id, shard, &entries, start_routing_bucket, end_routing_bucket,
            );
            if !ownership.mismatches.is_empty() {
                return Err(Self::owner_mismatch_failure(ownership.mismatches.len()));
            }
            let mut owners = std::collections::BTreeMap::new();
            for entry in entries {
                let on_drain = pass.drain_block_slab_ids.contains(&entry.address.block_slab_id());
                owners.entry(entry.object_key)
                    .and_modify(|held| *held |= on_drain).or_insert(on_drain);
            }
            if owners.is_empty() { examined += 1; }
            pass.pending_bucket_owners = owners.into_iter().rev().collect();
        }
        Ok(keys)
    }

    /// A slice failed part way. Commit the consistent partial state the way a whole round does, keep
    /// the anchor so the next pass resumes on the same slab, and hand back the original error.
    fn fail_pass(
        &self,
        shard_id: ShardId,
        shard: &mut ShardState,
        pass: SlicedPass,
        err: Status,
    ) -> Status {
        if let Err(barrier) = self.compaction_barrier() {
            self.pin_unpublished_compaction(shard_id, &pass.pre_round_slabs);
            return Status::error(
                "block_compaction_failed",
                format!(
                    "durability barrier failed while committing a partial compaction: {barrier}"
                ),
            );
        }
        match self.publish_pass(shard_id, shard, &pass) {
            Ok(()) => err,
            Err(publish) => {
                self.pin_unpublished_compaction(shard_id, &pass.pre_round_slabs);
                publish
            }
        }
    }

    /// Every slice is done: complete the index, fsync, publish, release the pin and the anchor.
    fn finish_pass(
        &self,
        shard_id: ShardId,
        shard: &mut ShardState,
        pass: SlicedPass,
        start_routing_bucket: u32,
        end_routing_bucket: u32,
    ) -> Result<(), Status> {
        // Blocks written between slices may be missing from the bucket index; complete it before the
        // image is cut, as the setup did before anything moved. A confirmed-authoritative index is
        // kept complete by every write, so the walk is only owed when it is not.
        if !self.bucket_index_is_confirmed_authoritative(shard) {
            storage_bucket_internals::promote_model_maps_to_bucket_index_authority(
                shard_id, shard, start_routing_bucket, end_routing_bucket,
            );
        }
        let barrier_started = std::time::Instant::now();
        if let Err(barrier) = self.compaction_barrier() {
            self.pin_unpublished_compaction(shard_id, &pass.pre_round_slabs);
            return Err(Status::error(
                "block_compaction_failed",
                format!(
                    "durability barrier failed before publishing the compacted base index: {barrier}"
                ),
            ));
        }
        phase_timing("finish_barrier", shard_id, barrier_started);
        if let Err(publish) = self.publish_pass(shard_id, shard, &pass) {
            self.pin_unpublished_compaction(shard_id, &pass.pre_round_slabs);
            return Err(publish);
        }
        self.compaction_rounds
            .write()
            .expect("compaction round lock poisoned")
            .remove(&shard_id);
        if pass.rewritten_block_refs > 0 {
            self.block_store
                .mark_write_window_at_compaction(pass.bytes_written_before_relocation);
        }
        Ok(())
    }

    /// Publish the index the pass has built and release the pin. The registrations retired are the
    /// ones no writer has replaced since a slice saw them.
    fn publish_pass(
        &self,
        shard_id: ShardId,
        shard: &mut ShardState,
        pass: &SlicedPass,
    ) -> Result<(), Status> {
        // Publication owes complete flags and TTL rollups; intermediate slices do not.
        let flags_started = std::time::Instant::now();
        refresh_bucket_runtime_flags(shard);
        phase_timing("finish_flags", shard_id, flags_started);
        let publish_started = std::time::Instant::now();
        let mut stats = CompactionRewriteStats::for_drain_round(
            pass.target_block_slab_id,
            u64::MAX,
            usize::MAX,
            pass.drain_block_slab_ids.clone(),
        );
        stats.retired_container_wal_blocks = pass
            .retired_container_wal_blocks
            .iter()
            .filter(|(object_id, element_name)| {
                let key = crate::engine::block_in_wal::wal_resident_key(*object_id, Some(element_name));
                let now = shard
                    .wal_resident_blocks
                    .get(&key)
                    .map(|placement| (placement.log_id, placement.sequence));
                now == pass.wal_placements_seen.get(&key).copied()
            })
            .cloned()
            .collect();
        // THE BLOCKS THE LOG WAS HOLDING THAT THIS PASS TOOK OUT, those whose placement is still the
        // one a slice saw. A registration a writer has replaced since names a newer record, which the
        // pass did not move and must not be told it may retire.
        stats.relocated_wal_blocks = pass
            .relocated_wal_blocks
            .iter()
            .filter(|block| {
                let key = crate::engine::block_in_wal::wal_resident_key(
                    block.object_id,
                    block.element_name.as_deref(),
                );
                shard
                    .wal_resident_blocks
                    .get(&key)
                    .is_some_and(|now| now.log_id == block.from_log_id && now.sequence == block.from_sequence)
            })
            .cloned()
            .collect();
        let index_bytes = self.publish_compacted_container_index(
            shard_id, shard, &stats, pass.target_block_slab_id,
        )?;
        phase_timing("finish_publish", shard_id, publish_started);
        self.clear_unpublished_compaction(shard_id);
        let anchor_started = std::time::Instant::now();
        let _ = self.index_log_store.append_index_bytes(shard_id, &index_bytes);
        phase_timing("finish_anchor", shard_id, anchor_started);
        Ok(())
    }
}

impl SlicedPass {
    /// Fold one slice's outcome into the pass, and record the placement each container block the
    /// slice retired had in the log when it did, so the finish can tell a registration a writer has
    /// since replaced from one nobody has touched.
    fn note_slice(&mut self, shard_id: ShardId, shard: &ShardState, mut stats: CompactionRewriteStats) {
        stats.note_relocated_wal_blocks(shard_id, shard);
        self.relocated_wal_blocks.append(&mut stats.relocated_wal_blocks);
        for (object_id, element_name) in &stats.retired_container_wal_blocks {
            let key = crate::engine::block_in_wal::wal_resident_key(*object_id, Some(element_name));
            if let Some(placement) = shard.wal_resident_blocks.get(&key) {
                self.wal_placements_seen
                    .entry(key)
                    .or_insert((placement.log_id, placement.sequence));
            }
        }
        self.retired_container_wal_blocks.extend(stats.retired_container_wal_blocks);
        self.rewritten_block_refs += stats.rewritten_block_refs;
    }
}
