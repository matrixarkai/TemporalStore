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
//!     index complete, and move the blocks of the models that are not walked by key.
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

/// Whether the data node's draining stage runs a sliced pass. OFF unless `TS_SLICED_COMPACTION` is
/// `1` or `true`: each slice still walks the shard's keyed maps to find its keys and refreshes every
/// bucket, so a pass costs slices x shard size and a large shard waits far longer than it would for
/// a whole round. The whole round is the default until a slice costs what it moves.
pub(crate) fn sliced_compaction_enabled() -> bool {
    matches!(
        std::env::var("TS_SLICED_COMPACTION").as_deref(),
        Ok("1") | Ok("true")
    )
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
    /// Keys still to move, the next ones at the end.
    remaining_keys: Vec<crate::engine::state::ModelKey>,
    unkeyed_done: bool,
    /// Container blocks that were in the log before a slice moved them, and the placement each had
    /// then. The finish retires a registration only if it is still that placement.
    retired_container_wal_blocks: Vec<(u64, String)>,
    wal_placements_seen: HashMap<u64, (u64, u64)>,
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
                    // Let a writer that queued behind the slice take the guard before the next one.
                    std::thread::yield_now();
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
            match self.begin_pass(
                shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
                drain_block_slab_ids,
            )? {
                Some(pass) => *slot = Some(pass),
                None => return Ok(SliceProgress::Done),
            }
        }
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
        );
        pass.note_slice(shard, stats);
        let pass_ended = moved.is_err() || pass.remaining_keys.is_empty();
        if !pass_ended {
            return Ok(SliceProgress::More);
        }
        let pass = slot.take().expect("the pass is in flight");
        match moved {
            Err(err) => Err(self.fail_pass(shard_id, shard, pass, err)),
            Ok(()) => self
                .finish_pass(shard_id, shard, pass, start_routing_bucket, end_routing_bucket)
                .map(|()| SliceProgress::Done),
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
                "block_compaction_owner_mismatch",
                format!(
                    "refusing compaction because {} live block refs disagree with object/block/slot ownership",
                    ownership.mismatches.len()
                ),
            ));
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
        let skippable = |kind: StoredModelKind| matches!(
            kind,
            StoredModelKind::String | StoredModelKind::Hash | StoredModelKind::Set
                | StoredModelKind::Zset | StoredModelKind::List
        );
        let mut remaining_keys = entries
            .iter()
            .filter(|entry| {
                skippable(entry.kind) && drain_block_slab_ids.contains(&entry.address.block_slab_id())
            })
            .map(|entry| entry.object_key.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        remaining_keys.sort();
        remaining_keys.reverse();
        drop(entries);
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
        storage_bucket_internals::promote_model_maps_to_bucket_index_authority(
            shard_id, shard, start_routing_bucket, end_routing_bucket,
        );
        Ok(Some(SlicedPass {
            target_block_slab_id,
            drain_block_slab_ids: drain_block_slab_ids.clone(),
            pre_round_slabs,
            remaining_keys,
            unkeyed_done: false,
            retired_container_wal_blocks: Vec::new(),
            wal_placements_seen: HashMap::new(),
            rewritten_block_refs: 0,
            bytes_written_before_relocation,
        }))
    }

    /// Move one slice's blocks: the unkeyed models on the first slice, then the next keys.
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
        if !pass.unkeyed_done {
            pass.unkeyed_done = true;
            self.relocate_unkeyed_blocks(
                shard_id, shard, start_routing_bucket, end_routing_bucket, stats,
            )?;
            return Self::unreadable_blocks_failure(stats);
        }
        let take = pass.remaining_keys.len().min(keys_per_slice.max(1));
        let keys: HashSet<crate::engine::state::ModelKey> = pass
            .remaining_keys
            .split_off(pass.remaining_keys.len() - take)
            .into_iter()
            .collect();
        self.relocate_keyed_blocks(
            shard_id,
            shard,
            start_routing_bucket,
            end_routing_bucket,
            &|key: &str| keys.contains(key),
            stats,
        )?;
        Self::unreadable_blocks_failure(stats)
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
        // image is cut, as the setup did before anything moved.
        storage_bucket_internals::promote_model_maps_to_bucket_index_authority(
            shard_id, shard, start_routing_bucket, end_routing_bucket,
        );
        if let Err(barrier) = self.compaction_barrier() {
            self.pin_unpublished_compaction(shard_id, &pass.pre_round_slabs);
            return Err(Status::error(
                "block_compaction_failed",
                format!(
                    "durability barrier failed before publishing the compacted base index: {barrier}"
                ),
            ));
        }
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
        let index_bytes = self.publish_compacted_container_index(
            shard_id, shard, &stats, pass.target_block_slab_id,
        )?;
        self.clear_unpublished_compaction(shard_id);
        let _ = self.index_log_store.append_index_bytes(shard_id, &index_bytes);
        Ok(())
    }
}

impl SlicedPass {
    /// Fold one slice's outcome into the pass, and record the placement each container block the
    /// slice retired had in the log when it did, so the finish can tell a registration a writer has
    /// since replaced from one nobody has touched.
    fn note_slice(&mut self, shard: &ShardState, stats: CompactionRewriteStats) {
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
