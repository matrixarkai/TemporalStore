// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT AN OVERWRITE RECORDS ABOUT WHAT IT RETIRED, and what that makes reportable.
//!
//! # THE THING THAT DID NOT EXIST
//!
//! Liveness was tracked exactly and staleness was never recorded. `insert_block` and `remove` both
//! discharge `block_slab_live`, and that tally is right to the byte -- 2,000 keys x 256 B holds at
//! 536,000 B across five rewrites, which is precisely the live set. But nothing wrote down what
//! LEFT, so garbage was only derivable as `logical_ever - live` per slab, and only after a
//! storage-manager cycle had published the union tally. Before that publish every slab reported
//! `live 0, garbage 10000 bp` -- a slab holding live data reading as entirely garbage.
//!
//! Recording the displaced range SYNCHRONOUSLY, in the call that displaces the entry, is what
//! makes the figure available at the moment the waste is created rather than after a cycle. This
//! arm is the evidence that it is recorded there, that the whole GC control vocabulary derives from
//! the list, and that the figure moves.
//!
//! # FOUR CLAIMS, AND THE BOUND IS ONE OF THEM
//!
//!   1. the range is recorded where the live tally is discharged, so the two cannot disagree;
//!   2. the list is BOUNDED -- ranges merge, the per-slab count is capped, and a discarded slab's
//!      detail leaves with the slab. An append-per-overwrite list grows with the write RATE and
//!      never with the data, which is the shape the changefeed already has in this tree;
//!   3. the cause is attributed, so rewrite pressure is distinguishable from a shrinking live set;
//!   4. space amplification computed from the record moves 1.000x -> 5.000x under five writes of
//!      every key, where the published `space_amplification_ppm` stays pinned at 1.000x because its
//!      denominator counts a live slab's whole file.
//!
//! # THE VALUES ARE INCOMPRESSIBLE, AND EACH ROUND WRITES DIFFERENT BYTES
//!
//! `vec![b'v'; n]` compresses to almost nothing, which makes stored bytes framing rather than
//! content: on that fixture the live figure reads the same 70,000 B at value 256, 1,024 and 4,096
//! alike, so a stored-byte denominator stops tracking the data. And a rewrite with IDENTICAL bytes
//! is a workload a store is allowed to recognise and skip, which would make the rewrite rounds
//! measure deduplication. Every figure here is in stored bytes and every round writes new ones.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_overwrite_records_what_it_retired
//!   cargo test -p temporalstore-rust --lib the_space_figure_moves_at_the_overwrite -- --ignored --nocapture

use crate::block_store::ElementEntry;
use crate::engine::state::{
    BlockSlabLiveIndex, StaleCause, MIN_DISCARD_RANGE_BYTES, STALE_RANGES_PER_SLAB,
    STALE_SLABS_TRACKED,
};

/// An entry naming a byte range on a slab, which is all the record reads off it.
fn entry(block_slab_id: u64, offset: u64, length: u64) -> ElementEntry {
    ElementEntry::from_parts(block_slab_id, offset, length, Some(0), None)
}

/// Charge a block to the live tally and then retire it, which is the pair every production path
/// makes. Charging first matters: a slab with no live tally takes `remove_address`'s early return,
/// and an arm that skipped the charge would be testing that path by accident.
fn charge_then_retire(
    live: &mut BlockSlabLiveIndex,
    block_slab_id: u64,
    offset: u64,
    length: u64,
    cause: StaleCause,
) {
    let entry = entry(block_slab_id, offset, length);
    live.add_address(&entry);
    live.remove_address(&entry, cause);
}

#[test]
fn the_overwrite_records_what_it_retired() {
    let mut live = BlockSlabLiveIndex::default();
    for block in 0..10_u64 {
        charge_then_retire(&mut live, 7, block * 100, 100, StaleCause::Overwrite);
    }

    let tally = live.stale_tally(7);
    assert_eq!(
        tally.block_refs, 10,
        "ten blocks retired off slab 7 and the record counts {} -- the ref count is EXACT and \
         nothing but a prune may reduce it",
        tally.block_refs
    );
    assert_eq!(
        tally.bytes, 1_000,
        "ten 100 B blocks retired and the record holds {} B. The byte figure sums \
         `ElementEntry::length`, the same unit the live tally is in, so a ratio between them means \
         something only if this is exact",
        tally.bytes
    );
    // ---- THE MERGE IS WHAT BOUNDS THE LIST. Ten contiguous ranges collapse to one, so the count
    // ---- grows with how FRAGMENTED the garbage is and not with how much of it there is.
    assert_eq!(
        tally.range_count(),
        1,
        "ten CONTIGUOUS ranges were recorded and the list holds {} of them. Without a merge this \
         list grows once per overwrite forever, which is the changefeed shape and is worse than no \
         list at all",
        tally.range_count()
    );
    let merged = tally.ranges()[0];
    assert_eq!(
        (merged.offset, merged.length),
        (0, 1_000),
        "the merged range reads offset {} length {}, and the ten blocks covered 0..1000 exactly. A \
         merge that moved either end would claim bytes that did not retire",
        merged.offset,
        merged.length
    );
    assert_eq!(tally.dropped_range_count(), 0, "nothing came near the cap here");
}

#[test]
fn a_retired_range_between_two_others_bridges_them() {
    // THE ORDER IS THE POINT. Retiring the MIDDLE block last makes all three one range, and a
    // merge that only extended the left neighbour would leave the right one covering bytes the
    // left one now also covers -- double-counting the same garbage and reporting fragmentation
    // that had just been closed.
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 3, 0, 100, StaleCause::Overwrite);
    charge_then_retire(&mut live, 3, 200, 100, StaleCause::Overwrite);
    assert_eq!(
        live.stale_tally(3).range_count(),
        2,
        "two ranges with 100 B of live block between them must not merge"
    );
    charge_then_retire(&mut live, 3, 100, 100, StaleCause::Overwrite);
    let tally = live.stale_tally(3);
    assert_eq!(
        tally.range_count(),
        1,
        "the block between two recorded ranges retired and the list holds {} ranges. The three \
         cover 0..300 with no hole, so they are one range",
        tally.range_count()
    );
    assert_eq!((tally.ranges()[0].offset, tally.ranges()[0].length), (0, 300));
    assert_eq!(tally.bytes, 300, "three 100 B blocks, and the byte figure is independent of merging");
}

#[test]
fn ranges_that_retired_for_different_reasons_do_not_merge() {
    // The cause is a FIELD of the range, so a merged range would have to pick one cause and lie
    // about the other. Two touching ranges with different causes are therefore kept apart, which
    // is what keeps the attribution exact at the cost of a range.
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 1, 0, 100, StaleCause::Overwrite);
    charge_then_retire(&mut live, 1, 100, 100, StaleCause::Removed);
    charge_then_retire(&mut live, 1, 200, 100, StaleCause::Removed);
    let tally = live.stale_tally(1);
    assert_eq!(
        tally.range_count(),
        2,
        "an overwritten range touching a deleted one must stay separate, and the two deleted ones \
         must merge: expected 2 ranges, found {}",
        tally.range_count()
    );
    let summary = live.stale_summary();
    assert_eq!(
        (summary.overwrite_bytes, summary.removed_bytes, summary.swept_bytes),
        (100, 200, 0),
        "the cause split reads {:?}, and it is what separates rewrite pressure from a live set \
         that simply got smaller -- the same pending total means opposite things",
        (summary.overwrite_bytes, summary.removed_bytes, summary.swept_bytes)
    );
    assert_eq!(
        summary.stale_bytes,
        summary.overwrite_bytes + summary.removed_bytes + summary.swept_bytes,
        "the window total must be the sum of its causes"
    );
}

#[test]
fn the_range_list_is_capped_and_says_so_while_the_bytes_stay_exact() {
    // EVERY RANGE NON-ADJACENT, so none of them can merge and the cap is the only thing standing
    // between this list and unbounded growth.
    let mut live = BlockSlabLiveIndex::default();
    let retirements = STALE_RANGES_PER_SLAB * 3;
    for block in 0..retirements as u64 {
        charge_then_retire(&mut live, 11, block * 1_000, 100, StaleCause::Overwrite);
    }
    let tally = live.stale_tally(11);
    assert_eq!(
        tally.range_count(),
        STALE_RANGES_PER_SLAB,
        "{retirements} non-mergeable ranges were recorded against a cap of \
         {STALE_RANGES_PER_SLAB}, and the list holds {}",
        tally.range_count()
    );
    assert_eq!(
        tally.dropped_range_count() as usize,
        retirements - STALE_RANGES_PER_SLAB,
        "the ranges the cap refused must be COUNTED, so a reader can tell incomplete detail from \
         no detail: expected {} dropped, found {}",
        retirements - STALE_RANGES_PER_SLAB,
        tally.dropped_range_count()
    );
    // ---- AND THIS IS THE DIVISION THE DESIGN RESTS ON. The detail is droppable; the bytes are
    // ---- not. A space figure standing on `bytes` is standing on recorded fact however far the
    // ---- range list overflowed.
    assert_eq!(
        tally.bytes,
        retirements as u64 * 100,
        "the cap refused {} ranges and the byte figure must still be exact: {} retirements x 100 B \
         is {} B, and the record holds {}",
        tally.dropped_range_count(),
        retirements,
        retirements as u64 * 100,
        tally.bytes
    );
    assert_eq!(tally.block_refs, retirements as u64, "and so must the ref count");
    assert!(
        live.stale_summary().dropped_range_count > 0,
        "the summary must carry the dropped count, or an overflowed list looks complete"
    );
}

#[test]
fn the_tracked_slab_count_is_capped_and_the_victim_is_a_finished_slab() {
    // THE BACKSTOP, for a process that never ran a reclaim round. One range per slab, so the
    // per-slab cap cannot be what bounds this.
    let mut live = BlockSlabLiveIndex::default();
    let slabs = STALE_SLABS_TRACKED as u64 + 8;
    for block_slab_id in 0..slabs {
        charge_then_retire(&mut live, block_slab_id, 0, 100, StaleCause::Overwrite);
    }
    let summary = live.stale_summary();
    assert_eq!(
        summary.slabs_tracked, STALE_SLABS_TRACKED as u64,
        "{slabs} slabs retired a block against a cap of {STALE_SLABS_TRACKED}, and the record \
         tracks {}",
        summary.slabs_tracked
    );
    assert_eq!(
        summary.slabs_pruned, 8,
        "eight slabs past the cap must have been pruned, and the count says {}",
        summary.slabs_pruned
    );
    // ---- PRUNED BYTES LEAVE `pending` AND ARRIVE IN `discarded`, so the window total is
    // ---- conserved and a figure that fell because detail was dropped is distinguishable from one
    // ---- that fell because garbage was collected.
    assert_eq!(
        summary.pending_discard_bytes + summary.discarded_bytes,
        summary.stale_bytes,
        "pending {} + discarded {} must equal the window total {}",
        summary.pending_discard_bytes,
        summary.discarded_bytes,
        summary.stale_bytes
    );
    assert_eq!(summary.discarded_bytes, 800, "eight pruned slabs at 100 B each");
}

#[test]
fn the_prune_takes_a_discarded_slabs_detail_with_it() {
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 1, 0, 500, StaleCause::Overwrite);
    charge_then_retire(&mut live, 2, 0, 700, StaleCause::Overwrite);
    let before = live.stale_summary();
    assert_eq!(before.pending_discard_bytes, 1_200);
    assert_eq!(before.slabs_tracked, 2);

    live.discard_stale_slabs([1_u64]);
    let after = live.stale_summary();
    assert_eq!(
        after.slabs_tracked, 1,
        "slab 1 was discarded, so its range list must go with it"
    );
    assert_eq!(
        after.pending_discard_bytes, 700,
        "the collector took slab 1's 500 B, so they have stopped being PENDING a discard. Leaving \
         them would report the same garbage twice -- here and as reclaimable_physical_bytes"
    );
    assert_eq!(after.discarded_bytes, 500);
    assert_eq!(
        after.stale_bytes, before.stale_bytes,
        "the WINDOW total does not fall on a discard: 1,200 B retired and 1,200 B retired, however \
         much of it has since been collected. That is what makes discard_backlog_ppm meaningful"
    );
    assert_eq!(
        after.discard_backlog_ppm,
        700 * 1_000_000 / 1_200,
        "the backlog is pending over retired, and it must fall as the collector catches up"
    );
}

#[test]
fn a_wholesale_rebuild_forgets_the_retired_set() {
    // `clear` is called where the index this describes is REPLACED -- a compaction round, a
    // manifest install. The retired ranges name offsets in slabs the rebuild is about to stop
    // pointing at, and a compaction has just handed those slabs to the collector, so carrying them
    // across would report the same garbage twice. This is also the reload behaviour: the record is
    // derived state, and the window opens again where the index was built.
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 4, 0, 900, StaleCause::Overwrite);
    assert!(live.stale_summary().pending_discard_bytes > 0);
    live.clear();
    let summary = live.stale_summary();
    assert_eq!(
        summary,
        Default::default(),
        "a wholesale rebuild must leave the retired set empty in every field, and it left {summary:?}"
    );
}

#[test]
fn the_drift_check_does_not_delete_the_retired_set() {
    // `reset_from` is how the drift check corrects the LIVE half from a walk of the index. That
    // walk reads the blocks the index still points at, so it has nothing to say about the ones it
    // stopped pointing at -- a recomputed stale set is empty BY CONSTRUCTION, and resetting to it
    // would delete true records every time the check ran.
    let mut live = BlockSlabLiveIndex::default();
    charge_then_retire(&mut live, 5, 0, 300, StaleCause::Overwrite);
    let recorded = live.stale_summary().pending_discard_bytes;
    assert_eq!(recorded, 300);
    live.reset_from(Default::default());
    assert_eq!(
        live.stale_summary().pending_discard_bytes,
        recorded,
        "correcting the live tally from the walk must leave the retired set alone, or the drift \
         check silently erases the record it cannot recompute"
    );
}

#[test]
fn a_dead_slab_is_a_discard_candidate_and_a_small_hole_is_a_compaction_candidate() {
    let mut live = BlockSlabLiveIndex::default();
    // SLAB 20: every block retired, so the collector can drop it whole. The hole is one merged
    // range well above the smallest discard that returns space, so there is nothing to compact.
    charge_then_retire(&mut live, 20, 0, MIN_DISCARD_RANGE_BYTES * 2, StaleCause::Overwrite);
    // SLAB 21: still holds a live block, and the hole is too small for a discard to return space.
    // Only relocating the live remainder recovers it, which is a compaction.
    let kept = entry(21, 0, 10_000);
    live.add_address(&kept);
    charge_then_retire(&mut live, 21, 10_000, 100, StaleCause::Overwrite);

    let summary = live.stale_summary();
    assert_eq!(
        summary.discard_candidate_slabs, 1,
        "slab 20 has no live block left, which is what a discard candidate is"
    );
    assert_eq!(summary.discard_candidate_bytes, MIN_DISCARD_RANGE_BYTES * 2);
    assert_eq!(
        summary.fragmented_range_count, 1,
        "the 100 B hole is below the {MIN_DISCARD_RANGE_BYTES} B floor a discard can return, and \
         the {} B one is not -- so exactly one range is fragmented",
        MIN_DISCARD_RANGE_BYTES * 2
    );
    assert_eq!(
        summary.compaction_candidate_slabs, 1,
        "slab 21 is partly live AND carrying a hole too small to punch, which is the one shape \
         only a compaction recovers"
    );
    assert_eq!(summary.compaction_candidate_bytes, 100);
    assert!(
        summary.compaction_recommended(),
        "a compaction candidate exists, so the flag derived from exactly that -- a slab both partly \
         live and carrying a hole too small to punch -- must be true"
    );

    // ---- AND IT IS FALSE WHEN EVERY PENDING BYTE IS ON A SLAB THE COLLECTOR COULD DROP WHOLE,
    // ---- which is what makes the flag worth publishing rather than a constant.
    let mut only_dead = BlockSlabLiveIndex::default();
    charge_then_retire(&mut only_dead, 30, 0, MIN_DISCARD_RANGE_BYTES * 2, StaleCause::Overwrite);
    assert!(
        !only_dead.stale_summary().compaction_recommended(),
        "every pending byte is on a wholly dead slab, so a compaction would be wasted work and \
         the flag must say so"
    );
}

#[test]
fn the_space_figure_is_live_plus_pending_over_live() {
    // The arithmetic of the published ratio, stated here so that the end-to-end arm below is
    // measuring a workload rather than re-deriving a formula. Both halves are block-granular and
    // both are in stored bytes, so the compression factor cancels instead of dividing in.
    let mut live = BlockSlabLiveIndex::default();
    let held = entry(40, 0, 1_000);
    live.add_address(&held);
    let summary = live.stale_summary();
    assert_eq!(
        summary.logical_image_bytes(1_000),
        1_000,
        "nothing has retired, so the image is the live set and the ratio is 1.0x"
    );
    charge_then_retire(&mut live, 40, 1_000, 4_000, StaleCause::Overwrite);
    assert_eq!(
        live.stale_summary().logical_image_bytes(1_000),
        5_000,
        "1,000 B live with 4,000 B retired and uncollected is a 5.0x image, and it is 5.0x the \
         moment the overwrite happens -- not after a cycle publishes a tally"
    );
}

// ===========================================================================================
// AND THE SAME THING THROUGH THE PRODUCTION WRITE PATH, on the workload the published figure
// cannot see.
// ===========================================================================================

const KEYS: usize = 2_000;
const VALUE: usize = 256;
const ROUNDS: usize = 4;

/// Values that cannot compress, keyed by index AND round so each pass writes different bytes.
fn noisy_value(index: usize, round: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((round as u64).wrapping_mul(0x1234_5678_9ABC_DEF0))
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

fn live_bytes(engine: &crate::TemporalEngine) -> u64 {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    assert!(
        shard.bucket_index.block_slab_live.is_ready(),
        "the live tally is not derived, so every ratio below has a provisional zero denominator"
    );
    shard
        .bucket_index
        .block_slab_live
        .iter()
        .fold(0_u64, |total, (_, tally)| total.saturating_add(tally.bytes))
}

#[test]
#[ignore = "writes 2,000 keys five times over and runs a reclaim cycle; run by name"]
fn the_space_figure_moves_at_the_overwrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    println!();
    println!("  WHAT THE OVERWRITE RECORDED -- {KEYS} keys x {VALUE} B, incompressible, new bytes each round");
    println!();
    println!(
        "    {:<16} {:>10} {:>11} {:>8} {:>7} {:>8} {:>9} {:>9}",
        "", "live", "pending", "ranges", "frag", "refs", "sp slab", "sp rec"
    );

    let mut rows = Vec::new();
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(crate::types::ExecuteRequest {
                shard_id: 1,
                command: crate::types::Command::StringSet {
                    key: format!("rec-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
        let live = live_bytes(&engine);
        let stale = engine
            .recorded_stale_summary(1)
            .expect("a loaded shard holds a record");
        let published = engine.shard_stats(1).expect("a loaded shard reports stats").storage.clone();
        println!(
            "    {:<16} {:>10} {:>11} {:>8} {:>7} {:>8} {:>8.3}x {:>8.3}x",
            format!("after {} writes", round + 1),
            live,
            stale.pending_discard_bytes,
            stale.merged_discard_range_count,
            stale.fragmented_range_count,
            stale.pending_discard_block_refs,
            published.space_amplification_ppm as f64 / 1_000_000.0,
            published.recorded_space_amplification_ppm as f64 / 1_000_000.0,
        );
        rows.push((live, stale, published));
    }

    let (first_live, _, first_published) = rows[0].clone();
    let (last_live, last_stale, last_published) = rows[ROUNDS].clone();

    // ---- NON-VACUITY FIRST. Every figure here is a ratio or a difference, and a zero live set
    // ---- satisfies any bound while measuring nothing.
    assert!(first_live > 0 && last_live > 0, "the live tally is zero, so nothing was measured");
    assert!(
        last_stale.pending_discard_bytes > 0,
        "{} writes of every key retired NOTHING. Either the rewrites never reached the store or \
         the record is not being appended at the displacement, and the ratios below are vacuous",
        ROUNDS + 1
    );

    // ---- THE PUBLISHED FIGURE IS STILL PINNED, AND THAT IS STILL THE DEFECT. Its denominator
    // ---- counts a live slab's whole file, so it cannot see a dead block inside one. Left where it
    // ---- was on purpose: re-pointing it moves a serde key consumers already read.
    assert_eq!(
        last_published.space_amplification_ppm, first_published.space_amplification_ppm,
        "space_amplification_ppm moved from {} to {} ppm across {} rewrites. It has been pinned \
         because a live slab contributes its whole file size -- if it now moves, its denominator \
         changed and the note on that field needs restating",
        first_published.space_amplification_ppm,
        last_published.space_amplification_ppm,
        ROUNDS
    );

    // ---- AND THE RECORDED ONE MOVES, BY THE RIGHT FACTOR. Each round retires exactly the previous
    // ---- round's copy of every key, so after N+1 writes the image is (N+1) live sets: 5.000x at
    // ---- five writes, which is the ceiling these ratios are held against and had never been
    // ---- approached by any arm in this area.
    let expected = (ROUNDS as u64 + 1) * 1_000_000;
    let recorded = last_published.recorded_space_amplification_ppm;
    assert!(
        recorded.abs_diff(expected) < 300_000,
        "after {} writes of every key the recorded space figure should be about {expected} ppm -- \
         the live set plus {ROUNDS} retired copies of it, over the live set -- and it reads \
         {recorded}. Live {last_live} B, pending {} B",
        ROUNDS + 1,
        last_stale.pending_discard_bytes
    );
    assert_eq!(
        first_published.recorded_space_amplification_ppm, 1_000_000,
        "after ONE write nothing has retired, so the recorded figure must be exactly 1.000x and it \
         reads {} ppm",
        first_published.recorded_space_amplification_ppm
    );

    // ---- THE LIVE SET NEVER CHANGED SIZE, only its storage did. A pending figure that tracked
    // ---- bytes WRITTEN rather than bytes retired would be indistinguishable from one that
    // ---- tracked them correctly without this.
    assert!(
        last_live.abs_diff(first_live) * 10 < first_live,
        "the live tally went {first_live} -> {last_live} B while the live SET held the same {KEYS} \
         keys at {VALUE} B throughout"
    );
    assert!(
        last_stale
            .pending_discard_bytes
            .abs_diff(last_live.saturating_mul(ROUNDS as u64))
            * 10
            < last_live.saturating_mul(ROUNDS as u64),
        "{} rounds of rewrite over a {last_live} B live set should have retired about {} B and the \
         record holds {}. More would mean a replacement that retired nothing was recorded anyway; \
         less would mean a displacement went unrecorded",
        ROUNDS,
        last_live.saturating_mul(ROUNDS as u64),
        last_stale.pending_discard_bytes
    );

    // ---- THE BOUND, MEASURED RATHER THAN CLAIMED. This is the assertion that separates a record
    // ---- from a leak: the range list must not grow in proportion to the number of retirements.
    println!();
    println!(
        "    {} retirements are held as {} merged range(s) across {} slab(s), {} dropped by the cap",
        last_stale.pending_discard_block_refs,
        last_stale.merged_discard_range_count,
        last_stale.slabs_tracked,
        last_stale.dropped_range_count
    );
    assert!(
        last_stale.merged_discard_range_count.saturating_mul(50)
            < last_stale.pending_discard_block_refs,
        "{} blocks retired and the list holds {} ranges. Contiguous retirements MERGE, so a count \
         anywhere near the number of retirements means the merge is not working and this list \
         grows with the write rate -- the changefeed shape this codebase has already been bitten by",
        last_stale.pending_discard_block_refs,
        last_stale.merged_discard_range_count
    );
    assert!(
        last_stale.merged_discard_range_count
            <= (STALE_SLABS_TRACKED * STALE_RANGES_PER_SLAB) as u64,
        "the record holds {} ranges against a hard ceiling of {} x {}",
        last_stale.merged_discard_range_count,
        STALE_SLABS_TRACKED,
        STALE_RANGES_PER_SLAB
    );

    // ---- THE CAUSE IS ATTRIBUTED. Nothing was deleted or expired here, so every retired byte must
    // ---- read as an overwrite -- which is what says this is rewrite pressure against a live set
    // ---- that did not shrink, rather than a live set that got smaller.
    assert_eq!(
        last_stale.overwrite_bytes, last_stale.stale_bytes,
        "every retirement in this workload is an overwrite, and the split reads overwrite {} / \
         removed {} / swept {} of {} retired",
        last_stale.overwrite_bytes,
        last_stale.removed_bytes,
        last_stale.swept_bytes,
        last_stale.stale_bytes
    );
    assert_eq!(
        last_published.stale_bytes_by_overwrite, last_stale.overwrite_bytes,
        "and the published field must carry it"
    );

    // ---- NOW DELETE HALF THE KEYS, which is the other cause and the other direction: the live set
    // ---- shrinks, and the record must say the garbage came from a deletion rather than a rewrite.
    for index in 0..KEYS / 2 {
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id: 1,
            command: crate::types::Command::CommonDelete { key: format!("rec-{index:06}") },
        });
        assert!(response.status.ok, "delete {index}: {:?}", response.status);
    }
    let after_delete = engine
        .recorded_stale_summary(1)
        .expect("a loaded shard holds a record");
    println!();
    println!(
        "    after deleting {} keys: overwrite {} B, removed {} B, swept {} B",
        KEYS / 2,
        after_delete.overwrite_bytes,
        after_delete.removed_bytes,
        after_delete.swept_bytes
    );
    assert!(
        after_delete.removed_bytes > 0 || after_delete.swept_bytes > 0,
        "{} keys were deleted and the record attributes {} B to a removal and {} B to a sweep. A \
         delete retires the block it pointed at, so one of the two must have moved",
        KEYS / 2,
        after_delete.removed_bytes,
        after_delete.swept_bytes
    );
    assert_eq!(
        after_delete.overwrite_bytes, last_stale.overwrite_bytes,
        "a delete must not be charged to the overwrite cause"
    );

    // ---- AND THE CONSERVATION LAW AT EVERY POINT: what was retired is what is pending plus what
    // ---- has been discarded, in every reading, which is what makes `discard_backlog_ppm` a real
    // ---- fraction rather than two unrelated counters divided.
    for (label, stale) in [
        ("after the rewrites", &last_stale),
        ("after the deletes", &after_delete),
    ] {
        assert_eq!(
            stale.pending_discard_bytes + stale.discarded_bytes,
            stale.stale_bytes,
            "{label}: pending {} + discarded {} != retired {}",
            stale.pending_discard_bytes,
            stale.discarded_bytes,
            stale.stale_bytes
        );
        assert_eq!(
            stale.stale_bytes,
            stale.overwrite_bytes + stale.removed_bytes + stale.swept_bytes,
            "{label}: the causes must sum to the window total"
        );
    }

    // ---- A RECLAIM ROUND, so the prune is exercised through production rather than by hand. What
    // ---- it does to the record is REPORTED and not asserted beyond the conservation law: the
    // ---- round both hands slabs to the collector (which prunes their detail) and rebuilds the
    // ---- bucket index (which empties the record wholesale), so a target here would be pinning
    // ---- which of the two happened first.
    let buckets: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    assert!(!buckets.is_empty(), "no bucket to dump, so the reclaim round has nothing to act on");
    let cycle = engine.run_storage_manager_cycle(crate::engine::reports::StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: false,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        selected_dump_buckets: buckets.clone(),
        max_dump_buckets_per_round: buckets.len(),
        min_undumped_wal_records: 1,
        min_undumped_wal_bytes: 1,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        ..crate::engine::reports::StorageManagerCycleRequest::default()
    });
    std::hint::black_box(&cycle);

    let after_cycle = engine
        .recorded_stale_summary(1)
        .expect("a loaded shard holds a record");
    println!();
    println!(
        "    after a reclaim round: retired {} B, pending {} B, discarded {} B, backlog {} ppm, \
         {} slab(s) tracked, {} pruned",
        after_cycle.stale_bytes,
        after_cycle.pending_discard_bytes,
        after_cycle.discarded_bytes,
        after_cycle.discard_backlog_ppm,
        after_cycle.slabs_tracked,
        after_cycle.slabs_pruned
    );
    assert_eq!(
        after_cycle.pending_discard_bytes + after_cycle.discarded_bytes,
        after_cycle.stale_bytes,
        "the conservation law has to survive a production round, or the prune and the wholesale \
         clear are disagreeing about what left"
    );
    assert!(
        after_cycle.pending_discard_bytes <= after_delete.pending_discard_bytes,
        "a reclaim round took the pending figure from {} B to {} B. A round discards slabs and \
         rebuilds the index; neither can CREATE pending garbage",
        after_delete.pending_discard_bytes,
        after_cycle.pending_discard_bytes
    );
    // AND THE READING AFTER A ROUND IS ZERO, SO SAY SO RATHER THAN LET THE CHECK ABOVE LOOK
    // LIKE EVIDENCE. Measured: every field reads 0 here. The round rebuilds the bucket index,
    // and `BlockSlabLiveIndex::clear` empties the retired set with the index it describes --
    // the documented behaviour, because a compaction has just relocated the live set and handed
    // the old slabs to the collector, so those bytes have been ACTED ON rather than are
    // pending. The conservation assertion is therefore VACUOUS at this point (0 + 0 == 0), and
    // the two readings before it are where it has content.
    if after_cycle.stale_bytes == 0 {
        println!(
            "    (the record is EMPTY after the round, which is `clear` running where the index"
        );
        println!("     was rebuilt -- so the conservation check above is vacuous here, and the");
        println!("     window has reopened. The readings before the round are the ones with");
        println!("     content.)");
    }

    println!();
    println!("    WHAT THIS ARM ESTABLISHES, and the one thing it does not:");
    println!("    the record moves at the overwrite -- 1.000x -> {:.3}x with no cycle run --", recorded as f64 / 1_000_000.0);
    println!("    where the published slab-granular figure holds at {:.3}x throughout. The record", last_published.space_amplification_ppm as f64 / 1_000_000.0);
    println!("    is DERIVED STATE and is not persisted, so it covers the discharges made since");
    println!("    the index was last built: after a restart it reads zero however much garbage the");
    println!("    store is carrying, and `slab_live_fractions()` -- which GC candidate selection");
    println!("    already reads -- stays the reload-independent fallback. It understates, never");
    println!("    overstates, which is the only safe direction for a figure a collector may act on.");
    println!();
}
