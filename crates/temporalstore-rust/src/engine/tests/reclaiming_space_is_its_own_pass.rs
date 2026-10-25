// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RECLAIMING DISK SPACE IS ITS OWN PASS, WITH ITS OWN TRIGGER, AND THE TRIGGER IS NOT A RATIO.
//!
//! # TWO OPERATIONS, NOT ONE
//!
//! Relieving memory pressure and reclaiming disk space are different operations in this store and
//! run as different stages. `apply_storage_eviction` dumps and releases a bucket's resident page
//! list; the bytes on disk are exactly where they were when it returns. `compact_pages` rewrites
//! live pages off the slabs carrying dead space so the collector can destroy them. Neither is a
//! side effect of the other, and the third test below is the measurement that says so.
//!
//! # THE RATIO THE TRIGGER DOES NOT READ, AND WHY THAT IS THE CORRECT CHOICE
//!
//! `space_amplification_ppm` is `total_known_physical_bytes / live_physical_bytes`, and both halves
//! are blind to reclamation: the denominator adds a slab's WHOLE file while its state is `Active`
//! or `Sealed`, so a slab counts fully live however many of its blocks are dead, and the numerator
//! accumulates before the state match, so a purged slab still contributes its full size
//! (`a_space_ratio_whose_numerator_falls`, `the_live_and_stale_shares_are_a_partition`).
//!
//! Measured again here, on this module's own fixture: the published figure holds at **1.000x
//! through every round** while the block-granular figure beside it goes 1.000x -> 4.000x and the
//! published stale share goes 0.00% -> 75.00%. A reclaimer keyed on the published ratio would
//! never fire.
//!
//! # WHAT THE SHIPPED TRIGGER IS KEYED ON, MEASURED
//!
//! `stale_block_pressure` in the periodic loop is a disjunction of three terms, and the first test
//! below reads all three on the same sweep:
//!
//! ```text
//!   stale_block_slab_count        >= stale_block_slab_pressure            (1 by default)
//!   reclaim_candidate_count       >= stale_block_slab_pressure            (1 by default)
//!   reclaimable_physical_bytes    >= reclaimable_physical_bytes_pressure  (1 by default)
//! ```
//!
//! The term that sees dead space inside a LIVE slab is the candidate COUNT. A slab enters
//! `reclaim_candidates` as soon as `physical_bytes - live_physical_bytes` is non-zero, so the
//! count rises from the first byte of garbage -- measured 0 -> 1 -> 2 -> 3 over the same rounds the
//! published ratio sits still through. At a pressure setting of 1 that is as sensitive as a trigger
//! can be, and strictly more sensitive than a threshold at "roughly twice the logical size" would
//! be: every state such a threshold would catch is already inside the set this one catches.
//!
//! `reclaimable_physical_bytes` is NOT that term, and the first test pins it. It sums the
//! DELAYED-DESTROY reports -- slabs already quarantined for destruction -- so it reads **0** on a
//! shard carrying 220,000 bytes of dead space inside live slabs. It is a true statement about
//! quarantined files and it is not a statement about garbage, so the byte half of that disjunction
//! contributes nothing on the shape that matters and the count half carries it.
//!
//! # AND THE VICTIM IS THE ONE HOLDING THE MOST GARBAGE
//!
//! `storage_reclaim_candidates_from_slab_reports` scores each slab
//! `stale_physical_bytes * (1 - live_ref_density) + stale_block_estimate` and sorts DESCENDING, so
//! the plan hands the heaviest-garbage slab first. The second test is the gate on that order, on a
//! fixture whose slabs carry deliberately different amounts of dead space.
//!
//! # WHAT THESE TESTS ARE FOR
//!
//! The three measurements fix the quantities. The fourth is a SOURCE guard, and it is here because
//! a declined design is only durable if something fails when it is re-attempted: keying any
//! reclamation branch on a published `*_space_amplification_ppm` field is the design this store has
//! declined, and a comment saying so does not fail.

#![allow(clippy::all)]
use super::*;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 400;
const VALUE_LEN: usize = 256;

/// Incompressible bytes, so a rewrite genuinely occupies new space rather than being folded away
/// by the block codec -- a compressible fixture makes every byte figure here a figure about zstd.
fn noisy_value(index: usize, round: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((round as u64).wrapping_mul(0x1234_5678_9ABC_DEF0))
        | 1;
    (0..VALUE_LEN)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

fn key_of(index: usize) -> String {
    format!("reclaim-{index:06}")
}

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

/// The rounds this fixture writes, each naming how many keys of the PREFIX it rewrites.
///
/// WHY A HALVING PREFIX AND NOT A WHOLE-STORE REWRITE. Rewriting every key and rolling leaves each
/// older slab with NOTHING live on it: every candidate comes back `orphan_segment` with
/// `live_physical_bytes` 0, every score is identical, and nothing is relocatable -- so a fixture
/// built that way cannot tell an ordering from any other order and cannot issue a compaction round
/// at all. Measured on that shape first: three candidates, all at score 107,600, 0 relocatable
/// page refs.
///
/// Halving the rewritten prefix gives each slab a DIFFERENT amount of dead space while leaving
/// live pages on it, which is the state the collector actually has to choose between.
const REWRITE_ROUNDS: &[usize] = &[KEYS, KEYS / 2, KEYS / 4, KEYS / 8];

/// Seed the fixture and return, per key, the round whose value it now holds.
///
/// The returned vector is what a read-back is checked against: without it a later arm would have
/// to guess which of four values a key should carry, and a check that guesses is a check that
/// passes on the wrong bytes.
fn seed_overlapping_slabs(engine: &TemporalEngine) -> Vec<usize> {
    let mut written_in = vec![0usize; KEYS];
    for (round, prefix) in REWRITE_ROUNDS.iter().copied().enumerate() {
        for index in 0..prefix {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key_of(index),
                    value: noisy_value(index, round),
                },
            });
            assert!(
                response.status.ok,
                "round {round} write {index}: {:?}",
                response.status
            );
            written_in[index] = round;
        }
        // Each round's bytes land on their own slab, so the dead space the NEXT round creates is
        // attributable to a slab rather than smeared across the active one.
        let _ = engine.block_store.prepare_next_slab_with_target(1);
    }
    written_in
}

/// The reclaim plan, asked the same way the periodic loop asks it.
fn plan_of(engine: &TemporalEngine) -> crate::StorageLifecyclePlan {
    engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..crate::StorageLifecycleRequest::default()
    })
}

/// Dead bytes the plan can actually see: summed over the candidates it named.
///
/// NOT `plan.reclaimable_physical_bytes`, which sums the delayed-destroy reports and reads 0 here.
fn dead_bytes(plan: &crate::StorageLifecyclePlan) -> u64 {
    plan.reclaim_candidates
        .iter()
        .map(|candidate| candidate.stale_physical_bytes)
        .sum()
}

fn as_x(ppm: u64) -> f64 {
    ppm as f64 / 1_000_000.0
}

/// ONE SWEEP, AND THE PUBLISHED RATIO DOES NOT MOVE WHILE THE TRIGGER'S OWN TERM DOES.
///
/// Rewriting keys that are already there is the one instrument that separates "nothing is dead"
/// from "dead is not being counted": the live set never changes size, only its storage does. The
/// sweep is what makes this a statement about the quantities rather than about one reading of them.
///
/// FIVE THINGS ASSERTED, and the first two are each other's control:
///   1. the published `space_amplification_ppm` stays at 1.000x through every round, so a
///      reclaimer keyed on it never sees the dead space and never fires;
///   2. the block-granular published figure and the published stale share both RISE through the
///      same rounds, which attributes (1) to the ratio's own denominator rather than to a fixture
///      with no garbage in it;
///   3. the candidate COUNT rises, which is the term in the shipped disjunction that sees dead
///      space inside a live slab;
///   4. `reclaimable_physical_bytes` stays 0, because it sums quarantined slabs and not garbage --
///      stated so that reading the byte half of that disjunction as a garbage figure fails here;
///   5. the shipped trigger is OPEN at the default settings and something is relocatable, so the
///      stage this test is about would actually run.
#[test]
fn the_reclamation_trigger_reads_dead_bytes_where_the_published_ratio_reads_one_x() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());

    struct Row {
        round: usize,
        prefix: usize,
        published_space_ppm: u64,
        published_block_space_ppm: u64,
        stale_share_ppm: u64,
        plan_reclaimable_bytes: u64,
        candidate_dead_bytes: u64,
        candidates: usize,
        top_stale_bytes: u64,
        top_score: u64,
        relocatable_refs: u64,
    }

    let mut rows: Vec<Row> = Vec::new();
    let mut written_in = vec![0usize; KEYS];
    for (round, prefix) in REWRITE_ROUNDS.iter().copied().enumerate() {
        for index in 0..prefix {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key_of(index),
                    value: noisy_value(index, round),
                },
            });
            assert!(response.status.ok, "round {round} write {index}");
            written_in[index] = round;
        }
        let _ = engine.block_store.prepare_next_slab_with_target(1);

        let published = engine
            .shard_stats(1)
            .expect("a loaded shard reports stats")
            .storage;
        let plan = plan_of(&engine);
        let top = plan.reclaim_candidates.first();
        rows.push(Row {
            round,
            prefix,
            published_space_ppm: published.space_amplification_ppm,
            published_block_space_ppm: published.block_space_amplification_ppm,
            stale_share_ppm: published.stale_bytes_ppm,
            plan_reclaimable_bytes: plan.reclaimable_physical_bytes,
            candidate_dead_bytes: dead_bytes(&plan),
            candidates: plan.reclaim_candidates.len(),
            top_stale_bytes: top.map(|c| c.stale_physical_bytes).unwrap_or(0),
            top_score: top.map(|c| c.reclaim_score).unwrap_or(0),
            relocatable_refs: crate::engine::compaction_relocatable_block_refs(
                &plan.reclaim_candidates,
            ),
        });
    }
    let _ = written_in;

    eprintln!();
    eprintln!(
        "  [reclaim-trigger] {KEYS} keys x {VALUE_LEN} B incompressible; each round rewrites a \
halved prefix onto its own slab"
    );
    eprintln!(
        "    {:>5} {:>7} {:>11} {:>11} {:>11} {:>13} {:>13} {:>11} {:>12} {:>12} {:>11}",
        "round",
        "rewrote",
        "space_ppm",
        "block_ppm",
        "stale_shr",
        "plan_reclm_B",
        "cand_dead_B",
        "candidates",
        "top_stale_B",
        "top_score",
        "reloc_refs",
    );
    for row in &rows {
        eprintln!(
            "    {:>5} {:>7} {:>10.3}x {:>10.3}x {:>10.2}% {:>13} {:>13} {:>11} {:>12} {:>12} {:>11}",
            row.round,
            row.prefix,
            as_x(row.published_space_ppm),
            as_x(row.published_block_space_ppm),
            row.stale_share_ppm as f64 / 10_000.0,
            row.plan_reclaimable_bytes,
            row.candidate_dead_bytes,
            row.candidates,
            row.top_stale_bytes,
            row.top_score,
            row.relocatable_refs,
        );
    }

    let first = rows.first().expect("the sweep ran");
    let last = rows.last().expect("the sweep ran");

    // --- NON-VACUITY: the sweep accumulated dead space inside LIVE slabs. ---
    assert!(
        last.candidate_dead_bytes > 0,
        "the candidates hold no dead bytes after {} rounds, so nothing below is a statement about \
         a blind ratio",
        rows.len()
    );
    assert!(
        last.top_stale_bytes > 0 && last.top_score > 0,
        "the heaviest candidate holds {} stale bytes at score {}",
        last.top_stale_bytes,
        last.top_score
    );

    // --- 1. THE PUBLISHED RATIO DOES NOT MOVE. ---
    for row in &rows {
        assert!(
            (as_x(row.published_space_ppm) - 1.0).abs() < 0.01,
            "round {}: `space_amplification_ppm` read {:.3}x. It is pinned at 1.0x because a live \
             slab contributes its WHOLE file to the denominator however many of its blocks are \
             dead -- if it now moves, the denominator changed and the case for not keying a \
             reclaimer on it has to be re-measured rather than relaxed",
            row.round,
            as_x(row.published_space_ppm)
        );
    }

    // --- 2. WHILE THE BLOCK-GRANULAR FIGURES DO, which is what attributes (1) to the ratio. ---
    assert!(
        last.published_block_space_ppm > first.published_block_space_ppm,
        "`block_space_amplification_ppm` went {:.3}x -> {:.3}x. Its denominator counts the blocks \
         the index still points at, so dead blocks have to push it up; a figure that does not move \
         here means this fixture is not accumulating garbage and assertion (1) is vacuous",
        as_x(first.published_block_space_ppm),
        as_x(last.published_block_space_ppm)
    );
    assert!(
        last.stale_share_ppm > first.stale_share_ppm,
        "the published stale share went {:.2}% -> {:.2}%",
        first.stale_share_ppm as f64 / 10_000.0,
        last.stale_share_ppm as f64 / 10_000.0
    );

    // --- 3. THE TERM THE TRIGGER READS MOVES. ---
    assert!(
        last.candidates > first.candidates,
        "the candidate count went {} -> {} across the sweep. That count is the term in \
         `stale_block_pressure` that sees dead space inside a live slab, so if it does not move \
         the trigger cannot see this garbage either",
        first.candidates,
        last.candidates
    );

    // --- 4. AND THE BYTE TERM IS ABOUT SOMETHING ELSE, which is worth failing on. ---
    assert_eq!(
        last.plan_reclaimable_bytes, 0,
        "`reclaimable_physical_bytes` read {} on a shard holding {} dead bytes inside live slabs. \
         It sums the DELAYED-DESTROY reports -- slabs already quarantined for destruction -- so on \
         this shape it is 0 by construction and the candidate COUNT is what carries the trigger. \
         If it now reports garbage, the byte half of `stale_block_pressure` has become load-bearing \
         and the sensitivity of that disjunction has to be re-measured",
        last.plan_reclaimable_bytes,
        last.candidate_dead_bytes
    );

    // --- 5. THE SHIPPED TRIGGER IS OPEN, read against the shipped defaults. ---
    let options = crate::data_node::StorageManagerOptions::default();
    let stale_pressure = last.candidates >= options.stale_block_slab_pressure.max(1)
        || last.plan_reclaimable_bytes >= options.reclaimable_physical_bytes_pressure.max(1);
    assert!(
        stale_pressure,
        "{} candidates and {} plan-reclaimable bytes did not reach the shipped settings ({} \
         candidates / {} bytes), so the stage this test is about would not run",
        last.candidates,
        last.plan_reclaimable_bytes,
        options.stale_block_slab_pressure,
        options.reclaimable_physical_bytes_pressure
    );
    assert!(
        last.relocatable_refs > 0,
        "nothing is relocatable, so `compact_pages` would decline with \
         `compact_pages_nothing_to_relocate` and the trigger above would not issue a round"
    );

    eprintln!(
        "    => the published ratio held at {:.3}x while the block-granular one went {:.3}x -> \
{:.3}x and the stale share 0.00% -> {:.2}%. The trigger's own term -- the candidate count -- went \
{} -> {} over {} dead bytes in live slabs, at a shipped pressure setting of {}. \
`reclaimable_physical_bytes` stayed {} throughout, because it counts quarantined slabs and not \
garbage",
        as_x(last.published_space_ppm),
        as_x(first.published_block_space_ppm),
        as_x(last.published_block_space_ppm),
        last.stale_share_ppm as f64 / 10_000.0,
        first.candidates,
        last.candidates,
        last.candidate_dead_bytes,
        options.stale_block_slab_pressure,
        last.plan_reclaimable_bytes,
    );
}

/// THE PLAN HANDS THE SLAB HOLDING THE MOST GARBAGE FIRST.
///
/// `storage_reclaim_candidates_from_slab_reports` scores a slab on the dead bytes it holds,
/// weighted down by how live it still is, and sorts descending. This is the gate on that order.
///
/// WHAT MAKES IT FAIL RATHER THAN BE VACUOUS: a list of one, or a list whose scores are all equal,
/// is sorted whatever the comparator does. Both are asserted away -- at least two candidates and
/// at least two DISTINCT scores -- before the order is read. The halving prefix is what produces
/// the distinct scores; a whole-store rewrite gives every slab the same one.
#[test]
fn the_reclaim_candidates_are_ordered_by_the_dead_space_they_hold() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    seed_overlapping_slabs(&engine);

    let plan = plan_of(&engine);
    let candidates = &plan.reclaim_candidates;
    eprintln!();
    eprintln!("  [reclaim-order] {} candidates", candidates.len());
    eprintln!(
        "    {:>6} {:>13} {:>13} {:>12} {:>14} {:>12} {:>12} {:>18}",
        "slab", "physical_B", "live_B", "stale_B", "stale_blocks", "live_refs", "score", "reason"
    );
    for candidate in candidates {
        eprintln!(
            "    {:>6} {:>13} {:>13} {:>12} {:>14} {:>12} {:>12} {:>18}",
            candidate.block_slab_id,
            candidate.physical_bytes,
            candidate.live_physical_bytes,
            candidate.stale_physical_bytes,
            candidate.stale_block_estimate,
            candidate.live_block_refs,
            candidate.reclaim_score,
            candidate.reason,
        );
    }

    // --- NON-VACUITY, both halves. ---
    assert!(
        candidates.len() >= 2,
        "{} candidate(s): a list of one is ordered whatever the comparator does",
        candidates.len()
    );
    let distinct_scores = candidates
        .iter()
        .map(|candidate| candidate.reclaim_score)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    assert!(
        distinct_scores >= 2,
        "all {} candidates carry the same score, so any order satisfies the assertion below. A \
         whole-store rewrite does exactly this -- see `REWRITE_ROUNDS`",
        candidates.len()
    );

    // --- THE ORDER. ---
    for pair in candidates.windows(2) {
        assert!(
            pair[0].reclaim_score >= pair[1].reclaim_score,
            "slab {} (score {}) is handed before slab {} (score {}); the plan has to put the \
             heaviest garbage first, because that is the slab a bounded round recovers the most by \
             emptying",
            pair[0].block_slab_id,
            pair[0].reclaim_score,
            pair[1].block_slab_id,
            pair[1].reclaim_score,
        );
    }
    let heaviest = candidates
        .iter()
        .max_by_key(|candidate| candidate.reclaim_score)
        .expect("a non-empty list has a maximum");
    assert_eq!(
        candidates[0].block_slab_id, heaviest.block_slab_id,
        "the plan hands slab {} first while slab {} holds the heaviest garbage (score {} against \
         {})",
        candidates[0].block_slab_id,
        heaviest.block_slab_id,
        heaviest.reclaim_score,
        candidates[0].reclaim_score,
    );
    eprintln!(
        "    => the heaviest candidate is slab {} at score {} ({} stale bytes of {} physical), and \
it is handed first of {}",
        candidates[0].block_slab_id,
        candidates[0].reclaim_score,
        candidates[0].stale_physical_bytes,
        candidates[0].physical_bytes,
        candidates.len(),
    );
}

/// RELIEVING MEMORY RECLAIMS NO SPACE, AND RECLAIMING SPACE IS A DIFFERENT CALL.
///
/// This is the measurement behind "two operations, not one". An eviction round is bracketed by the
/// space terms and they do not move; a compaction round over the same store moves them.
///
/// SIX CLAUSES:
///   1. the eviction round ACTED -- `bucket_index_buckets_released > 0`. Not
///      `dropped_object_count > 0`: that counter belongs to the retired branch that deletes
///      records, so asserting it positive would demand the one arm memory pressure must never
///      take.
///   2. and it dropped nothing.
///   3. and the dead bytes on disk are EXACTLY where they were, by three terms: the candidates'
///      summed stale bytes, the candidate count, and the non-purged file bytes.
///   4. while a compaction round over the same store moves what IT is measured by -- it relocates
///      page refs and empties the drain set -- which is what makes (3) a statement about eviction
///      rather than about a store with nothing to reclaim.
///   5. every acknowledged key still reads back the value its LAST write gave it, cold.
///   6. with a never-written control beside it.
#[test]
fn relieving_memory_reclaims_no_space_and_reclaiming_space_is_its_own_call() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_at(dir.path());
    let written_in = seed_overlapping_slabs(&engine);

    let before = plan_of(&engine);
    let dead_before = dead_bytes(&before);
    let candidates_before = before.reclaim_candidates.len();
    let relocatable_before =
        crate::engine::compaction_relocatable_block_refs(&before.reclaim_candidates);
    let non_purged_before = engine
        .shard_stats(1)
        .expect("stats")
        .storage
        .non_purged_physical_bytes;
    assert!(
        dead_before > 0 && relocatable_before > 0,
        "the fixture holds {dead_before} dead bytes and {relocatable_before} relocatable refs; \
         'eviction did not reclaim' says nothing without both"
    );

    // --- THE EVICTION ROUND. ---
    let report = engine.apply_storage_eviction(1, 1, 16, true, false);
    assert_eq!(
        report.skipped_reason, "",
        "the eviction gate declined, so this arm measures a refusal: {report:?}"
    );
    assert!(
        report.bucket_index_buckets_released > 0,
        "the eviction round released no bucket ({} refused), so it did not act and (3) is vacuous",
        report.bucket_index_release_refused
    );
    assert_eq!(
        report.dropped_object_count, 0,
        "the eviction round deleted {} records; relieving memory releases pages and never deletes",
        report.dropped_object_count
    );

    let after_evict = plan_of(&engine);
    let non_purged_after_evict = engine
        .shard_stats(1)
        .expect("stats")
        .storage
        .non_purged_physical_bytes;

    // --- 3. THE SPACE TERMS DID NOT MOVE, all three. ---
    assert_eq!(
        dead_bytes(&after_evict),
        dead_before,
        "an eviction round moved the candidates' dead bytes {dead_before} -> {}. Relieving memory \
         releases a resident page list and writes no block, so it cannot reclaim disk space; if it \
         now does, the two passes have been fused and the trigger for the second is no longer what \
         decides it",
        dead_bytes(&after_evict)
    );
    assert_eq!(
        after_evict.reclaim_candidates.len(),
        candidates_before,
        "an eviction round moved the candidate count {candidates_before} -> {}",
        after_evict.reclaim_candidates.len()
    );
    assert_eq!(
        non_purged_after_evict, non_purged_before,
        "an eviction round moved the bytes on disk {non_purged_before} -> {non_purged_after_evict}"
    );

    // --- 4. AND A COMPACTION ROUND DOES MOVE WHAT IT IS MEASURED BY. ---
    let drain = crate::engine::compaction_drain_block_slab_ids(&after_evict.reclaim_candidates);
    assert!(
        !drain.is_empty(),
        "no slab is drainable, so the compaction arm cannot move anything and (3) is not \
         attributable to eviction"
    );
    let compaction = engine
        .compact_shard_blocks_draining(1, drain.clone())
        .expect("a compaction round over a loaded shard");
    let after_compact = plan_of(&engine);
    let relocatable_after =
        crate::engine::compaction_relocatable_block_refs(&after_compact.reclaim_candidates);
    eprintln!();
    eprintln!(
        "  [two-passes] candidate dead bytes {dead_before} (before) -> {} (after eviction released \
{} buckets, dropped {}) -> {} (after one compaction round relocated {} page refs over {} drained \
slabs); candidates {candidates_before} -> {} -> {}; relocatable refs {relocatable_before} -> {}; \
non-purged bytes {non_purged_before} -> {non_purged_after_evict}",
        dead_bytes(&after_evict),
        report.bucket_index_buckets_released,
        report.dropped_object_count,
        dead_bytes(&after_compact),
        compaction.rewritten_block_refs,
        drain.len(),
        after_evict.reclaim_candidates.len(),
        after_compact.reclaim_candidates.len(),
        relocatable_after,
    );
    assert!(
        compaction.rewritten_block_refs > 0,
        "the compaction round relocated nothing, so it is not the counter-arm this test needs"
    );
    assert!(
        relocatable_after < relocatable_before,
        "relocatable page refs went {relocatable_before} -> {relocatable_after}; a round that \
         drained its slabs has to leave fewer pages on slabs anyone wants emptied, and this is the \
         term that says the second pass did something the first one could not"
    );

    // --- 5 and 6. NOTHING ACKNOWLEDGED WAS LOST, with the control beside it. ---
    let _ = engine.cache().invalidate_shard(1);
    let mut verified = 0usize;
    let mut wrong: Vec<String> = Vec::new();
    for index in 0..KEYS {
        let answer = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key_of(index) },
        });
        let expected = noisy_value(index, written_in[index]);
        match answer.response {
            crate::types::CommandResponse::Bytes { value: Some(value) } if value == expected => {
                verified += 1
            }
            other => {
                if wrong.len() < 64 {
                    wrong.push(format!(
                        "{} (last written in round {}): {other:?}",
                        key_of(index),
                        written_in[index]
                    ));
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {KEYS} acknowledged keys did not read back after an eviction round and a compaction \
         round: first few {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
    assert_eq!(
        verified, KEYS,
        "only {verified} of {KEYS} keys were verified, so the empty list above is a loop that did \
         not run"
    );
    let absent = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet {
            key: "reclaim-never-written".to_string(),
        },
    });
    assert!(
        matches!(
            absent.response,
            crate::types::CommandResponse::Bytes { value: None }
        ),
        "a key that was never written answered {:?}, so the {verified} reads above are not \
         evidence that the store knows what it holds",
        absent.response
    );
}

/// THE DECLINED DESIGN, PLANTED: NO PRODUCTION BRANCH READS A PUBLISHED SPACE-AMPLIFICATION FIELD.
///
/// The three `*_space_amplification_ppm` fields are published figures and nothing more. Two of them
/// are measured blind -- see this module's header -- so a trigger, threshold or comparison keyed on
/// one of them would be a reclaimer that cannot see the garbage it exists to collect. That decision
/// is recorded in several comments, and a comment does not fail when the design is re-attempted.
/// This does.
///
/// WHAT IT SCANS AND WHAT IT ALLOWS. Every `.rs` file under the crate's `src` that is not a test
/// module: an assignment, a struct field, a `pub` declaration or a doc line naming one of these
/// fields is fine, because publishing them is the whole point. What is refused is a line that both
/// names one and carries a comparison or a branch.
///
/// THREE NON-VACUITY CLAUSES, because a scan that finds nothing looks the same as a clean tree:
///   * the walk reached a floor of files and recursed;
///   * the fields were actually FOUND in production source, so the needle still matches something;
///   * and the matcher itself is exercised on a line that must be reported and on one that must
///     not, so a matcher that has stopped matching anything fails here rather than passing the
///     tree.
#[test]
fn no_reclamation_branch_reads_a_published_space_amplification_field() {
    const FIELDS: &[&str] = &[
        "space_amplification_ppm",
        "block_space_amplification_ppm",
        "recorded_space_amplification_ppm",
    ];
    /// Tokens that make a line a DECISION rather than a publication.
    const DECIDING: &[&str] = &[
        "if ", "while ", "match ", ".then(", ".then_some(", ">=", "<=", "==", "!=", "&&", "||",
        "> ", "< ",
    ];
    const MIN_FILES: usize = 100;

    /// True when this line both names a published ratio and decides something on it.
    fn decides_on_a_ratio(line: &str) -> bool {
        let code = line.trim_start();
        // A doc line or a comment is prose, not a branch.
        if code.starts_with("//") || code.starts_with("/*") || code.starts_with('*') {
            return false;
        }
        FIELDS.iter().any(|field| code.contains(field))
            && DECIDING.iter().any(|token| code.contains(token))
    }

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("read_dir {} failed: {err}", dir.display()));
        for entry in entries {
            let entry = entry.expect("dir entry readable");
            let path = entry.path();
            let kind = entry.file_type().expect("file type readable");
            if kind.is_dir() {
                stack.push(path);
            } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();

    // --- NON-VACUITY: the walk ran and recursed. ---
    assert!(
        files.len() >= MIN_FILES,
        "the walk found {} files under {}, floor is {MIN_FILES} -- that is a broken walk, not a \
         clean tree",
        files.len(),
        root.display()
    );
    assert!(
        files
            .iter()
            .filter(|path| path.parent() != Some(root.as_path()))
            .count()
            > 0,
        "the walk did not recurse, so most of this crate was not scanned"
    );

    // --- NON-VACUITY: the matcher still matches, in both directions. ---
    assert!(
        decides_on_a_ratio("        if published.space_amplification_ppm >= 2_000_000 {"),
        "the matcher no longer reports a line that keys a branch on a published ratio, so a clean \
         scan below means nothing"
    );
    assert!(
        !decides_on_a_ratio("    pub space_amplification_ppm: u64,"),
        "the matcher reports a plain field declaration, so it would fail on a tree that only \
         publishes the figure"
    );

    let mut production_mentions = 0usize;
    let mut findings: Vec<String> = Vec::new();
    for path in &files {
        let relative = path.strip_prefix(&root).unwrap_or(path);
        let text = relative.to_string_lossy().replace('\\', "/");
        // Test modules are allowed to compare these figures -- that is what the measurements in
        // this crate do, including the ones in this very file.
        if text.starts_with("tests/")
            || text.contains("/tests/")
            || text.ends_with("tests.rs")
            || text.contains("_guard.rs")
        {
            continue;
        }
        let body = std::fs::read_to_string(path)
            .unwrap_or_else(|err| panic!("read {} failed: {err}", path.display()));
        for (number, line) in body.lines().enumerate() {
            if FIELDS.iter().any(|field| line.contains(field)) {
                production_mentions += 1;
            }
            if decides_on_a_ratio(line) {
                findings.push(format!("{text}:{}: {}", number + 1, line.trim()));
            }
        }
    }

    // --- NON-VACUITY: the fields are present in production source at all. ---
    assert!(
        production_mentions > 0,
        "no production line names any of {FIELDS:?}, so this guard is watching a name that has \
         been renamed and would pass whatever a reclaimer did"
    );

    assert!(
        findings.is_empty(),
        "{} production line(s) key a decision on a published space-amplification figure:\n{}\n\
         Two of those three figures are measured BLIND -- the denominator adds a live slab's whole \
         file however many of its blocks are dead, so the published figure holds at 1.000x where \
         the block-granular truth is 4.000x. A reclaimer keyed on one of them does not fire. The \
         term to key on is the one the shipped trigger already uses: the count of slabs carrying \
         dead space, which \
         `the_reclamation_trigger_reads_dead_bytes_where_the_published_ratio_reads_one_x` measures \
         rising over the same rounds the ratio sits still through.",
        findings.len(),
        findings.join("\n")
    );
    eprintln!(
        "  [declined-design] {} files scanned, {production_mentions} production mentions of the \
three published space-amplification figures, 0 of them in a branch, comparison or trigger",
        files.len()
    );
}
