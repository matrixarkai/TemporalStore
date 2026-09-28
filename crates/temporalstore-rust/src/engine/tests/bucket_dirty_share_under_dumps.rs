// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE DIRTY SHARE IS NOT A CONSTANT, AND THE ONE READING #1995 PRICED IS THE WORST POINT ON ITS
//! CURVE.
//!
//! #1995 refuted hoisting `first_dirty_wal_sequence` and `first_dirty_index_log_sequence` into a
//! side map keyed by bucket id, on a measured dirty share of **99.02% and 100.00%** at the shipped
//! `0..1023` range. At that share a map holding one entry per dirty bucket costs more than the
//! sixteen bytes it takes off every node, so it loses.
//!
//! THE FIXTURE DID DUMP, so the obvious objection does not apply. `one_round_at_the_configured_cap`
//! sits between the two write phases of
//! `bucket_sequence_budget::what_hoisting_the_two_transient_claims_would_cost_at_two_corpus_sizes`,
//! and that is worth stating plainly because a never-dumped store would have made the whole figure
//! an artefact of the same kind #1959 was caught by.
//!
//! BUT IT DUMPED ONCE, AT A CAP OF SIXTY-FOUR, OVER ROUGHLY A THOUSAND BUCKETS. `CONFIGURED_DUMP_CAP`
//! is 64 and the shipped range fills about 1,012 of 1,024 buckets at four thousand records, so ONE
//! round can reach about **6.3%** of the store. The fixture then writes the second half of its
//! corpus, re-dirtying what it just cleaned. The reading is taken immediately after that burst.
//!
//! So 99-100% is neither an artefact nor a steady state. It is the dirty share **one capped round
//! into a corpus that needs about sixteen of them**, measured at the moment a write burst has just
//! finished. That is the worst point on the curve, and nothing in #1995 says where the rest of the
//! curve is -- which matters, because a structure that is DRAINED when a bucket is dumped holds only
//! the buckets dirty SINCE THE LAST DUMP, and that is a different set from "every dirty bucket".
//!
//! THIS MODULE MEASURES THE DRAINING CURVE, as a series over rounds rather than one reading, at
//! both corpus sizes and both ranges, and prices three shapes against it at every point. What it
//! does NOT establish is the share a store under CONTINUOUS write load settles at -- that arm was
//! built, could not be believed, and is recorded in the body as an open question. So this module
//! says the 99-100% figure is the undrained state and not a constant; it does not by itself say the
//! side structure wins:
//!
//!   1. **today** -- two `u64` on every node, 16 B x buckets, no allocation;
//!   2. **a drained side structure** keyed by bucket id, holding an entry only while a bucket is
//!      dirty, popped when it is dumped;
//!   3. **one global minimum** -- 16 bytes for the whole shard.
//!
//! WHAT THE THIRD ARM COSTS THAT IS NOT BYTES, stated here because the arithmetic will make it look
//! free: a single minimum gives the reclaim FLOOR correctly -- that floor is the minimum of these
//! claims either way -- but it cannot give the dump ORDER, and `first_dirty_rank` is a sort key over
//! N buckets. `part4::dump_selection_prioritizes_the_least_recently_dumped_bucket_not_the_lowest_id`
//! is the guard that fails if the order is lost, and `state.rs` states the trade in the field's own
//! doc. The arm is priced because it was asked for; it is not recommended by this module.
//!
//! AND ONE CORRECTION TO THE TARGET, because it changes 24 bytes into 16. `dirty_generation` cannot
//! join a structure that starts empty after a load. It is DURABLE -- the node's deserializer refuses
//! a node without it (`missing_field("dirty_generation")`) -- and it is a per-bucket IDENTITY
//! compared against the copy stored in a dump manifest (`storage_reporting.rs:858`), which is why
//! `storage_bucket_internals.rs:1336` preserves it across a rebuild rather than defaulting it. A
//! transient side structure that is empty after a load would silently answer 0 for every bucket and
//! make every stored manifest appear to match. Only the two TRANSIENT claims are movable, and they
//! are 8 bytes each.
//!
//! THE CONTROL IS THE ROUND-ZERO READING at the default range, where the change predicts nothing:
//! a key lands in a bucket of its own there, so the dirty share and the bucket count move together
//! and the side structure holds one entry per bucket whatever the dump does. Reported at its own
//! row rather than argued.

#![allow(clippy::all)]
use super::*;

use crate::engine::state::BucketNode;
use std::mem::size_of;

const SHARE_SHARD: ShardId = 1;

/// The documented production routing range, `TS_SHARD_END_ROUTING_BUCKET=1023`.
const SHARE_END_BUCKET: u32 = 1023;

/// The cap #1995 measured at, and the one an operator is told to configure.
const SHARE_DUMP_CAP: usize = 64;

/// How many capped rounds to run before giving up on draining.
const MAX_ROUNDS: usize = 12;

/// The two transient claims, which are the only movable ones. `dirty_generation` is durable.
const MOVABLE_BYTES_PER_NODE: usize = 16;

fn share_engine(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("share-cache"),
        dir.join("share-pages"),
        dir.join("share-indexes"),
    )
}

fn share_load(engine: &TemporalEngine, end_routing_bucket: u32) {
    engine.load_shard_with(LoadShardRequest {
        shard_id: SHARE_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
}

fn one_capped_round(engine: &TemporalEngine) {
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: SHARE_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: SHARE_DUMP_CAP,
        ..StorageManagerCycleRequest::default()
    });
}

/// (buckets, dirty buckets, buckets holding a non-zero WAL claim).
fn dirty_census(engine: &TemporalEngine) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARE_SHARD).expect("shard is loaded");
    let mut buckets = 0usize;
    let mut dirty = 0usize;
    let mut claiming = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        buckets += 1;
        if bucket.dirty() {
            dirty += 1;
        }
        if bucket.first_dirty_wal_sequence > 0 {
            claiming += 1;
        }
    }
    (buckets, dirty, claiming)
}

fn write_corpus(engine: &TemporalEngine, keys: &[String], tag: u8) {
    for chunk in keys.chunks(512) {
        let commands = chunk
            .iter()
            .map(|key| Command::StringSet { key: key.clone(), value: vec![tag; 48] })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARE_SHARD,
            commands,
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// WHAT THE DIRTY SHARE DOES AS A DUMP DRAINS IT, AND WHAT EACH OF THREE SHAPES COSTS ALONG THE WAY.
///
/// ONE PHASE IS REPORTED, AND THE SECOND ONE IS AN OPEN QUESTION RATHER THAN A RESULT.
///
///   * **DRAIN**, reported. Write the corpus, then run capped rounds with NO intervening writes,
///     recording the dirty share after every round. This is the ceiling on what draining achieves
///     and it says how many rounds the shipped cap needs to reach a corpus. It is internally
///     consistent across all four arms: exactly 128 buckets leave the dirty set per round,
///     independent of corpus size and range.
///   * **SUSTAINED**, built and WITHDRAWN. A write burst alternating with one capped round is the
///     reading that would actually decide a drained side structure. It read 0.00% dirty at the first
///     step of every arm -- including the arm holding 40,000 dirty buckets -- which cannot be true
///     beside a drain that clears 128 a round. The body carries the note; the number is not
///     reported, because it looked like a decisive win and a broken arm looks exactly like one.
///
/// EVERY DENOMINATOR IS ASSERTED. A run that wrote nothing, a run whose rounds dumped nothing, and a
/// run at a range where every key lands in its own bucket all produce tidy-looking shares, and the
/// third of those is a real population rather than a mistake -- so it is an ARM, reported, not
/// excluded.
///
/// rust-internal: seeds up to 40,000 records per arm and runs up to 12 rounds, no external surface
#[test]
#[ignore = "seeds 8,000 and 40,000 records at two ranges and runs up to 12 dump rounds; run by name"]
fn the_dirty_share_falls_as_a_dump_drains_it_and_the_side_structure_is_priced_along_the_curve() {
    println!(
        "  movable per node: {MOVABLE_BYTES_PER_NODE} B (the two TRANSIENT claims only; \
         dirty_generation is durable and required on load)"
    );
    println!("  dump cap per round: {SHARE_DUMP_CAP}\n");

    for (records, end_bucket, range_label) in [
        (8_000usize, SHARE_END_BUCKET, "0..1023, the configured range"),
        (8_000usize, u32::MAX, "the default range (CONTROL)"),
        (40_000usize, SHARE_END_BUCKET, "0..1023, the configured range"),
        (40_000usize, u32::MAX, "the default range (CONTROL)"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = share_engine(dir.path());
        share_load(&engine, end_bucket);
        let keys: Vec<String> =
            (0..records).map(|index| format!("share-{index:06}")).collect();

        write_corpus(&engine, &keys, b'a');
        let (buckets0, dirty0, claiming0) = dirty_census(&engine);
        assert!(buckets0 > 0, "DENOMINATOR: no bucket after {records} records");
        assert!(
            dirty0 > 0,
            "DENOMINATOR: no dirty bucket after {records} records, so there is nothing for a dump \
             to drain and nothing for a side structure to hold"
        );

        println!("=== {records} records, {range_label}");
        println!(
            "  round 0 (never dumped): buckets {buckets0}  dirty {dirty0} ({:.2}%)  claiming \
             {claiming0} ({:.2}%)",
            100.0 * dirty0 as f64 / buckets0 as f64,
            100.0 * claiming0 as f64 / buckets0 as f64
        );
        println!(
            "  one capped round can reach {SHARE_DUMP_CAP} of {buckets0} buckets = {:.2}%, so \
             draining needs about {} rounds",
            100.0 * SHARE_DUMP_CAP as f64 / buckets0 as f64,
            buckets0.div_ceil(SHARE_DUMP_CAP)
        );

        // ---------------- PHASE 1: DRAIN, no writes.
        let mut drain_shares: Vec<f64> = Vec::new();
        let mut rounds_to_drain: Option<usize> = None;
        let mut dumped_anything = false;
        println!("  DRAIN (no writes between rounds)");
        println!("    round  dirty  share%   today B   drained B   global B");
        for round in 1..=MAX_ROUNDS {
            one_capped_round(&engine);
            let (buckets, dirty, _claiming) = dirty_census(&engine);
            let share = 100.0 * dirty as f64 / buckets as f64;
            drain_shares.push(share);
            if dirty < dirty0 {
                dumped_anything = true;
            }
            let today = MOVABLE_BYTES_PER_NODE * buckets;
            // an entry in the drained structure: the key, the two claims, and a conservative
            // per-entry overhead for whatever map holds it
            let drained = dirty * (size_of::<u32>() + 2 * size_of::<u64>() + 8);
            let global = 2 * size_of::<u64>();
            if round <= 6 || dirty == 0 || round == MAX_ROUNDS {
                println!(
                    "    {round:>5}  {dirty:>5}  {share:>6.2}   {today:>7}   {drained:>9}   {global:>8}"
                );
            }
            if dirty == 0 {
                rounds_to_drain = Some(round);
                break;
            }
        }
        assert!(
            dumped_anything,
            "[{records}/{range_label}] {MAX_ROUNDS} capped rounds did not reduce the dirty count \
             from {dirty0}; the rounds are not dumping and every share below is the same reading \
             repeated"
        );
        match rounds_to_drain {
            Some(n) => println!("    drained to 0% dirty after {n} rounds"),
            None => println!(
                "    still dirty after {MAX_ROUNDS} rounds (lowest share {:.2}%)",
                drain_shares.iter().copied().fold(f64::INFINITY, f64::min)
            ),
        }

        // ---------------- WHAT IS NOT MEASURED HERE, AND WHY IT IS NOT.
        //
        // A SUSTAINED arm -- a write burst, then one capped round, repeated -- is the reading that
        // would actually decide a drained side structure, because it says what the dirty share
        // settles at under load rather than what it falls to when writing stops. It was built and
        // it is NOT reported, because it could not be believed: it read 0.00% dirty at the FIRST
        // step of every arm, including the arm holding 40,000 dirty buckets, while the DRAIN phase
        // above cleans exactly 128 buckets a round on every arm. One capped round cannot clean
        // 40,000 buckets and 128 buckets in the same binary, so that arm was measuring something
        // other than the dirty share it printed.
        //
        // It is recorded as an OPEN QUESTION rather than a result. The plausible readings are that
        // a write burst crosses a threshold escalating to a flush that ignores
        // `max_dump_buckets_per_round`, or that the harness's second engine was not in the state it
        // assumed. Either would matter -- the first is a real and undocumented escalation -- and
        // neither is established. Whoever resolves it should start by asserting the dirty count
        // BEFORE the first round of the sustained arm, which is the clause that would have caught
        // it: a fixture that reached 0% before the round ran is indistinguishable here from a round
        // that cleaned everything.
        let break_even = 100.0 * MOVABLE_BYTES_PER_NODE as f64
            / (size_of::<u32>() + 2 * size_of::<u64>() + 8) as f64;
        println!(
            "  BREAK-EVEN dirty share for a drained structure = {break_even:.2}% -- it holds fewer \
             bytes than the fields only BELOW this"
        );
        let crossed = drain_shares.iter().position(|share| *share < break_even);
        match crossed {
            Some(index) => println!(
                "  this arm falls below the break-even share after round {} of draining\n",
                index + 1
            ),
            None => println!(
                "  this arm never falls below the break-even share within {MAX_ROUNDS} rounds\n"
            ),
        }
    }

    println!(
        "  NOTE on the node: {} B today. Taking the two transient claims off it leaves the tail \
         arithmetic unchanged, because both sit in the eight-aligned group.",
        size_of::<BucketNode>()
    );
}
