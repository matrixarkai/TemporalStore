// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! DOES A COLD SET LISTING PAY FOR MEMBERS THAT ARE ALREADY GONE?
//!
//! `SetMembers` does not read the resident model map. It folds EVERY BLOCK of the object by
//! `append_position`, treating a value and a tombstone as the same kind of statement so the later
//! block wins -- which is what makes a removal stick, and is deliberate: an index entry no longer
//! names its element, and a hand-rolled loop over the payload lacks that precedence and resurrected
//! a removed member once. The fold sits behind a `set_members` response cache that writes
//! invalidate.
//!
//! So a cold listing reads blocks. The question this file answers is WHICH blocks: only the ones
//! holding live members, or the tombstones too. Those are different claims with different fixes,
//! and the only way to tell them apart is to vary the dead count while holding the live count
//! fixed.
//!
//! # WHY THE FIGURE IS BLOCK READS AND NOT WALL TIME
//!
//! This runs on a shared machine whose load average has been north of 100 this session. A latency
//! figure there is mostly the box. `BlockStoreStats::reads` is a count the store itself keeps: it
//! is deterministic, it is the read amplification being asked about, and it does not move when a
//! sibling starts compiling. Wall time is printed beside it for scale and is asserted on by
//! nothing.
//!
//! # TWO TRAPS, BOTH GUARDED RATHER THAN NOTED
//!
//! **A removal only leaves a tombstone if the member's block has already been FOLDED.** Before a
//! fold every member owns its own block, so removing one takes its block away and files nothing.
//! The obvious construction -- add D, remove D -- therefore produces ZERO tombstones, and the
//! sweep would print a flat line from an independent variable pinned at zero and read as "we are
//! level". `compact_shard_blocks` runs before the removals, and the guard is that the removals
//! must ADD blocks over the post-fold baseline. Note it cannot be "blocks > live members": the
//! fold PACKS members into fewer blocks, so that count legitimately drops below the member count.
//!
//! **The response cache will hide the whole defect.** A second listing is served from
//! `set_members` and reads nothing, so a figure taken from it would say we are fine whatever the
//! fold does. The cold arm is measured before anything else reads the key, and the proof that it
//! was cold is that the warm arm immediately after reads EXACTLY ZERO blocks. If the warm arm
//! reads anything, the cache is not serving and the word "cold" meant nothing, so the figure is
//! void rather than merely odd.
//!
//! # WHY ONLY THE SET MODEL IS MEASURED HERE
//!
//! Of the three basic models, set is the only one where the native implementation is a real model
//! and so the only one where a measurement can show us behind. Its sorted set and its list are
//! both RESP-layer constructions over a single string value -- one whole-container GET, a linear
//! member scan, and a whole-container SET on every mutation -- so copying either would cost a
//! factor of the container size on every write and turn ordered reads into full decodes. Those two
//! were answered by reading, and needed no run. Its set is an OR-Set over a per-member persistent
//! map with timestamped tombstones, which is the same shape as ours; the one place the two designs
//! could diverge in cost is what a listing walks, and that is this file.

use super::*;
use std::collections::BTreeSet;

/// Live members, HELD FIXED across the sweep. If this moved with the tombstone count the two
/// claims -- "pays for dead members" and "pays for members" -- would be inseparable.
const LIVE: usize = 64;
/// The independent variable: how many members are removed AFTER the fold.
const TOMBSTONES: [usize; 4] = [0, 64, 256, 1024];
const KEY: &str = "cold-set-listing";

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "cold-set".to_string(),
        shard_uri: "local://cold-set/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn member_bytes(index: usize) -> Vec<u8> {
    format!("member-{index:06}").into_bytes()
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// The listing under test. NEVER call this before the cold measurement -- it populates the very
/// cache whose absence the cold arm depends on.
fn listed(engine: &TemporalEngine) -> BTreeSet<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: KEY.to_string(),
        },
    });
    assert!(response.status.ok, "SMEMBERS failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("SetMembers answered {other:?}"),
    }
}

/// How many `set` blocks the bucket index holds, read off the engine's own index. Only one set
/// object exists in this fixture, so a shard-wide count is that object's count.
fn set_blocks(engine: &TemporalEngine) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut blocks = 0;
    for (_routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        for (_handle, block) in bucket.block_index.iter() {
            if block.model_id.as_str() == "set" {
                blocks += 1;
            }
        }
    }
    blocks
}

struct Row {
    tombstones: usize,
    blocks_after_fold: usize,
    blocks_after_removes: usize,
    cold_reads: u64,
    warm_reads: u64,
    cold_us: u128,
    warm_us: u128,
}

fn drive(tombstones: usize) -> Row {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine);

    for index in 0..(LIVE + tombstones) {
        write(
            &engine,
            Command::SetAdd {
                key: KEY.to_string(),
                member: member_bytes(index),
            },
        );
    }

    // FOLD BEFORE REMOVING. See the module note: without this the removals take their own blocks
    // with them and file no tombstone at all.
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let blocks_after_fold = set_blocks(&engine);

    for index in LIVE..(LIVE + tombstones) {
        write(
            &engine,
            Command::SetRemove {
                key: KEY.to_string(),
                member: member_bytes(index),
            },
        );
    }
    let blocks_after_removes = set_blocks(&engine);

    // THE FOLD-FIRST GUARD, and it is the experiment rather than a precaution. If the removals
    // added no block then nothing was tombstoned, the sweep's independent variable is zero at
    // every point, and the flat line that follows would be read as "we are level".
    assert!(
        tombstones == 0 || blocks_after_removes > blocks_after_fold,
        "removing {tombstones} folded members added no block ({blocks_after_fold} -> \
         {blocks_after_removes}), so no tombstone was filed and this sweep's independent variable \
         is pinned at zero -- the fold before the removals is what makes a tombstone necessary, \
         and any flat result from here is an artefact"
    );

    // COLD. Nothing has listed this key yet, so `set_members` holds nothing for it.
    let reads_before_cold = engine.block_store.stats().reads;
    let started = std::time::Instant::now();
    let members = listed(&engine);
    let cold_us = started.elapsed().as_micros();
    let cold_reads = engine.block_store.stats().reads - reads_before_cold;

    assert_eq!(
        members.len(),
        LIVE,
        "the listing must answer exactly the {LIVE} live members at every point in the sweep, or \
         the live count is moving with the dead count and the two claims cannot be separated"
    );

    // WARM, immediately after, same key: this is the coldness PROOF.
    let reads_before_warm = engine.block_store.stats().reads;
    let started = std::time::Instant::now();
    let warm_members = listed(&engine);
    let warm_us = started.elapsed().as_micros();
    let warm_reads = engine.block_store.stats().reads - reads_before_warm;
    assert_eq!(warm_members.len(), LIVE, "the warm listing must agree with the cold one");

    assert_eq!(
        warm_reads, 0,
        "the warm listing read {warm_reads} blocks, so `set_members` is not serving it -- which \
         means the arm labelled cold was not distinguishable from a warm one and every figure in \
         this row is void"
    );
    assert!(
        cold_reads > 0,
        "the cold listing read no blocks at all, so it did not fold anything and this file is \
         measuring something other than the fold"
    );

    Row {
        tombstones,
        blocks_after_fold,
        blocks_after_removes,
        cold_reads,
        warm_reads,
        cold_us,
        warm_us,
    }
}

/// rust-internal: a cold set listing's block reads against the number of removed members
#[test]
fn what_a_cold_set_listing_reads_against_the_tombstone_count() {
    println!(
        "
  load average now: {}
  live members held fixed at {LIVE}
",
        std::fs::read_to_string("/proc/loadavg").unwrap_or_else(|_| "unavailable".to_string()).trim()
    );
    println!("  tombstones  blocks(fold->removes)  cold_reads  warm_reads  cold_us  warm_us");

    let rows: Vec<Row> = TOMBSTONES.iter().copied().map(drive).collect();
    for row in &rows {
        println!(
            "  {:>10}  {:>9} -> {:<9}  {:>10}  {:>10}  {:>7}  {:>7}",
            row.tombstones,
            row.blocks_after_fold,
            row.blocks_after_removes,
            row.cold_reads,
            row.warm_reads,
            row.cold_us,
            row.warm_us
        );
    }

    let baseline = rows.first().expect("the sweep ran");
    let widest = rows.last().expect("the sweep ran");
    println!(
        "
  live={LIVE} fixed; tombstones {} -> {}; cold block reads {} -> {}
",
        baseline.tombstones, widest.tombstones, baseline.cold_reads, widest.cold_reads
    );

    // THE CLAIM IS PINNED IN THE DIRECTION THAT IS TRUE TODAY, and the message tells whoever
    // breaks it which way the finding went rather than telling them to restore a number.
    //
    // A cold listing folds every block of the object. Tombstone blocks are blocks, so the reads
    // are expected to grow with the number of removed members even though the answer does not --
    // which is the one place our set could be dearer than an OR-Set that walks live map entries
    // after load. If this assertion ever fails because the growth has GONE, the fold has stopped
    // reading dead blocks: that is the improvement this measurement argues for, so move the bound
    // and rewrite the paragraph, do not restore the cost.
    assert!(
        widest.cold_reads > baseline.cold_reads,
        "a cold listing read {} blocks with {} tombstones and {} with {} -- the growth with the \
         dead count has gone. If that is deliberate, the fold now skips blocks that hold only \
         removed members and this bound should be inverted rather than restored",
        baseline.cold_reads,
        baseline.tombstones,
        widest.cold_reads,
        widest.tombstones
    );
}
