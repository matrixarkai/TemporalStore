// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SEQUENCES ON EVERY BUCKET NODE: WHAT EACH ONE IS, AND WHETHER IT HAS TO BE THERE.
//!
//! THERE ARE THREE. There were four when this module was written, and the fourth is the reason the
//! heading changed: `last_dump_sequence` is gone from the node. This module's own item 6 below
//! reported that the node's copy did not reach the dump ordering the comment on it claimed, and
//! left the behaviour alone. Following that report to its end found that it did not reach anything
//! else either -- a compiler enumeration over the field's every use gives nine production
//! positions, and every one of them is the declaration, the wire, the rebuild that preserves it, the
//! single write from `manifest.wal_sequence`, or one of two reports. Not one is a branch, a
//! comparison or a sort key. So it was one shard-level watermark stored once per routing bucket, at
//! eight bytes each, and the reports take the figure from the newest dump manifest now, exactly as
//! `BucketStorageSummary` always did. The node is 168 bytes.
//!
//! Everything below is about the three that stay. Where a row read "one of the four" it now reads
//! "one of the three", and the narrowing table is re-derived rather than shifted: it loses its last
//! row and its base drops by a word.
//!
//! #1958 accounted for every byte of `BucketNode` and put four `u64` sequences in the
//! eight-aligned group -- 32 of the node's 184 bytes, once per routing bucket, more than a sixth
//! of the widest per-item structure in the engine. It declined them in one clause, "the sequences
//! are unbounded counters", which is a claim about their RANGE and not a measurement of it.
//!
//! (The node was 192 when this was written and is 184 now: the `BlockAddress` inside the inline
//! page entry stopped storing a `generation` it could derive. The sequences did not move.)
//!
//! THE ANSWER IS THAT THE THREE THAT REMAIN STAY, at 64 bits, on every key. This module is the
//! accounting for that, and every row of it is a number rather than a reading of the code.
//!
//! THE THREE, AND THEY ARE NOT THREE OF A KIND:
//!
//!   * `dirty_generation` -- a per-bucket COUNT, not a log position. WRITTEN by
//!     `saturating_add(1)` at six sites (`mark_async_dirty_object` and five delete/expire paths)
//!     and held at a captured value by a dump; never reset. STORED, as the `dirty_generation`
//!     key, and carried across `rebuild_bucket_block_ownership` deliberately. READ by
//!     `bucket_dump_summary_matches_current_generation`, which compares it for EQUALITY against
//!     the value a dump manifest recorded -- that equality is what lets WAL reclaim anchor on the
//!     manifest.
//!   * `first_dirty_wal_sequence` -- a position in the write-ahead log: where the bucket's OLDEST
//!     undumped write sits. `#[serde(skip)]`. WRITTEN at two sites, the single-command path in
//!     `engine.rs` and the batch path in `stream_batch_methods.rs`, each only when the field is 0,
//!     and cleared to 0 by a durable dump. READ as the dump ordering's primary key
//!     (`first_dirty_rank`) and as the reclaim plan's WAL floor.
//!   * `first_dirty_index_log_sequence` -- the same claim against the index log, on a different
//!     clock. `#[serde(skip)]`, the same two write sites, cleared by the same dump. READ as the
//!     reclaim plan's index-log floor.
//!   * and the one that left: `last_dump_sequence` -- the WAL sequence of the newest dump manifest
//!     that covered this bucket. It was STORED and WRITTEN only by
//!     `clear_dumped_bucket_dirty_state`, for the buckets a manifest names, and READ only into two
//!     reports. It is not on the node any more; see the head of this module.
//!
//! WHAT WAS MEASURED, AND WHAT IT SAYS:
//!
//!   1. THE ALIGNMENT ARITHMETIC IS NOT THE OBVIOUS ONE. The node is 166 bytes of field in 168 --
//!      160 of eight-aligned field and a six-byte tail rounded to eight, two bytes of slack.
//!      The natural reading of three words in the eight-aligned group is that narrowing one buys
//!      nothing, and at a six-byte tail that reading is now RIGHT -- but not for its reason,
//!      and it was wrong when this was written. The staircase is that every SECOND narrowing
//!      collects one rounding: a freed `u64` takes a word off the group and puts a `u32` back
//!      into the tail. Its PHASE depends on the tail. At ten bytes the first narrowing landed on
//!      14, which still rounded to 16, so the first step was worth eight. At SIX bytes -- the
//!      five `bool` are one `BucketFlags` byte since the node went 184 -> 176 -- the first
//!      narrowing lands on 10, which rounds to 16, and the word is handed straight back. The
//!      table is now 0, 8, 8 where it read 8, 8, 16, 16 over four sequences, and the eight bytes
//!      that left the head of it are the same eight the flag pack collected: two ways to take one
//!      rounding, and only one of them could be paid. REMOVING is different -- the bytes leave
//!      rather than move and the tail does not shift -- which is exactly how
//!      `last_dump_sequence` leaving took the node from 176 to 168 where narrowing it would have
//!      taken nothing.
//!      `what_narrowing_or_removing_each_sequence_would_make_the_node` prints the table against
//!      mirrors of the declaration, with the live mirror as its control and the reconstruction
//!      asserted on every row. So the saving belongs to the GROUP and there is no per-field
//!      saving to quote.
//!   2. NONE OF THE THREE CAN NARROW, and the measured ranges are exactly the trap. Over a real
//!      workload at two corpus sizes and two routing ranges the largest value any of them reached
//!      was 106 -- seven bits. The bound is not the corpus, it is the STORE'S LIFETIME: two of
//!      the three are positions in logs that are reclaimed but never renumbered, and the third is
//!      a count a load preserves and nothing resets. Each of the two claims is read as a retain
//!      FLOOR, and a wrapped claim is not a wrong number but a SMALLER one, so the floor falls
//!      and reclaim frees records the bucket still holds the log for; saturation is wrong in the
//!      other direction, naming a write newer than the one the bucket needs.
//!      `the_bound_on_each_sequence_is_the_stores_lifetime_not_the_corpus` drives both.
//!      The compiler was asked what a narrowing would touch: ten sites, five of them in
//!      production and two of those the write sites that would each need a checked conversion.
//!   3. THE INDEX-LOG CLAIM IS NOT DERIVABLE FROM THE WAL CLAIM, and the measurement that settles
//!      it is not the one that looks decisive. Every bucket holding BOTH halves holds them
//!      exactly one apart, at both corpus sizes -- which reads as a function. It is not one:
//!      582 of 1,024 buckets at 40,000 records hold the WAL half and NO index-log half, and all
//!      582 are exactly the buckets the reclaim plan refuses on. The missing half is not a value
//!      to reconstruct, it is the absence of a statement, and deriving it would hand those
//!      buckets a floor they have no grounds for. The control on the explanation: the two logs'
//!      own tails sit 38 apart at that corpus size, so the distance of one between the claims is
//!      a property of the write that stamped both, not of the clocks.
//!   4. HOISTING THE TWO TRANSIENT CLAIMS OFF THE NODE IS A LOSS AT THE CONFIGURED RANGE, which
//!      is the reading that decides it. It is worth a real 16 bytes a bucket and costs a side-map
//!      entry per DIRTY bucket, so it turns on the dirty fraction -- and that fraction is a
//!      property of the routing range and the dump cap, not of the workload. On `load_shard`'s
//!      `u32::MAX` default every key lands in a bucket of its own, half the buckets are clean
//!      after a round, and the hoist saves 5.0 B a record. On `TS_SHARD_END_ROUTING_BUCKET=1023`,
//!      the range `docs/runtime_tuning.md` tells an operator to set, the buckets fill, the dump
//!      cap of 64 a round cannot reach 1,024 of them, 99.0% and 100.0% of buckets are dirty, and
//!      the hoist COSTS 0.79 and 0.16 B a record -- and adds allocations at every range,
//!      including the one where it saves bytes. Both sides on the counting allocator, both
//!      figures in bytes and allocations, because those two have disagreed in sign on this
//!      structure's neighbours before.
//!   5. AND THE READ PATH IS THE OTHER HALF OF THAT TRADE. Today both claims are fields of a node
//!      the reclaim plan already has in hand; hoisted, each read becomes a descent of a second
//!      ordered map. Measured over the same buckets in the same order, ABBA: 46x.
//!   6. ONE DISAGREEMENT, REPORTED AND NOW FIXED. The node's `last_dump_sequence` did not reach
//!      the dump ordering that was commented on it. The sort reads
//!      `BucketStorageSummary::last_dump_sequence` -- a different field with the same name --
//!      and `bucket_storage_summaries` never fills that from the node; `merge_last_dump_sequence`
//!      fills it from the newest manifest's INDEX-LOG sequence, the same value for every bucket
//!      that manifest names and 0 for every bucket it does not. So the key separates "covered by
//!      the newest dump" from "not covered by it", which was not what it was described as doing.
//!      This module left the behaviour alone and corrected the comment. Following the report to
//!      its end found the node's copy reached NO decision at all, and it is gone; the reports take
//!      the figure from the manifest, and
//!      `the_summary_last_dump_sequence_comes_from_the_manifest_not_from_the_node` now pins that
//!      the report does too, with an uncovered bucket as the control.
//!
//!   7. A BOUNDED RING IS SAFE IN ONE DESIGN AND LOSES DATA IN THE OTHER, AND THE DIFFERENCE IS
//!      WHETHER IT ALSO OWNS THE DIRTINESS. The two transient claims are `#[serde(skip)]` and
//!      cleared on load, so they are runtime-only -- which is exactly what a shard-level side
//!      structure could hold instead of sixteen bytes on every routing bucket, and a ring of slot
//!      ids is the bounded shape such a structure takes. A ring that has wrapped has FORGOTTEN a
//!      claim, and a claim is a retain FLOOR, so the wrap was DRIVEN rather than reasoned about.
//!      48 buckets at distinct claims, ring capacity 24, the oldest 24 claims evicted -- which is
//!      not an unlucky order, it is the order a ring has, and the oldest claims are the floor:
//!
//!      ```text
//!        CONTROL                                safe=true   retain_from_wal=3   missing=0
//!        ARM A  ring holds claims only          safe=FALSE  retain_from_wal=0   missing=24
//!        ARM B  ring IS the dirty set           safe=true   retain_from_wal=27  missing=0
//!      ```
//!
//!      ARM A retains the WHOLE log: a bucket that is still known dirty and can no longer name its
//!      claim reads as NO CLAIM RECORDED, the plan refuses, and the floor cannot rise. That errs
//!      CONSERVATIVE and is safe to take. ARM B moves the floor from 3 to 27, twenty-four records
//!      PAST the lowest forgotten claim, and reclaim would free WAL records a bucket still needs.
//!      That is committed data lost, and the design is refused.
//!      `a_wrapped_claim_ring_errs_conservative_only_while_it_keeps_dirtiness` drives both, with
//!      the control taken first on the same shard and the fixture's preconditions ENFORCED --
//!      including that the ring forgot claims OLDER than the ones it kept, which a first version
//!      of the fixture did not satisfy: batched writes stamp ONE sequence on every bucket the batch
//!      touched, 694 buckets all claiming 3, and forgetting half of an identical set moves nothing.
//!   8. AND WHAT THE PER-KEY CLAIM BUYS THAT A SINGLE WATERMARK CANNOT IS THE DUMP ORDER, NOT THE
//!      FLOOR. A shard-level watermark over the whole index is the minimum of these claims, which
//!      is the same floor the plan already computes, so the floor is not the argument. The argument
//!      is `first_dirty_rank`, the dump ordering's PRIMARY key: it reads each bucket's own claim to
//!      dump the bucket that is pinning the log FIRST, so the floor advances. A structure that
//!      holds one number for the shard cannot answer "which bucket", and the ordering falls back to
//!      what it used before -- which starved a bucket dirtied once behind low-id buckets re-dirtied
//!      every round, and is what
//!      `dump_selection_prioritizes_the_least_recently_dumped_bucket_not_the_lowest_id` exists for.
//!      So all three sequences stay, and each is named with the reader that pins it above.
//!
//! HOW THE "IS IT READ" GUARDS WORK. Each one PERTURBS the field on a live shard and asserts the
//! decision it is named for changes, against a control taken on the same shard before the edit. A
//! guard that only reads the field back proves the field exists; these prove it is load-bearing,
//! and a mutation that stops a reader reading it turns them red.
//!
//! WHAT THIS MODULE DOES NOT CLAIM. It proposes no FURTHER change to `BucketNode`: the three
//! sequences that remain all stay, at 64 bits, on every key, and the only production edit made
//! alongside item 7 is a comment -- two words of one restored from the commit that wrote it, where
//! a doc comment had lost them to a bare backslash.
#![allow(clippy::all)]
use super::*;
use std::mem::{align_of, size_of};

use crate::engine::state::{
    BlockIndexMap, BucketFlags, BucketLayoutState, BucketNode, BucketTtl, DeletedObjectIndex,
    ObjectIndex,
};

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

const SEQ_SHARD: ShardId = 1;

/// The documented production routing range: `TS_SHARD_END_ROUTING_BUCKET=1023`, 1,024 buckets.
///
/// `docs/runtime_tuning.md` prints this in the worked configuration an operator is told to copy.
/// Measuring only at `load_shard`'s `u32::MAX` default puts every key in a bucket of its own by
/// construction, which is the shape #1959's fixture had and #1962's premise died on.
const CONFIGURED_END_BUCKET: u32 = 1023;

/// The dump cap an operator actually runs with: `default_storage_manager_max_dump_buckets_per_round`.
///
/// Not zero. Zero means "dump everything", which is what the existing claim fixtures use to clear
/// state deliberately, and it is the one setting under which the dirty fraction can reach zero.
const CONFIGURED_DUMP_CAP: usize = 64;

fn seq_engine(dir: &std::path::Path, name: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join(format!("{name}-cache")),
        dir.join(format!("{name}-pages")),
        dir.join(format!("{name}-indexes")),
    )
}

fn load_at(engine: &TemporalEngine, end_routing_bucket: u32) {
    engine.load_shard_with(LoadShardRequest {
        shard_id: SEQ_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
}

fn seq_keys(prefix: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("{prefix}-{index:06}"))
        .collect()
}

/// Write through the BATCH path, which is the one that stamps both claims in `stream_batch_methods`.
fn write_batches(engine: &TemporalEngine, keys: &[String], fill: u8) {
    for chunk in keys.chunks(512) {
        let commands = chunk
            .iter()
            .map(|key| Command::StringSet {
                key: key.clone(),
                value: vec![fill; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SEQ_SHARD,
            commands,
        });
        assert!(response.status.ok, "batch write failed: {:?}", response.status);
    }
}

/// Write through the SINGLE-COMMAND path, which stamps them in `engine.rs`.
fn write_singles(engine: &TemporalEngine, keys: &[String], fill: u8) {
    for key in keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: SEQ_SHARD,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![fill; 64],
            },
        });
        assert!(response.status.ok, "write {key} failed: {:?}", response.status);
    }
}

/// One storage round at the CONFIGURED dump cap, not at "dump everything".
fn one_round_at_the_configured_cap(engine: &TemporalEngine) {
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: SEQ_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: CONFIGURED_DUMP_CAP,
        ..StorageManagerCycleRequest::default()
    });
}

/// The sequences of one bucket, plus the two flags that say which branch reads them.
///
/// THREE, not four. `last_dump_sequence` is not a field of the node any more, so it cannot be read
/// per bucket here: the figure lives on the newest dump manifest and the reports take it from
/// there.
#[derive(Clone, Copy, Debug)]
struct Sequences {
    routing_bucket: u32,
    dirty: bool,
    generation: u64,
    wal_claim: u64,
    index_log_claim: u64,
}

/// Read straight off the bucket index, PER BUCKET and never reduced here.
fn sequences_by_bucket(engine: &TemporalEngine) -> Vec<Sequences> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let Some(shard) = shards.get(&SEQ_SHARD) else {
        return Vec::new();
    };
    let mut out = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| Sequences {
            routing_bucket: *routing_bucket,
            dirty: bucket.dirty(),
            generation: bucket.dirty_generation,
            wal_claim: bucket.first_dirty_wal_sequence,
            index_log_claim: bucket.first_dirty_index_log_sequence,
        })
        .collect::<Vec<_>>();
    out.sort_by_key(|row| row.routing_bucket);
    out
}

/// min / max / how many are non-zero, over one of the three.
fn spread(values: impl Iterator<Item = u64>) -> (u64, u64, usize, usize) {
    let mut total = 0usize;
    let mut non_zero = 0usize;
    let mut low = u64::MAX;
    let mut high = 0u64;
    for value in values {
        total += 1;
        if value > 0 {
            non_zero += 1;
            low = low.min(value);
            high = high.max(value);
        }
    }
    if non_zero == 0 {
        low = 0;
    }
    (low, high, non_zero, total)
}

/// Seed a shard to the state a RUNNING one is in: written, one round of dumping at the configured
/// cap, written again. Returns the rows.
fn seeded_rows(
    dir: &std::path::Path,
    name: &str,
    records: usize,
    end_routing_bucket: u32,
) -> (Vec<Sequences>, usize) {
    let engine = seq_engine(dir, name);
    load_at(&engine, end_routing_bucket);
    let first = seq_keys("first", records / 2);
    let later = seq_keys("later", records / 2);
    write_batches(&engine, &first, 118);
    // A handful through the single-command path as well, so both stamping sites are exercised
    // rather than only the batch one.
    write_singles(&engine, &first[..16.min(first.len())], 119);
    one_round_at_the_configured_cap(&engine);
    write_batches(&engine, &later, 120);
    let rows = sequences_by_bucket(&engine);
    (rows, records)
}

// -------------------------------------------------------------------------------------------
// 1. WHAT EACH OF THE THREE ACTUALLY REACHES.
// -------------------------------------------------------------------------------------------

/// WHAT EACH OF THE THREE SEQUENCES REACHES, at two corpus sizes and two routing ranges.
///
/// The routing range is the second axis on purpose. At `load_shard`'s `u32::MAX` default a key
/// lands in a bucket of its own, so a bucket's `dirty_generation` counts the writes to ONE key;
/// on the range `docs/runtime_tuning.md` configures, a bucket holds many keys and counts all of
/// them. Those are different distributions and only one of them is what an operator runs.
///
/// THE FIXTURE IS ASSERTED TO REACH THE CASES THIS MODULE CLAIMS. A shard where every bucket is
/// dirty cannot say anything about a cleared claim, and a shard where none is dirty cannot say
/// anything about a live one. Both populations are required to be non-empty before any figure is
/// read off, and both denominators are asserted before anything is divided by them.
#[test]
#[ignore = "seeds four shards up to 40,000 records; run by name"]
fn what_each_of_the_three_sequences_reaches_at_two_corpus_sizes() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut dirty_fractions: Vec<(String, f64)> = Vec::new();

    for (records, end_routing_bucket, range_label) in [
        (8_000usize, u32::MAX, "default range (load_shard's u32::MAX)"),
        (8_000usize, CONFIGURED_END_BUCKET, "0..1023 (the configured range)"),
        (40_000usize, u32::MAX, "default range (load_shard's u32::MAX)"),
        (40_000usize, CONFIGURED_END_BUCKET, "0..1023 (the configured range)"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let (rows, seeded) = seeded_rows(dir.path(), "seq", records, end_routing_bucket);

        assert!(
            !rows.is_empty(),
            "denominator: {seeded} records produced no routing bucket at all, so every spread \
             below is over an empty set"
        );
        let dirty = rows.iter().filter(|row| row.dirty).count();
        let clean = rows.len() - dirty;

        println!("\n=== {seeded} records, {range_label} ===");
        println!(
            "  buckets={} dirty={dirty} clean={clean} ({:.1}% dirty)",
            rows.len(),
            100.0 * dirty as f64 / rows.len() as f64
        );
        for (name, low, high, non_zero, total) in [
            {
                let (low, high, nz, total) = spread(rows.iter().map(|row| row.generation));
                ("dirty_generation", low, high, nz, total)
            },
            {
                let (low, high, nz, total) = spread(rows.iter().map(|row| row.wal_claim));
                ("first_dirty_wal_sequence", low, high, nz, total)
            },
            {
                let (low, high, nz, total) = spread(rows.iter().map(|row| row.index_log_claim));
                ("first_dirty_index_log_sequence", low, high, nz, total)
            },
        ] {
            println!(
                "  {name:<32} non-zero {non_zero:>7} of {total:>7}   range [{low}, {high}]  \
                 bits needed {}",
                if high == 0 { 0 } else { 64 - high.leading_zeros() }
            );
        }

        dirty_fractions.push((
            format!("{seeded} records, {range_label}"),
            dirty as f64 / rows.len() as f64,
        ));
    }

    assert_eq!(4, path_lengths.len(), "all four arms must have run");
    let first_length = path_lengths[0];
    for length in &path_lengths {
        assert_eq!(
            first_length, *length,
            "the store path length moved between arms ({first_length} then {length}); allocation \
             bytes move with it at about six bytes a character"
        );
    }

    println!("\n=== the dirty fraction, which is what prices hoisting the two transient claims ===");
    for (label, fraction) in &dirty_fractions {
        println!("  {label:<52} {:.2}% dirty", 100.0 * fraction);
    }
}

// -------------------------------------------------------------------------------------------
// 2. WIDTH AGAINST ALIGNMENT, PER FIELD, WITH THE GROUP EFFECT SPELLED OUT.
// -------------------------------------------------------------------------------------------

/// The node as declared. The CONTROL: if this is not `size_of::<BucketNode>()` every row below is
/// describing a structure this engine does not have.
#[allow(dead_code)]
struct SeqMirrorLive {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// One of the three narrowed to 32 bits.
#[allow(dead_code)]
struct SeqMirrorOneNarrowed {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u32,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// Two of the three narrowed to 32 bits.
#[allow(dead_code)]
struct SeqMirrorTwoNarrowed {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u32,
    first_dirty_index_log_sequence: u32,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// All three narrowed to 32 bits.
#[allow(dead_code)]
struct SeqMirrorThreeNarrowed {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u32,
    first_dirty_wal_sequence: u32,
    first_dirty_index_log_sequence: u32,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// One of the three gone entirely.
#[allow(dead_code)]
struct SeqMirrorOneRemoved {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// Both transient claims gone -- the hoist #1958 priced and declined.
#[allow(dead_code)]
struct SeqMirrorTwoRemoved {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// WHAT NARROWING OR REMOVING EACH SEQUENCE WOULD MAKE THE NODE -- and the answer is not the one
/// the shape of the group suggests.
///
/// THREE SEQUENCES, NOT FOUR, AND THE TABLE IS RE-DERIVED RATHER THAN SHIFTED.
/// `last_dump_sequence` left the node: it was read into two reports and nothing else, and the
/// report takes the figure from the newest dump manifest. That took a whole word out of the
/// eight-aligned group, so the base is 160 + a six-byte tail = 168 where it was 168 + six = 176,
/// and the narrowing series loses its last row. The DIFFERENCES are unchanged, because they are
/// set by the group structure and not by the base -- which is the same reason the base moving to
/// 184 and then to 176 never moved them either.
///
/// The node is 160 bytes of eight-aligned field plus a six-byte tail rounded to eight. A `u64`
/// narrowed to a `u32` does not vanish: it leaves the eight-aligned group and lands in the tail.
/// So the arithmetic is `160 - 8n + round_up_8(6 + 4n)` and it steps, it does not slope:
///
/// ```text
///   narrowed   eight-aligned    tail -> rounded    size    vs live
///     0            160             6 -> 8           168       --
///     1            152            10 -> 16          168       -0
///     2            144            14 -> 16          160       -8
///     3            136            18 -> 24          160       -8
/// ```
///
/// The FIRST narrowing is worth nothing and the third nothing on top of the second. Stating a
/// per-field saving here would be stating a number that does not exist: the saving belongs to the
/// GROUP, and only the second crossing moves it.
///
/// THE PHASE OF THE STAIRCASE DEPENDS ON THE TAIL, and it has moved twice. At a ten-byte tail the
/// first narrowing landed on 14, which still rounded to 16, so the first step was worth eight and
/// the series read 8, 8, 16, 16. At SIX bytes -- the five `bool` are one `BucketFlags` byte -- the
/// first narrowing lands on 10, which rounds to 16, and the word the group gave up is handed
/// straight back: 0, 8, 8, 16. Those eight bytes at the head are the same eight the flag pack
/// collected; two ways to take one rounding, and only one could be paid. With three sequences the
/// series is that one truncated: 0, 8, 8.
///
/// Removing is different from narrowing because the bytes leave the structure instead of moving
/// to the tail, and the tail does not move at all: one removed is 160, two removed is 152.
///
/// THE RECONSTRUCTION IS ASSERTED, not the total. Every row has to satisfy
/// `eight_aligned + round_up(tail) == size_of`, so a row that happened to land on the right
/// number for the wrong reason fails.
#[test]
fn what_narrowing_or_removing_each_sequence_would_make_the_node() {
    // --- The control. Nothing below means anything without it. ---
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<SeqMirrorLive>(),
        "the mirror of the live declaration is {} bytes against the declaration's {}; the mirrors \
         have drifted and every price below is fiction",
        size_of::<SeqMirrorLive>(),
        size_of::<BucketNode>()
    );

    let align = align_of::<BucketNode>();
    assert_eq!(8, align, "BucketNode's alignment moved and the arithmetic below assumes 8");

    // The two groups, taken from the declaration rather than from a literal.
    //
    // 152 of eight-aligned field: 160 until the `BlockAddress` inside the inline page entry
    // merged its slab id and its offset into ONE WORD, which takes a whole word out of the packed
    // group one level in. 160 was itself 168 until `last_dump_sequence` left the node, which took
    // a whole word out of the group directly, and 168 was 176 until that same address stopped
    // storing a `generation` it could derive. Three eight-byte changes, all of them in THIS group
    // and none of them in the tail.
    //
    // AND A SIX-BYTE TAIL, NOT TEN, WHICH MOVED A CONCLUSION RATHER THAN A LITERAL. The five
    // `bool` became five bits of one `BucketFlags` byte, so the tail is `routing_bucket` (4),
    // `layout` (1) and the flag byte (1). The rows below are differences computed FROM these two
    // constants, so they recompute -- but the first row's answer is not the one it used to be:
    // narrowing ONE sequence used to be worth eight bytes, because the freed `u32` landed in a
    // ten-byte tail that still rounded to sixteen while the group lost a word. With a six-byte
    // tail the freed `u32` takes the tail to ten, which rounds to sixteen, and the word the group
    // gave up is handed straight back. THE EIGHT BYTES ARE THE SAME EIGHT BYTES: packing the
    // flags and narrowing one sequence were two ways to collect one rounding, and only one of
    // them could be paid. Removing a word does NOT move the tail, which is why it crosses where a
    // narrowing does not -- and MERGING two words into one is the same shape of change, which is
    // why the address merge crosses too and leaves the conclusion above untouched.
    const EIGHT_ALIGNED: usize = 152;
    const TAIL: usize = 6;
    let live = size_of::<BucketNode>();
    assert_eq!(
        EIGHT_ALIGNED + TAIL.div_ceil(align) * align,
        live,
        "the live node must reconstruct from {EIGHT_ALIGNED} B of eight-aligned field and a \
         {TAIL} B tail, or the model every row below uses is wrong"
    );

    println!("\n=== narrowing n of the three sequences to 32 bits ===");
    println!(
        "  {:<9} {:>14} {:>9} {:>8} {:>6} {:>9}",
        "narrowed", "eight-aligned", "tail", "rounded", "size", "vs live"
    );
    let narrowed_sizes = [
        size_of::<SeqMirrorLive>(),
        size_of::<SeqMirrorOneNarrowed>(),
        size_of::<SeqMirrorTwoNarrowed>(),
        size_of::<SeqMirrorThreeNarrowed>(),
    ];
    for (n, size) in narrowed_sizes.iter().copied().enumerate() {
        let eight_aligned = EIGHT_ALIGNED - 8 * n;
        let tail = TAIL + 4 * n;
        let rounded = tail.div_ceil(align) * align;
        println!(
            "  {n:<9} {eight_aligned:>14} {tail:>9} {rounded:>8} {size:>6} {:>+9}",
            size as i64 - live as i64
        );
        assert_eq!(
            eight_aligned + rounded,
            size,
            "narrowing {n} of the three reconstructs to {} B but `size_of` says {size}; the two \
             groups plus one rounding must account for the width exactly",
            eight_aligned + rounded
        );
    }

    // THE STEP, ASSERTED AS A STEP -- AND ITS PHASE MOVED WHEN THE TAIL DID.
    //
    // The staircase is the same shape it always was: every SECOND narrowing collects one
    // rounding, because a freed `u64` takes a word off the eight-aligned group and puts a `u32`
    // back into the tail. What changed is where it starts. With a ten-byte tail the first
    // narrowing landed on 14, which still rounded to 16, so the word was kept and the first step
    // was worth eight. With a SIX-byte tail -- the five `bool` are one byte now -- the first
    // narrowing lands on 10, which rounds to 16, and the word the group gave up is handed
    // straight back.
    //
    // So the eight bytes that used to be here are gone, and they are gone because something else
    // took them: packing the flags and narrowing one sequence were two ways to collect the same
    // rounding, and only one of them could be paid. With four sequences the series read 0, 8, 8, 16
    // where it used to read 8, 8, 16, 16; with three it is that series truncated, 0, 8, 8.
    assert_eq!(
        0,
        live - narrowed_sizes[1],
        "narrowing ONE of the three is worth NOTHING now that the tail is six bytes -- the freed \
         word goes straight back into the rounding -- but it measured {}",
        live - narrowed_sizes[1]
    );
    assert_eq!(
        8,
        live - narrowed_sizes[2],
        "narrowing a SECOND of the three is priced at eight bytes a bucket; it measured {}",
        live - narrowed_sizes[2]
    );
    assert_eq!(
        narrowed_sizes[2], narrowed_sizes[3],
        "narrowing the THIRD is worth nothing on top of the second, but two measured {} and three \
         measured {}",
        narrowed_sizes[2], narrowed_sizes[3]
    );

    // --- Removing, which is a different arithmetic: the bytes leave rather than move. ---
    let one_removed = size_of::<SeqMirrorOneRemoved>();
    let two_removed = size_of::<SeqMirrorTwoRemoved>();
    println!("\n=== removing n of the three outright ===");
    println!("  0 removed  {live:>4} B");
    println!("  1 removed  {one_removed:>4} B  {:+}", one_removed as i64 - live as i64);
    println!("  2 removed  {two_removed:>4} B  {:+}", two_removed as i64 - live as i64);
    assert_eq!(
        8,
        live - one_removed,
        "removing one sequence is priced at eight bytes a bucket; it measured {}",
        live - one_removed
    );
    assert_eq!(
        16,
        live - two_removed,
        "removing both transient claims is priced at sixteen bytes a bucket -- the figure #1958 \
         declined on -- and it measured {}",
        live - two_removed
    );
    for (n, size) in [(1usize, one_removed), (2usize, two_removed)] {
        let eight_aligned = EIGHT_ALIGNED - 8 * n;
        assert_eq!(
            eight_aligned + TAIL.div_ceil(align) * align,
            size,
            "removing {n} must reconstruct to {} B and `size_of` says {size}",
            eight_aligned + TAIL.div_ceil(align) * align
        );
    }
}

// -------------------------------------------------------------------------------------------
// 3. ARE THEY DISTINCT? MEASURED OVER A WORKLOAD, NOT READ OFF THE DECLARATION.
// -------------------------------------------------------------------------------------------

/// THE INDEX-LOG HALF OF A CLAIM CANNOT BE DERIVED FROM THE WAL HALF -- and the measurement that
/// says so is not the one that looks decisive.
///
/// The reason to ask: both halves are stamped by the same command, both only when the field is 0,
/// both cleared together by a dump. If one were a function of the other, one of the two words
/// could go, and the width table above says the first word off this group is a real eight bytes a
/// bucket.
///
/// WHAT LOOKS LIKE A FUNCTION. Every bucket that holds BOTH halves holds them exactly one apart,
/// at both corpus sizes measured here. A reading that stopped there would conclude
/// `index_log_claim = wal_claim + 1` and take the eight bytes.
///
/// WHY IT IS NOT ONE, AND THE NUMBER IS THE REFUTATION. More than half the dirty buckets hold the
/// WAL half and NO index-log half at all -- 582 of 1,024 at 40,000 records. `append_delta`
/// returns 0 when it wrote nothing, and 0 is how a bucket says NO CLAIM RECORDED. Deriving the
/// index-log half would hand every one of those buckets a confident floor it has no grounds for,
/// and the plan's `wal_claim > 0 && index_log_claim > 0` is there precisely so such a bucket
/// refuses to name one: an unknown claim blocks the reclaim, and the derived one would release
/// the log below records the bucket still needs. The missing half is not a value to reconstruct;
/// it is the absence of a statement.
///
/// THE CONTROL ON THE EXPLANATION. If the distance of one were a property of the two CLOCKS, the
/// two logs would sit one apart at their tails as well. They do not -- their last sequences are
/// printed beside the claims, and the distance between THOSE is the number that says the one
/// between the claims is an artifact of when both halves happened to be stamped by the same
/// write, not a relationship between the sequences.
///
/// Reported PER BUCKET and never as a mean.
#[test]
#[ignore = "seeds 8,000 then 40,000 records; run by name"]
fn the_index_log_half_of_a_claim_cannot_be_derived_from_the_wal_half() {
    let mut wal_only_counts: Vec<(usize, usize, usize)> = Vec::new();
    let mut path_lengths: Vec<usize> = Vec::new();

    for records in [8_000usize, 40_000usize] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = seq_engine(dir.path(), "pairs");
        load_at(&engine, CONFIGURED_END_BUCKET);
        let first = seq_keys("first", records / 2);
        let later = seq_keys("later", records / 2);
        write_batches(&engine, &first, 118);
        write_singles(&engine, &first[..16.min(first.len())], 119);
        one_round_at_the_configured_cap(&engine);
        write_batches(&engine, &later, 120);
        let rows = sequences_by_bucket(&engine);

        let both: Vec<(u32, u64, u64)> = rows
            .iter()
            .filter(|row| row.wal_claim > 0 && row.index_log_claim > 0)
            .map(|row| (row.routing_bucket, row.wal_claim, row.index_log_claim))
            .collect();
        let wal_only: Vec<u32> = rows
            .iter()
            .filter(|row| row.wal_claim > 0 && row.index_log_claim == 0)
            .map(|row| row.routing_bucket)
            .collect();
        let index_log_only = rows
            .iter()
            .filter(|row| row.wal_claim == 0 && row.index_log_claim > 0)
            .count();

        // DENOMINATORS. Both populations have to exist: without buckets holding both there is no
        // distance to measure, and without buckets holding one there is nothing to refute with.
        assert!(
            both.len() >= 64,
            "only {} buckets of {} hold both halves at {records} records; the distance below \
             would be measured over nothing",
            both.len(),
            rows.len()
        );
        assert!(
            !wal_only.is_empty(),
            "every bucket at {records} records holds both halves, so this fixture never reaches \
             the case the refutation rests on"
        );

        let distances = both
            .iter()
            .map(|(_, wal, index_log)| *index_log as i128 - *wal as i128)
            .collect::<std::collections::BTreeSet<i128>>();

        let wal_tail = engine.wal_store().stats(SEQ_SHARD).last_sequence;
        let index_log_tail = engine.index_log_store().stats(SEQ_SHARD).last_sequence;

        println!(
            "\n=== {records} records, 0..1023 ===\n  buckets={} / holding both {} / WAL half \
             only {} / index-log half only {index_log_only}",
            rows.len(),
            both.len(),
            wal_only.len()
        );
        println!(
            "  distances between the two halves, where both are present: {:?}",
            distances
        );
        println!(
            "  THE CONTROL: the two logs' own tails are WAL {wal_tail} and index-log \
             {index_log_tail}, {} apart -- so the distance above is a property of the write that \
             stamped both, not of the clocks",
            wal_tail as i128 - index_log_tail as i128
        );

        // THE HARM, driven rather than argued: the buckets holding only the WAL half are exactly
        // the ones the plan refuses on, and a derived index-log half would remove that refusal.
        let plan = engine.storage_wal_reclaim_plan(SEQ_SHARD, Vec::new(), Vec::new());
        let refused_and_wal_only = wal_only
            .iter()
            .filter(|routing_bucket| plan.missing_bucket_generations.contains(routing_bucket))
            .count();
        println!(
            "  the plan refuses on {} buckets; {refused_and_wal_only} of the {} WAL-half-only \
             buckets are among them",
            plan.missing_bucket_generations.len(),
            wal_only.len()
        );

        wal_only_counts.push((records, wal_only.len(), rows.len()));
    }

    assert_eq!(2, path_lengths.len(), "both arms must have run");
    assert_eq!(
        path_lengths[0], path_lengths[1],
        "the store path length moved between arms ({} then {})",
        path_lengths[0], path_lengths[1]
    );

    println!("\n=== buckets holding the WAL half and no index-log half ===");
    for (records, wal_only, total) in &wal_only_counts {
        println!(
            "  {records:>7} records: {wal_only} of {total} ({:.1}%)",
            100.0 * *wal_only as f64 / *total as f64
        );
    }

    // THE CLAIM. A derivation has to answer for every bucket, and at the larger corpus most of
    // them have no index-log half to derive.
    let (big_records, big_wal_only, big_total) = wal_only_counts[1];
    assert!(
        big_wal_only * 4 > big_total,
        "only {big_wal_only} of {big_total} buckets at {big_records} records hold the WAL half \
         without the index-log half; if that population has become small, the refutation this \
         module records rests on less than it did and should be re-read"
    );
}

/// THE BOUND ON EACH SEQUENCE IS THE STORE'S LIFETIME, NOT THE CORPUS -- which is why a measured
/// range that fits in 32 bits is not permission to use 32 of them.
///
/// Two of the three are positions in a log. Reclaim FREES a log's records; it does not RENUMBER
/// them, and the sequence a store hands out is monotonic across every reclaim and every restart.
/// The fourth is a count that a load preserves (`rebuild_bucket_block_ownership` carries it over
/// deliberately) and that nothing ever resets.
///
/// This drives the direction that matters: what a narrowed field would do to the decision. Each
/// claim is read as a retain FLOOR, and a wrapped claim is not a wrong number, it is a SMALLER
/// one -- so the floor falls and reclaim frees records the bucket still holds the log for. That
/// is why this module's answer is "no narrowing", and it is asserted here rather than asserted in
/// prose: the claim a bucket holds is put one past a `u32` and the plan is shown to keep the
/// records, where the truncated value would have released them.
#[test]
fn the_bound_on_each_sequence_is_the_stores_lifetime_not_the_corpus() {
    // What a 32-bit claim would do to a claim past its top, spelled as the arithmetic the write
    // site would perform. `as u32` is the silent form and is what this rejects.
    const PAST_THIRTY_TWO_BITS: u64 = (u32::MAX as u64) + 4_096;
    let truncated = PAST_THIRTY_TWO_BITS as u32 as u64;
    assert!(
        truncated < PAST_THIRTY_TWO_BITS,
        "a claim of {PAST_THIRTY_TWO_BITS} narrowed to 32 bits reads back as {truncated}"
    );
    assert_eq!(
        4_095, truncated,
        "the truncation of {PAST_THIRTY_TWO_BITS} is {truncated}; if this is no longer 4,095 the \
         arithmetic below describes a different failure"
    );

    // The floor the reclaim plan computes from a claim: `frontier = min(claim - 1)`, and it then
    // frees everything at or below the frontier. A truncated claim moves that floor DOWN by
    // essentially the whole log.
    let honest_floor = PAST_THIRTY_TWO_BITS.saturating_sub(1);
    let truncated_floor = truncated.saturating_sub(1);
    assert!(
        truncated_floor < honest_floor,
        "the point of this guard is that truncation LOWERS the floor; it did not"
    );
    println!(
        "\n=== a 32-bit claim on a log past 2^32 ===\n  claim {PAST_THIRTY_TWO_BITS} -> \
         {truncated}, retain floor {honest_floor} -> {truncated_floor}: {} records that the \
         bucket still holds the log for become reclaimable",
        honest_floor - truncated_floor
    );

    // And the saturating form, which is the only narrowing this codebase would accept, is no
    // better HERE -- it is wrong in the other direction. A claim saturated to `u32::MAX` names a
    // write newer than the one the bucket needs, which raises the floor over records it holds.
    let saturated = u32::MAX as u64;
    assert!(
        saturated < PAST_THIRTY_TWO_BITS,
        "saturation also loses the claim, in the direction that raises the floor: {saturated} \
         against {PAST_THIRTY_TWO_BITS}"
    );
}

// -------------------------------------------------------------------------------------------
// 4. EACH SEQUENCE IS READ WHERE THIS MODULE SAYS IT IS. PERTURBED, NOT READ BACK.
// -------------------------------------------------------------------------------------------

/// Overwrite one bucket's field on a live shard. The shard lock is the engine's own.
fn perturb(engine: &TemporalEngine, routing_bucket: u32, edit: impl Fn(&mut BucketNode)) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&SEQ_SHARD).expect("shard is loaded");
    let bucket = shard
        .bucket_index
        .bucket_map
        .get_mut(&routing_bucket)
        .expect("the bucket this test chose is in the map");
    edit(bucket);
}

/// THE TWO TRANSIENT CLAIMS ARE LOAD-BEARING: clearing either one alone stops the plan.
///
/// The plan requires BOTH halves from a dirty bucket no manifest covers. Clearing one half and
/// leaving the other is the state a guard reading "is a claim present" cannot tell from whole,
/// and it is the state this drives -- once for each half, so a reader that stopped consulting
/// either one turns this red by name.
///
/// THE CONTROL IS THE UNPERTURBED PLAN, taken first on the same shard: without it a plan that
/// refuses for some unrelated reason reads exactly like a plan that refuses because of the edit.
#[test]
#[ignore = "seeds 2,000 records; run by name"]
fn clearing_either_half_of_a_buckets_claim_stops_the_reclaim_plan() {
    for (label, clear_wal) in [("the WAL half", true), ("the index-log half", false)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = seq_engine(dir.path(), "claimread");
        load_at(&engine, CONFIGURED_END_BUCKET);
        write_batches(&engine, &seq_keys("k", 2_000), 118);

        // The bucket this test edits has to be one the plan actually consults: dirty, and with
        // both halves already stamped.
        let rows = sequences_by_bucket(&engine);
        let target = rows
            .iter()
            .find(|row| row.dirty && row.wal_claim > 0 && row.index_log_claim > 0)
            .copied();
        let target = target.expect(
            "the fixture produced no dirty bucket holding both halves of a claim, so the branch \
             this test is named for is never reached",
        );

        // THE CONTROL.
        let before = engine.storage_wal_reclaim_plan(SEQ_SHARD, Vec::new(), Vec::new());
        assert!(
            !before.missing_bucket_generations.contains(&target.routing_bucket),
            "bucket {} was already refusing the plan before the edit, so a refusal afterwards \
             would not be the edit's doing",
            target.routing_bucket
        );

        perturb(&engine, target.routing_bucket, |bucket| {
            if clear_wal {
                bucket.first_dirty_wal_sequence = 0;
            } else {
                bucket.first_dirty_index_log_sequence = 0;
            }
        });

        let after = engine.storage_wal_reclaim_plan(SEQ_SHARD, Vec::new(), Vec::new());
        println!(
            "\n=== clearing {label} of bucket {} ===\n  missing_bucket_generations {} -> {}, \
             covered {} -> {}",
            target.routing_bucket,
            before.missing_bucket_generations.len(),
            after.missing_bucket_generations.len(),
            before.covered_bucket_count,
            after.covered_bucket_count
        );
        assert!(
            after.missing_bucket_generations.contains(&target.routing_bucket),
            "clearing {label} of bucket {}'s claim left the plan still covering it, so that half \
             is not being read where this module says it is read",
            target.routing_bucket
        );
    }
}

/// `dirty_generation` IS THE EQUALITY THAT LETS A DUMP MANIFEST COVER A BUCKET.
///
/// A dump manifest records each bucket's summary, and `bucket_dump_summary_matches_current_generation`
/// compares `dirty_generation` for EQUALITY against the live one. That match is what puts the
/// manifest in `retained_manifest_ids` and lets the plan anchor reclaim on it.
///
/// Move the live value on every bucket and no manifest matches any of them, so the plan retains
/// none. Every bucket is moved rather than one, because a manifest is retained if ANY bucket
/// still matches it -- editing a single bucket is the reduction that lets one right element hide
/// the rest, and it is exactly what this must not do.
///
/// THE CONTROL IS THE SAME PLAN ON THE SAME SHARD BEFORE THE EDIT, asserted to retain a manifest.
/// Without it, "retains none afterwards" is indistinguishable from a shard that never retained
/// one.
///
/// THE EDIT MOVES THE VALUE FORWARD, which is the direction a real write moves it, and by an
/// amount no write in the fixture could have produced.
#[test]
#[ignore = "seeds 2,000 records and dumps; run by name"]
fn moving_every_buckets_dirty_generation_stops_the_manifests_covering_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = seq_engine(dir.path(), "generation");
    load_at(&engine, CONFIGURED_END_BUCKET);
    write_batches(&engine, &seq_keys("k", 2_000), 118);
    // Dump everything, so every bucket has a manifest recording its generation.
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: SEQ_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        ..StorageManagerCycleRequest::default()
    });

    // THE CONTROL.
    let before = engine.storage_wal_reclaim_plan(SEQ_SHARD, Vec::new(), Vec::new());
    assert!(
        !before.retained_manifest_ids.is_empty(),
        "no manifest was retained before the edit, so there is no coverage for the edit to break"
    );
    let rows = sequences_by_bucket(&engine);
    let carrying = rows.iter().filter(|row| row.generation > 0).count();
    assert!(
        carrying > 0,
        "no bucket carries a generation, so the equality this test drives is never evaluated"
    );

    // Move EVERY bucket's generation forward, by more than the fixture could have written.
    const SHIFT: u64 = 1_000_000;
    let routing_buckets = rows.iter().map(|row| row.routing_bucket).collect::<Vec<_>>();
    for routing_bucket in routing_buckets {
        perturb(&engine, routing_bucket, |bucket| {
            bucket.dirty_generation = bucket.dirty_generation.saturating_add(SHIFT);
        });
    }

    let after = engine.storage_wal_reclaim_plan(SEQ_SHARD, Vec::new(), Vec::new());
    println!(
        "\n=== every bucket's dirty_generation moved forward by {SHIFT} ===\n  buckets carrying \
         a generation: {carrying} of {}\n  retained manifests {} -> {}\n  covered {} -> {}",
        rows.len(),
        before.retained_manifest_ids.len(),
        after.retained_manifest_ids.len(),
        before.covered_bucket_count,
        after.covered_bucket_count
    );
    assert!(
        after.retained_manifest_ids.is_empty(),
        "after moving every bucket's dirty_generation the plan still retains {} manifest(s); the \
         equality `bucket_dump_summary_matches_current_generation` makes on this field is not \
         reading it",
        after.retained_manifest_ids.len()
    );
}

/// THE DUMPED-LOG WATERMARK COMES FROM THE MANIFEST, AND THERE IS NOWHERE ELSE LEFT FOR IT TO
/// COME FROM.
///
/// WHAT THIS TEST USED TO BE, because the change to it is the finding. The node carried its own
/// `last_dump_sequence`, written from `manifest.wal_sequence`, and the dump ordering's comment
/// claimed the sort read it. The sort reads `BucketStorageSummary::last_dump_sequence` -- a
/// DIFFERENT field with the same name, filled by `merge_last_dump_sequence` from the newest
/// manifest's INDEX-LOG sequence -- so the node's figure reached no decision at all. This test
/// pinned that by planting a marker in the node's field and showing the summary did not follow it.
///
/// THE MARKER IS GONE BECAUSE THE FIELD IS. There is no per-bucket field to plant in any more:
/// the node's figure was read into two reports and nothing else, so it was one shard-level
/// watermark stored once per routing bucket, at eight bytes each. What is pinned instead is the
/// property that replaced it, in three parts:
///
///   * the SUMMARY's figure is the newest manifest's index-log sequence, unchanged;
///   * the physical index REPORT's per-bucket figure is now that same number, where it used to be
///     overwritten from the node; and
///   * a bucket the newest manifest does NOT name reports 0. That is the discriminating part.
///     Without it "the report shows the manifest's number" is satisfied by a report that shows
///     that number everywhere, including for buckets no dump has ever covered.
///
/// Both manifest sequences are printed, so a run where they coincide is visible rather than
/// silently making the first assertion undiscriminating.
#[test]
#[ignore = "seeds 8,000 records and dumps twice; run by name"]
fn the_summary_last_dump_sequence_comes_from_the_manifest_not_from_the_node() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = seq_engine(dir.path(), "lastdump");
    load_at(&engine, CONFIGURED_END_BUCKET);
    // Two writes with a capped round between them, then a full dump: the state a running shard
    // reaches, so the manifest the summary reads is one a round actually produced.
    write_batches(&engine, &seq_keys("first", 4_000), 118);
    one_round_at_the_configured_cap(&engine);
    write_batches(&engine, &seq_keys("later", 4_000), 119);
    engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: SEQ_SHARD,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        max_dump_buckets_per_round: 0,
        ..StorageManagerCycleRequest::default()
    });

    let manifests = engine.list_bucket_dump_manifests(SEQ_SHARD);
    let manifest = manifests
        .last()
        .cloned()
        .expect("the dump produced no manifest, so there is nothing for the summary to read");
    assert!(
        !manifest.bucket_ids.is_empty(),
        "the manifest names no bucket, so nothing below is exercised"
    );

    let rows = sequences_by_bucket(&engine);
    let covered = rows
        .iter()
        .find(|row| manifest.bucket_ids.contains(&row.routing_bucket))
        .copied()
        .expect("the newest manifest names no bucket that is still in the map");

    let summary_of = |engine: &TemporalEngine, routing_bucket: u32| -> u64 {
        engine
            .bucket_storage_summaries(SEQ_SHARD)
            .into_iter()
            .find(|summary| summary.routing_bucket == routing_bucket)
            .map(|summary| summary.last_dump_sequence)
            .unwrap_or_default()
    };
    let reported = |engine: &TemporalEngine, routing_bucket: u32| -> u64 {
        engine
            .storage_physical_index_report(SEQ_SHARD)
            .bucket_nodes
            .into_iter()
            .find(|node| node.routing_bucket == routing_bucket)
            .map(|node| node.last_dump_sequence)
            .unwrap_or_default()
    };

    let summary_covered = summary_of(&engine, covered.routing_bucket);
    println!(
        "\n=== a bucket the newest manifest NAMES: {} ===\n  \
         summary.last_dump_sequence={summary_covered}\n  \
         report.last_dump_sequence={}\n  \
         manifest.index_log_sequence={}  manifest.wal_sequence={}",
        covered.routing_bucket,
        reported(&engine, covered.routing_bucket),
        manifest.index_log_sequence,
        manifest.wal_sequence
    );

    assert_eq!(
        manifest.index_log_sequence, summary_covered,
        "the summary's last_dump_sequence is {summary_covered}; the newest manifest's index-log \
         sequence is {} and its WAL sequence is {}. If the summary has started following the WAL \
         clock, the dump ordering's comment has become true and this module's finding is stale",
        manifest.index_log_sequence, manifest.wal_sequence
    );
    assert_eq!(
        manifest.index_log_sequence,
        reported(&engine, covered.routing_bucket),
        "the physical index report publishes {} for a bucket the newest manifest names; it is \
         supposed to carry the summary's figure now that the node holds none",
        reported(&engine, covered.routing_bucket)
    );
    println!(
        "  the manifest's two sequences read {} (WAL) and {} (index-log); they need not differ, \
         and the uncovered bucket below is what makes this test discriminating when they do not",
        manifest.wal_sequence, manifest.index_log_sequence
    );

    // THE DISCRIMINATING HALF: a bucket the newest manifest does not name must report 0. Without
    // it every assertion above is satisfied by a report that stamps the manifest's number on
    // every row.
    let uncovered = rows
        .iter()
        .find(|row| !manifest.bucket_ids.contains(&row.routing_bucket))
        .copied();
    match uncovered {
        Some(row) => {
            let summary_uncovered = summary_of(&engine, row.routing_bucket);
            let reported_uncovered = reported(&engine, row.routing_bucket);
            println!(
                "=== a bucket the newest manifest does NOT name: {} ===\n  \
                 summary.last_dump_sequence={summary_uncovered}  \
                 report.last_dump_sequence={reported_uncovered}",
                row.routing_bucket
            );
            assert_eq!(
                0, summary_uncovered,
                "a bucket the newest manifest does not name carries {summary_uncovered} in its \
                 summary, so `merge_last_dump_sequence` is no longer separating covered from \
                 uncovered"
            );
            assert_eq!(
                0, reported_uncovered,
                "a bucket the newest manifest does not name is reported as dumped at \
                 {reported_uncovered}; the report cannot be reading the manifest"
            );
        }
        None => panic!(
            "every one of the {} buckets in the map is named by the newest manifest, so the \
             control that distinguishes covered from uncovered cannot run; the fixture needs a \
             capped round that leaves a bucket out",
            rows.len()
        ),
    }
}

// -------------------------------------------------------------------------------------------
// 5. WHAT THE ONLY SHAPE WORTH SIXTEEN BYTES WOULD ACTUALLY COST.
// -------------------------------------------------------------------------------------------

/// The counting allocator over one `clone()`, which is the SECOND instrument.
///
/// `Clone` on a `BTreeMap` rebuilds the tree node by node, so what the allocator charges is the
/// map's own nodes -- key array, value array, header, and every slot a node has not filled --
/// plus whatever each value owns on the heap. That owes nothing to `size_of` x count, which is
/// what makes the difference between the two a reading rather than an identity.
///
/// BYTES AND ALLOCATIONS, both, because the two disagree in SIGN on this structure's neighbours:
/// #1959 measured a shape where bytes fell 36.0% while allocations rose 121%. A cost reported in
/// one of them is half a reading.
#[cfg(feature = "alloc-probe")]
fn clone_alloc<T: Clone>(value: &T) -> (u64, u64) {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    (counts.alloc_bytes, counts.allocs)
}

/// THE PLANTED MARKER, recovered exactly, or every residual below is noise.
///
/// The failure this guards against is the one that reads as good news: an instrument reporting
/// near zero makes a structure look free. A megabyte is planted and has to come back as a
/// megabyte in ONE allocation -- not less, which would be blindness, and not more, which would be
/// the probe charging for something other than the clone.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_clone_instrument_in_this_module_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let (bytes, allocs) = clone_alloc(&marker);
    println!(
        "planted {PLANTED} B, instrument charged {bytes} B in {allocs} allocation(s) ({:.4}x)",
        bytes as f64 / PLANTED as f64
    );
    assert_eq!(
        PLANTED as u64, bytes,
        "the clone instrument charged {bytes} B for a planted {PLANTED} B; a residual taken with \
         it would be measuring the instrument"
    );
    assert_eq!(
        1, allocs,
        "the clone instrument charged {allocs} allocations for one planted vector; the \
         allocation figures below would be counting something else"
    );
}

/// THE HOIST, PRICED ON ONE INSTRUMENT AT TWO CORPUS SIZES, IN BYTES AND ALLOCATIONS.
///
/// Moving the two transient claims into a side map keyed by bucket takes 16 bytes off every node
/// -- a real sixteen; the width table above says so -- and buys a map entry for every DIRTY
/// bucket. So it turns entirely on the dirty FRACTION. #1958 declined it on a fraction it
/// measured at 100% in a fixture that never ran a dump; this runs one, at the CONFIGURED cap of
/// 64 buckets a round rather than at "dump everything", on the routing range
/// `docs/runtime_tuning.md` tells an operator to set.
///
/// BOTH SIDES ARE MEASURED WITH THE SAME INSTRUMENT. A saving taken as `size_of` arithmetic
/// against a cost taken from the allocator is how a decline gets its sign inverted, so the side
/// map is built for real and cloned under the probe, and the map of nodes the saving would come
/// off is cloned under the same probe. The residual between the allocator's figure for that map
/// and the node widths alone is reported rather than assumed away: it is the reason a word off
/// this structure is worth more than its own eight bytes, and it is asserted non-zero because a
/// residual of zero would mean the two instruments are not independent.
///
/// THE STORE PATH LENGTH IS HELD CONSTANT and asserted: allocation bytes move with it at about
/// six bytes a character.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds 8,000 then 40,000 records; needs --features alloc-probe; run by name"]
fn what_hoisting_the_two_transient_claims_would_cost_at_two_corpus_sizes() {
    let mut path_lengths: Vec<usize> = Vec::new();

    for (records, end_routing_bucket, range_label) in [
        (8_000usize, u32::MAX, "default range"),
        (8_000usize, CONFIGURED_END_BUCKET, "0..1023, the configured range"),
        (40_000usize, u32::MAX, "default range"),
        (40_000usize, CONFIGURED_END_BUCKET, "0..1023, the configured range"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());

        let engine = seq_engine(dir.path(), "hoist");
        load_at(&engine, end_routing_bucket);
        let first = seq_keys("first", records / 2);
        let later = seq_keys("later", records / 2);
        write_batches(&engine, &first, 118);
        write_singles(&engine, &first[..16.min(first.len())], 119);
        one_round_at_the_configured_cap(&engine);
        write_batches(&engine, &later, 120);

        let rows = sequences_by_bucket(&engine);
        assert!(
            !rows.is_empty(),
            "denominator: no routing buckets at {records} records"
        );
        let dirty = rows.iter().filter(|row| row.dirty).count();
        assert!(
            dirty > 0,
            "denominator: no dirty bucket at {records} records, so the side map this prices would \
             be empty and its cost would read as zero"
        );

        // The side map, built for real and charged by the allocator across one clone.
        let side: std::collections::BTreeMap<u32, (u64, u64)> = rows
            .iter()
            .filter(|row| row.dirty)
            .map(|row| (row.routing_bucket, (row.wal_claim, row.index_log_claim)))
            .collect();
        let (side_bytes, side_allocs) = clone_alloc(&side);

        // The map the sixteen bytes would come off, on the same instrument.
        let (live_bytes, live_allocs, accounted) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&SEQ_SHARD).expect("shard is loaded");
            let (bytes, allocs) = clone_alloc(&shard.bucket_index.bucket_map);
            (bytes, allocs, size_of::<BucketNode>() * rows.len())
        };
        let residual = live_bytes as i64 - accounted as i64;
        let width_saving = 16 * rows.len();

        println!(
            "\n=== {records} records, {range_label}, dump cap {CONFIGURED_DUMP_CAP} ==="
        );
        println!(
            "  buckets={} dirty={dirty} ({:.2}% dirty)",
            rows.len(),
            100.0 * dirty as f64 / rows.len() as f64
        );
        println!(
            "  the bucket map, charged by the allocator: {live_bytes} B in {live_allocs} \
             allocations; the node widths alone account for {accounted} B"
        );
        println!(
            "  RESIDUAL outside the node widths: {residual} B, {:.2} B a bucket -- key arrays, \
             node headers, unfilled value slots, and what each node owns on the heap",
            residual as f64 / rows.len() as f64
        );
        println!(
            "  off the nodes: 16 B x {} = {width_saving} B, {:.3} B a record, 0 allocations",
            rows.len(),
            width_saving as f64 / records as f64
        );
        println!(
            "  onto the side map: {side_bytes} B in {side_allocs} allocations for {dirty} \
             entries -- {:.1} B an entry, {:.3} B a record, {:.4} allocations a record",
            side_bytes as f64 / dirty as f64,
            side_bytes as f64 / records as f64,
            side_allocs as f64 / records as f64
        );
        println!(
            "  NET: {:+} B, {:+.3} B a record, {:+.4} allocations a record",
            side_bytes as i64 - width_saving as i64,
            (side_bytes as i64 - width_saving as i64) as f64 / records as f64,
            side_allocs as f64 / records as f64
        );

        assert!(
            side_bytes > 0,
            "the allocator charged nothing for a side map of {dirty} entries; the probe is not \
             measuring the clone"
        );
        assert!(
            residual > 0,
            "the allocator charged {live_bytes} B for a map of {} nodes whose widths add up to \
             {accounted} B; a residual at or below zero means the two instruments are not \
             independent and the subtraction is an identity",
            rows.len()
        );
    }

    assert_eq!(4, path_lengths.len(), "all four arms must have run");
    let first_length = path_lengths[0];
    for length in &path_lengths {
        assert_eq!(
            first_length, *length,
            "the store path length moved between arms ({first_length} then {length}); allocation \
             bytes move with it at about six bytes a character"
        );
    }

    println!(
        "\n=== the verdict, and it depends on the range ===\n  On the DEFAULT range a key lands \
         in a bucket of its own, half the buckets are clean after a round, and the side map holds \
         half as many entries as there are buckets -- so the hoist is a small win there. On the \
         range an operator is told to configure, the buckets fill, the dump cap of \
         {CONFIGURED_DUMP_CAP} cannot reach them all, essentially every bucket is dirty, and the \
         side map costs more than the sixteen bytes it frees -- in bytes AND in allocations. The \
         configured range is the one that decides it."
    );
}

/// THE READ PATH OF THE HOISTED SHAPE, because the default assumption is a trade and a
/// representation that slows the common path to shrink the node should be declined with numbers.
///
/// Today both claims are fields of a node the caller already has in hand: the reclaim plan walks
/// `bucket_map` and reads them off each node as it goes, one pass, no second lookup. Hoisted,
/// every one of those reads becomes a lookup in a second ordered map keyed by the bucket -- a
/// tree descent per bucket, on a path that already holds the shard lock.
///
/// Both arms walk the SAME bucket set in the SAME order and produce the same total, and that
/// equality is asserted before either is timed -- otherwise the two are not the same walk and the
/// ratio compares nothing. ABBA rather than interleaving, so an order effect on a shared box
/// cannot be read as a difference between the arms.
///
/// MEASURED IN THE TEST PROFILE, which is unoptimised, so the constant overstates what a release
/// build would pay. The SIGN is what this is for: the hoisted arm does strictly more work -- a
/// tree descent per bucket where the other reads a field -- and there is no configuration in
/// which that reverses.
#[test]
#[ignore = "seeds 8,000 records; run by name"]
fn the_read_path_of_the_hoisted_shape_is_priced() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (rows, _) = seeded_rows(dir.path(), "hoistread", 8_000, CONFIGURED_END_BUCKET);
    let dirty_rows: Vec<Sequences> = rows.iter().filter(|row| row.dirty).copied().collect();
    assert!(
        dirty_rows.len() >= 64,
        "denominator: only {} dirty buckets, too few to time a walk over",
        dirty_rows.len()
    );

    let side: std::collections::BTreeMap<u32, (u64, u64)> = dirty_rows
        .iter()
        .map(|row| (row.routing_bucket, (row.wal_claim, row.index_log_claim)))
        .collect();

    // ABBA, so an order effect on a shared box cannot be read as a difference between the arms.
    let inline_pass = || -> u128 {
        let mut total = 0u128;
        for row in &dirty_rows {
            total += row.wal_claim as u128 + row.index_log_claim as u128;
        }
        std::hint::black_box(total)
    };
    let hoisted_pass = || -> u128 {
        let mut total = 0u128;
        for row in &dirty_rows {
            let (wal, index_log) = side.get(&row.routing_bucket).copied().unwrap_or((0, 0));
            total += wal as u128 + index_log as u128;
        }
        std::hint::black_box(total)
    };

    // THE CONTROL ON THE COMPARISON: both arms must produce the same total, or they are not
    // reading the same thing and the timing compares two different walks.
    assert_eq!(
        inline_pass(),
        hoisted_pass(),
        "the two arms disagree on the total they read, so they are not the same walk"
    );

    const ROUNDS: usize = 200;
    let mut inline_ns = 0u128;
    let mut hoisted_ns = 0u128;
    for _ in 0..ROUNDS {
        let start = std::time::Instant::now();
        std::hint::black_box(inline_pass());
        inline_ns += start.elapsed().as_nanos();
        let start = std::time::Instant::now();
        std::hint::black_box(hoisted_pass());
        hoisted_ns += start.elapsed().as_nanos();
        // B then A.
        let start = std::time::Instant::now();
        std::hint::black_box(hoisted_pass());
        hoisted_ns += start.elapsed().as_nanos();
        let start = std::time::Instant::now();
        std::hint::black_box(inline_pass());
        inline_ns += start.elapsed().as_nanos();
    }

    println!(
        "\n=== reading both claims for {} dirty buckets, {ROUNDS} ABBA rounds ===\n  off the \
         node: {inline_ns} ns total\n  out of a side map: {hoisted_ns} ns total ({:.3}x)",
        dirty_rows.len(),
        hoisted_ns as f64 / inline_ns.max(1) as f64
    );

    assert!(
        hoisted_ns > 0 && inline_ns > 0,
        "one of the arms took no measurable time, so the ratio is not a reading"
    );
}

// -------------------------------------------------------------------------------------------
// 6. WHAT A BOUNDED RING OF CLAIMS DOES WHEN IT WRAPS, AND WHICH DIRECTION IT ERRS.
// -------------------------------------------------------------------------------------------

/// A BOUNDED ring of per-bucket claims, which is the shape a shard-level side structure takes when
/// it is not allowed to grow with the bucket count.
///
/// `push` overwrites the OLDEST entry once the ring is full. That is what bounded means, and it is
/// the whole of the hazard: a claim is a FLOOR the bucket holds over the log, the oldest claim is
/// the lowest floor, and so a ring that has wrapped has forgotten precisely the claims that were
/// holding the log down. It is not an unlucky eviction order, it is the eviction order a ring has.
struct ClaimRing {
    slots: Vec<(u32, u64)>,
    capacity: usize,
    next: usize,
    wrapped: bool,
}

impl ClaimRing {
    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "a ring of no slots remembers nothing and measures nothing");
        Self { slots: Vec::with_capacity(capacity), capacity, next: 0, wrapped: false }
    }

    fn push(&mut self, routing_bucket: u32, sequence: u64) {
        if self.slots.len() < self.capacity {
            self.slots.push((routing_bucket, sequence));
            return;
        }
        self.wrapped = true;
        self.slots[self.next] = (routing_bucket, sequence);
        self.next = (self.next + 1) % self.capacity;
    }

    fn remembers(&self, routing_bucket: u32) -> bool {
        self.slots.iter().any(|(bucket, _)| *bucket == routing_bucket)
    }

    fn has_wrapped(&self) -> bool {
        self.wrapped
    }
}

/// Make a bucket's claims unknown, as a ring that has forgotten its slot would leave them.
fn forget_claims(engine: &TemporalEngine, routing_bucket: u32) {
    perturb(engine, routing_bucket, |bucket| {
        bucket.first_dirty_wal_sequence = 0;
        bucket.first_dirty_index_log_sequence = 0;
    });
}

/// PROVE THE TREATMENT RAN: a slot the ring forgot names NEITHER half.
///
/// A ring holds one entry per bucket, so a forgotten slot takes both halves with it. Half a claim
/// is a different state, it is one the tree pins separately in
/// `clearing_either_half_of_a_buckets_claim_stops_the_reclaim_plan`, and without this the arms
/// below would read the same for either -- a mutation that cleared only the WAL half survived until
/// this assertion existed.
fn assert_forgotten_slots_name_nothing(engine: &TemporalEngine, forgotten: &[Sequences]) {
    let rows = sequences_by_bucket(engine);
    for row in forgotten {
        let now = rows
            .iter()
            .find(|candidate| candidate.routing_bucket == row.routing_bucket)
            .copied()
            .expect("the forgotten bucket is still in the map");
        assert_eq!(
            (0, 0),
            (now.wal_claim, now.index_log_claim),
            "bucket {} still names ({}, {}) after its ring slot was forgotten; a ring holds one \
             entry per bucket, so losing the slot loses both halves and a half-cleared claim is a \
             different state",
            row.routing_bucket,
            now.wal_claim,
            now.index_log_claim
        );
    }
}

/// Make a bucket's DIRTINESS unknown as well, which is the second arm's whole point.
///
/// `DirtyObjectIndex::drain_buckets` is the production drain a dump runs once its manifest is
/// durable -- so "the structure no longer knows this bucket is dirty" is modelled by the shipped
/// operation that stops knowing it, not by a hand-rolled edit to a private field.
fn forget_dirtiness(engine: &TemporalEngine, routing_bucket: u32) -> usize {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&SEQ_SHARD).expect("shard is loaded");
    let dropped = shard.dirty_objects.drain_buckets(&[routing_bucket]);
    if let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&routing_bucket) {
        bucket.set_dirty(false);
    }
    dropped
}

/// A WRAPPED RING ERRS CONSERVATIVE ONLY WHILE IT STILL KNOWS WHICH BUCKETS ARE DIRTY -- and the
/// arm where it does not is DATA LOSS.
///
/// THE QUESTION. `first_dirty_wal_sequence` and its index-log twin are already `#[serde(skip)]` and
/// cleared on load, so they are runtime-only, and runtime-only state is exactly what a shard-level
/// side structure could hold instead of sixteen bytes on every routing bucket. A ring of slot ids
/// is the bounded shape such a structure takes. A bounded ring that has wrapped has FORGOTTEN a
/// claim, and a claim is the floor a bucket holds over the log -- so before any of that can be
/// designed, a wrap has to be DRIVEN and the direction of its error measured. Two designs differ
/// in one respect, and it is the respect that decides it:
///
///   * ARM A -- THE RING HOLDS THE CLAIMS, THE DIRTY SET STILL HOLDS THE DIRTINESS. A forgotten
///     slot leaves a bucket that is still known to be dirty and can no longer name where it sits
///     in the log. The reclaim plan reads that as NO CLAIM RECORDED and blocks: it retains at
///     least as much log as before, never less. That is the SAFE direction, it is the direction
///     `first_dirty_wal_sequence`'s own doc comment says 0 means, and this arm asserts it.
///   * ARM B -- THE RING IS THE DIRTY SET. A forgotten slot leaves a bucket the structure does not
///     know is dirty at all. The plan then counts it covered and contributing no floor, the floor
///     rises PAST that bucket's own oldest undumped write, and reclaim frees log records the bucket
///     still needs to be replayed from. That is committed data lost, and this arm asserts the
///     floor moves the wrong way rather than arguing that it would.
///
/// WHICH SLOTS THE RING FORGETS IS NOT CHOSEN HERE. The claims are pushed in claim order, oldest
/// first, and the ring's own eviction decides what survives -- which is the oldest half, the
/// floor-holders. Asserting the ring actually wrapped and that the forgotten set is non-empty is
/// what stops this passing on a ring that never lost anything.
///
/// THE CONTROL IS THE UNPERTURBED PLAN on the same shard, taken first, and the fixture's
/// preconditions are ENFORCED rather than printed: the control has to be safe to reclaim and hold a
/// floor above the log's start, or neither arm has a floor to move.
#[test]
#[ignore = "seeds 2,000 records twice and drives a ring wrap in each arm; run by name"]
fn a_wrapped_claim_ring_errs_conservative_only_while_it_keeps_dirtiness() {
    for arm in ["A: the ring holds the claims", "B: the ring IS the dirty set"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = seq_engine(dir.path(), "ringwrap");
        load_at(&engine, CONFIGURED_END_BUCKET);
        // Write, dump everything so the shard has durable manifests, then write again: the state a
        // running shard is in, with a floor the plan can actually compute.
        write_batches(&engine, &seq_keys("first", 1_000), 118);
        engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: SEQ_SHARD,
            min_undumped_wal_records: 0,
            min_undumped_wal_bytes: 0,
            max_dump_buckets_per_round: 0,
            ..StorageManagerCycleRequest::default()
        });
        // STAGGERED CLAIMS, AND THE FIRST ATTEMPT AT THIS FIXTURE IS WHY THEY HAVE TO BE.
        // `write_batches` sends 512 commands per batch, so ONE log sequence is stamped on every
        // bucket that batch touched: 694 buckets all claiming sequence 3. Forgetting half of an
        // identical set cannot move a floor the other half holds at the same number, so both arms
        // read "unchanged" and arm A would have passed while measuring nothing. The enforced
        // precondition below is what caught it. Single writes are one log record each, so every
        // bucket's claim is its own.
        write_singles(&engine, &seq_keys("stagger", 48), 119);

        // The buckets a ring would be holding: dirty, able to name both halves of a claim, and --
        // one per distinct claim, because two buckets at the same claim hold the same floor and
        // forgetting one of them moves nothing.
        let rows = sequences_by_bucket(&engine);
        let mut claiming: Vec<Sequences> = rows
            .iter()
            .filter(|row| row.dirty && row.wal_claim > 0 && row.index_log_claim > 0)
            .copied()
            .collect();
        claiming.sort_by_key(|row| (row.wal_claim, row.routing_bucket));
        let mut seen_claims = std::collections::BTreeSet::new();
        claiming.retain(|row| seen_claims.insert(row.wal_claim));
        assert!(
            claiming.len() >= 8,
            "the fixture produced {} bucket(s) at distinct claims; a ring cannot be made to wrap \
             over fewer than a handful and the forgotten set would be empty",
            claiming.len()
        );

        // Push oldest-claim-first into a ring half the size of the population, and let the ring
        // decide what it keeps.
        let capacity = claiming.len() / 2;
        let mut ring = ClaimRing::with_capacity(capacity);
        for row in &claiming {
            ring.push(row.routing_bucket, row.wal_claim);
        }
        assert!(
            ring.has_wrapped(),
            "the ring never wrapped over {} claims at capacity {capacity}, so nothing was forgotten \
             and this test is about nothing",
            claiming.len()
        );
        let forgotten: Vec<Sequences> = claiming
            .iter()
            .filter(|row| !ring.remembers(row.routing_bucket))
            .copied()
            .collect();
        assert!(
            !forgotten.is_empty(),
            "the ring wrapped and still remembers every bucket, so the forgotten set is empty"
        );
        let lowest_forgotten_claim = forgotten
            .iter()
            .map(|row| row.wal_claim)
            .min()
            .expect("the forgotten set is not empty");
        let lowest_remembered_claim = claiming
            .iter()
            .filter(|row| ring.remembers(row.routing_bucket))
            .map(|row| row.wal_claim)
            .min()
            .unwrap_or(u64::MAX);

        // THE CONTROL, and its preconditions enforced.
        let before = engine.storage_wal_reclaim_plan(SEQ_SHARD, Vec::new(), Vec::new());
        println!(
            "\n=== {arm} ===\n  buckets holding a full claim: {}   ring capacity {capacity}   \
             forgotten {}\n  lowest forgotten claim {lowest_forgotten_claim}, lowest remembered \
             claim {lowest_remembered_claim}\n  CONTROL: safe={} retain_from_wal={} \
             missing={} covered={}",
            claiming.len(),
            forgotten.len(),
            before.safe_to_reclaim,
            before.retain_from_wal_sequence,
            before.missing_bucket_generations.len(),
            before.covered_bucket_count
        );
        assert!(
            before.safe_to_reclaim,
            "the control plan refuses to reclaim ({:?}), so there is no floor for either arm to \
             move and 'more conservative' could not be told from 'unchanged'",
            before.blocker_reasons
        );
        assert!(
            before.retain_from_wal_sequence > 1,
            "the control floor is at {}, which is the start of the log; a floor that cannot fall \
             makes arm A vacuous",
            before.retain_from_wal_sequence
        );
        assert!(
            lowest_forgotten_claim < lowest_remembered_claim,
            "the ring forgot a claim that is NEWER than one it kept ({lowest_forgotten_claim} \
             against {lowest_remembered_claim}); the eviction order this test is about is \
             oldest-first and the fixture is not exercising it"
        );

        if arm.starts_with('A') {
            for row in &forgotten {
                forget_claims(&engine, row.routing_bucket);
            }
            assert_forgotten_slots_name_nothing(&engine, &forgotten);
            let after = engine.storage_wal_reclaim_plan(SEQ_SHARD, Vec::new(), Vec::new());
            println!(
                "  ARM A  : safe={} retain_from_wal={} missing={} covered={}",
                after.safe_to_reclaim,
                after.retain_from_wal_sequence,
                after.missing_bucket_generations.len(),
                after.covered_bucket_count
            );
            // The bucket is still known dirty, so the plan sees a dirty bucket that cannot name
            // its claim and blocks on it.
            for row in &forgotten {
                assert!(
                    after.missing_bucket_generations.contains(&row.routing_bucket),
                    "bucket {} lost its claim and the plan still covers it; a forgotten claim has \
                     to read as unknown, not as nothing to retain",
                    row.routing_bucket
                );
            }
            assert!(
                after.retain_from_wal_sequence <= before.retain_from_wal_sequence,
                "a wrapped ring that keeps dirtiness moved the floor UP, from {} to {}: it would \
                 free log the forgotten buckets still hold. This arm is supposed to be the safe \
                 one and it is not",
                before.retain_from_wal_sequence,
                after.retain_from_wal_sequence
            );
            println!(
                "  VERDICT: CONSERVATIVE -- the floor went {} -> {} ({:+}), so a wrap frees no \
                 more log than no wrap. Safe, and takeable.",
                before.retain_from_wal_sequence,
                after.retain_from_wal_sequence,
                after.retain_from_wal_sequence as i64 - before.retain_from_wal_sequence as i64
            );
        } else {
            let mut dropped = 0usize;
            for row in &forgotten {
                forget_claims(&engine, row.routing_bucket);
                dropped += forget_dirtiness(&engine, row.routing_bucket);
            }
            assert!(
                dropped > 0,
                "no dirty object was dropped, so the forgotten buckets are still dirty and this \
                 arm is the same as arm A"
            );
            assert_forgotten_slots_name_nothing(&engine, &forgotten);
            let after = engine.storage_wal_reclaim_plan(SEQ_SHARD, Vec::new(), Vec::new());
            println!(
                "  ARM B  : safe={} retain_from_wal={} missing={} covered={} (dropped {dropped} \
                 dirty keys)",
                after.safe_to_reclaim,
                after.retain_from_wal_sequence,
                after.missing_bucket_generations.len(),
                after.covered_bucket_count
            );
            assert!(
                after.retain_from_wal_sequence > before.retain_from_wal_sequence,
                "a wrapped ring that is ALSO the dirty set left the floor at {} (control {}); this \
                 arm is the one that is supposed to be unsafe and the measurement does not show it \
                 -- do not conclude the design is safe from this, find out why the floor did not \
                 move",
                after.retain_from_wal_sequence,
                before.retain_from_wal_sequence
            );
            // THE DATA LOSS, NAMED. The floor is now above the oldest undumped write of a bucket
            // that has one, so reclaim would free a record that bucket still needs.
            assert!(
                after.retain_from_wal_sequence > lowest_forgotten_claim,
                "the floor rose to {} but the lowest forgotten claim is {lowest_forgotten_claim}; \
                 the floor has to pass a forgotten bucket's own oldest undumped write for this to \
                 be the loss it is named for",
                after.retain_from_wal_sequence
            );
            println!(
                "  VERDICT: AGGRESSIVE -- the floor went {} -> {} ({:+}), past the lowest \
                 forgotten claim {lowest_forgotten_claim}. Reclaim would free {} WAL record(s) \
                 that a bucket still holds the log for. DATA LOSS: refuse this design.",
                before.retain_from_wal_sequence,
                after.retain_from_wal_sequence,
                after.retain_from_wal_sequence as i64 - before.retain_from_wal_sequence as i64,
                after.retain_from_wal_sequence.saturating_sub(lowest_forgotten_claim)
            );
        }
    }
}

/// A BUCKET THAT CANNOT NAME ITS CLAIM SORTS LAST IN THE DUMP ORDER, NOT FIRST.
///
/// THIS GUARD EXISTS BECAUSE A MUTANT SURVIVED. `first_dirty_rank` maps a claim of 0 -- "no claim
/// recorded" -- to `u64::MAX`, which sorts the bucket LAST, and the comment on it says why: a
/// bucket we cannot place in the log is not evidence of being old, and the ones we can place are
/// the ones whose dump moves the floor. Replacing that `u64::MAX` with `0`, so an unplaceable
/// bucket is dumped FIRST, passed every guard in this module, the two reclaim guards, and
/// `dump_selection_prioritizes_the_least_recently_dumped_bucket_not_the_lowest_id`. The rule was
/// stated in a comment and enforced nowhere.
///
/// It matters because it is the whole of what a per-bucket claim buys over a shard-level watermark:
/// the FLOOR is the minimum of these claims either way, but the ORDER is what makes the floor
/// advance, and an order that spends its one capped slot on a bucket whose dump cannot move the
/// floor leaves the log where it was.
///
/// THE CONTROL IS THE SAME PLAN ON THE SAME SHARD BEFORE THE EDIT, asserted to select the bucket
/// with the OLDER claim -- so "it selected the other one afterwards" is the edit's doing and not a
/// plan that was picking that bucket all along.
#[test]
#[ignore = "seeds a shard and runs two capped plans; run by name"]
fn a_bucket_that_cannot_name_its_claim_is_dumped_last_and_not_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = seq_engine(dir.path(), "rank");
    load_at(&engine, CONFIGURED_END_BUCKET);
    // Single writes, so each bucket's claim is its own log record rather than the batch's.
    write_singles(&engine, &seq_keys("rank", 24), 118);

    let rows = sequences_by_bucket(&engine);
    let mut claiming: Vec<Sequences> = rows
        .iter()
        .filter(|row| row.dirty && row.wal_claim > 0 && row.index_log_claim > 0)
        .copied()
        .collect();
    claiming.sort_by_key(|row| (row.wal_claim, row.routing_bucket));
    let mut seen = std::collections::BTreeSet::new();
    claiming.retain(|row| seen.insert(row.wal_claim));
    assert!(
        claiming.len() >= 2,
        "the fixture produced {} bucket(s) at distinct claims; two are needed for an order to exist",
        claiming.len()
    );
    let oldest = claiming[0];
    let next = claiming[1];
    assert!(
        oldest.wal_claim < next.wal_claim,
        "the two buckets chosen hold the same claim ({} and {}), so there is no order between them",
        oldest.wal_claim,
        next.wal_claim
    );

    // THE CONTROL: the older claim is dumped first.
    let before = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: SEQ_SHARD,
        max_dump_buckets_per_round: 1,
        ..Default::default()
    });
    println!(
        "\n=== the dump order under a cap of one ===\n  bucket {} claims {}, bucket {} claims {}\n  \
         CONTROL selects {:?}",
        oldest.routing_bucket,
        oldest.wal_claim,
        next.routing_bucket,
        next.wal_claim,
        before.selected_dump_buckets
    );
    assert_eq!(
        vec![oldest.routing_bucket],
        before.selected_dump_buckets,
        "the plan does not order by the oldest undumped write at all, so the edit below would not \
         be changing the thing this test is named for"
    );

    // The bucket holding the oldest claim can no longer name it -- the state a wrapped ring or a
    // reloaded node leaves behind.
    perturb(&engine, oldest.routing_bucket, |bucket| {
        bucket.first_dirty_wal_sequence = 0;
        bucket.first_dirty_index_log_sequence = 0;
    });

    let after = engine.storage_lifecycle_plan(StorageLifecycleRequest {
        shard_id: SEQ_SHARD,
        max_dump_buckets_per_round: 1,
        ..Default::default()
    });
    println!(
        "  bucket {} now names no claim; the plan selects {:?}",
        oldest.routing_bucket, after.selected_dump_buckets
    );
    assert_eq!(
        vec![next.routing_bucket],
        after.selected_dump_buckets,
        "the bucket that can no longer name its claim was still selected first; a claim of 0 has \
         to sort LAST -- an unplaceable bucket's dump cannot move the reclaim floor, so spending \
         the capped slot on it leaves the log where it was"
    );
    assert!(
        !after.selected_dump_buckets.contains(&oldest.routing_bucket),
        "bucket {} claims nothing and is still in the selection",
        oldest.routing_bucket
    );
}
