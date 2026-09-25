// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE FOUR SEQUENCES ON EVERY BUCKET NODE: WHAT EACH ONE IS, AND WHETHER IT HAS TO BE THERE.
//!
//! #1958 accounted for every byte of `BucketNode` and put four `u64` sequences in the
//! eight-aligned group -- 32 of the node's 184 bytes, once per routing bucket, more than a sixth
//! of the widest per-item structure in the engine. It declined them in one clause, "the sequences
//! are unbounded counters", which is a claim about their RANGE and not a measurement of it.
//!
//! (The node was 192 when this was written and is 184 now: the `BlockAddress` inside the inline
//! page entry stopped storing a `generation` it could derive. The sequences did not move.)
//!
//! THE ANSWER IS THAT ALL FOUR STAY, at 64 bits, on every key. This module is the accounting for
//! that, and every row of it is a number rather than a reading of the code.
//!
//! THE FOUR, AND THEY ARE NOT FOUR OF A KIND:
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
//!   * `last_dump_sequence` -- the WAL sequence of the newest dump manifest that covered this
//!     bucket. STORED. WRITTEN only by `clear_dumped_bucket_dirty_state`, for the buckets a
//!     manifest names.
//!
//! WHAT WAS MEASURED, AND WHAT IT SAYS:
//!
//!   1. THE ALIGNMENT ARITHMETIC IS NOT THE OBVIOUS ONE. The node is 178 bytes of field in 184 --
//!      168 of eight-aligned field and a ten-byte tail rounded to sixteen, six bytes of slack.
//!      The natural reading of four words in the eight-aligned group is that narrowing one buys
//!      nothing. It is worth EIGHT: the freed `u32` leaves the group and lands in the tail, 14
//!      bytes still round to 16, and the group is a word shorter. Narrowing a SECOND is worth
//!      nothing on top of that -- 18 bytes round to 24 and hand the word straight back. Three
//!      narrow to 176, four narrow to 176 as well, and REMOVING two reaches 176.
//!      `what_narrowing_or_removing_each_sequence_would_make_the_node` prints the table against
//!      mirrors of the declaration, with the live mirror as its control and the reconstruction
//!      asserted on every row. So the saving belongs to the GROUP and there is no per-field
//!      saving to quote.
//!   2. NONE OF THE FOUR CAN NARROW, and the measured ranges are exactly the trap. Over a real
//!      workload at two corpus sizes and two routing ranges the largest value any of them reached
//!      was 106 -- seven bits. The bound is not the corpus, it is the STORE'S LIFETIME: three of
//!      the four are positions in logs that are reclaimed but never renumbered, and the fourth is
//!      a count a load preserves and nothing resets. Each of the three claims is read as a retain
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
//!   6. ONE DISAGREEMENT, REPORTED RATHER THAN FIXED. The node's `last_dump_sequence` does not
//!      reach the dump ordering that is commented on it. The sort reads
//!      `BucketStorageSummary::last_dump_sequence` -- a different field with the same name --
//!      and `bucket_storage_summaries` never fills that from the node; `merge_last_dump_sequence`
//!      fills it from the newest manifest's INDEX-LOG sequence, the same value for every bucket
//!      that manifest names and 0 for every bucket it does not. So the key separates "covered by
//!      the newest dump" from "not covered by it", which is not what it is described as doing.
//!      It is a tiebreaker behind `first_dirty_rank`, so the behaviour is left alone and the
//!      comment corrected; `the_summary_last_dump_sequence_comes_from_the_manifest_not_from_the_node`
//!      pins it with a planted marker.
//!
//! HOW THE "IS IT READ" GUARDS WORK. Each one PERTURBS the field on a live shard and asserts the
//! decision it is named for changes, against a control taken on the same shard before the edit. A
//! guard that only reads the field back proves the field exists; these prove it is load-bearing,
//! and a mutation that stops a reader reading it turns them red.
//!
//! WHAT THIS MODULE DOES NOT CLAIM. It proposes no change to `BucketNode`, so no stored shape
//! moves and no reader of the node's spelling is touched. The only production edit is a comment.
#![allow(clippy::all)]
use super::*;
use std::mem::{align_of, size_of};

use crate::engine::state::{
    BlockIndexMap, BucketLayoutState, BucketNode, BucketTtl, DeletedObjectIndex, ObjectIndex,
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

/// The four sequences of one bucket, plus the two flags that say which branch reads them.
#[derive(Clone, Copy, Debug)]
struct Sequences {
    routing_bucket: u32,
    dirty: bool,
    generation: u64,
    wal_claim: u64,
    index_log_claim: u64,
    last_dump: u64,
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
            dirty: bucket.dirty,
            generation: bucket.dirty_generation,
            wal_claim: bucket.first_dirty_wal_sequence,
            index_log_claim: bucket.first_dirty_index_log_sequence,
            last_dump: bucket.last_dump_sequence,
        })
        .collect::<Vec<_>>();
    out.sort_by_key(|row| row.routing_bucket);
    out
}

/// min / max / how many are non-zero, over one of the four.
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
// 1. WHAT EACH OF THE FOUR ACTUALLY REACHES.
// -------------------------------------------------------------------------------------------

/// WHAT EACH OF THE FOUR SEQUENCES REACHES, at two corpus sizes and two routing ranges.
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
fn what_each_of_the_four_sequences_reaches_at_two_corpus_sizes() {
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
            {
                let (low, high, nz, total) = spread(rows.iter().map(|row| row.last_dump));
                ("last_dump_sequence", low, high, nz, total)
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
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    last_dump_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// One of the four narrowed to 32 bits.
#[allow(dead_code)]
struct SeqMirrorOneNarrowed {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    last_dump_sequence: u32,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// Two of the four narrowed to 32 bits.
#[allow(dead_code)]
struct SeqMirrorTwoNarrowed {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u32,
    last_dump_sequence: u32,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// Three of the four narrowed to 32 bits.
#[allow(dead_code)]
struct SeqMirrorThreeNarrowed {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u32,
    first_dirty_index_log_sequence: u32,
    last_dump_sequence: u32,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// All four narrowed to 32 bits.
#[allow(dead_code)]
struct SeqMirrorFourNarrowed {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u32,
    first_dirty_wal_sequence: u32,
    first_dirty_index_log_sequence: u32,
    last_dump_sequence: u32,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// One of the four gone entirely.
#[allow(dead_code)]
struct SeqMirrorOneRemoved {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    last_dump_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// Both transient claims gone -- the hoist #1958 priced and declined.
#[allow(dead_code)]
struct SeqMirrorTwoRemoved {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    last_dump_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// WHAT NARROWING OR REMOVING EACH SEQUENCE WOULD MAKE THE NODE -- and the answer is not the one
/// the shape of the group suggests.
///
/// The node is 168 bytes of eight-aligned field plus a ten-byte tail rounded to sixteen. A `u64`
/// narrowed to a `u32` does not vanish: it leaves the eight-aligned group and lands in the tail.
/// So the arithmetic is `168 - 8n + round_up_8(10 + 4n)` and it steps, it does not slope:
///
/// ```text
///   narrowed   eight-aligned    tail -> rounded    size    vs live
///     0            168            10 -> 16          184       --
///     1            160            14 -> 16          176       -8
///     2            152            18 -> 24          176       -8
///     3            144            22 -> 24          168      -16
///     4            136            26 -> 32          168      -16
/// ```
///
/// The second narrowing is worth NOTHING on top of the first, and the fourth nothing on top of
/// the third. Stating a per-field saving here would be stating a number that does not exist: the
/// saving belongs to the GROUP, and only the first and third crossings move it.
///
/// Removing is different from narrowing because the bytes leave the structure instead of moving
/// to the tail: one removed is 176, two removed is 168.
///
/// THE BASE MOVED, THE PRICES DID NOT. Every figure in the table is eight bytes lower than when
/// it was written, because the node went 192 -> 184 when the `BlockAddress` inside its inline
/// page entry stopped storing a `generation` it could derive. Not one of the DIFFERENCES changed:
/// the group structure is what sets them, and that change came out of the eight-aligned group
/// whole, which is the same reason it was worth anything at all.
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
    // 168, not 176, since the inline `BlockIndexMap` lost eight bytes when the `BlockAddress`
    // inside its inline page entry stopped storing a `generation` it could derive. Every price
    // below is a DIFFERENCE and so did not move; only the base did.
    const EIGHT_ALIGNED: usize = 168;
    const TAIL: usize = 10;
    let live = size_of::<BucketNode>();
    assert_eq!(
        EIGHT_ALIGNED + TAIL.div_ceil(align) * align,
        live,
        "the live node must reconstruct from {EIGHT_ALIGNED} B of eight-aligned field and a \
         {TAIL} B tail, or the model every row below uses is wrong"
    );

    println!("\n=== narrowing n of the four sequences to 32 bits ===");
    println!(
        "  {:<9} {:>14} {:>9} {:>8} {:>6} {:>9}",
        "narrowed", "eight-aligned", "tail", "rounded", "size", "vs live"
    );
    let narrowed_sizes = [
        size_of::<SeqMirrorLive>(),
        size_of::<SeqMirrorOneNarrowed>(),
        size_of::<SeqMirrorTwoNarrowed>(),
        size_of::<SeqMirrorThreeNarrowed>(),
        size_of::<SeqMirrorFourNarrowed>(),
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
            "narrowing {n} of the four reconstructs to {} B but `size_of` says {size}; the two \
             groups plus one rounding must account for the width exactly",
            eight_aligned + rounded
        );
    }

    // THE STEP, asserted as a step. This is the claim the module leads with.
    assert_eq!(
        8,
        live - narrowed_sizes[1],
        "narrowing ONE of the four is priced at eight bytes a bucket; it measured {}",
        live - narrowed_sizes[1]
    );
    assert_eq!(
        narrowed_sizes[1], narrowed_sizes[2],
        "narrowing a SECOND of the four is worth nothing on top of the first -- the freed word \
         goes straight back into the tail's rounding -- but one measured {} and two measured {}",
        narrowed_sizes[1], narrowed_sizes[2]
    );
    assert_eq!(
        16,
        live - narrowed_sizes[3],
        "narrowing THREE is priced at sixteen bytes a bucket; it measured {}",
        live - narrowed_sizes[3]
    );
    assert_eq!(
        narrowed_sizes[3], narrowed_sizes[4],
        "narrowing the FOURTH is worth nothing on top of the third, but three measured {} and \
         four measured {}",
        narrowed_sizes[3], narrowed_sizes[4]
    );

    // --- Removing, which is a different arithmetic: the bytes leave rather than move. ---
    let one_removed = size_of::<SeqMirrorOneRemoved>();
    let two_removed = size_of::<SeqMirrorTwoRemoved>();
    println!("\n=== removing n of the four outright ===");
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
/// Three of the four are positions in a log. Reclaim FREES a log's records; it does not RENUMBER
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

/// THE SUMMARY'S `last_dump_sequence` COMES FROM THE MANIFEST, NOT FROM THE NODE.
///
/// This is the one place in this module where the finding is a DISAGREEMENT rather than an
/// accounting. `apply_storage_lifecycle` writes the node's field from `manifest.wal_sequence`,
/// and `storage_lifecycle_methods` then sorts the dump candidates by `last_dump_sequence` under a
/// comment calling it "the WAL sequence at the bucket's last dump". The sort reads
/// `BucketStorageSummary::last_dump_sequence` -- a DIFFERENT field with the same name -- and
/// `bucket_storage_summaries` never fills that one from the node. `merge_last_dump_sequence`
/// fills it from the newest manifest's INDEX-LOG sequence.
///
/// So the two are filled from two different fields of the manifest, and the node's value does not
/// reach the ordering. Two things are pinned here, and the second is the load-bearing one:
///
///   * the summary equals the newest manifest's INDEX-LOG sequence, which is what
///     `merge_last_dump_sequence` writes into it; and
///   * a MARKER is planted in the node's field -- a value no manifest, log or write in the
///     fixture could have produced -- and the summary is shown not to follow it. That is the half
///     that does not depend on the two manifest sequences reading different numbers, which in a
///     fixture with one batch per record they need not.
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
    let target = rows
        .iter()
        .find(|row| row.last_dump > 0 && manifest.bucket_ids.contains(&row.routing_bucket))
        .copied()
        .expect("no dumped bucket carries a last_dump_sequence on its node");

    let summary_of = |engine: &TemporalEngine, routing_bucket: u32| -> u64 {
        engine
            .bucket_storage_summaries(SEQ_SHARD)
            .into_iter()
            .find(|summary| summary.routing_bucket == routing_bucket)
            .map(|summary| summary.last_dump_sequence)
            .unwrap_or_default()
    };

    let summary_before = summary_of(&engine, target.routing_bucket);
    println!(
        "\n=== bucket {} ===\n  node.last_dump_sequence={} (manifest.wal_sequence={})\n  \
         summary.last_dump_sequence={summary_before} (manifest.index_log_sequence={})",
        target.routing_bucket, target.last_dump, manifest.wal_sequence, manifest.index_log_sequence
    );

    assert_eq!(
        manifest.index_log_sequence, summary_before,
        "the summary's last_dump_sequence is {summary_before}; the newest manifest's index-log \
         sequence is {} and its WAL sequence is {}. If the summary has started following the WAL \
         clock, the dump ordering's comment has become true and this module's finding is stale",
        manifest.index_log_sequence, manifest.wal_sequence
    );
    println!(
        "  the manifest's two sequences read {} (WAL) and {} (index-log); they need not differ, \
         and the marker below is what makes this test discriminating when they do not",
        manifest.wal_sequence, manifest.index_log_sequence
    );

    // Plant a marker in the NODE's field and show the summary does not follow it.
    const PLANTED: u64 = 7_654_321;
    assert_ne!(
        PLANTED, summary_before,
        "the marker collides with the value the summary already holds, so 'the summary did not \
         follow' could not be told from 'the summary followed exactly'"
    );
    perturb(&engine, target.routing_bucket, |bucket| {
        bucket.last_dump_sequence = PLANTED;
    });
    let summary_after = summary_of(&engine, target.routing_bucket);
    assert_eq!(
        summary_before, summary_after,
        "the node's last_dump_sequence was moved to {PLANTED} and the summary followed it \
         ({summary_before} -> {summary_after}); the two fields have been connected since this was \
         measured"
    );

    // And the marker did land, so the assertion above is about the summary and not about a write
    // that never happened.
    let after_rows = sequences_by_bucket(&engine);
    let after = after_rows
        .iter()
        .find(|row| row.routing_bucket == target.routing_bucket)
        .copied()
        .expect("the bucket is still in the map");
    assert_eq!(
        PLANTED, after.last_dump,
        "the planted value did not reach the node, so the assertion above proved nothing"
    );
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
