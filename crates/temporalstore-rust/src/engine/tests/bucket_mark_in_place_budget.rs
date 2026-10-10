// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A DELETE THAT MARKS IN PLACE WOULD COST, AND WHY NO COMPACTION CAP RESCUES IT.
//!
//! #1995 refuted folding `deleted_object_index` into a per-page bit on a LIFETIME argument: by the
//! time the deletion matters there is no page left to carry the bit, because the delete path
//! removes the pages and keeps the id. That is correct, and it is a property of OUR delete path
//! rather than a law. A design that MARKED IN PLACE -- kept the entry and set its `deleted` bit --
//! would have a page to mark, and `BlockIndex.deleted` already exists, beside `dirty` and
//! `log_backed`. So the open question was never whether the bit exists. It is what KEEPING THE
//! ENTRY costs, against a compaction policy rather than in isolation.
//!
//! THIS MODULE PRICES IT AND THE ANSWER IS NO, AT EVERY CAP, INCLUDING ONE.
//!
//! The arithmetic is short enough to state before the measurement, and both halves are read from
//! the types rather than quoted:
//!
//! ```text
//!   what removing the field SAVES : size_of::<DeletedObjectIndex>()      = 8 B per bucket
//!   what keeping ONE entry COSTS  : size_of::<(u64, BlockIndex)>()       = 72 B per entry
//! ```
//!
//! So a mark-in-place design breaks even only while a bucket holds fewer than `8/72 = 0.111`
//! retained deleted entries -- **fewer than one bucket in nine may hold even a single one.** Every
//! delete of an object with a page produces one. There is no compaction cap that satisfies this
//! except a cap of ZERO, and a cap of zero is what the engine already implements: it removes the
//! entry. The comparison design's bounded chain and rewrite-into-the-first-page do not change the
//! sign, because the bound they impose is 128 entries and the break-even bound is a ninth of one.
//!
//! WHY THIS IS A WORSE TRADE THAN THE ONE #1995 PRICED, and by how much. #1995 measured retaining
//! dead IDS, at 8 bytes each, and found two of its four points winning:
//!
//! ```text
//!    4,000 / 1-in-20 :   197 dead ids   -6,584 B   wins
//!    4,000 / 1-in-4  :   979            -328 B     wash
//!   40,000 / 1-in-20 : 2,000            +7,808 B   loses
//!   40,000 / 1-in-4  : 10,000           +71,808 B  loses 8.8x
//! ```
//!
//! A retained ENTRY is 72 bytes where a retained id is 8, so every one of those rows moves by 9x on
//! the cost side while the saving is unchanged. The two winning rows do not survive it. This module
//! drives the same four points and reports the entry-priced net beside the cap sweep, so the two
//! measurements can be read against each other.
//!
//! WHAT IS MEASURED RATHER THAN ASSUMED. The count of entries a mark-in-place delete would retain
//! is not derived from the delete count: it is the page count each bucket LOSES across the delete,
//! measured by snapshotting `block_index.len()` per bucket before and after. A delete of a key
//! whose pages were never filed removes nothing and would retain nothing, and that difference is
//! exactly the sort of thing an assumed count gets wrong.
//!
//! THE CONTROL IS AN ARM, NOT AN ASIDE. A fifth arm deletes NOTHING. The change predicts no
//! retained entry there, and the arm reports 0 retained entries and 0.00% of the node budget -- so
//! a harness that silently failed to reach the delete path would be visible as a fifth zero row
//! rather than as a refutation.
//!
//! WHAT THIS MODULE DOES NOT CLAIM. It does not claim the delete path should stay as it is for any
//! reason other than this one. The naming defect #1996 recorded beside it -- that
//! `mark_bucket_index_block_deleted` is named for a mark it does not make -- is real and is
//! untouched by this: the function's body is a `retain` returning false, and the measurement here
//! is the reason it should stay one. It also does not price the per-page work a mark-in-place
//! design would unblock; that is a separate question with a separate denominator, and the bytes
//! measured here are the ones that decide THIS change.

#![allow(clippy::all)]
use super::*;

use crate::engine::state::{BlockIndex, BucketNode, DeletedObjectIndex};
use std::mem::size_of;

const MIP_SHARD: ShardId = 1;

/// The documented production routing range, `TS_SHARD_END_ROUTING_BUCKET=1023`.
const MIP_END_BUCKET: u32 = 1023;

/// What removing `deleted_object_index` from the node is worth, per bucket. Read from the type, so
/// a change to the field's shape moves this rather than contradicting it.
fn saving_per_bucket() -> usize {
    size_of::<DeletedObjectIndex>()
}

/// What KEEPING one deleted page entry costs: the handle plus the entry, as the page map stores the
/// pair. Read from the tuple so padding is counted the way the container counts it.
fn cost_per_retained_entry() -> usize {
    size_of::<(u64, BlockIndex)>()
}

// -----------------------------------------------------------------------------------------------
// 1. THE ARITHMETIC, AND THE BREAK-EVEN CAP DERIVED RATHER THAN ASSERTED.
// -----------------------------------------------------------------------------------------------

/// THE BREAK-EVEN COMPACTION CAP IS LESS THAN ONE ENTRY PER BUCKET, so no cap above zero pays.
///
/// THE CONTROL: every width comes from a type. If `size_of::<BucketNode>()` is not what the node's
/// own `const _` assert pins, this module is describing a structure the engine does not have, and
/// the row that says so is checked first.
///
/// rust-internal: reads type widths only, no store
#[test]
fn the_break_even_compaction_cap_is_under_one_entry_per_bucket() {
    let node = size_of::<BucketNode>();
    let saving = saving_per_bucket();
    let entry = size_of::<BlockIndex>();
    let stride = cost_per_retained_entry();

    println!("  {:<44} {:>6}", "size_of::<BucketNode>()", node);
    println!("  {:<44} {:>6}", "size_of::<DeletedObjectIndex>()  (saving)", saving);
    println!("  {:<44} {:>6}", "size_of::<BlockIndex>()", entry);
    println!("  {:<44} {:>6}", "size_of::<(u64, BlockIndex)>()  (stride)", stride);

    // The control. These are the widths this module's whole argument rests on.
    assert_eq!(node, 96, "the node moved; every figure in this module's header is stale");
    assert_eq!(saving, 8, "the tombstone field is no longer 8 bytes; re-price the saving");
    // 40, NOT 56: the entry stopped naming its element, so it lost a fat optional pointer -- two
    // whole words out of the eight-aligned group -- and the `(u64, BlockIndex)` stride printed
    // above went 64 -> 48 with it. The module's argument is a RATIO, `saving` against `stride`, so
    // it survives the move untouched, and that is worth saying rather than leaving the reader to
    // check: 8 against 48 is the same refutation 8 against 64 and 8 against 72 both were. THREE
    // widths now and one ratio, which is the standing argument for pricing a ratio.
    //
    // THIS ASSERTION IS WHY THE WIDTH SWEEP WAS NOT ENOUGH. It names no literal beside
    // `size_of::<BlockAddress>()`, so a scan for a stale address width could not see it, and
    // `cargo check` cannot see an `assert_eq!` in a test body. It failed on the first run of the
    // suite, which is the only thing that could have found it.
    assert_eq!(entry, 40, "the page entry moved; re-price the retained-entry cost");
    assert_eq!(
        stride, 48,
        "the retained-entry stride is {stride}; the cap arithmetic below prices one retained entry \
         at the stride and not at the entry, so a stride that moves without the entry means the \
         pair above has drifted"
    );
    assert!(
        stride >= entry,
        "a stride of {stride} cannot be smaller than the {entry}-byte entry it carries"
    );

    // The break-even cap: keeping C entries per bucket costs `stride * C` and saves `saving`.
    // Expressed as a ratio so it does not depend on either literal.
    println!(
        "\n  break-even retained entries per bucket = saving/stride = {saving}/{stride} = {:.4}",
        saving as f64 / stride as f64
    );
    assert!(
        saving < stride,
        "PRE: one retained entry ({stride} B) must cost more than the field saves ({saving} B), or \
         the whole argument inverts and a mark-in-place delete becomes cheap"
    );

    // Every cap a bounded chain could choose, including the tightest one above zero.
    println!("\n  cap  cost/bucket  net vs saving   verdict");
    let mut any_cap_wins = false;
    for cap in [1usize, 8, 32, 128] {
        let cost = stride * cap;
        let net = cost as i64 - saving as i64;
        let verdict = if net < 0 { "WINS" } else { "LOSES" };
        if net < 0 {
            any_cap_wins = true;
        }
        println!("  {cap:>3}  {cost:>11}  {net:>+13}   {verdict}");
    }
    assert!(
        !any_cap_wins,
        "a compaction cap of at least one entry per bucket now pays; the refutation below is void \
         and this module must be re-measured"
    );

    // A cap of zero is the engine as it stands, and it is not a mark-in-place design.
    println!(
        "\n  cap    0  {:>11}  {:>+13}   WINS, and is the delete path this engine already has",
        0, -(saving as i64)
    );
}

// -----------------------------------------------------------------------------------------------
// 2. THE MEASUREMENT, DRIVEN, AT TWO CORPUS SIZES AND TWO DELETE RATES, WITH A ZERO-DELETE CONTROL.
// -----------------------------------------------------------------------------------------------

fn mip_engine(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("mip-cache"),
        dir.join("mip-pages"),
        dir.join("mip-indexes"),
    )
}

fn mip_load(engine: &TemporalEngine, end_routing_bucket: u32) {
    engine.load_shard_with(LoadShardRequest {
        shard_id: MIP_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
}

/// Pages per bucket, as the shard holds them right now.
fn pages_by_bucket(engine: &TemporalEngine) -> std::collections::BTreeMap<u32, usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&MIP_SHARD).expect("shard is loaded");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.block_index.len()))
        .collect()
}

/// Tombstoned ids per bucket, as the shard holds them right now.
fn tombstones_by_bucket(engine: &TemporalEngine) -> std::collections::BTreeMap<u32, usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&MIP_SHARD).expect("shard is loaded");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.deleted_object_index.object_count()))
        .collect()
}

/// Percentile off a sorted slice, nearest-rank. Returns 0 for an empty sample.
fn pct(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

struct Arm {
    label: &'static str,
    records: usize,
    /// Delete every Nth key. `None` deletes nothing -- the control.
    delete_every: Option<usize>,
}

/// WHAT A MARK-IN-PLACE DELETE WOULD RETAIN, AND WHAT IT WOULD COST, AT FOUR POINTS AND A CONTROL.
///
/// The state is produced, not constructed. Strings are written and a share of them deleted through
/// the single-command delete path -- the one that retires the pages and writes the tombstone.
///
/// EVERY DENOMINATOR IS PRINTED AND ASSERTED, because an arm that wrote nothing, an arm that
/// deleted nothing and an arm whose deletes found no page all produce "0 entries would be
/// retained", which reads exactly like a cheap design:
///
///   * records acked (the seed reached the store);
///   * keys deleted (the delete path ran);
///   * buckets occupied (the denominator every per-bucket figure divides by);
///   * pages LOST across the delete, per bucket, measured before and after -- the entries a
///     mark-in-place design would have kept; and
///   * tombstoned ids actually written, which must be non-zero wherever pages were lost, or the
///     two halves are not describing the same delete.
///
/// A HISTOGRAM, NEVER A MEAN: p50, p90, p99 and MAX of the per-bucket retained-entry count, with
/// the sample count beside them. A mean of one retained entry per bucket can hold a bucket with
/// none and a bucket with forty, and the cap sweep is decided by the tail.
///
/// THE CONTROL ARM deletes nothing and must report 0 retained entries and 0.00% -- the change
/// predicts exactly nothing there.
///
/// rust-internal: seeds up to 40,000 records per arm, no external surface
#[test]
#[ignore = "seeds 4,000 and 40,000 records across five arms; run by name"]
fn what_a_mark_in_place_delete_would_retain_at_two_corpus_sizes_and_two_delete_rates() {
    let saving = saving_per_bucket();
    let stride = cost_per_retained_entry();
    println!(
        "  saving/bucket = {saving} B   cost/retained entry = {stride} B   break-even = {:.4} entries/bucket\n",
        saving as f64 / stride as f64
    );

    let arms = [
        Arm { label: "4,000  / 1-in-20", records: 4_000, delete_every: Some(20) },
        Arm { label: "4,000  / 1-in-4 ", records: 4_000, delete_every: Some(4) },
        Arm { label: "40,000 / 1-in-20", records: 40_000, delete_every: Some(20) },
        Arm { label: "40,000 / 1-in-4 ", records: 40_000, delete_every: Some(4) },
        Arm { label: "CONTROL: no delete", records: 4_000, delete_every: None },
    ];

    let mut rows = Vec::new();

    for arm in &arms {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = mip_engine(dir.path());
        mip_load(&engine, MIP_END_BUCKET);

        let keys: Vec<String> =
            (0..arm.records).map(|index| format!("mip-{index:06}")).collect();
        let mut acked = 0usize;
        for chunk in keys.chunks(512) {
            let commands = chunk
                .iter()
                .map(|key| Command::StringSet { key: key.clone(), value: vec![b'm'; 64] })
                .collect::<Vec<_>>();
            let response = engine.batch_execute(crate::types::BatchExecuteRequest {
                shard_id: MIP_SHARD,
                commands,
            });
            assert!(response.status.ok, "seed must ack: {:?}", response.status);
            acked += chunk.len();
        }
        assert_eq!(acked, arm.records, "DENOMINATOR: the seed did not ack every record");

        let before = pages_by_bucket(&engine);
        let pages_before: usize = before.values().sum();
        assert!(
            pages_before > 0,
            "DENOMINATOR [{}]: the seed filed no page at all, so nothing could be retained",
            arm.label
        );

        let mut deleted = 0usize;
        if let Some(step) = arm.delete_every {
            for key in keys.iter().step_by(step) {
                let response = engine.execute(ExecuteRequest {
                    shard_id: MIP_SHARD,
                    command: Command::StringDelete { key: key.clone() },
                });
                assert!(response.status.ok, "delete {key} failed: {:?}", response.status);
                deleted += 1;
            }
            assert!(
                deleted > 0,
                "DENOMINATOR [{}]: the arm claims a delete rate and deleted nothing",
                arm.label
            );
        }

        let after = pages_by_bucket(&engine);
        let tombs = tombstones_by_bucket(&engine);
        let pages_after: usize = after.values().sum();
        let tombs_total: usize = tombs.values().sum();

        // The entries a mark-in-place delete would have KEPT: the pages each bucket lost.
        let buckets: std::collections::BTreeSet<u32> =
            before.keys().chain(after.keys()).copied().collect();
        let mut retained_per_bucket: Vec<usize> = Vec::new();
        for routing_bucket in &buckets {
            let b = before.get(routing_bucket).copied().unwrap_or(0);
            let a = after.get(routing_bucket).copied().unwrap_or(0);
            retained_per_bucket.push(b.saturating_sub(a));
        }
        let retained_total: usize = retained_per_bucket.iter().sum();
        let mut sorted = retained_per_bucket.clone();
        sorted.sort_unstable();
        let carrying = sorted.iter().filter(|n| **n > 0).count();

        // The two halves must describe the same delete.
        if deleted > 0 {
            assert!(
                retained_total > 0,
                "[{}]: {deleted} keys were deleted and no bucket lost a page, so the fixture did \
                 not reach the delete path this module prices",
                arm.label
            );
            assert!(
                tombs_total > 0,
                "[{}]: {retained_total} pages were removed and no tombstone was written; the page \
                 removal and the tombstone are supposed to be the same delete",
                arm.label
            );
        } else {
            // THE CONTROL. The change predicts nothing here.
            assert_eq!(
                retained_total, 0,
                "CONTROL: {retained_total} entries would be retained on an arm that deleted \
                 nothing, so the measurement is counting something other than deletes"
            );
            assert_eq!(
                tombs_total, 0,
                "CONTROL: {tombs_total} tombstones on an arm that deleted nothing"
            );
        }

        let n_buckets = buckets.len();
        let today = saving * n_buckets; // what removing the field would recover
        println!("=== {}", arm.label);
        println!(
            "    records acked {acked}   keys deleted {deleted}   buckets {n_buckets}   \
             pages {pages_before} -> {pages_after}   tombstoned ids {tombs_total}"
        );
        println!(
            "    retained entries would be {retained_total} over {n_buckets} buckets; \
             {carrying} buckets carry >=1 ({:.2}%)",
            if n_buckets == 0 { 0.0 } else { 100.0 * carrying as f64 / n_buckets as f64 }
        );
        println!(
            "    per-bucket retained entries: p50 {}  p90 {}  p99 {}  MAX {}   (n={})",
            pct(&sorted, 50.0),
            pct(&sorted, 90.0),
            pct(&sorted, 99.0),
            sorted.last().copied().unwrap_or(0),
            sorted.len()
        );
        println!("    field saving at {saving} B x {n_buckets} buckets = {today} B");
        println!("    cap   retained  cost B      net B        verdict");
        let mut nets = Vec::new();
        for cap in [usize::MAX, 128usize, 8, 1] {
            let kept: usize = retained_per_bucket.iter().map(|n| (*n).min(cap)).sum();
            let cost = kept * stride;
            let net = cost as i64 - today as i64;
            let cap_label = if cap == usize::MAX { "none".to_string() } else { cap.to_string() };
            println!(
                "    {cap_label:<5} {kept:>9}  {cost:>9}   {net:>+10}   {}",
                if net < 0 { "WINS" } else { "LOSES" }
            );
            nets.push((cap_label, net));
        }
        let pct_of_node = if n_buckets == 0 {
            0.0
        } else {
            100.0 * (retained_total * stride) as f64 / (size_of::<BucketNode>() * n_buckets) as f64
        };
        println!(
            "    unbounded retained-entry cost as a share of the node budget: {pct_of_node:.2}%\n"
        );
        rows.push((arm.label, deleted, n_buckets, retained_total, nets, pct_of_node));
    }

    // ------------------------------------------------------------------ the verdict, per arm
    println!("SUMMARY: net bytes of a mark-in-place delete, by compaction cap");
    println!("  {:<19} {:>8} {:>8} {:>9}  {:>12} {:>12} {:>12} {:>12}", "arm", "deleted", "buckets", "retained", "cap none", "cap 128", "cap 8", "cap 1");
    for (label, deleted, n_buckets, retained, nets, _) in &rows {
        println!(
            "  {label:<19} {deleted:>8} {n_buckets:>8} {retained:>9}  {:>+12} {:>+12} {:>+12} {:>+12}",
            nets[0].1, nets[1].1, nets[2].1, nets[3].1
        );
    }

    // Every arm that actually deleted must LOSE at every cap, including a cap of one.
    let mut deleting_arms = 0usize;
    for (label, deleted, _, _, nets, pct_of_node) in &rows {
        if *deleted == 0 {
            assert_eq!(
                *pct_of_node, 0.0,
                "CONTROL [{label}]: the retained-entry cost is {pct_of_node:.2}% of the node \
                 budget on an arm that deleted nothing, and must be 0.00%"
            );
            continue;
        }
        deleting_arms += 1;
        for (cap_label, net) in nets {
            assert!(
                *net > 0,
                "[{label}] at cap {cap_label}: a mark-in-place delete nets {net} B, so it PAYS \
                 here and the refutation this module reports is wrong for this arm"
            );
        }
    }
    assert_eq!(
        deleting_arms, 4,
        "DENOMINATOR: {deleting_arms} arms exercised a delete, not the four this module claims"
    );
}
