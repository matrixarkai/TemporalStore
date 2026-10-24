// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE APPEND COSTS WHEN IT TRAVELS WITH OTHERS, per entry.
//!
//! # THE QUESTION
//!
//! One append of one entry costs a fixed envelope over the user's bytes, and at a small value that
//! envelope IS the amplification: an eight-byte value carries it whole. `publish_wal_entries_batch`
//! makes `N` entries share exactly TWO object-store appends instead of `2N`, so the obvious hope is
//! that it divides the envelope by `N`.
//!
//! IT DOES NOT, and this program is how that is established rather than argued. The envelope is
//! charged PER FRAME, and batching changes how many appends carry the frames, not how many frames
//! there are. Each entry still gets its own log frame and its own sidecar frame, so the per-entry
//! byte cost of a batch of two hundred and fifty-six is the per-entry byte cost of a batch of one.
//!
//! Measured, at an 8-byte value: 93.0 B per entry at `N = 1` and 100.2 B at `N = 256`. Flat, and
//! what movement there is goes the WRONG WAY -- see below.
//!
//! # IT IS SLIGHTLY WORSE, AND THE TERM HAS A NAME
//!
//! A batch does not merely fail to save bytes, it costs a few. Every entry's sidecar records
//! `wal_blob_object_length` as the length AFTER THE WHOLE BATCH, where separate appends each
//! record the length as it stood at their own append. The batched number is larger, and the start
//! and end offsets it sits beside grow with the batch too, so their varints widen. Fitted over the
//! payload sweep the per-entry envelope is 92.7 B on this path against 88.1 B for a single append
//! -- the same denominator, the same sweep, about 4.6 B dearer.
//!
//! That is the honest shape of the trade: batching buys appends and barriers and pays for them in
//! a handful of bytes per entry. It is still worth doing. It is just not an answer to the envelope.
//!
//! # WHAT BATCHING DOES AMORTISE, AND WHY THIS DENOMINATOR CANNOT SEE IT
//!
//! Two appends instead of `2N` is a real and large saving in the things an append costs besides
//! bytes: round trips, durability barriers, and one EXTENT per append in each object's metadata.
//! The published denominator sums `extent.length()` -- the DATA each extent holds -- so a hundred
//! extents holding a hundred bytes and one extent holding a hundred bytes are the same number.
//! That is not a flaw to be corrected by folding extent metadata into the ratio; it is a different
//! term. So the extent COUNT is reported beside the byte figure as its own column, and the
//! unbatched control is run at every point to show the counts actually diverge.
//!
//! # TWO SWEEPS, BECAUSE THERE ARE TWO VARIABLES
//!
//! `N` is swept at a fixed payload to test whether the per-entry cost depends on batch size, and
//! the payload is swept at a fixed `N` to fit `per_entry = rate * payload + envelope`. Quoting a
//! single ratio would hide whichever of the two was moving.
//!
//! The fitted line is `1.0010 * payload + 92.7` with a worst residual of 4.8 B, which puts the
//! per-entry ratio at 1.10x around a 930-byte value and 1.01x around a 10,300-byte one. Below
//! that the envelope is most of what is written: 6.72x at an 8-byte value, 2.21x at 64 B, 1.17x
//! at 512 B. Those crossings are properties of the ENVELOPE, so they do not move with `N` either.
//!
//! rust-internal: measures the shared store's own layout, no product behaviour

use std::sync::Arc;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{Command, MatrixObjectObjectStore};
use temporalstore_snapshot::object_store::ObjectStore;

const BUCKET: &str = "temporalstore-shared";
const SHARD: u64 = 1;
const KEY: &str = "amp-key";
const CLUSTER: &str = "cluster-batched-append";

/// What one run of `count` entries left in the shared WAL prefix.
struct Measured {
    /// key + value, summed over every entry the run published.
    user_bytes: u64,
    /// Sum of every extent's LENGTH over every object under the shared WAL prefix. The same
    /// numerator `one_append_amplification_report` uses, so the two are comparable.
    blob_bytes: u64,
    /// How many objects the prefix holds. Asserted, never assumed.
    objects: usize,
    /// How many extents those objects hold between them -- one per append that reached the store.
    /// This is what batching moves and what the byte figure cannot see.
    extents: usize,
}

/// Publish `count` entries of `payload` bytes each into a store of its own.
///
/// `batched` picks the path under test: one `publish_wal_entries_batch` call, or `count` separate
/// `publish_wal_entry` calls. Everything else is held identical so the difference between the two
/// is the path and nothing else.
async fn run(count: usize, payload: usize, batched: bool, options: &StoreOptions) -> Measured {
    let store =
        Arc::new(MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("a store of its own"));
    let replicator = SharedStoreReplicator::new(CLUSTER, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    let entries: Vec<SharedStoreWalEntry> = (0..count)
        .map(|at| SharedStoreWalEntry {
            shard_id: SHARD,
            wal_index: at as u64 + 1,
            command: Some(Command::StringSet {
                key: KEY.to_string(),
                value: vec![b'v'; payload],
            }),
            staged_blocks: Vec::new(),
            outcomes: Vec::new(),
        })
        .collect();
    let user_bytes = entries.len() as u64 * (KEY.len() + payload) as u64;

    if batched {
        let reports = replicator
            .publish_wal_entries_batch(&entries)
            .await
            .unwrap_or_else(|err| panic!("batch of {count} x {payload} B: {err:?}"));
        // The batch must have published every entry, or the byte total below is over fewer
        // entries than the divisor says.
        assert_eq!(
            reports.len(),
            count,
            "the batch reported {} entries for {count} published",
            reports.len()
        );
        assert!(
            reports.iter().all(|report| report.published),
            "the batch left an entry unpublished, so its bytes are not in this total"
        );
    } else {
        for entry in &entries {
            replicator
                .publish_wal_entry(entry.clone())
                .await
                .unwrap_or_else(|err| panic!("single append of {payload} B: {err:?}"));
        }
    }

    let prefix = format!("{CLUSTER}/shards/{SHARD}/shared/wal/");
    let objects = store.list(&prefix).await.expect("the wal listing");
    let inner = store.inner();
    let guard = inner.lock().expect("store lock poisoned");

    let mut blob_bytes = 0u64;
    let mut extents = 0usize;
    for key in &objects {
        if let Ok(object) = guard.get_object(BUCKET, key) {
            blob_bytes += object
                .metadata
                .extents
                .iter()
                .map(|extent| extent.length())
                .sum::<u64>();
            extents += object.metadata.extents.len();
        }
    }

    Measured {
        user_bytes,
        blob_bytes,
        objects: objects.len(),
        extents,
    }
}

/// Least squares, and the largest residual it leaves.
fn fit(points: &[(f64, f64)]) -> (f64, f64, f64) {
    let n = points.len() as f64;
    let sx: f64 = points.iter().map(|p| p.0).sum();
    let sy: f64 = points.iter().map(|p| p.1).sum();
    let sxx: f64 = points.iter().map(|p| p.0 * p.0).sum();
    let sxy: f64 = points.iter().map(|p| p.0 * p.1).sum();
    let denom = n * sxx - sx * sx;
    let rate = if denom == 0.0 {
        0.0
    } else {
        (n * sxy - sx * sy) / denom
    };
    let envelope = (sy - rate * sx) / n;
    let worst = points
        .iter()
        .map(|p| (p.1 - (rate * p.0 + envelope)).abs())
        .fold(0.0f64, f64::max);
    (rate, envelope, worst)
}

/// Check a measurement is a measurement before any ratio is taken from it.
///
/// An empty listing sums to "0 bytes over 0 entries", which divides into something that reads like
/// a store writing nothing at all. So the denominator is asserted non-zero and the object count is
/// asserted to be the two objects this path writes.
fn assert_sound(measured: &Measured, count: usize, label: &str) {
    assert_eq!(
        measured.objects, 2,
        "{label}: {count} entries left {} objects under the shared WAL prefix, not the log and \
         its offset index. Every byte figure here is a sum over that listing, so a different \
         object count means this is measuring something else",
        measured.objects
    );
    assert!(
        measured.user_bytes > 0,
        "{label}: the user-byte denominator is zero, so any ratio over it is meaningless"
    );
    assert!(
        measured.blob_bytes >= measured.user_bytes,
        "{label}: {} stored bytes against {} user bytes -- an uncompressed append cannot store \
         less than it was given",
        measured.blob_bytes,
        measured.user_bytes
    );
}

#[tokio::main]
async fn main() {
    let options = StoreOptions {
        page_size: 1024,
        max_extent_bytes: 4096,
        chunk_size: 4096,
        replica_count: 3,
        write_quorum: 2,
        ..StoreOptions::default()
    };

    println!("WHAT A BATCHED APPEND COSTS, PER ENTRY");
    println!();
    println!(
        "  store: page_size {}  chunk_size {}  max_extent_bytes {}  replica_count {}  write_quorum {}",
        options.page_size, options.chunk_size, options.max_extent_bytes,
        options.replica_count, options.write_quorum
    );
    println!("  numerator: every extent's length over every object under");
    println!("             {{cluster}}/shards/{{shard}}/shared/wal/");
    println!("  denominator: user bytes -- key + value, summed over the batch");
    println!("  replication (x{}) is a separate multiplier and is NOT folded in", options.replica_count);
    println!();

    // ---- SWEEP ONE: does the per-entry cost depend on the batch size at all?
    const FIXED_PAYLOAD: usize = 8;
    let counts = [1usize, 2, 4, 16, 64, 256];
    println!("  SWEEP OVER BATCH SIZE, at a fixed {FIXED_PAYLOAD} B value");
    println!();
    println!("      N   user B   blob B   per-entry B   blob/user   extents   extents unbatched");

    let mut per_entry_by_count: Vec<f64> = Vec::new();
    for &count in &counts {
        let batched = run(count, FIXED_PAYLOAD, true, &options).await;
        assert_sound(&batched, count, "batched");
        let control = run(count, FIXED_PAYLOAD, false, &options).await;
        assert_sound(&control, count, "unbatched control");

        let per_entry = batched.blob_bytes as f64 / count as f64;
        per_entry_by_count.push(per_entry);
        println!(
            "    {:>3}   {:>6}   {:>6}   {:>11.1}   {:>9.2}   {:>7}   {:>17}",
            count,
            batched.user_bytes,
            batched.blob_bytes,
            per_entry,
            batched.blob_bytes as f64 / batched.user_bytes as f64,
            batched.extents,
            control.extents,
        );

        // ---- THE BYTES MUST NOT FALL WITH N. That is the whole claim, and it is asserted
        // ---- against the control rather than eyeballed from the table.
        // ----
        // ---- The two paths are not quite byte-identical, and the difference is NAMED rather
        // ---- than tolerated blindly: a batch records the object length AFTER THE WHOLE BATCH in
        // ---- every entry's sidecar, where separate appends each record the length as it stood
        // ---- at their own append. The batched number is the larger one, so its varint can be a
        // ---- byte wider -- which makes the batched total slightly BIGGER, never smaller.
        // ----
        // ---- So the assertion is on the SIGN. Amortisation would show up as the opposite one.
        assert!(
            batched.blob_bytes >= control.blob_bytes,
            "a batch of {count} stored {} B where {count} separate appends stored {} B. FEWER \
             bytes batched would mean the per-entry envelope does amortise after all, which is \
             the claim this program exists to test -- re-derive it before quoting anything here",
            batched.blob_bytes,
            control.blob_bytes
        );
        let widening = batched.blob_bytes - control.blob_bytes;
        assert!(
            widening <= 4 * count as u64,
            "the batched total is {widening} B over the unbatched one across {count} entries, \
             which is more than a few varint widenings per sidecar can account for. Something \
             other than the object-length field differs between the two paths; find it before \
             calling the per-entry cost path-independent"
        );

        // ---- AND THE REGIME MUST HAVE BEEN REACHED. For N > 1 the batch has to have used fewer
        // ---- extents than the control, or no batching happened and this row is two identical
        // ---- runs agreeing with each other.
        if count > 1 {
            assert!(
                batched.extents < control.extents,
                "a batch of {count} used {} extents and {count} separate appends used {} -- the \
                 batch did not coalesce, so this row proves nothing about batching",
                batched.extents,
                control.extents
            );
        }
    }

    // ---- THE HEADLINE OF SWEEP ONE, as a spread rather than a pair of endpoints.
    let smallest = per_entry_by_count.iter().copied().fold(f64::MAX, f64::min);
    let largest = per_entry_by_count.iter().copied().fold(0.0f64, f64::max);
    println!();
    println!("  PER-ENTRY BYTES ARE FLAT IN THE BATCH SIZE: {smallest:.1} B at the cheapest N,");
    println!("  {largest:.1} B at the dearest, over N from {} to {}.", counts[0], counts[counts.len() - 1]);
    assert!(
        counts.len() >= 5 && counts[counts.len() - 1] >= 64 * counts[0],
        "the batch-size sweep spans too little for flatness in N to mean anything"
    );

    // ---- WHAT THIS TOLERANCE CANNOT SEE, said out loud rather than left implicit. The sidecar
    // ---- carries five offset-shaped fields -- start, end, bytes written, object length, first
    // ---- physical offset -- and each is a varint that gains a byte as the log grows past 127,
    // ---- then past 16383. Across this sweep that is a few bytes of unavoidable UPWARD drift per
    // ---- entry, and the bound has to admit it.
    // ----
    // ---- It is still an order of magnitude tighter than what it is guarding against: dividing
    // ---- an ~88 B envelope by the largest batch here would take per-entry bytes DOWN by ~87 B,
    // ---- not up by a handful. A tolerance that admitted that would be the wrong tolerance.
    assert!(
        largest - smallest < 10.0,
        "per-entry bytes moved from {smallest:.1} to {largest:.1} B across the batch-size sweep, \
         a spread of {:.1} B. That is more than varint widening in the sidecar's offset fields \
         can account for, so something else in the envelope DOES depend on N; find it before \
         quoting a flat figure",
        largest - smallest
    );

    // ---- AND THE STRONG FORM, which no tolerance can blur: per-entry bytes at the largest batch
    // ---- must still be most of what they are at a batch of one. Amortisation would put the last
    // ---- rung near zero, and this fails long before the spread bound above would.
    let at_one = per_entry_by_count[0];
    let at_many = per_entry_by_count[per_entry_by_count.len() - 1];
    assert!(
        at_many > at_one * 0.9,
        "per-entry bytes fell from {at_one:.1} B at N={} to {at_many:.1} B at N={}. That is the \
         envelope amortising with the batch size, and every claim in this program's header is \
         wrong",
        counts[0],
        counts[counts.len() - 1]
    );

    // ---- SWEEP TWO: the per-entry line over the payload, at a batch size well above one.
    const FIXED_COUNT: usize = 64;
    let sizes = [0usize, 8, 32, 128, 512, 2048, 8192];
    assert!(
        sizes.len() >= 5 && *sizes.last().unwrap() >= 16 * sizes[1].max(1),
        "the payload sweep is too narrow to separate the slope from the envelope"
    );
    println!();
    println!("  SWEEP OVER VALUE SIZE, at a fixed batch of {FIXED_COUNT}");
    println!();
    println!("    payload   user B   blob B   per-entry B   blob/user");

    let mut points: Vec<(f64, f64)> = Vec::new();
    for &payload in &sizes {
        let measured = run(FIXED_COUNT, payload, true, &options).await;
        assert_sound(&measured, FIXED_COUNT, "payload sweep");
        let per_entry = measured.blob_bytes as f64 / FIXED_COUNT as f64;
        println!(
            "    {:>7}   {:>6}   {:>6}   {:>11.1}   {:>9.2}",
            payload,
            measured.user_bytes,
            measured.blob_bytes,
            per_entry,
            measured.blob_bytes as f64 / measured.user_bytes as f64,
        );
        points.push((payload as f64, per_entry));
    }

    let (rate, envelope, resid) = fit(&points);
    println!();
    println!("  THE PER-ENTRY LINE, fitted over the sweep rather than quoted from a point");
    println!();
    println!("    per entry = {rate:.4} * payload + {envelope:.1}      worst residual {resid:.1} B");
    println!();

    // ---- THE FIT HAS TO BE A FIT.
    assert!(
        resid < envelope.abs().max(1.0),
        "the per-entry fit leaves a residual of {resid:.1} B against an envelope of \
         {envelope:.1} B, so `rate * payload + envelope` is not the shape of this cost"
    );
    assert!(
        rate >= 1.0,
        "the marginal rate is {rate:.4}, under 1.0 -- an append cannot store less than it is given"
    );

    // ---- WHERE IT CROSSES. The envelope is what makes a small value expensive, so the useful
    // ---- statement is the payload at which it stops mattering. Derived from the fitted line,
    // ---- and the key's own bytes are part of the user total.
    let crossing = |target: f64| (envelope / (target - rate)) - KEY.len() as f64;
    println!("  WHERE THE PER-ENTRY RATIO CROSSES, derived from that line:");
    println!("    1.10x at a value of about {:>8.0} B", crossing(1.10));
    println!("    1.01x at a value of about {:>8.0} B", crossing(1.01));
    println!();
    println!("  AND AT THE SMALL VALUES THE ENVELOPE DOMINATES:");
    for &payload in &[8usize, 64, 512] {
        let user = (KEY.len() + payload) as f64;
        println!("    a {:>4} B value -> {:>6.2}x per entry", payload, (rate * payload as f64 + envelope) / user);
    }
    println!();

    // ---- WHAT BATCHING ACTUALLY BOUGHT, stated in the term it bought it in.
    let batched = run(256, FIXED_PAYLOAD, true, &options).await;
    let control = run(256, FIXED_PAYLOAD, false, &options).await;
    assert_sound(&batched, 256, "extent summary batched");
    assert_sound(&control, 256, "extent summary control");
    println!("  WHAT BATCHING MOVES IS NOT BYTES. 256 entries of {FIXED_PAYLOAD} B:");
    println!("    batched     {:>6} B over {:>4} extents", batched.blob_bytes, batched.extents);
    println!("    unbatched   {:>6} B over {:>4} extents", control.blob_bytes, control.extents);
    println!(
        "    bytes differ by {} B ({:+.2}%); extents differ {:.0}x",
        batched.blob_bytes as i64 - control.blob_bytes as i64,
        100.0 * (batched.blob_bytes as f64 - control.blob_bytes as f64) / control.blob_bytes as f64,
        control.extents as f64 / batched.extents as f64
    );
    println!();
    println!("  So the per-entry envelope is NOT a batching problem. Batching removes appends,");
    println!("  barriers and extents -- all of which this numerator is blind to by construction --");
    println!("  and leaves every frame's own envelope exactly where it was. The envelope moves");
    println!("  only by making a FRAME smaller.");

    // ---- THE CLAIM ABOVE, ASSERTED. Bytes do not fall; extents do.
    assert!(
        batched.blob_bytes >= control.blob_bytes,
        "256 entries cost {} B batched and {} B unbatched -- batching saved bytes, which \
         contradicts the summary above",
        batched.blob_bytes,
        control.blob_bytes
    );
    assert!(
        batched.blob_bytes - control.blob_bytes <= 4 * 256,
        "the batched total is {} B over the unbatched one, more than varint widening explains",
        batched.blob_bytes - control.blob_bytes
    );
    assert!(
        control.extents > batched.extents * 8,
        "the unbatched control used {} extents against the batch's {}, which is not the order-of\
         -magnitude difference the summary claims",
        control.extents,
        batched.extents
    );
}
