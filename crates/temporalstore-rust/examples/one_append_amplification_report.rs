// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE APPEND COSTS, as a line rather than a point.
//!
//! # WHY A SWEEP AND NOT ONE WRITE
//!
//! One value size cannot separate the per-entry envelope from the per-byte cost. At 5 bytes of
//! payload a 90-byte envelope reads as 19x; at 4 KiB the same envelope reads as 1.02x. Both are
//! "the amplification" and neither is a fact about the system. So the value size is swept and
//! `stored = rate * payload + envelope` is fitted over it, with the residual printed so a
//! non-linear term cannot hide inside a quoted slope.
//!
//! # THE TWO LEGS ARE PRICED SEPARATELY
//!
//! One append in `ProtobufAppendBlob` mode writes exactly TWO objects and they are not the same
//! cost, so folding them would hide whichever is larger:
//!
//! ```text
//!   the log frame   the entry itself: a 4-byte length prefix and a protobuf record
//!   the sidecar     one frame in the per-shard offset index, saying WHERE the entry is
//! ```
//!
//! There is NO third leg per append. A checkpoint is a whole-index serialize, and nothing on this
//! path provokes one: `publish_checkpoint` is driven by the operator endpoint and once at start,
//! never by an append. So one append's cost is these two objects and the replica count, and this
//! program asserts the object count rather than assuming it.
//!
//! # FOUR DENOMINATORS, NOT INTERCHANGEABLE
//!
//! ```text
//!   user bytes      what the caller handed us -- key + value
//!   entry bytes     one WAL entry's extent length: the protobuf encoding
//!   blob bytes      every extent the append blob holds -- BOTH legs
//!   physical bytes  blob bytes times the replica count -- the store's durability choice
//! ```
//!
//! A ratio without its pair named is a number nobody can reproduce, so every figure below says
//! which two it is over.
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

/// The cluster id the sweep runs under, and a much longer one used to show the envelope no longer
/// depends on it. These differ by 40 characters.
const CLUSTER: &str = "cluster-one-append";
const LONG_CLUSTER: &str = "cluster-one-append-with-a-deliberately-much-longer-name";

/// One append of `payload` bytes into a store of its own, and what the store then holds.
async fn one_append(
    cluster: &str,
    payload: usize,
    options: &StoreOptions,
) -> (usize, u64, u64, usize) {
    let store = Arc::new(
        MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("a store of its own"),
    );
    let replicator = SharedStoreReplicator::new(cluster, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    let value = vec![b'v'; payload];
    let user_bytes = KEY.len() + value.len();

    replicator
        .publish_wal_entry(SharedStoreWalEntry {
            shard_id: SHARD,
            wal_index: 1,
            command: Some(Command::StringSet { key: KEY.to_string(), value }),
            staged_blocks: Vec::new(),
            outcomes: Vec::new(),
        })
        .await
        .unwrap_or_else(|err| panic!("append of {payload} bytes: {err:?}"));

    // ---- THE ENTRY, and then EVERY object the append left behind. The sidecar is a second
    // ---- object and it is part of what an append costs, so it is counted rather than ignored.
    let blob_key = format!("{cluster}/shards/{SHARD}/shared/wal/wal.protobuf.blob");
    let objects = store
        .list(&format!("{cluster}/shards/{SHARD}/shared/wal/"))
        .await
        .expect("the wal listing");

    let inner = store.inner();
    let guard = inner.lock().expect("store lock poisoned");

    let entry_bytes: u64 = guard
        .get_object(BUCKET, &blob_key)
        .expect("the append blob")
        .metadata
        .extents
        .iter()
        .map(|extent| extent.length())
        .sum();

    let mut blob_bytes = 0u64;
    for key in &objects {
        if let Ok(object) = guard.get_object(BUCKET, key) {
            blob_bytes += object.metadata.extents.iter().map(|e| e.length()).sum::<u64>();
        }
    }

    (user_bytes, entry_bytes, blob_bytes, objects.len())
}

/// Least squares over the sweep, and the largest residual it leaves.
fn fit(points: &[(f64, f64)]) -> (f64, f64, f64) {
    let n = points.len() as f64;
    let sx: f64 = points.iter().map(|p| p.0).sum();
    let sy: f64 = points.iter().map(|p| p.1).sum();
    let sxx: f64 = points.iter().map(|p| p.0 * p.0).sum();
    let sxy: f64 = points.iter().map(|p| p.0 * p.1).sum();
    let denom = n * sxx - sx * sx;
    let rate = if denom == 0.0 { 0.0 } else { (n * sxy - sx * sy) / denom };
    let envelope = (sy - rate * sx) / n;
    let worst = points
        .iter()
        .map(|p| (p.1 - (rate * p.0 + envelope)).abs())
        .fold(0.0f64, f64::max);
    (rate, envelope, worst)
}

/// Sweep the value size under one cluster id and return the fitted line for both legs.
async fn sweep(
    cluster: &str,
    sizes: &[usize],
    options: &StoreOptions,
    print: bool,
) -> (f64, f64, f64) {
    let mut blob_points: Vec<(f64, f64)> = Vec::new();
    let mut entry_points: Vec<(f64, f64)> = Vec::new();

    if print {
        println!("    payload   user B   entry B   sidecar B   blob B   objects   blob/user   physical/user");
    }
    for &payload in sizes {
        let (user, entry, blob, objects) = one_append(cluster, payload, options).await;
        // ---- THE OBJECT COUNT IS ASSERTED, NOT ASSUMED. A listing that came back short would
        // ---- make every ratio below a ratio over the wrong denominator, and an empty one would
        // ---- read as "0 bytes over 0 objects", which looks free.
        assert_eq!(
            objects, 2,
            "one append left {objects} objects behind, not the log frame and its sidecar. Every \
             byte figure in this table is a sum over that listing, so a different object count \
             means this is measuring something else"
        );
        assert!(
            entry > 0 && blob >= entry,
            "entry {entry} B and blob {blob} B are not a log frame inside a listing that \
             contains it"
        );
        let physical = blob * options.replica_count as u64;
        if print {
            println!(
                "    {:>7}   {:>6}   {:>7}   {:>9}   {:>6}   {:>7}   {:>9.2}   {:>13.2}",
                payload,
                user,
                entry,
                blob - entry,
                blob,
                objects,
                blob as f64 / user as f64,
                physical as f64 / user as f64,
            );
        }
        blob_points.push((payload as f64, blob as f64));
        entry_points.push((payload as f64, entry as f64));
    }

    let (blob_rate, blob_env, blob_resid) = fit(&blob_points);
    if print {
        let (entry_rate, entry_env, entry_resid) = fit(&entry_points);
        println!();
        println!("  THE LINE, fitted over the sweep rather than quoted from a point");
        println!();
        println!("    log frame    = {entry_rate:.4} * payload + {entry_env:.1}      worst residual {entry_resid:.1} B");
        println!("    both legs    = {blob_rate:.4} * payload + {blob_env:.1}      worst residual {blob_resid:.1} B");
        println!("    the sidecar is the difference: {:.1} B, and it does not move with the payload", blob_env - entry_env);
    }
    (blob_rate, blob_env, blob_resid)
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

    println!("WHAT ONE APPEND COSTS");
    println!();
    println!("  one WAL entry per store, swept over the value size");
    println!("  store: page_size {}  chunk_size {}  max_extent_bytes {}  replica_count {}  write_quorum {}",
             options.page_size, options.chunk_size, options.max_extent_bytes,
             options.replica_count, options.write_quorum);
    println!();

    let sizes = [0usize, 8, 32, 128, 512, 2048, 8192];
    // ---- THE SWEEP MUST SPAN ENOUGH TO SEPARATE A SLOPE FROM A CONSTANT. Over a narrow range a
    // ---- constant and a slope are not distinguishable and the fit would report whichever the
    // ---- noise preferred.
    assert!(
        sizes.len() >= 5 && *sizes.last().unwrap() >= 16 * sizes[1].max(1),
        "the sweep is too narrow to separate the slope from the envelope"
    );

    let (blob_rate, blob_env, blob_resid) = sweep(CLUSTER, &sizes, &options, true).await;

    println!();
    println!("  SO THE PER-APPEND ENVELOPE IS ABOUT {:.0} BYTES, and the marginal rate is {:.3}x.", blob_env, blob_rate);
    println!("  A ratio quoted at one payload size is mostly that envelope divided by that size:");
    println!("    at {:>5} B payload the envelope alone is {:>8.2}x", sizes[1], blob_env / (KEY.len() + sizes[1]) as f64);
    println!("    at {:>5} B payload the envelope alone is {:>8.2}x", sizes[sizes.len() - 1], blob_env / (KEY.len() + sizes[sizes.len() - 1]) as f64);
    println!();

    // ---- AND THE ENVELOPE NO LONGER DEPENDS ON THE CLUSTER ID. The sidecar used to write the
    // ---- log's whole object key into every frame, so a longer cluster id made every append more
    // ---- expensive forever. The key is now written only when it is not the derived one, which
    // ---- means this comparison is the test of that: two cluster ids 40 characters apart.
    println!("  THE ENVELOPE IS INDEPENDENT OF THE CLUSTER ID, which it was not when the sidecar");
    println!("  wrote the log's object key into every frame:");
    let (_, long_env, _) = sweep(LONG_CLUSTER, &sizes, &options, false).await;
    println!("    cluster id {:>2} chars -> envelope {:.1} B", CLUSTER.len(), blob_env);
    println!("    cluster id {:>2} chars -> envelope {:.1} B", LONG_CLUSTER.len(), long_env);
    assert!(
        LONG_CLUSTER.len() >= CLUSTER.len() + 30,
        "the two cluster ids differ by {} characters, too few for their difference to show up as \
         bytes if it were still being written",
        LONG_CLUSTER.len() - CLUSTER.len()
    );
    assert!(
        (blob_env - long_env).abs() < 1.0,
        "the envelope moved from {blob_env:.1} B to {long_env:.1} B when the cluster id grew by \
         {} characters, so a per-append cost is scaling with the length of a name again. The \
         sidecar is the usual culprit: it writes the log's object key only when that key is not \
         the one the shard derives",
        LONG_CLUSTER.len() - CLUSTER.len()
    );
    println!();
    println!("  AND THE REPLICATION FACTOR IS SEPARATE, and multiplies whatever the above is:");
    println!("    replica_count {} with stripe_redundancy_units {} -> {:.1}x of the blob",
             options.replica_count, options.stripe_redundancy_units, options.replica_count as f64);
    println!("    a non-zero stripe_redundancy_units would make that {:.2}x instead",
             options.replica_count as f64 / (options.replica_count - 1) as f64);

    // ---- THE FIT HAS TO BE A FIT. A line quoted over points that do not lie on one is a
    // ---- summary of nothing, so the residual is asserted small against the envelope.
    assert!(
        blob_resid < blob_env.abs().max(1.0),
        "the blob-byte fit leaves a residual of {blob_resid:.1} B against an envelope of \
         {blob_env:.1} B, so `rate * payload + envelope` is not the shape of this cost and the \
         slope above is not a rate. Print the sweep and find the term that is missing"
    );

    // ---- AND THE MARGINAL RATE MUST BE AT LEAST ONE. Below 1.0 would mean the store writes
    // ---- fewer bytes than the payload, which for an uncompressed append means the sweep is
    // ---- measuring something other than the payload reaching storage.
    assert!(
        blob_rate >= 1.0,
        "the marginal rate is {blob_rate:.4}, under 1.0 -- an append cannot store less than it is \
         given unless something is compressing or dropping it, and neither is expected on this path"
    );

    // ---- THE ENVELOPE HAS A CEILING, AND THE CEILING FOLLOWS THE ENVELOPE DOWN. The slope is at
    // ---- its floor and cannot improve; the envelope is the only part of this cost anyone can
    // ---- still move, so a regression in it should fail here rather than be discovered in a later
    // ---- report. A ceiling left where it was after the envelope dropped is a guard that has
    // ---- stopped guarding: it would admit the whole of the saving being handed back.
    // ----
    // ---- The bound is loose enough to survive a protobuf varint widening and tight enough that
    // ---- undoing the narrowing of the log frame's checksum -- a CRC32C back to a 32-byte digest
    // ---- is 29 B, and back to 64 hex characters is 59 -- trips it, as does re-adding either
    // ---- term retired from the sidecar.
    assert!(
        blob_env < 70.0,
        "the per-append envelope is {blob_env:.1} B, over the 70 B ceiling. Four named terms were \
         removed or narrowed to get it here -- a duplicate hex digest the read path never \
         consulted, the log's object key in every frame, the transcription of the frame's own \
         digest, and then that digest replaced by the CRC32C the check was always for. Re-price \
         the two legs field by field before raising this bound"
    );
}
