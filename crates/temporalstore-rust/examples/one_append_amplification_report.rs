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
//! Both command records and outcomes captured from an engine write are measured separately.
//! Every append is read back through a fresh reader before its size is accepted.
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
//!   replicated bytes  blob bytes times the replica count -- a model, not disk usage
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
use temporalstore_rust::{Command, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine};
use temporalstore_snapshot::object_store::ObjectStore;

const BUCKET: &str = "temporalstore-shared";
const SHARD: u64 = 1;
const KEY: &str = "amp-key";

/// The cluster id the sweep runs under, and a much longer one used to show the envelope no longer
/// depends on it. These differ by 40 characters.
/// What the design this one follows was measured to put on disk for one operation, and the payload that
/// measurement carried. Both come from a disk measurement of one of its hash operations:
/// about 158.6 B written per operation over a payload of about 144 B.
///
/// The payload figure is a LOWER bound on what that operation carried, so the envelope these
/// two imply -- 158.6 - 144 = 14.6 B -- is an UPPER bound on theirs. Every distance printed
/// below is therefore a LOWER bound on ours, which is the direction that cannot flatter us.
const THEIR_BYTES_PER_OP: f64 = 158.6;
const THEIR_PAYLOAD_BYTES: f64 = 144.0;

const CLUSTER: &str = "cluster-one-append";
const LONG_CLUSTER: &str = "cluster-one-append-with-a-deliberately-much-longer-name";

#[derive(Clone, Copy, Debug)]
enum Shape {
    Command,
    Outcome,
}

// Capture the engine's own record: invented object IDs or omitted blocks change the cost.
fn entry_for(shape: Shape, value: Vec<u8>) -> SharedStoreWalEntry {
    let command = Command::StringSet {
        key: KEY.to_string(),
        value,
    };
    if matches!(shape, Shape::Command) {
        return SharedStoreWalEntry {
            shard_id: SHARD,
            wal_index: 1,
            command: Some(command),
            staged_blocks: Vec::new(),
            outcomes: Vec::new(),
        };
    }
    let dir = tempfile::tempdir().expect("bounded engine fixture");
    let primary = TemporalEngine::with_local_dirs(
        1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("index"),
    );
    primary.load_shard(SHARD);
    assert!(
        primary
            .execute(ExecuteRequest {
                shard_id: SHARD,
                command
            })
            .status
            .ok
    );
    let records = primary
        .write_ahead_log_store()
        .scan(SHARD, 0, u64::MAX, u64::MAX)
        .expect("scan the engine append");
    let mut writes = records
        .iter()
        .map(|(_, bytes)| {
            temporalstore_rust::wal::decode_wal_line(bytes).expect("decode engine record")
        })
        .filter(|record| !record.outcomes.is_empty());
    let record = writes.next().expect("the engine must write outcomes");
    assert!(
        writes.next().is_none(),
        "one write must produce one outcome record"
    );
    assert!(
        record.command.is_none(),
        "the outcome sweep must not carry a command"
    );
    assert_eq!(
        record.outcomes.len(),
        1,
        "one string write must yield one outcome"
    );
    let mut staged_blocks = record.staged_blocks;
    if staged_blocks.is_empty() {
        for item in &record.outcomes {
            if let Some(address) = item.resolved_address() {
                staged_blocks.push(temporalstore_rust::wal::StagedBlock {
                    object_id: item.object_id,
                    component: item.component.as_deref().map(Arc::from),
                    bytes: primary.block_store().read(&address).expect("carried page"),
                });
            }
        }
    }
    SharedStoreWalEntry {
        shard_id: SHARD,
        wal_index: 1,
        command: record.command,
        staged_blocks,
        outcomes: record.outcomes,
    }
}

/// One append of `payload` bytes into a store of its own, and what the store then holds.
async fn one_append(
    shape: Shape,
    cluster: &str,
    payload: usize,
    options: &StoreOptions,
) -> (usize, u64, u64, usize) {
    let store = Arc::new(
        MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("a store of its own"),
    );
    let replicator = SharedStoreReplicator::new(cluster, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    // Deterministic dense bytes keep block compression from hiding per-byte storage costs.
    let mut seed = 0x1234_5678u32;
    let value: Vec<u8> = (0..payload)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect();
    let user_bytes = KEY.len() + value.len();

    let expected = entry_for(shape, value);
    replicator
        .publish_wal_entry(expected.clone())
        .await
        .unwrap_or_else(|err| panic!("append of {payload} bytes: {err:?}"));
    // A fresh reader cannot answer from the writer's state. Every measured byte must recover.
    let reader = SharedStoreReplicator::new(cluster, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
    let read = reader
        .read_wal_entry_by_offset_metadata(SHARD, 1)
        .await
        .expect("indexed read")
        .expect("the measured entry must exist");
    assert_eq!(
        read.entry, expected,
        "the measured append must read back intact"
    );

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
        let object = guard
            .get_object(BUCKET, key)
            .expect("every listed object must be counted");
        blob_bytes += object
            .metadata
            .extents
            .iter()
            .map(|e| e.length())
            .sum::<u64>();
    }

    (user_bytes, entry_bytes, blob_bytes, objects.len())
}

/// `count` appends of `payload` bytes each, published as ONE batch, into a store of its
/// own -- and what the store then holds in total.
///
/// This exists to turn an argument into a measurement. `publish_wal_entries_batch`
/// concatenates every entry's frame into one append and every sidecar into one more, so a
/// batch amortizes the two durable barriers and the per-OBJECT overhead beneath them. What it
/// cannot amortize is the per-ENTRY frame and the per-entry sidecar message, because each
/// entry still gets one of each. Sweeping the batch size says how much of the envelope is
/// which, instead of asserting it in a comment.
async fn batched_appends(
    shape: Shape,
    cluster: &str,
    payload: usize,
    count: usize,
    options: &StoreOptions,
) -> (usize, u64, u64, usize) {
    assert!(count >= 1, "a batch of {count} entries measures nothing");
    let store = Arc::new(
        MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("a store of its own"),
    );
    let replicator = SharedStoreReplicator::new(cluster, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    let mut seed = 0x1234_5678u32;
    let value: Vec<u8> = (0..payload)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            (seed & 0xff) as u8
        })
        .collect();

    // One entry's worth of content, repeated under successive indices. The content is held
    // constant on purpose: this sweep is about FRAMING, so varying the payload between
    // entries would move the thing being measured.
    let template = entry_for(shape, value);
    let entries: Vec<SharedStoreWalEntry> = (1..=count as u64)
        .map(|index| {
            let mut entry = template.clone();
            entry.wal_index = index;
            entry
        })
        .collect();
    let reports = replicator
        .publish_wal_entries_batch(&entries)
        .await
        .unwrap_or_else(|err| panic!("batch of {count} x {payload} bytes: {err:?}"));
    assert_eq!(
        reports.len(),
        count,
        "the batch reported on {} entries having been given {count}",
        reports.len()
    );

    // ---- AND THE LAST ONE MUST READ BACK. Its offset is derived from every frame length
    // ---- ahead of it, so an entry recovered at the END of the batch is what says the single
    // ---- append was carved up correctly -- a per-entry check would pass on the first.
    let reader = SharedStoreReplicator::new(cluster, store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
    let last = reader
        .read_wal_entry_by_offset_metadata(SHARD, count as u64)
        .await
        .expect("indexed read of the last entry")
        .expect("the last entry of the batch must exist");
    assert_eq!(
        last.entry.wal_index, count as u64,
        "index {} came back where {count} was asked for",
        last.entry.wal_index
    );
    assert_eq!(
        last.entry.outcomes, template.outcomes,
        "the last entry of a {count}-entry batch did not read back intact"
    );

    let blob_key = format!("{cluster}/shards/{SHARD}/shared/wal/wal.protobuf.blob");
    let objects = store
        .list(&format!("{cluster}/shards/{SHARD}/shared/wal/"))
        .await
        .expect("the wal listing");
    let inner = store.inner();
    let guard = inner.lock().expect("store lock poisoned");
    // ---- THE TWO LEGS SEPARATELY. A figure that moves with the batch size is attributable
    // ---- only if it is known WHICH leg moved: the log frames, or the offset sidecar beside
    // ---- them. Summed into one number it would be a fact without a cause.
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
        blob_bytes += guard
            .get_object(BUCKET, key)
            .expect("every listed object must be counted")
            .metadata
            .extents
            .iter()
            .map(|e| e.length())
            .sum::<u64>();
    }
    (
        (KEY.len() + payload) * count,
        entry_bytes,
        blob_bytes,
        objects.len(),
    )
}

/// Least squares over the sweep, and the largest residual it leaves.
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

/// Sweep the value size under one cluster id and return the fitted line for both legs.
async fn sweep(
    shape: Shape,
    cluster: &str,
    sizes: &[usize],
    options: &StoreOptions,
    print: bool,
) -> (f64, f64, f64) {
    let mut blob_points: Vec<(f64, f64)> = Vec::new();
    let mut entry_points: Vec<(f64, f64)> = Vec::new();

    if print {
        println!("    payload   user B   entry B   sidecar B   blob B   objects   blob/user   replicated/user");
    }
    for &payload in sizes {
        let (user, entry, blob, objects) = one_append(shape, cluster, payload, options).await;
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
            entry > 0 && blob > entry,
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
        println!("    the sidecar is the difference: {:.1} B, at the fitted intercept; offset varints can widen with the payload", blob_env - entry_env);
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

    for shape in [Shape::Command, Shape::Outcome] {
        report(shape, &options).await;
    }
}

async fn report(shape: Shape, options: &StoreOptions) {
    println!("WHAT ONE APPEND COSTS: {shape:?}");
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

    let (blob_rate, blob_env, blob_resid) = sweep(shape, CLUSTER, &sizes, options, true).await;

    println!();
    println!(
        "  SO THE PER-APPEND ENVELOPE IS ABOUT {:.0} BYTES, and the marginal rate is {:.3}x.",
        blob_env, blob_rate
    );
    println!("  A ratio quoted at one payload size is mostly that envelope divided by that size:");
    println!(
        "    at {:>5} B payload the envelope alone is {:>8.2}x",
        sizes[1],
        blob_env / (KEY.len() + sizes[1]) as f64
    );
    println!(
        "    at {:>5} B payload the envelope alone is {:>8.2}x",
        sizes[sizes.len() - 1],
        blob_env / (KEY.len() + sizes[sizes.len() - 1]) as f64
    );
    println!();

    // ---- AND THE ENVELOPE NO LONGER DEPENDS ON THE CLUSTER ID. The sidecar used to write the
    // ---- log's whole object key into every frame, so a longer cluster id made every append more
    // ---- expensive forever. The key is now written only when it is not the derived one, which
    // ---- means this comparison is the test of that: two cluster ids 40 characters apart.
    println!("  THE ENVELOPE IS INDEPENDENT OF THE CLUSTER ID, which it was not when the sidecar");
    println!("  wrote the log's object key into every frame:");
    let (_, long_env, _) = sweep(shape, LONG_CLUSTER, &sizes, options, false).await;
    println!(
        "    cluster id {:>2} chars -> envelope {:.1} B",
        CLUSTER.len(),
        blob_env
    );
    println!(
        "    cluster id {:>2} chars -> envelope {:.1} B",
        LONG_CLUSTER.len(),
        long_env
    );
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
    println!(
        "    replica_count {} with stripe_redundancy_units {} -> {:.1}x of the blob",
        options.replica_count, options.stripe_redundancy_units, options.replica_count as f64
    );
    println!(
        "    replicated/user models copied blob bytes; allocator and disk overhead are excluded"
    );

    // ---- AND THE ENVELOPE IS THE WHOLE OF THE DISTANCE TO THE DESIGN BEING FOLLOWED. Its cost has
    // ---- same shape as ours -- one payload plus a constant -- so the two lines differ only in
    // ---- their intercepts, and the ratio either of them quotes is mostly its own intercept
    // ---- divided by the payload it was quoted at. Both are evaluated here at THEIR payload
    // ---- size, which is the only arrangement under which the two ratios mean the same thing.
    let their_env = THEIR_BYTES_PER_OP - THEIR_PAYLOAD_BYTES;
    assert!(
        THEIR_PAYLOAD_BYTES < THEIR_BYTES_PER_OP,
        "the measured figures say a {THEIR_PAYLOAD_BYTES:.0} B payload costs {THEIR_BYTES_PER_OP:.1} B \
         on disk, which leaves it no envelope at all -- one of the two numbers was misread"
    );
    println!();
    println!("  AGAINST THE DESIGN BEING FOLLOWED, BOTH LINES AT THE SAME PAYLOAD");
    println!();
    println!(
        "    their envelope  {their_env:>6.1} B   upper bound, from {THEIR_BYTES_PER_OP:.1} B/op \
over a {THEIR_PAYLOAD_BYTES:.0} B payload"
    );
    println!("    our envelope    {blob_env:>6.1} B   fitted over the sweep above");
    println!();
    println!("    payload B   ours x   theirs x   ours/theirs");
    for payload in [THEIR_PAYLOAD_BYTES, 1024.0, 8192.0, 65536.0] {
        let ours = blob_rate * payload + blob_env;
        let theirs = payload + their_env;
        println!(
            "    {payload:>9.0}   {:>6.3}   {:>8.3}   {:>11.3}",
            ours / payload,
            theirs / payload,
            ours / theirs
        );
    }
    let matched = (blob_rate * THEIR_PAYLOAD_BYTES + blob_env) / (THEIR_PAYLOAD_BYTES + their_env);
    println!();
    println!(
        "    So at their payload size one append of ours costs {matched:.3}x what theirs was"
    );
    println!(
        "    measured to cost, and the whole of that is {:.1} B of envelope against their \
{their_env:.1} B.",
        blob_env
    );
    println!("    Ours is NOT the cheaper of the two there today. Both marginal rates are one");
    println!("    payload, so the envelope is the only term that can move this at any size, and");
    println!("    the distance closes with the payload rather than with any change to the store.");
    println!();
    println!("    AND THE REASON IS BATCHING, NOT A TIGHTER ENCODING. Their record header is");
    println!("    written once per flush over a batch of items, so their per-item share of it");
    println!("    falls as the batch grows. Ours is charged per ENTRY and stays charged inside a");
    println!("    batch: `publish_wal_entries_batch` concatenates N frames into one append, which");
    println!("    amortizes the durable barrier and not one byte of the envelope. Each of the N");
    println!("    still carries its own frame marker, length varint, CRC32C and record fields,");
    println!("    and its own offset-metadata sidecar beside them.");
    println!();
    println!("    So the term to attack is the per-entry frame, not the fields inside it -- every");
    println!("    field of both legs has already been priced one by one. The entry shape already");
    println!("    admits the fix: it carries a VECTOR of outcome items, so N writes can travel in");
    println!("    one frame under one sidecar, which is the shape their log already has.");

    // ---- AND THAT RATIO IS A CEILING, so handing the envelope's saving back fails here rather
    // ---- than being discovered in a later report. It moves only with our envelope: theirs is
    // ---- two constants above, and both rates are pinned to one payload by the asserts below.
    let matched_ceiling = if matches!(shape, Shape::Command) {
        1.30
    } else {
        1.39
    };
    assert!(
        matched < matched_ceiling,
        "at a {THEIR_PAYLOAD_BYTES:.0} B payload one append costs {matched:.3}x what the design followed \
         was measured to cost, over the {matched_ceiling:.2}x ceiling for {shape:?}. Both marginal \
         rates are one payload, so the whole of this is our {blob_env:.1} B envelope against their \
         {their_env:.1} B -- re-price the log frame and the sidecar field by field before raising \
         this bound"
    );

    // ---- AND HOW MUCH OF THAT ENVELOPE A BATCH CAN RECOVER, MEASURED. The paragraph above
    // ---- says a batch amortizes the barrier and not the envelope. These are the numbers that
    // ---- say it, at a fixed payload, so the only thing varying is how many entries share one
    // ---- append.
    const BATCH_PAYLOAD: usize = 256;
    let batches = [1usize, 2, 4, 16, 64];
    println!();
    println!("  WHAT A BATCH AMORTIZES, AND WHAT IT DOES NOT, at a {BATCH_PAYLOAD} B payload");
    println!();
    println!(
        "    batch   blob B   blob B/entry   frame B/entry   sidecar B/entry   envelope B/entry   objects"
    );
    let mut envelopes: Vec<(usize, f64)> = Vec::new();
    let mut sidecars: Vec<(usize, f64)> = Vec::new();
    for count in batches {
        let (user, entry, blob, objects) =
            batched_appends(shape, CLUSTER, BATCH_PAYLOAD, count, options).await;
        assert_eq!(
            user,
            (KEY.len() + BATCH_PAYLOAD) * count,
            "the user bytes for a {count}-entry batch should be the payload {count} times"
        );
        assert!(
            objects >= 2,
            "a batch of {count} left {objects} objects; the log and its sidecar are two, and a \
             listing that came back short would make every figure here a ratio over the wrong \
             denominator"
        );
        assert!(
            blob >= entry,
            "the listing totals {blob} B against {entry} B in the log alone, so the sidecar is \
             being counted as negative bytes"
        );
        let per_entry = blob as f64 / count as f64;
        let frame_per_entry = entry as f64 / count as f64;
        let sidecar_per_entry = (blob - entry) as f64 / count as f64;
        let envelope = per_entry - (KEY.len() + BATCH_PAYLOAD) as f64;
        println!(
            "    {count:>5}   {blob:>6}   {per_entry:>12.1}   {frame_per_entry:>13.1}   \
{sidecar_per_entry:>15.1}   {envelope:>16.1}   {objects:>7}",
        );
        envelopes.push((count, envelope));
        sidecars.push((count, sidecar_per_entry));
    }
    let (_, envelope_at_one) = envelopes[0];
    let (largest_batch, envelope_at_largest) = envelopes[envelopes.len() - 1];
    let (_, sidecar_at_one) = sidecars[0];
    let (_, sidecar_at_largest) = sidecars[sidecars.len() - 1];
    let recovered = (envelope_at_one - envelope_at_largest) / envelope_at_one;
    println!();
    println!(
        "    A BATCH RECOVERS NONE OF THE ENVELOPE. It costs {:.1}% MORE per entry at \
{largest_batch} than at 1",
        -recovered * 100.0
    );
    println!(
        "    ({envelope_at_one:.1} -> {envelope_at_largest:.1} B/entry), and ALL of the rise is \
the sidecar leg:"
    );
    println!(
        "    {sidecar_at_one:.1} -> {sidecar_at_largest:.1} B/entry, +{:.1}. The log frame per \
entry does not move AT ALL,",
        sidecar_at_largest - sidecar_at_one
    );
    println!("    which is asserted below rather than read off this table.");
    println!();
    println!("    The sidecar is the only leg holding offsets, and it holds three of them -- the");
    println!("    frame's start and end, and the blob's first physical offset. They are varints, a");
    println!("    zero one is omitted by protobuf entirely, and they all grow with the log, so a");
    println!("    later entry's sidecar is wider than an earlier one's. That is where the rise is");
    println!("    and it is the only term in either leg that moves with the batch size; the exact");
    println!("    split between those three fields is not measured here.");
    println!();
    println!("    There are two objects at every batch size, so there was never a per-BATCH");
    println!("    overhead here to divide: what a batch saves is the two durable barriers, which");
    println!("    is TIME, not bytes. Every entry pays its own frame and its own sidecar message");
    println!("    in full however many of them share one append -- which is the whole of the");
    println!("    distance to a design whose record header is written once per flush.");

    // ---- THE LOG FRAME PER ENTRY IS THE PART THAT MUST NOT MOVE, because it is the leg the
    // ---- batch path only concatenates. A frame that grew with the batch size would mean an
    // ---- entry inside a batch is being framed differently from one on its own.
    let frame_at_one = envelope_at_one - sidecar_at_one;
    let frame_at_largest = envelope_at_largest - sidecar_at_largest;
    assert!(
        (frame_at_largest - frame_at_one).abs() < 1.0,
        "the log frame costs {frame_at_largest:.1} B/entry of envelope in a batch of \
         {largest_batch} against {frame_at_one:.1} on its own. The batch path is supposed to \
         CONCATENATE frames, so an entry's frame cannot depend on how many travelled with it"
    );

    // ---- AND THE ENVELOPE MUST NOT BALLOON. A regression that wrote a sidecar per entry AND
    // ---- per batch, or re-published an entry inside its own batch, would show here. The bound
    // ---- is loose enough for the offset varints to reach their widest and no looser.
    assert!(
        envelope_at_largest < envelope_at_one + 8.0,
        "a batch of {largest_batch} costs {envelope_at_largest:.1} B/entry of envelope against \
         {envelope_at_one:.1} on its own. Offset varints widening with the log accounts for a \
         few bytes; this is more than that, so something in the batch path is writing per entry \
         what it should write once"
    );

    // ---- AND IT MUST NOT FALL, which sounds backwards and is the point. As the log is shaped
    // ---- today every entry carries its own frame and its own sidecar message, so batching
    // ---- cannot divide either. A figure that DROPPED here would mean that stopped being true.
    // ----
    // ---- IF THIS FAILS BECAUSE THE FIGURE DROPPED, that is the format change landing -- N
    // ---- entries travelling in one frame under one sidecar. That is the outcome this report
    // ---- argues for, so the right response is to rewrite the paragraph above and move this
    // ---- bound, not to restore the cost.
    assert!(
        recovered < 0.10,
        "a batch of {largest_batch} recovered {:.1}% of the envelope ({envelope_at_one:.1} -> \
         {envelope_at_largest:.1} B/entry). The report says a batch amortizes the barrier and \
         not one byte of the envelope, and that is no longer what the log does",
        recovered * 100.0
    );

    // ---- THE FIT HAS TO BE A FIT. A line quoted over points that do not lie on one is a
    // ---- summary of nothing, so the residual is asserted small against the envelope.
    assert!(
        blob_resid < blob_env.abs().max(1.0),
        "the blob-byte fit leaves a residual of {blob_resid:.1} B against an envelope of \
         {blob_env:.1} B, so `rate * payload + envelope` is not the shape of this cost and the \
         slope above is not a rate. Print the sweep and find the term that is missing"
    );

    assert!(
        blob_rate < 1.01,
        "the append writes more than one payload: {blob_rate:.4}x"
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
    // Measured outcome envelope: 69.8 B; keep enough room for small varint changes.
    assert!(
        blob_env < if matches!(shape, Shape::Command) { 60.0 } else { 74.0 },
        "the per-append envelope is {blob_env:.1} B, over the ceiling for {shape:?}. Five named terms were \
         removed or narrowed to get it here -- a duplicate hex digest the read path never \
         consulted, the log's object key in every frame, the transcription of the frame's own \
         digest, that digest replaced by the CRC32C the check was always for, and a payload \
         length the frame was restating. Re-price the two legs field by field before raising \
         this bound"
    );
}
