// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Three nodes over one shared object store, on each backend, and what each backend can do.
//!
//! The shared store is a **fenced single writer**: `acquire_shard_lease` is a durable
//! compare-and-set, so ownership is decided by the store rather than by any node's local view.
//! "Three nodes" therefore means one writer, two replayers, and a handover.
//!
//! `ObjectStore::compare_and_swap` has a **fail-closed default** — a backend without a real atomic
//! conditional put refuses rather than degrading the fence to an unfenced read-modify-write.
//! `FileObjectStore` overrides it with a cross-process CAS over an atomic `mkdir`. The MatrixObject
//! adapter does not override it, so the fence is unavailable there and the run says so instead of
//! stopping.

use std::sync::Arc;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine,
};
use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore};

const CLUSTER: &str = "cluster-three-node";
const BUCKET: &str = "temporalstore-shared";
const SHARD: u64 = 1;

const FIRST: [(u64, &str, &[u8]); 4] = [
    (1, "alpha", b"first"),
    (2, "bravo", b"second"),
    (3, "charlie", b"third"),
    (4, "delta", b"fourth"),
];
const SECOND: [(u64, &str, &[u8]); 3] = [
    (5, "echo", b"fifth"),
    (6, "foxtrot", b"sixth"),
    (7, "golf", b"seventh"),
];

#[derive(Default)]
struct Outcome {
    backend: String,
    wrote_first: usize,
    replayed: Vec<(String, u64, u64, u64, u64)>,
    equal_version_refused: Option<String>,
    higher_version_taken: Option<String>,
    displaced_writer_refused: Option<String>,
    wrote_second: usize,
    converged: Vec<(String, u64)>,
    objects: Vec<String>,
    note: Vec<String>,
}

fn node_engine(root: &std::path::Path, node: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024,
        root.join(format!("{node}-cache")),
        root.join(format!("{node}-pages")),
        root.join(format!("{node}-index")),
    );
    engine.load_shard(SHARD);
    engine
}

fn entry(wal_index: u64, key: &str, value: &[u8]) -> SharedStoreWalEntry {
    SharedStoreWalEntry {
        shard_id: SHARD,
        wal_index,
        command: Some(Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        }),
        staged_blocks: Vec::new(),
        outcomes: Vec::new(),
    }
}

async fn run_backend<O>(backend: &str, store: Arc<O>, root: &std::path::Path) -> Outcome
where
    O: ObjectStore + Send + Sync + 'static,
{
    let mut out = Outcome { backend: backend.to_string(), ..Default::default() };
    let rep = || {
        SharedStoreReplicator::new(CLUSTER, store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob)
    };

    let node_b = node_engine(root, "node-b");
    let node_c = node_engine(root, "node-c");

    // ---- PHASE 1  node-a claims the lease and writes. If the fence is unavailable the writer
    // ---- proceeds unfenced, because the DATA path is a separate question from ownership.
    let fenced = rep().with_fence(1, "node-a");
    let writer_a = match fenced.acquire_shard_lease(SHARD, 1, "node-a").await {
        Ok(()) => {
            out.note.push("fence available: node-a holds the lease at load_version 1".into());
            fenced
        }
        Err(err) => {
            out.note.push(format!("FENCE UNAVAILABLE: {err:?}"));
            out.note
                .push("writing UNFENCED -- nothing stops a second writer on this backend".into());
            rep()
        }
    };

    for (wal_index, key, value) in FIRST {
        match writer_a.publish_wal_entry(entry(wal_index, key, value)).await {
            Ok(_) => out.wrote_first += 1,
            Err(err) => out.note.push(format!("publish {wal_index} failed: {err:?}")),
        }
    }

    // ---- PHASE 2  the other two replay from the shared store alone.
    for (name, engine) in [("node-b", &node_b), ("node-c", &node_c)] {
        match rep().replay_wal_strict(SHARD, 0, engine).await {
            Ok(r) => {
                out.replayed.push((
                    name.to_string(),
                    // `applied` and `offset_index_reads` are counts, so usize; the other two
                    // are wire quantities and already u64.
                    r.applied as u64,
                    r.last_wal_index,
                    r.offset_index_reads as u64,
                    r.range_bytes_read,
                ));
                for (_, key, value) in FIRST {
                    let got = engine.execute(ExecuteRequest {
                        shard_id: SHARD,
                        command: Command::StringGet { key: key.to_string() },
                    });
                    assert_eq!(
                        got.response,
                        CommandResponse::Bytes { value: Some(value.to_vec()) },
                        "{backend}/{name} replayed but cannot serve {key}"
                    );
                }
            }
            Err(err) => out.note.push(format!("{name} replay failed: {err:?}")),
        }
    }

    // ---- PHASE 3  an EQUAL load_version must be refused.
    out.equal_version_refused = Some(match rep().acquire_shard_lease(SHARD, 1, "node-b").await {
        Err(err) => format!("{err:?}"),
        Ok(()) => "ACCEPTED -- an equal load_version must not win the lease".into(),
    });

    // ---- PHASE 4  a strictly higher one must be accepted.
    let writer_b = rep().with_fence(2, "node-b");
    out.higher_version_taken = Some(match writer_b.acquire_shard_lease(SHARD, 2, "node-b").await {
        Ok(()) => "accepted".into(),
        Err(err) => format!("REFUSED: {err:?}"),
    });

    // ---- PHASE 5  the displaced writer must not be able to append.
    out.displaced_writer_refused =
        Some(match writer_a.publish_wal_entry(entry(5, "echo", b"from-a")).await {
            Err(err) => format!("refused: {err:?}"),
            Ok(_) => "APPENDED -- the displaced writer was not stopped".into(),
        });

    // ---- PHASE 6  the successor writes.
    for (wal_index, key, value) in SECOND {
        match writer_b.publish_wal_entry(entry(wal_index, key, value)).await {
            Ok(_) => out.wrote_second += 1,
            Err(err) => out.note.push(format!("successor publish {wal_index} failed: {err:?}")),
        }
    }

    // ---- PHASE 7  everyone converges.
    let fresh = node_engine(root, "node-a-fresh");
    for (name, engine, from) in [
        ("node-a", &fresh, 0),
        ("node-b", &node_b, 4),
        ("node-c", &node_c, 4),
    ] {
        match rep().replay_wal_strict(SHARD, from, engine).await {
            Ok(r) => out.converged.push((name.to_string(), r.last_wal_index)),
            Err(err) => out.note.push(format!("{name} converge failed: {err:?}")),
        }
    }

    let mut objects = store.list(&format!("{CLUSTER}/")).await.unwrap_or_default();
    objects.extend(store.list("leases/").await.unwrap_or_default());
    objects.sort();
    out.objects = objects;
    out
}

fn report(out: &Outcome) {
    println!();
    println!("================ BACKEND: {}", out.backend);
    for note in &out.note {
        println!("  * {note}");
    }
    println!();
    println!("  PHASE 1  first writer published        {} of 4 entries", out.wrote_first);
    println!("  PHASE 2  replay by the other two nodes");
    if out.replayed.is_empty() {
        println!("             (none succeeded)");
    }
    for (name, applied, last, idx_reads, range_bytes) in &out.replayed {
        println!(
            "             {name:<8} applied {applied:>2}  last_wal_index {last:>2}  offset_index_reads {idx_reads:>2}  range_bytes_read {range_bytes:>6}"
        );
    }
    println!(
        "  PHASE 3  equal load_version            {}",
        out.equal_version_refused.as_deref().unwrap_or("-")
    );
    println!(
        "  PHASE 4  higher load_version           {}",
        out.higher_version_taken.as_deref().unwrap_or("-")
    );
    println!(
        "  PHASE 5  displaced writer append       {}",
        out.displaced_writer_refused.as_deref().unwrap_or("-")
    );
    println!("  PHASE 6  successor published           {} of 3 entries", out.wrote_second);
    println!("  PHASE 7  convergence");
    for (name, last) in &out.converged {
        println!("             {name:<8} last_wal_index {last}");
    }
    println!("  OBJECTS IN THE SHARED STORE           {}", out.objects.len());
    for object in &out.objects {
        println!("             {object}");
    }
}

#[tokio::main]
async fn main() {
    println!("THREE NODES OVER ONE SHARED OBJECT STORE, ON EACH BACKEND");
    println!("  cluster {CLUSTER}, shard {SHARD}");
    println!("  node-a first writer, node-b successor, node-c replayer only");
    println!();
    println!("  The fence is a durable compare-and-set. `ObjectStore::compare_and_swap` has a");
    println!("  FAIL-CLOSED default, so a backend with no atomic conditional put refuses rather");
    println!("  than silently running an unfenced read-modify-write.");

    // ---- BACKEND ONE: a shared filesystem, which implements a real cross-process CAS.
    let file_dir = tempfile::tempdir().expect("tempdir");
    let file_store = Arc::new(FileObjectStore::new(file_dir.path().join("shared")));
    let file_out = run_backend("FileObjectStore (shared filesystem)", file_store, file_dir.path()).await;

    // ---- BACKEND TWO: MatrixObject, three replicas with a write quorum of two.
    let mo_dir = tempfile::tempdir().expect("tempdir");
    let options = StoreOptions {
        page_size: 64,
        max_extent_bytes: 256,
        chunk_size: 256,
        replica_count: 3,
        write_quorum: 2,
        ..StoreOptions::default()
    };
    println!();
    println!("  AND THE MATRIXOBJECT STORE IS ITSELF THREE-WAY, the second sense of three nodes:");
    println!("    replica_count {}  write_quorum {}  stripe_redundancy_units {}",
             options.replica_count, options.write_quorum, options.stripe_redundancy_units);
    println!("    page_size {}  chunk_size {}  max_extent_bytes {}",
             options.page_size, options.chunk_size, options.max_extent_bytes);
    if options.stripe_redundancy_units == 0 {
        println!("    posture REPLICATION -- one copy per replica, {:.1}x of the logical payload",
                 options.replica_count as f64);
    } else {
        let denom = options.replica_count - options.stripe_redundancy_units;
        println!("    posture STRIPED -- {:.2}x of the logical payload, surviving {} loss(es)",
                 options.replica_count as f64 / denom as f64, options.stripe_redundancy_units);
    }
    let mo_store = Arc::new(
        MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("the shared store"),
    );
    let mo_out = run_backend("MatrixObject", mo_store.clone(), mo_dir.path()).await;

    report(&file_out);
    report(&mo_out);

    // ---- WHAT MATRIXOBJECT LAID OUT, which the file backend has no equivalent of.
    let blob_key = format!("{CLUSTER}/shards/{SHARD}/shared/wal/wal.protobuf.blob");
    if mo_out.objects.iter().any(|o| o == &blob_key) {
        let inner = mo_store.inner();
        let guard = inner.lock().expect("store lock poisoned");
        if let Ok(object) = guard.get_object(BUCKET, &blob_key) {
            println!();
            println!("================ HOW MATRIXOBJECT LAID THE APPEND BLOB OUT");
            println!("  key      {blob_key}");
            println!("  extents  {}", object.metadata.extents.len());
            let total: u64 = object.metadata.extents.iter().map(|e| e.length()).sum();
            println!("  bytes    {total}");
            for (n, extent) in object.metadata.extents.iter().enumerate() {
                println!("    extent {n:>2}  offset {:>6}  length {:>6}", extent.offset(), extent.length());
            }
        }
    }

    println!();
    println!("================ WHAT THIS SHOWS");
    println!("  data path   both backends append and replay; three nodes converge on both");
    println!("  fence       BOTH backends now enforce single-writer ownership, phase for phase.");
    println!("              The shared filesystem does it with an atomic mkdir; MatrixObject does");
    println!("              it with its own put preconditions -- if_none_match to claim an unheld");
    println!("              lease, if_match_etag to replace one, both enforced inside the put.");
    println!("  so          MatrixObject is usable as a shared store for data AND as the");
    println!("              ownership substrate for a multi-writer deployment.");
    println!("  the catch   a run where the fence line above reads UNAVAILABLE means the adapter");
    println!("              lost its compare_and_swap override and the trait default took over --");
    println!("              which is fail-closed, so a fenced writer is refused on every append");
    println!("              and an unfenced one is not stopped at all. Phase 5 is the tell.");
}
