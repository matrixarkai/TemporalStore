// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A shared-store WAL and its offset index, end to end, through a gateway and directly, on one
//! real MatrixObject cluster.
//!
//! One WAL segment is two objects, a log and an offset index. This writes a shard's WAL through
//! the shared store's own appender (so both objects are appended, as in a deployment), rotates
//! through several segments, replays from the cluster alone with a fresh connection, trims old
//! segments, and appends concurrently through one store handle. It does the same through a
//! gateway and through the in-process door, in two key prefixes of the same bucket, and then
//! compares what each left on the cluster.
//!
//! It speaks only the `ObjectStore` surface and the shared store's public API, so it does not
//! depend on how the cluster stores object metadata.
//!
//! ```text
//! GATEWAY=matrixobject://127.0.0.1:24410 META=127.0.0.1:24404,127.0.0.1:24405,127.0.0.1:24406 \
//!   TS_MATRIXOBJECT_META_GROUP=1 SEGMENTS=6 WRITERS=4 \
//!   cargo run --release -p temporalstore-rust --example direct_wal_index_e2e
//! ```
//!
//! Every check prints one `CHECK name ok|FAIL detail` line, and the run exits non-zero on the
//! first failure. Timings are printed and never asserted.
//!
//! Not covered here: a checkpoint (it needs a block store), and killing a server mid-append.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use temporalstore_rust::matrixobject_direct::DirectConfig;
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};
use temporalstore_snapshot::object_store::{
    MatrixObjectMode, NetworkedMatrixObject, ObjectStore,
};

const SHARD: u64 = 1;
const PER_SEGMENT: usize = 40;
const VALUE_BYTES: usize = 200;

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn check(name: &str, ok: bool, detail: impl std::fmt::Display) {
    if ok {
        println!("CHECK {name} ok");
    } else {
        println!("CHECK {name} FAIL {detail}");
        std::process::exit(1);
    }
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn connect(mode: MatrixObjectMode) -> NetworkedMatrixObject {
    match mode {
        MatrixObjectMode::Gateway => {
            let uri = std::env::var("GATEWAY").expect("GATEWAY: the gateway URI");
            NetworkedMatrixObject::connect(mode, &uri).expect("connect to the gateway")
        }
        MatrixObjectMode::Direct => {
            let meta = std::env::var("META").expect("META: the metadata endpoints");
            DirectConfig::from_env(&meta)
                .and_then(|config| config.connect())
                .expect("connect directly")
        }
    }
}

fn engine(root: &std::path::Path, name: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        root.join(format!("{name}-cache")),
        root.join(format!("{name}-blocks")),
        root.join(format!("{name}-index")),
    );
    engine.load_shard(SHARD);
    engine
}

fn key_of(index: u64) -> String {
    format!("e2e-key-{index:08}")
}

fn value_of(index: u64) -> Vec<u8> {
    vec![(index % 251) as u8; VALUE_BYTES]
}

fn replicator(
    store: &Arc<NetworkedMatrixObject>,
    cluster: &str,
) -> SharedStoreReplicator<NetworkedMatrixObject> {
    SharedStoreReplicator::new(cluster.to_string(), store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob)
        .with_wal_segment_appends(PER_SEGMENT)
}

/// What a run leaves behind: every object under the cluster prefix, keyed without the prefix.
async fn snapshot(
    store: &NetworkedMatrixObject,
    cluster: &str,
) -> BTreeMap<String, Vec<u8>> {
    let prefix = format!("{cluster}/");
    let mut objects = BTreeMap::new();
    for key in store.list(&prefix).await.expect("list the run's objects") {
        let bytes = store.get(&key).await.expect("read an object back");
        objects.insert(key[prefix.len()..].to_string(), bytes.to_vec());
    }
    objects
}

struct Run {
    snapshot: BTreeMap<String, Vec<u8>>,
    append_micros: Vec<u128>,
}

async fn one_mode(mode: MatrixObjectMode, segments: usize, writers: usize) -> Run {
    let tag = format!("{mode:?}").to_lowercase();
    let cluster = format!("e2e-{}-{tag}", std::process::id());
    let entries = (segments * PER_SEGMENT) as u64;
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(connect(mode));
    check(&format!("{tag}: connected in the named mode"), store.mode() == mode, format!("{:?}", store.mode()));

    // ---- WRITE: one fenced writer, the shared store's own appender, rotating through segments.
    // The networked store refuses compare_and_swap in BOTH modes (the lease needs it), so this
    // example writes unfenced and reports the refusal as a finding rather than aborting.
    let writer = replicator(&store, &cluster);
    let lease = replicator(&store, &cluster).acquire_shard_lease(SHARD, 1, "e2e-writer").await;
    println!("FINDING {tag}: shard lease over the networked store => {}", match &lease { Ok(_) => "ok".to_string(), Err(e) => format!("refused: {e:?}") });
    let mut append_micros = Vec::with_capacity(entries as usize);
    for index in 1..=entries {
        let started = Instant::now();
        writer
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: SHARD,
                wal_index: index,
                command: Some(Command::StringSet { key: key_of(index), value: value_of(index) }),
                staged_blocks: Vec::new(),
                outcomes: Vec::new(),
            })
            .await
            .unwrap_or_else(|err| panic!("{tag}: append {index}: {err:?}"));
        append_micros.push(started.elapsed().as_micros());
    }

    // ---- ROTATION: every segment has BOTH its log object and its index object.
    let segments_found = writer.wal_segments(SHARD).await.expect("list segments");
    check(
        &format!("{tag}: {segments} segments, each a log and an index"),
        segments_found.len() == segments && segments_found.iter().all(|s| s.has_log && s.has_index),
        format!("{segments_found:?}"),
    );

    // ---- RESTART AND REPLAY: a fresh connection and an empty engine, from the cluster alone.
    drop(writer);
    let reopened = Arc::new(connect(mode));
    let reader = replicator(&reopened, &cluster);
    let fresh = engine(dir.path(), "replayer");
    let report = reader.replay_wal_strict(SHARD, 0, &fresh).await.expect("replay");
    check(
        &format!("{tag}: replay applied every entry"),
        report.applied as u64 == entries && report.last_wal_index == entries,
        format!("applied {} last {}", report.applied, report.last_wal_index),
    );
    for index in [1, entries / 2, entries] {
        let got = fresh
            .execute(ExecuteRequest { shard_id: SHARD, command: Command::StringGet { key: key_of(index) } })
            .response;
        check(
            &format!("{tag}: replayed entry {index} is served"),
            got == CommandResponse::Bytes { value: Some(value_of(index)) },
            format!("{got:?}"),
        );
    }

    // ---- TRIM: segments wholly before the retained index go; the rest still replay.
    let retain_from = (PER_SEGMENT * 2 + 1) as u64;
    let gc = reader.gc_wal_before(SHARD, retain_from).await.expect("trim");
    let after = reader.wal_segments(SHARD).await.expect("list segments after trim");
    check(
        &format!("{tag}: trim removed the old segments and kept the retained one"),
        gc.deleted_wal_objects > 0
            && after.len() < segments
            && after.iter().any(|s| s.first_wal_index <= retain_from)
            && after.iter().all(|s| s.has_log == s.has_index),
        format!("deleted {} left {after:?}", gc.deleted_wal_objects),
    );
    let tail = engine(dir.path(), "tail");
    let tail_report = reader.replay_wal_strict(SHARD, retain_from - 1, &tail).await.expect("replay the tail");
    check(
        &format!("{tag}: the tail still replays after the trim"),
        tail_report.last_wal_index == entries && tail_report.applied as u64 == entries - retain_from + 1,
        format!("{tail_report:?}"),
    );

    // ---- CONCURRENT APPENDS through one store handle (one door in direct mode): several tasks
    // ---- append to ONE object. Every acknowledged receipt must be a distinct, adjacent range
    // ---- holding exactly that record; an error is reported, not a panic, so the object can be
    // ---- read back and compared with the receipts (lost? duplicated? or only errors returned?).
    // ---- IDENTICAL=1 gives every record the same bytes (the case a content-equality settle cannot
    // ---- tell apart): then the object must hold exactly as many records as were acknowledged.
    let identical = env("IDENTICAL", "0") == "1";
    let shared_key = format!("{cluster}/concurrent/blob");
    let per_writer = 25usize;
    let mut tasks = Vec::new();
    for writer_id in 0..writers {
        let store = reopened.clone();
        let key = shared_key.clone();
        tasks.push(tokio::spawn(async move {
            let mut acked: Vec<(u64, u64, Bytes)> = Vec::new();
            let mut errors: Vec<String> = Vec::new();
            for n in 0..per_writer {
                let record = if identical {
                    Bytes::from_static(b"[same-payload]")
                } else {
                    Bytes::from(format!("[w{writer_id:02}n{n:03}]").into_bytes())
                };
                match store.append_blob(&key, record.clone()).await {
                    Ok(receipt) => acked.push((receipt.start_offset, receipt.end_offset, record)),
                    Err(error) => errors.push(error.to_string()),
                }
            }
            (acked, errors)
        }));
    }
    let mut all: Vec<(u64, u64, Bytes)> = Vec::new();
    let mut errors: Vec<String> = Vec::new();
    for task in tasks {
        let (acked, errs) = task.await.expect("writer task");
        all.extend(acked);
        errors.extend(errs);
    }
    all.sort_by_key(|(start, _, _)| *start);
    let object = reopened.get(&shared_key).await.expect("read the concurrent object").to_vec();
    let contiguous = all.windows(2).all(|pair| pair[0].1 == pair[1].0)
        && all.first().map(|r| r.0) == Some(0);
    let overlapping = all.windows(2).filter(|pair| pair[0].1 > pair[1].0).count();
    let wrong_bytes = all
        .iter()
        .filter(|(start, end, record)| {
            object.get(*start as usize..*end as usize).map(|found| found != record.as_ref()) != Some(false)
        })
        .count();
    let record_len = all.first().map(|r| r.2.len()).unwrap_or(1);
    let object_records = object.len() / record_len.max(1);
    let first_error = errors.first().map(|e| e.chars().take(160).collect::<String>()).unwrap_or_default();
    println!(
        "ANALYSIS {tag}: identical={identical} writers={writers} appends={} acknowledged={} errors={} object_bytes={} object_records={} acknowledged_bytes={} overlapping_receipts={overlapping} receipts_not_matching_object={wrong_bytes} first_error={first_error:?}",
        writers * per_writer,
        all.len(),
        errors.len(),
        object.len(),
        object_records,
        all.iter().map(|r| r.2.len()).sum::<usize>(),
    );
    check(
        &format!("{tag}: {writers} concurrent writers, {} acknowledged records, adjacent ranges, each stored once", all.len()),
        errors.is_empty()
            && all.len() == writers * per_writer
            && contiguous
            && overlapping == 0
            && wrong_bytes == 0
            && object_records == all.len()
            && object.len() == all.iter().map(|r| r.2.len()).sum::<usize>(),
        format!("see the ANALYSIS line: errors {}", errors.len()),
    );

    // ---- Snapshot what is left, WAL objects only (the concurrent blob's order is a race).
    let mut left = snapshot(&reopened, &cluster).await;
    left.retain(|key, _| key.contains("/wal/"));
    Run { snapshot: left, append_micros }
}

#[tokio::main]
async fn main() {
    let segments: usize = env("SEGMENTS", "6").parse().expect("SEGMENTS");
    let writers: usize = env("WRITERS", "4").parse().expect("WRITERS");
    assert!(segments >= 4, "SEGMENTS must be at least 4 so a trim leaves something to replay");

    // MODES=gateway or MODES=direct runs one side only (to isolate a failure); the default runs both.
    let modes = env("MODES", "gateway,direct");
    if modes != "gateway,direct" {
        let mode = if modes == "gateway" { MatrixObjectMode::Gateway } else { MatrixObjectMode::Direct };
        one_mode(mode, segments, writers).await;
        println!("RESULT ok (one mode only: {modes})");
        return;
    }
    let gateway = one_mode(MatrixObjectMode::Gateway, segments, writers).await;
    let direct = one_mode(MatrixObjectMode::Direct, segments, writers).await;

    // ---- BYTE IDENTITY: the same writes leave the same WAL objects, whichever way they came in.
    let keys = |run: &Run| run.snapshot.keys().cloned().collect::<Vec<_>>();
    check(
        "gateway and direct left the same WAL object names",
        keys(&gateway) == keys(&direct),
        format!("gateway {:?} direct {:?}", keys(&gateway), keys(&direct)),
    );
    for (name, bytes) in &gateway.snapshot {
        let other = &direct.snapshot[name];
        let first_difference = bytes.iter().zip(other.iter()).position(|(a, b)| a != b);
        check(
            &format!("object {name} is byte-identical ({} bytes)", bytes.len()),
            bytes == other,
            format!("gateway {} bytes, direct {} bytes, first difference at {first_difference:?}", bytes.len(), other.len()),
        );
    }

    for (label, run) in [("gateway", &gateway), ("direct", &direct)] {
        let mut micros = run.append_micros.clone();
        micros.sort_unstable();
        println!(
            "LATENCY wal_append mode={label} n={} p50_us={} p99_us={} max_us={}",
            micros.len(),
            percentile(&micros, 0.50),
            percentile(&micros, 0.99),
            micros.last().copied().unwrap_or(0)
        );
    }
    println!("RESULT ok");
}
