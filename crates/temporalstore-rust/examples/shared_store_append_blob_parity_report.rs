use std::sync::Arc;
use std::time::Instant;

use matrixobjectstore_rs::StoreOptions;
use serde::Serialize;
use temporalstore_rust::shared_store::{
    ReplayReport, SharedStoreFlushReport, SharedStoreReplicator, SharedStoreStorageMode,
    SharedStoreWalAppendMode, SharedStoreWalEntry, SharedStoreWriteReport,
};
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine,
};
use temporalstore_snapshot::object_store::AppendBlobReceipt;

const DEFAULT_ENTRY_COUNT: u64 = 8;
const DEFAULT_VALUE_BYTES: usize = 64;

#[derive(Debug, Serialize)]
struct Report {
    schema: &'static str,
    backend: &'static str,
    matrixobject_mode: &'static str,
    entry_count: u64,
    value_bytes: usize,
    direct_publish: PublishPhaseReport,
    sync_writer: WriterPhaseReport,
    async_writer: AsyncWriterPhaseReport,
    summary: Summary,
}

#[derive(Debug, Serialize)]
struct PublishPhaseReport {
    shard_id: u64,
    blob_key: String,
    publish_latencies_us: Vec<u128>,
    append_receipts: Vec<AppendBlobReceipt>,
    blob_object_length: u64,
    blob_physical_extent_count: usize,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct WriterPhaseReport {
    shard_id: u64,
    write_latencies_us: Vec<u128>,
    write_reports: Vec<SharedStoreWriteReport>,
    blob_object_length: u64,
    blob_physical_extent_count: usize,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct AsyncWriterPhaseReport {
    shard_id: u64,
    enqueue_latencies_us: Vec<u128>,
    write_reports: Vec<SharedStoreWriteReport>,
    flush_latency_us: u128,
    flush_report: SharedStoreFlushReport,
    blob_object_length: u64,
    blob_physical_extent_count: usize,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct ReplayPhaseReport {
    replay_latency_us: u128,
    replay_report: ReplayReport,
    retrieval_latencies_us: Vec<u128>,
    retrieved_records: u64,
    retrieval_ok: bool,
}

#[derive(Debug, Serialize)]
struct Summary {
    direct_offsets_monotonic: bool,
    direct_offsets_contiguous: bool,
    sync_reports_include_offsets: bool,
    async_flush_reports_include_offsets: bool,
    replay_recovered_all_records: bool,
    retrieval_recovered_all_records: bool,
    append_latency_avg_us: u128,
    append_latency_p95_us: u128,
    replay_latency_total_us: u128,
    retrieval_latency_avg_us: u128,
}

fn test_engine(root: &std::path::Path, role: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        root.join(format!("{role}-cache")),
        root.join(format!("{role}-pages")),
        root.join(format!("{role}-index")),
    )
}

#[tokio::main]
async fn main() {
    let entry_count = std::env::var("TEMPORALSTORE_APPEND_BLOB_PARITY_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ENTRY_COUNT)
        .max(1);
    let value_bytes = std::env::var("TEMPORALSTORE_APPEND_BLOB_PARITY_VALUE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_VALUE_BYTES)
        .max(1);

    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(
        MatrixObjectObjectStore::new(
            "temporalstore-shared",
            StoreOptions {
                segment_size: 32,
                max_extent_bytes: 8,
                chunk_size: 8,
                ..StoreOptions::default()
            },
        )
        .expect("matrixobject store"),
    );
    let replicator = SharedStoreReplicator::new("cluster-a", store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    let direct_publish = run_direct_publish(
        dir.path(),
        store.clone(),
        &replicator,
        1,
        entry_count,
        value_bytes,
    )
    .await;
    let sync_writer = run_sync_writer(
        dir.path(),
        store.clone(),
        &replicator,
        2,
        entry_count,
        value_bytes,
    )
    .await;
    let async_writer = run_async_writer(
        dir.path(),
        store.clone(),
        &replicator,
        3,
        entry_count,
        value_bytes,
    )
    .await;

    let append_latencies = direct_publish
        .publish_latencies_us
        .iter()
        .chain(sync_writer.write_latencies_us.iter())
        .chain(async_writer.enqueue_latencies_us.iter())
        .copied()
        .collect::<Vec<_>>();
    let retrieval_latencies = direct_publish
        .replay
        .retrieval_latencies_us
        .iter()
        .chain(sync_writer.replay.retrieval_latencies_us.iter())
        .chain(async_writer.replay.retrieval_latencies_us.iter())
        .copied()
        .collect::<Vec<_>>();

    let summary = Summary {
        direct_offsets_monotonic: offsets_monotonic(&direct_publish.append_receipts),
        direct_offsets_contiguous: offsets_contiguous(&direct_publish.append_receipts),
        sync_reports_include_offsets: sync_writer.write_reports.iter().all(|report| {
            report.wal_blob_start_offset.is_some()
                && report.wal_blob_end_offset.is_some()
                && report.wal_blob_object_length.is_some()
        }),
        async_flush_reports_include_offsets: async_writer
            .flush_report
            .last_wal_blob_start_offset
            .is_some()
            && async_writer.flush_report.last_wal_blob_end_offset.is_some()
            && async_writer
                .flush_report
                .last_wal_blob_object_length
                .is_some(),
        replay_recovered_all_records: direct_publish.replay.replay_report.applied
            == entry_count as usize
            && sync_writer.replay.replay_report.applied == entry_count as usize
            && async_writer.replay.replay_report.applied == entry_count as usize,
        retrieval_recovered_all_records: direct_publish.replay.retrieved_records == entry_count
            && sync_writer.replay.retrieved_records == entry_count
            && async_writer.replay.retrieved_records == entry_count,
        append_latency_avg_us: avg(&append_latencies),
        append_latency_p95_us: percentile(&append_latencies, 95),
        replay_latency_total_us: direct_publish.replay.replay_latency_us
            + sync_writer.replay.replay_latency_us
            + async_writer.replay.replay_latency_us,
        retrieval_latency_avg_us: avg(&retrieval_latencies),
    };

    let report = Report {
        schema: "temporalstore_shared_store_append_blob_parity_report_v1",
        backend: "rust",
        matrixobject_mode: "protobuf_append_blob",
        entry_count,
        value_bytes,
        direct_publish,
        sync_writer,
        async_writer,
        summary,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("json report")
    );
}

async fn run_direct_publish(
    root: &std::path::Path,
    store: Arc<MatrixObjectObjectStore>,
    replicator: &SharedStoreReplicator<MatrixObjectObjectStore>,
    shard_id: u64,
    entry_count: u64,
    value_bytes: usize,
) -> PublishPhaseReport {
    let mut append_receipts = Vec::new();
    let mut publish_latencies_us = Vec::new();
    for index in 1..=entry_count {
        let start = Instant::now();
        let receipt = replicator
            .publish_wal_entry(entry(shard_id, index, "direct", value_bytes))
            .await
            .expect("direct publish");
        publish_latencies_us.push(start.elapsed().as_micros());
        append_receipts.push(receipt.expect("protobuf append receipt"));
    }
    let blob_key = blob_key(shard_id);
    let metadata = matrixobject_metadata(&store, &blob_key);
    PublishPhaseReport {
        shard_id,
        blob_key,
        publish_latencies_us,
        append_receipts,
        blob_object_length: metadata.length,
        blob_physical_extent_count: metadata.extents.len(),
        replay: replay_and_retrieve(root, replicator, shard_id, entry_count, "direct").await,
    }
}

async fn run_sync_writer(
    root: &std::path::Path,
    store: Arc<MatrixObjectObjectStore>,
    replicator: &SharedStoreReplicator<MatrixObjectObjectStore>,
    shard_id: u64,
    entry_count: u64,
    value_bytes: usize,
) -> WriterPhaseReport {
    let writer = replicator.storage_writer(SharedStoreStorageMode::Sync, 1);
    let mut write_reports = Vec::new();
    let mut write_latencies_us = Vec::new();
    for index in 1..=entry_count {
        let start = Instant::now();
        let report = writer
            .write(shard_id, command(shard_id, index, "sync", value_bytes))
            .await
            .expect("sync write");
        write_latencies_us.push(start.elapsed().as_micros());
        write_reports.push(report);
    }
    let blob_key = blob_key(shard_id);
    let metadata = matrixobject_metadata(&store, &blob_key);
    WriterPhaseReport {
        shard_id,
        write_latencies_us,
        write_reports,
        blob_object_length: metadata.length,
        blob_physical_extent_count: metadata.extents.len(),
        replay: replay_and_retrieve(root, replicator, shard_id, entry_count, "sync").await,
    }
}

async fn run_async_writer(
    root: &std::path::Path,
    store: Arc<MatrixObjectObjectStore>,
    replicator: &SharedStoreReplicator<MatrixObjectObjectStore>,
    shard_id: u64,
    entry_count: u64,
    value_bytes: usize,
) -> AsyncWriterPhaseReport {
    let writer = replicator.storage_writer(SharedStoreStorageMode::Async, 1);
    let mut write_reports = Vec::new();
    let mut enqueue_latencies_us = Vec::new();
    for index in 1..=entry_count {
        let start = Instant::now();
        let report = writer
            .write(shard_id, command(shard_id, index, "async", value_bytes))
            .await
            .expect("async enqueue");
        enqueue_latencies_us.push(start.elapsed().as_micros());
        write_reports.push(report);
    }
    let flush_start = Instant::now();
    let flush_report = writer
        .flush_pending(entry_count as usize)
        .await
        .expect("async flush");
    let flush_latency_us = flush_start.elapsed().as_micros();
    let blob_key = blob_key(shard_id);
    let metadata = matrixobject_metadata(&store, &blob_key);
    AsyncWriterPhaseReport {
        shard_id,
        enqueue_latencies_us,
        write_reports,
        flush_latency_us,
        flush_report,
        blob_object_length: metadata.length,
        blob_physical_extent_count: metadata.extents.len(),
        replay: replay_and_retrieve(root, replicator, shard_id, entry_count, "async").await,
    }
}

async fn replay_and_retrieve(
    root: &std::path::Path,
    replicator: &SharedStoreReplicator<MatrixObjectObjectStore>,
    shard_id: u64,
    entry_count: u64,
    phase: &str,
) -> ReplayPhaseReport {
    let follower = test_engine(root, &format!("follower-{phase}-{shard_id}"));
    follower.load_shard(shard_id);
    let replay_start = Instant::now();
    let replay_report = replicator
        .replay_wal_strict(shard_id, 0, &follower)
        .await
        .expect("replay");
    let replay_latency_us = replay_start.elapsed().as_micros();

    let mut retrieved_records = 0;
    let mut retrieval_latencies_us = Vec::new();
    for index in 1..=entry_count {
        let key = key_for(shard_id, index, phase);
        let start = Instant::now();
        let response = follower
            .execute(ExecuteRequest {
                shard_id,
                command: Command::StringGet { key },
            })
            .response;
        retrieval_latencies_us.push(start.elapsed().as_micros());
        if matches!(response, CommandResponse::Bytes { value: Some(_) }) {
            retrieved_records += 1;
        }
    }

    ReplayPhaseReport {
        replay_latency_us,
        replay_report,
        retrieval_latencies_us,
        retrieved_records,
        retrieval_ok: retrieved_records == entry_count,
    }
}

fn matrixobject_metadata(
    store: &MatrixObjectObjectStore,
    key: &str,
) -> matrixobjectstore_rs::ObjectMetadata {
    store
        .inner()
        .lock()
        .expect("matrixobject lock poisoned")
        .get_object("temporalstore-shared", key)
        .expect("matrixobject blob")
        .metadata
}

fn entry(shard_id: u64, index: u64, phase: &str, value_bytes: usize) -> SharedStoreWalEntry {
    SharedStoreWalEntry {
        shard_id,
        oplog_index: index,
        command: command(shard_id, index, phase, value_bytes),
    }
}

fn command(shard_id: u64, index: u64, phase: &str, value_bytes: usize) -> Command {
    Command::StringSet {
        key: key_for(shard_id, index, phase),
        value: vec![index as u8; value_bytes],
    }
}

fn key_for(shard_id: u64, index: u64, phase: &str) -> String {
    format!("{phase}-{shard_id}-{index}")
}

fn blob_key(shard_id: u64) -> String {
    format!("cluster-a/shards/{shard_id}/shared/oplog/oplog.protobuf.blob")
}

fn offsets_monotonic(receipts: &[AppendBlobReceipt]) -> bool {
    receipts.windows(2).all(|pair| {
        pair[0].start_offset <= pair[0].end_offset && pair[0].end_offset <= pair[1].start_offset
    })
}

fn offsets_contiguous(receipts: &[AppendBlobReceipt]) -> bool {
    receipts
        .windows(2)
        .all(|pair| pair[0].end_offset == pair[1].start_offset)
}

fn avg(values: &[u128]) -> u128 {
    if values.is_empty() {
        return 0;
    }
    values.iter().sum::<u128>() / values.len() as u128
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}
