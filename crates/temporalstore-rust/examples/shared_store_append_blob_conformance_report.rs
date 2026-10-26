// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;
use std::time::Instant;

use matrixobjectstore_rs::StoreOptions;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use temporalstore_rust::shared_store::{
    ReplayReport, SharedStoreFlushReport, SharedStoreReplicator, SharedStoreStorageMode,
    SharedStoreWalAppendMode, SharedStoreWalEntry, SharedStoreWalOffsetMetadata,
    SharedStoreWriteReport,
};
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine,
};
use temporalstore_snapshot::object_store::AppendBlobReceipt;

const DEFAULT_ENTRY_COUNT: u64 = 8;
const DEFAULT_VALUE_BYTES: usize = 64;
const PERSISTENT_JOURNAL_FILE: &str = "matrixobjectstore-incremental.journal";

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
    journal_reopen: JournalReopenReport,
    summary: Summary,
}

/// This report's OWN reader of the log frame, independent of the one in the crate.
///
/// Being independent is the point: it checks that a frame on the wire says what the writer meant,
/// without borrowing the writer's own decoder to do it. The cost of that independence is that it
/// has to be kept in step with the frame by hand, and nothing makes it fail when it is not --
/// `all_offsets_decode_expected_frame` is a REPORTED FIELD, so a reader that understands none of
/// the frames prints `false` and exits zero.
///
/// That is not hypothetical. When the log frame's digest moved from tag 5 to tag 9, this reader
/// still looked only at tag 5: it matched 0 of 8 frames, reported `false`, and the program exited
/// successfully. Hence the digest dispatch below AND the assertion in `main` -- the dispatch keeps
/// it correct, the assertion is what makes it fail next time instead of whispering.
#[derive(Clone, PartialEq, Message)]
struct ReportWalFrameProto {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    wal_index: u64,
    #[prost(bytes = "vec", tag = "3")]
    command_payload: Vec<u8>,
    #[prost(uint64, tag = "4")]
    command_byte_size: u64,
    /// The digest as hex. Populated by frames declaring the hex format; empty in a narrow frame.
    #[prost(string, tag = "5")]
    command_sha256: String,
    #[prost(uint32, tag = "6")]
    command_encoding: u32,
    /// The digest as its own 32 bytes, which is what a narrow frame carries instead of tag 5.
    #[prost(bytes = "vec", tag = "9")]
    command_digest: Vec<u8>,
    /// Which digest shape this frame carries. Absent is zero is the hex shape, so a frame written
    /// before the field existed reads correctly without a special case.
    #[prost(uint32, tag = "10")]
    frame_format: u32,
    /// A CRC32C of the payload, which is what the narrowest frame carries instead of either.
    #[prost(fixed32, tag = "11")]
    command_crc32c: u32,
}

/// The digest shapes this reader understands, by the value the frame declares.
///
/// Every format the ENCODER can produce is listed here, including ones it does not yet write by
/// default. That is deliberate: the last time this reader knew fewer formats than the writer could
/// emit, it matched 0 of 8 frames and said so in a field nobody was asserting. Teaching it a
/// format early costs one match arm; learning about one late costs a silent report.
const REPORT_FRAME_FORMAT_HEX_DIGEST: u32 = 0;
const REPORT_FRAME_FORMAT_RAW_DIGEST: u32 = 2;
const REPORT_FRAME_FORMAT_CRC32C: u32 = 3;
const REPORT_FRAME_FORMAT_NO_SIZE: u32 = 4;

/// The crate's CRC32C, not a second copy of it.
///
/// This reader is independent of the writer about the FRAME LAYOUT, which is what it exists to
/// check. It is deliberately not independent about the checksum algorithm: a second
/// implementation of CRC32C here could drift from the one the store actually uses, and then this
/// report would be checking frames against a checksum nothing writes.
fn crc32c_of(bytes: &[u8]) -> u32 {
    temporalstore_rust::checksum::crc32c(bytes)
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

#[derive(Debug, Serialize)]
struct OffsetFrameValidation {
    checked_frames: usize,
    matched_frames: usize,
    all_offsets_decode_expected_frame: bool,
    contiguous_coverage_bytes: u64,
}

#[derive(Debug, Serialize)]
struct OffsetIndexValidation {
    checked_entries: usize,
    matched_entries: usize,
    all_wal_indexes_have_offset_metadata: bool,
    all_metadata_ranges_match_append_receipts: bool,
}

#[derive(Debug, Serialize)]
struct OffsetMetadataMapping {
    shard_id: u64,
    wal_index: u64,
    blob_key: String,
    start_offset: u64,
    end_offset: u64,
    bytes_written: u64,
    command_encoding: u32,
    object_length: u64,
    physical_slab_count: u64,
    first_physical_offset: Option<u64>,
    expected_start_offset: Option<u64>,
    expected_end_offset: Option<u64>,
    expected_bytes_written: Option<u64>,
    matches_expected_offsets: bool,
}

#[derive(Debug, Serialize)]
struct AuthoritativeOffsetLookupReport {
    checked_entries: u64,
    metadata_hits: u64,
    decoded_entries: u64,
    matched_entries: u64,
    range_bytes_read: u64,
    exact_offset_slice_reads: u64,
    range_reads_smaller_than_blob: u64,
    slab_metadata_entries: u64,
    first_physical_offset_entries: u64,
    all_wal_indexes_directly_read_from_offset_metadata: bool,
    all_direct_reads_match_expected_entries: bool,
    all_direct_reads_have_slab_metadata: bool,
    lower_layer_range_reads_exact_offset_slices: bool,
    lower_layer_range_reads_avoid_full_blob_scans: bool,
    lower_layer_blob_offset_reads_proven: bool,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct PublishPhaseReport {
    shard_id: u64,
    blob_key: String,
    publish_latencies_us: Vec<u128>,
    append_receipts: Vec<AppendBlobReceipt>,
    offset_metadata_mappings: Vec<OffsetMetadataMapping>,
    blob_object_length: u64,
    blob_physical_slab_count: usize,
    offset_frame_validation: OffsetFrameValidation,
    offset_index_validation: OffsetIndexValidation,
    authoritative_offset_lookup: AuthoritativeOffsetLookupReport,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct WriterPhaseReport {
    shard_id: u64,
    write_latencies_us: Vec<u128>,
    write_reports: Vec<SharedStoreWriteReport>,
    offset_metadata_mappings: Vec<OffsetMetadataMapping>,
    blob_object_length: u64,
    blob_physical_slab_count: usize,
    authoritative_offset_lookup: AuthoritativeOffsetLookupReport,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct AsyncWriterPhaseReport {
    shard_id: u64,
    enqueue_latencies_us: Vec<u128>,
    write_reports: Vec<SharedStoreWriteReport>,
    flush_latency_us: u128,
    flush_report: SharedStoreFlushReport,
    offset_metadata_mappings: Vec<OffsetMetadataMapping>,
    blob_object_length: u64,
    blob_physical_slab_count: usize,
    authoritative_offset_lookup: AuthoritativeOffsetLookupReport,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct MatrixObjectCacheStatsReport {
    entries: u64,
    bytes: u64,
    max_bytes: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    direct_fill_blocks: u64,
    direct_fill_bytes: u64,
    compressed_fill_blocks: u64,
    compressed_fill_bytes: u64,
    ec_fill_blocks: u64,
    ec_fill_bytes: u64,
    pressure: bool,
}

#[derive(Debug, Serialize)]
struct JournalReopenReport {
    snapshot_export_latency_us: u128,
    snapshot_disk_write_latency_us: u128,
    snapshot_disk_read_latency_us: u128,
    snapshot_import_latency_us: u128,
    snapshot_bytes: u64,
    snapshot_sha256: String,
    reopened_offset_metadata_ok: bool,
    cache_before_replay: MatrixObjectCacheStatsReport,
    cache_after_retrieval: MatrixObjectCacheStatsReport,
    cache_metrics_available: bool,
    reopened_replay_recovered_all_records: bool,
    reopened_retrieval_recovered_all_records: bool,
    replay_latency_total_us: u128,
    retrieval_latency_avg_us: u128,
}

#[derive(Debug, Serialize)]
struct ReplayPhaseReport {
    replay_latency_us: u128,
    replay_report: ReplayReport,
    retrieval_latencies_us: Vec<u128>,
    retrieved_records: u64,
    retrieval_ok: bool,
    secondary_replay_latency_us: u128,
    secondary_replay_report: ReplayReport,
    secondary_retrieved_records: u64,
    secondary_retrieval_ok: bool,
    single_node_reload_latency_us: u128,
    single_node_reload_report: ReplayReport,
    single_node_reload_retrieved_records: u64,
    single_node_reload_ok: bool,
}

#[derive(Debug, Serialize)]
struct Summary {
    direct_offsets_monotonic: bool,
    direct_offsets_contiguous: bool,
    direct_offset_slices_decode_expected_frames: bool,
    direct_offset_index_maps_wal_to_blob_offsets: bool,
    authoritative_offset_lookup_reads_all_records: bool,
    authoritative_offset_lookup_matches_all_records: bool,
    authoritative_offset_lookup_has_slab_metadata: bool,
    lower_layer_blob_offset_reads_proven: bool,
    lower_layer_range_reads_avoid_full_blob_scans: bool,
    snapshot_reopen_restores_offset_metadata: bool,
    snapshot_reopen_recovered_all_records: bool,
    snapshot_reopen_cache_metrics_available: bool,
    secondary_replay_recovered_all_records: bool,
    single_node_reload_recovered_all_records: bool,
    replay_uses_offset_index_metadata: bool,
    secondary_replay_uses_offset_index_metadata: bool,
    single_node_reload_uses_offset_index_metadata: bool,
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
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_ENTRY_COUNT)
        .max(1);
    let value_bytes = std::env::var("TEMPORALSTORE_APPEND_BLOB_PARITY_VALUE_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_VALUE_BYTES)
        .max(1);

    let dir = tempfile::tempdir().expect("tempdir");
    let persistent_journal_path = dir.path().join(PERSISTENT_JOURNAL_FILE);
    let store = Arc::new(
        MatrixObjectObjectStore::new(
            "temporalstore-shared",
            matrixobject_options(Some(&persistent_journal_path)),
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
    let journal_reopen = run_journal_reopen(
        dir.path(),
        persistent_journal_path,
        1,
        entry_count,
        &["direct", "sync", "async"],
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
        direct_offset_slices_decode_expected_frames: direct_publish
            .offset_frame_validation
            .all_offsets_decode_expected_frame,
        direct_offset_index_maps_wal_to_blob_offsets: direct_publish
            .offset_index_validation
            .all_wal_indexes_have_offset_metadata
            && direct_publish
                .offset_index_validation
                .all_metadata_ranges_match_append_receipts,
        authoritative_offset_lookup_reads_all_records: direct_publish
            .authoritative_offset_lookup
            .all_wal_indexes_directly_read_from_offset_metadata
            && sync_writer
                .authoritative_offset_lookup
                .all_wal_indexes_directly_read_from_offset_metadata
            && async_writer
                .authoritative_offset_lookup
                .all_wal_indexes_directly_read_from_offset_metadata,
        authoritative_offset_lookup_matches_all_records: direct_publish
            .authoritative_offset_lookup
            .all_direct_reads_match_expected_entries
            && sync_writer
                .authoritative_offset_lookup
                .all_direct_reads_match_expected_entries
            && async_writer
                .authoritative_offset_lookup
                .all_direct_reads_match_expected_entries,
        authoritative_offset_lookup_has_slab_metadata: direct_publish
            .authoritative_offset_lookup
            .all_direct_reads_have_slab_metadata
            && sync_writer
                .authoritative_offset_lookup
                .all_direct_reads_have_slab_metadata
            && async_writer
                .authoritative_offset_lookup
                .all_direct_reads_have_slab_metadata,
        lower_layer_blob_offset_reads_proven: direct_publish
            .authoritative_offset_lookup
            .lower_layer_blob_offset_reads_proven
            && sync_writer
                .authoritative_offset_lookup
                .lower_layer_blob_offset_reads_proven
            && async_writer
                .authoritative_offset_lookup
                .lower_layer_blob_offset_reads_proven,
        lower_layer_range_reads_avoid_full_blob_scans: direct_publish
            .authoritative_offset_lookup
            .lower_layer_range_reads_avoid_full_blob_scans
            && sync_writer
                .authoritative_offset_lookup
                .lower_layer_range_reads_avoid_full_blob_scans
            && async_writer
                .authoritative_offset_lookup
                .lower_layer_range_reads_avoid_full_blob_scans,
        snapshot_reopen_restores_offset_metadata: journal_reopen.reopened_offset_metadata_ok,
        snapshot_reopen_recovered_all_records: journal_reopen.reopened_replay_recovered_all_records
            && journal_reopen.reopened_retrieval_recovered_all_records,
        snapshot_reopen_cache_metrics_available: journal_reopen.cache_metrics_available,
        secondary_replay_recovered_all_records: direct_publish.replay.secondary_retrieved_records
            == entry_count
            && sync_writer.replay.secondary_retrieved_records == entry_count
            && async_writer.replay.secondary_retrieved_records == entry_count,
        single_node_reload_recovered_all_records: direct_publish
            .replay
            .single_node_reload_retrieved_records
            == entry_count
            && sync_writer.replay.single_node_reload_retrieved_records == entry_count
            && async_writer.replay.single_node_reload_retrieved_records == entry_count,
        replay_uses_offset_index_metadata: replay_report_uses_offset_index(
            &direct_publish.replay.replay_report,
            entry_count,
        ) && replay_report_uses_offset_index(
            &sync_writer.replay.replay_report,
            entry_count,
        ) && replay_report_uses_offset_index(
            &async_writer.replay.replay_report,
            entry_count,
        ),
        secondary_replay_uses_offset_index_metadata: replay_report_uses_offset_index(
            &direct_publish.replay.secondary_replay_report,
            entry_count,
        ) && replay_report_uses_offset_index(
            &sync_writer.replay.secondary_replay_report,
            entry_count,
        ) && replay_report_uses_offset_index(
            &async_writer.replay.secondary_replay_report,
            entry_count,
        ),
        single_node_reload_uses_offset_index_metadata: replay_report_uses_offset_index(
            &direct_publish.replay.single_node_reload_report,
            entry_count,
        ) && replay_report_uses_offset_index(
            &sync_writer.replay.single_node_reload_report,
            entry_count,
        ) && replay_report_uses_offset_index(
            &async_writer.replay.single_node_reload_report,
            entry_count,
        ),
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

    // ---- TEETH FOR THE ONE CLAIM THAT CHECKS THIS REPORT'S OWN READER.
    // ----
    // ---- Every field in the summary above is REPORTED, and a reported `false` still exits zero.
    // ---- For most of them that is a choice about what this program is for. For this one it is a
    // ---- hole: `direct_offset_slices_decode_expected_frames` is the result of decoding the log
    // ---- frame with THIS FILE's own copy of the frame message, so it goes false precisely when
    // ---- that copy has fallen behind the frame -- the one condition under which every other
    // ---- offset claim in this report is being checked against a frame nobody parsed.
    // ----
    // ---- It has already happened once. When the frame's digest moved from tag 5 to tag 9 this
    // ---- reader matched 0 of 8 frames, printed `false`, and the program exited successfully.
    // ---- An assertion is the difference between that and a failure.
    // ----
    // ---- Deliberately only this field. The rest are pre-existing claims, and turning them all
    // ---- into assertions in the same change would mix a guard with the question of what they
    // ---- currently say -- which is its own investigation, not a side effect of this one.
    assert!(
        summary.direct_offset_slices_decode_expected_frames,
        "this report's own reader decoded {} of {} log frames. It is a SECOND implementation of \
         the frame reader, kept in step by hand, so the first thing to check is \
         `ReportWalFrameProto` against `SharedStoreWalFrameProto` -- a field the frame has moved \
         to and this reader does not know makes every frame read as unmatched. Fix the reader \
         before trusting any other offset figure in this report",
        direct_publish.offset_frame_validation.matched_frames,
        direct_publish.offset_frame_validation.checked_frames,
    );

    let report = Report {
        schema: "temporalstore_shared_store_append_blob_conformance_report_v1",
        backend: "rust",
        matrixobject_mode: "rust_matrixobject_incremental_journal_protobuf_append_blob",
        entry_count,
        value_bytes,
        direct_publish,
        sync_writer,
        async_writer,
        journal_reopen,
        summary,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("json report")
    );
}

fn replay_report_uses_offset_index(report: &ReplayReport, entry_count: u64) -> bool {
    report.applied == entry_count as usize
        && report.offset_index_reads == entry_count as usize
        && report.range_bytes_read > 0
}

fn matrixobject_options(persistent_journal_path: Option<&std::path::Path>) -> StoreOptions {
    StoreOptions {
        // `segment_size` became `slab_size` and is now `page_size`, following the store
        // dropping its slab tier -- see the note at the matching site in
        // `shared_store.rs`; the external name survives in the object-store crate's own bench.
        // page_size must be < chunk_size -- see the note in the smoke example; the figures
        // were a slab/chunk pair and had to be re-ordered rather than renamed.
        page_size: 1024,
        max_extent_bytes: 1024,
        chunk_size: 4096,
        persistent_journal_path: persistent_journal_path
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_default(),
        ..StoreOptions::default()
    }
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
    let offset_frame_validation =
        validate_offset_frames(&store, &blob_key, &append_receipts, shard_id);
    let offset_metadata = replicator
        .load_wal_offset_metadata(shard_id)
        .await
        .expect("offset metadata");
    let offset_index_validation =
        validate_offset_index(&blob_key, &append_receipts, &offset_metadata);
    let offset_metadata_mappings = build_offset_metadata_mappings(
        &offset_metadata,
        Some(&expected_offsets_from_receipts(&append_receipts)),
    );
    let authoritative_offset_lookup = validate_authoritative_offset_lookup(
        replicator,
        shard_id,
        entry_count,
        "direct",
        value_bytes,
    )
    .await;
    PublishPhaseReport {
        shard_id,
        blob_key,
        publish_latencies_us,
        append_receipts,
        offset_metadata_mappings,
        blob_object_length: metadata.length,
        blob_physical_slab_count: metadata.extents.len(),
        offset_frame_validation,
        offset_index_validation,
        authoritative_offset_lookup,
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
    let offset_metadata = replicator
        .load_wal_offset_metadata(shard_id)
        .await
        .expect("sync writer offset metadata");
    let offset_metadata_mappings = build_offset_metadata_mappings(
        &offset_metadata,
        Some(&expected_offsets_from_write_reports(&write_reports)),
    );
    let authoritative_offset_lookup = validate_authoritative_offset_lookup(
        replicator,
        shard_id,
        entry_count,
        "sync",
        value_bytes,
    )
    .await;
    WriterPhaseReport {
        shard_id,
        write_latencies_us,
        write_reports,
        offset_metadata_mappings,
        blob_object_length: metadata.length,
        blob_physical_slab_count: metadata.extents.len(),
        authoritative_offset_lookup,
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
    let offset_metadata = replicator
        .load_wal_offset_metadata(shard_id)
        .await
        .expect("async writer offset metadata");
    let offset_metadata_mappings = build_offset_metadata_mappings(&offset_metadata, None);
    let authoritative_offset_lookup = validate_authoritative_offset_lookup(
        replicator,
        shard_id,
        entry_count,
        "async",
        value_bytes,
    )
    .await;
    AsyncWriterPhaseReport {
        shard_id,
        enqueue_latencies_us,
        write_reports,
        flush_latency_us,
        flush_report,
        offset_metadata_mappings,
        blob_object_length: metadata.length,
        blob_physical_slab_count: metadata.extents.len(),
        authoritative_offset_lookup,
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

    let (retrieved_records, retrieval_latencies_us) =
        retrieve_records(&follower, shard_id, entry_count, phase);

    let secondary = test_engine(root, &format!("secondary-{phase}-{shard_id}"));
    secondary.load_shard(shard_id);
    let secondary_replay_start = Instant::now();
    let secondary_replay_report = replicator
        .replay_wal_strict(shard_id, 0, &secondary)
        .await
        .expect("secondary replay");
    let secondary_replay_latency_us = secondary_replay_start.elapsed().as_micros();
    let (secondary_retrieved_records, _) =
        retrieve_records(&secondary, shard_id, entry_count, phase);

    let single_node_reload = test_engine(root, &format!("single-node-reload-{phase}-{shard_id}"));
    single_node_reload.load_shard(shard_id);
    let single_node_reload_start = Instant::now();
    let single_node_reload_report = replicator
        .replay_wal_strict(shard_id, 0, &single_node_reload)
        .await
        .expect("single-node reload replay");
    let single_node_reload_latency_us = single_node_reload_start.elapsed().as_micros();
    let (single_node_reload_retrieved_records, _) =
        retrieve_records(&single_node_reload, shard_id, entry_count, phase);

    ReplayPhaseReport {
        replay_latency_us,
        replay_report,
        retrieval_latencies_us,
        retrieved_records,
        retrieval_ok: retrieved_records == entry_count,
        secondary_replay_latency_us,
        secondary_replay_report,
        secondary_retrieved_records,
        secondary_retrieval_ok: secondary_retrieved_records == entry_count,
        single_node_reload_latency_us,
        single_node_reload_report,
        single_node_reload_retrieved_records,
        single_node_reload_ok: single_node_reload_retrieved_records == entry_count,
    }
}

async fn run_journal_reopen(
    root: &std::path::Path,
    persistent_journal_path: std::path::PathBuf,
    direct_shard_id: u64,
    entry_count: u64,
    phases: &[&str],
) -> JournalReopenReport {
    let disk_read_start = Instant::now();
    let journal_bytes = std::fs::read(&persistent_journal_path)
        .expect("read persistent matrixobject journal bytes for checksum");
    let snapshot_disk_read_latency_us = disk_read_start.elapsed().as_micros();
    let snapshot_sha256 = sha256_hex(&journal_bytes);
    let import_start = Instant::now();
    let reopened_store = Arc::new(
        MatrixObjectObjectStore::new(
            "temporalstore-shared",
            matrixobject_options(Some(&persistent_journal_path)),
        )
        .expect("reopen persistent matrixobject journal"),
    );
    let snapshot_import_latency_us = import_start.elapsed().as_micros();
    let snapshot_export_latency_us = 0;
    let snapshot_disk_write_latency_us = 0;
    let reopened_replicator = SharedStoreReplicator::new("cluster-a", reopened_store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
    let reopened_offset_metadata_ok = !reopened_replicator
        .load_wal_offset_metadata(direct_shard_id)
        .await
        .expect("reopened offset metadata")
        .is_empty();
    let cache_before_replay = matrixobject_cache_stats(&reopened_store);

    let mut replay_latency_total_us = 0u128;
    let mut retrieval_latencies = Vec::new();
    let mut replay_ok = true;
    let mut retrieval_ok = true;
    for (phase_index, phase) in phases.iter().enumerate() {
        let shard_id = phase_index as u64 + 1;
        let engine = test_engine(root, &format!("snapshot-reopen-{phase}-{shard_id}"));
        engine.load_shard(shard_id);
        let replay_start = Instant::now();
        let replay_report = reopened_replicator
            .replay_wal_strict(shard_id, 0, &engine)
            .await
            .expect("journal reopened replay");
        replay_latency_total_us += replay_start.elapsed().as_micros();
        replay_ok &= replay_report.applied == entry_count as usize;
        let (retrieved, latencies) = retrieve_records(&engine, shard_id, entry_count, phase);
        retrieval_ok &= retrieved == entry_count;
        retrieval_latencies.extend(latencies);
    }

    let cache_after_retrieval = matrixobject_cache_stats(&reopened_store);
    let cache_metrics_available = cache_after_retrieval.max_bytes > 0;

    JournalReopenReport {
        snapshot_export_latency_us,
        snapshot_disk_write_latency_us,
        snapshot_disk_read_latency_us,
        snapshot_import_latency_us,
        snapshot_bytes: journal_bytes.len() as u64,
        snapshot_sha256,
        reopened_offset_metadata_ok,
        cache_before_replay,
        cache_after_retrieval,
        cache_metrics_available,
        reopened_replay_recovered_all_records: replay_ok,
        reopened_retrieval_recovered_all_records: retrieval_ok,
        replay_latency_total_us,
        retrieval_latency_avg_us: avg(&retrieval_latencies),
    }
}

fn matrixobject_cache_stats(store: &MatrixObjectObjectStore) -> MatrixObjectCacheStatsReport {
    let stats = store
        .inner()
        .lock()
        .expect("matrixobject lock poisoned")
        .read_cache_stats();
    MatrixObjectCacheStatsReport {
        entries: stats.entries,
        bytes: stats.bytes,
        max_bytes: stats.max_bytes,
        hits: stats.hits,
        misses: stats.misses,
        evictions: stats.evictions,
        // A COUNT'S SUCCESSOR IS A COUNT, AND THE COMPILER SUGGESTED THE OTHER ONE.
        // `ChunkReadCacheStats` carries BOTH `direct_fill_blocks` and `direct_fill_bytes`, so
        // when `direct_fill_blocks` went away rustc offered `direct_fill_bytes` -- the nearest
        // NAME, and the wrong quantity. Taking it would have put a byte total under a field
        // this report publishes as a number of fills, in all three of these rows, and the
        // report would still have compiled and still have looked right.
        //
        // The report's own keys stay spelled `_blocks` because they are an output surface and
        // the quantity has not changed; only the upstream field's name did.
        direct_fill_blocks: stats.direct_fill_blocks,
        direct_fill_bytes: stats.direct_fill_bytes,
        compressed_fill_blocks: stats.compressed_fill_blocks,
        compressed_fill_bytes: stats.compressed_fill_bytes,
        ec_fill_blocks: stats.ec_fill_blocks,
        ec_fill_bytes: stats.ec_fill_bytes,
        pressure: stats.pressure,
    }
}

fn retrieve_records(
    engine: &TemporalEngine,
    shard_id: u64,
    entry_count: u64,
    phase: &str,
) -> (u64, Vec<u128>) {
    let mut retrieved_records = 0;
    let mut retrieval_latencies_us = Vec::new();
    for index in 1..=entry_count {
        let key = key_for(shard_id, index, phase);
        let start = Instant::now();
        let response = engine
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
    (retrieved_records, retrieval_latencies_us)
}

fn validate_offset_index(
    blob_key: &str,
    receipts: &[AppendBlobReceipt],
    offset_metadata: &std::collections::BTreeMap<u64, SharedStoreWalOffsetMetadata>,
) -> OffsetIndexValidation {
    let mut matched_entries = 0usize;
    for (index, receipt) in receipts.iter().enumerate() {
        let Some(metadata) = offset_metadata.get(&(index as u64 + 1)) else {
            continue;
        };
        if metadata.wal_blob_key == blob_key
            && metadata.wal_blob_start_offset == receipt.start_offset
            && metadata.wal_blob_end_offset == receipt.end_offset
            && metadata.wal_blob_bytes_written == receipt.bytes_written
            && metadata.wal_blob_object_length == receipt.object_length
        {
            matched_entries += 1;
        }
    }
    OffsetIndexValidation {
        checked_entries: receipts.len(),
        matched_entries,
        all_wal_indexes_have_offset_metadata: offset_metadata.len() >= receipts.len(),
        all_metadata_ranges_match_append_receipts: matched_entries == receipts.len(),
    }
}

fn expected_offsets_from_receipts(
    receipts: &[AppendBlobReceipt],
) -> std::collections::BTreeMap<u64, (u64, u64, u64)> {
    receipts
        .iter()
        .enumerate()
        .map(|(index, receipt)| {
            (
                index as u64 + 1,
                (
                    receipt.start_offset,
                    receipt.end_offset,
                    receipt.bytes_written,
                ),
            )
        })
        .collect()
}

fn expected_offsets_from_write_reports(
    reports: &[SharedStoreWriteReport],
) -> std::collections::BTreeMap<u64, (u64, u64, u64)> {
    reports
        .iter()
        .filter_map(|report| {
            Some((
                report.wal_index,
                (
                    report.wal_blob_start_offset?,
                    report.wal_blob_end_offset?,
                    report.wal_blob_bytes_written?,
                ),
            ))
        })
        .collect()
}

fn build_offset_metadata_mappings(
    offset_metadata: &std::collections::BTreeMap<u64, SharedStoreWalOffsetMetadata>,
    expected_offsets: Option<&std::collections::BTreeMap<u64, (u64, u64, u64)>>,
) -> Vec<OffsetMetadataMapping> {
    offset_metadata
        .iter()
        .map(|(wal_index, metadata)| {
            let expected = expected_offsets.and_then(|offsets| offsets.get(wal_index));
            let (
                expected_start_offset,
                expected_end_offset,
                expected_bytes_written,
                matches_expected_offsets,
            ) = match expected {
                Some((start, end, bytes)) => (
                    Some(*start),
                    Some(*end),
                    Some(*bytes),
                    metadata.wal_blob_start_offset == *start
                        && metadata.wal_blob_end_offset == *end
                        && metadata.wal_blob_bytes_written == *bytes,
                ),
                None => (None, None, None, true),
            };
            OffsetMetadataMapping {
                shard_id: metadata.shard_id,
                wal_index: *wal_index,
                blob_key: metadata.wal_blob_key.clone(),
                start_offset: metadata.wal_blob_start_offset,
                end_offset: metadata.wal_blob_end_offset,
                bytes_written: metadata.wal_blob_bytes_written,
                command_encoding: metadata.command_encoding,
                object_length: metadata.wal_blob_object_length,
                physical_slab_count: metadata.wal_blob_physical_slab_count,
                first_physical_offset: metadata.wal_blob_first_physical_offset,
                expected_start_offset,
                expected_end_offset,
                expected_bytes_written,
                matches_expected_offsets,
            }
        })
        .collect()
}

async fn validate_authoritative_offset_lookup(
    replicator: &SharedStoreReplicator<MatrixObjectObjectStore>,
    shard_id: u64,
    entry_count: u64,
    phase: &str,
    value_bytes: usize,
) -> AuthoritativeOffsetLookupReport {
    let mut metadata_hits = 0u64;
    let mut decoded_entries = 0u64;
    let mut matched_entries = 0u64;
    let mut range_bytes_read = 0u64;
    let mut exact_offset_slice_reads = 0u64;
    let mut range_reads_smaller_than_blob = 0u64;
    let mut slab_metadata_entries = 0u64;
    let mut first_physical_offset_entries = 0u64;
    let mut errors = Vec::new();
    for index in 1..=entry_count {
        match replicator
            .read_wal_entry_by_offset_metadata(shard_id, index)
            .await
        {
            Ok(Some(read)) => {
                metadata_hits += 1;
                decoded_entries += 1;
                range_bytes_read += read.range_bytes_read;
                if read.range_bytes_read == read.metadata.wal_blob_bytes_written
                    && read.range_bytes_read
                        == read
                            .metadata
                            .wal_blob_end_offset
                            .saturating_sub(read.metadata.wal_blob_start_offset)
                {
                    exact_offset_slice_reads += 1;
                } else {
                    errors.push(format!(
                        "lower-layer range read did not match offset slice at wal index {index}"
                    ));
                }
                if read.metadata.wal_blob_object_length > read.range_bytes_read {
                    range_reads_smaller_than_blob += 1;
                }
                if read.metadata.wal_blob_physical_slab_count > 0 {
                    slab_metadata_entries += 1;
                }
                if read.metadata.wal_blob_first_physical_offset.is_some() {
                    first_physical_offset_entries += 1;
                }
                if read.entry == entry(shard_id, index, phase, value_bytes) {
                    matched_entries += 1;
                } else {
                    errors.push(format!("decoded entry mismatch at wal index {index}"));
                }
            }
            Ok(None) => errors.push(format!("missing offset metadata at wal index {index}")),
            Err(err) => errors.push(format!(
                "offset metadata lookup failed at wal index {index}: {err}"
            )),
        }
    }
    AuthoritativeOffsetLookupReport {
        checked_entries: entry_count,
        metadata_hits,
        decoded_entries,
        matched_entries,
        range_bytes_read,
        exact_offset_slice_reads,
        range_reads_smaller_than_blob,
        slab_metadata_entries,
        first_physical_offset_entries,
        all_wal_indexes_directly_read_from_offset_metadata: metadata_hits == entry_count
            && decoded_entries == entry_count,
        all_direct_reads_match_expected_entries: matched_entries == entry_count,
        all_direct_reads_have_slab_metadata: slab_metadata_entries == entry_count
            && first_physical_offset_entries == entry_count,
        lower_layer_range_reads_exact_offset_slices: exact_offset_slice_reads == entry_count,
        lower_layer_range_reads_avoid_full_blob_scans: range_reads_smaller_than_blob
            >= entry_count.saturating_sub(1),
        lower_layer_blob_offset_reads_proven: metadata_hits == entry_count
            && decoded_entries == entry_count
            && exact_offset_slice_reads == entry_count
            && range_reads_smaller_than_blob >= entry_count.saturating_sub(1),
        errors,
    }
}

fn validate_offset_frames(
    store: &MatrixObjectObjectStore,
    blob_key: &str,
    receipts: &[AppendBlobReceipt],
    shard_id: u64,
) -> OffsetFrameValidation {
    let object = store
        .inner()
        .lock()
        .expect("matrixobject lock poisoned")
        .get_object("temporalstore-shared", blob_key)
        .expect("matrixobject blob");
    let bytes = object.data;
    let mut matched_frames = 0usize;
    let mut contiguous_coverage_bytes = 0u64;
    for (expected_index, receipt) in receipts.iter().enumerate() {
        let start = receipt.start_offset as usize;
        let end = receipt.end_offset as usize;
        if end > bytes.len() || start >= end || end - start < 4 {
            continue;
        }
        let slice = &bytes[start..end];
        let frame_len = u32::from_be_bytes(
            slice[0..4]
                .try_into()
                .expect("frame length slice is exactly 4 bytes"),
        ) as usize;
        if frame_len + 4 != slice.len() {
            continue;
        }
        let Ok(frame) = ReportWalFrameProto::decode(&slice[4..]) else {
            continue;
        };
        let command_sha256 = sha256_hex(&frame.command_payload);
        // Verify the digest in WHICHEVER SHAPE the frame declares. A format this reader does not
        // know is NOT treated as a pass: it falls through to false, the frame counts as unmatched,
        // and the assertion in `main` turns that into a failure instead of a printed `false`.
        let digest_matches = match frame.frame_format {
            REPORT_FRAME_FORMAT_HEX_DIGEST => frame.command_sha256 == command_sha256,
            REPORT_FRAME_FORMAT_RAW_DIGEST => hex_lower(&frame.command_digest) == command_sha256,
            REPORT_FRAME_FORMAT_CRC32C | REPORT_FRAME_FORMAT_NO_SIZE => {
                frame.command_crc32c == crc32c_of(&frame.command_payload)
            }
            _ => false,
        };
        // The narrowest format does not restate the payload length, so this check derives it the
        // way the decoder does rather than demanding a field that frame deliberately omits.
        let size_matches = match frame.frame_format {
            REPORT_FRAME_FORMAT_NO_SIZE => frame.command_byte_size == 0,
            _ => frame.command_byte_size == frame.command_payload.len() as u64,
        };
        if frame.shard_id == shard_id
            && frame.wal_index == expected_index as u64 + 1
            && size_matches
            && digest_matches
        {
            matched_frames += 1;
            contiguous_coverage_bytes = receipt.end_offset;
        }
    }
    OffsetFrameValidation {
        checked_frames: receipts.len(),
        matched_frames,
        all_offsets_decode_expected_frame: matched_frames == receipts.len(),
        contiguous_coverage_bytes,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
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
    // See the note at the matching site in `shared_store_append_blob_smoke.rs`: the entry gained
    // `staged_blocks` and `outcomes`, and `command` became optional. This builder keeps naming a
    // command, because that is what the conformance report exercises.
    SharedStoreWalEntry {
        shard_id,
        wal_index: index,
        command: Some(command(shard_id, index, phase, value_bytes)),
        staged_blocks: Vec::new(),
        outcomes: Vec::new(),
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
    format!("cluster-a/shards/{shard_id}/shared/wal/wal.protobuf.blob")
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
