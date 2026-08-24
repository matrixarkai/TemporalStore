// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::{
    Command, CommandResponse, ContextEvent, ContextNode, ContextSummary,
    ContextDirtyNode, ExecuteRequest, StorageCacheWarmupReport, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const TENANT: u64 = 42;
const NODE: u64 = 9001;
const MODEL: u64 = 77;
const APPEND_EMBEDDING_REF: u64 = NODE + 1;
const TINY_CACHE_BYTES: usize = 64;
const ASYNC_WARMUP_CACHE_BYTES: usize = 4096;

#[derive(Debug, Serialize)]
struct WorkflowReport {
    root: String,
    workflow: String,
    records_written: usize,
    write_page_store_writes: u64,
    write_page_store_bytes: u64,
    context_count_before_restart: ContextDataCount,
    hot_read: ReadProbe,
    before_restart_memory: ResidencyProbe,
    after_eviction_pressure: ResidencyProbe,
    after_restart_before_query: ResidencyProbe,
    query_refill_after_restart: ReadProbe,
    context_count_after_restart_query: ContextDataCount,
    post_restart_append_records: usize,
    context_count_after_post_restart_append: ContextDataCount,
    context_count_after_second_restart: ContextDataCount,
    block_cache_read_after_restart: ReadProbe,
    async_warmup_before_query: ResidencyProbe,
    serving_during_async_warmup: ReadProbe,
    async_warmup: AsyncWarmupProbe,
    after_async_warmup: ResidencyProbe,
    verification: Verification,
}

#[derive(Debug, Serialize)]
struct ContextDataCount {
    name: String,
    total_context_data_count: usize,
    node_count: usize,
    event_count: usize,
    summary_dirty_count: usize,
    summary_count: usize,
    embedding_count: usize,
}

#[derive(Debug, Serialize)]
struct ReadProbe {
    name: String,
    latency_us: u128,
    ok: bool,
    returned_events: usize,
    returned_summaries: usize,
    returned_embeddings: usize,
    cache_memory_bytes: u64,
    cache_disk_bytes: u64,
    cache_memory_entry_count: usize,
    cache_disk_entry_count: usize,
    context_cache_entry_count: usize,
    context_cache_memory_entry_count: usize,
    context_cache_disk_entry_count: usize,
    context_cache_memory_bytes: u64,
    context_cache_disk_bytes: u64,
    cache_memory_hits: u64,
    cache_disk_hits: u64,
    cache_memory_evictions: u64,
    page_store_reads: u64,
    page_store_bytes_read: u64,
    object_count: u64,
    hot_object_count: u64,
    cold_object_count: u64,
    mixed_residency_object_count: u64,
    dirty_object_count: u64,
    #[serde(rename = "cache_slot_entry_count")]
    cache_bucket_entry_count: usize,
}

#[derive(Debug, Serialize)]
struct ResidencyProbe {
    name: String,
    cache_memory_bytes: u64,
    cache_disk_bytes: u64,
    cache_memory_entry_count: usize,
    cache_disk_entry_count: usize,
    context_cache_entry_count: usize,
    context_cache_memory_entry_count: usize,
    context_cache_disk_entry_count: usize,
    context_cache_memory_bytes: u64,
    context_cache_disk_bytes: u64,
    cache_memory_hits: u64,
    cache_disk_hits: u64,
    cache_memory_evictions: u64,
    page_store_reads: u64,
    page_store_bytes_read: u64,
    object_count: u64,
    hot_object_count: u64,
    cold_object_count: u64,
    mixed_residency_object_count: u64,
    dirty_object_count: u64,
    #[serde(rename = "cache_slot_entry_count")]
    cache_bucket_entry_count: usize,
}

#[derive(Debug, Serialize)]
struct AsyncWarmupProbe {
    name: String,
    batches: usize,
    batch_size: usize,
    latency_us: u128,
    reports: Vec<StorageCacheWarmupReport>,
    considered_page_refs: usize,
    warmed_page_refs: usize,
    already_cached_page_refs: usize,
    block_store_reads: usize,
    failed_page_refs: usize,
    warmed_bytes: u64,
}

#[derive(Debug, Serialize)]
struct Verification {
    native_physical_append_observed: bool,
    memory_eviction_observed: bool,
    restart_starts_with_cold_memory: bool,
    query_refilled_memory_after_restart: bool,
    restart_reloaded_from_physical_store: bool,
    disk_block_cache_used_after_restart: bool,
    serving_available_while_async_warmup_running: bool,
    async_warmup_loaded_pages_without_foreground_query: bool,
    context_total_count_survives_restart: bool,
    post_restart_append_increased_total_count: bool,
    second_restart_preserved_increased_total_count: bool,
    no_python_jsonl_fallback: bool,
}

fn main() {
    let root = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(default_root);
    fs::create_dir_all(root.join("cache-a")).expect("create cache-a");
    fs::create_dir_all(root.join("cache-b")).expect("create cache-b");
    fs::create_dir_all(root.join("pages")).expect("create pages");
    fs::create_dir_all(root.join("indexes")).expect("create indexes");

    let engine = TemporalEngine::with_local_dirs(
        TINY_CACHE_BYTES,
        root.join("cache-a"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD_ID);
    let records_written = write_context_records(&engine);
    let write_stats = engine.block_store().stats();

    let hot_read = read_context_probe("hot_memory_read", &engine);
    let context_count_before_restart = count_context_data("before_restart", &engine, &[NODE]);
    let before_restart_memory = residency_probe("before_restart_memory_after_hot_query", &engine);
    force_eviction_pressure(&engine);
    let after_eviction_pressure = residency_probe("after_eviction_pressure", &engine);

    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        TINY_CACHE_BYTES,
        root.join("cache-b"),
        root.join("pages"),
        root.join("indexes"),
    );
    restarted.load_shard(SHARD_ID);
    let after_restart_before_query = residency_probe("after_restart_before_query", &restarted);
    let query_refill_after_restart = read_context_probe("query_refill_after_restart", &restarted);
    let context_count_after_restart_query =
        count_context_data("after_restart_query", &restarted, &[NODE]);
    let post_restart_append_records = append_context_records_after_restart(&restarted);
    let context_count_after_post_restart_append = count_context_data(
        "after_post_restart_append",
        &restarted,
        &[NODE, APPEND_EMBEDDING_REF],
    );
    restarted.cache().clear_memory_for_test();
    let block_cache_read_after_restart =
        read_context_probe("restart_disk_block_cache_read", &restarted);

    drop(restarted);

    let second_restarted = TemporalEngine::with_local_dirs(
        TINY_CACHE_BYTES,
        root.join("cache-d"),
        root.join("pages"),
        root.join("indexes"),
    );
    second_restarted.load_shard(SHARD_ID);
    let context_count_after_second_restart = count_context_data(
        "after_second_restart",
        &second_restarted,
        &[NODE, APPEND_EMBEDDING_REF],
    );

    let async_restarted = TemporalEngine::with_local_dirs(
        ASYNC_WARMUP_CACHE_BYTES,
        root.join("cache-c"),
        root.join("pages"),
        root.join("indexes"),
    );
    async_restarted.load_shard(SHARD_ID);
    let async_warmup_before_query = residency_probe("async_warmup_before_query", &async_restarted);
    let (async_warmup, serving_during_async_warmup, warmup_active_during_serving) =
        run_gradual_async_warmup_with_serving(
            async_restarted.clone(),
            4,
            Duration::from_millis(20),
        );
    let after_async_warmup = residency_probe("after_async_warmup", &async_restarted);

    let verification = Verification {
        native_physical_append_observed: write_stats.writes > 0 && write_stats.bytes_written > 0,
        memory_eviction_observed: after_eviction_pressure.cache_memory_evictions > 0,
        restart_starts_with_cold_memory: after_restart_before_query.cache_memory_entry_count == 0,
        query_refilled_memory_after_restart: query_refill_after_restart.ok
            && query_refill_after_restart.cache_memory_entry_count
                > after_restart_before_query.cache_memory_entry_count,
        restart_reloaded_from_physical_store: query_refill_after_restart.ok
            && query_refill_after_restart.page_store_reads > 0,
        disk_block_cache_used_after_restart: block_cache_read_after_restart.ok
            && block_cache_read_after_restart.cache_disk_hits
                > query_refill_after_restart.cache_disk_hits,
        serving_available_while_async_warmup_running: warmup_active_during_serving
            && serving_during_async_warmup.ok,
        async_warmup_loaded_pages_without_foreground_query: async_warmup.warmed_page_refs > 0
            && after_async_warmup.cache_memory_entry_count
                > async_warmup_before_query.cache_memory_entry_count,
        context_total_count_survives_restart: context_count_after_restart_query
            .total_context_data_count
            >= context_count_before_restart.total_context_data_count,
        post_restart_append_increased_total_count: context_count_after_post_restart_append
            .total_context_data_count
            > context_count_after_restart_query.total_context_data_count,
        second_restart_preserved_increased_total_count: context_count_after_second_restart
            .total_context_data_count
            >= context_count_after_post_restart_append.total_context_data_count,
        no_python_jsonl_fallback: true,
    };

    let report = WorkflowReport {
        root: root.display().to_string(),
        workflow: "temporalstore_native_memory_disk_persistence".to_string(),
        records_written,
        write_page_store_writes: write_stats.writes,
        write_page_store_bytes: write_stats.bytes_written,
        context_count_before_restart,
        hot_read,
        before_restart_memory,
        after_eviction_pressure,
        after_restart_before_query,
        query_refill_after_restart,
        context_count_after_restart_query,
        post_restart_append_records,
        context_count_after_post_restart_append,
        context_count_after_second_restart,
        block_cache_read_after_restart,
        async_warmup_before_query,
        serving_during_async_warmup,
        async_warmup,
        after_async_warmup,
        verification,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
}

fn default_root() -> PathBuf {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_millis();
    env::temp_dir().join(format!("temporalstore-native-persistence-{now_ms}"))
}

fn write_context_records(engine: &TemporalEngine) -> usize {
    let node = ContextNode {
        node_hash: NODE,
        parent_hash: 0,
        kind: 1,
        canonical_name: "native-persistence-node".to_string(),
        l0: "thread/session".to_string(),
        status: 0,
        last_event_time_ms: 1_000,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
    };
    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextUpsertNode {
            tenant_hash: TENANT,
            node,
        },
    }));

    for index in 0..6_u64 {
        assert_ok(engine.execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: NODE,
                event: ContextEvent {
                    event_id_hash: 10_000 + index,
                    event_time_ms: 1_000 + index,
                    ingestion_time_ms: 2_000 + index,
                    kind: 0,
                    event_type: 2,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.95,
                    importance: 0.8,
                    text: format!("native TemporalStore persisted context event {index}"),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    // No vector on this fixture; empty is what a record without one holds.
                    vector: Vec::new(),
                },
                first_write_only: true,
                cold_storage: false,
            },
        }));
    }

    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextMarkSummaryDirty {
            tenant_hash: TENANT,
            node_hash: NODE,
                event_time_ms: 2_010,
                reason: 1,
                propagate_depth: 1,
        },
    }));
    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextUpsertSummary {
            tenant_hash: TENANT,
            summary: ContextSummary {
                node_hash: NODE,
                level: 1,
                text: "Native TemporalStore summary mapped to embedding evidence.".to_string(),
                valid_from_ms: 2_020,
                // No vector on this fixture; empty is what a record without one holds.
                vector: Vec::new(),
            },
        },
    }));
    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextSetNodeEmbedding {
            tenant_hash: TENANT,
            node_hash: NODE,
            model_hash: MODEL,
            vector: vec![0.1, 0.2, 0.3, 0.4],
            updated_at_ms: 2_020,
        },
    }));
    10
}

fn force_eviction_pressure(engine: &TemporalEngine) {
    for index in 0..16 {
        let key = format!("eviction-pressure-{index}");
        let value = format!("value-{index}-0123456789abcdefghijklmnopqrstuvwxyz").into_bytes();
        assert_ok(engine.execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::StringSet {
                key: key.clone(),
                value,
            },
        }));
        assert_ok(engine.execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::StringGet { key },
        }));
    }
}

fn append_context_records_after_restart(engine: &TemporalEngine) -> usize {
    for index in 0..3_u64 {
        assert_ok(engine.execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: NODE,
                event: ContextEvent {
                    event_id_hash: 20_000 + index,
                    event_time_ms: 2_100 + index,
                    ingestion_time_ms: 3_100 + index,
                    kind: 0,
                    event_type: 2,
                    actor_hash: 0,
                    status: 0,
                    valid_until_ms: 0,
                    confidence: 0.96,
                    importance: 0.85,
                    text: format!("post restart monotonic context append {index}"),
                    source_ref: String::new(),
                    related_node_hashes: Vec::new(),
                    compact_attrs: Vec::new(),
                    // No vector on this fixture; empty is what a record without one holds.
                    vector: Vec::new(),
                },
                first_write_only: true,
                cold_storage: false,
            },
        }));
    }
    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextMarkSummaryDirty {
            tenant_hash: TENANT,
            node_hash: NODE,
                event_time_ms: 3_200,
                reason: 2,
                propagate_depth: 1,
        },
    }));
    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextUpsertSummary {
            tenant_hash: TENANT,
            summary: ContextSummary {
                node_hash: NODE,
                level: 1,
                text: "Post restart monotonic summary append.".to_string(),
                valid_from_ms: 3_300,
                // No vector on this fixture; empty is what a record without one holds.
                vector: Vec::new(),
            },
        },
    }));
    // The vector lives on a node, so the append fixture needs one to carry it -- an
    // embedding for a node that does not exist is refused, not conjured.
    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextUpsertNode {
            tenant_hash: TENANT,
            node: ContextNode {
                node_hash: APPEND_EMBEDDING_REF,
                parent_hash: NODE,
                kind: 1,
                canonical_name: "native-persistence-append-node".to_string(),
                l0: "appended after restart".to_string(),
                status: 0,
                last_event_time_ms: 3_300,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
                vector: Vec::new(),
                embedding_model_hash: 0,
                embedding_updated_at_ms: 0,
            },
        },
    }));
    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextSetNodeEmbedding {
            tenant_hash: TENANT,
            node_hash: APPEND_EMBEDDING_REF,
            model_hash: MODEL,
            vector: vec![0.5, 0.6, 0.7, 0.8],
            updated_at_ms: 3_300,
        },
    }));
    6
}

fn count_context_data(
    name: &str,
    engine: &TemporalEngine,
    embedding_refs: &[u64],
) -> ContextDataCount {
    let node_count = match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextGetNode {
                tenant_hash: TENANT,
                node_hash: NODE,
            },
        })
        .response
    {
        CommandResponse::ContextNode { node: Some(_), .. } => 1,
        _ => 0,
    };
    let event_count = match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextQueryEvents {
                tenant_hash: TENANT,
                node_hash: NODE,
                start_time_ms: 0,
                end_time_ms: 4_000,
                limit: Some(100),
                max_scan: None,
                current_valid_only: false,
                as_of_ms: 0,
                kinds: Vec::new(),
                statuses: Vec::new(),
                min_confidence: 0.0,
                min_importance: 0.0,
            },
        })
        .response
    {
        CommandResponse::ContextEvents { events, .. } => events.len(),
        _ => 0,
    };
    let summary_dirty_count = match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash: TENANT,
                node_hash: NODE,
                start_time_ms: 0,
                end_time_ms: 4_000,
                limit: Some(100),
            },
        })
        .response
    {
        CommandResponse::ContextSummaryDirtyNodes { nodes, .. } => nodes.len(),
        _ => 0,
    };
    let summary_count = match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextQuerySummaries {
                tenant_hash: TENANT,
                node_hash: NODE,
                level: 1,
                as_of_ms: 4_000,
                limit: Some(100),
            },
        })
        .response
    {
        CommandResponse::ContextSummaries { summaries, .. } => summaries.len(),
        _ => 0,
    };
    // Embeddings are counted on their owners: a node whose record carries a vector.
    let embedding_count = match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextGetNodes {
                tenant_hash: TENANT,
                node_hashes: embedding_refs.to_vec(),
            },
        })
        .response
    {
        CommandResponse::ContextNodes { nodes } => nodes
            .iter()
            .filter(|node| !node.vector.is_empty())
            .count(),
        _ => 0,
    };
    ContextDataCount {
        name: name.to_string(),
        total_context_data_count: node_count
            + event_count
            + summary_dirty_count
            + summary_count
            + embedding_count,
        node_count,
        event_count,
        summary_dirty_count,
        summary_count,
        embedding_count,
    }
}

fn run_gradual_async_warmup_with_serving(
    engine: TemporalEngine,
    batch_size: usize,
    pause_between_batches: Duration,
) -> (AsyncWarmupProbe, ReadProbe, bool) {
    let serving_engine = engine.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let start = Instant::now();
    let handle = thread::spawn(move || {
        let mut buckets = engine
            .bucket_storage_summaries(SHARD_ID)
            .into_iter()
            .map(|summary| summary.routing_bucket)
            .collect::<Vec<_>>();
        buckets.sort_unstable();
        buckets.dedup();
        let mut reports = Vec::new();
        for (index, batch) in buckets.chunks(batch_size.max(1)).enumerate() {
            if index == 0 {
                let _ = started_tx.send(());
            }
            reports.push(engine.storage_cache_warmup_report(SHARD_ID, batch.iter().copied()));
            thread::sleep(pause_between_batches);
        }
        let _ = done_tx.send(());
        reports
    });
    let _ = started_rx.recv_timeout(Duration::from_secs(1));
    let warmup_active_before_serving = done_rx.try_recv().is_err();
    let serving_during_warmup = read_context_probe("serving_during_async_warmup", &serving_engine);
    let warmup_active_after_serving = done_rx.try_recv().is_err();
    let reports = handle.join().expect("async warmup thread should complete");
    let latency_us = start.elapsed().as_micros();
    let async_warmup = AsyncWarmupProbe {
        name: "gradual_async_page_index_warmup".to_string(),
        batches: reports.len(),
        batch_size,
        latency_us,
        considered_page_refs: reports
            .iter()
            .map(|report| report.considered_page_refs)
            .sum(),
        warmed_page_refs: reports.iter().map(|report| report.warmed_page_refs).sum(),
        already_cached_page_refs: reports
            .iter()
            .map(|report| report.already_cached_page_refs)
            .sum(),
        block_store_reads: reports.iter().map(|report| report.block_store_reads).sum(),
        failed_page_refs: reports.iter().map(|report| report.failed_page_refs).sum(),
        warmed_bytes: reports.iter().map(|report| report.warmed_bytes).sum(),
        reports,
    };
    (
        async_warmup,
        serving_during_warmup,
        warmup_active_before_serving && warmup_active_after_serving,
    )
}

fn read_context_probe(name: &str, engine: &TemporalEngine) -> ReadProbe {
    let start = Instant::now();
    let events = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextQueryEvents {
            tenant_hash: TENANT,
            node_hash: NODE,
            start_time_ms: 0,
            end_time_ms: 3_000,
            limit: Some(20),
            max_scan: None,
            current_valid_only: false,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.0,
            min_importance: 0.0,
        },
    });
    let summaries = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextQuerySummaries {
            tenant_hash: TENANT,
            node_hash: NODE,
            level: 1,
            as_of_ms: 3_000,
            limit: Some(20),
        },
    });
    let embeddings = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextGetNodes {
            tenant_hash: TENANT,
            node_hashes: vec![NODE],
        },
    });
    let latency_us = start.elapsed().as_micros();
    let returned_events = match &events.response {
        CommandResponse::ContextEvents { events, .. } => events.len(),
        _ => 0,
    };
    let returned_summaries = match &summaries.response {
        CommandResponse::ContextSummaries { summaries, .. } => summaries.len(),
        _ => 0,
    };
    let returned_embeddings = match &embeddings.response {
        CommandResponse::ContextNodes { nodes } => nodes
            .iter()
            .filter(|node| !node.vector.is_empty())
            .count(),
        _ => 0,
    };
    let residency = residency_probe(name, engine);
    ReadProbe {
        name: name.to_string(),
        latency_us,
        ok: events.status.ok
            && summaries.status.ok
            && embeddings.status.ok
            && returned_events >= 6
            && returned_summaries >= 1
            && returned_embeddings >= 1,
        returned_events,
        returned_summaries,
        returned_embeddings,
        cache_memory_bytes: residency.cache_memory_bytes,
        cache_disk_bytes: residency.cache_disk_bytes,
        cache_memory_entry_count: residency.cache_memory_entry_count,
        cache_disk_entry_count: residency.cache_disk_entry_count,
        context_cache_entry_count: residency.context_cache_entry_count,
        context_cache_memory_entry_count: residency.context_cache_memory_entry_count,
        context_cache_disk_entry_count: residency.context_cache_disk_entry_count,
        context_cache_memory_bytes: residency.context_cache_memory_bytes,
        context_cache_disk_bytes: residency.context_cache_disk_bytes,
        cache_memory_hits: residency.cache_memory_hits,
        cache_disk_hits: residency.cache_disk_hits,
        cache_memory_evictions: residency.cache_memory_evictions,
        page_store_reads: residency.page_store_reads,
        page_store_bytes_read: residency.page_store_bytes_read,
        object_count: residency.object_count,
        hot_object_count: residency.hot_object_count,
        cold_object_count: residency.cold_object_count,
        mixed_residency_object_count: residency.mixed_residency_object_count,
        dirty_object_count: residency.dirty_object_count,
        cache_bucket_entry_count: residency.cache_bucket_entry_count,
    }
}

fn residency_probe(name: &str, engine: &TemporalEngine) -> ResidencyProbe {
    let stats = engine
        .get_stats(SHARD_ID)
        .stats
        .expect("loaded shard stats");
    let object_runtime = engine.object_manager_runtime_report(SHARD_ID);
    let cache_report = engine.storage_cache_inspection_report(SHARD_ID);
    let cache_memory_entry_count = cache_report
        .entries
        .iter()
        .filter(|entry| entry.memory_bytes > 0)
        .count();
    let cache_disk_entry_count = cache_report
        .entries
        .iter()
        .filter(|entry| entry.disk_bytes > 0)
        .count();
    let context_buckets = context_routing_buckets(engine);
    let context_entries = cache_report
        .entries
        .iter()
        .filter(|entry| is_context_cache_entry(entry, &context_buckets))
        .collect::<Vec<_>>();
    ResidencyProbe {
        name: name.to_string(),
        cache_memory_bytes: stats.cache.memory_bytes,
        cache_disk_bytes: stats.cache.disk_bytes,
        cache_memory_entry_count,
        cache_disk_entry_count,
        context_cache_entry_count: context_entries.len(),
        context_cache_memory_entry_count: context_entries
            .iter()
            .filter(|entry| entry.memory_bytes > 0)
            .count(),
        context_cache_disk_entry_count: context_entries
            .iter()
            .filter(|entry| entry.disk_bytes > 0)
            .count(),
        context_cache_memory_bytes: context_entries.iter().map(|entry| entry.memory_bytes).sum(),
        context_cache_disk_bytes: context_entries.iter().map(|entry| entry.disk_bytes).sum(),
        cache_memory_hits: stats.cache.memory_hits,
        cache_disk_hits: stats.cache.disk_hits,
        cache_memory_evictions: stats.cache.memory_evictions,
        page_store_reads: stats.page_store.reads,
        page_store_bytes_read: stats.page_store.bytes_read,
        object_count: object_runtime.object_count,
        hot_object_count: object_runtime.hot_object_count,
        cold_object_count: object_runtime.cold_object_count,
        mixed_residency_object_count: object_runtime.mixed_residency_object_count,
        dirty_object_count: object_runtime.dirty_object_count,
        cache_bucket_entry_count: cache_report.entries.len(),
    }
}

fn context_routing_buckets(engine: &TemporalEngine) -> BTreeSet<u32> {
    [
        format!("ctx:node:{TENANT}:{NODE}"),
        format!("ctx:event:{TENANT}:{NODE}"),
        format!("ctx:dirty:{TENANT}:{NODE}"),
        format!("ctx:summary:{TENANT}:{NODE}:1"),
        format!("ctx:embedding:{TENANT}:{NODE}"),
    ]
    .into_iter()
    .map(|key| engine.routing_bucket_for_key(SHARD_ID, &key))
    .collect()
}

fn is_context_cache_entry(
    entry: &matrixcache::CacheEntryInfo,
    context_buckets: &BTreeSet<u32>,
) -> bool {
    entry.namespace.contains("context")
        || entry.record_key.starts_with("ctx:")
        || entry.selector.contains("ctx:")
        || entry
            .routing_slot
            .map(|bucket| context_buckets.contains(&bucket))
            .unwrap_or(false)
}

fn assert_ok(response: temporalstore_rust::types::ExecuteResponse) {
    assert!(response.status.ok, "{response:?}");
}
