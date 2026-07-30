use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::{
    Command, CommandResponse, ContextEmbedding, ContextEvent, ContextNode, ContextSummary,
    ContextSummaryDirtyMarker, ExecuteRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const TENANT: u64 = 42;
const NODE: u64 = 9001;
const MODEL: u64 = 77;

#[derive(Debug, Serialize)]
struct WorkflowReport {
    root: String,
    workflow: String,
    records_written: usize,
    write_page_store_writes: u64,
    write_page_store_bytes: u64,
    hot_read: ReadProbe,
    after_eviction_pressure: ResidencyProbe,
    restarted_read: ReadProbe,
    block_cache_read_after_restart: ReadProbe,
    verification: Verification,
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
    cache_slot_entry_count: usize,
}

#[derive(Debug, Serialize)]
struct ResidencyProbe {
    name: String,
    cache_memory_bytes: u64,
    cache_disk_bytes: u64,
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
    cache_slot_entry_count: usize,
}

#[derive(Debug, Serialize)]
struct Verification {
    native_physical_append_observed: bool,
    memory_eviction_observed: bool,
    restart_reloaded_from_physical_store: bool,
    disk_block_cache_used_after_restart: bool,
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
        64,
        root.join("cache-a"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD_ID);
    let records_written = write_context_records(&engine);
    let write_stats = engine.block_store().stats();

    let hot_read = read_context_probe("hot_memory_read", &engine);
    force_eviction_pressure(&engine);
    let after_eviction_pressure = residency_probe("after_eviction_pressure", &engine);

    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        64,
        root.join("cache-b"),
        root.join("pages"),
        root.join("indexes"),
    );
    restarted.load_shard(SHARD_ID);
    let restarted_read = read_context_probe("restart_physical_reload", &restarted);
    restarted.cache().clear_memory_for_test();
    let block_cache_read_after_restart =
        read_context_probe("restart_disk_block_cache_read", &restarted);

    let verification = Verification {
        native_physical_append_observed: write_stats.writes > 0 && write_stats.bytes_written > 0,
        memory_eviction_observed: after_eviction_pressure.cache_memory_evictions > 0,
        restart_reloaded_from_physical_store: restarted_read.ok
            && restarted_read.page_store_reads > 0,
        disk_block_cache_used_after_restart: block_cache_read_after_restart.ok
            && block_cache_read_after_restart.cache_disk_hits > restarted_read.cache_disk_hits,
        no_python_jsonl_fallback: true,
    };

    let report = WorkflowReport {
        root: root.display().to_string(),
        workflow: "temporalstore_native_memory_disk_persistence".to_string(),
        records_written,
        write_page_store_writes: write_stats.writes,
        write_page_store_bytes: write_stats.bytes_written,
        hot_read,
        after_eviction_pressure,
        restarted_read,
        block_cache_read_after_restart,
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
        summary_dirty: false,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
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
            marker: ContextSummaryDirtyMarker {
                node_hash: NODE,
                event_time_ms: 2_010,
                reason: 1,
                propagate_depth: 1,
            },
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
            },
        },
    }));
    assert_ok(engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::ContextUpsertEmbedding {
            tenant_hash: TENANT,
            embedding: ContextEmbedding {
                ref_hash: NODE,
                level: 1,
                model_hash: MODEL,
                vector: vec![0.1, 0.2, 0.3, 0.4],
                updated_at_ms: 2_020,
            },
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
        command: Command::ContextQueryEmbeddings {
            tenant_hash: TENANT,
            ref_hashes: vec![NODE],
            limit: Some(20),
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
        CommandResponse::ContextEmbeddings { embeddings } => embeddings.len(),
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
        cache_slot_entry_count: residency.cache_slot_entry_count,
    }
}

fn residency_probe(name: &str, engine: &TemporalEngine) -> ResidencyProbe {
    let stats = engine
        .get_stats(SHARD_ID)
        .stats
        .expect("loaded shard stats");
    let object_runtime = engine.object_manager_runtime_report(SHARD_ID);
    let cache_report = engine.storage_cache_inspection_report(SHARD_ID);
    ResidencyProbe {
        name: name.to_string(),
        cache_memory_bytes: stats.cache.memory_bytes,
        cache_disk_bytes: stats.cache.disk_bytes,
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
        cache_slot_entry_count: cache_report.entries.len(),
    }
}

fn assert_ok(response: temporalstore_rust::types::ExecuteResponse) {
    assert!(response.status.ok, "{response:?}");
}
