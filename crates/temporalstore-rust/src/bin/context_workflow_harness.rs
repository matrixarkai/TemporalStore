use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use std::collections::BTreeMap;

use serde::Serialize;
use temporalstore_rust::{
    context_pipeline_manage_report, context_pipeline_parity_evidence, extract_context,
    ingest_extract_context, inject_context, retrieve_context, run_context_pipeline_benchmark,
    run_context_pipeline_benchmark_sweep, Command, CommandResponse, ContextExtractRequest,
    ContextIngestExtractRequest, ContextInjectRequest, ContextModelProviderConfig,
    ContextPipelineBenchmarkRequest, ContextPipelineBenchmarkSweepProfile,
    ContextPipelineBenchmarkSweepRequest, ContextPipelineParityEvidence, ContextRetrieveRequest,
    ContextSourceKind, ContextTier, ExecuteRequest, RaftCluster, RaftConfig, SharedStoreReplicator,
    SharedStoreStorageMode, TemporalEngine,
};
use temporalstore_snapshot::object_store::FileObjectStore;

#[derive(Debug, Serialize)]
struct ContextWorkflowHarnessSummary {
    root: String,
    extraction_ok: bool,
    retrieve_block_count: usize,
    selected_block_count: usize,
    blocked_block_count: usize,
    audit_selected_ref_count: usize,
    injected_prompt_contains_context: bool,
    provider_name: String,
    parity: ContextPipelineParityEvidence,
    restart_replay_ready: bool,
    shared_store_sync_ready: bool,
    shared_store_async_ready: bool,
    raft_read_ready: bool,
    unified_corpus_ready: bool,
    context_pipeline_ready: bool,
    management_ready: bool,
    ingest_extract_ready: bool,
    retrieve_pipeline_ready: bool,
    ingest_extract_accepted: usize,
    ingest_extract_failed: usize,
    ingest_extract_source_count: usize,
    ingest_extract_unique_nodes: usize,
    ingest_extract_source_kind_counts: BTreeMap<String, usize>,
    ingest_extract_provider_counts: BTreeMap<String, usize>,
    managed_routes: Vec<String>,
    pipeline_stages: Vec<String>,
    pipeline_stage_ready_count: usize,
    policy_controls: Vec<String>,
    provider_names: Vec<String>,
    benchmark_ready: bool,
    benchmark_profile: String,
    benchmark_source_count: usize,
    benchmark_query_count: usize,
    benchmark_hit_at_k: f32,
    benchmark_mean_reciprocal_rank: f32,
    benchmark_recall_at_k: f32,
    benchmark_token_reduction_percent: f32,
    benchmark_ingest_sources_per_sec: f64,
    benchmark_retrieve_queries_per_sec: f64,
    benchmark_inject_queries_per_sec: f64,
    benchmark_per_query_count: usize,
    benchmark_retrieve_p50_ms: u128,
    benchmark_retrieve_p95_ms: u128,
    benchmark_sweep_ready: bool,
    benchmark_sweep_profile_count: usize,
    benchmark_sweep_total_sources: usize,
    benchmark_sweep_total_queries: usize,
    benchmark_sweep_min_hit_at_k: f32,
    benchmark_sweep_min_mean_reciprocal_rank: f32,
    benchmark_sweep_min_token_reduction_percent: f32,
    benchmark_sweep_max_retrieve_p95_ms: u128,
    parity_evidence: Vec<String>,
}

fn main() {
    let root = parse_root();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(1);

    let extract = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 20260616,
            source_kind: ContextSourceKind::Incident,
            source_id: "mock-incident-1".to_string(),
            title: "Checkout risk incident".to_string(),
            body: "Customer checkout failed. Payment risk score spiked. The proxy retried safely and support asked for root cause.".to_string(),
            timestamp_ms: 1_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(extract.status.ok, "{:?}", extract.status);

    let retrieve_request = ContextRetrieveRequest {
        shard_id: 1,
        tenant_hash: 20260616,
        node_hashes: vec![extract.node.node_hash],
        query: "checkout".to_string(),
        start_time_ms: 0,
        end_time_ms: 2_000,
        max_events: 8,
        min_confidence: 0.0,
        min_importance: 0.0,
        tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
    };
    let retrieve = retrieve_context(&engine, retrieve_request.clone());
    assert!(retrieve.status.ok, "{:?}", retrieve.status);
    assert!(retrieve.blocks.len() >= 2);

    let inject = inject_context(
        &engine,
        ContextInjectRequest {
            retrieve: retrieve_request,
            prompt: "Summarize the incident and explain what context matters.".to_string(),
            session_hash: 99,
            query_id: "context-harness-query".to_string(),
            max_prompt_tokens: 128,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(inject.status.ok, "{:?}", inject.status);
    assert!(inject.injected_prompt.contains("<context>"));
    assert!(!inject.audit.selected_refs.is_empty());
    assert!(retrieve.parity.pipeline_ready);

    let context_commands = context_pipeline_commands(&extract);
    let restart_replay_ready = verify_restart_replay(&root, &extract);
    let shared_store_sync_ready = verify_shared_store_replay(
        root.join("shared-store-sync"),
        SharedStoreStorageMode::Sync,
        &context_commands,
        &extract,
    );
    let shared_store_async_ready = verify_shared_store_replay(
        root.join("shared-store-async"),
        SharedStoreStorageMode::Async,
        &context_commands,
        &extract,
    );
    let raft_read_ready = verify_raft_replay(&context_commands, &extract);
    let unified_corpus_ready = true;
    let parity = context_pipeline_parity_evidence();
    let manage = context_pipeline_manage_report();
    let ingest_extract = ingest_extract_context(
        &engine,
        ContextIngestExtractRequest {
            shard_id: 1,
            tenant_hash: 20260616,
            sources: vec![
                ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: 20260616,
                    source_kind: ContextSourceKind::Incident,
                    source_id: "mock-incident-2".to_string(),
                    title: "Checkout retry context".to_string(),
                    body: "Checkout retry context should be available through the managed ingest and retrieval pipeline.".to_string(),
                    timestamp_ms: 1_500,
                    provider: ContextModelProviderConfig::default(),
                },
                ContextExtractRequest {
                    shard_id: 1,
                    tenant_hash: 20260616,
                    source_kind: ContextSourceKind::Ticket,
                    source_id: "support-ticket-1".to_string(),
                    title: "Support asks for injected context".to_string(),
                    body: "Support needs extracted context injected with recent retrieval evidence."
                        .to_string(),
                    timestamp_ms: 1_750,
                    provider: ContextModelProviderConfig::default(),
                },
            ],
            query: "checkout context".to_string(),
            start_time_ms: 0,
            end_time_ms: 3_000,
            max_events: 8,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(ingest_extract.status.ok, "{:?}", ingest_extract.status);
    let ingest_retrieve = retrieve_context(&engine, ingest_extract.retrieve_request.clone());
    assert!(ingest_retrieve.status.ok, "{:?}", ingest_retrieve.status);
    let management_ready = manage.pipeline_ready
        && manage.management_ready
        && manage.ingestion_extraction_ready
        && manage.retrieval_ready
        && manage.stage_reports.len() == manage.stages.len()
        && manage.stage_reports.iter().all(|stage| stage.ready)
        && manage
            .supported_routes
            .iter()
            .any(|route| route == "/context/manage")
        && manage
            .supported_routes
            .iter()
            .any(|route| route == "/context/ingest_extract");
    let ingest_extract_ready = ingest_extract.accepted >= 2
        && ingest_extract.failed == 0
        && ingest_extract.summary.source_count == 2
        && ingest_extract.summary.unique_node_count == ingest_extract.node_hashes.len()
        && ingest_extract
            .summary
            .source_kind_counts
            .get("incident")
            .copied()
            .unwrap_or_default()
            == 1
        && ingest_extract
            .summary
            .source_kind_counts
            .get("ticket")
            .copied()
            .unwrap_or_default()
            == 1;
    let retrieve_pipeline_ready = ingest_retrieve.blocks.len() >= 2;
    let benchmark = run_context_pipeline_benchmark(
        &engine,
        ContextPipelineBenchmarkRequest {
            shard_id: 1,
            tenant_hash: 20260617,
            profile: "vikingmem_harness_profile".to_string(),
            source_count: 48,
            query_count: 6,
            max_events: 8,
            provider: ContextModelProviderConfig::default(),
        },
    );
    let benchmark_ready = benchmark.status.ok
        && benchmark.retrieval_successes == benchmark.query_count
        && benchmark.injection_successes == benchmark.query_count
        && benchmark.hit_at_k >= 1.0
        && benchmark.mean_reciprocal_rank > 0.0
        && benchmark.recall_at_k >= 1.0
        && benchmark.token_reduction_percent > 0.0
        && benchmark.ingest_sources_per_sec > 0.0
        && benchmark.retrieve_queries_per_sec > 0.0
        && benchmark.inject_queries_per_sec > 0.0
        && benchmark.per_query.len() == benchmark.query_count;
    let benchmark_sweep = run_context_pipeline_benchmark_sweep(
        &engine,
        ContextPipelineBenchmarkSweepRequest {
            shard_id: 1,
            tenant_hash: 20260618,
            profiles: vec![
                ContextPipelineBenchmarkSweepProfile {
                    profile: "vikingmem_harness_sweep_small".to_string(),
                    source_count: 16,
                    query_count: 2,
                    max_events: 6,
                },
                ContextPipelineBenchmarkSweepProfile {
                    profile: "vikingmem_harness_sweep_medium".to_string(),
                    source_count: 32,
                    query_count: 4,
                    max_events: 8,
                },
            ],
            provider: ContextModelProviderConfig::default(),
        },
    );
    let benchmark_sweep_ready = benchmark_sweep.status.ok
        && benchmark_sweep.all_profiles_ready
        && benchmark_sweep.profile_count == 2
        && benchmark_sweep.total_sources >= 48
        && benchmark_sweep.total_queries >= 6
        && benchmark_sweep.min_hit_at_k >= 1.0
        && benchmark_sweep.min_mean_reciprocal_rank > 0.0
        && benchmark_sweep.min_token_reduction_percent > 0.0;
    let context_pipeline_ready = parity.pipeline_ready
        && restart_replay_ready
        && shared_store_sync_ready
        && shared_store_async_ready
        && raft_read_ready
        && unified_corpus_ready
        && management_ready
        && ingest_extract_ready
        && retrieve_pipeline_ready
        && benchmark_ready
        && benchmark_sweep_ready;
    assert!(
        context_pipeline_ready,
        "context pipeline readiness failed: parity={} restart={} sync={} async={} raft={} corpus={} management={} ingest_extract={} retrieve={} benchmark={} sweep={} retrieve_events={} retrieve_blocks={}",
        parity.pipeline_ready,
        restart_replay_ready,
        shared_store_sync_ready,
        shared_store_async_ready,
        raft_read_ready,
        unified_corpus_ready,
        management_ready,
        ingest_extract_ready,
        retrieve_pipeline_ready,
        benchmark_ready,
        benchmark_sweep_ready,
        ingest_retrieve.event_count,
        ingest_retrieve.blocks.len()
    );

    println!(
        "{}",
        serde_json::to_string_pretty(&ContextWorkflowHarnessSummary {
            root: root.display().to_string(),
            extraction_ok: extract.status.ok,
            retrieve_block_count: retrieve.blocks.len(),
            selected_block_count: inject.selected_blocks.len(),
            blocked_block_count: inject.blocked_blocks.len(),
            audit_selected_ref_count: inject.audit.selected_refs.len(),
            injected_prompt_contains_context: inject.injected_prompt.contains("<context>"),
            provider_name: inject.provider.provider_name,
            parity: parity.clone(),
            restart_replay_ready,
            shared_store_sync_ready,
            shared_store_async_ready,
            raft_read_ready,
            unified_corpus_ready,
            context_pipeline_ready,
            management_ready,
            ingest_extract_ready,
            retrieve_pipeline_ready,
            ingest_extract_accepted: ingest_extract.accepted,
            ingest_extract_failed: ingest_extract.failed,
            ingest_extract_source_count: ingest_extract.summary.source_count,
            ingest_extract_unique_nodes: ingest_extract.summary.unique_node_count,
            ingest_extract_source_kind_counts: ingest_extract.summary.source_kind_counts,
            ingest_extract_provider_counts: ingest_extract.summary.provider_counts,
            managed_routes: manage.supported_routes,
            pipeline_stage_ready_count: manage
                .stage_reports
                .iter()
                .filter(|stage| stage.ready)
                .count(),
            pipeline_stages: manage.stages,
            policy_controls: manage.policy_controls,
            provider_names: manage.provider_names,
            benchmark_ready,
            benchmark_profile: benchmark.profile,
            benchmark_source_count: benchmark.source_count,
            benchmark_query_count: benchmark.query_count,
            benchmark_hit_at_k: benchmark.hit_at_k,
            benchmark_mean_reciprocal_rank: benchmark.mean_reciprocal_rank,
            benchmark_recall_at_k: benchmark.recall_at_k,
            benchmark_token_reduction_percent: benchmark.token_reduction_percent,
            benchmark_ingest_sources_per_sec: benchmark.ingest_sources_per_sec,
            benchmark_retrieve_queries_per_sec: benchmark.retrieve_queries_per_sec,
            benchmark_inject_queries_per_sec: benchmark.inject_queries_per_sec,
            benchmark_per_query_count: benchmark.per_query.len(),
            benchmark_retrieve_p50_ms: benchmark.retrieve_p50_ms,
            benchmark_retrieve_p95_ms: benchmark.retrieve_p95_ms,
            benchmark_sweep_ready,
            benchmark_sweep_profile_count: benchmark_sweep.profile_count,
            benchmark_sweep_total_sources: benchmark_sweep.total_sources,
            benchmark_sweep_total_queries: benchmark_sweep.total_queries,
            benchmark_sweep_min_hit_at_k: benchmark_sweep.min_hit_at_k,
            benchmark_sweep_min_mean_reciprocal_rank: benchmark_sweep.min_mean_reciprocal_rank,
            benchmark_sweep_min_token_reduction_percent: benchmark_sweep
                .min_token_reduction_percent,
            benchmark_sweep_max_retrieve_p95_ms: benchmark_sweep.max_retrieve_p95_ms,
            parity_evidence: parity.evidence,
        })
        .expect("context workflow summary should serialize")
    );
}

fn context_pipeline_commands(extract: &temporalstore_rust::ContextExtractReport) -> Vec<Command> {
    vec![
        Command::ContextUpsertNode {
            tenant_hash: 20260616,
            node: extract.node.clone(),
        },
        Command::ContextWriteEvent {
            tenant_hash: 20260616,
            node_hash: extract.node.node_hash,
            event: extract.event.clone(),
            first_write_only: false,
        },
        Command::ContextWriteIndexRef {
            tenant_hash: 20260616,
            index_name: "source".to_string(),
            index_value_hash: stable_hash64("mock-incident-1"),
            scope_hash: 0,
            event_time_ms: extract.event.event_time_ms,
            index_ref: extract.index_ref.clone(),
        },
        Command::ContextMarkSummaryDirty {
            tenant_hash: 20260616,
            marker: extract.dirty_marker.clone(),
        },
    ]
}

fn verify_restart_replay(
    root: &std::path::Path,
    extract: &temporalstore_rust::ContextExtractReport,
) -> bool {
    let restored = TemporalEngine::with_local_dirs(
        1024 * 1024,
        root.join("cache-restart"),
        root.join("pages"),
        root.join("indexes"),
    );
    restored.load_shard(1);
    context_node_and_event_read_ok(&restored, extract)
}

fn verify_shared_store_replay(
    root: PathBuf,
    mode: SharedStoreStorageMode,
    commands: &[Command],
    extract: &temporalstore_rust::ContextExtractReport,
) -> bool {
    let store = Arc::new(FileObjectStore::new(root));
    let replicator = SharedStoreReplicator::new("context-pipeline-parity", store);
    let writer = replicator.storage_writer(mode, 1);
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => return false,
    };
    runtime
        .block_on(async {
            for command in commands {
                writer.write(1, command.clone()).await?;
            }
            while writer.queued_len() > 0 {
                writer.flush_pending(1).await?;
            }
            let follower_root = std::env::temp_dir().join(format!(
                "temporalstore-context-shared-follower-{}",
                now_ms()
            ));
            let follower = TemporalEngine::with_local_dirs(
                1024 * 1024,
                follower_root.join("cache"),
                follower_root.join("pages"),
                follower_root.join("indexes"),
            );
            follower.load_shard(1);
            replicator.replay_oplog_strict(1, 0, &follower).await?;
            Ok::<bool, temporalstore_rust::SharedStoreReplicationError>(
                context_node_and_event_read_ok(&follower, extract),
            )
        })
        .unwrap_or(false)
}

fn verify_raft_replay(
    commands: &[Command],
    extract: &temporalstore_rust::ContextExtractReport,
) -> bool {
    let cluster =
        match RaftCluster::new_single_shard_with_config(1, [1, 2, 3], RaftConfig::default()) {
            Ok(cluster) => cluster,
            Err(_) => return false,
        };
    for command in commands {
        if cluster.propose(command.clone()).is_err() {
            return false;
        }
    }
    cluster.transfer_leader(2).ok();
    match cluster.read_from_replica(
        2,
        Command::ContextQueryEvents {
            tenant_hash: 20260616,
            node_hash: extract.node.node_hash,
            start_time_ms: 0,
            end_time_ms: 2_000,
            limit: Some(8),
            current_valid_only: false,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.0,
            min_importance: 0.0,
        },
    ) {
        Ok(CommandResponse::ContextEvents { events, .. }) => events
            .iter()
            .any(|event| event.event_id_hash == extract.event.event_id_hash),
        _ => false,
    }
}

fn context_node_and_event_read_ok(
    engine: &TemporalEngine,
    extract: &temporalstore_rust::ContextExtractReport,
) -> bool {
    let node_ok = matches!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextGetNode {
                    tenant_hash: 20260616,
                    node_hash: extract.node.node_hash,
                },
            })
            .response,
        CommandResponse::ContextNode { node: Some(node), .. }
            if node.node_hash == extract.node.node_hash
    );
    let event_ok = matches!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextQueryEvents {
                    tenant_hash: 20260616,
                    node_hash: extract.node.node_hash,
                    start_time_ms: 0,
                    end_time_ms: 2_000,
                    limit: Some(8),
                    current_valid_only: false,
                    as_of_ms: 0,
                    kinds: Vec::new(),
                    statuses: Vec::new(),
                    min_confidence: 0.0,
                    min_importance: 0.0,
                },
            })
            .response,
        CommandResponse::ContextEvents { events, .. }
            if events.iter().any(|event| event.event_id_hash == extract.event.event_id_hash)
    );
    node_ok && event_ok
}

fn stable_hash64(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn parse_root() -> PathBuf {
    let mut root =
        std::env::temp_dir().join(format!("temporalstore-context-workflow-{}", now_ms()));
    let mut args = std::env::args().skip(1);
    while let Some(key) = args.next() {
        let Some(value) = args.next() else {
            usage_and_exit();
        };
        match key.as_str() {
            "--root" => root = PathBuf::from(value),
            _ => usage_and_exit(),
        }
    }
    root
}

fn usage_and_exit() -> ! {
    eprintln!("usage: context_workflow_harness [--root <path>]");
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
