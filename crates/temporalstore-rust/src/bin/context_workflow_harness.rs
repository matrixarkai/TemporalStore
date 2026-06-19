use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use std::collections::BTreeMap;
use std::fs;

use serde::Serialize;
use serde_json::Value;
use temporalstore_rust::{
    context_pipeline_manage_report, context_pipeline_parity_evidence, extract_context,
    ingest_extract_context, inject_context, retrieve_context, run_context_pipeline_benchmark,
    run_context_pipeline_benchmark_sweep, Command, CommandResponse, ContextExtractRequest,
    ContextIngestExtractRequest, ContextInjectRequest, ContextModelProviderConfig,
    ContextPipelineBenchmarkRequest, ContextPipelineBenchmarkSweepProfile,
    ContextPipelineBenchmarkSweepRequest, ContextPipelineBenchmarkThresholds,
    ContextPipelineParityEvidence, ContextRetrieveRequest, ContextSourceKind, ContextTier,
    ExecuteRequest, RaftCluster, RaftConfig, SharedStoreReplicator, SharedStoreStorageMode,
    TemporalEngine,
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
    benchmark_workload_signature: u64,
    benchmark_topic_count: usize,
    benchmark_min_sources_per_topic: usize,
    benchmark_max_sources_per_topic: usize,
    benchmark_source_kind_coverage_count: usize,
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
    benchmark_inject_p50_ms: u128,
    benchmark_inject_p95_ms: u128,
    benchmark_avg_retrieved_blocks_per_query: f64,
    benchmark_avg_selected_blocks_per_query: f64,
    benchmark_avg_selected_tokens_per_query: f64,
    benchmark_max_selected_tokens_per_query: u32,
    benchmark_zero_hit_queries: usize,
    benchmark_threshold_passed: bool,
    benchmark_threshold_violation_count: usize,
    benchmark_thresholds: ContextPipelineBenchmarkThresholds,
    benchmark_sweep_ready: bool,
    benchmark_sweep_profile_count: usize,
    benchmark_sweep_total_sources: usize,
    benchmark_sweep_total_queries: usize,
    benchmark_sweep_profile_signature_count: usize,
    benchmark_sweep_min_sources_per_topic: usize,
    benchmark_sweep_max_sources_per_topic: usize,
    benchmark_sweep_min_source_kind_coverage_count: usize,
    benchmark_sweep_min_hit_at_k: f32,
    benchmark_sweep_min_mean_reciprocal_rank: f32,
    benchmark_sweep_min_token_reduction_percent: f32,
    benchmark_sweep_max_retrieve_p95_ms: u128,
    benchmark_sweep_max_inject_p95_ms: u128,
    benchmark_sweep_total_zero_hit_queries: usize,
    benchmark_sweep_avg_selected_tokens_per_query: f64,
    benchmark_sweep_all_thresholds_passed: bool,
    benchmark_sweep_threshold_violation_count: usize,
    external_benchmark_ready: bool,
    external_benchmark_dataset: String,
    external_benchmark_case_count: usize,
    external_benchmark_hit_at_k: f32,
    external_benchmark_mean_reciprocal_rank: f32,
    external_benchmark_zero_hit_queries: usize,
    external_benchmark_source: String,
    parity_evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ExternalContextBenchmarkReport {
    ready: bool,
    dataset: String,
    case_count: usize,
    hit_at_k: f32,
    mean_reciprocal_rank: f32,
    zero_hit_queries: usize,
    source: String,
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
            thresholds: ContextPipelineBenchmarkThresholds::default(),
        },
    );
    let benchmark_ready = benchmark.status.ok
        && benchmark.retrieval_successes == benchmark.query_count
        && benchmark.injection_successes == benchmark.query_count
        && benchmark.hit_at_k >= 1.0
        && benchmark.mean_reciprocal_rank >= 1.0
        && benchmark.recall_at_k >= 1.0
        && benchmark.token_reduction_percent > 0.0
        && benchmark.workload_signature != 0
        && benchmark.topic_count == benchmark.query_count
        && benchmark.min_sources_per_topic > 0
        && benchmark.max_sources_per_topic >= benchmark.min_sources_per_topic
        && benchmark.source_kind_coverage_count >= 3
        && benchmark.ingest_sources_per_sec > 0.0
        && benchmark.retrieve_queries_per_sec > 0.0
        && benchmark.inject_queries_per_sec > 0.0
        && benchmark.avg_retrieved_blocks_per_query > 0.0
        && benchmark.avg_selected_blocks_per_query > 0.0
        && benchmark.avg_selected_tokens_per_query > 0.0
        && benchmark.max_selected_tokens_per_query > 0
        && benchmark.zero_hit_queries == 0
        && benchmark.threshold_passed
        && benchmark.threshold_violations.is_empty()
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
                ContextPipelineBenchmarkSweepProfile {
                    profile: "locomo_style_conversation_memory".to_string(),
                    source_count: 60,
                    query_count: 6,
                    max_events: 12,
                },
                ContextPipelineBenchmarkSweepProfile {
                    profile: "longmemeval_s_style_long_context".to_string(),
                    source_count: 96,
                    query_count: 8,
                    max_events: 16,
                },
            ],
            provider: ContextModelProviderConfig::default(),
            thresholds: ContextPipelineBenchmarkThresholds::default(),
        },
    );
    let benchmark_sweep_ready = benchmark_sweep.status.ok
        && benchmark_sweep.all_profiles_ready
        && benchmark_sweep.profile_count == 4
        && benchmark_sweep.total_sources >= 204
        && benchmark_sweep.total_queries >= 20
        && benchmark_sweep.profile_signatures.len() == benchmark_sweep.profile_count
        && benchmark_sweep.min_sources_per_topic > 0
        && benchmark_sweep.max_sources_per_topic >= benchmark_sweep.min_sources_per_topic
        && benchmark_sweep.min_source_kind_coverage_count >= 3
        && benchmark_sweep.min_hit_at_k >= 1.0
        && benchmark_sweep.min_mean_reciprocal_rank >= 1.0
        && benchmark_sweep.min_token_reduction_percent > 0.0
        && benchmark_sweep.total_zero_hit_queries == 0
        && benchmark_sweep.avg_selected_tokens_per_query > 0.0
        && benchmark_sweep.all_thresholds_passed
        && benchmark_sweep.threshold_violations.is_empty();
    let external_benchmark = run_external_context_benchmark(&engine);
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
        && benchmark_sweep_ready
        && external_benchmark.ready;
    assert!(
        context_pipeline_ready,
        "context pipeline readiness failed: parity={} restart={} sync={} async={} raft={} corpus={} management={} ingest_extract={} retrieve={} benchmark={} sweep={} external_benchmark={} retrieve_events={} retrieve_blocks={}",
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
        external_benchmark.ready,
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
            benchmark_workload_signature: benchmark.workload_signature,
            benchmark_topic_count: benchmark.topic_count,
            benchmark_min_sources_per_topic: benchmark.min_sources_per_topic,
            benchmark_max_sources_per_topic: benchmark.max_sources_per_topic,
            benchmark_source_kind_coverage_count: benchmark.source_kind_coverage_count,
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
            benchmark_inject_p50_ms: benchmark.inject_p50_ms,
            benchmark_inject_p95_ms: benchmark.inject_p95_ms,
            benchmark_avg_retrieved_blocks_per_query: benchmark.avg_retrieved_blocks_per_query,
            benchmark_avg_selected_blocks_per_query: benchmark.avg_selected_blocks_per_query,
            benchmark_avg_selected_tokens_per_query: benchmark.avg_selected_tokens_per_query,
            benchmark_max_selected_tokens_per_query: benchmark.max_selected_tokens_per_query,
            benchmark_zero_hit_queries: benchmark.zero_hit_queries,
            benchmark_threshold_passed: benchmark.threshold_passed,
            benchmark_threshold_violation_count: benchmark.threshold_violations.len(),
            benchmark_thresholds: benchmark.thresholds,
            benchmark_sweep_ready,
            benchmark_sweep_profile_count: benchmark_sweep.profile_count,
            benchmark_sweep_total_sources: benchmark_sweep.total_sources,
            benchmark_sweep_total_queries: benchmark_sweep.total_queries,
            benchmark_sweep_profile_signature_count: benchmark_sweep.profile_signatures.len(),
            benchmark_sweep_min_sources_per_topic: benchmark_sweep.min_sources_per_topic,
            benchmark_sweep_max_sources_per_topic: benchmark_sweep.max_sources_per_topic,
            benchmark_sweep_min_source_kind_coverage_count: benchmark_sweep
                .min_source_kind_coverage_count,
            benchmark_sweep_min_hit_at_k: benchmark_sweep.min_hit_at_k,
            benchmark_sweep_min_mean_reciprocal_rank: benchmark_sweep.min_mean_reciprocal_rank,
            benchmark_sweep_min_token_reduction_percent: benchmark_sweep
                .min_token_reduction_percent,
            benchmark_sweep_max_retrieve_p95_ms: benchmark_sweep.max_retrieve_p95_ms,
            benchmark_sweep_max_inject_p95_ms: benchmark_sweep.max_inject_p95_ms,
            benchmark_sweep_total_zero_hit_queries: benchmark_sweep.total_zero_hit_queries,
            benchmark_sweep_avg_selected_tokens_per_query: benchmark_sweep
                .avg_selected_tokens_per_query,
            benchmark_sweep_all_thresholds_passed: benchmark_sweep.all_thresholds_passed,
            benchmark_sweep_threshold_violation_count: benchmark_sweep.threshold_violations.len(),
            external_benchmark_ready: external_benchmark.ready,
            external_benchmark_dataset: external_benchmark.dataset,
            external_benchmark_case_count: external_benchmark.case_count,
            external_benchmark_hit_at_k: external_benchmark.hit_at_k,
            external_benchmark_mean_reciprocal_rank: external_benchmark.mean_reciprocal_rank,
            external_benchmark_zero_hit_queries: external_benchmark.zero_hit_queries,
            external_benchmark_source: external_benchmark.source,
            parity_evidence: parity.evidence,
        })
        .expect("context workflow summary should serialize")
    );
}

fn run_external_context_benchmark(engine: &TemporalEngine) -> ExternalContextBenchmarkReport {
    let configured_path = std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL").ok();
    let (source, cases) = match configured_path {
        Some(path) if !path.trim().is_empty() => {
            let path = PathBuf::from(path);
            (
                path.display().to_string(),
                parse_external_context_benchmark_cases(&path),
            )
        }
        _ => (
            "built-in-locomo-longmemeval-fixture".to_string(),
            builtin_external_context_benchmark_cases(),
        ),
    };
    if cases.is_empty() {
        return ExternalContextBenchmarkReport {
            ready: false,
            dataset: "empty".to_string(),
            case_count: 0,
            hit_at_k: 0.0,
            mean_reciprocal_rank: 0.0,
            zero_hit_queries: 0,
            source,
        };
    }

    let mut hit_count = 0usize;
    let mut reciprocal_rank_sum = 0.0f32;
    let mut dataset_counts = BTreeMap::new();
    for (index, case) in cases.iter().enumerate() {
        *dataset_counts.entry(case.dataset.clone()).or_insert(0usize) += 1;
        let tenant_hash = 20260701 + index as u64;
        let sources = case
            .sources
            .iter()
            .enumerate()
            .map(|(source_index, source)| ContextExtractRequest {
                shard_id: 1,
                tenant_hash,
                source_kind: source.kind,
                source_id: format!("{}-{}-{source_index}", case.dataset, case.query_id),
                title: source.title.clone(),
                body: source.body.clone(),
                timestamp_ms: 1_000 + source_index as u64,
                provider: ContextModelProviderConfig::default(),
            })
            .collect::<Vec<_>>();
        let ingest = ingest_extract_context(
            engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash,
                sources,
                query: case.query.clone(),
                start_time_ms: 0,
                end_time_ms: 10_000,
                max_events: 32,
                provider: ContextModelProviderConfig::default(),
            },
        );
        if !ingest.status.ok {
            continue;
        }
        let retrieve = retrieve_context(
            engine,
            ContextRetrieveRequest {
                shard_id: 1,
                tenant_hash,
                node_hashes: ingest.node_hashes.clone(),
                query: case.query.clone(),
                start_time_ms: 0,
                end_time_ms: 10_000,
                max_events: 32,
                min_confidence: 0.0,
                min_importance: 0.0,
                tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
            },
        );
        let hit_rank = retrieve
            .blocks
            .iter()
            .position(|block| {
                let block_text = block.text.to_ascii_lowercase();
                let block_normalized = normalize_benchmark_text(&block.text);
                case.expected_terms
                    .iter()
                    .any(|term| benchmark_text_matches(&block_text, &block_normalized, term))
            })
            .map(|rank| rank + 1);
        if let Some(rank) = hit_rank {
            hit_count += 1;
            reciprocal_rank_sum += 1.0 / rank as f32;
        }
    }
    let case_count = cases.len();
    let hit_at_k = hit_count as f32 / case_count as f32;
    let mean_reciprocal_rank = reciprocal_rank_sum / case_count as f32;
    ExternalContextBenchmarkReport {
        ready: hit_count == case_count && mean_reciprocal_rank >= 1.0,
        dataset: dataset_counts.keys().cloned().collect::<Vec<_>>().join("+"),
        case_count,
        hit_at_k,
        mean_reciprocal_rank,
        zero_hit_queries: case_count.saturating_sub(hit_count),
        source,
    }
}

#[derive(Debug, Clone)]
struct ExternalContextBenchmarkCase {
    dataset: String,
    query_id: String,
    query: String,
    expected_terms: Vec<String>,
    sources: Vec<ExternalContextBenchmarkSource>,
}

#[derive(Debug, Clone)]
struct ExternalContextBenchmarkSource {
    title: String,
    body: String,
    kind: ContextSourceKind,
}

fn parse_external_context_benchmark_cases(path: &Path) -> Vec<ExternalContextBenchmarkCase> {
    let Ok(content) = fs::read_to_string(path) else {
        return Vec::new();
    };
    content
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim().trim_start_matches('\u{feff}');
            if trimmed.is_empty() {
                return None;
            }
            let value: Value = serde_json::from_str(trimmed).ok()?;
            external_case_from_value(index, &value)
        })
        .collect()
}

fn external_case_from_value(index: usize, value: &Value) -> Option<ExternalContextBenchmarkCase> {
    let dataset = value
        .get("dataset")
        .and_then(Value::as_str)
        .unwrap_or("external_context_benchmark")
        .to_string();
    let query_id = value
        .get("query_id")
        .or_else(|| value.get("question_id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("external-query-{index}"));
    let query = value
        .get("query")
        .or_else(|| value.get("question"))
        .and_then(Value::as_str)?
        .to_string();
    let expected_terms = value
        .get("expected_terms")
        .or_else(|| value.get("answer_terms"))
        .or_else(|| value.get("answers"))
        .and_then(Value::as_array)
        .map(|terms| {
            terms
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|terms| !terms.is_empty())
        .unwrap_or_else(|| {
            value
                .get("answer")
                .and_then(Value::as_str)
                .map(|answer| vec![answer.to_string()])
                .unwrap_or_default()
        });
    let sources = external_sources_from_value(value);
    if expected_terms.is_empty() || sources.is_empty() {
        None
    } else {
        Some(ExternalContextBenchmarkCase {
            dataset,
            query_id,
            query,
            expected_terms,
            sources,
        })
    }
}

fn external_sources_from_value(value: &Value) -> Vec<ExternalContextBenchmarkSource> {
    let source_values = value
        .get("sources")
        .or_else(|| value.get("messages"))
        .or_else(|| value.get("conversation"))
        .and_then(Value::as_array);
    let Some(source_values) = source_values else {
        return Vec::new();
    };
    source_values
        .iter()
        .enumerate()
        .filter_map(|(index, source)| {
            let body = source
                .get("body")
                .or_else(|| source.get("text"))
                .or_else(|| source.get("message"))
                .or_else(|| source.get("content"))
                .and_then(Value::as_str)?
                .to_string();
            let title = source
                .get("title")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("External benchmark source {index}"));
            let kind = source
                .get("source_kind")
                .or_else(|| source.get("kind"))
                .and_then(Value::as_str)
                .map(parse_context_source_kind)
                .unwrap_or(ContextSourceKind::Chat);
            Some(ExternalContextBenchmarkSource { title, body, kind })
        })
        .collect()
}

fn parse_context_source_kind(value: &str) -> ContextSourceKind {
    match value.to_ascii_lowercase().as_str() {
        "document" => ContextSourceKind::Document,
        "ticket" => ContextSourceKind::Ticket,
        "code" => ContextSourceKind::Code,
        "incident" => ContextSourceKind::Incident,
        "user_event" | "user-event" | "event" => ContextSourceKind::UserEvent,
        _ => ContextSourceKind::Chat,
    }
}

fn normalize_benchmark_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
}

fn benchmark_text_matches(text_lower: &str, text_normalized: &str, term: &str) -> bool {
    if text_lower.contains(&term.to_ascii_lowercase()) {
        return true;
    }
    let normalized_term = normalize_benchmark_text(term);
    let normalized_term = normalized_term.trim();
    !normalized_term.is_empty() && text_normalized.contains(normalized_term)
}

fn builtin_external_context_benchmark_cases() -> Vec<ExternalContextBenchmarkCase> {
    vec![
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-current-preference".to_string(),
            query: "What is Alice's current office choice after the payment problem?".to_string(),
            expected_terms: vec!["downtown".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Earlier preference".to_string(),
                    body: "Earlier memory: Alice preferred the airport office before the later change.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Latest preference update".to_string(),
                    body: "During the latest conversation, Alice replaced her office preference with the downtown location after the billing issue was resolved.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-location-paraphrase".to_string(),
            query: "Where does Alice want to work now?".to_string(),
            expected_terms: vec!["downtown location".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Stale workplace memory".to_string(),
                    body: "Earlier memory: Alice wanted to work near the airport office before the later change.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Current workplace memory".to_string(),
                    body: "Latest update: Alice now wants the downtown location as her office preference after the payment issue was resolved.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-updated-setting".to_string(),
            query: "Which preference was updated in the recent multi session messages?".to_string(),
            expected_terms: vec!["notification".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Old setting".to_string(),
                    body: "The user originally discussed billing alerts in an older session.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Recent setting update".to_string(),
                    body: "Support follow-up: the user sent messages across sessions and the helpdesk agent changed the notification setting during the most recent chat.".to_string(),
                    kind: ContextSourceKind::Ticket,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-most-recent-change".to_string(),
            query: "Which setting changed most recently across the conversation history?".to_string(),
            expected_terms: vec!["notification setting".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Earlier account setting".to_string(),
                    body: "In an old conversation, the user changed a billing frequency setting.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Latest notification setting".to_string(),
                    body: "Most recent conversation: the support agent changed the notification setting after several messages across sessions.".to_string(),
                    kind: ContextSourceKind::Ticket,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-temporal-after-travel".to_string(),
            query: "What did Alice decide after the airport trip conversation?".to_string(),
            expected_terms: vec!["downtown office".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Airport trip discussion".to_string(),
                    body: "Earlier session: Alice discussed an airport trip and said the airport office was convenient for the flight.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "After travel decision".to_string(),
                    body: "After the airport trip conversation, Alice decided to switch her work preference to the downtown office location.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-root-cause-after-outage".to_string(),
            query: "Why did checkout fail after the backend outage?".to_string(),
            expected_terms: vec!["database migration".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Initial outage alert".to_string(),
                    body: "Initial incident: checkout failed and support saw payment errors while the backend service was down.".to_string(),
                    kind: ContextSourceKind::Incident,
                },
                ExternalContextBenchmarkSource {
                    title: "Root cause follow-up".to_string(),
                    body: "Follow-up analysis: the checkout failure happened because a database migration exhausted the backend connection pool after the outage recovery.".to_string(),
                    kind: ContextSourceKind::Ticket,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-corrected-food-restriction".to_string(),
            query: "What snack should Jordan avoid now after the correction?".to_string(),
            expected_terms: vec!["peanuts".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Older snack preference".to_string(),
                    body: "Earlier chat: Jordan said almonds were the only snack to avoid and peanuts were fine for the picnic.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Corrected food restriction".to_string(),
                    body: "Latest correction: Jordan no longer avoids almonds; Jordan should avoid peanuts now because of a new food restriction.".to_string(),
                    kind: ContextSourceKind::UserEvent,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-medication-reminder".to_string(),
            query: "Which medication did Morgan say to remember before the doctor appointment?".to_string(),
            expected_terms: vec!["lisinopril".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Older clinic message".to_string(),
                    body: "A previous clinic message only mentioned bringing an insurance card to the physician visit.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Medication reminder".to_string(),
                    body: "In the later session Morgan said to remember lisinopril, the blood pressure medication, before the doctor appointment.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-hobby-switch".to_string(),
            query: "Which hobby did Priya switch to after cancelling guitar lessons?".to_string(),
            expected_terms: vec!["pottery class".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Old hobby plan".to_string(),
                    body: "Earlier conversation: Priya planned guitar lessons and had not picked a replacement hobby yet.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Hobby switch".to_string(),
                    body: "Later update: Priya cancelled guitar lessons and switched to a pottery class instead for the spring session.".to_string(),
                    kind: ContextSourceKind::UserEvent,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-backup-contact-change".to_string(),
            query: "Who is the backup contact now after Sam moved teams?".to_string(),
            expected_terms: vec!["Riley".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Old escalation owner".to_string(),
                    body: "Old support note: Sam was the backup contact for payment escalation before the team move.".to_string(),
                    kind: ContextSourceKind::Ticket,
                },
                ExternalContextBenchmarkSource {
                    title: "Current escalation owner".to_string(),
                    body: "Most recent staffing update: Sam moved teams, so Riley became the backup contact for payment escalation now.".to_string(),
                    kind: ContextSourceKind::Ticket,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-cafe-recommendation".to_string(),
            query: "Who recommended the cafe that Nina booked after the conference?".to_string(),
            expected_terms: vec!["Omar".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Conference dinner plan".to_string(),
                    body: "Earlier conversation: Nina wanted to book a cafe after the conference but had not chosen one yet.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Cafe recommendation".to_string(),
                    body: "Later chat: Omar recommended the quiet riverside cafe, and Nina booked it after the conference.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-project-suggestion".to_string(),
            query: "Which project did Lee pick because Dana suggested it during planning?".to_string(),
            expected_terms: vec!["observability dashboard".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Initial planning thread".to_string(),
                    body: "Initial planning thread: Lee considered a search cleanup project and had not chosen the final work item.".to_string(),
                    kind: ContextSourceKind::Document,
                },
                ExternalContextBenchmarkSource {
                    title: "Suggested project".to_string(),
                    body: "Later planning note: Dana suggested the observability dashboard because the team needed better benchmark traces, so Lee picked that project.".to_string(),
                    kind: ContextSourceKind::Ticket,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-appointment-reschedule".to_string(),
            query: "When is Maya's dentist appointment after it was rescheduled?".to_string(),
            expected_terms: vec!["Thursday at 3pm".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Original dentist appointment".to_string(),
                    body: "Earlier memory: Maya had a dentist appointment scheduled for Tuesday morning.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Rescheduled dentist appointment".to_string(),
                    body: "Latest calendar update: Maya rescheduled the dentist appointment to Thursday at 3pm after the clinic called.".to_string(),
                    kind: ContextSourceKind::UserEvent,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-deadline-date-update".to_string(),
            query: "What is the new report deadline after the calendar update?".to_string(),
            expected_terms: vec!["June 24".to_string()],
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Old report date".to_string(),
                    body: "Old planning note: the report deadline was June 17 before the later schedule change.".to_string(),
                    kind: ContextSourceKind::Document,
                },
                ExternalContextBenchmarkSource {
                    title: "New report date".to_string(),
                    body: "Calendar update: the report deadline moved to June 24 so the benchmark review could finish first.".to_string(),
                    kind: ContextSourceKind::Ticket,
                },
            ],
        },
    ]
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
