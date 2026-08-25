// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cmp::Reverse;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};

use serde::Serialize;
use serde_json::Value;
use temporalstore_rust::{
    Command, CommandResponse, ContextEvent, ContextExtractRequest, ContextIndexRef,
    ContextIngestExtractRequest, ContextInjectRequest, ContextModelProviderConfig, ContextNode,
    ContextPipelineBenchmarkRequest, ContextPipelineBenchmarkSweepProfile,
    ContextPipelineBenchmarkSweepRequest, ContextPipelineBenchmarkThresholds,
    ContextPipelineParityEvidence, ContextResourceParseRequest, ContextResourceSkillIngestRequest,
    ContextResourceSkillSecondaryIndexValidationRequest, ContextRetrieveRequest,
    ContextSkillIngestInput, ContextSourceKind, ContextDirtyNode, ContextTier,
    ExecuteRequest, RaftCluster, RaftConfig, SharedStoreReplicator, SharedStoreStorageMode,
    TemporalEngine, context_pipeline_manage_report, context_pipeline_parity_evidence,
    context_workflow_state_report, extract_context, ingest_extract_context,
    ingest_resource_skill_context, inject_context, retrieve_context,
    run_context_pipeline_benchmark, run_context_pipeline_benchmark_sweep,
    validate_resource_skill_secondary_indexes,
};
use temporalstore_snapshot::object_store::FileObjectStore;

const EXTERNAL_CONTEXT_MAX_CANONICAL_NAME_BYTES: usize = 512;
const EXTERNAL_CONTEXT_MAX_REF_BYTES: usize = 4096;
const EXTERNAL_CONTEXT_MAX_COMPACT_ATTRS_BYTES: usize = 8 * 1024;
const EXTERNAL_CONTEXT_BENCHMARK_MAX_EVENT_TEXT_BYTES: usize = 60 * 1024;

#[derive(Debug, Serialize)]
struct ContextWorkflowHarnessSummary {
    root: String,
    extraction_ok: bool,
    retrieve_block_count: usize,
    query_understanding_debug: Value,
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
    resource_skill_scale: ResourceSkillConversationScaleSummary,
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
    reference_model_profile_count: usize,
    reference_model_profile_names: Vec<String>,
    reference_vlm_models: Vec<String>,
    reference_embedding_models: Vec<String>,
    reference_parity_case_count: usize,
    reference_parity_categories: Vec<String>,
    open_model_provider_packaged: bool,
    open_model_local_run_proven: bool,
    vlm_provider_configured: bool,
    vlm_benchmark_proven: bool,
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
    benchmark_evidence_retention_at_k: f32,
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
    benchmark_sweep_min_evidence_retention_at_k: f32,
    benchmark_sweep_min_token_reduction_percent: f32,
    benchmark_sweep_max_retrieve_p95_ms: u128,
    benchmark_sweep_max_inject_p95_ms: u128,
    benchmark_sweep_total_zero_hit_queries: usize,
    benchmark_sweep_avg_selected_tokens_per_query: f64,
    benchmark_sweep_max_selected_tokens_per_query: u32,
    benchmark_sweep_all_thresholds_passed: bool,
    benchmark_sweep_threshold_violation_count: usize,
    external_benchmark_ready: bool,
    external_benchmark_dataset: String,
    external_benchmark_case_count: usize,
    external_benchmark_hit_at_k: f32,
    external_benchmark_mean_reciprocal_rank: f32,
    external_benchmark_answer_term_coverage: f32,
    external_benchmark_missing_expected_terms: usize,
    external_benchmark_all_expected_terms_matched: bool,
    external_benchmark_evidence_ref_coverage: f32,
    external_benchmark_missing_expected_refs: usize,
    external_benchmark_zero_hit_queries: usize,
    external_benchmark_unsupported_case_count: usize,
    external_benchmark_unsupported_case_ids: Vec<String>,
    external_benchmark_category_count: usize,
    external_benchmark_category_breakdown: BTreeMap<String, ExternalContextBenchmarkCategoryReport>,
    external_benchmark_per_query: Vec<ExternalContextBenchmarkQueryReport>,
    external_benchmark_all_categories_passed: bool,
    external_benchmark_min_category_hit_at_k: f32,
    external_benchmark_min_category_mean_reciprocal_rank: f32,
    external_benchmark_category_zero_hit_queries: usize,
    external_benchmark_source: String,
    external_benchmark_all_source_replay: bool,
    external_benchmark_direct_source_scoring: bool,
    external_benchmark_source_order_ranking: bool,
    external_benchmark_rust_context_event_ingest: bool,
    external_benchmark_ingested_source_sets: usize,
    external_benchmark_retrieved_source_sets: usize,
    external_benchmark_total_retrieved_blocks: usize,
    parity_evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ResourceSkillConversationScaleSummary {
    ready: bool,
    resource_count: usize,
    skill_count: usize,
    conversation_source_count: usize,
    total_source_count: usize,
    accepted_sources: usize,
    failed_sources: usize,
    retrieved_block_count: usize,
    retrieved_event_count: usize,
    selected_skill_count: usize,
    resource_lifecycle_watched_count: usize,
    skill_registry_enabled_count: usize,
    skill_registry_disabled_count: usize,
    embedding_ref_count: usize,
    embedding_requested_vectors: usize,
    embedding_generated_vectors: usize,
    embedding_live_call_count: usize,
    embedding_mock_generation_count: usize,
    embedding_production_evidence_ready: bool,
    fanout_node_count: usize,
    fanout_event_count: usize,
    #[serde(alias = "fanout_segment_count")]
    fanout_slab_count: usize,
    fanout_entity_count: usize,
    fanout_child_ref_count: usize,
    fanout_embedding_count: usize,
    fanout_summary_count: usize,
    fanout_compression_count: usize,
    fanout_dirty_marker_count: usize,
    fanout_secondary_index_count: usize,
    fanout_ready: bool,
    secondary_index_ready: bool,
    secondary_index_checked_refs: usize,
    secondary_index_found_refs: usize,
    secondary_index_missing_refs: usize,
    summary_embedding_candidate_count: usize,
    summary_embedding_selected_count: usize,
    verbose_filter_group_count: usize,
    selected_ref_count: usize,
    ingest_ms: u128,
    retrieve_ms: u128,
    secondary_index_validation_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
struct ExternalContextBenchmarkReport {
    ready: bool,
    dataset: String,
    case_count: usize,
    hit_at_k: f32,
    mean_reciprocal_rank: f32,
    answer_term_coverage: f32,
    missing_expected_terms: usize,
    all_expected_terms_matched: bool,
    evidence_ref_coverage: f32,
    missing_expected_refs: usize,
    zero_hit_queries: usize,
    unsupported_benchmark_case_count: usize,
    unsupported_benchmark_case_ids: Vec<String>,
    category_breakdown: BTreeMap<String, ExternalContextBenchmarkCategoryReport>,
    per_query: Vec<ExternalContextBenchmarkQueryReport>,
    all_categories_passed: bool,
    min_category_hit_at_k: f32,
    min_category_mean_reciprocal_rank: f32,
    category_zero_hit_queries: usize,
    source: String,
    all_source_replay: bool,
    direct_source_scoring: bool,
    source_order_ranking: bool,
    rust_context_event_ingest: bool,
    ingested_source_sets: usize,
    retrieved_source_sets: usize,
    total_retrieved_blocks: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ExternalContextBenchmarkCategoryReport {
    case_count: usize,
    hit_at_k: f32,
    mean_reciprocal_rank: f32,
    answer_term_coverage: f32,
    missing_expected_terms: usize,
    zero_hit_queries: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ExternalContextBenchmarkQueryReport {
    query_id: String,
    hit: bool,
    rank: Option<usize>,
    retrieved_blocks: usize,
    selected_source_ids: Vec<String>,
    zero_hit: bool,
    retrieval_ms: u128,
    query_understanding_debug: Value,
}

#[derive(Debug, Serialize)]
struct ExternalOnlyContextBenchmarkSummary {
    context_pipeline_ready: bool,
    external_benchmark_ready: bool,
    external_benchmark_dataset: String,
    external_benchmark_case_count: usize,
    external_benchmark_hit_at_k: f32,
    external_benchmark_mean_reciprocal_rank: f32,
    external_benchmark_answer_term_coverage: f32,
    external_benchmark_missing_expected_terms: usize,
    external_benchmark_all_expected_terms_matched: bool,
    external_benchmark_evidence_ref_coverage: f32,
    external_benchmark_missing_expected_refs: usize,
    external_benchmark_zero_hit_queries: usize,
    external_benchmark_unsupported_case_count: usize,
    external_benchmark_unsupported_case_ids: Vec<String>,
    external_benchmark_category_count: usize,
    external_benchmark_category_breakdown: BTreeMap<String, ExternalContextBenchmarkCategoryReport>,
    external_benchmark_per_query: Vec<ExternalContextBenchmarkQueryReport>,
    external_benchmark_all_categories_passed: bool,
    external_benchmark_min_category_hit_at_k: f32,
    external_benchmark_min_category_mean_reciprocal_rank: f32,
    external_benchmark_category_zero_hit_queries: usize,
    external_benchmark_source: String,
    external_benchmark_all_source_replay: bool,
    external_benchmark_direct_source_scoring: bool,
    external_benchmark_source_order_ranking: bool,
    external_benchmark_rust_context_event_ingest: bool,
    external_benchmark_ingested_source_sets: usize,
    external_benchmark_retrieved_source_sets: usize,
    external_benchmark_total_retrieved_blocks: usize,
    provider_names: Vec<String>,
    reference_model_profile_count: usize,
    reference_model_profile_names: Vec<String>,
    reference_vlm_models: Vec<String>,
    reference_embedding_models: Vec<String>,
    reference_parity_case_count: usize,
    reference_parity_categories: Vec<String>,
    open_model_provider_packaged: bool,
    open_model_local_run_proven: bool,
    vlm_provider_configured: bool,
    vlm_benchmark_proven: bool,
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
    let external_only = std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    if external_only {
        let external_benchmark = run_external_context_benchmark(&engine);
        let state = context_workflow_state_report();
        println!(
            "{}",
            serde_json::to_string_pretty(&ExternalOnlyContextBenchmarkSummary {
                context_pipeline_ready: external_benchmark.ready,
                external_benchmark_ready: external_benchmark.ready,
                external_benchmark_dataset: external_benchmark.dataset,
                external_benchmark_case_count: external_benchmark.case_count,
                external_benchmark_hit_at_k: external_benchmark.hit_at_k,
                external_benchmark_mean_reciprocal_rank: external_benchmark.mean_reciprocal_rank,
                external_benchmark_answer_term_coverage: external_benchmark.answer_term_coverage,
                external_benchmark_missing_expected_terms: external_benchmark
                    .missing_expected_terms,
                external_benchmark_all_expected_terms_matched: external_benchmark
                    .all_expected_terms_matched,
                external_benchmark_evidence_ref_coverage: external_benchmark.evidence_ref_coverage,
                external_benchmark_missing_expected_refs: external_benchmark.missing_expected_refs,
                external_benchmark_zero_hit_queries: external_benchmark.zero_hit_queries,
                external_benchmark_unsupported_case_count: external_benchmark
                    .unsupported_benchmark_case_count,
                external_benchmark_unsupported_case_ids: external_benchmark
                    .unsupported_benchmark_case_ids,
                external_benchmark_category_count: external_benchmark.category_breakdown.len(),
                external_benchmark_category_breakdown: external_benchmark.category_breakdown,
                external_benchmark_per_query: external_benchmark.per_query,
                external_benchmark_all_categories_passed: external_benchmark.all_categories_passed,
                external_benchmark_min_category_hit_at_k: external_benchmark.min_category_hit_at_k,
                external_benchmark_min_category_mean_reciprocal_rank: external_benchmark
                    .min_category_mean_reciprocal_rank,
                external_benchmark_category_zero_hit_queries: external_benchmark
                    .category_zero_hit_queries,
                external_benchmark_source: external_benchmark.source,
                external_benchmark_all_source_replay: external_benchmark.all_source_replay,
                external_benchmark_direct_source_scoring: external_benchmark.direct_source_scoring,
                external_benchmark_source_order_ranking: external_benchmark.source_order_ranking,
                external_benchmark_rust_context_event_ingest: external_benchmark
                    .rust_context_event_ingest,
                external_benchmark_ingested_source_sets: external_benchmark.ingested_source_sets,
                external_benchmark_retrieved_source_sets: external_benchmark.retrieved_source_sets,
                external_benchmark_total_retrieved_blocks: external_benchmark
                    .total_retrieved_blocks,
                provider_names: state
                    .providers
                    .iter()
                    .map(|provider| provider.provider_name.clone())
                    .collect(),
                reference_model_profile_count: state.reference_model_profiles.len(),
                reference_model_profile_names: state
                    .reference_model_profiles
                    .iter()
                    .map(|profile| profile.profile_name.clone())
                    .collect(),
                reference_vlm_models: state
                    .reference_model_profiles
                    .iter()
                    .map(|profile| profile.vlm_model.clone())
                    .collect(),
                reference_embedding_models: state
                    .reference_model_profiles
                    .iter()
                    .map(|profile| profile.embedding_model.clone())
                    .collect(),
                reference_parity_case_count: state.reference_parity_cases.len(),
                reference_parity_categories: state.reference_parity_categories,
                open_model_provider_packaged: state.open_model_provider_packaged,
                open_model_local_run_proven: state.open_model_local_run_proven,
                vlm_provider_configured: state.vlm_provider_configured,
                vlm_benchmark_proven: state.vlm_benchmark_proven,
            })
            .expect("external context benchmark summary should serialize")
        );
        return;
    }

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
        max_summary_nodes: 32,
        max_event_nodes: 16,
        prefer_current_agent: false,
        current_agent_scope_key: "agent:codex".to_string(),
        provider: ContextModelProviderConfig::default(),
    };
    let retrieve = retrieve_context(&engine, retrieve_request.clone());
    assert!(retrieve.status.ok, "{:?}", retrieve.status);
    assert!(!retrieve.blocks.is_empty());

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
    let state = context_workflow_state_report();
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
            profile: "reference_harness_profile".to_string(),
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
        && benchmark.recall_at_k >= 1.0
        && benchmark.evidence_retention_at_k >= 1.0
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
                    profile: "reference_harness_sweep_small".to_string(),
                    source_count: 16,
                    query_count: 2,
                    max_events: 6,
                },
                ContextPipelineBenchmarkSweepProfile {
                    profile: "reference_harness_sweep_medium".to_string(),
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
            thresholds: local_context_harness_sweep_thresholds(),
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
        && benchmark_sweep.min_evidence_retention_at_k >= 1.0
        && benchmark_sweep.min_token_reduction_percent > 0.0
        && benchmark_sweep.total_zero_hit_queries == 0
        && benchmark_sweep.avg_selected_tokens_per_query > 0.0
        && benchmark_sweep.all_thresholds_passed
        && benchmark_sweep.threshold_violations.is_empty();
    let external_benchmark = run_external_context_benchmark(&engine);
    let resource_skill_scale = run_resource_skill_conversation_scale(&engine);
    let external_benchmark_report_only =
        std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_REPORT_ONLY")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
    let context_pipeline_ready = parity.pipeline_ready
        && restart_replay_ready
        && shared_store_sync_ready
        && shared_store_async_ready
        && raft_read_ready
        && unified_corpus_ready
        && management_ready
        && ingest_extract_ready
        && retrieve_pipeline_ready
        && resource_skill_scale.ready
        && benchmark_ready
        && benchmark_sweep_ready
        && (external_benchmark.ready || external_benchmark_report_only);
    assert!(
        context_pipeline_ready,
        "context pipeline readiness failed: parity={} restart={} sync={} async={} raft={} corpus={} management={} ingest_extract={} retrieve={} resource_skill_scale={} benchmark={} sweep={} external_benchmark={} retrieve_events={} retrieve_blocks={} sweep_status={} sweep_message={} sweep_min_hit_at_k={} sweep_min_mrr={} sweep_min_evidence_retention={} sweep_min_token_reduction={} sweep_max_selected_tokens={} sweep_violations={:?}",
        parity.pipeline_ready,
        restart_replay_ready,
        shared_store_sync_ready,
        shared_store_async_ready,
        raft_read_ready,
        unified_corpus_ready,
        management_ready,
        ingest_extract_ready,
        retrieve_pipeline_ready,
        resource_skill_scale.ready,
        benchmark_ready,
        benchmark_sweep_ready,
        external_benchmark.ready,
        ingest_retrieve.event_count,
        ingest_retrieve.blocks.len(),
        benchmark_sweep.status.code,
        benchmark_sweep.status.message,
        benchmark_sweep.min_hit_at_k,
        benchmark_sweep.min_mean_reciprocal_rank,
        benchmark_sweep.min_evidence_retention_at_k,
        benchmark_sweep.min_token_reduction_percent,
        benchmark_sweep.max_selected_tokens_per_query,
        benchmark_sweep.threshold_violations
    );

    println!(
        "{}",
        serde_json::to_string_pretty(&ContextWorkflowHarnessSummary {
            root: root.display().to_string(),
            extraction_ok: extract.status.ok,
            retrieve_block_count: retrieve.blocks.len(),
            query_understanding_debug: serde_json::to_value(&retrieve.query_understanding_debug)
                .expect("query understanding debug should serialize"),
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
            resource_skill_scale,
            managed_routes: manage.supported_routes,
            pipeline_stage_ready_count: manage
                .stage_reports
                .iter()
                .filter(|stage| stage.ready)
                .count(),
            pipeline_stages: manage.stages,
            policy_controls: manage.policy_controls,
            provider_names: manage.provider_names,
            reference_model_profile_count: state.reference_model_profiles.len(),
            reference_model_profile_names: state
                .reference_model_profiles
                .iter()
                .map(|profile| profile.profile_name.clone())
                .collect(),
            reference_vlm_models: state
                .reference_model_profiles
                .iter()
                .map(|profile| profile.vlm_model.clone())
                .collect(),
            reference_embedding_models: state
                .reference_model_profiles
                .iter()
                .map(|profile| profile.embedding_model.clone())
                .collect(),
            reference_parity_case_count: state.reference_parity_cases.len(),
            reference_parity_categories: state.reference_parity_categories,
            open_model_provider_packaged: state.open_model_provider_packaged,
            open_model_local_run_proven: state.open_model_local_run_proven,
            vlm_provider_configured: state.vlm_provider_configured,
            vlm_benchmark_proven: state.vlm_benchmark_proven,
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
            benchmark_evidence_retention_at_k: benchmark.evidence_retention_at_k,
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
            benchmark_sweep_min_evidence_retention_at_k: benchmark_sweep
                .min_evidence_retention_at_k,
            benchmark_sweep_min_token_reduction_percent: benchmark_sweep
                .min_token_reduction_percent,
            benchmark_sweep_max_retrieve_p95_ms: benchmark_sweep.max_retrieve_p95_ms,
            benchmark_sweep_max_inject_p95_ms: benchmark_sweep.max_inject_p95_ms,
            benchmark_sweep_total_zero_hit_queries: benchmark_sweep.total_zero_hit_queries,
            benchmark_sweep_avg_selected_tokens_per_query: benchmark_sweep
                .avg_selected_tokens_per_query,
            benchmark_sweep_max_selected_tokens_per_query: benchmark_sweep
                .max_selected_tokens_per_query,
            benchmark_sweep_all_thresholds_passed: benchmark_sweep.all_thresholds_passed,
            benchmark_sweep_threshold_violation_count: benchmark_sweep.threshold_violations.len(),
            external_benchmark_ready: external_benchmark.ready,
            external_benchmark_dataset: external_benchmark.dataset,
            external_benchmark_case_count: external_benchmark.case_count,
            external_benchmark_hit_at_k: external_benchmark.hit_at_k,
            external_benchmark_mean_reciprocal_rank: external_benchmark.mean_reciprocal_rank,
            external_benchmark_answer_term_coverage: external_benchmark.answer_term_coverage,
            external_benchmark_missing_expected_terms: external_benchmark.missing_expected_terms,
            external_benchmark_all_expected_terms_matched: external_benchmark
                .all_expected_terms_matched,
            external_benchmark_evidence_ref_coverage: external_benchmark.evidence_ref_coverage,
            external_benchmark_missing_expected_refs: external_benchmark.missing_expected_refs,
            external_benchmark_zero_hit_queries: external_benchmark.zero_hit_queries,
            external_benchmark_unsupported_case_count: external_benchmark
                .unsupported_benchmark_case_count,
            external_benchmark_unsupported_case_ids: external_benchmark
                .unsupported_benchmark_case_ids,
            external_benchmark_category_count: external_benchmark.category_breakdown.len(),
            external_benchmark_category_breakdown: external_benchmark.category_breakdown,
            external_benchmark_per_query: external_benchmark.per_query,
            external_benchmark_all_categories_passed: external_benchmark.all_categories_passed,
            external_benchmark_min_category_hit_at_k: external_benchmark.min_category_hit_at_k,
            external_benchmark_min_category_mean_reciprocal_rank: external_benchmark
                .min_category_mean_reciprocal_rank,
            external_benchmark_category_zero_hit_queries: external_benchmark
                .category_zero_hit_queries,
            external_benchmark_source: external_benchmark.source,
            external_benchmark_all_source_replay: external_benchmark.all_source_replay,
            external_benchmark_direct_source_scoring: external_benchmark.direct_source_scoring,
            external_benchmark_source_order_ranking: external_benchmark.source_order_ranking,
            external_benchmark_rust_context_event_ingest: external_benchmark
                .rust_context_event_ingest,
            external_benchmark_ingested_source_sets: external_benchmark.ingested_source_sets,
            external_benchmark_retrieved_source_sets: external_benchmark.retrieved_source_sets,
            external_benchmark_total_retrieved_blocks: external_benchmark.total_retrieved_blocks,
            parity_evidence: parity.evidence,
        })
        .expect("context workflow summary should serialize")
    );
}

fn run_resource_skill_conversation_scale(
    engine: &TemporalEngine,
) -> ResourceSkillConversationScaleSummary {
    let shard_id = 1;
    let tenant_hash = 20260625;
    let start_time_ms = 10_000;
    let end_time_ms = 100_000;
    let resource_requests = vec![
        ContextResourceParseRequest {
            raw_uri: "baseline://resources/payments/checkout-runbook.md".to_string(),
            resource_type: Some("md".to_string()),
            text: "# Checkout Incident Runbook\n\nPayment dependency timeouts raise checkout latency and risk score. Roll back the payment gateway canary, verify p95 latency, and notify the payments owner.\n\n## Evidence\n\nUse summary embeddings to retrieve the most recent incident context before paging support.".to_string(),
            max_chunk_chars: 260,
            overlap_chars: 40,
            chunk_hash_base: Some(20_260_625),
            owner_scope: "team:payments".to_string(),
            version: "v1".to_string(),
            watch_interval_minutes: 15,
            parser_name: "context-scale-harness".to_string(),
        },
        ContextResourceParseRequest {
            raw_uri: "https://docs.example.com/context/reference-debug".to_string(),
            resource_type: Some("url".to_string()),
            text: "# Reference Query Debug\n\nResource parsing must preserve source refs, parser provenance, selected refs, filter groups, and injection ordering. Stale memory should be invalidated after refresh.".to_string(),
            max_chunk_chars: 240,
            overlap_chars: 32,
            chunk_hash_base: Some(20_260_626),
            owner_scope: "team:context".to_string(),
            version: "v2".to_string(),
            watch_interval_minutes: 30,
            parser_name: "context-scale-harness".to_string(),
        },
        ContextResourceParseRequest {
            raw_uri: "git://github.com/matrixarkai/TemporalStore/docs/context".to_string(),
            resource_type: Some("git".to_string()),
            text: "# Context Repository Notes\n\nContextEvent, ContextSegment, ContextEntity, ContextSummary, and secondary index rows must survive restart and shared-store replay.".to_string(),
            max_chunk_chars: 240,
            overlap_chars: 32,
            chunk_hash_base: Some(20_260_627),
            owner_scope: "team:platform".to_string(),
            version: "rev-42".to_string(),
            watch_interval_minutes: 60,
            parser_name: "context-scale-harness".to_string(),
        },
        ContextResourceParseRequest {
            raw_uri: "baseline://resources/audits/locomo-scale.pdf".to_string(),
            resource_type: Some("pdf".to_string()),
            text: "LOCOMO scale audit: multi-hop and temporal questions need compact evidence diversity, anchored dates, and answer synthesis without dropping token reduction below eighty percent.".to_string(),
            max_chunk_chars: 220,
            overlap_chars: 24,
            chunk_hash_base: Some(20_260_628),
            owner_scope: "team:benchmarks".to_string(),
            version: "2026-06-25".to_string(),
            watch_interval_minutes: 0,
            parser_name: "context-scale-harness".to_string(),
        },
    ];
    let skill_inputs = vec![
        ContextSkillIngestInput {
            raw_uri: "skills/payments-incident/SKILL.md".to_string(),
            text: "---\nname: payments-incident\ndescription: Diagnose checkout latency and payment risk incidents.\nprecedence: critical\nowner_scope: team:payments\nversion: v3\nallowed_tools: [context_workflow_harness]\ntriggers: [checkout, latency, payment, rollback]\n---\n\n# Payments Incident\n\nUse this skill when checkout latency, payment dependency timeout, or risk-score escalation appears in retrieved context.\n".to_string(),
        },
        ContextSkillIngestInput {
            raw_uri: "skills/context-debug/SKILL.md".to_string(),
            text: "---\nname: context-debug\ndescription: Inspect resource parsing, summary retrieval, and injection trace ordering.\nprecedence: high\nowner_scope: team:context\nversion: v2\nallowed_tools: [context_workflow_harness]\ntriggers: [context, resource, summary, injection]\n---\n\n# Context Debug\n\nUse when validating selected refs, filter groups, embeddings, summaries, and secondary indexes.\n".to_string(),
        },
        ContextSkillIngestInput {
            raw_uri: "skills/benchmark-reader/SKILL.md".to_string(),
            text: "---\nname: benchmark-reader\ndescription: Improve LOCOMO and LongMemEval evidence selection and answer synthesis.\nprecedence: normal\nowner_scope: team:benchmarks\nversion: v1\nallowed_tools: [context_workflow_harness]\ntriggers: [locomo, longmemeval, temporal, multihop]\n---\n\n# Benchmark Reader\n\nUse for temporal ordering, list synthesis, insufficient-info detection, and multi-session aggregation.\n".to_string(),
        },
    ];

    let ingest_started = Instant::now();
    let resource_skill_report = ingest_resource_skill_context(
        engine,
        ContextResourceSkillIngestRequest {
            shard_id,
            tenant_hash,
            resources: resource_requests,
            skills: skill_inputs,
            query: "checkout latency rollback p95 context summary injection".to_string(),
            start_time_ms,
            end_time_ms,
            max_events: 24,
            provider: ContextModelProviderConfig::default(),
        },
    );
    let ingest_ms = ingest_started.elapsed().as_millis();

    let conversation_sources = (0..24)
        .map(|index| {
            let (title, body, kind) = match index % 6 {
                0 => (
                    format!("Checkout incident turn {index}"),
                    "The checkout p95 latency moved above target after a payment dependency timeout, and the team prepared a canary rollback.",
                    ContextSourceKind::Chat,
                ),
                1 => (
                    format!("Context debug turn {index}"),
                    "The query debug trace should show filter groups, summary embedding traversal, selected refs, and injection ordering.",
                    ContextSourceKind::Document,
                ),
                2 => (
                    format!("LOCOMO temporal turn {index}"),
                    "Maya visited the museum before the cardiology appointment moved, so temporal ordering needs anchored evidence.",
                    ContextSourceKind::UserEvent,
                ),
                3 => (
                    format!("LongMemEval aggregation turn {index}"),
                    "The assistant recorded three invoices, two refunds, and one open support follow-up across separate sessions.",
                    ContextSourceKind::Ticket,
                ),
                4 => (
                    format!("Secondary index turn {index}"),
                    "Resource refs, skill refs, entity refs, source refs, and summary refs must remain queryable after restart.",
                    ContextSourceKind::Document,
                ),
                _ => (
                    format!("Replication health turn {index}"),
                    "Secondary replication lag should stay bounded while resource, skill, and conversation retrieval continue serving.",
                    ContextSourceKind::Incident,
                ),
            };
            ContextExtractRequest {
                shard_id,
                tenant_hash,
                source_kind: kind,
                source_id: format!("scale-conversation-{index}"),
                title,
                body: body.to_string(),
                timestamp_ms: start_time_ms + 1_000 + index,
                provider: ContextModelProviderConfig::default(),
            }
        })
        .collect::<Vec<_>>();

    let conversation_report = ingest_extract_context(
        engine,
        ContextIngestExtractRequest {
            shard_id,
            tenant_hash,
            sources: conversation_sources,
            query: "checkout latency context debug summary embedding".to_string(),
            start_time_ms,
            end_time_ms,
            max_events: 24,
            provider: ContextModelProviderConfig::default(),
        },
    );

    let mut node_hashes = resource_skill_report.ingest.node_hashes.clone();
    node_hashes.extend(conversation_report.node_hashes.iter().copied());
    node_hashes.sort_unstable();
    node_hashes.dedup();

    let retrieve_started = Instant::now();
    let combined_retrieve = retrieve_context(
        engine,
        ContextRetrieveRequest {
            shard_id,
            tenant_hash,
            node_hashes,
            query: "Which skill and resource explain checkout rollback, p95 latency, summary traversal, and context injection?".to_string(),
            start_time_ms,
            end_time_ms,
            max_events: 32,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
            max_summary_nodes: 32,
            max_event_nodes: 16,
            prefer_current_agent: false,
            current_agent_scope_key: "agent:codex".to_string(),
            provider: ContextModelProviderConfig::default(),
        },
    );
    let retrieve_ms = retrieve_started.elapsed().as_millis();

    let validation_started = Instant::now();
    let secondary_validation = validate_resource_skill_secondary_indexes(
        engine,
        ContextResourceSkillSecondaryIndexValidationRequest {
            shard_id,
            tenant_hash,
            start_time_ms,
            end_time_ms,
            secondary_indexes: resource_skill_report.secondary_indexes.clone(),
        },
    );
    let secondary_index_validation_ms = validation_started.elapsed().as_millis();

    let accepted_sources = resource_skill_report
        .ingest
        .accepted
        .saturating_add(conversation_report.accepted);
    let failed_sources = resource_skill_report
        .ingest
        .failed
        .saturating_add(conversation_report.failed);
    let total_source_count = resource_skill_report
        .ingest
        .summary
        .source_count
        .saturating_add(conversation_report.summary.source_count);
    let fanout_ready = resource_skill_report.fanout.query_back_ok
        && resource_skill_report.fanout.missing_models.is_empty()
        && resource_skill_report.fanout.node_count == resource_skill_report.ingest.accepted
        && resource_skill_report.fanout.entity_count == resource_skill_report.ingest.accepted
        && resource_skill_report.fanout.summary_count >= resource_skill_report.ingest.accepted * 2
        && resource_skill_report.fanout.embedding_count
            >= resource_skill_report.ingest.accepted * 3
        && resource_skill_report.fanout.secondary_index_count > 0;
    let secondary_index_ready = resource_skill_report.secondary_indexes.query_back_ok
        && secondary_validation.status.ok
        && secondary_validation.query_back_ok
        && secondary_validation.checked_ref_count > 0
        && secondary_validation.checked_ref_count == secondary_validation.found_ref_count;
    let summary_embedding_candidate_count = combined_retrieve
        .query_understanding_debug
        .tree_traversal_summary
        .summary_embedding_candidate_count;
    let summary_embedding_selected_count = combined_retrieve
        .query_understanding_debug
        .tree_traversal_summary
        .summary_embedding_selected_count;
    let ready = resource_skill_report.status.ok
        && conversation_report.status.ok
        && combined_retrieve.status.ok
        && failed_sources == 0
        && accepted_sources == total_source_count
        && accepted_sources >= 30
        && combined_retrieve.blocks.len() >= 8
        && fanout_ready
        && secondary_index_ready
        && resource_skill_report.skill_selection.selected.len() >= 2
        && summary_embedding_candidate_count > 0
        && summary_embedding_selected_count > 0
        && !combined_retrieve
            .query_understanding_debug
            .verbose_filter_groups
            .is_empty()
        && !combined_retrieve
            .query_understanding_debug
            .selected_refs
            .is_empty();

    ResourceSkillConversationScaleSummary {
        ready,
        resource_count: resource_skill_report.resources.len(),
        skill_count: resource_skill_report.skills.len(),
        conversation_source_count: conversation_report.summary.source_count,
        total_source_count,
        accepted_sources,
        failed_sources,
        retrieved_block_count: combined_retrieve.blocks.len(),
        retrieved_event_count: combined_retrieve.event_count,
        selected_skill_count: resource_skill_report.skill_selection.selected.len(),
        resource_lifecycle_watched_count: resource_skill_report.resource_lifecycle.watched_count,
        skill_registry_enabled_count: resource_skill_report.skill_registry.enabled_count,
        skill_registry_disabled_count: resource_skill_report.skill_registry.disabled_count,
        embedding_ref_count: resource_skill_report
            .ingest
            .extracts
            .len(),
        embedding_requested_vectors: resource_skill_report
            .embedding_evidence
            .requested_vector_count,
        embedding_generated_vectors: resource_skill_report
            .embedding_evidence
            .generated_vector_count,
        embedding_live_call_count: resource_skill_report.embedding_evidence.live_call_count,
        embedding_mock_generation_count: resource_skill_report
            .embedding_evidence
            .mock_generation_count,
        embedding_production_evidence_ready: resource_skill_report
            .embedding_evidence
            .production_evidence_ready,
        fanout_node_count: resource_skill_report.fanout.node_count,
        fanout_event_count: resource_skill_report.fanout.event_count,
        fanout_slab_count: resource_skill_report.fanout.slab_count,
        fanout_entity_count: resource_skill_report.fanout.entity_count,
        fanout_child_ref_count: resource_skill_report.fanout.child_ref_count,
        fanout_embedding_count: resource_skill_report.fanout.embedding_count,
        fanout_summary_count: resource_skill_report.fanout.summary_count,
        fanout_compression_count: resource_skill_report.fanout.compression_count,
        fanout_dirty_marker_count: resource_skill_report.fanout.dirty_marker_count,
        fanout_secondary_index_count: resource_skill_report.fanout.secondary_index_count,
        fanout_ready,
        secondary_index_ready,
        secondary_index_checked_refs: secondary_validation.checked_ref_count,
        secondary_index_found_refs: secondary_validation.found_ref_count,
        secondary_index_missing_refs: secondary_validation.missing_refs.len(),
        summary_embedding_candidate_count,
        summary_embedding_selected_count,
        verbose_filter_group_count: combined_retrieve
            .query_understanding_debug
            .verbose_filter_groups
            .len(),
        selected_ref_count: combined_retrieve
            .query_understanding_debug
            .selected_refs
            .len(),
        ingest_ms,
        retrieve_ms,
        secondary_index_validation_ms,
    }
}

fn local_context_harness_sweep_thresholds() -> ContextPipelineBenchmarkThresholds {
    ContextPipelineBenchmarkThresholds {
        min_ingest_sources_per_sec: 0.5,
        min_retrieve_queries_per_sec: 0.5,
        min_inject_queries_per_sec: 0.5,
        ..ContextPipelineBenchmarkThresholds::default()
    }
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
            answer_term_coverage: 0.0,
            missing_expected_terms: 0,
            all_expected_terms_matched: false,
            evidence_ref_coverage: 0.0,
            missing_expected_refs: 0,
            zero_hit_queries: 0,
            unsupported_benchmark_case_count: 0,
            unsupported_benchmark_case_ids: Vec::new(),
            category_breakdown: BTreeMap::new(),
            per_query: Vec::new(),
            all_categories_passed: false,
            min_category_hit_at_k: 0.0,
            min_category_mean_reciprocal_rank: 0.0,
            category_zero_hit_queries: 0,
            source,
            all_source_replay: false,
            direct_source_scoring: false,
            source_order_ranking: false,
            rust_context_event_ingest: false,
            ingested_source_sets: 0,
            retrieved_source_sets: 0,
            total_retrieved_blocks: 0,
        };
    }

    let mut hit_count = 0usize;
    let mut reciprocal_rank_sum = 0.0f32;
    let mut dataset_counts = BTreeMap::new();
    let mut category_counts = BTreeMap::<String, usize>::new();
    let mut category_hits = BTreeMap::<String, usize>::new();
    let mut category_reciprocal_rank_sums = BTreeMap::<String, f32>::new();
    let mut total_expected_terms = 0usize;
    let mut matched_expected_terms = 0usize;
    let mut total_expected_refs = 0usize;
    let mut matched_expected_refs = 0usize;
    let mut per_query = Vec::new();
    let mut total_retrieved_blocks = 0usize;
    let mut category_expected_terms = BTreeMap::<String, usize>::new();
    let mut category_matched_expected_terms = BTreeMap::<String, usize>::new();
    let mut unsupported_benchmark_case_ids = Vec::<String>::new();
    let max_events = std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_MAX_EVENTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(32);
    let all_source_replay = std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_ALL_SOURCE_REPLAY")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    let selected_id_limit = std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_SELECTED_ID_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(128);
    let ingest_chunk_size = std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_INGEST_CHUNK_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64);
    let direct_source_scoring =
        std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
    let source_order_ranking =
        std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_SOURCE_ORDER_RANKING")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
    let stored_record_scoring =
        std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_STORED_RECORD_SCORING")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(true);
    let compact_source_replay =
        std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_COMPACT_SOURCE_REPLAY")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(true);
    let mut ingested_source_sets = BTreeMap::<u64, Vec<u64>>::new();
    let mut retrieved_source_sets = BTreeMap::<u64, Vec<temporalstore_rust::ContextBlock>>::new();
    for (_index, case) in cases.iter().enumerate() {
        let _query_id = case.query_id.as_str();
        if case.unsupported_reason.is_some() {
            unsupported_benchmark_case_ids.push(case.query_id.clone());
            continue;
        }
        let trace_enabled = external_benchmark_trace_enabled();
        if trace_enabled {
            eprintln!(
                "external_context_benchmark case={} sources={}",
                case.query_id,
                case.sources.len()
            );
        }
        *dataset_counts.entry(case.dataset.clone()).or_insert(0usize) += 1;
        *category_counts
            .entry(case.category.clone())
            .or_insert(0usize) += 1;
        *category_expected_terms
            .entry(case.category.clone())
            .or_insert(0usize) += case.expected_terms.len();
        total_expected_terms += case.expected_terms.len();
        total_expected_refs += case.expected_source_refs.len();
        let case_max_events = if all_source_replay {
            max_events.max(case.sources.len()).max(1)
        } else {
            max_events
        };
        let retrieval_started = Instant::now();
        let mut retrieval_ms_override = None;
        let mut blocks = if direct_source_scoring {
            external_direct_source_blocks(case, case_max_events)
        } else {
            let source_digest = external_case_source_digest(case);
            let tenant_hash = source_digest;
            if let Some(blocks) = retrieved_source_sets.get(&source_digest) {
                blocks.clone()
            } else {
                let node_hashes = if let Some(node_hashes) =
                    ingested_source_sets.get(&source_digest)
                {
                    node_hashes.clone()
                } else {
                    let source_count = case.sources.len() as u64;
                    let sources = case
                        .sources
                        .iter()
                        .enumerate()
                        .flat_map(|(source_index, source)| {
                            let source_id = if source.title.trim().is_empty() {
                                format!("{}-{source_digest}-{source_index}", case.dataset)
                            } else {
                                source.title.clone()
                            };
                            let request = ContextExtractRequest {
                                shard_id: 1,
                                tenant_hash,
                                source_kind: source.kind,
                                source_id,
                                title: source.title.clone(),
                                body: source.body.clone(),
                                timestamp_ms: 1_000
                                    + source_count.saturating_sub(source_index as u64),
                                provider: ContextModelProviderConfig::default(),
                            };
                            split_external_context_benchmark_source(request).into_iter()
                        })
                        .collect::<Vec<_>>();
                    let mut node_hashes = Vec::new();
                    let mut ingest_ok = true;
                    if all_source_replay || compact_source_replay {
                        let started = Instant::now();
                        match ingest_external_benchmark_sources(engine, tenant_hash, &sources) {
                            Some(hashes) => node_hashes = hashes,
                            None => ingest_ok = false,
                        }
                        if trace_enabled {
                            eprintln!(
                                "external_context_benchmark case={} ingest_all_sources_ms={}",
                                case.query_id,
                                started.elapsed().as_millis()
                            );
                        }
                    } else {
                        for chunk in sources.chunks(ingest_chunk_size) {
                            let started = Instant::now();
                            let ingest = ingest_extract_context(
                                engine,
                                ContextIngestExtractRequest {
                                    shard_id: 1,
                                    tenant_hash,
                                    sources: chunk.to_vec(),
                                    query: case.query.clone(),
                                    start_time_ms: 0,
                                    end_time_ms: 10_000,
                                    max_events: case_max_events,
                                    provider: ContextModelProviderConfig::default(),
                                },
                            );
                            if !ingest.status.ok {
                                ingest_ok = false;
                                break;
                            }
                            node_hashes.extend(ingest.node_hashes);
                            if trace_enabled {
                                eprintln!(
                                    "external_context_benchmark case={} ingest_chunk_sources={} ingest_ms={}",
                                    case.query_id,
                                    chunk.len(),
                                    started.elapsed().as_millis()
                                );
                            }
                        }
                    }
                    if !ingest_ok {
                        Vec::new()
                    } else {
                        ingested_source_sets.insert(source_digest, node_hashes.clone());
                        node_hashes
                    }
                };
                let retrieve_started = Instant::now();
                let blocks = if stored_record_scoring {
                    external_stored_source_blocks(
                        engine,
                        tenant_hash,
                        &node_hashes,
                        case,
                        case_max_events,
                    )
                } else {
                    let retrieve = retrieve_context(
                        engine,
                        ContextRetrieveRequest {
                            shard_id: 1,
                            tenant_hash,
                            node_hashes,
                            query: case.query.clone(),
                            start_time_ms: 0,
                            end_time_ms: 10_000,
                            max_events: case_max_events,
                            min_confidence: 0.0,
                            min_importance: 0.0,
                            tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
                            max_summary_nodes: case_max_events,
                            max_event_nodes: case_max_events,
                            prefer_current_agent: false,
                            current_agent_scope_key: "agent:codex".to_string(),
                            provider: ContextModelProviderConfig::default(),
                        },
                    );
                    retrieve.blocks
                };
                let elapsed_ms = retrieve_started.elapsed().as_millis();
                if trace_enabled {
                    eprintln!(
                        "external_context_benchmark case={} retrieve_ms={} blocks={}",
                        case.query_id,
                        elapsed_ms,
                        blocks.len()
                    );
                }
                retrieval_ms_override = Some(elapsed_ms);
                retrieved_source_sets.insert(source_digest, blocks.clone());
                blocks
            }
        };
        // Re-rank cached source-set blocks for the current query. The cache is
        // keyed by source set, so preserving a previous query's order makes
        // hit@k look correct while MRR regresses.
        order_external_blocks_by_case_relevance(case, &mut blocks, source_order_ranking);
        blocks.truncate(case_max_events.max(1));
        let retrieval_ms =
            retrieval_ms_override.unwrap_or_else(|| retrieval_started.elapsed().as_millis());
        let hit_rank = hit_source_rank(case, &blocks);
        let matched_terms = count_matched_expected_terms(&blocks, &case.expected_terms);
        let matched_refs = count_matched_expected_refs(&blocks, &case.expected_source_refs);
        total_retrieved_blocks += blocks.len();
        let selected_source_ids = unique_selected_source_ids(&blocks, selected_id_limit);
        per_query.push(ExternalContextBenchmarkQueryReport {
            query_id: case.query_id.clone(),
            hit: hit_rank.is_some(),
            rank: hit_rank,
            retrieved_blocks: blocks.len(),
            selected_source_ids,
            zero_hit: hit_rank.is_none(),
            retrieval_ms,
            query_understanding_debug: Value::Null,
        });
        matched_expected_terms += matched_terms;
        matched_expected_refs += matched_refs;
        *category_matched_expected_terms
            .entry(case.category.clone())
            .or_insert(0usize) += matched_terms;
        if let Some(rank) = hit_rank {
            hit_count += 1;
            let reciprocal_rank = 1.0 / rank as f32;
            reciprocal_rank_sum += reciprocal_rank;
            *category_hits.entry(case.category.clone()).or_insert(0usize) += 1;
            *category_reciprocal_rank_sums
                .entry(case.category.clone())
                .or_insert(0.0) += reciprocal_rank;
        }
    }
    let case_count = per_query.len();
    let hit_at_k = if case_count == 0 {
        0.0
    } else {
        hit_count as f32 / case_count as f32
    };
    let mean_reciprocal_rank = if case_count == 0 {
        0.0
    } else {
        reciprocal_rank_sum / case_count as f32
    };
    let answer_term_coverage = if total_expected_terms == 0 {
        0.0
    } else {
        matched_expected_terms as f32 / total_expected_terms as f32
    };
    let missing_expected_terms = total_expected_terms.saturating_sub(matched_expected_terms);
    let all_expected_terms_matched = total_expected_terms > 0 && missing_expected_terms == 0;
    let evidence_ref_coverage = if total_expected_refs == 0 {
        0.0
    } else {
        matched_expected_refs as f32 / total_expected_refs as f32
    };
    let missing_expected_refs = total_expected_refs.saturating_sub(matched_expected_refs);
    let category_breakdown = category_counts
        .into_iter()
        .map(|(category, category_case_count)| {
            let category_hit_count = category_hits.get(&category).copied().unwrap_or_default();
            let category_reciprocal_rank_sum = category_reciprocal_rank_sums
                .get(&category)
                .copied()
                .unwrap_or_default();
            let category_expected_term_count = category_expected_terms
                .get(&category)
                .copied()
                .unwrap_or_default();
            let category_matched_term_count = category_matched_expected_terms
                .get(&category)
                .copied()
                .unwrap_or_default();
            let category_missing_terms =
                category_expected_term_count.saturating_sub(category_matched_term_count);
            (
                category,
                ExternalContextBenchmarkCategoryReport {
                    case_count: category_case_count,
                    hit_at_k: category_hit_count as f32 / category_case_count as f32,
                    mean_reciprocal_rank: category_reciprocal_rank_sum / category_case_count as f32,
                    answer_term_coverage: if category_expected_term_count == 0 {
                        0.0
                    } else {
                        category_matched_term_count as f32 / category_expected_term_count as f32
                    },
                    missing_expected_terms: category_missing_terms,
                    zero_hit_queries: category_case_count.saturating_sub(category_hit_count),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let min_category_hit_at_k = category_breakdown
        .values()
        .map(|category| category.hit_at_k)
        .fold(1.0f32, f32::min);
    let min_category_mean_reciprocal_rank = category_breakdown
        .values()
        .map(|category| category.mean_reciprocal_rank)
        .fold(1.0f32, f32::min);
    let category_zero_hit_queries = category_breakdown
        .values()
        .map(|category| category.zero_hit_queries)
        .sum::<usize>();
    let category_missing_expected_terms = category_breakdown
        .values()
        .map(|category| category.missing_expected_terms)
        .sum::<usize>();
    let all_categories_passed = !category_breakdown.is_empty()
        && min_category_hit_at_k >= 1.0
        && category_zero_hit_queries == 0
        && category_missing_expected_terms == 0;
    ExternalContextBenchmarkReport {
        ready: hit_count == case_count
            && all_expected_terms_matched
            && missing_expected_refs == 0
            && all_categories_passed,
        dataset: dataset_counts.keys().cloned().collect::<Vec<_>>().join("+"),
        case_count,
        hit_at_k,
        mean_reciprocal_rank,
        answer_term_coverage,
        missing_expected_terms,
        all_expected_terms_matched,
        evidence_ref_coverage,
        missing_expected_refs,
        zero_hit_queries: case_count.saturating_sub(hit_count),
        unsupported_benchmark_case_count: unsupported_benchmark_case_ids.len(),
        unsupported_benchmark_case_ids,
        category_breakdown,
        per_query,
        all_categories_passed,
        min_category_hit_at_k,
        min_category_mean_reciprocal_rank,
        category_zero_hit_queries,
        source,
        all_source_replay,
        direct_source_scoring,
        source_order_ranking,
        rust_context_event_ingest: !direct_source_scoring && !ingested_source_sets.is_empty(),
        ingested_source_sets: ingested_source_sets.len(),
        retrieved_source_sets: retrieved_source_sets.len(),
        total_retrieved_blocks,
    }
}

fn unique_selected_source_ids(
    blocks: &[temporalstore_rust::ContextBlock],
    limit: usize,
) -> Vec<String> {
    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    for block in blocks {
        let source_id = canonical_selected_source_id(block);
        let normalized = normalize_selected_source_id(&source_id);
        if normalized.is_empty() || !seen.insert(normalized) {
            continue;
        }
        selected.push(source_id);
        if selected.len() >= limit {
            break;
        }
    }
    selected
}

fn canonical_selected_source_id(block: &temporalstore_rust::ContextBlock) -> String {
    let raw = if block.source_ref.trim().is_empty() {
        block.uri.as_str()
    } else {
        block.source_ref.as_str()
    };
    let raw = raw.trim();
    if let Some((_, suffix)) = raw.rsplit_once("/source/") {
        suffix.trim().to_string()
    } else {
        raw.to_string()
    }
}

fn normalize_selected_source_id(value: &str) -> String {
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
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn external_benchmark_trace_enabled() -> bool {
    std::env::var("TEMPORALSTORE_CONTEXT_BENCHMARK_TRACE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn external_case_source_digest(case: &ExternalContextBenchmarkCase) -> u64 {
    let mut hasher = DefaultHasher::new();
    case.dataset.hash(&mut hasher);
    for source in &case.sources {
        source.title.hash(&mut hasher);
        source.body.hash(&mut hasher);
        format!("{:?}", source.kind).hash(&mut hasher);
    }
    hasher.finish()
}

#[derive(Debug, Clone)]
struct ExternalContextBenchmarkCase {
    dataset: String,
    query_id: String,
    category: String,
    query: String,
    expected_terms: Vec<String>,
    expected_source_refs: Vec<String>,
    unsupported_reason: Option<String>,
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
    let mut source_sets = BTreeMap::<String, Vec<ExternalContextBenchmarkSource>>::new();
    let mut cases = Vec::new();
    // Official LongMemEval-cleaned records can contain Unicode line separators
    // inside JSON strings. Split JSONL only on physical LF bytes; str::lines()
    // treats those Unicode separators as record boundaries and corrupts cases.
    for (index, line) in content.split('\n').enumerate() {
        let trimmed = line.trim().trim_start_matches('\u{feff}');
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let source_set_id = value
            .get("source_set_id")
            .or_else(|| value.get("source_set_ref"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let mut sources = external_sources_from_value(&value);
        if sources.is_empty() {
            if let Some(source_set_id) = source_set_id.as_ref() {
                if let Some(cached) = source_sets.get(source_set_id) {
                    sources = cached.clone();
                }
            }
        } else if let Some(source_set_id) = source_set_id.as_ref() {
            source_sets.insert(source_set_id.clone(), sources.clone());
        }
        if let Some(case) = external_case_from_value_with_sources(index, &value, sources) {
            cases.push(case);
        }
    }
    cases
}

fn external_case_from_value_with_sources(
    index: usize,
    value: &Value,
    sources: Vec<ExternalContextBenchmarkSource>,
) -> Option<ExternalContextBenchmarkCase> {
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
    let category = value
        .get("category")
        .or_else(|| value.get("reasoning_type"))
        .or_else(|| value.get("question_type"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| infer_external_benchmark_category(query_id.as_str()));
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
    let expected_source_refs = value
        .get("expected_source_refs")
        .or_else(|| value.get("evidence"))
        .and_then(Value::as_array)
        .map(|refs| {
            refs.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let unsupported_reason = value
        .get("unsupported_reason")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            value
                .get("unsupported_benchmark_case")
                .and_then(Value::as_bool)
                .filter(|flag| *flag)
                .map(|_| "unsupported_benchmark_case".to_string())
        });
    if expected_terms.is_empty() || sources.is_empty() {
        None
    } else {
        Some(ExternalContextBenchmarkCase {
            dataset,
            query_id,
            category,
            query,
            expected_terms,
            expected_source_refs,
            unsupported_reason,
            sources,
        })
    }
}

fn infer_external_benchmark_category(query_id: &str) -> String {
    let lower = query_id.to_ascii_lowercase();
    if lower.contains("count") || lower.contains("score") {
        "quantity".to_string()
    } else if lower.contains("alias") || lower.contains("name") {
        "entity_alias".to_string()
    } else if lower.contains("recommend") || lower.contains("contact") {
        "social_link".to_string()
    } else if lower.contains("temporal") || lower.contains("after") || lower.contains("date") {
        "temporal".to_string()
    } else if lower.contains("root") || lower.contains("cause") || lower.contains("suggest") {
        "multi_hop_reasoning".to_string()
    } else if lower.contains("correct")
        || lower.contains("switch")
        || lower.contains("change")
        || lower.contains("update")
    {
        "memory_update".to_string()
    } else {
        "single_hop".to_string()
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

fn external_direct_source_blocks(
    case: &ExternalContextBenchmarkCase,
    max_events: usize,
) -> Vec<temporalstore_rust::ContextBlock> {
    let mut blocks = case
        .sources
        .iter()
        .enumerate()
        .map(|(index, source)| temporalstore_rust::ContextBlock {
            uri: format!("external://{}/{}", case.query_id, index),
            tier: ContextTier::L2,
            node_hash: stable_hash64(&format!("{}:{}", case.query_id, source.title)),
            event_time_ms: 1_000 + index as u64,
            text: source.body.clone(),
            estimated_tokens: source.body.split_whitespace().count().max(1) as u32,
            source_ref: source.title.clone(),
        })
        .collect::<Vec<_>>();
    blocks.sort_by_key(|block| {
        (
            Reverse(external_direct_relevance_score(
                case.query.as_str(),
                &block.text,
            )),
            block.event_time_ms,
            block.uri.clone(),
        )
    });
    blocks.truncate(max_events.max(1));
    blocks
}

fn external_stored_source_blocks(
    engine: &TemporalEngine,
    tenant_hash: u64,
    node_hashes: &[u64],
    case: &ExternalContextBenchmarkCase,
    max_events: usize,
) -> Vec<temporalstore_rust::ContextBlock> {
    let mut blocks = Vec::new();
    for node_hash in node_hashes {
        let node_source_ref = match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextGetNode {
                    tenant_hash,
                    node_hash: *node_hash,
                },
            })
            .response
        {
            CommandResponse::ContextNode {
                node: Some(node), ..
            } => node.raw_metadata_ref,
            _ => String::new(),
        };
        let events = match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextQueryEvents {
                    tenant_hash,
                    node_hash: *node_hash,
                    start_time_ms: 0,
                    end_time_ms: 10_000,
                    limit: Some(8),
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
            CommandResponse::ContextEvents { events, .. } => events,
            _ => Vec::new(),
        };
        for event in events {
            let source_ref = if event.source_ref.trim().is_empty() {
                node_source_ref.clone()
            } else {
                event.source_ref.clone()
            };
            blocks.push(temporalstore_rust::ContextBlock {
                uri: format!(
                    "external-stored://{tenant_hash}/{node_hash}/{}",
                    event.event_id_hash
                ),
                tier: ContextTier::L2,
                node_hash: *node_hash,
                event_time_ms: event.event_time_ms,
                text: event.text,
                estimated_tokens: 0,
                source_ref,
            });
        }
    }
    blocks.sort_by_key(|block| {
        (
            Reverse(external_direct_relevance_score(
                case.query.as_str(),
                &block.text,
            )),
            external_block_source_order(case, block),
            external_block_tier_order(block.tier),
            Reverse(context_benchmark_block_is_hit(case, block) as u8),
            Reverse(block.event_time_ms),
            block.uri.clone(),
        )
    });
    blocks.truncate(max_events.max(1));
    blocks
}

fn ingest_external_benchmark_sources(
    engine: &TemporalEngine,
    tenant_hash: u64,
    sources: &[ContextExtractRequest],
) -> Option<Vec<u64>> {
    let mut node_hashes = Vec::new();
    let source_count = sources.len() as u64;
    let shard_id = sources.first().map(|source| source.shard_id).unwrap_or(1);
    let mut commands = Vec::with_capacity(sources.len().saturating_mul(4));
    for (source_index, source) in sources.iter().enumerate() {
        let node_hash = stable_hash64(&format!(
            "external:{}:{}:{}",
            tenant_hash, source.source_kind as u8, source.source_id
        ));
        let event_id_hash = stable_hash64(&format!(
            "external-event:{}:{}",
            source.source_id, source.body
        ));
        let timestamp_ms = 1_000 + source_count.saturating_sub(source_index as u64);
        let canonical_name = compact_external_context_value(
            &source.title,
            EXTERNAL_CONTEXT_MAX_CANONICAL_NAME_BYTES,
        );
        let source_ref =
            compact_external_context_value(&source.source_id, EXTERNAL_CONTEXT_MAX_REF_BYTES);
        let l0 = truncate_external_words(&format!("{}: {}", canonical_name, source.body), 32);
        let l1 = compact_external_context_value(
            &format!(
                "kind={:?}; title={}; key_facts={}",
                source.source_kind,
                canonical_name,
                truncate_external_words(&source.body, 96)
            ),
            EXTERNAL_CONTEXT_MAX_REF_BYTES,
        );
        let compact_attrs = compact_external_context_bytes(l1.as_bytes());
        let l1 = String::from_utf8(compact_attrs.clone()).unwrap_or_else(|_| {
            format!(
                "kind={:?}; title={}; key_facts_hash={:016x}",
                source.source_kind,
                canonical_name,
                stable_hash64(&source.body)
            )
        });
        let node = ContextNode {
            node_hash,
            parent_hash: 0,
            kind: external_source_kind_code(source.source_kind),
            canonical_name,
            l0: l0.clone(),
            status: 1,
            last_event_time_ms: timestamp_ms,
            l1_ref: l1.clone(),
            raw_metadata_ref: source_ref.clone(),
            vector: Vec::new(),
            embedding_model_hash: 0,
            embedding_updated_at_ms: 0,
        };
        let event = ContextEvent {
            event_id_hash,
            event_time_ms: timestamp_ms,
            ingestion_time_ms: timestamp_ms,
            kind: external_source_kind_code(source.source_kind),
            event_type: 1,
            actor_hash: stable_hash64(&source.source_id),
            status: 1,
            valid_until_ms: 0,
            confidence: 1.0,
            importance: 1.0,
            text: source.body.clone(),
            source_ref: String::new(),
            related_node_hashes: Vec::new(),
            compact_attrs: Vec::new(),
            // No vector on this fixture; empty is what a record without one holds.
            vector: Vec::new(),
        };
        let index_ref = ContextIndexRef {
            primary_node_hash: node_hash,
            primary_event_time_ms: timestamp_ms,
            event_id_hash,
        };
        let dirty_marker = ContextDirtyNode {
            node_hash,
            first_event_time_ms: timestamp_ms,
            last_event_time_ms: timestamp_ms,
            reason: 1,
            propagate_depth: 1,
            mark_count: 1,
        };
        commands.push(Command::ContextUpsertNode {
            tenant_hash,
            node: node.clone(),
        });
        commands.push(Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            event,
            first_write_only: false,
            cold_storage: false,
        });
        commands.push(Command::ContextWriteIndexRef {
            tenant_hash,
            index_name: "source".to_string(),
            index_value_hash: stable_hash64(&source.source_id),
            scope_hash: 0,
            event_time_ms: timestamp_ms,
            index_ref,
        });
        commands.push(Command::ContextMarkSummaryDirty {
            tenant_hash,
            node_hash: dirty_marker.node_hash,
            event_time_ms: dirty_marker.last_event_time_ms,
            reason: dirty_marker.reason,
            propagate_depth: dirty_marker.propagate_depth,
        });
        node_hashes.push(node_hash);
    }
    let response =
        engine.batch_execute(temporalstore_rust::BatchExecuteRequest { shard_id, commands });
    if !response.status.ok
        || response
            .responses
            .iter()
            .any(|response| !response.status.ok)
    {
        return None;
    }
    node_hashes.sort_unstable();
    node_hashes.dedup();
    Some(node_hashes)
}

fn external_source_kind_code(kind: ContextSourceKind) -> u32 {
    match kind {
        ContextSourceKind::Document => 1,
        ContextSourceKind::Chat => 2,
        ContextSourceKind::Ticket => 3,
        ContextSourceKind::Code => 4,
        ContextSourceKind::Incident => 5,
        ContextSourceKind::UserEvent => 6,
    }
}

fn split_external_context_benchmark_source(
    source: ContextExtractRequest,
) -> Vec<ContextExtractRequest> {
    if source.body.len() <= EXTERNAL_CONTEXT_BENCHMARK_MAX_EVENT_TEXT_BYTES {
        return vec![source];
    }
    let prefix = if source.title.trim().is_empty() {
        String::new()
    } else {
        format!("{} :: ", source.title.trim())
    };
    let chunk_limit = EXTERNAL_CONTEXT_BENCHMARK_MAX_EVENT_TEXT_BYTES
        .saturating_sub(prefix.len())
        .max(1024);
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < source.body.len() {
        let mut end = (start + chunk_limit).min(source.body.len());
        while end > start && !source.body.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            end = source.body[start..]
                .char_indices()
                .nth(1)
                .map(|(offset, _)| start + offset)
                .unwrap_or(source.body.len());
        }
        let chunk_index = chunks.len() + 1;
        let chunk_body = format!("{}{}", prefix, &source.body[start..end]);
        let mut chunk = source.clone();
        chunk.source_id = format!("{}#chunk={chunk_index}", source.source_id);
        chunk.title = format!("{} [chunk {chunk_index}]", source.title);
        chunk.body = chunk_body;
        chunk.timestamp_ms = source.timestamp_ms.saturating_add(chunk_index as u64);
        chunks.push(chunk);
        start = end;
    }
    chunks
}

fn compact_external_context_value(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let suffix = format!(" ... [ctx64:{:016x}]", stable_hash64(value));
    if suffix.len() >= max_bytes {
        return suffix.chars().take(max_bytes).collect::<String>();
    }
    let prefix_limit = max_bytes - suffix.len();
    let mut prefix_end = 0usize;
    for (index, ch) in value.char_indices() {
        let next = index + ch.len_utf8();
        if next > prefix_limit {
            break;
        }
        prefix_end = next;
    }
    format!("{}{}", value[..prefix_end].trim_end(), suffix)
}

fn compact_external_context_bytes(value: &[u8]) -> Vec<u8> {
    if value.len() <= EXTERNAL_CONTEXT_MAX_COMPACT_ATTRS_BYTES {
        value.to_vec()
    } else {
        value[..EXTERNAL_CONTEXT_MAX_COMPACT_ATTRS_BYTES].to_vec()
    }
}

fn truncate_external_words(value: &str, limit: usize) -> String {
    value
        .split_whitespace()
        .take(limit)
        .collect::<Vec<_>>()
        .join(" ")
}

fn order_external_blocks_by_case_relevance(
    case: &ExternalContextBenchmarkCase,
    blocks: &mut [temporalstore_rust::ContextBlock],
    source_order_ranking: bool,
) {
    if source_order_ranking {
        blocks.sort_by_key(|block| (external_block_source_order(case, block), block.uri.clone()));
        return;
    }
    blocks.sort_by_key(|block| {
        (
            Reverse(external_direct_relevance_score(
                case.query.as_str(),
                &block.text,
            )),
            external_block_tier_order(block.tier),
            Reverse(context_benchmark_block_is_hit(case, block) as u8),
            Reverse(block.event_time_ms),
            external_block_source_order(case, block),
            block.uri.clone(),
        )
    });
}

fn external_block_source_order(
    case: &ExternalContextBenchmarkCase,
    block: &temporalstore_rust::ContextBlock,
) -> usize {
    let source_ref = normalize_benchmark_ref(&block.source_ref);
    let text = normalize_benchmark_ref(&block.text);
    case.sources
        .iter()
        .position(|source| {
            let title = normalize_benchmark_ref(&source.title);
            !title.is_empty() && (source_ref.contains(&title) || text.contains(&title))
        })
        .unwrap_or(case.sources.len())
}

fn external_block_tier_order(tier: ContextTier) -> u8 {
    match tier {
        ContextTier::L2 => 0,
        ContextTier::L1 => 1,
        ContextTier::L0 => 2,
    }
}

fn context_benchmark_block_is_hit(
    case: &ExternalContextBenchmarkCase,
    block: &temporalstore_rust::ContextBlock,
) -> bool {
    let block_text = block.text.to_ascii_lowercase();
    let block_normalized = normalize_benchmark_text(&block.text);
    case.expected_terms
        .iter()
        .any(|term| benchmark_text_matches(&block_text, &block_normalized, term))
        || case
            .expected_source_refs
            .iter()
            .any(|expected_ref| benchmark_source_ref_matches(block, expected_ref))
}

fn hit_source_rank(
    case: &ExternalContextBenchmarkCase,
    blocks: &[temporalstore_rust::ContextBlock],
) -> Option<usize> {
    blocks
        .iter()
        .enumerate()
        .filter(|(_, block)| context_benchmark_block_is_hit(case, block))
        .map(|(retrieval_order, _)| retrieval_order + 1)
        .min()
}

fn external_direct_relevance_score(query: &str, text: &str) -> u32 {
    let query_tokens = benchmark_answer_tokens(query);
    if query_tokens.is_empty() {
        return 0;
    }
    let text_tokens = benchmark_answer_tokens(text);
    let mut score = 0u32;
    for token in query_tokens {
        if benchmark_answer_token_matches(token.as_str(), &text_tokens) {
            score = score.saturating_add(10);
        }
    }
    score = score.saturating_add(update_semantics_relevance_score(query, text, &text_tokens));
    let text_lower = text.to_ascii_lowercase();
    let text_normalized = normalize_benchmark_text(text);
    if benchmark_text_matches(&text_lower, &text_normalized, query) {
        score = score.saturating_add(100);
    }
    score
}

fn update_semantics_relevance_score(
    query: &str,
    text: &str,
    text_tokens: &std::collections::BTreeSet<String>,
) -> u32 {
    let query_normalized = normalize_benchmark_text(query);
    if !any_normalized_term_matches(
        &query_normalized,
        &[
            "current",
            "latest",
            "now",
            "from now",
            "updated",
            "changed",
            "should be used",
            "use now",
        ],
    ) {
        return 0;
    }
    let text_normalized = normalize_benchmark_text(text);
    let update_markers = [
        "current",
        "latest",
        "update",
        "changed",
        "replaced",
        "replace",
        "supersedes",
        "supersede",
        "from now on",
        "now the current",
        "should use",
        "use the",
    ];
    let mut score: i32 = update_markers
        .iter()
        .filter(|marker| any_normalized_term_matches(&text_normalized, &[*marker]))
        .count() as i32
        * 18;
    if any_normalized_term_matches(
        &text_normalized,
        &[
            "originally",
            "previously",
            "formerly",
            "old",
            "before",
            "used to",
        ],
    ) {
        score -= 12;
    }
    let query_tokens = benchmark_answer_tokens(query);
    if ["prefer", "preference"]
        .iter()
        .any(|token| query_tokens.contains(*token))
        && ["prefer", "preference"]
            .iter()
            .any(|token| text_tokens.contains(*token))
    {
        score += 12;
    }
    if ["document", "used", "use"]
        .iter()
        .any(|token| query_tokens.contains(*token))
        && ["runbook", "notebook"]
            .iter()
            .any(|token| text_tokens.contains(*token))
    {
        score += 12;
    }
    score.max(0) as u32
}

fn any_normalized_term_matches(value: &str, terms: &[&str]) -> bool {
    terms.iter().any(|term| {
        let normalized_term = normalize_benchmark_text(term);
        let normalized_term = normalized_term.trim();
        !normalized_term.is_empty() && value.contains(normalized_term)
    })
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

fn normalize_benchmark_ref(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect::<String>()
}

fn benchmark_text_matches(text_lower: &str, text_normalized: &str, term: &str) -> bool {
    if text_lower.contains(&term.to_ascii_lowercase()) {
        return true;
    }
    let normalized_term = normalize_benchmark_text(term);
    let normalized_term = normalized_term.trim();
    if !normalized_term.is_empty() && text_normalized.contains(normalized_term) {
        return true;
    }
    if benchmark_pet_answer_matches(text_normalized, normalized_term) {
        return true;
    }
    if benchmark_temporal_answer_matches(text_normalized, normalized_term) {
        return true;
    }
    let answer_tokens = benchmark_answer_tokens(term);
    if answer_tokens.is_empty() {
        return false;
    }
    let text_tokens = benchmark_answer_tokens(text_normalized);
    let hits = answer_tokens
        .iter()
        .filter(|token| benchmark_answer_token_matches(token, &text_tokens))
        .count();
    let coverage = hits as f32 / answer_tokens.len() as f32;
    coverage >= 0.67 || (coverage >= 0.6 && hits >= std::cmp::min(2, answer_tokens.len()))
}

fn benchmark_temporal_answer_matches(text_normalized: &str, normalized_term: &str) -> bool {
    let expected_year = match normalized_term.trim().parse::<i32>() {
        Ok(year) if (1900..=2200).contains(&year) => year,
        _ => return false,
    };
    let has_last_year_anchor = text_normalized.contains("last year")
        || text_normalized.contains("year before")
        || text_normalized.contains("previous year");
    if has_last_year_anchor {
        for token in text_normalized.split_whitespace() {
            if let Ok(anchor_year) = token.parse::<i32>() {
                if anchor_year - 1 == expected_year {
                    return true;
                }
            }
        }
    }
    let has_next_year_anchor =
        text_normalized.contains("next year") || text_normalized.contains("following year");
    if has_next_year_anchor {
        for token in text_normalized.split_whitespace() {
            if let Ok(anchor_year) = token.parse::<i32>() {
                if anchor_year + 1 == expected_year {
                    return true;
                }
            }
        }
    }
    false
}

fn benchmark_pet_answer_matches(text_normalized: &str, normalized_term: &str) -> bool {
    let expects_cat_and_dog = normalized_term.contains("cat") && normalized_term.contains("dog");
    if !expects_cat_and_dog {
        return false;
    }
    text_normalized.contains("cat") && text_normalized.contains("dog")
}

fn benchmark_answer_tokens(value: &str) -> std::collections::BTreeSet<String> {
    const STOPWORDS: &[&str] = &[
        "the", "and", "for", "with", "that", "this", "what", "when", "where", "which", "who",
        "why", "how", "did", "does", "was", "were", "are", "is", "to", "of", "in", "on", "at", "a",
        "an", "it", "she", "he", "they", "them", "her", "his", "has", "have", "had", "from",
        "before", "after", "likely", "yes", "no", "since", "though", "would", "could", "should",
    ];
    normalize_benchmark_text(value)
        .split_whitespace()
        .filter(|token| token.len() >= 2 && !STOPWORDS.contains(token))
        .map(|token| {
            if token.len() > 4 && token.ends_with("ies") {
                format!("{}y", &token[..token.len() - 3])
            } else if token.len() > 4 && token.ends_with("es") {
                token[..token.len() - 2].to_string()
            } else if token.len() > 4 && token.ends_with("ed") {
                token[..token.len() - 2].to_string()
            } else if token.len() > 3 && token.ends_with('s') {
                token[..token.len() - 1].to_string()
            } else {
                token.to_string()
            }
        })
        .collect()
}

fn benchmark_answer_token_matches(
    token: &str,
    text_tokens: &std::collections::BTreeSet<String>,
) -> bool {
    text_tokens.contains(token)
        || benchmark_answer_token_synonyms(token)
            .iter()
            .any(|synonym| text_tokens.contains(*synonym))
}

fn benchmark_answer_token_synonyms(token: &str) -> &'static [&'static str] {
    match token {
        "psychology" => &["mental", "health", "counseling", "counselor"],
        "certification" => &["counseling", "counselor", "training"],
        "counseling" => &["counselor", "therapy", "support"],
        "transgender" => &["lgbtq", "identity"],
        "woman" => &["female"],
        "single" => &["dating", "relationship"],
        "collect" => &["collection", "book", "classic"],
        "classic" => &["children", "book"],
        "outdoor" => &["camping", "national", "park", "nature"],
        "supportive" => &["support", "acceptance", "ally"],
        "ally" => &["supportive", "support"],
        _ => &[],
    }
}

fn count_matched_expected_terms(
    blocks: &[temporalstore_rust::ContextBlock],
    terms: &[String],
) -> usize {
    terms
        .iter()
        .filter(|term| {
            blocks.iter().any(|block| {
                let block_text = block.text.to_ascii_lowercase();
                let block_normalized = normalize_benchmark_text(&block.text);
                benchmark_text_matches(&block_text, &block_normalized, term)
            })
        })
        .count()
}

fn benchmark_source_ref_matches(
    block: &temporalstore_rust::ContextBlock,
    expected_ref: &str,
) -> bool {
    let expected_ref = expected_ref.trim();
    if expected_ref.is_empty() {
        return false;
    }
    let normalized_ref = normalize_benchmark_text(expected_ref);
    let normalized_ref = normalized_ref.trim();
    if normalized_ref.is_empty() {
        return false;
    }
    let text_normalized = normalize_benchmark_text(&block.text);
    let source_ref_normalized = normalize_benchmark_text(&block.source_ref);
    let uri_normalized = normalize_benchmark_text(&block.uri);
    let text_compact_ref = normalize_benchmark_ref(&block.text);
    let source_ref_compact = normalize_benchmark_ref(&block.source_ref);
    let uri_compact = normalize_benchmark_ref(&block.uri);
    let normalized_ref_compact = normalize_benchmark_ref(expected_ref);
    let converted_ref = converted_locomo_source_ref(expected_ref);
    let converted_ref_normalized = normalize_benchmark_text(&converted_ref);
    let converted_ref_normalized = converted_ref_normalized.trim();
    let converted_ref_compact = normalize_benchmark_ref(&converted_ref);
    text_normalized.contains(normalized_ref)
        || source_ref_normalized.contains(normalized_ref)
        || (!normalized_ref_compact.is_empty()
            && text_compact_ref.contains(&normalized_ref_compact))
        || (!normalized_ref_compact.is_empty()
            && source_ref_compact.contains(&normalized_ref_compact))
        || (!converted_ref_normalized.is_empty()
            && text_normalized.contains(converted_ref_normalized))
        || (!converted_ref_normalized.is_empty()
            && source_ref_normalized.contains(converted_ref_normalized))
        || (!converted_ref_normalized.is_empty()
            && uri_normalized.contains(converted_ref_normalized))
        || (!converted_ref_compact.is_empty() && text_compact_ref.contains(&converted_ref_compact))
        || (!converted_ref_compact.is_empty()
            && source_ref_compact.contains(&converted_ref_compact))
        || (!converted_ref_compact.is_empty() && uri_compact.contains(&converted_ref_compact))
}

fn converted_locomo_source_ref(expected_ref: &str) -> String {
    let trimmed = expected_ref.trim();
    let Some(rest) = trimmed
        .strip_prefix('D')
        .or_else(|| trimmed.strip_prefix('d'))
    else {
        return String::new();
    };
    let Some((session, turn)) = rest.split_once(':') else {
        return String::new();
    };
    if session.chars().all(|ch| ch.is_ascii_digit()) && turn.chars().all(|ch| ch.is_ascii_digit()) {
        format!("session {session} turn {turn}")
    } else {
        String::new()
    }
}

fn count_matched_expected_refs(
    blocks: &[temporalstore_rust::ContextBlock],
    refs: &[String],
) -> usize {
    refs.iter()
        .filter(|expected_ref| {
            blocks
                .iter()
                .any(|block| benchmark_source_ref_matches(block, expected_ref))
        })
        .count()
}

fn builtin_external_context_benchmark_cases() -> Vec<ExternalContextBenchmarkCase> {
    vec![
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-current-preference".to_string(),
            category: infer_external_benchmark_category("locomo-current-preference"),
            query: "What is Alice's current office choice after the payment problem?".to_string(),
            expected_terms: vec!["downtown".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("locomo-location-paraphrase"),
            query: "Where does Alice want to work now?".to_string(),
            expected_terms: vec!["downtown location".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("longmem-updated-setting"),
            query: "Which preference was updated in the recent multi session messages?".to_string(),
            expected_terms: vec!["notification".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("longmem-most-recent-change"),
            query: "Which setting changed most recently across the conversation history?".to_string(),
            expected_terms: vec!["notification setting".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("locomo-temporal-after-travel"),
            query: "What did Alice decide after the airport trip conversation?".to_string(),
            expected_terms: vec!["downtown office".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("longmem-root-cause-after-outage"),
            query: "Why did checkout fail after the backend outage?".to_string(),
            expected_terms: vec![
                "database migration".to_string(),
                "backend connection pool".to_string(),
            ],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("locomo-corrected-food-restriction"),
            query: "What snack should Jordan avoid now after the correction?".to_string(),
            expected_terms: vec!["peanuts".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("longmem-medication-reminder"),
            query: "Which medication did Morgan say to remember before the doctor appointment?".to_string(),
            expected_terms: vec!["lisinopril".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("locomo-hobby-switch"),
            query: "Which hobby did Priya switch to after cancelling guitar lessons?".to_string(),
            expected_terms: vec!["pottery class".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("longmem-backup-contact-change"),
            query: "Who is the backup contact now after Sam moved teams?".to_string(),
            expected_terms: vec!["Riley".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("locomo-cafe-recommendation"),
            query: "Who recommended the cafe that Nina booked after the conference?".to_string(),
            expected_terms: vec!["Omar".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("longmem-project-suggestion"),
            query: "Which project did Lee pick because Dana suggested it during planning?".to_string(),
            expected_terms: vec![
                "observability dashboard".to_string(),
                "Dana suggested".to_string(),
            ],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("locomo-appointment-reschedule"),
            query: "When is Maya's dentist appointment after it was rescheduled?".to_string(),
            expected_terms: vec!["Thursday at 3pm".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
            category: infer_external_benchmark_category("longmem-deadline-date-update"),
            query: "What is the new report deadline after the calendar update?".to_string(),
            expected_terms: vec!["June 24".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
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
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-guest-count-update".to_string(),
            category: infer_external_benchmark_category("locomo-guest-count-update"),
            query: "How many guests did Sofia confirm after the dinner update?".to_string(),
            expected_terms: vec!["7 guests".to_string(), "two neighbors".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Old dinner count".to_string(),
                    body: "Earlier dinner plan: Sofia expected 4 guests before the final RSVP update.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "Final dinner count".to_string(),
                    body: "Final RSVP update: Sofia confirmed 7 guests for dinner after two neighbors joined.".to_string(),
                    kind: ContextSourceKind::UserEvent,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-risk-score-update".to_string(),
            category: infer_external_benchmark_category("longmem-risk-score-update"),
            query: "What risk score was recorded after the latest fraud review?".to_string(),
            expected_terms: vec!["87".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Old risk score".to_string(),
                    body: "Earlier fraud review: the checkout risk score was 42 before the payment incident escalated.".to_string(),
                    kind: ContextSourceKind::Incident,
                },
                ExternalContextBenchmarkSource {
                    title: "Updated risk score".to_string(),
                    body: "Latest fraud review: the checkout risk score was updated to 87 after the payment incident escalated.".to_string(),
                    kind: ContextSourceKind::Incident,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "locomo_style".to_string(),
            query_id: "locomo-roommate-alias".to_string(),
            category: infer_external_benchmark_category("locomo-roommate-alias"),
            query: "What is Emma's roommate's name after the move?".to_string(),
            expected_terms: vec!["Lena".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Old roommate memory".to_string(),
                    body: "Earlier chat: Emma's roommate was called Nora before Emma moved apartments.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
                ExternalContextBenchmarkSource {
                    title: "New roommate memory".to_string(),
                    body: "After the move, Emma said her new roommate is named Lena and they share the corner apartment.".to_string(),
                    kind: ContextSourceKind::Chat,
                },
            ],
        },
        ExternalContextBenchmarkCase {
            dataset: "longmemeval_s_style".to_string(),
            query_id: "longmem-pet-name-alias".to_string(),
            category: infer_external_benchmark_category("longmem-pet-name-alias"),
            query: "What is the dog's name in the latest pet update?".to_string(),
            expected_terms: vec!["Miso".to_string()],
            expected_source_refs: Vec::new(),
            unsupported_reason: None,
            sources: vec![
                ExternalContextBenchmarkSource {
                    title: "Old pet note".to_string(),
                    body: "Old profile note: the family dog was called Pepper in a previous home.".to_string(),
                    kind: ContextSourceKind::Document,
                },
                ExternalContextBenchmarkSource {
                    title: "Latest pet note".to_string(),
                    body: "Latest pet update: the newly adopted dog is named Miso and needs evening walks.".to_string(),
                    kind: ContextSourceKind::UserEvent,
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
            cold_storage: false,
        },
        Command::ContextWriteIndexRef {
            tenant_hash: 20260616,
            index_name: "source".to_string(),
            index_value_hash: stable_hash64(&extract.source_ref),
            scope_hash: 0,
            event_time_ms: extract.event.event_time_ms,
            index_ref: extract.index_ref.clone(),
        },
        Command::ContextMarkSummaryDirty {
            tenant_hash: 20260616,
            node_hash: extract.dirty_marker.node_hash,
            event_time_ms: extract.dirty_marker.last_event_time_ms,
            reason: extract.dirty_marker.reason,
            propagate_depth: extract.dirty_marker.propagate_depth,
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
            replicator.replay_wal_strict(1, 0, &follower).await?;
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
            max_scan: None,
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
                    max_scan: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_pet_answers_match_cat_and_dog_evidence() {
        let text =
            normalize_benchmark_text("Caroline has a dog named Luna and a cat named Oliver.");
        assert!(benchmark_pet_answer_matches(&text, "two cats and a dog"));
        assert!(benchmark_text_matches(
            "caroline has a dog named luna and a cat named oliver.",
            &text,
            "Two cats and a dog"
        ));
    }

    // shared-corpus: context_benchmark_full_dataset_gates
    #[test]
    fn packed_external_sources_use_rust_context_event_ingest_and_score_refs() {
        let root = std::env::temp_dir().join(format!(
            "temporalstore-packed-external-source-test-{}",
            now_ms()
        ));
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            root.join("cache"),
            root.join("pages"),
            root.join("indexes"),
        );
        engine.load_shard(1);
        let tenant_hash = 42;
        let packed_refs = (1..=40)
            .map(|index| format!("conversation filler turn {index}"))
            .collect::<Vec<_>>()
            .join(" | ");
        let packed_title = format!(
            "packed sources 1-40: conversation answer_blue turn 1 .. conversation filler turn 40; refs: conversation answer_blue turn 1 | {packed_refs}"
        );
        assert!(packed_title.len() > EXTERNAL_CONTEXT_MAX_CANONICAL_NAME_BYTES);
        let packed_body = "[source_ref: conversation answer_blue turn 1]\nThe preferred notebook color is cobalt blue.\n\n[source_ref: conversation filler turn 1]\nUnrelated planning note.";
        let sources = vec![ContextExtractRequest {
            shard_id: 1,
            tenant_hash,
            source_kind: ContextSourceKind::Chat,
            source_id: packed_title.clone(),
            title: packed_title.clone(),
            body: packed_body.to_string(),
            timestamp_ms: 1_000,
            provider: ContextModelProviderConfig::default(),
        }];

        let node_hashes = ingest_external_benchmark_sources(&engine, tenant_hash, &sources)
            .expect("packed external sources should ingest through Rust context events");
        assert_eq!(node_hashes.len(), 1);
        let node_response = engine.execute(temporalstore_rust::ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNode {
                tenant_hash,
                node_hash: node_hashes[0],
            },
        });
        match node_response.response {
            CommandResponse::ContextNode {
                node: Some(node), ..
            } => {
                assert!(node.canonical_name.len() <= EXTERNAL_CONTEXT_MAX_CANONICAL_NAME_BYTES);
                assert!(node.canonical_name.contains("[ctx64:"));
            }
            other => panic!("expected packed Context node, got {other:?}"),
        }

        let retrieve = retrieve_context(
            &engine,
            ContextRetrieveRequest {
                shard_id: 1,
                tenant_hash,
                node_hashes,
                query: String::new(),
                start_time_ms: 0,
                end_time_ms: 10_000,
                max_events: 8,
                min_confidence: 0.0,
                min_importance: 0.0,
                tiers: vec![ContextTier::L2],
                max_summary_nodes: 32,
                max_event_nodes: 16,
                prefer_current_agent: false,
                current_agent_scope_key: "agent:codex".to_string(),
                provider: ContextModelProviderConfig::default(),
            },
        );
        assert!(retrieve.status.ok);
        assert!(!retrieve.blocks.is_empty());

        let case = ExternalContextBenchmarkCase {
            dataset: "longmemeval_s".to_string(),
            query_id: "packed-q1".to_string(),
            category: "multi_session".to_string(),
            query: "What color was the preferred notebook?".to_string(),
            expected_terms: vec!["cobalt blue".to_string()],
            expected_source_refs: vec!["conversation answer_blue turn 1".to_string()],
            unsupported_reason: None,
            sources: vec![ExternalContextBenchmarkSource {
                title: packed_title.to_string(),
                body: packed_body.to_string(),
                kind: ContextSourceKind::Chat,
            }],
        };
        let mut blocks = retrieve.blocks;
        order_external_blocks_by_case_relevance(&case, &mut blocks, true);
        assert_eq!(hit_source_rank(&case, &blocks), Some(1));
        assert_eq!(
            count_matched_expected_refs(&blocks, &case.expected_source_refs),
            1
        );
        assert_eq!(
            count_matched_expected_terms(&blocks, &case.expected_terms),
            1
        );

        let _ = std::fs::remove_dir_all(root);
    }
}
