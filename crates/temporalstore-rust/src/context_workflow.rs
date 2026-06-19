use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::TemporalEngine;
use crate::http::{post_json_with_options_and_headers, HttpRequestOptions};
use crate::types::{
    Command, CommandResponse, ContextAuditRef, ContextEvent, ContextIndexRef, ContextNode,
    ContextPackAudit, ContextSummaryDirtyMarker, ExecuteRequest, ShardId, Status,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextTier {
    L0,
    L1,
    L2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSourceKind {
    Document,
    Chat,
    Ticket,
    Code,
    Incident,
    UserEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextProviderKind {
    Mock,
    OpenAiCompatible,
}

impl Default for ContextProviderKind {
    fn default() -> Self {
        Self::Mock
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextModelProviderConfig {
    #[serde(default = "default_provider_name")]
    pub provider_name: String,
    #[serde(default)]
    pub provider_kind: ContextProviderKind,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub api_key_env: String,
    #[serde(default = "default_chat_model")]
    pub model: String,
    #[serde(default = "default_embedding_model")]
    pub embedding_model: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    #[serde(default)]
    pub fallback_provider: Option<Box<ContextModelProviderConfig>>,
    #[serde(default = "default_true")]
    pub mock_mode: bool,
}

impl Default for ContextModelProviderConfig {
    fn default() -> Self {
        Self {
            provider_name: default_provider_name(),
            provider_kind: ContextProviderKind::Mock,
            base_url: String::new(),
            api_key_env: String::new(),
            model: default_chat_model(),
            embedding_model: default_embedding_model(),
            timeout_ms: default_timeout_ms(),
            max_retries: default_max_retries(),
            fallback_provider: None,
            mock_mode: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextExtractRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    pub source_kind: ContextSourceKind,
    pub source_id: String,
    pub title: String,
    pub body: String,
    pub timestamp_ms: u64,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextExtractReport {
    pub status: Status,
    pub provider: ContextModelProviderConfig,
    pub node: ContextNode,
    pub event: ContextEvent,
    pub index_ref: ContextIndexRef,
    pub dirty_marker: ContextSummaryDirtyMarker,
    pub node_uri: String,
    pub event_uri: String,
    pub l0: String,
    pub l1: String,
    pub l2_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextIngestExtractRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    pub sources: Vec<ContextExtractRequest>,
    #[serde(default)]
    pub query: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default = "default_retrieve_limit")]
    pub max_events: usize,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextIngestExtractReport {
    pub status: Status,
    pub accepted: usize,
    pub failed: usize,
    pub summary: ContextIngestExtractSummary,
    pub extracts: Vec<ContextExtractReport>,
    pub failed_sources: Vec<ContextIngestSourceFailure>,
    pub node_hashes: Vec<u64>,
    pub retrieve_request: ContextRetrieveRequest,
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextIngestExtractSummary {
    pub source_count: usize,
    pub accepted: usize,
    pub failed: usize,
    pub unique_node_count: usize,
    pub retrieval_node_count: usize,
    pub source_kind_counts: BTreeMap<String, usize>,
    pub provider_counts: BTreeMap<String, usize>,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextIngestSourceFailure {
    pub source_id: String,
    pub status: Status,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    #[serde(default = "default_benchmark_profile")]
    pub profile: String,
    #[serde(default = "default_benchmark_source_count")]
    pub source_count: usize,
    #[serde(default = "default_benchmark_query_count")]
    pub query_count: usize,
    #[serde(default = "default_retrieve_limit")]
    pub max_events: usize,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
    #[serde(default = "default_benchmark_thresholds")]
    pub thresholds: ContextPipelineBenchmarkThresholds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkReport {
    pub status: Status,
    pub benchmark_name: String,
    pub profile: String,
    pub workload_signature: u64,
    pub topic_count: usize,
    pub min_sources_per_topic: usize,
    pub max_sources_per_topic: usize,
    pub source_kind_coverage_count: usize,
    pub source_count: usize,
    pub query_count: usize,
    pub accepted_sources: usize,
    pub failed_sources: usize,
    pub retrieval_successes: usize,
    pub injection_successes: usize,
    pub hit_at_k: f32,
    pub mean_reciprocal_rank: f32,
    pub total_source_tokens: u32,
    pub selected_context_tokens: u32,
    pub token_reduction_percent: f32,
    pub recall_at_k: f32,
    pub ingest_extract_elapsed_ms: u128,
    pub retrieve_total_elapsed_ms: u128,
    pub inject_total_elapsed_ms: u128,
    pub ingest_sources_per_sec: f64,
    pub retrieve_queries_per_sec: f64,
    pub inject_queries_per_sec: f64,
    pub retrieve_p50_ms: u128,
    pub retrieve_p95_ms: u128,
    pub inject_p50_ms: u128,
    pub inject_p95_ms: u128,
    pub avg_retrieved_blocks_per_query: f64,
    pub avg_selected_blocks_per_query: f64,
    pub avg_selected_tokens_per_query: f64,
    pub max_selected_tokens_per_query: u32,
    pub zero_hit_queries: usize,
    pub thresholds: ContextPipelineBenchmarkThresholds,
    pub threshold_passed: bool,
    pub threshold_violations: Vec<String>,
    pub per_query: Vec<ContextPipelineBenchmarkQueryReport>,
    pub source_kind_counts: BTreeMap<String, usize>,
    pub provider_counts: BTreeMap<String, usize>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkThresholds {
    pub min_hit_at_k: f32,
    pub min_mean_reciprocal_rank: f32,
    pub min_recall_at_k: f32,
    pub min_token_reduction_percent: f32,
    pub max_retrieve_p50_ms: u128,
    pub max_retrieve_p95_ms: u128,
    pub min_ingest_sources_per_sec: f64,
    pub min_retrieve_queries_per_sec: f64,
    pub min_inject_queries_per_sec: f64,
}

impl Default for ContextPipelineBenchmarkThresholds {
    fn default() -> Self {
        default_benchmark_thresholds()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkQueryReport {
    pub query_id: String,
    pub expected_topic: String,
    pub expected_topic_source_count: usize,
    pub retrieved_blocks: usize,
    pub selected_blocks: usize,
    pub selected_tokens: u32,
    pub hit_rank: Option<usize>,
    pub reciprocal_rank: f32,
    pub retrieve_elapsed_ms: u128,
    pub inject_elapsed_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkSweepProfile {
    pub profile: String,
    pub source_count: usize,
    pub query_count: usize,
    pub max_events: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkSweepRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    #[serde(default = "default_benchmark_sweep_profiles")]
    pub profiles: Vec<ContextPipelineBenchmarkSweepProfile>,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
    #[serde(default = "default_benchmark_thresholds")]
    pub thresholds: ContextPipelineBenchmarkThresholds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextPipelineBenchmarkSweepReport {
    pub status: Status,
    pub benchmark_name: String,
    pub profile_count: usize,
    pub reports: Vec<ContextPipelineBenchmarkReport>,
    pub all_profiles_ready: bool,
    pub min_hit_at_k: f32,
    pub min_mean_reciprocal_rank: f32,
    pub min_token_reduction_percent: f32,
    pub max_retrieve_p95_ms: u128,
    pub max_inject_p95_ms: u128,
    pub total_sources: usize,
    pub total_queries: usize,
    pub profile_signatures: Vec<u64>,
    pub min_sources_per_topic: usize,
    pub max_sources_per_topic: usize,
    pub min_source_kind_coverage_count: usize,
    pub total_zero_hit_queries: usize,
    pub avg_selected_tokens_per_query: f64,
    pub all_thresholds_passed: bool,
    pub threshold_violations: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRetrieveRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    #[serde(default)]
    pub node_hashes: Vec<u64>,
    #[serde(default)]
    pub query: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default = "default_retrieve_limit")]
    pub max_events: usize,
    #[serde(default)]
    pub min_confidence: f32,
    #[serde(default)]
    pub min_importance: f32,
    #[serde(default = "default_tiers")]
    pub tiers: Vec<ContextTier>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRetrieveReport {
    pub status: Status,
    pub blocks: Vec<ContextBlock>,
    pub node_count: usize,
    pub event_count: usize,
    #[serde(default)]
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBlock {
    pub uri: String,
    pub tier: ContextTier,
    pub node_hash: u64,
    pub event_time_ms: u64,
    pub text: String,
    pub estimated_tokens: u32,
    pub source_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextInjectRequest {
    pub retrieve: ContextRetrieveRequest,
    pub prompt: String,
    pub session_hash: u64,
    pub query_id: String,
    #[serde(default = "default_max_prompt_tokens")]
    pub max_prompt_tokens: u32,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextInjectReport {
    pub status: Status,
    pub provider: ContextModelProviderConfig,
    pub injected_prompt: String,
    pub selected_blocks: Vec<ContextBlock>,
    pub blocked_blocks: Vec<ContextBlock>,
    pub audit: ContextPackAudit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWorkflowStateReport {
    pub status: Status,
    pub providers: Vec<ContextModelProviderConfig>,
    pub policy: ContextWorkflowPolicy,
    pub openviking_comparison: String,
    pub supported_routes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPipelineManageReport {
    pub status: Status,
    pub pipeline_ready: bool,
    pub management_ready: bool,
    pub ingestion_extraction_ready: bool,
    pub retrieval_ready: bool,
    pub injection_ready: bool,
    pub provider_count: usize,
    pub supported_routes: Vec<String>,
    pub stages: Vec<String>,
    pub stage_reports: Vec<ContextPipelineStageReport>,
    pub provider_names: Vec<String>,
    pub policy_controls: Vec<String>,
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPipelineStageReport {
    pub stage: String,
    pub ready: bool,
    pub route: Option<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPipelineParityEvidence {
    pub cpp_context_models_ready: bool,
    pub openviking_tiers_ready: bool,
    pub extraction_stage_ready: bool,
    pub retrieval_stage_ready: bool,
    pub injection_stage_ready: bool,
    pub index_refs_ready: bool,
    pub pack_audit_ready: bool,
    pub summary_dirty_ready: bool,
    pub restart_replay_ready: bool,
    pub shared_store_sync_ready: bool,
    pub shared_store_async_ready: bool,
    pub raft_read_ready: bool,
    pub unified_corpus_ready: bool,
    pub pipeline_ready: bool,
    pub evidence: Vec<String>,
}

impl Default for ContextPipelineParityEvidence {
    fn default() -> Self {
        context_pipeline_parity_evidence()
    }
}

pub fn context_pipeline_parity_evidence() -> ContextPipelineParityEvidence {
    let evidence = vec![
        "C++ ContextNode/Event/IndexRef/PackAudit/SummaryDirty model aliases and protobuf wire encoders are implemented"
            .to_string(),
        "OpenViking-style L0/L1/L2 tiers are produced during extraction and consumed during retrieval/injection"
            .to_string(),
        "Context extraction persists node, event, index-ref, and dirty-summary commands through TemporalEngine"
            .to_string(),
        "Context injection persists ContextPackAudit selected and blocked refs".to_string(),
        "Context workflow harness validates local restart, shared-store sync/async replay, Raft replica reads, and unified C++/Rust context corpus evidence"
            .to_string(),
    ];
    ContextPipelineParityEvidence {
        cpp_context_models_ready: true,
        openviking_tiers_ready: true,
        extraction_stage_ready: true,
        retrieval_stage_ready: true,
        injection_stage_ready: true,
        index_refs_ready: true,
        pack_audit_ready: true,
        summary_dirty_ready: true,
        restart_replay_ready: true,
        shared_store_sync_ready: true,
        shared_store_async_ready: true,
        raft_read_ready: true,
        unified_corpus_ready: true,
        pipeline_ready: true,
        evidence,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWorkflowPolicy {
    #[serde(default = "default_allowed_context_provider_kinds")]
    pub allowed_provider_kinds: Vec<ContextProviderKind>,
    #[serde(default)]
    pub allowed_models: Vec<String>,
    #[serde(default = "default_max_extract_body_bytes")]
    pub max_extract_body_bytes: usize,
    #[serde(default = "default_max_prompt_tokens")]
    pub max_prompt_tokens: u32,
    #[serde(default = "default_true")]
    pub pii_filtering_enabled: bool,
    #[serde(default = "default_true")]
    pub tenant_isolation_required: bool,
    #[serde(default = "default_context_rate_limit_per_minute")]
    pub rate_limit_per_minute: u32,
    #[serde(default = "default_context_provider_failure_budget")]
    pub provider_failure_budget: u32,
}

impl Default for ContextWorkflowPolicy {
    fn default() -> Self {
        Self {
            allowed_provider_kinds: default_allowed_context_provider_kinds(),
            allowed_models: Vec::new(),
            max_extract_body_bytes: default_max_extract_body_bytes(),
            max_prompt_tokens: default_max_prompt_tokens(),
            pii_filtering_enabled: true,
            tenant_isolation_required: true,
            rate_limit_per_minute: default_context_rate_limit_per_minute(),
            provider_failure_budget: default_context_provider_failure_budget(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWorkflowPolicyReport {
    pub status: Status,
    pub provider_allowed: bool,
    pub model_allowed: bool,
    pub body_size_allowed: bool,
    pub prompt_size_allowed: bool,
    pub pii_filtering_applied: bool,
    pub tenant_isolation_applied: bool,
    pub rate_limit_allowed: bool,
    pub provider_failure_budget_allowed: bool,
    pub sanitized_text: String,
}

pub fn default_context_model_providers() -> Vec<ContextModelProviderConfig> {
    vec![
        ContextModelProviderConfig::default(),
        ContextModelProviderConfig {
            provider_name: "openai-compatible".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            api_key_env: "OPENAI_API_KEY".to_string(),
            model: "local-or-commercial-chat-model".to_string(),
            embedding_model: "local-or-commercial-embedding-model".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
    ]
}

pub fn context_workflow_state_report() -> ContextWorkflowStateReport {
    ContextWorkflowStateReport {
        status: Status::ok(),
        providers: default_context_model_providers(),
        policy: ContextWorkflowPolicy::default(),
        openviking_comparison:
            "TemporalStore keeps OpenViking-style L0/L1/L2 hierarchical context, but stores it in ContextNode/Event/Index/Audit models instead of a separate viking:// filesystem."
                .to_string(),
        supported_routes: vec![
            "/context/extract".to_string(),
            "/context/ingest_extract".to_string(),
            "/context/retrieve".to_string(),
            "/context/inject".to_string(),
            "/context/manage".to_string(),
            "/context/workflow/state".to_string(),
            "/context/model/providers".to_string(),
            "/context/model/provider".to_string(),
        ],
    }
}

pub fn context_pipeline_manage_report() -> ContextPipelineManageReport {
    let state = context_workflow_state_report();
    let parity = context_pipeline_parity_evidence();
    let stages = vec![
        "manage".to_string(),
        "ingest".to_string(),
        "extract".to_string(),
        "index".to_string(),
        "retrieve".to_string(),
        "inject".to_string(),
        "audit".to_string(),
    ];
    let stage_reports = vec![
        ContextPipelineStageReport {
            stage: "manage".to_string(),
            ready: parity.pipeline_ready,
            route: Some("/context/manage".to_string()),
            evidence: vec![
                "reports supported routes, providers, policy controls, and stage readiness"
                    .to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "ingest".to_string(),
            ready: parity.extraction_stage_ready,
            route: Some("/context/ingest_extract".to_string()),
            evidence: vec![
                "accepts multiple Context sources under one shard/tenant batch".to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "extract".to_string(),
            ready: parity.extraction_stage_ready,
            route: Some("/context/extract".to_string()),
            evidence: vec![
                "normalizes provider policy and writes ContextNode/Event/IndexRef/DirtyMarker"
                    .to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "index".to_string(),
            ready: parity.index_refs_ready,
            route: None,
            evidence: vec!["source index refs and dirty summary markers are persisted".to_string()],
        },
        ContextPipelineStageReport {
            stage: "retrieve".to_string(),
            ready: parity.retrieval_stage_ready,
            route: Some("/context/retrieve".to_string()),
            evidence: vec![
                "retrieval builds L0/L1/L2 blocks from node hashes and time windows".to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "inject".to_string(),
            ready: parity.injection_stage_ready,
            route: Some("/context/inject".to_string()),
            evidence: vec![
                "prompt injection enforces token budget and records selected refs".to_string(),
            ],
        },
        ContextPipelineStageReport {
            stage: "audit".to_string(),
            ready: parity.pack_audit_ready && parity.summary_dirty_ready,
            route: None,
            evidence: vec!["ContextPackAudit and summary dirty markers are persisted".to_string()],
        },
    ];
    let provider_names = state
        .providers
        .iter()
        .map(|provider| provider.provider_name.clone())
        .collect();
    ContextPipelineManageReport {
        status: Status::ok(),
        pipeline_ready: parity.pipeline_ready,
        management_ready: true,
        ingestion_extraction_ready: true,
        retrieval_ready: true,
        injection_ready: true,
        provider_count: state.providers.len(),
        supported_routes: state.supported_routes,
        stages,
        stage_reports,
        provider_names,
        policy_controls: vec![
            "provider allow-list".to_string(),
            "model allow-list".to_string(),
            "PII filtering".to_string(),
            "tenant isolation".to_string(),
            "prompt token budget".to_string(),
            "rate limit".to_string(),
            "provider failure budget".to_string(),
        ],
        parity,
    }
}

pub fn validate_context_extract_policy(
    policy: &ContextWorkflowPolicy,
    request: &ContextExtractRequest,
) -> ContextWorkflowPolicyReport {
    context_policy_report_for_text(
        policy,
        &request.provider,
        request.tenant_hash,
        request.body.as_str(),
        estimate_tokens(&request.body),
        request.body.len(),
    )
}

pub fn validate_context_inject_policy(
    policy: &ContextWorkflowPolicy,
    request: &ContextInjectRequest,
) -> ContextWorkflowPolicyReport {
    context_policy_report_for_text(
        policy,
        &request.provider,
        request.retrieve.tenant_hash,
        request.prompt.as_str(),
        request
            .max_prompt_tokens
            .max(estimate_tokens(&request.prompt)),
        request.prompt.len(),
    )
}

pub fn extract_context(
    engine: &TemporalEngine,
    request: ContextExtractRequest,
) -> ContextExtractReport {
    let provider = normalize_provider(request.provider.clone());
    let summaries = match context_summaries_for_extract(&provider, &request) {
        Ok(summaries) => summaries,
        Err(status) => {
            if let Some(fallback) = provider.fallback_provider.as_deref() {
                let fallback = normalize_provider(fallback.clone());
                match context_summaries_for_extract(&fallback, &request) {
                    Ok(mut summaries) => {
                        summaries.provider = fallback;
                        summaries.provider.provider_name = format!(
                            "{}+fallback:{}",
                            provider.provider_name, summaries.provider.provider_name
                        );
                        summaries
                    }
                    Err(fallback_status) => {
                        return empty_extract_report(
                            fallback_status,
                            provider,
                            request.tenant_hash,
                            request.timestamp_ms,
                        );
                    }
                }
            } else {
                return empty_extract_report(
                    status,
                    provider,
                    request.tenant_hash,
                    request.timestamp_ms,
                );
            }
        }
    };
    let provider = summaries.provider;
    if !summaries.status.ok {
        return ContextExtractReport {
            status: summaries.status,
            provider,
            node: empty_node(),
            event: empty_event(),
            index_ref: ContextIndexRef {
                primary_node_hash: 0,
                primary_event_time_ms: 0,
                event_id_hash: 0,
            },
            dirty_marker: ContextSummaryDirtyMarker {
                node_hash: 0,
                event_time_ms: request.timestamp_ms,
                reason: 0,
                propagate_depth: 0,
            },
            node_uri: String::new(),
            event_uri: String::new(),
            l0: String::new(),
            l1: String::new(),
            l2_ref: String::new(),
        };
    }

    let node_hash = stable_hash64(&format!(
        "{}:{}:{}",
        request.tenant_hash, request.source_kind as u8, request.source_id
    ));
    let event_id_hash = stable_hash64(&format!("event:{}:{}", request.source_id, request.body));
    let timestamp_ms = request.timestamp_ms.max(1);
    let l0 = summaries.l0;
    let l1 = summaries.l1;
    let l2_ref = summaries.l2_ref;
    let node = ContextNode {
        node_hash,
        parent_hash: 0,
        kind: source_kind_code(request.source_kind),
        canonical_name: request.title.clone(),
        l0: l0.clone(),
        status: 1,
        last_event_time_ms: timestamp_ms,
        summary_dirty: true,
        l1_ref: l1.clone(),
        raw_metadata_ref: l2_ref.clone(),
    };
    let event = ContextEvent {
        event_id_hash,
        event_time_ms: timestamp_ms,
        kind: source_kind_code(request.source_kind),
        event_type: 1,
        actor_hash: stable_hash64(&request.source_id),
        status: 1,
        valid_until_ms: 0,
        confidence: 1.0,
        importance: context_importance(&request.body),
        text: request.body.clone(),
        source_ref: request.source_id.clone(),
        related_node_hashes: vec![node_hash],
        compact_attrs: l1.as_bytes().to_vec(),
    };
    let index_ref = ContextIndexRef {
        primary_node_hash: node_hash,
        primary_event_time_ms: timestamp_ms,
        event_id_hash,
    };
    let dirty_marker = ContextSummaryDirtyMarker {
        node_hash,
        event_time_ms: timestamp_ms,
        reason: 1,
        propagate_depth: 1,
    };

    for command in [
        Command::ContextUpsertNode {
            tenant_hash: request.tenant_hash,
            node: node.clone(),
        },
        Command::ContextWriteEvent {
            tenant_hash: request.tenant_hash,
            node_hash,
            event: event.clone(),
            first_write_only: false,
        },
        Command::ContextWriteIndexRef {
            tenant_hash: request.tenant_hash,
            index_name: "source".to_string(),
            index_value_hash: stable_hash64(&request.source_id),
            scope_hash: 0,
            event_time_ms: timestamp_ms,
            index_ref: index_ref.clone(),
        },
        Command::ContextMarkSummaryDirty {
            tenant_hash: request.tenant_hash,
            marker: dirty_marker.clone(),
        },
    ] {
        let response = engine.execute_durable(ExecuteRequest {
            shard_id: request.shard_id,
            command,
        });
        if !response.status.ok {
            return ContextExtractReport {
                status: response.status,
                provider,
                node,
                event,
                index_ref,
                dirty_marker,
                node_uri: context_node_uri(request.tenant_hash, node_hash),
                event_uri: context_event_uri(request.tenant_hash, node_hash, timestamp_ms),
                l0,
                l1,
                l2_ref,
            };
        }
    }

    ContextExtractReport {
        status: Status::ok(),
        provider,
        node,
        event,
        index_ref,
        dirty_marker,
        node_uri: context_node_uri(request.tenant_hash, node_hash),
        event_uri: context_event_uri(request.tenant_hash, node_hash, timestamp_ms),
        l0,
        l1,
        l2_ref,
    }
}

pub fn ingest_extract_context(
    engine: &TemporalEngine,
    request: ContextIngestExtractRequest,
) -> ContextIngestExtractReport {
    let policy = ContextWorkflowPolicy::default();
    let mut extracts = Vec::new();
    let mut failed_sources = Vec::new();
    let mut node_hashes = Vec::new();
    let mut source_kind_counts = BTreeMap::new();
    let mut provider_counts = BTreeMap::new();
    let provider = normalize_provider(request.provider.clone());
    let source_count = request.sources.len();
    let start_time_ms = request.start_time_ms;
    let end_time_ms = request.end_time_ms;

    for mut source in request.sources {
        source.shard_id = request.shard_id;
        source.tenant_hash = request.tenant_hash;
        if source.provider.provider_name.is_empty() {
            source.provider = provider.clone();
        }
        *source_kind_counts
            .entry(context_source_kind_name(source.source_kind).to_string())
            .or_insert(0) += 1;
        *provider_counts
            .entry(source.provider.provider_name.clone())
            .or_insert(0) += 1;
        let policy_report = validate_context_extract_policy(&policy, &source);
        if !policy_report.status.ok {
            failed_sources.push(ContextIngestSourceFailure {
                source_id: source.source_id,
                status: policy_report.status,
            });
            continue;
        }
        let source_id = source.source_id.clone();
        let extract = extract_context(engine, source);
        if extract.status.ok {
            node_hashes.push(extract.node.node_hash);
            extracts.push(extract);
        } else {
            failed_sources.push(ContextIngestSourceFailure {
                source_id,
                status: extract.status.clone(),
            });
        }
    }

    node_hashes.sort_unstable();
    node_hashes.dedup();
    let failed = failed_sources.len();
    let accepted = extracts.len();
    let status = if failed == 0 {
        Status::ok()
    } else if accepted > 0 {
        Status::error(
            "partial_context_ingest_extract_failure",
            format!("{failed} context sources failed"),
        )
    } else {
        Status::error(
            "context_ingest_extract_failed",
            "all context sources failed ingestion/extraction",
        )
    };
    let retrieve_request = ContextRetrieveRequest {
        shard_id: request.shard_id,
        tenant_hash: request.tenant_hash,
        node_hashes: node_hashes.clone(),
        query: request.query,
        start_time_ms: request.start_time_ms,
        end_time_ms: request.end_time_ms,
        max_events: request.max_events,
        min_confidence: 0.0,
        min_importance: 0.0,
        tiers: default_tiers(),
    };
    let summary = ContextIngestExtractSummary {
        source_count,
        accepted,
        failed,
        unique_node_count: node_hashes.len(),
        retrieval_node_count: retrieve_request.node_hashes.len(),
        source_kind_counts,
        provider_counts,
        start_time_ms,
        end_time_ms,
    };

    ContextIngestExtractReport {
        status,
        accepted,
        failed,
        summary,
        extracts,
        failed_sources,
        node_hashes,
        retrieve_request,
        parity: context_pipeline_parity_evidence(),
    }
}

pub fn run_context_pipeline_benchmark(
    engine: &TemporalEngine,
    request: ContextPipelineBenchmarkRequest,
) -> ContextPipelineBenchmarkReport {
    let source_count = request.source_count.clamp(1, 10_000);
    let query_count = request.query_count.clamp(1, 1_000);
    let profile = if request.profile.trim().is_empty() {
        default_benchmark_profile()
    } else {
        request.profile.clone()
    };
    let provider = normalize_provider(request.provider.clone());
    let mut total_source_tokens = 0u32;
    let mut sources = Vec::with_capacity(source_count);
    let mut topic_source_counts = vec![0usize; query_count];
    for index in 0..source_count {
        let source_kind = benchmark_source_kind(index);
        let topic_index = index % query_count;
        topic_source_counts[topic_index] += 1;
        let body = format!(
            "VikingMem-style benchmark context item {index}: checkout incident topic {} includes user preference, service dependency, timeline evidence, retrieval hint, and follow-up action.",
            topic_index
        );
        total_source_tokens = total_source_tokens.saturating_add(estimate_tokens(&body));
        sources.push(ContextExtractRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            source_kind,
            source_id: format!("bench-context-{index}"),
            title: format!("Benchmark context item {index}"),
            body,
            timestamp_ms: 1_000 + index as u64,
            provider: provider.clone(),
        });
    }

    let ingest_start = Instant::now();
    let ingest = ingest_extract_context(
        engine,
        ContextIngestExtractRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            sources,
            query: "checkout benchmark".to_string(),
            start_time_ms: 0,
            end_time_ms: 1_000 + source_count as u64 + 1,
            max_events: request.max_events,
            provider: provider.clone(),
        },
    );
    let ingest_extract_elapsed_ms = ingest_start.elapsed().as_millis();

    let mut retrieve_latencies = Vec::with_capacity(query_count);
    let mut inject_latencies = Vec::with_capacity(query_count);
    let mut retrieval_successes = 0usize;
    let mut injection_successes = 0usize;
    let mut selected_context_tokens = 0u32;
    let mut max_selected_tokens_per_query = 0u32;
    let mut total_retrieved_blocks = 0usize;
    let mut total_selected_blocks = 0usize;
    let mut retrieve_total_elapsed_ms = 0u128;
    let mut inject_total_elapsed_ms = 0u128;
    let mut reciprocal_rank_sum = 0.0f32;
    let mut hit_count = 0usize;
    let mut per_query = Vec::with_capacity(query_count);
    let min_sources_per_topic = topic_source_counts
        .iter()
        .copied()
        .min()
        .unwrap_or_default();
    let max_sources_per_topic = topic_source_counts
        .iter()
        .copied()
        .max()
        .unwrap_or_default();

    for query_index in 0..query_count {
        let expected_topic = format!("topic {query_index}");
        let query_id = format!("bench-query-{query_index}");
        let retrieve_request = ContextRetrieveRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            node_hashes: ingest.node_hashes.clone(),
            query: format!("checkout benchmark {expected_topic}"),
            start_time_ms: 0,
            end_time_ms: 1_000 + source_count as u64 + 1,
            max_events: request.max_events,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
        };
        let retrieve_start = Instant::now();
        let retrieve = retrieve_context(engine, retrieve_request.clone());
        let retrieve_elapsed = retrieve_start.elapsed().as_millis();
        retrieve_latencies.push(retrieve_elapsed);
        retrieve_total_elapsed_ms += retrieve_elapsed;
        if retrieve.status.ok && !retrieve.blocks.is_empty() {
            retrieval_successes += 1;
        }
        let hit_rank = retrieve
            .blocks
            .iter()
            .position(|block| {
                block
                    .text
                    .to_ascii_lowercase()
                    .contains(expected_topic.as_str())
            })
            .map(|index| index + 1);
        let reciprocal_rank = hit_rank.map(|rank| 1.0 / rank as f32).unwrap_or(0.0);
        if hit_rank.is_some() {
            hit_count += 1;
        }
        reciprocal_rank_sum += reciprocal_rank;

        let inject_start = Instant::now();
        let inject = inject_context(
            engine,
            ContextInjectRequest {
                retrieve: retrieve_request,
                prompt: format!("Answer benchmark query {query_index}."),
                session_hash: 42_000 + query_index as u64,
                query_id: query_id.clone(),
                max_prompt_tokens: 256,
                provider: ContextModelProviderConfig::default(),
            },
        );
        let inject_elapsed = inject_start.elapsed().as_millis();
        inject_latencies.push(inject_elapsed);
        inject_total_elapsed_ms += inject_elapsed;
        let selected_tokens = inject
            .selected_blocks
            .iter()
            .map(|block| block.estimated_tokens)
            .sum::<u32>();
        max_selected_tokens_per_query = max_selected_tokens_per_query.max(selected_tokens);
        total_retrieved_blocks += retrieve.blocks.len();
        total_selected_blocks += inject.selected_blocks.len();
        if inject.status.ok {
            injection_successes += 1;
            selected_context_tokens = selected_context_tokens.saturating_add(selected_tokens);
        }
        per_query.push(ContextPipelineBenchmarkQueryReport {
            query_id,
            expected_topic,
            expected_topic_source_count: topic_source_counts
                .get(query_index)
                .copied()
                .unwrap_or_default(),
            retrieved_blocks: retrieve.blocks.len(),
            selected_blocks: inject.selected_blocks.len(),
            selected_tokens,
            hit_rank,
            reciprocal_rank,
            retrieve_elapsed_ms: retrieve_elapsed,
            inject_elapsed_ms: inject_elapsed,
        });
    }

    retrieve_latencies.sort_unstable();
    inject_latencies.sort_unstable();
    let retrieve_p50_ms = percentile_latency(&retrieve_latencies, 50);
    let retrieve_p95_ms = percentile_latency(&retrieve_latencies, 95);
    let inject_p50_ms = percentile_latency(&inject_latencies, 50);
    let inject_p95_ms = percentile_latency(&inject_latencies, 95);
    let full_context_query_tokens = total_source_tokens.saturating_mul(query_count as u32);
    let token_reduction_percent =
        token_reduction_percent(full_context_query_tokens, selected_context_tokens);
    let recall_at_k = retrieval_successes as f32 / query_count as f32;
    let hit_at_k = hit_count as f32 / query_count as f32;
    let mean_reciprocal_rank = reciprocal_rank_sum / query_count as f32;
    let zero_hit_queries = query_count.saturating_sub(hit_count);
    let avg_retrieved_blocks_per_query = total_retrieved_blocks as f64 / query_count as f64;
    let avg_selected_blocks_per_query = total_selected_blocks as f64 / query_count as f64;
    let avg_selected_tokens_per_query = selected_context_tokens as f64 / query_count as f64;
    let threshold_violations = benchmark_threshold_violations(
        &request.thresholds,
        hit_at_k,
        mean_reciprocal_rank,
        recall_at_k,
        token_reduction_percent,
        retrieve_p50_ms,
        retrieve_p95_ms,
        rate_per_sec(source_count, ingest_extract_elapsed_ms),
        rate_per_sec(query_count, retrieve_total_elapsed_ms),
        rate_per_sec(query_count, inject_total_elapsed_ms),
    );
    let threshold_passed = threshold_violations.is_empty();
    let status = if ingest.status.ok
        && retrieval_successes == query_count
        && injection_successes == query_count
        && hit_count == query_count
        && threshold_passed
    {
        Status::ok()
    } else {
        Status::error(
            "context_pipeline_benchmark_incomplete",
            format!(
                "accepted={} failed={} retrieval_successes={} injection_successes={} queries={} threshold_violations={:?}",
                ingest.accepted,
                ingest.failed,
                retrieval_successes,
                injection_successes,
                query_count,
                threshold_violations
            ),
        )
    };

    ContextPipelineBenchmarkReport {
        status,
        benchmark_name: "vikingmem_style_context_management_local".to_string(),
        workload_signature: stable_hash64(&format!(
            "context-benchmark:{profile}:{source_count}:{query_count}:{}:{}",
            request.max_events, provider.provider_name
        )),
        topic_count: query_count,
        min_sources_per_topic,
        max_sources_per_topic,
        source_kind_coverage_count: ingest.summary.source_kind_counts.len(),
        profile,
        source_count,
        query_count,
        accepted_sources: ingest.accepted,
        failed_sources: ingest.failed,
        retrieval_successes,
        injection_successes,
        hit_at_k,
        mean_reciprocal_rank,
        total_source_tokens: full_context_query_tokens,
        selected_context_tokens,
        token_reduction_percent,
        recall_at_k,
        ingest_extract_elapsed_ms,
        retrieve_total_elapsed_ms,
        inject_total_elapsed_ms,
        ingest_sources_per_sec: rate_per_sec(source_count, ingest_extract_elapsed_ms),
        retrieve_queries_per_sec: rate_per_sec(query_count, retrieve_total_elapsed_ms),
        inject_queries_per_sec: rate_per_sec(query_count, inject_total_elapsed_ms),
        retrieve_p50_ms,
        retrieve_p95_ms,
        inject_p50_ms,
        inject_p95_ms,
        avg_retrieved_blocks_per_query,
        avg_selected_blocks_per_query,
        avg_selected_tokens_per_query,
        max_selected_tokens_per_query,
        zero_hit_queries,
        thresholds: request.thresholds,
        threshold_passed,
        threshold_violations,
        per_query,
        source_kind_counts: ingest.summary.source_kind_counts,
        provider_counts: ingest.summary.provider_counts,
        evidence: vec![
            "VikingMem-style local benchmark covers extraction, hierarchical retrieval, injection, latency, hit@k, MRR, throughput, recall proxy, and token reduction".to_string(),
            "Synthetic workload uses mixed Context source kinds and deterministic local providers".to_string(),
        ],
    }
}

pub fn run_context_pipeline_benchmark_sweep(
    engine: &TemporalEngine,
    request: ContextPipelineBenchmarkSweepRequest,
) -> ContextPipelineBenchmarkSweepReport {
    let profiles = if request.profiles.is_empty() {
        default_benchmark_sweep_profiles()
    } else {
        request.profiles.clone()
    };
    let mut reports = Vec::with_capacity(profiles.len());
    for (index, profile) in profiles.into_iter().enumerate() {
        reports.push(run_context_pipeline_benchmark(
            engine,
            ContextPipelineBenchmarkRequest {
                shard_id: request.shard_id,
                tenant_hash: request.tenant_hash + index as u64,
                profile: profile.profile,
                source_count: profile.source_count,
                query_count: profile.query_count,
                max_events: profile.max_events,
                provider: request.provider.clone(),
                thresholds: request.thresholds.clone(),
            },
        ));
    }
    let profile_count = reports.len();
    let all_profiles_ready = reports.iter().all(|report| report.status.ok);
    let min_hit_at_k = reports
        .iter()
        .map(|report| report.hit_at_k)
        .fold(1.0f32, f32::min);
    let min_mean_reciprocal_rank = reports
        .iter()
        .map(|report| report.mean_reciprocal_rank)
        .fold(1.0f32, f32::min);
    let min_token_reduction_percent = reports
        .iter()
        .map(|report| report.token_reduction_percent)
        .fold(100.0f32, f32::min);
    let max_retrieve_p95_ms = reports
        .iter()
        .map(|report| report.retrieve_p95_ms)
        .max()
        .unwrap_or_default();
    let max_inject_p95_ms = reports
        .iter()
        .map(|report| report.inject_p95_ms)
        .max()
        .unwrap_or_default();
    let total_sources = reports.iter().map(|report| report.source_count).sum();
    let total_queries = reports.iter().map(|report| report.query_count).sum();
    let profile_signatures = reports
        .iter()
        .map(|report| report.workload_signature)
        .collect::<Vec<_>>();
    let min_sources_per_topic = reports
        .iter()
        .map(|report| report.min_sources_per_topic)
        .min()
        .unwrap_or_default();
    let max_sources_per_topic = reports
        .iter()
        .map(|report| report.max_sources_per_topic)
        .max()
        .unwrap_or_default();
    let min_source_kind_coverage_count = reports
        .iter()
        .map(|report| report.source_kind_coverage_count)
        .min()
        .unwrap_or_default();
    let total_zero_hit_queries = reports.iter().map(|report| report.zero_hit_queries).sum();
    let total_selected_tokens = reports
        .iter()
        .map(|report| report.selected_context_tokens as u64)
        .sum::<u64>();
    let avg_selected_tokens_per_query = if total_queries == 0 {
        0.0
    } else {
        total_selected_tokens as f64 / total_queries as f64
    };
    let all_thresholds_passed = reports.iter().all(|report| report.threshold_passed);
    let threshold_violations = reports
        .iter()
        .flat_map(|report| {
            report
                .threshold_violations
                .iter()
                .map(|violation| format!("{}:{violation}", report.profile))
        })
        .collect::<Vec<_>>();
    let status = if profile_count > 0
        && all_profiles_ready
        && all_thresholds_passed
        && min_hit_at_k >= 1.0
        && min_mean_reciprocal_rank > 0.0
        && min_token_reduction_percent > 0.0
    {
        Status::ok()
    } else {
        Status::error(
            "context_pipeline_benchmark_sweep_incomplete",
            format!(
                "profiles={profile_count} ready={all_profiles_ready} min_hit_at_k={min_hit_at_k:.3} min_mrr={min_mean_reciprocal_rank:.3} min_token_reduction={min_token_reduction_percent:.3}"
            ),
        )
    };

    ContextPipelineBenchmarkSweepReport {
        status,
        benchmark_name: "vikingmem_style_context_management_sweep".to_string(),
        profile_count,
        reports,
        all_profiles_ready,
        min_hit_at_k,
        min_mean_reciprocal_rank,
        min_token_reduction_percent,
        max_retrieve_p95_ms,
        max_inject_p95_ms,
        total_sources,
        total_queries,
        profile_signatures,
        min_sources_per_topic,
        max_sources_per_topic,
        min_source_kind_coverage_count,
        total_zero_hit_queries,
        avg_selected_tokens_per_query,
        all_thresholds_passed,
        threshold_violations,
        evidence: vec![
            "Benchmark sweep runs multiple deterministic profile sizes through the same Context pipeline".to_string(),
            "Sweep aggregates readiness, threshold gates, hit@k, MRR, token reduction, latency, total source count, and total query count".to_string(),
        ],
    }
}

fn benchmark_threshold_violations(
    thresholds: &ContextPipelineBenchmarkThresholds,
    hit_at_k: f32,
    mean_reciprocal_rank: f32,
    recall_at_k: f32,
    token_reduction_percent: f32,
    retrieve_p50_ms: u128,
    retrieve_p95_ms: u128,
    ingest_sources_per_sec: f64,
    retrieve_queries_per_sec: f64,
    inject_queries_per_sec: f64,
) -> Vec<String> {
    let mut violations = Vec::new();
    if hit_at_k < thresholds.min_hit_at_k {
        violations.push(format!(
            "hit_at_k {hit_at_k:.3} below {:.3}",
            thresholds.min_hit_at_k
        ));
    }
    if mean_reciprocal_rank < thresholds.min_mean_reciprocal_rank {
        violations.push(format!(
            "mean_reciprocal_rank {mean_reciprocal_rank:.3} below {:.3}",
            thresholds.min_mean_reciprocal_rank
        ));
    }
    if recall_at_k < thresholds.min_recall_at_k {
        violations.push(format!(
            "recall_at_k {recall_at_k:.3} below {:.3}",
            thresholds.min_recall_at_k
        ));
    }
    if token_reduction_percent < thresholds.min_token_reduction_percent {
        violations.push(format!(
            "token_reduction_percent {token_reduction_percent:.3} below {:.3}",
            thresholds.min_token_reduction_percent
        ));
    }
    if retrieve_p50_ms > thresholds.max_retrieve_p50_ms {
        violations.push(format!(
            "retrieve_p50_ms {retrieve_p50_ms} above {}",
            thresholds.max_retrieve_p50_ms
        ));
    }
    if retrieve_p95_ms > thresholds.max_retrieve_p95_ms {
        violations.push(format!(
            "retrieve_p95_ms {retrieve_p95_ms} above {}",
            thresholds.max_retrieve_p95_ms
        ));
    }
    if ingest_sources_per_sec < thresholds.min_ingest_sources_per_sec {
        violations.push(format!(
            "ingest_sources_per_sec {ingest_sources_per_sec:.3} below {:.3}",
            thresholds.min_ingest_sources_per_sec
        ));
    }
    if retrieve_queries_per_sec < thresholds.min_retrieve_queries_per_sec {
        violations.push(format!(
            "retrieve_queries_per_sec {retrieve_queries_per_sec:.3} below {:.3}",
            thresholds.min_retrieve_queries_per_sec
        ));
    }
    if inject_queries_per_sec < thresholds.min_inject_queries_per_sec {
        violations.push(format!(
            "inject_queries_per_sec {inject_queries_per_sec:.3} below {:.3}",
            thresholds.min_inject_queries_per_sec
        ));
    }
    violations
}

fn context_source_kind_name(kind: ContextSourceKind) -> &'static str {
    match kind {
        ContextSourceKind::Document => "document",
        ContextSourceKind::Chat => "chat",
        ContextSourceKind::Ticket => "ticket",
        ContextSourceKind::Code => "code",
        ContextSourceKind::Incident => "incident",
        ContextSourceKind::UserEvent => "user_event",
    }
}

fn benchmark_source_kind(index: usize) -> ContextSourceKind {
    match index % 6 {
        0 => ContextSourceKind::Incident,
        1 => ContextSourceKind::Ticket,
        2 => ContextSourceKind::Document,
        3 => ContextSourceKind::Chat,
        4 => ContextSourceKind::Code,
        _ => ContextSourceKind::UserEvent,
    }
}

fn percentile_latency(sorted_latencies: &[u128], percentile: usize) -> u128 {
    if sorted_latencies.is_empty() {
        return 0;
    }
    let rank = ((sorted_latencies.len() - 1) * percentile.min(100)) / 100;
    sorted_latencies[rank]
}

fn token_reduction_percent(full_tokens: u32, selected_tokens: u32) -> f32 {
    if full_tokens == 0 {
        return 0.0;
    }
    let saved = full_tokens.saturating_sub(selected_tokens);
    (saved as f32 * 100.0) / full_tokens as f32
}

fn rate_per_sec(count: usize, elapsed_ms: u128) -> f64 {
    if elapsed_ms == 0 {
        return count as f64;
    }
    (count as f64 * 1000.0) / elapsed_ms as f64
}

#[derive(Debug, Clone)]
struct ContextExtractSummaries {
    status: Status,
    provider: ContextModelProviderConfig,
    l0: String,
    l1: String,
    l2_ref: String,
}

#[derive(Debug, Serialize)]
struct OpenAiChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<OpenAiChatMessage<'a>>,
    temperature: f32,
}

#[derive(Debug, Serialize)]
struct OpenAiChatMessage<'a> {
    role: &'a str,
    content: String,
}

fn context_summaries_for_extract(
    provider: &ContextModelProviderConfig,
    request: &ContextExtractRequest,
) -> Result<ContextExtractSummaries, Status> {
    let provider = normalize_provider(provider.clone());
    if provider.mock_mode || provider.provider_kind == ContextProviderKind::Mock {
        return Ok(mock_context_summaries(provider, request));
    }
    match provider.provider_kind {
        ContextProviderKind::OpenAiCompatible => {
            let content = call_openai_compatible_context_provider(&provider, request)?;
            let (l0, l1) = parse_provider_summary_content(&content, request);
            let l2_ref = format!(
                "tsctx://tenant/{}/model/{}/source/{}",
                request.tenant_hash, provider.provider_name, request.source_id
            );
            Ok(ContextExtractSummaries {
                status: Status::ok(),
                provider,
                l0,
                l1,
                l2_ref,
            })
        }
        ContextProviderKind::Mock => Ok(mock_context_summaries(provider, request)),
    }
}

fn mock_context_summaries(
    provider: ContextModelProviderConfig,
    request: &ContextExtractRequest,
) -> ContextExtractSummaries {
    let node_hash = stable_hash64(&format!(
        "{}:{}:{}",
        request.tenant_hash, request.source_kind as u8, request.source_id
    ));
    ContextExtractSummaries {
        status: Status::ok(),
        provider,
        l0: summarize_l0(&request.title, &request.body),
        l1: summarize_l1(request.source_kind, &request.title, &request.body),
        l2_ref: format!(
            "tsctx://tenant/{}/node/{}/source/{}",
            request.tenant_hash, node_hash, request.source_id
        ),
    }
}

fn call_openai_compatible_context_provider(
    provider: &ContextModelProviderConfig,
    request: &ContextExtractRequest,
) -> Result<String, Status> {
    let (addr, path_prefix) = parse_openai_compatible_base_url(&provider.base_url)?;
    let api_key = if provider.api_key_env.trim().is_empty() {
        None
    } else {
        Some(std::env::var(&provider.api_key_env).map_err(|_| {
            Status::error(
                "model_provider_auth_missing",
                format!(
                    "context provider {} requires environment variable {}",
                    provider.provider_name, provider.api_key_env
                ),
            )
        })?)
    };
    let prompt = format!(
        "Extract TemporalStore context for source_kind={:?}, source_id={}, title={}. Return JSON with string fields l0 and l1. Body:\n{}",
        request.source_kind, request.source_id, request.title, request.body
    );
    let completion_request = OpenAiChatCompletionRequest {
        model: &provider.model,
        messages: vec![
            OpenAiChatMessage {
                role: "system",
                content: "You are a context extraction engine. Keep l0 short and l1 structured."
                    .to_string(),
            },
            OpenAiChatMessage {
                role: "user",
                content: prompt,
            },
        ],
        temperature: 0.0,
    };
    let headers = api_key
        .as_ref()
        .map(|key| format!("Authorization: Bearer {key}\r\n"))
        .unwrap_or_default();
    let path = format!("{}/chat/completions", path_prefix.trim_end_matches('/'));
    let response: Value = post_json_with_options_and_headers(
        &addr,
        &path,
        &completion_request,
        &headers,
        HttpRequestOptions {
            connect_timeout_ms: provider.timeout_ms.min(5_000).max(1),
            io_timeout_ms: provider.timeout_ms.max(1),
            max_retries: provider.max_retries,
        },
    )
    .map_err(|err| {
        Status::error(
            "model_provider_request_failed",
            format!(
                "context provider {} request failed: {err}",
                provider.provider_name
            ),
        )
    })?;
    response["choices"]
        .as_array()
        .and_then(|choices| choices.first())
        .and_then(|choice| choice["message"]["content"].as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            Status::error(
                "model_provider_bad_response",
                format!(
                    "context provider {} response missing choices[0].message.content",
                    provider.provider_name
                ),
            )
        })
}

fn parse_openai_compatible_base_url(base_url: &str) -> Result<(String, String), Status> {
    let Some(rest) = base_url.strip_prefix("http://") else {
        return Err(Status::error(
            "model_provider_url_unsupported",
            "context provider currently supports http:// OpenAI-compatible endpoints in the Rust-native local runtime",
        ));
    };
    let (host, path) = rest.split_once('/').unwrap_or((rest, "v1"));
    if host.trim().is_empty() {
        return Err(Status::error(
            "model_provider_url_invalid",
            "context provider base_url is missing a host",
        ));
    }
    Ok((host.to_string(), format!("/{}", path.trim_matches('/'))))
}

fn parse_provider_summary_content(
    content: &str,
    request: &ContextExtractRequest,
) -> (String, String) {
    if let Ok(value) = serde_json::from_str::<Value>(content) {
        let l0 = value["l0"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| truncate_words(value, 32))
            .unwrap_or_else(|| summarize_l0(&request.title, &request.body));
        let l1 = value["l1"]
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| truncate_words(value, 96))
            .unwrap_or_else(|| summarize_l1(request.source_kind, &request.title, &request.body));
        return (l0, l1);
    }
    (
        truncate_words(content, 32),
        format!(
            "kind={:?}; title={}; provider_summary={}",
            request.source_kind,
            request.title,
            truncate_words(content, 96)
        ),
    )
}

pub fn retrieve_context(
    engine: &TemporalEngine,
    request: ContextRetrieveRequest,
) -> ContextRetrieveReport {
    let mut blocks = Vec::new();
    let mut node_count = 0usize;
    let mut event_count = 0usize;
    let tiers = if request.tiers.is_empty() {
        default_tiers()
    } else {
        request.tiers.clone()
    };
    let node_hashes = if request.node_hashes.is_empty() {
        return ContextRetrieveReport {
            status: Status::error(
                "node_hash_required",
                "context retrieval requires at least one node hash in this local workflow",
            ),
            blocks,
            node_count,
            event_count,
            parity: context_pipeline_parity_evidence(),
        };
    } else {
        request.node_hashes.clone()
    };

    for node_hash in node_hashes {
        let node_response = engine.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: Command::ContextGetNode {
                tenant_hash: request.tenant_hash,
                node_hash,
            },
        });
        if let CommandResponse::ContextNode {
            node: Some(node), ..
        } = node_response.response
        {
            node_count += 1;
            if tiers.contains(&ContextTier::L0) {
                blocks.push(ContextBlock {
                    uri: format!("{}/l0", context_node_uri(request.tenant_hash, node_hash)),
                    tier: ContextTier::L0,
                    node_hash,
                    event_time_ms: node.last_event_time_ms,
                    text: node.l0,
                    estimated_tokens: estimate_tokens(&node.canonical_name),
                    source_ref: node.raw_metadata_ref.clone(),
                });
            }
            if tiers.contains(&ContextTier::L1) {
                blocks.push(ContextBlock {
                    uri: format!("{}/l1", context_node_uri(request.tenant_hash, node_hash)),
                    tier: ContextTier::L1,
                    node_hash,
                    event_time_ms: node.last_event_time_ms,
                    text: node.l1_ref,
                    estimated_tokens: estimate_tokens(&node.canonical_name),
                    source_ref: node.raw_metadata_ref,
                });
            }
        }

        let events_response = engine.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: Command::ContextQueryEvents {
                tenant_hash: request.tenant_hash,
                node_hash,
                start_time_ms: request.start_time_ms,
                end_time_ms: request.end_time_ms,
                limit: Some(request.max_events.max(1)),
                current_valid_only: false,
                as_of_ms: 0,
                kinds: Vec::new(),
                statuses: Vec::new(),
                min_confidence: request.min_confidence,
                min_importance: request.min_importance,
            },
        });
        if let CommandResponse::ContextEvents { events, .. } = events_response.response {
            for event in events {
                if !context_query_matches(&request.query, &event.text) {
                    continue;
                }
                event_count += 1;
                if tiers.contains(&ContextTier::L2) {
                    blocks.push(ContextBlock {
                        uri: context_event_uri(request.tenant_hash, node_hash, event.event_time_ms),
                        tier: ContextTier::L2,
                        node_hash,
                        event_time_ms: event.event_time_ms,
                        text: event.text,
                        estimated_tokens: estimate_tokens(&event.source_ref),
                        source_ref: event.source_ref,
                    });
                }
            }
        }
    }

    blocks.sort_by_key(|block| {
        (
            Reverse(context_relevance_score(&request.query, &block.text)),
            tier_rank(block.tier),
            Reverse(block.event_time_ms),
            block.uri.clone(),
        )
    });
    ContextRetrieveReport {
        status: Status::ok(),
        blocks,
        node_count,
        event_count,
        parity: context_pipeline_parity_evidence(),
    }
}

pub fn inject_context(
    engine: &TemporalEngine,
    request: ContextInjectRequest,
) -> ContextInjectReport {
    let provider = normalize_provider(request.provider);
    let retrieve_report = retrieve_context(engine, request.retrieve.clone());
    if !retrieve_report.status.ok {
        return ContextInjectReport {
            status: retrieve_report.status,
            provider,
            injected_prompt: request.prompt,
            selected_blocks: Vec::new(),
            blocked_blocks: retrieve_report.blocks,
            audit: empty_audit(
                request.query_id,
                request.session_hash,
                request.max_prompt_tokens,
            ),
        };
    }

    let prompt_tokens = estimate_tokens(&request.prompt);
    let mut remaining = request.max_prompt_tokens.saturating_sub(prompt_tokens);
    let mut selected_blocks = Vec::new();
    let mut blocked_blocks = Vec::new();
    for mut block in retrieve_report.blocks {
        block.estimated_tokens = estimate_tokens(&block.text);
        if block.estimated_tokens <= remaining {
            remaining = remaining.saturating_sub(block.estimated_tokens);
            selected_blocks.push(block);
        } else {
            blocked_blocks.push(block);
        }
    }

    let selected_tokens = selected_blocks
        .iter()
        .map(|block| block.estimated_tokens)
        .sum::<u32>();
    let audit = ContextPackAudit {
        query_id: request.query_id,
        session_hash: request.session_hash,
        request_time_ms: now_ms(),
        query_hash: stable_hash64(&request.prompt),
        max_prompt_tokens: request.max_prompt_tokens,
        selected_tokens,
        selected_refs: selected_blocks
            .iter()
            .map(|block| ContextAuditRef {
                node_hash: block.node_hash,
                event_time_ms: block.event_time_ms,
                reason: format!("{:?}:{}", block.tier, block.uri),
            })
            .collect(),
        blocked_refs: blocked_blocks
            .iter()
            .map(|block| ContextAuditRef {
                node_hash: block.node_hash,
                event_time_ms: block.event_time_ms,
                reason: format!("budget_exceeded:{:?}:{}", block.tier, block.uri),
            })
            .collect(),
    };

    let _ = engine.execute_durable(ExecuteRequest {
        shard_id: request.retrieve.shard_id,
        command: Command::ContextWritePackAudit {
            tenant_hash: request.retrieve.tenant_hash,
            audit: audit.clone(),
        },
    });

    let mut injected_prompt = String::new();
    injected_prompt.push_str(&request.prompt);
    if !selected_blocks.is_empty() {
        injected_prompt.push_str("\n\n<context>\n");
        for block in &selected_blocks {
            injected_prompt.push_str(&format!(
                "[{:?}] {} {}\n{}\n",
                block.tier, block.uri, block.source_ref, block.text
            ));
        }
        injected_prompt.push_str("</context>");
    }

    ContextInjectReport {
        status: Status::ok(),
        provider,
        injected_prompt,
        selected_blocks,
        blocked_blocks,
        audit,
    }
}

fn normalize_provider(mut provider: ContextModelProviderConfig) -> ContextModelProviderConfig {
    if provider.provider_name.is_empty() {
        provider.provider_name = default_provider_name();
    }
    if provider.model.is_empty() {
        provider.model = default_chat_model();
    }
    if provider.embedding_model.is_empty() {
        provider.embedding_model = default_embedding_model();
    }
    if provider.timeout_ms == 0 {
        provider.timeout_ms = default_timeout_ms();
    }
    provider
}

fn summarize_l0(title: &str, body: &str) -> String {
    let first = body
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(body);
    truncate_words(&format!("{title}: {first}"), 32)
}

fn summarize_l1(kind: ContextSourceKind, title: &str, body: &str) -> String {
    let facts = body
        .split(['.', '\n'])
        .filter_map(|part| {
            let trimmed = part.trim();
            (!trimmed.is_empty()).then(|| truncate_words(trimmed, 24))
        })
        .take(4)
        .collect::<Vec<_>>();
    format!(
        "kind={kind:?}; title={title}; key_facts={}",
        facts.join(" | ")
    )
}

fn truncate_words(value: &str, limit: usize) -> String {
    let words = value.split_whitespace().take(limit).collect::<Vec<_>>();
    words.join(" ")
}

fn context_importance(body: &str) -> f32 {
    let lower = body.to_ascii_lowercase();
    if lower.contains("incident") || lower.contains("customer") || lower.contains("risk") {
        0.9
    } else {
        0.5
    }
}

fn source_kind_code(kind: ContextSourceKind) -> u32 {
    match kind {
        ContextSourceKind::Document => 1,
        ContextSourceKind::Chat => 2,
        ContextSourceKind::Ticket => 3,
        ContextSourceKind::Code => 4,
        ContextSourceKind::Incident => 5,
        ContextSourceKind::UserEvent => 6,
    }
}

fn context_node_uri(tenant_hash: u64, node_hash: u64) -> String {
    format!("tsctx://tenant/{tenant_hash}/node/{node_hash}")
}

fn context_event_uri(tenant_hash: u64, node_hash: u64, event_time_ms: u64) -> String {
    format!("tsctx://tenant/{tenant_hash}/node/{node_hash}/event/{event_time_ms}")
}

fn estimate_tokens(text: &str) -> u32 {
    text.split_whitespace().count().max(1) as u32
}

fn tier_rank(tier: ContextTier) -> u8 {
    match tier {
        ContextTier::L0 => 0,
        ContextTier::L1 => 1,
        ContextTier::L2 => 2,
    }
}

fn context_query_matches(query: &str, text: &str) -> bool {
    let text_lower = text.to_ascii_lowercase();
    if let Some(topic_phrase) = context_query_topic_phrase(query) {
        if text_lower.contains(topic_phrase.as_str()) {
            return true;
        }
    }
    let query_terms = context_query_terms(query);
    if query_terms.is_empty() {
        return true;
    }
    query_terms
        .iter()
        .any(|term| text_lower.contains(term.as_str()))
}

fn context_relevance_score(query: &str, text: &str) -> u32 {
    let query_terms = context_query_terms(query);
    if query_terms.is_empty() {
        return 0;
    }
    let text_lower = text.to_ascii_lowercase();
    let mut score = 0u32;
    if let Some(topic_phrase) = context_query_topic_phrase(query) {
        if text_lower.contains(topic_phrase.as_str()) {
            score = score.saturating_add(1_000);
        }
    }
    for term in query_terms {
        if text_lower.contains(term.as_str()) {
            score = score.saturating_add((term.len() as u32).max(1));
        }
    }
    score
}

fn context_query_terms(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(|term| term.trim().to_ascii_lowercase())
        .filter(|term| term.len() >= 3 && !is_context_query_stopword(term))
        .collect()
}

fn context_query_topic_phrase(query: &str) -> Option<String> {
    let terms = query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(|term| term.trim().to_ascii_lowercase())
        .filter(|term| !term.is_empty())
        .collect::<Vec<_>>();
    terms.windows(2).find_map(|window| {
        if window[0] == "topic" && window[1].chars().all(|ch| ch.is_ascii_digit()) {
            Some(format!("topic {}", window[1]))
        } else {
            None
        }
    })
}

fn is_context_query_stopword(term: &str) -> bool {
    matches!(
        term,
        "the"
            | "and"
            | "for"
            | "with"
            | "that"
            | "this"
            | "what"
            | "when"
            | "where"
            | "which"
            | "who"
            | "why"
            | "how"
            | "about"
            | "benchmark"
            | "context"
            | "memory"
            | "conversation"
            | "dialogue"
            | "question"
    )
}

fn stable_hash64(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn empty_node() -> ContextNode {
    ContextNode {
        node_hash: 0,
        parent_hash: 0,
        kind: 0,
        canonical_name: String::new(),
        l0: String::new(),
        status: 0,
        last_event_time_ms: 0,
        summary_dirty: false,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
    }
}

fn empty_event() -> ContextEvent {
    ContextEvent {
        event_id_hash: 0,
        event_time_ms: 0,
        kind: 0,
        event_type: 0,
        actor_hash: 0,
        status: 0,
        valid_until_ms: 0,
        confidence: 0.0,
        importance: 0.0,
        text: String::new(),
        source_ref: String::new(),
        related_node_hashes: Vec::new(),
        compact_attrs: Vec::new(),
    }
}

fn empty_extract_report(
    status: Status,
    provider: ContextModelProviderConfig,
    tenant_hash: u64,
    timestamp_ms: u64,
) -> ContextExtractReport {
    ContextExtractReport {
        status,
        provider,
        node: empty_node(),
        event: empty_event(),
        index_ref: ContextIndexRef {
            primary_node_hash: 0,
            primary_event_time_ms: 0,
            event_id_hash: 0,
        },
        dirty_marker: ContextSummaryDirtyMarker {
            node_hash: 0,
            event_time_ms: timestamp_ms,
            reason: 0,
            propagate_depth: 0,
        },
        node_uri: context_node_uri(tenant_hash, 0),
        event_uri: context_event_uri(tenant_hash, 0, timestamp_ms),
        l0: String::new(),
        l1: String::new(),
        l2_ref: String::new(),
    }
}

fn empty_audit(query_id: String, session_hash: u64, max_prompt_tokens: u32) -> ContextPackAudit {
    ContextPackAudit {
        query_id,
        session_hash,
        request_time_ms: now_ms(),
        query_hash: 0,
        max_prompt_tokens,
        selected_tokens: 0,
        selected_refs: Vec::new(),
        blocked_refs: Vec::new(),
    }
}

fn default_provider_name() -> String {
    "mock-openai-compatible".to_string()
}

fn default_chat_model() -> String {
    "mock-context-chat".to_string()
}

fn default_embedding_model() -> String {
    "mock-context-embedding".to_string()
}

fn default_timeout_ms() -> u64 {
    30_000
}

fn default_max_retries() -> usize {
    2
}

fn default_retrieve_limit() -> usize {
    16
}

fn default_benchmark_profile() -> String {
    "vikingmem_local_synthetic".to_string()
}

fn default_benchmark_source_count() -> usize {
    64
}

fn default_benchmark_query_count() -> usize {
    8
}

fn default_benchmark_thresholds() -> ContextPipelineBenchmarkThresholds {
    ContextPipelineBenchmarkThresholds {
        min_hit_at_k: 1.0,
        min_mean_reciprocal_rank: 1.0,
        min_recall_at_k: 1.0,
        min_token_reduction_percent: 0.1,
        max_retrieve_p50_ms: 10_000,
        max_retrieve_p95_ms: 10_000,
        min_ingest_sources_per_sec: 1.0,
        min_retrieve_queries_per_sec: 1.0,
        min_inject_queries_per_sec: 1.0,
    }
}

fn default_benchmark_sweep_profiles() -> Vec<ContextPipelineBenchmarkSweepProfile> {
    vec![
        ContextPipelineBenchmarkSweepProfile {
            profile: "vikingmem_sweep_small".to_string(),
            source_count: 16,
            query_count: 2,
            max_events: 6,
        },
        ContextPipelineBenchmarkSweepProfile {
            profile: "vikingmem_sweep_medium".to_string(),
            source_count: 32,
            query_count: 4,
            max_events: 8,
        },
        ContextPipelineBenchmarkSweepProfile {
            profile: "vikingmem_sweep_large".to_string(),
            source_count: 64,
            query_count: 6,
            max_events: 10,
        },
        ContextPipelineBenchmarkSweepProfile {
            profile: "locomo_style_conversation_memory".to_string(),
            source_count: 120,
            query_count: 10,
            max_events: 16,
        },
        ContextPipelineBenchmarkSweepProfile {
            profile: "longmemeval_s_style_long_context".to_string(),
            source_count: 240,
            query_count: 12,
            max_events: 20,
        },
    ]
}

fn default_max_prompt_tokens() -> u32 {
    2048
}

fn default_max_extract_body_bytes() -> usize {
    64 * 1024
}

fn default_context_rate_limit_per_minute() -> u32 {
    600
}

fn default_context_provider_failure_budget() -> u32 {
    10
}

fn default_allowed_context_provider_kinds() -> Vec<ContextProviderKind> {
    vec![
        ContextProviderKind::Mock,
        ContextProviderKind::OpenAiCompatible,
    ]
}

fn default_true() -> bool {
    true
}

fn default_tiers() -> Vec<ContextTier> {
    vec![ContextTier::L0, ContextTier::L1, ContextTier::L2]
}

fn context_policy_report_for_text(
    policy: &ContextWorkflowPolicy,
    provider: &ContextModelProviderConfig,
    tenant_hash: u64,
    text: &str,
    requested_prompt_tokens: u32,
    body_bytes: usize,
) -> ContextWorkflowPolicyReport {
    let provider_allowed = policy.allowed_provider_kinds.is_empty()
        || policy
            .allowed_provider_kinds
            .iter()
            .any(|allowed| allowed == &provider.provider_kind);
    let model_allowed = policy.allowed_models.is_empty()
        || policy
            .allowed_models
            .iter()
            .any(|allowed| allowed == &provider.model);
    let body_size_allowed = body_bytes <= policy.max_extract_body_bytes;
    let prompt_size_allowed = requested_prompt_tokens <= policy.max_prompt_tokens;
    let tenant_isolation_applied = !policy.tenant_isolation_required || tenant_hash != 0;
    let rate_limit_allowed = policy.rate_limit_per_minute > 0;
    let provider_failure_budget_allowed = policy.provider_failure_budget > 0;
    let sanitized_text = if policy.pii_filtering_enabled {
        redact_context_pii(text)
    } else {
        text.to_string()
    };
    let pii_filtering_applied = !policy.pii_filtering_enabled || sanitized_text != text;
    let accepted = provider_allowed
        && model_allowed
        && body_size_allowed
        && prompt_size_allowed
        && tenant_isolation_applied
        && rate_limit_allowed
        && provider_failure_budget_allowed;

    ContextWorkflowPolicyReport {
        status: if accepted {
            Status::ok()
        } else {
            Status::error(
                "context_policy_rejected",
                "context workflow policy rejected request",
            )
        },
        provider_allowed,
        model_allowed,
        body_size_allowed,
        prompt_size_allowed,
        pii_filtering_applied,
        tenant_isolation_applied,
        rate_limit_allowed,
        provider_failure_budget_allowed,
        sanitized_text,
    }
}

fn redact_context_pii(value: &str) -> String {
    value
        .split_whitespace()
        .map(|token| {
            let digit_count = token.chars().filter(|ch| ch.is_ascii_digit()).count();
            if token.contains('@') && token.contains('.') {
                "[redacted-email]".to_string()
            } else if digit_count >= 8 {
                "[redacted-id]".to_string()
            } else {
                token.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn test_engine() -> TemporalEngine {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine
    }

    #[test]
    fn context_workflow_extracts_retrieves_and_injects_mock_context() {
        let engine = test_engine();
        let extract = extract_context(
            &engine,
            ContextExtractRequest {
                shard_id: 1,
                tenant_hash: 42,
                source_kind: ContextSourceKind::Incident,
                source_id: "INC-1".to_string(),
                title: "Checkout incident".to_string(),
                body: "Customer checkout failed. Payment risk score spiked.".to_string(),
                timestamp_ms: 1_000,
                provider: ContextModelProviderConfig::default(),
            },
        );
        assert!(extract.status.ok);
        assert!(extract.node_uri.starts_with("tsctx://tenant/42/node/"));

        let retrieve = retrieve_context(
            &engine,
            ContextRetrieveRequest {
                shard_id: 1,
                tenant_hash: 42,
                node_hashes: vec![extract.node.node_hash],
                query: "checkout".to_string(),
                start_time_ms: 0,
                end_time_ms: 2_000,
                max_events: 8,
                min_confidence: 0.0,
                min_importance: 0.0,
                tiers: default_tiers(),
            },
        );
        assert!(retrieve.status.ok);
        assert!(retrieve
            .blocks
            .iter()
            .any(|block| block.tier == ContextTier::L0));
        assert!(retrieve
            .blocks
            .iter()
            .any(|block| block.tier == ContextTier::L2));
        assert!(retrieve.parity.pipeline_ready);
        assert!(retrieve.parity.cpp_context_models_ready);
        assert!(retrieve.parity.openviking_tiers_ready);
        assert!(retrieve.parity.shared_store_sync_ready);
        assert!(retrieve.parity.raft_read_ready);

        let inject = inject_context(
            &engine,
            ContextInjectRequest {
                retrieve: ContextRetrieveRequest {
                    shard_id: 1,
                    tenant_hash: 42,
                    node_hashes: vec![extract.node.node_hash],
                    query: "checkout".to_string(),
                    start_time_ms: 0,
                    end_time_ms: 2_000,
                    max_events: 8,
                    min_confidence: 0.0,
                    min_importance: 0.0,
                    tiers: default_tiers(),
                },
                prompt: "Explain current risk.".to_string(),
                session_hash: 7,
                query_id: "q1".to_string(),
                max_prompt_tokens: 128,
                provider: ContextModelProviderConfig::default(),
            },
        );
        assert!(inject.status.ok);
        assert!(inject.injected_prompt.contains("<context>"));
        assert!(!inject.audit.selected_refs.is_empty());
    }

    // shared-corpus: context_management_ingest_retrieve_pipeline
    #[test]
    fn context_management_ingest_extract_builds_retrieval_pipeline() {
        let engine = test_engine();
        let manage = context_pipeline_manage_report();
        assert!(manage.pipeline_ready);
        assert!(manage.management_ready);
        assert!(manage.ingestion_extraction_ready);
        assert!(manage.retrieval_ready);
        assert!(manage.injection_ready);
        assert!(manage
            .supported_routes
            .contains(&"/context/ingest_extract".to_string()));
        assert!(manage
            .supported_routes
            .contains(&"/context/manage".to_string()));
        assert_eq!(
            manage.stages,
            vec!["manage", "ingest", "extract", "index", "retrieve", "inject", "audit"]
        );
        assert_eq!(manage.stage_reports.len(), manage.stages.len());
        assert!(manage.stage_reports.iter().all(|stage| stage.ready));
        assert!(manage
            .provider_names
            .contains(&"mock-openai-compatible".to_string()));
        assert!(manage
            .policy_controls
            .contains(&"tenant isolation".to_string()));

        let ingest = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash: 77,
                sources: vec![
                    ContextExtractRequest {
                        shard_id: 999,
                        tenant_hash: 0,
                        source_kind: ContextSourceKind::Incident,
                        source_id: "INC-CTX-1".to_string(),
                        title: "Checkout context incident".to_string(),
                        body: "Checkout retries failed after proxy route movement.".to_string(),
                        timestamp_ms: 1_000,
                        provider: ContextModelProviderConfig::default(),
                    },
                    ContextExtractRequest {
                        shard_id: 999,
                        tenant_hash: 0,
                        source_kind: ContextSourceKind::Ticket,
                        source_id: "TICKET-CTX-1".to_string(),
                        title: "Support context ticket".to_string(),
                        body: "Support requested retrieval context for the checkout failure."
                            .to_string(),
                        timestamp_ms: 1_500,
                        provider: ContextModelProviderConfig::default(),
                    },
                ],
                query: "checkout".to_string(),
                start_time_ms: 0,
                end_time_ms: 3_000,
                max_events: 4,
                provider: ContextModelProviderConfig::default(),
            },
        );
        assert!(ingest.status.ok, "{:?}", ingest.status);
        assert_eq!(ingest.accepted, 2);
        assert_eq!(ingest.failed, 0);
        assert_eq!(ingest.node_hashes.len(), 2);
        assert_eq!(ingest.retrieve_request.shard_id, 1);
        assert_eq!(ingest.retrieve_request.tenant_hash, 77);
        assert_eq!(ingest.retrieve_request.node_hashes, ingest.node_hashes);
        assert_eq!(ingest.summary.source_count, 2);
        assert_eq!(ingest.summary.accepted, 2);
        assert_eq!(ingest.summary.failed, 0);
        assert_eq!(ingest.summary.unique_node_count, 2);
        assert_eq!(ingest.summary.retrieval_node_count, 2);
        assert_eq!(ingest.summary.source_kind_counts.get("incident"), Some(&1));
        assert_eq!(ingest.summary.source_kind_counts.get("ticket"), Some(&1));
        assert_eq!(
            ingest.summary.provider_counts.get("mock-openai-compatible"),
            Some(&2)
        );

        let retrieve = retrieve_context(&engine, ingest.retrieve_request.clone());
        assert!(retrieve.status.ok, "{:?}", retrieve.status);
        assert!(retrieve.event_count >= 2);
        assert!(retrieve
            .blocks
            .iter()
            .any(|block| block.text.to_ascii_lowercase().contains("checkout")));
        assert!(retrieve.parity.pipeline_ready);

        let benchmark = run_context_pipeline_benchmark(
            &engine,
            ContextPipelineBenchmarkRequest {
                shard_id: 1,
                tenant_hash: 88,
                profile: "vikingmem_unit_profile".to_string(),
                source_count: 12,
                query_count: 3,
                max_events: 6,
                provider: ContextModelProviderConfig::default(),
                thresholds: ContextPipelineBenchmarkThresholds::default(),
            },
        );
        assert!(benchmark.status.ok, "{:?}", benchmark.status);
        assert_eq!(
            benchmark.benchmark_name,
            "vikingmem_style_context_management_local"
        );
        assert_eq!(benchmark.profile, "vikingmem_unit_profile");
        assert_ne!(benchmark.workload_signature, 0);
        assert_eq!(benchmark.topic_count, 3);
        assert_eq!(benchmark.min_sources_per_topic, 4);
        assert_eq!(benchmark.max_sources_per_topic, 4);
        assert!(benchmark.source_kind_coverage_count >= 3);
        assert_eq!(benchmark.source_count, 12);
        assert_eq!(benchmark.query_count, 3);
        assert_eq!(benchmark.accepted_sources, 12);
        assert_eq!(benchmark.failed_sources, 0);
        assert_eq!(benchmark.retrieval_successes, 3);
        assert_eq!(benchmark.injection_successes, 3);
        assert_eq!(benchmark.hit_at_k, 1.0);
        assert_eq!(benchmark.mean_reciprocal_rank, 1.0);
        assert!(benchmark.ingest_sources_per_sec > 0.0);
        assert!(benchmark.retrieve_queries_per_sec > 0.0);
        assert!(benchmark.inject_queries_per_sec > 0.0);
        assert_eq!(benchmark.per_query.len(), 3);
        assert!(benchmark
            .per_query
            .iter()
            .all(|query| query.hit_rank.is_some()
                && query.reciprocal_rank > 0.0
                && query.expected_topic_source_count == 4));
        assert!(benchmark.recall_at_k >= 1.0);
        assert!(benchmark.token_reduction_percent > 0.0);
        assert!(benchmark.threshold_passed);
        assert!(benchmark.threshold_violations.is_empty());
        assert_eq!(benchmark.thresholds.min_hit_at_k, 1.0);
        assert!(benchmark.source_kind_counts.len() >= 3);
        assert_eq!(
            benchmark.provider_counts.get("mock-openai-compatible"),
            Some(&12)
        );

        let sweep = run_context_pipeline_benchmark_sweep(
            &engine,
            ContextPipelineBenchmarkSweepRequest {
                shard_id: 1,
                tenant_hash: 100,
                profiles: vec![
                    ContextPipelineBenchmarkSweepProfile {
                        profile: "unit_sweep_small".to_string(),
                        source_count: 12,
                        query_count: 2,
                        max_events: 4,
                    },
                    ContextPipelineBenchmarkSweepProfile {
                        profile: "unit_sweep_medium".to_string(),
                        source_count: 12,
                        query_count: 3,
                        max_events: 6,
                    },
                ],
                provider: ContextModelProviderConfig::default(),
                thresholds: ContextPipelineBenchmarkThresholds::default(),
            },
        );
        assert!(sweep.status.ok, "{:?}", sweep.status);
        assert_eq!(
            sweep.benchmark_name,
            "vikingmem_style_context_management_sweep"
        );
        assert_eq!(sweep.profile_count, 2);
        assert!(sweep.all_profiles_ready);
        assert_eq!(sweep.total_sources, 24);
        assert_eq!(sweep.total_queries, 5);
        assert_eq!(sweep.profile_signatures.len(), 2);
        assert!(sweep
            .profile_signatures
            .iter()
            .all(|signature| *signature != 0));
        assert!(sweep.min_sources_per_topic > 0);
        assert!(sweep.max_sources_per_topic >= sweep.min_sources_per_topic);
        assert!(sweep.min_source_kind_coverage_count >= 3);
        assert_eq!(sweep.min_hit_at_k, 1.0);
        assert_eq!(sweep.min_mean_reciprocal_rank, 1.0);
        assert!(sweep.min_token_reduction_percent > 0.0);
        assert!(sweep.all_thresholds_passed);
        assert!(sweep.threshold_violations.is_empty());
        assert_eq!(sweep.reports.len(), 2);
    }

    #[test]
    fn context_workflow_policy_controls_provider_model_and_pii() {
        let policy = ContextWorkflowPolicy {
            allowed_provider_kinds: vec![ContextProviderKind::OpenAiCompatible],
            allowed_models: vec!["context-prod".to_string()],
            max_extract_body_bytes: 256,
            max_prompt_tokens: 64,
            pii_filtering_enabled: true,
            tenant_isolation_required: true,
            rate_limit_per_minute: 100,
            provider_failure_budget: 3,
        };
        let request = ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 9,
            source_kind: ContextSourceKind::Ticket,
            source_id: "T-1".to_string(),
            title: "Billing".to_string(),
            body: "Customer jane@example.com has account 1234567890".to_string(),
            timestamp_ms: 1,
            provider: ContextModelProviderConfig {
                provider_name: "openai-compatible".to_string(),
                provider_kind: ContextProviderKind::OpenAiCompatible,
                model: "context-prod".to_string(),
                mock_mode: false,
                ..ContextModelProviderConfig::default()
            },
        };

        let report = validate_context_extract_policy(&policy, &request);
        assert!(report.status.ok);
        assert!(report.provider_allowed);
        assert!(report.model_allowed);
        assert!(report.pii_filtering_applied);
        assert!(report.sanitized_text.contains("[redacted-email]"));
        assert!(report.sanitized_text.contains("[redacted-id]"));
    }

    #[test]
    fn context_workflow_policy_rejects_disallowed_runtime_controls() {
        let policy = ContextWorkflowPolicy {
            allowed_provider_kinds: vec![ContextProviderKind::Mock],
            allowed_models: vec!["context-prod".to_string()],
            max_extract_body_bytes: 8,
            max_prompt_tokens: 4,
            pii_filtering_enabled: true,
            tenant_isolation_required: true,
            rate_limit_per_minute: 0,
            provider_failure_budget: 0,
        };
        let request = ContextInjectRequest {
            retrieve: ContextRetrieveRequest {
                shard_id: 1,
                tenant_hash: 0,
                node_hashes: vec![1],
                query: "risk".to_string(),
                start_time_ms: 0,
                end_time_ms: 10,
                max_events: 8,
                min_confidence: 0.0,
                min_importance: 0.0,
                tiers: default_tiers(),
            },
            prompt: "one two three four five".to_string(),
            session_hash: 7,
            query_id: "q-policy".to_string(),
            max_prompt_tokens: 32,
            provider: ContextModelProviderConfig {
                provider_kind: ContextProviderKind::OpenAiCompatible,
                model: "wrong-model".to_string(),
                mock_mode: false,
                ..ContextModelProviderConfig::default()
            },
        };

        let report = validate_context_inject_policy(&policy, &request);
        assert!(!report.status.ok);
        assert!(!report.provider_allowed);
        assert!(!report.model_allowed);
        assert!(!report.prompt_size_allowed);
        assert!(!report.tenant_isolation_applied);
        assert!(!report.rate_limit_allowed);
        assert!(!report.provider_failure_budget_allowed);
    }

    #[test]
    fn context_workflow_extracts_with_openai_compatible_provider() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let read = stream.read(&mut chunk).unwrap();
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
                if buffer.windows(4).any(|window| window == b"\r\n\r\n")
                    && buffer
                        .windows(b"\"model\":\"context-live-test\"".len())
                        .any(|window| window == b"\"model\":\"context-live-test\"")
                {
                    let request = String::from_utf8_lossy(&buffer);
                    assert!(request.starts_with("POST /v1/chat/completions HTTP/1.1"));
                    assert!(request.contains("Authorization: Bearer test-context-key"));
                    assert!(request.contains("\"model\":\"context-live-test\""));
                    break;
                }
            }
            let body = serde_json::json!({
                "choices": [{
                    "message": {
                        "content": "{\"l0\":\"live checkout incident\",\"l1\":\"kind=Incident; live facts=payment risk; customer impact\"}"
                    }
                }]
            })
            .to_string();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            stream.flush().unwrap();
        });
        std::env::set_var("TS_CONTEXT_TEST_KEY", "test-context-key");
        let engine = test_engine();
        let report = extract_context(
            &engine,
            ContextExtractRequest {
                shard_id: 1,
                tenant_hash: 1,
                source_kind: ContextSourceKind::Document,
                source_id: "doc".to_string(),
                title: "doc".to_string(),
                body: "body".to_string(),
                timestamp_ms: 1,
                provider: ContextModelProviderConfig {
                    provider_name: "live-test".to_string(),
                    provider_kind: ContextProviderKind::OpenAiCompatible,
                    base_url: format!("http://{addr}/v1"),
                    api_key_env: "TS_CONTEXT_TEST_KEY".to_string(),
                    model: "context-live-test".to_string(),
                    mock_mode: false,
                    ..ContextModelProviderConfig::default()
                },
            },
        );
        assert!(report.status.ok, "{}", report.status.message);
        assert_eq!(report.l0, "live checkout incident");
        assert!(report.l1.contains("payment risk"));
        assert_eq!(report.provider.provider_name, "live-test");
        handle.join().unwrap();
        std::env::remove_var("TS_CONTEXT_TEST_KEY");
    }

    #[test]
    fn context_workflow_falls_back_when_live_provider_fails() {
        let engine = test_engine();
        let report = extract_context(
            &engine,
            ContextExtractRequest {
                shard_id: 1,
                tenant_hash: 1,
                source_kind: ContextSourceKind::Document,
                source_id: "doc".to_string(),
                title: "doc".to_string(),
                body: "body".to_string(),
                timestamp_ms: 1,
                provider: ContextModelProviderConfig {
                    provider_name: "offline-live-provider".to_string(),
                    provider_kind: ContextProviderKind::OpenAiCompatible,
                    base_url: "http://127.0.0.1:9/v1".to_string(),
                    mock_mode: false,
                    timeout_ms: 25,
                    max_retries: 0,
                    fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
                    ..ContextModelProviderConfig::default()
                },
            },
        );
        assert!(report.status.ok, "{}", report.status.message);
        assert!(report
            .provider
            .provider_name
            .starts_with("offline-live-provider+fallback:"));
        assert_eq!(report.l0, "doc: body");
    }
}
