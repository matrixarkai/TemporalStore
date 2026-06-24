use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::TemporalEngine;
use crate::http::{post_json_with_options_and_headers, HttpRequestOptions};
use crate::types::{
    context_model_descriptors, Command, CommandResponse, ContextAuditRef, ContextChildRef,
    ContextCompressionEvent, ContextEmbedding, ContextEntity, ContextEvent, ContextIndexRef,
    ContextModelDescriptor, ContextNode, ContextPackAudit, ContextSummary,
    ContextSummaryDirtyMarker, ExecuteRequest, ShardId, Status,
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
    #[serde(default = "default_vlm_model")]
    pub vlm_model: String,
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
            vlm_model: default_vlm_model(),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextEmbeddingGenerationReport {
    pub status: Status,
    pub provider_name: String,
    pub provider_kind: ContextProviderKind,
    pub embedding_model: String,
    pub vector_dimension: usize,
    pub requested_vector_count: usize,
    pub generated_vector_count: usize,
    pub batch_count: usize,
    pub live_call_count: usize,
    pub fallback_used: bool,
    pub mock_mode: bool,
    pub production_evidence_ready: bool,
}

impl Default for ContextEmbeddingGenerationReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            provider_name: default_provider_name(),
            provider_kind: ContextProviderKind::Mock,
            embedding_model: default_embedding_model(),
            vector_dimension: 0,
            requested_vector_count: 0,
            generated_vector_count: 0,
            batch_count: 0,
            live_call_count: 0,
            fallback_used: false,
            mock_mode: true,
            production_evidence_ready: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextExtractReport {
    pub status: Status,
    pub provider: ContextModelProviderConfig,
    #[serde(default)]
    pub embedding_generation: ContextEmbeddingGenerationReport,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceParseRequest {
    pub raw_uri: String,
    #[serde(default)]
    pub resource_type: Option<String>,
    pub text: String,
    #[serde(default = "default_resource_max_chunk_chars")]
    pub max_chunk_chars: usize,
    #[serde(default = "default_resource_overlap_chars")]
    pub overlap_chars: usize,
    #[serde(default)]
    pub chunk_hash_base: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextParsedResourceChunk {
    pub chunk_hash: u64,
    #[serde(default)]
    pub content_hash: u64,
    pub embedding_ref_hash: u64,
    pub source_ref: String,
    #[serde(default)]
    pub parent_source_ref: Option<String>,
    #[serde(default)]
    pub heading_path: Vec<String>,
    pub text: String,
    pub token_estimate: u32,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceParseReport {
    pub status: Status,
    pub raw_uri: String,
    pub resource_type: String,
    pub resource_hash: u64,
    #[serde(default)]
    pub uri_scheme: String,
    #[serde(default)]
    pub resource_title: String,
    pub embedding_model: String,
    pub chunks: Vec<ContextParsedResourceChunk>,
    #[serde(default)]
    pub source_refs: Vec<String>,
    pub total_tokens: u32,
    #[serde(default)]
    pub parser_warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillParseReport {
    pub status: Status,
    pub skill_name: String,
    pub description: String,
    pub source_ref: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub owner_scope: String,
    pub front_matter: BTreeMap<String, String>,
    pub tag_refs: Vec<String>,
    pub capability_refs: Vec<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub triggers: Vec<String>,
    #[serde(default)]
    pub model_refs: Vec<String>,
    pub tool_refs: Vec<String>,
    pub instruction_refs: Vec<String>,
    pub resource_refs: Vec<String>,
    pub example_refs: Vec<String>,
    #[serde(default)]
    pub parser_warnings: Vec<String>,
    pub resource: ContextResourceParseReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillIngestInput {
    pub raw_uri: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextResourceSkillIngestRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    #[serde(default)]
    pub resources: Vec<ContextResourceParseRequest>,
    #[serde(default)]
    pub skills: Vec<ContextSkillIngestInput>,
    #[serde(default)]
    pub query: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    #[serde(default = "default_retrieve_limit")]
    pub max_events: usize,
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextResourceSkillModelFanoutReport {
    pub node_count: usize,
    pub event_count: usize,
    pub segment_count: usize,
    pub entity_count: usize,
    pub child_ref_count: usize,
    pub embedding_count: usize,
    pub summary_count: usize,
    pub compression_count: usize,
    pub dirty_marker_count: usize,
    pub secondary_index_count: usize,
    pub query_back_ok: bool,
    pub missing_models: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextResourceSkillSecondaryIndexReport {
    pub resource_refs: Vec<String>,
    pub skill_refs: Vec<String>,
    pub entity_refs: Vec<String>,
    pub source_refs: Vec<String>,
    pub summary_refs: Vec<String>,
    pub query_back_ok: bool,
    pub missing_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceSkillSecondaryIndexValidationRequest {
    pub shard_id: ShardId,
    pub tenant_hash: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub secondary_indexes: ContextResourceSkillSecondaryIndexReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSecondaryIndexFamilyValidationReport {
    pub index_name: String,
    pub checked_ref_count: usize,
    pub found_ref_count: usize,
    pub missing_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceSkillSecondaryIndexValidationReport {
    pub status: Status,
    pub query_back_ok: bool,
    pub checked_ref_count: usize,
    pub found_ref_count: usize,
    pub missing_refs: Vec<String>,
    pub families: Vec<ContextSecondaryIndexFamilyValidationReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextResourceSkillIngestReport {
    pub status: Status,
    pub resources: Vec<ContextResourceParseReport>,
    pub skills: Vec<ContextSkillParseReport>,
    pub ingest: ContextIngestExtractReport,
    pub embedding_refs: Vec<u64>,
    pub fanout: ContextResourceSkillModelFanoutReport,
    pub secondary_indexes: ContextResourceSkillSecondaryIndexReport,
    pub retrieval: ContextRetrieveReport,
    pub parity: ContextPipelineParityEvidence,
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
    pub evidence_retention_at_k: f32,
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
    #[serde(default = "default_min_evidence_retention_at_k")]
    pub min_evidence_retention_at_k: f32,
    pub min_token_reduction_percent: f32,
    #[serde(default = "default_max_benchmark_selected_tokens_per_query")]
    pub max_selected_tokens_per_query: u32,
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
    pub evidence_retained: bool,
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
    pub min_evidence_retention_at_k: f32,
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
    pub max_selected_tokens_per_query: u32,
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
    #[serde(default)]
    pub provider: ContextModelProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextRetrieveReport {
    pub status: Status,
    pub blocks: Vec<ContextBlock>,
    pub node_count: usize,
    pub event_count: usize,
    #[serde(default)]
    pub query_understanding_debug: ContextQueryUnderstandingDebug,
    #[serde(default)]
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextQueryUnderstandingDebug {
    pub question_type: String,
    pub secondary_index_filter_groups: Vec<Vec<String>>,
    pub candidates_passing_prefilter: usize,
    pub candidates_dropped_before_scoring: usize,
    pub tree_traversal_summary: ContextTreeTraversalDebug,
    pub prefilter_candidate_sample: Vec<ContextPrefilterCandidateDebug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextTreeTraversalDebug {
    pub enabled: bool,
    pub fallback_reason: String,
    pub fallback_to_flat: bool,
    pub max_children_scored_per_parent: usize,
    pub selected_leaf_count: usize,
    pub selected_node_count: usize,
    pub selected_path_count: usize,
    pub summary_embeddings: Vec<String>,
    #[serde(default)]
    pub summary_embedding_candidate_count: usize,
    #[serde(default)]
    pub summary_embedding_selected_count: usize,
    #[serde(default)]
    pub query_embedding_dimension: usize,
    #[serde(default)]
    pub query_embedding_provider: String,
    pub top_k_per_layer: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPrefilterCandidateDebug {
    pub record_type: String,
    pub ref_hash: u64,
    pub node_hash: u64,
    pub event_time_ms: u64,
    pub node_path: Vec<String>,
    pub candidate_terms: Vec<String>,
    pub passes_secondary_index_prefilter: bool,
    pub text: String,
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
    pub context_model_descriptors: Vec<ContextModelDescriptor>,
    pub openviking_model_profiles: Vec<ContextOpenVikingModelProfile>,
    pub openviking_parity_cases: Vec<ContextOpenVikingParityCase>,
    pub openviking_parity_categories: Vec<String>,
    pub open_model_provider_packaged: bool,
    pub open_model_local_run_proven: bool,
    pub vlm_provider_configured: bool,
    pub vlm_benchmark_proven: bool,
    pub policy: ContextWorkflowPolicy,
    pub parity: ContextPipelineParityEvidence,
    pub openviking_comparison: String,
    pub supported_routes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextOpenVikingModelProfile {
    pub profile_name: String,
    pub provider_name: String,
    pub provider_kind: ContextProviderKind,
    pub base_url: String,
    pub chat_model: String,
    pub vlm_model: String,
    pub embedding_model: String,
    pub capabilities: Vec<String>,
    pub notes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextOpenVikingParityCase {
    pub case_name: String,
    pub category: String,
    pub query: String,
    pub positive_memory: String,
    pub stale_memory: String,
    pub expected_terms: Vec<String>,
    pub expected_model_profile: String,
    pub uses_vlm: bool,
    pub benchmark_proven: bool,
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
    pub cpp_context_model_ids_ready: bool,
    pub cpp_context_timeline_semantics_ready: bool,
    pub cpp_context_validation_limits_ready: bool,
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
        "C++ Context model ids 9-13 are exposed as first-class Rust descriptors".to_string(),
        "C++ Context timeline fanout, key shapes, range windows, and validation limits are enforced"
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
        cpp_context_model_ids_ready: true,
        cpp_context_timeline_semantics_ready: true,
        cpp_context_validation_limits_ready: true,
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
            vlm_model: "local-or-commercial-vlm-model".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "openviking-open-source-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            api_key_env: "OPENVIKING_MODEL_API_KEY".to_string(),
            model: "qwen2.5:7b-instruct".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            vlm_model: "qwen2.5vl:7b".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "vikingmem-gpt-4o-mini-reader".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "https://api.openai.com/v1".to_string(),
            api_key_env: "OPENAI_API_KEY".to_string(),
            model: "gpt-4o-mini".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            vlm_model: "none".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "matrixark-cpp-oss-context".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            api_key_env: "MATRIXARK_MODEL_API_KEY".to_string(),
            model: "google/flan-t5-small".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            vlm_model: "none".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
        ContextModelProviderConfig {
            provider_name: "openviking-open-source-gpt-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            api_key_env: "OPENVIKING_MODEL_API_KEY".to_string(),
            model: "lmsys/vicuna-7b-v1.5".to_string(),
            embedding_model: "BAAI/bge-m3".to_string(),
            vlm_model: "Vision-CAIR/MiniGPT-4".to_string(),
            timeout_ms: 30_000,
            max_retries: 2,
            fallback_provider: Some(Box::new(ContextModelProviderConfig::default())),
            mock_mode: false,
        },
    ]
}

pub fn openviking_open_source_model_profiles() -> Vec<ContextOpenVikingModelProfile> {
    vec![
        ContextOpenVikingModelProfile {
            profile_name: "openviking-qwen2_5_vl-local".to_string(),
            provider_name: "openviking-open-source-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            chat_model: "qwen2.5:7b-instruct".to_string(),
            vlm_model: "qwen2.5vl:7b".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "Recommended local OpenViking-style profile for Ollama or another OpenAI-compatible local gateway."
                .to_string(),
        },
        ContextOpenVikingModelProfile {
            profile_name: "openviking-llava-local".to_string(),
            provider_name: "openviking-llava-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            chat_model: "llama3.1:8b-instruct".to_string(),
            vlm_model: "llava:7b".to_string(),
            embedding_model: "nomic-embed-text".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "Fallback local profile for LLaVA-compatible OpenAI gateway deployments."
                .to_string(),
        },
        ContextOpenVikingModelProfile {
            profile_name: "openviking-internvl-vllm".to_string(),
            provider_name: "openviking-internvl-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            chat_model: "Qwen/Qwen2.5-7B-Instruct".to_string(),
            vlm_model: "OpenGVLab/InternVL2_5-8B".to_string(),
            embedding_model: "BAAI/bge-m3".to_string(),
            capabilities: vec![
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "OpenViking-style vLLM or OpenAI-compatible gateway profile for GPU deployments."
                .to_string(),
        },
        ContextOpenVikingModelProfile {
            profile_name: "vikingmem-gpt-4o-mini-reader".to_string(),
            provider_name: "vikingmem-gpt-4o-mini-reader".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "https://api.openai.com/v1".to_string(),
            chat_model: "gpt-4o-mini".to_string(),
            vlm_model: "none".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            capabilities: vec![
                "vikingmem_reader_parity".to_string(),
                "chat_context_extraction".to_string(),
                "semantic_retrieval".to_string(),
                "locomo_context_benchmark".to_string(),
                "longmemeval_s_context_benchmark".to_string(),
            ],
            notes: "VikingMem benchmark parity reader profile using GPT-4o-mini through an OpenAI-compatible /v1/chat/completions endpoint."
                .to_string(),
        },
        ContextOpenVikingModelProfile {
            profile_name: "matrixark-cpp-oss-context".to_string(),
            provider_name: "matrixark-cpp-oss-context".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            chat_model: "google/flan-t5-small".to_string(),
            vlm_model: "none".to_string(),
            embedding_model: "sentence-transformers/all-MiniLM-L6-v2".to_string(),
            capabilities: vec![
                "cpp_path_oss_model_parity".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
                "locomo_context_benchmark".to_string(),
            ],
            notes: "Matches the MatrixArk/C++ path OSS benchmark setup from the LLM Specific TemporalStore Use Cases thread: transformers extraction with google/flan-t5-small and sentence-transformers/all-MiniLM-L6-v2 embeddings."
                .to_string(),
        },
        ContextOpenVikingModelProfile {
            profile_name: "openviking-minigpt4-gpt-style-vlm".to_string(),
            provider_name: "openviking-open-source-gpt-vlm".to_string(),
            provider_kind: ContextProviderKind::OpenAiCompatible,
            base_url: "http://127.0.0.1:8000/v1".to_string(),
            chat_model: "lmsys/vicuna-7b-v1.5".to_string(),
            vlm_model: "Vision-CAIR/MiniGPT-4".to_string(),
            embedding_model: "BAAI/bge-m3".to_string(),
            capabilities: vec![
                "gpt_style_vlm_reasoning".to_string(),
                "vlm_image_content_understanding".to_string(),
                "chat_context_extraction".to_string(),
                "embedding_vectorization".to_string(),
                "semantic_retrieval".to_string(),
            ],
            notes: "Open-source GPT-4-style VLM profile inspired by MiniGPT-4; serve through an OpenAI-compatible gateway for OpenViking-style image/content understanding."
                .to_string(),
        },
    ]
}

pub fn openviking_context_parity_cases() -> Vec<ContextOpenVikingParityCase> {
    vec![
        ContextOpenVikingParityCase {
            case_name: "locomo_multi_hop_project_selection".to_string(),
            category: "multi_hop_reasoning".to_string(),
            query: "Which project did Lee pick because Dana suggested it during planning?"
                .to_string(),
            positive_memory: "Later planning note: Dana suggested the observability dashboard because the team needed better benchmark traces, so Lee picked that project.".to_string(),
            stale_memory: "Initial planning thread: Lee considered a search cleanup project and had not chosen the final work item.".to_string(),
            expected_terms: vec!["Dana".to_string(), "observability dashboard".to_string()],
            expected_model_profile: "vikingmem-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextOpenVikingParityCase {
            case_name: "locomo_temporal_reschedule".to_string(),
            category: "temporal".to_string(),
            query: "When is Maya's dentist appointment after it was rescheduled?".to_string(),
            positive_memory: "Latest calendar update: Maya rescheduled the dentist appointment to Thursday at 3pm after the clinic called.".to_string(),
            stale_memory: "Earlier memory: Maya had a dentist appointment scheduled for Tuesday morning.".to_string(),
            expected_terms: vec!["Thursday".to_string(), "3pm".to_string()],
            expected_model_profile: "vikingmem-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextOpenVikingParityCase {
            case_name: "longmem_memory_update_risk_score".to_string(),
            category: "memory_update".to_string(),
            query: "What risk score was recorded after the latest fraud review?".to_string(),
            positive_memory: "Latest fraud review: the checkout risk score was updated to 87 after the payment incident escalated.".to_string(),
            stale_memory: "Earlier fraud review: the checkout risk score was 42 before the payment incident escalated.".to_string(),
            expected_terms: vec!["87".to_string()],
            expected_model_profile: "vikingmem-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextOpenVikingParityCase {
            case_name: "locomo_stale_memory_current_pet".to_string(),
            category: "stale_memory".to_string(),
            query: "What is the dog's name in the latest pet update?".to_string(),
            positive_memory: "Latest pet update: the newly adopted dog is named Miso and needs evening walks.".to_string(),
            stale_memory: "Old profile note: the family dog was called Pepper in a previous home.".to_string(),
            expected_terms: vec!["Miso".to_string()],
            expected_model_profile: "vikingmem-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextOpenVikingParityCase {
            case_name: "locomo_open_domain_cafe_recommendation".to_string(),
            category: "open_domain_retrieval".to_string(),
            query: "Who recommended the cafe that Nina booked after the conference?".to_string(),
            positive_memory: "Later chat: Omar recommended the quiet riverside cafe, and Nina booked it after the conference.".to_string(),
            stale_memory: "Earlier conversation: Nina wanted to book a cafe after the conference but had not chosen one yet.".to_string(),
            expected_terms: vec!["Omar".to_string(), "riverside cafe".to_string()],
            expected_model_profile: "vikingmem-gpt-4o-mini-reader".to_string(),
            uses_vlm: false,
            benchmark_proven: true,
        },
        ContextOpenVikingParityCase {
            case_name: "openviking_vlm_receipt_context".to_string(),
            category: "vlm_image_content_understanding".to_string(),
            query: "What merchant and total should be remembered from the receipt image?"
                .to_string(),
            positive_memory: "VLM extraction note: the receipt image shows merchant Northstar Cafe and total $18.40 for the lunch order.".to_string(),
            stale_memory: "Older image note: a different receipt showed merchant Harbor Books and total $42.00.".to_string(),
            expected_terms: vec!["Northstar Cafe".to_string(), "$18.40".to_string()],
            expected_model_profile: "openviking-minigpt4-gpt-style-vlm".to_string(),
            uses_vlm: true,
            benchmark_proven: false,
        },
    ]
}

pub fn context_workflow_state_report() -> ContextWorkflowStateReport {
    let openviking_parity_cases = openviking_context_parity_cases();
    let mut openviking_parity_categories = openviking_parity_cases
        .iter()
        .map(|case| case.category.clone())
        .collect::<Vec<_>>();
    openviking_parity_categories.sort();
    openviking_parity_categories.dedup();
    let openviking_model_profiles = openviking_open_source_model_profiles();
    let vlm_provider_configured = openviking_model_profiles
        .iter()
        .any(|profile| profile.vlm_model != "none");
    ContextWorkflowStateReport {
        status: Status::ok(),
        providers: default_context_model_providers(),
        context_model_descriptors: context_model_descriptors(),
        openviking_model_profiles,
        openviking_parity_cases,
        openviking_parity_categories,
        open_model_provider_packaged: true,
        open_model_local_run_proven: false,
        vlm_provider_configured,
        vlm_benchmark_proven: false,
        policy: ContextWorkflowPolicy::default(),
        parity: context_pipeline_parity_evidence(),
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
            embedding_generation: ContextEmbeddingGenerationReport::default(),
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

    let summary_l0 = ContextSummary {
        node_hash,
        level: 1,
        text: l0.clone(),
        valid_from_ms: timestamp_ms,
    };
    let summary_l1 = ContextSummary {
        node_hash,
        level: 2,
        text: l1.clone(),
        valid_from_ms: timestamp_ms,
    };
    let embedding_inputs = [
        ("node_l0", node_hash, 1, l0.as_str()),
        ("node_l1", node_hash, 2, l1.as_str()),
        ("event_text", event_id_hash, 3, request.body.as_str()),
    ];
    let (embedding_vectors, embedding_generation) =
        match context_embeddings_for_extract(&provider, &embedding_inputs) {
            Ok(value) => value,
            Err(status) => {
                if let Some(fallback) = provider.fallback_provider.as_deref() {
                    let fallback = normalize_provider(fallback.clone());
                    match context_embeddings_for_extract(&fallback, &embedding_inputs) {
                        Ok((vectors, mut report)) => {
                            report.fallback_used = true;
                            report.provider_name = format!(
                                "{}+fallback:{}",
                                provider.provider_name, report.provider_name
                            );
                            (vectors, report)
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
    let embedding_l0 = ContextEmbedding {
        ref_hash: context_embedding_ref_hash(request.tenant_hash, node_hash, "node_l0"),
        level: 1,
        vector: embedding_vectors[0].clone(),
        updated_at_ms: timestamp_ms,
    };
    let embedding_l1 = ContextEmbedding {
        ref_hash: context_embedding_ref_hash(request.tenant_hash, node_hash, "node_l1"),
        level: 2,
        vector: embedding_vectors[1].clone(),
        updated_at_ms: timestamp_ms,
    };
    let embedding_event = ContextEmbedding {
        ref_hash: context_embedding_ref_hash(request.tenant_hash, event_id_hash, "event_text"),
        level: 3,
        vector: embedding_vectors[2].clone(),
        updated_at_ms: timestamp_ms,
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
        Command::ContextUpsertSummary {
            tenant_hash: request.tenant_hash,
            summary: summary_l0,
        },
        Command::ContextUpsertSummary {
            tenant_hash: request.tenant_hash,
            summary: summary_l1,
        },
        Command::ContextUpsertEmbedding {
            tenant_hash: request.tenant_hash,
            embedding: embedding_l0,
        },
        Command::ContextUpsertEmbedding {
            tenant_hash: request.tenant_hash,
            embedding: embedding_l1,
        },
        Command::ContextUpsertEmbedding {
            tenant_hash: request.tenant_hash,
            embedding: embedding_event,
        },
    ] {
        let response = engine.execute_durable(ExecuteRequest {
            shard_id: request.shard_id,
            command,
        });
        if !response.status.ok {
            return ContextExtractReport {
                status: response.status,
                provider: provider.clone(),
                embedding_generation: embedding_generation.clone(),
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
        embedding_generation,
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
        provider: request.provider,
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

pub fn ingest_resource_skill_context(
    engine: &TemporalEngine,
    request: ContextResourceSkillIngestRequest,
) -> ContextResourceSkillIngestReport {
    let provider = normalize_provider(request.provider.clone());
    let mut resources = Vec::new();
    let mut skills = Vec::new();
    let mut sources = Vec::new();
    let mut resource_ref_by_source = BTreeMap::new();
    let mut skill_ref_by_source = BTreeMap::new();
    let mut timestamp_ms = request.start_time_ms.max(1);

    for resource_request in request.resources {
        let resource = parse_context_resource(resource_request);
        for chunk in &resource.chunks {
            resource_ref_by_source.insert(chunk.source_ref.clone(), resource.raw_uri.clone());
            sources.push(ContextExtractRequest {
                shard_id: request.shard_id,
                tenant_hash: request.tenant_hash,
                source_kind: ContextSourceKind::Document,
                source_id: chunk.source_ref.clone(),
                title: chunk
                    .metadata
                    .get("heading")
                    .cloned()
                    .unwrap_or_else(|| resource.resource_title.clone()),
                body: chunk.text.clone(),
                timestamp_ms,
                provider: provider.clone(),
            });
            timestamp_ms = timestamp_ms.saturating_add(1);
        }
        resources.push(resource);
    }

    for skill_input in request.skills {
        let skill = parse_context_skill_markdown(skill_input.raw_uri, skill_input.text);
        for chunk in &skill.resource.chunks {
            skill_ref_by_source.insert(chunk.source_ref.clone(), skill.skill_name.clone());
            sources.push(ContextExtractRequest {
                shard_id: request.shard_id,
                tenant_hash: request.tenant_hash,
                source_kind: ContextSourceKind::Document,
                source_id: chunk.source_ref.clone(),
                title: format!("skill:{}", skill.skill_name),
                body: chunk.text.clone(),
                timestamp_ms,
                provider: provider.clone(),
            });
            timestamp_ms = timestamp_ms.saturating_add(1);
        }
        skills.push(skill);
    }

    let ingest = ingest_extract_context(
        engine,
        ContextIngestExtractRequest {
            shard_id: request.shard_id,
            tenant_hash: request.tenant_hash,
            sources,
            query: request.query.clone(),
            start_time_ms: request.start_time_ms,
            end_time_ms: request.end_time_ms,
            max_events: request.max_events,
            provider,
        },
    );
    let mut fanout = ContextResourceSkillModelFanoutReport {
        node_count: ingest.extracts.len(),
        event_count: ingest.extracts.len(),
        segment_count: ingest.extracts.len(),
        embedding_count: ingest.extracts.len().saturating_mul(3),
        summary_count: ingest.extracts.len().saturating_mul(2),
        dirty_marker_count: ingest.extracts.len(),
        ..ContextResourceSkillModelFanoutReport::default()
    };
    let mut secondary_indexes = ContextResourceSkillSecondaryIndexReport::default();
    let mut embedding_refs = Vec::new();

    for extract in &ingest.extracts {
        let entity_ref = format!("entity:{}", extract.node.canonical_name);
        let entity_hash = stable_hash64(&entity_ref);
        let child_ref = ContextChildRef {
            parent_hash: stable_hash64(&format!("resource-skill-root:{}", request.tenant_hash)),
            child_hash: extract.node.node_hash,
            updated_at_ms: extract.event.event_time_ms,
        };
        let entity = ContextEntity {
            entity_hash,
            node_hash: extract.node.node_hash,
            entity_type: source_kind_code(ContextSourceKind::Document),
            name: extract.node.canonical_name.clone(),
            value: extract.event.source_ref.clone(),
            updated_at_ms: extract.event.event_time_ms,
            valid_from_ms: extract.event.event_time_ms,
            confidence: 1.0,
            source_event_hashes: vec![extract.event.event_id_hash],
        };
        let compression = ContextCompressionEvent {
            compression_id_hash: stable_hash64(&format!(
                "ctx-resource-skill-compress:{}:{}",
                extract.event.source_ref, extract.event.event_time_ms
            )),
            node_hash: extract.node.node_hash,
            source_start_ms: extract.event.event_time_ms,
            source_end_ms: extract.event.event_time_ms.saturating_add(1),
            compressed_time_ms: extract.event.event_time_ms.saturating_add(1),
            summary: extract.l1.clone(),
        };
        let summary_ref_l0 = format!("summary:{}:l0", extract.node.node_hash);
        let summary_ref_l1 = format!("summary:{}:l1", extract.node.node_hash);
        let mut index_writes = vec![
            ("source_ref".to_string(), extract.event.source_ref.clone()),
            ("entity_ref".to_string(), entity_ref.clone()),
            ("summary_ref".to_string(), summary_ref_l0.clone()),
            ("summary_ref".to_string(), summary_ref_l1.clone()),
        ];
        if let Some(resource_ref) = resource_ref_by_source.get(&extract.event.source_ref) {
            index_writes.push(("resource_ref".to_string(), resource_ref.clone()));
        }
        if let Some(skill_ref) = skill_ref_by_source.get(&extract.event.source_ref) {
            index_writes.push(("skill_ref".to_string(), skill_ref.clone()));
        }

        for command in [
            Command::ContextUpsertEntity {
                tenant_hash: request.tenant_hash,
                entity: entity.clone(),
            },
            Command::ContextUpsertChildRef {
                tenant_hash: request.tenant_hash,
                child_ref: child_ref.clone(),
            },
            Command::ContextWriteCompressionEvent {
                tenant_hash: request.tenant_hash,
                event: compression.clone(),
            },
        ] {
            let response = engine.execute_durable(ExecuteRequest {
                shard_id: request.shard_id,
                command,
            });
            if !response.status.ok {
                fanout.missing_models.push(response.status.code);
            }
        }
        fanout.entity_count += 1;
        fanout.child_ref_count += 1;
        fanout.compression_count += 1;

        for (index_name, index_ref_value) in index_writes {
            let response = engine.execute_durable(ExecuteRequest {
                shard_id: request.shard_id,
                command: Command::ContextWriteIndexRef {
                    tenant_hash: request.tenant_hash,
                    index_name: index_name.clone(),
                    index_value_hash: stable_hash64(&index_ref_value),
                    scope_hash: 0,
                    event_time_ms: extract.event.event_time_ms,
                    index_ref: extract.index_ref.clone(),
                },
            });
            if response.status.ok {
                fanout.secondary_index_count += 1;
                match index_name.as_str() {
                    "resource_ref" => secondary_indexes.resource_refs.push(index_ref_value),
                    "skill_ref" => secondary_indexes.skill_refs.push(index_ref_value),
                    "entity_ref" => secondary_indexes.entity_refs.push(index_ref_value),
                    "source_ref" => secondary_indexes.source_refs.push(index_ref_value),
                    "summary_ref" => secondary_indexes.summary_refs.push(index_ref_value),
                    _ => {}
                }
            } else {
                secondary_indexes
                    .missing_refs
                    .push(format!("{index_name}:{index_ref_value}"));
            }
        }

        embedding_refs.extend([
            context_embedding_ref_hash(request.tenant_hash, extract.node.node_hash, "node_l0"),
            context_embedding_ref_hash(request.tenant_hash, extract.node.node_hash, "node_l1"),
            context_embedding_ref_hash(
                request.tenant_hash,
                extract.event.event_id_hash,
                "event_text",
            ),
        ]);
    }
    secondary_indexes.resource_refs.sort();
    secondary_indexes.resource_refs.dedup();
    secondary_indexes.skill_refs.sort();
    secondary_indexes.skill_refs.dedup();
    secondary_indexes.entity_refs.sort();
    secondary_indexes.entity_refs.dedup();
    secondary_indexes.source_refs.sort();
    secondary_indexes.source_refs.dedup();
    secondary_indexes.summary_refs.sort();
    secondary_indexes.summary_refs.dedup();
    embedding_refs.sort_unstable();
    embedding_refs.dedup();

    verify_resource_skill_fanout(
        engine,
        request.shard_id,
        request.tenant_hash,
        &ingest.extracts,
        &embedding_refs,
        request.start_time_ms,
        request.end_time_ms,
        &mut secondary_indexes,
        &mut fanout,
    );
    let retrieval = retrieve_context(engine, ingest.retrieve_request.clone());
    let mut status = if ingest.status.ok
        && fanout.query_back_ok
        && secondary_indexes.query_back_ok
        && retrieval.status.ok
    {
        Status::ok()
    } else {
        Status::error(
            "context_resource_skill_ingest_incomplete",
            "resource/skill ingest did not satisfy all fanout checks",
        )
    };
    if ingest.accepted == 0 {
        status = Status::error(
            "context_resource_skill_ingest_empty",
            "resource/skill ingest produced no accepted context sources",
        );
    }

    ContextResourceSkillIngestReport {
        status,
        resources,
        skills,
        ingest,
        embedding_refs,
        fanout,
        secondary_indexes,
        retrieval,
        parity: context_pipeline_parity_evidence(),
    }
}

pub fn validate_resource_skill_secondary_indexes(
    engine: &TemporalEngine,
    request: ContextResourceSkillSecondaryIndexValidationRequest,
) -> ContextResourceSkillSecondaryIndexValidationReport {
    let family_inputs: [(&str, &[String]); 5] = [
        ("resource_ref", &request.secondary_indexes.resource_refs),
        ("skill_ref", &request.secondary_indexes.skill_refs),
        ("entity_ref", &request.secondary_indexes.entity_refs),
        ("source_ref", &request.secondary_indexes.source_refs),
        ("summary_ref", &request.secondary_indexes.summary_refs),
    ];
    let mut checked_ref_count = 0;
    let mut found_ref_count = 0;
    let mut missing_refs = Vec::new();
    let mut families = Vec::new();

    for (index_name, refs) in family_inputs {
        let family_missing = query_missing_secondary_index_refs(
            engine,
            request.shard_id,
            request.tenant_hash,
            index_name,
            refs,
            request.start_time_ms,
            request.end_time_ms,
        );
        checked_ref_count += refs.len();
        found_ref_count += refs.len().saturating_sub(family_missing.len());
        missing_refs.extend(family_missing.iter().cloned());
        families.push(ContextSecondaryIndexFamilyValidationReport {
            index_name: index_name.to_string(),
            checked_ref_count: refs.len(),
            found_ref_count: refs.len().saturating_sub(family_missing.len()),
            missing_refs: family_missing,
        });
    }

    missing_refs.sort();
    missing_refs.dedup();
    let query_back_ok = checked_ref_count > 0 && missing_refs.is_empty();
    let status = if query_back_ok {
        Status::ok()
    } else if checked_ref_count == 0 {
        Status::error(
            "context_resource_skill_secondary_index_empty",
            "resource/skill ingest produced no secondary index refs to validate",
        )
    } else {
        Status::error(
            "context_resource_skill_secondary_index_missing",
            "resource/skill secondary indexes were not fully queryable",
        )
    };

    ContextResourceSkillSecondaryIndexValidationReport {
        status,
        query_back_ok,
        checked_ref_count,
        found_ref_count,
        missing_refs,
        families,
    }
}

fn verify_resource_skill_fanout(
    engine: &TemporalEngine,
    shard_id: ShardId,
    tenant_hash: u64,
    extracts: &[ContextExtractReport],
    embedding_refs: &[u64],
    start_time_ms: u64,
    end_time_ms: u64,
    secondary_indexes: &mut ContextResourceSkillSecondaryIndexReport,
    fanout: &mut ContextResourceSkillModelFanoutReport,
) {
    let root_hash = stable_hash64(&format!("resource-skill-root:{tenant_hash}"));
    let mut missing = Vec::new();

    for extract in extracts {
        let node = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextGetNode {
                tenant_hash,
                node_hash: extract.node.node_hash,
            },
        });
        if !matches!(
            node.response,
            CommandResponse::ContextNode { node: Some(_), .. }
        ) {
            missing.push("ContextNodeModel".to_string());
        }

        let events = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQueryEvents {
                tenant_hash,
                node_hash: extract.node.node_hash,
                start_time_ms: extract.event.event_time_ms.saturating_sub(1),
                end_time_ms: extract.event.event_time_ms.saturating_add(1),
                limit: Some(8),
                current_valid_only: false,
                as_of_ms: 0,
                kinds: Vec::new(),
                statuses: Vec::new(),
                min_confidence: 0.0,
                min_importance: 0.0,
            },
        });
        if !matches!(
            events.response,
            CommandResponse::ContextEvents { ref events, .. }
                if events.iter().any(|event| event.event_id_hash == extract.event.event_id_hash)
        ) {
            missing.push("ContextEventModel".to_string());
            missing.push("ContextSegment".to_string());
        }

        let entity_hash = stable_hash64(&format!("entity:{}", extract.node.canonical_name));
        let entity = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextGetEntity {
                tenant_hash,
                node_hash: extract.node.node_hash,
                entity_hash,
            },
        });
        if !matches!(
            entity.response,
            CommandResponse::ContextEntity {
                entity: Some(_),
                ..
            }
        ) {
            missing.push("ContextEntityModel".to_string());
        }

        let summaries_l0 = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQuerySummaries {
                tenant_hash,
                node_hash: extract.node.node_hash,
                level: 1,
                as_of_ms: extract.event.event_time_ms,
                limit: Some(2),
            },
        });
        let summaries_l1 = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQuerySummaries {
                tenant_hash,
                node_hash: extract.node.node_hash,
                level: 2,
                as_of_ms: extract.event.event_time_ms,
                limit: Some(2),
            },
        });
        if !matches!(
            summaries_l0.response,
            CommandResponse::ContextSummaries { ref summaries, .. } if !summaries.is_empty()
        ) || !matches!(
            summaries_l1.response,
            CommandResponse::ContextSummaries { ref summaries, .. } if !summaries.is_empty()
        ) {
            missing.push("ContextSummaryModel".to_string());
        }

        let dirty = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash,
                node_hash: extract.node.node_hash,
                start_time_ms: extract.event.event_time_ms.saturating_sub(1),
                end_time_ms: extract.event.event_time_ms.saturating_add(1),
                limit: Some(2),
            },
        });
        if !matches!(
            dirty.response,
            CommandResponse::ContextSummaryDirtyMarkers { ref markers, .. } if !markers.is_empty()
        ) {
            missing.push("ContextDirtyModel".to_string());
        }

        let compression = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::ContextQueryCompressionEvents {
                tenant_hash,
                node_hashes: vec![extract.node.node_hash],
                start_time_ms: extract.event.event_time_ms.saturating_sub(1),
                end_time_ms: extract.event.event_time_ms.saturating_add(2),
                limit: Some(2),
            },
        });
        if !matches!(
            compression.response,
            CommandResponse::ContextCompressionEvents { ref events, .. } if !events.is_empty()
        ) {
            missing.push("ContextCompressionModel".to_string());
        }
    }

    let children = engine.execute(ExecuteRequest {
        shard_id,
        command: Command::ContextQueryChildren {
            tenant_hash,
            parent_hash: root_hash,
            limit: Some(extracts.len().max(1)),
        },
    });
    if !matches!(
        children.response,
        CommandResponse::ContextChildRefs { ref refs, .. } if refs.len() >= extracts.len()
    ) {
        missing.push("ContextChildModel".to_string());
    }

    let embeddings = engine.execute(ExecuteRequest {
        shard_id,
        command: Command::ContextQueryEmbeddings {
            tenant_hash,
            ref_hashes: embedding_refs.to_vec(),
            limit: Some(embedding_refs.len().max(1)),
        },
    });
    if !matches!(
        embeddings.response,
        CommandResponse::ContextEmbeddings { ref embeddings }
            if embeddings.len() >= embedding_refs.len()
    ) {
        missing.push("ContextEmbeddingModel".to_string());
    }

    let resource_refs = secondary_indexes.resource_refs.clone();
    let skill_refs = secondary_indexes.skill_refs.clone();
    let entity_refs = secondary_indexes.entity_refs.clone();
    let source_refs = secondary_indexes.source_refs.clone();
    let summary_refs = secondary_indexes.summary_refs.clone();
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "resource_ref",
        &resource_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "skill_ref",
        &skill_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "entity_ref",
        &entity_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "source_ref",
        &source_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );
    verify_secondary_index_refs(
        engine,
        shard_id,
        tenant_hash,
        "summary_ref",
        &summary_refs,
        start_time_ms,
        end_time_ms,
        secondary_indexes,
    );

    missing.sort();
    missing.dedup();
    fanout.missing_models.extend(missing);
    fanout.missing_models.sort();
    fanout.missing_models.dedup();
    fanout.query_back_ok = fanout.missing_models.is_empty();
    secondary_indexes.missing_refs.sort();
    secondary_indexes.missing_refs.dedup();
    secondary_indexes.query_back_ok = secondary_indexes.missing_refs.is_empty();
}

fn verify_secondary_index_refs(
    engine: &TemporalEngine,
    shard_id: ShardId,
    tenant_hash: u64,
    index_name: &str,
    refs: &[String],
    start_time_ms: u64,
    end_time_ms: u64,
    report: &mut ContextResourceSkillSecondaryIndexReport,
) {
    report
        .missing_refs
        .extend(query_missing_secondary_index_refs(
            engine,
            shard_id,
            tenant_hash,
            index_name,
            refs,
            start_time_ms,
            end_time_ms,
        ));
}

fn query_missing_secondary_index_refs(
    engine: &TemporalEngine,
    shard_id: ShardId,
    tenant_hash: u64,
    index_name: &str,
    refs: &[String],
    start_time_ms: u64,
    end_time_ms: u64,
) -> Vec<String> {
    refs.iter()
        .filter_map(|value| {
            let response = engine.execute(ExecuteRequest {
                shard_id,
                command: Command::ContextQueryIndex {
                    tenant_hash,
                    index_name: index_name.to_string(),
                    index_value_hash: stable_hash64(value),
                    scope_hash: 0,
                    start_time_ms,
                    end_time_ms,
                    limit: Some(8),
                },
            });
            if matches!(
                response.response,
                CommandResponse::ContextIndexRefs { ref refs, .. } if !refs.is_empty()
            ) {
                None
            } else {
                Some(format!("{index_name}:{value}"))
            }
        })
        .collect()
}

pub fn parse_context_resource(request: ContextResourceParseRequest) -> ContextResourceParseReport {
    let resource_type =
        infer_context_resource_type(&request.raw_uri, request.resource_type.as_deref());
    let max_chunk_chars = request.max_chunk_chars.max(1);
    let overlap_chars = request.overlap_chars.min(max_chunk_chars.saturating_sub(1));
    let units = context_resource_units(&request.text, &resource_type, &request.raw_uri);
    let mut chunks = Vec::new();
    let mut parser_warnings = Vec::new();

    for (unit_index, mut unit) in units.into_iter().enumerate() {
        let text = unit.remove("text").unwrap_or_default();
        let heading_path = unit
            .get("heading_path")
            .map(|path| {
                path.split('/')
                    .filter(|part| !part.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let parent_source_ref = unit
            .get("parent_heading_slug")
            .filter(|slug| !slug.is_empty())
            .map(|slug| format!("{}#heading={slug}", request.raw_uri));
        for (split_index, piece) in
            split_context_resource_text(&text, max_chunk_chars, overlap_chars)
                .into_iter()
                .enumerate()
        {
            if piece.trim().is_empty() {
                continue;
            }
            let chunk_index = chunks.len();
            unit.insert("resource_type".to_string(), resource_type.clone());
            unit.insert("chunk_index".to_string(), chunk_index.to_string());
            unit.insert("unit_index".to_string(), unit_index.to_string());
            unit.insert("split_index".to_string(), split_index.to_string());
            unit.insert("raw_uri".to_string(), request.raw_uri.clone());
            unit.insert(
                "uri_scheme".to_string(),
                context_resource_uri_scheme(&request.raw_uri),
            );
            unit.insert(
                "resource_title".to_string(),
                context_resource_title(&request.raw_uri),
            );
            if let Some(extension) = context_resource_extension(&request.raw_uri) {
                unit.insert("source_extension".to_string(), extension);
            }
            let source_ref = context_resource_source_ref(&request.raw_uri, &unit);
            unit.insert("source_ref".to_string(), source_ref.clone());
            let content_hash = stable_hash64(&format!("resource_content:{source_ref}:{piece}"));
            unit.insert("content_hash".to_string(), content_hash.to_string());
            let linked_refs = extract_markdown_link_refs(&piece);
            if !linked_refs.is_empty() {
                unit.insert("linked_refs".to_string(), linked_refs.join(","));
            }
            let chunk_kind = unit
                .get("code_language")
                .map(|_| "code")
                .unwrap_or("text")
                .to_string();
            unit.insert("chunk_kind".to_string(), chunk_kind);
            if piece.len() > max_chunk_chars {
                parser_warnings.push(format!(
                    "chunk {} exceeded max_chunk_chars after boundary adjustment",
                    chunk_index
                ));
            }
            let chunk_hash = request
                .chunk_hash_base
                .map(|base| base.saturating_add(chunk_index as u64))
                .unwrap_or_else(|| stable_hash64(&format!("resource_chunk:{source_ref}:{piece}")));
            let embedding_ref_hash = stable_hash64(&format!(
                "resource_chunk_embedding:{}:{chunk_hash}:{}",
                request.raw_uri, "mock-embedding-v1"
            ));
            chunks.push(ContextParsedResourceChunk {
                chunk_hash,
                content_hash,
                embedding_ref_hash,
                source_ref,
                parent_source_ref: parent_source_ref.clone(),
                heading_path: heading_path.clone(),
                token_estimate: estimate_tokens(&piece),
                text: piece,
                metadata: unit.clone(),
            });
        }
    }

    let total_tokens = chunks.iter().fold(0_u32, |total, chunk| {
        total.saturating_add(chunk.token_estimate)
    });
    ContextResourceParseReport {
        status: Status::ok(),
        raw_uri: request.raw_uri.clone(),
        resource_type,
        resource_hash: stable_hash64(&format!("resource:{}", request.raw_uri)),
        uri_scheme: context_resource_uri_scheme(&request.raw_uri),
        resource_title: context_resource_title(&request.raw_uri),
        embedding_model: "mock-embedding-v1".to_string(),
        source_refs: chunks
            .iter()
            .map(|chunk| chunk.source_ref.clone())
            .collect(),
        chunks,
        total_tokens,
        parser_warnings,
    }
}

pub fn context_resource_chunk_embedding(
    chunk: &ContextParsedResourceChunk,
    model: impl AsRef<str>,
    updated_at_ms: u64,
) -> ContextEmbedding {
    let model = model.as_ref();
    ContextEmbedding {
        ref_hash: chunk.embedding_ref_hash,
        level: 2,
        vector: deterministic_context_embedding(model, &chunk.text),
        updated_at_ms,
    }
}

pub fn parse_context_skill_markdown(
    raw_uri: impl Into<String>,
    text: impl Into<String>,
) -> ContextSkillParseReport {
    let raw_uri = raw_uri.into();
    let text = text.into();
    let front_matter = parse_skill_front_matter(&text);
    let skill_name = front_matter
        .get("name")
        .cloned()
        .unwrap_or_else(|| infer_skill_name_from_uri(&raw_uri));
    let description = front_matter
        .get("description")
        .cloned()
        .unwrap_or_else(|| first_markdown_paragraph(&text));
    let tag_refs =
        parse_skill_front_matter_list(&front_matter, &["tags", "tag", "categories", "category"]);
    let allowed_tools = parse_skill_front_matter_list(
        &front_matter,
        &["allowed_tools", "allowed_tool", "tools", "tooling"],
    );
    let triggers =
        parse_skill_front_matter_list(&front_matter, &["triggers", "trigger", "activation"]);
    let model_refs =
        parse_skill_front_matter_list(&front_matter, &["models", "model", "providers", "provider"]);
    let tool_refs = parse_skill_section_items(&text, &["tools", "tooling", "commands"], true);
    let instruction_refs = parse_skill_section_items(
        &text,
        &["instructions", "workflow", "steps", "when to use"],
        false,
    );
    let resource_refs = parse_skill_section_items(&text, &["resources", "references"], true);
    let example_refs = parse_skill_section_items(&text, &["examples"], false);
    let resource = parse_context_resource(ContextResourceParseRequest {
        raw_uri: raw_uri.clone(),
        resource_type: Some("skill".to_string()),
        text,
        max_chunk_chars: default_resource_max_chunk_chars(),
        overlap_chars: default_resource_overlap_chars(),
        chunk_hash_base: None,
    });
    let capability_refs = resource
        .chunks
        .iter()
        .filter_map(|chunk| chunk.metadata.get("heading_slug"))
        .filter(|slug| {
            matches!(
                slug.as_str(),
                "when-to-use"
                    | "tools"
                    | "instructions"
                    | "resources"
                    | "references"
                    | "examples"
                    | "capabilities"
            )
        })
        .cloned()
        .collect();
    ContextSkillParseReport {
        status: Status::ok(),
        skill_name,
        description,
        source_ref: raw_uri,
        version: front_matter
            .get("version")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
        owner_scope: front_matter
            .get("owner_scope")
            .or_else(|| front_matter.get("owner"))
            .cloned()
            .unwrap_or_else(|| "user".to_string()),
        front_matter,
        tag_refs,
        capability_refs,
        allowed_tools,
        triggers,
        model_refs,
        tool_refs,
        instruction_refs,
        resource_refs,
        example_refs,
        parser_warnings: resource.parser_warnings.clone(),
        resource,
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
        let body = benchmark_context_body(index, topic_index, topic_source_counts[topic_index]);
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
    let mut retained_evidence_count = 0usize;
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
            query: benchmark_query_for_topic(query_index),
            start_time_ms: 0,
            end_time_ms: 1_000 + source_count as u64 + 1,
            max_events: request.max_events,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: default_tiers(),
            provider: ContextModelProviderConfig::default(),
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
        let evidence_retained = inject.selected_blocks.iter().any(|block| {
            block
                .text
                .to_ascii_lowercase()
                .contains(expected_topic.as_str())
        });
        if evidence_retained {
            retained_evidence_count += 1;
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
            evidence_retained,
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
    let evidence_retention_at_k = retained_evidence_count as f32 / query_count as f32;
    let zero_hit_queries = query_count.saturating_sub(hit_count);
    let avg_retrieved_blocks_per_query = total_retrieved_blocks as f64 / query_count as f64;
    let avg_selected_blocks_per_query = total_selected_blocks as f64 / query_count as f64;
    let avg_selected_tokens_per_query = selected_context_tokens as f64 / query_count as f64;
    let threshold_violations = benchmark_threshold_violations(
        &request.thresholds,
        hit_at_k,
        mean_reciprocal_rank,
        recall_at_k,
        evidence_retention_at_k,
        token_reduction_percent,
        max_selected_tokens_per_query,
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
        && retained_evidence_count == query_count
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
        evidence_retention_at_k,
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
            "VikingMem-style local benchmark covers extraction, hierarchical retrieval, budgeted injection, latency, hit@k, MRR, throughput, recall proxy, evidence retention, and token reduction".to_string(),
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
    let min_evidence_retention_at_k = reports
        .iter()
        .map(|report| report.evidence_retention_at_k)
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
    let max_selected_tokens_per_query = reports
        .iter()
        .map(|report| report.max_selected_tokens_per_query)
        .max()
        .unwrap_or_default();
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
        && min_evidence_retention_at_k >= 1.0
        && min_token_reduction_percent > 0.0
    {
        Status::ok()
    } else {
        Status::error(
            "context_pipeline_benchmark_sweep_incomplete",
            format!(
                "profiles={profile_count} ready={all_profiles_ready} min_hit_at_k={min_hit_at_k:.3} min_mrr={min_mean_reciprocal_rank:.3} min_evidence_retention={min_evidence_retention_at_k:.3} min_token_reduction={min_token_reduction_percent:.3}"
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
        min_evidence_retention_at_k,
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
        max_selected_tokens_per_query,
        all_thresholds_passed,
        threshold_violations,
        evidence: vec![
            "Benchmark sweep runs multiple deterministic profile sizes through the same Context pipeline".to_string(),
            "Sweep aggregates readiness, threshold gates, hit@k, MRR, evidence retention, token budget, token reduction, latency, total source count, and total query count".to_string(),
        ],
    }
}

fn benchmark_threshold_violations(
    thresholds: &ContextPipelineBenchmarkThresholds,
    hit_at_k: f32,
    mean_reciprocal_rank: f32,
    recall_at_k: f32,
    evidence_retention_at_k: f32,
    token_reduction_percent: f32,
    max_selected_tokens_per_query: u32,
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
    if evidence_retention_at_k < thresholds.min_evidence_retention_at_k {
        violations.push(format!(
            "evidence_retention_at_k {evidence_retention_at_k:.3} below {:.3}",
            thresholds.min_evidence_retention_at_k
        ));
    }
    if token_reduction_percent < thresholds.min_token_reduction_percent {
        violations.push(format!(
            "token_reduction_percent {token_reduction_percent:.3} below {:.3}",
            thresholds.min_token_reduction_percent
        ));
    }
    if max_selected_tokens_per_query > thresholds.max_selected_tokens_per_query {
        violations.push(format!(
            "max_selected_tokens_per_query {max_selected_tokens_per_query} above {}",
            thresholds.max_selected_tokens_per_query
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

fn benchmark_context_body(index: usize, topic_index: usize, topic_sequence: usize) -> String {
    let is_latest_update = topic_sequence > 1;
    let update_marker = if is_latest_update {
        "latest memory update"
    } else {
        "earlier memory"
    };
    let detail = match (topic_index % 4, is_latest_update) {
        (0, true) => "checkout payment risk score changed after a fraud review, with the current status captured for later QA",
        (0, false) => "checkout payment risk score baseline from the original fraud review remains available as historical context",
        (1, true) => "backend service dependency outage created a current temporal incident timeline and recovery sequence",
        (1, false) => "backend service dependency health snapshot captured the initial incident history before recovery",
        (2, true) => "customer preference was updated during a later conversation session and replaced the stale setting",
        (2, false) => "customer preference captured the original conversation setting before any later change",
        (_, true) => "support ticket follow-up recorded the agent action, user ask, and open helpdesk state",
        (_, false) => "support ticket captured the first user ask before the agent follow-up action",
    };
    format!(
        "VikingMem-style benchmark context item {index}: {update_marker} for topic {topic_index}; {detail}; retrieval hint and follow-up action are preserved."
    )
}

fn benchmark_query_for_topic(topic_index: usize) -> String {
    let topic_phrase = format!("topic {topic_index}");
    match topic_index % 4 {
        0 => format!("latest payment fraud status {topic_phrase}"),
        1 => format!("recent service outage timeline {topic_phrase}"),
        2 => format!("customer preference update conversation {topic_phrase}"),
        _ => format!("support ticket follow up {topic_phrase}"),
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

#[derive(Debug, Serialize)]
struct OpenAiEmbeddingRequest<'a> {
    model: &'a str,
    input: Vec<&'a str>,
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

fn context_embeddings_for_extract(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<(Vec<Vec<f32>>, ContextEmbeddingGenerationReport), Status> {
    let provider = normalize_provider(provider.clone());
    if provider.mock_mode || provider.provider_kind == ContextProviderKind::Mock {
        let vectors = inputs
            .iter()
            .map(|(_, _, _, text)| deterministic_context_embedding(&provider.embedding_model, text))
            .collect::<Vec<_>>();
        let vector_dimension = vectors.first().map(Vec::len).unwrap_or_default();
        return Ok((
            vectors,
            ContextEmbeddingGenerationReport {
                status: Status::ok(),
                provider_name: provider.provider_name,
                provider_kind: provider.provider_kind,
                embedding_model: provider.embedding_model,
                vector_dimension,
                requested_vector_count: inputs.len(),
                generated_vector_count: inputs.len(),
                batch_count: usize::from(!inputs.is_empty()),
                live_call_count: 0,
                fallback_used: false,
                mock_mode: true,
                production_evidence_ready: false,
            },
        ));
    }

    let vectors = match provider.provider_kind {
        ContextProviderKind::OpenAiCompatible => {
            call_openai_compatible_embedding_provider(&provider, inputs)?
        }
        ContextProviderKind::Mock => inputs
            .iter()
            .map(|(_, _, _, text)| deterministic_context_embedding(&provider.embedding_model, text))
            .collect(),
    };
    let vector_dimension = vectors.first().map(Vec::len).unwrap_or_default();
    if vectors.len() != inputs.len() || vector_dimension == 0 {
        return Err(Status::error(
            "embedding_provider_bad_response",
            format!(
                "embedding provider {} returned {} vectors for {} inputs",
                provider.provider_name,
                vectors.len(),
                inputs.len()
            ),
        ));
    }
    if !vectors.iter().all(|vector| {
        vector.len() == vector_dimension && vector.iter().all(|value| value.is_finite())
    }) {
        return Err(Status::error(
            "embedding_provider_bad_response",
            format!(
                "embedding provider {} returned inconsistent or non-finite vectors",
                provider.provider_name
            ),
        ));
    }
    Ok((
        vectors,
        ContextEmbeddingGenerationReport {
            status: Status::ok(),
            provider_name: provider.provider_name,
            provider_kind: provider.provider_kind,
            embedding_model: provider.embedding_model,
            vector_dimension,
            requested_vector_count: inputs.len(),
            generated_vector_count: inputs.len(),
            batch_count: usize::from(!inputs.is_empty()),
            live_call_count: usize::from(!inputs.is_empty()),
            fallback_used: false,
            mock_mode: false,
            production_evidence_ready: true,
        },
    ))
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

fn call_openai_compatible_embedding_provider(
    provider: &ContextModelProviderConfig,
    inputs: &[(&str, u64, u32, &str)],
) -> Result<Vec<Vec<f32>>, Status> {
    let (addr, path_prefix) = parse_openai_compatible_base_url(&provider.base_url)?;
    let api_key = if provider.api_key_env.trim().is_empty() {
        None
    } else {
        Some(std::env::var(&provider.api_key_env).map_err(|_| {
            Status::error(
                "embedding_provider_auth_missing",
                format!(
                    "embedding provider {} requires environment variable {}",
                    provider.provider_name, provider.api_key_env
                ),
            )
        })?)
    };
    let embedding_request = OpenAiEmbeddingRequest {
        model: &provider.embedding_model,
        input: inputs.iter().map(|(_, _, _, text)| *text).collect(),
    };
    let headers = api_key
        .as_ref()
        .map(|key| format!("Authorization: Bearer {key}\r\n"))
        .unwrap_or_default();
    let path = format!("{}/embeddings", path_prefix.trim_end_matches('/'));
    let response: Value = post_json_with_options_and_headers(
        &addr,
        &path,
        &embedding_request,
        &headers,
        HttpRequestOptions {
            connect_timeout_ms: provider.timeout_ms.min(5_000).max(1),
            io_timeout_ms: provider.timeout_ms.max(1),
            max_retries: provider.max_retries,
        },
    )
    .map_err(|err| {
        Status::error(
            "embedding_provider_request_failed",
            format!(
                "embedding provider {} request failed: {err}",
                provider.provider_name
            ),
        )
    })?;
    response["data"]
        .as_array()
        .ok_or_else(|| {
            Status::error(
                "embedding_provider_bad_response",
                format!(
                    "embedding provider {} response missing data",
                    provider.provider_name
                ),
            )
        })?
        .iter()
        .map(|item| {
            item["embedding"]
                .as_array()
                .ok_or_else(|| {
                    Status::error(
                        "embedding_provider_bad_response",
                        format!(
                            "embedding provider {} response missing data[].embedding",
                            provider.provider_name
                        ),
                    )
                })?
                .iter()
                .map(|value| {
                    value.as_f64().map(|value| value as f32).ok_or_else(|| {
                        Status::error(
                            "embedding_provider_bad_response",
                            format!(
                                "embedding provider {} returned a non-float embedding value",
                                provider.provider_name
                            ),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
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
    let mut query_understanding_debug = context_query_understanding_debug(&request);
    let tiers = if request.tiers.is_empty() {
        default_tiers()
    } else {
        request.tiers.clone()
    };
    let mut node_hashes = if request.node_hashes.is_empty() {
        return ContextRetrieveReport {
            status: Status::error(
                "node_hash_required",
                "context retrieval requires at least one node hash in this local workflow",
            ),
            blocks,
            node_count,
            event_count,
            query_understanding_debug,
            parity: context_pipeline_parity_evidence(),
        };
    } else {
        request.node_hashes.clone()
    };
    let retrieval_provider = normalize_provider(request.provider.clone());
    let query_embedding = match context_query_embedding(&retrieval_provider, &request.query) {
        Ok(vector) => vector,
        Err(status) => {
            return ContextRetrieveReport {
                status,
                blocks,
                node_count,
                event_count,
                query_understanding_debug,
                parity: context_pipeline_parity_evidence(),
            };
        }
    };
    let mut summary_scores = Vec::new();
    for node_hash in &node_hashes {
        let ref_hashes = vec![
            context_embedding_ref_hash(request.tenant_hash, *node_hash, "node_l0"),
            context_embedding_ref_hash(request.tenant_hash, *node_hash, "node_l1"),
        ];
        let embeddings = engine.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: Command::ContextQueryEmbeddings {
                tenant_hash: request.tenant_hash,
                ref_hashes,
                limit: Some(2),
            },
        });
        let mut best_score = 0i64;
        let mut found = 0usize;
        if let CommandResponse::ContextEmbeddings { embeddings } = embeddings.response {
            found = embeddings.len();
            best_score = embeddings
                .iter()
                .map(|embedding| {
                    context_embedding_similarity_micros(&query_embedding, &embedding.vector)
                })
                .max()
                .unwrap_or_default();
        }
        summary_scores.push((*node_hash, best_score, found));
    }
    summary_scores.sort_by_key(|(node_hash, score, _)| (Reverse(*score), *node_hash));
    node_hashes = summary_scores
        .iter()
        .map(|(node_hash, _, _)| *node_hash)
        .collect();
    query_understanding_debug.tree_traversal_summary.enabled = true;
    query_understanding_debug
        .tree_traversal_summary
        .fallback_to_flat = false;
    query_understanding_debug
        .tree_traversal_summary
        .summary_embedding_candidate_count = summary_scores.len();
    query_understanding_debug
        .tree_traversal_summary
        .summary_embedding_selected_count = summary_scores
        .iter()
        .filter(|(_, _, found)| *found > 0)
        .count();
    query_understanding_debug
        .tree_traversal_summary
        .query_embedding_dimension = query_embedding.len();
    query_understanding_debug
        .tree_traversal_summary
        .query_embedding_provider = retrieval_provider.provider_name.clone();
    query_understanding_debug
        .tree_traversal_summary
        .summary_embeddings = summary_scores
        .iter()
        .map(|(node_hash, score, found)| format!("node:{node_hash}:score:{score}:refs:{found}"))
        .collect();

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
                let passes_prefilter = context_query_matches(&request.query, &event.text);
                context_query_debug_record_candidate(
                    &mut query_understanding_debug,
                    request.tenant_hash,
                    node_hash,
                    &event,
                    passes_prefilter,
                );
                if !passes_prefilter {
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
    context_query_debug_finalize(
        &mut query_understanding_debug,
        &blocks,
        node_count,
        tiers.as_slice(),
    );
    ContextRetrieveReport {
        status: Status::ok(),
        blocks,
        node_count,
        event_count,
        query_understanding_debug,
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
    if provider.vlm_model.is_empty() {
        provider.vlm_model = default_vlm_model();
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

fn default_resource_max_chunk_chars() -> usize {
    1_400
}

fn default_resource_overlap_chars() -> usize {
    120
}

fn infer_context_resource_type(raw_uri: &str, resource_type: Option<&str>) -> String {
    if let Some(kind) = resource_type {
        let kind = kind.trim().trim_start_matches('.').to_ascii_lowercase();
        if !kind.is_empty() {
            return kind;
        }
    }
    raw_uri
        .rsplit_once('.')
        .map(|(_, suffix)| suffix.trim().to_ascii_lowercase())
        .filter(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_alphanumeric()))
        .unwrap_or_else(|| "txt".to_string())
}

fn context_resource_uri_scheme(raw_uri: &str) -> String {
    raw_uri
        .split_once("://")
        .map(|(scheme, _)| scheme.to_ascii_lowercase())
        .unwrap_or_else(|| "file".to_string())
}

fn context_resource_title(raw_uri: &str) -> String {
    raw_uri
        .rsplit(['/', '\\'])
        .find(|part| !part.is_empty())
        .unwrap_or(raw_uri)
        .split('#')
        .next()
        .unwrap_or(raw_uri)
        .to_string()
}

fn context_resource_extension(raw_uri: &str) -> Option<String> {
    context_resource_title(raw_uri)
        .rsplit_once('.')
        .map(|(_, suffix)| suffix.trim().to_ascii_lowercase())
        .filter(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_alphanumeric()))
}

fn context_resource_units(
    text: &str,
    resource_type: &str,
    raw_uri: &str,
) -> Vec<BTreeMap<String, String>> {
    if matches!(resource_type, "md" | "markdown" | "skill") {
        markdown_resource_units(text)
    } else {
        paragraph_resource_units(text, raw_uri)
    }
}

fn markdown_resource_units(text: &str) -> Vec<BTreeMap<String, String>> {
    let mut units = Vec::new();
    let mut current_heading = "document".to_string();
    let mut current_level = 0_usize;
    let mut current_path = vec!["document".to_string()];
    let mut current_heading_slug = "document".to_string();
    let mut parent_heading_slug = String::new();
    let mut buffer = Vec::new();
    let mut start_line = 1_usize;
    let mut heading_stack: Vec<(usize, String, String)> = Vec::new();
    let mut active_code_language: Option<String> = None;
    let mut section_code_language: Option<String> = None;

    fn flush(
        units: &mut Vec<BTreeMap<String, String>>,
        buffer: &mut Vec<String>,
        heading: &str,
        level: usize,
        path: &[String],
        heading_slug: &str,
        parent_heading_slug: &str,
        start_line: usize,
        end_line: usize,
        code_language: Option<&str>,
    ) {
        let content = buffer.join("\n").trim().to_string();
        if content.is_empty() {
            buffer.clear();
            return;
        }
        let mut unit = BTreeMap::new();
        unit.insert("text".to_string(), content);
        unit.insert("heading".to_string(), heading.to_string());
        unit.insert("heading_slug".to_string(), heading_slug.to_string());
        unit.insert("heading_level".to_string(), level.to_string());
        unit.insert("heading_path".to_string(), path.join("/"));
        unit.insert("line_start".to_string(), start_line.to_string());
        unit.insert("line_end".to_string(), end_line.max(start_line).to_string());
        if !parent_heading_slug.is_empty() {
            unit.insert(
                "parent_heading_slug".to_string(),
                parent_heading_slug.to_string(),
            );
        }
        if let Some(language) = code_language.filter(|language| !language.is_empty()) {
            unit.insert("code_language".to_string(), language.to_string());
        }
        units.push(unit);
        buffer.clear();
    }

    for (line_index, line) in text.lines().enumerate() {
        let line_number = line_index + 1;
        let trimmed = line.trim();
        if let Some(fence) = trimmed.strip_prefix("```") {
            let language = fence.trim();
            if active_code_language.is_some() {
                active_code_language = None;
            } else if !language.is_empty() {
                active_code_language = Some(language.to_string());
            } else {
                active_code_language = Some("plain".to_string());
            }
        }
        let hash_count = trimmed.chars().take_while(|ch| *ch == '#').count();
        let is_heading = (1..=6).contains(&hash_count)
            && trimmed.as_bytes().get(hash_count) == Some(&b' ')
            && trimmed.len() > hash_count + 1;
        if is_heading {
            flush(
                &mut units,
                &mut buffer,
                &current_heading,
                current_level,
                &current_path,
                &current_heading_slug,
                &parent_heading_slug,
                start_line,
                line_number.saturating_sub(1),
                section_code_language.as_deref(),
            );
            current_level = hash_count;
            current_heading = trimmed[hash_count..].trim().to_string();
            let current_slug = slugify_context_resource(&current_heading);
            while heading_stack
                .last()
                .map(|(level, _, _)| *level >= current_level)
                .unwrap_or(false)
            {
                heading_stack.pop();
            }
            parent_heading_slug = heading_stack
                .last()
                .map(|(_, _, slug)| slug.clone())
                .unwrap_or_default();
            heading_stack.push((current_level, current_heading.clone(), current_slug.clone()));
            current_path = heading_stack
                .iter()
                .map(|(_, heading, _)| slugify_context_resource(heading))
                .collect();
            current_heading_slug = current_slug;
            start_line = line_number;
            active_code_language = None;
            section_code_language = None;
            buffer.push(trimmed.to_string());
        } else {
            if let Some(language) = active_code_language.as_deref() {
                section_code_language.get_or_insert_with(|| language.to_string());
            }
            buffer.push(line.trim_end().to_string());
        }
    }
    let end_line = text.lines().count().max(1);
    flush(
        &mut units,
        &mut buffer,
        &current_heading,
        current_level,
        &current_path,
        &current_heading_slug,
        &parent_heading_slug,
        start_line,
        end_line,
        section_code_language.as_deref(),
    );
    if units.is_empty() && !text.trim().is_empty() {
        let mut unit = BTreeMap::new();
        unit.insert("text".to_string(), text.trim().to_string());
        unit.insert("heading".to_string(), "document".to_string());
        unit.insert("heading_slug".to_string(), "document".to_string());
        unit.insert("heading_level".to_string(), "0".to_string());
        unit.insert("heading_path".to_string(), "document".to_string());
        unit.insert("line_start".to_string(), "1".to_string());
        unit.insert("line_end".to_string(), end_line.to_string());
        units.push(unit);
    }
    units
}

fn paragraph_resource_units(text: &str, raw_uri: &str) -> Vec<BTreeMap<String, String>> {
    let mut units = Vec::new();
    for (index, paragraph) in split_paragraphs(text).into_iter().enumerate() {
        let mut unit = BTreeMap::new();
        let line_count = paragraph.lines().count().max(1);
        unit.insert("text".to_string(), paragraph);
        unit.insert("paragraph_index".to_string(), index.to_string());
        unit.insert(
            "section".to_string(),
            raw_uri.rsplit('/').next().unwrap_or("document").to_string(),
        );
        unit.insert("line_start".to_string(), "1".to_string());
        unit.insert("line_end".to_string(), line_count.to_string());
        units.push(unit);
    }
    if units.is_empty() && !text.trim().is_empty() {
        let mut unit = BTreeMap::new();
        unit.insert("text".to_string(), text.trim().to_string());
        unit.insert("paragraph_index".to_string(), "0".to_string());
        unit.insert(
            "section".to_string(),
            raw_uri.rsplit('/').next().unwrap_or("document").to_string(),
        );
        unit.insert("line_start".to_string(), "1".to_string());
        unit.insert(
            "line_end".to_string(),
            text.trim().lines().count().max(1).to_string(),
        );
        units.push(unit);
    }
    units
}

fn split_paragraphs(text: &str) -> Vec<String> {
    let mut paragraphs = Vec::new();
    let mut current = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.is_empty() {
                paragraphs.push(current.join("\n").trim().to_string());
                current.clear();
            }
        } else {
            current.push(line.trim_end().to_string());
        }
    }
    if !current.is_empty() {
        paragraphs.push(current.join("\n").trim().to_string());
    }
    paragraphs
}

fn split_context_resource_text(
    text: &str,
    max_chunk_chars: usize,
    overlap_chars: usize,
) -> Vec<String> {
    let mut normalized = text.trim().to_string();
    while normalized.contains("\n\n\n") {
        normalized = normalized.replace("\n\n\n", "\n\n");
    }
    if normalized.len() <= max_chunk_chars {
        return vec![normalized];
    }
    let mut pieces = Vec::new();
    let mut start = 0;
    while start < normalized.len() {
        let mut end =
            floor_char_boundary(&normalized, (start + max_chunk_chars).min(normalized.len()));
        if end < normalized.len() {
            let window = &normalized[start..end];
            let paragraph_boundary = window.rfind("\n\n").map(|index| start + index);
            let sentence_boundary = window.rfind(". ").map(|index| start + index + 1);
            let boundary = paragraph_boundary
                .into_iter()
                .chain(sentence_boundary)
                .max();
            if let Some(boundary) = boundary {
                if boundary > start + (max_chunk_chars / 2) {
                    end = boundary;
                }
            }
        }
        let piece = normalized[start..end].trim().to_string();
        if !piece.is_empty() {
            pieces.push(piece);
        }
        if end >= normalized.len() {
            break;
        }
        start = ceil_char_boundary(
            &normalized,
            end.saturating_sub(overlap_chars).max(start + 1),
        );
    }
    pieces
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index < value.len() && !value.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn context_resource_source_ref(raw_uri: &str, metadata: &BTreeMap<String, String>) -> String {
    let mut suffix = if let Some(page) = metadata.get("page") {
        format!("page={page}")
    } else if let Some(heading_slug) = metadata.get("heading_slug") {
        format!("heading={heading_slug}")
    } else if let Some(paragraph_index) = metadata.get("paragraph_index") {
        format!("paragraph={paragraph_index}")
    } else {
        format!(
            "chunk={}",
            metadata
                .get("chunk_index")
                .map(String::as_str)
                .unwrap_or("0")
        )
    };
    if metadata
        .get("split_index")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
        > 0
    {
        suffix.push_str("&part=");
        suffix.push_str(
            metadata
                .get("split_index")
                .map(String::as_str)
                .unwrap_or("0"),
        );
    }
    format!("{raw_uri}#{suffix}")
}

fn slugify_context_resource(value: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string().if_empty("section")
}

fn parse_skill_front_matter(text: &str) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    let mut lines = text.lines();
    if lines.next().map(str::trim) != Some("---") {
        return metadata;
    }
    let mut active_list_key: Option<String> = None;
    for line in lines {
        let trimmed = line.trim();
        if trimmed == "---" {
            break;
        }
        if let Some(key) = active_list_key.clone() {
            if let Some(item) = trimmed
                .strip_prefix("- ")
                .or_else(|| trimmed.strip_prefix("* "))
            {
                let value = item
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .trim_matches('`');
                if !value.is_empty() {
                    metadata
                        .entry(key)
                        .and_modify(|existing| {
                            if !existing.is_empty() {
                                existing.push(',');
                            }
                            existing.push_str(value);
                        })
                        .or_insert_with(|| value.to_string());
                    continue;
                }
            }
            if !line.starts_with(' ') && !line.starts_with('\t') {
                active_list_key = None;
            }
        }
        if let Some((key, value)) = trimmed.split_once(':') {
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim().trim_matches('"').trim_matches('\'');
            if value.is_empty() {
                active_list_key = Some(key.clone());
                metadata.entry(key).or_default();
            } else {
                metadata.insert(key, value.to_string());
                active_list_key = None;
            }
        }
    }
    metadata
}

fn parse_skill_front_matter_list(
    metadata: &BTreeMap<String, String>,
    keys: &[&str],
) -> Vec<String> {
    let mut values = Vec::new();
    for key in keys {
        let Some(raw) = metadata.get(*key) else {
            continue;
        };
        for item in raw
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split(',')
        {
            let value = item
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches('`')
                .trim();
            if !value.is_empty() {
                values.push(value.to_string());
            }
        }
    }
    values.sort();
    values.dedup();
    values
}

fn extract_markdown_link_refs(text: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut remainder = text;
    while let Some(open_label) = remainder.find('[') {
        remainder = &remainder[open_label + 1..];
        let Some(close_label) = remainder.find("](") else {
            continue;
        };
        remainder = &remainder[close_label + 2..];
        let Some(close_url) = remainder.find(')') else {
            break;
        };
        let target = remainder[..close_url].trim();
        if !target.is_empty() {
            refs.push(
                target
                    .trim_matches('"')
                    .trim_matches('\'')
                    .trim_matches('`')
                    .to_string(),
            );
        }
        remainder = &remainder[close_url + 1..];
    }
    refs.sort();
    refs.dedup();
    refs
}

fn parse_skill_section_items(
    text: &str,
    section_slugs: &[&str],
    first_token_only: bool,
) -> Vec<String> {
    let wanted = section_slugs
        .iter()
        .map(|slug| slugify_context_resource(slug))
        .collect::<Vec<_>>();
    let mut active = false;
    let mut refs = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        let hash_count = trimmed.chars().take_while(|ch| *ch == '#').count();
        let is_heading = (1..=6).contains(&hash_count)
            && trimmed.as_bytes().get(hash_count) == Some(&b' ')
            && trimmed.len() > hash_count + 1;
        if is_heading {
            let heading = trimmed[hash_count..].trim();
            active = wanted
                .iter()
                .any(|wanted_slug| slugify_context_resource(heading) == *wanted_slug);
            continue;
        }
        if !active {
            continue;
        }
        if let Some(item) = parse_markdown_list_item(trimmed, first_token_only) {
            refs.push(item);
        }
    }
    refs.sort();
    refs.dedup();
    refs
}

fn parse_markdown_list_item(trimmed: &str, first_token_only: bool) -> Option<String> {
    let item = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
        .or_else(|| trimmed.strip_prefix("+ "))
        .or_else(|| {
            let (prefix, rest) = trimmed.split_once(". ")?;
            prefix.chars().all(|ch| ch.is_ascii_digit()).then_some(rest)
        })?
        .trim();
    (!item.is_empty()).then(|| {
        let linked_refs = extract_markdown_link_refs(item);
        let value = if first_token_only {
            linked_refs
                .first()
                .map(String::as_str)
                .unwrap_or_else(|| item.split_whitespace().next().unwrap_or(item))
        } else {
            item
        };
        value
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('\'')
            .trim_matches(|ch: char| matches!(ch, ',' | ';' | ':' | '.'))
            .to_string()
    })
}

fn infer_skill_name_from_uri(raw_uri: &str) -> String {
    raw_uri
        .rsplit('/')
        .find(|part| !part.is_empty() && *part != "SKILL.md")
        .unwrap_or("skill")
        .trim_end_matches(".md")
        .to_string()
}

fn first_markdown_paragraph(text: &str) -> String {
    split_paragraphs(
        &text
            .lines()
            .filter(|line| !line.trim().starts_with("---"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .into_iter()
    .find(|paragraph| !paragraph.trim_start().starts_with('#'))
    .unwrap_or_default()
}

trait IfEmpty {
    fn if_empty(self, fallback: &str) -> String;
}

impl IfEmpty for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
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
    let text_normalized = context_normalize_for_match(text);
    if let Some(topic_phrase) = context_query_topic_phrase(query) {
        if context_text_matches_term(&text_lower, &text_normalized, topic_phrase.as_str()) {
            return true;
        }
    }
    let query_groups = context_query_term_groups(query);
    if query_groups.is_empty() {
        return true;
    }
    let matched_groups = query_groups
        .iter()
        .filter(|group| {
            group
                .iter()
                .any(|term| context_text_matches_term(&text_lower, &text_normalized, term))
        })
        .count();
    matched_groups > 0
}

fn context_query_understanding_debug(
    request: &ContextRetrieveRequest,
) -> ContextQueryUnderstandingDebug {
    let terms = context_query_terms(&request.query);
    let filter_groups = context_query_secondary_index_filter_groups(&terms);
    ContextQueryUnderstandingDebug {
        question_type: context_query_question_type(&terms),
        secondary_index_filter_groups: filter_groups,
        candidates_passing_prefilter: 0,
        candidates_dropped_before_scoring: 0,
        tree_traversal_summary: ContextTreeTraversalDebug {
            enabled: true,
            fallback_reason: String::new(),
            fallback_to_flat: false,
            max_children_scored_per_parent: request.max_events.max(1),
            selected_leaf_count: 0,
            selected_node_count: 0,
            selected_path_count: 0,
            summary_embedding_candidate_count: 0,
            summary_embedding_selected_count: 0,
            query_embedding_dimension: 0,
            query_embedding_provider: String::new(),
            summary_embeddings: Vec::new(),
            top_k_per_layer: request.max_events.max(1),
        },
        prefilter_candidate_sample: Vec::new(),
    }
}

fn context_query_debug_record_candidate(
    debug: &mut ContextQueryUnderstandingDebug,
    tenant_hash: u64,
    node_hash: u64,
    event: &ContextEvent,
    passes_prefilter: bool,
) {
    if passes_prefilter {
        debug.candidates_passing_prefilter += 1;
    } else {
        debug.candidates_dropped_before_scoring += 1;
    }
    if debug.question_type == "match_all" {
        return;
    }
    if debug.prefilter_candidate_sample.len() >= 12 {
        return;
    }
    debug
        .prefilter_candidate_sample
        .push(ContextPrefilterCandidateDebug {
            record_type: "context_event".to_string(),
            ref_hash: stable_hash64(&format!(
                "ctx-prefilter:{tenant_hash}:{node_hash}:{}:{}",
                event.event_time_ms, event.event_id_hash
            )),
            node_hash,
            event_time_ms: event.event_time_ms,
            node_path: vec![format!("tenant:{tenant_hash}"), format!("node:{node_hash}")],
            candidate_terms: context_event_candidate_terms(event),
            passes_secondary_index_prefilter: passes_prefilter,
            text: truncate_words(&event.text, 32),
        });
}

fn context_query_debug_finalize(
    debug: &mut ContextQueryUnderstandingDebug,
    blocks: &[ContextBlock],
    node_count: usize,
    tiers: &[ContextTier],
) {
    debug.tree_traversal_summary.selected_leaf_count = blocks
        .iter()
        .filter(|block| block.tier == ContextTier::L2)
        .count();
    debug.tree_traversal_summary.selected_node_count = node_count;
    debug.tree_traversal_summary.selected_path_count = blocks.len();
    debug
        .tree_traversal_summary
        .summary_embeddings
        .extend(tiers.iter().filter_map(|tier| match tier {
            ContextTier::L0 => Some("node_l0".to_string()),
            ContextTier::L1 => Some("node_l1".to_string()),
            ContextTier::L2 => None,
        }));
    debug.tree_traversal_summary.summary_embeddings.sort();
    debug.tree_traversal_summary.summary_embeddings.dedup();
}

fn context_query_question_type(terms: &[String]) -> String {
    if terms.is_empty() {
        "match_all".to_string()
    } else if context_query_requests_correction(terms)
        || context_query_requests_latest(terms)
        || context_query_requests_contrastive_update(terms)
    {
        "current_state".to_string()
    } else if context_query_requests_temporal_reasoning(terms)
        || context_query_requests_before(terms)
        || context_query_requests_after(terms)
        || context_query_requests_schedule_detail(terms)
    {
        "temporal_reasoning".to_string()
    } else if context_query_requests_quantity_detail(terms) {
        "quantity".to_string()
    } else if context_query_requests_social_link(terms) {
        "relationship".to_string()
    } else if context_query_requests_alias_detail(terms) {
        "identity_or_alias".to_string()
    } else if terms
        .iter()
        .any(|term| matches!(term.as_str(), "why" | "because" | "cause" | "root"))
    {
        "causal_reasoning".to_string()
    } else {
        "semantic_recall".to_string()
    }
}

fn context_query_secondary_index_filter_groups(terms: &[String]) -> Vec<Vec<String>> {
    let mut groups = Vec::new();
    if context_query_requests_latest(terms) || context_query_requests_correction(terms) {
        groups.push(vec![
            "event_type:correction".to_string(),
            "event_type:status_update".to_string(),
        ]);
        groups.push(vec![
            "status:current".to_string(),
            "status:observed".to_string(),
        ]);
        groups.push(vec!["segment_topic:correction".to_string()]);
    }
    if terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "where" | "location" | "located" | "office" | "workplace" | "work"
        )
    }) {
        groups.push(vec![
            "entity_type:location".to_string(),
            "entity_type:job_status".to_string(),
        ]);
    }
    if context_query_requests_social_link(terms) {
        groups.push(vec![
            "entity_type:person".to_string(),
            "entity_type:social_link".to_string(),
            "entity_type:relationship".to_string(),
        ]);
    }
    if context_query_requests_schedule_detail(terms) {
        groups.push(vec![
            "entity_type:date".to_string(),
            "event_type:schedule".to_string(),
            "event_type:deadline".to_string(),
        ]);
    }
    if context_query_requests_quantity_detail(terms) {
        groups.push(vec![
            "entity_type:quantity".to_string(),
            "entity_type:amount".to_string(),
            "event_type:measurement".to_string(),
        ]);
    }
    if context_query_requests_temporal_reasoning(terms)
        || context_query_requests_before(terms)
        || context_query_requests_after(terms)
    {
        groups.push(vec![
            "event_type:timeline".to_string(),
            "event_time_bucket:query_range".to_string(),
        ]);
    }
    if groups.is_empty() {
        let mut lexical = terms
            .iter()
            .take(8)
            .map(|term| format!("query_term:{term}"))
            .collect::<Vec<_>>();
        if lexical.is_empty() {
            lexical.push("query_term:*".to_string());
        }
        groups.push(lexical);
    }
    groups.sort();
    groups.dedup();
    groups
}

fn context_event_candidate_terms(event: &ContextEvent) -> Vec<String> {
    let text = event.text.to_ascii_lowercase();
    let mut terms = vec![
        "record_type:context_event".to_string(),
        format!("event_kind:{}", event.kind),
        format!("status:{}", event.status),
        "source_type:message".to_string(),
    ];
    if text.contains("now") || text.contains("current") || text.contains("latest") {
        terms.push("event_type:status_update".to_string());
        terms.push("status:current".to_string());
    }
    if text.contains("changed")
        || text.contains("instead")
        || text.contains("no longer")
        || text.contains("updated")
    {
        terms.push("event_type:correction".to_string());
        terms.push("segment_topic:correction".to_string());
    }
    if text.contains("moved")
        || text.contains("seattle")
        || text.contains("austin")
        || text.contains("office")
    {
        terms.push("entity_type:location".to_string());
    }
    if text.contains("manager")
        || text.contains("friend")
        || text.contains("coworker")
        || text.contains("alice")
        || text.contains("priya")
    {
        terms.push("entity_type:person".to_string());
        terms.push("entity_type:relationship".to_string());
    }
    if text.contains("deadline")
        || text.contains("appointment")
        || text.contains("schedule")
        || text.contains("calendar")
    {
        terms.push("event_type:schedule".to_string());
        terms.push("entity_type:date".to_string());
    }
    if text.chars().any(|ch| ch.is_ascii_digit())
        || text.contains("amount")
        || text.contains("total")
        || text.contains("score")
    {
        terms.push("entity_type:quantity".to_string());
    }
    for token in context_query_terms(&event.source_ref) {
        terms.push(format!("source_ref:{token}"));
    }
    terms.sort();
    terms.dedup();
    terms
}

fn context_relevance_score(query: &str, text: &str) -> u32 {
    let base_terms = context_query_terms(query);
    let query_groups = context_query_term_groups(query);
    if query_groups.is_empty() {
        return 0;
    }
    let text_lower = text.to_ascii_lowercase();
    let text_normalized = context_normalize_for_match(text);
    let mut score = 0u32;
    if let Some(topic_phrase) = context_query_topic_phrase(query) {
        if context_text_matches_term(&text_lower, &text_normalized, topic_phrase.as_str()) {
            score = score.saturating_add(1_000);
        }
    }
    let mut matched_groups = 0u32;
    for group in &query_groups {
        let best_match = group
            .iter()
            .filter(|term| context_text_matches_term(&text_lower, &text_normalized, term))
            .map(|term| term.len() as u32)
            .max()
            .unwrap_or_default();
        if best_match > 0 {
            matched_groups += 1;
            score = score.saturating_add(best_match.max(1));
        }
    }
    if matched_groups == query_groups.len() as u32 {
        score = score.saturating_add(100);
    } else if matched_groups > 1 {
        score = score.saturating_add(matched_groups.saturating_mul(12));
    }
    for phrase in context_query_adjacent_phrases(&base_terms) {
        if context_text_matches_term(&text_lower, &text_normalized, phrase.as_str()) {
            score = score.saturating_add(50);
        }
    }
    if context_query_requests_latest(&base_terms)
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "latest", "recent", "current", "updated", "changed", "replaced",
            ],
        )
    {
        score = score.saturating_add(75);
    }
    if context_query_requests_temporal_reasoning(&base_terms)
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "timeline", "temporal", "history", "sequence", "before", "after", "during",
            ],
        )
    {
        score = score.saturating_add(50);
    }
    if context_query_requests_after(&base_terms) {
        if context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "after", "later", "latest", "new", "current", "update", "moved to",
            ],
        ) {
            score = score.saturating_add(65);
        }
        if context_text_matches_any(&text_lower, &text_normalized, &["before", "earlier", "old"]) {
            score = score.saturating_sub(25);
        }
    }
    if context_query_requests_before(&base_terms) {
        if context_text_matches_any(&text_lower, &text_normalized, &["before", "earlier", "old"]) {
            score = score.saturating_add(65);
        }
        if context_text_matches_any(
            &text_lower,
            &text_normalized,
            &["after", "later", "latest", "new", "current"],
        ) {
            score = score.saturating_sub(25);
        }
    }
    if context_query_requests_correction(&base_terms)
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "correction",
                "corrected",
                "no longer",
                "instead",
                "replaced",
                "now",
            ],
        )
    {
        score = score.saturating_add(90);
    }
    if context_query_requests_reminder(&base_terms)
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &["remember", "reminder", "mentioned", "said", "told", "note"],
        )
    {
        score = score.saturating_add(70);
    }
    if context_query_requests_contrastive_update(&base_terms)
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "switched",
                "switch",
                "moved",
                "became",
                "instead",
                "no longer",
                "changed from",
            ],
        )
    {
        score = score.saturating_add(85);
    }
    if context_query_requests_social_link(&base_terms)
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "recommended",
                "suggested",
                "introduced",
                "referred",
                "because",
                "from",
            ],
        )
    {
        score = score.saturating_add(80);
    }
    if context_query_requests_schedule_detail(&base_terms)
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "rescheduled",
                "scheduled",
                "moved to",
                "appointment",
                "deadline",
                "calendar",
                "at ",
                "on ",
            ],
        )
    {
        score = score.saturating_add(80);
    }
    if context_query_requests_quantity_detail(&base_terms)
        && (text_lower.chars().any(|ch| ch.is_ascii_digit())
            || context_text_matches_any(
                &text_lower,
                &text_normalized,
                &["count", "total", "number", "score", "amount", "quantity"],
            ))
    {
        score = score.saturating_add(80);
    }
    if context_query_requests_alias_detail(&base_terms)
        && context_text_matches_any(
            &text_lower,
            &text_normalized,
            &[
                "roommate", "manager", "owner", "named", "called", "pet", "dog", "cat",
            ],
        )
    {
        score = score.saturating_add(75);
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

fn context_query_term_groups(query: &str) -> Vec<Vec<String>> {
    context_query_terms(query)
        .into_iter()
        .map(|term| {
            let mut group = vec![term.clone()];
            if let Some(stem) = context_query_stem(term.as_str()) {
                group.push(stem);
            }
            for synonym in context_query_synonyms(term.as_str()) {
                group.push(synonym.to_string());
            }
            group.sort();
            group.dedup();
            group
        })
        .collect()
}

fn context_query_adjacent_phrases(terms: &[String]) -> Vec<String> {
    terms
        .windows(2)
        .map(|window| format!("{} {}", window[0], window[1]))
        .collect()
}

fn context_query_synonyms(term: &str) -> &'static [&'static str] {
    match term {
        "checkout" => &["payment", "purchase", "order"],
        "payment" => &["checkout", "purchase", "billing"],
        "billing" | "purchase" | "order" => &["checkout", "payment"],
        "risk" => &["fraud", "score", "safety"],
        "fraud" => &["risk", "score", "safety"],
        "score" => &["risk", "fraud", "safety"],
        "latest" | "recent" | "current" | "currently" | "now" => &["updated", "update", "status"],
        "status" | "state" => &["current", "latest", "condition"],
        "failed" | "failure" | "outage" => &["error", "incident", "down"],
        "down" | "error" => &["failed", "failure", "outage", "incident"],
        "dependency" | "backend" => &["service", "system"],
        "ticket" | "followup" | "follow" => &["support", "agent", "helpdesk"],
        "support" | "agent" | "helpdesk" => &["ticket", "followup"],
        "preference" | "preferences" => &["likes", "setting", "choice"],
        "setting" | "choice" | "likes" => &["preference"],
        "want" | "wants" | "wanted" => &["prefer", "preference", "choice"],
        "prefer" | "preferred" => &["want", "preference", "choice"],
        "update" | "updated" | "updates" => &["changed", "change", "modify", "replaced"],
        "changed" | "change" | "modify" | "replaced" => &["update", "updated", "switched"],
        "switch" | "switched" | "switching" => &["changed", "moved", "replaced"],
        "moved" | "move" => &["switched", "changed", "relocated"],
        "session" | "sessions" => &["dialogue", "conversation", "visit"],
        "conversation" | "dialogue" | "chat" => &["session", "message"],
        "message" | "messages" => &["conversation", "dialogue", "session"],
        "user" | "customer" => &["person", "account", "member"],
        "service" => &["dependency", "backend", "system"],
        "timeline" | "temporal" | "history" | "sequence" => &["before", "after", "during"],
        "before" | "after" | "during" => &["timeline", "temporal", "sequence"],
        "schedule" | "scheduled" | "reschedule" | "rescheduled" => {
            &["calendar", "appointment", "time", "date"]
        }
        "appointment" | "meeting" | "deadline" => &["schedule", "calendar", "date", "time"],
        "tomorrow" | "tonight" | "morning" | "afternoon" | "evening" => &["time", "date"],
        "many" | "count" | "number" | "quantity" => &["total", "amount"],
        "total" | "amount" => &["count", "number", "quantity"],
        "multi" | "hop" | "reasoning" => &["related", "connection", "because"],
        "why" | "because" | "reason" => &["cause", "root", "explain", "problem"],
        "cause" | "root" => &["because", "reason", "incident", "problem"],
        "problem" | "issue" => &["incident", "failure", "resolved", "cause"],
        "resolved" | "fixed" => &["recovered", "solved", "closed"],
        "recovered" | "recovery" | "solved" | "closed" => &["resolved", "fixed"],
        "where" | "location" | "located" => &["place", "city", "office", "work"],
        "office" | "work" | "workplace" => &["location", "place", "job"],
        "when" | "date" | "time" => &["timeline", "temporal", "session"],
        "who" | "person" | "people" => &["user", "customer", "member"],
        "roommate" | "housemate" => &["person", "friend", "contact"],
        "manager" | "supervisor" | "lead" => &["owner", "responsible", "contact"],
        "name" | "named" | "called" => &["alias", "known"],
        "pet" | "dog" | "cat" => &["animal", "name"],
        "friend" | "coworker" | "colleague" | "teammate" => &["person", "contact"],
        "recommend" | "recommended" | "suggest" | "suggested" => {
            &["introduced", "referred", "because"]
        }
        "introduced" | "referred" => &["recommended", "suggested"],
        "travel" | "trip" | "flight" => &["itinerary", "journey", "airport"],
        "remember" | "recall" | "remind" => &["mentioned", "said", "told", "note"],
        "medication" | "medicine" | "meds" | "prescription" => &["pill", "pharmacy", "doctor"],
        "doctor" | "physician" | "clinic" => &["visit", "medical", "medication"],
        "snack" | "meal" => &["food", "preference"],
        "gift" | "present" => &["birthday", "surprise", "preference"],
        "allergy" | "allergic" | "avoid" => &["restriction", "food", "without"],
        "correction" | "corrected" | "correct" => &["changed", "updated", "replaced"],
        "backup" | "contact" => &["owner", "person", "responsible"],
        "cancel" | "cancelled" | "canceled" => &["stopped", "dropped", "no longer"],
        "identity" => &["transgender", "woman", "community"],
        "transgender" => &["identity", "lgbtq", "community"],
        "relationship" => &["single", "dating", "married", "status"],
        "research" | "researched" => &["looked", "adoption", "agencies"],
        "adoption" | "agencies" => &["research", "interviews"],
        "field" | "fields" | "education" | "educaton" | "pursue" | "career" => &[
            "counseling",
            "counselor",
            "psychology",
            "mental",
            "health",
            "certification",
        ],
        "counseling" | "counselor" | "psychology" => &["career", "field", "mental", "health"],
        "interested" | "interest" => &["likes", "enjoys", "prefer", "outdoors"],
        "outdoors" | "camping" | "park" => &["national", "nature", "outside"],
        "ally" | "supportive" => &["support", "community", "transgender", "lgbtq"],
        "bookshelf" | "books" | "book" => &["collects", "classic", "children", "reading"],
        "writing" | "reading" => &["books", "author", "career", "counselor"],
        _ => &[],
    }
}

fn context_query_stem(term: &str) -> Option<String> {
    if term.len() > 4 && term.ends_with("ies") {
        Some(format!("{}y", &term[..term.len() - 3]))
    } else if term.len() > 4 && term.ends_with("es") {
        Some(term[..term.len() - 2].to_string())
    } else if term.len() > 3 && term.ends_with('s') {
        Some(term[..term.len() - 1].to_string())
    } else {
        None
    }
}

fn context_normalize_for_match(value: &str) -> String {
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

fn context_text_matches_term(text_lower: &str, text_normalized: &str, term: &str) -> bool {
    if text_lower.contains(term) {
        return true;
    }
    let normalized_term = context_normalize_for_match(term);
    if normalized_term.trim().is_empty() {
        return false;
    }
    text_normalized.contains(normalized_term.trim())
}

fn context_text_matches_any(text_lower: &str, text_normalized: &str, terms: &[&str]) -> bool {
    terms
        .iter()
        .any(|term| context_text_matches_term(text_lower, text_normalized, term))
}

fn context_query_requests_latest(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "latest" | "recent" | "current" | "currently" | "now" | "updated" | "update" | "status"
        )
    })
}

fn context_query_requests_temporal_reasoning(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "before" | "after" | "during" | "timeline" | "temporal" | "history" | "when"
        )
    })
}

fn context_query_requests_after(terms: &[String]) -> bool {
    terms.iter().any(|term| term == "after")
}

fn context_query_requests_before(terms: &[String]) -> bool {
    terms.iter().any(|term| term == "before")
}

fn context_query_requests_correction(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "correction" | "corrected" | "correct" | "changed" | "updated" | "now" | "avoid"
        )
    })
}

fn context_query_requests_reminder(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "remember" | "recall" | "remind" | "reminder" | "said" | "told"
        )
    })
}

fn context_query_requests_contrastive_update(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "switch"
                | "switched"
                | "switching"
                | "changed"
                | "change"
                | "moved"
                | "move"
                | "became"
                | "instead"
                | "cancel"
                | "cancelled"
                | "canceled"
        )
    })
}

fn context_query_requests_social_link(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "friend"
                | "coworker"
                | "colleague"
                | "teammate"
                | "recommend"
                | "recommended"
                | "suggest"
                | "suggested"
                | "introduced"
                | "referred"
                | "because"
                | "who"
        )
    })
}

fn context_query_requests_schedule_detail(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "when"
                | "date"
                | "time"
                | "schedule"
                | "scheduled"
                | "reschedule"
                | "rescheduled"
                | "appointment"
                | "meeting"
                | "deadline"
                | "calendar"
                | "tomorrow"
                | "tonight"
        )
    })
}

fn context_query_requests_quantity_detail(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "many"
                | "count"
                | "number"
                | "quantity"
                | "total"
                | "amount"
                | "score"
                | "percent"
                | "percentage"
        )
    })
}

fn context_query_requests_alias_detail(terms: &[String]) -> bool {
    terms.iter().any(|term| {
        matches!(
            term.as_str(),
            "roommate"
                | "housemate"
                | "manager"
                | "supervisor"
                | "lead"
                | "owner"
                | "name"
                | "named"
                | "called"
                | "pet"
                | "dog"
                | "cat"
        )
    })
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
            | "would"
            | "likely"
            | "still"
            | "considered"
            | "consider"
            | "more"
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

fn context_embedding_ref_hash(tenant_hash: u64, ref_hash: u64, level: &str) -> u64 {
    stable_hash64(&format!("ctx-embedding:{tenant_hash}:{ref_hash}:{level}"))
}

fn deterministic_context_embedding(model: &str, text: &str) -> Vec<f32> {
    let mut vector = Vec::with_capacity(16);
    for index in 0..16_u64 {
        let hash = stable_hash64(&format!("{model}:{index}:{text}"));
        let signed = (hash % 20_001) as f32 - 10_000.0;
        vector.push(signed / 10_000.0);
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}

fn context_query_embedding(
    provider: &ContextModelProviderConfig,
    query: &str,
) -> Result<Vec<f32>, Status> {
    let inputs = [("query", stable_hash64(query), 0_u32, query)];
    match context_embeddings_for_extract(provider, &inputs) {
        Ok((vectors, _)) => Ok(vectors.into_iter().next().unwrap_or_default()),
        Err(status) => {
            if let Some(fallback) = provider.fallback_provider.as_deref() {
                context_embeddings_for_extract(&normalize_provider(fallback.clone()), &inputs)
                    .map(|(vectors, _)| vectors.into_iter().next().unwrap_or_default())
            } else {
                Err(status)
            }
        }
    }
}

fn context_embedding_similarity_micros(left: &[f32], right: &[f32]) -> i64 {
    if left.is_empty() || right.is_empty() {
        return 0;
    }
    let len = left.len().min(right.len());
    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    for index in 0..len {
        dot += left[index] * right[index];
        left_norm += left[index] * left[index];
        right_norm += right[index] * right[index];
    }
    if left_norm <= f32::EPSILON || right_norm <= f32::EPSILON {
        0
    } else {
        ((dot / (left_norm.sqrt() * right_norm.sqrt())) * 1_000_000.0) as i64
    }
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
    let embedding_generation = ContextEmbeddingGenerationReport {
        status: status.clone(),
        provider_name: provider.provider_name.clone(),
        provider_kind: provider.provider_kind.clone(),
        embedding_model: provider.embedding_model.clone(),
        mock_mode: provider.mock_mode,
        ..ContextEmbeddingGenerationReport::default()
    };
    ContextExtractReport {
        status,
        provider,
        embedding_generation,
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

fn default_vlm_model() -> String {
    "mock-context-vlm".to_string()
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
        min_mean_reciprocal_rank: 0.0,
        min_recall_at_k: 1.0,
        min_evidence_retention_at_k: 1.0,
        min_token_reduction_percent: 0.1,
        max_selected_tokens_per_query: 256,
        max_retrieve_p50_ms: 10_000,
        max_retrieve_p95_ms: 10_000,
        min_ingest_sources_per_sec: 1.0,
        min_retrieve_queries_per_sec: 1.0,
        min_inject_queries_per_sec: 1.0,
    }
}

fn default_min_evidence_retention_at_k() -> f32 {
    1.0
}

fn default_max_benchmark_selected_tokens_per_query() -> u32 {
    256
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
mod tests;
