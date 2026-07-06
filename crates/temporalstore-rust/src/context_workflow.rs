use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;

mod query;
mod resource;
mod skill;

pub use resource::{
    context_resource_chunk_embedding, parse_context_resource, update_context_resource_lifecycle,
};
pub use skill::{
    context_skill_registry_from_parsed, parse_context_skill_markdown,
    select_context_skills_for_retrieval, update_context_skill_registry,
};

use self::query::*;
use self::resource::{
    context_resource_lifecycle_report, default_resource_max_chunk_chars,
    default_resource_overlap_chars, default_resource_parser_name, default_resource_parser_version,
};

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
    #[serde(default)]
    pub source_ref: String,
    #[serde(default)]
    pub related_node_hashes: Vec<u64>,
    #[serde(default)]
    pub summary_refs: Vec<String>,
    #[serde(default)]
    pub compact_summary_ref: String,
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
    #[serde(default)]
    pub owner_scope: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub watch_interval_minutes: u64,
    #[serde(default)]
    pub parser_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContextResourceLifecycleAction {
    #[default]
    Add,
    Watch,
    Refresh,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContextResourceImportKind {
    #[default]
    Text,
    Markdown,
    Skill,
    Url,
    GitRepo,
    CodeRepo,
    Pdf,
    Document,
    FeishuDoc,
    WatchedResource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContextScopeLayer {
    Global,
    Tenant,
    User,
    #[default]
    Workspace,
    Agent,
    Session,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextScopeDescriptor {
    pub raw_scope: String,
    pub layer: ContextScopeLayer,
    pub owner_id: String,
    pub shared_graph_scope: String,
    pub producer_agent_id: String,
    pub precedence_rank: u8,
}

impl Default for ContextScopeDescriptor {
    fn default() -> Self {
        context_scope_descriptor("user")
    }
}

pub fn context_scope_layer_name(layer: ContextScopeLayer) -> &'static str {
    match layer {
        ContextScopeLayer::Global => "global",
        ContextScopeLayer::Tenant => "tenant",
        ContextScopeLayer::User => "user",
        ContextScopeLayer::Workspace => "workspace",
        ContextScopeLayer::Agent => "agent",
        ContextScopeLayer::Session => "session",
    }
}

pub fn context_scope_descriptor(raw_scope: impl AsRef<str>) -> ContextScopeDescriptor {
    let trimmed = raw_scope.as_ref().trim();
    let raw_scope = if trimmed.is_empty() { "user" } else { trimmed };
    let lower = raw_scope.to_ascii_lowercase();
    let parse_owner = |prefix: &str| -> String {
        raw_scope
            .split_once(':')
            .map(|(_, rest)| rest.trim())
            .filter(|rest| !rest.is_empty())
            .unwrap_or(prefix)
            .to_string()
    };

    let (layer, owner_id, shared_graph_scope, producer_agent_id, precedence_rank) =
        if matches!(lower.as_str(), "global" | "all") {
            (
                ContextScopeLayer::Global,
                "global".to_string(),
                "global".to_string(),
                String::new(),
                0,
            )
        } else if lower.starts_with("tenant:") || lower.starts_with("org:") {
            let owner = parse_owner("tenant");
            (
                ContextScopeLayer::Tenant,
                owner.clone(),
                format!("tenant:{owner}"),
                String::new(),
                10,
            )
        } else if lower == "user" || lower.starts_with("user:") {
            let owner = parse_owner("user");
            (
                ContextScopeLayer::User,
                owner.clone(),
                format!("user:{owner}"),
                String::new(),
                20,
            )
        } else if lower.starts_with("agent:") || lower.starts_with("producer:") {
            let owner = parse_owner("agent");
            (
                ContextScopeLayer::Agent,
                owner.clone(),
                "user:user".to_string(),
                owner,
                40,
            )
        } else if lower.starts_with("session:") {
            let owner = parse_owner("session");
            (
                ContextScopeLayer::Session,
                owner,
                "user:user".to_string(),
                String::new(),
                50,
            )
        } else {
            let owner = raw_scope
                .split_once(':')
                .map(|(_, rest)| rest.trim())
                .filter(|rest| !rest.is_empty())
                .unwrap_or(raw_scope)
                .to_string();
            (
                ContextScopeLayer::Workspace,
                owner.clone(),
                format!("workspace:{owner}"),
                String::new(),
                30,
            )
        };

    ContextScopeDescriptor {
        raw_scope: raw_scope.to_string(),
        layer,
        owner_id,
        shared_graph_scope,
        producer_agent_id,
        precedence_rank,
    }
}

pub fn context_scope_matches(
    requested: &ContextScopeDescriptor,
    candidate: &ContextScopeDescriptor,
) -> bool {
    if matches!(candidate.layer, ContextScopeLayer::Global) {
        return true;
    }
    if matches!(requested.layer, ContextScopeLayer::Global) {
        return true;
    }
    if candidate
        .raw_scope
        .eq_ignore_ascii_case(&requested.raw_scope)
        || candidate.shared_graph_scope == requested.shared_graph_scope
    {
        return true;
    }
    matches!(
        (candidate.layer, requested.layer),
        (ContextScopeLayer::Tenant, ContextScopeLayer::User)
            | (ContextScopeLayer::Tenant, ContextScopeLayer::Workspace)
            | (ContextScopeLayer::Tenant, ContextScopeLayer::Agent)
            | (ContextScopeLayer::Tenant, ContextScopeLayer::Session)
            | (ContextScopeLayer::User, ContextScopeLayer::Workspace)
            | (ContextScopeLayer::User, ContextScopeLayer::Agent)
            | (ContextScopeLayer::User, ContextScopeLayer::Session)
            | (ContextScopeLayer::Agent, ContextScopeLayer::User)
            | (ContextScopeLayer::Agent, ContextScopeLayer::Workspace)
            | (ContextScopeLayer::Session, ContextScopeLayer::User)
            | (ContextScopeLayer::Session, ContextScopeLayer::Workspace)
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceLifecycleRecord {
    pub raw_uri: String,
    pub target_uri: String,
    pub owner_scope: String,
    #[serde(default)]
    pub scope: ContextScopeDescriptor,
    pub parser_name: String,
    pub parser_version: String,
    pub resource_type: String,
    pub import_kind: ContextResourceImportKind,
    pub action: ContextResourceLifecycleAction,
    pub version: String,
    pub content_hash: u64,
    pub stale: bool,
    pub invalidates_version: String,
    pub watched: bool,
    pub watch_interval_minutes: u64,
    pub next_refresh_after_ms: u64,
    pub deleted: bool,
    pub chunk_count: usize,
}

impl Default for ContextResourceLifecycleRecord {
    fn default() -> Self {
        Self {
            raw_uri: String::new(),
            target_uri: String::new(),
            owner_scope: "user".to_string(),
            scope: context_scope_descriptor("user"),
            parser_name: default_resource_parser_name(),
            parser_version: default_resource_parser_version(),
            resource_type: "txt".to_string(),
            import_kind: ContextResourceImportKind::Text,
            action: ContextResourceLifecycleAction::Add,
            version: String::new(),
            content_hash: 0,
            stale: false,
            invalidates_version: String::new(),
            watched: false,
            watch_interval_minutes: 0,
            next_refresh_after_ms: 0,
            deleted: false,
            chunk_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceLifecycleUpdate {
    pub raw_uri: String,
    pub action: ContextResourceLifecycleAction,
    #[serde(default)]
    pub owner_scope: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub watch_interval_minutes: u64,
    #[serde(default)]
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextResourceLifecycleReport {
    pub status: Status,
    pub resources: Vec<ContextResourceLifecycleRecord>,
    pub watched_count: usize,
    pub stale_count: usize,
    pub deleted_count: usize,
    pub refresh_due_count: usize,
    pub import_kinds: BTreeMap<String, usize>,
}

impl Default for ContextResourceLifecycleReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            resources: Vec::new(),
            watched_count: 0,
            stale_count: 0,
            deleted_count: 0,
            refresh_due_count: 0,
            import_kinds: BTreeMap::new(),
        }
    }
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
    #[serde(default)]
    pub lifecycle: ContextResourceLifecycleRecord,
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
    #[serde(default)]
    pub scope: ContextScopeDescriptor,
    #[serde(default = "default_context_skill_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub precedence: ContextSkillPrecedence,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum ContextSkillPrecedence {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillRegistryEntry {
    pub skill_name: String,
    pub source_ref: String,
    pub description: String,
    pub version: String,
    pub owner_scope: String,
    #[serde(default)]
    pub scope: ContextScopeDescriptor,
    pub enabled: bool,
    pub precedence: ContextSkillPrecedence,
    pub triggers: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub tag_refs: Vec<String>,
    pub model_refs: Vec<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillRegistryUpdate {
    pub skill_name: String,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub precedence: Option<ContextSkillPrecedence>,
    #[serde(default)]
    pub owner_scope: Option<String>,
    #[serde(default)]
    pub triggers: Option<Vec<String>>,
    #[serde(default)]
    pub allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillRegistryReport {
    pub status: Status,
    pub entries: Vec<ContextSkillRegistryEntry>,
    pub enabled_count: usize,
    pub disabled_count: usize,
    pub highest_precedence: ContextSkillPrecedence,
    pub version_updates: Vec<String>,
    #[serde(default)]
    pub scope_layers: BTreeMap<String, usize>,
    #[serde(default)]
    pub shared_graph_scope_count: usize,
    #[serde(default)]
    pub producer_agent_count: usize,
}

impl Default for ContextSkillRegistryReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            entries: Vec::new(),
            enabled_count: 0,
            disabled_count: 0,
            highest_precedence: ContextSkillPrecedence::Normal,
            version_updates: Vec::new(),
            scope_layers: BTreeMap::new(),
            shared_graph_scope_count: 0,
            producer_agent_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillSelectionRequest {
    pub query: String,
    #[serde(default)]
    pub owner_scope: String,
    #[serde(default)]
    pub allowed_scope_layers: Vec<ContextScopeLayer>,
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub include_disabled: bool,
    #[serde(default = "default_skill_selection_limit")]
    pub limit: usize,
    pub registry: Vec<ContextSkillRegistryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillSelectionCandidate {
    pub skill_name: String,
    pub version: String,
    pub owner_scope: String,
    #[serde(default)]
    pub scope: ContextScopeDescriptor,
    pub precedence: ContextSkillPrecedence,
    pub score: i64,
    pub matched_triggers: Vec<String>,
    pub allowed_tool_match: bool,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSkillSelectionReport {
    pub status: Status,
    pub query: String,
    pub selected: Vec<ContextSkillSelectionCandidate>,
    pub skipped_disabled: Vec<String>,
    pub skipped_owner_scope: Vec<String>,
    pub skipped_tool: Vec<String>,
    #[serde(default)]
    pub scope_resolution_order: Vec<String>,
    #[serde(default)]
    pub agent_producers: Vec<String>,
    #[serde(default)]
    pub shared_graph_scope_count: usize,
}

impl Default for ContextSkillSelectionReport {
    fn default() -> Self {
        Self {
            status: Status::ok(),
            query: String::new(),
            selected: Vec::new(),
            skipped_disabled: Vec::new(),
            skipped_owner_scope: Vec::new(),
            skipped_tool: Vec::new(),
            scope_resolution_order: Vec::new(),
            agent_producers: Vec::new(),
            shared_graph_scope_count: 0,
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextResourceSkillEmbeddingEvidenceReport {
    pub extract_count: usize,
    pub requested_vector_count: usize,
    pub generated_vector_count: usize,
    pub live_call_count: usize,
    pub mock_generation_count: usize,
    pub fallback_generation_count: usize,
    pub production_evidence_ready: bool,
    pub provider_names: Vec<String>,
    pub embedding_models: Vec<String>,
    pub vector_dimensions: Vec<usize>,
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
    #[serde(default)]
    pub resource_lifecycle: ContextResourceLifecycleReport,
    pub skills: Vec<ContextSkillParseReport>,
    #[serde(default)]
    pub skill_registry: ContextSkillRegistryReport,
    #[serde(default)]
    pub skill_selection: ContextSkillSelectionReport,
    pub ingest: ContextIngestExtractReport,
    pub embedding_refs: Vec<u64>,
    #[serde(default)]
    pub embedding_evidence: ContextResourceSkillEmbeddingEvidenceReport,
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
    #[serde(default)]
    pub extracted_node_count: usize,
    #[serde(default)]
    pub extracted_event_count: usize,
    #[serde(default)]
    pub extracted_index_ref_count: usize,
    #[serde(default)]
    pub extracted_dirty_marker_count: usize,
    #[serde(default)]
    pub extracted_summary_ref_count: usize,
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
    #[serde(default = "default_summary_fanout_node_limit")]
    pub max_summary_nodes: usize,
    #[serde(default = "default_event_fanout_node_limit")]
    pub max_event_nodes: usize,
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
    pub fanout_plan: ContextFanoutPlanReport,
    #[serde(default)]
    pub parity: ContextPipelineParityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextFanoutPlanReport {
    pub strategy: String,
    pub namespace_node_candidates: usize,
    pub summary_candidate_nodes: usize,
    pub summary_selected_nodes: usize,
    pub event_expanded_nodes: usize,
    pub skipped_node_count: usize,
    pub summary_lookup_batches: usize,
    pub secondary_index_filter_group_count: usize,
    pub selected_node_hashes: Vec<u64>,
    pub skipped_node_hashes: Vec<u64>,
    pub locality_keys: Vec<String>,
    pub fallback_to_flat: bool,
    pub fanout_reduced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextQueryUnderstandingDebug {
    #[serde(default)]
    pub debug_schema: String,
    #[serde(default)]
    pub query_hash: u64,
    #[serde(default)]
    pub normalized_query_terms: Vec<String>,
    pub question_type: String,
    pub secondary_index_filter_groups: Vec<Vec<String>>,
    #[serde(default)]
    pub verbose_filter_groups: Vec<ContextQueryFilterGroupDebug>,
    #[serde(default)]
    pub filter_group_summary: ContextQueryFilterGroupSummaryDebug,
    pub candidates_passing_prefilter: usize,
    pub candidates_dropped_before_scoring: usize,
    pub tree_traversal_summary: ContextTreeTraversalDebug,
    pub prefilter_candidate_sample: Vec<ContextPrefilterCandidateDebug>,
    #[serde(default)]
    pub selected_refs: Vec<ContextSelectedRefDebug>,
    #[serde(default)]
    pub injection_ordering: Vec<ContextInjectionOrderingDebug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextQueryFilterGroupSummaryDebug {
    pub total_groups: usize,
    pub secondary_index_group_count: usize,
    pub lexical_group_count: usize,
    pub total_candidate_count: usize,
    pub total_matched_count: usize,
    pub total_dropped_count: usize,
    pub total_selected_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextQueryFilterGroupDebug {
    pub group_id: String,
    pub group_kind: String,
    pub terms: Vec<String>,
    pub candidate_ref_hashes: Vec<u64>,
    pub matched_ref_hashes: Vec<u64>,
    pub dropped_ref_hashes: Vec<u64>,
    pub selected_ref_hashes: Vec<u64>,
    pub candidate_count: usize,
    pub matched_count: usize,
    pub dropped_count: usize,
    pub selected_count: usize,
    #[serde(default)]
    pub candidate_decisions: Vec<ContextFilterGroupCandidateDecisionDebug>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextFilterGroupCandidateDecisionDebug {
    pub ref_hash: u64,
    pub record_type: String,
    pub event_time_ms: u64,
    pub decision: String,
    pub reason: String,
    pub matched_terms: Vec<String>,
    pub candidate_terms: Vec<String>,
    pub text: String,
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
    pub summary_embedding_lookup_batches: usize,
    #[serde(default)]
    pub query_embedding_dimension: usize,
    #[serde(default)]
    pub query_embedding_provider: String,
    #[serde(default)]
    pub namespace_node_candidates: usize,
    #[serde(default)]
    pub event_expanded_node_count: usize,
    #[serde(default)]
    pub skipped_node_count: usize,
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
    #[serde(default)]
    pub drop_reason: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSelectedRefDebug {
    pub rank: usize,
    pub uri: String,
    pub source_ref: String,
    pub tier: ContextTier,
    pub ref_hash: u64,
    pub node_hash: u64,
    pub event_time_ms: u64,
    pub relevance_score: u32,
    pub matched_filter_groups: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextInjectionOrderingDebug {
    pub prompt_rank: usize,
    pub source_ref: String,
    pub tier: ContextTier,
    pub ref_hash: u64,
    pub token_estimate: u32,
    pub selection_reason: String,
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
            source_ref: String::new(),
            related_node_hashes: Vec::new(),
            summary_refs: Vec::new(),
            compact_summary_ref: String::new(),
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
    let event = context_event_with_storage_keys(
        node_hash,
        ContextEvent {
            event_id_hash,
            event_time_ms: timestamp_ms,
            ingestion_time_ms: timestamp_ms,
            kind: source_kind_code(request.source_kind),
            event_type: 1,
            actor_hash: stable_hash64(&request.source_id),
            status: 1,
            valid_until_ms: 0,
            confidence: 1.0,
            importance: context_importance(&request.body),
            text: request.body.clone(),
            source_ref: String::new(),
            related_node_hashes: Vec::new(),
            compact_attrs: Vec::new(),
        },
    );
    let index_ref = ContextIndexRef {
        primary_node_hash: node_hash,
        primary_event_time_ms: timestamp_ms,
        event_id_hash,
    };
    let summary_refs = vec![
        format!("summary:{node_hash}:l0"),
        format!("summary:{node_hash}:l1"),
    ];
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
        model_hash: context_embedding_model_hash(&provider.model),
        vector: embedding_vectors[0].clone(),
        updated_at_ms: timestamp_ms,
    };
    let embedding_l1 = ContextEmbedding {
        ref_hash: context_embedding_ref_hash(request.tenant_hash, node_hash, "node_l1"),
        level: 2,
        model_hash: context_embedding_model_hash(&provider.model),
        vector: embedding_vectors[1].clone(),
        updated_at_ms: timestamp_ms,
    };
    let embedding_event = ContextEmbedding {
        ref_hash: context_embedding_ref_hash(request.tenant_hash, event_id_hash, "event_text"),
        level: 3,
        model_hash: context_embedding_model_hash(&provider.model),
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
                source_ref: request.source_id.clone(),
                related_node_hashes: vec![node_hash],
                summary_refs,
                compact_summary_ref: l1.clone(),
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
        source_ref: request.source_id,
        related_node_hashes: vec![node_hash],
        summary_refs,
        compact_summary_ref: l1.clone(),
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
        max_summary_nodes: default_summary_fanout_node_limit(),
        max_event_nodes: default_event_fanout_node_limit(),
        provider: request.provider,
    };
    let summary = ContextIngestExtractSummary {
        source_count,
        accepted,
        failed,
        unique_node_count: node_hashes.len(),
        extracted_node_count: extracts.len(),
        extracted_event_count: extracts.len(),
        extracted_index_ref_count: extracts.len(),
        extracted_dirty_marker_count: extracts.len(),
        extracted_summary_ref_count: extracts
            .iter()
            .map(|extract| extract.summary_refs.len())
            .sum(),
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
        if skill.enabled {
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
        }
        skills.push(skill);
    }
    let skill_registry = context_skill_registry_from_parsed(&skills, request.start_time_ms);
    let skill_selection = if skill_registry.entries.is_empty() {
        ContextSkillSelectionReport::default()
    } else {
        select_context_skills_for_retrieval(ContextSkillSelectionRequest {
            query: request.query.clone(),
            owner_scope: String::new(),
            allowed_scope_layers: Vec::new(),
            tool_name: "context_workflow_harness".to_string(),
            include_disabled: false,
            limit: default_skill_selection_limit(),
            registry: skill_registry.entries.clone(),
        })
    };

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
            value: extract.source_ref.clone(),
            updated_at_ms: extract.event.event_time_ms,
            valid_from_ms: extract.event.event_time_ms,
            confidence: 1.0,
            source_event_hashes: vec![extract.event.event_id_hash],
        };
        let compression = ContextCompressionEvent {
            compression_id_hash: stable_hash64(&format!(
                "ctx-resource-skill-compress:{}:{}",
                extract.source_ref, extract.event.event_time_ms
            )),
            node_hash: extract.node.node_hash,
            source_start_ms: extract.event.event_time_ms,
            source_end_ms: extract.event.event_time_ms.saturating_add(1),
            compressed_time_ms: extract.event.event_time_ms.saturating_add(1),
            summary: extract.compact_summary_ref.clone(),
        };
        let mut index_writes = vec![
            ("source_ref".to_string(), extract.source_ref.clone()),
            ("entity_ref".to_string(), entity_ref.clone()),
        ];
        for summary_ref in &extract.summary_refs {
            index_writes.push(("summary_ref".to_string(), summary_ref.clone()));
        }
        if let Some(resource_ref) = resource_ref_by_source.get(&extract.source_ref) {
            index_writes.push(("resource_ref".to_string(), resource_ref.clone()));
        }
        if let Some(skill_ref) = skill_ref_by_source.get(&extract.source_ref) {
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
    let embedding_evidence = context_resource_skill_embedding_evidence(&ingest.extracts);

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
    let resource_lifecycle = context_resource_lifecycle_report(
        resources
            .iter()
            .map(|resource| resource.lifecycle.clone())
            .collect(),
        request.start_time_ms,
    );

    ContextResourceSkillIngestReport {
        status,
        resources,
        resource_lifecycle,
        skills,
        skill_registry,
        skill_selection,
        ingest,
        embedding_refs,
        embedding_evidence,
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

fn context_resource_skill_embedding_evidence(
    extracts: &[ContextExtractReport],
) -> ContextResourceSkillEmbeddingEvidenceReport {
    let mut report = ContextResourceSkillEmbeddingEvidenceReport {
        extract_count: extracts.len(),
        production_evidence_ready: !extracts.is_empty(),
        ..ContextResourceSkillEmbeddingEvidenceReport::default()
    };
    for extract in extracts {
        let generation = &extract.embedding_generation;
        report.requested_vector_count = report
            .requested_vector_count
            .saturating_add(generation.requested_vector_count);
        report.generated_vector_count = report
            .generated_vector_count
            .saturating_add(generation.generated_vector_count);
        report.live_call_count = report
            .live_call_count
            .saturating_add(generation.live_call_count);
        if generation.mock_mode {
            report.mock_generation_count = report.mock_generation_count.saturating_add(1);
        }
        if generation.fallback_used {
            report.fallback_generation_count = report.fallback_generation_count.saturating_add(1);
        }
        report.provider_names.push(generation.provider_name.clone());
        report
            .embedding_models
            .push(generation.embedding_model.clone());
        if generation.vector_dimension > 0 {
            report.vector_dimensions.push(generation.vector_dimension);
        }
        report.production_evidence_ready &= generation.production_evidence_ready
            && !generation.mock_mode
            && generation.live_call_count > 0
            && generation.generated_vector_count == generation.requested_vector_count
            && generation.vector_dimension > 0;
    }
    report.provider_names.sort();
    report.provider_names.dedup();
    report.embedding_models.sort();
    report.embedding_models.dedup();
    report.vector_dimensions.sort_unstable();
    report.vector_dimensions.dedup();
    report
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
            max_summary_nodes: default_summary_fanout_node_limit(),
            max_event_nodes: default_event_fanout_node_limit(),
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
    let mut fanout_plan = ContextFanoutPlanReport {
        strategy: "hierarchical_summary_secondary_index_node_resource_colocation".to_string(),
        secondary_index_filter_group_count: query_understanding_debug
            .filter_group_summary
            .secondary_index_group_count,
        ..ContextFanoutPlanReport::default()
    };
    let tiers = if request.tiers.is_empty() {
        default_tiers()
    } else {
        request.tiers.clone()
    };
    let include_l0 = tiers.contains(&ContextTier::L0);
    let include_l1 = tiers.contains(&ContextTier::L1);
    let include_l2 = tiers.contains(&ContextTier::L2);
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
            fanout_plan,
            parity: context_pipeline_parity_evidence(),
        };
    } else {
        request.node_hashes.clone()
    };
    fanout_plan.namespace_node_candidates = node_hashes.len();
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
                fanout_plan,
                parity: context_pipeline_parity_evidence(),
            };
        }
    };
    let mut summary_ref_owners = BTreeMap::new();
    let mut summary_ref_hashes = Vec::with_capacity(node_hashes.len().saturating_mul(2));
    for node_hash in &node_hashes {
        for label in ["node_l0", "node_l1"] {
            let ref_hash = context_embedding_ref_hash(request.tenant_hash, *node_hash, label);
            summary_ref_owners.insert(ref_hash, *node_hash);
            summary_ref_hashes.push(ref_hash);
        }
    }
    let mut summary_scores_by_node = BTreeMap::<u64, (i64, usize)>::new();
    if !summary_ref_hashes.is_empty() {
        let embeddings = engine.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: Command::ContextQueryEmbeddings {
                tenant_hash: request.tenant_hash,
                ref_hashes: summary_ref_hashes,
                limit: Some(node_hashes.len().saturating_mul(2).max(1)),
            },
        });
        if let CommandResponse::ContextEmbeddings { embeddings } = embeddings.response {
            for embedding in embeddings {
                if let Some(node_hash) = summary_ref_owners.get(&embedding.ref_hash) {
                    let score =
                        context_embedding_similarity_micros(&query_embedding, &embedding.vector);
                    let entry = summary_scores_by_node.entry(*node_hash).or_default();
                    entry.0 = entry.0.max(score);
                    entry.1 = entry.1.saturating_add(1);
                }
            }
        }
    }
    let mut summary_scores = node_hashes
        .iter()
        .map(|node_hash| {
            let (best_score, found) = summary_scores_by_node
                .get(node_hash)
                .copied()
                .unwrap_or_default();
            (*node_hash, best_score, found)
        })
        .collect::<Vec<_>>();
    summary_scores.sort_by_key(|(node_hash, score, _)| (Reverse(*score), *node_hash));
    let summary_node_limit = request
        .max_summary_nodes
        .max(1)
        .min(summary_scores.len().max(1));
    let event_node_limit = request
        .max_event_nodes
        .max(1)
        .min(summary_node_limit.max(1));
    node_hashes = summary_scores
        .iter()
        .take(summary_node_limit)
        .map(|(node_hash, _, _)| *node_hash)
        .collect();
    let event_node_hashes = node_hashes
        .iter()
        .copied()
        .take(event_node_limit)
        .collect::<Vec<_>>();
    let skipped_node_hashes = summary_scores
        .iter()
        .skip(event_node_limit)
        .map(|(node_hash, _, _)| *node_hash)
        .collect::<Vec<_>>();
    fanout_plan.summary_candidate_nodes = summary_scores.len();
    fanout_plan.summary_selected_nodes = node_hashes.len();
    fanout_plan.event_expanded_nodes = event_node_hashes.len();
    fanout_plan.skipped_node_count = skipped_node_hashes.len();
    fanout_plan.summary_lookup_batches = usize::from(!summary_scores.is_empty());
    fanout_plan.selected_node_hashes = event_node_hashes.clone();
    fanout_plan.skipped_node_hashes = skipped_node_hashes;
    fanout_plan.locality_keys = event_node_hashes
        .iter()
        .map(|node_hash| format!("tenant:{}:node:{node_hash}", request.tenant_hash))
        .collect();
    fanout_plan.fanout_reduced =
        fanout_plan.event_expanded_nodes < fanout_plan.namespace_node_candidates;
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
        .summary_embedding_lookup_batches = usize::from(!summary_scores.is_empty());
    query_understanding_debug
        .tree_traversal_summary
        .query_embedding_dimension = query_embedding.len();
    query_understanding_debug
        .tree_traversal_summary
        .query_embedding_provider = retrieval_provider.provider_name.clone();
    query_understanding_debug
        .tree_traversal_summary
        .namespace_node_candidates = fanout_plan.namespace_node_candidates;
    query_understanding_debug
        .tree_traversal_summary
        .event_expanded_node_count = fanout_plan.event_expanded_nodes;
    query_understanding_debug
        .tree_traversal_summary
        .skipped_node_count = fanout_plan.skipped_node_count;
    query_understanding_debug
        .tree_traversal_summary
        .summary_embeddings = summary_scores
        .iter()
        .map(|(node_hash, score, found)| format!("node:{node_hash}:score:{score}:refs:{found}"))
        .collect();

    for node_hash in event_node_hashes {
        let mut node_source_ref = String::new();
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
            node_source_ref = node.raw_metadata_ref.clone();
            if include_l0 {
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
            if include_l1 {
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
                if include_l2 {
                    let source_ref = if event.source_ref.is_empty() {
                        node_source_ref.clone()
                    } else {
                        event.source_ref.clone()
                    };
                    blocks.push(ContextBlock {
                        uri: context_event_uri(request.tenant_hash, node_hash, event.event_time_ms),
                        tier: ContextTier::L2,
                        node_hash,
                        event_time_ms: event.event_time_ms,
                        text: event.text,
                        estimated_tokens: estimate_tokens(&source_ref),
                        source_ref,
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
        &request.query,
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
        fanout_plan,
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
        ingestion_time_ms: 0,
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
        source_ref: String::new(),
        related_node_hashes: Vec::new(),
        summary_refs: Vec::new(),
        compact_summary_ref: String::new(),
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

fn default_summary_fanout_node_limit() -> usize {
    32
}

fn default_event_fanout_node_limit() -> usize {
    16
}

fn default_context_skill_enabled() -> bool {
    true
}

fn default_skill_selection_limit() -> usize {
    8
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
