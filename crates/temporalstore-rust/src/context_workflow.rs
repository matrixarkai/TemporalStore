// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Max ref_hashes per ContextQueryEmbeddings command. Must not exceed the
/// engine's CONTEXT_MAX_LIMIT (engine/constants.rs), which command validation
/// enforces by rejecting larger requests. Retrieval chunks its summary-embedding
/// lookups at this size so a namespace with more nodes than the cap is still
/// fully scored instead of silently falling back to lexical ranking.
const CONTEXT_EMBEDDING_QUERY_CHUNK: usize = 1000;

/// Reason code stamped on an embedding-dirty marker written by the live extraction
/// path when the inline embedding call fails (after any fallback provider). Purely
/// diagnostic; the drainer treats every pending marker identically. Distinct from
/// the bulk-ingest deferral reason (2, see context_batch_ingest.rs).
const EMBEDDING_DIRTY_REASON_LIVE_FAILURE: u32 = 3;

/// Raw lexical-relevance score at/above which a node is treated as a full-strength
/// lexical match, mapped to `LEXICAL_MATCH_MICROS`. Scores below scale linearly.
/// A topic-phrase hit alone already contributes 1000 in `context_relevance_score_plan`.
const LEXICAL_SCORE_SATURATION: i64 = 1_000;
/// Micros a saturated lexical match maps to. Kept below the 1_000_000 ceiling a
/// perfect cosine match reaches so that, in a MIXED store, a strong semantic
/// (embedded) match still outranks a purely lexical one, while un-embedded nodes
/// remain rankable (never a flat 0) instead of collapsing to recency order.
const LEXICAL_MATCH_MICROS: i64 = 500_000;

/// Hybrid lexical fallback for un-embedded nodes in `retrieve_context`
/// (`MATRIXARK_CONTEXT_HYBRID_LEXICAL`, default ON). When enabled, a node with no
/// stored summary embedding is scored by query/text lexical overlap instead of the
/// flat 0 it used to get, so a freshly bulk-loaded store returns relevant results
/// before the embed drainer catches up. Embedded-node scoring is unchanged.
/// MATRIXARK_CONTEXT_SECONDARY_INDEX (default OFF): whether ingest builds the ctxidx
/// secondary-index refs at all.
///
/// Retrieval does not read them -- candidate selection is namespace nodes plus vector and
/// lexical scoring -- so on the live path they are write-only cost: one durable command per
/// (index, value) per ingest whose only reader is the ingest verifier checking that the writes
/// it just made landed. Skipped by default until the redesign gives them a reader; the env var
/// is the escape hatch for anything still relying on the query-back surface.
pub(crate) fn context_secondary_index_enabled() -> bool {
    std::env::var("MATRIXARK_CONTEXT_SECONDARY_INDEX")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn context_hybrid_lexical_enabled() -> bool {
    std::env::var("MATRIXARK_CONTEXT_HYBRID_LEXICAL")
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(true)
}

/// Map a raw lexical-relevance score into the same micros scale cosine similarity
/// uses (`context_embedding_similarity_micros`), so lexical and cosine node scores
/// merge into one ranking. 0 stays 0 (no overlap => not surfaced by lexical).
fn lexical_score_to_micros(raw: u32) -> i64 {
    let raw = (raw as i64).min(LEXICAL_SCORE_SATURATION);
    raw.saturating_mul(LEXICAL_MATCH_MICROS) / LEXICAL_SCORE_SATURATION
}

mod query;
mod embed_drainer;
mod resource;
mod benchmark;
mod model_provider;
mod ingest;
mod reports;
mod skill;

pub use resource::{
    parse_context_resource, update_context_resource_lifecycle,
};
pub use benchmark::{run_context_pipeline_benchmark, run_context_pipeline_benchmark_sweep};
pub(crate) use benchmark::*;
pub use reports::*;
pub use ingest::{ingest_extract_context, ingest_resource_skill_context, validate_resource_skill_secondary_indexes};
pub(crate) use model_provider::*;
pub use model_provider::context_backfill_embeddings;
pub use query::context_embedding_ref_hash;
pub use embed_drainer::{
    drain_embedding_dirty_once, embed_drainer_config_from_env, embed_drainer_enabled,
    run_embed_drainer_loop, EmbedDrainReport, EmbedDrainerConfig,
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
    ContextCompressionEvent, ContextEntity, ContextEvent, ContextIndexRef,
    ContextModelDescriptor, ContextNode, ContextPackAudit, ContextSummary,
    ContextDirtyNode, ExecuteRequest, ShardId, Status,
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
    pub dirty_marker: ContextDirtyNode,
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

pub fn default_context_resource_max_inline_bytes() -> usize {
    1 * 1024 * 1024
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextScopeDescriptor {
    pub raw_scope: String,
    pub layer: String,
    pub owner_id: String,
    pub shared_graph_scope: String,
    pub producer_agent_id: String,
    pub precedence_rank: u32,
}

fn context_scope_descriptor(owner_scope: &str) -> ContextScopeDescriptor {
    let raw_scope = owner_scope.trim();
    let normalized = if raw_scope.is_empty() {
        "user".to_string()
    } else {
        raw_scope.to_ascii_lowercase()
    };
    let (layer, owner_id) = normalized
        .split_once(':')
        .or_else(|| normalized.split_once('/'))
        .map(|(layer, owner_id)| (layer.to_string(), owner_id.to_string()))
        .unwrap_or_else(|| (normalized.clone(), String::new()));
    let layer = match layer.as_str() {
        "global" | "workspace" | "user" | "agent" | "resource" | "skill" => layer,
        _ => "user".to_string(),
    };
    let shared_graph_scope = match layer.as_str() {
        "global" => "global".to_string(),
        "workspace" => format!("workspace:{owner_id}"),
        "agent" => {
            if owner_id.is_empty() {
                "user".to_string()
            } else {
                format!("agent:{owner_id}")
            }
        }
        "resource" | "skill" => format!("{layer}:{owner_id}"),
        _ => {
            if owner_id.is_empty() {
                "user".to_string()
            } else {
                format!("user:{owner_id}")
            }
        }
    };
    let precedence_rank = match layer.as_str() {
        "agent" => 0,
        "user" => 10,
        "workspace" => 20,
        "resource" | "skill" => 30,
        "global" => 40,
        _ => 50,
    };
    ContextScopeDescriptor {
        raw_scope: raw_scope.to_string(),
        layer,
        owner_id: owner_id.clone(),
        shared_graph_scope,
        producer_agent_id: if normalized.starts_with("agent") {
            owner_id
        } else {
            String::new()
        },
        precedence_rank,
    }
}

fn context_scope_layer_name(layer: impl AsRef<str>) -> &'static str {
    match layer.as_ref() {
        "agent" => "agent",
        "user" => "user",
        "workspace" => "workspace",
        "resource" => "resource",
        "skill" => "skill",
        "global" => "global",
        _ => "user",
    }
}

fn context_scope_matches(
    requested: &ContextScopeDescriptor,
    candidate: &ContextScopeDescriptor,
) -> bool {
    candidate.layer == "global"
        || requested
            .raw_scope
            .eq_ignore_ascii_case(&candidate.raw_scope)
        || requested.shared_graph_scope == candidate.shared_graph_scope
        || (requested.layer == "agent" && matches!(candidate.layer.as_str(), "user" | "workspace"))
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
    WikiDoc,
    WatchedResource,
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
    #[serde(default)]
    pub payload_size_bytes: usize,
    #[serde(default)]
    pub max_inline_bytes: usize,
    /// Whether the payload is held in the record rather than in the object store.
    ///
    /// Defaulted explicitly rather than with a bare `#[serde(default)]`: on a bool that decodes an
    /// ABSENT field to `false`, which here means "the payload is elsewhere" and sends a reader to
    /// an `external_object_uri` that such a record does not carry. This type's own `Default` says
    /// true, and decoding it should not disagree with constructing it.
    #[serde(default = "inline_payload_default")]
    pub inline_payload: bool,
    #[serde(default)]
    pub external_object_uri: String,
}

/// A record with nothing said about where its payload lives holds it inline; see the field.
fn inline_payload_default() -> bool {
    true
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
            payload_size_bytes: 0,
            max_inline_bytes: default_context_resource_max_inline_bytes(),
            inline_payload: true,
            external_object_uri: String::new(),
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
    pub payload_size_bytes: usize,
    #[serde(default)]
    pub max_inline_bytes: usize,
    #[serde(default)]
    pub inline_payload: bool,
    #[serde(default)]
    pub external_object_uri: String,
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
    pub tool_name: String,
    #[serde(default)]
    pub include_disabled: bool,
    #[serde(default)]
    pub allowed_scope_layers: Vec<String>,
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
    #[serde(alias = "segment_count")]
    pub slab_count: usize,
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
    pub prefer_current_agent: bool,
    #[serde(default = "default_current_agent_scope_key")]
    pub current_agent_scope_key: String,
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
    // Candidate nodes whose node record carries no vector at all -- un-embedded, scored by the
    // hybrid lexical pass instead. (Named for the retired separate-row fallback it once
    // counted; the rows are gone, so any nonzero here means the backfill has work to do.)
    #[serde(default)]
    pub l0_row_fallback_nodes: usize,
    // Candidate vectors declined because their width did not match the query's, i.e. they were
    // written in a different embedding space. Counted separately from the un-embedded ones above
    // because the two ask for different repairs: an un-embedded node needs the backfill to run,
    // whereas a width conflict means the store holds vectors from two encoders and re-embedding
    // is the only fix. Nonzero here is the signal that would otherwise not exist -- comparing
    // across embedding spaces raises no error on its own.
    #[serde(default)]
    pub embedding_width_conflict_nodes: usize,
    /// Nodes declined because their vector was written by a DIFFERENT ENCODER at the same
    /// width. Separate from the width count on purpose: two widths means a provider outage
    /// seeded fallback vectors, while two encoders at one width means a model swap. Same
    /// symptom, different cause, different fix.
    pub embedding_model_conflict_nodes: usize,
    #[serde(default)]
    pub event_query_budget: usize,
    #[serde(default)]
    pub event_query_node_count: usize,
    #[serde(default)]
    pub event_query_returned_count: usize,
    pub secondary_index_filter_group_count: usize,
    pub selected_node_hashes: Vec<u64>,
    pub skipped_node_hashes: Vec<u64>,
    pub locality_keys: Vec<String>,
    pub current_agent_default_boost: u32,
    pub peer_agent_default_boost: u32,
    pub prefer_current_agent_configured: bool,
    pub peer_agent_demoted_by_default: bool,
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
    pub reference_model_profiles: Vec<ContextReferenceModelProfile>,
    pub reference_parity_cases: Vec<ContextReferenceParityCase>,
    pub reference_parity_categories: Vec<String>,
    pub open_model_provider_packaged: bool,
    pub open_model_local_run_proven: bool,
    pub vlm_provider_configured: bool,
    pub vlm_benchmark_proven: bool,
    pub policy: ContextWorkflowPolicy,
    pub parity: ContextPipelineParityEvidence,
    pub reference_comparison: String,
    pub supported_routes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextReferenceModelProfile {
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
pub struct ContextReferenceParityCase {
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
    pub native_context_models_ready: bool,
    pub native_context_model_ids_ready: bool,
    pub native_context_timeline_semantics_ready: bool,
    pub native_context_validation_limits_ready: bool,
    pub reference_tiers_ready: bool,
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
        "ContextNode/Event/IndexRef/PackAudit/SummaryDirty model aliases and protobuf wire encoders are implemented"
            .to_string(),
        "Context model ids 9-13 are exposed as first-class Rust descriptors".to_string(),
        "Context timeline fanout, key shapes, range windows, and validation limits are enforced"
            .to_string(),
        "Hierarchical L0/L1/L2 tiers are produced during extraction and consumed during retrieval/injection"
            .to_string(),
        "Context extraction persists node, event, index-ref, and dirty-summary commands through TemporalEngine"
            .to_string(),
        "Context injection persists ContextPackAudit selected and blocked refs".to_string(),
        "Context workflow harness validates local restart, shared-store sync/async replay, Raft replica reads, and unified conformance context corpus evidence"
            .to_string(),
    ];
    ContextPipelineParityEvidence {
        native_context_models_ready: true,
        native_context_model_ids_ready: true,
        native_context_timeline_semantics_ready: true,
        native_context_validation_limits_ready: true,
        reference_tiers_ready: true,
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

/// Single-source extraction (codex-hook path). Gates L1 by source richness so
/// thin events do not pay for the richer L1 tier.
pub fn extract_context(
    engine: &TemporalEngine,
    request: ContextExtractRequest,
) -> ContextExtractReport {
    extract_context_gated(engine, request, true)
}

/// Extraction with explicit control over L1 gating. The batch ingest path
/// (`ingest_extract_context`) passes `gate_l1 = false` so bulk resource/skill
/// docs always produce L1 and keep their fixed fanout contract (2 summaries +
/// 2 embeddings per extract -- the node and its level-2 summary are built from the
/// same `l1` text and share the one vector it produces).
pub(crate) fn extract_context_gated(
    engine: &TemporalEngine,
    request: ContextExtractRequest,
    gate_l1: bool,
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
            dirty_marker: ContextDirtyNode {
                node_hash: 0,
                first_event_time_ms: request.timestamp_ms,
                last_event_time_ms: request.timestamp_ms,
                reason: 0,
                propagate_depth: 0,
                mark_count: 0,
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
    // Emit L1 when not gating (bulk ingest) or when the source is rich enough to
    // warrant the richer, more expensive L1 tier. Thin gated nodes keep only L0
    // (no l1_ref, no l1 summary, no l1 embedding) -- the frugal path.
    let emit_l1 = !gate_l1 || context_source_warrants_l1(&request.body);
    let l1 = if emit_l1 {
        summaries.l1
    } else {
        String::new()
    };
    let l2_ref = summaries.l2_ref;
    let mut node = ContextNode {
        node_hash,
        parent_hash: 0,
        kind: source_kind_code(request.source_kind),
        canonical_name: request.title.clone(),
        l0: l0.clone(),
        status: 1,
        last_event_time_ms: timestamp_ms,
        l1_ref: l1.clone(),
        raw_metadata_ref: request.source_id.clone(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
    };
    let mut event = context_event_with_storage_keys(
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
            vector: Vec::new(),
        },
    );
    let index_ref = ContextIndexRef {
        primary_node_hash: node_hash,
        primary_event_time_ms: timestamp_ms,
        event_id_hash,
    };
    let mut summary_refs = vec![format!("summary:{node_hash}:l0")];
    if emit_l1 {
        summary_refs.push(format!("summary:{node_hash}:l1"));
    }
    let dirty_marker = ContextDirtyNode {
        node_hash,
        first_event_time_ms: timestamp_ms,
            last_event_time_ms: timestamp_ms,
        reason: 1,
        propagate_depth: 1,
        mark_count: 1,
    };

    let mut summary_l0 = ContextSummary {
        node_hash,
        level: 1,
        text: l0.clone(),
        valid_from_ms: timestamp_ms,
        vector: Vec::new(),
        embedding_model_hash: 0,
    };
    let mut summary_l1 = emit_l1.then(|| ContextSummary {
        node_hash,
        level: 2,
        text: l1.clone(),
        valid_from_ms: timestamp_ms,
        vector: Vec::new(),
        embedding_model_hash: 0,
    });
    // What the node is SEARCHED by is not what it is SHOWN as. `l0` is the routing preview --
    // title plus one sentence, 18 words -- and embedding it gave the node a vector built from
    // about 25 tokens of a 512-token window. Traversal ranks nodes on that vector, so the node
    // was represented by roughly 5% of what the encoder can read.
    //
    // `l1` is the same content with the most information-dense remaining sentences added, built
    // for exactly this ("carries more content for broader traversal"). The node embeds that and
    // keeps `l0` as its display text.
    //
    // Measured over 298 query pairs across 79 documents, e5-large at 512 dims, scoring nodes by
    // the vector each text produces:
    //
    //     node vector embeds        hit@1    hit@5
    //     L0 preview (57 chars)     73.8%    86.6%
    //     L1 summary (376 chars)    73.5%    91.6%
    //
    // hit@1 is unchanged -- the top answer was already as good as the preview could make it --
    // and hit@5 gains 5.0 points, which is what more signal in the vector buys: the right node
    // is far likelier to be in the set at all.
    //
    // When L1 is emitted, the string the node embeds is the string the level-2 summary embeds --
    // both are `l1`. It goes to the provider ONCE and the single vector that comes back is handed
    // to every owner of it. Listing it twice made the batch carry the same text in two slots, and
    // `context_embeddings_for_extract` maps inputs to vectors one for one with no dedupe, so a
    // real OpenAI-compatible provider was asked to encode it twice on every rich ingest and billed
    // for both. Embedding is the slowest part of an ingest; the second copy bought a vector that
    // was already in hand.
    //
    // So the batch is two texts whether or not L1 is emitted: the text the node is searched by,
    // and the event body.
    let node_embedding_text = if emit_l1 { l1.as_str() } else { l0.as_str() };
    let embedding_inputs: Vec<(&str, u64, u32, &str)> = vec![
        ("node_text", node_hash, 1, node_embedding_text),
        ("event_text", event_id_hash, 3, request.body.as_str()),
    ];
    // Compute embeddings, trying the fallback provider on error. On total failure
    // the behavior depends on `context_embed_defer_on_failure()`: the default is to
    // DEFER (persist the node/event/summaries without vectors and mark the node
    // embedding-dirty so the async drainer attaches vectors later, keeping it
    // lexically rankable meanwhile); the fail-closed path aborts the extract.
    let embed_outcome = match context_embeddings_for_extract(&provider, &embedding_inputs) {
        Ok(value) => Ok(value),
        Err(status) => {
            if let Some(fallback) = provider.fallback_provider.as_deref() {
                let fallback = normalize_provider(fallback.clone());
                context_embeddings_for_extract(&fallback, &embedding_inputs).map(
                    |(vectors, mut report)| {
                        report.fallback_used = true;
                        report.provider_name = format!(
                            "{}+fallback:{}",
                            provider.provider_name, report.provider_name
                        );
                        (vectors, report)
                    },
                )
            } else {
                Err(status)
            }
        }
    };
    let (embedding_vectors, embedding_generation, embedding_deferred) = match embed_outcome {
        Ok((vectors, report)) => (vectors, report, false),
        Err(status) => {
            if !context_embed_defer_on_failure() {
                return empty_extract_report(
                    status,
                    provider,
                    request.tenant_hash,
                    request.timestamp_ms,
                );
            }
            (Vec::new(), ContextEmbeddingGenerationReport::default(), true)
        }
    };

    // The vectors live on their owners and nowhere else: the summaries, the event and the node
    // each carry their own. The separate ContextEmbedding rows this used to also write were
    // addressed by a one-way hash of (tenant, owner, level) -- nothing holding one could ever
    // find its owner again -- and every reader now asks the owner, so the rows are retired.
    //
    // Left empty when embedding was deferred (provider failure): the node is marked
    // embedding-dirty and the async drainer attaches vectors later, so an empty vector here
    // means "not yet", never "none". skip_serializing_if keeps that costing nothing on disk.
    if !embedding_deferred {
        // embedding_inputs order is node text then event text, so the vectors line up with their
        // owners: index 0 is whatever the node was embedded from -- taken by the node, the L0
        // summary, and the L1 summary when one is emitted, because one string produced it -- and
        // index 1 is the event.
        //
        // One encoder produced all of them, so its identity is computed once and stamped on
        // every owner that takes a vector. The summaries need it as much as the node does: the
        // retrieve pass scores an L1 summary vector too, and an unstamped one cannot be told
        // apart from one the encoder in use wrote.
        let embedding_model_hash = context_embedding_model_hash(&provider.embedding_model);
        if let Some(vector) = embedding_vectors.first() {
            // The level-1 summary carries its own vector because that is the only place a
            // summary's vector lives -- the embedding fold moved vectors off separate rows and
            // onto the records that own them. Retrieval does not read it yet (it takes L0 from
            // node.vector and only ever queries level 2 for summary vectors), and dropping the
            // write on that basis is the exact mistake
            // context_extract_stores_embedding_vectors_on_the_records_themselves exists to catch.
            summary_l0.vector = vector.clone();
            summary_l0.embedding_model_hash = embedding_model_hash;
            // The node itself carries its L0 vector too: the traversal scores children from
            // node.vector first, and without this the happy path would leave it empty on every
            // fresh ingest -- only the drainer's deferred path would ever fill it, so the
            // fallback to the separate record could never be retired.
            node.vector = vector.clone();
            node.embedding_model_hash = embedding_model_hash;
            node.embedding_updated_at_ms = timestamp_ms;
        }
        if let (Some(summary), Some(vector)) = (summary_l1.as_mut(), embedding_vectors.first()) {
            // Index 0, not a slot of its own: this summary's text IS `l1`, and `l1` is what the
            // node was embedded from, so the vector it wants is the one already returned.
            summary.vector = vector.clone();
            // The one summary vector retrieval actually scores: level 2 is the only level the
            // retrieve pass queries for vectors, so this stamp is what the guard there reads.
            summary.embedding_model_hash = embedding_model_hash;
        }
        // The event is at index 1 either way, because the node now contributes exactly one text
        // to the batch whether or not L1 was emitted.
        if let Some(vector) = embedding_vectors.get(1) {
            event.vector = vector.clone();
        }
    }

    let mut commands = vec![
        Command::ContextUpsertNode {
            tenant_hash: request.tenant_hash,
            node: node.clone(),
        },
        Command::ContextWriteEvent {
            tenant_hash: request.tenant_hash,
            node_hash,
            event: event.clone(),
            first_write_only: false,
            cold_storage: false,
        },
        Command::ContextMarkSummaryDirty {
            tenant_hash: request.tenant_hash,
            node_hash: dirty_marker.node_hash,
            event_time_ms: dirty_marker.last_event_time_ms,
            reason: dirty_marker.reason,
            propagate_depth: dirty_marker.propagate_depth,
        },
        Command::ContextUpsertSummary {
            tenant_hash: request.tenant_hash,
            summary: summary_l0,
        },
    ];
    // L1 summary is written only when L1 was emitted (see `emit_l1`).
    if let Some(summary_l1) = summary_l1 {
        commands.push(Command::ContextUpsertSummary {
            tenant_hash: request.tenant_hash,
            summary: summary_l1,
        });
    }
    if context_secondary_index_enabled() {
        commands.push(Command::ContextWriteIndexRef {
            tenant_hash: request.tenant_hash,
            index_name: "source".to_string(),
            index_value_hash: stable_hash64(&request.source_id),
            scope_hash: 0,
            event_time_ms: timestamp_ms,
            index_ref: index_ref.clone(),
        });
    }
    if embedding_deferred {
        // Live-path embed failed: persist the node without vectors and mark it
        // embedding-dirty so the async drainer retries. Happy path marks nothing.
        commands.push(Command::ContextMarkEmbeddingDirty {
            tenant_hash: request.tenant_hash,
            node_hash,
                event_time_ms: timestamp_ms,
                reason: EMBEDDING_DIRTY_REASON_LIVE_FAILURE,
                propagate_depth: 0,
            clear: false,
        });
    }
    // Embedding upserts (empty when deferred).
    for command in commands {
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

pub fn retrieve_context(
    engine: &TemporalEngine,
    request: ContextRetrieveRequest,
) -> ContextRetrieveReport {
    let trace_retrieve = matches!(
        std::env::var("MATRIXARK_CONTEXT_RETRIEVE_TRACE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    );
    let trace_started = Instant::now();
    let trace_stage = |stage: &str| {
        if trace_retrieve {
            eprintln!(
                "context_retrieve_stage={stage} elapsed_seconds={:.6}",
                trace_started.elapsed().as_secs_f64()
            );
        }
    };
    trace_stage("start");
    let mut blocks = Vec::new();
    let mut node_count = 0usize;
    let mut event_count = 0usize;
    let query_plan = context_query_plan(&request.query);
    let mut query_understanding_debug =
        context_query_understanding_debug_for_plan(&request, &query_plan);
    let mut fanout_plan = ContextFanoutPlanReport {
        strategy: "hierarchical_summary_secondary_index_node_resource_colocation".to_string(),
        current_agent_default_boost: if request.prefer_current_agent {
            125
        } else {
            100
        },
        peer_agent_default_boost: 100,
        prefer_current_agent_configured: request.prefer_current_agent,
        peer_agent_demoted_by_default: false,
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
    trace_stage("query_embedding");
    // The encoder that produced the query vector, read from the RAW request rather than the
    // normalized provider: normalisation substitutes a mock sentinel for an absent
    // embedding_model, and hashing that would conflict with everything a real ingest wrote,
    // skipping every stored vector for any caller that carries no provider config. An unnamed
    // encoder is unknown, and unknown never conflicts.
    let active_embedding_model_hash =
        context_embedding_model_hash(request.provider.embedding_model.trim());
    let mut summary_scores_by_node = BTreeMap::<u64, (i64, usize)>::new();
    // node_l0 comes from the node records themselves: the vector lives on the node, which is
    // addressable by the hash already in hand -- no one-way ref hash to reconstruct, and no
    // separate rows left to fall back to. A node carrying no vector is simply un-embedded and
    // is handed to the hybrid lexical pass below, exactly like before it was embedded. Chunked
    // because a single oversized command is rejected outright, not truncated, and an unscored
    // node silently collapses to the lexical/recency fallback.
    let mut l0_row_fallback: Vec<u64> = Vec::new();
    let mut width_conflict_nodes = 0usize;
    let mut model_conflict_nodes = 0usize;
    for chunk in node_hashes.chunks(CONTEXT_EMBEDDING_QUERY_CHUNK) {
        if chunk.is_empty() {
            continue;
        }
        let response = engine.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: Command::ContextGetNodes {
                tenant_hash: request.tenant_hash,
                node_hashes: chunk.to_vec(),
            },
        });
        let mut returned = BTreeSet::new();
        if let CommandResponse::ContextNodes { nodes } = response.response {
            for node in nodes {
                returned.insert(node.node_hash);
                if node.vector.is_empty() {
                    l0_row_fallback.push(node.node_hash);
                } else if context_embedding_width_conflicts(&query_embedding, &node.vector) {
                    width_conflict_nodes = width_conflict_nodes.saturating_add(1);
                    l0_row_fallback.push(node.node_hash);
                } else if context_embedding_model_conflicts(
                    node.embedding_model_hash,
                    active_embedding_model_hash,
                ) {
                    // Same width, different encoder: no length mismatch and no error, so this
                    // branch is the only thing standing between a model swap and a plausible
                    // cosine computed across two vector spaces. Hand it to the lexical pass.
                    //
                    // Handed over exactly as an un-embedded node is, which means NOT recording a
                    // score: the lexical pass selects on the score COUNT being zero, not on the
                    // score value, so a zero would mark the node scored and strand it at the
                    // bottom of the ranking with no second chance.
                    model_conflict_nodes = model_conflict_nodes.saturating_add(1);
                    l0_row_fallback.push(node.node_hash);
                } else {
                    let score =
                        context_embedding_similarity_micros(&query_embedding, &node.vector);
                    let entry = summary_scores_by_node.entry(node.node_hash).or_default();
                    entry.0 = entry.0.max(score);
                    entry.1 = entry.1.saturating_add(1);
                }
            }
        }
        // A node the engine did not return cannot carry a vector either.
        for node_hash in chunk {
            if !returned.contains(node_hash) {
                l0_row_fallback.push(*node_hash);
            }
        }
    }
    // Diagnostic only now: candidates whose node carries no vector at all (un-embedded).
    fanout_plan.l0_row_fallback_nodes = l0_row_fallback.len();
    // node_l1 comes from the L1 summaries' own vectors -- the ingest has filled them since the
    // fold, and the summary is addressable by the node hash already in hand. Level 2 here is
    // the summary-record level for L1 (ContextQuerySummaries uses 1 = L0, 2 = L1).
    for chunk in node_hashes.chunks(CONTEXT_EMBEDDING_QUERY_CHUNK) {
        if chunk.is_empty() {
            continue;
        }
        let response = engine.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: Command::ContextQuerySummaryVectors {
                tenant_hash: request.tenant_hash,
                node_hashes: chunk.to_vec(),
                level: 2,
                as_of_ms: request.end_time_ms.max(1),
            },
        });
        if let CommandResponse::ContextSummaryVectors { vectors } = response.response {
            for entry in vectors {
                if context_embedding_width_conflicts(&query_embedding, &entry.vector) {
                    // Same reasoning as the node pass: skip entirely rather than record a zero,
                    // so the count stays at zero and the lexical pass still owns this node.
                    width_conflict_nodes = width_conflict_nodes.saturating_add(1);
                    continue;
                }
                if context_embedding_model_conflicts(
                    entry.embedding_model_hash,
                    active_embedding_model_hash,
                ) {
                    // The node's own vector is declined above when its encoder was replaced,
                    // which removed only ONE of this node's two routes into the ranking. Both
                    // passes fill THIS map, so scoring the summary here would rank the node on a
                    // cosine taken across two vector spaces AND mark it scored -- withdrawing the
                    // lexical fallback the other guard deliberately handed it to. Skip without
                    // recording, exactly as there.
                    model_conflict_nodes = model_conflict_nodes.saturating_add(1);
                    continue;
                }
                let score =
                    context_embedding_similarity_micros(&query_embedding, &entry.vector);
                let scores = summary_scores_by_node.entry(entry.node_hash).or_default();
                scores.0 = scores.0.max(score);
                scores.1 = scores.1.saturating_add(1);
            }
        }
    }
    // Set after BOTH vector passes -- the node pass and the summary pass each contribute, and the
    // node pass alone would under-report.
    fanout_plan.embedding_width_conflict_nodes = width_conflict_nodes;
    fanout_plan.embedding_model_conflict_nodes = model_conflict_nodes;
    trace_stage("summary_embedding_lookup");
    // Hybrid lexical fallback: nodes with NO stored summary embedding used to score
    // a flat 0 here (missing-embedding -> unwrap_or_default), so a freshly
    // bulk-loaded (un-embedded) store collapsed to recency order and lost focused
    // recall. Instead, load those un-embedded candidates and score them by
    // query/text lexical overlap, normalized into the same micros scale as cosine.
    // Embedded nodes keep their cosine score untouched, so when every node is
    // embedded this pass does nothing and behavior is identical to before.
    let mut prefetched_nodes = BTreeMap::<u64, ContextNode>::new();
    let mut lexical_scores_by_node = BTreeMap::<u64, i64>::new();
    if context_hybrid_lexical_enabled() {
        let unembedded: Vec<u64> = node_hashes
            .iter()
            .copied()
            .filter(|node_hash| {
                summary_scores_by_node
                    .get(node_hash)
                    .map(|(_, found)| *found == 0)
                    .unwrap_or(true)
            })
            .collect();
        if !unembedded.is_empty() {
            for chunk in unembedded.chunks(CONTEXT_EMBEDDING_QUERY_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }
                let nodes_response = engine.execute(ExecuteRequest {
                    shard_id: request.shard_id,
                    command: Command::ContextGetNodes {
                        tenant_hash: request.tenant_hash,
                        node_hashes: chunk.to_vec(),
                    },
                });
                if let CommandResponse::ContextNodes { nodes } = nodes_response.response {
                    for node in nodes {
                        let text = format!("{} {} {}", node.canonical_name, node.l0, node.l1_ref);
                        let lexical = context_relevance_score_plan(&query_plan, &text);
                        lexical_scores_by_node
                            .insert(node.node_hash, lexical_score_to_micros(lexical));
                        prefetched_nodes.insert(node.node_hash, node);
                    }
                }
            }
        }
    }
    trace_stage("lexical_fallback");
    let mut summary_scores = node_hashes
        .iter()
        .map(|node_hash| {
            let (best_score, found) = summary_scores_by_node
                .get(node_hash)
                .copied()
                .unwrap_or_default();
            // Embedded node (found > 0): cosine score, exactly as before. Otherwise
            // fall back to the hybrid lexical score (0 when there was no overlap or
            // hybrid is disabled) so un-embedded nodes are still rankable.
            let best_score = if found > 0 {
                best_score
            } else {
                lexical_scores_by_node
                    .get(node_hash)
                    .copied()
                    .unwrap_or(best_score)
            };
            (*node_hash, best_score, found, 0u32, 0u64)
        })
        .collect::<Vec<_>>();
    summary_scores.sort_by_key(|(node_hash, score, found, summary_score, freshness_ms)| {
        (
            Reverse(*score),
            Reverse(*found),
            Reverse(*summary_score),
            Reverse(*freshness_ms),
            *node_hash,
        )
    });
    trace_stage("summary_score_sort");
    let summary_node_limit = request
        .max_summary_nodes
        .max(1)
        .min(summary_scores.len().max(1));
    let query_aware_event_limit = request.max_events.max(1);
    let event_node_limit = request
        .max_event_nodes
        .max(1)
        .min(summary_node_limit.max(1))
        .min(query_aware_event_limit);
    let rerank_node_limit = summary_node_limit.min(summary_scores.len());
    let rerank_node_hashes = summary_scores
        .iter()
        .take(rerank_node_limit)
        .map(|(node_hash, _, _, _, _)| *node_hash)
        .collect::<Vec<_>>();
    let rerank_nodes = if rerank_node_hashes.is_empty() {
        Vec::new()
    } else {
        let nodes_response = engine.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: Command::ContextGetNodes {
                tenant_hash: request.tenant_hash,
                node_hashes: rerank_node_hashes,
            },
        });
        match nodes_response.response {
            CommandResponse::ContextNodes { nodes } => nodes,
            _ => Vec::new(),
        }
    };
    trace_stage("rerank_node_load");
    for node in rerank_nodes {
        let node_hash = node.node_hash;
        if let Some(score) = summary_scores
            .iter_mut()
            .find(|(candidate_hash, _, _, _, _)| *candidate_hash == node_hash)
        {
            let summary_text = format!("{} {}", node.l0, node.l1_ref);
            let summary_score = context_relevance_score_plan(&query_plan, &summary_text);
            let freshness_ms = node.last_event_time_ms;
            let agent_scope_boost = if request.prefer_current_agent
                && context_record_scope_matches(
                    &node.raw_metadata_ref,
                    &request.current_agent_scope_key,
                ) {
                125_000i64
            } else {
                0i64
            };
            score.1 = score
                .1
                .saturating_add((summary_score as i64).saturating_mul(1_000))
                .saturating_add(agent_scope_boost);
            score.3 = summary_score;
            score.4 = freshness_ms;
        }
        prefetched_nodes.insert(node_hash, node);
    }
    summary_scores.sort_by_key(|(node_hash, score, found, summary_score, freshness_ms)| {
        (
            Reverse(*score),
            Reverse(*found),
            Reverse(*summary_score),
            Reverse(*freshness_ms),
            *node_hash,
        )
    });
    trace_stage("rerank_sort");
    node_hashes = summary_scores
        .iter()
        .take(summary_node_limit)
        .map(|(node_hash, _, _, _, _)| *node_hash)
        .collect();
    let event_node_hashes = node_hashes
        .iter()
        .copied()
        .take(event_node_limit)
        .collect::<Vec<_>>();
    let event_pack_budget = request.max_events.max(1);
    let event_overfetch = std::env::var("MATRIXARK_CONTEXT_EVENT_QUERY_OVERFETCH")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2);
    let skipped_node_hashes = summary_scores
        .iter()
        .skip(event_node_limit)
        .map(|(node_hash, _, _, _, _)| *node_hash)
        .collect::<Vec<_>>();
    fanout_plan.summary_candidate_nodes = summary_scores.len();
    fanout_plan.summary_selected_nodes = node_hashes.len();
    fanout_plan.event_expanded_nodes = event_node_hashes.len();
    fanout_plan.skipped_node_count = skipped_node_hashes.len();
    fanout_plan.summary_lookup_batches = usize::from(!summary_scores.is_empty());
    fanout_plan.event_query_budget = if include_l2 { event_pack_budget } else { 0 };
    fanout_plan.event_query_node_count = event_node_hashes.len();
    fanout_plan.selected_node_hashes = node_hashes.clone();
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
        .filter(|(_, _, found, _, _)| *found > 0)
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
        .map(|(node_hash, score, found, summary_score, freshness_ms)| {
            format!(
                "node:{node_hash}:score:{score}:refs:{found}:summary_score:{summary_score}:last_event:{freshness_ms}"
            )
        })
        .collect();

    let event_node_set = event_node_hashes.iter().copied().collect::<BTreeSet<_>>();
    let mut event_query_returned_count = 0usize;
    let mut event_node_index = 0usize;
    for node_hash in node_hashes.iter().copied() {
        let mut node_source_ref = String::new();
        let cached_node = prefetched_nodes.remove(&node_hash).or_else(|| {
            let node_response = engine.execute(ExecuteRequest {
                shard_id: request.shard_id,
                command: Command::ContextGetNode {
                    tenant_hash: request.tenant_hash,
                    node_hash,
                },
            });
            match node_response.response {
                CommandResponse::ContextNode {
                    node: Some(node), ..
                } => Some(node),
                _ => None,
            }
        });
        if let Some(node) = cached_node {
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

        if include_l2 && event_count < event_pack_budget && event_node_set.contains(&node_hash) {
            let remaining_budget = event_pack_budget.saturating_sub(event_count).max(1);
            let remaining_nodes = event_node_hashes
                .len()
                .saturating_sub(event_node_index)
                .max(1);
            event_node_index = event_node_index.saturating_add(1);
            let per_node_event_limit = remaining_budget
                .div_ceil(remaining_nodes)
                .max(1)
                .saturating_mul(event_overfetch)
                .min(event_pack_budget);
            let event_scan_cap = std::env::var("MATRIXARK_CONTEXT_EVENT_SCAN_CAP")
                .ok()
                .and_then(|value| value.trim().parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(64);
            let events_response = engine.execute(ExecuteRequest {
                shard_id: request.shard_id,
                command: Command::ContextQueryEvents {
                    tenant_hash: request.tenant_hash,
                    node_hash,
                    start_time_ms: request.start_time_ms,
                    end_time_ms: request.end_time_ms,
                    limit: Some(per_node_event_limit),
                    max_scan: Some(event_scan_cap),
                    current_valid_only: false,
                    as_of_ms: 0,
                    kinds: Vec::new(),
                    statuses: Vec::new(),
                    min_confidence: request.min_confidence,
                    min_importance: request.min_importance,
                },
            });
            if let CommandResponse::ContextEvents { events, .. } = events_response.response {
                event_query_returned_count =
                    event_query_returned_count.saturating_add(events.len());
                for event in events {
                    if event_count >= event_pack_budget {
                        break;
                    }
                    let passes_prefilter = context_query_matches_plan(&query_plan, &event.text);
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
    fanout_plan.event_query_returned_count = event_query_returned_count;
    trace_stage("event_expansion");

    blocks.sort_by_cached_key(|block| {
        (
            Reverse(context_relevance_score_plan(&query_plan, &block.text)),
            tier_rank(block.tier),
            Reverse(block.event_time_ms),
            block.uri.clone(),
        )
    });
    trace_stage("block_sort");
    dedupe_context_blocks_by_source_ref(&mut blocks);
    trace_stage("block_dedupe");
    context_query_debug_finalize(
        &mut query_understanding_debug,
        &query_plan,
        &blocks,
        node_count,
        tiers.as_slice(),
    );
    trace_stage("debug_finalize");
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

fn dedupe_context_blocks_by_source_ref(blocks: &mut Vec<ContextBlock>) {
    let mut seen = BTreeMap::<String, usize>::new();
    let mut deduped = Vec::<ContextBlock>::with_capacity(blocks.len());
    for block in blocks.drain(..) {
        let key = if block.source_ref.trim().is_empty() {
            format!("uri:{}", block.uri)
        } else {
            format!(
                "source:{}:{}",
                block.node_hash,
                block.source_ref.trim().to_ascii_lowercase()
            )
        };
        if let Some(existing_index) = seen.get(&key).copied() {
            if should_replace_context_block_duplicate(&deduped[existing_index], &block) {
                deduped[existing_index] = block;
            }
        } else {
            seen.insert(key, deduped.len());
            deduped.push(block);
        }
    }
    *blocks = deduped;
}

fn should_replace_context_block_duplicate(
    current: &ContextBlock,
    candidate: &ContextBlock,
) -> bool {
    let current_detail_rank = tier_rank(current.tier);
    let candidate_detail_rank = tier_rank(candidate.tier);
    candidate_detail_rank > current_detail_rank
        || (candidate_detail_rank == current_detail_rank
            && candidate.text.len() > current.text.len())
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

fn context_sentences(body: &str) -> Vec<&str> {
    body.split(['.', '\n', '!', '?'])
        .map(str::trim)
        .filter(|sentence| !sentence.is_empty())
        .collect()
}

/// Whether a source carries enough to justify the (more expensive) richer L1
/// synthesis; thin sources skip it and stay cheap. Thresholds mirror the
/// internal `node_l1_generation_policy` (>=3 events / >=180 tokens); at
/// extract time sentences stand in for events.
pub(crate) fn context_source_warrants_l1(body: &str) -> bool {
    context_sentences(body).len() >= 3 || body.split_whitespace().count() >= 180
}

/// L0: the short routing/preview summary (the required
/// traversal summary). Title plus the single leading sentence, kept short --
/// just enough to route and recall. L1 is a strict superset that carries more
/// content; raw event evidence lives in the separate L2 tier.
fn summarize_l0(title: &str, body: &str) -> String {
    let lead = context_sentences(body)
        .into_iter()
        .next()
        .unwrap_or_else(|| body.trim());
    truncate_words(&format!("{title}: {lead}"), 18)
}

/// Rank a sentence by how much concrete detail it carries: figures, proper
/// nouns, and temporal/correction markers are what queries actually target.
fn context_fact_score(sentence: &str) -> i32 {
    let lower = sentence.to_ascii_lowercase();
    let mut score = 0;
    if sentence.chars().any(|ch| ch.is_ascii_digit()) {
        score += 3;
    }
    score += sentence
        .split_whitespace()
        .skip(1)
        .filter(|word| word.chars().next().is_some_and(|ch| ch.is_ascii_uppercase()))
        .count() as i32;
    for marker in [
        "now",
        "changed",
        "updated",
        "corrected",
        "correction",
        "instead",
        "no longer",
        "moved",
        "switched",
        "rescheduled",
        "deadline",
        "because",
        "after",
        "before",
    ] {
        if lower.contains(marker) {
            score += 2;
        }
    }
    score
}

/// L1: the richer summary that "carries more content for broader traversal".
/// It is a strict **superset** of L0 -- the same leading
/// sentence L0 previews, followed by the most information-dense remaining
/// sentences ranked by `context_fact_score`. So L0 and L1 never merely reformat
/// the same facts: L1 always adds detail on top of L0. Raw event evidence stays
/// in the separate L2 tier.
fn summarize_l1(kind: ContextSourceKind, title: &str, body: &str) -> String {
    let sentences = context_sentences(body);
    let lead = sentences.first().copied();
    let mut ranked = sentences
        .iter()
        .enumerate()
        .skip(1)
        .map(|(index, sentence)| (context_fact_score(sentence), index, *sentence))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.0.cmp(&left.0).then(left.1.cmp(&right.1)));
    let mut facts = Vec::new();
    if let Some(lead) = lead {
        facts.push(truncate_words(lead, 32));
    }
    for (_, _, sentence) in ranked.into_iter().take(7) {
        facts.push(truncate_words(sentence, 32));
    }
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
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
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
        vector: Vec::new(),
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
        dirty_marker: ContextDirtyNode {
            node_hash: 0,
            first_event_time_ms: timestamp_ms,
            last_event_time_ms: timestamp_ms,
            reason: 0,
            propagate_depth: 0,
            mark_count: 0,
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
    std::env::var("MATRIXARK_CONTEXT_EVENT_FANOUT_NODES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
}

fn default_current_agent_scope_key() -> String {
    "agent:codex".to_string()
}

fn context_record_scope_matches(source_ref: &str, scope_key: &str) -> bool {
    !scope_key.trim().is_empty() && source_ref.contains(scope_key)
}

fn default_context_skill_enabled() -> bool {
    true
}

fn default_skill_selection_limit() -> usize {
    8
}

fn default_benchmark_profile() -> String {
    "reference_local_synthetic".to_string()
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
            profile: "reference_sweep_small".to_string(),
            source_count: 16,
            query_count: 2,
            max_events: 6,
        },
        ContextPipelineBenchmarkSweepProfile {
            profile: "reference_sweep_medium".to_string(),
            source_count: 32,
            query_count: 4,
            max_events: 8,
        },
        ContextPipelineBenchmarkSweepProfile {
            profile: "reference_sweep_large".to_string(),
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
    // "Applied" is about whether this text went through the filter, not about whether the
    // filter found anything. Answering `sanitized_text != text` meant a record with no personal
    // data in it -- the ordinary case, and the great majority of them -- reported that filtering
    // had not been applied, so anyone auditing whether it runs read "no" nearly every time.
    let pii_filtering_applied = policy.pii_filtering_enabled;
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
