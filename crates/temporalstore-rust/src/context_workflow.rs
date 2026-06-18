use std::cmp::Reverse;

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
            "/context/retrieve".to_string(),
            "/context/inject".to_string(),
            "/context/workflow/state".to_string(),
            "/context/model/providers".to_string(),
            "/context/model/provider".to_string(),
        ],
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
                if !request.query.is_empty()
                    && !event
                        .text
                        .to_ascii_lowercase()
                        .contains(&request.query.to_ascii_lowercase())
                {
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
