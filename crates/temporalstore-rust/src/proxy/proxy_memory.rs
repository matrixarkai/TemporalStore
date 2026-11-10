// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Memory-client HTTP compatibility built on TemporalStore context primitives.

use std::collections::{BTreeSet, HashSet};

use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::*;
use crate::sdk::v1::{LlmMemoryHistoryRecord, LlmMemoryRecord};
use crate::types::CommandResponse;

const MEMORY_BATCH_LIMIT: usize = 1_000;

#[derive(Debug, Clone, Default, Deserialize)]
struct MemoryIdentity {
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    agent_id: String,
    #[serde(default, alias = "session_id")]
    run_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MemoryScope {
    #[serde(default)]
    tenant_id: String,
    #[serde(default)]
    account_id: String,
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    agent_id: String,
    #[serde(default, alias = "session_id")]
    run_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum MemoryMessages {
    Text(String),
    Many(Vec<Value>),
}

#[derive(Debug, Clone, Deserialize)]
struct MemoryAddRequest {
    messages: MemoryMessages,
    #[serde(default)]
    scope: MemoryScope,
    #[serde(flatten)]
    identity: MemoryIdentity,
    #[serde(default)]
    metadata: Value,
    #[serde(default)]
    provider: Option<Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MemoryQueryRequest {
    #[serde(default)]
    query: String,
    #[serde(default)]
    scope: MemoryScope,
    #[serde(flatten)]
    identity: MemoryIdentity,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    provider: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct MemoryUpdateRequest {
    #[serde(default, alias = "text")]
    data: String,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct MemoryFeedbackRequest {
    feedback: String,
    #[serde(default)]
    feedback_reason: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MemoryBatchRequest {
    #[serde(default)]
    memories: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MemoryRecord {
    id: String,
    memory: String,
    metadata: Value,
    #[serde(default)]
    tenant_id: String,
    #[serde(default)]
    account_id: String,
    user_id: String,
    agent_id: String,
    run_id: String,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MemoryHistoryEntry {
    id: String,
    event: String,
    at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    feedback: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    feedback_reason: Option<String>,
}

impl MemoryScope {
    fn identity(&self, top: &MemoryIdentity) -> MemoryIdentity {
        MemoryIdentity {
            user_id: non_empty(&top.user_id, &self.user_id).to_string(),
            agent_id: non_empty(&top.agent_id, &self.agent_id).to_string(),
            run_id: non_empty(&top.run_id, &self.run_id).to_string(),
        }
    }

    fn context_scope(&self, identity: &MemoryIdentity) -> context::ProxyContextScope {
        context::ProxyContextScope {
            tenant_id: self.tenant_id.clone(),
            account_id: self.account_id.clone(),
            user_id: identity.user_id.clone(),
            session_id: identity.run_id.clone(),
        }
    }
}

impl MemoryRecord {
    fn matches(&self, identity: &MemoryIdentity) -> bool {
        matches_dimension(&identity.user_id, &self.user_id)
            && matches_dimension(&identity.agent_id, &self.agent_id)
            && matches_dimension(&identity.run_id, &self.run_id)
    }

    fn encode(&self) -> Vec<u8> {
        LlmMemoryRecord {
            id: self.id.clone(),
            memory: self.memory.clone(),
            metadata_msgpack: rmp_serde::to_vec_named(&self.metadata).unwrap_or_default(),
            tenant_id: self.tenant_id.clone(),
            account_id: self.account_id.clone(),
            user_id: self.user_id.clone(),
            agent_id: self.agent_id.clone(),
            run_id: self.run_id.clone(),
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
        }
        .encode_to_vec()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let record = LlmMemoryRecord::decode(bytes).ok()?;
        let metadata = if record.metadata_msgpack.is_empty() {
            Value::Null
        } else {
            rmp_serde::from_slice(&record.metadata_msgpack).ok()?
        };
        Some(Self {
            id: record.id,
            memory: record.memory,
            metadata,
            tenant_id: record.tenant_id,
            account_id: record.account_id,
            user_id: record.user_id,
            agent_id: record.agent_id,
            run_id: record.run_id,
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
        })
    }
}

impl MemoryHistoryEntry {
    fn encode(&self) -> Vec<u8> {
        LlmMemoryHistoryRecord {
            id: self.id.clone(),
            event: self.event.clone(),
            at_ms: self.at_ms,
            memory: self.memory.clone(),
            metadata_msgpack: self
                .metadata
                .as_ref()
                .and_then(|value| rmp_serde::to_vec_named(value).ok())
                .unwrap_or_default(),
            feedback: self.feedback.clone(),
            feedback_reason: self.feedback_reason.clone(),
        }
        .encode_to_vec()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let record = LlmMemoryHistoryRecord::decode(bytes).ok()?;
        let metadata = if record.metadata_msgpack.is_empty() {
            None
        } else {
            Some(rmp_serde::from_slice(&record.metadata_msgpack).ok()?)
        };
        Some(Self {
            id: record.id,
            event: record.event,
            at_ms: record.at_ms,
            memory: record.memory,
            metadata,
            feedback: record.feedback,
            feedback_reason: record.feedback_reason,
        })
    }
}

fn non_empty<'a>(preferred: &'a str, fallback: &'a str) -> &'a str {
    if preferred.is_empty() {
        fallback
    } else {
        preferred
    }
}

fn matches_dimension(filter: &str, value: &str) -> bool {
    filter.is_empty() || filter == value
}

fn memory_catalog_key(tenant_hash: u64) -> String {
    format!("context:memory:{tenant_hash:016x}")
}

fn memory_history_key(tenant_hash: u64, memory_id: &str) -> String {
    format!("context:memory-history:{tenant_hash:016x}:{memory_id}")
}

fn memory_id(tenant_hash: u64, now: u64, sequence: u64, text: &str) -> String {
    let suffix = context::stable_scope_hash(&format!("{now}:{sequence}:{text}"));
    format!("{tenant_hash:016x}-{suffix:016x}")
}

fn tenant_hash_from_memory_id(memory_id: &str) -> Option<u64> {
    let prefix = memory_id.split_once('-')?.0;
    (prefix.len() == 16)
        .then(|| u64::from_str_radix(prefix, 16).ok())
        .flatten()
}

fn normalized_messages(messages: MemoryMessages) -> Result<Vec<(String, String)>, String> {
    match messages {
        MemoryMessages::Text(text) => {
            let text = text.trim().to_string();
            if text.is_empty() {
                Err("messages must contain non-empty text".to_string())
            } else {
                Ok(vec![("user".to_string(), text)])
            }
        }
        MemoryMessages::Many(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Value::String(text) if !text.trim().is_empty() => {
                        out.push(("user".to_string(), text.trim().to_string()));
                    }
                    Value::Object(map) => {
                        let role = map.get("role").and_then(Value::as_str).unwrap_or("user");
                        let content = map
                            .get("content")
                            .or_else(|| map.get("text"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .trim();
                        if !content.is_empty() {
                            out.push((role.to_string(), content.to_string()));
                        }
                    }
                    _ => {}
                }
            }
            if out.is_empty() {
                Err("messages must contain non-empty text".to_string())
            } else {
                Ok(out)
            }
        }
    }
}

fn history_field(now: u64, sequence: u64) -> String {
    format!("{now:020}:{sequence:020}")
}

impl ProxyService {
    pub(super) fn handle_memory_route(&self, request: &HttpRequest) -> Option<(u16, Vec<u8>)> {
        use crate::http::{json_response, parse_json};

        let static_response = match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/memories") => Some(match parse_json::<MemoryAddRequest>(&request.body) {
                Ok(body) => self.memory_add(body),
                Err(err) => self.memory_bad_request(err.to_string()),
            }),
            ("POST", "/memories/search") => {
                Some(match parse_json::<MemoryQueryRequest>(&request.body) {
                    Ok(body) => self.memory_search(body),
                    Err(err) => self.memory_bad_request(err.to_string()),
                })
            }
            ("POST", "/memories/list") => {
                Some(match parse_json::<MemoryQueryRequest>(&request.body) {
                    Ok(body) => self.memory_list(body),
                    Err(err) => self.memory_bad_request(err.to_string()),
                })
            }
            ("POST", "/memories/users") => {
                Some(match parse_json::<MemoryQueryRequest>(&request.body) {
                    Ok(body) => self.memory_users(body),
                    Err(err) => self.memory_bad_request(err.to_string()),
                })
            }
            ("POST", "/memories/delete_all") => {
                Some(match parse_json::<MemoryQueryRequest>(&request.body) {
                    Ok(body) => self.memory_delete_all(body),
                    Err(err) => self.memory_bad_request(err.to_string()),
                })
            }
            ("POST", "/memories/reset") => {
                Some(match parse_json::<MemoryQueryRequest>(&request.body) {
                    Ok(mut body) => {
                        body.identity = MemoryIdentity::default();
                        body.scope.user_id.clear();
                        body.scope.agent_id.clear();
                        body.scope.run_id.clear();
                        self.memory_delete_all(body)
                    }
                    Err(err) => self.memory_bad_request(err.to_string()),
                })
            }
            ("POST", "/memories/batch/update") => {
                Some(match parse_json::<MemoryBatchRequest>(&request.body) {
                    Ok(body) => self.memory_batch_update(body),
                    Err(err) => self.memory_bad_request(err.to_string()),
                })
            }
            ("POST", "/memories/batch/delete") => {
                Some(match parse_json::<MemoryBatchRequest>(&request.body) {
                    Ok(body) => self.memory_batch_delete(body),
                    Err(err) => self.memory_bad_request(err.to_string()),
                })
            }
            _ => None,
        };
        if static_response.is_some() {
            return static_response;
        }

        let suffix = request.path.strip_prefix("/memories/")?;
        let (memory_id, operation) = suffix
            .rsplit_once('/')
            .filter(|(_, tail)| matches!(*tail, "history" | "feedback"))
            .map_or((suffix, ""), |(id, tail)| (id, tail));
        if memory_id.is_empty() {
            return Some(json_response(
                404,
                &Status::error("not_found", "memory id is missing"),
            ));
        }
        Some(match (request.method.as_str(), operation) {
            ("GET", "") => self.memory_get(memory_id),
            ("PUT", "") | ("POST", "update") => {
                match parse_json::<MemoryUpdateRequest>(&request.body) {
                    Ok(body) => self.memory_update(memory_id, body),
                    Err(err) => self.memory_bad_request(err.to_string()),
                }
            }
            ("DELETE", "") | ("POST", "delete") => self.memory_delete(memory_id),
            ("GET", "history") => self.memory_history(memory_id),
            ("POST", "feedback") => match parse_json::<MemoryFeedbackRequest>(&request.body) {
                Ok(body) => self.memory_feedback(memory_id, body),
                Err(err) => self.memory_bad_request(err.to_string()),
            },
            _ => json_response(404, &Status::error("not_found", "unknown memory route")),
        })
    }

    fn memory_bad_request(&self, message: String) -> (u16, Vec<u8>) {
        self.inc_bad_request();
        crate::http::json_response(400, &Status::error("bad_request", message))
    }

    fn memory_add(&self, request: MemoryAddRequest) -> (u16, Vec<u8>) {
        let messages = match normalized_messages(request.messages) {
            Ok(messages) => messages,
            Err(err) => return self.memory_bad_request(err),
        };
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let shard_id = self.context_shard_id(tenant_hash);
        let now = now_ms().max(1);
        let base_sequence = self
            .inner
            .context_ingest_sequence
            .fetch_add(messages.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let mut records = Vec::with_capacity(messages.len());
        let mut sources = Vec::with_capacity(messages.len());
        for (index, (role, text)) in messages.into_iter().enumerate() {
            let id = memory_id(tenant_hash, now, base_sequence + index as u64, &text);
            sources.push(json!({
                "body": text,
                "shard_id": shard_id,
                "source_id": id,
                "source_kind": "chat",
                "tenant_hash": tenant_hash,
                "timestamp_ms": now + index as u64,
                "title": role,
            }));
            records.push(MemoryRecord {
                id,
                memory: sources
                    .last()
                    .and_then(|source| source.get("body"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                metadata: request.metadata.clone(),
                tenant_id: request.scope.tenant_id.clone(),
                account_id: request.scope.account_id.clone(),
                user_id: identity.user_id.clone(),
                agent_id: identity.agent_id.clone(),
                run_id: identity.run_id.clone(),
                created_at_ms: now,
                updated_at_ms: now,
            });
        }
        let extract = context::ProxyContextIngestRequest {
            scope,
            messages: Vec::new(),
            sources,
            records: Vec::new(),
            query: String::new(),
            start_time_ms: 0,
            end_time_ms: 0,
            max_events: Some(records.len().max(1)),
            provider: request.provider,
        };
        let (status, body) = self.context_extract(extract);
        if status != 200 || !json_status_ok(&body) {
            return (status, body);
        }
        let mut commands = Vec::with_capacity(records.len() * 2);
        for (index, record) in records.iter().enumerate() {
            commands.push(Command::HashSet {
                key: memory_catalog_key(tenant_hash),
                field: record.id.clone(),
                value: record.encode(),
            });
            commands.push(Command::HashSet {
                key: memory_history_key(tenant_hash, &record.id),
                field: history_field(now, base_sequence + index as u64),
                value: MemoryHistoryEntry {
                    id: record.id.clone(),
                    event: "ADD".to_string(),
                    at_ms: now,
                    memory: Some(record.memory.clone()),
                    metadata: Some(record.metadata.clone()),
                    feedback: None,
                    feedback_reason: None,
                }
                .encode(),
            });
        }
        let stored = self.batch_execute(BatchExecuteRequest { shard_id, commands });
        if !stored.status.ok {
            return crate::http::json_response(502, &stored.status);
        }
        crate::http::json_response(
            200,
            &json!({
                "results": records.iter().map(|record| json!({
                    "id": record.id,
                    "memory": record.memory,
                    "event": "ADD",
                })).collect::<Vec<_>>()
            }),
        )
    }

    fn memory_search(&self, request: MemoryQueryRequest) -> (u16, Vec<u8>) {
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let limit = request.limit.unwrap_or(10).clamp(1, 100);
        let retrieval = context::ProxyContextRetrieveRequest {
            scope,
            query: request.query,
            start_time_ms: 0,
            end_time_ms: 0,
            max_events: Some(limit.saturating_mul(4)),
            node_hashes: Vec::new(),
            provider: request.provider,
            include_debug: false,
        };
        let (status, body) = self.context_retrieve(retrieval);
        if status != 200 {
            return (status, body);
        }
        let report: crate::context_workflow::ContextRetrieveReport =
            match serde_json::from_slice(&body) {
                Ok(report) => report,
                Err(err) => {
                    return crate::http::json_response(
                        502,
                        &Status::error("invalid_backend_response", err.to_string()),
                    )
                }
            };
        let blocks = report.blocks;
        let mut ids = Vec::with_capacity(blocks.len());
        let mut seen = HashSet::with_capacity(blocks.len());
        for block in &blocks {
            if !block.source_ref.is_empty() && seen.insert(block.source_ref.clone()) {
                ids.push(block.source_ref.clone());
            }
        }
        let hydrated = self.memory_read_many(tenant_hash, &ids);
        let records = ids
            .into_iter()
            .zip(hydrated)
            .filter_map(|(id, record)| record.map(|record| (id, record)))
            .filter(|(_, record)| record.matches(&identity))
            .collect::<std::collections::HashMap<_, _>>();
        let mut results = Vec::with_capacity(limit.min(blocks.len()));
        for block in blocks {
            if results.len() >= limit {
                break;
            }
            let record = records.get(&block.source_ref);
            if tenant_hash_from_memory_id(&block.source_ref).is_some() && record.is_none() {
                continue;
            }
            let rank = results.len();
            let mut metadata = record
                .map(|record| record.metadata.clone())
                .filter(Value::is_object)
                .unwrap_or_else(|| json!({}));
            if let Some(map) = metadata.as_object_mut() {
                map.insert("record_type".into(), json!(block.record_type));
                map.insert("tier".into(), json!(block.tier));
                map.insert("uri".into(), json!(block.uri));
                map.insert("node_hash".into(), json!(block.node_hash));
                map.insert("event_time_ms".into(), json!(block.event_time_ms));
            }
            results.push(json!({
                "id": record.map(|record| record.id.as_str()).unwrap_or(&block.source_ref),
                "memory": block.text,
                "metadata": metadata,
                "score": 1.0 - (rank as f64 / (limit.max(1) as f64 + 1.0)),
                "created_at": record.map(|record| record.created_at_ms),
                "updated_at": record.map(|record| record.updated_at_ms),
            }));
        }
        crate::http::json_response(200, &json!({"results": results}))
    }

    fn memory_get(&self, memory_id: &str) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        match self.memory_read_one(tenant_hash, memory_id) {
            Some(record) => crate::http::json_response(200, &record),
            None => crate::http::json_response(404, &json!({"found": false, "id": memory_id})),
        }
    }

    fn memory_list(&self, request: MemoryQueryRequest) -> (u16, Vec<u8>) {
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let limit = request.limit.unwrap_or(100).clamp(1, 10_000);
        let mut records = self.memory_read_all(tenant_hash);
        records.retain(|record| record.matches(&identity));
        records.sort_by_key(|record| std::cmp::Reverse(record.updated_at_ms));
        records.truncate(limit);
        let count = records.len();
        crate::http::json_response(200, &json!({"results": records, "memories": count}))
    }

    fn memory_update(&self, memory_id: &str, request: MemoryUpdateRequest) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        let Some(mut record) = self.memory_read_one(tenant_hash, memory_id) else {
            return crate::http::json_response(404, &json!({"updated": false, "id": memory_id}));
        };
        if request.data.trim().is_empty() && request.metadata.is_none() {
            return self.memory_bad_request("update requires data or metadata".to_string());
        }
        let now = now_ms().max(record.updated_at_ms.saturating_add(1));
        if !request.data.trim().is_empty() {
            record.memory = request.data.trim().to_string();
        }
        if let Some(metadata) = request.metadata {
            record.metadata = metadata;
        }
        record.updated_at_ms = now;
        let shard_id = self.context_shard_id(tenant_hash);
        let extract = context::ProxyContextIngestRequest {
            scope: context::ProxyContextScope {
                tenant_id: record.tenant_id.clone(),
                account_id: record.account_id.clone(),
                user_id: record.user_id.clone(),
                session_id: record.run_id.clone(),
            },
            messages: Vec::new(),
            sources: vec![json!({
                "body": &record.memory,
                "shard_id": shard_id,
                "source_id": &record.id,
                "source_kind": "chat",
                "tenant_hash": tenant_hash,
                "timestamp_ms": now,
                "title": "memory",
            })],
            records: Vec::new(),
            query: String::new(),
            start_time_ms: 0,
            end_time_ms: 0,
            max_events: Some(1),
            provider: None,
        };
        let (extract_status, extract_body) = self.context_extract(extract);
        if extract_status != 200 || !json_status_ok(&extract_body) {
            return (extract_status, extract_body);
        }
        let commands = vec![
            Command::HashSet {
                key: memory_catalog_key(tenant_hash),
                field: record.id.clone(),
                value: record.encode(),
            },
            Command::HashSet {
                key: memory_history_key(tenant_hash, memory_id),
                field: history_field(now, 0),
                value: MemoryHistoryEntry {
                    id: record.id.clone(),
                    event: "UPDATE".to_string(),
                    at_ms: now,
                    memory: Some(record.memory.clone()),
                    metadata: Some(record.metadata.clone()),
                    feedback: None,
                    feedback_reason: None,
                }
                .encode(),
            },
        ];
        let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
        if !response.status.ok {
            return crate::http::json_response(502, &response.status);
        }
        crate::http::json_response(200, &json!({"updated": true, "id": memory_id}))
    }

    fn memory_delete(&self, memory_id: &str) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        let existed = self.memory_read_one(tenant_hash, memory_id).is_some();
        if !existed {
            return crate::http::json_response(404, &json!({"deleted": false, "id": memory_id}));
        }
        let now = now_ms().max(1);
        let shard_id = self.context_shard_id(tenant_hash);
        let response = self.batch_execute(BatchExecuteRequest {
            shard_id,
            commands: vec![
                Command::HashDelete {
                    key: memory_catalog_key(tenant_hash),
                    field: memory_id.to_string(),
                },
                Command::HashSet {
                    key: memory_history_key(tenant_hash, memory_id),
                    field: history_field(now, 0),
                    value: MemoryHistoryEntry {
                        id: memory_id.to_string(),
                        event: "DELETE".to_string(),
                        at_ms: now,
                        memory: None,
                        metadata: None,
                        feedback: None,
                        feedback_reason: None,
                    }
                    .encode(),
                },
            ],
        });
        if !response.status.ok {
            return crate::http::json_response(502, &response.status);
        }
        crate::http::json_response(200, &json!({"deleted": true, "id": memory_id}))
    }

    fn memory_history(&self, memory_id: &str) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        let shard_id = self.context_shard_id(tenant_hash);
        let response = self.execute(ExecuteRequest {
            shard_id,
            command: Command::HashGetAll {
                key: memory_history_key(tenant_hash, memory_id),
            },
        });
        let mut entries = match response.response {
            CommandResponse::HashEntries { entries } => entries,
            _ => Vec::new(),
        };
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let history: Vec<MemoryHistoryEntry> = entries
            .into_iter()
            .filter_map(|(_, bytes)| MemoryHistoryEntry::decode(&bytes))
            .collect();
        crate::http::json_response(200, &json!({"results": history}))
    }

    fn memory_feedback(&self, memory_id: &str, request: MemoryFeedbackRequest) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        if self.memory_read_one(tenant_hash, memory_id).is_none() {
            return crate::http::json_response(404, &json!({"recorded": false, "id": memory_id}));
        }
        let feedback = request.feedback.to_ascii_uppercase();
        if !matches!(feedback.as_str(), "POSITIVE" | "NEGATIVE" | "VERY_NEGATIVE") {
            return self.memory_bad_request(
                "feedback must be POSITIVE, NEGATIVE, or VERY_NEGATIVE".to_string(),
            );
        }
        let now = now_ms().max(1);
        let response = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            command: Command::HashSet {
                key: memory_history_key(tenant_hash, memory_id),
                field: history_field(now, 0),
                value: MemoryHistoryEntry {
                    id: memory_id.to_string(),
                    event: "FEEDBACK".to_string(),
                    at_ms: now,
                    memory: None,
                    metadata: None,
                    feedback: Some(feedback),
                    feedback_reason: (!request.feedback_reason.is_empty())
                        .then_some(request.feedback_reason),
                }
                .encode(),
            },
        });
        if !response.status.ok {
            return crate::http::json_response(502, &response.status);
        }
        crate::http::json_response(200, &json!({"recorded": true, "id": memory_id}))
    }

    fn memory_users(&self, request: MemoryQueryRequest) -> (u16, Vec<u8>) {
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let records = self.memory_read_all(tenant_hash);
        let mut users = BTreeSet::new();
        let mut agents = BTreeSet::new();
        let mut runs = BTreeSet::new();
        for record in records {
            if !record.user_id.is_empty() {
                users.insert(record.user_id);
            }
            if !record.agent_id.is_empty() {
                agents.insert(record.agent_id);
            }
            if !record.run_id.is_empty() {
                runs.insert(record.run_id);
            }
        }
        let mut results = Vec::with_capacity(users.len() + agents.len() + runs.len());
        results.extend(
            users
                .into_iter()
                .map(|name| json!({"type": "user", "name": name})),
        );
        results.extend(
            agents
                .into_iter()
                .map(|name| json!({"type": "agent", "name": name})),
        );
        results.extend(
            runs.into_iter()
                .map(|name| json!({"type": "run", "name": name})),
        );
        if let Some(limit) = request.limit {
            results.truncate(limit);
        }
        crate::http::json_response(200, &json!({"results": results}))
    }

    fn memory_delete_all(&self, request: MemoryQueryRequest) -> (u16, Vec<u8>) {
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let records = self.memory_read_all(tenant_hash);
        let ids: Vec<String> = records
            .into_iter()
            .filter(|row| row.matches(&identity))
            .map(|row| row.id)
            .collect();
        let shard_id = self.context_shard_id(tenant_hash);
        if ids.is_empty() {
            return crate::http::json_response(200, &json!({"deleted": 0}));
        }
        let now = now_ms().max(1);
        let mut commands = Vec::with_capacity(ids.len() * 2);
        for (index, id) in ids.iter().enumerate() {
            commands.push(Command::HashDelete {
                key: memory_catalog_key(tenant_hash),
                field: id.clone(),
            });
            commands.push(Command::HashSet {
                key: memory_history_key(tenant_hash, id),
                field: history_field(now, index as u64),
                value: MemoryHistoryEntry {
                    id: id.clone(),
                    event: "DELETE".to_string(),
                    at_ms: now,
                    memory: None,
                    metadata: None,
                    feedback: None,
                    feedback_reason: None,
                }
                .encode(),
            });
        }
        let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
        if !response.status.ok {
            return crate::http::json_response(502, &response.status);
        }
        crate::http::json_response(200, &json!({"deleted": ids.len()}))
    }

    fn memory_batch_update(&self, request: MemoryBatchRequest) -> (u16, Vec<u8>) {
        if request.memories.len() > MEMORY_BATCH_LIMIT {
            return self.memory_bad_request(format!(
                "batch accepts at most {MEMORY_BATCH_LIMIT} memories"
            ));
        }
        let mut results = Vec::with_capacity(request.memories.len());
        let mut failed = Vec::new();
        for value in request.memories {
            let id = value
                .get("memory_id")
                .or_else(|| value.get("id"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let update = MemoryUpdateRequest {
                data: value
                    .get("text")
                    .or_else(|| value.get("data"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                metadata: value.get("metadata").cloned(),
            };
            if id.is_empty() {
                failed.push(json!({"error": "memory_id is required"}));
                continue;
            }
            let (status, body) = self.memory_update(id, update);
            if status == 200 {
                results.push(json!({"memory_id": id}));
            } else {
                failed.push(json!({"memory_id": id, "status": status, "body": String::from_utf8_lossy(&body)}));
            }
        }
        let updated = results.len();
        crate::http::json_response(
            200,
            &json!({"results": results, "updated": updated, "failed": failed}),
        )
    }

    fn memory_batch_delete(&self, request: MemoryBatchRequest) -> (u16, Vec<u8>) {
        if request.memories.len() > MEMORY_BATCH_LIMIT {
            return self.memory_bad_request(format!(
                "batch accepts at most {MEMORY_BATCH_LIMIT} memories"
            ));
        }
        let mut results = Vec::with_capacity(request.memories.len());
        let mut failed = Vec::new();
        for value in request.memories {
            let id = value
                .as_str()
                .or_else(|| {
                    value
                        .get("memory_id")
                        .or_else(|| value.get("id"))
                        .and_then(Value::as_str)
                })
                .unwrap_or_default();
            if id.is_empty() {
                failed.push(json!({"error": "memory_id is required"}));
                continue;
            }
            let (status, body) = self.memory_delete(id);
            if status == 200 {
                results.push(json!({"memory_id": id}));
            } else {
                failed.push(json!({"memory_id": id, "status": status, "body": String::from_utf8_lossy(&body)}));
            }
        }
        let deleted = results.len();
        crate::http::json_response(
            200,
            &json!({"results": results, "deleted": deleted, "failed": failed}),
        )
    }

    fn memory_read_one(&self, tenant_hash: u64, memory_id: &str) -> Option<MemoryRecord> {
        self.memory_read_many(tenant_hash, &[memory_id.to_string()])
            .into_iter()
            .next()
            .flatten()
    }

    fn memory_read_many(&self, tenant_hash: u64, ids: &[String]) -> Vec<Option<MemoryRecord>> {
        if ids.is_empty() {
            return Vec::new();
        }
        let response = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            command: Command::HashMultiGet {
                key: memory_catalog_key(tenant_hash),
                fields: ids.to_vec(),
            },
        });
        match response.response {
            CommandResponse::Values { values } => values
                .into_iter()
                .map(|value| value.and_then(|bytes| MemoryRecord::decode(&bytes)))
                .collect(),
            _ => vec![None; ids.len()],
        }
    }

    fn memory_read_all(&self, tenant_hash: u64) -> Vec<MemoryRecord> {
        let response = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            command: Command::HashGetAll {
                key: memory_catalog_key(tenant_hash),
            },
        });
        match response.response {
            CommandResponse::HashEntries { entries } => entries
                .into_iter()
                .filter_map(|(_, bytes)| MemoryRecord::decode(&bytes))
                .collect(),
            _ => Vec::new(),
        }
    }
}

fn json_status_ok(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("status").cloned())
        .and_then(|status| status.get("ok").cloned())
        .and_then(|ok| ok.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_ids_carry_the_tenant_route() {
        let id = memory_id(42, 100, 7, "remember this");
        assert_eq!(tenant_hash_from_memory_id(&id), Some(42));
        assert_eq!(id.len(), 33);
    }

    #[test]
    fn identity_aliases_map_run_to_session() {
        let scope = MemoryScope {
            tenant_id: "t".into(),
            run_id: "run-a".into(),
            ..Default::default()
        };
        let identity = scope.identity(&MemoryIdentity {
            user_id: "u".into(),
            ..Default::default()
        });
        let context = scope.context_scope(&identity);
        assert_eq!(context.user_id, "u");
        assert_eq!(context.session_id, "run-a");
    }

    #[test]
    fn messages_accept_strings_and_role_content_objects() {
        let messages = normalized_messages(MemoryMessages::Many(vec![
            Value::String("one".into()),
            json!({"role": "assistant", "content": "two"}),
        ]))
        .unwrap();
        assert_eq!(
            messages,
            vec![
                ("user".into(), "one".into()),
                ("assistant".into(), "two".into())
            ]
        );
    }

    #[test]
    fn scoped_filters_do_not_cross_identity_dimensions() {
        let record = MemoryRecord {
            id: "id".into(),
            memory: "m".into(),
            metadata: Value::Null,
            tenant_id: "tenant".into(),
            account_id: "account".into(),
            user_id: "alice".into(),
            agent_id: "codex".into(),
            run_id: "r1".into(),
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        assert!(record.matches(&MemoryIdentity {
            user_id: "alice".into(),
            ..Default::default()
        }));
        assert!(!record.matches(&MemoryIdentity {
            user_id: "bob".into(),
            ..Default::default()
        }));
    }

    #[test]
    fn memory_records_round_trip_as_protobuf_with_typed_metadata() {
        let record = MemoryRecord {
            id: "id".into(),
            memory: "Use bounded candidates".into(),
            metadata: json!({"count": 3, "enabled": true}),
            tenant_id: "tenant".into(),
            account_id: "account".into(),
            user_id: "alice".into(),
            agent_id: "assistant".into(),
            run_id: "run".into(),
            created_at_ms: 10,
            updated_at_ms: 11,
        };
        assert_eq!(MemoryRecord::decode(&record.encode()), Some(record));

        let history = MemoryHistoryEntry {
            id: "id".into(),
            event: "UPDATE".into(),
            at_ms: 12,
            memory: Some("Use exact reranking".into()),
            metadata: Some(json!({"quality": 0.9})),
            feedback: None,
            feedback_reason: None,
        };
        assert_eq!(MemoryHistoryEntry::decode(&history.encode()), Some(history));
    }
}
