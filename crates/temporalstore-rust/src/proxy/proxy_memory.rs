// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Memory-client HTTP compatibility built on TemporalStore context primitives.

use std::collections::{BTreeMap, HashMap, HashSet};

use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::*;
use crate::sdk::v1::{LlmMemoryHistoryRecord, LlmMemoryRecord};
use crate::types::CommandResponse;

#[path = "memory_node_lookup.rs"]
mod node_lookup;
use node_lookup::*;

const MEMORY_BATCH_LIMIT: usize = 1_000;
const MEMORY_CATALOG_PAGE_SIZE: usize = 512;
const MEMORY_UPDATE_PAGE_SIZE: usize = 128;
// Postings contain batches of memories, so cap posting fan-out before hydration.
const MEMORY_IDENTITY_POSTINGS_PER_PAGE: usize = 1;
const MEMORY_IDENTITY_CANDIDATE_LIMIT: usize = 4_096;
const MEMORY_IDENTITY_INDEX_VERSION: &[u8] = b"3";
const MEMORY_TOTAL_MEMBER: &[u8] = b"\0total";

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
    #[serde(default)]
    infer: Option<bool>,
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
    #[serde(default)]
    provider: Option<Value>,
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
    #[serde(default)]
    provider: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MemoryRecord {
    id: String,
    #[serde(default)]
    context_ref: String,
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

#[derive(Default)]
struct MemoryRecordPage {
    after: Vec<u8>,
    rows: Vec<(Vec<u8>, MemoryRecord)>,
    candidate_count: usize,
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
            context_ref: self.context_ref.clone(),
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
            context_ref: record.context_ref,
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

fn memory_identity_scan_page(
    response: CommandResponse,
    after: &[u8],
) -> Result<(Vec<u8>, Vec<Vec<u8>>), Status> {
    let CommandResponse::Members { members } = response else {
        return Err(Status::error(
            "invalid_backend_response",
            "expected identity scan members",
        ));
    };
    if members.len() % 2 != 0 {
        return Err(Status::error(
            "invalid_backend_response",
            "identity scan has an incomplete pair",
        ));
    }
    let mut cursor = after.to_vec();
    let mut identities = Vec::with_capacity(members.len() / 2);
    for pair in members.chunks_exact(2) {
        if pair[0].as_slice() <= cursor.as_slice() {
            return Err(Status::error(
                "invalid_backend_response",
                "identity scan did not advance",
            ));
        }
        cursor.clone_from(&pair[0]);
        if pair[0] != MEMORY_TOTAL_MEMBER {
            identities.push(pair[0].clone());
        }
    }
    Ok((cursor, identities))
}

fn memory_catalog_write<'a>(
    tenant_hash: u64,
    records: impl Iterator<Item = &'a MemoryRecord>,
) -> Command {
    Command::HashMultiSet {
        key: memory_catalog_key(tenant_hash),
        entries: records
            .map(|record| (record.id.clone(), record.encode()))
            .collect(),
    }
}

fn memory_delete_ack(memory_id: &str, status: u16, body: &[u8]) -> Result<(), (u16, String)> {
    #[derive(Deserialize)]
    struct Acknowledgment {
        id: Option<String>,
        deleted: Option<bool>,
        code: Option<String>,
    }
    let acknowledgment = serde_json::from_slice::<Acknowledgment>(body).ok();
    if status != 200 {
        let code = acknowledgment
            .and_then(|value| value.code)
            .filter(|code| !code.is_empty())
            .unwrap_or_else(|| match status {
                404 => "memory_not_found".into(),
                _ => "memory_delete_failed".into(),
            });
        return Err((status, code));
    }
    if let Some(value) = acknowledgment {
        if value.id.as_deref() == Some(memory_id) {
            match value.deleted {
                Some(true) => return Ok(()),
                Some(false) => return Err((409, "memory_delete_not_applied".into())),
                None => {}
            }
        }
    }
    Err((502, "invalid_memory_delete_response".into()))
}

fn memory_write_ack(response: &BatchExecuteResponse, expected: usize) -> Result<(), Status> {
    if !response.status.ok {
        return Err(response.status.clone());
    }
    if response.responses.len() != expected {
        return Err(Status::error(
            "memory_catalog_write_incomplete",
            "memory mutation batch returned an incomplete response",
        ));
    }
    if let Some(failed) = response.responses.iter().find(|response| !response.status.ok) {
        return Err(failed.status.clone());
    }
    Ok(())
}

fn memory_tombstone_write<'a>(
    tenant_hash: u64,
    records: impl Iterator<Item = &'a MemoryRecord>,
) -> Command {
    Command::HashMultiSet {
        key: memory_tombstone_key(tenant_hash),
        entries: records
            .map(|record| (record.id.clone(), b"1".to_vec()))
            .collect(),
    }
}

fn memory_tombstone_key(tenant_hash: u64) -> String {
    format!("context:memory-tombstone:{tenant_hash:016x}")
}

fn memory_order_key(tenant_hash: u64) -> String {
    format!("context:memory-order:{tenant_hash:016x}")
}

fn memory_order_ready_key(tenant_hash: u64) -> String {
    format!("context:memory-order-ready:{tenant_hash:016x}")
}

fn memory_identity_key(tenant_hash: u64) -> String {
    format!("context:memory-identity:{tenant_hash:016x}")
}

fn memory_identity_ready_key(tenant_hash: u64) -> String {
    format!("context:memory-identity-ready:{tenant_hash:016x}")
}

fn memory_identity_records_key(tenant_hash: u64, member: &[u8]) -> String {
    let identity = String::from_utf8_lossy(member);
    let hash = context::stable_scope_hash(&format!("{}:{identity}", member[0]));
    format!("context:memory-by-identity:{tenant_hash:016x}:{hash:016x}")
}

fn memory_identity_postings_key(tenant_hash: u64) -> String {
    format!("context:memory-identity-postings:{tenant_hash:016x}")
}

fn memory_identity_record_member(memory_id: &str) -> Vec<u8> {
    memory_id
        .split_once('-')
        .and_then(|(_, suffix)| u64::from_str_radix(suffix, 16).ok())
        .map(u64::to_be_bytes)
        .map(Vec::from)
        .unwrap_or_else(|| memory_id.as_bytes().to_vec())
}

fn memory_id_from_identity_record_member(tenant_hash: u64, member: &[u8]) -> Option<String> {
    if let Ok(suffix) = <[u8; 8]>::try_from(member) {
        return Some(format!(
            "{tenant_hash:016x}-{:016x}",
            u64::from_be_bytes(suffix)
        ));
    }
    std::str::from_utf8(member).ok().map(str::to_owned)
}

fn memory_identity_posting(records: &[MemoryRecord]) -> Vec<u8> {
    let mut posting = Vec::with_capacity(records.len() * 8);
    for record in records {
        posting.extend_from_slice(&memory_identity_record_member(&record.id));
    }
    posting
}

fn memory_ids_from_identity_posting(tenant_hash: u64, posting: &[u8]) -> Vec<String> {
    posting
        .chunks_exact(8)
        .filter_map(|member| memory_id_from_identity_record_member(tenant_hash, member))
        .collect()
}

fn memory_identity_member(kind: u8, value: &str) -> Option<Vec<u8>> {
    if value.is_empty() {
        return None;
    }
    let mut member = Vec::with_capacity(1 + value.len());
    member.push(kind);
    member.extend_from_slice(value.as_bytes());
    Some(member)
}

fn memory_identity_members(record: &MemoryRecord) -> Vec<Vec<u8>> {
    [
        (b'u', record.user_id.as_str()),
        (b'a', record.agent_id.as_str()),
        (b'r', record.run_id.as_str()),
    ]
    .into_iter()
    .filter_map(|(kind, value)| memory_identity_member(kind, value))
    .collect()
}

fn memory_identity_result(member: &[u8]) -> Option<Value> {
    let (kind, value) = member.split_first()?;
    let name = std::str::from_utf8(value).ok()?;
    let kind = match kind {
        b'u' => "user",
        b'a' => "agent",
        b'r' => "run",
        _ => return None,
    };
    Some(json!({"type": kind, "name": name}))
}

fn memory_order_member(updated_at_ms: u64, memory_id: &str) -> Vec<u8> {
    let mut member = Vec::with_capacity(8 + memory_id.len());
    member.extend_from_slice(&u64::MAX.saturating_sub(updated_at_ms).to_be_bytes());
    member.extend_from_slice(memory_id.as_bytes());
    member
}

fn memory_list_keep_best(
    ranked: &mut BTreeMap<Vec<u8>, MemoryRecord>,
    retained: &mut HashMap<String, Vec<u8>>,
    record: MemoryRecord,
    limit: usize,
) {
    let key = memory_order_member(record.updated_at_ms, &record.id);
    if let Some(previous) = retained.get(&record.id) {
        if previous <= &key {
            return;
        }
        let previous = retained.remove(&record.id).expect("retained memory key");
        ranked.remove(&previous);
    }
    if ranked.len() == limit
        && ranked
            .last_key_value()
            .is_some_and(|(worst, _)| worst <= &key)
    {
        return;
    }
    retained.insert(record.id.clone(), key.clone());
    ranked.insert(key, record);
    if ranked.len() > limit {
        let (_, removed) = ranked.pop_last().expect("nonempty ranked memories");
        retained.remove(&removed.id);
    }
}

fn memory_id_from_order_member(member: &[u8]) -> Option<String> {
    (member.len() > 8)
        .then(|| std::str::from_utf8(&member[8..]).ok().map(str::to_owned))
        .flatten()
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

fn memory_catalog_id_from_context_ref(context_ref: &str) -> &str {
    context_ref
        .split_once('~')
        .map(|(memory_id, _)| memory_id)
        .unwrap_or(context_ref)
}

fn memory_context_ref(memory_id: &str, updated_at_ms: u64) -> String {
    format!("{memory_id}~{updated_at_ms:016x}")
}

fn normalized_messages(
    messages: MemoryMessages,
    infer: bool,
) -> Result<Vec<(String, String, Option<String>)>, String> {
    match messages {
        MemoryMessages::Text(text) => {
            let text = if infer { text.trim().to_string() } else { text };
            if text.trim().is_empty() {
                Err("messages must contain non-empty text".to_string())
            } else {
                Ok(vec![("user".to_string(), text, None)])
            }
        }
        MemoryMessages::Many(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Value::String(text) if !text.trim().is_empty() => {
                        let text = if infer { text.trim().to_string() } else { text };
                        out.push(("user".to_string(), text, None));
                    }
                    Value::Object(map) => {
                        let role = map.get("role").and_then(Value::as_str).unwrap_or("user");
                        if !infer && (role == "system" || map.get("role").and_then(Value::as_str).is_none()) {
                            continue;
                        }
                        let content = map
                            .get("content")
                            .or_else(|| map.get("text"))
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        if !content.trim().is_empty() {
                            let content = if infer { content.trim() } else { content };
                            let actor_id = map.get("name").and_then(Value::as_str)
                                .filter(|name| !name.is_empty()).map(str::to_owned);
                            out.push((role.to_string(), content.to_string(), actor_id));
                        }
                    }
                    _ => {}
                }
            }
            if out.is_empty() && infer {
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
    fn memory_store_explicit_records<'a>(
        &self,
        scope: context::ProxyContextScope,
        tenant_hash: u64,
        records: impl IntoIterator<Item = &'a MemoryRecord>,
        provider: Option<Value>,
    ) -> Result<(), (u16, Vec<u8>)> {
        let provider = provider.map(serde_json::from_value::<crate::ContextModelProviderConfig>)
            .transpose().map_err(|error| self.memory_bad_request(error.to_string()))?
            .unwrap_or_default();
        let shard_id = self.context_shard_id(tenant_hash);
        let sources = records.into_iter().map(|record| crate::ContextExtractRequest {
            shard_id, tenant_hash, source_kind: crate::ContextSourceKind::Chat,
            source_id: non_empty(&record.context_ref, &record.id).to_owned(),
            title: record.metadata["role"].as_str().unwrap_or("memory").to_owned(),
            body: record.memory.clone(), timestamp_ms: record.updated_at_ms,
            provider: crate::ContextModelProviderConfig { provider_name: String::new(), ..Default::default() },
        }).collect::<Vec<_>>();
        let end_time_ms = sources.iter().map(|source| source.timestamp_ms).max().unwrap_or(0).saturating_add(1);
        let request = crate::ContextIngestExtractRequest {
            shard_id, tenant_hash, query: String::new(), start_time_ms: 0,
            end_time_ms, max_events: sources.len(), sources, provider,
        };
        self.context_store_explicit(&scope, request)
    }

    pub(super) fn handle_memory_route(&self, request: &HttpRequest) -> Option<(u16, Vec<u8>)> {
        use crate::http::{json_response, parse_json};
        if (request.path == "/memories" || request.path.starts_with("/memories/"))
            && request.body.len() > crate::context_workflow::ingest_wire::EXPLICIT_MAX_BODY_BYTES
        {
            return Some(self.memory_bad_request("memory request exceeds 16 MiB body limit".into()));
        }

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
            ("POST", "/memories/indexes/rebuild") => Some(
                match parse_json::<MemoryNodeLookupRebuildRequest>(&request.body) {
                    Ok(body) => self.memory_rebuild_node_lookup(body),
                    Err(err) => self.memory_bad_request(err.to_string()),
                },
            ),
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
            .filter(|(_, tail)| matches!(*tail, "history" | "feedback" | "update" | "delete"))
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
        let infer = request.infer.unwrap_or(true);
        if !infer && !request.metadata.is_null() && !request.metadata.is_object() {
            return self.memory_bad_request("metadata must be an object for explicit memories".into());
        }
        let messages = match normalized_messages(request.messages, infer) {
            Ok(messages) => messages,
            Err(err) => return self.memory_bad_request(err),
        };
        if messages.is_empty() {
            return crate::http::json_response(200, &json!({"results":[]}));
        }
        if messages.len() > MEMORY_BATCH_LIMIT {
            return self.memory_bad_request(format!("add accepts at most {MEMORY_BATCH_LIMIT} messages"));
        }
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let shard_id = self.context_shard_id(tenant_hash);
        if let Err(status) = self.memory_prepare_indexes(tenant_hash) {
            return crate::http::json_response(502, &status);
        }
        let now = now_ms().max(1);
        let base_sequence = self
            .inner
            .context_ingest_sequence
            .fetch_add(messages.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let mut records = Vec::with_capacity(messages.len());
        let mut sources = if infer {
            Vec::with_capacity(messages.len())
        } else {
            Vec::new()
        };
        let mut actor_ids = Vec::new();
        for (index, (role, text, actor_id)) in messages.into_iter().enumerate() {
            let id = memory_id(tenant_hash, now, base_sequence + index as u64, &text);
            if infer {
                sources.push(json!({
                    "body": text,
                    "shard_id": shard_id,
                    "source_id": id,
                    "source_kind": "chat",
                    "tenant_hash": tenant_hash,
                    "timestamp_ms": now + index as u64,
                    "title": role,
                }));
            }
            let mut metadata = request.metadata.clone();
            if !infer {
                if metadata.is_null() {
                    metadata = json!({});
                }
                let fields = metadata.as_object_mut().expect("explicit metadata is an object");
                fields.insert("role".into(), Value::String(role));
                if let Some(actor) = &actor_id {
                    fields.insert("actor_id".into(), Value::String(actor.clone()));
                }
                actor_ids.push(actor_id);
            }
            records.push(MemoryRecord {
                context_ref: id.clone(),
                id,
                memory: text,
                metadata,
                tenant_id: request.scope.tenant_id.clone(),
                account_id: request.scope.account_id.clone(),
                user_id: identity.user_id.clone(),
                agent_id: identity.agent_id.clone(),
                run_id: identity.run_id.clone(),
                created_at_ms: now,
                updated_at_ms: now,
            });
        }
        if infer {
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
        } else {
            if let Err(response) = self.memory_store_explicit_records(scope, tenant_hash, records.iter(), request.provider) {
                return response;
            }
        }
        let mut commands = Vec::with_capacity(records.len() * 3 + 3);
        commands.push(memory_catalog_write(tenant_hash, records.iter()));
        commands.push(memory_node_lookup_write(tenant_hash, records.iter()));
        for (index, record) in records.iter().enumerate() {
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
            commands.push(Command::SetAdd {
                key: memory_order_key(tenant_hash),
                member: memory_order_member(record.updated_at_ms, &record.id),
            });
        }
        if let Some(record) = records.first() {
            let increment = records.len().min(i64::MAX as usize) as i64;
            let posting_field = record.id.clone();
            commands.push(Command::HashSet {
                key: memory_identity_postings_key(tenant_hash),
                field: posting_field.clone(),
                value: memory_identity_posting(&records),
            });
            commands.extend(memory_identity_members(record).into_iter().map(|member| {
                Command::SetAdd {
                    key: memory_identity_records_key(tenant_hash, &member),
                    member: posting_field.as_bytes().to_vec(),
                }
            }));
            commands.push(Command::ZSetCountAdjust {
                key: memory_identity_key(tenant_hash),
                member: MEMORY_TOTAL_MEMBER.to_vec(),
                increment,
            });
            commands.extend(memory_identity_members(record).into_iter().map(|member| {
                Command::ZSetCountAdjust {
                    key: memory_identity_key(tenant_hash),
                    member,
                    increment,
                }
            }));
        }
        let expected_commands = commands.len();
        let stored = self.batch_execute(BatchExecuteRequest { shard_id, commands });
        if let Err(status) = memory_write_ack(&stored, expected_commands) {
            return crate::http::json_response(502, &status);
        }
        crate::http::json_response(
            200,
            &json!({
                "results": records.iter().enumerate().map(|(index, record)| {
                    let mut result = json!({
                        "id": record.id,
                        "memory": record.memory,
                        "event": "ADD",
                    });
                    if !infer {
                        result["role"] = record.metadata["role"].clone();
                        result["actor_id"] = json!(actor_ids.get(index).and_then(Option::as_deref));
                    }
                    result
                }).collect::<Vec<_>>()
            }),
        )
    }

    fn memory_search(&self, request: MemoryQueryRequest) -> (u16, Vec<u8>) {
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let limit = request.limit.unwrap_or(10).clamp(1, 100);
        let scoped_records = if identity.user_id.is_empty()
            && identity.agent_id.is_empty()
            && identity.run_id.is_empty()
        {
            None
        } else {
            self.memory_ranked_identity_records(
                tenant_hash,
                &identity,
                MEMORY_IDENTITY_CANDIDATE_LIMIT,
                MEMORY_IDENTITY_CANDIDATE_LIMIT,
            )
        };
        let node_hashes: Vec<u64> = scoped_records
            .as_ref()
            .map(|records| {
                records
                    .iter()
                    .map(|record| memory_record_node_hash(tenant_hash, record))
                    .collect()
            })
            .unwrap_or_default();
        let node_sources = scoped_records.as_ref().map(|records| {
            node_hashes
                .iter()
                .copied()
                .zip(records.iter().map(|record| {
                    if record.context_ref.is_empty() {
                        record.id.as_str()
                    } else {
                        record.context_ref.as_str()
                    }
                }))
                .collect::<HashMap<_, _>>()
        });
        let retrieval = context::ProxyContextRetrieveRequest {
            scope,
            query: request.query,
            start_time_ms: 0,
            end_time_ms: 0,
            max_events: Some(limit.saturating_mul(4)),
            node_hashes,
            provider: request.provider,
            include_debug: false,
        };
        let (status, body) = if scoped_records.is_some() {
            self.context_retrieve_from_nodes(retrieval)
        } else {
            self.context_retrieve(retrieval)
        };
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
        let mut blocks = report.blocks;
        if let Some(sources) = &node_sources {
            blocks.retain_mut(|block| {
                let Some(source) = sources.get(&block.node_hash) else {
                    return false;
                };
                block.source_ref.clear();
                block.source_ref.push_str(source);
                true
            });
        } else {
            let aliases = match self.memory_lookup_node_ids(tenant_hash, &blocks) {
                Ok(aliases) => aliases,
                Err(status) => return crate::http::json_response(502, &status),
            };
            for block in &mut blocks {
                if let Some(id) = aliases.get(&block.node_hash) {
                    block.source_ref.clone_from(id);
                }
            }
        }
        drop(node_sources);
        let mut ids = Vec::with_capacity(blocks.len());
        let mut seen = HashSet::with_capacity(blocks.len());
        for block in &blocks {
            let memory_id = memory_catalog_id_from_context_ref(&block.source_ref);
            if !memory_id.is_empty() && seen.insert(memory_id.to_string()) {
                ids.push(memory_id.to_string());
            }
        }
        let hydrated = if let Some(records) = scoped_records {
            let mut records = records
                .into_iter()
                .map(|record| (record.id.clone(), record))
                .collect::<HashMap<_, _>>();
            ids.iter().map(|id| records.remove(id)).collect()
        } else {
            match self.memory_read_many_checked(tenant_hash, &ids) {
                Ok(records) => records,
                Err(status) => return crate::http::json_response(502, &status),
            }
        };
        let records = ids
            .into_iter()
            .zip(hydrated)
            .filter_map(|(id, record)| record.map(|record| (id, record)))
            .filter(|(_, record)| record.matches(&identity))
            .collect::<std::collections::HashMap<_, _>>();
        let mut results = Vec::with_capacity(limit.min(blocks.len()));
        let mut emitted = HashSet::with_capacity(limit.min(blocks.len()));
        for block in blocks {
            if results.len() >= limit {
                break;
            }
            let memory_id = memory_catalog_id_from_context_ref(&block.source_ref);
            let Some(record) = records.get(memory_id) else {
                continue;
            };
            if memory_record_node_hash(tenant_hash, record) != block.node_hash
                || !emitted.insert(record.id.clone())
            {
                continue;
            }
            let rank = results.len();
            let mut metadata = if record.metadata.is_object() {
                record.metadata.clone()
            } else {
                json!({})
            };
            if let Some(map) = metadata.as_object_mut() {
                map.insert("record_type".into(), json!(block.record_type));
                map.insert("tier".into(), json!(block.tier));
                map.insert("uri".into(), json!(block.uri));
                map.insert("node_hash".into(), json!(block.node_hash));
                map.insert("event_time_ms".into(), json!(block.event_time_ms));
            }
            results.push(json!({
                "id": record.id,
                "memory": record.memory,
                "metadata": metadata,
                "score": 1.0 - (rank as f64 / (limit.max(1) as f64 + 1.0)),
                "created_at": record.created_at_ms,
                "updated_at": record.updated_at_ms,
            }));
        }
        crate::http::json_response(200, &json!({"results": results}))
    }

    fn memory_get(&self, memory_id: &str) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        match self.memory_read_one_checked(tenant_hash, memory_id) {
            Ok(Some(record)) => crate::http::json_response(200, &record),
            Ok(None) => crate::http::json_response(404, &json!({"found": false, "id": memory_id})),
            Err(status) => crate::http::json_response(502, &status),
        }
    }

    fn memory_list(&self, request: MemoryQueryRequest) -> (u16, Vec<u8>) {
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let limit = request.limit.unwrap_or(100).clamp(1, 10_000);
        if let Err(status) = self.memory_prepare_indexes(tenant_hash) {
            return crate::http::json_response(502, &status);
        }
        let first = self.memory_order_page(tenant_hash, &[], MEMORY_CATALOG_PAGE_SIZE);
        let mut records = first
            .rows
            .into_iter()
            .map(|(_, record)| record)
            .filter(|record| record.matches(&identity))
            .take(limit)
            .collect::<Vec<_>>();
        let mut after = first.after;
        if records.len() < limit && first.candidate_count == MEMORY_CATALOG_PAGE_SIZE {
            if let Some(indexed) = self.memory_ranked_identity_records(
                tenant_hash,
                &identity,
                limit,
                limit - records.len(),
            ) {
                let count = indexed.len();
                return crate::http::json_response(
                    200,
                    &json!({"results": indexed, "memories": count}),
                );
            }
        }
        if first.candidate_count < MEMORY_CATALOG_PAGE_SIZE {
            after.clear();
        }
        while records.len() < limit && !after.is_empty() {
            let page = self.memory_order_page(tenant_hash, &after, MEMORY_CATALOG_PAGE_SIZE);
            if page.after.is_empty() {
                break;
            }
            after = page.after;
            for (_, record) in page.rows {
                if record.matches(&identity) {
                    records.push(record);
                    if records.len() == limit {
                        break;
                    }
                }
            }
        }
        let count = records.len();
        crate::http::json_response(200, &json!({"results": records, "memories": count}))
    }

    fn memory_ranked_identity_records(
        &self,
        tenant_hash: u64,
        identity: &MemoryIdentity,
        limit: usize,
        remaining: usize,
    ) -> Option<Vec<MemoryRecord>> {
        let members = [
            memory_identity_member(b'u', &identity.user_id),
            memory_identity_member(b'a', &identity.agent_id),
            memory_identity_member(b'r', &identity.run_id),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        if members.is_empty() {
            return None;
        }
        let mut commands = vec![Command::ZSetScore {
            key: memory_identity_key(tenant_hash),
            member: MEMORY_TOTAL_MEMBER.to_vec(),
        }];
        commands.extend(members.iter().map(|member| Command::ZSetScore {
            key: memory_identity_key(tenant_hash),
            member: member.clone(),
        }));
        let counts = self.batch_execute(BatchExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            commands,
        });
        if !counts.status.ok {
            return None;
        }
        let count = |response: &ExecuteResponse| -> Option<usize> {
            if !response.status.ok {
                return None;
            }
            let CommandResponse::Bytes { value: Some(value) } = &response.response else {
                return None;
            };
            let value = std::str::from_utf8(value).ok()?.parse::<f64>().ok()?;
            (value.is_finite()
                && value >= 0.0
                && value.fract() == 0.0
                && value <= usize::MAX as f64)
                .then_some(value as usize)
        };
        let total = count(counts.responses.first()?)?;
        let (member, matched) = members
            .into_iter()
            .zip(counts.responses.iter().skip(1))
            .filter_map(|(member, response)| {
                count(response)
                    .filter(|count| *count > 0)
                    .map(|count| (member, count))
            })
            .min_by_key(|(_, count)| *count)?;
        if matched > total
            || matched > MEMORY_IDENTITY_CANDIDATE_LIMIT
            || (matched > MEMORY_CATALOG_PAGE_SIZE
                && (matched as u128) * (matched as u128) > (total as u128) * (remaining as u128))
        {
            return None;
        }

        let mut budget = matched
            .saturating_mul(2)
            .max(MEMORY_CATALOG_PAGE_SIZE)
            .min(MEMORY_IDENTITY_CANDIDATE_LIMIT);
        let mut after = Vec::new();
        let mut seen = HashSet::new();
        let mut ranked = BTreeMap::new();
        let mut retained = HashMap::new();
        loop {
            let page = self.memory_identity_postings_page_with_budget(
                tenant_hash,
                &member,
                &after,
                MEMORY_IDENTITY_POSTINGS_PER_PAGE,
                Some(budget.min(MEMORY_CATALOG_PAGE_SIZE)),
            )?;
            if page.after.is_empty() {
                break;
            }
            if page.after <= after {
                return None;
            }
            after = page.after;
            budget = budget.checked_sub(page.candidate_count)?;
            for (_, record) in page.rows {
                let (kind, value) = member.split_first()?;
                let selected = match kind {
                    b'u' => record.user_id.as_bytes(),
                    b'a' => record.agent_id.as_bytes(),
                    b'r' => record.run_id.as_bytes(),
                    _ => return None,
                };
                if selected == value {
                    seen.insert(record.id.clone());
                }
                if record.matches(identity) {
                    memory_list_keep_best(&mut ranked, &mut retained, record, limit);
                }
            }
        }
        // Missing, stale, or concurrently changing postings cannot establish a complete list.
        if seen.len() != matched {
            return None;
        }
        Some(ranked.into_values().collect())
    }

    fn memory_update(&self, memory_id: &str, request: MemoryUpdateRequest) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        if let Err(status) = self.memory_prepare_indexes(tenant_hash) {
            return crate::http::json_response(502, &status);
        }
        let record = match self.memory_read_one_checked(tenant_hash, memory_id) {
            Ok(record) => record,
            Err(status) => return crate::http::json_response(502, &status),
        };
        let Some(mut record) = record else {
            return crate::http::json_response(404, &json!({"updated": false, "id": memory_id}));
        };
        let old_order_member = memory_order_member(record.updated_at_ms, &record.id);
        let old_node_hash = memory_record_node_hash(tenant_hash, &record);
        if request.data.trim().is_empty() && request.metadata.is_none() {
            return self.memory_bad_request("update requires data or metadata".to_string());
        }
        let content_updated = !request.data.trim().is_empty();
        let now = now_ms().max(record.updated_at_ms.saturating_add(1));
        if content_updated {
            record.memory = request.data;
        }
        if let Some(metadata) = request.metadata {
            record.metadata = metadata;
        }
        record.updated_at_ms = now;
        let shard_id = self.context_shard_id(tenant_hash);
        let mut commands = Vec::with_capacity(6);
        if content_updated {
            record.context_ref = memory_context_ref(&record.id, now);
            if let Err(response) = self.memory_store_explicit_records(
                context::ProxyContextScope {
                    tenant_id: record.tenant_id.clone(),
                    account_id: record.account_id.clone(),
                    user_id: record.user_id.clone(),
                    session_id: record.run_id.clone(),
                }, tenant_hash, std::iter::once(&record), request.provider,
            ) {
                return response;
            }
            commands.push(memory_node_lookup_remove(tenant_hash, old_node_hash));
            commands.push(memory_node_lookup_write(tenant_hash, std::iter::once(&record)));
        }
        commands.extend([
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
            Command::SetRemove {
                key: memory_order_key(tenant_hash),
                member: old_order_member,
            },
            Command::SetAdd {
                key: memory_order_key(tenant_hash),
                member: memory_order_member(record.updated_at_ms, &record.id),
            },
        ]);
        let expected = commands.len();
        let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
        if let Err(status) = memory_write_ack(&response, expected) {
            return crate::http::json_response(502, &status);
        }
        crate::http::json_response(200, &json!({"updated": true, "id": memory_id}))
    }

    fn memory_delete(&self, memory_id: &str) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        if let Err(status) = self.memory_prepare_indexes(tenant_hash) {
            return crate::http::json_response(502, &status);
        }
        let record = match self.memory_read_one_checked(tenant_hash, memory_id) {
            Ok(record) => record,
            Err(status) => return crate::http::json_response(502, &status),
        };
        let Some(record) = record else {
            return crate::http::json_response(404, &json!({"deleted": false, "id": memory_id}));
        };
        let now = now_ms().max(1);
        let shard_id = self.context_shard_id(tenant_hash);
        let removed = self.execute(ExecuteRequest {
            shard_id,
            command: Command::HashDelete {
                key: memory_catalog_key(tenant_hash),
                field: memory_id.to_string(),
            },
        });
        if !removed.status.ok {
            return crate::http::json_response(502, &removed.status);
        }
        if !matches!(
            removed.response,
            CommandResponse::Empty | CommandResponse::Integer { value: 1 }
        ) {
            return crate::http::json_response(200, &json!({"deleted": false, "id": memory_id}));
        }
        let mut commands = vec![
            memory_node_lookup_remove(tenant_hash, memory_record_node_hash(tenant_hash, &record)),
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
            Command::SetRemove {
                key: memory_order_key(tenant_hash),
                member: memory_order_member(record.updated_at_ms, memory_id),
            },
            Command::ZSetCountAdjust {
                key: memory_identity_key(tenant_hash),
                member: MEMORY_TOTAL_MEMBER.to_vec(),
                increment: -1,
            },
        ];
        commands.extend(memory_identity_members(&record).into_iter().map(|member| {
            Command::ZSetCountAdjust {
                key: memory_identity_key(tenant_hash),
                member,
                increment: -1,
            }
        }));
        let expected = commands.len();
        let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
        if !response.status.ok {
            let _ = self.batch_execute(BatchExecuteRequest {
                shard_id,
                commands: vec![
                    Command::HashSet {
                        key: memory_catalog_key(tenant_hash),
                        field: memory_id.to_string(),
                        value: record.encode(),
                    },
                    memory_node_lookup_write(tenant_hash, std::iter::once(&record)),
                ],
            });
            return crate::http::json_response(502, &response.status);
        }
        if let Err(status) = memory_write_ack(&response, expected) {
            return crate::http::json_response(502, &status);
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
        if !response.status.ok {
            return crate::http::json_response(502, &response.status);
        }
        let mut entries = match response.response {
            CommandResponse::HashEntries { entries } => entries,
            _ => return crate::http::json_response(502, &Status::error(
                "invalid_memory_history_response", "expected memory history entries",
            )),
        };
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let history = entries
            .into_iter()
            .map(|(_, bytes)| MemoryHistoryEntry::decode(&bytes).ok_or_else(|| Status::error(
                "invalid_memory_history_record", "memory history entry cannot be decoded",
            )))
            .collect::<Result<Vec<_>, _>>();
        let history = match history {
            Ok(history) => history,
            Err(status) => return crate::http::json_response(502, &status),
        };
        crate::http::json_response(200, &json!({"results": history}))
    }

    fn memory_feedback(&self, memory_id: &str, request: MemoryFeedbackRequest) -> (u16, Vec<u8>) {
        let Some(tenant_hash) = tenant_hash_from_memory_id(memory_id) else {
            return self.memory_bad_request("invalid memory id".to_string());
        };
        let record = match self.memory_read_one_checked(tenant_hash, memory_id) {
            Ok(record) => record,
            Err(status) => return crate::http::json_response(502, &status),
        };
        if record.is_none() {
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
        if let Err(status) = self.memory_prepare_indexes(tenant_hash) {
            return crate::http::json_response(502, &status);
        }
        let limit = request.limit.unwrap_or(1_000).clamp(1, 10_000);
        let response = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            command: Command::ZSetScan {
                key: memory_identity_key(tenant_hash),
                after: Vec::new(),
                limit,
            },
        });
        if !response.status.ok {
            return crate::http::json_response(502, &response.status);
        }
        let results = match response.response {
            CommandResponse::Members { members } => members
                .chunks_exact(2)
                .filter_map(|entry| memory_identity_result(&entry[0]))
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        };
        crate::http::json_response(200, &json!({"results": results}))
    }

    fn memory_delete_all(&self, request: MemoryQueryRequest) -> (u16, Vec<u8>) {
        let identity = request.scope.identity(&request.identity);
        let scope = request.scope.context_scope(&identity);
        let tenant_hash = context::context_tenant_hash(&scope);
        let shard_id = self.context_shard_id(tenant_hash);
        let identity_members = [
            memory_identity_member(b'u', &identity.user_id),
            memory_identity_member(b'a', &identity.agent_id),
            memory_identity_member(b'r', &identity.run_id),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let mut count_commands = Vec::with_capacity(identity_members.len() + 1);
        count_commands.push(Command::ZSetScore {
            key: memory_identity_key(tenant_hash),
            member: MEMORY_TOTAL_MEMBER.to_vec(),
        });
        count_commands.extend(
            identity_members
                .iter()
                .cloned()
                .map(|member| Command::ZSetScore {
                    key: memory_identity_key(tenant_hash),
                    member,
                }),
        );
        let counts = self.batch_execute(BatchExecuteRequest {
            shard_id,
            commands: count_commands,
        });
        if !counts.status.ok {
            return crate::http::json_response(502, &counts.status);
        }
        let score = |response: &CommandResponse| match response {
            CommandResponse::Bytes { value: Some(value) } => std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<f64>().ok()),
            _ => None,
        };
        let total = counts
            .responses
            .first()
            .and_then(|response| score(&response.response))
            .map(|value| value.max(0.0) as usize);
        let identity_counts = counts
            .responses
            .iter()
            .skip(1)
            .map(|response| score(&response.response))
            .collect::<Vec<_>>();
        let identity_covers_scope = total.is_some()
            && identity_counts
                .iter()
                .all(|count| count.map(|value| value as usize) == total);
        let delete_whole_scope = identity_members.is_empty() || identity_covers_scope;
        if delete_whole_scope {
            let deleted = total.unwrap_or_default();
            let withdrawn = self.batch_execute(BatchExecuteRequest {
                shard_id,
                commands: vec![
                    Command::StringDelete { key: memory_order_ready_key(tenant_hash) },
                    Command::StringDelete { key: memory_identity_ready_key(tenant_hash) },
                ],
            });
            if let Err(status) = memory_write_ack(&withdrawn, 2) {
                return crate::http::json_response(502, &status);
            }
            self.memory_forget_indexes_ready(tenant_hash);
            let mut after = Vec::new();
            loop {
                let identities = self.execute(ExecuteRequest {
                    shard_id,
                    command: Command::ZSetScan {
                        key: memory_identity_key(tenant_hash),
                        after: after.clone(),
                        limit: MEMORY_CATALOG_PAGE_SIZE,
                    },
                });
                if !identities.status.ok {
                    return crate::http::json_response(502, &identities.status);
                }
                let (next, members) = match memory_identity_scan_page(identities.response, &after) {
                    Ok(page) => page,
                    Err(status) => return crate::http::json_response(502, &status),
                };
                if next == after {
                    break;
                }
                after = next;
                if !members.is_empty() {
                    let response = self.batch_execute(BatchExecuteRequest {
                        shard_id,
                        commands: members
                            .iter()
                            .map(|member| Command::StringDelete {
                                key: memory_identity_records_key(tenant_hash, member),
                            })
                            .collect(),
                    });
                    if let Err(status) = memory_write_ack(&response, members.len()) {
                        return crate::http::json_response(502, &status);
                    }
                }
            }
            let delete_commands = vec![
                Command::StringDelete {
                    key: memory_catalog_key(tenant_hash),
                },
                Command::StringDelete {
                    key: memory_node_lookup_key(tenant_hash),
                },
                Command::StringDelete {
                    key: memory_tombstone_key(tenant_hash),
                },
                Command::StringDelete {
                    key: memory_order_key(tenant_hash),
                },
                Command::StringDelete {
                    key: memory_identity_key(tenant_hash),
                },
                Command::StringDelete {
                    key: memory_identity_postings_key(tenant_hash),
                },
            ];
            let expected = delete_commands.len();
            let response = self.batch_execute(BatchExecuteRequest {
                shard_id,
                commands: delete_commands,
            });
            if let Err(status) = memory_write_ack(&response, expected) {
                return crate::http::json_response(502, &status);
            }
            let marker_commands = vec![
                Command::StringSet {
                    key: memory_order_ready_key(tenant_hash),
                    value: b"1".to_vec(),
                },
                Command::StringSet {
                    key: memory_identity_ready_key(tenant_hash),
                    value: MEMORY_IDENTITY_INDEX_VERSION.to_vec(),
                },
            ];
            let response = self.batch_execute(BatchExecuteRequest {
                shard_id,
                commands: marker_commands,
            });
            if let Err(status) = memory_write_ack(&response, 2) {
                return crate::http::json_response(502, &status);
            }
            self.memory_remember_indexes_ready(tenant_hash);
            return crate::http::json_response(200, &json!({"deleted": deleted}));
        }
        if let Err(status) = self.memory_prepare_indexes(tenant_hash) {
            return crate::http::json_response(502, &status);
        }
        let selected_identity = identity_members
            .iter()
            .zip(identity_counts.iter())
            .min_by(|(_, left), (_, right)| {
                left.unwrap_or(f64::MAX)
                    .total_cmp(&right.unwrap_or(f64::MAX))
            })
            .map(|(member, _)| member.clone())
            .unwrap_or_else(|| identity_members[0].clone());
        let mut deleted = 0usize;
        let mut after = Vec::new();
        loop {
            let (next_after, page) = self.memory_identity_page(
                tenant_hash,
                &selected_identity,
                &after,
                MEMORY_CATALOG_PAGE_SIZE,
            );
            if next_after.is_empty() {
                break;
            }
            after = next_after;
            let candidates = page
                .into_iter()
                .filter(|(_, record)| record.matches(&identity))
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                continue;
            }
            let removals = self.execute(ExecuteRequest {
                shard_id,
                command: memory_tombstone_write(
                    tenant_hash,
                    candidates.iter().map(|(_, record)| record),
                ),
            });
            if !removals.status.ok {
                return crate::http::json_response(502, &removals.status);
            }
            let mut removed_records = Vec::new();
            let mut removed_postings = BTreeMap::<Vec<u8>, MemoryRecord>::new();
            let mut identity_counts = BTreeMap::<Vec<u8>, i64>::new();
            for (posting, record) in candidates {
                removed_postings
                    .entry(posting)
                    .or_insert_with(|| record.clone());
                for member in memory_identity_members(&record) {
                    *identity_counts.entry(member).or_default() -= 1;
                }
                removed_records.push(record);
                deleted += 1;
            }
            let mut commands = Vec::new();
            commands.extend(removed_records.iter().map(|record| {
                memory_node_lookup_remove(tenant_hash, memory_record_node_hash(tenant_hash, record))
            }));
            for (posting, record) in &removed_postings {
                commands.extend(memory_identity_members(record).into_iter().map(|member| {
                    Command::SetRemove {
                        key: memory_identity_records_key(tenant_hash, &member),
                        member: posting.clone(),
                    }
                }));
                if let Ok(field) = std::str::from_utf8(posting) {
                    commands.push(Command::HashDelete {
                        key: memory_identity_postings_key(tenant_hash),
                        field: field.to_string(),
                    });
                }
            }
            commands.push(Command::ZSetCountAdjust {
                key: memory_identity_key(tenant_hash),
                member: MEMORY_TOTAL_MEMBER.to_vec(),
                increment: -(removed_records.len().min(i64::MAX as usize) as i64),
            });
            commands.extend(identity_counts.into_iter().map(|(member, increment)| {
                Command::ZSetCountAdjust {
                    key: memory_identity_key(tenant_hash),
                    member,
                    increment,
                }
            }));
            if !commands.is_empty() {
                let expected = commands.len();
                let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
                if !response.status.ok {
                    let mut rollback = vec![memory_node_lookup_write(
                        tenant_hash,
                        removed_records.iter(),
                    )];
                    rollback.extend(removed_records.into_iter().map(|record| {
                        Command::HashDelete {
                            key: memory_tombstone_key(tenant_hash),
                            field: record.id,
                        }
                    }));
                    let _ = self.batch_execute(BatchExecuteRequest {
                        shard_id,
                        commands: rollback,
                    });
                    return crate::http::json_response(502, &response.status);
                }
                if let Err(status) = memory_write_ack(&response, expected) {
                    return crate::http::json_response(502, &status);
                }
            }
        }
        crate::http::json_response(200, &json!({"deleted": deleted}))
    }

    fn memory_batch_update(&self, request: MemoryBatchRequest) -> (u16, Vec<u8>) {
        if request.memories.len() > MEMORY_BATCH_LIMIT {
            return self.memory_bad_request(format!(
                "batch accepts at most {MEMORY_BATCH_LIMIT} memories"
            ));
        }
        let mut results = Vec::with_capacity(request.memories.len());
        let mut failed = Vec::new();
        let mut grouped = BTreeMap::<(u64, Option<String>), Vec<(String, MemoryUpdateRequest)>>::new();
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
                provider: value
                    .get("provider")
                    .filter(|provider| !provider.is_null())
                    .cloned()
                    .or_else(|| request.provider.clone()),
            };
            if id.is_empty() {
                failed.push(json!({"error": "memory_id is required"}));
                continue;
            }
            let Some(tenant_hash) = tenant_hash_from_memory_id(id) else {
                failed.push(json!({"memory_id": id, "status": 400, "error": "invalid memory id"}));
                continue;
            };
            grouped
                .entry((
                    tenant_hash,
                    (!update.data.trim().is_empty()).then(|| {
                        update.provider.as_ref().map(Value::to_string).unwrap_or_default()
                    }),
                ))
                .or_default()
                .push((id.to_string(), update));
        }

        for ((tenant_hash, provider_key), updates) in grouped {
            // Metadata-only work is independent of model availability, including in mixed batches.
            let content_updated = provider_key.is_some();
            if let Err(status) = self.memory_prepare_indexes(tenant_hash) {
                for (id, _) in updates {
                    failed.push(json!({"memory_id": id, "status": 502, "code":status.code, "body": status.message}));
                }
                continue;
            }

            let ids: Vec<String> = updates.iter().map(|(id, _)| id.clone()).collect();
            let records = match self.memory_read_many_checked(tenant_hash, &ids) {
                Ok(records) => records,
                Err(status) => {
                    failed.extend(ids.into_iter().map(|id| json!({
                        "memory_id":id,"status":502,"code":status.code,"body":status.message
                    })));
                    continue;
                }
            };
            let provider = updates
                .first()
                .and_then(|(_, update)| update.provider.clone());
            let mut planned = Vec::with_capacity(updates.len());
            for ((id, update), record) in updates.into_iter().zip(records) {
                let Some(mut record) = record else {
                    failed
                        .push(json!({"memory_id": id, "status": 404, "code":"memory_not_found", "body": "memory not found"}));
                    continue;
                };
                if update.data.trim().is_empty() && update.metadata.is_none() {
                    failed.push(json!({"memory_id": id, "status": 400, "body": "update requires data or metadata"}));
                    continue;
                }
                let old_order_member = memory_order_member(record.updated_at_ms, &record.id);
                let old_node_hash = memory_record_node_hash(tenant_hash, &record);
                let now = now_ms().max(record.updated_at_ms.saturating_add(1));
                if !update.data.trim().is_empty() {
                    record.memory = update.data;
                }
                if let Some(metadata) = update.metadata {
                    record.metadata = metadata;
                }
                record.updated_at_ms = now;
                if content_updated {
                    record.context_ref = memory_context_ref(&record.id, now);
                }
                planned.push((id, old_order_member, old_node_hash, record, now));
            }
            if planned.is_empty() {
                continue;
            }

            let shard_id = self.context_shard_id(tenant_hash);
            if content_updated {
                let scope_record = &planned[0].3;
                if let Err((extract_status, extract_body)) = self.memory_store_explicit_records(
                    context::ProxyContextScope {
                        tenant_id: scope_record.tenant_id.clone(),
                        account_id: scope_record.account_id.clone(),
                        user_id: scope_record.user_id.clone(),
                        session_id: scope_record.run_id.clone(),
                    }, tenant_hash, planned.iter().map(|(_, _, _, record, _)| record), provider,
                ) {
                    let body = String::from_utf8_lossy(&extract_body);
                    for (id, _, _, _, _) in planned {
                        failed.push(json!({"memory_id": id, "status": extract_status, "body": body}));
                    }
                    continue;
                }
            }

            for page in planned.chunks(MEMORY_UPDATE_PAGE_SIZE) {
                let mut commands = Vec::with_capacity(page.len() * 4 + 2);
                commands.push(memory_catalog_write(
                    tenant_hash,
                    page.iter().map(|(_, _, _, record, _)| record),
                ));
                if content_updated {
                    commands.push(memory_node_lookup_write(
                        tenant_hash,
                        page.iter().map(|(_, _, _, record, _)| record),
                    ));
                }
                for (sequence, (id, old_order_member, old_node_hash, record, now)) in
                    page.iter().enumerate()
                {
                    if content_updated {
                        commands.push(memory_node_lookup_remove(tenant_hash, *old_node_hash));
                    }
                    commands.extend([
                        Command::HashSet {
                            key: memory_history_key(tenant_hash, id),
                            field: history_field(*now, sequence as u64),
                            value: MemoryHistoryEntry {
                                id: record.id.clone(),
                                event: "UPDATE".to_string(),
                                at_ms: *now,
                                memory: Some(record.memory.clone()),
                                metadata: Some(record.metadata.clone()),
                                feedback: None,
                                feedback_reason: None,
                            }
                            .encode(),
                        },
                        Command::SetRemove {
                            key: memory_order_key(tenant_hash),
                            member: old_order_member.clone(),
                        },
                        Command::SetAdd {
                            key: memory_order_key(tenant_hash),
                            member: memory_order_member(record.updated_at_ms, &record.id),
                        },
                    ]);
                }
                let expected = commands.len();
                let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
                if let Err(status) = memory_write_ack(&response, expected) {
                    failed.extend(page.iter().map(|(id, _, _, _, _)| {
                        json!({"memory_id": id, "status": 502, "code":status.code, "body": status.message})
                    }));
                } else {
                    results.extend(page.iter().map(|(id, _, _, _, _)| json!({"memory_id": id})));
                }
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
            match memory_delete_ack(id, status, &body) {
                Ok(()) => results.push(json!({"memory_id": id})),
                Err((status, code)) => failed.push(json!({
                    "memory_id": id, "status": status, "code": code,
                    "body": String::from_utf8_lossy(&body),
                })),
            }
        }
        let deleted = results.len();
        crate::http::json_response(
            200,
            &json!({"results": results, "deleted": deleted, "failed": failed}),
        )
    }

    fn memory_prepare_order_index(&self, tenant_hash: u64) -> Result<(), Status> {
        let shard_id = self.context_shard_id(tenant_hash);
        let marker = self.execute(ExecuteRequest {
            shard_id,
            command: Command::StringGet {
                key: memory_order_ready_key(tenant_hash),
            },
        });
        if !marker.status.ok {
            return Err(marker.status);
        }
        if matches!(marker.response, CommandResponse::Bytes { value: Some(_) }) {
            return Ok(());
        }

        let records = self.memory_read_all(tenant_hash)?;
        for page in records.chunks(MEMORY_CATALOG_PAGE_SIZE) {
            let commands: Vec<_> = page
                .iter()
                .map(|record| Command::SetAdd {
                    key: memory_order_key(tenant_hash),
                    member: memory_order_member(record.updated_at_ms, &record.id),
                })
                .collect();
            let expected = commands.len();
            let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
            memory_write_ack(&response, expected)?;
        }
        let marker = self.execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: memory_order_ready_key(tenant_hash),
                value: b"1".to_vec(),
            },
        });
        marker.status.ok.then_some(()).ok_or(marker.status)
    }

    fn memory_prepare_indexes(&self, tenant_hash: u64) -> Result<(), Status> {
        if self
            .inner
            .memory_indexes_ready
            .read()
            .map(|ready| ready.contains(&tenant_hash))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let marker = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            command: Command::StringGet {
                key: memory_identity_ready_key(tenant_hash),
            },
        });
        if !marker.status.ok {
            return Err(marker.status);
        }
        if matches!(marker.response, CommandResponse::Bytes { value: Some(ref value) } if value == MEMORY_IDENTITY_INDEX_VERSION)
        {
            self.memory_remember_indexes_ready(tenant_hash);
            return Ok(());
        }
        if matches!(marker.response, CommandResponse::Bytes { value: None }) {
            let legacy = self.execute(ExecuteRequest {
                shard_id: self.context_shard_id(tenant_hash),
                command: Command::HashLen {
                    key: memory_catalog_key(tenant_hash),
                },
            });
            if !legacy.status.ok {
                return Err(legacy.status);
            }
            if matches!(legacy.response, CommandResponse::Integer { value: 0 }) {
                let initialized = self.batch_execute(BatchExecuteRequest {
                    shard_id: self.context_shard_id(tenant_hash),
                    commands: vec![
                        Command::StringSet {
                            key: memory_order_ready_key(tenant_hash),
                            value: b"1".to_vec(),
                        },
                        Command::StringSet {
                            key: memory_identity_ready_key(tenant_hash),
                            value: MEMORY_IDENTITY_INDEX_VERSION.to_vec(),
                        },
                    ],
                });
                memory_write_ack(&initialized, 2)?;
                self.memory_remember_indexes_ready(tenant_hash);
                return Ok(());
            }
        }
        self.memory_prepare_order_index(tenant_hash)?;
        self.memory_prepare_identity_index(tenant_hash)?;
        self.memory_remember_indexes_ready(tenant_hash);
        Ok(())
    }

    fn memory_remember_indexes_ready(&self, tenant_hash: u64) {
        if let Ok(mut ready) = self.inner.memory_indexes_ready.write() {
            if ready.len() >= 4_096 {
                ready.clear();
            }
            ready.insert(tenant_hash);
        }
    }

    fn memory_forget_indexes_ready(&self, tenant_hash: u64) {
        if let Ok(mut ready) = self.inner.memory_indexes_ready.write() {
            ready.remove(&tenant_hash);
        }
    }

    fn memory_prepare_identity_index(&self, tenant_hash: u64) -> Result<(), Status> {
        let shard_id = self.context_shard_id(tenant_hash);
        let mut owns_claim = false;
        for _ in 0..300 {
            let marker = self.execute(ExecuteRequest {
                shard_id,
                command: Command::StringGet {
                    key: memory_identity_ready_key(tenant_hash),
                },
            });
            if !marker.status.ok {
                return Err(marker.status);
            }
            if matches!(marker.response, CommandResponse::Bytes { value: Some(ref value) } if value == MEMORY_IDENTITY_INDEX_VERSION)
            {
                return Ok(());
            }
            if matches!(marker.response, CommandResponse::Bytes { value: Some(ref value) } if value != b"building")
            {
                let cleared = self.execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringDelete {
                        key: memory_identity_ready_key(tenant_hash),
                    },
                });
                if !cleared.status.ok {
                    return Err(cleared.status);
                }
            }
            let claim = self.execute(ExecuteRequest {
                shard_id,
                command: Command::StringSetConditional {
                    key: memory_identity_ready_key(tenant_hash),
                    value: b"building".to_vec(),
                    ttl_ms: Some(600_000),
                    condition: StringSetCondition::IfNotExists,
                    return_old: false,
                    keep_ttl: false,
                },
            });
            if !claim.status.ok {
                return Err(claim.status);
            }
            if matches!(claim.response, CommandResponse::Integer { value: 1 }) {
                owns_claim = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if !owns_claim {
            return Err(Status::error(
                "identity_index_timeout",
                "timed out waiting for the memory identity index",
            ));
        }

        let records = self.memory_read_all(tenant_hash)?;
        let mut counts = BTreeMap::<Vec<u8>, u64>::new();
        counts.insert(MEMORY_TOTAL_MEMBER.to_vec(), records.len() as u64);
        for record in &records {
            for member in memory_identity_members(&record) {
                *counts.entry(member).or_default() += 1;
            }
        }
        let mut commands = Vec::with_capacity(MEMORY_CATALOG_PAGE_SIZE);
        for (member, count) in counts {
            commands.push(Command::ZSetAdd {
                key: memory_identity_key(tenant_hash),
                member,
                score: count as f64,
            });
            if commands.len() == MEMORY_CATALOG_PAGE_SIZE {
                let expected = commands.len();
                let response = self.batch_execute(BatchExecuteRequest {
                    shard_id,
                    commands: std::mem::take(&mut commands),
                });
                memory_write_ack(&response, expected)?;
            }
        }
        if !commands.is_empty() {
            let expected = commands.len();
            let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
            memory_write_ack(&response, expected)?;
        }
        let mut groups = BTreeMap::<(String, String, String), Vec<MemoryRecord>>::new();
        for record in records {
            groups
                .entry((
                    record.user_id.clone(),
                    record.agent_id.clone(),
                    record.run_id.clone(),
                ))
                .or_default()
                .push(record);
        }
        for group in groups.into_values() {
            for page in group.chunks(MEMORY_CATALOG_PAGE_SIZE) {
                let posting_field = page[0].id.clone();
                let mut commands = vec![
                    Command::HashSet {
                        key: memory_identity_postings_key(tenant_hash),
                        field: posting_field.clone(),
                        value: memory_identity_posting(page),
                    },
                    memory_node_lookup_write(tenant_hash, page.iter()),
                ];
                commands.extend(memory_identity_members(&page[0]).into_iter().map(|member| {
                    Command::SetAdd {
                        key: memory_identity_records_key(tenant_hash, &member),
                        member: posting_field.as_bytes().to_vec(),
                    }
                }));
                let expected = commands.len();
                let response = self.batch_execute(BatchExecuteRequest { shard_id, commands });
                memory_write_ack(&response, expected)?;
            }
        }
        let marker = self.execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: memory_identity_ready_key(tenant_hash),
                value: MEMORY_IDENTITY_INDEX_VERSION.to_vec(),
            },
        });
        marker.status.ok.then_some(()).ok_or(marker.status)
    }

    fn memory_order_page(&self, tenant_hash: u64, after: &[u8], limit: usize) -> MemoryRecordPage {
        let response = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            command: Command::SetScan {
                key: memory_order_key(tenant_hash),
                after: after.to_vec(),
                limit: limit.clamp(1, 10_000),
            },
        });
        let members = match response.response {
            CommandResponse::Members { members } => members,
            _ => return MemoryRecordPage::default(),
        };
        let candidate_count = members.len();
        let next_after = members.last().cloned().unwrap_or_default();
        let keyed_ids: Vec<(Vec<u8>, String)> = members
            .iter()
            .filter_map(|member| memory_id_from_order_member(member).map(|id| (member.clone(), id)))
            .collect();
        let ids: Vec<String> = keyed_ids.iter().map(|(_, id)| id.clone()).collect();
        let records = self.memory_read_many(tenant_hash, &ids);
        let rows = keyed_ids
            .into_iter()
            .zip(records)
            .filter_map(|((member, _), record)| record.map(|record| (member, record)))
            .collect();
        MemoryRecordPage {
            after: next_after,
            rows,
            candidate_count,
        }
    }

    fn memory_identity_page(
        &self,
        tenant_hash: u64,
        identity_member: &[u8],
        after: &[u8],
        limit: usize,
    ) -> (Vec<u8>, Vec<(Vec<u8>, MemoryRecord)>) {
        self.memory_identity_postings_page(
            tenant_hash,
            identity_member,
            after,
            limit.clamp(1, MEMORY_IDENTITY_POSTINGS_PER_PAGE),
        )
    }

    fn memory_identity_postings_page(
        &self,
        tenant_hash: u64,
        identity_member: &[u8],
        after: &[u8],
        limit: usize,
    ) -> (Vec<u8>, Vec<(Vec<u8>, MemoryRecord)>) {
        self.memory_identity_postings_page_with_budget(
            tenant_hash,
            identity_member,
            after,
            limit,
            None,
        )
        .map(|page| (page.after, page.rows))
        .unwrap_or_default()
    }

    fn memory_identity_postings_page_with_budget(
        &self,
        tenant_hash: u64,
        identity_member: &[u8],
        after: &[u8],
        limit: usize,
        budget: Option<usize>,
    ) -> Option<MemoryRecordPage> {
        let response = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            command: Command::SetScan {
                key: memory_identity_records_key(tenant_hash, identity_member),
                after: after.to_vec(),
                limit: limit.clamp(1, 10_000),
            },
        });
        if budget.is_some() && !response.status.ok {
            return None;
        }
        let postings = match response.response {
            CommandResponse::Members { members } => members,
            _ => return None,
        };
        if postings.is_empty() {
            return Some(MemoryRecordPage::default());
        }
        let next_after = postings.last().cloned().unwrap_or_default();
        let fields = postings
            .iter()
            .filter_map(|posting| std::str::from_utf8(posting).ok().map(str::to_owned))
            .collect::<Vec<_>>();
        if budget.is_some() && fields.len() != postings.len() {
            return None;
        }
        let response = self.execute(ExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            command: Command::HashMultiGet {
                key: memory_identity_postings_key(tenant_hash),
                fields,
            },
        });
        if budget.is_some() && !response.status.ok {
            return None;
        }
        let values = match response.response {
            CommandResponse::Values { values } => values,
            _ => return None,
        };
        let candidate_count = values
            .iter()
            .flatten()
            .fold(0usize, |count, value| count.saturating_add(value.len() / 8));
        if let Some(budget) = budget {
            if candidate_count > budget
                || values.len() != postings.len()
                || values
                    .iter()
                    .any(|value| value.as_ref().map_or(true, |value| value.len() % 8 != 0))
            {
                return None;
            }
        }
        let keyed_ids = postings
            .into_iter()
            .zip(values)
            .flat_map(|(posting, value)| {
                value
                    .map(|value| {
                        memory_ids_from_identity_posting(tenant_hash, &value)
                            .into_iter()
                            .map(|id| (posting.clone(), id))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        let ids = keyed_ids
            .iter()
            .map(|(_, id)| id.clone())
            .collect::<Vec<_>>();
        let records = self.memory_read_many(tenant_hash, &ids);
        let rows = keyed_ids
            .into_iter()
            .zip(records)
            .filter_map(|((posting, _), record)| record.map(|record| (posting, record)))
            .collect();
        Some(MemoryRecordPage {
            after: next_after,
            rows,
            candidate_count,
        })
    }

    #[cfg(test)]
    fn memory_read_one(&self, tenant_hash: u64, memory_id: &str) -> Option<MemoryRecord> {
        self.memory_read_one_checked(tenant_hash, memory_id).ok().flatten()
    }

    fn memory_read_many(&self, tenant_hash: u64, ids: &[String]) -> Vec<Option<MemoryRecord>> {
        if ids.is_empty() {
            return Vec::new();
        }
        let response = self.batch_execute(BatchExecuteRequest {
            shard_id: self.context_shard_id(tenant_hash),
            commands: vec![
                Command::HashMultiGet {
                    key: memory_catalog_key(tenant_hash),
                    fields: ids.to_vec(),
                },
                Command::HashMultiGet {
                    key: memory_tombstone_key(tenant_hash),
                    fields: ids.to_vec(),
                },
            ],
        });
        let mut responses = response.responses.into_iter();
        let mut records = match responses.next().map(|response| response.response) {
            Some(CommandResponse::Values { values }) => values
                .into_iter()
                .map(|value| value.and_then(|bytes| MemoryRecord::decode(&bytes)))
                .collect::<Vec<_>>(),
            _ => vec![None; ids.len()],
        };
        if let Some(CommandResponse::Values { values }) =
            responses.next().map(|response| response.response)
        {
            for (record, tombstone) in records.iter_mut().zip(values) {
                if tombstone.is_some() {
                    *record = None;
                }
            }
        }
        records
    }

    fn memory_read_all(&self, tenant_hash: u64) -> Result<Vec<MemoryRecord>, Status> {
        let shard_id = self.context_shard_id(tenant_hash);
        let response = self.batch_execute(BatchExecuteRequest {
            shard_id,
            commands: vec![
                Command::HashGetAll {
                    key: memory_catalog_key(tenant_hash),
                },
                Command::HashGetAll {
                    key: memory_tombstone_key(tenant_hash),
                },
            ],
        });
        memory_write_ack(&response, 2)?;
        let mut responses = response.responses.into_iter();
        let catalog = responses.next().map(|response| response.response);
        let tombstones = match responses.next().map(|response| response.response) {
            Some(CommandResponse::HashEntries { entries }) => entries
                .into_iter()
                .map(|(field, _)| field)
                .collect::<HashSet<_>>(),
            _ => return Err(Status::error("invalid_memory_catalog_response", "expected catalog tombstones")),
        };
        match catalog {
            Some(CommandResponse::HashEntries { entries }) => entries
                .into_iter()
                .filter(|(field, _)| !tombstones.contains(field))
                .map(|(id, bytes)| {
                    let record = MemoryRecord::decode(&bytes).ok_or_else(||
                        Status::error("invalid_memory_catalog_record", "catalog record cannot be decoded"))?;
                    if record.id != id || tenant_hash_from_memory_id(&id) != Some(tenant_hash) {
                        return Err(Status::error("invalid_memory_catalog_identity", "catalog record identity mismatch"));
                    }
                    Ok(record)
                })
                .collect(),
            _ => Err(Status::error("invalid_memory_catalog_response", "expected catalog entries")),
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
#[path = "memory_identity_page_tests.rs"]
mod identity_page_tests;

#[cfg(test)]
#[path = "memory_filtered_list_tests.rs"]
mod filtered_list_tests;

#[cfg(test)]
#[path = "memory_update_provider_tests.rs"]
mod update_provider_tests;

#[cfg(test)]
#[path = "memory_explicit_tests.rs"]
mod explicit_tests;

#[cfg(test)]
#[path = "memory_read_error_tests.rs"]
mod read_error_tests;

#[cfg(test)]
#[path = "memory_write_ack_tests.rs"]
mod write_ack_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_identity_scan_pages_beyond_ten_thousand() {
        let mut cursor = Vec::new();
        let mut visited = 0;
        for start in (0..10_001).step_by(MEMORY_CATALOG_PAGE_SIZE) {
            let mut members = Vec::new();
            for index in start..(start + MEMORY_CATALOG_PAGE_SIZE).min(10_001) {
                members.push(format!("u{index:08}").into_bytes());
                members.push(b"1".to_vec());
            }
            let (next, identities) =
                memory_identity_scan_page(CommandResponse::Members { members }, &cursor).unwrap();
            assert!(next > cursor);
            assert!(identities.len() <= MEMORY_CATALOG_PAGE_SIZE);
            visited += identities.len();
            cursor = next;
        }
        assert_eq!(visited, 10_001);
        let (next, identities) = memory_identity_scan_page(
            CommandResponse::Members {
                members: Vec::new(),
            },
            &cursor,
        )
        .unwrap();
        assert_eq!(next, cursor);
        assert!(identities.is_empty());
    }

    #[test]
    fn memory_identity_scan_rejects_malformed_or_stalled_pages() {
        assert!(memory_identity_scan_page(CommandResponse::Empty, b"").is_err());
        assert!(memory_identity_scan_page(
            CommandResponse::Members {
                members: vec![b"a".to_vec()],
            },
            b""
        )
        .is_err());
        assert!(memory_identity_scan_page(
            CommandResponse::Members {
                members: vec![b"a".to_vec(), b"1".to_vec()],
            },
            b"a"
        )
        .is_err());
        let (cursor, identities) = memory_identity_scan_page(
            CommandResponse::Members {
                members: vec![MEMORY_TOTAL_MEMBER.to_vec(), b"5".to_vec()],
            },
            b"",
        )
        .unwrap();
        assert_eq!(cursor, MEMORY_TOTAL_MEMBER);
        assert!(identities.is_empty());
    }

    #[test]
    fn memory_ids_carry_the_tenant_route() {
        let id = memory_id(42, 100, 7, "remember this");
        assert_eq!(tenant_hash_from_memory_id(&id), Some(42));
        assert_eq!(id.len(), 33);
    }

    #[test]
    fn memory_order_members_sort_newest_first_and_round_trip_ids() {
        let newest = memory_order_member(300, "memory-new");
        let middle = memory_order_member(200, "memory-middle");
        let oldest = memory_order_member(100, "memory-old");
        assert!(newest < middle);
        assert!(middle < oldest);
        assert_eq!(
            memory_id_from_order_member(&middle).as_deref(),
            Some("memory-middle")
        );
        assert_eq!(memory_id_from_order_member(b"short"), None);
    }

    #[test]
    fn memory_identity_members_are_compact_and_typed() {
        let record = MemoryRecord {
            id: "id".into(),
            context_ref: "id".into(),
            memory: "m".into(),
            metadata: Value::Null,
            tenant_id: "tenant".into(),
            account_id: "account".into(),
            user_id: "alice".into(),
            agent_id: "codex".into(),
            run_id: "run-1".into(),
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        let members = memory_identity_members(&record);
        let Command::HashMultiSet { key, entries } =
            memory_catalog_write(42, std::iter::once(&record))
        else {
            panic!("catalog batch must use one multi-field write");
        };
        assert_eq!(key, memory_catalog_key(42));
        assert_eq!(entries, vec![(record.id.clone(), record.encode())]);
        assert_eq!(
            members,
            [b"ualice".to_vec(), b"acodex".to_vec(), b"rrun-1".to_vec()]
        );
        assert_eq!(
            memory_identity_result(&members[0]),
            Some(json!({"type": "user", "name": "alice"}))
        );
    }

    #[test]
    fn identity_postings_pack_ids_into_eight_bytes_each() {
        let records = [
            MemoryRecord {
                id: "000000000000002a-0000000000000001".into(),
                context_ref: String::new(),
                memory: String::new(),
                metadata: Value::Null,
                tenant_id: String::new(),
                account_id: String::new(),
                user_id: String::new(),
                agent_id: String::new(),
                run_id: String::new(),
                created_at_ms: 0,
                updated_at_ms: 0,
            },
            MemoryRecord {
                id: "000000000000002a-0000000000000002".into(),
                context_ref: String::new(),
                memory: String::new(),
                metadata: Value::Null,
                tenant_id: String::new(),
                account_id: String::new(),
                user_id: String::new(),
                agent_id: String::new(),
                run_id: String::new(),
                created_at_ms: 0,
                updated_at_ms: 0,
            },
        ];
        let posting = memory_identity_posting(&records);
        assert_eq!(posting.len(), 16);
        assert_eq!(
            memory_ids_from_identity_posting(42, &posting),
            records.map(|record| record.id).to_vec()
        );
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
        ]), true)
        .unwrap();
        assert_eq!(
            messages,
            vec![
                ("user".into(), "one".into(), None),
                ("assistant".into(), "two".into(), None)
            ]
        );
    }

    #[test]
    fn scoped_filters_do_not_cross_identity_dimensions() {
        let record = MemoryRecord {
            id: "id".into(),
            context_ref: "id".into(),
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
            context_ref: "id~000000000000000b".into(),
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

    #[test]
    fn context_revisions_map_to_the_stable_memory_id() {
        assert_eq!(
            memory_catalog_id_from_context_ref(
                "0123456789abcdef-fedcba9876543210~000000000000002a"
            ),
            "0123456789abcdef-fedcba9876543210"
        );
        assert_eq!(
            memory_context_ref("0123456789abcdef-fedcba9876543210", 42),
            "0123456789abcdef-fedcba9876543210~000000000000002a"
        );
    }
}
