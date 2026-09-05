// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! High-level `/context/*` routing for the service proxy (the "direct network
//! path"), split fast-ack / batched:
//!
//! * `POST /context/ingest`  -- FAST raw store. Each message is written as a
//!   lightweight `raw_event` hash record via the datanode's `/ingest/batch`
//!   (a plain command store, ~3ms), with NO inline extraction. This is the hot
//!   path `/v1/ingest` uses so ingest QPS is bounded by a raw write, not by
//!   embedding + summary generation (~218ms).
//! * `POST /context/extract` -- BATCHED extraction. Builds a
//!   `ContextIngestExtractRequest` and forwards to the datanode's
//!   `/context/ingest_extract` (full node/event/summary/embedding fanout). Used
//!   on commit / finalize. When the request carries no messages/sources it reads
//!   the buffered `raw_event` records back (HGETALL over `/execute`) and extracts
//!   those, then clears the buffer.
//! * `POST /context/retrieve` -- forwards a `ContextRetrieveRequest`.
//!
//! In every case the proxy computes `tenant_hash` + `shard_id` reusing the SAME
//! routing `/execute` uses (`crate::client::shard_id_for_key`) and looks up the
//! owning datanode through the metaserver topology. The `/v1` gateway sends raw
//! identifiers only -- the proxy owns all hashing.
use super::*;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::client::shard_id_for_key;
use crate::http::{post_json_with_options, request_bytes_with_options};
use crate::types::{Command, CommandResponse, ExecuteRequest, ExecuteResponse};

/// Tenant scope carried in a high-level `/context/*` request. The proxy owns
/// hashing; callers (the `/v1` gateway) send raw identifiers only.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyContextScope {
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub account_id: String,
    // Reserved for future user-scoped routing; accepted on the wire today so the
    // gateway can forward it unchanged, but tenant hashing keys on account+tenant.
    #[allow(dead_code)]
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub session_id: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyContextMessage {
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub timestamp_ms: Option<u64>,
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyContextIngestRequest {
    #[serde(default)]
    pub scope: ProxyContextScope,
    #[serde(default)]
    pub messages: Vec<ProxyContextMessage>,
    /// Pre-shaped low-level sources (`ContextExtractRequest` json). `shard_id`
    /// and `tenant_hash` are re-stamped by the proxy.
    #[serde(default)]
    pub sources: Vec<Value>,
    /// Alias for `sources` accepted from `/v1` callers who post `records`.
    #[serde(default)]
    pub records: Vec<Value>,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub start_time_ms: u64,
    #[serde(default)]
    pub end_time_ms: u64,
    #[serde(default)]
    pub max_events: Option<usize>,
    #[serde(default)]
    pub provider: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyContextRetrieveRequest {
    #[serde(default)]
    pub scope: ProxyContextScope,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub start_time_ms: u64,
    #[serde(default)]
    pub end_time_ms: u64,
    #[serde(default)]
    pub max_events: Option<usize>,
    #[serde(default)]
    pub node_hashes: Vec<u64>,
    #[serde(default)]
    pub provider: Option<Value>,
}

/// Scope hash used for context tenancy. Matches the Python control-plane
/// `identity_hashes` (`tools/matrixark_mcp_core_identity.py`): the low 8 bytes
/// of `sha256("{account_id}:{tenant_id}")`, masked to a positive i63.
pub(super) fn context_tenant_hash(scope: &ProxyContextScope) -> u64 {
    let account_id = if scope.account_id.is_empty() {
        "acct_local"
    } else {
        scope.account_id.as_str()
    };
    let tenant_id = if scope.tenant_id.is_empty() {
        "tenant_local_agent"
    } else {
        scope.tenant_id.as_str()
    };
    stable_scope_hash(&format!("{account_id}:{tenant_id}"))
}

fn stable_scope_hash(value: &str) -> u64 {
    let digest = Sha256::digest(value.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes) & 0x7FFF_FFFF_FFFF_FFFF
}

fn wide_window_end(now: u64) -> u64 {
    now.saturating_add(86_400_000)
}

fn scope_session(scope: &ProxyContextScope) -> String {
    if scope.session_id.is_empty() {
        "default".to_string()
    } else {
        scope.session_id.clone()
    }
}

/// Buffer key holding the per-scope `raw_event` records awaiting extraction.
fn rawlog_key(tenant_hash: u64, session: &str) -> String {
    // Sized up front rather than left to `format!`, which grows into its buffer. A u64 is at
    // most 20 digits, and the two literals are 15 and 1.
    let mut key = String::with_capacity(16 + 20 + 1 + session.len());
    key.push_str("context:rawlog:");
    push_fixed_width(&mut key, tenant_hash, 0);
    key.push(':');
    key.push_str(session);
    key
}

/// Append `value` zero-padded to `width`, the way `{value:0width$}` would.
///
/// Fixed width is what makes lexicographic order equal arrival order for these keys, so the
/// padding is part of the contract rather than cosmetic.
fn push_fixed_width(out: &mut String, value: u64, width: usize) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut rest = value;
    loop {
        i -= 1;
        buf[i] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let digits = &buf[i..];
    for _ in digits.len()..width {
        out.push('0');
    }
    // ASCII digits by construction.
    out.push_str(std::str::from_utf8(digits).unwrap_or("0"));
}

fn source_from_fields(
    shard_id: ShardId,
    tenant_hash: u64,
    source_id: String,
    title: String,
    body: String,
    timestamp_ms: u64,
) -> Value {
    json!({
        "shard_id": shard_id,
        "tenant_hash": tenant_hash,
        "source_kind": "chat",
        "source_id": source_id,
        "title": title,
        "body": body,
        "timestamp_ms": timestamp_ms,
    })
}

/// One buffered raw event, as it is stored.
///
/// Field names and order match what `json!` produced here before, and what every reader
/// downstream expects. Borrowed so serialising copies nothing that the request already owns.
#[derive(serde::Serialize)]
struct RawEventRecord<'a> {
    // ALPHABETICAL, and that is load-bearing. `json!` renders through a `serde_json::Map`,
    // which is a BTreeMap, so its output is key-sorted; serde renders a struct in DECLARATION
    // order. Declaring these out of order silently changes the stored bytes of every buffered
    // event. `a_raw_event_serialises_exactly_as_it_did_when_it_went_through_a_value` fails if
    // this order is disturbed.
    body: &'a str,
    record_type: &'a str,
    role: &'a str,
    timestamp_ms: u64,
    title: &'a str,
}

impl ProxyService {
    /// Route a tenant to its owning shard using the same key hashing as
    /// `/execute` (`shard_id_for_key`). The tenant hash string is the routing
    /// key so every request for a tenant lands on one shard.
    pub(super) fn context_shard_id(&self, tenant_hash: u64) -> ShardId {
        let first = self.with_options(|options| options.context_first_shard_id);
        shard_id_for_key(
            &tenant_hash.to_string(),
            first,
            self.effective_context_shard_count(),
            first,
        )
    }

    fn context_http_options(&self) -> HttpRequestOptions {
        self.with_options(|options| HttpRequestOptions {
            connect_timeout_ms: options.connect_timeout_ms.max(1_000),
            io_timeout_ms: options.context_io_timeout_ms,
            max_retries: options.max_retries,
        })
    }

    /// Resolve the datanode that owns `shard_id` through the client's route cache, or
    /// an HTTP error response ready to return.
    fn context_shard_addr(&self, shard_id: ShardId) -> Result<String, (u16, Vec<u8>)> {
        // Drop routes the metaserver has moved, then resolve through the proxy's shared
        // client: the same route cache, topology invalidation and continuous-failure check
        // that every command entry point uses.
        //
        // This used to be a direct `/shards/{id}` GET, which is the one routing path in the
        // proxy that was written by hand, and so the one that had none of the above. The
        // invalidation is not optional here: a proxy fronting the context gateway serves
        // only these three routes and never calls `execute`, so nothing else on it would
        // ever notice a shard had moved.
        self.invalidate_cached_routes_if_meta_changed();
        match self.client().shard_primary_addr(shard_id, false) {
            Ok(server_addr) => Ok(server_addr),
            Err(err) => {
                self.inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .metaserver_errors += 1;
                Err(crate::http::json_response(
                    502,
                    &Status::error(
                        "metaserver_error",
                        format!("shard {shard_id} route lookup failed: {err}"),
                    ),
                ))
            }
        }
    }

    fn context_forward_to(&self, server_addr: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
        match request_bytes_with_options(
            server_addr,
            "POST",
            path,
            body,
            "application/json",
            self.context_http_options(),
        ) {
            Ok(raw) => (200, raw),
            Err(err) => {
                self.inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .backend_errors += 1;
                // Report the failure against the address so a datanode that keeps failing
                // stops being served from the route cache. Forwarding by hand means this
                // accounting has to be done by hand too -- without it the continuous-failure
                // check only ever saw command traffic, and never the gateway's.
                self.client().note_backend_failure(server_addr);
                crate::http::json_response(
                    502,
                    &Status::error(
                        "backend_error",
                        format!("context forward to {server_addr}{path} failed: {err}"),
                    ),
                )
            }
        }
    }

    /// FAST path: buffer the messages as lightweight `raw_event` records in ONE
    /// routed `/execute` `HashMultiSet` (a single raw write, NO extraction, no
    /// embeddings/summaries). Reuses the proxy's cached-route execute path, so it
    /// is one datanode write regardless of message count.
    pub(super) fn context_ingest(&self, request: ProxyContextIngestRequest) -> (u16, Vec<u8>) {
        self.inner
            .context_ingest_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _admitted = match self.admit_context(&request.scope, true) {
            Ok(guard) => guard,
            Err(response) => return response,
        };
        let tenant_hash = context_tenant_hash(&request.scope);
        let shard_id = self.context_shard_id(tenant_hash);
        let session = scope_session(&request.scope);
        let key = rawlog_key(tenant_hash, &session);
        let now = now_ms();
        // Kept inside 8 digits so the field stays fixed-width and lexicographic order
        // remains arrival order. Wrapping needs a hundred million ingests AND a same-
        // millisecond collision with the exact sequence a wrap apart.
        let call = self
            .inner
            .context_ingest_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % 100_000_000;

        let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
        for (idx, message) in request.messages.iter().enumerate() {
            let timestamp_ms = message.timestamp_ms.unwrap_or(now);
            let title = message
                .title
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| {
                    if message.role.is_empty() {
                        "message".to_string()
                    } else {
                        message.role.clone()
                    }
                });
            // Orders raw events by (timestamp, call, index within the call), all
            // fixed-width so lexicographic order is arrival order. The call component is
            // what stops two ingests in the same millisecond writing the same field and
            // one silently overwriting the other; `{idx}` alone restarts at zero per call.
            // 20 + 1 + 8 + 1 + 6 -- every part is fixed width, so the length is known and the
            // string is built in one allocation instead of grown into.
            let mut field = String::with_capacity(36);
            push_fixed_width(&mut field, timestamp_ms, 20);
            field.push(':');
            push_fixed_width(&mut field, call, 8);
            field.push(':');
            push_fixed_width(&mut field, idx as u64, 6);
            // Serialised straight from a borrowed view rather than through `json!`, which
            // builds a whole `Value` -- a map, a `String` for each of the five keys, and a
            // `Value::String` copy of role, title and body -- only to render it and drop it.
            // The field names and their order are the same, because these records are read
            // back by everything downstream.
            let value = serde_json::to_string(&RawEventRecord {
                body: &message.content,
                record_type: "raw_event",
                role: &message.role,
                timestamp_ms,
                title: &title,
            })
            .unwrap_or_default();
            entries.push((field, value.into_bytes()));
        }
        if entries.is_empty() {
            // Nothing to buffer (e.g. pre-shaped records only) -- still a fast ack.
            return crate::http::json_response(200, &Status::ok());
        }
        let server_addr = match self.context_shard_addr(shard_id) {
            Ok(addr) => addr,
            Err(resp) => return resp,
        };
        // Forward the raw store straight to the owning datanode's /execute
        // (bypassing the proxy's command admission policy, exactly as the
        // extract/retrieve context routes forward) so a single HashMultiSet
        // write is the whole cost of a fast-ack ingest.
        let request = ExecuteRequest {
            shard_id,
            command: Command::HashMultiSet { key, entries },
        };
        let body = serde_json::to_vec(&request).unwrap_or_default();
        self.context_forward_to(&server_addr, "/execute", &body)
    }

    /// BATCHED path: build a `ContextIngestExtractRequest` and forward to the
    /// datanode's `/context/ingest_extract` (full extraction). Used on commit /
    /// finalize. When no messages/sources are supplied, replay the buffered
    /// `raw_event` records for the scope and extract those.
    pub(super) fn context_extract(&self, request: ProxyContextIngestRequest) -> (u16, Vec<u8>) {
        self.inner
            .context_extract_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _admitted = match self.admit_context(&request.scope, true) {
            Ok(guard) => guard,
            Err(response) => return response,
        };
        let tenant_hash = context_tenant_hash(&request.scope);
        let shard_id = self.context_shard_id(tenant_hash);
        let session = scope_session(&request.scope);
        let now = now_ms();
        let server_addr = match self.context_shard_addr(shard_id) {
            Ok(addr) => addr,
            Err(resp) => return resp,
        };

        let mut sources: Vec<Value> = Vec::new();
        for (idx, message) in request.messages.iter().enumerate() {
            let timestamp_ms = message.timestamp_ms.unwrap_or(now + idx as u64);
            let title = message
                .title
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| {
                    if message.role.is_empty() {
                        "message".to_string()
                    } else {
                        message.role.clone()
                    }
                });
            sources.push(source_from_fields(
                shard_id,
                tenant_hash,
                format!("chat:{tenant_hash}:{session}:{idx}"),
                title,
                message.content.clone(),
                timestamp_ms,
            ));
        }
        for extra in request.sources.iter().chain(request.records.iter()) {
            sources.push(extra.clone());
        }

        // Commit with no inline messages: replay the buffered raw events.
        let mut replayed_buffer = false;
        if sources.is_empty() {
            sources = self.context_replay_rawlog(&server_addr, shard_id, tenant_hash, &session);
            replayed_buffer = !sources.is_empty();
        }

        let end_time_ms = if request.end_time_ms == 0 {
            wide_window_end(now)
        } else {
            request.end_time_ms
        };
        let mut payload = json!({
            "shard_id": shard_id,
            "tenant_hash": tenant_hash,
            "sources": sources,
            "query": request.query,
            "start_time_ms": request.start_time_ms,
            "end_time_ms": end_time_ms,
        });
        if let Some(max_events) = request.max_events {
            payload["max_events"] = json!(max_events);
        }
        if let Some(provider) = request.provider {
            payload["provider"] = provider;
        }
        let body = serde_json::to_vec(&payload).unwrap_or_default();
        let response = self.context_forward_to(&server_addr, "/context/ingest_extract", &body);
        // Clear the buffer only after a successful replay-driven extraction.
        if replayed_buffer && response.0 == 200 {
            self.context_clear_rawlog(&server_addr, shard_id, tenant_hash, &session);
        }
        response
    }

    /// Forward a `ContextRetrieveRequest` to the owning datanode.
    pub(super) fn context_retrieve(&self, request: ProxyContextRetrieveRequest) -> (u16, Vec<u8>) {
        self.inner
            .context_retrieve_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _admitted = match self.admit_context(&request.scope, false) {
            Ok(guard) => guard,
            Err(response) => return response,
        };
        let tenant_hash = context_tenant_hash(&request.scope);
        let shard_id = self.context_shard_id(tenant_hash);
        let now = now_ms();
        let end_time_ms = if request.end_time_ms == 0 {
            wide_window_end(now)
        } else {
            request.end_time_ms
        };
        let mut payload = json!({
            "shard_id": shard_id,
            "tenant_hash": tenant_hash,
            "query": request.query,
            "node_hashes": request.node_hashes,
            "start_time_ms": request.start_time_ms,
            "end_time_ms": end_time_ms,
        });
        if let Some(max_events) = request.max_events {
            payload["max_events"] = json!(max_events);
        }
        if let Some(provider) = request.provider {
            payload["provider"] = provider;
        }
        let body = serde_json::to_vec(&payload).unwrap_or_default();
        let server_addr = match self.context_shard_addr(shard_id) {
            Ok(addr) => addr,
            Err(resp) => return resp,
        };
        self.context_forward_to(&server_addr, "/context/retrieve", &body)
    }

    /// Read the buffered `raw_event` records (HGETALL) back into extract sources.
    /// Forwards straight to the owning datanode (bypassing admission policy).
    fn context_replay_rawlog(
        &self,
        server_addr: &str,
        shard_id: ShardId,
        tenant_hash: u64,
        session: &str,
    ) -> Vec<Value> {
        let request = ExecuteRequest {
            shard_id,
            command: Command::HashGetAll {
                key: rawlog_key(tenant_hash, session),
            },
        };
        let response: ExecuteResponse = match post_json_with_options(
            server_addr,
            "/execute",
            &request,
            self.context_http_options(),
        ) {
            Ok(response) => response,
            Err(_) => return Vec::new(),
        };
        let CommandResponse::HashEntries { mut entries } = response.response else {
            return Vec::new();
        };
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let mut sources = Vec::new();
        for (field, value) in entries {
            let parsed: Value = match serde_json::from_slice(&value) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            let body = parsed
                .get("body")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if body.is_empty() {
                continue;
            }
            let title = parsed
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or("message")
                .to_string();
            let timestamp_ms = parsed
                .get("timestamp_ms")
                .and_then(Value::as_u64)
                .unwrap_or_else(now_ms);
            sources.push(source_from_fields(
                shard_id,
                tenant_hash,
                format!("chat:{tenant_hash}:{session}:{field}"),
                title,
                body,
                timestamp_ms,
            ));
        }
        sources
    }

    /// Best-effort clear of the per-scope raw-event buffer after extraction.
    fn context_clear_rawlog(
        &self,
        server_addr: &str,
        shard_id: ShardId,
        tenant_hash: u64,
        session: &str,
    ) {
        let request = ExecuteRequest {
            shard_id,
            command: Command::CommonDelete {
                key: rawlog_key(tenant_hash, session),
            },
        };
        let _ = post_json_with_options::<ExecuteRequest, ExecuteResponse>(
            server_addr,
            "/execute",
            &request,
            self.context_http_options(),
        );
    }
}

#[cfg(test)]
mod raw_event_record_tests {
    use super::*;
    use serde_json::json;

    /// The borrowed form serialises byte-for-byte what `json!` did.
    ///
    /// These records are stored and read back by everything downstream, so the shape is a
    /// contract, not an implementation detail. `json!` built a whole `Value` to render it;
    /// this asserts the cheaper path produces the identical string, including field ORDER,
    /// which serde takes from declaration order and a `json!` object takes from its literal.
    #[test]
    fn a_raw_event_serialises_exactly_as_it_did_when_it_went_through_a_value() {
        let cases: [(&str, &str, &str, u64); 4] = [
            ("user", "message", "hello", 1_700_000_000_000),
            ("", "", "", 0),
            // Characters that force escaping, so this covers the encoder and not just ASCII.
            ("assistant", "a \"quoted\" title", "line\nbreak \\ backslash", 42),
            ("tool", "check \u{2713} and \u{1F600}", "tab\there", u64::MAX),
        ];
        for (role, title, body, ts) in cases {
            let borrowed = serde_json::to_string(&RawEventRecord {
                record_type: "raw_event",
                role,
                title,
                body,
                timestamp_ms: ts,
            })
            .expect("borrowed form serialises");

            let through_value = json!({
                "record_type": "raw_event",
                "role": role,
                "title": title,
                "body": body,
                "timestamp_ms": ts,
            })
            .to_string();

            assert_eq!(
                borrowed, through_value,
                "the stored shape changed for role={role:?} title={title:?}
  borrowed: {borrowed}
  value:    {through_value}"
            );
        }
    }
}

#[cfg(test)]
mod ingest_key_tests {
    use super::*;

    /// The hand-built keys are byte-identical to the `format!` they replaced.
    ///
    /// Both are ordering contracts, not cosmetics: raw events are read back in lexicographic
    /// order and that is only arrival order while every component stays fixed width. A key that
    /// merely looks right would reorder history.
    #[test]
    fn the_ingest_keys_are_what_format_produced() {
        for tenant_hash in [0u64, 1, 42, 9_999_999, u64::MAX] {
            for session in ["default", "", "s1", "a-longer-session-id"] {
                assert_eq!(
                    rawlog_key(tenant_hash, session),
                    format!("context:rawlog:{tenant_hash}:{session}"),
                    "rawlog key changed for hash={tenant_hash} session={session:?}"
                );
            }
        }

        for timestamp_ms in [0u64, 1, 1_700_000_000_000, u64::MAX] {
            for call in [0u64, 7, 99_999_999] {
                for idx in [0usize, 5, 999_999] {
                    let mut built = String::with_capacity(36);
                    push_fixed_width(&mut built, timestamp_ms, 20);
                    built.push(':');
                    push_fixed_width(&mut built, call, 8);
                    built.push(':');
                    push_fixed_width(&mut built, idx as u64, 6);

                    assert_eq!(
                        built,
                        format!("{timestamp_ms:020}:{call:08}:{idx:06}"),
                        "ordering field changed for ts={timestamp_ms} call={call} idx={idx}"
                    );
                }
            }
        }

        // A value wider than its field is NOT truncated -- `{:020}` does not truncate either,
        // and silently dropping high digits would collapse distinct keys onto one.
        let mut wide = String::new();
        push_fixed_width(&mut wide, u64::MAX, 3);
        assert_eq!(wide, u64::MAX.to_string(), "a wide value must keep every digit");
    }
}
