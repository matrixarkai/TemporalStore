use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use temporalstore_rust::{
    extract_context, inject_context, ContextExtractRequest, ContextInjectRequest,
    ContextModelProviderConfig, ContextRetrieveRequest, ContextSourceKind, ContextTier,
    TemporalEngine,
};

#[derive(Debug, Clone)]
struct Args {
    agent_name: String,
    event: String,
    root: PathBuf,
    event_log: PathBuf,
    account_id: String,
    tenant_id: String,
    user_id: String,
    session_id: String,
    query: String,
    max_context_tokens: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SessionIndex {
    sessions: BTreeMap<String, Vec<u64>>,
}

fn main() {
    let args = parse_args();
    let payload = read_payload();
    let text = payload_text(&payload)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| (!args.query.trim().is_empty()).then(|| args.query.clone()))
        .unwrap_or_default();

    if text.is_empty() && !matches!(args.event.as_str(), "Stop" | "PostCompact" | "SubagentStop") {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "status": "skipped",
                "reason": "empty hook payload",
                "event": args.event,
            }))
            .expect("skip report should serialize")
        );
        return;
    }

    fs::create_dir_all(&args.root).expect("hook root should be creatable");
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        args.root.join("cache"),
        args.root.join("pages"),
        args.root.join("indexes"),
    );
    engine.load_shard(1);

    let tenant_hash = stable_hash64(&format!(
        "{}:{}:{}:{}",
        args.account_id, args.tenant_id, args.user_id, args.session_id
    ));
    let now_ms = now_ms();
    let source_id = format!(
        "{}:{}:{}:{:016x}",
        args.agent_name,
        args.session_id,
        args.event,
        stable_hash64(&text)
    );

    let mut ingest_status = json!({});
    let mut extracted_node_hash = None;
    if !text.is_empty() {
        let extract = extract_context(
            &engine,
            ContextExtractRequest {
                shard_id: 1,
                tenant_hash,
                source_kind: source_kind_for_event(&args.event),
                source_id: source_id.clone(),
                title: format!("{} {} {}", args.agent_name, args.event, args.session_id),
                body: format!(
                    "agent={}; role={}: {}",
                    args.agent_name,
                    role_for_event(&args.event),
                    text
                ),
                timestamp_ms: now_ms,
                provider: ContextModelProviderConfig::default(),
            },
        );
        ingest_status = json!({
            "status": if extract.status.ok { "accepted" } else { "failed" },
            "code": extract.status.code,
            "message": extract.status.message,
            "node_hash": extract.node.node_hash,
            "event_id_hash": extract.event.event_id_hash,
            "event_time_ms": extract.event.event_time_ms,
            "node_uri": extract.node_uri,
            "event_uri": extract.event_uri,
            "source_id": source_id,
        });
        if extract.status.ok && extract.node.node_hash != 0 {
            extracted_node_hash = Some(extract.node.node_hash);
            remember_session_node(
                &args.root,
                &args.agent_name,
                &args.session_id,
                extract.node.node_hash,
            );
        }
    }

    let mut node_hashes = session_nodes(&args.root, &args.agent_name, &args.session_id);
    if let Some(node_hash) = extracted_node_hash {
        node_hashes.push(node_hash);
    }
    node_hashes.sort_unstable();
    node_hashes.dedup();

    let query = if args.query.trim().is_empty() {
        text.chars().take(500).collect::<String>()
    } else {
        args.query.clone()
    };
    let should_retrieve = args.event == "UserPromptSubmit" || !args.query.trim().is_empty();
    let mut retrieve_status = json!({});
    if should_retrieve && !node_hashes.is_empty() {
        let retrieve = ContextRetrieveRequest {
            shard_id: 1,
            tenant_hash,
            node_hashes: node_hashes.clone(),
            query: query.clone(),
            start_time_ms: 0,
            end_time_ms: now_ms.saturating_add(1),
            max_events: 32,
            min_confidence: 0.0,
            min_importance: 0.0,
            tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
            max_summary_nodes: 16,
            max_event_nodes: 8,
            provider: ContextModelProviderConfig::default(),
        };
        let inject = inject_context(
            &engine,
            ContextInjectRequest {
                retrieve,
                prompt: query.clone(),
                session_hash: stable_hash64(&args.session_id),
                query_id: format!("{}:{:016x}", args.event, stable_hash64(&query)),
                max_prompt_tokens: args.max_context_tokens,
                provider: ContextModelProviderConfig::default(),
            },
        );
        retrieve_status = json!({
            "status": if inject.status.ok { "ok" } else { "failed" },
            "code": inject.status.code,
            "message": inject.status.message,
            "context_pack_id": format!("rust-agent-pack-{:016x}", stable_hash64(&inject.audit.query_id)),
            "selected_ref_count": inject.audit.selected_refs.len(),
            "blocked_ref_count": inject.audit.blocked_refs.len(),
            "selected_block_count": inject.selected_blocks.len(),
            "blocked_block_count": inject.blocked_blocks.len(),
            "used_context_tokens": inject.audit.selected_tokens,
            "injected_prompt_contains_context": inject.injected_prompt.contains("<context>"),
            "selected_refs": inject.audit.selected_refs,
        });
    }

    let report = json!({
        "status": "ok",
        "backend": "rust-temporalstore",
        "agent_name": args.agent_name,
        "agent_profile": agent_profile(&args.agent_name),
        "event": args.event,
        "lifecycle_stage": {
            "before_llm_retrieve": args.event == "UserPromptSubmit",
            "after_llm_ingest_only": matches!(args.event.as_str(), "PostToolUse" | "PreToolUse" | "PermissionRequest"),
            "hook_boundary_commit": matches!(args.event.as_str(), "Stop" | "PostCompact" | "SubagentStop"),
            "idle_timeout_commit": matches!(args.event.as_str(), "IdleTimeout" | "SessionIdle"),
        },
        "scope": {
            "account_id": args.account_id,
            "tenant_id": args.tenant_id,
            "user_id": args.user_id,
            "session_id": args.session_id,
            "tenant_hash": tenant_hash,
        },
        "ingest": ingest_status,
        "retrieve": retrieve_status,
        "node_index": {
            "session_node_count": node_hashes.len(),
            "path": session_index_path(&args.root, &args.agent_name),
        },
        "root": args.root,
        "event_log": args.event_log,
    });
    append_event_log(&args.event_log, &report);
    println!(
        "{}",
        serde_json::to_string(&report).expect("hook report should serialize")
    );
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let agent_name = std::env::var("MATRIXARK_AGENT_NAME")
        .or_else(|_| std::env::var("TEMPORALSTORE_AGENT_NAME"))
        .unwrap_or_else(|_| "codex".to_string());
    let mut parsed = Args {
        agent_name,
        event: std::env::var("CODEX_HOOK_EVENT").unwrap_or_else(|_| "UserPromptSubmit".to_string()),
        root: PathBuf::from(
            std::env::var("TEMPORALSTORE_RUST_CODEX_HOOK_ROOT")
                .unwrap_or_else(|_| "/tmp/temporalstore-rust-codex-hook".to_string()),
        ),
        event_log: PathBuf::from(
            std::env::var("TEMPORALSTORE_RUST_CODEX_EVENT_LOG")
                .unwrap_or_else(|_| "/tmp/temporalstore-rust-codex-hook.jsonl".to_string()),
        ),
        account_id: std::env::var("MATRIXARK_ACCOUNT_ID")
            .unwrap_or_else(|_| "acct_codex".to_string()),
        tenant_id: std::env::var("MATRIXARK_TENANT_ID")
            .unwrap_or_else(|_| "tenant_codex".to_string()),
        user_id: std::env::var("MATRIXARK_USER_ID")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "codex_user".to_string()),
        session_id: std::env::var("MATRIXARK_SESSION_ID")
            .unwrap_or_else(|_| "codex_session".to_string()),
        query: String::new(),
        max_context_tokens: std::env::var("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(1024),
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--agent-name" | "--agent" => {
                parsed.agent_name = args.next().unwrap_or(parsed.agent_name)
            }
            "--event" => parsed.event = args.next().unwrap_or(parsed.event),
            "--root" => parsed.root = PathBuf::from(args.next().unwrap_or_default()),
            "--event-log" => parsed.event_log = PathBuf::from(args.next().unwrap_or_default()),
            "--account-id" => parsed.account_id = args.next().unwrap_or(parsed.account_id),
            "--tenant-id" => parsed.tenant_id = args.next().unwrap_or(parsed.tenant_id),
            "--user-id" => parsed.user_id = args.next().unwrap_or(parsed.user_id),
            "--session-id" => parsed.session_id = args.next().unwrap_or(parsed.session_id),
            "--query" => parsed.query = args.next().unwrap_or_default(),
            "--max-context-tokens" => {
                parsed.max_context_tokens = args
                    .next()
                    .and_then(|value| value.parse::<u32>().ok())
                    .unwrap_or(parsed.max_context_tokens);
            }
            _ => {}
        }
    }
    parsed
}

fn read_payload() -> Value {
    let mut raw = String::new();
    io::stdin()
        .read_to_string(&mut raw)
        .expect("stdin should be readable");
    if raw.trim().is_empty() {
        return Value::Object(Default::default());
    }
    serde_json::from_str(&raw).unwrap_or_else(|_| json!({ "raw_text": raw }))
}

fn payload_text(payload: &Value) -> Option<String> {
    for path in [
        &["prompt"][..],
        &["user_prompt"][..],
        &["userPrompt"][..],
        &["input"][..],
        &["text"][..],
        &["message"][..],
        &["query"][..],
        &["instruction"][..],
        &["params", "prompt"][..],
        &["params", "input"][..],
        &["params", "text"][..],
        &["turn", "input"][..],
        &["request", "prompt"][..],
        &["request", "input"][..],
        &["request", "text"][..],
        &["conversation", "last_message"][..],
        &["cursor", "prompt"][..],
        &["claude", "prompt"][..],
        &["raw_text"][..],
    ] {
        if let Some(value) = string_at(payload, path) {
            if !value.trim().is_empty() {
                return Some(value.trim().to_string());
            }
        }
    }
    for key in ["messages", "items", "input", "transcript", "conversation"] {
        if let Some(items) = payload.get(key).and_then(Value::as_array) {
            let parts = items
                .iter()
                .filter_map(|item| {
                    item.as_str()
                        .map(str::to_string)
                        .or_else(|| string_at(item, &["content"]))
                        .or_else(|| string_at(item, &["text"]))
                        .or_else(|| string_at(item, &["message"]))
                })
                .collect::<Vec<_>>();
            if !parts.is_empty() {
                return Some(parts.join("\n"));
            }
        }
    }
    Some(
        serde_json::to_string(payload)
            .ok()?
            .chars()
            .take(4000)
            .collect(),
    )
}

fn string_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    current.as_str().map(str::to_string)
}

fn source_kind_for_event(event: &str) -> ContextSourceKind {
    match event {
        "PostToolUse" | "PreToolUse" | "PermissionRequest" | "ToolCall" | "ToolResult"
        | "cursor.tool" | "claude.tool" => ContextSourceKind::Code,
        "Stop" | "PostCompact" | "SubagentStop" | "SessionEnd" | "ConversationStop"
        | "cursor.stop" | "claude.stop" => ContextSourceKind::UserEvent,
        _ => ContextSourceKind::Chat,
    }
}

fn role_for_event(event: &str) -> &'static str {
    match event {
        "PostToolUse" | "PreToolUse" | "PermissionRequest" | "ToolCall" | "ToolResult"
        | "cursor.tool" | "claude.tool" => "tool",
        "Stop" | "PostCompact" | "SubagentStop" | "SessionEnd" | "ConversationStop"
        | "cursor.stop" | "claude.stop" => "assistant",
        _ => "user",
    }
}

fn session_nodes(root: &PathBuf, agent_name: &str, session_id: &str) -> Vec<u64> {
    let path = session_index_path(root, agent_name);
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str::<SessionIndex>(&text).ok())
        .and_then(|index| index.sessions.get(session_id).cloned())
        .unwrap_or_default()
}

fn remember_session_node(root: &PathBuf, agent_name: &str, session_id: &str, node_hash: u64) {
    let path = session_index_path(root, agent_name);
    let mut index = fs::read_to_string(&path)
        .ok()
        .and_then(|text| serde_json::from_str::<SessionIndex>(&text).ok())
        .unwrap_or_default();
    let nodes = index.sessions.entry(session_id.to_string()).or_default();
    nodes.push(node_hash);
    nodes.sort_unstable();
    nodes.dedup();
    let _ = fs::write(
        path,
        serde_json::to_vec_pretty(&index).expect("session index should serialize"),
    );
}

fn session_index_path(root: &PathBuf, agent_name: &str) -> PathBuf {
    root.join(format!(
        "{}-session-index.json",
        sanitize_agent_name(agent_name)
    ))
}

fn sanitize_agent_name(agent_name: &str) -> String {
    let value = agent_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let trimmed = value.trim_matches('-');
    if trimmed.is_empty() {
        "agent".to_string()
    } else {
        trimmed.to_string()
    }
}

fn agent_profile(agent_name: &str) -> &'static str {
    match agent_name.to_ascii_lowercase().as_str() {
        "codex" => "codex",
        "claude" | "claude-ai" | "claude_code" | "claude-code" => "claude",
        "cursor" | "cursor-ai" => "cursor",
        _ => "generic",
    }
}

fn append_event_log(path: &PathBuf, report: &Value) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(
            file,
            "{}",
            serde_json::to_string(report).expect("hook event should serialize")
        );
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(1)
}

fn stable_hash64(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    // shared-corpus: codex_mcp_multi_agent_context_hook_parity
    #[test]
    fn payload_text_accepts_claude_and_cursor_shapes() {
        let claude = json!({
            "claude": {
                "prompt": "Remember that Claude owns the release checklist."
            }
        });
        assert_eq!(
            payload_text(&claude).as_deref(),
            Some("Remember that Claude owns the release checklist.")
        );

        let cursor = json!({
            "cursor": {
                "prompt": "Cursor edited context retrieval tests."
            }
        });
        assert_eq!(
            payload_text(&cursor).as_deref(),
            Some("Cursor edited context retrieval tests.")
        );

        let transcript = json!({
            "transcript": [
                {"content": "first turn"},
                {"text": "second turn"}
            ]
        });
        assert_eq!(
            payload_text(&transcript).as_deref(),
            Some("first turn\nsecond turn")
        );
    }

    // shared-corpus: codex_mcp_multi_agent_context_hook_parity
    #[test]
    fn agent_profiles_and_session_indexes_are_agent_specific() {
        let root = PathBuf::from("/tmp/agent-hook-test");
        assert_eq!(agent_profile("claude-ai"), "claude");
        assert_eq!(agent_profile("cursor"), "cursor");
        assert_eq!(agent_profile("other-agent"), "generic");
        assert_eq!(
            session_index_path(&root, "Claude AI"),
            root.join("claude-ai-session-index.json")
        );
        assert_eq!(
            session_index_path(&root, "Cursor"),
            root.join("cursor-session-index.json")
        );
    }

    // shared-corpus: codex_mcp_multi_agent_context_hook_parity
    #[test]
    fn agent_events_map_to_context_source_and_role() {
        assert_eq!(
            source_kind_for_event("claude.tool"),
            ContextSourceKind::Code
        );
        assert_eq!(
            source_kind_for_event("cursor.tool"),
            ContextSourceKind::Code
        );
        assert_eq!(
            source_kind_for_event("claude.stop"),
            ContextSourceKind::UserEvent
        );
        assert_eq!(role_for_event("cursor.tool"), "tool");
        assert_eq!(role_for_event("claude.stop"), "assistant");
    }
}
