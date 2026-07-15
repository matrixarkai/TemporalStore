use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Read, Write};
use std::time::Instant;

use serde_json::{json, Value};
use temporalstore::{Client, Options};

#[path = "../matrixark_rust_proxy_cache.rs"]
mod matrixark_rust_proxy_cache;
#[path = "../matrixark_rust_proxy_candidates.rs"]
mod matrixark_rust_proxy_candidates;
#[path = "../matrixark_rust_proxy_command_stats.rs"]
mod matrixark_rust_proxy_command_stats;
#[path = "../matrixark_rust_proxy_metrics.rs"]
mod matrixark_rust_proxy_metrics;
#[path = "../matrixark_rust_proxy_pack.rs"]
mod matrixark_rust_proxy_pack;
#[path = "../matrixark_rust_proxy_protocol.rs"]
mod matrixark_rust_proxy_protocol;
#[path = "../matrixark_rust_proxy_records.rs"]
mod matrixark_rust_proxy_records;
#[path = "../matrixark_rust_proxy_scan.rs"]
mod matrixark_rust_proxy_scan;
#[path = "../matrixark_rust_proxy_scope.rs"]
mod matrixark_rust_proxy_scope;
use matrixark_rust_proxy_candidates::{record_node_hash, record_ref_hash};
use matrixark_rust_proxy_command_stats::{command_entries, command_stats};
use matrixark_rust_proxy_metrics::{
    matrixark_rust_service_mode, unix_ms, CommandStats, MetricsSnapshot,
};
use matrixark_rust_proxy_pack::{
    candidate_text, context_class_name, is_serving_selected_ref_class, pack_ref_from_record,
    sparse_query_score, token_estimate,
};
use matrixark_rust_proxy_protocol::Command;
#[cfg(test)]
use matrixark_rust_proxy_records::{
    matrixark_context_event_time_field, matrixark_context_event_time_key,
    matrixark_context_event_time_payload, matrixark_record_id, matrixark_record_type,
    matrixark_storage_field, matrixark_storage_key, matrixark_tenant_hash,
};
use matrixark_rust_proxy_records::{read_matrixark_record, write_matrixark_record};
use matrixark_rust_proxy_scan::scan_matrixark_candidates;
use matrixark_rust_proxy_scope::{
    continuity_boost, cross_session_rerank_boost, json_field, parse_cross_session_policy,
    record_scope_string, session_continuity_status, session_scope_mode,
};

fn required(value: Option<String>, name: &str) -> Result<String, String> {
    value
        .filter(|item| !item.is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

fn effective_config(command: &Command) -> (String, String, String, i32, i32) {
    (
        command
            .metaserver
            .clone()
            .unwrap_or_else(|| "127.0.0.1:18000".to_string()),
        command
            .namespace
            .clone()
            .unwrap_or_else(|| "deploy_ns".to_string()),
        command
            .table
            .clone()
            .unwrap_or_else(|| "deploy_table".to_string()),
        command.request_timeout_ms.unwrap_or(20_000),
        command.io_timeout_ms.unwrap_or(20_000),
    )
}

fn connect(command: &Command) -> Result<Client, String> {
    let (metaserver, namespace, table, request_timeout_ms, io_timeout_ms) =
        effective_config(command);
    let mut options = Options::new(metaserver, namespace, table);
    options.psm = "matrixark.rust.mcp".to_string();
    options.request_timeout_ms = request_timeout_ms;
    options.io_timeout_ms = io_timeout_ms;
    Client::connect(options).map_err(|err| err.to_string())
}

fn config_key(command: &Command) -> String {
    let (metaserver, namespace, table, request_timeout_ms, io_timeout_ms) =
        effective_config(command);
    format!(
        "{metaserver}\u{1f}{namespace}\u{1f}{table}\u{1f}{request_timeout_ms}\u{1f}{io_timeout_ms}"
    )
}

fn cross_session_key(record: &Value) -> String {
    record_scope_string(record, "session_id")
        .or_else(|| record_scope_string(record, "scope_key"))
        .or_else(|| record_node_hash(record).map(|node| format!("node:{node}")))
        .unwrap_or_else(|| "unknown_cross_session".to_string())
}

fn retrieve_context_pack_via_sdk_native(
    client: &Client,
    command: &Command,
) -> Result<Value, String> {
    let count_key = required(command.count_key.clone(), "count_key")?;
    let record_hash_key = required(command.record_hash_key.clone(), "record_hash_key")?;
    let shard_size = command.shard_size.unwrap_or(1024).max(1) as usize;
    let request = command.record.clone().unwrap_or_else(|| json!({}));
    let raw = client
        .matrixark_retrieve_context_pack(
            &count_key,
            &record_hash_key,
            shard_size,
            &request.to_string(),
        )
        .map_err(|err| err.to_string())?;
    let mut response: Value = serde_json::from_str(&raw)
        .map_err(|err| format!("native retrieve context pack returned invalid JSON: {err}"))?;
    if response.get("context_pack").is_none() {
        response = json!({
            "context_pack": response,
        });
    }
    if let Some(obj) = response.as_object_mut() {
        obj.insert("ok".to_string(), Value::Bool(true));
        obj.insert("native_pack_assembly".to_string(), Value::Bool(true));
        obj.insert(
            "rust_proxy_native_sdk_path".to_string(),
            Value::String("temporalstore_matrixark_retrieve_context_pack".to_string()),
        );
        obj.insert("cache_hit".to_string(), Value::Bool(true));
    }
    if let Some(pack) = response
        .get_mut("context_pack")
        .and_then(Value::as_object_mut)
    {
        pack.entry("context_pack_assembly".to_string())
            .or_insert_with(|| Value::String("native_cpp_direct_via_rust_proxy".to_string()));
        let selected_count = pack
            .get("selected_ref_count")
            .and_then(Value::as_u64)
            .or_else(|| {
                pack.get("selected_refs")
                    .and_then(Value::as_array)
                    .map(|refs| refs.len() as u64)
            })
            .unwrap_or(0);
        pack.insert("selected_ref_count".to_string(), json!(selected_count));
        let recall_policy = pack
            .entry("recall_policy".to_string())
            .or_insert_with(|| json!({}));
        if let Some(recall_obj) = recall_policy.as_object_mut() {
            recall_obj.insert(
                "rust_proxy_native_sdk_path".to_string(),
                Value::String("temporalstore_matrixark_retrieve_context_pack".to_string()),
            );
            recall_obj.insert("python_hot_path_records".to_string(), json!(0));
        }
    }
    Ok(response)
}

fn retrieve_context_pack_native(client: &Client, command: &Command) -> Result<Value, String> {
    let use_sdk_native = std::env::var("MATRIXARK_RUST_PROXY_DISABLE_SDK_NATIVE_PACK")
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(true);
    if use_sdk_native {
        match retrieve_context_pack_via_sdk_native(client, command) {
            Ok(response) => return Ok(response),
            Err(err) => {
                if std::env::var("MATRIXARK_RUST_PROXY_DISABLE_LEGACY_PACK_FALLBACK")
                    .map(|value| {
                        matches!(
                            value.trim().to_ascii_lowercase().as_str(),
                            "1" | "true" | "yes"
                        )
                    })
                    .unwrap_or(false)
                {
                    return Err(err);
                }
            }
        }
    }
    let request = command.record.clone().unwrap_or_else(|| json!({}));
    let query = request
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let query_terms: HashSet<String> = query
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| part.len() > 2)
        .map(str::to_string)
        .collect();
    let remote_budget = json_field(&request, &["local_budget", "remote_budget_tokens"])
        .and_then(Value::as_u64)
        .or_else(|| request.get("max_context_tokens").and_then(Value::as_u64))
        .unwrap_or(4000);
    let max_refs = json_field(&request, &["ranking", "max_selected_refs"])
        .and_then(Value::as_u64)
        .unwrap_or(24)
        .max(1);
    let max_global_candidates = json_field(&request, &["ranking", "max_global_candidates"])
        .and_then(Value::as_u64)
        .unwrap_or(512)
        .max(1);
    let min_similarity_score = json_field(&request, &["ranking", "min_similarity_score"])
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    let budget_fill_policy = json_field(&request, &["ranking", "budget_fill_policy"])
        .and_then(Value::as_str)
        .filter(|policy| *policy == "quality_first" || *policy == "force_fill")
        .unwrap_or("quality_first")
        .to_string();
    let question_type = request
        .get("question_type")
        .and_then(Value::as_str)
        .unwrap_or("fact")
        .to_string();
    let mut scan_command = command.clone();
    scan_command.scope = request
        .get("scope")
        .cloned()
        .or_else(|| command.scope.clone());
    scan_command.secondary_index_groups = request
        .get("secondary_index_groups")
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .map(|group| {
                    group
                        .as_array()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .or_else(|| command.secondary_index_groups.clone());
    if scan_command
        .record_types
        .as_ref()
        .map(Vec::is_empty)
        .unwrap_or(true)
    {
        scan_command.record_types = Some(vec![
            "context_compression_event".to_string(),
            "context_entity".to_string(),
            "context_event".to_string(),
            "context_segment".to_string(),
            "context_summary".to_string(),
            "resource_chunk".to_string(),
            "skill_section".to_string(),
            "context_index".to_string(),
        ]);
    }
    let scan = scan_matrixark_candidates(client, &scan_command)?;
    let empty_records = Vec::new();
    let records = scan
        .get("records")
        .and_then(Value::as_array)
        .unwrap_or(&empty_records);
    let scope_for_continuity = scan_command.scope.clone();
    let cross_policy = parse_cross_session_policy(
        &request,
        scope_for_continuity.as_ref(),
        remote_budget,
        &question_type,
    );
    let mut scored: Vec<(f64, &Value, String, f64, f64)> = records
        .iter()
        .filter(|record| {
            matches!(
                record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                "context_compression_event"
                    | "context_entity"
                    | "context_event"
                    | "context_segment"
                    | "context_summary"
                    | "resource_chunk"
                    | "skill_section"
            ) && !candidate_text(record).is_empty()
        })
        .map(|record| {
            let text = candidate_text(record);
            let mut score = sparse_query_score(&query_terms, &text);
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_entity")
            ) {
                score += 0.08;
            }
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_compression_event")
            ) {
                score += 0.06;
            }
            let context_class = context_class_name(record);
            let session_continuity =
                session_continuity_status(record, scope_for_continuity.as_ref());
            let continuity_boost_value =
                continuity_boost(record, &context_class, &session_continuity);
            score += continuity_boost_value;
            let cross_session_rerank_boost_value = cross_session_rerank_boost(
                record,
                &context_class,
                &session_continuity,
                &question_type,
            );
            score += cross_session_rerank_boost_value;
            (
                score,
                record,
                session_continuity,
                continuity_boost_value,
                cross_session_rerank_boost_value,
            )
        })
        .filter(|(score, _, _, _, _)| *score >= min_similarity_score)
        .collect();
    scored.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if scored.len() > max_global_candidates as usize {
        scored.truncate(max_global_candidates as usize);
    }
    let mut selected = Vec::new();
    let mut selected_signatures: HashSet<String> = HashSet::new();
    let mut dropped_duplicate_ref = 0_u64;
    let mut selected_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_nodes: HashSet<u64> = HashSet::new();
    let mut dropped_over_budget = 0_u64;
    let mut dropped_cross_budget = 0_u64;
    let mut dropped_cross_session_cap = 0_u64;
    let mut dropped_cross_candidate_cap = 0_u64;
    let mut dropped_low_score = 0_u64;
    let mut dropped_policy_ref = 0_u64;
    let mut used_tokens = 0_u64;
    let mut cross_used_tokens = 0_u64;
    let mut cross_selected_refs = 0_u64;
    let mut entity_bridge_selected_refs = 0_u64;
    let mut selected_cross_sessions: HashSet<String> = HashSet::new();
    for (
        score,
        record,
        session_continuity,
        continuity_boost_value,
        cross_session_rerank_boost_value,
    ) in scored
    {
        if selected.len() as u64 >= max_refs {
            break;
        }
        let text = candidate_text(record);
        let tokens = token_estimate(&text);
        let context_class = context_class_name(record);
        if !is_serving_selected_ref_class(&context_class) {
            dropped_policy_ref += 1;
            continue;
        }
        let is_cross_session = session_continuity == "cross_session";
        let record_type = record
            .get("record_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        let is_entity_bridge = is_cross_session && context_class == "entity";
        let is_cross_session_raw_evidence =
            is_cross_session && matches!(record_type, "context_event" | "context_segment");
        let cross_key = if is_cross_session {
            cross_session_key(record)
        } else {
            String::new()
        };
        if is_cross_session && !cross_policy.enabled {
            dropped_cross_budget += 1;
            continue;
        }
        if is_cross_session && cross_policy.min_score > 0.0 && score < cross_policy.min_score {
            dropped_low_score += 1;
            continue;
        }
        if is_cross_session_raw_evidence
            && cross_policy.raw_evidence_min_score > 0.0
            && score < cross_policy.raw_evidence_min_score
        {
            dropped_low_score += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.max_candidates > 0
            && cross_selected_refs >= cross_policy.max_candidates
        {
            dropped_cross_candidate_cap += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.max_sessions > 0
            && !selected_cross_sessions.contains(&cross_key)
            && selected_cross_sessions.len() as u64 >= cross_policy.max_sessions
        {
            dropped_cross_session_cap += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.budget_tokens > 0
            && cross_used_tokens + tokens > cross_policy.budget_tokens
            && !(is_entity_bridge
                && entity_bridge_selected_refs < cross_policy.min_entity_bridge_refs)
        {
            dropped_cross_budget += 1;
            continue;
        }
        let ref_signature = format!(
            "{}:{}",
            context_class,
            record_ref_hash(record).unwrap_or_else(|| {
                record
                    .get("record_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            })
        );
        if !selected_signatures.insert(ref_signature) {
            dropped_duplicate_ref += 1;
            continue;
        }
        if used_tokens + tokens > remote_budget {
            dropped_over_budget += 1;
            continue;
        }
        used_tokens += tokens;
        if is_cross_session {
            cross_used_tokens += tokens;
            cross_selected_refs += 1;
            selected_cross_sessions.insert(cross_key);
            if is_entity_bridge {
                entity_bridge_selected_refs += 1;
            }
        }
        *selected_counts.entry(context_class).or_default() += 1;
        if let Some(node_hash) = record_node_hash(record) {
            selected_nodes.insert(node_hash);
        }
        selected.push(pack_ref_from_record(
            record,
            score,
            "native_rust_proxy_score_pack",
            &session_continuity,
            continuity_boost_value,
            cross_session_rerank_boost_value,
        ));
    }
    let context_pack_id = format!("rust-native-{}-{}", unix_ms(), selected.len());
    let mut scan_stats = scan.get("scan_stats").cloned().unwrap_or_else(|| json!({}));
    if let Some(stats) = scan_stats.as_object_mut() {
        stats.insert("native_pack_assembly".to_string(), json!(true));
        stats.insert(
            "pack_assembly_location".to_string(),
            json!("rust_proxy_native"),
        );
        stats.insert("next_native_gap".to_string(), json!(""));
    }
    let pack = json!({
        "context_pack_id": context_pack_id,
        "query": query,
        "question_type": request.get("question_type").cloned().unwrap_or_else(|| json!("fact")),
        "selected_ref_counts": selected_counts,
        "remote_context_refs": selected,
        "selected_refs": selected,
        "dropped_refs": {
            "over_budget": dropped_over_budget,
            "cross_session_budget": dropped_cross_budget,
            "cross_session_session_cap": dropped_cross_session_cap,
            "cross_session_candidate_cap": dropped_cross_candidate_cap,
            "low_score": dropped_low_score,
            "duplicate_ref": dropped_duplicate_ref,
            "policy_ref": dropped_policy_ref,
            "reason_counts": {
                "over_budget": dropped_over_budget,
                "cross_session_budget": dropped_cross_budget,
                "cross_session_session_cap": dropped_cross_session_cap,
                "cross_session_candidate_cap": dropped_cross_candidate_cap,
                "low_score": dropped_low_score,
                "duplicate_ref": dropped_duplicate_ref,
                "policy_ref": dropped_policy_ref
            }
        },
        "used_context_tokens": used_tokens,
        "used_remote_context_tokens": used_tokens,
        "remote_context_budget_tokens": remote_budget,
        "requested_max_context_tokens": request.get("max_context_tokens").cloned().unwrap_or_else(|| json!(remote_budget)),
        "packing_policy": "native_rust_proxy_question_type_aware",
        "context_pack_assembly": "native_rust_proxy",
        "context_sources_order": ["entities", "events", "segments", "resources", "skills", "summaries"],
        "recall_policy": {
            "native_context_pack": {
                "enabled": true,
                "backend": "rust_proxy",
                "scan_filter_score_pack": true
            },
            "native_response_contract": {
                "raw_records_returned_to_python": false,
                "python_hot_path_records": 0,
                "python_role": "dispatch_request_receive_context_pack",
                "backend_role": "scan_filter_score_pack"
            },
            "scan_stats": scan_stats,
            "rerank": {
                "enabled": true,
                "mode": "native_weighted_recall_plus_cross_session_rerank",
                "cross_session_rerank_enabled": true,
                "cross_session_signals": ["entity_state", "resource_fact_citation", "answer_event", "compression", "summary_demotion"],
                "heavy_rerank_enabled": false
            },
            "ranking": {
                "min_similarity_score": min_similarity_score,
                "max_global_candidates": max_global_candidates,
                "max_selected_refs": max_refs,
                "budget_fill_policy": budget_fill_policy,
                "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first"
            },
            "session_continuity": {
                "mode": scan_command.scope.as_ref().map(session_scope_mode).unwrap_or("only"),
                "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                "same_session_selected_ref_count": selected.iter().filter(|item| item.get("session_continuity").and_then(Value::as_str) == Some("same_session")).count(),
                "cross_session_selected_ref_count": cross_selected_refs,
                "entity_bridge_selected_ref_count": entity_bridge_selected_refs
            },
            "cross_session": {
                "enabled": cross_policy.enabled,
                "mode": if cross_policy.enabled { "prefer" } else { "disabled" },
                "budget_ratio": cross_policy.budget_ratio,
                "max_budget_ratio": cross_policy.max_budget_ratio,
                "budget_tokens": cross_policy.budget_tokens,
                "remote_budget_tokens": remote_budget,
                "max_budget_tokens": cross_policy.max_budget_tokens,
                "max_sessions": cross_policy.max_sessions,
                "max_candidates": cross_policy.max_candidates,
                "min_score": cross_policy.min_score,
                "raw_evidence_min_score": cross_policy.raw_evidence_min_score,
                "parallelism": cross_policy.parallelism,
                "selected_tokens": cross_used_tokens,
                "selected_ref_count": cross_selected_refs,
                "selected_session_count": selected_cross_sessions.len() as u64,
                "entity_bridge_selected_ref_count": entity_bridge_selected_refs,
                "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
                "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events"
            },
            "tree_traversal": {
                "enabled": true,
                "native_backend": true,
                "fallback_to_flat": false,
                "selected_node_count": selected_nodes.len() as u64,
                "selected_leaf_count": selected_nodes.len() as u64,
                "summary_embeddings": ["node_l0", "node_l1"]
            },
            "secondary_index_filter": {
                "enabled": true,
                "native_backend": true,
                "applied_before_embedding_scoring": true,
                "matched_candidate_count": scan.get("scan_stats").and_then(|v| v.get("secondary_index_matched_candidate_count")).cloned().unwrap_or_else(|| json!(0)),
                "dropped_candidate_count": scan.get("scan_stats").and_then(|v| v.get("secondary_index_dropped_candidate_count")).cloned().unwrap_or_else(|| json!(0))
            }
        },
        "quality_warnings": []
    });
    let scan_dropped_count = scan_stats
        .get("dropped_by_type")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        + scan_stats
            .get("dropped_by_scope")
            .and_then(Value::as_u64)
            .unwrap_or(0)
        + scan_stats
            .get("selected_node_dropped_candidate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0)
        + scan_stats
            .get("secondary_index_dropped_candidate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
    let dropped_ref_count = dropped_over_budget
        + dropped_cross_budget
        + dropped_cross_session_cap
        + dropped_cross_candidate_cap
        + dropped_policy_ref
        + dropped_duplicate_ref
        + scan_dropped_count;
    let scan_cache_hit = scan_stats
        .get("native_filtered_scan_cache_hit")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || scan_stats
            .get("native_scan_record_cache_hit")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    Ok(json!({
        "ok": true,
        "count": selected.len(),
        "native_pack_assembly": true,
        "raw_records_returned": false,
        "python_hot_path_records": 0,
        "scan_count": scan_stats.get("scanned_records").and_then(Value::as_u64).unwrap_or(0),
        "cache_hit": scan_cache_hit,
        "cache_hit_used": scan_cache_hit,
        "selected_ref_count": selected.len(),
        "dropped_ref_count": dropped_ref_count,
        "dropped_duplicate_ref_count": dropped_duplicate_ref,
        "context_pack": pack,
        "scan_stats": scan_stats
    }))
}

fn run_with_client(client: &Client, command: Command) -> Result<Value, String> {
    match command.op.as_str() {
        "put_string" => {
            client
                .put_string(
                    &required(command.key, "key")?,
                    &required(command.value, "value")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true}))
        }
        "get_string" => {
            let value = client
                .get_string(&required(command.key, "key")?)
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true, "value": value}))
        }
        "hset" => {
            client
                .hset(
                    &required(command.key, "key")?,
                    &required(command.field, "field")?,
                    &required(command.value, "value")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true}))
        }
        "batch_hset" => {
            let entries = command_entries(&command)?;
            if entries.is_empty() {
                return Err("missing entries".to_string());
            }
            for entry in &entries {
                client
                    .hset(entry.key, entry.field, entry.value)
                    .map_err(|err| err.to_string())?;
            }
            Ok(json!({"ok": true, "written": entries.len(), "batch_lowering": "raw_hset"}))
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            let entries = command_entries(&command)?;
            if entries.is_empty()
                && command
                    .key
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .is_none()
            {
                return Err("missing entries".to_string());
            }
            let count_key = command.key.as_deref().filter(|value| !value.is_empty());
            let count_value = command.value.as_deref().filter(|value| !value.is_empty());
            let batch: Vec<(&str, &str, &str)> = entries
                .iter()
                .map(|entry| (entry.key, entry.field, entry.value))
                .collect();
            client
                .matrixark_batch_append_records(&batch, count_key, count_value)
                .map_err(|err| err.to_string())?;
            let mut written = entries.len();
            if count_key.is_some() && count_value.is_some() {
                written += 1;
            }
            let append_options = command.append_options.as_ref();
            let raw_backend = append_options
                .and_then(|options| options.get("raw_storage_backend"))
                .and_then(Value::as_str)
                .unwrap_or("temporalstore");
            let append_path = append_options
                .and_then(|options| options.get("append_path"))
                .and_then(Value::as_str)
                .unwrap_or("native_batch_append_records");
            Ok(json!({
                "ok": true,
                "written": written,
                "append_api": command.op,
                "native_append": true,
                "append_path": append_path,
                "raw_storage_backend": raw_backend,
                "batch_lowering": "none"
            }))
        }
        "batch_hget" => {
            let entries = command_entries(&command)?;
            if entries.is_empty() {
                return Err("missing entries".to_string());
            }
            let mut reads = Vec::with_capacity(entries.len());
            for entry in &entries {
                let value = client
                    .hget(entry.key, entry.field)
                    .map_err(|err| err.to_string())?;
                reads.push(json!({"key": entry.key, "field": entry.field, "value": value}));
            }
            Ok(json!({"ok": true, "read": reads.len(), "records": reads}))
        }
        "hgetall" | "scan_hash" => {
            let key = required(command.key, "key")?;
            let rows = client.scan_hash(&key).map_err(|err| err.to_string())?;
            let records: Vec<Value> = rows
                .iter()
                .map(|(field, value)| json!({"key": key, "field": field, "value": value}))
                .collect();
            Ok(
                json!({"ok": true, "count": records.len(), "read": records.len(), "records": records}),
            )
        }
        "matrixark_scan_candidates" => scan_matrixark_candidates(client, &command),
        "matrixark_retrieve_context_pack" => retrieve_context_pack_native(client, &command),
        "write_matrixark_record" => {
            let record = command
                .record
                .as_ref()
                .ok_or_else(|| "missing record".to_string())?;
            let write = write_matrixark_record(
                client,
                record,
                command.record_type.as_ref(),
                command.tenant_hash,
                command.record_id.as_ref(),
            )?;
            Ok(json!({"ok": true, "write": write}))
        }
        "write_matrixark_records" => {
            let records = command
                .records
                .as_ref()
                .ok_or_else(|| "missing records".to_string())?;
            let mut writes = Vec::with_capacity(records.len());
            for record in records {
                writes.push(write_matrixark_record(
                    client,
                    record,
                    command.record_type.as_ref(),
                    command.tenant_hash,
                    None,
                )?);
            }
            Ok(json!({"ok": true, "written": writes.len(), "writes": writes}))
        }
        "read_matrixark_record" => {
            let record_type = required(command.record_type, "record_type")?;
            let tenant_hash = command
                .tenant_hash
                .ok_or_else(|| "missing tenant_hash".to_string())?;
            let record_id = required(command.record_id, "record_id")?;
            let read = read_matrixark_record(client, &record_type, tenant_hash, &record_id)?;
            Ok(json!({"ok": true, "read": read}))
        }
        "read_matrixark_records" => {
            let record_type = required(command.record_type, "record_type")?;
            let tenant_hash = command
                .tenant_hash
                .ok_or_else(|| "missing tenant_hash".to_string())?;
            let record_ids = command
                .record_ids
                .as_ref()
                .ok_or_else(|| "missing record_ids".to_string())?;
            let mut reads = Vec::with_capacity(record_ids.len());
            for record_id in record_ids {
                reads.push(read_matrixark_record(
                    client,
                    &record_type,
                    tenant_hash,
                    record_id,
                )?);
            }
            Ok(json!({"ok": true, "read": reads.len(), "records": reads}))
        }
        "hget" => {
            let value = client
                .hget(
                    &required(command.key, "key")?,
                    &required(command.field, "field")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true, "value": value}))
        }
        other => Err(format!("unsupported op {other}")),
    }
}

fn run(command: Command) -> Result<Value, String> {
    let client = connect(&command)?;
    run_with_client(&client, command)
}

fn print_result(result: Result<Value, String>, engine_ms: u128) -> (bool, u128) {
    match result {
        Ok(mut value) => {
            if let Some(object) = value.as_object_mut() {
                object.insert("rust_engine_time_ms".to_string(), json!(engine_ms));
            }
            let serialize_started = Instant::now();
            let _ = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
            let serialization_ms = serialize_started.elapsed().as_millis();
            let total_ms = engine_ms + serialization_ms;
            if let Some(object) = value.as_object_mut() {
                object.insert("serialization_time_ms".to_string(), json!(serialization_ms));
                object.insert("elapsed_ms".to_string(), json!(total_ms));
            }
            println!("{}", value);
            (true, total_ms)
        }
        Err(err) => {
            let mut value = json!({
                "ok": false,
                "error": err,
                "rust_engine_time_ms": engine_ms
            });
            let serialize_started = Instant::now();
            let _ = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
            let serialization_ms = serialize_started.elapsed().as_millis();
            let total_ms = engine_ms + serialization_ms;
            if let Some(object) = value.as_object_mut() {
                object.insert("serialization_time_ms".to_string(), json!(serialization_ms));
                object.insert("elapsed_ms".to_string(), json!(total_ms));
            }
            println!("{}", value);
            (false, total_ms)
        }
    }
}

fn export_metrics_if_configured(metrics: &MetricsSnapshot) {
    let Ok(path) = std::env::var("MATRIXARK_RUST_METRICS_PATH") else {
        return;
    };
    if path.trim().is_empty() {
        return;
    }
    let _ = std::fs::write(path, metrics.render_prometheus());
}

fn serve() -> i32 {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut clients: HashMap<String, Client> = HashMap::new();
    let mut metrics = MetricsSnapshot::default();
    export_metrics_if_configured(&metrics);
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(value) => value,
            Err(err) => {
                println!("{}", json!({"ok": false, "error": err.to_string()}));
                let _ = stdout.flush();
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let command: Command = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(err) => {
                metrics.parse_errors += 1;
                export_metrics_if_configured(&metrics);
                println!("{}", json!({"ok": false, "error": err.to_string()}));
                let _ = stdout.flush();
                continue;
            }
        };
        if command.op == "metrics_prometheus" {
            println!(
                "{}",
                json!({"ok": true, "prometheus": metrics.render_prometheus()})
            );
            let _ = stdout.flush();
            continue;
        }
        if command.op == "health" {
            println!(
                "{}",
                json!({"ok": true, "status": "ok", "mode": matrixark_rust_service_mode()})
            );
            let _ = stdout.flush();
            continue;
        }
        if command.op == "shutdown" {
            println!("{}", json!({"ok": true, "status": "shutting_down"}));
            let _ = stdout.flush();
            return 0;
        }
        let key = config_key(&command);
        if !clients.contains_key(&key) {
            match connect(&command) {
                Ok(client) => {
                    clients.insert(key.clone(), client);
                    metrics.clients_created += 1;
                    export_metrics_if_configured(&metrics);
                }
                Err(err) => {
                    metrics.client_connect_errors += 1;
                    export_metrics_if_configured(&metrics);
                    println!("{}", json!({"ok": false, "error": err}));
                    let _ = stdout.flush();
                    continue;
                }
            }
        }
        if command.op == "readiness" {
            println!(
                "{}",
                json!({
                    "ok": true,
                    "status": "ready",
                    "mode": matrixark_rust_service_mode(),
                    "cached_clients": clients.len()
                })
            );
            let _ = stdout.flush();
            continue;
        }
        let op = command.op.clone();
        let started = Instant::now();
        let result = clients
            .get(&key)
            .ok_or_else(|| "missing cached TemporalStore client".to_string())
            .and_then(|client| run_with_client(client, command.clone()));
        let elapsed_ms = started.elapsed().as_millis();
        let (ok, stats) = match &result {
            Ok(value) => (true, command_stats(&command, value)),
            Err(_) => (false, CommandStats::default()),
        };
        let serialization_started = Instant::now();
        match &result {
            Ok(value) => {
                let _ = serde_json::to_string(value);
            }
            Err(err) => {
                let _ = serde_json::to_string(&json!({"ok": false, "error": err}));
            }
        }
        let serialization_ms = serialization_started.elapsed().as_millis();
        metrics.observe(
            &op,
            ok,
            elapsed_ms,
            serialization_ms,
            result.as_ref().ok(),
            stats,
        );
        export_metrics_if_configured(&metrics);
        let _ = print_result(result, elapsed_ms);
        let _ = stdout.flush();
    }
    0
}

fn single_shot() -> i32 {
    let mut input = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut input) {
        println!("{}", json!({"ok": false, "error": err.to_string()}));
        return 1;
    }
    let command: Command = match serde_json::from_str(&input) {
        Ok(value) => value,
        Err(err) => {
            println!("{}", json!({"ok": false, "error": err.to_string()}));
            return 1;
        }
    };
    let started = Instant::now();
    if print_result(run(command), started.elapsed().as_millis()).0 {
        0
    } else {
        1
    }
}

fn main() {
    let code = if std::env::args().any(|arg| arg == "--serve") {
        serve()
    } else {
        single_shot()
    };
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrixark_record_derives_storage_key_from_common_ids() {
        let record = json!({
            "record_type": "resource_manifest",
            "tenant_hash": 77,
            "resource_hash": 7001,
            "raw_uri": "file:///runbooks/gpu.md"
        });
        assert_eq!(
            matrixark_record_type(&record, None).unwrap(),
            "resource_manifest"
        );
        assert_eq!(matrixark_tenant_hash(&record, None).unwrap(), 77);
        assert_eq!(matrixark_record_id(&record, None).unwrap(), "7001");
        assert_eq!(
            matrixark_storage_key("resource_manifest", 77),
            "matrixark:record:resource_manifest:77"
        );
        assert_eq!(matrixark_storage_field("7001"), "7001");
    }

    #[test]
    fn matrixark_record_allows_explicit_fallbacks() {
        let record = json!({"payload": "minimal"});
        assert_eq!(
            matrixark_record_type(&record, Some(&"skill_section".to_string())).unwrap(),
            "skill_section"
        );
        assert_eq!(matrixark_tenant_hash(&record, Some(9)).unwrap(), 9);
        assert_eq!(
            matrixark_record_id(&record, Some(&"section-a".to_string())).unwrap(),
            "section-a"
        );
    }

    #[test]
    fn matrixark_record_rejects_missing_identity() {
        let record = json!({"record_type": "context_event", "tenant_hash": 1});
        assert!(matrixark_record_id(&record, None).is_err());
        assert!(matrixark_record_type(&json!({}), None).is_err());
        assert!(matrixark_tenant_hash(&json!({}), None).is_err());
    }

    #[test]
    fn matrixark_record_storage_key_is_shared_for_batch_read_write() {
        assert_eq!(
            matrixark_storage_key("context_pack_audit", 77),
            "matrixark:record:context_pack_audit:77"
        );
        assert_eq!(matrixark_storage_field("query-1"), "query-1");
    }

    #[test]
    fn context_event_uses_timestamp_ordered_storage_field() {
        let record = json!({
            "record_type": "context_event",
            "tenant_hash": 77,
            "event_id_hash": 42,
            "updated_at_ms": 1782500000123_u64,
            "text": "timestamp keyed"
        });
        assert_eq!(
            matrixark_context_event_time_key(matrixark_tenant_hash(&record, None).unwrap()),
            "matrixark:record:context_event_by_ingestion_time:77"
        );
        assert_eq!(
            matrixark_context_event_time_field(
                &record,
                &matrixark_record_id(&record, None).unwrap()
            ),
            "00000001782500000123:42"
        );
        let indexed_payload: Value = serde_json::from_str(
            &matrixark_context_event_time_payload(&json!({
                "record_type": "context_event",
                "tenant_hash": 77,
                "event_id_hash": 42,
                "ingestion_time_ms": 1782500000123_u64,
                "event_time_key": "00000001782500000123:42",
                "text": "timestamp keyed"
            }))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(indexed_payload["record_type"], "context_event");
        assert_eq!(indexed_payload["text"], "timestamp keyed");
        assert!(indexed_payload.get("ingestion_time_ms").is_none());
        assert!(indexed_payload.get("event_time_key").is_none());
    }

    #[test]
    fn metrics_render_prometheus_records_op_status_and_latency() {
        let mut metrics = MetricsSnapshot::default();
        metrics.observe(
            "write_matrixark_record",
            true,
            12,
            1,
            None,
            CommandStats {
                records_written: 1,
                bytes_written: 128,
                ..CommandStats::default()
            },
        );
        metrics.observe(
            "write_matrixark_record",
            false,
            30,
            2,
            None,
            CommandStats::default(),
        );
        let text = metrics.render_prometheus();
        assert!(text.contains(
            "matrixark_rust_proxy_commands_total{op=\"write_matrixark_record\",status=\"ok\"} 1"
        ));
        assert!(text.contains(
            "matrixark_rust_proxy_commands_total{op=\"write_matrixark_record\",status=\"error\"} 1"
        ));
        assert!(text.contains(
            "matrixark_rust_proxy_command_latency_ms_sum{op=\"write_matrixark_record\"} 42"
        ));
        assert!(text.contains(
            "matrixark_rust_proxy_command_latency_ms_max{op=\"write_matrixark_record\"} 30"
        ));
        assert!(text.contains("matrixark_rust_proxy_records_written_total 1"));
        assert!(text.contains("matrixark_rust_proxy_bytes_written_total 128"));
        assert!(text.contains("matrixark_rust_proxy_commands_failed_total 1"));
    }

    #[test]
    fn command_stats_counts_scan_hash_records() {
        let command: Command = serde_json::from_value(json!({
            "op": "scan_hash",
            "key": "matrixark:mcp:records:000000"
        }))
        .expect("command");
        let stats = command_stats(&command, &json!({"ok": true, "count": 3, "records": []}));
        assert_eq!(stats.records_read, 3);
    }

    #[test]
    fn command_stats_counts_matrixark_batch_records() {
        let command: Command = serde_json::from_value(json!({
            "op": "write_matrixark_records",
            "records": [
                {"record_type": "context_event", "tenant_hash": 1, "event_id_hash": 10, "text": "a"},
                {"record_type": "context_event", "tenant_hash": 1, "event_id_hash": 11, "text": "bb"}
            ]
        }))
        .unwrap();
        let stats = command_stats(&command, &json!({"ok": true, "written": 2}));
        assert_eq!(stats.records_written, 2);
        assert!(stats.bytes_written > 0);
    }
}
