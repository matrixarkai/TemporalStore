use serde_json::{json, Value};

use crate::matrixark_rust_proxy_command_stats::command_stats;
use crate::matrixark_rust_proxy_metrics::{CommandStats, MetricsSnapshot};
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_record_time_index::{
    matrixark_context_event_time_field, matrixark_context_event_time_key,
    matrixark_context_event_time_payload,
};
use crate::matrixark_rust_proxy_records::{
    matrixark_record_id, matrixark_record_type, matrixark_storage_field, matrixark_storage_key,
    matrixark_tenant_hash,
};
use crate::matrixark_rust_proxy_cross_session::CrossSessionPolicy;
use crate::matrixark_rust_proxy_retrieve_scoring::ScoredCandidate;
use crate::matrixark_rust_proxy_retrieve_select::select_retrieve_refs;

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
        matrixark_context_event_time_field(&record, &matrixark_record_id(&record, None).unwrap()),
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

#[test]
fn retrieve_selection_reserves_required_profile_entity_bridge_slot() {
    let same_session = json!({
        "record_type": "context_event",
        "event_id_hash": 1_u64,
        "text": "Current session says the storage migration is blocked on capacity review."
    });
    let profile_entity = json!({
        "record_type": "context_entity",
        "entity_hash": 2_u64,
        "scope": {"session_id": "prior-session"},
        "state": "User profile says Alice approved the GPU request after finance review."
    });
    let scored = vec![
        ScoredCandidate {
            score: 0.99,
            record: &same_session,
            session_continuity: "same_session".to_string(),
            continuity_boost: 0.0,
            cross_session_rerank_boost: 0.0,
        },
        ScoredCandidate {
            score: 0.21,
            record: &profile_entity,
            session_continuity: "cross_session".to_string(),
            continuity_boost: 0.0,
            cross_session_rerank_boost: 0.0,
        },
    ];
    let selection = select_retrieve_refs(
        scored,
        &CrossSessionPolicy {
            enabled: true,
            budget_ratio: 1.0,
            budget_tokens: 200,
            max_budget_ratio: 1.0,
            max_budget_tokens: 200,
            max_sessions: 3,
            max_candidates: 8,
            min_score: 0.0,
            raw_evidence_min_score: 0.45,
            min_entity_bridge_refs: 1,
            parallelism: 1,
        },
        200,
        1,
    );

    assert_eq!(selection.selected.len(), 1);
    assert_eq!(
        selection.selected[0]
            .get("session_continuity")
            .and_then(Value::as_str),
        Some("cross_session")
    );
    assert_eq!(selection.entity_bridge_selected_refs, 1);
    assert_eq!(selection.dropped_entity_bridge_slot_reserved, 1);
}
