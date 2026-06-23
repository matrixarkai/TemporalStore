use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

fn corpus_path() -> PathBuf {
    if let Ok(path) = std::env::var("TEMPORALSTORE_UNIFIED_CORPUS") {
        return PathBuf::from(path);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../unified/temporalstore_unified_corpus.json")
}

fn string_field<'a>(value: &'a Value, field: &str) -> &'a str {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field {field}"))
}

fn int_field(value: &Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("missing integer field {field}"))
}

#[test]
fn unified_corpus_proxy_contract() {
    let path = corpus_path();
    let raw = fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {path:?}: {err}"));
    let corpus: Value = serde_json::from_str(&raw).unwrap_or_else(|err| panic!("parse {path:?}: {err}"));
    assert_eq!(int_field(&corpus, "schema_version"), 1);

    let cases = corpus
        .get("cases")
        .and_then(Value::as_array)
        .expect("cases must be an array");
    assert!(!cases.is_empty(), "unified corpus must have cases");

    let mut case_names = BTreeSet::new();
    let mut command_kinds = BTreeSet::new();
    let mut context_case_count = 0usize;
    let mut context_step_count = 0usize;

    for case in cases {
        let case_name = string_field(case, "name");
        assert!(case_names.insert(case_name.to_string()), "duplicate case {case_name}");
        assert!(int_field(case, "shard_id") > 0, "{case_name} shard_id must be positive");
        let steps = case
            .get("steps")
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("{case_name} steps must be an array"));
        assert!(!steps.is_empty(), "{case_name} must have steps");
        let mut has_context = false;
        for step in steps {
            let command = step.get("command").expect("step needs command");
            let kind = string_field(command, "kind");
            command_kinds.insert(kind.to_string());
            if kind.starts_with("context_") {
                has_context = true;
                context_step_count += 1;
                validate_context_command(case_name, kind, command);
            }
        }
        if has_context {
            context_case_count += 1;
        }
    }

    let coverage = corpus.get("coverage").expect("coverage is required for Rust/C++ parity");
    assert_required_strings(coverage, "required_case_names", &case_names);
    assert_required_strings(coverage, "required_command_kinds", &command_kinds);

    for required in [
        "context_tree_event_pack_replay",
        "context_raw_extraction_query_pipeline",
        "context_incident_time_aware_pipeline",
        "context_resource_feedback_second_query_pipeline",
        "context_pack_token_budget_parity",
        "context_layered_resource_parsing_pipeline",
        "context_batch_extraction_query_ingestion_x8",
        "context_stream_batch_api_ingestion_compression",
        "context_eight_parity_gates",
        "context_nine_ingestion_compression_parity_gates",
        "context_ten_model_config_parity_gates",
    ] {
        assert!(case_names.contains(required), "missing required context parity case {required}");
    }

    for required in [
        "context_upsert_node",
        "context_upsert_child_ref",
        "context_upsert_embedding",
        "context_write_event",
        "context_upsert_entity",
        "context_write_index_ref",
        "context_upsert_summary",
        "context_write_compression",
        "context_ingest_resource",
        "context_retrieve",
        "context_retrieve_with_resources",
        "context_assert_parity_gates",
    ] {
        assert!(command_kinds.contains(required), "missing required context command {required}");
    }

    assert!(context_case_count >= 11, "expected all context parity cases");
    assert!(context_step_count >= 30, "expected broad context command coverage");
}

fn assert_required_strings(coverage: &Value, field: &str, actual: &BTreeSet<String>) {
    let expected = coverage
        .get(field)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("coverage.{field} must be an array"));
    for item in expected {
        let value = item.as_str().unwrap_or_else(|| panic!("coverage.{field} entries must be strings"));
        assert!(actual.contains(value), "coverage.{field} includes missing value {value}");
    }
}

fn validate_context_command(case_name: &str, kind: &str, command: &Value) {
    match kind {
        "context_upsert_node"
        | "context_upsert_child_ref"
        | "context_write_event"
        | "context_write_index_ref"
        | "context_mark_summary_dirty"
        | "context_upsert_summary"
        | "context_write_compression"
        | "context_write_pack_audit"
        | "context_upsert_entity" => {
            assert!(command.get("record").and_then(Value::as_object).is_some(), "{case_name}:{kind} needs record");
        }
        "context_upsert_embedding" => {
            let record = command.get("record").expect("embedding record");
            assert!(record.get("vector").and_then(Value::as_array).map(|v| !v.is_empty()).unwrap_or(false));
        }
        "context_retrieve" | "context_retrieve_with_resources" | "context_traverse_tree" => {
            int_field(command, "tenant_hash");
            int_field(command, "root_node_hash");
            assert!(command.get("query_vector").and_then(Value::as_array).map(|v| !v.is_empty()).unwrap_or(false));
        }
        "context_assert_parity_gates" => {
            int_field(command, "expect_passed_gates");
            int_field(command, "root_node_hash");
            int_field(command, "approval_node_hash");
            assert!(command.get("expect_resource_chunk_any").and_then(Value::as_array).map(|v| !v.is_empty()).unwrap_or(false));
        }
        _ => {
            assert!(kind.starts_with("context_"));
        }
    }
}
