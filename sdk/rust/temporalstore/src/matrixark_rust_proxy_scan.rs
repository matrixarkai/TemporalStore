use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};
use temporalstore::Client;

use crate::matrixark_rust_proxy_cache::{
    filtered_scan_cache_key, get_filtered_scan_cache, put_filtered_scan_cache,
    scan_record_cache_key, FilteredScanCacheEntry,
};
use crate::matrixark_rust_proxy_candidates::{
    node_path_matches_filters, query_node_path_filters, record_node_hash,
};
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_scan_records::{
    load_scan_records, required, serving_count_key,
};
use crate::matrixark_rust_proxy_scope::scope_matches_record;
use crate::matrixark_rust_proxy_scan_secondary::apply_secondary_prefilter;

pub(crate) fn scan_matrixark_candidates(
    client: &Client,
    command: &Command,
) -> Result<Value, String> {
    let count_key = required(command.count_key.clone(), "count_key")?;
    let record_hash_key = required(command.record_hash_key.clone(), "record_hash_key")?;
    let shard_size = command.shard_size.unwrap_or(1024).max(1);
    let count_text = client
        .get_string(&count_key)
        .map_err(|err| err.to_string())?;
    let count = count_text.parse::<u64>().unwrap_or(0);
    let serving_count_text = client
        .get_string(&serving_count_key(&count_key))
        .unwrap_or_default();
    let serving_count = serving_count_text.parse::<u64>().unwrap_or(count);
    let allowed_types: HashSet<String> = command
        .record_types
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let selected_nodes: HashSet<u64> = command
        .selected_node_hashes
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let secondary_groups = command.secondary_index_groups.clone().unwrap_or_default();
    let cache_key = scan_record_cache_key(&record_hash_key, shard_size, serving_count);
    let filtered_cache_key = filtered_scan_cache_key(
        &cache_key,
        &allowed_types,
        &selected_nodes,
        &secondary_groups,
        command.scope.as_ref(),
    );
    if let Some(entry) = get_filtered_scan_cache(&filtered_cache_key) {
        let returned_records = entry.records.len();
        let dropped_ref_count = entry.dropped_by_type
            + entry.dropped_by_scope
            + entry.selected_node_dropped
            + entry.secondary_dropped;
        return Ok(json!({
            "ok": true,
            "count": returned_records,
            "records": entry.records,
            "native_candidate_prefilter": true,
            "scan_count": entry.scanned_records,
            "cache_hit": true,
            "cache_hit_used": true,
            "selected_ref_count": 0,
            "dropped_ref_count": dropped_ref_count,
            "scan_stats": {
                "execution_mode": "rust_proxy_native_candidate_prefilter",
                "native_prefix_scan": true,
                "native_scan_record_cache_hit": true,
                "native_filtered_scan_cache_hit": true,
                "native_scan_record_cache_keyed_by_count": true,
                "native_scan_record_cache_key_kind": "serving_count",
                "native_secondary_index_prefilter": !secondary_groups.is_empty(),
                "native_node_path_scope_prefilter": entry.node_path_filter_count > 0,
                "native_node_path_scope_filter_count": entry.node_path_filter_count,
                "scanned_records": entry.scanned_records,
                "total_record_count": count,
                "serving_record_watermark": serving_count,
                "returned_records": returned_records,
                "dropped_by_type": entry.dropped_by_type,
                "dropped_by_scope": entry.dropped_by_scope,
                "selected_node_dropped_candidate_count": entry.selected_node_dropped,
                "secondary_index_groups_supplied": secondary_groups.len(),
                "secondary_index_matched_candidate_count": entry.secondary_matched,
                "secondary_index_dropped_candidate_count": entry.secondary_dropped,
                "native_pack_assembly": false,
                "pack_assembly_location": "python_reference_packer",
                "next_native_gap": "C++/Rust ContextPack scoring and budget assembly APIs"
            }
        }));
    }
    let (records_source, scanned_records, cache_hit) =
        load_scan_records(client, &record_hash_key, shard_size, count, cache_key)?;
    let mut dropped_by_type = 0_u64;
    let mut dropped_by_scope = 0_u64;
    let mut selected_node_dropped = 0_u64;
    let mut node_paths_by_hash: HashMap<u64, Vec<String>> = HashMap::new();
    for record in records_source.iter() {
        if record.get("record_type").and_then(Value::as_str) != Some("context_node") {
            continue;
        }
        if let (Some(node_hash), Some(path)) = (
            record_node_hash(record),
            record.get("node_path").and_then(Value::as_array),
        ) {
            let values: Vec<String> = path
                .iter()
                .filter_map(Value::as_str)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
                .collect();
            if !values.is_empty() {
                node_paths_by_hash.insert(node_hash, values);
            }
        }
    }
    let node_path_filters = query_node_path_filters(command.scope.as_ref());
    let records = records_source
        .iter()
        .filter_map(|record| {
            let record_type = record
                .get("record_type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !allowed_types.is_empty() && !allowed_types.contains(record_type) {
                dropped_by_type += 1;
                return None;
            }
            if !scope_matches_record(record, command.scope.as_ref()) {
                dropped_by_scope += 1;
                return None;
            }
            if !node_path_matches_filters(record, &node_path_filters, &node_paths_by_hash) {
                dropped_by_scope += 1;
                return None;
            }
            if !selected_nodes.is_empty() {
                let keep_index = matches!(record_type, "context_index" | "context_embedding");
                let keep_node = record_node_hash(record)
                    .map(|node| selected_nodes.contains(&node))
                    .unwrap_or(false);
                if !keep_index && !keep_node {
                    selected_node_dropped += 1;
                    return None;
                }
            }
            Some(record.clone())
        })
        .collect::<Vec<_>>();

    let secondary_filter = apply_secondary_prefilter(records, &secondary_groups);
    let filtered = secondary_filter.records;
    let secondary_dropped = secondary_filter.secondary_dropped;
    let secondary_matched = secondary_filter.secondary_matched;

    let dropped_ref_count =
        dropped_by_type + dropped_by_scope + selected_node_dropped + secondary_dropped;
    put_filtered_scan_cache(
        filtered_cache_key,
        FilteredScanCacheEntry {
            records: filtered.clone(),
            scanned_records,
            dropped_by_type,
            dropped_by_scope,
            selected_node_dropped,
            secondary_dropped,
            secondary_matched,
            node_path_filter_count: node_path_filters.len(),
        },
    );
    Ok(json!({
        "ok": true,
        "count": filtered.len(),
        "records": filtered,
        "native_candidate_prefilter": true,
        "scan_count": scanned_records,
        "cache_hit": cache_hit,
        "cache_hit_used": cache_hit,
        "selected_ref_count": 0,
        "dropped_ref_count": dropped_ref_count,
        "scan_stats": {
            "execution_mode": "rust_proxy_native_candidate_prefilter",
            "native_prefix_scan": true,
            "native_scan_record_cache_hit": cache_hit,
            "native_filtered_scan_cache_hit": false,
            "native_scan_record_cache_keyed_by_count": true,
            "native_scan_record_cache_key_kind": "serving_count",
            "native_secondary_index_prefilter": !secondary_groups.is_empty(),
            "native_node_path_scope_prefilter": !node_path_filters.is_empty(),
            "native_node_path_scope_filter_count": node_path_filters.len(),
            "scanned_records": scanned_records,
            "total_record_count": count,
            "serving_record_watermark": serving_count,
            "returned_records": filtered.len(),
            "dropped_by_type": dropped_by_type,
            "dropped_by_scope": dropped_by_scope,
            "selected_node_dropped_candidate_count": selected_node_dropped,
            "secondary_index_groups_supplied": secondary_groups.len(),
            "secondary_index_matched_candidate_count": secondary_matched,
            "secondary_index_dropped_candidate_count": secondary_dropped,
            "native_pack_assembly": false,
            "pack_assembly_location": "python_reference_packer",
            "next_native_gap": "C++/Rust ContextPack scoring and budget assembly APIs"
        }
    }))
}
