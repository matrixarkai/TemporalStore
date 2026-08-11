// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde_json::{json, Value};

use crate::matrixark_rust_proxy_cache::FilteredScanCacheEntry;

pub(crate) struct ScanResponseInput {
    pub(crate) filtered: Vec<Value>,
    pub(crate) scanned_records: u64,
    pub(crate) cache_hit: bool,
    pub(crate) count: u64,
    pub(crate) serving_count: u64,
    pub(crate) dropped_by_type: u64,
    pub(crate) dropped_by_scope: u64,
    pub(crate) selected_node_dropped: u64,
    pub(crate) secondary_groups_len: usize,
    pub(crate) secondary_matched: u64,
    pub(crate) secondary_dropped: u64,
    pub(crate) node_path_filter_count: usize,
}

pub(crate) fn build_filtered_cache_hit_response(
    entry: FilteredScanCacheEntry,
    count: u64,
    serving_count: u64,
    secondary_groups_len: usize,
) -> Value {
    let returned_records = entry.records.len();
    let dropped_ref_count = entry.dropped_by_type
        + entry.dropped_by_scope
        + entry.selected_node_dropped
        + entry.secondary_dropped;
    json!({
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
            "native_secondary_index_prefilter": secondary_groups_len > 0,
            "native_node_path_scope_prefilter": entry.node_path_filter_count > 0,
            "native_node_path_scope_filter_count": entry.node_path_filter_count,
            "scanned_records": entry.scanned_records,
            "total_record_count": count,
            "serving_record_watermark": serving_count,
            "returned_records": returned_records,
            "dropped_by_type": entry.dropped_by_type,
            "dropped_by_scope": entry.dropped_by_scope,
            "selected_node_dropped_candidate_count": entry.selected_node_dropped,
            "secondary_index_groups_supplied": secondary_groups_len,
            "secondary_index_matched_candidate_count": entry.secondary_matched,
            "secondary_index_dropped_candidate_count": entry.secondary_dropped,
            "native_pack_assembly": false,
            "pack_assembly_location": "python_reference_packer",
            "next_native_gap": "C++/Rust ContextPack scoring and budget assembly APIs"
        }
    })
}

pub(crate) fn build_scan_response(input: ScanResponseInput) -> Value {
    let dropped_ref_count = input.dropped_by_type
        + input.dropped_by_scope
        + input.selected_node_dropped
        + input.secondary_dropped;
    json!({
        "ok": true,
        "count": input.filtered.len(),
        "records": input.filtered,
        "native_candidate_prefilter": true,
        "scan_count": input.scanned_records,
        "cache_hit": input.cache_hit,
        "cache_hit_used": input.cache_hit,
        "selected_ref_count": 0,
        "dropped_ref_count": dropped_ref_count,
        "scan_stats": {
            "execution_mode": "rust_proxy_native_candidate_prefilter",
            "native_prefix_scan": true,
            "native_scan_record_cache_hit": input.cache_hit,
            "native_filtered_scan_cache_hit": false,
            "native_scan_record_cache_keyed_by_count": true,
            "native_scan_record_cache_key_kind": "serving_count",
            "native_secondary_index_prefilter": input.secondary_groups_len > 0,
            "native_node_path_scope_prefilter": input.node_path_filter_count > 0,
            "native_node_path_scope_filter_count": input.node_path_filter_count,
            "scanned_records": input.scanned_records,
            "total_record_count": input.count,
            "serving_record_watermark": input.serving_count,
            "returned_records": input.filtered.len(),
            "dropped_by_type": input.dropped_by_type,
            "dropped_by_scope": input.dropped_by_scope,
            "selected_node_dropped_candidate_count": input.selected_node_dropped,
            "secondary_index_groups_supplied": input.secondary_groups_len,
            "secondary_index_matched_candidate_count": input.secondary_matched,
            "secondary_index_dropped_candidate_count": input.secondary_dropped,
            "native_pack_assembly": false,
            "pack_assembly_location": "python_reference_packer",
            "next_native_gap": "C++/Rust ContextPack scoring and budget assembly APIs"
        }
    })
}
