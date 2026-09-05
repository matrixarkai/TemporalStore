// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use temporalstore::Client;

use crate::matrixark_rust_proxy_native_pack::retrieve_context_pack_via_sdk_native;
use crate::matrixark_rust_proxy_protocol::Command;

pub(crate) enum SdkNativePackAttempt {
    Response(Value),
    FallbackAllowed,
    Error(String),
}

pub(crate) struct RetrieveSelection {
    pub(crate) selected: Vec<Value>,
    pub(crate) selected_counts: HashMap<String, u64>,
    pub(crate) selected_nodes: HashSet<u64>,
    pub(crate) used_tokens: u64,
    pub(crate) cross_used_tokens: u64,
    pub(crate) cross_selected_refs: u64,
    pub(crate) entity_bridge_selected_refs: u64,
    pub(crate) selected_cross_sessions: HashSet<String>,
    pub(crate) dropped_over_budget: u64,
    pub(crate) dropped_cross_budget: u64,
    pub(crate) dropped_cross_session_cap: u64,
    pub(crate) dropped_cross_candidate_cap: u64,
    pub(crate) dropped_entity_bridge_slot_reserved: u64,
    pub(crate) dropped_low_score: u64,
    pub(crate) dropped_policy_ref: u64,
    pub(crate) dropped_duplicate_ref: u64,
}

pub(crate) fn try_sdk_native_pack(
    client: &Client,
    command: &Command,
) -> SdkNativePackAttempt {
    // The native pack is always attempted; the fallback below is for when it FAILS.
    match retrieve_context_pack_via_sdk_native(client, command) {
        Ok(response) => SdkNativePackAttempt::Response(response),
        Err(err) => {
            let disable_fallback = std::env::var("MATRIXARK_RUST_PROXY_DISABLE_LEGACY_PACK_FALLBACK")
                .map(|value| {
                    matches!(
                        value.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes"
                    )
                })
                .unwrap_or(false);
            if disable_fallback {
                SdkNativePackAttempt::Error(err)
            } else {
                SdkNativePackAttempt::FallbackAllowed
            }
        }
    }
}

pub(crate) fn scan_dropped_count(scan_stats: &Value) -> u64 {
    scan_stats
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
            .unwrap_or(0)
}

pub(crate) fn scan_cache_hit(scan_stats: &Value) -> bool {
    scan_stats
        .get("native_filtered_scan_cache_hit")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || scan_stats
            .get("native_scan_record_cache_hit")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}
