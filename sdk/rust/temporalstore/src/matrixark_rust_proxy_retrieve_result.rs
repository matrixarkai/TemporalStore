use serde_json::Value;
use temporalstore::Client;

use crate::matrixark_rust_proxy_native_pack::retrieve_context_pack_via_sdk_native;
use crate::matrixark_rust_proxy_protocol::Command;

pub(crate) enum SdkNativePackAttempt {
    Response(Value),
    FallbackAllowed,
    Error(String),
}

pub(crate) fn try_sdk_native_pack(
    client: &Client,
    command: &Command,
) -> SdkNativePackAttempt {
    let use_sdk_native = std::env::var("MATRIXARK_RUST_PROXY_DISABLE_SDK_NATIVE_PACK")
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(true);
    if !use_sdk_native {
        return SdkNativePackAttempt::FallbackAllowed;
    }
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
