// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::client::{ClientOptions, TemporalStoreClient};

use super::ProxyOptions;

pub(super) fn default_proxy_addr() -> String {
    "127.0.0.1:17000".to_string()
}

pub(super) fn default_service_registry_ttl_ms() -> u64 {
    30_000
}

pub(super) fn default_pin_primary_reads() -> bool {
    true
}

pub(super) fn default_context_first_shard_id() -> crate::types::ShardId {
    1
}

pub(super) fn default_context_shard_count() -> u64 {
    // 0 = follow the cluster. See `ProxyOptions::context_shard_count`.
    0
}

pub(super) fn default_heartbeat_interval_ms() -> u64 {
    // Unchanged from what `main` passed before this became configuration.
    10_000
}

pub(super) fn default_heartbeat_timeout_ms() -> u64 {
    5_000
}

pub(super) fn default_context_io_timeout_ms() -> u64 {
    30_000
}

pub(super) fn default_topology_check_interval_ms() -> u64 {
    50
}

pub(super) fn default_auto_register_min_interval_ms() -> u64 {
    60_000
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub(super) fn proxy_client_from_options(options: &ProxyOptions) -> TemporalStoreClient {
    TemporalStoreClient::with_options(ClientOptions {
        proxy_addr: options.proxy_addr.clone(),
        meta_addr: Some(options.meta_addr.clone()),
        connect_timeout_ms: options.connect_timeout_ms,
        io_timeout_ms: options.io_timeout_ms,
        max_retries: options.max_retries,
        route_cache_ttl_ms: options.route_cache_ttl_ms,
        topo_error_retry_interval_ms: options.backend_continuous_failed_time_ms,
        drop_percent: options.drop_percent.min(100),
        // Previously never passed through: the proxy accepted this option, defaulted it,
        // read it from TS_PROXY_REFRESH_ROUTE_ON_BACKEND_ERROR and folded it into the config
        // hash, while the client refreshed unconditionally. Setting it to false changed
        // nothing and said nothing.
        refresh_route_on_backend_error: options.refresh_route_on_backend_error,
        // Where this proxy is. The client falls back to it when a table does not name its
        // own `preferred_location`, and that value is what `choose_cached_route` uses to
        // pick a replica -- so without it a proxy configured with a location got no locality
        // preference at all and read cross-zone. The proxy already reported this location to
        // the metaserver and in its own status; it just never reached the thing that routes.
        local_location: options.location.clone(),
        ..ClientOptions::default()
    })
}

pub(super) fn proxy_config_version(options: &ProxyOptions) -> u64 {
    if options.config_version != 0 {
        return options.config_version;
    }
    // Derived from the WHOLE options document, not from a hand-listed subset of it.
    //
    // Whether a pushed config is applied at all is decided by comparing this version to
    // the running one. The list this used to hash left five fields out -- among them
    // `context_shard_count` and `context_first_shard_id`, which decide where every
    // tenant's context is routed -- so a push that changed only one of those hashed to
    // the same number, and `update_options_report` answered "unchanged" and dropped it.
    // The operator was told, in the report, that their change was a no-op.
    //
    // Hashing the document covers every field, including ones added after this was
    // written, which a list cannot do: the list was already five fields behind when this
    // was found, and nothing said so.
    //
    // `config_version` is removed first -- it is this function's answer, not an input.
    let mut document = serde_json::to_value(options).unwrap_or(serde_json::Value::Null);
    if let serde_json::Value::Object(fields) = &mut document {
        fields.remove("config_version");
    }
    let mut version = 1469598103934665603u64;
    for byte in serde_json::to_vec(&document).unwrap_or_default() {
        version ^= byte as u64;
        version = version.wrapping_mul(1099511628211);
    }
    version
}

