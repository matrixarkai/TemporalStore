// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use temporalstore_rust::http::serve;
use temporalstore_rust::{ProxyOptions, ProxyService, ProxyServingMode};
use tracing::info;

fn main() {
    temporalstore_rust::telemetry::init();
    let addr = std::env::var("TS_PROXY_BIND_ADDR")
        .or_else(|_| std::env::var("TS_PROXY_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17000".to_string());
    let meta_addr = std::env::var("TS_META_ADDR").unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    // Every environment fallback below reads its default from here rather than repeating a
    // literal. Repeating them meant the binary could disagree with the option's declared
    // default and nothing would say so: context_shard_count was declared 0, meaning "follow
    // the cluster", while this file passed 1 -- so the deployed proxy always looked explicitly
    // configured and the cluster-following it was given never ran. Tests did not catch it
    // because they build ProxyOptions directly and get the real default.
    let defaults = ProxyOptions::default();
    let options = ProxyOptions {
        meta_addr,
        proxy_addr: std::env::var("TS_PROXY_ADVERTISED_ADDR").unwrap_or_else(|_| addr.clone()),
        listen_addr: addr.clone(),
        config_version: env_u64("TS_PROXY_CONFIG_VERSION", defaults.config_version),
        namespace: std::env::var("TS_PROXY_NAMESPACE").unwrap_or_default(),
        location: std::env::var("TS_PROXY_LOCATION").unwrap_or_default(),
        binary_version: std::env::var("TS_PROXY_BINARY_VERSION")
            .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()),
        route_cache_ttl_ms: env_u64("TS_PROXY_ROUTE_CACHE_TTL_MS", defaults.route_cache_ttl_ms),
        connect_timeout_ms: env_u64("TS_PROXY_CONNECT_TIMEOUT_MS", defaults.connect_timeout_ms),
        io_timeout_ms: env_u64("TS_PROXY_IO_TIMEOUT_MS", defaults.io_timeout_ms),
        max_retries: env_usize("TS_PROXY_MAX_RETRIES", defaults.max_retries),
        refresh_route_on_backend_error: env_bool(
            "TS_PROXY_REFRESH_ROUTE_ON_BACKEND_ERROR",
            defaults.refresh_route_on_backend_error,
        ),
        backend_continuous_failed_time_ms: env_u64(
            "TS_PROXY_BACKEND_CONTINUOUS_FAILED_TIME_MS",
            defaults.backend_continuous_failed_time_ms,
        ),
        service_registry_ttl_ms: env_u64("TS_PROXY_SERVICE_REGISTRY_TTL_MS", defaults.service_registry_ttl_ms),
        serving_mode: env_serving_mode("TS_PROXY_SERVING_MODE", defaults.serving_mode),
        drop_percent: env_u8("TS_PROXY_DROP_PERCENT", defaults.drop_percent),
        ingestion_account: std::env::var("TS_PROXY_INGESTION_ACCOUNT").unwrap_or_default(),
        enforce_ingestion_account: env_bool(
            "TS_PROXY_ENFORCE_INGESTION_ACCOUNT",
            defaults.enforce_ingestion_account,
        ),
        max_inflight_requests: env_u64("TS_PROXY_MAX_INFLIGHT_REQUESTS", defaults.max_inflight_requests),
        max_inflight_write_requests: env_u64(
            "TS_PROXY_MAX_INFLIGHT_WRITE_REQUESTS",
            defaults.max_inflight_write_requests,
        ),
        pin_primary_reads: env_bool("TS_PROXY_PIN_PRIMARY_READS", defaults.pin_primary_reads),
        heartbeat_timeout_ms: env_u64("TS_PROXY_HEARTBEAT_TIMEOUT_MS", defaults.heartbeat_timeout_ms),
        heartbeat_interval_ms: env_u64(
            "TS_PROXY_HEARTBEAT_INTERVAL_MS",
            defaults.heartbeat_interval_ms,
        ),
        topology_check_interval_ms: env_u64(
            "TS_PROXY_TOPOLOGY_CHECK_INTERVAL_MS",
            defaults.topology_check_interval_ms,
        ),
        auto_register_min_interval_ms: env_u64(
            "TS_PROXY_AUTO_REGISTER_MIN_INTERVAL_MS",
            defaults.auto_register_min_interval_ms,
        ),
        context_first_shard_id: env_u64("TS_PROXY_CONTEXT_FIRST_SHARD", defaults.context_first_shard_id),
        context_shard_count: env_u64("TS_PROXY_CONTEXT_SHARD_COUNT", defaults.context_shard_count),
        context_io_timeout_ms: env_u64("TS_PROXY_CONTEXT_IO_TIMEOUT_MS", defaults.context_io_timeout_ms),
    };
    let proxy = ProxyService::new(options);
    let _heartbeat_loop = proxy.start_heartbeat_loop();
    info!(%addr, "temporalstore proxy listening");
    serve(&addr, move |request| proxy.handle(request)).expect("proxy failed");
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_serving_mode(name: &str, default: ProxyServingMode) -> ProxyServingMode {
    std::env::var(name)
        .ok()
        .and_then(
            |value| match value.to_ascii_lowercase().replace('-', "_").as_str() {
                "serving" => Some(ProxyServingMode::Serving),
                "readonly" | "read_only" => Some(ProxyServingMode::Readonly),
                "write_disabled" => Some(ProxyServingMode::WriteDisabled),
                "degraded" => Some(ProxyServingMode::Degraded),
                "not_serving" | "disabled" => Some(ProxyServingMode::NotServing),
                _ => None,
            },
        )
        .unwrap_or(default)
}
