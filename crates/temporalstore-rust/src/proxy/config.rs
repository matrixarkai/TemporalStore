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
        ..ClientOptions::default()
    })
}

pub(super) fn proxy_config_version(options: &ProxyOptions) -> u64 {
    if options.config_version != 0 {
        return options.config_version;
    }
    let mut version = 1469598103934665603u64;
    let view = ProxyConfigHashView {
        meta_addr: &options.meta_addr,
        proxy_addr: &options.proxy_addr,
        namespace: &options.namespace,
        location: &options.location,
        binary_version: &options.binary_version,
        route_cache_ttl_ms: options.route_cache_ttl_ms,
        connect_timeout_ms: options.connect_timeout_ms,
        io_timeout_ms: options.io_timeout_ms,
        max_retries: options.max_retries,
        refresh_route_on_backend_error: options.refresh_route_on_backend_error,
        backend_continuous_failed_time_ms: options.backend_continuous_failed_time_ms,
    };
    for byte in serde_json::to_vec(&view).unwrap_or_default() {
        version ^= byte as u64;
        version = version.wrapping_mul(1099511628211);
    }
    version
}

#[derive(Serialize)]
struct ProxyConfigHashView<'a> {
    meta_addr: &'a str,
    proxy_addr: &'a str,
    namespace: &'a str,
    location: &'a str,
    binary_version: &'a str,
    route_cache_ttl_ms: u64,
    connect_timeout_ms: u64,
    io_timeout_ms: u64,
    max_retries: usize,
    refresh_route_on_backend_error: bool,
    backend_continuous_failed_time_ms: u64,
}
