use temporalstore_rust::http::serve;
use temporalstore_rust::{ProxyOptions, ProxyService};

fn main() {
    let addr = std::env::var("TS_PROXY_BIND_ADDR")
        .or_else(|_| std::env::var("TS_PROXY_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17000".to_string());
    let meta_addr = std::env::var("TS_META_ADDR").unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    let options = ProxyOptions {
        meta_addr,
        proxy_addr: std::env::var("TS_PROXY_ADVERTISED_ADDR").unwrap_or_else(|_| addr.clone()),
        config_version: env_u64("TS_PROXY_CONFIG_VERSION", 0),
        namespace: std::env::var("TS_PROXY_NAMESPACE").unwrap_or_default(),
        location: std::env::var("TS_PROXY_LOCATION").unwrap_or_default(),
        binary_version: std::env::var("TS_PROXY_BINARY_VERSION")
            .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()),
        route_cache_ttl_ms: env_u64("TS_PROXY_ROUTE_CACHE_TTL_MS", 1_000),
        connect_timeout_ms: env_u64("TS_PROXY_CONNECT_TIMEOUT_MS", 200),
        io_timeout_ms: env_u64("TS_PROXY_IO_TIMEOUT_MS", 200),
        max_retries: env_usize("TS_PROXY_MAX_RETRIES", 0),
        refresh_route_on_backend_error: env_bool("TS_PROXY_REFRESH_ROUTE_ON_BACKEND_ERROR", true),
        backend_continuous_failed_time_ms: env_u64(
            "TS_PROXY_BACKEND_CONTINUOUS_FAILED_TIME_MS",
            10_000,
        ),
    };
    let proxy = ProxyService::new(options);
    let heartbeat_interval_ms = env_u64("TS_PROXY_HEARTBEAT_INTERVAL_MS", 10_000);
    let _heartbeat_loop = proxy.start_heartbeat_loop(heartbeat_interval_ms);
    println!("temporalstore proxy listening on {addr}");
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
