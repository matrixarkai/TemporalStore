use temporalstore_rust::serve_redis_proxy;

fn main() {
    let addr = std::env::var("TS_REDIS_BIND_ADDR")
        .or_else(|_| std::env::var("TS_REDIS_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:16379".to_string());
    let proxy_addr =
        std::env::var("TS_PROXY_ADDR").unwrap_or_else(|_| "127.0.0.1:17000".to_string());
    let shard_id = std::env::var("TS_SHARD_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    println!(
        "temporalstore redis proxy listening on {addr}, routing shard {shard_id} via {proxy_addr}"
    );
    serve_redis_proxy(&addr, proxy_addr, shard_id).expect("redis proxy failed");
}
