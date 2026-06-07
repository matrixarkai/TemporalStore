# Client Vs C++ TemporalStore

## C++ Client Shape

The C++ client has these important pieces:

- `ClientOptions` with master address/consul, psm, cluster, idc, meta sync interval, topology retry interval, fetch timeout, and partition pick policy.
- `TableOptions` with IO timeout, connect timeout, and continuous failed time threshold.
- `Client::OpenTable(namespace, table_name)` returning a table handle.
- `Client::CloseTable(table)` unregistering that handle from `MetaSyncer`.
- `Table` typed methods: `Set`, `Get`, `SetEx`, `Del`, `Expire`, `Ttl`, `HSet`, `HGet`, `HDel`, plus richer Risk APIs.
- `Pipeline` that queues table commands and sends them as batch requests on `Sync`.
- `MetaSyncer` that periodically fetches table topology from metaserver.
- `Router` that maps `key -> slot -> partition -> primary/secondary endpoint`.
- `BackendServerPool` that caches backend channels, tracks failures, and triggers standalone meta refresh after continuous failures.
- Writes force primary routing; reads may use read replicas depending on partition pick policy.

## Rust Client Coverage

Rust now has:

- `ClientOptions`
- `TableOptions`
- `RequestOptions`
- `TemporalStoreClient::open_table`
- `TemporalStoreClient::close_table`
- table cache/stats for open/close, execute/batch, route cache hit/miss, route refresh, and backend errors
- `TemporalStoreTable`
- `TemporalStorePipeline`
- typed table methods for common string/hash/set/feature/sequence/IPS-lite/risk-lite flows:
  - `exists`
  - `set`
  - `setex`
  - `get`
  - `hset`
  - `hget`
  - `hmget`
  - `hmset`
  - `hincrby`
  - `hgetall`
  - `hlen`
  - `hdel`
  - `del`
  - `expire`
  - `ttl`
  - `sadd`
  - `smembers`
  - `srem`
  - `feature_append`
  - `feature_query`
  - `sequence_add`
  - `sequence_query`
  - `ips_add`
  - `ips_query_last`
  - `risk_increment`
  - `risk_count`
- key-to-shard routing using `TableOptions { first_shard_id, shard_count }`
- pipeline `sync` backed by `BatchExecuteRequest`
- pipeline grouping by routed shard, with responses reassembled in original command order
- configurable HTTP connect/read/write timeouts
- configurable HTTP retries
- optional direct metaserver-backed route cache
- route refresh after a cached direct backend fails

The old `TemporalStoreClient::new(proxy_addr)` API still works and routes through the proxy.

## Still Missing

Rust still does not have C++ client wire parity:

- no brpc/protobuf client
- no exact C++ `CmdRequest`/`CmdResponse` command encoding
- no brpc/protobuf wire-compatible backend pool; Rust now uses the C++ slot formula `crc64_signed(0, key) >> 34` for table shard routing
- no partition-set primary/secondary endpoint selection
- no VDC-affinity routing
- no backend server pool with continuous failure counters
- no Neptune/drop-percent routing behavior
- no async callback API
- no full Risk/IPS typed client methods; Rust exposes only the simplified commands currently implemented by the engine

The current Rust client is a behavior-compatible open-source layer over the Rust HTTP API, not a drop-in replacement for the internal C++ SDK.
