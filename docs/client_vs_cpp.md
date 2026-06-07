# Client Vs C++ TemporalStore

## C++ Client Shape

The C++ client has these important pieces:

- `ClientOptions` with master address/consul, psm, cluster, idc, meta sync interval, topology retry interval, fetch timeout, and partition pick policy.
- `TableOptions` with IO timeout, connect timeout, and continuous failed time threshold.
- `Client::OpenTable(namespace, table_name)` returning a table handle.
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
- `TemporalStoreTable`
- `TemporalStorePipeline`
- typed table methods for common string/hash flows:
  - `set`
  - `setex`
  - `get`
  - `hset`
  - `hget`
  - `hdel`
  - `del`
  - `expire`
  - `ttl`
- pipeline `sync` backed by `BatchExecuteRequest`
- configurable HTTP connect/read/write timeouts
- configurable HTTP retries
- optional direct metaserver-backed route cache
- route refresh after a cached direct backend fails

The old `TemporalStoreClient::new(proxy_addr)` API still works and routes through the proxy.

## Still Missing

Rust still does not have C++ client wire parity:

- no brpc/protobuf client
- no exact C++ `CmdRequest`/`CmdResponse` command encoding
- no namespace/table topology from metaserver
- no CRC64 slot router
- no partition-set primary/secondary endpoint selection
- no VDC-affinity routing
- no periodic background `MetaSyncer`
- no backend server pool with continuous failure counters
- no Neptune/drop-percent routing behavior
- no async callback API
- no full Risk/IPS typed client methods

The current Rust client is a behavior-compatible open-source layer over the Rust HTTP API, not a drop-in replacement for the internal C++ SDK.

