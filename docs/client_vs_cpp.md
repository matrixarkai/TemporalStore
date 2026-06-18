# Client Vs C++ TemporalStore

## C++ Client Shape

The C++ client has these important pieces:

- `ClientOptions` with master address/consul, psm, cluster, idc, meta sync interval, topology retry interval, fetch timeout, and partition pick policy.
- `TableOptions` with IO timeout, connect timeout, and continuous failed time threshold.
- `TableOptions` also carries `pin_primary` and a replica read policy. The default stays
  primary-only; callers can opt into first-replica reads.
- `Client::OpenTable(namespace, table_name)` returning a table handle.
- `Client::CloseTable(table)` unregistering that handle from `MetaSyncer`.
- `Table` typed methods: `Set`, `Get`, `SetEx`, `Del`, `Expire`, `Ttl`, `HSet`, `HGet`, `HDel`, Feature, IPS, and richer Risk APIs.
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
- close-table unregisters the table from the local meta-sync table cache and evicts cached routes,
  avoiding stale table routing after close
- C++ wrapper-style status retry policy: retryable backend statuses such as `retry_later`,
  `partition_loading`, `meta_changed`, `topom_error`, `unavailable`, `deadline_exceeded`, and
  `internal` are retried with separate read/write budgets and linear backoff; reads default to one
  retry, writes default to zero retries
- `TemporalStoreTable`
- `TemporalStorePipeline`
- typed table methods for common string/hash/set/feature/sequence/IPS/risk flows:
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
  - `feature_append_with_policy`
  - `feature_query`
  - `feature_replace`
  - `feature_delete`
  - `feature_agg_query`
  - `sequence_add`
  - `sequence_query`
  - `sequence_batch_query`
  - `ips_add`
  - `ips_add_with_options`
  - `ips_query_last`
  - `ips_query_range`
  - `ips_query_range_with_options`
  - `ips_batch_query_last`
  - `ips_remove`
  - `ips_delete`
  - `ips_count`
  - `risk_increment`
  - `risk_increment_with_options`
  - `risk_count`
  - `risk_query`
  - `risk_detail`
- key-to-shard routing using `TableOptions { first_shard_id, shard_count }`
- pipeline `sync` backed by `BatchExecuteRequest`
- pipeline grouping by routed shard, with responses reassembled in original command order
- configurable HTTP connect/read/write timeouts
- configurable HTTP retries
- optional direct metaserver-backed route cache
- topology-backed route cache with primary plus replica endpoints from metaserver partitions
- route refresh after a cached direct backend fails
- backend failure pool behavior with per-backend continuous-failure windows from `TableOptions.continuous_failed_time_ms`
- writes force primary routing; reads stay primary by default and can route to the first secondary
  when `pin_primary = false` and `replica_read_policy = FirstReplica`
- VDC/location-affinity routing for replica reads: `ClientOptions.local_location` seeds
  `TableOptions.preferred_location`, and the route cache prefers a matching-location replica when
  metaserver topology includes endpoint locations
- deterministic drop-percent traffic shedding: `ClientOptions.drop_percent` seeds
  `TableOptions.drop_percent`, and table execute/batch paths reject sampled keys with
  `traffic_dropped` before contacting a backend
- stats for backend errors, backend error streaks, continuous backend failures, and successful retry recovery

The old `TemporalStoreClient::new(proxy_addr)` API still works and routes through the proxy.

## Still Missing

Rust still does not have C++ client wire parity:

- no brpc/protobuf client
- no exact C++ `CmdRequest`/`CmdResponse` command encoding
- no brpc/protobuf wire-compatible backend pool; Rust now uses the C++ slot formula `crc64_signed(0, key) >> 34` for table shard routing
- no full partition-set hierarchy, but Rust now has primary/secondary endpoint selection from
  metaserver table topology for the open-source route model
- Neptune/deployment placement hooks are present for the Rust-native route model, but not the full
  internal C++ partition-set hierarchy
- no async callback API
- no full internal C++ Risk/IPS proto semantics such as manager/debug APIs, CPC/list-specific behavior, IPS load/snapshot/stat/filter, or server aggregation

The current Rust client is a behavior-compatible open-source layer over the Rust HTTP API, not a drop-in replacement for the internal C++ SDK.
