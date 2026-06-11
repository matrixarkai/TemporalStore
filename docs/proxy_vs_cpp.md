# Proxy Vs C++ TemporalStore

## C++ Proxy Shape

The C++ proxy is a production front door:

- starts a brpc server with Thrift framed service support
- parses thrift methods such as `Get`, `Set`, `FeatureAdd`, `FeatureQuery`, `RiskHset`, `HMGet`, `HMSet`, `HGetAll`, and `HLen`
- opens tables through the C++ client per namespace/table
- forwards requests through the C++ client's router and backend server pool
- uses the client `MetaSyncer` for topology refresh
- has heartbeat to metaserver
- receives proxy config from metaserver heartbeat responses
- registers/deregisters consul service names
- reports boot time, endpoint, location, binary version, namespace, and config version
- can auto-register with metaserver when heartbeat returns not found

Important local C++ files:

- `/home/vj/temporalstore-native/src/proxy/proxy.cc`
- `/home/vj/temporalstore-native/src/proxy/service.cc`
- `/home/vj/temporalstore-native/src/proxy/heartbeat.cc`
- `/home/vj/temporalstore-native/src/metaserver_v2/meta/proxy.cc`

## Rust Proxy Coverage

Rust now has a reusable `ProxyService`:

- `/execute` forwarding
- `/batch_execute` forwarding
- `/shards/<id>` metaserver lookup
- `/health`
- `/proxy/info`
- `/proxy/config` get/update
- C++ `Proxy::UpdateConfig`-style no-op handling when namespace and config version are unchanged,
  avoiding needless client/cache reset on duplicate heartbeat config
- `/proxy/open_table`, `/proxy/table_execute`, and `/proxy/table_batch_execute` for namespace/table
  request bodies
- C++ service-name JSON aliases for open-source migration tests:
  `/ProxyService/ExecuteCmd`, `/ProxyService/BatchExecuteCmd`, `/ProxyService/OpenTable`,
  `/ProxyService/TableExecuteCmd`, `/ProxyService/ExecuteTableCmd`,
  `/ProxyService/TableBatchExecuteCmd`, and `/ProxyService/BatchExecuteTableCmd`
- table execute paths auto-open table topology from metaserver when the table is not already cached,
  matching the C++ proxy shape where requests can drive client table opening
- forwarding delegates through the Rust `TemporalStoreClient`
- client-owned route cache with TTL
- client-owned route refresh after backend error
- client-owned backend failure pool behavior with continuous-failure windows and cached-backend bypass
- HTTP connect/read/write timeout options
- HTTP retry options
- proxy stats:
  - execute requests
  - batch requests
  - route cache hits/misses
  - route refreshes
  - backend errors
  - continuous backend failures
  - metaserver errors
  - bad requests

The Rust `proxy` binary now delegates to `ProxyService` and supports:

- `TS_PROXY_ROUTE_CACHE_TTL_MS`
- `TS_PROXY_CONNECT_TIMEOUT_MS`
- `TS_PROXY_IO_TIMEOUT_MS`
- `TS_PROXY_MAX_RETRIES`
- `TS_PROXY_REFRESH_ROUTE_ON_BACKEND_ERROR`
- `TS_PROXY_BACKEND_CONTINUOUS_FAILED_TIME_MS`

## Still Missing

Rust proxy is still not a C++ proxy drop-in:

- no brpc server
- no thrift framed parser
- no C++ thrift request/response wire compatibility
- no command-specific thrift method aliases such as `Get`, `Set`, `FeatureAdd`, `RiskHset`,
  `HMGet`, `HMSet`, `HGetAll`, and `HLen`
- no full C++ partition-set topology/slot router beyond the open-source table topology path
- no consul registration
- no proxy location/VDC/CMDB integration

The current Rust proxy is an HTTP/JSON proxy that wraps the Rust client library for routing/cache/retry behavior. It is suitable for the open-source Rust path, but not yet a wire-compatible replacement for the internal C++ proxy.
