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
- no namespace/table open path in proxy request bodies
- no full topology/slot router
- no consul registration
- no proxy location/VDC/CMDB integration
- no Risk/IPS thrift method coverage

The current Rust proxy is an HTTP/JSON proxy that wraps the Rust client library for routing/cache/retry behavior. It is suitable for the open-source Rust path, but not yet a wire-compatible replacement for the internal C++ proxy.
