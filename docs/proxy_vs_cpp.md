# Proxy Vs C++ TemporalStore

## C++ Proxy Shape

The C++ proxy is a production front door:

- starts a legacy C++ RPC server with framed service support
- parses legacy C++ framed methods such as `Get`, `Set`, `FeatureAdd`, `FeatureQuery`, `RiskHset`, `HMGet`, `HMSet`, `HGetAll`, and `HLen`
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
- C++ heartbeat-control-loop behavior for the open-source config model: when metaserver heartbeat
  returns a newer namespace/config version, the proxy adopts it and rebuilds the underlying client
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
- tracked C++ proxy migration decision: legacy C++ command transport is explicitly out of scope,
  and HTTP/JSON aliases plus tonic are the production replacement
- `/proxy/cpp_migration_contract` and `/ProxyService/GetCppMigrationContract` expose the migration
  contract, including topology-version invalidation, admission policy, route quarantine, and
  heartbeat/config behavior preservation
- `/proxy/operational_surface` and `/ProxyService/GetOperationalSurface` compare the C++ proxy
  admin/config/heartbeat/status surface one by one against Rust-native replacement routes
- `/proxy/ports` and `/ProxyService/GetPorts` expose the Rust listen/announce port shape matching
  the C++ `GetListenPort`/`GetAnnouncePort` operational use case
- `/proxy/consul_names` and `/ProxyService/GetConsulNames` expose deterministic Rust service
  registry names while keeping legacy Consul out of scope
- `/proxy/notify_stop` and `/ProxyService/NotifyStop` mark local service-discovery state stopped;
  metaserver proxy freeze/drop APIs remain the Rust production drain/remove path

The Rust `proxy` binary now delegates to `ProxyService` and supports:

- `TS_PROXY_ROUTE_CACHE_TTL_MS`
- `TS_PROXY_CONFIG_VERSION`
- `TS_PROXY_CONNECT_TIMEOUT_MS`
- `TS_PROXY_IO_TIMEOUT_MS`
- `TS_PROXY_MAX_RETRIES`
- `TS_PROXY_REFRESH_ROUTE_ON_BACKEND_ERROR`
- `TS_PROXY_BACKEND_CONTINUOUS_FAILED_TIME_MS`

## Wire Compatibility Decision

Rust keeps the Rust-native proxy surface instead of implementing legacy C++ command transport in
this pass. The production replacement is:

- HTTP/JSON proxy routes and C++ service-name JSON aliases for migration tests.
- tonic `temporalstore.v1.ProxyService` streaming/callback shape.
- Existing topology-version invalidation, admission policy, backend quarantine/recovery, and
  heartbeat/config readiness behavior remain preserved.
- Operational JSON aliases for C++ admin/config/heartbeat/status workflows: `GetInfo`,
  `GetConfig`, `UpdateConfig`, `Heartbeat`, `Preflight`, `GetPolicy`, `GetPorts`,
  `GetConsulNames`, `NotifyStop`, `GetOperationalSurface`, and `Metrics`.

This decision is tracked in code through `ProxyCppMigrationContract` and readiness fields. The
readiness gate can therefore distinguish "decision documented and Rust-native replacement ready"
from "legacy C++ wire-compatible proxy transport still not implemented."

## Still Missing

Rust proxy is still not a C++ proxy drop-in:

- no legacy C++ RPC server
- no legacy framed parser
- no legacy C++ framed request/response wire compatibility
- no legacy framed method compatibility; Rust exposes JSON aliases for `Get`, `Set`, `FeatureAdd`,
  `RiskHset`, `HMGet`, `HMSet`, `HGetAll`, and `HLen` instead
- no full C++ partition-set topology/slot router beyond the open-source table topology path
- no live Consul registration; Rust exposes deterministic service-registry name/status reports
- no proxy location/VDC/CMDB integration

The current Rust proxy is an HTTP/JSON proxy plus Rust-native tonic `ProxyService` streaming/callback
contract that wraps the Rust client library for routing/cache/retry behavior. It is suitable for the
open-source Rust path, but not a legacy C++ wire-compatible replacement for the internal C++ proxy.
