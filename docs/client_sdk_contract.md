# Rust-Native Client SDK Contract

## Purpose

This document defines the open-source Rust production SDK contract that must stay aligned with the
existing HTTP/JSON client and proxy paths. It is not a claim of legacy C++ wire compatibility. The
production target is a Rust-native API surface with a stable schema, generated SDK bindings, and
the same logical behavior as the shared C++/Rust corpus.

The client/proxy wire-compatibility decision is explicit: legacy C++ wire migration shims stay out of
scope for this pass. Existing C++ callers migrate through the Rust-native HTTP/JSON, RESP, or tonic
contract while preserving typed table clients, topology sync, retry budgets, Neptune routing hooks,
deployment placement hooks, proxy admission, route quarantine, topology-version invalidation, and
heartbeat/config application behavior.

The Rust client route cache models the C++ partition-set hierarchy as Rust-native data: table id,
combine name, C++ partition-id mode, partition version, member partition ids, slot ranges,
primary/replica endpoints, topology version, refresh reason, and missing-route counts are exposed
through client preflight and route-cache reports.

The versioned schema lives at:

```text
proto/temporalstore/v1/temporalstore.proto
```

## Required Service Surface

The `TemporalStoreService` contract contains five required RPCs:

- `Execute`: single command execution with shard, trace id, status, response, and topology version.
- `BatchExecute`: ordered batch execution with one response per command.
- `OpenTable`: namespace/table open that returns table topology and serving policy.
- `SyncTopology`: client MetaSyncer refresh API with topology-version and per-call deadline fields.
- `GetClientPreflight`: client health and route-cache inspection for readiness and degraded mode.

The same schema also defines the Rust-native `DataNodeService` contract for production data-node
adapters:

- `ExecuteStream`: bidirectional shard-affine execute stream for command and batch requests.
- `LifecycleCallbacks`: streaming load/reload/unload callback channel with explicit ack status.
- `WatchJobStatus`: server-streamed async job status updates for lifecycle and execute jobs.

The Rust-native `ProxyService` contract covers proxy streaming and route callback shape:

- `ProxyExecuteStream`: bidirectional proxy execute/topology stream.
- `RouteCallbacks`: streaming route-refresh callback channel with explicit ack status.
- `WatchProxyPreflight`: server-streamed proxy readiness/degraded-mode updates.

Proxy streaming maturity is part of the replacement contract, not only a schema presence check. The
Rust proxy exposes `/proxy/tonic_contract` and `/ProxyService/GetTonicContract` with evidence for:

- long-running request support through bounded in-flight stream requests and per-request timeout;
- client cancellation with a gRPC cancelled status and callback ack fence;
- server backpressure with `resource_exhausted` admission status;
- reconnect behavior with jitterable exponential backoff windows for callback streams.

The committed schema is the source of truth for generated tonic/prost bindings. The Rust crate
generates client and server binding types at build time from `crates/temporalstore-rust/build.rs`
and exports them through `temporalstore_rust::sdk::v1`. The schema and generation path are
validated against the existing Rust paths and docs by `tools/validate_sdk_contract.py`.

## HTTP/JSON Mapping

The current Rust implementation serves the same logical operations through HTTP/JSON:

- proxy/server execute routes map to `Execute`.
- proxy/server batch routes map to `BatchExecute`.
- proxy table-open and metaserver table topology routes map to `OpenTable`.
- metaserver topology-version/table-topology routes map to `SyncTopology`.
- client/proxy/admin preflight routes map to `GetClientPreflight`.

The HTTP implementation must preserve command names, status codes, response ordering, topology
version refresh, readonly/write-disabled rejection, and retry-safe stale-route behavior while the
gRPC bindings are being wired.

`TemporalStoreTonicAdapter` currently wires generated `Execute` and `BatchExecute` calls through
protobuf-to-internal command conversion and delegates them to the existing engine execution path.
`OpenTable`, `SyncTopology`, and `GetClientPreflight` delegate to the existing
`TemporalStoreClient` table, topology refresh, and preflight paths.

## Test-Backed Replacement Matrix

The replacement contract is readiness-eligible only when every Rust-native surface is backed by
tests or validators:

- HTTP/JSON replacement is covered by proxy/server execute, batch, table-open, topology refresh,
  preflight, and C++ service-name JSON alias tests.
- RESP replacement is covered by the Redis command corpus and RESP adapter tests for string, hash,
  set, Feature, Sequence, IPS, Risk, admin, and Context-facing migration paths.
- tonic replacement is covered by generated `temporalstore.v1` bindings plus adapter tests for
  `Execute`, `BatchExecute`, `OpenTable`, `SyncTopology`, `GetClientPreflight`, proxy streaming,
  route callbacks, preflight watch surfaces, long-running request, cancellation, backpressure, and
  reconnect evidence.
- typed client migration is covered by `TemporalStoreClient`, `TemporalStoreTable`, and pipeline
  tests that route common, Feature, Sequence, IPS, Risk, and Context commands through the same
  logical contract.
- topology sync and route invalidation are covered by MetaSyncer, topology-version refresh,
  stale-route invalidation, C++ partition-set/member/version route-cache tests, proxy route
  refresh, and route-quarantine tests.
- MetaSyncer production behavior is covered by deadline-limited topology calls, exponential backoff with deterministic jitter, metaserver outage survival, and topology-version route churn refresh tests.
- retry budgets are covered by separate read/write retry-budget tests, including the no-duplicate
  unsafe write retry path.
- pipeline parity is covered by ordered batch execution, per-command partial-failure preservation,
  retry-safe versus unsafe write classification, and timeout-budget propagation tests.
- admission policy is covered by readonly, write-disabled, not-serving, drop-percent, and degraded
  proxy/client preflight tests.
- migration docs are validated by `tools/validate_sdk_contract.py` and readiness tests that require
  the HTTP/JSON, RESP, tonic, typed client, topology sync, retry budget, route invalidation,
  admission policy, alias, and docs evidence flags.

## Command Coverage

The v1 contract includes the production command families covered by the shared corpus and local
readiness work:

- common string, TTL, existence, and delete commands
- hash multi-set/multi-get commands
- set add/member commands
- packed timestamp/value Feature pages
- Sequence rows in the C++ feature-row shape
- IPS timeline add/query
- Risk counter/window query
- Context node upsert/get
- Redis-compatible aliases and admin-facing migration commands

For every supported command family above, the C++ caller migration contract is HTTP/JSON, RESP, or
tonic replacement. The client compatibility report exposes the supported command families so
readiness tests can reject accidental undocumented C++-only command paths.

New production command families must update all of these in the same change:

- `proto/temporalstore/v1/temporalstore.proto`
- `compat/unified_temporalstore_cases.json` when behavior is externally observable
- Rust client/proxy/server command handling
- C++ native corpus runner support, when the command is part of C++ parity
- `tools/validate_sdk_contract.py`

## Readiness Status

This closes the "versioned Rust-native SDK contract", "generated tonic/prost binding type", and
"Rust-native HTTP/JSON, RESP, and tonic migration contract" sub-gaps. It also closes the runtime
tonic adapter sub-gap for `Execute`, `BatchExecute`, `OpenTable`, `SyncTopology`, and
`GetClientPreflight`. The client/proxy readiness gate treats the Rust-native replacement contract
as ready when the compatibility-result evidence is present. Broader global production readiness can
still be blocked by deployment-scale or closed-scope gates outside the client/proxy slice.

Legacy C++ wire-compatible migration for existing C++ client callers remains explicitly out of
scope for the Rust-native schema. The C++ partition-set hierarchy is covered behaviorally by the
Rust route-cache/preflight model; legacy wire protocols remain a separate compatibility target if a
deployment later reopens them.
