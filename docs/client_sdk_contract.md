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
  route callbacks, and preflight watch surfaces.
- typed client migration is covered by `TemporalStoreClient`, `TemporalStoreTable`, and pipeline
  tests that route common, Feature, Sequence, IPS, Risk, and Context commands through the same
  logical contract.
- topology sync and route invalidation are covered by MetaSyncer, topology-version refresh,
  stale-route invalidation, proxy route refresh, and route-quarantine tests.
- retry budgets are covered by separate read/write retry-budget tests, including the no-duplicate
  unsafe write retry path.
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
scope for the Rust-native schema. If a deployment later requires the full C++ partition-set
hierarchy or legacy wire protocols, that must be reopened as a separate compatibility target rather
than silently weakening this contract.
