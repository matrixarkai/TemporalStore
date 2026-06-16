# Rust-Native Client SDK Contract

## Purpose

This document defines the open-source Rust production SDK contract that must stay aligned with the
existing HTTP/JSON client and proxy paths. It is not a claim of legacy C++ wire compatibility. The
production target is a Rust-native API surface with a stable schema, generated SDK bindings, and
the same logical behavior as the shared C++/Rust corpus.

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
`OpenTable`, `SyncTopology`, and `GetClientPreflight` are exposed by the generated trait but remain
production blockers until they delegate to the existing client/proxy/metaserver runtime paths.

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

This closes the "versioned Rust-native SDK contract" and "generated tonic/prost binding type"
sub-gaps. It also closes the first runtime adapter sub-gap for `Execute` and `BatchExecute`.
Production readiness still blocks on:

- runtime tonic `OpenTable`, `SyncTopology`, and `GetClientPreflight` adapters wired to the
  existing client/proxy/metaserver execution paths
- full C++ partition-set hierarchy and Neptune-specific routing, if required by a deployment
- wire-compatible migration for existing C++ client callers, if existing callers must migrate
  without adapting to the Rust-native schema

The readiness gate must continue to report the client as blocked until those remaining capabilities
are implemented and locally validated.
