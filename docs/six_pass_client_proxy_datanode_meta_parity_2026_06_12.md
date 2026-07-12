# Six-Pass Client/Proxy/Data-Node/Metaserver Parity Review

Date: 2026-06-12

Branch reviewed: `rust-main`

Rust head reviewed: `30fa170`

## Scope

This review repeats the C++ vs Rust comparison six times across the four serving
surfaces that matter for the current Rust alpha:

- client SDK and CLI
- proxy
- data-node/server
- metaserver

The comparison input was the current Rust source under `crates/temporalstore-rust`,
the existing C++ parity docs in `docs/`, and the local C++ build/source artifacts
available under `build-ubuntu22/src`. The Rust target remains HTTP/JSON plus RESP
and Prometheus for the open-source path; legacy C++ wire are not claimed as Rust
wire parity.

## Pass 1: Client Surface

Compared:

- Rust library client typed methods
- Rust `client` binary
- documented C++ client shape: table open/close, router, pipeline, meta sync, retry,
  hash/string/common/feature/sequence/IPS/Risk families

Findings:

- The Rust library client is much broader than the CLI.
- The CLI could not directly exercise several already-implemented commands, which made
  local C++-style functional testing depend on ad hoc JSON or custom tests.

Filled:

- `client json '<command-json>'` now lets local validation drive any `Command` enum
  variant without waiting for a new binary subcommand.
- The CLI now exposes common missing direct commands:
  `exists`, `sdel`, `setnx`, `setxx`, `hmset`, `hmget`, `hincrby`, `hgetall`,
  `hlen`, `hdel`, `fappendnx`, `fappendxx`, `ipsrange`, `ipsremove`, `ipsdel`,
  `ipscount`, `riskquery`, `riskdetail`, `riskhset`, `cpcset`, `folset`,
  `folquery`, and `riskmanager`.

Remaining client gaps:

- tonic/prost SDK surface for the future Rust wire API
- full C++ partition-set hierarchy and Neptune-specific routing
- C++ legacy framed RPC request/response compatibility

## Pass 2: Proxy Surface

Compared:

- `ProxyService` HTTP routes
- service-name JSON aliases
- table-aware execute and batch execute
- heartbeat/config behavior

Findings:

- Proxy route/cache/retry behavior is implemented through the Rust client.
- C++ service-name aliases exist for generic execute/table execute paths.
- The remaining proxy gaps are mostly wire/service-discovery gaps, not local engine
  gaps.

Filled this pass:

- No proxy code change was needed for local parity because the missing local testability
  issue was on the client CLI side.

Remaining proxy gaps:

- legacy C++ wire framed server compatibility
- command-specific C++ legacy framed RPC method aliases such as `Get`, `Set`, `FeatureAdd`,
  `RiskHset`, `HMGet`, `HMSet`, `HGetAll`, and `HLen`
- consul/service-discovery registration
- full C++ partition-set topology beyond the current open-source table topology model

## Pass 3: Data-Node/Server Surface

Compared:

- `server` binary routes
- `DataNodeRuntime`
- checked execute/batch/load/config/stream/update-membership routes
- C++ `ServerService`-named JSON aliases already present in Rust

Findings:

- The Rust server has the important local control-plane routes for load/unload,
  execute, batch execute, config, info, stats, streams, membership update, ping, and
  data-Raft log apply.
- Runtime queueing, shard worker ownership, cancellation state, dirty-object tracking,
  local compaction/GC hooks, replay, and heartbeat payloads are present.

Filled this pass:

- No direct data-node code change was needed. The local functional gap was client-side
  reachability of already-implemented command variants.

Remaining data-node gaps:

- tonic streaming/callback API
- production page header/zone format compatibility
- binary/protobuf oplog and index-log compatibility
- hard preemptive cancellation of already-running arbitrary user work
- crash recovery golden corpus that proves oplog, index-log, pages, and snapshots
  recover exactly after process death

## Pass 4: Metaserver Surface

Compared:

- namespace/table topology
- server/proxy register/list/heartbeat/freeze/drop
- table create/open/close/delete/update
- scheduler and snapshot routes
- Raft-backed metadata facade

Findings:

- Rust has durable local mutation-log replay, snapshots, Raft-mode mutation forwarding,
  table serving options, load-aware placement, host/location diversity, and scheduler
  admin routes.
- The remaining metaserver gap is not a missing single JSON endpoint; it is the full
  production background workflow loop and multi-process Raft transport.

Filled this pass:

- No metaserver code change was needed.

Remaining metaserver gaps:

- networked multi-process metaserver Raft transport
- background scheduler loop that executes membership plans against real data-node
  processes continuously
- full C++ placement rule chain
- service discovery and production deployment integration

## Pass 5: Redis/Functional Command Reachability

Compared:

- RESP command handler coverage
- Rust `Command` enum coverage
- client CLI coverage used by local function testing

Findings:

- RESP already covers common string/hash/set, feature, sequence, IPS, Risk, FOL, admin,
  and partition smoke commands.
- CLI coverage lagged behind RESP and the library client.

Filled:

- CLI coverage now tracks the implemented command families more closely.
- `json` command support provides a stable escape hatch for future C++ parity tests.

Remaining Redis/function gaps:

- sorted-set/list families are not implemented unless the target Redis contract expands
  to require them
- RESP admin commands such as `PARTITION` are smoke/local-state shims, not a full
  C++ partition-manager backend

## Pass 6: Local Scale Readiness

Compared:

- current scale harness
- parity gate scripts
- distributed Raft harness expectations

Findings:

- The local scale harness is the right validation for current Rust alpha behavior:
  multi-node routing, failover cadence, string/hash/sequence writes, and optional
  shared-store comparison.
- It does not prove production C++ parity for legacy C++ wire, real MatrixObjectStore, OpenRaft or
  raft-rs FSM/storage, AWS multi-node chaos, or crash recovery under disk faults.

Filled:

- The client CLI expansion gives scale and manual validation one binary that can reach
  all implemented command variants, including raw JSON commands.

## Current Recommendation

The Rust path is stronger after this pass, but it should still be described as
open-source Rust alpha parity, not full C++ production parity.

Next gaps to fill before making a stronger parity claim:

1. Add tonic/prost service definitions for client/proxy/server/metaserver.
2. Add crash-recovery golden tests for oplog, index-log, page files, snapshots, and
   metaserver mutation logs.
3. Wire the metaserver scheduler loop to continuously apply membership plans against
   real data-node processes.
4. Replace the local Raft model with a real OpenRaft or raft-rs FSM/storage layer.
5. Build a C++ golden command corpus for feature, sequence, IPS, and Risk edge cases
   and run it through the Rust RESP/client paths.
