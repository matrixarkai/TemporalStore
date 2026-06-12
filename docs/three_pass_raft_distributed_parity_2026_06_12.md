# Three-Pass Raft Distributed Parity Review

Date: 2026-06-12

Branch reviewed: `rust-main`

## Scope

This review repeats the Raft parity comparison three times, focused only on distributed
replication and failover:

- C++ `RaftControlService` and `ServerService::ApplyDataRaftLog` shapes from the local
  generated C++ protocol artifacts under `build-ubuntu22/src/protocol`
- Rust standalone `raft_node`
- Rust raft-enabled `server`
- local distributed Raft harnesses

The Rust path remains HTTP/JSON for the open-source control plane. This pass does not claim
brpc/protobuf wire compatibility.

## Pass 1: C++ Control Surface

Compared C++:

- `RaftControlService.AddNode`
- `RaftControlService.RemoveNode`
- `RaftControlService.ListMembership`
- `RaftControlService.TriggerSnapshot`
- `ServerService.ApplyDataRaftLog`

Rust coverage before this pass:

- standalone `raft_node` exposed list/add/remove/trigger-snapshot
- raft-enabled `server` exposed list/add/remove/trigger-snapshot
- both exposed `read_index` and `transfer_leader` extensions
- `server` exposed `ApplyDataRaftLog` through `/ServerService/ApplyDataRaftLog`

Gap found:

- standalone `raft_node` had `/raft/control/accept_leadership`, but raft-enabled `server`
  did not expose the same route. That meant integrated data-node Raft lacked one process-boundary
  leadership handoff hook already present in the standalone Raft process.

Filled:

- raft-enabled `server` now exposes `POST /raft/control/accept_leadership`.
- The route rejects requests for a different node id, catches up the local node, and transfers
  leadership to that local node, matching the standalone `raft_node` route behavior.

## Pass 2: Distributed Replication And Failover

Compared Rust behavior:

- network AppendEntries catch-up
- follower write rejection
- leader transfer
- majority-side writes while a follower is down
- safe scale down and scale up
- external snapshot publish/bootstrap catch-up
- commit-to-apply health reporting

Result:

- Existing distributed harness coverage already exercises these flows.
- The server-side leadership-accept route closes a surface parity gap but does not change the
  underlying consensus model.

Still open:

- real OpenRaft or raft-rs FSM/storage integration
- production durable log-store adapter beyond the local segmented WAL model
- production mTLS transport implementation
- disk-pressure and packet-loss chaos outside the local model

## Pass 3: Local Validation Boundary

Validated locally:

- targeted raft-enabled `server` route test for `/raft/control/accept_leadership`
- full `server` binary tests
- full `raft_node` binary tests
- distributed in-process Raft harness
- real OS-process secondary replication/failover harness

What the local validation proves:

- the integrated server and standalone raft-node control surfaces now match for membership,
  snapshot, read-index, transfer-leader, and accept-leadership control routes
- distributed replication and failover still work after the route addition
- commit-to-apply health remains green in the harnesses

What it does not prove:

- C++ brpc/protobuf wire compatibility
- production OpenRaft/raft-rs storage behavior
- external multi-host network partitions, disk-full behavior, or packet loss
- production engine freeze/flush/snapshot install lifecycle

## Recommendation

Keep describing the current Rust implementation as local-model distributed Raft parity plus
process-boundary control-plane coverage. Do not call it production C++ Raft parity until the real
consensus/storage engine, mTLS transport, and external chaos validation land.
