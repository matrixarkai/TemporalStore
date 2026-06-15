# TemporalStore Client, Proxy, Data Node, And Metaserver Gap Plan

This plan tracks the next production-readiness loops for local Ubuntu 22
validation. Each loop should add one concrete guard, run locally, and publish
Prometheus-compatible evidence when practical.

## Loop 1: Local Port Isolation

Status: implemented.

Gap:
Raft and distributed scale harnesses used high ports that can overlap Linux
ephemeral client ports. This can create false port conflicts and noisy failure
signals during full-suite runs.

Implementation:
- Move default raft production/stress ports below the default Linux ephemeral
  range.
- Detect `/proc/sys/net/ipv4/ip_local_port_range`.
- Fail fast when planned harness ports overlap the ephemeral range unless
  `ALLOW_EPHEMERAL_PORT_RANGE=1` is set.

Services covered:
- Data node: raft, snapshot, scale, mixed read/write, failover harness ports.
- Metaserver: membership and failover harness ports.

## Loop 2: Client Retry And Routing Evidence

Status: partially implemented.

Gap:
Client scale runs report QPS/errors, but do not yet expose routing retries,
visibility retries, timeout buckets, and stale routing refreshes as first-class
gate metrics.

Implementation:
- Export 2-node raft client scale p95/p99 latency, exit code, QPS, and error
  metrics from local benchmark outputs.
- Add production checks for zero unexpected errors, zero benchmark exit codes,
  nonzero QPS, and bounded client p99 latency.

Remaining target:
- Export client retry, timeout, and stale route refresh counts when benchmark
  binaries expose those fields.

Services covered:
- Client.
- Proxy when proxy smoke is enabled.

## Loop 3: Proxy Ingestion Gate

Status: partially implemented.

Gap:
Proxy smoke validates basic reachability, but production readiness needs
batching, retry, quota, and backpressure signals.

Implementation:
- Export proxy artifact presence, live validation, and proxy smoke success as
  Prometheus-compatible metrics from the local Prometheus validation harness.

Implementation target:
- Add a proxy ingestion local gate with direct SDK writes through proxy.
- Emit proxy accepted, retried, rejected, and latency metrics.

Services covered:
- Proxy.
- Client.
- Data node.

## Loop 4: Data Node Primary Kill Under Load

Gap:
Data failover validates primary-down recovery, but the full suite needs a
separate high-load primary kill case with active writes and follower reads.

Implementation target:
- Keep write/read traffic active before and after the kill.
- Gate on bounded write recovery, bounded apply lag, and zero fatal raft events.

Services covered:
- Data node.
- Client.

## Loop 5: Metaserver Failover Evidence Bundle

Status: implemented.

Gap:
Metaserver failover failures need better triage artifacts, especially when a
process exits unexpectedly.

Implementation:
- Capture per-node stderr tail, exit status, core-pattern hint, `/vars` raft
  lines, leader query history, and port state.
- Publish failover timing and failed-node diagnostics into metrics JSON.

Services covered:
- Metaserver.

## Loop 6: Add/Remove Node Rejoin Gate

Gap:
Membership tests cover add/remove convergence, but not removed-node restart
behavior or stale local data rejoin behavior.

Implementation target:
- Restart removed data node and metaserver nodes.
- Verify they do not serve stale leadership or stale primary reads.

Services covered:
- Data node.
- Metaserver.

## Loop 7: Follower Read SLA

Gap:
Follower-read correctness is checked, but bounded-stale SLA needs an explicit
latency and lag gate.

Implementation target:
- Track follower read p95/p99 and max apply lag by phase.
- Fail if bounded-stale lag exceeds configured limits.

Services covered:
- Client.
- Data node.

## Loop 8: Snapshot And Restore Under Writes

Gap:
Snapshot restore validates artifacts, but sustained writes during snapshot and
restart need a stronger gate.

Implementation target:
- Trigger snapshot pressure while writes continue.
- Restart nodes and verify read-after-write for keys written before, during,
  and after snapshot generation.

Services covered:
- Data node.
- Metaserver.

## Loop 9: Prometheus Service Coverage

Gap:
Raft gate metrics are exported, but client/proxy/data/metaserver runtime
coverage should be checked in one service matrix.

Implementation target:
- Add a Prometheus query sanity gate for each service role.
- Require scrape health, role-up, raft lag, request error, and latency metrics.

Services covered:
- Client.
- Proxy.
- Data node.
- Metaserver.

## Loop 10: CI And Push Guard

Gap:
Local guard exists, but heavy distributed gates are not yet separated into fast,
nightly, and release lanes with explicit push requirements.

Implementation target:
- Keep fast CI for syntax and synthetic metrics.
- Add nightly Docker/local raft scale gate.
- Add release lane for 5x full raft production gate.
- Document that code push is allowed only after the relevant guard passes.

Services covered:
- Client.
- Proxy.
- Data node.
- Metaserver.
