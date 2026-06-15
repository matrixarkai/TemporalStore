# TemporalStore Production Gap Queue Runbook

This runbook tracks the local-first gates for raft, distributed failover,
replication lag, rebalance, ingestion, metrics, and CI readiness. Local Ubuntu
22 validation is the source of truth before AWS or long-running cloud tests.

## Queue Levels

- `quick`: low-cost checks for metrics, ingestion replay, alert wiring, runbook
  coverage, CI smoke, and remote-auth visibility.
- `pr`: raft and distributed gates that should pass before merging service
  changes.
- `nightly`: longer scale or soak profiles that are useful after the PR gate is
  stable.
- `release`: full 5-repeat raft production gate.
- `manual`: important production gaps that still need dedicated harnesses or
  fault-injection support.

## Commands

Plan only:

```bash
QUEUE_LEVEL=quick RUN_EXECUTION=0 bash tools/run_production_gap_queue_ubuntu22.sh
```

Run the quick executable queue:

```bash
QUEUE_LEVEL=quick RUN_EXECUTION=1 bash tools/run_production_gap_queue_ubuntu22.sh
```

Run PR-level raft and distributed checks:

```bash
QUEUE_LEVEL=pr RUN_EXECUTION=1 bash tools/run_production_gap_queue_ubuntu22.sh
```

Run the release raft gate:

```bash
QUEUE_LEVEL=release RUN_EXECUTION=1 bash tools/run_production_gap_queue_ubuntu22.sh
```

## Pass Criteria

- The queue script exits `0`.
- `summary.md` lists no executed failures.
- `runs.csv` has only `pass` rows for selected executable gaps.
- No service logs contain fatal assertions.
- Prometheus exposes local validation metrics and loads
  `temporalstore-alerts.yml`.
- Raft gates report `temporalstore_raft_gate_production_ready 1` when the raft
  gate is part of the selected level.

## Failure Triage

- Failover failure: inspect metaserver and data-node logs first, then compare
  leader terms and membership rows in the raft gate result directory.
- Replication lag failure: check secondary visibility p50/p95/p99 values and
  confirm follower reads were routed to replica-eligible paths.
- Rebalance failure: confirm placement eligibility, freeze/unfreeze lifecycle,
  and whether partition counts converge after adding a node.
- Snapshot restore failure: inspect snapshot path creation, restore ordering,
  and whether writes overlapped with restore windows.
- Prometheus failure: check the node-exporter textfile directory and the
  `vars-exporter` scrape health metrics.
- Ingestion failure: compare committed watermark, max partition lag, retry
  count, and dead-letter count.

## Current Gap Policy

Covered gaps have an executable local gate. Partial gaps have a nearby gate and
clear missing hardening. Planned gaps are intentionally visible in the queue so
they cannot be forgotten while raft, distributed failover, and rebalance work
move toward production readiness.

Manual planned gates that still need implementation:

- Dedicated rebalance harness with active writes during add/remove.
- Metaserver snapshot restore gate.
- Network timeout and port-block fault gate.
- Process restart with stale local data gate.
- Disk/path failure simulation.
- Multi-tenant noisy-neighbor gate.

## Remote And CI

Push only after local validation passes and only to the intended TemporalStore
remote. For this C++ worktree that remote is expected to be
`https://github.com/bjmeetsfo/TemporalStore.git`.

Remote CI should mirror the local command shape:

```bash
RUN_FULL_GATE=1 bash tools/run_ci_guard_ubuntu22.sh
```

The remote workflow should use dependency caches for the Ubuntu 22 build and
must not vendor generated build outputs.
