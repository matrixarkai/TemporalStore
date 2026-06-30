# RustRaft

RustRaft is the TemporalStore-owned Raft contract, readiness, and safety helper
library. It is intentionally small and runtime-neutral: the crate defines the
public Raft-facing API surface that TemporalStore data-node and metaserver
runtimes must implement and prove.

RustRaft does not start servers, own disk files, or replace the TemporalStore
storage engine by itself. TemporalStore consumes RustRaft for:

- OpenRaft-free public request/response and status types.
- Storage and transport trait boundaries.
- Read-index, append, learner promotion, and compacted-entry safety helpers.
- Metric-name constants for shared dashboards.
- Fail-closed semantic and production readiness reports.

## Production Readiness

Use `rustraft_parity_report` to validate the semantic contract:

```rust
let report = rustraft::rustraft_parity_report(&readiness_snapshot);
assert!(report.ready);
```

Use `rustraft_production_readiness_report` for production deployment gates. It
requires semantic readiness plus live evidence for:

- peer pipeline and backpressure;
- snapshot sender/downloader/install lifecycle;
- WAL segment, range, compaction, and fsync backpressure behavior;
- data-node process rollout and operational semantics;
- metaserver process rollout and operational semantics.

If evidence is missing, the report returns `production_status = "blocked"` with
specific `production_blockers` and `recommended_next_actions`.

## Integration Boundary

TemporalStore keeps application-specific runtime code in
`crates/temporalstore-rust`: storage engine calls, HTTP wiring, topology
management, data-node state machines, and metaserver state machines. RustRaft
owns the reusable consensus contract and readiness language.

This split lets RustRaft move to a standalone repository later without carrying
TemporalStore implementation internals with it.

## Validation

From the TemporalStore workspace:

```bash
cargo test -p rustraft
cargo check -p temporalstore-rust --lib --quiet
cargo test -p temporalstore-rust rustraft --lib -- --nocapture
```

## License

Apache-2.0, inherited from the TemporalStore workspace.
