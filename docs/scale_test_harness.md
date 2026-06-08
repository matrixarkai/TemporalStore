# Scale Test Harness

`scale_harness` is an in-process TemporalStore Rust scale/fault harness for fast local and CI
coverage. It exercises the same Rust Raft/data APIs as the unit and compatibility tests, without
requiring a deployed cluster.

It covers:

- Raft write replication across multiple voters
- sampled follower reads with read-index lag checks
- repeated leader transfer/failover
- safe scale up and scale down
- string and hash write volume
- long sequence feature writes and filtered reads
- final replication-health verification with max lag `0`

Quick smoke:

```bash
TS_SCALE_STRING_OPS=200 \
TS_SCALE_HASH_OPS=50 \
TS_SCALE_SEQUENCE_KEYS=2 \
TS_SCALE_SEQUENCE_LEN=200 \
TS_SCALE_EVENTS=2 \
tools/run_temporalstore_scale_harness.sh
```

Heavier local profile:

```bash
TS_SCALE_NODES=5 \
TS_SCALE_STRING_OPS=50000 \
TS_SCALE_HASH_OPS=10000 \
TS_SCALE_SEQUENCE_KEYS=32 \
TS_SCALE_SEQUENCE_LEN=5000 \
TS_SCALE_EVENTS=8 \
TS_SCALE_FAILOVER_EVERY=5000 \
tools/run_temporalstore_scale_harness.sh
```

Direct binary usage:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-single-node --bin scale_harness -- \
  --nodes 3 \
  --string-ops 1000 \
  --hash-ops 250 \
  --sequence-keys 4 \
  --sequence-len 500 \
  --scale-events 2 \
  --failover-every 250 \
  --read-sample-every 100
```

The harness prints a JSON summary and exits non-zero if final replication health fails.

This is not a substitute for multi-process chaos testing. It is intended as a fast regression gate
for scale/failover logic while the production Raft runtime is still being hardened.
