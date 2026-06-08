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
TS_SCALE_STRING_OPS=40 \
TS_SCALE_HASH_OPS=10 \
TS_SCALE_SEQUENCE_KEYS=2 \
TS_SCALE_SEQUENCE_LEN=100 \
TS_SCALE_EVENTS=2 \
TS_SCALE_FAILOVER_EVERY=10 \
TS_SCALE_READ_SAMPLE_EVERY=10 \
tools/run_temporalstore_scale_harness.sh
```

The wrapper uses release mode by default. Set `TS_SCALE_PROFILE=debug` only when debugging the
harness itself.

Heavier local profile:

```bash
TS_SCALE_NODES=5 \
TS_SCALE_STRING_OPS=1000 \
TS_SCALE_HASH_OPS=250 \
TS_SCALE_SEQUENCE_KEYS=8 \
TS_SCALE_SEQUENCE_LEN=1000 \
TS_SCALE_EVENTS=8 \
TS_SCALE_FAILOVER_EVERY=100 \
tools/run_temporalstore_scale_harness.sh
```

Direct binary usage:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes 3 \
  --string-ops 1000 \
  --hash-ops 250 \
  --sequence-keys 4 \
  --sequence-len 500 \
  --scale-events 2 \
  --failover-every 250 \
  --read-sample-every 100
```

Long-sequence Raft usage:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes 3 \
  --string-ops 120 \
  --hash-ops 40 \
  --sequence-keys 1 \
  --sequence-len 5000 \
  --scale-events 2 \
  --failover-every 40 \
  --read-sample-every 20
```

The harness prints a JSON summary and exits non-zero if final replication health fails.

On the Windows-mounted local workspace, the in-process Raft/engine model is intentionally
correctness-heavy and writes local page/index data. Treat the output as a local regression signal
for scale/failover behavior, not as production throughput.

The default Raft config still exposes the ByteRaft-style `32 KiB` max memory replicate-log entry
limit. A 5k-row sequence command is larger than that in the Rust JSON command shape, so the Raft
proposal path now chunks `SequenceAdd` commands into ordered smaller entries. `--max-log-entry-bytes`
is still available for stress testing and for future command shapes where one logical row is larger
than the default entry limit. Production still needs a transactional multi-entry policy if callers
require all-or-nothing semantics across a chunked logical append.

This is not a substitute for multi-process chaos testing. It is intended as a fast regression gate
for scale/failover logic while the production Raft runtime is still being hardened.
