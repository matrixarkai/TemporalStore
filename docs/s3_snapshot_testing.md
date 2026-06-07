# S3 Snapshot Testing

This follows the C++ TemporalStore testing style: local smoke first, then an opt-in remote object
store smoke. The C++ tree uses onebox configs, `tools/fake_s3_server.py`, and
`tools/run_replication_guardrail_ubuntu22.sh` to separate local validation from remote durability
checks. The Rust snapshot crate mirrors that split.

## Local

Run all local snapshot tests:

```bash
./tools/run_snapshot_smoke_ubuntu22.sh
```

This verifies:

- local snapshot creation
- manifest-last visibility
- upload/download through the object-store boundary
- checksum rejection for corrupt snapshots
- stale snapshot install guard
- Prometheus metric exposition names

## AWS S3

The AWS smoke test is ignored by default and runs only when explicit credentials and a bucket are
provided:

```bash
AWS_MODE=1 \
TS_SNAPSHOT_AWS_BUCKET=<bucket> \
TS_SNAPSHOT_AWS_PREFIX=temporalstore-rust-snapshot-smoke/manual-001 \
./tools/run_snapshot_smoke_ubuntu22.sh
```

Requirements:

- `aws` CLI available on PATH
- AWS credentials already configured by `AWS_PROFILE`, environment variables, or instance role
- bucket permissions for `PutObject`, `GetObject`, `ListBucket`, and `DeleteObject`

The test writes under:

```text
s3://<bucket>/<prefix>/aws-smoke-cluster/shards/101/snapshots/...
```

It uploads a snapshot, verifies checksums, downloads it, compares restored page-segment bytes, and
deletes the snapshot prefix.

## What This Does Not Cover Yet

This is the snapshot storage smoke, not a full Raft cluster test. The next layer should reuse the
C++ guardrail pattern:

```text
start 3 shard nodes -> write records -> force snapshot -> add new replica ->
restore from S3 -> catch up via Raft log -> kill leader -> verify S3 is not on election path
```

That belongs in the future `temporalstore-server` crate once OpenRaft shard replicas exist.
