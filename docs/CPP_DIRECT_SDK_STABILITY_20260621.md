# C++ Direct SDK Stability For MatrixArk Benchmarks

Date: 2026-06-21

## Why This Matters

Full MatrixArk benchmark parity needs the native C++ TemporalStore service and direct SDK to survive long repeated `hset`, `hget`, feature ingest, and feature query workloads. The benchmark path writes context nodes, events, indexes, summaries, embeddings, and audits through hash-like and time-series APIs, so repeated hash write/read stability is a release gate.

## Fix

`TemporalStoreClient::HSet`, `HGet`, and `HDel` now use the same raw module execution path as the feature APIs. This matters because the raw path sets the per-request controller timeout from `TemporalStoreClientOptions::request_timeout_ms`.

Before:

- Hash APIs used the older table convenience methods.
- Long stress could still show a fixed 5000 ms BRPC timeout even when the Python direct SDK requested a larger timeout.

After:

- Hash APIs route through `Impl::ExecuteRaw(Module::HASH, ...)`.
- Direct SDK benchmark runs can tune both `request_timeout_ms` and `io_timeout_ms`.
- Feature APIs and hash APIs share one timeout-aware direct SDK execution path.

## New Stress Harness

Added:

- `sdk/python/examples/direct_sdk_stress.py`
- `RUN_PYTHON_DIRECT_STRESS=1` support in `tools/run_sdk_smoke_ubuntu22.sh`

The harness starts the normal TemporalStore Ubuntu mini-cluster, loads the direct C SDK through Python `ctypes`, and performs:

- repeated `hset`
- repeated `hget` validation
- repeated `hset` overwrite validation
- feature point writes
- feature point time-range queries

It also keeps a Python in-memory oracle for the same operations. A stress run
only passes if the real C++ direct SDK/server readback matches the oracle:

- hash key/field values must match after insert and overwrite
- sampled hash fields must match the oracle after the write phase
- feature queries must match the oracle in count, timestamp order, and value

The JSON report includes `parity_checked`, `hash_oracle_digest`, and
`feature_oracle_digest` so benchmark artifacts can prove that a native C++ run
matched the Python memory semantics instead of only proving that the service did
not crash.

It writes `python_direct_stress.json` and `python_direct_stress.out` under the configured `RESULT_DIR`.

## Validation

### Long Direct SDK Workload

Command shape:

```bash
RESULT_DIR=/mnt/c/root/matrixark_benchmarks/artifacts/cpp_scale_20260621/direct_sdk_stress_long_after_fix_seq \
BUILD_TYPE=Release \
RUN_PYTHON_DIRECT_STRESS=1 \
PYTHON_DIRECT_STRESS_HASH_OPS=5000 \
PYTHON_DIRECT_STRESS_FEATURE_KEYS=64 \
PYTHON_DIRECT_STRESS_FEATURE_POINTS_PER_KEY=64 \
PYTHON_DIRECT_STRESS_VALUE_BYTES=1024 \
PYTHON_DIRECT_STRESS_REQUEST_TIMEOUT_MS=20000 \
PYTHON_DIRECT_STRESS_IO_TIMEOUT_MS=20000 \
MS_PORT=18540 \
MS_RAFT_PORT=18550 \
MS_SNAPSHOT_PORT=18560 \
SERVER_PORT=18541 \
CLUSTER_NAME=directstresslongfixseq \
./tools/run_sdk_smoke_ubuntu22.sh
```

Result:

```json
{
  "status": "passed",
  "parity_checked": true,
  "hash_ops": 5000,
  "hash_reads": 2667,
  "hash_fields": 5000,
  "hash_keys": 97,
  "hash_overwrite_checks": 100,
  "feature_points_written": 4096,
  "feature_points_read": 4096,
  "value_bytes": 1024,
  "elapsed_ms": 27811.817
}
```

Artifact:

`C:\root\matrixark_benchmarks\artifacts\cpp_scale_20260621\direct_sdk_oracle_parity_long\python_direct_stress.json`

### C++ Context Scale Gate

Command:

```bash
python3 tools/run_context_pipeline_scale_e2e.py \
  --events-per-lane 20 \
  --model-provider deterministic \
  --skip-rust \
  --write-results /mnt/c/root/matrixark_benchmarks/artifacts/cpp_scale_20260621/cpp_unified_context_scale_e2e_after_sdk_fix.json
```

Result:

- `status`: `passed`
- context steps: `64`
- api events: `1`
- batch events: `20`
- stream events: `20`
- resource chunks: `1`
- entity records: `2`
- summary records: `7`
- summary embedding refs: `6`
- compression records: `2`
- compression source event refs: `43`

Artifact:

`C:\root\matrixark_benchmarks\artifacts\cpp_scale_20260621\cpp_unified_context_scale_e2e_after_sdk_fix.json`

## Notes

- A forced small-blob stress run with `--stream_max_blob_size=1048576` no longer showed the old `IsCoContext()` assertion signature, but it can still time out under intentionally aggressive blob-switch settings. Treat that as a separate stream-throughput tuning case, not a MatrixArk benchmark parity blocker.
- Running multiple full mini-clusters in parallel on this WSL/Windows-mounted tree can make metaserver startup unstable. Run direct SDK benchmark gates sequentially.
- The release shared library rebuild completed with warnings only and produced `output-ubuntu22/release/sdk/lib/libbcache2.so`.

## Stale Binary And Old Process Guard

The benchmark runner now guards against the exact failure mode where an old service process, stale server binary, or stale SDK shared library keeps showing a previously fixed fatal path.

`tools/run_sdk_smoke_ubuntu22.sh` defaults to:

```bash
REQUIRE_FRESH_BINARIES=1
```

That checks the release metaserver, server, and SDK shared library before a benchmark run. If the key source file is newer than the binary, the script exits before starting the mini-cluster.

For benchmark gates, also run with:

```bash
REQUIRE_NO_TEMPORALSTORE_PROCESSES=1
```

That makes the script fail if any existing `bcache2-metaserver` or `bcache2-server` process is already running. This prevents a benchmark from accidentally connecting to an old process that was started from a previous build.

Cleanup check:

```bash
pkill -f 'bcache2-metaserver.*<cluster_name>' || true
pkill -f 'bcache2-server.*<cluster_name>' || true
pgrep -af 'bcache2-(metaserver|server)' || true
```

If `pgrep` still prints a server or metaserver, stop that process before running C++ benchmark parity gates.

## Benchmark Readiness Policy

For LOCOMO and LongMemEval parity runs through native C++ TemporalStore:

1. Run C++ benchmark transport parity first:

   ```bash
   BUILD_TYPE=Release \
   RESULT_DIR=/mnt/c/root/matrixark_benchmarks/artifacts/cpp_scale_20260621/transport_parity \
   ./tools/run_cpp_benchmark_transport_parity_ubuntu22.sh
   ```

   This runs both required serving paths:

   - direct SDK against a real C++ mini-cluster, with Python in-memory oracle parity
   - live C++ proxy against a real C++ SDK mini-cluster, with proxy smoke plus verified readback pressure

   The gate writes `transport_parity_report.json` and
   `transport_parity_report.md`. Benchmark runs should require direct
   `parity_checked: true`, proxy smoke pass, and zero proxy write/read/RPC/status
   failures. The proxy pressure client retries an individual write before
   counting it failed, so short backend-readiness races are absorbed without
   weakening the final clean-status gate.

   `TRANSPORT_REQUIRE_FRESH_BINARIES=1` is the default and should stay enabled
   for benchmark claims. Set it to `0` only for a local diagnostic run when the
   existing C++ proxy binary must be validated before a slow rebuild completes;
   the report records this as `fresh_binary_gate`.

   Latest local diagnostic artifact using the existing proxy binary:

   `C:\root\matrixark_benchmarks\artifacts\cpp_scale_20260621\transport_parity_proxy_retry_fix\transport_parity_report.json`

   Result summary:

   - direct SDK: `parity_checked=true`, `hash_ops=800`, `feature_points_read=128`
   - proxy: smoke passed, `read_verified=80`, `write_failed=0`, `read_failed=0`, `rpc_failed=0`, `status_failed=0`, `write_retry_attempts=1`

2. Run the C++ context scale gate.
3. Run MatrixArk benchmark with the C++ direct backend and, where applicable, the C++ proxy backend.
4. Save every artifact next to the benchmark report.
5. If a benchmark fails, first classify the failure as SDK transport, proxy transport, C++ service, context mapping, retrieval quality, reader quality, or judge quality.
