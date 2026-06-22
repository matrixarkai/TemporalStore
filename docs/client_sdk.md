# TemporalStore Client SDK

The customer SDK is implemented in:

- C++ API: `src/client/temporalstore_client.h`
- C++ implementation: `src/client/temporalstore_client.cc`
- C API: `src/client/temporalstore_c_client.h`
- C wrapper: `src/client/temporalstore_c_client.cc`
- C++ example: `src/client/example/customer_client_example.cc`
- C example: `src/client/example/customer_c_client_example.c`
- C++ package include: `sdk/cpp/include/temporalstore/client.h`
- Python SDK: `sdk/python/temporalstore`
- Go SDK: `sdk/go/temporalstore`
- Java SDK: `sdk/java/temporalstore`
- Rust SDK: `sdk/rust/temporalstore`
- Proxy API contract: `sdk/proxy/openapi.yaml`

There are two SDK modes:

- Direct SDKs load or link the native client and route directly to data servers.
- Proxy SDKs are pure-language HTTP clients. They call a TemporalStore proxy that owns routing, topology refresh, retries, auth, quotas, and observability.

Use direct SDKs for the lowest latency backend path. Use proxy SDKs for customer onboarding, Python/Java services that should avoid native library loading, serverless jobs, notebooks, and any deployment where centralized policy is more important than one extra network hop.

## Production Defaults

The SDK now uses primary-pinned reads by default. That is the safer customer default because simple write-then-read flows should not randomly read a lagging secondary.

Important options:

| Option | Default | Purpose |
|---|---:|---|
| `request_timeout_ms` | 5000 | Per-RPC deadline |
| `max_read_retries` | 1 | Retry transient read failures |
| `max_write_retries` | 0 | Avoid duplicate non-idempotent writes by default |
| `retry_backoff_ms` | 2 | Linear backoff base |
| `max_feature_points_per_request` | 1000 | Split large sequence writes into bounded batches |
| `max_feature_query_count` | 5000 | Guard against accidental huge raw sequence scans |
| `max_key_bytes` | 4096 | Client-side key guardrail |
| `max_value_bytes` | 16 MB | Client-side value guardrail |
| `pin_primary` | true | Read-your-write consistency default |

## Sequence Feature API

Use typed sequence rows for the default long-sequence feature schema:

```cpp
std::vector<bcache2::client::SequenceFeatureRow> rows = {
    {1700000000000ULL, 900, 1, 31, 7000},
    {1700000001000ULL, 901, 3, 120, 7001},
};
client->AddSequenceFeatureRows("user:42:sequence", rows);

bcache2::client::TemporalFeatureQuery query;
query.start_ts = 1700000000000ULL;
query.end_ts = 1700000002000ULL;
query.count = 10;
query.filters.push_back({"action_type", bcache2::client::TemporalFeatureFilterOp::kEqual, 3});

std::vector<bcache2::client::SequenceFeatureRow> out;
client->QuerySequenceFeatureRows("user:42:sequence", query, &out);
```

The SDK builds the server filter strings for you, validates the query window, and decodes the protobuf row values into typed output.

## Raw Feature API

The old raw feature API is still available:

```cpp
std::vector<bcache2::client::TemporalFeaturePoint> points;
client->QueryFeaturePoints("user:42:sequence", 1700000000000ULL, 1700000002000ULL, 100, &points);
```

For filtered raw reads, use `TemporalFeatureQuery`:

```cpp
bcache2::client::TemporalFeatureQuery query;
query.start_ts = 1700000000000ULL;
query.end_ts = 1700000002000ULL;
query.count = 100;
query.filters.push_back({"duration", bcache2::client::TemporalFeatureFilterOp::kGreaterThan, 120});

std::vector<bcache2::client::TemporalFeaturePoint> points;
client->QueryFeaturePoints("user:42:sequence", query, &points);
```

## C API

The C wrapper exposes equivalent typed sequence calls:

```c
temporalstore_sequence_feature_row_t rows[2] = {
    {1700000000000ULL, 900, 1, 31, 7000},
    {1700000001000ULL, 901, 3, 120, 7001},
};
temporalstore_add_sequence_feature_rows(client, "user:42:sequence", rows, 2, &error);

temporalstore_feature_filter_t filters[1] = {
    {"action_type", TEMPORALSTORE_FEATURE_FILTER_EQUAL, 3},
};
temporalstore_sequence_feature_row_array_t out = {0, NULL};
temporalstore_query_sequence_feature_rows(client, "user:42:sequence",
    1700000000000ULL, 1700000002000ULL, 10, filters, 1, &out, &error);
temporalstore_sequence_feature_row_array_free(&out);
```

## Verification

Build and smoke test:

```bash
cmake --build build-ubuntu22 --target customer_client_example customer_c_client_example -j4
bash tools/run_sdk_smoke_ubuntu22.sh
```

Latest local run passed both examples:

- C++: `PASS customer production client example`
- C: `PASS customer C client example`

The shared library target also builds and exports the C ABI symbols:

```bash
cmake --build build-ubuntu22 --target bcache2-shared -j4
nm -D output/sdk/lib/libbcache2d.so | grep temporalstore_
```

See `sdk/README.md` for Go, Java, Python, C++, and Rust wrapper usage.

Full direct SDK smoke test:

```bash
RUN_PYTHON_SDK=1 RUN_GO_SDK=1 RUN_JAVA_SDK=1 RUN_RUST_SDK=1 \
  RUN_UNIFIED_TESTS=1 \
  TEMPORALSTORE_PYTHON_LIB=/path/to/libbcache2.so \
  tools/run_sdk_smoke_ubuntu22.sh
```

This validates the selected language SDKs against a real local metaserver/server
cluster. The legacy C++ and C customer examples are opt-in with
`RUN_CUSTOMER_EXAMPLES=1`; shared behavior should live in the unified corpus.

Enable the shared C++/Rust unified corpus tests through the same runner. The
default corpus lives at `sdk/unified/temporalstore_unified_corpus.json`; the C++
hook validates the corpus contract and the Rust proxy integration test executes
the same SDK cases. The corpus also lists existing C++ multi-layer cache,
storage, and RAFT gates as `existing_test` steps so they share one test
inventory.

```bash
tools/run_rust_unified_tests.sh
```

To include this in the full direct SDK smoke run:

```bash
RUN_RUST_SDK=1 RUN_UNIFIED_TESTS=1 tools/run_sdk_smoke_ubuntu22.sh
```

Use `RUST_UNIFIED_VALIDATE_ONLY=1` for a fast schema/contract validation pass.
Use `TS_CPP_UNIFIED_NATIVE_CMD='...'` when the unified run should also execute a
full C++ corpus command; the command string can reference `{corpus}` and
`{cpp_repo}`. Use `RUST_UNIFIED_CORPUS=/path/to/corpus.json` to test an
alternate corpus.

To execute the existing C++ cache/storage/RAFT runners listed in the corpus:

```bash
TS_CPP_UNIFIED_RUN_EXISTING=1 tools/run_rust_unified_tests.sh
```

This path is intentionally opt-in because it includes heavier smoke and stress
tests.

## Proxy SDK API

The proxy SDKs use the same logical data model as the direct SDKs:

- STRING: put/get with optional TTL.
- COMMON: delete, expire, TTL.
- HASH: hset/hget/hdel.
- SET: sadd/smembers.
- FEATURE: raw timestamped feature points with filters.
- SEQUENCE FEATURE: typed long-sequence rows with filters.
- IPS: add/query recent instances.
- RISK: increment and count over a time window.

Current proxy SDK entry points:

- Python: `sdk/python/temporalstore/proxy_client.py`
- Go: `sdk/go/temporalstore/proxy_client.go`
- Java: `sdk/java/temporalstore/src/main/java/com/temporalstore/TemporalStoreProxyClient.java`
- Rust: `sdk/rust/temporalstore`, built with `--no-default-features --features proxy`

The existing `src/proxy` binary is a brpc/Thrift proxy. The customer-facing proxy SDK contract is HTTP/JSON and is documented in `docs/direct_vs_proxy_sdk.md` plus `sdk/proxy/openapi.yaml`.

Proxy SDK smoke test:

```bash
tools/run_proxy_sdk_smoke_ubuntu22.sh
```

This starts a small mock HTTP gateway and validates the Python, Go, Java, and Rust
proxy SDK examples.
