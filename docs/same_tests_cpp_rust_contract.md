# Same Tests For C++ And Rust TemporalStore

## What "Same Tests" Means

The same-test contract is not "Rust has similar tests" or "C++ has a separate smoke test with the
same idea." It means both codebases execute the same ordered test corpus file and compare against
the same expected logical responses.

The current shared corpus is:

```text
compat/unified_temporalstore_cases.json
```

That file is the source of truth for cross-codebase behavior. It contains serialized command JSON,
expected response JSON, restart markers, shard ids, case names, and step names. The Rust and C++
runners must not hard-code different expected values.

## Current Shared Test Coverage

Current corpus:

```text
schema_version: 1
name: temporalstore-unified-cpp-rust-corpus
cases: 8
```

The shared cases are:

- `common_string_hash_core`: string set/get plus hash multi-set/multi-get.
- `feature_packed_timestamped_pages`: packed timestamped Feature points and restart query.
- `sequence_cpp_feature_rows`: Sequence rows encoded in the C++ feature-row shape.
- `ips_options_range`: IPS add/query range with action/table/request metadata.
- `risk_counter_window`: Risk increment/count over a time window.
- `context_node_roundtrip`: Context node upsert/read.
- `common_restart_persistence`: string/hash restart-read persistence.
- `mixed_model_restart_persistence`: Feature plus Context restart-read persistence in one case.

## Rust Runner

Rust executes the corpus in:

```text
crates/temporalstore-rust/tests/unified_temporalstore_corpus.rs
```

Run it with:

```bash
tools/run_temporalstore_unified_tests.sh
```

That script runs:

```bash
cargo test -p temporalstore-rust --test unified_temporalstore_corpus -- --test-threads=1
```

Rust currently runs the same corpus through two paths:

- direct `TemporalEngine`
- `TemporalStoreClient` plus `TemporalStoreTable::execute` over the local HTTP API

Both Rust paths reuse the same page/index directories for restart-read steps.

## Required C++ Runner

C++ must provide a runner that accepts the corpus path and executes every case and step in order.
Install the shared wrapper into the C++ checkout from this Rust checkout:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
  python3 tools/run_temporalstore_unified_tests.py --install-cpp-runner
```

That creates:

```text
/path/to/cpp/TemporalStore/tools/run_temporalstore_unified_tests.sh
```

The installed C++ wrapper has the same entry point as the Rust wrapper, but delegates to the native
C++ executor through `TS_CPP_UNIFIED_NATIVE_CMD`.

The native C++ executor contract is:

```bash
TS_CPP_UNIFIED_NATIVE_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  tools/run_temporalstore_unified_tests.sh --corpus /absolute/path/to/compat/unified_temporalstore_cases.json
```

Or, when launched from the Rust repo:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_TEST_CMD='{cpp_repo}/tools/run_temporalstore_unified_tests.sh --corpus {corpus}' \
  tools/run_temporalstore_unified_tests.sh
```

The native C++ executor must:

- parse `schema_version`, `cases`, `steps`, `command`, `expect`, and `restart_before`
- execute each command against C++ TemporalStore
- compare the actual logical response to `expect`
- restart or reload the local C++ engine when `restart_before=true`
- fail closed on unknown command fields, missing expected fields, unsupported data models, or
  response mismatches

## Running Both Codebases

From the Rust checkout:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_TEST_CMD='{cpp_repo}/tools/run_temporalstore_unified_tests.sh --corpus {corpus}' \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

If the C++ repo already contains `tools/run_temporalstore_unified_tests.sh`, this shorter form is
valid:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

If the C++ wrapper is installed, set only the native executor command:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_NATIVE_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

For strict same-test enforcement, require native C++ corpus execution:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_NATIVE_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp-native
```

The shell wrapper also supports the strict mode:

```bash
TS_RUN_CPP_UNIFIED_TESTS=1 \
TS_REQUIRE_CPP_NATIVE=1 \
TS_CPP_UNIFIED_NATIVE_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  tools/run_temporalstore_unified_tests.sh
```

`--require-cpp` means a C++ hook must run. `--require-cpp-native` means a C++ native executor must
be configured through `TS_CPP_UNIFIED_TEST_CMD` or `TS_CPP_UNIFIED_NATIVE_CMD`, so the C++ side
actually applies every corpus command and compares every expected response.

For CI, require C++ execution:

```bash
TS_RUN_CPP_UNIFIED_TESTS=1 \
TS_CPP_UNIFIED_TEST_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  tools/run_temporalstore_unified_tests.sh
```

## What Is Not Yet The Same

These are not yet true same tests:

- Rust-only unit tests under `crates/temporalstore-rust/src/**`.
- C++ local smoke tests that do not consume `compat/unified_temporalstore_cases.json`.
- Rust storage migration tests using `compat/storage_migration_corpus.json`; those validate
  migration/replay behavior, not the shared command-response contract.
- C++ p99/performance gates; those compare thresholds and workload classes, but do not yet execute
  the exact same operation trace.
- Raft, proxy, metaserver, data-node lifecycle, ingestion, Redis, and context provider tests outside
  the unified corpus.

## Gap Fill Plan For Real Same-Test Parity

1. Add the C++ corpus runner if it does not exist in the C++ repo.
2. Make C++ CI call the runner with `compat/unified_temporalstore_cases.json`.
3. Expand the corpus into separate cases for Redis command parity, context extract/inject, storage
   dump/load/restart, proxy/client routing, metaserver topology, data-node lifecycle, ingestion,
   and Raft failover.
4. Add negative cases: not found, stale route, readonly table, bad lifecycle state, corrupt storage
   artifact, invalid timestamped page payload, and retry-safe write failure.
5. Require `--both --require-cpp` in the parity gate before claiming C++ parity.

Until C++ consumes the shared corpus in CI, the honest status is: Rust has a same-test contract and
runner, but cross-codebase same-test enforcement is only complete when the C++ runner executes the
same corpus and fails on the same expected-response mismatches.

## Local Test Run: 2026-06-16

Rust checkout:

```text
C:\Users\Deeproute\Documents\Codex\2026-06-10\pull-rust-temporalstore-code-from-matrixarkai\work\TemporalStore
```

C++ checkout:

```text
C:\Users\Deeproute\Documents\Codex\2026-06-07\what-s-the-topology-for-all\temporalstore-service-fix
```

Shared corpus command:

```bash
python3 tools/run_temporalstore_unified_tests.py \
  --both \
  --require-cpp \
  --cpp-repo /mnt/c/Users/Deeproute/Documents/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix
```

Result:

- Rust unified corpus runner passed.
- Rust direct engine path passed.
- Rust `TemporalStoreClient` plus local HTTP path passed.
- C++ unified hook passed against the same `compat/unified_temporalstore_cases.json`.
- C++ hook confirmed the required local C++ parity surfaces are present.

C++ fast local CI guard command:

```bash
env \
  ITERATIONS=1 \
  RUN_FULL_GATE=0 \
  DEPENDENCY_CACHE_RUN_BUILD_SMOKE=0 \
  RESULT_DIR=/tmp/temporalstore-cpp-ci-guard-unified-1781642126 \
  tools/run_ci_guard_ubuntu22.sh
```

Result:

- `syntax`: pass
- `dependency_cache`: pass
- `prometheus_unit`: pass
- `raft_summary`: pass
- `monitoring_health`: pass
- total passed cases: 5
- total failed cases: 0

Important caveat: this local run did not execute a full native C++ command-by-command corpus
executor because `TS_CPP_UNIFIED_NATIVE_CMD` was not configured. The C++ side validated the shared
corpus and required parity surfaces through its hook, then the C++ fast CI guard passed. Full
same-test enforcement still requires wiring `TS_CPP_UNIFIED_NATIVE_CMD` to a native C++ executor
that applies every corpus command and compares every expected response.

Strict native enforcement check:

```bash
python3 tools/run_temporalstore_unified_tests.py \
  --cpp \
  --require-cpp-native \
  --cpp-repo /mnt/c/Users/Deeproute/Documents/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix
```

Expected current result: fail closed until `TS_CPP_UNIFIED_NATIVE_CMD` or `TS_CPP_UNIFIED_TEST_CMD`
is configured. This is intentional; it prevents a C++ hook-only run from being mistaken for true
same-test C++ execution.
