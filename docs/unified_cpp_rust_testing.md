# Unified C++ And Rust TemporalStore Testing

The shared compatibility contract lives in:

`compat/unified_temporalstore_cases.json`

Both implementations should execute that same ordered corpus. The file stores
literal `Command` and `CommandResponse` JSON payloads so it can cover common
KV, hash, packed timestamped feature pages, sequence rows, IPS, risk, context,
and restart reads without duplicating expected behavior in separate test code.

## Rust

Run:

```bash
tools/run_temporalstore_unified_tests.sh
```

This validates the JSON schema and runs:

```bash
cargo test -p temporalstore-rust --test unified_temporalstore_corpus -- --test-threads=1
```

The Rust integration test executes every corpus step twice:

- directly through `TemporalEngine`
- through `TemporalStoreClient` and `TemporalStoreTable::execute` over the local
  HTTP API

Both paths also exercise restart reads against the same page/index directories.

## C++

The C++ codebase should provide a corpus runner that accepts the JSON path as
its final argument, or use `{corpus}` as a placeholder in the command. Then run:

```bash
TS_CPP_UNIFIED_TEST_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  tools/run_temporalstore_unified_tests.sh
```

To require C++ execution in CI:

```bash
TS_RUN_CPP_UNIFIED_TESTS=1 \
TS_CPP_UNIFIED_TEST_CMD='/path/to/cpp_temporalstore_corpus_runner {corpus}' \
  tools/run_temporalstore_unified_tests.sh
```

To run both repositories from this Rust checkout:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
TS_CPP_UNIFIED_TEST_CMD='{cpp_repo}/tools/run_temporalstore_unified_tests.sh --corpus {corpus}' \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

If the C++ repo contains `tools/run_temporalstore_unified_tests.sh`, the command
can be shortened:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
```

If `TS_RUN_CPP_UNIFIED_TESTS=1` is set and the C++ command is missing, the
runner fails closed. This keeps Rust-only local development fast while making
cross-codebase validation strict in parity CI.

## Existing C++-Like Gate

`tools/run_temporalstore_cpp_like_tests.sh` now invokes the unified corpus
before the Raft, stream, metaserver, scale, and storage-mode harnesses. That
means every C++-like gate run checks the same shared behavioral contract first.
