# Unified C++ And Rust TemporalStore Testing

The shared compatibility contract lives in:

`compat/unified_temporalstore_cases.json`

Current case inventory: `docs/unified_test_case_inventory.md`.

Both implementations should execute that same ordered corpus. The file stores
literal `Command` and `CommandResponse` JSON payloads so it can cover common
KV, hash, packed timestamped feature pages, sequence rows, IPS, risk, context,
advanced Feature policy/filter/aggregate flows, Sequence batch/filter groups,
and restart reads without duplicating expected behavior in separate test code.
The same corpus also carries static `existing_test` parity gates for C++
storage/Raft plus client, proxy, metaserver, and data-node control-plane
surfaces that Rust is expected to track.
`tools/validate_raft_storage_parity_evidence.py` ties those storage/Raft
surfaces back to Rust implementation, tests, and harnesses across eight
explicit parity areas so the C++ surface gates cannot drift away from Rust
evidence.
`tools/validate_control_plane_parity_evidence.py` applies the same rule to
client, proxy, metaserver, and data-node lifecycle/control-plane parity
surfaces.
`tools/validate_api_model_parity_evidence.py` keeps executable API/model corpus
coverage tied to Rust Redis, Feature, Sequence, IPS, Risk, Context, and SDK
evidence.
`tools/validate_ingestion_ops_parity_evidence.py` keeps ingestion durability,
production-readiness, Prometheus, and scale/fault validation evidence tied to
the Rust code paths and local harnesses.

Policy: all externally observable product behavior should be represented in
Rust-owned shared corpus files first, then consumed by both Rust and C++.
Language-specific tests should remain only for implementation mechanics that
are not a cross-codebase TemporalStore contract, such as Rust helper internals,
C++ ownership/build glue, or temporary fixture plumbing.

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

## Unified Local Validation Gate

For one local pass that covers unit tests, API/corpus tests, storage integration,
scale/shared-store validation, and readiness reporting, run:

```bash
bash tools/run_temporalstore_unified_validation.sh
```

That gate unifies the existing local checks instead of replacing them:

- unit/API compatibility: `temporalstore_compat`
- API contract: `validate_sdk_contract.py`
- shared Rust/C++ corpus: `tools/run_temporalstore_unified_tests.sh`
- Raft/storage evidence: `tools/validate_raft_storage_parity_evidence.py`
- storage/Raft production order: `tools/validate_storage_raft_production_plan.py`
- control-plane evidence: `tools/validate_control_plane_parity_evidence.py`
- API/model evidence: `tools/validate_api_model_parity_evidence.py`
- ingestion/ops evidence: `tools/validate_ingestion_ops_parity_evidence.py`
- storage integration: `storage_migration_corpus` and `storage_crash_harness`
- scale/shared-store: compact `scale_harness` run with tunable `TS_UNIFIED_*`
  knobs
- storage modes: `storage_modes_harness`
- production readiness: `readiness_gate --service-reports`

For the heavier one-by-one storage/Raft local production-readiness pass, run:

```bash
tools/run_storage_raft_production_readiness.sh
```

That gate is documented in `docs/storage_raft_production_readiness_plan.md`.

To include the configured C++ checkout hook in the same pass:

```bash
TS_CPP_REPO=/path/to/cpp/TemporalStore \
bash tools/run_temporalstore_unified_validation.sh --with-cpp
```

The readiness gate is allowed to report known production blockers; unexpected
readiness process failures still fail the unified validation pass.
