# C++-Like TemporalStore Testing

This maps the local C++ TemporalStore test files to the Rust tests and harnesses
that exercise the same behavior.

## Source C++ Suite

Reference tree:

`<cpp-temporalstore-checkout>`

Representative C++ files inspected:

- `test/smoketest/basic_smoketest.cc`
- `test/smoketest/consistency_bench.cc`
- `src/extension/feature/test.cc`
- `src/model/test/feature_model_test.cc`
- `sdk/cpp/examples/sequence_features.cc`
- `src/client/example/feature_sequence_benchmark.cc`
- `docs/feature_sequence_benchmark.md`
- `src/partition/storage/test/data_raft_replication_test.cc`
- `src/stream/test/stream_test.cc`
- `src/stream/test/log_based_stream_test.cc`
- `src/partition/index/test/index_test.cc`
- `src/metaserver_v2/test/placement_test.cc`
- `test/onebox/test_feature/test_proxy.py`
- `test/onebox/test_feature/test_metaserver.py`
- `test/onebox/test_consistency/test_consistency.py`
- `test/onebox/test_chaos.py`

## Rust Equivalents

- Shared C++/Rust behavioral corpus:
  `tools/run_temporalstore_unified_tests.sh`

- Shared C++/Rust integration target:
  `cargo test -p temporalstore-rust --test unified_temporalstore_corpus -- --test-threads=1`

  This replaces the old Rust-local `temporalstore_compat` integration target. Basic smoke,
  consistency, stream, Feature, Sequence, IPS, Risk, Redis/admin, Context, control-plane,
  ingestion, storage, and Raft behavior should be added to the shared corpus first, then
  executed through this target.

- C++ DataRaft serialization, corrupt payload rejection, fail-closed backend:
  `cargo test -p temporalstore-rust cpp_data_raft`
  `cargo test -p temporalstore-rust data_raft_log_codec_round_trips_cxx_style_header`
  `cargo test -p temporalstore-rust data_raft_command_codec_round_trips_batch_request`

- C++ page/index/oplog stream read and scan:
  `cargo test -p temporalstore-rust control_api_reads_page_and_index_streams`
  `cargo test -p temporalstore-rust control_api_reads_and_scans_wal_stream`
  `cargo test -p temporalstore-rust control_api_reads_and_scans_index_log_stream`

- C++ metaserver placement and scheduler behavior:
  `cargo test -p temporalstore-rust metaserver_topology_prefers_location_diversity_before_same_zone_load`
  `cargo test -p temporalstore-rust metaserver_topology_prefers_lower_load_replicas`
  `cargo test -p temporalstore-rust rebalance_moves_from_overloaded_node_to_low_load_node`
  `cargo test -p temporalstore-rust task_scheduler_runs_lower_priority_first_and_skips_postponed_tasks`

- C++ onebox/chaos/replication style validation:
  `cargo run -p temporalstore-rust --bin distributed_raft_harness`
  `cargo run -p temporalstore-rust --bin scale_harness -- --compare-shared-store true`
  `cargo run -p temporalstore-rust --bin storage_modes_harness`

## Focused Runner

Run:

```bash
tools/run_temporalstore_cpp_like_tests.sh
```

The runner executes the unified shared corpus, selected unit tests, distributed
Raft harness, scale/failover/shared-store harness, and storage-mode harness.
It also executes the shared C++/Rust corpus contract in
`compat/unified_temporalstore_cases.json`; set `TS_CPP_UNIFIED_TEST_CMD` to run
the same corpus against a C++ runner, or set `TS_RUN_CPP_UNIFIED_TESTS=1` to
make missing C++ execution fail the gate.
The compatibility target now includes the C++ feature-module behaviors around
missing keys, ordered windows, `feature_max_size` truncation, insert/replace
write policies, range replacement, delete, protobuf-compatible sequence row
filters, scan-bound `count` behavior, 5K-row long sequence windows, batch
sequence queries, and RESP feature commands.
