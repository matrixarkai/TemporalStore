# C++-Like TemporalStore Testing

This maps the local C++ TemporalStore test files to the Rust tests and harnesses
that exercise the same behavior.

## Source C++ Suite

Reference tree:

`C:\Users\Vincent Jiang\Documents\Codex\2026-05-10\bytekv-in-local-vs-etcd\clean_push\TemporalStore`

Representative C++ files inspected:

- `test/smoketest/basic_smoketest.cc`
- `test/smoketest/consistency_bench.cc`
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

- C++ basic smoke string/hash reload, TTL, delete:
  `cargo test -p temporalstore-rust --test temporalstore_compat cxx_basic_smoketest`

- C++ consistency bench mixed hash writes/reads:
  `cargo test -p temporalstore-rust --test temporalstore_compat consistency_bench_style`

- C++ stream random-size, reopen, scan, and cross-block large records:
  `cargo test -p temporalstore-rust --test temporalstore_compat cxx_stream`

- C++ DataRaft serialization, corrupt payload rejection, fail-closed backend:
  `cargo test -p temporalstore-rust cpp_data_raft`
  `cargo test -p temporalstore-rust data_raft_log_codec_round_trips_cxx_style_header`
  `cargo test -p temporalstore-rust data_raft_command_codec_round_trips_batch_request`

- C++ page/index/oplog stream read and scan:
  `cargo test -p temporalstore-rust control_api_reads_page_and_index_streams`
  `cargo test -p temporalstore-rust control_api_reads_and_scans_oplog_stream`
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

The runner executes the compatibility tests, selected unit tests, distributed
Raft harness, scale/failover/shared-store harness, and storage-mode harness.
