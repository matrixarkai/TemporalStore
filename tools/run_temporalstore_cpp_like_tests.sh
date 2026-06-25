#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-cpp-like-target}"
LOCAL_SCALE_TIMEOUT="${TS_CPP_LIKE_SCALE_TIMEOUT:-120s}"

cd "${ROOT}"
export CARGO_TARGET_DIR="${TARGET_DIR}"

echo "== shared c++/rust corpus integration tests =="
cargo test -p temporalstore-rust --test unified_temporalstore_corpus -- --test-threads=1

echo "== shared c++/rust corpus contract =="
tools/run_temporalstore_unified_tests.sh

echo "== c++ DataRaft serialization and fail-closed parity =="
cargo test -p temporalstore-rust \
  cpp_data_raft_replication_rejects_corrupt_log_payload \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  cpp_data_raft_replication_rejects_invalid_command_payload \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  cpp_data_raft_unavailable_consensus_fails_closed_for_safety_operations \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  data_raft_log_codec_round_trips_cxx_style_header \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  data_raft_command_codec_round_trips_batch_request \
  -- --test-threads=1

echo "== c++ stream/page/index control parity =="
cargo test -p temporalstore-rust \
  control_api_reads_page_and_index_streams \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  control_api_reads_and_scans_oplog_stream \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  control_api_reads_and_scans_index_log_stream \
  -- --test-threads=1

echo "== c++ metaserver placement and scheduler parity =="
cargo test -p temporalstore-rust \
  metaserver_topology_prefers_location_diversity_before_same_zone_load \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  metaserver_topology_prefers_lower_load_replicas \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  rebalance_moves_from_overloaded_node_to_low_load_node \
  -- --test-threads=1
cargo test -p temporalstore-rust \
  task_scheduler_runs_lower_priority_first_and_skips_postponed_tasks \
  -- --test-threads=1

echo "== c++ onebox / raft / shared-store harnesses =="
cargo run -p temporalstore-rust --bin distributed_raft_harness \
  > /tmp/temporalstore-cpp-like-raft.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-cpp-like-raft-validation \
  --log /tmp/temporalstore-cpp-like-raft.log

timeout "${LOCAL_SCALE_TIMEOUT}" cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes "${TS_CPP_LIKE_SCALE_NODES:-3}" \
  --string-ops "${TS_CPP_LIKE_STRING_OPS:-30}" \
  --hash-ops "${TS_CPP_LIKE_HASH_OPS:-10}" \
  --sequence-keys "${TS_CPP_LIKE_SEQUENCE_KEYS:-1}" \
  --sequence-len "${TS_CPP_LIKE_SEQUENCE_LEN:-50}" \
  --scale-events "${TS_CPP_LIKE_SCALE_EVENTS:-2}" \
  --failover-every "${TS_CPP_LIKE_FAILOVER_EVERY:-10}" \
  --read-sample-every "${TS_CPP_LIKE_READ_SAMPLE_EVERY:-5}" \
  --compare-shared-store true \
  --shared-store-ops "${TS_CPP_LIKE_SHARED_STORE_OPS:-30}" \
  --shared-store-flush-every "${TS_CPP_LIKE_SHARED_STORE_FLUSH_EVERY:-5}" \
  > /tmp/temporalstore-cpp-like-scale.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-cpp-like-scale-validation \
  --log /tmp/temporalstore-cpp-like-scale.log

cargo run -p temporalstore-rust --bin storage_modes_harness \
  > /tmp/temporalstore-cpp-like-storage.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-cpp-like-storage-validation \
  --log /tmp/temporalstore-cpp-like-storage.log

echo "TemporalStore C++-like test suite passed."
