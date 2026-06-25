#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-raft-distributed-parity-target}"
ARTIFACT_DIR="${TS_RAFT_PARITY_ARTIFACT_DIR:-/tmp/temporalstore-raft-distributed-parity-$(date +%s)-$$}"
TIMEOUT="${TS_RAFT_PARITY_TIMEOUT:-180s}"

cd "${ROOT}"
export CARGO_TARGET_DIR="${TARGET_DIR}"
mkdir -p "${ARTIFACT_DIR}"

echo "== 1/4 data-node distributed Raft replication, membership, snapshot =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin distributed_raft_harness -- \
  --root "${ARTIFACT_DIR}/distributed-raft" \
  > "${ARTIFACT_DIR}/distributed-raft.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-validation \
  --log "${ARTIFACT_DIR}/distributed-raft.json"

echo "== 2/4 data-node secondary replication, restart, partition, failover =="
cargo build -p temporalstore-rust --bins
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin raft_secondary_replication_harness -- \
  --root "${ARTIFACT_DIR}/raft-secondary" \
  --heartbeat-ms "${TS_RAFT_PARITY_HEARTBEAT_MS:-25}" \
  > "${ARTIFACT_DIR}/raft-secondary.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-secondary-validation \
  --log "${ARTIFACT_DIR}/raft-secondary.json"

echo "== 3/4 metaserver Raft membership, read-index, snapshot, failover =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin metaserver_raft_harness -- \
  --root "${ARTIFACT_DIR}/metaserver-raft" \
  > "${ARTIFACT_DIR}/metaserver-raft.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-metaserver-raft-validation \
  --log "${ARTIFACT_DIR}/metaserver-raft.json"

echo "== 4/4 combined Raft parity summary =="
python3 tools/build_raft_distributed_parity_summary.py \
  --artifact-dir "${ARTIFACT_DIR}" \
  --output "${ARTIFACT_DIR}/raft-distributed-parity.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-distributed-parity-validation \
  --log "${ARTIFACT_DIR}/raft-distributed-parity.json"

echo "TemporalStore distributed Raft parity gate passed."
echo "Artifacts: ${ARTIFACT_DIR}"
