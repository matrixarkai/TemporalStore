#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-storage-raft-production-target}"
ARTIFACT_DIR="${TS_STORAGE_RAFT_ARTIFACT_DIR:-/tmp/temporalstore-storage-raft-production-$(date +%s)-$$}"
TIMEOUT="${TS_STORAGE_RAFT_TIMEOUT:-120s}"

cd "${ROOT}"
export CARGO_TARGET_DIR="${TARGET_DIR}"
mkdir -p "${ARTIFACT_DIR}"

echo "== 1/7 storage recovery/fault matrix hardening =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin storage_fault_matrix_harness -- \
  --root "${ARTIFACT_DIR}/storage-fault-matrix" \
  > "${ARTIFACT_DIR}/storage-fault-matrix.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-storage-fault-matrix-validation \
  --log "${ARTIFACT_DIR}/storage-fault-matrix.json"

echo "== 2/7 slot dump/load atomicity and manifest rejection =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin storage_production_harness -- \
  --root "${ARTIFACT_DIR}/storage-production" \
  > "${ARTIFACT_DIR}/storage-production.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-storage-production-validation \
  --log "${ARTIFACT_DIR}/storage-production.json"

echo "== 3/7 follower-safe GC and cache pressure =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin storage_modes_harness \
  > "${ARTIFACT_DIR}/storage-modes.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-storage-validation \
  --log "${ARTIFACT_DIR}/storage-modes.json"

echo "== 4/7 real Raft FSM/storage readiness selection =="
python3 tools/validate_storage_raft_production_plan.py
cargo run -p temporalstore-rust --bin readiness_gate -- --service raft_replication \
  > "${ARTIFACT_DIR}/raft-readiness.json" \
  2> "${ARTIFACT_DIR}/raft-readiness.log" || READINESS_STATUS=$?
READINESS_STATUS="${READINESS_STATUS:-0}"
if [[ "${READINESS_STATUS}" -ne 0 && "${READINESS_STATUS}" -ne 1 && "${READINESS_STATUS}" -ne 2 ]]; then
  cat "${ARTIFACT_DIR}/raft-readiness.log" >&2
  exit "${READINESS_STATUS}"
fi
python3 - <<PY
import json
report = json.load(open("${ARTIFACT_DIR}/raft-readiness.json"))
if "failed_capabilities" in report:
    blockers = [
        item["capability"]
        for item in report["failed_capabilities"]
        if item.get("area") == "raft_replication"
    ]
    ready = not blockers
else:
    blockers = report.get("missing", [])
    ready = bool(report.get("ready"))
print(f"raft_replication_ready={ready}")
if blockers:
    print("raft_replication_missing:")
    print("\\n".join(f"- {item}" for item in blockers))
if ${TS_REQUIRE_STORAGE_RAFT_READY:-0} and not ready:
    raise SystemExit("raft_replication readiness is still blocked")
PY

echo "== 5/7 raft snapshot/restart/failover harness =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin distributed_raft_harness -- \
  --root "${ARTIFACT_DIR}/distributed-raft" \
  > "${ARTIFACT_DIR}/distributed-raft.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-validation \
  --log "${ARTIFACT_DIR}/distributed-raft.json"

cargo build -p temporalstore-rust --bins
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin raft_secondary_replication_harness -- \
  --root "${ARTIFACT_DIR}/raft-secondary" \
  --heartbeat-ms "${TS_STORAGE_RAFT_HEARTBEAT_MS:-25}" \
  > "${ARTIFACT_DIR}/raft-secondary.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-secondary-validation \
  --log "${ARTIFACT_DIR}/raft-secondary.json"

echo "== 6/7 combined storage plus raft production harness =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin external_chaos_gate -- \
  --root "${ARTIFACT_DIR}/external-chaos" \
  --profile quick \
  > "${ARTIFACT_DIR}/external-chaos.json"
python3 - <<PY
import json
report = json.load(open("${ARTIFACT_DIR}/external-chaos.json"))
if not report.get("production_ready_slice"):
    raise SystemExit("external chaos storage/raft slice failed")
print(
    "external-chaos-validation: "
    f"scenarios={report['scenario_count']} passed={report['passed_count']}"
)
PY

echo "== 7/7 unified corpus and readiness docs =="
python3 tools/run_temporalstore_unified_tests.py --validate-only
python3 tools/validate_raft_storage_parity_evidence.py
python3 tools/validate_no_duplicate_tests.py
git diff --check

echo "TemporalStore storage/Raft production-readiness local gate passed."
echo "Artifacts: ${ARTIFACT_DIR}"
