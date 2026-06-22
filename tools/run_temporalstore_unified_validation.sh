#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-unified-validation-target}"
LOCAL_SCALE_TIMEOUT="${TS_UNIFIED_SCALE_TIMEOUT:-90s}"
RUN_CPP="${TS_UNIFIED_RUN_CPP:-0}"

usage() {
  cat >&2 <<'USAGE'
usage: run_temporalstore_unified_validation.sh [--with-cpp]

Runs one local unified TemporalStore validation pass across:
  - unit/compat tests
  - API and shared C++/Rust corpus tests
  - storage integration tests
  - data-node plus metaserver Raft distributed parity
  - local scale/shared-store validation
  - production-readiness reporting

Set TS_UNIFIED_RUN_CPP=1 or pass --with-cpp to also run the configured C++
corpus hook through tools/run_temporalstore_unified_tests.py --both.
USAGE
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-cpp)
      RUN_CPP=1
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

cd "${ROOT}"
export CARGO_TARGET_DIR="${TARGET_DIR}"
RAFT_CPP_EVIDENCE_ARGS=()
if [[ -n "${TS_CPP_REPO:-}" ]]; then
  RAFT_CPP_EVIDENCE_ARGS+=(--cpp-repo "${TS_CPP_REPO}")
fi

echo "== unified: workflow/test contract guards =="
python3 tools/validate_readiness_workflow.py
python3 tools/validate_rust_vs_cpp_parity_report.py
python3 tools/run_temporalstore_unified_tests.py --validate-only
python3 tools/validate_no_duplicate_tests.py
python3 tools/validate_raft_storage_parity_evidence.py "${RAFT_CPP_EVIDENCE_ARGS[@]}"
python3 tools/run_raft_shared_cases.py --validate-only
python3 tools/validate_storage_raft_production_plan.py
python3 tools/validate_control_plane_parity_evidence.py
python3 tools/run_control_plane_shared_cases.py --validate-only
python3 tools/validate_api_model_parity_evidence.py
python3 tools/validate_ingestion_ops_parity_evidence.py
python3 tools/run_ingestion_shared_cases.py --validate-only
python3 tools/validate_sdk_contract.py

echo "== unified: unit and API compatibility tests =="
cargo test -p temporalstore-rust --test temporalstore_compat -- --test-threads=1

echo "== unified: shared control-plane cases =="
python3 tools/run_control_plane_shared_cases.py --rust

echo "== unified: shared ingestion cases =="
python3 tools/run_ingestion_shared_cases.py --rust

echo "== unified: shared API corpus =="
tools/run_temporalstore_unified_tests.sh
if [[ "${RUN_CPP}" == "1" ]]; then
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
fi

echo "== unified: storage integration tests =="
cargo test -p temporalstore-rust --test storage_migration_corpus -- --test-threads=1
cargo test -p temporalstore-rust --test storage_crash_harness -- --test-threads=1

echo "== unified: data-node/metaserver raft distributed parity =="
TS_RAFT_PARITY_ARTIFACT_DIR="${TS_UNIFIED_RAFT_PARITY_ARTIFACT_DIR:-/tmp/temporalstore-unified-raft-parity}" \
TS_RAFT_PARITY_TIMEOUT="${TS_UNIFIED_RAFT_PARITY_TIMEOUT:-180s}" \
tools/run_raft_distributed_parity.sh

echo "== unified: compact scale/shared-store harness =="
timeout "${LOCAL_SCALE_TIMEOUT}" cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes "${TS_UNIFIED_SCALE_NODES:-3}" \
  --string-ops "${TS_UNIFIED_STRING_OPS:-12}" \
  --hash-ops "${TS_UNIFIED_HASH_OPS:-4}" \
  --sequence-keys "${TS_UNIFIED_SEQUENCE_KEYS:-1}" \
  --sequence-len "${TS_UNIFIED_SEQUENCE_LEN:-12}" \
  --scale-events "${TS_UNIFIED_SCALE_EVENTS:-1}" \
  --failover-every "${TS_UNIFIED_FAILOVER_EVERY:-6}" \
  --read-sample-every "${TS_UNIFIED_READ_SAMPLE_EVERY:-3}" \
  --compare-shared-store true \
  --shared-store-ops "${TS_UNIFIED_SHARED_STORE_OPS:-12}" \
  --shared-store-flush-every "${TS_UNIFIED_SHARED_STORE_FLUSH_EVERY:-4}" \
  > /tmp/temporalstore-unified-scale-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-scale-validation \
  --log /tmp/temporalstore-unified-scale-validation.log

echo "== unified: storage modes integration harness =="
cargo run -p temporalstore-rust --bin storage_modes_harness \
  > /tmp/temporalstore-unified-storage-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-storage-validation \
  --log /tmp/temporalstore-unified-storage-validation.log

echo "== unified: readiness report =="
set +e
cargo run -p temporalstore-rust --bin readiness_gate -- --service-reports \
  > /tmp/temporalstore-unified-readiness.json \
  2> /tmp/temporalstore-unified-readiness.log
READINESS_STATUS=$?
set -e
cat /tmp/temporalstore-unified-readiness.log
python3 - <<'PY'
import json
report = json.load(open("/tmp/temporalstore-unified-readiness.json"))
if isinstance(report, list):
    services = report
    blocker_count = sum(service["blocker_count"] for service in services)
    production_ready = all(service["ready"] for service in services)
    print(f"production_ready={production_ready}")
    print("cpp_parity_ready=false")
    print(f"blocker_count={blocker_count}")
else:
    services = report["service_summaries"]
    print(f"production_ready={report['production_ready']}")
    print(f"cpp_parity_ready={report['cpp_parity_ready']}")
    print(f"blocker_count={report['blocker_count']}")
for service in services:
    print(
        f"- {service['service']}: ready={service['ready']} "
        f"blockers={service['blocker_count']} next={service['next_action']}"
    )
PY
if [[ "${READINESS_STATUS}" -ne 0 && "${READINESS_STATUS}" -ne 1 && "${READINESS_STATUS}" -ne 2 ]]; then
  exit "${READINESS_STATUS}"
fi

echo "== unified: whitespace =="
git diff --check

echo "TemporalStore unified validation passed."
