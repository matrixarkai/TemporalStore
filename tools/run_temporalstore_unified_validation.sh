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

echo "== unified: workflow/test contract guards =="
python3 tools/validate_readiness_workflow.py
python3 tools/run_temporalstore_unified_tests.py --validate-only
python3 tools/validate_sdk_contract.py

echo "== unified: unit and API compatibility tests =="
cargo test -p temporalstore-rust --test temporalstore_compat -- --test-threads=1

echo "== unified: shared API corpus =="
tools/run_temporalstore_unified_tests.sh
if [[ "${RUN_CPP}" == "1" ]]; then
  python3 tools/run_temporalstore_unified_tests.py --both --require-cpp
fi

echo "== unified: storage integration tests =="
cargo test -p temporalstore-rust --test storage_migration_corpus -- --test-threads=1
cargo test -p temporalstore-rust --test storage_crash_harness -- --test-threads=1

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
