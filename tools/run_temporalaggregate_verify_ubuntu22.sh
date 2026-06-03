#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${ROOT}/output-client/debug}"
BIN_DIR="${BIN_DIR:-/home/vj/temporalstore-build-client/debug/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalaggregate-verify-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
CLUSTER_NAME="${CLUSTER_NAME:-temporalaggregate_verify}"
MS_PORT="${MS_PORT:-25000}"
MS_RAFT_PORT="${MS_RAFT_PORT:-25100}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-25200}"
SERVER_PORT="${SERVER_PORT:-25300}"

mkdir -p "${RESULT_DIR}"
rm -rf "${SMOKE_DIR}"

cleanup() {
  local status=$?
  if [[ -d "${SMOKE_DIR}" ]]; then
    for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
      [[ -f "${pid_file}" ]] || continue
      kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
    done
  fi
  if [[ -f "${RESULT_DIR}/smoke.pid" ]]; then
    kill "$(cat "${RESULT_DIR}/smoke.pid")" >/dev/null 2>&1 || true
  fi
  return "${status}"
}
trap cleanup EXIT

(
  cd "${ROOT}"
  OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${SMOKE_DIR}" \
    CLUSTER_NAME="${CLUSTER_NAME}" \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="${MS_RAFT_PORT}" \
    MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
    SERVER_PORT="${SERVER_PORT}" \
    KEEP_RUNNING=1 \
    bash tools/smoke_ubuntu22.sh
) >"${RESULT_DIR}/smoke.log" 2>&1 &
echo "$!" >"${RESULT_DIR}/smoke.pid"

for _ in $(seq 1 180); do
  if grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/smoke.log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$(cat "${RESULT_DIR}/smoke.pid")" >/dev/null 2>&1; then
    echo "smoke exited early" >&2
    cat "${RESULT_DIR}/smoke.log" >&2 || true
    exit 1
  fi
  sleep 1
done

if ! grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/smoke.log"; then
  echo "smoke timed out" >&2
  tail -120 "${RESULT_DIR}/smoke.log" >&2 || true
  exit 1
fi

leader="$(awk '/metaserver leader:/ {print $3}' "${RESULT_DIR}/smoke.log")"
if [[ -z "${leader}" ]]; then
  echo "could not parse metaserver leader" >&2
  cat "${RESULT_DIR}/smoke.log" >&2
  exit 1
fi

partition_ready_json="${RESULT_DIR}/partition_ready.json"
for _ in $(seq 1 120); do
  if curl -fsS -m 3 \
    -H "Content-Type: application/json" \
    -d "{\"id\":{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"temporalaggregate_verify\"},\"read_stale\":false,\"namespace_name\":\"ns1\",\"table_name\":\"table1\"}" \
    "http://${leader}/QueryService/ListPartition" \
    >"${partition_ready_json}" 2>"${partition_ready_json}.err"; then
    if python3 - "${partition_ready_json}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
for table in data.get("info", []):
    if table.get("set_info", {}).get("state") != "PSET_NORMAL":
        continue
    partitions = table.get("partition_info", [])
    if partitions and all(p.get("state") == "P_NORMAL" for p in partitions):
        sys.exit(0)
sys.exit(1)
PY
    then
      break
    fi
  fi
  sleep 0.5
done

if ! python3 - "${partition_ready_json}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
for table in data.get("info", []):
    if table.get("set_info", {}).get("state") == "PSET_NORMAL":
        partitions = table.get("partition_info", [])
        if partitions and all(p.get("state") == "P_NORMAL" for p in partitions):
            sys.exit(0)
sys.exit(1)
PY
then
  echo "timed out waiting for partition readiness" >&2
  cat "${partition_ready_json}" >&2 || true
  cat "${partition_ready_json}.err" >&2 || true
  exit 1
fi

"${BIN_DIR}/module_ingest_query_example" \
  "${leader}" vdc1 ns1 table1 \
  >"${RESULT_DIR}/module_ingest.out" \
  2>"${RESULT_DIR}/module_ingest.err"

{
  echo "result_dir=${RESULT_DIR}"
  echo "leader=${leader}"
  echo
  grep -E "TemporalStore Ubuntu smoke test passed|metaserver leader|server[0-9] pid|logs:" \
    "${RESULT_DIR}/smoke.log" || true
  echo
  cat "${RESULT_DIR}/module_ingest.out"
  if [[ -s "${RESULT_DIR}/module_ingest.err" ]]; then
    echo
    echo "stderr"
    cat "${RESULT_DIR}/module_ingest.err"
  fi
} | tee "${RESULT_DIR}/summary.txt"

echo "PASS temporal aggregate verification"
