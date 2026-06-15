#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-multitenant-noisy-neighbor-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
CLUSTER_NAME="${CLUSTER_NAME:-multitenant_noisy_neighbor}"
NOISY_NAMESPACE="${NOISY_NAMESPACE:-tenant_noisy}"
NOISY_TABLE="${NOISY_TABLE:-hot_table}"
VICTIM_NAMESPACE="${VICTIM_NAMESPACE:-tenant_victim}"
VICTIM_TABLE="${VICTIM_TABLE:-victim_table}"
IDC="${IDC:-vdc1}"
MS_PORT="${MS_PORT:-40100}"
SERVER_PORT="${SERVER_PORT:-15100}"
NOISY_OPS="${NOISY_OPS:-800}"
NOISY_THREADS="${NOISY_THREADS:-4}"
VICTIM_OPS="${VICTIM_OPS:-160}"
VICTIM_THREADS="${VICTIM_THREADS:-1}"
VALUE_BYTES="${VALUE_BYTES:-128}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-180}"

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

need_file "${OUT_DIR}/bcache2-server"
need_file "${OUT_DIR}/bcache2-metaserver"
need_file "${BIN_DIR}/string_scale_benchmark"

mkdir -p "${RESULT_DIR}"
rm -rf "${SMOKE_DIR}"

cleanup() {
  local status=$?
  if [[ -f "${RESULT_DIR}/bootstrap.pid" ]]; then
    kill "$(cat "${RESULT_DIR}/bootstrap.pid")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  sleep 0.2
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill -9 "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  return "${status}"
}
trap cleanup EXIT

post_json() {
  local port="$1"
  local path="$2"
  local body="$3"
  curl -fsS -m 8 \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "http://127.0.0.1:${port}/${path}"
}

wait_for_json_field() {
  local path="$1"
  local body="$2"
  local expr="$3"
  local output_file="$4"
  local attempts="${5:-120}"
  for _ in $(seq 1 "${attempts}"); do
    if post_json "${MS_PORT}" "${path}" "${body}" > "${output_file}" 2>"${output_file}.err"; then
      if python3 - "${output_file}" "${expr}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
expr = sys.argv[2]
safe_builtins = {"all": all, "any": any, "len": len, "sum": sum}
sys.exit(0 if eval(expr, {"__builtins__": safe_builtins}, {"data": data}) else 1)
PY
      then
        return 0
      fi
    fi
    sleep 0.5
  done
  echo "timed out waiting for ${path}: ${expr}" >&2
  [[ -f "${output_file}" ]] && cat "${output_file}" >&2 || true
  [[ -f "${output_file}.err" ]] && cat "${output_file}.err" >&2 || true
  return 1
}

check_status_ok() {
  local path="$1"
  python3 - "${path}" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1], encoding="utf-8"))
status = data.get("status", {})
if status.get("code", 0) not in (0, 6):
    raise SystemExit(f"request failed: {data}")
PY
}

csv_zero_errors() {
  local file="$1"
  awk -F, '
    $1 == "system" {
      for (i = 1; i <= NF; ++i) {
        if ($i == "errors") err_col = i
      }
      next
    }
    NF > 0 {
      rows += 1
      if (!err_col || $err_col != 0) bad += 1
    }
    END { exit (rows > 0 && bad == 0) ? 0 : 1 }
  ' "${file}"
}

(
  cd "${ROOT}"
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${SMOKE_DIR}" \
    CLUSTER_NAME="${CLUSTER_NAME}" \
    NAMESPACE_NAME="${NOISY_NAMESPACE}" \
    TABLE_NAME="${NOISY_TABLE}" \
    META_COUNT=1 \
    SERVER_COUNT=3 \
    REPLICA_COUNT=1 \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="$((MS_PORT + 10))" \
    MS_SNAPSHOT_PORT="$((MS_PORT + 20))" \
    SERVER_PORT="${SERVER_PORT}" \
    KEEP_RUNNING=1 \
    bash tools/smoke_ubuntu22.sh
) > "${RESULT_DIR}/bootstrap.log" 2>&1 &
echo "$!" > "${RESULT_DIR}/bootstrap.pid"

for _ in $(seq 1 180); do
  if grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/bootstrap.log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$(cat "${RESULT_DIR}/bootstrap.pid")" >/dev/null 2>&1; then
    echo "bootstrap exited early" >&2
    cat "${RESULT_DIR}/bootstrap.log" >&2 || true
    exit 1
  fi
  sleep 1
done

if ! grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/bootstrap.log"; then
  echo "bootstrap timed out" >&2
  tail -120 "${RESULT_DIR}/bootstrap.log" >&2 || true
  exit 1
fi

leader="$(awk '/metaserver leader:/ {print $3}' "${RESULT_DIR}/bootstrap.log")"
if [[ -z "${leader}" ]]; then
  echo "could not parse metaserver leader" >&2
  exit 1
fi

request_id="{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"multitenant_noisy_neighbor\"}"
id_body="{\"id\":${request_id}}"

echo "result_dir=${RESULT_DIR}" | tee "${RESULT_DIR}/summary.txt"
echo "leader=${leader}" | tee -a "${RESULT_DIR}/summary.txt"

post_json "${MS_PORT}" "ManageService/AddNamespace" \
  "{\"id\":${request_id},\"name\":\"${VICTIM_NAMESPACE}\"}" > "${RESULT_DIR}/add_victim_namespace.json"
check_status_ok "${RESULT_DIR}/add_victim_namespace.json"

post_json "${MS_PORT}" "ManageService/AddTable" \
  "{
    \"id\": ${request_id},
    \"namespace_name\": \"${VICTIM_NAMESPACE}\",
    \"name\": \"${VICTIM_TABLE}\",
    \"partition_set_num\": 1,
    \"partition_units\": [
      {
        \"partition_num\": 1,
        \"placement_set\": [{\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau2\"}],
        \"storage_pool_uri\": \"file://${SMOKE_DIR}/storage/\",
        \"primary_prefer\": {\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau2\"}
      }
    ],
    \"partition_unit_relation\": \"ANTI_ENTROPY\",
    \"election_policy\": \"PROMOTE_DERIVED\",
    \"quota\": {\"ops_read\": 1000},
    \"config\": {}
  }" > "${RESULT_DIR}/add_victim_table.json"
check_status_ok "${RESULT_DIR}/add_victim_table.json"

wait_for_json_field \
  "QueryService/ListPartition" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${VICTIM_NAMESPACE}\",\"table_name\":\"${VICTIM_TABLE}\"}" \
  "len(data.get('info', [])) >= 1 and all(p.get('state') == 'P_NORMAL' for p in data.get('info', [{}])[0].get('partition_info', []))" \
  "${RESULT_DIR}/victim_partition_ready.json" \
  120

timeout "${BENCH_TIMEOUT_S}" \
  "${BIN_DIR}/string_scale_benchmark" "${leader}" "${IDC}" "${NOISY_NAMESPACE}" "${NOISY_TABLE}" \
  "${NOISY_OPS}" "${NOISY_THREADS}" "${VALUE_BYTES}" 1 1000 \
  > "${RESULT_DIR}/noisy.out" 2> "${RESULT_DIR}/noisy.err" &
noisy_pid="$!"
sleep 1

timeout "${BENCH_TIMEOUT_S}" \
  "${BIN_DIR}/string_scale_benchmark" "${leader}" "${IDC}" "${VICTIM_NAMESPACE}" "${VICTIM_TABLE}" \
  "${VICTIM_OPS}" "${VICTIM_THREADS}" "${VALUE_BYTES}" 1 1000 \
  > "${RESULT_DIR}/victim.out" 2> "${RESULT_DIR}/victim.err"
victim_code=$?

wait "${noisy_pid}"
noisy_code=$?

cat "${RESULT_DIR}/victim.out" | tee -a "${RESULT_DIR}/summary.txt"
cat "${RESULT_DIR}/noisy.out" | tee -a "${RESULT_DIR}/summary.txt"

csv_zero_errors "${RESULT_DIR}/victim.out"
csv_zero_errors "${RESULT_DIR}/noisy.out"
if [[ "${victim_code}" != "0" || "${noisy_code}" != "0" ]]; then
  echo "benchmark failure victim=${victim_code} noisy=${noisy_code}" >&2
  cat "${RESULT_DIR}/victim.err" "${RESULT_DIR}/noisy.err" >&2 || true
  exit 1
fi

echo "PASS multitenant noisy-neighbor gate"
echo "${RESULT_DIR}"
