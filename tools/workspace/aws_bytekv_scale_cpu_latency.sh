#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${META_ID:=i-003c930417f7ee609}"
: "${DATA01_ID:=i-0724d90b323786546}"
: "${DATA02_ID:=i-096334bd8cc7ab259}"
: "${OUT_DIR:=outputs}"

: "${THREAD_SWEEP:=1 2 4 8}"
: "${OPS:=20000}"
: "${KEY_COUNT:=5000}"
: "${VALUE_SIZE:=128}"
: "${MONITOR_SECONDS:=90}"
: "${MONITOR_INTERVAL_SECONDS:=1}"
: "${WAIT_SECONDS:=180}"
: "${REPLICA_COUNT:=1}"
: "${QUOTA_GB:=1}"
: "${PARTITION_SIZE_MB:=1024}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
mkdir -p "${OUT_DIR}"

ts="$(date -u +%Y%m%dT%H%M%SZ)"
result_dir="${OUT_DIR}/aws_bytekv_scale_cpu_${ts}"
mkdir -p "${result_dir}"

run_ssm_async() {
  local instance_id="$1"
  local command="$2"
  local payload
  payload="$(mktemp)"
  COMMAND_PAYLOAD="${command}" python3 - <<'PY' > "${payload}"
import json
import os
script = os.environ["COMMAND_PAYLOAD"]
print(json.dumps({
    "commands": [
        "cat > /tmp/aws_bytekv_monitor.sh <<'__BYTEKV_MONITOR__'\n"
        + script
        + "\n__BYTEKV_MONITOR__\n"
        + "bash /tmp/aws_bytekv_monitor.sh"
    ],
    "executionTimeout": ["900"],
}))
PY
  local cmd_id
  cmd_id="$(aws --profile "${AWS_PROFILE}" --region "${AWS_REGION}" ssm send-command \
    --instance-ids "${instance_id}" \
    --document-name AWS-RunShellScript \
    --parameters "file://${payload}" \
    --query 'Command.CommandId' \
    --output text)"
  rm -f "${payload}"
  echo "${cmd_id}"
}

wait_ssm() {
  local instance_id="$1"
  local cmd_id="$2"
  local out="$3"
  aws --profile "${AWS_PROFILE}" --region "${AWS_REGION}" ssm wait command-executed \
    --command-id "${cmd_id}" \
    --instance-id "${instance_id}" || true
  aws --profile "${AWS_PROFILE}" --region "${AWS_REGION}" ssm get-command-invocation \
    --command-id "${cmd_id}" \
    --instance-id "${instance_id}" \
    --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
    --output json > "${out}"
}

monitor_script() {
  local seconds="$1"
  local interval="$2"
  cat <<SCRIPT
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
if ! command -v pidstat >/dev/null 2>&1; then
  sudo apt-get update >/dev/null 2>&1 || true
  sudo apt-get install -y sysstat >/dev/null 2>&1 || true
fi
echo "node=\$(hostname)"
echo "start_utc=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "--- lscpu ---"
lscpu | egrep 'CPU\\(s\\)|Model name|Thread|Core|Socket' || true
echo "--- bytekv pids ---"
pgrep -af 'partitionserver|kvmaster|kvproxy|tso' || true
echo "--- disk before ---"
df -h / /mnt/temporalstore-cache /mnt/bytekv-data 2>/dev/null || true
echo "--- vmstat ---"
vmstat ${interval} ${seconds} || true
echo "--- pidstat bytekv ---"
PIDS="\$(pgrep -d, -f 'partitionserver|kvmaster|kvproxy|tso' || true)"
if [ -n "\$PIDS" ] && command -v pidstat >/dev/null 2>&1; then
  pidstat -h -u -r -d -p "\$PIDS" ${interval} ${seconds} || true
fi
echo "--- top bytekv snapshots ---"
for i in \$(seq 1 ${seconds}); do
  date -u +snapshot_utc=%Y-%m-%dT%H:%M:%SZ
  ps -eo pid,comm,%cpu,%mem,rss,args | egrep 'partitionserver|kvmaster|kvproxy|tso' | grep -v egrep || true
  sleep ${interval}
done
echo "--- disk after ---"
df -h / /mnt/temporalstore-cache /mnt/bytekv-data 2>/dev/null || true
echo "end_utc=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SCRIPT
}

run_bench() {
  local label="$1"
  local threads="$2"
  local read_percent="$3"
  local ops="$4"
  local table="bench_${label}_t${threads}_${ts}"
  echo "=== ByteKV ${label} threads=${threads} read_percent=${read_percent} ops=${ops} ==="

  local mon01 mon02
  mon01="$(run_ssm_async "${DATA01_ID}" "$(monitor_script "${MONITOR_SECONDS}" "${MONITOR_INTERVAL_SECONDS}")")"
  mon02="$(run_ssm_async "${DATA02_ID}" "$(monitor_script "${MONITOR_SECONDS}" "${MONITOR_INTERVAL_SECONDS}")")"
  sleep 3

  local bench_out="${result_dir}/bench_${label}_t${threads}.json"
  THREADS="${threads}" \
    OPS="${ops}" \
    READ_PERCENT="${read_percent}" \
    KEY_COUNT="${KEY_COUNT}" \
    VALUE_SIZE="${VALUE_SIZE}" \
    TABLE="${table}" \
    WAIT_SECONDS="${WAIT_SECONDS}" \
    REPLICA_COUNT="${REPLICA_COUNT}" \
    QUOTA_GB="${QUOTA_GB}" \
    PARTITION_SIZE_MB="${PARTITION_SIZE_MB}" \
    AWS_PROFILE="${AWS_PROFILE}" \
    AWS_REGION="${AWS_REGION}" \
    META_ID="${META_ID}" \
    tools/aws_bytekv_benchmark.sh | tee "${bench_out}"

  wait_ssm "${DATA01_ID}" "${mon01}" "${result_dir}/cpu_data01_${label}_t${threads}.json"
  wait_ssm "${DATA02_ID}" "${mon02}" "${result_dir}/cpu_data02_${label}_t${threads}.json"
}

{
  echo "# ByteKV AWS Scale CPU/Latency Run"
  echo
  echo "- timestamp: ${ts}"
  echo "- ops per run: ${OPS}"
  echo "- key_count: ${KEY_COUNT}"
  echo "- value_size: ${VALUE_SIZE}"
  echo "- monitor_seconds: ${MONITOR_SECONDS}"
  echo "- thread_sweep: ${THREAD_SWEEP}"
} > "${result_dir}/README.md"

for threads in ${THREAD_SWEEP}; do
  run_bench "read" "${threads}" 100 "${OPS}"
  run_bench "mixed" "${threads}" 50 "${OPS}"
  run_bench "write" "${threads}" 0 "${OPS}"
done

echo "Result dir: ${result_dir}"
