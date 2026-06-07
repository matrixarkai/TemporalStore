#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${META_ID:=i-003c930417f7ee609}"
: "${REDIS_HOST:=127.0.0.1}"
: "${REDIS_PORT:=19078}"
: "${CLIENTS:=8}"
: "${REQUESTS:=20000}"
: "${DATA_SIZE:=128}"
: "${KEYSPACE:=10000}"
: "${OUT_DIR:=outputs}"

mkdir -p "${OUT_DIR}"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
out="${OUT_DIR}/aws_abase_redis_api_scale_${ts}.json"

PARAMS="$(mktemp)"
REDIS_HOST="${REDIS_HOST}" \
REDIS_PORT="${REDIS_PORT}" \
CLIENTS="${CLIENTS}" \
REQUESTS="${REQUESTS}" \
DATA_SIZE="${DATA_SIZE}" \
KEYSPACE="${KEYSPACE}" \
python3 - <<'PY' > "${PARAMS}"
import json
import os

env = {k: os.environ[k] for k in [
    "REDIS_HOST", "REDIS_PORT", "CLIENTS", "REQUESTS", "DATA_SIZE", "KEYSPACE"
]}

script = r'''
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
if ! command -v redis-cli >/dev/null 2>&1 || ! command -v redis-benchmark >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y redis-tools
fi

HOST="${REDIS_HOST}"
PORT="${REDIS_PORT}"
CLIENTS="${CLIENTS}"
REQUESTS="${REQUESTS}"
DATA_SIZE="${DATA_SIZE}"
KEYSPACE="${KEYSPACE}"

python3 - <<'PYRUN'
import json
import os
import subprocess
import time

host = os.environ["REDIS_HOST"]
port = os.environ["REDIS_PORT"]
clients = os.environ["CLIENTS"]
requests = os.environ["REQUESTS"]
data_size = os.environ["DATA_SIZE"]
keyspace = os.environ["KEYSPACE"]

def run(cmd, timeout=120):
    started = time.time()
    p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    return {
        "cmd": cmd,
        "returncode": p.returncode,
        "stdout": p.stdout,
        "stderr": p.stderr,
        "seconds": time.time() - started,
    }

def redis_cli(*args):
    return run(["redis-cli", "-h", host, "-p", port, "--raw", *args], timeout=30)

smoke = {}
smoke["ping"] = redis_cli("PING")
smoke["set"] = redis_cli("SET", "abase:redis:smoke:string", "v1")
smoke["get"] = redis_cli("GET", "abase:redis:smoke:string")
smoke["incr"] = redis_cli("INCR", "abase:redis:smoke:counter")
smoke["hset"] = redis_cli("HSET", "abase:redis:smoke:hash", "field1", "value1")
smoke["hget"] = redis_cli("HGET", "abase:redis:smoke:hash", "field1")
smoke["mget"] = redis_cli("MGET", "abase:redis:smoke:string", "abase:redis:smoke:missing")

benchmark_tests = ["set", "get", "mget", "hset", "hget", "incr"]
benchmarks = {}
for test in benchmark_tests:
    cmd = [
        "redis-benchmark",
        "-h", host,
        "-p", port,
        "-c", clients,
        "-n", requests,
        "-d", data_size,
        "-r", keyspace,
        "-t", test,
        "--csv",
    ]
    benchmarks[test] = run(cmd, timeout=300)

result = {
    "system": "abase",
    "api": "redis-resp",
    "endpoint": f"{host}:{port}",
    "clients": int(clients),
    "requests": int(requests),
    "data_size": int(data_size),
    "keyspace": int(keyspace),
    "smoke": smoke,
    "benchmarks": benchmarks,
}
print(json.dumps(result, sort_keys=True))
PYRUN
'''

for key, value in env.items():
    script = f'export {key}="{value}"\n' + script
print(json.dumps({
    "commands": [
        "cat > /tmp/aws_abase_redis_api_benchmark.sh <<'__ABASE_REDIS_BENCH__'\n"
        + script
        + "\n__ABASE_REDIS_BENCH__\n"
        + "bash /tmp/aws_abase_redis_api_benchmark.sh"
    ]
}))
PY

CMD_ID="$(aws --profile "${AWS_PROFILE}" --region "${AWS_REGION}" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "${META_ID}" \
  --parameters "file://${PARAMS}" \
  --query 'Command.CommandId' \
  --output text)"
rm -f "${PARAMS}"

aws --profile "${AWS_PROFILE}" --region "${AWS_REGION}" ssm wait command-executed \
  --command-id "${CMD_ID}" \
  --instance-id "${META_ID}" || true

aws --profile "${AWS_PROFILE}" --region "${AWS_REGION}" ssm get-command-invocation \
  --command-id "${CMD_ID}" \
  --instance-id "${META_ID}" \
  --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
  --output json | tee "${out}"

echo "Wrote ${out}" >&2
