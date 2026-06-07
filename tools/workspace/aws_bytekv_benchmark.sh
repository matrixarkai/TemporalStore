#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

META_ID="${META_ID:-i-003c930417f7ee609}"
ACCOUNT_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" sts get-caller-identity --query Account --output text)"
BUCKET="${ARTIFACT_BUCKET:-temporalstore-test-artifacts-${ACCOUNT_ID}-${AWS_REGION}}"
KEY="one-cluster/clients/bytekv_aws_smoke-bench-$(date -u +%Y%m%dT%H%M%SZ)"
LOCAL_BIN="${LOCAL_BIN:-/home/vj/bytekv-rocksdb-server/build-release/stripped/bytekv_aws_smoke}"

REPLICA_COUNT_VALUE="${REPLICA_COUNT:-1}"
WAIT_SECONDS_VALUE="${WAIT_SECONDS:-180}"
QUOTA_GB_VALUE="${QUOTA_GB:-1}"
PARTITION_SIZE_MB_VALUE="${PARTITION_SIZE_MB:-1024}"

THREADS_VALUE="${THREADS:-2}"
OPS_VALUE="${OPS:-20000}"
READ_PERCENT_VALUE="${READ_PERCENT:-50}"
KEY_COUNT_VALUE="${KEY_COUNT:-2000}"
VALUE_SIZE_VALUE="${VALUE_SIZE:-128}"
TIMEOUT_MS_VALUE="${TIMEOUT_MS:-5000}"
TABLE_VALUE="${TABLE:-bench_$(date -u +%H%M%S)}"

aws --profile "$AWS_PROFILE" --region "$AWS_REGION" s3 cp "$LOCAL_BIN" "s3://${BUCKET}/${KEY}" >/dev/null
URL="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" s3 presign "s3://${BUCKET}/${KEY}" --expires-in 3600)"

PARAMS="$(mktemp)"
REMOTE_SCRIPT="$(cat <<SCRIPT
set -e
mkdir -p /opt/bytekv/client-tools /tmp/bytekv-bench
BIN=/opt/bytekv/client-tools/bytekv_aws_smoke_bench_$(date -u +%H%M%S)_$RANDOM
curl -fL '${URL}' -o \$BIN
chmod +x \$BIN
echo bytekv_benchmark_params table=${TABLE_VALUE} replica_count=${REPLICA_COUNT_VALUE} wait_seconds=${WAIT_SECONDS_VALUE} quota_gb=${QUOTA_GB_VALUE} partition_size_mb=${PARTITION_SIZE_MB_VALUE} threads=${THREADS_VALUE} ops=${OPS_VALUE} read_percent=${READ_PERCENT_VALUE} key_count=${KEY_COUNT_VALUE} value_size=${VALUE_SIZE_VALUE}
date -u '+start_utc=%Y-%m-%dT%H:%M:%SZ'
\$BIN --master=10.70.1.79:26010 --tso=10.70.1.79:26020 \
  --ns=aws_scale --table=${TABLE_VALUE} --key=smoke_key --value=smoke_value \
  --replica_count=${REPLICA_COUNT_VALUE} --wait_seconds=${WAIT_SECONDS_VALUE} \
  --quota_gb=${QUOTA_GB_VALUE} --partition_size_mb=${PARTITION_SIZE_MB_VALUE} \
  --benchmark_mode=1 --threads=${THREADS_VALUE} --ops=${OPS_VALUE} \
  --read_percent=${READ_PERCENT_VALUE} --key_count=${KEY_COUNT_VALUE} \
  --value_size=${VALUE_SIZE_VALUE} --timeout_ms=${TIMEOUT_MS_VALUE}
date -u '+end_utc=%Y-%m-%dT%H:%M:%SZ'
echo process_snapshot
ps -eo pid,comm,%cpu,%mem,args | egrep 'partitionserver|kvmaster|kvproxy|tso|bytekv_aws_smoke' | grep -v egrep || true
SCRIPT
)"
SCRIPT_TEXT="$REMOTE_SCRIPT" python3 - <<'PY' > "$PARAMS"
import json
import os

print(json.dumps({"commands": [os.environ["SCRIPT_TEXT"]]}))
PY

CMD_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "$META_ID" \
  --parameters "file://$PARAMS" \
  --query 'Command.CommandId' \
  --output text)"
rm -f "$PARAMS"
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm wait command-executed --command-id "$CMD_ID" --instance-id "$META_ID" || true
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm get-command-invocation \
  --command-id "$CMD_ID" \
  --instance-id "$META_ID" \
  --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
  --output json
