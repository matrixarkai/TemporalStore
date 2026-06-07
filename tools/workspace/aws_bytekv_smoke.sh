#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

META_ID="${META_ID:-i-003c930417f7ee609}"
ACCOUNT_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" sts get-caller-identity --query Account --output text)"
BUCKET="${ARTIFACT_BUCKET:-temporalstore-test-artifacts-${ACCOUNT_ID}-${AWS_REGION}}"
KEY="one-cluster/clients/bytekv_aws_smoke-$(date -u +%Y%m%dT%H%M%SZ)"
LOCAL_BIN="${LOCAL_BIN:-/home/vj/bytekv-rocksdb-server/build-release/stripped/bytekv_aws_smoke}"
REPLICA_COUNT_VALUE="${REPLICA_COUNT:-1}"
WAIT_SECONDS_VALUE="${WAIT_SECONDS:-420}"
QUOTA_GB_VALUE="${QUOTA_GB:-1}"
PARTITION_SIZE_MB_VALUE="${PARTITION_SIZE_MB:-1024}"

aws --profile "$AWS_PROFILE" --region "$AWS_REGION" s3 cp "$LOCAL_BIN" "s3://${BUCKET}/${KEY}" >/dev/null
URL="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" s3 presign "s3://${BUCKET}/${KEY}" --expires-in 3600)"

PARAMS="$(mktemp)"
REMOTE_SCRIPT="$(cat <<SCRIPT
set -e
mkdir -p /opt/bytekv/client-tools
BIN=/opt/bytekv/client-tools/bytekv_aws_smoke_$(date -u +%H%M%S)_$RANDOM
curl -fL '${URL}' -o \$BIN
chmod +x \$BIN
ldd \$BIN || true
echo bytekv_smoke_params replica_count=${REPLICA_COUNT_VALUE} wait_seconds=${WAIT_SECONDS_VALUE} quota_gb=${QUOTA_GB_VALUE} partition_size_mb=${PARTITION_SIZE_MB_VALUE} bin=\$BIN
\$BIN --master=10.70.1.79:26010 --tso=10.70.1.79:26020 --ns=aws_scale --table=smoke_$(date -u +%H%M%S) --key=k1 --value=v1 --replica_count=${REPLICA_COUNT_VALUE} --wait_seconds=${WAIT_SECONDS_VALUE} --quota_gb=${QUOTA_GB_VALUE} --partition_size_mb=${PARTITION_SIZE_MB_VALUE}
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
