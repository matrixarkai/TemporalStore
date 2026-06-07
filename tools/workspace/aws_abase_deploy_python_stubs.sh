#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
META_ID="${META_ID:-i-003c930417f7ee609}"
LOCAL_TGZ="${LOCAL_TGZ:-/tmp/abase_bytebase_thrift_py.tar.gz}"

ACCOUNT_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" sts get-caller-identity --query Account --output text)"
BUCKET="${ARTIFACT_BUCKET:-temporalstore-test-artifacts-${ACCOUNT_ID}-${AWS_REGION}}"
KEY="one-cluster/abase/abase_bytebase_thrift_py-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"

aws --profile "$AWS_PROFILE" --region "$AWS_REGION" s3 cp "$LOCAL_TGZ" "s3://${BUCKET}/${KEY}" >/dev/null
URL="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" s3 presign "s3://${BUCKET}/${KEY}" --expires-in 3600)"

PARAMS="$(mktemp)"
URL="$URL" python3 - <<'PY' > "$PARAMS"
import json
import os

url = os.environ["URL"]
script = f'''
set -e
mkdir -p /opt/abase/abase-runtime/python/test
curl -fL {url!r} -o /tmp/abase_bytebase_thrift_py.tar.gz
rm -rf /opt/abase/abase-runtime/python/test/bytebase
rm -rf /opt/abase/abase-runtime/python/base
mkdir -p /tmp/abase-thrift-unpack
rm -rf /tmp/abase-thrift-unpack/*
tar -C /tmp/abase-thrift-unpack -xzf /tmp/abase_bytebase_thrift_py.tar.gz
mv /tmp/abase-thrift-unpack/bytebase /opt/abase/abase-runtime/python/test/bytebase
if [ -d /tmp/abase-thrift-unpack/base ]; then
  mv /tmp/abase-thrift-unpack/base /opt/abase/abase-runtime/python/base
fi
touch /opt/abase/abase-runtime/python/test/__init__.py
PYTHONPATH=/opt/abase/abase-runtime/python python3 - <<'P'
from test.bytebase import ThriftService
from test.bytebase.ttypes import ErrorCode
print("abase-thrift-stubs-ok", ErrorCode.OK)
P
'''
print(json.dumps({"commands": [script]}))
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
