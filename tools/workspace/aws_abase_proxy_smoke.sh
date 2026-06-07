#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${NAMESPACE:=aws_scale}"
: "${TABLE:=bench}"
: "${PROXY:=127.0.0.1:19078}"

META_ID="${META_ID:-i-003c930417f7ee609}"

PARAMS="$(mktemp)"
NAMESPACE="$NAMESPACE" TABLE="$TABLE" PROXY="$PROXY" python3 - <<'PY' > "$PARAMS"
import json
import os

ns = os.environ["NAMESPACE"]
table = os.environ["TABLE"]
proxy = os.environ["PROXY"]
script = f'''
set -e
cd /opt/abase/abase-runtime/python
PYTHONPATH=/opt/abase/abase-runtime/python python3 local_sdks/python/abase_proxy_client.py \
  --proxy={proxy} --namespace={ns} --table={table} --key=aws_proxy_smoke_key --value=aws_proxy_smoke_value
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
