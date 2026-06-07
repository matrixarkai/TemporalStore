#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
META_ID="${META_ID:-i-003c930417f7ee609}"

PARAMS="$(mktemp)"
python3 - <<'PY' > "$PARAMS"
import json

print(json.dumps({"commands": [r'''
set -e
find /opt/abase/abase-runtime -path '*abase_proxy_client.py' -o -path '*abase_direct_client.py'
for f in $(find /opt/abase/abase-runtime -path '*abase_proxy_client.py' -o -path '*abase_direct_client.py'); do
  echo "### $f"
  sed -n '1,260p' "$f"
done
''']}))
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
