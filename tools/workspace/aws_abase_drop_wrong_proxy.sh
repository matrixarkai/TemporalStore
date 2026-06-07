#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${META_ID:=i-003c930417f7ee609}"

PARAMS="$(mktemp)"
python3 - <<'PY' > "$PARAMS"
import json
script = r'''
set -e
curl -sS -X POST "http://127.0.0.1:19074/MasterManageService/DropProxy" \
  -H 'Content-Type: application/json' \
  --data '{"request_id":{"cluster_name":"onecluster"},"proxy_info":{"proxy_addr":{"ip":"10.70.1.79","port":19077}}}'
echo
curl -sS -X POST "http://127.0.0.1:19074/MasterQueryService/ListProxy" \
  -H 'Content-Type: application/json' \
  --data '{}'
echo
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
