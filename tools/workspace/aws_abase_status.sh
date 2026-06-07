#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

META_ID="${META_ID:-i-003c930417f7ee609}"
IDS=(${IDS:-i-003c930417f7ee609 i-0724d90b323786546 i-096334bd8cc7ab259})

PARAMS="$(mktemp)"
python3 - <<'PY' > "$PARAMS"
import json

print(json.dumps({"commands": [r'''
set +e
hostname
date -u
echo PROCESSES
ps -ef | egrep 'abase-master|abase-proxy|abase-datanode' | grep -v egrep
echo PORTS
ss -ltnp | egrep '1907|1908|1909'
if hostname | grep -q '10-70-1-79'; then
  echo MASTER_LIST_DATANODE
  curl -sS -X POST 'http://127.0.0.1:19074/MasterQueryService/ListDataNode' \
    -H 'Content-Type: application/json' --data '{}'
  echo
fi
echo DATANODE_RECENT_WARNINGS
grep -R "not success\|datanode not exist\|startup success\|Heartbeat" /var/log/abase/datanode 2>/dev/null | tail -40
''']}))
PY

CMD_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "${IDS[@]}" \
  --parameters "file://$PARAMS" \
  --query 'Command.CommandId' \
  --output text)"
rm -f "$PARAMS"

for id in "${IDS[@]}"; do
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm wait command-executed --command-id "$CMD_ID" --instance-id "$id" || true
  echo "===== $id ====="
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm get-command-invocation \
    --command-id "$CMD_ID" \
    --instance-id "$id" \
    --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
    --output json
done
