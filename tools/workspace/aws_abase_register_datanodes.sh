#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

META_ID="${META_ID:-i-003c930417f7ee609}"

PARAMS="$(mktemp)"
python3 - <<'PY' > "$PARAMS"
import json

script = r'''
set -e
call() {
  local method="$1"
  local data="$2"
  echo "CALL $method $data"
  curl -sS -X POST "http://127.0.0.1:19074/MasterManageService/${method}" \
    -H 'Content-Type: application/json' \
    --data "$data"
  echo
}

query() {
  local method="$1"
  local data="${2:-\{\}}"
  echo "QUERY $method $data"
  curl -sS -X POST "http://127.0.0.1:19074/MasterQueryService/${method}" \
    -H 'Content-Type: application/json' \
    --data "$data"
  echo
}

query CheckLeader '{}'
call AddDataNode '{"request_id":{"cluster_name":"onecluster"},"datanode_info":{"datanode_addr":{"ip":"10.70.1.163","port":19078},"idc_name":"local","pod_name":"aws-data01","rack_name":"rack-a","tag_name":"local"}}'
call AddDisk '{"request_id":{"cluster_name":"onecluster"},"disk_info":{"datanode_addr":{"ip":"10.70.1.163","port":19078},"disk_id":1,"disk_port":19088,"core_num":1}}'
call AddDataNode '{"request_id":{"cluster_name":"onecluster"},"datanode_info":{"datanode_addr":{"ip":"10.70.1.202","port":19079},"idc_name":"local","pod_name":"aws-data02","rack_name":"rack-b","tag_name":"local"}}'
call AddDisk '{"request_id":{"cluster_name":"onecluster"},"disk_info":{"datanode_addr":{"ip":"10.70.1.202","port":19079},"disk_id":2,"disk_port":19089,"core_num":1}}'
sleep 3
query ListDataNode '{}'
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
