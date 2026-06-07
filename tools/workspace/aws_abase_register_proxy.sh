#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${META_ID:=i-003c930417f7ee609}"
: "${NAMESPACE:=aws_scale}"
: "${TABLE:=bench}"

PARAMS="$(mktemp)"
NAMESPACE="$NAMESPACE" TABLE="$TABLE" python3 - <<'PY' > "$PARAMS"
import json
import os

ns = os.environ["NAMESPACE"]
table = os.environ["TABLE"]
script = f'''
set -e
call() {{
  local method="$1"
  local data="$2"
  echo "CALL $method $data"
  curl -sS -X POST "http://127.0.0.1:19074/MasterManageService/${{method}}" \
    -H 'Content-Type: application/json' \
    --data "$data"
  echo
}}

query() {{
  local method="$1"
  local data="${{2:-{{}}}}"
  echo "QUERY $method $data"
  curl -sS -X POST "http://127.0.0.1:19074/MasterQueryService/${{method}}" \
    -H 'Content-Type: application/json' \
    --data "$data"
  echo
}}

query CheckLeader '{{}}'
call AddProxy '{{"request_id":{{"cluster_name":"onecluster"}},"proxy_info":{{"proxy_addr":{{"ip":"10.70.1.79","port":19078}},"idc_name":"local","pod_name":"aws-meta","rack_name":"rack-meta","tag_name":"local","proxy_consul":"aws-scale-bench-proxy","proxy_namespace":"{ns}","proxy_table":"{table}","protocol_type":"ABASE2_THRIFT_PROTOCOL","proxy_type":"TYPE_NORMAL"}},"proxy_conf":{{"migration_read_type":"ONLY_READ_ABASE2","migration_write_type":"ONLY_WRITE_ABASE2","enable_gdpr":false,"ignore_gdpr":true,"acp_platform":"abase2"}}}}'
sleep 5
query ListProxy '{{}}'
curl -s http://127.0.0.1:19077/proxy_info/ || true
echo
ss -ltnp | grep 190 || true
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
