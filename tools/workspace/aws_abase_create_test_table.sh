#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${TABLE_NAME:=aws_scale/bench}"

META_ID="${META_ID:-i-003c930417f7ee609}"

PARAMS="$(mktemp)"
TABLE_NAME="$TABLE_NAME" python3 - <<'PY' > "$PARAMS"
import json
import os

table = os.environ["TABLE_NAME"]
req = {
    "request_id": {"cluster_name": "onecluster"},
    "table_info": {
        "table_name": table,
        "replica_num": 1,
        "partition_num": 2,
        "location_policy": {"idc_list": ["local"], "tag": "local"},
        "replica_log_max_hold_size": 134217728,
        "replica_log_deprecate_delay_s": 30,
        "replica_log_destroy_delay_s": 30,
        "cache_not_found": True,
        "engine_info": {"engine_type": "ENGINE_TYPE_ROCKSDB"},
        "hash_info": {"enable": True},
    },
    "unlimited_quota": True,
}

script = f'''
set -e
echo CREATE_TABLE {table}
curl -sS -X POST 'http://127.0.0.1:19074/MasterManageService/CreateTable' \
  -H 'Content-Type: application/json' \
  --data {json.dumps(json.dumps(req))}
echo
sleep 10
echo LIST_TABLE
curl -sS -X POST 'http://127.0.0.1:19074/MasterQueryService/ListTable' \
  -H 'Content-Type: application/json' \
  --data '{{}}'
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
