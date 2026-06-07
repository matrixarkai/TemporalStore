#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

IDS=(${IDS:-i-0724d90b323786546 i-096334bd8cc7ab259})

PARAMS="$(mktemp)"
python3 - <<'PY' > "$PARAMS"
import json

print(json.dumps({"commands": [r'''
set +e
hostname
date -u
echo PROCESSES
ps -ef | egrep 'abase-datanode|abase-master|abase-proxy' | grep -v egrep
echo PORTS
ss -ltnp | egrep '1907|1908|1909|191|192|abase'
echo AB_LOG_DIRS
find /var/log/abase -maxdepth 3 -type f 2>/dev/null | sort
echo DATANODE_LOG_TAILS
for f in $(find /var/log/abase -maxdepth 3 -type f 2>/dev/null | grep -i datanode | sort | tail -5); do
  echo "### $f"
  tail -80 "$f"
done
echo OPT_ABASE
find /opt/abase -maxdepth 2 -type f 2>/dev/null | sort
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
