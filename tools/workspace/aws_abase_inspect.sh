#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

IDS=(${IDS:-i-003c930417f7ee609 i-0724d90b323786546 i-096334bd8cc7ab259})

PARAMS="$(mktemp)"
python3 - <<'PY' > "$PARAMS"
import json

commands = [r'''
set +e
hostname
date -u
echo --- abase files
find /opt/abase -maxdepth 3 -type f 2>/dev/null | sed -n '1,120p'
echo --- processes
ps -ef | egrep 'abase|albase|redis-server|alchemy' | grep -v egrep
echo --- ports
ss -ltnp | egrep '190|180|6379|abase|redis'
echo --- logs
find /var/log /opt/abase -maxdepth 3 -type f \( -iname '*abase*' -o -iname '*.log' \) 2>/dev/null | sed -n '1,120p'
echo --- recent log tails
for f in $(find /var/log /opt/abase -maxdepth 3 -type f \( -iname '*abase*' -o -iname '*.log' \) 2>/dev/null | head -20); do
  echo "### $f"
  tail -40 "$f"
done
''']
print(json.dumps({"commands": commands}))
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
