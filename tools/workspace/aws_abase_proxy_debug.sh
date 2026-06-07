#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${META_ID:=i-003c930417f7ee609}"

PARAMS="$(mktemp)"
python3 - <<'PY' > "$PARAMS"
import json
script = r'''
set +e
echo "== process =="
ps -ef | grep abase-proxy | grep -v grep
echo
echo "== listeners =="
ss -ltnp | grep 190
echo
echo "== proxy info endpoint =="
curl -s http://127.0.0.1:19077/proxy_info/ || true
echo
echo "== logs =="
find /opt/abase/abase-runtime -maxdepth 3 -type f \( -iname '*proxy*' -o -iname '*.log' \) -printf '%TY-%Tm-%Td %TH:%TM %p\n' 2>/dev/null | sort | tail -20
echo
for f in $(find /opt/abase/abase-runtime -maxdepth 3 -type f \( -iname '*proxy*' -o -iname '*.log' \) 2>/dev/null | sort | tail -8); do
  echo "---- $f ----"
  tail -80 "$f"
done
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
