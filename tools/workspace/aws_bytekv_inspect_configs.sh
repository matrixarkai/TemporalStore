#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-temporalstore}"
REGION="${AWS_REGION:-us-west-2}"
META="${META_INSTANCE_ID:-i-003c930417f7ee609}"
DATA01="${DATA01_INSTANCE_ID:-i-0724d90b323786546}"
DATA02="${DATA02_INSTANCE_ID:-i-096334bd8cc7ab259}"

COMMANDS=$(python3 - <<'PY'
import json
cmd = r'''
bash -lc '
set -euo pipefail
hostname
echo "--- master ---"; [ -f /opt/bytekv/conf/master.json ] && cat /opt/bytekv/conf/master.json || true
echo "--- tso ---"; [ -f /opt/bytekv/conf/tso.json ] && cat /opt/bytekv/conf/tso.json || true
echo "--- proxy ---"; [ -f /opt/bytekv/conf/proxy.json ] && cat /opt/bytekv/conf/proxy.json || true
echo "--- partitionserver ---"; [ -f /opt/bytekv/conf/partitionserver.json ] && cat /opt/bytekv/conf/partitionserver.json || true
echo "--- machine_info ---"; [ -f /opt/bytekv/conf/machine_info.json ] && cat /opt/bytekv/conf/machine_info.json || true
echo "--- bytekv processes ---"; pgrep -af "kvmaster|kvproxy|tso|partitionserver" || true
'
'''
print(json.dumps({"commands":[cmd]}))
PY
)

cmd_id=$(aws ssm send-command \
  --profile "$PROFILE" \
  --region "$REGION" \
  --instance-ids "$META" "$DATA01" "$DATA02" \
  --document-name AWS-RunShellScript \
  --parameters "$COMMANDS" \
  --query Command.CommandId \
  --output text)

for inst in "$META" "$DATA01" "$DATA02"; do
  aws ssm wait command-executed \
    --profile "$PROFILE" \
    --region "$REGION" \
    --command-id "$cmd_id" \
    --instance-id "$inst" || true
  echo "===== $inst ====="
  aws ssm get-command-invocation \
    --profile "$PROFILE" \
    --region "$REGION" \
    --command-id "$cmd_id" \
    --instance-id "$inst" \
    --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
    --output json
done
