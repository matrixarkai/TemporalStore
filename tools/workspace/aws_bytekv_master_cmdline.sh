#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-temporalstore}"
REGION="${AWS_REGION:-us-west-2}"
META="${META_INSTANCE_ID:-i-003c930417f7ee609}"

COMMANDS=$(python3 - <<'PY'
import json
cmd = r'''
bash -lc '
set -eu
pid=$(pgrep -f "/opt/bytekv/bin/kvmaster" | head -1 || true)
echo "pid=${pid}"
if [ -n "$pid" ]; then
  tr "\000" " " < "/proc/$pid/cmdline"
  echo
fi
echo "--- nohup ---"
cat /var/log/bytekv/kvmaster.nohup 2>/dev/null || true
echo "--- master shard hints ---"
grep -aE "internal table|num_internal|Create internal|partition id:" /var/log/bytekv/master.log 2>/dev/null | tail -120 || true
'
'''
print(json.dumps({"commands":[cmd]}))
PY
)

cmd_id=$(aws ssm send-command \
  --profile "$PROFILE" \
  --region "$REGION" \
  --instance-ids "$META" \
  --document-name AWS-RunShellScript \
  --parameters "$COMMANDS" \
  --query Command.CommandId \
  --output text)

aws ssm wait command-executed \
  --profile "$PROFILE" \
  --region "$REGION" \
  --command-id "$cmd_id" \
  --instance-id "$META" || true

aws ssm get-command-invocation \
  --profile "$PROFILE" \
  --region "$REGION" \
  --command-id "$cmd_id" \
  --instance-id "$META" \
  --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
  --output json
