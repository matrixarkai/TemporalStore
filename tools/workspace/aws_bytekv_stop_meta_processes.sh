#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-temporalstore}"
REGION="${AWS_REGION:-us-west-2}"
META="${META_INSTANCE_ID:-i-003c930417f7ee609}"

PARAMS=$(mktemp)
cat > "$PARAMS" <<'JSON'
{
  "commands": [
    "bash -lc 'pkill -9 -f /opt/bytekv/bin/kvmaster || true; pkill -9 -f /opt/bytekv/bin/kvproxy || true; pkill -9 -f /opt/bytekv/bin/tso || true; pkill -9 -f bytekv_aws_smoke || true; sleep 2; pgrep -af \"kvmaster|kvproxy|tso|bytekv_aws_smoke\" || true'"
  ]
}
JSON

cmd_id=$(aws ssm send-command \
  --profile "$PROFILE" \
  --region "$REGION" \
  --document-name AWS-RunShellScript \
  --instance-ids "$META" \
  --parameters "file://$PARAMS" \
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

rm -f "$PARAMS"
