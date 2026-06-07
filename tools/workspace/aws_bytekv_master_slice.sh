#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
META_ID="${META_ID:-i-003c930417f7ee609}"
START_LINE="${START_LINE:-140}"
END_LINE="${END_LINE:-260}"

PARAMS="$(mktemp)"
cat > "$PARAMS" <<JSON
{
  "commands": [
    "set +e\nhostname -f\ndate -u\nnl -ba /var/log/bytekv/master.log | sed -n '${START_LINE},${END_LINE}p'\n"
  ]
}
JSON

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
  --query 'StandardOutputContent' \
  --output text
