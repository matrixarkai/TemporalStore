#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
META_ID="${META_ID:-i-003c930417f7ee609}"

aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "$META_ID" \
  --parameters '{"commands":["set +e\npgrep -af bytekv_aws_smoke || true\npkill -f bytekv_aws_smoke || true\nsleep 1\npgrep -af bytekv_aws_smoke || true"]}' \
  --query 'Command.CommandId' \
  --output text
