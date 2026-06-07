#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

INSTANCES=(
  i-003c930417f7ee609
  i-0724d90b323786546
  i-096334bd8cc7ab259
)

aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "${INSTANCES[@]}" \
  --parameters '{"commands":["sudo systemctl disable --now bytekv-master-onecluster bytekv-proxy-onecluster bytekv-partitionserver-onecluster abase-datanode-onecluster 2>/dev/null || true","systemctl --no-pager --full status bytekv-master-onecluster bytekv-proxy-onecluster bytekv-partitionserver-onecluster abase-datanode-onecluster 2>/dev/null | head -160 || true"]}' \
  --query 'Command.CommandId' \
  --output text
