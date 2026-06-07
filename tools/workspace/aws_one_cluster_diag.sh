#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-temporalstore}"
REGION="${AWS_REGION:-us-west-2}"

run_ssm() {
  local name="$1"
  local instance_id="$2"
  local command="$3"
  local payload
  payload="$(mktemp)"
  COMMAND_PAYLOAD="$(printf "cat >/tmp/onecluster_diag.sh <<'ONECLUSTER_DIAG_EOF'\n%s\nONECLUSTER_DIAG_EOF\nbash /tmp/onecluster_diag.sh" "$command")" \
    python3 - <<'PY' >"$payload"
import json
import os
print(json.dumps({"commands": [os.environ["COMMAND_PAYLOAD"]], "executionTimeout": ["900"]}))
PY
  local command_id
  command_id="$(aws --profile "$PROFILE" --region "$REGION" ssm send-command \
    --instance-ids "$instance_id" \
    --document-name AWS-RunShellScript \
    --parameters "file://${payload}" \
    --query 'Command.CommandId' \
    --output text)"
  rm -f "$payload"
  echo "=== ${name} ${instance_id} ${command_id} ==="
  for _ in $(seq 1 90); do
    local status
    status="$(aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
      --command-id "$command_id" \
      --instance-id "$instance_id" \
      --query 'Status' \
      --output text 2>/dev/null || true)"
    case "$status" in
      Success|Failed|Cancelled|TimedOut)
        break
        ;;
    esac
    sleep 2
  done
  aws --profile "$PROFILE" --region "$REGION" ssm get-command-invocation \
    --command-id "$command_id" \
    --instance-id "$instance_id" \
    --query '{status:Status,stdout:StandardOutputContent,stderr:StandardErrorContent}' \
    --output json
}

DIAG_CMD='
set -x
hostname
systemctl --no-pager --full status \
  abase-datanode-onecluster \
  abase-master-onecluster \
  abase-proxy-onecluster \
  bytekv-master-onecluster \
  bytekv-proxy-onecluster \
  bytekv-partitionserver-onecluster 2>/dev/null || true
pgrep -af "abase|kvmaster|kvproxy|partitionserver" || true
ss -lntp | grep -E ":(19043|19074|19077|6510|3742|3743|3744)" || true
journalctl -u abase-datanode-onecluster -n 80 --no-pager 2>/dev/null || true
journalctl -u bytekv-master-onecluster -n 80 --no-pager 2>/dev/null || true
journalctl -u bytekv-proxy-onecluster -n 80 --no-pager 2>/dev/null || true
journalctl -u bytekv-partitionserver-onecluster -n 80 --no-pager 2>/dev/null || true
ls -l /opt/abase/abase-runtime/bin /opt/bytekv/bytekv-runtime-release/bin 2>/dev/null || true
'

run_ssm meta i-003c930417f7ee609 "$DIAG_CMD"
run_ssm data01 i-0724d90b323786546 "$DIAG_CMD"
run_ssm data02 i-096334bd8cc7ab259 "$DIAG_CMD"
