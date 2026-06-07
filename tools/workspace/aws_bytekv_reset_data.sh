#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

META_ID="${META_ID:-i-003c930417f7ee609}"
DATA01_ID="${DATA01_ID:-i-0724d90b323786546}"
DATA02_ID="${DATA02_ID:-i-096334bd8cc7ab259}"

run_group() {
  local ids=("$@")
  local params="$1"
}

send_wait_print() {
  local label="$1"; shift
  local params="$1"; shift
  local ids=("$@")
  local cmd_id
  cmd_id="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
    --document-name AWS-RunShellScript \
    --instance-ids "${ids[@]}" \
    --parameters "file://$params" \
    --query 'Command.CommandId' \
    --output text)"
  for id in "${ids[@]}"; do
    aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm wait command-executed --command-id "$cmd_id" --instance-id "$id" || true
    echo "===== $label $id ====="
    aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm get-command-invocation \
      --command-id "$cmd_id" \
      --instance-id "$id" \
      --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
      --output json
  done
}

META_PARAMS="$(mktemp)"
cat > "$META_PARAMS" <<'JSON'
{
  "commands": [
    "set -eux\nsystemctl disable --now bytekv-master-onecluster bytekv-proxy-onecluster 2>/dev/null || true\nsystemctl stop bytekv-master bytekv-proxy bytekv-tso 2>/dev/null || true\npkill -f '/opt/bytekv/bin/kvmaster|/opt/bytekv/bin/kvproxy|/opt/bytekv/bin/tso' 2>/dev/null || true\nrm -rf /var/lib/bytekv/master-raft /var/lib/bytekv/tso-raft\nmkdir -p /var/lib/bytekv/master-raft /var/lib/bytekv/tso-raft /var/log/bytekv\nfind /var/log/bytekv -type f -name '*.log' -delete 2>/dev/null || true\nsystemctl start bytekv-master bytekv-tso\nsleep 5\nsystemctl start bytekv-proxy\nsleep 5\nps -ef | egrep 'kvmaster|kvproxy|tso' | grep -v egrep\nss -lntp | egrep '2601|2602|2603' || true\ndf -hT /"
  ]
}
JSON

DATA_PARAMS="$(mktemp)"
cat > "$DATA_PARAMS" <<'JSON'
{
  "commands": [
    "set -eux\nsystemctl disable --now bytekv-partitionserver-onecluster 2>/dev/null || true\nsystemctl stop bytekv-partitionserver 2>/dev/null || true\npkill -f '/opt/bytekv/bin/partitionserver' 2>/dev/null || true\nrm -rf /var/lib/bytekv/store1 /var/lib/bytekv/partitionserver\nmkdir -p /var/lib/bytekv/store1 /var/lib/bytekv/partitionserver /var/log/bytekv\nfind /var/log/bytekv -type f -name '*.log' -delete 2>/dev/null || true\nsystemctl start bytekv-partitionserver\nsleep 8\nps -ef | egrep 'partitionserver' | grep -v egrep\nss -lntp | egrep '2604' || true\ndf -hT /"
  ]
}
JSON

send_wait_print "meta" "$META_PARAMS" "$META_ID"
send_wait_print "data" "$DATA_PARAMS" "$DATA01_ID" "$DATA02_ID"

rm -f "$META_PARAMS" "$DATA_PARAMS"
