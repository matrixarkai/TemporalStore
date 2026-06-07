#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

META_ID="${META_ID:-i-003c930417f7ee609}"
DATA01_ID="${DATA01_ID:-i-0724d90b323786546}"
DATA02_ID="${DATA02_ID:-i-096334bd8cc7ab259}"

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
    "set -eux\npkill -f '/opt/bytekv/bin/kvmaster|/opt/bytekv/bin/kvproxy|/opt/bytekv/bin/tso' 2>/dev/null || true\nmkdir -p /var/log/bytekv /var/lib/bytekv/master-raft /var/lib/bytekv/tso-raft\nnohup /opt/bytekv/bin/kvmaster --config=/opt/bytekv/conf/master.json >/var/log/bytekv/kvmaster.nohup 2>&1 &\nsleep 3\nnohup /opt/bytekv/bin/tso --config=/opt/bytekv/conf/tso.json >/var/log/bytekv/tso.nohup 2>&1 &\nsleep 3\nnohup /opt/bytekv/bin/kvproxy --config=/opt/bytekv/conf/proxy.json >/var/log/bytekv/kvproxy.nohup 2>&1 &\nsleep 8\nps -ef | egrep 'kvmaster|kvproxy|tso' | grep -v egrep\nss -lntp | egrep '2601|2602|2603' || true\ndf -hT /"
  ]
}
JSON

DATA_PARAMS="$(mktemp)"
cat > "$DATA_PARAMS" <<'JSON'
{
  "commands": [
    "set -eux\npkill -f '/opt/bytekv/bin/partitionserver' 2>/dev/null || true\nmkdir -p /var/log/bytekv /var/lib/bytekv/store1 /var/lib/bytekv/partitionserver\nnohup /opt/bytekv/bin/partitionserver --config=/opt/bytekv/conf/partitionserver.json --machine_info_file=/opt/bytekv/conf/machine_info.json >/var/log/bytekv/partitionserver.nohup 2>&1 &\nsleep 10\nps -ef | egrep 'partitionserver' | grep -v egrep\nss -lntp | egrep '2604' || true\ndf -hT /"
  ]
}
JSON

send_wait_print "meta" "$META_PARAMS" "$META_ID"
send_wait_print "data" "$DATA_PARAMS" "$DATA01_ID" "$DATA02_ID"

rm -f "$META_PARAMS" "$DATA_PARAMS"
