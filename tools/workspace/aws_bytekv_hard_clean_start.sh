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

STOP_META="$(mktemp)"
cat > "$STOP_META" <<'JSON'
{
  "commands": [
    "set -eux\nsystemctl disable --now bytekv-master-onecluster bytekv-proxy-onecluster 2>/dev/null || true\npkill -9 -f '/opt/bytekv/bin/kvmaster|/opt/bytekv/bin/kvproxy|/opt/bytekv/bin/tso|bytekv_aws_smoke' 2>/dev/null || true\nsleep 2\nrm -rf /var/lib/bytekv/master-raft /var/lib/bytekv/tso-raft\nmkdir -p /var/lib/bytekv/master-raft /var/lib/bytekv/tso-raft /var/log/bytekv\nfind /var/log/bytekv -type f -name '*.log' -delete 2>/dev/null || true\npgrep -af 'kvmaster|kvproxy|tso|bytekv_aws_smoke' || true\ndf -hT /"
  ]
}
JSON

STOP_DATA="$(mktemp)"
cat > "$STOP_DATA" <<'JSON'
{
  "commands": [
    "set -eux\nsystemctl disable --now bytekv-partitionserver-onecluster 2>/dev/null || true\npkill -9 -f '/opt/bytekv/bin/partitionserver' 2>/dev/null || true\nsleep 2\nrm -rf /var/lib/bytekv/store1 /var/lib/bytekv/partitionserver /mnt/temporalstore-cache/bytekv\nmkdir -p /mnt/temporalstore-cache/bytekv/store1/data /mnt/temporalstore-cache/bytekv/store1/wal /mnt/temporalstore-cache/bytekv/store1/snapshot /mnt/temporalstore-cache/bytekv/partitionserver /var/log/bytekv\npython3 - <<'PY'\nimport json\np='/opt/bytekv/conf/partitionserver.json'\nwith open(p) as f: cfg=json.load(f)\ncfg['kv']['partition_size_mb']=1024\ncfg['kv']['working_dir']='/mnt/temporalstore-cache/bytekv/partitionserver'\nstore=cfg['stores'][0]\nstore['capacity_gb']=8\nstore['data_dir']='/mnt/temporalstore-cache/bytekv/store1/data'\nstore['wal_dir']='/mnt/temporalstore-cache/bytekv/store1/wal'\nstore['snapshot_dir']='/mnt/temporalstore-cache/bytekv/store1/snapshot'\nwith open(p,'w') as f: json.dump(cfg,f,indent=2)\nPY\nfind /var/log/bytekv -type f -name '*.log' -delete 2>/dev/null || true\npgrep -af partitionserver || true\ndf -hT / /mnt/temporalstore-cache\ncat /opt/bytekv/conf/partitionserver.json"
  ]
}
JSON

START_META="$(mktemp)"
cat > "$START_META" <<'JSON'
{
  "commands": [
    "set -eux\nnohup /opt/bytekv/bin/kvmaster --config=/opt/bytekv/conf/master.json --num_internal_table_shard=2 >/var/log/bytekv/kvmaster.nohup 2>&1 &\nsleep 5\nnohup /opt/bytekv/bin/tso --config=/opt/bytekv/conf/tso.json >/var/log/bytekv/tso.nohup 2>&1 &\nsleep 5\nnohup /opt/bytekv/bin/kvproxy --config=/opt/bytekv/conf/proxy.json >/var/log/bytekv/kvproxy.nohup 2>&1 &\nsleep 10\nps -ef | egrep 'kvmaster|kvproxy|tso' | grep -v egrep\nss -lntp | egrep '2601|2602|2603' || true\ntail -n 80 /var/log/bytekv/master.log || true"
  ]
}
JSON

START_DATA="$(mktemp)"
cat > "$START_DATA" <<'JSON'
{
  "commands": [
    "set -eux\nnohup /opt/bytekv/bin/partitionserver --config=/opt/bytekv/conf/partitionserver.json --machine_info_file=/opt/bytekv/conf/machine_info.json >/var/log/bytekv/partitionserver.nohup 2>&1 &\nsleep 12\nps -ef | egrep 'partitionserver' | grep -v egrep\nss -lntp | egrep '2604' || true\ntail -n 80 /var/log/bytekv/partitionserver.log || true"
  ]
}
JSON

send_wait_print "stop-meta" "$STOP_META" "$META_ID"
send_wait_print "stop-data" "$STOP_DATA" "$DATA01_ID" "$DATA02_ID"
send_wait_print "start-meta" "$START_META" "$META_ID"
send_wait_print "start-data" "$START_DATA" "$DATA01_ID" "$DATA02_ID"

rm -f "$STOP_META" "$STOP_DATA" "$START_META" "$START_DATA"
