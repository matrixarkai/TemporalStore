#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

DATA01_ID="${DATA01_ID:-i-0724d90b323786546}"
DATA02_ID="${DATA02_ID:-i-096334bd8cc7ab259}"
MASTER_URI="${MASTER_URI:-bytebase://10.70.1.79:19074}"

start_node() {
  local id="$1"
  local node_port="$2"
  local disk_port="$3"
  local disk_id="$4"
  local params
  params="$(mktemp)"
  NODE_PORT="$node_port" DISK_PORT="$disk_port" DISK_ID="$disk_id" MASTER_URI="$MASTER_URI" python3 - <<'PY' > "$params"
import json
import os

node_port = os.environ["NODE_PORT"]
disk_port = os.environ["DISK_PORT"]
disk_id = int(os.environ["DISK_ID"])
master_uri = os.environ["MASTER_URI"]

script = f'''
set -e
pkill -f '/opt/abase/abase-runtime/bin/abase-datanode' || true
mkdir -p /opt/abase/abase-runtime/conf /var/log/abase/datanode /mnt/temporalstore-cache/abase/disk{disk_id}
cat > /opt/abase/abase-runtime/conf/disk.conf <<'EOF'
{{
  "disk_list": [
    {{
      "disk_id": {disk_id},
      "disk_path": "/mnt/temporalstore-cache/abase/disk{disk_id}",
      "disk_port": {disk_port},
      "core_num": 1
    }}
  ]
}}
EOF
nohup /opt/abase/abase-runtime/bin/abase-datanode \
  --bytebase_datanode_port={node_port} \
  --bytebase_log_dir=/var/log/abase/datanode \
  --bytebase_log_name=datanode \
  --bytebase_master_uri={master_uri} \
  --bytebase_master_uri_v6={master_uri} \
  --bytebase_datanode_working_dir=/opt/abase/abase-runtime/conf \
  --bytebase_datanode_disk_conf_file=disk.conf \
  --bytebase_datanode_cluster=onecluster \
  --bytebase_datanode_verify_cluster=true \
  --bytebase_datanode_heartbeat_ms=1000 \
  --bytebase_datanode_heartbeat_timeout_ms=1000 \
  </dev/null >/var/log/abase/datanode/stdout.log 2>&1 &
sleep 3
echo PROCESS
ps -ef | egrep 'abase-datanode' | grep -v egrep || true
echo PORTS
ss -ltnp | egrep '{node_port}|{disk_port}' || true
echo LOG
tail -120 /var/log/abase/datanode/stdout.log || true
for f in $(find /var/log/abase/datanode -maxdepth 1 -type f -name 'datanode.log*' | sort | tail -2); do
  echo "### $f"
  tail -80 "$f"
done
'''
print(json.dumps({"commands": [script]}))
PY
  local cmd_id
  cmd_id="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
    --document-name AWS-RunShellScript \
    --instance-ids "$id" \
    --parameters "file://$params" \
    --query 'Command.CommandId' \
    --output text)"
  rm -f "$params"
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm wait command-executed --command-id "$cmd_id" --instance-id "$id" || true
  echo "===== $id ====="
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm get-command-invocation \
    --command-id "$cmd_id" \
    --instance-id "$id" \
    --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
    --output json
}

start_node "$DATA01_ID" 19078 19088 1
start_node "$DATA02_ID" 19079 19089 2
