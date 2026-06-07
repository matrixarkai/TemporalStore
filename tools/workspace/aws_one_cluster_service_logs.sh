#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"

META_ID="${META_ID:-i-003c930417f7ee609}"
DATA01_ID="${DATA01_ID:-i-0724d90b323786546}"
DATA02_ID="${DATA02_ID:-i-096334bd8cc7ab259}"

aws_cli() {
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" "$@"
}

run_ssm() {
  local name="$1"
  local ids="$2"
  local script="$3"
  local cmd_id
  local params_file
  params_file="$(mktemp)"
  SCRIPT_TEXT="$script" python3 - <<'PY' > "$params_file"
import json
import os

print(json.dumps({"commands": [os.environ["SCRIPT_TEXT"]]}))
PY
  cmd_id="$(aws_cli ssm send-command \
    --document-name AWS-RunShellScript \
    --instance-ids $ids \
    --parameters "file://$params_file" \
    --query 'Command.CommandId' \
    --output text)"
  rm -f "$params_file"
  aws_cli ssm wait command-executed --command-id "$cmd_id" --instance-id "${ids%% *}" || true
  for id in $ids; do
    echo "===== $name $id ====="
    aws_cli ssm get-command-invocation \
      --command-id "$cmd_id" \
      --instance-id "$id" \
      --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
      --output json
  done
}

run_ssm "meta" "$META_ID" '
set +e
hostname -f
date -u
echo "--- processes ---"
ps -ef | egrep "abase|bytekv|kvmaster|kvproxy|partitionserver|bcache2" | grep -v egrep
echo "--- listening ports ---"
ss -lntp | egrep "170|190|180|200|210|220|230|240|250|260|270|280|290" || true
echo "--- systemd onecluster ---"
systemctl --no-pager --full status abase-master-onecluster abase-proxy-onecluster bytekv-master-onecluster bytekv-proxy-onecluster 2>&1 || true
echo "--- bytekv onecluster unit files ---"
systemctl cat bytekv-master-onecluster bytekv-proxy-onecluster 2>&1 || true
echo "--- bytekv conf old/new ---"
ls -lah /opt/bytekv/conf /opt/bytekv-onecluster/conf 2>&1 || true
for f in /opt/bytekv/conf/*.json /opt/bytekv-onecluster/conf/*.json; do echo "### $f"; sed -n "1,220p" "$f"; done 2>&1 || true
echo "--- bytekv logs ---"
find /var/log/bytekv /opt/bytekv /opt/bytekv-onecluster -maxdepth 3 -type f \( -name "*.log" -o -name "*.err" -o -name "*.out" \) -print 2>/dev/null | head -40 | while read f; do echo "### $f"; tail -n 120 "$f"; done
echo "--- abase logs ---"
find /var/log/abase /opt/abase-onecluster -maxdepth 4 -type f \( -name "*.log" -o -name "*.err" -o -name "*.out" \) -print 2>/dev/null | head -40 | while read f; do echo "### $f"; tail -n 120 "$f"; done
'

run_ssm "data" "$DATA01_ID $DATA02_ID" '
set +e
hostname -f
date -u
echo "--- processes ---"
ps -ef | egrep "abase|bytekv|partitionserver|bcache2" | grep -v egrep
echo "--- listening ports ---"
ss -lntp | egrep "170|190|180|200|210|220|230|240|250|260|270|280|290" || true
echo "--- systemd onecluster ---"
systemctl --no-pager --full status abase-datanode-onecluster bytekv-partitionserver-onecluster 2>&1 || true
echo "--- unit files ---"
systemctl cat abase-datanode-onecluster bytekv-partitionserver-onecluster 2>&1 || true
echo "--- bytekv conf old/new ---"
ls -lah /opt/bytekv/conf /opt/bytekv-onecluster/conf 2>&1 || true
for f in /opt/bytekv/conf/*.json /opt/bytekv-onecluster/conf/*.json; do echo "### $f"; sed -n "1,240p" "$f"; done 2>&1 || true
echo "--- bytekv logs ---"
find /var/log/bytekv /opt/bytekv /opt/bytekv-onecluster -maxdepth 3 -type f \( -name "*.log" -o -name "*.err" -o -name "*.out" \) -print 2>/dev/null | head -50 | while read f; do echo "### $f"; tail -n 160 "$f"; done
echo "--- abase conf/logs ---"
ls -lah /opt/abase-onecluster /opt/abase-onecluster/conf /var/log/abase 2>&1 || true
find /opt/abase-onecluster/conf -type f -maxdepth 2 -print -exec sed -n "1,220p" {} \; 2>&1 || true
find /var/log/abase /opt/abase-onecluster -maxdepth 4 -type f \( -name "*.log" -o -name "*.err" -o -name "*.out" \) -print 2>/dev/null | head -60 | while read f; do echo "### $f"; tail -n 180 "$f"; done
'
