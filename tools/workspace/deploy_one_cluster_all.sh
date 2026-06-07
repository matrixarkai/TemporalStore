#!/usr/bin/env bash
set -euo pipefail

# Deploy ABase and ByteKV onto the existing TemporalStore AWS test cluster.
# This script intentionally does not create or destroy AWS resources.

PROFILE="${AWS_PROFILE:-temporalstore}"
REGION="${AWS_REGION:-us-west-2}"
NAME_PREFIX="${TEMPORALSTORE_NAME_PREFIX:-temporalstore-test}"
BUCKET="${ARTIFACT_BUCKET:-temporalstore-test-artifacts-657817560042-us-west-2}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

ROOT_WIN="/mnt/c/Users/Vincent Jiang/Documents/Codex/2026-05-10/bytekv-in-local-vs-etcd"
ABASE_ARTIFACT="${ABASE_ARTIFACT:-${ROOT_WIN}/outputs/abase-runtime-stripped.tar.gz}"
BYTEKV_ARTIFACT="${BYTEKV_ARTIFACT:-${ROOT_WIN}/bytekv-runtime-release.tar.gz}"

ABASE_KEY="one-cluster/${RUN_ID}/abase-runtime-stripped.tar.gz"
BYTEKV_KEY="one-cluster/${RUN_ID}/bytekv-runtime-release.tar.gz"

ABASE_MASTER_PORT=19074
ABASE_PROXY_PORT=19077
ABASE_PROXY_THRIFT_PORT=19078
ABASE_DATANODE_PORT=19043

BYTEKV_MASTER_PORT=6510
BYTEKV_PROXY_PORT=3742
BYTEKV_PS_BASE_PORT=3743

log() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "missing file: $1" >&2
    exit 1
  fi
}

aws_cli() {
  aws --profile "$PROFILE" --region "$REGION" "$@"
}

check_auth() {
  log "checking AWS SSO auth"
  aws_cli sts get-caller-identity >/dev/null
}

instance_id_by_name() {
  local name="$1"
  aws_cli ec2 describe-instances \
    --filters "Name=tag:Name,Values=${name}" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].InstanceId' \
    --output text
}

private_ip_by_id() {
  local id="$1"
  aws_cli ec2 describe-instances \
    --instance-ids "$id" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' \
    --output text
}

discover_cluster() {
  META_ID="$(instance_id_by_name "${NAME_PREFIX}-meta-01")"
  DATA1_ID="$(instance_id_by_name "${NAME_PREFIX}-data-01")"
  DATA2_ID="$(instance_id_by_name "${NAME_PREFIX}-data-02")"

  if [[ -z "$META_ID" || "$META_ID" == "None" ]]; then
    echo "missing running instance ${NAME_PREFIX}-meta-01" >&2
    exit 1
  fi
  if [[ -z "$DATA1_ID" || "$DATA1_ID" == "None" ]]; then
    echo "missing running instance ${NAME_PREFIX}-data-01" >&2
    exit 1
  fi
  if [[ -z "$DATA2_ID" || "$DATA2_ID" == "None" ]]; then
    echo "missing running instance ${NAME_PREFIX}-data-02" >&2
    exit 1
  fi

  META_IP="$(private_ip_by_id "$META_ID")"
  DATA1_IP="$(private_ip_by_id "$DATA1_ID")"
  DATA2_IP="$(private_ip_by_id "$DATA2_ID")"

  log "existing cluster:"
  log "  meta-01 ${META_ID} ${META_IP}"
  log "  data-01 ${DATA1_ID} ${DATA1_IP}"
  log "  data-02 ${DATA2_ID} ${DATA2_IP}"
}

upload_artifacts() {
  require_file "$ABASE_ARTIFACT"
  require_file "$BYTEKV_ARTIFACT"
  log "uploading ABase artifact to s3://${BUCKET}/${ABASE_KEY}"
  aws_cli s3 cp "$ABASE_ARTIFACT" "s3://${BUCKET}/${ABASE_KEY}"
  log "uploading ByteKV artifact to s3://${BUCKET}/${BYTEKV_KEY}"
  aws_cli s3 cp "$BYTEKV_ARTIFACT" "s3://${BUCKET}/${BYTEKV_KEY}"
  ABASE_URL="$(aws_cli s3 presign "s3://${BUCKET}/${ABASE_KEY}" --expires-in 7200)"
  BYTEKV_URL="$(aws_cli s3 presign "s3://${BUCKET}/${BYTEKV_KEY}" --expires-in 7200)"
}

ssm_run() {
  local name="$1"
  local instance_id="$2"
  local command="$3"
  local wrapped_command
  wrapped_command="$(printf "cat >/tmp/onecluster_cmd.sh <<'ONECLUSTER_EOF'\n%s\nONECLUSTER_EOF\nbash /tmp/onecluster_cmd.sh" "$command")"
  local payload
  payload="$(mktemp)"
  COMMAND_PAYLOAD="$wrapped_command" python3 - <<'PY' >"$payload"
import json
import os
print(json.dumps({"commands": [os.environ["COMMAND_PAYLOAD"]], "executionTimeout": ["3600"]}))
PY
  local command_id
  command_id="$(aws_cli ssm send-command \
    --instance-ids "$instance_id" \
    --document-name AWS-RunShellScript \
    --parameters "file://${payload}" \
    --query 'Command.CommandId' \
    --output text)"
  rm -f "$payload"
  log "sent ${name} command ${command_id} to ${instance_id}"

  for _ in $(seq 1 180); do
    local status
    status="$(aws_cli ssm get-command-invocation \
      --command-id "$command_id" \
      --instance-id "$instance_id" \
      --query 'Status' \
      --output text 2>/dev/null || true)"
    case "$status" in
      Success)
        aws_cli ssm get-command-invocation \
          --command-id "$command_id" \
          --instance-id "$instance_id" \
          --query '{stdout:StandardOutputContent,stderr:StandardErrorContent}' \
          --output json
        return 0
        ;;
      Failed|Cancelled|TimedOut|Cancelling)
        aws_cli ssm get-command-invocation \
          --command-id "$command_id" \
          --instance-id "$instance_id" \
          --query '{status:Status,stdout:StandardOutputContent,stderr:StandardErrorContent}' \
          --output json
        return 1
        ;;
    esac
    sleep 5
  done
  echo "SSM command ${command_id} on ${instance_id} did not finish" >&2
  return 1
}

common_install_cmd() {
  cat <<EOF
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y awscli curl gzip htop jq libev4 libgflags2.2 libleveldb1d liblz4-1 libprotobuf23 libsnappy1v5 libthrift-0.16.0 tar zlib1g
mkdir -p /opt/abase /opt/bytekv /var/log/abase /var/log/bytekv /var/lib/abase /var/lib/bytekv
curl -fL '${ABASE_URL}' -o /tmp/abase-runtime.tar.gz
curl -fL '${BYTEKV_URL}' -o /tmp/bytekv-runtime.tar.gz
rm -rf /opt/abase/abase-runtime /opt/bytekv/bytekv-runtime-release
tar -C /opt/abase -xzf /tmp/abase-runtime.tar.gz
tar -C /opt/bytekv -xzf /tmp/bytekv-runtime.tar.gz
chmod +x /opt/abase/abase-runtime/bin/* /opt/bytekv/bytekv-runtime-release/bin/* || true
EOF
}

start_abase_meta_cmd() {
  cat <<EOF
$(common_install_cmd)
pkill -f '/opt/abase/abase-runtime/bin/abase-(master|proxy)' || true
rm -rf /var/lib/abase/master /var/lib/abase/proxy
mkdir -p /var/lib/abase/master/wal /var/lib/abase/master/snapshot /var/lib/abase/proxy/meta /var/log/abase/master /var/log/abase/proxy
cat >/etc/systemd/system/abase-master-onecluster.service <<'UNIT'
[Unit]
Description=ABase master on TemporalStore cluster
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
Environment=BYTEBASE_MOCK_IPV6_ONLY=off
ExecStart=/opt/abase/abase-runtime/bin/abase-master --bytebase_master_raft_id=1 --bytebase_master_scheduler_pause_balance_replica=true --bytebase_master_verify_cluster=true --bytebase_master_cluster=onecluster --bytebase_master_raft_peers=1,${META_IP}:19084,${META_IP}:19094,0 --bytebase_master_raft_wal_dir=/var/lib/abase/master/wal --bytebase_master_raft_snapshot_dir=/var/lib/abase/master/snapshot --bytebase_master_server_port=${ABASE_MASTER_PORT} --bytebase_log_dir=/var/log/abase/master --bytebase_log_name=master --bytebase_logtostderr=false --bytebase_master_cmdb_enabled=false --bytebase_master_max_replica_in_same_ip=0 --bytebase_master_public_consul_list=onebox1:bytedance.abase2.onebox --bytebase_master_proxy_restricted_vdc_list=onebox1,onebox2,onebox3,local --bytebase_master_datanode_restricted_vdc_list=onebox1,onebox2,onebox3,local --bytebase_master_idc_region_mapping=onebox1:onebox,onebox2:onebox,onebox3:onebox,local:local
Restart=always
RestartSec=5
LimitNOFILE=1048576
WorkingDirectory=/opt/abase/abase-runtime
[Install]
WantedBy=multi-user.target
UNIT
cat >/etc/systemd/system/abase-proxy-onecluster.service <<'UNIT'
[Unit]
Description=ABase proxy on TemporalStore cluster
After=network-online.target abase-master-onecluster.service
Wants=network-online.target
[Service]
Type=simple
Environment=BYTEBASE_MOCK_IPV6_ONLY=off
ExecStart=/opt/abase/abase-runtime/bin/abase-proxy --bytebase_proxy_port=${ABASE_PROXY_PORT} --bytebase_log_dir=/var/log/abase/proxy --bytebase_log_name=proxy --bytebase_meta_sync_file_dir=/var/lib/abase/proxy/meta --bytebase_proxy_master_addr=bytebase://${META_IP}:${ABASE_MASTER_PORT} --bytebase_proxy_master_addr_v6=bytebase://[${META_IP}]:${ABASE_MASTER_PORT} --bytebase_default_timeout_ms=1000 --bytebase_proxy_heartbeat_interval_s=1 --metasync_grab_interval_s=1 --bytebase_proxy_heartbeat_report_interval=1
Restart=always
RestartSec=5
LimitNOFILE=1048576
WorkingDirectory=/opt/abase/abase-runtime
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now abase-master-onecluster
sleep 5
systemctl enable --now abase-proxy-onecluster
systemctl --no-pager --full status abase-master-onecluster abase-proxy-onecluster || true
EOF
}

start_abase_data_cmd() {
  local node_name="$1"
  cat <<EOF
$(common_install_cmd)
pkill -f '/opt/abase/abase-runtime/bin/abase-datanode' || true
rm -rf /var/lib/abase/${node_name}
mkdir -p /var/lib/abase/${node_name}/disk0 /var/log/abase/datanode
cat >/etc/systemd/system/abase-datanode-onecluster.service <<'UNIT'
[Unit]
Description=ABase datanode on TemporalStore cluster
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
Environment=BYTEBASE_MOCK_IPV6_ONLY=off
ExecStart=/opt/abase/abase-runtime/bin/abase-datanode --bytebase_datanode_port=${ABASE_DATANODE_PORT} --bytebase_datanode_verify_cluster=true --bytebase_hlc_enable_clock_check=false --bytebase_datanode_cluster=onecluster --bytebase_master_uri=bytebase://${META_IP}:${ABASE_MASTER_PORT} --bytebase_master_uri_v6=bytebase://[${META_IP}]:${ABASE_MASTER_PORT} --bytebase_log_dir=/var/log/abase/datanode --bytebase_log_name=datanode --bytebase_datanode_enable_rocksdb_perf=true --bytebase_datanode_heartbeat_ms=100 --bytebase_datanode_gossip_interval_ms=500 --bytebase_datanode_enable_consistency_check=true
Restart=always
RestartSec=5
LimitNOFILE=1048576
WorkingDirectory=/opt/abase/abase-runtime
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now abase-datanode-onecluster
systemctl --no-pager --full status abase-datanode-onecluster || true
EOF
}

start_bytekv_meta_cmd() {
  cat <<EOF
$(common_install_cmd)
pkill -f '/opt/bytekv/bytekv-runtime-release/bin/(kvmaster|kvproxy)' || true
mkdir -p /opt/bytekv/conf /var/lib/bytekv/master-raft /var/log/bytekv
cat >/opt/bytekv/conf/master-onecluster.json <<JSON
{
  "cluster_id": 1,
  "cluster_name": "bytekv_onecluster",
  "masters": ["1:127.0.0.1:${BYTEKV_MASTER_PORT}"],
  "port": ${BYTEKV_MASTER_PORT},
  "service_thread_num": 4,
  "tso_thread_num": 4,
  "ns_thread_num": 4,
  "report_thread_num": 4,
  "location_cache_thread_num": 4,
  "table": {"default_replica_count": 2, "default_security_level": "server"},
  "internal_table": {"default_replica_count": 1, "default_security_level": "server"},
  "raft": {"data_dir": "/var/lib/bytekv/master-raft", "election_cycle_tick": 3, "wal_sync": true, "max_segment_bytes": 67108864},
  "log": {"filename": "/var/log/bytekv/master.log", "level": "info", "rotate_type": "size", "file_size_in_mb": 100, "keep_file_num": 20},
  "metrics": {"counter_window_size": 30, "enable_logging": false, "report_interval_secs": 30, "server_ip": "127.0.0.1", "server_port": 9123}
}
JSON
cat >/opt/bytekv/conf/proxy-onecluster.json <<JSON
{
  "cluster_id": 1,
  "cluster_name": "bytekv_onecluster",
  "port": ${BYTEKV_PROXY_PORT},
  "service_thread_num": 4,
  "client": {"masters": ["127.0.0.1:${BYTEKV_MASTER_PORT}"], "tsos": ["127.0.0.1:${BYTEKV_MASTER_PORT}"], "require_consistent_timestamp": false},
  "auth": {"auth_enabled": false, "auth_required": false, "audit_log_file": "/var/log/bytekv/proxy-audit.log"},
  "log": {"filename": "/var/log/bytekv/proxy.log", "level": "info", "rotate_type": "size", "file_size_in_mb": 100, "keep_file_num": 20},
  "metrics": {"counter_window_size": 30, "enable_logging": false, "report_interval_secs": 30, "server_ip": "127.0.0.1", "server_port": 9123}
}
JSON
cat >/etc/systemd/system/bytekv-master-onecluster.service <<'UNIT'
[Unit]
Description=ByteKV master on TemporalStore cluster
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
ExecStart=/opt/bytekv/bytekv-runtime-release/bin/kvmaster --config=/opt/bytekv/conf/master-onecluster.json
Restart=always
RestartSec=5
LimitNOFILE=1048576
WorkingDirectory=/opt/bytekv/bytekv-runtime-release
[Install]
WantedBy=multi-user.target
UNIT
cat >/etc/systemd/system/bytekv-proxy-onecluster.service <<'UNIT'
[Unit]
Description=ByteKV proxy on TemporalStore cluster
After=network-online.target bytekv-master-onecluster.service
Wants=network-online.target
[Service]
Type=simple
ExecStart=/opt/bytekv/bytekv-runtime-release/bin/kvproxy --config=/opt/bytekv/conf/proxy-onecluster.json
Restart=always
RestartSec=5
LimitNOFILE=1048576
WorkingDirectory=/opt/bytekv/bytekv-runtime-release
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now bytekv-master-onecluster
sleep 5
systemctl enable --now bytekv-proxy-onecluster
systemctl --no-pager --full status bytekv-master-onecluster bytekv-proxy-onecluster || true
EOF
}

start_bytekv_data_cmd() {
  local store_id="$1"
  local port="$2"
  cat <<EOF
$(common_install_cmd)
pkill -f '/opt/bytekv/bytekv-runtime-release/bin/partitionserver' || true
mkdir -p /opt/bytekv/conf /var/log/bytekv /var/lib/bytekv/store-${store_id}/data /var/lib/bytekv/store-${store_id}/wal /var/lib/bytekv/store-${store_id}/snapshot
cat >/opt/bytekv/conf/partitionserver-onecluster.json <<JSON
{
  "kv": {
    "cluster_id": 1,
    "cluster_name": "bytekv_onecluster",
    "masters": ["${META_IP}:${BYTEKV_MASTER_PORT}"],
    "tsos": ["${META_IP}:${BYTEKV_MASTER_PORT}"],
    "partition_size_mb": 1,
    "service.peer_port": ${port},
    "service_thread_num": 4,
    "working_dir": "/var/lib/bytekv",
    "wal_type": "file",
    "engine_type": "rocksdb"
  },
  "raft": {"election_cycle_tick": 3, "wal_sync": false, "max_segment_bytes": 67108864},
  "raft_storage": {"block_cache_size_mb": 128, "block_size_kb": 32, "compression": true, "max_background_jobs": 2},
  "rocksdb": {"block_cache_size_mb": 128, "block_size_kb": 32, "compression": true, "max_background_jobs": 2},
  "stores": [{
    "capacity_gb": 10,
    "data_dir": "/var/lib/bytekv/store-${store_id}/data",
    "dc_tag": "vdc1",
    "disk_read_limit_mb": 300,
    "disk_write_limit_mb": 150,
    "bg_rate_limit_mb": 150,
    "enable": true,
    "local_id": ${store_id},
    "rack_tag": "rack1",
    "snapshot_concurrency": 2,
    "snapshot_dir": "/var/lib/bytekv/store-${store_id}/snapshot",
    "wal_dir": "/var/lib/bytekv/store-${store_id}/wal"
  }],
  "log": {"filename": "/var/log/bytekv/ps.log", "level": "info", "rotate_type": "time", "file_size_in_mb": 100, "keep_file_num": 20},
  "metrics": {"counter_window_size": 15, "enable_logging": false, "prefix": "onecluster", "report_interval_secs": 15, "server_ip": "127.0.0.1", "server_port": 9123}
}
JSON
cat >/etc/systemd/system/bytekv-partitionserver-onecluster.service <<'UNIT'
[Unit]
Description=ByteKV partitionserver on TemporalStore cluster
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
ExecStart=/opt/bytekv/bytekv-runtime-release/bin/partitionserver --config=/opt/bytekv/conf/partitionserver-onecluster.json
Restart=always
RestartSec=5
LimitNOFILE=1048576
WorkingDirectory=/opt/bytekv/bytekv-runtime-release
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now bytekv-partitionserver-onecluster
systemctl --no-pager --full status bytekv-partitionserver-onecluster || true
EOF
}

combined_test_cmd() {
  cat <<EOF
set -euxo pipefail
echo "=== TemporalStore ==="
systemctl --no-pager --full status temporalstore-metaserver temporalstore-proxy 2>/dev/null || true
pgrep -af 'bcache2|temporalstore|metaserver|proxy' || true
echo "=== ABase ==="
systemctl --no-pager --full status abase-master-onecluster abase-proxy-onecluster 2>/dev/null || true
pgrep -af 'abase-(master|proxy|datanode)' || true
echo "=== ByteKV ==="
systemctl --no-pager --full status bytekv-master-onecluster bytekv-proxy-onecluster bytekv-partitionserver-onecluster 2>/dev/null || true
pgrep -af 'kvmaster|kvproxy|partitionserver' || true
echo "=== Listening ports ==="
ss -lntp | grep -E ':(${ABASE_MASTER_PORT}|${ABASE_PROXY_PORT}|${ABASE_PROXY_THRIFT_PORT}|${ABASE_DATANODE_PORT}|${BYTEKV_MASTER_PORT}|${BYTEKV_PROXY_PORT}|${BYTEKV_PS_BASE_PORT}|17000|17080)' || true
EOF
}

main() {
  check_auth
  discover_cluster
  upload_artifacts

  ssm_run "install/start meta services" "$META_ID" "$(start_abase_meta_cmd; echo; start_bytekv_meta_cmd)"
  ssm_run "install/start data-01 services" "$DATA1_ID" "$(start_abase_data_cmd data-01; echo; start_bytekv_data_cmd 1 ${BYTEKV_PS_BASE_PORT})"
  ssm_run "install/start data-02 services" "$DATA2_ID" "$(start_abase_data_cmd data-02; echo; start_bytekv_data_cmd 2 $((BYTEKV_PS_BASE_PORT + 1)))"

  ssm_run "combined meta smoke" "$META_ID" "$(combined_test_cmd)"
  ssm_run "combined data-01 smoke" "$DATA1_ID" "$(combined_test_cmd)"
  ssm_run "combined data-02 smoke" "$DATA2_ID" "$(combined_test_cmd)"
}

main "$@"
