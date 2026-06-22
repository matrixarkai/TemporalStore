param(
  [string]$Profile = "temporalstore",
  [string]$Region = "us-west-2",
  [string]$MetaInstanceId = "i-05f55360d92c43908",
  [string]$Data01InstanceId = "i-0cfbef56e86551535",
  [string]$Data02InstanceId = "i-04c93ad8271e5b64a",
  [string]$Data03InstanceId = "",
  [string]$MetaIp = "10.70.1.161",
  [string]$Data01Ip = "10.70.1.214",
  [string]$Data02Ip = "10.70.1.24",
  [string]$Data03Ip = "",
  [string]$ThreadList = "1 2",
  [int]$Ops = 4000,
  [int]$ValueBytes = 128,
  [string]$Modes = "shared_store",
  [ValidateSet("true", "false")]
  [string]$StorageAsync = "true",
  [switch]$AllowRaftBringup
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

function Write-JsonFileNoBom {
  param([string]$Path, [string]$Text)
  [System.IO.File]::WriteAllText($Path, $Text, [System.Text.UTF8Encoding]::new($false))
}

function Invoke-SsmScript {
  param([string]$InstanceId, [string]$Name, [string]$Script, [int]$Timeout = 900)
  $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Script))
  $command = "echo '$encoded' | base64 -d | tr -d '\r' >/tmp/$Name.sh && bash /tmp/$Name.sh"
  $params = @{ commands = @($command); executionTimeout = @("$Timeout") } | ConvertTo-Json -Depth 6 -Compress
  $paramFile = Join-Path $scriptDir "ssm-$Name-params.json"
  Write-JsonFileNoBom -Path $paramFile -Text $params

  for ($attempt = 1; $attempt -le 3; $attempt++) {
    $response = aws ssm send-command `
      --profile $Profile `
      --region $Region `
      --document-name AWS-RunShellScript `
      --instance-ids $InstanceId `
      --parameters "file://$paramFile" `
      --comment "$Name attempt=$attempt" | ConvertFrom-Json
    $commandId = $response.Command.CommandId
    if (-not $commandId) {
      throw "$Name failed to submit SSM command. Check AWS SSO/auth for profile '$Profile' in region '$Region'."
    }
    Write-Host "$Name command=$commandId instance=$InstanceId attempt=$attempt"
    while ($true) {
      Start-Sleep -Seconds 5
      $invocation = aws ssm get-command-invocation `
        --profile $Profile `
        --region $Region `
        --command-id $commandId `
        --instance-id $InstanceId | ConvertFrom-Json
      Write-Host "$(Get-Date -Format s) $Name $($invocation.Status)"
      if ($invocation.Status -notin @("Pending", "InProgress", "Delayed")) {
        $resultFile = Join-Path $scriptDir "ssm-$Name-result.json"
        $invocation | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resultFile -Encoding ascii
        if ($invocation.StandardOutputContent) { Write-Host $invocation.StandardOutputContent }
        if ($invocation.StandardErrorContent) { Write-Host $invocation.StandardErrorContent }
        if ($invocation.Status -eq "Success") { return $invocation }

        $emptyFailure = -not $invocation.StandardOutputContent -and -not $invocation.StandardErrorContent -and $invocation.ResponseCode -eq 1
        if ($emptyFailure -and $attempt -lt 3) {
          Write-Warning "$Name failed before shell output on $InstanceId; retrying SSM command"
          Start-Sleep -Seconds 10
          break
        }
        throw "$Name failed with status $($invocation.Status)"
      }
    }
  }
}

function New-StartMetaScript {
  return @"
set -euo pipefail
CLUSTER=aws_scale
BASE=/var/lib/temporalstore/raft-vs-shared-metaserver
export LD_LIBRARY_PATH=/opt/temporalstore/lib:`${LD_LIBRARY_PATH:-}
if [ -f /opt/temporalstore/runtime/lib/libthrift_conv_shim.so ]; then
  export LD_LIBRARY_PATH=/opt/temporalstore/runtime/lib:`$LD_LIBRARY_PATH
  export LD_PRELOAD=/opt/temporalstore/runtime/lib/libthrift_conv_shim.so:`${LD_PRELOAD:-}
fi
vendor_prefix=BYTE
vendor_prefix="${vendor_prefix}D"
export "${vendor_prefix}_HOST_IP=$MetaIp"
export "${vendor_prefix}_HOST_IPV6="
pkill -TERM -f "/opt/temporalstore/bin/bcache2-metaserver.*metaserver_server_port=17000" || true
sudo fuser -k 17000/tcp >/dev/null 2>&1 || true
sudo fuser -k 18000/tcp >/dev/null 2>&1 || true
sudo fuser -k 19000/tcp >/dev/null 2>&1 || true
sleep 2
rm -rf "`$BASE/data" "`$BASE/log"
mkdir -p "`$BASE/data" "`$BASE/log"
cd "`$BASE"
nohup /opt/temporalstore/bin/bcache2-metaserver \
  --metaserver_cluster_name="`$CLUSTER" \
  --metaserver_server_port=17000 \
  --metaserver_work_dir="`$BASE/data" \
  --metaserver_log_dir="`$BASE/log" \
  --metaserver_raft_id=1 \
  --metaserver_raft_peers=1,${MetaIp}:18000,${MetaIp}:19000,0 \
  --metaserver_raft_heartbeat_cycle_ms=500 \
  --metaserver_raft_election_cycle_ms=1500 \
  --metaserver_snapshot_trigger_interval_sec=0 \
  --metaserver_meta_check_routine_interval_sec=1 \
  --metaserver_convict_routine_interval_ms=500 \
  --metaserver_convict_safe_mode_warning_ratio=100 \
  --metaserver_convict_safe_mode_critical_ratio=100 \
  --metaserver_meta_check_max_freeze_partition_per_min=100 \
  --metaserver_balance_routine_interval_ms=3000 \
  --metaserver_placement_host_deduplicate=false \
  --metaserver_forbid_auto_register_for_convict_server=false \
  --metaserver_log_level=2 > "`$BASE/stdout" 2> "`$BASE/stderr" &
echo `$! > "`$BASE/metaserver.pid"
for i in `$(seq 1 90); do
  if curl -fsS "http://127.0.0.1:17000/" >/dev/null 2>&1 || ss -ltn | grep -q ":17000 "; then
    echo "META_READY ${MetaIp}:17000"
    ps -C bcache2-metaserver -o pid,pcpu,pmem,etime,args | grep 17000 || true
    exit 0
  fi
  sleep 1
done
echo "META_TIMEOUT"
tail -160 "`$BASE/stderr" || true
exit 1
"@
}

function New-StartDataScript {
  param([string]$Mode, [string]$NodeName, [string]$Ip, [string]$Vau, [int]$Port)
  $storageAsync = $StorageAsync
  $preflight = if ($Mode -eq "raft") {
    @'
help_text="$(/opt/temporalstore/bin/bcache2-server --help 2>&1 || true)"
for flag in data_replication_mode data_raft_read_mode data_raft_bounded_stale_max_index_lag data_raft_raft_port_delta data_raft_snapshot_port_delta; do
  if ! grep -q "$flag" <<<"$help_text"; then
    echo "STALE_TEMPORALSTORE_SERVER missing_flag=$flag"
    echo "Deploy one coherent current-source package before raft benchmarking."
    exit 2
  fi
done
'@
  } else {
    ""
  }
  $extraReplicaFlags = if ($Mode -eq "raft") {
    '--data_replication_mode=raft_consensus --data_raft_work_dir="$BASE/raft" --data_raft_enable_empty_snapshot_for_tests=false --data_raft_propose_timeout_ms=5000 --data_raft_read_mode=bounded_stale --data_raft_bounded_stale_max_index_lag=16'
  } else {
    "--data_replication_mode=shared_store --secondary_pull_stream_from_primary=false --replicator_loop_interval_us=1000 --replicator_max_oplog_per_loop=20000 --replicator_max_indexlog_per_loop=20000 --replicator_update_remote_interval_ms=20"
  }
  return @"
set -euo pipefail
CLUSTER=aws_scale
BASE=/var/lib/temporalstore/raft-vs-shared-$Mode-storage_async_$storageAsync-$NodeName
CACHE=/mnt/temporalstore-cache/mtcache-ssd
export LD_LIBRARY_PATH=/opt/temporalstore/lib:`${LD_LIBRARY_PATH:-}
if [ -f /opt/temporalstore/runtime/lib/libthrift_conv_shim.so ]; then
  export LD_LIBRARY_PATH=/opt/temporalstore/runtime/lib:`$LD_LIBRARY_PATH
  export LD_PRELOAD=/opt/temporalstore/runtime/lib/libthrift_conv_shim.so:`${LD_PRELOAD:-}
fi
vendor_prefix=BYTE
vendor_prefix="${vendor_prefix}D"
export "${vendor_prefix}_HOST_IP=$Ip"
export "${vendor_prefix}_HOST_IPV6="
$preflight
if [ $(( $Port + 21000 )) -gt 65535 ]; then
  echo "INVALID_RAFT_PORTS port=$Port raft=$($Port + 20000) snapshot=$($Port + 21000)"
  exit 2
fi
sudo mkdir -p "`$BASE/data" "`$BASE/log" "`$CACHE"
sudo chmod -R a+rwx "`$BASE" "`$CACHE"
pkill -TERM -f "/opt/temporalstore/bin/bcache2-server.*port=$Port" || true
sudo fuser -k ${Port}/tcp >/dev/null 2>&1 || true
sudo fuser -k $($Port + 20000)/tcp >/dev/null 2>&1 || true
sudo fuser -k $($Port + 21000)/tcp >/dev/null 2>&1 || true
sleep 2
rm -rf "`$BASE/data" "`$BASE/raft"
mkdir -p "`$BASE/data" "`$BASE/log"
cat > "`$BASE/host_spec.json" <<JSON
{
  "endpoint": {"addr_family": "ADDR_V4", "ip4": "$Ip", "port": $Port},
  "location": {"vregion": "vregion", "vdc": "vdc1", "vau": "$Vau"},
  "numa_nodes": [{"id": 0, "cpu_list": "-", "memory_size_mb": 1}]
}
JSON
cd "`$BASE"
nohup /opt/temporalstore/bin/bcache2-server \
  --cluster_name="`$CLUSTER" \
  --metaserver_uri=${MetaIp}:17000 \
  --host_spec_path="`$BASE/host_spec.json" \
  --host=$Ip \
  --port=$Port \
  --server_log_dir="`$BASE/log" \
  --server_log_level=2 \
  --server_meta_tinker_interval_ms=1000 \
  --server_heartbeat_interval_ms=1000 \
  --storage_zone_size=268435456 \
  --stream_max_blob_size=268435456 \
  --storage_async=$storageAsync \
  --storage_oplog_delay_dump_length=0 \
  --enable_blockcache=false \
  --blockcache_dram_capacity=0 \
  --blockcache_pmem_capacity=0 \
  --blockcache_ssd_capacity=0 \
  --blockcache_ssd_path="`$CACHE" \
  $extraReplicaFlags > "`$BASE/stdout" 2> "`$BASE/stderr" &
echo `$! > "`$BASE/server.pid"
for i in `$(seq 1 90); do
  if ss -ltnp | grep ":$Port " | grep -q bcache2-server; then
    if [ "$Mode" != "raft" ] || {
      ss -ltnp | grep ":$($Port + 20000) " | grep -q bcache2-server &&
      ss -ltnp | grep ":$($Port + 21000) " | grep -q bcache2-server
    }; then
      echo "SERVER_READY $Mode $NodeName ${Ip}:$Port"
      ps -C bcache2-server -o pid,pcpu,pmem,etime,args | grep "$Port" || true
      exit 0
    fi
  fi
  sleep 1
done
echo "SERVER_TIMEOUT $Mode $NodeName"
echo "listening ports:"
ss -ltnp | grep -E ":($Port|$($Port + 20000)|$($Port + 21000)) " || true
tail -160 "`$BASE/stderr" || true
tail -160 "`$BASE/log/"*WARNING* 2>/dev/null || true
tail -160 "`$BASE/log/"*ERROR* 2>/dev/null || true
exit 1
"@
}

function New-BenchScript {
  param([string]$Mode, [string]$RunId)
  $threadList = $ThreadList
  $electionPolicy = if ($Mode -eq "raft") { "PROMOTE_SECONDARY" } else { "PROMOTE_DERIVED" }
  $partitionRelation = if ($Mode -eq "raft") { "INDEPENDENT" } else { "ANTI_ENTROPY" }
  $hasData03 = -not [string]::IsNullOrWhiteSpace($Data03Ip)
  $partitionNum = if ($hasData03) { 3 } else { 2 }
  $addServer3 = if ($hasData03) {
    "post_json `"ManageService/AddServer`" '{`"id`":{`"cluster_name`":`"aws_scale`",`"timestamp`":1,`"operator_name`":`"raft_vs_shared`",`"operator`":`"raft_vs_shared`"},`"endpoint`":{`"addr_family`":`"ADDR_V4`",`"ip4`":`"$Data03Ip`",`"port`":17003},`"location`":{`"vregion`":`"vregion`",`"vdc`":`"vdc1`",`"vau`":`"vau3`"},`"numa_nodes`":[{`"id`":0,`"cpu_list`":`"-`",`"memory_size_mb`":1}]}' > add_server_3.json`ncheck_status add_server_3.json"
  } else {
    ""
  }
  $placementJson = if ($hasData03) {
    '[{\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau1\"},{\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau2\"},{\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau3\"}]'
  } else {
    '[{\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau1\"},{\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau2\"}]'
  }
  $storageUri = if ($Mode -eq "raft") {
    "file:///var/lib/temporalstore/raft-local/$RunId/"
  } else {
    "shared-file:///mnt/temporalstore-shared/aws-scale/storage/$RunId/"
  }
  return @"
set -euo pipefail
export LD_LIBRARY_PATH=/opt/temporalstore/lib:`${LD_LIBRARY_PATH:-}
ROOT=/var/lib/temporalstore/raft-vs-shared-results/$RunId
mkdir -p "`$ROOT"
cd "`$ROOT"
post_json() {
  curl -sS -H 'Content-Type: application/json' -X POST -d "`$2" "http://127.0.0.1:17000/`$1"
}
check_status() {
  python3 - "`$1" <<'PY'
import json, sys
data=json.load(open(sys.argv[1]))
code=data.get("status",{}).get("code",0)
msg=data.get("status",{}).get("message","")
if code not in (0,6,9):
    raise SystemExit(data)
if code == 9 and "name already used" not in msg:
    raise SystemExit(data)
PY
}
echo "mode=$Mode storage_async=$StorageAsync run_id=$RunId storage_uri=$storageUri" | tee summary.log
post_json "ManageService/AddServer" '{"id":{"cluster_name":"aws_scale","timestamp":1,"operator_name":"raft_vs_shared","operator":"raft_vs_shared"},"endpoint":{"addr_family":"ADDR_V4","ip4":"$Data01Ip","port":17001},"location":{"vregion":"vregion","vdc":"vdc1","vau":"vau1"},"numa_nodes":[{"id":0,"cpu_list":"-","memory_size_mb":1}]}' > add_server_1.json
check_status add_server_1.json
post_json "ManageService/AddServer" '{"id":{"cluster_name":"aws_scale","timestamp":1,"operator_name":"raft_vs_shared","operator":"raft_vs_shared"},"endpoint":{"addr_family":"ADDR_V4","ip4":"$Data02Ip","port":17002},"location":{"vregion":"vregion","vdc":"vdc1","vau":"vau2"},"numa_nodes":[{"id":0,"cpu_list":"-","memory_size_mb":1}]}' > add_server_2.json
check_status add_server_2.json
$addServer3
sleep 5
NS="rv_${Mode}_$(Get-Date -Format yyyyMMddHHmmss)"
TABLE="tbl"
REQ=`$(date +%s)
post_json "ManageService/AddNamespace" "{\"id\":{\"cluster_name\":\"aws_scale\",\"timestamp\":`$REQ,\"operator_name\":\"raft_vs_shared\",\"operator\":\"raft_vs_shared\"},\"name\":\"`$NS\"}" > add_ns.json
check_status add_ns.json
table_request="{\"id\":{\"cluster_name\":\"aws_scale\",\"timestamp\":`$REQ,\"operator_name\":\"raft_vs_shared\",\"operator\":\"raft_vs_shared\"},\"namespace_name\":\"`$NS\",\"name\":\"`$TABLE\",\"partition_set_num\":1,\"partition_units\":[{\"partition_num\":$partitionNum,\"placement_set\":$placementJson,\"storage_pool_uri\":\"$storageUri\",\"primary_prefer\":{\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau1\"}}],\"partition_unit_relation\":\"$partitionRelation\",\"election_policy\":\"$electionPolicy\",\"quota\":{\"ops_read\":1000},\"config\":{}}"
printf '%s\n' "`$table_request" > add_table_request.json
post_json "ManageService/AddTable" "`$table_request" > add_table.json
check_status add_table.json
if [ "$Mode" = "raft" ]; then
  if ! grep -q 'PROMOTE_SECONDARY' add_table_request.json; then
    echo "RAFT_CONTROL_PLANE_GUARD failed: raft table used PROMOTE_DERIVED"
    cat add_table_request.json
    exit 3
  fi
fi
wait_json() {
  local path="`$1"
  local body="`$2"
  local expr="`$3"
  local out="`$4"
  local attempts="`$5"
  for i in `$(seq 1 "`$attempts"); do
    post_json "`$path" "`$body" > "`$out" 2> "`$out.err" || true
    if python3 - "`$out" "`$expr" <<'PY'
import json, sys
path, expr = sys.argv[1], sys.argv[2]
try:
    data=json.load(open(path))
except Exception:
    raise SystemExit(1)
ok=eval(expr, {"__builtins__": {"all": all, "any": any, "len": len, "int": int}}, {"data": data})
raise SystemExit(0 if ok else 1)
PY
    then
      return 0
    fi
    sleep 1
  done
  echo "WAIT_TIMEOUT path=`$path expr=`$expr"
  cat "`$out" 2>/dev/null || true
  cat "`$out.err" 2>/dev/null || true
  return 1
}

list_table_body="{\"id\":{\"cluster_name\":\"aws_scale\",\"operator_name\":\"raft_vs_shared\"},\"read_stale\":false,\"namespace_name\":\"`$NS\",\"table_name\":\"`$TABLE\"}"
list_partition_body="{\"id\":{\"cluster_name\":\"aws_scale\",\"operator_name\":\"raft_vs_shared\"},\"read_stale\":false,\"namespace_name\":\"`$NS\",\"table_name\":\"`$TABLE\"}"
wait_json "QueryService/ListTable" "`$list_table_body" "len(data.get('tables', [])) >= 1 and data.get('tables', [{}])[0].get('state') == 'TABLE_NORMAL'" list_table_ready.json 180
wait_json "QueryService/ListPartition" "`$list_partition_body" "len(data.get('info', [])) >= 1 and len(data.get('info', [{}])[0].get('partition_info', [])) >= 2 and all(p.get('state') == 'P_NORMAL' for p in data.get('info', [{}])[0].get('partition_info', []))" list_partition_ready.json 180

echo "== replication smoke ==" | tee -a summary.log
set +e
timeout 120 /opt/temporalstore/bin/replication_smoke_example "${MetaIp}:17000" vdc1 "`$NS" "`$TABLE" > replication_smoke.out 2> replication_smoke.err
replication_code=`$?
set -e
cat replication_smoke.out replication_smoke.err | tee -a summary.log
if [ "`$replication_code" != "0" ]; then
  echo "REPLICATION_SMOKE_FAILED code=`$replication_code" | tee -a summary.log
  exit 4
fi

echo "== tight secondary visibility lag ==" | tee -a summary.log
set +e
timeout 120 /opt/temporalstore/bin/secondary_visibility_lag_benchmark "${MetaIp}:17000" vdc1 "`$NS" "`$TABLE" 100 1 $ValueBytes 10000 1 1 > secondary_visibility_lag.out 2> secondary_visibility_lag.err
visibility_code=`$?
set -e
cat secondary_visibility_lag.out secondary_visibility_lag.err | tee -a summary.log
if [ "`$visibility_code" != "0" ]; then
  echo "SECONDARY_VISIBILITY_LAG_FAILED code=`$visibility_code" | tee -a summary.log
  exit 12
fi

echo "threads,set_qps,set_p50_us,set_p95_us,set_p99_us,get_qps,get_p50_us,get_p95_us,get_p99_us,errors,exit_code" | tee results.csv
for t in $threadList; do
  out="string_t`$t.out"
  set +e
  timeout 300 /opt/temporalstore/bin/string_scale_benchmark "${MetaIp}:17000" vdc1 "`$NS" "`$TABLE" $Ops "`$t" $ValueBytes 1 1000 > "`$out" 2> "string_t`$t.err"
  bench_code=`$?
  set -e
  cat "`$out" >> raw_string.log
  python3 - "`$out" "`$t" "`$bench_code" <<'PY' | tee -a results.csv
import csv, sys
path, threads, code = sys.argv[1], sys.argv[2], sys.argv[3]
setrow = getrow = None
try:
    rows = list(csv.reader(open(path)))
except Exception:
    rows = []
for row in rows:
    if row and row[0] == "TemporalStore" and row[1] == "set":
        setrow = row
    if row and row[0] == "TemporalStore" and row[1] in ("get", "get_raw_success_attempt"):
        getrow = row
if not setrow or not getrow:
    print(",".join([threads,"","","","","","","","","1",code]))
else:
    errors = int(setrow[5]) + int(getrow[5])
    print(",".join([threads,setrow[6],setrow[8],setrow[9],setrow[10],getrow[6],getrow[8],getrow[9],getrow[10],str(errors),code]))
PY
  if [ "`$bench_code" != "0" ]; then
    echo "STRING_SCALE_FAILED threads=`$t code=`$bench_code" | tee -a summary.log
    cat "string_t`$t.err" | tee -a summary.log
    exit 5
  fi
done

echo "== local cpu =="
ps -C bcache2-server -o pid,pcpu,pmem,etime,args | tee cpu_meta.log || true
echo "RESULT_DIR=`$ROOT"
"@
}

$runId = Get-Date -Format "yyyyMMdd_HHmmss"
foreach ($mode in ($Modes -split '\s+')) {
  if (-not $mode) { continue }
  if ($mode -eq "raft" -and -not $AllowRaftBringup) {
    throw "raft mode is still a guarded bring-up path because real partition snapshots/failover validation are not complete. Re-run with -AllowRaftBringup only for explicit Raft debugging, not production benchmarks."
  }
  Invoke-SsmScript -InstanceId $MetaInstanceId -Name "ts-start-meta-$mode-$runId" -Script (New-StartMetaScript)
  Invoke-SsmScript -InstanceId $Data01InstanceId -Name "ts-start-$mode-data01-$runId" -Script (New-StartDataScript -Mode $mode -NodeName "data01" -Ip $Data01Ip -Vau "vau1" -Port 17001)
  Invoke-SsmScript -InstanceId $Data02InstanceId -Name "ts-start-$mode-data02-$runId" -Script (New-StartDataScript -Mode $mode -NodeName "data02" -Ip $Data02Ip -Vau "vau2" -Port 17002)
  if (-not [string]::IsNullOrWhiteSpace($Data03InstanceId)) {
    Invoke-SsmScript -InstanceId $Data03InstanceId -Name "ts-start-$mode-data03-$runId" -Script (New-StartDataScript -Mode $mode -NodeName "data03" -Ip $Data03Ip -Vau "vau3" -Port 17003)
  }
  $bench = Invoke-SsmScript -InstanceId $MetaInstanceId -Name "ts-bench-$mode-$runId" -Script (New-BenchScript -Mode $mode -RunId "$mode-$runId") -Timeout 1200
  $cpuScript = "ps -C bcache2-server -o pid,pcpu,pmem,etime,args | grep -E '17001|17002|17003' || true"
  try {
    Invoke-SsmScript -InstanceId $Data01InstanceId -Name "ts-cpu-$mode-data01-$runId" -Script $cpuScript
  } catch {
    Write-Warning "CPU scrape failed on data01 for ${mode}: $_"
  }
  try {
    Invoke-SsmScript -InstanceId $Data02InstanceId -Name "ts-cpu-$mode-data02-$runId" -Script $cpuScript
  } catch {
    Write-Warning "CPU scrape failed on data02 for ${mode}: $_"
  }
  if (-not [string]::IsNullOrWhiteSpace($Data03InstanceId)) {
    try {
      Invoke-SsmScript -InstanceId $Data03InstanceId -Name "ts-cpu-$mode-data03-$runId" -Script $cpuScript
    } catch {
      Write-Warning "CPU scrape failed on data03 for ${mode}: $_"
    }
  }
}
