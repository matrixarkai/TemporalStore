#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/release}"
SERVER_OUT_DIR="${SERVER_OUT_DIR:-${OUT_DIR}}"
METASERVER_OUT_DIR="${METASERVER_OUT_DIR:-${OUT_DIR}}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/temporalstore-redis-live-smoke}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-redis-live-result}"
CLUSTER_NAME="${CLUSTER_NAME:-redis-live-$$}"
MS_PORT="${MS_PORT:-18100}"
MS_RAFT_PORT="${MS_RAFT_PORT:-18110}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-18120}"
SERVER_PORT="${SERVER_PORT:-18101}"
MAX_SLOT="${MAX_SLOT:-1073741823}"
REDIS_COMPAT_SURFACE="${REDIS_COMPAT_SURFACE:-trimmed}"
REDIS_EXPECT_UNSUPPORTED_COLLECTIONS="${REDIS_EXPECT_UNSUPPORTED_COLLECTIONS:-0}"

smoke_out="${SMOKE_DIR}.out"
bin_dir="${SMOKE_DIR}.bin"
smoke_pid=""

cleanup() {
  local status=$?
  if [[ "${KEEP_ON_FAIL:-0}" == "1" && "${status}" != "0" ]]; then
    echo "KEEP_ON_FAIL=1, preserving failed cluster at ${SMOKE_DIR}" >&2
    return "${status}"
  fi
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  if [[ -n "${smoke_pid}" ]]; then
    kill "${smoke_pid}" >/dev/null 2>&1 || true
  fi
  rm -rf "${bin_dir}"
  return "${status}"
}
trap cleanup EXIT

rm -rf "${SMOKE_DIR}" "${RESULT_DIR}" "${smoke_out}" "${bin_dir}"
mkdir -p "${bin_dir}"
ln -sf "${SERVER_OUT_DIR}/bcache2-server" "${bin_dir}/bcache2-server"
ln -sf "${METASERVER_OUT_DIR}/bcache2-metaserver" "${bin_dir}/bcache2-metaserver"

env \
  BUILD_TYPE="${BUILD_TYPE}" \
  OUT_DIR="${bin_dir}" \
  SMOKE_DIR="${SMOKE_DIR}" \
  CLUSTER_NAME="${CLUSTER_NAME}" \
  MS_PORT="${MS_PORT}" \
  MS_RAFT_PORT="${MS_RAFT_PORT}" \
  MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
  SERVER_PORT="${SERVER_PORT}" \
  SERVER_EXTRA_FLAGS="${SERVER_EXTRA_FLAGS:---storage_async=true}" \
  KEEP_RUNNING=1 \
  SERVER_COUNT=1 \
  REPLICA_COUNT=1 \
  META_COUNT=1 \
  bash "${ROOT}/tools/smoke_ubuntu22.sh" > "${smoke_out}" 2>&1 &
smoke_pid=$!

ready=0
for _ in $(seq 1 180); do
  if grep -q "KEEP_RUNNING=1" "${smoke_out}" 2>/dev/null; then
    ready=1
    break
  fi
  if ! kill -0 "${smoke_pid}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if [[ "${ready}" != "1" ]]; then
  echo "TemporalStore live smoke startup failed" >&2
  tail -120 "${smoke_out}" >&2 || true
  exit 1
fi

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "missing redis-cli; install redis-tools" >&2
  exit 2
fi

partition_uri="file://${SMOKE_DIR}/redis-partition"
mkdir -p "${SMOKE_DIR}/redis-partition"
load_reply="$(timeout 10 redis-cli -h 127.0.0.1 -p "${SERVER_PORT}" --raw \
  partition load 1 1 "${partition_uri}" 0 "${MAX_SLOT}" master)"
if [[ "${load_reply}" != "OK" ]]; then
  echo "Redis partition load failed: ${load_reply}" >&2
  exit 1
fi
sleep "${REDIS_PARTITION_READY_SLEEP_S:-1}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
: > "${SUMMARY}"

redis_cmd() {
  timeout 10 redis-cli -h 127.0.0.1 -p "${SERVER_PORT}" --raw --no-auth-warning "$@"
}

expect_eq() {
  local name="$1"
  local expected="$2"
  shift 2
  local out
  out="$(redis_cmd "$@")"
  printf '%s\n' "${out}" > "${RESULT_DIR}/${name}.out"
  if [[ "${out}" != "${expected}" ]]; then
    echo "FAIL ${name}: expected [${expected}] got [${out}]" | tee -a "${SUMMARY}"
    exit 1
  fi
  echo "PASS ${name}" | tee -a "${SUMMARY}"
}

expect_error() {
  local name="$1"
  shift
  set +e
  local out
  out="$(redis_cmd "$@" 2>&1)"
  local code=$?
  set -e
  printf '%s\n' "${out}" > "${RESULT_DIR}/${name}.out"
  if [[ "${code}" == "0" ]] && ! printf '%s\n' "${out}" | grep -q '^ERR '; then
    echo "FAIL ${name}: expected Redis error got [${out}]" | tee -a "${SUMMARY}"
    exit 1
  fi
  echo "PASS ${name}" | tee -a "${SUMMARY}"
}

expect_eq ping PONG PING
expect_eq set OK SET rk rv
expect_eq get rv GET rk
expect_eq set_nx_missing OK SET rnx first NX
expect_eq set_nx_existing "" SET rnx second NX
expect_eq get_nx first GET rnx
expect_eq set_xx_missing "" SET rxx value XX
expect_eq set_xx_existing OK SET rnx updated XX
expect_eq get_xx updated GET rnx
expect_eq setnx_missing 1 SETNX rsetnx once
expect_eq setnx_existing 0 SETNX rsetnx twice
expect_eq get_setnx once GET rsetnx
expect_eq append 11 APPEND rk "-appended"
expect_eq strlen 11 STRLEN rk
expect_eq getset rv-appended GETSET rk swapped
expect_eq get_after_getset swapped GET rk
expect_eq getdel swapped GETDEL rk
expect_eq get_after_getdel "" GET rk
expect_eq reset_after_getdel OK SET rk restored
expect_eq incr 1 INCR rcount
expect_eq incrby 6 INCRBY rcount 5
expect_eq decr 5 DECR rcount
expect_eq decrby 3 DECRBY rcount 2
expect_eq exists 2 EXISTS rk rcount missing-key
expect_eq mset OK MSET rm1 a rm2 b
expect_eq mget $'a\nb' MGET rm1 rm2
expect_eq expire 1 EXPIRE rk 60
expect_eq pexpire 1 PEXPIRE rnx 60000
expect_eq psetex OK PSETEX rpsetex 60000 rv3
expect_eq get_psetex rv3 GET rpsetex
ttl_value="$(redis_cmd TTL rk)"
printf '%s\n' "${ttl_value}" > "${RESULT_DIR}/ttl.out"
if ! [[ "${ttl_value}" =~ ^[0-9]+$ ]] || [[ "${ttl_value}" -le 0 ]]; then
  echo "FAIL ttl: expected positive TTL got [${ttl_value}]" | tee -a "${SUMMARY}"
  exit 1
fi
echo "PASS ttl" | tee -a "${SUMMARY}"
pttl_value="$(redis_cmd PTTL rnx)"
printf '%s\n' "${pttl_value}" > "${RESULT_DIR}/pttl.out"
if ! [[ "${pttl_value}" =~ ^[0-9]+$ ]] || [[ "${pttl_value}" -le 0 ]]; then
  echo "FAIL pttl: expected positive PTTL got [${pttl_value}]" | tee -a "${SUMMARY}"
  exit 1
fi
echo "PASS pttl" | tee -a "${SUMMARY}"
expect_eq persist 1 PERSIST rnx
expect_eq ttl_after_persist -1 TTL rnx
expect_eq hset 2 HSET rh f1 v1 f2 v2
expect_eq hset_existing 0 HSET rh f1 v1b
expect_eq hget v1b HGET rh f1
expect_eq hmget $'v1b\nv2' HMGET rh f1 f2
expect_eq mget_missing $'a\n\nb' MGET rm1 rm_missing rm2
expect_eq hmget_missing $'v1b\n\nv2' HMGET rh f1 nofield f2
expect_eq hexists 1 HEXISTS rh f2
expect_eq hlen 2 HLEN rh
expect_eq hincrby 5 HINCRBY rh counter 5
expect_eq hdel 1 HDEL rh f1
if [[ "${REDIS_COMPAT_SURFACE}" == "full" ]]; then
expect_eq sadd 2 SADD rs a b a
expect_eq scard 2 SCARD rs
expect_eq sismember_present 1 SISMEMBER rs a
expect_eq sismember_missing 0 SISMEMBER rs missing
expect_eq srem 1 SREM rs a missing
expect_eq scard_after_srem 1 SCARD rs
expect_eq lpush 3 LPUSH rl a b c
expect_eq rpush 5 RPUSH rl d e
expect_eq llen 5 LLEN rl
expect_eq lindex c LINDEX rl 0
expect_eq lrange $'c\nb\na\nd\ne' LRANGE rl 0 -1
expect_eq lpop c LPOP rl
expect_eq rpop e RPOP rl
expect_eq ltrim OK LTRIM rl 0 1
expect_eq lrange_after_ltrim $'b\na' LRANGE rl 0 -1
expect_eq zadd 3 ZADD rz 1 alice 2 bob 1.5 carol
expect_eq zadd_update 0 ZADD rz 3 bob
expect_eq zcard 3 ZCARD rz
expect_eq zscore 3 ZSCORE rz bob
expect_eq zrank 2 ZRANK rz bob
expect_eq zrevrank 0 ZREVRANK rz bob
expect_eq zrange $'alice\ncarol\nbob' ZRANGE rz 0 -1
expect_eq zrevrange $'bob\ncarol\nalice' ZREVRANGE rz 0 -1
expect_eq zrange_withscores $'alice\n1\ncarol\n1.5\nbob\n3' ZRANGE rz 0 -1 WITHSCORES
expect_eq zrangebyscore $'carol\nbob' ZRANGEBYSCORE rz 1.1 3
expect_eq zcount 2 ZCOUNT rz 1.1 3
expect_eq zrem 1 ZREM rz carol missing
expect_eq zcard_after_zrem 2 ZCARD rz

else
  echo "SKIP live collection clone compatibility: REDIS_COMPAT_SURFACE=${REDIS_COMPAT_SURFACE}" | tee -a "${SUMMARY}"
  if [[ "${REDIS_EXPECT_UNSUPPORTED_COLLECTIONS}" == "1" ]]; then
    expect_error unsupported_sadd_trimmed SADD rs a
    expect_error unsupported_lpush_trimmed LPUSH rl a
    expect_error unsupported_zadd_trimmed ZADD rz 1 a
  fi
fi
expect_error unsupported_bgsave BGSAVE
expect_error unsupported_flushall FLUSHALL
expect_error unsupported_pslotinfo PSLOTINFO
expect_error unsupported_scan SCAN 0
expect_error unsupported_multi MULTI
expect_error unsupported_eval EVAL "return 1" 0
expect_error unsupported_xadd XADD rx "*" f v

if [[ "${RUN_COMPAT_SMOKE:-1}" == "1" ]]; then
  REDIS_HOST=127.0.0.1 \
    REDIS_PORT="${SERVER_PORT}" \
    RESULT_DIR="${RESULT_DIR}/compat" \
    KEY_PREFIX="ts:redis:live:${CLUSTER_NAME}" \
    REDIS_COMPAT_SURFACE="${REDIS_COMPAT_SURFACE}" \
    REDIS_EXPECT_UNSUPPORTED_COLLECTIONS="${REDIS_EXPECT_UNSUPPORTED_COLLECTIONS}" \
    bash "${ROOT}/tools/run_redis_compat_smoke_ubuntu22.sh"
fi

echo "PASS Redis live storage smoke" | tee -a "${SUMMARY}"
echo "${RESULT_DIR}"
