#!/usr/bin/env bash
set -euo pipefail

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_AUTH="${REDIS_AUTH:-}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-redis-compat-$(date +%Y%m%d_%H%M%S)}"
KEY_PREFIX="${KEY_PREFIX:-ts:redis:compat:$(date +%s):$$}"
TIMEOUT_S="${TIMEOUT_S:-5}"
RUN_BENCH="${RUN_BENCH:-0}"
BENCH_REQUESTS="${BENCH_REQUESTS:-1000}"
BENCH_CLIENTS="${BENCH_CLIENTS:-8}"
BENCH_KEYSPACE="${BENCH_KEYSPACE:-100000}"
REDIS_COMPAT_SURFACE="${REDIS_COMPAT_SURFACE:-trimmed}"
REDIS_TEST_MODEL_COMMANDS="${REDIS_TEST_MODEL_COMMANDS:-0}"
REDIS_EXPECT_UNSUPPORTED_COLLECTIONS="${REDIS_EXPECT_UNSUPPORTED_COLLECTIONS:-0}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
: > "${SUMMARY}"

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "missing redis-cli; install redis-tools" >&2
  exit 2
fi

redis_cmd() {
  local args=(redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" --raw --no-auth-warning)
  if [[ -n "${REDIS_AUTH}" ]]; then
    args+=(-a "${REDIS_AUTH}")
  fi
  timeout "${TIMEOUT_S}" "${args[@]}" "$@"
}

expect_eq() {
  local name="$1"
  local expected="$2"
  shift 2
  local out
  set +e
  out="$(redis_cmd "$@" 2>"${RESULT_DIR}/${name}.err")"
  local code=$?
  set -e
  printf '%s\n' "${out}" > "${RESULT_DIR}/${name}.out"
  if [[ "${code}" != "0" ]]; then
    echo "FAIL ${name}: command exited ${code}" | tee -a "${SUMMARY}"
    cat "${RESULT_DIR}/${name}.err" >&2 || true
    exit 1
  fi
  if [[ "${out}" != "${expected}" ]]; then
    echo "FAIL ${name}: expected [${expected}] got [${out}]" | tee -a "${SUMMARY}"
    exit 1
  fi
  echo "PASS ${name}" | tee -a "${SUMMARY}"
}

expect_contains_line() {
  local name="$1"
  local expected_line="$2"
  shift 2
  local out
  out="$(redis_cmd "$@" 2>"${RESULT_DIR}/${name}.err")"
  printf '%s\n' "${out}" > "${RESULT_DIR}/${name}.out"
  if ! printf '%s\n' "${out}" | grep -Fxq "${expected_line}"; then
    echo "FAIL ${name}: missing line [${expected_line}]" | tee -a "${SUMMARY}"
    exit 1
  fi
  echo "PASS ${name}" | tee -a "${SUMMARY}"
}

expect_error() {
  local name="$1"
  shift
  local out
  set +e
  out="$(redis_cmd "$@" 2>&1)"
  local code=$?
  set -e
  printf '%s\n' "${out}" > "${RESULT_DIR}/${name}.out"
  if ! printf '%s\n' "${out}" | grep -Eiq 'ERR|unsupported|unknown|wrong'; then
    echo "FAIL ${name}: expected Redis error text got [${out}]" | tee -a "${SUMMARY}"
    exit 1
  fi
  echo "PASS ${name}" | tee -a "${SUMMARY}"
}

k() { printf '%s:%s' "${KEY_PREFIX}" "$1"; }

expect_eq ping PONG PING
expect_eq echo hello ECHO hello
expect_eq select_db0 OK SELECT 0
expect_error select_nonzero SELECT 1
expect_eq client_setname OK CLIENT SETNAME matrixark-smoke
expect_eq client_getname "" CLIENT GETNAME
expect_eq client_id 0 CLIENT ID
command_count="$(redis_cmd COMMAND COUNT 2>/dev/null || true)"
printf '%s
' "${command_count}" > "${RESULT_DIR}/command_count.out"
if [[ "${command_count}" =~ ^[0-9]+$ ]]; then
  echo "PASS command_count" | tee -a "${SUMMARY}"
else
  echo "FAIL command_count: expected integer got [${command_count}]" | tee -a "${SUMMARY}"
  exit 1
fi
expect_eq set OK SET "$(k string)" v1
expect_eq get v1 GET "$(k string)"
expect_eq type_string string TYPE "$(k string)"
expect_eq type_missing none TYPE "$(k missing)"
expect_eq set_nx_missing OK SET "$(k nx)" first NX
expect_eq set_nx_existing "" SET "$(k nx)" second NX
expect_eq get_nx first GET "$(k nx)"
expect_eq set_xx_missing "" SET "$(k xx)" value XX
expect_eq set_xx_existing OK SET "$(k nx)" updated XX
expect_eq get_xx updated GET "$(k nx)"
expect_eq set_ex_option OK SET "$(k setexopt)" ttlv EX 60
expect_eq get_set_ex_option ttlv GET "$(k setexopt)"
expect_eq set_px_option OK SET "$(k setpxopt)" pxttl PX 60000
expect_eq get_set_px_option pxttl GET "$(k setpxopt)"
expect_eq set_get_existing updated SET "$(k nx)" get-replaced GET
expect_eq get_after_set_get get-replaced GET "$(k nx)"
expect_eq set_get_missing "" SET "$(k setgetmissing)" first GET
expect_eq get_set_get_missing first GET "$(k setgetmissing)"
expect_error set_ttl_conditional SET "$(k setbad)" v EX 60 NX
expect_eq setnx_missing 1 SETNX "$(k setnx)" once
expect_eq setnx_existing 0 SETNX "$(k setnx)" twice
expect_eq get_setnx once GET "$(k setnx)"
expect_eq append 7 APPEND "$(k string)" -tail
expect_eq strlen 7 STRLEN "$(k string)"
expect_eq getset v1-tail GETSET "$(k string)" swapped
expect_eq get_after_getset swapped GET "$(k string)"
expect_eq getdel swapped GETDEL "$(k string)"
expect_eq get_after_getdel "" GET "$(k string)"
expect_eq reset_after_getdel OK SET "$(k string)" restored
expect_eq incr 1 INCR "$(k counter)"
expect_eq incrby 6 INCRBY "$(k counter)" 5
expect_eq decr 5 DECR "$(k counter)"
expect_eq decrby 3 DECRBY "$(k counter)" 2
expect_eq exists 2 EXISTS "$(k string)" "$(k counter)" "$(k missing)"
expect_eq setex OK SETEX "$(k setex)" 60 v2
expect_eq get_setex v2 GET "$(k setex)"
expect_eq psetex OK PSETEX "$(k psetex)" 60000 v3
expect_eq get_psetex v3 GET "$(k psetex)"
expect_eq getex_plain v3 GETEX "$(k psetex)"
expect_eq getex_ex v3 GETEX "$(k psetex)" EX 60
expect_eq getex_persist v3 GETEX "$(k psetex)" PERSIST
expect_eq mset OK MSET "$(k m1)" a "$(k m2)" b
expect_eq mget $'a\nb' MGET "$(k m1)" "$(k m2)"
expect_eq mget_missing $'a\n\nb' MGET "$(k m1)" "$(k missing)" "$(k m2)"
expect_eq expire 1 EXPIRE "$(k string)" 60
expect_eq pexpire 1 PEXPIRE "$(k nx)" 60000
ttl_value="$(redis_cmd TTL "$(k string)")"
printf '%s
' "${ttl_value}" > "${RESULT_DIR}/ttl_positive.out"
if ! [[ "${ttl_value}" =~ ^[0-9]+$ ]] || [[ "${ttl_value}" -le 0 ]]; then
  echo "FAIL ttl_positive: expected positive TTL got [${ttl_value}]" | tee -a "${SUMMARY}"
  exit 1
fi
echo "PASS ttl_positive" | tee -a "${SUMMARY}"
pttl_value="$(redis_cmd PTTL "$(k nx)")"
printf '%s\n' "${pttl_value}" > "${RESULT_DIR}/pttl_positive.out"
if ! [[ "${pttl_value}" =~ ^[0-9]+$ ]] || [[ "${pttl_value}" -le 0 ]]; then
  echo "FAIL pttl_positive: expected positive PTTL got [${pttl_value}]" | tee -a "${SUMMARY}"
  exit 1
fi
echo "PASS pttl_positive" | tee -a "${SUMMARY}"
expect_eq persist 1 PERSIST "$(k nx)"
expect_eq ttl_after_persist -1 TTL "$(k nx)"
expect_eq del_existing 1 DEL "$(k string)"
expect_eq ttl_missing -2 TTL "$(k string)"
expect_eq unlink_existing 1 UNLINK "$(k counter)"
expect_eq type_after_unlink none TYPE "$(k counter)"
expect_eq hset 2 HSET "$(k hash)" f1 v1 f2 v2
expect_eq type_hash hash TYPE "$(k hash)"
expect_eq hset_existing 0 HSET "$(k hash)" f1 v1b
expect_eq hsetnx_existing 0 HSETNX "$(k hash)" f1 should-not-set
expect_eq hsetnx_missing 1 HSETNX "$(k hash)" f3 v3
expect_eq hget_hsetnx v3 HGET "$(k hash)" f3
expect_eq hget v1b HGET "$(k hash)" f1
expect_eq hmget $'v1b\nv2' HMGET "$(k hash)" f1 f2
expect_eq hmget_missing $'v1b\n\nv2' HMGET "$(k hash)" f1 nofield f2
expect_eq hexists 1 HEXISTS "$(k hash)" f2
expect_eq hlen 3 HLEN "$(k hash)"
expect_eq hincrby_1 3 HINCRBY "$(k hash)" counter 3
expect_eq hincrby_2 7 HINCRBY "$(k hash)" counter 4
expect_eq hincrbyfloat_1 1.5 HINCRBYFLOAT "$(k hash)" float 1.5
expect_eq hincrbyfloat_2 2 HINCRBYFLOAT "$(k hash)" float 0.5
expect_contains_line hgetall_f1 f1 HGETALL "$(k hash)"
expect_contains_line hgetall_v1 v1b HGETALL "$(k hash)"
expect_contains_line hkeys_f1 f1 HKEYS "$(k hash)"
expect_contains_line hkeys_f2 f2 HKEYS "$(k hash)"
expect_contains_line hvals_v1 v1b HVALS "$(k hash)"
expect_contains_line hvals_v2 v2 HVALS "$(k hash)"
expect_eq hstrlen 3 HSTRLEN "$(k hash)" f1
expect_eq hstrlen_missing 0 HSTRLEN "$(k hash)" nofield
expect_eq hdel 1 HDEL "$(k hash)" f1
expect_eq hexists_after_hdel 0 HEXISTS "$(k hash)" f1
expect_eq hmset OK HMSET "$(k hash2)" a 1 b 2
expect_eq hmget_hmset $'1\n2' HMGET "$(k hash2)" a b

if [[ "${REDIS_TEST_MODEL_COMMANDS}" == "1" ]]; then
  expect_eq fappend OK FAPPEND "$(k feature)" 10 2
  expect_eq fappend_policy 1 FAPPENDPOLICY "$(k feature)" 20 3 UPSERT
  expect_eq fagg 5 FAGG "$(k feature)" 0 30 sum
  expect_eq riskincr OK RISKINCR "$(k risk)" 10 5
  expect_eq riskcount 5 RISKCOUNT "$(k risk)" 0 30
fi
if [[ "${REDIS_COMPAT_SURFACE}" == "full" ]]; then
expect_eq sadd 2 SADD "$(k set)" a b a
expect_eq sadd_other 3 SADD "$(k set2)" b c d
expect_eq type_set set TYPE "$(k set)"
expect_eq scard 2 SCARD "$(k set)"
expect_eq sismember_present 1 SISMEMBER "$(k set)" a
expect_eq sismember_missing 0 SISMEMBER "$(k set)" missing
expect_eq smismember $'1\n0\n1' SMISMEMBER "$(k set)" a missing b
expect_contains_line smembers_a a SMEMBERS "$(k set)"
expect_contains_line smembers_b b SMEMBERS "$(k set)"
expect_contains_line sinter_b b SINTER "$(k set)" "$(k set2)"
expect_contains_line sunion_a a SUNION "$(k set)" "$(k set2)"
expect_contains_line sunion_d d SUNION "$(k set)" "$(k set2)"
expect_contains_line sdiff_a a SDIFF "$(k set)" "$(k set2)"
expect_eq srandmember a SRANDMEMBER "$(k set)"
expect_eq srandmember_count $'a\nb' SRANDMEMBER "$(k set)" 2
expect_eq srem 1 SREM "$(k set)" a missing
expect_eq scard_after_srem 1 SCARD "$(k set)"
expect_eq spop b SPOP "$(k set)"
expect_eq scard_after_spop 0 SCARD "$(k set)"
expect_eq lpushx_missing 0 LPUSHX "$(k listxmissing)" ghost
expect_eq rpushx_missing 0 RPUSHX "$(k listxmissing)" ghost
expect_eq lpush 3 LPUSH "$(k list)" a b c
expect_eq type_list list TYPE "$(k list)"
expect_eq lpushx_existing 4 LPUSHX "$(k list)" head
expect_eq rpush 6 RPUSH "$(k list)" d e
expect_eq rpushx_existing 7 RPUSHX "$(k list)" tail
expect_eq llen 7 LLEN "$(k list)"
expect_eq lindex head LINDEX "$(k list)" 0
expect_eq rpush_listmut 5 RPUSH "$(k listmut)" a b b c b
expect_eq lset OK LSET "$(k listmut)" 0 newhead
expect_eq lindex_after_lset newhead LINDEX "$(k listmut)" 0
expect_eq lrem_dupes 3 LREM "$(k listmut)" 0 b
expect_eq lrange_after_lrem $'newhead\nc' LRANGE "$(k listmut)" 0 -1
expect_eq lrange $'head\nc\nb\na\nd\ne\ntail' LRANGE "$(k list)" 0 -1
expect_eq lpop head LPOP "$(k list)"
expect_eq rpop tail RPOP "$(k list)"
expect_eq ltrim OK LTRIM "$(k list)" 0 1
expect_eq lrange_after_ltrim $'c\nb' LRANGE "$(k list)" 0 -1
expect_eq zadd 3 ZADD "$(k zset)" 1 alice 2 bob 1.5 carol
expect_eq type_zset zset TYPE "$(k zset)"
expect_eq zadd_update 0 ZADD "$(k zset)" 3 bob
expect_eq zcard 3 ZCARD "$(k zset)"
expect_eq zscore 3 ZSCORE "$(k zset)" bob
expect_eq zmscore $'1\n3' ZMSCORE "$(k zset)" alice bob missing
expect_eq zincrby 4.5 ZINCRBY "$(k zset)" 1.5 bob
expect_eq zscore_after_zincrby 4.5 ZSCORE "$(k zset)" bob
expect_eq zrank 2 ZRANK "$(k zset)" bob
expect_eq zrevrank 0 ZREVRANK "$(k zset)" bob
expect_eq zrange $'alice\ncarol\nbob' ZRANGE "$(k zset)" 0 -1
expect_eq zrevrange $'bob\ncarol\nalice' ZREVRANGE "$(k zset)" 0 -1
expect_eq zrange_withscores $'alice\n1\ncarol\n1.5\nbob\n4.5' ZRANGE "$(k zset)" 0 -1 WITHSCORES
expect_eq zrangebyscore $'carol\nbob' ZRANGEBYSCORE "$(k zset)" 1.1 5
expect_eq zrangebyscore_withscores $'carol\n1.5\nbob\n4.5' ZRANGEBYSCORE "$(k zset)" 1.1 5 WITHSCORES
expect_eq zrevrangebyscore $'bob\ncarol' ZREVRANGEBYSCORE "$(k zset)" 5 1.1
expect_eq zrevrangebyscore_withscores $'bob\n4.5\ncarol\n1.5' ZREVRANGEBYSCORE "$(k zset)" 5 1.1 WITHSCORES
expect_eq zrangebyscore_limit bob ZRANGEBYSCORE "$(k zset)" 1 5 LIMIT 2 1
expect_eq zrangebyscore_withscores_limit $'carol\n1.5' ZRANGEBYSCORE "$(k zset)" 1 5 WITHSCORES LIMIT 1 1
expect_eq zcount 2 ZCOUNT "$(k zset)" 1.1 5
expect_eq zremrangebyscore 1 ZREMRANGEBYSCORE "$(k zset)" 1.4 1.6
expect_eq zremrangebyrank 1 ZREMRANGEBYRANK "$(k zset)" 0 0
expect_eq zcard_after_zremrange 1 ZCARD "$(k zset)"
expect_eq zrem 0 ZREM "$(k zset)" carol missing
expect_eq zcard_after_zrem 1 ZCARD "$(k zset)"
expect_eq zadd_pop 3 ZADD "$(k zpop)" 1 low 2 mid 3 high
expect_eq zpopmin $'low\n1\nmid\n2' ZPOPMIN "$(k zpop)" 2
expect_eq zpopmax $'high\n3' ZPOPMAX "$(k zpop)"

else
  echo "SKIP collection clone compatibility: REDIS_COMPAT_SURFACE=${REDIS_COMPAT_SURFACE}" | tee -a "${SUMMARY}"
  if [[ "${REDIS_EXPECT_UNSUPPORTED_COLLECTIONS}" == "1" ]]; then
    expect_error unsupported_sadd_trimmed SADD "$(k set)" a
    expect_error unsupported_lpush_trimmed LPUSH "$(k list)" a
    expect_error unsupported_zadd_trimmed ZADD "$(k zset)" 1 a
  fi
fi
expect_error unsupported_bgsave BGSAVE
expect_error unsupported_flushall FLUSHALL
expect_error unsupported_pslotinfo PSLOTINFO
expect_error unsupported_scan SCAN 0
if [[ "${REDIS_COMPAT_SURFACE}" == "full" ]]; then
  expect_error unsupported_hscan HSCAN "$(k hash)" 0
fi
expect_error unsupported_sscan SSCAN "$(k set2)" 0
expect_error unsupported_zscan ZSCAN "$(k zset)" 0
expect_error unsupported_keys KEYS "*"
expect_error unsupported_dbsize DBSIZE
expect_error unsupported_multi MULTI
expect_error unsupported_eval EVAL "return 1" 0
expect_error unsupported_evalsha EVALSHA abcdef 0
expect_error unsupported_cluster CLUSTER NODES
expect_error unsupported_xadd XADD "$(k stream)" "*" f v
expect_error unsupported_xgroup XGROUP CREATE "$(k stream)" g "$"

python3 - "${REDIS_HOST}" "${REDIS_PORT}" "${REDIS_AUTH}" "${KEY_PREFIX}" "${RESULT_DIR}" <<'RESPPY'
import socket
import sys
import threading

host, port_s, auth, prefix, result_dir = sys.argv[1:6]
port = int(port_s)

def encode(*parts):
    out = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        data = str(part).encode()
        out.append(f"${len(data)}\r\n".encode())
        out.append(data + b"\r\n")
    return b"".join(out)

class Resp:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b""
    def read_line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("connection closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line
    def read_exact(self, n):
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("connection closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data
    def parse(self):
        line = self.read_line()
        tag, payload = line[:1], line[1:]
        if tag == b"+":
            return payload.decode()
        if tag == b"-":
            return {"error": payload.decode()}
        if tag == b":":
            return int(payload)
        if tag == b"$":
            n = int(payload)
            if n < 0:
                return None
            return self.read_exact(n).decode()
        if tag == b"*":
            return [self.parse() for _ in range(int(payload))]
        raise RuntimeError(f"unsupported RESP line: {line!r}")

def connect():
    sock = socket.create_connection((host, port), timeout=5)
    sock.settimeout(5)
    resp = Resp(sock)
    if auth:
        sock.sendall(encode("AUTH", auth))
        reply = resp.parse()
        if reply != "OK":
            raise AssertionError(f"AUTH failed: {reply!r}")
    return sock, resp

sock, resp = connect()
pipe_key = f"{prefix}:pipeline"
sock.sendall(b"".join([
    encode("SET", pipe_key, "1"),
    encode("GET", pipe_key),
    encode("MSET", f"{pipe_key}:m1", "a", f"{pipe_key}:m2", "b"),
    encode("MGET", f"{pipe_key}:m1", f"{pipe_key}:m2"),
]))
replies = [resp.parse() for _ in range(4)]
sock.close()
if replies != ["OK", "1", "OK", ["a", "b"]]:
    raise AssertionError(f"pipeline replies mismatch: {replies!r}")

sock, resp = connect()
sock.sendall(encode("NO_SUCH_TEMPORALSTORE_COMMAND", f"{prefix}:x"))
unknown = resp.parse()
sock.close()
if not (isinstance(unknown, dict) and unknown.get("error")):
    raise AssertionError(f"unsupported command did not return Redis error: {unknown!r}")

errors = []
def worker(worker_id):
    try:
        s, r = connect()
        for _ in range(25):
            key = f"{prefix}:worker:{worker_id}:{_}"
            s.sendall(encode("SET", key, f"value-{worker_id}-{_}"))
            if r.parse() != "OK":
                raise AssertionError(f"worker {worker_id} SET failed")
            s.sendall(encode("GET", key))
            value = r.parse()
            if value != f"value-{worker_id}-{_}":
                raise AssertionError(f"worker {worker_id} bad GET reply {value!r}")
        s.close()
    except Exception as exc:
        errors.append(str(exc))
threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
if errors:
    raise AssertionError("; ".join(errors))

with open(f"{result_dir}/python_resp.out", "w", encoding="utf-8") as out:
    out.write("pipeline=pass\nunsupported_error=pass\nconcurrent_set_get=pass\n")
print("PASS python_resp_pipeline_and_concurrency")
RESPPY

echo "PASS python_resp_pipeline_and_concurrency" | tee -a "${SUMMARY}"

if [[ "${RUN_BENCH}" == "1" ]]; then
  if ! command -v redis-benchmark >/dev/null 2>&1; then
    echo "missing redis-benchmark; install redis-tools" >&2
    exit 2
  fi
  bench_args=(-h "${REDIS_HOST}" -p "${REDIS_PORT}" -n "${BENCH_REQUESTS}" -c "${BENCH_CLIENTS}" --csv)
  if [[ -n "${REDIS_AUTH}" ]]; then
    bench_args+=(-a "${REDIS_AUTH}")
  fi

  redis-benchmark "${bench_args[@]}" -t set,get \
    > "${RESULT_DIR}/redis-benchmark.csv" \
    2> "${RESULT_DIR}/redis-benchmark.err"
  echo "PASS redis_benchmark_set_get" | tee -a "${SUMMARY}"

  redis_cmd HSET "${KEY_PREFIX}:bench:hash" hit value >/dev/null
  redis_cmd SET "${KEY_PREFIX}:bench:string" value >/dev/null

  redis-benchmark "${bench_args[@]}" -r "${BENCH_KEYSPACE}" \
    HSET "${KEY_PREFIX}:bench:hash" __rand_int__ value \
    > "${RESULT_DIR}/redis-benchmark-hset.csv" \
    2> "${RESULT_DIR}/redis-benchmark-hset.err"
  echo "PASS redis_benchmark_hset" | tee -a "${SUMMARY}"

  redis-benchmark "${bench_args[@]}" \
    HGET "${KEY_PREFIX}:bench:hash" hit \
    > "${RESULT_DIR}/redis-benchmark-hget.csv" \
    2> "${RESULT_DIR}/redis-benchmark-hget.err"
  echo "PASS redis_benchmark_hget" | tee -a "${SUMMARY}"

  redis-benchmark "${bench_args[@]}" -r "${BENCH_KEYSPACE}" \
    HINCRBY "${KEY_PREFIX}:bench:hash" counter:__rand_int__ 1 \
    > "${RESULT_DIR}/redis-benchmark-hincrby.csv" \
    2> "${RESULT_DIR}/redis-benchmark-hincrby.err"
  echo "PASS redis_benchmark_hincrby" | tee -a "${SUMMARY}"

  redis-benchmark "${bench_args[@]}" -r "${BENCH_KEYSPACE}" \
    HINCRBYFLOAT "${KEY_PREFIX}:bench:hash" float:__rand_int__ 1.5 \
    > "${RESULT_DIR}/redis-benchmark-hincrbyfloat.csv" \
    2> "${RESULT_DIR}/redis-benchmark-hincrbyfloat.err"
  echo "PASS redis_benchmark_hincrbyfloat" | tee -a "${SUMMARY}"

  redis-benchmark "${bench_args[@]}" -r "${BENCH_KEYSPACE}" \
    INCR "${KEY_PREFIX}:bench:counter:__rand_int__" \
    > "${RESULT_DIR}/redis-benchmark-incr.csv" \
    2> "${RESULT_DIR}/redis-benchmark-incr.err"
  echo "PASS redis_benchmark_incr" | tee -a "${SUMMARY}"

  redis-benchmark "${bench_args[@]}" \
    EXPIRE "${KEY_PREFIX}:bench:string" 60 \
    > "${RESULT_DIR}/redis-benchmark-expire.csv" \
    2> "${RESULT_DIR}/redis-benchmark-expire.err"
  echo "PASS redis_benchmark_expire" | tee -a "${SUMMARY}"

  python3 - "${RESULT_DIR}" "${BENCH_REQUESTS}" "${BENCH_CLIENTS}" "${BENCH_KEYSPACE}" <<'BENCHPY'
import csv
import json
import sys
from pathlib import Path

result_dir = Path(sys.argv[1])
requests = int(sys.argv[2])
clients = int(sys.argv[3])
keyspace = int(sys.argv[4])
artifacts = [
    ("set_get", "redis-benchmark.csv"),
    ("hset", "redis-benchmark-hset.csv"),
    ("hget", "redis-benchmark-hget.csv"),
    ("hincrby", "redis-benchmark-hincrby.csv"),
    ("hincrbyfloat", "redis-benchmark-hincrbyfloat.csv"),
    ("incr", "redis-benchmark-incr.csv"),
    ("expire", "redis-benchmark-expire.csv"),
]

commands = []
for command, file_name in artifacts:
    path = result_dir / file_name
    rows = []
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.reader(source):
            if len(row) >= 2:
                rows.append(row)
    if not rows:
        raise SystemExit(f"benchmark artifact has no rows: {file_name}")
    qps_values = []
    for row in rows:
        try:
            qps_values.append(float(row[1]))
        except ValueError:
            pass
    if not qps_values:
        raise SystemExit(f"benchmark artifact has no numeric QPS: {file_name}")
    commands.append(
        {
            "command": command,
            "csv": file_name,
            "row_count": len(rows),
            "requests_per_second_min": min(qps_values),
            "requests_per_second_max": max(qps_values),
            "requests_per_second_avg": sum(qps_values) / len(qps_values),
        }
    )

summary = {
    "schema": "temporalstore_trimmed_redis_benchmark_summary_v1",
    "requests": requests,
    "clients": clients,
    "keyspace": keyspace,
    "commands": commands,
}
(result_dir / "redis-benchmark-summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
BENCHPY
  echo "PASS redis_benchmark_summary" | tee -a "${SUMMARY}"
fi

echo "PASS Redis compatibility smoke" | tee -a "${SUMMARY}"
echo "${RESULT_DIR}"
