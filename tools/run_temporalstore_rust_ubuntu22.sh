#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

export TS_PROXY_ADDR="${TS_PROXY_ADDR:-127.0.0.1:17000}"
export TS_REDIS_ADDR="${TS_REDIS_ADDR:-127.0.0.1:16379}"
export TS_META_ADDR="${TS_META_ADDR:-127.0.0.1:17001}"
export TS_SERVER_ADDR="${TS_SERVER_ADDR:-127.0.0.1:17002}"
export TS_SHARD_ID="${TS_SHARD_ID:-1}"
export TS_CACHE_DIR="${TS_CACHE_DIR:-${ROOT}/target/temporalstore-rust-smoke/cache}"
export TS_PAGE_STORE_DIR="${TS_PAGE_STORE_DIR:-${ROOT}/target/temporalstore-rust-smoke/pages}"
export TS_INDEX_DIR="${TS_INDEX_DIR:-${ROOT}/target/temporalstore-rust-smoke/indexes}"

rm -rf "${ROOT}/target/temporalstore-rust-smoke"

cargo build -p temporalstore-rust --bins

cleanup() {
  for pid in ${META_PID:-} ${SERVER_PID:-} ${PROXY_PID:-} ${REDIS_PID:-}; do
    [[ -n "${pid}" ]] && kill "${pid}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

target/debug/metaserver &
META_PID=$!
sleep 0.3

target/debug/server &
SERVER_PID=$!
sleep 0.5

target/debug/proxy &
PROXY_PID=$!
sleep 0.3

target/debug/redis_proxy &
REDIS_PID=$!
sleep 0.3

target/debug/client set hello world
target/debug/client setex session alive 10000
target/debug/client get hello
target/debug/client get session
target/debug/client hset user:1 name vincent
target/debug/client hget user:1 name
target/debug/client sadd group:1 alice
target/debug/client sadd group:1 bob
target/debug/client smembers group:1
target/debug/client fappend feature:1 100 abc
target/debug/client fquery feature:1 0 200
target/debug/client fappend feature:agg 100 2
target/debug/client fappend feature:agg 200 3
target/debug/client fagg feature:agg 0 300 sum
target/debug/client freplace feature:agg 0 150 100 5
target/debug/client fagg feature:agg 0 300 sum
target/debug/client fdel feature:agg
target/debug/client fagg feature:agg 0 300 count
target/debug/client seqadd seq:1 100 900 3 120 7001
target/debug/client seqquery seq:1 0 200 10 action_type eq 3
target/debug/client ipsadd ips:1 100 inst-a
target/debug/client ipsadd ips:1 200 inst-b
target/debug/client ipslast ips:1 1
target/debug/client riskinc risk:1 100 2
target/debug/client riskinc risk:1 200 3
target/debug/client riskcount risk:1 0 200
target/debug/client ttl hello

python3 - <<'PY'
import os
import socket

host, port = os.environ["TS_REDIS_ADDR"].rsplit(":", 1)
port = int(port)

def frame(*parts):
    out = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        data = str(part).encode()
        out.append(f"${len(data)}\r\n".encode() + data + b"\r\n")
    return b"".join(out)

def call(*parts):
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(frame(*parts))
        sock.shutdown(socket.SHUT_WR)
        return sock.recv(4096)

checks = [
    (("PING",), b"+PONG\r\n"),
    (("SET", "redis:k", "v"), b"+OK\r\n"),
    (("GET", "redis:k"), b"$1\r\nv\r\n"),
    (("MSET", "redis:m1", "v1", "redis:m2", "v2"), b"+OK\r\n"),
    (("MGET", "redis:m1", "redis:missing", "redis:m2"), b"*3\r\n$2\r\nv1\r\n$-1\r\n$2\r\nv2\r\n"),
    (("EXISTS", "redis:k", "redis:missing", "redis:m1"), b":2\r\n"),
    (("GETDEL", "redis:m1"), b"$2\r\nv1\r\n"),
    (("DEL", "redis:k", "redis:missing", "redis:m2"), b":2\r\n"),
    (("HSET", "redis:h", "f", "x"), b":1\r\n"),
    (("HGET", "redis:h", "f"), b"$1\r\nx\r\n"),
    (("HINCRBY", "redis:h", "n", "3"), b":3\r\n"),
    (("HINCRBY", "redis:h", "n", "-1"), b":2\r\n"),
    (("HMSET", "redis:hm", "field111", "value111", "field222", "value222"), b"+OK\r\n"),
    (("HMGET", "redis:hm", "field111", "field200"), b"*2\r\n$8\r\nvalue111\r\n$-1\r\n"),
    (("HLEN", "redis:hm"), b":2\r\n"),
    (("SADD", "redis:s", "m"), b":1\r\n"),
    (("FAPPEND", "redis:f", "10", "2"), b"+OK\r\n"),
    (("FAPPEND", "redis:f", "20", "3"), b"+OK\r\n"),
    (("FAGG", "redis:f", "0", "30", "sum"), b":5\r\n"),
]

for command, expected in checks:
    response = call(*command)
    print(command, response.decode(errors="replace").strip())
    assert response == expected, (command, response, expected)
PY
