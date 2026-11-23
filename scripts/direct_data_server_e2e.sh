#!/bin/bash
# The data server as a real process against a real MatrixObject cluster, driven with a mix of command families, in both
# ways into the cluster. Three runs, each on a FRESH node (empty local directories), so everything a later run knows it has
# to read from the cluster:
#
#   1. no mode set (direct is the default)  applies the first batch of writes, then reads every family back
#   2. TS_MATRIXOBJECT_MODE=gateway          replays the cluster; its reads must equal run 1's; then it applies a second
#                                            batch (re-writes, deletes, pops, removes) and reads every family again
#   3. no mode set (direct again)            replays both batches; its reads must equal run 2's second reads
#
# Families: string, hash, set, zset, list (writes, overwrites, deletes, pops). Each family gets PASS, FAIL or NOT COVERED:
# PASS requires successful writes and exact expected values from all three fresh nodes, including overwritten and deleted keys. The feature-append and sequence families are driven by the write
# latency harness (`--op=featappend`, `--op=seqadd`): their writes must succeed and the replayed record count must grow.
# The object surface (put/get/range/list/delete/append on bytes) is checked by examples/direct_surface_cluster.rs.
#
#   SERVER=target/debug/server HARNESS=target/debug/data_server_write_latency_harness \
#   GATEWAY=127.0.0.1:24410 META=127.0.0.1:24404,127.0.0.1:24405,127.0.0.1:24406 \
#   TS_MATRIXOBJECT_META_GROUP=1 scripts/direct_data_server_e2e.sh
#
# Servers this script starts are stopped before it exits. A debug harness refuses to print timings without
# --allow-debug; this script passes it because it checks answers, not time.
set -u
ASSERTIONS=$(cd -- "$(dirname -- "$0")" && pwd)/direct_data_server_e2e_assertions.py
SERVER=${SERVER:?path of the temporalstore data server binary}
HARNESS=${HARNESS:?path of data_server_write_latency_harness}
GATEWAY=${GATEWAY:?host:port of a gateway on the cluster}
META=${META:?comma separated metadata endpoints}
OPS=${OPS:-100}
PORT=${PORT:-17102}
WORK=${WORK:-$(mktemp -d)}
export TS_MATRIXOBJECT_META_GROUP=${TS_MATRIXOBJECT_META_GROUP:-1}
FAMILIES="string hash set zset list"
overall=0
pid=""

b() { printf '%s' "$1" | od -An -tu1 | tr -s ' \n' ',' | sed 's/^,//; s/,$//'; }   # bytes as a JSON array body
ex() { curl -s -m 30 -X POST -H 'content-type: application/json' --data "{\"shard_id\":1,\"command\":$1}" "http://127.0.0.1:$PORT/execute"; }
cmd_set() { printf '{"kind":"string_set","key":"%s","value":[%s]}' "$1" "$(b "$2")"; }

# family<TAB>command, one per line
writes_1() {
  printf 'string\t%s\n' "$(cmd_set s1 v1)" "$(cmd_set s2 v2)" "$(cmd_set s3 v3)"
  printf 'hash\t{"kind":"hash_set","key":"h1","field":"f1","value":[%s]}\n' "$(b a)"
  printf 'hash\t{"kind":"hash_set","key":"h1","field":"f2","value":[%s]}\n' "$(b b)"
  printf 'hash\t{"kind":"hash_set","key":"h2","field":"f1","value":[%s]}\n' "$(b c)"
  for m in x y z; do printf 'set\t{"kind":"set_add","key":"st1","member":[%s]}\n' "$(b $m)"; done
  printf 'set\t{"kind":"set_add","key":"st2","member":[%s]}\n' "$(b p)"
  printf 'zset\t{"kind":"z_set_add","key":"z1","member":[%s],"score":1.5}\n' "$(b m1)"
  printf 'zset\t{"kind":"z_set_add","key":"z1","member":[%s],"score":2.5}\n' "$(b m2)"
  printf 'zset\t{"kind":"z_set_add","key":"z1","member":[%s],"score":0.5}\n' "$(b m3)"
  for m in e1 e2 e3; do printf 'list\t{"kind":"list_push","key":"l1","member":[%s],"left":false}\n' "$(b $m)"; done
}
writes_2() {
  printf 'string\t%s\n' "$(cmd_set s2 v2b)"
  printf 'string\t{"kind":"string_delete","key":"s3"}\n'
  printf 'hash\t{"kind":"hash_set","key":"h1","field":"f3","value":[%s]}\n' "$(b d)"
  printf 'hash\t{"kind":"hash_delete","key":"h1","field":"f1"}\n'
  printf 'set\t{"kind":"set_remove","key":"st1","member":[%s]}\n' "$(b y)"
  printf 'set\t{"kind":"set_add","key":"st1","member":[%s]}\n' "$(b w)"
  printf 'zset\t{"kind":"z_set_add","key":"z1","member":[%s],"score":9.0}\n' "$(b m2)"
  printf 'zset\t{"kind":"z_set_remove","key":"z1","member":[%s]}\n' "$(b m3)"
  printf 'list\t{"kind":"list_pop","key":"l1","left":true}\n'
  printf 'list\t{"kind":"list_push","key":"l1","member":[%s],"left":false}\n' "$(b e4)"
}
reads() { python3 "$ASSERTIONS" --commands; }

# apply <file of family<TAB>command> <out file>: one response per line, prefixed by the family
apply() { : > "$2"; while IFS=$'\t' read -r fam body; do printf '%s\t%s\n' "$fam" "$(ex "$body")" >> "$2"; done < "$1"; }

start_node() {  # label, extra env...
  local label=$1; shift
  local dir=$WORK/$label; mkdir -p "$dir"
  env -u TS_MATRIXOBJECT_MODE -u TS_SHARED_STORE_URI -u TS_MATRIXOBJECT_META TS_SERVER_BIND_ADDR=127.0.0.1:$PORT TS_SHARD_ID=1 TS_META_ADDR="" \
      TS_CACHE_DIR=$dir/cache TS_BLOCK_STORE_DIR=$dir/blocks TS_INDEX_DIR=$dir/indexes \
      TS_SHARED_STORE_CLUSTER_ID="$CLUSTER_ID" "$@" "$SERVER" > "$dir/server.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 60); do
    (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && return 0
    kill -0 $pid 2>/dev/null || { echo "FAIL $label: the server exited"; tail -n 20 "$dir/server.log"; return 1; }
    sleep 1
  done
  echo "FAIL $label: the server never listened"; return 1
}
stop_node() {
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null || true
    pid=""
  fi
}
trap 'stop_node' EXIT

mkdir -p "$WORK"
writes_1 > "$WORK/w1.txt"; writes_2 > "$WORK/w2.txt"; reads > "$WORK/r.txt"

echo "== run 1: direct (no mode set)"
start_node direct-first TS_MATRIXOBJECT_META="$META" || exit 1
echo "$(sed 's/\x1b\[[0-9;]*m//g' "$WORK/direct-first/server.log" | grep -o 'networked matrixobject durability active.*uri="[^"]*"' | head -1)"
apply "$WORK/w1.txt" "$WORK/w1.resp"
apply "$WORK/r.txt" "$WORK/reads1.txt"
"$HARNESS" 127.0.0.1:$PORT "$OPS" 16 64 0 --allow-debug --op=featappend > "$WORK/direct-first/featappend.log" 2>&1; echo "$?" > "$WORK/feature-append.status"
"$HARNESS" 127.0.0.1:$PORT "$OPS" 16 64 0 --allow-debug --op=seqadd > "$WORK/direct-first/seqadd.log" 2>&1; echo "$?" > "$WORK/sequence.status"
stop_node

echo "== run 2: gateway, fresh node (must replay run 1)"
start_node gateway-after-direct TS_MATRIXOBJECT_MODE=gateway TS_SHARED_STORE_URI="matrixobject://$GATEWAY" || exit 1
apply "$WORK/r.txt" "$WORK/reads2.txt"

apply "$WORK/w2.txt" "$WORK/w2.resp"
apply "$WORK/r.txt" "$WORK/reads2b.txt"
stop_node

echo "== run 3: direct again, fresh node (must replay runs 1 and 2)"
start_node direct-after-both TS_MATRIXOBJECT_META="$META" || exit 1
apply "$WORK/r.txt" "$WORK/reads3.txt"

stop_node

python3 "$ASSERTIONS" "$WORK"
exit $?
