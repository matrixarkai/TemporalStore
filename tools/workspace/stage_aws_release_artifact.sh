#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC=${SRC:-${ROOT}/output-ubuntu22/release}
BUILD_DIR=${BUILD_DIR:-${ROOT}/build-ubuntu22/release}
DST=${DST:-${ROOT}/infra/aws/temporalstore-test/artifact-stage}
UI=${UI:-${ROOT}/matrixark-site}

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "missing required release artifact: $1" >&2
    exit 1
  fi
}

require_executable() {
  require_file "$1"
  if [[ ! -x "$1" ]]; then
    echo "release artifact is not executable: $1" >&2
    exit 1
  fi
}

require_server_flag() {
  local flag="$1"
  local help_text
  help_text="$("$SRC/bcache2-server" --help 2>&1 || true)"
  if ! grep -q -- "$flag" <<<"$help_text"; then
    echo "stale bcache2-server: missing required flag $flag" >&2
    echo "Rebuild from current source before staging the AWS artifact." >&2
    exit 1
  fi
}

rm -rf "$DST"
mkdir -p "$DST/bin" "$DST/lib" "$DST/sdk/lib" "$DST/sdk/include/bcache2" "$DST/monitoring-ui"

require_executable "$SRC/bcache2-server"
require_executable "$SRC/bcache2-metaserver"
require_executable "$SRC/bcache2-proxy"
require_executable "$SRC/string_scale_benchmark"
require_executable "$SRC/replication_smoke_example"
require_executable "$SRC/secondary_visibility_lag_benchmark"
require_server_flag "data_raft_read_mode"
require_server_flag "data_raft_bounded_stale_max_index_lag"
require_server_flag "data_replication_mode"

cp "$SRC/bcache2-server" "$DST/bin/"
cp "$SRC/bcache2-metaserver" "$DST/bin/"
cp "$SRC/bcache2-proxy" "$DST/bin/"
cp "$SRC/string_scale_benchmark" "$DST/bin/"
cp "$SRC/replication_smoke_example" "$DST/bin/"
cp "$SRC/secondary_visibility_lag_benchmark" "$DST/bin/"

if [[ -f "$SRC/temporal_aggregate_scale_benchmark" ]]; then
  cp "$SRC/temporal_aggregate_scale_benchmark" "$DST/bin/"
fi
if [[ -f "$SRC/module_ingest_query_example" ]]; then
  cp "$SRC/module_ingest_query_example" "$DST/bin/"
fi

if [[ -f "$BUILD_DIR/lib/libthrift.so.0.11.0" ]]; then
  cp "$BUILD_DIR/lib/libthrift.so.0.11.0" "$DST/lib/"
elif [[ -f /usr/lib/libthrift.so.0.11.0 ]]; then
  cp /usr/lib/libthrift.so.0.11.0 "$DST/lib/"
fi
if [[ -f /usr/lib/librocksdb.so.6.11 ]]; then
  cp /usr/lib/librocksdb.so.6.11 "$DST/lib/"
fi
if [[ -f "$SRC/sdk/lib/libbcache2.so" ]]; then
  cp "$SRC/sdk/lib/libbcache2.so" "$DST/sdk/lib/"
fi
if [[ -f "$SRC/sdk/lib/libbcache2.a" ]]; then
  cp "$SRC/sdk/lib/libbcache2.a" "$DST/sdk/lib/"
fi
if [[ -f "$SRC/sdk/include/bcache2/bcache2.h" ]]; then
  cp "$SRC/sdk/include/bcache2/bcache2.h" "$DST/sdk/include/bcache2/"
fi
if [[ -d "$UI" ]]; then
  cp -a "$UI/." "$DST/monitoring-ui/"
fi

cat > "$DST/MANIFEST.txt" <<EOF
TemporalStore AWS release artifact
Source dir: ${ROOT}
Build dir: ${BUILD_DIR}
Output dir: ${SRC}
Required Raft flags verified:
  data_raft_read_mode
  data_raft_bounded_stale_max_index_lag
  data_replication_mode
Created: $(date -Is)
EOF

find "$DST" -maxdepth 4 -type f -printf '%P %s\n' | sort
