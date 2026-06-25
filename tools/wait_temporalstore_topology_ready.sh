#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND="cpp"
METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
PREFIX="${MATRIXARK_TEMPORALSTORE_PREFIX:-matrixark:topology-ready}"
TIMEOUT_MS="${MATRIXARK_BACKEND_READINESS_TIMEOUT_MS:-30000}"
REQUEST_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS:-60000}"
IO_TIMEOUT_MS="${MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS:-60000}"
TEMPORALSTORE_LIB="${TEMPORALSTORE_LIB:-${ROOT}/output-ubuntu22/release/sdk/lib/libbcache2.so}"
RUST_CLI="${MATRIXARK_TEMPORALSTORE_RUST_CLI:-${ROOT}/sdk/rust/temporalstore/target/release/matrixark_record_log}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --backend)
      BACKEND="$2"
      shift 2
      ;;
    --metaserver)
      METASERVER="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --table)
      TABLE="$2"
      shift 2
      ;;
    --prefix)
      PREFIX="$2"
      shift 2
      ;;
    --timeout-ms)
      TIMEOUT_MS="$2"
      shift 2
      ;;
    --timeout-sec)
      TIMEOUT_MS="$(( $2 * 1000 ))"
      shift 2
      ;;
    --temporalstore-lib)
      TEMPORALSTORE_LIB="$2"
      shift 2
      ;;
    --rust-cli)
      RUST_CLI="$2"
      shift 2
      ;;
    -h|--help)
      cat <<EOF
usage: $0 [--backend cpp|rust] [--metaserver host:port] [--namespace name] [--table name]
          [--prefix key-prefix] [--timeout-sec seconds] [--temporalstore-lib path] [--rust-cli path]
EOF
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

export PYTHONPATH="${ROOT}:${ROOT}/sdk/python:${PYTHONPATH:-}"
export LD_LIBRARY_PATH="${ROOT}/output-ubuntu22/release/sdk/lib:${LD_LIBRARY_PATH:-}"
export MATRIXARK_BACKEND_READINESS_TIMEOUT_MS="${TIMEOUT_MS}"

python3 - "$BACKEND" "$METASERVER" "$NAMESPACE" "$TABLE" "$PREFIX" "$TEMPORALSTORE_LIB" "$RUST_CLI" "$REQUEST_TIMEOUT_MS" "$IO_TIMEOUT_MS" <<'PY'
import json
import sys

from tools.matrixark_mcp_server import MatrixArkTemporalStoreDirectAdapter, MatrixArkTemporalStoreRustAdapter

backend, metaserver, namespace, table, prefix, temporalstore_lib, rust_cli, request_timeout_ms, io_timeout_ms = sys.argv[1:]
common = {
    "metaserver": metaserver,
    "namespace": namespace,
    "table": table,
    "storage_prefix": prefix.rstrip(":") + ":readiness-ci",
    "request_timeout_ms": int(request_timeout_ms),
    "io_timeout_ms": int(io_timeout_ms),
}
try:
    if backend == "cpp":
        adapter = MatrixArkTemporalStoreDirectAdapter(library_path=temporalstore_lib, **common)
    elif backend == "rust":
        adapter = MatrixArkTemporalStoreRustAdapter(rust_cli=rust_cli, **common)
    else:
        raise SystemExit(f"unknown backend {backend!r}")
    result = adapter.ensure_backend_ready(reason="wait_temporalstore_topology_ready", probe=True)
except Exception as exc:
    result = {
        "status": "topology_not_ready",
        "backend": backend,
        "error": str(exc),
        "topology": {"metaserver": metaserver, "namespace": namespace, "table": table, "storage_prefix": prefix},
    }
print(json.dumps(result, indent=2, sort_keys=True))
raise SystemExit(0 if result.get("status") == "ready" else 2)
PY
