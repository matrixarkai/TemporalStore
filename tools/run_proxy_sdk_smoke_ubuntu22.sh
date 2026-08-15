#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-proxy-sdk-smoke-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "${RESULT_DIR}"

python3 "${ROOT}/tools/mock_temporalstore_proxy.py" > "${RESULT_DIR}/mock_proxy.out" 2>&1 &
proxy_pid="$!"
cleanup() {
  kill "${proxy_pid}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  if python3 - <<'PY' >/dev/null 2>&1
import json
import urllib.request

request = urllib.request.Request(
    "http://127.0.0.1:8080/v1/string/get",
    data=json.dumps({"namespace": "sdk_ns", "table": "sdk_table", "key": "health"}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=0.2) as response:
    response.read()
PY
  then
    break
  fi
  sleep 0.1
done

PYTHONPATH="${ROOT}/sdk/python" \
  python3 "${ROOT}/sdk/python/examples/proxy_sequence_features.py" \
  | tee "${RESULT_DIR}/python_proxy.out"

(
  cd "${ROOT}/sdk/rust/temporalstore"
  TEMPORALSTORE_UNIFIED_CORPUS="$(python3 "${ROOT}/tools/resolve_temporalstore_test_corpus.py")" \
    cargo test --no-default-features --features proxy --test unified_corpus
) | tee "${RESULT_DIR}/rust_proxy.out"

grep -q "profile=" "${RESULT_DIR}/python_proxy.out"
grep -q "SequenceFeatureRow" "${RESULT_DIR}/python_proxy.out"
grep -q "unified_corpus_proxy_contract ... ok" "${RESULT_DIR}/rust_proxy.out"

echo "PASS proxy SDK smoke"
echo "wrote:"
echo "  ${RESULT_DIR}/python_proxy.out"
echo "  ${RESULT_DIR}/rust_proxy.out"
