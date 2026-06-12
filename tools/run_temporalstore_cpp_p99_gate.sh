#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_FLAG=("--release")
CARGO_PREFIX=()
OUT="${TS_CPP_P99_OUT:-}"

if [[ "${TS_CPP_P99_PROFILE:-release}" == "debug" ]]; then
  PROFILE_FLAG=()
fi

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  CARGO_PREFIX=(env "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}")
fi

if [[ -z "${OUT}" ]]; then
  OUT="$(mktemp "${TMPDIR:-/tmp}/temporalstore-cpp-p99-gate.XXXXXX.json")"
fi

cd "$ROOT"

echo "== temporalstore rust c++ p99 gate =="
echo "output=${OUT}"

"${CARGO_PREFIX[@]}" cargo run "${PROFILE_FLAG[@]}" -p temporalstore-rust --bin scale_harness -- \
  --nodes "${TS_CPP_P99_NODES:-3}" \
  --string-ops "${TS_CPP_P99_STRING_OPS:-2000}" \
  --hash-ops "${TS_CPP_P99_HASH_OPS:-250}" \
  --sequence-keys "${TS_CPP_P99_SEQUENCE_KEYS:-2}" \
  --sequence-len "${TS_CPP_P99_SEQUENCE_LEN:-500}" \
  --scale-events "${TS_CPP_P99_SCALE_EVENTS:-0}" \
  --failover-every "${TS_CPP_P99_FAILOVER_EVERY:-0}" \
  --read-sample-every "${TS_CPP_P99_READ_SAMPLE_EVERY:-1}" \
  --max-log-entry-bytes "${TS_CPP_P99_MAX_LOG_ENTRY_BYTES:-32768}" \
  --compare-shared-store true \
  --shared-store-ops "${TS_CPP_P99_SHARED_STORE_OPS:-2000}" \
  --shared-store-flush-every "${TS_CPP_P99_SHARED_STORE_FLUSH_EVERY:-20}" \
  | tee "$OUT"

python3 - "$OUT" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text()
start = text.find("{")
end = text.rfind("}")
if start < 0 or end < start:
    raise SystemExit(f"no JSON object found in {path}")
summary = json.loads(text[start:end + 1])

thresholds = {
    # C++ doc: FEATURE query_window_one_point p99. Rust aggregate includes string
    # replica reads plus sequence filtered reads, so this is the fair read target.
    "raft_replica_read_latency.p99_us": int(
        __import__("os").environ.get("TS_CPP_P99_RAFT_READ_US", "1593")
    ),
    # C++ doc: STRING ingest_set p99.
    "shared_store.sync_primary_write_latency.p99_us": int(
        __import__("os").environ.get("TS_CPP_P99_SYNC_WRITE_US", "15695")
    ),
    "shared_store.async_primary_write_latency.p99_us": int(
        __import__("os").environ.get("TS_CPP_P99_ASYNC_WRITE_US", "15695")
    ),
    # C++ doc: STRING query_get p99.
    "shared_store.sync_replica_read_latency.p99_us": int(
        __import__("os").environ.get("TS_CPP_P99_SYNC_READ_US", "1353")
    ),
    "shared_store.async_replica_read_latency.p99_us": int(
        __import__("os").environ.get("TS_CPP_P99_ASYNC_READ_US", "1353")
    ),
}

def get(path):
    cur = summary
    for part in path.split("."):
        cur = cur[part]
    return cur

failures = []
for metric, limit in thresholds.items():
    actual = get(metric)
    status = "PASS" if actual <= limit else "FAIL"
    print(f"{status} {metric}: actual={actual}us target<={limit}us")
    if actual > limit:
        failures.append((metric, actual, limit))

if summary["max_replica_lag"] != 0:
    failures.append(("max_replica_lag", summary["max_replica_lag"], 0))
if summary["shared_store"]["sync_max_lag"] != 0:
    failures.append(("shared_store.sync_max_lag", summary["shared_store"]["sync_max_lag"], 0))

flush = summary["shared_store"].get("async_storage_flush_latency")
if flush:
    print(
        "INFO shared_store.async_storage_flush_latency.p99_us: "
        f"actual={flush['p99_us']}us samples={flush['samples']}"
    )
    if flush["samples"] == 0:
        failures.append(("shared_store.async_storage_flush_latency.samples", 0, ">0"))
    elif flush["p99_us"] == 0:
        failures.append(("shared_store.async_storage_flush_latency.p99_us", 0, ">0"))

async_durable = summary["shared_store"].get("async_storage_write_latency")
if async_durable:
    print(
        "INFO shared_store.async_storage_write_latency.p99_us: "
        f"actual={async_durable['p99_us']}us samples={async_durable['samples']}"
    )
    if async_durable["samples"] == 0:
        failures.append(("shared_store.async_storage_write_latency.samples", 0, ">0"))
    elif async_durable["p99_us"] == 0:
        failures.append(("shared_store.async_storage_write_latency.p99_us", 0, ">0"))
    if flush and async_durable["p99_us"] > flush["p99_us"]:
        failures.append(
            (
                "shared_store.async_storage_write_latency",
                async_durable.get("p99_us"),
                "<= async_storage_flush_latency batch p99",
            )
        )

if failures:
    print("\nC++ p99 gate failed:")
    for metric, actual, limit in failures:
        print(f"- {metric}: actual={actual}, target<={limit}")
    raise SystemExit(1)

print("\nC++ p99 gate passed.")
PY
