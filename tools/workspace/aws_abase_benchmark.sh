#!/usr/bin/env bash
set -eu

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${META_ID:=i-003c930417f7ee609}"
: "${NAMESPACE:=aws_scale}"
: "${TABLE:=bench}"
: "${PROXY:=127.0.0.1:19078}"
: "${MODE:=mixed}"
: "${THREADS:=2}"
: "${OPS:=10000}"
: "${KEYS:=1000}"
: "${VALUE_SIZE:=128}"

PARAMS="$(mktemp)"
NAMESPACE="$NAMESPACE" TABLE="$TABLE" PROXY="$PROXY" MODE="$MODE" THREADS="$THREADS" OPS="$OPS" KEYS="$KEYS" VALUE_SIZE="$VALUE_SIZE" python3 - <<'PY' > "$PARAMS"
import json
import os

env = {k: os.environ[k] for k in ["NAMESPACE", "TABLE", "PROXY", "MODE", "THREADS", "OPS", "KEYS", "VALUE_SIZE"]}
script = r'''
set -eu
cd /opt/abase/abase-runtime/python
cat > /tmp/abase_proxy_bench.py <<'PYBENCH'
import argparse
import json
import random
import statistics
import threading
import time

from local_sdks.python.abase_proxy_client import ProxyABaseClient


def percentile(values, pct):
    if not values:
        return 0
    values = sorted(values)
    idx = int((len(values) - 1) * pct / 100)
    return values[idx]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--proxy", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--mode", choices=["read", "write", "mixed"], default="mixed")
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--ops", type=int, default=10000)
    p.add_argument("--keys", type=int, default=1000)
    p.add_argument("--value-size", type=int, default=128)
    args = p.parse_args()

    host, port_s = args.proxy.split(":", 1)
    port = int(port_s)
    prefix = f"abase_bench_{int(time.time())}_"
    value = "x" * args.value_size

    # Preload read keys so read-only and mixed tests measure lookups, not misses.
    preload = ProxyABaseClient(host, port, timeout_ms=5000)
    for i in range(args.keys):
        preload.set(args.namespace, args.table, prefix + str(i), value)

    lat_us = []
    errors = 0
    reads = 0
    writes = 0
    lock = threading.Lock()
    start = time.time()

    def worker(tid):
        nonlocal errors, reads, writes
        client = ProxyABaseClient(host, port, timeout_ms=5000)
        local_lat = []
        local_errors = 0
        local_reads = 0
        local_writes = 0
        rng = random.Random(1000 + tid)
        per_thread = args.ops // args.threads
        if tid == args.threads - 1:
            per_thread += args.ops % args.threads
        for op in range(per_thread):
            key_id = rng.randrange(args.keys)
            key = prefix + str(key_id)
            do_write = args.mode == "write" or (args.mode == "mixed" and (op % 2 == 0))
            t0 = time.perf_counter_ns()
            try:
                if do_write:
                    client.set(args.namespace, args.table, key, value)
                    local_writes += 1
                else:
                    got = client.get(args.namespace, args.table, key)
                    if got is None:
                        raise RuntimeError("missing value")
                    local_reads += 1
                local_lat.append((time.perf_counter_ns() - t0) / 1000.0)
            except Exception:
                local_errors += 1
        with lock:
            lat_us.extend(local_lat)
            errors += local_errors
            reads += local_reads
            writes += local_writes

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    seconds = max(time.time() - start, 1e-9)
    result = {
        "system": "abase",
        "mode": args.mode,
        "threads": args.threads,
        "ops_requested": args.ops,
        "ops_success": len(lat_us),
        "reads": reads,
        "writes": writes,
        "errors": errors,
        "seconds": seconds,
        "qps": len(lat_us) / seconds,
        "latency_us": {
            "p50": percentile(lat_us, 50),
            "p95": percentile(lat_us, 95),
            "p99": percentile(lat_us, 99),
            "max": max(lat_us) if lat_us else 0,
            "avg": statistics.mean(lat_us) if lat_us else 0,
        },
    }
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
PYBENCH
PYTHONPATH=/opt/abase/abase-runtime/python python3 /tmp/abase_proxy_bench.py \
  --proxy="${PROXY}" --namespace="${NAMESPACE}" --table="${TABLE}" --mode="${MODE}" \
  --threads="${THREADS}" --ops="${OPS}" --keys="${KEYS}" --value-size="${VALUE_SIZE}"
'''
for key, value in env.items():
    script = f'export {key}="{value}"\n' + script
print(json.dumps({"commands": [script]}))
PY

CMD_ID="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm send-command \
  --document-name AWS-RunShellScript \
  --instance-ids "$META_ID" \
  --parameters "file://$PARAMS" \
  --query 'Command.CommandId' \
  --output text)"
rm -f "$PARAMS"
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm wait command-executed --command-id "$CMD_ID" --instance-id "$META_ID" || true
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" ssm get-command-invocation \
  --command-id "$CMD_ID" \
  --instance-id "$META_ID" \
  --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
  --output json
