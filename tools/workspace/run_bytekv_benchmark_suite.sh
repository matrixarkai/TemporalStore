#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p outputs
ts="$(date -u +%Y%m%dT%H%M%SZ)"

THREADS=1 OPS=5000 READ_PERCENT=100 KEY_COUNT=1000 VALUE_SIZE=128 TABLE="bench_read_${ts}" \
  tools/aws_bytekv_benchmark.sh | tee "outputs/aws_bytekv_bench_read_${ts}.json"

THREADS=2 OPS=20000 READ_PERCENT=50 KEY_COUNT=2000 VALUE_SIZE=128 TABLE="bench_mixed_${ts}" \
  tools/aws_bytekv_benchmark.sh | tee "outputs/aws_bytekv_bench_mixed_${ts}.json"

THREADS=2 OPS=10000 READ_PERCENT=0 KEY_COUNT=2000 VALUE_SIZE=128 TABLE="bench_write_${ts}" \
  tools/aws_bytekv_benchmark.sh | tee "outputs/aws_bytekv_bench_write_${ts}.json"

echo "bytekv_benchmark_suite_outputs timestamp=${ts}"
