#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=temporalstore}"
: "${AWS_REGION:=us-west-2}"
: "${OUT_DIR:=outputs}"

mkdir -p "$OUT_DIR"
ts="$(date -u +%Y%m%dT%H%M%SZ)"

run_one() {
  local name="$1"
  shift
  echo "== $name =="
  local out="$OUT_DIR/aws_abase_${name}_${ts}.json"
  env AWS_PROFILE="$AWS_PROFILE" AWS_REGION="$AWS_REGION" "$@" | tee "$out"
}

run_one read MODE=read THREADS=1 OPS=5000 KEYS=1000 ./tools/aws_abase_benchmark.sh
run_one mixed MODE=mixed THREADS=2 OPS=10000 KEYS=1000 ./tools/aws_abase_benchmark.sh
run_one write MODE=write THREADS=2 OPS=5000 KEYS=1000 ./tools/aws_abase_benchmark.sh
