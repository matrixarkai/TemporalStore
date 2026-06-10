#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-local-validation-target}"
RUN_AWS=0
LOCAL_SCALE_TIMEOUT="${TS_LOCAL_SCALE_TIMEOUT:-120s}"

usage() {
  cat >&2 <<'USAGE'
usage: run_temporalstore_parity_gate.sh [--local-only|--aws]

Runs the local TemporalStore Rust parity/basic-function gate. With --aws, also
deploys to the configured existing EKS cluster and validates AWS reads/writes,
Raft replication, storage modes, and Redis-compatible QPS.

Required for --aws:
  AWS_REGION
  TS_EKS_CLUSTER_NAME
  TS_IMAGE
  kubectl, terraform, aws, python3
USAGE
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --local-only)
      RUN_AWS=0
      shift
      ;;
    --aws)
      RUN_AWS=1
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 127
  fi
}

cd "${ROOT}"
export CARGO_TARGET_DIR="${TARGET_DIR}"

echo "== local: cargo test all targets =="
cargo test -p temporalstore-rust --all-targets -- --test-threads=1

echo "== local: cargo check all targets =="
cargo check -p temporalstore-rust --all-targets

echo "== local: distributed raft harness =="
cargo run -p temporalstore-rust --bin distributed_raft_harness > /tmp/temporalstore-raft-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-validation \
  --log /tmp/temporalstore-raft-validation.log

echo "== local: scale/read-write/shared-store harness =="
timeout "${LOCAL_SCALE_TIMEOUT}" cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes "${TS_LOCAL_SCALE_NODES:-3}" \
  --string-ops "${TS_LOCAL_STRING_OPS:-30}" \
  --hash-ops "${TS_LOCAL_HASH_OPS:-5}" \
  --sequence-keys "${TS_LOCAL_SEQUENCE_KEYS:-1}" \
  --sequence-len "${TS_LOCAL_SEQUENCE_LEN:-20}" \
  --scale-events "${TS_LOCAL_SCALE_EVENTS:-2}" \
  --failover-every "${TS_LOCAL_FAILOVER_EVERY:-10}" \
  --read-sample-every "${TS_LOCAL_READ_SAMPLE_EVERY:-5}" \
  --compare-shared-store true \
  --shared-store-ops "${TS_LOCAL_SHARED_STORE_OPS:-20}" \
  --shared-store-flush-every "${TS_LOCAL_SHARED_STORE_FLUSH_EVERY:-5}" \
  > /tmp/temporalstore-scale-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-scale-validation \
  --log /tmp/temporalstore-scale-validation.log

echo "== local: storage modes harness =="
cargo run -p temporalstore-rust --bin storage_modes_harness > /tmp/temporalstore-storage-validation.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-storage-validation \
  --log /tmp/temporalstore-storage-validation.log

echo "== local: terraform fmt and script syntax =="
terraform -chdir=infra/aws-existing-eks fmt -check
bash -n tools/deploy_and_test_aws_existing_eks.sh
bash -n tools/validate_aws_existing_eks.sh
bash -n tools/scale_test_aws_existing_eks.sh
python3 -m py_compile tools/validate_aws_validation_log.py

echo "== local: whitespace =="
git diff --check

if [[ "${RUN_AWS}" == "1" ]]; then
  require aws
  require kubectl
  require terraform
  require python3
  : "${AWS_REGION:?set AWS_REGION}"
  : "${TS_EKS_CLUSTER_NAME:?set TS_EKS_CLUSTER_NAME}"
  : "${TS_IMAGE:?set TS_IMAGE}"

  echo "== aws: deploy and validate =="
  tools/deploy_and_test_aws_existing_eks.sh
else
  echo "== aws: skipped =="
  echo "Pass --aws with AWS_REGION, TS_EKS_CLUSTER_NAME, and TS_IMAGE to run EKS validation."
fi

echo "TemporalStore parity gate passed."
