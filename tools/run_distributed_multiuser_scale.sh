#!/usr/bin/env bash
# Distributed multi-user scale validation with forced eviction + cold-read promotion.
# Several users, large corpus each, partitioned namespace/table -> shard -> datanode, with a
# small per-datanode memory cache so writes spill memory->disk->block-store and reads promote back.
# Exits non-zero if any iteration fails correctness / partition-isolation / eviction / promotion.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATANODES="${TS_MU_DATANODES:-8}"
SHARDS="${TS_MU_SHARDS:-32}"
USERS="${TS_MU_USERS:-32}"
STRINGS="${TS_MU_STRINGS_PER_USER:-600}"
FEATURES="${TS_MU_FEATURES_PER_USER:-300}"
ITERATIONS="${TS_MU_ITERATIONS:-3}"
MEMORY_BYTES="${TS_MU_MEMORY_BYTES:-524288}"
VALUE_BYTES="${TS_MU_VALUE_BYTES:-96}"
GROWTH="${TS_MU_SCALE_GROWTH_PERCENT:-60}"

cargo run --release -p temporalstore-rust --bin distributed_multiuser_scale_harness -- \
  --datanodes "${DATANODES}" \
  --shards "${SHARDS}" \
  --users "${USERS}" \
  --string-records-per-user "${STRINGS}" \
  --feature-points-per-user "${FEATURES}" \
  --iterations "${ITERATIONS}" \
  --memory-bytes "${MEMORY_BYTES}" \
  --value-bytes "${VALUE_BYTES}" \
  --scale-growth-percent "${GROWTH}"
