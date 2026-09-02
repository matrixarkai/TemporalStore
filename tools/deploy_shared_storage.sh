#!/usr/bin/env bash
# =============================================================================
# deploy_shared_storage.sh - many nodes over one authoritative shared store.
#
# The only profile with a distance to span. The durable copy does not live on
# the node, so shard data follows shards on rebalance and survives losing a
# datanode outright - and reads that miss memory would otherwise cross a network
# to reach it. That is what the on-disk cache tier is for, and this is the only
# profile that opens one.
#
# The tier is not forced on here. The backend rule turns it on for exactly the
# shared-storage backends, so the storage the node opens and the storage this
# script provisions come from one decision rather than two that can disagree.
#
# Usage (per node), against a shared filesystem root:
#   TS_SHARED_STORE_DIR=/mnt/shared/ts TS_META_ADDR=10.0.0.10:17001 \
#   TS_SERVER_NODE_ID=2 sudo -E tools/deploy_shared_storage.sh
#
# or against the object-store service:
#   TS_MATRIXOBJECT_ENDPOINT=10.0.0.20:17200 TS_META_ADDR=10.0.0.10:17001 \
#   TS_SERVER_NODE_ID=2 sudo -E tools/deploy_shared_storage.sh
# =============================================================================
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_profile_common.sh"

if [[ -z "${TS_SHARED_STORE_DIR:-}" && -z "${TS_MATRIXOBJECT_ENDPOINT:-}" ]]; then
  echo "[deploy] shared storage needs somewhere to be shared:" >&2
  echo "         TS_SHARED_STORE_DIR=<root>  or  TS_MATRIXOBJECT_ENDPOINT=<host:port>" >&2
  exit 1
fi
: "${TS_META_ADDR:?shared storage is a multi-node profile - set TS_META_ADDR}"
: "${TS_SERVER_NODE_ID:?shared storage needs a unique node id (TS_SERVER_NODE_ID)}"

export TS_DISTRIBUTED=1
export TS_STANDALONE=0

# Name the backend from what was configured. `auto` would reach the same answer
# when the endpoint is reachable, but it degrades to raft when it is not - and a
# node silently serving from local disk is not the deployment that was asked
# for. Naming it makes an unreachable store fail at startup instead.
if [[ -n "${TS_MATRIXOBJECT_ENDPOINT:-}" ]]; then
  export TS_STORAGE_BACKEND="${TS_STORAGE_BACKEND:-matrixobject}"
  export TS_MATRIXOBJECT_BUCKET="${TS_MATRIXOBJECT_BUCKET:-temporalstore}"
else
  export TS_STORAGE_BACKEND="${TS_STORAGE_BACKEND:-shared}"
fi

# Every node must agree on this or they namespace their keys apart and each
# ends up alone with a private copy of what was meant to be shared.
export TS_SHARED_STORE_CLUSTER_ID="${TS_SHARED_STORE_CLUSTER_ID:-temporalstore}"
export TS_CLUSTER_ID="${TS_CLUSTER_ID:-temporalstore}"

export TS_SHARD_ID="${TS_SHARD_ID:-1}"
export TS_SERVER_ADDR="${TS_SERVER_ADDR:-0.0.0.0:17002}"
export TS_SERVER_ADVERTISE_ADDR="${TS_SERVER_ADVERTISE_ADDR:-${TS_SERVER_ADDR}}"
export TS_CACHE_MEMORY_BYTES="${TS_CACHE_MEMORY_BYTES:-536870912}"

# The on-disk tier wants room: it is sized to hold what memory cannot and the
# shared store should not be asked for twice.
export TS_PROFILE_DATA="${TS_PROFILE_DATA:-/var/lib/temporalstore}"

TS_PROFILE_EXPECT=shared-storage ts_profile_launch
