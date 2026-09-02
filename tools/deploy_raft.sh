#!/usr/bin/env bash
# =============================================================================
# deploy_raft.sh - many nodes, each on memory + its own local disk (EBS).
#
# This is what scaling out means by default. Every node keeps a full durable
# copy on its own volume and the raft log keeps those copies in step, so the
# storage shape per node is identical to one-box - memory plus local disk, no
# on-disk cache tier, because a node's durable copy is already under it.
#
# What changes against one-box is node count, not storage: a metaserver to
# register with, a node id that must be unique, and peers to replicate to.
# Shared storage is the opt-in alternative (see deploy_shared_storage.sh); a
# node told to be distributed and nothing else lands here.
#
# Usage (per node):
#   TS_META_ADDR=10.0.0.10:17001 TS_SERVER_NODE_ID=2 TS_SHARD_ID=1 \
#   TS_SERVER_ADDR=10.0.0.11:17002 sudo -E tools/deploy_raft.sh
# =============================================================================
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_profile_common.sh"

# Required, and checked here rather than left to a default that would quietly
# produce a one-node cluster wearing a raft name.
: "${TS_META_ADDR:?raft needs a metaserver address (TS_META_ADDR)}"
: "${TS_SERVER_NODE_ID:?raft needs a unique node id (TS_SERVER_NODE_ID)}"
case "$(tr '[:upper:]' '[:lower:]' <<<"${TS_META_ADDR}")" in
  ''|local|none|standalone|off)
    echo "[deploy] TS_META_ADDR='${TS_META_ADDR}' names no metaserver - that is one-box." >&2
    echo "         Use tools/deploy_onebox.sh, or give a real address." >&2
    exit 1 ;;
esac

export TS_DISTRIBUTED=1
export TS_STANDALONE=0

# Raft replication: the durable copy is local, kept in step by the log.
export TS_STORAGE_BACKEND="${TS_STORAGE_BACKEND:-raft}"

# No shared store, so no on-disk cache tier - same as one-box, for the same
# reason: there is no distance between this node and its own durable copy.
unset TS_SHARED_STORE_DIR TS_MATRIXOBJECT_ENDPOINT TS_CACHE_DISK_TIER 2>/dev/null || true

export TS_SHARD_ID="${TS_SHARD_ID:-1}"
export TS_SERVER_ADDR="${TS_SERVER_ADDR:-0.0.0.0:17002}"
export TS_SERVER_ADVERTISE_ADDR="${TS_SERVER_ADVERTISE_ADDR:-${TS_SERVER_ADDR}}"
export TS_CLUSTER_ID="${TS_CLUSTER_ID:-temporalstore}"

# Several shards may share a box, so the per-process budget is smaller than the
# one-box default by design.
export TS_CACHE_MEMORY_BYTES="${TS_CACHE_MEMORY_BYTES:-536870912}"

TS_PROFILE_EXPECT=raft ts_profile_launch
