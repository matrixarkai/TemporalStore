#!/usr/bin/env bash
# =============================================================================
# deploy_onebox.sh - one node, one process, memory + local disk (EBS).
#
# The smallest thing that is a whole TemporalStore: no metaserver, no peers, no
# shared store. The durable copy is this node's own disk, so there is no
# distance for an on-disk cache tier to span and none is opened - the cache is
# memory, and the durable tier is the volume under TS_PROFILE_DATA.
#
# This is the default shape. A datanode started with no configuration at all
# lands here, and this script is that shape written down and checked rather than
# left implicit.
#
# Usage:
#   sudo tools/deploy_onebox.sh
#   TS_PROFILE_DATA=/mnt/ebs/ts TS_SERVER_ADDR=0.0.0.0:17002 tools/deploy_onebox.sh
# =============================================================================
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/deploy_profile_common.sh"

# One node: no metaserver to register with, and no heartbeat loop.
export TS_STANDALONE=1
unset TS_META_ADDR TS_DISTRIBUTED 2>/dev/null || true

# Name the backend rather than letting `auto` probe for a shared store that a
# one-box deployment does not have. An explicit choice also skips the probe, so
# startup does not pay a timeout to discover what this script already knows.
export TS_STORAGE_BACKEND="${TS_STORAGE_BACKEND:-raft}"

# No shared store, therefore no on-disk cache tier. Left to the backend rule
# rather than forced, so the two cannot disagree.
unset TS_SHARED_STORE_DIR TS_MATRIXOBJECT_ENDPOINT TS_CACHE_DISK_TIER 2>/dev/null || true

export TS_SHARD_ID="${TS_SHARD_ID:-1}"
export TS_SERVER_NODE_ID="${TS_SERVER_NODE_ID:-1}"
export TS_SERVER_ADDR="${TS_SERVER_ADDR:-127.0.0.1:17002}"

# The whole memory budget belongs to one process here.
export TS_CACHE_MEMORY_BYTES="${TS_CACHE_MEMORY_BYTES:-1073741824}"

# One-box serves the ranking from the embeddings: dense 1.00, sparse 0.00.
export MATRIXARK_ONEBOX_EMBEDDING_FIRST="${MATRIXARK_ONEBOX_EMBEDDING_FIRST:-1}"

TS_PROFILE_EXPECT=one-box ts_profile_launch
