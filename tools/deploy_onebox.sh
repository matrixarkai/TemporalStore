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

# There is no metaserver in a one-box, and the client has to be told so.
#
# Left unset, the record-log client falls back to a metaserver address nothing is listening on,
# and every call fails with `record_log_remote_execute_failed: http error: Connection refused`
# -- while a direct curl to the datanode on TS_SERVER_ADDR answers perfectly, which makes it read
# like a gateway fault rather than a missing sentinel. `local` is one of the accepted
# no-metaserver sentinels ("", local, none, standalone, off).
#
# Measured: with the sentinel set, ingest through the gateway is ~75 ms and gated correct;
# without it every ingest returns a backend error in ~11 ms.
export TS_META_ADDR="${TS_META_ADDR:-local}"
export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-local}"

# The blob tier is the datanode, so point at the datanode.
#
# Unset, the gateway defaults to http://127.0.0.1:17102 -- which its own source calls
# "a working-looking address that is not the one anybody" runs. In a one-box nothing listens
# there, so every resource upload fails with
#   blob_store_unreachable: Could not reach the blob tier at http://127.0.0.1:17102
# and the message ingest path, which needs no blob, keeps working -- so the deployment looks
# healthy while every skill and file upload 502s. Measured: 71 of 71 skill ingests failed this
# way while message ingest ran at a 53 ms p50.
#
# Derived from TS_SERVER_ADDR so the two cannot drift apart.
export MATRIXARK_DATANODE_BLOB_URL="${MATRIXARK_DATANODE_BLOB_URL:-http://${TS_SERVER_ADDR}}"
export MATRIXARK_TS_BLOB_URL="${MATRIXARK_TS_BLOB_URL:-http://${TS_SERVER_ADDR}}"
export MATRIXARK_DATANODE_URL="${MATRIXARK_DATANODE_URL:-http://${TS_SERVER_ADDR}}"

# The whole memory budget belongs to one process here.
export TS_CACHE_MEMORY_BYTES="${TS_CACHE_MEMORY_BYTES:-1073741824}"

# One-box serves the ranking from the embeddings: dense 1.00, sparse 0.00.
export MATRIXARK_ONEBOX_EMBEDDING_FIRST="${MATRIXARK_ONEBOX_EMBEDDING_FIRST:-1}"

# Cap the number of glibc allocation arenas.
#
# Unset, glibc gives a threaded process up to 8 arenas per core and grows each in 64 MiB steps
# it never returns. The gateway runs 16 threads on 8 vCPU, and a soak measured 14 such arenas
# holding 896 MiB -- 42% of its 2,151 MiB RSS -- while the live Python heap was a fraction of
# that. This is fragmentation across arenas, not retention. It must be exported BEFORE the
# process starts; glibc reads it once at startup.
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"

# Extraction provider: an OpenAI-compatible endpoint.
#
# The key is READ FROM A FILE, never written here -- this script is committed. Absent the file,
# extraction is left unconfigured rather than half-configured.
#
# Endpoint note, measured: the BigModel chat path is /api/paas/v4, NOT /api/v1. Against /api/v1
# the same key lists models happily and then refuses every completion with "No permission to
# access model", which reads like an entitlement problem and is actually a wrong path.
export MATRIXARK_EXTRACTION_BASE_URL="${MATRIXARK_EXTRACTION_BASE_URL:-https://open.bigmodel.cn/api/paas/v4}"
export MATRIXARK_EXTRACTION_MODEL="${MATRIXARK_EXTRACTION_MODEL:-glm-4-flash}"
export MATRIXARK_EXTRACTION_API_KEY_ENV="${MATRIXARK_EXTRACTION_API_KEY_ENV:-GLM_API_KEY}"
_ts_glm_key_file="${MATRIXARK_EXTRACTION_KEY_FILE:-${HOME:-/home/ubuntu}/.glm_key}"
if [ -z "${GLM_API_KEY:-}" ] && [ -r "${_ts_glm_key_file}" ]; then
  GLM_API_KEY="$(cat "${_ts_glm_key_file}")"
  export GLM_API_KEY
fi
unset _ts_glm_key_file

TS_PROFILE_EXPECT=one-box ts_profile_launch
