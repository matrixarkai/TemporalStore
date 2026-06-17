#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-raft-distributed-parity-target}"
ARTIFACT_DIR="${TS_RAFT_PARITY_ARTIFACT_DIR:-/tmp/temporalstore-raft-distributed-parity-$(date +%s)-$$}"
TIMEOUT="${TS_RAFT_PARITY_TIMEOUT:-180s}"

cd "${ROOT}"
export CARGO_TARGET_DIR="${TARGET_DIR}"
mkdir -p "${ARTIFACT_DIR}"

echo "== 1/4 data-node distributed Raft replication, membership, snapshot =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin distributed_raft_harness -- \
  --root "${ARTIFACT_DIR}/distributed-raft" \
  > "${ARTIFACT_DIR}/distributed-raft.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-validation \
  --log "${ARTIFACT_DIR}/distributed-raft.json"

echo "== 2/4 data-node secondary replication, restart, partition, failover =="
cargo build -p temporalstore-rust --bins
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin raft_secondary_replication_harness -- \
  --root "${ARTIFACT_DIR}/raft-secondary" \
  --heartbeat-ms "${TS_RAFT_PARITY_HEARTBEAT_MS:-25}" \
  > "${ARTIFACT_DIR}/raft-secondary.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-secondary-validation \
  --log "${ARTIFACT_DIR}/raft-secondary.json"

echo "== 3/4 metaserver Raft membership, read-index, snapshot, failover =="
timeout "${TIMEOUT}" cargo run -p temporalstore-rust --bin metaserver_raft_harness -- \
  --root "${ARTIFACT_DIR}/metaserver-raft" \
  > "${ARTIFACT_DIR}/metaserver-raft.json"
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-metaserver-raft-validation \
  --log "${ARTIFACT_DIR}/metaserver-raft.json"

echo "== 4/4 combined Raft parity summary =="
python3 - <<PY
import json
from pathlib import Path

artifact_dir = Path("${ARTIFACT_DIR}")

def load(name):
    text = (artifact_dir / name).read_text(encoding="utf-8")
    return json.loads(text[text.find("{"):text.rfind("}") + 1])

distributed = load("distributed-raft.json")
secondary = load("raft-secondary.json")
metaserver = load("metaserver-raft.json")

summary = {
    "production_ready_slice": True,
    "artifact_dir": str(artifact_dir),
    "data_node": {
        "distributed_node_count": len(distributed["nodes"]),
        "distributed_all_nodes_have_majority": all(
            node["status"]["has_majority"] and node["apply_health"]["healthy"]
            for node in distributed["nodes"]
        ),
        "replica_read_values": [read["value"] for read in distributed["replica_reads"]],
        "follower_write_rejected": not distributed["follower_write_rejection"]["ok"],
        "scale_down_voters": [item["voters"] for item in distributed["scale_down"]],
        "scale_up_voters": [item["voters"] for item in distributed["scale_up"]],
        "external_snapshot_read": distributed["external_snapshot_read"]["value"],
        "secondary_restart_reads": secondary["reads_after_restart"],
        "partition_isolated_read_rejected": not secondary["partition"]["isolated_read_status"]["ok"],
        "lagging_follower_observed_lag": secondary["lagging_follower"]["observed_lag"],
        "leader_crash_failover_ok": secondary["failover"]["status"]["ok"],
        "post_leader_crash_values": [read["value"] for read in secondary["reads_after_leader_crash"]],
    },
    "metaserver": {
        "initial_membership": metaserver["initial_membership"],
        "membership_after_add": metaserver["membership_after_add"],
        "membership_after_remove": metaserver["membership_after_remove"],
        "unsupported_role_rejected": metaserver["unsupported_role_rejected"],
        "wait_for_log_applied_index": metaserver["wait_for_log_applied_index"],
        "snapshot_index": metaserver["snapshot_index"],
        "snapshot_restore_read": metaserver["snapshot_restore_read"],
        "leader_after_transfer": metaserver["leader_after_transfer"],
        "leader_after_failover": metaserver["leader_after_failover"],
        "namespace_after_failover_visible": metaserver["namespace_after_failover_visible"],
        "unavailable_without_majority": metaserver["unavailable_without_majority"],
    },
}
(artifact_dir / "raft-distributed-parity.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\\n",
    encoding="utf-8",
)
print(json.dumps(summary, indent=2, sort_keys=True))
PY
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-raft-distributed-parity-validation \
  --log "${ARTIFACT_DIR}/raft-distributed-parity.json"

echo "TemporalStore distributed Raft parity gate passed."
echo "Artifacts: ${ARTIFACT_DIR}"
