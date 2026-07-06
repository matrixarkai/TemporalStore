# Rust Multi-Agent Context Scan Colocation Evidence

## Summary

This document records the Rust TemporalStore evidence for multi-layer context scanning across current-agent, user-shared, workspace-shared, and global-shared context. The goal is bounded fanout with useful colocation: boost the current agent, keep user/global resources and skills visible, and avoid colocating an entire namespace on every node.

## Command

```bash
cargo build -p temporalstore-rust --bin context_workflow_harness
TEMPORALSTORE_CONTEXT_WORKFLOW_ROOT=/tmp/temporalstore-context-multiagent-scan \
TEMPORALSTORE_CONTEXT_MULTI_AGENT_SCAN_ONLY=1 \
target/debug/context_workflow_harness > /tmp/context_multiagent_scan_summary.json
```

## Validation

```bash
python3 tools/validate_context_multi_agent_scan.py docs/benchmark_archives/context_multiagent_scan_20260706_summary.json
```

## Evidence

Archived report: `docs/benchmark_archives/context_multiagent_scan_20260706_summary.json`

```json
{
  "ready": true,
  "namespace_node_candidates": 13,
  "event_expanded_nodes": 4,
  "fanout_reduced": true,
  "layer_quota_applied": true,
  "shared_layer_quota_nodes": 4,
  "selected_current_agent_nodes": 1,
  "peer_agent_nodes": 2,
  "selected_peer_agent_nodes": 0,
  "skipped_peer_agent_nodes": 2,
  "selected_user_shared_nodes": 1,
  "selected_workspace_shared_nodes": 1,
  "selected_global_shared_nodes": 1,
  "scan_layers": [
    "agent",
    "global",
    "user",
    "workspace"
  ],
  "colocation_groups": [
    "global",
    "user:user",
    "workspace:context"
  ],
  "locality_keys": [
    "tenant:20260706:scope:user:user:node:8424729405653484612",
    "tenant:20260706:scope:workspace:context:node:16379766558787635764",
    "tenant:20260706:scope:user:user:node:12205718754729647577",
    "tenant:20260706:scope:global:node:12994693500116009283"
  ],
  "retrieved_block_count": 12,
  "retrieved_event_count": 4,
  "selected_ref_count": 12,
  "current_agent_id": "codex"
}
```

## What This Proves

- The retrieval path uses Rust TemporalStore ingestion, extraction, storage, summary embeddings, and retrieval.
- Fanout is reduced from namespace candidates to bounded selected nodes.
- The selected nodes include current-agent, user-shared, workspace-shared, and global-shared layers.
- Locality keys are scoped by colocation group instead of colocating the whole namespace.
- Layer quotas are applied before expansion so shared resources are not crowded out by many current-agent matches.
- Peer-agent candidates are counted but skipped in the tight focused scan so current-agent plus user/workspace/global shared layers remain bounded and visible.

## Honest Limits

This is a fast focused harness for the multi-agent scan policy. The broader full context workflow harness still needs separate runtime tuning before it can be used as the only end-to-end scale proof for every context subsystem in one run.
