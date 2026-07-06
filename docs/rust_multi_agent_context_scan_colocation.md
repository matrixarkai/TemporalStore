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
  "summary_embedding_query_nodes": 11,
  "summary_pruned_peer_agent_nodes": 2,
  "event_expanded_nodes": 4,
  "selected_colocation_group_count": 4,
  "selected_colocation_groups": [
    "global",
    "user:user",
    "workspace:context"
  ],
  "selected_colocation_scope_keys": [
    "agent:codex",
    "global",
    "user:user",
    "workspace:context"
  ],
  "selected_colocation_scope_order": [
    "agent:codex",
    "workspace:context",
    "user:user",
    "global"
  ],
  "current_agent_first_selected": true,
  "avoided_namespace_replication_nodes": 9,
  "fanout_reduction_percent": 69,
  "namespace_replication_avoided": true,
  "fanout_reduced": true,
  "layer_quota_applied": true,
  "shared_layer_quota_nodes": 4,
  "selected_current_agent_nodes": 1,
  "peer_agent_nodes": 2,
  "selected_peer_agent_nodes": 0,
  "skipped_peer_agent_nodes": 2,
  "peer_agent_limit_applied": true,
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
  "colocation_scope_keys": [
    "agent:claude",
    "agent:codex",
    "global",
    "user:user",
    "workspace:context"
  ],
  "required_scan_scope_keys": [
    "agent:codex",
    "workspace:context",
    "user:user",
    "global"
  ],
  "locality_keys": [
    "tenant:20260706:scope:agent:codex:node:8424729405653484612",
    "tenant:20260706:scope:workspace:context:node:16379766558787635764",
    "tenant:20260706:scope:user:user:node:12205718754729647577",
    "tenant:20260706:scope:global:node:12994693500116009283"
  ],
  "retrieved_block_count": 12,
  "retrieved_current_agent_block_count": 3,
  "retrieved_user_shared_block_count": 3,
  "retrieved_workspace_shared_block_count": 3,
  "retrieved_global_shared_block_count": 3,
  "retrieved_event_count": 4,
  "selected_ref_count": 12,
  "current_agent_id": "codex"
}
```

## What This Proves

- The retrieval path uses Rust TemporalStore ingestion, extraction, storage, summary embeddings, and retrieval.
- Fanout is reduced from namespace candidates to bounded selected nodes.
- Peer-agent capping now happens before summary embedding lookup: 2 peer-agent nodes are pruned from summary scoring in this focused scan.
- The scan avoids full namespace replication: 9 candidate nodes are left unexpanded, a 69% fanout reduction across 4 selected colocation groups.
- The selected nodes include current-agent, user-shared, workspace-shared, and global-shared layers.
- The selected colocation scope set proves the expanded scan covers `agent:codex`, `user:user`, `workspace:context`, and `global`.
- The selected colocation scope order starts with `agent:codex`, proving current-agent context gets the first expansion slot before shared resources.
- Retrieved block coverage is scope-aware: returned context includes current-agent, user-shared, workspace-shared, and global-shared blocks, not only selected node metadata.
- Required scan scopes are derived from the current agent and owner scope, so user and global shared layers stay visible even when callers do not manually pass every shared scope.
- Locality keys are producer-aware: current-agent context is scoped as `agent:codex`, while shared resources stay in `user:user`, `workspace:context`, and `global` groups instead of colocating the whole namespace.
- Layer quotas are applied before expansion so shared resources are not crowded out by many current-agent matches.
- Peer-agent candidates are counted and capped by `max_peer_agent_nodes`, so the tight focused scan keeps current-agent plus user/workspace/global shared layers bounded and visible without duplicating the whole namespace.

## Honest Limits

This is a fast focused harness for the multi-agent scan policy. The broader full context workflow harness still needs separate runtime tuning before it can be used as the only end-to-end scale proof for every context subsystem in one run.
