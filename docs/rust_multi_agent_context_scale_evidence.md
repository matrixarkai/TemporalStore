# Rust Multi-Agent Context Scale Evidence

## Summary

This document records the Rust TemporalStore resource/skill/conversation scale evidence for multi-agent context scanning. It proves the broader harness, not only the tiny focused scan, keeps current-agent context boosted while user, workspace, and global shared resources remain visible and peer-agent context is bounded instead of expanding the whole namespace.

## Command

```bash
cargo build -p temporalstore-rust --bin context_workflow_harness
TEMPORALSTORE_CONTEXT_WORKFLOW_ROOT=/tmp/temporalstore-context-resource-skill-scale \
TEMPORALSTORE_CONTEXT_RESOURCE_SKILL_SCALE_ONLY=1 \
target/debug/context_workflow_harness > /tmp/context_resource_skill_scale_summary.json
```

## Validation

```bash
python3 tools/validate_context_resource_skill_scale.py docs/benchmark_archives/context_resource_skill_scale_20260706_summary.json
```

## Evidence

Archived report: `docs/benchmark_archives/context_resource_skill_scale_20260706_summary.json`

```json
{
  "ready": true,
  "total_source_count": 44,
  "accepted_sources": 44,
  "failed_sources": 0,
  "retrieved_block_count": 45,
  "retrieved_current_agent_block_count": 31,
  "retrieved_user_shared_block_count": 3,
  "retrieved_workspace_shared_block_count": 8,
  "retrieved_global_shared_block_count": 3,
  "multi_agent_scan_ready": true,
  "fanout_namespace_node_candidates": 44,
  "fanout_summary_embedding_query_nodes": 40,
  "fanout_summary_pruned_peer_agent_nodes": 4,
  "fanout_event_expanded_nodes": 16,
  "fanout_selected_colocation_group_count": 4,
  "fanout_selected_colocation_groups": [
    "global",
    "user:user",
    "workspace:context"
  ],
  "fanout_selected_colocation_scope_keys": [
    "agent:codex",
    "global",
    "user:user",
    "workspace:context"
  ],
  "fanout_selected_colocation_scope_order": [
    "agent:codex",
    "workspace:context",
    "user:user",
    "global",
    "agent:codex"
  ],
  "fanout_current_agent_first_selected": true,
  "fanout_avoided_namespace_replication_nodes": 28,
  "fanout_reduction_percent": 63,
  "fanout_namespace_replication_avoided": true,
  "fanout_selected_current_agent_nodes": 11,
  "fanout_peer_agent_nodes": 4,
  "fanout_selected_peer_agent_nodes": 0,
  "fanout_skipped_peer_agent_nodes": 4,
  "fanout_peer_agent_limit_applied": true,
  "fanout_selected_user_shared_nodes": 1,
  "fanout_selected_workspace_shared_nodes": 3,
  "fanout_selected_global_shared_nodes": 1,
  "fanout_shared_layer_quota_nodes": 4,
  "fanout_layer_quota_applied": true,
  "fanout_scan_layers": [
    "agent",
    "global",
    "user",
    "workspace"
  ],
  "fanout_colocation_groups": [
    "global",
    "user:user",
    "workspace:context"
  ],
  "fanout_colocation_scope_keys": [
    "agent:claude",
    "agent:codex",
    "global",
    "user:user",
    "workspace:context"
  ],
  "fanout_required_scan_scope_keys": [
    "agent:codex",
    "workspace:context",
    "user:user",
    "global"
  ],
  "fanout_locality_key_count": 16,
  "fanout_locality_scope_keys": [
    "agent:codex",
    "global",
    "user:user",
    "workspace:context"
  ],
  "fanout_peer_locality_key_count": 0,
  "fanout_selected_ref_scope_keys": [
    "agent:codex",
    "global",
    "user:user",
    "workspace:context"
  ],
  "fanout_selected_ref_current_agent_first": true,
  "fanout_selected_peer_agent_ref_count": 0,
  "fanout_injection_current_agent_first": true
}
```

## What This Proves

- The broader Rust harness ingests resources, skills, and conversations through TemporalStore context models.
- The scan reduces fanout from namespace candidates to bounded expanded nodes.
- Peer-agent capping now happens before summary embedding lookup: 4 peer-agent nodes are pruned from summary scoring in this scale run.
- The scale scan avoids full namespace replication: 28 candidate nodes are left unexpanded, a 63% fanout reduction across 4 selected colocation groups.
- Current-agent context is selected with agent-aware locality while user, workspace, and global shared resources remain visible.
- The selected colocation scope set proves the expanded scale scan covers `agent:codex`, `user:user`, `workspace:context`, and `global`.
- Locality key count equals expanded nodes: 16 expanded nodes produce 16 locality keys, covering exactly `agent:codex`, `user:user`, `workspace:context`, and `global` with zero peer-agent locality keys.
- Selected refs and injection ordering start with `agent:codex`, proving current-agent context stays first after final retrieval scoring, not only during fanout planning.
- Required scan scopes are derived from current-agent plus owner-scope policy, so shared user/global resources do not depend on every caller spelling out the full scope list.
- Peer-agent candidates are present but capped out of expansion in this current-agent scan.
- Secondary indexes and selected references remain active in the same scale run.

## Honest Limits

This is local Rust harness evidence, not a multi-process distributed scale proof. It strengthens the multi-agent scan and colocation policy evidence while broader end-to-end deployment scale remains separate.
