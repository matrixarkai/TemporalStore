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
  "total_source_count": 43,
  "accepted_sources": 43,
  "failed_sources": 0,
  "retrieved_block_count": 45,
  "multi_agent_scan_ready": true,
  "fanout_namespace_node_candidates": 43,
  "fanout_event_expanded_nodes": 16,
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
  ]
}
```

## What This Proves

- The broader Rust harness ingests resources, skills, and conversations through TemporalStore context models.
- The scan reduces fanout from namespace candidates to bounded expanded nodes.
- Current-agent context is selected with agent-aware locality while user, workspace, and global shared resources remain visible.
- Peer-agent candidates are present but capped out of expansion in this current-agent scan.
- Secondary indexes and selected references remain active in the same scale run.

## Honest Limits

This is local Rust harness evidence, not a multi-process distributed scale proof. It strengthens the multi-agent scan and colocation policy evidence while broader end-to-end deployment scale remains separate.
