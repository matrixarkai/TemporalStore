# Rust Multi-Agent Context Scan Colocation Evidence

## Summary

This document records the Rust TemporalStore evidence for multi-layer context scanning across current-agent, user-shared, workspace-shared, and global-shared context. The goal is bounded fanout with useful colocation: keep peer-agent and current-agent evidence at equal default boost, keep user/global resources and skills visible, and avoid colocating an entire namespace on every node.

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
  "summary_pruned_colocation_group_counts": {
    "user:user": 2
  },
  "summary_pruned_colocation_scope_counts": {
    "agent:claude": 2
  },
  "configured_summary_node_limit": 11,
  "effective_summary_node_limit": 11,
  "configured_event_node_limit": 4,
  "effective_event_node_limit": 4,
  "configured_peer_agent_node_limit": 0,
  "event_expanded_nodes": 4,
  "skipped_summary_budget_node_count": 7,
  "skipped_colocation_group_counts": {
    "user:user": 7
  },
  "skipped_colocation_scope_counts": {
    "agent:codex": 7
  },
  "selected_colocation_group_count": 3,
  "selected_colocation_scope_count": 4,
  "selected_colocation_groups": [
    "global",
    "user:user",
    "workspace:context"
  ],
  "selected_colocation_group_counts": {
    "global": 1,
    "user:user": 2,
    "workspace:context": 1
  },
  "max_selected_colocation_group_nodes": 2,
  "colocation_group_fanout_reduced": true,
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
  "selected_colocation_scope_distribution": {
    "agent:codex": 1,
    "global": 1,
    "user:user": 1,
    "workspace:context": 1
  },
  "current_agent_first_selected": true,
  "avoided_namespace_replication_nodes": 9,
  "fanout_reduction_percent": 69,
  "namespace_replication_avoided": true,
  "candidate_current_agent_nodes": 8,
  "candidate_peer_agent_nodes": 2,
  "candidate_user_shared_nodes": 1,
  "candidate_workspace_shared_nodes": 1,
  "candidate_global_shared_nodes": 1,
  "candidate_shared_node_count": 3,
  "candidate_shared_scope_coverage_count": 3,
  "candidate_scope_pressure_ready": true,
  "colocation_group_candidate_counts": {
    "global": 1,
    "user:user": 11,
    "workspace:context": 1
  },
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
  "selected_shared_layer_nodes": 3,
  "selected_current_agent_percent": 25,
  "selected_shared_layer_percent": 75,
  "selected_peer_agent_percent": 0,
  "required_shared_scope_count": 3,
  "selected_shared_scope_coverage_count": 3,
  "shared_scope_coverage_ready": true,
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
  "scan_policy_current_agent_scope_key": "agent:codex",
  "scan_policy_owner_scope_key": "workspace:context",
  "scan_policy_shared_scope_keys": [
    "global",
    "user:user"
  ],
  "scan_policy_implicit_current_agent_scope_added": true,
  "scan_policy_owner_scope_included": true,
  "scan_policy_shared_scopes_included": true,
  "scan_policy_ready": true,
  "locality_keys": [
    "tenant:20260706:scope:agent:codex:node:8424729405653484612",
    "tenant:20260706:scope:workspace:context:node:16379766558787635764",
    "tenant:20260706:scope:user:user:node:12205718754729647577",
    "tenant:20260706:scope:global:node:12994693500116009283"
  ],
  "locality_key_count": 4,
  "locality_scope_keys": [
    "agent:codex",
    "global",
    "user:user",
    "workspace:context"
  ],
  "peer_locality_key_count": 0,
  "retrieved_block_count": 12,
  "retrieved_current_agent_block_count": 3,
  "retrieved_user_shared_block_count": 3,
  "retrieved_workspace_shared_block_count": 3,
  "retrieved_global_shared_block_count": 3,
  "retrieved_event_count": 4,
  "selected_ref_count": 12,
  "selected_ref_scope_keys": [
    "agent:codex",
    "global",
    "user:user",
    "workspace:context"
  ],
  "selected_ref_current_agent_first": true,
  "selected_peer_agent_ref_count": 0,
  "injection_current_agent_first": true,
  "current_agent_id": "codex"
}
```

## What This Proves

- The retrieval path uses Rust TemporalStore ingestion, extraction, storage, summary embeddings, and retrieval.
- Fanout is reduced from namespace candidates to bounded selected nodes.
- Peer-agent capping now happens before summary embedding lookup: 2 peer-agent nodes are pruned from summary scoring in this focused scan.
- The scan avoids full namespace replication: 9 candidate nodes are left unexpanded, a 69% fanout reduction across 3 selected graph groups and 4 selected scope keys.
- The graph-group pressure map is complete: `user:user` is reduced from 11 candidate nodes to 2 selected nodes, while `global` and `workspace:context` remain covered.
- The phase split is explicit: 2 peer-agent nodes are pruned before summary scoring and 7 current-agent overflow nodes are skipped by the summary/event budget.
- Configured and effective scan budgets are explicit: focused scan uses summary node limit 11, event node limit 4, and peer-agent node limit 0.
- Candidate pressure is scope-aware before selection: 8 current-agent nodes, 2 peer-agent nodes, and 3 shared nodes across user/workspace/global are classified before fanout pruning.
- Shared-layer coverage is now reported by the core retrieval path: all 3 required shared scopes are selected, with 3 selected user/workspace/global shared nodes.
- Selection percentages are explicit in the core report: this tight focused scan is 25% current-agent, 75% shared-layer, and 0% peer-agent.
- The selected nodes include current-agent, user-shared, workspace-shared, and global-shared layers.
- The selected colocation scope set proves the expanded scan covers `agent:codex`, `user:user`, `workspace:context`, and `global`.
- The selected colocation distribution is exact and balanced in the focused gate: one selected node per current-agent, user, workspace, and global scope.
- The selected colocation scope order starts with `agent:codex`, proving current-agent context gets the first expansion slot before shared resources.
- Retrieved block coverage is scope-aware: returned context includes current-agent, user-shared, workspace-shared, and global-shared blocks, not only selected node metadata.
- Selected refs and injection ordering now start with `agent:codex`, proving the current-agent boost survives from fanout selection into prompt-facing evidence ordering.
- Required scan scopes are policy-derived: `agent:codex` is added implicitly for the current agent, `workspace:context` is included from owner scope, and `user:user`/`global` come from the shared resource policy.
- Locality keys are producer-aware: current-agent context is scoped as `agent:codex`, while shared resources stay in `user:user`, `workspace:context`, and `global` groups instead of colocating the whole namespace.
- Locality key count equals expanded nodes: 4 expanded nodes produce 4 locality keys, covering exactly `agent:codex`, `user:user`, `workspace:context`, and `global` with zero peer-agent locality keys.
- Layer quotas are applied before expansion so shared resources are not crowded out by many current-agent matches.
- Peer-agent candidates are counted under the same default agent-scope boost as current-agent candidates, so the tight focused scan keeps agent plus user/workspace/global shared layers bounded and visible without duplicating the whole namespace.

## Honest Limits

This is a fast focused harness for the multi-agent scan policy. The broader full context workflow harness still needs separate runtime tuning before it can be used as the only end-to-end scale proof for every context subsystem in one run.
