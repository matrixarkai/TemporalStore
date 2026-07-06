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
  "selected_skill_count": 3,
  "selected_skill_names": [
    "benchmark-reader",
    "context-debug",
    "payments-incident"
  ],
  "selected_skill_owner_scopes": [
    "team:benchmarks",
    "team:context",
    "team:payments"
  ],
  "selected_skill_trigger_terms": [
    "checkout",
    "context",
    "injection",
    "latency",
    "rollback",
    "summary"
  ],
  "selected_skill_allowed_tool_matches": 3,
  "resource_import_kinds": {
    "git_repo": 1,
    "markdown": 1,
    "pdf": 1,
    "url": 1
  },
  "resource_owner_scopes": [
    "team:benchmarks",
    "team:context",
    "team:payments",
    "team:platform"
  ],
  "resource_parser_names": [
    "context-scale-harness"
  ],
  "multi_agent_scan_ready": true,
  "multi_agent_scan_blockers": [],
  "fanout_namespace_node_candidates": 44,
  "fanout_summary_embedding_query_nodes": 40,
  "fanout_summary_pruned_peer_agent_nodes": 4,
  "fanout_summary_pruned_colocation_group_counts": {
    "user:user": 4
  },
  "fanout_summary_pruned_colocation_scope_counts": {
    "agent:claude": 4
  },
  "fanout_configured_summary_node_limit": 32,
  "fanout_effective_summary_node_limit": 32,
  "fanout_configured_event_node_limit": 16,
  "fanout_effective_event_node_limit": 16,
  "fanout_configured_peer_agent_node_limit": 0,
  "fanout_event_expanded_nodes": 16,
  "fanout_skipped_summary_budget_node_count": 24,
  "fanout_skipped_colocation_group_counts": {
    "user:user": 13,
    "workspace:context": 11
  },
  "fanout_skipped_colocation_scope_counts": {
    "agent:codex": 13,
    "workspace:context": 11
  },
  "fanout_selected_colocation_group_count": 3,
  "fanout_selected_colocation_scope_count": 4,
  "fanout_selected_colocation_groups": [
    "global",
    "user:user",
    "workspace:context"
  ],
  "fanout_selected_colocation_group_counts": {
    "global": 1,
    "user:user": 12,
    "workspace:context": 3
  },
  "fanout_max_selected_colocation_group_nodes": 12,
  "fanout_colocation_group_fanout_reduced": true,
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
  "fanout_selected_colocation_scope_distribution": {
    "agent:codex": 11,
    "global": 1,
    "user:user": 1,
    "workspace:context": 3
  },
  "fanout_current_agent_first_selected": true,
  "fanout_avoided_namespace_replication_nodes": 28,
  "fanout_reduction_percent": 63,
  "fanout_namespace_replication_avoided": true,
  "fanout_candidate_current_agent_nodes": 24,
  "fanout_candidate_peer_agent_nodes": 4,
  "fanout_candidate_user_shared_nodes": 1,
  "fanout_candidate_workspace_shared_nodes": 14,
  "fanout_candidate_global_shared_nodes": 1,
  "fanout_candidate_shared_node_count": 16,
  "fanout_candidate_shared_scope_coverage_count": 3,
  "fanout_candidate_scope_pressure_ready": true,
  "fanout_colocation_group_candidate_counts": {
    "global": 1,
    "user:user": 29,
    "workspace:context": 14
  },
  "fanout_selected_current_agent_nodes": 11,
  "fanout_peer_agent_nodes": 4,
  "fanout_selected_peer_agent_nodes": 0,
  "fanout_skipped_peer_agent_nodes": 4,
  "fanout_peer_agent_limit_applied": true,
  "fanout_selected_user_shared_nodes": 1,
  "fanout_selected_workspace_shared_nodes": 3,
  "fanout_selected_global_shared_nodes": 1,
  "fanout_shared_selected_node_count": 5,
  "fanout_selected_current_agent_percent": 68,
  "fanout_selected_shared_layer_percent": 31,
  "fanout_selected_peer_agent_percent": 0,
  "fanout_shared_scope_coverage_count": 3,
  "fanout_shared_scope_coverage_ready": true,
  "fanout_current_agent_boost_percent": 68,
  "fanout_current_agent_boost_bounded": true,
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
  "fanout_source_class_candidate_counts": {
    "conversation": 31,
    "resource": 7,
    "skill": 6
  },
  "fanout_selected_source_class_counts": {
    "conversation": 13,
    "resource": 2,
    "skill": 1
  },
  "fanout_requested_source_classes": [
    "resource",
    "skill"
  ],
  "fanout_selected_requested_source_class_counts": {
    "resource": 2,
    "skill": 1
  },
  "fanout_requested_source_class_coverage_ready": true,
  "fanout_required_scan_scope_keys": [
    "agent:codex",
    "workspace:context",
    "user:user",
    "global"
  ],
  "fanout_scan_policy_current_agent_scope_key": "agent:codex",
  "fanout_scan_policy_owner_scope_key": "workspace:context",
  "fanout_scan_policy_shared_scope_keys": [
    "global",
    "user:user"
  ],
  "fanout_scan_policy_implicit_current_agent_scope_added": true,
  "fanout_scan_policy_owner_scope_included": true,
  "fanout_scan_policy_shared_scopes_included": true,
  "fanout_scan_policy_ready": true,
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
  "selected_ref_source_class_counts": {
    "conversation": 37,
    "resource": 6,
    "skill": 2
  },
  "selected_source_class_coverage_ready": true,
  "selected_ref_source_ref_samples": [
    "agent:codex:scale-conversation-18",
    "scope:workspace:context|https://docs.example.com/context/openviking-debug#paragraph=1",
    "scope:workspace:context|skills/benchmark-reader/SKILL.md#heading=document"
  ],
  "fanout_selected_ref_current_agent_first": true,
  "fanout_selected_peer_agent_ref_count": 0,
  "fanout_injection_source_class_counts": {
    "conversation": 37,
    "resource": 6,
    "skill": 2
  },
  "fanout_injection_source_class_coverage_ready": true,
  "fanout_injection_current_agent_first": true
}
```

## What This Proves

- The broader Rust harness ingests resources, skills, and conversations through TemporalStore context models.
- Shared resource/skill provenance is named in the archive: markdown, URL, git repo, and PDF resources are parsed by `context-scale-harness` across payments, context, platform, and benchmark owner scopes.
- Retrieval-time skill selection is also named: `payments-incident`, `context-debug`, and `benchmark-reader` are selected with matching trigger terms and allowed-tool evidence.
- The scan reduces fanout from namespace candidates to bounded expanded nodes.
- Peer-agent capping now happens before summary embedding lookup: 4 peer-agent nodes are pruned from summary scoring in this scale run.
- The scale scan avoids full namespace replication: 28 candidate nodes are left unexpanded, a 63% fanout reduction across 3 selected graph groups and 4 selected scope keys.
- The graph-group pressure map is complete: `user:user` is reduced from 29 candidate nodes to 12 selected nodes and `workspace:context` is reduced from 14 to 3, while `global` remains covered.
- The phase split is explicit: 4 peer-agent nodes are pruned before summary scoring and 24 remaining nodes are skipped by the summary/event budget.
- Configured and effective scan budgets are explicit: scale scan uses summary node limit 32, event node limit 16, and peer-agent node limit 0.
- Candidate pressure is scope-aware before selection: 24 current-agent nodes, 4 peer-agent nodes, and 16 shared nodes across user/workspace/global are classified before fanout pruning.
- Source-class pressure is explicit: fanout sees 31 conversation nodes, 7 resource nodes, and 6 skill nodes, then expands a bounded mix of 13 conversation, 2 resource, and 1 skill nodes.
- Requested source-class coverage is explicit: the query asks for resource and skill evidence, and fanout selects both before event expansion.
- Shared-layer coverage is now enforced from the core retrieval report: all 3 required shared scopes are covered while the fill phase can select extra high-value workspace nodes.
- Current-agent context is selected with agent-aware locality while user, workspace, and global shared resources remain visible.
- Selection percentages are explicit in the core report: this scale scan is 68% current-agent, 31% shared-layer, and 0% peer-agent.
- Current-agent boost is explicit and bounded: 11 of 16 expanded nodes are `agent:codex`, giving a 68% current-agent boost while 5 shared nodes still satisfy the shared-layer quota.
- The selected colocation distribution is exact: 11 current-agent nodes, 3 workspace nodes, 1 user node, and 1 global node, with zero peer-agent nodes.
- Shared-scope coverage is explicit: user, workspace, and global layers are all represented in the selected node set.
- The selected colocation scope set proves the expanded scale scan covers `agent:codex`, `user:user`, `workspace:context`, and `global`.
- Locality key count equals expanded nodes: 16 expanded nodes produce 16 locality keys, covering exactly `agent:codex`, `user:user`, `workspace:context`, and `global` with zero peer-agent locality keys.
- Selected refs and injection ordering start with `agent:codex`, proving current-agent context stays first after final retrieval scoring, not only during fanout planning.
- Selected refs and injection both include all source classes: 37 conversation refs, 6 resource refs, and 2 skill refs, with samples showing a current-agent conversation, an OpenViking debug resource URL, and a benchmark-reader skill file.
- Required scan scopes are policy-derived: `agent:codex` is added implicitly for the current agent, `workspace:context` is included from owner scope, and `user:user`/`global` come from the shared resource policy.
- Peer-agent candidates are present but capped out of expansion in this current-agent scan.
- Secondary indexes and selected references remain active in the same scale run.

## Honest Limits

This is local Rust harness evidence, not a multi-process distributed scale proof. It strengthens the multi-agent scan and colocation policy evidence while broader end-to-end deployment scale remains separate.
