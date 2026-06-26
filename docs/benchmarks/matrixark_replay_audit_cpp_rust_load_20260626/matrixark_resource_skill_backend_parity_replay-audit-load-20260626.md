# MatrixArk Resource And Skill Backend Parity

Run ID: replay-audit-load-20260626
All OK: True
Comparison: passed

## What Was Tested

- Markdown, text, and PDF resource ingestion
- SKILL.md parsing into manifest and sections
- ResourceManifest, ResourceChunk, SkillManifest, SkillSection writes
- L0 summaries and embeddings for resources and skills
- ContextIndex entries for resource and skill filtering
- ResourceRegistry and SkillRegistry list APIs
- ContextPack retrieval for resource chunks and selected skill instructions
- Skill disable/update behavior
- Cross-user scope isolation

## cpp

- OK: True
- Status: ready
- Storage prefix: matrixark:resource-skill-parity:cpp:replay-audit-load-20260626
- Readiness status: ready
- Readiness attempts: 1
- Final topology: `{"metaserver": "127.0.0.1:18000", "namespace": "deploy_ns", "storage_prefix": "matrixark:resource-skill-parity:cpp:replay-audit-load-20260626", "table": "deploy_table", "warmup_field": "107935:1782499919248:6707811118145807255", "warmup_key": "matrixark:resource-skill-parity:cpp:replay-audit-load-20260626:readiness"}`
- Resource count: 3
- Skill count: 1
- Resource selected refs: 13
- Skill selected refs: 10
- Disabled skill selected refs: 8
- Cross-user selected refs: 0
- Record counts: {"context_child_ref": 6, "context_embedding": 70, "context_entity": 15, "context_event": 19, "context_index": 213, "context_node": 8, "context_pack_audit": 2, "context_summary": 26, "context_summary_dirty": 22, "context_summary_refresh_audit": 9, "matrixark_audit_log": 12, "matrixark_metric": 4, "resource_chunk": 8, "resource_import_task": 12, "resource_manifest": 3, "resource_registry": 3, "session_buffer_event": 4, "skill_manifest": 1, "skill_registry": 1, "skill_section": 2}
- Embedding types: entity_state, event_text, node_l0, node_l1, resource_chunk, resource_l0, session_l0, skill_l0, skill_section, skill_summary
- Index names: classification:new_event, entity_type:resource_api_contract, entity_type:resource_approval, entity_type:resource_cost, entity_type:resource_fact, entity_type:resource_owner, entity_type:resource_policy, entity_type:resource_troubleshooting, event_type:dialogue_batch, event_type:resource_api_contract, event_type:resource_approval, event_type:resource_cost, event_type:resource_owner, event_type:resource_policy, event_type:resource_troubleshooting_step, heading_slug:context-debugger, heading_slug:gpu-runbook, heading_slug:rollback, heading_slug:steps, keyword:approval

- Resource replay audit: True
- Skill replay audit: True
- Resource replay selected refs: 13
- Skill replay selected refs: 10

- Resource tree traversal: `{"enabled": true, "fallback_reason": "", "fallback_to_flat": false, "max_children_scored_per_parent": 10000, "selected_leaf_count": 4, "selected_node_count": 8, "selected_path_count": 8, "summary_embeddings": ["node_l0", "node_l1"], "top_k_per_layer": 8}`
- Resource secondary-index filter: `{"applied_before_embedding_scoring": true, "dropped_candidate_count": 17, "effective_mode": "any_group", "enabled": true, "matched_candidate_count": 25, "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group", "required_groups": [["classification:confirmation", "classification:resource_fact", "entity_type:approval_state", "entity_type:confirmation", "entity_type:resource_fact", "event_type:confirmation", "event_type:resource_approval_fact", "segment_topic:approval_budget", "source_type:resource", "source_type:resource_fact"], ["source_type:resource", "source_type:resource_fact"], ["keyword:approval", "keyword:budget", "keyword:finance", "keyword:requests", "keyword:require", "keyword:says"]]}`
- Skill tree traversal: `{"enabled": true, "fallback_reason": "", "fallback_to_flat": false, "max_children_scored_per_parent": 10000, "selected_leaf_count": 4, "selected_node_count": 8, "selected_path_count": 8, "summary_embeddings": ["node_l0", "node_l1"], "top_k_per_layer": 8}`
- Skill secondary-index filter: `{"applied_before_embedding_scoring": true, "dropped_candidate_count": 20, "effective_mode": "any_group", "enabled": true, "matched_candidate_count": 22, "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group", "required_groups": [["keyword:evidence", "keyword:helps", "keyword:inspect", "keyword:refs", "keyword:replay", "keyword:selected"], ["source_type:skill"], ["skill_tool:replay"], ["skill_trigger:evidence", "skill_trigger:helps", "skill_trigger:helps_inspect", "skill_trigger:helps_inspect_selected", "skill_trigger:inspect", "skill_trigger:inspect_selected", "skill_trigger:inspect_selected_refs", "skill_trigger:refs", "skill_trigger:refs_replay", "skill_trigger:refs_replay_evidence", "skill_trigger:replay", "skill_trigger:replay_evidence", "skill_trigger:selected", "skill_trigger:selected_refs", "skill_trigger:selected_refs_replay"], ["source_type:feedback", "source_type:message"]]}`
- Resource selected ref counts: `{"compression": 0, "entity": 0, "event": 1, "resource_chunk": 6, "resource_entity_fact": 0, "resource_fact": 6, "segment": 0, "skill_section": 0, "summary": 0}`
- Skill selected ref counts: `{"compression": 0, "entity": 0, "event": 1, "resource_chunk": 1, "resource_entity_fact": 0, "resource_fact": 6, "segment": 0, "skill_section": 2, "summary": 0}`

- Admin audit count: 15
- Admin audit actions: context.ingest, context.refresh_summaries, context.replay, context.retrieve, resource.list, skill.list, skill.update
- Admin audit resource/skill/replay: True / True / True

## rust

- OK: True
- Status: ready
- Storage prefix: matrixark:resource-skill-parity:rust:replay-audit-load-20260626
- Readiness status: ready
- Readiness attempts: 1
- Final topology: `{"metaserver": "127.0.0.1:18000", "namespace": "deploy_ns", "storage_prefix": "matrixark:resource-skill-parity:rust:replay-audit-load-20260626", "table": "deploy_table", "warmup_field": "107996:1782499920723:6765721983035223422", "warmup_key": "matrixark:resource-skill-parity:rust:replay-audit-load-20260626:readiness"}`
- Resource count: 3
- Skill count: 1
- Resource selected refs: 13
- Skill selected refs: 10
- Disabled skill selected refs: 8
- Cross-user selected refs: 0
- Record counts: {"context_child_ref": 6, "context_embedding": 81, "context_entity": 15, "context_event": 19, "context_index": 213, "context_node": 8, "context_pack_audit": 2, "context_summary": 37, "context_summary_dirty": 22, "context_summary_refresh_audit": 14, "matrixark_audit_log": 12, "matrixark_metric": 4, "resource_chunk": 8, "resource_import_task": 12, "resource_manifest": 3, "resource_registry": 3, "session_buffer_event": 4, "skill_manifest": 1, "skill_registry": 1, "skill_section": 2}
- Embedding types: entity_state, event_text, node_l0, node_l1, resource_chunk, resource_l0, session_l0, skill_l0, skill_section, skill_summary
- Index names: classification:new_event, entity_type:resource_api_contract, entity_type:resource_approval, entity_type:resource_cost, entity_type:resource_fact, entity_type:resource_owner, entity_type:resource_policy, entity_type:resource_troubleshooting, event_type:dialogue_batch, event_type:resource_api_contract, event_type:resource_approval, event_type:resource_cost, event_type:resource_owner, event_type:resource_policy, event_type:resource_troubleshooting_step, heading_slug:context-debugger, heading_slug:gpu-runbook, heading_slug:rollback, heading_slug:steps, keyword:approval

- Resource replay audit: True
- Skill replay audit: True
- Resource replay selected refs: 13
- Skill replay selected refs: 10

- Resource tree traversal: `{"enabled": true, "fallback_reason": "", "fallback_to_flat": false, "max_children_scored_per_parent": 10000, "selected_leaf_count": 4, "selected_node_count": 8, "selected_path_count": 8, "summary_embeddings": ["node_l0", "node_l1"], "top_k_per_layer": 8}`
- Resource secondary-index filter: `{"applied_before_embedding_scoring": true, "dropped_candidate_count": 17, "effective_mode": "any_group", "enabled": true, "matched_candidate_count": 25, "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group", "required_groups": [["classification:confirmation", "classification:resource_fact", "entity_type:approval_state", "entity_type:confirmation", "entity_type:resource_fact", "event_type:confirmation", "event_type:resource_approval_fact", "segment_topic:approval_budget", "source_type:resource", "source_type:resource_fact"], ["source_type:resource", "source_type:resource_fact"], ["keyword:approval", "keyword:budget", "keyword:finance", "keyword:requests", "keyword:require", "keyword:says"]]}`
- Skill tree traversal: `{"enabled": true, "fallback_reason": "", "fallback_to_flat": false, "max_children_scored_per_parent": 10000, "selected_leaf_count": 4, "selected_node_count": 8, "selected_path_count": 8, "summary_embeddings": ["node_l0", "node_l1"], "top_k_per_layer": 8}`
- Skill secondary-index filter: `{"applied_before_embedding_scoring": true, "dropped_candidate_count": 20, "effective_mode": "any_group", "enabled": true, "matched_candidate_count": 22, "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group", "required_groups": [["keyword:evidence", "keyword:helps", "keyword:inspect", "keyword:refs", "keyword:replay", "keyword:selected"], ["source_type:skill"], ["skill_tool:replay"], ["skill_trigger:evidence", "skill_trigger:helps", "skill_trigger:helps_inspect", "skill_trigger:helps_inspect_selected", "skill_trigger:inspect", "skill_trigger:inspect_selected", "skill_trigger:inspect_selected_refs", "skill_trigger:refs", "skill_trigger:refs_replay", "skill_trigger:refs_replay_evidence", "skill_trigger:replay", "skill_trigger:replay_evidence", "skill_trigger:selected", "skill_trigger:selected_refs", "skill_trigger:selected_refs_replay"], ["source_type:feedback", "source_type:message"]]}`
- Resource selected ref counts: `{"compression": 0, "entity": 0, "event": 1, "resource_chunk": 6, "resource_entity_fact": 0, "resource_fact": 6, "segment": 0, "skill_section": 0, "summary": 0}`
- Skill selected ref counts: `{"compression": 0, "entity": 0, "event": 1, "resource_chunk": 1, "resource_entity_fact": 0, "resource_fact": 6, "segment": 0, "skill_section": 2, "summary": 0}`

- Admin audit count: 15
- Admin audit actions: context.ingest, context.refresh_summaries, context.replay, context.retrieve, resource.list, skill.list, skill.update
- Admin audit resource/skill/replay: True / True / True

## C++ Vs Rust Comparison

{
  "checks": {
    "embedding_types_equal": true,
    "resource_ref_types_equal": true,
    "resource_registry_count_equal": true,
    "skill_ref_types_equal": true,
    "skill_registry_count_equal": true
  },
  "status": "passed"
}
