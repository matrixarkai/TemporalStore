# MatrixArk Required Pipeline Parity

Run ID: `latest_cpp_rust_20260627_1502_rerun`
All OK: `False`
Comparison: `skipped`

## Required Pipeline

```text
ingest -> extraction -> async summary refresh -> tree traversal using node_l0/node_l1
-> secondary-index prefilter -> retrieve events/entities/resources/skills
-> ContextPack -> audit/replay
```

## cpp

- OK: `False`
- Elapsed ms: `939.76`
- Error: `cpp failed required_record_types_present`

## rust

- OK: `True`
- Elapsed ms: `958.14`
- Storage prefix: `matrixark:required-pipeline:rust:latest_cpp_rust_20260627_1502_rerun`
- Record counts: `{"context_child_ref": 7, "context_debug_record": 45, "context_embedding": 83, "context_entity": 17, "context_entity_update_audit": 9, "context_event": 31, "context_extraction_audit": 1, "context_index": 149, "context_node": 10, "context_pack_audit": 3, "context_pack_telemetry": 3, "context_recall_reinforcement": 75, "context_segment": 4, "context_summary": 24, "context_summary_dirty": 16, "context_summary_refresh_audit": 10, "matrixark_audit_log": 12, "matrixark_metric": 2, "resource_chunk": 4, "resource_import_task": 6, "resource_manifest": 1, "resource_registry": 1, "session_buffer_event": 3, "skill_manifest": 1, "skill_registry": 1, "skill_section": 2}`
- Embedding types: `batch_l0, entity_state, event_text, node_l0, node_l1, resource_chunk, resource_l0, segment_text, session_l0, skill_l0, skill_section, skill_summary`
- Summary types: `batch_l0, node_l0, node_l1, resource_l0, session_l0, skill_l0`
- Memory selected counts: `{"compression": 0, "entity": 3, "event": 23, "resource_chunk": 2, "resource_entity_fact": 0, "resource_fact": 2, "segment": 1, "skill_section": 0, "summary": 0}`
- Resource selected counts: `{"compression": 0, "entity": 2, "event": 23, "resource_chunk": 2, "resource_entity_fact": 0, "resource_fact": 2, "segment": 1, "skill_section": 0, "summary": 0}`
- Skill selected counts: `{"compression": 0, "entity": 0, "event": 23, "resource_chunk": 1, "resource_entity_fact": 0, "resource_fact": 2, "segment": 0, "skill_section": 2, "summary": 0}`
- Memory tree: `{"candidate_records_after_tree": 296, "children_scoring_policy": "score_all_children_up_to_hard_cap_then_split_node_layers", "cold_events_represented_by_compression": false, "enabled": true, "fallback_reason": "", "fallback_to_flat": false, "hard_max_children_scored_per_parent": 100000, "leaf_record_fetch_policy": "events/entities/resources/skills/compressions scanned only inside selected L0/L1 folders", "max_candidates_per_node": 256, "max_children_scored_per_parent": 100000, "max_raw_events_per_node": 256, "max_selected_refs": 256, "raw_events_dropped_by_time_window": 0, "records_dropped_by_node_fanout": 0, "records_dropped_by_tree": 0, "selected_leaf_count": 4, "selected_node_count": 10, "selected_path_count": 10, "summary_embeddings": ["node_l0", "node_l1"], "top_k_per_layer": 8}`
- Resource tree: `{"candidate_records_after_tree": 295, "children_scoring_policy": "score_all_children_up_to_hard_cap_then_split_node_layers", "cold_events_represented_by_compression": false, "enabled": true, "fallback_reason": "", "fallback_to_flat": false, "hard_max_children_scored_per_parent": 100000, "leaf_record_fetch_policy": "events/entities/resources/skills/compressions scanned only inside selected L0/L1 folders", "max_candidates_per_node": 256, "max_children_scored_per_parent": 100000, "max_raw_events_per_node": 256, "max_selected_refs": 256, "raw_events_dropped_by_time_window": 0, "records_dropped_by_node_fanout": 0, "records_dropped_by_tree": 0, "selected_leaf_count": 4, "selected_node_count": 10, "selected_path_count": 10, "summary_embeddings": ["node_l0", "node_l1"], "top_k_per_layer": 8}`
- Skill tree: `{"candidate_records_after_tree": 314, "children_scoring_policy": "score_all_children_up_to_hard_cap_then_split_node_layers", "cold_events_represented_by_compression": false, "enabled": true, "fallback_reason": "", "fallback_to_flat": false, "hard_max_children_scored_per_parent": 100000, "leaf_record_fetch_policy": "events/entities/resources/skills/compressions scanned only inside selected L0/L1 folders", "max_candidates_per_node": 256, "max_children_scored_per_parent": 100000, "max_raw_events_per_node": 256, "max_selected_refs": 256, "raw_events_dropped_by_time_window": 0, "records_dropped_by_node_fanout": 0, "records_dropped_by_tree": 0, "selected_leaf_count": 4, "selected_node_count": 10, "selected_path_count": 10, "summary_embeddings": ["node_l0", "node_l1"], "top_k_per_layer": 8}`
- Audit actions: `context.batch_extract, context.ingest, context.refresh_summaries, context.replay, context.retrieve`

## C++ Vs Rust Comparison

```json
{
  "reason": "need both cpp and rust",
  "status": "skipped"
}
```
