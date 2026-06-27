# MatrixArk Required Pipeline Parity

Run ID: `thin_python_native_semantic_parity_20260627`
All OK: `True`
Comparison: `passed`

## Required Pipeline

```text
ingest -> extraction -> async summary refresh -> tree traversal using node_l0/node_l1
-> secondary-index prefilter -> retrieve events/entities/resources/skills
-> ContextPack -> audit/replay
```

## cpp

- OK: `True`
- Elapsed ms: `1642.34`
- Storage prefix: `matrixark:required-pipeline:cpp:thin_python_native_semantic_parity_20260627`
- Record counts: `{"context_child_ref": 7, "context_debug_record": 45, "context_embedding": 83, "context_entity": 17, "context_entity_update_audit": 9, "context_event": 31, "context_extraction_audit": 1, "context_index": 149, "context_node": 10, "context_pack_audit": 3, "context_segment": 4, "context_summary": 24, "context_summary_dirty": 16, "context_summary_refresh_audit": 10, "matrixark_audit_log": 12, "matrixark_metric": 2, "resource_chunk": 4, "resource_import_task": 6, "resource_manifest": 1, "resource_registry": 1, "session_buffer_event": 3, "skill_manifest": 1, "skill_registry": 1, "skill_section": 2}`
- Embedding types: `batch_l0, entity_state, event_text, node_l0, node_l1, resource_chunk, resource_l0, segment_text, session_l0, skill_l0, skill_section, skill_summary`
- Summary types: `batch_l0, node_l0, node_l1, resource_l0, session_l0, skill_l0`
- Memory selected counts: `{"entity": 1, "event": 2, "segment": 1, "summary": 10}`
- Resource selected counts: `{"entity": 6, "event": 1, "resource_fact": 3, "summary": 5}`
- Skill selected counts: `{"entity": 2, "resource_chunk": 1, "resource_fact": 2, "skill_section": 1, "summary": 5}`
- Memory tree: `{"candidate_records_after_tree": 76, "enabled": true, "fallback_to_flat": false, "native_backend": true, "selected_leaf_count": 6, "selected_node_count": 6, "summary_embeddings": ["node_l0", "node_l1"]}`
- Resource tree: `{"candidate_records_after_tree": 76, "enabled": true, "fallback_to_flat": false, "native_backend": true, "selected_leaf_count": 3, "selected_node_count": 3, "summary_embeddings": ["node_l0", "node_l1"]}`
- Skill tree: `{"candidate_records_after_tree": 73, "enabled": true, "fallback_to_flat": false, "native_backend": true, "selected_leaf_count": 5, "selected_node_count": 5, "summary_embeddings": ["node_l0", "node_l1"]}`
- Audit actions: `context.batch_extract, context.ingest, context.refresh_summaries, context.replay, context.retrieve`

## rust

- OK: `True`
- Elapsed ms: `81689.05`
- Storage prefix: `matrixark:required-pipeline:rust:thin_python_native_semantic_parity_20260627`
- Record counts: `{"context_child_ref": 7, "context_debug_record": 45, "context_embedding": 96, "context_entity": 17, "context_entity_update_audit": 9, "context_event": 31, "context_extraction_audit": 1, "context_index": 149, "context_node": 10, "context_pack_audit": 3, "context_segment": 4, "context_summary": 37, "context_summary_dirty": 16, "context_summary_refresh_audit": 18, "matrixark_audit_log": 17, "matrixark_metric": 2, "resource_chunk": 4, "resource_import_task": 6, "resource_manifest": 1, "resource_registry": 1, "session_buffer_event": 3, "skill_manifest": 1, "skill_registry": 1, "skill_section": 2}`
- Embedding types: `batch_l0, entity_state, event_text, node_l0, node_l1, resource_chunk, resource_l0, segment_text, session_l0, skill_l0, skill_section, skill_summary`
- Summary types: `batch_l0, node_l0, node_l1, resource_l0, session_l0, skill_l0`
- Memory selected counts: `{"entity": 1, "event": 3, "segment": 1, "summary": 11}`
- Resource selected counts: `{"entity": 7, "resource_chunk": 1, "summary": 7}`
- Skill selected counts: `{"entity": 1, "event": 1, "resource_chunk": 1, "skill_section": 1, "summary": 5}`
- Memory tree: `{"enabled": true, "fallback_to_flat": false, "native_backend": true, "selected_leaf_count": 5, "selected_node_count": 5, "summary_embeddings": ["node_l0", "node_l1"]}`
- Resource tree: `{"enabled": true, "fallback_to_flat": false, "native_backend": true, "selected_leaf_count": 4, "selected_node_count": 4, "summary_embeddings": ["node_l0", "node_l1"]}`
- Skill tree: `{"enabled": true, "fallback_to_flat": false, "native_backend": true, "selected_leaf_count": 4, "selected_node_count": 4, "summary_embeddings": ["node_l0", "node_l1"]}`
- Audit actions: `context.batch_extract, context.ingest, context.refresh_summaries, context.replay, context.retrieve`

## C++ Vs Rust Comparison

```json
{
  "checks": {
    "cpp_semantic_retrieval_parity": true,
    "embedding_types_equal": true,
    "required_embedding_types_present": true,
    "required_record_types_present": true,
    "required_summary_types_present": true,
    "rust_semantic_retrieval_parity": true,
    "summary_types_equal": true
  },
  "cpp_semantic_checks": {
    "memory_has_entity": true,
    "memory_has_event_or_segment_or_summary": true,
    "resource_has_fact_or_chunk": true,
    "secondary_index_prefilter_enabled": true,
    "skill_has_skill_section": true,
    "tree_traversal_enabled": true
  },
  "exact_selected_ref_counts_match": false,
  "ranking_telemetry_note": "Selected ref counts are telemetry, not a byte-identical parity gate. C++ and Rust pass when both satisfy required semantic retrieval coverage, tree traversal, secondary-index prefilter, ContextPack, audit, and replay.",
  "rust_semantic_checks": {
    "memory_has_entity": true,
    "memory_has_event_or_segment_or_summary": true,
    "resource_has_fact_or_chunk": true,
    "secondary_index_prefilter_enabled": true,
    "skill_has_skill_section": true,
    "tree_traversal_enabled": true
  },
  "selected_ref_count_deltas": {
    "memory": {
      "entity": {
        "cpp": 1,
        "delta_rust_minus_cpp": 0,
        "rust": 1
      },
      "event": {
        "cpp": 2,
        "delta_rust_minus_cpp": 1,
        "rust": 3
      },
      "segment": {
        "cpp": 1,
        "delta_rust_minus_cpp": 0,
        "rust": 1
      },
      "summary": {
        "cpp": 10,
        "delta_rust_minus_cpp": 1,
        "rust": 11
      }
    },
    "resource": {
      "entity": {
        "cpp": 6,
        "delta_rust_minus_cpp": 1,
        "rust": 7
      },
      "event": {
        "cpp": 1,
        "delta_rust_minus_cpp": -1,
        "rust": 0
      },
      "resource_chunk": {
        "cpp": 0,
        "delta_rust_minus_cpp": 1,
        "rust": 1
      },
      "resource_fact": {
        "cpp": 3,
        "delta_rust_minus_cpp": -3,
        "rust": 0
      },
      "summary": {
        "cpp": 5,
        "delta_rust_minus_cpp": 2,
        "rust": 7
      }
    },
    "skill": {
      "entity": {
        "cpp": 2,
        "delta_rust_minus_cpp": -1,
        "rust": 1
      },
      "event": {
        "cpp": 0,
        "delta_rust_minus_cpp": 1,
        "rust": 1
      },
      "resource_chunk": {
        "cpp": 1,
        "delta_rust_minus_cpp": 0,
        "rust": 1
      },
      "resource_fact": {
        "cpp": 2,
        "delta_rust_minus_cpp": -2,
        "rust": 0
      },
      "skill_section": {
        "cpp": 1,
        "delta_rust_minus_cpp": 0,
        "rust": 1
      },
      "summary": {
        "cpp": 5,
        "delta_rust_minus_cpp": 0,
        "rust": 5
      }
    }
  },
  "status": "passed"
}
```
