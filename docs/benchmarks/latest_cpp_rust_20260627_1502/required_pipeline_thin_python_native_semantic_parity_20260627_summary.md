# Thin Python Native Semantic Parity Proof

Run ID: `thin_python_native_semantic_parity_20260627`
All OK: `True`
Comparison status: `passed`

## What This Proves

- Python MCP dispatches C++/Rust native ContextPack requests for TemporalStore backends by default.
- C++ and Rust both return final ContextPack data plus native traversal/filter/pack telemetry.
- Python reference scan/filter/pack is local/debug fallback only and can be explicitly disabled/enabled by policy.
- Selected-ref count differences are preserved as ranking telemetry, not treated as a missing native hot-path blocker.

## Comparison

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

## cpp

- OK: `True`
- Elapsed ms: `1642.34`
- Memory refs: `{"entity": 1, "event": 2, "segment": 1, "summary": 10}`
- Resource refs: `{"entity": 6, "event": 1, "resource_fact": 3, "summary": 5}`
- Skill refs: `{"entity": 2, "resource_chunk": 1, "resource_fact": 2, "skill_section": 1, "summary": 5}`

## rust

- OK: `True`
- Elapsed ms: `81689.05`
- Memory refs: `{"entity": 1, "event": 3, "segment": 1, "summary": 11}`
- Resource refs: `{"entity": 7, "resource_chunk": 1, "summary": 7}`
- Skill refs: `{"entity": 1, "event": 1, "resource_chunk": 1, "skill_section": 1, "summary": 5}`
