# MatrixArk Operators And Time Compression Validation

This doc records the local validation for MatrixArk context operators and time compression.
The test uses the Python MatrixArk runtime with TemporalStore-shaped records. C++ TemporalStore already has native `COMPRESS_EVENTS`, `WRITE_COMPRESSION_EVENT`, `QUERY_COMPRESSION_EVENTS`, and `QUERY_NODE_CONTEXT` tests; this run proves the MatrixArk extraction/retrieval path can also use compressed windows in a ContextPack.

## Command

```bash
PYTHONPATH=. python3 tools/run_matrixark_operator_compression_test.py
PYTHONPATH=. python3 -m unittest tools.test_matrixark_mcp_server
```

## Result

```json
{
  "compression_query_count": 1,
  "record_counts": {
    "context_compression_event": 1,
    "context_embedding": 37,
    "context_event": 3,
    "context_pack_audit": 1,
    "context_summary": 33,
    "matrixark_audit_log": 4,
    "session_buffer_event": 3
  },
  "statistical": {
    "AVG": 13.333333,
    "COUNT": 3,
    "MAX": 25.0,
    "SUM": 40.0
  },
  "status": "passed"
}
```

## Operators Covered

| Operator | Status | Notes |
| --- | --- | --- |
| `COUNT` | Passed | Counts records: `3`. |
| `SUM` | Passed | Sums numeric values: `40.0`. |
| `AVG` | Passed | Averages numeric values: `13.333333`. |
| `MAX` | Passed | Max numeric value: `25.0`. |
| `LATEST` | Passed | Latest record chosen by `updated_at_ms`: `new`. |
| `DECAY_SCORE` | Passed | Emits `origin_score`, `time_score`, `business_score`, `final_score`, and formula. |
| `LLM_MERGE` | Covered | Entity update tests cover deterministic field patches and `ContextEntity` merge/update audit; production LLM merge remains provider-backed. |
| `TIME_COMPRESS` | Passed | Writes a source-linked compressed window and retrieval selects it in a tight ContextPack. |
| `VALID_AS_OF` | Covered in C++ | `QuerySummaries` / `QueryNodeContext` tests select summaries as of a timestamp. |
| `BLOCK_IF_STALE` | Policy covered | Stale-blocker packing policy is present; destructive pruning is not enabled in MVP. |

## TIME_COMPRESS Debug Data

```json
{
  "compression_id_hash": 8849697440636724129,
  "selected_in_context_pack": true,
  "selected_ref": {
    "business_score": 0.5,
    "embedding_score": 0.499999,
    "event_type": "time_compress",
    "final_score": 0.827226,
    "keyword_score": 5,
    "node_hash": 9049490510714952373,
    "node_path": [
      "account:acct_dev",
      "tenant:tenant_dev",
      "principal:user:alice",
      "collection:sessions",
      "session:operator-window"
    ],
    "node_score": 0.785048,
    "operator": "TIME_COMPRESS",
    "origin_score": 0.9317519999999999,
    "packing_policy": "fact",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_time_compression",
    "ref_hash": 8849697440636724129,
    "ref_type": "compression",
    "scope": {
      "account_id": "acct_dev",
      "session_hash": 7101050098322077057,
      "session_id": "operator-window",
      "team": "infra",
      "tenant_hash": 5019518775864453950,
      "tenant_id": "tenant_dev",
      "user_hash": 4964277056467908383,
      "user_id": "alice"
    },
    "score": 0.827226,
    "source_end_ms": 1782119740794,
    "source_event_ids": [
      1264545819372063038,
      6457433297074149587
    ],
    "source_start_ms": 1782119740784,
    "sparse_score": 1.0,
    "text": "TIME_COMPRESS: Temporal compression window [1782119740784, 1782119740794] contains 2 selected events plus additional source events. user: Alice approved the old GPU purchase after finance reviewed it. | user: The GPU approval budget was 42000 dollars.",
    "time_score": 1.0,
    "token_estimate": 33,
    "updated_at_ms": 1782119750794
  },
  "source_event_count": 2,
  "source_event_ids": [
    1264545819372063038,
    6457433297074149587
  ],
  "truncated_source_events": true
}
```

Key checks:

- Compression is non-destructive: raw `context_event` records remain in replay.
- The compression record stores `source_event_ids` for audit and replay.
- Retrieval treats the compressed summary as `ref_type = compression`.
- Packing boosts source-linked compression summaries when they cover multiple source events, so old/cold windows can save tokens.

## Selected ContextPack

```json
{
  "recall_policy": {
    "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
    "auxiliary_quota": 2,
    "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
    "secondary_index_filter": {
      "applied_before_embedding_scoring": true,
      "dropped_candidate_count": 1,
      "enabled": true,
      "matched_candidate_count": 2,
      "mode": "AND across groups, OR within each group",
      "required_groups": [
        [
          "classification:confirmation",
          "entity_type:approval_state",
          "entity_type:confirmation",
          "event_type:confirmation",
          "segment_topic:approval_budget"
        ]
      ]
    },
    "time_decay": {
      "freshness_tolerance_ms": 86400000,
      "half_life_ms": 604800000
    },
    "tree_traversal": {
      "enabled": true,
      "fallback_to_flat": false,
      "max_children_scored_per_parent": 10000,
      "selected_leaf_count": 1,
      "selected_node_count": 5,
      "selected_path_count": 5,
      "summary_embeddings": [
        "node_l0",
        "node_l1"
      ],
      "top_k_per_layer": 8
    },
    "weights": {
      "business": 0.25,
      "time": 0.05
    }
  },
  "selected_refs": [
    {
      "business_score": 0.5,
      "embedding_score": 0.499999,
      "event_type": "time_compress",
      "final_score": 0.827226,
      "keyword_score": 5,
      "node_hash": 9049490510714952373,
      "node_path": [
        "account:acct_dev",
        "tenant:tenant_dev",
        "principal:user:alice",
        "collection:sessions",
        "session:operator-window"
      ],
      "node_score": 0.785048,
      "operator": "TIME_COMPRESS",
      "origin_score": 0.9317519999999999,
      "packing_policy": "fact",
      "packing_score": 1.0,
      "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
      "recall_path": "primary_time_compression",
      "ref_hash": 8849697440636724129,
      "ref_type": "compression",
      "scope": {
        "account_id": "acct_dev",
        "session_hash": 7101050098322077057,
        "session_id": "operator-window",
        "team": "infra",
        "tenant_hash": 5019518775864453950,
        "tenant_id": "tenant_dev",
        "user_hash": 4964277056467908383,
        "user_id": "alice"
      },
      "score": 0.827226,
      "source_end_ms": 1782119740794,
      "source_event_ids": [
        1264545819372063038,
        6457433297074149587
      ],
      "source_start_ms": 1782119740784,
      "sparse_score": 1.0,
      "text": "TIME_COMPRESS: Temporal compression window [1782119740784, 1782119740794] contains 2 selected events plus additional source events. user: Alice approved the old GPU purchase after finance reviewed it. | user: The GPU approval budget was 42000 dollars.",
      "time_score": 1.0,
      "token_estimate": 33,
      "updated_at_ms": 1782119750794
    }
  ],
  "used_context_tokens": 33
}
```

## C++ TemporalStore Coverage

Native C++ coverage lives in `src/extension/context/test.cc`:

- `TreeEmbeddingSummaryAndCompressionRoundTrip`
- `TemporalCompressionBuildsReplayableSummaryWithoutDeletingSources`
- decayed event query tests using `decay_half_life_ms`, `min_decayed_score`, and `rank_by_decayed_score`

Those tests prove the storage-side records and time-window query semantics. This MatrixArk validation proves the application-side retrieval and token-packing path can consume the compressed records.
