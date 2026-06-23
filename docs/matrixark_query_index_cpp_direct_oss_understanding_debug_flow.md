# MatrixArk Query, Index, Ingestion, Extraction, Retrieval Debug Flow

## What This Run Proves

- Backend: `temporalstore-direct`.
- Batched 20-message conversations are accepted through `matrixark_batch_extract`.
- ContextEvent, ContextSegment, ContextEntity, ContextSummary, ContextEmbedding, ContextIndex, and ContextPackAudit records are written.
- L0/L1 node summaries are refreshed before retrieval.
- Query text is parsed into a question type and secondary-index filter groups before semantic scoring.
- Real OSS embedding vectors are generated and stored. In this run, the vector dimension is visible in `context_embedding` records.

## Storage Boundary

- backend: `temporalstore-direct`
- temporalstore: `{"metaserver": "127.0.0.1:18000", "namespace": "deploy_ns", "storage_prefix": "matrixark:cpp:query:index:oss-understanding:1782232028871", "table": "deploy_table", "temporalstore_lib": "/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so"}`

## Model Boundary

- embedding_provider: `oss`
- embedding_model: `/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2`
- understanding_provider: `oss_encoder`
- note: This debug run requires real OSS embeddings and OSS-encoder extraction/query understanding. Deterministic extraction, segmentation, and query-understanding are disabled for this run.

## Data Model Counts

- `context_embedding`: 85
- `context_entity`: 14
- `context_entity_update_audit`: 12
- `context_event`: 40
- `context_extraction_audit`: 2
- `context_index`: 16
- `context_pack_audit`: 8
- `context_segment`: 17
- `context_summary`: 14
- `context_summary_dirty`: 6
- `context_summary_refresh_audit`: 6
- `matrixark_audit_log`: 12

## Ingestion API Format

`matrixark_batch_extract` request shape used in this run:
```json
{
  "messages": [
    {
      "role": "user",
      "content": "I moved to Seattle at the start of March."
    },
    {
      "role": "assistant",
      "content": "I will remember Seattle as the earlier location."
    },
    {
      "role": "user",
      "content": "On April 10 I moved to Austin for the storage project."
    },
    {
      "role": "assistant",
      "content": "Austin is now the newer location."
    },
    {
      "role": "user",
      "content": "I prefer Python for dashboards."
    },
    {
      "role": "assistant",
      "content": "Preference noted: Python for dashboards."
    },
    {
      "role": "user",
      "content": "Actually I now prefer Rust for low latency storage engines."
    },
    {
      "role": "assistant",
      "content": "Preference update noted: Rust for low latency storage engines."
    },
    {
      "role": "user",
      "content": "Alice approved the GPU purchase after finance reviewed the budget."
    },
    {
      "role": "assistant",
      "content": "The GPU purchase approval by Alice is confirmed."
    },
    {
      "role": "user",
      "content": "The amount for the GPU purchase is 42000 dollars."
    },
    {
      "role": "assistant",
      "content": "Budget amount recorded as 42000 dollars."
    },
    {
      "role": "user",
      "content": "My manager Priya is helping with the launch plan."
    },
    {
      "role": "assistant",
      "content": "Manager relationship noted: Priya."
    },
    {
      "role": "user",
      "content": "My job role is storage infrastructure lead."
    },
    {
      "role": "assistant",
      "content": "Job status recorded as storage infrastructure lead."
    },
    {
      "role": "user",
      "content": "I plan to visit Berlin next month for the conference."
    },
    {
      "role": "assistant",
      "content": "Current plan recorded: visit Berlin next month."
    },
    {
      "role": "user",
      "content": "Thanks, that is all for this memory batch."
    },
    {
      "role": "assistant",
      "content": "Batch summary can now be committed."
    }
  ],
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_a",
    "session_id": "session_a",
    "tenant_hash": 1875121732827492989,
    "user_hash": 3876043158382393130,
    "session_hash": 8237187342837907451
  },
  "metadata": {
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ]
  },
  "threshold_messages": 20,
  "force": false,
  "skip_prior_context": false,
  "understanding_provider": "oss_encoder",
  "segment_provider": "oss_encoder",
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```

## Retrieval Query Format And Secondary Index Filtering

### Query: Where is the user currently located?
Request:
```json
{
  "query": "Where is the user currently located?",
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_a",
    "session_id": "session_a",
    "tenant_hash": 1875121732827492989,
    "user_hash": 3876043158382393130,
    "session_hash": 8237187342837907451
  },
  "max_context_tokens": 120,
  "ranking": {
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 4
  },
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```
Question type: `current_state`
Secondary-index filter groups inferred from query:
```json
[
  [
    "entity_type:job_status",
    "entity_type:location"
  ],
  [
    "event_type:correction",
    "event_type:status_update"
  ],
  [
    "segment_topic:correction"
  ]
]
```
Candidates passing prefilter: `1`; dropped before scoring: `34`
Tree traversal summary:
```json
{
  "enabled": true,
  "fallback_reason": "",
  "fallback_to_flat": false,
  "max_children_scored_per_parent": 10000,
  "selected_leaf_count": 1,
  "selected_node_count": 3,
  "selected_path_count": 3,
  "summary_embeddings": [
    "node_l0",
    "node_l1"
  ],
  "top_k_per_layer": 8
}
```
Selected refs:
```json
[
  {
    "business_score": 0.76,
    "embedding_score": 0.482336,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "final_score": 0.795911,
    "keyword_score": 2,
    "metadata": {},
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.604196,
    "origin_score": 0.747852,
    "packing_policy": "current_state",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1339818069656098215,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.795911,
    "sparse_score": 0.4,
    "text": "job_status: job_status = job_status: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer l...",
    "time_score": 1.0,
    "token_estimate": 41,
    "updated_at_ms": 1782232029203
  }
]
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 5206067727979080171,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7423521290845491206,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1144979047994807768,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1997586556034887939,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Austin is now the newer location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6390775253570792492,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: I prefer Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5281155962483197587,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2895883390329778773,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5501532726691688082,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8061520447825392871,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1556002124741292395,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: The GPU purchase approval by Alice is confirmed."
  }
]
```
### Query: What does the user prefer for low latency storage now?
Request:
```json
{
  "query": "What does the user prefer for low latency storage now?",
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_a",
    "session_id": "session_a",
    "tenant_hash": 1875121732827492989,
    "user_hash": 3876043158382393130,
    "session_hash": 8237187342837907451
  },
  "max_context_tokens": 120,
  "ranking": {
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 4
  },
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```
Question type: `current_state`
Secondary-index filter groups inferred from query:
```json
[
  [
    "event_type:correction",
    "event_type:plan_update"
  ],
  [
    "segment_topic:correction"
  ],
  [
    "entity_type:current_plan",
    "entity_type:location"
  ]
]
```
Candidates passing prefilter: `21`; dropped before scoring: `14`
Tree traversal summary:
```json
{
  "enabled": true,
  "fallback_reason": "",
  "fallback_to_flat": false,
  "max_children_scored_per_parent": 10000,
  "selected_leaf_count": 1,
  "selected_node_count": 3,
  "selected_path_count": 3,
  "summary_embeddings": [
    "node_l0",
    "node_l1"
  ],
  "top_k_per_layer": 8
}
```
Selected refs:
```json
[
  {
    "business_score": 0.78,
    "embedding_score": 0.222375,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "final_score": 0.780174,
    "keyword_score": 5,
    "metadata": {},
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.662736,
    "origin_score": 0.71429,
    "packing_policy": "current_state",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8370292454781575004,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.780174,
    "sparse_score": 0.5,
    "text": "current_plan: current_plan = current_plan: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer...",
    "time_score": 1.0,
    "token_estimate": 40,
    "updated_at_ms": 1782232029203
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.69059,
    "event_type": "session",
    "final_score": 0.754829,
    "keyword_score": 7,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.662736,
    "origin_score": 0.793049,
    "packing_policy": "current_state",
    "packing_score": 0.754829,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2895883390329778773,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.754829,
    "sparse_score": 0.7,
    "text": "user: Actually I now prefer Rust for low latency storage engines.",
    "time_score": 1.0,
    "token_estimate": 11,
    "updated_at_ms": 1782232029203
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.688212,
    "event_type": "session",
    "final_score": 0.691437,
    "keyword_score": 4,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.662736,
    "origin_score": 0.687395,
    "packing_policy": "current_state",
    "packing_score": 0.691437,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 5501532726691688082,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.691437,
    "sparse_score": 0.4,
    "text": "assistant: Preference update noted: Rust for low latency storage engines.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782232029203
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.305395,
    "event_type": "session",
    "final_score": 0.628272,
    "keyword_score": 4,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.662736,
    "origin_score": 0.58212,
    "packing_policy": "current_state",
    "packing_score": 0.628272,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1144979047994807768,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.628272,
    "sparse_score": 0.4,
    "text": "user: On April 10 I moved to Austin for the storage project.",
    "time_score": 1.0,
    "token_estimate": 12,
    "updated_at_ms": 1782232029203
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.380898,
    "event_type": "session",
    "final_score": 0.59873,
    "keyword_score": 2,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.662736,
    "origin_score": 0.532884,
    "packing_policy": "current_state",
    "packing_score": 0.59873,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 6036342798569964928,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 5206067727979080171,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7423521290845491206,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1144979047994807768,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1997586556034887939,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Austin is now the newer location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6390775253570792492,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I prefer Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5281155962483197587,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2895883390329778773,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5501532726691688082,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8061520447825392871,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1556002124741292395,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: The GPU purchase approval by Alice is confirmed."
  }
]
```
### Query: Who approved the GPU purchase budget?
Request:
```json
{
  "query": "Who approved the GPU purchase budget?",
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_a",
    "session_id": "session_a",
    "tenant_hash": 1875121732827492989,
    "user_hash": 3876043158382393130,
    "session_hash": 8237187342837907451
  },
  "max_context_tokens": 120,
  "ranking": {
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 4
  },
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```
Question type: `date`
Secondary-index filter groups inferred from query:
```json
[
  [
    "segment_topic:approval_budget"
  ],
  [
    "entity_type:approval_state",
    "entity_type:confirmation"
  ],
  [
    "event_type:confirmation",
    "event_type:plan_update"
  ]
]
```
Candidates passing prefilter: `22`; dropped before scoring: `13`
Tree traversal summary:
```json
{
  "enabled": true,
  "fallback_reason": "",
  "fallback_to_flat": false,
  "max_children_scored_per_parent": 10000,
  "selected_leaf_count": 1,
  "selected_node_count": 3,
  "selected_path_count": 3,
  "summary_embeddings": [
    "node_l0",
    "node_l1"
  ],
  "top_k_per_layer": 8
}
```
Selected refs:
```json
[
  {
    "business_score": 0.45,
    "embedding_score": 0.790315,
    "event_type": "session",
    "final_score": 0.798802,
    "keyword_score": 5,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.646674,
    "origin_score": 0.866337,
    "packing_policy": "date",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8061520447825392871,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.798802,
    "sparse_score": 0.8333333333333334,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget.",
    "time_score": 1.0,
    "token_estimate": 11,
    "updated_at_ms": 1782232029203
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.139252,
    "event_type": "session",
    "final_score": 0.551377,
    "keyword_score": 1,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.646674,
    "origin_score": 0.453961,
    "packing_policy": "date",
    "packing_score": 0.831377,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1144979047994807768,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.551377,
    "sparse_score": 0.16666666666666666,
    "text": "user: On April 10 I moved to Austin for the storage project.",
    "time_score": 1.0,
    "token_estimate": 12,
    "updated_at_ms": 1782232029203
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.080321,
    "entity_name": "confirmation",
    "entity_type": "confirmation",
    "final_score": 0.734653,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.646674,
    "origin_score": 0.557755,
    "packing_policy": "date",
    "packing_score": 0.814653,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8754569050557217005,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.734653,
    "sparse_score": 0.16666666666666666,
    "text": "confirmation: confirmation = confirmation: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer...",
    "time_score": 1.0,
    "token_estimate": 40,
    "updated_at_ms": 1782232029203
  },
  {
    "business_score": 0.95,
    "embedding_score": 0.146644,
    "entity_name": "approval_state: user: I moved to Seattle at the start of March. assistant: I wil",
    "entity_type": "approval_state",
    "final_score": 0.734596,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.646674,
    "origin_score": 0.575994,
    "packing_policy": "date",
    "packing_score": 0.814596,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 3735556605684008968,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.734596,
    "sparse_score": 0.16666666666666666,
    "text": "approval_state: approval_state: user: I moved to Seattle at the start of March. assistant: I wil = approval_state: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the new...",
    "time_score": 1.0,
    "token_estimate": 53,
    "updated_at_ms": 1782232029203
  }
]
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 5206067727979080171,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7423521290845491206,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1144979047994807768,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1997586556034887939,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Austin is now the newer location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6390775253570792492,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I prefer Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5281155962483197587,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2895883390329778773,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5501532726691688082,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8061520447825392871,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1556002124741292395,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: The GPU purchase approval by Alice is confirmed."
  }
]
```
### Query: Who is the user's manager?
Request:
```json
{
  "query": "Who is the user's manager?",
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_a",
    "session_id": "session_a",
    "tenant_hash": 1875121732827492989,
    "user_hash": 3876043158382393130,
    "session_hash": 8237187342837907451
  },
  "max_context_tokens": 120,
  "ranking": {
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 4
  },
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```
Question type: `current_state`
Secondary-index filter groups inferred from query:
```json
[
  [
    "entity_type:job_status",
    "entity_type:location",
    "entity_type:relationship"
  ],
  [
    "event_type:plan_update",
    "event_type:status_update"
  ]
]
```
Candidates passing prefilter: `1`; dropped before scoring: `34`
Tree traversal summary:
```json
{
  "enabled": true,
  "fallback_reason": "",
  "fallback_to_flat": false,
  "max_children_scored_per_parent": 10000,
  "selected_leaf_count": 1,
  "selected_node_count": 3,
  "selected_path_count": 3,
  "summary_embeddings": [
    "node_l0",
    "node_l1"
  ],
  "top_k_per_layer": 8
}
```
Selected refs:
```json
[
  {
    "business_score": 0.76,
    "embedding_score": 0.327783,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "final_score": 0.793845,
    "keyword_score": 2,
    "metadata": {},
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.685356,
    "origin_score": 0.744408,
    "packing_policy": "current_state",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1339818069656098215,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.793845,
    "sparse_score": 0.5,
    "text": "job_status: job_status = job_status: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer l...",
    "time_score": 1.0,
    "token_estimate": 41,
    "updated_at_ms": 1782232029203
  }
]
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 5206067727979080171,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7423521290845491206,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1144979047994807768,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1997586556034887939,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Austin is now the newer location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6390775253570792492,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: I prefer Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5281155962483197587,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2895883390329778773,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5501532726691688082,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8061520447825392871,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_event",
    "ref_hash": 1556002124741292395,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:confirmation",
      "entity_type:approval_state",
      "entity_type:confirmation",
      "entity_type:current_plan",
      "entity_type:session",
      "event_type:session",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: The GPU purchase approval by Alice is confirmed."
  }
]
```
### Query: What pet is in the user's family?
Request:
```json
{
  "query": "What pet is in the user's family?",
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_b",
    "session_id": "session_b",
    "tenant_hash": 1875121732827492989,
    "user_hash": 931086106910965805,
    "session_hash": 5917028181796202772
  },
  "max_context_tokens": 120,
  "ranking": {
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 4
  },
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```
Question type: `why_emotion`
Secondary-index filter groups inferred from query:
```json
[
  [
    "entity_type:family_profile",
    "entity_type:preference",
    "entity_type:relationship"
  ],
  [
    "source_type:feedback",
    "source_type:message"
  ]
]
```
Candidates passing prefilter: `23`; dropped before scoring: `13`
Tree traversal summary:
```json
{
  "enabled": true,
  "fallback_reason": "",
  "fallback_to_flat": false,
  "max_children_scored_per_parent": 10000,
  "selected_leaf_count": 1,
  "selected_node_count": 3,
  "selected_path_count": 3,
  "summary_embeddings": [
    "node_l0",
    "node_l1"
  ],
  "top_k_per_layer": 8
}
```
Selected refs:
```json
[
  {
    "business_score": 0.74,
    "embedding_score": 0.524167,
    "entity_name": "relationship: user: My family has a dog named Mochi. assistant: Family profile n",
    "entity_type": "relationship",
    "final_score": 0.798663,
    "keyword_score": 2,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.612526,
    "origin_score": 0.759772,
    "packing_policy": "why_emotion",
    "packing_score": 0.798663,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 3686328392930307948,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.798663,
    "sparse_score": 0.4,
    "text": "relationship: relationship: user: My family has a dog named Mochi. assistant: Family profile n = relationship: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live i...",
    "time_score": 1.0,
    "token_estimate": 50,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.75,
    "embedding_score": 0.504491,
    "entity_name": "preference",
    "entity_type": "preference",
    "final_score": 0.797617,
    "keyword_score": 2,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.612526,
    "origin_score": 0.754361,
    "packing_policy": "why_emotion",
    "packing_score": 0.797617,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 5637488518656154424,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.797617,
    "sparse_score": 0.4,
    "text": "preference: preference = preference: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in ...",
    "time_score": 1.0,
    "token_estimate": 38,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.606886,
    "event_type": "dialogue_batch",
    "final_score": 0.676512,
    "keyword_score": 2,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_b",
        "family_plan_updates"
      ]
    },
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.612526,
    "origin_score": 0.66252,
    "packing_policy": "why_emotion",
    "packing_score": 0.676512,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1320886596389251293,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.676512,
    "sparse_score": 0.4,
    "text": "user: My family has a dog named Mochi.",
    "time_score": 1.0,
    "token_estimate": 8,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.472437,
    "event_type": "dialogue_batch",
    "final_score": 0.612328,
    "keyword_score": 1,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_b",
        "family_plan_updates"
      ]
    },
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.612526,
    "origin_score": 0.555546,
    "packing_policy": "why_emotion",
    "packing_score": 0.612328,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 3685891262800622525,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.612328,
    "sparse_score": 0.2,
    "text": "assistant: Family profile noted: dog named Mochi.",
    "time_score": 1.0,
    "token_estimate": 7,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.17387,
    "event_type": "dialogue_batch",
    "final_score": 0.605065,
    "keyword_score": 2,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_b",
        "family_plan_updates"
      ]
    },
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.612526,
    "origin_score": 0.543441,
    "packing_policy": "why_emotion",
    "packing_score": 0.605065,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 4801651245404203611,
    "ref_type": "event",
    "scope"
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 1320886596389251293,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 3685891262800622525,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Family profile noted: dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4821393749184653598,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My sister Emma lives in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5441864648168132156,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Relationship noted: sister Emma in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6314850742699520865,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I used to live in Boston before the move."
  },
  {
    "record_type": "context_event",
    "ref_hash": 875075073924585621,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Historical location Boston noted."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4801651245404203611,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Now I live in Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 239114935866171225,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Current location Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8894091651616392732,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I like Java for backend prototypes."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7311856666480965345,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Java for backend prototypes."
  }
]
```
### Query: Where does the user currently live?
Request:
```json
{
  "query": "Where does the user currently live?",
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_b",
    "session_id": "session_b",
    "tenant_hash": 1875121732827492989,
    "user_hash": 931086106910965805,
    "session_hash": 5917028181796202772
  },
  "max_context_tokens": 120,
  "ranking": {
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 4
  },
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```
Question type: `current_state`
Secondary-index filter groups inferred from query:
```json
[
  [
    "entity_type:job_status",
    "entity_type:location"
  ],
  [
    "segment_topic:correction"
  ],
  [
    "event_type:correction",
    "event_type:status_update"
  ]
]
```
Candidates passing prefilter: `2`; dropped before scoring: `34`
Tree traversal summary:
```json
{
  "enabled": true,
  "fallback_reason": "",
  "fallback_to_flat": false,
  "max_children_scored_per_parent": 10000,
  "selected_leaf_count": 1,
  "selected_node_count": 3,
  "selected_path_count": 3,
  "summary_embeddings": [
    "node_l0",
    "node_l1"
  ],
  "top_k_per_layer": 8
}
```
Selected refs:
```json
[
  {
    "business_score": 0.76,
    "embedding_score": 0.194238,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "final_score": 0.733884,
    "keyword_score": 2,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.58782,
    "origin_score": 0.644473,
    "packing_policy": "current_state",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 7198407890252352012,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.733884,
    "sparse_score": 0.3333333333333333,
    "text": "job_status: job_status = job_status: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in ...",
    "time_score": 1.0,
    "token_estimate": 38,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 1.0,
    "coordinate_tuples": [
      [
        11,
        11
      ]
    ],
    "embedding_score": 0.057188,
    "final_score": 0.709236,
    "keyword_score": 1,
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.58782,
    "non_contiguous": false,
    "origin_score": 0.5153932,
    "packing_policy": "current_state",
    "packing_score": 0.869236,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 6358915194129015027,
    "ref_type": "segment",
    "saliency_score": 0.627033,
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.709236,
    "sparse_score": 0.16666666666666666,
    "text": "11: Correction recorded: Go is the newer backend prototype preference.",
    "time_score": 1.0,
    "token_estimate": 10,
    "topic": "correction",
    "updated_at_ms": 1782232044824
  }
]
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 1320886596389251293,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 3685891262800622525,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Family profile noted: dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4821393749184653598,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: My sister Emma lives in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5441864648168132156,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Relationship noted: sister Emma in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6314850742699520865,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: I used to live in Boston before the move."
  },
  {
    "record_type": "context_event",
    "ref_hash": 875075073924585621,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Historical location Boston noted."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4801651245404203611,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: Now I live in Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 239114935866171225,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Current location Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8894091651616392732,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "user: I like Java for backend prototypes."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7311856666480965345,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": false,
    "text": "assistant: Preference noted: Java for backend prototypes."
  }
]
```
### Query: What backend language does the user currently prefer?
Request:
```json
{
  "query": "What backend language does the user currently prefer?",
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_b",
    "session_id": "session_b",
    "tenant_hash": 1875121732827492989,
    "user_hash": 931086106910965805,
    "session_hash": 5917028181796202772
  },
  "max_context_tokens": 120,
  "ranking": {
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 4
  },
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```
Question type: `current_state`
Secondary-index filter groups inferred from query:
```json
[
  [
    "entity_type:current_plan",
    "entity_type:preference"
  ],
  [
    "event_type:plan_update"
  ],
  [
    "segment_topic:correction"
  ],
  [
    "source_type:message"
  ]
]
```
Candidates passing prefilter: `23`; dropped before scoring: `13`
Tree traversal summary:
```json
{
  "enabled": true,
  "fallback_reason": "",
  "fallback_to_flat": false,
  "max_children_scored_per_parent": 10000,
  "selected_leaf_count": 1,
  "selected_node_count": 3,
  "selected_path_count": 3,
  "summary_embeddings": [
    "node_l0",
    "node_l1"
  ],
  "top_k_per_layer": 8
}
```
Selected refs:
```json
[
  {
    "business_score": 0.78,
    "embedding_score": 0.135017,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "final_score": 0.684493,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.578832,
    "origin_score": 0.554821,
    "packing_policy": "current_state",
    "packing_score": 0.964493,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1750016207060048809,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.684493,
    "sparse_score": 0.125,
    "text": "current_plan: current_plan = current_plan: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live i...",
    "time_score": 1.0,
    "token_estimate": 38,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.75,
    "embedding_score": 0.165485,
    "entity_name": "preference",
    "entity_type": "preference",
    "final_score": 0.68292,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.578832,
    "origin_score": 0.5631999999999999,
    "packing_policy": "current_state",
    "packing_score": 0.96292,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 5637488518656154424,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.68292,
    "sparse_score": 0.125,
    "text": "preference: preference = preference: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in ...",
    "time_score": 1.0,
    "token_estimate": 38,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 1.0,
    "coordinate_tuples": [
      [
        11,
        11
      ]
    ],
    "embedding_score": 0.365605,
    "final_score": 0.760325,
    "keyword_score": 2,
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.578832,
    "non_contiguous": false,
    "origin_score": 0.6005421999999999,
    "packing_policy": "current_state",
    "packing_score": 0.920325,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 6358915194129015027,
    "ref_type": "segment",
    "saliency_score": 0.627033,
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.760325,
    "sparse_score": 0.25,
    "text": "11: Correction recorded: Go is the newer backend prototype preference.",
    "time_score": 1.0,
    "token_estimate": 10,
    "topic": "correction",
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.493443,
    "event_type": "dialogue_batch",
    "final_score": 0.651533,
    "keyword_score": 3,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_b",
        "family_plan_updates"
      ]
    },
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.578832,
    "origin_score": 0.620888,
    "packing_policy": "current_state",
    "packing_score": 0.651533,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 864243862243558987,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.651533,
    "sparse_score": 0.375,
    "text": "user: Correction: I now prefer Go for backend prototypes.",
    "time_score": 1.0,
    "token_estimate": 9,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.548713,
    "event_type": "dialogue_batch",
    "final_score": 0.634403,
    "keyword_score": 2,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_b",
        "family_plan_updates"
      ]
    },
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.578832,
    "origin_score": 0.592338,
    "packing_policy": "current_state",
    "packing_score": 0.634403,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8894091651616392732,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_h
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 1320886596389251293,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 3685891262800622525,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Family profile noted: dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4821393749184653598,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My sister Emma lives in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5441864648168132156,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Relationship noted: sister Emma in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6314850742699520865,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I used to live in Boston before the move."
  },
  {
    "record_type": "context_event",
    "ref_hash": 875075073924585621,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Historical location Boston noted."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4801651245404203611,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Now I live in Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 239114935866171225,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Current location Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8894091651616392732,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I like Java for backend prototypes."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7311856666480965345,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Java for backend prototypes."
  }
]
```
### Query: What is the user's current role status?
Request:
```json
{
  "query": "What is the user's current role status?",
  "scope": {
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "user_id": "locomo_user_b",
    "session_id": "session_b",
    "tenant_hash": 1875121732827492989,
    "user_hash": 931086106910965805,
    "session_hash": 5917028181796202772
  },
  "max_context_tokens": 120,
  "ranking": {
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 4
  },
  "_matrixark_auth": {
    "mode": "dev",
    "api_key_id": "dev",
    "account_id": "acct_query_index_debug",
    "tenant_id": "tenant_context",
    "role": "dev_admin"
  }
}
```
Question type: `current_state`
Secondary-index filter groups inferred from query:
```json
[
  [
    "event_type:status_update"
  ],
  [
    "entity_type:current_plan",
    "entity_type:job_status",
    "entity_type:location"
  ],
  [
    "segment_topic:correction"
  ]
]
```
Candidates passing prefilter: `23`; dropped before scoring: `13`
Tree traversal summary:
```json
{
  "enabled": true,
  "fallback_reason": "",
  "fallback_to_flat": false,
  "max_children_scored_per_parent": 10000,
  "selected_leaf_count": 1,
  "selected_node_count": 3,
  "selected_path_count": 3,
  "summary_embeddings": [
    "node_l0",
    "node_l1"
  ],
  "top_k_per_layer": 8
}
```
Selected refs:
```json
[
  {
    "business_score": 0.78,
    "embedding_score": 0.116886,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "final_score": 0.689737,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.561673,
    "origin_score": 0.563561,
    "packing_policy": "current_state",
    "packing_score": 0.969737,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1750016207060048809,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.689737,
    "sparse_score": 0.16666666666666666,
    "text": "current_plan: current_plan = current_plan: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live i...",
    "time_score": 1.0,
    "token_estimate": 38,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.76,
    "embedding_score": 0.058083,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "final_score": 0.675634,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.561673,
    "origin_score": 0.54739,
    "packing_policy": "current_state",
    "packing_score": 0.955634,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 7198407890252352012,
    "ref_type": "entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.675634,
    "sparse_score": 0.16666666666666666,
    "text": "job_status: job_status = job_status: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in ...",
    "time_score": 1.0,
    "token_estimate": 38,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 1.0,
    "coordinate_tuples": [
      [
        11,
        11
      ]
    ],
    "embedding_score": 0.117286,
    "final_score": 0.716085,
    "keyword_score": 1,
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.561673,
    "non_contiguous": false,
    "origin_score": 0.5268082,
    "packing_policy": "current_state",
    "packing_score": 0.876085,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 6358915194129015027,
    "ref_type": "segment",
    "saliency_score": 0.627033,
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.716085,
    "sparse_score": 0.16666666666666666,
    "text": "11: Correction recorded: Go is the newer backend prototype preference.",
    "time_score": 1.0,
    "token_estimate": 10,
    "topic": "correction",
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.430487,
    "event_type": "dialogue_batch",
    "final_score": 0.63188,
    "keyword_score": 2,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_b",
        "family_plan_updates"
      ]
    },
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.561673,
    "origin_score": 0.588134,
    "packing_policy": "current_state",
    "packing_score": 0.63188,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1585093639471241877,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "score": 0.63188,
    "sparse_score": 0.3333333333333333,
    "text": "user: My role changed to AI memory platform owner.",
    "time_score": 1.0,
    "token_estimate": 9,
    "updated_at_ms": 1782232044824
  },
  {
    "business_score": 0.45,
    "embedding_score": 0.160593,
    "event_type": "dialogue_batch",
    "final_score": 0.622348,
    "keyword_score": 3,
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_b",
        "family_plan_updates"
      ]
    },
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.561673,
    "origin_score": 0.572247,
    "packing_policy": "current_state",
    "packing_score": 0.622348,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8551639094670922557,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 1320886596389251293,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 3685891262800622525,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Family profile noted: dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4821393749184653598,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My sister Emma lives in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5441864648168132156,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Relationship noted: sister Emma in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6314850742699520865,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I used to live in Boston before the move."
  },
  {
    "record_type": "context_event",
    "ref_hash": 875075073924585621,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Historical location Boston noted."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4801651245404203611,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Now I live in Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 239114935866171225,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Current location Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8894091651616392732,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I like Java for backend prototypes."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7311856666480965345,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:current_plan",
      "entity_type:family_profile",
      "entity_type:relationship",
      "entity_type:session",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Java for backend prototypes."
  }
]
```

## Records By Data Model

### context_embedding
```json
[
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 5206067727979080171,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      0.099127,
      -0.002947,
      0.060389,
      0.021439,
      0.002824,
      0.03914,
      0.040649,
      -0.03877
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 7423521290845491206,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      0.059749,
      0.047139,
      0.003234,
      0.015314,
      -0.02008,
      0.057534,
      0.011932,
      -0.062616
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 1144979047994807768,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      -0.022253,
      -0.002018,
      0.084155,
      0.065894,
      0.015617,
      -0.038025,
      0.002436,
      0.000171
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 1997586556034887939,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      0.009165,
      -0.026346,
      0.09926,
      0.017793,
      -0.026476,
      -0.016948,
      -0.026467,
      -0.073837
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 6390775253570792492,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      -0.081196,
      -0.011496,
      -0.091653,
      -0.02124,
      -0.00858,
      -0.126708,
      -0.00019,
      0.116935
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 5281155962483197587,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      -0.088047,
      0.005048,
      -0.072922,
      -0.002347,
      -0.021332,
      -0.066458,
      0.00259,
      0.101277
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 2895883390329778773,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      -0.07715,
      0.031523,
      -0.015217,
      0.08191,
      0.006523,
      -0.087347,
      -0.012371,
      0.07084
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 5501532726691688082,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      -0.10125,
      0.017615,
      -0.011481,
      0.066641,
      -0.020204,
      -0.057158,
      -0.001992,
      0.055019
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 8061520447825392871,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      -0.028551,
      0.060351,
      -0.082751,
      0.01934,
      0.001187,
      -0.0431,
      0.022453,
      0.043852
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 1556002124741292395,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      -0.111982,
      0.003425,
      -0.017134,
      0.011947,
      -0.027062,
      -0.051534,
      0.003312,
      0.004301
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 628809200610426314,
    "ref_type": "event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "vector_dim": 384,
    "vector_preview": [
      -0.013017,
      0.024708,
      -0.012101,
      -0.028526,
      -0.062533,
      -0.095254,
      -0.015195,
      0.059083
    ]
  },
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "/root/src/github-services/TemporalStore/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
 
```
### context_entity
```json
[
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 3126342872758517950,
    "entity_name": "session: user: I moved to Seattle at the start of March. assistant: I will remem",
    "entity_type": "session",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "state": "session: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer loca..."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 3735556605684008968,
    "entity_name": "approval_state: user: I moved to Seattle at the start of March. assistant: I wil",
    "entity_type": "approval_state",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "state": "approval_state: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the new..."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 8370292454781575004,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "state": "current_plan: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer..."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 8754569050557217005,
    "entity_name": "confirmation",
    "entity_type": "confirmation",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "state": "confirmation: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer..."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 2690897074610495568,
    "entity_name": "preference",
    "entity_type": "preference",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "state": "preference: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer l..."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 1339818069656098215,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "state": "job_status: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer l..."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 5891142747006644318,
    "entity_name": "correction",
    "entity_type": "correction",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "state": "correction: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer l..."
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 3686328392930307948,
    "entity_name": "relationship: user: My family has a dog named Mochi. assistant: Family profile n",
    "entity_type": "relationship",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "state": "relationship: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live i..."
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 1750016207060048809,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "state": "current_plan: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live i..."
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 1939080212215184585,
    "entity_name": "session: user: My family has a dog named Mochi. assistant: Family profile noted:",
    "entity_type": "session",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "state": "session: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in Bos..."
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 3115011876535055373,
    "entity_name": "family_profile",
    "entity_type": "family_profile",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "state": "family_profile: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: 
```
### context_entity_update_audit
```json
[
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 3735556605684008968,
    "entity_name": "approval_state: user: I moved to Seattle at the start of March. assistant: I wil",
    "entity_type": "approval_state",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 8370292454781575004,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 8754569050557217005,
    "entity_name": "confirmation",
    "entity_type": "confirmation",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 2690897074610495568,
    "entity_name": "preference",
    "entity_type": "preference",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 1339818069656098215,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "entity_hash": 5891142747006644318,
    "entity_name": "correction",
    "entity_type": "correction",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 3686328392930307948,
    "entity_name": "relationship: user: My family has a dog named Mochi. assistant: Family profile n",
    "entity_type": "relationship",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 1750016207060048809,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 3115011876535055373,
    "entity_name": "family_profile",
    "entity_type": "family_profile",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 7198407890252352012,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 5637488518656154424,
    "entity_name": "preference",
    "entity_type": "preference",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "entity_hash": 8491721315543744111,
    "entity_name": "correction",
    "entity_type": "correction",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "previous_state": "",
    "record_type": "context_entity_update_audit"
  }
]
```
### context_event
```json
[
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 5206067727979080171,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "user: I moved to Seattle at the start of March.",
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 7423521290845491206,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "assistant: I will remember Seattle as the earlier location.",
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 1144979047994807768,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "user: On April 10 I moved to Austin for the storage project.",
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 1997586556034887939,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "assistant: Austin is now the newer location.",
    "text": "assistant: Austin is now the newer location."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 6390775253570792492,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "user: I prefer Python for dashboards.",
    "text": "user: I prefer Python for dashboards."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 5281155962483197587,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "assistant: Preference noted: Python for dashboards.",
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 2895883390329778773,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "user: Actually I now prefer Rust for low latency storage engines.",
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 5501532726691688082,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "assistant: Preference update noted: Rust for low latency storage engines.",
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 8061520447825392871,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "user: Alice approved the GPU purchase after finance reviewed the budget.",
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "batch_id_hash": 7111315829382289959,
    "event_id_hash": 1556002124741292395,
    "internal_extraction": {
      "batch_id_hash": 7111315829382289959,
      "classification": "CONFIRMATION",
      "event_type": "session",
      "mode": "matrixark_one_pass_schema_oss_encoder"
    },
    "metadata": {
      "node_path": [
        "personal_memory",
        "session_a",
        "mixed_updates"
      ]
    },
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_event",
    "scope": {
      "account_id": "acct_query_index_debug",
      "sess
```
### context_extraction_audit
```json
[
  {
    "batch_id_hash": 7111315829382289959,
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_extraction_audit"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "record_type": "context_extraction_audit"
  }
]
```
### context_index
```json
[
  {
    "batch_id_hash": 7111315829382289959,
    "index_hash": 7058909476284721333,
    "index_name": "event_type:session",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "batch_id_hash": 7111315829382289959,
    "index_hash": 6099949071327077316,
    "index_name": "classification:confirmation",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "batch_id_hash": 7111315829382289959,
    "index_hash": 3678647570816273904,
    "index_name": "status:observed",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "batch_id_hash": 7111315829382289959,
    "index_hash": 7052406023957600596,
    "index_name": "source_type:message",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "batch_id_hash": 7111315829382289959,
    "index_hash": 7845343631004434458,
    "index_name": "entity_type:session",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "batch_id_hash": 7111315829382289959,
    "index_hash": 1130548055980844123,
    "index_name": "entity_type:approval_state",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "batch_id_hash": 7111315829382289959,
    "index_hash": 7602280238763884035,
    "index_name": "entity_type:current_plan",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "batch_id_hash": 7111315829382289959,
    "index_hash": 414086223223986127,
    "index_name": "entity_type:confirmation",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "batch_id_hash": 6272395221935502168,
    "index_hash": 142878589951243199,
    "index_name": "event_type:dialogue_batch",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  },
  {
    "batch_id_hash": 6272395221935502168,
    "index_hash": 5475655709313417499,
    "index_name": "classification:correction",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  },
  {
    "batch_id_hash": 6272395221935502168,
    "index_hash": 2743485875454753452,
    "index_name": "status:observed",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  },
  {
    "batch_id_hash": 6272395221935502168,
    "index_hash": 8895977689717833148,
    "index_name": "source_type:message",
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "record_type": "context_index",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  }
]
```
### context_pack_audit
```json
[
  {
    "record_type": "context_pack_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "job_status: job_status = job_status: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer l..."
  },
  {
    "record_type": "context_pack_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "current_plan: current_plan = current_plan: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer... user: Actually I now prefer Rust for low latency storage engines. assistant: Preference update noted: Rust for low latency storage engines. user: On April 10 I moved to Austin for the storage project. user: My job role is storage infrastructure lead. user: I ..."
  },
  {
    "record_type": "context_pack_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "user: Alice approved the GPU purchase after finance reviewed the budget. user: On April 10 I moved to Austin for the storage project. confirmation: confirmation = confirmation: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer... approval_state: approval_state: user: I moved to Seattle at the start of March. assistant: I wil = approval_state: user: I mo..."
  },
  {
    "record_type": "context_pack_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "job_status: job_status = job_status: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer l..."
  },
  {
    "record_type": "context_pack_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "summary_text": "relationship: relationship: user: My family has a dog named Mochi. assistant: Family profile n = relationship: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live i... preference: preference = preference: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship not..."
  },
  {
    "record_type": "context_pack_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "summary_text": "job_status: job_status = job_status: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in ... 11: Correction recorded: Go is the newer backend prototype preference."
  },
  {
    "record_type": "context_pack_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "summary_text": "current_plan: current_plan = current_plan: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live i... preference: preference = preference: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in ... 11: Correctio..."
  },
  {
    "record_type": "context_pack_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "summary_text": "current_plan: current_plan = current_plan: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live i... job_status: job_status = job_status: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in ... 11: Correctio..."
  }
]
```
### context_segment
```json
[
  {
    "batch_id_hash": 7111315829382289959,
    "coordinate_tuples": [
      [
        14,
        15
      ]
    ],
    "message_indexes": [
      14,
      15
    ],
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "non_contiguous": false,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "segment_hash": 1795026590203590462,
    "summary_text": "14: My job role is storage infrastructure lead. 15: Job status recorded as storage infrastructure lead.",
    "text": "14: My job role is storage infrastructure lead.\n15: Job status recorded as storage infrastructure lead.",
    "topic": "status_update"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "coordinate_tuples": [
      [
        0,
        3
      ]
    ],
    "message_indexes": [
      0,
      1,
      2,
      3
    ],
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "non_contiguous": false,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "segment_hash": 3958145678372135935,
    "summary_text": "0: I moved to Seattle at the start of March. 1: I will remember Seattle as the earlier location. 2: On April 10 I moved to Austin for the storage project. 3: Austin is now the newer location.",
    "text": "0: I moved to Seattle at the start of March.\n1: I will remember Seattle as the earlier location.\n2: On April 10 I moved to Austin for the storage project.\n3: Austin is now the newer location.",
    "topic": "location"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "coordinate_tuples": [
      [
        16,
        17
      ]
    ],
    "message_indexes": [
      16,
      17
    ],
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "non_contiguous": false,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "segment_hash": 1644484665580180249,
    "summary_text": "16: I plan to visit Berlin next month for the conference. 17: Current plan recorded: visit Berlin next month.",
    "text": "16: I plan to visit Berlin next month for the conference.\n17: Current plan recorded: visit Berlin next month.",
    "topic": "plan_update"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "coordinate_tuples": [
      [
        13,
        13
      ]
    ],
    "message_indexes": [
      13
    ],
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "non_contiguous": false,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "segment_hash": 4602687348935403149,
    "summary_text": "13: Manager relationship noted: Priya.",
    "text": "13: Manager relationship noted: Priya.",
    "topic": "relationship"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "coordinate_tuples": [
      [
        12,
        12
      ],
      [
        19,
        19
      ]
    ],
    "message_indexes": [
      12,
      19
    ],
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "non_contiguous": true,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "segment_hash": 4628744952788223204,
    "summary_text": "12: My manager Priya is helping with the launch plan. 19: Batch summary can now be committed.",
    "text": "12: My manager Priya is helping with the launch plan.\n19: Batch summary can now be committed.",
    "topic": "current_plan"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "coordinate_tuples": [
      [
        8,
        11
      ]
    ],
    "message_indexes": [
      8,
      9,
      10,
      11
    ],
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "non_contiguous": false,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "segment_hash": 790341382967314593,
    "summary_text": "8: Alice approved the GPU purchase after finance reviewed the budget. 9: The GPU purchase approval by Alice is confirmed. 10: The amount for the GPU purchase is 42000 dollars. 11: Budget amount recorded as 42000 dollars.",
    "text": "8: Alice approved the GPU purchase after finance reviewed the budget.\n9: The GPU purchase approval by Alice is confirmed.\n10: The amount for the GPU purchase is 42000 dollars.\n11: Budget amount recorded as 42000 dollars.",
    "topic": "approval"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "coordinate_tuples": [
      [
        4,
        5
      ]
    ],
    "message_indexes": [
      4,
      5
    ],
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "non_contiguous": false,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "segment_hash": 6863592008964852823,
    "summary_text": "4: I prefer Python for dashboards. 5: Preference noted: Python for dashboards.",
    "text": "4: I prefer Python for dashboards.\n5: Preference noted: Python for dashboards.",
    "topic": "preference_update"
  },
  {
    "batch_id_hash": 7111315829382289959,
    "coordinate_tuples": [
      [
        6,
        7
      ],
      [
        18,
        18
      ]
    ],
    "message_indexes": [
      6,
      7,
      18
    ],
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "non_contiguous": true,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "segment_hash": 3090038204484342887,
    "summary_text": "6: Actually I now prefer Rust for low latency storage engines. 7: Preference update noted: Rust for low latency storage engines. 18: Thanks, that is all for this memory batch.",
    "text": "6: Actually I now prefer Rust for low latency storage engines.\n7: Preference update noted: Rust for low latency storage engines.\n18: Thanks, that is all for this memory batch.",
    "topic": "session"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "coordinate_tuples": [
      [
        0,
        1
      ]
    ],
    "message_indexes": [
      0,
      1
    ],
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "non_contiguous": false,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "segment_hash": 6393337591163641575,
    "summary_text": "0: My family has a dog named Mochi. 1: Family profile noted: dog named Mochi.",
    "text": "0: My family has a dog named Mochi.\n1: Family profile noted: dog named Mochi.",
    "topic": "family_profile"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "coordinate_tuples": [
      [
        3,
        3
      ]
    ],
    "message_indexes": [
      3
    ],
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "non_contiguous": false,
    "record_type": "context_segment",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "segment_hash": 7101122966200363690,
    "summary_text": "3: Relations
```
### context_summary
```json
[
  {
    "batch_id_hash": 7111315829382289959,
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_hash": 373249867251399890,
    "summary_text": "user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer location. user: I prefer Python for dashboards. assistant: Preference noted: Python for dashboards. user: Actually I now prefer Rust for low latency storage engines. assistant: Preference update noted: Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget. assistant: The GPU purchase approval by Alice is confirmed. user: The amount for the GPU purchase is 42000 dollars. assistant: Budget amount recorded as 42000 dollars. user: My man...",
    "summary_type": "batch_l0"
  },
  {
    "node_hash": 4269209692194459413,
    "node_path": [
      "personal_memory"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "personal_memory :: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the ...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 4269209692194459413,
    "node_path": [
      "personal_memory"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "Context node personal_memory. Overview: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer location. user: I prefer Python for dashboards. assistant: Preference noted: Python for dashboards. user: Actually I now prefer Rust for low latency storage engines. assistant: Preference update noted: Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget. assistant: The GPU purchase approval by Alice is confirmed. user: The amount for the GPU purchase is 42000 dollars. assistant: Budget amount recorded as 42000 dollars. user: My man... user: My manager Priya is helping with the launch plan. assistant: Manager relationship noted: Priya. user: My job role is storage infrastructure lead. assistant: Job status recorded as storage infrastructure lead. user: I plan to visit Berlin next month for the conference. assistant: Current plan recorded: visit Berlin next month. user: Thanks, that is all for this memory batch. assistant: Batch summary can now be committed.. This node belongs to path...",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 3256715336375095390,
    "node_path": [
      "personal_memory",
      "session_a"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "personal_memory / session_a :: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 3256715336375095390,
    "node_path": [
      "personal_memory",
      "session_a"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "Context node personal_memory / session_a. Overview: user: I moved to Seattle at the start of March. assistant: I will remember Seattle as the earlier location. user: On April 10 I moved to Austin for the storage project. assistant: Austin is now the newer location. user: I prefer Python for dashboards. assistant: Preference noted: Python for dashboards. user: Actually I now prefer Rust for low latency storage engines. assistant: Preference update noted: Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget. assistant: The GPU purchase approval by Alice is confirmed. user: The amount for the GPU purchase is 42000 dollars. assistant: Budget amount recorded as 42000 dollars. user: My man... user: My manager Priya is helping with the launch plan. assistant: Manager relationship noted: Priya. user: My job role is storage infrastructure lead. assistant: Job status recorded as storage infrastructure lead. user: I plan to visit Berlin next month for the conference. assistant: Current plan recorded: visit Berlin next month. user: Thanks, that is all for this memory batch. assistant: Batch summary can now be committed.. This node bel...",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "personal_memory / session_a / mixed_updates :: user: My manager Priya is helping with the launch plan. assistant: Manager relationship noted: Priya. user: My job role is storage infrastructure lead. assistant: Job sta...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "summary_text": "Context node personal_memory / session_a / mixed_updates. Overview: user: My manager Priya is helping with the launch plan. assistant: Manager relationship noted: Priya. user: My job role is storage infrastructure lead. assistant: Job status recorded as storage infrastructure lead. user: I plan to visit Berlin next month for the conference. assistant: Current plan recorded: visit Berlin next month. user: Thanks, that is all for this memory batch. assistant: Batch summary can now be committed.. This node belongs to path personal_memory / session_a / mixed_updates and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "batch_id_hash": 6272395221935502168,
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "summary_hash": 1898253719079701326,
    "summary_text": "user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in Denver. user: I used to live in Boston before the move. assistant: Historical location Boston noted. user: Now I live in Chicago near the lake. assistant: Current location Chicago near the lake. user: I like Java for backend prototypes. assistant: Preference noted: Java for backend prototypes. user: Correction: I now prefer Go for backend prototypes. assistant: Correction recorded: Go is the newer backend prototype preference. user: Bob confirmed the travel budget for Berlin. assistant: Travel budget confirmation by Bo...",
    "summary_type": "batch_l0"
  },
  {
    "node_hash": 4269209692194459413,
    "node_path": [
      "personal_memory"
    ],
    "record_type": "context_summary",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    },
    "summary_text": "personal_memory :: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma 
```
### context_summary_dirty
```json
[
  {
    "node_hash": 4269209692194459413,
    "node_path": [
      "personal_memory"
    ],
    "record_type": "context_summary_dirty",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "node_hash": 3256715336375095390,
    "node_path": [
      "personal_memory",
      "session_a"
    ],
    "record_type": "context_summary_dirty",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_summary_dirty",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "node_hash": 4269209692194459413,
    "node_path": [
      "personal_memory"
    ],
    "record_type": "context_summary_dirty",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  },
  {
    "node_hash": 762316300143733094,
    "node_path": [
      "personal_memory",
      "session_b"
    ],
    "record_type": "context_summary_dirty",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  },
  {
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "record_type": "context_summary_dirty",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  }
]
```
### context_summary_refresh_audit
```json
[
  {
    "node_hash": 4269209692194459413,
    "node_path": [
      "personal_memory"
    ],
    "record_type": "context_summary_refresh_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "node_hash": 3256715336375095390,
    "node_path": [
      "personal_memory",
      "session_a"
    ],
    "record_type": "context_summary_refresh_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_summary_refresh_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    }
  },
  {
    "node_hash": 4269209692194459413,
    "node_path": [
      "personal_memory"
    ],
    "record_type": "context_summary_refresh_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  },
  {
    "node_hash": 762316300143733094,
    "node_path": [
      "personal_memory",
      "session_b"
    ],
    "record_type": "context_summary_refresh_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  },
  {
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "record_type": "context_summary_refresh_audit",
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 5917028181796202772,
      "session_id": "session_b",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 931086106910965805,
      "user_id": "locomo_user_b"
    }
  }
]
```
### matrixark_audit_log
```json
[
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  },
  {
    "record_type": "matrixark_audit_log"
  }
]
```
