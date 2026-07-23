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
- temporalstore: `{"metaserver": "127.0.0.1:18000", "namespace": "deploy_ns", "storage_prefix": "matrixark:cpp:query:index:debug:1782231394422", "table": "deploy_table", "temporalstore_lib": "<repo>/output-ubuntu22/release/sdk/lib/libbcache2.so"}`

## Model Boundary

- embedding_provider: `oss`
- embedding_model: `<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2`
- note: This debug run uses real OSS embeddings. Extraction and query-understanding are the current MatrixArk internal deterministic schema/rule path unless an LLM provider is wired in.

## Data Model Counts

- `context_embedding`: 82
- `context_entity`: 16
- `context_entity_update_audit`: 16
- `context_event`: 40
- `context_extraction_audit`: 2
- `context_index`: 16
- `context_pack_audit`: 8
- `context_segment`: 12
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
    "entity_type:location"
  ]
]
```
Candidates passing prefilter: `21`; dropped before scoring: `12`
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
    "business_score": 0.7,
    "embedding_score": 0.241963,
    "entity_name": "location",
    "entity_type": "location",
    "final_score": 0.70105,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.604196,
    "origin_score": 0.61175,
    "packing_policy": "current_state",
    "packing_score": 0.98105,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1499683877809583262,
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
    "score": 0.70105,
    "sparse_score": 0.2,
    "text": "location: location = Austin for the storage project",
    "time_score": 1.0,
    "token_estimate": 7,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.377862,
    "event_type": "confirmation",
    "final_score": 0.759473,
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
    "node_score": 0.604196,
    "origin_score": 0.599122,
    "packing_policy": "current_state",
    "packing_score": 0.919473,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2514438122329628730,
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
    "score": 0.759473,
    "sparse_score": 0.4,
    "text": "user: On April 10 I moved to Austin for the storage project.",
    "time_score": 1.0,
    "token_estimate": 12,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.361825,
    "event_type": "confirmation",
    "final_score": 0.756827,
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
    "node_score": 0.604196,
    "origin_score": 0.594712,
    "packing_policy": "current_state",
    "packing_score": 0.916827,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2051498356381668566,
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
    "score": 0.756827,
    "sparse_score": 0.4,
    "text": "user: I moved to Seattle at the start of March.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.354701,
    "event_type": "confirmation",
    "final_score": 0.755652,
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
    "node_score": 0.604196,
    "origin_score": 0.592753,
    "packing_policy": "current_state",
    "packing_score": 0.915652,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 3117837784164868870,
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
    "score": 0.755652,
    "sparse_score": 0.4,
    "text": "user: My manager Priya is helping with the launch plan.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.172934,
    "event_type": "confirmation",
    "final_score": 0.72566,
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
    "node_score": 0.604196,
    "origin_score": 0.542767,
    "packing_policy": "current_state",
    "packing_score": 0.88566,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 4119226674901368293,
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
    "score": 0.72566,
    "sparse_score": 0.4,
    "text": "user: The amount for the GPU purchase is 42000 dollars.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231394615

```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 2051498356381668566,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7284734633180256280,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:plan_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2514438122329628730,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4502923893479021359,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Austin is now the newer location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6946084915914117496,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I prefer Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7231234139405670262,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4529445785084401915,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7607337643396144146,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6320570160608372008,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8480328614703112771,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: The GPU purchase 
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
    "entity_type:preference",
    "event_type:preference_update"
  ]
]
```
Candidates passing prefilter: `21`; dropped before scoring: `12`
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
    "business_score": 0.75,
    "embedding_score": 0.695252,
    "entity_name": "preference",
    "entity_type": "preference",
    "final_score": 0.830599,
    "keyword_score": 4,
    "metadata": {},
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.662736,
    "origin_score": 0.809331,
    "packing_policy": "current_state",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2690897074610495568,
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
    "score": 0.830599,
    "sparse_score": 0.4,
    "text": "preference: preference = Rust for low latency storage engines",
    "time_score": 1.0,
    "token_estimate": 8,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.69059,
    "event_type": "confirmation",
    "final_score": 0.875829,
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
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 4529445785084401915,
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
    "score": 0.875829,
    "sparse_score": 0.7,
    "text": "user: Actually I now prefer Rust for low latency storage engines.",
    "time_score": 1.0,
    "token_estimate": 11,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.688212,
    "event_type": "confirmation",
    "final_score": 0.812437,
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
    "packing_score": 0.972437,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 7607337643396144146,
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
    "score": 0.812437,
    "sparse_score": 0.4,
    "text": "assistant: Preference update noted: Rust for low latency storage engines.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.305395,
    "event_type": "confirmation",
    "final_score": 0.749272,
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
    "packing_score": 0.909272,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2514438122329628730,
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
    "score": 0.749272,
    "sparse_score": 0.4,
    "text": "user: On April 10 I moved to Austin for the storage project.",
    "time_score": 1.0,
    "token_estimate": 12,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.380898,
    "event_type": "confirmation",
    "final_score": 0.71973,
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
    "packing_score": 0.87973,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 5131449452536787666,
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
    "score": 0.71973,
    "sparse_score": 0.2,
    "text": "user: My job role is storage infrastructure lead.",
    "time_score": 1.0,
    "token_estimate": 8,
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 2051498356381668566,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7284734633180256280,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:plan_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2514438122329628730,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4502923893479021359,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Austin is now the newer location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6946084915914117496,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I prefer Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7231234139405670262,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4529445785084401915,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7607337643396144146,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6320570160608372008,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8480328614703112771,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: The GPU purchase 
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
Question type: `fact`
Secondary-index filter groups inferred from query:
```json
[
  [
    "classification:confirmation",
    "entity_type:approval_state",
    "entity_type:confirmation",
    "event_type:confirmation",
    "segment_topic:approval_budget"
  ]
]
```
Candidates passing prefilter: `23`; dropped before scoring: `10`
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
    "business_score": 1.0,
    "embedding_score": 0.790315,
    "event_type": "confirmation",
    "final_score": 0.919802,
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
    "packing_policy": "fact",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 6320570160608372008,
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
    "score": 0.919802,
    "sparse_score": 0.8333333333333334,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget.",
    "time_score": 1.0,
    "token_estimate": 11,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 0.95,
    "embedding_score": 0.793878,
    "entity_name": "the GPU purchase after finance reviewed the budget",
    "entity_type": "approval_state",
    "final_score": 0.94639,
    "keyword_score": 4,
    "metadata": {},
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.646674,
    "origin_score": 0.928983,
    "packing_policy": "fact",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 3885986472343810344,
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
    "score": 0.94639,
    "sparse_score": 0.6666666666666666,
    "text": "approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget",
    "time_score": 1.0,
    "token_estimate": 17,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.675824,
    "event_type": "confirmation",
    "final_score": 0.830911,
    "keyword_score": 3,
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
    "origin_score": 0.718185,
    "packing_policy": "fact",
    "packing_score": 0.970911,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 4119226674901368293,
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
    "score": 0.830911,
    "sparse_score": 0.5,
    "text": "user: The amount for the GPU purchase is 42000 dollars.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.652865,
    "event_type": "confirmation",
    "final_score": 0.827123,
    "keyword_score": 3,
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
    "origin_score": 0.711872,
    "packing_policy": "fact",
    "packing_score": 0.967123,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8480328614703112771,
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
    "score": 0.827123,
    "sparse_score": 0.5,
    "text": "assistant: The GPU purchase approval by Alice is confirmed.",
    "time_score": 1.0,
    "token_estimate": 9,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 0.95,
    "coordinate_tuples": [
      [
        8,
        11
      ]
    ],
    "embedding_score": 0.808915,
    "final_score": 0.903028,
    "keyword_score": 5,
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "node_score": 0.646674,
    "non_contiguous": false,
    "origin_score": 0.856714,
    "packing_policy": "fact",
    "packing_score": 0.933028,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2081066652511314382,
    "ref_type": "segment",
    "saliency_score": 0.6875,
    "scope": {
      "account_id": "acct_query_index_debug",
      "session_hash": 8237187342837907451,
      "session_id": "session_a",
      "tenant_hash": 1875121732827492989,
      "tenant_id": "tenant_context",
      "user_hash": 3876043158382393130,
      "user_id": "locomo_user_a"
    },
    "score": 0.903028,
    "sparse_score": 0.8333333333333334,
    "text": "8: Alice approved the GPU purcha
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 2051498356381668566,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7284734633180256280,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:plan_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2514438122329628730,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4502923893479021359,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Austin is now the newer location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6946084915914117496,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I prefer Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7231234139405670262,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4529445785084401915,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7607337643396144146,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6320570160608372008,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8480328614703112771,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: The GPU purchase 
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
Question type: `fact`
Secondary-index filter groups inferred from query:
```json
[
  [
    "entity_type:family_profile",
    "entity_type:relationship"
  ]
]
```
Candidates passing prefilter: `22`; dropped before scoring: `11`
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
    "business_score": 1.0,
    "embedding_score": 0.54572,
    "event_type": "confirmation",
    "final_score": 0.863105,
    "keyword_score": 3,
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
    "node_score": 0.685356,
    "origin_score": 0.771841,
    "packing_policy": "fact",
    "packing_score": 1.0,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 3117837784164868870,
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
    "score": 0.863105,
    "sparse_score": 0.75,
    "text": "user: My manager Priya is helping with the launch plan.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.23874,
    "event_type": "confirmation",
    "final_score": 0.759953,
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
    "node_score": 0.685356,
    "origin_score": 0.599921,
    "packing_policy": "fact",
    "packing_score": 0.899953,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 6320570160608372008,
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
    "score": 0.759953,
    "sparse_score": 0.5,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget.",
    "time_score": 1.0,
    "token_estimate": 11,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.213988,
    "event_type": "confirmation",
    "final_score": 0.755869,
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
    "node_score": 0.685356,
    "origin_score": 0.593115,
    "packing_policy": "fact",
    "packing_score": 0.895869,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2514438122329628730,
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
    "score": 0.755869,
    "sparse_score": 0.5,
    "text": "user: On April 10 I moved to Austin for the storage project.",
    "time_score": 1.0,
    "token_estimate": 12,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.200382,
    "event_type": "confirmation",
    "final_score": 0.753624,
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
    "node_score": 0.685356,
    "origin_score": 0.589373,
    "packing_policy": "fact",
    "packing_score": 0.893624,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2624245096263660550,
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
    "score": 0.753624,
    "sparse_score": 0.5,
    "text": "user: I plan to visit Berlin next month for the conference.",
    "time_score": 1.0,
    "token_estimate": 11,
    "updated_at_ms": 1782231394615
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.18878,
    "event_type": "confirmation",
    "final_score": 0.751709,
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
    "node_score": 0.685356,
    "origin_score": 0.586182,
    "packing_policy": "fact",
    "packing_score": 0.891709,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2051498356381668566,
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
    "score": 0.751709,
    "sparse_score": 0.5,
    "text": "user: I moved to Seattle at the start of March.",
    "time_score": 1.0,
   
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 2051498356381668566,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I moved to Seattle at the start of March."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7284734633180256280,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:plan_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: I will remember Seattle as the earlier location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2514438122329628730,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: On April 10 I moved to Austin for the storage project."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4502923893479021359,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Austin is now the newer location."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6946084915914117496,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I prefer Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7231234139405670262,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Python for dashboards."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4529445785084401915,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Actually I now prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7607337643396144146,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference update noted: Rust for low latency storage engines."
  },
  {
    "record_type": "context_event",
    "ref_hash": 6320570160608372008,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8480328614703112771,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "candidate_terms": [
      "classification:batch_memory",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:confirmation",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: The GPU purchase 
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
Question type: `fact`
Secondary-index filter groups inferred from query:
```json
[
  [
    "entity_type:family_profile",
    "entity_type:relationship"
  ]
]
```
Candidates passing prefilter: `23`; dropped before scoring: `12`
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
    "business_score": 1.0,
    "embedding_score": 0.606886,
    "event_type": "correction",
    "final_score": 0.797512,
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
    "packing_policy": "fact",
    "packing_score": 0.937512,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 938375684980133608,
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
    "score": 0.797512,
    "sparse_score": 0.4,
    "text": "user: My family has a dog named Mochi.",
    "time_score": 1.0,
    "token_estimate": 8,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.472437,
    "event_type": "correction",
    "final_score": 0.733328,
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
    "packing_policy": "fact",
    "packing_score": 0.873328,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 965381327260416369,
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
    "score": 0.733328,
    "sparse_score": 0.2,
    "text": "assistant: Family profile noted: dog named Mochi.",
    "time_score": 1.0,
    "token_estimate": 7,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.17387,
    "event_type": "correction",
    "final_score": 0.726065,
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
    "packing_policy": "fact",
    "packing_score": 0.866065,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 5220895800864208990,
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
    "score": 0.726065,
    "sparse_score": 0.4,
    "text": "user: Now I live in Chicago near the lake.",
    "time_score": 1.0,
    "token_estimate": 9,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.143577,
    "event_type": "correction",
    "final_score": 0.721066,
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
    "origin_score": 0.53511,
    "packing_policy": "fact",
    "packing_score": 0.861066,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8785702830230598761,
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
    "score": 0.721066,
    "sparse_score": 0.4,
    "text": "user: I used to live in Boston before the move.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.086222,
    "event_type": "correction",
    "final_score": 0.711602,
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
    "origin_score": 0.519337,
    "packing_policy": "fact",
    "packing_score": 0.851602,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1092420839721165496,
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
    "score": 0.711602,
    "sparse_score": 0.4,
    "text": "user: Bob confirmed the travel budget for Berlin.",
    "time_score": 1.0,
    "token_estimate": 8,
  
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 938375684980133608,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 965381327260416369,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Family profile noted: dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7471706062047254740,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My sister Emma lives in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 531939124286836142,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Relationship noted: sister Emma in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8785702830230598761,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I used to live in Boston before the move."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2074962612305027242,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Historical location Boston noted."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5220895800864208990,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Now I live in Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2845368815607877116,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Current location Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 720920173715482383,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I like Java for backend prototypes."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4839756949912235450,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Java for backend protot
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
    "business_score": 0.7,
    "embedding_score": 0.261863,
    "entity_name": "location",
    "entity_type": "location",
    "final_score": 0.696842,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.58782,
    "origin_score": 0.604737,
    "packing_policy": "current_state",
    "packing_score": 0.976842,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 9070524035486646356,
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
    "score": 0.696842,
    "sparse_score": 0.16666666666666666,
    "text": "location: location = Chicago near the lake",
    "time_score": 1.0,
    "token_estimate": 6,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.578298,
    "event_type": "correction",
    "final_score": 0.813054,
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
    "node_score": 0.58782,
    "origin_score": 0.688423,
    "packing_policy": "current_state",
    "packing_score": 0.973054,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8785702830230598761,
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
    "score": 0.813054,
    "sparse_score": 0.5,
    "text": "user: I used to live in Boston before the move.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.49027,
    "event_type": "correction",
    "final_score": 0.798529,
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
    "node_score": 0.58782,
    "origin_score": 0.664215,
    "packing_policy": "current_state",
    "packing_score": 0.958529,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 5220895800864208990,
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
    "score": 0.798529,
    "sparse_score": 0.5,
    "text": "user: Now I live in Chicago near the lake.",
    "time_score": 1.0,
    "token_estimate": 9,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.449092,
    "event_type": "correction",
    "final_score": 0.721735,
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
    "node_score": 0.58782,
    "origin_score": 0.536225,
    "packing_policy": "current_state",
    "packing_score": 0.881735,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 7471706062047254740,
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
    "score": 0.721735,
    "sparse_score": 0.16666666666666666,
    "text": "user: My sister Emma lives in Denver.",
    "time_score": 1.0,
    "token_estimate": 7,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.21561,
    "event_type": "correction",
    "final_score": 0.71821,
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
    "node_score": 0.58782,
    "origin_score": 0.53035,
    "packing_policy": "current_state",
    "packing_score": 0.87821,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 1092420839721165496,
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
    "score": 0.71821,
    "sparse_score": 0.3333333333333333,
    "text": "user: Bob confirmed the travel budget for Berlin.",
    "time_score": 1.0,
    "token_estimate": 8,
    "
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 938375684980133608,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 965381327260416369,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Family profile noted: dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7471706062047254740,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My sister Emma lives in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 531939124286836142,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Relationship noted: sister Emma in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8785702830230598761,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I used to live in Boston before the move."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2074962612305027242,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Historical location Boston noted."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5220895800864208990,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Now I live in Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2845368815607877116,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Current location Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 720920173715482383,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I like Java for backend prototypes."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4839756949912235450,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Java for backend protot
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
    "entity_type:preference",
    "event_type:preference_update"
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
    "business_score": 0.75,
    "embedding_score": 0.493208,
    "entity_name": "preference",
    "entity_type": "preference",
    "final_score": 0.736994,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.578832,
    "origin_score": 0.653324,
    "packing_policy": "current_state",
    "packing_score": 1.0,
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
    "score": 0.736994,
    "sparse_score": 0.125,
    "text": "preference: preference = Go for backend prototypes",
    "time_score": 1.0,
    "token_estimate": 6,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.493443,
    "event_type": "correction",
    "final_score": 0.772533,
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
    "packing_score": 0.932533,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 5763682577735492769,
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
    "score": 0.772533,
    "sparse_score": 0.375,
    "text": "user: Correction: I now prefer Go for backend prototypes.",
    "time_score": 1.0,
    "token_estimate": 9,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.548713,
    "event_type": "correction",
    "final_score": 0.755403,
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
    "packing_score": 0.915403,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 720920173715482383,
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
    "score": 0.755403,
    "sparse_score": 0.25,
    "text": "user: I like Java for backend prototypes.",
    "time_score": 1.0,
    "token_estimate": 7,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.401392,
    "event_type": "correction",
    "final_score": 0.731094,
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
    "origin_score": 0.551824,
    "packing_policy": "current_state",
    "packing_score": 0.891094,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 7611564639366118105,
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
    "score": 0.731094,
    "sparse_score": 0.25,
    "text": "assistant: Correction recorded: Go is the newer backend prototype preference.",
    "time_score": 1.0,
    "token_estimate": 10,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.549525,
    "event_type": "correction",
    "final_score": 0.729287,
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
    "node_score": 0.578832,
    "origin_score": 0.548811,
    "packing_policy": "current_state",
    "packing_score": 0.889287,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 4839756949912235450,
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
    "score": 0.729287,
    "sparse_score": 0.125,
    "text": "assistant: Preference noted: Java for backend prototypes.",
    "time_score":
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 938375684980133608,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 965381327260416369,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Family profile noted: dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7471706062047254740,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My sister Emma lives in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 531939124286836142,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Relationship noted: sister Emma in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8785702830230598761,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I used to live in Boston before the move."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2074962612305027242,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Historical location Boston noted."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5220895800864208990,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Now I live in Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2845368815607877116,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Current location Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 720920173715482383,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I like Java for backend prototypes."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4839756949912235450,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Java for backend protot
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
    "entity_type:job_status",
    "event_type:status_update"
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
    "business_score": 0.76,
    "embedding_score": 0.381535,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "final_score": 0.729003,
    "keyword_score": 1,
    "metadata": {},
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "node_score": 0.561673,
    "origin_score": 0.636339,
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
    "score": 0.729003,
    "sparse_score": 0.16666666666666666,
    "text": "job_status: job_status = status update recorded: AI memory platform owner",
    "time_score": 1.0,
    "token_estimate": 9,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.430487,
    "event_type": "correction",
    "final_score": 0.75288,
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
    "packing_score": 0.91288,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 5188459644336298055,
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
    "score": 0.75288,
    "sparse_score": 0.3333333333333333,
    "text": "user: My role changed to AI memory platform owner.",
    "time_score": 1.0,
    "token_estimate": 9,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.160593,
    "event_type": "correction",
    "final_score": 0.743348,
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
    "packing_score": 0.903348,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 8799880466906387227,
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
    "score": 0.743348,
    "sparse_score": 0.5,
    "text": "user: The current plan is to ship the benchmark report Friday.",
    "time_score": 1.0,
    "token_estimate": 11,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.200447,
    "event_type": "correction",
    "final_score": 0.714924,
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
    "origin_score": 0.524873,
    "packing_policy": "current_state",
    "packing_score": 0.874924,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 2845368815607877116,
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
    "score": 0.714924,
    "sparse_score": 0.3333333333333333,
    "text": "assistant: Current location Chicago near the lake.",
    "time_score": 1.0,
    "token_estimate": 7,
    "updated_at_ms": 1782231413746
  },
  {
    "business_score": 1.0,
    "embedding_score": 0.395109,
    "event_type": "correction",
    "final_score": 0.712043,
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
    "node_score": 0.561673,
    "origin_score": 0.520072,
    "packing_policy": "current_state",
    "packing_score": 0.872043,
    "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
    "recall_path": "primary_hybrid",
    "ref_hash": 6002214941565108249,
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
    "score": 0.712043,
    "sparse_score": 0.16666666666666666,
    "text": "assistant: Job 
```
Prefilter candidate sample:
```json
[
  {
    "record_type": "context_event",
    "ref_hash": 938375684980133608,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 965381327260416369,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Family profile noted: dog named Mochi."
  },
  {
    "record_type": "context_event",
    "ref_hash": 7471706062047254740,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: My sister Emma lives in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 531939124286836142,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Relationship noted: sister Emma in Denver."
  },
  {
    "record_type": "context_event",
    "ref_hash": 8785702830230598761,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I used to live in Boston before the move."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2074962612305027242,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Historical location Boston noted."
  },
  {
    "record_type": "context_event",
    "ref_hash": 5220895800864208990,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: Now I live in Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 2845368815607877116,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:dialogue_batch",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Current location Chicago near the lake."
  },
  {
    "record_type": "context_event",
    "ref_hash": 720920173715482383,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "user: I like Java for backend prototypes."
  },
  {
    "record_type": "context_event",
    "ref_hash": 4839756949912235450,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "candidate_terms": [
      "classification:correction",
      "entity_type:job_status",
      "entity_type:location",
      "entity_type:preference",
      "entity_type:relationship",
      "event_type:correction",
      "event_type:preference_update",
      "source_type:message",
      "status:observed"
    ],
    "passes_secondary_index_prefilter": true,
    "text": "assistant: Preference noted: Java for backend protot
```

## Records By Data Model

### context_embedding
```json
[
  {
    "dim": 384,
    "embedding_type": "event_text",
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 2051498356381668566,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 7284734633180256280,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 2514438122329628730,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 4502923893479021359,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 6946084915914117496,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 7231234139405670262,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 4529445785084401915,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 7607337643396144146,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 6320570160608372008,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 8480328614703112771,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_embedding",
    "ref_hash": 4119226674901368293,
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
    "model": "<repo>/.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2",

```
### context_entity
```json
[
  {
    "batch_id_hash": 66424972306504357,
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
    "state": "Rust for low latency storage engines"
  },
  {
    "batch_id_hash": 66424972306504357,
    "entity_hash": 5710008802372227833,
    "entity_name": "Priya is helping with the launch plan",
    "entity_type": "relationship",
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
    "state": "Priya is helping with the launch plan"
  },
  {
    "batch_id_hash": 66424972306504357,
    "entity_hash": 3513236292051679635,
    "entity_name": "relationship noted: Priya",
    "entity_type": "relationship",
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
    "state": "relationship noted: Priya"
  },
  {
    "batch_id_hash": 66424972306504357,
    "entity_hash": 1499683877809583262,
    "entity_name": "location",
    "entity_type": "location",
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
    "state": "Austin for the storage project"
  },
  {
    "batch_id_hash": 66424972306504357,
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
    "state": "status recorded as storage infrastructure lead"
  },
  {
    "batch_id_hash": 66424972306504357,
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
    "state": "recorded: visit Berlin next month"
  },
  {
    "batch_id_hash": 66424972306504357,
    "entity_hash": 3885986472343810344,
    "entity_name": "the GPU purchase after finance reviewed the budget",
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
    "state": "the GPU purchase after finance reviewed the budget"
  },
  {
    "batch_id_hash": 66424972306504357,
    "entity_hash": 3221884208695485165,
    "entity_name": "by Alice is confirmed",
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
    "state": "by Alice is confirmed"
  },
  {
    "batch_id_hash": 8801035647126472357,
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
    "state": "Go for backend prototypes"
  },
  {
    "batch_id_hash": 8801035647126472357,
    "entity_hash": 1594604547028064822,
    "entity_name": "Emma lives in Denver",
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
    "state": "Emma lives in Denver"
  },
  {
    "batch_id_hash": 8801035647126472357,
    "entity_hash": 8877835311637067735,
    "entity_name": "Emma in Denver",
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
    "state": "Emma in Denver"
  },
  {
    "batch_id_hash": 8801035647126472357,
    "entity_hash": 9070524035486646356,
    "entity_name": "location",
    "entity_type": "location",
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
    "state": "Chicago near the lake"
  }
]
```
### context_entity_update_audit
```json
[
  {
    "batch_id_hash": 66424972306504357,
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
    "batch_id_hash": 66424972306504357,
    "entity_hash": 5710008802372227833,
    "entity_name": "Priya is helping with the launch plan",
    "entity_type": "relationship",
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
    "batch_id_hash": 66424972306504357,
    "entity_hash": 3513236292051679635,
    "entity_name": "relationship noted: Priya",
    "entity_type": "relationship",
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
    "batch_id_hash": 66424972306504357,
    "entity_hash": 1499683877809583262,
    "entity_name": "location",
    "entity_type": "location",
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
    "batch_id_hash": 66424972306504357,
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
    "batch_id_hash": 66424972306504357,
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
    "batch_id_hash": 66424972306504357,
    "entity_hash": 3885986472343810344,
    "entity_name": "the GPU purchase after finance reviewed the budget",
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
    "batch_id_hash": 66424972306504357,
    "entity_hash": 3221884208695485165,
    "entity_name": "by Alice is confirmed",
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
    "batch_id_hash": 8801035647126472357,
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
    "batch_id_hash": 8801035647126472357,
    "entity_hash": 1594604547028064822,
    "entity_name": "Emma lives in Denver",
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
    "batch_id_hash": 8801035647126472357,
    "entity_hash": 8877835311637067735,
    "entity_name": "Emma in Denver",
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
    "batch_id_hash": 8801035647126472357,
    "entity_hash": 9070524035486646356,
    "entity_name": "location",
    "entity_type": "location",
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 2051498356381668566,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 7284734633180256280,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 2514438122329628730,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 4502923893479021359,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 6946084915914117496,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 7231234139405670262,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 4529445785084401915,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 7607337643396144146,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 6320570160608372008,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
    "batch_id_hash": 66424972306504357,
    "event_id_hash": 8480328614703112771,
    "internal_extraction": {
      "batch_id_hash": 66424972306504357,
      "classification": "BATCH_MEMORY",
      "event_type": "confirmation",
      "mode": "matrixark_one_pass_schema"
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
   
```
### context_extraction_audit
```json
[
  {
    "batch_id_hash": 66424972306504357,
    "node_hash": 8525532004486225381,
    "node_path": [
      "personal_memory",
      "session_a",
      "mixed_updates"
    ],
    "record_type": "context_extraction_audit"
  },
  {
    "batch_id_hash": 8801035647126472357,
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
    "batch_id_hash": 66424972306504357,
    "index_hash": 6330152217573744737,
    "index_name": "event_type:confirmation",
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
    "batch_id_hash": 66424972306504357,
    "index_hash": 3731577094839855875,
    "index_name": "classification:batch_memory",
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
    "batch_id_hash": 66424972306504357,
    "index_hash": 3472525142670917688,
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
    "batch_id_hash": 66424972306504357,
    "index_hash": 4821043609260220553,
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
    "batch_id_hash": 66424972306504357,
    "index_hash": 6155484803375954793,
    "index_name": "entity_type:preference",
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
    "batch_id_hash": 66424972306504357,
    "index_hash": 6539353758926517540,
    "index_name": "entity_type:relationship",
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
    "batch_id_hash": 66424972306504357,
    "index_hash": 1362547879788272863,
    "index_name": "entity_type:location",
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
    "batch_id_hash": 66424972306504357,
    "index_hash": 6903362605797802309,
    "index_name": "entity_type:job_status",
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
    "batch_id_hash": 8801035647126472357,
    "index_hash": 3550458486923263487,
    "index_name": "event_type:correction",
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
    "batch_id_hash": 8801035647126472357,
    "index_hash": 7787846427518932040,
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
    "batch_id_hash": 8801035647126472357,
    "index_hash": 8847715016700792103,
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
    "batch_id_hash": 8801035647126472357,
    "index_hash": 4602380737437675524,
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
    "summary_text": "location: location = Austin for the storage project user: On April 10 I moved to Austin for the storage project. user: I moved to Seattle at the start of March. user: My manager Priya is helping with the launch plan. user: The amount for the GPU purchase is 42000 dollars. user: I plan to visit Berlin next month for the conference. user: Alice approved the GPU purchase after finance reviewed the budget. user: My job role is storage infrastructure lead. assistant: Austin is now the newer location. assistan..."
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
    "summary_text": "preference: preference = Rust for low latency storage engines user: Actually I now prefer Rust for low latency storage engines. assistant: Preference update noted: Rust for low latency storage engines. user: On April 10 I moved to Austin for the storage project. user: My job role is storage infrastructure lead. user: I prefer Python for dashboards. user: The amount for the GPU purchase is 42000 dollars. user: Thanks, that is all for this memory batch. user: I plan to visit Berlin next month for the confe..."
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
    "summary_text": "user: Alice approved the GPU purchase after finance reviewed the budget. approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget user: The amount for the GPU purchase is 42000 dollars. assistant: The GPU purchase approval by Alice is confirmed. 8: Alice approved the GPU purchase after finance reviewed the budget. 9: The GPU purchase approval by Alice is confirmed. 10: The amount for the GPU purchase is 42000 dollars. 11: Budget amount record..."
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
    "summary_text": "user: My manager Priya is helping with the launch plan. user: Alice approved the GPU purchase after finance reviewed the budget. user: On April 10 I moved to Austin for the storage project. user: I plan to visit Berlin next month for the conference. user: I moved to Seattle at the start of March. assistant: Manager relationship noted: Priya. user: My job role is storage infrastructure lead. user: The amount for the GPU purchase is 42000 dollars. user: Thanks, that is all for this memory batch. relationsh..."
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
    "summary_text": "user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: Now I live in Chicago near the lake. user: I used to live in Boston before the move. user: Bob confirmed the travel budget for Berlin. family_profile: family_profile = profile noted: dog named Mochi user: My sister Emma lives in Denver. user: The current plan is to ship the benchmark report Friday. user: Please remember these details for later. user: I like Java for backend prototypes. user: My role changed to ..."
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
    "summary_text": "location: location = Chicago near the lake user: I used to live in Boston before the move. user: Now I live in Chicago near the lake. user: My sister Emma lives in Denver. user: Bob confirmed the travel budget for Berlin. assistant: Current location Chicago near the lake. user: The current plan is to ship the benchmark report Friday. user: My role changed to AI memory platform owner. user: Please remember these details for later. user: My family has a dog named Mochi. user: Correction: I now prefer Go fo..."
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
    "summary_text": "preference: preference = Go for backend prototypes user: Correction: I now prefer Go for backend prototypes. user: I like Java for backend prototypes. assistant: Correction recorded: Go is the newer backend prototype preference. assistant: Preference noted: Java for backend prototypes. user: The current plan is to ship the benchmark report Friday. user: Bob confirmed the travel budget for Berlin. user: Now I live in Chicago near the lake. user: I used to live in Boston before the move. user: Please remem..."
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
    "summary_text": "job_status: job_status = status update recorded: AI memory platform owner user: My role changed to AI memory platform owner. user: The current plan is to ship the benchmark report Friday. assistant: Current location Chicago near the lake. assistant: Job status update recorded: AI memory platform owner. user: I used to live in Boston before the move. user: Now I live in Chicago near the lake. user: Bob confirmed the travel budget for Berlin. user: Please remember these details for later. assistant: Curren..."
  }
]
```
### context_segment
```json
[
  {
    "batch_id_hash": 66424972306504357,
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
    "segment_hash": 6102635588171190629,
    "summary_text": "0: I moved to Seattle at the start of March. 1: I will remember Seattle as the earlier location. 2: On April 10 I moved to Austin for the storage project. 3: Austin is now the newer location.",
    "text": "0: I moved to Seattle at the start of March.\n1: I will remember Seattle as the earlier location.\n2: On April 10 I moved to Austin for the storage project.\n3: Austin is now the newer location.",
    "topic": "location"
  },
  {
    "batch_id_hash": 66424972306504357,
    "coordinate_tuples": [
      [
        12,
        12
      ],
      [
        16,
        17
      ]
    ],
    "message_indexes": [
      12,
      16,
      17
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
    "segment_hash": 977444788836309295,
    "summary_text": "12: My manager Priya is helping with the launch plan. 16: I plan to visit Berlin next month for the conference. 17: Current plan recorded: visit Berlin next month.",
    "text": "12: My manager Priya is helping with the launch plan.\n16: I plan to visit Berlin next month for the conference.\n17: Current plan recorded: visit Berlin next month.",
    "topic": "plan_status"
  },
  {
    "batch_id_hash": 66424972306504357,
    "coordinate_tuples": [
      [
        4,
        4
      ],
      [
        6,
        7
      ]
    ],
    "message_indexes": [
      4,
      6,
      7
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
    "segment_hash": 8085008742321159450,
    "summary_text": "4: I prefer Python for dashboards. 6: Actually I now prefer Rust for low latency storage engines. 7: Preference update noted: Rust for low latency storage engines.",
    "text": "4: I prefer Python for dashboards.\n6: Actually I now prefer Rust for low latency storage engines.\n7: Preference update noted: Rust for low latency storage engines.",
    "topic": "preference"
  },
  {
    "batch_id_hash": 66424972306504357,
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
    "segment_hash": 2081066652511314382,
    "summary_text": "8: Alice approved the GPU purchase after finance reviewed the budget. 9: The GPU purchase approval by Alice is confirmed. 10: The amount for the GPU purchase is 42000 dollars. 11: Budget amount recorded as 42000 dollars.",
    "text": "8: Alice approved the GPU purchase after finance reviewed the budget.\n9: The GPU purchase approval by Alice is confirmed.\n10: The amount for the GPU purchase is 42000 dollars.\n11: Budget amount recorded as 42000 dollars.",
    "topic": "approval_budget"
  },
  {
    "batch_id_hash": 66424972306504357,
    "coordinate_tuples": [
      [
        18,
        18
      ]
    ],
    "message_indexes": [
      18
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
    "segment_hash": 2563593453345787616,
    "summary_text": "18: Thanks, that is all for this memory batch.",
    "text": "18: Thanks, that is all for this memory batch.",
    "topic": "thanks"
  },
  {
    "batch_id_hash": 8801035647126472357,
    "coordinate_tuples": [
      [
        10,
        11
      ]
    ],
    "message_indexes": [
      10,
      11
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
    "segment_hash": 8486839138337678296,
    "summary_text": "10: Correction: I now prefer Go for backend prototypes. 11: Correction recorded: Go is the newer backend prototype preference.",
    "text": "10: Correction: I now prefer Go for backend prototypes.\n11: Correction recorded: Go is the newer backend prototype preference.",
    "topic": "preference"
  },
  {
    "batch_id_hash": 8801035647126472357,
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
    "segment_hash": 5734488270343824174,
    "summary_text": "14: The current plan is to ship the benchmark report Friday. 15: Current plan recorded: ship benchmark report Friday.",
    "text": "14: The current plan is to ship the benchmark report Friday.\n15: Current plan recorded: ship benchmark report Friday.",
    "topic": "plan_status"
  },
  {
    "batch_id_hash": 8801035647126472357,
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
    "segment_hash": 2105386333729608851,
    "summary_text": "16: My role changed to AI memory platform owner. 17: Job status update recorded: AI memory platform owner.",
    "text": "16: My role changed to AI memory platform owner.\n17: Job status update recorded: AI memory platform owner.",
    "topic": "task_decision"
  },
  {
    "batch_id_hash": 8801035647126472357,
    "coordinate_tuples": [
      [
        2,
        2
      ],
      [
        4,
        7
      ]
    ],
    "message_indexes": [
      2,
      4,
      5,
      6,
      7
    ],
    "node_hash": 1585125673964551143,
    "node_path": [
      "personal_memory",
      "session_b",
      "family_plan_updates"
    ],
    "non_contiguous": true,
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
    "segment_hash": 6599575259891863474,
    "summary_text": "2: My sister Emma lives in Denver. 4: I used to live in Boston before the move. 5: Historical location Boston noted. 6: Now I live in Chicago near the lake. 7: Current location Chicago near the lake.",
    "text": "2: My sister Emma lives in Denver.\n4: I used to live in Boston before the move.\n5: Historical location Boston noted.\n6: Now I live in Chicago near the lake.\n7: Current location Chicago near the lake.",
    "topic": "location"
  },
  {
    "batch_id_hash": 8801035647126472357,
    "coordinate_tuples": [
      [
        12,
        13
      ]
    ],
    "message_indexes": [
      12,
      1
```
### context_summary
```json
[
  {
    "batch_id_hash": 66424972306504357,
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
    "summary_hash": 125833450695295592,
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
    "batch_id_hash": 8801035647126472357,
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
    "summary_hash": 6513510749421846944,
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
    "summary_text": "personal_memory :: user: My family has a dog named Mochi. assistant: Family profile noted: dog named Mochi. user: My sister Emma lives in Denver. assistant: Relationship noted: sister Emma in
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
