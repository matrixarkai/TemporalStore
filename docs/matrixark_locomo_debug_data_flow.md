# MatrixArk LOCOMO-Style Debug Data Flow

This doc shows an actual local MatrixArk extraction and retrieval run using three LOCOMO-style long-memory conversations. It is intended for debugging the data flow across MatrixArk logical TemporalStore models.

Important note: no official LOCOMO dataset file was present in this repository at run time, so the conversations below are LOCOMO-style samples that exercise the same benchmark categories: current-state memory, temporal updates, preferences, relationship/profile facts, and approval facts. The raw machine-readable artifact is committed at:

```text
docs/benchmarks/matrixark_locomo_debug_flow_debug.json
```

## 1. Pipeline Under Test

```text
one incoming conversation message
-> matrixark_ingest
-> raw ContextEvent + event embedding + node L0/L1 summaries/embeddings
-> session_buffer_event
-> matrixark_session_commit at conversation boundary
-> one-pass extraction over buffered source events
-> ContextSegment + ContextEntity + ContextSummary + ContextIndex
-> matrixark_retrieve(query)
-> L0/L1 ContextNode tree traversal
-> secondary-index prefilter
-> event/entity/segment scoring
-> ContextPackAudit
```

## 2. Run Summary

Model counts from the run:

```json
{
  "context_batch_commit": 3,
  "context_embedding": 144,
  "context_entity": 9,
  "context_entity_update_audit": 2,
  "context_event": 12,
  "context_extraction_audit": 3,
  "context_index": 24,
  "context_pack_audit": 9,
  "context_segment": 6,
  "context_summary": 117,
  "matrixark_audit_log": 24,
  "session_buffer_event": 12
}
```

High-level checks:

- Raw events are preserved: `context_event = 12`.
- Session commits do not duplicate raw events: each commit reports `events_written = 0` and `raw_events_duplicated = false`.
- One session batch can produce multiple segments: `context_segment = 6`.
- Evolving state is materialized: `context_entity = 9`.
- L0/L1 traversal data is present: `context_summary = 117`, `context_embedding = 144`.
- Secondary filters are present: `context_index = 24`.
- Retrieval is replayable: `context_pack_audit = 9`.

## 3. Conversations And Session Commit Results

### conversation_a_location_preference_approval

Scope:

```json
{
  "account_id": "acct_locomo_debug",
  "session_id": "locomo_a",
  "tenant_id": "tenant_memory",
  "user_id": "locomo_user_a"
}
```

Raw hook/event ingest results:

```json
[
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 4109012007957822100,
    "node_hash": 8055349922769951567,
    "pending": 1
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 8837567241745951633,
    "node_hash": 8055349922769951567,
    "pending": 2
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 3462847964236206031,
    "node_hash": 8055349922769951567,
    "pending": 3
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 2434307828575000503,
    "node_hash": 8055349922769951567,
    "pending": 4
  }
]
```

Session commit result:

```json
{
  "batch_id_hash": 3594571312706790369,
  "commit_id_hash": 2401017782631027298,
  "entities_written": 3,
  "events_written": 0,
  "indexes_written": 8,
  "raw_events_duplicated": false,
  "segments_written": 3,
  "source_event_ids": [
    4109012007957822100,
    8837567241745951633,
    3462847964236206031,
    2434307828575000503
  ],
  "status": "committed",
  "summary_hash": 5666341062667857588
}
```
### conversation_b_relationship_family_job

Scope:

```json
{
  "account_id": "acct_locomo_debug",
  "session_id": "locomo_b",
  "tenant_id": "tenant_memory",
  "user_id": "locomo_user_b"
}
```

Raw hook/event ingest results:

```json
[
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 3158054631515625843,
    "node_hash": 8055349922769951567,
    "pending": 1
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 2486448463476126726,
    "node_hash": 8055349922769951567,
    "pending": 2
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 5305440919480100567,
    "node_hash": 8055349922769951567,
    "pending": 3
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 2929071368754855336,
    "node_hash": 8055349922769951567,
    "pending": 4
  }
]
```

Session commit result:

```json
{
  "batch_id_hash": 5748753973677234196,
  "commit_id_hash": 3903289936087838679,
  "entities_written": 4,
  "events_written": 0,
  "indexes_written": 8,
  "raw_events_duplicated": false,
  "segments_written": 1,
  "source_event_ids": [
    3158054631515625843,
    2486448463476126726,
    5305440919480100567,
    2929071368754855336
  ],
  "status": "committed",
  "summary_hash": 960952954607837408
}
```
### conversation_c_temporal_update

Scope:

```json
{
  "account_id": "acct_locomo_debug",
  "session_id": "locomo_c",
  "tenant_id": "tenant_memory",
  "user_id": "locomo_user_c"
}
```

Raw hook/event ingest results:

```json
[
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 2471621481987065144,
    "node_hash": 8055349922769951567,
    "pending": 1
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 8576428530949693515,
    "node_hash": 8055349922769951567,
    "pending": 2
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 7901673189379945536,
    "node_hash": 8055349922769951567,
    "pending": 3
  },
  {
    "classification": "NEW_EVENT",
    "event_id_hash": 3506792510939175715,
    "node_hash": 8055349922769951567,
    "pending": 4
  }
]
```

Session commit result:

```json
{
  "batch_id_hash": 820053002934938583,
  "commit_id_hash": 4479495525952149348,
  "entities_written": 2,
  "events_written": 0,
  "indexes_written": 8,
  "raw_events_duplicated": false,
  "segments_written": 2,
  "source_event_ids": [
    2471621481987065144,
    8576428530949693515,
    7901673189379945536,
    3506792510939175715
  ],
  "status": "committed",
  "summary_hash": 5751404679254249214
}
```


## 4. Retrieval Debug Traces

### conversation_a_location_preference_approval: Where is the user currently located?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "current_state",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 4,
    "enabled": true,
    "matched_candidate_count": 6,
    "mode": "AND across groups, OR within each group",
    "required_groups": [
      [
        "entity_type:location"
      ]
    ]
  },
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 75
}
```

Top selected refs:

```json
[
  {
    "entity_type": "approval_state",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "ref_type": "entity",
    "score": 0.736883,
    "text": "approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget",
    "topic": null
  },
  {
    "entity_type": "location",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "ref_type": "entity",
    "score": 0.644501,
    "text": "location: location = Seattle today, please remember this location",
    "topic": null
  },
  {
    "entity_type": "preference",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "ref_type": "entity",
    "score": 0.628001,
    "text": "preference: preference = Rust for low latency storage engines",
    "topic": null
  }
]
```
### conversation_a_location_preference_approval: What does the user prefer for low latency storage?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "current_state",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 3,
    "enabled": true,
    "matched_candidate_count": 7,
    "mode": "AND across groups, OR within each group",
    "required_groups": [
      [
        "entity_type:preference",
        "event_type:preference_update"
      ]
    ]
  },
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 62
}
```

Top selected refs:

```json
[
  {
    "entity_type": "preference",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "ref_type": "entity",
    "score": 0.811711,
    "text": "preference: preference = Rust for low latency storage engines",
    "topic": null
  },
  {
    "entity_type": "approval_state",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "ref_type": "entity",
    "score": 0.733068,
    "text": "approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget",
    "topic": null
  },
  {
    "entity_type": "location",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "ref_type": "entity",
    "score": 0.642559,
    "text": "location: location = Seattle today, please remember this location",
    "topic": null
  }
]
```
### conversation_a_location_preference_approval: Who approved the GPU purchase?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "fact",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 3,
    "enabled": true,
    "matched_candidate_count": 7,
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
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 64
}
```

Top selected refs:

```json
[
  {
    "entity_type": "approval_state",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "ref_type": "entity",
    "score": 0.882042,
    "text": "approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget",
    "topic": null
  },
  {
    "entity_type": null,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "ref_type": "event",
    "score": 0.77478,
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget.",
    "topic": null
  },
  {
    "entity_type": null,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "ref_type": "segment",
    "score": 0.862227,
    "text": "3: Alice approved the GPU purchase after finance reviewed the budget.",
    "topic": "approval_budget"
  }
]
```
### conversation_b_relationship_family_job: Who is the user's manager?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "fact",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 4,
    "enabled": true,
    "matched_candidate_count": 5,
    "mode": "AND across groups, OR within each group",
    "required_groups": [
      [
        "entity_type:family_profile",
        "entity_type:relationship"
      ]
    ]
  },
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 60
}
```

Top selected refs:

```json
[
  {
    "entity_type": "current_plan",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.70916,
    "text": "current_plan: current_plan = to visit Berlin next month for the conference",
    "topic": null
  },
  {
    "entity_type": "relationship",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.709056,
    "text": "relationship: Priya is helping with the launch plan = Priya is helping with the launch plan",
    "topic": null
  },
  {
    "entity_type": "job_status",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.656168,
    "text": "job_status: job_status = role is storage infrastructure lead",
    "topic": null
  }
]
```
### conversation_b_relationship_family_job: What pet is in the user's family?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "fact",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 4,
    "enabled": true,
    "matched_candidate_count": 5,
    "mode": "AND across groups, OR within each group",
    "required_groups": [
      [
        "entity_type:family_profile",
        "entity_type:relationship"
      ]
    ]
  },
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 60
}
```

Top selected refs:

```json
[
  {
    "entity_type": "current_plan",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.715784,
    "text": "current_plan: current_plan = to visit Berlin next month for the conference",
    "topic": null
  },
  {
    "entity_type": "relationship",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.698287,
    "text": "relationship: Priya is helping with the launch plan = Priya is helping with the launch plan",
    "topic": null
  },
  {
    "entity_type": "job_status",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.677202,
    "text": "job_status: job_status = role is storage infrastructure lead",
    "topic": null
  }
]
```
### conversation_b_relationship_family_job: What is the user's current job role?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "current_state",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 3,
    "enabled": true,
    "matched_candidate_count": 6,
    "mode": "AND across groups, OR within each group",
    "required_groups": [
      [
        "entity_type:job_status",
        "event_type:status_update"
      ]
    ]
  },
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 68
}
```

Top selected refs:

```json
[
  {
    "entity_type": "job_status",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.732761,
    "text": "job_status: job_status = role is storage infrastructure lead",
    "topic": null
  },
  {
    "entity_type": "relationship",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.716634,
    "text": "relationship: Priya is helping with the launch plan = Priya is helping with the launch plan",
    "topic": null
  },
  {
    "entity_type": "current_plan",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "ref_type": "entity",
    "score": 0.690649,
    "text": "current_plan: current_plan = to visit Berlin next month for the conference",
    "topic": null
  }
]
```
### conversation_c_temporal_update: Where is the user currently located?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "current_state",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 4,
    "enabled": true,
    "matched_candidate_count": 4,
    "mode": "AND across groups, OR within each group",
    "required_groups": [
      [
        "entity_type:location"
      ]
    ]
  },
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 26
}
```

Top selected refs:

```json
[
  {
    "entity_type": "preference",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "entity",
    "score": 0.625863,
    "text": "preference: preference = Rust for backend services",
    "topic": null
  },
  {
    "entity_type": "location",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "entity",
    "score": 0.614863,
    "text": "location: location = Austin",
    "topic": null
  },
  {
    "entity_type": null,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "segment",
    "score": 0.651509,
    "text": "3: Actually I now prefer Rust for backend services.",
    "topic": "preference"
  }
]
```
### conversation_c_temporal_update: What language does the user currently prefer?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "current_state",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 2,
    "enabled": true,
    "matched_candidate_count": 6,
    "mode": "AND across groups, OR within each group",
    "required_groups": [
      [
        "entity_type:preference",
        "event_type:preference_update"
      ]
    ]
  },
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 42
}
```

Top selected refs:

```json
[
  {
    "entity_type": "location",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "entity",
    "score": 0.659162,
    "text": "location: location = Austin",
    "topic": null
  },
  {
    "entity_type": "preference",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "entity",
    "score": 0.626064,
    "text": "preference: preference = Rust for backend services",
    "topic": null
  },
  {
    "entity_type": null,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "segment",
    "score": 0.6698,
    "text": "3: Actually I now prefer Rust for backend services.",
    "topic": "preference"
  }
]
```
### conversation_c_temporal_update: Where was the user before April 10?

Retrieval controls:

```json
{
  "insufficient_context": false,
  "question_type": "fact",
  "secondary_index_filter": {
    "applied_before_embedding_scoring": true,
    "dropped_candidate_count": 4,
    "enabled": true,
    "matched_candidate_count": 4,
    "mode": "AND across groups, OR within each group",
    "required_groups": [
      [
        "entity_type:location"
      ]
    ]
  },
  "tree_traversal": {
    "enabled": true,
    "fallback_to_flat": false,
    "max_children_scored_per_parent": 10000,
    "selected_leaf_count": 2,
    "selected_node_count": 8,
    "selected_path_count": 8,
    "summary_embeddings": [
      "node_l0",
      "node_l1"
    ],
    "top_k_per_layer": 8
  },
  "used_context_tokens": 26
}
```

Top selected refs:

```json
[
  {
    "entity_type": "preference",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "entity",
    "score": 0.630227,
    "text": "preference: preference = Rust for backend services",
    "topic": null
  },
  {
    "entity_type": "location",
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "entity",
    "score": 0.619227,
    "text": "location: location = Austin",
    "topic": null
  },
  {
    "entity_type": null,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "ref_type": "segment",
    "score": 0.681273,
    "text": "1: On April 10 I moved to Austin.",
    "topic": "location"
  }
]
```


## 5. Data Written By Logical Model

### context_event

```json
[
  {
    "event_id_hash": 4109012007957822100,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: I moved to Seattle today, please remember this location.",
    "text": "user: I moved to Seattle today, please remember this location."
  },
  {
    "event_id_hash": 8837567241745951633,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: Actually I moved to Austin now for the new infra project.",
    "text": "user: Actually I moved to Austin now for the new infra project."
  },
  {
    "event_id_hash": 3462847964236206031,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: I prefer Rust for low latency storage engines.",
    "text": "user: I prefer Rust for low latency storage engines."
  },
  {
    "event_id_hash": 2434307828575000503,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: Alice approved the GPU purchase after finance reviewed the budget.",
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "event_id_hash": 3158054631515625843,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: My manager Priya is helping with the launch plan.",
    "text": "user: My manager Priya is helping with the launch plan."
  },
  {
    "event_id_hash": 2486448463476126726,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: My family has a dog named Mochi.",
    "text": "user: My family has a dog named Mochi."
  },
  {
    "event_id_hash": 5305440919480100567,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: My job role is storage infrastructure lead.",
    "text": "user: My job role is storage infrastructure lead."
  },
  {
    "event_id_hash": 2929071368754855336,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: I plan to visit Berlin next month for the conference.",
    "text": "user: I plan to visit Berlin next month for the conference."
  },
  {
    "event_id_hash": 2471621481987065144,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: On March 2 I lived in Seattle.",
    "text": "user: On March 2 I lived in Seattle."
  },
  {
    "event_id_hash": 8576428530949693515,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: On April 10 I moved to Austin.",
    "text": "user: On April 10 I moved to Austin."
  },
  {
    "event_id_hash": 7901673189379945536,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: I liked Python for dashboards before.",
    "text": "user: I liked Python for dashboards before."
  },
  {
    "event_id_hash": 3506792510939175715,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_event",
    "summary_text": "user: Actually I now prefer Rust for backend services.",
    "text": "user: Actually I now prefer Rust for backend services."
  }
]
```
### session_buffer_event

```json
[
  {
    "event_id_hash": 4109012007957822100,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 8837567241745951633,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 3462847964236206031,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 2434307828575000503,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 3158054631515625843,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 2486448463476126726,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 5305440919480100567,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 2929071368754855336,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 2471621481987065144,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 8576428530949693515,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 7901673189379945536,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 3506792510939175715,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  }
]
```
### context_batch_commit

```json
[
  {
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_batch_commit",
    "source_event_ids": [
      4109012007957822100,
      8837567241745951633,
      3462847964236206031,
      2434307828575000503
    ]
  },
  {
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_batch_commit",
    "source_event_ids": [
      3158054631515625843,
      2486448463476126726,
      5305440919480100567,
      2929071368754855336
    ]
  },
  {
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_batch_commit",
    "source_event_ids": [
      2471621481987065144,
      8576428530949693515,
      7901673189379945536,
      3506792510939175715
    ]
  }
]
```
### context_segment

```json
[
  {
    "coordinate_tuples": [
      [
        3,
        3
      ]
    ],
    "message_indexes": [
      3
    ],
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_segment",
    "segment_hash": 4564271584891971745,
    "source_event_ids": [
      2434307828575000503
    ],
    "summary_text": "3: Alice approved the GPU purchase after finance reviewed the budget.",
    "text": "3: Alice approved the GPU purchase after finance reviewed the budget.",
    "topic": "approval_budget"
  },
  {
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
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_segment",
    "segment_hash": 8120192491446508597,
    "source_event_ids": [
      4109012007957822100,
      8837567241745951633
    ],
    "summary_text": "0: I moved to Seattle today, please remember this location. 1: Actually I moved to Austin now for the new infra project.",
    "text": "0: I moved to Seattle today, please remember this location.\n1: Actually I moved to Austin now for the new infra project.",
    "topic": "location"
  },
  {
    "coordinate_tuples": [
      [
        2,
        2
      ]
    ],
    "message_indexes": [
      2
    ],
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_segment",
    "segment_hash": 6678307946829922074,
    "source_event_ids": [
      3462847964236206031
    ],
    "summary_text": "2: I prefer Rust for low latency storage engines.",
    "text": "2: I prefer Rust for low latency storage engines.",
    "topic": "preference"
  },
  {
    "coordinate_tuples": [
      [
        0,
        0
      ],
      [
        3,
        3
      ]
    ],
    "message_indexes": [
      0,
      3
    ],
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_segment",
    "segment_hash": 6607889882927428667,
    "source_event_ids": [
      3158054631515625843,
      2929071368754855336
    ],
    "summary_text": "0: My manager Priya is helping with the launch plan. 3: I plan to visit Berlin next month for the conference.",
    "text": "0: My manager Priya is helping with the launch plan.\n3: I plan to visit Berlin next month for the conference.",
    "topic": "plan_status"
  },
  {
    "coordinate_tuples": [
      [
        3,
        3
      ]
    ],
    "message_indexes": [
      3
    ],
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_segment",
    "segment_hash": 2990144363128382229,
    "source_event_ids": [
      3506792510939175715
    ],
    "summary_text": "3: Actually I now prefer Rust for backend services.",
    "text": "3: Actually I now prefer Rust for backend services.",
    "topic": "preference"
  },
  {
    "coordinate_tuples": [
      [
        1,
        1
      ]
    ],
    "message_indexes": [
      1
    ],
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_segment",
    "segment_hash": 3721282551382278212,
    "source_event_ids": [
      8576428530949693515
    ],
    "summary_text": "1: On April 10 I moved to Austin.",
    "text": "1: On April 10 I moved to Austin.",
    "topic": "location"
  }
]
```
### context_entity

```json
[
  {
    "entity_hash": 4950599206796422882,
    "entity_name": "preference",
    "entity_type": "preference",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      4109012007957822100,
      8837567241745951633,
      3462847964236206031,
      2434307828575000503
    ],
    "source_refs": [
      "4109012007957822100",
      "8837567241745951633",
      "3462847964236206031",
      "2434307828575000503"
    ],
    "state": "Rust for low latency storage engines"
  },
  {
    "entity_hash": 1095861174750854234,
    "entity_name": "location",
    "entity_type": "location",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      4109012007957822100,
      8837567241745951633,
      3462847964236206031,
      2434307828575000503
    ],
    "source_refs": [
      "4109012007957822100",
      "8837567241745951633",
      "3462847964236206031",
      "2434307828575000503"
    ],
    "state": "Seattle today, please remember this location"
  },
  {
    "entity_hash": 2340647924214384547,
    "entity_name": "the GPU purchase after finance reviewed the budget",
    "entity_type": "approval_state",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      4109012007957822100,
      8837567241745951633,
      3462847964236206031,
      2434307828575000503
    ],
    "source_refs": [
      "4109012007957822100",
      "8837567241745951633",
      "3462847964236206031",
      "2434307828575000503"
    ],
    "state": "the GPU purchase after finance reviewed the budget"
  },
  {
    "entity_hash": 7468951861799448500,
    "entity_name": "Priya is helping with the launch plan",
    "entity_type": "relationship",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3158054631515625843,
      2486448463476126726,
      5305440919480100567,
      2929071368754855336
    ],
    "source_refs": [
      "3158054631515625843",
      "2486448463476126726",
      "5305440919480100567",
      "2929071368754855336"
    ],
    "state": "Priya is helping with the launch plan"
  },
  {
    "entity_hash": 5709091068117281195,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3158054631515625843,
      2486448463476126726,
      5305440919480100567,
      2929071368754855336
    ],
    "source_refs": [
      "3158054631515625843",
      "2486448463476126726",
      "5305440919480100567",
      "2929071368754855336"
    ],
    "state": "role is storage infrastructure lead"
  },
  {
    "entity_hash": 1274680141737714062,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3158054631515625843,
      2486448463476126726,
      5305440919480100567,
      2929071368754855336
    ],
    "source_refs": [
      "3158054631515625843",
      "2486448463476126726",
      "5305440919480100567",
      "2929071368754855336"
    ],
    "state": "to visit Berlin next month for the confere
... truncated in doc; see docs/benchmarks/matrixark_locomo_debug_flow_debug.json
```
### context_summary

```json
[
  {
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_summary",
    "summary_text": "default_team :: user: I moved to Seattle today, please remember this location.",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_summary",
    "summary_text": "Context node default_team. Overview: user: I moved to Seattle today, please remember this location.. This node belongs to path default_team and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_summary",
    "summary_text": "default_team / default_project :: user: I moved to Seattle today, please remember this location.",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_summary",
    "summary_text": "Context node default_team / default_project. Overview: user: I moved to Seattle today, please remember this location.. This node belongs to path default_team / default_project and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_summary",
    "summary_text": "default_team / default_project / message :: user: I moved to Seattle today, please remember this location.",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_summary",
    "summary_text": "Context node default_team / default_project / message. Overview: user: I moved to Seattle today, please remember this location.. This node belongs to path default_team / default_project / message and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_summary",
    "summary_hash": 2962234381734522119,
    "summary_text": "user: I moved to Seattle today, please remember this location.",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_summary",
    "summary_text": "default_team :: user: Actually I moved to Austin now for the new infra project.",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_summary",
    "summary_text": "Context node default_team. Overview: user: Actually I moved to Austin now for the new infra project.. This node belongs to path default_team and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_summary",
    "summary_text": "default_team / default_project :: user: Actually I moved to Austin now for the new infra project.",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_summary",
    "summary_text": "Context node default_team / default_project. Overview: user: Actually I moved to Austin now for the new infra project.. This node belongs to path default_team / default_project and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_summary",
    "summary_text": "default_team / default_project / message :: user: Actually I moved to Austin now for the new infra project.",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "mess
... truncated in doc; see docs/benchmarks/matrixark_locomo_debug_flow_debug.json
```
### context_embedding

```json
[
  {
    "embedding_type": "node_l0",
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.229416,
      0.0,
      0.0,
      0.0,
      0.229416,
      0.0
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      -0.145865,
      0.0,
      0.145865,
      0.0
    ]
  },
  {
    "embedding_type": "node_l0",
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.235702,
      0.0,
      0.0,
      0.0,
      0.235702,
      0.0
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      -0.152499,
      0.0,
      0.152499,
      0.0
    ]
  },
  {
    "embedding_type": "node_l0",
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.229416,
      0.0,
      0.0,
      0.0,
      0.229416,
      0.0
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      -0.145865,
      0.0,
      0.145865,
      0.0
    ]
  },
  {
    "embedding_type": "session_l0",
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.235702,
      0.0,
      0.0,
      0.0,
      0.235702,
      0.0
    ]
  },
  {
    "embedding_type": "event_text",
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.235702,
      0.0,
      0.0,
      0.0,
      0.235702,
      0.0
    ]
  },
  {
    "embedding_type": "node_l0",
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.654654,
      0.0
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      -0.13484,
      0.0,
      -0.13484,
      0.0,
      0.40452,
      0.0
    ]
  },
  {
    "embedding_type": "node_l0",
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.67082,
      0.0
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      -0.140028,
      0.0,
      -0.140028,
      0.0,
      0.420084,
      0.0
    ]
  },
  {
    "embedding_type": "node_l0",
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.625543,
      0.0
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      -0.130189,
      0.0,
      -0.130189,
      0.0,
      0.390567,
      0.0
    ]
  },
  {
    "embedding_
... truncated in doc; see docs/benchmarks/matrixark_locomo_debug_flow_debug.json
```
### context_index

```json
[
  {
    "index_name": "event_type:confirmation",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "classification:batch_memory",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "status:observed",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "source_type:message",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:preference",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:location",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:approval_state",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "segment_topic:approval_budget",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "event_type:plan_update",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "classification:batch_memory",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "status:observed",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "source_type:message",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:relationship",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:job_status",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:current_plan",
    "node_hash": 4200235650375758793,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory"
... truncated in doc; see docs/benchmarks/matrixark_locomo_debug_flow_debug.json
```
### context_pack_audit

```json
[
  {
    "record_type": "context_pack_audit",
    "summary_text": "approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget location: location = Seattle today, please remember this location preference: preference = Rust for low latency storage engines 3: Alice approved the GPU purchase after finance reviewed the budget. 0: I moved to Seattle today, please remember this location. 1: Actually I moved to Austin now for the new infra project. 2: I prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "preference: preference = Rust for low latency storage engines approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget location: location = Seattle today, please remember this location 2: I prefer Rust for low latency storage engines. user: I prefer Rust for low latency storage engines. 3: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget user: Alice approved the GPU purchase after finance reviewed the budget. 3: Alice approved the GPU purchase after finance reviewed the budget. preference: preference = Rust for low latency storage engines location: location = Seattle today, please remember this location 2: I prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "current_plan: current_plan = to visit Berlin next month for the conference relationship: Priya is helping with the launch plan = Priya is helping with the launch plan job_status: job_status = role is storage infrastructure lead family_profile: family_profile = has a dog named Mochi 0: My manager Priya is helping with the launch plan. 3: I plan to visit Berlin next month for the conference."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "current_plan: current_plan = to visit Berlin next month for the conference relationship: Priya is helping with the launch plan = Priya is helping with the launch plan job_status: job_status = role is storage infrastructure lead family_profile: family_profile = has a dog named Mochi 0: My manager Priya is helping with the launch plan. 3: I plan to visit Berlin next month for the conference."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "job_status: job_status = role is storage infrastructure lead relationship: Priya is helping with the launch plan = Priya is helping with the launch plan current_plan: current_plan = to visit Berlin next month for the conference family_profile: family_profile = has a dog named Mochi user: My job role is storage infrastructure lead. 0: My manager Priya is helping with the launch plan. 3: I plan to visit Berlin next month for the conference."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "preference: preference = Rust for backend services location: location = Austin 3: Actually I now prefer Rust for backend services. 1: On April 10 I moved to Austin."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "location: location = Austin preference: preference = Rust for backend services 3: Actually I now prefer Rust for backend services. 1: On April 10 I moved to Austin. user: Actually I now prefer Rust for backend services. user: I liked Python for dashboards before."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "preference: preference = Rust for backend services location: location = Austin 1: On April 10 I moved to Austin. 3: Actually I now prefer Rust for backend services."
  }
]
```


## 6. What This Proves

- Single hook messages can be captured one by one without forcing immediate batch extraction.
- Conversation end/session commit can derive multiple topic segments from one buffered session.
- Segments/entities/summaries reference `source_event_ids`, so raw evidence remains replayable.
- L0/L1 node summaries and embeddings are generated for tree traversal.
- Query understanding derives secondary-index filters such as `entity_type:location`, `entity_type:preference`, and `segment_topic:approval_budget`.
- Retrieval applies tree traversal plus secondary-index filtering before leaf scoring.

## 7. How To Reproduce

```bash
cd /root/src/github-services/TemporalStore
PYTHONPATH=. python3 tools/run_matrixark_locomo_debug_flow.py > /tmp/matrixark_locomo_debug_flow.json
```

To regenerate this doc from the JSON, rerun the same debug command and update `docs/matrixark_locomo_debug_data_flow.md`.
