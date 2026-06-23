# MatrixArk LOCOMO Debug Data Flow

Event log: `.local/context-debug/locomo-oss-docker-debug/matrixark_locomo_debug_event_log.jsonl`
Embedding provider: `oss`
Embedding model: `/models/sentence-transformers/all-MiniLM-L6-v2`

## Data Model Counts

- `context_batch_commit`: 3
- `context_embedding`: 90
- `context_entity`: 9
- `context_entity_update_audit`: 9
- `context_event`: 12
- `context_extraction_audit`: 3
- `context_index`: 24
- `context_pack_audit`: 9
- `context_segment`: 6
- `context_summary`: 63
- `context_summary_dirty`: 51
- `context_summary_refresh_audit`: 24
- `matrixark_audit_log`: 27
- `session_buffer_event`: 12

## Retrieval Queries

### Where is the user currently located?
- session: `conversation_a_location_preference_approval`
- question_type: `current_state`
- context_pack_id: `2729108047481238049`
- used_context_tokens: `9`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `entity` score=0.700977 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_a', 'collection:sessions', 'session:locomo_a'] text=location: location = Austin now for the new infra project

### What does the user prefer for low latency storage?
- session: `conversation_a_location_preference_approval`
- question_type: `current_state`
- context_pack_id: `6780686362125065011`
- used_context_tokens: `17`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `entity` score=0.832907 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_a', 'collection:sessions', 'session:locomo_a'] text=preference: preference = Rust for low latency storage engines
  - `event` score=0.757172 node=['default_team', 'default_project', 'message'] text=user: I prefer Rust for low latency storage engines.

### Who approved the GPU purchase?
- session: `conversation_a_location_preference_approval`
- question_type: `fact`
- context_pack_id: `1209877608356494786`
- used_context_tokens: `39`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `entity` score=0.915328 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_a', 'collection:sessions', 'session:locomo_a'] text=approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget
  - `event` score=0.795768 node=['default_team', 'default_project', 'message'] text=user: Alice approved the GPU purchase after finance reviewed the budget.
  - `segment` score=0.891865 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_a', 'collection:sessions', 'session:locomo_a'] text=3: Alice approved the GPU purchase after finance reviewed the budget.

### Who is the user's manager?
- session: `conversation_b_relationship_family_job`
- question_type: `fact`
- context_pack_id: `5448656841211536278`
- used_context_tokens: `22`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `entity` score=0.711964 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_b', 'collection:sessions', 'session:locomo_b'] text=relationship: Priya is helping with the launch plan = Priya is helping with the launch plan
  - `entity` score=0.656002 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_b', 'collection:sessions', 'session:locomo_b'] text=family_profile: family_profile = has a dog named Mochi

### What pet is in the user's family?
- session: `conversation_b_relationship_family_job`
- question_type: `fact`
- context_pack_id: `7658615883332866040`
- used_context_tokens: `22`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `entity` score=0.714821 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_b', 'collection:sessions', 'session:locomo_b'] text=family_profile: family_profile = has a dog named Mochi
  - `entity` score=0.691324 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_b', 'collection:sessions', 'session:locomo_b'] text=relationship: Priya is helping with the launch plan = Priya is helping with the launch plan

### What is the user's current job role?
- session: `conversation_b_relationship_family_job`
- question_type: `current_state`
- context_pack_id: `2607653875530177420`
- used_context_tokens: `15`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `entity` score=0.753743 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_b', 'collection:sessions', 'session:locomo_b'] text=job_status: job_status = role is storage infrastructure lead
  - `event` score=0.710748 node=['default_team', 'default_project', 'message'] text=user: My job role is storage infrastructure lead.

### Where is the user currently located?
- session: `conversation_c_temporal_update`
- question_type: `current_state`
- context_pack_id: `3865606364986862534`
- used_context_tokens: `3`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `entity` score=0.668919 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_c', 'collection:sessions', 'session:locomo_c'] text=location: location = Austin

### What language does the user currently prefer?
- session: `conversation_c_temporal_update`
- question_type: `current_state`
- context_pack_id: `8213547396988274382`
- used_context_tokens: `22`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `entity` score=0.663532 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_c', 'collection:sessions', 'session:locomo_c'] text=preference: preference = Rust for backend services
  - `event` score=0.606905 node=['default_team', 'default_project', 'message'] text=user: Actually I now prefer Rust for backend services.
  - `event` score=0.576756 node=['default_team', 'default_project', 'message'] text=user: I liked Python for dashboards before.

### Where was the user before April 10?
- session: `conversation_c_temporal_update`
- question_type: `date`
- context_pack_id: `5823124350559962024`
- used_context_tokens: `35`
- tree traversal: selected_nodes=8 selected_paths=8 fallback=False
- selected refs:
  - `event` score=0.671345 node=['default_team', 'default_project', 'message'] text=user: On April 10 I moved to Austin.
  - `event` score=0.636438 node=['default_team', 'default_project', 'message'] text=user: On March 2 I lived in Seattle.
  - `event` score=0.607561 node=['default_team', 'default_project', 'message'] text=user: I liked Python for dashboards before.
  - `entity` score=0.660851 node=['account:acct_locomo_debug', 'tenant:tenant_memory', 'principal:user:locomo_user_c', 'collection:sessions', 'session:locomo_c'] text=location: location = Austin
  - `event` score=0.554599 node=['default_team', 'default_project', 'message'] text=user: Actually I now prefer Rust for backend services.


## Compact Records By Model

### context_event
```json
[
  {
    "event_id_hash": 7570832604580476177,
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
    "event_id_hash": 7311142423822763961,
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
    "event_id_hash": 6138936802764366545,
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
    "event_id_hash": 5941503599347645105,
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
    "event_id_hash": 920277984218572951,
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
    "event_id_hash": 6389707507579638321,
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
    "event_id_hash": 247641261204072080,
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
    "event_id_hash": 9124681662871465807,
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
    "event_id_hash": 1973686423928385754,
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
    "event_id_hash": 242956858601536488,
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
    "event_id_hash": 266141310369888037,
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
    "event_id_hash": 2387217973418514035,
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
    "event_id_hash": 7570832604580476177,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 7311142423822763961,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 6138936802764366545,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 5941503599347645105,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 920277984218572951,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 6389707507579638321,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 247641261204072080,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 9124681662871465807,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 1973686423928385754,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 242956858601536488,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 266141310369888037,
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 2387217973418514035,
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
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
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
      920277984218572951,
      6389707507579638321,
      247641261204072080,
      9124681662871465807
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
      1973686423928385754,
      242956858601536488,
      266141310369888037,
      2387217973418514035
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
    "segment_hash": 5396457113987538536,
    "source_event_ids": [
      5941503599347645105
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
    "segment_hash": 1836260830955661456,
    "source_event_ids": [
      7570832604580476177,
      7311142423822763961
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
    "segment_hash": 592407927067584115,
    "source_event_ids": [
      6138936802764366545
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
    "segment_hash": 3182940402083000845,
    "source_event_ids": [
      920277984218572951,
      9124681662871465807
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
    "segment_hash": 7481300389678453768,
    "source_event_ids": [
      2387217973418514035
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
    "segment_hash": 4574550720328123077,
    "source_event_ids": [
      242956858601536488
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
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "source_refs": [
      "7570832604580476177",
      "7311142423822763961",
      "6138936802764366545",
      "5941503599347645105"
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
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "source_refs": [
      "7570832604580476177",
      "7311142423822763961",
      "6138936802764366545",
      "5941503599347645105"
    ],
    "state": "Austin now for the new infra project"
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
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "source_refs": [
      "7570832604580476177",
      "7311142423822763961",
      "6138936802764366545",
      "5941503599347645105"
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
      920277984218572951,
      6389707507579638321,
      247641261204072080,
      9124681662871465807
    ],
    "source_refs": [
      "920277984218572951",
      "6389707507579638321",
      "247641261204072080",
      "9124681662871465807"
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
      920277984218572951,
      6389707507579638321,
      247641261204072080,
      9124681662871465807
    ],
    "source_refs": [
      "920277984218572951",
      "6389707507579638321",
      "247641261204072080",
      "9124681662871465807"
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
      920277984218572951,
      6389707507579638321,
      247641261204072080,
      9124681662871465807
    ],
    "source_refs": [
      "920277984218572951",
      "6389707507579638321",
      "247641261204072080",
      "9124681662871465807"
    ],
    "state": "to visit Berlin next month for the conference"
  },
  {
    "entity_hash": 4728765086146969721,
    "entity_name": "family_profile",
    "entity_type": "family_profile",
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
      920277984218572951,
      6389707507579638321,
      247641261204072080,
      9124681662871465807
    ],
    "source_refs": [
      "920277984218572951",
      "6389707507579638321",
      "247641261204072080",
      "9124681662871465807"
    ],
    "state": "has a dog named Mochi"
  },
  {
    "entity_hash": 5393950348500104652,
    "entity_name": "preference",
    "entity_type": "preference",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      1973686423928385754,
      242956858601536488,
      266141310369888037,
      2387217973418514035
    ],
    "source_refs": [
      "1973686423928385754",
      "242956858601536488",
      "266141310369888037",
      "2387217973418514035"
    ],
    "state": "Rust for backend services"
  },
  {
    "entity_hash": 1127864556131246544,
    "entity_name": "location",
    "entity_type": "location",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      1973686423928385754,
      242956858601536488,
      266141310369888037,
      2387217973418514035
    ],
    "source_refs": [
      "1973686423928385754",
      "242956858601536488",
      "266141310369888037",
      "2387217973418514035"
    ],
    "state": "Austin"
  }
]
```

### context_summary
```json
[
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
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_summary",
    "summary_hash": 2962234381734522119,
    "summary_text": "user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project.",
    "summary_type": "session_l0"
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
    "summary_text": "user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. use...",
    "summary_type": "session_l0"
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
    "summary_text": "user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. use...",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "summary_hash": 8726806307546336116,
    "summary_text": "user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.",
    "summary_type": "batch_l0"
  },
  {
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "summary_text": "default_team :: user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actual...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "summary_text": "Context node default_team. Overview: user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. use... user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path default_team and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "summary_text": "default_team / default_project :: user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this loca...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 1644032532652301419,
    "node_path": [
      "default_team",
      "default_project"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "summary_text": "Context node default_team / default_project. Overview: user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. use... user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path default_team / default_project and should be used for tree-first retrieval before leaf event/entity recall.",
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
    "source_event_ids": [
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "summary_text": "default_team / default_project / message :: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage en...",
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
    "source_event_ids": [
      7570832604580476177,
      7311142423822763961,
      6138936802764366545,
      5941503599347645105
    ],
    "summary_text": "Context node default_team / default_project / message. Overview: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path default_team / default_project / message and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 8693054764298772795,
    "node_path": [
      "account:acct_locomo_debug"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "account:acct_locomo_debug :: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Al...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 8693054764298772795,
    "node_path": [
      "account:acct_locomo_debug"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "Context node account:acct_locomo_debug. Overview: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path account:acct_locomo_debug and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 5168753367304870008,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "account:acct_locomo_debug / tenant:tenant_memory :: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency st...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 5168753367304870008,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "Context node account:acct_locomo_debug / tenant:tenant_memory. Overview: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path account:acct_locomo_debug / tenant:tenant_memory and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 2876112719397074770,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "account:acct_locomo_debug / tenant:tenant_memory / principal:user:locomo_user_a :: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 2876112719397074770,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "Context node account:acct_locomo_debug / tenant:tenant_memory / principal:user:locomo_user_a. Overview: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path account:acct_locomo_debug / tenant:tenant_memory / principal:user:locomo_user_a and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 3637146953197469304,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "account:acct_locomo_debug / tenant:tenant_memory / principal:user:locomo_user_a / collection:sessions :: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new ...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 3637146953197469304,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "Context node account:acct_locomo_debug / tenant:tenant_memory / principal:user:locomo_user_a / collection:sessions. Overview: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path account:acct_locomo_debug / tenant:tenant_memory / principal:user:locomo_user_a / collection:sessions and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [],
    "summary_text": "account:acct_locomo_debug / tenant:tenant_memory / principal:user:locomo_user_a / collection:sessions / session:locomo_a :: account:acct_locomo_debug tenant:tenant_memory principal:user:locomo_user_a collection:sessio...",
    "summary_type": "node_l0"
  }
]
```

### context_embedding
```json
[
  {
    "embedding_type": "session_l0",
    "node_hash": 8055349922769951567,
    "node_path": [
      "default_team",
      "default_project",
      "message"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      0.110351,
      0.033211,
      0.034226,
      0.029132,
      -0.019359,
      0.064912
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
    "vector_dim": 384,
    "vector_preview": [
      0.110351,
      0.033211,
      0.034226,
      0.029132,
      -0.019359,
      0.064912
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
    "vector_dim": 384,
    "vector_preview": [
      0.059204,
      -0.007162,
      0.025576,
      0.018365,
      0.003706,
      0.031547
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
    "vector_dim": 384,
    "vector_preview": [
      -0.034345,
      -0.057782,
      0.106371,
      0.042288,
      -0.013944,
      -0.109066
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
    "vector_dim": 384,
    "vector_preview": [
      0.037567,
      -0.009227,
      0.008283,
      0.016594,
      0.011206,
      0.045565
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
    "vector_dim": 384,
    "vector_preview": [
      -0.097252,
      0.03182,
      -0.019792,
      0.071623,
      0.012002,
      -0.080943
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
    "vector_dim": 384,
    "vector_preview": [
      0.037567,
      -0.009227,
      0.008283,
      0.016594,
      0.011206,
      0.045565
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
    "vector_dim": 384,
    "vector_preview": [
      -0.028551,
      0.060351,
      -0.082751,
      0.01934,
      0.001187,
      -0.0431
    ]
  },
  {
    "embedding_type": "entity_state",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      -0.085584,
      0.033689,
      -0.001248,
      0.080202,
      -0.003265,
      -0.039969
    ]
  },
  {
    "embedding_type": "entity_state",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      -0.010166,
      -0.024317,
      0.068798,
      0.049037,
      0.003507,
      -0.042855
    ]
  },
  {
    "embedding_type": "entity_state",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      -0.03733,
      0.03683,
      -0.04369,
      0.048769,
      0.065121,
      -0.059191
    ]
  },
  {
    "embedding_type": "segment_text",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      -0.033274,
      0.039726,
      -0.081958,
      0.021489,
      0.057272,
      -0.055093
    ]
  },
  {
    "embedding_type": "segment_text",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      0.060018,
      -0.021845,
      0.054004,
      0.043016,
      0.013539,
      0.005257
    ]
  },
  {
    "embedding_type": "segment_text",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      -0.096845,
      0.033835,
      -0.013383,
      0.062101,
      0.026017,
      -0.039021
    ]
  },
  {
    "embedding_type": "batch_l0",
    "node_hash": 2605184825361232507,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_a",
      "collection:sessions",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      0.043868,
      0.000451,
      -0.050493,
      0.06084,
      -0.010749,
      -0.073809
    ]
  },
  {
    "embedding_type": "node_l0",
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      0.067036,
      -0.010558,
      -0.037046,
      -0.001407,
      0.005859,
      0.098174
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 8066816596465017513,
    "node_path": [
      "default_team"
    ],
    "record_type": "context_embedding",
    "vector_dim": 384,
    "vector_preview": [
      0.020495,
      -0.012829,
      -0.017933,
      0.075714,
      0.116219,
      0.018423
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
    "vector_dim": 384,
    "vector_preview": [
      0.062221,
      0.004917,
      -0.030778,
      -0.018147,
      0.016155,
      0.087011
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
    "vector_dim": 384,
    "vector_preview": [
      0.026684,
      -0.012139,
      -0.026245,
      0.05878,
      0.100001,
      0.003813
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
    "vector_dim": 384,
    "vector_preview": [
      0.005397,
      -0.014132,
      -0.000627,
      0.025267,
      0.049371,
      -0.047566
    ]
  }
]
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
      "tenant:tenant_memory",
      "principal:user:locomo_user_b",
      "collection:sessions",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:family_profile",
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
    "index_name": "event_type:preference_update",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "classification:batch_memory",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "status:observed",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "source_type:message",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:preference",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:location",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "segment_topic:preference",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "segment_topic:location",
    "node_hash": 4722027507052340619,
    "node_path": [
      "account:acct_locomo_debug",
      "tenant:tenant_memory",
      "principal:user:locomo_user_c",
      "collection:sessions",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  }
]
```

### context_pack_audit
```json
[
  {
    "record_type": "context_pack_audit",
    "summary_text": "location: location = Austin now for the new infra project"
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "preference: preference = Rust for low latency storage engines user: I prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget user: Alice approved the GPU purchase after finance reviewed the budget. 3: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "relationship: Priya is helping with the launch plan = Priya is helping with the launch plan family_profile: family_profile = has a dog named Mochi"
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "family_profile: family_profile = has a dog named Mochi relationship: Priya is helping with the launch plan = Priya is helping with the launch plan"
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "job_status: job_status = role is storage infrastructure lead user: My job role is storage infrastructure lead."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "location: location = Austin"
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "preference: preference = Rust for backend services user: Actually I now prefer Rust for backend services. user: I liked Python for dashboards before."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "user: On April 10 I moved to Austin. user: On March 2 I lived in Seattle. user: I liked Python for dashboards before. location: location = Austin user: Actually I now prefer Rust for backend services."
  }
]
```

