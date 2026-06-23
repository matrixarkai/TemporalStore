# MatrixArk LOCOMO Debug Data Flow

Backend: `temporalstore-direct`
Event log: ``
Storage prefix: `matrixark:locomo:parity:cpp:20260623b`
Embedding provider: `hash`
Embedding model: `matrixark-local-token-hash-v1`

## Data Model Counts

- `context_batch_commit`: 3
- `context_child_ref`: 3
- `context_embedding`: 54
- `context_entity`: 9
- `context_entity_update_audit`: 9
- `context_event`: 12
- `context_extraction_audit`: 3
- `context_index`: 24
- `context_node`: 6
- `context_pack_audit`: 9
- `context_segment`: 6
- `context_summary`: 27
- `context_summary_dirty`: 30
- `context_summary_refresh_audit`: 6
- `matrixark_audit_log`: 27
- `session_buffer_event`: 12

## Retrieval Queries

### Where is the user currently located?
- session: `conversation_a_location_preference_approval`
- question_type: `current_state`
- context_pack_id: `2255688379099717051`
- used_context_tokens: `51`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `entity` score=0.719987 node=['user:locomo_user_a', 'session:locomo_a'] text=location: location = Austin now for the new infra project
  - `event` score=0.66144 node=['user:locomo_user_a', 'session:locomo_a'] text=user: Actually I moved to Austin now for the new infra project.
  - `event` score=0.633413 node=['user:locomo_user_a', 'session:locomo_a'] text=user: Alice approved the GPU purchase after finance reviewed the budget.
  - `event` score=0.584541 node=['user:locomo_user_a', 'session:locomo_a'] text=user: I moved to Seattle today, please remember this location.
  - `event` score=0.57315 node=['user:locomo_user_a', 'session:locomo_a'] text=user: I prefer Rust for low latency storage engines.

### What does the user prefer for low latency storage?
- session: `conversation_a_location_preference_approval`
- question_type: `current_state`
- context_pack_id: `2119156336233804482`
- used_context_tokens: `50`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `entity` score=0.808072 node=['user:locomo_user_a', 'session:locomo_a'] text=preference: preference = Rust for low latency storage engines
  - `event` score=0.754587 node=['user:locomo_user_a', 'session:locomo_a'] text=user: I prefer Rust for low latency storage engines.
  - `event` score=0.648377 node=['user:locomo_user_a', 'session:locomo_a'] text=user: Actually I moved to Austin now for the new infra project.
  - `event` score=0.597016 node=['user:locomo_user_a', 'session:locomo_a'] text=user: Alice approved the GPU purchase after finance reviewed the budget.
  - `event` score=0.553848 node=['user:locomo_user_a', 'session:locomo_a'] text=user: I moved to Seattle today, please remember this location.

### Who approved the GPU purchase?
- session: `conversation_a_location_preference_approval`
- question_type: `fact`
- context_pack_id: `614429369314433100`
- used_context_tokens: `70`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `entity` score=0.878533 node=['user:locomo_user_a', 'session:locomo_a'] text=approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget
  - `event` score=0.769999 node=['user:locomo_user_a', 'session:locomo_a'] text=user: Alice approved the GPU purchase after finance reviewed the budget.
  - `segment` score=0.859596 node=['user:locomo_user_a', 'session:locomo_a'] text=3: Alice approved the GPU purchase after finance reviewed the budget.
  - `event` score=0.558171 node=['user:locomo_user_a', 'session:locomo_a'] text=user: Actually I moved to Austin now for the new infra project.
  - `event` score=0.524268 node=['user:locomo_user_a', 'session:locomo_a'] text=user: I prefer Rust for low latency storage engines.

### Who is the user's manager?
- session: `conversation_b_relationship_family_job`
- question_type: `fact`
- context_pack_id: `8290909756204640182`
- used_context_tokens: `59`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `event` score=0.772085 node=['user:locomo_user_b', 'session:locomo_b'] text=user: My manager Priya is helping with the launch plan.
  - `entity` score=0.711467 node=['user:locomo_user_b', 'session:locomo_b'] text=relationship: Priya is helping with the launch plan = Priya is helping with the launch plan
  - `event` score=0.630471 node=['user:locomo_user_b', 'session:locomo_b'] text=user: I plan to visit Berlin next month for the conference.
  - `entity` score=0.628478 node=['user:locomo_user_b', 'session:locomo_b'] text=family_profile: family_profile = has a dog named Mochi
  - `event` score=0.624482 node=['user:locomo_user_b', 'session:locomo_b'] text=user: My job role is storage infrastructure lead.

### What pet is in the user's family?
- session: `conversation_b_relationship_family_job`
- question_type: `fact`
- context_pack_id: `8324063939210716832`
- used_context_tokens: `59`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `entity` score=0.701094 node=['user:locomo_user_b', 'session:locomo_b'] text=relationship: Priya is helping with the launch plan = Priya is helping with the launch plan
  - `event` score=0.65611 node=['user:locomo_user_b', 'session:locomo_b'] text=user: My manager Priya is helping with the launch plan.
  - `entity` score=0.649907 node=['user:locomo_user_b', 'session:locomo_b'] text=family_profile: family_profile = has a dog named Mochi
  - `event` score=0.647548 node=['user:locomo_user_b', 'session:locomo_b'] text=user: My family has a dog named Mochi.
  - `event` score=0.626991 node=['user:locomo_user_b', 'session:locomo_b'] text=user: I plan to visit Berlin next month for the conference.

### What is the user's current job role?
- session: `conversation_b_relationship_family_job`
- question_type: `current_state`
- context_pack_id: `6484199105046176547`
- used_context_tokens: `44`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `entity` score=0.733181 node=['user:locomo_user_b', 'session:locomo_b'] text=job_status: job_status = role is storage infrastructure lead
  - `event` score=0.716583 node=['user:locomo_user_b', 'session:locomo_b'] text=user: My job role is storage infrastructure lead.
  - `event` score=0.660282 node=['user:locomo_user_b', 'session:locomo_b'] text=user: My manager Priya is helping with the launch plan.
  - `event` score=0.576972 node=['user:locomo_user_b', 'session:locomo_b'] text=user: My family has a dog named Mochi.
  - `event` score=0.575076 node=['user:locomo_user_b', 'session:locomo_b'] text=user: I plan to visit Berlin next month for the conference.

### Where is the user currently located?
- session: `conversation_c_temporal_update`
- question_type: `current_state`
- context_pack_id: `8724482723790144631`
- used_context_tokens: `35`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `entity` score=0.617421 node=['user:locomo_user_c', 'session:locomo_c'] text=location: location = Austin
  - `event` score=0.601757 node=['user:locomo_user_c', 'session:locomo_c'] text=user: On March 2 I lived in Seattle.
  - `event` score=0.589183 node=['user:locomo_user_c', 'session:locomo_c'] text=user: Actually I now prefer Rust for backend services.
  - `event` score=0.574603 node=['user:locomo_user_c', 'session:locomo_c'] text=user: I liked Python for dashboards before.
  - `event` score=0.567237 node=['user:locomo_user_c', 'session:locomo_c'] text=user: On April 10 I moved to Austin.

### What language does the user currently prefer?
- session: `conversation_c_temporal_update`
- question_type: `current_state`
- context_pack_id: `2568808236014838427`
- used_context_tokens: `38`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `entity` score=0.625609 node=['user:locomo_user_c', 'session:locomo_c'] text=preference: preference = Rust for backend services
  - `event` score=0.575905 node=['user:locomo_user_c', 'session:locomo_c'] text=user: Actually I now prefer Rust for backend services.
  - `event` score=0.546612 node=['user:locomo_user_c', 'session:locomo_c'] text=user: On April 10 I moved to Austin.
  - `event` score=0.528609 node=['user:locomo_user_c', 'session:locomo_c'] text=user: I liked Python for dashboards before.
  - `event` score=0.528609 node=['user:locomo_user_c', 'session:locomo_c'] text=user: On March 2 I lived in Seattle.

### Where was the user before April 10?
- session: `conversation_c_temporal_update`
- question_type: `date`
- context_pack_id: `5496279768100078673`
- used_context_tokens: `35`
- tree traversal: selected_nodes=2 selected_paths=2 fallback=False
- selected refs:
  - `event` score=0.653139 node=['user:locomo_user_c', 'session:locomo_c'] text=user: On April 10 I moved to Austin.
  - `event` score=0.61533 node=['user:locomo_user_c', 'session:locomo_c'] text=user: I liked Python for dashboards before.
  - `event` score=0.558199 node=['user:locomo_user_c', 'session:locomo_c'] text=user: On March 2 I lived in Seattle.
  - `entity` score=0.619753 node=['user:locomo_user_c', 'session:locomo_c'] text=location: location = Austin
  - `event` score=0.554008 node=['user:locomo_user_c', 'session:locomo_c'] text=user: Actually I now prefer Rust for backend services.


## Compact Records By Model

### context_event
```json
[
  {
    "event_id_hash": 3199282477671782654,
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_event",
    "summary_text": "user: I moved to Seattle today, please remember this location.",
    "text": "user: I moved to Seattle today, please remember this location."
  },
  {
    "event_id_hash": 70957050267175957,
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_event",
    "summary_text": "user: Actually I moved to Austin now for the new infra project.",
    "text": "user: Actually I moved to Austin now for the new infra project."
  },
  {
    "event_id_hash": 1284311978739576877,
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_event",
    "summary_text": "user: I prefer Rust for low latency storage engines.",
    "text": "user: I prefer Rust for low latency storage engines."
  },
  {
    "event_id_hash": 7075087912260012156,
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_event",
    "summary_text": "user: Alice approved the GPU purchase after finance reviewed the budget.",
    "text": "user: Alice approved the GPU purchase after finance reviewed the budget."
  },
  {
    "event_id_hash": 3751499551272030011,
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_event",
    "summary_text": "user: My manager Priya is helping with the launch plan.",
    "text": "user: My manager Priya is helping with the launch plan."
  },
  {
    "event_id_hash": 4769388284908161345,
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_event",
    "summary_text": "user: My family has a dog named Mochi.",
    "text": "user: My family has a dog named Mochi."
  },
  {
    "event_id_hash": 7770314201943024968,
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_event",
    "summary_text": "user: My job role is storage infrastructure lead.",
    "text": "user: My job role is storage infrastructure lead."
  },
  {
    "event_id_hash": 4337337313558222841,
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_event",
    "summary_text": "user: I plan to visit Berlin next month for the conference.",
    "text": "user: I plan to visit Berlin next month for the conference."
  },
  {
    "event_id_hash": 2703010026126289689,
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_event",
    "summary_text": "user: On March 2 I lived in Seattle.",
    "text": "user: On March 2 I lived in Seattle."
  },
  {
    "event_id_hash": 4727707316315375027,
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_event",
    "summary_text": "user: On April 10 I moved to Austin.",
    "text": "user: On April 10 I moved to Austin."
  },
  {
    "event_id_hash": 5386086959540097598,
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_event",
    "summary_text": "user: I liked Python for dashboards before.",
    "text": "user: I liked Python for dashboards before."
  },
  {
    "event_id_hash": 1033949689524731850,
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
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
    "event_id_hash": 3199282477671782654,
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 70957050267175957,
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 1284311978739576877,
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 7075087912260012156,
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 3751499551272030011,
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 4769388284908161345,
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 7770314201943024968,
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 4337337313558222841,
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 2703010026126289689,
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 4727707316315375027,
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 5386086959540097598,
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "session_buffer_event"
  },
  {
    "event_id_hash": 1033949689524731850,
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "session_buffer_event"
  }
]
```

### context_batch_commit
```json
[
  {
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_batch_commit",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ]
  },
  {
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_batch_commit",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ]
  },
  {
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_batch_commit",
    "source_event_ids": [
      2703010026126289689,
      4727707316315375027,
      5386086959540097598,
      1033949689524731850
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
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_segment",
    "segment_hash": 6987341564811954135,
    "source_event_ids": [
      7075087912260012156
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
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_segment",
    "segment_hash": 1650695322988557265,
    "source_event_ids": [
      3199282477671782654,
      70957050267175957
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
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_segment",
    "segment_hash": 6185130051524304258,
    "source_event_ids": [
      1284311978739576877
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
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_segment",
    "segment_hash": 381015012967861979,
    "source_event_ids": [
      3751499551272030011,
      4337337313558222841
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
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_segment",
    "segment_hash": 454602837135543669,
    "source_event_ids": [
      1033949689524731850
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
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_segment",
    "segment_hash": 2165148859925844529,
    "source_event_ids": [
      4727707316315375027
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
    "entity_hash": 4368207921958537161,
    "entity_name": "preference",
    "entity_type": "preference",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ],
    "source_refs": [
      "3199282477671782654",
      "70957050267175957",
      "1284311978739576877",
      "7075087912260012156"
    ],
    "state": "Rust for low latency storage engines"
  },
  {
    "entity_hash": 1639419467397175869,
    "entity_name": "location",
    "entity_type": "location",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ],
    "source_refs": [
      "3199282477671782654",
      "70957050267175957",
      "1284311978739576877",
      "7075087912260012156"
    ],
    "state": "Austin now for the new infra project"
  },
  {
    "entity_hash": 2639207394552949247,
    "entity_name": "the GPU purchase after finance reviewed the budget",
    "entity_type": "approval_state",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ],
    "source_refs": [
      "3199282477671782654",
      "70957050267175957",
      "1284311978739576877",
      "7075087912260012156"
    ],
    "state": "the GPU purchase after finance reviewed the budget"
  },
  {
    "entity_hash": 6491685851751749674,
    "entity_name": "Priya is helping with the launch plan",
    "entity_type": "relationship",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "source_refs": [
      "3751499551272030011",
      "4769388284908161345",
      "7770314201943024968",
      "4337337313558222841"
    ],
    "state": "Priya is helping with the launch plan"
  },
  {
    "entity_hash": 3401292816335826860,
    "entity_name": "job_status",
    "entity_type": "job_status",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "source_refs": [
      "3751499551272030011",
      "4769388284908161345",
      "7770314201943024968",
      "4337337313558222841"
    ],
    "state": "role is storage infrastructure lead"
  },
  {
    "entity_hash": 6580215612133735141,
    "entity_name": "current_plan",
    "entity_type": "current_plan",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "source_refs": [
      "3751499551272030011",
      "4769388284908161345",
      "7770314201943024968",
      "4337337313558222841"
    ],
    "state": "to visit Berlin next month for the conference"
  },
  {
    "entity_hash": 615315919857288100,
    "entity_name": "family_profile",
    "entity_type": "family_profile",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "source_refs": [
      "3751499551272030011",
      "4769388284908161345",
      "7770314201943024968",
      "4337337313558222841"
    ],
    "state": "has a dog named Mochi"
  },
  {
    "entity_hash": 8048610667407445074,
    "entity_name": "preference",
    "entity_type": "preference",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      2703010026126289689,
      4727707316315375027,
      5386086959540097598,
      1033949689524731850
    ],
    "source_refs": [
      "2703010026126289689",
      "4727707316315375027",
      "5386086959540097598",
      "1033949689524731850"
    ],
    "state": "Rust for backend services"
  },
  {
    "entity_hash": 4393558289296873974,
    "entity_name": "location",
    "entity_type": "location",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "previous_state": "",
    "record_type": "context_entity",
    "source_event_ids": [
      2703010026126289689,
      4727707316315375027,
      5386086959540097598,
      1033949689524731850
    ],
    "source_refs": [
      "2703010026126289689",
      "4727707316315375027",
      "5386086959540097598",
      "1033949689524731850"
    ],
    "state": "Austin"
  }
]
```

### context_summary
```json
[
  {
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "summary_hash": 2962234381734522119,
    "summary_text": "user: I moved to Seattle today, please remember this location.",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "summary_hash": 2962234381734522119,
    "summary_text": "user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project.",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "summary_hash": 2962234381734522119,
    "summary_text": "user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. use...",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "summary_hash": 2962234381734522119,
    "summary_text": "user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. use...",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ],
    "summary_hash": 4273725117655748622,
    "summary_text": "user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.",
    "summary_type": "batch_l0"
  },
  {
    "node_hash": 1400509432320306660,
    "node_path": [
      "user:locomo_user_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ],
    "summary_text": "user:locomo_user_a :: user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: ...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 1400509432320306660,
    "node_path": [
      "user:locomo_user_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ],
    "summary_text": "Context node user:locomo_user_a. Overview: user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. use... user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget. user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path user:locomo_user_a and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ],
    "summary_text": "user:locomo_user_a / session:locomo_a :: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engin...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3199282477671782654,
      70957050267175957,
      1284311978739576877,
      7075087912260012156
    ],
    "summary_text": "Context node user:locomo_user_a / session:locomo_a. Overview: user: I moved to Seattle today, please remember this location. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: Alice approved the GPU purchase after finance reviewed the budget.. This node belongs to path user:locomo_user_a / session:locomo_a and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_summary",
    "summary_hash": 165260480146054573,
    "summary_text": "user: My manager Priya is helping with the launch plan.",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_summary",
    "summary_hash": 165260480146054573,
    "summary_text": "user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi.",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_summary",
    "summary_hash": 165260480146054573,
    "summary_text": "user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My family has a dog named Mochi. user: My manager Priya is helping with the launch plan. ...",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_summary",
    "summary_hash": 165260480146054573,
    "summary_text": "user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My family has a dog named Mochi. user: My manager Priya is helping with the launch plan. ...",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "summary_hash": 201607810208560836,
    "summary_text": "user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My job role is storage infrastructure lead. user: I plan to visit Berlin next month for the conference.",
    "summary_type": "batch_l0"
  },
  {
    "node_hash": 4856627695467181539,
    "node_path": [
      "user:locomo_user_b"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "summary_text": "user:locomo_user_b :: user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My family has a dog n...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 4856627695467181539,
    "node_path": [
      "user:locomo_user_b"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "summary_text": "Context node user:locomo_user_b. Overview: user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My family has a dog named Mochi. user: My manager Priya is helping with the launch plan. ... user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My job role is storage infrastructure lead. user: I plan to visit Berlin next month for the conference. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My job role is storage infrastructure lead. user: I plan to visit Berlin next month for the conference.. This node belongs to path user:locomo_user_b and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "summary_text": "user:locomo_user_b / session:locomo_b :: user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My job role is storage infrastructure lead. user: I plan to visit Berlin ne...",
    "summary_type": "node_l0"
  },
  {
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_summary",
    "source_event_ids": [
      3751499551272030011,
      4769388284908161345,
      7770314201943024968,
      4337337313558222841
    ],
    "summary_text": "Context node user:locomo_user_b / session:locomo_b. Overview: user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: My job role is storage infrastructure lead. user: I plan to visit Berlin next month for the conference.. This node belongs to path user:locomo_user_b / session:locomo_b and should be used for tree-first retrieval before leaf event/entity recall.",
    "summary_type": "node_l1"
  },
  {
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_summary",
    "summary_hash": 432637146151229606,
    "summary_text": "user: On March 2 I lived in Seattle.",
    "summary_type": "session_l0"
  },
  {
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_summary",
    "summary_hash": 432637146151229606,
    "summary_text": "user: On March 2 I lived in Seattle. user: On March 2 I lived in Seattle. user: On March 2 I lived in Seattle. user: On April 10 I moved to Austin.",
    "summary_type": "session_l0"
  }
]
```

### context_embedding
```json
[
  {
    "embedding_type": "session_l0",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
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
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
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
    "embedding_type": "session_l0",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.192847,
      0.0,
      0.0,
      0.0,
      0.385695,
      0.0
    ]
  },
  {
    "embedding_type": "event_text",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
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
    "embedding_type": "session_l0",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.192748,
      0.0,
      0.0,
      -0.032125,
      0.385496,
      0.0
    ]
  },
  {
    "embedding_type": "event_text",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.333333,
      0.0
    ]
  },
  {
    "embedding_type": "session_l0",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.192748,
      0.0,
      0.0,
      -0.032125,
      0.385496,
      0.0
    ]
  },
  {
    "embedding_type": "event_text",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      -0.27735,
      0.0,
      0.0
    ]
  },
  {
    "embedding_type": "entity_state",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ]
  },
  {
    "embedding_type": "entity_state",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.353553,
      0.0
    ]
  },
  {
    "embedding_type": "entity_state",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.27735,
      0.0,
      -0.27735,
      -0.27735,
      0.0
    ]
  },
  {
    "embedding_type": "segment_text",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      -0.25,
      -0.25,
      0.0
    ]
  },
  {
    "embedding_type": "segment_text",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.137361,
      0.0,
      0.0,
      0.0,
      0.274721,
      0.0
    ]
  },
  {
    "embedding_type": "segment_text",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ]
  },
  {
    "embedding_type": "batch_l0",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.093659,
      0.093659,
      0.0,
      -0.093659,
      0.561951,
      0.0
    ]
  },
  {
    "embedding_type": "node_l0",
    "node_hash": 1400509432320306660,
    "node_path": [
      "user:locomo_user_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.224231,
      0.0,
      0.0,
      0.0,
      0.373718,
      0.0
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 1400509432320306660,
    "node_path": [
      "user:locomo_user_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.136966,
      0.0,
      -0.019567,
      -0.0587,
      0.469596,
      0.0
    ]
  },
  {
    "embedding_type": "node_l0",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.099504,
      0.099504,
      0.0,
      0.0,
      0.597022,
      0.0
    ]
  },
  {
    "embedding_type": "node_l1",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.151186,
      -0.075593,
      -0.075593,
      0.52915,
      0.0
    ]
  },
  {
    "embedding_type": "session_l0",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_embedding",
    "vector_dim": 32,
    "vector_preview": [
      0.0,
      0.0,
      0.0,
      0.408248,
      0.408248,
      0.0
    ]
  }
]
```

### context_index
```json
[
  {
    "index_name": "event_type:confirmation",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "classification:batch_memory",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "status:observed",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "source_type:message",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:preference",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:location",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:approval_state",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "segment_topic:approval_budget",
    "node_hash": 1226958135442153109,
    "node_path": [
      "user:locomo_user_a",
      "session:locomo_a"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "event_type:plan_update",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "classification:batch_memory",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "status:observed",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "source_type:message",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:relationship",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:job_status",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:current_plan",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:family_profile",
    "node_hash": 8016836484261499701,
    "node_path": [
      "user:locomo_user_b",
      "session:locomo_b"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "event_type:preference_update",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "classification:batch_memory",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "status:observed",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "source_type:message",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:preference",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "entity_type:location",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "segment_topic:preference",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
      "session:locomo_c"
    ],
    "record_type": "context_index"
  },
  {
    "index_name": "segment_topic:location",
    "node_hash": 4664759118317631097,
    "node_path": [
      "user:locomo_user_c",
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
    "summary_text": "location: location = Austin now for the new infra project user: Actually I moved to Austin now for the new infra project. user: Alice approved the GPU purchase after finance reviewed the budget. user: I moved to Seattle today, please remember this location. user: I prefer Rust for low latency storage engines."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "preference: preference = Rust for low latency storage engines user: I prefer Rust for low latency storage engines. user: Actually I moved to Austin now for the new infra project. user: Alice approved the GPU purchase after finance reviewed the budget. user: I moved to Seattle today, please remember this location."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "approval_state: the GPU purchase after finance reviewed the budget = the GPU purchase after finance reviewed the budget user: Alice approved the GPU purchase after finance reviewed the budget. 3: Alice approved the GPU purchase after finance reviewed the budget. user: Actually I moved to Austin now for the new infra project. user: I prefer Rust for low latency storage engines. user: I moved to Seattle today, please remember this location."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "user: My manager Priya is helping with the launch plan. relationship: Priya is helping with the launch plan = Priya is helping with the launch plan user: I plan to visit Berlin next month for the conference. family_profile: family_profile = has a dog named Mochi user: My job role is storage infrastructure lead. user: My family has a dog named Mochi."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "relationship: Priya is helping with the launch plan = Priya is helping with the launch plan user: My manager Priya is helping with the launch plan. family_profile: family_profile = has a dog named Mochi user: My family has a dog named Mochi. user: I plan to visit Berlin next month for the conference. user: My job role is storage infrastructure lead."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "job_status: job_status = role is storage infrastructure lead user: My job role is storage infrastructure lead. user: My manager Priya is helping with the launch plan. user: My family has a dog named Mochi. user: I plan to visit Berlin next month for the conference."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "location: location = Austin user: On March 2 I lived in Seattle. user: Actually I now prefer Rust for backend services. user: I liked Python for dashboards before. user: On April 10 I moved to Austin."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "preference: preference = Rust for backend services user: Actually I now prefer Rust for backend services. user: On April 10 I moved to Austin. user: I liked Python for dashboards before. user: On March 2 I lived in Seattle."
  },
  {
    "record_type": "context_pack_audit",
    "summary_text": "user: On April 10 I moved to Austin. user: I liked Python for dashboards before. user: On March 2 I lived in Seattle. location: location = Austin user: Actually I now prefer Rust for backend services."
  }
]
```

