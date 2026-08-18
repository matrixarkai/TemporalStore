# mem0-style Memory API — Examples (HTTP & Python)

MatrixArk exposes a **mem0-compatible** memory API for **ingestion** (writing memory from
conversation turns) and **retrieval** (semantic recall). Existing mem0 code works unchanged —
same method names and result shapes — over two transports:

- **HTTP API** (`/v1/...`) — any language, no install. In dev mode the API key is optional.
- **Python SDK** — a drop-in `Memory()` (today `from matrixark_mem0_compat import Memory`; a
  published `matrixark` pip package is coming) that calls the HTTP API under the hood.

Runnable examples live next to this doc:
| file | what it shows | needs a server? |
|---|---|---|
| [`examples/mem0/local_inprocess_example.py`](../examples/mem0/local_inprocess_example.py) | full lifecycle in-process | **no** — `python3 local_inprocess_example.py` |
| [`examples/mem0/python_sdk_example.py`](../examples/mem0/python_sdk_example.py) | the Python SDK (over HTTP) | yes (a gateway) |
| [`examples/mem0/http_curl_example.sh`](../examples/mem0/http_curl_example.sh) | raw HTTP via curl | yes (a gateway) |

---

## The scope model (who the memory belongs to)

Every call carries a `scope`. The **minimum to write** a memory is `scope.user_id` + `messages`.

| field | where | required | meaning (mem0 equivalent) |
|---|---|---|---|
| `user_id` | scope | **yes** | the subject — whose memory (`user_id`) |
| `session_id` | scope | no | conversation/thread (`run_id`) |
| `agent_name` | scope | no | which agent (`agent_id`) |
| `tenant_id` / `account_id` | scope | no* | org isolation (*derived from the API key if omitted) |
| `messages` | body | **yes** (ingest) | `[{role, content}]` — role ∈ user/assistant/system |
| `finalize` | body | no | `true` = extract now; omit = buffer for batch extraction |

---

## Ingestion

**`add` — write memory from a turn.** Returns the anchor id (your handle for get/update/delete).

**HTTP**
```bash
curl -X POST $BASE/v1/ingest -H "Content-Type: application/json" -d '{
  "scope": {"user_id": "alice", "session_id": "s1"},
  "messages": [{"role": "user", "content": "I am vegetarian and allergic to peanuts"}],
  "finalize": true
}'
# -> {"event_id_hash": 2837806361758721992, "status": "accepted", ...}
```

**Python**
```python
res = m.add([{"role": "user", "content": "I am vegetarian and allergic to peanuts"}],
            user_id="alice", run_id="s1", agent_id="support-bot")
mid = res["results"][0]["id"]     # == event_id_hash
```

**MatrixArk extensions (optional, beyond mem0):**
- **Per-record TTL** — `ttl_seconds` / `expires_at`: the memory auto-expires and is excluded from summaries.
- **Keyed-upsert** — `identity_key` + `truth_class`: supersede the prior value of a fact only if the new
  observation's truth rank ≥ the old one (prevents a low-confidence value clobbering a high-confidence one).

```python
m.add([{"role":"user","content":"Use gate B12 today"}], user_id="alice", ttl=86400)
m.add([{"role":"user","content":"Preferred seat: aisle"}], user_id="alice",
      identity_key="pref.seat", truth_class="asserted")
```

---

## Retrieval

**`search` — semantic recall.** Returns mem0's shape `{"results": [{"id","memory","score","metadata"}]}`.
(The native `/v1/retrieve` returns the richer token-budgeted **context pack**; the shim flattens it. Pass
`raw=True` to `m.search(...)` to get the full pack.)

**HTTP**
```bash
curl -X POST $BASE/v1/retrieve -d '{"scope":{"user_id":"alice"},"query":"dietary restrictions","limit":8}'
curl $BASE/v1/memory/<id>                 # get one memory by id
curl "$BASE/v1/memories?user_id=alice"    # get_all — list a user's live memories
```

**Python**
```python
hits = m.search("dietary restrictions", user_id="alice", limit=10)
m.get(mid)                        # one memory
m.get_all(user_id="alice")        # all live memories for the subject
```

---

## Update, history, delete, forget, reset

| op | HTTP | Python | notes |
|---|---|---|---|
| update | `POST /v1/update` | `m.update(id, data)` | supersedes + **re-extracts** derived memory; old is closure-tombstoned |
| history | `GET /v1/memory/<id>/history` | `m.history(id)` | ordered `ingested → superseded → deleted` change log (a temporal capability) |
| delete | `POST /v1/delete` | `m.delete(id)` | removes one memory **and its derived entities/embeddings** (no orphans) |
| **forget** | `POST /v1/forget` | `m.delete_all(user_id)` | erase **one subject**; `confirm` must equal `user_id` |
| **reset** | `POST /v1/reset` | `m.reset()` | wipe the **whole tenant**; `confirm` must equal `tenant_id` or `"RESET"` |

```python
m.update(mid, "I am vegan now, still allergic to peanuts")
m.history(mid)                    # ["ingested", "superseded"]
m.delete(mid)
m.delete_all(user_id="alice")     # forget alice
m.reset()                         # wipe the tenant
```

**forget vs reset** — `forget`/`delete_all(user_id)` erases exactly one user (scope-isolated; other users
untouched). `reset()` erases every user under the tenant. Both require the matching `confirm` so you cannot
accidentally wipe a tenant when you meant one user.

---

## Verified end-to-end

`local_inprocess_example.py` runs the whole flow with no server:
```
add() -> id 2837806361758721992
search('dietary restrictions') finds 'peanut': true
get(id): "user: I'm vegetarian and allergic to peanuts."
get_all() count: 1
update() -> superseded=true, new_id=...
history(old id): ["ingested", "superseded"]
delete(new id): true ; get_all after delete: 0
forget(user): true
```

## Response & error shapes
`200` success · `401` missing/invalid/expired key · `403` valid key but not authorized ·
`404` id not found · `429` per-key quota exceeded. Error body: `{"error": {"code","message","type"}}`.
