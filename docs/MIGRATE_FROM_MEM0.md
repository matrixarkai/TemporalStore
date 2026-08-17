# Migrating from mem0 to MatrixArk

MatrixArk accepts mem0's memory-identity conventions so an existing
[`mem0`](https://github.com/mem0ai/mem0) codebase can point at a MatrixArk /
TemporalStore deployment with a **one-line import swap** and no call-site
rewrites.

There are two pieces:

1. A **drop-in `Memory` shim** (`tools/matrixark_mem0_compat.py`) with mem0's
   `add()` / `search()` signatures.
2. The ingest **envelope itself accepts mem0's top-level identity kwargs**
   (`user_id` / `agent_id` / `run_id`) directly on `POST /v1/ingest`, so even
   hand-rolled HTTP callers migrate cleanly.

---

## The drop-in shim

```python
# before:
# from mem0 import Memory
# after:
from matrixark_mem0_compat import Memory

m = Memory()   # base_url / api_key resolved from the environment (see below)

# add — messages may be a list of {role, content} OR a bare string
m.add(
    [{"role": "user", "content": "I prefer dark mode"}],
    user_id="alice",
    agent_id="support-bot",
    run_id="session-2026-08-16",
    metadata={"topic": "preferences"},
)

# search
hits = m.search(
    "what are my UI preferences?",
    user_id="alice",
    agent_id="support-bot",
    limit=8,
)
```

`Memory(base_url=None, api_key=None)` resolves configuration exactly like the
ingest client:

| Setting    | Precedence                                                                        |
| ---------- | --------------------------------------------------------------------------------- |
| `base_url` | explicit arg › `$MATRIXARK_BASE_URL` › `$MATRIXARK_GATEWAY_URL` › `http://127.0.0.1:8080` |
| `api_key`  | explicit arg › `$MATRIXARK_API_KEY` › anonymous (no `Authorization` header sent)  |

The shim is stdlib-only (`http.client` + `json`) and reuses
`matrixark_ingest_client`'s connection helpers.

---

## Memory management: delete / forget / get_all / reset

The shim also covers mem0's memory-management surface. The **subject** is
`scope.user_id`, so these map onto the same identity kwargs as `add`/`search`.

```python
m.get_all(user_id="alice")                 # list alice's active memories
m.delete("<memory_id>")                     # delete a single memory by id
m.delete_all(user_id="alice")               # delete ALL of alice's memory
m.reset()                                   # wipe the caller's whole tenant
```

| mem0 method                     | MatrixArk endpoint / shim        | Notes                                                                       |
| ------------------------------- | -------------------------------- | --------------------------------------------------------------------------- |
| `delete(memory_id)`             | `POST /v1/delete`                | `memory_id` is the id `add`/`get_all` returns (MatrixArk's `event_id_hash`). |
| `delete_all(user_id=…)`         | `POST /v1/forget`                | subject = `scope.user_id`; server requires `confirm == user_id` (exact match, no wildcard). The shim sets `confirm` for you. Returns the count removed. |
| `get_all(user_id=…, limit=…)`   | `GET`/`POST /v1/memories`        | Lists a subject's active memories; forgotten/deleted memories are excluded. |
| `reset()`                       | `POST /v1/reset`                 | Wipes the caller's tenant. Guarded by an explicit `confirm` — the shim sends the literal `"RESET"`; the server also accepts the resolved `tenant_id`. |

Scope gates (enforced-mode keys): `forget`/`delete`/`reset` require
`context:forget`; `get_all` requires `context:retrieve`. Tenant identity is always
pinned from the authenticated key, so one tenant can never delete another's memory.

A "deleted" or "forgotten" memory is excluded from **retrieve** as well as
`get_all` — it does not come back. Removal is a durable soft-delete (a tombstone in
the same event log) and survives a reload; `forget` also writes a payload-free,
hashed-subject audit record (`sha256(user_id)` + count).

**Direct helpers** are available in `matrixark_ingest_client` without the shim:
`forget(user_id=…)`, `delete_memory(memory_id)`, `get_all(user_id=…)`,
`reset_memory()`.

### Deferred optimizations

For this pass, deletion guarantees **logical exclusion** (a removed memory never
resurfaces). The following are follow-ons that only reclaim space or extend the
delete blast-radius — they do not change what a caller can observe:

* **Provenance-closure delete** — `delete(memory_id)` removes the addressed record
  (and its own event embedding) but does not yet re-derive multi-source
  summaries/entities that cited it.
* **Rust-datanode-native delete** — the engine already exposes
  `StringDelete`/`CommonDelete`/`FeatureDelete`; wiring the tombstones down to a
  native delete is not done here (the local/reference adapter is authoritative).
* **Physical vector/index purge** — tombstoned rows are filtered at read time, not
  yet compacted out of the underlying vector/index storage.

---

## kwarg → scope mapping

mem0 passes identity as **top-level kwargs**; MatrixArk stores it under a
canonical `scope` object. The mapping is:

| mem0 kwarg  | MatrixArk scope field | Notes                                          |
| ----------- | --------------------- | ---------------------------------------------- |
| `user_id`   | `scope.user_id`       | end-user identity                              |
| `agent_id`  | `scope.agent_id`      | logical agent/assistant identity (see below)   |
| `run_id`    | `scope.session_id`    | a mem0 "run" is a MatrixArk session            |
| `metadata`  | `metadata`            | passed through unchanged                        |

Empty identity fields are omitted from the request body.

`add()` posts:

```json
{ "kind": "message",
  "messages": [ ... ],
  "scope": { "user_id": "...", "agent_id": "...", "session_id": "<run_id>" },
  "metadata": { ... } }
```

`search()` posts to `POST /v1/retrieve`:

```json
{ "query": "...",
  "scope": { "user_id": "...", "agent_id": "...", "session_id": "<run_id>" },
  "ranking": { "max_selected_refs": <limit> } }
```

mem0's `limit` maps to `ranking.max_selected_refs` — the number of context refs
MatrixArk selects into the response.

---

## Top-level aliases accepted directly on `/v1/ingest`

You don't need the shim to migrate raw HTTP calls. `normalize_envelope` folds
mem0-style **top-level** identity kwargs into the canonical scope before anything
reads it, so this works too:

```json
{ "kind": "message",
  "messages": [ ... ],
  "user_id": "alice",
  "agent_id": "support-bot",
  "run_id": "session-42" }
```

is treated identically to:

```json
{ "kind": "message",
  "messages": [ ... ],
  "scope": { "user_id": "alice", "agent_id": "support-bot", "session_id": "session-42" } }
```

**Precedence:** a canonical `scope.*` field always **wins** over its alias. If you
provide both `scope.session_id` and a top-level `run_id`, `scope.session_id`
wins. A nested `scope.run_id` is also accepted and is preferred over a top-level
`run_id`. Callers that already pass a full canonical `scope` and no aliases are
completely unaffected (byte-identical scope).

---

## `agent_id` and per-agent isolation

`agent_id` is a **first-class optional identity dimension** in the scope,
alongside `user_id` and `session_id`. When supplied it participates in the
scope key (so memories are isolable per agent) and in retrieval matching:

- A `search(...)` that carries `agent_id` returns **only** that agent's memories.
- A broader `search(...)` with `user_id` but **no** `agent_id` still spans all of
  that user's agents (agent filtering is only applied when the query specifies an
  agent — mirroring how `user_id` / `session_id` behave).

`agent_id` is optional and blankable: omit it and behavior is exactly as before.

---

## Ingesting from OpenAI / Anthropic output

Already holding the object your model SDK produced or consumed? Normalize it into
MatrixArk `messages` with `tools/matrixark_model_adapters.py` (stdlib only — it
does **not** import the `openai` / `anthropic` SDKs; pass plain dicts or the SDK
response object):

```python
from matrixark_model_adapters import from_openai, from_anthropic
from matrixark_ingest_client import ingest_messages

ingest_messages(from_openai(openai_resp), scope={"user_id": "alice"})   # OpenAI chat request OR response
ingest_messages(from_anthropic(claude_resp))                            # Anthropic request OR Messages response
```

`from_openai` accepts a chat-completions request (`{"messages":[...]}`), a
response (`{"choices":[{"message":{...}}]}`), a single message, or a raw list;
`function` role → `tool`. `from_anthropic` accepts a messages request or a
Messages response, flattening `content` blocks and mapping a `tool_result`
user-turn → `tool`. Both flatten list-shaped content to a string, skip empty
turns, and return `[]` for empty/None. `to_messages(obj, provider="auto")` sniffs
the shape for you. The `Memory` shim also takes provider-shaped input directly:

```python
m.add(openai_resp)                    # auto-sniffed
m.add(claude_resp, provider="anthropic")
```

## MatrixArk vs mem0 at a glance

| Capability                     | mem0                               | MatrixArk                                                        |
| ------------------------------ | ---------------------------------- | --------------------------------------------------------------- |
| Identity kwargs                | `user_id` / `agent_id` / `run_id`  | same kwargs accepted (folded into `scope`)                      |
| Message format                 | OpenAI `{role, content}`           | same (also accepts a bare string)                              |
| `add()` / `search()` API       | ✓                                  | ✓ via `matrixark_mem0_compat.Memory`                            |
| Backing store                  | vector DB + graph (per config)     | TemporalStore (time-aware context tiers, native retrieval)      |
| Session / run scoping          | `run_id`                           | `session_id` (mapped from `run_id`)                             |
| Per-agent isolation            | `agent_id`                         | `agent_id` in the scope key + retrieval filter                 |
| Sharing scopes                 | —                                  | `private_user` / `tenant_shared` / `global_shared`              |
| Large-file / resource ingest   | —                                  | blob-tier upload + pointer ingest (see `ingest_large_files.md`) |

---

## What did NOT change

This support is purely additive. Existing MatrixArk callers that pass a full
`scope` object see identical behavior; the scope key is byte-identical when no
`agent_id` is supplied. Nothing about the existing ingest/retrieve contract is
removed or altered.
