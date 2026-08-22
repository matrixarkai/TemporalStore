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

### `add()` returns the mem0 `results` envelope (literal drop-in)

`add()` returns the mem0-shaped result **and** keeps MatrixArk's fields, so both accessors work:

```python
r = m.add("I prefer dark mode", user_id="alice")
r["results"][0]["id"]       # "<event_id_hash>"   (strict mem0 callers)
r["results"][0]["event"]    # "ADD" | "UPDATE" | "NONE"
r["results"][0]["memory"]   # the ingested text
r["event_id_hash"]          # still present (backward compatible)
```

The `event` maps the keyed-upsert outcome (see below): `ADD` for a new memory, `UPDATE` when a
keyed write superseded a prior value, `NONE` when a keyed write was rejected by the rank guard
(`id` is then the surviving record).

---

## PurchaseMemory: TTL + keyed-upsert (the PurchaseMemory migration story)

Two mem0-adjacent primitives that map onto `add()` kwargs (all optional and additive):

```python
# Per-record TTL — an ephemeral memory that auto-expires and is NEVER summarized:
m.add("one-time passcode 4821", user_id="alice", ttl=300)          # expires in 300s
m.add("promo valid this week", user_id="alice", expires_at=1_786_000_000)  # absolute unix seconds

# Keyed-upsert with a truth-rank guard — a single current value per logical fact:
m.add("email is a@x.com", user_id="alice", identity_key="user.email", truth_class="reported")
m.add("email is b@x.com", user_id="alice", identity_key="user.email", truth_class="asserted")
#   → supersedes (asserted 3 >= reported 2); recall now returns b@x.com. event == "UPDATE"
m.add("email might be c@x.com", user_id="alice", identity_key="user.email", truth_class="inferred")
#   → rank-guarded (inferred 1 < asserted 3): NO write; b@x.com survives. event == "NONE"
```

| Behavior | How |
|---|---|
| **TTL / expiry** | `ttl` (relative seconds) or `expires_at` (absolute unix seconds). The whole ingest closure is stamped ephemeral: it stops surfacing from `search` / `get_all` once expired, is **excluded from summaries/rollups**, and is lazily tombstoned + purged (durable, crash-safe, survives reload). |
| **retention cutoff** | Send `retention_cutoff_ts` on the raw `/v1/ingest` body to hide/reclaim records in the subject scope older than a cutoff. |
| **keyed-upsert** | `identity_key` + `truth_class`. A new write with rank `>=` the existing keyed value supersedes it (old closure-tombstoned, `superseded_by` set); a lower rank is rejected (no-op). |
| **recall by key** | `GET /v1/memory/by-key?identity_key=…` returns the single current live keyed value. |

Truth-rank table (override via `MATRIXARK_TRUTH_RANK` env JSON): `asserted=3, reported=2,
inferred=1, unknown=0`.

> NOTE: expiry is enforced at the application layer (read-time filter + lazy tombstone-purge),
> correct for every backend. Native engine-level key-expiry (`ttl_ms`) wiring on the Rust temporal
> backend is a documented TODO — records already carry `expires_at_ms`, which the temporal read
> path honors via its retention filter.

---

## Memory management: get / update / history / delete / forget / get_all / reset

The shim covers mem0's full memory-management surface. The **subject** is
`scope.user_id`, so these map onto the same identity kwargs as `add`/`search`.

```python
m.get_all(user_id="alice")                  # list alice's active memories
m.get("<memory_id>")                         # fetch a single memory by id
m.update("<memory_id>", "new content")      # update a memory (supersede)
m.history("<memory_id>")                     # change history for a memory id
m.delete("<memory_id>")                      # delete a single memory by id
m.delete_all(user_id="alice")               # delete ALL of alice's memory
m.reset()                                    # wipe the caller's whole tenant
```

| mem0 method                     | MatrixArk endpoint / shim        | Notes                                                                       |
| ------------------------------- | -------------------------------- | --------------------------------------------------------------------------- |
| `get(memory_id)`                | `GET /v1/memory/<id>`            | Returns `{found, memory, text, metadata, derived}` for the id; a deleted/forgotten memory returns `{found: false}` (HTTP 404). |
| `update(memory_id, data)`       | `POST /v1/update`                | **Supersede**: ingests the new `data` in the memory's own scope and tombstones the old id, so `search`/`get_all` return the new version and the old never resurfaces. |
| `history(memory_id)`            | `GET /v1/memory/<id>/history`    | Ordered event-sourced history for the id: `ingested` → `superseded`/`deleted`, with timestamps; the new version of an update carries a `created` link back to the id it supersedes. |
| `delete(memory_id)`             | `POST /v1/delete`                | `memory_id` is the id `add`/`get_all` returns (MatrixArk's `event_id_hash`). Provenance-closure aware (see below). |
| `delete_all(user_id=…)`         | `POST /v1/forget`                | subject = `scope.user_id`; server requires `confirm == user_id` (exact match, no wildcard). The shim sets `confirm` for you. Returns the count removed. |
| `get_all(user_id=…, limit=…)`   | `GET`/`POST /v1/memories`        | Lists a subject's active memories; forgotten/deleted memories are excluded. |
| `reset()`                       | `POST /v1/reset`                 | Wipes the caller's tenant. Guarded by an explicit `confirm` — the shim sends the literal `"RESET"`; the server also accepts the resolved `tenant_id`. |
| `batch_update(memories)`        | one `POST /v1/update` per entry  | Takes mem0's shape — `[{memory_id, text?, metadata?}]`, `text` mapped onto `data` — capped at 1000 entries. **Not atomic**: an update here is a supersede with no cross-memory transaction behind it, so entries are applied in order and a failure is reported per memory (`{"results": [...], "updated": n, "failed": [...]}`) instead of aborting the rest. |
| `batch_delete(memories)`        | one `POST /v1/delete` per entry  | `[{memory_id}]`, or a bare list of ids. Capped at 1000, same non-atomic reporting as `batch_update`. |
| `users(limit=…)`                | `POST /v1/users`                 | The users / agents / runs that **hold** memories, in mem0's `{"results": [{"type", "name"}]}` shape, with a `memory_count` on users. A subject whose memories were all forgotten or expired is not listed — this answers "who has memories", not "who was provisioned" (the admin user list is that other thing). Gated on `context:retrieve`, and scoped to the caller's tenant. |

`add()` **finalizes by default**, which is what makes it read-after-write: a `get_all` / `search`
issued straight after `add` sees the memory, as on mem0. Pass `finalize=False` to stream a
conversation in and let the idle commit close it — cheaper per call, but the memory only becomes
visible once a debounce elapses, and every further write to the same scope pushes that debounce
out, so a burst of un-finalized `add` calls can stay invisible for as long as the burst lasts.

Scope gates (enforced-mode keys): `forget`/`delete`/`reset` require
`context:forget`; `update` requires `context:ingest`; `get_all`/`get`/`history`
require `context:retrieve`. Tenant identity is always pinned from the authenticated
key, so one tenant can never read or delete another's memory. `update` additionally
refuses to cross tenants (the memory must belong to the caller's tenant).

A "deleted" or "forgotten" memory is excluded from **retrieve** as well as
`get_all` — it does not come back. Removal is a durable soft-delete (a tombstone in
the same event log) and survives a reload; `forget` also writes a payload-free,
hashed-subject audit record (`sha256(user_id)` + count).

**Direct helpers** are available in `matrixark_ingest_client` without the shim:
`get_memory(memory_id)`, `update_memory(memory_id, data)`,
`memory_history(memory_id)`, `forget(user_id=…)`, `delete_memory(memory_id)`,
`get_all(user_id=…)`, `reset_memory()`.

### `search()` return shape (mem0 parity)

`m.search(...)` returns mem0's shape so existing mem0 code that reads
`res["results"][i]["memory"]` works unchanged:

```json
{ "results": [ { "id": "...", "memory": "<text>", "score": 0.91, "metadata": { ... } }, ... ] }
```

Each result is mapped from a MatrixArk ContextPack `selected_ref` (`text` →
`memory`, a stable `id`, the ref `score` when present, and remaining ref fields as
`metadata`). Pass `raw=True` (or call `m.search_raw(...)`) to get the full
ContextPack response instead.

### Delete optimizations (done)

Deletion guarantees **logical exclusion** (a removed memory never resurfaces) AND
now reclaims space and extends the delete blast-radius:

* **Provenance-closure delete** — deleting a *source event* cascades: derived
  records built **solely** from it (single-source segments/entities/summaries) are
  removed by the same tombstone. **Multi-source** derivatives are kept but demoted —
  the deleted source is trimmed from their `source_event_ids` / `source_refs` /
  `source_event_hash` evidence (a derivative whose evidence becomes empty is
  removed). Deleting a leaf record still just tombstones that record (backward
  compatible).
* **Physical purge** — a crash-safe compaction pass rewrites the JSONL event log
  **without** the tombstoned records and their tombstone markers, reclaiming space;
  the purged log replays to the same logical state. It runs on `reset`, on `forget`
  (threshold-gated), and whenever the tombstone count crosses
  `MATRIXARK_MEMORY_PURGE_THRESHOLD` (env-gated; `0` = off). Durability is a
  temp-write + `fsync` + atomic `os.replace` onto the primary shard.

**Still deferred** (separate parallel workstream): rust-datanode-native delete
(the engine exposes `StringDelete`/`CommonDelete`/`FeatureDelete`; wiring tombstones
down to a native delete is owned elsewhere — the local/reference adapter is
authoritative here), and true **re-derivation** of a demoted multi-source
entity/summary — the closure delete trims the deleted source from a derivative's
evidence but does not re-run extraction/summarization to recompute its state.

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

`add()` posts (PurchaseMemory kwargs — `expires_at` / `ttl` → `ttl_seconds` / `identity_key` /
`truth_class` — are added to the body only when supplied):

```json
{ "kind": "message",
  "messages": [ ... ],
  "scope": { "user_id": "...", "agent_id": "...", "session_id": "<run_id>" },
  "metadata": { ... },
  "expires_at": 1786000000, "identity_key": "user.email", "truth_class": "asserted" }
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
| `add()` / `search()` API       | ✓                                  | ✓ via `matrixark_mem0_compat.Memory` (mem0 `results[]` shape)   |
| `get` / `update` / `history`   | ✓                                  | ✓ (`update` = supersede; `history` = event-sourced timeline)    |
| `delete` / `delete_all` / `reset` | ✓                               | ✓ (closure-aware delete + crash-safe physical purge)            |
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
