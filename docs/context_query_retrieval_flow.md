# Context Query Retrieval Flow

How a query becomes an injected context pack in the Rust-native context workflow:
encoding the query, recalling candidate nodes, reranking, filtering, and assembling
the final blocks under a token budget.

All code references are to `crates/temporalstore-rust/src/`.

- Entry points: `retrieve_context` and `inject_context` (in `context_workflow.rs`)
- Query understanding + scoring primitives: `context_workflow/query.rs`
- Storage-side command handlers: `engine/execute_on_shard.rs`, `engine/context.rs`

---

## Tier model (OpenViking / VikingMem)

Three tiers, matching the OpenViking/VikingMem hierarchy. L0 and L1 are **derived
summaries** of the source; L2 is the **raw event** itself.

| Tier | Source | Contents | Role |
|------|--------|----------|------|
| **L0** | `summarize_l0(title, body)` → `node.l0` | `"{title}: {lead sentence}"`, ≤ 18 words | Short **routing/preview** summary (required traversal summary) |
| **L1** | `summarize_l1(kind, title, body)` → `node.l1_ref` | L0's lead **plus** up to 7 more sentences ranked by information density | **Richer** summary that carries more content for broader traversal |
| **L2** | `ContextEvent.text` (the raw record) | The full raw event text | **Raw event evidence** |

`ContextTier { L0, L1, L2 }` is defined in `context_workflow.rs`; `default_tiers()`
returns `[L0, L1, L2]`.

### L0 vs L1: distinct, and L1 is a strict superset

L0 is only the leading sentence (a short preview). L1 begins with that **same** lead
sentence (so it always contains what L0 shows) and then appends the most
information-dense remaining sentences, ranked by `context_fact_score`:

- figures / digits: `+3`
- proper-noun-ish tokens (capitalized, not sentence-initial): `+1` each
- temporal / correction markers (`now`, `changed`, `after`, `before`, `deadline`, …): `+2`

Because L1 = `lead + more`, **L0 and L1 never merely reformat the same facts** — L1
always carries additional detail on top of L0.

Example — body *"Alice moved to the Seattle office. Her new manager is Priya. The
transfer completed on 2026-03-01."*:

- **L0**: `Team update: Alice moved to the Seattle office`
- **L1**: `kind=Chat; title=Team update; key_facts=Alice moved to the Seattle office | The transfer completed on 2026-03-01 | Her new manager is Priya`

### No raw-event duplication

The raw text is stored exactly once, as `ContextEvent.text` (in the `context_events`
store). At extraction (`extract_context`):

- `node.l0` / `node.l1_ref` are **derived summaries**, not copies of the raw text.
- `l2_ref` is a **URI pointer** (`tsctx://tenant/{t}/model/{m}/source/{s}`), not a copy.
- Retrieval's L2 block reads that one `ContextEvent` back and returns it — there is no
  second stored copy.

VFS layout: `tsctx://tenant/{tenant}/node/{node}/event/{event_time_ms}` is the L2 raw
event evidence (see `docs/CONTEXT_VFS_LAYOUT.md`).

---

## Request shape

`ContextRetrieveRequest` (in `context_workflow.rs`):

| Field | Default | Meaning |
|-------|---------|---------|
| `query` | `""` | Natural-language query |
| `node_hashes` | — (**required**) | Candidate namespace; retrieval errors if empty |
| `tiers` | `[L0, L1, L2]` | Which tiers to emit |
| `max_events` | `16` | Per-node event scan limit (clamped to `[1, 1000]`) |
| `max_summary_nodes` | `32` | Nodes kept after summary rerank |
| `max_event_nodes` | `16` | Nodes expanded into L2 events |
| `min_confidence` / `min_importance` | `0.0` | Storage-side event filters |
| `start_time_ms` / `end_time_ms` | — | Event timeline window |
| `prefer_current_agent` + `current_agent_scope_key` | `false` / `agent:codex` | Agent-scope boost |
| `provider` | mock | Embedding/chat provider config |

---

## Layer-by-layer flow

```
query ─┬─▶ [enc] lexical plan (terms → groups → synonyms/stems, intent flags, question type)
       ├─▶ [enc] secondary-index predicate groups   (query understanding / debug / fanout)
       └─▶ [enc] dense query embedding (provider, L2-normalized)
                          │
node_hashes ─▶ [recall] batched summary-embedding fetch → cosine → per-node recall score
                          │  (prune namespace → rerank_node_limit)
                          ▼
             [rerank] ContextGetNodes → embed + lexical×1000 + freshness + agentBoost
                          │  split → summary nodes / event-expansion nodes / skipped
                          ▼
             [expand] per node: L0 preview block + L1 richer block
                      + ContextQueryEvents (range scan + attribute filter)
                      + lexical prefilter → L2 raw-event blocks
                          ▼
             [order]  final sort (relevance, tier, recency) + source_ref dedupe
                          ▼
             [inject] token-budget greedy pack → <context> + durable audit
```

### 1 — Query encoding & understanding (`query.rs`)

Three encodings are derived from the raw query.

**1a. Lexical plan** — `context_query_plan()`:
- `context_query_terms()`: split on non-alphanumerics, lowercase, drop stopwords, keep len ≥ 3.
- `context_query_term_groups_from_terms()`: each term → a group `{term, stem, synonyms}`.
  Stemming is suffix rules (`ies→y`, `es`, `s`); synonyms are a curated map
  (`checkout→payment/purchase/order`, `office→location/place/job`, `moved→switched/relocated`, …).
  A document matches a group if it hits **any** member.
- Adjacent bigram phrases, a `topic N` extractor, and **intent flags**: `requests_latest`,
  `requests_temporal_reasoning`, `requests_after`/`_before`, `requests_correction`,
  `requests_reminder`, `requests_contrastive_update`, `requests_social_link`,
  `requests_schedule_detail`, `requests_quantity_detail`, `requests_alias_detail`.

**1b. Question type** — `context_query_question_type()`: `match_all` / `current_state` /
`temporal_reasoning` / `quantity` / `relationship` / `identity_or_alias` /
`causal_reasoning` / `semantic_recall`.

**1c. Secondary-index filter groups** — `context_query_secondary_index_filter_groups()`
maps intent flags to typed predicate groups (`event_type:correction`, `status:current`,
`entity_type:location`, …) or falls back to `query_term:*` groups. Used for query
understanding, debug, and fan-out planning.

**1d. Dense embedding** — `context_query_embedding()` (provider embed path, with fallback).
Mock mode = deterministic 16-dim, L2-normalized vector. A hard failure aborts retrieval.

### 2 — Node recall via summary embeddings

For each candidate `node_hash`, build the `node_l0` and `node_l1` embedding refs and issue
**one batched** `ContextQueryEmbeddings` fetch. Each stored vector is scored against the
query vector with cosine (`context_embedding_similarity_micros`), keeping the **max** of
L0/L1 per node. Result: a `(node_hash, embed_score, refs_found, …)` list sorted descending.
This coarse semantic recall prunes the namespace to `rerank_node_limit` before any
expensive fetch.

### 3 — Node rerank

Top nodes are fetched in bulk via `ContextGetNodes`, then re-scored:

| Signal | Source | Weight |
|--------|--------|--------|
| Summary-embedding cosine | Layer 2 | base score |
| Lexical relevance on `l0 + l1_ref` | `context_relevance_score_plan()` | `× 1000` |
| Freshness | `node.last_event_time_ms` | tiebreak |
| Agent-scope match | `context_record_scope_matches()` when `prefer_current_agent` | `+125_000` |

Re-sorted, then split into **summary nodes** (top `max_summary_nodes`; contribute L0/L1
blocks), **event-expansion nodes** (top `max_event_nodes`; get L2 events read), and
**skipped nodes** (recorded in `fanout_plan`).

`context_relevance_score_plan()` is intent-aware: topic-phrase `+1000`, per-group matches
add matched-term length, all-groups-match `+100`, adjacent phrase `+50`, intent bonuses
(latest `+75`, correction `+90`, contrastive `+85`, schedule `+80`, quantity `+80`, …) and
**directional penalties** (an `after` query subtracts 25 for `before`/`earlier`/`old`, and
vice-versa). That asymmetry makes "current state" / "after X" queries prefer newer facts.

### 4 — Node summaries + L2 event expansion

For each event-expansion node:
1. Emit an **L0** preview block (`node.l0`) and an **L1** richer block (`node.l1_ref`) when
   those tiers are requested.
2. `ContextQueryEvents`: a **timeline range scan** of the per-node event series
   (`start_time_ms..end_time_ms`, `take(limit)`) with a storage-side attribute filter
   `context_event_matches_filter()` (kinds, statuses, `min_confidence`, `min_importance`,
   `current_valid_only` / `as_of_ms` bitemporal validity).
3. **Lexical prefilter** each event with `context_query_matches_plan()` (topic-phrase OR any
   term-group match). Misses are dropped (`candidates_dropped_before_scoring`); passers are
   emitted as **L2** raw-event blocks.

### 5 — Final block rerank + dedupe

```
sort key = (Reverse(relevance_score_plan(text)),  // lexical relevance first
            tier_rank(tier),                        // then L0 (0) < L1 (1) < L2 (2)
            Reverse(event_time_ms),                 // then most recent
            uri)                                     // stable tiebreak
```

Then `dedupe_context_blocks_by_source_ref()` collapses blocks sharing a `source_ref`,
keeping the richer tier / longer text.

### 6 — Injection (token-budget packing)

`inject_context()` calls `retrieve_context`, then greedy-packs in ranked order:
`remaining = max_prompt_tokens − prompt_tokens`; take blocks while they fit, overflow →
`blocked_blocks`. Writes a **durable** `ContextPackAudit` via `execute_durable`, then appends
a `<context>` section tagged per block `[tier] uri source_ref`.

---

## Worked example — `"checkout"` over a "Checkout incident" node

Ingested: `tenant=42`, kind `Incident`, title `"Checkout incident"`, body
`"Customer checkout failed. Payment risk score spiked."`, `t=1000`.

Stored (once each):
- `node.l0` = `"Checkout incident: Customer checkout failed"`
- `node.l1_ref` = `"kind=Incident; title=Checkout incident; key_facts=Customer checkout failed | Payment risk score spiked"`
- `ContextEvent.text` (raw) = `"Customer checkout failed. Payment risk score spiked."`
- embeddings `node_l0`, `node_l1`, `event_text`

Query `"checkout"`:

1. **encode** — `terms=["checkout"]`; group `["checkout","order","payment","purchase"]`;
   `question_type="semantic_recall"`; fallback filter group `["query_term:checkout"]`; 16-dim vector.
2. **recall** — batched fetch of `node_l0` + `node_l1` → cosine → node score.
3. **rerank** — `ContextGetNodes`; lexical score of `l0 + l1_ref` added ×1000; node selected.
4. **expand** — emit L0 preview + L1 richer blocks; scan events; `"...checkout..."` passes
   the prefilter → emitted as an **L2** raw-event block.
5. **order** — L0 < L1 < L2 at equal relevance (`tier_rank`); dedupe by `source_ref`.
6. **inject** — pack under `max_prompt_tokens`, write audit, emit `<context>`.

---

## Worked example — scoring that drives rerank

Query: `"What is Alice's current office choice after the payment problem?"`

Terms after stopword/short filtering:
`[alice, current, office, choice, after, payment, problem]`; `question_type="current_state"`;
flags `requests_latest`, `requests_after`, `requests_temporal_reasoning`.

`context_relevance_score_plan` on two candidate memories:

**Fresh** — *"…Alice replaced her office preference with the downtown location after the
billing issue was resolved."*

| Contribution | Match | Points |
|---|---|---|
| groups (alice, office→location, choice→preference, after, payment→billing, problem→resolved) | 6 of 7 | +43 |
| 6 groups matched (>1, not all) → `6×12` | | +72 |
| `requests_latest` ("replaced") | | +75 |
| `requests_temporal_reasoning` ("after") | | +50 |
| `requests_after` ("after") | | +65 |
| **Total** | | **305** |

**Stale** — *"…Alice preferred the airport office before the later change."*

| Contribution | Match | Points |
|---|---|---|
| groups (alice, office) | 2 | +11 |
| 2 groups → `2×12` | | +24 |
| `requests_temporal_reasoning` ("before") | | +50 |
| `requests_after` ("later") | | +65 |
| `requests_after` penalty ("before"/"earlier") | | −25 |
| **Total** | | **125** |

`305 > 125` → the post-change memory ranks first, via intent bonuses **and** the
directional penalty docking the stale doc for "before/earlier".

---

## Secondary indexes (available engines)

The engine ships index-driven primitives. The current `retrieve_context` path uses
summary-embedding recall and evaluates the filter groups for understanding/debug only;
these commands back explicit index-driven retrieval:

- `ContextQueryIndex` — range scan of one secondary index.
- `ContextQueryIndexIntersection` — **AND-intersection** across index predicates, deduped
  and sorted by `(primary_event_time_ms, event_id_hash, primary_node_hash)`.
- `ContextTraverseTree` → `traverse_context_tree()` — cosine-scored beam-search BFS down the
  node hierarchy (`top_k_per_depth`, `max_children_scored_per_parent`, `leaf_only`); not
  currently on the `retrieve_context` path.

---

## Capacity & limits (`engine/constants.rs`)

| Limit | Value |
|-------|-------|
| Records per query (`CONTEXT_MAX_LIMIT`) | 1000 (default 100) |
| Event text | 64 KB |
| Summary / L0 / ref bytes | 16 KB / 2 KB / 4 KB |
| Embedding dimension | ≤ 4096 |

`max_events` (default 16) is the per-node event scan limit (clamped to `[1, 1000]`); total
L2 candidates per query ≈ `max_event_nodes × max_events`, then trimmed by prefilter, rerank,
and token budget.

---

## Model provider (encoding / embeddings)

Provider is config-driven per request via `ContextModelProviderConfig`: `provider_kind`
(`Mock` | `OpenAiCompatible`), `base_url`, `api_key_env` (env-only key), `model`
(chat/summary), `embedding_model` (separate), `vlm_model`, `timeout_ms`, `max_retries`,
`fallback_provider` (chained), `mock_mode`.

Switching the embedding model requires **re-embedding the corpus** — stored vectors were
written by the previous model and cosine across models is meaningless.
`context_embedding_model_hash()` exists for versioning but is not yet enforced on the recall
path. The OpenAI-compatible client currently accepts `http://` endpoints only.
