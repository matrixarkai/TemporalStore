# TemporalStore Backlog

This backlog tracks open engineering and product work discussed during the AWS/EFS, replication, cache, SDK, and productization work. It is intentionally explicit so we can turn items into issues later.

## Current Status Snapshot - 2026-06-10

Latest AWS/test state:

- The 2026-06-09 AWS replication benchmark is documented in `docs/TEMPORALSTORE_AWS_REPLICATION_BENCH_2026_06_09.md`.
- Shared-store async passed on two 2-vCPU data nodes with 2-thread write QPS `4395`, read QPS `3072`, and secondary visibility around `104 ms`.
- Shared-store sync passed with 2-thread write QPS `167`, read QPS `4705`, and secondary visibility around `101 ms`; the write path is EFS-latency-bound.
- Raft consensus mode started servers but failed on the first primary write timeout; the missing piece is still control-plane integration for a stable Raft replica group instead of anti-entropy derived-partition churn.
- The temporary AWS benchmark data nodes and EFS were destroyed after the run.
- The only remaining AWS node is `temporalstore-test-meta-01` / `i-05f55360d92c43908` (`t3.small`, public IP `34.223.234.63`) for the website/observation/monitoring pages.
- No EFS filesystems or data-node EC2 instances are currently running.
- Live public pages are `https://matrixark.ai/`, `https://matrixark.ai/observation/`, and `https://matrixark.ai/monitoring/`.

## MatrixArk LLM Context Backlog

- Add Rust direct SDK parity as an embedded/local optimization after the Rust proxy path is stable.
  - Current production decision: use the existing long-lived Rust proxy, currently `matrixark_record_log --serve`, for MCP, benchmark, and production MatrixArk paths.
  - New configuration should use `MATRIXARK_TEMPORALSTORE_RUST_PROXY` / `--rust-proxy`; `MATRIXARK_TEMPORALSTORE_RUST_CLI` / `--rust-cli` stay as compatibility aliases and debug wording only.
  - Direct SDK parity target: expose an in-process Rust SDK/binding with the same contract as C++ direct SDK for local embedded deployments, while keeping proxy/client mode as the operational default for isolation, health/readiness, metrics, backpressure, graceful restart, and deployment consistency.
  - Shared tests must prove proxy path first, then direct SDK path as an additional mode, not a replacement for production proxy mode.

- Add cold archive from TemporalStore to MatrixKV/MatrixDB without synchronous double-write.
  - Design doc: `docs/matrixark_temporalstore_cold_archive_matrixdb_matrixkv.md`.
  - Product decision: TemporalStore remains the hot serving store for active events, entities, summaries, embeddings, indexes, resources, skills, and compact telemetry. MatrixKV stores control-plane/archive metadata such as cold refs, retention policies, watermarks, and idempotency state. MatrixDB stores historical replay/debug payloads, benchmark traces, token/quality metrics, and offline analytics.
  - Hot-path rule: write once to TemporalStore; an async archiver tails the TemporalStore oplog or scans cold candidates, writes MatrixKV/MatrixDB idempotently, verifies checksums, then writes compact `context_archive_marker` / `context_cold_ref` pointers.
  - Avoid default synchronous dual-write. Allow it only as an explicit compliance mode because it increases latency and creates cross-store partial-failure risk.
  - Retention safety: raw events/chunks become TTL eligible only after cold archive verification plus compression safety gates; recalled old records can be pinned or reinforced back to hot state.
  - Backend work: add shared C++/Rust tests for archive markers, cold refs, archive watermarks, retention policies, replay fetches, checksum verification, TTL blocking, and recovery after worker restart.

- Add async `ContextNode` L0/L1 summary refresh as a first-class production pipeline.
  - Current status:
    - MatrixArk already writes `ContextSummary` and `ContextEmbedding` records for node/session/batch summaries in the Python/MCP runtime;
    - retrieval can use L0/L1 summary embeddings for tree-first traversal before leaf event/entity/segment recall;
    - current node summary generation is still too inline/test-oriented and should be separated into a durable async refresh lane before production scale.
  - Product goal:
    - event ingestion must stay lightweight and predictable;
    - `ContextNode` summaries should improve traversal quality and token density, but missing/stale summaries must not block ingestion or retrieval;
    - L0/L1 summaries should make the tree filesystem-like for developers while still being stronger than a filesystem because the summaries are temporal, embedded, versioned, filterable, and replayable.
  - Hot ingestion path:
    - append `ContextEvent` with primary key based on ingestion time;
    - write event embedding when an encoder is available;
    - write general secondary indexes such as `event_type`, `entity_type`, `classification`, `status`, `source_type`, and high-saliency terms;
    - update cheap `ContextEntity` state only when bounded and deterministic;
    - write or update a dirty summary marker for the affected leaf node;
    - never regenerate parent L0/L1 summaries synchronously on every event.
  - Dirty marker model:
    - minimal fields: `tenant_hash`, `node_hash`, `dirty_reason`, `first_dirty_at_ms`, `last_event_time_ms`, `changed_ref_count`, `propagate_depth`, `priority`, and `worker_claim_until_ms`;
    - reasons: `new_event`, `entity_update`, `segment_commit`, `resource_chunk`, `feedback`, `compression`, `manual_rebuild`;
    - duplicate dirty markers should coalesce by node and reason/window so high-volume sessions do not create write amplification.
  - Async worker behavior:
    - poll dirty nodes by priority and age;
    - batch recent changed refs and bounded prior context;
    - regenerate leaf `node_l0` first, then `node_l1` only for large/high-value/resource-heavy nodes;
    - write `ContextSummary(summary_type=node_l0|node_l1)` as a versioned record;
    - write `ContextEmbedding(ref_type=summary, embedding_type=node_l0|node_l1)`;
    - mark the processed dirty marker superseded or clear it with an audit record.
  - Parent propagation:
    - only propagate dirty state to a bounded number of ancestors;
    - parent summaries should be rebuilt from child L0 summaries plus selected entity/operator state, not by scanning all raw descendant events;
    - use debounce windows so a burst of child events triggers one parent refresh;
    - do not rewrite `ContextChildRef` edges unless a child is created, renamed, archived, or has a meaningful rank/status change.
  - Fallback behavior during retrieval:
    - if a child has fresh L0/L1 embeddings, score those first during layer-by-layer traversal;
    - if summary embedding is missing or stale, fall back to child path terms, `ContextIndex`, recent event/entity embeddings, and sparse lexical score;
    - record fallback reason in `ContextPackAudit`;
    - never fail a query only because a summary refresh worker is behind.
  - Token-budget policy:
    - prefer current `ContextEntity` state, stale blockers, exact events, and answer-bearing segments before broad summaries for answer construction;
    - use L0/L1 mostly for traversal and orientation;
    - include L1 in the prompt only when the question asks for overview/context or when raw evidence would exceed budget.
  - Observability:
    - metrics: dirty node count, oldest dirty age, refresh throughput, refresh latency, summary token size, embedding latency, parent propagation count, stale-summary fallback count;
    - UI should show summary freshness per node, latest L0/L1 text, embedding model/version, source refs, and refresh audit;
    - benchmark artifacts should report summary-hit rate, summary-stale fallback rate, and candidate children scored per layer.
  - C++/TemporalStore gap:
    - add native context APIs for dirty marker upsert/query/claim/complete;
    - ensure C++ direct SDK and proxy paths can round-trip `ContextSummary`, `ContextEmbedding`, and dirty markers under repeated benchmark ingestion;
    - keep Python memory, Rust mock/proxy, and C++ direct/proxy behavior identical for summary freshness and fallback semantics.
  - Tests:
    - event write marks leaf summary dirty without rewriting ancestors synchronously;
    - worker refresh writes L0/L1 summary text and embeddings;
    - parent summary refresh uses child summaries and respects propagation depth;
    - retrieval falls back correctly when summaries are missing/stale;
    - repeated events coalesce dirty markers instead of creating unbounded writes;
    - C++ TemporalStore backend matches Python memory backend for dirty marker and summary records.
  - Acceptance gates:
    - ingestion p95 does not materially change when async summaries are enabled;
    - summary worker lag is observable and bounded in local scale tests;
    - no benchmark query fails due to missing summaries;
    - tree-first retrieval improves or matches flat recall under the same token budget.

- Add future cross-user context sharing with explicit consent and policy gates.
  - Current status:
    - MatrixArk retrieval supports cross-session context for the same resolved user, bounded by `cross_session` budget, session fanout, score threshold, and rerank policy;
    - cross-user context is not current default behavior and must not be inferred from tenant/account membership alone;
    - same-user context remains the only automatic memory bridge unless a future sharing policy explicitly authorizes more.
  - Product goal:
    - support team/org knowledge sharing without leaking private user memory;
    - let users, teams, and admins publish selected memories/resources/skills into governed shared scopes;
    - keep personal memory private by default while allowing reusable project facts, runbooks, decisions, and lessons learned to help other users.
  - Scope model:
    - `private_user`: default personal conversation/events/entities, visible only to the resolved MatrixArk user and authorized service keys;
    - `session_shared`: explicitly shared within one session/thread or collaboration room;
    - `team_shared`: curated or policy-approved context visible to members of a team/workspace;
    - `tenant_shared`: org-wide shared resources/skills/policies, usually admin-managed;
    - `public_template`: non-sensitive examples, skills, docs, and reusable patterns with no private user facts.
  - Write/publish workflow:
    - default ingestion writes to `private_user`;
    - users or policy workers can promote selected refs into shared scope by writing a `ContextShareGrant` / `ContextPublishedRef`;
    - promotions should copy or reference only safe summaries/facts/chunks, not raw private dialogue unless explicitly allowed;
    - every share/publish action writes audit metadata: actor, source ref, destination scope, policy id, reason, expiry, and revocation state.
  - Retrieval workflow:
    - resolve identity and access scope first from API key/SSO/session;
    - retrieve same-session and same-user private context first;
    - retrieve shared resources/skills next because they are intentionally governed context;
    - retrieve cross-user shared context only from authorized published scopes;
    - apply secondary-index filters and score thresholds before scoring shared/cross-user candidates;
    - apply a separate `cross_user` budget cap that is lower than cross-session by default, for example 5-10% of remote context budget, unless the query is clearly asking for team/shared knowledge;
    - return provenance labels such as `private_user`, `team_shared`, `tenant_shared`, and `published_by` in `ContextPack` refs so the agent can cite why a memory is visible.
  - Rerank policy:
    - cross-user candidates must pass the normal similarity threshold first;
    - rerank should favor curated shared resources, skill sections, approved decisions, and high-confidence project facts;
    - demote raw cross-user conversation snippets unless they are explicitly published, cited, and high-confidence;
    - prefer summaries/entities over raw dialogue for cross-user context to reduce privacy risk and token cost.
  - Governance and security:
    - require explicit sharing grants, RBAC scopes, and tenant policy before any cross-user memory is eligible;
    - support expiry, revocation, legal hold, and data-retention class per shared ref;
    - audit every cross-user candidate selection and replay request;
    - never let a user query enumerate another user's private sessions through timing, counts, or dropped-ref metadata;
    - portal should show "shared with me", "shared by me", policy source, expiry, and revoke controls.
  - Data model candidates:
    - `ContextShareGrant`: source scope, target scope, role/group/user selectors, allowed ref types, expiry, policy id;
    - `ContextPublishedRef`: shared ref hash, source ref hash, sanitized text/summary, citation, sensitivity label, owner, version;
    - `ContextAccessDecision`: request id, candidate ref, allow/deny reason, policy id, audit classification.
  - Tests:
    - private user A memory is invisible to user B by default;
    - user B can retrieve user A's published team decision only when a valid share grant exists;
    - revoked or expired grants disappear from normal retrieval but remain auditable;
    - cross-user retrieval respects score thresholds, budget caps, and provenance labels;
    - shared resources/skills remain retrievable separately from private memory;
    - C++ and Rust native retrieval return identical allow/deny and selected-ref behavior under the shared corpus.
  - Acceptance gates:
    - zero private cross-user leakage in negative tests;
    - portal and audit can explain every cross-user selected ref;
    - benchmark/report fields separate `same_user_cross_session` from `cross_user_shared`;
    - default `cross_user.enabled=false` until product policy, UI, and shared corpus tests are complete.

- Promote secondary-index filtering from MatrixArk runtime into native TemporalStore serving APIs.
  - Current status: MatrixArk now writes general `ContextIndex` terms such as `event_type:*`, `entity_type:*`, `classification:*`, `status:*`, `source_type:*`, and `segment_topic:*`; scope fields such as `team` and `project` remain scope/path isolation fields, not default secondary indexes.
  - Current runtime behavior: retrieval infers conservative AND/OR filter groups from the query, applies them after tree selection and before event/entity/segment scoring, and records matched/dropped candidate counts in `ContextPack` audit metadata.
  - Native TemporalStore gap: add server-side secondary-index lookup and AND filtering so the store can return candidate refs before MatrixArk reads full records or computes similarity.
  - Next tasks: expose `QUERY_CONTEXT_INDEX`, index-intersection APIs, and benchmark dense-only vs secondary-prefiltered vs hybrid sparse+index retrieval at tenant scale.

- Replace product-visible per-term `ContextIndex` fanout with TemporalStore-native feature/sequence-style secondary filtering for hot context serving.
  - Design note: see `docs/matrixark_temporalstore_secondary_index_design.md`.
  - Problem:
    - the legacy compatibility path writes compact `ContextIndexRef` postings under `ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}`;
    - C++ default extracted-context writes now use bucketed postings under `ctxidx2:{tenant_hash}:{scope_hash}:{index_name}:{time_bucket_ms}` with `index_value_hash` stored in the ref payload;
    - those refs are smaller than duplicated events, but they still grow with `objects x index_terms_per_object`;
    - resource imports, PDFs, repos, and keyword-heavy chunks can create too many index refs and too much debug/audit noise.
  - Target:
    - keep `ContextEvent` and other hot context data as timestamp-keyed series;
    - apply declared compact field filters in the TemporalStore native scan/query path, similar to the older `TemporalFeatureQuery` / `TemporalFeatureFilter` API;
    - use a small fixed index-family set: `source_type`, `event_type`, `entity_type`, `resource_type`, `unit_kind`, `skill_trigger`, `skill_tool`, `keyword_id`, `relative_path_hash`, and `visibility_scope`;
    - store field/value ids or hashes in native structures instead of repeated `index_name` strings in serving records.
  - C++/Rust work:
    - add `matrixark_scan_candidates` that accepts scope, node ids, data model set, time range, compact secondary filters, and a candidate limit;
    - make `matrixark_retrieve_context_pack` call this native scan before scoring and packing;
    - C++ native retrieve now caches parsed placement-event candidates by `scope_key + node_hash + record_type + append_watermark + resource_version_watermark + skill_status_watermark + index_posting_watermark`;
    - C++ SDK now exposes a `temporalstore_matrixark_batch_append_records_v2` native append boundary that accepts append policy JSON, coalesces by key/field, groups writes by placement/storage route, and keeps full audit/debug out of the synchronous hot path;
    - keep old `ContextIndexRef` writes as a compatibility/debug path until native candidate scan is fully used by both C++ and Rust;
    - hide `ContextIndexRef` rows from serving ContextPack output by default and expose them only through debug/audit sampling.
  - Acceptance gates:
    - PDF/resource debug traces no longer show unbounded `context_index` rows;
    - index writes are capped per object and per import;
    - native scan returns fewer candidates than broad prefix scan before embedding scoring;
    - C++ and Rust return the same candidate ids and same selected refs under shared corpus tests.

- Keep MVP entity extraction flat, but attach entities to filesystem-like `ContextNode` paths; add multi-layer entity/path extraction later.
  - Product decision:
    - OpenViking's public design is clearly hierarchical through a filesystem/context-layer model for resources, memory, and skills;
    - VikingMem's paper design is clearly event/entity/operator based, with events dynamically updating typed entity state;
    - neither should be treated as proof that production VikingMem currently extracts arbitrary deep parent/child entity trees from every conversation turn;
    - MatrixArk should keep v1 simpler: extract flat typed `ContextEntity` records, attach them to the best `ContextNode`, and let the node path carry the hierarchy.
  - MVP model:
    - `ContextNode` owns hierarchy: tenant/team/project/topic/segment/business-object;
    - `ContextEntity` owns evolving state: preference, relationship, location, job/status, current plan, family/profile fact, approval state, budget state;
    - `ContextEvent` owns raw replayable evidence;
    - `ContextSegment` can remain segment metadata, but high-value segments should materialize as `ContextNode(node_type=segment)` so they can parent events/entities.
  - Example:
    - node path: `company_a/infra_team/project_1/approvals/gpu_purchase`;
    - entities attached to that node: `approval_status=approved by Alice`, `budget=$42,000`, `owner=infra_team`;
    - events under that node: raw approval, budget, review, correction, and confirmation turns.
  - Why not deep entity extraction in MVP:
    - LLMs can over-create entity nodes and produce inconsistent names such as `GPU purchase`, `GPU request`, and `gpu_purchase_8891`;
    - deep entity extraction requires canonicalization, merge/dedupe, confidence thresholds, and write-amplification controls;
    - v1 benchmark and product quality depend more on reliable entity state, temporal validity, replay evidence, and tree-first retrieval than on arbitrary entity nesting.
  - Future multi-layer extraction target:
    - extract candidate `node_path` plus flat entities in one pass;
    - canonicalize node path deterministically against existing `ContextNode` paths before creating new nodes;
    - allow only bounded node creation per batch/session;
    - use parent-child `ContextNode` paths for durable hierarchy, not nested JSON inside `ContextEntity`;
    - attach `ContextEntity` state to the most specific stable node;
    - materialize high-saliency `ContextSegment` as segment-type nodes when they become useful traversal parents.
  - Relationship to VectorDB removal:
    - for small/medium scopes, tree-first traversal with L0/L1 node summary embeddings can avoid a separate VectorDB because each layer scores bounded children, then leaf timelines are filtered by time/status;
    - to scale this without a VectorDB at very large tenant scope, MatrixArk needs better multi-layer path construction so candidate children per layer stay bounded and meaningful;
    - the long-term replacement for global ANN is a parent -> child entity/topic structure: MatrixArk should route context into stable hierarchical `ContextNode` paths such as user/session/topic/entity/business-object, then score only siblings at each layer instead of searching one huge flat candidate pool;
    - this hierarchy should be created from session-level extraction, resource structure, entity/topic canonicalization, and feedback, not from arbitrary user-defined schemas;
    - L0/L1 summary embeddings on each parent/child node become the "routing index"; leaf events, entities, segments, and compressed windows become the evidence store;
    - global ANN can remain an optional escape hatch for poorly organized legacy data, but the target architecture should make it unnecessary for well-ingested MatrixArk context;
    - if paths remain too flat, MatrixArk will need stronger sparse indexes, keyword graph, or external vector/sparse retrieval for recall at scale;
    - therefore multi-layer node/path extraction is not required for MVP, but it is important for the long-term "TemporalStore-only serving store" strategy.
  - Backlog tasks:
    - add `node_type` to logical node summaries where useful: `folder`, `segment`, `business_object`, `resource`, `session`;
    - add path-canonicalization tests for equivalent names and aliases;
    - add max-new-nodes-per-batch and min-confidence thresholds;
    - add batch extraction output field `candidate_node_path` with `canonical_node_path` after service-side normalization;
    - update retrieval metrics to report candidate children scored per layer and whether a query fell back to sparse/flat recall because hierarchy was too shallow;
    - benchmark flat entity state vs multi-layer node/entity state on LOCOMO/LongMemEval update and multi-hop buckets.
  - Child fanout policy:
    - retrieval should score every child under a selected parent while sibling fanout remains bounded;
    - default runtime policy is `MATRIXARK_MAX_CHILDREN_SCORED_PER_PARENT=100000` with `MATRIXARK_HARD_MAX_CHILDREN_SCORED_PER_PARENT=100000` as the guardrail;
    - 100k children is an upper safety ceiling for brute-force summary similarity in a scoped node, not the target steady state;
    - when extraction/import sees a parent approaching a warning threshold such as 50k children, it should introduce stable intermediate layers such as topic, entity type, resource type, time bucket, repository path, or business object;
    - if a caller tries to score more than the hard cap, retrieval rejects the request and tells the caller to split over-wide `ContextNode` children into deeper layers;
    - add metrics for `children_scored_per_parent`, `fanout_warning_count`, and `fanout_hard_cap_rejections` so topology health is visible before recall quality degrades.

- Implement a TemporalStore-native ContextOperator suite for event/entity memory.
  - Current status: MatrixArk already has retrieval-time `DECAY_SCORE` behavior in `tools/matrixark_mcp_server.py`: dense/sparse/node `origin_score` is combined with `time_score` and `business_score` using `Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi`. Defaults are `wtime=0.18` and `wbusi=0.22`, with configurable freshness tolerance, half-life, type weights, and instance-level business weight fields.
  - Gap: the broader VikingMem-style operator layer is not yet represented as first-class persisted operator definitions, operator execution audits, or async worker outputs in TemporalStore.
  - Operators to support:
    - `LATEST`: choose the latest valid state/event for current-state answers.
    - `VALID_AS_OF`: answer from the state valid at a requested timestamp.
    - `BLOCK_IF_STALE`: block superseded or expired facts for current-state queries while keeping them replayable for historical questions.
    - `DECAY_SCORE`: keep the current retrieval-time score formula and add persisted config/audit records.
    - `COUNT`, `SUM`, `AVG`, `MAX`: deterministic statistical operators over numeric/event fields without LLM calls.
    - `LLM_MERGE`: optional model-backed synthesis for dedupe, conflict resolution, and natural-language entity state merging.
    - `TIME_COMPRESS`: async temporal compression over cold event windows into replayable summaries with source ids.
  - Execution model:
    - hot ingestion should append events, update cheap indexes, and mark operator work dirty;
    - deterministic operators (`LATEST`, `COUNT`, `SUM`, `AVG`, `MAX`, `VALID_AS_OF`, `BLOCK_IF_STALE`, `DECAY_SCORE`) can run inline only when bounded and cheap, otherwise in a background worker;
    - LLM operators (`LLM_MERGE`, `TIME_COMPRESS`) should be async/offline by default because they are expensive, nondeterministic, and can exceed serving deadlines;
    - query serving should read precomputed entity/operator state first, then fall back to raw events and compressed summaries.
  - Storage model:
    - `ContextOperatorDefinition`: operator name, scope, target entity/event type, input fields, output field, freshness policy, and model/provider config if needed.
    - `ContextOperatorState`: target ref, output value, source refs, valid_from_ms, valid_until_ms, version, confidence, and stale/superseded markers.
    - `ContextOperatorAudit`: operator run id, dirty marker id, input refs, output ref, latency, model id, token use, fallback reason, and replay metadata.
  - LLM_MERGE policy:
    - do not rely on a second LLM call for every event;
    - prefer one-pass extraction to emit entity patches and apply deterministic approximate patching online;
    - use `LLM_MERGE` only for complex conflicts, low-confidence patches, or periodic state cleanup;
    - every merged state must keep raw source events replayable.
  - TIME_COMPRESS policy:
    - group old events by node/entity/topic and time window;
    - write compressed summaries as first-class context records with source ids;
    - never hide answer-bearing facts in benchmark runs; keep `compression_answer_hidden_count == 0` as a release gate;
    - physical TTL/pruning of raw events is optional and must be policy-controlled.
  - Cold scan/cache isolation for temporal compression:
    - cold compression scans must use a separate low-priority scan path, not the serving retrieval scan lane;
    - cold scan reads should be `no_cache_fill` / `no_promote` by default so old raw pages are not admitted into hot serving cache;
    - cold scan reads must not update hot LRU admission or recency state unless a user retrieval/replay explicitly reinforces the source refs;
    - cold scan buffers must be bounded and separate from the serving cache to avoid evicting hot events/entities/resource chunks;
    - cold scan workers should run with lower IO and CPU priority than serving retrieval and agent-facing ingestion;
    - cold scan metrics must be separate from serving scan metrics: bytes/pages scanned, no-promote reads, summary writes, skipped windows, cache-pollution prevention, IO throttling, and worker lag;
    - a cold scan may write a warm `ContextCompressionEvent` / `TIME_COMPRESS` summary, but it must not warm all raw source pages unless the query or replay explicitly requests raw evidence.
  - Tests:
    - statistical operators return exact arithmetic without LLM calls;
    - latest/current-state queries prefer entity/operator state over stale raw events;
    - LLM_MERGE fallback path preserves raw evidence and audit trail;
    - TIME_COMPRESS produces source-linked summaries and does not hide benchmark answers;
    - C++ TemporalStore direct backend stores operator definitions, states, and audits.

- Add production hybrid recall for dense, sparse, and auxiliary keyword paths.
  - Current status: MatrixArk already has MVP hybrid recall:
    - dense similarity from stored TemporalStore embeddings;
    - lightweight lexical term-overlap scoring through `sparse_lexical_score`;
    - node/tree score;
    - time decay and business weighting;
    - an auxiliary keyword path using node path, `ContextIndex`, event/entity/segment text, and quota merge.
  - Gap: this is not yet a production sparse retrieval engine. The current sparse path scans candidate records and computes token overlap; it does not build a real inverted index, BM25 index, SPLADE sparse vector index, or learned sparse retrieval model.
  - Why this matters:
    - dense embeddings are strong for paraphrase and semantic similarity, but exact names, nicknames, IDs, ticket numbers, file paths, API names, product codes, dates, and rare business terms are often better served by sparse retrieval;
    - VikingMem-style benchmark questions can include indirect or exact lexical hooks, so dense-only recall can miss answer-bearing turns even when the context exists;
    - large enterprise context cannot rely on scanning every event/entity/segment at query time, even if tree traversal keeps candidate sets smaller than a flat VectorDB/RAG layout;
    - sparse-first retrieval gives MatrixArk a high-precision path before dense scoring, while still keeping TemporalStore as the only serving store.
  - Target design:
    - add a real sparse index, starting with BM25 for MVP and leaving SPLADE-style sparse vectors as the learned sparse follow-up;
    - support sparse-first retrieval for exact/rare-term queries, then dense reranking over the sparse candidates;
    - keep dense embeddings in TemporalStore as the primary semantic recall signal;
    - keep `ContextIndex` and the keyword graph as the auxiliary path for indirect-memory and entity-keyword questions;
    - add optional late reranking later, likely outside MVP, after first-stage recall is stable;
    - expose recall mode/config for `dense_only`, `sparse_only`, `dense_sparse_hybrid`, and `hybrid_plus_keyword`.
  - Retrieval modes:
    - `dense_only`: tree/L0/L1 summary embedding traversal plus event/entity/segment dense scoring;
    - `sparse_only`: BM25/SPLADE candidates only, useful for exact-name and deterministic regression tests;
    - `sparse_first_dense_rerank`: BM25/SPLADE candidate generation, then dense score and time/business priors;
    - `dense_sparse_hybrid`: independent dense and sparse candidate pools merged by quota;
    - `hybrid_plus_keyword`: primary dense+sparse pool plus auxiliary keyword graph expansion;
    - `tree_first_hybrid`: layer-by-layer `ContextNode` traversal using L0/L1 summary embeddings, then sparse/dense recall only inside selected subtrees.
  - Storage model:
    - `ContextSparseTerm`: term hash, document/ref hash, term frequency, document length, scope/node path, updated time, and optional field name.
    - `ContextSparseStats`: term document frequency, corpus/document count by tenant/scope, average document length, and version.
    - Optional future `ContextSparseVector`: sparse dimension ids and weights for SPLADE-style retrieval.
  - Minimal fields for `ContextSparseTerm`:
    - `tenant_hash`;
    - `term_hash`;
    - `ref_hash`;
    - `ref_type` (`event`, `entity`, `segment`, `summary`, `resource_chunk`);
    - `node_hash`;
    - `field` (`text`, `summary`, `entity_state`, `path`, `metadata`);
    - `tf`;
    - `doc_len`;
    - `updated_at_ms`.
  - Minimal fields for `ContextSparseStats`:
    - `tenant_hash`;
    - `term_hash`;
    - `df`;
    - `doc_count`;
    - `avg_doc_len`;
    - `version`.
  - SPLADE follow-up fields:
    - `ref_hash`;
    - `model_id`;
    - `sparse_dim_ids`;
    - `sparse_weights`;
    - `top_n_terms`;
    - `quantization`;
    - `updated_at_ms`.
  - Ingestion path:
    - tokenize normalized event/entity/segment/summary text;
    - write `ContextSparseTerm` rows for high-value fields only, not every arbitrary payload field;
    - update `ContextSparseStats` asynchronously or in bounded mini-batches to avoid write amplification;
    - use stop-word filtering, stemming/normalization, and tenant-local term dictionaries;
    - cap per-record indexed terms so long resources do not explode the index;
    - for resources, index chunk summaries and selected answer-bearing spans first, not full raw L2 content by default.
  - Retrieval path:
    - apply scope/time/status filters first;
    - fetch sparse candidates from the sparse index instead of scanning all records;
    - compute BM25 with tenant/scope-local `df`, `doc_count`, and `avg_doc_len`;
    - for SPLADE, dot product the query sparse vector against `ContextSparseVector` candidates;
    - normalize sparse score into the existing `origin_score` blend;
    - merge with dense node/tree traversal candidates by explicit quota, not one flat unbounded list;
    - continue to combine final score as `Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi`;
    - independently rank auxiliary keyword results and merge them with an explicit quota.
  - Default first implementation:
    - implement BM25 first because it is deterministic, cheap, explainable, and testable without GPU/model dependencies;
    - store BM25 term postings in TemporalStore using hash/table records;
    - use current dense embeddings for semantic recall;
    - keep SPLADE behind a feature flag until model download/runtime and storage overhead are measured.
  - Scale thresholds:
    - if selected subtree has fewer than a bounded number of candidate records, exact dense scoring remains acceptable;
    - if selected subtree or tenant scope exceeds the threshold, use BM25 sparse-first or hybrid candidate generation;
    - candidate limits should be config-driven: `sparse_top_k`, `dense_top_k`, `keyword_top_k`, `final_top_k`, and `deadline_ms`.
  - Query planning:
    - exact identifiers, names, file paths, API names, dates, and quoted phrases should raise the sparse quota;
    - vague semantic questions should raise the dense quota;
    - "remember my nickname" and similar indirect-memory questions should raise the keyword-graph quota;
    - current-state questions should consult `ContextEntity`/operator state before raw event sparse search.
  - Observability:
    - record per-path candidate counts, selected refs, dropped refs, score distributions, and latency;
    - report dense-only vs sparse-only vs hybrid hit deltas in benchmark artifacts;
    - add counters for sparse index freshness lag and postings scanned per query;
    - expose why a candidate was selected: dense, sparse, keyword, time, business, entity-state, or stale-blocker.
  - Benchmark plan:
    - dense-only vs sparse-only vs dense+sparse hybrid vs hybrid+keyword path;
    - run LOCOMO and LongMemEval-style/official datasets with the same token budgets;
    - report context recall, evidence-session recall, final judge score, token use, p50/p95 retrieval latency, and failure buckets;
    - add ablations showing whether the keyword path helps nickname/preference/indirect-memory questions.
  - Tests:
    - sparse-only recovers exact-name/nickname/event-type facts that dense-only misses;
    - dense-only recovers semantic paraphrases that sparse-only misses;
    - hybrid beats or matches both on mixed benchmark subsets;
    - hybrid+keyword improves indirect-memory examples without overwhelming primary recall;
    - C++ TemporalStore direct backend stores sparse terms/stats and returns deterministic sparse candidates.
  - Acceptance gates:
    - no full-record sparse scan for large tenant scopes;
    - sparse index writes are idempotent for repeated ingestion;
    - BM25 scores are deterministic between Python memory and C++ TemporalStore direct backend;
    - sparse-first retrieval improves exact/rare-term benchmark buckets without reducing broad semantic recall;
    - benchmark report includes path-level recall and token-efficiency metrics for dense, sparse, hybrid, and keyword paths.

- Add a budget-aware ContextPack optimizer for L0/L1 summaries, messages, and resource chunks.
  - Problem:
    - current packing is question-type-aware, but still mostly ranks candidate refs independently;
    - under tight budgets, MatrixArk needs to decide whether to spend tokens on L0 orientation, L1 richer overview, more conversation turns/events, entity state, or one highly relevant resource chunk;
    - this decision should be explicit and benchmarked because it is central to "same token budget, higher answer quality".
  - Target policy by query type:
    - overview/broad exploration: include L0 first, then L1 for top nodes, then a small number of representative events/resource chunks;
    - fact/current-state: prefer ContextEntity/operator state and answer-bearing event/resource facts before L1 summaries;
    - evidence/citation: prefer raw cited resource chunks or raw dialogue turns before summaries;
    - procedure/how-to: prefer relevant SkillSection or troubleshooting/resource chunks before extra conversation history;
    - multi-hop: allocate across multiple selected nodes/sessions/entities before adding deeper detail from one node;
    - date/temporal: include session date/exact turn and valid-as-of state before broad summaries.
  - Resource chunk versus more messages:
    - choose a resource chunk first when it is cited, answer-dense, current version, and has high lexical/entity overlap with the query;
    - choose more messages/events first when the query asks what the user said/did/felt, requires temporal sequence, or needs dialogue evidence;
    - choose entity state first when asking current preference/status/owner/budget/deadline;
    - include L1 only when it improves orientation or compresses many low-value turns better than raw evidence.
  - Implementation shape:
    - add a pack planning stage before `select_token_budgeted_refs` that assigns per-class token budgets: summaries, entities, events, resources, skills, compression;
    - expose config knobs: `summary_budget_ratio`, `resource_budget_ratio`, `conversation_budget_ratio`, `min_evidence_refs`, `max_summary_refs`, and `allow_l1_in_prompt`;
    - record pack plan, selected class mix, dropped class mix, and answer-density estimates in `context_pack_telemetry` and sampled audit;
    - add ablation: same candidates packed chronologically vs semantic score vs MatrixArk budget optimizer.
  - Acceptance gates:
    - same max_context_tokens improves or matches judge score versus current packing;
    - same judge score uses fewer tokens on LOCOMO/LongMemEval/resource QA slices;
    - evidence-turn recall and citation recall do not regress;
    - L0/L1 prompt inclusion is explainable in audit/telemetry.

- Make operational telemetry the default visibility layer; keep replay/audit bounded.
  - Product stance:
    - always-on visibility should look like lightweight service telemetry: QPS, p50/p95/p99 latency, token pressure, candidate counts, time-weighted recall stats, fallback flags, and error/timeout counts;
    - rich ContextPack replay/audit is valuable for debugging, compliance, and benchmark artifacts, but should be controlled by mode, sampling, retention, and access scope;
    - this keeps MatrixArk closer to production observability while preserving stronger replay/debug governance as an opt-in capability.
  - Implemented MVP:
    - `audit_mode=full|telemetry_only|off` on retrieval, also configurable with `MATRIXARK_CONTEXT_AUDIT_MODE`;
    - `MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE` / `audit_sample_rate` for tunable rich audit sampling when `audit_mode=full`;
    - `context_pack_telemetry` records with compact operational counters and no raw selected/dropped text;
    - `telemetry_only` writes telemetry but skips heavy `context_pack_audit` replay payloads;
    - rich audit is forced for partial ContextPacks, insufficient context, or quality warnings when `audit_mode=full`;
    - dashboard context-pack listing can show telemetry rows as well as audit rows.
  - Next production work:
    - add policy routing for force-full-audit on low confidence, explicit debug, or compliance scopes;
    - add retention tiers: telemetry hot, audit warm/cold, replay artifacts object-store backed;
    - expose Prometheus counters directly from telemetry fields;
    - add portal controls for per-account audit mode and retention.

- Borrow the useful VikingMem retrieval/extraction ideas without copying heavy infra.
  - Product stance:
    - MatrixArk should keep TemporalStore as the serving store and keep the hot path simple;
    - borrow the memory-quality loop: one-pass extraction, event/entity state, operators, multi-path recall, time/business priors, keyword graph, and bounded reranking;
    - avoid adding a default VectorDB or always-on cross-encoder reranker unless benchmarks prove the simpler TemporalStore path is insufficient.
  - Already implemented or MVP-present:
    - one-pass batch/session extraction via `matrixark_batch_extract` / `matrixark_session_commit`;
    - lightweight online event ingestion before batch extraction;
    - typed `ContextEvent` and evolving `ContextEntity` state with stale blockers;
    - resource-specific extraction into normal events/entities with `source_chunk_hash`;
    - time decay plus business/importance scoring through `Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi`;
    - primary plus auxiliary recall paths with quota merge;
    - question-type-aware packing and deterministic lightweight reranking through `packing_sort_key`;
    - dropped-ref audit for token-budget tuning and replay.
  - Easy next implementations:
    - expose the existing lightweight rerank in reports as `rerank_stage=packing_rerank`;
    - add per-query counters for first-stage candidates, reranked candidates, selected refs, and rerank/pack latency;
    - add ablation mode `same_candidates_chrono_vs_semantic_vs_matrixark_pack` to prove the token win comes from packing/rerank, not different recall;
    - add config docs for `ranking.rerank.mode=none|packing|multivector` with `packing` as default.
  - Medium implementation:
    - implement BM25 first-stage sparse index before SPLADE because it is deterministic, cheap, explainable, and backend-parity friendly;
    - implement full `ContextKeyword` / `ContextKeywordEdge` graph for indirect-memory questions;
    - add operator-state records for `LATEST`, `VALID_AS_OF`, `BLOCK_IF_STALE`, `COUNT`, `SUM`, `AVG`, `MAX`, `DECAY_SCORE`, `LLM_MERGE`, and `TIME_COMPRESS`.
  - Later / not MVP:
    - ColBERT-style multi-vector rerank with compressed token vectors;
    - learned SPLADE sparse vectors;
    - cross-encoder rerank only as optional offline/high-latency mode, not the default interactive agent path.
  - Rerank policy:
    - use the current lightweight packing rerank by default;
    - add heavy rerank only for top 32/64/128 first-stage candidates;
    - enforce a rerank deadline and fallback to weighted recall;
    - never let rerank block partial ContextPack return;
    - record rerank mode, deadline, candidate count, fallback reason, and score deltas in `ContextPackAudit`.
  - Benchmark gates:
    - rerank improves or matches judge score under fixed token budgets;
    - rerank does not reduce context recall on LOCOMO/LongMemEval buckets;
    - p95/p99 rerank latency stays inside the interactive-agent target;
    - fallback path returns a valid ContextPack when rerank times out.
  - Retrieval-quality ideas to borrow next:
    - report time-weighted recall separately from temporal compression so we can tune recency without confusing it with old-event summarization;
    - add benchmark ablations for recency off/on and short/long half-life;
    - add query-type overrides where date/history/valid-as-of questions reduce recency bias while current-state questions increase it;
    - add explicit evidence-density metrics after rerank: answer-bearing tokens, selected old-vs-recent refs, and selected entity/event/resource/skill mix;
    - add keyword-graph recall for indirect-memory questions before adding heavy neural rerank.

- Implement a full VikingMem-style Keyword Graph for auxiliary recall.
  - Current status: `docs/matrixark_weighted_multi_path_recall.md` implements an auxiliary keyword path over node path, `ContextIndex`, event type, entity text, and segment topics. This is useful, but it is not yet the full keyword graph described in VikingMem.
  - Target behavior: for indirect-memory questions such as "Do you remember my nickname?", dense semantic matching can fail because the query and the original memory may have low direct similarity. Build a keyword graph where each keyword is connected to associated memories and memory segments.
  - Keyword embedding: compute each keyword embedding as the average of embeddings from memory segments containing that keyword, so the keyword captures the semantic neighborhood of the memories it links to.
  - Storage model:
    - `ContextKeyword`: keyword text, keyword hash, scope/node path, averaged embedding, source segment refs, source event/entity refs, updated time, confidence, and optional business weight.
    - `ContextKeywordEdge`: keyword hash -> memory ref, with edge weight, source count, last_seen_ms, and node hash.
  - Ingestion/update path:
    - extract salient keywords from `ContextSegment`, `ContextEntity`, and high-importance `ContextEvent`;
    - update keyword averages asynchronously or in bounded mini-batches;
    - keep writes lightweight by appending edges and refreshing keyword embeddings outside the hot event write path when needed.
  - Retrieval path:
    - encode query;
    - retrieve keyword candidates by lexical match plus keyword embedding similarity;
    - expand from top keyword nodes to linked memory refs;
    - rank this auxiliary path independently from primary dense-sparse recall;
    - merge with a smaller auxiliary quota, preserving the existing primary-path-heavy behavior.
  - Tests:
    - nickname/preference indirect query where dense semantic search alone misses the memory;
    - keyword graph recovers the linked memory;
    - averaged keyword embedding changes after new segments arrive;
    - stale/superseded linked memories are blocked or demoted;
    - C++ TemporalStore direct backend round trip for keyword nodes and edges.

- Add multi-vector rerank for high-quality memory reranking under p99 latency budgets.
  - Current status: MatrixArk has primary dense/sparse scoring, time decay, business weights, and an auxiliary keyword path. OpenViking open-source appears to support external/cloud rerank providers, but the backlog still needs a TemporalStore-native multi-vector reranker for low-latency memory search.
  - Problem: cross-encoder rerankers can be accurate but may add seconds of p99 latency when candidate sets are large. Memory retrieval needs reranking in hundreds of milliseconds, especially for interactive agents.
  - Target design: precompute and store ColBERT-style token-level or phrase-level vectors during extraction for `ContextSegment`, high-value `ContextEvent`, and `ContextEntity` state.
  - Scoring: at query time, encode the query into multi-vectors and use late interaction / MaxSim against the stored memory multi-vectors after the first-stage TemporalStore tree recall.
  - Compression:
    - quantize multi-vectors to compact int8/4-bit or product-quantized payloads;
    - merge near-duplicate token vectors inside a memory segment;
    - cap vectors per memory by saliency and token budget;
    - keep storage overhead near dense-vector storage for common memories.
  - Storage model:
    - `ContextMultiVector`: ref hash, ref type, model id, vector count, dim, quantization, compressed vector bytes, source text checksum, updated time.
    - `ContextRerankAudit`: query id/context pack id, candidate count, reranked count, p50/p95/p99 timing, selected refs, dropped refs, and fallback reason.
  - Retrieval path:
    - run existing primary/auxiliary recall first;
    - rerank only a bounded candidate set, e.g. top 32/64/128;
    - return fallback ranked results if rerank exceeds deadline;
    - record whether rerank was used in `ContextPackAudit`.
  - API/config:
    - `ranking.rerank.enabled`;
    - `ranking.rerank.max_candidates`;
    - `ranking.rerank.deadline_ms`;
    - `ranking.rerank.model`;
    - `ranking.rerank.quantization`;
    - `ranking.rerank.fallback=weighted_recall`.
  - Tests:
    - rerank improves a hard query where dense/sparse first-stage order is wrong;
    - timeout fallback returns weighted-recall results without failure;
    - quantized scores stay close to float scores;
    - C++ TemporalStore direct backend stores and reads multi-vector payloads;
    - benchmark reports p50/p95/p99 rerank latency and answer-quality delta.

## Prior Status Snapshot - 2026-06-08

Completed since the last backlog update:

- Restore checkpoint:
  - Restored the cleaned source tree on branch `main-no-deps`.
  - Restored the optional client-example CMake guard so missing benchmark-only sources do not break normal builds.
  - Restored the AWS shared-store vs Raft comparison harness under `tools/workspace/aws_temporalstore_raft_vs_shared_test.ps1`.
  - Added the recovered AWS result note: `docs/aws_raft_vs_shared_store_scale_2026-06-08.md`.

- AWS shared-store vs Raft comparison:
  - Shared-store/EFS path passed the low-thread STRING replication smoke and scale run on two 2-vCPU data nodes.
  - Best observed low-thread shared-store result in that run was about 17.2k write QPS and 19.7k read QPS at 8 threads, with zero benchmark errors.
  - Raft path was not yet positive-tested on AWS; it timed out on primary writes at the 5s propose/request boundary.
  - The Raft result is a correctness blocker, not a performance result.
  - A newer 2026-06-08 shared-store async STRING run passed at 19.9k write QPS and 20.7k read QPS at 8 threads with zero errors; see `docs/aws_shared_store_string_qps_2026-06-08.md`.
  - A 2026-06-08 Raft bring-up attempt was blocked before benchmark because the deployed AWS server binary is stale and does not recognize current-source read-mode flags; see `docs/raft_production_readiness_and_qps_2026-06-08.md`.
  - Current source now has data-Raft snapshot/load callback hooks, a per-partition applied-index sidecar, local-stream loading for Raft replicas, and a scoped readonly bypass for committed Raft apply.
  - Local `file://` partition snapshot export/import is implemented for Raft: condition, index, oplog, and page stream files are copied into the snapshot and reinstalled by rebuilding volatile partition managers. Raft remains guarded for non-local stores until S3/shared-store snapshot adapters and failover tests pass.

- Website and product docs:
  - MatrixArk website now uses public product names: TemporalStore, MatrixDB, and MatrixKV.
  - TemporalStore blog was expanded with architecture, write/read workflow, storage layout, replication, recovery, feature-platform positioning, LLM-context boundaries, and engineering caveats.
  - Product observability pages now exist for TemporalStore, MatrixDB, MatrixKV, and Prometheus under the website/monitoring UI source.

- Observability:
  - Prometheus-compatible text endpoint is live for TemporalStore in the AWS test cluster.
  - Current scrape sources are metaserver, data01, and data02.
  - MatrixDB and MatrixKV observability pages are prepared, but live exporter targets are not wired yet.

- Source-control hygiene:
  - Workspace AWS/deploy/package helper scripts were added under `tools/workspace/`.
  - Smoke packaging helpers and safe SSM runtime templates were added under `tools/workspace/smoke-packaging/`.
  - Runtime archives, binaries, dependency trees, logs, presigned URL files, release/debug outputs, and generated artifacts were intentionally excluded.

- AWS testing state:
  - Latest 30-minute TemporalStore service smoke kept STRING, COMMON, HASH, SET, FEATURE, IPS, and RISK stable for 1,525 iterations.
  - TemporalAggregate failed in the deployed artifact with `Request server resp size check failed`; this is still open and must not be presented as a successful aggregate scale result.
  - Two-replica table creation/recovery still has a `Missing condition info` path in the deployed runtime; secondary aggregate reads remain untrusted until fixed.

## P0 Correctness And Recovery

- Complete RustRaft-backed data-node Raft replication mode.
  - Design doc: `docs/data_node_raft_replication_design.md`.
  - Keep it separate from shared-store replay and `secondary_pull_stream_from_primary`.
  - Add partition-level Raft FSM that applies committed `storage::OpLog` through `ObjectManager::ReplayOplog()`.
  - Ack strong writes only after quorum commit.
  - Add leader-only write guard and explicit read modes: leader, linearizable/read-index, replica-stale, and replica-min-index.
  - Add snapshot/install-snapshot before allowing new/far-behind nodes to recover without the old primary's local files.
  - Current code has a guarded flag `--data_replication_mode=raft_consensus` backed by RustRaft. Direct command writes still need to be refactored to propose through Raft before local mutation.
  - Added isolated `DataRaftCommandEntry` command envelope and codec.
  - Added committed command apply path through `Partition::ApplyDataRaftCommand` / `ApplyDataRaftEntry`.
  - Added a first write-only leader path that proposes the command envelope through RustRaft and waits for the local FSM applied index before acknowledging.
  - Added pending apply-response tracking so the leader returns the response/status produced by the committed FSM apply.
  - Added a fail-closed guard for empty RustRaft snapshots. Snapshot/checkpoint tests must opt in explicitly with `--data_raft_enable_empty_snapshot_for_tests=true`; production `file://` Raft snapshots should use the real partition snapshot path.
  - Added partition-thread-safe FSM apply: Raft apply bounces to the partition's owning worker thread, while leader proposal waiting runs on the server background async pool to avoid self-deadlock.
  - Added ReadIndex, AddLearner, PromotePeer, and bounded-stale-read guardrail hooks.
  - Added an explicit Raft read policy gate: leader-only by default, linearizable leader reads through ReadIndex, bounded-stale secondary reads with an index-lag budget, and an unsafe bring-up mode.
  - Added ByteKV-style `FlexibleApply` handling for data Raft batches so data/no-op/meta/config-change entries advance applied index in order.
  - Added per-partition applied-index sidecar restore/advance under `--data_raft_work_dir/applied/<partition_id>`.
  - Added data-Raft snapshot/load callback wiring from RustRaft FSM into `Partition`.
  - Changed data-Raft replicas to load their own local streams instead of restoring primary/shared-store stream metadata.
  - Changed data-Raft replicas to open local streams writable for committed Raft apply while keeping direct client writes blocked on readonly partitions.
  - Changed committed data-Raft apply to bypass local readonly/quota write-admission checks so committed entries apply deterministically on replicas.
  - Hardened the AWS shared-store vs Raft harness so Raft runs are opt-in bring-up tests, not accidental production benchmarks.
  - The current AWS runtime must be rebuilt before Raft can be tested because the deployed binary does not include the latest `data_raft_read_mode` flags.
  - Remaining work is real snapshots/install-snapshot, atomic storage+applied-index recovery point, learner catch-up proof, promotion/failover tests, routing integration, and AWS zero-error Raft smoke.
  - Production policy: use `raft_consensus` for no-data-loss deployments after proposal/apply/snapshot are complete; use `shared_store --storage_async=false` as the conservative fallback today; use `storage_async=true` only for streaming data that can be replayed.

- Enforce one primary lease/epoch per logical shard.
  - Metaserver must be the authority for each logical shard's primary partition id and epoch.
  - Every primary promotion must create a strictly higher epoch and publish it through membership metadata.
  - Data nodes must reject writes if their local partition role or epoch is stale.
  - Clients/proxy must refresh routing after a stale-primary or epoch-mismatch error.
  - Add a split-brain guardrail test where an old primary keeps running after metaserver promotes a secondary; the old primary must reject writes or be fenced before writes can succeed.
  - Existing code has `primary_id`, `partition_set_version`, and `election_term`; harden this into an explicit lease/fencing contract.

- Add a freshness gate before secondary promotion.
  - Only promote a secondary if its replicator status is healthy.
  - Verify it has replayed through the required oplog/index-log checkpoint.
  - Verify it can serve required page streams after promotion.
  - Reject or delay promotion if the candidate is behind.

- Test primary-down failover with `PROMOTE_SECONDARY`.
  - Kill the primary data-node process.
  - Confirm metaserver freezes the old primary.
  - Confirm a secondary becomes the new primary.
  - Confirm clients refresh routing and writes go to the new primary.
  - Confirm other replicas pull from the new leader.

- Define the recovery contract for old pages.
  - Oplog alone is not enough after page/index dump.
  - Recovery needs dumped page streams, index metadata, and oplog after the checkpoint.
  - For primary-pull mode, ensure the promoted leader has local/shared page streams or add a page-copy/snapshot-copy path.

- Add a primary-pull recovery test that includes dumped historical pages.
  - Force page dump and oplog truncation.
  - Restart a secondary.
  - Verify it rebuilds from page streams plus recent oplog.
  - Repeat after primary promotion.

- Keep both replica recovery paths.
  - `secondary_pull_stream_from_primary=true`: pull index/oplog/page streams from current primary.
  - `secondary_pull_stream_from_primary=false`: read directly from shared/object store.
  - Make both paths easy to switch by table/cluster config, not only process flags.

- Define primary vs secondary read policy.
  - Primary reads are the default for freshest and safest read-after-write semantics.
  - Secondary reads are allowed only for replica-eligible, stale-tolerant reads.
  - Secondary responses must expose or be gated by freshness/lag metadata.
  - Do not route correctness-critical risk/fraud aggregate reads to secondary until replay and lag tests pass.

## P0 EFS And Shared Storage Cleanup

- Tune old blob deletion for EFS.
  - Current defaults are conservative: `stream_blob_deletion_min_age=24h`, `stream_blob_deletion_min_gap=10GB`.
  - Add EFS/test profiles with smaller values.
  - Keep production values safer until failover tests prove the retention window.

- Add shared-store orphan scanner.
  - List files under the shared store root.
  - Compare with live stream headers/index metadata.
  - Report unreferenced blobs before deleting.
  - Add a dry-run mode.

- Add EFS storage metrics.
  - Live page bytes.
  - Obsolete blob bytes.
  - Deleted bytes per minute.
  - Oldest obsolete blob age.
  - Oplog retained bytes.
  - Index-log retained bytes.

- Add an EFS cleanup guardrail test.
  - Write enough data to create multiple blobs/zones.
  - Force dump/GC/truncate.
  - Verify files are actually removed from EFS/shared-file path.
  - Verify readers still work after cleanup.

## P0 Replication And Lag Testing

- Keep the AWS primary-pull vs shared-store comparison repeatable.
  - Current result doc: `docs/primary_pull_vs_shared_store_replication_2026-06-05.md`.
  - Convert the manual SSM commands into a checked-in script.
  - Run with fixed low thread counts for 2-vCPU instances.

- Add concurrent write/read lag benchmarks.
  - STRING visibility lag under background writes and reads.
  - TemporalAggregate visibility lag under background writes and reads.
  - Sequence/Feature model lag under background writes and reads.
  - Run both no-sleep time-to-visible checks and steady-state read-latency checks.

- Separate raw read latency from visibility/retry latency.
  - Report first-attempt read latency.
  - Report retry count.
  - Report time-to-visible on secondary.

- Add lag metrics to the monitoring UI.
  - p50/p95/p99/max lag.
  - Missing keys/features during lag probe.
  - Replica replay throughput.
  - Oplog/index-log gap.

- Fix deployed TemporalAggregate replay/read path.
  - First reproduce the `Request server resp size check failed` with the deployed client/server artifacts.
  - Verify protocol/module version compatibility between client tools and server binaries.
  - Add a minimal TemporalAggregate single-key write/query smoke before returning to high-cardinality benchmarks.
  - Add a secondary replay smoke specifically for TemporalAggregate after the primary path is fixed.

## P0 Build And Release Hygiene

- Keep debug and release outputs separate. Status: partially done, keep enforcing.
  - Avoid overwriting debug and release binaries.
  - Keep `release` and `debug` folders stable.

- Reduce binary/artifact size.
  - Split dynamic libraries from executables.
  - Strip release binaries.
  - Package only needed runtime libraries.
  - Keep binaries out of Git.

- Fix runtime launch scripts to always set `LD_LIBRARY_PATH`.
  - AWS restart failed once because `libthrift.so.0.11.0` was not found.
  - Server, proxy, benchmark, and client examples should use the same runtime env file.

- Never commit third-party dependency code or generated binary artifacts.
  - No internal dependency code.
  - No build archives.
  - No large `.a`, `.so`, `.tar.gz`, or build directories.
  - No presigned URL files or temporary AWS security-token JSON.

- Keep packaging scripts source-controlled.
  - `tools/workspace/` now contains one-cluster AWS and packaging helpers.
  - `tools/workspace/smoke-packaging/` now contains smoke packaging helpers and safe SSM templates.
  - Next: move one-off root scripts into these folders or delete them after confirming they are obsolete.

## P1 Storage Backend Roadmap

- Harden `shared-file://` for EFS.
  - Confirm condition checks work under multi-node EFS.
  - Decide whether `flock` is needed or intentionally avoided.
  - Add compare-and-condition semantics tests.

- Keep S3/object-store backend as a future durable shared storage option.
  - Use one unified stream/store interface.
  - Support S3-compatible stores and future ByteStore-like backends.
  - Test MinIO or another S3-compatible implementation for local/dev only.

- Add AWS credential and cloud object-store readiness for MatrixArk resource/skill ingestion.
  - Support standard AWS credential discovery: `AWS_PROFILE`, `AWS_REGION`/`AWS_DEFAULT_REGION`, environment credentials, web identity/IRSA, EC2/ECS instance roles, and explicit `MATRIXARK_S3_ENDPOINT_URL` for S3-compatible stores.
  - Add a credential preflight for cloud-mode ingestion: verify caller can `PutObject`, `GetObject`, and optional `DeleteObject` under the configured `s3_bucket`/`s3_prefix` before resource extraction starts.
  - Keep MatrixArk API keys separate from AWS credentials: MatrixArk auth decides who may ingest; AWS IAM decides where raw files are uploaded/read.
  - Define least-privilege IAM templates for resource uploads, resource reads, snapshot/archive reads, and optional lifecycle cleanup.
  - Add local MinIO tests for `raw_storage_mode=cloud` so S3 upload/download behavior is validated without real AWS credentials.
  - Never persist AWS access keys, session tokens, presigned URLs, or credential JSON in TemporalStore records, logs, ContextPacks, or benchmark artifacts; store only `s3://` raw URIs plus redacted credential-source metadata.
  - Add future hooks for KMS/SSE, bucket policy validation, cross-account role assumption, and cloud audit correlation.

- Define backend selection.
  - Local file: single-node/dev only.
  - Shared file/EFS: AWS simple shared-storage deployment.
  - S3/object store: compute-storage separation, higher latency, stronger durability.
  - Primary-pull: catch-up/recovery optimization.

- Add retention policy by backend.
  - Local file can be aggressive.
  - EFS should balance cost and recovery retention.
  - S3 can use lifecycle policies plus stream-aware deletion.

## P1 Performance

- Tune durable write batching.
  - Current durable mode `storage_async=false` protects recovery but EFS write QPS is low.
  - Evaluate bounded async commit profiles:
    - low-latency: 1 ms or 256 KB
    - default: 2 ms or 512 KB
    - throughput: 5 ms or 1 MB
    - batch ingest: 10-50 ms or 4 MB
  - Record RPO tradeoff for each profile.

- Tune stream/blob size for high-QPS writes.
  - Too-small blobs cause frequent blob switching/freezing/opening.
  - Test larger `stream_max_blob_size` on EFS.

- Test with bigger instances and local NVMe.
  - Compare EFS durable path vs local NVMe plus primary-pull.
  - Measure whether write latency is mostly EFS sync latency.

- Add realistic high-cardinality TemporalAggregate workloads.
  - 1k, 10k, and 100k features.
  - Multiple dimensions.
  - Full-window filters.
  - Concurrent writes and reads.
  - Secondary lag under load.

## P1 Data Models

- Finish TemporalAggregate product shape.
  - Counters: count/sum/min/max.
  - Dimensions: country, merchant, campaign, action type, device type.
  - Window query: arbitrary recent windows over bucketed state.
  - Filter query: dimension predicates over bucketed state.
  - Clear semantics for raw events vs bucket increments.

- Clarify IPS vs TemporalAggregate.
  - IPS: profile/event history with richer per-event state.
  - TemporalAggregate: compact bucketed aggregate state for risk/fraud/frequency cap.
  - Document when to use each.

- Add long sequence feature benchmarks.
  - Thousands of rows per key.
  - Complex filters.
  - Windowed sequence retrieval.
  - Compare primary reads and secondary reads.

- Add JSON model decision.
  - Either implement RedisJSON-like model later or document it as out of scope for the first open-source release.

## P1 Observability And UI

- Polish monitoring UI with node-level view. Status: first multi-product version done.
  - Metaserver, proxy, and data-node status.
  - Partition role and placement.
  - Primary/secondary mapping.
  - Replay mode: primary-pull vs shared-store.
  - Next: wire live MatrixDB and MatrixKV scrape targets.

- Add scale-test panel. Status: placeholder/page structure exists, live data wiring remains.
  - Workload type.
  - Threads.
  - QPS.
  - p50/p95/p99/max latency.
  - Error count.
  - Secondary lag.

- Add data-model panel. Status: first UI copy exists, live module metrics remain.
  - STRING.
  - HASH.
  - Feature/sequence.
  - IPS.
  - TemporalAggregate.

- Add GC/storage panel.
  - EFS bytes.
  - Page-store live bytes.
  - Obsolete bytes.
  - Oplog retained bytes.
  - Last GC time.
  - Delete failures.

## P1 SDK And Client Experience

- Stabilize direct SDK and proxy SDK.
  - C++ dynamic/static libraries.
  - Python client.
  - Go client.
  - Java client.
  - Rust client.

- Document production client behavior.
  - Routing refresh.
  - Primary-pinned writes.
  - Secondary-eligible reads.
  - Retry policy.
  - Visibility semantics.

- Add proxy test coverage.
  - STRING and HASH smoke already passed.
  - FeatureQuery through proxy returned zero points before and needs debugging.

## P1 MatrixArk Structured Retrieval Plan

- Keep query understanding ahead of embedding scoring. Status: implemented in
  Python MatrixArk retrieval.
  - Produce `query_type`, structured secondary filters, temporal window, and
    execution order in `ContextPack.recall_policy.query_plan`.
  - Enforce scope before candidate eligibility.
  - Use `ContextIndex` hits as node prefilter/hints before L0/L1 traversal.
  - Verify leaf candidates against secondary filters before embedding scoring.

- Push the same plan into native C++/Rust retrieval. Status: backlog.
  - Scope + secondary-index filters should prune prefix/index scans before
    Python sees records.
  - Keep fallback when no index matches so recall does not silently drop valid
    candidates.
  - Report matched node count, dropped candidates, and fallback reason in
    ContextPack audit/telemetry.

## P1 Cloud Deployment

- Keep AWS Terraform small for testing.
  - One metaserver/proxy/client node.
  - Two data nodes.
  - No EFS mount on metaserver unless it also runs a data node.
  - Reuse existing TemporalStore cluster for tests.

- Add deployment runbook.
  - Apply Terraform.
  - Upload binaries.
  - Start metaserver/proxy/data nodes.
  - Run smoke tests.
  - Run scale tests.
  - Collect results.
  - Destroy or keep resources intentionally.

- Add cost controls.
  - Instance stop automation.
  - Clear owner/project tags.
  - EFS cleanup and size alarms.
  - Document that stopped EC2 does not charge compute but EBS/EFS/IPs may still charge.

## P2 Product And Documentation

- Update the one-cluster TemporalStore doc with latest AWS numbers. Status: partially done.
  - Primary-pull vs shared-store comparison.
  - EFS write latency caveats.
  - Secondary lag numbers.
  - Add the newest Prometheus/live-observability screenshots or endpoint references.

- Update the blog/product docs. Status: TemporalStore deep-dive blog done; keep product pages current.
  - High-cardinality temporal features.
  - Fraud/risk/frequency cap examples.
  - Why not plain Redis/ordinary KV for arbitrary windows and filtered aggregates.
  - Storage cleanup story for durable online serving.
  - Add a shorter executive version for non-technical buyers.

- Add architecture diagrams. Status: blog/website diagrams added; deeper docs still need diagrams.
  - Metaserver/proxy/data-node request flow.
  - Primary-pull replication.
  - Shared-store recovery.
  - Page/index/oplog cleanup.
  - Failover and promotion.

- Add model-extension UI/DSL design.
  - Register custom model.
  - Register UDF.
  - Validate schema.
  - Test model in sandbox.
  - Deploy model version.

## P0 C++ And Rust TemporalStore Parity

- Make C++ and Rust parity a release gate, not a documentation claim.
  - Feature parity means the same shared corpus cases pass on both backends.
  - Production readiness means the same live topology gates, failover tests,
    observability, and storage lifecycle policies pass on both backends.
  - Performance parity means Rust and C++ run the same live benchmark profile
    with comparable p50/p95/p99, QPS, errors, timeout rate, fallback flags,
    and selected-ref quality.

- Lock the shared contract first.
  - All cross-language behavior should live in TemporalStoreTestCorpus or an
    equivalent shared contract runner, not in separate "almost same" C++ and
    Rust tests.
  - Required shared cases:
    - append/read/scan/delete;
    - native batch append;
    - prefix scan;
    - compact secondary-index postings;
    - cache hot/cold residency;
    - eviction and rehydrate from persistence;
    - TTL/GC/compaction;
    - async oplog and sync oplog;
    - no-metaserver single-node mode;
    - metaserver namespace/table/topology;
    - proxy/client routing;
    - Redis command compatibility;
    - multi-node shared-store mode;
    - Raft write/read/failover/snapshot/membership/scale-up/scale-down;
    - MatrixArk ContextMemory ingestion, extraction, retrieval, audit-light
      telemetry, replay/debug, resources, skills, cross-session, and shared
      resource paths.

- Close C++ native context retrieval correctness first.
  - C++ native retrieve must return the same logical selected refs as Rust and
    the Python reference packer for the same corpus.
  - Validate:
    - scope filtering;
    - placement-key filtering;
    - compact secondary-index prefiltering;
    - stale/superseded filtering;
    - shared resource/skill quota;
    - cross-session quota and rerank;
    - event/entity/resource/skill candidate packing.
  - Do not tune C++ latency until selected refs are correct.

- Push serving-critical work into both engines.
  - Python MCP should remain API/auth/model orchestration only.
  - C++ and Rust should own:
    - placement-key routing;
    - append queue and batch append;
    - sync/async/shared-store/Raft durability routing;
    - prefix scan;
    - compact secondary-index lookup;
    - parsed candidate cache;
    - candidate fetch;
    - score/rerank;
    - token-budget pack assembly;
    - telemetry counters.
  - Python should receive a mostly finished ContextPack and compact telemetry,
    not thousands of raw records or JSONL logs.

- Make storage and lifecycle knobs symmetric.
  - C++ and Rust must accept the same public tuning names:
    - `TS_CONTEXT_PAGE_TARGET_BYTES`;
    - `TS_BLOCK_SEGMENT_TARGET_BYTES`;
    - `TS_STORAGE_ZONE_SIZE`;
    - `TS_STREAM_MAX_BLOB_SIZE`;
    - `TS_COMPACTION_WATERMARK_BYTES`;
    - `TS_COLD_SCAN_NO_CACHE_FILL`;
    - `TS_PAGE_INDEX_CACHE_BYTES`;
    - `TS_BLOCK_INDEX_CACHE_BYTES`.
  - C++ may map the storage-facing subset into existing gflags; Rust should
    consume the same names through a typed config surface.
  - Scale and parity reports must include `effective_storage_tuning` for each
    backend so C++ and Rust runs are comparable before QPS/latency claims.
  - Add parity validation to CI so one side cannot add a production knob without
    the other side seeing it.

- Production readiness gates.
  - Before MatrixArk or benchmark runs:
    - start C++ and Rust backends;
    - verify metaserver reachability;
    - verify namespace/table/topology;
    - verify slot coverage and primary assignment;
    - run a same-backend warmup write/read/delete;
    - fail closed with `topology_not_ready` rather than letting parsing or
      retrieval fail later.
  - For Raft:
    - prove metaserver failover;
    - prove data-node replication failover;
    - prove membership add/remove;
    - prove learner catch-up and promotion;
    - prove snapshot restore;
    - prove scale-up/scale-down.
  - For storage lifecycle:
    - prove cache eviction is memory-only;
    - prove durable page/block reclaim through compaction/GC;
    - prove raw context/resource retention policy is separated from physical
      store reclaim.

- Performance parity gates.
  - Run C++ and Rust under the same:
    - dataset/corpus;
    - backend mode;
    - storage family;
    - write mode;
    - Raft/shared-store topology;
    - token budget;
    - batch size;
    - embedding/reader/judge model config for MatrixArk benchmarks.
  - Required measurements:
    - ingest QPS;
    - retrieve QPS;
    - p50/p95/p99 ingest latency;
    - p50/p95/p99 retrieve latency;
    - timeout count;
    - error count;
    - backend fallback flags;
    - memory/hash/model fallback flags;
    - selected-ref count and selected-ref parity;
    - scanned records, index hits, candidate-cache hits, placement partitions
      touched, pack tokens, and audit/debug queue depth.
  - Required scale points:
    - 1K, 10K, and 100K event ingestion;
    - retrieve workers 4, 8, 16, and 32;
    - large PDF/CSV/repo resource import;
    - full ContextMemory pipeline with retrieval and audit-light telemetry
      enabled;
    - full LOCOMO and LongMemEval_s only when official dataset paths and model
      endpoints are available.

- C++ performance focus.
  - Fix native context retrieve correctness before latency work.
  - Replace broad scan/score with placement-key plus compact index postings.
  - Add parsed candidate cache keyed by `scope_key + node_hash + record_type +
    watermark`.
  - Move final pack assembly native so Python does not materialize candidates.
  - Move MatrixArk batch writes below HSet into the native append queue and
    coalesced write path.
  - Separate audit/debug writes from the serving path by default.
  - Detailed C++ context performance optimization plan:
    - Phase 0: establish a stable baseline.
      - Run the same corpus through C++ native, Rust native/proxy, and Python
        reference retrieval.
      - Record selected refs, dropped refs, scanned records, index hits,
        candidate count, token count, p50/p95/p99, timeout count, and fallback
        flags.
      - Treat `selected_refs=0` or major selected-ref drift as correctness
        failure, not a latency result.
    - Phase 1: fix native retrieve correctness.
      - Normalize `scope_key`, `node_hash`, `placement_key`, resource/skill
        scope, stale/superseded state, and shared-resource visibility before
        scoring.
      - Add a parity assertion:
        same source corpus -> C++ selected refs approximately equal Rust/Python
        selected refs, with allowed ordering differences only when scores tie.
      - Add explicit debug counters for candidates dropped by scope, placement,
        index filter, stale version, token budget, and score threshold.
    - Phase 2: replace broad scans with placement-key routing.
      - Route hot context records by `context:{scope_key}:node={node_hash}`.
      - Fetch only selected node placement partitions after L0/L1 traversal.
      - Keep broad prefix scan only as fallback/debug and mark it in telemetry.
      - Track `placement_partitions_touched` and `records_scanned_per_query`.
    - Phase 3: push compact secondary-index prefilter fully native.
      - Use compact postings keyed by `scope_key + index_name + time_bucket` or
        equivalent old TemporalStore secondary-index mechanism.
      - Query plan:
        query understanding -> secondary filters -> postings lookup ->
        candidate ref ids -> candidate fetch -> score/rerank.
      - Avoid one index row per event in the hot serving path; split large
        postings only after the configured max refs per posting.
    - Phase 4: add parsed candidate cache.
      - Cache compact candidate structs, not JSON strings.
      - Key cache by `scope_key + node_hash + record_type + append_watermark`.
      - Invalidate on append watermark change, resource version change, skill
        status change, or index posting update.
      - Track cache hit/miss, parse time saved, and invalidation reason.
    - Phase 5: move score/rerank/pack native.
      - C++ owns candidate scoring, temporal decay, business boosts,
        same-session boost, shared resource quota, cross-session quota, and
        token-budget packing.
      - Python receives compact ContextPack payload plus telemetry, never raw
        candidate tables.
      - Keep debug/audit details out of the prompt payload by default.
    - Phase 6: optimize write path below HSet.
      - Add or harden `matrixark_batch_append_records` in the C++ engine.
      - Route by placement key, coalesce record/index/embedding/audit-light
        writes, and persist according to `storage_options`.
      - Measure append queue wait, coalesced batch size, storage engine time,
        sync/async durability result, and per-record write amplification.
    - Phase 7: separate audit/debug from serving latency.
      - Inline only cheap telemetry counters.
      - Sample or enqueue ContextPack audit/debug records asynchronously.
      - Full replay/audit is enabled by policy, not on every hot retrieval by
        default.
    - Phase 8: add C++ per-stage metrics.
      - `query_plan_ms`, `node_traversal_ms`, `index_prefilter_ms`,
        `candidate_fetch_ms`, `score_ms`, `pack_ms`, `audit_ms`,
        `append_queue_wait_ms`, `append_engine_ms`, `selected_refs`,
        `dropped_refs`, `scanned_records`, `index_postings_read`,
        `candidate_cache_hit`, and `placement_partitions_touched`.
    - Phase 9: run gated scale validation after each fix.
      - 1K, 10K, and 100K event ingestion.
      - Retrieve workers 4, 8, 16, and 32.
      - Large PDF/CSV/repo resource imports.
      - ContextMemory pipeline with resources, skills, cross-session retrieval,
        compact secondary indexes, and audit-light telemetry.
      - Compare C++ vs Rust for selected-ref parity, p50/p95/p99, QPS, errors,
        timeouts, and fallback flags.
    - Acceptance target:
      - C++ native retrieval returns non-empty, relevant ContextPacks for the
        shared corpus.
      - C++ selected refs are logically equivalent to Rust/Python reference.
      - C++ no longer performs broad scans on normal indexed queries.
      - C++ hot retrieval path does not block on full audit/debug writes.
      - C++ p95/p99 are within the agreed parity envelope for the same storage
        mode and topology.

- Rust performance focus.
  - Keep Rust on proxy/direct-SDK paths, not the old record-log concept for
    production.
  - Add Rust direct SDK parity with C++ direct SDK where embedded/local mode
    matters.
  - Reduce Rust write-path tail by batching below proxy serialization, pooling
    clients, and coalescing append batches.
  - Keep Rust native retrieve/pack behavior byte/logically symmetric with C++.
  - Expose the same Prometheus/Grafana metrics as C++.

- Reporting and status labels.
  - `feature_correct`: shared contract cases pass for C++ and Rust.
  - `performance_candidate`: live C++ and Rust runs complete under the same
    config with no silent fallbacks.
  - `production_performance_parity`: full benchmark metrics are within agreed
    thresholds and selected-ref quality is equivalent.
  - Reports must include C++ pass/fail, Rust pass/fail, unsupported cases,
    output diffs, latency deltas, QPS deltas, fallback flags, and open blockers.

## P2 Future Areas

- Multi-tenancy.
  - Namespace isolation.
  - Per-tenant quota.
  - Placement policy.
  - Metrics and billing labels.

- Multi-region.
  - Async replication first.
  - Conflict policy for active-active only if needed.
  - CRDT support is not a near-term requirement.

- LLM context/cache exploration.
  - Temporal context/state store.
  - Integration under context retrieval systems.
  - LMCache-compatible remote storage interface only if it maps cleanly.
  - Do not position as GPU KV-cache replacement without tensor-aware cache semantics.
  - Add MatrixArk `ContextMemory` aggregate model.
    - Treat context management as one logical product/domain model, even though
      serving storage uses multiple compact record types underneath.
    - `ContextMemory` should be the API and documentation box that owns:
      `ContextNode`, `ContextEvent`, `ContextSegment`, `ContextEntity`,
      `ContextSummary`, `ContextEmbedding`, `ContextIndex`, `ResourceChunk`,
      `SkillSection`, ContextPack telemetry, and lifecycle state.
    - Do not collapse every record into one huge physical TemporalStore object:
      keep separate serving records for scan/filter/pack efficiency, independent
      retention, and low write amplification.
    - Use one canonical identity envelope for every record:
      `scope_key`, `node_hash`, `record_type`, `event_time_key` or stable
      `record_id`, `placement_key`, and optional `source_ref`.
    - Avoid repeated strings in hot records:
      account/tenant/user/session ids resolve to `scope_key`; full node paths
      resolve through `ContextNode`; model names resolve through a model
      registry/ref; keywords live in compact `ContextIndex` postings; raw/debug
      extraction payloads live in audit/debug records.
    - `ContextMemory` write path:
      agent/resource/skill input -> request-scope resolution -> node placement
      key -> event/chunk/entity/index/embedding append batch -> dirty summary
      markers -> async summary refresh.
    - `ContextMemory` read path:
      query understanding -> scope gate -> same-session node traversal ->
      shared resources/skills quota -> cross-session quota -> compact secondary
      index prefilter -> native candidate fetch/score/rerank -> budget pack.
    - Low-level TemporalStore read path must stay canonical across C++ and Rust:
      logical key/timestamp range -> `PageIndex` lookup -> `PageAddress` list ->
      `BlockIndex` lookup -> page read -> decode records.
    - Cold scan path must be separate from hot reads:
      timestamp range -> `PageIndex` scan -> no-cache page read -> bounded
      decode -> no hot-cache promotion.
    - Placement policy should colocate records for the same hot serving unit
      where practical:
      `context:{scope_key}:node={node_hash}` for session/user nodes,
      `context:{tenant_scope}:shared:resource={resource_hash}` for tenant-shared
      resources, and `context:global:*` only for explicitly global memory.
    - Cross-session memory should be a controlled part of `ContextMemory`, not a
      brute-force scan:
      default to current session first, shared resources/skills second, and
      cross-session summaries/entities/compressions only when score and budget
      justify it; raw cross-session events require high-confidence evidence
      needs.
    - `ContextMemory` should expose one management/debug view:
      node topology, raw conversation events, extracted facts/entities,
      summaries, embeddings, resources/chunks, skill sections, selected/dropped
      retrieval refs, and lifecycle/GC/backfill state.
    - C++ and Rust parity should be tested at the aggregate level:
      same source corpus -> same logical `ContextMemory` records, same compact
      indexes, equivalent ContextPack selected refs, and comparable telemetry.
    - Backlog implementation milestones:
      - add a formal `ContextMemory` schema/readme with hot-record vs debug
        fields;
      - add C++/Rust shared expected-record fixtures for each owned record type;
      - add compact API responses that return `ContextMemory` views without
        leaking audit/debug fields into the model prompt;
      - add placement-key routing and native index prefilter tests for
        session, shared resource, skill, and cross-session reads.
  - Add MatrixArk hot/cold context storage split.
    - TemporalStore should hold hot serving records only: `ContextEvent`,
      `ContextEntity`, `ContextSummary`, `ContextEmbedding`, `ContextIndex`,
      resource chunks, skill records, and compact `ContextPack` telemetry needed
      for online retrieval.
    - MatrixKV or another SQL-compatible cold metadata DB should hold immutable
      raw agent ingestion logs for backfill, audit-light replay, portal history,
      and offline analysis.
    - S3 or object storage should hold large raw resource files and original
      bytes, referenced from serving records by `raw_uri`; TemporalStore should
      keep parsed chunks, citations, summaries, embeddings, and indexes rather
      than raw file bytes.
    - Backfill should read raw messages/resources from MatrixKV/S3 in batches,
      rebuild serving context records, and write to TemporalStore through native
      batch append paths with idempotency keys.
    - Add MatrixKV/S3 to TemporalStore backfill pipeline.
      - Source of truth:
        - MatrixKV or SQL-compatible metadata tables store immutable raw agent
          ingestion envelopes, raw message batches, agent/session/account scope,
          local-context refs, resource import manifests, parser inputs, model
          config, and ingestion timestamps.
        - S3/object storage stores raw file bytes for PDFs, repos, images, and
          other large resources; MatrixKV rows store `raw_uri`, checksum, content
          length, media type, and access scope.
      - Backfill cursoring:
        - scan MatrixKV by `(account_id, tenant_id, user_id, session_id,
          ingestion_time, ingestion_id)` or by a compact `scope_key +
          ingestion_time` key;
        - keep a durable `backfill_job` cursor with last processed source key,
          source checksum, batch number, output watermark, status, retry count,
          and error summary;
        - support resume, pause, cancel, dry-run, and replay-from-time without
          duplicating TemporalStore records.
      - Batching semantics:
        - batch raw messages by session and time window, not arbitrary global
          order, so summaries/entities remain stable;
        - preserve original per-message event timestamps with a finer-grained
          event key such as `timestamp_ms:sequence:event_hash` so multiple
          events from one message never overwrite each other;
        - use deterministic extraction windows, for example 20-message rolling
          commits, so online ingestion and offline backfill can be compared even
          when batching changes boundaries;
        - for resources, batch parsed chunks by resource version and
          `content_hash`, then write manifest/chunks/indexes/embeddings through
          native batch append.
      - Idempotency and versioning:
        - derive idempotency keys from source row id, raw checksum, parser
          version, extraction model config, resource version, and destination
          scope;
        - record superseded resource chunks by `content_hash` and keep old
          chunks replayable while excluding them from normal retrieval;
        - use stable entity keys so re-backfill merges state instead of creating
          duplicate entities.
      - Processing stages:
        - validate scope and API-key/access metadata before writing;
        - parse resources from S3/object storage, never from TemporalStore raw
          bytes;
        - run extraction and embeddings with the configured OSS/OpenAI provider
          or deterministic CI fallback, and record fallback flags;
        - write hot serving records to TemporalStore: `ContextEvent`,
          `ContextEntity`, `ContextSummary`, `ContextEmbedding`,
          `ContextIndex`, `ResourceChunk`, `SkillSection`, and compact
          ContextPack telemetry;
        - mark affected ContextNodes summary-dirty and let async summary refresh
          rebuild L0/L1 parent summaries from child summaries plus selected
          entity/operator state.
      - Native write path:
        - Python should orchestrate jobs and model calls, then send one
          `matrixark_batch_append_records` request per bounded batch;
        - C++/Rust TemporalStore should route by placement key, coalesce writes,
          enforce async/sync/Raft storage options, update compact secondary
          index postings, and emit backfill metrics;
        - avoid local JSONL full-log scans in production backfill.
      - Correctness gates:
        - run online-vs-backfill diff reports for the same source corpus:
          selected refs, event/entity counts, summaries, resource chunks,
          embeddings, index postings, and retrieval ContextPacks;
        - tolerate deterministic window-boundary differences only when the
          logical facts and retrieved evidence remain equivalent;
        - record `backfill_source`, `backfill_job_id`, `source_watermark`, and
          model/parser versions in debug/audit-light records, not in hot
          ContextPack payloads.
      - Operations and metrics:
        - expose backfill QPS, source scan lag, output watermark lag, batch
          latency, parse/extraction/embedding latency, retry counts, skipped
          duplicate count, write failures, and fallback flags;
        - throttle by tenant and storage family so backfill does not starve live
          ingestion/retrieval;
        - use cold/no-cache scans for old source data and bounded worker pools
          for parser/model stages.
    - Add MatrixArk production storage lifecycle workers.
      - Important distinction:
        - evicting from cache only frees memory; the durable local/shared store
          still grows;
        - compacting pages/blocks rewrites live records and can reclaim stale
          page/block space;
        - deleting context records requires explicit policy, tombstones,
          replay/audit safety, and compaction;
        - deleting raw files belongs in S3/object-storage lifecycle policy, not
          TemporalStore.
      - Next milestone has two layers:
        - MatrixArk-specific retention workers such as native C++/Rust
          `matrixark_gc_expired_context_events` plus resource/audit retention
          workers decide which logical records are eligible for deletion;
        - a general TemporalStore storage lifecycle worker reclaims old
          pages/blocks for all data models, not only MatrixArk context records.
      - Recommended production behavior:
        - TemporalStore keeps only hot serving records in the online path;
        - MatrixArk compresses old events into `context_compression_event`
          records before raw event deletion is eligible;
        - raw events receive `evict_after_ms` retention markers after
          compression;
        - the native C++/Rust GC worker scans timestamp-keyed raw events in a
          cold/no-cache mode so compression does not warm old source pages;
        - before deletion, the worker verifies that compression exists,
          replay/audit retention permits deletion, no recall reinforcement
          protects the raw event, and no Raft/shared-store follower cursor still
          needs old pages.
      - Logical deletion should write tombstone and audit-light records first.
      - Physical reclaim should be storage-engine generic:
        - scan reclaimable page/block manifests across all TemporalStore data
          models;
        - respect live-record indexes, snapshots, Raft/shared-store follower
          cursors, delayed-destroy windows, and recovery retention;
        - compact or delete stale pages/blocks later in bounded batches;
        - expose reclaimed bytes, stale bytes, delayed-destroy backlog,
          compaction latency, and reclaim errors as common storage metrics.
      - MatrixArk GC workers should use timestamp-keyed context event ranges,
        compact secondary indexes, no-cache cold scans, bounded batches,
        tombstone records, retention/audit safety gates, and compaction hooks.
    - Until scheduled logical GC, TTL enforcement, and general TemporalStore
      page/block reclaim scheduling run in production profiles, local/shared
      store growth is expected as data volume grows, even when hot cache
      eviction works.
    - Shared page/block contract:
      - use `docs/temporalstore_page_block_address_contract.md` as the single
        public contract for C++/Rust `PageAddress`, `BlockAddress`,
        `PageIndexEntry`, `BlockIndexEntry`, `ObjectIndex`, `StorageZone`,
        `Segment`, `Extent`, `AppendWatermark`, `CompactionWatermark`, and
        `TombstoneGcMetadata`;
      - public reports, metrics, and parity tests should use the canonical
        names from that contract instead of backend-specific `page_store` vs
        `block_store` naming;
      - migration strategy:
        - Phase 1 documents the shared schema and compatibility aliases for old
          report fields;
        - Phase 2 renames/report-normalizes compatibility fields without
          breaking old reports;
        - Phase 3 updates Rust public structs and report DTOs to match the
          shared/C++ public names;
        - Phase 4 updates C++ reports to emit the same canonical shape as Rust;
        - Phase 5 adds shared C++/Rust tests and old-report compatibility
          fixtures;
        - Phase 6 makes parity gates fail if fields, config, metrics, or alias
          placement drift.
      - add shared C++/Rust cases for address encode/decode, stable ordering,
        timestamp range lookup, object lookup, page split, compaction rewrite,
        tombstone-before-reclaim, restart index rebuild, and no-cache cold
        scans.
      - keep `compat/page_address_compatibility_corpus.json` as the shared
        PageAddress/BlockAddress corpus and require C++/Rust CI to validate:
        encode/decode `PageAddress`, encode/decode `BlockAddress`, stable
        ordering by `{shard_id, zone_id, segment_id, page_id, offset}` and
        `{shard_id, zone_id, block_id, offset}`, timestamp range -> page
        address lookup, page split behavior, page compaction rewrite preserving
        logical records, tombstone filtering, no-promote cold scans, and
        crash/restart rebuild of `PageIndex`, `BlockIndex`, and `ObjectIndex`.
      - keep `tools/validate_page_block_metrics_parity.py` in CI so both
        engines and scale reports expose the same page/block metric names.
      - keep `tools/validate_storage_lifecycle_parity.py` in CI so both engines
        and scale reports expose the same stream, zone, eviction, GC, reclaim,
        compaction, watermark, cache-admission, and StorageManager lifecycle
        metric names.
      - keep `compat/storage_lifecycle_report_pair_corpus.json` in CI so C++
        and Rust report comparisons normalize `page_store`, `block_store`,
        stream/blob, and page-segment aliases only from
        `compatibility_aliases`, while public comparisons use
        `PageAddress`, `BlockAddress`, `PageIndexEntry`, `BlockIndexEntry`,
        `StorageZone`, `Segment`, `Extent`, `AppendWatermark`, and
        `CompactionWatermark`.
      - Acceptance: page/block parity is done only when:
        - C++ and Rust encode the same logical `PageAddress` and
          `BlockAddress`;
        - both can rebuild `PageIndex`, `BlockIndex`, and `ObjectIndex` after
          restart;
        - both expose the same page/block/stream/zone config;
        - the same corpus produces equivalent page/block index summaries;
        - cold scans, cache admission, eviction, compaction, GC, and physical
          reclaim are measured identically.

- GPU-specific models.
  - Most TemporalStore data models do not need GPU compute.
  - GPU work may matter only for vector/tensor transformations or embedding/reranking pipelines, not the core temporal storage engine.
