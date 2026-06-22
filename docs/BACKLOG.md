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

- Promote secondary-index filtering from MatrixArk runtime into native TemporalStore serving APIs.
  - Current status: MatrixArk now writes general `ContextIndex` terms such as `event_type:*`, `entity_type:*`, `classification:*`, `status:*`, `source_type:*`, and `segment_topic:*`; scope fields such as `team` and `project` remain scope/path isolation fields, not default secondary indexes.
  - Current runtime behavior: retrieval infers conservative AND/OR filter groups from the query, applies them after tree selection and before event/entity/segment scoring, and records matched/dropped candidate counts in `ContextPack` audit metadata.
  - Native TemporalStore gap: add server-side secondary-index lookup and AND filtering so the store can return candidate refs before MatrixArk reads full records or computes similarity.
  - Next tasks: expose `QUERY_CONTEXT_INDEX`, index-intersection APIs, and benchmark dense-only vs secondary-prefiltered vs hybrid sparse+index retrieval at tenant scale.

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
    - if paths remain too flat, MatrixArk will need stronger sparse indexes, keyword graph, or external vector/sparse retrieval for recall at scale;
    - therefore multi-layer node/path extraction is not required for MVP, but it is important for the long-term "TemporalStore-only serving store" strategy.
  - Backlog tasks:
    - add `node_type` to logical node summaries where useful: `folder`, `segment`, `business_object`, `resource`, `session`;
    - add path-canonicalization tests for equivalent names and aliases;
    - add max-new-nodes-per-batch and min-confidence thresholds;
    - add batch extraction output field `candidate_node_path` with `canonical_node_path` after service-side normalization;
    - update retrieval metrics to report candidate children scored per layer and whether a query fell back to sparse/flat recall because hierarchy was too shallow;
    - benchmark flat entity state vs multi-layer node/entity state on LOCOMO/LongMemEval update and multi-hop buckets.

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

- Complete Byteraft-backed data-node Raft replication mode.
  - Design doc: `docs/data_node_raft_replication_design.md`.
  - Keep it separate from shared-store replay and `secondary_pull_stream_from_primary`.
  - Add partition-level Raft FSM that applies committed `storage::OpLog` through `ObjectManager::ReplayOplog()`.
  - Ack strong writes only after quorum commit.
  - Add leader-only write guard and explicit read modes: leader, linearizable/read-index, replica-stale, and replica-min-index.
  - Add snapshot/install-snapshot before allowing new/far-behind nodes to recover without the old primary's local files.
  - Current code has a guarded flag `--data_replication_mode=raft_consensus` backed by Byteraft. Direct command writes still need to be refactored to propose through Raft before local mutation.
  - Added isolated `DataRaftCommandEntry` command envelope and codec.
  - Added committed command apply path through `Partition::ApplyDataRaftCommand` / `ApplyDataRaftEntry`.
  - Added a first write-only leader path that proposes the command envelope through Byteraft and waits for the local FSM applied index before acknowledging.
  - Added pending apply-response tracking so the leader returns the response/status produced by the committed FSM apply.
  - Added a fail-closed guard for empty Byteraft snapshots. Snapshot/checkpoint tests must opt in explicitly with `--data_raft_enable_empty_snapshot_for_tests=true`; production `file://` Raft snapshots should use the real partition snapshot path.
  - Added partition-thread-safe FSM apply: Raft apply bounces to the partition's owning worker thread, while leader proposal waiting runs on the server background async pool to avoid self-deadlock.
  - Added ReadIndex, AddLearner, PromotePeer, and bounded-stale-read guardrail hooks.
  - Added an explicit Raft read policy gate: leader-only by default, linearizable leader reads through ReadIndex, bounded-stale secondary reads with an index-lag budget, and an unsafe bring-up mode.
  - Added ByteKV-style `FlexibleApply` handling for data Raft batches so data/no-op/meta/config-change entries advance applied index in order.
  - Added per-partition applied-index sidecar restore/advance under `--data_raft_work_dir/applied/<partition_id>`.
  - Added data-Raft snapshot/load callback wiring from Byteraft FSM into `Partition`.
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

- GPU-specific models.
  - Most TemporalStore data models do not need GPU compute.
  - GPU work may matter only for vector/tensor transformations or embedding/reranking pipelines, not the core temporal storage engine.
