# MatrixArk Weighted Multi-Path Recall

MatrixArk recall now follows a VikingMem-style two-path policy while keeping
TemporalStore as the serving store.

## Primary Path

The primary path is now tree-first, then hybrid at the selected leaves:

1. Encode the raw query.
2. Start from the matching tenant/session/team tree roots.
3. Score child `ContextNode` folders with L0/L1 summary embeddings plus sparse path/summary terms.
4. Keep `top_k_per_layer` children for each selected parent.
5. Continue layer by layer until selected leaves are reached.
6. Run event/entity/segment recall only inside those selected leaf subtrees.

Extraction writes both summary levels before retrieval:

- `node_l0`: short folder abstract for cheap layer-by-layer top-K folder choice.
- `node_l1`: richer node overview for stronger folder scoring and future prompt packing.
- both levels get `ContextEmbedding` records and are stored in the same TemporalStore-backed record stream.

Leaf recall then combines:

- dense semantic score from stored event/entity/segment embeddings;
- sparse lexical score from query token overlap;
- node/tree score from the selected L0/L1 summary path.

Those are normalized into `Sorigin`. This is intentionally different from a flat RAG scan: MatrixArk should not score every event first and merely use node score as a bonus. Folder selection comes first.

## Time-Weighted Recall Versus Temporal Compression

VikingMem-style time-weighted recall is a retrieval-time ranking prior. It does
not rewrite memory. It takes the candidates that survived tree traversal,
secondary-index filtering, and hybrid recall, then boosts or decays them based
on age.

Temporal compression is different: it is a lifecycle/storage operation that
turns older low-level events into summaries/compression events, optionally with
TTL/eviction policy for raw details. Compression can reduce the number of old
raw events that retrieval scans; time-weighted recall decides how fresh versus
old candidates are ranked after they are found.

The two should work together:

- time-weighted recall: query-time scoring, no mutation;
- temporal compression: background summarization/eviction, mutates stored
  memory shape;
- valid-as-of/current-state questions can reduce or disable recency bias when
  older historical evidence is the answer.

## Time And Business Priors

Each candidate is then rescored with:

```text
Sfinal = (1 - wtime - wbusi) * Sorigin
       + wtime * Stime
       + wbusi * Sbusi
```

All scores are normalized to `[0, 1]`.

`Stime`:

- equals `1.0` inside `freshness_tolerance_ms`
- decays exponentially after that with a fast-then-slow curve
- is controlled by `half_life_ms`

`Sbusi`:

- uses an instance-level field first:
  `business_weight`, `business_score`, `importance`, or `priority`
- otherwise falls back to type-level weights such as confirmation,
  correction, approval, budget, preference, plan, status, and dialogue

## Auxiliary Path

The auxiliary path is a keyword graph recall over:

- node path
- `ContextIndex` terms
- event type
- entity type/name/state
- segment topic and summary

Primary and auxiliary results are ranked independently. MatrixArk then merges
them with a larger quota for primary recall and a smaller configurable
`auxiliary_quota`, avoiding the lower-quality behavior of one flat merged list.

## Rerank Policy

MatrixArk does need reranking, but not necessarily a heavy neural reranker on
the MVP hot path.

Current MVP rerank:

1. Run tree-first primary recall plus the auxiliary keyword path.
2. Merge primary and auxiliary candidates by quota.
3. Apply a lightweight second-stage rerank before packing:
   - question-type boost, for example `skill_section` for procedures,
     `ContextEntity` for current state, raw cited chunks for evidence, and
     extracted facts for fact questions;
   - token-efficiency score so answer-dense refs beat broad/noisy text under
     the same budget;
   - multi-hop diversification across nodes.
4. Pack selected refs under `max_context_tokens`.
5. Record selected/dropped refs and reasons in `ContextPackAudit`.

This is enough for the first production path because it is deterministic,
cheap, explainable, and easy to keep identical across Python, C++, and Rust
backends.

Future heavier rerank:

- use BM25/SPLADE to generate exact sparse candidates at scale;
- add a bounded ColBERT-style multi-vector reranker only after first-stage
  recall is stable;
- rerank only top 32/64/128 candidates;
- use strict deadlines with fallback to weighted recall;
- report rerank latency and judge-score delta in benchmark artifacts.

Backlog note: this is the first auxiliary path. The full Keyword Graph backlog
is tracked in `docs/BACKLOG.md`: build `ContextKeyword` and
`ContextKeywordEdge`, average keyword embeddings from linked memory segments,
then expand from query-matched keywords to associated memories.

Second-stage rerank backlog: `docs/BACKLOG.md` also tracks a future
ColBERT-style multi-vector reranker. The intended flow is to keep this weighted
recall as the low-latency first stage, then rerank only a bounded top-K
candidate set with precomputed compressed multi-vectors and strict deadline
fallback.

## Retrieve API Knobs

```json
{
  "query": "GPU approval risk ledger",
  "scope": {"user_id": "u1", "session_id": "s1"},
  "max_context_tokens": 6,
  "ranking": {
    "weights": {"time": 0.2, "business": 0.35},
    "freshness_tolerance_ms": 86400000,
    "half_life_ms": 604800000,
    "rerank": {
      "enabled": true,
      "mode": "packing",
      "fallback": "weighted_recall"
    },
    "business_type_weights": {
      "confirmation": 1.0,
      "dialogue_batch": 0.2
    },
    "top_k_per_layer": 8,
    "max_children_scored_per_parent": 10000,
    "auxiliary_quota": 2
  }
}
```

The response `recall_policy.tree_traversal` records whether tree traversal ran, `top_k_per_layer`, `max_children_scored_per_parent`, selected node/path counts, and whether the runtime had to fall back to flat recall because no node summaries existed.

Each selected ref now includes:

- `origin_score`
- `time_score`
- `business_score`
- `final_score`
- `recall_path`
- `ranking_formula`

The response and `ContextPackAudit` also include
`recall_policy.time_weighted_recall`, with selected-ref time-score averages,
old-versus-recent counts, max selected age, and a marker that this is a
`ranking_prior_not_temporal_compression`.

## Operational Telemetry Versus Replay Audit

For production serving, MatrixArk should favor telemetry-style visibility over
heavy replay payloads on every request.

Retrieval now supports:

- `audit_mode=full`: write compact telemetry plus rich `ContextPackAudit` for
  replay/debug. This remains the default for compatibility and benchmark proof.
- `audit_mode=telemetry_only`: write compact `context_pack_telemetry` counters
  and skip the heavy selected/dropped replay payload.
- `audit_mode=off`: skip both telemetry and rich audit for highly constrained
  paths.

`context_pack_telemetry` records include operational counters such as selected
ref count, dropped ref buckets, token use, partial-pack flags, tree fallback,
secondary-index match/drop counts, rerank candidate count, time-weighted recall
stats, and stage latency budgets. They intentionally avoid raw selected/dropped
text so the default operational view stays small.

Use `MATRIXARK_CONTEXT_AUDIT_MODE=telemetry_only` for high-throughput production
traffic, and switch to `full` for benchmark runs, debugging, compliance scopes,
or user-requested replay.

## Validation

Local:

```bash
python3 tools/run_matrixark_weighted_recall_test.py \
  --backend local \
  --report-json /tmp/matrixark_weighted_recall_local.json
```

If the local wrapper is not present in a checkout, use the shared corpus runner:

```bash
python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_weighted_recall_test.py \
  --backend local \
  --report-json /tmp/matrixark_weighted_recall_local.json
```

C++ TemporalStore direct:

```bash
PYTHONPATH=$PWD/sdk/python \
TEMPORALSTORE_LIB=$PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_weighted_recall_test.py \
  --backend temporalstore-direct \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --temporalstore-lib $PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --report-json /tmp/matrixark_weighted_recall_cpp_direct.json
```
