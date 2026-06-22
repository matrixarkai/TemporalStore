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

## Validation

Local:

```bash
python3 tools/run_matrixark_weighted_recall_test.py \
  --backend local \
  --report-json /tmp/matrixark_weighted_recall_local.json
```

C++ TemporalStore direct:

```bash
PYTHONPATH=$PWD/sdk/python \
TEMPORALSTORE_LIB=$PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
python3 tools/run_matrixark_weighted_recall_test.py \
  --backend temporalstore-direct \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --temporalstore-lib $PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --report-json /tmp/matrixark_weighted_recall_cpp_direct.json
```
