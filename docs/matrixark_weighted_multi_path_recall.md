# MatrixArk Weighted Multi-Path Recall

MatrixArk recall now follows a VikingMem-style two-path policy while keeping
TemporalStore as the serving store.

## Primary Path

The primary path does hybrid retrieval over MatrixArk records:

- dense semantic score from stored embeddings
- sparse lexical score from query token overlap
- node/tree score from TemporalStore context-node embeddings

Those are normalized into `Sorigin`.

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
    "auxiliary_quota": 2
  }
}
```

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
