# Rust Context Pipeline Scale Benchmark - 2026-06-26

## Summary

This run validates the Rust TemporalStore context management pipeline with local deterministic scale evidence. It covers resource and skill parsing, conversation ingestion, extraction, context model fanout, secondary indexes, retrieval, injection, restart replay, sync and async shared-store replay, Raft read path, unified corpus validation, and a shared C++/Rust scale contract.

Evidence label: **local Rust TemporalStore deterministic scale evidence**.

This is not live OSS-model evidence. The open-source model attempt timed out locally after 604 seconds, so live embedding/VLM production evidence remains a blocker.

Compact report:

```text
docs/benchmark_archives/context_pipeline_scale_20260626_summary.json
```

Raw local artifacts:

```text
/tmp/ts_context_scale_20260626
```

## Commands

Rust TemporalStore context workflow gate:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-target \
cargo run -p temporalstore-rust --bin context_workflow_harness -- \
  --root /tmp/ts_context_scale_20260626/context_workflow_root
```

Shared C++/Rust scale E2E, 100 events per lane:

```bash
python3 tools/run_context_pipeline_scale_e2e.py \
  --consumer-repo <workspace>/Codex/2026-06-10/pull-rust-temporalstore-code-from-matrixarkai/work/TemporalStore \
  --events-per-lane 100 \
  --model-provider deterministic \
  --write-results /tmp/ts_context_scale_20260626/context_pipeline_scale_e2e_100.json
```

Shared C++/Rust scale E2E, 500 events per lane:

```bash
python3 tools/run_context_pipeline_scale_e2e.py \
  --consumer-repo <workspace>/Codex/2026-06-10/pull-rust-temporalstore-code-from-matrixarkai/work/TemporalStore \
  --events-per-lane 500 \
  --model-provider deterministic \
  --write-results /tmp/ts_context_scale_20260626/context_pipeline_scale_e2e_500.json
```

OSS model attempt:

```bash
python3 tools/run_context_pipeline_scale_e2e.py \
  --consumer-repo <workspace>/Codex/2026-06-10/pull-rust-temporalstore-code-from-matrixarkai/work/TemporalStore \
  --events-per-lane 100 \
  --model-provider open_source \
  --write-results /tmp/ts_context_scale_20260626/context_pipeline_scale_e2e_oss_attempt_100.json
```

The OSS attempt timed out after 604 seconds and was stopped. No live OSS-model pass is claimed from this run.

## Pipeline Readiness

| Pipeline area | Result |
| --- | ---: |
| `context_pipeline_ready` | `true` |
| Ready pipeline stages | `7` |
| Stages | `manage`, `ingest`, `extract`, `index`, `retrieve`, `inject`, `audit` |
| `ingest_extract_ready` | `true` |
| `retrieve_pipeline_ready` | `true` |
| `injected_prompt_contains_context` | `true` |
| `restart_replay_ready` | `true` |
| `shared_store_sync_ready` | `true` |
| `shared_store_async_ready` | `true` |
| `raft_read_ready` | `true` |
| `unified_corpus_ready` | `true` |
| `management_ready` | `true` |

## Resource, Skill, Conversation Scale

| Metric | Value |
| --- | ---: |
| `ready` | `true` |
| Resources parsed | `4` |
| Skills parsed | `3` |
| Conversation sources | `24` |
| Total sources | `37` |
| Accepted sources | `37` |
| Failed sources | `0` |
| Retrieved blocks | `102` |
| Retrieved events | `28` |
| Selected skills | `3` |
| Watched resources | `3` |
| Enabled skills | `3` |
| Disabled skills | `0` |
| Ingest time | `25,999 ms` |
| Retrieve time | `553 ms` |
| Secondary-index validation time | `1 ms` |

## Context Model Fanout

| Data model / index | Count |
| --- | ---: |
| Context nodes | `13` |
| Context events | `13` |
| Context segments | `13` |
| Context entities | `13` |
| Child refs | `13` |
| Embedding refs | `39` |
| Summary records | `26` |
| Compression records | `13` |
| Dirty summary markers | `13` |
| Secondary index refs | `65` |
| Secondary refs checked | `54` |
| Secondary refs found | `54` |
| Secondary refs missing | `0` |
| Summary embedding candidates | `37` |
| Summary embedding selected | `37` |
| Verbose filter groups | `1` |
| Selected refs | `102` |

The fanout and secondary-index checks passed. Resource refs, skill refs, entity refs, source refs, and summary refs were queryable through the secondary-index validation path.

## Embedding Evidence

| Metric | Value |
| --- | ---: |
| Embedding refs | `39` |
| Requested vectors | `39` |
| Generated vectors | `39` |
| Live embedding calls | `0` |
| Mock generation count | `13` |
| Production embedding evidence ready | `false` |

The deterministic pipeline generated embeddings and used summary embeddings for retrieval expansion. Live model-backed embedding evidence is still missing in this local run.

## Retrieval And Injection Benchmark

| Metric | Value |
| --- | ---: |
| `benchmark_ready` | `true` |
| Hit@K | `1.0` |
| Recall@K | `1.0` |
| MRR | `1.0` |
| Token reduction | `83.67876%` |
| Retrieve p50 | `193 ms` |
| Retrieve p95 | `223 ms` |
| Inject p50 | `212 ms` |
| Inject p95 | `234 ms` |
| Zero-hit queries | `0` |

External built-in LOCOMO/LongMemEval-style fixture:

| Metric | Value |
| --- | ---: |
| Case count | `18` |
| Hit@K | `1.0` |
| MRR | `0.5555556` |
| Zero-hit queries | `0` |
| Missing expected terms | `0` |
| Rust ContextEvent ingest | `true` |
| Direct source scoring | `false` |
| Retrieved blocks | `108` |

## Shared Scale E2E

The shared scale E2E validates a generated unified corpus through the C++ contract runner and the Rust unified mock proxy test.

| Events per lane | Status | Rust executed | Total expected events | C++ contract seconds | Rust mock seconds |
| ---: | --- | --- | ---: | ---: | ---: |
| `100` | `passed` | `true` | `203` | `4.3219521139981225` | `14.237458586001594` |
| `500` | `passed` | `true` | `1003` | `4.399508158996468` | `8.693146834993968` |

Shared E2E model/provider fields:

| Field | Value |
| --- | --- |
| Requested provider | `deterministic` |
| Effective provider | `deterministic` |
| Embedding model label | `sentence-transformers/all-MiniLM-L6-v2` |
| Summary embedding model label | `sentence-transformers/all-MiniLM-L6-v2` |
| VLM model label | `Salesforce/blip-image-captioning-base` |
| Embedding backend | `deterministic-local-fallback` |
| VLM backend | `metadata-only-vlm-fallback` |
| Embedding dimensions | `16` |
| Shared contract steps | `64` |

The generated scale case covers:

- raw query plus hints
- model query understanding boundary
- scope and time filter planning
- context node traversal
- context event and resource retrieval
- algorithmic staleness scoring
- token budgeting
- context pack generation
- API, batch, stream, resource, feedback, agent-envelope, hook, compression, summary, and summary-embedding paths

## Remaining Blockers

- Live OSS embedding/VLM scale evidence is still missing. The open-source provider attempt did not complete within 604 seconds on this local machine.
- This is local deterministic scale evidence, not Docker/AWS multi-process SLO evidence.
- The shared scale E2E uses the shared C++ contract and Rust mock proxy path; the Rust `context_workflow_harness` is the direct Rust TemporalStore engine evidence for ingestion, extraction, retrieval, injection, sync/async shared-store, and Raft read readiness.

## Verdict

The Rust deterministic context pipeline scale gate is green for local evidence. The remaining gap is live model-backed OSS scale evidence and broader Docker/AWS SLO validation.
